//! 根据保留窗口裁剪分区 Stream；ACK 只解除 PEL 引用，不释放条目正文。
//!
//! 当前 leader 每隔 auto_trim_rate_ms 触发一轮，零值禁用自动裁剪。
//! Redis TIME 与 data_expire_ms 确定时间边界，已观测最老 PEL ID 限制可裁剪范围。
//! XTRIM MINID 的近似模式按存储块处理，可能保留超出窗口的条目；裁剪不能代替业务确认。

use std::sync::Arc;
use std::time::Duration;

use super::GroupRuntime;

/// 业务作用：由当前 leader 按保留窗口和已观测 PEL 边界周期裁剪分区日志，限制历史存储占用。
///
/// 参数说明：
/// - `rt`: 提供裁剪周期、保留窗口、分区布局和停止信号的消费组运行时。
///
/// 返回：裁剪周期为零时不启动选举；启用时持续竞选并调度裁剪，收到停止信号后等待 leader 退位。
/// 在途裁剪仍按其任期与停止信号收口，已发送 Redis 操作不能由取消撤销。
pub(super) async fn auto_trim_loop(rt: Arc<GroupRuntime>) {
    let rate = rt.stream_cfg.auto_trim_rate_ms;
    if rate == 0 {
        tracing::info!(group = %rt.layout.prefix, "autoTrim 已禁用(auto_trim_rate_ms=0)");
        return;
    }
    // leader 选举:全集群仅 leader 跑 XTRIM。leader 锁 key 与分区锁(`:lock:{p}`)区分,用 `:leader`。
    // graceful 停机经下方 break → leader.shutdown() 干净退位。
    let leader = crate::leader::Leader::elect(
        Arc::clone(&rt.lock),
        format!("{}:leader", rt.layout.prefix),
        Duration::from_millis(rt.cfg.rebalance_ms.max(1)),
    );
    // election 与裁剪循环必须一同接受停机控制，避免裁剪循环中止后孤立的选举任务继续续租，
    // 阻碍其它节点取得裁剪领导权。
    if let Some(ah) = leader.abort_handle() {
        rt.bg_aborts.lock().expect("bg_aborts").push(ah);
    }

    let mut tick = tokio::time::interval(Duration::from_millis(rate));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // 首跳立即完成,跳过(避免启动即 trim)

    loop {
        tokio::select! {
            _ = rt.bg_cancel.cancelled() => break,
            _ = tick.tick() => {
                // 裁剪绑定当前领导任期的取消令牌，失权后停止继续派发；已发送的 Redis 命令无法撤销。
                let rt2 = Arc::clone(&rt);
                leader
                    .run_if_leader_cancellable(|tok| async move { trim_round(&rt2, &tok).await })
                    .await;
            }
        }
    }
    leader.shutdown().await;
}

/// 业务作用：在领导任期内按 Redis 时间保留窗裁剪，并在删除时原子复验本消费组的最老 PEL 边界。
///
/// 参数说明：
/// - `rt`: 提供消费组、分区范围、保留时长、Redis 连接和停止信号的运行时。
/// - `term`: 当前领导任期的失权取消信号，不是可跨任期复用的授权。
///
/// 返回：时间读取失败时跳过全轮，PEL 读取失败时跳过该分区，单次裁剪失败后继续其它分区。
/// 每个分区及删除派发前检查停机与失权；脚本复验与 XTRIM 原子执行，近似裁剪可能保留更多历史。
async fn trim_round(rt: &Arc<GroupRuntime>, term: &tokio_util::sync::CancellationToken) {
    const TRIM_GUARDED_LUA: &str = r#"
local function uint(s)
  if type(s) ~= 'string' or not string.match(s, '^%d+$') then return nil end
  local normalized = string.gsub(s, '^0+', '')
  if normalized == '' then normalized = '0' end
  if #normalized > 20 or (#normalized == 20 and normalized > '18446744073709551615') then return nil end
  return normalized
end
local function sid(s)
  if type(s) ~= 'string' then return nil end
  local ms, seq = string.match(s, '^(%d+)%-(%d+)$')
  if not ms then return nil end
  ms, seq = uint(ms), uint(seq)
  if not ms or not seq then return nil end
  return {ms, seq}
end
local function less(a, b)
  for i = 1, 2 do
    if #a[i] ~= #b[i] then return #a[i] < #b[i] end
    if a[i] ~= b[i] then return a[i] < b[i] end
  end
  return false
end
if #KEYS ~= 1 or #ARGV ~= 2 or ARGV[1] == '' then
  return redis.error_reply('invalid trim arguments')
end
local boundary = ARGV[2]
local bound = sid(boundary)
if not bound then return redis.error_reply('invalid trim boundary') end
-- 查询后的旧快照不能授权删除；同一原子步骤内重新读取 PEL，先完成全部验证再执行 XTRIM。
local pending = redis.call('XPENDING', KEYS[1], ARGV[1])
if type(pending) ~= 'table' or #pending ~= 4 or type(pending[1]) ~= 'number'
    or pending[1] < 0 or pending[1] ~= math.floor(pending[1]) then
  return redis.error_reply('invalid pending summary')
end
if pending[1] == 0 then
  if pending[2] ~= false or pending[3] ~= false
      or (pending[4] ~= false and (type(pending[4]) ~= 'table' or #pending[4] ~= 0)) then
    return redis.error_reply('inconsistent empty pending summary')
  end
else
  local oldest, newest = sid(pending[2]), sid(pending[3])
  if not oldest or not newest or less(newest, oldest) or type(pending[4]) ~= 'table' then
    return redis.error_reply('invalid pending boundaries')
  end
  for _, consumer in ipairs(pending[4]) do
    if type(consumer) ~= 'table' or #consumer ~= 2 or type(consumer[1]) ~= 'string' then
      return redis.error_reply('invalid pending consumer')
    end
    local count = consumer[2]
    if type(count) == 'string' then
      count = uint(count)
      if not count or count == '0' then return redis.error_reply('invalid pending consumer count') end
    elseif type(count) ~= 'number' or count <= 0 or count ~= math.floor(count) then
      return redis.error_reply('invalid pending consumer count')
    end
  end
  -- 新增的更早 PEL 降低删除边界；旧边界更低时仍保留它，不因确认并发而扩大本轮删除范围。
  if less(oldest, bound) then boundary = pending[2] end
end
return redis.call('XTRIM', KEYS[1], 'MINID', '~', boundary)
"#;
    // 保留边界 = now - data_expire_ms(Redis TIME 毫秒 → MINID `{ms}-0`)
    let now = match super::command::redis_now(&rt.client).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(err = %e, "autoTrim 取 Redis TIME 失败,跳过本轮");
            return;
        }
    };
    let time_min_id = format!("{}-0", now.saturating_sub(rt.stream_cfg.data_expire_ms));
    let group = rt.layout.group().to_string();
    let mut trimmed = 0i64;
    for p in 0..rt.count {
        // 停机或失主:提前中断本轮;XTRIM 幂等,半轮中断不会破坏后续裁剪。
        if rt.bg_cancel.is_cancelled() || term.is_cancelled() {
            break;
        }
        let stream = rt.layout.stream(p);
        // Park/RetryBackoff/DLQ-pending 消息可能在 PEL 滞留超过 data_expire。
        // 纯按时间 MINID 会移除这些 entry,导致 takeover/disposition `XRANGE` 取 payload 失败并丢失消息体。
        // 先核对读取证据并选择保守边界；实际删除时还须原子复验，防止此后新增的更早 PEL 丢失正文。
        let min_id = match oldest_pending_id(&rt.client, &stream, &group).await {
            Ok(Some(pel_oldest)) => lesser_stream_id(&time_min_id, &pel_oldest),
            Ok(None) => time_min_id.clone(), // 真·空 PEL:仅时间边界
            Err(e) => {
                tracing::warn!(p, stream = %stream, err = %e, "autoTrim 取 PEL 最老 id 失败,保守跳过本分区");
                continue; // 取不到 PEL 边界则不裁(宁可不裁,绝不误删)
            }
        };
        // PEL 查询可能跨越停机或失权，不能只沿用本分区开始时的本地许可继续派发删除。
        if rt.bg_cancel.is_cancelled() || term.is_cancelled() {
            break;
        }
        let r: std::result::Result<i64, redis::RedisError> = redis::Script::new(TRIM_GUARDED_LUA)
            .key(&stream)
            .arg(&group)
            .arg(&min_id)
            .invoke_async(&mut rt.client.conn())
            .await;
        match r {
            Ok(n) if n >= 0 => trimmed = trimmed.saturating_add(n),
            Ok(_) => tracing::warn!(p, stream = %stream, "autoTrim 裁剪计数为负，无法确认删除数量"),
            // NOGROUP/不存在的 stream 等非致命:跳过该分区,不中断整轮
            Err(e) => tracing::warn!(p, stream = %stream, err = %e, "autoTrim XTRIM 失败"),
        }
    }
    if trimmed > 0 {
        tracing::debug!(group = %rt.layout.prefix, time_min_id = %time_min_id, trimmed, "autoTrim 裁剪 stream entries(裁剪边界已按各分区 PEL 最老 id 兜底)");
    }
}

/// 业务作用：读取指定消费组的 PEL 摘要，为时间裁剪提供最老待确认 ID。
///
/// 参数说明：
/// - `client`: 查询 PEL 的 Redis 客户端。
/// - `stream`: 待裁剪分区的完整 Stream 键。
/// - `group`: 需要保留待确认正文的消费组名称。
///
/// 返回：完整空摘要返回 None；正计数且 ID 边界、consumer 列表形态均合法时返回最老 ID。
/// 负数、残缺摘要、非法 ID 或边界倒置返回解析错误，调用方必须跳过裁剪；该查询仍只是一个瞬时快照。
async fn oldest_pending_id(
    client: &crate::client::RedisClient,
    stream: &str,
    group: &str,
) -> std::result::Result<Option<String>, redis::RedisError> {
    /// 业务作用：把无法解释的 PEL 摘要标为解析错误，让裁剪调用方保留原数据。
    ///
    /// 参数说明：
    /// - `msg`: 固定的结构失败原因，不承载消息正文或凭据。
    ///
    /// 返回：携带摘要解析类别的 RedisError，调用方不能把该错误当作空 PEL。
    fn parse_err(msg: &str) -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::Parse,
            "XPENDING summary 形态异常",
            msg.to_string(),
        ))
    }
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(&mut client.conn())
        .await?;
    let redis::Value::Array(cols) = v else {
        return Err(parse_err("顶层非 Array"));
    };
    // 空 PEL 必须由完整摘要证明，截断或自相矛盾的零计数不能开放纯时间裁剪。
    if cols.len() != 4 {
        return Err(parse_err("摘要字段数不是四"));
    }
    let count = match cols.first() {
        Some(redis::Value::Int(n)) if *n >= 0 => *n,
        _ => return Err(parse_err("count 不是非负整数")),
    };
    if count == 0 {
        if !matches!(cols[1], redis::Value::Nil)
            || !matches!(cols[2], redis::Value::Nil)
            || !(matches!(cols[3], redis::Value::Nil)
                || matches!(&cols[3], redis::Value::Array(rows) if rows.is_empty()))
        {
            return Err(parse_err("零计数与边界或 consumer 列表不一致"));
        }
        return Ok(None);
    }

    // 最老 ID 将直接限制删除范围，不能接受有损 UTF-8 或把非法 ID 回退成更大的时间边界。
    let min = super::wire::id(cols[1].clone()).map_err(|_| parse_err("min-id 非法"))?;
    let max = super::wire::id(cols[2].clone()).map_err(|_| parse_err("max-id 非法"))?;
    if lesser_stream_id(&min, &max) != min {
        return Err(parse_err("min-id 大于 max-id"));
    }
    let redis::Value::Array(consumers) = &cols[3] else {
        return Err(parse_err("consumer 列表非数组"));
    };
    for consumer in consumers {
        let redis::Value::Array(fields) = consumer else {
            return Err(parse_err("consumer 项非数组"));
        };
        if fields.len() != 2
            || !matches!(
                fields[0],
                redis::Value::BulkString(_) | redis::Value::SimpleString(_)
            )
        {
            return Err(parse_err("consumer 项结构非法"));
        }
        let valid_count = match &fields[1] {
            redis::Value::Int(value) => *value > 0,
            redis::Value::BulkString(value) => {
                !value.is_empty()
                    && value.iter().all(u8::is_ascii_digit)
                    && std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .is_some_and(|value| value > 0)
            }
            redis::Value::SimpleString(value) => {
                !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_digit())
                    && value.parse::<u64>().is_ok_and(|value| value > 0)
            }
            _ => false,
        };
        if !valid_count {
            return Err(parse_err("consumer 计数非法"));
        }
    }
    Ok(Some(min))
}

/// 业务作用：比较裁剪时间边界与 PEL 最老 ID，选择数值顺序更小的边界文本。
///
/// 参数说明：
/// - `a`: 按 Redis 时间和保留窗口计算的裁剪边界。
/// - `b`: 从 PEL 摘要读取的最老待确认 ID。
///
/// 返回：两个完整数字 ID 均可解析时按毫秒和序号选较小者；任一非法时返回零边界，禁止删除任何条目。
/// 零边界只表示保守保留，不是 PEL 为空的证明。
fn lesser_stream_id(a: &str, b: &str) -> String {
    /// 业务作用：为裁剪边界比较提取 Stream ID 的数值分量。
    ///
    /// 参数说明：
    /// - `id`: 待比较的时间边界或 PEL ID 文本，未给出序号时按零处理。
    ///
    /// 返回：可表示为 u64 的毫秒与序号；分量解析失败或越界时返回 None。
    fn parse(id: &str) -> Option<(u64, u64)> {
        let mut it = id.splitn(2, '-');
        let ms = it.next()?.parse::<u64>().ok()?;
        let seq = it.next().unwrap_or("0").parse::<u64>().ok()?;
        Some((ms, seq))
    }
    // 无法解释任一边界时不能取另一方较大的值，否则未知 PEL 身份会退化为纯时间删除。
    let complete = |id: &str| {
        id.split_once('-').is_some_and(|(ms, seq)| {
            !ms.is_empty()
                && !seq.is_empty()
                && ms.bytes().all(|byte| byte.is_ascii_digit())
                && seq.bytes().all(|byte| byte.is_ascii_digit())
        })
    };
    if !complete(a) || !complete(b) {
        return "0-0".into();
    }
    match (parse(a), parse(b)) {
        (Some(pa), Some(pb)) => {
            if pa <= pb {
                a.to_string()
            } else {
                b.to_string()
            }
        }
        _ => "0-0".into(),
    }
}
