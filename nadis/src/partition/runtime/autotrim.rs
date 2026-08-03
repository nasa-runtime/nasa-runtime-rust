// ============================================================================
// runtime/autotrim.rs：对齐既有 streamAutoTrim 语义的自动裁剪任务。
//
// 问题:partition stream 的 ACK **只移 PEL、不删 entry**,长跑节点 stream 单调增长 → OOM。
// 解法:**leader** 每 `auto_trim_rate_ms`(默认 60s)对所有分区 stream 跑 `XTRIM MINID ~ {now-保留窗}`,
// 保留 `data_expire_ms`(默认 1h)时间窗。
//
// 为何 leader-only:避免每节点重复 XTRIM(无害但浪费);leader-local-once + XTRIM 幂等,短暂双主无害。
// MINID 用 **Redis TIME**(与 RustV2 心跳同源)算保留边界,`~` = 近似裁剪(低成本,按宏块整删)。
// `auto_trim_rate_ms = 0` → 禁用(不裁剪,回退旧行为)。
// ============================================================================

use std::sync::Arc;
use std::time::Duration;

use super::GroupRuntime;

/// Runs periodic stream trimming for partition logs.
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
    //把内层 election 任务的 abort_handle 也纳入分区统一 abort 集——否则 bg deadline
    // 超时 abort_all 掉 auto_trim_loop 时,`leader.shutdown()` 来不及执行、election_loop 成孤儿、leader 锁
    // 看门狗继续续租,阻塞新 leader 接任至 lease 过期(此前靠 lease 兜底,现彻底干净)。
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
                //经 run_if_leader_cancellable 跑整轮 XTRIM(不再裸读 is_leader)——
                // 失主即中断本轮(term token cancel),双主跑长任务窗口压回 ≤1 tick;cancellable 不再是死代码。
                let rt2 = Arc::clone(&rt);
                leader
                    .run_if_leader_cancellable(|tok| async move { trim_round(&rt2, &tok).await })
                    .await;
            }
        }
    }
    leader.shutdown().await;
}

/// 跑一轮全分区 XTRIM(leader 任内;`term` = 本任期失主信号,失主/停机即提前中断本轮)。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `term`: 自动裁剪或消费任期的版本号。
async fn trim_round(rt: &Arc<GroupRuntime>, term: &tokio_util::sync::CancellationToken) {
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
        // 故裁剪边界取 min(时间边界,本分区 PEL 最老 id),永不裁剪仍被 PEL 引用的 entry。
        let min_id = match oldest_pending_id(&rt.client, &stream, &group).await {
            Ok(Some(pel_oldest)) => lesser_stream_id(&time_min_id, &pel_oldest),
            Ok(None) => time_min_id.clone(), // 真·空 PEL:仅时间边界
            Err(e) => {
                tracing::warn!(p, stream = %stream, err = %e, "autoTrim 取 PEL 最老 id 失败,保守跳过本分区");
                continue; // 取不到 PEL 边界则不裁(宁可不裁,绝不误删)
            }
        };
        let r: std::result::Result<i64, redis::RedisError> = redis::cmd("XTRIM")
            .arg(&stream)
            .arg("MINID")
            .arg("~")
            .arg(&min_id)
            .query_async(&mut rt.client.conn())
            .await;
        match r {
            Ok(n) => trimmed += n,
            // NOGROUP/不存在的 stream 等非致命:跳过该分区,不中断整轮
            Err(e) => tracing::warn!(p, stream = %stream, err = %e, "autoTrim XTRIM 失败"),
        }
    }
    if trimmed > 0 {
        tracing::debug!(group = %rt.layout.prefix, time_min_id = %time_min_id, trimmed, "autoTrim 裁剪 stream entries(裁剪边界已按各分区 PEL 最老 id 兜底)");
    }
}

/// 取消费组 PEL 中最老的 pending 消息 id(XPENDING summary 的 min-id)。
/// summary 形态:`[count, min-id, max-id, [[consumer,count],...]]`;空 PEL 时 count==0、min/max 为 nil。
///
///**fail-closed**——只有 `count==0`(真·空 PEL)才返 `Ok(None)`(调用方仅按时间裁剪);
/// 顶层/count/min-id **形态异常一律 `Err`**(让调用方保守跳过本分区,绝不退化成纯时间裁剪而误删 PEL 中
/// Park/DLQ-pending 的 entry)。此前把"解析失败"也当空 PEL 是残留的 fail-open 入口。
///
/// # 参数
/// - `client`: 底层客户端或连接句柄。
/// - `stream`: 待检查 PEL 的 Redis Stream key。
/// - `group`: 消费组、服务分组或任务分组名称。
async fn oldest_pending_id(
    client: &crate::client::RedisClient,
    stream: &str,
    group: &str,
) -> std::result::Result<Option<String>, redis::RedisError> {
    // Builds a Redis protocol parse error.
    ///
    /// # 参数
    /// - `msg`: 业务消息体或事件载荷。
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
    let (redis::Value::Array(cols) | redis::Value::Set(cols)) = &v else {
        return Err(parse_err("顶层非 Array/Set"));
    };
    // cols[0] = count;==0 即真·空 PEL(此时无需避让,纯时间裁剪安全)
    let count = match cols.first() {
        Some(redis::Value::Int(n)) => *n,
        _ => return Err(parse_err("缺 count 或非整数")),
    };
    if count == 0 {
        return Ok(None);
    }
    // count>0:min-id 必为有效 id;非 string 视为协议异常(fail-closed)
    match cols.get(1) {
        Some(redis::Value::BulkString(b)) => Ok(Some(String::from_utf8_lossy(b).into_owned())),
        Some(redis::Value::SimpleString(s)) => Ok(Some(s.clone())),
        _ => Err(parse_err("count>0 但 min-id 非字符串")),
    }
}

/// 比较两个 stream id(`ms-seq`),返回**较小**者(数值比较,非字符串字典序——
/// "100-0" < "99-0" 字典序会错判)。解析失败的一方视为较大(保守不参与压低边界)。
///
/// # 参数
/// - `a`: 参与当前计算或编码的第一个输入值。
/// - `b`: 参与当前计算或编码的第二个输入值。
fn lesser_stream_id(a: &str, b: &str) -> String {
    // Parses a stream entry id into numeric parts.
    ///
    /// # 参数
    /// - `id`: 业务标识,用于定位具体对象或记录。
    fn parse(id: &str) -> Option<(u64, u64)> {
        let mut it = id.splitn(2, '-');
        let ms = it.next()?.parse::<u64>().ok()?;
        let seq = it.next().unwrap_or("0").parse::<u64>().ok()?;
        Some((ms, seq))
    }
    match (parse(a), parse(b)) {
        (Some(pa), Some(pb)) => {
            if pa <= pb {
                a.to_string()
            } else {
                b.to_string()
            }
        }
        (Some(_), None) => a.to_string(),
        (None, Some(_)) => b.to_string(),
        (None, None) => a.to_string(),
    }
}
