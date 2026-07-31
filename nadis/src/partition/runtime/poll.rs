use super::*;
use std::collections::HashMap;
use std::time::Duration;

/// 合批 poll + demux(不变量 3:一条多 stream XREADGROUP NOBLOCK)。
pub(super) async fn poll_and_dispatch(
    rt: &Arc<GroupRuntime>,
    slots: &mut HashMap<u32, ClaimSlot>,
    polled: Vec<(u32, tokio::sync::OwnedSemaphorePermit)>,
) {
    // 组命令:XREADGROUP GROUP <g> <consumer> COUNT <n> STREAMS k1..kn > .. >
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP")
        .arg(rt.layout.group())
        .arg(&rt.node_id)
        .arg("COUNT")
        .arg(rt.cfg_batch());
    cmd.arg("STREAMS");
    for (p, _) in &polled {
        cmd.arg(rt.layout.stream(*p));
    }
    for _ in &polled {
        cmd.arg(">");
    }

    // NOBLOCK(不带 BLOCK 参数即非阻塞):连接瞬借,响应立即返回
    let resp: std::result::Result<redis::Value, redis::RedisError> =
        cmd.query_async(&mut rt.client.conn()).await;

    // demux:stream key → 分区号 → 批次;无数据分区 → Backoff(冷流)
    let mut got: HashMap<u32, Vec<(String, Vec<u8>)>> = HashMap::new();
    let mut protocol_err: std::collections::HashSet<u32> = std::collections::HashSet::new();
    match resp {
        Ok(v) => {
            if !parse_xreadgroup(&rt.layout, v, &mut got, &mut protocol_err) {
                // 顶层形态异常:本轮全部 polled 分区都当协议错误(消息可能已进 PEL)
                tracing::error!("XREADGROUP 响应顶层形态异常,本轮 polled 分区转 Recovering 收编");
                protocol_err.extend(polled.iter().map(|(p, _)| *p));
            }
        }
        Err(e) => {
            //**传输/超时/断连**错误下,`>` XREADGROUP 在服务端可能已执行
            //(消息已移入本 consumer PEL),客户端却收 Err——若退回 Ready,下轮 `>` 读不到这些
            // 已在 PEL 的消息,只能等 ~min_idle_ms 的 orphan sweep(默认 30s 不可消费)。故按
            // IO/超时/断连分类:这类把本轮 polled 分区转 Recovering(经 PEL 立即收编);仅明确的
            // 确定性错误(语法/NOGROUP 等)才退回 Ready 下轮重试。
            if e.is_io_error() || e.is_timeout() || e.is_connection_dropped() {
                tracing::warn!(err = %e, "XREADGROUP 传输错误,polled 分区转 Recovering(消息可能已进 PEL)");
                protocol_err.extend(polled.iter().map(|(p, _)| *p));
            } else if e.to_string().contains("NOGROUP") {
                //**NOGROUP 自愈**(control_loop 有,poll 此前没有)——consumer group
                // 被外部清理后,`>` 永远 NOGROUP → 退回 Ready 死循环空转、消息无法消费且无告警。
                // 重建各 polled 分区的 consumer group(BUSYGROUP 幂等),下轮重新拉。
                let group = rt.layout.group().to_string();
                for (p, _) in &polled {
                    let r: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
                        .arg("CREATE")
                        .arg(rt.layout.stream(*p))
                        .arg(&group)
                        .arg("0")
                        .arg("MKSTREAM")
                        .query_async(&mut rt.client.conn())
                        .await;
                    if let Err(e) = &r {
                        if !e.to_string().contains("BUSYGROUP") {
                            tracing::warn!(p, err = %e, "poll NOGROUP 自愈:重建 group 失败,下轮再试");
                        }
                    }
                }
                tracing::warn!("XREADGROUP NOGROUP:已重建 consumer group,下轮重拉");
                return;
            } else {
                tracing::warn!(err = %e, "XREADGROUP 确定性错误,本轮退回 Ready 重试");
                return;
            }
        }
    }

    // 冷流 Backoff 间隔 = stream.poll_timeout_ms
    let backoff =
        tokio::time::Instant::now() + Duration::from_millis(rt.stream_cfg.poll_timeout_ms.max(1));
    for (p, permit) in polled {
        let Some(slot) = slots.get_mut(&p) else {
            continue;
        };
        //协议错误分区(消息可能已进 PEL 但解析失败)→ **Recovering**(经 PEL
        // 收编),不当空轮 Backoff(那会让消息只能等 orphan sweep);告警留痕。
        if protocol_err.contains(&p) {
            tracing::error!(p, "XREADGROUP 解析协议错误,转 Recovering 经 PEL 收编");
            slot.generation += 1;
            let gen = slot.generation;
            slot.state = ClaimState::Recovering { generation: gen };
            let rec_cancel = slot.worker_cancel.clone();
            let rt2 = Arc::clone(rt);
            let h = tokio::spawn(async move {
                let ids = super::retry::recover_pel(&rt2, p, &rec_cancel).await;
                if !rec_cancel.is_cancelled() {
                    let _ = rt2
                        .event_tx
                        .send(Event::RecoveryFinished {
                            p,
                            generation: gen,
                            ids,
                        })
                        .await;
                }
            });
            slot.recovery_handle = Some(AbortOnDropTask::new(h));
            drop(permit);
            continue;
        }
        match got.remove(&p) {
            Some(records) if !records.is_empty() => {
                // 有数据:进 InFlight,批次连同 permit 移交 worker(容量 1,必成功)
                slot.generation += 1;
                slot.state = ClaimState::InFlight {
                    generation: slot.generation,
                };
                let batch = WorkBatch {
                    generation: slot.generation,
                    records,
                    _permit: permit,
                };
                if let Err(e) = slot.work_tx.try_send(batch) {
                    // mailbox 满/关闭 = 状态机 bug,但消息**已进 PEL**——翻回 Ready 会让
                    // 它们永久滞留(Ready 只读 `>`;复审 修正,撤回原"自愈
                    // Ready")。改:保留 ID 转 RetryBackoff,走 PEL 重投路径找回。
                    let ids: Vec<String> = match e {
                        mpsc::error::TrySendError::Full(b)
                        | mpsc::error::TrySendError::Closed(b) => {
                            b.records.iter().map(|(id, _)| id.clone()).collect()
                        }
                    };
                    tracing::error!(
                        p,
                        ?ids,
                        "worker mailbox 投递失败——状态机不变量被破坏,批次转 PEL 重投"
                    );
                    slot.state = ClaimState::RetryBackoff {
                        ids,
                        attempt: slot.retry_attempt,
                        until: tokio::time::Instant::now() + Duration::from_millis(500),
                    };
                }
            }
            _ => {
                // 空轮:冷流 Backoff(permit 立即归还 = drop)
                slot.state = ClaimState::Backoff { until: backoff };
                drop(permit);
            }
        }
    }
}

/// 解析 XREADGROUP 响应:[[stream, [[id, [f1,v1,...]], ...]], ...](RESP2 数组形态)。
/// 只取 DATA_FIELD 字段;其余 field 忽略(与 原实现 容器 demux 行为一致)。
///
///**协议错误不再静默跳过**——XREADGROUP 已把消息放进 PEL,若 entry/stream
/// 形态异常被 `continue`,coordinator 会把该分区当空轮 Backoff,消息只能等 orphan sweep。现在
/// 把形态异常的**分区号收进 `protocol_err`**(调用方据此转 Recovering 立即收编 + 告警);
/// 返回 `false` = 顶层形态异常(整个响应不可解析,调用方把本轮全部 polled 分区当协议错误)。
/// 注意:data field **缺失**是合法 tombstone(空 body 交 worker ACK 清 PEL),**不**算协议错误。
pub(super) fn parse_xreadgroup(
    layout: &KeyLayout,
    v: redis::Value,
    out: &mut HashMap<u32, Vec<(String, Vec<u8>)>>,
    protocol_err: &mut std::collections::HashSet<u32>,
) -> bool {
    use redis::Value as V;
    //第十四轮 P1(疑似 P0):XREADGROUP NOBLOCK **无新消息时返回 Nil**(RESP2 `*-1`)——这是稳态
    // 最常见路径,**绝不是协议错误**。此前 Nil 落 else→return false→调用方把全部 polled 分区判协议错误转
    // Recovering→空 PEL 立即回 Ready→下轮又 Nil→**无限 busy-spin**(每秒数千 XAUTOCLAIM)+ 分区几乎从不停
    // 在 Ready→ReleaseExcess 的 victim(仅收 Ready|Backoff)恒空→**再均衡不收敛**(即 two_nodes_rebalance
    // flake 的真因)。Nil = 本轮无新消息:正常返回 true、不写任何 entries。
    if matches!(v, V::Nil) {
        return true;
    }
    //任一 stream key 反解失败(parse_one_stream 返 false)→ 整体返 false,
    // 调用方把本轮全 polled 转 Recovering 兜底(不静默丢)。
    let mut all_ok = true;
    match v {
        // RESP2:`[[stream, [entries]], ...]`(部分形态为 Set)
        V::Array(streams) | V::Set(streams) => {
            for s in streams {
                let (V::Array(pair) | V::Set(pair)) = s else {
                    continue;
                };
                let mut it = pair.into_iter();
                let (Some(key_v), Some(entries_v)) = (it.next(), it.next()) else {
                    continue;
                };
                all_ok &= parse_one_stream(layout, key_v, entries_v, out, protocol_err);
            }
        }
        // RESP3(HELLO 3):`{stream: [entries], ...}`(Map)——当前 connect 默认 RESP2 不触发,前瞻兼容。
        V::Map(pairs) => {
            for (key_v, entries_v) in pairs {
                all_ok &= parse_one_stream(layout, key_v, entries_v, out, protocol_err);
            }
        }
        _ => return false, // 真·顶层形态异常:调用方把全部 polled 当协议错误
    }
    all_ok
}

/// 解析单个 stream 的 (key, entries):key 反解分区号,entries 提取 (id, data body)。
/// entries/entry/fields 段形态异常 → 该分区记入 `protocol_err`(转 Recovering 收编)。
/// 返回 `false` = **stream key 无法反解出分区号**(理论不可达:key 是我们自己拼的 STREAMS 入参;
/// 仅 prefix 损坏/RESP3 异形态触发)。调用方据此**全 polled 转 Recovering 兜底**(
/// 解析不出 p ≠ 没消息,不可静默丢整 stream,规范 #3/#20)。
///
/// # 参数
/// - `layout`: 分区 key 布局,用于生成 stream、marker 和 lock key。
/// - `key_v`: 业务 key 或 Redis key,用于定位数据。
/// - `entries_v`: 从 Redis 读取到的 stream 条目集合。
/// - `out`: 输出缓冲区,用于收集解析结果。
/// - `protocol_err`: 协议解析失败时保留的错误信息。
fn parse_one_stream(
    layout: &KeyLayout,
    key_v: redis::Value,
    entries_v: redis::Value,
    out: &mut HashMap<u32, Vec<(String, Vec<u8>)>>,
    protocol_err: &mut std::collections::HashSet<u32>,
) -> bool {
    use redis::Value as V;
    let key = value_bytes(key_v);
    // stream key → 分区号(布局反解:prefix:p)
    let Some(p) = key
        .strip_prefix(format!("{}:", layout.prefix).as_bytes())
        .and_then(|t| std::str::from_utf8(t).ok())
        .and_then(|t| t.parse::<u32>().ok())
    else {
        tracing::error!(
            key = %String::from_utf8_lossy(&key),
            "XREADGROUP 响应含无法反解分区号的 stream key——本轮全 polled 转 Recovering 兜底(规范 #3)"
        );
        return false;
    };
    let (V::Array(entries) | V::Set(entries)) = entries_v else {
        protocol_err.insert(p); // entries 段形态异常 → 该分区协议错误(p 已知,非 key 反解失败)
        return true;
    };
    let mut recs = Vec::with_capacity(entries.len());
    for e in entries {
        let (V::Array(epair) | V::Set(epair)) = e else {
            protocol_err.insert(p);
            continue;
        };
        let mut eit = epair.into_iter();
        let (Some(id_v), Some(fields_v)) = (eit.next(), eit.next()) else {
            protocol_err.insert(p);
            continue;
        };
        let id = String::from_utf8_lossy(&value_bytes(id_v)).into_owned();
        // fields = [f1, v1, f2, v2, ...];找 data field
        let (V::Array(fields) | V::Set(fields)) = fields_v else {
            protocol_err.insert(p);
            continue;
        };
        let fields_empty = fields.is_empty();
        let mut data: Option<Vec<u8>> = None;
        let mut fit = fields.into_iter();
        while let (Some(f), Some(val)) = (fit.next(), fit.next()) {
            if value_bytes(f) == DATA_FIELD.as_bytes() {
                data = Some(value_bytes(val));
            }
        }
        match data {
            Some(d) => recs.push((id, d)),
            // data field 缺失 → 空 body tombstone(worker ACK 清 PEL)。区分两种成因——
            //   · fields 为空([])= 被 XDEL 半删的合法 tombstone(预期,debug);
            //   · fields **非空但无 DATA_FIELD** = 可能是跨语言 publish 端写错 field 名的 bug → **warn 暴露**
            //     (不再静默掩盖)。仍按 tombstone 清,避免无限重投;若要改成"留 PEL→走毒处置",
            //     需同步改造 poll/recover_pel/worker 并评估重复投递风险。
            None => {
                if !fields_empty {
                    tracing::warn!(p, id, "entry 有 fields 但缺 data field(疑似 publish 端字段名 bug),按 tombstone 清");
                }
                recs.push((id, Vec::new()));
            }
        }
    }
    out.entry(p).or_default().extend(recs);
    true
}

/// 从 redis::Value 提取 bytes(BulkString/SimpleString 两形态)。
pub(super) fn value_bytes(v: redis::Value) -> Vec<u8> {
    match v {
        redis::Value::BulkString(b) => b,
        redis::Value::SimpleString(s) => s.into_bytes(),
        other => format!("{other:?}").into_bytes(),
    }
}
