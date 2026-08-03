use super::*;
use crate::lock::HoldStatus;
use std::time::Duration;

/// 首见序分桶:`(topic,event)` → 该桶内 `(entry_id, data)`,**保持插入序**(对齐 原实现
/// RecycleLinkedMap, 顺序模型)。用 Vec 而非 HashMap 以保序。
type OrderedBuckets = Vec<((String, String), Vec<(String, serde_json::Value)>)>;

/// Runs a partition worker until shutdown.
pub(super) async fn worker_loop(
    rt: Arc<GroupRuntime>,
    p: u32,
    lock_holder: String,
    stamp: Option<super::fencing::FencingStamp>,
    mut work_rx: mpsc::Receiver<WorkBatch>,
    cancel: CancellationToken,
) {
    // handler 路由表:worker 不直接持有(coordinator spawn 时未传)——
    // 设计修正:经 GroupRuntime 全局共享(见下 handlers());串行性由 mailbox=1 保证。
    let lock_key = rt.layout.lock_business_key(p);
    loop {
        let batch = tokio::select! {
            b = work_rx.recv() => match b { Some(b) => b, None => return },
            _ = cancel.cancelled() => return,
        };
        let generation = batch.generation;
        // handler panic 必须被捕获——裸 panic 会杀死本 worker task,WorkFinished
        // 永不发出 → 该分区永卡 InFlight 且 permit 泄漏。catch_unwind 包整个批处理。
        //**批处理期间必须监听 cancel**——此前只在等下一批时 select! cancel,
        // 一旦进入 process_batch().await 就不再监听,导致停机 deadline 到了 handler future 仍在
        // 跑(访问 DB/外部服务、长期持 permit/Arc)。现 select! 包住:cancel 触发即丢弃
        // process_batch future(handler 在其下一个 await 点被 drop)、不 ACK、return,批次(含
        // permit)随 drop 归还,消息留 PEL 交新 owner recovery(取消按失败)。
        let ids: Vec<String> = batch.records.iter().map(|(id, _)| id.clone()).collect();
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::warn!(p, "worker 在批处理中被取消,丢弃批次不 ACK(留 PEL,交新 owner)");
                return;
            }
            r = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                process_batch(&rt, p, &lock_key, &lock_holder, stamp.as_ref(), batch),
            )) => match r {
                Ok(outcome) => outcome,
                Err(panic) => {
                    let msg = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "<non-string panic>".into());
                    tracing::error!(p, panic = %msg, "handler/批处理 panic,按失败重投(不 ACK)");
                    WorkOutcome::Failed { ids }
                }
            },
        };
        // 终态事件:send().await(cancellation-aware;coordinator 已退则丢弃无害)
        let _ = rt
            .event_tx
            .send(Event::WorkFinished {
                p,
                generation,
                outcome,
            })
            .await;
    }
}

/// 单批处理全流程(permit 在 batch 内,函数返回即随 drop 归还预算)。
pub(super) async fn process_batch(
    rt: &Arc<GroupRuntime>,
    p: u32,
    lock_key: &str,
    lock_holder: &str,
    stamp: Option<&super::fencing::FencingStamp>,
    batch: WorkBatch,
) -> WorkOutcome {
    let all_ids: Vec<String> = batch.records.iter().map(|(id, _)| id.clone()).collect();

    // 第一次 fencing 检查必须发生在业务调用前；Unknown 也不得执行。
    match rt.lock.holds_status(lock_key, lock_holder).await {
        HoldStatus::Held => {}
        HoldStatus::Lost => return WorkOutcome::LostOwnership,
        HoldStatus::Unknown => return WorkOutcome::Failed { ids: all_ids }, // 保守:当失败重投
    }

    // ── decode:逐 ID 分桶(保留 id↔value 关联,用于子集结果)。
    //**坏信封只让该 ID 单独进重投/毒消息,绝不连坐整批**——此前一条解码
    //   失败即 `decode_failed=true` 返回整批 Failed,同批正常消息从未到 handler 也被一起
    //   Drop = 确定性丢消息。data field 空 = tombstone,直接清 PEL。──
    //分桶必须**保持首见
    // 插入序**(原实现 用 RecycleLinkedMap),不能用 std::HashMap(迭代序随机会打乱同 key 跨事件
    // 顺序)。这里用 Vec<((topic,event), items)> 维护首见序:同 (topic,event) 追加到既有桶,
    // 新 (topic,event) 按首见追加到末尾;`flush` 逐桶**按插入序**处理。
    let mut buckets: OrderedBuckets = Vec::new();
    let mut ack_ids: Vec<String> = Vec::new(); // 成功 / 可清理(tombstone / 未知 event)→ ACK
    let mut failed_ids: Vec<String> = Vec::new(); // 坏信封 / handler 失败 → 留 PEL 重投
    for (id, body) in &batch.records {
        if body.is_empty() {
            ack_ids.push(id.clone()); // tombstone:只清 PEL
            continue;
        }
        match serde_json::from_slice::<Envelope>(body) {
            Ok(env) => {
                let key = (env.topic, env.event);
                match buckets.iter_mut().find(|(k, _)| *k == key) {
                    Some((_, items)) => items.push((id.clone(), env.data)),
                    None => buckets.push((key, vec![(id.clone(), env.data)])),
                }
            }
            Err(e) => {
                tracing::error!(p, id, err = %e, "信封解码失败,该 ID 单独进重投/毒消息(不连坐整批)");
                failed_ids.push(id.clone());
            }
        }
    }

    // ── handler:按 (topic,event) 查路由表。erased handler 逐 ID typed-decode + 调用,
    //   返回**失败 ID 子集**(坏 data / handler 失败 / handler panic 只影响相关 ID,
    //   不连坐本桶其他成功 ID,更不连坐其他桶);未知 event = 告警 + ACK 丢弃(对齐 原实现)。──
    for ((topic, event), items) in buckets {
        let bucket_ids: Vec<String> = items.iter().map(|(id, _)| id.clone()).collect();
        match rt.handlers().get(&(topic.clone(), event.clone())) {
            Some(h) => {
                // handler 强制 timeout(文档 契约:超时按失败留 PEL)。超时 → 整桶 ID
                // 进 failed(留 PEL 重投);返回的 future 被 drop(会 yield 的 handler 在下一个
                // await 点退出,纯 CPU 不让出的中断不了——那类业务须 spawn_blocking)。
                let timeout = Duration::from_millis(rt.cfg.handler_timeout_ms);
                let failed = match tokio::time::timeout(timeout, h(items)).await {
                    Ok(failed) => failed,
                    Err(_) => {
                        tracing::warn!(
                            p,
                            topic,
                            event,
                            timeout_ms = rt.cfg.handler_timeout_ms,
                            "handler 超时,整桶转重投(留 PEL)"
                        );
                        bucket_ids.clone()
                    }
                };
                let failed_set: std::collections::HashSet<&str> =
                    failed.iter().map(|s| s.as_str()).collect();
                for id in bucket_ids {
                    if failed_set.contains(id.as_str()) {
                        failed_ids.push(id);
                    } else {
                        ack_ids.push(id);
                    }
                }
            }
            None => {
                tracing::warn!(p, topic, event, "无 handler 注册,按未知 event ACK 丢弃");
                ack_ids.extend(bucket_ids);
            }
        }
    }

    // 第二次 fencing 检查必须发生在 ACK 前；Unknown/Lost 一律不 ACK，并保留 PEL。
    match rt.lock.holds_status(lock_key, lock_holder).await {
        HoldStatus::Held => {}
        HoldStatus::Lost => return WorkOutcome::LostOwnership,
        HoldStatus::Unknown => return WorkOutcome::Failed { ids: all_ids },
    }

    // ── ACK **成功子集**(ack_ids);失败子集(failed_ids)留 PEL 重投 ──
    if !ack_ids.is_empty() {
        match ack_with_retry(rt, p, stamp, lock_holder, &ack_ids).await {
            AckResult::Ok => rt.enqueue_async_delete(p, &ack_ids).await,
            AckResult::Lost => return WorkOutcome::LostOwnership,
            AckResult::Failed => {
                // ACK 耗尽:这批未确认 → 并入 failed 重投(handler 已成功,重投会重跑 =
                // at-least-once；AckTicket 只重试 ACK 而不重跑 handler，避免重复业务副作用。
                failed_ids.extend(ack_ids);
            }
        }
    }

    if failed_ids.is_empty() {
        WorkOutcome::Acked
    } else {
        WorkOutcome::Failed { ids: failed_ids }
    }
}

/// ACK 结果(子集 ACK 用)。
enum AckResult {
    Ok,
    /// fenced 拒绝:所有权丢失。
    Lost,
    /// 重试耗尽未确认。
    Failed,
}

/// 对一组 ID 做 ACK(按 profile 分流;XACK 幂等可重试)。
/// V2(有 stamp):单 Lua 原子校验 holder+fence 三元组+任期 counter 后 XACK;
/// V1(无 stamp):holds 双检查已在调用链上方,此处裸 XACK(与 原实现 同协议)。
///
/// # 参数
/// - `rt`: 分区消费组运行时状态。
/// - `p`: 分区编号或当前协议步骤中的短名参数。
/// - `stamp`: 分区消费持有者的 fencing 戳。
/// - `lock_holder`: 当前持有分区锁的节点标识。
/// - `ids`: Redis stream entry id 或业务记录 id 列表。
async fn ack_with_retry(
    rt: &Arc<GroupRuntime>,
    p: u32,
    stamp: Option<&super::fencing::FencingStamp>,
    lock_holder: &str,
    ids: &[String],
) -> AckResult {
    let stream = rt.layout.stream(p);
    let group = rt.layout.group().to_string();
    for retry in 0..=ACK_RETRY {
        let r: std::result::Result<bool, String> =
            if let (Some(meta), Some(st)) = (&rt.fence_meta, stamp) {
                match super::fencing::fenced_ack(
                    &rt.client,
                    &rt.layout,
                    meta,
                    st,
                    lock_key_full(rt, p).as_str(),
                    lock_holder,
                    ids,
                )
                .await
                {
                    Ok(super::fencing::FencedAck::Acked(_)) => Ok(true),
                    Ok(super::fencing::FencedAck::Rejected(tag)) => {
                        tracing::warn!(p, tag, "fenced ACK 被拒,按 LostOwnership 处理");
                        return AckResult::Lost;
                    }
                    Err(e) => Err(e.to_string()),
                }
            } else {
                let mut ack = redis::cmd("XACK");
                ack.arg(&stream).arg(&group);
                for id in ids {
                    ack.arg(id);
                }
                ack.query_async::<i64>(&mut rt.client.conn())
                    .await
                    .map(|_| true)
                    .map_err(|e| e.to_string())
            };
        match r {
            Ok(_) => return AckResult::Ok,
            Err(e) if retry < ACK_RETRY => {
                tracing::warn!(p, retry, err = %e, "ACK 异常,重试");
                tokio::time::sleep(Duration::from_millis(50 << retry)).await;
            }
            Err(e) => {
                tracing::error!(p, err = %e, "ACK 重试耗尽,留 PEL 交后续接管");
                return AckResult::Failed;
            }
        }
    }
    AckResult::Ok
}

// handlers 的全局共享(coordinator 与 worker 都要查;启动后只读)
