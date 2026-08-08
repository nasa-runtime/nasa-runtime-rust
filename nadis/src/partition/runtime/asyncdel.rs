// ============================================================================
// ACK 后异步 XDEL：单 owner、有界通道、按分区批量回收 stream entry。
//
// XACK 决定消费终态，XDEL 只回收日志空间。两者不能绑成一个事务：删除失败不应把已经成功
// 处理的业务消息重新投递。发送端受 inflight 预算和有界 channel 双重约束；Redis 暂时失败时
// 保留 ID 到下一周期重试，停机在 coordinator 排空后做一次最终 flush。
// ============================================================================

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::{AsyncDeleteBatch, GroupRuntime};

/// 单条 XDEL 命令的 ID 上限，避免构造过大的 Redis 请求。
const XDEL_BATCH_SIZE: usize = 1_000;
/// owner 本地积压软上限；达到后暂停收 channel，让背压传回消费端。
const MAX_PENDING_IDS: usize = 100_000;

/// 业务作用：运行一个分区组的异步删除 owner。
///
/// # 参数
/// - `rt`: 当前分区组运行时。
/// - `rx`: ACK 成功批次的唯一接收端。
pub(super) async fn async_delete_loop(
    rt: Arc<GroupRuntime>,
    mut rx: mpsc::Receiver<AsyncDeleteBatch>,
) {
    let period = rt.stream_cfg.async_del_record_period_ms;
    if period == 0 {
        return;
    }

    let mut tick = tokio::time::interval(Duration::from_millis(period));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // interval 首跳立即完成；真实删除从配置周期后开始。

    let mut pending: HashMap<u32, Vec<String>> = HashMap::new();
    let mut pending_count = 0usize;

    loop {
        tokio::select! {
            biased;
            _ = rt.async_delete_cancel.cancelled() => {
                while let Ok(batch) = rx.try_recv() {
                    append_batch(&mut pending, &mut pending_count, batch);
                }
                flush_pending(&rt, &mut pending, &mut pending_count).await;
                if pending_count > 0 {
                    tracing::warn!(
                        group = %rt.layout.prefix,
                        pending = pending_count,
                        auto_trim_enabled = rt.stream_cfg.auto_trim_rate_ms > 0,
                        "异步 XDEL 末次 flush 未清空；entry 保留在 stream，启用 autoTrim 时由保留窗继续回收"
                    );
                }
                return;
            }
            batch = rx.recv(), if pending_count < MAX_PENDING_IDS => {
                match batch {
                    Some(batch) => append_batch(&mut pending, &mut pending_count, batch),
                    None => {
                        flush_pending(&rt, &mut pending, &mut pending_count).await;
                        return;
                    }
                }
            }
            _ = tick.tick() => {
                flush_pending(&rt, &mut pending, &mut pending_count).await;
            }
        }
    }
}

/// 业务作用：把一个已确认批次并入 owner 本地缓冲。
///
/// 达到软上限后 select 不再读取 channel；单个批次可能让计数略超过上限，但批次大小已由
/// `stream.batch_size` 控制，不会因循环继续读取而无界增长。
fn append_batch(
    pending: &mut HashMap<u32, Vec<String>>,
    pending_count: &mut usize,
    batch: AsyncDeleteBatch,
) {
    *pending_count = pending_count.saturating_add(batch.ids.len());
    pending
        .entry(batch.partition)
        .or_default()
        .extend(batch.ids);
}

/// 业务作用：尝试删除当前所有积压；失败的分区 ID 留到下一周期。
///
/// 每条命令只访问一个 stream key，因此单点和 Cluster 同样成立，不需要跨 slot pipeline。
async fn flush_pending(
    rt: &Arc<GroupRuntime>,
    pending: &mut HashMap<u32, Vec<String>>,
    pending_count: &mut usize,
) {
    if *pending_count == 0 {
        return;
    }

    let partitions: Vec<u32> = pending.keys().copied().collect();
    for partition in partitions {
        let Some(ids) = pending.get_mut(&partition) else {
            continue;
        };
        let count_before_dedup = ids.len();
        ids.sort_unstable();
        ids.dedup();
        decrement_gauge(rt, count_before_dedup.saturating_sub(ids.len()));
        let current = std::mem::take(ids);
        let mut failed = Vec::new();

        for chunk in current.chunks(XDEL_BATCH_SIZE) {
            let mut command = redis::cmd("XDEL");
            command.arg(rt.layout.stream(partition));
            for id in chunk {
                command.arg(id);
            }
            let result: std::result::Result<i64, redis::RedisError> =
                command.query_async(&mut rt.client.conn()).await;
            match result {
                Ok(_) => decrement_gauge(rt, chunk.len()),
                Err(error) => {
                    tracing::warn!(
                        partition,
                        count = chunk.len(),
                        err = %error,
                        "异步 XDEL 失败，保留到下一周期"
                    );
                    failed.extend(chunk.iter().cloned());
                }
            }
        }
        *ids = failed;
    }
    pending.retain(|_, ids| !ids.is_empty());
    *pending_count = pending.values().map(Vec::len).sum();
}

/// 业务作用：从端到端待删 gauge 扣除已成功删除或已去重的 ID 数，防御性避免计数下溢。
fn decrement_gauge(rt: &GroupRuntime, count: usize) {
    if count == 0 {
        return;
    }
    let _ = rt
        .async_delete_pending
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(count))
        });
}
