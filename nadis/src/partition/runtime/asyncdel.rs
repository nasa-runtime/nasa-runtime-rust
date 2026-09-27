// ============================================================================
// ACK 后异步 XDEL：单 owner、有界通道、按分区批量回收 stream entry。
//
// XACK 决定消费终态，XDEL 只回收日志空间。两者不能绑成一个事务：删除失败不应把已经成功
// 处理的业务消息重新投递。发送端受 inflight 预算和有界 channel 双重约束；Redis 暂时失败时
// 保留 ID 到下一周期重试，停机在 coordinator 排空后做一次最终 flush；退出时未完成 ID
// 转为 DeleteRetained 诊断终态，Stream 正文交由保留窗或 auto trim 回收。
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

struct AsyncDeleteOwner {
    rt: Arc<GroupRuntime>,
    rx: mpsc::Receiver<AsyncDeleteBatch>,
}

impl Drop for AsyncDeleteOwner {
    /// 业务作用：删除 owner 的全部退出路径收口已受理责任，不撤销 ACK 或重新执行 handler。
    /// 参数说明: 无。
    /// 返回：关闭交接后把剩余计数转为 DeleteRetained；普通退出、展开和中止均归还共享容量。
    fn drop(&mut self) {
        let mut counts = self
            .rt
            .async_delete_admission
            .lock()
            .expect("delete admission");
        // 先关闭新交接，再迁移全部已受理 ID；同步发送方不得在此窗口登记后又自行扣回预留。
        self.rx.close();
        let retained = self.rt.async_delete_pending.swap(0, Ordering::AcqRel);
        for (partition, count) in counts.iter_mut().enumerate() {
            let domain = self
                .rt
                .core
                .executions
                .domain(&self.rt.layout.prefix, partition as u32);
            self.rt.core.executions.domains[domain]
                .delete_records
                .fetch_sub(*count, Ordering::AcqRel);
            *count = 0;
        }
        self.rt
            .core
            .async_delete_records
            .fetch_sub(retained, Ordering::AcqRel);
        self.rt
            .core
            .delete_retained_records
            .fetch_add(retained as u64, Ordering::AcqRel);
        self.rt.core.changed.notify_waiters();
    }
}

/// 业务作用：运行一个分区组的异步删除 owner。
/// 参数说明：`rt` 为当前分区组；`rx` 为 ACK 成功批次的唯一接收端。
/// 返回：预先持有退出责任的 Future，即使首次轮询前被丢弃也会完成计数移交。
pub(super) fn async_delete_loop(
    rt: Arc<GroupRuntime>,
    rx: mpsc::Receiver<AsyncDeleteBatch>,
) -> impl std::future::Future<Output = ()> {
    run_async_delete(AsyncDeleteOwner { rt, rx })
}

/// 业务作用：串行归并删除批次，正常停机执行末次删除尝试。
/// 参数说明：`owner` 持有唯一接收端与退出计数责任。
/// 返回：成功删除清除 pending；未删除正文由 owner 析构转为留存终态。
async fn run_async_delete(mut owner: AsyncDeleteOwner) {
    let rt = owner.rt.clone();
    let rx = &mut owner.rx;
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
            _ = rt.async_delete_flush.notified() => {
                while let Ok(batch) = rx.try_recv() {
                    append_batch(&mut pending, &mut pending_count, batch);
                }
                flush_pending(&rt, &mut pending, &mut pending_count).await;
                if pending_count > 0 && rt.core.roots_closed.load(Ordering::Acquire) {
                    // 排干中的 Commit 可能正在等待删除容量；末次失败先关闭删除交接，避免互相等待退出。
                    return;
                }
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
        decrement_gauge(rt, partition, count_before_dedup.saturating_sub(ids.len()));
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
                Ok(_) => decrement_gauge(rt, partition, chunk.len()),
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

/// 业务作用：删除完成后同时归还物理分区、执行域和源级删除份额。
/// 参数说明：`rt` 为组；`partition` 为已处理的物理分区；`count` 为删除或去重的数量。
/// 返回：交接屏障内清除责任，其他执行域的删除份额不受影响。
fn decrement_gauge(rt: &GroupRuntime, partition: u32, count: usize) {
    if count == 0 {
        return;
    }
    let mut counts = rt.async_delete_admission.lock().expect("delete admission");
    let count = count.min(counts[partition as usize]);
    counts[partition as usize] -= count;
    let domain = rt.core.executions.domain(&rt.layout.prefix, partition);
    rt.core.executions.domains[domain]
        .delete_records
        .fetch_sub(count, Ordering::AcqRel);
    let _ = rt
        .async_delete_pending
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(count))
        });
    let _ =
        rt.core
            .async_delete_records
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(count))
            });
}
