//! ACK 后通过唯一 owner、有界通道和分区批次回收 Stream 正文。
//!
//! XACK 确认消费结果，XDEL 只回收存储，删除失败不会撤销成功结果或重新调用业务。
//! 发送端同时受共享删除预算与通道容量约束，失败 ID 保留到后续周期重试。
//! 正常停机在协调器排空后执行末次尝试；退出时未完成责任转为留存计数，正文可由保留策略继续回收。

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
    ///
    /// 参数说明: 无。
    ///
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
///
/// 参数说明：
/// - `rt`: 持有删除预算、物理布局与停止控制的消费组运行时。
/// - `rx`: 已确认批次的唯一接收端，发送方已在交接屏障内登记对应删除责任。
///
/// 返回：预先持有唯一退出守卫的 Future；首次轮询前被丢弃、展开或中止仍会关闭接收并归还尚未完成的删除预算。
pub(super) fn async_delete_loop(
    rt: Arc<GroupRuntime>,
    rx: mpsc::Receiver<AsyncDeleteBatch>,
) -> impl std::future::Future<Output = ()> {
    run_async_delete(AsyncDeleteOwner {
        rt,
        rx,
    })
}

/// 业务作用：串行归并删除批次，正常停机执行末次删除尝试。
///
/// 参数说明：
/// - `owner`: 持有唯一接收端、所属运行时和全部已受理删除预算的退出守卫。
///
/// 返回：周期为零时立即结束；正常停止或通道关闭时先完成末次删除尝试再退出。
/// 排干通知后仍失败则关闭交接，未完成责任由守卫归还预算并计入留存；这些退出不撤销已经确认的业务结果。
async fn run_async_delete(mut owner: AsyncDeleteOwner) {
    let rt = owner.rt.clone();
    let rx = &mut owner.rx;
    let period = rt.stream_cfg.async_del_record_period_ms;
    // 未启用周期删除时不能留下无人消费的接收端；退出守卫负责拒绝后续交接并收口已有预算。
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
                // 正常停止在协调器退出后请求末次尝试；强停可中止等待，守卫始终负责收口剩余预算。
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
                        // 唯一通道关闭后不会有新交接，当前缓冲仍应获得最后一次删除机会。
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
/// 参数说明：
/// - `pending`: 按物理分区归并的待删除 ID，唯一删除 owner 持有其修改权。
/// - `pending_count`: 本地缓冲的责任计数，包含尚未去重的重复 ID。
/// - `batch`: 已在发送端占用分区、执行域和源级预算的已确认批次。
///
/// 返回：移入整批 ID 并更新本地计数，不重新申请或释放共享预算。软上限只限制常规接收，
/// 主动 flush 可接过更多已受理批次，但总量仍由发送端的共享删除预算约束。
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
/// 参数说明：
/// - `rt`: 提供各物理 Stream、Redis 连接和共享删除预算的消费组。
/// - `pending`: 唯一 owner 持有的分区待删除 ID，已取得业务 ACK 事实。
/// - `pending_count`: 本地剩余 ID 总数，在本轮结束时按保留集合重新计算。
///
/// 返回：先去重并归还重复责任，再按单 Stream、有界 ID 数量发送 XDEL；删除数在零到请求 ID 数以内时归还该批全部责任。
/// Redis 错误或越界删除数保留整批待重试；中止时尚未归还的预算由 owner 退出守卫收口，不重新投递已确认业务。
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
        // 相同物理 ID 只需一次删除，但每次交接均占用了预算；先归还重复占用，避免重试越积越多。
        decrement_gauge(rt, partition, count_before_dedup.saturating_sub(ids.len()));
        let current = std::mem::take(ids);
        let mut failed = Vec::new();

        for chunk in current.chunks(XDEL_BATCH_SIZE) {
            let mut command = redis::cmd("XDEL");
            command.arg(rt.layout.stream(partition));
            for id in chunk {
                command.arg(id);
            }
            let result = command
                .query_async::<i64>(&mut rt.client.conn())
                .await
                .and_then(|deleted| {
                    // 不存在的 ID 可返回零，但不可能删除负数或超过请求数；异常证据不能解除重试责任。
                    if usize::try_from(deleted).is_ok_and(|count| count <= chunk.len()) {
                        Ok(deleted)
                    } else {
                        Err(redis::RedisError::from((
                            redis::ErrorKind::Parse,
                            "XDEL returned an invalid deleted count",
                        )))
                    }
                });
            match result {
                Ok(_) => decrement_gauge(rt, partition, chunk.len()),
                Err(error) => {
                    // 网络失败可能已在远端完成删除；保留精确 ID 重试，XDEL 幂等且不改变 ACK 成功事实。
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
///
/// 参数说明：
/// - `rt`: 持有分区计数、执行域映射和全源删除预算的消费组。
/// - `partition`: 本次完成删除或去重的有效物理分区编号。
/// - `count`: 可解除的交接责任数，不是服务端实际删除条目数；已不存在的 ID 也可完成责任。
///
/// 返回：零数量不操作；其余在交接屏障内按本分区尚存计数归还各级份额，不扣减其它分区责任。
/// 本函数不执行 Redis 命令，也不改变业务 ACK 状态。
fn decrement_gauge(rt: &GroupRuntime, partition: u32, count: usize) {
    if count == 0 {
        return;
    }
    let mut counts = rt.async_delete_admission.lock().expect("delete admission");
    // 退出守卫与完成回包共用交接屏障，只能归还此分区仍持有的责任，不能借重复完成扣减其它来源。
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
