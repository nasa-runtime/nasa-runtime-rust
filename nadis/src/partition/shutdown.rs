//! 停机等待者与停机操作分离，期限只限制等待，不撤销排干责任。

use super::{
    publisher::{PublisherCoordinator, PublisherSnapshot},
    runtime::{engine::RedisPartitionRuntime, GroupRuntime, PartitionSnapshot},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::Notify;

/// 停机操作的当前证明与剩余责任；不收敛报告可继续等待同一操作。
#[derive(Debug, Clone)]
pub struct PartitionShutdownReport {
    pub converged: bool,
    pub forced: bool,
    pub local_execution_uncertain: bool,
    pub remaining: PartitionSnapshot,
    pub publishes: PublisherSnapshot,
    pub async_delete_pending: usize,
    pub background_tasks: usize,
}

pub(super) struct ShutdownOperation {
    pub core: Arc<RedisPartitionRuntime>,
    pub publisher: Arc<PublisherCoordinator>,
    groups: Vec<Arc<GroupRuntime>>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    started: AtomicBool,
    done: AtomicBool,
    forced: AtomicBool,
    changed: Notify,
}

impl ShutdownOperation {
    /// 业务作用：为运行句柄冻结一次性停机资源集合。
    /// 参数说明：`core` 为共享消费运行时；`publisher` 为发布 owner；`groups` 为全部组。
    /// 返回：尚未请求停止的单一操作。
    pub(super) fn new(
        core: Arc<RedisPartitionRuntime>,
        publisher: Arc<PublisherCoordinator>,
        groups: Vec<Arc<GroupRuntime>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            core,
            publisher,
            groups,
            handle: Mutex::new(None),
            started: AtomicBool::new(false),
            done: AtomicBool::new(false),
            forced: AtomicBool::new(false),
            changed: Notify::new(),
        })
    }

    /// 业务作用：关闭根准入并登记唯一排干任务，调用方取消不会取消该操作。
    /// 参数说明: 无。
    /// 返回：已有操作被复用，新操作继续负责全部资源收口。
    pub(super) fn begin(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        // 根准入先关闭，已登记业务仍可发布 Commit 后继。
        self.publisher.close();
        self.core.close_roots();
        let owner = self.clone();
        let handle = tokio::spawn(async move {
            owner.publisher.drain().await;
            futures::future::join_all(owner.groups.iter().map(|group| group.shutdown())).await;
            owner.core.stop().await;
            owner.done.store(true, Ordering::Release);
            owner.changed.notify_waiters();
        });
        *self.handle.lock().expect("shutdown operation") = Some(handle);
    }

    /// 业务作用：等待同一排干操作，超时不终止续租或提前解锁。
    /// 参数说明：`deadline` 为本次等待绝对期限。
    /// 返回：真实退出时 converged；期限到达时报告剩余责任，后台继续收口。
    pub(super) async fn wait(self: &Arc<Self>, deadline: Instant) -> PartitionShutdownReport {
        self.begin();
        loop {
            let changed = self.changed.notified();
            if self.done.load(Ordering::Acquire) {
                break;
            }
            if tokio::time::timeout_at(deadline.into(), changed)
                .await
                .is_err()
            {
                break;
            }
        }
        self.report()
    }

    /// 业务作用：显式强停先撤销来源权威，再中止受监督任务，缺少 join 证明时禁止主动解锁。
    /// 参数说明：`deadline` 为强停等待期限。
    /// 返回：forced 报告；未取得退出证明时标记本地执行不确定。
    pub(super) async fn force(self: &Arc<Self>, deadline: Instant) -> PartitionShutdownReport {
        self.forced.store(true, Ordering::Release);
        self.publisher.force();
        self.core.revoke_all();
        self.begin();
        for group in &self.groups {
            group.cancel_best_effort();
        }
        // 所有域同时收到有损停止请求，单个慢域不能延迟其他域的撤权与 join。
        futures::future::join_all(
            self.core
                .executions
                .domains
                .iter()
                .map(|domain| domain.runner.force_stop(deadline)),
        )
        .await;
        self.wait(deadline).await
    }

    /// 业务作用：读取退出证明和未完成责任，避免把取消请求误报为完整排干。
    /// 参数说明: 无。
    /// 返回：分别采样消费、发布、删除与后台状态；只有停机操作完成且全部剩余责任为零才报告收敛。
    fn report(&self) -> PartitionShutdownReport {
        let finished = self.done.load(Ordering::Acquire);
        let remaining = self.core.snapshot();
        let publishes = self.publisher.snapshot();
        let async_delete_pending = self.groups.iter().map(|g| g.async_delete_pending()).sum();
        let background_tasks = self
            .groups
            .iter()
            .map(|group| group.unfinished_background_tasks())
            .sum();
        // 控制任务退出不能替代责任清空证明，异常退出留下的记录、I/O 或锁仍必须报告未收敛。
        let converged = finished
            && remaining.inflight_records == 0
            && remaining.inflight_tasks == 0
            && remaining.payload_bytes == 0
            && remaining.active_batches == 0
            && remaining.ordered_keys == 0
            && remaining.source_io == 0
            && remaining.unjoined_readers == 0
            && remaining.unconfirmed_locks == 0
            && remaining
                .execution_domains
                .iter()
                .all(|domain| domain.runner_stopped)
            && publishes.inflight == 0
            && publishes.unjoined_tasks == 0
            && async_delete_pending == 0
            && background_tasks == 0;
        PartitionShutdownReport {
            converged,
            forced: self.forced.load(Ordering::Acquire),
            local_execution_uncertain: !converged && self.forced.load(Ordering::Acquire),
            remaining,
            publishes,
            async_delete_pending,
            background_tasks,
        }
    }
}
