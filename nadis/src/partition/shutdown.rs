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
    /// 停机操作已完成，且消费、发布、删除、锁与后台任务均取得收口证明。
    pub converged: bool,
    /// 是否曾请求显式强停；此标志本身不代表任务已退出。
    pub forced: bool,
    /// 已请求强停但尚未收敛，不能证明本地执行已全部结束。
    pub local_execution_uncertain: bool,
    /// 消费侧剩余责任与执行域状态的当前采样。
    pub remaining: PartitionSnapshot,
    /// 发布侧在途责任与累计结果的当前采样。
    pub publishes: PublisherSnapshot,
    /// 仍由各消费组持有的异步删除责任数。
    pub async_delete_pending: usize,
    /// 各消费组尚未结束的后台任务数。
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
    dependency: Mutex<(bool, Option<Box<dyn Send>>)>,
}

/// 保留排干任务的启动责任，区分尚未进入收口与已经可能产生副作用的退出。
struct ShutdownTask {
    /// 即使运行句柄被释放，排干任务也必须继续拥有同一停机操作。
    operation: Arc<ShutdownOperation>,
    /// 首次轮询前为 false；进入收口后不能因任务异常而盲目重放部分清理。
    entered: bool,
}

impl Drop for ShutdownTask {
    /// 业务作用：归还尚未进入收口的启动资格，并让等待者在任务任何退出路径上重新观察完成状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务从未被轮询时允许后续在可用执行器上重试；已进入收口则保留原操作状态，
    /// 不把取消或展开当作完成证明，依赖析构展开也不会吞掉已经发布的完成通知。
    fn drop(&mut self) {
        // 首次轮询前不存在组清理副作用，执行器拒收不能永久消耗唯一启动资格。
        if !self.entered {
            self.operation.started.store(false, Ordering::Release);
        }
        self.operation.changed.notify_waiters();
    }
}

impl ShutdownOperation {
    /// 业务作用：为运行句柄冻结一次性停机资源集合。
    ///
    /// 参数说明：
    /// - `core`: 共享消费执行与责任账本，全部组排干后由本操作停止。
    /// - `publisher`: 同一来源的发布协调器，新准入关闭后仍需等待已登记票据终结。
    /// - `groups`: 当前运行态的全部消费组，停机时共同承担读取、续租、删除与锁收口。
    ///
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
            dependency: Mutex::new((false, None)),
        })
    }

    /// 业务作用：关闭根准入并登记唯一排干任务，调用方取消不会取消该操作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已有操作被复用；无 Tokio 上下文时只关闭准入，后续调用可在可用执行器上继续启动。
    /// 执行器在首次轮询前拒收任务时归还启动资格，已进入收口的任务异常则保留未完成责任，不盲目重放清理。
    pub(super) fn begin(self: &Arc<Self>) {
        // 根准入先关闭，已登记业务仍可发布 Commit 后继。
        self.publisher.close();
        self.core.close_roots();
        // 没有执行上下文时不能消耗唯一启动资格；同步关闭准入仍生效，等待者可稍后继续同一操作。
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut registered = self.handle.lock().expect("shutdown operation");
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let startup = ShutdownTask {
            operation: self.clone(),
            entered: false,
        };
        // 句柄登记与启动资格共用锁，防止同步拒收释放资格后，新调用的句柄被旧调用覆盖。
        let handle = runtime.spawn(async move {
            let mut task = startup;
            task.entered = true;
            let owner = &task.operation;
            owner.publisher.drain().await;
            futures::future::join_all(owner.groups.iter().map(|group| group.shutdown())).await;
            owner.core.stop().await;
            owner.done.store(true, Ordering::Release);
            // 只有完整排干之后才释放宿主依赖；超时等待者退出不会让数据库或 Redis 提前关闭。
            if owner.report().converged {
                let dependency = owner
                    .dependency
                    .lock()
                    .expect("shutdown dependency")
                    .1
                    .take();
                drop(dependency);
            }
            owner.changed.notify_waiters();
        });
        *registered = Some(handle);
    }

    /// 业务作用：原子登记一次宿主依赖保留权，避免完成与登记竞争造成永久保留。
    ///
    /// 参数说明：
    /// - `guard`: 宿主提供的依赖守卫，其析构代表允许释放数据库或其它下游资源。
    ///
    /// 返回：成功接管或在完成后释放；重复登记返回原值。
    pub(super) fn retain_dependency<T: Send + 'static>(
        &self,
        guard: T,
    ) -> std::result::Result<(), T> {
        let mut dependency = self.dependency.lock().expect("shutdown dependency");
        // 只允许一位宿主登记依赖，重复调用不能替换仍受排干责任保护的原守卫。
        if dependency.0 {
            return Err(guard);
        }
        dependency.0 = true;
        if self.report().converged {
            drop(dependency);
            drop(guard);
        } else {
            dependency.1 = Some(Box::new(guard));
        }
        Ok(())
    }

    /// 业务作用：等待同一排干操作，超时不终止续租或提前解锁。
    ///
    /// 参数说明：
    /// - `deadline`: 当前等待者的单调时钟绝对截止，不改变已登记停机操作的生命周期。
    ///
    /// 返回：操作结束或等待到期后返回当前报告；只有实际责任全部清空才标记 converged，
    /// 超时或调用方取消等待不会中止已经派发的后台排干任务。
    pub(super) async fn wait(self: &Arc<Self>, deadline: Instant) -> PartitionShutdownReport {
        self.begin();
        loop {
            // 先创建通知再读取完成标志；notify_waiters 的代次会覆盖通知创建后尚未轮询的窗口。
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
    ///
    /// 参数说明：
    /// - `deadline`: 所有执行域强停与本次停机等待共用的单调时钟绝对截止。
    ///
    /// 返回：包含 forced 标记的当前报告；未取得完整退出证明时标记本地执行不确定，
    /// 本地中止不能撤销 Redis 中可能已经完成的命令。
    pub(super) async fn force(self: &Arc<Self>, deadline: Instant) -> PartitionShutdownReport {
        // 先公布强停并撤销发布和消费权威，再请求任务停止，避免取消期间仍接纳新的业务责任。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分别采样消费、发布、删除与后台状态；只有停机操作完成且全部剩余责任为零才报告收敛。
    pub(super) fn report(&self) -> PartitionShutdownReport {
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
