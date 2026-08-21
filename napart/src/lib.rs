//! 命名隔离、严格 FIFO，并通过盗洞完成保序任务窃取的分区任务执行器。
//!
//! # 核心价值
//!
//! 每个 [`PartitionRunner`] 是独立的有界执行域：名称、generation、slot、主 MPSC 队列、类型
//! 状态、盗洞、延迟索引、容量、指标、健康和停机互不共享。严格顺序边界是“Runner + 原始
//! 分区 + [`TaskType`]”；任务跨 slot 迁移与归还时仍使用同一受理序号，因此 stock、incremental
//! 和 staging 的物理切换不会改变 FIFO。
//!
//! 每个分区只有一个主 `SequencedMpsc` consumer。集中 observer 只向源 worker 发布
//! 窃取请求，只有源 worker 能安装盗洞和移动主队列存量。非严格类型允许多目标租约盗洞；严格
//! 类型只有一个盗洞，并分离存量、增量和归还暂存 FIFO。任何无法证明 owner、物理位置或顺序
//! 的状态都会关闭最小故障域并留下有界证据。
//!
//! # 背压、延迟与观测
//!
//! 每个 Runner 分别限制类型排队量、全局在飞量、类型状态数和目标入站盗洞数。非阻塞提交会以
//! 稳定 [`SubmitRejection`] 表示容量或控制门禁；等待型提交按固定次序取得许可并保持取消安全。
//! [`PartitionRunner::submit_after`] 登记时只占全局许可，到期后才竞争类型容量，因此登记成功的
//! delayed 任务仍可能以 [`TaskStatus::Rejected`] 结束。取消和到期重新准入只能有一个稳定终态，
//! 许可与排队投影只结算一次，拒绝指标只记录实际取得拒绝权威的路径。
//!
//! [`PartitionRunner::metrics_snapshot`] 提供 Runner 级容量、队列、timer、moving、盗洞、终态、
//! 拒绝分类与停止损耗；[`Submission::await_outcome`] 和稳定 reason 用于逐任务证明。
//!
//! # 接入方式
//!
//! 直接动态模式由业务持有一个进程级 [`PartitionRunnerRegistry`]，运行期间可以按稳定名称调用
//! [`PartitionRunnerRegistry::get_or_create`] 并显式启动。名称身份只在同一注册表内成立；为每次请求
//! 新建注册表会创建互不相干的执行域。调用方必须保留注册表或 Runner 控制句柄，并负责确定停机。
//!
//! `napp` 或 `nasa` 的 Application 受管模式在 Service UserHook 结束时冻结 YAML 与代码计划，随后
//! 于 Prepare 统一启动并发布不含启停权的业务句柄。该模式负责 readiness 与反序停机，但不在 Running
//! 阶段增加 Runner；运行期才确定执行域的业务应选择直接动态模式。
//!
//! # 生命周期
//!
//! 命名调用方显式拥有 [`PartitionRunnerRegistry`]，取得 Runner 后调用 [`PartitionRunner::start`]。
//! [`PartitionRunner::stop`] 只做无损收口，期限不足会保持 Stopping 并返回错误；只有显式
//! [`PartitionRunner::force_stop`] 才允许中止与冻结。生命周期动作由常驻 supervisor 推进，公开
//! 等待 Future 被取消不会撤销已经发布的动作。最后一个控制句柄析构只触发无等待的有损兜底，
//! 不提供退出证明；需要确定结果的调用方必须显式停止。
//!
//! [`PartitionExecutor`] 是不进入共享注册表、构造时立即启动的 standalone 兼容包装。新代码若
//! 需要多个隔离执行域，应使用命名 Runner。
//!
//! # 能力边界
//!
//! Runner 之间隔离调度状态、容量、健康和生命周期，但共享调用方提供的 Tokio runtime，不提供
//! CPU、进程内存或故障域的硬隔离。napart 也不提供跨进程接管、持久化队列、至少一次投递或进程
//! 崩溃恢复；单个严格顺序方向始终只有一个业务任务执行权。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod entry;
mod lifecycle;
mod metrics;
mod observer;
mod queue;
mod registry;
mod route;
mod runner;
mod shutdown;
mod slot;
mod timer;
mod tunnel;

use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

pub use entry::{Submission, TaskStatus};
pub use metrics::{FrozenEvidence, FrozenEvidenceSnapshot, MetricsSnapshot, RunnerMetricsSnapshot};
pub use registry::{
    PartitionRunnerRegistry, PartitionRunnerRegistryBuilder, RunnerConfig, RunnerName,
    RunnerRegistryError, StopAllEntry, StopAllReport, DEFAULT_RUNNER,
};
pub use route::{TaskOrdering, TaskSpec, TaskType};
pub use runner::{PartitionRunner, RunnerHealth, RunnerPhase, SubmitError, SubmitRejection};
pub use shutdown::{ForceStopError, ShutdownReport, StartError, StopError};

const DEFAULT_QUEUE_CAPACITY: usize = 65_536;
const DEFAULT_MAX_TYPES: usize = 4_096;
const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_STOP_TIMEOUT: Duration = Duration::from_secs(31_536_000);
static NEXT_STANDALONE_ID: AtomicU64 = AtomicU64::new(1_u64 << 63);

/// standalone 自管兼容执行器。
///
/// 本类型保持原有构造和提交入口，但内部使用完整 `PartitionRunner` slot/盗洞内核。Drop 只发布
/// 有损兜底请求；确定停机必须调用 [`PartitionExecutor::shutdown_with_report`]。
pub struct PartitionExecutor {
    runner: PartitionRunner,
    stop_timeout: Duration,
}

impl PartitionExecutor {
    /// 业务作用：按可用并行度两倍构造并立即启动 standalone Runner。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可立即提交的兼容执行器；必须在 Tokio runtime 内调用。
    pub fn new() -> Self {
        Self::with_partitions(default_partitions())
    }

    /// 业务作用：按指定分区数和默认类型队列容量构造并立即启动 standalone Runner。
    ///
    /// 参数说明：
    /// - `partitions`: 期望分区数，至少取一并规范化为 2 的幂。
    ///
    /// 返回：已启动兼容执行器。
    pub fn with_partitions(partitions: usize) -> Self {
        Self::with_partitions_and_capacity(partitions, DEFAULT_QUEUE_CAPACITY)
    }

    /// 业务作用：按指定分区数和每类型排队容量构造 standalone Runner，全局预算按每分区
    /// 一个执行位加排队容量推导。
    ///
    /// 参数说明：
    /// - `partitions`: 期望分区数。
    /// - `queue_capacity`: 每个 `(home, TaskType)` 的排队上限。
    ///
    /// 返回：已启动兼容执行器。
    pub fn with_partitions_and_capacity(partitions: usize, queue_capacity: usize) -> Self {
        let partitions = normalize_partitions(partitions);
        let capacity = queue_capacity.max(1);
        let global = partitions.saturating_mul(capacity.saturating_add(1));
        Self::with_limits(partitions, capacity, global)
    }

    /// 业务作用：完整指定分区、每类型排队容量与 Runner 全局在飞预算并立即启动。
    ///
    /// 参数说明：
    /// - `partitions`: 期望分区数。
    /// - `queue_capacity`: 每类型排队上限。
    /// - `global_inflight`: 延迟、排队、迁移和执行中任务总上限。
    ///
    /// 返回：已启动兼容执行器；超界输入按兼容合同钳制到可表示范围。
    pub fn with_limits(partitions: usize, queue_capacity: usize, global_inflight: usize) -> Self {
        let partitions = normalize_partitions(partitions);
        let queue_capacity = queue_capacity.clamp(1, u32::MAX as usize);
        let global_inflight = global_inflight.clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        let config = RunnerConfig {
            partitions,
            queue_capacity_per_type: queue_capacity,
            global_inflight,
            max_type_states: DEFAULT_MAX_TYPES.max(partitions),
            load_observer_interval: Duration::from_millis(10),
            ..RunnerConfig::default()
        }
        .validated()
        .expect("standalone partition runner config is normalized");
        let id = NEXT_STANDALONE_ID
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .expect("standalone partition runner id space is exhausted");
        let runner = PartitionRunner::standalone(id, config)
            .expect("PartitionExecutor must be constructed inside a Tokio runtime");
        Self {
            runner,
            stop_timeout: DEFAULT_STOP_TIMEOUT,
        }
    }

    /// 业务作用：设置兼容 `shutdown*` 在升级有损模式前等待无损排空的预算。
    ///
    /// 参数说明：
    /// - `timeout`: 零表示立即升级；超过一年按一年处理。
    ///
    /// 返回：链式返回执行器。
    pub fn with_stop_timeout(mut self, timeout: Duration) -> Self {
        self.stop_timeout = timeout.min(MAX_STOP_TIMEOUT);
        self
    }

    /// 业务作用：调整 standalone 当前 generation 的类型状态总上限，兼容旧构造链。
    ///
    /// 参数说明：
    /// - `max_lanes`: 至少覆盖全部分区，包含兼容保留类型在内的全部类型状态。
    ///
    /// 返回：链式返回执行器。
    pub fn with_max_lanes(self, max_lanes: usize) -> Self {
        self.runner.set_standalone_type_limit(max_lanes);
        self
    }

    /// 业务作用：非阻塞提交同原始分区保留严格类型任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区与严格顺序边界的业务键。
    /// - `task`: 受理后在受监督边界内执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；停机、容量耗尽或分区失权时返回兼容错误。
    pub fn submit<K, F, Fut>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.submit_legacy(key, task)
    }

    /// 业务作用：非阻塞提交同步短任务，顺序和拒绝语义与 `submit` 相同。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区与严格顺序边界的业务键。
    /// - `task`: 受理后执行的无阻塞同步动作。
    ///
    /// 返回：受理成功返回空值；停机、容量耗尽或分区失权时返回兼容错误。
    pub fn submit_sync<K, F>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        self.runner
            .submit_legacy(key, move || async move { task() })
    }

    /// 业务作用：取消安全地等待容量后提交保留严格类型任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区与严格顺序边界的业务键。
    /// - `task`: 容量取得后才转移给 Runner 的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；等待 Future 被取消时归还已取得许可，停机或
    /// 分区失权时返回兼容错误。
    pub async fn submit_async<K, F, Fut>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.submit_legacy_waiting(key, task).await
    }

    /// 业务作用：按显式任务类型非阻塞提交并返回权威终态句柄。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务键。
    /// - `spec`: 冻结 `TaskType` 与严格或非严格顺序语义。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：成功时返回可取消、可等待稳定终态的 `Submission`；门禁或容量拒绝时
    /// 返回稳定 `SubmitRejection`。
    pub fn submit_typed<K, F, Fut>(
        &self,
        key: K,
        spec: TaskSpec,
        task: F,
    ) -> Result<Submission, SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.submit_typed(key, spec, task)
    }

    /// 业务作用：按显式任务类型提交无需逐任务句柄的任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务键。
    /// - `spec`: 冻结任务类型和顺序语义。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；门禁或容量拒绝时返回稳定原因，后续终态只通过
    /// Runner 指标与证据观测。
    pub fn exec_typed<K, F, Fut>(
        &self,
        key: K,
        spec: TaskSpec,
        task: F,
    ) -> Result<(), SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.exec_typed(key, spec, task)
    }

    /// 业务作用：登记延迟类型任务，到期后按当时路由与容量合同受理。
    ///
    /// 参数说明：
    /// - `key`: 到期时决定原始分区的业务键。
    /// - `delay`: 任务不早于该时长进入类型队列。
    /// - `spec`: 冻结任务类型和顺序语义。
    /// - `task`: 到期且重新准入成功后执行的异步任务工厂。
    ///
    /// 返回：登记成功返回权威终态句柄；当前 Runner 未接纳或全局容量已满时返回
    /// 稳定拒绝原因。
    pub fn submit_after<K, F, Fut>(
        &self,
        key: K,
        delay: Duration,
        spec: TaskSpec,
        task: F,
    ) -> Result<Submission, SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.submit_after(key, delay, spec, task)
    }

    /// 业务作用：读取 standalone Runner 是否仍处于 Accepting。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 开放业务准入时返回 true。
    pub fn is_started(&self) -> bool {
        self.runner.phase() == RunnerPhase::Accepting
    }

    /// 业务作用：读取是否健康且没有已隔离类型或 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Runner 处于 `Healthy` 时返回 true；未启动、已停止或存在隔离时返回 false。
    pub fn is_healthy(&self) -> bool {
        self.runner.health() == RunnerHealth::Healthy
    }

    /// 业务作用：读取配置冻结的分区数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已规范化为二的幂的 slot/worker 数。
    pub fn partitions(&self) -> usize {
        self.runner.config().partitions
    }

    /// 业务作用：读取异常退出或隔离的 slot 数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前或最近 generation 的异常 worker 累计数。
    pub fn dead_partitions(&self) -> i64 {
        self.runner.metrics_snapshot().dead_workers
    }

    /// 业务作用：读取当前 generation 类型状态数量，兼容旧 `lanes` 观测名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 slot 中已创建 `(home, TaskType)` 状态数之和。
    pub fn lanes(&self) -> usize {
        self.runner.metrics_snapshot().lanes as usize
    }

    /// 业务作用：读取当前隔离失败类型数，兼容旧 `failed_lanes` 观测名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前或最近 generation 累计的失败类型数。
    pub fn failed_lanes(&self) -> u64 {
        self.runner.metrics_snapshot().failed_lanes
    }

    /// 业务作用：导出当前或最近 generation 指标快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不持有任务或调度结构引用的独立低基数快照。
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.runner.metrics_snapshot()
    }

    /// 业务作用：导出有界冻结证据最近样本，保持旧返回类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最近失败样本的独立列表，不包含业务 Future 或任务引用。
    pub fn frozen_evidence(&self) -> Vec<FrozenEvidence> {
        self.runner.frozen_evidence().recent
    }

    /// 业务作用：按兼容配置先等待无损排空，超时后显式升级为有损停止并等待退出证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务完成、取消、冻结与中止数量构成的最终损耗报告。
    pub async fn shutdown_with_report(&self) -> ShutdownReport {
        self.shutdown_with_report_timeout(self.stop_timeout).await
    }

    /// 业务作用：使用调用方预算执行兼容有损停机；任务协作让出后返回完整报告。
    ///
    /// 参数说明：
    /// - `timeout`: 无损排空预算；超过一年按一年处理，有损收口至少获得两秒协作预算。
    ///
    /// 返回：取得退出证明时返回精确报告；兼容内部预算仍不足时返回不声称已
    /// 收敛的保守指标快照。
    pub async fn shutdown_with_report_timeout(&self, timeout: Duration) -> ShutdownReport {
        let graceful_deadline = Instant::now()
            .checked_add(timeout.min(MAX_STOP_TIMEOUT))
            .unwrap_or_else(Instant::now);
        match self.runner.stop(graceful_deadline).await {
            Ok(report) => report,
            Err(_) => {
                let force_budget = timeout.max(Duration::from_secs(2));
                let force_deadline = Instant::now()
                    .checked_add(force_budget.min(MAX_STOP_TIMEOUT))
                    .unwrap_or_else(Instant::now);
                self.runner
                    .force_stop(force_deadline)
                    .await
                    .unwrap_or_else(|_| self.runner.metrics_snapshot().into())
            }
        }
    }

    /// 业务作用：执行兼容停机并丢弃损耗报告；需要审计时应调用 `shutdown_with_report`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；在兼容预算内完成无损或显式有损收口。
    pub async fn shutdown(&self) {
        let _ = self.shutdown_with_report().await;
    }
}

impl Default for PartitionExecutor {
    /// 业务作用：委托 `new` 创建默认 standalone Runner。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已立即启动的兼容执行器。
    fn default() -> Self {
        Self::new()
    }
}

impl From<RunnerMetricsSnapshot> for ShutdownReport {
    /// 业务作用：停止未在兼容内部兜底预算内收敛时生成保守损耗快照，不声称取得退出证明。
    ///
    /// 参数说明：
    /// - `snapshot`: 当前或最近 generation 的指标投影。
    ///
    /// 返回：以全部分区尚未收敛为前提的保守 `ShutdownReport`。
    fn from(snapshot: RunnerMetricsSnapshot) -> Self {
        Self {
            aborted: snapshot.aborted,
            frozen: snapshot.frozen,
            unconverged_slots: snapshot.partitions as u32,
            ..Self::default()
        }
    }
}

/// 业务作用：把外部回调、Waker 或业务载荷析构的单次展开隔离在当前任务边界。
///
/// 参数说明：
/// - `context`: 仅用于控制面日志的稳定上下文。
/// - `action`: 不可信边界动作。
///
/// 返回：无；展开载荷在第二层边界内析构，不能越过框架状态提交。
pub(crate) fn run_isolated(context: &str, action: impl FnOnce()) {
    let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) else {
        return;
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        tracing::error!(context, "napart 隔离了外部回调展开");
        drop(payload);
    }));
}

struct ShieldWake {
    downstream: Mutex<Option<Waker>>,
}

impl ShieldWake {
    /// 业务作用：持有调用方 Waker 的隔离副本，使内部同步原语只接触框架控制的唤醒对象。
    ///
    /// 参数说明：
    /// - `downstream`: 本次 poll 由调用方提供的真实 Waker。
    ///
    /// 返回：析构与唤醒均受隔离边界保护的新对象。
    fn new(downstream: Waker) -> Arc<Self> {
        Arc::new(Self {
            downstream: Mutex::new(Some(downstream)),
        })
    }
}

impl Wake for ShieldWake {
    /// 业务作用：消费式唤醒调用方 Future，并把回调或真实 Waker 析构展开限制在当前边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；调用方 Waker 最多消费一次，异常不会截断同一内部同步原语的其它等待者。
    fn wake(self: Arc<Self>) {
        let downstream = self
            .downstream
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        run_isolated("外部 Waker 消费式唤醒", move || {
            if let Some(downstream) = downstream {
                downstream.wake();
            }
        });
    }

    /// 业务作用：借用式唤醒调用方 Future，保留真实 Waker 给后续通知或隔离析构。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；回调展开被吸收，同批其它等待者仍可继续接收通知。
    fn wake_by_ref(self: &Arc<Self>) {
        let downstream = self.downstream.lock().unwrap_or_else(|e| e.into_inner());
        run_isolated("外部 Waker 借用式唤醒", || {
            if let Some(downstream) = downstream.as_ref() {
                downstream.wake_by_ref();
            }
        });
    }
}

impl Drop for ShieldWake {
    /// 业务作用：内部等待点撤销或替换 Waker 时隔离真实 Waker 的析构逻辑，防止批量通知
    /// 因一个调用方异常而中断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；尚未消费的真实 Waker 在独立展开边界内释放。
    fn drop(&mut self) {
        let downstream = self
            .downstream
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        run_isolated("外部 Waker 隔离析构", move || drop(downstream));
    }
}

/// 业务作用：用框架 Waker 轮询可能把调用方唤醒对象登记到内部同步原语的 Future，隔离
/// 调用方 wake、wake_by_ref 和 Waker 析构展开。
///
/// 参数说明：
/// - `future`: 需要跨内部 Notify、Semaphore 或其它等待点推进的 Future。
///
/// 返回：底层 Future 的原始输出；调用方 Waker 无法安全克隆时使用 noop Waker 保持框架边界。
pub(crate) async fn shield_future<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(move |context| {
        let downstream = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            context.waker().clone()
        })) {
            Ok(waker) => waker,
            Err(payload) => {
                run_isolated("外部 Waker 克隆", move || drop(payload));
                Waker::noop().clone()
            }
        };
        let shield = Waker::from(ShieldWake::new(downstream));
        let mut context = Context::from_waker(&shield);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => Poll::Ready(output),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// 业务作用：计算默认分区数，使用可用并行度两倍并规范化为 2 的幂。
///
/// 参数说明: 无。
///
/// 返回：可用并行度两倍向上规范化后的二的幂；超过实现边界时钳制为 65536。
fn default_partitions() -> usize {
    let parallelism = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    normalize_partitions(parallelism.saturating_mul(2))
}

/// 业务作用：把兼容输入钳制到 Runner 支持的非零 2 的幂分区范围。
///
/// 参数说明：
/// - `partitions`: 调用方期望的分区数，零按一处理。
///
/// 返回：向上规范化且不超过 65536 的二的幂。
fn normalize_partitions(partitions: usize) -> usize {
    partitions
        .max(1)
        .checked_next_power_of_two()
        .unwrap_or(65_536)
        .min(65_536)
}
