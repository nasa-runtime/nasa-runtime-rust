//! 命名 Runner、generation 数据面与生命周期 supervisor。

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};

use crate::entry::{
    EntryState, EnvelopeBinding, Job, LogicalContainer, LogicalOwner, MoveStage, Owner, Submission,
    TaskAuthorityError, TaskEnvelope, TaskStatus,
};
use crate::lifecycle::{
    ChildTaskKind, GenerationControl, LifecycleMode, RegisterError, SupervisorStartError,
};
use crate::metrics::{
    FrozenEvidence, FrozenEvidenceRing, FrozenEvidenceSnapshot, RunnerMetrics,
    RunnerMetricsSnapshot,
};
use crate::observer::{self, StealRequest};
use crate::queue::{Reservation, ReserveError};
use crate::registry::{RunnerConfig, RunnerName};
use crate::route::{
    StrictExecutionLease, StrictHandlingLease, StrictRouteState, TaskOrdering, TaskSpec, TypeState,
    LEGACY_TYPE,
};
use crate::shutdown::{ForceStopError, ShutdownReport, StartError, StopError};
use crate::slot::{self, PartitionSlot, SlotConsumers};
use crate::timer::RunnerTimer;
use crate::tunnel::{NonStrictTunnel, StrictQueue, StrictTunnel};

const PHASE_STARTING: u8 = 0;
const PHASE_ACCEPTING: u8 = 1;
const PHASE_STOPPING: u8 = 2;
const PHASE_STOPPED: u8 = 3;
const PHASE_FAILED: u8 = 4;

/// Runner 生命周期阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerPhase {
    /// 正在建立 supervisor、slot、timer 和 observer，尚未发布业务入口。
    Starting,
    /// 当前 generation 接受新提交。
    Accepting,
    /// 已关闭新提交，supervisor 正在取得全部退出证明。
    Stopping,
    /// 当前没有活动 generation，可以重新启动。
    Stopped,
    /// 控制面已失去安全推进条件且资源已经完成收口。
    Failed,
}

impl Default for RunnerPhase {
    /// 业务作用：为尚未启动的指标快照提供保守生命周期初值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未建立 generation 时使用的 `Stopped` 阶段。
    fn default() -> Self {
        Self::Stopped
    }
}

/// Runner 健康状态；该值用于 readiness，不是进程 liveness 探针。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerHealth {
    /// 尚未启动或已经完整停止。
    Stopped,
    /// 正在建立当前 generation。
    Starting,
    /// 接受任务且没有已隔离类型或 slot。
    Healthy,
    /// 尚有可接纳 slot，但至少一个类型或 slot 已被隔离。
    Degraded,
    /// 数据入口已经关闭，或控制面失去唯一权威。
    Failed,
    /// 正常停止正在收口且没有致命失败。
    Stopping,
}

impl Default for RunnerHealth {
    /// 业务作用：为尚未启动的指标快照提供保守健康初值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Stopped。
    fn default() -> Self {
        Self::Stopped
    }
}

/// 非阻塞兼容提交被拒的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// Runner 正在停止或尚未启动。
    ShuttingDown,
    /// 目标 slot 或类型失去安全执行条件。
    PartitionDead,
    /// 类型排队预算或 Runner 全局预算当前已满。
    QueueFull,
}

impl fmt::Display for SubmitError {
    /// 业务作用：把兼容提交拒绝格式化为稳定文本。
    ///
    /// 参数说明：
    /// - `f`: 接收错误文本的格式化目标。
    ///
    /// 返回：错误文本写入成功时返回 `Ok`，底层格式化目标拒绝写入时返回对应错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::ShuttingDown => "partition runner is shutting down",
            Self::PartitionDead => "target partition is unavailable",
            Self::QueueFull => "target partition queue is full",
        };
        f.write_str(text)
    }
}

impl std::error::Error for SubmitError {}

/// 类型化提交被拒的稳定原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubmitRejection {
    /// Runner 未启动、正在停止或已停止。
    ShuttingDown,
    /// 固定 `(home, TaskType)` 的排队许可耗尽。
    QueueFull,
    /// Runner 全局在飞许可耗尽。
    Overloaded,
    /// 已存在类型状态的顺序要求与本次提交不同。
    OrderingConflict,
    /// 目标类型或 slot 已隔离失败。
    LaneFailed,
    /// 当前 generation 的类型状态总上限已经耗尽。
    LaneLimitExceeded,
    /// 类型化入口使用了 standalone 兼容保留值。
    ReservedTaskType,
}

impl fmt::Display for SubmitRejection {
    /// 业务作用：把类型化拒绝格式化为稳定文本，供重试和熔断策略分类。
    ///
    /// 参数说明：
    /// - `f`: 接收错误文本的格式化目标。
    ///
    /// 返回：错误文本写入成功时返回 `Ok`，底层格式化目标拒绝写入时返回对应错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::ShuttingDown => "partition runner is shutting down",
            Self::QueueFull => "task type queue is full",
            Self::Overloaded => "partition runner global budget is exhausted",
            Self::OrderingConflict => "task type ordering conflicts with its frozen definition",
            Self::LaneFailed => "task type or partition slot has failed",
            Self::LaneLimitExceeded => "partition runner type state limit is exhausted",
            Self::ReservedTaskType => "task type value is reserved",
        };
        f.write_str(text)
    }
}

impl std::error::Error for SubmitRejection {}

/// 业务作用：把类型化拒绝投影到兼容三值错误，不丢失容量与持久失权的处理差异。
///
/// 参数说明：
/// - `error`: 类型化入口产生的稳定拒绝原因。
///
/// 返回：供兼容入口消费的停机、容量或分区不可用错误。
fn legacy_rejection(error: SubmitRejection) -> SubmitError {
    match error {
        SubmitRejection::ShuttingDown => SubmitError::ShuttingDown,
        SubmitRejection::QueueFull | SubmitRejection::Overloaded => SubmitError::QueueFull,
        SubmitRejection::OrderingConflict
        | SubmitRejection::LaneFailed
        | SubmitRejection::LaneLimitExceeded
        | SubmitRejection::ReservedTaskType => SubmitError::PartitionDead,
    }
}

/// 业务作用：冻结停止动作开始时的终态基线，用于生成本次停机而非进程累计的报告。
struct StopBaseline {
    completed: u64,
    cancelled: u64,
    aborted: u64,
    frozen: u64,
}

/// 业务作用：让并发停止调用共享唯一动作、强制升级信号与最终停机报告。
struct StopOperation {
    started: AtomicBool,
    force_requested: AtomicBool,
    baseline: OnceLock<StopBaseline>,
    result: Mutex<Option<ShutdownReport>>,
    done: Notify,
}

/// 业务作用：绑定延迟索引所需的任务权威、路由哈希、执行规格与信封身份。
struct DelayedTask {
    entry: Arc<crate::entry::TaskEntry<Job>>,
    hash: u64,
    spec: TaskSpec,
    envelope: EnvelopeBinding,
}

impl StopOperation {
    /// 业务作用：创建尚未发布停止动作的共享 operation，供全部公开等待者复用同一终局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：初始状态未启动、未请求强制收口且尚无报告的共享 operation。
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: AtomicBool::new(false),
            force_requested: AtomicBool::new(false),
            baseline: OnceLock::new(),
            result: Mutex::new(None),
            done: Notify::new(),
        })
    }

    /// 业务作用：读取已经发布的稳定停机报告。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：报告已发布时返回副本，否则返回 `None`。
    fn result(&self) -> Option<ShutdownReport> {
        *self.result.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 业务作用：只发布一次停机报告，并唤醒所有取消安全等待者。
    ///
    /// 参数说明：
    /// - `report`: supervisor 取得完整收口证明后生成的终局报告。
    ///
    /// 返回：无；首次调用发布报告并唤醒等待者，后续调用保持原有终局不变。
    fn finish(&self, report: ShutdownReport) {
        let mut result = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if result.is_none() {
            *result = Some(report);
            drop(result);
            self.done.notify_waiters();
        }
    }
}

/// 业务作用：保存当前与最近 generation，支持重启切换和终态报告查询使用同一权威。
struct ControlState {
    current: Option<Arc<RunnerInner>>,
    last: Option<Arc<RunnerInner>>,
}

/// 业务作用：拥有命名 Runner 的 generation 切换、配置与跨 generation 停止控制。
pub(crate) struct RunnerControl {
    id: u64,
    name: RunnerName,
    config: Arc<RunnerConfig>,
    next_generation: AtomicU64,
    state: Mutex<ControlState>,
}

impl Drop for RunnerControl {
    /// 业务作用：最后一个外部控制句柄离场时同步关闭入口并升级为有损收口；不等待 join，
    /// 仍存活的 supervisor 在 runtime 可调度时继续取得退出证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；若仍有活动 generation，则同步关闭准入并请求有损收口。
    fn drop(&mut self) {
        if let Some(inner) = self
            .state
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .current
            .as_ref()
        {
            inner.request_force_stop("runner_control_dropped");
        }
    }
}

/// 可克隆的命名 Runner 控制句柄。
#[derive(Clone)]
pub struct PartitionRunner {
    control: Arc<RunnerControl>,
}

impl fmt::Debug for PartitionRunner {
    /// 业务作用：输出命名执行域的稳定身份与生命周期快照，支持调用方直接记录或断言
    /// `Result<PartitionRunner, RunnerRegistryError>`，同时避免泄露完整容量配置和内部句柄。
    ///
    /// 参数说明：
    /// - `f`: 接收安全诊断字段的格式化器。
    ///
    /// 返回：名称、代次和阶段写入成功时返回 Ok；底层格式化目标拒绝写入时返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (epoch, phase) = {
            let state = self
                .control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state
                .current
                .as_ref()
                .or(state.last.as_ref())
                .map_or((0, RunnerPhase::Stopped), |inner| {
                    (inner.generation, inner.phase())
                })
        };
        f.debug_struct("PartitionRunner")
            .field("name", self.name())
            .field("epoch", &epoch)
            .field("phase", &phase)
            .finish_non_exhaustive()
    }
}

impl PartitionRunner {
    /// 业务作用：由显式注册表创建冻结名称、配置与不可复用 ID 的控制对象。
    ///
    /// 参数说明：
    /// - `id`: 注册表为 Runner 分配且进程内不复用的身份。
    /// - `name`: 已通过命名规则校验的隔离域名称。
    /// - `config`: 已规范化并通过有界校验的冻结配置。
    ///
    /// 返回：尚未启动 generation 的命名 Runner 句柄。
    pub(crate) fn registered(id: u64, name: RunnerName, config: RunnerConfig) -> Self {
        Self {
            control: Arc::new(RunnerControl {
                id,
                name,
                config: Arc::new(config),
                next_generation: AtomicU64::new(1),
                state: Mutex::new(ControlState {
                    current: None,
                    last: None,
                }),
            }),
        }
    }

    /// 业务作用：创建不进入共享注册表的 standalone Runner，供 `PartitionExecutor` 兼容包装。
    ///
    /// 参数说明：
    /// - `id`: standalone 执行域使用的进程内身份。
    /// - `config`: 兼容执行域的冻结配置。
    ///
    /// 返回：启动成功时返回已开放准入的 Runner；runtime 缺失或控制面建立失败时返回启动错误。
    pub(crate) fn standalone(id: u64, config: RunnerConfig) -> Result<Self, StartError> {
        let runner = Self::registered(
            id,
            RunnerName::new("standalone").map_err(|_| StartError::ControlPlaneUnavailable)?,
            config,
        );
        runner.start_now()?;
        Ok(runner)
    }

    /// 业务作用：读取注册时冻结的稳定名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：注册表已校验且同控制对象重启不变的名称引用。
    pub fn name(&self) -> &RunnerName {
        &self.control.name
    }

    /// 业务作用：读取完整冻结配置；同名重启不会改变该引用指向的值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已规范化并通过有界校验的 `RunnerConfig` 引用。
    pub fn config(&self) -> &RunnerConfig {
        &self.control.config
    }

    /// 业务作用：启动全新 generation；初始化在首次 poll 内由独立 supervisor 接管，调用
    /// Future 随后取消不会撤销已建立的控制面。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 worker、timer 与 observer 已受监督且业务准入开放时返回 Ok；
    /// runtime 缺失、已启动、旧代未收敛或控制面建立失败时返回稳定错误。
    pub async fn start(&self) -> Result<(), StartError> {
        if let Some(previous) = self.current_inner() {
            if previous.operation.result().is_none()
                || !matches!(previous.phase(), RunnerPhase::Stopped | RunnerPhase::Failed)
            {
                return if previous.phase() == RunnerPhase::Accepting {
                    Err(StartError::AlreadyStarted)
                } else {
                    Err(StartError::PreviousGenerationActive)
                };
            }
            let deadline = Instant::now() + self.config().shutdown_timeout;
            if !join_supervisor_until(&previous, deadline).await {
                return Err(StartError::PreviousGenerationActive);
            }
            // 旧 supervisor 自身已经 join 后才能摘除 generation；被取消的旧 stop 等待者不再
            // 是重启前必须由调用方手动补做的隐式前置步骤。
            self.complete_generation(&previous);
        }
        let result = self.start_now();
        if matches!(result, Err(StartError::ControlPlaneUnavailable)) {
            if let Some(inner) = self.current_inner() {
                // 子任务只建立了一部分时立即关闭入口并交给同一 supervisor 收口；只有取得
                // 子任务与 supervisor 的 join 证明后才摘除失败 generation。
                inner.request_force_stop("start_rollback");
                let deadline = Instant::now() + self.config().shutdown_timeout;
                let operation_done = wait_operation_until(&inner, deadline).await.is_some();
                if operation_done && join_supervisor_until(&inner, deadline).await {
                    self.complete_generation(&inner);
                }
            }
        }
        result
    }

    /// 业务作用：同步建立 supervisor，再创建 slot、timer 与 observer，最后一次性开放提交。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：控制面和全部子任务登记完成时返回 `Ok`；runtime、生命周期或控制面门禁拒绝时返回启动错误。
    fn start_now(&self) -> Result<(), StartError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| StartError::RuntimeUnavailable)?;
        let mut state = self.control.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = &state.current {
            return if current.phase() == RunnerPhase::Accepting {
                Err(StartError::AlreadyStarted)
            } else {
                Err(StartError::PreviousGenerationActive)
            };
        }
        let generation = self
            .control
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| StartError::ControlPlaneUnavailable)?;
        let generation_control = GenerationControl::new(generation);
        let mut slots = Vec::with_capacity(self.config().partitions);
        let mut consumers = Vec::with_capacity(self.config().partitions);
        for index in 0..self.config().partitions as u32 {
            let (slot, consumer) = PartitionSlot::new(
                self.control.id,
                generation,
                index,
                self.config().max_inbound_tunnels,
            );
            slots.push(slot);
            consumers.push(consumer);
        }
        let inner = Arc::new(RunnerInner::new(
            self.control.id,
            generation,
            self.control.config.clone(),
            slots.into(),
            generation_control.clone(),
            runtime,
        ));
        let supervisor_inner = inner.clone();
        generation_control
            .spawn_supervisor(async move { supervise_guarded(supervisor_inner).await })
            .map_err(map_supervisor_start)?;
        state.current = Some(inner.clone());
        drop(state);

        if inner.start_children(consumers).is_err() {
            inner.fail_control_plane("child_start_failed");
            return Err(StartError::ControlPlaneUnavailable);
        }
        // 所有 worker、timer 和 observer 的 JoinHandle 都已登记后才开放业务路由。
        inner.phase.store(PHASE_ACCEPTING, Ordering::Release);
        inner.lifecycle().notify_progress();
        Ok(())
    }

    /// 业务作用：同步发布无损停止请求，供 `stop_all` 先关全部 Runner 再顺序等待。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；动作由 generation supervisor 持续推进，调用方无需持续 poll。
    pub fn request_stop(&self) {
        if let Some(inner) = self.current_inner() {
            inner.request_graceful_stop();
        }
    }

    /// 业务作用：无损排空当前 generation，并在绝对期限内等待全部子任务与 supervisor join。
    ///
    /// 参数说明：
    /// - `deadline`: 调用方共享预算的绝对时刻，不会被内部等待重置。
    ///
    /// 返回：子任务和 supervisor 都取得退出证明时返回无损报告；期限内未收敛时
    /// 保持 `Stopping` 并返回 `NotConverged`。
    pub async fn stop(&self, deadline: Instant) -> Result<ShutdownReport, StopError> {
        let Some(inner) = self.current_inner() else {
            return Ok(self.last_report());
        };
        inner.request_graceful_stop();
        let Some(report) = wait_operation_until(&inner, deadline).await else {
            return Err(StopError::NotConverged);
        };
        if !join_supervisor_until(&inner, deadline).await {
            return Err(StopError::NotConverged);
        }
        self.complete_generation(&inner);
        Ok(report)
    }

    /// 业务作用：显式升级同一个停止 operation 为有损模式，中止协作让出的业务 Future 并
    /// 冻结未执行任务；deadline 不能替代真实 join 证明。
    ///
    /// 参数说明：
    /// - `deadline`: 等待有损收口、全部子任务与 supervisor 退出的绝对时刻。
    ///
    /// 返回：取得完整退出证明时返回损耗报告；期限内未收敛时返回
    /// `NotConverged` 且不声称已停止。
    pub async fn force_stop(&self, deadline: Instant) -> Result<ShutdownReport, ForceStopError> {
        let Some(inner) = self.current_inner() else {
            return Ok(self.last_report());
        };
        inner.request_force_stop("explicit_force_stop");
        let Some(report) = wait_operation_until(&inner, deadline).await else {
            return Err(ForceStopError::NotConverged);
        };
        if !join_supervisor_until(&inner, deadline).await {
            return Err(ForceStopError::NotConverged);
        }
        self.complete_generation(&inner);
        Ok(report)
    }

    /// 业务作用：按显式类型非阻塞提交任务并返回权威终态句柄。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务键。
    /// - `spec`: 冻结 `TaskType` 与严格或非严格顺序语义。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回 `Submission`；保留类型、容量、停机或最小故障域门禁拒绝时
    /// 返回稳定原因。
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
        self.submit_with_route(|| crate::RouteHash::from_key(&key), spec, task)
    }

    /// 业务作用：直接使用已冻结摘要非阻塞提交，使调用方的顺序门禁与 Runner 共用路由。
    ///
    /// 参数说明：
    /// - `route`: 由业务 key 冻结的本地摘要，不再进行第二次哈希。
    /// - `spec`: 当前 Runner 内稳定的任务类型和顺序策略。
    /// - `task`: 受理后在监督边界执行的异步任务工厂。
    ///
    /// 返回：成功返回稳定 `Submission`；容量、保留类型、停机和失权使用普通提交的拒绝语义。
    pub fn submit_routed_typed<F, Fut>(
        &self,
        route: crate::RouteHash,
        spec: TaskSpec,
        task: F,
    ) -> Result<Submission, SubmitRejection>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.submit_with_route(|| route, spec, task)
    }

    /// 业务作用：统一两种提交入口的拒绝与容量逻辑，在门禁通过后才求值路由。
    ///
    /// 参数说明：
    /// - `route`: 普通 key 的摘要工厂或已经冻结的路由值。
    /// - `spec`: 任务类型和顺序合同。
    /// - `task`: 受理后移交给任务监督边界的工厂。
    ///
    /// 返回：成功时返回稳定票据；拒绝不执行任务，保留类型或停止门禁也不调用业务 Hash。
    fn submit_with_route<F, Fut>(
        &self,
        route: impl FnOnce() -> crate::RouteHash,
        spec: TaskSpec,
        task: F,
    ) -> Result<Submission, SubmitRejection>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if spec.ty == LEGACY_TYPE {
            if let Some(inner) = self.current_inner() {
                inner
                    .metrics
                    .rejected_reserved_type
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(SubmitRejection::ReservedTaskType);
        }
        let inner = self.accepting_inner()?;
        inner.submit_now(
            route().0,
            spec,
            Box::pin(async move { task().await }),
            false,
        )
    }

    /// 业务作用：为 Application 默认兼容入口提交同原始分区严格串行任务，不开放保留
    /// `TaskType` 给业务调用方。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；停机、容量耗尽或分区失权返回兼容错误。
    pub fn submit<K, F, Fut>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.submit_legacy(key, task)
    }

    /// 业务作用：为 Application 默认兼容入口提交同步短任务，顺序边界与 `submit` 一致。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `task`: 受理后执行的同步任务。
    ///
    /// 返回：受理成功返回空值；停机、容量耗尽或分区失权返回兼容错误。
    pub fn submit_sync<K, F>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        self.submit_legacy(key, move || async move { task() })
    }

    /// 业务作用：为 Application 默认兼容入口取消安全地等待容量，再提交严格串行任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `task`: 取得容量后执行的异步任务工厂。
    ///
    /// 返回：成功受理返回空值；停机或分区失权返回兼容错误。
    pub async fn submit_compat_async<K, F, Fut>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.submit_legacy_waiting(key, task).await
    }

    /// 业务作用：按显式类型提交无需逐任务句柄的任务，失败仍通过稳定拒绝原因和累计指标可见。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务键。
    /// - `spec`: 冻结任务类型和顺序语义。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；门禁或容量拒绝时返回稳定原因。
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
        self.submit_typed(key, spec, task).map(|_| ())
    }

    /// 业务作用：取消安全地等待类型与全局容量后提交任务；等待期间不保留 producer 临界计数。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务键。
    /// - `spec`: 冻结任务类型和顺序语义。
    /// - `task`: 两层容量都取得后才转移给 Runner 的任务工厂。
    ///
    /// 返回：受理成功返回终态句柄；等待 Future 被取消时归还已取得许可，Runner
    /// 停止或类型失权时返回稳定拒绝。
    pub async fn submit_async<K, F, Fut>(
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
        if spec.ty == LEGACY_TYPE {
            if let Some(inner) = self.current_inner() {
                inner
                    .metrics
                    .rejected_reserved_type
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(SubmitRejection::ReservedTaskType);
        }
        let inner = self.accepting_inner()?;
        let hash = hash_key(key);
        let home = inner.home(hash);
        let type_state = inner.get_or_create_type(home, spec, false)?;
        let queued = crate::shield_future(type_state.queued_budget().acquire_owned())
            .await
            .map_err(|_| SubmitRejection::ShuttingDown)?;
        let global = crate::shield_future(inner.global.clone().acquire_owned())
            .await
            .map_err(|_| SubmitRejection::ShuttingDown)?;
        inner.submit_admitted(
            home,
            type_state,
            Box::pin(async move { task().await }),
            global,
            queued,
        )
    }

    /// 业务作用：登记延迟任务；登记期只占 Runner 全局预算，到期后才竞争类型排队许可与严格序号。
    ///
    /// 参数说明：
    /// - `key`: 到期时决定原始分区的业务键。
    /// - `delay`: 任务不早于该时长进入类型路由。
    /// - `spec`: 冻结任务类型和顺序语义。
    /// - `task`: 到期且重新准入成功后执行的异步任务工厂。
    ///
    /// 返回：登记成功返回终态句柄；保留类型、Runner 停止或全局容量耗尽时返回
    /// 稳定拒绝。
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
        if spec.ty == LEGACY_TYPE {
            if let Some(inner) = self.current_inner() {
                inner
                    .metrics
                    .rejected_reserved_type
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(SubmitRejection::ReservedTaskType);
        }
        let inner = self.accepting_inner()?;
        inner.submit_delayed(
            hash_key(key),
            delay,
            spec,
            Box::pin(async move { task().await }),
        )
    }

    /// 业务作用：读取当前 Runner 健康，不把一个 Runner 的失败传播到同注册表其它执行域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前或最近 generation 的 `Healthy`、`Degraded`、`Failed` 或 `Stopped`。
    pub fn health(&self) -> RunnerHealth {
        let Some(inner) = self.snapshot_inner() else {
            return RunnerHealth::Stopped;
        };
        inner.health()
    }

    /// 业务作用：导出当前或最近 generation 的低基数指标快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未启动时为全零，否则为不持有业务任务引用的独立快照。
    pub fn metrics_snapshot(&self) -> RunnerMetricsSnapshot {
        self.snapshot_inner()
            .map(|inner| inner.metrics_snapshot())
            .unwrap_or_default()
    }

    /// 业务作用：导出当前或最近 generation 的有界失败证据，不持有任务或业务载荷引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最近样本、累计样本数和覆盖数的独立快照。
    pub fn frozen_evidence(&self) -> FrozenEvidenceSnapshot {
        self.snapshot_inner()
            .map(|inner| inner.frozen.snapshot())
            .unwrap_or_default()
    }

    /// 业务作用：读取当前或最近 generation 的生命周期阶段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未启动时为 `Stopped`，否则为当前或最近一代的稳定阶段。
    pub fn phase(&self) -> RunnerPhase {
        self.snapshot_inner()
            .map(|inner| inner.phase())
            .unwrap_or(RunnerPhase::Stopped)
    }

    /// 业务作用：读取当前或最近 generation epoch，尚未启动时为零。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未启动为零；否则为每次成功建立新代都单调增加且不复用的值。
    pub fn epoch(&self) -> u64 {
        self.snapshot_inner().map_or(0, |inner| inner.generation)
    }

    /// 业务作用：取得当前活动 generation，不把最近已停止 generation 当成可提交对象。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在活动 generation 时返回共享内核，否则返回 `None`。
    fn current_inner(&self) -> Option<Arc<RunnerInner>> {
        self.control
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current
            .clone()
    }

    /// 业务作用：取得当前或最近 generation，供停止后的指标与证据继续可读。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：优先返回活动 generation；不存在时返回最近已收口 generation；从未启动时返回 `None`。
    fn snapshot_inner(&self) -> Option<Arc<RunnerInner>> {
        let state = self.control.state.lock().unwrap_or_else(|e| e.into_inner());
        state.current.clone().or_else(|| state.last.clone())
    }

    /// 业务作用：取得现役且开放提交的 generation，否则登记稳定停机拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前代处于 `Accepting` 时返回内核；未启动或已关闭准入时返回 `ShuttingDown`。
    fn accepting_inner(&self) -> Result<Arc<RunnerInner>, SubmitRejection> {
        let inner = self.current_inner().ok_or(SubmitRejection::ShuttingDown)?;
        if inner.phase() != RunnerPhase::Accepting {
            inner
                .metrics
                .rejected_shutting_down
                .fetch_add(1, Ordering::Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        Ok(inner)
    }

    /// 业务作用：取得 supervisor join 证明后从控制对象摘除当前 generation，并保留只读快照。
    ///
    /// 参数说明：
    /// - `inner`: 已取得 supervisor join 证明的 generation 内核。
    ///
    /// 返回：无；仅当参数仍是当前代时完成摘除，避免旧等待者覆盖新代。
    fn complete_generation(&self, inner: &Arc<RunnerInner>) {
        let mut state = self.control.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, inner))
        {
            state.last = state.current.take();
        }
    }

    /// 业务作用：读取最近停止报告；从未启动时返回全零无损报告。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最近 generation 的稳定报告；从未启动或尚未发布报告时返回默认报告。
    fn last_report(&self) -> ShutdownReport {
        self.snapshot_inner()
            .and_then(|inner| inner.operation.result())
            .unwrap_or_default()
    }

    /// 业务作用：standalone 兼容包装调整类型状态上限，不改变命名注册表的冻结配置。
    ///
    /// 参数说明：
    /// - `limit`: 兼容入口希望允许的类型状态总数。
    ///
    /// 返回：无；实际下限保持为 slot 数，命名 Runner 配置不受影响。
    pub(crate) fn set_standalone_type_limit(&self, limit: usize) {
        if let Some(inner) = self.current_inner() {
            inner
                .type_limit
                .store(limit.max(inner.slots.len()), Ordering::Release);
        }
    }

    /// 业务作用：standalone 兼容入口提交保留严格类型，不开放保留值给类型化调用方。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的兼容业务键。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；停机、容量耗尽或执行域失权时返回兼容错误。
    pub(crate) fn submit_legacy<K, F, Fut>(&self, key: K, task: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let inner = self.accepting_inner().map_err(legacy_rejection)?;
        inner
            .submit_now(
                hash_key(key),
                TaskSpec {
                    ty: LEGACY_TYPE,
                    ordering: TaskOrdering::Strict,
                },
                Box::pin(async move { task().await }),
                true,
            )
            .map(|_| ())
            .map_err(legacy_rejection)
    }

    /// 业务作用：standalone 兼容入口等待容量后提交保留严格类型。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的兼容业务键。
    /// - `task`: 两层容量取得后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；等待期间停止或执行域失权时返回兼容错误。
    pub(crate) async fn submit_legacy_waiting<K, F, Fut>(
        &self,
        key: K,
        task: F,
    ) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let inner = self.accepting_inner().map_err(legacy_rejection)?;
        let home = inner.home(hash_key(key));
        let type_state = inner
            .get_or_create_type(
                home,
                TaskSpec {
                    ty: LEGACY_TYPE,
                    ordering: TaskOrdering::Strict,
                },
                true,
            )
            .map_err(legacy_rejection)?;
        let queued = crate::shield_future(type_state.queued_budget().acquire_owned())
            .await
            .map_err(|_| SubmitError::ShuttingDown)?;
        let global = crate::shield_future(inner.global.clone().acquire_owned())
            .await
            .map_err(|_| SubmitError::ShuttingDown)?;
        inner
            .submit_admitted(
                home,
                type_state,
                Box::pin(async move { task().await }),
                global,
                queued,
            )
            .map(|_| ())
            .map_err(legacy_rejection)
    }
}

/// 当前 generation 数据面；后台任务持有本对象，不反向持有 `RunnerControl`。
pub(crate) struct RunnerInner {
    id: u64,
    generation: u64,
    config: Arc<RunnerConfig>,
    phase: AtomicU8,
    failure: Mutex<Option<&'static str>>,
    slots: Arc<[Arc<PartitionSlot>]>,
    mask: usize,
    producers: AtomicUsize,
    accepting_slots: AtomicUsize,
    global: Arc<Semaphore>,
    type_state_count: AtomicUsize,
    type_limit: AtomicUsize,
    next_task_id: AtomicU64,
    next_tunnel_id: AtomicU64,
    observation_epoch: AtomicU64,
    target_cursor: AtomicUsize,
    steal_pending: Arc<[Arc<AtomicBool>]>,
    delayed: Mutex<HashMap<u64, DelayedTask>>,
    moving: Mutex<HashMap<u64, Arc<TaskEnvelope>>>,
    timer: Arc<RunnerTimer>,
    observer_wake: Arc<Notify>,
    metrics: RunnerMetrics,
    frozen: FrozenEvidenceRing,
    generation_control: Arc<GenerationControl>,
    operation: Arc<StopOperation>,
    runtime: tokio::runtime::Handle,
}

impl RunnerInner {
    /// 业务作用：创建尚处 Starting 的 generation 内核，所有预算和可变状态仅属于当前 Runner。
    ///
    /// 参数说明：
    /// - `id`: 所属 Runner 的稳定身份。
    /// - `generation`: 本次启动单调递增且不复用的代号。
    /// - `config`: 当前 Runner 的冻结配置。
    /// - `slots`: 当前代独占的分区 slot 集合。
    /// - `generation_control`: 监督当前代全部后台任务的生命周期核心。
    /// - `runtime`: 创建 generation 时所在 Tokio runtime 的稳定派发句柄。
    ///
    /// 返回：准入尚未开放、预算与控制状态完成初始化的 generation 内核。
    fn new(
        id: u64,
        generation: u64,
        config: Arc<RunnerConfig>,
        slots: Arc<[Arc<PartitionSlot>]>,
        generation_control: Arc<GenerationControl>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let partitions = slots.len();
        let steal_pending = (0..partitions)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect::<Vec<_>>()
            .into();
        Self {
            id,
            generation,
            phase: AtomicU8::new(PHASE_STARTING),
            failure: Mutex::new(None),
            mask: partitions - 1,
            slots,
            producers: AtomicUsize::new(0),
            accepting_slots: AtomicUsize::new(partitions),
            global: Arc::new(Semaphore::new(config.global_inflight)),
            type_state_count: AtomicUsize::new(0),
            type_limit: AtomicUsize::new(config.max_type_states),
            next_task_id: AtomicU64::new(1),
            next_tunnel_id: AtomicU64::new(1),
            observation_epoch: AtomicU64::new(0),
            target_cursor: AtomicUsize::new(0),
            steal_pending,
            delayed: Mutex::new(HashMap::new()),
            moving: Mutex::new(HashMap::new()),
            timer: RunnerTimer::new(),
            observer_wake: Arc::new(Notify::new()),
            metrics: RunnerMetrics::new(),
            frozen: FrozenEvidenceRing::new(config.frozen_evidence_capacity),
            generation_control,
            operation: StopOperation::new(),
            runtime,
            config,
        }
    }

    /// 业务作用：读取冻结配置，后台任务不得从其它 Runner 借用控制参数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 Runner 创建时冻结的配置引用。
    pub(crate) fn config(&self) -> &RunnerConfig {
        &self.config
    }

    /// 业务作用：取得当前 generation 生命周期核心。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：负责子任务登记、退出通知和权威失败传播的共享核心。
    pub(crate) fn lifecycle(&self) -> Arc<crate::lifecycle::LifecycleCore> {
        self.generation_control.lifecycle()
    }

    /// 业务作用：把数值阶段解码为封闭公开枚举，未知值按 Failed 处理。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前生命周期阶段；无法识别的内部值保守映射为 `Failed`。
    fn phase(&self) -> RunnerPhase {
        match self.phase.load(Ordering::Acquire) {
            PHASE_STARTING => RunnerPhase::Starting,
            PHASE_ACCEPTING => RunnerPhase::Accepting,
            PHASE_STOPPING => RunnerPhase::Stopping,
            PHASE_STOPPED => RunnerPhase::Stopped,
            _ => RunnerPhase::Failed,
        }
    }

    /// 业务作用：启动全部 worker、唯一 timer 和唯一 observer，并把每个 JoinHandle 纳入控制核心。
    ///
    /// 参数说明：
    /// - `consumers`: 与 slot 一一对应且仅能消费当前代队列的接收端集合。
    ///
    /// 返回：全部子任务成功登记时返回 `Ok`；身份校验或生命周期登记失败时返回对应错误。
    fn start_children(
        self: &Arc<Self>,
        consumers: Vec<SlotConsumers>,
    ) -> Result<(), RegisterError> {
        for (slot, consumers) in self.slots.iter().cloned().zip(consumers) {
            if slot.runner_id() != self.id || slot.generation() != self.generation {
                // worker 获得的 slot 身份不属于当前 generation 时必须在启动前拒绝，避免旧代
                // consumer 取得新代队列的执行权威。
                self.lifecycle().fail_authority();
                return Err(RegisterError::AuthorityFailed);
            }
            let inner = self.clone();
            self.spawn_registered(ChildTaskKind::Worker, async move {
                slot::run(inner, slot, consumers).await;
            })?;
        }
        let timer = self.timer.clone();
        let inner = self.clone();
        self.spawn_registered(ChildTaskKind::Timer, async move {
            timer.run(inner).await;
        })?;
        let inner = self.clone();
        self.spawn_registered(ChildTaskKind::Observer, async move {
            observer::run(inner).await;
        })?;
        Ok(())
    }

    /// 业务作用：先登记 JoinHandle 再释放启动门禁，防止子任务在监督权建立前产生外部副作用。
    ///
    /// 参数说明：
    /// - `kind`: 子任务类别，用于 join 统计与异常隔离。
    /// - `future`: 通过启动门禁后运行且必须被 supervisor 监督的任务。
    ///
    /// 返回：JoinHandle 完成登记后返回 `Ok`；生命周期已关闭登记时返回错误且不授予执行权。
    fn spawn_registered(
        self: &Arc<Self>,
        kind: ChildTaskKind,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), RegisterError> {
        let (start_tx, start_rx) = oneshot::channel();
        let lifecycle = self.lifecycle();
        let owner = self.clone();
        let handle = self.runtime.spawn(async move {
            if start_rx.await.is_ok() {
                let outcome = AssertUnwindSafe(future).catch_unwind().await;
                if outcome.is_err() {
                    // 业务 Future 自身的展开已在更内层转成单任务失败；这里仅处理执行框架
                    // 或后台控制任务的异常离场，并关闭当前 Runner 而不影响其它名称。
                    owner.fail_control_plane(match kind {
                        ChildTaskKind::Worker => "worker_panicked",
                        ChildTaskKind::Observer => "observer_panicked",
                        ChildTaskKind::Timer => "timer_panicked",
                        ChildTaskKind::Business => "business_boundary_panicked",
                        ChildTaskKind::Cleanup => "cleanup_boundary_panicked",
                    });
                }
            }
            lifecycle.notify_progress();
        });
        self.lifecycle().register_child(kind, handle)?;
        let _ = start_tx.send(());
        Ok(())
    }

    /// 业务作用：先登记 JoinHandle 再在 Tokio 阻塞线程池释放未执行业务载荷，使公开取消
    /// 不执行不可信析构，同时让停止 supervisor 保留完整退出证明。
    ///
    /// 参数说明：
    /// - `action`: 需要在受监督阻塞边界执行的一次性清理动作。
    ///
    /// 返回：句柄成功登记并开放启动门禁时返回 `Ok`；生命周期失权时中止任务并返回错误。
    fn spawn_cleanup_registered(
        self: &Arc<Self>,
        action: impl FnOnce() + Send + 'static,
    ) -> Result<(), RegisterError> {
        let (start_tx, start_rx) = oneshot::channel();
        let lifecycle = self.lifecycle();
        let owner = self.clone();
        let handle = self.runtime.spawn_blocking(move || {
            if start_rx.blocking_recv().is_ok() {
                if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(action)) {
                    owner.fail_control_plane("cleanup_boundary_panicked");
                    crate::run_isolated("清理边界 panic payload 析构", move || drop(payload));
                }
            }
            lifecycle.notify_progress();
        });
        self.lifecycle()
            .register_child(ChildTaskKind::Cleanup, handle)?;
        let _ = start_tx.send(());
        Ok(())
    }

    /// 业务作用：把 key 哈希稳定映射到当前 generation 的 2 的幂 slot。
    ///
    /// 参数说明：
    /// - `hash`: 业务键的稳定哈希值。
    ///
    /// 返回：当前 generation 内合法的原始 slot 编号。
    fn home(&self, hash: u64) -> u32 {
        (hash as usize & self.mask) as u32
    }

    /// 业务作用：读取或并发创建固定类型状态，Runner 级 reservation 防止跨 slot 放大上限。
    ///
    /// 参数说明：
    /// - `home`: 类型状态永久归属的原始 slot。
    /// - `spec`: 本次提交要求冻结的类型与顺序语义。
    /// - `_legacy`: 标记兼容入口，保留于内部调用边界以显式区分保留类型来源。
    ///
    /// 返回：现有定义一致或新建成功时返回类型状态；停机、定义冲突、失权或总量门禁拒绝时返回稳定原因。
    fn get_or_create_type(
        &self,
        home: u32,
        spec: TaskSpec,
        _legacy: bool,
    ) -> Result<Arc<TypeState>, SubmitRejection> {
        if self.phase() != RunnerPhase::Accepting {
            return Err(SubmitRejection::ShuttingDown);
        }
        let slot = &self.slots[home as usize];
        if !slot.accepting() {
            self.metrics
                .rejected_type_failed
                .fetch_add(1, Ordering::Relaxed);
            return Err(SubmitRejection::LaneFailed);
        }
        if let Some(existing) = slot.type_states.get(&spec.ty) {
            return if existing.ordering() == spec.ordering {
                if existing.failed() {
                    Err(SubmitRejection::LaneFailed)
                } else {
                    Ok(existing.clone())
                }
            } else {
                self.metrics
                    .rejected_ordering_conflict
                    .fetch_add(1, Ordering::Relaxed);
                Err(SubmitRejection::OrderingConflict)
            };
        }
        let limit = self.type_limit.load(Ordering::Acquire);
        if self
            .type_state_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < limit).then_some(value + 1)
            })
            .is_err()
        {
            self.metrics
                .rejected_type_limit
                .fetch_add(1, Ordering::Relaxed);
            return Err(SubmitRejection::LaneLimitExceeded);
        }
        let candidate = TypeState::new(
            self.id,
            self.generation,
            home,
            spec.ty,
            spec.ordering,
            self.config.queue_capacity_per_type,
        );
        use dashmap::mapref::entry::Entry;
        match slot.type_states.entry(spec.ty) {
            Entry::Vacant(entry) => {
                entry.insert(candidate.clone());
                if self.phase() == RunnerPhase::Accepting && slot.accepting() {
                    Ok(candidate)
                } else {
                    // 停止可能与类型创建交错；新状态已经进入低基数表时保持冻结，但必须关闭
                    // 其许可并拒绝本次提交，不能在 close_admission 扫描后留下开放等待点。
                    candidate.fail();
                    Err(SubmitRejection::ShuttingDown)
                }
            }
            Entry::Occupied(entry) => {
                self.type_state_count.fetch_sub(1, Ordering::AcqRel);
                let existing = entry.get().clone();
                if existing.failed() {
                    Err(SubmitRejection::LaneFailed)
                } else if existing.ordering() == spec.ordering {
                    Ok(existing)
                } else {
                    self.metrics
                        .rejected_ordering_conflict
                        .fetch_add(1, Ordering::Relaxed);
                    Err(SubmitRejection::OrderingConflict)
                }
            }
        }
    }

    /// 业务作用：执行非阻塞双层准入并把任务发布到现役主队列或盗洞。
    ///
    /// 参数说明：
    /// - `hash`: 已计算的业务键哈希。
    /// - `spec`: 任务类型及其冻结顺序语义。
    /// - `job`: 受理成功后由业务边界执行的 Future。
    /// - `legacy`: 是否由 standalone 兼容入口提交保留类型。
    ///
    /// 返回：双层容量与路由发布都成功时返回终态句柄；任一准入门禁拒绝时返回稳定原因。
    fn submit_now(
        self: &Arc<Self>,
        hash: u64,
        spec: TaskSpec,
        job: Job,
        legacy: bool,
    ) -> Result<Submission, SubmitRejection> {
        let _producer = ProducerGuard::enter(self)?;
        let home = self.home(hash);
        let type_state = self.get_or_create_type(home, spec, legacy)?;
        let queued = type_state
            .queued_budget()
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics
                    .rejected_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                SubmitRejection::QueueFull
            })?;
        let global = self.global.clone().try_acquire_owned().map_err(|_| {
            self.metrics
                .rejected_overloaded
                .fetch_add(1, Ordering::Relaxed);
            SubmitRejection::Overloaded
        })?;
        self.submit_admitted(home, type_state, job, global, queued)
    }

    /// 业务作用：在 RAII 许可齐备后签发任务 ID、严格序号并完成一次物理发布。
    ///
    /// 参数说明：
    /// - `home`: 任务固定的原始 slot。
    /// - `type_state`: 已冻结定义且属于当前代的类型状态。
    /// - `job`: 尚未执行的业务 Future。
    /// - `global`: 当前 Runner 的全局在飞许可。
    /// - `queued`: 固定 `(home, TaskType)` 的排队许可。
    ///
    /// 返回：任务权威和物理落点均发布成功时返回 `Submission`；失权或路由失败时返回稳定拒绝。
    fn submit_admitted(
        self: &Arc<Self>,
        home: u32,
        type_state: Arc<TypeState>,
        job: Job,
        global: OwnedSemaphorePermit,
        queued: OwnedSemaphorePermit,
    ) -> Result<Submission, SubmitRejection> {
        let _producer = ProducerGuard::enter(self)?;
        if type_state.failed() {
            return Err(SubmitRejection::LaneFailed);
        }
        let strict_sequence = if type_state.ordering() == TaskOrdering::Strict {
            Some(
                type_state
                    .issue_strict_sequence()
                    .ok_or(SubmitRejection::LaneFailed)?,
            )
        } else {
            None
        };
        let id = self.next_task_id()?;
        let entry = crate::entry::TaskEntry::new(
            id,
            self.id,
            self.generation,
            home,
            u64::from(type_state.task_type().0),
            job,
            Some(global),
            Some(queued),
        )
        .map_err(|_| SubmitRejection::LaneFailed)?;
        let envelope = TaskEnvelope::new(entry.clone(), type_state.clone(), strict_sequence);
        let binding = Arc::new(OnceLock::new());
        if binding.set(envelope.clone()).is_err() {
            self.settle_queued_projection(&envelope);
            type_state.settle_strict(strict_sequence);
            return Err(SubmitRejection::LaneFailed);
        }
        if let Err(error) = self.publish_initial(envelope.clone()) {
            self.settle_queued_projection(&envelope);
            type_state.settle_strict(strict_sequence);
            return Err(error);
        }
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        Ok(Submission {
            entry,
            envelope: binding,
            inner: Arc::downgrade(self),
            delayed_id: None,
        })
    }

    /// 业务作用：建立延迟任务权威和 timer 索引；到期前不取得类型排队许可或严格序号。
    ///
    /// 参数说明：
    /// - `hash`: 到期后重新计算原始 slot 所用的业务键哈希。
    /// - `delay`: 不早于该时长触发重新准入。
    /// - `spec`: 到期时冻结的类型与顺序语义。
    /// - `job`: 重新准入成功后执行的业务 Future。
    ///
    /// 返回：timer 与延迟权威登记成功时返回终态句柄；全局容量、ID 或控制面门禁拒绝时返回稳定原因。
    fn submit_delayed(
        self: &Arc<Self>,
        hash: u64,
        delay: Duration,
        spec: TaskSpec,
        job: Job,
    ) -> Result<Submission, SubmitRejection> {
        let _producer = ProducerGuard::enter(self)?;
        let home = self.home(hash);
        let global = self.global.clone().try_acquire_owned().map_err(|_| {
            self.metrics
                .rejected_overloaded
                .fetch_add(1, Ordering::Relaxed);
            SubmitRejection::Overloaded
        })?;
        let id = self.next_task_id()?;
        let entry = crate::entry::TaskEntry::new(
            id,
            self.id,
            self.generation,
            home,
            u64::from(spec.ty.0),
            job,
            Some(global),
            None,
        )
        .map_err(|_| SubmitRejection::LaneFailed)?;
        entry
            .publish_delayed()
            .map_err(|_| SubmitRejection::LaneFailed)?;
        let envelope = Arc::new(OnceLock::new());
        self.delayed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                id,
                DelayedTask {
                    entry: entry.clone(),
                    hash,
                    spec,
                    envelope: envelope.clone(),
                },
            );
        if !self.timer.register(id, delay) {
            self.delayed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            let _ = entry.fail_from(
                EntryState::Delayed,
                EntryState::Rejected,
                "timer_registration_failed",
            );
            let _ = entry.finalize_termination(|_| {});
            return Err(SubmitRejection::ShuttingDown);
        }
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        Ok(Submission {
            entry,
            envelope,
            inner: Arc::downgrade(self),
            delayed_id: Some(id),
        })
    }

    /// 业务作用：timer 到期后重新取得延迟任务，竞争类型许可与严格序号，再按当前路由发布。
    ///
    /// 参数说明：
    /// - `id`: timer 返回且必须与延迟索引一致的任务 ID。
    ///
    /// 返回：无；成功时把任务发布到现役路由，任何到期门禁拒绝都形成可观测终态。
    pub(crate) fn expire_delayed(self: &Arc<Self>, id: u64) {
        let Some(delayed) = self
            .delayed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
        else {
            return;
        };
        let entry = &delayed.entry;
        if entry.claim_expired().is_err() {
            return;
        }
        if self.phase() != RunnerPhase::Accepting {
            self.reject_unbound_delayed(entry, "shutdown_before_expiry");
            return;
        }
        let home = self.home(delayed.hash);
        if home != entry.home() {
            self.reject_unbound_delayed(entry, "delayed_home_mismatch");
            return;
        }
        let type_state = match self.get_or_create_type(home, delayed.spec, false) {
            Ok(state) => state,
            Err(error) => {
                let reason = match error {
                    SubmitRejection::OrderingConflict => "ordering_conflict_at_expiry",
                    SubmitRejection::LaneLimitExceeded => "type_limit_at_expiry",
                    SubmitRejection::LaneFailed => "type_failed_at_expiry",
                    SubmitRejection::ShuttingDown => "shutdown_before_expiry",
                    SubmitRejection::QueueFull => "queue_full_at_expiry",
                    SubmitRejection::Overloaded => "overloaded_at_expiry",
                    SubmitRejection::ReservedTaskType => "reserved_type_at_expiry",
                };
                self.reject_unbound_delayed(entry, reason);
                return;
            }
        };
        let queued = match type_state.queued_budget().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                if self.reject_unbound_delayed(entry, "queue_full_at_expiry") {
                    self.metrics
                        .rejected_queue_full
                        .fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
        };
        if entry.install_queued_permit(queued).is_err() {
            self.reject_unbound_delayed(entry, "delayed_permit_install_failed");
            return;
        }
        let strict_sequence = if type_state.ordering() == TaskOrdering::Strict {
            let Some(sequence) = type_state.issue_strict_sequence() else {
                self.reject_unbound_delayed(entry, "strict_sequence_exhausted");
                return;
            };
            Some(sequence)
        } else {
            None
        };
        let envelope = TaskEnvelope::new(entry.clone(), type_state.clone(), strict_sequence);
        if delayed.envelope.set(envelope.clone()).is_err() {
            self.settle_queued_projection(&envelope);
            type_state.settle_strict(strict_sequence);
            self.reject_unbound_delayed(entry, "delayed_binding_conflict");
            return;
        }
        if let Err(error) = self.publish_initial(envelope.clone()) {
            let reason = match error {
                SubmitRejection::ShuttingDown => "shutdown_at_delayed_publish",
                SubmitRejection::QueueFull => "queue_full_at_delayed_publish",
                SubmitRejection::Overloaded => "overloaded_at_delayed_publish",
                SubmitRejection::OrderingConflict => "ordering_conflict_at_delayed_publish",
                SubmitRejection::LaneFailed => "route_failed_at_delayed_publish",
                SubmitRejection::LaneLimitExceeded => "type_limit_at_delayed_publish",
                SubmitRejection::ReservedTaskType => "reserved_type_at_delayed_publish",
            };
            let rejected = match entry.state() {
                Ok(EntryState::Enqueueing)
                    if entry
                        .fail_from(EntryState::Enqueueing, EntryState::Rejected, reason)
                        .is_ok() =>
                {
                    self.finalize_terminal(&envelope, false, false);
                    true
                }
                Ok(EntryState::Terminating) => {
                    self.finalize_terminal(&envelope, false, false);
                    false
                }
                // placement.publish 可能已经完成 Rejected 终态再把错误返回上层；该分支仍由
                // 本次 delayed 到期发布负责分类，但并发取消形成的 Cancelled 不能重复记账。
                Ok(EntryState::Rejected) => true,
                Ok(state) if state.is_terminal() => false,
                _ => {
                    self.record_authority_failure(envelope.clone(), reason);
                    false
                }
            };
            if rejected {
                self.record_delayed_publish_rejection(error);
            }
            // 取消清理可能在 envelope 绑定前已经发布稳定终态；到期路径随后创建的排队
            // 投影没有其它 consumer 可见，必须在发布失败处幂等撤销，避免健康 Runner
            // 永久保留虚假的 queued_depth。
            self.settle_queued_projection(&envelope);
            type_state.settle_strict(strict_sequence);
        }
    }

    /// 业务作用：为已经登记成功、但到期物理发布被拒的 delayed 任务记录稳定拒绝分类，
    /// 使 submitted 与各终态及拒绝原因保持闭合。
    ///
    /// 参数说明：
    /// - `error`: 到期物理发布返回且已经取得 Rejected 终态权威的稳定原因。
    ///
    /// 返回：无；只增加对应低基数累计量。
    fn record_delayed_publish_rejection(&self, error: SubmitRejection) {
        let metric = match error {
            SubmitRejection::ShuttingDown => &self.metrics.rejected_shutting_down,
            SubmitRejection::QueueFull => &self.metrics.rejected_queue_full,
            SubmitRejection::Overloaded => &self.metrics.rejected_overloaded,
            SubmitRejection::OrderingConflict => &self.metrics.rejected_ordering_conflict,
            SubmitRejection::LaneFailed => &self.metrics.rejected_type_failed,
            SubmitRejection::LaneLimitExceeded => &self.metrics.rejected_type_limit,
            SubmitRejection::ReservedTaskType => &self.metrics.rejected_reserved_type,
        };
        metric.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：取消或停止路径从逻辑 delayed 表和 timer 索引幂等摘除任务。
    ///
    /// 参数说明：
    /// - `id`: 需要从两个延迟索引移除的任务 ID。
    ///
    /// 返回：无；索引不存在时保持幂等，并通知 supervisor 重新判断收口条件。
    pub(crate) fn retire_delayed(&self, id: u64) {
        self.delayed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        self.timer.cancel(id);
        self.lifecycle().notify_progress();
    }

    /// 业务作用：按现役路由预留物理容器，建立逻辑 owner 后再发布任务 Arc。
    ///
    /// 参数说明：
    /// - `envelope`: 已建立任务权威、类型状态和可选严格序号的任务投影。
    ///
    /// 返回：唯一物理落点发布成功时返回 `Ok`；路由、队列或权威门禁失败时返回稳定拒绝并结算资源。
    fn publish_initial(
        self: &Arc<Self>,
        envelope: Arc<TaskEnvelope>,
    ) -> Result<(), SubmitRejection> {
        let placement = self.reserve_placement(&envelope)?;
        let target = placement.target();
        let logical = placement.logical_owner();
        let type_state = envelope.type_state();
        type_state.increment_logical();
        self.increment_container(logical);
        let authority = envelope.authority();
        if authority
            .prepare_initial_queue(Owner::Slot(authority.home()), placement.owner(), logical)
            .is_err()
        {
            self.decrement_container(logical);
            let _ = type_state.decrement_logical();
            return Err(SubmitRejection::LaneFailed);
        }
        if placement.publish(envelope.clone()).is_err() {
            let _ = authority.detach_unpublished_container();
            let _ = authority.fail_from(
                EntryState::Queued,
                EntryState::Rejected,
                "queue_publish_failed",
            );
            self.finalize_terminal(&envelope, false, false);
            return Err(SubmitRejection::LaneFailed);
        }
        self.slots[target as usize].wake_handle().notify_one();
        Ok(())
    }

    /// 业务作用：在严格路由权威或非严格活动快照下选择唯一物理落点并取得 reservation。
    ///
    /// 参数说明：
    /// - `envelope`: 提供冻结类型定义和当前路由权威的任务投影。
    ///
    /// 返回：成功时返回尚未发布的唯一 `Placement`；类型失权或目标队列不可用时返回稳定拒绝。
    fn reserve_placement(
        &self,
        envelope: &Arc<TaskEnvelope>,
    ) -> Result<Placement, SubmitRejection> {
        let state = envelope.type_state();
        if state.failed() {
            return Err(SubmitRejection::LaneFailed);
        }
        match state.ordering() {
            TaskOrdering::Relaxed => {
                if let Some(tunnel) = state.select_non_strict_tunnel() {
                    let target = tunnel.target();
                    let reservation = tunnel.reserve().map_err(queue_rejection)?;
                    Ok(Placement::NonStrict {
                        tunnel,
                        target,
                        reservation,
                    })
                } else {
                    self.reserve_main(state.home())
                }
            }
            TaskOrdering::Strict => state
                .with_strict_route(|route| match route.state {
                    StrictRouteState::Local | StrictRouteState::LocalCatchup => {
                        self.reserve_main(state.home())
                    }
                    StrictRouteState::Migrating
                    | StrictRouteState::Stolen
                    | StrictRouteState::StolenCatchup => {
                        let tunnel = route.tunnel.clone().ok_or(SubmitRejection::LaneFailed)?;
                        let target = tunnel.target();
                        let reservation = tunnel
                            .reserve(StrictQueue::Incremental)
                            .map_err(queue_rejection)?;
                        Ok(Placement::Strict {
                            tunnel,
                            target,
                            queue: StrictQueue::Incremental,
                            reservation,
                        })
                    }
                    StrictRouteState::ReturnPrepare | StrictRouteState::Returning => {
                        let tunnel = route.tunnel.clone().ok_or(SubmitRejection::LaneFailed)?;
                        let target = tunnel.target();
                        let reservation = tunnel
                            .reserve(StrictQueue::Staging)
                            .map_err(queue_rejection)?;
                        Ok(Placement::Strict {
                            tunnel,
                            target,
                            queue: StrictQueue::Staging,
                            reservation,
                        })
                    }
                    StrictRouteState::Failed => Err(SubmitRejection::LaneFailed),
                })
                .unwrap_or(Err(SubmitRejection::LaneFailed)),
        }
    }

    /// 业务作用：预留指定 slot 主队列，关门或失败统一映射为持久拒收。
    ///
    /// 参数说明：
    /// - `target`: 当前 generation 内的目标 slot 编号。
    ///
    /// 返回：主队列开放且可预留时返回 `Placement`；目标不存在、关闭或耗尽时返回稳定拒绝。
    fn reserve_main(&self, target: u32) -> Result<Placement, SubmitRejection> {
        let slot = self
            .slots
            .get(target as usize)
            .ok_or(SubmitRejection::LaneFailed)?
            .clone();
        let reservation = slot.reserve_main().map_err(queue_rejection)?;
        Ok(Placement::Main { slot, reservation })
    }

    /// 业务作用：消费主队列任务；源 worker 按现役路由决定执行或搬入盗洞 stock。
    ///
    /// 参数说明：
    /// - `slot`: 唯一摘取该主队列载荷的源 slot。
    /// - `sequence`: 载荷在主队列中的物理序号证据。
    /// - `envelope`: 从队列取得且等待 owner 复验的任务投影。
    ///
    /// 返回：任务已派发、迁移或终结时返回 `None`；需要 worker 稍后继续处理时返回原投影。
    pub(crate) fn consume_main(
        self: &Arc<Self>,
        slot: &Arc<PartitionSlot>,
        sequence: u64,
        envelope: Arc<TaskEnvelope>,
    ) -> Option<Arc<TaskEnvelope>> {
        let authority = envelope.authority();
        if authority.state().is_ok_and(|state| state.is_terminal()) {
            return None;
        }
        if authority
            .claim_container(Owner::Slot(slot.index()))
            .is_err()
        {
            if authority.state().is_ok_and(|state| state.is_terminal()) {
                return None;
            }
            self.record_authority_failure(envelope, "main_claim_failed");
            return None;
        }
        if authority.state() == Ok(EntryState::Terminating) {
            self.finalize_terminal(&envelope, false, false);
            return None;
        }
        if slot.index() == envelope.type_state().home() {
            if envelope.type_state().ordering() == TaskOrdering::Strict {
                if let Some(route) = envelope.type_state().strict_route() {
                    if matches!(
                        route.state,
                        StrictRouteState::Migrating | StrictRouteState::Stolen
                    ) {
                        if let Some(tunnel) = route.tunnel {
                            if self.move_to_strict(
                                slot,
                                sequence,
                                envelope.clone(),
                                tunnel,
                                StrictQueue::Stock,
                                MoveStage::StrictStock,
                            ) {
                                return None;
                            }
                        }
                    }
                }
            } else if let Some(tunnel) = if slot.execution_available() {
                envelope.type_state().select_non_strict_tunnel()
            } else {
                envelope.type_state().select_non_strict_tunnel_for_move()
            } {
                if self.move_to_non_strict(slot, sequence, envelope.clone(), tunnel) {
                    return None;
                }
            }
        }
        if !slot.execution_available() {
            // 其它类型已有出站盗洞时 worker 仍会穿过主队列；当前类型没有可用落点则保留
            // Handling 与物理序号，避免提前登记业务等待者而封死后来出现的严格迁移机会。
            return Some(envelope);
        }
        self.dispatch_business(slot.index(), envelope);
        None
    }

    /// 业务作用：重新处理从源主队列摘取并保留物理序号的任务；控制请求若在等待期间
    /// 安装了盗洞，优先按现役路由迁出，否则只在源执行位可用时派发业务 Future。
    ///
    /// 参数说明：
    /// - `source`: 唯一摘取主队列载荷并保留 Handling 责任的源 slot。
    /// - `sequence`: 任务原始主队列序号，严格迁移边界必须继续携带。
    /// - `envelope`: 尚未交给业务 task 的任务投影。
    ///
    /// 返回：任务已迁移、派发或终结时返回 None；仍无安全执行位置时归还任务供 worker
    /// 继续保留。
    pub(crate) fn resume_main_handling(
        self: &Arc<Self>,
        source: &Arc<PartitionSlot>,
        sequence: u64,
        envelope: Arc<TaskEnvelope>,
    ) -> Option<Arc<TaskEnvelope>> {
        match envelope.authority().state() {
            Ok(EntryState::Terminating) => {
                self.finalize_terminal(&envelope, false, false);
                return None;
            }
            Ok(state) if state.is_terminal() => return None,
            _ => {}
        }
        if source.index() == envelope.type_state().home() {
            if envelope.type_state().ordering() == TaskOrdering::Strict {
                if let Some(route) = envelope.type_state().strict_route() {
                    if matches!(
                        route.state,
                        StrictRouteState::Migrating | StrictRouteState::Stolen
                    ) {
                        if let Some(tunnel) = route.tunnel {
                            if self.move_to_strict(
                                source,
                                sequence,
                                envelope.clone(),
                                tunnel,
                                StrictQueue::Stock,
                                MoveStage::StrictStock,
                            ) {
                                return None;
                            }
                        }
                    }
                }
            } else if let Some(tunnel) = envelope.type_state().select_non_strict_tunnel_for_move() {
                if self.move_to_non_strict(source, sequence, envelope.clone(), tunnel) {
                    return None;
                }
            }
        }
        if source.execution_available() {
            self.dispatch_business(source.index(), envelope);
            None
        } else {
            Some(envelope)
        }
    }

    /// 业务作用：slot worker 在重新取得执行位后继续派发此前由处理栈独占的队头任务。
    ///
    /// 参数说明：
    /// - `owner`: 当前持有处理责任的 slot 编号。
    /// - `envelope`: worker 私有且尚未交给业务 task 的任务投影。
    ///
    /// 返回：成功派发或终结时返回 `None`；当前仍无法派发时归还投影供 worker 保留。
    pub(crate) fn resume_handling(
        self: &Arc<Self>,
        owner: u32,
        envelope: Arc<TaskEnvelope>,
    ) -> Option<Arc<TaskEnvelope>> {
        if self.dispatch_business(owner, envelope.clone()) {
            None
        } else {
            Some(envelope)
        }
    }

    /// 业务作用：有损停止把 worker 私有 deferred 任务发布为冻结终态，不能随栈析构静默消失。
    ///
    /// 参数说明：
    /// - `envelope`: 当前 worker 私有且尚未运行的任务投影。
    /// - `reason`: 写入失败终态与有界证据的静态原因码。
    ///
    /// 返回：无；能够取得终态权威时立即完成资源结算。
    pub(crate) fn fail_deferred(&self, envelope: &Arc<TaskEnvelope>, reason: &'static str) {
        self.fail_before_running(envelope, reason);
    }

    /// 业务作用：消费入站盗洞任务并在目标 owner 复验后交给业务执行边界。
    ///
    /// 参数说明：
    /// - `slot`: 当前盗洞的唯一目标 consumer。
    /// - `_sequence`: 保留的物理 FIFO 序号证据，严格业务顺序由任务受理序号门禁。
    /// - `envelope`: 从盗洞取得且等待 owner 复验的任务投影。
    ///
    /// 返回：任务已派发或终结时返回 `None`；需要 worker 稍后继续处理时返回原投影。
    pub(crate) fn consume_tunnel(
        self: &Arc<Self>,
        slot: &Arc<PartitionSlot>,
        _sequence: u64,
        envelope: Arc<TaskEnvelope>,
    ) -> Option<Arc<TaskEnvelope>> {
        let authority = envelope.authority();
        if authority.state().is_ok_and(|state| state.is_terminal()) {
            return None;
        }
        if authority
            .claim_container(Owner::Slot(slot.index()))
            .is_err()
        {
            if authority.state().is_ok_and(|state| state.is_terminal()) {
                return None;
            }
            self.record_authority_failure(envelope, "tunnel_claim_failed");
            return None;
        }
        if authority.state() == Ok(EntryState::Terminating) {
            self.finalize_terminal(&envelope, false, false);
            return None;
        }
        if self.dispatch_business(slot.index(), envelope.clone()) {
            None
        } else {
            Some(envelope)
        }
    }

    /// 业务作用：按 owner 移动协议把源主队列中的非严格存量搬入目标单 consumer 盗洞。
    ///
    /// 参数说明：
    /// - `source`: 当前摘取任务的原始 slot。
    /// - `source_sequence`: 任务在源主队列中的物理序号。
    /// - `envelope`: 等待转移 owner 与物理容器的任务投影。
    /// - `tunnel`: 已由源路由发布并指向目标 slot 的非严格盗洞。
    ///
    /// 返回：任务已转交目标或进入失败收口时返回 true；目标无法预留且源仍可执行时返回 false。
    fn move_to_non_strict(
        self: &Arc<Self>,
        source: &Arc<PartitionSlot>,
        source_sequence: u64,
        envelope: Arc<TaskEnvelope>,
        tunnel: Arc<NonStrictTunnel>,
    ) -> bool {
        let Ok(reservation) = tunnel.reserve() else {
            return false;
        };
        self.move_entry(
            source,
            tunnel.target(),
            source_sequence,
            envelope,
            MoveStage::NonStrictOutbound,
            LogicalContainer::NonStrictTunnel,
            reservation,
            |published| {
                if published {
                    tunnel.commit_reserved();
                } else {
                    tunnel.abort_reserved();
                }
            },
        )
    }

    /// 业务作用：按 owner 移动协议把严格任务搬入 stock 或归还阶段 FIFO。
    ///
    /// 参数说明：
    /// - `source`: 当前摘取任务并持有移动权威的 slot。
    /// - `source_sequence`: 任务在来源 FIFO 中的物理序号。
    /// - `envelope`: 携带严格受理序号的任务投影。
    /// - `tunnel`: 当前严格路由唯一引用的目标盗洞。
    /// - `queue`: stock、incremental 或 staging 目标 FIFO。
    /// - `stage`: 与目标 FIFO 对应的权威移动阶段。
    ///
    /// 返回：任务已转交目标或进入失败收口时返回 true；目标无法预留且来源仍可处理时返回 false。
    fn move_to_strict(
        self: &Arc<Self>,
        source: &Arc<PartitionSlot>,
        source_sequence: u64,
        envelope: Arc<TaskEnvelope>,
        tunnel: Arc<StrictTunnel>,
        queue: StrictQueue,
        stage: MoveStage,
    ) -> bool {
        let Ok(reservation) = tunnel.reserve(queue) else {
            return false;
        };
        let container = match queue {
            StrictQueue::Stock => LogicalContainer::StrictStock,
            StrictQueue::Incremental => LogicalContainer::StrictIncremental,
            StrictQueue::Staging => LogicalContainer::ReturnStaging,
        };
        self.move_entry(
            source,
            tunnel.target(),
            source_sequence,
            envelope,
            stage,
            container,
            reservation,
            |published| {
                if published {
                    tunnel.commit_reserved(queue);
                } else {
                    tunnel.abort_reserved(queue);
                }
            },
        )
    }

    /// 业务作用：执行“目标增计、owner 转移、物理承诺、逻辑替换、来源减计、稳定状态、物理
    /// 发布”的完整移动顺序，任何无法回滚的状态都会冻结当前类型。
    ///
    /// 参数说明：
    /// - `source`: 当前唯一摘取载荷的源 slot。
    /// - `target`: 移动完成后承担逻辑责任的 slot。
    /// - `source_sequence`: 载荷在来源 FIFO 中的物理序号证据。
    /// - `envelope`: 携带任务权威、类型状态与受理序号的共享投影。
    /// - `stage`: 当前物理移动协议阶段。
    /// - `container`: 目标逻辑容器类别。
    /// - `reservation`: 目标 FIFO 已预留且尚未发布的唯一槽位。
    /// - `finish_physical`: 物理发布成功或失败后恢复深度与队列门禁的一次性动作。
    ///
    /// 返回：来源载荷已交给目标或已转入失败收口时返回 true；尚可由源 slot
    /// 继续执行或重试移动时返回 false。
    #[allow(clippy::too_many_arguments)]
    fn move_entry(
        self: &Arc<Self>,
        source: &Arc<PartitionSlot>,
        target: u32,
        source_sequence: u64,
        envelope: Arc<TaskEnvelope>,
        stage: MoveStage,
        container: LogicalContainer,
        reservation: Reservation<Arc<TaskEnvelope>>,
        finish_physical: impl FnOnce(bool),
    ) -> bool {
        let authority = envelope.authority();
        if authority.runner_id() != self.id
            || authority.runner_generation() != self.generation
            || authority.task_type() != u64::from(envelope.type_state().task_type().0)
        {
            self.record_authority_failure(envelope, "move_identity_mismatch");
            return true;
        }
        let ticket = match authority.prepare_move(
            stage,
            source.index(),
            target,
            source_sequence,
            Instant::now(),
        ) {
            Ok(ticket) => ticket,
            Err(_) => {
                // 任务从来源容器进入 Handling 后仍可被调用方取消；此时目标 reservation
                // 只需要以 poison 保持 FIFO 连续，不能把合法取消竞争升级为整条严格路由失权。
                drop(reservation);
                match authority.state() {
                    Ok(EntryState::Terminating) => {
                        self.finalize_terminal(&envelope, false, false);
                    }
                    Ok(state) if state.is_terminal() => {}
                    _ => self.record_authority_failure(envelope, "move_prepare_failed"),
                }
                return true;
            }
        };
        self.moving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(authority.id(), envelope.clone());
        self.metrics.moving.fetch_add(1, Ordering::Relaxed);
        if authority.activate_move(ticket).is_err() {
            let abandoned = authority.abandon_prepared_move(ticket).is_ok();
            self.finish_moving(authority.id());
            // Queued 到 Moving 的 CAS 可与合法取消竞争；描述符撤销后由当前 Handling
            // 责任方完成既定取消终态，目标 poison 仅用于推进已预留的物理序号。
            drop(reservation);
            if abandoned && authority.state() == Ok(EntryState::Terminating) {
                self.finalize_terminal(&envelope, false, false);
            } else if !authority.state().is_ok_and(|state| state.is_terminal()) {
                self.record_authority_failure(envelope, "move_activation_failed");
            }
            return true;
        }
        let target_logical = LogicalOwner {
            slot: target,
            route_epoch: ticket.epoch(),
            container,
        };
        self.increment_container(target_logical);
        if authority.transfer_move_owner(ticket).is_err()
            || authority.prepare_move_container(ticket).is_err()
        {
            self.decrement_container(target_logical);
            self.fail_active_move(&envelope, ticket, "move_authority_failed");
            return true;
        }
        let old_logical = match authority.replace_logical_owner(ticket, target_logical) {
            Ok(logical) => logical,
            Err(_) => {
                self.decrement_container(target_logical);
                self.fail_active_move(&envelope, ticket, "move_authority_failed");
                return true;
            }
        };
        // 新逻辑位置、owner 与物理承诺都已建立后才能减少来源；短暂双计保护迁移审计
        // 不会在任一观察时刻把已受理任务漏出全部容器计数。
        self.decrement_container(old_logical);
        let _completion = match authority.publish_move(ticket) {
            Ok(completion) => completion,
            Err(_) => {
                self.fail_active_move(&envelope, ticket, "move_publish_failed");
                return true;
            }
        };
        if reservation.publish(envelope.clone()).is_err() {
            // 物理队列已失去可消费证明，先撤销预留深度再发布任务失败，
            // 否则停机会永久等待一个不可能被 consumer 取得的槽位。
            finish_physical(false);
            let _ = authority.detach_unpublished_container();
            let _ = authority.fail_from(
                EntryState::Queued,
                EntryState::Failed,
                "move_physical_publish_failed",
            );
            let _ = authority.clear_completed_move(ticket);
            self.finish_moving(authority.id());
            envelope.type_state().fail();
            self.finalize_terminal(&envelope, true, false);
            return true;
        }
        finish_physical(true);
        let _ = authority.clear_completed_move(ticket);
        self.finish_moving(authority.id());
        // 移动期间登记的取消意图已经由 move publisher 转为 Terminating；目标物理队列的
        // 唯一 consumer 摘取条目后再完成结算，不能越过 Container retention。
        self.slots[target as usize].wake_handle().notify_one();
        true
    }

    /// 业务作用：取消方取得终态意图但任务仍由物理容器持有时唤醒现役 consumer，由其完成
    /// 唯一摘取和资源结算。
    ///
    /// 参数说明：
    /// - `envelope`: 已进入 Terminating、仍等待物理 consumer 的任务投影。
    ///
    /// 返回：无；owner 无法定位时唤醒本 generation 全部 slot，避免归还暂存任务失去推进者。
    pub(crate) fn notify_pending_terminal(&self, envelope: &Arc<TaskEnvelope>) {
        if let Ok(Owner::Slot(slot)) = envelope.authority().owner() {
            if let Some(slot) = self.slots.get(slot as usize) {
                slot.wake_handle().notify_one();
                return;
            }
        }
        for slot in self.slots.iter() {
            slot.wake_handle().notify_one();
        }
    }

    /// 业务作用：移动进入不可证明状态时只冻结当前类型和任务，保留其它 slot 与 Runner 服务。
    ///
    /// 参数说明：
    /// - `envelope`: 当前移动涉及的任务投影。
    /// - `ticket`: 唯一标识当前 move epoch 和来源证据的动作票据。
    /// - `reason`: 写入失败终态和诊断证据的静态原因码。
    ///
    /// 返回：无；关闭当前类型并完成可取得的任务结算。
    fn fail_active_move(
        &self,
        envelope: &Arc<TaskEnvelope>,
        ticket: crate::entry::MoveTicket,
        reason: &'static str,
    ) {
        let authority = envelope.authority();
        let _ = authority.fail_move(ticket, reason);
        let _ = authority.clear_completed_move(ticket);
        self.finish_moving(authority.id());
        envelope.type_state().fail();
        self.finalize_terminal(envelope, true, false);
    }

    /// 业务作用：从 moving 审计表摘除任务并更新瞬时计数。
    ///
    /// 参数说明：
    /// - `id`: 已完成、撤销或冻结的任务 ID。
    ///
    /// 返回：无；仅在审计表确有记录时减少瞬时移动计数，并通知 supervisor。
    fn finish_moving(&self, id: u64) {
        if self
            .moving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
            .is_some()
        {
            self.metrics.moving.fetch_sub(1, Ordering::Relaxed);
        }
        self.lifecycle().notify_progress();
    }

    /// 业务作用：审计 moving 表中因 worker 异常离场而未完成的条目；转换期限内保留
    /// Queued 准备窗口，超时后按 Handling 或 Container 责任分别冻结或清理。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；尚在合法转换期限内的条目保持原责任方权威。
    fn audit_moves(&self) {
        let entries = self
            .moving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let now = Instant::now();
        for envelope in entries {
            let authority = envelope.authority();
            let Some((ticket, started_at)) = authority.move_audit_snapshot() else {
                self.finish_moving(authority.id());
                continue;
            };
            let expired = self.lifecycle().mode() == LifecycleMode::ForceStop
                || now.saturating_duration_since(started_at) >= self.config.transition_timeout;
            match authority.state() {
                Ok(EntryState::Moving) if expired => {
                    // 超时条目已经失去原 worker 的及时完成证明，不能猜测物理位置或继续执行；
                    // 当前 move epoch 取得唯一失败权威后再结算。
                    self.fail_active_move(&envelope, ticket, "move_transition_timed_out");
                }
                Ok(EntryState::Moving) => {}
                Ok(EntryState::Queued | EntryState::Terminating) if !expired => {
                    // moving 表必须先于 Queued -> Moving 发布；在此准备窗口清理描述符会让
                    // 合法发布者失去 ticket，并把一次普通移动误判成路由失权。
                }
                Ok(EntryState::Queued | EntryState::Terminating) => {
                    match authority.physical_retention() {
                        Ok(crate::entry::PhysicalRetention::Container) => {
                            // 目标 Container 已经建立，说明物理发布路径只遗留了审计描述符；
                            // 清理后仍由目标唯一 consumer 负责终态，不能在来源重复执行。
                            let _ = authority.clear_completed_move(ticket);
                            self.finish_moving(authority.id());
                        }
                        Ok(crate::entry::PhysicalRetention::Handling) => {
                            let abandoned = authority.abandon_prepared_move(ticket).is_ok();
                            self.finish_moving(authority.id());
                            if abandoned && authority.state() == Ok(EntryState::Terminating) {
                                self.finalize_terminal(&envelope, false, false);
                            } else {
                                self.fail_before_running(&envelope, "move_preparation_timed_out");
                            }
                        }
                        _ => {
                            let _ = authority.abandon_prepared_move(ticket);
                            self.finish_moving(authority.id());
                            self.record_authority_failure(
                                envelope,
                                "move_preparation_retention_mismatch",
                            );
                        }
                    }
                }
                Ok(_) => {
                    let _ = authority.clear_completed_move(ticket);
                    self.finish_moving(authority.id());
                }
                Err(_) => self.fail_control_plane("move_state_corrupted"),
            }
        }
    }

    /// 业务作用：把已取得 Handling 的任务交给独立受监督业务 task，slot worker 不被长 Future 阻塞。
    ///
    /// 参数说明：
    /// - `owner`: 承担本次执行责任并提供执行位的 slot 编号。
    /// - `envelope`: 已由物理 consumer 摘取并取得处理权威的任务投影。
    ///
    /// 返回：任务已终结或成功登记业务 task 时返回 true；当前实现不保留未派发返回路径。
    fn dispatch_business(self: &Arc<Self>, owner: u32, envelope: Arc<TaskEnvelope>) -> bool {
        match envelope.authority().state() {
            Ok(EntryState::Terminating) => {
                self.finalize_terminal(&envelope, false, false);
                return true;
            }
            Ok(state) if state.is_terminal() => return true,
            _ => {}
        }
        if self.lifecycle().mode() == LifecycleMode::ForceStop {
            self.fail_before_running(&envelope, "shutdown_frozen");
            return true;
        }
        let strict = envelope.type_state().ordering() == TaskOrdering::Strict;
        let strict_handling = if strict {
            let Some(handling) = envelope.type_state().begin_strict_handling() else {
                self.fail_before_running(&envelope, "strict_handling_exhausted");
                return true;
            };
            Some(handling)
        } else {
            None
        };
        let strict_execution = if strict
            && envelope
                .strict_sequence()
                .is_some_and(|sequence| envelope.type_state().strict_turn_ready(sequence))
        {
            envelope.type_state().try_acquire_strict_execution()
        } else {
            None
        };
        let execution = if strict {
            strict_execution
                .as_ref()
                .and_then(|_| self.slots[owner as usize].try_acquire_execution())
        } else {
            self.slots[owner as usize].try_acquire_execution()
        };
        let inner = self.clone();
        let envelope_for_task = envelope.clone();
        if self
            .spawn_registered(ChildTaskKind::Business, async move {
                run_business(
                    inner,
                    owner,
                    envelope_for_task,
                    execution,
                    strict_execution,
                    strict_handling,
                )
                .await;
            })
            .is_err()
        {
            self.fail_before_running(&envelope, "business_registration_failed");
        }
        true
    }

    /// 业务作用：把尚未运行且由当前处理栈独占的任务冻结为稳定失败终态。
    ///
    /// 参数说明：
    /// - `envelope`: 尚未跨越业务执行边界的任务投影。
    /// - `reason`: 写入任务失败状态的静态原因码。
    ///
    /// 返回：无；已有终态保持不变，能够取得失败权威时完成结算。
    fn fail_before_running(&self, envelope: &Arc<TaskEnvelope>, reason: &'static str) {
        match envelope.authority().state() {
            Ok(EntryState::Terminating) => {
                self.finalize_terminal(envelope, false, false);
                return;
            }
            Ok(state) if state.is_terminal() => return,
            _ => {}
        }
        if envelope
            .authority()
            .fail_from(EntryState::Queued, EntryState::Failed, reason)
            .is_ok()
        {
            self.finalize_terminal(envelope, true, false);
        }
    }

    /// 业务作用：完成任务终态的逻辑计数、容量许可、严格序号、指标与有界证据结算。
    ///
    /// 参数说明：
    /// - `envelope`: 已取得终态意图且由当前责任方帮助结算的任务。
    /// - `frozen`: 是否把失败计入冻结损耗与证据环。
    /// - `aborted`: 是否把失败计入有损停止中止量。
    ///
    /// 返回：无；首个结算者发布稳定终态并归还资源，迟到帮助者幂等退出，其它
    /// 权威不一致会关闭当前类型。
    fn finalize_terminal(&self, envelope: &Arc<TaskEnvelope>, frozen: bool, aborted: bool) {
        let authority = envelope.authority();
        let type_state = envelope.type_state();
        match authority.finalize_termination_with_accounting(
            |logical| self.commit_logical(type_state, logical),
            |terminal| {
                self.settle_queued_projection(envelope);
                self.commit_terminal_accounting(terminal, frozen, aborted);
            },
        ) {
            Ok(()) => {}
            Err(TaskAuthorityError::SettlementOwned) => return,
            Err(_) => {
                type_state.fail();
                return;
            }
        }
        type_state.settle_strict(envelope.strict_sequence());
        if authority.state() == Ok(EntryState::Failed) && (frozen || aborted) {
            self.record_frozen(envelope);
        }
        self.lifecycle().notify_progress();
    }

    /// 业务作用：在任务容量许可释放和稳定终态公开前提交唯一终态累计量，使停机报告
    /// 能按累计量与停止基线的原子先后准确区分运行期完成和收口期处置。
    ///
    /// 参数说明：
    /// - `terminal`: 当前唯一结算者即将发布的稳定终态。
    /// - `frozen`: Failed 是否属于未执行任务冻结损耗。
    /// - `aborted`: Failed 是否属于执行中任务有损中止。
    ///
    /// 返回：无；每个任务只能由唯一终态结算者调用一次。
    pub(crate) fn commit_terminal_accounting(
        &self,
        terminal: EntryState,
        frozen: bool,
        aborted: bool,
    ) {
        match terminal {
            EntryState::Completed => {
                self.metrics.completed.fetch_add(1, Ordering::Relaxed);
            }
            EntryState::Cancelled => {
                self.metrics.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            EntryState::Failed => {
                self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                if aborted {
                    self.metrics.aborted.fetch_add(1, Ordering::Relaxed);
                }
                if frozen {
                    self.metrics.frozen.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => {}
        }
    }

    /// 业务作用：撤销任务的已受理排队投影；重复帮助保持幂等，底层下溢时隔离最小类型。
    ///
    /// 参数说明：
    /// - `envelope`: 携带唯一排队结算位和固定类型状态的任务投影。
    ///
    /// 返回：无；正常与重复结算不改变健康，下溢时登记失败类型并关闭该类型准入。
    pub(crate) fn settle_queued_projection(&self, envelope: &Arc<TaskEnvelope>) {
        if !envelope.settle_queued() && envelope.type_state().fail() {
            self.metrics.failed_types.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 业务作用：按任务逻辑 owner 恰好减少一次容器与类型计数，终态可见前完成账本提交。
    ///
    /// 参数说明：
    /// - `type_state`: 任务归属且需要减少逻辑数量的类型状态。
    /// - `logical`: 终态前仍承担任务责任的逻辑容器。
    ///
    /// 返回：无；计数下溢会关闭当前 Runner 控制面，避免错误宣告排空。
    pub(crate) fn commit_logical(&self, type_state: &TypeState, logical: LogicalOwner) {
        self.decrement_container(logical);
        if !type_state.decrement_logical() {
            self.fail_control_plane("logical_count_underflow");
        }
    }

    /// 业务作用：把尚未绑定物理容器的延迟取消交给受监督阻塞清理任务，调用线程只发布
    /// 取消意图，不直接运行用户 Future 的析构逻辑。
    ///
    /// 参数说明：
    /// - `entry`: 已取得取消权且仍等待稳定终态的任务权威。
    /// - `envelope`: 到期竞争可能稍后写入的任务投影绑定。
    ///
    /// 返回：清理任务成功纳入当前 generation 监督时返回 true；登记失权时返回 false，
    /// 调用方必须同步兜底结算。
    pub(crate) fn schedule_delayed_cancel(
        self: &Arc<Self>,
        entry: Arc<crate::entry::TaskEntry<Job>>,
        envelope: EnvelopeBinding,
    ) -> bool {
        let inner = self.clone();
        self.spawn_cleanup_registered(move || {
            inner.settle_delayed_cancel(entry, envelope);
        })
        .is_ok()
    }

    /// 业务作用：在受监督清理边界完成延迟取消的逻辑账本、严格序号、容量、载荷和累计量
    /// 结算；到期发布竞争已取得物理责任时只唤醒唯一 consumer。
    ///
    /// 参数说明：
    /// - `entry`: 已处 `Terminating` 或被并发结算者接管的任务权威。
    /// - `envelope`: 到期路径建立后用于定位类型与逻辑容器的共享绑定。
    ///
    /// 返回：无；成功时发布 `Cancelled`，并发结算幂等退出，无法证明权威时关闭最小
    /// 故障域或当前 Runner 控制面。
    fn settle_delayed_cancel(
        &self,
        entry: Arc<crate::entry::TaskEntry<Job>>,
        envelope: EnvelopeBinding,
    ) {
        let result = entry.finalize_termination_with_accounting(
            |logical| {
                let Some(bound) = envelope.get() else {
                    panic!("延迟取消存在逻辑 owner 但缺少任务投影绑定");
                };
                self.commit_logical(bound.type_state(), logical);
            },
            |terminal| {
                if let Some(bound) = envelope.get() {
                    self.settle_queued_projection(bound);
                }
                self.commit_terminal_accounting(terminal, false, false);
            },
        );
        let bound = envelope.get().cloned();
        match result {
            Ok(()) => {
                if let Some(bound) = &bound {
                    bound.type_state().settle_strict(bound.strict_sequence());
                }
                self.lifecycle().notify_progress();
            }
            Err(TaskAuthorityError::SettlementOwned) => {}
            Err(TaskAuthorityError::RetentionMismatch)
                if entry
                    .state()
                    .is_ok_and(|state| state == EntryState::Terminating || state.is_terminal()) =>
            {
                if let Some(bound) = bound {
                    self.notify_pending_terminal(&bound);
                }
            }
            Err(_) => {
                if let Some(bound) = bound {
                    self.record_authority_failure(bound, "delayed_cancel_settlement_failed");
                } else {
                    self.fail_unbound_delayed("delayed_cancel_settlement_failed");
                }
            }
        }
    }

    /// 业务作用：任务 owner 或结算权威不一致时冻结最小类型故障域并登记有界证据。
    ///
    /// 参数说明：
    /// - `envelope`: 需要复验终态、owner 与类型归属的任务。
    /// - `reason`: 记入稳定失败意图和有界证据的静态原因码。
    ///
    /// 返回：无；终态或正常结算竞争幂等退出，可帮助的活动状态先结算，仍无法
    /// 证明权威时才冻结类型。
    pub(crate) fn record_authority_failure(
        &self,
        envelope: Arc<TaskEnvelope>,
        reason: &'static str,
    ) {
        if let Ok(state) = envelope.authority().state() {
            if state.is_terminal() {
                return;
            }
            if state == EntryState::Terminating {
                // 已有取消或失败结算者取得唯一终态权威时只帮助完成既定结果，不能把正常
                // 取消竞争升级为类型隔离。
                self.finalize_terminal(&envelope, false, false);
                return;
            }
            if matches!(
                state,
                EntryState::Enqueueing | EntryState::Delayed | EntryState::Queued
            ) && envelope
                .authority()
                .fail_from(state, EntryState::Failed, reason)
                .is_ok()
            {
                self.finalize_terminal(&envelope, true, false);
                return;
            }
        }
        if envelope.type_state().fail() {
            self.metrics.failed_types.fetch_add(1, Ordering::Relaxed);
        }
        self.record_frozen(&envelope);
    }

    /// 业务作用：把稳定失败事实投影到有界诊断环，不保存任务 Arc 或业务 Future。
    ///
    /// 参数说明：
    /// - `envelope`: 提供任务类型、原始分区、终态与静态原因码的任务投影。
    ///
    /// 返回：无；证据环满时按有界覆盖策略保留最近样本。
    fn record_frozen(&self, envelope: &Arc<TaskEnvelope>) {
        self.frozen.push(FrozenEvidence {
            task_type: envelope.type_state().task_type(),
            partition: envelope.type_state().home(),
            status: TaskStatus::Failed,
            reason: envelope.authority().reason(),
        });
    }

    /// 业务作用：为逻辑容器增加短暂双计中的目标份额；移动完成后来源份额再减少。
    ///
    /// 参数说明：
    /// - `logical`: 需要承担任务责任的 slot、路由 epoch 与容器类型。
    ///
    /// 返回：无；主队列与盗洞分别更新独立逻辑计数。
    fn increment_container(&self, logical: LogicalOwner) {
        let slot = &self.slots[logical.slot as usize];
        match logical.container {
            LogicalContainer::Main => {
                slot.local_task_count.fetch_add(1, Ordering::AcqRel);
            }
            _ => {
                slot.tunnel_task_count.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    /// 业务作用：终态或移动提交后减少旧逻辑容器份额，下溢会隔离对应 slot。
    ///
    /// 参数说明：
    /// - `logical`: 即将解除任务责任的逻辑容器描述。
    ///
    /// 返回：无；计数无法安全递减时隔离对应 slot，防止错误宣告排空。
    fn decrement_container(&self, logical: LogicalOwner) {
        let slot = &self.slots[logical.slot as usize];
        let counter = match logical.container {
            LogicalContainer::Main => &slot.local_task_count,
            _ => &slot.tunnel_task_count,
        };
        if counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_err()
        {
            self.fail_slot(slot, "container_count_underflow");
        }
    }

    /// 业务作用：源 worker 处理 observer 请求并安装同代盗洞；请求完成权只属于源 worker。
    ///
    /// 参数说明：
    /// - `source`: 唯一允许扫描并改写其类型路由的源 slot。
    /// - `request`: observer 生成的目标、观察代次与 pending 所有权。
    ///
    /// 返回：已安装或可以明确拒绝时释放目标 pending 门禁并返回 true；
    /// 唯一阻断条件是当前严格任务尚未到达边界时保留请求并返回 false。
    pub(crate) fn handle_steal_request(
        &self,
        source: &Arc<PartitionSlot>,
        request: Arc<StealRequest>,
    ) -> bool {
        let target_index = request.target();
        let Some(target) = self.slots.get(target_index as usize) else {
            request.complete();
            return true;
        };
        if request.observation_epoch() > self.observation_epoch.load(Ordering::Acquire)
            || !target.accepting()
            || target.index() == source.index()
            || source.has_inbound_from(target.index())
        {
            request.complete();
            return true;
        }
        // 候选选择只由源 worker 读取本 slot 的类型表；observer 只提交目标和观察代次，
        // 因而不能从控制线程并发改写 TypeState 路由。
        let relaxed = source
            .type_states
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|state| {
                state.ordering() == TaskOrdering::Relaxed
                    && !state.failed()
                    && state.logical_count() >= self.config.idle_task_threshold
                    && !state.has_non_strict_target(target.index())
            })
            .max_by_key(|state| state.logical_count());
        let mut strict_waiting_for_boundary = false;
        let mut strict = source
            .type_states
            .iter()
            .map(|entry| entry.value().clone())
            .filter_map(|state| {
                let local_hotspot = state.ordering() == TaskOrdering::Strict
                    && !state.failed()
                    && state.logical_count() >= self.config.idle_task_threshold
                    && state
                        .strict_route()
                        .is_some_and(|route| route.state == StrictRouteState::Local);
                if !local_hotspot {
                    return None;
                }
                if state.strict_execution_idle() {
                    Some(state)
                } else {
                    strict_waiting_for_boundary = true;
                    None
                }
            })
            .collect::<Vec<_>>();
        strict.sort_by_key(|state| std::cmp::Reverse(state.logical_count()));

        let mut installed_any = false;
        if let Some(state) = relaxed {
            installed_any |= self.install_tunnel(source, target, &state);
        }
        // 非严格背景流量只消费一个普通机会；严格候选拥有独立固定次数，不能被同轮
        // relaxed 候选耗尽。目标入站上限仍在每次登记时原子裁决。
        for state in strict
            .into_iter()
            .take(self.config.strict_opportunity_attempts)
        {
            installed_any |= self.install_tunnel(source, target, &state);
        }
        if installed_any {
            target.wake_handle().notify_one();
        }
        if installed_any || !strict_waiting_for_boundary {
            request.complete();
            true
        } else {
            false
        }
    }

    /// 业务作用：由源 worker 为已经选定的同代类型安装一条指向目标的盗洞，目标登记先于
    /// 源路由发布，任一步竞争失败都会撤销孤立引用。
    ///
    /// 参数说明：
    /// - `source`: 类型固定的原始 slot。
    /// - `target`: observer 选定的空闲目标 slot。
    /// - `state`: 源 worker 已按顺序策略选定的类型状态。
    ///
    /// 返回：完整发布新盗洞并更新指标时返回 true；身份、容量或路由竞争失败返回 false。
    fn install_tunnel(
        &self,
        source: &Arc<PartitionSlot>,
        target: &Arc<PartitionSlot>,
        state: &Arc<TypeState>,
    ) -> bool {
        if state.runner_id() != self.id
            || state.generation() != self.generation
            || state.home() != source.index()
        {
            return false;
        }
        let Some(id) = self.next_tunnel_id() else {
            self.fail_control_plane("tunnel_id_exhausted");
            return false;
        };
        let installed = match state.ordering() {
            TaskOrdering::Relaxed => {
                let tunnel = NonStrictTunnel::new(
                    id,
                    self.id,
                    self.generation,
                    source.index(),
                    target.index(),
                    state.task_type(),
                    self.config.tunnel_lease,
                );
                if !target.register_non_strict(tunnel.clone()) {
                    false
                } else if state.add_non_strict_tunnel(tunnel.clone()) {
                    true
                } else {
                    // 目标登记必须先于源路由发布；第二步竞争失败时立即撤销目标，不能留下
                    // 没有源快照可达的孤立队列。
                    tunnel.close();
                    target.unregister_non_strict(tunnel.id());
                    false
                }
            }
            TaskOrdering::Strict => {
                let tunnel = StrictTunnel::new(
                    id,
                    self.id,
                    self.generation,
                    source.index(),
                    target.index(),
                    state.task_type(),
                );
                if !target.register_strict(tunnel.clone()) {
                    false
                } else if state.install_strict_tunnel(tunnel.clone()) {
                    true
                } else {
                    // 严格路由没有发布时目标不得保留可消费引用，否则后续迟到 worker 会看到
                    // 一个没有唯一源权威的盗洞。
                    tunnel.close_all();
                    target.unregister_strict(tunnel.id());
                    false
                }
            }
        };
        if installed {
            self.metrics.steal_successes.fetch_add(1, Ordering::Relaxed);
            match state.ordering() {
                TaskOrdering::Relaxed => {
                    self.metrics
                        .non_strict_tunnels
                        .fetch_add(1, Ordering::Relaxed);
                }
                TaskOrdering::Strict => {
                    self.metrics.strict_tunnels.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        installed
    }

    /// 业务作用：集中按本地逻辑负载选择繁忙源和空闲目标；已有同向借入时可复用目标的
    /// 执行能力，为后来出现的严格热点保留机会。observer 不扫描类型或直接改写路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；达到迁移阈值且请求门禁可取得时向源控制队列发布一次机会。
    pub(crate) fn observe_load(&self) {
        if self.phase() != RunnerPhase::Accepting || self.slots.len() < 2 {
            return;
        }
        let Some(source) = self
            .slots
            .iter()
            .filter(|slot| slot.accepting())
            .max_by_key(|slot| slot.local_task_load())
            .cloned()
        else {
            return;
        };
        if source.local_task_load() < self.config.idle_task_threshold {
            return;
        }
        let start = self.target_cursor.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let Some(target) = self
            .slots
            .iter()
            .cycle()
            .skip(start)
            .take(self.slots.len())
            .enumerate()
            .filter(|(_, slot)| {
                slot.accepting()
                    && slot.index() != source.index()
                    && !source.has_inbound_from(slot.index())
                    && (slot.load() <= self.config.idle_task_threshold
                        || slot.has_inbound_from(source.index()))
            })
            .min_by_key(|(offset, slot)| (slot.load(), *offset))
            .map(|(_, slot)| slot)
            .cloned()
        else {
            return;
        };
        let reuses_existing_direction = target.has_inbound_from(source.index());
        if !reuses_existing_direction && source.load() <= target.load() {
            return;
        }
        let Some(epoch) = self
            .observation_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .ok()
            .map(|value| value + 1)
        else {
            self.fail_control_plane("observation_epoch_exhausted");
            return;
        };
        let Some(request) = StealRequest::try_new(
            target.index(),
            epoch,
            self.steal_pending[target.index() as usize].clone(),
        ) else {
            return;
        };
        self.metrics.steal_attempts.fetch_add(1, Ordering::Relaxed);
        if !source.offer_control(request.clone()) {
            request.complete();
        }
    }

    /// 业务作用：由目标 worker 定向推进严格归还的观察、可逆准备、不可逆回写和追赶；
    /// 每个阶段只处理本 slot 已登记的严格盗洞，不扫描全 Runner 类型表。
    ///
    /// 参数说明：
    /// - `target`: 唯一消费待归还严格盗洞并推进状态机的目标 slot。
    ///
    /// 返回：无；每次调用只推进当前可证明阶段，失权时冻结最小严格类型与盗洞。
    pub(crate) fn progress_returns(self: &Arc<Self>, target: &Arc<PartitionSlot>) {
        for tunnel in target.strict_tunnels() {
            if tunnel.runner_id() != self.id
                || tunnel.generation() != self.generation
                || tunnel.target() != target.index()
            {
                self.fail_strict_tunnel(target, &tunnel, "strict_return_identity_mismatch");
                self.drain_failed_strict(target, &tunnel);
                continue;
            }
            let Some(source) = self.slots.get(tunnel.source() as usize) else {
                self.fail_strict_tunnel(target, &tunnel, "strict_return_source_missing");
                self.drain_failed_strict(target, &tunnel);
                continue;
            };
            let Some(state) = source
                .type_states
                .get(&tunnel.task_type())
                .map(|entry| entry.value().clone())
            else {
                self.fail_strict_tunnel(target, &tunnel, "strict_return_type_missing");
                self.drain_failed_strict(target, &tunnel);
                continue;
            };
            let Some(route) = state.strict_route() else {
                self.fail_strict_tunnel(target, &tunnel, "strict_return_route_missing");
                self.drain_failed_strict(target, &tunnel);
                continue;
            };
            if route.tunnel.as_ref().map(|value| value.id()) != Some(tunnel.id()) {
                self.drain_failed_strict(target, &tunnel);
                continue;
            }
            if self.lifecycle().mode() == LifecycleMode::ForceStop
                || route.state == StrictRouteState::Failed
            {
                self.drain_failed_strict(target, &tunnel);
                continue;
            }
            match route.state {
                StrictRouteState::Migrating => {}
                StrictRouteState::Stolen => {
                    let low_load = target.execution_available()
                        && state.strict_execution_idle()
                        && tunnel.executable_empty()
                        && state.logical_count() < self.config.idle_task_threshold;
                    if state.observe_return_candidate(low_load, tunnel.activity_epoch())
                        >= self.config.return_observations
                        && state.begin_strict_return().is_some()
                    {
                        // 先发布 ReturnPrepare 使新 producer 改投 staging，再停止目标消费；
                        // 此时尚未关闭 direct FIFO，条件消失仍可安全撤销。当前就是唯一目标
                        // worker，不自唤醒立即跨过准备态；下一 control tick 再复验旧 producer
                        // 与 staging 活动，使可逆准备窗口真实对并发提交可见。
                        tunnel.begin_return_prepare();
                    }
                }
                StrictRouteState::ReturnPrepare => {
                    if tunnel.staging_depth() > 0
                        || self.lifecycle().mode() != LifecycleMode::Running
                    {
                        // staging 出现新活动且尚无物理回写时撤销归还；旧 ReturnPrepare producer
                        // 离场后由目标按严格序号接管暂存前缀。
                        if !state.cancel_strict_return(tunnel.id()) {
                            self.fail_strict_tunnel(target, &tunnel, "strict_return_cancel_failed");
                        }
                        continue;
                    }
                    if !tunnel.direct_producers_stopped() || !state.strict_execution_idle() {
                        continue;
                    }
                    // 准备条件仍成立后才关闭 direct FIFO 并进入不可逆阶段；后续任何发布异常
                    // 都冻结该类型，不能退回目标执行。
                    tunnel.begin_return();
                    if !state.mark_strict_returning(tunnel.id()) {
                        self.fail_strict_tunnel(target, &tunnel, "strict_return_transition_failed");
                    }
                }
                StrictRouteState::Returning => {
                    let mut finished_direct = false;
                    for _ in 0..self.config.drain_batch {
                        match tunnel.poll_return(StrictQueue::Incremental) {
                            crate::queue::ConsumerPoll::Item { sequence, value } => {
                                if value
                                    .authority()
                                    .claim_container(Owner::Slot(target.index()))
                                    .is_err()
                                {
                                    self.record_authority_failure(
                                        value,
                                        "return_incremental_claim_failed",
                                    );
                                    continue;
                                }
                                if !self.move_to_main(
                                    target,
                                    state.home(),
                                    sequence,
                                    value.clone(),
                                    MoveStage::ReturnToSource,
                                ) {
                                    self.fail_before_running(
                                        &value,
                                        "return_incremental_publish_failed",
                                    );
                                    self.fail_strict_tunnel(
                                        target,
                                        &tunnel,
                                        "strict_return_publish_failed",
                                    );
                                    break;
                                }
                            }
                            crate::queue::ConsumerPoll::Poisoned { .. } => continue,
                            crate::queue::ConsumerPoll::Empty
                            | crate::queue::ConsumerPoll::Closed => {
                                finished_direct = true;
                                break;
                            }
                            crate::queue::ConsumerPoll::Reserved { .. } => break,
                            crate::queue::ConsumerPoll::Failed => {
                                self.fail_strict_tunnel(
                                    target,
                                    &tunnel,
                                    "strict_return_incremental_failed",
                                );
                                break;
                            }
                        }
                    }
                    if finished_direct && tunnel.direct_producers_stopped() {
                        if state.begin_local_catchup(tunnel.id()) {
                            // route 先切到 LocalCatchup，使新 producer 进入源主队列，再关闭
                            // staging 并等待旧 ReturnPrepare/Returning reservation 离场。
                            tunnel.begin_staging_drain();
                            source.wake_handle().notify_one();
                        } else {
                            self.fail_strict_tunnel(
                                target,
                                &tunnel,
                                "strict_local_catchup_transition_failed",
                            );
                        }
                    }
                }
                StrictRouteState::LocalCatchup => {
                    if !tunnel.staging_producers_stopped() {
                        continue;
                    }
                    let mut finished_staging = false;
                    for _ in 0..self.config.drain_batch {
                        match tunnel.poll_return(StrictQueue::Staging) {
                            crate::queue::ConsumerPoll::Item { sequence, value } => {
                                if value
                                    .authority()
                                    .claim_container(Owner::ReturnStaging)
                                    .is_err()
                                {
                                    self.record_authority_failure(
                                        value,
                                        "return_staging_claim_failed",
                                    );
                                    continue;
                                }
                                if !self.move_to_main(
                                    target,
                                    state.home(),
                                    sequence,
                                    value.clone(),
                                    MoveStage::ReturnToSource,
                                ) {
                                    self.fail_before_running(
                                        &value,
                                        "return_staging_publish_failed",
                                    );
                                    self.fail_strict_tunnel(
                                        target,
                                        &tunnel,
                                        "strict_return_publish_failed",
                                    );
                                    break;
                                }
                            }
                            crate::queue::ConsumerPoll::Poisoned { .. } => continue,
                            crate::queue::ConsumerPoll::Empty
                            | crate::queue::ConsumerPoll::Closed => {
                                finished_staging = true;
                                break;
                            }
                            crate::queue::ConsumerPoll::Reserved { .. } => break,
                            crate::queue::ConsumerPoll::Failed => {
                                self.fail_strict_tunnel(
                                    target,
                                    &tunnel,
                                    "strict_return_staging_failed",
                                );
                                break;
                            }
                        }
                    }
                    if finished_staging && state.finish_strict_return(tunnel.id()) {
                        tunnel.close_all();
                        target.unregister_strict(tunnel.id());
                        self.metrics.releases.fetch_add(1, Ordering::Relaxed);
                        self.metrics.strict_tunnels.fetch_sub(1, Ordering::Relaxed);
                    }
                }
                StrictRouteState::StolenCatchup => {
                    if !tunnel.staging_producers_stopped() || !target.execution_available() {
                        continue;
                    }
                    let mut caught_up = false;
                    for _ in 0..self.config.drain_batch {
                        if !target.execution_available() {
                            break;
                        }
                        match tunnel.poll_return(StrictQueue::Staging) {
                            crate::queue::ConsumerPoll::Item { value, .. } => {
                                if value
                                    .authority()
                                    .claim_return_staging(target.index())
                                    .is_err()
                                {
                                    self.record_authority_failure(
                                        value,
                                        "stolen_catchup_claim_failed",
                                    );
                                    continue;
                                }
                                self.dispatch_business(target.index(), value);
                            }
                            crate::queue::ConsumerPoll::Poisoned { .. } => continue,
                            crate::queue::ConsumerPoll::Empty
                            | crate::queue::ConsumerPoll::Closed => {
                                caught_up = true;
                                break;
                            }
                            crate::queue::ConsumerPoll::Reserved { .. } => break,
                            crate::queue::ConsumerPoll::Failed => {
                                self.fail_strict_tunnel(
                                    target,
                                    &tunnel,
                                    "strict_stolen_catchup_failed",
                                );
                                break;
                            }
                        }
                    }
                    if caught_up && state.finish_stolen_catchup(tunnel.id()) {
                        tunnel.resume_stolen_execution();
                        target.wake_handle().notify_one();
                    }
                }
                StrictRouteState::Local | StrictRouteState::Failed => {}
            }
        }
    }

    /// 业务作用：目标 worker 在严格类型失败或有损停止后排空三条 FIFO，并为每笔残留发布
    /// 稳定失败终态；全部 producer 与物理深度归零后才撤销目标引用。
    ///
    /// 参数说明：
    /// - `target`: 当前盗洞唯一 consumer slot。
    /// - `tunnel`: 已关闭或正在有损收口的严格盗洞。
    ///
    /// 返回：无；未离场 reservation 保持队头并由下一控制 tick 继续处理。
    fn drain_failed_strict(&self, target: &Arc<PartitionSlot>, tunnel: &Arc<StrictTunnel>) {
        tunnel.close_all();
        let failure_reason = tunnel.failure_reason().unwrap_or("strict_route_failed");
        for _ in 0..self.config.drain_batch {
            let (queue, poll) = tunnel.poll_failure();
            match poll {
                crate::queue::ConsumerPoll::Item { value, .. } => {
                    let owner = if queue == StrictQueue::Staging {
                        Owner::ReturnStaging
                    } else {
                        Owner::Slot(target.index())
                    };
                    if value.authority().claim_container(owner).is_err() {
                        if !value
                            .authority()
                            .state()
                            .is_ok_and(|entry_state| entry_state.is_terminal())
                        {
                            self.record_authority_failure(value, "strict_failure_claim_failed");
                        }
                        continue;
                    }
                    self.fail_before_running(&value, failure_reason);
                }
                crate::queue::ConsumerPoll::Poisoned { .. } => continue,
                crate::queue::ConsumerPoll::Reserved { .. } => break,
                crate::queue::ConsumerPoll::Failed => break,
                crate::queue::ConsumerPoll::Empty | crate::queue::ConsumerPoll::Closed => break,
            }
        }
        if tunnel.depth() == 0
            && tunnel.direct_producers_stopped()
            && tunnel.staging_producers_stopped()
            && target.unregister_strict(tunnel.id())
        {
            self.metrics.strict_tunnels.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// 业务作用：源 worker 在旧 Local producer 归零后冻结主队列边界，并在 consumer 到达后
    /// 发布 Stolen，目标由此获得执行 incremental 的顺序证明。
    ///
    /// 参数说明：
    /// - `source`: 当前唯一消费主队列的源 slot。
    /// - `consumer`: 本轮主队列消费后的单调边界。
    ///
    /// 返回：无；每个类型的状态变化由其路由 epoch 幂等裁决。
    pub(crate) fn progress_migrations(
        &self,
        source: &Arc<PartitionSlot>,
        consumer: crate::queue::ConsumerBoundary,
    ) {
        let states = source
            .type_states
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for state in states {
            let Some(route) = state.strict_route() else {
                continue;
            };
            if route.state != StrictRouteState::Migrating {
                continue;
            }
            let Some(tunnel) = route.tunnel else {
                state.fail();
                continue;
            };
            if source.main_producer_inflight() == 0
                && !tunnel.install_source_boundary(source.main_producer_boundary())
            {
                self.fail_strict_tunnel(source, &tunnel, "strict_source_boundary_mismatch");
                continue;
            }
            if tunnel.complete_migration(consumer) && state.finish_strict_migration(tunnel.id()) {
                self.slots[tunnel.target() as usize]
                    .wake_handle()
                    .notify_one();
            }
        }
    }

    /// 业务作用：目标 worker 在租约关闭且队列排空后同时撤销源快照和目标入站引用。
    ///
    /// 参数说明：
    /// - `target`: 当前盗洞唯一 consumer slot。
    /// - `tunnel`: 已关闭并取得 producer 与物理深度归零证明的盗洞。
    ///
    /// 返回：两侧引用首次完成撤销时返回 true。
    pub(crate) fn retire_non_strict_tunnel(
        &self,
        target: &Arc<PartitionSlot>,
        tunnel: &Arc<NonStrictTunnel>,
    ) -> bool {
        if !tunnel.drained()
            || tunnel.runner_id() != self.id
            || tunnel.generation() != self.generation
            || tunnel.target() != target.index()
        {
            return false;
        }
        let removed_source = self
            .slots
            .get(tunnel.source() as usize)
            .and_then(|source| source.type_states.get(&tunnel.task_type()))
            .is_some_and(|state| state.remove_non_strict_tunnel(tunnel.id()));
        let removed_target = target.unregister_non_strict(tunnel.id());
        if removed_target {
            self.metrics
                .non_strict_tunnels
                .fetch_sub(1, Ordering::Relaxed);
        }
        removed_source || removed_target
    }

    /// 业务作用：把归还暂存任务按 owner 移动协议回写源主队列，严格序号保持原值。
    ///
    /// 参数说明：
    /// - `source`: 当前摘取待归还任务的目标 slot。
    /// - `target`: 类型固定的原始 slot 编号。
    /// - `source_sequence`: 任务在归还 FIFO 中的物理序号证据。
    /// - `envelope`: 保留原严格受理序号的任务投影。
    /// - `stage`: 本次回写对应的权威移动阶段。
    ///
    /// 返回：主队列成功接管或任务进入失败收口时返回 true；无法预留目标时返回 false。
    fn move_to_main(
        self: &Arc<Self>,
        source: &Arc<PartitionSlot>,
        target: u32,
        source_sequence: u64,
        envelope: Arc<TaskEnvelope>,
        stage: MoveStage,
    ) -> bool {
        let Some(slot) = self.slots.get(target as usize).cloned() else {
            return false;
        };
        let Ok(reservation) = slot.reserve_main_transfer() else {
            return false;
        };
        let commit_slot = slot.clone();
        self.move_entry(
            source,
            target,
            source_sequence,
            envelope,
            stage,
            LogicalContainer::Main,
            reservation,
            move |published| {
                if published {
                    commit_slot.commit_main();
                } else {
                    commit_slot.abort_main_reserved();
                }
            },
        )
    }

    /// 业务作用：关闭失败严格盗洞和对应类型，避免队列失权后继续直投。
    ///
    /// 参数说明：
    /// - `_slot`: 发现异常的 worker slot，用于保留调用边界中的责任归属。
    /// - `tunnel`: 必须关闭并由目标继续排空的严格盗洞。
    /// - `reason`: 首次触发隔离的静态原因码，后续残留任务沿用该根因。
    ///
    /// 返回：无；关闭路由后仅唤醒目标唯一 consumer，不会提前撤销含残留任务的引用。
    pub(crate) fn fail_strict_tunnel(
        &self,
        _slot: &Arc<PartitionSlot>,
        tunnel: &Arc<StrictTunnel>,
        reason: &'static str,
    ) {
        tunnel.record_failure(reason);
        tunnel.close_all();
        if let Some(source) = self.slots.get(tunnel.source() as usize) {
            for state in source.type_states.iter() {
                if state
                    .strict_route()
                    .and_then(|route| route.tunnel)
                    .is_some_and(|current| current.id() == tunnel.id())
                    && state.fail_strict_route(tunnel.id())
                {
                    self.metrics.failed_types.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if let Some(target) = self.slots.get(tunnel.target() as usize) {
            // 失败盗洞仍由目标唯一 worker 持有，直到三条 FIFO 和 reservation 全部排空；
            // 提前撤销会把已受理任务及其全局许可永久留在无人可达的关闭队列中。
            target.wake_handle().notify_one();
        }
    }

    /// 业务作用：单向隔离 slot；最后一个可接纳 slot 失效前先关闭 Runner 总入口并请求失败收口。
    ///
    /// 参数说明：
    /// - `slot`: 失去安全消费或计数证明的最小分区故障域。
    /// - `reason`: 最后一个 slot 失效时升级控制面所用的静态原因码。
    ///
    /// 返回：无；重复隔离幂等，仍有可接纳 slot 时其它分区继续服务。
    pub(crate) fn fail_slot(&self, slot: &Arc<PartitionSlot>, reason: &'static str) {
        if !slot.fail() {
            return;
        }
        self.metrics.dead_workers.fetch_add(1, Ordering::Relaxed);
        let remaining = self.accepting_slots.fetch_sub(1, Ordering::AcqRel) - 1;
        if remaining == 0 {
            self.fail_control_plane(reason);
        }
    }

    /// 业务作用：控制面失权时先关闭数据入口，再安装稳定原因并升级同一停止 operation。
    ///
    /// 参数说明：
    /// - `reason`: 记录当前代失权来源的静态原因码。
    ///
    /// 返回：无；当前代单调进入有损停止，不影响其它命名 Runner。
    fn fail_control_plane(&self, reason: &'static str) {
        *self.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
        self.phase.store(PHASE_STOPPING, Ordering::Release);
        self.request_force_stop(reason);
    }

    /// 业务作用：首次无损停止发布基线、关闭 Runner 准入和所有容量等待者，supervisor 持续收口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；重复请求复用首次 operation，不重置报告基线或等待预算。
    fn request_graceful_stop(&self) {
        if self.operation.started.swap(true, Ordering::AcqRel) {
            return;
        }
        self.capture_stop_baseline();
        // 先关 Runner 阶段，再关闭预算与 slot producer，迟到提交只能完整拒绝。
        self.phase.store(PHASE_STOPPING, Ordering::SeqCst);
        self.lifecycle().request_graceful_stop();
        self.close_admission();
    }

    /// 业务作用：把现有 operation 单调升级为有损模式；Drop 和显式 force 共用同一权威。
    ///
    /// 参数说明：
    /// - `_reason`: 发起有损收口的静态原因码，实际失败原因由最先失权路径稳定保存。
    ///
    /// 返回：无；关闭全部准入并通知 supervisor 进入强制处置。
    fn request_force_stop(&self, _reason: &'static str) {
        if !self.operation.started.swap(true, Ordering::AcqRel) {
            self.capture_stop_baseline();
            self.phase.store(PHASE_STOPPING, Ordering::SeqCst);
        }
        self.operation
            .force_requested
            .store(true, Ordering::Release);
        self.lifecycle().request_force_stop();
        self.close_admission();
    }

    /// 业务作用：保存停机开始累计量基线，最终报告只统计收口阶段发生的处置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；基线通过 `OnceLock` 只记录首次停止请求时的累计值。
    fn capture_stop_baseline(&self) {
        let _ = self.operation.baseline.set(StopBaseline {
            completed: self.metrics.completed.load(Ordering::Acquire),
            cancelled: self.metrics.cancelled.load(Ordering::Acquire),
            aborted: self.metrics.aborted.load(Ordering::Acquire),
            frozen: self.metrics.frozen.load(Ordering::Acquire),
        });
    }

    /// 业务作用：关闭全部提交等待点、延迟索引和物理 producer，并唤醒 worker 进入排空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；已经受理的物理任务保留给唯一 consumer，未到期延迟任务转为稳定拒绝。
    fn close_admission(&self) {
        self.global.close();
        for slot in self.slots.iter() {
            for state in slot.type_states.iter() {
                state.queued_budget().close();
            }
            slot.close();
        }
        for id in self.timer.close_and_drain() {
            self.reject_delayed_shutdown(id);
        }
        self.observer_wake.notify_waiters();
        self.observer_wake.notify_one();
        self.lifecycle().notify_progress();
    }

    /// 业务作用：停止路径把未到期 delayed 任务发布为稳定拒绝并归还全局许可。
    ///
    /// 参数说明：
    /// - `id`: 从 timer 关闭结果取得的延迟任务 ID。
    ///
    /// 返回：无；索引已被取消路径摘除时幂等退出，否则完成拒绝终态结算。
    fn reject_delayed_shutdown(&self, id: u64) {
        let Some(delayed) = self
            .delayed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
        else {
            return;
        };
        let rejected = delayed
            .entry
            .fail_from(
                EntryState::Delayed,
                EntryState::Rejected,
                "shutdown_before_expiry",
            )
            .is_ok();
        if rejected {
            let _ = delayed.entry.finalize_termination(|_| {});
            self.metrics
                .rejected_shutting_down
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 业务作用：延迟任务到期后在建立 TypeState 之前遇到稳定拒绝条件，直接结算其全局许可
    /// 与业务载荷；此时不存在类型或逻辑容器计数。
    ///
    /// 参数说明：
    /// - `entry`: 已从 delayed 索引取得唯一责任的任务权威。
    /// - `reason`: 可观察的稳定拒绝原因。
    ///
    /// 返回：本路径取得拒绝终态权威时返回 true；竞争取消已先取得权威时返回 false，
    /// 调用方不得把取消重复计入拒绝分类。
    fn reject_unbound_delayed(
        &self,
        entry: &Arc<crate::entry::TaskEntry<Job>>,
        reason: &'static str,
    ) -> bool {
        let rejected = entry
            .fail_from(EntryState::Enqueueing, EntryState::Rejected, reason)
            .is_ok();
        if rejected {
            let _ = entry.finalize_termination(|_| {});
        }
        self.lifecycle().notify_progress();
        rejected
    }

    /// 业务作用：未绑定 TypeState 的延迟任务在结算权威损坏时关闭当前 Runner 控制面，避免
    /// 没有可归属最小类型故障域的任务静默悬挂。
    ///
    /// 参数说明：
    /// - `reason`: 稳定内部失败原因。
    ///
    /// 返回：无；同一停止 operation 被单调升级为有损模式。
    pub(crate) fn fail_unbound_delayed(&self, reason: &'static str) {
        self.fail_control_plane(reason);
    }

    /// 业务作用：observer 仅在本代 Accepting 且 lifecycle 仍 Running 时继续扫描。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：准入开放且生命周期未进入停止时返回 true。
    pub(crate) fn observer_should_run(&self) -> bool {
        self.phase() == RunnerPhase::Accepting && self.lifecycle().mode() == LifecycleMode::Running
    }

    /// 业务作用：只有生命周期已经离开 Running 时 observer 才退出；Starting 期间必须等待
    /// Accepting 发布，不能把启动调度先后误判为停止。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：生命周期已进入无损或有损停止时返回 true。
    pub(crate) fn observer_should_exit(&self) -> bool {
        self.lifecycle().mode() != LifecycleMode::Running
    }

    /// 业务作用：向 slot worker 暴露当前单调生命周期模式，不开放修改权威。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前代的 `Running`、`GracefulStop` 或 `ForceStop` 模式。
    pub(crate) fn lifecycle_mode(&self) -> LifecycleMode {
        self.lifecycle().mode()
    }

    /// 业务作用：取得 observer 独立唤醒点，停止时无需等待完整扫描间隔。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前代 observer 使用的共享通知句柄。
    pub(crate) fn observer_wake(&self) -> Arc<Notify> {
        self.observer_wake.clone()
    }

    /// 业务作用：worker 在关门、producer 归零且全部物理容器排空后才允许正常退出。
    ///
    /// 参数说明：
    /// - `_slot`: 发起退出判断的 worker slot，排空门禁按整个 generation 统一裁决。
    ///
    /// 返回：当前代已关闭准入且提交者、延迟表、移动表和全部 slot 均归零时返回 true。
    pub(crate) fn worker_should_exit(&self, _slot: &PartitionSlot) -> bool {
        matches!(
            self.phase(),
            RunnerPhase::Stopping | RunnerPhase::Stopped | RunnerPhase::Failed
        ) && self.producers.load(Ordering::SeqCst) == 0
            && self
                .delayed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            && self
                .moving
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
            && self.slots.iter().all(|slot| slot.queues_empty())
    }

    /// 业务作用：读取健康状态；隔离失败只影响当前 Runner，不更改注册表其它对象。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：结合生命周期、可接纳 slot 和失败类型数量得到的 readiness 状态。
    fn health(&self) -> RunnerHealth {
        match self.phase() {
            RunnerPhase::Starting => RunnerHealth::Starting,
            RunnerPhase::Accepting => {
                if self.accepting_slots.load(Ordering::Acquire) < self.slots.len()
                    || self.metrics.failed_types.load(Ordering::Acquire) > 0
                {
                    RunnerHealth::Degraded
                } else {
                    RunnerHealth::Healthy
                }
            }
            RunnerPhase::Stopping => {
                if self
                    .failure
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some()
                {
                    RunnerHealth::Failed
                } else {
                    RunnerHealth::Stopping
                }
            }
            RunnerPhase::Failed => RunnerHealth::Failed,
            RunnerPhase::Stopped => RunnerHealth::Stopped,
        }
    }

    /// 业务作用：汇总当前 generation 计数与资源瞬时量，不持有全局一致性锁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含累计事件、瞬时容量、物理深度、严格路由和监督任务数量的独立快照。
    fn metrics_snapshot(&self) -> RunnerMetricsSnapshot {
        let admit_available = self.global.available_permits() as u64;
        let delayed_pending = self.timer.logical_len() as u64;
        let running = self.metrics.running.load(Ordering::Acquire);
        // 独立排队账本只在双层准入完成后建立，容量等待者即使暂持类型许可也不会冒充
        // 已受理任务；每个 TaskEnvelope 的原子结算位保证跨终态帮助只减少一次。
        let queued_depth = self
            .slots
            .iter()
            .flat_map(|slot| slot.type_states.iter())
            .map(|state| state.queued_count() as u64)
            .fold(0_u64, u64::saturating_add);
        let mut main_queue_depth = 0_u64;
        let mut control_queue_depth = 0_u64;
        let mut reserved_queue_heads = 0_u64;
        let mut producer_inflight = self.producers.load(Ordering::Acquire) as u64;
        let mut strict_stock_depth = 0_u64;
        let mut strict_incremental_depth = 0_u64;
        let mut strict_staging_depth = 0_u64;
        let mut non_strict_tunnels_open = 0_u64;
        let mut non_strict_tunnels_draining = 0_u64;
        let mut strict_routes = [0_u64; 8];
        for slot in self.slots.iter() {
            let snapshot = slot.metrics_snapshot();
            main_queue_depth = main_queue_depth.saturating_add(snapshot.main_depth);
            control_queue_depth = control_queue_depth.saturating_add(snapshot.control_depth);
            reserved_queue_heads = reserved_queue_heads.saturating_add(snapshot.reserved_heads);
            producer_inflight = producer_inflight.saturating_add(snapshot.producer_inflight);
            strict_stock_depth = strict_stock_depth.saturating_add(snapshot.stock_depth);
            strict_incremental_depth =
                strict_incremental_depth.saturating_add(snapshot.incremental_depth);
            strict_staging_depth = strict_staging_depth.saturating_add(snapshot.staging_depth);
            non_strict_tunnels_open =
                non_strict_tunnels_open.saturating_add(snapshot.non_strict_open);
            non_strict_tunnels_draining =
                non_strict_tunnels_draining.saturating_add(snapshot.non_strict_draining);
            for (total, current) in strict_routes.iter_mut().zip(snapshot.strict_routes) {
                *total = total.saturating_add(current);
            }
        }
        let (frozen_evidence_samples, frozen_evidence_overwritten) = self.frozen.counts();
        RunnerMetricsSnapshot {
            phase: self.phase(),
            health: self.health(),
            epoch: self.generation,
            partitions: self.slots.len() as u64,
            accepting_partitions: self.accepting_slots.load(Ordering::Acquire) as u64,
            failed_partitions: (self.slots.len() - self.accepting_slots.load(Ordering::Acquire))
                as u64,
            submitted: self.metrics.submitted.load(Ordering::Acquire),
            completed: self.metrics.completed.load(Ordering::Acquire),
            cancelled: self.metrics.cancelled.load(Ordering::Acquire),
            failed: self.metrics.failed.load(Ordering::Acquire),
            task_panics: self.metrics.task_panics.load(Ordering::Acquire),
            aborted: self.metrics.aborted.load(Ordering::Acquire),
            rejected_shutting_down: self.metrics.rejected_shutting_down.load(Ordering::Acquire),
            rejected_queue_full: self.metrics.rejected_queue_full.load(Ordering::Acquire),
            rejected_overloaded: self.metrics.rejected_overloaded.load(Ordering::Acquire),
            rejected_ordering_conflict: self
                .metrics
                .rejected_ordering_conflict
                .load(Ordering::Acquire),
            rejected_lane_failed: self.metrics.rejected_type_failed.load(Ordering::Acquire),
            rejected_lane_limit: self.metrics.rejected_type_limit.load(Ordering::Acquire),
            rejected_reserved_type: self.metrics.rejected_reserved_type.load(Ordering::Acquire),
            steal_attempts: self.metrics.steal_attempts.load(Ordering::Acquire),
            steal_successes: self.metrics.steal_successes.load(Ordering::Acquire),
            releases: self.metrics.releases.load(Ordering::Acquire),
            frozen: self.metrics.frozen.load(Ordering::Acquire),
            lanes: self
                .slots
                .iter()
                .map(|slot| slot.type_states.len() as u64)
                .sum(),
            type_state_limit: self.type_limit.load(Ordering::Acquire) as u64,
            failed_lanes: self.metrics.failed_types.load(Ordering::Acquire),
            dead_workers: self.metrics.dead_workers.load(Ordering::Acquire),
            queued_depth,
            main_queue_depth,
            control_queue_depth,
            reserved_queue_heads,
            producer_inflight,
            admit_available,
            delayed_pending,
            timer_physical_slots: self.timer.physical_len() as u64,
            timer_cancelled: self.timer.cancelled_count(),
            timer_compactions: self.timer.compaction_count(),
            running,
            moving: self.metrics.moving.load(Ordering::Acquire),
            non_strict_tunnels: self.metrics.non_strict_tunnels.load(Ordering::Acquire),
            strict_tunnels: self.metrics.strict_tunnels.load(Ordering::Acquire),
            non_strict_tunnels_open,
            non_strict_tunnels_draining,
            strict_stock_depth,
            strict_incremental_depth,
            strict_staging_depth,
            strict_local: strict_routes[0],
            strict_migrating: strict_routes[1],
            strict_stolen: strict_routes[2],
            strict_return_prepare: strict_routes[3],
            strict_returning: strict_routes[4],
            strict_local_catchup: strict_routes[5],
            strict_stolen_catchup: strict_routes[6],
            strict_failed: strict_routes[7],
            frozen_evidence_samples,
            frozen_evidence_overwritten,
            supervised_tasks: self.lifecycle().child_count() as u64,
        }
    }

    /// 业务作用：签发 generation 内不可复用任务标识，耗尽时关闭控制面。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仍有编号空间时返回当前唯一 ID；计数耗尽时返回持久执行域拒绝。
    fn next_task_id(&self) -> Result<u64, SubmitRejection> {
        self.next_task_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| SubmitRejection::LaneFailed)
    }

    /// 业务作用：签发 generation 内不可复用盗洞标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仍有编号空间时返回当前唯一 ID；计数耗尽时返回 `None`。
    fn next_tunnel_id(&self) -> Option<u64> {
        self.next_tunnel_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .ok()
    }
}

/// 业务作用：保护一次提交从接纳复验到物理发布的临界区，并在离场时归还 producer 计数。
struct ProducerGuard {
    inner: Arc<RunnerInner>,
}

impl ProducerGuard {
    /// 业务作用：按“读 Accepting、登记 producer、复验同代”的顺序进入提交临界区。
    ///
    /// 参数说明：
    /// - `inner`: 需要保护准入与物理发布竞态的当前 generation。
    ///
    /// 返回：两次阶段复验一致时返回 RAII guard；停机与登记交错时回滚计数并拒绝提交。
    fn enter(inner: &Arc<RunnerInner>) -> Result<Self, SubmitRejection> {
        if inner.phase() != RunnerPhase::Accepting {
            inner
                .metrics
                .rejected_shutting_down
                .fetch_add(1, Ordering::Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        inner.producers.fetch_add(1, Ordering::SeqCst);
        if inner.phase() != RunnerPhase::Accepting {
            inner.producers.fetch_sub(1, Ordering::SeqCst);
            inner
                .metrics
                .rejected_shutting_down
                .fetch_add(1, Ordering::Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        Ok(Self {
            inner: inner.clone(),
        })
    }
}

impl Drop for ProducerGuard {
    /// 业务作用：提交任意返回或展开路径离场时归还 producer 计数并唤醒停止 supervisor。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；producer 计数恰好减少一次并发布生命周期进度。
    fn drop(&mut self) {
        self.inner.producers.fetch_sub(1, Ordering::SeqCst);
        self.inner.lifecycle().notify_progress();
    }
}

enum Placement {
    Main {
        slot: Arc<PartitionSlot>,
        reservation: Reservation<Arc<TaskEnvelope>>,
    },
    NonStrict {
        tunnel: Arc<NonStrictTunnel>,
        target: u32,
        reservation: Reservation<Arc<TaskEnvelope>>,
    },
    Strict {
        tunnel: Arc<StrictTunnel>,
        target: u32,
        queue: StrictQueue,
        reservation: Reservation<Arc<TaskEnvelope>>,
    },
}

impl Placement {
    /// 业务作用：读取物理落点 slot，作为任务 owner。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：主队列所属 slot 或盗洞唯一目标 slot。
    fn target(&self) -> u32 {
        match self {
            Self::Main { slot, .. } => slot.index(),
            Self::NonStrict { target, .. } | Self::Strict { target, .. } => *target,
        }
    }

    /// 业务作用：把物理落点转换成 TaskEntry owner；归还 staging 使用专属 owner，目标只在
    /// 撤销归还或回写时通过协议取得责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：归还暂存返回 `ReturnStaging`，其它落点返回对应 `Slot`。
    fn owner(&self) -> Owner {
        match self {
            Self::Strict {
                queue: StrictQueue::Staging,
                ..
            } => Owner::ReturnStaging,
            _ => Owner::Slot(self.target()),
        }
    }

    /// 业务作用：构造初始逻辑 owner，route epoch 由物理容器身份稳定区分。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与主队列、非严格盗洞或严格阶段 FIFO 一致的初始逻辑归属。
    fn logical_owner(&self) -> LogicalOwner {
        let (slot, container) = match self {
            Self::Main { slot, .. } => (slot.index(), LogicalContainer::Main),
            Self::NonStrict { target, .. } => (*target, LogicalContainer::NonStrictTunnel),
            Self::Strict { target, queue, .. } => (
                *target,
                match queue {
                    StrictQueue::Stock => LogicalContainer::StrictStock,
                    StrictQueue::Incremental => LogicalContainer::StrictIncremental,
                    StrictQueue::Staging => LogicalContainer::ReturnStaging,
                },
            ),
        };
        LogicalOwner {
            slot,
            route_epoch: 0,
            container,
        }
    }

    /// 业务作用：在任务 owner、逻辑计数和 Queued 状态稳定后完成唯一物理发布并提交深度。
    ///
    /// 参数说明：
    /// - `envelope`: 已冻结身份、owner、逻辑归属与容量许可的任务投影。
    ///
    /// 返回：物理载荷发布成功时返回 Ok；预留队列失权时撤销深度、关闭最小
    /// 故障域并返回尚未交给 consumer 的载荷。
    fn publish(self, envelope: Arc<TaskEnvelope>) -> Result<(), Arc<TaskEnvelope>> {
        match self {
            Self::Main { slot, reservation } => {
                if let Err(error) = reservation.publish(envelope) {
                    slot.abort_main_reserved();
                    return Err(error.into_inner());
                }
                slot.commit_main();
            }
            Self::NonStrict {
                tunnel,
                reservation,
                ..
            } => {
                if let Err(error) = reservation.publish(envelope) {
                    tunnel.abort_reserved();
                    return Err(error.into_inner());
                }
                tunnel.commit_reserved();
            }
            Self::Strict {
                tunnel,
                queue,
                reservation,
                ..
            } => {
                if let Err(error) = reservation.publish(envelope) {
                    tunnel.abort_reserved(queue);
                    return Err(error.into_inner());
                }
                tunnel.commit_reserved(queue);
            }
        }
        Ok(())
    }
}

/// 业务作用：执行单笔业务 Future，严格门禁、panic 边界和终态结算都不占用 slot worker。
///
/// 参数说明：
/// - `inner`: 当前 Runner generation 内核。
/// - `owner`: 为该任务提供执行位与唤醒责任的 slot。
/// - `envelope`: 已脱离物理容器且等待或已取得运行权的任务。
/// - `execution`: 已立即取得的 slot 执行位；空值表示需要在受监督边界内等待。
/// - `strict_execution`: 已取得的类型严格执行权；非严格任务为空。
/// - `_strict_handling`: 严格任务脱离容器后阻止路由切相的处理责任。
///
/// 返回：无；任务在正常、取消、panic 或有损停止路径均发布唯一稳定终态。
async fn run_business(
    inner: Arc<RunnerInner>,
    owner: u32,
    envelope: Arc<TaskEnvelope>,
    execution: Option<OwnedSemaphorePermit>,
    strict_execution: Option<StrictExecutionLease>,
    _strict_handling: Option<StrictHandlingLease>,
) {
    let mut backstop = BusinessBackstop {
        inner: inner.clone(),
        envelope: envelope.clone(),
        running: false,
        armed: true,
    };
    let _execution_wake = SlotExecutionWake {
        wake: inner.slots[owner as usize].wake_handle(),
    };
    if let Some(sequence) = envelope.strict_sequence() {
        envelope.type_state().wait_strict_turn(sequence).await;
    }
    if envelope
        .authority()
        .state()
        .is_ok_and(|state| state.is_terminal())
    {
        backstop.armed = false;
        return;
    }
    let _strict_execution = if envelope.type_state().ordering() == TaskOrdering::Strict {
        match strict_execution {
            Some(execution) => Some(execution),
            None => match envelope.type_state().acquire_strict_execution().await {
                Some(execution) => Some(execution),
                None => {
                    inner.fail_before_running(&envelope, "strict_execution_gate_closed");
                    backstop.armed = false;
                    return;
                }
            },
        }
    } else {
        None
    };
    let _execution = match execution {
        Some(execution) => execution,
        None => match inner.slots[owner as usize].acquire_execution().await {
            Ok(execution) => execution,
            Err(_) => {
                inner.fail_before_running(&envelope, "slot_execution_gate_closed");
                backstop.armed = false;
                return;
            }
        },
    };
    if envelope.authority().state() == Ok(EntryState::Terminating) {
        inner.finalize_terminal(&envelope, false, false);
        backstop.armed = false;
        return;
    }
    let job = match envelope.authority().take_for_running(owner, || {
        inner.settle_queued_projection(&envelope);
    }) {
        Ok(job) => job,
        Err(_) => {
            inner.record_authority_failure(envelope, "running_claim_failed");
            backstop.armed = false;
            return;
        }
    };
    backstop.running = true;
    inner.metrics.running.fetch_add(1, Ordering::Relaxed);
    // 恢复提交时捕获的链路作用域:业务 Future 内未显式绑定的出站调用延续提交方 trace;
    // None 时零成本直通,不为无链路任务制造新根。
    let job = natelemetry::with_ambient(envelope.authority().trace(), job);
    let outcome = AssertUnwindSafe(job).catch_unwind().await;
    let panicked = outcome.is_err();
    match outcome {
        Ok(()) => {
            let _ = envelope
                .authority()
                .finish_running(EntryState::Completed, None);
        }
        Err(payload) => {
            inner.metrics.task_panics.fetch_add(1, Ordering::Relaxed);
            crate::run_isolated("业务 Future panic payload 析构", move || drop(payload));
            let _ = envelope
                .authority()
                .finish_running(EntryState::Failed, Some("task_panicked"));
        }
    }
    inner.finalize_terminal(&envelope, panicked, false);
    // 终态结算先归还全局许可；running 份额随后解除，非事务性指标快照最多短暂低估
    // 排队量，不会在交接窗口把同一任务重复投影到 queued。
    inner.metrics.running.fetch_sub(1, Ordering::Relaxed);
    backstop.running = false;
    backstop.armed = false;
    inner.lifecycle().notify_progress();
}

/// 业务作用：在 slot 执行许可归还时唤醒对应 worker，避免已排队任务等待周期控制 tick。
struct SlotExecutionWake {
    wake: Arc<Notify>,
}

impl Drop for SlotExecutionWake {
    /// 业务作用：业务边界离场并归还 slot 执行许可后唤醒唯一 worker，使已排队任务无需依赖
    /// 周期轮询才能继续取得执行位。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；向对应 slot worker 发布一次执行位可用通知。
    fn drop(&mut self) {
        self.wake.notify_one();
    }
}

/// 业务作用：在业务 Future 异常离场时帮助发布终态并归还执行账目，避免任务永久悬挂。
struct BusinessBackstop {
    inner: Arc<RunnerInner>,
    envelope: Arc<TaskEnvelope>,
    running: bool,
    armed: bool,
}

impl Drop for BusinessBackstop {
    /// 业务作用：业务 task 被有损停止中止或异常离场时发布稳定失败终态，句柄不得永久等待。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；仅在仍 armed 时取得或帮助终态权威并完成资源结算。
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.envelope.authority().state() == Ok(EntryState::Terminating) {
            self.inner.finalize_terminal(&self.envelope, false, false);
            if self.running {
                self.inner.metrics.running.fetch_sub(1, Ordering::Relaxed);
                self.running = false;
                self.inner.lifecycle().notify_progress();
            }
            return;
        }
        let expected = if self.running {
            EntryState::Running
        } else {
            EntryState::Queued
        };
        let reason = if self.running {
            "aborted_during_shutdown"
        } else {
            "shutdown_frozen"
        };
        if self
            .envelope
            .authority()
            .fail_from(expected, EntryState::Failed, reason)
            .is_ok()
        {
            self.inner
                .finalize_terminal(&self.envelope, !self.running, self.running);
        }
        if self.running {
            // 有损中止与正常完成遵守同一观测顺序：先结算终态和全局许可，再解除
            // running 份额，避免瞬时排队深度越过真实容量。
            self.inner.metrics.running.fetch_sub(1, Ordering::Relaxed);
            self.running = false;
            self.inner.lifecycle().notify_progress();
        }
    }
}

/// 业务作用：常驻 supervisor 在 Running 期回收已结束业务句柄，停止期关闭资源并取得全部 join 证明。
///
/// 参数说明：
/// - `inner`: 当前 generation 唯一数据面、子任务注册表与停止 operation。
///
/// 返回：无；只有生产者、延迟、移动和全部受监督子任务均取得退出证明后才发布终局并退出。
async fn supervise(inner: Arc<RunnerInner>) {
    loop {
        inner.audit_moves();
        for id in inner.lifecycle().finished_child_ids() {
            if let Some(lease) = inner.lifecycle().lease_child(id) {
                let _ = lease.join().await;
            }
        }
        if inner.phase() == RunnerPhase::Stopping {
            if inner.lifecycle().mode() == LifecycleMode::ForceStop {
                inner.lifecycle().abort_kind(ChildTaskKind::Business);
                inner.lifecycle().abort_kind(ChildTaskKind::Observer);
                inner.lifecycle().abort_kind(ChildTaskKind::Timer);
                for slot in inner.slots.iter() {
                    let wake = slot.wake_handle();
                    wake.notify_waiters();
                    wake.notify_one();
                }
            }
            if inner.producers.load(Ordering::SeqCst) == 0
                && inner.lifecycle().child_count() == 0
                && inner
                    .delayed
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty()
                && inner
                    .moving
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty()
            {
                let report = build_shutdown_report(&inner);
                let failed = inner
                    .failure
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some();
                inner.phase.store(
                    if failed {
                        PHASE_FAILED
                    } else {
                        PHASE_STOPPED
                    },
                    Ordering::Release,
                );
                inner.operation.finish(report);
                return;
            }
        }
        let revision = inner.lifecycle().revision();
        let lifecycle = inner.lifecycle();
        let needs_control_tick = inner.phase() == RunnerPhase::Stopping
            || !inner
                .moving
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty();
        if needs_control_tick {
            tokio::select! {
                _ = lifecycle.changed_since(revision) => {}
                _ = tokio::time::sleep(inner.config.control_tick) => {}
            }
        } else {
            // 稳态没有 Moving 审计或停止动作时只等待真实变化，避免每个空闲 Runner
            // 都按快速控制间隔唤醒 supervisor。
            lifecycle.changed_since(revision).await;
        }
    }
}

/// 业务作用：在 supervisor 最外层捕获内部展开并复用同一 generation 权威继续最小收口，
/// 公开等待者不会收到裸 JoinError，也不会并发建立第二个清理者。
///
/// 参数说明：
/// - `inner`: 当前 generation 唯一数据面与停止 operation。
///
/// 返回：正常取得全部子任务退出证明后返回；内部展开会先关闭入口并在同一任务内重试收口。
async fn supervise_guarded(inner: Arc<RunnerInner>) {
    loop {
        let outcome = AssertUnwindSafe(supervise(inner.clone()))
            .catch_unwind()
            .await;
        if outcome.is_ok() {
            return;
        }
        // supervisor 已失去正常推进路径，先关门并升级有损模式，再由当前唯一监督任务继续
        // 回收注册表内仍有 AbortHandle 和 JoinHandle 的子任务。
        inner.fail_control_plane("supervisor_panicked");
    }
}

/// 业务作用：从停机基线和终局累计量构造损耗报告，避免阶段切换后的处置漏计。
///
/// 参数说明：
/// - `inner`: 已完成或正在完成收口的 generation 数据面。
///
/// 返回：仅统计停机基线之后排空、取消、中止和冻结数量的独立报告。
fn build_shutdown_report(inner: &RunnerInner) -> ShutdownReport {
    let baseline = inner.operation.baseline.get().unwrap_or(&StopBaseline {
        completed: 0,
        cancelled: 0,
        aborted: 0,
        frozen: 0,
    });
    let frozen = inner
        .metrics
        .frozen
        .load(Ordering::Acquire)
        .saturating_sub(baseline.frozen);
    ShutdownReport {
        drained: inner
            .metrics
            .completed
            .load(Ordering::Acquire)
            .saturating_sub(baseline.completed),
        cancelled: inner
            .metrics
            .cancelled
            .load(Ordering::Acquire)
            .saturating_sub(baseline.cancelled),
        aborted: inner
            .metrics
            .aborted
            .load(Ordering::Acquire)
            .saturating_sub(baseline.aborted),
        frozen,
        timed_out_lanes: if inner.operation.force_requested.load(Ordering::Acquire) && frozen > 0 {
            inner
                .slots
                .iter()
                .map(|slot| slot.type_states.len() as u32)
                .sum()
        } else {
            0
        },
        unconverged_strict_types: 0,
        unconverged_slots: 0,
        unconverged_timers: 0,
    }
}

/// 业务作用：在共享期限内取得 supervisor 自身的 join 证明，并允许并发等待者复用结果。
///
/// 参数说明：
/// - `inner`: 提供 supervisor epoch、租约和 join 句柄的 generation 内核。
/// - `deadline`: 全部竞争等待者共同遵守的绝对期限。
///
/// 返回：supervisor 已无现役 epoch 或期限内 join 成功时返回 true；超时或 JoinError 时返回 false。
async fn join_supervisor_until(inner: &Arc<RunnerInner>, deadline: Instant) -> bool {
    loop {
        if inner.generation_control.supervisor_epoch().is_none() {
            return true;
        }
        if let Some(lease) = inner.generation_control.lease_supervisor() {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(value) => value,
                None => return false,
            };
            return tokio::time::timeout(remaining, lease.join())
                .await
                .is_ok_and(|result| result.is_ok());
        }
        let remaining = match deadline.checked_duration_since(Instant::now()) {
            Some(value) => value,
            None => return false,
        };
        if tokio::time::timeout(remaining.min(Duration::from_millis(2)), async {
            tokio::task::yield_now().await
        })
        .await
        .is_err()
        {
            return false;
        }
    }
}

/// 业务作用：等待停止 operation 终局，并在唯一 supervisor 异常离场时先取得旧句柄 join
/// 证明，再用新 epoch 建立同 generation 的替代监督者继续收口。
///
/// 参数说明：
/// - `inner`: 当前停止 operation 与 generation 控制权。
/// - `deadline`: 本次公开等待共享的绝对期限。
///
/// 返回：期限内取得稳定报告时返回报告；仍缺少 supervisor 或资源退出证明时返回 None。
async fn wait_operation_until(
    inner: &Arc<RunnerInner>,
    deadline: Instant,
) -> Option<ShutdownReport> {
    loop {
        if let Some(report) = inner.operation.result() {
            return Some(report);
        }
        if inner.generation_control.supervisor_finished() {
            if let Some(lease) = inner.generation_control.lease_supervisor() {
                let _ = lease.join().await;
                if let Some(report) = inner.operation.result() {
                    return Some(report);
                }
                let replacement = inner.clone();
                if inner
                    .generation_control
                    .spawn_supervisor(async move { supervise_guarded(replacement).await })
                    .is_err()
                {
                    inner.lifecycle().fail_authority();
                    return None;
                }
            }
        }
        let remaining = deadline.checked_duration_since(Instant::now())?;
        let revision = inner.lifecycle().revision();
        let lifecycle = inner.lifecycle();
        let operation_done = crate::shield_future(inner.operation.done.notified());
        tokio::select! {
            _ = operation_done => {}
            _ = lifecycle.changed_since(revision) => {}
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(2))) => {}
        }
    }
}

/// 业务作用：映射 supervisor 建立失败，不向公开 API 暴露内部句柄状态。
///
/// 参数说明：
/// - `_error`: 生命周期核心返回的内部 supervisor 建立错误。
///
/// 返回：公开 API 稳定的控制面不可用错误。
fn map_supervisor_start(_error: SupervisorStartError) -> StartError {
    StartError::ControlPlaneUnavailable
}

/// 业务作用：把任意业务 key 哈希成进程内路由值；分区号不作为跨进程持久标识。
///
/// 参数说明：
/// - `key`: 实现 `Hash` 的业务路由键。
///
/// 返回：仅用于当前进程和 generation 选择原始 slot 的 64 位哈希值。
fn hash_key(key: impl Hash) -> u64 {
    crate::RouteHash::from_key(&key).0
}

/// 业务作用：把底层队列关门、耗尽或失权统一映射为当前类型不可安全接纳。
///
/// 参数说明：
/// - `_error`: reservation 层返回的内部拒绝原因。
///
/// 返回：公开提交 API 的持久执行域拒绝 `LaneFailed`。
fn queue_rejection(_error: ReserveError) -> SubmitRejection {
    SubmitRejection::LaneFailed
}
