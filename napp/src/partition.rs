//! 保序分 lane 执行器的 Application 生命周期接入。
//!
//! 容量计划来自 YAML 或 UserHook 的有界纯参数；组件在 Prepare 才创建 `PartitionExecutor`，因此
//! UserHook 失败不会遗留提前启动且无人拥有的 worker。句柄在 Prepare 发布，业务 initializer
//! 可以取得同一实例；运行期 worker 或 lane 失去安全执行权会把 readiness 置为 NotReady，
//! 同时让关键 monitor 退出以触发应用统一停机。

use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ShutdownAction,
    ShutdownContext, StartContext,
};

/// Application 允许的最大 worker 分区数；更大的线程常驻量不适合作为进程内受管默认能力。
const MAX_PARTITIONS: usize = 65_536;
/// Application 允许的最大类型化 lane 数，限制只增注册表与指标基数。
const MAX_LANES: usize = 1_048_576;
/// 计划允许的最大停机预算，与 Application 和 napart 的可表示 deadline 保持一致。
const MAX_STOP_TIMEOUT: Duration = Duration::from_secs(31_536_000);
/// 结构健康检查周期；执行器失权后应快速摘流而不对每次业务提交增加探针开销。
const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
/// readiness stale 上界，为正常检查周期留出调度抖动余量。
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(5);

/// 业务作用：描述由 Application 唯一拥有的保序执行器容量与收口预算。
///
/// 计划不持有 worker 或业务 future，可以安全留在 UserHook 登记表直到 Prepare。所有容量都必须
/// 显式有界，避免动态任务类型把 lane 注册表、队列或全局在飞量扩成不可审计的资源占用。
#[derive(Debug, Clone)]
pub struct PartitionApplicationPlan {
    partitions: usize,
    queue_capacity: usize,
    global_inflight: usize,
    max_lanes: usize,
    shutdown_timeout: Duration,
}

impl PartitionApplicationPlan {
    /// 业务作用：创建完整指定 worker、单 lane 排队量与全局在飞量的执行器计划。
    ///
    /// 参数说明：
    /// - `partitions`: 期望 worker 分区数；执行器会向上取为 2 的幂。
    /// - `queue_capacity`: 每条 lane 的最大排队量，不包含正在执行的任务。
    /// - `global_inflight`: 全部 lane 的排队与执行中任务总上限。
    ///
    /// 返回：容量均处于受管上界内时返回计划；零值或不可表示的容量返回 UserHook 配置错误。
    pub fn new(
        partitions: usize,
        queue_capacity: usize,
        global_inflight: usize,
    ) -> ApplicationResult<Self> {
        let plan = Self {
            partitions,
            queue_capacity,
            global_inflight,
            max_lanes: 4_096_usize.max(normalized_partitions(partitions)),
            shutdown_timeout: Duration::from_secs(2),
        };
        plan.validate_at(ApplicationPhase::UserHook)?;
        Ok(plan)
    }

    /// 业务作用：设置类型化 lane 注册总上限，约束开放 `TaskType` 带来的内存和指标基数。
    ///
    /// 参数说明：
    /// - `max_lanes`: 类型化 lane 上限，必须不小于规范化后的分区数且不超过受管上界。
    ///
    /// 返回：新上限合法时返回更新后的计划，否则返回 UserHook 配置错误。
    pub fn with_max_lanes(mut self, max_lanes: usize) -> ApplicationResult<Self> {
        self.max_lanes = max_lanes;
        self.validate_at(ApplicationPhase::UserHook)?;
        Ok(self)
    }

    /// 业务作用：设置执行器在 Application 反向停机中最多消费的排空预算。
    ///
    /// 实际停机还会受 Application 全局剩余 deadline 约束，计划值不能延长容器预算。
    ///
    /// 参数说明：
    /// - `timeout`: 等待已受理任务自然完成的最长时长，必须大于零且不超过 365 天。
    ///
    /// 返回：预算可表示时返回更新后的计划，否则返回 UserHook 配置错误。
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> ApplicationResult<Self> {
        self.shutdown_timeout = timeout;
        self.validate_at(ApplicationPhase::UserHook)?;
        Ok(self)
    }

    /// 业务作用：校验执行器计划不会依赖底层构造器的静默钳制形成隐藏容量合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部容量与预算满足受管边界时成功，否则返回稳定配置错误。
    fn validate_at(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.partitions == 0 || self.partitions > MAX_PARTITIONS {
            return Err(partition_error(
                phase,
                format!("partition.partitions must be within 1..={MAX_PARTITIONS}"),
            ));
        }
        if self.queue_capacity == 0 || self.queue_capacity > u32::MAX as usize {
            return Err(partition_error(
                phase,
                "partition.queue_capacity must be within 1..=4294967295",
            ));
        }
        if self.global_inflight == 0 || self.global_inflight > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(partition_error(
                phase,
                "partition.global_inflight exceeds the Tokio semaphore capacity",
            ));
        }
        let minimum_lanes = normalized_partitions(self.partitions);
        if self.max_lanes < minimum_lanes || self.max_lanes > MAX_LANES {
            return Err(partition_error(
                phase,
                format!("partition.max_lanes must be within {minimum_lanes}..={MAX_LANES}"),
            ));
        }
        if self.shutdown_timeout.is_zero() || self.shutdown_timeout > MAX_STOP_TIMEOUT {
            return Err(partition_error(
                phase,
                "partition shutdown timeout must be within (0, 365 days]",
            ));
        }
        Ok(())
    }

    /// 业务作用：在 Tokio 运行时内把纯参数计划转成已启动、尚未对业务发布的执行器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：使用已校验容量创建的共享执行器；worker 从本调用开始存在。
    fn build(&self) -> Arc<napart::PartitionExecutor> {
        Arc::new(
            napart::PartitionExecutor::with_limits(
                self.partitions,
                self.queue_capacity,
                self.global_inflight,
            )
            .with_max_lanes(self.max_lanes)
            .with_stop_timeout(self.shutdown_timeout),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// 业务作用：承载 YAML 中保序执行器的容量、lane 基数与停机预算。
struct PartitionSettings {
    partitions: usize,
    queue_capacity: usize,
    global_inflight: usize,
    #[serde(default)]
    max_lanes: Option<usize>,
    #[serde(default = "default_partition_shutdown_timeout_ms")]
    shutdown_timeout_ms: u64,
}

impl PartitionSettings {
    /// 业务作用：把无副作用的 YAML 设置转换为与 UserHook API 共用的受管计划。
    ///
    /// 参数说明：`phase` 是配置错误应归属的生命周期阶段。
    ///
    /// 返回：字段完整且容量有界时返回计划；未知字段、零值或越界值会阻止候选配置发布。
    fn into_plan(self, phase: ApplicationPhase) -> ApplicationResult<PartitionApplicationPlan> {
        let plan = PartitionApplicationPlan {
            partitions: self.partitions,
            queue_capacity: self.queue_capacity,
            global_inflight: self.global_inflight,
            max_lanes: self
                .max_lanes
                .unwrap_or_else(|| 4_096_usize.max(normalized_partitions(self.partitions))),
            shutdown_timeout: Duration::from_millis(self.shutdown_timeout_ms),
        };
        plan.validate_at(phase)?;
        Ok(plan)
    }
}

/// 业务作用：提供 YAML 未显式填写时的执行器排空预算。
///
/// 参数说明: 无。
///
/// 返回：与 `PartitionApplicationPlan::default` 一致的两秒毫秒值。
fn default_partition_shutdown_timeout_ms() -> u64 {
    2_000
}

/// 业务作用：在不创建 worker 的前提下校验候选配置中的 `partition` 容量合同。
///
/// 参数说明：
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：启动首帧或运行期候选校验阶段。
///
/// 返回：配置段缺失或合法时成功；结构、未知字段或容量非法时返回 Partition 组件错误。
pub(crate) fn validate_partition_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    partition_plan_from_tree(tree, phase).map(|_| ())
}

/// 业务作用：从完整配置树读取可选的 YAML 执行器计划，并与动态入口共用同一校验规则。
///
/// 参数说明：
/// - `tree`：最终或候选完整配置树。
/// - `phase`：解析失败的生命周期归因。
///
/// 返回：未声明 `partition` 段时返回 `None`；存在且合法时返回计划；非法时拒绝。
fn partition_plan_from_tree(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<PartitionApplicationPlan>> {
    let Some(section) = tree.get("partition") else {
        return Ok(None);
    };
    let settings: PartitionSettings = serde_json::from_value(section.clone()).map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Partition,
            phase,
            "invalid `partition` configuration section",
            error,
        )
    })?;
    settings.into_plan(phase).map(Some)
}

impl Default for PartitionApplicationPlan {
    /// 业务作用：提供与 napart 默认容量一致、可直接提交的受管计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分区数为可用并行度两倍并向上取 2 的幂，使用固定队列、lane 与停机上界的计划。
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let partitions = parallelism
            .saturating_mul(2)
            .next_power_of_two()
            .min(MAX_PARTITIONS);
        let queue_capacity = 65_536;
        let global_inflight = partitions
            .saturating_mul(queue_capacity + 1)
            .min(tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            partitions,
            queue_capacity,
            global_inflight,
            max_lanes: 4_096_usize.max(partitions),
            shutdown_timeout: Duration::from_secs(2),
        }
    }
}

/// Application 受管执行器的业务句柄。
///
/// 该投影开放任务提交与只读观测，但不开放 `shutdown*`。唯一完整执行器只由生命周期 action
/// 持有，业务代码不能提前关闭共享 worker 或改变停机证据的归属。
#[derive(Clone)]
pub struct PartitionApplicationHandle {
    executor: Arc<napart::PartitionExecutor>,
}

impl PartitionApplicationHandle {
    /// 业务作用：从生命周期组件拥有的完整执行器派生不含收口权的业务投影。
    ///
    /// 参数说明：
    /// - `executor`: 已由 active stack 覆盖停机所有权的唯一执行器。
    ///
    /// 返回：共享任务内核但不暴露 `shutdown*` 的可克隆句柄。
    fn new(executor: Arc<napart::PartitionExecutor>) -> Self {
        Self { executor }
    }

    /// 业务作用：提交一个同 key 严格串行的异步任务，不等待容量。
    ///
    /// 参数说明：
    /// - `key`: 决定严格顺序方向的业务路由键。
    /// - `task`: 被受理后由受管 worker 执行的异步任务工厂。
    ///
    /// 返回：受理成功时为空；停机、满载或目标方向失权时返回对应拒绝。
    pub fn submit<K, F, Fut>(&self, key: K, task: F) -> Result<(), napart::SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.executor.submit(key, task)
    }

    /// 业务作用：提交一个同 key 严格串行的同步短任务，不等待容量。
    ///
    /// 参数说明：
    /// - `key`: 决定严格顺序方向的业务路由键。
    /// - `task`: 被受理后由受管 worker 执行的同步任务。
    ///
    /// 返回：受理成功时为空；停机、满载或目标方向失权时返回对应拒绝。
    pub fn submit_sync<K, F>(&self, key: K, task: F) -> Result<(), napart::SubmitError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        self.executor.submit_sync(key, task)
    }

    /// 业务作用：等待容量后提交同 key 严格串行的异步任务。
    ///
    /// 参数说明：
    /// - `key`: 决定严格顺序方向的业务路由键。
    /// - `task`: 取得容量后交给受管 worker 的异步任务工厂。
    ///
    /// 返回：成功取得容量并受理时为空；停机或目标方向失权时返回对应拒绝。
    pub async fn submit_async<K, F, Fut>(&self, key: K, task: F) -> Result<(), napart::SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.executor.submit_async(key, task).await
    }

    /// 业务作用：按显式任务类型与顺序要求提交任务并返回终态句柄。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `spec`: 冻结任务类型和严格或非严格顺序语义。
    /// - `task`: 被受理后由受管 worker 执行的异步任务工厂。
    ///
    /// 返回：受理成功时返回可取消、可等待的提交句柄；容量或 lane 门禁拒绝时返回封闭原因。
    pub fn submit_typed<K, F, Fut>(
        &self,
        key: K,
        spec: napart::TaskSpec,
        task: F,
    ) -> Result<napart::Submission, napart::SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.executor.submit_typed(key, spec, task)
    }

    /// 业务作用：按显式任务类型提交无需终态句柄的异步任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `spec`: 冻结任务类型和严格或非严格顺序语义。
    /// - `task`: 被受理后由受管 worker 执行的异步任务工厂。
    ///
    /// 返回：受理成功时为空；容量或 lane 门禁拒绝时返回封闭原因。
    pub fn exec_typed<K, F, Fut>(
        &self,
        key: K,
        spec: napart::TaskSpec,
        task: F,
    ) -> Result<(), napart::SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.executor.exec_typed(key, spec, task)
    }

    /// 业务作用：登记在指定延迟后进入 typed lane 的任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `delay`: 不早于该时长进入 lane 的延迟预算。
    /// - `spec`: 冻结任务类型和严格或非严格顺序语义。
    /// - `task`: 到期且仍具准入权时执行的异步任务工厂。
    ///
    /// 返回：登记成功时返回可取消、可等待的提交句柄；停机或容量门禁拒绝时返回封闭原因。
    pub fn submit_after<K, F, Fut>(
        &self,
        key: K,
        delay: Duration,
        spec: napart::TaskSpec,
        task: F,
    ) -> Result<napart::Submission, napart::SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.executor.submit_after(key, delay, spec, task)
    }

    /// 业务作用：读取受管执行器是否仍开放任务准入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Application 尚未开始收口且结构健康时返回 `true`。
    pub fn is_started(&self) -> bool {
        self.executor.is_started()
    }

    /// 业务作用：综合读取执行阶段、worker 存活与 lane 冻结状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仍接受任务且没有 worker 或 lane 失权时返回 `true`。
    pub fn is_healthy(&self) -> bool {
        self.executor.is_healthy()
    }

    /// 业务作用：读取固定 worker 分区数，供容量诊断使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：计划经 2 的幂规范化后的分区数量。
    pub fn partitions(&self) -> usize {
        self.executor.partitions()
    }

    /// 业务作用：读取当前已建立的 lane 数，供容量与任务类型基数诊断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：兼容 lane 与 typed lane 的当前总数。
    pub fn lanes(&self) -> usize {
        self.executor.lanes()
    }

    /// 业务作用：读取异常死亡的 worker 数，供受鉴权管理面定位结构性失权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：异常死亡的 worker 计数，健康状态下为零。
    pub fn dead_partitions(&self) -> i64 {
        self.executor.dead_partitions()
    }

    /// 业务作用：读取已冻结 lane 数，供受鉴权管理面定位拒收方向。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已冻结 lane 计数，健康状态下为零。
    pub fn failed_lanes(&self) -> u64 {
        self.executor.failed_lanes()
    }

    /// 业务作用：读取执行器低基数运行指标，不改变任何任务状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：提交、完成、拒绝、lane、worker 与停机计数的当前原子快照。
    pub fn metrics_snapshot(&self) -> napart::MetricsSnapshot {
        self.executor.metrics_snapshot()
    }

    /// 业务作用：读取不含业务载荷的冻结证据，供受鉴权管理面诊断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前已保留的任务类型、分区、终态和稳定原因码快照。
    pub fn frozen_evidence(&self) -> Vec<napart::FrozenEvidence> {
        self.executor.frozen_evidence()
    }
}

/// 业务作用：把分区数转换为 napart 实际使用的 2 的幂，供计划上下界采用同一口径。
///
/// 参数说明：
/// - `partitions`: 用户期望的正分区数；零值只用于错误路径并按 1 计算。
///
/// 返回：至少为 1 的规范化分区数。
fn normalized_partitions(partitions: usize) -> usize {
    partitions.max(1).next_power_of_two()
}

/// YAML/UserHook 计划与 Prepare 发布共用的单实例状态。
pub(crate) struct PartitionRuntimeState {
    plan: Mutex<PartitionPlanState>,
    executor: OnceLock<PartitionApplicationHandle>,
}

enum PartitionPlanState {
    Open(Option<PartitionApplicationPlan>),
    Taken,
}

impl PartitionRuntimeState {
    /// 业务作用：创建计划入口开放、执行器尚未发布的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅允许一次配置与一次发布的空状态。
    pub(crate) fn new() -> Self {
        Self {
            plan: Mutex::new(PartitionPlanState::Open(None)),
            executor: OnceLock::new(),
        }
    }

    /// 业务作用：在 UserHook 把完整计划一次性移交给生命周期组件。
    ///
    /// 参数说明：
    /// - `plan`: 已完成容量校验且不持有运行副作用的计划。
    ///
    /// 返回：首次登记成功；重复登记或 Prepare 已取走入口时返回阶段错误。
    pub(crate) fn configure(&self, plan: PartitionApplicationPlan) -> ApplicationResult<()> {
        plan.validate_at(ApplicationPhase::UserHook)?;
        let mut state = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            PartitionPlanState::Open(slot @ None) => {
                *slot = Some(plan);
                Ok(())
            }
            PartitionPlanState::Open(Some(_)) => Err(partition_error(
                ApplicationPhase::UserHook,
                "partition plan can be configured only once",
            )),
            PartitionPlanState::Taken => Err(partition_error(
                ApplicationPhase::Prepare,
                "partition plan registration is closed",
            )),
        }
    }

    /// 业务作用：在 Prepare 原子关闭计划入口并取得唯一计划。
    ///
    /// 参数说明：`configured_plan` 是从最终 YAML 解析出的可选计划。
    ///
    /// 返回：YAML 或 UserHook 恰好提供一份计划时返回所有权；两者冲突或都缺失时拒绝启动，且入口保持关闭。
    fn take_plan(
        &self,
        configured_plan: Option<PartitionApplicationPlan>,
    ) -> ApplicationResult<PartitionApplicationPlan> {
        let mut state = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::mem::replace(&mut *state, PartitionPlanState::Taken);
        match (previous, configured_plan) {
            (PartitionPlanState::Open(Some(_)), Some(_)) => Err(partition_error(
                ApplicationPhase::Prepare,
                "partition plan conflict: use either YAML `partition` or configure_partition, not both",
            )),
            (PartitionPlanState::Open(Some(plan)), None)
            | (PartitionPlanState::Open(None), Some(plan)) => Ok(plan),
            (PartitionPlanState::Open(None), None) => Err(partition_error(
                ApplicationPhase::Prepare,
                "partition component requires YAML `partition` or configure_partition during the Service user hook",
            )),
            (PartitionPlanState::Taken, _) => Err(partition_error(
                ApplicationPhase::Prepare,
                "partition plan was already consumed",
            )),
        }
    }

    /// 业务作用：在资源登记成功后发布唯一执行器，供 Initializer 与运行期业务取得同一实例。
    ///
    /// 参数说明：
    /// - `executor`: 已受 active stack 停机所有权覆盖的共享执行器。
    ///
    /// 返回：首次发布成功；重复发布返回 Prepare 错误。
    fn publish(&self, executor: Arc<napart::PartitionExecutor>) -> ApplicationResult<()> {
        self.executor
            .set(PartitionApplicationHandle::new(executor))
            .map_err(|_| {
                partition_error(
                    ApplicationPhase::Prepare,
                    "partition executor was already published",
                )
            })
    }

    /// 业务作用：取得仍接受任务的已发布执行器，不把停机后的残留 Arc 伪装为可用能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Prepare 已发布且执行器仍开放时返回共享句柄；未就绪或已停机时返回阶段错误。
    pub(crate) fn executor(&self) -> ApplicationResult<PartitionApplicationHandle> {
        let executor = self.executor.get().cloned().ok_or_else(|| {
            partition_error(
                ApplicationPhase::Prepare,
                "partition executor is not published yet",
            )
        })?;
        if !executor.is_started() {
            return Err(partition_error(
                ApplicationPhase::Running,
                "partition executor is stopping or stopped",
            ));
        }
        Ok(executor)
    }
}

/// 受管保序执行器组件。
pub(crate) struct PartitionComponent {
    contributor: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl PartitionComponent {
    /// 业务作用：创建尚未接收计划、尚未建立 worker 的生命周期组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：等待 Start 注册 readiness、Prepare 消费业务计划的组件。
    pub(crate) fn new() -> Self {
        Self {
            contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for PartitionComponent {
    /// 业务作用：返回保序执行器的稳定组件身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`partition` 组件身份。
    fn id(&self) -> ComponentId {
        ComponentId::Partition
    }

    /// 业务作用：在业务计划提交前登记关键 readiness 名称，保证 Seal 后不增长观测基数。
    ///
    /// 参数说明：
    /// - `context`: 提供共享 Application 的 Start 上下文。
    ///
    /// 返回：贡献项首次登记成功时完成；名称冲突或策略非法时拒绝启动。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.contributor = Some(context.application().register_readiness(
                ComponentId::Partition,
                Arc::<str>::from("partition:executor"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: Some(HEALTH_STALE_AFTER),
                },
            )?);
            Ok(())
        })
    }

    /// 业务作用：在 Initializer 之前消费纯参数计划、创建 worker、发布强类型句柄并建立停机所有权。
    ///
    /// 参数说明：
    /// - `context`: 提供资源登记、active stack 和共享启动 deadline 的 Prepare 上下文。
    ///
    /// 返回：执行器已经进入容器统一资源视图并由停机 action 覆盖时成功；缺失计划或发布冲突时
    /// 先完整停止新建执行器再返回错误。
    fn prepare<'a>(&'a mut self, context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let runtime = context.application().partition_runtime();
            let snapshot = context.application().config();
            let configured_plan =
                partition_plan_from_tree(snapshot.value(), ApplicationPhase::Prepare)?;
            let plan = runtime.take_plan(configured_plan)?;
            let shutdown_timeout = plan.shutdown_timeout;
            let executor = plan.build();
            let business_handle = PartitionApplicationHandle::new(Arc::clone(&executor));
            if let Err(error) = context.register_resource(None, business_handle) {
                let _ = executor
                    .shutdown_with_report_timeout(context.remaining())
                    .await;
                return Err(error);
            }
            if let Err(error) = runtime.publish(Arc::clone(&executor)) {
                let _ = executor
                    .shutdown_with_report_timeout(context.remaining())
                    .await;
                return Err(error);
            }
            let contributor = self.contributor.clone().ok_or_else(|| {
                partition_error(
                    ApplicationPhase::Prepare,
                    "partition readiness contributor was not registered during Start",
                )
            })?;
            contributor.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            );
            self.critical_task = Some(Box::pin(run_health_monitor(
                context.application().clone(),
                Arc::clone(&executor),
                contributor.clone(),
            )));
            // 句柄和 readiness 都已经发布后才压入 action；此后任何初始化或 Ready 失败都会先停止
            // worker，再释放资源容器中的 Arc，不留下仍可接收任务的孤立执行器。
            context.activate(Box::new(PartitionShutdown {
                executor,
                contributor,
                configured_timeout: shutdown_timeout,
            }));
            Ok(())
        })
    }

    /// 业务作用：把结构健康 monitor 移交 Runner，运行期失权会触发统一失败停机。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Prepare 已建立执行器时返回一次性关键任务，否则返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("partition-health-monitor", task))
    }
}

/// 业务作用：周期观察 worker 与 lane 的安全执行权，并把结构性失权升级为应用关键失败。
///
/// 参数说明：
/// - `application`: 用于区分运行与正常停机阶段的容器句柄。
/// - `executor`: Prepare 发布的唯一执行器。
/// - `contributor`: `partition:executor` readiness 的独占更新句柄。
///
/// 返回：正常停机时成功退出；worker 异常死亡或 lane 冻结时返回运行错误并触发应用停机。
async fn run_health_monitor(
    application: Application,
    executor: Arc<napart::PartitionExecutor>,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                );
                return Ok(());
            }
            ApplicationState::Starting | ApplicationState::Ready => {}
        }
        if !executor.is_started() {
            contributor.observe(
                DependencyState::NotReady,
                reason::PARTITION_UNHEALTHY,
                std::time::Instant::now(),
            );
            return Err(partition_error(
                ApplicationPhase::Running,
                "partition executor stopped outside Application shutdown",
            ));
        }
        if executor.dead_partitions() > 0 {
            contributor.observe(
                DependencyState::NotReady,
                reason::PARTITION_UNHEALTHY,
                std::time::Instant::now(),
            );
            return Err(partition_error(
                ApplicationPhase::Running,
                "partition executor lost worker execution authority",
            ));
        }
        if executor.failed_lanes() > 0 {
            contributor.observe(
                DependencyState::NotReady,
                reason::PARTITION_UNHEALTHY,
                std::time::Instant::now(),
            );
            return Err(partition_error(
                ApplicationPhase::Running,
                "partition executor retained a failed lane",
            ));
        }
        contributor.observe(
            DependencyState::Ready,
            reason::HEALTHY,
            std::time::Instant::now(),
        );
        tokio::time::sleep(HEALTH_INTERVAL).await;
    }
}

/// 停机 action 持有执行器最后的控制所有权。
struct PartitionShutdown {
    executor: Arc<napart::PartitionExecutor>,
    contributor: ReadinessContributor,
    configured_timeout: Duration,
}

impl ShutdownAction for PartitionShutdown {
    /// 业务作用：返回停机报告使用的稳定 action 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含业务键或配置值的固定名称。
    fn label(&self) -> &'static str {
        "partition-executor"
    }

    /// 业务作用：关闭任务准入，在容器剩余预算内排空 worker，并把任何有损终局升级为停机失败。
    ///
    /// 参数说明：
    /// - `context`: 提供全部 active step 共用的绝对停机 deadline。
    ///
    /// 返回：全部 worker、在途任务和延迟定时器无损退出时成功；冻结或中止任务时返回包含有界
    /// 计数的停机错误，证据仍保留在执行器内。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.contributor.observe(
                DependencyState::NotReady,
                reason::NOT_READY,
                std::time::Instant::now(),
            );
            // 子排空必须先于绝对 deadline 收口，给损耗报告与后续资源释放留下固定尾部预算。
            let timeout = context.child_budget(self.configured_timeout);
            let report = self.executor.shutdown_with_report_timeout(timeout).await;
            if report.frozen > 0 || report.aborted > 0 {
                return Err(partition_error(
                    ApplicationPhase::Stopping,
                    format!(
                        "partition shutdown retained loss evidence: frozen={}, aborted={}, timed_out_lanes={}",
                        report.frozen, report.aborted, report.timed_out_lanes
                    ),
                ));
            }
            Ok(())
        })
    }
}

/// 业务作用：创建不包含业务载荷或动态配置值的 partition 生命周期错误。
///
/// 参数说明：
/// - `phase`: 错误被裁决时的 Application 生命周期阶段。
/// - `message`: 稳定配置或结构状态摘要。
///
/// 返回：归属 `partition` 组件的统一错误。
fn partition_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Partition, phase, message)
}
