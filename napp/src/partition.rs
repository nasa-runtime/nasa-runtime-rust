//! 命名分区 Runner 的 Application 生命周期接入。
//!
//! Application 显式拥有一个 [`napart::PartitionRunnerRegistry`]。Prepare 按稳定名称创建并启动
//! 全部 Runner，全部成功后一次性发布不含启停权的业务句柄；停机按启动反序收口。扁平配置
//! 与 [`Application::partition`] 继续表示 default Runner。
//! YAML 与 Service UserHook 提交的计划在 Prepare 前合并并冻结；UserHook 调用只登记参数，不能在
//! 同一 Hook 中取得尚未发布的 Runner。Running 阶段不追加受管 Runner，运行期动态成员应由业务直接
//! 持有 napart 注册表并承担对应生命周期。
//!
//! 每个命名 Runner 独立承担严格 FIFO 的保序任务窃取、容量、健康与停止责任。业务句柄登记的
//! delayed 任务到期时仍需重新竞争类型容量和路由，可能形成稳定 `Rejected`；取消与到期竞争只
//! 发布一个终态并只结算一次许可。Application 负责生命周期，不改变 napart 的任务权威合同。

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const MAX_PARTITIONS: usize = 65_536;
const MAX_TYPE_STATES: usize = 1_048_576;
const MAX_STOP_TIMEOUT: Duration = Duration::from_secs(31_536_000);
const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(5);

/// Application 管理一个命名 Runner 所需的容量、控制参数和停机策略。
///
/// 本类型只持有已校验纯参数，不创建 worker。`critical` 决定该 Runner 失去健康时是否触发
/// Application 统一停机；`force_on_timeout` 决定无损预算耗尽后是否显式升级为有损收口。
#[derive(Debug, Clone)]
pub struct PartitionApplicationPlan {
    config: napart::RunnerConfig,
    critical: bool,
    force_on_timeout: bool,
}

impl PartitionApplicationPlan {
    /// 业务作用：创建 default Runner 兼容计划，冻结分区数、每类型排队量与全局在飞量。
    ///
    /// 参数说明：
    /// - `partitions`: 期望 worker 分区数，最终向上规范化为 2 的幂。
    /// - `queue_capacity`: 每个 `(home, TaskType)` 尚未运行任务的上限。
    /// - `global_inflight`: 当前 Runner 延迟、排队、迁移和执行中任务总上限。
    ///
    /// 返回：参数满足受管边界时返回关键 Runner 计划；否则返回 UserHook 配置错误。
    pub fn new(
        partitions: usize,
        queue_capacity: usize,
        global_inflight: usize,
    ) -> ApplicationResult<Self> {
        let normalized = normalized_partitions(partitions);
        Self {
            config: napart::RunnerConfig {
                partitions,
                queue_capacity_per_type: queue_capacity,
                global_inflight,
                max_type_states: 4_096_usize.max(normalized),
                ..napart::RunnerConfig::default()
            },
            critical: true,
            force_on_timeout: true,
        }
        .validated_at(ApplicationPhase::UserHook)
    }

    /// 业务作用：从完整 napart 配置创建受管计划，允许不同命名 Runner 使用独立控制参数。
    ///
    /// 参数说明：
    /// - `config`: napart Runner 的完整容量、观察、迁移和停止配置。
    ///
    /// 返回：配置完整合法时返回关键 Runner 计划；否则返回 UserHook 配置错误。
    pub fn from_runner_config(config: napart::RunnerConfig) -> ApplicationResult<Self> {
        Self {
            config,
            critical: true,
            force_on_timeout: true,
        }
        .validated_at(ApplicationPhase::UserHook)
    }

    /// 业务作用：设置当前 Runner 的类型状态总上限，兼容旧 `max_lanes` 计划入口。
    ///
    /// 参数说明：
    /// - `max_lanes`: 当前 Runner 全部 `(home, TaskType)` 状态上限。
    ///
    /// 返回：新上限合法时返回更新计划；否则返回 UserHook 配置错误。
    pub fn with_max_lanes(mut self, max_lanes: usize) -> ApplicationResult<Self> {
        self.config.max_type_states = max_lanes;
        self.validated_at(ApplicationPhase::UserHook)
    }

    /// 业务作用：设置 Application 为当前 Runner 分配的无损收口预算。
    ///
    /// 参数说明：
    /// - `timeout`: 必须大于零且不超过 365 天；实际值仍受 Application 剩余期限约束。
    ///
    /// 返回：预算合法时返回更新计划；否则返回 UserHook 配置错误。
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> ApplicationResult<Self> {
        self.config.shutdown_timeout = timeout;
        self.validated_at(ApplicationPhase::UserHook)
    }

    /// 业务作用：声明当前 Runner 失去安全执行条件时是否触发 Application 统一停机。
    ///
    /// 参数说明：
    /// - `critical`: true 表示健康失败属于应用关键失败；false 只隔离当前 Runner。
    ///
    /// 返回：链式返回更新后的计划。
    pub fn with_critical(mut self, critical: bool) -> Self {
        self.critical = critical;
        self
    }

    /// 业务作用：声明无损预算耗尽后是否允许 Application 显式调用有损停止入口。
    ///
    /// 参数说明：
    /// - `enabled`: true 允许中止执行中任务并冻结未执行任务；false 保留未收敛错误。
    ///
    /// 返回：链式返回更新后的计划。
    pub fn with_force_on_timeout(mut self, enabled: bool) -> Self {
        self.force_on_timeout = enabled;
        self
    }

    /// 业务作用：读取完整冻结 Runner 配置，供诊断和复用计划参数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含运行态的配置引用。
    pub fn runner_config(&self) -> &napart::RunnerConfig {
        &self.config
    }

    /// 业务作用：读取当前 Runner 是否属于 Application 关键执行域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：关键计划返回 true。
    pub(crate) fn critical(&self) -> bool {
        self.critical
    }

    /// 业务作用：在指定生命周期阶段校验并规范化完整 Runner 配置。
    ///
    /// 参数说明：
    /// - `phase`: 配置错误应归属的 Application 阶段。
    ///
    /// 返回：合法时返回规范化副本；任何容量或时长越界返回 Partition 配置错误。
    fn validated_at(mut self, phase: ApplicationPhase) -> ApplicationResult<Self> {
        if self.config.partitions == 0 || self.config.partitions > MAX_PARTITIONS {
            return Err(partition_error(
                phase,
                format!("partition.partitions must be within 1..={MAX_PARTITIONS}"),
            ));
        }
        if self.config.max_type_states > MAX_TYPE_STATES {
            return Err(partition_error(
                phase,
                format!("partition.max_type_states must not exceed {MAX_TYPE_STATES}"),
            ));
        }
        if self.config.shutdown_timeout.is_zero() || self.config.shutdown_timeout > MAX_STOP_TIMEOUT
        {
            return Err(partition_error(
                phase,
                "partition shutdown timeout must be within (0, 365 days]",
            ));
        }
        self.config = self.config.validated().map_err(|error| {
            partition_error(phase, format!("invalid partition Runner config: {error}"))
        })?;
        Ok(self)
    }
}

impl Default for PartitionApplicationPlan {
    /// 业务作用：提供与 napart 默认配置一致的关键 default Runner 计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：所有资源均有固定上界且允许超时后显式有损收口的计划。
    fn default() -> Self {
        Self {
            config: napart::RunnerConfig::default(),
            critical: true,
            force_on_timeout: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// 业务作用：承载单个分区 Runner 的可选 YAML 字段，并在启动期补齐有界默认值。
struct PartitionRunnerSettings {
    #[serde(default)]
    partitions: Option<usize>,
    #[serde(default)]
    queue_capacity: Option<usize>,
    #[serde(default)]
    queue_capacity_per_type: Option<usize>,
    #[serde(default)]
    global_inflight: Option<usize>,
    #[serde(default)]
    max_lanes: Option<usize>,
    #[serde(default)]
    max_type_states: Option<usize>,
    #[serde(default)]
    frozen_evidence_capacity: Option<usize>,
    #[serde(default)]
    max_inbound_tunnels: Option<usize>,
    #[serde(default)]
    idle_task_threshold: Option<usize>,
    #[serde(default)]
    strict_opportunity_attempts: Option<usize>,
    #[serde(default)]
    return_observations: Option<usize>,
    #[serde(default)]
    tunnel_lease_ms: Option<u64>,
    #[serde(default)]
    load_observer_interval_ms: Option<u64>,
    #[serde(default)]
    control_tick_ms: Option<u64>,
    #[serde(default)]
    transition_timeout_ms: Option<u64>,
    #[serde(default)]
    shutdown_timeout_ms: Option<u64>,
    #[serde(default)]
    drain_batch: Option<usize>,
    #[serde(default = "default_true")]
    critical: bool,
    #[serde(default = "default_true")]
    force_on_timeout: bool,
}

impl PartitionRunnerSettings {
    /// 业务作用：把一个 YAML Runner 段转换为与 UserHook 共用的完整计划。
    ///
    /// 参数说明：
    /// - `phase`: 解析错误应归属的 Application 阶段。
    ///
    /// 返回：字段无冲突且配置有界时返回计划；否则阻止候选配置发布。
    fn into_plan(self, phase: ApplicationPhase) -> ApplicationResult<PartitionApplicationPlan> {
        let defaults = napart::RunnerConfig::default();
        let queue_capacity = match (self.queue_capacity, self.queue_capacity_per_type) {
            (Some(value), None) | (None, Some(value)) => value,
            (Some(_), Some(_)) => {
                return Err(partition_error(
                    phase,
                    "partition Runner cannot set both queue_capacity and queue_capacity_per_type",
                ));
            }
            (None, None) => defaults.queue_capacity_per_type,
        };
        let partitions = self.partitions.unwrap_or(defaults.partitions);
        let normalized = normalized_partitions(partitions);
        let max_type_states = match (self.max_lanes, self.max_type_states) {
            (Some(value), None) | (None, Some(value)) => value,
            (Some(_), Some(_)) => {
                return Err(partition_error(
                    phase,
                    "partition Runner cannot set both max_lanes and max_type_states",
                ));
            }
            (None, None) => defaults.max_type_states.max(normalized),
        };
        PartitionApplicationPlan {
            config: napart::RunnerConfig {
                partitions,
                queue_capacity_per_type: queue_capacity,
                global_inflight: self.global_inflight.unwrap_or(defaults.global_inflight),
                max_type_states,
                frozen_evidence_capacity: self
                    .frozen_evidence_capacity
                    .unwrap_or(defaults.frozen_evidence_capacity),
                max_inbound_tunnels: self
                    .max_inbound_tunnels
                    .unwrap_or(defaults.max_inbound_tunnels),
                idle_task_threshold: self
                    .idle_task_threshold
                    .unwrap_or(defaults.idle_task_threshold),
                strict_opportunity_attempts: self
                    .strict_opportunity_attempts
                    .unwrap_or(defaults.strict_opportunity_attempts),
                return_observations: self
                    .return_observations
                    .unwrap_or(defaults.return_observations),
                tunnel_lease: self
                    .tunnel_lease_ms
                    .map(Duration::from_millis)
                    .unwrap_or(defaults.tunnel_lease),
                load_observer_interval: self
                    .load_observer_interval_ms
                    .map(Duration::from_millis)
                    .unwrap_or(defaults.load_observer_interval),
                control_tick: self
                    .control_tick_ms
                    .map(Duration::from_millis)
                    .unwrap_or(defaults.control_tick),
                transition_timeout: self
                    .transition_timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(defaults.transition_timeout),
                shutdown_timeout: self
                    .shutdown_timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(defaults.shutdown_timeout),
                drain_batch: self.drain_batch.unwrap_or(defaults.drain_batch),
            },
            critical: self.critical,
            force_on_timeout: self.force_on_timeout,
        }
        .validated_at(phase)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// 业务作用：绑定默认 Runner 名与命名 Runner 配置表，避免运行期猜测唯一实例。
struct NamedPartitionSettings {
    #[serde(default = "default_runner_name")]
    default_runner: String,
    runners: BTreeMap<String, PartitionRunnerSettings>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PartitionSettingsShape {
    Named(NamedPartitionSettings),
    Flat(Box<PartitionRunnerSettings>),
}

#[derive(Clone)]
/// 业务作用：保存已经规范化的默认 Runner 身份与全部唯一命名启动计划。
struct PartitionPlanSet {
    default_runner: napart::RunnerName,
    runners: BTreeMap<napart::RunnerName, PartitionApplicationPlan>,
}

impl PartitionSettingsShape {
    /// 业务作用：把新命名映射或旧扁平 YAML 统一转换为稳定名称计划集合。
    ///
    /// 参数说明：
    /// - `phase`: 配置错误应归属的 Application 阶段。
    ///
    /// 返回：名称和每项计划合法且 default 已声明时返回集合；混用或缺失时拒绝。
    fn into_plan_set(self, phase: ApplicationPhase) -> ApplicationResult<PartitionPlanSet> {
        match self {
            Self::Flat(settings) => {
                let name = runner_name(napart::DEFAULT_RUNNER, phase)?;
                Ok(PartitionPlanSet {
                    default_runner: name.clone(),
                    runners: BTreeMap::from([(name, settings.into_plan(phase)?)]),
                })
            }
            Self::Named(settings) => {
                if settings.runners.is_empty() {
                    return Err(partition_error(
                        phase,
                        "partition.runners must contain at least one named Runner",
                    ));
                }
                let default_runner = runner_name(&settings.default_runner, phase)?;
                let mut runners = BTreeMap::new();
                for (name, settings) in settings.runners {
                    let name = runner_name(&name, phase)?;
                    runners.insert(name, settings.into_plan(phase)?);
                }
                if !runners.contains_key(&default_runner) {
                    return Err(partition_error(
                        phase,
                        "partition.default_runner must name an entry in partition.runners",
                    ));
                }
                Ok(PartitionPlanSet {
                    default_runner,
                    runners,
                })
            }
        }
    }
}

/// 业务作用：提供布尔策略字段的兼容默认值。
///
/// 参数说明: 无。
///
/// 返回：true。
fn default_true() -> bool {
    true
}

/// 业务作用：提供命名 YAML 未显式填写时的 default Runner 名称。
///
/// 参数说明: 无。
///
/// 返回：`default` 文本。
fn default_runner_name() -> String {
    napart::DEFAULT_RUNNER.to_owned()
}

/// 业务作用：在不创建 worker 的前提下校验候选配置中的 partition 执行域合同。
///
/// 参数说明：
/// - `tree`: 合并、插值完成但尚未发布的候选配置树。
/// - `phase`: 启动首帧或运行期候选校验阶段。
///
/// 返回：配置段缺失或合法时成功；结构、名称、冲突或容量非法时返回 Partition 错误。
pub(crate) fn validate_partition_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    partition_plans_from_tree(tree, phase).map(|_| ())
}

/// 业务作用：从完整配置树读取可选命名计划集合，并保留旧扁平 default Runner 语法。
///
/// 参数说明：
/// - `tree`: 最终或候选完整配置树。
/// - `phase`: 解析失败的生命周期阶段。
///
/// 返回：未声明 partition 时返回 None；存在且合法时返回稳定计划集合。
fn partition_plans_from_tree(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<PartitionPlanSet>> {
    let Some(section) = tree.get("partition") else {
        return Ok(None);
    };
    let settings: PartitionSettingsShape =
        serde_json::from_value(section.clone()).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Partition,
                phase,
                "invalid `partition` configuration section",
                error,
            )
        })?;
    settings.into_plan_set(phase).map(Some)
}

/// Application 受管命名 Runner 的业务句柄。
///
/// 句柄开放提交、健康与观测，不开放 `start`、`stop` 或 `force_stop`。Drop 也不代表退出证明；
/// Runner 的确定停机只由 Application 生命周期 action 持有。
#[derive(Clone)]
pub struct PartitionApplicationHandle {
    runner: napart::PartitionRunner,
}

impl PartitionApplicationHandle {
    /// 业务作用：从生命周期组件持有的 Runner 派生不含启停入口的业务投影。
    ///
    /// 参数说明：
    /// - `runner`: 已由 Application active stack 覆盖停机所有权的 Runner。
    ///
    /// 返回：共享同一执行域的可克隆业务句柄。
    fn new(runner: napart::PartitionRunner) -> Self {
        Self { runner }
    }

    /// 业务作用：读取本业务句柄绑定的稳定 Runner 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：注册表校验过的名称引用。
    pub fn name(&self) -> &napart::RunnerName {
        self.runner.name()
    }

    /// 业务作用：提交一个同原始分区严格串行的兼容异步任务，不等待容量。
    ///
    /// 参数说明：
    /// - `key`: 决定严格顺序方向的业务路由键。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；停机、满载或执行域失权时返回兼容错误。
    pub fn submit<K, F, Fut>(&self, key: K, task: F) -> Result<(), napart::SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.submit(key, task)
    }

    /// 业务作用：提交一个同原始分区严格串行的同步短任务，不等待容量。
    ///
    /// 参数说明：
    /// - `key`: 决定严格顺序方向的业务路由键。
    /// - `task`: 受理后执行的同步任务。
    ///
    /// 返回：受理成功返回空值；停机、满载或执行域失权时返回兼容错误。
    pub fn submit_sync<K, F>(&self, key: K, task: F) -> Result<(), napart::SubmitError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        self.runner.submit_sync(key, task)
    }

    /// 业务作用：取消安全地等待容量后提交同原始分区严格串行任务。
    ///
    /// 参数说明：
    /// - `key`: 决定严格顺序方向的业务路由键。
    /// - `task`: 取得容量后执行的异步任务工厂。
    ///
    /// 返回：成功受理返回空值；停机或执行域失权时返回兼容错误。
    pub async fn submit_async<K, F, Fut>(&self, key: K, task: F) -> Result<(), napart::SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.runner.submit_compat_async(key, task).await
    }

    /// 业务作用：按显式任务类型与顺序要求提交任务并返回权威终态句柄。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `spec`: 冻结任务类型和严格或非严格顺序语义。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回可取消、可等待句柄；容量或路由门禁拒绝时返回稳定原因。
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
        self.runner.submit_typed(key, spec, task)
    }

    /// 业务作用：按显式任务类型提交无需逐任务句柄的异步任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `spec`: 冻结任务类型和顺序要求。
    /// - `task`: 受理后执行的异步任务工厂。
    ///
    /// 返回：受理成功返回空值；容量或路由门禁拒绝时返回稳定原因。
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
        self.runner.exec_typed(key, spec, task)
    }

    /// 业务作用：登记指定延迟后进入当前 Runner 类型路由的任务。
    ///
    /// 参数说明：
    /// - `key`: 决定原始分区的业务路由键。
    /// - `delay`: 不早于该时长进入物理队列的延迟预算。
    /// - `spec`: 冻结任务类型和顺序要求。
    /// - `task`: 到期且仍具准入权时执行的异步任务工厂。
    ///
    /// 返回：登记成功返回终态句柄；停机或全局容量门禁拒绝时返回稳定原因。
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
        self.runner.submit_after(key, delay, spec, task)
    }

    /// 业务作用：读取当前 Runner 是否仍处于 Accepting。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Application 尚未开始收口且本 Runner 开放准入时返回 true。
    pub fn is_started(&self) -> bool {
        self.runner.phase() == napart::RunnerPhase::Accepting
    }

    /// 业务作用：读取当前 Runner 是否没有已隔离类型或 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：健康状态为 Healthy 时返回 true。
    pub fn is_healthy(&self) -> bool {
        self.runner.health() == napart::RunnerHealth::Healthy
    }

    /// 业务作用：读取计划规范化后的固定 worker 分区数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 Runner 配置分区数。
    pub fn partitions(&self) -> usize {
        self.runner.config().partitions
    }

    /// 业务作用：读取当前 generation 已建立的类型状态数，保留旧观测名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 slot 类型状态数之和。
    pub fn lanes(&self) -> usize {
        self.runner.metrics_snapshot().lanes as usize
    }

    /// 业务作用：读取异常退出或已隔离 worker 数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：结构健康时为零的 worker 计数。
    pub fn dead_partitions(&self) -> i64 {
        self.runner.metrics_snapshot().dead_workers
    }

    /// 业务作用：读取已冻结类型数，保留旧观测名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 累计失败类型数。
    pub fn failed_lanes(&self) -> u64 {
        self.runner.metrics_snapshot().failed_lanes
    }

    /// 业务作用：读取当前 Runner 的低基数指标快照，不改变任务状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前或最近 generation 的独立快照。
    pub fn metrics_snapshot(&self) -> napart::MetricsSnapshot {
        self.runner.metrics_snapshot()
    }

    /// 业务作用：读取不含业务载荷和调度结构引用的有界失败证据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最近样本、累计数和覆盖数的独立快照。
    pub fn frozen_evidence_snapshot(&self) -> napart::FrozenEvidenceSnapshot {
        self.runner.frozen_evidence()
    }

    /// 业务作用：读取有界失败证据最近样本，保留旧返回形态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含业务载荷的最近失败记录。
    pub fn frozen_evidence(&self) -> Vec<napart::FrozenEvidence> {
        self.runner.frozen_evidence().recent
    }
}

/// 业务作用：把分区数转换为 napart 实际使用的非零 2 的幂。
///
/// 参数说明：
/// - `partitions`: 用户期望分区数；零只在配置错误路径按一计算。
///
/// 返回：不超过实现上限的规范化分区数。
fn normalized_partitions(partitions: usize) -> usize {
    partitions
        .max(1)
        .checked_next_power_of_two()
        .unwrap_or(MAX_PARTITIONS)
        .min(MAX_PARTITIONS)
}

#[derive(Clone)]
/// 业务作用：原子发布 Ready 后可访问的默认 Runner 与命名只读句柄表。
struct PublishedRunners {
    default_runner: napart::RunnerName,
    handles: BTreeMap<napart::RunnerName, PartitionApplicationHandle>,
}

/// YAML/UserHook 计划、readiness 与 Prepare 原子发布共用的命名状态。
pub(crate) struct PartitionRuntimeState {
    plans: Mutex<PartitionPlanState>,
    contributors: Mutex<BTreeMap<napart::RunnerName, ReadinessContributor>>,
    yaml_names: Mutex<BTreeSet<napart::RunnerName>>,
    published: OnceLock<PublishedRunners>,
}

enum PartitionPlanState {
    Open(BTreeMap<napart::RunnerName, PartitionApplicationPlan>),
    Taken,
}

impl PartitionRuntimeState {
    /// 业务作用：创建 UserHook 计划入口开放、尚无 Runner 或 readiness 发布的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可接收有界命名计划的新状态。
    pub(crate) fn new() -> Self {
        Self {
            plans: Mutex::new(PartitionPlanState::Open(BTreeMap::new())),
            contributors: Mutex::new(BTreeMap::new()),
            yaml_names: Mutex::new(BTreeSet::new()),
            published: OnceLock::new(),
        }
    }

    /// 业务作用：在 UserHook 为稳定名称登记一次纯参数计划，不创建任何 Runner。
    ///
    /// 参数说明：
    /// - `name`: 已校验 Runner 名称。
    /// - `plan`: 已完成容量校验的计划。
    ///
    /// 返回：名称首次登记成功；重复或 Prepare 已封口时返回阶段错误。
    pub(crate) fn configure(
        &self,
        name: napart::RunnerName,
        plan: PartitionApplicationPlan,
    ) -> ApplicationResult<()> {
        let plan = plan.validated_at(ApplicationPhase::UserHook)?;
        let mut state = self
            .plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            PartitionPlanState::Open(plans) if !plans.contains_key(&name) => {
                plans.insert(name, plan);
                Ok(())
            }
            PartitionPlanState::Open(_) => Err(partition_error(
                ApplicationPhase::UserHook,
                "partition plan can be configured only once per Runner name",
            )),
            PartitionPlanState::Taken => Err(partition_error(
                ApplicationPhase::Prepare,
                "partition plan registration is closed",
            )),
        }
    }

    /// 业务作用：登记一个 Runner 的独占 readiness 更新句柄，名称在 Seal 前保持有界。
    ///
    /// 参数说明：
    /// - `name`: readiness 对应的 Runner 名称。
    /// - `contributor`: Application 注册表返回的独占更新句柄。
    ///
    /// 返回：首次登记成功；重复名称返回 Start 错误。
    pub(crate) fn install_contributor(
        &self,
        name: napart::RunnerName,
        contributor: ReadinessContributor,
    ) -> ApplicationResult<()> {
        if self
            .contributors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name, contributor)
            .is_some()
        {
            return Err(partition_error(
                ApplicationPhase::Start,
                "partition Runner readiness was already registered",
            ));
        }
        Ok(())
    }

    /// 业务作用：判断 YAML Start 阶段是否已占用名称，供 UserHook 输出配置源冲突语义。
    ///
    /// 参数说明：
    /// - `name`: 待配置的稳定名称。
    ///
    /// 返回：该名称已有 readiness 贡献项时返回 true。
    pub(crate) fn has_yaml_plan(&self, name: &napart::RunnerName) -> bool {
        self.yaml_names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(name)
    }

    /// 业务作用：记录 Start 阶段从 YAML 取得的稳定名称，UserHook 同名配置必须明确拒绝。
    ///
    /// 参数说明：
    /// - `name`: 已成功登记 readiness 的 YAML Runner 名称。
    ///
    /// 返回：首次记录返回 true；重复名称返回 false。
    fn mark_yaml_plan(&self, name: napart::RunnerName) -> bool {
        self.yaml_names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name)
    }

    /// 业务作用：在 Prepare 原子关闭 UserHook 入口，合并不重名的 YAML 与动态命名计划。
    ///
    /// 参数说明：
    /// - `configured`: 最终 YAML 中的可选计划集合。
    ///
    /// 返回：至少一个计划且 default 已声明时返回完整集合；同名冲突或缺失时拒绝启动。
    fn take_plans(
        &self,
        configured: Option<PartitionPlanSet>,
    ) -> ApplicationResult<PartitionPlanSet> {
        let mut state = self
            .plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::mem::replace(&mut *state, PartitionPlanState::Taken);
        let PartitionPlanState::Open(mut hook) = previous else {
            return Err(partition_error(
                ApplicationPhase::Prepare,
                "partition plans were already consumed",
            ));
        };
        let mut default_runner = runner_name(napart::DEFAULT_RUNNER, ApplicationPhase::Prepare)?;
        if let Some(configured) = configured {
            default_runner = configured.default_runner;
            for (name, plan) in configured.runners {
                if hook.insert(name, plan).is_some() {
                    return Err(partition_error(
                        ApplicationPhase::Prepare,
                        "partition plan conflict: use either YAML `partition` or configure_partition for the same Runner, not both",
                    ));
                }
            }
        }
        if hook.is_empty() {
            return Err(partition_error(
                ApplicationPhase::Prepare,
                "partition component requires YAML `partition` or configure_partition during the Service user hook",
            ));
        }
        if !hook.contains_key(&default_runner) {
            return Err(partition_error(
                ApplicationPhase::Prepare,
                "partition default Runner is not configured",
            ));
        }
        Ok(PartitionPlanSet {
            default_runner,
            runners: hook,
        })
    }

    /// 业务作用：取得计划集合中每个名称已经注册的 readiness 句柄。
    ///
    /// 参数说明：
    /// - `plans`: Prepare 已冻结的完整计划集合。
    ///
    /// 返回：每个名称都有贡献项时返回克隆映射；缺失表示生命周期登记不完整并拒绝启动。
    fn contributors_for(
        &self,
        plans: &PartitionPlanSet,
    ) -> ApplicationResult<BTreeMap<napart::RunnerName, ReadinessContributor>> {
        let contributors = self
            .contributors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut result = BTreeMap::new();
        for name in plans.runners.keys() {
            let contributor = contributors.get(name).cloned().ok_or_else(|| {
                partition_error(
                    ApplicationPhase::Prepare,
                    format!("partition Runner `{name}` readiness was not registered"),
                )
            })?;
            result.insert(name.clone(), contributor);
        }
        Ok(result)
    }

    /// 业务作用：全部 Runner 成功启动并进入资源容器后一次性发布名称到业务句柄的映射。
    ///
    /// 参数说明：
    /// - `published`: default 名称和全部业务投影。
    ///
    /// 返回：首次发布成功；重复发布返回 Prepare 错误。
    fn publish(&self, published: PublishedRunners) -> ApplicationResult<()> {
        self.published.set(published).map_err(|_| {
            partition_error(
                ApplicationPhase::Prepare,
                "partition Runner map was already published",
            )
        })
    }

    /// 业务作用：取得 default Runner 的业务句柄，保持 `app.partition()` 源码兼容。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Prepare 已发布且 default 仍接纳时返回句柄；否则返回阶段错误。
    pub(crate) fn default_runner(&self) -> ApplicationResult<PartitionApplicationHandle> {
        let published = self.published.get().ok_or_else(|| {
            partition_error(
                ApplicationPhase::Prepare,
                "partition Runners are not published yet",
            )
        })?;
        self.runner(published.default_runner.as_str())
    }

    /// 业务作用：按稳定名称取得已发布且仍接纳任务的 Runner 业务句柄。
    ///
    /// 参数说明：
    /// - `name`: 配置中声明的 Runner 名称。
    ///
    /// 返回：名称存在且 Runner 仍 Accepting 时返回句柄；否则返回明确运行期错误。
    pub(crate) fn runner(&self, name: &str) -> ApplicationResult<PartitionApplicationHandle> {
        let name = runner_name(name, ApplicationPhase::Running)?;
        let handle = self
            .published
            .get()
            .and_then(|published| published.handles.get(&name))
            .cloned()
            .ok_or_else(|| {
                partition_error(
                    ApplicationPhase::Running,
                    format!("partition Runner `{name}` is not published"),
                )
            })?;
        if !handle.is_started() {
            return Err(partition_error(
                ApplicationPhase::Running,
                format!("partition Runner `{name}` is stopping or stopped"),
            ));
        }
        Ok(handle)
    }
}

/// 受管命名 Runner 组件。
pub(crate) struct PartitionComponent {
    critical_task: Option<ApplicationFuture<'static>>,
}

impl PartitionComponent {
    /// 业务作用：创建尚未登记 readiness、计划或 Runner 的生命周期组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：等待 Start 与 Prepare 的组件。
    pub(crate) fn new() -> Self {
        Self {
            critical_task: None,
        }
    }
}

impl ApplicationComponent for PartitionComponent {
    /// 业务作用：返回命名分区执行域的稳定组件身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Partition 组件身份。
    fn id(&self) -> ComponentId {
        ComponentId::Partition
    }

    /// 业务作用：为 YAML 已知 Runner 预登记独立 readiness；UserHook Runner 在配置调用中登记。
    ///
    /// 参数说明：
    /// - `context`: 提供共享 Application 和最终配置快照的 Start 上下文。
    ///
    /// 返回：全部稳定名称登记成功时完成；名称或策略冲突时拒绝启动。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let configured = partition_plans_from_tree(
                context.application().config().value(),
                ApplicationPhase::Start,
            )?;
            if let Some(configured) = configured {
                for (name, plan) in configured.runners {
                    let contributor =
                        register_runner_readiness(context.application(), &name, plan.critical)?;
                    context
                        .application()
                        .partition_runtime()
                        .install_contributor(name.clone(), contributor)?;
                    if !context
                        .application()
                        .partition_runtime()
                        .mark_yaml_plan(name)
                    {
                        return Err(partition_error(
                            ApplicationPhase::Start,
                            "partition YAML contains a duplicate Runner name",
                        ));
                    }
                }
            }
            Ok(())
        })
    }

    /// 业务作用：按稳定名称批量创建并启动 Runner，全部成功后原子发布映射与反序停机权。
    ///
    /// 参数说明：
    /// - `context`: 提供资源登记、active stack 和共享启动 deadline 的 Prepare 上下文。
    ///
    /// 返回：全部 Runner 已受管且业务映射发布时成功；任一失败先反序收口已启动项再返回错误。
    fn prepare<'a>(&'a mut self, context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let runtime = context.application().partition_runtime();
            let configured = partition_plans_from_tree(
                context.application().config().value(),
                ApplicationPhase::Prepare,
            )?;
            let plans = runtime.take_plans(configured)?;
            let contributors = runtime.contributors_for(&plans)?;
            let registry = napart::PartitionRunnerRegistry::builder()
                .max_runners(plans.runners.len().max(64))
                .build()
                .map_err(|error| {
                    partition_error(
                        ApplicationPhase::Prepare,
                        format!("failed to create partition Runner registry: {error}"),
                    )
                })?;
            let mut started = Vec::with_capacity(plans.runners.len());
            for (name, plan) in &plans.runners {
                let runner = registry
                    .get_or_create(name.as_str(), plan.config.clone())
                    .map_err(|error| {
                        partition_error(
                            ApplicationPhase::Prepare,
                            format!("failed to register partition Runner `{name}`: {error}"),
                        )
                    })?;
                started.push((name.clone(), runner.clone()));
                if let Err(error) = runner.start().await {
                    rollback_started(&started, context.deadline()).await;
                    return Err(partition_error(
                        ApplicationPhase::Prepare,
                        format!("failed to start partition Runner `{name}`: {error}"),
                    ));
                }
            }

            let handles = started
                .iter()
                .map(|(name, runner)| {
                    (
                        name.clone(),
                        PartitionApplicationHandle::new(runner.clone()),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            for (name, handle) in &handles {
                if let Err(error) = context.register_resource(Some(name.as_str()), handle.clone()) {
                    rollback_started(&started, context.deadline()).await;
                    return Err(error);
                }
            }
            let default_handle = handles
                .get(&plans.default_runner)
                .cloned()
                .expect("default Runner membership was validated");
            if let Err(error) = context.register_resource(None, default_handle) {
                rollback_started(&started, context.deadline()).await;
                return Err(error);
            }
            if let Err(error) = runtime.publish(PublishedRunners {
                default_runner: plans.default_runner.clone(),
                handles,
            }) {
                rollback_started(&started, context.deadline()).await;
                return Err(error);
            }

            let mut managed = Vec::with_capacity(started.len());
            for (name, runner) in started {
                let plan = plans
                    .runners
                    .get(&name)
                    .cloned()
                    .expect("started Runner has a frozen plan");
                let contributor = contributors
                    .get(&name)
                    .cloned()
                    .expect("started Runner has readiness authority");
                contributor.observe(
                    DependencyState::Ready,
                    reason::HEALTHY,
                    std::time::Instant::now(),
                );
                managed.push(ManagedRunner {
                    name,
                    runner,
                    plan,
                    contributor,
                });
            }
            self.critical_task = Some(Box::pin(run_health_monitor(
                context.application().clone(),
                managed.clone(),
            )));
            // 业务映射与 readiness 同时可见后才发布停机所有权，后续任何启动失败都会先关闭
            // 全部 Runner，再释放资源容器中的业务句柄。
            context.activate(Box::new(PartitionShutdown { managed, registry }));
            Ok(())
        })
    }

    /// 业务作用：把命名 Runner 健康监控移交应用 Supervisor。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Prepare 已建立 Runner 时返回一次性关键任务，否则返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("partition-runner-health-monitor", task))
    }
}

#[derive(Clone)]
/// 业务作用：把一个已启动 Runner 与其冻结计划、名称和 readiness 更新权绑定到同一所有权单元。
struct ManagedRunner {
    name: napart::RunnerName,
    runner: napart::PartitionRunner,
    plan: PartitionApplicationPlan,
    contributor: ReadinessContributor,
}

/// 业务作用：为一个稳定 Runner 名称登记独立 readiness，策略不使用业务动态标签。
///
/// 参数说明：
/// - `application`: 当前 Application。
/// - `name`: 已校验 Runner 名称。
/// - `critical`: 是否影响整体 readiness 并触发统一停机。
///
/// 返回：首次登记返回独占更新句柄；名称冲突或 Seal 后返回错误。
pub(crate) fn register_runner_readiness(
    application: &Application,
    name: &napart::RunnerName,
    critical: bool,
) -> ApplicationResult<ReadinessContributor> {
    application.register_readiness(
        ComponentId::Partition,
        Arc::<str>::from(format!("partition:runner:{name}")),
        ReadinessPolicy {
            affects_ready: critical,
            failure_threshold: 1,
            recovery_threshold: 1,
            stale_after: Some(HEALTH_STALE_AFTER),
        },
    )
}

/// 业务作用：任一 Prepare 启动失败时反序关闭已经创建的 Runner，避免暴露半启动执行域。
///
/// 参数说明：
/// - `started`: 已按稳定名称顺序创建的 Runner。
/// - `deadline`: 全部回滚共享的绝对启动期限。
///
/// 返回：无；每个 Runner 都先尝试无损停止，期限不足时显式升级有损模式。
async fn rollback_started(
    started: &[(napart::RunnerName, napart::PartitionRunner)],
    deadline: Instant,
) {
    for (_, runner) in started.iter().rev() {
        if runner.stop(deadline).await.is_err() {
            let _ = runner.force_stop(deadline).await;
        }
    }
}

/// 业务作用：周期观察每个 Runner 的独立健康；仅关键 Runner 失权会触发 Application 停机。
///
/// 参数说明：
/// - `application`: 用于区分正常运行与停机阶段。
/// - `managed`: Prepare 发布的稳定 Runner 与 readiness 列表。
///
/// 返回：正常停机时成功；关键 Runner 进入 Degraded、Failed 或意外停止时返回运行错误。
async fn run_health_monitor(
    application: Application,
    managed: Vec<ManagedRunner>,
) -> ApplicationResult<()> {
    loop {
        if matches!(
            application.state(),
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
        ) {
            for item in &managed {
                item.contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                );
            }
            return Ok(());
        }
        for item in &managed {
            match item.runner.health() {
                napart::RunnerHealth::Healthy => item.contributor.observe(
                    DependencyState::Ready,
                    reason::HEALTHY,
                    std::time::Instant::now(),
                ),
                napart::RunnerHealth::Starting => item.contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                ),
                napart::RunnerHealth::Degraded
                | napart::RunnerHealth::Failed
                | napart::RunnerHealth::Stopping
                | napart::RunnerHealth::Stopped => {
                    item.contributor.observe(
                        DependencyState::NotReady,
                        reason::PARTITION_UNHEALTHY,
                        std::time::Instant::now(),
                    );
                    if item.plan.critical {
                        return Err(partition_error(
                            ApplicationPhase::Running,
                            format!("critical partition Runner `{}` is not healthy", item.name),
                        ));
                    }
                }
            }
        }
        tokio::time::sleep(HEALTH_INTERVAL).await;
    }
}

/// 停机 action 持有命名注册表和全部 Runner 的显式收口权。
struct PartitionShutdown {
    managed: Vec<ManagedRunner>,
    registry: napart::PartitionRunnerRegistry,
}

impl ShutdownAction for PartitionShutdown {
    /// 业务作用：返回停机报告使用的稳定 action 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定名称，不含 Runner 或业务动态值。
    fn label(&self) -> &'static str {
        "partition-runners"
    }

    /// 业务作用：先关闭全部 Runner 入口，再按启动反序使用独立计划预算取得退出证明。
    ///
    /// 参数说明：
    /// - `context`: 提供全部 active step 共用的绝对停机期限。
    ///
    /// 返回：全部 Runner 无损退出时成功；未收敛或产生冻结、中止证据时聚合名称与计数返回错误。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let _registry_guard = &self.registry;
            for item in &self.managed {
                item.runner.request_stop();
                item.contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                );
            }
            let mut failures = Vec::new();
            for item in self.managed.iter().rev() {
                let total = context.child_budget(item.plan.config.shutdown_timeout);
                let final_deadline = Instant::now()
                    .checked_add(total)
                    .unwrap_or_else(Instant::now);
                let graceful = total / 2;
                let graceful_deadline = Instant::now()
                    .checked_add(graceful)
                    .unwrap_or_else(Instant::now);
                let report = match item.runner.stop(graceful_deadline).await {
                    Ok(report) => Some(report),
                    Err(_) if item.plan.force_on_timeout => {
                        // 无损阶段与有损阶段共享同一绝对期限，前一阶段消耗的时间自动从后续
                        // 预算扣除；再次从 context 取相对预算会因调度时刻变化而意外缩短期限。
                        match item.runner.force_stop(final_deadline).await {
                            Ok(report) => Some(report),
                            Err(_) => {
                                failures.push(format!(
                                    "{} did not obtain forced exit proof",
                                    item.name
                                ));
                                None
                            }
                        }
                    }
                    Err(_) => {
                        failures.push(format!(
                            "{} did not converge before its graceful deadline",
                            item.name
                        ));
                        None
                    }
                };
                if let Some(report) = report {
                    if report.frozen > 0 || report.aborted > 0 {
                        failures.push(format!(
                            "frozen={}, aborted={}, timed_out_lanes={}, runner={}",
                            report.frozen, report.aborted, report.timed_out_lanes, item.name
                        ));
                    }
                }
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(partition_error(
                    ApplicationPhase::Stopping,
                    format!(
                        "partition shutdown retained loss evidence: {}",
                        failures.join("; ")
                    ),
                ))
            }
        })
    }
}

/// 业务作用：校验 Runner 名称并把注册表错误归属到 Application 阶段。
///
/// 参数说明：
/// - `name`: YAML 或 UserHook 提供的稳定名称。
/// - `phase`: 错误归属阶段。
///
/// 返回：名称合法返回冻结值；否则返回 Partition 配置错误。
pub(crate) fn runner_name(
    name: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<napart::RunnerName> {
    napart::RunnerName::new(name).map_err(|error| {
        partition_error(
            phase,
            format!("invalid partition Runner name `{name}`: {error}"),
        )
    })
}

/// 业务作用：创建不包含业务载荷、凭据或无界动态值的 Partition 生命周期错误。
///
/// 参数说明：
/// - `phase`: 错误被裁决时的 Application 生命周期阶段。
/// - `message`: 稳定配置或结构状态摘要。
///
/// 返回：归属 Partition 组件的统一错误。
fn partition_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Partition, phase, message)
}
