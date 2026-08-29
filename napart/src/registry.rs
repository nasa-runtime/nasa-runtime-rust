//! 命名 Runner 注册表与冻结配置。
//!
//! 注册表由调用方显式拥有，不使用库级全局状态。同名身份只在同一个注册表内成立；业务若需要从
//! 多个调用点取得相同 Runner，必须共享进程级注册表，不能按请求重复构造。不同注册表中的同名
//! Runner 是彼此隔离的执行域。同名 Runner 首次配置后永久冻结，注册表不自动驱逐对象，避免旧业务
//! 句柄在同名重建后指向另一个执行域。

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::runner::PartitionRunner;
use crate::ShutdownReport;

/// 兼容入口和受管 Application 默认使用的稳定 Runner 名称。
pub const DEFAULT_RUNNER: &str = "default";

const DEFAULT_MAX_RUNNERS: usize = 64;
const MAX_RUNNERS_LIMIT: usize = 4_096;
const MAX_RUNNER_NAME_BYTES: usize = 128;
const MAX_PARTITIONS: usize = 65_536;
const MAX_TYPE_STATES: usize = 1_048_576;
const MAX_DURATION: Duration = Duration::from_secs(31_536_000);

/// 已校验、可用于指标标签和注册表索引的稳定 Runner 名称。
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunnerName(Arc<str>);

impl RunnerName {
    /// 业务作用：校验并冻结 Runner 名称，阻止空白、控制字符、首尾空格和超长标签进入
    /// 注册表与指标系统。
    ///
    /// 参数说明：
    /// - `name`: 配置或有界业务常量提供的名称。
    ///
    /// 返回：合法名称返回稳定值；不满足边界时返回 InvalidName。
    pub fn new(name: impl AsRef<str>) -> Result<Self, RunnerRegistryError> {
        let name = name.as_ref();
        if name.is_empty()
            || name.len() > MAX_RUNNER_NAME_BYTES
            || name.trim() != name
            || name.chars().any(char::is_control)
        {
            return Err(RunnerRegistryError::InvalidName);
        }
        Ok(Self(Arc::from(name)))
    }

    /// 业务作用：读取已校验名称文本，供配置映射、日志与低基数指标使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的名称。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RunnerName {
    /// 业务作用：渲染稳定 Runner 名称，供配置错误与运行诊断定位执行域。
    ///
    /// 参数说明：
    /// - `f`: 接收结构化名称的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RunnerName").field(&self.as_str()).finish()
    }
}

impl fmt::Display for RunnerName {
    /// 业务作用：按原始已校验文本展示 Runner 名称。
    ///
    /// 参数说明：
    /// - `f`: 接收原始名称文本的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for RunnerName {
    /// 业务作用：把名称借用为字符串切片，避免注册表查询产生临时分配。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 `RunnerName` 生命周期一致的已校验文本切片。
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// 单个 Runner 的完整冻结配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerConfig {
    /// 分区 worker 数；注册时规范化为不小于该值的 2 的幂。
    pub partitions: usize,
    /// 每个 `(home, TaskType)` 的排队容量。
    pub queue_capacity_per_type: usize,
    /// 当前 Runner 全部延迟、排队、迁移和执行中任务总上限。
    pub global_inflight: usize,
    /// 当前 generation 允许创建的类型状态总数。
    pub max_type_states: usize,
    /// 有界冻结诊断环的容量。
    pub frozen_evidence_capacity: usize,
    /// 单 slot 允许同时接入的盗洞上限。
    pub max_inbound_tunnels: usize,
    /// 触发热点迁移观察的排队任务阈值。
    pub idle_task_threshold: usize,
    /// 每次观察中严格候选允许的接力次数。
    pub strict_opportunity_attempts: usize,
    /// 连续多少次低负载观察后请求严格归还。
    pub return_observations: usize,
    /// 非严格盗洞没有进展时的租约上界。
    pub tunnel_lease: Duration,
    /// 全量负载观察间隔。
    pub load_observer_interval: Duration,
    /// 活动迁移、归还和失败收口的控制推进间隔。
    pub control_tick: Duration,
    /// 单次任务移动允许保持 Moving 的最长时长。
    pub transition_timeout: Duration,
    /// standalone 兼容入口默认无损等待预算。
    pub shutdown_timeout: Duration,
    /// worker 单轮最多处理的主队列与盗洞任务数。
    pub drain_batch: usize,
}

impl RunnerConfig {
    /// 业务作用：完整校验并规范化 Runner 配置，确保所有注册表、队列、定时器和观测基数
    /// 都有明确上界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：合法时返回分区数已规范化的冻结副本；零值、溢出或不可表示时长返回 InvalidConfig。
    pub fn validated(&self) -> Result<Self, RunnerRegistryError> {
        if self.partitions == 0 || self.partitions > MAX_PARTITIONS {
            return Err(RunnerRegistryError::InvalidConfig(
                "partitions must be within 1..=65536",
            ));
        }
        let partitions = self
            .partitions
            .checked_next_power_of_two()
            .filter(|value| *value <= MAX_PARTITIONS)
            .ok_or(RunnerRegistryError::InvalidConfig(
                "normalized partitions exceed 65536",
            ))?;
        if self.queue_capacity_per_type == 0
            || self.queue_capacity_per_type > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(RunnerRegistryError::InvalidConfig(
                "queue_capacity_per_type exceeds the Tokio semaphore capacity",
            ));
        }
        if self.global_inflight == 0 || self.global_inflight > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(RunnerRegistryError::InvalidConfig(
                "global_inflight exceeds the Tokio semaphore capacity",
            ));
        }
        if self.max_type_states < partitions || self.max_type_states > MAX_TYPE_STATES {
            return Err(RunnerRegistryError::InvalidConfig(
                "max_type_states must cover every partition and remain bounded",
            ));
        }
        if self.frozen_evidence_capacity == 0
            || self.max_inbound_tunnels == 0
            || self.idle_task_threshold == 0
            || self.strict_opportunity_attempts == 0
            || self.return_observations == 0
            || self.drain_batch == 0
        {
            return Err(RunnerRegistryError::InvalidConfig(
                "all capacities and control counts must be positive",
            ));
        }
        for (duration, name) in [
            (self.tunnel_lease, "tunnel_lease"),
            (self.load_observer_interval, "load_observer_interval"),
            (self.control_tick, "control_tick"),
            (self.transition_timeout, "transition_timeout"),
            (self.shutdown_timeout, "shutdown_timeout"),
        ] {
            if duration.is_zero() || duration > MAX_DURATION {
                return Err(RunnerRegistryError::InvalidConfig(name));
            }
        }
        let mut validated = self.clone();
        validated.partitions = partitions;
        Ok(validated)
    }
}

impl Default for RunnerConfig {
    /// 业务作用：提供适合通用服务的有界 Runner 配置，调用方可以按独立业务负载覆盖字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分区数为可用并行度两倍并规范化为 2 的幂，其余字段均有固定资源上界。
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let partitions = parallelism
            .saturating_mul(2)
            .next_power_of_two()
            .min(MAX_PARTITIONS);
        let queue_capacity_per_type = 65_536;
        Self {
            partitions,
            queue_capacity_per_type,
            global_inflight: partitions
                .saturating_mul(queue_capacity_per_type.saturating_add(1))
                .min(tokio::sync::Semaphore::MAX_PERMITS),
            max_type_states: 4_096_usize.max(partitions),
            frozen_evidence_capacity: 1_024,
            max_inbound_tunnels: 64,
            idle_task_threshold: 8,
            strict_opportunity_attempts: 2,
            return_observations: 3,
            tunnel_lease: Duration::from_secs(2),
            load_observer_interval: Duration::from_secs(1),
            control_tick: Duration::from_millis(1),
            transition_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(2),
            drain_batch: 64,
        }
    }
}

/// 命名 Runner 注册或查询失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerRegistryError {
    /// 名称不满足稳定标签约束。
    InvalidName,
    /// 配置字段不满足有界运行合同。
    InvalidConfig(&'static str),
    /// 同名 Runner 已用另一份完整配置冻结。
    ConfigConflict,
    /// 新名称会超过注册表配置的最大 Runner 数。
    RegistryFull,
    /// 进程内 RunnerId 已耗尽，不能回绕复用。
    IdExhausted,
}

impl fmt::Display for RunnerRegistryError {
    /// 业务作用：把注册表拒绝格式化为稳定文本，供配置面分类展示。
    ///
    /// 参数说明：
    /// - `f`: 接收稳定拒绝文本的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName => f.write_str("invalid partition runner name"),
            Self::InvalidConfig(field) => write!(f, "invalid partition runner config: {field}"),
            Self::ConfigConflict => f.write_str("partition runner name is bound to another config"),
            Self::RegistryFull => f.write_str("partition runner registry is full"),
            Self::IdExhausted => f.write_str("partition runner id space is exhausted"),
        }
    }
}

impl std::error::Error for RunnerRegistryError {}

/// 注册表构造器。
#[derive(Debug, Clone)]
pub struct PartitionRunnerRegistryBuilder {
    max_runners: usize,
}

impl PartitionRunnerRegistryBuilder {
    /// 业务作用：设置注册表允许冻结的稳定名称总数，限制 Runner 与指标标签基数。
    ///
    /// 参数说明：
    /// - `max_runners`: 必须位于 1 到 4096。
    ///
    /// 返回：链式返回更新后的构造器；最终 `build` 统一校验。
    pub fn max_runners(mut self, max_runners: usize) -> Self {
        self.max_runners = max_runners;
        self
    }

    /// 业务作用：创建不含任何 Runner 的显式注册表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：上限合法时返回注册表；否则返回 InvalidConfig。
    pub fn build(self) -> Result<PartitionRunnerRegistry, RunnerRegistryError> {
        if self.max_runners == 0 || self.max_runners > MAX_RUNNERS_LIMIT {
            return Err(RunnerRegistryError::InvalidConfig(
                "max_runners must be within 1..=4096",
            ));
        }
        Ok(PartitionRunnerRegistry {
            inner: Arc::new(RegistryInner {
                max_runners: self.max_runners,
                next_id: AtomicU64::new(1),
                runners: Mutex::new(BTreeMap::new()),
            }),
        })
    }
}

/// 业务作用：集中拥有命名 Runner 的容量、身份分配器与唯一实例表。
struct RegistryInner {
    max_runners: usize,
    next_id: AtomicU64,
    runners: Mutex<BTreeMap<RunnerName, PartitionRunner>>,
}

/// 显式拥有的命名 Runner 注册表。
#[derive(Clone)]
pub struct PartitionRunnerRegistry {
    inner: Arc<RegistryInner>,
}

impl PartitionRunnerRegistry {
    /// 业务作用：创建带默认名称上限的注册表构造器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认最多冻结 64 个 Runner 名称的构造器。
    pub fn builder() -> PartitionRunnerRegistryBuilder {
        PartitionRunnerRegistryBuilder {
            max_runners: DEFAULT_MAX_RUNNERS,
        }
    }

    /// 业务作用：按稳定名称幂等取得 Runner；首次调用冻结完整配置，后续不同配置必须拒绝。
    ///
    /// 参数说明：
    /// - `name`: 需要注册或查询的稳定名称。
    /// - `config`: 首次注册时冻结的完整配置。
    ///
    /// 返回：同名同配置返回同一控制对象；名称、配置、容量或 ID 不合法时返回明确错误。
    pub fn get_or_create(
        &self,
        name: impl AsRef<str>,
        config: RunnerConfig,
    ) -> Result<PartitionRunner, RunnerRegistryError> {
        let name = RunnerName::new(name)?;
        let config = config.validated()?;
        let mut runners = self.inner.runners.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = runners.get(&name) {
            return if existing.config() == &config {
                Ok(existing.clone())
            } else {
                Err(RunnerRegistryError::ConfigConflict)
            };
        }
        if runners.len() >= self.inner.max_runners {
            return Err(RunnerRegistryError::RegistryFull);
        }
        let id = self
            .inner
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| RunnerRegistryError::IdExhausted)?;
        let runner = PartitionRunner::registered(id, name.clone(), config);
        runners.insert(name, runner.clone());
        Ok(runner)
    }

    /// 业务作用：只按已校验文本查询注册表，不创建名称也不改变配置冻结状态。
    ///
    /// 参数说明：
    /// - `name`: 需要查找的名称文本。
    ///
    /// 返回：名称合法且已注册时返回同一 Runner 控制对象，否则返回 None。
    pub fn get(&self, name: impl AsRef<str>) -> Option<PartitionRunner> {
        let name = RunnerName::new(name).ok()?;
        self.inner
            .runners
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&name)
            .cloned()
    }

    /// 业务作用：先向全部 Runner 发布停止请求，再按稳定名称等待各自退出，避免顺序等待期间
    /// 尚未轮到的 Runner 继续接纳任务。
    ///
    /// 参数说明：
    /// - `deadline`: 全部 Runner 共享的绝对停止期限。
    ///
    /// 返回：按名称排序的逐 Runner 结果；任一项未收敛时 `converged` 为 false。
    pub async fn stop_all(&self, deadline: Instant) -> StopAllReport {
        let runners = self
            .inner
            .runners
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for runner in &runners {
            runner.request_stop();
        }
        let mut results = Vec::with_capacity(runners.len());
        for runner in runners.into_iter().rev() {
            let name = runner.name().clone();
            let result = runner.stop(deadline).await;
            results.push(StopAllEntry {
                name,
                converged: result.is_ok(),
                report: result.ok(),
            });
        }
        results.sort_by(|left, right| left.name.cmp(&right.name));
        StopAllReport {
            converged: results.iter().all(|entry| entry.converged),
            runners: results,
        }
    }
}

impl Default for PartitionRunnerRegistry {
    /// 业务作用：创建使用默认 Runner 数上限的空注册表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接注册命名 Runner 的实例。
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default runner registry bounds are valid")
    }
}

/// `stop_all` 中单个 Runner 的稳定结果。
#[derive(Debug, Clone)]
pub struct StopAllEntry {
    /// Runner 名称。
    pub name: RunnerName,
    /// 是否在共享期限内取得退出证明。
    pub converged: bool,
    /// 收敛成功时的损耗报告。
    pub report: Option<ShutdownReport>,
}

/// 注册表聚合停止报告。
#[derive(Debug, Clone)]
pub struct StopAllReport {
    /// 全部 Runner 是否都取得退出证明。
    pub converged: bool,
    /// 按稳定名称排序的逐 Runner 结果。
    pub runners: Vec<StopAllEntry>,
}
