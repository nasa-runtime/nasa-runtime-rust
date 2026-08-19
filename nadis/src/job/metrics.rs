//! RedisJob 低基数观测快照：运行循环只写进程内累计，查询不访问 Redis，也不接收运行期任意 label。

use std::collections::BTreeMap;
use std::sync::Mutex;

/// 调度触发脚本的封闭结局；文本用于 `redis_job_fire_total{result}` 的固定 label 域。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobFireResult {
    /// 首次建立 Run 与 Dispatch 消息。
    Fired,
    /// 同一逻辑时刻已存在 Run，采用既有事实。
    Adopted,
    /// 误触发策略只推进逻辑时刻。
    Skipped,
    /// 候选尚未到期。
    NotDue,
    /// 下一逻辑时刻需要按新的 Redis 时间重算。
    NeedRecompute,
    /// 定义或命名空间状态不允许触发。
    StateMismatch,
    /// 定义修订或调度 score 已被其它节点推进。
    Stale,
}

impl JobFireResult {
    /// 业务作用：返回触发指标使用的稳定低基数结果文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与脚本封闭结局一一对应的 label 值。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fired => "fired",
            Self::Adopted => "adopted",
            Self::Skipped => "skipped",
            Self::NotDue => "not_due",
            Self::NeedRecompute => "need_recompute",
            Self::StateMismatch => "state_mismatch",
            Self::Stale => "stale",
        }
    }

    /// 业务作用：把批量触发脚本的已校验返回码映射为封闭指标结果。
    ///
    /// 参数说明：`code` 为 `fire_due_batch.lua` 返回的单项状态码。
    ///
    /// 返回：已知码返回对应结果；未知码返回 `None`，由协议层拒绝而不是扩张 label 域。
    pub(crate) fn from_script_code(code: &str) -> Option<Self> {
        match code {
            "OK" => Some(Self::Fired),
            "ADOPTED" => Some(Self::Adopted),
            "SKIPPED" => Some(Self::Skipped),
            "NOT_DUE" => Some(Self::NotDue),
            "NEED_RECOMPUTE" => Some(Self::NeedRecompute),
            "STATE_MISMATCH" => Some(Self::StateMismatch),
            "STALE" => Some(Self::Stale),
            _ => None,
        }
    }
}

/// 受监督循环的封闭身份；文本可直接作为低基数指标 label。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobSupervisorLoop {
    /// RESP3 Fanout 订阅与通知定位。
    Subscription,
    /// 普通任务调度、可见性与恢复扫描。
    Scanner,
    /// Dispatch Stream 消费与 Handler 执行。
    Dispatcher,
    /// Fanout receipt、ready、lease 与根对账。
    FanoutMonitor,
    /// Completion、删除队列与 tombstone 有界回收。
    Completion,
    /// 执行器权威心跳。
    Heartbeat,
}

impl JobSupervisorLoop {
    /// 业务作用：返回指标和健康快照使用的稳定低基数文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不会包含 source、任务名或 endpoint 的固定循环名。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Subscription => "subscription",
            Self::Scanner => "scanner",
            Self::Dispatcher => "dispatcher",
            Self::FanoutMonitor => "fanout_monitor",
            Self::Completion => "completion",
            Self::Heartbeat => "heartbeat",
        }
    }
}

/// 监督器最近一次代次迁移的封闭结局。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobSupervisorResult {
    /// 当前代次正常运行。
    Running,
    /// 异常后仍在预算内，已安排下一代。
    RestartScheduled,
    /// 新代次已完成一次成功推进。
    Recovered,
    /// 重启次数或时间窗耗尽。
    BudgetExhausted,
    /// 维持 attempt 权威的循环退出，不允许原地重启。
    AuthorityLost,
}

impl JobSupervisorResult {
    /// 业务作用：返回指标和健康快照使用的稳定结局文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：封闭、低基数的结果名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::RestartScheduled => "restart_scheduled",
            Self::Recovered => "recovered",
            Self::BudgetExhausted => "budget_exhausted",
            Self::AuthorityLost => "authority_lost",
        }
    }
}

/// 单个受监督循环的只读指标投影。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSupervisorMetricsSnapshot {
    /// 封闭循环身份。
    pub loop_name: JobSupervisorLoop,
    /// 当前或下一次将运行的单调代次，初始为 1。
    pub generation: u64,
    /// 已安排的新代次数量。
    pub restart_total: u64,
    /// 最近一次代次迁移结局。
    pub result: JobSupervisorResult,
    /// 各封闭结局的历史累计，供带 `result` label 的 counter 保持单调。
    pub result_totals: BTreeMap<JobSupervisorResult, u64>,
}

#[derive(Debug, Clone)]
struct SupervisorMetric {
    generation: u64,
    restart_total: u64,
    result: JobSupervisorResult,
    result_totals: BTreeMap<JobSupervisorResult, u64>,
}

impl Default for SupervisorMetric {
    /// 业务作用：建立尚未发生重启的首个监督代次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：generation=1、restart_total=0 的运行状态。
    fn default() -> Self {
        Self {
            generation: 1,
            restart_total: 0,
            result: JobSupervisorResult::Running,
            result_totals: ALL_SUPERVISOR_RESULTS
                .iter()
                .copied()
                .map(|result| (result, 0))
                .collect(),
        }
    }
}

/// 一个 source 的全部本地指标快照；数值读取不会发出 Redis 命令。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobMetricsSnapshot {
    /// canonical source id，是冻结计划提供的低基数维度。
    pub qualifier: String,
    /// 固定指标名到当前值的映射；全集在 source 创建时一次性建立，运行期不会增长。
    pub values: BTreeMap<&'static str, i64>,
    /// 调度触发各封闭结局的单调累计；全集在 source 创建时建立。
    pub fire_results: BTreeMap<JobFireResult, u64>,
    /// 每类受监督循环的代次与重启累计。
    pub supervisors: Vec<JobSupervisorMetricsSnapshot>,
    /// Completion 双界裁剪后最近观测到的最旧 Stream ID。
    pub completion_oldest_id: Option<String>,
}

#[derive(Debug)]
struct JobMetricsState {
    values: BTreeMap<&'static str, i64>,
    fire_results: BTreeMap<JobFireResult, u64>,
    shard_gauges: BTreeMap<&'static str, BTreeMap<u32, i64>>,
    supervisors: BTreeMap<JobSupervisorLoop, SupervisorMetric>,
    completion_oldest_id: Option<String>,
}

/// 单 source 进程内指标容器；所有 key 与 label 域在构造时封闭。
#[derive(Debug)]
pub(crate) struct JobMetrics {
    qualifier: String,
    state: Mutex<JobMetricsState>,
}

impl JobMetrics {
    /// 业务作用：按冻结 source 建立完整指标全集，避免运行期因新事件动态增加 descriptor。
    ///
    /// 参数说明：`qualifier` 为计划冻结的 canonical source id。
    ///
    /// 返回：全部计数和 gauge 为零、监督器处于首代的本地容器。
    pub(crate) fn new(qualifier: impl Into<String>) -> Self {
        let values = JOB_METRIC_NAMES
            .iter()
            .copied()
            .map(|name| (name, 0))
            .collect();
        let supervisors = ALL_SUPERVISOR_LOOPS
            .iter()
            .copied()
            .map(|loop_name| (loop_name, SupervisorMetric::default()))
            .collect();
        Self {
            qualifier: qualifier.into(),
            state: Mutex::new(JobMetricsState {
                values,
                fire_results: ALL_FIRE_RESULTS
                    .iter()
                    .copied()
                    .map(|result| (result, 0))
                    .collect(),
                shard_gauges: BTreeMap::new(),
                supervisors,
                completion_oldest_id: None,
            }),
        }
    }

    /// 业务作用：累计一个已预登记的低基数计数器。
    ///
    /// 参数说明：`name` 必须来自本模块固定全集，`delta` 为非负增量。
    ///
    /// 返回：无；未知名称被忽略，防止运行期输入扩张指标域。
    pub(crate) fn add(&self, name: &'static str, delta: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(value) = state.values.get_mut(name) {
            *value = value.saturating_add(delta.max(0));
        }
    }

    /// 业务作用：更新一个已预登记的 gauge。
    ///
    /// 参数说明：`name` 必须来自固定全集，`value` 为当前观测值。
    ///
    /// 返回：无；未知名称被忽略，指标域保持封闭。
    pub(crate) fn set(&self, name: &'static str, value: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(current) = state.values.get_mut(name) {
            *current = value;
        }
    }

    /// 业务作用：更新一个分片局部 gauge，并把全部已观测分片求和发布为 source 聚合值。
    ///
    /// 参数说明：`name` 为固定 gauge 名，`shard` 为冻结布局下标，`value` 为该分片当前值。
    ///
    /// 返回：无；未知指标名被忽略，运行期不会建立新 family。
    pub(crate) fn set_shard_gauge(&self, name: &'static str, shard: u32, value: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.values.contains_key(name) {
            return;
        }
        let aggregate = {
            let shards = state.shard_gauges.entry(name).or_default();
            shards.insert(shard, value.max(0));
            shards.values().copied().fold(0_i64, i64::saturating_add)
        };
        state.values.insert(name, aggregate);
    }

    /// 业务作用：累计一批调度触发的封闭结局，使各 result series 保持独立单调。
    ///
    /// 参数说明：`result` 来自脚本返回码的封闭映射，`delta` 为本轮同类结局数量。
    ///
    /// 返回：无返回值。
    pub(crate) fn record_fire(&self, result: JobFireResult, delta: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(total) = state.fire_results.get_mut(&result) {
            *total = total.saturating_add(delta);
        }
    }

    /// 业务作用：记录一个循环安排新代次，并同步监督器公开计数与 generation。
    ///
    /// 参数说明：`loop_name` 为封闭循环身份。
    ///
    /// 返回：递增后的 generation。
    pub(crate) fn restart_scheduled(&self, loop_name: JobSupervisorLoop) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metric = state.supervisors.entry(loop_name).or_default();
        metric.generation = metric.generation.saturating_add(1);
        metric.restart_total = metric.restart_total.saturating_add(1);
        metric.result = JobSupervisorResult::RestartScheduled;
        if let Some(total) = metric
            .result_totals
            .get_mut(&JobSupervisorResult::RestartScheduled)
        {
            *total = total.saturating_add(1);
        }
        metric.generation
    }

    /// 业务作用：标记一个新代次已成功推进，供健康状态从 Degraded 恢复。
    ///
    /// 参数说明：`loop_name` 为封闭循环身份。
    ///
    /// 返回：无；不改变累计重启次数。
    pub(crate) fn recovered(&self, loop_name: JobSupervisorLoop) {
        self.set_supervisor_result(loop_name, JobSupervisorResult::Recovered);
    }

    /// 业务作用：记录循环重启预算耗尽。
    ///
    /// 参数说明：`loop_name` 为封闭循环身份。
    ///
    /// 返回：无；source 健康门禁由调用方同步关闭。
    pub(crate) fn budget_exhausted(&self, loop_name: JobSupervisorLoop) {
        self.set_supervisor_result(loop_name, JobSupervisorResult::BudgetExhausted);
    }

    /// 业务作用：记录权威循环失效且不得原地重启。
    ///
    /// 参数说明：`loop_name` 为封闭循环身份。
    ///
    /// 返回：无；source 健康门禁由调用方同步关闭。
    pub(crate) fn authority_lost(&self, loop_name: JobSupervisorLoop) {
        self.set_supervisor_result(loop_name, JobSupervisorResult::AuthorityLost);
    }

    /// 业务作用：记录 Completion 双界裁剪数量、最旧 ID 与积压年龄。
    ///
    /// 参数说明：两类裁剪数、最旧 ID、Redis 权威当前时刻共同形成本轮观测。
    ///
    /// 返回：无；累计删除数单调增长，年龄与最旧 ID 覆盖为最近观测。
    pub(crate) fn record_completion_trim(
        &self,
        length_trimmed: i64,
        retention_trimmed: i64,
        oldest_id: Option<&str>,
        redis_now: i64,
    ) {
        self.add("redis_job_completion_length_trimmed_total", length_trimmed);
        self.add(
            "redis_job_completion_retention_trimmed_total",
            retention_trimmed,
        );
        let oldest_millis = oldest_id
            .and_then(|id| id.split_once('-').map(|(millis, _)| millis))
            .and_then(|millis| millis.parse::<i64>().ok());
        self.set(
            "redis_job_completion_oldest_age_ms",
            oldest_millis.map_or(0, |oldest| redis_now.saturating_sub(oldest).max(0)),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.completion_oldest_id = oldest_id.map(ToOwned::to_owned);
    }

    /// 业务作用：读取当前 source 的完整本地指标快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定指标全集、监督代次与 Completion 观测；不访问 Redis。
    pub(crate) fn snapshot(&self) -> JobMetricsSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        JobMetricsSnapshot {
            qualifier: self.qualifier.clone(),
            values: state.values.clone(),
            fire_results: state.fire_results.clone(),
            supervisors: state
                .supervisors
                .iter()
                .map(|(loop_name, metric)| JobSupervisorMetricsSnapshot {
                    loop_name: *loop_name,
                    generation: metric.generation,
                    restart_total: metric.restart_total,
                    result: metric.result,
                    result_totals: metric.result_totals.clone(),
                })
                .collect(),
            completion_oldest_id: state.completion_oldest_id.clone(),
        }
    }

    /// 业务作用：更新一个监督循环的最近结局而不改变 generation 或累计次数。
    ///
    /// 参数说明：`loop_name` 与 `result` 均来自封闭枚举。
    ///
    /// 返回：无返回值。
    fn set_supervisor_result(&self, loop_name: JobSupervisorLoop, result: JobSupervisorResult) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metric = state.supervisors.entry(loop_name).or_default();
        metric.result = result;
        if let Some(total) = metric.result_totals.get_mut(&result) {
            *total = total.saturating_add(1);
        }
    }
}

const ALL_SUPERVISOR_LOOPS: [JobSupervisorLoop; 6] = [
    JobSupervisorLoop::Subscription,
    JobSupervisorLoop::Scanner,
    JobSupervisorLoop::Dispatcher,
    JobSupervisorLoop::FanoutMonitor,
    JobSupervisorLoop::Completion,
    JobSupervisorLoop::Heartbeat,
];

const ALL_SUPERVISOR_RESULTS: [JobSupervisorResult; 5] = [
    JobSupervisorResult::Running,
    JobSupervisorResult::RestartScheduled,
    JobSupervisorResult::Recovered,
    JobSupervisorResult::BudgetExhausted,
    JobSupervisorResult::AuthorityLost,
];

const ALL_FIRE_RESULTS: [JobFireResult; 7] = [
    JobFireResult::Fired,
    JobFireResult::Adopted,
    JobFireResult::Skipped,
    JobFireResult::NotDue,
    JobFireResult::NeedRecompute,
    JobFireResult::StateMismatch,
    JobFireResult::Stale,
];

const JOB_METRIC_NAMES: [&str; 36] = [
    "redis_job_schedule_lag_ms",
    "redis_job_due_scan_total",
    "redis_job_fire_total",
    "redis_job_visible_size",
    "redis_job_running",
    "redis_job_started_total",
    "redis_job_success_total",
    "redis_job_failed_total",
    "redis_job_retry_total",
    "redis_job_dead_total",
    "redis_job_skipped_total",
    "redis_job_lease_expired_total",
    "redis_job_stale_finish_total",
    "redis_job_fencing_regression_total",
    "redis_job_self_fence_total",
    "redis_job_serial_wait_depth",
    "redis_job_executor_capacity",
    "redis_job_definition_conflict_total",
    "redis_job_capable_executor_count",
    "redis_job_invalid_payload_total",
    "redis_job_fanout_shard_total",
    "redis_job_fanout_receipt_timeout_total",
    "redis_job_fanout_receipt_retry_total",
    "redis_job_fanout_reassign_total",
    "redis_job_fanout_stale_assignment_total",
    "redis_job_fanout_delivery_pending",
    "redis_job_fanout_ready_wakeup_total",
    "redis_job_fanout_capacity_defer_total",
    "redis_job_fanout_capacity_exhausted_total",
    "redis_job_fanout_gc_pending",
    "redis_job_registry_gc_total",
    "redis_job_script_reload_total",
    "redis_job_completion_length_trimmed_total",
    "redis_job_completion_retention_trimmed_total",
    "redis_job_completion_oldest_age_ms",
    "redis_job_pubsub_recovery_window_ms",
];
