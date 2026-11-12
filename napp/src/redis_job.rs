//! RedisJob 受管组件：UserHook 只移交核心计划，Prepare 绑定已托管 Redis source，Ready 才开放领取。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use nadis::job::{
    JobConfig, JobDefinition, JobHandler, JobQuery, JobSourceHealthReason, JobSourceHealthState,
    PreparedJobRuntime, RedisJobPlan, RedisJobSources, RunningJobRuntime,
};

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

macro_rules! job_metric_descriptor {
    ($ident:ident, $name:literal, $kind:ident, $labels:expr) => {
        static $ident: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
            name: $name,
            help: concat!("RedisJob ", $name, " 的受管运行快照。"),
            unit: "",
            kind: nametrics_core::MetricKind::$kind,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}

job_metric_descriptor!(
    JOB_SCHEDULE_LAG,
    "redis_job_schedule_lag_ms",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_DUE_SCAN,
    "redis_job_due_scan_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FIRE,
    "redis_job_fire_total",
    Counter,
    &["qualifier", "result"]
);
job_metric_descriptor!(
    JOB_VISIBLE_SIZE,
    "redis_job_visible_size",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(JOB_RUNNING, "redis_job_running", Gauge, &["qualifier"]);
job_metric_descriptor!(
    JOB_STARTED,
    "redis_job_started_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_SUCCESS,
    "redis_job_success_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FAILED,
    "redis_job_failed_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(JOB_RETRY, "redis_job_retry_total", Counter, &["qualifier"]);
job_metric_descriptor!(JOB_DEAD, "redis_job_dead_total", Counter, &["qualifier"]);
job_metric_descriptor!(
    JOB_SKIPPED,
    "redis_job_skipped_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_LEASE_EXPIRED,
    "redis_job_lease_expired_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_STALE_FINISH,
    "redis_job_stale_finish_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FENCING_REGRESSION,
    "redis_job_fencing_regression_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_SELF_FENCE,
    "redis_job_self_fence_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_SERIAL_WAIT,
    "redis_job_serial_wait_depth",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_EXECUTOR_CAPACITY,
    "redis_job_executor_capacity",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_DEFINITION_CONFLICT,
    "redis_job_definition_conflict_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_CAPABLE_EXECUTOR,
    "redis_job_capable_executor_count",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_INVALID_PAYLOAD,
    "redis_job_invalid_payload_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_SHARD,
    "redis_job_fanout_shard_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_RECEIPT_TIMEOUT,
    "redis_job_fanout_receipt_timeout_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_RECEIPT_RETRY,
    "redis_job_fanout_receipt_retry_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_REASSIGN,
    "redis_job_fanout_reassign_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_STALE_ASSIGNMENT,
    "redis_job_fanout_stale_assignment_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_DELIVERY_PENDING,
    "redis_job_fanout_delivery_pending",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_READY_WAKEUP,
    "redis_job_fanout_ready_wakeup_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_CAPACITY_DEFER,
    "redis_job_fanout_capacity_defer_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_CAPACITY_EXHAUSTED,
    "redis_job_fanout_capacity_exhausted_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_FANOUT_GC_PENDING,
    "redis_job_fanout_gc_pending",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_REGISTRY_GC,
    "redis_job_registry_gc_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_SCRIPT_RELOAD,
    "redis_job_script_reload_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_COMPLETION_LENGTH_TRIMMED,
    "redis_job_completion_length_trimmed_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_COMPLETION_RETENTION_TRIMMED,
    "redis_job_completion_retention_trimmed_total",
    Counter,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_COMPLETION_OLDEST_AGE,
    "redis_job_completion_oldest_age_ms",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_PUBSUB_RECOVERY_WINDOW,
    "redis_job_pubsub_recovery_window_ms",
    Gauge,
    &["qualifier"]
);
job_metric_descriptor!(
    JOB_SUPERVISOR_RESTART,
    "redis_job_supervisor_restart_total",
    Counter,
    &["qualifier", "loop", "result"]
);
job_metric_descriptor!(
    JOB_SUPERVISOR_GENERATION,
    "redis_job_supervisor_generation",
    Gauge,
    &["qualifier", "loop"]
);

static REDIS_JOB_METRIC_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 38] = [
    &JOB_SCHEDULE_LAG,
    &JOB_DUE_SCAN,
    &JOB_FIRE,
    &JOB_VISIBLE_SIZE,
    &JOB_RUNNING,
    &JOB_STARTED,
    &JOB_SUCCESS,
    &JOB_FAILED,
    &JOB_RETRY,
    &JOB_DEAD,
    &JOB_SKIPPED,
    &JOB_LEASE_EXPIRED,
    &JOB_STALE_FINISH,
    &JOB_FENCING_REGRESSION,
    &JOB_SELF_FENCE,
    &JOB_SERIAL_WAIT,
    &JOB_EXECUTOR_CAPACITY,
    &JOB_DEFINITION_CONFLICT,
    &JOB_CAPABLE_EXECUTOR,
    &JOB_INVALID_PAYLOAD,
    &JOB_FANOUT_SHARD,
    &JOB_FANOUT_RECEIPT_TIMEOUT,
    &JOB_FANOUT_RECEIPT_RETRY,
    &JOB_FANOUT_REASSIGN,
    &JOB_FANOUT_STALE_ASSIGNMENT,
    &JOB_FANOUT_DELIVERY_PENDING,
    &JOB_FANOUT_READY_WAKEUP,
    &JOB_FANOUT_CAPACITY_DEFER,
    &JOB_FANOUT_CAPACITY_EXHAUSTED,
    &JOB_FANOUT_GC_PENDING,
    &JOB_REGISTRY_GC,
    &JOB_SCRIPT_RELOAD,
    &JOB_COMPLETION_LENGTH_TRIMMED,
    &JOB_COMPLETION_RETENTION_TRIMMED,
    &JOB_COMPLETION_OLDEST_AGE,
    &JOB_PUBSUB_RECOVERY_WINDOW,
    &JOB_SUPERVISOR_RESTART,
    &JOB_SUPERVISOR_GENERATION,
];

/// 每来源预留普通指标、调度结果、六类监督任务结果与代次的固定样本上界。
const REDIS_JOB_METRIC_SERIES_PER_SOURCE: usize = 35 + 7 + (6 * 5) + 6;
/// 任务指标在统一观测目录内可申请的样本预算。
const REDIS_JOB_METRIC_SERIES_BUDGET: usize = 10_000;

enum PlanState {
    /// 尚未由 Prepare 取走；空槽允许首次登记，有值时拒绝覆盖。
    Open(Option<RedisJobPlan>),
    /// 计划已被取走或确认没有动态声明，后续失败也不重新开放。
    Taken,
}

/// UserHook 与 RedisJob 组件共享的一次性计划槽；计划取走后不能重新开放。
pub(crate) struct RedisJobRuntimeState {
    /// 保护登记与取走唯一性的计划槽位。
    plan: Mutex<PlanState>,
    /// Ready 阶段一次性发布的业务门面，不拥有运行时关闭权。
    handle: OnceLock<nadis::job::JobRuntimeHandle>,
}

impl RedisJobRuntimeState {
    /// 业务作用：创建尚未登记计划的开放状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只允许一次 configure 和一次 take 的状态容器。
    pub(crate) fn new() -> Self {
        Self {
            plan: Mutex::new(PlanState::Open(None)),
            handle: OnceLock::new(),
        }
    }

    /// 业务作用：在 UserHook 把拥有式 Job 计划一次性交给生命周期组件。
    ///
    /// 参数说明：
    /// - `plan`：`plan` 为尚未接触 Redis 的核心计划。
    ///
    /// 返回：首次登记成功；重复登记或 Prepare 已封口时返回阶段错误。
    pub(crate) fn configure(&self, plan: RedisJobPlan) -> ApplicationResult<()> {
        let mut state = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            PlanState::Open(slot @ None) => {
                *slot = Some(plan);
                Ok(())
            }
            PlanState::Open(Some(_)) => Err(job_error(
                ApplicationPhase::UserHook,
                "RedisJob plan can be configured only once",
            )),
            PlanState::Taken => Err(job_error(
                ApplicationPhase::Prepare,
                "RedisJob plan registration is closed",
            )),
        }
    }

    /// 业务作用：在 Prepare 永久关闭登记入口并取得唯一计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在动态计划时转移所有权；纯静态声明返回 `None`；重复取走时拒绝启动。
    fn take(&self) -> ApplicationResult<Option<RedisJobPlan>> {
        let mut state = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 取走与封口在同一锁内完成，Prepare 失败后不能再装配另一份动态计划。
        match std::mem::replace(&mut *state, PlanState::Taken) {
            PlanState::Open(plan) => Ok(plan),
            PlanState::Taken => Err(job_error(
                ApplicationPhase::Prepare,
                "RedisJob plan was already consumed",
            )),
        }
    }

    /// 业务作用：在后台运行时已交给停机栈后一次性发布无停机权的业务门面。
    ///
    /// 参数说明：
    /// - `handle`：`handle` 包含冻结 source 的控制与查询投影。
    ///
    /// 返回：首次发布成功；重复发布表示生命周期装配不一致并拒绝 Ready。
    fn publish_handle(&self, handle: nadis::job::JobRuntimeHandle) -> ApplicationResult<()> {
        self.handle.set(handle).map_err(|_| {
            job_error(
                ApplicationPhase::Ready,
                "RedisJob runtime handle was already published",
            )
        })
    }

    /// 业务作用：读取 Ready 阶段发布的 RedisJob 业务门面，不转移后台任务或停机所有权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发布后取得克隆，尚未发布时为 None；该槽位不检查 Application 当前状态，
    /// 取得句柄不表示整轮 Ready 已完成，也不保证后续调用仍被运行时接纳。
    pub(crate) fn handle(&self) -> Option<nadis::job::JobRuntimeHandle> {
        self.handle.get().cloned()
    }
}

/// 属性入口擦除后的静态 Job 构造器；Application 只在声明确实需要注入时由生成 Handler 持有。
pub type ErasedRedisJobFactory =
    fn(Application) -> ApplicationResult<(JobDefinition, Arc<dyn JobHandler>)>;

/// 业务二进制内由 `#[redis_job]` 生成的静态任务描述。
pub struct RedisJobDescriptor {
    /// 构造静态定义和处理器的函数，可使用应用容器完成注入。
    factory: ErasedRedisJobFactory,
    /// 生成代码提供的声明位置，仅用于冲突定位。
    source: &'static str,
}

impl RedisJobDescriptor {
    /// 业务作用：供过程宏以常量表达式建立静态 RedisJob 描述。
    ///
    /// 参数说明：
    /// - `factory`：`factory` 构造定义和 Handler。
    /// - `source`：`source` 只用于重复声明定位。
    ///
    /// 返回：可放入 linkme 分布式切片的不可变描述。
    #[doc(hidden)]
    pub const fn __new(factory: ErasedRedisJobFactory, source: &'static str) -> Self {
        Self {
            factory,
            source,
        }
    }
}

/// 当前业务 binary 内静态收集的全部 RedisJob 描述。
#[linkme::distributed_slice]
pub static COLLECTED_REDIS_JOBS: [RedisJobDescriptor];

/// 业务作用：构造并合并全部静态任务与可选动态计划，使两条声明路径共用唯一性和冻结门禁。
///
/// 参数说明：
/// - `application`：`application` 为宏 Handler 可选注入的容器。
/// - `dynamic`：`dynamic` 为 UserHook 可选提交的计划。
///
/// 返回：至少存在一项声明时返回统一计划；构造失败、组内重名或空计划时拒绝 Prepare。
fn collect_plan(
    application: &Application,
    dynamic: Option<RedisJobPlan>,
) -> ApplicationResult<RedisJobPlan> {
    let mut plan = RedisJobPlan::new();
    for descriptor in COLLECTED_REDIS_JOBS {
        let (definition, handler) = (descriptor.factory)(application.clone()).map_err(|error| {
            job_error_source(
                ApplicationPhase::Prepare,
                format!(
                    "RedisJob static descriptor rejected at {}",
                    descriptor.source
                ),
                error,
            )
        })?;
        plan = plan
            .register_handler(definition, handler)
            .map_err(|error| {
                job_error_source(
                    ApplicationPhase::Prepare,
                    format!(
                        "RedisJob static descriptor conflicts at {}",
                        descriptor.source
                    ),
                    error,
                )
            })?;
    }
    if let Some(dynamic) = dynamic {
        plan = plan.merge(dynamic).map_err(|error| {
            job_error_source(
                ApplicationPhase::Prepare,
                "RedisJob static and dynamic plans conflict",
                error,
            )
        })?;
    }
    if plan
        .source_ids()
        .map_err(|error| {
            job_error_source(
                ApplicationPhase::Prepare,
                "invalid RedisJob source set",
                error,
            )
        })?
        .is_empty()
    {
        return Err(job_error(
            ApplicationPhase::Prepare,
            "component `redis-job` requires at least one static or dynamic definition",
        ));
    }
    Ok(plan)
}

/// 受管 RedisJob 生命周期；Prepare 完成外部门禁，Ready 才启动后台任务。
pub(crate) struct RedisJobComponent {
    /// 通过来源绑定、布局和能力校验后待启动的运行时。
    prepared: Option<PreparedJobRuntime>,
    /// 所有任务来源共同影响的关键就绪贡献。
    contributor: Option<ReadinessContributor>,
    /// Start 时冻结名称的逐来源关键贡献，未被任务使用的来源初始为 Ready。
    source_contributors: BTreeMap<String, ReadinessContributor>,
    /// 指标出口共享的本地查询目录，核心启动完成后发布。
    metric_queries: Arc<Mutex<Vec<JobQuery>>>,
    /// 待交给 Runner 的聚合健康循环，取出后组件不再保留 future。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl RedisJobComponent {
    /// 业务作用：创建尚未消费计划、尚未开放执行权的组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：等待 Start/Prepare/Ready 三阶段推进的组件。
    pub(crate) fn new() -> Self {
        Self {
            prepared: None,
            contributor: None,
            source_contributors: BTreeMap::new(),
            metric_queries: Arc::new(Mutex::new(Vec::new())),
            critical_task: None,
        }
    }
}

impl ApplicationComponent for RedisJobComponent {
    /// 业务作用：返回独立 RedisJob 组件身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：ComponentId::RedisJob，用于生命周期错误归类和资源所有权登记，与底层 Redis 来源身份分开。
    fn id(&self) -> ComponentId {
        ComponentId::RedisJob
    }

    /// 业务作用：要求 Redis 来源先于任务运行时完成生命周期装配，使任务停机责任先于来源释放。
    ///
    /// 参数说明：无。
    ///
    /// 返回：只含 Redis 组件的固定依赖目录；不在此选择具体来源或探测远端能力。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[ComponentId::Redis]
    }

    /// 业务作用：预登记 RedisJob readiness 名称，运行期不动态增长观测基数。
    ///
    /// 参数说明：
    /// - `context`：`context` 提供共享 Application。
    ///
    /// 返回：贡献项登记成功时完成。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application();
            self.contributor = Some(application.register_readiness(
                ComponentId::RedisJob,
                "redis-job:runtime",
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?);
            for (qualifier, _) in crate::redis::read_redis_configs(application)? {
                let contributor = application.register_readiness(
                    ComponentId::RedisJob,
                    Arc::<str>::from(format!("redis-job:source:{qualifier}")),
                    ReadinessPolicy {
                        affects_ready: true,
                        failure_threshold: 1,
                        recovery_threshold: 1,
                        stale_after: None,
                    },
                )?;
                // 源名集合必须在 readiness 封口前固定；未被 Job plan 引用的托管 Redis 源不应阻断应用就绪。
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                self.source_contributors.insert(qualifier, contributor);
            }
            Ok(())
        })
    }

    /// 业务作用：消费冻结计划，精确绑定其引用的受管 Redis source，并完成能力、布局和定义门禁。
    ///
    /// 参数说明：
    /// - `context`：`context` 提供最终配置、Redis 资源和共享启动预算。
    ///
    /// 返回：全部 source 准备成功后保存 Prepared 类型；任一失败时不开放后台领取。
    fn prepare<'a>(&'a mut self, context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let dynamic = application.redis_job_runtime_state().take()?;
            let plan = collect_plan(&application, dynamic)?;
            let source_ids = plan.source_ids().map_err(|error| {
                job_error_source(
                    ApplicationPhase::Prepare,
                    "invalid RedisJob source set",
                    error,
                )
            })?;
            let worst_case_series = redis_job_worst_case_metric_series(source_ids.len())?;
            application
                .metrics_hub()
                .register_legacy_source_reserved(
                    Arc::new(RedisJobMetricsSource {
                        queries: self.metric_queries.clone(),
                    }),
                    worst_case_series,
                )
                .map_err(|error| match error {
                    nametrics_core::MetricSourceRegistrationError::Conflict(conflict) => job_error(
                        ApplicationPhase::Prepare,
                        format!(
                            "RedisJob metric descriptor `{}` conflicts with an existing registration",
                            conflict.name
                        ),
                    ),
                    nametrics_core::MetricSourceRegistrationError::SeriesBudgetExceeded => job_error(
                        ApplicationPhase::Prepare,
                        "RedisJob metric series reservation exceeds the process budget",
                    ),
                })?;
            let mut sources = RedisJobSources::builder();
            for source_id in source_ids {
                let client = application.redis(source_id.as_str()).await?;
                sources = sources
                    .register(source_id.as_str(), client)
                    .map_err(|error| {
                        job_error_source(
                            ApplicationPhase::Prepare,
                            "RedisJob source binding rejected",
                            error,
                        )
                    })?;
            }
            let sources = sources.build().map_err(|error| {
                job_error_source(
                    ApplicationPhase::Prepare,
                    "RedisJob source table is empty",
                    error,
                )
            })?;
            let mut config = read_job_config(&application)?;
            config.application_name = application.info().name().to_owned();
            if config.instance_identity.trim().is_empty() {
                return Err(job_error(
                    ApplicationPhase::Prepare,
                    "redis.job.instance_identity must be configured with a restart-stable value",
                ));
            }
            let prepared = plan
                .prepare_sources(sources, config)
                .await
                .map_err(|error| {
                    job_error_source(
                        ApplicationPhase::Prepare,
                        "RedisJob prepare rejected",
                        error,
                    )
                })?;
            self.prepared = Some(prepared);
            Ok(())
        })
    }

    /// 业务作用：在全部 initializer 成功后启动全部 source，并立即建立反向停机所有权。
    ///
    /// 参数说明：
    /// - `context`：`context` 提供 active stack 与共享 Application。
    ///
    /// 返回：核心启动成功后将运行时交给停机 owner，依次发布指标查询、业务门面、监督任务和初始就绪观测。
    /// 任一步失败都会拒绝本组件 Ready；已登记的停机动作仍承担启动任务或运行时的收口责任。
    /// 这些发布不是一个原子操作，组件成功也不代表 Application 的整体接流阶段已经完成。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let prepared = self.prepared.take().ok_or_else(|| {
                job_error(
                    ApplicationPhase::Ready,
                    "RedisJob prepared runtime is missing",
                )
            })?;
            let contributor = self.contributor.clone().ok_or_else(|| {
                job_error(
                    ApplicationPhase::Ready,
                    "RedisJob readiness contributor is missing",
                )
            })?;
            let lifecycle = Arc::new(tokio::sync::Mutex::new(RedisJobLifecycle::Starting(
                tokio::spawn(prepared.start()),
            )));
            // 启动任务一经调度就可能登记部分 source；必须先把接管句柄压入反向停机栈，任何 Ready 取消才能继续排空和注销。
            context.activate(Box::new(RedisJobShutdown {
                lifecycle: lifecycle.clone(),
            }));
            let mut state = lifecycle.lock().await;
            let started = match &mut *state {
                RedisJobLifecycle::Starting(handle) => handle.await,
                _ => {
                    return Err(job_error(
                        ApplicationPhase::Ready,
                        "RedisJob startup ownership changed unexpectedly",
                    ));
                }
            };
            let running = match started {
                Ok(Ok(running)) => running,
                Ok(Err(error)) => {
                    *state = RedisJobLifecycle::Empty;
                    return Err(job_error_source(
                        ApplicationPhase::Ready,
                        "RedisJob start rejected",
                        error,
                    ));
                }
                Err(error) => {
                    // 启动任务已经终止，不能把该槽位当成仍持有可接管的 Running 资源。
                    *state = RedisJobLifecycle::Empty;
                    return Err(job_error_source(
                        ApplicationPhase::Ready,
                        "RedisJob startup task terminated",
                        error,
                    ));
                }
            };
            *state = RedisJobLifecycle::Running(running);
            let (runtime_handle, health_queries) = match &*state {
                RedisJobLifecycle::Running(running) => (running.handle(), running.health_queries()),
                _ => unreachable!("RedisJob 启动成功后状态恒为 Running"),
            };
            drop(state);
            *self
                .metric_queries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = health_queries.clone();
            let application = context.application().clone();
            application
                .redis_job_runtime_state()
                .publish_handle(runtime_handle)?;
            self.critical_task = Some(Box::pin(run_job_health_monitor(
                application,
                health_queries,
                contributor,
                self.source_contributors.clone(),
            )));
            if let Some(contributor) = &self.contributor {
                contributor.observe(
                    DependencyState::Ready,
                    reason::HEALTHY,
                    std::time::Instant::now(),
                );
            }
            Ok(())
        })
    }

    /// 业务作用：把聚合 source 健康监视交给 Runner 作为唯一 RedisJob 关键 future。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 成功后首次取得监视任务及稳定名称；未启动或重复取得返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("redis-job-health-monitor", task))
    }
}

/// 业务作用：持续聚合逐 source 健康；预算内恢复保持 Ready，任一 source NotReady 时撤销聚合 Ready。
///
/// 参数说明：
/// - `application`: 提供全局生命周期状态。
/// - `queries`: 每个 source 的独立健康查询门面。
/// - `contributor`: RedisJob 的关键 readiness 贡献项。
///
/// - `source_contributors`: 已在 Start 封口前登记的逐 source readiness 句柄。
///
/// 返回：正常停机时成功退出；预算内降级只改变 readiness，任一 source 进入不可恢复 NotReady 时返回运行错误并交 Runner 收口。
async fn run_job_health_monitor(
    application: Application,
    queries: Vec<JobQuery>,
    contributor: ReadinessContributor,
    source_contributors: BTreeMap<String, ReadinessContributor>,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                for source in source_contributors.values() {
                    source.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                }
                return Ok(());
            }
            ApplicationState::Starting => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            ApplicationState::Ready => {}
        }
        let mut all_ready = true;
        let mut terminal_source = None;
        for query in &queries {
            let snapshot = query.health();
            let (state, source_reason) = match snapshot.state {
                JobSourceHealthState::Up => (DependencyState::Ready, reason::HEALTHY),
                JobSourceHealthState::Degraded => (
                    DependencyState::Degraded,
                    snapshot
                        .reason
                        .map_or(reason::DEGRADED, |value| value.as_str()),
                ),
                JobSourceHealthState::NotReady => {
                    terminal_source.get_or_insert_with(|| {
                        (
                            snapshot.qualifier.clone(),
                            snapshot
                                .reason
                                .unwrap_or(JobSourceHealthReason::CriticalRuntime),
                        )
                    });
                    (
                        DependencyState::NotReady,
                        snapshot
                            .reason
                            .map_or(reason::NOT_READY, |value| value.as_str()),
                    )
                }
                JobSourceHealthState::Draining => (
                    DependencyState::NotReady,
                    snapshot
                        .reason
                        .map_or(reason::NOT_READY, |value| value.as_str()),
                ),
            };
            if matches!(state, DependencyState::NotReady) {
                all_ready = false;
            }
            if let Some(source) = source_contributors.get(&snapshot.qualifier) {
                source.observe(state, source_reason, Instant::now());
            }
        }
        contributor.observe(
            if all_ready {
                DependencyState::Ready
            } else {
                DependencyState::NotReady
            },
            if all_ready {
                reason::HEALTHY
            } else {
                reason::NOT_READY
            },
            Instant::now(),
        );
        if let Some((qualifier, terminal_reason)) = terminal_source {
            return Err(job_error(
                ApplicationPhase::Running,
                format!(
                    "RedisJob source `{qualifier}` entered terminal NotReady: {}",
                    terminal_reason.as_str()
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// 业务作用：按冻结 source 数量计算 RedisJob 指标的最坏公开 series 数。
///
/// 参数说明：
/// - `source_count`：`source_count` 来自已通过计划唯一性校验的 canonical qualifier 集合。
///
/// 返回：乘法无溢出且未超过 RedisJob 子预算时返回精确上界；否则拒绝 Prepare。
fn redis_job_worst_case_metric_series(source_count: usize) -> ApplicationResult<usize> {
    let total = source_count
        .checked_mul(REDIS_JOB_METRIC_SERIES_PER_SOURCE)
        .ok_or_else(|| {
            job_error(
                ApplicationPhase::Prepare,
                "RedisJob metric series calculation overflowed",
            )
        })?;
    if total > REDIS_JOB_METRIC_SERIES_BUDGET {
        return Err(job_error(
            ApplicationPhase::Prepare,
            "RedisJob metric series budget is exceeded",
        ));
    }
    Ok(total)
}

/// 受管 RedisJob 指标桥；查询句柄只在 Ready 开放运行时一次发布。
struct RedisJobMetricsSource {
    /// 逐来源本地指标查询句柄，不在采集时发 Redis 命令。
    queries: Arc<Mutex<Vec<JobQuery>>>,
}

impl nametrics_core::LegacyMetricsSource for RedisJobMetricsSource {
    /// 业务作用：返回 RedisJob 固定 family 目录，供启动期冲突审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只含冻结 qualifier、监督 loop 和结果枚举维度的 descriptor。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &REDIS_JOB_METRIC_DESCRIPTORS
    }

    /// 业务作用：将所有 source 的无 Redis I/O 快照投影为统一结构化样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 前返回空样本；Ready 后返回按 qualifier 隔离的 counter、gauge 与监督代次。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let queries = self
            .queries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut samples = Vec::new();
        for query in queries {
            let snapshot = query.metrics();
            for (name, value) in snapshot.values {
                if name == JOB_FIRE.name {
                    continue;
                }
                let Some(descriptor) = REDIS_JOB_METRIC_DESCRIPTORS
                    .iter()
                    .copied()
                    .find(|descriptor| descriptor.name == name)
                else {
                    continue;
                };
                let labels = vec![("qualifier", snapshot.qualifier.clone())];
                samples.push(job_metric_sample(descriptor, labels, value));
            }
            for (result, total) in snapshot.fire_results {
                samples.push(nametrics_core::MetricSample {
                    name: JOB_FIRE.name,
                    labels: vec![
                        ("qualifier", snapshot.qualifier.clone()),
                        ("result", result.as_str().to_owned()),
                    ],
                    value: nametrics_core::MetricValue::Counter(total),
                });
            }
            for supervisor in snapshot.supervisors {
                let loop_name = supervisor.loop_name.as_str().to_owned();
                samples.push(nametrics_core::MetricSample {
                    name: JOB_SUPERVISOR_GENERATION.name,
                    labels: vec![
                        ("qualifier", snapshot.qualifier.clone()),
                        ("loop", loop_name.clone()),
                    ],
                    value: nametrics_core::MetricValue::Gauge(supervisor.generation as f64),
                });
                for (result, total) in supervisor.result_totals {
                    samples.push(nametrics_core::MetricSample {
                        name: JOB_SUPERVISOR_RESTART.name,
                        labels: vec![
                            ("qualifier", snapshot.qualifier.clone()),
                            ("loop", loop_name.clone()),
                            ("result", result.as_str().to_owned()),
                        ],
                        value: nametrics_core::MetricValue::Counter(total),
                    });
                }
            }
        }
        Some(samples)
    }

    /// 业务作用：保留兼容 trait 入口，文本与非文本出口统一使用结构化快照。
    ///
    /// 参数说明：
    /// - `_output`：`_output` 是未由本源直接写入的兼容缓冲区。
    ///
    /// 返回：无；统一 hub 负责渲染 `snapshot()`。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：按 descriptor 类型把有符号本地值转换为合法结构化样本。
///
/// 参数说明：
/// - `descriptor`：`descriptor` 决定 family 与值类型。
/// - `labels`：`labels` 来自冻结低基数域。
/// - `value`：`value` 来自单 source 快照。
///
/// 返回：counter 对防御性负值按零处理，gauge 保留有符号值。
fn job_metric_sample(
    descriptor: &'static nametrics_core::MetricDescriptor,
    labels: Vec<(&'static str, String)>,
    value: i64,
) -> nametrics_core::MetricSample {
    let value = match descriptor.kind {
        nametrics_core::MetricKind::Counter => {
            nametrics_core::MetricValue::Counter(u64::try_from(value.max(0)).unwrap_or(u64::MAX))
        }
        nametrics_core::MetricKind::Gauge => nametrics_core::MetricValue::Gauge(value as f64),
        nametrics_core::MetricKind::Histogram => {
            unreachable!("RedisJob 当前不声明 histogram descriptor")
        }
    };
    nametrics_core::MetricSample {
        name: descriptor.name,
        labels,
        value,
    }
}

/// RedisJob Ready 启动与运行所有权的共享状态；停机动作在启动完成前即持有它。
enum RedisJobLifecycle {
    /// 已调度的启动任务，取消 Ready 等待后仍由停机 owner 接管。
    Starting(tokio::task::JoinHandle<nadis::Result<RunningJobRuntime>>),
    /// 成功启动且尚未移交核心关闭的运行时。
    Running(RunningJobRuntime),
    /// 尚无可接管运行时或所有权已取走；不单独证明外部资源清理完成。
    Empty,
}

/// 业务作用：持有 RedisJob 启动任务或运行时的唯一停机权，保证两种阶段都能完成受管收口。
struct RedisJobShutdown {
    /// 在 Ready 与停机之间串行转移启动任务或运行时所有权的状态。
    lifecycle: Arc<tokio::sync::Mutex<RedisJobLifecycle>>,
}

impl ShutdownAction for RedisJobShutdown {
    /// 业务作用：返回稳定的 Job 停机动作名。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定名称 redis-job-runtime，归类聚合停机结果，不附带来源名或任务业务内容。
    fn label(&self) -> &'static str {
        "redis-job-runtime"
    }

    /// 业务作用：在宿主剩余预算内接管未完成的启动任务或运行时，并委托核心关闭全部来源。
    ///
    /// 参数说明：
    /// - `context`：提供共享停机剩余预算，锁等待、启动等待与运行时关闭共用该子预算。
    ///
    /// 返回：没有剩余运行时或核心完成关闭时成功；取锁超时、启动失败、任务异常或关闭失败返回组件错误。
    /// 启动等待超时会请求 abort 并清空槽位，但不在此 join 已中止任务；错误不能解释为外部登记已经清理。
    /// 停机准入关闭由核心在取得 Running 状态后执行，构造 future 本身不关闭业务入口。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let budget = context.child_budget(Duration::MAX);
            let deadline = tokio::time::Instant::now() + budget;
            let mut state = tokio::time::timeout_at(deadline, self.lifecycle.lock())
                .await
                .map_err(|_| {
                    job_error(
                        ApplicationPhase::Stopping,
                        "RedisJob startup ownership did not become available before shutdown deadline",
                    )
                })?;
            if matches!(*state, RedisJobLifecycle::Starting(_)) {
                let started = match &mut *state {
                    RedisJobLifecycle::Starting(handle) => {
                        tokio::time::timeout_at(deadline, handle).await
                    }
                    _ => unreachable!("RedisJob 状态已确认处于 Starting"),
                };
                match started {
                    Ok(Ok(Ok(running))) => *state = RedisJobLifecycle::Running(running),
                    Ok(Ok(Err(error))) => {
                        *state = RedisJobLifecycle::Empty;
                        return Err(job_error_source(
                            ApplicationPhase::Stopping,
                            "RedisJob startup failed while shutdown was taking ownership",
                            error,
                        ));
                    }
                    Ok(Err(error)) => {
                        *state = RedisJobLifecycle::Empty;
                        return Err(job_error_source(
                            ApplicationPhase::Stopping,
                            "RedisJob startup task terminated while shutdown was taking ownership",
                            error,
                        ));
                    }
                    Err(_) => {
                        if let RedisJobLifecycle::Starting(handle) = &mut *state {
                            // 截止后必须终止仍在建立副作用的启动任务，避免清理返回后继续登记执行器。
                            handle.abort();
                        }
                        *state = RedisJobLifecycle::Empty;
                        return Err(job_error(
                            ApplicationPhase::Stopping,
                            "RedisJob startup did not finish before shutdown deadline",
                        ));
                    }
                }
            }
            let running = match std::mem::replace(&mut *state, RedisJobLifecycle::Empty) {
                RedisJobLifecycle::Running(running) => Some(running),
                RedisJobLifecycle::Empty => None,
                RedisJobLifecycle::Starting(_) => {
                    unreachable!("Starting 已在上文收敛")
                }
            };
            drop(state);
            if let Some(running) = running {
                // 启动接管与运行时排空共用同一子截止；子预算之外的全局余量留给后续逆序清理。
                running
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await
                    .map_err(|error| {
                        job_error_source(
                            ApplicationPhase::Stopping,
                            "RedisJob shutdown incomplete",
                            error,
                        )
                    })?;
            }
            Ok(())
        })
    }
}

/// 业务作用：从最终 `redis.job` 配置段读取根级默认与逐 source 覆盖。
///
/// 参数说明：
/// - `application`：`application` 提供不可变最终配置快照。
///
/// 返回：反序列化后的 JobConfig；配置段缺失或非法时拒绝 Prepare。
fn read_job_config(application: &Application) -> ApplicationResult<JobConfig> {
    let value = application
        .config()
        .value()
        .get("redis")
        .and_then(|redis| redis.get("job"))
        .cloned()
        .ok_or_else(|| {
            job_error(
                ApplicationPhase::Prepare,
                "component `redis-job` requires the `redis.job` configuration section",
            )
        })?;
    serde_json::from_value(value).map_err(|error| {
        job_error_source(
            ApplicationPhase::Prepare,
            "invalid `redis.job` configuration section",
            error,
        )
    })
}

/// 业务作用：在建立 Redis 连接和执行 UserHook 前校验 `redis.job` 根配置及全部逐 source 覆盖。
///
/// 参数说明：
/// - `tree`：`tree` 为尚未发布副作用的最终候选配置树。
/// - `phase`：`phase` 为当前校验阶段。
///
/// 返回：根配置、稳定实例身份和每个稀疏覆盖均合法时成功；缺段、未知字段、禁用之外的类型错误或安全门禁失败时拒绝整帧。
pub(crate) fn validate_redis_job_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let value = tree
        .get("redis")
        .and_then(|redis| redis.get("job"))
        .cloned()
        .ok_or_else(|| {
            job_error(
                phase,
                "component `redis-job` requires the `redis.job` configuration section",
            )
        })?;
    let config: JobConfig = serde_json::from_value(value).map_err(|error| {
        job_error_source(phase, "invalid `redis.job` configuration section", error)
    })?;
    config
        .validate()
        .map_err(|error| job_error_source(phase, "invalid `redis.job` configuration", error))?;
    if config.instance_identity.trim().is_empty() {
        return Err(job_error(
            phase,
            "redis.job.instance_identity must be configured with a restart-stable value",
        ));
    }
    for qualifier in config.sources.keys() {
        let mut validation = config.clone();
        if let Some(source) = validation.sources.get_mut(qualifier) {
            // `enabled=false` 是使用期门禁，不应阻止对该覆盖块其它字段做无副作用语法校验。
            source.enabled = Some(true);
        }
        validation
            .resolve_source(qualifier, "managed-validation")
            .map_err(|error| {
                job_error_source(
                    phase,
                    format!("invalid `redis.job.sources.{qualifier}` configuration"),
                    error,
                )
            })?;
    }
    Ok(())
}

/// 业务作用：把任务装配或运行失败归属于 RedisJob 组件及对应生命周期阶段。
///
/// 参数说明：
/// - `phase`：观察到失败的生命周期阶段。
/// - `message`：调用方准备的诊断摘要，须避免凭据和业务载荷。
///
/// 返回：保留传入摘要的组件错误；不在此过滤文本、执行重试或改变任务状态。
fn job_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::RedisJob, phase, message)
}

/// 业务作用：构造保留底层错误链的 RedisJob 生命周期错误，统一归因到独立组件。
///
/// 参数说明：
/// - `phase`：`phase` 为失败阶段。
/// - `message`：`message` 为稳定诊断信息。
/// - `source`：`source` 为底层错误链。
///
/// 返回：归因到 RedisJob 组件且原样保留底层原因的生命周期错误；此处不自动脱敏，
/// 调用方及最终输出通道分别负责诊断材料边界与输出处理。
fn job_error_source(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::RedisJob, phase, message, source)
}
