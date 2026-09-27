//! Inbox 去重标记的受管保留清理。
//!
//! 清理是显式 opt-in：业务必须声明消息源最大重投视界、去重标记最小年龄、轮次预算和执行间隔。
//! Application 在 Ready 后为每个消费命名空间运行一个串行 fixed-delay 循环；停机时不再开启新轮次，
//! 已进入数据库的轮次依靠策略预算完成解锁与连接处置后退出，先于受管数据库资源释放。

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use nainbox_core::{DurableInboxRetention, InboxRetentionPolicy, InboxRetentionRoundReport};

use crate::{Application, ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

const MIN_INTERVAL_MS: u64 = 100;
const MAX_INTERVAL_MS: u64 = 86_400_000;
const INBOX_RETENTION_METRIC_SERIES: usize = 6;

macro_rules! inbox_metric {
    ($ident:ident, $name:literal, $help:literal, $kind:expr) => {
        static $ident: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: $kind,
            label_names: &[],
            histogram_bounds: &[],
        };
    };
}

inbox_metric!(
    INBOX_RETENTION_ROUNDS,
    "napp_inbox_retention_rounds_total",
    "Inbox retention rounds completed by the managed carrier.",
    nametrics_core::MetricKind::Counter
);
inbox_metric!(
    INBOX_RETENTION_DELETED,
    "napp_inbox_retention_deleted_total",
    "Expired Inbox markers deleted after the redelivery horizon.",
    nametrics_core::MetricKind::Counter
);
inbox_metric!(
    INBOX_RETENTION_CLAIM_CONTENDED,
    "napp_inbox_retention_claim_contended_total",
    "Inbox retention rounds skipped because another owner held the claim.",
    nametrics_core::MetricKind::Counter
);
inbox_metric!(
    INBOX_RETENTION_BUDGET_EXHAUSTED,
    "napp_inbox_retention_budget_exhausted_total",
    "Inbox retention rounds stopped at their declared time budget.",
    nametrics_core::MetricKind::Counter
);
inbox_metric!(
    INBOX_RETENTION_FAILED,
    "napp_inbox_retention_failed_rounds_total",
    "Inbox retention rounds that returned a storage error.",
    nametrics_core::MetricKind::Counter
);
inbox_metric!(
    INBOX_RETENTION_OLDEST_AGE,
    "napp_inbox_retention_oldest_candidate_age_ms",
    "Oldest remaining Inbox retention candidate age across configured consumers.",
    nametrics_core::MetricKind::Gauge
);

static INBOX_RETENTION_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 6] = [
    &INBOX_RETENTION_ROUNDS,
    &INBOX_RETENTION_DELETED,
    &INBOX_RETENTION_CLAIM_CONTENDED,
    &INBOX_RETENTION_BUDGET_EXHAUSTED,
    &INBOX_RETENTION_FAILED,
    &INBOX_RETENTION_OLDEST_AGE,
];

/// 业务作用：冻结一个消费命名空间的去重标记保留边界与串行巡检节奏。
///
/// 每个计划都必须显式声明重投视界；Application 不会根据 Kafka、数据库或其它消息源配置推断
/// 删除窗口。相同 datasource 内的 `consumer_name` 在一个 Application 中只能登记一次；不同
/// datasource 的同名消费空间彼此独立。
pub struct InboxRetentionPlan {
    datasource_ref: String,
    consumer_name: String,
    policy: InboxRetentionPolicy,
    interval: Duration,
}

impl InboxRetentionPlan {
    /// 业务作用：校验并冻结一个受管 Inbox 保留计划，阻止不安全窗口进入后台清理循环。
    ///
    /// 参数说明：
    /// - `datasource_ref`：Inbox 表所属的受管 datasource 名称。
    /// - `consumer_name`：与消息处理路径使用的稳定消费命名空间完全一致。
    /// - `policy`：含最大重投视界、最小标记年龄、批量上限与单轮预算的安全策略。
    /// - `interval`：上一轮结束到下一轮开始之间的 fixed-delay 间隔。
    ///
    /// 返回：全部名称和预算合法时返回冻结计划；删除窗口、名称或间隔越界时返回 UserHook 错误。
    pub fn new(
        datasource_ref: impl Into<String>,
        consumer_name: impl Into<String>,
        policy: InboxRetentionPolicy,
        interval: Duration,
    ) -> ApplicationResult<Self> {
        let datasource_ref = datasource_ref.into();
        natx_core::DatasourceRef::new(&datasource_ref).map_err(|error| {
            inbox_source_error(
                ApplicationPhase::UserHook,
                "inbox retention datasource_ref is invalid",
                error,
            )
        })?;
        let consumer_name = consumer_name.into();
        if consumer_name.is_empty()
            || consumer_name.len() > 128
            || consumer_name.chars().any(char::is_control)
        {
            return Err(inbox_error(
                ApplicationPhase::UserHook,
                "inbox retention consumer_name must contain 1..=128 non-control bytes",
            ));
        }
        policy.validate().map_err(|reason| {
            inbox_error(
                ApplicationPhase::UserHook,
                format!("inbox retention policy is invalid: {reason}"),
            )
        })?;
        let interval_ms = u64::try_from(interval.as_millis()).map_err(|_| {
            inbox_error(
                ApplicationPhase::UserHook,
                "inbox retention interval is out of range",
            )
        })?;
        if !(MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&interval_ms) {
            return Err(inbox_error(
                ApplicationPhase::UserHook,
                "inbox retention interval_ms must be within 100..=86400000",
            ));
        }
        Ok(Self {
            datasource_ref,
            consumer_name,
            policy,
            interval,
        })
    }
}

/// 业务作用：提供所有已配置 Inbox 保留循环的低基数聚合快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboxRetentionSnapshot {
    /// 已完成并形成报告的轮次总数。
    pub rounds: u64,
    /// 已删除的过期去重标记总数。
    pub deleted: u64,
    /// 因其它 owner 持有清理权而跳过的轮次总数。
    pub claim_contended: u64,
    /// 在声明预算处结束的轮次总数。
    pub budget_exhausted: u64,
    /// 数据库返回失败的轮次总数。
    pub failed_rounds: u64,
    /// 各消费命名空间最近成功轮次中最老候选年龄的最大值；均无候选时为零。
    pub oldest_candidate_age_ms: u64,
}

/// 业务作用：唯一标识一个 datasource 内的消费命名空间，避免多源同名计划互相排斥。
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct InboxRetentionKey {
    datasource_ref: String,
    consumer_name: String,
}

impl InboxRetentionKey {
    /// 业务作用：从已经校验的 datasource 与消费命名空间构造内部计划身份。
    ///
    /// 参数说明：
    /// - `datasource_ref`：受管 datasource 名称。
    /// - `consumer_name`：该 datasource 内的消费命名空间。
    ///
    /// 返回：同时拥有两个身份字段的内部键，不再执行重复校验。
    fn new(datasource_ref: String, consumer_name: String) -> Self {
        Self {
            datasource_ref,
            consumer_name,
        }
    }
}

/// 业务作用：累计所有受管消费命名空间的清理账目，并保留每个计划最近候选年龄供聚合。
struct InboxRetentionMetrics {
    rounds: AtomicU64,
    deleted: AtomicU64,
    claim_contended: AtomicU64,
    budget_exhausted: AtomicU64,
    failed_rounds: AtomicU64,
    oldest_by_plan: Mutex<BTreeMap<InboxRetentionKey, u64>>,
}

impl InboxRetentionMetrics {
    /// 业务作用：创建尚未执行任何清理轮次的聚合指标状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：所有计数为零且没有消费命名空间快照的状态。
    fn new() -> Self {
        Self {
            rounds: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            claim_contended: AtomicU64::new(0),
            budget_exhausted: AtomicU64::new(0),
            failed_rounds: AtomicU64::new(0),
            oldest_by_plan: Mutex::new(BTreeMap::new()),
        }
    }

    /// 业务作用：把一轮已完成清理的全部账目原子地并入进程累计指标。
    ///
    /// 参数说明：
    /// - `plan_key`：本轮对应的 datasource 与消费命名空间，仅用于更新内部聚合槽。
    /// - `report`：数据库适配器完成 owner 释放后返回的轮次报告。
    ///
    /// 返回：无；累计计数单调增加，候选年龄更新为该命名空间最近一次观测。
    fn observe(&self, plan_key: &InboxRetentionKey, report: InboxRetentionRoundReport) {
        self.rounds.fetch_add(1, Ordering::Relaxed);
        self.deleted.fetch_add(report.deleted, Ordering::Relaxed);
        if report.claim_contended {
            self.claim_contended.fetch_add(1, Ordering::Relaxed);
        }
        if report.budget_exhausted {
            self.budget_exhausted.fetch_add(1, Ordering::Relaxed);
        }
        let age = report
            .oldest_candidate_age_ms
            .and_then(|age| u64::try_from(age).ok())
            .unwrap_or(0);
        self.oldest_by_plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(plan_key.clone(), age);
    }

    /// 业务作用：记录未形成轮次报告的存储失败，供运维区分空轮与异常轮次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；失败累计值单调增加。
    fn observe_failure(&self) {
        self.failed_rounds.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：读取不包含 datasource、消费身份或消息身份的聚合观测快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前累计计数以及所有已配置命名空间中最大的最近候选年龄。
    fn snapshot(&self) -> InboxRetentionSnapshot {
        let oldest_candidate_age_ms = self
            .oldest_by_plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        InboxRetentionSnapshot {
            rounds: self.rounds.load(Ordering::Relaxed),
            deleted: self.deleted.load(Ordering::Relaxed),
            claim_contended: self.claim_contended.load(Ordering::Relaxed),
            budget_exhausted: self.budget_exhausted.load(Ordering::Relaxed),
            failed_rounds: self.failed_rounds.load(Ordering::Relaxed),
            oldest_candidate_age_ms,
        }
    }
}

/// 业务作用：把 Inbox retention 聚合状态桥接到 Application 唯一指标目录。
struct InboxRetentionMetricsSource {
    metrics: Arc<InboxRetentionMetrics>,
}

impl nametrics_core::LegacyMetricsSource for InboxRetentionMetricsSource {
    /// 业务作用：返回 Inbox retention 的固定无标签指标目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：六个低基数 descriptor，数量不随 datasource 或 consumer 数量增长。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &INBOX_RETENTION_DESCRIPTORS
    }

    /// 业务作用：把当前保留清理账目转换为统一指标 hub 的结构化样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终包含六个无标签样本，不泄露消费身份或数据库身份。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let snapshot = self.metrics.snapshot();
        Some(vec![
            counter(INBOX_RETENTION_ROUNDS.name, snapshot.rounds),
            counter(INBOX_RETENTION_DELETED.name, snapshot.deleted),
            counter(
                INBOX_RETENTION_CLAIM_CONTENDED.name,
                snapshot.claim_contended,
            ),
            counter(
                INBOX_RETENTION_BUDGET_EXHAUSTED.name,
                snapshot.budget_exhausted,
            ),
            counter(INBOX_RETENTION_FAILED.name, snapshot.failed_rounds),
            gauge(
                INBOX_RETENTION_OLDEST_AGE.name,
                snapshot.oldest_candidate_age_ms,
            ),
        ])
    }

    /// 业务作用：保留兼容文本入口；统一 hub 使用结构化快照生成 Prometheus 文本。
    ///
    /// 参数说明：`_output` 是兼容 trait 的文本缓冲区，本源不直接写入。
    ///
    /// 返回：无。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：保存 Application 内全部 Inbox retention 计划的唯一性与聚合指标所有权。
pub(crate) struct InboxRetentionRuntimeState {
    configured_plans: Mutex<BTreeSet<InboxRetentionKey>>,
    metrics_registered: Mutex<bool>,
    metrics: Arc<InboxRetentionMetrics>,
}

impl InboxRetentionRuntimeState {
    /// 业务作用：创建尚未登记消费命名空间和指标源的运行时所有权根。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由一个 Application 独占的空运行时状态。
    pub(crate) fn new() -> Self {
        Self {
            configured_plans: Mutex::new(BTreeSet::new()),
            metrics_registered: Mutex::new(false),
            metrics: Arc::new(InboxRetentionMetrics::new()),
        }
    }

    /// 业务作用：线性化登记 datasource 内的消费命名空间，禁止同一去重集合重复启动清理循环。
    ///
    /// 参数说明：`plan_key` 是计划中已经校验的 datasource 与消费命名空间。
    ///
    /// 返回：同一 datasource 内首次登记成功；重复登记返回 UserHook 错误。
    fn register_plan(&self, plan_key: &InboxRetentionKey) -> ApplicationResult<()> {
        let mut plans = self
            .configured_plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !plans.insert(plan_key.clone()) {
            return Err(inbox_error(
                ApplicationPhase::UserHook,
                "inbox retention datasource and consumer_name can be configured only once",
            ));
        }
        Ok(())
    }

    /// 业务作用：任务登记未被 Supervisor 接受时撤销计划身份，让调用方可在 UserHook 内重新提交。
    ///
    /// 参数说明：`plan_key` 是此前已线性化登记、但尚未取得任务所有权的计划身份。
    ///
    /// 返回：无；只移除完全相同的 datasource 与消费命名空间组合。
    fn unregister_plan(&self, plan_key: &InboxRetentionKey) {
        self.configured_plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(plan_key);
    }

    /// 业务作用：在首个计划登记时把唯一指标源并入进程目录，后续计划复用同一聚合源。
    ///
    /// 参数说明：`application` 提供进程级指标 hub。
    ///
    /// 返回：源已存在或首次登记成功时完成；descriptor 冲突或序列预算不足时拒绝启动。
    fn ensure_metrics_registered(&self, application: &Application) -> ApplicationResult<()> {
        let mut registered = self
            .metrics_registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *registered {
            return Ok(());
        }
        application
            .metrics_hub()
            .register_legacy_source_reserved(
                Arc::new(InboxRetentionMetricsSource {
                    metrics: Arc::clone(&self.metrics),
                }),
                INBOX_RETENTION_METRIC_SERIES,
            )
            .map_err(|error| {
                inbox_error(
                    ApplicationPhase::UserHook,
                    format!("inbox retention metrics registration failed: {error}"),
                )
            })?;
        *registered = true;
        Ok(())
    }

    /// 业务作用：返回当前进程内所有受管清理循环的聚合快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含 datasource 与 consumer label 的低基数账目。
    fn snapshot(&self) -> InboxRetentionSnapshot {
        self.metrics.snapshot()
    }
}

/// 业务作用：把无标签累计值构造成统一目录的 Counter 样本。
///
/// 参数说明：
/// - `name`：已登记的固定指标名。
/// - `value`：当前单调累计值。
///
/// 返回：不携带业务 label 的 Counter 样本。
fn counter(name: &'static str, value: u64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Counter(value),
    }
}

/// 业务作用：把无标签当前值构造成统一目录的 Gauge 样本。
///
/// 参数说明：
/// - `name`：已登记的固定指标名。
/// - `value`：当前非负观测值。
///
/// 返回：不携带业务 label 的 Gauge 样本。
fn gauge(name: &'static str, value: u64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Gauge(value as f64),
    }
}

/// 业务作用：按 datasource catalog 冻结数据库后端，并创建不会回退到其它数据源的清理适配器。
///
/// 参数说明：
/// - `application`：提供受管 datasource catalog 与类型化 pool getter。
/// - `datasource_ref`：计划绑定的规范 datasource 名称。
///
/// 返回：名称与已编入后端一致时返回清理能力；缺失、driver 错配或停机时返回阶段错误。
async fn create_managed_inbox(
    application: &Application,
    datasource_ref: &str,
) -> ApplicationResult<Arc<dyn DurableInboxRetention>> {
    #[cfg(all(feature = "inbox", feature = "inbox-pgsql"))]
    let driver = {
        let catalog = application
            .resource::<Arc<natx_core::DataSourceCatalog>>()
            .await?;
        let reference = natx_core::DatasourceRef::new(datasource_ref).map_err(|error| {
            inbox_source_error(
                ApplicationPhase::UserHook,
                "inbox retention datasource_ref is invalid",
                error,
            )
        })?;
        catalog
            .entries()
            .into_iter()
            .find_map(|(candidate, driver)| (candidate == reference).then_some(driver))
            .ok_or_else(|| {
                inbox_error(
                    ApplicationPhase::UserHook,
                    "inbox retention datasource_ref is not present in the managed catalog",
                )
            })?
    };

    #[cfg(all(feature = "inbox", not(feature = "inbox-pgsql")))]
    let driver = natx_core::DatabaseDriver::MySql;
    #[cfg(all(not(feature = "inbox"), feature = "inbox-pgsql"))]
    let driver = natx_core::DatabaseDriver::PostgreSql;

    match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "inbox")]
            {
                let _pool = application.datasource(datasource_ref).await?;
                let inbox = nainbox_mysql::MySqlInbox::with_datasource(datasource_ref).map_err(
                    |error| {
                        inbox_source_error(
                            ApplicationPhase::UserHook,
                            "inbox retention MySQL datasource_ref is invalid",
                            error,
                        )
                    },
                )?;
                Ok(Arc::new(inbox))
            }
            #[cfg(not(feature = "inbox"))]
            Err(inbox_error(
                ApplicationPhase::UserHook,
                "inbox retention datasource requires the MySQL Inbox capability",
            ))
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "inbox-pgsql")]
            {
                let _pool = application.pg_datasource(datasource_ref).await?;
                let inbox =
                    nainbox_pgsql::PgInbox::with_datasource(datasource_ref).map_err(|error| {
                        inbox_source_error(
                            ApplicationPhase::UserHook,
                            "inbox retention PostgreSQL datasource_ref is invalid",
                            error,
                        )
                    })?;
                Ok(Arc::new(inbox))
            }
            #[cfg(not(feature = "inbox-pgsql"))]
            Err(inbox_error(
                ApplicationPhase::UserHook,
                "inbox retention datasource requires the PostgreSQL Inbox capability",
            ))
        }
    }
}

/// 业务作用：在 Ready 后串行执行单个消费命名空间的 fixed-delay 清理轮次。
///
/// 参数说明：
/// - `cancellation`：TaskSupervisor 在停机开始时触发的组级取消令牌。
/// - `retention`：已经绑定 datasource 的后端清理能力。
/// - `consumer_name`：本循环唯一负责的消费命名空间。
/// - `policy`：每轮复验并执行的保留安全策略。
/// - `interval`：一轮结束后到下一轮开始前的延迟。
/// - `metrics`：所有循环共享的无标签聚合指标。
///
/// 返回：收到停机信号且当前轮次已在预算内完成时正常结束；循环本身不会因单轮存储失败退出。
async fn run_retention_loop(
    cancellation: tokio_util::sync::CancellationToken,
    retention: Arc<dyn DurableInboxRetention>,
    plan_key: InboxRetentionKey,
    consumer_name: String,
    policy: InboxRetentionPolicy,
    interval: Duration,
    metrics: Arc<InboxRetentionMetrics>,
) -> anyhow::Result<()> {
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }

        // 协作取消不会中断已进入数据库的轮次；adapter 的墙钟预算覆盖取连接、owner 释放和会话处置。
        // 若全局停机截止时间先到，Supervisor 会终止任务，adapter 的 armed session guard 会关闭物理
        // 连接而不是把锁状态未知的 session 放回池。
        match retention.retention_round(&consumer_name, &policy).await {
            Ok(report) => metrics.observe(&plan_key, report),
            Err(error) => {
                metrics.observe_failure();
                tracing::warn!(error = %error, "Inbox retention round failed");
            }
        }

        if cancellation.is_cancelled() {
            return Ok(());
        }
        // 延迟从上一轮完整收口后开始，慢轮次不会与下一轮重叠，也不会积累补跑债务。
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

impl Application {
    /// 业务作用：登记一个或多个由 Application 所有的 Inbox 去重标记保留循环。
    ///
    /// 计划只在 UserHook 接受；数据源存在性在 Ready 之前复验，实际清理在全局 Ready 后才开始。
    /// 相同 datasource 内的消费命名空间不能重复登记；不同去重集合各自串行且共享无标签指标源。
    ///
    /// 参数说明：`plan` 包含 datasource、消费命名空间、保留安全窗口和 fixed-delay 间隔。
    ///
    /// 返回：数据源已冻结、指标源已登记且监督器受理任务时成功；阶段、名称、后端或指标合同不满足时
    /// 拒绝启动。成功不代表已经执行首轮删除，首轮只会在 Application 发布 Ready 后开始。
    pub async fn configure_inbox_retention(
        &self,
        plan: InboxRetentionPlan,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("inbox retention configuration")?;
        self.ensure_component_declared(
            ComponentId::Db,
            ApplicationPhase::UserHook,
            "inbox retention configuration",
        )?;
        let runtime = self.inbox_retention_runtime();
        let retention = create_managed_inbox(self, &plan.datasource_ref).await?;
        runtime.ensure_metrics_registered(self)?;
        let plan_key = InboxRetentionKey::new(plan.datasource_ref, plan.consumer_name);
        runtime.register_plan(&plan_key)?;
        let task_name = format!(
            "inbox-retention:{}:{}",
            plan_key.datasource_ref, plan_key.consumer_name
        );
        let metrics = Arc::clone(&runtime.metrics);
        let task_plan_key = plan_key.clone();
        let task_consumer_name = plan_key.consumer_name.clone();
        let registration = self
            .serve_when_ready(task_name, move |cancellation| {
                run_retention_loop(
                    cancellation,
                    retention,
                    task_plan_key,
                    task_consumer_name,
                    plan.policy,
                    plan.interval,
                    metrics,
                )
            })
            .await;
        if let Err(error) = registration {
            runtime.unregister_plan(&plan_key);
            return Err(error);
        }
        Ok(())
    }

    /// 业务作用：读取所有受管 Inbox 保留循环的聚合账目，不授予删除或停机权限。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件能力已编入时返回当前低基数快照；没有计划或尚未执行轮次时各值为零。
    pub fn inbox_retention_snapshot(&self) -> InboxRetentionSnapshot {
        self.inbox_retention_runtime().snapshot()
    }
}

/// 业务作用：构造不携带底层错误链的 Inbox retention 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败被观察到的 Application 阶段。
/// - `message`：不含数据库身份和消费身份的稳定摘要。
///
/// 返回：归类到 Application 组件的统一错误。
fn inbox_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, phase, message)
}

/// 业务作用：构造保留内部诊断链但公开摘要稳定的 Inbox retention 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败被观察到的 Application 阶段。
/// - `message`：不含数据库身份和消费身份的稳定摘要。
/// - `source`：只进入内部错误链的底层原因。
///
/// 返回：归类到 Application 组件且公开格式不拼接 source 的统一错误。
fn inbox_source_error(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Application, phase, message, source)
}
