//! 事务型 Outbox 的 Application 生命周期组件。
//!
//! 业务在 UserHook 只提交发布端和明确的毒丸策略；组件在 Ready 前验证数据库可达，随后持续执行
//! 当前 datasource 对应的 dispatcher，并把连续失败映射到统一 readiness。停机顺序固定为先停止投递，再释放 transport
//! 和数据库，避免发布中途失去下游或持久化连接。

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use naoutbox_core::{DurableOutbox, OutboxArchive, OutboxPublisher, OutboxRetentionPolicy};
use serde::Deserialize;
use tokio::sync::{watch, Mutex as AsyncMutex};

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 1_000;
const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_BATCH_SIZE: u32 = 100;
const MAX_INTERVAL_MS: u64 = 60_000;
const MAX_BATCH_SIZE: u32 = 10_000;
const MAX_FAILURE_THRESHOLD: u32 = 100;
/// 持久化积压指标的最短刷新间隔，避免高频出口按 lane 放大数据库全量计数负载。
const OUTBOX_METRICS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// 持久化指标刷新失败后的重试间隔，避免瞬时故障让 last-good 快照长期停滞。
const OUTBOX_METRICS_FAILURE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL: &str = "global";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboxBackend {
    #[cfg(feature = "outbox")]
    MySql,
    #[cfg(feature = "outbox-pgsql")]
    PostgreSql,
}

/// 业务作用：把受管 Outbox 的后端身份与后端中立能力绑定，避免运行循环按具体数据库复制。
#[derive(Clone)]
struct ManagedOutbox {
    backend: OutboxBackend,
    store: Arc<dyn DurableOutbox>,
}

impl std::ops::Deref for ManagedOutbox {
    type Target = dyn DurableOutbox;

    /// 业务作用：把 dispatcher、指标与 retention 调用统一委托给 Ready 时冻结的后端能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同一 datasource 绑定的后端中立 Outbox 引用。
    fn deref(&self) -> &Self::Target {
        self.store.as_ref()
    }
}

/// 业务作用：按当前编译能力校验 lane 名，保证配置在具体 adapter 安装前得到一致裁决。
///
/// 参数说明：`channel` 是待进入路由表与指标 label 的 lane 名。
///
/// 返回：任一已编入 adapter 接受该规范名称时返回真。
fn valid_channel_name(channel: &str) -> bool {
    #[cfg(feature = "outbox")]
    return naoutbox_mysql::valid_channel_name(channel);
    #[cfg(all(not(feature = "outbox"), feature = "outbox-pgsql"))]
    return naoutbox_pgsql::valid_channel_name(channel);
}

/// 业务作用：从冻结 catalog 解析 datasource 后端，并构造对应的受管 Outbox 能力。
///
/// 参数说明：
/// - `application`：提供当前 Application 的 catalog 与类型化 pool getter。
/// - `datasource`：Outbox 配置绑定的规范名称。
///
/// 返回：名称、driver 与已编入能力一致时返回后端中立句柄；任何错配在 dispatcher 启动前失败。
async fn create_managed_outbox(
    application: &Application,
    datasource: &str,
) -> ApplicationResult<ManagedOutbox> {
    #[cfg(all(feature = "outbox", feature = "outbox-pgsql"))]
    let driver = {
        let catalog = application
            .resource::<Arc<natx_core::DataSourceCatalog>>()
            .await?;
        let reference = natx_core::DatasourceRef::new(datasource).map_err(|error| {
            outbox_source_error(
                ApplicationPhase::Ready,
                "outbox datasource_ref is invalid",
                error,
            )
        })?;
        catalog
            .entries()
            .into_iter()
            .find_map(|(candidate, driver)| (candidate == reference).then_some(driver))
            .ok_or_else(|| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox datasource_ref is not present in the managed catalog",
                )
            })?
    };

    #[cfg(all(feature = "outbox", not(feature = "outbox-pgsql")))]
    let driver = natx_core::DatabaseDriver::MySql;
    #[cfg(all(not(feature = "outbox"), feature = "outbox-pgsql"))]
    let driver = natx_core::DatabaseDriver::PostgreSql;

    match driver {
        natx_core::DatabaseDriver::MySql => {
            #[cfg(feature = "outbox")]
            {
                let _pool = application.datasource(datasource).await?;
                let store =
                    naoutbox_mysql::MySqlOutbox::with_datasource(datasource).map_err(|error| {
                        outbox_source_error(
                            ApplicationPhase::Ready,
                            "outbox MySQL datasource_ref is invalid",
                            error,
                        )
                    })?;
                Ok(ManagedOutbox {
                    backend: OutboxBackend::MySql,
                    store: Arc::new(store),
                })
            }
            #[cfg(not(feature = "outbox"))]
            Err(outbox_error(
                ApplicationPhase::Ready,
                "outbox datasource requires the MySQL Outbox capability",
            ))
        }
        natx_core::DatabaseDriver::PostgreSql => {
            #[cfg(feature = "outbox-pgsql")]
            {
                let _pool = application.pg_datasource(datasource).await?;
                let store =
                    naoutbox_pgsql::PgOutbox::with_datasource(datasource).map_err(|error| {
                        outbox_source_error(
                            ApplicationPhase::Ready,
                            "outbox PostgreSQL datasource_ref is invalid",
                            error,
                        )
                    })?;
                Ok(ManagedOutbox {
                    backend: OutboxBackend::PostgreSql,
                    store: Arc::new(store),
                })
            }
            #[cfg(not(feature = "outbox-pgsql"))]
            Err(outbox_error(
                ApplicationPhase::Ready,
                "outbox datasource requires the PostgreSQL Outbox capability",
            ))
        }
    }
}

/// 业务作用：按冻结后端安装 Outbox 进程级租户配额，保持写侧与受管 dispatcher 口径一致。
fn install_tenant_quotas(
    backend: OutboxBackend,
    quotas: BTreeMap<String, u64>,
) -> Result<(), naoutbox_core::OutboxStoreError> {
    match backend {
        #[cfg(feature = "outbox")]
        OutboxBackend::MySql => naoutbox_mysql::install_outbox_tenant_quotas(quotas),
        #[cfg(feature = "outbox-pgsql")]
        OutboxBackend::PostgreSql => naoutbox_pgsql::install_outbox_tenant_quotas(quotas),
    }
}

/// 业务作用：按冻结后端安装 aggregate 到 lane 的唯一写侧路由。
fn install_channel_routes(
    backend: OutboxBackend,
    routes: BTreeMap<String, String>,
) -> Result<(), naoutbox_core::OutboxStoreError> {
    match backend {
        #[cfg(feature = "outbox")]
        OutboxBackend::MySql => naoutbox_mysql::install_channel_routes(routes),
        #[cfg(feature = "outbox-pgsql")]
        OutboxBackend::PostgreSql => naoutbox_pgsql::install_channel_routes(routes),
    }
}

/// 业务作用：在 Ready 前按实际后端复验 Outbox 事件表与可选配额账本。
async fn verify_outbox_schema(
    outbox: &ManagedOutbox,
    datasource: &str,
    quotas_enabled: bool,
) -> Result<(), naoutbox_core::OutboxStoreError> {
    match outbox.backend {
        #[cfg(feature = "outbox")]
        OutboxBackend::MySql => {
            naoutbox_mysql::verify_outbox_event_schema_for(datasource).await?;
            if quotas_enabled {
                naoutbox_mysql::verify_outbox_tenant_quota_schema_for(datasource).await?;
            }
        }
        #[cfg(feature = "outbox-pgsql")]
        OutboxBackend::PostgreSql => {
            naoutbox_pgsql::verify_outbox_event_schema_for(datasource).await?;
            if quotas_enabled {
                naoutbox_pgsql::verify_outbox_tenant_quota_schema_for(datasource).await?;
            }
        }
    }
    Ok(())
}

/// 业务作用：读取当前受管 Outbox 后端的低基数配额拒绝累计值。
fn quota_rejections_total(outbox: Option<&ManagedOutbox>) -> u64 {
    match outbox.map(|outbox| outbox.backend) {
        #[cfg(feature = "outbox")]
        Some(OutboxBackend::MySql) => naoutbox_mysql::outbox_quota_rejections_total(),
        #[cfg(feature = "outbox-pgsql")]
        Some(OutboxBackend::PostgreSql) => naoutbox_pgsql::outbox_quota_rejections_total(),
        None => 0,
    }
}

macro_rules! outbox_metric {
    ($ident:ident, $name:literal, $help:literal, $kind:expr, $labels:expr) => {
        static $ident: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: $kind,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}

outbox_metric!(
    OUTBOX_ROUNDS,
    "napp_outbox_rounds_total",
    "Dispatcher completed rounds.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_PUBLISHED,
    "napp_outbox_published_total",
    "Events confirmed by the downstream publisher.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_FAILED_ROUNDS,
    "napp_outbox_failed_rounds_total",
    "Dispatcher rounds ending in storage, timeout, or publish failure.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_PENDING,
    "napp_outbox_pending",
    "Durable events waiting for downstream confirmation.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_DEAD,
    "napp_outbox_dead",
    "Durable events retained in the local dead-letter set.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_METRICS_SNAPSHOT_AGE,
    "napp_outbox_metrics_snapshot_age_seconds",
    "Age in seconds of the last complete durable metrics snapshot; zero before the first success.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_METRICS_REFRESH_FAILED,
    "napp_outbox_metrics_refresh_failed",
    "Whether the latest durable metrics refresh attempt failed.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_METRICS_REFRESH_FAILURES,
    "napp_outbox_metrics_refresh_failures_total",
    "Durable metrics refresh attempts ending in a storage query failure.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_QUOTA_REJECTIONS,
    "napp_outbox_tenant_quota_rejections_total",
    "Appends rejected by the per-tenant in-flight quota.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_ROUNDS,
    "napp_outbox_retention_rounds_total",
    "Retention rounds completed, including idle and contended rounds.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_ARCHIVED,
    "napp_outbox_retention_archived_total",
    "Events confirmed archived with a verifiable receipt.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_DELETED_DISPATCHED,
    "napp_outbox_retention_deleted_dispatched_total",
    "Dispatched rows deleted under the approved policy.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_DELETED_DEAD,
    "napp_outbox_retention_deleted_dead_total",
    "Dead-letter rows deleted under the approved dead policy.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_FAILED_ROUNDS,
    "napp_outbox_retention_failed_rounds_total",
    "Retention rounds ending in storage, archive, or timeout failure.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_CLAIM_CONTENDED,
    "napp_outbox_retention_claim_contended_total",
    "Retention rounds skipped because another owner held the claim.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_BUDGET_EXHAUSTED,
    "napp_outbox_retention_budget_exhausted_total",
    "Retention rounds ended by budget before candidates were fully processed.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_OLDEST_AGE,
    "napp_outbox_retention_oldest_candidate_age_ms",
    "Age of the oldest retention candidate observed in the latest round.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_LOCK_CONTENTION,
    "napp_outbox_retention_lock_contention_total",
    "Rounds yielded because disposal statements encountered row-lock contention.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_COMMIT_UNCERTAIN,
    "napp_outbox_retention_commit_uncertain_total",
    "Rounds whose deletion commit acknowledgement could not be confirmed.",
    nametrics_core::MetricKind::Counter,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_INTERVAL,
    "napp_outbox_retention_interval_ms",
    "Configured pause between retention rounds.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_RETENTION_LAST_SUCCESS,
    "napp_outbox_retention_last_success_ms",
    "Epoch milliseconds of the last successful retention round.",
    nametrics_core::MetricKind::Gauge,
    &[]
);
outbox_metric!(
    OUTBOX_LANE_PUBLISHED,
    "napp_outbox_lane_published_total",
    "Events confirmed by the downstream publisher in one frozen channel lane.",
    nametrics_core::MetricKind::Counter,
    &["channel"]
);
outbox_metric!(
    OUTBOX_LANE_FAILED,
    "napp_outbox_lane_failed_rounds_total",
    "Failed dispatcher rounds in one frozen channel lane.",
    nametrics_core::MetricKind::Counter,
    &["channel"]
);
outbox_metric!(
    OUTBOX_LANE_HEALTHY,
    "napp_outbox_lane_healthy",
    "Whether the latest round in one frozen channel lane was healthy.",
    nametrics_core::MetricKind::Gauge,
    &["channel"]
);
outbox_metric!(
    OUTBOX_LANE_PENDING,
    "napp_outbox_lane_pending",
    "Durable events waiting in one frozen channel lane.",
    nametrics_core::MetricKind::Gauge,
    &["channel"]
);

/// 无 label 的 Outbox family 数;快照对每个 family 恒产一条,是预算的固定项。
const OUTBOX_FIXED_METRIC_SERIES: usize = 21;
/// 每个冻结 channel lane 的 family 数(published/failed/healthy/pending);预算按 lane 数线性扩展。
const OUTBOX_LANE_METRIC_SERIES: usize = 4;

static OUTBOX_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 25] = [
    &OUTBOX_ROUNDS,
    &OUTBOX_PUBLISHED,
    &OUTBOX_FAILED_ROUNDS,
    &OUTBOX_PENDING,
    &OUTBOX_DEAD,
    &OUTBOX_METRICS_SNAPSHOT_AGE,
    &OUTBOX_METRICS_REFRESH_FAILED,
    &OUTBOX_METRICS_REFRESH_FAILURES,
    &OUTBOX_QUOTA_REJECTIONS,
    &OUTBOX_RETENTION_ROUNDS,
    &OUTBOX_RETENTION_ARCHIVED,
    &OUTBOX_RETENTION_DELETED_DISPATCHED,
    &OUTBOX_RETENTION_DELETED_DEAD,
    &OUTBOX_RETENTION_FAILED_ROUNDS,
    &OUTBOX_RETENTION_CLAIM_CONTENDED,
    &OUTBOX_RETENTION_BUDGET_EXHAUSTED,
    &OUTBOX_RETENTION_OLDEST_AGE,
    &OUTBOX_RETENTION_LOCK_CONTENTION,
    &OUTBOX_RETENTION_COMMIT_UNCERTAIN,
    &OUTBOX_RETENTION_INTERVAL,
    &OUTBOX_RETENTION_LAST_SUCCESS,
    &OUTBOX_LANE_PUBLISHED,
    &OUTBOX_LANE_FAILED,
    &OUTBOX_LANE_HEALTHY,
    &OUTBOX_LANE_PENDING,
];

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 业务作用：承载 Outbox dispatcher 的轮询、退避、单轮预算、批次与摘流配置。
struct OutboxSettings {
    datasource_ref: String,
    poll_interval_ms: u64,
    error_backoff_ms: u64,
    operation_timeout_ms: u64,
    batch_size: u32,
    failure_threshold: u32,
}

impl Default for OutboxSettings {
    /// 业务作用：提供不会忙循环且单轮工作量有界的 Outbox 运行参数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：500ms 轮询、1s 故障退避、5s 单轮预算、100 行批次和三次失败摘流。
    fn default() -> Self {
        Self {
            datasource_ref: crate::db::DEFAULT_DATASOURCE.to_owned(),
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            error_backoff_ms: DEFAULT_ERROR_BACKOFF_MS,
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
            batch_size: DEFAULT_BATCH_SIZE,
            failure_threshold: 3,
        }
    }
}

/// 业务作用：明确 Outbox 首个毒丸是否允许让出有序投递通道。
///
/// 默认 `Block` 保持严格顺序并等待人工处置；`DeadLetter` 是显式可用性让步，达到预算后把毒丸留在
/// 数据库死信集合，并允许后续事件继续投递。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxPoisonPolicy {
    /// 首个失败事件持续阻塞后续事件，不自动改变持久化事实。
    Block,
    /// 同一事件达到正数失败预算后进入数据库死信集合。
    DeadLetter {
        /// 标记死信前允许的失败轮次。
        max_attempts: u32,
    },
}

/// 业务作用：描述一个进程唯一的受管 Outbox 发布计划。
///
/// 发布端负责把事件映射到 Kafka、Redis Streams 或其它下游；组件只负责持久化轮询和生命周期，
/// 因而 Outbox 可以脱离 Saga 独立使用。
pub struct OutboxApplicationPlan {
    publisher: Arc<dyn OutboxPublisher + Send + Sync>,
    poison_policy: OutboxPoisonPolicy,
    retention: Option<OutboxRetentionPlan>,
    channels: Option<OutboxChannelPlan>,
    tenant_quotas: Option<std::collections::BTreeMap<String, u64>>,
}

/// 业务作用：冻结多通道分片子计划——路由与 lane 集合在 UserHook 一次提交，Ready 时
/// 安装为进程级冻结路由并按 lane 拆分 dispatcher 所有权。
///
/// 分片是显式 opt-in：未提交本计划时保持单一 `global` lane 的未分片 dispatcher，
/// 升级不会静默开启并行发布。
pub struct OutboxChannelPlan {
    routes: std::collections::BTreeMap<String, String>,
    lanes: Vec<String>,
}

/// 业务作用：单个 lane 的进程级观测状态，区分"某个领域停摆"与"整个 dispatcher 退出"。
pub(crate) struct LaneRuntime {
    channel: String,
    published: AtomicU64,
    failed_rounds: AtomicU64,
    healthy: AtomicBool,
    pending: AtomicU64,
}

impl LaneRuntime {
    /// 业务作用：读取 lane 名，作为按 lane 指标的有界标签值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：lane 名切片。
    pub(crate) fn channel_name(&self) -> &str {
        &self.channel
    }

    /// 业务作用：读取本 lane 已获下游确认的事件累计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本 lane 的发布累计值。
    pub(crate) fn published_total(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    /// 业务作用：读取本 lane 的失败轮次，定位单领域停摆。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本 lane 的失败轮次累计。
    pub(crate) fn failed_rounds_total(&self) -> u64 {
        self.failed_rounds.load(Ordering::Relaxed)
    }

    /// 业务作用：读取本 lane 最近一轮是否健康。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最近一轮无失败返回真。
    pub(crate) fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// 业务作用：读取最近一次观测的 lane 持久化积压。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：指标出口最近一次查询到的未确认事件数。
    fn pending_count(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }
}

/// 业务作用：冻结受管 Outbox 的保留清理子计划——策略、归档端与轮询节奏在 UserHook
/// 一次提交，Ready 前完成全部校验，没有已批准策略就没有清理任务。
pub struct OutboxRetentionPlan {
    policy: OutboxRetentionPolicy,
    archive: Option<Arc<dyn OutboxArchive + Send + Sync>>,
    interval_ms: u64,
}

impl OutboxApplicationPlan {
    /// 业务作用：创建默认严格保序的受管发布计划。
    ///
    /// 参数说明：
    /// - `publisher`：线程安全的下游发布端；成功必须代表下游已经确认接收。
    ///
    /// 返回：使用 `Block` 毒丸策略的计划。
    pub fn new<P>(publisher: Arc<P>) -> Self
    where
        P: OutboxPublisher + Send + Sync + 'static,
    {
        Self {
            publisher,
            poison_policy: OutboxPoisonPolicy::Block,
            retention: None,
            channels: None,
            tenant_quotas: None,
        }
    }

    /// 业务作用：提交每租户在飞事件配额——限制单租户挤占 Outbox 积压容量的显式 opt-in。
    ///
    /// Ready 时安装为进程级冻结配额;列出的租户在受信 append 事务内原子预留、投递/死信
    /// 裁决时同事务释放,未列出的租户不记账不设限。**把某租户纳入配额前,该租户全部
    /// 写入必须已改走受信上下文入口**,否则释放路径会造成账本漂移(由有界对账收敛)。
    ///
    /// 参数说明：
    /// - `quotas`: 租户到在飞事件上限的冻结映射;`0` 表示禁止该租户新 append。
    ///
    /// 返回：租户名合法且首次提交时返回自身;重复提交或名称越界返回 UserHook 错误。
    pub fn with_tenant_quotas(
        mut self,
        quotas: std::collections::BTreeMap<String, u64>,
    ) -> ApplicationResult<Self> {
        if self.tenant_quotas.is_some() {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox tenant quotas can be configured only once",
            ));
        }
        for tenant in quotas.keys() {
            if tenant.is_empty() || tenant.len() > 256 || tenant.chars().any(char::is_control) {
                return Err(outbox_error(
                    ApplicationPhase::UserHook,
                    "outbox tenant quotas require bounded tenant names",
                ));
            }
        }
        self.tenant_quotas = Some(quotas);
        Ok(self)
    }

    /// 业务作用：为计划附加多通道分片子计划——显式 opt-in 改变停摆半径，不改变投递
    /// 不变量（每 lane 仍是"成功前缀才标记"，`Block` 语义不变）。
    ///
    /// 校验在 UserHook 立即执行：lane 名与路由目标必须 canonical、路由目标必须落在
    /// lane 集合内、lane 集合必须包含默认 `global` lane（未路由类型的归属）。启用后
    /// 同库不得再运行未分片 dispatcher——本组件内部自动切换，跨进程部署纪律见迁移说明。
    ///
    /// 参数说明：
    /// - `routes`：aggregate_type 到 lane 名的冻结映射；未列出的类型归入 `global`。
    /// - `lanes`：本进程要运行的 lane 集合（含 `global`）。
    ///
    /// 返回：校验通过返回更新后的计划；任何不自洽返回 UserHook 配置错误。
    pub fn with_channel_lanes(
        mut self,
        routes: std::collections::BTreeMap<String, String>,
        lanes: Vec<String>,
    ) -> ApplicationResult<Self> {
        if lanes.is_empty() {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox channel plan requires at least one lane",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for lane in &lanes {
            if !valid_channel_name(lane) {
                return Err(outbox_error(
                    ApplicationPhase::UserHook,
                    "outbox lane names must be canonical identifiers",
                ));
            }
            if !seen.insert(lane.as_str()) {
                return Err(outbox_error(
                    ApplicationPhase::UserHook,
                    "outbox lanes must be unique",
                ));
            }
        }
        if !seen.contains(DEFAULT_CHANNEL) {
            // 未路由类型都会落入默认 lane;不服务它就会留下永远无人投递的行。
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox lanes must include the default 'global' lane",
            ));
        }
        for (aggregate_type, channel) in &routes {
            if aggregate_type.is_empty() || !valid_channel_name(channel) {
                return Err(outbox_error(
                    ApplicationPhase::UserHook,
                    "outbox channel routes must be canonical",
                ));
            }
            if !seen.contains(channel.as_str()) {
                return Err(outbox_error(
                    ApplicationPhase::UserHook,
                    "outbox channel routes must target a declared lane",
                ));
            }
        }
        self.channels = Some(OutboxChannelPlan { routes, lanes });
        Ok(self)
    }

    /// 业务作用：为计划附加保留清理子计划——执行器绝不从"开启了 Outbox"推断保留期，
    /// 未调用本方法就没有任何删除。
    ///
    /// 校验在 UserHook 立即执行、Ready 前 fail-closed：策略值不自洽、`archive_required`
    /// 或 `delete_dead` 缺归档端、清理间隔越界都拒绝启动，不做"自动修正"。
    ///
    /// 参数说明：
    /// - `policy`：已获治理批准并冻结的保留策略。
    /// - `interval_ms`：两轮清理之间的间隔（1s..=1h）；清理是低频治理动作，不是热路径。
    /// - `archive`：归档端；策略要求收据时必须提供。
    ///
    /// 返回：校验通过返回更新后的计划；任何不自洽返回 UserHook 配置错误。
    pub fn with_retention(
        mut self,
        policy: OutboxRetentionPolicy,
        interval_ms: u64,
        archive: Option<Arc<dyn OutboxArchive + Send + Sync>>,
    ) -> ApplicationResult<Self> {
        if let Err(reason) = policy.validate() {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                format!("outbox retention policy is invalid: {reason}"),
            ));
        }
        if (policy.archive_required || policy.delete_dead) && archive.is_none() {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox retention policy requires an archive target",
            ));
        }
        if !(1_000..=3_600_000).contains(&interval_ms) {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox retention interval must be within 1s..=1h",
            ));
        }
        self.retention = Some(OutboxRetentionPlan {
            policy,
            archive,
            interval_ms,
        });
        Ok(self)
    }

    /// 业务作用：显式允许毒丸达到预算后进入死信集合，避免永久阻塞后续事件。
    ///
    /// 参数说明：
    /// - `max_attempts`：同一首个失败事件进入死信前的正数失败轮次。
    ///
    /// 返回：预算有效时返回更新后的计划；零预算返回 UserHook 配置错误。
    pub fn dead_letter_after(mut self, max_attempts: u32) -> ApplicationResult<Self> {
        if max_attempts == 0 {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox dead-letter attempts must be greater than zero",
            ));
        }
        self.poison_policy = OutboxPoisonPolicy::DeadLetter { max_attempts };
        Ok(self)
    }
}

/// 业务作用：提供 Outbox 当前累计投递状态的低基数快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxSnapshot {
    rounds: u64,
    published: u64,
    failed_rounds: u64,
    retention_rounds: u64,
    retention_archived: u64,
    retention_deleted_dispatched: u64,
    retention_deleted_dead: u64,
    retention_failed_rounds: u64,
    retention_claim_contended: u64,
    retention_budget_exhausted: u64,
    retention_oldest_candidate_age_ms: u64,
    retention_lock_contention: u64,
    retention_commit_uncertain: u64,
    retention_interval_ms: u64,
    retention_last_success_ms: u64,
}

impl OutboxSnapshot {
    /// 业务作用：读取当前进程已完成的 dispatcher 轮次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含空轮次与失败轮次的累计值。
    pub fn rounds(self) -> u64 {
        self.rounds
    }

    /// 业务作用：读取当前进程已获得下游确认的事件累计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含仍待投递或已进入死信集合的事件数。
    pub fn published(self) -> u64 {
        self.published
    }

    /// 业务作用：读取发生存储错误、超时或发布失败的 dispatcher 轮次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：进程启动以来的累计失败轮次。
    pub fn failed_rounds(self) -> u64 {
        self.failed_rounds
    }

    /// 业务作用：读取保留清理已完成的轮次（含空轮次与竞争让路轮次）。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：进程启动以来的累计清理轮次；未配置保留计划时恒为 0。
    pub fn retention_rounds(self) -> u64 {
        self.retention_rounds
    }

    /// 业务作用：读取已取得可复验归档收据的事件累计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：真实写入或收据重查确认的累计值。
    pub fn retention_archived(self) -> u64 {
        self.retention_archived
    }

    /// 业务作用：读取按保留策略删除的已投递行累计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已提交 DELETE 的行数。
    pub fn retention_deleted_dispatched(self) -> u64 {
        self.retention_deleted_dispatched
    }

    /// 业务作用：读取按独立批准清理的死信行累计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已提交 DELETE 的死信行数。
    pub fn retention_deleted_dead(self) -> u64 {
        self.retention_deleted_dead
    }

    /// 业务作用：读取保留清理失败轮次，长期增长即"严格治理 degraded"信号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存储、归档或超时导致的失败轮次累计。
    pub fn retention_failed_rounds(self) -> u64 {
        self.retention_failed_rounds
    }

    /// 业务作用：读取 retention claim 竞争让路的轮次，观察多副本清理互斥。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：claim 已被其它 owner 持有的轮次累计。
    pub fn retention_claim_contended(self) -> u64 {
        self.retention_claim_contended
    }

    /// 业务作用：读取因预算耗尽而未走完候选的清理轮累计——它们不刷新"最近成功"。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预算耗尽轮累计值。
    pub fn retention_budget_exhausted(self) -> u64 {
        self.retention_budget_exhausted
    }

    /// 业务作用：读取删除/处置语句因行锁竞争(锁等待超时/死锁)让路的轮次累计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：锁竞争轮累计值。
    pub fn retention_lock_contention(self) -> u64 {
        self.retention_lock_contention
    }

    /// 业务作用：读取删除事务已发出 COMMIT、但数据库应答无法确认的轮次累计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：提交不确定轮次累计值；这些轮次同时计入 retention 失败总数，但不会计入
    /// 已确认删除数或刷新最近成功时刻。
    pub fn retention_commit_uncertain(self) -> u64 {
        self.retention_commit_uncertain
    }

    /// 业务作用：读取清理循环的固定间隔——失败不缩短,它就是可观测的退避量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两轮之间的间隔毫秒。
    pub fn retention_interval_ms(self) -> u64 {
        self.retention_interval_ms
    }

    /// 业务作用：读取最近一轮观察到的最老清理候选年龄——候选长期滞留的直接证据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最老候选年龄毫秒;本轮无候选为 0。
    pub fn retention_oldest_candidate_age_ms(self) -> u64 {
        self.retention_oldest_candidate_age_ms
    }

    /// 业务作用：读取最近一轮成功清理的时刻，服务"最后成功时刻"告警。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：epoch 毫秒；0 表示从未成功（或未配置保留计划）。
    pub fn retention_last_success_ms(self) -> u64 {
        self.retention_last_success_ms
    }
}

/// 业务作用：提供 Ready 后的 Outbox 只读能力，不授予替换 publisher 或停止 dispatcher 的权限。
#[derive(Clone)]
pub struct OutboxHandle {
    pub(crate) state: Arc<OutboxRuntimeState>,
}

impl OutboxHandle {
    /// 业务作用：读取当前数据库仍等待发布的事件数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回持久化积压；停机或数据库失败返回统一错误。
    pub async fn pending_count(&self) -> ApplicationResult<u64> {
        self.state.ensure_ready()?;
        self.state.outbox()?.pending_count().await.map_err(|error| {
            outbox_source_error(
                ApplicationPhase::Running,
                "outbox pending count failed",
                error,
            )
        })
    }

    /// 业务作用：读取数据库中保留的 Outbox 死信事件数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回死信累计值；停机或数据库失败返回统一错误。
    pub async fn dead_count(&self) -> ApplicationResult<u64> {
        self.state.ensure_ready()?;
        self.state.outbox()?.dead_count().await.map_err(|error| {
            outbox_source_error(ApplicationPhase::Running, "outbox dead count failed", error)
        })
    }

    /// 业务作用：读取不含事件正文和业务身份的进程级投递计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回原子快照；停机或尚未发布时返回阶段错误。
    pub fn snapshot(&self) -> ApplicationResult<OutboxSnapshot> {
        self.state.ensure_ready()?;
        Ok(self.state.snapshot())
    }

    /// 业务作用：导出 Outbox 串行通道的积压、死信和 dispatcher 进程计数，区分空闲、下游故障与毒丸停摆。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：组件 Ready 时返回无业务标签的 Prometheus 文本；持久化查询失败时保留 last-good
    /// 数据，并用刷新失败与快照年龄指标公开其可信边界；停机或尚未 Ready 时返回统一错误。
    pub async fn render_prometheus(&self) -> ApplicationResult<String> {
        let snapshot = self.snapshot()?;
        // 单一持久化源不可决定整个指标出口的可用性；失败状态已进入同一快照供告警判断。
        let _ = refresh_metrics(&self.state).await;
        let pending = self.state.metrics_pending.load(Ordering::Relaxed);
        let dead = self.state.metrics_dead.load(Ordering::Relaxed);
        let snapshot_age = self.state.metrics_snapshot_age_seconds();
        let refresh_failed = u8::from(self.state.metrics_refresh_failed.load(Ordering::Acquire));
        let refresh_failures = self.state.metrics_refresh_failures.load(Ordering::Relaxed);
        // 拒绝计数来自当前后端 adapter 的进程内累计:低基数、不携带租户标签,
        // 精确租户用量只经受鉴权管理查询返回。
        let quota_rejections = quota_rejections_total(self.state.outbox.get());
        // lane 标签值来自 Ready 时冻结的 lane 集合,基数有界;未分片时不输出 lane 行。
        let mut lane_lines = String::new();
        for lane in self.state.lanes() {
            let lane_pending = lane.pending_count();
            lane_lines.push_str(&format!(
                "napp_outbox_lane_published_total{{channel=\"{channel}\"}} {published}\n\
                 napp_outbox_lane_failed_rounds_total{{channel=\"{channel}\"}} {failed}\n\
                 napp_outbox_lane_healthy{{channel=\"{channel}\"}} {healthy}\n\
                 napp_outbox_lane_pending{{channel=\"{channel}\"}} {lane_pending}\n",
                channel = lane.channel_name(),
                published = lane.published_total(),
                failed = lane.failed_rounds_total(),
                healthy = u8::from(lane.is_healthy()),
            ));
        }
        Ok(format!(
            "# HELP napp_outbox_rounds_total Dispatcher completed rounds.\n\
             # TYPE napp_outbox_rounds_total counter\n\
             napp_outbox_rounds_total {}\n\
             # HELP napp_outbox_published_total Events confirmed by the downstream publisher.\n\
             # TYPE napp_outbox_published_total counter\n\
             napp_outbox_published_total {}\n\
             # HELP napp_outbox_failed_rounds_total Dispatcher rounds ending in storage, timeout, or publish failure.\n\
             # TYPE napp_outbox_failed_rounds_total counter\n\
             napp_outbox_failed_rounds_total {}\n\
             # HELP napp_outbox_pending Durable events waiting for downstream confirmation.\n\
             # TYPE napp_outbox_pending gauge\n\
             napp_outbox_pending {pending}\n\
             # HELP napp_outbox_dead Durable events retained in the local dead-letter set.\n\
             # TYPE napp_outbox_dead gauge\n\
             napp_outbox_dead {dead}\n\
             # HELP napp_outbox_metrics_snapshot_age_seconds Age in seconds of the last complete durable metrics snapshot (0 before the first success).\n\
             # TYPE napp_outbox_metrics_snapshot_age_seconds gauge\n\
             napp_outbox_metrics_snapshot_age_seconds {snapshot_age}\n\
             # HELP napp_outbox_metrics_refresh_failed Whether the latest durable metrics refresh attempt failed.\n\
             # TYPE napp_outbox_metrics_refresh_failed gauge\n\
             napp_outbox_metrics_refresh_failed {refresh_failed}\n\
             # HELP napp_outbox_metrics_refresh_failures_total Durable metrics refresh attempts ending in a storage query failure.\n\
             # TYPE napp_outbox_metrics_refresh_failures_total counter\n\
             napp_outbox_metrics_refresh_failures_total {refresh_failures}\n\
             # HELP napp_outbox_tenant_quota_rejections_total Appends rejected by the per-tenant in-flight quota (no tenant labels; exact usage is an authorized management query).\n\
             # TYPE napp_outbox_tenant_quota_rejections_total counter\n\
             napp_outbox_tenant_quota_rejections_total {quota_rejections}\n\
             # HELP napp_outbox_retention_rounds_total Retention rounds completed (including idle and contended).\n\
             # TYPE napp_outbox_retention_rounds_total counter\n\
             napp_outbox_retention_rounds_total {}\n\
             # HELP napp_outbox_retention_archived_total Events confirmed archived with a verifiable receipt.\n\
             # TYPE napp_outbox_retention_archived_total counter\n\
             napp_outbox_retention_archived_total {}\n\
             # HELP napp_outbox_retention_deleted_dispatched_total Dispatched rows deleted under the approved policy.\n\
             # TYPE napp_outbox_retention_deleted_dispatched_total counter\n\
             napp_outbox_retention_deleted_dispatched_total {}\n\
             # HELP napp_outbox_retention_deleted_dead_total Dead-letter rows deleted under the approved dead policy.\n\
             # TYPE napp_outbox_retention_deleted_dead_total counter\n\
             napp_outbox_retention_deleted_dead_total {}\n\
             # HELP napp_outbox_retention_failed_rounds_total Retention rounds ending in storage, archive, or timeout failure.\n\
             # TYPE napp_outbox_retention_failed_rounds_total counter\n\
             napp_outbox_retention_failed_rounds_total {}\n\
             # HELP napp_outbox_retention_claim_contended_total Retention rounds skipped because another owner held the claim.\n\
             # TYPE napp_outbox_retention_claim_contended_total counter\n\
             napp_outbox_retention_claim_contended_total {}\n\
             # HELP napp_outbox_retention_budget_exhausted_total Retention rounds ended by budget before candidates were fully processed (these do not refresh last-success).\n\
             # TYPE napp_outbox_retention_budget_exhausted_total counter\n\
             napp_outbox_retention_budget_exhausted_total {}\n\
             # HELP napp_outbox_retention_oldest_candidate_age_ms Age of the oldest retention candidate observed in the latest round (0 = none).\n\
             # TYPE napp_outbox_retention_oldest_candidate_age_ms gauge\n\
             napp_outbox_retention_oldest_candidate_age_ms {}\n\
             # HELP napp_outbox_retention_lock_contention_total Rounds yielded because delete/disposal statements hit row-lock wait timeout or deadlock.\n\
             # TYPE napp_outbox_retention_lock_contention_total counter\n\
             napp_outbox_retention_lock_contention_total {}\n\
             # HELP napp_outbox_retention_commit_uncertain_total Rounds whose deletion COMMIT was sent but acknowledgement could not be confirmed.\n\
             # TYPE napp_outbox_retention_commit_uncertain_total counter\n\
             napp_outbox_retention_commit_uncertain_total {}\n\
             # HELP napp_outbox_retention_interval_ms Configured pause between retention rounds (fixed; failures never shorten it).\n\
             # TYPE napp_outbox_retention_interval_ms gauge\n\
             napp_outbox_retention_interval_ms {}\n\
             # HELP napp_outbox_retention_last_success_ms Epoch milliseconds of the last successful retention round (0 = never).\n\
             # TYPE napp_outbox_retention_last_success_ms gauge\n\
             napp_outbox_retention_last_success_ms {}\n{lane_lines}",
            snapshot.rounds,
            snapshot.published,
            snapshot.failed_rounds,
            snapshot.retention_rounds,
            snapshot.retention_archived,
            snapshot.retention_deleted_dispatched,
            snapshot.retention_deleted_dead,
            snapshot.retention_failed_rounds,
            snapshot.retention_claim_contended,
            snapshot.retention_budget_exhausted,
            snapshot.retention_oldest_candidate_age_ms,
            snapshot.retention_lock_contention,
            snapshot.retention_commit_uncertain,
            snapshot.retention_interval_ms,
            snapshot.retention_last_success_ms
        ))
    }
}

/// 业务作用：保存 Outbox 计划、生命周期门禁、累计计数和最近一份完整持久化指标快照。
pub(crate) struct OutboxRuntimeState {
    pending: Mutex<Option<OutboxApplicationPlan>>,
    sealed: AtomicBool,
    lifecycle: AtomicU8,
    /// Ready 门禁、dispatcher、retention 与指标查询共用的命名 datasource Outbox。
    outbox: OnceLock<ManagedOutbox>,
    rounds: AtomicU64,
    published: AtomicU64,
    failed_rounds: AtomicU64,
    retention_rounds: AtomicU64,
    retention_archived: AtomicU64,
    retention_deleted_dispatched: AtomicU64,
    retention_deleted_dead: AtomicU64,
    retention_failed_rounds: AtomicU64,
    retention_claim_contended: AtomicU64,
    retention_budget_exhausted: AtomicU64,
    retention_oldest_candidate_age_ms: AtomicU64,
    retention_lock_contention: AtomicU64,
    retention_commit_uncertain: AtomicU64,
    retention_interval_ms: AtomicU64,
    /// 最近一轮成功清理的 epoch 毫秒；0 表示从未成功。长期不前进即"严格治理 degraded"
    /// 信号，但清理停摆绝不反向停止 dispatcher。
    retention_last_success_ms: AtomicU64,
    /// 指标出口最近一次从已提交事实查询到的待投递数。
    metrics_pending: AtomicU64,
    /// 指标出口最近一次从已提交事实查询到的死信数。
    metrics_dead: AtomicU64,
    /// 持久化指标年龄使用的单调时钟起点，避免系统时间校准造成快照年龄跳变。
    metrics_clock_started: Instant,
    /// 最近一次完整持久化指标快照相对单调时钟起点的毫秒数加一；0 表示尚未成功。
    metrics_last_success_tick: AtomicU64,
    /// 持久化指标刷新失败累计值。
    metrics_refresh_failures: AtomicU64,
    /// 最近一次持久化指标刷新是否失败。
    metrics_refresh_failed: AtomicBool,
    /// 串行化持久化指标查询，并缓存最近一次尝试的时间与结果。
    metrics_refresh: AsyncMutex<OutboxMetricsRefreshGate>,
    /// 分片模式下的 lane 观测状态;未分片时为空。Ready 时一次发布,此后只读。
    lanes: Mutex<Option<Vec<Arc<LaneRuntime>>>>,
}

impl OutboxRuntimeState {
    /// 业务作用：创建开放 UserHook 计划入口、尚未授予 dispatcher 权限的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空的 configuring 状态。
    pub(crate) fn new() -> Self {
        Self {
            pending: Mutex::new(None),
            sealed: AtomicBool::new(false),
            lifecycle: AtomicU8::new(0),
            outbox: OnceLock::new(),
            rounds: AtomicU64::new(0),
            published: AtomicU64::new(0),
            failed_rounds: AtomicU64::new(0),
            retention_rounds: AtomicU64::new(0),
            retention_archived: AtomicU64::new(0),
            retention_deleted_dispatched: AtomicU64::new(0),
            retention_deleted_dead: AtomicU64::new(0),
            retention_failed_rounds: AtomicU64::new(0),
            retention_claim_contended: AtomicU64::new(0),
            retention_budget_exhausted: AtomicU64::new(0),
            retention_oldest_candidate_age_ms: AtomicU64::new(0),
            retention_lock_contention: AtomicU64::new(0),
            retention_commit_uncertain: AtomicU64::new(0),
            retention_interval_ms: AtomicU64::new(0),
            retention_last_success_ms: AtomicU64::new(0),
            metrics_pending: AtomicU64::new(0),
            metrics_dead: AtomicU64::new(0),
            metrics_clock_started: Instant::now(),
            metrics_last_success_tick: AtomicU64::new(0),
            metrics_refresh_failures: AtomicU64::new(0),
            metrics_refresh_failed: AtomicBool::new(false),
            metrics_refresh: AsyncMutex::new(OutboxMetricsRefreshGate::new()),
            lanes: Mutex::new(None),
        }
    }

    /// 业务作用：Ready 时一次性发布 lane 观测状态,供快照与指标读取。
    ///
    /// 参数说明：
    /// - `lanes`：按计划冻结的 lane 运行状态集合。
    ///
    /// 返回：无返回值。
    fn publish_lanes(&self, lanes: Vec<Arc<LaneRuntime>>) {
        *self
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(lanes);
    }

    /// 业务作用：读取 lane 观测状态快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分片模式返回 lane 集合;未分片返回空。
    pub(crate) fn lanes(&self) -> Vec<Arc<LaneRuntime>> {
        self.lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_default()
    }

    /// 业务作用：计算最近完整持久化指标快照的非负年龄，供出口显式标识缓存边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：距最近成功刷新的秒数；尚无成功快照时返回 0，并由 refresh_failed 指标共同判定。
    fn metrics_snapshot_age_seconds(&self) -> f64 {
        let last_success_tick = self.metrics_last_success_tick.load(Ordering::Acquire);
        if last_success_tick == 0 {
            return 0.0;
        }
        let now_ms =
            u64::try_from(self.metrics_clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        now_ms.saturating_sub(last_success_tick - 1) as f64 / 1_000.0
    }

    /// 业务作用：线性化接收唯一发布计划，禁止多个 publisher 竞争同一数据库顺序通道。
    ///
    /// 参数说明：
    /// - `plan`：已冻结发布端和毒丸策略的完整计划。
    ///
    /// 返回：首次提交成功；Ready 已封口或重复提交返回 UserHook 错误。
    pub(crate) fn configure(&self, plan: OutboxApplicationPlan) -> ApplicationResult<()> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox configuration is sealed before Ready",
            ));
        }
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.sealed.load(Ordering::Acquire) || pending.is_some() {
            return Err(outbox_error(
                ApplicationPhase::UserHook,
                "outbox plan can be configured only once",
            ));
        }
        *pending = Some(plan);
        Ok(())
    }

    /// 业务作用：在 Ready 入口永久封口并移交发布计划唯一所有权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已提交计划；缺失时拒绝启动空 dispatcher。
    fn take_plan(&self) -> ApplicationResult<OutboxApplicationPlan> {
        self.sealed.store(true, Ordering::Release);
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox component requires a publishing plan during the application startup hook",
                )
            })
    }

    /// 业务作用：在数据库探针成功且停机动作已压栈后发布运行权限。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次发布成功；内部重复发布返回 Ready 错误。
    fn publish_ready(&self) -> ApplicationResult<()> {
        self.lifecycle
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox runtime was already published",
                )
            })
    }

    /// 业务作用：在 Ready 前一次性发布受管 Outbox 的 datasource 绑定。
    ///
    /// 参数说明：`outbox` 是已经过资源存在性与数据库探针复验的句柄。
    ///
    /// 返回：首次发布成功；重复发布返回 Ready 阶段错误。
    fn publish_outbox(&self, outbox: ManagedOutbox) -> ApplicationResult<()> {
        self.outbox.set(outbox).map_err(|_| {
            outbox_error(
                ApplicationPhase::Ready,
                "outbox datasource binding was already published",
            )
        })
    }

    /// 业务作用：取得 Ready 时冻结的 Outbox datasource 句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已发布时返回同源句柄 clone；未发布时返回阶段错误。
    fn outbox(&self) -> ApplicationResult<ManagedOutbox> {
        self.outbox.get().cloned().ok_or_else(|| {
            outbox_error(
                ApplicationPhase::Ready,
                "outbox datasource binding has not been published",
            )
        })
    }

    /// 业务作用：确认 Outbox 能力仍处于 Ready，阻止停机后继续发起管理读取。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 时成功；尚未发布或已停止时返回阶段错误。
    pub(crate) fn ensure_ready(&self) -> ApplicationResult<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            1 => Ok(()),
            2 => Err(outbox_error(
                ApplicationPhase::Stopping,
                "outbox runtime is stopping",
            )),
            _ => Err(outbox_error(
                ApplicationPhase::Ready,
                "outbox runtime has not passed its Ready gate",
            )),
        }
    }

    /// 业务作用：发布停机保护态，使能力入口与 dispatcher 在下一轮边界拒绝新投递。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；Release 发布保证任务随后观察到停止状态，应用生命周期通知负责唤醒等待任务。
    fn stop(&self) {
        self.lifecycle.store(2, Ordering::Release);
    }

    /// 业务作用：生成进程级投递计数的一致近似快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：各字段独立单调，不携带事件或发布端身份。
    fn snapshot(&self) -> OutboxSnapshot {
        OutboxSnapshot {
            rounds: self.rounds.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            failed_rounds: self.failed_rounds.load(Ordering::Relaxed),
            retention_rounds: self.retention_rounds.load(Ordering::Relaxed),
            retention_archived: self.retention_archived.load(Ordering::Relaxed),
            retention_deleted_dispatched: self.retention_deleted_dispatched.load(Ordering::Relaxed),
            retention_deleted_dead: self.retention_deleted_dead.load(Ordering::Relaxed),
            retention_failed_rounds: self.retention_failed_rounds.load(Ordering::Relaxed),
            retention_claim_contended: self.retention_claim_contended.load(Ordering::Relaxed),
            retention_budget_exhausted: self.retention_budget_exhausted.load(Ordering::Relaxed),
            retention_oldest_candidate_age_ms: self
                .retention_oldest_candidate_age_ms
                .load(Ordering::Relaxed),
            retention_lock_contention: self.retention_lock_contention.load(Ordering::Relaxed),
            retention_commit_uncertain: self.retention_commit_uncertain.load(Ordering::Relaxed),
            retention_interval_ms: self.retention_interval_ms.load(Ordering::Relaxed),
            retention_last_success_ms: self.retention_last_success_ms.load(Ordering::Relaxed),
        }
    }
}

/// 持久化指标刷新门禁；同一进程的 Prometheus 与 OTLP 出口共享该状态。
struct OutboxMetricsRefreshGate {
    last_attempt: Option<Instant>,
    last_succeeded: bool,
}

impl OutboxMetricsRefreshGate {
    /// 业务作用：创建尚未访问数据库的刷新门禁，确保第一次指标请求立即取得持久化事实。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：没有历史尝试且未发布成功结果的门禁。
    fn new() -> Self {
        Self {
            last_attempt: None,
            last_succeeded: false,
        }
    }
}

/// 将 Outbox 已提交事实与进程计数接入唯一指标目录的兼容源。
struct OutboxMetricsSource {
    state: Arc<OutboxRuntimeState>,
}

impl nametrics_core::LegacyMetricsSource for OutboxMetricsSource {
    /// 业务作用：返回 Outbox 固定 family 与有界 channel label 目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期登记并用于结构化样本校验的全部 Outbox descriptor。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &OUTBOX_DESCRIPTORS
    }

    /// 业务作用：读取 Outbox 当前进程计数与最近持久化快照，不清零任何值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终为 `Some`，其中是进程计数、完整持久化缓存与冻结 channel lane 的当前结构化样本。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let snapshot = self.state.snapshot();
        let mut samples = vec![
            outbox_counter(OUTBOX_ROUNDS.name, snapshot.rounds),
            outbox_counter(OUTBOX_PUBLISHED.name, snapshot.published),
            outbox_counter(OUTBOX_FAILED_ROUNDS.name, snapshot.failed_rounds),
            outbox_gauge(
                OUTBOX_PENDING.name,
                self.state.metrics_pending.load(Ordering::Relaxed),
            ),
            outbox_gauge(
                OUTBOX_DEAD.name,
                self.state.metrics_dead.load(Ordering::Relaxed),
            ),
            outbox_float_gauge(
                OUTBOX_METRICS_SNAPSHOT_AGE.name,
                self.state.metrics_snapshot_age_seconds(),
            ),
            outbox_gauge(
                OUTBOX_METRICS_REFRESH_FAILED.name,
                u64::from(self.state.metrics_refresh_failed.load(Ordering::Acquire)),
            ),
            outbox_counter(
                OUTBOX_METRICS_REFRESH_FAILURES.name,
                self.state.metrics_refresh_failures.load(Ordering::Relaxed),
            ),
            outbox_counter(
                OUTBOX_QUOTA_REJECTIONS.name,
                quota_rejections_total(self.state.outbox.get()),
            ),
            outbox_counter(OUTBOX_RETENTION_ROUNDS.name, snapshot.retention_rounds),
            outbox_counter(OUTBOX_RETENTION_ARCHIVED.name, snapshot.retention_archived),
            outbox_counter(
                OUTBOX_RETENTION_DELETED_DISPATCHED.name,
                snapshot.retention_deleted_dispatched,
            ),
            outbox_counter(
                OUTBOX_RETENTION_DELETED_DEAD.name,
                snapshot.retention_deleted_dead,
            ),
            outbox_counter(
                OUTBOX_RETENTION_FAILED_ROUNDS.name,
                snapshot.retention_failed_rounds,
            ),
            outbox_counter(
                OUTBOX_RETENTION_CLAIM_CONTENDED.name,
                snapshot.retention_claim_contended,
            ),
            outbox_counter(
                OUTBOX_RETENTION_BUDGET_EXHAUSTED.name,
                snapshot.retention_budget_exhausted,
            ),
            outbox_gauge(
                OUTBOX_RETENTION_OLDEST_AGE.name,
                snapshot.retention_oldest_candidate_age_ms,
            ),
            outbox_counter(
                OUTBOX_RETENTION_LOCK_CONTENTION.name,
                snapshot.retention_lock_contention,
            ),
            outbox_counter(
                OUTBOX_RETENTION_COMMIT_UNCERTAIN.name,
                snapshot.retention_commit_uncertain,
            ),
            outbox_gauge(
                OUTBOX_RETENTION_INTERVAL.name,
                snapshot.retention_interval_ms,
            ),
            outbox_gauge(
                OUTBOX_RETENTION_LAST_SUCCESS.name,
                snapshot.retention_last_success_ms,
            ),
        ];
        for lane in self.state.lanes() {
            let labels = vec![("channel", lane.channel_name().to_owned())];
            samples.push(nametrics_core::MetricSample {
                name: OUTBOX_LANE_PUBLISHED.name,
                labels: labels.clone(),
                value: nametrics_core::MetricValue::Counter(lane.published_total()),
            });
            samples.push(nametrics_core::MetricSample {
                name: OUTBOX_LANE_FAILED.name,
                labels: labels.clone(),
                value: nametrics_core::MetricValue::Counter(lane.failed_rounds_total()),
            });
            samples.push(nametrics_core::MetricSample {
                name: OUTBOX_LANE_HEALTHY.name,
                labels: labels.clone(),
                value: nametrics_core::MetricValue::Gauge(f64::from(lane.is_healthy())),
            });
            samples.push(nametrics_core::MetricSample {
                name: OUTBOX_LANE_PENDING.name,
                labels,
                value: nametrics_core::MetricValue::Gauge(lane.pending_count() as f64),
            });
        }
        Some(samples)
    }

    /// 业务作用：保留旧 trait 入口；统一 hub 对本源始终使用非空结构化快照渲染。
    ///
    /// 参数说明：
    /// - `_output`: 兼容 trait 的文本缓冲区；本源不直接写入。
    ///
    /// 返回：无；文本出口由统一 hub 使用 `snapshot()` 结果生成。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：构造无 label 的 Outbox 单调计数样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `value`: 当前累计值。
///
/// 返回：不含业务身份的 counter 样本。
fn outbox_counter(name: &'static str, value: u64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Counter(value),
    }
}

/// 业务作用：构造无 label 的 Outbox 当前状态样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `value`: 最近观测值。
///
/// 返回：不含业务身份的 gauge 样本。
fn outbox_gauge(name: &'static str, value: u64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Gauge(value as f64),
    }
}

/// 业务作用：构造保留小数精度的无 label Outbox gauge 样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `value`: 当前浮点状态值。
///
/// 返回：不含业务身份的 gauge 样本。
fn outbox_float_gauge(name: &'static str, value: f64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Gauge(value),
    }
}

/// 业务作用：按独立低频窗口从 Outbox 已提交持久化事实刷新 pending/dead gauge。
///
/// Prometheus 与 OTLP 共享一次串行刷新；窗口内直接复用缓存，避免导出周期和 lane 数量相乘形成
/// 数据库查询风暴。全部查询成功后才一次发布新值，任一查询失败都保留上一份完整快照。
///
/// 参数说明：
/// - `state`: 已冻结 lane 集合与快照原子单元。
///
/// 返回：缓存仍有效或全局和全部 lane 查询成功时返回成功；最近一次刷新失败且仍在短重试窗口时
/// 返回稳定错误，数据库失败时保留上一份完整快照并更新失败观测值。
pub(crate) async fn refresh_metrics(state: &Arc<OutboxRuntimeState>) -> ApplicationResult<()> {
    refresh_metrics_with_policy(state, false).await
}

/// 业务作用：停机前忽略刷新窗口并读取最后一份持久化事实，供数据库释放后的最终导出使用。
///
/// 参数说明：
/// - `state`: 已冻结 lane 集合与快照原子单元。
///
/// 返回：完整查询并发布成功时返回成功；数据库失败时保留上一份完整快照并返回统一错误。
async fn force_refresh_metrics(state: &Arc<OutboxRuntimeState>) -> ApplicationResult<()> {
    refresh_metrics_with_policy(state, true).await
}

/// 业务作用：串行裁决持久化指标应复用缓存还是访问数据库，并原子发布完整查询结果。
///
/// 参数说明：
/// - `state`: 已冻结 lane 集合、缓存与刷新门禁。
/// - `force`: 为真时忽略冷却窗口，用于依赖释放前的最终快照。
///
/// 返回：缓存可复用或新查询完整成功时返回成功；冷却中的失败状态或数据库查询失败返回统一错误。
async fn refresh_metrics_with_policy(
    state: &Arc<OutboxRuntimeState>,
    force: bool,
) -> ApplicationResult<()> {
    let mut gate = state.metrics_refresh.lock().await;
    let refresh_interval = if gate.last_succeeded {
        OUTBOX_METRICS_REFRESH_INTERVAL
    } else {
        OUTBOX_METRICS_FAILURE_RETRY_INTERVAL
    };
    if !force
        && gate
            .last_attempt
            .is_some_and(|last| last.elapsed() < refresh_interval)
    {
        return if gate.last_succeeded {
            Ok(())
        } else {
            Err(outbox_error(
                ApplicationPhase::Running,
                "outbox metrics snapshot refresh remains unavailable during the retry interval",
            ))
        };
    }

    let result = query_and_publish_metrics(state).await;
    gate.last_attempt = Some(Instant::now());
    gate.last_succeeded = result.is_ok();
    if result.is_ok() {
        // 完整查询发布后再清除失败态，确保 age=0 与 last-good 数值来自同一成功批次。
        let success_tick = u64::try_from(state.metrics_clock_started.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        state
            .metrics_last_success_tick
            .store(success_tick, Ordering::Release);
        state.metrics_refresh_failed.store(false, Ordering::Release);
    } else {
        state
            .metrics_refresh_failures
            .fetch_add(1, Ordering::Relaxed);
        state.metrics_refresh_failed.store(true, Ordering::Release);
    }
    result
}

/// 业务作用：查询全局与各 lane 的持久化计数，并仅在全部成功后发布同一批次快照。
///
/// 参数说明：
/// - `state`: 已冻结 lane 集合与待更新的原子指标单元。
///
/// 返回：全部数据库计数成功时发布并返回成功；任一查询失败时不改变既有缓存。
async fn query_and_publish_metrics(state: &Arc<OutboxRuntimeState>) -> ApplicationResult<()> {
    let outbox = state.outbox()?;
    let (pending, dead) =
        tokio::try_join!(outbox.pending_count(), outbox.dead_count()).map_err(|error| {
            outbox_source_error(
                ApplicationPhase::Running,
                "outbox metrics snapshot query failed",
                error,
            )
        })?;
    let lanes = state.lanes();
    let mut lane_pending = Vec::with_capacity(lanes.len());
    for lane in &lanes {
        let value = outbox
            .pending_count_channel(lane.channel_name())
            .await
            .map_err(|error| {
                outbox_source_error(
                    ApplicationPhase::Running,
                    "outbox lane metrics snapshot query failed",
                    error,
                )
            })?;
        lane_pending.push(value);
    }
    state.metrics_pending.store(pending, Ordering::Relaxed);
    state.metrics_dead.store(dead, Ordering::Relaxed);
    for (lane, value) in lanes.iter().zip(lane_pending) {
        lane.pending.store(value, Ordering::Relaxed);
    }
    Ok(())
}

/// 业务作用：在 Application 生命周期内建立、监督并逆序关闭 Outbox dispatcher 与清理循环。
pub(crate) struct OutboxComponent {
    settings: Option<OutboxSettings>,
    contributor: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl OutboxComponent {
    /// 业务作用：创建尚未读取配置和发布计划的 Outbox 生命周期组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由 Runner 独占的空组件。
    pub(crate) fn new() -> Self {
        Self {
            settings: None,
            contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for OutboxComponent {
    /// 业务作用：返回 Outbox 生命周期的稳定组件身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`ComponentId::Outbox`。
    fn id(&self) -> ComponentId {
        ComponentId::Outbox
    }

    /// 业务作用：声明 MySQL dispatcher 必须晚于数据库组件启动。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含 `ComponentId::Db` 的静态依赖。
    fn dependencies(&self) -> &'static [ComponentId] {
        &[ComponentId::Db]
    }

    /// 业务作用：校验轮询预算并登记关键 readiness 贡献项。
    ///
    /// 参数说明：
    /// - `context`：提供最终配置与 readiness 注册入口。
    ///
    /// 返回：配置合法且贡献项登记成功时完成；否则阻止进入 UserHook。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let settings = read_outbox_settings(context.application(), ApplicationPhase::Start)?;
            let contributor = context.application().register_readiness(
                ComponentId::Outbox,
                Arc::<str>::from("outbox:dispatcher"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: settings.failure_threshold,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?;
            self.settings = Some(settings);
            self.contributor = Some(contributor);
            Ok(())
        })
    }

    /// 业务作用：封口发布计划、验证数据库通道并把持续 dispatcher 移交监督器。
    ///
    /// 参数说明：
    /// - `context`：提供 Application、启动预算和反向停机 action 登记入口。
    ///
    /// 返回：计划和数据库均可用时发布 Ready；缺失计划、探针失败或超时拒绝启动。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let state = application.outbox_runtime();
            let mut plan = state.take_plan()?;
            let settings = self.settings.clone().ok_or_else(|| {
                outbox_error(ApplicationPhase::Ready, "outbox settings are missing")
            })?;
            // catalog 是 datasource 与 driver 的唯一权威；先完成类型化 getter 复验，禁止 store 回退到其它默认库。
            let outbox = create_managed_outbox(&application, &settings.datasource_ref).await?;
            let probe_budget = context
                .remaining()
                .min(Duration::from_millis(settings.operation_timeout_ms));
            tokio::time::timeout(probe_budget, outbox.pending_count())
                .await
                .map_err(|error| {
                    outbox_source_error(
                        ApplicationPhase::Ready,
                        "outbox database probe timed out",
                        error,
                    )
                })?
                .map_err(|error| {
                    outbox_source_error(
                        ApplicationPhase::Ready,
                        "outbox database probe failed",
                        error,
                    )
                })?;

            let contributor = self.contributor.as_ref().cloned().ok_or_else(|| {
                outbox_error(
                    ApplicationPhase::Ready,
                    "outbox readiness contributor is missing",
                )
            })?;
            // 通用结构合同:受信租户归因列是每笔 Outbox 写入都要落的列,其宽度上界由公开
            // 身份合同定义,与是否启用租户配额无关。存量窄列必须在 Ready 拒绝,否则
            // 191..=256 字节的合法租户要到第一笔业务写入才被数据库拒绝。
            verify_outbox_schema(&outbox, &settings.datasource_ref, false)
                .await
                .map_err(|error| {
                    outbox_source_error(
                        ApplicationPhase::Ready,
                        "outbox event schema is incomplete",
                        error,
                    )
                })?;
            // 配额 opt-in:Ready 时安装进程级冻结配额——安装失败(与既有配额冲突)
            // 必须拒绝 Ready,否则写侧的预留口径会在同进程内分裂。
            if let Some(quotas) = plan.tenant_quotas.take() {
                install_tenant_quotas(outbox.backend, quotas).map_err(|error| {
                    outbox_source_error(
                        ApplicationPhase::Ready,
                        "outbox tenant quotas could not be installed",
                        error,
                    )
                })?;
                // 配额依赖 tenant 归因列与账本表;迁移漏跑必须在 Ready 暴露,否则
                // 第一轮投递才失败——那时业务写入已经在按配额受理了。
                verify_outbox_schema(&outbox, &settings.datasource_ref, true)
                    .await
                    .map_err(|error| {
                        outbox_source_error(
                            ApplicationPhase::Ready,
                            "outbox tenant quota schema is incomplete",
                            error,
                        )
                    })?;
            }
            // 分片 opt-in:Ready 时安装进程级冻结路由——安装失败(与既有路由冲突)
            // 必须拒绝 Ready,不能带着口径分裂的写侧继续启动。
            let lanes = match plan.channels.take() {
                Some(channel_plan) => {
                    install_channel_routes(outbox.backend, channel_plan.routes.clone()).map_err(
                        |error| {
                            outbox_source_error(
                                ApplicationPhase::Ready,
                                "outbox channel routes could not be installed",
                                error,
                            )
                        },
                    )?;
                    let lanes: Vec<Arc<LaneRuntime>> = channel_plan
                        .lanes
                        .iter()
                        .map(|channel| {
                            Arc::new(LaneRuntime {
                                channel: channel.clone(),
                                published: AtomicU64::new(0),
                                failed_rounds: AtomicU64::new(0),
                                healthy: AtomicBool::new(true),
                                pending: AtomicU64::new(0),
                            })
                        })
                        .collect();
                    state.publish_lanes(lanes.clone());
                    Some(lanes)
                }
                None => None,
            };

            // 指标源在 lane 计划冻结后注册,最坏序列数因此可精确承诺:
            // 21 个无 label family 各一条 + 每个冻结 channel lane 四条。
            let worst_case_series = OUTBOX_FIXED_METRIC_SERIES.saturating_add(
                lanes
                    .as_ref()
                    .map_or(0, |lanes| lanes.len())
                    .saturating_mul(OUTBOX_LANE_METRIC_SERIES),
            );
            context
                .application()
                .metrics_hub()
                .register_legacy_source_reserved(
                    Arc::new(OutboxMetricsSource {
                        state: Arc::clone(&state),
                    }),
                    worst_case_series,
                )
                .map_err(|error| match error {
                    nametrics_core::MetricSourceRegistrationError::Conflict(conflict) => {
                        outbox_error(
                            ApplicationPhase::Ready,
                            format!(
                                "outbox metric descriptor `{}` conflicts with an existing registration",
                                conflict.name
                            ),
                        )
                    }
                    nametrics_core::MetricSourceRegistrationError::SeriesBudgetExceeded => {
                        outbox_error(
                            ApplicationPhase::Ready,
                            "outbox metric series reservation exceeds the process limit",
                        )
                    }
                })?;

            state.publish_outbox(outbox.clone())?;
            // 停机保护必须先于权限发布入栈；否则后续 Ready 失败时 dispatcher 状态可能游离于
            // Application 反向清理之外，并在 transport 或数据库开始释放后继续投递。
            context.activate(Box::new(OutboxShutdown {
                state: Arc::clone(&state),
            }));
            state.publish_ready()?;
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());

            // 保留清理与 dispatcher 在同一关键任务内并行:清理停摆只降级观测面,绝不
            // 反向终止投递;全部循环只在应用停机边界退出。启用分片时未分片 dispatcher
            // 被按 lane 循环整体取代,同进程不存在两种 claim 并行。
            let retention = plan.retention.take();
            self.critical_task = Some(Box::pin(async move {
                let dispatch: ApplicationFuture<'static> = match lanes {
                    Some(lanes) => {
                        // 每 lane 一个受监督子任务:任一 lane 循环异常终止都会把整个
                        // 关键任务失败上抛,不允许"半死"的 dispatcher 伪装健康。
                        let mut lane_tasks = tokio::task::JoinSet::new();
                        for lane in lanes {
                            lane_tasks.spawn(run_lane_dispatch_loop(
                                application.clone(),
                                Arc::clone(&state),
                                outbox.clone(),
                                Arc::clone(&plan.publisher),
                                plan.poison_policy,
                                settings.clone(),
                                contributor.clone(),
                                lane,
                            ));
                        }
                        Box::pin(async move {
                            while let Some(joined) = lane_tasks.join_next().await {
                                joined.map_err(|error| {
                                    outbox_source_error(
                                        ApplicationPhase::Running,
                                        "outbox lane dispatcher terminated abnormally",
                                        error,
                                    )
                                })??;
                            }
                            Ok(())
                        })
                    }
                    None => Box::pin(run_dispatch_loop(
                        application.clone(),
                        Arc::clone(&state),
                        outbox.clone(),
                        plan,
                        settings,
                        contributor,
                    )),
                };
                match retention {
                    Some(retention_plan) => {
                        let retention =
                            run_retention_loop(application, state, outbox, retention_plan);
                        let (dispatch_outcome, retention_outcome) =
                            tokio::join!(dispatch, retention);
                        dispatch_outcome.and(retention_outcome)
                    }
                    None => dispatch.await,
                }
            }));
            Ok(())
        })
    }

    /// 业务作用：把唯一 Outbox dispatcher 移交 Runner 作为关键任务监督。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次调用返回任务；重复调用返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("outbox-dispatcher", task))
    }
}

/// 业务作用：在数据库释放前刷新最终事实并关闭新的 Outbox 投递轮次。
struct OutboxShutdown {
    state: Arc<OutboxRuntimeState>,
}

impl ShutdownAction for OutboxShutdown {
    /// 业务作用：返回不含 publisher 或数据库身份的稳定清理动作名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定 Outbox 停机标签。
    fn label(&self) -> &'static str {
        "outbox-runtime"
    }

    /// 业务作用：在数据库释放前保存最后一份完整持久化事实，再关闭新投递轮次。
    ///
    /// 参数说明：
    /// - `_context`：Runner 共享停机预算；本动作不创建独立超时。
    ///
    /// 返回：即使最终指标读取失败也发布保护态并成功；失败只保留上一份完整快照，不能阻断业务停机。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            // 先在数据库仍可用时发布最终缓存，使稍后执行的 telemetry flush 不依赖已释放连接。
            if let Err(error) = force_refresh_metrics(&self.state).await {
                tracing::warn!(
                    "outbox final metric refresh failed; retaining the last complete snapshot: {error}"
                );
            }
            self.state.stop();
            Ok(())
        })
    }
}

/// 业务作用：持续执行有界 Outbox 投递轮次，并把发布和存储健康映射到 readiness。
///
/// 参数说明：
/// - `application`：提供统一进程停机状态。
/// - `state`：提供 Outbox 权限状态和低基数计数。
/// - `outbox`：Ready 时冻结且绑定 `datasource_ref` 的持久句柄。
/// - `plan`：冻结的 publisher 与毒丸策略。
/// - `settings`：轮询、退避、单轮预算和批次上限。
/// - `contributor`：Outbox 独占的 readiness 贡献项。
///
/// 返回：应用停机时正常退出；循环本身不因瞬时下游或数据库故障丢失持久化事件。
async fn run_dispatch_loop(
    application: Application,
    state: Arc<OutboxRuntimeState>,
    outbox: ManagedOutbox,
    plan: OutboxApplicationPlan,
    settings: OutboxSettings,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    let mut application_states = application.subscribe_state();
    let mut committed_appends = outbox.store.subscribe_committed_appends();
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                wait_for_next_round(
                    &mut application_states,
                    &mut committed_appends,
                    settings.poll_interval_ms,
                    false,
                )
                .await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let operation = async {
            match plan.poison_policy {
                OutboxPoisonPolicy::Block => {
                    outbox
                        .dispatch_batch(plan.publisher.as_ref(), settings.batch_size)
                        .await
                }
                OutboxPoisonPolicy::DeadLetter { max_attempts } => {
                    outbox
                        .dispatch_batch_with_dlt(
                            plan.publisher.as_ref(),
                            settings.batch_size,
                            max_attempts,
                        )
                        .await
                }
            }
        };
        let outcome = tokio::time::timeout(
            Duration::from_millis(settings.operation_timeout_ms),
            operation,
        )
        .await;
        state.rounds.fetch_add(1, Ordering::Relaxed);
        let healthy = match outcome {
            Ok(Ok(report)) => {
                state
                    .published
                    .fetch_add(report.published as u64, Ordering::Relaxed);
                report.first_error.is_none()
            }
            Ok(Err(_)) | Err(_) => false,
        };
        let delay = if healthy {
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            settings.poll_interval_ms
        } else {
            state.failed_rounds.fetch_add(1, Ordering::Relaxed);
            // 未确认的发布必须留在数据库并退避；禁止为保持 Ready 而跳过首个失败事件。
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            settings.error_backoff_ms
        };
        // 健康轮次允许提交信号提前结束等待，使步骤预算不再承担固定轮询相位；失败轮次保持完整
        // 退避，避免持续业务写在下游故障时绕过重试预算。数据库轮询仍覆盖跨进程与崩溃恢复。
        wait_for_next_round(
            &mut application_states,
            &mut committed_appends,
            delay,
            healthy,
        )
        .await;
    }
}

/// 业务作用：持续执行单个 lane 的有界投递轮次——lane 独占 claim,毒丸停摆半径收窄到
/// 本 lane,其余 lane 不受影响。
///
/// readiness 口径:整体 contributor 在"全部 lane 健康"时 Ready,任一 lane 失败时
/// NotReady;lane 级健康另经 `napp_outbox_lane_healthy{channel=...}` 区分"某个领域
/// 停摆"与"整个 dispatcher 退出"。
///
/// 参数说明：
/// - `application`：提供统一进程停机状态。
/// - `state`：共享轮次计数(rounds/published/failed_rounds 聚合全部 lane)。
/// - `outbox`：Ready 时冻结且绑定 `datasource_ref` 的持久句柄。
/// - `publisher`：下游发布端。
/// - `poison_policy`：本 lane 的毒丸策略。
/// - `settings`：轮询、退避、单轮预算和批次上限。
/// - `contributor`：Outbox 整体 readiness 贡献项(各 lane 共享)。
/// - `lane`：本 lane 的观测状态。
///
/// 返回：应用停机时正常退出。
#[allow(clippy::too_many_arguments)]
async fn run_lane_dispatch_loop(
    application: Application,
    state: Arc<OutboxRuntimeState>,
    outbox: ManagedOutbox,
    publisher: Arc<dyn OutboxPublisher + Send + Sync>,
    poison_policy: OutboxPoisonPolicy,
    settings: OutboxSettings,
    contributor: ReadinessContributor,
    lane: Arc<LaneRuntime>,
) -> ApplicationResult<()> {
    let mut application_states = application.subscribe_state();
    let mut committed_appends = outbox.store.subscribe_committed_appends();
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                wait_for_next_round(
                    &mut application_states,
                    &mut committed_appends,
                    settings.poll_interval_ms,
                    false,
                )
                .await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let operation = async {
            match poison_policy {
                OutboxPoisonPolicy::Block => {
                    outbox
                        .dispatch_batch_channel(
                            publisher.as_ref(),
                            &lane.channel,
                            settings.batch_size,
                        )
                        .await
                }
                OutboxPoisonPolicy::DeadLetter { max_attempts } => {
                    outbox
                        .dispatch_batch_channel_with_dlt(
                            publisher.as_ref(),
                            &lane.channel,
                            settings.batch_size,
                            max_attempts,
                        )
                        .await
                }
            }
        };
        let outcome = tokio::time::timeout(
            Duration::from_millis(settings.operation_timeout_ms),
            operation,
        )
        .await;
        state.rounds.fetch_add(1, Ordering::Relaxed);
        let healthy = match outcome {
            Ok(Ok(report)) => {
                state
                    .published
                    .fetch_add(report.published as u64, Ordering::Relaxed);
                lane.published
                    .fetch_add(report.published as u64, Ordering::Relaxed);
                report.first_error.is_none()
            }
            Ok(Err(_)) | Err(_) => false,
        };
        lane.healthy.store(healthy, Ordering::Relaxed);
        let delay = if healthy {
            // 整体 readiness 只有在全部 lane 健康时回到 Ready:单 lane 恢复不能掩盖
            // 其它领域仍在停摆的事实。
            let all_healthy = state
                .lanes()
                .iter()
                .all(|entry| entry.healthy.load(Ordering::Relaxed));
            if all_healthy {
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            }
            settings.poll_interval_ms
        } else {
            state.failed_rounds.fetch_add(1, Ordering::Relaxed);
            lane.failed_rounds.fetch_add(1, Ordering::Relaxed);
            // 本 lane 未确认的发布留在数据库并退避;其余 lane 继续独立推进——这正是
            // 停摆半径从整表收窄到单 lane 的行为面。
            contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            settings.error_backoff_ms
        };
        wait_for_next_round(
            &mut application_states,
            &mut committed_appends,
            delay,
            healthy,
        )
        .await;
    }
}

/// 业务作用：按批准策略周期执行保留清理轮次，并把结果累计进组件观测面。
///
/// 清理与 dispatcher 完全隔离：独立 retention claim、独立退避；清理失败或长期停摆只
/// 反映在 `retention_failed_rounds` 与"最后成功时刻"上（严格治理 degraded 信号），
/// 绝不改变 dispatcher 的运行与 readiness。停机顺序天然满足"先停新批次"：应用切出
/// Ready 后本循环不再开启新轮。进行中轮次的提交前步骤受单轮时间预算约束；一旦发出
/// COMMIT 就等待数据库明确应答，不再用外层 timeout 取消可能已经生效的删除，随后释放 claim。
///
/// 参数说明：
/// - `application`：提供统一进程停机状态。
/// - `state`：观测计数累计目标。
/// - `outbox`：Ready 时冻结且绑定 `datasource_ref` 的持久句柄。
/// - `plan`：冻结的策略、归档端与轮询间隔。
///
/// 返回：应用停机时正常退出。
async fn run_retention_loop(
    application: Application,
    state: Arc<OutboxRuntimeState>,
    outbox: ManagedOutbox,
    plan: OutboxRetentionPlan,
) -> ApplicationResult<()> {
    // 清理节奏是显式配置的固定间隔(失败不缩短):把它导出为 gauge,锁冲突/失败告警
    // 才能换算成"最长多久没有推进"。
    state
        .retention_interval_ms
        .store(plan.interval_ms, Ordering::Relaxed);
    let mut application_states = application.subscribe_state();
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                return Ok(());
            }
            ApplicationState::Starting | ApplicationState::Ready => {}
        }
        if application.state() == ApplicationState::Ready {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis().min(u128::from(u64::MAX)) as i64)
                .unwrap_or(0);
            // 执行器内部对连接、claim、会话配置、查询、归档和释放逐步应用统一 deadline；
            // 此处不能再包整体 timeout，否则可能在 COMMIT 已发出后丢弃 future，制造
            // “服务端已提交、进程却按超时记账”的不确定窗口。
            let archive = plan
                .archive
                .as_deref()
                .map(|archive| archive as &(dyn OutboxArchive + Sync));
            let outcome = outbox.retention_round(&plan.policy, archive, now_ms).await;
            state.retention_rounds.fetch_add(1, Ordering::Relaxed);
            match outcome {
                Ok(report) => {
                    state
                        .retention_archived
                        .fetch_add(report.archived, Ordering::Relaxed);
                    state
                        .retention_deleted_dispatched
                        .fetch_add(report.deleted_dispatched, Ordering::Relaxed);
                    state
                        .retention_deleted_dead
                        .fetch_add(report.deleted_dead, Ordering::Relaxed);
                    state.retention_oldest_candidate_age_ms.store(
                        report.oldest_candidate_age_ms.unwrap_or(0).max(0) as u64,
                        Ordering::Relaxed,
                    );
                    if report.budget_exhausted {
                        state
                            .retention_budget_exhausted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    if report.claim_contended {
                        state
                            .retention_claim_contended
                            .fetch_add(1, Ordering::Relaxed);
                    } else if !report.budget_exhausted {
                        // "最近成功"只表达"候选处理到完成或确认无候选":预算耗尽的轮
                        // 没有走完候选,刷新它会让长期零进展的清理在监控面持续显示健康。
                        state
                            .retention_last_success_ms
                            .store(now_ms.max(0) as u64, Ordering::Relaxed);
                    }
                }
                Err(error) => {
                    // 锁竞争与归档/存储故障分开计数:行锁冲突是并发治理信号,折叠进
                    // 通用失败会掩盖"谁在和清理抢行"。候选保留等待下一轮;失败不缩短
                    // 间隔,清理不进入忙重试。
                    let lock_contention = match outbox.backend {
                        #[cfg(feature = "outbox")]
                        OutboxBackend::MySql => {
                            error.reason == naoutbox_mysql::RETENTION_LOCK_CONTENTION_REASON
                        }
                        #[cfg(feature = "outbox-pgsql")]
                        OutboxBackend::PostgreSql => {
                            error.reason == naoutbox_pgsql::RETENTION_LOCK_CONTENTION_REASON
                        }
                    };
                    if lock_contention {
                        state
                            .retention_lock_contention
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        let commit_uncertain = match outbox.backend {
                            #[cfg(feature = "outbox")]
                            OutboxBackend::MySql => {
                                error.reason == naoutbox_mysql::RETENTION_COMMIT_UNCERTAIN_REASON
                            }
                            #[cfg(feature = "outbox-pgsql")]
                            OutboxBackend::PostgreSql => {
                                error.reason == naoutbox_pgsql::RETENTION_COMMIT_UNCERTAIN_REASON
                            }
                        };
                        if commit_uncertain {
                            // 提交不确定必须单独告警，不能混入普通连接失败后让运维误以为
                            // 本批确定回滚；下一轮只依赖持久候选事实继续收敛。
                            state
                                .retention_commit_uncertain
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        state
                            .retention_failed_rounds
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        // 间隔等待可被停机边界打断:清理是治理动作,停机时不需要"最后一轮"。
        let interval = Duration::from_millis(plan.interval_ms);
        let _ = tokio::time::timeout(interval, async {
            loop {
                if application_states.changed().await.is_err() {
                    return;
                }
                let state = *application_states.borrow();
                if matches!(
                    state,
                    ApplicationState::Stopping
                        | ApplicationState::Stopped
                        | ApplicationState::Failed
                ) {
                    return;
                }
            }
        })
        .await;
    }
}

/// 业务作用：等待轮询兜底、已提交事件或停机信号中的最早者，控制下一轮 Outbox 投递时机。
///
/// 参数说明：
/// - `application_states`：提供不会丢失最新值的 Ready 与停机边界通知。
/// - `committed_appends`：当前进程数据库提交代际订阅。
/// - `delay_ms`：没有新提交时的轮询或故障退避上限。
/// - `accept_commit_wake`：健康路径允许提交打断等待；故障路径关闭以保持退避。
///
/// 返回：任一条件先满足即返回；不解释数据库事实，调用方下一轮仍以持久化待投递行为准。
async fn wait_for_next_round(
    application_states: &mut watch::Receiver<ApplicationState>,
    committed_appends: &mut watch::Receiver<u64>,
    delay_ms: u64,
    accept_commit_wake: bool,
) {
    let delay = tokio::time::sleep(Duration::from_millis(delay_ms));
    tokio::pin!(delay);
    if accept_commit_wake {
        tokio::select! {
            biased;
            _ = application_states.changed() => {}
            _ = committed_appends.changed() => {}
            _ = &mut delay => {}
        }
    } else {
        tokio::select! {
            biased;
            _ = application_states.changed() => {}
            _ = &mut delay => {}
        }
    }
}

/// 业务作用：读取并校验 Outbox 配置段，缺失时使用保守默认预算。
///
/// 参数说明：
/// - `application`：提供当前不可变配置快照。
/// - `phase`：错误发生的真实生命周期阶段。
///
/// 返回：设置合法时返回强类型值；未知字段或越界预算返回阶段错误。
fn read_outbox_settings(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<OutboxSettings> {
    let snapshot = application.config();
    let settings = match snapshot.value().get("outbox") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            outbox_source_error(phase, "invalid outbox configuration section", error)
        })?,
        None => OutboxSettings::default(),
    };
    validate_settings(&settings, phase)?;
    Ok(settings)
}

/// 业务作用：读取受管 Outbox 冻结的 datasource 引用，供 Saga 组合在启动前复验同源性。
///
/// 参数说明：
/// - `application`: 提供同一份最终配置快照。
/// - `phase`: 读取失败应归属的生命周期阶段。
///
/// 返回：配置合法时返回 canonical datasource qualifier；结构或名称错误时阻止组合启动。
#[cfg(any(feature = "saga", feature = "saga-pgsql"))]
pub(crate) fn datasource_ref(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<String> {
    Ok(read_outbox_settings(application, phase)?.datasource_ref)
}

/// 业务作用：在配置发布前验证 Outbox 段，阻止不可执行预算进入运行快照。
///
/// 参数说明：
/// - `tree`：合并完成但尚未发布的候选配置树。
/// - `phase`：启动或运行期配置校验阶段。
///
/// 返回：段缺失或设置合法时成功；反序列化和预算错误拒绝整帧配置。
pub(crate) fn validate_outbox_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let settings = match tree.get("outbox") {
        Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
            outbox_source_error(phase, "invalid outbox configuration section", error)
        })?,
        None => OutboxSettings::default(),
    };
    validate_settings(&settings, phase)
}

/// 业务作用：约束轮询周期、单轮预算、批次和失败阈值，防止忙循环与无界占用。
///
/// 参数说明：
/// - `settings`：待校验的 Outbox 设置。
/// - `phase`：形成错误时的真实生命周期阶段。
///
/// 返回：全部字段处于封闭范围时成功，否则返回脱敏配置错误。
fn validate_settings(settings: &OutboxSettings, phase: ApplicationPhase) -> ApplicationResult<()> {
    natx_core::DatasourceRef::new(&settings.datasource_ref).map_err(|error| {
        outbox_source_error(
            phase,
            "outbox.datasource_ref is not a canonical datasource qualifier",
            error,
        )
    })?;
    for value in [
        settings.poll_interval_ms,
        settings.error_backoff_ms,
        settings.operation_timeout_ms,
    ] {
        if !(10..=MAX_INTERVAL_MS).contains(&value) {
            return Err(outbox_error(
                phase,
                "outbox interval is outside the supported range",
            ));
        }
    }
    if !(1..=MAX_BATCH_SIZE).contains(&settings.batch_size) {
        return Err(outbox_error(
            phase,
            "outbox batch size is outside the supported range",
        ));
    }
    if !(1..=MAX_FAILURE_THRESHOLD).contains(&settings.failure_threshold) {
        return Err(outbox_error(
            phase,
            "outbox failure threshold is outside the supported range",
        ));
    }
    Ok(())
}

/// 业务作用：创建不含配置值、事件内容或下游身份的 Outbox 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：稳定脱敏摘要。
///
/// 返回：归因到 Outbox 组件的统一错误。
fn outbox_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Outbox, phase, message)
}

/// 业务作用：创建保留诊断链、公开摘要保持脱敏的 Outbox 生命周期错误。
///
/// 参数说明：
/// - `phase`：失败所属生命周期阶段。
/// - `message`：稳定脱敏摘要。
/// - `source`：只进入统一诊断链的底层错误。
///
/// 返回：归因到 Outbox 组件的统一错误。
fn outbox_source_error(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Outbox, phase, message, source)
}
