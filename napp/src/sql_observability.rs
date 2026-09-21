//! Mapper、连接池与离散通知的统一启动装配。
//!
//! 在数据库外部动作前冻结方法目录、配置与系列预算；UserHook 即可使用原子指标和有界队列。
//! Pool 仅在指标抓取时读取，通知仅在 Ready 后投递，二者均不参与数据库 readiness 裁决。

use crate::sql_notifications::{NotificationRuntime, NotificationsConfig};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationMode,
    ApplicationPhase, ApplicationResult, ApplicationState, ComponentId, PrepareContext,
    ReadyContext, ShutdownAction, ShutdownContext, StartContext,
};
use namapper_core::observability::{
    config::{EffectivePolicy, LogLevel, SqlConfig, WaitPolicy},
    MAPPER_METHOD_META,
};
use nametrics_core::{LegacyMetricsSource, MetricDescriptor, MetricSample};
use nanotify_core::{AlertRoute, NotificationIdentity, NotificationProducer, Notify};
#[cfg(any(feature = "db", feature = "db-pgsql"))]
use natx_core::observability::StatementLogging;
use natx_core::{
    observability::{
        self as connections, ConnectionMetricsSource, ConnectionPurpose, PoolMetricsSource,
        WaitLogLevel, WaitObservationPolicy,
    },
    DatabaseDriver,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct DatasourceObservation {
    driver: Option<DatabaseDriver>,
    acquire_timeout_ms: Option<u64>,
}

impl DatasourceObservation {
    /// 业务作用：为显式延后建池的 default 名额预占有限后端候选，不猜测业务将移交的 Pool 类型。
    /// 参数说明：无。
    /// 返回：已知后端返回单项；双后端延后模式固定返回两个候选。
    fn drivers(&self) -> impl Iterator<Item = DatabaseDriver> + '_ {
        [DatabaseDriver::MySql, DatabaseDriver::PostgreSql]
            .into_iter()
            .filter(|driver| self.driver.is_none_or(|known| known == *driver))
    }
}

/// 对业务只公开冻结的有效配置和 custom provider 注册，不暴露连接 URL 或凭据。
pub struct SqlObservabilityRuntime {
    settings: SqlConfig,
    catalog: BTreeMap<String, DatasourceObservation>,
    notifications: Arc<NotificationRuntime>,
    pools: Vec<Arc<DeferredPoolSource>>,
    #[cfg(any(feature = "db", feature = "db-pgsql"))]
    pending_default_driver: Option<std::sync::OnceLock<DatabaseDriver>>,
}

impl SqlObservabilityRuntime {
    /// 业务作用：在 Application 校验过 UserHook 窗口后登记已声明的渠道实现。
    /// 参数说明：`provider_id` 为配置名称，`provider` 为只在后台 worker 中执行的实现。
    /// 返回：唯一登记成功；封口、未知类型或重名返回安全阶段错误。
    pub fn register_custom(
        &self,
        provider_id: &str,
        provider: Arc<dyn Notify>,
    ) -> ApplicationResult<()> {
        self.notifications
            .register_custom(provider_id, provider)
            .map_err(|error| observation_error(ApplicationPhase::UserHook, error.to_string()))
    }

    /// 业务作用：返回与实际运行一致且不包含 SQL、URL 或凭据的有效配置。
    /// 参数说明：`datasource` 必须属于冻结目录，`method` 为可选完整静态方法身份。
    /// 返回：逐叶继承后的策略；未知身份不产生隐式策略。
    pub fn effective(
        &self,
        datasource: &str,
        method: Option<&str>,
    ) -> ApplicationResult<EffectivePolicy> {
        if !self.catalog.contains_key(datasource)
            || method.is_some_and(|name| {
                !MAPPER_METHOD_META
                    .iter()
                    .any(|meta| meta.method == name && meta.datasource == datasource)
            })
        {
            return Err(observation_error(
                ApplicationPhase::Running,
                "unknown SQL observation identity",
            ));
        }
        Ok(self.settings.observability.effective(datasource, method))
    }

    /// 业务作用：只在建池后一次填入预先登记的指标代理，不改变 descriptor 或预算。
    /// 参数说明：`driver` 为 typed 后端，`names` 为实际建池目录，`source` 持有只读 Pool 句柄。
    /// 返回：实际目录与预留目录完全匹配时发布；不允许运行期增加数据源。
    #[cfg(any(feature = "db", feature = "db-pgsql"))]
    fn install_pools(
        &self,
        driver: DatabaseDriver,
        names: BTreeSet<String>,
        source: PoolMetricsSource,
    ) -> ApplicationResult<()> {
        let expected: BTreeSet<String> = self
            .catalog
            .iter()
            .filter(|(_, value)| value.drivers().any(|candidate| candidate == driver))
            .map(|(name, _)| name.clone())
            .collect();
        if expected != names {
            return Err(observation_error(
                ApplicationPhase::Start,
                "Pool observation catalog differs from the frozen datasource catalog",
            ));
        }
        let proxy = self
            .pools
            .iter()
            .find(|pool| pool.driver == driver)
            .ok_or_else(|| {
                observation_error(
                    ApplicationPhase::Start,
                    "Pool observation backend is undeclared",
                )
            })?;
        let mut installed = proxy.source.write().map_err(|_| {
            observation_error(
                ApplicationPhase::Start,
                "Pool observation state unavailable",
            )
        })?;
        if installed.is_some() {
            return Err(observation_error(
                ApplicationPhase::Start,
                "Pool observation source is already installed",
            ));
        }
        // 首个 typed Pool 决定 default 的唯一后端；另一候选仅保留预算，不可再发布第二个后端。
        if let Some(pending) = &self.pending_default_driver {
            let selected = pending.get_or_init(|| driver);
            if *selected != driver {
                return Err(observation_error(
                    ApplicationPhase::Prepare,
                    "deferred default Pool driver is already frozen",
                ));
            }
        }
        *installed = Some(Arc::new(source));
        Ok(())
    }

    /// 业务作用：在 Pool 释放之前撤销出口侧句柄，避免抓取已关闭的资源。
    /// 参数说明：无。
    /// 返回：无；descriptor 保留，Pool 样本在后续快照中消失。
    fn detach_pools(&self) {
        for proxy in &self.pools {
            proxy
                .source
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
    }
}

/// 先注册静态合同与预算、后装入 typed Pool 的只读代理。
struct DeferredPoolSource {
    #[cfg(any(feature = "db", feature = "db-pgsql"))]
    driver: DatabaseDriver,
    descriptors: &'static [&'static MetricDescriptor],
    source: RwLock<Option<Arc<PoolMetricsSource>>>,
}

impl LegacyMetricsSource for DeferredPoolSource {
    /// 业务作用：在连接建立前提供固定 Pool gauge 合同。
    /// 参数说明：无。
    /// 返回：后端共用的静态 descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        self.descriptors
    }
    /// 业务作用：只在抓取阶段借用已经建好的 Pool。
    /// 参数说明：无。
    /// 返回：未安装或已停机时为空快照，其它情况透传同源样本。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        Some(
            self.source
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .and_then(|source| source.snapshot())
                .unwrap_or_default(),
        )
    }
    /// 业务作用：使用统一结构化样本渲染 Pool 文本。
    /// 参数说明：`output` 为出口缓冲区。
    /// 返回：追加当前样本而不持有长期连接借用。
    fn render_prometheus(&self, output: &mut String) {
        nametrics_core::atomic::render_snapshot(
            self.descriptors,
            &self.snapshot().unwrap_or_default(),
            output,
        );
    }
}

struct SqlMetricsSource {
    descriptors: &'static [&'static MetricDescriptor],
    sources: Vec<Arc<dyn LegacyMetricsSource>>,
}

impl SqlMetricsSource {
    /// 业务作用：把多后端共享 descriptor 的源合成一个原子注册事务，避免半套指标可见。
    /// 参数说明：`sources` 为 Mapper、连接、Pool 和通知的只读出口。
    /// 返回：静态 descriptor 去重后的聚合源；冲突语义在注册前拒绝。
    fn new(sources: Vec<Arc<dyn LegacyMetricsSource>>) -> ApplicationResult<Self> {
        let mut descriptors: BTreeMap<&'static str, &'static MetricDescriptor> = BTreeMap::new();
        for source in &sources {
            for descriptor in source.descriptors() {
                if let Some(existing) = descriptors.insert(descriptor.name, *descriptor) {
                    if existing.kind != descriptor.kind
                        || existing.label_names != descriptor.label_names
                        || existing.histogram_bounds != descriptor.histogram_bounds
                        || existing.unit != descriptor.unit
                    {
                        return Err(observation_error(
                            ApplicationPhase::Start,
                            "SQL metric descriptor collision",
                        ));
                    }
                }
            }
        }
        // descriptor 引用随进程存活，旧 guard 和并发出口不会观察到释放后的目录。
        let descriptors = Box::leak(
            descriptors
                .into_values()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        Ok(Self {
            descriptors,
            sources,
        })
    }
}

impl LegacyMetricsSource for SqlMetricsSource {
    /// 业务作用：给 MetricHub 一次提交全部 SQL 观测合同。
    /// 参数说明：无。
    /// 返回：名称唯一的静态 descriptor 目录。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        self.descriptors
    }
    /// 业务作用：让 Prometheus 与 OTLP 读取同一组结构化领域样本。
    /// 参数说明：无。
    /// 返回：所有已安装源的合并快照，不在 SQL 路径进行采样。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        Some(
            self.sources
                .iter()
                .flat_map(|source| source.snapshot().unwrap_or_default())
                .collect(),
        )
    }
    /// 业务作用：从同一快照追加 Prometheus 文本，保持桶和标签一致。
    /// 参数说明：`output` 为出口缓冲区。
    /// 返回：追加已登记指标，不创建第二份统计状态。
    fn render_prometheus(&self, output: &mut String) {
        nametrics_core::atomic::render_snapshot(
            self.descriptors,
            &self.snapshot().unwrap_or_default(),
            output,
        );
    }
}

/// 在受管 DB 前初始化、在业务入口前激活通知的 SQL 观测节点。
pub(crate) struct SqlObservabilityComponent {
    runtime: Option<Arc<SqlObservabilityRuntime>>,
    task: Option<ApplicationFuture<'static>>,
}

impl SqlObservabilityComponent {
    /// 业务作用：创建尚未读取配置或分配队列的生命周期组件。
    /// 参数说明：无。
    /// 返回：无外部副作用的组件。
    pub(crate) fn new() -> Self {
        Self {
            runtime: None,
            task: None,
        }
    }
}

impl ApplicationComponent for SqlObservabilityComponent {
    /// 业务作用：为配置、资源与停机动作提供独立的稳定归属。
    /// 参数说明：无。
    /// 返回：SQL 观测组件标识。
    fn id(&self) -> ComponentId {
        ComponentId::SqlObservability
    }

    /// 业务作用：在 DB 外部动作前冻结策略、预留全部 SQL 指标并安装有界生产端。
    /// 参数说明：`context` 提供最终配置、统一 MetricHub 和受管资源登记。
    /// 返回：全部目录、预算及路由验证成功时发布运行时；失败阻止业务 I/O。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let app = context.application().clone();
            let snapshot = app.config();
            validate_section(snapshot.value(), ApplicationPhase::Start)?;
            crate::process::configure_fallback_sql_logging(snapshot.value())?;
            let settings = parse_sql(snapshot.value(), ApplicationPhase::Start)?;
            let catalog = datasource_catalog(snapshot.value(), ApplicationPhase::Start)?;
            let references = active_provider_refs(snapshot.value())
                .map_err(|message| observation_error(ApplicationPhase::Start, message))?;
            let notification_config = NotificationsConfig::parse(
                snapshot.value().get("notifications"),
            )
            .map_err(|error| observation_error(ApplicationPhase::Start, error.to_string()))?;
            // Batch 可引用进程级实现，但没有命名渠道的启动登记窗口，不能在准备完成后替换命名路由图。
            if app.info().mode() == ApplicationMode::Batch
                && references
                    .iter()
                    .any(|name| name != nanotify_core::DEFAULT_PROVIDER_ID)
            {
                return Err(observation_error(ApplicationPhase::Start, "Batch named notification providers require a Service registration window; use the nanotify_core default implementation for Batch"));
            }
            let notifications = Arc::new(
                NotificationRuntime::start(
                    notification_config,
                    settings.observability.dispatcher.clone(),
                    references,
                )
                .map_err(|error| observation_error(ApplicationPhase::Start, error.to_string()))?,
            );
            let producer = notifications.producer();
            let identity = Arc::new(notification_identity(&app, snapshot.value())?);
            for method in MAPPER_METHOD_META {
                let policy = settings
                    .observability
                    .effective(method.datasource, Some(method.method));
                method
                    .configure(policy.clone(), settings.observability.metrics.record_rows)
                    .map_err(|message| observation_error(ApplicationPhase::Start, message))?;
                if policy.alerts.slow_sql.enabled || policy.alerts.execution_error.enabled {
                    method
                        .configure_alerts(
                            identity.clone(),
                            alert_route(
                                &producer,
                                policy.alerts.slow_sql.enabled,
                                policy.alerts.slow_sql.provider_ref.as_deref(),
                            )?,
                            alert_route(
                                &producer,
                                policy.alerts.execution_error.enabled,
                                policy.alerts.execution_error.provider_ref.as_deref(),
                            )?,
                        )
                        .map_err(|message| observation_error(ApplicationPhase::Start, message))?;
                }
            }
            for (name, source) in &catalog {
                let policy = settings.observability.effective(name, None);
                for driver in source.drivers() {
                    connections::configure_datasource(
                        driver,
                        name,
                        wait_policy(&policy.acquire_wait),
                        wait_policy(&policy.transaction_slot_wait),
                    )
                    .map_err(|_| {
                        observation_error(
                            ApplicationPhase::Start,
                            "connection observation policy could not freeze",
                        )
                    })?;
                    let alert = &policy.alerts.acquire_timeout;
                    if let Some(route) =
                        alert_route(&producer, alert.enabled, alert.provider_ref.as_deref())?
                    {
                        let purposes = alert
                            .purposes
                            .iter()
                            .map(|purpose| match purpose.as_str() {
                                "mapper" => ConnectionPurpose::Mapper,
                                "migration" => ConnectionPurpose::Migration,
                                "probe" => ConnectionPurpose::Probe,
                                _ => ConnectionPurpose::Direct,
                            })
                            .collect();
                        connections::configure_acquire_alert(
                            driver,
                            name,
                            connections::AcquireAlertPolicy {
                                route,
                                severity: alert.severity,
                                cooldown: Duration::from_millis(alert.cooldown_ms),
                                purposes,
                                service: identity.service.clone(),
                                instance: identity.instance.clone(),
                                environment: identity.environment.clone(),
                                cluster: identity.cluster.clone(),
                            },
                        )
                        .map_err(|_| {
                            observation_error(
                                ApplicationPhase::Start,
                                "connection notification policy could not freeze",
                            )
                        })?;
                    }
                }
            }
            let mapper = namapper_core::observability::metrics_source();
            let mut budget = mapper.worst_case_series();
            let mut sources: Vec<Arc<dyn LegacyMetricsSource>> = vec![mapper];
            let mut pools = Vec::new();
            for driver in [DatabaseDriver::MySql, DatabaseDriver::PostgreSql] {
                let count = catalog
                    .values()
                    .filter(|source| source.drivers().any(|candidate| candidate == driver))
                    .count();
                if count == 0 {
                    continue;
                }
                let connections = Arc::new(ConnectionMetricsSource::new(driver));
                budget += connections.series_budget();
                sources.push(connections);
                let empty = PoolMetricsSource::new(driver, Vec::new()).map_err(|_| {
                    observation_error(ApplicationPhase::Start, "Pool metrics could not initialize")
                })?;
                let pool = Arc::new(DeferredPoolSource {
                    #[cfg(any(feature = "db", feature = "db-pgsql"))]
                    driver,
                    descriptors: empty.descriptors(),
                    source: RwLock::new(None),
                });
                budget += count * 4;
                sources.push(pool.clone());
                pools.push(pool);
            }
            for (source, source_budget) in notifications.metrics_sources() {
                budget += source_budget;
                sources.push(source);
            }
            let metrics = Arc::new(SqlMetricsSource::new(sources)?);
            let hub = app.metrics_hub();
            hub.register_legacy_source_reserved(metrics, budget).map_err(|_| observation_error(ApplicationPhase::Start,
                format!("SQL metric registration failed: methods={}, datasources={}, required_series={}, available_series={}", MAPPER_METHOD_META.len(), catalog.len(), budget, nametrics_core::MAX_METRIC_SERIES.saturating_sub(hub.reserved_series()))))?;
            let runtime = Arc::new(SqlObservabilityRuntime {
                settings,
                #[cfg(any(feature = "db", feature = "db-pgsql"))]
                pending_default_driver: catalog
                    .values()
                    .any(|source| source.driver.is_none())
                    .then(std::sync::OnceLock::new),
                catalog,
                notifications,
                pools,
            });
            if snapshot
                .value()
                .pointer("/log/level")
                .and_then(Value::as_str)
                .is_some_and(|level| level.contains("sqlx::query="))
            {
                tracing::warn!(target: "napp::sql", event="legacy_sql_console_configuration", "逐条 SQL 配置可迁移至 sql.observability.console");
            }
            context.register_resource(None, runtime.clone())?;
            context.activate(Box::new(StopSqlSources(runtime.clone())));
            tracing::info!(target: "napp::sql", event="sql_observation_installed", methods=MAPPER_METHOD_META.len(), datasources=runtime.catalog.len(), required_series=budget,
                slow_threshold_ms=runtime.settings.observability.slow_sql.threshold_ms, "SQL 观测已冻结");
            self.runtime = Some(runtime);
            Ok(())
        })
    }

    /// 业务作用：封口命名通知路由，默认路由保持对 nanotify-core 业务实现的按需引用。
    /// 参数说明：`_context` 为宿主 Prepare 阶段上下文；不构造协议客户端或读取渠道凭据。
    /// 返回：路由准备完成后成功；未提供实现不阻止启动，使用时忽略。
    fn prepare<'a>(&'a mut self, _context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(runtime) = &self.runtime {
                runtime.notifications.prepare().map_err(|error| {
                    observation_error(ApplicationPhase::Prepare, error.to_string())
                })?;
            }
            Ok(())
        })
    }

    /// 业务作用：出口已接管后启动唯一通知 dispatcher，并登记先撤销生产权再排空的逆序动作。
    /// 参数说明：`context` 提供停机清理栈；业务流量尚未开放。
    /// 返回：消费者所有权安全移交给 Supervisor 时成功。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(runtime) = &self.runtime else {
                return Ok(());
            };
            #[cfg(any(feature = "db", feature = "db-pgsql"))]
            if runtime
                .pending_default_driver
                .as_ref()
                .is_some_and(|driver| driver.get().is_none())
            {
                // 双后端候选只是预算预留，真实 typed Pool 尚未移交时不能把观测目录当作就绪。
                return Err(observation_error(
                    ApplicationPhase::Ready,
                    "deferred default Pool driver has not been established",
                ));
            }
            // Ready 再登记出口句柄撤销，使正常反向清理发生在 DB 池关闭之前。
            context.activate(Box::new(StopSqlSources(runtime.clone())));
            let dispatcher = runtime
                .notifications
                .take_dispatcher()
                .map_err(|error| observation_error(ApplicationPhase::Ready, error.to_string()))?;
            if let Some(dispatcher) = dispatcher {
                let stop = CancellationToken::new();
                let force = CancellationToken::new();
                let (done, completed) = tokio::sync::oneshot::channel();
                context.activate(Box::new(StopNotifications {
                    runtime: runtime.clone(),
                    stop: stop.clone(),
                    force: force.clone(),
                    completed: Some(completed),
                }));
                let application = context.application().clone();
                self.task = Some(Box::pin(async move {
                    // 组件 Ready 先于全应用 Ready；后续组件失败时不能把 UserHook 候选提前发往外部。
                    if wait_application_ready(&application, &stop, &force).await {
                        tokio::select! { biased; _ = force.cancelled() => {}, _ = dispatcher.run(stop) => {} }
                    }
                    let _ = done.send(());
                    Ok(())
                }));
            }
            Ok(())
        })
    }

    /// 业务作用：让 Runner 持有唯一后台任务，禁止通知脱离 Application 生命周期。
    /// 参数说明：无。
    /// 返回：只移交一次通知任务；未启用通知时为 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.task
            .take()
            .map(|task| ("sql-notification-dispatcher", task))
    }
}

struct StopSqlSources(Arc<SqlObservabilityRuntime>);
impl ShutdownAction for StopSqlSources {
    /// 业务作用：标识撤销 SQL 观测对运行资源的借用步骤。
    /// 参数说明：无。
    /// 返回：固定动作名称。
    fn label(&self) -> &'static str {
        "detach-sql-observation-sources"
    }
    /// 业务作用：在连接池释放前撤销生产能力和 Pool 快照句柄。
    /// 参数说明：`context` 是共享停机边界，本动作不等待外部资源。
    /// 返回：内存所有权撤销后成功。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.0.notifications.stop_accepting();
            self.0.detach_pools();
            Ok(())
        })
    }
}

struct StopNotifications {
    runtime: Arc<SqlObservabilityRuntime>,
    stop: CancellationToken,
    force: CancellationToken,
    completed: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl ShutdownAction for StopNotifications {
    /// 业务作用：标识与业务数据库隔离的通知排空步骤。
    /// 参数说明：无。
    /// 返回：固定动作名称。
    fn label(&self) -> &'static str {
        "drain-sql-notifications"
    }
    /// 业务作用：先关新通知，再在组件与全局剩余预算内排空，超时取消剩余请求。
    /// 参数说明：`context` 提供不可延长的全局停机预算。
    /// 返回：排空或有限放弃后成功；不等待外部渠道恢复。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.runtime.notifications.stop_accepting();
            self.stop.cancel();
            let budget = context.child_budget(Duration::from_millis(
                self.runtime
                    .settings
                    .observability
                    .dispatcher
                    .shutdown_drain_timeout_ms,
            ));
            if let Some(completed) = self.completed.take() {
                if tokio::time::timeout(budget, completed).await.is_err() {
                    self.force.cancel();
                }
            }
            Ok(())
        })
    }
}

/// 业务作用：在 DB probe/build 之前读取冻结的逐数据源语句日志策略。
/// 参数说明：`application` 为当前宿主，`datasource` 为受管目录名称。
/// 返回：SQLx 日志级别；禁用时两个 SQLx statement 出口均应关闭。
#[cfg(any(feature = "db", feature = "db-pgsql"))]
pub(crate) async fn statement_logging(
    application: &Application,
    datasource: &str,
) -> ApplicationResult<StatementLogging> {
    let runtime = application
        .resources()
        .get::<Arc<SqlObservabilityRuntime>>()
        .await?;
    let policy = runtime.effective(datasource, None)?.console;
    Ok(if !policy.enabled {
        StatementLogging::Disabled
    } else if policy.statement_level == LogLevel::Trace {
        StatementLogging::Trace
    } else {
        StatementLogging::Debug
    })
}

/// 业务作用：MySQL 建池成功后填入启动期已预留的指标代理。
/// 参数说明：`application` 为宿主，`pools` 为已经拥有显式停机动作的受管池。
/// 返回：目录一致且仅安装一次时成功。
#[cfg(feature = "db")]
pub(crate) async fn install_mysql_pools(
    application: &Application,
    pools: &BTreeMap<String, natx::MySqlPool>,
) -> ApplicationResult<()> {
    if pools.is_empty() {
        return Ok(());
    }
    let runtime = application
        .resources()
        .get::<Arc<SqlObservabilityRuntime>>()
        .await?;
    for (name, pool) in pools {
        if Duration::from_millis(runtime.effective(name, None)?.acquire_wait.threshold_ms)
            > pool.options().get_acquire_timeout()
        {
            return Err(observation_error(
                ApplicationPhase::Prepare,
                "SQL acquire_wait threshold exceeds the actual MySQL Pool acquire timeout",
            ));
        }
    }
    let source = natx::pool_metrics_source(
        pools
            .iter()
            .map(|(name, pool)| (name.clone(), pool.clone())),
    )
    .map_err(|_| observation_error(ApplicationPhase::Start, "invalid MySQL Pool metrics"))?;
    runtime.install_pools(
        DatabaseDriver::MySql,
        pools.keys().cloned().collect(),
        source,
    )
}

/// 业务作用：PostgreSQL 建池成功后填入同一合同的指标代理。
/// 参数说明：`application` 为宿主，`pools` 为已经拥有显式停机动作的受管池。
/// 返回：目录一致且仅安装一次时成功。
#[cfg(feature = "db-pgsql")]
pub(crate) async fn install_postgres_pools(
    application: &Application,
    pools: &BTreeMap<String, natx_pgsql::PgPool>,
) -> ApplicationResult<()> {
    if pools.is_empty() {
        return Ok(());
    }
    let runtime = application
        .resources()
        .get::<Arc<SqlObservabilityRuntime>>()
        .await?;
    for (name, pool) in pools {
        if Duration::from_millis(runtime.effective(name, None)?.acquire_wait.threshold_ms)
            > pool.options().get_acquire_timeout()
        {
            return Err(observation_error(
                ApplicationPhase::Prepare,
                "SQL acquire_wait threshold exceeds the actual PostgreSQL Pool acquire timeout",
            ));
        }
    }
    let source = natx_pgsql::pool_metrics_source(
        pools
            .iter()
            .map(|(name, pool)| (name.clone(), pool.clone())),
    )
    .map_err(|_| observation_error(ApplicationPhase::Start, "invalid PostgreSQL Pool metrics"))?;
    runtime.install_pools(
        DatabaseDriver::PostgreSql,
        pools.keys().cloned().collect(),
        source,
    )
}

/// 业务作用：从最终配置与静态方法集合计算实际启用的渠道引用，供 secret 惰性投影共用。
/// 参数说明：`raw` 是完整且尚未执行外部动作的配置树。
/// 返回：按最终逐叶覆盖去重的 provider 名称；未显式指定时使用默认进程路由，不要求通知凭据。
pub fn active_provider_refs(raw: &Value) -> Result<BTreeSet<String>, String> {
    let settings =
        parse_sql(raw, ApplicationPhase::Bootstrap).map_err(|error| error.to_string())?;
    let catalog =
        datasource_catalog(raw, ApplicationPhase::Bootstrap).map_err(|error| error.to_string())?;
    let mut references = BTreeSet::new();
    for name in catalog.keys() {
        let policy = settings.observability.effective(name, None);
        if policy.alerts.acquire_timeout.enabled {
            references.insert(
                policy
                    .alerts
                    .acquire_timeout
                    .provider_ref
                    .unwrap_or_else(|| nanotify_core::DEFAULT_PROVIDER_ID.into()),
            );
        }
    }
    for method in MAPPER_METHOD_META {
        let policy = settings
            .observability
            .effective(method.datasource, Some(method.method));
        for alert in [&policy.alerts.slow_sql, &policy.alerts.execution_error] {
            if alert.enabled {
                references.insert(
                    alert
                        .provider_ref
                        .clone()
                        .unwrap_or_else(|| nanotify_core::DEFAULT_PROVIDER_ID.into()),
                );
            }
        }
    }
    Ok(references)
}

/// 业务作用：在配置加载阶段验证所有 SQL 观测叶子、静态目录和渠道引用，不创建资源。
/// 参数说明：`raw` 为完整配置，`phase` 指定安全错误归因。
/// 返回：所有启动合同成立时成功；方法、数据源或预算不一致在 I/O 前拒绝。
pub(crate) fn validate_section(raw: &Value, phase: ApplicationPhase) -> ApplicationResult<()> {
    let config = parse_sql(raw, phase)?;
    let catalog = datasource_catalog(raw, phase)?;
    namapper_core::observability::config::console_directive(raw)
        .map_err(|message| observation_error(phase, message))?;
    let has_parameters = config.observability.console.include_parameters
        || catalog.keys().any(|name| {
            config
                .observability
                .effective(name, None)
                .console
                .include_parameters
        });
    if has_parameters {
        let environments: Vec<&str> = [
            "/grafana/observability/identity/environment",
            "/application/profile",
            "/profile",
            "/environment",
        ]
        .into_iter()
        .filter_map(|path| raw.pointer(path).and_then(Value::as_str))
        .collect();
        if environments.is_empty()
            || environments.iter().any(|environment| {
                !matches!(*environment, "local" | "development" | "dev" | "test")
            })
        {
            return Err(observation_error(phase, "SQL console parameters require an explicit local/development/dev/test environment or profile"));
        }
        if catalog.keys().any(|name| {
            let console = config.observability.effective(name, None).console;
            console.include_parameters && !console.enabled
        }) || catalog.is_empty() && !config.observability.console.enabled
        {
            return Err(observation_error(
                phase,
                "SQL console parameters require console.enabled",
            ));
        }
    }
    let mut methods = BTreeSet::new();
    for method in MAPPER_METHOD_META {
        if !methods.insert(method.method) {
            return Err(observation_error(
                phase,
                "duplicate static Mapper method identity",
            ));
        }
        let source = catalog.get(method.datasource).ok_or_else(|| {
            observation_error(phase, "Mapper references an undeclared datasource")
        })?;
        if source.driver.map(driver_name) != Some(method.driver) {
            return Err(observation_error(
                phase,
                "Mapper driver differs from its datasource",
            ));
        }
        config
            .observability
            .effective(method.datasource, Some(method.method))
            .validate()
            .map_err(|message| observation_error(phase, message))?;
    }
    if config
        .observability
        .method_overrides
        .keys()
        .any(|method| !methods.contains(method.as_str()))
    {
        return Err(observation_error(
            phase,
            "SQL method override is not in the static catalog",
        ));
    }
    if config
        .observability
        .datasource_overrides
        .keys()
        .any(|name| !catalog.contains_key(name))
    {
        return Err(observation_error(
            phase,
            "SQL datasource override is not in the frozen catalog",
        ));
    }
    for (name, source) in &catalog {
        let policy = config.observability.effective(name, None);
        if source
            .acquire_timeout_ms
            .is_some_and(|timeout| policy.acquire_wait.threshold_ms > timeout)
        {
            return Err(observation_error(
                phase,
                "SQL acquire_wait threshold exceeds datasource acquire timeout",
            ));
        }
    }
    let notifications = NotificationsConfig::parse(raw.get("notifications"))
        .map_err(|error| observation_error(phase, error.to_string()))?;
    let references =
        active_provider_refs(raw).map_err(|message| observation_error(phase, message))?;
    notifications
        .validate_references(&references, &config.observability.dispatcher)
        .map_err(|error| observation_error(phase, error.to_string()))?;
    Ok(())
}

/// 业务作用：把统一默认配置与日志兼容模式组合成可冻结策略。
/// 参数说明：`raw` 为完整配置，`phase` 是诊断阶段。
/// 返回：已严格验证的 SQL 配置；console 与低层 directive 同时声明时拒绝歧义。
fn parse_sql(raw: &Value, phase: ApplicationPhase) -> ApplicationResult<SqlConfig> {
    let mut config =
        SqlConfig::parse(raw.get("sql")).map_err(|message| observation_error(phase, message))?;
    let legacy = raw
        .pointer("/log/level")
        .and_then(Value::as_str)
        .and_then(|level| {
            level
                .split(',')
                .find_map(|directive| directive.trim().strip_prefix("sqlx::query="))
        });
    if let Some(level) = legacy {
        let explicit = raw.pointer("/sql/observability/console").is_some()
            || raw
                .pointer("/sql/observability/datasource_overrides")
                .and_then(Value::as_object)
                .is_some_and(|items| items.values().any(|value| value.get("console").is_some()));
        if explicit {
            return Err(observation_error(
                phase,
                "use sql.observability.console instead of simultaneous sqlx::query directives",
            ));
        }
        match level.trim() {
            "debug" => {
                config.observability.console.enabled = true;
                config.observability.console.statement_level = LogLevel::Debug;
            }
            "trace" => {
                config.observability.console.enabled = true;
                config.observability.console.statement_level = LogLevel::Trace;
            }
            _ => {}
        }
    }
    Ok(config)
}

/// 业务作用：在 probe 之前提取数据库观测所需的非敏感目录，不保存 URL。
/// 参数说明：`raw` 为完整配置，`phase` 为错误归因。
/// 返回：名字、driver 与 acquire 预算；无显式数据库配置时保留空目录。
fn datasource_catalog(
    raw: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<BTreeMap<String, DatasourceObservation>> {
    let values: BTreeMap<String, &Value> = match (raw.get("database"), raw.get("datasources")) {
        (Some(_), Some(_)) => {
            return Err(observation_error(
                phase,
                "database and datasources cannot both be declared",
            ))
        }
        (Some(value), None) => BTreeMap::from([("default".into(), value)]),
        (None, Some(value)) => value
            .as_object()
            .ok_or_else(|| observation_error(phase, "datasources must be an object"))?
            .iter()
            .map(|(name, value)| (name.clone(), value))
            .collect(),
        (None, None) => BTreeMap::new(),
    };
    if values.len() > natx_core::MAX_MANAGED_DATASOURCES {
        return Err(observation_error(
            phase,
            "datasource observation catalog is too large",
        ));
    }
    let mut result = BTreeMap::new();
    for (name, value) in values {
        natx_core::DatasourceRef::new(&name)
            .map_err(|_| observation_error(phase, "invalid datasource observation name"))?;
        if !value.is_object() {
            return Err(observation_error(phase, "datasource must be an object"));
        }
        let driver = match value.get("driver") {
            Some(Value::String(driver)) if driver == "mysql" => DatabaseDriver::MySql,
            Some(Value::String(driver)) if driver == "postgresql" => DatabaseDriver::PostgreSql,
            None if !cfg!(feature = "db-pgsql") && cfg!(feature = "db") => {
                // MySQL-only 入口允许省略 driver，必须与建池配置解释保持一致。
                DatabaseDriver::MySql
            }
            None if value
                .get("url")
                .and_then(Value::as_str)
                .is_some_and(|url| url.starts_with("mysql://")) =>
            {
                DatabaseDriver::MySql
            }
            _ => {
                return Err(observation_error(
                    phase,
                    "invalid or missing datasource driver",
                ))
            }
        };
        let acquire_timeout_ms = match value.get("acquire_timeout_ms") {
            None => 2000,
            Some(value) => value
                .as_u64()
                .filter(|value| *value > 0)
                .ok_or_else(|| observation_error(phase, "invalid datasource acquire timeout"))?,
        };
        result.insert(
            name,
            DatasourceObservation {
                driver: Some(driver),
                acquire_timeout_ms: Some(acquire_timeout_ms),
            },
        );
    }
    // 显式 UserHook 建池尚未产生真实 Pool；只预占静态身份，真实超时在移交池时复验。
    if result.is_empty()
        && raw
            .pointer("/saga/database_bootstrap")
            .and_then(Value::as_str)
            == Some("user_hook")
    {
        for method in MAPPER_METHOD_META {
            let driver = match method.driver {
                "mysql" => DatabaseDriver::MySql,
                "postgresql" => DatabaseDriver::PostgreSql,
                _ => return Err(observation_error(phase, "invalid static Mapper driver")),
            };
            if result
                .get(method.datasource)
                .is_some_and(|value| value.driver != Some(driver))
            {
                return Err(observation_error(
                    phase,
                    "deferred datasource has conflicting Mapper drivers",
                ));
            }
            result.insert(
                method.datasource.into(),
                DatasourceObservation {
                    driver: Some(driver),
                    acquire_timeout_ms: None,
                },
            );
        }
        if result.is_empty() {
            let driver = if cfg!(all(feature = "db", feature = "db-pgsql")) {
                None
            } else if cfg!(feature = "db") {
                Some(DatabaseDriver::MySql)
            } else {
                Some(DatabaseDriver::PostgreSql)
            };
            result.insert(
                "default".into(),
                DatasourceObservation {
                    driver,
                    acquire_timeout_ms: None,
                },
            );
        }
    }
    Ok(result)
}

/// 业务作用：在启动期解析 concrete 通知路由，不让执行路径查表或调用第三方代码。
/// 参数说明：`producer` 为可选有界队列，`enabled` 和 `provider` 为有效叶子。
/// 返回：关闭规则返回 None；启用但缺失 route 属于启动错误。
fn alert_route(
    producer: &Option<NotificationProducer>,
    enabled: bool,
    provider: Option<&str>,
) -> ApplicationResult<Option<AlertRoute>> {
    if !enabled {
        return Ok(None);
    }
    producer
        .as_ref()
        .and_then(|producer| match provider {
            None | Some(nanotify_core::DEFAULT_PROVIDER_ID) => producer.default_route(),
            Some(name) => producer.route(name),
        })
        .map(Some)
        .ok_or_else(|| {
            observation_error(
                ApplicationPhase::Start,
                "enabled SQL alert has no frozen route",
            )
        })
}

/// 业务作用：把共享 YAML 等待策略转换成连接核心的后端中立结构。
/// 参数说明：`policy` 为已经逐叶继承和验证的有效配置。
/// 返回：固定阈值、级别与冷却，不包含任意 observer。
fn wait_policy(policy: &WaitPolicy) -> WaitObservationPolicy {
    WaitObservationPolicy {
        threshold: Duration::from_millis(policy.threshold_ms),
        log_enabled: policy.log_enabled,
        log_level: if policy.log_level == LogLevel::Info {
            WaitLogLevel::Info
        } else {
            WaitLogLevel::Warn
        },
        log_cooldown: Duration::from_millis(policy.log_cooldown_ms),
    }
}

/// 业务作用：把已有冻结观测身份用于通知，未启用出口时仍创建稳定进程来源。
/// 参数说明：`app` 提供应用名和启动时刻，`raw` 只读取非敏感身份字段。
/// 返回：启用统一出口时复用其已冻结身份，否则按应用启动身份生成；身份读取失败阻止启动。
fn notification_identity(
    app: &Application,
    raw: &Value,
) -> ApplicationResult<NotificationIdentity> {
    #[cfg(feature = "observability")]
    if let Some(identity) = crate::observability::application_identity(app)? {
        return Ok(NotificationIdentity {
            service: identity
                .labels
                .get("service_name")
                .cloned()
                .unwrap_or_else(|| app.info().name().into()),
            instance: identity
                .labels
                .get("service_instance_id")
                .cloned()
                .unwrap_or_default(),
            environment: identity.labels.get("deployment_environment").cloned(),
            cluster: identity.labels.get("cluster").cloned(),
        });
    }
    let identity = raw.pointer("/grafana/observability/identity");
    let value = |key| {
        identity
            .and_then(|identity| identity.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    Ok(NotificationIdentity {
        service: value("service_name").unwrap_or_else(|| app.info().name().into()),
        instance: value("instance_id").unwrap_or_else(|| {
            format!(
                "{}-{}",
                std::process::id(),
                app.info()
                    .started_at()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            )
        }),
        environment: value("environment"),
        cluster: value("cluster"),
    })
}

/// 业务作用：保持数据库后端标签与 Mapper 静态身份一致。
/// 参数说明：`driver` 是冻结 typed 后端。
/// 返回：稳定协议名称。
fn driver_name(driver: DatabaseDriver) -> &'static str {
    match driver {
        DatabaseDriver::MySql => "mysql",
        DatabaseDriver::PostgreSql => "postgresql",
    }
}

/// 业务作用：把已脱敏的配置和装配错误绑定到 SQL 观测阶段。
/// 参数说明：`phase` 是生命周期阶段，`message` 不得包含 SQL、URL 或凭据。
/// 返回：可安全输出的应用错误。
fn observation_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::SqlObservability, phase, message.into())
}

/// 业务作用：把组件准备完成与全应用开放流量区分，启动失败不得产生外部通知。
/// 参数说明：`application` 提供全局状态，`stop` 与 `force` 提供正常及强制停止权威。
/// 返回：Service 观察到全应用 Ready，或 Batch 观测专用屏障后允许执行；停止、失败或状态通道结束返回 false。
async fn wait_application_ready(
    application: &Application,
    stop: &CancellationToken,
    force: &CancellationToken,
) -> bool {
    // Batch 只激活观测组件的 Ready 屏障，不开放 Service 路由；该 Future 仅在屏障成功后由 Runner 接管。
    if application.info().mode() == ApplicationMode::Batch {
        return !stop.is_cancelled() && !force.is_cancelled();
    }
    let mut states = application.subscribe_state();
    loop {
        match application.state() {
            ApplicationState::Ready => return true,
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                return false
            }
            ApplicationState::Starting => {}
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return false,
            _ = force.cancelled() => return false,
            changed = states.changed() => if changed.is_err() { return false; },
        }
    }
}
