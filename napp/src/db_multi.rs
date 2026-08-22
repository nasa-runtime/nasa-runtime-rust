use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use natx_core::{
    BootstrapKind, DataSourceCatalog, DatabaseDriver, ManagedInstallationToken,
    ManagedRegistryOwner,
};
use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ShutdownAction, ShutdownContext,
    StartContext,
};

const DB_MONITOR_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const DEFAULT_DATASOURCE: &str = "default";

#[derive(Default, Deserialize)]
#[serde(default)]
struct DbConfigRoot {
    database: Option<serde_json::Value>,
    datasources: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgConnectionTopology {
    Direct,
    SessionPool,
    TransactionPool,
}

impl PgConnectionTopology {
    /// 业务作用：把配置中的 PostgreSQL 连接拓扑冻结为迁移门禁可判定的封闭枚举。
    ///
    /// 参数说明：`raw` 是可选的 `connection_topology` 配置值。
    ///
    /// 返回：缺省为直连；未知值在建连前返回启动错误。
    fn parse(raw: Option<&str>) -> ApplicationResult<Self> {
        match raw.unwrap_or("direct") {
            "direct" => Ok(Self::Direct),
            "session_pool" => Ok(Self::SessionPool),
            "transaction_pool" => Ok(Self::TransactionPool),
            _ => Err(db_error(
                ApplicationPhase::Start,
                "PostgreSQL connection_topology must be direct, session_pool or transaction_pool",
            )),
        }
    }
}

#[derive(Clone)]
struct MigrationPlan {
    settings: namigrate_core::MigrationSettings,
    schema: String,
    session_url: Option<String>,
    topology: PgConnectionTopology,
}

enum DriverConfig {
    #[cfg(feature = "db")]
    MySql(natx::datasource::DataSourceConfig),
    PostgreSql {
        config: natx_pgsql::datasource::DataSourceConfig,
        schema: Option<String>,
    },
}

struct DatasourceSpec {
    name: String,
    driver: DatabaseDriver,
    config: DriverConfig,
    migration: Option<MigrationPlan>,
}

enum ManagedPool {
    #[cfg(feature = "db")]
    MySql(natx::MySqlPool),
    PostgreSql(natx_pgsql::PgPool),
}

impl ManagedPool {
    /// 业务作用：以统一方式探测一种受管数据库池是否仍可取得连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得并立即释放连接时成功；池关闭或后端不可达时返回基础设施错误。
    async fn acquire(&self) -> anyhow::Result<()> {
        match self {
            #[cfg(feature = "db")]
            Self::MySql(pool) => {
                pool.acquire().await?;
            }
            Self::PostgreSql(pool) => {
                pool.acquire().await?;
            }
        }
        Ok(())
    }

    /// 业务作用：等待一种受管数据库池停止发放连接并关闭其共享资源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；两种 SQLx pool 的关闭均等待已借出连接归还。
    async fn close(&self) {
        match self {
            #[cfg(feature = "db")]
            Self::MySql(pool) => pool.close().await,
            Self::PostgreSql(pool) => pool.close().await,
        }
    }
}

struct DbMonitorInput {
    pool: ManagedPool,
    contributor: ReadinessContributor,
}

/// 业务作用：统一管理同一 Application 中全部 MySQL 与 PostgreSQL datasource 的生命周期。
pub(crate) struct DbComponent {
    critical_task: Option<ApplicationFuture<'static>>,
    #[cfg(feature = "db")]
    mysql_pools: BTreeMap<String, natx::MySqlPool>,
    pg_pools: BTreeMap<String, natx_pgsql::PgPool>,
    drivers: BTreeMap<String, DatabaseDriver>,
    migrations: BTreeMap<String, MigrationPlan>,
    catalog: Option<Arc<DataSourceCatalog>>,
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
    deferred: Option<DeferredBootstrap>,
}

#[cfg(any(feature = "saga", feature = "saga-pgsql"))]
struct DeferredBootstrap {
    owner: ManagedRegistryOwner,
    token: ManagedInstallationToken,
    contributor: ReadinessContributor,
}

impl DbComponent {
    /// 业务作用：创建尚未读取 datasource 配置的统一数据库组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不持有连接、catalog 或健康任务的生命周期组件。
    pub(crate) fn new() -> Self {
        Self {
            critical_task: None,
            #[cfg(feature = "db")]
            mysql_pools: BTreeMap::new(),
            pg_pools: BTreeMap::new(),
            drivers: BTreeMap::new(),
            migrations: BTreeMap::new(),
            catalog: None,
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            deferred: None,
        }
    }
}

/// 业务作用：定义数据库动态探测对 Application Ready 的统一影响阈值。
///
/// 参数说明: 无。
///
/// 返回：连续三次失败转为 NotReady、一次成功恢复的关键依赖策略。
fn db_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy {
        affects_ready: true,
        failure_threshold: 3,
        recovery_threshold: 1,
        stale_after: None,
    }
}

/// 业务作用：持续探测全部受管数据库池，并在停机开始时撤销其 readiness 证据。
///
/// 参数说明：
/// - `application`：提供统一生命周期状态。
/// - `inputs`：冻结的数据源池与 readiness 贡献项。
///
/// 返回：应用停机时正常退出；探测失败反映为 NotReady，不结束监督任务。
async fn run_db_monitor(
    application: Application,
    inputs: Vec<DbMonitorInput>,
) -> ApplicationResult<()> {
    let mut lifecycle = application.subscribe_state();
    'monitor: loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                let now = Instant::now();
                for input in &inputs {
                    input
                        .contributor
                        .observe(DependencyState::NotReady, reason::NOT_READY, now);
                }
                return Ok(());
            }
            ApplicationState::Starting => {
                // Ready 或停机转换必须立即唤醒 monitor，不能让固定探测间隔侵占全局停机预算。
                if lifecycle.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }
            ApplicationState::Ready => {}
        }
        let now = Instant::now();
        for input in &inputs {
            // 停机权威优先于连接探测；连接池不可达时也不能等待 acquire timeout 后才开始反向清理。
            let acquired = tokio::select! {
                biased;
                changed = lifecycle.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                    continue 'monitor;
                }
                result = input.pool.acquire() => result.is_ok(),
            };
            let state = if acquired {
                (DependencyState::Ready, reason::HEALTHY)
            } else {
                (DependencyState::NotReady, reason::PROBE_TIMEOUT)
            };
            input.contributor.observe(state.0, state.1, now);
        }
        // 生命周期变化优先，正常运行时才等待下一轮探测。
        tokio::select! {
            biased;
            changed = lifecycle.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(DB_MONITOR_INTERVAL) => {}
        }
    }
}

impl ApplicationComponent for DbComponent {
    /// 业务作用：声明统一数据库组件在生命周期图中的稳定身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定的数据库组件标识。
    fn id(&self) -> ComponentId {
        ComponentId::Db
    }

    /// 业务作用：全表校验并建立全部 MySQL/PostgreSQL pool，在单一 owner 下原子发布 catalog 与 typed registry。
    ///
    /// 参数说明：`context` 提供启动资源批量登记和失败清理栈。
    ///
    /// 返回：全部数据源均可受管时成功；任何配置、探测或发布失败都触发本轮资源逆序撤销。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            if application
                .ensure_component_declared(
                    ComponentId::Saga,
                    ApplicationPhase::Start,
                    "saga database bootstrap",
                )
                .is_ok()
                && crate::saga::database_bootstrap(&application, ApplicationPhase::Start)?
                    == crate::saga::SagaDatabaseBootstrap::UserHook
            {
                ensure_deferred_registry_empty()?;
                let (owner, token) = natx_core::begin_managed_bootstrap(
                    BootstrapKind::DeferredDefault,
                )
                .map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot reserve deferred datasource bootstrap",
                        error,
                    )
                })?;
                context.activate(Box::new(BootstrapShutdown {
                    token: Some(token.clone()),
                }));
                let contributor = application.register_readiness(
                    ComponentId::Db,
                    Arc::<str>::from("db:default"),
                    db_readiness_policy(),
                )?;
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                self.deferred = Some(DeferredBootstrap {
                    owner,
                    token,
                    contributor,
                });
                return Ok(());
            }

            let sources = read_datasources(&application, ApplicationPhase::Start)?;
            let (owner, token) = natx_core::begin_managed_bootstrap(BootstrapKind::Configured)
                .map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot reserve managed datasource catalog",
                        error,
                    )
                })?;
            context.activate(Box::new(BootstrapShutdown {
                token: Some(token.clone()),
            }));

            for source in sources {
                let name = source.name.clone();
                let pool = match source.config {
                    #[cfg(feature = "db")]
                    DriverConfig::MySql(config) => {
                        if config.probe_on_start {
                            natx::datasource::probe(&config).await.map_err(|error| {
                                db_error_src(
                                    ApplicationPhase::Start,
                                    format!("datasource `{name}` MySQL probe failed"),
                                    error,
                                )
                            })?;
                        }
                        let pool = natx::datasource::build_pool(&config).map_err(|error| {
                            db_error_src(
                                ApplicationPhase::Start,
                                format!("cannot create datasource `{name}` MySQL pool"),
                                error,
                            )
                        })?;
                        self.mysql_pools.insert(name.clone(), pool.clone());
                        ManagedPool::MySql(pool)
                    }
                    DriverConfig::PostgreSql { config, schema } => {
                        if config.probe_on_start {
                            let probe = match schema.as_deref() {
                                Some(schema) => {
                                    natx_pgsql::datasource::probe_in_schema(&config, schema).await
                                }
                                None => natx_pgsql::datasource::probe(&config).await,
                            };
                            probe.map_err(|error| {
                                db_error_src(
                                    ApplicationPhase::Start,
                                    format!("datasource `{name}` PostgreSQL probe failed"),
                                    error,
                                )
                            })?;
                        }
                        let pool = match schema.as_deref() {
                            Some(schema) => {
                                natx_pgsql::datasource::build_pool_in_schema(&config, schema)
                            }
                            None => natx_pgsql::datasource::build_pool(&config),
                        }
                        .map_err(|error| {
                            db_error_src(
                                ApplicationPhase::Start,
                                format!("cannot create datasource `{name}` PostgreSQL pool"),
                                error,
                            )
                        })?;
                        self.pg_pools.insert(name.clone(), pool.clone());
                        ManagedPool::PostgreSql(pool)
                    }
                };
                context.activate(Box::new(DbPoolShutdown {
                    name: name.clone(),
                    pool: Some(pool),
                }));
                self.drivers.insert(name.clone(), source.driver);
                if let Some(migration) = source.migration {
                    self.migrations.insert(name, migration);
                }
            }

            let catalog = Arc::new(
                DataSourceCatalog::try_new(
                    owner,
                    self.drivers
                        .iter()
                        .map(|(name, driver)| (name.clone(), *driver)),
                )
                .map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot freeze managed datasource catalog",
                        error,
                    )
                })?,
            );
            #[cfg(feature = "db")]
            let mysql_registry = if self.mysql_pools.is_empty() {
                None
            } else {
                Some(Arc::new(
                    natx::DataSourceRegistry::try_new_managed(
                        self.mysql_pools
                            .iter()
                            .map(|(name, pool)| (name.clone(), pool.clone())),
                        &token,
                    )
                    .map_err(|error| {
                        db_error_src(
                            ApplicationPhase::Start,
                            "cannot freeze managed MySQL datasource registry",
                            error,
                        )
                    })?,
                ))
            };
            let pg_registry = if self.pg_pools.is_empty() {
                None
            } else {
                Some(Arc::new(
                    natx_pgsql::PgDataSourceRegistry::try_new_managed(
                        self.pg_pools
                            .iter()
                            .map(|(name, pool)| (name.clone(), pool.clone())),
                        &token,
                    )
                    .map_err(|error| {
                        db_error_src(
                            ApplicationPhase::Start,
                            "cannot freeze managed PostgreSQL datasource registry",
                            error,
                        )
                    })?,
                ))
            };

            // 先把精确清理动作压入 active stack，后续任一 registry/catalog 发布失败都会按 owner 撤销本轮全局状态。
            context.activate(Box::new(DbRegistryShutdown {
                catalog_owner: Some(token.owner().clone()),
                #[cfg(feature = "db")]
                mysql: mysql_registry.clone(),
                pgsql: pg_registry.clone(),
            }));

            // typed registry 先进入 Open，但所有查询仍受关闭的 catalog 门控；最后开放 catalog 才会
            // 一次性授予整张多后端资源表的读取权，避免业务看到只发布一半的数据源。
            #[cfg(feature = "db")]
            if let Some(registry) = &mysql_registry {
                natx::install_closed_managed_registry(registry, &token).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot install closed MySQL datasource registry",
                        error,
                    )
                })?;
            }
            if let Some(registry) = &pg_registry {
                natx_pgsql::install_closed_managed_registry(registry, &token).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot install closed PostgreSQL datasource registry",
                        error,
                    )
                })?;
            }
            natx_core::install_managed_catalog(&token, &catalog).map_err(|error| {
                db_error_src(
                    ApplicationPhase::Start,
                    "cannot install closed datasource catalog",
                    error,
                )
            })?;
            #[cfg(feature = "db")]
            if let Some(registry) = &mysql_registry {
                natx::attach_managed_catalog(registry, &catalog, &token).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot bind MySQL registry to datasource catalog",
                        error,
                    )
                })?;
            }
            if let Some(registry) = &pg_registry {
                natx_pgsql::attach_managed_catalog(registry, &catalog, &token).map_err(
                    |error| {
                        db_error_src(
                            ApplicationPhase::Start,
                            "cannot bind PostgreSQL registry to datasource catalog",
                            error,
                        )
                    },
                )?;
            }

            context.register_resource_batch(|batch| {
                #[cfg(feature = "db")]
                for (name, pool) in &self.mysql_pools {
                    batch.push(Some(name), pool.clone())?;
                }
                for (name, pool) in &self.pg_pools {
                    batch.push(Some(name), pool.clone())?;
                }
                batch.push(None, Arc::clone(&catalog))?;
                #[cfg(feature = "db")]
                if let Some(registry) = &mysql_registry {
                    batch.push(None, Arc::clone(registry))?;
                }
                if let Some(registry) = &pg_registry {
                    batch.push(None, Arc::clone(registry))?;
                }
                Ok(())
            })?;
            #[cfg(feature = "db")]
            if let Some(registry) = &mysql_registry {
                natx::open_managed_registry(registry, &token).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot open MySQL datasource registry",
                        error,
                    )
                })?;
            }
            if let Some(registry) = &pg_registry {
                natx_pgsql::open_managed_registry(registry, &token).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        "cannot open PostgreSQL datasource registry",
                        error,
                    )
                })?;
            }
            natx_core::open_managed_catalog(&token, &catalog).map_err(|error| {
                db_error_src(
                    ApplicationPhase::Start,
                    "cannot open datasource catalog",
                    error,
                )
            })?;
            self.catalog = Some(Arc::clone(&catalog));

            let monitor_inputs = build_monitor_inputs(&application, self)?;
            if !monitor_inputs.is_empty() {
                self.critical_task = Some(Box::pin(run_db_monitor(application, monitor_inputs)));
            }
            Ok(())
        })
    }

    /// 业务作用：执行每个 datasource 的迁移门禁，并在全部成功后登记冻结的后端观测事实。
    ///
    /// 参数说明：`context` 提供已发布 Application 资源和延后接管入口。
    ///
    /// 返回：全部 schema 与 topology 合同成立时成功；任一数据源失败都会阻止 Application Ready。
    fn prepare<'a>(
        &'a mut self,
        context: &'a mut crate::PrepareContext<'_>,
    ) -> ApplicationFuture<'a> {
        Box::pin(async move {
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            if self.deferred.is_some() {
                self.adopt_deferred(context).await?;
            }
            let application = context.application().clone();
            for (name, migrator) in application.take_migrations() {
                let driver = self.drivers.get(&name).copied().ok_or_else(|| {
                    db_error(
                        ApplicationPhase::Prepare,
                        format!("migration datasource `{name}` is not configured"),
                    )
                })?;
                let plan = self
                    .migrations
                    .get(&name)
                    .cloned()
                    .unwrap_or(MigrationPlan {
                        settings: namigrate_core::MigrationSettings::default(),
                        schema: "public".to_owned(),
                        session_url: None,
                        topology: PgConnectionTopology::Direct,
                    });
                match driver {
                    DatabaseDriver::MySql => {
                        #[cfg(feature = "db")]
                        {
                            let pool = self.mysql_pools.get(&name).ok_or_else(|| {
                                db_error(ApplicationPhase::Prepare, "MySQL pool is unavailable")
                            })?;
                            namigrate::run_gate(pool, &migrator, &plan.settings)
                                .await
                                .map_err(|error| {
                                    db_error(
                                        ApplicationPhase::Prepare,
                                        format!(
                                            "datasource `{name}` migration gate failed: {error}"
                                        ),
                                    )
                                })?;
                        }
                        #[cfg(not(feature = "db"))]
                        return Err(db_error(
                            ApplicationPhase::Prepare,
                            "MySQL migration support is not compiled",
                        ));
                    }
                    DatabaseDriver::PostgreSql => {
                        let business_pool = self.pg_pools.get(&name).ok_or_else(|| {
                            db_error(ApplicationPhase::Prepare, "PostgreSQL pool is unavailable")
                        })?;
                        if plan.settings.mode != namigrate_core::MigrationMode::Disabled
                            && plan.topology == PgConnectionTopology::TransactionPool
                            && plan.session_url.is_none()
                        {
                            return Err(db_error(
                                ApplicationPhase::Prepare,
                                format!(
                                    "datasource `{name}` requires migrations.session_url for transaction pooling"
                                ),
                            ));
                        }
                        if plan.settings.mode == namigrate_core::MigrationMode::Disabled {
                            namigrate_pgsql::run_gate(
                                business_pool,
                                &plan.schema,
                                &migrator,
                                &plan.settings,
                            )
                            .await
                            .map_err(|error| {
                                db_error(
                                    ApplicationPhase::Prepare,
                                    format!("datasource `{name}` migration gate failed: {error}"),
                                )
                            })?;
                        } else if let Some(session_url) = &plan.session_url {
                            let config = natx_pgsql::datasource::DataSourceConfig {
                                url: session_url.clone(),
                                max_connections: 1,
                                min_connections: 0,
                                acquire_timeout_ms: 2_000,
                                connect_timeout_ms: 5_000,
                                probe_on_start: true,
                            };
                            let pool =
                                natx_pgsql::datasource::build_pool(&config).map_err(|error| {
                                    db_error_src(
                                        ApplicationPhase::Prepare,
                                        format!(
                                            "datasource `{name}` migration session pool is invalid"
                                        ),
                                        error,
                                    )
                                })?;
                            let result: ApplicationResult<()> = async {
                                let mut migration_connection = pool
                                    .acquire()
                                    .await
                                    .map_err(|error| {
                                        db_error_src(
                                            ApplicationPhase::Prepare,
                                            format!(
                                                "datasource `{name}` migration session is unavailable"
                                            ),
                                            error,
                                        )
                                    })?
                                    .detach();
                                // 独立 endpoint 可能因配置漂移指向另一 database 或 schema；必须在取得
                                // advisory lock 前与业务池复验身份，避免在错误目标上通过门禁。
                                namigrate_pgsql::verify_target_identity(
                                    business_pool,
                                    &mut migration_connection,
                                    &plan.schema,
                                )
                                .await
                                .map_err(|error| {
                                    db_error(
                                        ApplicationPhase::Prepare,
                                        format!(
                                            "datasource `{name}` migration target verification failed: {error}"
                                        ),
                                    )
                                })?;
                                namigrate_pgsql::run_gate_on_connection(
                                    migration_connection,
                                    &plan.schema,
                                    &migrator,
                                    &plan.settings,
                                    &[],
                                )
                                .await
                                .map_err(|error| {
                                    db_error(
                                        ApplicationPhase::Prepare,
                                        format!(
                                            "datasource `{name}` migration gate failed: {error}"
                                        ),
                                    )
                                })?;
                                Ok(())
                            }
                            .await;
                            pool.close().await;
                            result?;
                        } else {
                            namigrate_pgsql::run_gate(
                                business_pool,
                                &plan.schema,
                                &migrator,
                                &plan.settings,
                            )
                            .await
                            .map_err(|error| {
                                db_error(
                                    ApplicationPhase::Prepare,
                                    format!("datasource `{name}` migration gate failed: {error}"),
                                )
                            })?;
                        }
                    }
                }
            }
            let catalog = self.catalog.as_ref().ok_or_else(|| {
                db_error(
                    ApplicationPhase::Prepare,
                    "managed datasource catalog is unavailable",
                )
            })?;
            application.register_database_backend_metrics(Arc::clone(catalog))?;
            Ok(())
        })
    }

    /// 业务作用：把唯一数据库健康监控任务移交给 Application 监督器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次调用返回已暂存任务；之后返回 `None`，避免重复监督。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("db-health-monitor", task))
    }
}

#[cfg(any(feature = "saga", feature = "saga-pgsql"))]
impl DbComponent {
    /// 业务作用：接管 UserHook 安装的单一默认池，并把它纳入与配置建池相同的 owner/catalog 生命周期。
    ///
    /// 参数说明：`context` 提供 Prepare 期资源批量登记与失败清理栈。
    ///
    /// 返回：待接管 driver、pool 与 owner 身份一致时成功；缺失或错配时在开放 catalog 前失败。
    async fn adopt_deferred(
        &mut self,
        context: &mut crate::PrepareContext<'_>,
    ) -> ApplicationResult<()> {
        let deferred = self.deferred.take().ok_or_else(|| {
            db_error(
                ApplicationPhase::Prepare,
                "deferred datasource bootstrap state is unavailable",
            )
        })?;
        let driver = match natx_core::registry_mode() {
            natx_core::RegistryModeSnapshot::Bootstrapping {
                kind: BootstrapKind::DeferredDefault,
                deferred_driver: Some(driver),
            } => driver,
            _ => {
                return Err(db_error(
                    ApplicationPhase::Prepare,
                    "UserHook did not install one adoptable default datasource",
                ))
            }
        };
        let catalog = Arc::new(
            DataSourceCatalog::try_new(deferred.owner, [(DEFAULT_DATASOURCE.to_owned(), driver)])
                .map_err(|error| {
                db_error_src(
                    ApplicationPhase::Prepare,
                    "cannot freeze deferred datasource catalog",
                    error,
                )
            })?,
        );
        #[cfg(feature = "db")]
        let mut mysql_registry = None;
        #[allow(unused_assignments)]
        let mut pg_registry = None;
        let pool = match driver {
            DatabaseDriver::MySql => {
                #[cfg(feature = "db")]
                {
                    let registry = natx::adopt_standalone_default_registry_closed(&deferred.token)
                        .map_err(|error| {
                            db_error_src(
                                ApplicationPhase::Prepare,
                                "cannot adopt deferred MySQL datasource",
                                error,
                            )
                        })?;
                    let pool = registry
                        .managed_pool(DEFAULT_DATASOURCE, &deferred.token)
                        .map_err(|error| {
                            db_error_src(
                                ApplicationPhase::Prepare,
                                "deferred MySQL datasource is unavailable",
                                error,
                            )
                        })?;
                    self.mysql_pools
                        .insert(DEFAULT_DATASOURCE.to_owned(), pool.clone());
                    mysql_registry = Some(registry);
                    ManagedPool::MySql(pool)
                }
                #[cfg(not(feature = "db"))]
                return Err(db_error(
                    ApplicationPhase::Prepare,
                    "UserHook installed MySQL but the MySQL feature is not compiled",
                ));
            }
            DatabaseDriver::PostgreSql => {
                let registry = natx_pgsql::adopt_standalone_default_registry_closed(
                    &deferred.token,
                )
                .map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Prepare,
                        "cannot adopt deferred PostgreSQL datasource",
                        error,
                    )
                })?;
                let pool = registry
                    .managed_pool(DEFAULT_DATASOURCE, &deferred.token)
                    .map_err(|error| {
                        db_error_src(
                            ApplicationPhase::Prepare,
                            "deferred PostgreSQL datasource is unavailable",
                            error,
                        )
                    })?;
                self.pg_pools
                    .insert(DEFAULT_DATASOURCE.to_owned(), pool.clone());
                pg_registry = Some(registry);
                ManagedPool::PostgreSql(pool)
            }
        };
        context.activate(Box::new(DbPoolShutdown {
            name: DEFAULT_DATASOURCE.to_owned(),
            pool: Some(match &pool {
                #[cfg(feature = "db")]
                ManagedPool::MySql(pool) => ManagedPool::MySql(pool.clone()),
                ManagedPool::PostgreSql(pool) => ManagedPool::PostgreSql(pool.clone()),
            }),
        }));
        // 接管后 pool 已不再属于 standalone；先登记 owner-aware 清理，防止 catalog 绑定失败留下半完成全局状态。
        context.activate(Box::new(DbRegistryShutdown {
            catalog_owner: Some(deferred.token.owner().clone()),
            #[cfg(feature = "db")]
            mysql: mysql_registry.clone(),
            pgsql: pg_registry.clone(),
        }));
        natx_core::install_managed_catalog(&deferred.token, &catalog).map_err(|error| {
            db_error_src(
                ApplicationPhase::Prepare,
                "cannot install deferred datasource catalog",
                error,
            )
        })?;
        // typed registry 只先开放自身状态；catalog 仍关闭，任何外部查询都不能越过尚未完整登记的资源批次。
        #[cfg(feature = "db")]
        if let Some(registry) = &mysql_registry {
            natx::attach_managed_catalog(registry, &catalog, &deferred.token).map_err(|error| {
                db_error_src(
                    ApplicationPhase::Prepare,
                    "cannot bind deferred MySQL registry",
                    error,
                )
            })?;
        }
        if let Some(registry) = &pg_registry {
            natx_pgsql::attach_managed_catalog(registry, &catalog, &deferred.token).map_err(
                |error| {
                    db_error_src(
                        ApplicationPhase::Prepare,
                        "cannot bind deferred PostgreSQL registry",
                        error,
                    )
                },
            )?;
        }
        context.register_resource_batch(|batch| {
            match &pool {
                #[cfg(feature = "db")]
                ManagedPool::MySql(pool) => {
                    batch.push(Some(DEFAULT_DATASOURCE), pool.clone())?;
                }
                ManagedPool::PostgreSql(pool) => {
                    batch.push(Some(DEFAULT_DATASOURCE), pool.clone())?;
                }
            }
            batch.push(None, Arc::clone(&catalog))?;
            #[cfg(feature = "db")]
            if let Some(registry) = &mysql_registry {
                batch.push(None, Arc::clone(registry))?;
            }
            if let Some(registry) = &pg_registry {
                batch.push(None, Arc::clone(registry))?;
            }
            Ok(())
        })?;
        #[cfg(feature = "db")]
        if let Some(registry) = &mysql_registry {
            natx::open_managed_registry(registry, &deferred.token).map_err(|error| {
                db_error_src(
                    ApplicationPhase::Prepare,
                    "cannot open deferred MySQL registry",
                    error,
                )
            })?;
        }
        if let Some(registry) = &pg_registry {
            natx_pgsql::open_managed_registry(registry, &deferred.token).map_err(|error| {
                db_error_src(
                    ApplicationPhase::Prepare,
                    "cannot open deferred PostgreSQL registry",
                    error,
                )
            })?;
        }
        natx_core::open_managed_catalog(&deferred.token, &catalog).map_err(|error| {
            db_error_src(
                ApplicationPhase::Prepare,
                "cannot open deferred datasource catalog",
                error,
            )
        })?;
        self.drivers.insert(DEFAULT_DATASOURCE.to_owned(), driver);
        self.catalog = Some(catalog);
        pool.acquire().await.map_err(|error| {
            db_error_src(
                ApplicationPhase::Prepare,
                "deferred datasource probe failed",
                error,
            )
        })?;
        deferred
            .contributor
            .observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
        self.critical_task = Some(Box::pin(run_db_monitor(
            context.application().clone(),
            vec![DbMonitorInput {
                pool,
                contributor: deferred.contributor,
            }],
        )));
        Ok(())
    }
}

/// 业务作用：为冻结 catalog 中的每个 datasource 建立同名 readiness 贡献项与 typed pool 视图。
///
/// 参数说明：
/// - `application`：readiness registry 的所有者。
/// - `component`：已经完成 pool 建立和 driver 冻结的数据库组件。
///
/// 返回：driver 与 typed pool 一一对应时返回监控输入；内部资源缺失时拒绝进入 Ready。
fn build_monitor_inputs(
    application: &Application,
    component: &DbComponent,
) -> ApplicationResult<Vec<DbMonitorInput>> {
    let mut inputs = Vec::with_capacity(component.drivers.len());
    for (name, driver) in &component.drivers {
        let contributor = application.register_readiness(
            ComponentId::Db,
            Arc::<str>::from(format!("db:{name}")),
            db_readiness_policy(),
        )?;
        contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
        let pool = match driver {
            DatabaseDriver::MySql => {
                #[cfg(feature = "db")]
                {
                    ManagedPool::MySql(
                        component
                            .mysql_pools
                            .get(name)
                            .ok_or_else(|| {
                                db_error(ApplicationPhase::Start, "MySQL pool is unavailable")
                            })?
                            .clone(),
                    )
                }
                #[cfg(not(feature = "db"))]
                return Err(db_error(
                    ApplicationPhase::Start,
                    "MySQL datasource requires napp feature `db`",
                ));
            }
            DatabaseDriver::PostgreSql => ManagedPool::PostgreSql(
                component
                    .pg_pools
                    .get(name)
                    .ok_or_else(|| {
                        db_error(ApplicationPhase::Start, "PostgreSQL pool is unavailable")
                    })?
                    .clone(),
            ),
        };
        inputs.push(DbMonitorInput { pool, contributor });
    }
    Ok(inputs)
}

struct BootstrapShutdown {
    token: Option<ManagedInstallationToken>,
}

impl ShutdownAction for BootstrapShutdown {
    /// 业务作用：标识尚未完成 catalog 发布的启动权威清理动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定清理标签。
    fn label(&self) -> &'static str {
        "db-bootstrap"
    }

    /// 业务作用：在启动失败或停机时撤销仍未完成的 bootstrap token，释放进程级模式占用。
    ///
    /// 参数说明：`_context` 未参与该进程级身份撤销。
    ///
    /// 返回：幂等完成；token 已消费时不再动作。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(token) = self.token.take() {
                let _ = natx_core::abort_managed_bootstrap(&token);
            }
            Ok(())
        })
    }
}

struct DbRegistryShutdown {
    catalog_owner: Option<ManagedRegistryOwner>,
    #[cfg(feature = "db")]
    mysql: Option<Arc<natx::DataSourceRegistry>>,
    pgsql: Option<Arc<natx_pgsql::PgDataSourceRegistry>>,
}

impl ShutdownAction for DbRegistryShutdown {
    /// 业务作用：标识数据库 catalog 与 typed registry 的统一封口动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定清理标签。
    fn label(&self) -> &'static str {
        "db-registry"
    }

    /// 业务作用：先撤销全局 datasource 权威，再清空同 owner typed registry，阻止新事务进入待关闭 pool。
    ///
    /// 参数说明：`_context` 未参与 owner 身份复验。
    ///
    /// 返回：每个槽只处理一次；旧 owner 不能撤销后来 Application 的资源。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            // catalog 是所有 lookup 的第一道门；先封口可避免 typed pool 关闭期间仍有新事务取得句柄。
            if let Some(owner) = self.catalog_owner.take() {
                let _ = natx_core::clear_managed_catalog(&owner);
            }
            #[cfg(feature = "db")]
            if let Some(registry) = self.mysql.take() {
                natx::clear_managed_registry_slot(&registry);
            }
            if let Some(registry) = self.pgsql.take() {
                natx_pgsql::clear_managed_registry_slot(&registry);
            }
            Ok(())
        })
    }
}

struct DbPoolShutdown {
    name: String,
    pool: Option<ManagedPool>,
}

impl ShutdownAction for DbPoolShutdown {
    /// 业务作用：标识一个已命名数据库池的最终关闭动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定清理标签。
    fn label(&self) -> &'static str {
        "db-pool"
    }

    /// 业务作用：在 registry 已封口后等待指定 pool 排空并释放后端连接。
    ///
    /// 参数说明：`_context` 未改变 SQLx pool 的关闭语义。
    ///
    /// 返回：首次调用完成关闭；重复调用无副作用。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(pool) = self.pool.take() {
                tracing::debug!("closing datasource `{}` pool", self.name);
                pool.close().await;
            }
            Ok(())
        })
    }
}

/// 业务作用：从最终配置读取互斥的单源或多源根，并在任何建连前完成全表结构校验。
///
/// 参数说明：
/// - `application`：提供不可变配置快照。
/// - `phase`：用于保留错误发生的生命周期边界。
///
/// 返回：按名称排序的完整数据源规格；缺失、空表、双根或超限时失败。
fn read_datasources(
    application: &Application,
    phase: ApplicationPhase,
) -> ApplicationResult<Vec<DatasourceSpec>> {
    let snapshot = application.config();
    let root: DbConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            db_error_src(
                phase,
                "invalid database or datasources configuration",
                error,
            )
        })?;
    let values = match (root.database, root.datasources) {
        (Some(_), Some(_)) => {
            return Err(db_error(
                phase,
                "declare either database or datasources, not both",
            ))
        }
        (Some(value), None) => BTreeMap::from([(DEFAULT_DATASOURCE.to_owned(), value)]),
        (None, Some(values)) if !values.is_empty() => values,
        (None, Some(_)) => return Err(db_error(phase, "datasources cannot be empty")),
        (None, None) => {
            return Err(db_error(
                phase,
                "database component requires database or datasources configuration",
            ))
        }
    };
    if values.len() > natx_core::MAX_MANAGED_DATASOURCES {
        return Err(db_error(
            phase,
            "configured datasource count exceeds the managed limit",
        ));
    }
    values
        .into_iter()
        .map(|(name, value)| split_datasource(name, value, phase))
        .collect()
}

/// 业务作用：从 datasource 对象取出可选字符串编排字段，并阻止非字符串值退化为默认配置。
///
/// 参数说明：
/// - `object`：尚未交给具体 driver 反序列化的 datasource 对象。
/// - `datasource`：错误定位使用的 qualifier。
/// - `field`：待取出的 napp 编排字段名。
/// - `phase`：本次配置读取所属生命周期阶段。
///
/// 返回：字段缺席时返回 `None`，字符串值返回其所有权；其它 JSON 类型在任何网络动作前被拒绝。
fn take_optional_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    datasource: &str,
    field: &'static str,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<String>> {
    match object.remove(field) {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(db_error(
            phase,
            format!("datasource `{datasource}` field `{field}` must be a string"),
        )),
    }
}

/// 业务作用：把单个 datasource 配置拆成 driver 专属建池参数和迁移计划。
///
/// 参数说明：
/// - `name`：跨 driver 唯一 qualifier。
/// - `value`：该 datasource 的 YAML 对象。
/// - `phase`：用于构造稳定生命周期错误。
///
/// 返回：driver、URL、schema、topology 与 feature 相互一致时返回规格；不一致时在 I/O 前拒绝。
fn split_datasource(
    name: String,
    mut value: serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<DatasourceSpec> {
    natx_core::DatasourceRef::new(&name)
        .map_err(|error| db_error_src(phase, "invalid datasource qualifier", error))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| db_error(phase, format!("datasource `{name}` must be an object")))?;
    let driver_raw = take_optional_string_field(object, &name, "driver", phase)?;
    let migrations_raw = object.remove("migrations");
    let topology_raw = take_optional_string_field(object, &name, "connection_topology", phase)?;
    let schema_raw = take_optional_string_field(object, &name, "schema", phase)?;
    let schema_was_present = schema_raw.is_some();
    let migration_schema = schema_raw.clone().unwrap_or_else(|| "public".to_owned());
    let url = object
        .get("url")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| db_error(phase, format!("datasource `{name}` url is required")))?;
    let driver = match driver_raw.as_deref() {
        None if url.starts_with("mysql://") => DatabaseDriver::MySql,
        Some("mysql") => DatabaseDriver::MySql,
        Some("postgresql") => DatabaseDriver::PostgreSql,
        None => {
            return Err(db_error(
                phase,
                format!("datasource `{name}` PostgreSQL driver must be explicit"),
            ))
        }
        Some(_) => {
            return Err(db_error(
                phase,
                format!("datasource `{name}` driver is invalid"),
            ))
        }
    };
    let migration = parse_migration_plan(
        &name,
        migrations_raw,
        topology_raw.as_deref(),
        migration_schema,
        driver,
        phase,
    )?;
    let config = match driver {
        DatabaseDriver::MySql => {
            if !url.starts_with("mysql://") {
                return Err(db_error(
                    phase,
                    format!("datasource `{name}` driver and URL scheme do not match"),
                ));
            }
            if topology_raw.is_some() {
                return Err(db_error(
                    phase,
                    format!("datasource `{name}` MySQL does not accept connection_topology"),
                ));
            }
            if schema_was_present {
                return Err(db_error(
                    phase,
                    format!("datasource `{name}` MySQL does not accept schema"),
                ));
            }
            #[cfg(feature = "db")]
            {
                let config = serde_json::from_value::<natx::datasource::DataSourceConfig>(value)
                    .map_err(|error| {
                        db_error_src(
                            phase,
                            format!("invalid MySQL datasource `{name}` configuration"),
                            error,
                        )
                    })?;
                config.validate().map_err(|error| {
                    db_error_src(
                        phase,
                        format!("invalid MySQL datasource `{name}` configuration"),
                        error,
                    )
                })?;
                DriverConfig::MySql(config)
            }
            #[cfg(not(feature = "db"))]
            return Err(db_error(
                phase,
                format!("datasource `{name}` requires napp feature `db`"),
            ));
        }
        DatabaseDriver::PostgreSql => {
            if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
                return Err(db_error(
                    phase,
                    format!("datasource `{name}` driver and URL scheme do not match"),
                ));
            }
            PgConnectionTopology::parse(topology_raw.as_deref())?;
            let config = serde_json::from_value::<natx_pgsql::datasource::DataSourceConfig>(value)
                .map_err(|error| {
                    db_error_src(
                        phase,
                        format!("invalid PostgreSQL datasource `{name}` configuration"),
                        error,
                    )
                })?;
            config.validate().map_err(|error| {
                db_error_src(
                    phase,
                    format!("invalid PostgreSQL datasource `{name}` configuration"),
                    error,
                )
            })?;
            DriverConfig::PostgreSql {
                config,
                schema: schema_raw,
            }
        }
    };
    Ok(DatasourceSpec {
        name,
        driver,
        config,
        migration,
    })
}

/// 业务作用：把 datasource 内的迁移配置冻结为后端可执行计划，并隔离 PostgreSQL session 参数。
///
/// 参数说明：
/// - `datasource`：错误定位使用的 datasource qualifier。
/// - `raw`：可选迁移配置对象。
/// - `topology`：PostgreSQL 连接拓扑配置。
/// - `schema`：PostgreSQL 目标 schema。
/// - `driver`：该 datasource 已确定的后端。
/// - `phase`：用于构造稳定生命周期错误。
///
/// 返回：字段与后端匹配时返回计划；未写 `migrations` 时仍保留 datasource 的 schema/topology，
/// 仅策略使用默认值，否则失败。
fn parse_migration_plan(
    datasource: &str,
    raw: Option<serde_json::Value>,
    topology: Option<&str>,
    schema: String,
    driver: DatabaseDriver,
    phase: ApplicationPhase,
) -> ApplicationResult<Option<MigrationPlan>> {
    let mut raw = raw.unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    let session_url = match raw
        .as_object_mut()
        .and_then(|object| object.remove("session_url"))
    {
        None => None,
        Some(serde_json::Value::String(value)) => Some(value),
        Some(_) => {
            return Err(db_error(
                phase,
                format!(
                    "datasource `{datasource}` field `migrations.session_url` must be a string"
                ),
            ));
        }
    };
    if driver == DatabaseDriver::MySql && session_url.is_some() {
        return Err(db_error(
            phase,
            "MySQL migrations do not accept session_url",
        ));
    }
    let settings = serde_json::from_value(raw)
        .map_err(|error| db_error_src(phase, "invalid datasource migration settings", error))?;
    Ok(Some(MigrationPlan {
        settings,
        schema,
        session_url,
        topology: PgConnectionTopology::parse(topology)?,
    }))
}

/// 业务作用：在组件启动前静态校验配置中的全部 datasource，不建立连接或发布全局 registry。
///
/// 参数说明：
/// - `tree`：最终合并后的 YAML 值。
/// - `phase`：当前校验阶段。
///
/// 返回：没有数据库根或全表合法时成功；任一条目非法则拒绝整个配置。
pub(crate) fn validate_datasource_sections(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let root: DbConfigRoot = serde_json::from_value(tree.clone()).map_err(|error| {
        db_error_src(
            phase,
            "invalid database or datasources configuration",
            error,
        )
    })?;
    let values = match (root.database, root.datasources) {
        (Some(_), Some(_)) => {
            return Err(db_error(
                phase,
                "declare either database or datasources, not both",
            ))
        }
        (Some(value), None) => BTreeMap::from([(DEFAULT_DATASOURCE.to_owned(), value)]),
        (None, Some(values)) if !values.is_empty() => values,
        (None, Some(_)) => return Err(db_error(phase, "datasources cannot be empty")),
        (None, None) => return Ok(()),
    };
    if values.len() > natx_core::MAX_MANAGED_DATASOURCES {
        return Err(db_error(
            phase,
            "configured datasource count exceeds the managed limit",
        ));
    }
    for (name, value) in values {
        split_datasource(name, value, phase)?;
    }
    Ok(())
}

#[cfg(any(feature = "saga", feature = "saga-pgsql"))]
/// 业务作用：在延后 Saga 自举领取权威前确认两种 standalone registry 都为空。
///
/// 参数说明: 无。
///
/// 返回：进程级模式和两套 typed registry 均为空时成功；任何遗留资源都会阻止接管。
fn ensure_deferred_registry_empty() -> ApplicationResult<()> {
    if !matches!(
        natx_core::registry_mode(),
        natx_core::RegistryModeSnapshot::Empty
    ) {
        return Err(db_error(
            ApplicationPhase::Start,
            "deferred database bootstrap requires an empty datasource registry",
        ));
    }
    #[cfg(feature = "db")]
    natx::ensure_standalone_datasources_empty_for_managed_bootstrap().map_err(|error| {
        db_error_src(
            ApplicationPhase::Start,
            "MySQL standalone registry is not empty",
            error,
        )
    })?;
    natx_pgsql::ensure_standalone_datasources_empty_for_managed_bootstrap().map_err(|error| {
        db_error_src(
            ApplicationPhase::Start,
            "PostgreSQL standalone registry is not empty",
            error,
        )
    })?;
    Ok(())
}

#[cfg(feature = "db")]
/// 业务作用：经冻结 catalog 复验 driver 后，从当前 Application 取得命名 MySQL pool。
///
/// 参数说明：
/// - `application`：资源与 catalog 的当前 owner。
/// - `name`：待解析的 datasource qualifier。
///
/// 返回：名称确实绑定 MySQL 时返回 pool；错配、缺失或封口后返回生命周期错误。
pub(crate) async fn datasource_handle(
    application: &Application,
    name: &str,
) -> ApplicationResult<natx::MySqlPool> {
    // 先经过进程级模式机，延后引导与 catalog staging 期间必须返回 RegistryUnavailable，
    // 不能让尚未登记 Application 资源这一内部细节覆盖公开的数据源查找分类。
    natx_core::resolve_datasource(name, DatabaseDriver::MySql).map_err(|error| {
        db_error_src(
            ApplicationPhase::Running,
            "MySQL datasource lookup failed",
            error,
        )
    })?;
    let catalog = application.resource::<Arc<DataSourceCatalog>>().await?;
    catalog
        .resolve(name, DatabaseDriver::MySql)
        .map_err(|error| {
            db_error_src(
                ApplicationPhase::Running,
                "MySQL datasource lookup failed",
                error,
            )
        })?;
    drop(catalog);
    Ok(application
        .named_resource::<natx::MySqlPool>(name)
        .await?
        .clone())
}

static DATASOURCE_BACKEND_INFO: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_datasource_backend_info",
        help: "受管 datasource 与数据库后端的冻结对应关系。",
        unit: "",
        kind: nametrics_core::MetricKind::Gauge,
        label_names: &["datasource", "driver"],
        histogram_bounds: &[],
    };

static DATASOURCE_BACKEND_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 1] =
    [&DATASOURCE_BACKEND_INFO];

struct DatasourceBackendMetricsSource {
    entries: Vec<(String, DatabaseDriver)>,
}

impl nametrics_core::LegacyMetricsSource for DatasourceBackendMetricsSource {
    /// 业务作用：声明 datasource 后端对应关系指标的唯一静态描述符。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含单个低基数 gauge 指标族的目录。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &DATASOURCE_BACKEND_DESCRIPTORS
    }

    /// 业务作用：把冻结 datasource catalog 投影为每个名称恰好一条的后端事实。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终提供结构化 gauge 样本，不读取连接状态或 endpoint。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        Some(
            self.entries
                .iter()
                .map(|(datasource, driver)| nametrics_core::MetricSample {
                    name: DATASOURCE_BACKEND_INFO.name,
                    labels: vec![
                        ("datasource", datasource.clone()),
                        (
                            "driver",
                            match driver {
                                DatabaseDriver::MySql => "mysql",
                                DatabaseDriver::PostgreSql => "postgresql",
                            }
                            .to_owned(),
                        ),
                    ],
                    value: nametrics_core::MetricValue::Gauge(1.0),
                })
                .collect(),
        )
    }

    /// 业务作用：保留旧文本渲染入口，实际文本由统一指标中心从结构化快照生成。
    ///
    /// 参数说明：`_output` 是兼容接口缓冲区，本源不直接写入。
    ///
    /// 返回：无。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：从当前 Application 的冻结 catalog 构造低基数 datasource 后端指标源。
///
/// 参数说明：`catalog` 是已完成资源发布的同 owner catalog。
///
/// 返回：指标源与最坏序列数；每个 datasource 精确对应一个 gauge 序列。
pub(crate) fn backend_metrics_source(
    catalog: &DataSourceCatalog,
) -> (Arc<dyn nametrics_core::LegacyMetricsSource>, usize) {
    let entries = catalog
        .entries()
        .into_iter()
        .map(|(name, driver)| (name.as_str().to_owned(), driver))
        .collect::<Vec<_>>();
    let worst_case_series = entries.len();
    (
        Arc::new(DatasourceBackendMetricsSource { entries }),
        worst_case_series,
    )
}

/// 业务作用：经冻结 catalog 复验 driver 后，从当前 Application 取得命名 PostgreSQL pool。
///
/// 参数说明：
/// - `application`：资源与 catalog 的当前 owner。
/// - `name`：待解析的 datasource qualifier。
///
/// 返回：名称确实绑定 PostgreSQL 时返回 pool；错配、缺失或封口后返回生命周期错误。
pub(crate) async fn pg_datasource_handle(
    application: &Application,
    name: &str,
) -> ApplicationResult<natx_pgsql::PgPool> {
    // staging 期间只允许模式机给出封闭的 RegistryUnavailable，避免资源表的建立顺序泄漏为另一类错误。
    natx_core::resolve_datasource(name, DatabaseDriver::PostgreSql).map_err(|error| {
        db_error_src(
            ApplicationPhase::Running,
            "PostgreSQL datasource lookup failed",
            error,
        )
    })?;
    let catalog = application.resource::<Arc<DataSourceCatalog>>().await?;
    catalog
        .resolve(name, DatabaseDriver::PostgreSql)
        .map_err(|error| {
            db_error_src(
                ApplicationPhase::Running,
                "PostgreSQL datasource lookup failed",
                error,
            )
        })?;
    drop(catalog);
    Ok(application
        .named_resource::<natx_pgsql::PgPool>(name)
        .await?
        .clone())
}

/// 业务作用：为数据库生命周期边界构造不携带底层原因的稳定错误。
///
/// 参数说明：`phase` 是失败阶段，`message` 是脱敏业务原因。
///
/// 返回：归属数据库组件的 Application 错误。
fn db_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Db, phase, message)
}

/// 业务作用：为数据库生命周期边界构造携带内部错误链、公开信息仍脱敏的错误。
///
/// 参数说明：`phase` 是失败阶段，`message` 是稳定原因，`source` 是内部诊断链。
///
/// 返回：归属数据库组件的 Application 错误。
fn db_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Db, phase, message, source)
}
