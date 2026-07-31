use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use natx::{
    datasource::{build_pool, probe, DataSourceConfig},
    MySqlPool,
};
use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ShutdownAction, ShutdownContext,
    StartContext,
};

/// DB 健康 monitor 的固定探测间隔。从既有池 acquire 一条连接(触发 sqlx 的 test_before_acquire
/// ping)即可判活,不占用业务连接。
const DB_MONITOR_INTERVAL: Duration = Duration::from_secs(5);

/// 默认数据源使用的 qualifier；单库配置 `database` 与多库配置里的同名条目共用它。
pub(crate) const DEFAULT_DATASOURCE: &str = "default";

/// 完整配置树中数据源组件负责读取的两种顶层写法。
///
/// 两种写法只允许出现一种：同时出现时无法确定 `default` 该取谁，隐式合并会在多库场景下
/// 静默改变事务默认库，因此按 直接报冲突。
///
/// 两种写法都以**原始 JSON 值**接收:`migrations` 是 napp 编排字段而非 natx
/// 数据源字段,而 `DataSourceConfig` 是 `deny_unknown_fields`,因此必须先把 `migrations` 从每个
/// 数据源对象里剥离(见 [`split_datasource`]),再反序列化成 `DataSourceConfig`。
#[derive(Default, Deserialize)]
#[serde(default)]
struct DbConfigRoot {
    database: Option<serde_json::Value>,
    datasources: Option<BTreeMap<String, serde_json::Value>>,
}

/// MySQL 数据源组件：启动期探测连通性、建池，并把池同时交给资源容器与事务运行时。
///
/// 每个池的清理所有权由建池后立即激活的 `ShutdownAction` 持有：它显式 `close().await`，因此不依赖
/// 最后一个共享句柄何时被 drop。资源容器条目由 Runner 的组件资源步骤在同一逆序链移除。
pub(crate) struct DbComponent {
    /// Ready 后交由 Runner 按关键任务监督的唯一 DB 健康 monitor；无数据源时为 None。
    critical_task: Option<ApplicationFuture<'static>>,
    /// Start 建好的各数据源池;供 Ready 迁移门禁按名查找。
    ///
    /// 不经资源容器读取:资源在 UserHook 后已封口,直接用 Start 已持有的 clone 更直观且不与容器
    /// 读取语义耦合。池的清理所有权仍在各自的 `DbShutdown` action,本表只是只读借道。
    pools: BTreeMap<String, MySqlPool>,
    /// 各数据源**显式**配置的迁移门禁设置;未显式配置的数据源在门禁时用默认(生产 `validate`)。
    migration_settings: BTreeMap<String, namigrate::MigrationSettings>,
}

impl DbComponent {
    /// 创建尚未读取配置的数据源组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；连接池与健康 monitor 在 Start 阶段按最终配置创建。
    pub(crate) fn new() -> Self {
        Self {
            critical_task: None,
            pools: BTreeMap::new(),
            migration_settings: BTreeMap::new(),
        }
    }
}

/// 单个数据源健康 monitor 的不可变输入。
struct DbMonitorInput {
    pool: MySqlPool,
    contributor: ReadinessContributor,
}

/// DB 就绪策略:关键依赖,连续 3 次 acquire 失败才摘流(容忍瞬时抖动),一次成功即恢复。
///
/// 不设 stale 窗:monitor 是关键任务,其意外退出由 Runner 判为启动/运行失败并停机,不需 stale 兜底。
fn db_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy {
        affects_ready: true,
        failure_threshold: 3,
        recovery_threshold: 1,
        stale_after: None,
    }
}

/// 运行期 DB 健康 monitor:进入 Ready 后按固定间隔从各池 acquire 判活并发布就绪观测。
///
/// # 参数
///
/// - `application`：用于读取全局生命周期状态；进入 Stopping/Stopped/Failed 时发布未就绪并优雅退出。
/// - `inputs`：各数据源的池与就绪贡献句柄。
///
/// # 返回
///
/// Application 进入停机态时返回 `Ok(())`；本 monitor 不产生错误退出(acquire 失败发布 NotReady 而非返回)。
async fn run_db_monitor(
    application: Application,
    inputs: Vec<DbMonitorInput>,
) -> ApplicationResult<()> {
    loop {
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
                tokio::time::sleep(DB_MONITOR_INTERVAL).await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let now = Instant::now();
        for input in &inputs {
            // acquire 成功即触发 sqlx test_before_acquire ping;连接立即归还池,不占业务连接。
            match input.pool.acquire().await {
                Ok(_conn) => {
                    input
                        .contributor
                        .observe(DependencyState::Ready, reason::HEALTHY, now)
                }
                Err(_) => {
                    input
                        .contributor
                        .observe(DependencyState::NotReady, reason::PROBE_TIMEOUT, now)
                }
            }
        }
        tokio::time::sleep(DB_MONITOR_INTERVAL).await;
    }
}

impl ApplicationComponent for DbComponent {
    /// 返回数据源组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类数据源相关错误与资源所有者。
    fn id(&self) -> ComponentId {
        ComponentId::Db
    }

    /// 使用最终配置逐个数据源探测、建池并完成双写注册。
    ///
    /// 顺序固定为"校验 → 探测 → 建池 → 注册资源 → 注入事务运行时 → 压栈清理动作"：探测在建池之前，
    /// 启动期才能看到 `Access denied`/`Unknown database` 这类真实原因而不是模糊的 acquire timeout；
    /// 清理动作在池注册成功后立刻压栈，任一后续数据源失败都能把已建好的池显式关掉。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置、组件资源登记入口和 active stack 的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let sources = read_datasources(&application)?;
            let mut monitor_inputs: Vec<DbMonitorInput> = Vec::new();
            for (name, config, migrations) in sources {
                config.validate().map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        format!("invalid datasource `{name}` configuration"),
                        error,
                    )
                })?;
                //:无条件单连接握手探针(`probe_on_start=false` 已在配置校验期被 `split_datasource`
                // 拒绝),先于建池——启动期即可拿到 `Access denied`/`Unknown database`/`Connection refused`
                // 这类真实根因,而非模糊的 acquire timeout。endpoint() 只含 host:port/database,凭据留在
                // url 里不进 message;底层 sqlx 错误经统一 report 管道脱敏后展开。
                probe(&config).await.map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        format!(
                            "datasource `{name}` probe failed during handshake on {}",
                            config.endpoint()
                        ),
                        error,
                    )
                })?;
                let pool = build_pool(&config).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        format!(
                            "cannot create datasource `{name}` pool for {}",
                            config.endpoint()
                        ),
                        error,
                    )
                })?;

                context.register_resource(Some(name.as_str()), pool.clone())?;
                // 资源登记之后立刻压入显式关池动作。后续事务运行时双写仍可能失败，提前压栈才能让
                // 该半初始化分支也执行 `close().await`，而不只依赖资源条目被动释放最后一个句柄。
                context.activate(Box::new(DbShutdown {
                    name: name.clone(),
                    pool: Some(pool.clone()),
                }));
                // 双写事务运行时：`#[transactional]` 与 Mapper 仍按名字查进程级注册表，
                // 因此业务代码零改动即可继续工作。
                natx::try_init_datasource(name.clone(), pool.clone()).map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Start,
                        format!("cannot register datasource `{name}` with the transaction runtime"),
                        error,
                    )
                })?;

                // 注册就绪贡献项:启动探测已验证连通,首个观测即 Ready;运行期由 monitor 周期刷新。
                let contributor = application.register_readiness(
                    ComponentId::Db,
                    Arc::<str>::from(format!("db:{name}")),
                    db_readiness_policy(),
                )?;
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                monitor_inputs.push(DbMonitorInput {
                    pool: pool.clone(),
                    contributor,
                });

                // 供 Ready 迁移门禁使用:池按名留一份 clone,显式配置的门禁设置按名留档
                // (未配置的数据源在门禁时回退默认 validate)。
                self.pools.insert(name.clone(), pool.clone());
                if let Some(settings) = migrations {
                    self.migration_settings.insert(name.clone(), settings);
                }
            }

            // 有数据源时构建唯一健康 monitor,交由 Runner 在 Ready 阶段按关键任务监督。
            if !monitor_inputs.is_empty() {
                self.critical_task = Some(Box::pin(run_db_monitor(application, monitor_inputs)));
            }
            Ok(())
        })
    }

    /// 在监听器就绪之前运行各数据源的 migration 门禁。
    ///
    /// DB 声明序在 `web`/`kafka` 之前,故本阶段早于任何监听器接流、任何 Kafka consumer 启动。逐个取出
    /// UserHook 经 `configure_migrations` 登记的 `(数据源, migrator)`,按数据源配置的
    /// `database.migrations.mode`(未配置回退 `validate`)调用 [`namigrate::run_gate`]:validate 只读
    /// 校验版本/checksum 一致、apply 应用未决 migration。未应用、checksum 漂移或后端错误直接 fail
    /// startup(错误只含版本与稳定 reason,无 SQL 正文)。指向未配置数据源的登记同样拒绝启动。
    ///
    /// 没有任何登记项时本阶段是无副作用的空操作,不触碰数据库。
    ///
    /// # 参数
    ///
    /// - `context`：提供 Application(据此取走迁移登记队列)的 Ready 上下文。
    fn ready<'a>(&'a mut self, context: &'a mut crate::ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let registrations = application.take_migrations();
            for (name, migrator) in &registrations {
                let pool = self.pools.get(name).ok_or_else(|| {
                    db_error(
                        ApplicationPhase::Ready,
                        format!(
                            "configure_migrations registered datasource `{name}`, but it is not a configured datasource"
                        ),
                    )
                })?;
                // 未显式配置门禁的数据源回退默认 MigrationSettings(生产 validate);登记了 migrator 即
                // 表示业务要求版本一致,默认只读校验最安全。
                let settings = self
                    .migration_settings
                    .get(name)
                    .cloned()
                    .unwrap_or_default();
                let report = namigrate::run_gate(pool, migrator, &settings)
                    .await
                    .map_err(|error| {
                        // MigrationError 的 Display 已脱敏(只含版本与稳定 reason,无 SQL);不接源链,
                        // 避免底层错误正文经统一报告管道泄露。
                        db_error(
                            ApplicationPhase::Ready,
                            format!("datasource `{name}` migration gate failed: {error}"),
                        )
                    })?;
                tracing::info!(
                    "datasource `{name}` migration gate passed: mode={:?} embedded={} applied={}",
                    report.mode,
                    report.embedded,
                    report.applied
                );
            }
            Ok(())
        })
    }

    /// 取出唯一 DB 健康 monitor,交由 Runner 按关键任务监督。
    ///
    /// # 返回
    ///
    /// 有数据源时首次调用返回 monitor 任务;无数据源或重复调用返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("db-health-monitor", task))
    }
}

/// 持有单个连接池并在停机时显式关闭它的可逆 action。
struct DbShutdown {
    name: String,
    pool: Option<MySqlPool>,
}

impl ShutdownAction for DbShutdown {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含连接串或凭据。
    fn label(&self) -> &'static str {
        "db-pool"
    }

    /// 关闭连接池并等待已借出的连接归还。
    ///
    /// # 参数
    ///
    /// - `_context`：共享停机预算；`close()` 的等待由 Runner 的整体 timeout 约束。
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

/// 从最终配置解析出本进程要创建的全部数据源。
///
/// 返回值按名称排序，保证多库启动顺序、错误顺序和停机逆序在同一份配置下完全确定。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
#[allow(clippy::type_complexity)]
fn read_datasources(
    application: &Application,
) -> ApplicationResult<
    Vec<(
        String,
        DataSourceConfig,
        Option<namigrate::MigrationSettings>,
    )>,
> {
    let snapshot = application.config();
    let root: DbConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            db_error_src(
                ApplicationPhase::Start,
                "invalid `database`/`datasources` configuration section",
                error,
            )
        })?;

    match (root.database, root.datasources) {
        (Some(_), Some(_)) => Err(db_error(
            ApplicationPhase::Start,
            "configuration conflict: declare either `database` or `datasources`, not both",
        )),
        (Some(database), None) => {
            let (config, migrations) =
                split_datasource(database, DEFAULT_DATASOURCE, ApplicationPhase::Start)?;
            Ok(vec![(DEFAULT_DATASOURCE.to_owned(), config, migrations)])
        }
        (None, Some(datasources)) => {
            if datasources.is_empty() {
                return Err(db_error(
                    ApplicationPhase::Start,
                    "`datasources` is declared but empty; remove it or declare at least one datasource",
                ));
            }
            let mut out = Vec::with_capacity(datasources.len());
            for (name, value) in datasources {
                let (config, migrations) = split_datasource(value, &name, ApplicationPhase::Start)?;
                out.push((name, config, migrations));
            }
            Ok(out)
        }
        (None, None) => Err(db_error(
            ApplicationPhase::Start,
            "component `db` is declared but neither `database` nor `datasources` is configured",
        )),
    }
}

/// 从单个数据源配置对象里剥离 napp 编排字段 `migrations`,返回 (纯数据源配置, 可选门禁设置)。
///
/// `DataSourceConfig` 是 `deny_unknown_fields`,内嵌的 `migrations` 会让它整体解析失败,因此必须在
/// 反序列化前把该键摘出。摘出后的剩余对象仍按 `deny_unknown_fields` 严格校验,数据源字段本身的拼写
/// 错误不受影响;`MigrationSettings` 自身同样 `deny_unknown_fields`,其内部拼写错误在此处即被拒绝。
///
/// # 参数
///
/// - `value`：配置树中某个数据源对象(`database` 或 `datasources.<name>` 的值)。
/// - `name`：数据源 qualifier,仅用于错误信息定位。
/// - `phase`：本次读取/校验所属生命周期阶段。
fn split_datasource(
    mut value: serde_json::Value,
    name: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<(DataSourceConfig, Option<namigrate::MigrationSettings>)> {
    let migrations_value = value
        .as_object_mut()
        .and_then(|map| map.remove("migrations"));
    let migrations = match migrations_value {
        Some(raw) => {
            let settings =
                serde_json::from_value::<namigrate::MigrationSettings>(raw).map_err(|error| {
                    db_error_src(
                        phase,
                        format!("invalid `migrations` configuration for datasource `{name}`"),
                        error,
                    )
                })?;
            settings.validate().map_err(|error| {
                db_error(
                    phase,
                    format!("invalid `migrations` configuration for datasource `{name}`: {error}"),
                )
            })?;
            Some(settings)
        }
        None => None,
    };
    let config = serde_json::from_value::<DataSourceConfig>(value).map_err(|error| {
        db_error_src(
            phase,
            format!("invalid datasource `{name}` configuration"),
            error,
        )
    })?;
    //:napp 声明 `db` 即**强制**启动握手探针,不继承 natx 的 `probe_on_start` 逃生口——否则错误
    // 地址仍能建 lazy pool、Application 误入 Ready,直到首条业务 SQL 才暴露 pool timeout。natx 默认 true,
    // 故最终值为 false ⟺ 配置里**显式**写了 `probe_on_start: false`:在配置校验期直接拒绝(不静默改 true)。
    // 本 choke point 同时覆盖 Start 期 `read_datasources` 与配置发布期 `validate_datasource_sections`。
    if !config.probe_on_start {
        return Err(db_error(
            phase,
            format!(
                "datasource `{name}` sets probe_on_start=false, which napp forbids; the startup handshake probe is mandatory. Remove the field to keep it enabled."
            ),
        ));
    }
    Ok((config, migrations))
}

/// 在不建立任何连接的前提下校验候选配置树中的数据源段。
///
/// 供启动期初始校验与配置热刷新使用：只做结构反序列化与取值校验，冲突（同时写 `database` 和
/// `datasources`）在这里就会被拒绝，不会等到建池时才发现。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_datasource_sections(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let root: DbConfigRoot = serde_json::from_value(tree.clone()).map_err(|error| {
        db_error_src(
            phase,
            "invalid `database`/`datasources` configuration section",
            error,
        )
    })?;
    match (root.database, root.datasources) {
        (Some(_), Some(_)) => Err(db_error(
            phase,
            "configuration conflict: declare either `database` or `datasources`, not both",
        )),
        (Some(database), None) => {
            // 剥离并校验 `migrations`(deny_unknown_fields),再对纯数据源配置做取值校验。
            let (config, _migrations) = split_datasource(database, DEFAULT_DATASOURCE, phase)?;
            config.validate().map_err(|error| {
                db_error_src(phase, "invalid datasource `default` configuration", error)
            })
        }
        (None, Some(datasources)) => {
            for (name, value) in datasources {
                let (config, _migrations) =
                    split_datasource(value, &name, ApplicationPhase::Running)?;
                config.validate().map_err(|error| {
                    db_error_src(
                        ApplicationPhase::Running,
                        format!("invalid datasource `{name}` configuration"),
                        error,
                    )
                })?;
            }
            Ok(())
        }
        (None, None) => Ok(()),
    }
}

/// 按 qualifier 借出一个已注册的数据源连接池句柄。
///
/// 返回的是池的 clone handle——这是 `MySqlPool` 自身的共享语义，不是把资源所有权交出容器。
///
/// # 参数
///
/// - `application`：持有组件资源的共享应用上下文。
/// - `name`：数据源 qualifier；单库配置固定为 `default`。
pub(crate) async fn datasource_handle(
    application: &Application,
    name: &str,
) -> ApplicationResult<MySqlPool> {
    let pool = application.named_resource::<MySqlPool>(name).await?;
    Ok(pool.clone())
}

/// 创建数据源组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：只含 qualifier 与 host/port/database 等定位信息的摘要。
fn db_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Db, phase, message)
}

/// 创建带底层错误链的数据源错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含连接串的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
fn db_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Db, phase, message, source)
}
