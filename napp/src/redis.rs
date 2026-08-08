use std::sync::Arc;
use std::time::{Duration, Instant};

use nadis::{RedisClient, RedisConfig};

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ShutdownAction, ShutdownContext,
    StartContext,
};

/// Redis 健康 monitor 的固定 PING 间隔。
const REDIS_MONITOR_INTERVAL: Duration = Duration::from_secs(5);

/// 默认 Redis 实例使用的 qualifier；V1 只支持单实例 `redis` 配置段。
pub(crate) const DEFAULT_REDIS: &str = "default";

/// Redis 组件：用统一客户端建连，并把 `Arc<RedisClient>` 交给资源容器。
///
/// 建连、配置校验、cluster/standalone 探测与协议 profile 都由 `nadis` 负责；组件只做编排与
/// 生命周期挂接。客户端不再另立注册表（nadis 自带的 `RedisRegistry` 有意不用），
/// 资源容器是唯一真理来源。
pub(crate) struct RedisComponent {
    /// Ready 后交由 Runner 按关键任务监督的 Redis 健康 monitor；未建连时为 None。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl RedisComponent {
    /// 业务作用：创建尚未建连的 Redis 组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；客户端与健康 monitor 在 Start 阶段按最终配置建立。
    pub(crate) fn new() -> Self {
        Self {
            critical_task: None,
        }
    }
}

/// 业务作用：Redis 就绪策略:默认**非关键**——Redis 故障使实例 Degraded(仍 200)而非摘流;
/// 会话/锁强依赖 Redis 的部署可自行改为 critical。连续 3 次 PING 失败才降级,一次成功即恢复。
fn redis_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy {
        affects_ready: false,
        failure_threshold: 3,
        recovery_threshold: 1,
        stale_after: None,
    }
}

/// 业务作用：运行期 Redis 健康 monitor:进入 Ready 后按固定间隔 PING 判活并发布就绪观测。
///
/// # 参数
///
/// - `application`:读取全局生命周期状态;进入停机态时发布未就绪并优雅退出。
/// - `client`:受监督的 Redis 客户端(只读 PING,不改数据/拓扑)。
/// - `contributor`:Redis 就绪贡献句柄。
///
/// # 返回
///
/// Application 进入停机态时返回 `Ok(())`;PING 失败发布 Degraded 而非返回错误(非关键依赖)。
async fn run_redis_monitor(
    application: Application,
    client: Arc<RedisClient>,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return Ok(());
            }
            ApplicationState::Starting => {
                tokio::time::sleep(REDIS_MONITOR_INTERVAL).await;
                continue;
            }
            ApplicationState::Ready => {}
        }

        let now = Instant::now();
        match client.ping().await {
            Ok(()) => contributor.observe(DependencyState::Ready, reason::HEALTHY, now),
            // 非关键:PING 失败降级为 Degraded(仍 200),不摘流。
            Err(_) => contributor.observe(DependencyState::Degraded, reason::DEGRADED, now),
        }
        tokio::time::sleep(REDIS_MONITOR_INTERVAL).await;
    }
}

impl ApplicationComponent for RedisComponent {
    /// 业务作用：返回 Redis 组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类 Redis 错误与资源所有者。
    fn id(&self) -> ComponentId {
        ComponentId::Redis
    }

    /// 业务作用：读取最终配置的 `redis` 段并建立客户端。
    ///
    /// 建连成功后先登记资源再压栈清理动作：这样任何后续启动失败都能沿同一条逆序链显式关闭客户端，
    /// 而不是依赖最后一个 `Arc` 何时释放。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置、组件资源登记入口和 active stack 的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let application = context.application().clone();
            let config = read_redis_config(&application)?;
            // 定位信息取 namespace;失败阶段与脱敏 endpoint 由 nadis 的 `ConnectProbe` 错误链带出,
            // 连接串凭据不进 message。
            let namespace = config.namespace.clone();
            let client = RedisClient::connect(config).await.map_err(|error| {
                redis_error_src(
                    ApplicationPhase::Start,
                    format!("redis instance `{namespace}` startup probe failed"),
                    error,
                )
            })?;
            context.register_resource(Some(DEFAULT_REDIS), client.clone())?;

            // 注册就绪贡献项:建连成功即 Ready;运行期由 monitor 周期 PING 刷新(非关键)。
            let contributor = application.register_readiness(
                ComponentId::Redis,
                Arc::<str>::from(format!("redis:{DEFAULT_REDIS}")),
                redis_readiness_policy(),
            )?;
            contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            self.critical_task = Some(Box::pin(run_redis_monitor(
                application,
                client.clone(),
                contributor,
            )));

            context.activate(Box::new(RedisShutdown {
                client: Some(client),
            }));
            Ok(())
        })
    }

    /// 业务作用：取出 Redis 健康 monitor,交由 Runner 按关键任务监督。
    ///
    /// # 返回
    ///
    /// 建连成功后首次调用返回 monitor 任务;未建连或重复调用返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("redis-health-monitor", task))
    }
}

/// 持有 Redis 客户端并在停机时显式调用其 shutdown 的可逆 action。
struct RedisShutdown {
    client: Option<Arc<RedisClient>>,
}

impl ShutdownAction for RedisShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含连接串或命名空间取值。
    fn label(&self) -> &'static str {
        "redis-client"
    }

    /// 业务作用：调用客户端的显式停机，再释放组件侧强引用。
    ///
    /// # 参数
    ///
    /// - `_context`：共享停机预算；该动作只做常数时间通知，不做网络等待。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(client) = self.client.take() {
                client.shutdown().await;
            }
            Ok(())
        })
    }
}

/// 业务作用：从最终配置读取 `redis` 段。
///
/// 段缺失是明确错误而不是默认值：`profile` 与 `namespace` 在 nadis 里无默认，静默兜底只会把
/// 配置缺失推迟成第一条命令的运行期故障。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
fn read_redis_config(application: &Application) -> ApplicationResult<RedisConfig> {
    let snapshot = application.config();
    if snapshot.value().get("redis").is_none() {
        return Err(redis_error(
            ApplicationPhase::Start,
            "component `redis` is declared but the `redis` configuration section is missing",
        ));
    }
    snapshot.section::<RedisConfig>("redis")
}

/// 业务作用：在不建立连接的前提下校验候选配置树中的 `redis` 段。
///
/// 供启动期初始校验与配置热刷新使用；`profile` 必填正是在这一步暴露的。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_redis_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("redis") else {
        return Ok(());
    };
    let config: RedisConfig = serde_json::from_value(section.clone()).map_err(|error| {
        redis_error_src(
            phase,
            "invalid `redis` configuration section (note: `profile` is required, LegacyV1 | RustV2)",
            error,
        )
    })?;
    config
        .validate()
        .map_err(|error| redis_error_src(phase, "invalid `redis` configuration values", error))?;
    //:受管模式(napp 声明 `redis` 组件)拒绝 `command.response_timeout_ms=0`——0 会关掉连接级
    // deadline,Redis 黑洞时建连/`INFO cluster`/命令永久 pending,启动 fail-fast 与运行期超时双双失效。
    // nadis 独立使用是否允许 0 由 nadis 自行决定(escape hatch),napp 编排下不继承这个逃生口。
    if config.command.response_timeout_ms == 0 {
        return Err(redis_error(
            phase,
            "redis.command.response_timeout_ms must be greater than zero in managed mode (0 disables the connection deadline and defeats startup fail-fast)",
        ));
    }
    Ok(())
}

/// 业务作用：按 qualifier 借出一个已注册的 Redis 客户端句柄。
///
/// 返回 `Arc` clone 是该客户端本身的共享语义，不是把资源移出容器。
///
/// # 参数
///
/// - `application`：持有组件资源的共享应用上下文。
/// - `name`：Redis 实例 qualifier；单实例配置固定为 `default`。
pub(crate) async fn redis_handle(
    application: &Application,
    name: &str,
) -> ApplicationResult<Arc<RedisClient>> {
    let client = application.named_resource::<Arc<RedisClient>>(name).await?;
    Ok(client.clone())
}

/// 业务作用：创建 Redis 组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含连接串和口令的稳定摘要。
fn redis_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Redis, phase, message)
}

/// 业务作用：创建带底层错误链的 Redis 错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含连接串和口令的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
fn redis_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Redis, phase, message, source)
}
