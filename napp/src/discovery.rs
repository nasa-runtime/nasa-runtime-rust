use std::{sync::Arc, time::Duration};

use rest_discovery_nacos::{
    prepare_from_config_with_span_recorder, AppRegistrationInfo, DiscoveryConfig, DiscoverySession,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use std::time::Instant;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

/// 摘流允许消耗的最长时间；同时受全局剩余停机预算约束，取两者较小值。
const DEREGISTER_TIMEOUT: Duration = Duration::from_secs(3);

/// 完整配置树中服务发现组件负责读取的顶层投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct DiscoveryConfigRoot {
    rest_discovery: DiscoveryConfig,
}

/// 服务发现组件：把"出站客户端"、"本实例注册"和"关闭客户端后台任务"拆成三个独立生命周期段。
///
/// 三段的时序是这个组件存在的全部理由：
/// - Start 只装配出站 runtime，因此 UserHook 里的建表与预热已经能用 `lb://` 调下游；
/// - Ready 才用真实监听端口注册本实例，Hook 失败时对外从未出现过一个未就绪实例；
/// - 停机先摘流，等 Web/用户任务/业务资源 drain 完成后，才关闭出站 runtime 的后台任务。
///
/// 后两条由 active stack 的严格逆序天然保证：注册动作压在 Ready，runtime 动作压在 Start，
/// 中间隔着 business-resources / user-tasks 两个动态步骤。
pub(crate) struct NacosDiscoveryComponent {
    session: Option<Arc<Mutex<DiscoverySession>>>,
    /// 入站服务的就绪贡献句柄:Start 注册(seal 前),Ready 注册实例成功后 observe Ready;
    /// 纯消费者(只出站)不注册,为 None。
    readiness: Option<ReadinessContributor>,
}

impl NacosDiscoveryComponent {
    /// 创建尚未连接注册中心的服务发现组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；provider 连接发生在 Start 阶段。
    pub(crate) fn new() -> Self {
        Self {
            session: None,
            readiness: None,
        }
    }
}

/// 服务发现就绪策略:入站服务是关键依赖——注册中心未注册成功则不接收流量。
fn discovery_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy::critical_immediate()
}

impl ApplicationComponent for NacosDiscoveryComponent {
    /// 返回服务发现组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类注册与出站调用相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::NacosDiscovery
    }

    /// 连接 provider 并安装出站 RestDiscovery runtime，不注册本实例。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置与 active stack 写入口的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_discovery_config(context.application())?;
            #[cfg(feature = "telemetry")]
            let span_recorder = context.application().span_recorder();
            #[cfg(not(feature = "telemetry"))]
            let span_recorder = None;
            let session = prepare_from_config_with_span_recorder(&config, span_recorder)
                .await
                .map_err(|error| {
                    discovery_error_src(
                        ApplicationPhase::Start,
                        "cannot prepare the outbound discovery runtime",
                        error,
                    )
                })?;
            let session = Arc::new(Mutex::new(session));
            self.session = Some(session.clone());
            // runtime 动作在 Start 压栈：它必须最后清理，位置由压栈顺序而不是特例分支决定。
            context.activate(Box::new(NacosDiscoveryRuntimeShutdown {
                session: Some(session.clone()),
            }));
            if !context
                .application()
                .nacos_discovery_runtime()
                .publish_session(&session)
            {
                return Err(discovery_error(
                    ApplicationPhase::Start,
                    "discovery capability state was already published",
                ));
            }
            // 入站服务在 Start(seal 前)注册关键就绪贡献项;真实注册在 Ready 完成后 observe Ready。
            // 纯消费者(只出站)不注册贡献项——它没有"本实例是否已注册"这一就绪含义。
            if session.lock().await.wants_registration() {
                self.readiness = Some(context.application().register_readiness(
                    ComponentId::NacosDiscovery,
                    Arc::<str>::from("nacos-discovery:self"),
                    discovery_readiness_policy(),
                )?);
            }
            Ok(())
        })
    }

    /// 用真实监听端口注册本实例，并把摘流动作压入清理栈。
    ///
    /// # 参数
    ///
    /// - `context`：提供 Application（含真实监听地址）与 active stack 的 Ready 上下文。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(session) = self.session.clone() else {
                return Ok(());
            };
            let config = read_discovery_config(context.application())?;
            {
                let session = session.lock().await;
                if !session.wants_registration() {
                    // 纯消费者：出站能力已就绪，不注册也不校验注册 IP/端口。
                    return Ok(());
                }
            }

            let application = context.application();
            let port = registration_port(application, &config)?;
            let info = AppRegistrationInfo::new(
                application.info().name(),
                application
                    .web_addr()
                    .map(|address| address.ip().to_string())
                    .unwrap_or_default(),
                port,
            );
            session.lock().await.register(info).await.map_err(|error| {
                discovery_error_src(
                    ApplicationPhase::Ready,
                    "cannot register this instance with the discovery provider",
                    error,
                )
            })?;
            context.activate(Box::new(NacosDiscoveryRegistrationShutdown {
                session: Some(session.clone()),
            }));
            // 本实例已注册到发现中心 → 关键就绪贡献项发布 Ready(句柄在 Start 已注册);
            // 交给 monitor 周期回查本实例是否仍在自己服务的健康实例集里,运行期反映注册健康。
            // monitor 由随后压栈的 action 拥有停机:它压在注册摘流 action 之后,故逆序停机时先取消 monitor、
            // 再摘流——monitor 不会在我们主动 deregister 期间误判"本实例已消失"。
            if let Some(contributor) = self.readiness.take() {
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                let application = context.application().clone();
                let critical = config.registration.readiness_critical;
                let monitor_cancel = CancellationToken::new();
                let monitor = tokio::spawn(run_discovery_monitor(
                    application,
                    session,
                    contributor,
                    critical,
                    monitor_cancel.clone(),
                ));
                context.activate(Box::new(NacosDiscoveryMonitorShutdown {
                    cancel: monitor_cancel,
                    monitor: Some(monitor),
                }));
            }
            Ok(())
        })
    }
}

/// 停机时先摘流的可逆 action。
struct NacosDiscoveryRegistrationShutdown {
    session: Option<Arc<Mutex<DiscoverySession>>>,
}

impl ShutdownAction for NacosDiscoveryRegistrationShutdown {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含服务名或注册地址。
    fn label(&self) -> &'static str {
        "nacos-discovery-registration"
    }

    /// 在自身上限与全局剩余预算的较小值内完成摘流。
    ///
    /// # 参数
    ///
    /// - `context`：提供全局剩余停机预算的清理上下文。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(session) = self.session.take() else {
                return Ok(());
            };
            let budget = DEREGISTER_TIMEOUT.min(context.remaining());
            match tokio::time::timeout(budget, async { session.lock().await.deregister().await })
                .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(discovery_error_src(
                    ApplicationPhase::Stopping,
                    "deregistering this instance failed",
                    error,
                )),
                Err(_) => Err(discovery_error(
                    ApplicationPhase::Stopping,
                    "deregistering this instance exceeded its shutdown budget",
                )),
            }
        })
    }
}

/// 在所有下游调用结束后关闭出站 runtime 的可逆 action。
struct NacosDiscoveryRuntimeShutdown {
    session: Option<Arc<Mutex<DiscoverySession>>>,
}

impl ShutdownAction for NacosDiscoveryRuntimeShutdown {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含注册中心地址。
    fn label(&self) -> &'static str {
        "nacos-discovery-runtime"
    }

    /// 从进程级槽取下并关闭出站客户端后台任务。
    ///
    /// # 参数
    ///
    /// - `_context`：共享停机预算；该动作只做取下与 abort，不做网络等待。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(session) = self.session.take() {
                session
                    .lock()
                    .await
                    .shutdown_runtime()
                    .await
                    .map_err(|error| {
                        discovery_error_src(
                            ApplicationPhase::Stopping,
                            "shutting down the outbound discovery runtime failed",
                            error,
                        )
                    })?;
            }
            Ok(())
        })
    }
}

/// 自注册就绪 monitor:周期回查本实例是否仍在自己服务的健康实例集里,把注册健康反映进 `/readyz`。
///
/// 调用 [`DiscoverySession::self_registration_healthy`](rest_discovery_nacos::DiscoverySession::self_registration_healthy)
/// 向注册中心查询本服务的可用实例集(已过滤健康/启用/正权重),按注册身份 `(ip, port)` 定位本实例:
/// - 在册且健康 → Ready(HEALTHY);
/// - 不在册(心跳滞后/短暂驱逐)→ **Degraded**(NOT_READY):本地注册 guard 仍持有、SDK 持续心跳会自愈,
///   surface 异常但不摘流,避免注册表抖动把关键依赖打成 503(与 web-mapping monitor 同哲学);
/// - 回查本身失败(provider 抖动)→ **Degraded**(PROBE_TIMEOUT):探测失败不等于本实例失联,不升级为摘流。
///
/// 进入停机态即优雅退出。只读注册中心、低频执行,绝不从 `/readyz` handler 直接回查远程。
///
/// # 参数
///
/// - `application`:读取全局生命周期状态,停机即退出。
/// - `session`:出站/注册会话;`self_registration_healthy` 需其保存的注册身份与 provider 客户端。
/// - `contributor`:自注册就绪贡献句柄。
/// - `critical`:确认失联是否升级为关键(`rest_discovery.registration.readiness_critical`):`true` → 置
///   NotReady(→503),`false` → 置 Degraded(不摘流)。回查失败(失联未确认)始终 Degraded,不受此旗影响。
/// - `cancel`:停机取消令牌。
async fn run_discovery_monitor(
    application: Application,
    session: Arc<Mutex<DiscoverySession>>,
    contributor: ReadinessContributor,
    critical: bool,
    cancel: CancellationToken,
) {
    /// 自注册回查周期。
    const INTERVAL: Duration = Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(INTERVAL) => {}
        }
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                return;
            }
            ApplicationState::Starting => continue,
            ApplicationState::Ready => {}
        }
        let health = session.lock().await.self_registration_healthy().await;
        match health {
            // 本实例仍在自己服务的健康实例集 → 注册健康。
            Ok(Some(true)) => {
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            }
            // 确认失联:默认非摘流降级(SDK 心跳会自愈、本地 guard 仍持有,surface 不 503);
            // `readiness_critical` 时升级为 NotReady(affects_ready 关键 → `/readyz` 503,交编排替换)。
            Ok(Some(false)) => {
                let state = if critical {
                    DependencyState::NotReady
                } else {
                    DependencyState::Degraded
                };
                contributor.observe(state, reason::NOT_READY, Instant::now());
            }
            // 纯消费者不会 spawn 本 monitor;真无注册身份则无可观测,跳过本轮。
            Ok(None) => {}
            // 回查失败(provider 抖动)→ 探测失败降级,不升级为本实例失联。
            Err(_error) => {
                contributor.observe(
                    DependencyState::Degraded,
                    reason::PROBE_TIMEOUT,
                    Instant::now(),
                );
            }
        }
    }
}

/// 持有自注册就绪 monitor 的可逆停机 action:取消并在全局剩余预算内 join(辅助任务,超时不阻断其余清理)。
struct NacosDiscoveryMonitorShutdown {
    /// monitor 停机取消令牌。
    cancel: CancellationToken,
    /// spawned monitor 句柄;只 join 一次。
    monitor: Option<JoinHandle<()>>,
}

impl ShutdownAction for NacosDiscoveryMonitorShutdown {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数;名称不含注册中心地址。
    fn label(&self) -> &'static str {
        "nacos-discovery-monitor"
    }

    /// 取消 monitor 并在全局剩余停机预算内 join;超时不阻断其余清理(辅助任务)。
    ///
    /// # 参数
    ///
    /// - `context`:提供全局剩余停机预算的清理上下文。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.cancel.cancel();
            let Some(monitor) = self.monitor.as_mut() else {
                return Ok(());
            };
            if tokio::time::timeout(context.remaining(), monitor)
                .await
                .is_err()
            {
                let monitor = self
                    .monitor
                    .take()
                    .expect("discovery monitor remains installed while shutdown awaits it");
                monitor.abort();
                let _ = monitor.await;
            } else {
                self.monitor.take();
            }
            Ok(())
        })
    }
}

impl Drop for NacosDiscoveryMonitorShutdown {
    /// 停机 future 被取消或句柄提前释放时终止 monitor，避免后台注册任务脱离生命周期。
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(monitor) = self.monitor.take() {
            monitor.abort();
        }
    }
}

/// 解析注册使用的端口。
///
/// 有 Web 时必须用 `web_addr()` 的真实端口——`server.port=0` 场景下配置里的 0 不是可拨号端口；
/// 无 Web 时只能由配置显式给出非 0 端口，否则给定向错误而不是注册一个不可达实例。
///
/// # 参数
///
/// - `application`：提供真实监听地址的共享上下文。
/// - `config`：已读取的服务发现配置。
fn registration_port(
    application: &Application,
    config: &DiscoveryConfig,
) -> ApplicationResult<u16> {
    if let Some(address) = application.web_addr() {
        return Ok(address.port());
    }
    if config.registration.port != 0 {
        return Ok(config.registration.port);
    }
    Err(discovery_error(
        ApplicationPhase::Ready,
        "cannot register without a port: declare the `web` component, \
         set an explicit `rest_discovery.registration.port`, or disable registration",
    ))
}

/// 从最终配置读取 `rest_discovery` 段；段缺失时使用禁用的缺省配置。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
fn read_discovery_config(application: &Application) -> ApplicationResult<DiscoveryConfig> {
    let snapshot = application.config();
    let root: DiscoveryConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            discovery_error_src(
                ApplicationPhase::Start,
                "invalid `rest_discovery` configuration section",
                error,
            )
        })?;
    Ok(root.rest_discovery)
}

/// 在不连接注册中心的前提下校验候选配置树中的 `rest_discovery` 段。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_discovery_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("rest_discovery") else {
        return Ok(());
    };
    serde_json::from_value::<DiscoveryConfig>(section.clone())
        .map(|_| ())
        .map_err(|error| {
            discovery_error_src(
                phase,
                "invalid `rest_discovery` configuration section",
                error,
            )
        })
}

/// 创建服务发现组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含注册中心凭据的稳定摘要。
fn discovery_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::NacosDiscovery, phase, message)
}

/// 创建带底层错误链的服务发现错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含注册中心凭据的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
fn discovery_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::NacosDiscovery, phase, message, source)
}
