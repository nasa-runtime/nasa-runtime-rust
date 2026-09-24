use std::{sync::Arc, time::Duration};

use rest_discovery_nacos::{
    prepare_from_config_with_span_recorder, AppRegistrationInfo, DiscoveryConfig, DiscoverySession,
};
use serde::Deserialize;
use tokio::sync::Mutex;
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
/// - Ready 准备真实监听端口与注册计划，全应用放行后才注册；启动门禁失败时不发布实例；
/// - 停机先摘流，等 Web/用户任务/业务资源 drain 完成后，才关闭出站 runtime 的后台任务。
///
/// 后两条由 active stack 的严格逆序天然保证：注册动作压在 Ready，runtime 动作压在 Start，
/// 中间隔着 business-resources / user-tasks 两个动态步骤。
pub(crate) struct NacosDiscoveryComponent {
    session: Option<Arc<Mutex<DiscoverySession>>>,
    /// 入站服务的就绪贡献句柄：Start 登记，全应用放行后注册实例成功才 observe Ready；
    /// 纯消费者(只出站)不注册,为 None。
    readiness: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl NacosDiscoveryComponent {
    /// 业务作用：创建尚未连接注册中心的服务发现组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；provider 连接发生在 Start 阶段。
    pub(crate) fn new() -> Self {
        Self {
            session: None,
            readiness: None,
            critical_task: None,
        }
    }
}

/// 业务作用：服务发现就绪策略：注册中心尚未确认本实例时保持应用 readiness 为不可用。
/// 参数说明：无。
/// 返回：即时关键依赖策略，未注册或失去就绪证据时不报告应用可用。
fn discovery_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy::critical_immediate()
}

impl ApplicationComponent for NacosDiscoveryComponent {
    /// 业务作用：返回服务发现组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类注册与出站调用相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::NacosDiscovery
    }

    /// 业务作用：连接 provider 并安装出站 RestDiscovery runtime，不注册本实例。
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
            // 入站服务在 Start（seal 前）登记关键就绪贡献项；全应用放行后，远端确认注册才 observe Ready。
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

    /// 业务作用：以预绑定地址冻结注册计划，登记摘流所有权，外部注册延后至全应用放行。
    /// 参数说明：`context` 提供 Application、共享启动截止时刻与 active stack。
    /// 返回：计划与清理权就绪时成功；未登记出站会话或纯消费者不创建注册任务。
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
                registration_bind_ip(application),
                port,
            );
            #[cfg(feature = "grpc")]
            let info = if let Some(endpoint) = application.grpc_runtime().endpoint_registration() {
                // 这里仅取得预绑定事实；真正发布到 provider 前还须确认 listener 已取得接流许可。
                // 元数据由框架封闭生成，业务不能注入未受管端口或 TLS 模式。
                info.with_metadata("nasa.grpc.protocol", "grpc")
                    .with_metadata("nasa.grpc.port", endpoint.port.to_string())
                    .with_metadata("nasa.grpc.tls_mode", endpoint.tls_mode)
                    .with_metadata(
                        "nasa.grpc.authority",
                        endpoint.authority.unwrap_or_default(),
                    )
            } else {
                info
            };
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            let info = {
                let mut info = info;
                for (key, value) in crate::saga::discovery_registration_metadata(
                    application,
                    &registration_bind_ip(application),
                    port,
                )? {
                    info = info.with_metadata(key, value);
                }
                info
            };
            let cancel = CancellationToken::new();
            context.activate(Box::new(NacosDiscoveryRegistrationShutdown {
                session: Some(session.clone()),
                cancel: cancel.clone(),
            }));
            let application = context.application().clone();
            let deadline = context.deadline();
            let contributor = self.readiness.take();
            self.critical_task = Some(Box::pin(async move {
                // 此主体与 listener 共用放行屏障；注册依旧受原启动截止时刻约束，不从此处重置预算。
                tokio::time::timeout_at(deadline.into(), async {
                    #[cfg(feature = "grpc")]
                    if let Ok(observer) = application.grpc() {
                        loop {
                            match observer.state() {
                                nagrpc::GrpcServerState::Running => break,
                                nagrpc::GrpcServerState::Bound => {
                                    tokio::select! {
                                        biased;
                                        _ = cancel.cancelled() => return Ok(()),
                                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                                    }
                                }
                                _ => {
                                    return Err(discovery_error(
                                        ApplicationPhase::Running,
                                        "gRPC endpoint is not serving",
                                    ))
                                }
                            }
                        }
                    }
                    let mut session = session.lock().await;
                    // 摘流 action 先撤销本令牌再锁会话，防止停机期间启动尚未发出的注册。
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    session.register(info).await.map_err(|error| {
                        discovery_error_src(
                            ApplicationPhase::Running,
                            "cannot register this instance with the discovery provider",
                            error,
                        )
                    })
                })
                .await
                .map_err(|_| {
                    discovery_error(
                        ApplicationPhase::Running,
                        "instance registration exceeded its startup budget",
                    )
                })??;
                if let Some(contributor) = contributor {
                    if !cancel.is_cancelled() {
                        contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            Instant::now(),
                        );
                    }
                    run_discovery_monitor(
                        application,
                        session,
                        contributor,
                        config.registration.readiness_critical,
                        cancel,
                    )
                    .await;
                }
                Ok(())
            }));
            Ok(())
        })
    }

    /// 业务作用：将外部注册与健康回查交给统一放行和停机监督，避免 Ready 内部自行产生可发现实例。
    /// 参数说明：无。
    /// 返回：一次性注册/monitor 任务；纯消费者或已经移交时返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("nacos-discovery-registration", task))
    }
}

/// 停机时先摘流的可逆 action。
struct NacosDiscoveryRegistrationShutdown {
    session: Option<Arc<Mutex<DiscoverySession>>>,
    cancel: CancellationToken,
}

impl ShutdownAction for NacosDiscoveryRegistrationShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含服务名或注册地址。
    fn label(&self) -> &'static str {
        "nacos-discovery-registration"
    }

    /// 业务作用：在自身上限与全局剩余预算的较小值内完成摘流。
    ///
    /// 参数说明：`context` 提供全局剩余停机预算。
    /// 返回：先取消后续注册和回查，再摘除已有实例；失败或超时交给统一清理报告，不阻止后续资源收口。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            // 先阻止尚未发出的注册与后续健康回查，再等待会话内的注册完成并摘流。
            self.cancel.cancel();
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

impl Drop for NacosDiscoveryRegistrationShutdown {
    /// 业务作用：生命周期 owner 被直接释放时撤销注册与监控许可，不遗留后置发布动作。
    /// 参数说明：无。
    /// 返回：发布取消信号；受管任务和会话的实际释放由各自所有者完成。
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// 在所有下游调用结束后关闭出站 runtime 的可逆 action。
struct NacosDiscoveryRuntimeShutdown {
    session: Option<Arc<Mutex<DiscoverySession>>>,
}

impl ShutdownAction for NacosDiscoveryRuntimeShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含注册中心地址。
    fn label(&self) -> &'static str {
        "nacos-discovery-runtime"
    }

    /// 业务作用：撤下本实例出站入口并等待它的任务和调用退出。
    /// 参数说明：`context` 为宿主共享停机预算。
    /// 返回：排干后释放会话；未排干报告失败并保留当前 owner。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if let Some(session) = &self.session {
                session
                    .lock()
                    .await
                    .shutdown_runtime_until(tokio::time::Instant::from_std(context.deadline()))
                    .await
                    .map_err(|error| {
                        discovery_error_src(
                            ApplicationPhase::Stopping,
                            "shutting down the outbound discovery runtime failed",
                            error,
                        )
                    })?;
            }
            self.session = None;
            Ok(())
        })
    }
}

/// 业务作用：自注册就绪 monitor:周期回查本实例是否仍在自己服务的健康实例集里,把注册健康反映进 `/readyz`。
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

/// 业务作用：解析注册使用的端口。
///
/// Web 与 gRPC 共存时保留 Web 主端口；纯 gRPC 进程使用 listener 实际端口；其它进程才读取显式端口。
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
    #[cfg(feature = "grpc")]
    if let Some(endpoint) = application.grpc_runtime().endpoint_registration() {
        return Ok(endpoint.port);
    }
    if config.registration.port != 0 {
        return Ok(config.registration.port);
    }
    Err(discovery_error(
        ApplicationPhase::Ready,
        "cannot register without a port: declare `web` or `grpc`, \
         set an explicit `rest_discovery.registration.port`, or disable registration",
    ))
}

/// 业务作用：选择仅用于兼容展示的监听 IP，不参与 provider 的注册 IP 优先级。
///
/// 参数说明：
/// - `application`: 提供 Web 与 gRPC 已发布 listener 的共享上下文。
///
/// 返回：优先返回 Web 绑定 IP；纯 gRPC 返回其绑定 IP；均不存在时返回空串。
fn registration_bind_ip(application: &Application) -> String {
    if let Some(address) = application.web_addr() {
        return address.ip().to_string();
    }
    #[cfg(feature = "grpc")]
    if let Some(endpoint) = application.grpc_runtime().endpoint_registration() {
        return endpoint.authority.unwrap_or_default();
    }
    String::new()
}

/// 业务作用：从最终配置读取 `rest_discovery` 段；段缺失时使用禁用的缺省配置。
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

/// 业务作用：在不连接注册中心的前提下校验候选配置树中的 `rest_discovery` 段。
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

/// 业务作用：创建服务发现组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含注册中心凭据的稳定摘要。
fn discovery_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::NacosDiscovery, phase, message)
}

/// 业务作用：创建带底层错误链的服务发现错误。
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
