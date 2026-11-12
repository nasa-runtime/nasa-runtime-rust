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
    /// 出站发现与自注册配置，缺段时采用禁用发现的默认设置。
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
    /// Start 已装配的共享会话，注册和运行时关闭动作按各自阶段借用。
    session: Option<Arc<Mutex<DiscoverySession>>>,
    /// 入站服务的就绪贡献句柄：Start 登记，全应用放行后注册实例成功才 observe Ready；
    /// 纯消费者(只出站)不注册,为 None。
    readiness: Option<ReadinessContributor>,
    /// Ready 准备、等待 Runner 放行的注册及后续回查任务，只移交一次。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl NacosDiscoveryComponent {
    /// 业务作用：创建尚未连接注册中心的服务发现组件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：没有会话、就绪贡献和注册任务的组件；不读取配置、连接 provider 或发布实例。
    pub(crate) fn new() -> Self {
        Self {
            session: None,
            readiness: None,
            critical_task: None,
        }
    }
}

/// 业务作用：服务发现就绪策略：注册中心尚未确认本实例时保持应用 readiness 为不可用。
///
/// 参数说明：无。
///
/// 返回：即时关键依赖策略，未注册或失去就绪证据时不报告应用可用。
fn discovery_readiness_policy() -> ReadinessPolicy {
    ReadinessPolicy::critical_immediate()
}

impl ApplicationComponent for NacosDiscoveryComponent {
    /// 业务作用：返回服务发现组件稳定身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 ComponentId::NacosDiscovery，供生命周期排序和错误归类，不反映远端注册是否成功。
    fn id(&self) -> ComponentId {
        ComponentId::NacosDiscovery
    }

    /// 业务作用：准备出站 RestDiscovery 运行时并托管会话；启用发现时连接 provider，禁用时保留普通 HTTP 出站能力。
    ///
    /// 参数说明：
    ///
    /// - `context`：提供最终配置与 active stack 写入口的 Start 上下文。
    ///
    /// 返回：会话和关闭动作已登记、能力入口发布成功后完成；要求自注册时同时预登记就绪贡献，但不发布远端实例。
    /// 配置、连接、能力重复发布或就绪登记失败时返回 Start 错误，已激活的运行时由 Runner 统一清理。
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
    ///
    /// 参数说明：
    /// - `context`：提供预绑定监听事实、固定启动截止点及摘流动作的清理栈。
    ///
    /// 返回：注册信息、取消令牌和摘流动作准备好后保存受监督任务；成功不代表远端已注册，实际任务仍受原启动截止点限制。
    /// 无出站会话或不要求注册时直接成功；端口、配置或安全元数据不满足要求时拒绝准备，尚不向 provider 发布实例。
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
                    None,
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
                                        _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                                    }
                                }
                                _ => {
                                    return Err(discovery_error(
                                        ApplicationPhase::Running,
                                        "gRPC endpoint is not serving",
                                    ));
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：一次性注册/monitor 任务；纯消费者或已经移交时返回 None。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("nacos-discovery-registration", task))
    }
}

/// 停机时先摘流的可逆 action。
struct NacosDiscoveryRegistrationShutdown {
    /// 尚未移交摘流流程的会话引用，取出后本动作不自动重试远端删除。
    session: Option<Arc<Mutex<DiscoverySession>>>,
    /// 通知尚未发出的注册及轮次间回查停止，不代表远端操作已经结束。
    cancel: CancellationToken,
}

impl ShutdownAction for NacosDiscoveryRegistrationShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 nacos-discovery-registration 标签，标识停机摘流步骤，不含服务名或注册地址。
    fn label(&self) -> &'static str {
        "nacos-discovery-registration"
    }

    /// 业务作用：在自身上限与全局剩余预算的较小值内完成摘流。
    ///
    /// 参数说明：
    /// - `context`：提供全局剩余停机预算，摘流等待取其与三秒局部上限的较小值。
    ///
    /// 返回：先通知停止后续注册与回查，再等待会话锁并摘流；未持有会话或摘流成功时完成，锁等待或调用超时也计入同一预算。
    /// 失败或超时返回 Stopping 错误，不证明远端实例已删除；本动作已取出的会话不会在后续调用中自动重试。
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
    /// 业务作用：摘流动作被直接释放时发出取消通知，使尚未开始的注册与后续回查能够停止。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅发布取消信号，不等待在途注册或回查退出，也不执行远端摘流；任务结束及会话释放仍由宿主监督和各自所有者负责。
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// 在所有下游调用结束后关闭出站 runtime 的可逆 action。
struct NacosDiscoveryRuntimeShutdown {
    /// 负责出站能力关闭与排空的会话，成功后才从本动作移除。
    session: Option<Arc<Mutex<DiscoverySession>>>,
}

impl ShutdownAction for NacosDiscoveryRuntimeShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 nacos-discovery-runtime 标签，标识出站运行时清理步骤，不含注册中心地址或凭据。
    fn label(&self) -> &'static str {
        "nacos-discovery-runtime"
    }

    /// 业务作用：撤下本实例出站入口并等待它的任务和调用退出。
    ///
    /// 参数说明：
    /// - `context`：提供宿主固定停机截止点，出站任务排空不得从此重新起算等待时间。
    ///
    /// 返回：撤下属于本实例的出站入口并完成任务排空后清空会话；错误时保留本动作的会话引用以便后续处置。
    /// 异步锁等待由外层 Runner 的预算约束，底层排空使用同一截止点；动作最终释放不等于远端注销已成功。
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

/// 业务作用：周期查询本实例在注册中心健康实例集中的状态，将结果提交给应用就绪聚合，避免探针请求路径直接调用远端。
///
/// 参数说明：
/// - `application`：每轮查询前检查的生命周期状态，Starting 跳过查询，停机或失败时结束。
/// - `session`：持有实际注册身份和 provider 的共享会话，回查期间持有异步锁。
/// - `contributor`：接收自注册健康、确认缺席或回查失败的观测槽。
/// - `critical`：确认缺席时是否记 NotReady；为 false 时记 Degraded，回查错误始终记 Degraded。
/// - `cancel`：摘流动作发出的取消信号，可中断轮次间五秒等待。
///
/// 返回：取消等待或轮次前观察到停机时结束；在册记 Ready，无注册身份跳过，确认缺席按策略处理。
/// 锁等待和远端回查本身不监听该取消信号，也无本层独立超时；退出还依赖底层请求预算和宿主任务监督。
/// 本循环不重新注册实例，不保证底层 SDK 必然恢复注册，也不把查询失败等同于实例失联。
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
            _ = tokio::time::sleep(INTERVAL) => {},
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
            // 查询确认不在可用实例集中，与查询失败分别处理；critical 决定是否让该事实阻止应用就绪。
            // 此处只发布观测，不重新注册，也不保证底层 SDK 的后续恢复结果。
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
/// 参数说明：
///
/// - `application`：提供真实监听地址的共享上下文。
/// - `config`：已读取的服务发现配置。
///
/// 返回：按 Web 实际端口、gRPC 预绑定端口、显式非零端口的顺序选择；均无可用值时返回 Ready 错误。
/// 不绑定监听器、不检查端口可达性，实际注册仍由会话结合注册配置解析。
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

/// 业务作用：选择传入注册信息及派生元数据的本地监听标识，不替代 provider 的注册地址解析。
///
/// 参数说明：
/// - `application`: 提供 Web 与 gRPC 已发布 listener 的共享上下文。
///
/// 返回：优先使用 Web 绑定 IP 字符串；纯 gRPC 使用端点 authority，未设置或无端点时为空串。
/// 不做主机解析或可路由性检查，返回字符串不能一概视为绑定 IP。
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
/// 参数说明：
///
/// - `application`：提供当前不可变配置快照的共享上下文。
///
/// 返回：从一次快照反序列化的发现配置，缺段时采用 DiscoveryConfig 默认值；类型错误返回 Start 错误。
/// 不执行 provider 连接、注册地址解析或端口可用性检查。
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
/// 参数说明：
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
///
/// 返回：段缺失或能按 DiscoveryConfig 反序列化时成功，否则返回指定阶段错误；只做结构绑定，不证明 provider 可达或注册计划可用。
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
/// 参数说明：
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含注册中心凭据的稳定摘要。
///
/// 返回：带 NacosDiscovery 身份与指定阶段、不含底层 source 的 ApplicationError；不主动输出或脱敏传入摘要。
fn discovery_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::NacosDiscovery, phase, message)
}

/// 业务作用：创建带底层错误链的服务发现错误。
///
/// 参数说明：
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含注册中心凭据的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
///
/// 返回：带组件、阶段与底层原因的 ApplicationError；source 保持原样供诊断，对外呈现仍须经过统一脱敏边界。
fn discovery_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::NacosDiscovery, phase, message, source)
}
