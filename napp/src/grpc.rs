//! 实验性 gRPC listener 的 Application 生命周期接入。
//!
//! 业务在 UserHook 只提交 Router 工厂；组件在 Prepare 封口装配入口，在全部业务 initializer
//! 成功后的 Ready 阶段才构造 Router、绑定端口并发布只读观察句柄。serve 状态由关键任务监督，
//! 停机先关闭准入，再按 listener 子预算与 Application 全局剩余时间派生提前收口预算，
//! 为结果归类和后续逆序清理保留尾部时间。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(5);
/// accept 失败后持续未成功接流达到该时长即摘流，不终止仍持有 listener 的 serve 任务。
const ACCEPT_FAILURE_NOT_READY_AFTER: Duration = Duration::from_secs(1);
/// 摘流后的本机 TCP 恢复探测上限；只验证 listener 能重新 accept，不发送业务协议数据。
const ACCEPT_RECOVERY_PROBE_TIMEOUT: Duration = Duration::from_millis(100);

/// 业务二进制构造 tonic Router 的一次性工厂。
type GrpcRouterFactory = Box<
    dyn FnOnce(
            &nagrpc::GrpcServerConfig,
        ) -> ApplicationResult<nagrpc::tonic_api::transport::server::Router>
        + Send
        + 'static,
>;

/// 业务作用：描述由 Application 在 Ready 阶段唯一消费的 gRPC service 装配入口。
///
/// 计划只持有未执行工厂，不绑定端口、不启动任务。工厂收到框架已经校验的同一份
/// `GrpcServerConfig`，必须用其 `server_builder()` 和 `message_limits` 装配所有 generated service。
pub struct GrpcApplicationPlan {
    factory: Option<GrpcRouterFactory>,
}

impl GrpcApplicationPlan {
    /// 业务作用：创建只在 Ready 阶段执行一次的 gRPC Router 计划。
    ///
    /// 参数说明：
    /// - `factory`: 用受管配置构造 health、reflection 与业务 service Router 的一次性工厂。
    ///
    /// 返回：不产生网络或任务副作用的可提交计划。
    pub fn new<F>(factory: F) -> Self
    where
        F: FnOnce(
                &nagrpc::GrpcServerConfig,
            ) -> ApplicationResult<nagrpc::tonic_api::transport::server::Router>
            + Send
            + 'static,
    {
        Self {
            factory: Some(Box::new(factory)),
        }
    }

    /// 业务作用：在 Ready 阶段线性消费工厂并构造唯一 Router。
    ///
    /// 参数说明：
    /// - `config`: 从最终 Application 配置读取并通过全部硬上限校验的 server 配置。
    ///
    /// 返回：首次调用返回业务 Router；重复消费返回 gRPC Ready 错误。
    fn build(
        &mut self,
        config: &nagrpc::GrpcServerConfig,
    ) -> ApplicationResult<nagrpc::tonic_api::transport::server::Router> {
        let factory = self.factory.take().ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Ready,
                "gRPC router factory was already consumed",
            )
        })?;
        factory(config)
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
/// 业务作用：承载最终 Application 配置中的 `grpc` 根节点，避免解析其它配置段时放宽字段合同。
struct GrpcConfigRoot {
    grpc: GrpcSettings,
}

/// 固定 `grpc` 配置根；全部容量与时间字段都必须在绑定端口前通过硬上限校验。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GrpcSettings {
    bind: String,
    max_connections: usize,
    concurrency_limit_per_connection: usize,
    request_timeout_ms: u64,
    keepalive_interval_ms: u64,
    keepalive_timeout_ms: u64,
    max_concurrent_streams: u32,
    drain_timeout_ms: u64,
    max_decoding_bytes: usize,
    max_encoding_bytes: usize,
}

impl Default for GrpcSettings {
    /// 业务作用：提供仅绑定 loopback、容量有界且可显式覆盖的实验 listener 缺省配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端口 50051、1024 连接及 nagrpc 其余保守默认值的配置投影。
    fn default() -> Self {
        let config = nagrpc::GrpcServerConfig::default();
        Self {
            bind: "127.0.0.1:50051".to_owned(),
            max_connections: config.max_connections,
            concurrency_limit_per_connection: config.concurrency_limit_per_connection,
            request_timeout_ms: duration_millis(config.request_timeout),
            keepalive_interval_ms: duration_millis(config.keepalive_interval),
            keepalive_timeout_ms: duration_millis(config.keepalive_timeout),
            max_concurrent_streams: config.max_concurrent_streams,
            drain_timeout_ms: duration_millis(config.drain_timeout),
            max_decoding_bytes: config.message_limits.max_decoding_bytes,
            max_encoding_bytes: config.message_limits.max_encoding_bytes,
        }
    }
}

impl GrpcSettings {
    /// 业务作用：解析绑定地址并把毫秒配置投影为 nagrpc 的唯一 server 边界对象。
    ///
    /// 参数说明：
    /// - `phase`: 当前配置校验所属 Application 阶段。
    ///
    /// 返回：地址与全部硬上限合法时返回可构造 Router 和 listener 的配置，否则返回 gRPC 错误。
    fn validate(
        &self,
        phase: ApplicationPhase,
    ) -> ApplicationResult<(SocketAddr, nagrpc::GrpcServerConfig)> {
        let bind = self.bind.parse::<SocketAddr>().map_err(|error| {
            grpc_error_src(phase, "grpc.bind must be an IP socket address", error)
        })?;
        let config = nagrpc::GrpcServerConfig {
            max_connections: self.max_connections,
            concurrency_limit_per_connection: self.concurrency_limit_per_connection,
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            keepalive_interval: Duration::from_millis(self.keepalive_interval_ms),
            keepalive_timeout: Duration::from_millis(self.keepalive_timeout_ms),
            max_concurrent_streams: self.max_concurrent_streams,
            drain_timeout: Duration::from_millis(self.drain_timeout_ms),
            message_limits: nagrpc::GrpcMessageLimits {
                max_decoding_bytes: self.max_decoding_bytes,
                max_encoding_bytes: self.max_encoding_bytes,
            },
        };
        config.validate().map_err(|error| {
            grpc_error_src(
                phase,
                "grpc configuration exceeds a managed server boundary",
                error,
            )
        })?;
        Ok((bind, config))
    }
}

/// UserHook 计划、Ready 发布与运行期观察共用的单实例状态。
pub(crate) struct GrpcRuntimeState {
    plan: Mutex<GrpcPlanState>,
    observer: OnceLock<nagrpc::GrpcServerObserver>,
}

enum GrpcPlanState {
    Open(Option<GrpcApplicationPlan>),
    Taken,
}

impl GrpcRuntimeState {
    /// 业务作用：创建计划入口开放、listener 尚未发布的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只允许一次计划提交与一次观察句柄发布的共享状态。
    pub(crate) fn new() -> Self {
        Self {
            plan: Mutex::new(GrpcPlanState::Open(None)),
            observer: OnceLock::new(),
        }
    }

    /// 业务作用：在 UserHook 把无网络副作用的 Router 计划一次性移交给组件。
    ///
    /// 参数说明：
    /// - `plan`: 尚未消费 Router 工厂的完整装配计划。
    ///
    /// 返回：首次且入口开放时成功；重复或晚到提交返回阶段错误。
    pub(crate) fn configure(&self, plan: GrpcApplicationPlan) -> ApplicationResult<()> {
        let mut state = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            GrpcPlanState::Open(slot @ None) => {
                *slot = Some(plan);
                Ok(())
            }
            GrpcPlanState::Open(Some(_)) => Err(grpc_error(
                ApplicationPhase::UserHook,
                "gRPC application plan can be configured only once",
            )),
            GrpcPlanState::Taken => Err(grpc_error(
                ApplicationPhase::Prepare,
                "gRPC application plan registration is closed",
            )),
        }
    }

    /// 业务作用：在 Prepare 永久关闭计划入口并取得唯一 Router 工厂。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：UserHook 已提交时返回计划；缺失或重复消费时拒绝启动。
    fn take_plan(&self) -> ApplicationResult<GrpcApplicationPlan> {
        let mut state = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::mem::replace(&mut *state, GrpcPlanState::Taken);
        match previous {
            GrpcPlanState::Open(Some(plan)) => Ok(plan),
            GrpcPlanState::Open(None) => Err(grpc_error(
                ApplicationPhase::Prepare,
                "gRPC component requires configure_grpc during the Service user hook",
            )),
            GrpcPlanState::Taken => Err(grpc_error(
                ApplicationPhase::Prepare,
                "gRPC application plan was already consumed",
            )),
        }
    }

    /// 业务作用：在停机 action 已建立后发布只读 listener 观察句柄。
    ///
    /// 参数说明：
    /// - `observer`: 不含 shutdown 权限的地址、状态与连接计数视图。
    ///
    /// 返回：首次发布成功；重复发布返回 Ready 错误。
    fn publish(&self, observer: nagrpc::GrpcServerObserver) -> ApplicationResult<()> {
        self.observer.set(observer).map_err(|_| {
            grpc_error(
                ApplicationPhase::Ready,
                "gRPC listener observer was already published",
            )
        })
    }

    /// 业务作用：取得已由 Ready 阶段发布且不含停机权限的 listener 观察句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：listener 已绑定时返回共享观察句柄；尚未 Ready 时返回阶段错误。
    pub(crate) fn observer(&self) -> ApplicationResult<nagrpc::GrpcServerObserver> {
        self.observer.get().cloned().ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Running,
                "gRPC listener is not published yet",
            )
        })
    }
}

/// 实验性受管 gRPC listener 组件。
pub(crate) struct GrpcComponent {
    bind: Option<SocketAddr>,
    config: Option<nagrpc::GrpcServerConfig>,
    plan: Option<GrpcApplicationPlan>,
    contributor: Option<ReadinessContributor>,
    critical_task: Option<ApplicationFuture<'static>>,
}

impl GrpcComponent {
    /// 业务作用：创建尚未读取配置、计划和 Router 的 listener 组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：等待 Start 校验配置、Prepare 消费计划、Ready 绑定端口的组件。
    pub(crate) fn new() -> Self {
        Self {
            bind: None,
            config: None,
            plan: None,
            contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for GrpcComponent {
    /// 业务作用：返回实验 listener 的稳定生命周期身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`grpc` 组件身份。
    fn id(&self) -> ComponentId {
        ComponentId::Grpc
    }

    /// 业务作用：从最终配置校验全部 listener 边界，并登记关键 readiness 贡献项。
    ///
    /// 参数说明：
    /// - `context`: 提供配置快照与共享 readiness 注册表的 Start 上下文。
    ///
    /// 返回：配置和贡献项合法时成功；端口、容量、消息或时间边界非法时拒绝启动。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let root: GrpcConfigRoot = context.application().config_as()?;
            let (bind, config) = root.grpc.validate(ApplicationPhase::Start)?;
            self.contributor = Some(context.application().register_readiness(
                ComponentId::Grpc,
                Arc::<str>::from("grpc:listener"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: Some(HEALTH_STALE_AFTER),
                },
            )?);
            self.bind = Some(bind);
            self.config = Some(config);
            Ok(())
        })
    }

    /// 业务作用：在 initializer 之前封口 Router 计划，确保后续装配不能越过生命周期边界。
    ///
    /// 参数说明：
    /// - `context`: 提供共享 Application 的 Prepare 上下文。
    ///
    /// 返回：UserHook 已提交唯一计划时成功；缺失或重复消费时禁止进入业务初始化。
    fn prepare<'a>(&'a mut self, context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.plan = Some(context.application().grpc_runtime().take_plan()?);
            Ok(())
        })
    }

    /// 业务作用：在全部 initializer 成功后构造 Router、绑定端口、发布 Ready 并建立停机所有权。
    ///
    /// 参数说明：
    /// - `context`: 提供共享 Application、启动剩余预算和 active stack 的 Ready 上下文。
    ///
    /// 返回：listener 已绑定、观察句柄已发布且 shutdown action 已压栈时成功；工厂 panic、
    /// 绑定失败或发布冲突时完整关闭新 listener 后返回错误。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let bind = self.bind.ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC bind address was not prepared during Start",
                )
            })?;
            let config = self.config.clone().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC server configuration was not prepared during Start",
                )
            })?;
            let mut plan = self.plan.take().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC application plan was not prepared before initialization",
                )
            })?;
            let router = match catch_unwind(AssertUnwindSafe(|| plan.build(&config))) {
                Ok(result) => result?,
                Err(payload) => {
                    std::mem::forget(payload);
                    return Err(grpc_error(
                        ApplicationPhase::Ready,
                        "gRPC router factory panicked",
                    ));
                }
            };
            let handle = config.start(router, bind).await.map_err(|error| {
                grpc_error_src(
                    ApplicationPhase::Ready,
                    "gRPC listener could not start",
                    error,
                )
            })?;
            let observer = handle.observer();
            let contributor = self.contributor.clone().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC readiness contributor was not registered during Start",
                )
            })?;

            if let Err(error) = context
                .application()
                .grpc_runtime()
                .publish(observer.clone())
            {
                let _ = handle.shutdown_with_timeout(context.remaining()).await;
                return Err(error);
            }
            contributor.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            );
            self.critical_task = Some(Box::pin(run_health_monitor(
                context.application().clone(),
                observer,
                contributor.clone(),
            )));
            // 地址和观察面成功发布后才压入 shutdown owner；此后任一失败都先停止 gRPC 准入，
            // 再按反向顺序释放 handler 依赖的数据库、消息 transport 与业务资源。
            context.activate(Box::new(GrpcShutdown {
                handle,
                contributor,
            }));
            Ok(())
        })
    }

    /// 业务作用：把 listener 状态 monitor 移交 Runner，serve 异常退出会触发统一失败停机。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 已建立 listener 时返回一次性关键任务，否则返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("grpc-listener-monitor", task))
    }
}

/// 业务作用：周期复验 listener 所有权与接流进展，把持续 accept 失败摘流并将所有权丢失升级为关键失败。
///
/// 参数说明：
/// - `application`: 区分正常停机与运行期失权的共享容器。
/// - `observer`: 不含取消权的 listener 状态视图。
/// - `contributor`: `grpc:listener` readiness 的独占更新句柄。
///
/// 返回：正常停机时成功退出；持续 accept 失败只维持 NotReady 并等待恢复，运行期进入 Draining、
/// Closed 或 Failed 时返回 gRPC 运行错误。
async fn run_health_monitor(
    application: Application,
    observer: nagrpc::GrpcServerObserver,
    contributor: ReadinessContributor,
) -> ApplicationResult<()> {
    loop {
        if matches!(
            application.state(),
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
        ) {
            contributor.observe(
                DependencyState::NotReady,
                reason::NOT_READY,
                std::time::Instant::now(),
            );
            return Ok(());
        }
        match observer.state() {
            nagrpc::GrpcServerState::Running => {
                if observer
                    .accept_failure_duration()
                    .is_some_and(|elapsed| elapsed >= ACCEPT_FAILURE_NOT_READY_AFTER)
                {
                    // listener 仍由 serve 任务持有时不能终止它；先从业务路由摘流并持续观察，
                    // 成功 accept 会清零连续失败计数，下一轮即可恢复 Ready。
                    contributor.observe(
                        DependencyState::NotReady,
                        reason::GRPC_ACCEPT_STALLED,
                        std::time::Instant::now(),
                    );
                    // 摘流后外部负载均衡可能不再建立新连接；主动发起一次本机 TCP 探测，
                    // 资源恢复时促成成功 accept 并清除连续失败状态，避免恢复依赖业务流量。
                    let probe =
                        tokio::net::TcpStream::connect(local_probe_address(observer.local_addr()));
                    let _ = tokio::time::timeout(ACCEPT_RECOVERY_PROBE_TIMEOUT, probe).await;
                } else {
                    contributor.observe(
                        DependencyState::Ready,
                        reason::HEALTHY,
                        std::time::Instant::now(),
                    );
                }
            }
            nagrpc::GrpcServerState::Draining
            | nagrpc::GrpcServerState::Closed
            | nagrpc::GrpcServerState::Failed => {
                contributor.observe(
                    DependencyState::NotReady,
                    reason::GRPC_LISTENER_UNAVAILABLE,
                    std::time::Instant::now(),
                );
                return Err(grpc_error(
                    ApplicationPhase::Running,
                    "gRPC listener lost serving ownership",
                ));
            }
        }
        tokio::time::sleep(HEALTH_INTERVAL).await;
    }
}

/// 业务作用：把 listener 的本机绑定地址转换为可主动连接的恢复探测地址。
///
/// 参数说明：
/// - `address`: listener 实际绑定地址，可能使用 IPv4 或 IPv6 未指定地址。
///
/// 返回：保留端口与地址族；未指定 IP 映射为对应 loopback，其余地址保持不变。
fn local_probe_address(mut address: SocketAddr) -> SocketAddr {
    if address.ip().is_unspecified() {
        address.set_ip(match address.ip() {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        });
    }
    address
}

/// 停机 action 独占 gRPC listener 的取消和 join 权限。
struct GrpcShutdown {
    handle: nagrpc::GrpcServerHandle,
    contributor: ReadinessContributor,
}

impl ShutdownAction for GrpcShutdown {
    /// 业务作用：返回停机报告使用的稳定 action 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含地址或业务 service 名的固定标签。
    fn label(&self) -> &'static str {
        "grpc-listener"
    }

    /// 业务作用：先把 listener 摘出 Ready，再在全局剩余预算内停止准入并排空在途 RPC。
    ///
    /// 参数说明：
    /// - `context`: 提供全部 active step 共用的绝对停机 deadline。
    ///
    /// 返回：serve task 在子预算内完整退出时成功；排空超时或 serve 失败时返回可归因停机错误。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.contributor.observe(
                DependencyState::NotReady,
                reason::NOT_READY,
                std::time::Instant::now(),
            );
            self.handle
                .shutdown_with_timeout(context.child_budget(Duration::MAX))
                .await
                .map_err(|error| {
                    grpc_error_src(
                        ApplicationPhase::Stopping,
                        "gRPC listener did not drain cleanly",
                        error,
                    )
                })
        })
    }
}

/// 业务作用：把受管默认时长投影为配置毫秒且保持上界可表示。
///
/// 参数说明：
/// - `duration`: nagrpc 默认 server 时长。
///
/// 返回：不超过 `u64` 的毫秒值。
fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// 业务作用：在不创建 Router、socket 或任务的前提下校验候选 `grpc` 配置段。
///
/// 参数说明：
/// - `tree`: 合并、插值完成但尚未发布的候选配置树。
/// - `phase`: 本轮无副作用校验所属生命周期阶段。
///
/// 返回：配置段缺失或全部字段合法时成功；反序列化、地址或边界非法时拒绝候选快照。
pub(crate) fn validate_grpc_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("grpc") else {
        return Ok(());
    };
    let settings: GrpcSettings = serde_json::from_value(section.clone())
        .map_err(|error| grpc_error_src(phase, "invalid `grpc` configuration section", error))?;
    settings.validate(phase).map(|_| ())
}

/// 业务作用：创建不包含绑定地址或业务 service 名的 gRPC 生命周期错误。
///
/// 参数说明：
/// - `phase`: 错误被裁决时的 Application 生命周期阶段。
/// - `message`: 稳定配置或所有权摘要。
///
/// 返回：归属 `grpc` 组件的统一错误。
fn grpc_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Grpc, phase, message)
}

/// 业务作用：创建保留底层错误链且不把动态细节拼入稳定摘要的 gRPC 错误。
///
/// 参数说明：
/// - `phase`: 错误被裁决时的 Application 生命周期阶段。
/// - `message`: 不含地址、证书或业务 service 名的稳定摘要。
/// - `source`: 仅供统一诊断通道处理的底层错误。
///
/// 返回：归属 `grpc` 组件并保留 source 的统一错误。
fn grpc_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Grpc, phase, message, source)
}
