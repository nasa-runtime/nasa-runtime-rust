use std::{sync::Arc, time::Duration};

use naws::{RunningServer, Server, ServerBuilder};
use serde::Deserialize;

use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ComponentId, ReadyContext, ShutdownAction, ShutdownContext, StartContext,
};

/// 完整配置树中长连接组件负责读取的顶层投影。
///
/// 独立配置根 `ws`：Web 组件独占 `server`，两者监听不同端口、生命周期也各自独立，共用一个根只会让
/// 任一侧新增字段就打破另一侧的 `deny_unknown_fields`。
#[derive(Default, Deserialize)]
#[serde(default)]
struct WsConfigRoot {
    ws: WsConfig,
}

/// 长连接服务的监听、握手与背压参数。
///
/// 只覆盖"能从 YAML 声明"的部分；鉴权回调、endpoint 事件表、集群 notifier 是业务闭包，
/// 由 `Application::configure_ws` 在启动 Hook 里注入。
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WsConfig {
    /// TCP 长连接监听地址；`host:port`，端口 0 表示由系统分配。
    addr: String,
    /// 可选 WebSocket 监听地址；缺省表示只跑 TCP 接入。
    ws_addr: Option<String>,
    /// 连接建立后等待鉴权帧的上限，毫秒。
    auth_timeout_ms: u64,
    /// 鉴权成功后允许的最长心跳静默，毫秒。
    heartbeat_timeout_ms: u64,
    /// 单条协议帧的内层长度上限，字节。
    max_frame_bytes: Option<usize>,
    /// WebSocket 单条 message 的外层聚合上限，字节；缺省由帧上限派生。
    max_ws_message_bytes: Option<usize>,
    /// 单连接出站队列的条数上限。
    outbox_capacity: Option<usize>,
    /// 单连接出站队列的字节上限。
    outbox_max_bytes: Option<usize>,
    /// 全局在飞事件处理并发上限。
    max_inflight_handlers: Option<usize>,
    /// 单连接在飞事件处理并发上限；0 表示只受全局上限约束。
    max_inflight_handlers_per_conn: Option<usize>,
    /// 活跃连接总数上限；0 表示不限制。
    max_connections: Option<usize>,
    /// 未完成鉴权的连接数上限；0 表示不限制。
    max_unauthenticated: Option<usize>,
    /// 集群未就绪时是否降级继续启动；false 表示 bind 直接失败。
    cluster_degraded: bool,
    /// 停机摘流子预算，毫秒；实际上限取它与全局剩余停机预算的较小值。
    graceful_shutdown_timeout_ms: u64,
}

impl Default for WsConfig {
    /// 业务作用：返回只监听本机回环、不启用 WebSocket 端口的安全缺省配置。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未显式配置的限额沿用底层框架默认值。
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:9090".to_owned(),
            ws_addr: None,
            auth_timeout_ms: 5_000,
            heartbeat_timeout_ms: 60_000,
            max_frame_bytes: None,
            max_ws_message_bytes: None,
            outbox_capacity: None,
            outbox_max_bytes: None,
            max_inflight_handlers: None,
            max_inflight_handlers_per_conn: None,
            max_connections: None,
            max_unauthenticated: None,
            cluster_degraded: false,
            graceful_shutdown_timeout_ms: 10_000,
        }
    }
}

impl WsConfig {
    /// 业务作用：校验监听地址与各项限额。
    ///
    /// # 参数
    ///
    /// - `phase`：配置被校验时所属的生命周期阶段。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.addr.trim().is_empty() {
            return Err(ws_error(phase, "ws.addr cannot be empty"));
        }
        if self
            .ws_addr
            .as_deref()
            .is_some_and(|addr| addr.trim().is_empty())
        {
            return Err(ws_error(
                phase,
                "ws.ws_addr must be a bind address or be omitted",
            ));
        }
        for (name, value) in [
            ("ws.auth_timeout_ms", self.auth_timeout_ms),
            ("ws.heartbeat_timeout_ms", self.heartbeat_timeout_ms),
            (
                "ws.graceful_shutdown_timeout_ms",
                self.graceful_shutdown_timeout_ms,
            ),
        ] {
            if value == 0 {
                return Err(ws_error(phase, format!("{name} must be greater than zero")));
            }
            if Duration::from_millis(value) > crate::runner::MAX_LIFECYCLE_TIMEOUT {
                return Err(ws_error(phase, format!("{name} cannot exceed 365 days")));
            }
        }
        for (name, value) in [
            ("ws.max_frame_bytes", self.max_frame_bytes),
            ("ws.max_ws_message_bytes", self.max_ws_message_bytes),
            ("ws.outbox_capacity", self.outbox_capacity),
            ("ws.outbox_max_bytes", self.outbox_max_bytes),
            ("ws.max_inflight_handlers", self.max_inflight_handlers),
        ] {
            if value == Some(0) {
                return Err(ws_error(phase, format!("{name} must be greater than zero")));
            }
        }
        if self
            .max_inflight_handlers
            .is_some_and(|value| value > tokio::sync::Semaphore::MAX_PERMITS)
        {
            return Err(ws_error(
                phase,
                "ws.max_inflight_handlers exceeds the runtime semaphore limit",
            ));
        }
        Ok(())
    }

    /// 业务作用：把声明式配置写进底层 builder。
    ///
    /// 只覆盖配置显式给出的项，其余保持底层默认值，避免框架把"没配"翻译成"配了个默认数"。
    ///
    /// # 参数
    ///
    /// - `builder`：尚未接收业务定制的初始 builder。
    fn apply(&self, mut builder: ServerBuilder) -> ServerBuilder {
        builder = builder
            .addr(self.addr.clone())
            .auth_timeout(Duration::from_millis(self.auth_timeout_ms))
            .heartbeat_timeout(Duration::from_millis(self.heartbeat_timeout_ms))
            .cluster_degraded(self.cluster_degraded);
        if let Some(ws_addr) = &self.ws_addr {
            builder = builder.ws_addr(ws_addr.clone());
        }
        if let Some(value) = self.max_frame_bytes {
            builder = builder.max_frame_bytes(value);
        }
        if let Some(value) = self.max_ws_message_bytes {
            builder = builder.max_ws_message_bytes(value);
        }
        if let Some(value) = self.outbox_capacity {
            builder = builder.outbox_capacity(value);
        }
        if let Some(value) = self.outbox_max_bytes {
            builder = builder.outbox_max_bytes(value);
        }
        if let Some(value) = self.max_inflight_handlers {
            builder = builder.max_inflight_handlers(value);
        }
        if let Some(value) = self.max_inflight_handlers_per_conn {
            builder = builder.max_inflight_handlers_per_conn(value);
        }
        if let Some(value) = self.max_connections {
            builder = builder.max_connections(value);
        }
        if let Some(value) = self.max_unauthenticated {
            builder = builder.max_unauthenticated(value);
        }
        builder
    }
}

/// 长连接组件：Start 冻结配置，Ready 装配 builder、绑定监听并交出可逆停机动作。
///
/// 与 Web 组件同构地分两段：可失败的配置解析发生在没有网络副作用的 Start，真正的 bind 发生在
/// 业务资源封存之后的 Ready——因此 UserHook 失败时进程从未对外接受过一条长连接。
pub(crate) struct WsComponent {
    config: Option<WsConfig>,
}

impl WsComponent {
    /// 业务作用：创建尚未读取配置的长连接组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；监听器在 Ready 阶段才创建。
    pub(crate) fn new() -> Self {
        Self { config: None }
    }
}

impl ApplicationComponent for WsComponent {
    /// 业务作用：返回长连接组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类长连接相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::Ws
    }

    /// 业务作用：从最终配置读取并冻结长连接设置。
    ///
    /// # 参数
    ///
    /// - `context`：提供最终配置树的 Start 上下文。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_ws_config(context.application())?;
            config.validate(ApplicationPhase::Start)?;
            self.config = Some(config);
            Ok(())
        })
    }

    /// 业务作用：应用业务定制、构建并绑定长连接服务。
    ///
    /// 装配顺序固定为"配置预填 → configure_ws 注册序 → build → bind"：业务定制在配置之后，
    /// 因此可以覆盖声明式取值，也能注入配置无法表达的鉴权回调与 endpoint 事件表；
    /// `Sender` 在 bind 之前就发布到晚绑定运行态，业务 handler 一旦被触发即可广播，不存在丢消息窗口。
    ///
    /// # 参数
    ///
    /// - `context`：提供 Application、组件资源登记入口和 active stack 的 Ready 上下文。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = self.config.clone().ok_or_else(|| {
                ws_error(
                    ApplicationPhase::Ready,
                    "ws configuration was not prepared during start",
                )
            })?;
            let application = context.application().clone();

            let mut builder = config.apply(Server::builder());
            // 取走即封口：此后业务再调 configure_ws 会得到阶段错误，而不是被静默丢弃。
            for customize in application.take_ws_customizations() {
                builder = customize(builder);
            }
            let server = builder.build().map_err(|error| {
                // BuildError 只含结构性原因（缺 authorize、帧上限非法等），不含业务负载。
                ws_error(
                    ApplicationPhase::Ready,
                    format!("ws server build failed: {error}"),
                )
            })?;

            // Sender 是 Ready 才存在的晚绑定状态：资源容器此刻已封存，只能进 RuntimeState。
            // 发布必须早于 bind：bind 之后 handler 随时可能被触发，广播能力必须预先就绪。
            application.set_ws_sender(server.sender().clone())?;

            let running = server.bind().await.map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Ws,
                    ApplicationPhase::Ready,
                    format!("ws listener bind failed on {}", config.addr),
                    error,
                )
            })?;
            application.set_ws_addrs(running.local_addr, running.ws_local_addr)?;
            match running.ws_local_addr {
                Some(ws_addr) => tracing::info!(
                    "ws listening on {} (websocket {ws_addr})",
                    running.local_addr
                ),
                None => tracing::info!("ws listening on {}", running.local_addr),
            }

            let running = Arc::new(running);
            // 监听器已在接受连接后才压栈；此后任何退出路径都会先停 accept 再 drain。
            context.activate(Box::new(WsShutdown {
                server: Some(running),
                budget: Duration::from_millis(config.graceful_shutdown_timeout_ms),
            }));
            Ok(())
        })
    }
}

/// 停机时取消 accept 并在预算内 join 全部连接任务的可逆 action。
struct WsShutdown {
    server: Option<Arc<RunningServer>>,
    budget: Duration,
}

impl ShutdownAction for WsShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含监听地址或会话信息。
    fn label(&self) -> &'static str {
        "ws-active"
    }

    /// 业务作用：在自身子预算与全局剩余预算的较小值内排空长连接。
    ///
    /// 未排空不视为失败：底层已 cancel 全部任务，剩余连接会尽快退出；但必须如实记一条报告，
    /// 不把"到点仍有连接在跑"静默说成优雅停机。
    ///
    /// # 参数
    ///
    /// - `context`：提供全局剩余停机预算的清理上下文。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(server) = self.server.take() else {
                return Ok(());
            };
            let budget = self.budget.min(context.remaining());
            if server.shutdown_graceful(budget).await {
                return Ok(());
            }
            Err(ws_error(
                ApplicationPhase::Stopping,
                "ws connections were still running when the drain budget expired",
            ))
        })
    }
}

/// 业务作用：从最终配置读取 `ws` 段；段缺失时使用回环缺省配置。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
fn read_ws_config(application: &Application) -> ApplicationResult<WsConfig> {
    let snapshot = application.config();
    let root: WsConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Ws,
                ApplicationPhase::Start,
                "invalid `ws` configuration section",
                error,
            )
        })?;
    Ok(root.ws)
}

/// 业务作用：在不创建监听器的前提下校验候选配置树中的 `ws` 段。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_ws_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("ws") else {
        return Ok(());
    };
    let config: WsConfig = serde_json::from_value(section.clone()).map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Ws,
            phase,
            "invalid `ws` configuration section",
            error,
        )
    })?;
    config.validate(phase)
}

/// 业务作用：创建长连接组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含会话负载与鉴权信息的稳定摘要。
fn ws_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Ws, phase, message)
}
