use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::task::{JoinHandle, JoinSet};

use axum::{
    extract::{connect_info::ConnectInfo, DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{from_fn, from_fn_with_state, Next},
    response::Response,
    routing::get,
    Extension, Router,
};
use serde::Deserialize;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{web_handle::WebRuntimeState, RouteInfo};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationSpec, ApplicationState, ComponentId, ReadyContext, RouteMeta,
    ShutdownAction, ShutdownContext, StartContext, WebBuildContext, WebRouteMetaFactory,
    WebRouterFactory,
};

/// 完整配置树中 Web 组件负责读取的顶层投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct WebConfigRoot {
    server: ServerConfig,
}

/// CORS 策略配置:默认关闭。启用时 `allowed_origins` 不得为空,且 `*` 不得与
/// `allow_credentials` 并存(在 `validate` 期经 `CorsPolicy::new` 校验)。预检 OPTIONS 在鉴权之外
/// 直接 204 答复,非预检响应按来源白名单追加 `Access-Control-Allow-Origin`。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CorsConfig {
    /// 是否启用 CORS;默认关闭(同源应用无需跨域头, 默认关)。
    enabled: bool,
    /// 允许的来源白名单;启用时不得为空。
    allowed_origins: Vec<String>,
    /// 是否允许携带凭据;为真时 `allowed_origins` 不得含 `*`(否则拒绝启动)。
    allow_credentials: bool,
    /// 预检回传的允许方法。
    allowed_methods: String,
    /// 预检回传的允许请求头。
    allowed_headers: String,
    /// 预检结果缓存秒数。
    max_age_secs: u64,
}

impl Default for CorsConfig {
    /// 业务作用：返回默认关闭的安全缺省(启用需业务显式配置来源白名单)。
    fn default() -> Self {
        Self {
            enabled: false,
            allowed_origins: Vec::new(),
            allow_credentials: false,
            allowed_methods: "GET,POST,PUT,PATCH,DELETE,OPTIONS".to_owned(),
            allowed_headers: "content-type,authorization".to_owned(),
            max_age_secs: 600,
        }
    }
}

/// 响应压缩策略;默认关闭。
///
/// 启用后对文本类白名单响应做 gzip;密文(命中 `naweb::UncompressibleResponse` 标记)、已带
/// `Content-Encoding` 的响应、以及非白名单类型一律跳过——由 `governance::should_compress_response`
/// 谓词统一裁决,规避压缩+加密同用的 CRIME/BREACH 侧信道。小于 `min_size_bytes` 的响应不压缩
/// (小体积压缩得不偿失)。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompressionConfig {
    /// 是否启用响应压缩;默认关闭。
    enabled: bool,
    /// 触发压缩的最小响应体字节数(tower-http `SizeAbove`,上限 65535);小于此值不压缩。
    min_size_bytes: u16,
}

impl Default for CompressionConfig {
    /// 业务作用：返回默认关闭的安全缺省(压缩需业务显式开启)。
    fn default() -> Self {
        Self {
            enabled: false,
            min_size_bytes: 1024,
        }
    }
}

/// 单实例每客户端限流策略;默认关闭。
///
/// 启用后按真实客户端 IP(`ClientIp`,受 `trusted_proxies` 语义约束)做令牌桶,超额即 429 +
/// `Retry-After`。只保护本进程;跨副本的租户/主体总配额是另一层(`RateLimitProvider` + 共享后端),
/// 不由本地 gate 承担。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RateLimitConfig {
    /// 是否启用每客户端限流;默认关闭。
    enabled: bool,
    /// 每客户端每秒平均放行请求数(令牌补充速率),启用时须 > 0。
    requests_per_second: u32,
    /// 突发容量(令牌桶上限),启用时须 > 0;允许短时高于平均速率的突发。
    burst: u32,
}

impl Default for RateLimitConfig {
    /// 业务作用：返回默认关闭的安全缺省(限流需业务显式开启)。
    fn default() -> Self {
        Self {
            enabled: false,
            requests_per_second: 10,
            burst: 20,
        }
    }
}

/// 明文 Web listener 的 HTTP/2 安全边界。
///
/// `enabled=false` 时 listener 只接受 HTTP/1；启用后同一明文端口按连接前言区分 HTTP/1 与 h2c。
/// 所有容量值在 Start 阶段校验并在连接创建时冻结，避免依赖其它 Cargo feature 改变协议行为。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Http2Config {
    /// 是否接受 h2c prior knowledge；不实现 `Upgrade: h2c` 协商。
    enabled: bool,
    /// listener 同时持有的 TCP 连接上限；同时约束 HTTP/1 与 h2c。
    max_connections: usize,
    /// 协议前言最多允许等待的毫秒数。
    connection_handshake_timeout_ms: u64,
    /// 主动轮转连接的毫秒数；`None` 表示不主动轮转。
    max_connection_age_ms: Option<u64>,
    /// 单条 HTTP/2 连接允许同时处理的 stream 上限。
    max_concurrent_streams: u32,
    /// HTTP/2 单 stream 初始流控窗口。
    initial_stream_window_size: u32,
    /// HTTP/2 单连接初始流控窗口。
    initial_connection_window_size: u32,
    /// HTTP/2 frame 最大字节数。
    max_frame_size: u32,
    /// 单次 HTTP/2 请求头列表最大字节数。
    max_header_list_size: u32,
    /// HTTP/2 HPACK 动态表最大字节数。
    header_table_size: u32,
    /// 单条 HTTP/2 stream 的发送缓冲上限。
    max_send_buffer_size: usize,
    /// 对端尚未确认的 reset stream 上限。
    max_pending_accept_reset_streams: usize,
    /// 本端因协议错误产生的 reset stream 上限。
    max_local_error_reset_streams: usize,
    /// HTTP/2 PING 探活周期毫秒数。
    keep_alive_interval_ms: u64,
    /// 等待 HTTP/2 PING ACK 的毫秒数。
    keep_alive_timeout_ms: u64,
}

impl Default for Http2Config {
    /// 业务作用：提供默认关闭 h2c、启用后连接与 stream 均有界的保守配置。
    ///
    /// # 参数
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回：可直接通过启动校验的 HTTP/2 配置；开启前不会接受 h2c。
    fn default() -> Self {
        Self {
            enabled: false,
            max_connections: 256,
            connection_handshake_timeout_ms: 10_000,
            max_connection_age_ms: None,
            max_concurrent_streams: 128,
            initial_stream_window_size: 64 * 1024,
            initial_connection_window_size: 1024 * 1024,
            max_frame_size: 16 * 1024,
            max_header_list_size: 16 * 1024,
            header_table_size: 4 * 1024,
            max_send_buffer_size: 64 * 1024,
            max_pending_accept_reset_streams: 20,
            max_local_error_reset_streams: 20,
            keep_alive_interval_ms: 30_000,
            keep_alive_timeout_ms: 10_000,
        }
    }
}

impl Http2Config {
    /// 业务作用：校验 Web 连接容量与 HTTP/2 transport 上界，阻止无界或底层不接受的配置进入监听阶段。
    ///
    /// # 参数
    ///
    /// - `phase`：配置校验所属的 Application 生命周期阶段。
    ///
    /// # 返回
    ///
    /// 返回：全部字段处于受管范围内时成功，否则返回稳定的 Web 配置错误。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.max_connections == 0 || self.max_connections > 4_096 {
            return Err(web_error(
                phase,
                "server.http2.max_connections must be between 1 and 4096",
            ));
        }
        if self.connection_handshake_timeout_ms == 0
            || self.connection_handshake_timeout_ms > 60_000
        {
            return Err(web_error(
                phase,
                "server.http2.connection_handshake_timeout_ms must be between 1 and 60000",
            ));
        }
        if self.max_connection_age_ms.is_some_and(|age| {
            !(Duration::from_secs(60)..=Duration::from_secs(24 * 60 * 60))
                .contains(&Duration::from_millis(age))
        }) {
            return Err(web_error(
                phase,
                "server.http2.max_connection_age_ms must be between 60000 and 86400000",
            ));
        }
        if self.max_concurrent_streams == 0 || self.max_concurrent_streams > 65_535 {
            return Err(web_error(
                phase,
                "server.http2.max_concurrent_streams must be between 1 and 65535",
            ));
        }
        if self.initial_stream_window_size == 0 || self.initial_stream_window_size > 1024 * 1024 {
            return Err(web_error(
                phase,
                "server.http2.initial_stream_window_size must be between 1 and 1048576",
            ));
        }
        if self.initial_connection_window_size == 0
            || self.initial_connection_window_size > 16 * 1024 * 1024
        {
            return Err(web_error(
                phase,
                "server.http2.initial_connection_window_size must be between 1 and 16777216",
            ));
        }
        if !(16_384..=65_535).contains(&self.max_frame_size) {
            return Err(web_error(
                phase,
                "server.http2.max_frame_size must be between 16384 and 65535",
            ));
        }
        if self.max_header_list_size == 0 || self.max_header_list_size > 64 * 1024 {
            return Err(web_error(
                phase,
                "server.http2.max_header_list_size must be between 1 and 65536",
            ));
        }
        if self.header_table_size == 0 || self.header_table_size > 16 * 1024 {
            return Err(web_error(
                phase,
                "server.http2.header_table_size must be between 1 and 16384",
            ));
        }
        if self.max_send_buffer_size == 0 || self.max_send_buffer_size > 1024 * 1024 {
            return Err(web_error(
                phase,
                "server.http2.max_send_buffer_size must be between 1 and 1048576",
            ));
        }
        for (field, value) in [
            (
                "server.http2.max_pending_accept_reset_streams",
                self.max_pending_accept_reset_streams,
            ),
            (
                "server.http2.max_local_error_reset_streams",
                self.max_local_error_reset_streams,
            ),
        ] {
            if value == 0 || value > 1_024 {
                return Err(web_error(
                    phase,
                    format!("{field} must be between 1 and 1024"),
                ));
            }
        }
        for (field, value) in [
            (
                "server.http2.keep_alive_interval_ms",
                self.keep_alive_interval_ms,
            ),
            (
                "server.http2.keep_alive_timeout_ms",
                self.keep_alive_timeout_ms,
            ),
        ] {
            if value == 0 || Duration::from_millis(value) > crate::runner::MAX_LIFECYCLE_TIMEOUT {
                return Err(web_error(
                    phase,
                    format!("{field} must be greater than zero and cannot exceed 365 days"),
                ));
            }
        }
        Ok(())
    }
}

/// Web 监听、路径前缀、探针和请求跟踪的初始配置。
///
/// 默认值保证必需配置文件内容为 `{}` 时仍可启动最小本地服务。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServerConfig {
    host: String,
    port: u16,
    #[serde(alias = "context-path")]
    context_path: String,
    health: bool,
    trace: bool,
    /// 单个请求体的上限；`None` 表示保留底层 Web 引擎的默认上限。
    request_body_limit_bytes: Option<usize>,
    /// Web 摘流子预算；实际 drain 上限是它与全局剩余停机预算的较小值。
    /// `None` 表示不额外收紧，完全由全局停机预算约束。
    graceful_shutdown_timeout_ms: Option<u64>,
    /// 全局并发准入上限(load shed);`None` 表示不启用,达上限即 503 + `Retry-After`。
    max_inflight_requests: Option<usize>,
    /// 过载 503 响应回传的建议重试秒数(仅在 `max_inflight_requests` 启用时生效)。
    overload_retry_after_secs: u64,
    /// 请求总 deadline 毫秒;`None` 表示不施加总超时(仍受下游各自超时约束)。
    request_deadline_ms: Option<u64>,
    /// CORS 策略;默认关闭。启用时预检 OPTIONS 在鉴权之外直接 204。
    cors: CorsConfig,
    /// 可信代理列表;每项为精确 IP 或 CIDR。默认空 = 永不采信 `X-Forwarded-For`
    /// (客户端 IP 恒取直连对端,防伪造)。仅当直连对端落在列表内时才按 XFF 解析真实客户端 IP。
    trusted_proxies: Vec<String>,
    /// 响应压缩策略;默认关闭。
    compression: CompressionConfig,
    /// 单实例每客户端限流策略;默认关闭。
    rate_limit: RateLimitConfig,
    /// 明文 HTTP/2 与共享 TCP 连接容量策略；默认只接受 HTTP/1。
    http2: Http2Config,
    /// mapping/安全运行时就绪失败(route audit 漂移 / active 签名 key 缺失 / required-replay 后端不可用)是否
    /// 升级为**关键**就绪:默认 `false` = 非关键,monitor 复审失败只 Degraded
    /// (last-good 路由/interceptor 合同仍服务、`/readyz` 保持 200,不把可恢复后端抖动升级成整实例摘流);
    /// `true` = `affects_ready` 关键,monitor 复审失败置 NotReady → `/readyz` 503,交由编排替换本实例。
    mapping_readiness_critical: bool,
}

impl Default for ServerConfig {
    /// 业务作用：返回最小本地 Web 服务的安全缺省配置。
    ///
    /// # 参数
    ///
    /// 本方法无参数；监听范围只包含本机回环地址。
    ///
    /// # 返回
    ///
    /// 返回：HTTP/1 可直接启动、h2c 默认关闭且高级 transport 字段均有界的配置。
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 8080,
            context_path: String::new(),
            health: true,
            trace: false,
            request_body_limit_bytes: None,
            graceful_shutdown_timeout_ms: None,
            max_inflight_requests: None,
            overload_retry_after_secs: 1,
            request_deadline_ms: None,
            cors: CorsConfig::default(),
            trusted_proxies: Vec::new(),
            compression: CompressionConfig::default(),
            rate_limit: RateLimitConfig::default(),
            http2: Http2Config::default(),
            mapping_readiness_critical: false,
        }
    }
}

impl ServerConfig {
    /// 业务作用：校验监听地址文本和可嵌套的统一路径前缀。
    ///
    /// # 参数
    ///
    /// - `phase`：配置被校验时所属的生命周期阶段。
    ///
    /// # 返回
    ///
    /// 返回：Web、治理与 HTTP transport 字段全部合法时成功，否则返回所属阶段的配置错误。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.host.trim().is_empty() {
            return Err(web_error(phase, "server.host cannot be empty"));
        }
        if !self.context_path.is_empty()
            && (!self.context_path.starts_with('/')
                || self.context_path == "/"
                || self.context_path.ends_with('/'))
        {
            return Err(web_error(
                phase,
                "server.context_path must be empty or start with one slash without a trailing slash",
            ));
        }
        if self.graceful_shutdown_timeout_ms == Some(0) {
            return Err(web_error(
                phase,
                "server.graceful_shutdown_timeout_ms must be greater than zero",
            ));
        }
        if self.request_body_limit_bytes == Some(0) {
            return Err(web_error(
                phase,
                "server.request_body_limit_bytes must be greater than zero",
            ));
        }
        if self.max_inflight_requests == Some(0) {
            return Err(web_error(
                phase,
                "server.max_inflight_requests must be greater than zero",
            ));
        }
        if self
            .max_inflight_requests
            .is_some_and(|value| value > tokio::sync::Semaphore::MAX_PERMITS)
        {
            return Err(web_error(
                phase,
                "server.max_inflight_requests exceeds the runtime semaphore limit",
            ));
        }
        if self.request_deadline_ms == Some(0) {
            return Err(web_error(
                phase,
                "server.request_deadline_ms must be greater than zero",
            ));
        }
        for (field, millis) in [
            (
                "server.graceful_shutdown_timeout_ms",
                self.graceful_shutdown_timeout_ms,
            ),
            ("server.request_deadline_ms", self.request_deadline_ms),
        ] {
            if millis.is_some_and(|millis| {
                Duration::from_millis(millis) > crate::runner::MAX_LIFECYCLE_TIMEOUT
            }) {
                return Err(web_error(phase, format!("{field} cannot exceed 365 days")));
            }
        }
        // 可信代理列表提前解析校验:非法 IP/CIDR 在 Start 即 fail-fast,不等到请求期。
        crate::governance::parse_trusted_proxies(&self.trusted_proxies)
            .map_err(|message| web_error(phase, message))?;
        if self.cors.enabled {
            // 启用时提前用同一构造器校验(空来源 / `*`+credentials),misconfig 在 Start 就 fail-closed。
            crate::governance::CorsPolicy::new(
                self.cors.allowed_origins.clone(),
                self.cors.allow_credentials,
                self.cors.allowed_methods.clone(),
                self.cors.allowed_headers.clone(),
                self.cors.max_age_secs,
            )
            .map_err(|message| web_error(phase, message))?;
        }
        if self.compression.enabled && self.compression.min_size_bytes == 0 {
            return Err(web_error(
                phase,
                "server.compression.min_size_bytes must be greater than zero",
            ));
        }
        if self.rate_limit.enabled
            && (self.rate_limit.requests_per_second == 0 || self.rate_limit.burst == 0)
        {
            return Err(web_error(
                phase,
                "server.rate_limit.requests_per_second and burst must be greater than zero when enabled",
            ));
        }
        self.http2.validate(phase)?;
        Ok(())
    }
}

/// `ApplicationSpec` 中 Web 工厂的运行期组件所有者。
///
/// Start 只冻结配置，Ready 才构造路由、绑定监听器并形成可逆网络副作用。
pub(crate) struct WebComponent {
    route_meta: WebRouteMetaFactory,
    factory: WebRouterFactory,
    config: Option<ServerConfig>,
    critical_task: Option<ApplicationFuture<'static>>,
    /// Start 登记(封口前)、Ready observe 并交给 monitor 的 mapping/安全运行时就绪贡献句柄。
    mapping_contributor: Option<ReadinessContributor>,
}

impl WebComponent {
    /// 业务作用：从已完成运行时绑定校验的静态描述中取得两个 Web 工厂。
    ///
    /// # 参数
    ///
    /// - `spec`：属性入口生成并已通过同步预检的应用描述。
    pub(crate) fn from_spec(spec: &ApplicationSpec) -> ApplicationResult<Self> {
        let route_meta = spec.web_route_meta().ok_or_else(|| {
            web_error(
                ApplicationPhase::Bootstrap,
                "web route metadata factory is missing",
            )
        })?;
        let factory = spec.web_factory().ok_or_else(|| {
            web_error(ApplicationPhase::Bootstrap, "web router factory is missing")
        })?;
        Ok(Self {
            route_meta,
            factory,
            config: None,
            critical_task: None,
            mapping_contributor: None,
        })
    }
}

impl ApplicationComponent for WebComponent {
    /// 业务作用：返回 Web 组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 使用该身份归类启动和停机错误。
    fn id(&self) -> ComponentId {
        ComponentId::Web
    }

    /// 业务作用：从初始配置快照读取并冻结 Web 设置。
    ///
    /// # 参数
    ///
    /// - `context`：提供已经完成同步预检的 Application 配置视图。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let root: WebConfigRoot = context.application().config_as()?;
            root.server.validate(ApplicationPhase::Start)?;
            context
                .application()
                .set_web_context_path(Arc::from(root.server.context_path.as_str()))?;
            // mapping/安全运行时就绪 contributor 必须在 UserHook 封口前(Start)登记;Ready 发布
            // MappingRuntime 后 observe、并由 monitor 反映运行期热刷新失败。默认非关键:reload 失败保留 last-good、
            // 只 Degraded(`/readyz` 仍 200);`server.mapping_readiness_critical=true` 时升级为关键(affects_ready)
            // → monitor 复审失败置 NotReady → `/readyz` 503。
            let contributor = context.application().register_readiness(
                ComponentId::Web,
                Arc::<str>::from("web:mapping-runtime"),
                ReadinessPolicy {
                    affects_ready: root.server.mapping_readiness_critical,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?;
            self.mapping_contributor = Some(contributor);
            self.config = Some(root.server);
            Ok(())
        })
    }

    /// 业务作用：在业务资源封存后构造路由、绑定端口并激活 Web 停机动作。
    ///
    /// # 参数
    ///
    /// - `context`：提供统一 Application 状态和 active stack 写入口的 Ready 上下文。
    ///
    /// # 返回
    ///
    /// 返回：路由、指标源、listener 与受监督 accept 任务成组建立后成功；任一步失败都阻止接流。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = self.config.clone().ok_or_else(|| {
                web_error(
                    ApplicationPhase::Ready,
                    "web configuration was not prepared during start",
                )
            })?;
            let mut routes = (self.route_meta)();
            validate_routes(&mut routes, config.health)?;
            let route_manifest = build_route_manifest(&routes, config.health);

            let application = context.application().clone();
            let factory = self.factory;
            // 手动 mapping 计划必须在自动路由工厂前封口；工厂随后合并
            // `#[interceptor(global = true)]` 的链接期自动 binding。二者共同参与
            // auth-before-decrypt 排序和启动审计。原始 configure_router 仍位于自动端点之后。
            let mut mapping_plan = naweb::MappingPlan::new();
            for transform in application.take_mapping_transforms() {
                mapping_plan = match catch_unwind(AssertUnwindSafe(move || transform(mapping_plan)))
                {
                    Ok(Ok(plan)) => plan,
                    Ok(Err(error)) => {
                        return Err(ApplicationError::with_source(
                            ComponentId::Web,
                            ApplicationPhase::Ready,
                            "a configure_mapping customization was rejected",
                            error,
                        ));
                    }
                    Err(payload) => {
                        std::mem::forget(payload);
                        return Err(web_error(
                            ApplicationPhase::Ready,
                            "a configure_mapping customization panicked while building the plan",
                        ));
                    }
                };
            }
            let mapping_runtime = mapping_plan.runtime_or_default();
            let build_context =
                WebBuildContext::new(application.clone(), mapping_runtime.clone(), mapping_plan);
            // 装配顺序固定为：手动 mapping plan → 自动 global binding → __mvc 自动端点
            // → configure_router → 框架探针 → with_state → context path → 框架外层。探针必须在业务安全层之外，否则 token
            // 拦截器会把 /healthz 拦成 401，业务指标也会污染框架探针流量。
            let mut router = match catch_unwind(AssertUnwindSafe(move || factory(build_context))) {
                Ok(result) => result?,
                Err(payload) => {
                    // 路由构造 panic 的 payload 可能含业务输入，故只保留稳定阶段错误并放弃格式化。
                    std::mem::forget(payload);
                    return Err(web_error(
                        ApplicationPhase::Ready,
                        "web router factory panicked",
                    ));
                }
            };
            application.publish_mapping_runtime(mapping_runtime.clone())?;
            // mapping/安全运行时就绪:MappingRuntime 已发布且 mvc_router! 建路由时经
            // `audit_route_plans` 冻结路由合同,故首个观测 Ready;spawn monitor 周期执行 `readiness_bound()`
            // (route audit + active key + required replay 后端探测),失败 → Degraded(last-good 仍服务、
            // `/readyz` 保持 200)。monitor 由下面压栈的 action 拥有停机(cancel + 全局预算内 join)。
            if let Some(contributor) = self.mapping_contributor.take() {
                contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                let mapping_critical = self
                    .config
                    .as_ref()
                    .map(|config| config.mapping_readiness_critical)
                    .unwrap_or(false);
                let monitor_cancel = CancellationToken::new();
                let monitor = tokio::spawn(run_mapping_monitor(
                    application.clone(),
                    contributor,
                    mapping_critical,
                    monitor_cancel.clone(),
                ));
                context.activate(Box::new(WebMappingMonitorShutdown {
                    cancel: monitor_cancel,
                    monitor: Some(monitor),
                }));
            }
            // 把 naweb 安全端点指标作为兼容源并入进程级统一 hub(descriptor 冲突审计 +
            // 由 hub 统一渲染)。naweb 自持 registry,本注册只把它的 descriptor 纳入 catalog 并保存源。
            // 仅 web-security 编入 naweb 的 auth/crypto 指标 registry,故 gate 到该特性。
            #[cfg(feature = "web-security")]
            if let Some(runtime) = application.mapping_runtime() {
                let source =
                    std::sync::Arc::new(crate::metrics::NawebMetricsSource::new(runtime.metrics()));
                crate::metrics::register_naweb_source(&application.metrics_hub(), source).map_err(
                    |conflict| {
                        web_error(
                            ApplicationPhase::Ready,
                            format!(
                                "naweb metric descriptor `{}` conflicts with an existing registration",
                                conflict.name
                            ),
                        )
                    },
                )?;
            }
            // 取走即封口：此后业务再调 configure_router 会得到阶段错误，而不是被静默丢弃。
            for transform in application.take_router_transforms() {
                router = match catch_unwind(AssertUnwindSafe(move || transform(router))) {
                    Ok(router) => router,
                    Err(payload) => {
                        // Axum 对重复路由/非法路径是 panic 而非 Result；`panic=unwind` 下把它收敛成启动错误，
                        // `panic=abort` 下无法恢复——这是文档明确记账的自定义 Router 边界。
                        std::mem::forget(payload);
                        return Err(web_error(
                            ApplicationPhase::Ready,
                            "a configure_router customization panicked while building the router",
                        ));
                    }
                };
            }
            if config.health {
                // 探针在业务定制之后挂载，因此不被 configure_router 里的全局 layer 覆盖。
                router = match catch_unwind(AssertUnwindSafe(move || {
                    router
                        .route("/healthz", get(liveness))
                        .route("/readyz", get(readiness))
                })) {
                    Ok(router) => router,
                    Err(payload) => {
                        // 手写路由可能占用保留路径；payload 不参与格式化，启动仍走正常反向清理。
                        std::mem::forget(payload);
                        return Err(web_error(
                            ApplicationPhase::Ready,
                            "a configure_router customization conflicts with a reserved health path",
                        ));
                    }
                };
                // 统一指标端点与探针同属框架 observability 出口,复用 health 开关一并挂载。
                // 仅当进程级 hub 存在(kafka / web-security 编入)时才有内容可暴露。
                #[cfg(any(feature = "kafka", feature = "web"))]
                {
                    router = match catch_unwind(AssertUnwindSafe(move || {
                        router.route("/metrics", get(metrics_endpoint))
                    })) {
                        Ok(router) => router,
                        Err(payload) => {
                            std::mem::forget(payload);
                            return Err(web_error(
                                ApplicationPhase::Ready,
                                "a configure_router customization conflicts with the reserved /metrics path",
                            ));
                        }
                    };
                }
            }
            let mut router = router.with_state(application.clone());
            if !config.context_path.is_empty() {
                router = Router::new().nest(&config.context_path, router);
            }
            // 框架外层顺序固定为 metrics → trace → body limit → router：后 layer 的一层在外，
            // 因此先装 body limit，最后装指标层。指标层覆盖探针与 404，才能如实反映监听器收到的全部请求。
            // 幂等中间件:仅当业务经 UserHook 注入 store 时启用;装在最内层直接包裹 handler,
            // 命中重放即短路 handler。默认(未注入)零行为。
            if let Some(store) = application.idempotency_store() {
                let state = crate::idempotency::IdempotencyLayerState::new(
                    store,
                    mapping_runtime.clone(),
                    Arc::<str>::from(config.context_path.as_str()),
                );
                router = router.layer(from_fn_with_state(state, crate::idempotency::idempotency));
            }
            // route 授权中间件:仅当业务注入策略注册表时启用;装在幂等之外——未授权请求不进入
            // 幂等/handler。主体由下方 authentication 层写入请求扩展。默认零行为。
            let registry = application.authz_registry();
            let object_authorizer = application.object_authorizer();
            if registry.is_some() || object_authorizer.is_some() {
                let (object_authorizer, object_timeout) = object_authorizer
                    .map(|(provider, timeout)| (Some(provider), timeout))
                    .unwrap_or((None, Duration::from_millis(200)));
                let state = crate::authz::AuthorizationLayerState::new(
                    registry,
                    object_authorizer,
                    object_timeout,
                );
                router = router.layer(from_fn_with_state(state, crate::authz::authorize));
            }
            // authentication 中间件:仅当业务注入认证器时启用;装在授权**之外**——认证永远早于
            // 授权。校验 Bearer JWT 通过则把已验证 Principal 写入扩展供 authz 判定;无头匿名放行;校验失败
            // 401。默认零行为。
            if let Some(authenticator) = application.authenticator() {
                router = router.layer(from_fn_with_state(
                    authenticator,
                    crate::authn::authenticate,
                ));
            }
            if let Some(limit) = config.request_body_limit_bytes {
                router = router.layer(DefaultBodyLimit::max(limit));
            }
            if config.trace {
                router = router.layer(TraceLayer::new_for_http());
            }
            // load shed:装在 request id 之内、body limit 之外;过载即早 503 + Retry-After。
            // 默认不启用(None),不改变既有行为。
            if let Some(max_inflight) = config.max_inflight_requests {
                let limit = Arc::new(crate::governance::ConcurrencyLimit::new(
                    max_inflight,
                    config.overload_retry_after_secs,
                ));
                router = router.layer(from_fn_with_state(limit, crate::governance::load_shed));
            }
            // 每客户端限流:装在全局 load shed 之外(单一来源被挡前不占全局并发额)、request-id/
            // 安全头之内(被拒 429 仍带 request-id 与安全头),依赖更外层 resolve_client_ip 写入的 ClientIp。
            // 默认不启用;validate() 已保证启用时 rps/burst 均 > 0。
            if config.rate_limit.enabled {
                let limit = Arc::new(crate::governance::RateLimit::new(
                    f64::from(config.rate_limit.requests_per_second),
                    f64::from(config.rate_limit.burst),
                ));
                router = router.layer(from_fn_with_state(limit, crate::governance::rate_limit));
            }
            // 总 deadline:装在 load shed 之外、panic 边界之内;写入 RequestBudget 供
            // handler/下游读剩余,并对整个请求施加绝对超时。默认不启用(None)。
            if let Some(deadline_ms) = config.request_deadline_ms {
                let total = std::time::Duration::from_millis(deadline_ms);
                router = router.layer(from_fn_with_state(
                    total,
                    crate::governance::enforce_request_deadline,
                ));
            }
            // panic 边界:捕获内层(handler/拦截器/解密等)panic → 固定 500,不泄漏
            // payload/stack;固定 500 再经外层安全头/request-id 正常回传。
            router = router.layer(tower_http::catch_panic::CatchPanicLayer::custom(
                crate::governance::panic_response,
            ));
            // 安全响应头:API 模板默认头,or_insert 不覆盖 handler 显式设置。
            router = router.layer(from_fn(crate::governance::api_security_headers));
            // 响应压缩:默认关。装在安全头之外——响应回程时安全头先加、再由本层压缩体。
            // 谓词 should_compress_response 统一裁决:密文标记 / 已编码 / 非白名单类型一律跳过,
            // 与 SizeAbove 组合再排除小体积,规避压缩+加密同用的 CRIME/BREACH 侧信道。
            if config.compression.enabled {
                use tower_http::compression::predicate::Predicate;
                let predicate = tower_http::compression::predicate::SizeAbove::new(
                    config.compression.min_size_bytes,
                )
                .and(crate::governance::should_compress_response);
                router = router.layer(
                    tower_http::compression::CompressionLayer::new()
                        .gzip(true)
                        .compress_when(predicate),
                );
            }
            // CORS:默认关;启用时预检 OPTIONS 在鉴权之外直接 204,非预检按白名单加 ACAO。
            // 装在安全头之外、request-id 之内——预检 204 仍带 request-id/trace 关联头,并绕过 panic/
            // 限流/deadline/鉴权等内层。validate() 已保证 CorsPolicy::new 成功。
            if config.cors.enabled {
                if let Ok(policy) = crate::governance::CorsPolicy::new(
                    config.cors.allowed_origins.clone(),
                    config.cors.allow_credentials,
                    config.cors.allowed_methods.clone(),
                    config.cors.allowed_headers.clone(),
                    config.cors.max_age_secs,
                ) {
                    router = router.layer(from_fn_with_state(
                        Arc::new(policy),
                        crate::governance::cors,
                    ));
                }
            }
            // request ID:装在指标层之内——指标层在最外先看到请求,随后 request id
            // 校验/生成并写入扩展、响应头回传,供日志/trace/handler 关联。
            router = router.layer(from_fn(crate::governance::attach_request_id));
            // 真实客户端 IP:始终启用。对端可信才采信 XFF,否则客户端即对端(防伪造);
            // 空 trusted_proxies(默认)= 永不采信 XFF。解析结果写入 ClientIp 扩展供限流/日志/业务读取。
            // 依赖下面 serve 的 connect-info 提供对端地址。
            let trusted_proxies = Arc::new(
                crate::governance::parse_trusted_proxies(&config.trusted_proxies)
                    .unwrap_or_default(),
            );
            router = router.layer(from_fn_with_state(
                trusted_proxies,
                crate::governance::resolve_client_ip,
            ));
            // W3C trace context:装在 request-id 之外、指标层之内,使整条服务端处理都落在同一
            // span 下。解析入站 traceparent(有效则沿用同一 trace-id 派生服务端 span,缺失/非法则开新
            // 链路),当前上下文写入扩展供 handler 下游透传、trace-id 回写响应头。与 request-id 同为
            // 始终启用的通用关联层(不 gate 到 config,零业务配置即得分布式追踪关联)。
            // 遥测组件激活(声明 telemetry 且已发布 exporter)时,改用会为每个请求产服务端 span 的变体;
            // 否则(未声明遥测/未启用)保持纯传播。传播行为在两个变体中完全一致。
            #[cfg(feature = "telemetry")]
            {
                if let Some(exporter) = application.telemetry_exporter() {
                    router = router.layer(from_fn_with_state(
                        exporter,
                        crate::trace::trace_context_export,
                    ));
                } else {
                    router = router.layer(from_fn(crate::trace::trace_context));
                }
            }
            #[cfg(not(feature = "telemetry"))]
            {
                router = router.layer(from_fn(crate::trace::trace_context));
            }
            router = router.layer(from_fn_with_state(
                application.web_runtime(),
                observe_web_request,
            ));

            let listener = TcpListener::bind((config.host.as_str(), config.port))
                .await
                .map_err(|error| {
                    // host/port 是定位信息而非秘密，直接进 message；OS 根因经错误链脱敏后输出。
                    ApplicationError::with_source(
                        ComponentId::Web,
                        ApplicationPhase::Ready,
                        format!(
                            "web listener bind failed on {}:{}",
                            config.host, config.port
                        ),
                        error,
                    )
                })?;
            let address = listener.local_addr().map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Web,
                    ApplicationPhase::Ready,
                    "web listener local address is unavailable",
                    error,
                )
            })?;
            let web_runtime = application.web_runtime();
            application
                .metrics_hub()
                .register_legacy_source_reserved(web_runtime.clone(), 8)
                .map_err(|error| match error {
                    nametrics_core::MetricSourceRegistrationError::Conflict(conflict) => web_error(
                        ApplicationPhase::Ready,
                        format!(
                            "web metric descriptor `{}` conflicts with an existing registration",
                            conflict.name
                        ),
                    ),
                    nametrics_core::MetricSourceRegistrationError::SeriesBudgetExceeded => {
                        web_error(
                            ApplicationPhase::Ready,
                            "web metric series reservation exceeds the process limit",
                        )
                    }
                })?;
            application.publish_web_runtime(address, route_manifest)?;
            // 监听地址与 context path 是启动完成的关键可观测信号；
            // 无 log 组件时由 run() 安装的兜底 subscriber 承接。
            if config.context_path.is_empty() {
                tracing::info!("web listening on {address}");
            } else {
                tracing::info!(
                    "web listening on {address} (context path `{}`)",
                    config.context_path
                );
            }

            let stop = CancellationToken::new();
            let task_stop = stop.clone();
            let drain_budget = config
                .graceful_shutdown_timeout_ms
                .map(std::time::Duration::from_millis);
            let transport = config.http2.clone();
            self.critical_task = Some(Box::pin(async move {
                run_web_listener(
                    listener,
                    router,
                    transport,
                    task_stop,
                    drain_budget,
                    web_runtime,
                )
                .await
            }));
            // 地址发布和任务构造均成功后再压栈；从这一点起任何退出路径都能先停止接收新请求。
            context.activate(Box::new(WebShutdown { stop }));
            Ok(())
        })
    }

    /// 业务作用：把已绑定监听器的服务 future 移交给 Runner 关键任务监督集合。
    ///
    /// # 参数
    ///
    /// 本方法无参数；任务只允许被取出一次，重复调用返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task.take().map(|task| ("web-accept", task))
    }
}

/// 持有 Web graceful stop 令牌的可逆 active action。
struct WebShutdown {
    stop: CancellationToken,
}

impl ShutdownAction for WebShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不包含监听地址或配置值。
    fn label(&self) -> &'static str {
        "web-active"
    }

    /// 业务作用：通知服务停止接收新连接并开始等待在途请求完成。
    ///
    /// # 参数
    ///
    /// - `_context`：Runner 后续收割关键任务时使用的共享停机预算。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.stop.cancel();
        Box::pin(async { Ok(()) })
    }
}

/// 业务作用：mapping/安全运行时就绪 monitor:周期执行 [`naweb::MappingRuntime::readiness_bound`],把安全
/// 运行时的**完整就绪合同**反映进 `/readyz`。
///
/// `readiness_bound` 用启动期(mvc_router! 建路由时经 `audit_route_plans` 冻结)的 last-good 路由/interceptor
/// 合同,对当前 last-good 快照复审路由(route audit)、校验 active 签名 key,并对声明 required replay 的路由
/// 探测 replay 后端可用性。任一失败(路由审计漂移 / active key 缺失 / required replay 后端不可用)→ Degraded
/// (last-good 快照仍在服务、`/readyz` 保持 200;非关键——不把可恢复的后端抖动升级成整实例摘流);成功→Ready。
/// 进入停机态即优雅退出。`readiness_bound` 只读 last-good、由 monitor 低频执行,绝不从 `/readyz` handler 直接
/// 调远程后端。
///
/// # 参数
///
/// - `application`:读取全局生命周期状态与已发布 MappingRuntime。
/// - `contributor`:mapping 就绪贡献句柄。
/// - `critical`:失败是否升级为关键(`server.mapping_readiness_critical`):`true` → 复审失败 NotReady(→503),
///   `false` → 复审失败 Degraded(last-good 仍服务、`/readyz` 保持 200)。
/// - `cancel`:停机取消令牌。
async fn run_mapping_monitor(
    application: Application,
    contributor: ReadinessContributor,
    critical: bool,
    cancel: CancellationToken,
) {
    /// mapping 就绪轮询周期。
    const INTERVAL: Duration = Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return;
            }
            _ = tokio::time::sleep(INTERVAL) => {}
        }
        match application.state() {
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
                return;
            }
            ApplicationState::Starting => continue,
            ApplicationState::Ready => {}
        }
        match application.mapping_runtime() {
            Some(runtime) => match runtime.readiness_bound().await {
                // 路由审计 + active key + required replay 后端探测均通过。
                Ok(_audit) => {
                    contributor.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                }
                // 安全合同复审失败:last-good 仍服务。默认非关键 Degraded(不摘流);
                // `mapping_readiness_critical` 时升级为 NotReady(affects_ready 关键 → `/readyz` 503)。
                Err(_error) => {
                    let state = if critical {
                        DependencyState::NotReady
                    } else {
                        DependencyState::Degraded
                    };
                    contributor.observe(state, reason::ROUTE_AUDIT_FAILED, Instant::now());
                }
            },
            // Ready 后 MappingRuntime 恒已发布;None 不应发生,保守发未就绪。
            None => {
                contributor.observe(DependencyState::NotReady, reason::NOT_READY, Instant::now())
            }
        }
    }
}

/// 持有 mapping 就绪 monitor 的可逆停机 action:取消并在全局剩余预算内 join(非关键辅助任务)。
struct WebMappingMonitorShutdown {
    /// monitor 停机取消令牌。
    cancel: CancellationToken,
    /// spawned monitor 句柄;只 join 一次。
    monitor: Option<JoinHandle<()>>,
}

impl ShutdownAction for WebMappingMonitorShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数;名称不含配置值。
    fn label(&self) -> &'static str {
        "web-mapping-monitor"
    }

    /// 业务作用：取消 monitor 并在全局剩余停机预算内 join;超时不阻断其余清理(辅助任务)。
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
                    .expect("mapping monitor remains installed while shutdown awaits it");
                monitor.abort();
                let _ = monitor.await;
            } else {
                self.monitor.take();
            }
            Ok(())
        })
    }
}

impl Drop for WebMappingMonitorShutdown {
    /// 业务作用：停机 future 被取消或 guard 提前释放时终止 mapping monitor，避免 detached task。
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(monitor) = self.monitor.take() {
            monitor.abort();
        }
    }
}

/// 业务作用：验证自动端点的保留路径、重复方法和结构路径冲突。
///
/// # 参数
///
/// - `routes`：业务二进制投影出的全部静态路由元数据。
/// - `health_enabled`：是否需要为存活和就绪探针保留完整路径。
fn validate_routes(routes: &mut [RouteMeta], health_enabled: bool) -> ApplicationResult<()> {
    routes.sort_by_key(|route| (route.path, route.method, route.handler));
    let mut path_tree = matchit::Router::new();
    let mut exact = HashMap::<&'static str, HashMap<&'static str, &'static str>>::new();

    if health_enabled {
        for path in ["/healthz", "/readyz"] {
            path_tree
                .insert(path, "application-probe")
                .map_err(|error| {
                    web_error(
                        ApplicationPhase::Ready,
                        format!("cannot reserve health route `{path}`: {error}"),
                    )
                })?;
        }
    }

    for route in routes.iter().copied() {
        validate_route_path(&route)?;
        if health_enabled && matches!(route.path, "/healthz" | "/readyz") {
            return Err(web_error(
                ApplicationPhase::Ready,
                format!(
                    "route `{}` from `{}` conflicts with a reserved health path",
                    route.path, route.handler
                ),
            ));
        }

        if let Some(methods) = exact.get_mut(route.path) {
            if let Some(previous) = methods.insert(route.method, route.handler) {
                return Err(web_error(
                    ApplicationPhase::Ready,
                    format!(
                        "duplicate route {} {} from `{previous}` and `{}`",
                        route.method, route.path, route.handler
                    ),
                ));
            }
            continue;
        }

        path_tree
            .insert(route.path, route.handler)
            .map_err(|error| {
                web_error(
                    ApplicationPhase::Ready,
                    format!(
                        "route path conflict for `{}` from `{}`: {error}",
                        route.path, route.handler
                    ),
                )
            })?;
        exact.insert(route.path, HashMap::from([(route.method, route.handler)]));
    }
    Ok(())
}

/// 业务作用：从预检后的静态元数据构造对外只读路由清单。
///
/// # 参数
///
/// - `routes`：已经按路径、方法和处理器稳定排序的业务路由元数据。
/// - `health_enabled`：是否把存活和就绪探针加入清单。
fn build_route_manifest(routes: &[RouteMeta], health_enabled: bool) -> Arc<[RouteInfo]> {
    let mut manifest = routes
        .iter()
        .copied()
        .map(RouteInfo::business)
        .collect::<Vec<_>>();
    if health_enabled {
        manifest.extend([
            RouteInfo::runtime("GET", "/healthz", "napp::web::liveness"),
            RouteInfo::runtime("GET", "/readyz", "napp::web::readiness"),
        ]);
    }
    manifest.sort_by_key(|route| (route.path(), route.method(), route.handler()));
    Arc::from(manifest)
}

/// 业务作用：补充底层路径树未覆盖的静态路由格式检查。
///
/// # 参数
///
/// - `route`：需要在构造 Router 前验证的单条自动端点元数据。
fn validate_route_path(route: &RouteMeta) -> ApplicationResult<()> {
    if !route.path.starts_with('/') {
        return Err(web_error(
            ApplicationPhase::Ready,
            format!("route path from `{}` must start with `/`", route.handler),
        ));
    }
    if route
        .path
        .split('/')
        .skip(1)
        .any(|segment| segment.starts_with(':') || segment.starts_with('*'))
    {
        return Err(web_error(
            ApplicationPhase::Ready,
            format!(
                "route `{}` from `{}` uses an unsupported parameter segment",
                route.path, route.handler
            ),
        ));
    }
    Ok(())
}

/// 业务作用：返回不依赖 Application 状态的存活探针结果。
///
/// # 参数
///
/// 本函数无参数；监听器能处理请求即返回成功。
async fn liveness() -> StatusCode {
    StatusCode::OK
}

/// 业务作用：根据公开生命周期状态返回就绪探针结果。
///
/// # 参数
///
/// - `application`：当前 Web Router 持有的统一 Application 状态。
async fn readiness(State(application): State<Application>) -> StatusCode {
    if application.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// 业务作用：把进程级统一指标 hub 的原生源与兼容源渲染为同一份 Prometheus 文本。
///
/// # 参数
///
/// - `application`：当前 Web Router 持有的统一 Application 状态,经它取进程级 hub。
#[cfg(any(feature = "kafka", feature = "web"))]
async fn metrics_endpoint(State(application): State<Application>) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    // 外部事实源失败时仍发布其它独立指标族与该源的 last-good 快照；源自身必须同时暴露
    // refresh_failed 和 snapshot_age，避免单一后端抖动让整个进程的观测面失明。
    let _ = application.refresh_metric_sources().await;
    let mut body = String::new();
    application.metrics_hub().render_prometheus(&mut body);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

/// 业务作用：在最外层 Web 边界记录请求进入、响应状态和在途数量。
///
/// 守卫的析构路径覆盖 future 被取消的情况，确保摘流期间不会留下虚高的在途计数。
///
/// # 参数
///
/// - `runtime`：只包含 Web 元数据和原子计数器的共享运行时状态。
/// - `request`：即将进入后续中间件和路由服务的请求。
/// - `next`：当前中间件之后的完整请求处理链。
///
/// # 返回
///
/// 返回：后续处理链生成的响应，并在形成响应前完成协议与状态会计。
async fn observe_web_request(
    State(runtime): State<Arc<WebRuntimeState>>,
    request: Request,
    next: Next,
) -> Response {
    let version = request.version();
    let guard = WebRuntimeState::begin_request(&runtime, version == axum::http::Version::HTTP_2);
    let response = next.run(request).await;
    guard.complete(response.status().as_u16());
    response
}

/// Web listener 在协议判定后使用的有限协议集合。
#[derive(Clone, Copy)]
enum WebConnectionProtocol {
    /// HTTP/1.0 或 HTTP/1.1，由 hyper 的 HTTP/1 driver 继续精确解析。
    Http1,
    /// 明文 prior knowledge HTTP/2。
    Http2,
}

/// 在协议判定前暂存已读取字节，并在 hyper driver 首次读取时原样回放。
struct RewindTcpStream {
    stream: TcpStream,
    prefix: Vec<u8>,
    offset: usize,
}

impl RewindTcpStream {
    /// 业务作用：把协议判定期间读取的前言与原 TCP stream 重新组合为无损字节流。
    ///
    /// # 参数
    ///
    /// - `stream`：已经建立、尚未交给 HTTP driver 的 TCP 连接。
    /// - `prefix`：协议判定期间按网络顺序读出的字节。
    ///
    /// # 返回
    ///
    /// 返回：先回放 `prefix`、再继续读取原连接的异步 I/O 对象。
    fn new(stream: TcpStream, prefix: Vec<u8>) -> Self {
        Self {
            stream,
            prefix,
            offset: 0,
        }
    }
}

impl AsyncRead for RewindTcpStream {
    /// 业务作用：保证协议判定消耗的字节先于后续网络字节交给 HTTP driver。
    ///
    /// # 参数
    ///
    /// - `self`：当前回放位置与底层连接。
    /// - `context`：异步读取任务上下文。
    /// - `buffer`：接收字节的目标缓冲区。
    ///
    /// # 返回
    ///
    /// 返回：已有前言立即写入；前言耗尽后透传底层 TCP 读取结果。
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset < self.prefix.len() && buffer.remaining() > 0 {
            let available = &self.prefix[self.offset..];
            let copied = available.len().min(buffer.remaining());
            buffer.put_slice(&available[..copied]);
            self.offset += copied;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for RewindTcpStream {
    /// 业务作用：把 HTTP driver 的响应字节直接写入原 TCP 连接。
    ///
    /// # 参数
    ///
    /// - `self`：持有底层连接的回放对象。
    /// - `context`：异步写入任务上下文。
    /// - `buffer`：待发送的响应字节。
    ///
    /// # 返回
    ///
    /// 返回：底层 TCP 写入的进度或 I/O 错误。
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    /// 业务作用：把 HTTP driver 的刷新要求传递给原 TCP 连接。
    ///
    /// # 参数
    ///
    /// - `self`：持有底层连接的回放对象。
    /// - `context`：异步刷新任务上下文。
    ///
    /// # 返回
    ///
    /// 返回：底层 TCP 刷新完成状态或 I/O 错误。
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    /// 业务作用：在 HTTP driver 结束连接时关闭原 TCP 写方向。
    ///
    /// # 参数
    ///
    /// - `self`：持有底层连接的回放对象。
    /// - `context`：异步关闭任务上下文。
    ///
    /// # 返回
    ///
    /// 返回：底层 TCP 关闭完成状态或 I/O 错误。
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }

    /// 业务作用：透传分段写入，保持 upgrade 与普通响应的写入语义一致。
    ///
    /// # 参数
    ///
    /// - `self`：持有底层连接的回放对象。
    /// - `context`：异步写入任务上下文。
    /// - `buffers`：按顺序发送的响应字节片段。
    ///
    /// # 返回
    ///
    /// 返回：底层 TCP 分段写入的进度或 I/O 错误。
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(context, buffers)
    }

    /// 业务作用：报告底层 TCP 是否支持有效的分段写入。
    ///
    /// # 参数
    ///
    /// 参数说明: 无。
    ///
    /// # 返回
    ///
    /// 返回：底层连接对 vectored write 的能力标志。
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

/// 业务作用：在明文连接上以 HTTP/2 固定前言判定 h2c，并完整保留已读取字节供后续 driver 使用。
///
/// 逐字节比较可让普通 HTTP/1 请求在首个不匹配字节立即分流，避免为短请求等待完整前言。
///
/// # 参数
///
/// - `stream`：新接受且尚未交给 HTTP driver 的明文 TCP 连接。
/// - `timeout`：慢速前言占用连接容量的最长时间。
///
/// # 返回
///
/// 返回：可无损回放前言的连接及协议；超时或连接提前关闭时返回 I/O 错误。
async fn detect_web_protocol(
    mut stream: TcpStream,
    timeout: Duration,
) -> io::Result<(RewindTcpStream, WebConnectionProtocol)> {
    const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    let mut prefix = Vec::with_capacity(H2_PREFACE.len());
    let detect = async {
        for expected in H2_PREFACE {
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte).await?;
            prefix.push(byte[0]);
            if byte[0] != *expected {
                return Ok::<WebConnectionProtocol, io::Error>(WebConnectionProtocol::Http1);
            }
        }
        Ok::<WebConnectionProtocol, io::Error>(WebConnectionProtocol::Http2)
    };
    let protocol = tokio::time::timeout(timeout, detect)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "web protocol preface timed out"))??;
    Ok((RewindTcpStream::new(stream, prefix), protocol))
}

/// 业务作用：运行受管 Web accept 循环，并把连接容量、停机信号和排空预算统一交给同一所有者。
///
/// # 参数
///
/// - `listener`：Ready 阶段已绑定并发布地址的 TCP listener。
/// - `router`：已经封口且包含治理中间件的 Axum 路由图。
/// - `config`：本次进程冻结的连接与 HTTP/2 transport 配置。
/// - `stop`：停止接收新连接并通知存量连接 graceful shutdown 的令牌。
/// - `drain_budget`：摘流后等待连接结束的可选组件子预算。
/// - `runtime`：Web 能力句柄和指标端点共用的原子观测状态。
///
/// # 返回
///
/// 返回：收到停机信号并按预算完成或放弃排空后成功；单连接协议错误不会结束整个 listener。
async fn run_web_listener(
    listener: TcpListener,
    router: Router,
    config: Http2Config,
    stop: CancellationToken,
    drain_budget: Option<Duration>,
    runtime: Arc<WebRuntimeState>,
) -> ApplicationResult<()> {
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => {
                // 停机令牌先关闭 accept 入口，确保排空阶段不会继续接纳新的业务连接。
                break;
            },
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::warn!(error = %error, "web connection task ended unexpectedly");
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer_addr)) => {
                        WebRuntimeState::accept_connection(&runtime);
                        let permit = match Arc::clone(&permits).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                // 容量耗尽时在协议解析前拒绝，避免额外连接继续占用内存和任务槽。
                                WebRuntimeState::reject_connection(&runtime);
                                drop(stream);
                                continue;
                            }
                        };
                        connections.spawn(drive_web_connection(
                            stream,
                            peer_addr,
                            router.clone(),
                            config.clone(),
                            stop.clone(),
                            permit,
                            Arc::clone(&runtime),
                        ));
                    }
                    Err(error) => {
                        WebRuntimeState::record_accept_error(&runtime);
                        if !matches!(
                            error.kind(),
                            io::ErrorKind::ConnectionRefused
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset
                        ) {
                            tracing::error!(error = %error, "web listener accept failed; retrying");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }
    drop(listener);

    let drain = async {
        while let Some(joined) = connections.join_next().await {
            if let Err(error) = joined {
                tracing::warn!(error = %error, "web connection task ended unexpectedly");
            }
        }
    };
    match drain_budget {
        Some(budget) => {
            if tokio::time::timeout(budget, drain).await.is_err() {
                // 组件子预算耗尽后终止剩余任务；Runner 的全局预算仍负责监督关键任务本身。
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                tracing::warn!(
                    "web drain budget exhausted; remaining in-flight requests were abandoned"
                );
            }
        }
        None => drain.await,
    }
    Ok(())
}

/// 业务作用：为单条连接完成协议判定、注入直连对端身份并运行对应的受管 HTTP driver。
///
/// # 参数
///
/// - `stream`：accept 循环接纳的新 TCP 连接。
/// - `peer_addr`：真实直连对端地址，供可信代理与客户端 IP 解析使用。
/// - `router`：本次 listener 冻结的路由服务图。
/// - `config`：连接与 HTTP/2 transport 配置。
/// - `stop`：listener 级停机令牌。
/// - `_permit`：连接容量所有权，函数结束时自动归还。
/// - `runtime`：连接与请求观测使用的共享原子状态。
///
/// # 返回
///
/// 返回：无；连接错误被记录并隔离，不传播为整个 Web listener 的退出。
async fn drive_web_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    router: Router,
    config: Http2Config,
    stop: CancellationToken,
    _permit: OwnedSemaphorePermit,
    runtime: Arc<WebRuntimeState>,
) {
    let _active = WebRuntimeState::track_connection(&runtime);
    let selected = if config.enabled {
        detect_web_protocol(
            stream,
            Duration::from_millis(config.connection_handshake_timeout_ms),
        )
        .await
    } else {
        Ok((
            RewindTcpStream::new(stream, Vec::new()),
            WebConnectionProtocol::Http1,
        ))
    };
    let (stream, protocol) = match selected {
        Ok(selected) => selected,
        Err(error) => {
            WebRuntimeState::record_connection_error(&runtime);
            tracing::debug!(peer = %peer_addr, error = %error, "web connection protocol detection failed");
            return;
        }
    };
    let result = match protocol {
        WebConnectionProtocol::Http1 => {
            drive_http1_connection(stream, peer_addr, router, &config, stop).await
        }
        WebConnectionProtocol::Http2 => {
            drive_http2_connection(stream, peer_addr, router, &config, stop).await
        }
    };
    if let Err(error) = result {
        WebRuntimeState::record_connection_error(&runtime);
        tracing::debug!(peer = %peer_addr, error = %error, "web connection ended with a protocol error");
    }
}

/// 业务作用：运行保留 upgrade 能力的 HTTP/1 连接，并在停机或连接年龄到达时关闭 keep-alive。
///
/// # 参数
///
/// - `stream`：已判定为 HTTP/1 且可回放全部已读字节的连接。
/// - `peer_addr`：写入 `ConnectInfo` 的直连对端地址。
/// - `router`：本次 listener 冻结的路由服务图。
/// - `config`：连接年龄配置。
/// - `stop`：listener 级停机令牌。
///
/// # 返回
///
/// 返回：连接正常结束或完成 graceful shutdown 时成功，协议或 I/O 失败时返回 hyper 错误。
async fn drive_http1_connection(
    stream: RewindTcpStream,
    peer_addr: SocketAddr,
    router: Router,
    config: &Http2Config,
    stop: CancellationToken,
) -> Result<(), hyper::Error> {
    let service = hyper_util::service::TowerToHyperService::new(
        router.layer(Extension(ConnectInfo(peer_addr))),
    );
    let connection = hyper::server::conn::http1::Builder::new()
        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
        .with_upgrades();
    tokio::pin!(connection);
    let age = wait_for_connection_age(config.max_connection_age_ms);
    tokio::pin!(age);
    tokio::select! {
        result = &mut connection => result,
        _ = stop.cancelled() => {
            // 停机先禁止 HTTP/1 keep-alive 复用，再等待当前请求或 upgrade 所有权自然结束。
            connection.as_mut().graceful_shutdown();
            connection.await
        }
        _ = &mut age => {
            // 年龄轮转只关闭 keep-alive 复用，避免中断已经接纳的请求或 upgrade 所有权。
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    }
}

/// 业务作用：按已校验限额运行 h2c 连接，并在停机或连接年龄到达时以 GOAWAY 收口新 stream。
///
/// # 参数
///
/// - `stream`：已确认带 HTTP/2 prior knowledge 前言且可回放全部已读字节的连接。
/// - `peer_addr`：写入 `ConnectInfo` 的直连对端地址。
/// - `router`：本次 listener 冻结的路由服务图。
/// - `config`：已校验并冻结的 HTTP/2 transport 配置。
/// - `stop`：listener 级停机令牌。
///
/// # 返回
///
/// 返回：连接正常结束或完成 GOAWAY 排空时成功，协议或 I/O 失败时返回 hyper 错误。
async fn drive_http2_connection(
    stream: RewindTcpStream,
    peer_addr: SocketAddr,
    router: Router,
    config: &Http2Config,
    stop: CancellationToken,
) -> Result<(), hyper::Error> {
    let service = hyper_util::service::TowerToHyperService::new(
        router.layer(Extension(ConnectInfo(peer_addr))),
    );
    let mut builder =
        hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder
        .timer(hyper_util::rt::TokioTimer::new())
        .adaptive_window(false)
        .initial_stream_window_size(Some(config.initial_stream_window_size))
        .initial_connection_window_size(Some(config.initial_connection_window_size))
        .max_concurrent_streams(Some(config.max_concurrent_streams))
        .keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval_ms)))
        .keep_alive_timeout(Duration::from_millis(config.keep_alive_timeout_ms))
        .max_pending_accept_reset_streams(Some(config.max_pending_accept_reset_streams))
        .max_local_error_reset_streams(Some(config.max_local_error_reset_streams))
        .max_frame_size(Some(config.max_frame_size))
        .max_header_list_size(config.max_header_list_size)
        .header_table_size(Some(config.header_table_size))
        .max_send_buf_size(config.max_send_buffer_size)
        .enable_connect_protocol();
    let connection = builder.serve_connection(hyper_util::rt::TokioIo::new(stream), service);
    tokio::pin!(connection);
    let age = wait_for_connection_age(config.max_connection_age_ms);
    tokio::pin!(age);
    tokio::select! {
        result = &mut connection => result,
        _ = stop.cancelled() => {
            // HTTP/2 必须先发送 GOAWAY 阻止新 stream，再等待已接纳请求完成。
            connection.as_mut().graceful_shutdown();
            connection.await
        }
        _ = &mut age => {
            // 年龄轮转先发送 GOAWAY，确保新 stream 转移到其它连接且存量 stream 可继续排空。
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    }
}

/// 业务作用：把可选连接年龄转换为可参与 `select` 的单次到期信号。
///
/// # 参数
///
/// - `max_connection_age_ms`：主动轮转时长；`None` 表示永久等待外部停机或连接自行结束。
///
/// # 返回
///
/// 返回：配置时长到达后完成；未配置时保持 pending。
async fn wait_for_connection_age(max_connection_age_ms: Option<u64>) {
    match max_connection_age_ms {
        Some(age) => tokio::time::sleep(Duration::from_millis(age)).await,
        None => std::future::pending::<()>().await,
    }
}

/// 业务作用：在不产生任何副作用的前提下校验候选配置树中的 `server` 段。
///
/// 供配置热刷新在发布候选前使用：段非法时整帧候选不发布，运行中的监听器保持不变。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_server_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let root: WebConfigRoot = serde_json::from_value(tree.clone()).map_err(|error| {
        ApplicationError::with_source(
            ComponentId::Web,
            phase,
            "invalid `server` configuration section",
            error,
        )
    })?;
    root.server.validate(phase)
}

/// 业务作用：创建 Web 组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的 Web 生命周期阶段。
/// - `message`：不包含请求体或配置秘密的诊断摘要。
fn web_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Web, phase, message)
}
