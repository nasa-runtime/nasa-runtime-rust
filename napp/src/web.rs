use std::{
    collections::HashMap,
    net::SocketAddr,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::task::JoinHandle;

use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{from_fn, from_fn_with_state, Next},
    response::Response,
    routing::get,
    Router,
};
use serde::Deserialize;
use tokio::net::TcpListener;
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
            self.critical_task = Some(Box::pin(async move {
                // 带 connect-info 的 make-service:让治理层拿到直连对端地址(真实客户端 IP 解析所需)。
                let serve = std::future::IntoFuture::into_future(
                    axum::serve(
                        listener,
                        router.into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .with_graceful_shutdown(task_stop.clone().cancelled_owned()),
                );
                tokio::pin!(serve);
                let Some(budget) = drain_budget else {
                    return serve.await.map_err(web_serve_error);
                };
                // server 子预算只在摘流开始后计时；全局停机预算仍由 Runner 的 join timeout 兜住，
                // 两者取较小值即 要求的 min(server graceful, remaining budget)。
                tokio::select! {
                    result = &mut serve => result.map_err(web_serve_error),
                    _ = task_stop.cancelled() => {
                        match tokio::time::timeout(budget, serve).await {
                            Ok(result) => result.map_err(web_serve_error),
                            Err(_) => {
                                // 预算耗尽即放弃剩余在途请求；这一步是可预期的停机行为而非故障，
                                // 因此返回 Ok，由 Runner 按任务组状态归类。
                                tracing::warn!(
                                    "web drain budget exhausted; remaining in-flight requests were abandoned"
                                );
                                Ok(())
                            }
                        }
                    }
                }
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

/// 业务作用：渲染进程级统一指标 hub 为 Prometheus 文本:nafka(原生)+ naweb(兼容源)。
///
/// # 参数
///
/// - `application`：当前 Web Router 持有的统一 Application 状态,经它取进程级 hub。
#[cfg(any(feature = "kafka", feature = "web"))]
async fn metrics_endpoint(State(application): State<Application>) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let mut body = String::new();
    application.metrics_hub().render_prometheus(&mut body);
    // Outbox 组件声明且 Ready 时必须完整暴露其积压/死信/预算文本:该组件的指标含
    // 数据库实测值,渲染失败按 503 拒绝整个抓取,禁止以缺失指标伪装健康;
    // 未声明组件或组件未就绪(启动/停机窗口)时不追加,不阻塞其余指标。
    if let Ok(outbox) = application.outbox() {
        match outbox.render_prometheus().await {
            Ok(text) => body.push_str(&text),
            Err(_) => {
                return (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "outbox metrics unavailable",
                )
                    .into_response();
            }
        }
    }
    // Saga Redis Streams 消费面的进程内计数:纯内存读取,无失败分支。
    #[cfg(feature = "saga-redis-stream")]
    body.push_str(&crate::saga::render_stream_metrics(
        &application.saga_runtime(),
    ));
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
async fn observe_web_request(
    State(runtime): State<Arc<WebRuntimeState>>,
    request: Request,
    next: Next,
) -> Response {
    let guard = WebRuntimeState::begin_request(&runtime);
    let response = next.run(request).await;
    guard.complete(response.status().as_u16());
    response
}

/// 业务作用：把底层服务循环错误包装为带组件和阶段的框架错误。
///
/// # 参数
///
/// - `error`：accept 循环返回的底层 IO 错误。
fn web_serve_error(error: std::io::Error) -> ApplicationError {
    ApplicationError::with_source(
        ComponentId::Web,
        ApplicationPhase::Running,
        "web listener exited with an error",
        error,
    )
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
