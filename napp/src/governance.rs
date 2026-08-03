//! Web 生产治理中间件:按 顺序装配的请求关联与韧性层。
//!
//! 首批:**request ID**——入站可信值经校验后沿用,否则生成;写入请求扩展供日志/trace/handler
//! 读取,并在响应头回传。只进日志/trace,**不**作为无限指标 label。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::header::HeaderName;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
pub use nabudget::RequestBudget;
use tokio::sync::Semaphore;

/// 请求关联 ID。业务/拦截器可从请求扩展 `req.extensions().get::<RequestId>()` 读取。
#[derive(Clone, Debug)]
pub struct RequestId(Arc<str>);

impl RequestId {
    /// 借用 ID 文本。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RequestId {
    /// 将已校验请求 ID 原样写入日志字段或响应头。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// 请求关联 ID 的请求/响应头名。
pub(crate) const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// 入站 request id 长度上限;超限或非法字符即视为不可信,改为生成。
const MAX_REQUEST_ID_LEN: usize = 128;

/// 入站 request id 是否可信:非空、长度有界、仅 ASCII 字母数字与 `-` `_` `.`(避免头注入/高基数)。
pub fn is_trusted_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REQUEST_ID_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 生成 OS 随机的 128-bit request id；跨实例、跨重启保持低碰撞。
fn generate_request_id() -> String {
    natelemetry::TraceContext::new_root(true).trace_id_hex()
}

/// 解析或生成本次请求的关联 ID；纯逻辑保证入口与下游使用同一信任边界。
pub fn resolve_request_id(inbound: Option<&str>) -> String {
    match inbound {
        Some(value) if is_trusted_request_id(value) => value.to_owned(),
        _ => generate_request_id(),
    }
}

/// request ID 中间件:校验/生成 → 写入请求扩展 → 响应头回传。
pub async fn attach_request_id(mut request: Request, next: Next) -> Response {
    let mut values = request.headers().get_all(&REQUEST_ID_HEADER).iter();
    let inbound = match (values.next(), values.next()) {
        (Some(value), None) => value.to_str().ok(),
        _ => None,
    };
    let id = resolve_request_id(inbound);

    request
        .extensions_mut()
        .insert(RequestId(Arc::from(id.as_str())));

    let mut response = next.run(request).await;
    // id 只含受信字符,HeaderValue 转换恒成功;仍防御性处理。
    if let Ok(header_value) = HeaderValue::from_str(&id) {
        response
            .headers_mut()
            .insert(REQUEST_ID_HEADER, header_value);
    }
    response
}

/// 全局并发准入(load shed):在途请求达上限时**尽早** 503 + `Retry-After`,而不是无界排队、
/// 拖垮内存与下游。单实例保护本进程;跨副本的业务配额是另一层(`RateLimitProvider`),不混为一谈。
pub(crate) struct ConcurrencyLimit {
    semaphore: Arc<Semaphore>,
    retry_after_secs: u64,
}

impl ConcurrencyLimit {
    /// 创建在途请求准入器；非 fallible 入口把零值和超大值收敛到 Tokio 可表达范围。
    ///
    /// # 参数
    ///
    /// - `max_inflight`:同时在途请求上限,必须大于 0。
    /// - `retry_after_secs`:过载响应回传的建议重试秒数。
    pub(crate) fn new(max_inflight: usize, retry_after_secs: u64) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(
                max_inflight.clamp(1, Semaphore::MAX_PERMITS),
            )),
            retry_after_secs,
        }
    }
}

/// load shed 中间件:抢不到许可即 503 + `Retry-After`;抢到则持许可执行请求,完成后释放。
pub async fn load_shed(
    State(limit): State<Arc<ConcurrencyLimit>>,
    request: Request,
    next: Next,
) -> Response {
    match Arc::clone(&limit.semaphore).try_acquire_owned() {
        Ok(permit) => {
            let response = next.run(request).await;
            drop(permit); // 请求完成后释放许可(显式,便于阅读)。
            response
        }
        Err(_) => {
            let retry_after = limit.retry_after_secs.to_string();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, retry_after)],
                "service overloaded",
            )
                .into_response()
        }
    }
}

/// 单实例**每客户端**限流:按真实客户端 IP 的令牌桶。与 load shed 同族——都只保护本进程;
/// 跨副本的租户/主体总配额是另一层(`RateLimitProvider` + 共享原子后端),两者正确性合同不同,不混为一谈。
///
/// 令牌桶:每客户端一个桶,按 `rate_per_sec` 匀速补充、封顶到 `burst`(容忍短时突发);每请求消费一个
/// 令牌,桶空即拒。内存背压:整表一把锁(临界区仅算术与 map 操作,不跨 `await`),到点清扫已回满(空闲)
/// 的桶把占用界定在「活跃窗口内不同客户端数」,并以 `max_keys` 作硬上限——达上限且无空闲桶可清时对**新**
/// 客户端 fail-open(单实例限流是进程护栏,不因内存上限误伤正常流量;更内层的全局并发 load shed 仍兜底)。
pub struct RateLimit {
    /// 每秒补充令牌数(平均放行速率),必须 > 0。
    rate_per_sec: f64,
    /// 桶容量(突发上限),必须 >= 1。
    burst: f64,
    /// 每客户端桶表 + 上次清扫时刻;一把锁保护,临界区极短。
    state: Mutex<RateLimitState>,
    /// 桶表条目硬上限(内存护栏)。
    max_keys: usize,
    /// 清扫间隔:每隔此时长移除已回满的空闲桶。
    sweep_interval: Duration,
}

/// `RateLimit` 的可变状态:客户端桶表与上次清扫时刻。
struct RateLimitState {
    /// 每客户端 IP → 令牌桶。
    buckets: HashMap<IpAddr, TokenBucket>,
    /// 上次清扫空闲桶的时刻。
    last_sweep: Instant,
}

/// 单个客户端的令牌桶:当前令牌数与上次补充时刻。
struct TokenBucket {
    /// 当前可用令牌(可为小数)。
    tokens: f64,
    /// 上次补充的时刻,用于按流逝时间累加令牌。
    last_refill: Instant,
}

impl RateLimit {
    /// 创建每客户端限流器。
    ///
    /// # 参数
    ///
    /// - `requests_per_second`:每客户端平均放行速率,必须 > 0。
    /// - `burst`:突发容量(令牌桶上限),必须 >= 1。
    pub fn new(requests_per_second: f64, burst: f64) -> Self {
        Self {
            rate_per_sec: requests_per_second,
            burst,
            state: Mutex::new(RateLimitState {
                buckets: HashMap::new(),
                last_sweep: Instant::now(),
            }),
            max_keys: 100_000,
            sweep_interval: Duration::from_secs(60),
        }
    }

    /// 为某客户端尝试消费一个令牌。
    ///
    /// # 参数
    ///
    /// - `client`:已解析的真实客户端 IP。
    /// - `now`:当前单调时刻(由调用方传入便于确定性验证)。
    ///
    /// # 返回
    ///
    /// 放行返回 `Ok(())`;拒绝返回 `Err(retry_after_secs)`,即补足一个令牌所需的建议重试秒数(>=1)。
    fn check(&self, client: IpAddr, now: Instant) -> Result<(), u64> {
        // 锁中毒(此前某处 panic)不应让限流整体卡死:取回内部值继续,数值语义无损。
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // 到点清扫:移除已回满(空闲)的桶。只读投影判定,不改留存桶的令牌数。
        if now.saturating_duration_since(state.last_sweep) >= self.sweep_interval {
            let (rate, burst) = (self.rate_per_sec, self.burst);
            state.buckets.retain(|_, bucket| {
                let refilled = (bucket.tokens
                    + now
                        .saturating_duration_since(bucket.last_refill)
                        .as_secs_f64()
                        * rate)
                    .min(burst);
                // 未回满 = 仍在消耗中,保留;已回满 = 空闲客户端,清掉。
                refilled < burst
            });
            state.last_sweep = now;
        }
        // 容量护栏:新客户端且已达硬上限 → fail-open(见类型文档)。
        if state.buckets.len() >= self.max_keys && !state.buckets.contains_key(&client) {
            return Ok(());
        }
        let (rate, burst) = (self.rate_per_sec, self.burst);
        let bucket = state.buckets.entry(client).or_insert(TokenBucket {
            tokens: burst,
            last_refill: now,
        });
        let elapsed = now
            .saturating_duration_since(bucket.last_refill)
            .as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            // 补足一个令牌还需的时长,向上取整到秒且至少 1(HTTP Retry-After 以秒计)。
            let secs = ((1.0 - bucket.tokens) / rate).ceil() as u64;
            Err(secs.max(1))
        }
    }
}

/// 单实例每客户端限流中间件:按 `ClientIp` 令牌桶,桶空即 429 + `Retry-After`。
///
/// 装在 `resolve_client_ip` 之内(依赖其写入的 `ClientIp` 扩展)、全局并发 load shed 之外(单一来源被
/// 挡下前不占全局并发额),且在 request-id/安全头之内——被拒的 429 仍带 request-id 与安全响应头。CORS
/// 预检在更外层短路,不受本层限流。无 `ClientIp`(理论上不会,始终在 resolve 之内)时保守放行,不误杀。
///
/// # 参数
///
/// - `limit`:启动期冻结的每客户端令牌桶限流器。
/// - `request`:入站请求;从其扩展读取已解析客户端 IP。
/// - `next`:下游放行句柄。
pub async fn rate_limit(
    State(limit): State<Arc<RateLimit>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(client) = request.extensions().get::<ClientIp>().map(ClientIp::ip) else {
        return next.run(request).await;
    };
    match limit.check(client, Instant::now()) {
        Ok(()) => next.run(request).await,
        Err(retry_after_secs) => (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, retry_after_secs.to_string())],
            "rate limit exceeded",
        )
            .into_response(),
    }
}

/// API 场景安全响应头模板:对 JSON API 合理的一组默认头。
///
/// 用 `or_insert` 语义——**不覆盖** handler 已显式设置的同名头,故它是"默认"而非"强制盲塞"(
/// 明确不能给所有响应塞同一组)。UI/浏览器场景(CSP、HSTS 等)应另配模板,不复用本组。
pub async fn api_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    // MIME 嗅探防护:响应必须按声明的 Content-Type 处理。
    headers
        .entry(HeaderName::from_static("x-content-type-options"))
        .or_insert(HeaderValue::from_static("nosniff"));
    // 禁止被 iframe 嵌入(点击劫持防护);API 响应无嵌入需求。
    headers
        .entry(HeaderName::from_static("x-frame-options"))
        .or_insert(HeaderValue::from_static("DENY"));
    // 不外泄来源(API 不需要 referrer)。
    headers
        .entry(HeaderName::from_static("referrer-policy"))
        .or_insert(HeaderValue::from_static("no-referrer"));
    response
}

/// 总 deadline 中间件:建立预算写入扩展,并对整个请求处理施加绝对超时;超时返回固定 504。
pub async fn enforce_request_deadline(
    State(total): State<std::time::Duration>,
    mut request: Request,
    next: Next,
) -> Response {
    let budget = RequestBudget::from_now(total);
    request.extensions_mut().insert(budget.clone());
    let result = tokio::time::timeout_at(budget.deadline(), next.run(request)).await;
    budget.cancel();
    match result {
        Ok(response) => response,
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            [(header::RETRY_AFTER, "1")],
            "request deadline exceeded",
        )
            .into_response(),
    }
}

/// panic 边界响应:handler panic 时返回**固定 500**,绝不暴露 payload/stack;
/// 同时写一条**脱敏**诊断(只记发生 panic 与 payload 是否字符串,不记内容——payload 可能含业务输入)。
///
/// 作为 `tower_http::catch_panic::CatchPanicLayer::custom` 的处理器使用。
pub fn panic_response(payload: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let is_string =
        payload.downcast_ref::<&str>().is_some() || payload.downcast_ref::<String>().is_some();
    tracing::error!(
        payload_is_string = is_string,
        "web handler panicked; returning fixed 500 without exposing payload or stack"
    );
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(
            header::CONTENT_TYPE,
            "application/problem+json; charset=utf-8",
        )],
        // 固定 problem+json:不含 payload/stack/内部细节。
        r#"{"type":"about:blank","title":"Internal Server Error","status":500,"code":"internal"}"#,
    )
        .into_response()
}

/// CORS 策略:**默认关闭**;**禁止 `*` + credentials**;预检在 auth 之外直接答复。
///
/// 精确 origin 白名单(或 `*` 表示任意,但不能与凭据同用)。跨域是浏览器安全边界,配置必须显式,
/// 不做隐式放行。
#[derive(Debug, Clone)]
pub struct CorsPolicy {
    allowed_origins: Vec<String>,
    allow_credentials: bool,
    allowed_methods: String,
    allowed_headers: String,
    max_age_secs: u64,
    allowed_method_set: std::collections::HashSet<axum::http::Method>,
    allowed_header_set: std::collections::HashSet<HeaderName>,
}

impl CorsPolicy {
    /// 构造并校验策略;`*` origin 与 credentials 同用即拒绝,启用但无 origin 也拒绝。
    ///
    /// # 错误
    ///
    /// 违反 `*`+credentials 或空 origin 约束时返回稳定原因(不含请求数据)。
    pub fn new(
        allowed_origins: Vec<String>,
        allow_credentials: bool,
        allowed_methods: impl Into<String>,
        allowed_headers: impl Into<String>,
        max_age_secs: u64,
    ) -> Result<Self, &'static str> {
        if allowed_origins.is_empty()
            || allowed_origins.iter().any(|origin| {
                origin.trim() != origin
                    || origin.is_empty()
                    || HeaderValue::from_str(origin).is_err()
            })
        {
            return Err("CORS: allowed_origins must not be empty when enabled");
        }
        let has_wildcard = allowed_origins.iter().any(|origin| origin == "*");
        if has_wildcard && allow_credentials {
            return Err("CORS: wildcard origin `*` must not be combined with credentials");
        }
        let allowed_methods = allowed_methods.into();
        let allowed_headers = allowed_headers.into();
        HeaderValue::from_str(&allowed_methods)
            .map_err(|_| "CORS: allowed_methods is not a valid response header value")?;
        HeaderValue::from_str(&allowed_headers)
            .map_err(|_| "CORS: allowed_headers is not a valid response header value")?;
        let method_parts = allowed_methods
            .split(',')
            .map(str::trim)
            .collect::<Vec<_>>();
        let allowed_method_set: std::collections::HashSet<axum::http::Method> = method_parts
            .iter()
            .copied()
            .map(|method| {
                axum::http::Method::from_bytes(method.as_bytes())
                    .map_err(|_| "CORS: allowed_methods contains an invalid method")
            })
            .collect::<Result<_, _>>()?;
        let header_parts = allowed_headers
            .split(',')
            .map(str::trim)
            .collect::<Vec<_>>();
        let allowed_header_set: std::collections::HashSet<HeaderName> = header_parts
            .iter()
            .copied()
            .map(|name| {
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| "CORS: allowed_headers contains an invalid header name")
            })
            .collect::<Result<_, _>>()?;
        if allowed_method_set.len() != method_parts.len()
            || allowed_header_set.len() != header_parts.len()
        {
            return Err("CORS: allowed methods/headers must not contain duplicates");
        }
        Ok(Self {
            allowed_origins,
            allow_credentials,
            allowed_methods,
            allowed_headers,
            max_age_secs,
            allowed_method_set,
            allowed_header_set,
        })
    }

    /// 给定 origin 是否被允许(精确匹配或 `*`)。
    fn origin_allowed(&self, origin: &str) -> bool {
        self.allowed_origins
            .iter()
            .any(|allowed| allowed == "*" || allowed == origin)
    }

    /// 在响应上写入 CORS 头(仅当 origin 允许)。
    fn apply_cors_headers(&self, response: &mut Response, origin: &str) {
        let headers = response.headers_mut();
        if let Ok(value) = HeaderValue::from_str(origin) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        }
        if self.allow_credentials {
            headers.insert(
                header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
    }

    /// 严格解析单值预检 method/header，并确认都落在冻结白名单内。
    fn preflight_allowed(&self, request: &Request) -> bool {
        let mut methods = request
            .headers()
            .get_all(header::ACCESS_CONTROL_REQUEST_METHOD)
            .iter();
        let method_allowed = match (methods.next(), methods.next()) {
            (Some(value), None) => axum::http::Method::from_bytes(value.as_bytes())
                .ok()
                .is_some_and(|method| self.allowed_method_set.contains(&method)),
            _ => false,
        };
        let mut requested_headers = request
            .headers()
            .get_all(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .iter();
        let headers_allowed = match (requested_headers.next(), requested_headers.next()) {
            (None, None) => true,
            (Some(value), None) => value.to_str().ok().is_some_and(|requested| {
                requested.split(',').map(str::trim).all(|name| {
                    HeaderName::from_bytes(name.as_bytes())
                        .ok()
                        .is_some_and(|name| self.allowed_header_set.contains(&name))
                })
            }),
            _ => false,
        };
        method_allowed && headers_allowed
    }
}

/// 附加缓存必须区分的 CORS 请求头，防止共享缓存跨 origin 或预检条件复用响应。
fn append_cors_vary(response: &mut Response, preflight: bool) {
    let headers = response.headers_mut();
    headers.append(header::VARY, HeaderValue::from_static("Origin"));
    if preflight {
        headers.append(
            header::VARY,
            HeaderValue::from_static("Access-Control-Request-Method"),
        );
        headers.append(
            header::VARY,
            HeaderValue::from_static("Access-Control-Request-Headers"),
        );
    }
}

/// CORS 中间件:预检(OPTIONS + `Access-Control-Request-Method`)在 auth 之外直接 204 答复;
/// 实际请求在放行后按 origin 白名单加 `Access-Control-Allow-Origin`。
pub async fn cors(State(policy): State<Arc<CorsPolicy>>, request: Request, next: Next) -> Response {
    let origin_present = request.headers().contains_key(header::ORIGIN);
    let mut origin_values = request.headers().get_all(header::ORIGIN).iter();
    let origin = match (origin_values.next(), origin_values.next()) {
        (Some(value), None) => value.to_str().ok().map(str::to_owned),
        _ => None,
    };
    let request_method_allowed = policy.allowed_method_set.contains(request.method());
    let is_preflight = request.method() == axum::http::Method::OPTIONS
        && request
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);

    if is_preflight {
        // 预检不进业务(auth 之外):直接 204 + 允许头。
        let mut response = Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        append_cors_vary(&mut response, true);
        if let Some(origin) = origin.as_deref() {
            if policy.origin_allowed(origin) && policy.preflight_allowed(&request) {
                policy.apply_cors_headers(&mut response, origin);
                let headers = response.headers_mut();
                if let Ok(value) = HeaderValue::from_str(&policy.allowed_methods) {
                    headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, value);
                }
                if let Ok(value) = HeaderValue::from_str(&policy.allowed_headers) {
                    headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, value);
                }
                if let Ok(value) = HeaderValue::from_str(&policy.max_age_secs.to_string()) {
                    headers.insert(header::ACCESS_CONTROL_MAX_AGE, value);
                }
            }
        }
        return response;
    }

    let mut response = next.run(request).await;
    if origin_present {
        append_cors_vary(&mut response, false);
    }
    if let Some(origin) = origin.as_deref() {
        if request_method_allowed && policy.origin_allowed(origin) {
            policy.apply_cors_headers(&mut response, origin);
        }
    }
    response
}

// ── 可信代理与真实客户端 IP──

/// 已解析的真实客户端 IP。业务/限流从请求扩展 `req.extensions().get::<ClientIp>()` 读取。
///
/// 语义:仅当**直连对端**在可信代理列表内时才采信 `X-Forwarded-For`(从右往左跳过可信代理,取首个
/// 不可信项为真实客户端);对端不可信时忽略 XFF(防伪造),客户端即对端地址。列表为空(默认)时
/// 永不采信 XFF——任何暴露在不可信网络的部署据此安全默认。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

impl ClientIp {
    /// 返回解析出的客户端 IP。
    ///
    /// # 参数
    ///
    /// 本方法无参数。
    pub fn ip(&self) -> IpAddr {
        self.0
    }
}

impl std::fmt::Display for ClientIp {
    /// 以标准 IP 文本写出,不含端口。
    ///
    /// # 参数
    ///
    /// - `formatter`:目标格式化缓冲。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// 把配置里的可信代理项(精确 IP 或 CIDR)解析成网段列表;任一非法即报错(启动前拒)。
///
/// # 参数
///
/// - `entries`:配置 `server.trusted_proxies` 的原始字符串列表。
///
/// # 错误
///
/// 某项既非合法 IP 也非合法 CIDR 时返回稳定摘要(含该项文本,不含其它配置)。
pub fn parse_trusted_proxies(entries: &[String]) -> Result<Vec<ipnet::IpNet>, String> {
    let mut nets = Vec::with_capacity(entries.len());
    for entry in entries {
        let text = entry.trim();
        if text.is_empty() {
            continue;
        }
        let net = text
            .parse::<ipnet::IpNet>()
            .or_else(|_| text.parse::<IpAddr>().map(ipnet::IpNet::from))
            .map_err(|_| {
                format!("invalid trusted proxy entry `{text}` (expected an IP address or CIDR)")
            })?;
        nets.push(net);
    }
    Ok(nets)
}

/// 判断某地址是否落在任一可信代理网段内。
///
/// # 参数
///
/// - `trusted`:已解析的可信代理网段列表。
/// - `address`:待判定地址(直连对端或某个 XFF 项)。
fn is_trusted(trusted: &[ipnet::IpNet], address: IpAddr) -> bool {
    trusted.iter().any(|net| net.contains(&address))
}

/// 从 `X-Forwarded-For` 值里按「从右往左跳过可信代理、取首个不可信项」解析真实客户端 IP。
///
/// # 参数
///
/// - `header_value`:原始 XFF 头文本(逗号分隔;非法项忽略)。
/// - `trusted`:可信代理网段列表。
///
/// # 返回
///
/// 首个不可信项;全部可信时取最左(最接近原始客户端的已知项);无可解析项时 `None`。
fn client_from_forwarded(header_value: &str, trusted: &[ipnet::IpNet]) -> Option<IpAddr> {
    if header_value.len() > 2048 {
        return None;
    }
    let parts = header_value.split(',').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 32 {
        return None;
    }
    let hops: Vec<IpAddr> = parts
        .into_iter()
        .map(|part| part.trim().parse::<IpAddr>())
        .collect::<Result<_, _>>()
        .ok()?;
    hops.iter()
        .rev()
        .find(|address| !is_trusted(trusted, **address))
        .copied()
        .or_else(|| hops.first().copied())
}

/// 解析真实客户端 IP 中间件:对端可信才采信 XFF,否则客户端即对端;结果写入 `ClientIp` 扩展。
///
/// 始终启用(与 request-id 同):可信列表为空时退化为「客户端 = 直连对端、永不信 XFF」的安全默认。
/// 依赖监听器以 `ConnectInfo<SocketAddr>` 提供对端地址;缺失(理论不应发生)时按 unspecified 记录,
/// 绝不 panic。
///
/// # 参数
///
/// - `trusted`:启动期解析并冻结的可信代理网段列表。
/// - `request`:入站请求;解析出的客户端 IP 写入其扩展。
/// - `next`:下游放行句柄。
pub async fn resolve_client_ip(
    State(trusted): State<Arc<Vec<ipnet::IpNet>>>,
    mut request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip());
    let client = match peer {
        // 对端可信:采信 XFF(从右跳过可信代理);XFF 缺失/全不可解析时退回对端。
        Some(peer_ip) if is_trusted(&trusted, peer_ip) => {
            let mut forwarded = request.headers().get_all("x-forwarded-for").iter();
            match (forwarded.next(), forwarded.next()) {
                (Some(value), None) => value
                    .to_str()
                    .ok()
                    .and_then(|value| client_from_forwarded(value, &trusted))
                    .unwrap_or(peer_ip),
                _ => peer_ip,
            }
        }
        // 对端不可信:忽略 XFF(防伪造),客户端即直连对端。
        Some(peer_ip) => peer_ip,
        // 无 ConnectInfo(监听器未带 connect-info;本运行时总是带):按 unspecified 记录,不 panic。
        None => IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
    };
    request.extensions_mut().insert(ClientIp(client));
    next.run(request).await
}

/// 判断响应 `Content-Type` 是否属于可安全压缩的文本类白名单。
///
/// 只压缩已知文本/结构化文本类型;二进制、图片(SVG 除外)、以及所有非白名单类型
/// (含 modern-v2 密文的自定义媒体类型)一律不压。base 只取分号前的主类型再小写比较,
/// 因此 `application/json; charset=utf-8` 也能命中。
///
/// # 参数
///
/// - `content_type`:响应的 `Content-Type` 头原始值(可带参数)。
pub fn is_compressible_content_type(content_type: &str) -> bool {
    let base = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    base.starts_with("text/")
        || matches!(
            base.as_str(),
            "application/json"
                | "application/xml"
                | "application/javascript"
                | "application/x-ndjson"
                | "application/rss+xml"
                | "application/atom+xml"
                | "image/svg+xml"
        )
}

/// 压缩谓词:与 `SizeAbove` 组合后决定某响应是否可被 gzip。
///
/// 作为 tower-http `Predicate` 使用(自由 fn 满足
/// `Fn(StatusCode, Version, &HeaderMap, &Extensions) -> bool + Clone`)。三重拦截:
/// 1. 命中 [`naweb::UncompressibleResponse`] 扩展标记的响应绝不压缩——这是密文(尤其 legacy-v1
///    密文 content-type 就是 `application/json`,凭类型无法与明文区分)规避 CRIME/BREACH 的唯一手段;
/// 2. 已带 `Content-Encoding` 的响应不重复编码;
/// 3. 其余仅当 `Content-Type` 命中 [`is_compressible_content_type`] 白名单时才压。
///
/// # 参数
///
/// - `_status` / `_version`:tower-http 谓词签名要求,本策略不据其决策。
/// - `headers`:响应头,用于读取 `Content-Encoding` 与 `Content-Type`。
/// - `extensions`:响应扩展,用于探测密文不可压缩标记。
pub fn should_compress_response(
    _status: axum::http::StatusCode,
    _version: axum::http::Version,
    headers: &axum::http::HeaderMap,
    extensions: &axum::http::Extensions,
) -> bool {
    if extensions.get::<naweb::UncompressibleResponse>().is_some() {
        return false;
    }
    if headers.contains_key(header::CONTENT_ENCODING) {
        return false;
    }
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(is_compressible_content_type)
        .unwrap_or(false)
}
