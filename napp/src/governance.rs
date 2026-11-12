//! Web 请求治理：关联 ID、实例限流、处理预算、CORS、代理地址与响应压缩策略。
//!
//! 关联 ID 经字符校验后供日志与追踪使用，不用于指标标签或身份授权。
//! 中间件的执行顺序由 Web 组件统一装配，单个中间件的准入策略不能代替业务权限校验。

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
/// 字段 0 共享经过字符与长度校验的关联文本，不保存原始请求头。
#[derive(Clone, Debug)]
pub struct RequestId(Arc<str>);

impl RequestId {
    /// 业务作用：借用本次请求的关联 ID，供日志及下游调用关联使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与当前句柄共存的文本引用；该 ID 不代表调用者身份或授权凭据。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RequestId {
    /// 业务作用：将已校验请求 ID 原样写入日志字段或响应头。
    ///
    /// 参数说明：
    /// - `formatter`：接收关联 ID 文本的格式化目标，不执行额外转义或脱敏。
    ///
    /// 返回：目标接收完整文本时成功；写入失败原样返回格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// 请求关联 ID 的请求/响应头名。
pub(crate) const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// 入站 request id 长度上限;超限或非法字符即视为不可信,改为生成。
const MAX_REQUEST_ID_LEN: usize = 128;

/// 业务作用：判定入站关联 ID 能否安全沿用，限制长度与字符集以避免响应头注入。
///
/// 参数说明：
/// - `value`：已读取为文本的候选 ID，不裁剪空白。
///
/// 返回：仅非空、至多 128 字节且只含 ASCII 字母、数字、`-`、`_`、`.` 时为 true。
/// 此检查不验证身份、唯一性或随机性，也不能限制不同 ID 的数量。
pub fn is_trusted_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REQUEST_ID_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 业务作用：通过追踪组件的随机源生成非零 128-bit 关联 ID，降低跨实例请求的标识碰撞概率。
///
/// 参数说明：无。
///
/// 返回：新根追踪上下文的十六进制 trace ID；不登记全局唯一性，也不将该上下文自动安装到请求。
fn generate_request_id() -> String {
    natelemetry::TraceContext::new_root(true).trace_id_hex()
}

/// 业务作用：解析或生成本次请求的关联 ID；纯逻辑保证入口与下游使用同一信任边界。
///
/// 参数说明：
/// - `inbound`：可选的单个入站 ID 文本；重复头的拒绝由调用方先行处理。
///
/// 返回：满足字符与长度要求时复制原值，否则生成新 ID；不拒绝请求，也不自动写入请求或响应。
pub fn resolve_request_id(inbound: Option<&str>) -> String {
    match inbound {
        Some(value) if is_trusted_request_id(value) => value.to_owned(),
        _ => generate_request_id(),
    }
}

/// 业务作用：request ID 中间件:校验/生成 → 写入请求扩展 → 响应头回传。
///
/// 参数说明：
/// - `request`：读取 x-request-id 的入站请求；重复、非文本或不合规的值均替换为新 ID。
/// - `next`：读取同一 RequestId 扩展的后续处理链。
///
/// 返回：下游响应，并覆盖其 x-request-id 头为本次选定值；入站原始头保持原样，业务应读取扩展中的已校验值。
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
    /// 响应对象产生前的处理额度；许可不随流式响应体继续持有。
    semaphore: Arc<Semaphore>,
    /// 立即拒绝时返回给调用方的建议等待秒数，不在服务端安排重试。
    retry_after_secs: u64,
}

impl ConcurrencyLimit {
    /// 业务作用：创建在途请求准入器；非 fallible 入口把零值和超大值收敛到 Tokio 可表达范围。
    ///
    /// 参数说明：
    ///
    /// - `max_inflight`：期望的并发处理数，构造时钳制到 1 至 Semaphore::MAX_PERMITS。
    /// - `retry_after_secs`:过载响应回传的建议重试秒数。
    ///
    /// 返回：持有独立信号量的实例级准入器；不创建等待队列，也不约束响应体传输期间的并发数。
    pub(crate) fn new(max_inflight: usize, retry_after_secs: u64) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(
                max_inflight.clamp(1, Semaphore::MAX_PERMITS),
            )),
            retry_after_secs,
        }
    }
}

/// 业务作用：在实例并发额度内执行处理链，容量不足立即返回 503 与 Retry-After，避免额外排队。
///
/// 参数说明：
/// - `limit`：本实例并发额度及过载重试提示。
/// - `request`：需要占据在途额度的入站请求。
/// - `next`：取得许可后调用的下游处理链。
///
/// 返回：允许时返回下游响应并释放许可；许可只覆盖响应对象产生前的处理，取消或展开异常也会释放。
/// 流式响应体后续传输不持有该许可，不能据此限制存活流的总数。
pub async fn load_shed(
    State(limit): State<Arc<ConcurrencyLimit>>,
    request: Request,
    next: Next,
) -> Response {
    match Arc::clone(&limit.semaphore).try_acquire_owned() {
        Ok(permit) => {
            let response = next.run(request).await;
            // 响应已产生即释放处理额度；响应体传输的生命周期由预算层负责。
            drop(permit);
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
    /// 业务作用：创建每客户端限流器。
    ///
    /// 参数说明：
    ///
    /// - `requests_per_second`：每客户端每秒补充的令牌数，调用方必须保证有限且大于 0。
    /// - `burst`：每客户端桶容量，调用方必须保证有限且至少为 1。
    ///
    /// 返回：空桶表的实例级限流器，最多记录 100000 个 IP、每隔至少 60 秒在请求路径清理回满桶。
    /// 本入口不验证数值；表满时新 IP 放行，不能代替必须严格执行的业务配额。
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

    /// 业务作用：为某客户端尝试消费一个令牌。
    ///
    /// 参数说明：
    ///
    /// - `client`:已解析的真实客户端 IP。
    /// - `now`：本次计费的单调时刻，应按请求先后非递减；早于记录的时刻本次按零补充处理。
    ///
    /// 返回：扣除一个令牌后成功，令牌不足返回至少 1 秒的建议等待；表满的新客户端直接成功且不扣令牌。
    /// 到达清扫间隔会先删除已回满的桶，所有桶状态在同一短临界区更新，不等待令牌。
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

/// 业务作用：单实例每客户端限流中间件:按 `ClientIp` 令牌桶,桶空即 429 + `Retry-After`。
///
/// 装在 `resolve_client_ip` 之内(依赖其写入的 `ClientIp` 扩展)、全局并发 load shed 之外(单一来源被
/// 挡下前不占全局并发额),且在 request-id/安全头之内——被拒的 429 仍带 request-id 与安全响应头。CORS
/// 预检在更外层短路,不受本层限流。无 `ClientIp`(理论上不会,始终在 resolve 之内)时保守放行,不误杀。
///
/// 参数说明：
///
/// - `limit`:启动期冻结的每客户端令牌桶限流器。
/// - `request`:入站请求;从其扩展读取已解析客户端 IP。
/// - `next`:下游放行句柄。
///
/// 返回：有令牌时执行下游，令牌不足返回 429 与 Retry-After；缺少 ClientIp 或新 IP 遇桶表容量上限时放行。
/// 令牌按请求进入时扣减，不因下游失败退还；本层不提供跨实例配额。
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

/// 业务作用：API 场景安全响应头模板:对 JSON API 合理的一组默认头。
///
/// 用 `or_insert` 语义——**不覆盖** handler 已显式设置的同名头,故它是"默认"而非"强制盲塞"(
/// 明确不能给所有响应塞同一组)。UI/浏览器场景(CSP、HSTS 等)应另配模板,不复用本组。
///
/// 参数说明：
/// - `request`：原样交给处理链的入站请求。
/// - `next`：产生待补充安全响应头的处理链。
///
/// 返回：保留下游状态和响应体，仅补上缺失的 nosniff、DENY、no-referrer 头；不检查已存在值是否足够严格。
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

/// 业务作用：让 handler 和响应体共享绝对预算，请求退出时取消相关下游等待。
///
/// 参数说明：
/// - `request`：`request` 为入站请求。
/// - `next`：`next` 为后续处理链。
/// - `total`：处理请求与响应体共用的绝对预算总时长。
///
/// 返回：响应头产生前预算终止返回 504；响应头发出后到期则终止响应体，保留已发送状态码。
pub async fn enforce_request_deadline(
    State(total): State<std::time::Duration>,
    mut request: Request,
    next: Next,
) -> Response {
    let budget = RequestBudget::from_now(total);
    let guard = budget.cancel_on_drop();
    request.extensions_mut().insert(budget.clone());
    let result = budget.run(next.run(request)).await;
    match result {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            // 响应头不是请求完成点；取消责任随 body 移交，覆盖流式消费、断开和未消费即丢弃。
            Response::from_parts(
                parts,
                axum::body::Body::new(BudgetBody::new(body, budget, guard)),
            )
        }
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            [(header::RETRY_AFTER, "1")],
            "request deadline exceeded",
        )
            .into_response(),
    }
}

/// 保留数据帧和 trailers 的预算响应体，不为每个响应另建后台任务。
struct BudgetBody {
    /// 原响应的数据帧与 trailers 来源，按需轮询，不预先收集正文。
    inner: axum::body::Body,
    /// 沿用入站处理的绝对截止点，产生响应后不重新分配预算。
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    /// 监听同一预算的主动取消通知，优先于读取下一帧裁决。
    cancelled: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    /// 结束、错误或丢弃响应体时通知相关下游取消的唯一守卫。
    owner: Option<nabudget::BudgetGuard>,
    /// 终止后不再读取源响应体，并保证预算错误最多报告一次。
    finished: bool,
}

impl BudgetBody {
    /// 业务作用：接管响应体及其预算取消责任。
    ///
    /// 参数说明：
    /// - `inner`：`inner` 为原响应体。
    /// - `budget`：`budget` 为入站预算。
    /// - `owner`：`owner` 为请求生命周期守卫。
    ///
    /// 返回：在同一截止点终止传输、结束时通知下游的响应体。
    fn new(inner: axum::body::Body, budget: RequestBudget, owner: nabudget::BudgetGuard) -> Self {
        let deadline = Box::pin(tokio::time::sleep_until(budget.deadline()));
        Self {
            inner,
            deadline,
            cancelled: Box::pin(async move { budget.cancelled().await }),
            owner: Some(owner),
            finished: false,
        }
    }
}

impl http_body::Body for BudgetBody {
    type Data = axum::body::Bytes;
    type Error = axum::BoxError;

    /// 业务作用：在预算有效时转发响应帧，取消或到期后停止流式发送。
    ///
    /// 参数说明：
    /// - `cx`：`cx` 为传输层轮询上下文。
    ///
    /// 返回：原数据或 trailers；预算终止时返回一次固定错误，随后结束。
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::future::Future;
        use std::task::Poll;
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        // 在读取下一帧前优先裁决取消，已经取消的请求不会继续消耗上游流。
        let stopped = if this.cancelled.as_mut().poll(cx).is_ready() {
            Some(nabudget::BudgetError::Cancelled)
        } else if this.deadline.as_mut().poll(cx).is_ready() {
            Some(nabudget::BudgetError::DeadlineExceeded)
        } else {
            None
        };
        if let Some(error) = stopped {
            this.finished = true;
            this.owner.take();
            return Poll::Ready(Some(Err(Box::new(error))));
        }
        match std::pin::Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(value) => {
                this.finished = true;
                this.owner.take();
                Poll::Ready(value.map(|result| result.map_err(Into::into)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// 业务作用：向传输层报告已完成的预算响应体。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅在本 owner 已结束时报告结束，确保空 body 也经过取消责任释放。
    fn is_end_stream(&self) -> bool {
        self.finished
    }

    /// 业务作用：保留响应体长度提示，避免改变已知长度响应的传输形式。
    ///
    /// 参数说明：无。
    ///
    /// 返回：原响应体的大小提示。
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// 业务作用：panic 边界响应:handler panic 时返回**固定 500**,绝不暴露 payload/stack;
/// 同时写一条**脱敏**诊断(只记发生 panic 与 payload 是否字符串,不记内容——payload 可能含业务输入)。
///
/// 作为 `tower_http::catch_panic::CatchPanicLayer::custom` 的处理器使用。
///
/// 参数说明：
/// - `payload`：上层已捕获的展开异常载荷，本函数仅判断其是否为字符串，不读取内容。
///
/// 返回：固定的 500 problem+json 响应，并记录载荷类型提示；本函数不建立捕获边界，也不能处理进程终止。
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

/// CORS 策略：Web 配置默认不启用；禁止通配来源与凭据并用；预检在认证之外答复。
///
/// 来源白名单控制浏览器是否可以读取跨域响应，不替代服务端业务鉴权，也不阻止实际请求执行。
#[derive(Debug, Clone)]
pub struct CorsPolicy {
    /// 原样匹配的来源白名单，通配值只允许用于不携带凭据的策略。
    allowed_origins: Vec<String>,
    /// 是否在获准来源响应中声明允许浏览器凭据。
    allow_credentials: bool,
    /// 已验证可写入响应的允许方法列表，保留配置的文本形式。
    allowed_methods: String,
    /// 已验证可写入响应的允许请求头列表。
    allowed_headers: String,
    /// 获准预检可缓存的秒数。
    max_age_secs: u64,
    /// 逐请求精确匹配的方法集合，避免请求路径重复解析策略。
    allowed_method_set: std::collections::HashSet<axum::http::Method>,
    /// 按 HTTP 头名语义匹配的预检请求头集合。
    allowed_header_set: std::collections::HashSet<HeaderName>,
}

impl CorsPolicy {
    /// 业务作用：在接收请求前冻结 CORS 来源、方法与头名集合，拒绝不能安全写入响应的策略。
    ///
    /// 参数说明：
    /// - `allowed_origins`：精确匹配的非空来源列表或通配值 `*`；不规范化 URL，也不校验来源的 URL 结构。
    /// - `allow_credentials`：是否允许浏览器携带凭据，启用时不能配置通配来源。
    /// - `allowed_methods`：逗号分隔的方法文本，每项必须可解析且不能重复，方法匹配区分大小写。
    /// - `allowed_headers`：逗号分隔的请求头名文本，每项必须可解析且按头名语义去重。
    /// - `max_age_secs`：预检响应允许缓存的秒数，零值保留，不在此限制上限。
    ///
    /// 返回：全部约束满足时返回冻结策略；空来源、来源空白或非法头值、通配凭据组合、非法或重复方法/头名均拒绝。
    /// 错误仅含固定原因；成功不表示已挂载中间件，也不提供业务权限校验。
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
        // 携带凭据的响应必须限定来源，不能用任意来源策略公开凭据关联的数据。
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

    /// 业务作用：给定 origin 是否被允许(精确匹配或 `*`)。
    ///
    /// 参数说明：
    /// - `origin`：请求头中的原始来源文本，不裁剪或规范化。
    ///
    /// 返回：白名单含通配值或完全相同文本时为 true；只裁决来源，不裁决方法、请求头或身份。
    fn origin_allowed(&self, origin: &str) -> bool {
        self.allowed_origins
            .iter()
            .any(|allowed| allowed == "*" || allowed == origin)
    }

    /// 业务作用：为调用方已批准的来源写入 CORS 响应头，统一回显实际来源的行为。
    ///
    /// 参数说明：
    /// - `response`：待覆盖允许来源头、按需增加允许凭据头的响应。
    /// - `origin`：已经过来源门禁的请求头值，通配策略也回显此值。
    ///
    /// 返回：可编码时写入来源头，允许凭据时写入 true；本函数不复核来源、不添加 Vary，也不删除已有的凭据头。
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

    /// 业务作用：严格解析单值预检 method/header，并确认都落在冻结白名单内。
    ///
    /// 参数说明：
    /// - `request`：待检查的预检请求；必须有且仅有一个方法头，请求头列表可以缺省但不能重复出现。
    ///
    /// 返回：方法可解析并在白名单中，且请求的每个头名均获准时为 true；非法、空项或重复头字段为 false。
    /// 同一逗号列表内重复的头名仍按逐项匹配，不在此判重；来源及 OPTIONS 判断由外层完成。
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

/// 业务作用：附加缓存必须区分的 CORS 请求头，防止共享缓存跨 origin 或预检条件复用响应。
///
/// 参数说明：
/// - `response`：保留已有 Vary 值并追加本策略缓存维度的响应。
/// - `preflight`：为 true 时同时区分预检方法与请求头列表，否则只区分 Origin。
///
/// 返回：原地追加固定 Vary 字段，不合并或去重已有值，也不改变允许来源的决策。
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

/// 业务作用：按冻结 CORS 策略处理浏览器预检与跨域响应头，预检在认证之前短路。
///
/// 参数说明：
/// - `policy`：启动时冻结的来源、方法、请求头与凭据政策。
/// - `request`：携带来源及可选预检信息的请求。
/// - `next`：所有非预检请求都会进入的处理链，其业务权限必须由独立认证授权层裁决。
///
/// 返回：预检一律返回 204，只有来源、方法和请求头均获准才附加允许头；不合规预检由浏览器拒绝。
/// 实际请求返回下游响应，仅为获准来源与方法附加允许头，不拒绝业务执行，也不清除下游自行设置的 CORS 头。
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
        // 预检不进入业务链；仅对完整获准的请求附加允许头，其余 204 由浏览器按缺失许可拒绝。
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
/// 字段 0 保存解析结果；公开构造本身不验证可信代理来源。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

impl ClientIp {
    /// 业务作用：返回解析出的客户端 IP。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前扩展保存的 IP 值；不重新验证代理链，缺失连接信息时可能是未指定地址。
    pub fn ip(&self) -> IpAddr {
        self.0
    }
}

impl std::fmt::Display for ClientIp {
    /// 业务作用：以标准 IP 文本写出,不含端口。
    ///
    /// 参数说明：
    ///
    /// - `formatter`:目标格式化缓冲。
    ///
    /// 返回：目标成功接收 IP 文本时成功，否则传播格式化错误；不输出端口或代理链。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// 业务作用：把配置里的可信代理项(精确 IP 或 CIDR)解析成网段列表;任一非法即报错(启动前拒)。
///
/// 参数说明：
///
/// - `entries`:配置 `server.trusted_proxies` 的原始字符串列表。
///
/// 返回：裁剪两端空白并略过空项后返回网段列表，精确 IP 转为单地址网段，不去重。
/// 任一非空项无法解析即返回含该项文本的错误，不返回部分列表；调用方须在开放监听前处理失败。
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

/// 业务作用：判断某地址是否落在任一可信代理网段内。
///
/// 参数说明：
///
/// - `trusted`:已解析的可信代理网段列表。
/// - `address`:待判定地址(直连对端或某个 XFF 项)。
///
/// 返回：任一网段包含目标时为 true，空列表为 false；本次只检查地址归属，不解析或校验整条代理链。
fn is_trusted(trusted: &[ipnet::IpNet], address: IpAddr) -> bool {
    trusted.iter().any(|net| net.contains(&address))
}

/// 业务作用：从 `X-Forwarded-For` 值里按「从右往左跳过可信代理、取首个不可信项」解析真实客户端 IP。
///
/// 参数说明：
///
/// - `header_value`：单个 XFF 头的逗号分隔地址链；每项裁剪空白，任一项非法则整条链拒绝。
/// - `trusted`:可信代理网段列表。
///
/// 返回：从右侧数首个不可信 IP，全部可信时返回最左项；超过 2048 字节、超过 32 项或任一地址非法时为 None。
/// 调用方必须先确认直连对端可信，并在 None 时退回对端地址，不能使用部分解析结果。
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

/// 业务作用：解析真实客户端 IP 中间件:对端可信才采信 XFF,否则客户端即对端;结果写入 `ClientIp` 扩展。
///
/// 始终启用(与 request-id 同):可信列表为空时退化为「客户端 = 直连对端、永不信 XFF」的安全默认。
/// 依赖监听器以 `ConnectInfo<SocketAddr>` 提供对端地址;缺失(理论不应发生)时按 unspecified 记录,
/// 绝不 panic。
///
/// 参数说明：
///
/// - `trusted`:启动期解析并冻结的可信代理网段列表。
/// - `request`:入站请求;解析出的客户端 IP 写入其扩展。
/// - `next`:下游放行句柄。
///
/// 返回：覆盖 ClientIp 扩展后执行并返回下游响应；只采信可信对端的单个有效 XFF，重复、超限或非法链均退回对端。
/// 缺少 ConnectInfo 时使用 0.0.0.0，不根据未经验证的请求头猜测来源。
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
        // 只有可信对端才能提交代理链；任一非法项或重复头均退回对端，避免使用不完整的信任证据。
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

/// 业务作用：按响应 Content-Type 筛选允许进入压缩判定的文本类媒体类型。
///
/// 只压缩已知文本/结构化文本类型;二进制、图片(SVG 除外)、以及所有非白名单类型
/// (含 modern-v2 密文的自定义媒体类型)一律不压。base 只取分号前的主类型再小写比较,
/// 因此 `application/json; charset=utf-8` 也能命中。
///
/// 参数说明：
///
/// - `content_type`:响应的 `Content-Type` 头原始值(可带参数)。
///
/// 返回：忽略参数、两端空白及大小写后，text/ 前缀或列出的结构化文本类型为 true，其它为 false。
/// 该判定不检查正文、保密要求或头值是否完整合法；密文标记和已有编码由外层谓词另行拦截。
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

/// 业务作用：压缩谓词:与 `SizeAbove` 组合后决定某响应是否可被 gzip。
///
/// 作为 tower-http `Predicate` 使用(自由 fn 满足
/// `Fn(StatusCode, Version, &HeaderMap, &Extensions) -> bool + Clone`)。三重拦截:
/// 1. 命中 [`naweb::UncompressibleResponse`] 扩展标记的响应不压缩；密文媒体类型可能仍为 JSON，
///    不能仅凭类型判断是否允许压缩，业务也应按自身保密要求设置此标记；
/// 2. 已带 `Content-Encoding` 的响应不重复编码;
/// 3. 其余仅当 `Content-Type` 命中 [`is_compressible_content_type`] 白名单时才压。
///
/// 参数说明：
///
/// - `_status`：响应状态码，本策略不根据状态码决定压缩资格。
/// - `_version`：响应 HTTP 协议版本，本策略不根据协议版本决定压缩资格。
/// - `headers`:响应头,用于读取 `Content-Encoding` 与 `Content-Type`。
/// - `extensions`:响应扩展,用于探测密文不可压缩标记。
///
/// 返回：无禁压标记、无 Content-Encoding 且媒体类型在白名单内时为 true；其余均为 false。
/// 此函数不读取正文或评估泄密风险，响应大小与客户端编码协商仍由压缩层处理。
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
