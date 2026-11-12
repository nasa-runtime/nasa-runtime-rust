//! 幂等请求中间件:把 [`naidempotency`] 状态机接到 Web 请求路径。
//!
//! 有 `Idempotency-Key` 头时按身份、路由、客户端键与请求指纹裁决：首次占位后执行，完成后同键同指纹重放；
//! 仍在执行返回 409，指纹不同返回 422。无键则透传；未知业务结果保留占位，不通过取消自动放弃。业务在
//! UserHook 注入一个 [`SharedIdempotencyStore`](内存/DB/Redis 实现同一 trait)。
//!
//! 说明:本层只缓存有限状态白名单、有限 body 和有限 header 白名单。当前层位于 mapping 端点加密层
//! 外侧，无法取得 plaintext 逻辑响应并为每次重放重新加密，因此对声明 crypto 的 route 在执行 handler
//! 前 fail-closed；不能缓存并重放旧 ciphertext。

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use naauthz::Principal;
use naidempotency::{
    ExecutionLease, IdempotencyKey, IdempotencyOutcome, IdempotencyStore, RequestFingerprint,
    StoredHeader, StoredResponse,
};
use rand::RngCore as _;
use sha2::{Digest as _, Sha256};

use crate::problem::ApiProblem;

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const MAX_CLIENT_KEY_BYTES: usize = 190;

/// 参与幂等的请求/响应体上限;超限拒绝。
const MAX_IDEMPOTENT_BODY: usize = 1 << 20;

/// 业务在 UserHook 注入的幂等 store(内存/DB/Redis 实现同一 trait)。
pub type SharedIdempotencyStore = Arc<dyn IdempotencyStore + Send + Sync>;

/// Web 装配时冻结的幂等层状态。
#[derive(Clone)]
pub struct IdempotencyLayerState {
    /// 决定执行权及完成状态的共享后端，其持久性与存活策略由业务所选实现提供。
    store: SharedIdempotencyStore,
    /// 启动时冻结的路由安全合同，用于拒绝无法安全重放的加密入口。
    mapping_runtime: Arc<naweb::MappingRuntime>,
    /// 路由挂载前缀，只用于查找静态安全合同，不作为可信主体身份。
    context_path: Arc<str>,
}

impl IdempotencyLayerState {
    /// 业务作用：绑定 store、已经完成路由审计的 mapping runtime 与 Web context path。
    ///
    /// 参数说明：
    /// - `store`：决定占位、重放及租约完成的共享后端，持久性由所选实现负责。
    /// - `mapping_runtime`：提供路由加密合同的已准备运行时，无法判明合同的请求必须拒绝。
    /// - `context_path`：Web 挂载前缀，用于将匹配路径映射回路由合同；调用方保证与实际挂载一致。
    ///
    /// 返回：只绑定依赖的中间件状态，不访问后端或复验路由；正文上限为本模块固定值，存活策略由后端决定。
    pub fn new(
        store: SharedIdempotencyStore,
        mapping_runtime: Arc<naweb::MappingRuntime>,
        context_path: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            store,
            mapping_runtime,
            context_path: context_path.into(),
        }
    }
}

/// 请求退出前是否取得持久完成确认；未知结果保留占位，不通过取消推断业务未执行。
struct ExecutionLeaseGuard {
    /// 尚未取得本次租约的持久完成确认，析构时报告未知结果而不撤销占位。
    armed: bool,
}

impl ExecutionLeaseGuard {
    /// 业务作用：在业务 handler 开始前建立未知结果的观测责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：尚未取得持久完成确认的守卫，不创建清理任务。
    fn new() -> Self {
        Self {
            armed: true,
        }
    }

    /// 业务作用：在后端确认完成后解除未知结果观测。
    ///
    /// 参数说明：无。
    ///
    /// 返回：退出时不再报告未确认状态。
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ExecutionLeaseGuard {
    /// 业务作用：保留未确认业务结果的占位，避免取消后立即允许相同请求再次产生副作用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：只记录固定原因，不发起 abort、不创建后台任务；后续裁决仍由 store 合同决定。
    fn drop(&mut self) {
        if self.armed {
            tracing::warn!(
                "idempotency execution ended without confirmed completion; reservation retained"
            );
        }
    }
}

/// 业务作用：对方法、稳定路由、实际路径、查询串、Content-Type 与正文做长度定界哈希，避免跨资源重放。
///
/// 参数说明：
/// - `request_parts`：请求方法、实际 URI 路径、原始查询串及 Content-Type 的来源，不规范化路径编码、查询顺序或媒体类型。
/// - `route_id`：命名空间中采用的稳定路由标识，通常包含方法与匹配模板。
/// - `bytes`：容量校验后完整读取的请求体，按原字节参与比较。
///
/// 返回：每个字段以前置长度隔离的 SHA-256 指纹；同模板下的不同路径也产生不同请求身份，不解析 JSON 或比较业务语义。
fn fingerprint(
    request_parts: &axum::http::request::Parts,
    route_id: &str,
    bytes: &[u8],
) -> RequestFingerprint {
    let mut hasher = Sha256::new();
    for field in [
        request_parts.method.as_str().as_bytes(),
        route_id.as_bytes(),
        // 模板只能确定处理器，实际路径还包含目标资源；不能以同一模板允许跨资源重放。
        request_parts.uri.path().as_bytes(),
        request_parts.uri.query().unwrap_or("").as_bytes(),
        request_parts
            .headers
            .get(header::CONTENT_TYPE)
            .map(HeaderValue::as_bytes)
            .unwrap_or_default(),
        bytes,
    ] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    RequestFingerprint(hasher.finalize().into())
}

/// 业务作用：将持久层返回的 header 再次按 HTTP 类型校验后附加到重放响应。
///
/// 参数说明：
/// - `response`：接受合法存储头的响应，保留已有头且允许追加同名值。
/// - `headers`：由受信后端返回的已存储头列表，写入时应已受白名单、数量和长度限制。
///
/// 返回：逐个跳过无法解析的名称或值，其余追加到响应；此处不复验存储白名单或容量，后端不能注入任意头。
fn replay_headers(response: &mut Response, headers: Vec<StoredHeader>) {
    for stored in headers {
        let Ok(name) = HeaderName::try_from(stored.name) else {
            continue;
        };
        let Ok(value) = HeaderValue::try_from(stored.value) else {
            continue;
        };
        response.headers_mut().append(name, value);
    }
}

/// 业务作用：只提取固定安全白名单内、数量与长度有界的响应 header。
///
/// 参数说明：
/// - `response`：业务处理后的响应，仅读取白名单头，不消费正文或修改原响应。
///
/// 返回：六类允许头各取前四个候选值，再保留可读文本且不超过 1024 字节的值，合计至多 24 项。
/// 不保存 Cookie 或认证头；被省略的头不会出现在重放响应中，也不等同于完整原响应复制。
fn storable_headers(response: &Response) -> Vec<StoredHeader> {
    const ALLOWED: &[HeaderName] = &[
        header::CONTENT_TYPE,
        header::LOCATION,
        header::ETAG,
        header::RETRY_AFTER,
        header::CACHE_CONTROL,
        header::VARY,
    ];
    let mut stored = Vec::new();
    for name in ALLOWED {
        for value in response.headers().get_all(name).iter().take(4) {
            if let Ok(value) = value.to_str() {
                if value.len() <= 1024 {
                    stored.push(StoredHeader {
                        name: name.as_str().to_owned(),
                        value: value.to_owned(),
                    });
                }
            }
        }
    }
    stored
}

/// 业务作用：从持久化状态、body 与白名单 header 重建无隐式 Content-Type 的 HTTP 响应。
///
/// 参数说明：
/// - `stored`：受信后端提供的完成记录或本层构造的固定替代响应，正文与头已在写入时限界。
///
/// 返回：直接移交存储正文的响应，状态码无法解析时改为 500，非法头逐项略过；不重新执行 handler 或推断 Content-Type。
/// 本入口不重新校验正文容量、业务权限或加密要求，相关门禁必须在选择重放之前完成。
fn stored_response(stored: StoredResponse) -> Response {
    let status = StatusCode::from_u16(stored.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    // `(StatusCode, Vec<u8>).into_response()` 会先注入 `application/octet-stream`；再 append 原始
    // Content-Type 会形成两个互相冲突的值，客户端通常只读到前者。直接用 Body 构造，确保白名单
    // header 是重放响应的唯一来源。
    let mut response = Response::new(Body::from(stored.body));
    *response.status_mut() = status;
    replay_headers(&mut response, stored.headers);
    response
}

/// 业务作用：判断状态码是否具有稳定重放语义，排除认证失败、限流及服务端瞬态错误。
///
/// 参数说明：
/// - `status`：业务响应的 HTTP 状态码，不能据此推断数据库事务是否提交。
///
/// 返回：2xx、3xx 及明确列出的六种 4xx 为 true，其余为 false；true 只允许尝试存储，仍须校验正文容量和租约。
/// false 不允许自动释放执行占位，以免部分完成的业务被再次执行。
fn storable_status(status: StatusCode) -> bool {
    status.is_success()
        || status.is_redirection()
        || matches!(
            status,
            StatusCode::BAD_REQUEST
                | StatusCode::NOT_FOUND
                | StatusCode::CONFLICT
                | StatusCode::GONE
                | StatusCode::PRECONDITION_FAILED
                | StatusCode::UNPROCESSABLE_ENTITY
        )
}

/// 业务作用：对超长或占用摘要前缀的幂等命名空间做统一摘要，维持持久列宽与身份隔离。
///
/// 参数说明：
/// - `value`：租户、带类型前缀的主体或路由标识，按 UTF-8 字节长度判断。
/// - `max`：调用处使用的列宽阈值，必须至少容纳摘要文本的 79 字节。
///
/// 返回：未超限且不占用保留前缀时保留原值，否则返回 overflow-sha256: 与 64 位十六进制摘要。
/// 普通文本与摘要输出使用不同表示范围；本函数不校验 max 或验证身份来源，摘要仍受密码学碰撞边界限制。
fn bounded_namespace(value: String, max: usize) -> String {
    // 保留摘要前缀，防止某个合法短租户名称直接等于另一个长租户的摘要表示。
    if value.len() <= max && !value.starts_with("overflow-sha256:") {
        return value;
    }
    let digest = Sha256::digest(value.as_bytes());
    let mut bounded = String::with_capacity(16 + digest.len() * 2);
    bounded.push_str("overflow-sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(bounded, "{byte:02x}");
    }
    bounded
}

/// 业务作用：按已认证身份、稳定路由及客户端键协调执行占位、完成保存与响应重放。
///
/// 参数说明：
/// - `state`：冻结的存储后端、路由安全合同与 Web 前缀；正文容量由本模块固定，存活期限由后端定义。
/// - `request`：用于计算幂等身份与正文指纹的原始请求。
/// - `next`：无幂等键或取得首次执行占位后进入的业务处理链。
///
/// 返回：无键原样透传；有键时先校验身份、路由与正文，已完成记录重放，执行中或内容冲突分别返回 409、422。
/// 取得占位后只在租约完成确认成功时返回可存储的业务响应；确认失败返回 409 或 503，不能据此认定业务未提交。
/// 不可存储状态保留占位并返回原响应，正文超限或读取失败则尝试保存固定 500；取消不会自动撤销占位。
/// 缓存响应只保留受限正文和白名单头，不保留 trailers；加密路由拒绝有键请求，避免重放旧密文。
pub async fn idempotency(
    State(state): State<IdempotencyLayerState>,
    request: Request,
    next: Next,
) -> Response {
    let mut client_key_values = request.headers().get_all(IDEMPOTENCY_HEADER).iter();
    let Some(client_key_value) = client_key_values.next() else {
        return next.run(request).await; // 未请求幂等
    };
    if client_key_values.next().is_some() {
        return ApiProblem::new(
            "about:blank",
            "Bad Request",
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
        )
        .with_detail("Idempotency-Key must occur exactly once")
        .into_response();
    }
    let client_key = match client_key_value.to_str() {
        Ok(value)
            if !value.is_empty()
                && value.len() <= MAX_CLIENT_KEY_BYTES
                && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) =>
        {
            value.to_owned()
        }
        _ => {
            return ApiProblem::new(
                "about:blank",
                "Bad Request",
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
            )
            .with_detail("Idempotency-Key must be 1..190 visible ASCII bytes")
            .into_response()
        }
    };
    let matched_path = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|matched| matched.as_str())
        .unwrap_or_else(|| request.uri().path());
    let route_path = if state.context_path.is_empty() {
        matched_path
    } else {
        matched_path
            .strip_prefix(state.context_path.as_ref())
            .unwrap_or(matched_path)
    };
    // 本层拿到的是端点加密之后的响应，无法重新加密重放内容时必须在执行业务前拒绝。
    match state
        .mapping_runtime
        .route_has_crypto(request.method().as_str(), route_path)
    {
        Ok(true) => {
            return ApiProblem::new(
                "about:blank",
                "Bad Request",
                StatusCode::BAD_REQUEST,
                "idempotency_crypto_not_supported",
            )
            .with_detail(
                "Idempotency-Key is not supported on encrypted routes until plaintext replay is configured",
            )
            .into_response();
        }
        Ok(false) => {}
        Err(_) => {
            return ApiProblem::new(
                "about:blank",
                "Service Unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                "idempotency_route_contract_unavailable",
            )
            .into_response();
        }
    }
    // 命名空间只采用已认证 Principal；没有主体身份时不能让不同调用者共享客户端提供的键。
    let (tenant, subject) = match request
        .extensions()
        .get::<Principal>()
        .and_then(|principal| {
            let identity = principal
                .subject
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!("sub:{value}"))
                .or_else(|| {
                    principal
                        .client_id
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .map(|value| format!("client:{value}"))
                })?;
            Some((
                bounded_namespace(principal.tenant.clone().unwrap_or_default(), 128),
                bounded_namespace(identity, 190),
            ))
        }) {
        Some(value) => value,
        None => {
            return ApiProblem::new(
                "about:blank",
                "Unauthorized",
                StatusCode::UNAUTHORIZED,
                "idempotency_identity_required",
            )
            .with_detail("Idempotency-Key requires an authenticated subject or client")
            .into_response()
        }
    };
    let route_id = bounded_namespace(format!("{} {matched_path}", request.method()), 190);

    // 缓冲请求体以算指纹;超限拒绝。
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_IDEMPOTENT_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return ApiProblem::new(
                "about:blank",
                "Payload Too Large",
                StatusCode::PAYLOAD_TOO_LARGE,
                "idempotency_body_too_large",
            )
            .into_response()
        }
    };
    let key = IdempotencyKey {
        tenant,
        subject,
        route_id,
        client_key,
    };
    let fingerprint = fingerprint(&parts, &key.route_id, &bytes);
    let mut lease_bytes = [0_u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut lease_bytes);
    let lease = ExecutionLease(lease_bytes);

    let outcome = match state.store.begin(&key, fingerprint, lease).await {
        Ok(outcome) => outcome,
        Err(_error) => {
            // fail-closed:幂等 store 不可用时不放行,避免重复副作用。
            return ApiProblem::new(
                "about:blank",
                "Service Unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                "idempotency_store_unavailable",
            )
            .with_detail("the idempotency store is temporarily unavailable; retry later")
            .into_response();
        }
    };
    match outcome {
        IdempotencyOutcome::Replay(stored) => stored_response(stored),
        IdempotencyOutcome::ConcurrentInFlight => ApiProblem::new(
            "about:blank",
            "Conflict",
            StatusCode::CONFLICT,
            "idempotency_in_flight",
        )
        .with_detail("a request with the same Idempotency-Key is still in progress")
        .into_response(),
        IdempotencyOutcome::FingerprintConflict => ApiProblem::new(
            "about:blank",
            "Unprocessable Entity",
            StatusCode::UNPROCESSABLE_ENTITY,
            "idempotency_key_reuse",
        )
        .with_detail("the Idempotency-Key was reused with a different request body")
        .into_response(),
        IdempotencyOutcome::FirstExecution => {
            let mut lease_guard = ExecutionLeaseGuard::new();
            let request = Request::from_parts(parts, Body::from(bytes));
            let response = next.run(request).await;
            if !storable_status(response.status()) {
                // HTTP 状态码不能证明业务没有提交；保留占位，禁止靠自动释放立即重放副作用。
                return response;
            }
            let stored_headers = storable_headers(&response);
            let (response_parts, response_body) = response.into_parts();
            let response_bytes =
                match axum::body::to_bytes(response_body, MAX_IDEMPOTENT_BODY).await {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        // 业务可能已经提交，保存固定失败响应而非释放占位，避免客户端重试再次产生副作用。
                        let sentinel = StoredResponse {
                            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                            body: br#"{"code":"idempotency_response_not_storable"}"#.to_vec(),
                            headers: vec![StoredHeader {
                                name: header::CONTENT_TYPE.as_str().to_owned(),
                                value: "application/problem+json".to_owned(),
                            }],
                        };
                        if matches!(
                            state
                                .store
                                .complete(&key, fingerprint, lease, sentinel.clone())
                                .await,
                            Ok(true)
                        ) {
                            lease_guard.disarm();
                        }
                        return stored_response(sentinel);
                    }
                };
            // 完成必须仍匹配本次租约；I/O 失败 fail-closed 记录告警，但不能删除或覆盖另一 owner。
            match state
                .store
                .complete(
                    &key,
                    fingerprint,
                    lease,
                    StoredResponse {
                        status: response_parts.status.as_u16(),
                        body: response_bytes.to_vec(),
                        headers: stored_headers,
                    },
                )
                .await
            {
                Ok(true) => {
                    lease_guard.disarm();
                    Response::from_parts(response_parts, Body::from(response_bytes))
                }
                Ok(false) => {
                    tracing::warn!(
                        route_id = %key.route_id,
                        "idempotency completion rejected because execution lease was lost"
                    );
                    ApiProblem::new(
                        "about:blank",
                        "Conflict",
                        StatusCode::CONFLICT,
                        "idempotency_lease_lost",
                    )
                    .into_response()
                }
                Err(_) => {
                    tracing::warn!(
                        route_id = %key.route_id,
                        "idempotency store failed to persist completion"
                    );
                    ApiProblem::new(
                        "about:blank",
                        "Service Unavailable",
                        StatusCode::SERVICE_UNAVAILABLE,
                        "idempotency_completion_unavailable",
                    )
                    .into_response()
                }
            }
        }
    }
}
