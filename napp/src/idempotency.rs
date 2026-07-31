//! 幂等请求中间件:把 [`naidempotency`] 状态机接到 Web 请求路径。
//!
//! 有 `Idempotency-Key` 头时按 (route, key, 请求体指纹) 裁决:首次→执行并保存结果;完成后同键同体→
//! 重放原结果(不重跑 handler);仍在执行→409;同键不同体→422。无键则透传(不启用幂等)。业务在
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
const LEASE_CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// 参与幂等的请求/响应体上限;超限拒绝。
const MAX_IDEMPOTENT_BODY: usize = 1 << 20;

/// 业务在 UserHook 注入的幂等 store(内存/DB/Redis 实现同一 trait)。
pub type SharedIdempotencyStore = Arc<dyn IdempotencyStore + Send + Sync>;

/// Web 装配时冻结的幂等层状态。
#[derive(Clone)]
pub struct IdempotencyLayerState {
    store: SharedIdempotencyStore,
    mapping_runtime: Arc<naweb::MappingRuntime>,
    context_path: Arc<str>,
}

impl IdempotencyLayerState {
    /// 绑定 store、已经完成路由审计的 mapping runtime 与 Web context path。
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

/// 首次执行占位的取消安全守卫。
///
/// Web 请求 future 可能被入口 deadline、客户端断连或服务排空直接取消；这些路径不会继续执行普通
/// `abort().await`。守卫在 Drop 时只针对本次 fingerprint + 随机 lease 发起 best-effort 清理，绝不会
/// 删除后来取得同一 key 的 owner。持久后端另有 lease TTL 作为进程崩溃时的最终兜底。
struct ExecutionLeaseGuard {
    store: SharedIdempotencyStore,
    key: IdempotencyKey,
    fingerprint: RequestFingerprint,
    lease: ExecutionLease,
    armed: bool,
}

impl ExecutionLeaseGuard {
    /// 为一次首次执行绑定 store、命名空间、fingerprint 与随机 lease，并默认启用清理。
    fn new(
        store: SharedIdempotencyStore,
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
        lease: ExecutionLease,
    ) -> Self {
        Self {
            store,
            key,
            fingerprint,
            lease,
            armed: true,
        }
    }

    /// 完成或显式 abort 后关闭 Drop 清理，防止重复请求后端。
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ExecutionLeaseGuard {
    /// 请求 future 非正常结束时，在有界后台任务中 best-effort 释放仍属于本 owner 的 lease。
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                route_id = %self.key.route_id,
                "idempotency lease cleanup could not start because no Tokio runtime is active"
            );
            return;
        };
        let store = Arc::clone(&self.store);
        let key = self.key.clone();
        let fingerprint = self.fingerprint;
        let lease = self.lease;
        runtime.spawn(async move {
            if !matches!(
                tokio::time::timeout(LEASE_CLEANUP_TIMEOUT, store.abort(&key, fingerprint, lease))
                    .await,
                Ok(Ok(true))
            ) {
                tracing::warn!(
                    route_id = %key.route_id,
                    "cancelled idempotency lease could not be released"
                );
            }
        });
    }
}

/// 对 method、route、query、Content-Type 与 body 做长度定界哈希，形成请求等价类指纹。
fn fingerprint(
    request_parts: &axum::http::request::Parts,
    route_id: &str,
    bytes: &[u8],
) -> RequestFingerprint {
    let mut hasher = Sha256::new();
    for field in [
        request_parts.method.as_str().as_bytes(),
        route_id.as_bytes(),
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

/// 将持久层返回的 header 再次按 HTTP 类型校验后附加到重放响应。
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

/// 只提取固定安全白名单内、数量与长度有界的响应 header。
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

/// 从持久化状态、body 与白名单 header 重建无隐式 Content-Type 的 HTTP 响应。
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

/// 判断状态码是否具有稳定重放语义，排除认证失败、限流及服务端瞬态错误。
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

/// 保留数据库列范围内的既有 namespace 文本；极端长值才摘要化，避免合法身份因列溢出变成 503，
/// 同时不打断常规 key 的已有持久 replay。
fn bounded_namespace(value: String, max: usize) -> String {
    if value.len() <= max {
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

/// 幂等中间件。无 `Idempotency-Key` 头则透传;有则按 (route, key, body 指纹) 裁决。
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
            let mut lease_guard =
                ExecutionLeaseGuard::new(Arc::clone(&state.store), key.clone(), fingerprint, lease);
            let request = Request::from_parts(parts, Body::from(bytes));
            let response = next.run(request).await;
            if !storable_status(response.status()) {
                match state.store.abort(&key, fingerprint, lease).await {
                    Ok(true) => lease_guard.disarm(),
                    Ok(false) | Err(_) => tracing::warn!(
                        route_id = %key.route_id,
                        "idempotency lease could not be released for a non-storable response"
                    ),
                }
                return response;
            }
            let stored_headers = storable_headers(&response);
            let (response_parts, response_body) = response.into_parts();
            let response_bytes =
                match axum::body::to_bytes(response_body, MAX_IDEMPOTENT_BODY).await {
                    Ok(bytes) => bytes,
                    Err(_) => {
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
