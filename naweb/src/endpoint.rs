//! 单一路由端点流水线：身份、条件、解密、重放、handler 与响应加密。

use std::sync::Arc;
#[cfg(feature = "crypto")]
use std::sync::Mutex;
use std::time::Instant;
#[cfg(feature = "crypto")]
use std::time::SystemTime;

#[cfg(feature = "crypto")]
use axum::body::{to_bytes, Body};
#[cfg(feature = "crypto")]
use axum::extract::OriginalUri;
use axum::extract::{Request, State};
use axum::http::header::CACHE_CONTROL;
#[cfg(feature = "crypto")]
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG};
#[cfg(feature = "crypto")]
use axum::http::HeaderName;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::{
    MappingRuntime, MappingRuntimeSnapshot, RoutePolicy, RouteSecurityMetrics,
    SecurityMetricOperation, SecurityMetricOutcome,
};
// AuthMetricOutcome 只在身份阶段使用；不启用 auth 的 web-crypto 构建不得因此产生未用导入告警。
#[cfg(feature = "auth")]
use crate::AuthMetricOutcome;

#[cfg(feature = "auth")]
use crate::auth::{AuthDecision, AuthError, AuthInput, AuthShortCircuit, ShortCircuitCrypto};
#[cfg(feature = "crypto")]
use crate::crypto::{
    CryptoConditionInput, CryptoError, CryptoErrorKind, ProtocolDecryptInput, ProtocolEncryptInput,
    RequestCryptoContext,
};
#[cfg(feature = "auth")]
use crate::AuthRequirement;
// 密文响应插入的不可压缩标记;类型无条件定义在 crate 根,这里仅在加密路径使用。
#[cfg(feature = "crypto")]
use crate::UncompressibleResponse;

/// 宏为每条安全路由注入的不可变 endpoint layer 状态。
#[derive(Clone)]
pub struct EndpointLayerState {
    /// 支持 last-good 原子替换的全局安全运行时。
    pub runtime: Arc<MappingRuntime>,
    /// 当前真实路由编译期生成的静态策略。
    pub policy: RoutePolicy,
    /// effective plan 是否已经在本层外侧挂载 auth-stage interceptor。
    pub auth_interceptor: bool,
    /// 当前静态路由的低基数指标槽位，跨快照持续累积。
    metrics: Arc<RouteSecurityMetrics>,
}

/// 总 deadline 触发后重建加密错误响应所需的固定请求上下文。
#[cfg(feature = "crypto")]
#[derive(Clone)]
struct TimeoutEncryptionContext {
    /// 当前请求固定的协议实现，不在超时后重新读取新快照。
    protocol: Arc<dyn crate::crypto::CryptoProtocol>,
    /// 已完成认证解密并包含旧 key/provider 的响应上下文。
    context: RequestCryptoContext,
    /// 当前请求固定的响应大小与 JSON 深度限制。
    limits: Arc<crate::crypto::CryptoLimits>,
}

/// endpoint 运行期间共享的可选超时加密上下文槽。
#[cfg(feature = "crypto")]
type EndpointTimeoutState = Arc<Mutex<Option<TimeoutEncryptionContext>>>;
/// 不启用密码 feature 时保留内部函数形状的空状态。
#[cfg(not(feature = "crypto"))]
type EndpointTimeoutState = ();

/// 密码 route 使用的绝对总截止时刻。
#[cfg(feature = "crypto")]
type EndpointDeadline = tokio::time::Instant;
/// 不启用密码 feature 时不构造截止时刻。
#[cfg(not(feature = "crypto"))]
type EndpointDeadline = ();

impl EndpointLayerState {
    /// 建立一条 route layer 独占的安全状态。
    ///
    /// # 参数
    ///
    /// - `runtime`: 应用启动期完成审计并共享给全部路由的运行时。
    /// - `policy`: 过程宏为当前方法和路径生成的静态合同。
    /// - `auth_interceptor`: 外层是否由业务 interceptor 负责建立 AuthContext。
    ///
    /// # 返回
    ///
    /// 返回可交给 Axum `from_fn_with_state` 的轻量状态。
    pub fn new(runtime: Arc<MappingRuntime>, policy: RoutePolicy, auth_interceptor: bool) -> Self {
        let metrics = runtime.metrics().route(policy);
        Self {
            runtime,
            policy,
            auth_interceptor,
            metrics,
        }
    }
}

/// 执行固定安全顺序的 Axum route middleware。
///
/// # 参数
///
/// - `state`: 当前路由绑定的运行时与静态策略。
/// - `request`: 尚未被 extractor 消费的完整请求；body 只在 crypto request 方向读取一次。
/// - `next`: 媒体类型层、业务 extractor 与 handler 组成的后续链。
///
/// # 返回
///
/// 返回 handler 响应、受控短路响应或统一安全错误；密码 route 使用单一总 deadline。
pub async fn endpoint_middleware(
    State(state): State<EndpointLayerState>,
    request: Request,
    next: Next,
) -> Response {
    let request_started = Instant::now();
    let metrics = state.metrics.clone();
    #[cfg(feature = "auth")]
    let snapshot = request
        .extensions()
        .get::<crate::interceptor::AuthRuntimeRequestSnapshot>()
        .map(crate::interceptor::AuthRuntimeRequestSnapshot::snapshot)
        .unwrap_or_else(|| state.runtime.snapshot());
    #[cfg(not(feature = "auth"))]
    let snapshot = state.runtime.snapshot();
    #[cfg(feature = "crypto")]
    if state.policy.has_crypto() {
        if let Some(runtime) = snapshot.crypto.as_ref() {
            let deadline = tokio::time::Instant::now() + runtime.limits().max_request_duration;
            let timeout_state = Arc::new(Mutex::new(None));
            let (response, outcome) = match tokio::time::timeout_at(
                deadline,
                endpoint_inner(
                    snapshot,
                    state.policy,
                    state.auth_interceptor,
                    request,
                    next,
                    Some(deadline),
                    Some(timeout_state.clone()),
                    metrics.clone(),
                ),
            )
            .await
            {
                Ok(response) => {
                    let outcome = response_metric_outcome(&response);
                    (response, outcome)
                }
                Err(_) => (
                    encrypted_timeout_response(timeout_state).await,
                    SecurityMetricOutcome::Timeout,
                ),
            };
            metrics.record_request(outcome, request_started.elapsed());
            return response;
        }
    }
    let response = endpoint_inner(
        snapshot,
        state.policy,
        state.auth_interceptor,
        request,
        next,
        None,
        None,
        metrics.clone(),
    )
    .await;
    metrics.record_request(
        response_metric_outcome(&response),
        request_started.elapsed(),
    );
    response
}

/// 在一次固定 snapshot 上执行完整端点组合。
///
/// # 参数
///
/// - `snapshot`: 请求开始时原子读取并持续持有的 last-good 快照。
/// - `policy`: 当前已匹配路由的静态合同。
/// - `auth_interceptor`: 外层 effective plan 是否负责写入 AuthContext。
/// - `request`: handler 前唯一可改写 body 和 extensions 的请求。
/// - `next`: 后续媒体类型校验、extractor 与 handler 链。
/// - `deadline`: 密码 route 的总处理截止时刻；普通 route 为 `None`。
/// - `timeout_state`: 认证解密成功后发布的响应加密上下文，供外层总超时使用。
/// - `metrics`: 当前静态 route 的低基数原子指标，不保存任何请求输入。
///
/// # 返回
///
/// 返回最终 HTTP 响应；任一安全阶段错误都关闭请求且不执行后续阶段。
// 参数逐一对应不可交换的安全流水线输入；合并成无语义 tuple 会降低 auth-before-decrypt 审读性。
#[allow(clippy::too_many_arguments)]
async fn endpoint_inner(
    snapshot: Arc<MappingRuntimeSnapshot>,
    policy: RoutePolicy,
    auth_interceptor: bool,
    mut request: Request,
    next: Next,
    deadline: Option<EndpointDeadline>,
    timeout_state: Option<EndpointTimeoutState>,
    metrics: Arc<RouteSecurityMetrics>,
) -> Response {
    #[cfg(not(feature = "crypto"))]
    let _ = (&deadline, &timeout_state);
    #[cfg(not(feature = "auth"))]
    let _ = auth_interceptor;
    #[cfg(feature = "auth")]
    let mut pending_short_circuit: Option<AuthShortCircuit> = None;

    // 身份验证必须早于任何昂贵解密；Fore 的 token 来自可信 header，不依赖加密 body。
    #[cfg(feature = "auth")]
    if !matches!(
        policy.auth,
        AuthRequirement::Unspecified | AuthRequirement::Public
    ) {
        let auth_started = Instant::now();
        if auth_interceptor {
            // 到达这里后 endpoint composer 会统一记录身份结果与总请求结果；通知最外层 auth
            // binding 不再把本请求当成“业务提前短路”，避免多层 interceptor 重复计数。
            if let Some(marker) = request
                .extensions()
                .get::<crate::interceptor::AuthInterceptorMetricMarker>()
            {
                marker.mark_reached_gate();
            }
            match request.extensions().get::<crate::auth::AuthContext>() {
                Some(context) => {
                    if let Err(error) = crate::auth::validate_auth_context(context) {
                        metrics.record_auth(AuthMetricOutcome::Rejected, auth_started.elapsed());
                        return auth_error_response(&error, policy.error_profile);
                    }
                    metrics.record_auth(AuthMetricOutcome::Authenticated, auth_started.elapsed());
                }
                None if policy.auth == AuthRequirement::Optional => {
                    metrics.record_auth(AuthMetricOutcome::Anonymous, auth_started.elapsed());
                }
                None if request
                    .extensions()
                    .get::<crate::interceptor::AuthRuntimeAnonymous>()
                    .copied()
                    .is_some_and(|marker| marker.matches(policy.route_id, snapshot.generation)) =>
                {
                    metrics.record_auth(AuthMetricOutcome::Anonymous, auth_started.elapsed());
                }
                None => {
                    metrics.record_auth(AuthMetricOutcome::Rejected, auth_started.elapsed());
                    return auth_error_response(
                        &AuthError::new(
                            crate::auth::AuthErrorKind::Missing,
                            "auth-context-missing",
                        ),
                        policy.error_profile,
                    );
                }
            }
        } else {
            let Some(runtime) = snapshot.auth.as_ref() else {
                metrics.record_auth(AuthMetricOutcome::Rejected, auth_started.elapsed());
                return auth_error_response(
                    &AuthError::new(crate::auth::AuthErrorKind::Policy, "auth-runtime-missing"),
                    policy.error_profile,
                );
            };
            let input = AuthInput {
                method: policy.method,
                route_id: policy.route_id,
                path: request.uri().path(),
                headers: request.headers(),
                extensions: request.extensions(),
            };
            match runtime.evaluate(&policy, input).await {
                Ok(AuthDecision::Anonymous) => {
                    metrics.record_auth(AuthMetricOutcome::Anonymous, auth_started.elapsed());
                }
                Ok(AuthDecision::Authenticated(context)) => {
                    metrics.record_auth(AuthMetricOutcome::Authenticated, auth_started.elapsed());
                    request.extensions_mut().insert(context);
                }
                Ok(AuthDecision::ShortCircuit(short_circuit)) => {
                    metrics.record_auth(AuthMetricOutcome::ShortCircuit, auth_started.elapsed());
                    if short_circuit.crypto == ShortCircuitCrypto::Plaintext {
                        return ensure_no_store(short_circuit.response);
                    }
                    pending_short_circuit = Some(short_circuit);
                }
                Err(error) => {
                    metrics.record_auth(AuthMetricOutcome::Rejected, auth_started.elapsed());
                    return auth_error_response(&error, policy.error_profile);
                }
            }
        }
    }

    #[cfg(feature = "crypto")]
    if policy.has_crypto() {
        let Some(runtime) = snapshot.crypto.as_ref() else {
            return crypto_error_response(&CryptoError::new(
                CryptoErrorKind::Policy,
                "crypto-runtime-missing",
            ));
        };
        let condition_input = CryptoConditionInput {
            method: policy.method,
            route_id: policy.route_id,
            path: request.uri().path(),
            headers: request.headers(),
            extensions: request.extensions(),
        };
        let condition_started = Instant::now();
        let directions = match runtime.evaluate_condition(&policy, condition_input).await {
            Ok(value) => value,
            Err(error) => {
                metrics.observe_duration(
                    SecurityMetricOperation::Condition,
                    condition_started.elapsed(),
                );
                return crypto_error_response(&error);
            }
        };
        metrics.observe_duration(
            SecurityMetricOperation::Condition,
            condition_started.elapsed(),
        );
        let declared_directions = crate::CryptoDirections::declared(&policy);
        if directions != declared_directions {
            metrics.record_bypass();
        }
        if directions.is_empty() {
            #[cfg(feature = "auth")]
            if let Some(short_circuit) = pending_short_circuit {
                return ensure_no_store(short_circuit.response);
            }
            let handler_started = Instant::now();
            let response = next.run(request).await;
            metrics.observe_duration(SecurityMetricOperation::Handler, handler_started.elapsed());
            return response;
        }

        let tenant_scope = {
            #[cfg(feature = "auth")]
            {
                request
                    .extensions()
                    .get::<crate::auth::AuthContext>()
                    .map(|context| context.tenant.clone())
                    .unwrap_or_else(|| runtime.default_tenant_scope())
            }
            #[cfg(not(feature = "auth"))]
            {
                runtime.default_tenant_scope()
            }
        };
        let protocol = match runtime.protocol(&policy) {
            Ok(value) => value,
            Err(error) => return crypto_error_response(&error),
        };
        let provider = match runtime.provider(&policy) {
            Ok(value) => value,
            Err(error) => return crypto_error_response(&error),
        };
        let key_ring = match runtime.key_ring(&tenant_scope, &policy) {
            Ok(value) => value,
            Err(error) => return crypto_error_response(&error),
        };
        // Router::nest 会把 request.uri 改写成内部相对路径，但 Axum 会把入口实际收到的 URI
        // 保存为可信 OriginalUri extension。AAD 必须绑定外部实际 target，否则 context path 会让
        // 客户端与服务端计算不同；同时不能读取客户端可伪造的 X-Original-* header。
        let trusted_uri = request
            .extensions()
            .get::<OriginalUri>()
            .map(|value| &value.0)
            .unwrap_or_else(|| request.uri());
        let target: Arc<str> = Arc::from(
            trusted_uri
                .path_and_query()
                .map(|value| value.as_str())
                .unwrap_or_else(|| trusted_uri.path()),
        );
        let limits = runtime.limits();
        let mut crypto_context: Option<RequestCryptoContext> = None;
        let mut request_memory_permit = None;

        if directions.request {
            let mut stage_metrics = CryptoStageMetricGuard::new(metrics.clone(), true);
            if !valid_content_encoding(&request) {
                return unsupported_encoding_response();
            }
            if !content_type_matches(&request, protocol.request_content_type()) {
                return unsupported_media_type_response(protocol.request_content_type());
            }
            if content_length_exceeds(&request, limits.max_request_envelope_bytes) {
                return crypto_error_response(&CryptoError::new(
                    CryptoErrorKind::TooLarge,
                    "request-content-length-too-large",
                ));
            }
            // 必须在读取任何 body chunk 之前占用最坏预算；否则 chunked 并发可以在 admission 前
            // 先分配完整信封，令“全进程预算”只剩事后记账而失去保护作用。
            request_memory_permit = Some(
                match runtime
                    .memory_budget()
                    .reserve(limits.request_memory_reservation())
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        stage_metrics.set_outcome(metric_outcome_for_crypto_error(&error));
                        return crypto_error_response(&error);
                    }
                },
            );
            let request_without_body = std::mem::replace(request.body_mut(), Body::empty());
            let envelope = match to_bytes(
                request_without_body,
                limits.max_request_envelope_bytes.saturating_add(1),
            )
            .await
            {
                Ok(value) if value.len() <= limits.max_request_envelope_bytes => value,
                _ => {
                    return crypto_error_response(&CryptoError::new(
                        CryptoErrorKind::TooLarge,
                        "request-body-read-limit",
                    ))
                }
            };
            let replay_guard = match runtime.replay_guard(&policy) {
                Ok(value) => value,
                Err(error) => {
                    stage_metrics.set_outcome(metric_outcome_for_crypto_error(&error));
                    return crypto_error_response(&error);
                }
            };
            let output = match protocol
                .decrypt(ProtocolDecryptInput {
                    envelope,
                    route: policy,
                    target: target.clone(),
                    tenant_scope: tenant_scope.clone(),
                    key_ring: key_ring.clone(),
                    provider: provider.clone(),
                    replay_guard,
                    replay_ttl: runtime.replay_ttl(),
                    replay_timeout: runtime.replay_request_timeout(),
                    now: SystemTime::now(),
                    limits: limits.clone(),
                })
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    stage_metrics.set_outcome(metric_outcome_for_crypto_error(&error));
                    if policy.replay == crate::ReplayRequirement::Required {
                        match error.kind {
                            CryptoErrorKind::Replayed => {
                                metrics.record_replay(crate::ReplayMetricOutcome::Duplicate)
                            }
                            CryptoErrorKind::ReplayBackend | CryptoErrorKind::Timeout => {
                                metrics.record_replay(crate::ReplayMetricOutcome::BackendFailure)
                            }
                            _ => {}
                        }
                    }
                    return crypto_error_response(&error);
                }
            };
            stage_metrics.set_outcome(SecurityMetricOutcome::Success);
            if policy.replay == crate::ReplayRequirement::Required {
                metrics.record_replay(crate::ReplayMetricOutcome::Fresh);
            }
            let plaintext = output.plaintext.into_vec();
            *request.body_mut() = Body::from(plaintext);
            request.headers_mut().remove(CONTENT_LENGTH);
            request.headers_mut().remove(CONTENT_ENCODING);
            request
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            crypto_context = Some(output.context);
        } else if directions.response {
            if protocol.id() == "modern-v2" {
                return crypto_error_response(&CryptoError::new(
                    CryptoErrorKind::Policy,
                    "modern-response-without-request-context",
                ));
            }
            let key = match key_ring.active_for_audit() {
                Ok(value) => value,
                Err(_) => {
                    return crypto_error_response(&CryptoError::new(
                        CryptoErrorKind::Key,
                        "response-active-key-missing",
                    ))
                }
            };
            crypto_context = Some(RequestCryptoContext {
                protocol_id: protocol.id(),
                rid: None,
                key,
                tenant_scope: tenant_scope.clone(),
                key_scope: Arc::from(policy.crypto_key_scope.unwrap_or_default()),
                audience: Arc::from(policy.audience),
                method: policy.method,
                target: target.clone(),
                provider: provider.clone(),
            });
        }

        // 总 deadline 可以在 handler 或响应加密期间触发。只有认证解密完成且路由声明响应加密后
        // 才发布上下文；外层超时据此生成同 snapshot 的加密 503，不会退化为明文业务错误。
        if directions.response {
            if let (Some(timeout_state), Some(context)) =
                (timeout_state.as_ref(), crypto_context.as_ref())
            {
                if let Ok(mut slot) = timeout_state.lock() {
                    *slot = Some(TimeoutEncryptionContext {
                        protocol: protocol.clone(),
                        context: context.clone(),
                        limits: limits.clone(),
                    });
                }
            }
        }

        // handler_started 落在 crypto 块内、必被下方 observe_duration 使用，因此不随 auth 开关；
        // 只有"是否可能存在待发布的身份短路响应"才依赖 auth feature，故仅 match 分支按 auth 分叉。
        let handler_started = Instant::now();
        #[cfg(feature = "auth")]
        let response = match pending_short_circuit {
            Some(short_circuit) => {
                // 短路响应不执行 handler，因此在释放明文预算前主动销毁已解密请求，避免未计费明文残留。
                drop(request);
                drop(request_memory_permit.take());
                short_circuit.response
            }
            None => run_crypto_handler(request, next, deadline, request_memory_permit.take()).await,
        };
        #[cfg(not(feature = "auth"))]
        let response =
            run_crypto_handler(request, next, deadline, request_memory_permit.take()).await;
        metrics.observe_duration(SecurityMetricOperation::Handler, handler_started.elapsed());

        if !directions.response {
            return response;
        }
        let mut stage_metrics = CryptoStageMetricGuard::new(metrics.clone(), false);
        let status = response.status();
        let (mut parts, body) = response.into_parts();
        // 响应 body 也可能是延迟产生的流；在收集前占用预算，避免多个 handler 同时返回大实体时
        // 先形成内存峰值、随后才发现 semaphore 已耗尽。
        let _memory_permit = match runtime
            .memory_budget()
            .reserve(limits.response_memory_reservation())
            .await
        {
            Ok(value) => value,
            Err(error) => {
                stage_metrics.set_outcome(metric_outcome_for_crypto_error(&error));
                return crypto_error_response(&error);
            }
        };
        let plaintext =
            match to_bytes(body, limits.max_response_plaintext_bytes.saturating_add(1)).await {
                Ok(value) if value.len() <= limits.max_response_plaintext_bytes => value.to_vec(),
                _ => {
                    return crypto_error_response(&CryptoError::new(
                        CryptoErrorKind::TooLarge,
                        "response-body-read-limit",
                    ))
                }
            };
        let context = match crypto_context {
            Some(value) => value,
            None => {
                return crypto_error_response(&CryptoError::new(
                    CryptoErrorKind::Policy,
                    "response-crypto-context-missing",
                ))
            }
        };
        let encrypted = match protocol
            .encrypt(ProtocolEncryptInput {
                plaintext: zeroize::Zeroizing::new(plaintext),
                status: status.as_u16(),
                context,
                now: SystemTime::now(),
                limits,
            })
            .await
        {
            Ok(value) => value,
            Err(error) => {
                stage_metrics.set_outcome(metric_outcome_for_crypto_error(&error));
                return crypto_error_response(&error);
            }
        };
        clear_entity_headers(&mut parts.headers);
        parts.headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static(encrypted.content_type),
        );
        parts
            .headers
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        // 密文响应绝不能再被下游压缩(压缩+加密同用有 CRIME/BREACH 侧信道风险)。留一个响应扩展
        // 标记,让外层压缩中间件据此跳过——注意 legacy-v1 密文的 content-type 就是 application/json,
        // 仅凭内容类型无法与明文 JSON 区分,因此必须显式标记。
        parts.extensions.insert(UncompressibleResponse);
        stage_metrics.set_outcome(SecurityMetricOutcome::Success);
        return Response::from_parts(parts, Body::from(encrypted.envelope));
    }

    #[cfg(feature = "auth")]
    if let Some(short_circuit) = pending_short_circuit {
        return ensure_no_store(short_circuit.response);
    }
    let handler_started = Instant::now();
    let response = next.run(request).await;
    metrics.observe_duration(SecurityMetricOperation::Handler, handler_started.elapsed());
    response
}

/// 在提前返回路径上也能完整记录单个密码方向结果的作用域守卫。
#[cfg(feature = "crypto")]
struct CryptoStageMetricGuard {
    /// 当前静态 route 的指标槽位。
    metrics: Arc<RouteSecurityMetrics>,
    /// `true` 为请求解密，`false` 为响应加密。
    request_direction: bool,
    /// 阶段开始的单调时刻。
    started: Instant,
    /// 当前结果；协议前置校验默认按客户端拒绝统计。
    outcome: SecurityMetricOutcome,
}

#[cfg(feature = "crypto")]
impl CryptoStageMetricGuard {
    /// 建立带方向安全默认值的密码阶段守卫。
    ///
    /// # 参数
    ///
    /// - `metrics`: 当前 route 的固定指标槽位。
    /// - `request_direction`: `true` 表示请求解密，`false` 表示响应加密。
    ///
    /// # 返回
    ///
    /// 返回在 drop 时必定记录一次结果和耗时的守卫；请求协议前置失败默认为拒绝，响应封装失败
    /// 默认为服务故障。
    fn new(metrics: Arc<RouteSecurityMetrics>, request_direction: bool) -> Self {
        Self {
            metrics,
            request_direction,
            started: Instant::now(),
            outcome: if request_direction {
                SecurityMetricOutcome::Rejected
            } else {
                SecurityMetricOutcome::Failure
            },
        }
    }

    /// 在成功、后端故障或超时已知时替换默认结果。
    ///
    /// # 参数
    ///
    /// - `outcome`: composer 收敛后的有限结果类别。
    fn set_outcome(&mut self, outcome: SecurityMetricOutcome) {
        self.outcome = outcome;
    }
}

#[cfg(feature = "crypto")]
impl Drop for CryptoStageMetricGuard {
    /// 在任意提前返回或正常结束时记录且只记录一次密码阶段观测。
    fn drop(&mut self) {
        self.metrics
            .record_crypto(self.request_direction, self.outcome, self.started.elapsed());
    }
}

/// 把内部密码错误映射为不暴露细节的有限指标结果。
///
/// # 参数
///
/// - `error`: 不携带 secret 的内部稳定错误类别。
///
/// # 返回
///
/// 输入、key、tag、时间窗和重放冲突归为拒绝；资源、后端和内部故障归为失败；明确预算超时
/// 单独归为 timeout。
#[cfg(feature = "crypto")]
fn metric_outcome_for_crypto_error(error: &CryptoError) -> SecurityMetricOutcome {
    match error.kind {
        CryptoErrorKind::Envelope
        | CryptoErrorKind::TooLarge
        | CryptoErrorKind::Key
        | CryptoErrorKind::Authentication
        | CryptoErrorKind::Expired
        | CryptoErrorKind::Replayed => SecurityMetricOutcome::Rejected,
        CryptoErrorKind::Timeout => SecurityMetricOutcome::Timeout,
        CryptoErrorKind::Policy
        | CryptoErrorKind::ReplayBackend
        | CryptoErrorKind::Overloaded
        | CryptoErrorKind::ResponseContract
        | CryptoErrorKind::Internal => SecurityMetricOutcome::Failure,
    }
}

/// 读取安全短路写入的内部指标结果，普通业务响应再按 HTTP 状态收敛。
///
/// # 参数
///
/// - `response`: 即将返回客户端的最终响应；extensions 不会被序列化到网络。
///
/// # 返回
///
/// 返回内部稳定结果；这能避免兼容 profile 用 HTTP 200 表达身份失败时被误记为成功。
fn response_metric_outcome(response: &Response) -> SecurityMetricOutcome {
    response
        .extensions()
        .get::<SecurityMetricOutcome>()
        .copied()
        .unwrap_or_else(|| SecurityMetricOutcome::from_status(response.status()))
}

/// 把内部指标结果写入不会发送到网络的响应扩展。
///
/// # 参数
///
/// - `response`: 安全错误或受控故障响应。
/// - `outcome`: 不含请求数据的有限结果类别。
///
/// # 返回
///
/// 返回带内部标记的同一响应；后续响应加密拆装 parts 时仍会保留该标记。
fn mark_metric_outcome(mut response: Response, outcome: SecurityMetricOutcome) -> Response {
    response.extensions_mut().insert(outcome);
    response
}

/// 在独立异步任务中执行 extractor 与业务 handler，并收敛 panic 和总 deadline。
///
/// # 参数
///
/// - `request`: 已完成身份和可选解密改写的请求，所有权随任务结束或取消而释放。
/// - `next`: Axum 后续 extractor 与 handler 链。
/// - `deadline`: 安全路由从进入 composer 起计算的绝对截止时刻；普通路由省略。
/// - `memory_permit`: 解密请求明文的全进程预算，必须与 handler 任务共同存活。
///
/// # 返回
///
/// 正常完成返回业务响应；panic 或超时返回稳定 503 结构。调用方随后仍按静态响应策略加密该
/// 结构，因此已经认证解密的请求不会因业务故障退化为明文错误。超时任务会先中止并等待释放
/// request，再归还请求明文内存预算。
#[cfg(feature = "crypto")]
async fn run_crypto_handler(
    request: Request,
    next: Next,
    deadline: Option<EndpointDeadline>,
    memory_permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Response {
    let Some(deadline) = deadline else {
        let _memory_permit = memory_permit;
        return next.run(request).await;
    };
    let mut task = tokio::spawn(async move {
        // permit 放进任务而不是外层 future；总超时取消外层时，业务任务真正停止前不会提前释放
        // 明文预算，也不会让大量断开客户端绕过全进程容量限制。
        let _memory_permit = memory_permit;
        next.run(request).await
    });
    let _abort_guard = HandlerAbortGuard(task.abort_handle());
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => handler_failure_response(),
        Err(_) => {
            task.abort();
            let _ = task.await;
            handler_failure_response()
        }
    }
}

/// 外层 endpoint future 被总超时取消时向业务任务发送中止信号。
#[cfg(feature = "crypto")]
struct HandlerAbortGuard(tokio::task::AbortHandle);

#[cfg(feature = "crypto")]
impl Drop for HandlerAbortGuard {
    /// 中止尚未完成的业务任务，使 request 和明文预算最终随任务销毁。
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// 在总 deadline 后尽力使用请求固定上下文加密稳定 503。
///
/// # 参数
///
/// - `timeout_state`: endpoint 在认证解密完成后发布的不可变协议、key/provider 与限制。
///
/// # 返回
///
/// 已建立响应密码上下文时返回加密错误信封；超时发生在认证前或补偿加密本身失败时返回统一明文
/// 安全错误，绝不透传 handler 的部分响应或明文业务数据。
#[cfg(feature = "crypto")]
async fn encrypted_timeout_response(timeout_state: EndpointTimeoutState) -> Response {
    let context = timeout_state.lock().ok().and_then(|slot| slot.clone());
    let Some(context) = context else {
        return crypto_error_response(&CryptoError::new(
            CryptoErrorKind::Timeout,
            "endpoint-total-deadline",
        ));
    };
    let plaintext = serde_json::to_vec(&serde_json::json!({
        "code": 503,
        "msg": "安全服务暂不可用",
        "data": null
    }))
    .unwrap_or_default();
    let encrypted = context
        .protocol
        .encrypt(ProtocolEncryptInput {
            plaintext: zeroize::Zeroizing::new(plaintext),
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            context: context.context,
            now: SystemTime::now(),
            limits: context.limits,
        })
        .await;
    match encrypted {
        Ok(encrypted) => {
            let mut response = Response::new(Body::from(encrypted.envelope));
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static(encrypted.content_type),
            );
            // 同上:密文响应标记为不可压缩,供外层压缩中间件跳过。
            response.extensions_mut().insert(UncompressibleResponse);
            ensure_no_store(response)
        }
        Err(_) => crypto_error_response(&CryptoError::new(
            CryptoErrorKind::Timeout,
            "endpoint-timeout-encryption",
        )),
    }
}

/// 建立业务 panic 或总 deadline 使用的稳定、禁缓存响应。
///
/// # 返回
///
/// 返回不含 panic、handler 或请求详情的 503 JSON；若路由声明响应加密，外层流程会继续封装。
// 仅在 crypto 数据面被 run_crypto_handler 调用；auth-only 构建不会实例化，故按 feature 收敛避免死代码告警。
#[cfg(feature = "crypto")]
fn handler_failure_response() -> Response {
    mark_metric_outcome(
        ensure_no_store(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "code": 503,
                    "msg": "安全服务暂不可用"
                })),
            )
                .into_response(),
        ),
        SecurityMetricOutcome::Failure,
    )
}

/// 判断请求 Content-Encoding 是否为空或 identity。
///
/// # 参数
///
/// - `request`: 尚未读取 body 的完整 HTTP 请求。
///
/// # 返回
///
/// 仅缺失或大小写不敏感的 `identity` 返回 `true`。
#[cfg(feature = "crypto")]
fn valid_content_encoding(request: &Request) -> bool {
    request
        .headers()
        .get(CONTENT_ENCODING)
        .map(|value| value.as_bytes().eq_ignore_ascii_case(b"identity"))
        .unwrap_or(true)
}

/// 精确检查路由协议要求的外层 Content-Type。
///
/// # 参数
///
/// - `request`: 尚未读取 body 的请求。
/// - `expected`: 已注册协议提供的静态媒体类型。
///
/// # 返回
///
/// modern-v2 要求完整精确匹配；legacy JSON 允许常见 charset 参数。
#[cfg(feature = "crypto")]
fn content_type_matches(request: &Request, expected: &str) -> bool {
    let Some(actual) = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if expected == crate::crypto::MODERN_V2_MEDIA_TYPE {
        actual == expected
    } else {
        actual
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
    }
}

/// 在读取 body 前利用公开 Content-Length 快速拒绝明显超限请求。
///
/// # 参数
///
/// - `request`: 尚未读取 body 的请求。
/// - `maximum`: route protocol 的外层信封字节上限。
///
/// # 返回
///
/// 只有合法数字且超过上限时返回 `true`；缺失或伪小值仍由实际读取硬限制兜底。
#[cfg(feature = "crypto")]
fn content_length_exceeds(request: &Request, maximum: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|value| value > maximum)
}

/// 移除基于旧明文实体计算或会触发压缩的响应头。
///
/// # 参数
///
/// - `headers`: handler 返回、即将改写 body 的响应头集合。
#[cfg(feature = "crypto")]
fn clear_entity_headers(headers: &mut axum::http::HeaderMap) {
    headers.remove(CONTENT_LENGTH);
    headers.remove(CONTENT_ENCODING);
    headers.remove(ETAG);
    for name in ["content-md5", "digest", "content-digest"] {
        headers.remove(HeaderName::from_static(name));
    }
}

/// 为安全边界错误生成固定结构和禁缓存响应。
///
/// # 参数
///
/// - `error`: 已收敛且不含敏感详情的密码错误。
///
/// # 返回
///
/// 返回稳定状态和公开类别，不暴露 kid、tag、padding 与后端信息。
#[cfg(feature = "crypto")]
fn crypto_error_response(error: &CryptoError) -> Response {
    let body = axum::Json(serde_json::json!({
        "code": error.public_code(),
        "msg": "安全请求处理失败"
    }));
    mark_metric_outcome(
        ensure_no_store((error.status(), body).into_response()),
        metric_outcome_for_crypto_error(error),
    )
}

/// 为标准或 Fore 兼容 profile 生成结构化身份错误。
///
/// # 参数
///
/// - `error`: 不含凭证和后端详情的身份错误。
/// - `profile`: route policy 静态绑定的错误 profile。
///
/// # 返回
///
/// 返回禁缓存响应；Fore 兼容 profile 保持旧 HTTP 200 业务码，其它 profile 使用 401/403/503。
#[cfg(feature = "auth")]
pub(crate) fn auth_error_response(error: &AuthError, profile: &str) -> Response {
    let (status, code, message) = if profile == "fore-rest-legacy" {
        match error.kind {
            crate::auth::AuthErrorKind::Disabled => (StatusCode::OK, 400, "账户不可用"),
            crate::auth::AuthErrorKind::Missing | crate::auth::AuthErrorKind::Invalid => {
                (StatusCode::OK, 1000, "请先登录")
            }
            crate::auth::AuthErrorKind::Backend | crate::auth::AuthErrorKind::Policy => {
                (StatusCode::SERVICE_UNAVAILABLE, 1503, "身份服务暂不可用")
            }
        }
    } else {
        let code = match error.kind {
            crate::auth::AuthErrorKind::Missing | crate::auth::AuthErrorKind::Invalid => 1401,
            crate::auth::AuthErrorKind::Disabled => 1403,
            crate::auth::AuthErrorKind::Backend | crate::auth::AuthErrorKind::Policy => 1503,
        };
        (error.status(), code, "身份认证失败")
    };
    let body = axum::Json(serde_json::json!({"code": code, "msg": message}));
    let outcome = match error.kind {
        crate::auth::AuthErrorKind::Missing
        | crate::auth::AuthErrorKind::Invalid
        | crate::auth::AuthErrorKind::Disabled => SecurityMetricOutcome::Rejected,
        crate::auth::AuthErrorKind::Backend | crate::auth::AuthErrorKind::Policy => {
            SecurityMetricOutcome::Failure
        }
    };
    mark_metric_outcome(ensure_no_store((status, body).into_response()), outcome)
}

/// 为所有安全响应强制添加 `Cache-Control: no-store`。
///
/// # 参数
///
/// - `response`: 身份短路或安全错误响应。
///
/// # 返回
///
/// 返回写入禁缓存头后的同一响应。
pub(crate) fn ensure_no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// 返回不支持加密实体压缩的稳定 415 响应。
///
/// # 返回
///
/// 返回禁缓存且不包含请求数据的结构化错误。
#[cfg(feature = "crypto")]
fn unsupported_encoding_response() -> Response {
    ensure_no_store(
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            axum::Json(serde_json::json!({
                "code": "invalid_secure_request",
                "msg": "不支持该 Content-Encoding"
            })),
        )
            .into_response(),
    )
}

/// 返回协议 Content-Type 不匹配的稳定 415 响应。
///
/// # 参数
///
/// - `expected`: route 静态协议要求的公开媒体类型。
///
/// # 返回
///
/// 返回禁缓存响应；只回显服务端静态值，不回显客户端输入。
#[cfg(feature = "crypto")]
fn unsupported_media_type_response(expected: &'static str) -> Response {
    ensure_no_store(
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            axum::Json(serde_json::json!({
                "code": "invalid_secure_request",
                "msg": format!("请求必须使用 {expected}")
            })),
        )
            .into_response(),
    )
}
