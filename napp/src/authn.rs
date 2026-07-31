//! authentication 中间件:校验 Bearer access token,把**已验证**主体写入请求扩展。
//!
//! 从 `Authorization: Bearer <jwt>` 取 token,经 [`verify_access_token`](JWKS 选 kid → RS256 验签 →
//! RFC 9068/8725 claims 校验)通过后,用 token 的 `scope` 构造 [`Principal`] 注入请求扩展,供下游
//! [`authorize`](crate::authorize)判定。**认证永远早于授权**,故本层装在 authz 之外。
//!
//! - 无 `Authorization` 头 → 匿名放行(不注入 Principal;受保护 route 由 authz 拒)。
//! - token 存在但校验失败 → 401 problem+json + `WWW-Authenticate: Bearer`(RFC 6750),不进入内层。
//!
//! **不验签的 claims 不足以信任**:本层用 `verify_access_token`(含签名验证),不是 `parse_unverified`。

use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use naauthz::Principal;
use nauth_oauth::{verify_access_token, JwksRegistry, TokenPolicy};

use crate::problem::ApiProblem;

/// 业务经 UserHook 注入的认证器:JWKS 注册表(热更新保 last-good)+ token 校验策略。
pub struct Authenticator {
    jwks: Arc<JwksRegistry>,
    policy: TokenPolicy,
}

impl Authenticator {
    /// 用 JWKS 注册表与校验策略构造。
    pub fn new(jwks: Arc<JwksRegistry>, policy: TokenPolicy) -> Self {
        Self { jwks, policy }
    }
}

/// 业务注入 Web 装配用的认证器句柄。
pub type SharedAuthenticator = Arc<Authenticator>;

/// 从 `Authorization` 头取 Bearer token。没有 header 返回 `Ok(None)`；header 已出现但重复、
/// 非 UTF-8、scheme 错或 token 为空都返回 `Err`，不能降级成匿名请求。
fn bearer_token(request: &Request) -> Result<Option<&str>, ()> {
    let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = first.to_str().map_err(|_| ())?;
    // `get(..7)` 字节切片越界/非 ASCII 边界安全地返回 None,不 panic。
    let token = value
        .get(..7)
        .filter(|prefix| prefix.eq_ignore_ascii_case("bearer "))
        .and_then(|_| value.get(7..))
        .ok_or(())?
        .trim();
    if token.is_empty() {
        Err(())
    } else {
        Ok(Some(token))
    }
}

/// 从**已经验证**的 claims 构造 Principal，保留授权 scope 与幂等命名空间所需身份。
fn principal_from_claims(claims: &nauth_oauth::AccessTokenClaims) -> Principal {
    let mut principal = match claims.scope.as_deref() {
        Some(scope) => Principal::with_scopes(scope.split_whitespace()),
        None => Principal::default(),
    };
    principal.subject = claims.sub.clone();
    principal.client_id = claims.client_id.clone();
    principal.tenant = claims.tenant.clone();
    principal
}

/// 构造统一 401 Problem Details，并附加 RFC 6750 Bearer challenge。
fn invalid_token_response() -> Response {
    let mut response = ApiProblem::new(
        "about:blank",
        "Unauthorized",
        StatusCode::UNAUTHORIZED,
        "invalid_token",
    )
    .with_detail(
        "the access token is missing required claims, expired, or has an invalid signature",
    )
    .into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer error=\"invalid_token\""),
    );
    response
}

/// authentication 中间件。见模块文档。
pub async fn authenticate(
    State(authenticator): State<SharedAuthenticator>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = match bearer_token(&request) {
        Ok(Some(token)) => token,
        Ok(None) => return next.run(request).await,
        Err(()) => return invalid_token_response(),
    };
    let jwks = authenticator.jwks.current();
    match verify_access_token(token, &jwks, &authenticator.policy, SystemTime::now()) {
        Ok(claims) => {
            request
                .extensions_mut()
                .insert(principal_from_claims(&claims));
            next.run(request).await
        }
        Err(_error) => invalid_token_response(),
    }
}
