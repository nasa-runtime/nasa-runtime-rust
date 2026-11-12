//! authentication 中间件:校验 Bearer access token,把**已验证**主体写入请求扩展。
//!
//! 从 `Authorization: Bearer <jwt>` 取 token，经 [`verify_access_token`] 完成 JWKS 选 kid、RS256 验签和
//! RFC 9068/8725 claims 校验后，用 token 的 `scope` 构造 [`Principal`] 注入请求扩展，供下游
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

/// 由认证组件或业务 UserHook 发布的认证器：共享 JWKS 注册表与固定 token 校验策略。
pub struct Authenticator {
    /// 每个请求读取的当前有效密钥来源，更新由注册表持有者负责。
    jwks: Arc<JwksRegistry>,
    /// 验签时强制的身份、算法和时间要求，不随请求内容改变。
    policy: TokenPolicy,
}

impl Authenticator {
    /// 业务作用：组合共享密钥快照来源与固定 token 策略，供请求认证阶段使用。
    ///
    /// 参数说明：
    /// - `jwks`：已完成初始密钥校验的注册表；请求读取其当前快照，轮换由注册表持有者负责。
    /// - `policy`：issuer、audience、算法与时钟容差要求；调用方须先校验配置。
    ///
    /// 返回：保存注册表与策略的认证器，不发起网络请求、不验签，也不自动安装 Web 中间件。
    pub fn new(jwks: Arc<JwksRegistry>, policy: TokenPolicy) -> Self {
        Self {
            jwks,
            policy,
        }
    }
}

/// 业务注入 Web 装配用的认证器句柄。
pub type SharedAuthenticator = Arc<Authenticator>;

/// 业务作用：从 `Authorization` 头取 Bearer token。没有 header 返回 `Ok(None)`；header 已出现但重复、
/// 非 UTF-8、scheme 错或 token 为空都返回 `Err`，不能降级成匿名请求。
///
/// 参数说明：
/// - `request`：待认证请求；只读取全部 Authorization 头，不读取正文或修改请求。
///
/// 返回：头缺失时为 None；唯一且以不区分大小写的 `Bearer ` 开头时，借用去除首尾空白的非空 token；
/// 重复头或格式不合法时为 Err，不判断 token 签名和 claims。
fn bearer_token(request: &Request) -> Result<Option<&str>, ()> {
    let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    // 多个认证头可能被代理与服务按不同顺序解释，不能择一接受或降级为匿名请求。
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

/// 业务作用：从**已经验证**的 claims 构造 Principal，保留授权 scope 与幂等命名空间所需身份。
///
/// 参数说明：
/// - `claims`：签名、时间与身份策略均已通过校验的 access token claims，不可传入仅解码的非受信内容。
///
/// 返回：按空白分割 scope 并复制 subject、client_id、tenant 的主体；缺少 scope 时无授权范围，
/// 不在此补授权限或查询业务身份。
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

/// 业务作用：构造统一 401 Problem Details，并附加 RFC 6750 Bearer challenge。
///
/// 参数说明：无。
///
/// 返回：包含固定 invalid_token 摘要与 WWW-Authenticate 头的 401 响应，不携带原 token、claims 或验签失败细节。
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

/// 业务作用：在授权前验证已提供的 Bearer 凭据，仅把验签和策略校验通过的主体写入请求扩展。
///
/// 参数说明：
/// - `authenticator`：启动时选择的受信认证实现。
/// - `request`：可能携带 Authorization 头的请求；成功认证后覆盖 Principal 扩展，缺少头时原样传递。
/// - `next`：接收无凭据请求或已验证主体的后续链，受保护资源仍需由授权层拒绝匿名访问。
///
/// 返回：头缺失或认证成功时返回后续链响应；头不合法或 token 验证失败时返回统一 401，不执行后续链。
/// 每次验签使用同一份当前密钥快照和当前系统时间，不在请求内拉取新密钥。
pub async fn authenticate(
    State(authenticator): State<SharedAuthenticator>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = match bearer_token(&request) {
        Ok(Some(token)) => token,
        // 缺少凭据仅表示本层不认证；受保护资源的拒绝责任仍在后续授权层。
        Ok(None) => return next.run(request).await,
        Err(()) => return invalid_token_response(),
    };
    let jwks = authenticator.jwks.current();
    match verify_access_token(token, &jwks, &authenticator.policy, SystemTime::now()) {
        Ok(claims) => {
            // 只有完整校验成功的 claims 能成为授权依据，不能在验签前发布主体扩展。
            request
                .extensions_mut()
                .insert(principal_from_claims(&claims));
            next.run(request).await
        }
        Err(_error) => invalid_token_response(),
    }
}
