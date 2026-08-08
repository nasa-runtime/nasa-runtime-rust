//! route 级授权中间件:把 [`naauthz`] 策略决策接到 Web 请求路径。
//!
//! 从请求扩展读取 [`Principal`](由上游 authentication 层写入),对 `METHOD /path` route 查
//! [`PolicyRegistry`] 决策:Permit 放行;Deny → 403 [`ApiProblem`](不回显主体敏感数据)。装在
//! authentication 之后、decrypt/handler 之前。

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use naauthz::{AuthzDecision, Principal};

use crate::problem::ApiProblem;

/// 业务/Auth 层发布的进程级授权策略注册表(热更新保 last-good)。
pub type SharedPolicyRegistry = Arc<naauthz::PolicyRegistry>;
/// 业务注入的对象级授权 provider。
pub type SharedObjectAuthorizer = Arc<dyn naauthz::ObjectAuthorizer>;

/// Web Ready 冻结的授权层状态。
#[derive(Clone)]
pub struct AuthorizationLayerState {
    registry: Option<SharedPolicyRegistry>,
    object_authorizer: Option<SharedObjectAuthorizer>,
    object_timeout: std::time::Duration,
}

impl AuthorizationLayerState {
    /// 业务作用：构造 route + object 授权统一边界。
    pub fn new(
        registry: Option<SharedPolicyRegistry>,
        object_authorizer: Option<SharedObjectAuthorizer>,
        object_timeout: std::time::Duration,
    ) -> Self {
        Self {
            registry,
            object_authorizer,
            // 公开低层构造器不能绕过应用 UserHook 的上限，防 Duration::MAX 进入 Tokio timeout。
            object_timeout: object_timeout.min(crate::runner::MAX_LIFECYCLE_TIMEOUT),
        }
    }
}

/// 业务作用：授权中间件。route 无策略或主体满足要求则放行;否则 403。
///
/// 主体来自请求扩展 `Principal`——未认证/无主体时视为空 scope 集,受保护 route 将被拒。
///
/// route_id 优先取路由**模板**([`axum::extract::MatchedPath`],如 `GET /users/{id}`)——含动态段的
/// 路由必须按模板写策略才可能命中;若退回原始 path(理论上仅无路由匹配时),动态段策略无法命中而
/// `decide` 未命中默认放行,会形成「想保护却静默放行」的失配脚枪,故绝不能用原始 path 匹配模板策略。
/// 静态路由两者相同,行为不变。
pub async fn authorize(
    State(state): State<AuthorizationLayerState>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|matched| matched.as_str().to_owned())
        .unwrap_or_else(|| request.uri().path().to_owned());
    let route_id = format!("{} {path}", request.method());
    let principal = request
        .extensions()
        .get::<Principal>()
        .cloned()
        .unwrap_or_default();

    let (policy_set, generation) = state
        .registry
        .as_ref()
        .map(|registry| registry.snapshot())
        .unwrap_or_else(|| (Arc::new(naauthz::PolicySet::default()), 0));
    let security = naauthz::RequestSecurityContext::new(
        principal,
        policy_set,
        generation,
        state.object_authorizer,
        state.object_timeout,
    );
    let decision = security.decide_route(&route_id);
    request.extensions_mut().insert(security);
    match decision {
        AuthzDecision::Permit => next.run(request).await,
        AuthzDecision::Deny(_reason) => ApiProblem::new(
            "about:blank",
            "Forbidden",
            StatusCode::FORBIDDEN,
            "forbidden",
        )
        .with_detail("the authenticated principal is not permitted to access this resource")
        .into_response(),
    }
}
