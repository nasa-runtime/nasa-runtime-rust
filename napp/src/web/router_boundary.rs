//! 共享 listener 中业务作用域与 Saga 保留前缀的独立分派。

use axum::{extract::Request, Router};
use tower::ServiceExt;

use crate::{ApplicationPhase, ApplicationResult};

/// 业务作用：限制手写业务子路由的挂载范围，使框架无需解析不透明 Axum Router 即可证明隔离。
/// 参数说明：`prefix` 是应用相对的静态业务前缀，不含 context path。
/// 返回：非根且规范的静态路径成功；通配、转义、空段或上级路径均拒绝登记。
pub(crate) fn validate_business_scope(prefix: &str) -> ApplicationResult<()> {
    if !prefix.starts_with('/')
        || prefix == "/"
        || prefix.ends_with('/')
        || prefix.split('/').skip(1).any(|part| {
            part.is_empty()
                || matches!(part, "." | "..")
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        return Err(super::web_error(
            ApplicationPhase::UserHook,
            "business router scope must be a canonical non-root static path",
        ));
    }
    Ok(())
}

/// 业务作用：按路径段识别完整保留前缀，避免相似业务名称被误分派到控制面。
/// 参数说明：`path` 是未经业务 middleware 改写的路径，`prefix` 是已规范化前缀。
/// 返回：前缀本身或它的子路径为真；仅字符串开头相同的相邻业务路径为假。
pub(crate) fn under_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|tail| tail.starts_with('/'))
}

/// 业务作用：在绑定 listener 前拒绝与 Saga 相交的业务路由，包含参数路径和父级通配路径。
/// 参数说明：`path` 是业务路由模式或静态作用域，`reserved` 是应用相对 Saga 前缀。
/// 返回：业务路由可能占用前缀或其内部时为真；静态路径段已经分离时为假。
pub(crate) fn intersects_reserved(path: &str, reserved: &str) -> bool {
    let mut route = path.trim_start_matches('/').split('/');
    for expected in reserved.trim_start_matches('/').split('/') {
        let Some(segment) = route.next() else {
            return false;
        };
        if segment.starts_with("{*") {
            return true;
        }
        if segment.contains('{') || segment.contains('}') {
            continue;
        }
        if segment != expected {
            return false;
        }
    }
    true
}

/// 业务作用：在进入任何业务路由或 layer 前将整个 Saga 前缀分派到专用认证链。
/// 参数说明：`business` 已冻结业务 middleware，`saga` 自带认证与未知路径处理，`prefix` 含唯一 context path。
/// 返回：共享原始请求与 listener 的独立分派器；Saga 未知路径和 method 也不会调用业务 fallback。
pub(crate) fn isolate_saga(business: Router, saga: Router, prefix: String) -> Router {
    Router::new().fallback_service(tower::service_fn(move |request: Request| {
        // 必须先按原始路径选定权限域，再进入任一子 Router；业务 layer 不得抢先消费签名原文。
        let selected = if under_prefix(request.uri().path(), &prefix) {
            saga.clone()
        } else {
            business.clone()
        };
        selected.oneshot(request)
    }))
}

/// 业务作用：以原生子路由挂载限制业务作用域，同时保留框架授权所需的完整路由模板。
/// 参数说明：`router` 是已有业务路由，`subtree` 内部路径相对于作用域，`prefix` 是已验证静态业务前缀。
/// 返回：子路由只能服务声明前缀；业务 Path<T> 仅含自身参数，框架仍能取得完整 MatchedPath。
pub(crate) fn mount_business_scope<S: Clone + Send + Sync + 'static>(
    router: Router<S>,
    subtree: Router<S>,
    prefix: &str,
) -> Router<S> {
    // 由 Axum 合并路由图，确保全局授权和幂等门禁在调用业务 handler 前已取得真实路由模板。
    router.nest(prefix, subtree)
}
