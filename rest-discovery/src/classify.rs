//! 三档识别:静态直连、服务名经注册中心、非法目标显式拒绝。
//!
//! 本模块只处理「字符串 URL 入口」(`get/post/.../request(method, url)`)。档1 `service_request(service,...)`
//! 由 client 直接带 service 标记,不经此处。
//!
//! ```text
//! 档2 lb://service/path?query  → 显式内部直连(不查索引、不外部 fallback)
//! 档3 http(s)://host/path      → 归为外部 Http;是否走内部 LB 由 client 按 heuristic_http + 服务名索引决定
//!                                (命中走内部 LB,未命中按 unknown_host)
//! 其它 scheme                  → InvalidUrl
//! ```

use url::Url;

use crate::error::{RestDiscoveryError, Result};

/// 字符串 URL 入口归类结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Classified {
    /// 档2:`lb://service/path?query`。`path_and_query` 形如 `/a/b?x=1`(已含前导 `/`)。
    Lb {
        service: String,
        path_and_query: String,
    },
    /// 档3:裸 `http(s)`。heuristic 关 → 外部直连;heuristic 开 → client 用 `host` 查服务名索引决定内部/外部。
    Http {
        /// 原始 URL 字符串(外部直连时直接用)。
        original: String,
        /// 原始 scheme(`http`/`https`;命中索引走内部时按 scheme_policy 默认保留)。
        scheme: String,
        /// host(未归一化;索引 lookup 时按 ServiceMatchMode 归一化)。
        host: String,
        /// `/path?query`(命中索引走内部时重写用)。
        path_and_query: String,
    },
}

/// 业务作用：解析并归类一个字符串 URL。
///
/// 只做语法归类:`http(s)` 一律返回 `Classified::Http`;是否命中服务名索引并走内部 LB,
/// 由 `client.resolve_plan` 按 `heuristic_http` 与 `ServiceNameIndex` 决定(本函数不查索引)。
pub(crate) fn classify(raw: &str) -> Result<Classified> {
    let parsed = Url::parse(raw).map_err(|e| RestDiscoveryError::InvalidUrl {
        url: raw.to_string(),
        reason: e.to_string(),
    })?;

    match parsed.scheme() {
        "lb" => {
            let host = parsed.host_str().filter(|h| !h.is_empty()).ok_or_else(|| {
                RestDiscoveryError::InvalidUrl {
                    url: raw.to_string(),
                    reason: "lb:// 缺少服务名(host)".to_string(),
                }
            })?;
            validate_service(host).map_err(|reason| RestDiscoveryError::InvalidUrl {
                url: raw.to_string(),
                reason,
            })?;
            Ok(Classified::Lb {
                service: host.to_string(),
                path_and_query: path_and_query(&parsed),
            })
        }
        "http" | "https" => {
            let host = parsed.host_str().filter(|h| !h.is_empty()).ok_or_else(|| {
                RestDiscoveryError::InvalidUrl {
                    url: raw.to_string(),
                    reason: "http(s):// 缺少 host".to_string(),
                }
            })?;
            Ok(Classified::Http {
                original: raw.to_string(),
                scheme: parsed.scheme().to_string(),
                host: host.to_string(),
                path_and_query: path_and_query(&parsed),
            })
        }
        other => Err(RestDiscoveryError::InvalidUrl {
            url: raw.to_string(),
            reason: format!("不支持的 scheme `{other}`(只支持 http/https/lb)"),
        }),
    }
}

/// 业务作用：从已解析 URL 取 `/path?query`(无 query 则仅 `/path`;空 path 归一为 `/`)。fragment 不带进 HTTP 请求。
///
/// # 参数
/// - `u`: 需要分类的请求 URL。
fn path_and_query(u: &Url) -> String {
    let path = u.path();
    let path = if path.is_empty() { "/" } else { path };
    match u.query() {
        Some(q) if !q.is_empty() => format!("{path}?{q}"),
        _ => path.to_string(),
    }
}

/// 业务作用：校验注册中心服务名:非空、不含 `/ ? #` 或空白。返回 `Err(reason)`。
pub(crate) fn validate_service(service: &str) -> std::result::Result<(), String> {
    if service.is_empty() {
        return Err("service 不能为空".to_string());
    }
    if let Some(c) = service
        .chars()
        .find(|c| c.is_whitespace() || matches!(c, '/' | '?' | '#'))
    {
        return Err(format!("service 不能包含空白或 `/?#`(发现 {c:?})"));
    }
    Ok(())
}

/// 业务作用：校验 `service_request` 的逻辑 path:必须以 `/` 开头,且不是完整 URL。返回 `Err(reason)`。
pub(crate) fn validate_logical_path(path: &str) -> std::result::Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("path 必须以 `/` 开头,当前 {path:?}"));
    }
    if path.contains("://") {
        return Err(format!("path 必须是逻辑路径,不能是完整 URL,当前 {path:?}"));
    }
    Ok(())
}
