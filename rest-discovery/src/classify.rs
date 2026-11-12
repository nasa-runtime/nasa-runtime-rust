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

/// 业务作用：按 URL 语法区分显式负载均衡请求和普通 HTTP 请求。
///
/// 参数说明：
/// - `raw`：待解析的原始 URL 文本。
///
/// 返回：lb 返回通过服务名检查的逻辑服务与路径，HTTP/HTTPS 保留原 URL 和主机供后续判定；非法 URL、缺 host 或其它 scheme 返回 InvalidUrl。不查询服务名索引或实例。
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

/// 业务作用：提取请求目标中的路径和非空查询串。
///
/// 参数说明：
/// - `u`：已经解析的 URL。
///
/// 返回：返回以默认根路径补空的路径及可选查询串，不包含 fragment，保留 URL 库已经编码的文本。
fn path_and_query(u: &Url) -> String {
    let path = u.path();
    let path = if path.is_empty() {
        "/"
    } else {
        path
    };
    match u.query() {
        Some(q) if !q.is_empty() => format!("{path}?{q}"),
        _ => path.to_string(),
    }
}

/// 业务作用：拒绝会破坏服务名在逻辑 URL 中表达的空值和分隔字符。
///
/// 参数说明：
/// - `service`：待使用的逻辑服务名。
///
/// 返回：名称非空且不含空白、斜杠、问号或井号时成功，否则返回原因文本；不检查长度或后端是否存在。
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

/// 业务作用：检查内部服务调用提供的是绝对逻辑路径。
///
/// 参数说明：
/// - `path`：调用方提供的路径及可选查询串。
///
/// 返回：以斜杠开头且不含 :// 时成功，否则返回原因；不做点段消解、百分号解码或完整 URI 安全校验。
pub(crate) fn validate_logical_path(path: &str) -> std::result::Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("path 必须以 `/` 开头,当前 {path:?}"));
    }
    if path.contains("://") {
        return Err(format!("path 必须是逻辑路径,不能是完整 URL,当前 {path:?}"));
    }
    Ok(())
}
