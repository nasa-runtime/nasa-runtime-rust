//! 声明式客户端宏(`rest-client-macro`)展开专用的 runtime helper + 第三方桥接。
//!
//! **不属于稳定业务 API**(`#[doc(hidden)]`)。宏生成的代码引用 `<root>::__private::*`:
//! - `async_trait`:桥接给生成的 `#[async_trait] impl`(业务无需直接依赖 async_trait)。
//! - `replace_path_variables` / `join_path`:把宏展开的复杂逻辑收敛到 runtime,展开代码更薄、错误集中。

// 宏生成的 trait/impl 用 `#[<root>::__private::async_trait::async_trait]`。
pub use async_trait;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};

use crate::error::{RestDiscoveryError, Result};

/// path **段**编码集:编码所有非字母数字,但保留 unreserved 的 `-._~`(RFC 3986)。
/// 故 `/`→`%2F`、空格→`%20`,而 `.`/`-` 原样保留(对齐)。
const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// 业务作用：把单个值编码成安全的 path 段(`/` 等会被转义,不会穿透成路径层级)。
///
/// # 参数
///
/// - `value`: 待作为单个 path segment 放入 URL 的原始值。
pub fn encode_path_segment(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, PATH_SEGMENT).to_string()
}

/// 业务作用：替换 path 模板里的 `{name}` 为 percent-encoded 值。缺失占位 / 未闭合 `{` → `RequestBuildFailed`。
///
/// # 参数
///
/// - `template`: 含 `{name}` 占位符的路径模板。
/// - `vars`: 占位符名称到原始值的映射，值会按 path segment 规则编码。
pub fn replace_path_variables(template: &str, vars: &[(&str, String)]) -> Result<String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .ok_or_else(|| RestDiscoveryError::RequestBuildFailed {
                reason: format!("path 模板 `{{` 未闭合:{template:?}"),
            })?;
        let name = &after[..close];
        let value = vars
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v)
            .ok_or_else(|| RestDiscoveryError::RequestBuildFailed {
                reason: format!("path 占位 `{{{name}}}` 无对应 PathVariable 参数"),
            })?;
        out.push_str(&encode_path_segment(value));
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// 业务作用：拼 `context_path` + `path`,保证只有一个 `/`。`context_path` 为空时直接用 `path`(已保证以 `/` 开头)。
///
/// # 参数
///
/// - `context_path`: 客户端级上下文路径，可为空或带尾部 `/`。
/// - `path`: 方法级路径，通常以 `/` 开头。
pub fn join_path(context_path: &str, path: &str) -> String {
    let ctx = context_path.trim_end_matches('/');
    if ctx.is_empty() {
        path.to_string()
    } else {
        format!("{ctx}{path}")
    }
}
