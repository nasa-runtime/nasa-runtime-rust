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

/// 业务作用：按路径参数字符集编码一个 UTF-8 值。
///
/// 参数说明：
/// - `value`：将被替换进单个路径段的原始文本。
///
/// 返回：转义字母数字及 -._~ 之外的字节；斜杠被编码，点号原样保留。本函数不拒绝单独的点或双点段，不保证后续 URL 解析保持所有路径层级。
pub fn encode_path_segment(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, PATH_SEGMENT).to_string()
}

/// 业务作用：按占位名查找参数并以路径段编码替换模板。
///
/// 参数说明：
/// - `template`：含花括号占位符的路径模板。
/// - `vars`：有序名称与原始值列表，同名使用首项。
///
/// 返回：成功返回替换后的路径；未闭合左括号或占位名缺失返回 RequestBuildFailed。多余参数和未配对的右括号不在此拒绝，不校验最终路径。
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

/// 业务作用：将去除尾斜杠的上下文路径与方法路径拼接。
///
/// 参数说明：
/// - `context_path`：可为空的客户端上下文路径。
/// - `path`：调用方已保证以斜杠开头的方法路径。
///
/// 返回：上下文去尾斜杠后为空则原样返回 path，否则直接拼接；不额外补斜杠或规范化点段。
pub fn join_path(context_path: &str, path: &str) -> String {
    let ctx = context_path.trim_end_matches('/');
    if ctx.is_empty() {
        path.to_string()
    } else {
        format!("{ctx}{path}")
    }
}
