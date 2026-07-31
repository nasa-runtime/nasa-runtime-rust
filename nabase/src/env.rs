//! 环境变量读取工具。

use crate::strings;

/// 把配置 key 转换成宽松环境变量名。
///
/// 转换规则:`.` 和 `-` 转 `_`,其余字符保留后整体转大写。
///
/// # 参数
///
/// - `key`: 配置路径或环境变量名候选值，例如 `app.redis-url`。
pub fn relaxed_env_key(key: &str) -> String {
    key.chars()
        .map(|c| if c == '.' || c == '-' { '_' } else { c })
        .collect::<String>()
        .to_ascii_uppercase()
}

/// 读取环境变量,先按原始 key 查找,再按宽松 key 查找。
///
/// 命中的空白值会被视为未命中,方便启动配置 fail-fast。
///
/// # 参数
///
/// - `key`: 调用方声明的配置键；会先按原值读取，再按 [`relaxed_env_key`] 的结果读取。
pub fn var_relaxed(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .and_then(strings::non_blank)
        .or_else(|| {
            let relaxed = relaxed_env_key(key);
            if relaxed == key {
                None
            } else {
                std::env::var(relaxed).ok().and_then(strings::non_blank)
            }
        })
}

/// 读取环境变量,未命中时返回指定默认值。
///
/// # 参数
///
/// - `key`: 调用方声明的配置键；读取规则与 [`var_relaxed`] 相同。
/// - `default`: 原始键和宽松键都未命中时返回的默认值。
pub fn var_relaxed_or(key: &str, default: impl Into<String>) -> String {
    var_relaxed(key).unwrap_or_else(|| default.into())
}
