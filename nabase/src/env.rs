//! 环境变量读取工具。

use crate::strings;

/// 业务作用：把配置 key 转换成宽松环境变量名。
///
/// 转换规则:`.` 和 `-` 转 `_`,其余字符保留后整体转大写。
///
/// 参数说明:
/// - `key`: 配置路径或环境变量名候选值，例如 `app.redis-url`。
///
/// 返回: 将分隔符统一为下划线并转换为大写后的环境变量名。
pub fn relaxed_env_key(key: &str) -> String {
    key.chars()
        .map(|c| if c == '.' || c == '-' { '_' } else { c })
        .collect::<String>()
        .to_ascii_uppercase()
}

/// 业务作用：读取环境变量,先按原始 key 查找,再按宽松 key 查找。
///
/// 命中的空白值会被视为未命中,方便启动配置 fail-fast。
///
/// 参数说明:
/// - `key`: 调用方声明的配置键；会先按原值读取，再按 [`relaxed_env_key`] 的结果读取。
///
/// 返回: 任一键命中非空白值时返回清洗后的文本，否则返回 `None`；读取过程不修改环境变量。
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

/// 业务作用：读取环境变量,未命中时返回指定默认值。
///
/// 参数说明:
/// - `key`: 调用方声明的配置键；读取规则与 [`var_relaxed`] 相同。
/// - `default`: 原始键和宽松键都未命中时返回的默认值。
///
/// 返回: 环境变量命中的非空白文本，未命中时返回调用方提供的默认值。
pub fn var_relaxed_or(key: &str, default: impl Into<String>) -> String {
    var_relaxed(key).unwrap_or_else(|| default.into())
}
