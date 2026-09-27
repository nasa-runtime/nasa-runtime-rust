//! Job 名称与命名空间校验：只允许可稳定进入 key、field 与标识摘要的短 ASCII 名称。

use crate::error::{NasaRedisError, Result};

/// 名称最大字节数；首字符外的部分允许有界 ASCII 标识字符。
const MAX_NAME_BYTES: usize = 128;

/// 业务作用：校验一个 Job 名称或命名空间，去除首尾空白后必须满足固定 ASCII 名称合同。
///
/// 参数说明：
/// - `value`: 待校验的原始名称。
/// - `field`: 出错时用于定位的字段名。
///
/// 返回：合法时返回去空白后的名称；空、超长或含非法字符时返回配置错误，避免非法字节进入 key 与标识。
pub(crate) fn require_name(value: &str, field: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_NAME_BYTES {
        return Err(err(field, "长度必须为 1..=128 字节"));
    }
    let mut bytes = trimmed.bytes();
    let first = bytes.next().expect("非空已在上文保证");
    // 首字符必须是字母或数字：与既有名称合同一致，避免前导分隔符破坏 key 解析。
    if !first.is_ascii_alphanumeric() {
        return Err(err(field, "首字符必须是 ASCII 字母或数字"));
    }
    if !bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-')) {
        return Err(err(field, "只能包含 ASCII 字母数字与 '.' '_' ':' '-'"));
    }
    Ok(trimmed.to_owned())
}

/// 业务作用：构造带字段定位的 Job 名称配置错误。
///
/// 参数说明：
/// - `field`: 非法字段名。
/// - `reason`: 稳定原因摘要。
///
/// 返回：`JobError::Config` 错误。
fn err(field: &str, reason: &str) -> NasaRedisError {
    crate::job::JobError::Config(format!("{field} {reason}")).into()
}
