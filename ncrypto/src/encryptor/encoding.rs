//! Base64 / Hex 编码。
//!
//! - **多数 Encryptor 函数用标准 Base64(带填充)**;仅 `base64_url_*` 用 URL-safe 无填充。
//! - hex 编码分别提供大写与小写入口，调用方按协议选择固定表示。

use crate::{CryptoError, Result};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;

/// 业务作用：把原始字节编码为带填充的标准 Base64。
///
/// 参数说明：
/// - `b`：待编码原始字节。
///
/// 返回：返回标准字母表的编码文本，空输入返回空串。
pub(crate) fn b64_encode(b: &[u8]) -> String {
    STANDARD.encode(b)
}

/// 业务作用：把标准 Base64 文本还原为字节。
///
/// 参数说明：
/// - `s`：符合标准字母表和填充规则的文本。
///
/// 返回：成功返回解码字节，非法字符、长度或填充返回解码错误。
pub(crate) fn b64_decode(s: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(s)
        .map_err(|e| CryptoError::decrypt(format!("base64 解码失败: {e}")))
}

/// 业务作用：把原始字节编码为大写十六进制文本。
///
/// 参数说明：
/// - `b`：待编码字节。
///
/// 返回：每字节输出两字符，无分隔符，空输入返回空串。
pub(crate) fn hex_upper(b: &[u8]) -> String {
    hex::encode_upper(b)
}

/// 业务作用：把原始字节编码为小写十六进制文本。
///
/// 参数说明：
/// - `b`：待编码字节。
///
/// 返回：每字节输出两字符，无分隔符，空输入返回空串。
pub(crate) fn hex_lower(b: &[u8]) -> String {
    hex::encode(b)
}

/// 业务作用：把大小写均可的十六进制文本解码为字节。
///
/// 参数说明：
/// - `s`：长度须为偶数且只含十六进制字符的文本。
///
/// 返回：成功返回字节，奇数长度或非法字符返回解码错误。
pub(crate) fn hex_decode(s: &str) -> Result<Vec<u8>> {
    hex::decode(s).map_err(|e| CryptoError::decrypt(format!("hex 解码失败: {e}")))
}

// ==================== Base64 URL-safe(无填充) ====================

/// 业务作用：把字节编码为无填充的 URL-safe Base64。
///
/// 参数说明：
/// - `b`：待编码字节。
///
/// 返回：返回使用连字符和下划线字母表、无末尾等号的文本，空输入返回空串。
pub fn base64_url_encode(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
}

/// 业务作用：将 UTF-8 文本字节编码为无填充的 URL-safe Base64。
///
/// 参数说明：
/// - `content`：待编码的原始文本，不进行字符归一化。
///
/// 返回：返回 URL-safe Base64 文本，无末尾填充。
pub fn base64_url_encode_str(content: &str) -> String {
    URL_SAFE_NO_PAD.encode(content.as_bytes())
}

/// 业务作用：解码无填充的 URL-safe Base64 文本。
///
/// 参数说明：
/// - `encoded`：采用 URL-safe 字母表和无填充规则的文本。
///
/// 返回：成功返回原始字节，非法编码返回解码错误。
pub fn base64_url_decode(encoded: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| CryptoError::decrypt(format!("base64url 解码失败: {e}")))
}

/// 业务作用：解码 URL-safe Base64 并验证其 UTF-8 内容。
///
/// 参数说明：
/// - `encoded`：无填充的 URL-safe Base64 文本。
///
/// 返回：成功返回 UTF-8 文本，非法编码或解码字节非 UTF-8 时返回错误。
pub fn base64_url_decode_str(encoded: &str) -> Result<String> {
    let bytes = base64_url_decode(encoded)?;
    String::from_utf8(bytes).map_err(|e| CryptoError::decrypt(format!("base64url 非 UTF-8: {e}")))
}
