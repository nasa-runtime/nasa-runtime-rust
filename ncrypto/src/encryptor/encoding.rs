//! Base64 / Hex 编码(对照 原实现 `Base64` / `StringUtils.toHex`)。
//!
//! - **多数 Encryptor 函数用标准 Base64(带填充)**;仅 `base64_url_*` 用 URL-safe 无填充。
//! - **hex 大小写**:原实现 `StringUtils.toHex` 产**大写**;小写变体再 `toLowerCase`。两套 helper 都给。

use crate::{CryptoError, Result};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;

/// 标准 Base64 编码(带填充)。
pub(crate) fn b64_encode(b: &[u8]) -> String {
    STANDARD.encode(b)
}

/// 标准 Base64 解码。
pub(crate) fn b64_decode(s: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(s)
        .map_err(|e| CryptoError::decrypt(format!("base64 解码失败: {e}")))
}

/// 大写 hex(对照 原实现 `toHex`)。
pub(crate) fn hex_upper(b: &[u8]) -> String {
    hex::encode_upper(b)
}

/// 小写 hex(原实现 小写变体)。
pub(crate) fn hex_lower(b: &[u8]) -> String {
    hex::encode(b)
}

/// hex 解码(大小写均接受)。
pub(crate) fn hex_decode(s: &str) -> Result<Vec<u8>> {
    hex::decode(s).map_err(|e| CryptoError::decrypt(format!("hex 解码失败: {e}")))
}

// ==================== Base64 URL-safe(无填充,对照 原实现 base64Url*) ====================

/// Base64 URL-safe 编码(无填充),用于 JWT / URL 参数(`+`→`-`,`/`→`_`,无末尾 `=`)。
///
/// # 参数
/// - `b`: 待编码的原始字节。
pub fn base64_url_encode(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
}

/// Base64 URL-safe 编码(字符串输入,UTF-8 字节)。
///
/// # 参数
/// - `content`: 待编码的 UTF-8 文本。
pub fn base64_url_encode_str(content: &str) -> String {
    URL_SAFE_NO_PAD.encode(content.as_bytes())
}

/// Base64 URL-safe 解码(返回字节)。
///
/// # 参数
/// - `encoded`: URL-safe 且无填充的 Base64 文本。
pub fn base64_url_decode(encoded: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| CryptoError::decrypt(format!("base64url 解码失败: {e}")))
}

/// Base64 URL-safe 解码为 UTF-8 字符串。
///
/// # 参数
/// - `encoded`: URL-safe 且无填充的 Base64 文本,解码后必须是 UTF-8。
pub fn base64_url_decode_str(encoded: &str) -> Result<String> {
    let bytes = base64_url_decode(encoded)?;
    String::from_utf8(bytes).map_err(|e| CryptoError::decrypt(format!("base64url 非 UTF-8: {e}")))
}
