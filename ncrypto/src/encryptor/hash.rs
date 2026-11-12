//! BCrypt、MD5 与 SHA 系列哈希兼容入口。
//!
//! hex 大小写合同：`sha256` 默认大写；`md5`/`sha1`/`sha384`/`sha512` 小写。

use super::{hex_lower, hex_upper};
use crate::{CryptoError, Result};
use md5::Md5;
use rand::{rngs::OsRng, RngCore};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};

// ==================== BCrypt ====================

/// 业务作用：使用随机盐和固定 cost 生成 BCrypt 口令哈希。
///
/// 参数说明：
/// - `content`：需要哈希的口令文本，长度等处理由 BCrypt 库执行。
///
/// 返回：返回 cost 为 10 且前缀为 $2a$ 的哈希文本；哈希计算返回的错误映射为加密错误，不输出明文。
pub fn bcrypt(content: &str) -> Result<String> {
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    bcrypt::hash_with_salt(content, 10, salt)
        .map(|parts| parts.format_for_version(bcrypt::Version::TwoA))
        .map_err(|e| CryptoError::encrypt(format!("bcrypt: {e}")))
}

/// 业务作用：依据已存哈希中的参数校验口令。
///
/// 参数说明：
/// - `content`：待验证的口令文本。
/// - `hash`：包含版本、cost 和盐的 BCrypt 哈希。
///
/// 返回：匹配返回 true；不匹配或哈希解析、验证失败统一返回 false。
pub fn bcrypt_check(content: &str, hash: &str) -> bool {
    bcrypt::verify(content, hash).unwrap_or(false)
}

// ==================== MD5 / SHA ====================

/// 业务作用：为既有数据格式计算 UTF-8 内容的 MD5 摘要。
///
/// 参数说明：
/// - `content`：需要计算兼容摘要的原始 UTF-8 文本。
///
/// 返回：返回 32 字符小写十六进制摘要，不添加盐或密钥；本入口只提供既有摘要格式兼容。
pub fn md5(content: &str) -> String {
    hex_lower(&Md5::digest(content.as_bytes()))
}

/// 业务作用：计算 UTF-8 内容的 SHA-256 摘要。
///
/// 参数说明：
/// - `content`：未做字符归一化的原始 UTF-8 文本。
///
/// 返回：返回 64 字符的大写十六进制摘要，不添加盐或密钥。
pub fn sha256(content: &str) -> String {
    hex_upper(&Sha256::digest(content.as_bytes()))
}

/// 业务作用：计算 SHA-256 并选择十六进制字母大小写。
///
/// 参数说明：
/// - `content`：要计算摘要的 UTF-8 原始文本。
/// - `upper`：true 输出大写，false 输出小写。
///
/// 返回：返回 64 字符十六进制摘要，不添加盐或密钥。
pub fn sha256_cased(content: &str, upper: bool) -> String {
    let h = Sha256::digest(content.as_bytes());
    if upper {
        hex_upper(&h)
    } else {
        hex_lower(&h)
    }
}

/// 业务作用：为既有数据格式计算 UTF-8 内容的 SHA-1 摘要。
///
/// 参数说明：
/// - `content`：需要计算兼容摘要的原始 UTF-8 文本。
///
/// 返回：返回 40 字符小写十六进制摘要，不添加盐或密钥；本入口只提供既有摘要格式兼容。
pub fn sha1(content: &str) -> String {
    hex_lower(&Sha1::digest(content.as_bytes()))
}

/// 业务作用：计算 UTF-8 内容的 SHA-384 摘要。
///
/// 参数说明：
/// - `content`：未做字符归一化的原始 UTF-8 文本。
///
/// 返回：返回 96 字符的小写十六进制摘要，不添加盐或密钥。
pub fn sha384(content: &str) -> String {
    hex_lower(&Sha384::digest(content.as_bytes()))
}

/// 业务作用：计算 UTF-8 内容的 SHA-512 摘要。
///
/// 参数说明：
/// - `content`：未做字符归一化的原始 UTF-8 文本。
///
/// 返回：返回 128 字符的小写十六进制摘要，不添加盐或密钥。
pub fn sha512(content: &str) -> String {
    hex_lower(&Sha512::digest(content.as_bytes()))
}
