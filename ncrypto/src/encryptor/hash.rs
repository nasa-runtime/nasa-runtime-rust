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

/// 业务作用: BCrypt 加密(密码哈希,OsRng 随机盐)。cost=10，并固定使用可与存量 jBCrypt 数据互验的
/// `$2a$` 版本前缀；≤72 字节口令下 `$2a`/`$2b` 算法等价，仅前缀字面不同。
///
/// # 参数
/// - `content`: 待哈希的口令文本。
pub fn bcrypt(content: &str) -> Result<String> {
    let mut salt = [0u8; 16];
    OsRng.fill_bytes(&mut salt);
    bcrypt::hash_with_salt(content, 10, salt)
        .map(|parts| parts.format_for_version(bcrypt::Version::TwoA))
        .map_err(|e| CryptoError::encrypt(format!("bcrypt: {e}")))
}

/// 业务作用: 验证密码是否匹配 BCrypt 哈希(hash 自带 cost/盐,跨语言可互验)。出错即 false。
///
/// # 参数
/// - `content`: 待验证的口令文本。
/// - `hash`: 已存储的 BCrypt 哈希串,其中包含版本、cost 和盐。
pub fn bcrypt_check(content: &str, hash: &str) -> bool {
    bcrypt::verify(content, hash).unwrap_or(false)
}

// ==================== MD5 / SHA ====================

/// 业务作用: MD5 摘要(**小写** hex,32 字符)。仅用于遗留数据兼容。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
///
/// # 返回
/// 返回 32 字符的小写十六进制 MD5 摘要；该入口不执行随机化或盐处理。
pub fn md5(content: &str) -> String {
    hex_lower(&Md5::digest(content.as_bytes()))
}

/// 业务作用: SHA-256(**大写** hex,默认;对照 原实现 `sha256(content)`)。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha256(content: &str) -> String {
    hex_upper(&Sha256::digest(content.as_bytes()))
}

/// 业务作用: SHA-256,`upper=true` 大写 / `false` 小写 hex。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
/// - `upper`: `true` 返回大写 HEX,`false` 返回小写 HEX。
pub fn sha256_cased(content: &str, upper: bool) -> String {
    let h = Sha256::digest(content.as_bytes());
    if upper {
        hex_upper(&h)
    } else {
        hex_lower(&h)
    }
}

/// 业务作用: SHA-1(小写 hex,40 字符)。已有碰撞,仅遗留兼容。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha1(content: &str) -> String {
    hex_lower(&Sha1::digest(content.as_bytes()))
}

/// 业务作用: SHA-384(小写 hex,96 字符)。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha384(content: &str) -> String {
    hex_lower(&Sha384::digest(content.as_bytes()))
}

/// 业务作用: SHA-512(小写 hex,128 字符)。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha512(content: &str) -> String {
    hex_lower(&Sha512::digest(content.as_bytes()))
}
