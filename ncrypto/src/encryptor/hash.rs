//! 哈希(对照 原实现 `bcrypt`/`md5`/`sha*`)。
//!
//! hex 大小写:`sha256` **默认大写**;`md5`(原框架 `md5DigestAsHex`=小写)/`sha1`/`sha384`/`sha512` 小写。

use super::{hex_lower, hex_upper};
use crate::{CryptoError, Result};
use md5::Md5;
use rand::{rngs::OsRng, RngCore};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};

// ==================== BCrypt ====================

/// BCrypt 加密(密码哈希,OsRng 随机盐)。cost=10 + **`$2a$` 版本前缀**对齐 原实现 原框架 `BCrypt.gensalt()`
/// (安全说明.3:bcrypt crate 默认产 `$2b$`,而 原框架/jBCrypt 用 `$2a$`;统一 `$2a$` 保 Rust 生成的 hash
/// 能被 原实现 侧 `checkpw` 验证。≤72 字节口令下 `$2a`/`$2b` 算法等价,仅前缀字面不同)。
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

/// 验证密码是否匹配 BCrypt 哈希(hash 自带 cost/盐,跨语言可互验)。出错即 false。
///
/// # 参数
/// - `content`: 待验证的口令文本。
/// - `hash`: 已存储的 BCrypt 哈希串,其中包含版本、cost 和盐。
pub fn bcrypt_check(content: &str, hash: &str) -> bool {
    bcrypt::verify(content, hash).unwrap_or(false)
}

// ==================== MD5 / SHA ====================

/// MD5 摘要(**小写** hex,32 字符;对照 原框架 `md5DigestAsHex`)。仅遗留兼容。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn md5(content: &str) -> String {
    hex_lower(&Md5::digest(content.as_bytes()))
}

/// SHA-256(**大写** hex,默认;对照 原实现 `sha256(content)`)。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha256(content: &str) -> String {
    hex_upper(&Sha256::digest(content.as_bytes()))
}

/// SHA-256,`upper=true` 大写 / `false` 小写 hex。
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

/// SHA-1(小写 hex,40 字符)。已有碰撞,仅遗留兼容。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha1(content: &str) -> String {
    hex_lower(&Sha1::digest(content.as_bytes()))
}

/// SHA-384(小写 hex,96 字符)。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha384(content: &str) -> String {
    hex_lower(&Sha384::digest(content.as_bytes()))
}

/// SHA-512(小写 hex,128 字符)。
///
/// # 参数
/// - `content`: 要计算摘要的 UTF-8 文本。
pub fn sha512(content: &str) -> String {
    hex_lower(&Sha512::digest(content.as_bytes()))
}
