//! PBKDF2 密码派生。

use super::hex_lower;
use crate::{CryptoError, Result};
use sha2::Sha256;

/// 业务作用：按 PBKDF2-HMAC-SHA256 从口令与盐派生密钥，输出小写十六进制。
/// 口令和盐均按 UTF-8 输入；跨语言互通必须使用相同字节、轮数与长度，不能只比较显示文本。
/// 返回：`key_bits / 8` 字节密钥的编码；轮数或位数为零时返回错误。非八倍数位数向下取整。
///
/// # 参数
/// - `password`: 派生密钥使用的口令文本,当前实现按 UTF-8 字节输入 PBKDF2。
/// - `salt`: 派生密钥使用的盐文本,当前实现按 UTF-8 字节输入 PBKDF2。
/// - `iterations`: PBKDF2 迭代次数,必须大于 0。
/// - `key_bits`: 目标密钥位数,必须大于 0;非 8 倍数按 `/8` 取整到字节数。
pub fn pbkdf2(password: &str, salt: &str, iterations: u32, key_bits: u32) -> Result<String> {
    if iterations == 0 {
        return Err(CryptoError::config("PBKDF2 iterations 必须 > 0"));
    }
    if key_bits == 0 {
        return Err(CryptoError::config("PBKDF2 key_bits 必须 > 0"));
    }
    let mut out = vec![0u8; (key_bits / 8) as usize];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt.as_bytes(), iterations, &mut out);
    Ok(hex_lower(&out))
}

/// 业务作用: PBKDF2 默认(100000 次迭代,256 位密钥)。
///
/// # 参数
/// - `password`: 派生密钥使用的口令文本。
/// - `salt`: 派生密钥使用的盐文本。
pub fn pbkdf2_default(password: &str, salt: &str) -> Result<String> {
    pbkdf2(password, salt, 100_000, 256)
}
