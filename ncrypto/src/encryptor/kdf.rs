//! PBKDF2 密码派生(对照 原实现 `pbkdf2`,`PBKDF2WithHmacSHA256`,小写 hex)。

use super::hex_lower;
use crate::{CryptoError, Result};
use sha2::Sha256;

/// PBKDF2-HMAC-SHA256 派生密钥(**小写** hex)。
/// `iterations` 推荐 >= 100000;`key_bits` 如 128/256。salt 取其 UTF-8 字节(对照 原实现)。
///
/// **`iterations==0` / `key_bits==0` fail-fast**(对照 原实现 `PBEKeySpec` 抛
/// `IllegalArgumentException`)——`iterations=0` 此前静默退化成 1 轮(严重削弱派生强度)、`key_bits=0`
/// 此前返回空串(把配置错误伪装成合法输出),违反 crate"可失败 API 返 Result、不静默"纪律,故改返 `Result`。
/// 注:`key_bits` 非 8 倍数沿用 `/8` floor(JDK `PBEKeySpec(...,129)` 实测也返回 16 字节,**不**拒绝)。
///
/// **跨语言 caveat(安全说明.2)**:口令取 `password.as_bytes()`(UTF-8 编码)。
/// **ASCII 口令已 KAT 实证与公认向量逐字节一致**;但 原实现 SunJCE `PBKDF2WithHmacSHA256`
/// 内部把 char[] 口令按其自身规则编码,**非 ASCII 口令(中文/重音符)的字节序列可能与 UTF-8 不同**,
/// 此时 Rust 与 原实现 派生结果会发散。若需用非 ASCII 口令跨语言互通,务必先对 原实现 golden 验证。
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

/// PBKDF2 默认(100000 次迭代,256 位密钥)。
///
/// # 参数
/// - `password`: 派生密钥使用的口令文本。
/// - `salt`: 派生密钥使用的盐文本。
pub fn pbkdf2_default(password: &str, salt: &str) -> Result<String> {
    pbkdf2(password, salt, 100_000, 256)
}
