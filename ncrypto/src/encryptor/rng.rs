//! 随机盐与密钥生成。
//!
//! 随机材料全部来自操作系统的 OsRng，每次生成独立字节，不依赖可预测的业务伪随机序列。

use super::{b64_encode, hex_lower};
use crate::{CryptoError, Result};
use rand::rngs::OsRng;
use rand::{Rng, RngCore};

/// 业务作用: 生成随机盐(小写 hex)。`byte_len` 推荐 16 或 32。
///
/// # 参数
/// - `byte_len`: 随机盐的原始字节长度;返回值会变成两倍长度的小写 HEX。
pub fn generate_salt(byte_len: usize) -> String {
    let mut salt = vec![0u8; byte_len];
    OsRng.fill_bytes(&mut salt);
    hex_lower(&salt)
}

/// 业务作用：生成指定强度的随机 AES 密钥并编码为 Base64。
/// 返回：128、192 或 256 位密钥；其它位数返回错误，不截断为另一种强度。
///
/// # 参数
/// - `bits`: AES 密钥位数,只接受 128、192 或 256。
pub fn generate_aes_key(bits: usize) -> Result<String> {
    let len = match bits {
        128 => 16,
        192 => 24,
        256 => 32,
        _ => return Err(CryptoError::config("AES key bitLength 必须是 128/192/256")),
    };
    let mut key = vec![0u8; len];
    OsRng.fill_bytes(&mut key);
    Ok(b64_encode(&key))
}

/// 业务作用: 生成 16 字节随机 AES-128 密钥(小写 hex,32 字符,可直接作 `encrypt_aes` 的 key 参数)。
pub fn generate_aes_key_hex() -> String {
    let mut key = [0u8; 16];
    OsRng.fill_bytes(&mut key);
    hex_lower(&key)
}

/// 业务作用: 生成 `len` 个 `[0-9a-zA-Z]` 随机字符。
/// Web RSA_AES 策略的临时 AES key 用它(`random_ascii(16)` = 16 字节 ASCII = 合法 AES-128 key)。
///
/// # 参数
/// - `len`: 要生成的 ASCII 字符个数。
pub fn random_ascii(len: usize) -> String {
    const CHARS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut rng = OsRng;
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}
