//! 随机盐与密钥生成。
//!
//! 随机材料全部来自操作系统的 OsRng，每次生成独立字节，不依赖可预测的业务伪随机序列。

use super::{b64_encode, hex_lower};
use crate::{CryptoError, Result};
use rand::rngs::OsRng;
use rand::{Rng, RngCore};

/// 业务作用：从操作系统随机源生成给定字节长度的盐。
///
/// 参数说明：
/// - `byte_len`：原始随机字节数，由调用方限制分配规模。
///
/// 返回：返回两倍字节数的小写 HEX 文本，零长度返回空串；此入口不返回随机源或分配错误。
pub fn generate_salt(byte_len: usize) -> String {
    let mut salt = vec![0u8; byte_len];
    OsRng.fill_bytes(&mut salt);
    hex_lower(&salt)
}

/// 业务作用：生成指定强度的随机 AES 密钥并编码为 Base64。
///
/// 返回：128、192 或 256 位密钥；其它位数返回错误，不截断为另一种强度。
///
/// 参数说明：
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

/// 业务作用：生成 16 个随机字节并输出十六进制密钥材料。
///
/// 参数说明: 无。
///
/// 返回：返回 32 字符小写 HEX。按 HEX 解码后是 16 字节；若直接传给按 UTF-8 取密钥的 AES 入口，则实际使用 32 字节文本密钥，不能把两种表示视为同一密钥。
pub fn generate_aes_key_hex() -> String {
    let mut key = [0u8; 16];
    OsRng.fill_bytes(&mut key);
    hex_lower(&key)
}

/// 业务作用：从字母数字集合独立采样固定长度的随机 ASCII 文本。
///
/// 参数说明：
/// - `len`：输出字符数，也等于输出 UTF-8 字节数。
///
/// 返回：返回仅含数字及大小写英文字母的文本，零长度返回空串；调用方限制长度，本入口不返回随机源错误。
pub fn random_ascii(len: usize) -> String {
    const CHARS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut rng = OsRng;
    (0..len)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}
