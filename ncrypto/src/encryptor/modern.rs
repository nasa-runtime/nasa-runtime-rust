//! 现代默认加密入口。
//!
//! 本模块服务新业务机密性边界，不追求与历史加密工具逐字节互通。默认写入格式使用
//! Argon2id + AES-256-GCM；既有 NC1 密文保持只读兼容。旧的 AES-ECB/CBC/RSA PKCS#1 v1.5
//! 入口继续由原模块承担，本模块不会把弱算法自动降级成现代协议的回退路径。

use super::{base64_url_decode, base64_url_encode};
use crate::{CryptoError, Result};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};
use rand::RngCore;
use sha2::Sha256;
use zeroize::Zeroizing;

/// 现代默认 token 的版本前缀；格式为 `NC2.m_cost.t_cost.p_cost.salt.nonce.ciphertext`。
pub const MODERN_TOKEN_PREFIX: &str = "NC2";

/// 既有现代 token 的只读兼容前缀。
pub const MODERN_TOKEN_V1_PREFIX: &str = "NC1";

/// NC1 使用的 PBKDF2-HMAC-SHA256 默认迭代次数。
pub const MODERN_PBKDF2_ITERATIONS: u32 = 210_000;

/// NC1 解密接受的最低 PBKDF2 迭代次数，防止参数被降级后放大请求吞吐。
pub const MODERN_MIN_PBKDF2_ITERATIONS: u32 = 100_000;

/// NC1 解密接受的最高 PBKDF2 迭代次数，限制不可信 token 能触发的 CPU 成本。
pub const MODERN_MAX_PBKDF2_ITERATIONS: u32 = 10_000_000;

/// NC2 默认 Argon2id 内存成本，单位 KiB。
pub const MODERN_ARGON2_MEMORY_KIB: u32 = 65_536;

/// NC2 默认 Argon2id 时间成本。
pub const MODERN_ARGON2_ITERATIONS: u32 = 3;

/// NC2 默认 Argon2id 并行度。
pub const MODERN_ARGON2_PARALLELISM: u32 = 4;

/// NC2 解密接受的最低 Argon2id 内存成本，单位 KiB。
pub const MODERN_MIN_ARGON2_MEMORY_KIB: u32 = 65_536;

/// NC2 解密接受的最高 Argon2id 内存成本，单位 KiB。
pub const MODERN_MAX_ARGON2_MEMORY_KIB: u32 = 131_072;

/// NC2 解密接受的最低 Argon2id 时间成本。
pub const MODERN_MIN_ARGON2_ITERATIONS: u32 = 3;

/// NC2 解密接受的最高 Argon2id 时间成本。
pub const MODERN_MAX_ARGON2_ITERATIONS: u32 = 6;

/// NC2 解密接受的最低 Argon2id 并行度。
pub const MODERN_MIN_ARGON2_PARALLELISM: u32 = 1;

/// NC2 解密接受的最高 Argon2id 并行度。
pub const MODERN_MAX_ARGON2_PARALLELISM: u32 = 8;

/// 现代 token 使用的随机 salt 长度，单位字节。
pub const MODERN_SALT_LEN: usize = 16;

/// AES-GCM nonce 长度，单位字节。
pub const MODERN_NONCE_LEN: usize = 12;

/// 单次现代加密接受的最大明文长度，单位字节。
pub const MODERN_MAX_PLAINTEXT_LEN: usize = 16 * 1024 * 1024;

/// 单次现代加解密接受的最大业务 AAD 长度，单位字节。
pub const MODERN_MAX_AAD_LEN: usize = 64 * 1024;

/// 现代加解密接受的最大口令长度，单位字节。
pub const MODERN_MAX_PASSWORD_LEN: usize = 1024;

/// 现代解密接受的最大 token 长度，单位字节。
pub const MODERN_MAX_TOKEN_LEN: usize = 24 * 1024 * 1024;

const NC1_TOKEN_PARTS: usize = 5;
const NC2_TOKEN_PARTS: usize = 7;
const AES_GCM_TAG_LEN: usize = 16;
const SALT_BASE64_LEN: usize = 22;
const NONCE_BASE64_LEN: usize = 16;
const MAX_CIPHERTEXT_BASE64_LEN: usize =
    ((MODERN_MAX_PLAINTEXT_LEN + AES_GCM_TAG_LEN) * 4).div_ceil(3);

/// NC1 解析结果；只服务既有 PBKDF2 密文的兼容读取。
struct Nc1Token {
    iterations: u32,
    salt: Vec<u8>,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
}

/// NC2 解析结果；携带受认证的 Argon2id 参数和 AES-GCM 数据。
struct Nc2Token {
    memory_kib: u32,
    iterations: u32,
    parallelism: u32,
    salt: Vec<u8>,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
}

/// 现代 token 的受支持版本分派。
enum ModernToken {
    Nc1(Nc1Token),
    Nc2(Nc2Token),
}

/// 业务作用: 校验口令边界，避免空口令和超大输入绕过现代加密的资源预算。
///
/// 参数说明:
/// - `password`: 调用方注入的口令或高熵密钥材料。
///
/// 返回: 合法时返回成功；非法时返回配置错误且不会执行 KDF。
fn validate_password(password: &str) -> Result<()> {
    if password.is_empty() {
        return Err(CryptoError::config("modern password 不能为空"));
    }
    if password.len() > MODERN_MAX_PASSWORD_LEN {
        return Err(CryptoError::config(format!(
            "modern password 不能超过 {MODERN_MAX_PASSWORD_LEN} 字节"
        )));
    }
    Ok(())
}

/// 业务作用: 校验加密输入预算，避免在 KDF 和密文分配前接收无界业务数据。
///
/// 参数说明:
/// - `plaintext_len`: 明文字节数。
/// - `aad_len`: 业务 AAD 字节数。
///
/// 返回: 输入在预算内时返回成功；超限时返回配置错误且不生成密文。
fn validate_encrypt_lengths(plaintext_len: usize, aad_len: usize) -> Result<()> {
    if plaintext_len > MODERN_MAX_PLAINTEXT_LEN {
        return Err(CryptoError::config(format!(
            "modern plaintext 不能超过 {MODERN_MAX_PLAINTEXT_LEN} 字节"
        )));
    }
    if aad_len > MODERN_MAX_AAD_LEN {
        return Err(CryptoError::config(format!(
            "modern AAD 不能超过 {MODERN_MAX_AAD_LEN} 字节"
        )));
    }
    Ok(())
}

/// 业务作用: 校验解密输入预算，阻断无界 token/AAD 在解析和 KDF 前消耗内存。
///
/// 参数说明:
/// - `token`: 待解析的自描述密文。
/// - `aad_len`: 调用方提供的业务 AAD 字节数。
///
/// 返回: 输入在预算内时返回成功；超限时返回解密错误且不会执行 KDF。
fn validate_decrypt_lengths(token: &str, aad_len: usize) -> Result<()> {
    if token.len() > MODERN_MAX_TOKEN_LEN {
        return Err(CryptoError::decrypt(format!(
            "modern token 不能超过 {MODERN_MAX_TOKEN_LEN} 字节"
        )));
    }
    if aad_len > MODERN_MAX_AAD_LEN {
        return Err(CryptoError::decrypt(format!(
            "modern AAD 不能超过 {MODERN_MAX_AAD_LEN} 字节"
        )));
    }
    Ok(())
}

/// 业务作用: 从操作系统安全随机源填充 salt 或 nonce，显式传播熵源故障。
///
/// 参数说明:
/// - `output`: 必须完整填充的固定长度随机字节缓冲区。
///
/// 返回: 熵源成功时返回成功；操作系统随机源不可用时返回加密错误且不继续加密。
fn fill_secure_random(output: &mut [u8]) -> Result<()> {
    rand::rngs::OsRng
        .try_fill_bytes(output)
        .map_err(|_| CryptoError::encrypt("操作系统安全随机源不可用"))
}

/// 业务作用: 用 PBKDF2-HMAC-SHA256 派生 NC1 兼容密钥，并限制不可信参数的 CPU 成本。
///
/// 参数说明:
/// - `password`: 口令或高熵密钥材料。
/// - `salt`: NC1 token 携带的随机盐。
/// - `iterations`: NC1 token 携带的 PBKDF2 迭代次数。
///
/// 返回: 参数合法时返回自动清理的 32 字节 AES 密钥；非法时不会执行 PBKDF2。
fn derive_nc1_key(password: &str, salt: &[u8], iterations: u32) -> Result<Zeroizing<[u8; 32]>> {
    validate_password(password)?;
    if !(MODERN_MIN_PBKDF2_ITERATIONS..=MODERN_MAX_PBKDF2_ITERATIONS).contains(&iterations) {
        return Err(CryptoError::decrypt(format!(
            "NC1 PBKDF2 iterations 超出允许范围: {iterations}"
        )));
    }
    let mut key = Zeroizing::new([0u8; 32]);
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut *key);
    Ok(key)
}

/// 业务作用: 校验 NC2 Argon2id 参数预算，避免参数篡改造成降级或资源放大。
///
/// 参数说明:
/// - `memory_kib`: Argon2id 内存成本，单位 KiB。
/// - `iterations`: Argon2id 时间成本。
/// - `parallelism`: Argon2id 并行度。
///
/// 返回: 参数处于受支持预算时返回成功；否则在内存分配前返回解密错误。
fn validate_nc2_params(memory_kib: u32, iterations: u32, parallelism: u32) -> Result<()> {
    if !(MODERN_MIN_ARGON2_MEMORY_KIB..=MODERN_MAX_ARGON2_MEMORY_KIB).contains(&memory_kib) {
        return Err(CryptoError::decrypt(format!(
            "NC2 Argon2id memory cost 超出允许范围: {memory_kib} KiB"
        )));
    }
    if !(MODERN_MIN_ARGON2_ITERATIONS..=MODERN_MAX_ARGON2_ITERATIONS).contains(&iterations) {
        return Err(CryptoError::decrypt(format!(
            "NC2 Argon2id time cost 超出允许范围: {iterations}"
        )));
    }
    if !(MODERN_MIN_ARGON2_PARALLELISM..=MODERN_MAX_ARGON2_PARALLELISM).contains(&parallelism) {
        return Err(CryptoError::decrypt(format!(
            "NC2 Argon2id parallelism 超出允许范围: {parallelism}"
        )));
    }
    Ok(())
}

/// 业务作用: 用 Argon2id 派生 NC2 AES-256-GCM 密钥，并在离开作用域时主动清理密钥字节。
///
/// 参数说明:
/// - `password`: 口令或高熵密钥材料。
/// - `salt`: NC2 token 携带的 16 字节随机盐。
/// - `memory_kib`: Argon2id 内存成本，单位 KiB。
/// - `iterations`: Argon2id 时间成本。
/// - `parallelism`: Argon2id 并行度。
///
/// 返回: 派生成功时返回自动清理的 32 字节 AES 密钥；配置或派生失败时返回错误。
fn derive_nc2_key(
    password: &str,
    salt: &[u8],
    memory_kib: u32,
    iterations: u32,
    parallelism: u32,
) -> Result<Zeroizing<[u8; 32]>> {
    validate_password(password)?;
    validate_nc2_params(memory_kib, iterations, parallelism)?;
    let params = Params::new(memory_kib, iterations, parallelism, Some(32))
        .map_err(|_| CryptoError::config("NC2 Argon2id 参数组合非法"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = Zeroizing::new([0u8; 32]);
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut *key)
        .map_err(|_| CryptoError::config("NC2 Argon2id 密钥派生失败"))?;
    Ok(key)
}

/// 业务作用: 生成 NC1 的历史认证上下文，保证既有密文仍按原格式验证。
///
/// 参数说明:
/// - `iterations`: PBKDF2 迭代次数。
/// - `salt`: KDF 随机盐。
/// - `nonce`: AES-GCM nonce。
///
/// 返回: 返回与 NC1 原实现逐字节一致的 AAD。
fn nc1_aad(iterations: u32, salt: &[u8], nonce: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(
        MODERN_TOKEN_V1_PREFIX.len() + std::mem::size_of::<u32>() + salt.len() + nonce.len(),
    );
    aad.extend_from_slice(MODERN_TOKEN_V1_PREFIX.as_bytes());
    aad.extend_from_slice(&iterations.to_be_bytes());
    aad.extend_from_slice(salt);
    aad.extend_from_slice(nonce);
    aad
}

/// 业务作用: 生成 NC2 认证上下文，把版本、KDF 参数、随机量和业务 AAD 绑定到认证标签。
///
/// 参数说明:
/// - `memory_kib`: Argon2id 内存成本。
/// - `iterations`: Argon2id 时间成本。
/// - `parallelism`: Argon2id 并行度。
/// - `salt`: KDF 随机盐。
/// - `nonce`: AES-GCM nonce。
/// - `external_aad`: 调用方提供且不进入密文的业务认证上下文。
///
/// 返回: 返回字段边界无歧义的二进制 AAD。
fn nc2_aad(
    memory_kib: u32,
    iterations: u32,
    parallelism: u32,
    salt: &[u8],
    nonce: &[u8],
    external_aad: &[u8],
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(
        MODERN_TOKEN_PREFIX.len()
            + 3 * std::mem::size_of::<u32>()
            + salt.len()
            + nonce.len()
            + std::mem::size_of::<u64>()
            + external_aad.len(),
    );
    aad.extend_from_slice(MODERN_TOKEN_PREFIX.as_bytes());
    aad.extend_from_slice(&memory_kib.to_be_bytes());
    aad.extend_from_slice(&iterations.to_be_bytes());
    aad.extend_from_slice(&parallelism.to_be_bytes());
    aad.extend_from_slice(salt);
    aad.extend_from_slice(nonce);
    aad.extend_from_slice(&(external_aad.len() as u64).to_be_bytes());
    aad.extend_from_slice(external_aad);
    aad
}

/// 业务作用: 解析十进制 KDF 参数，拒绝缺失、负值或溢出格式。
///
/// 参数说明:
/// - `value`: token 中的十进制参数文本。
/// - `name`: 用于错误定位的参数名。
///
/// 返回: 格式合法时返回 `u32`；否则返回解密错误。
fn parse_u32(value: &str, name: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .map_err(|_| CryptoError::decrypt(format!("modern token {name} 非法")))
}

/// 业务作用: 解析 NC1 token，并在 KDF 前校验段数、参数和字段长度。
///
/// 参数说明:
/// - `parts`: 已按 `.` 切分的 token 字段。
///
/// 返回: 合法时返回 NC1 结构；非法时返回解密错误且不执行 PBKDF2。
fn parse_nc1(parts: &[&str]) -> Result<Nc1Token> {
    if parts.len() != NC1_TOKEN_PARTS {
        return Err(CryptoError::decrypt(format!(
            "NC1 token 格式非法: 期望 {NC1_TOKEN_PARTS} 段"
        )));
    }
    let iterations = parse_u32(parts[1], "iterations")?;
    if !(MODERN_MIN_PBKDF2_ITERATIONS..=MODERN_MAX_PBKDF2_ITERATIONS).contains(&iterations) {
        return Err(CryptoError::decrypt(format!(
            "NC1 PBKDF2 iterations 超出允许范围: {iterations}"
        )));
    }
    if parts[2].len() != SALT_BASE64_LEN {
        return Err(CryptoError::decrypt("NC1 token salt 编码长度非法"));
    }
    let salt = base64_url_decode(parts[2])?;
    if salt.len() != MODERN_SALT_LEN {
        return Err(CryptoError::decrypt(format!(
            "NC1 token salt 长度非法: {}",
            salt.len()
        )));
    }
    if parts[3].len() != NONCE_BASE64_LEN {
        return Err(CryptoError::decrypt("NC1 token nonce 编码长度非法"));
    }
    let nonce = base64_url_decode(parts[3])?;
    if nonce.len() != MODERN_NONCE_LEN {
        return Err(CryptoError::decrypt(format!(
            "NC1 token nonce 长度非法: {}",
            nonce.len()
        )));
    }
    if parts[4].len() > MAX_CIPHERTEXT_BASE64_LEN {
        return Err(CryptoError::decrypt("NC1 token 密文编码超过允许上限"));
    }
    let ciphertext = base64_url_decode(parts[4])?;
    validate_ciphertext_len(&ciphertext)?;
    Ok(Nc1Token {
        iterations,
        salt,
        nonce,
        ciphertext,
    })
}

/// 业务作用: 解析 NC2 token，并在 Argon2id 分配内存前执行全部结构和预算校验。
///
/// 参数说明:
/// - `parts`: 已按 `.` 切分的 token 字段。
///
/// 返回: 合法时返回 NC2 结构；非法时返回解密错误且不执行 KDF。
fn parse_nc2(parts: &[&str]) -> Result<Nc2Token> {
    if parts.len() != NC2_TOKEN_PARTS {
        return Err(CryptoError::decrypt(format!(
            "NC2 token 格式非法: 期望 {NC2_TOKEN_PARTS} 段"
        )));
    }
    let memory_kib = parse_u32(parts[1], "memory cost")?;
    let iterations = parse_u32(parts[2], "time cost")?;
    let parallelism = parse_u32(parts[3], "parallelism")?;
    validate_nc2_params(memory_kib, iterations, parallelism)?;
    if parts[4].len() != SALT_BASE64_LEN {
        return Err(CryptoError::decrypt("NC2 token salt 编码长度非法"));
    }
    let salt = base64_url_decode(parts[4])?;
    if salt.len() != MODERN_SALT_LEN {
        return Err(CryptoError::decrypt(format!(
            "NC2 token salt 长度非法: {}",
            salt.len()
        )));
    }
    if parts[5].len() != NONCE_BASE64_LEN {
        return Err(CryptoError::decrypt("NC2 token nonce 编码长度非法"));
    }
    let nonce = base64_url_decode(parts[5])?;
    if nonce.len() != MODERN_NONCE_LEN {
        return Err(CryptoError::decrypt(format!(
            "NC2 token nonce 长度非法: {}",
            nonce.len()
        )));
    }
    if parts[6].len() > MAX_CIPHERTEXT_BASE64_LEN {
        return Err(CryptoError::decrypt("NC2 token 密文编码超过允许上限"));
    }
    let ciphertext = base64_url_decode(parts[6])?;
    validate_ciphertext_len(&ciphertext)?;
    Ok(Nc2Token {
        memory_kib,
        iterations,
        parallelism,
        salt,
        nonce,
        ciphertext,
    })
}

/// 业务作用: 校验认证密文长度，拒绝缺失认证标签或超出明文预算的输入。
///
/// 参数说明:
/// - `ciphertext`: 解码后的 `ciphertext || tag` 字节。
///
/// 返回: 长度合法时返回成功；非法时返回解密错误。
fn validate_ciphertext_len(ciphertext: &[u8]) -> Result<()> {
    if ciphertext.len() < AES_GCM_TAG_LEN {
        return Err(CryptoError::decrypt("modern token 密文过短"));
    }
    if ciphertext.len() > MODERN_MAX_PLAINTEXT_LEN + AES_GCM_TAG_LEN {
        return Err(CryptoError::decrypt("modern token 密文超过允许上限"));
    }
    Ok(())
}

/// 业务作用: 识别并解析现代 token 版本，拒绝未知版本而不做隐式算法降级。
///
/// 参数说明:
/// - `token`: 待解析的自描述密文。
///
/// 返回: 返回 NC1 或 NC2 结构；未知版本和非法格式返回解密错误。
fn parse_modern_token(token: &str) -> Result<ModernToken> {
    let version = token
        .split_once('.')
        .map(|(value, _)| value)
        .unwrap_or(token);
    match version {
        MODERN_TOKEN_V1_PREFIX => {
            // 最多切出“期望段数 + 1”，避免恶意点号序列把小 token 放大成巨型 Vec。
            let parts = token.splitn(NC1_TOKEN_PARTS + 1, '.').collect::<Vec<_>>();
            parse_nc1(&parts).map(ModernToken::Nc1)
        }
        MODERN_TOKEN_PREFIX => {
            // NC2 同样限制分段数量；多余字段会以第八段存在并由格式门禁拒绝。
            let parts = token.splitn(NC2_TOKEN_PARTS + 1, '.').collect::<Vec<_>>();
            parse_nc2(&parts).map(ModernToken::Nc2)
        }
        "" => Err(CryptoError::decrypt("modern token 为空")),
        _ => Err(CryptoError::decrypt("modern token 版本不受支持")),
    }
}

/// 业务作用: 使用固定长度 nonce 执行 AES-256-GCM 认证加密。
///
/// 参数说明:
/// - `key`: KDF 派生的 32 字节密钥。
/// - `nonce`: 本轮唯一的 12 字节随机数。
/// - `content`: 明文字节。
/// - `aad`: 需要认证但不加密的上下文。
///
/// 返回: 成功时返回 `ciphertext || tag`；失败时不返回部分密文。
fn encrypt_aead(
    key: &[u8; 32],
    nonce: [u8; MODERN_NONCE_LEN],
    content: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| CryptoError::config("AES-256-GCM key 初始化失败"))?;
    cipher
        .encrypt(&Nonce::from(nonce), Payload { msg: content, aad })
        .map_err(|_| CryptoError::encrypt("AES-256-GCM 加密失败"))
}

/// 业务作用: 使用固定长度 nonce 执行 AES-256-GCM 认证解密并统一认证失败结果。
///
/// 参数说明:
/// - `key`: KDF 派生的 32 字节密钥。
/// - `nonce`: token 携带的 12 字节随机数。
/// - `ciphertext`: `ciphertext || tag` 字节。
/// - `aad`: 重新构造的认证上下文。
///
/// 返回: 认证成功时返回完整明文；认证失败时不返回任何部分明文。
fn decrypt_aead(key: &[u8; 32], nonce: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    let nonce_bytes: [u8; MODERN_NONCE_LEN] = nonce
        .try_into()
        .map_err(|_| CryptoError::decrypt("modern token nonce 长度非法"))?;
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| CryptoError::config("AES-256-GCM key 初始化失败"))?;
    cipher
        .decrypt(
            &Nonce::from(nonce_bytes),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| CryptoError::decrypt("AES-256-GCM 认证失败"))
}

/// 业务作用: 使用现代默认方案加密 UTF-8 明文，生成可持久化的 NC2 token。
///
/// 参数说明:
/// - `content`: 要加密的 UTF-8 明文。
/// - `password`: 业务侧注入的口令或高熵密钥材料。
///
/// 返回: 成功时返回 NC2 自描述密文；输入非法、熵源或加密失败时返回错误。
pub fn encrypt_modern(content: &str, password: &str) -> Result<String> {
    encrypt_modern_bytes_with_aad(content.as_bytes(), password, &[])
}

/// 业务作用: 解密 NC2 或兼容 NC1 token，并将明文校验为 UTF-8。
///
/// 参数说明:
/// - `token`: 现代自描述密文。
/// - `password`: 加密时使用的口令或高熵密钥材料。
///
/// 返回: 认证及 UTF-8 校验成功时返回明文；失败时不返回部分内容。
pub fn decrypt_modern(token: &str, password: &str) -> Result<String> {
    decrypt_modern_with_aad(token, password, &[])
}

/// 业务作用: 使用业务 AAD 加密 UTF-8 明文，把租户、主键或协议上下文绑定到 NC2 token。
///
/// 参数说明:
/// - `content`: 要加密的 UTF-8 明文。
/// - `password`: 业务侧注入的口令或高熵密钥材料。
/// - `aad`: 需要认证但不写入 token 的业务上下文，解密时必须逐字节一致。
///
/// 返回: 成功时返回 NC2 自描述密文；输入非法、熵源或加密失败时返回错误。
pub fn encrypt_modern_with_aad(content: &str, password: &str, aad: &[u8]) -> Result<String> {
    encrypt_modern_bytes_with_aad(content.as_bytes(), password, aad)
}

/// 业务作用: 使用业务 AAD 解密 UTF-8 token，阻止密文跨租户、记录或协议上下文搬用。
///
/// 参数说明:
/// - `token`: [`encrypt_modern_with_aad`] 生成的 NC2 token，或在 AAD 为空时使用的 NC1 token。
/// - `password`: 加密时使用的口令或高熵密钥材料。
/// - `aad`: 加密时绑定的业务上下文。
///
/// 返回: 认证及 UTF-8 校验成功时返回明文；失败时不返回部分内容。
pub fn decrypt_modern_with_aad(token: &str, password: &str, aad: &[u8]) -> Result<String> {
    let plaintext = decrypt_modern_bytes_with_aad(token, password, aad)?;
    String::from_utf8(plaintext).map_err(|_| CryptoError::decrypt("modern 明文不是 UTF-8"))
}

/// 业务作用: 使用现代默认方案加密任意字节，生成可持久化的 NC2 token。
///
/// 参数说明:
/// - `content`: 要加密的原始字节。
/// - `password`: 业务侧注入的口令或高熵密钥材料。
///
/// 返回: 成功时返回 NC2 自描述密文；输入非法、熵源或加密失败时返回错误。
pub fn encrypt_modern_bytes(content: &[u8], password: &str) -> Result<String> {
    encrypt_modern_bytes_with_aad(content, password, &[])
}

/// 业务作用: 解密 NC2 或兼容 NC1 token，返回不强制编码的原始明文字节。
///
/// 参数说明:
/// - `token`: 现代自描述密文。
/// - `password`: 加密时使用的口令或高熵密钥材料。
///
/// 返回: 认证成功时返回完整明文字节；失败时不返回部分内容。
pub fn decrypt_modern_bytes(token: &str, password: &str) -> Result<Vec<u8>> {
    decrypt_modern_bytes_with_aad(token, password, &[])
}

/// 业务作用: 使用业务 AAD 加密任意字节，生成绑定业务上下文的 NC2 token。
///
/// 参数说明:
/// - `content`: 要加密的原始字节。
/// - `password`: 业务侧注入的口令或高熵密钥材料。
/// - `aad`: 需要认证但不写入 token 的业务上下文。
///
/// 返回: 成功时返回 NC2 自描述密文；输入非法、熵源或加密失败时返回错误。
pub fn encrypt_modern_bytes_with_aad(content: &[u8], password: &str, aad: &[u8]) -> Result<String> {
    validate_password(password)?;
    validate_encrypt_lengths(content.len(), aad.len())?;

    let mut salt = [0u8; MODERN_SALT_LEN];
    let mut nonce = [0u8; MODERN_NONCE_LEN];
    // salt 与 nonce 都必须来自可失败的 OS 熵源；任何一次失败都停止，不能用全零或伪随机值降级。
    fill_secure_random(&mut salt)?;
    fill_secure_random(&mut nonce)?;

    let key = derive_nc2_key(
        password,
        &salt,
        MODERN_ARGON2_MEMORY_KIB,
        MODERN_ARGON2_ITERATIONS,
        MODERN_ARGON2_PARALLELISM,
    )?;
    let authenticated_context = nc2_aad(
        MODERN_ARGON2_MEMORY_KIB,
        MODERN_ARGON2_ITERATIONS,
        MODERN_ARGON2_PARALLELISM,
        &salt,
        &nonce,
        aad,
    );
    let ciphertext = encrypt_aead(&key, nonce, content, &authenticated_context)?;

    Ok(format!(
        "{MODERN_TOKEN_PREFIX}.{MODERN_ARGON2_MEMORY_KIB}.{MODERN_ARGON2_ITERATIONS}.{MODERN_ARGON2_PARALLELISM}.{}.{}.{}",
        base64_url_encode(&salt),
        base64_url_encode(&nonce),
        base64_url_encode(&ciphertext)
    ))
}

/// 业务作用: 使用业务 AAD 解密任意字节，并按 token 版本选择 NC2 或 NC1 兼容路径。
///
/// 参数说明:
/// - `token`: 现代自描述密文。
/// - `password`: 加密时使用的口令或高熵密钥材料。
/// - `aad`: NC2 加密时绑定的业务上下文；NC1 只允许为空。
///
/// 返回: 认证成功时返回完整明文字节；格式、预算、口令或 AAD 不匹配时返回错误。
pub fn decrypt_modern_bytes_with_aad(token: &str, password: &str, aad: &[u8]) -> Result<Vec<u8>> {
    validate_password(password)?;
    validate_decrypt_lengths(token, aad.len())?;
    match parse_modern_token(token)? {
        ModernToken::Nc1(parsed) => {
            // NC1 原格式没有业务 AAD；非空 AAD 必须拒绝，不能假装已经建立上下文绑定。
            if !aad.is_empty() {
                return Err(CryptoError::decrypt("NC1 token 不支持业务 AAD"));
            }
            let key = derive_nc1_key(password, &parsed.salt, parsed.iterations)?;
            let authenticated_context = nc1_aad(parsed.iterations, &parsed.salt, &parsed.nonce);
            decrypt_aead(
                &key,
                &parsed.nonce,
                &parsed.ciphertext,
                &authenticated_context,
            )
        }
        ModernToken::Nc2(parsed) => {
            let key = derive_nc2_key(
                password,
                &parsed.salt,
                parsed.memory_kib,
                parsed.iterations,
                parsed.parallelism,
            )?;
            let authenticated_context = nc2_aad(
                parsed.memory_kib,
                parsed.iterations,
                parsed.parallelism,
                &parsed.salt,
                &parsed.nonce,
                aad,
            );
            decrypt_aead(
                &key,
                &parsed.nonce,
                &parsed.ciphertext,
                &authenticated_context,
            )
        }
    }
}
