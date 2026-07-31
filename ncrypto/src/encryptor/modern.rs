//! 现代默认加密入口。
//!
//! 本模块服务新业务机密性边界,不追求与历史加密工具逐字节互通。旧的 AES-ECB/CBC/RSA PKCS#1 v1.5
//! 入口继续保留给历史数据和跨语言兼容;新数据建议优先使用这里的 password-based AEAD token。

use super::{base64_url_decode, base64_url_encode};
use crate::{CryptoError, Result};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use rand::RngCore;
use sha2::Sha256;

/// 现代加密 token 的版本前缀;当前格式为 `NC1.iterations.salt.nonce.ciphertext`。
pub const MODERN_TOKEN_PREFIX: &str = "NC1";

/// 现代默认 PBKDF2-HMAC-SHA256 迭代次数。
pub const MODERN_PBKDF2_ITERATIONS: u32 = 210_000;

/// 现代 token 解密接受的最低 PBKDF2 迭代次数,防止被篡改成过低成本参数。
pub const MODERN_MIN_PBKDF2_ITERATIONS: u32 = 100_000;

/// 现代 token 解密接受的最高 PBKDF2 迭代次数。防止攻击者构造超高迭代 token 放大 KDF 成本做 CPU-DoS
/// ——不设上限时,`NC1.4294967295.…`(u32::MAX)会让单次解密跑 42.9 亿次 PBKDF2、耗时几十分钟。
/// 上限给足未来提升密钥派生成本的余量(约默认值的 48 倍),同时把单次解密最坏 CPU 时间封顶到秒级。
/// 注:解密**不可信来源** token 的接口仍应叠加自己的限流,本上限只堵住单次 KDF 成本被放大。
pub const MODERN_MAX_PBKDF2_ITERATIONS: u32 = 10_000_000;

/// 现代 token 使用的随机 salt 长度,单位字节。
pub const MODERN_SALT_LEN: usize = 16;

/// AES-GCM 推荐 nonce 长度,单位字节。
pub const MODERN_NONCE_LEN: usize = 12;

const MODERN_TOKEN_PARTS: usize = 5;

/// 现代 token 的解析结果；用于把自描述密文拆成 KDF 参数、nonce 和认证密文。
struct ModernToken {
    /// PBKDF2-HMAC-SHA256 迭代次数。
    iterations: u32,
    /// PBKDF2 随机盐。
    salt: Vec<u8>,
    /// AES-GCM nonce。
    nonce: Vec<u8>,
    /// AES-GCM 密文和 128-bit tag。
    ciphertext: Vec<u8>,
}

/// 用 password + salt 派生 AES-256-GCM 原始密钥；用于避免调用方把口令直接当 AES key。
///
/// # 参数
/// - `password`: 口令或密码,作为敏感输入使用。
/// - `salt`: KDF salt,用于派生密钥。
/// - `iterations`: KDF 迭代次数。
fn derive_modern_key(password: &str, salt: &[u8], iterations: u32) -> Result<[u8; 32]> {
    if password.is_empty() {
        return Err(CryptoError::config("modern password 不能为空"));
    }
    if iterations < MODERN_MIN_PBKDF2_ITERATIONS {
        return Err(CryptoError::config(format!(
            "PBKDF2 iterations 不能小于 {MODERN_MIN_PBKDF2_ITERATIONS},当前: {iterations}"
        )));
    }
    if iterations > MODERN_MAX_PBKDF2_ITERATIONS {
        return Err(CryptoError::config(format!(
            "PBKDF2 iterations 不能大于 {MODERN_MAX_PBKDF2_ITERATIONS},当前: {iterations}"
        )));
    }
    let mut key = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut key);
    Ok(key)
}

/// 生成 AES-GCM 附加认证数据；用于把版本、KDF 参数和 nonce 绑定进认证标签。
///
/// # 参数
/// - `iterations`: KDF 迭代次数。
/// - `salt`: KDF salt,用于派生密钥。
/// - `nonce`: 本轮 fencing/bootstrap 的随机标识。
fn modern_aad(iterations: u32, salt: &[u8], nonce: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(
        MODERN_TOKEN_PREFIX.len() + std::mem::size_of::<u32>() + salt.len() + nonce.len(),
    );
    aad.extend_from_slice(MODERN_TOKEN_PREFIX.as_bytes());
    aad.extend_from_slice(&iterations.to_be_bytes());
    aad.extend_from_slice(salt);
    aad.extend_from_slice(nonce);
    aad
}

/// 解析现代 token；用于在解密前校验格式、版本和参数长度。
///
/// # 参数
/// - `token`: 加解密或认证流程中的业务 token。
fn parse_modern_token(token: &str) -> Result<ModernToken> {
    let parts = token.split('.').collect::<Vec<_>>();
    if parts.len() != MODERN_TOKEN_PARTS {
        return Err(CryptoError::decrypt(format!(
            "modern token 格式非法: 期望 {MODERN_TOKEN_PARTS} 段"
        )));
    }
    if parts[0] != MODERN_TOKEN_PREFIX {
        return Err(CryptoError::decrypt(format!(
            "modern token 版本非法: {}",
            parts[0]
        )));
    }
    let iterations = parts[1]
        .parse::<u32>()
        .map_err(|e| CryptoError::decrypt(format!("modern token iterations 非法: {e}")))?;
    if iterations < MODERN_MIN_PBKDF2_ITERATIONS {
        return Err(CryptoError::decrypt(format!(
            "modern token iterations 过低: {iterations}"
        )));
    }
    // 上限:在【派生密钥之前】就拒绝超高迭代 token,避免攻击者用 KDF 成本放大做 CPU-DoS。
    if iterations > MODERN_MAX_PBKDF2_ITERATIONS {
        return Err(CryptoError::decrypt(format!(
            "modern token iterations 过高: {iterations}"
        )));
    }
    let salt = base64_url_decode(parts[2])?;
    if salt.len() != MODERN_SALT_LEN {
        return Err(CryptoError::decrypt(format!(
            "modern token salt 长度非法: {}",
            salt.len()
        )));
    }
    let nonce = base64_url_decode(parts[3])?;
    if nonce.len() != MODERN_NONCE_LEN {
        return Err(CryptoError::decrypt(format!(
            "modern token nonce 长度非法: {}",
            nonce.len()
        )));
    }
    let ciphertext = base64_url_decode(parts[4])?;
    if ciphertext.len() < 16 {
        return Err(CryptoError::decrypt("modern token 密文过短"));
    }
    Ok(ModernToken {
        iterations,
        salt,
        nonce,
        ciphertext,
    })
}

/// 现代默认加密(UTF-8 明文,password-based AES-256-GCM token)。
///
/// 该入口为新业务默认推荐方案:每次加密生成随机 salt 和 nonce,用 PBKDF2-HMAC-SHA256 从口令派生
/// 32 字节 AES key,再用 AES-256-GCM 做认证加密。返回值自带版本、迭代次数、salt、nonce 和密文,
/// 可直接入库或经 JSON/URL 传输。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文。
/// - `password`: 业务侧保护该数据使用的口令或高熵密钥材料;不能为空。
pub fn encrypt_modern(content: &str, password: &str) -> Result<String> {
    encrypt_modern_bytes(content.as_bytes(), password)
}

/// 现代默认解密(UTF-8 明文,password-based AES-256-GCM token)。
///
/// 解密时会校验 token 版本、PBKDF2 参数、salt/nonce 长度和 GCM 认证标签;口令错误、密文被篡改、
/// header 被改动都会返回 `Err`,不会产出伪明文。
///
/// # 参数
/// - `token`: [`encrypt_modern`] 生成的 `NC1.*` 自描述密文。
/// - `password`: 加密时使用的口令或高熵密钥材料;必须完全一致。
pub fn decrypt_modern(token: &str, password: &str) -> Result<String> {
    let plaintext = decrypt_modern_bytes(token, password)?;
    String::from_utf8(plaintext).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// 现代默认加密(字节明文,password-based AES-256-GCM token)。
///
/// 适用于图片片段、二进制字段或尚未确定编码的 payload。token 格式与 [`encrypt_modern`] 相同。
///
/// # 参数
/// - `content`: 要加密的原始字节。
/// - `password`: 业务侧保护该数据使用的口令或高熵密钥材料;不能为空。
pub fn encrypt_modern_bytes(content: &[u8], password: &str) -> Result<String> {
    let mut salt = [0u8; MODERN_SALT_LEN];
    let mut nonce = [0u8; MODERN_NONCE_LEN];
    let mut rng = rand::rngs::OsRng;
    rng.fill_bytes(&mut salt);
    rng.fill_bytes(&mut nonce);

    let key = derive_modern_key(password, &salt, MODERN_PBKDF2_ITERATIONS)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| CryptoError::config(format!("AES-256-GCM key 初始化失败: {e}")))?;
    let aad = modern_aad(MODERN_PBKDF2_ITERATIONS, &salt, &nonce);
    let cipher_nonce = Nonce::from(nonce);
    let ciphertext = cipher
        .encrypt(
            &cipher_nonce,
            Payload {
                msg: content,
                aad: &aad,
            },
        )
        .map_err(|e| CryptoError::encrypt(format!("AES-256-GCM 加密失败: {e}")))?;

    Ok(format!(
        "{MODERN_TOKEN_PREFIX}.{MODERN_PBKDF2_ITERATIONS}.{}.{}.{}",
        base64_url_encode(&salt),
        base64_url_encode(&nonce),
        base64_url_encode(&ciphertext)
    ))
}

/// 现代默认解密(字节明文,password-based AES-256-GCM token)。
///
/// 返回原始字节,不强制 UTF-8 校验;需要字符串时使用 [`decrypt_modern`]。
///
/// # 参数
/// - `token`: [`encrypt_modern_bytes`] 生成的 `NC1.*` 自描述密文。
/// - `password`: 加密时使用的口令或高熵密钥材料;必须完全一致。
pub fn decrypt_modern_bytes(token: &str, password: &str) -> Result<Vec<u8>> {
    let parsed = parse_modern_token(token)?;
    let key = derive_modern_key(password, &parsed.salt, parsed.iterations)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|e| CryptoError::config(format!("AES-256-GCM key 初始化失败: {e}")))?;
    let aad = modern_aad(parsed.iterations, &parsed.salt, &parsed.nonce);
    let nonce_bytes: [u8; MODERN_NONCE_LEN] = parsed
        .nonce
        .as_slice()
        .try_into()
        .map_err(|_| CryptoError::decrypt("现代密文 nonce 长度非法"))?;
    let cipher_nonce = Nonce::from(nonce_bytes);
    cipher
        .decrypt(
            &cipher_nonce,
            Payload {
                msg: &parsed.ciphertext,
                aad: &aad,
            },
        )
        .map_err(|e| CryptoError::decrypt(format!("AES-256-GCM 认证/解密失败: {e}")))
}
