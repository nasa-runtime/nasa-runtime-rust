//! Web 端点现代协议使用的纯算法原语。
//!
//! 本模块只处理密钥派生和认证加解密，不读取 HTTP、路由或配置。协议字段校验、重放占位和
//! 错误映射由上层完成，避免底层算法函数拥有隐式降级能力。

use crate::{CryptoError, Result};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;

/// Web 现代协议主密钥的固定字节数。
pub const WEB_AEAD_MASTER_KEY_LEN: usize = 32;

/// Web 现代协议随机数的固定字节数。
pub const WEB_AEAD_NONCE_LEN: usize = 12;

const WEB_AEAD_SALT: &[u8] = b"nasa-web-crypto/v2";
const WEB_AEAD_ALGORITHM: &str = "A256GCM";

/// 业务作用：为派生上下文字段追加固定宽度长度边界，避免相邻文本拼接歧义。
///
/// 参数说明：
/// - `output`：只在尾部追加的派生上下文缓冲区。
/// - `value`：按 UTF-8 字节计长的协议字段。
///
/// 返回：长度可表示为 u32 时追加四字节大端长度及字段内容；超界返回配置错误，本字段不追加部分内容。
fn push_length_prefixed(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| CryptoError::config("派生上下文字段长度超过 u32 上限"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

/// 业务作用：用受信主密钥及方向、租户和密钥域派生 Web AEAD 子密钥。
///
/// 参数说明：
/// - `master`：32 字节主密钥材料，熵质量由来源保证。
/// - `direction`：只允许 request 或 response 的协议方向。
/// - `tenant_scope`：非空的已认证租户或稳定服务域。
/// - `key_scope`：非空的路由静态密钥域。
/// - `kid`：非空的已选密钥标识，存在性由调用方检查。
///
/// 返回：返回 HKDF-SHA256 派生的 32 字节方向密钥；主密钥长度、方向、空字段或长度编码失败返回配置错误，不使用默认值继续派生。
pub fn derive_web_aead_key(
    master: &[u8],
    direction: &str,
    tenant_scope: &str,
    key_scope: &str,
    kid: &str,
) -> Result<[u8; WEB_AEAD_MASTER_KEY_LEN]> {
    if master.len() != WEB_AEAD_MASTER_KEY_LEN {
        return Err(CryptoError::config(format!(
            "Web AEAD 主密钥必须是 {WEB_AEAD_MASTER_KEY_LEN} 字节"
        )));
    }
    if !matches!(direction, "request" | "response") {
        return Err(CryptoError::config(
            "Web AEAD 方向必须是 request 或 response",
        ));
    }
    if tenant_scope.is_empty() || key_scope.is_empty() || kid.is_empty() {
        return Err(CryptoError::config(
            "Web AEAD tenant_scope、key_scope、kid 均不能为空",
        ));
    }

    let mut info = Vec::with_capacity(
        direction.len()
            + tenant_scope.len()
            + key_scope.len()
            + kid.len()
            + WEB_AEAD_ALGORITHM.len()
            + 20,
    );
    push_length_prefixed(&mut info, direction)?;
    push_length_prefixed(&mut info, tenant_scope)?;
    push_length_prefixed(&mut info, key_scope)?;
    push_length_prefixed(&mut info, kid)?;
    push_length_prefixed(&mut info, WEB_AEAD_ALGORITHM)?;

    let hkdf = Hkdf::<Sha256>::new(Some(WEB_AEAD_SALT), master);
    let mut key = [0u8; WEB_AEAD_MASTER_KEY_LEN];
    hkdf.expand(&info, &mut key)
        .map_err(|_| CryptoError::config("Web AEAD HKDF 输出长度非法"))?;
    Ok(key)
}

/// 业务作用：以调用方提供的随机数和业务 AAD 执行 AES-256-GCM 认证加密。
///
/// 参数说明：
/// - `key`：32 字节方向密钥。
/// - `nonce`：12 字节随机数，同一密钥下唯一性由调用方保证。
/// - `plaintext`：调用方已限制大小的原始明文字节，可为空。
/// - `aad`：协议层重建的附加认证字节，可为空。
///
/// 返回：成功返回密文及 16 字节标签，不包含 nonce 或 Base64；密钥、nonce 长度非法或加密失败返回错误，不返回部分密文。
pub fn encrypt_web_aead(key: &[u8], nonce: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    if nonce.len() != WEB_AEAD_NONCE_LEN {
        return Err(CryptoError::config(format!(
            "Web AEAD nonce 必须是 {WEB_AEAD_NONCE_LEN} 字节"
        )));
    }
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| CryptoError::config("Web AEAD key 必须是 32 字节"))?;
    let nonce_bytes: [u8; WEB_AEAD_NONCE_LEN] = nonce
        .try_into()
        .map_err(|_| CryptoError::config("Web AEAD nonce 长度非法"))?;
    let cipher_nonce = Nonce::from(nonce_bytes);
    cipher
        .encrypt(
            &cipher_nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptoError::encrypt("Web AEAD 加密失败"))
}

/// 业务作用：校验随机数、业务 AAD 和标签后解开 Web AEAD 密文。
///
/// 参数说明：
/// - `key`：与加密相同方向的 32 字节密钥。
/// - `nonce`：与加密一致的 12 字节随机数。
/// - `ciphertext`：至少包含 16 字节标签的密文字节。
/// - `aad`：必须与加密一致的可信业务认证字节。
///
/// 返回：认证成功返回完整明文；nonce 或密文长度非法及认证失败返回解密错误，密钥长度非法返回配置错误，不返回部分明文。
pub fn decrypt_web_aead(
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>> {
    if nonce.len() != WEB_AEAD_NONCE_LEN {
        return Err(CryptoError::decrypt(format!(
            "Web AEAD nonce 必须是 {WEB_AEAD_NONCE_LEN} 字节"
        )));
    }
    if ciphertext.len() < 16 {
        return Err(CryptoError::decrypt("Web AEAD 密文长度不足"));
    }
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| CryptoError::config("Web AEAD key 必须是 32 字节"))?;
    let nonce_bytes: [u8; WEB_AEAD_NONCE_LEN] = nonce
        .try_into()
        .map_err(|_| CryptoError::decrypt("Web AEAD nonce 长度非法"))?;
    let cipher_nonce = Nonce::from(nonce_bytes);
    cipher
        .decrypt(
            &cipher_nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| CryptoError::decrypt("Web AEAD 认证失败"))
}
