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

/// 业务作用: 把字符串按固定大端长度前缀追加到派生上下文。
///
/// 这种编码防止相邻字段产生拼接歧义，例如 `ab + c` 与 `a + bc` 得到同一输入。
///
/// # 参数
///
/// - `output`: 框架内部创建的派生上下文缓冲区，函数只向尾部追加数据。
/// - `value`: 已经过调用方长度校验的 UTF-8 协议字段，不允许超过 `u32::MAX` 字节。
///
/// # 错误
///
/// 当字段长度无法用四字节长度表示时返回配置错误，不生成截断后的派生上下文。
fn push_length_prefixed(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| CryptoError::config("派生上下文字段长度超过 u32 上限"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

/// 业务作用: 从高熵主密钥派生 Web 端点单方向 AES-256-GCM 密钥。
///
/// 请求和响应必须传入不同的 `direction`，从而让同一主密钥产生相互隔离的子密钥。租户、密钥域、
/// 密钥标识和算法也进入派生上下文，防止同一材料跨安全域复用。
///
/// # 参数
///
/// - `master`: secret source 提供的 32 字节高熵主密钥，不接受口令或任意长度文本。
/// - `direction`: 协议方向，只允许 `"request"` 或 `"response"`。
/// - `tenant_scope`: 已认证租户；单租户服务传稳定服务域，不能为空。
/// - `key_scope`: 路由静态声明的密钥域，不能为空。
/// - `kid`: key ring 内已经验证存在的密钥标识，不能为空。
///
/// # 返回
///
/// 返回当前方向专用的 32 字节 AES 密钥。调用方应缩短其生命周期并避免调试输出。
///
/// # 错误
///
/// 主密钥长度、方向或上下文字段非法时返回配置错误；不会用默认值继续派生。
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

/// 业务作用: 使用调用方给定的随机数和附加认证数据执行 AES-256-GCM 加密。
///
/// 本函数不生成随机数，因为协议层必须把同一随机数同时写入信封和认证上下文。调用方负责保证
/// 同一密钥下随机数不重复；函数只执行长度校验和认证加密。
///
/// # 参数
///
/// - `key`: [`derive_web_aead_key`] 生成的 32 字节方向密钥。
/// - `nonce`: 发起方密码学安全随机源生成的 12 字节随机数。
/// - `plaintext`: 已通过协议大小限制的原始明文字节，可以为空。
/// - `aad`: 协议层按固定二进制格式生成的附加认证数据，可以为空但 Web 协议不会传空。
///
/// # 返回
///
/// 返回 `ciphertext || 16-byte tag`，不包含随机数或 Base64 编码。
///
/// # 错误
///
/// 密钥、随机数长度非法或底层认证加密失败时返回错误，不返回部分密文。
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

/// 业务作用: 使用调用方给定的随机数和附加认证数据执行 AES-256-GCM 认证解密。
///
/// 认证标签、随机数或 AAD 任一不匹配都只返回统一错误，不向上层泄露具体失败位置。
///
/// # 参数
///
/// - `key`: 与加密方向一致的 32 字节派生密钥。
/// - `nonce`: 信封携带并经过长度校验的 12 字节随机数。
/// - `ciphertext`: `ciphertext || 16-byte tag` 原始字节，至少 16 字节。
/// - `aad`: 服务端根据可信路由和请求元数据重新构造的附加认证数据。
///
/// # 返回
///
/// 认证成功时返回完整明文字节；认证失败时不会返回任何部分明文。
///
/// # 错误
///
/// 输入长度非法或认证失败时返回统一解密错误。
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
