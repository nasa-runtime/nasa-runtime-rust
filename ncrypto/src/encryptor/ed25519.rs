//! Ed25519 现代椭圆曲线签名。
//! 密钥 = Base64(PKCS8 私钥 / SPKI 公钥);签名 = Base64(64 字节)。

use super::b64_encode;
use crate::{CryptoError, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// 业务作用：生成随机 Ed25519 签名密钥对并编码供存储或传输。
///
/// 参数说明: 无。
///
/// 返回：成功返回私钥 PKCS8 DER 的 Base64 和公钥 SPKI DER 的 Base64；DER 编码失败返回错误，不保留生成密钥。
pub fn generate_ed25519_key_pair() -> Result<(String, String)> {
    let mut rng = rand::rngs::OsRng;
    let signing = SigningKey::generate(&mut rng);
    let verifying = signing.verifying_key();
    let priv_der = signing
        .to_pkcs8_der()
        .map_err(|e| CryptoError::encrypt(format!("Ed25519 私钥编码: {e}")))?;
    let pub_der = verifying
        .to_public_key_der()
        .map_err(|e| CryptoError::encrypt(format!("Ed25519 公钥编码: {e}")))?;
    Ok((
        b64_encode(priv_der.as_bytes()),
        b64_encode(pub_der.as_bytes()),
    ))
}

/// 业务作用：用 PKCS8 Ed25519 私钥对 UTF-8 内容签名。
///
/// 参数说明：
/// - `content`：要签名的原始 UTF-8 内容。
/// - `private_key_b64`：标准 Base64 编码的 PKCS8 私钥 DER。
///
/// 返回：返回 64 字节签名的标准 Base64 文本；密钥 Base64 或 DER 无效时返回错误。
pub fn sign_ed25519(content: &str, private_key_b64: &str) -> Result<String> {
    let der = STANDARD
        .decode(private_key_b64)
        .map_err(|e| CryptoError::config(format!("Ed25519 私钥 base64: {e}")))?;
    let signing = SigningKey::from_pkcs8_der(&der)
        .map_err(|e| CryptoError::config(format!("Ed25519 私钥 PKCS8 解析: {e}")))?;
    let sig = signing.sign(content.as_bytes());
    Ok(b64_encode(&sig.to_bytes()))
}

/// 业务作用：用 SPKI Ed25519 公钥验证原始内容与签名。
///
/// 参数说明：
/// - `content`：必须与签名输入相同的 UTF-8 内容。
/// - `sign_b64`：标准 Base64 编码的 64 字节签名。
/// - `public_key_b64`：标准 Base64 编码的 SPKI 公钥 DER。
///
/// 返回：签名验证通过返回 true；编码、密钥、签名结构或验证失败统一返回 false。
pub fn verify_ed25519(content: &str, sign_b64: &str, public_key_b64: &str) -> bool {
    let run = || -> Result<bool> {
        let der = STANDARD
            .decode(public_key_b64)
            .map_err(|e| CryptoError::decrypt(format!("Ed25519 公钥 base64: {e}")))?;
        let verifying = VerifyingKey::from_public_key_der(&der)
            .map_err(|e| CryptoError::decrypt(format!("Ed25519 公钥 SPKI 解析: {e}")))?;
        let sig_bytes = STANDARD
            .decode(sign_b64)
            .map_err(|e| CryptoError::decrypt(format!("签名 base64: {e}")))?;
        let sig = Signature::from_slice(&sig_bytes)
            .map_err(|e| CryptoError::decrypt(format!("签名格式: {e}")))?;
        Ok(verifying.verify(content.as_bytes(), &sig).is_ok())
    };
    run().unwrap_or(false)
}
