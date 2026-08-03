//! Ed25519 现代椭圆曲线签名(对照 原实现 `generateEd25519KeyPair`/`signEd25519`/`verifyEd25519`)。
//! 密钥 = Base64(PKCS8 私钥 / SPKI 公钥);签名 = Base64(64 字节)。

use super::b64_encode;
use crate::{CryptoError, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// 业务作用: 生成 Ed25519 密钥对,返回 `(私钥 Base64(PKCS8), 公钥 Base64(SPKI))`。
///
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

/// 业务作用: Ed25519 私钥签名(Base64 输出,64 字节)。对照 原实现 `signEd25519`。
///
/// # 参数
/// - `content`: 要签名的 UTF-8 内容。
/// - `private_key_b64`: Base64 编码的 PKCS8 Ed25519 私钥 DER。
pub fn sign_ed25519(content: &str, private_key_b64: &str) -> Result<String> {
    let der = STANDARD
        .decode(private_key_b64)
        .map_err(|e| CryptoError::config(format!("Ed25519 私钥 base64: {e}")))?;
    let signing = SigningKey::from_pkcs8_der(&der)
        .map_err(|e| CryptoError::config(format!("Ed25519 私钥 PKCS8 解析: {e}")))?;
    let sig = signing.sign(content.as_bytes());
    Ok(b64_encode(&sig.to_bytes()))
}

/// 业务作用: Ed25519 公钥验签。出错即 false。对照 原实现 `verifyEd25519`。
///
/// # 参数
/// - `content`: 原始 UTF-8 内容,必须与签名时输入一致。
/// - `sign_b64`: Base64 编码的 64 字节 Ed25519 签名。
/// - `public_key_b64`: Base64 编码的 X.509/SPKI Ed25519 公钥 DER。
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
