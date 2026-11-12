//! RSA 非对称。
//!
//! 字节保真
//! - `"RSA"` = **PKCS1 v1.5**(非 OAEP);自动分段(encrypt 块=keyLen-11、decrypt 块=keyLen,顺序拼接)。
//! - 公钥 = Base64(X509 SPKI)、私钥 = Base64(PKCS8 DER)。
//! - **私钥加密 = PKCS1 type-1**:`rsa` crate 高层无此 API,用低层 raw 模幂 + 手工 `00 01 FF.. 00` padding;
//!   **公钥解密 = type-1 unpad**(同样低层)。这两种 type-1 运算仅用于明确选择的协议兼容。
//!
//! PKCS1v1.5 **私钥解密**受 RUSTSEC-2023-0071(Marvin)时序侧信道影响。默认构建不执行
//! 该路径；`decrypt_rsa_private` 与历史私钥 type-1 运算需要 `legacy-rsa-private` feature。

use super::b64_encode;
use crate::{CryptoError, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rsa::pkcs1v15::{Signature, SigningKey, VerifyingKey};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use rsa::signature::{SignatureEncoding, Signer, Verifier};
#[cfg(feature = "legacy-rsa-private")]
use rsa::traits::PrivateKeyParts;
use rsa::traits::PublicKeyParts;
use rsa::{BigUint, Oaep, Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use sha2::Sha256;

// ---------- 密钥解析(Base64 DER) ----------

/// 业务作用：从标准 Base64 SPKI DER 解析 RSA 公钥。
///
/// 参数说明：
/// - `b64`：不含 PEM 包装的 Base64 DER 文本。
///
/// 返回：成功返回公钥对象，Base64 或 DER 无效返回配置错误，不执行验签或独立的强度下限校验。
fn parse_public(b64: &str) -> Result<RsaPublicKey> {
    let der = STANDARD
        .decode(b64)
        .map_err(|e| CryptoError::config(format!("RSA 公钥 base64: {e}")))?;
    RsaPublicKey::from_public_key_der(&der)
        .map_err(|e| CryptoError::config(format!("RSA 公钥 SPKI 解析失败: {e}")))
}

/// 业务作用：从标准 Base64 PKCS8 DER 解析 RSA 私钥。
///
/// 参数说明：
/// - `b64`：不含 PEM 包装的 Base64 DER 文本。
///
/// 返回：成功返回私钥对象，Base64 或 DER 无效返回配置错误，不执行私钥运算。
fn parse_private(b64: &str) -> Result<RsaPrivateKey> {
    let der = STANDARD
        .decode(b64)
        .map_err(|e| CryptoError::config(format!("RSA 私钥 base64: {e}")))?;
    RsaPrivateKey::from_pkcs8_der(&der)
        .map_err(|e| CryptoError::config(format!("RSA 私钥 PKCS8 解析失败: {e}")))
}

/// 业务作用：检查既有 RSA 私钥编码和兼容入口允许的模数下限。
///
/// 参数说明：
/// - `private_key_b64`：标准 Base64 编码的 PKCS8 RSA 私钥 DER。
///
/// 返回：解析成功且模数至少 1024 位时返回实际位数，否则返回配置错误；不执行解密，不意味着允许生成同等长度的新密钥。
pub fn validate_rsa_private_key(private_key_b64: &str) -> Result<usize> {
    let key = parse_private(private_key_b64)?;
    let bits = key.n().bits();
    if bits < 1024 {
        return Err(CryptoError::config("legacy RSA 私钥模数不能小于 1024 位"));
    }
    Ok(bits)
}

/// 业务作用：按 RSA 单块容量切分输入，空输入仍保留一个空块参与 padding。
///
/// 返回：顺序不变的字节切片集合；非空输入按 block 容量分段。
///
/// 参数说明：
/// - `data`: 待加密或待解密的原始字节。
/// - `block`: 每个 RSA 分块允许处理的最大字节数。
fn blocks_of(data: &[u8], block: usize) -> Vec<&[u8]> {
    if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(block).collect()
    }
}

/// 业务作用：把模幂结果左补零到所需模数宽度。
///
/// 参数说明：
/// - `bytes`：大端无符号整数的当前字节表示。
/// - `len`：期望的最小字节宽度。
///
/// 返回：短于目标时返回左补零字节，否则原样返回；不截断超长输入。
fn left_pad(mut bytes: Vec<u8>, len: usize) -> Vec<u8> {
    if bytes.len() < len {
        let mut p = vec![0u8; len - bytes.len()];
        p.append(&mut bytes);
        p
    } else {
        bytes
    }
}

// ==================== 标准方向:公钥加密 / 私钥解密(PKCS1 v1.5 type-2,rsa 高层) ====================

/// 业务作用：用 RSA 公钥按 PKCS1 v1.5 type-2 分块加密 UTF-8 内容。
///
/// 参数说明：
/// - `content`：按模数字节数减十一自动分块的原始 UTF-8 内容。
/// - `public_key_b64`：标准 Base64 编码的 SPKI RSA 公钥 DER。
///
/// 返回：成功返回拼接密文块的标准 Base64；解析、模数预算或加密失败返回错误。空内容产生空密文，不附带分块身份或 AEAD 标签。
pub fn encrypt_rsa_public(content: &str, public_key_b64: &str) -> Result<String> {
    let key = parse_public(public_key_b64)?;
    let k = key.size();
    let block = k
        .checked_sub(11)
        .ok_or_else(|| CryptoError::config("RSA 模数过小"))?;
    let mut rng = rand::rngs::OsRng;
    let mut out = Vec::new();
    for chunk in blocks_of(content.as_bytes(), block.max(1)) {
        out.extend(
            key.encrypt(&mut rng, Pkcs1v15Encrypt, chunk)
                .map_err(|e| CryptoError::encrypt(format!("RSA 公钥加密: {e}")))?,
        );
    }
    Ok(b64_encode(&out))
}

/// 业务作用：在显式启用兼容能力时用 RSA 私钥解开 type-2 分块密文。
///
/// 参数说明：
/// - `cipher_b64`：非空且解码长度为模数宽度整数倍的 Base64 密文。
/// - `private_key_b64`：标准 Base64 编码的 PKCS8 RSA 私钥 DER。
///
/// 返回：成功返回拼接明文的 UTF-8 文本；解析、长度、填充、解密或 UTF-8 失败返回错误。此入口受 legacy-rsa-private 特性控制。
#[cfg(feature = "legacy-rsa-private")]
pub fn decrypt_rsa_private(cipher_b64: &str, private_key_b64: &str) -> Result<String> {
    let key = parse_private(private_key_b64)?;
    let k = key.size();
    let ct = STANDARD
        .decode(cipher_b64)
        .map_err(|e| CryptoError::decrypt(format!("RSA 密文 base64: {e}")))?;
    rsa_ciphertext_guard(&ct, k)?;
    let mut out = Vec::new();
    for block in ct.chunks(k) {
        out.extend(
            key.decrypt(Pkcs1v15Encrypt, block)
                .map_err(|e| CryptoError::decrypt(format!("RSA 私钥解密: {e}")))?,
        );
    }
    String::from_utf8(out).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// 业务作用：在未启用兼容私钥能力时拒绝旧分块解密入口。
///
/// 参数说明：
/// - `_cipher_b64`：未被处理的旧密文。
/// - `_private_key_b64`：未被解析的私钥文本。
///
/// 返回：始终返回要求启用 legacy-rsa-private 的配置错误，不执行任何密钥运算。
#[cfg(not(feature = "legacy-rsa-private"))]
pub fn decrypt_rsa_private(_cipher_b64: &str, _private_key_b64: &str) -> Result<String> {
    Err(CryptoError::config(
        "legacy RSA 私钥解密默认关闭；受控迁移需启用 ncrypto/legacy-rsa-private",
    ))
}

// ==================== ★ 私钥加密 / 公钥解密(PKCS1 type-1,低层 raw RSA) ====================

/// 业务作用：构造 type-1 填充并执行单块 RSA 私钥指数运算。
///
/// 参数说明：
/// - `key`：提供私钥指数及模数的 RSA 密钥。
/// - `data`：不超过模数宽度减十一的单块内容。
/// - `k`：调用方已校验且与模数相同的字节宽度。
///
/// 返回：成功返回左补齐至模数宽度的运算结果，数据过长返回加密错误；此运算可被公钥恢复，不提供机密性。
#[cfg(feature = "legacy-rsa-private")]
fn private_encrypt_block(key: &RsaPrivateKey, data: &[u8], k: usize) -> Result<Vec<u8>> {
    // type-1 padding: 至少 8 个 0xFF,故 data.len() <= k-11
    if data.len() > k.saturating_sub(11) {
        return Err(CryptoError::encrypt("RSA type-1 单块数据过长"));
    }
    let mut em = vec![0u8; k];
    em[0] = 0x00;
    em[1] = 0x01;
    let ps_end = k - data.len() - 1; // [2, ps_end) 填 0xFF,em[ps_end]=0x00
    for b in em.iter_mut().take(ps_end).skip(2) {
        *b = 0xFF;
    }
    em[ps_end] = 0x00;
    em[ps_end + 1..].copy_from_slice(data);
    // 用私钥指数对填充后的整数执行模幂，并按模数宽度编码结果。
    let m = BigUint::from_bytes_be(&em);
    let c = m.modpow(key.d(), key.n());
    Ok(left_pad(c.to_bytes_be(), k))
}

/// 业务作用：执行 RSA 公钥指数运算并检查 type-1 填充。
///
/// 参数说明：
/// - `key`：用于模幂恢复的 RSA 公钥。
/// - `block`：调用方已按模数宽度分好的字节块。
/// - `k`：与模数对应的输出字节宽度。
///
/// 返回：type-1 头部、最小填充长度及分隔符有效时返回内容，否则返回解密错误；不在此检查密文整数的规范范围。
fn public_decrypt_block(key: &RsaPublicKey, block: &[u8], k: usize) -> Result<Vec<u8>> {
    let m = BigUint::from_bytes_be(block).modpow(key.e(), key.n());
    let em = left_pad(m.to_bytes_be(), k);
    if em.len() < 11 || em[0] != 0x00 || em[1] != 0x01 {
        return Err(CryptoError::decrypt("RSA type-1 padding 头非法"));
    }
    let mut i = 2;
    while i < em.len() && em[i] == 0xFF {
        i += 1;
    }
    if i >= em.len() || em[i] != 0x00 || i < 10 {
        // 至少 8 个 0xFF(i>=10),再接 0x00
        return Err(CryptoError::decrypt("RSA type-1 padding 分隔符非法"));
    }
    Ok(em[i + 1..].to_vec())
}

/// 业务作用：在兼容能力开启时对 UTF-8 内容分块执行 RSA type-1 私钥运算。
///
/// 参数说明：
/// - `content`：按模数宽度减十一分块的内容。
/// - `private_key_b64`：标准 Base64 编码的 PKCS8 RSA 私钥 DER。
///
/// 返回：成功返回拼接块的 Base64，密钥或单块失败传播；公钥可恢复内容，不提供机密性。受 legacy-rsa-private 特性控制。
#[cfg(feature = "legacy-rsa-private")]
pub fn encrypt_rsa_private(content: &str, private_key_b64: &str) -> Result<String> {
    let key = parse_private(private_key_b64)?;
    let k = key.size();
    let block = k
        .checked_sub(11)
        .ok_or_else(|| CryptoError::config("RSA 模数过小"))?;
    let mut out = Vec::new();
    for chunk in blocks_of(content.as_bytes(), block.max(1)) {
        out.extend(private_encrypt_block(&key, chunk, k)?);
    }
    Ok(b64_encode(&out))
}

/// 业务作用：在默认构建中拒绝旧 RSA 私钥 type-1 运算。
///
/// 参数说明：
/// - `_content`：不会执行运算的旧协议内容。
/// - `_private_key_b64`：不会解析的私钥文本。
///
/// 返回：始终返回要求显式启用 legacy-rsa-private 的配置错误。
#[cfg(not(feature = "legacy-rsa-private"))]
pub fn encrypt_rsa_private(_content: &str, _private_key_b64: &str) -> Result<String> {
    Err(CryptoError::config(
        "legacy RSA 私钥运算默认关闭；受控迁移需启用 ncrypto/legacy-rsa-private",
    ))
}

/// 业务作用：用 RSA 公钥逐块恢复 type-1 包装的 UTF-8 内容。
///
/// 参数说明：
/// - `cipher_b64`：非空且长度符合模数分块要求的 Base64 文本。
/// - `public_key_b64`：标准 Base64 编码的 SPKI RSA 公钥 DER。
///
/// 返回：成功返回拼接明文；解析、分块、填充或 UTF-8 失败传播，不把此入口等同于标准消息签名验证。
pub fn decrypt_rsa_public(cipher_b64: &str, public_key_b64: &str) -> Result<String> {
    let key = parse_public(public_key_b64)?;
    let k = key.size();
    let ct = STANDARD
        .decode(cipher_b64)
        .map_err(|e| CryptoError::decrypt(format!("RSA 密文 base64: {e}")))?;
    rsa_ciphertext_guard(&ct, k)?;
    let mut out = Vec::new();
    for block in ct.chunks(k) {
        out.extend(public_decrypt_block(&key, block, k)?);
    }
    String::from_utf8(out).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// 业务作用：在 RSA 分块解密前拒绝空密文或不完整块。
///
/// 参数说明：
/// - `ct`：已完成 Base64 解码的原始密文。
/// - `block_size`：调用方必须保证非零的模数字节数。
///
/// 返回：密文非空且长度为块宽整数倍时成功，否则返回解密错误；不验证各块数学范围或填充。
fn rsa_ciphertext_guard(ct: &[u8], block_size: usize) -> Result<()> {
    if ct.is_empty() {
        return Err(CryptoError::decrypt("RSA 密文不能为空"));
    }
    if !ct.len().is_multiple_of(block_size) {
        return Err(CryptoError::decrypt(format!(
            "RSA 密文长度非法: {} 不是 block size {} 的整数倍",
            ct.len(),
            block_size
        )));
    }
    Ok(())
}

// ==================== 签名 / 验签(SHA256withRSA = PKCS1 v1.5) ====================

/// 业务作用：以 RSA 私钥对 UTF-8 内容生成 SHA-256 PKCS1 v1.5 签名。
///
/// 参数说明：
/// - `content`：要签名的原始 UTF-8 文本。
/// - `private_key_b64`：标准 Base64 编码的 PKCS8 RSA 私钥 DER。
///
/// 返回：返回签名字节的标准 Base64；密钥解析失败传播，不进行内容字符归一化。
pub fn sign_rsa(content: &str, private_key_b64: &str) -> Result<String> {
    let key = parse_private(private_key_b64)?;
    let sk = SigningKey::<Sha256>::new(key);
    let sig = sk.sign(content.as_bytes());
    Ok(b64_encode(&sig.to_bytes()))
}

/// 业务作用：以 RSA 公钥验证 UTF-8 内容的 SHA-256 PKCS1 v1.5 签名。
///
/// 参数说明：
/// - `content`：与签名时完全相同的 UTF-8 内容。
/// - `sign_b64`：标准 Base64 编码的签名字节。
/// - `public_key_b64`：标准 Base64 编码的 SPKI RSA 公钥 DER。
///
/// 返回：验证通过返回 true，编码、密钥、签名结构或验证失败统一返回 false。
pub fn verify_rsa(content: &str, sign_b64: &str, public_key_b64: &str) -> bool {
    let run = || -> Result<bool> {
        let key = parse_public(public_key_b64)?;
        let vk = VerifyingKey::<Sha256>::new(key);
        let sig_bytes = STANDARD
            .decode(sign_b64)
            .map_err(|e| CryptoError::decrypt(format!("签名 base64: {e}")))?;
        let sig = Signature::try_from(sig_bytes.as_slice())
            .map_err(|e| CryptoError::decrypt(format!("签名格式: {e}")))?;
        Ok(vk.verify(content.as_bytes(), &sig).is_ok())
    };
    run().unwrap_or(false)
}

/// 业务作用：提取 RSA 公钥的模数和指数供协议交换使用。
///
/// 参数说明：
/// - `public_key_b64`：标准 Base64 编码的 SPKI RSA 公钥 DER。
///
/// 返回：返回模数和指数的无前导填充大端字节；Base64 或公钥 DER 解析失败返回错误。
pub fn rsa_public_components(public_key_b64: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = parse_public(public_key_b64)?;
    Ok((key.n().to_bytes_be(), key.e().to_bytes_be()))
}

/// 业务作用：从大端公钥分量构造 RSA 公钥并验证 RS256 签名。
///
/// 参数说明：
/// - `message`：原始签名输入字节，例如 JWT 前两段的点号拼接。
/// - `signature`：已完成协议解码的签名字节。
/// - `modulus_be`：RSA 模数的无符号大端字节。
/// - `exponent_be`：RSA 公钥指数的无符号大端字节。
///
/// 返回：公钥、签名格式及验证均成功时为 true，其余返回 false；本入口不独立施加 2048 位模数下限或验证 JWT claims。
pub fn verify_rs256_components(
    message: &[u8],
    signature: &[u8],
    modulus_be: &[u8],
    exponent_be: &[u8],
) -> bool {
    let run = || -> Result<bool> {
        let n = BigUint::from_bytes_be(modulus_be);
        let e = BigUint::from_bytes_be(exponent_be);
        let key = RsaPublicKey::new(n, e)
            .map_err(|err| CryptoError::config(format!("RSA 公钥分量非法: {err}")))?;
        let vk = VerifyingKey::<Sha256>::new(key);
        let sig = Signature::try_from(signature)
            .map_err(|err| CryptoError::decrypt(format!("签名格式: {err}")))?;
        Ok(vk.verify(message, &sig).is_ok())
    };
    run().unwrap_or(false)
}

/// 业务作用：在发布公钥分量前检查 RSA 结构及模数长度。
///
/// 参数说明：
/// - `modulus_be`：RSA 模数的无符号大端字节。
/// - `exponent_be`：RSA 公钥指数的无符号大端字节。
///
/// 返回：可构造为公钥且模数至少 2048 位时返回 true，否则 false；不验证签名或主体身份。
pub fn validate_rs256_public_components(modulus_be: &[u8], exponent_be: &[u8]) -> bool {
    let n = BigUint::from_bytes_be(modulus_be);
    let e = BigUint::from_bytes_be(exponent_be);
    RsaPublicKey::new(n, e)
        .map(|key| key.n().bits() >= 2048)
        .unwrap_or(false)
}

// ==================== RSA-OAEP(SHA-256 / MGF1) ====================

/// 业务作用：用 RSA-OAEP SHA-256 对单段 UTF-8 内容加密。
///
/// 参数说明：
/// - `content`：必须落在单个 OAEP 块容量内的 UTF-8 内容。
/// - `public_key_b64`：标准 Base64 编码的 SPKI RSA 公钥 DER。
///
/// 返回：返回密文的标准 Base64；公钥解析、容量或加密失败返回错误，不自动分块。
pub fn encrypt_rsa_oaep(content: &str, public_key_b64: &str) -> Result<String> {
    let key = parse_public(public_key_b64)?;
    let mut rng = rand::rngs::OsRng;
    let ct = key
        .encrypt(&mut rng, Oaep::new::<Sha256>(), content.as_bytes())
        .map_err(|e| CryptoError::encrypt(format!("RSA-OAEP 加密: {e}")))?;
    Ok(b64_encode(&ct))
}

/// 业务作用：用 RSA 私钥解开单块 OAEP SHA-256 密文。
///
/// 参数说明：
/// - `cipher_b64`：标准 Base64 编码的 OAEP 密文。
/// - `private_key_b64`：标准 Base64 编码的 PKCS8 RSA 私钥 DER。
///
/// 返回：成功返回 UTF-8 明文；密钥、Base64、OAEP 解密或 UTF-8 失败返回错误，不自动分块。
pub fn decrypt_rsa_oaep(cipher_b64: &str, private_key_b64: &str) -> Result<String> {
    let key = parse_private(private_key_b64)?;
    let ct = STANDARD
        .decode(cipher_b64)
        .map_err(|e| CryptoError::decrypt(format!("OAEP 密文 base64: {e}")))?;
    let pt = key
        .decrypt(Oaep::new::<Sha256>(), &ct)
        .map_err(|e| CryptoError::decrypt(format!("RSA-OAEP 解密: {e}")))?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

// ==================== 密钥对生成 ====================

/// 业务作用：按显式模数位数生成 RSA 密钥对并编码。
///
/// 参数说明：
/// - `bits`：不能小于 2048 的目标模数位数，计算预算由调用方限制。
///
/// 返回：返回私钥 PKCS8 和公钥 SPKI DER 的 Base64；位数过小、生成或编码失败返回错误，入口不设置位数上限。
pub fn generate_rsa_key_pair(bits: usize) -> Result<(String, String)> {
    if bits < 2048 {
        return Err(CryptoError::config(format!(
            "RSA keysize 不能小于 2048,当前: {bits}"
        )));
    }
    let mut rng = rand::rngs::OsRng;
    let private_key = RsaPrivateKey::new(&mut rng, bits)
        .map_err(|e| CryptoError::encrypt(format!("RSA 生成: {e}")))?;
    let public_key = RsaPublicKey::from(&private_key);
    let priv_b64 = b64_encode(
        private_key
            .to_pkcs8_der()
            .map_err(|e| CryptoError::encrypt(format!("私钥编码: {e}")))?
            .as_bytes(),
    );
    let pub_b64 = b64_encode(
        public_key
            .to_public_key_der()
            .map_err(|e| CryptoError::encrypt(format!("公钥编码: {e}")))?
            .as_bytes(),
    );
    Ok((priv_b64, pub_b64))
}
