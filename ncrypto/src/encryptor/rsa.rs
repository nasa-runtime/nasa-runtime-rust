//! RSA 非对称(对照 原实现 `encryptRSA*`/`decryptRSA*`/`signRSA`/`verifyRSA`/OAEP/`generateRSAKeyPair`)。
//!
//! 字节保真
//! - `"RSA"` = **PKCS1 v1.5**(非 OAEP);自动分段(encrypt 块=keyLen-11、decrypt 块=keyLen,顺序拼接)。
//! - 公钥 = Base64(X509 SPKI)、私钥 = Base64(PKCS8 DER)。
//! - **私钥加密 = PKCS1 type-1**:`rsa` crate 高层无此 API,用低层 raw 模幂 + 手工 `00 01 FF.. 00` padding;
//!   **公钥解密 = type-1 unpad**(同样低层)。两者是 原实现 `encryptRSAPrivate`/`decryptRSAPublic` 的对等物。
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

/// 解析 parse public 输入；用于把文本或语法节点转换为内部结构。
///
/// # 参数
/// - `b64`: PEM 或密钥材料的 Base64 内容。
fn parse_public(b64: &str) -> Result<RsaPublicKey> {
    let der = STANDARD
        .decode(b64)
        .map_err(|e| CryptoError::config(format!("RSA 公钥 base64: {e}")))?;
    RsaPublicKey::from_public_key_der(&der)
        .map_err(|e| CryptoError::config(format!("RSA 公钥 SPKI 解析失败: {e}")))
}

/// 解析 parse private 输入；用于把文本或语法节点转换为内部结构。
///
/// # 参数
/// - `b64`: PEM 或密钥材料的 Base64 内容。
fn parse_private(b64: &str) -> Result<RsaPrivateKey> {
    let der = STANDARD
        .decode(b64)
        .map_err(|e| CryptoError::config(format!("RSA 私钥 base64: {e}")))?;
    RsaPrivateKey::from_pkcs8_der(&der)
        .map_err(|e| CryptoError::config(format!("RSA 私钥 PKCS8 解析失败: {e}")))
}

/// 校验既有系统注入的 Base64 PKCS#8 RSA 私钥，并返回模数位数。
///
/// 该入口只做启动期结构与最低强度检查，不执行私钥运算，也不返回或记录密钥材料。新生成密钥仍应
/// 使用至少 2048 位；1024 位只为已经存在的 legacy Web 协议迁移期互通保留。
///
/// # 参数
///
/// - `private_key_b64`: Base64 编码的 PKCS#8 RSA 私钥 DER。
///
/// # 返回
///
/// 合法且模数不少于 1024 位时返回实际位数，否则返回不含密钥内容的配置错误。
pub fn validate_rsa_private_key(private_key_b64: &str) -> Result<usize> {
    let key = parse_private(private_key_b64)?;
    let bits = key.n().bits();
    if bits < 1024 {
        return Err(CryptoError::config("legacy RSA 私钥模数不能小于 1024 位"));
    }
    Ok(bits)
}

/// 按块大小切分;**空输入也产 1 个(空)块**——对齐 原实现 `rsaDoFinal` 对空输入仍 `doFinal(empty)` 产 1 块
/// (安全说明.1:否则空 payload 跨语言不一致,Rust 输出空串、原实现 输出 1 块)。
///
/// # 参数
/// - `data`: 待加密或待解密的原始字节。
/// - `block`: 每个 RSA 分块允许处理的最大字节数。
fn blocks_of(data: &[u8], block: usize) -> Vec<&[u8]> {
    if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(block).collect()
    }
}

/// 左补零到 `len` 字节(BigUint 的 big-endian 可能短于模数字节数)。
///
/// # 参数
/// - `bytes`: 原始字节切片。
/// - `len`: RSA 模数字节数,也是补齐后的目标长度。
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

/// RSA 公钥加密(字符串输入,Base64 输出,自动分段)。对照 原实现 `encryptRSAPublic`。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文;长文本会按 RSA 明文块大小自动分段。
/// - `public_key_b64`: Base64 编码的 X.509/SPKI RSA 公钥 DER。
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

/// RSA 私钥解密(Base64 密文输入,UTF-8 输出,自动分段)。对照 原实现 `decryptRSAPrivate`。
/// ⚠ PKCS1v1.5 解密受 RUSTSEC-2023-0071(Marvin)影响,见模块文档。
///
/// # 参数
/// - `cipher_b64`: Base64 编码的 RSA 密文;解码后长度必须是 RSA 模数字节数的整数倍。
/// - `private_key_b64`: Base64 编码的 PKCS8 RSA 私钥 DER。
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

/// 默认构建拒绝 PKCS1 v1.5 私钥解密；受控迁移必须显式开启 `legacy-rsa-private`。
///
/// # 参数
/// - `cipher_b64`: Base64 编码的历史 RSA 密文。
/// - `private_key_b64`: Base64 编码的 PKCS8 RSA 私钥 DER。
#[cfg(not(feature = "legacy-rsa-private"))]
pub fn decrypt_rsa_private(_cipher_b64: &str, _private_key_b64: &str) -> Result<String> {
    Err(CryptoError::config(
        "legacy RSA 私钥解密默认关闭；受控迁移需启用 ncrypto/legacy-rsa-private",
    ))
}

// ==================== ★ 私钥加密 / 公钥解密(PKCS1 type-1,低层 raw RSA) ====================

/// 单块 PKCS1 type-1 私钥加密:`c = (00 01 FF.. 00 ‖ data)^d mod n`,输出 k 字节。
///
/// # 参数
/// - `key`: 执行私钥指数运算的 RSA 私钥。
/// - `data`: 单个明文分块,长度必须不超过 `k - 11`。
/// - `k`: RSA 模数字节数。
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
    // c = em^d mod n
    let m = BigUint::from_bytes_be(&em);
    let c = m.modpow(key.d(), key.n());
    Ok(left_pad(c.to_bytes_be(), k))
}

/// 单块 type-1 公钥解密:`m = c^e mod n`,去 `00 01 FF.. 00` padding。
///
/// # 参数
/// - `key`: 执行公钥指数运算的 RSA 公钥。
/// - `block`: 单个 RSA 密文分块,长度应等于 `k`。
/// - `k`: RSA 模数字节数。
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

/// RSA **私钥加密**(字符串输入,Base64 输出,自动分段;PKCS1 type-1)。对照 原实现 `encryptRSAPrivate`。
/// ⚠ 私钥加密是**签名语义、非机密**(公钥人人可解),仅为互通保真。
///
/// # 参数
/// - `content`: 要做 type-1 私钥运算的 UTF-8 内容;长文本会自动分段。
/// - `private_key_b64`: Base64 编码的 PKCS8 RSA 私钥 DER。
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

/// 默认构建拒绝历史 RSA 私钥 type-1 运算；受控迁移必须显式开启 `legacy-rsa-private`。
///
/// # 参数
/// - `content`: 待处理的历史协议文本。
/// - `private_key_b64`: Base64 编码的 PKCS8 RSA 私钥 DER。
#[cfg(not(feature = "legacy-rsa-private"))]
pub fn encrypt_rsa_private(_content: &str, _private_key_b64: &str) -> Result<String> {
    Err(CryptoError::config(
        "legacy RSA 私钥运算默认关闭；受控迁移需启用 ncrypto/legacy-rsa-private",
    ))
}

/// RSA **公钥解密**(Base64 密文输入,UTF-8 输出,自动分段;type-1 unpad)。对照 原实现 `decryptRSAPublic`。
///
/// # 参数
/// - `cipher_b64`: Base64 编码的 type-1 RSA 密文;解码后长度必须是 RSA 模数字节数的整数倍。
/// - `public_key_b64`: Base64 编码的 X.509/SPKI RSA 公钥 DER。
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

///RSA 密文合法性守卫——空密文 / 非完整 block 是非法密文(原实现 `cipher.doFinal` 会抛),
/// 不应解成空明文/截断。两个解密方向共用。
///
/// # 参数
/// - `ct`: 密文字节或密文文本。
/// - `block_size`: RSA 分块加解密的单块字节数。
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

/// RSA 私钥签名(SHA256withRSA,Base64 输出)。对照 原实现 `signRSA`。
///
/// # 参数
/// - `content`: 要签名的 UTF-8 内容。
/// - `private_key_b64`: Base64 编码的 PKCS8 RSA 私钥 DER。
pub fn sign_rsa(content: &str, private_key_b64: &str) -> Result<String> {
    let key = parse_private(private_key_b64)?;
    let sk = SigningKey::<Sha256>::new(key);
    let sig = sk.sign(content.as_bytes());
    Ok(b64_encode(&sig.to_bytes()))
}

/// RSA 公钥验签(SHA256withRSA)。出错即 false。对照 原实现 `verifyRSA`。
///
/// # 参数
/// - `content`: 原始 UTF-8 内容,必须与签名时输入一致。
/// - `sign_b64`: Base64 编码的 RSA 签名值。
/// - `public_key_b64`: Base64 编码的 X.509/SPKI RSA 公钥 DER。
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

/// 从 Base64(SPKI)RSA 公钥取模数 `n` 与公钥指数 `e` 的**大端字节**(供 JWKS 发布 `n`/`e`,或作
/// [`verify_rs256_components`] 的分量输入)。
///
/// # 参数
/// - `public_key_b64`: Base64 编码的 X.509/SPKI RSA 公钥 DER。
///
/// # 返回
/// `(modulus_be, exponent_be)` 两段大端字节(无前导零填充,与 JWKS `n`/`e` 的 base64url 解码结果对齐)。
pub fn rsa_public_components(public_key_b64: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = parse_public(public_key_b64)?;
    Ok((key.n().to_bytes_be(), key.e().to_bytes_be()))
}

/// RS256(RSASSA-PKCS1-v1_5 + SHA-256)**验签**,公钥用大端 `modulus`/`exponent` 字节(如 JWKS 的
/// `n`/`e` base64url 解码结果)。用于 OAuth Resource Server 校验 access token 的签名。
///
/// 出错即 `false`(密钥分量非法、签名格式错、验签不通过都返回 `false`,绝不 panic);`message` 是
/// JWT 的签名输入 `header_b64 "." payload_b64` 的原始字节(内部用 SHA-256 摘要)。
///
/// # 参数
/// - `message`: 被签名的原始字节(JWT 签名输入)。
/// - `signature`: 原始签名字节(JWT 第三段 base64url 解码结果)。
/// - `modulus_be`: RSA 模数 `n` 的大端字节。
/// - `exponent_be`: RSA 公钥指数 `e` 的大端字节。
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

/// 校验一组 JWKS RSA 公钥分量可构造为至少 2048 bit 的 RS256 公钥。
///
/// 只检查公钥结构和最低强度，不做验签。Resource Server 在发布 JWKS last-good 快照前调用本函数，
/// 避免把“base64url 可解码但数学上非法/过短”的 `n`/`e` 延迟到每个请求才发现。
pub fn validate_rs256_public_components(modulus_be: &[u8], exponent_be: &[u8]) -> bool {
    let n = BigUint::from_bytes_be(modulus_be);
    let e = BigUint::from_bytes_be(exponent_be);
    RsaPublicKey::new(n, e)
        .map(|key| key.n().bits() >= 2048)
        .unwrap_or(false)
}

// ==================== RSA-OAEP(SHA-256 / MGF1) ====================

/// RSA-OAEP 公钥加密(`RSA/ECB/OAEPWithSHA-256AndMGF1Padding`)。对照 原实现 `encryptRSAOAEP`。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文;OAEP 入口不做自动分段。
/// - `public_key_b64`: Base64 编码的 X.509/SPKI RSA 公钥 DER。
pub fn encrypt_rsa_oaep(content: &str, public_key_b64: &str) -> Result<String> {
    let key = parse_public(public_key_b64)?;
    let mut rng = rand::rngs::OsRng;
    let ct = key
        .encrypt(&mut rng, Oaep::new::<Sha256>(), content.as_bytes())
        .map_err(|e| CryptoError::encrypt(format!("RSA-OAEP 加密: {e}")))?;
    Ok(b64_encode(&ct))
}

/// RSA-OAEP 私钥解密。对照 原实现 `decryptRSAOAEP`。
///
/// # 参数
/// - `cipher_b64`: Base64 编码的 RSA-OAEP 密文。
/// - `private_key_b64`: Base64 编码的 PKCS8 RSA 私钥 DER。
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

/// 生成 RSA 公私钥对,返回 `(私钥 Base64(PKCS8), 公钥 Base64(SPKI))`。`bits` >= 2048(对照 原实现 校验)。
///
/// # 参数
/// - `bits`: RSA 模数位数;必须不小于 2048。
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
