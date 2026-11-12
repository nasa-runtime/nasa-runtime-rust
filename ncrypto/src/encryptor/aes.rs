//! AES 对称加密。ECB / CBC / GCM。
//!
//! 字节保真
//! - **AES key = key 字符串的 UTF-8 字节,不解码**;长度必须 16/24/32(决定 AES-128/192/256)。
//! - ECB/CBC 用 **PKCS5Padding(= PKCS7,16 字节块)**。
//! - **CBC 默认变体 IV = key 字节**;独立 IV 变体 IV = iv 串 UTF-8 字节。
//! - **GCM 布局 = `Base64(IV[12] ‖ ciphertext ‖ tag[16])`**,IV 随机(OsRng);tag 128 bit。
//! - HEX 输出/输入用**大写** hex。

use super::{b64_decode, b64_encode, hex_decode, hex_upper};
use crate::{AesMode, CryptoError, EncOutput, Result};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::Aead;
use aes_gcm::{AeadCore, Aes128Gcm, Aes256Gcm, AesGcm, KeyInit as GcmKeyInit, Nonce};
use cipher::block_padding::Pkcs7;
use cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};

type Aes192Gcm = AesGcm<Aes192, aes_gcm::aes::cipher::consts::U12>;

const GCM_IV_LEN: usize = 12;

// ---------- ECB ----------

/// 业务作用：用 AES-ECB 和 PKCS7 填充加密原始字节。
///
/// 参数说明：
/// - `key`：16、24 或 32 字节的 AES 密钥。
/// - `data`：需要加密的原始明文。
///
/// 返回：返回包含填充后的密文字节；密钥长度非法返回配置或加密错误，此模式不生成 IV 或认证标签。
fn ecb_encrypt(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    macro_rules! run {
        ($a:ty) => {
            ecb::Encryptor::<$a>::new_from_slice(key)
                .map_err(|_| CryptoError::encrypt("AES key 长度非法"))?
                .encrypt_padded_vec_mut::<Pkcs7>(data)
        };
    }
    Ok(match key.len() {
        16 => run!(Aes128),
        24 => run!(Aes192),
        32 => run!(Aes256),
        n => {
            return Err(CryptoError::config(format!(
                "AES key 必须 16/24/32 字节,实际 {n}"
            )))
        }
    })
}

/// 业务作用：用 AES-ECB 解密并移除 PKCS7 填充。
///
/// 参数说明：
/// - `key`：与加密相同的 16、24 或 32 字节密钥。
/// - `ct`：完整的分组密文字节。
///
/// 返回：返回去填充明文；密钥长度、分组长度或填充无效返回错误，不构成认证验证。
fn ecb_decrypt(key: &[u8], ct: &[u8]) -> Result<Vec<u8>> {
    macro_rules! run {
        ($a:ty) => {
            ecb::Decryptor::<$a>::new_from_slice(key)
                .map_err(|_| CryptoError::decrypt("AES key 长度非法"))?
                .decrypt_padded_vec_mut::<Pkcs7>(ct)
                .map_err(|e| CryptoError::decrypt(format!("AES-ECB 解密失败: {e}")))?
        };
    }
    Ok(match key.len() {
        16 => run!(Aes128),
        24 => run!(Aes192),
        32 => run!(Aes256),
        n => {
            return Err(CryptoError::config(format!(
                "AES key 必须 16/24/32 字节,实际 {n}"
            )))
        }
    })
}

// ---------- CBC ----------

/// 业务作用：使用独立 IV 执行 AES-CBC 和 PKCS7 填充。
///
/// 参数说明：
/// - `key`：16、24 或 32 字节 AES 密钥。
/// - `iv`：必须为 16 字节的初始化向量。
/// - `data`：待加密的原始字节。
///
/// 返回：成功返回密文字节；密钥或 IV 长度非法返回错误，不附加 IV 或认证标签。
fn cbc_encrypt(key: &[u8], iv: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    macro_rules! run {
        ($a:ty) => {
            cbc::Encryptor::<$a>::new_from_slices(key, iv)
                .map_err(|_| CryptoError::encrypt("AES key/iv 长度非法"))?
                .encrypt_padded_vec_mut::<Pkcs7>(data)
        };
    }
    Ok(match key.len() {
        16 => run!(Aes128),
        24 => run!(Aes192),
        32 => run!(Aes256),
        n => {
            return Err(CryptoError::config(format!(
                "AES key 必须 16/24/32 字节,实际 {n}"
            )))
        }
    })
}

/// 业务作用：用给定 IV 解密 AES-CBC 并验证 PKCS7 填充。
///
/// 参数说明：
/// - `key`：与加密一致的 AES 密钥。
/// - `iv`：与加密一致的 16 字节初始化向量。
/// - `ct`：完整的 CBC 密文字节。
///
/// 返回：成功返回去填充明文；密钥、IV、密文长度或填充非法时返回错误，不提供真实性认证。
fn cbc_decrypt(key: &[u8], iv: &[u8], ct: &[u8]) -> Result<Vec<u8>> {
    macro_rules! run {
        ($a:ty) => {
            cbc::Decryptor::<$a>::new_from_slices(key, iv)
                .map_err(|_| CryptoError::decrypt("AES key/iv 长度非法"))?
                .decrypt_padded_vec_mut::<Pkcs7>(ct)
                .map_err(|e| CryptoError::decrypt(format!("AES-CBC 解密失败: {e}")))?
        };
    }
    Ok(match key.len() {
        16 => run!(Aes128),
        24 => run!(Aes192),
        32 => run!(Aes256),
        n => {
            return Err(CryptoError::config(format!(
                "AES key 必须 16/24/32 字节,实际 {n}"
            )))
        }
    })
}

/// 业务作用：把密文字节转换为调用方选择的输出文本。
///
/// 参数说明：
/// - `bytes`：已加密的原始密文字节。
/// - `enc`：选择标准 Base64 或大写 HEX 的输出编码。
///
/// 返回：返回指定编码文本，不改变原密文字节的含义。
fn encode_out(bytes: &[u8], enc: EncOutput) -> String {
    match enc {
        EncOutput::Base64 => b64_encode(bytes),
        EncOutput::Hex => hex_upper(bytes), // HEX 输出固定使用大写，保持线格式一致。
    }
}

/// 业务作用：按选定格式还原密文文本的原始字节。
///
/// 参数说明：
/// - `s`：需要解码的密文文本。
/// - `enc`：必须与编码时一致的 Base64 或 HEX 格式。
///
/// 返回：成功返回原始字节，非法编码返回解码错误，不执行 AES 解密。
fn decode_in(s: &str, enc: EncOutput) -> Result<Vec<u8>> {
    match enc {
        EncOutput::Base64 => b64_decode(s),
        EncOutput::Hex => hex_decode(s),
    }
}

// ==================== AES-ECB(默认 encryptAES) ====================

/// 业务作用：以 AES-ECB 和 PKCS7 填充加密，使用 Base64 文本输出。
///
/// 参数说明：
/// - `content`：待加密的 UTF-8 明文。
/// - `key`：按 UTF-8 使用的 16、24 或 32 字节密钥。
///
/// 返回：返回标准 Base64密文；密钥或 IV 长度不符返回错误，不自动生成或附加认证材料。
pub fn encrypt_aes(content: &str, key: &str) -> Result<String> {
    Ok(encode_out(
        &ecb_encrypt(key.as_bytes(), content.as_bytes())?,
        EncOutput::Base64,
    ))
}

/// 业务作用：以 AES-ECB 和 PKCS7 填充解密，使用 Base64 文本输入。
///
/// 参数说明：
/// - `cipher`：Base64 编码的完整密文。
/// - `key`：按 UTF-8 使用的 16、24 或 32 字节密钥。
///
/// 返回：返回通过 UTF-8 检查的去填充明文；编码、密钥、IV、填充或 UTF-8 不符返回错误，不提供认证保证。
pub fn decrypt_aes(cipher: &str, key: &str) -> Result<String> {
    let pt = ecb_decrypt(key.as_bytes(), &b64_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// 业务作用：以 AES-ECB 和 PKCS7 填充加密，使用 HEX 文本输出。
///
/// 参数说明：
/// - `content`：待加密的 UTF-8 明文。
/// - `key`：按 UTF-8 使用的 16、24 或 32 字节密钥。
///
/// 返回：返回大写 HEX密文；密钥或 IV 长度不符返回错误，不自动生成或附加认证材料。
pub fn encrypt_aes_hex(content: &str, key: &str) -> Result<String> {
    Ok(encode_out(
        &ecb_encrypt(key.as_bytes(), content.as_bytes())?,
        EncOutput::Hex,
    ))
}

/// 业务作用：以 AES-ECB 和 PKCS7 填充解密，使用 HEX 文本输入。
///
/// 参数说明：
/// - `cipher`：HEX 编码的完整密文。
/// - `key`：按 UTF-8 使用的 16、24 或 32 字节密钥。
///
/// 返回：返回通过 UTF-8 检查的去填充明文；编码、密钥、IV、填充或 UTF-8 不符返回错误，不提供认证保证。
pub fn decrypt_aes_hex(cipher: &str, key: &str) -> Result<String> {
    let pt = ecb_decrypt(key.as_bytes(), &hex_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

// ==================== AES-CBC ====================

/// 业务作用：以 AES-CBC 和 PKCS7 填充加密，使用 Base64 文本输出。
///
/// 参数说明：
/// - `content`：待加密的 UTF-8 明文。
/// - `key`：同时用作密钥和 IV 的 UTF-8 字节，必须恰好 16 字节。
///
/// 返回：返回标准 Base64密文；密钥或 IV 长度不符返回错误，不自动生成或附加认证材料。
pub fn encrypt_aes_cbc(content: &str, key: &str) -> Result<String> {
    Ok(b64_encode(&cbc_encrypt(
        key.as_bytes(),
        key.as_bytes(),
        content.as_bytes(),
    )?))
}

/// 业务作用：以 AES-CBC 和 PKCS7 填充加密，使用 Base64 文本输出。
///
/// 参数说明：
/// - `content`：待加密的 UTF-8 明文。
/// - `key`：按 UTF-8 使用的 16、24 或 32 字节密钥。
/// - `iv`：按 UTF-8 使用、与加密一致的 16 字节初始化向量。
///
/// 返回：返回标准 Base64密文；密钥或 IV 长度不符返回错误，不自动生成或附加认证材料。
pub fn encrypt_aes_cbc_iv(content: &str, key: &str, iv: &str) -> Result<String> {
    Ok(b64_encode(&cbc_encrypt(
        key.as_bytes(),
        iv.as_bytes(),
        content.as_bytes(),
    )?))
}

/// 业务作用：以 AES-CBC 和 PKCS7 填充解密，使用 Base64 文本输入。
///
/// 参数说明：
/// - `cipher`：Base64 编码的完整密文。
/// - `key`：同时用作密钥和 IV 的 UTF-8 字节，必须恰好 16 字节。
///
/// 返回：返回通过 UTF-8 检查的去填充明文；编码、密钥、IV、填充或 UTF-8 不符返回错误，不提供认证保证。
pub fn decrypt_aes_cbc(cipher: &str, key: &str) -> Result<String> {
    let pt = cbc_decrypt(key.as_bytes(), key.as_bytes(), &b64_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// 业务作用：以 AES-CBC 和 PKCS7 填充解密，使用 Base64 文本输入。
///
/// 参数说明：
/// - `cipher`：Base64 编码的完整密文。
/// - `key`：按 UTF-8 使用的 16、24 或 32 字节密钥。
/// - `iv`：按 UTF-8 使用、与加密一致的 16 字节初始化向量。
///
/// 返回：返回通过 UTF-8 检查的去填充明文；编码、密钥、IV、填充或 UTF-8 不符返回错误，不提供认证保证。
pub fn decrypt_aes_cbc_iv(cipher: &str, key: &str, iv: &str) -> Result<String> {
    let pt = cbc_decrypt(key.as_bytes(), iv.as_bytes(), &b64_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

// ==================== AES-GCM(认证加密,推荐新用途) ====================

/// 业务作用：以随机 nonce 对 UTF-8 明文执行 AES-GCM 认证加密。
///
/// 参数说明：
/// - `content`：待认证加密的 UTF-8 明文。
/// - `key`：按 UTF-8 原字节使用的 16、24 或 32 字节密钥。
///
/// 返回：返回标准 Base64 编码的 12 字节 nonce、密文和认证标签；密钥长度或加密失败返回错误，nonce 随每次操作生成。
pub fn encrypt_aes_gcm(content: &str, key: &str) -> Result<String> {
    let key = key.as_bytes();
    let nonce_bytes = {
        macro_rules! gen {
            ($t:ty) => {{
                let c = <$t>::new_from_slice(key)
                    .map_err(|_| CryptoError::config("AES key 长度非法"))?;
                let nonce = <$t as AeadCore>::generate_nonce(&mut rand::rngs::OsRng);
                let ct = c
                    .encrypt(&nonce, content.as_bytes())
                    .map_err(|e| CryptoError::encrypt(format!("AES-GCM: {e}")))?;
                (nonce.to_vec(), ct)
            }};
        }
        let (iv, ct): (Vec<u8>, Vec<u8>) = match key.len() {
            16 => gen!(Aes128Gcm),
            24 => gen!(Aes192Gcm),
            32 => gen!(Aes256Gcm),
            n => {
                return Err(CryptoError::config(format!(
                    "AES key 必须 16/24/32 字节,实际 {n}"
                )))
            }
        };
        let mut out = Vec::with_capacity(iv.len() + ct.len());
        out.extend_from_slice(&iv);
        out.extend_from_slice(&ct);
        out
    };
    Ok(b64_encode(&nonce_bytes))
}

/// 业务作用：从密文头部取得 nonce，验证 AES-GCM 标签并解码明文。
///
/// 参数说明：
/// - `cipher`：Base64 编码的 nonce、密文和标签组合。
/// - `key`：与加密一致的 UTF-8 密钥字节。
///
/// 返回：认证和 UTF-8 检查通过才返回明文；编码、头部长度、密钥、标签或 UTF-8 失败返回错误。
pub fn decrypt_aes_gcm(cipher: &str, key: &str) -> Result<String> {
    let key = key.as_bytes();
    let decoded = b64_decode(cipher)?;
    if decoded.len() < GCM_IV_LEN {
        return Err(CryptoError::decrypt("AES-GCM 密文过短(无 IV)"));
    }
    let (iv, ct) = decoded.split_at(GCM_IV_LEN);
    let nonce_bytes: [u8; GCM_IV_LEN] = iv
        .try_into()
        .map_err(|_| CryptoError::decrypt("AES-GCM IV 长度非法"))?;
    let nonce = Nonce::from(nonce_bytes);
    macro_rules! dec {
        ($t:ty) => {{
            let c =
                <$t>::new_from_slice(key).map_err(|_| CryptoError::config("AES key 长度非法"))?;
            c.decrypt(&nonce, ct)
                .map_err(|e| CryptoError::decrypt(format!("AES-GCM 认证/解密失败: {e}")))?
        }};
    }
    let pt: Vec<u8> = match key.len() {
        16 => dec!(Aes128Gcm),
        24 => dec!(Aes192Gcm),
        32 => dec!(Aes256Gcm),
        n => {
            return Err(CryptoError::config(format!(
                "AES key 必须 16/24/32 字节,实际 {n}"
            )))
        }
    };
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

// ==================== 通用(自定义 mode + 编码) ====================

/// 业务作用：按指定分组模式和文本格式加密 UTF-8 内容。
///
/// 参数说明：
/// - `content`：待加密的 UTF-8 明文。
/// - `key`：AES 密钥原字节，CBC 分支还将其用作 IV。
/// - `mode`：选择 ECB 或 CBC 的公开模式枚举，实际使用 PKCS7 填充。
/// - `enc`：选择 Base64 或大写 HEX 输出。
///
/// 返回：返回选定编码的密文；密钥或 IV 长度非法返回错误。CBC 因复用密钥作 16 字节 IV，仅 16 字节密钥可用，不生成认证标签。
pub fn encrypt_aes_with(content: &str, key: &str, mode: AesMode, enc: EncOutput) -> Result<String> {
    let ct = match mode {
        AesMode::EcbPkcs5 => ecb_encrypt(key.as_bytes(), content.as_bytes())?,
        AesMode::CbcPkcs5 => cbc_encrypt(key.as_bytes(), key.as_bytes(), content.as_bytes())?,
    };
    Ok(encode_out(&ct, enc))
}

/// 业务作用：按指定模式与文本编码解密并检查 UTF-8 明文。
///
/// 参数说明：
/// - `cipher`：按 enc 编码的密文。
/// - `key`：与加密相同的密钥，CBC 同时将其用作 IV。
/// - `mode`：与加密相同的 ECB 或 CBC 模式。
/// - `enc`：密文采用的 Base64 或 HEX 编码。
///
/// 返回：成功返回 UTF-8 明文；编码、密钥、IV、填充或 UTF-8 失败返回错误。复用密钥作 IV 的 CBC 仅接受 16 字节密钥。
pub fn decrypt_aes_with(cipher: &str, key: &str, mode: AesMode, enc: EncOutput) -> Result<String> {
    let raw = decode_in(cipher, enc)?;
    let pt = match mode {
        AesMode::EcbPkcs5 => ecb_decrypt(key.as_bytes(), &raw)?,
        AesMode::CbcPkcs5 => cbc_decrypt(key.as_bytes(), key.as_bytes(), &raw)?,
    };
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}
