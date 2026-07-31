//! AES 对称加密(对照 原实现 `encryptAES*`/`decryptAES*`)。ECB / CBC / GCM。
//!
//! 字节保真
//! - **AES key = key 字符串的 UTF-8 字节,不解码**;长度必须 16/24/32(决定 AES-128/192/256)。
//! - ECB/CBC 用 **PKCS5Padding(= PKCS7,16 字节块)**。
//! - **CBC 默认变体 IV = key 字节**;独立 IV 变体 IV = iv 串 UTF-8 字节。
//! - **GCM 布局 = `Base64(IV[12] ‖ ciphertext ‖ tag[16])`**,IV 随机(OsRng);tag 128 bit。
//! - HEX 输出/输入用**大写** hex(对照 原实现 `toHex`)。

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

/// 执行 AES ECB 加密；用于兼容只传密钥的分组加密场景。
///
/// # 参数
/// - `key`: AES 密钥字节,长度必须为 16、24 或 32。
/// - `data`: 待加密的明文字节。
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

/// 执行 AES ECB 解密；用于还原只传密钥的密文数据。
///
/// # 参数
/// - `key`: AES 密钥字节,长度必须为 16、24 或 32。
/// - `ct`: 密文字节或密文文本。
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

/// 执行 AES CBC 加密；用于带初始化向量的分组加密场景。
///
/// # 参数
/// - `key`: AES 密钥字节,长度必须为 16、24 或 32。
/// - `iv`: AES-CBC 使用的初始化向量。
/// - `data`: 待加密的明文字节。
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

/// 执行 AES CBC 解密；用于还原带初始化向量的密文数据。
///
/// # 参数
/// - `key`: AES 密钥字节,长度必须为 16、24 或 32。
/// - `iv`: AES-CBC 使用的初始化向量。
/// - `ct`: 密文字节或密文文本。
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

/// 编码 encode out 数据；用于生成可传输或可存储的字节。
///
/// # 参数
/// - `bytes`: 原始字节切片。
/// - `enc`: 待解密的密文字节。
fn encode_out(bytes: &[u8], enc: EncOutput) -> String {
    match enc {
        EncOutput::Base64 => b64_encode(bytes),
        EncOutput::Hex => hex_upper(bytes), // 对照 原实现 toHex 大写
    }
}

/// 解码 decode in 数据；用于把输入还原为内部字节或结构。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
/// - `enc`: 待解密的密文字节。
fn decode_in(s: &str, enc: EncOutput) -> Result<Vec<u8>> {
    match enc {
        EncOutput::Base64 => b64_decode(s),
        EncOutput::Hex => hex_decode(s),
    }
}

// ==================== AES-ECB(默认 encryptAES) ====================

/// AES-ECB 加密(PKCS5,Base64 输出)。对照 原实现 `encryptAES(content,key)`。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文。
/// - `key`: AES 密钥字符串,按 UTF-8 字节直接使用;长度必须是 16/24/32 字节。
pub fn encrypt_aes(content: &str, key: &str) -> Result<String> {
    Ok(encode_out(
        &ecb_encrypt(key.as_bytes(), content.as_bytes())?,
        EncOutput::Base64,
    ))
}

/// AES-ECB 解密(PKCS5,Base64 输入)。
///
/// # 参数
/// - `cipher`: Base64 编码的 AES-ECB 密文。
/// - `key`: AES 密钥字符串,必须与加密时完全一致。
pub fn decrypt_aes(cipher: &str, key: &str) -> Result<String> {
    let pt = ecb_decrypt(key.as_bytes(), &b64_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// AES-ECB 加密(PKCS5,**大写 HEX** 输出)。对照 原实现 `encryptAESHex`。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文。
/// - `key`: AES 密钥字符串,按 UTF-8 字节直接使用;长度必须是 16/24/32 字节。
pub fn encrypt_aes_hex(content: &str, key: &str) -> Result<String> {
    Ok(encode_out(
        &ecb_encrypt(key.as_bytes(), content.as_bytes())?,
        EncOutput::Hex,
    ))
}

/// AES-ECB 解密(PKCS5,HEX 输入,大小写均可)。
///
/// # 参数
/// - `cipher`: HEX 编码的 AES-ECB 密文,大小写均可。
/// - `key`: AES 密钥字符串,必须与加密时完全一致。
pub fn decrypt_aes_hex(cipher: &str, key: &str) -> Result<String> {
    let pt = ecb_decrypt(key.as_bytes(), &hex_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

// ==================== AES-CBC ====================

/// AES-CBC 加密(PKCS5,**IV = key 字节**,Base64 输出)。对照 原实现 `encryptAESCBC(content,key)`。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文。
/// - `key`: AES 密钥字符串,同时作为 CBC IV 的字节来源;长度必须是 16/24/32 字节。
pub fn encrypt_aes_cbc(content: &str, key: &str) -> Result<String> {
    Ok(b64_encode(&cbc_encrypt(
        key.as_bytes(),
        key.as_bytes(),
        content.as_bytes(),
    )?))
}

/// AES-CBC 加密(PKCS5,独立 IV = iv 串 UTF-8 字节,Base64 输出)。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文。
/// - `key`: AES 密钥字符串,按 UTF-8 字节直接使用;长度必须是 16/24/32 字节。
/// - `iv`: CBC 初始化向量字符串,按 UTF-8 字节直接使用;长度必须匹配 AES 块大小 16 字节。
pub fn encrypt_aes_cbc_iv(content: &str, key: &str, iv: &str) -> Result<String> {
    Ok(b64_encode(&cbc_encrypt(
        key.as_bytes(),
        iv.as_bytes(),
        content.as_bytes(),
    )?))
}

/// AES-CBC 解密(PKCS5,IV = key 字节,Base64 输入)。
///
/// # 参数
/// - `cipher`: Base64 编码的 AES-CBC 密文。
/// - `key`: AES 密钥字符串,同时作为 CBC IV 的字节来源;必须与加密时完全一致。
pub fn decrypt_aes_cbc(cipher: &str, key: &str) -> Result<String> {
    let pt = cbc_decrypt(key.as_bytes(), key.as_bytes(), &b64_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

/// AES-CBC 解密(PKCS5,独立 IV,Base64 输入)。
///
/// # 参数
/// - `cipher`: Base64 编码的 AES-CBC 密文。
/// - `key`: AES 密钥字符串,必须与加密时完全一致。
/// - `iv`: CBC 初始化向量字符串,必须与加密时完全一致。
pub fn decrypt_aes_cbc_iv(cipher: &str, key: &str, iv: &str) -> Result<String> {
    let pt = cbc_decrypt(key.as_bytes(), iv.as_bytes(), &b64_decode(cipher)?)?;
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}

// ==================== AES-GCM(认证加密,推荐新用途) ====================

/// AES-GCM 加密。输出 `Base64(IV[12] ‖ ciphertext ‖ tag[16])`,IV 随机(OsRng)。对照 原实现 `encryptAESGCM`。
///
/// # 参数
/// - `content`: 要认证加密的 UTF-8 明文。
/// - `key`: AES-GCM 密钥字符串,按 UTF-8 字节直接使用;长度必须是 16/24/32 字节。
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

/// AES-GCM 解密(从密文头部取 IV)。对照 原实现 `decryptAESGCM`。
///
/// # 参数
/// - `cipher`: Base64 编码的 `IV + 密文 + tag` 数据。
/// - `key`: AES-GCM 密钥字符串,必须与加密时完全一致。
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

/// AES 加密(自定义 ECB/CBC + Base64/HEX 输出)。对照 原实现 通用 `encryptAES(c,key,transformation,mode)`。
/// CBC 模式 IV = key 字节(默认变体)。
///
/// # 参数
/// - `content`: 要加密的 UTF-8 明文。
/// - `key`: AES 密钥字符串,按 UTF-8 字节直接使用;CBC 模式也用它作为 IV。
/// - `mode`: AES 分组模式,决定走 ECB 还是 CBC。
/// - `enc`: 输出编码格式,决定返回 Base64 还是 HEX。
pub fn encrypt_aes_with(content: &str, key: &str, mode: AesMode, enc: EncOutput) -> Result<String> {
    let ct = match mode {
        AesMode::EcbPkcs5 => ecb_encrypt(key.as_bytes(), content.as_bytes())?,
        AesMode::CbcPkcs5 => cbc_encrypt(key.as_bytes(), key.as_bytes(), content.as_bytes())?,
    };
    Ok(encode_out(&ct, enc))
}

/// AES 解密(自定义 ECB/CBC + Base64/HEX 输入)。
///
/// # 参数
/// - `cipher`: 按 `enc` 编码的 AES 密文。
/// - `key`: AES 密钥字符串,必须与加密时完全一致;CBC 模式也用它作为 IV。
/// - `mode`: AES 分组模式,必须与加密时一致。
/// - `enc`: 输入编码格式,决定按 Base64 还是 HEX 解码。
pub fn decrypt_aes_with(cipher: &str, key: &str, mode: AesMode, enc: EncOutput) -> Result<String> {
    let raw = decode_in(cipher, enc)?;
    let pt = match mode {
        AesMode::EcbPkcs5 => ecb_decrypt(key.as_bytes(), &raw)?,
        AesMode::CbcPkcs5 => cbc_decrypt(key.as_bytes(), key.as_bytes(), &raw)?,
    };
    String::from_utf8(pt).map_err(|e| CryptoError::decrypt(format!("明文非 UTF-8: {e}")))
}
