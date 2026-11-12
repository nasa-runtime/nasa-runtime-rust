//! RSA+AES 混合加密。
//!
//! 把 demo 里手写的"随机 AES key + RSA 加密 key + AES 加密数据"机制收敛成纯函数,供复用。
//! **方向 = 服务端加密响应**:
//!   1. AES key:调用方给 `Some(key)` 用之,`None` → 随机 16B ASCII;
//!   2. `aes = encryptRSAPrivate(key)`(服务端**私钥**加密 AES key);
//!   3. `data = encryptAES(plaintext, key)`(AES-ECB 加密明文)。
//!
//! 返回 `(aes, data)`:`aes` 放进响应 `BaseResponse.aes`、`data` 放进 `data`。客户端用 RSA **公钥**解 `aes`
//! 得回 key、再 AES 解 `data`(见 [`rsa_aes_open`])。
//!
//! ⚠ 本模块**纯函数、不依赖 base/web**:只产出 `(aes, data)` 字符串,组装 `BaseResponse` 由 Web 层负责
//! (分层:crypto 库不认识响应壳)。

use super::aes::{decrypt_aes, encrypt_aes};
use super::rng::random_ascii;
use super::rsa::{decrypt_rsa_public, encrypt_rsa_private};
use crate::Result;

/// 业务作用：为既有私钥运算协议封装 AES 密钥和响应密文。
///
/// 参数说明：
/// - `plaintext`：需要按 AES-ECB 加密的 UTF-8 内容。
/// - `rsa_private_key_b64`：用于包装 AES 密钥的 Base64 PKCS8 RSA 私钥。
/// - `aes_key`：显式 AES 密钥文本；None 生成 16 字节随机 ASCII 密钥。
///
/// 返回：返回 AES 密钥包装字段及 AES 数据字段，两者均为 Base64；密钥解析、私钥运算或 AES 失败传播。默认未启用 legacy-rsa-private 时拒绝；公钥可恢复 AES 密钥，此封装不对公钥持有者保密。
pub fn rsa_aes_seal(
    plaintext: &str,
    rsa_private_key_b64: &str,
    aes_key: Option<&str>,
) -> Result<(String, String)> {
    // 16B ASCII = 合法 AES-128 key(key 串 UTF-8 字节直接作 key)。
    let key = match aes_key {
        Some(k) => k.to_string(),
        None => random_ascii(16),
    };
    let aes = encrypt_rsa_private(&key, rsa_private_key_b64)?;
    let data = encrypt_aes(plaintext, &key)?;
    Ok((aes, data))
}

/// 业务作用：按既有协议用 RSA 公钥恢复 AES 密钥并解开响应内容。
///
/// 参数说明：
/// - `aes`：私钥 type-1 运算包装的 AES 密钥 Base64 字段。
/// - `data`：AES-ECB 密文 Base64 字段。
/// - `rsa_public_key_b64`：Base64 SPKI RSA 公钥。
///
/// 返回：成功返回 UTF-8 明文；RSA、Base64、AES 填充或 UTF-8 失败传播，不提供独立的 AEAD 认证。
pub fn rsa_aes_open(aes: &str, data: &str, rsa_public_key_b64: &str) -> Result<String> {
    let key = decrypt_rsa_public(aes, rsa_public_key_b64)?;
    decrypt_aes(data, &key)
}
