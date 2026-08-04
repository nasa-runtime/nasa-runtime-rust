//! RSA+AES 混合加密(对照 原实现 `CryptoTactics.RSA_AES`)。
//!
//! 把 demo 里手写的"随机 AES key + RSA 加密 key + AES 加密数据"机制收敛成纯函数,供复用。
//! **方向 = 服务端加密响应**(对照 原实现 `CryptoTactics.RSA_AES` 的 `ENCRYPT` 分支):
//!   1. AES key:调用方给 `Some(key)` 用之,`None` → 随机 16B ASCII(对照 原实现 `StringUtils.random(16)`);
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

/// 业务作用: RSA+AES 混合**封装**(服务端加密响应方向;对照 原实现 `CryptoTactics.RSA_AES` ENCRYPT)。
///
/// # 参数
/// - `plaintext`: 待加密的 UTF-8 响应明文。
/// - `rsa_private_key_b64`: 服务端 RSA 私钥(Base64 PKCS8 DER);用于加密 AES key。
/// - `aes_key`: `Some(k)` 使用指定 AES key,`None` 生成随机 16B ASCII key。
///
/// 返回 `(aes, data)`:`aes` = `encryptRSAPrivate(key)` base64,`data` = `encryptAES(plaintext, key)` base64。
/// 此历史私钥运算要求显式启用 `legacy-rsa-private` feature。
///
/// ```no_run
/// # use ncrypto::{rsa_aes_seal, rsa_aes_open, generate_rsa_key_pair};
/// let (prik, pubk) = generate_rsa_key_pair(2048).unwrap();
/// let (aes, data) = rsa_aes_seal("hello", &prik, None).unwrap();
/// assert_eq!(rsa_aes_open(&aes, &data, &pubk).unwrap(), "hello");
/// ```
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

/// 业务作用: RSA+AES 混合**解封**(客户端解密响应方向;[`rsa_aes_seal`] 的逆)。
///
/// 用 RSA **公钥**解出 AES key(`decryptRSAPublic(aes)`),再 AES 解 `data` 得明文。
///
/// # 参数
/// - `aes`: 响应里的 AES key 包装字段,即私钥加密后的 Base64 RSA 密文。
/// - `data`: 响应里的业务数据字段,即 AES 加密后的 Base64 密文。
/// - `rsa_public_key_b64`: 客户端持有的 RSA 公钥(Base64 SPKI DER),用于恢复 AES key。
pub fn rsa_aes_open(aes: &str, data: &str, rsa_public_key_b64: &str) -> Result<String> {
    let key = decrypt_rsa_public(aes, rsa_public_key_b64)?;
    decrypt_aes(data, &key)
}
