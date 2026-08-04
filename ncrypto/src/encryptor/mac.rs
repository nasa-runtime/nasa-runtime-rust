//! HMAC 消息认证码(对照 原实现 `hmacMd5/Sha1/Sha256/Sha512` + `*Base64` 变体)。
//!
//! hex 输出**小写**;`*_base64` 输出标准 Base64。HMAC key 任意长度(直接取 secret 的 UTF-8 字节)。
//!
//! ⚠ HMAC 用于**校验**时勿用 `==` 比串(时序侧信道)。**请用本模块 `hmac_sha256_verify`/`hmac_sha512_verify`**
//! 常量时间便捷函数(内部 `Mac::verify_slice` → `subtle::ConstantTimeEq`),而非自行 `hmac_*(..) == expected`。

use super::{b64_encode, hex_lower};
use hmac::{Hmac, Mac};
use md5::Md5;
use sha1::Sha1;
use sha2::{Sha256, Sha512};

/// 生成 `hmac_xxx`(hex 小写)+ `hmac_xxx_base64` 函数。HMAC 接受任意长度 key,`new_from_slice` 不会失败。
macro_rules! hmac_hex {
    ($name:ident, $d:ty, $doc:literal) => {
        /// 业务作用: 使用指定摘要算法计算消息认证码，并输出小写 hex 文本。
        ///
        /// # 参数
        /// - `content`: 待认证的文本内容。
        /// - `secret`: 签名、加密或摘要使用的密钥材料。
        #[doc = $doc]
        pub fn $name(content: &str, secret: &str) -> String {
            let mut mac = Hmac::<$d>::new_from_slice(secret.as_bytes()).expect("HMAC 任意长度 key");
            mac.update(content.as_bytes());
            hex_lower(&mac.finalize().into_bytes())
        }
    };
}
macro_rules! hmac_b64 {
    ($name:ident, $d:ty, $doc:literal) => {
        /// 业务作用: 使用指定摘要算法计算消息认证码，并输出标准 Base64 文本。
        ///
        /// # 参数
        /// - `content`: 待认证的文本内容。
        /// - `secret`: 签名、加密或摘要使用的密钥材料。
        #[doc = $doc]
        pub fn $name(content: &str, secret: &str) -> String {
            let mut mac = Hmac::<$d>::new_from_slice(secret.as_bytes()).expect("HMAC 任意长度 key");
            mac.update(content.as_bytes());
            b64_encode(&mac.finalize().into_bytes())
        }
    };
}

hmac_hex!(hmac_md5, Md5, "HMAC-MD5(小写 hex)。仅遗留兼容。");
hmac_hex!(
    hmac_sha1,
    Sha1,
    "HMAC-SHA1(小写 hex,OAuth 1.0)。新系统建议 SHA256。"
);
hmac_hex!(
    hmac_sha256,
    Sha256,
    "HMAC-SHA256(小写 hex)—— 最常用的 API 签名算法。"
);
hmac_hex!(
    hmac_sha512,
    Sha512,
    "HMAC-SHA512(小写 hex)—— 如 Binance API v3 签名。"
);
hmac_b64!(
    hmac_sha256_base64,
    Sha256,
    "HMAC-SHA256(Base64,部分 API 如 AWS 要求)。"
);
hmac_b64!(hmac_sha512_base64, Sha512, "HMAC-SHA512(Base64)。");

/// 生成 `hmac_xxx_verify`:以**常量时间**校验消息+密钥是否产出给定的小写 hex MAC(安全说明.4)。
/// 对照 `hmac_xxx` 的逆向校验:内部用 `Mac::verify_slice`(底层 `subtle::ConstantTimeEq`,不泄露
/// 首个不等字节位置),从根上替代调用方可能误写的 `hmac_sha256(..) == expected` 时序敏感比较。
/// `expected_hex` 非法 hex / 长度不符 → `false`(hex 合法性非机密,不影响 MAC 比较的常量时间性)。
macro_rules! hmac_verify {
    ($name:ident, $d:ty, $doc:literal) => {
        /// 业务作用: 以常量时间比较给定 hex 消息认证码，避免比较过程泄露首个差异位置。
        ///
        /// # 参数
        /// - `content`: 待认证的文本内容。
        /// - `secret`: 签名、加密或摘要使用的密钥材料。
        /// - `expected_hex`: 期望的十六进制 MAC 摘要。
        #[doc = $doc]
        pub fn $name(content: &str, secret: &str, expected_hex: &str) -> bool {
            let expected = match hex::decode(expected_hex) {
                Ok(bytes) => bytes,
                Err(_) => return false,
            };
            let mut mac = Hmac::<$d>::new_from_slice(secret.as_bytes()).expect("HMAC 任意长度 key");
            mac.update(content.as_bytes());
            mac.verify_slice(&expected).is_ok()
        }
    };
}

hmac_verify!(
    hmac_sha256_verify,
    Sha256,
    "常量时间校验 HMAC-SHA256(对照 `hmac_sha256`,`expected_hex` 为小写 hex)。"
);
hmac_verify!(
    hmac_sha512_verify,
    Sha512,
    "常量时间校验 HMAC-SHA512(对照 `hmac_sha512`,`expected_hex` 为小写 hex)。"
);
