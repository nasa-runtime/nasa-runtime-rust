//! 无状态工具函数（兼容原实现 `Encryptor`）。按算法族拆分子模块，并在此统一导出为扁平 `ncrypto::*`。

mod aes;
mod ed25519;
mod encoding;
mod hash;
mod hybrid;
mod kdf;
mod mac;
mod modern;
mod rng;
mod rsa;
mod web_aead;

pub use aes::*;
pub use ed25519::*;
pub use encoding::{
    base64_url_decode, base64_url_decode_str, base64_url_encode, base64_url_encode_str,
};
pub use hash::*;
pub use hybrid::*;
pub use kdf::*;
pub use mac::*;
pub use modern::*;
pub use rng::*;
pub use rsa::*;
pub use web_aead::*;

// 内部编码/hex helper 不对外(仅 crate 内各模块用)。
pub(crate) use encoding::{b64_decode, b64_encode, hex_decode, hex_lower, hex_upper};
