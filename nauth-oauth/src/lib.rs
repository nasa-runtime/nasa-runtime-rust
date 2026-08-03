//! NASA OAuth Resource Server:JWT access token 校验与 JWKS 生命周期。
//!
//! - [`jwks`]:JWKS 解析与 [`JwksRegistry`](warmup / 校验后原子 rotate / 失败保留 last-good),对应
//!   「Start 建未激活 registry;Ready warmup;启动刷新失败保留 last-good」。
//! - [`token`]:JWT access token claims 校验(RFC 9068 `at+jwt`、algorithm 白名单、iss/aud、exp/nbf),
//!   防 ID Token/Access Token 混淆(RFC 8725)。签名验签由后续增量接 `ncrypto`。
//!
//! 本 crate **不依赖 `napp`**；时间由调用方以 `SystemTime` 传入，允许业务接入系统或虚拟时钟来源。

#![forbid(unsafe_code)]

pub mod jwks;
pub mod metadata;
pub mod token;

pub use jwks::{Jwk, JwkSet, JwksError, JwksRegistry};
pub use metadata::{AuthorizationServerMetadata, MetadataClient, MetadataError, MetadataOptions};
pub use token::{
    parse_unverified, validate_access_token, verify_access_token, verify_rs256_signature,
    AccessTokenClaims, Audience, JwtHeader, TokenError, TokenPolicy,
};
