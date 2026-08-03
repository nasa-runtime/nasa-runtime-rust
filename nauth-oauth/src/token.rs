//! JWT access token 校验(RFC 9068 / RFC 8725)。
//!
//! [`validate_access_token`] 校验 header + claims:algorithm 白名单(拒 `none`)、`typ=at+jwt`
//! (防 ID Token/Access Token 混淆)、`iss`/`aud` 匹配、`exp`/`nbf` 对时钟(带 leeway)。**签名验签**
//! 由 [`verify_access_token`] 使用 `ncrypto` 完成；[`parse_unverified`] 仅供需要先读取 header 的内部流程，
//! 其结果绝不能直接建立身份。

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::Deserialize;

/// JWT header(只取校验相关字段)。
#[derive(Debug, Clone, Deserialize)]
pub struct JwtHeader {
    /// 签名算法(`RS256` 等);`none` 一律拒绝。
    pub alg: String,
    /// token 类型;RFC 9068 access token 必须为 `at+jwt`。
    pub typ: Option<String>,
    /// 选 key 用的 key id。
    pub kid: Option<String>,
}

/// `aud` 可能是单串或数组。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Audience {
    /// 单个 audience。
    One(String),
    /// 多个 audience。
    Many(Vec<String>),
}

impl Audience {
    /// 业务作用：判断单值或数组 audience 是否包含策略要求的精确值。
    fn contains(&self, expected: &str) -> bool {
        match self {
            Audience::One(value) => value == expected,
            Audience::Many(values) => values.iter().any(|value| value == expected),
        }
    }
}

/// access token claims(RFC 9068 关注字段)。
#[derive(Debug, Clone, Deserialize)]
pub struct AccessTokenClaims {
    /// 签发者。
    pub iss: Option<String>,
    /// 受众。
    pub aud: Option<Audience>,
    /// 过期时间(epoch 秒)。
    pub exp: Option<u64>,
    /// 生效时间(epoch 秒)。
    pub nbf: Option<u64>,
    /// 签发时间(epoch 秒)。
    pub iat: Option<u64>,
    /// 主体。
    pub sub: Option<String>,
    /// 客户端 id。
    pub client_id: Option<String>,
    /// 可选租户 id；兼容常见 `tenant_id` / `tid` claim 名。
    #[serde(alias = "tenant_id", alias = "tid")]
    pub tenant: Option<String>,
    /// OAuth scope(空格分隔;RFC 9068)。授权层据此取 scope 集合。
    pub scope: Option<String>,
}

/// 校验策略。
#[derive(Debug, Clone)]
pub struct TokenPolicy {
    /// 期望 issuer。
    pub expected_issuer: String,
    /// 期望 audience。
    pub expected_audience: String,
    /// 算法白名单(不含 `none`)。
    pub allowed_algorithms: Vec<String>,
    /// 时钟偏移容忍秒数。
    pub leeway_secs: u64,
}

/// 校验失败原因(稳定,不含 token 内容)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
    /// 结构不是 `header.payload.signature`。
    Malformed,
    /// base64url / JSON 解码失败。
    Decode,
    /// `typ` 非 `at+jwt`(防 ID Token 混淆)。
    NotAtJwt,
    /// 算法不在白名单(含 `none`)。
    AlgorithmNotAllowed(String),
    /// issuer 不匹配。
    IssuerMismatch,
    /// audience 不匹配。
    AudienceMismatch,
    /// 已过期(或缺 `exp`)。
    Expired,
    /// 尚未生效(`nbf`)。
    NotYetValid,
    /// `iat` 位于容忍窗口之后的未来。
    IssuedInFuture,
    /// 找不到匹配 `kid` 的 JWK(或 header 缺 `kid`)。
    KeyNotFound,
    /// JWK 类型/算法不支持(当前只支持 `kty=RSA` / `RS256`)。
    UnsupportedKey,
    /// 签名验证不通过。
    SignatureInvalid,
}

impl std::fmt::Display for TokenError {
    /// 业务作用：输出稳定校验原因，不包含 token、claims 正文或签名字节。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Malformed => write!(formatter, "token is not a well-formed JWT"),
            TokenError::Decode => write!(formatter, "token header/claims could not be decoded"),
            TokenError::NotAtJwt => write!(formatter, "token typ is not `at+jwt`"),
            TokenError::AlgorithmNotAllowed(alg) => {
                write!(formatter, "algorithm `{alg}` is not allowed")
            }
            TokenError::IssuerMismatch => write!(formatter, "issuer does not match policy"),
            TokenError::AudienceMismatch => write!(formatter, "audience does not match policy"),
            TokenError::Expired => write!(formatter, "token is expired or missing exp"),
            TokenError::NotYetValid => write!(formatter, "token is not yet valid (nbf)"),
            TokenError::IssuedInFuture => write!(formatter, "token iat is in the future"),
            TokenError::KeyNotFound => write!(formatter, "no JWK matches the token kid"),
            TokenError::UnsupportedKey => {
                write!(
                    formatter,
                    "JWK type/algorithm is not supported (RSA/RS256 only)"
                )
            }
            TokenError::SignatureInvalid => {
                write!(formatter, "token signature verification failed")
            }
        }
    }
}

impl std::error::Error for TokenError {}

/// 业务作用：拆分 JWT 并 base64url 解码 header/claims(**不验签**);结构或解码错返回 [`TokenError`]。
pub fn parse_unverified(token: &str) -> Result<(JwtHeader, AccessTokenClaims), TokenError> {
    let mut parts = token.split('.');
    let header_b64 = parts.next().ok_or(TokenError::Malformed)?;
    let claims_b64 = parts.next().ok_or(TokenError::Malformed)?;
    let _signature = parts.next().ok_or(TokenError::Malformed)?;
    if parts.next().is_some() {
        return Err(TokenError::Malformed); // 必须恰好 3 段
    }
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header_bytes = engine.decode(header_b64).map_err(|_| TokenError::Decode)?;
    let claims_bytes = engine.decode(claims_b64).map_err(|_| TokenError::Decode)?;
    let header: JwtHeader =
        serde_json::from_slice(&header_bytes).map_err(|_| TokenError::Decode)?;
    let claims: AccessTokenClaims =
        serde_json::from_slice(&claims_bytes).map_err(|_| TokenError::Decode)?;
    Ok((header, claims))
}

/// 业务作用：校验 access token 的 header + claims(不含签名验签)。
///
/// # 参数
///
/// - `header`/`claims`:已解析的 JWT header 与 claims。
/// - `policy`:期望 issuer/audience、算法白名单、leeway。
/// - `now`:当前墙上时间(用 `nadate::UtcClock` 便于注入固定时钟)。
pub fn validate_access_token(
    header: &JwtHeader,
    claims: &AccessTokenClaims,
    policy: &TokenPolicy,
    now: SystemTime,
) -> Result<(), TokenError> {
    // 当前验签器只实现 RS256。不能让配置白名单声称支持其它算法、随后仍拿 RSA 验签器处理。
    if header.alg != "RS256"
        || !policy
            .allowed_algorithms
            .iter()
            .any(|allowed| allowed == &header.alg)
    {
        return Err(TokenError::AlgorithmNotAllowed(header.alg.clone()));
    }
    // RFC 9068:typ 必须 at+jwt(防 ID Token 混淆)。
    match &header.typ {
        Some(typ) if typ.eq_ignore_ascii_case("at+jwt") => {}
        _ => return Err(TokenError::NotAtJwt),
    }
    // issuer。
    match &claims.iss {
        Some(iss) if iss == &policy.expected_issuer => {}
        _ => return Err(TokenError::IssuerMismatch),
    }
    // audience。
    match &claims.aud {
        Some(aud) if aud.contains(&policy.expected_audience) => {}
        _ => return Err(TokenError::AudienceMismatch),
    }
    // 时间窗(RFC 9068:access token 必须有 exp)。
    let now_secs = now
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    match claims.exp {
        Some(exp) if now_secs < exp.saturating_add(policy.leeway_secs) => {}
        _ => return Err(TokenError::Expired),
    }
    if let Some(nbf) = claims.nbf {
        if now_secs.saturating_add(policy.leeway_secs) < nbf {
            return Err(TokenError::NotYetValid);
        }
    }
    if let Some(iat) = claims.iat {
        if now_secs.saturating_add(policy.leeway_secs) < iat {
            return Err(TokenError::IssuedInFuture);
        }
    }
    Ok(())
}

/// 业务作用：从 JWK 取某个 base64url 参数并解码为字节(如 RSA 的 `n`/`e`)。
fn jwk_component(jwk: &crate::jwks::Jwk, name: &str) -> Result<Vec<u8>, TokenError> {
    let value = jwk
        .params
        .get(name)
        .and_then(serde_json::Value::as_str)
        .ok_or(TokenError::UnsupportedKey)?;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| TokenError::Decode)
}

/// 业务作用：用 JWK 校验 JWT 的 **RS256 签名**(经 `ncrypto` 的 RSASSA-PKCS1-v1_5 + SHA-256)。
///
/// 仅支持 `kty=RSA` 且(若 JWK 声明 `alg`)`alg=RS256`,否则 [`TokenError::UnsupportedKey`]。签名输入为
/// `header_b64 "." payload_b64`(原样字节);签名与 `n`/`e` 均按 base64url 解码。验签不通过返回
/// [`TokenError::SignatureInvalid`]。
///
/// # 参数
/// - `token`:紧凑序列化 JWT(`header.payload.signature`)。
/// - `jwk`:选定的 JSON Web Key(其 `params` 含 RSA 的 `n`/`e`)。
pub fn verify_rs256_signature(token: &str, jwk: &crate::jwks::Jwk) -> Result<(), TokenError> {
    let mut parts = token.split('.');
    let header_b64 = parts.next().ok_or(TokenError::Malformed)?;
    let payload_b64 = parts.next().ok_or(TokenError::Malformed)?;
    let signature_b64 = parts.next().ok_or(TokenError::Malformed)?;
    if parts.next().is_some() {
        return Err(TokenError::Malformed);
    }
    if !jwk.kty.eq_ignore_ascii_case("RSA") {
        return Err(TokenError::UnsupportedKey);
    }
    if let Some(alg) = &jwk.alg {
        if !alg.eq_ignore_ascii_case("RS256") {
            return Err(TokenError::UnsupportedKey);
        }
    }
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| TokenError::Decode)?;
    let modulus = jwk_component(jwk, "n")?;
    let exponent = jwk_component(jwk, "e")?;
    let signing_input = format!("{header_b64}.{payload_b64}");
    if ncrypto::verify_rs256_components(signing_input.as_bytes(), &signature, &modulus, &exponent) {
        Ok(())
    } else {
        Err(TokenError::SignatureInvalid)
    }
}

/// 业务作用：**完整校验** access token:解析 → 按 header `kid` 选 JWK → RS256 验签 → header/claims 校验。
///
/// 返回已验证 claims。这是 OAuth Resource Server / authentication 中间件应调用的入口:先证明**签名
/// 真实**,再判断 claims 是否满足策略;二者缺一不可(仅 [`validate_access_token`] 不验签,不足以信任 claims)。
///
/// # 参数
/// - `token`:紧凑序列化 JWT。
/// - `jwks`:当前有效 JWKS(由 `JwksRegistry` 快照提供)。
/// - `policy`:期望 issuer/audience、算法白名单、leeway。
/// - `now`:当前墙上时间。
pub fn verify_access_token(
    token: &str,
    jwks: &crate::jwks::JwkSet,
    policy: &TokenPolicy,
    now: SystemTime,
) -> Result<AccessTokenClaims, TokenError> {
    let (header, claims) = parse_unverified(token)?;
    let kid = header.kid.as_deref().ok_or(TokenError::KeyNotFound)?;
    let jwk = jwks.find(kid).ok_or(TokenError::KeyNotFound)?;
    verify_rs256_signature(token, jwk)?;
    validate_access_token(&header, &claims, policy, now)?;
    Ok(claims)
}
