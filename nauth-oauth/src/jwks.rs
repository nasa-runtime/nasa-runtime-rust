//! JWKS 解析与生命周期。
//!
//! [`JwksRegistry`] 用 `ArcSwap` 原子发布当前 key set:warmup 建初值,rotate 先**校验候选**再原子发布,
//! 校验失败**保留 last-good**、generation 不变(对应「启动刷新失败保留 last-good」与轮换语义)。

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use base64::Engine as _;
use serde::Deserialize;

/// 单个运行时 JWKS 接受的最大 key 数，防止远端响应以大量小 key 绕过字节上限。
pub const DEFAULT_MAX_KEYS: usize = 128;

/// 一个 JSON Web Key(保留原始参数供后续签名验签映射)。
#[derive(Clone, Debug, Deserialize)]
pub struct Jwk {
    /// key id;JWT header `kid` 据此选 key。
    pub kid: String,
    /// key 类型(`RSA`/`EC`/`oct`)。
    pub kty: String,
    /// 签名算法(如 `RS256`);可选。
    pub alg: Option<String>,
    /// 用途(`sig`/`enc`);Resource Server 只用 `sig`。
    #[serde(rename = "use")]
    pub use_: Option<String>,
    /// 其余 key 参数(RSA 的 `n`/`e`、EC 的 `x`/`y` 等),原样保留供验签增量使用。
    #[serde(flatten)]
    pub params: serde_json::Map<String, serde_json::Value>,
}

/// 一组 JSON Web Key(JWKS)。
#[derive(Clone, Debug, Deserialize)]
pub struct JwkSet {
    /// key 列表。
    pub keys: Vec<Jwk>,
}

/// JWKS 解析/校验错误(不含 key 秘密内容)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwksError {
    /// JSON 解析失败(稳定摘要,不含原文)。
    Parse(String),
    /// key set 为空。
    Empty,
    /// 出现重复 `kid`。
    DuplicateKid(String),
    /// 某 key 缺 `kty`。
    MissingKty(String),
    /// `kid` 为空。
    EmptyKid,
    /// `kid` 超长或含日志不安全字符。
    InvalidKid,
    /// key 数超过上限。
    TooManyKeys {
        /// 实际解析到的 key 数量。
        actual: usize,
        /// 当前解析策略允许的 key 数量上限。
        max: usize,
    },
    /// 当前 Resource Server 不支持该 key 的类型、用途或算法。
    UnsupportedKey(String),
    /// RSA key 缺少或包含非法的 `n` / `e`。
    InvalidRsaKey(String),
}

impl std::fmt::Display for JwksError {
    /// 输出结构化 JWKS 错误；只在安全时携带已校验 kid，不包含 key material。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JwksError::Parse(detail) => write!(formatter, "JWKS parse failed: {detail}"),
            JwksError::Empty => write!(formatter, "JWKS has no keys"),
            JwksError::DuplicateKid(kid) => write!(formatter, "JWKS has duplicate kid `{kid}`"),
            JwksError::MissingKty(kid) => write!(formatter, "JWKS key `{kid}` is missing kty"),
            JwksError::EmptyKid => write!(formatter, "JWKS contains an empty kid"),
            JwksError::InvalidKid => write!(formatter, "JWKS contains an invalid kid"),
            JwksError::TooManyKeys { actual, max } => {
                write!(formatter, "JWKS has {actual} keys, exceeding limit {max}")
            }
            JwksError::UnsupportedKey(kid) => {
                write!(formatter, "JWKS key `{kid}` is not an RSA signing key")
            }
            JwksError::InvalidRsaKey(kid) => {
                write!(formatter, "JWKS key `{kid}` has invalid RSA parameters")
            }
        }
    }
}

impl std::error::Error for JwksError {}

impl JwkSet {
    /// 从 JWKS JSON 文本解析。
    pub fn parse(json: &str) -> Result<Self, JwksError> {
        serde_json::from_str(json).map_err(|error| JwksError::Parse(error.to_string()))
    }

    /// 按 `kid` 查找 key。
    pub fn find(&self, kid: &str) -> Option<&Jwk> {
        self.keys.iter().find(|key| key.kid == kid)
    }

    /// 结构校验：使用缺省 key 数上限。
    pub fn validate(&self) -> Result<(), JwksError> {
        self.validate_with_max_keys(DEFAULT_MAX_KEYS)
    }

    /// 结构与能力校验：非空、有界、`kid` 非空且唯一，并且每个 key 都是可用的 RSA/RS256
    /// signing key，含可解码且非空的 `n` / `e`。
    pub fn validate_with_max_keys(&self, max_keys: usize) -> Result<(), JwksError> {
        if self.keys.is_empty() {
            return Err(JwksError::Empty);
        }
        if self.keys.len() > max_keys {
            return Err(JwksError::TooManyKeys {
                actual: self.keys.len(),
                max: max_keys,
            });
        }
        let mut seen = BTreeSet::new();
        for key in &self.keys {
            if key.kid.trim().is_empty() {
                return Err(JwksError::EmptyKid);
            }
            if key.kid.len() > 256 || key.kid.chars().any(char::is_control) {
                return Err(JwksError::InvalidKid);
            }
            if key.kty.trim().is_empty() {
                return Err(JwksError::MissingKty(key.kid.clone()));
            }
            if !seen.insert(key.kid.as_str()) {
                return Err(JwksError::DuplicateKid(key.kid.clone()));
            }
            if key.kty != "RSA"
                || key.use_.as_deref().is_some_and(|usage| usage != "sig")
                || key
                    .alg
                    .as_deref()
                    .is_some_and(|algorithm| algorithm != "RS256")
            {
                return Err(JwksError::UnsupportedKey(key.kid.clone()));
            }
            let decode_component = |name: &str| {
                key.params
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .and_then(|value| {
                        base64::engine::general_purpose::URL_SAFE_NO_PAD
                            .decode(value)
                            .ok()
                    })
                    .filter(|bytes| !bytes.is_empty())
            };
            let (Some(modulus), Some(exponent)) = (decode_component("n"), decode_component("e"))
            else {
                return Err(JwksError::InvalidRsaKey(key.kid.clone()));
            };
            if !ncrypto::validate_rs256_public_components(&modulus, &exponent) {
                return Err(JwksError::InvalidRsaKey(key.kid.clone()));
            }
        }
        Ok(())
    }
}

/// JWKS 运行时注册表:原子发布当前 key set + 单调 generation。
pub struct JwksRegistry {
    current: ArcSwap<JwkSet>,
    generation: AtomicU64,
}

impl JwksRegistry {
    /// warmup:用初始 key set 建注册表,generation=1;初值必须先通过 [`JwkSet::validate`]。
    ///
    /// # 错误
    ///
    /// 初始 key set 结构非法时返回 [`JwksError`],不建注册表。
    pub fn warmup(initial: JwkSet) -> Result<Self, JwksError> {
        initial.validate()?;
        Ok(Self {
            current: ArcSwap::from_pointee(initial),
            generation: AtomicU64::new(1),
        })
    }

    /// rotate:**先校验候选**,通过则原子发布并 generation++,返回新 generation;
    /// 校验失败**保留 last-good**、generation 不变、返回错误(startup 刷新失败即走此路径)。
    pub fn rotate(&self, candidate: JwkSet) -> Result<u64, JwksError> {
        candidate.validate()?; // 失败:直接返回,current/generation 不变
        self.current.store(Arc::new(candidate));
        Ok(self.generation.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// 当前 key set(原子快照)。
    pub fn current(&self) -> Arc<JwkSet> {
        self.current.load_full()
    }

    /// 当前 generation(每次成功 rotate +1)。
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// 便捷:从当前 key set 按 `kid` 取 key 的克隆。
    pub fn find(&self, kid: &str) -> Option<Jwk> {
        self.current().find(kid).cloned()
    }
}
