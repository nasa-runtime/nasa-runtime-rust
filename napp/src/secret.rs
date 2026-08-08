//! 同代 config/secret 快照的解析与脱敏管道。
//!
//! 从**原始候选配置树**里按约定位置 `secrets.<id>` 读取 [`nasecret::SecretSpec`],在**脱敏前**解析出
//! 材料(合并有序 fragment → 一次性解码 → 校验),产出:
//! - [`SecretSnapshot`](与本代 config 同 generation),真实值只存在于此;
//! - **脱敏后的配置树**:被 `ConfigPath` fragment 引用的标量替换为 `<redacted>`,供普通
//!   `Application::config()` 使用;
//! - **候选 fingerprint**(对**原始**树求得):供无变化判断,避免拿原始树与上一帧 `<redacted>` 树
//!   直接比较而每次 watch 误增版本。
//!
//! 约定配置 schema
//!
//! ```yaml
//! secrets:
//!   legacy_aes:
//!     encoding: base64_after_concat
//!     max_bytes: 64
//!     fragments:
//!       - config_path: security.crypto.fragments.legacy_aes_pre
//!       - config_path: security.crypto.fragments.legacy_aes_suffix
//! ```

use sha2::{Digest as _, Sha256};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use nasecret::{
    SecretEncoding, SecretError, SecretFragmentRef, SecretSnapshot, SecretSpec,
    MAX_SECRET_FRAGMENTS, REDACTED,
};
use serde::Deserialize;
use serde_json::Value;

/// 解析 + 脱敏管道的产物(同一 generation)。
///
/// `Debug` 安全:`snapshot` 只露 generation+id,`redacted` 已脱敏,`candidate_fingerprint` 是散列。
#[derive(Debug)]
pub struct SecretResolution {
    /// 本代已解析 secret 集合;真实值只在此。
    pub snapshot: SecretSnapshot,
    /// 脱敏后的配置树:fragment 标量替换为 `<redacted>`。
    pub redacted: Value,
    /// 对**原始**候选树求得的私有 fingerprint,用于无变化判断。
    pub candidate_fingerprint: [u8; 32],
}

/// secret 解析管道错误:结构错误只含 ID/稳定摘要,解析错误转发 [`SecretError`](不含值)。
#[derive(Debug)]
pub enum SecretResolveError {
    /// `secrets` 段结构非法(非 map,或某 spec 反序列化失败)。摘要只描述结构,不含 secret 值。
    MalformedSpec {
        /// 出错 secret 的 ID(结构错误时可能是合成 ID `secrets`)。
        id: Arc<str>,
        /// 结构层稳定摘要(仅 spec 元信息,不含被引用的 secret 值)。
        detail: String,
    },
    /// 某 secret 的合并/解码/校验失败。
    Resolve(SecretError),
}

impl fmt::Display for SecretResolveError {
    /// 业务作用：输出 secret ID 与结构化原因，永不包含解析出的敏感值。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretResolveError::MalformedSpec { id, detail } => {
                write!(formatter, "secret `{id}` spec is malformed: {detail}")
            }
            SecretResolveError::Resolve(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for SecretResolveError {}

/// 配置里一个 secret 的原始声明(`secrets.<id>`)。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretKeyConfig {
    encoding: EncodingConfig,
    max_bytes: usize,
    fragments: Vec<FragmentConfig>,
}

/// 配置里的编码枚举(snake_case,对应 [`SecretEncoding`])。
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum EncodingConfig {
    Raw,
    Base64AfterConcat,
    HexAfterConcat,
}

impl From<EncodingConfig> for SecretEncoding {
    /// 业务作用：将声明式配置枚举映射到解析器的实际拼接后编码策略。
    fn from(value: EncodingConfig) -> Self {
        match value {
            EncodingConfig::Raw => SecretEncoding::Raw,
            EncodingConfig::Base64AfterConcat => SecretEncoding::Base64AfterConcat,
            EncodingConfig::HexAfterConcat => SecretEncoding::HexAfterConcat,
        }
    }
}

/// 配置里的一个分片来源(外部标签:`{config_path: ...}` / `{env: ...}` / `{file: ...}`)。
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FragmentConfig {
    ConfigPath(String),
    Env(String),
    File(PathBuf),
}

impl From<FragmentConfig> for SecretFragmentRef {
    /// 业务作用：将配置 fragment 标签转换为解析器使用的路径、环境变量或文件引用。
    fn from(value: FragmentConfig) -> Self {
        match value {
            FragmentConfig::ConfigPath(path) => SecretFragmentRef::ConfigPath(Arc::from(path)),
            FragmentConfig::Env(name) => SecretFragmentRef::Env(Arc::from(name)),
            FragmentConfig::File(path) => SecretFragmentRef::File(path),
        }
    }
}

/// 业务作用：从原始树的 `secrets` 段解析出全部 [`SecretSpec`];无 `secrets` 段时返回空。
fn parse_specs(raw: &Value) -> Result<Vec<SecretSpec>, SecretResolveError> {
    let Some(secrets) = raw.get("secrets") else {
        return Ok(Vec::new());
    };
    let Some(map) = secrets.as_object() else {
        return Err(SecretResolveError::MalformedSpec {
            id: Arc::from("secrets"),
            detail: "`secrets` must be a mapping of id to spec".to_owned(),
        });
    };
    let mut specs = Vec::with_capacity(map.len());
    for (id, spec_value) in map {
        if !valid_secret_id(id) {
            return Err(SecretResolveError::MalformedSpec {
                id: Arc::from("<invalid>"),
                detail: "secret id must be a bounded ASCII identifier".to_owned(),
            });
        }
        let config: SecretKeyConfig =
            serde_json::from_value(spec_value.clone()).map_err(|_error| {
                SecretResolveError::MalformedSpec {
                    id: Arc::from(id.as_str()),
                    detail: "invalid secret spec structure".to_owned(),
                }
            })?;
        if config.fragments.len() > MAX_SECRET_FRAGMENTS {
            return Err(SecretResolveError::MalformedSpec {
                id: Arc::from(id.as_str()),
                detail: format!("fragment count exceeds the hard limit of {MAX_SECRET_FRAGMENTS}"),
            });
        }
        if config.fragments.iter().any(|fragment| {
            matches!(
                fragment,
                FragmentConfig::ConfigPath(path)
                    if path.split('.').next() == Some("secrets")
            )
        }) {
            return Err(SecretResolveError::MalformedSpec {
                id: Arc::from(id.as_str()),
                detail: "config_path cannot reference the `secrets` subtree".to_owned(),
            });
        }
        specs.push(SecretSpec {
            id: Arc::from(id.as_str()),
            fragments: config
                .fragments
                .into_iter()
                .map(SecretFragmentRef::from)
                .collect(),
            encoding: config.encoding.into(),
            max_bytes: config.max_bytes,
        });
    }
    Ok(specs)
}

/// 业务作用：将 secret ID 限制为短 ASCII 标识，避免路径歧义、控制字符与无界诊断字段。
fn valid_secret_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// 业务作用：按点分路径在树中取**标量字符串**;非字符串或不存在返回 `None`(禁止引用子树/递归 spec)。
fn lookup_scalar(raw: &Value, path: &str) -> Option<Arc<str>> {
    let mut current = raw;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    match current {
        Value::String(text) => Some(Arc::from(text.as_str())),
        _ => None,
    }
}

/// 业务作用：把点分路径处的标量替换为 `<redacted>`;路径不存在则忽略(只脱敏已解析成功的 fragment)。
fn redact_path(redacted: &mut Value, path: &str) {
    let segments: Vec<&str> = path.split('.').collect();
    let Some((last, parents)) = segments.split_last() else {
        return;
    };
    let mut current = redacted;
    for parent in parents {
        match current.get_mut(*parent) {
            Some(next) => current = next,
            None => return,
        }
    }
    if let Some(object) = current.as_object_mut() {
        if object.contains_key(*last) {
            object.insert((*last).to_owned(), Value::String(REDACTED.to_owned()));
        }
    }
}

/// 业务作用：对原始候选树求私有 fingerprint(canonical JSON 文本的 hash);仅供无变化判断,不对外。
///
/// reload 用它比较相邻两帧的**原始**候选,而不是拿原始树与上一帧 `<redacted>` 树直接比较——后者
/// 因脱敏差异每次 watch 都会误判有变。
pub(crate) fn candidate_fingerprint(raw: &Value) -> [u8; 32] {
    let mut encoded = zeroize::Zeroizing::new(Vec::new());
    let _ = serde_json::to_writer(&mut *encoded, raw);
    Sha256::digest(encoded.as_slice()).into()
}

/// 业务作用：从原始候选树解析 secret、脱敏 fragment、求候选 fingerprint(同一 generation)。
///
/// # 参数
///
/// - `raw`:合并 profile/Nacos/env 后的**原始**候选树(未脱敏)。
/// - `generation`:本次发布代号,与 config 版本一致。
///
/// # 错误
///
/// `secrets` 段结构非法或任一 secret 解析失败时返回 [`SecretResolveError`](保留 last-good 由调用方负责)。
pub fn resolve_and_redact(
    raw: &Value,
    generation: u64,
) -> Result<SecretResolution, SecretResolveError> {
    let specs = parse_specs(raw)?;
    let snapshot = SecretSnapshot::resolve(generation, &specs, |path| lookup_scalar(raw, path))
        .map_err(SecretResolveError::Resolve)?;

    let mut redacted = raw.clone();
    for spec in &specs {
        for fragment in &spec.fragments {
            if let SecretFragmentRef::ConfigPath(path) = fragment {
                redact_path(&mut redacted, path);
            }
        }
    }

    let candidate_fingerprint = candidate_fingerprint(raw);
    Ok(SecretResolution {
        snapshot,
        redacted,
        candidate_fingerprint,
    })
}
