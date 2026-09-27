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
use std::collections::{BTreeMap, BTreeSet};
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
    Provider { provider: String, key: String },
}

impl From<FragmentConfig> for SecretFragmentRef {
    /// 业务作用：将配置 fragment 标签转换为解析器使用的路径、环境变量或文件引用。
    fn from(value: FragmentConfig) -> Self {
        match value {
            FragmentConfig::ConfigPath(path) => SecretFragmentRef::ConfigPath(Arc::from(path)),
            FragmentConfig::Env(name) => SecretFragmentRef::Env(Arc::from(name)),
            FragmentConfig::File(path) => SecretFragmentRef::File(path),
            FragmentConfig::Provider { provider, key } => SecretFragmentRef::Provider {
                provider: Arc::from(provider),
                key: Arc::from(key),
            },
        }
    }
}

/// 业务作用：从原始树的 `secrets` 段解析出全部 [`SecretSpec`];无 `secrets` 段时返回空。
pub(crate) fn parse_specs(raw: &Value) -> Result<Vec<SecretSpec>, SecretResolveError> {
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

/// 业务作用：将 secret ID 限制为有界 ASCII 层级标识，避免空分段、路径穿越与控制字符。
/// 参数说明：`value` 为声明 ID 或已移除协议前缀的引用 ID。
/// 返回：非空、不超过 128 字节且每个路径分段合法时为真，不读取任何材料。
pub(crate) fn valid_secret_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
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

/// 业务作用：对原始候选树求与 JSON object 键顺序无关的私有 fingerprint，仅供无变化判断，不对外。
///
/// reload 用它比较相邻两帧的**原始**候选,而不是拿原始树与上一帧 `<redacted>` 树直接比较——后者
/// 因脱敏差异每次 watch 都会误判有变。
///
/// 参数说明：
/// - `raw`：合并完成、尚未脱敏的完整候选树。
///
/// 返回：相同 JSON 语义生成相同的 SHA-256；object 的插入顺序不会改变结果，array 顺序仍保留业务语义。
pub(crate) fn candidate_fingerprint(raw: &Value) -> [u8; 32] {
    let mut encoded = zeroize::Zeroizing::new(Vec::new());
    write_canonical_json(raw, &mut encoded);
    Sha256::digest(encoded.as_slice()).into()
}

/// 业务作用：把 JSON 值编码成键有序、类型边界明确的稳定字节序列，作为候选配置散列输入。
///
/// 参数说明：
/// - `value`：当前递归编码的 JSON 节点。
/// - `output`：只在当前调用链内存活并在散列后清零的字节缓冲区。
///
/// 返回：无返回值；object 按键字典序编码，array 保持原顺序，标量沿用 JSON 标准转义。
fn write_canonical_json(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value {
            b"true"
        } else {
            b"false"
        }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => {
            let _ = serde_json::to_writer(output, value);
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output);
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                let _ = serde_json::to_writer(&mut *output, key);
                output.push(b':');
                write_canonical_json(value, output);
            }
            output.push(b'}');
        }
    }
}

/// 业务作用：同步准备当前活跃消费者的本地材料，并从完整候选中脱敏全部配置片段。
/// 参数说明：`raw` 为合并后的原始候选树，`generation` 为材料与配置共同的发布代次。
/// 返回：全部活跃材料成功后返回同代快照和脱敏树；结构、用途或材料解析失败时拒绝，旧视图由调用方保留。
pub fn resolve_and_redact(
    raw: &Value,
    generation: u64,
) -> Result<SecretResolution, SecretResolveError> {
    let specs = active_specs(raw)?;
    let snapshot = SecretSnapshot::resolve(generation, &specs, |path| lookup_scalar(raw, path))
        .map_err(SecretResolveError::Resolve)?;

    let redacted = redact_structure(raw)?;

    let candidate_fingerprint = candidate_fingerprint(raw);
    Ok(SecretResolution {
        snapshot,
        redacted,
        candidate_fingerprint,
    })
}

/// 业务作用：为材料解析和文件观察建立同一活跃消费者投影，禁用计划不取得外部材料所有权。
/// 参数说明：`raw` 为同代完整原始配置，业务与已启用计划的共享引用均保留。
/// 返回：经过全量结构校验的活跃声明；独属于禁用用途的声明被排除，不读取任何材料。
pub(crate) fn active_specs(raw: &Value) -> Result<Vec<SecretSpec>, SecretResolveError> {
    let mut specs = parse_specs(raw)?;
    if specs.len() > 256 {
        return Err(provider_error("secret count exceeds 256"));
    }
    let excluded = inactive_component_secrets(raw)?;
    specs.retain(|spec| !excluded.contains(spec.id.as_ref()));
    Ok(specs)
}

/// 业务作用：按组件用途投影新观测凭据，使应用不物化 controller 或未启用出口的材料。
/// 参数说明：`raw` 为同代完整配置；框架用途之外的业务 locator 按活跃消费者处理。
/// 返回：只有非活跃组件引用且没有活跃消费者的 secret ID；所有 config_path 仍统一脱敏。
fn inactive_component_secrets(raw: &Value) -> Result<BTreeSet<String>, SecretResolveError> {
    let mut classified = BTreeSet::new();
    let mut active = BTreeSet::new();
    for section in [
        "object_stores",
        "http_clients",
        "schema_registries",
        "ws_clients",
    ] {
        if let Some(plans) = raw.get(section).and_then(Value::as_object) {
            for plan in plans.values() {
                collect_locators(plan, &mut classified);
                if plan.get("enabled").and_then(Value::as_bool) == Some(true) {
                    collect_locators(plan, &mut active);
                }
            }
        }
    }
    let observability = raw.pointer("/grafana/observability");
    if let Some(value) = observability {
        collect_locators(value, &mut classified);
    }
    if let Some(value) =
        observability.filter(|v| v.get("enabled").and_then(Value::as_bool) == Some(true))
    {
        let mode = value
            .pointer("/prometheus/export_mode")
            .and_then(Value::as_str)
            .unwrap_or("scrape");
        for (path, used) in [
            ("/prometheus/scrape/auth", mode != "remote_write"),
            ("/prometheus/remote_write/auth", mode != "scrape"),
        ] {
            if used {
                if let Some(auth) = value
                    .pointer(path)
                    .filter(|auth| auth.get("mode").and_then(Value::as_str) == Some("bearer"))
                {
                    collect_locators(auth, &mut active);
                }
            }
        }
    }
    #[cfg(feature = "mapper-observability")]
    {
        crate::sql_observability::active_provider_refs(raw).map_err(|_| {
            SecretResolveError::MalformedSpec {
                id: Arc::from("notifications"),
                detail: "invalid SQL notification policy".into(),
            }
        })?;
        crate::sql_notifications::NotificationsConfig::parse(raw.get("notifications")).map_err(
            |_| SecretResolveError::MalformedSpec {
                id: Arc::from("notifications"),
                detail: "invalid notification provider configuration".into(),
            },
        )?;
    }
    // secret ID 可以被多个用途引用；未启用组件不能撤销其它业务子树明确声明的材料需求。
    collect_business_locators(raw, &mut active);
    // 只有独属于 controller 或其它非活跃用途的材料被排除，platform mode 自身不会取得控制面凭据。
    classified.retain(|id| !active.contains(id));
    Ok(classified)
}

/// 业务作用：保留框架观测用途之外的业务凭据引用，避免按 ID 排除时误伤共享材料。
/// 参数说明：`raw` 是完整候选树；`ids` 接收业务显式引用的 ID。
/// 返回：不读取材料；跳过 secret 定义及已单独裁决的观测、provider 子树。
fn collect_business_locators(raw: &Value, ids: &mut BTreeSet<String>) {
    let Some(root) = raw.as_object() else {
        return;
    };
    for (name, value) in root {
        let excluded_child = match name.as_str() {
            "secrets" | "secret_providers" | "object_stores" | "http_clients"
            | "schema_registries" | "ws_clients" => continue,
            "grafana" => Some("observability"),
            "notifications" => Some("providers"),
            _ => None,
        };
        if let Some(excluded_child) = excluded_child {
            if let Some(children) = value.as_object() {
                for (child, value) in children {
                    if child != excluded_child {
                        collect_locators(value, ids);
                    }
                }
            }
        } else {
            collect_locators(value, ids);
        }
    }
}

/// 业务作用：收集声明中的 secret locator，不读取环境或文件内容。
/// 参数说明：`value` 为用途限定子树，`ids` 接收稳定 ID。
/// 返回：无返回值；非 locator 字符串不被解释为材料。
fn collect_locators(value: &Value, ids: &mut BTreeSet<String>) {
    match value {
        Value::String(value) => {
            if let Some(id) = value.strip_prefix("secret://") {
                ids.insert(id.to_owned());
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_locators(value, ids);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_locators(value, ids);
            }
        }
        _ => {}
    }
}

/// 业务作用：判断候选是否需要外部材料，同步引导只做结构校验和脱敏。
/// 参数说明：`raw` 为原始树。
/// 返回：存在 provider fragment 时返回 true；非法声明直接拒绝。
pub(crate) fn requires_async(raw: &Value) -> Result<bool, SecretResolveError> {
    Ok(parse_specs(raw)?.iter().any(|spec| {
        spec.fragments
            .iter()
            .any(|fragment| matches!(fragment, SecretFragmentRef::Provider { .. }))
    }))
}

/// 业务作用：在首次材料消费之前构造只含脱敏值的结构视图，不访问任何外部 provider。
/// 参数说明：`raw` 为原始候选。
/// 返回：已经移除 ConfigPath 明文的副本；结构非法时拒绝。
pub(crate) fn redact_structure(raw: &Value) -> Result<Value, SecretResolveError> {
    let mut tree = raw.clone();
    for spec in parse_specs(raw)? {
        for fragment in spec.fragments {
            if let SecretFragmentRef::ConfigPath(path) = fragment {
                redact_path(&mut tree, &path);
            }
        }
    }
    Ok(tree)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(feature = "secret-vault"), allow(dead_code))]
struct ProviderPlan {
    #[serde(default)]
    enabled: bool,
    kind: Option<String>,
    endpoint: Option<String>,
    mount: Option<String>,
    token: Option<BootstrapToken>,
    timeout_ms: Option<u64>,
    max_response_bytes: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(not(feature = "secret-vault"), allow(dead_code))]
enum BootstrapToken {
    Env(String),
    File(PathBuf),
}

/// 业务作用：在消费者与凭据读取前校验 provider 声明的结构和容量。
/// 参数说明：`raw` 为候选配置；未启用声明不读取引导材料。
/// 返回：声明合法时成功；未知类型、缺失字段和越界配置拒绝。
pub(crate) fn validate_provider_plans(raw: &Value) -> Result<(), SecretResolveError> {
    parse_provider_plans(raw).map(|_| ())
}

/// 业务作用：解析有界 provider 计划，确保未被引用的启用项也具有完整声明。
/// 参数说明：`raw` 为原始配置树。
/// 返回：只含配置的计划表；不访问环境变量、文件或网络。
fn parse_provider_plans(raw: &Value) -> Result<BTreeMap<String, ProviderPlan>, SecretResolveError> {
    let plans: BTreeMap<String, ProviderPlan> = raw
        .get("secret_providers")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|_| provider_error("invalid provider declarations"))?
        .unwrap_or_default();
    if plans.len() > 16 {
        return Err(provider_error("secret provider count exceeds 16"));
    }
    for (name, plan) in &plans {
        if !plan.enabled {
            continue;
        }
        // 明确选择的 provider 必须可装配，避免无消费者时隐瞒错误或无限积累计划。
        if name.is_empty()
            || name.len() > 128
            || name.trim() != name
            || !matches!(plan.kind.as_deref(), Some("vault_kv2" | "openbao_kv2"))
        {
            return Err(provider_error("invalid provider name or kind"));
        }
        if plan
            .endpoint
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
            || plan.mount.as_deref().is_none_or(|value| {
                value.is_empty()
                    || value.len() > 256
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            })
            || plan.token.as_ref().is_none_or(|token| match token {
                BootstrapToken::Env(name) => name.is_empty() || name.contains(['=', '\0']),
                BootstrapToken::File(path) => path.as_os_str().is_empty(),
            })
        {
            return Err(provider_error(
                "provider endpoint, mount and bootstrap token are required",
            ));
        }
        if plan
            .timeout_ms
            .is_some_and(|value| value == 0 || value > 10_000)
            || plan
                .max_response_bytes
                .is_some_and(|value| value == 0 || value > 1024 * 1024)
        {
            return Err(provider_error(
                "provider limits are outside the supported range",
            ));
        }
    }
    Ok(plans)
}

/// 业务作用：从显式本地信任根建立 provider，并异步解析同代材料。
/// 参数说明：`raw` 为有界候选；`generation` 为发布代次。
/// 返回：全量材料成功才返回；最多 16 个 provider、256 个 secret，远端 I/O 有 15 秒总预算。
pub(crate) async fn resolve_async(
    raw: &Value,
    generation: u64,
) -> Result<SecretResolution, SecretResolveError> {
    let specs = active_specs(raw)?;
    let needed: BTreeSet<&str> = specs
        .iter()
        .flat_map(|spec| spec.fragments.iter())
        .filter_map(|fragment| match fragment {
            SecretFragmentRef::Provider { provider, .. } => Some(provider.as_ref()),
            _ => None,
        })
        .collect();
    let plans = parse_provider_plans(raw)?;
    #[allow(unused_mut)]
    let mut providers = nasecret::SecretProviderRegistry::new();
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        // 显式启用的 provider 即使尚无消费者，也不能把未知协议留到首次调用才发现。
        if !matches!(plan.kind.as_deref(), Some("vault_kv2" | "openbao_kv2")) {
            return Err(provider_error("unsupported secret provider kind"));
        }
        if !needed.contains(name.as_str()) {
            continue;
        }
        #[cfg(not(feature = "secret-vault"))]
        {
            let _ = (name, plan);
            return Err(provider_error(
                "external secret provider requires secret-vault feature",
            ));
        }
        #[cfg(feature = "secret-vault")]
        {
            let fragment = match plan
                .token
                .ok_or_else(|| provider_error("provider bootstrap token is required"))?
            {
                BootstrapToken::Env(name) => SecretFragmentRef::Env(Arc::from(name)),
                BootstrapToken::File(path) => SecretFragmentRef::File(path),
            };
            // bootstrap token 只能来自独立 env/file；配置不支持 provider 链，因此自引用与间接环都无入口。
            let token = SecretSpec {
                id: Arc::from("bootstrap-token"),
                fragments: vec![fragment],
                encoding: SecretEncoding::Raw,
                max_bytes: 8192,
            }
            .resolve(|_| None)
            .map_err(SecretResolveError::Resolve)?;
            let mut options = nasecret_vault::VaultOptions::default();
            if let Some(timeout) = plan.timeout_ms {
                if timeout == 0 || timeout > 10_000 {
                    return Err(provider_error(
                        "provider timeout must be within 1..=10000 ms",
                    ));
                }
                options.timeout = std::time::Duration::from_millis(timeout);
            }
            if let Some(max) = plan.max_response_bytes {
                options.max_response_bytes = max;
            }
            let provider = nasecret_vault::VaultKvV2Provider::new(
                plan.endpoint
                    .as_deref()
                    .ok_or_else(|| provider_error("provider endpoint is required"))?,
                plan.mount
                    .ok_or_else(|| provider_error("provider mount is required"))?,
                token,
                options,
            )
            .map_err(|_| provider_error("invalid provider trust root or bounds"))?;
            providers
                .register(Arc::<str>::from(name), Arc::new(provider))
                .map_err(|_| provider_error("invalid or duplicate provider name"))?;
        }
    }
    let snapshot = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        SecretSnapshot::resolve_async(generation, &specs, &providers, |path| {
            lookup_scalar(raw, path)
        }),
    )
    .await
    .map_err(|_| provider_error("secret candidate preparation exceeded its budget"))?
    .map_err(SecretResolveError::Resolve)?;
    Ok(SecretResolution {
        snapshot,
        redacted: redact_structure(raw)?,
        candidate_fingerprint: candidate_fingerprint(raw),
    })
}

/// 业务作用：为 provider 配置和准备失败生成不包含信任根细节的原因。
/// 参数说明：`detail` 为固定摘要。
/// 返回：仅归属秘密配置的结构错误。
fn provider_error(detail: &'static str) -> SecretResolveError {
    SecretResolveError::MalformedSpec {
        id: Arc::from("secret_providers"),
        detail: detail.to_owned(),
    }
}
