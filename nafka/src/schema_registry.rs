//! 实验性 Confluent-compatible Schema Registry client 与 wire envelope。
//!
//! 该模块是 Kafka codec 子能力，不拥有 Application 生命周期，也不启动 registry 服务端。生产默认禁止
//! 自动注册；业务数据面只按已批准 schema ID 拉取并使用有界正/负缓存。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use nasecret::SecretBytes;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

/// Confluent wire format 的固定 magic byte。
pub const CONFLUENT_MAGIC_BYTE: u8 = 0;
/// Confluent envelope 的 magic byte + i32 schema ID 长度。
pub const CONFLUENT_HEADER_LEN: usize = 5;
/// Registry JSON 响应与待提交 schema 文本的框架硬上限。
pub const MAX_SCHEMA_REGISTRY_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// HTTP timeout 与正/负缓存窗口的硬上限，避免不可信配置在计时器或 `Instant` 加法处溢出。
const MAX_REGISTRY_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Registry 支持的 schema 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RegistrySchemaType {
    /// Apache Avro。
    Avro,
    /// Protocol Buffers。
    Protobuf,
    /// JSON Schema。
    Json,
}

impl RegistrySchemaType {
    /// 返回 Confluent HTTP 合同要求的标准大写 schema 类型名。
    fn confluent_name(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Protobuf => "PROTOBUF",
            Self::Json => "JSON",
        }
    }
}

/// 已由 registry 分配的正 schema ID。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaId(i32);

impl SchemaId {
    /// 构造正 schema ID。
    pub fn new(value: i32) -> Result<Self, SchemaRegistryError> {
        if value <= 0 {
            return Err(SchemaRegistryError::InvalidSchemaId(value));
        }
        Ok(Self(value))
    }

    /// 返回 wire 上的 i32 ID。
    pub fn get(self) -> i32 {
        self.0
    }
}

/// 从 registry 读取并批准使用的 schema。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    /// 全局 schema ID。
    pub id: SchemaId,
    /// schema 类型。
    pub schema_type: RegistrySchemaType,
    /// 完整 schema 文本。
    pub schema: Arc<str>,
}

/// 数据面允许解码的 schema ID 白名单。
#[derive(Debug, Clone, Default)]
pub struct ApprovedSchemaIds {
    ids: BTreeSet<SchemaId>,
}

impl ApprovedSchemaIds {
    /// 创建空白名单。
    pub fn new() -> Self {
        Self::default()
    }

    /// 加入一个已批准 ID。
    pub fn insert(&mut self, id: SchemaId) -> bool {
        self.ids.insert(id)
    }

    /// 判断 ID 是否已批准。
    pub fn contains(&self, id: SchemaId) -> bool {
        self.ids.contains(&id)
    }
}

/// 解出的 Confluent wire envelope；payload 借用原始 Kafka record。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfluentEnvelope<'a> {
    /// schema ID。
    pub schema_id: SchemaId,
    /// 不含 5 字节头的 codec payload。
    pub payload: &'a [u8],
}

impl<'a> ConfluentEnvelope<'a> {
    /// 解码并执行 payload 上限与 schema ID 白名单。
    pub fn decode(
        wire: &'a [u8],
        max_payload_bytes: usize,
        approved: &ApprovedSchemaIds,
    ) -> Result<Self, SchemaRegistryError> {
        if wire.len() < CONFLUENT_HEADER_LEN {
            return Err(SchemaRegistryError::InvalidEnvelope);
        }
        if wire[0] != CONFLUENT_MAGIC_BYTE {
            return Err(SchemaRegistryError::UnsupportedMagic(wire[0]));
        }
        let schema_id = SchemaId::new(i32::from_be_bytes(
            wire[1..CONFLUENT_HEADER_LEN]
                .try_into()
                .map_err(|_| SchemaRegistryError::InvalidEnvelope)?,
        ))?;
        let payload = &wire[CONFLUENT_HEADER_LEN..];
        if payload.len() > max_payload_bytes {
            return Err(SchemaRegistryError::PayloadTooLarge {
                actual: payload.len(),
                max: max_payload_bytes,
            });
        }
        if !approved.contains(schema_id) {
            return Err(SchemaRegistryError::UnapprovedSchemaId(schema_id));
        }
        Ok(Self { schema_id, payload })
    }
}

/// 编码 Confluent wire envelope。
pub fn encode_confluent(
    schema_id: SchemaId,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<Vec<u8>, SchemaRegistryError> {
    if payload.len() > max_payload_bytes {
        return Err(SchemaRegistryError::PayloadTooLarge {
            actual: payload.len(),
            max: max_payload_bytes,
        });
    }
    let mut wire = Vec::with_capacity(CONFLUENT_HEADER_LEN + payload.len());
    wire.push(CONFLUENT_MAGIC_BYTE);
    wire.extend_from_slice(&schema_id.get().to_be_bytes());
    wire.extend_from_slice(payload);
    Ok(wire)
}

/// Schema Registry 认证信息；Debug 不输出 credential。
pub enum SchemaRegistryAuth {
    /// Bearer token。
    Bearer(SecretBytes),
    /// HTTP Basic username/password。
    Basic {
        /// 非敏感用户名。
        username: Arc<str>,
        /// 敏感密码。
        password: SecretBytes,
    },
}

impl fmt::Debug for SchemaRegistryAuth {
    /// 输出认证方式与非敏感用户名，同时固定隐藏 token 和密码。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bearer(_) => formatter.write_str("Bearer(<redacted>)"),
            Self::Basic { username, .. } => formatter
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
        }
    }
}

/// Confluent adapter 的有界配置。
#[derive(Debug)]
pub struct ConfluentRegistryOptions {
    /// Registry 根 URL。
    pub endpoint: String,
    /// 可选认证。
    pub auth: Option<SchemaRegistryAuth>,
    /// 单次 HTTP 总超时。
    pub request_timeout: Duration,
    /// 响应 body 硬上限。
    pub max_response_bytes: usize,
    /// 正/负缓存总容量。
    pub cache_capacity: usize,
    /// 成功 schema 的缓存时长。
    pub cache_ttl: Duration,
    /// 404 的负缓存时长。
    pub negative_cache_ttl: Duration,
    /// 是否允许运行时自动注册；生产缺省 false。
    pub auto_register: bool,
}

impl ConfluentRegistryOptions {
    /// 创建生产保守缺省。
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth: None,
            request_timeout: Duration::from_secs(3),
            max_response_bytes: 1024 * 1024,
            cache_capacity: 256,
            cache_ttl: Duration::from_secs(300),
            negative_cache_ttl: Duration::from_secs(5),
            auto_register: false,
        }
    }
}

/// Registry client 的 provider-neutral 合同。
#[async_trait::async_trait]
pub trait SchemaRegistryClient: Send + Sync {
    /// 按全局 ID 获取 schema。
    async fn schema_by_id(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError>;

    /// 检查候选对 subject/version 的兼容性。
    async fn is_compatible(
        &self,
        subject: &str,
        version: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<bool, SchemaRegistryError>;

    /// 注册新的 schema 修订；adapter 必须显式启用 `auto_register`。
    async fn register(
        &self,
        subject: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<SchemaId, SchemaRegistryError>;
}

/// 脱敏、有限分类的 registry 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRegistryError {
    /// endpoint/options 不合法。
    InvalidConfiguration,
    /// schema ID 必须为正数。
    InvalidSchemaId(i32),
    /// wire envelope 太短。
    InvalidEnvelope,
    /// magic byte 不支持。
    UnsupportedMagic(u8),
    /// schema ID 未在业务批准集合中。
    UnapprovedSchemaId(SchemaId),
    /// payload 超过硬上限。
    PayloadTooLarge {
        /// 实际字节数。
        actual: usize,
        /// 上限。
        max: usize,
    },
    /// Registry 返回 404。
    SchemaNotFound(SchemaId),
    /// HTTP 传输/超时失败。
    Transport,
    /// Registry 返回非成功状态。
    RemoteStatus(u16),
    /// 响应 body 超过上限。
    ResponseTooLarge,
    /// 待提交的 schema 文本超过配置上限。
    SchemaTooLarge {
        /// 实际 UTF-8 字节数。
        actual: usize,
        /// 配置上限。
        max: usize,
    },
    /// 响应 JSON/字段不符合合同。
    InvalidResponse,
    /// subject/version 不合法。
    InvalidSubject,
    /// 运行期自动注册未显式开启。
    AutoRegisterDisabled,
}

impl fmt::Display for SchemaRegistryError {
    /// 输出稳定错误分类，不附带 endpoint、认证信息或 schema 正文。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "schema registry error: {self:?}")
    }
}

impl std::error::Error for SchemaRegistryError {}

/// schema 正缓存与 404 负缓存的统一值域。
enum CachedSchema {
    Hit(Arc<RegisteredSchema>),
    Miss,
}

/// 带单调过期时刻的单个 schema 缓存条目。
struct CacheEntry {
    value: CachedSchema,
    expires_at: Instant,
}

/// 固定容量的 schema ID LRU，正负结果共享同一容量预算。
struct SchemaCache {
    entries: BTreeMap<SchemaId, CacheEntry>,
    lru: VecDeque<SchemaId>,
    capacity: usize,
}

impl SchemaCache {
    /// 创建空缓存；容量已由 adapter 构造器校验为正。
    fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    /// 读取未过期条目并刷新 LRU；外层 `Option` 表示未命中，内层表示正/负结果。
    fn get(&mut self, id: SchemaId, now: Instant) -> Option<Option<Arc<RegisteredSchema>>> {
        let expired = self
            .entries
            .get(&id)
            .is_some_and(|entry| entry.expires_at <= now);
        if expired {
            self.remove(id);
            return None;
        }
        let value = self.entries.get(&id).map(|entry| match &entry.value {
            CachedSchema::Hit(schema) => Some(Arc::clone(schema)),
            CachedSchema::Miss => None,
        })?;
        self.touch(id);
        Some(value)
    }

    /// 覆盖写入正或负结果，并按 LRU 淘汰到冻结容量以内。
    fn insert(&mut self, id: SchemaId, value: CachedSchema, expires_at: Instant) {
        self.remove(id);
        self.entries.insert(id, CacheEntry { value, expires_at });
        self.lru.push_back(id);
        while self.entries.len() > self.capacity {
            if let Some(oldest) = self.lru.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }

    /// 将已命中的 ID 移到 LRU 队尾。
    fn touch(&mut self, id: SchemaId) {
        if let Some(position) = self.lru.iter().position(|value| *value == id) {
            self.lru.remove(position);
        }
        self.lru.push_back(id);
    }

    /// 同时删除条目与 LRU 索引，保持两份结构一致。
    fn remove(&mut self, id: SchemaId) {
        self.entries.remove(&id);
        if let Some(position) = self.lru.iter().position(|value| *value == id) {
            self.lru.remove(position);
        }
    }
}

/// Confluent-compatible HTTP adapter。
pub struct ConfluentSchemaRegistry {
    endpoint: reqwest::Url,
    client: reqwest::Client,
    options: ConfluentRegistryOptions,
    cache: Mutex<SchemaCache>,
}

impl ConfluentSchemaRegistry {
    /// 校验配置并构造 adapter。
    pub fn new(options: ConfluentRegistryOptions) -> Result<Self, SchemaRegistryError> {
        if options.cache_capacity == 0
            || options.max_response_bytes == 0
            || options.max_response_bytes > MAX_SCHEMA_REGISTRY_RESPONSE_BYTES
            || options.request_timeout.is_zero()
            || options.cache_ttl.is_zero()
            || options.negative_cache_ttl.is_zero()
            || options.request_timeout > MAX_REGISTRY_DURATION
            || options.cache_ttl > MAX_REGISTRY_DURATION
            || options.negative_cache_ttl > MAX_REGISTRY_DURATION
            || Instant::now().checked_add(options.cache_ttl).is_none()
            || Instant::now()
                .checked_add(options.negative_cache_ttl)
                .is_none()
        {
            return Err(SchemaRegistryError::InvalidConfiguration);
        }
        let endpoint = reqwest::Url::parse(&options.endpoint)
            .map_err(|_| SchemaRegistryError::InvalidConfiguration)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.cannot_be_a_base()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(SchemaRegistryError::InvalidConfiguration);
        }
        if endpoint.scheme() == "http"
            && !endpoint
                .host_str()
                .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"))
        {
            return Err(SchemaRegistryError::InvalidConfiguration);
        }
        if let Some(auth) = &options.auth {
            validate_auth(auth)?;
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(options.request_timeout)
            .build()
            .map_err(|_| SchemaRegistryError::InvalidConfiguration)?;
        Ok(Self {
            endpoint,
            client,
            cache: Mutex::new(SchemaCache::new(options.cache_capacity)),
            options,
        })
    }

    /// 在保留 endpoint 基础路径的前提下安全追加已校验的 Registry 路径段。
    fn url(&self, segments: &[&str]) -> Result<reqwest::Url, SchemaRegistryError> {
        let mut url = self.endpoint.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| SchemaRegistryError::InvalidConfiguration)?;
        path.pop_if_empty();
        path.extend(segments);
        drop(path);
        Ok(url)
    }

    /// 按冻结认证配置附加 Authorization header，敏感中间字符串由 `Zeroizing` 承载。
    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.options.auth {
            None => request,
            Some(SchemaRegistryAuth::Bearer(token)) => {
                let value = std::str::from_utf8(token.expose())
                    .expect("Schema Registry auth was validated during construction");
                request.bearer_auth(value)
            }
            Some(SchemaRegistryAuth::Basic { username, password }) => {
                let raw = Zeroizing::new(format!(
                    "{}:{}",
                    username,
                    std::str::from_utf8(password.expose())
                        .expect("Schema Registry auth was validated during construction")
                ));
                let encoded = Zeroizing::new(
                    base64::engine::general_purpose::STANDARD.encode(raw.as_bytes()),
                );
                let authorization = Zeroizing::new(format!("Basic {}", encoded.as_str()));
                request.header(reqwest::header::AUTHORIZATION, authorization.as_str())
            }
        }
    }

    /// 在 Content-Length 与流式累计两层限制下读取并反序列化 JSON 响应。
    async fn bounded_json<T: for<'de> Deserialize<'de>>(
        &self,
        mut response: reqwest::Response,
    ) -> Result<T, SchemaRegistryError> {
        if response
            .content_length()
            .is_some_and(|length| length > self.options.max_response_bytes as u64)
        {
            return Err(SchemaRegistryError::ResponseTooLarge);
        }
        let initial_capacity = response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default()
            .min(self.options.max_response_bytes);
        let mut bytes = Vec::with_capacity(initial_capacity);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| SchemaRegistryError::Transport)?
        {
            if bytes.len().saturating_add(chunk.len()) > self.options.max_response_bytes {
                return Err(SchemaRegistryError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| SchemaRegistryError::InvalidResponse)
    }
}

/// Registry `GET /schemas/ids/{id}` 的最小响应投影。
#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

/// Registry compatibility API 的最小布尔响应投影。
#[derive(Deserialize)]
struct CompatibilityResponse {
    is_compatible: bool,
}

/// Registry 注册 API 返回的新 schema ID。
#[derive(Deserialize)]
struct RegisterResponse {
    id: i32,
}

/// compatibility 与 register API 共用的有界 schema 请求体。
#[derive(Serialize)]
struct SchemaRequest<'a> {
    schema: &'a str,
    #[serde(rename = "schemaType")]
    schema_type: &'static str,
}

#[async_trait::async_trait]
impl SchemaRegistryClient for ConfluentSchemaRegistry {
    /// 优先读取正/负缓存，未命中时按全局 ID 拉取并缓存 Registry schema。
    async fn schema_by_id(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError> {
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id, Instant::now());
        if let Some(value) = cached {
            return value.ok_or(SchemaRegistryError::SchemaNotFound(id));
        }

        let id_text = id.get().to_string();
        let url = self.url(&["schemas", "ids", &id_text])?;
        let response = self
            .authenticate(self.client.get(url))
            .send()
            .await
            .map_err(|_| SchemaRegistryError::Transport)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(
                    id,
                    CachedSchema::Miss,
                    Instant::now() + self.options.negative_cache_ttl,
                );
            return Err(SchemaRegistryError::SchemaNotFound(id));
        }
        if !response.status().is_success() {
            return Err(SchemaRegistryError::RemoteStatus(
                response.status().as_u16(),
            ));
        }
        let body: SchemaByIdResponse = self.bounded_json(response).await?;
        let schema_type = match body.schema_type.as_deref().unwrap_or("AVRO") {
            "AVRO" => RegistrySchemaType::Avro,
            "PROTOBUF" => RegistrySchemaType::Protobuf,
            "JSON" => RegistrySchemaType::Json,
            _ => return Err(SchemaRegistryError::InvalidResponse),
        };
        if body.schema.is_empty() {
            return Err(SchemaRegistryError::InvalidResponse);
        }
        let schema = Arc::new(RegisteredSchema {
            id,
            schema_type,
            schema: Arc::from(body.schema),
        });
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                id,
                CachedSchema::Hit(Arc::clone(&schema)),
                Instant::now() + self.options.cache_ttl,
            );
        Ok(schema)
    }

    /// 向 Registry 查询候选 schema 对指定 subject/version 的兼容性。
    async fn is_compatible(
        &self,
        subject: &str,
        version: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<bool, SchemaRegistryError> {
        validate_subject(subject, version, schema)?;
        self.validate_schema_size(schema)?;
        let url = self.url(&["compatibility", "subjects", subject, "versions", version])?;
        let response = self
            .authenticate(self.client.post(url))
            .json(&SchemaRequest {
                schema,
                schema_type: schema_type.confluent_name(),
            })
            .send()
            .await
            .map_err(|_| SchemaRegistryError::Transport)?;
        if !response.status().is_success() {
            return Err(SchemaRegistryError::RemoteStatus(
                response.status().as_u16(),
            ));
        }
        let body: CompatibilityResponse = self.bounded_json(response).await?;
        Ok(body.is_compatible)
    }

    /// 在显式开启自动注册后提交候选 schema，并校验返回的正 ID。
    async fn register(
        &self,
        subject: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<SchemaId, SchemaRegistryError> {
        if !self.options.auto_register {
            return Err(SchemaRegistryError::AutoRegisterDisabled);
        }
        validate_subject(subject, "latest", schema)?;
        self.validate_schema_size(schema)?;
        let url = self.url(&["subjects", subject, "versions"])?;
        let response = self
            .authenticate(self.client.post(url))
            .json(&SchemaRequest {
                schema,
                schema_type: schema_type.confluent_name(),
            })
            .send()
            .await
            .map_err(|_| SchemaRegistryError::Transport)?;
        if !response.status().is_success() {
            return Err(SchemaRegistryError::RemoteStatus(
                response.status().as_u16(),
            ));
        }
        let body: RegisterResponse = self.bounded_json(response).await?;
        SchemaId::new(body.id)
    }
}

impl ConfluentSchemaRegistry {
    /// 复用响应体上限约束待提交 schema 文本，避免构造无界 JSON 请求。
    fn validate_schema_size(&self, schema: &str) -> Result<(), SchemaRegistryError> {
        if schema.len() > self.options.max_response_bytes {
            return Err(SchemaRegistryError::SchemaTooLarge {
                actual: schema.len(),
                max: self.options.max_response_bytes,
            });
        }
        Ok(())
    }
}

/// 校验 subject 路径安全性、version 规范形式以及非空 schema。
fn validate_subject(subject: &str, version: &str, schema: &str) -> Result<(), SchemaRegistryError> {
    let subject_is_safe = !subject.is_empty()
        && subject.len() <= 255
        && !matches!(subject, "." | "..")
        && !subject.chars().any(char::is_control)
        && !subject.contains('/');
    let version_is_safe = version == "latest"
        || version
            .parse::<u32>()
            .is_ok_and(|parsed| parsed > 0 && parsed.to_string() == version);
    if !subject_is_safe || !version_is_safe || schema.is_empty() {
        return Err(SchemaRegistryError::InvalidSubject);
    }
    Ok(())
}

/// 限制认证材料长度与字符集，确保后续 header 构造不会接收控制字符或无界输入。
fn validate_auth(auth: &SchemaRegistryAuth) -> Result<(), SchemaRegistryError> {
    let valid = |value: &[u8]| {
        !value.is_empty() && value.len() <= 4096 && value.iter().all(|byte| byte.is_ascii_graphic())
    };
    match auth {
        SchemaRegistryAuth::Bearer(token) if valid(token.expose()) => Ok(()),
        SchemaRegistryAuth::Basic { username, password }
            if !username.is_empty()
                && username.len() <= 255
                && !username.chars().any(char::is_control)
                && !username.contains(':')
                && valid(password.expose()) =>
        {
            Ok(())
        }
        _ => Err(SchemaRegistryError::InvalidConfiguration),
    }
}
