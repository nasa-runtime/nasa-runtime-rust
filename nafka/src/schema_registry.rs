//! 有界 Confluent-compatible Schema Registry client 与 wire envelope。
//!
//! 该模块是 Kafka codec 子能力，不拥有 Application 生命周期，也不启动 registry 服务端。生产默认禁止
//! 自动注册；业务数据面只按已批准 schema ID 拉取并使用有界正/负缓存。
//!
//! 数据面先校验 Confluent magic byte、payload 上限与 `ApprovedSchemaIds`，再按 ID 读取缓存或
//! Registry；控制面的兼容性检查与注册独立记账，不会稀释数据面命中率。endpoint、响应体、缓存、
//! timeout 与 credential 均有边界，错误不携带 endpoint、subject、schema 正文或认证信息。
//!
//! 本模块不生成 Avro/Protobuf/JSON codec，不决定 subject 命名、兼容级别、发布审批、ACL 或灾备。
//! 调用取消后远端结果未知，写入方必须依靠 Registry 去重语义和自己的发布流程安全重试。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// 业务作用：返回 Confluent HTTP 合同要求的标准大写 schema 类型名。
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
    /// 业务作用：构造正 schema ID。
    pub fn new(value: i32) -> Result<Self, SchemaRegistryError> {
        if value <= 0 {
            return Err(SchemaRegistryError::InvalidSchemaId(value));
        }
        Ok(Self(value))
    }

    /// 业务作用：返回 wire 上的 i32 ID。
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
    /// 业务作用：创建空白名单。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：加入一个已批准 ID。
    pub fn insert(&mut self, id: SchemaId) -> bool {
        self.ids.insert(id)
    }

    /// 业务作用：判断 ID 是否已批准。
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
    /// 业务作用：解码并执行 payload 上限与 schema ID 白名单。
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

/// 业务作用：编码 Confluent wire envelope。
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
    /// 业务作用：输出认证方式与非敏感用户名，同时固定隐藏 token 和密码。
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
    /// 业务作用：创建生产保守缺省。
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
    /// 业务作用：按全局 ID 获取 schema。
    async fn schema_by_id(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError>;

    /// 业务作用：检查候选对 subject/version 的兼容性。
    async fn is_compatible(
        &self,
        subject: &str,
        version: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<bool, SchemaRegistryError>;

    /// 业务作用：注册新的 schema 修订；adapter 必须显式启用 `auto_register`。
    async fn register(
        &self,
        subject: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<SchemaId, SchemaRegistryError>;
}

/// schema 查询的封闭结局；label 取值域固定，不随 schema ID 或 subject 扩张。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchemaLookupOutcome {
    /// 正缓存命中，未访问 registry。
    CacheHit,
    /// 负缓存命中，未访问 registry，直接判定不存在。
    NegativeCacheHit,
    /// 缓存未命中，向 registry 拉取并成功写回正缓存。
    Fetched,
    /// 缓存未命中，registry 明确返回不存在并写回负缓存。
    FetchedMissing,
    /// 缓存未命中，拉取因传输、状态码或应答格式失败，未污染缓存。
    FetchFailed,
    /// 查询 future 在返回结局前被丢弃；远端是否收到或完成请求未知。
    Cancelled,
}

impl SchemaLookupOutcome {
    /// 业务作用：返回结局的稳定低基数 label 值，供导出面使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与枚举一一对应的固定字符串。
    pub const fn label(self) -> &'static str {
        match self {
            Self::CacheHit => "cache_hit",
            Self::NegativeCacheHit => "negative_cache_hit",
            Self::Fetched => "fetched",
            Self::FetchedMissing => "fetched_missing",
            Self::FetchFailed => "fetch_failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// 全部结局的固定顺序；导出面据此生成稳定序列。
const SCHEMA_LOOKUP_OUTCOMES: [SchemaLookupOutcome; 6] = [
    SchemaLookupOutcome::CacheHit,
    SchemaLookupOutcome::NegativeCacheHit,
    SchemaLookupOutcome::Fetched,
    SchemaLookupOutcome::FetchedMissing,
    SchemaLookupOutcome::FetchFailed,
    SchemaLookupOutcome::Cancelled,
];

/// 一个查询结局的累计次数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaLookupCount {
    /// 结局分类。
    pub outcome: SchemaLookupOutcome,
    /// 累计次数。
    pub lookups: u64,
}

/// Registry 控制面操作；label 取值域固定，不随 subject 或 schema 类型扩张。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchemaControlOperation {
    /// 检查候选 schema 的兼容性。
    Compatibility,
    /// 注册新的 schema 修订。
    Register,
}

impl SchemaControlOperation {
    /// 业务作用：返回控制面操作的稳定低基数 label 值。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与枚举一一对应的固定字符串。
    pub const fn label(self) -> &'static str {
        match self {
            Self::Compatibility => "compatibility",
            Self::Register => "register",
        }
    }
}

/// Registry 控制面请求的封闭结局；远端状态码和 subject 不进入 label。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchemaControlOutcome {
    /// 远端成功应答且响应符合合同；兼容性结论为 false 也属于成功应答。
    Success,
    /// 本地输入、容量或写入授权门禁拒绝，请求未发出。
    Rejected,
    /// HTTP 传输或超时失败，远端是否完成处理未知。
    Transport,
    /// Registry 返回非成功状态码。
    RemoteStatus,
    /// Registry 成功应答的 body 或字段不符合合同。
    InvalidResponse,
    /// 控制面 future 在返回结局前被丢弃；远端是否收到或完成请求未知。
    Cancelled,
}

impl SchemaControlOutcome {
    /// 业务作用：返回控制面结局的稳定低基数 label 值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与枚举一一对应的固定字符串。
    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Transport => "transport",
            Self::RemoteStatus => "remote_status",
            Self::InvalidResponse => "invalid_response",
            Self::Cancelled => "cancelled",
        }
    }
}

const SCHEMA_CONTROL_OPERATIONS: [SchemaControlOperation; 2] = [
    SchemaControlOperation::Compatibility,
    SchemaControlOperation::Register,
];
const SCHEMA_CONTROL_OUTCOMES: [SchemaControlOutcome; 6] = [
    SchemaControlOutcome::Success,
    SchemaControlOutcome::Rejected,
    SchemaControlOutcome::Transport,
    SchemaControlOutcome::RemoteStatus,
    SchemaControlOutcome::InvalidResponse,
    SchemaControlOutcome::Cancelled,
];

/// 一个 Registry 控制面操作与结局组合的累计请求数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaControlCount {
    /// 控制面操作。
    pub operation: SchemaControlOperation,
    /// 请求结局。
    pub outcome: SchemaControlOutcome,
    /// 累计请求数。
    pub requests: u64,
}

/// client 自构造以来的累计观测事实。
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRegistrySnapshot {
    /// 按结局分类的累计查询次数，只包含非零组合。
    pub lookups: Vec<SchemaLookupCount>,
    /// 当前正负缓存合计占用的条目数。
    pub cached_entries: u64,
    /// 缓存条目上限，用于判断是否已被容量而非 TTL 驱逐。
    pub cache_capacity: u64,
    /// 按操作与结局分类的兼容性检查和注册请求，只包含非零组合。
    pub control_requests: Vec<SchemaControlCount>,
}

/// client 内部的原子计数；读取不清零。
#[derive(Debug)]
struct SchemaRegistryMetrics {
    lookups: [AtomicU64; SCHEMA_LOOKUP_OUTCOMES.len()],
    control_requests: [[AtomicU64; SCHEMA_CONTROL_OUTCOMES.len()]; SCHEMA_CONTROL_OPERATIONS.len()],
}

impl SchemaRegistryMetrics {
    /// 业务作用：创建全部计数为零的 client 观测状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由并发查询写入、导出面并发读取的原子状态。
    fn new() -> Self {
        Self {
            lookups: std::array::from_fn(|_| AtomicU64::new(0)),
            control_requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }

    /// 业务作用：记录一次查询结局，作为唯一记账点覆盖缓存命中与拉取的全部返回路径。
    ///
    /// 参数说明：
    /// - `outcome`: 本次查询的封闭结局。
    ///
    /// 返回：无；只累加，不清零已有计数。
    fn record(&self, outcome: SchemaLookupOutcome) {
        self.lookups[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录一次兼容性检查或注册请求的封闭结局。
    ///
    /// 参数说明：
    /// - `operation`: 本次控制面操作。
    /// - `outcome`: 本次请求的封闭结局。
    ///
    /// 返回：无；只累加，不清零已有计数。
    fn record_control(&self, operation: SchemaControlOperation, outcome: SchemaControlOutcome) {
        self.control_requests[operation as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：导出当前累计查询结局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只含非零结局的累计次数；读取不清零。
    fn snapshot(&self) -> Vec<SchemaLookupCount> {
        SCHEMA_LOOKUP_OUTCOMES
            .into_iter()
            .filter_map(|outcome| {
                let lookups = self.lookups[outcome as usize].load(Ordering::Relaxed);
                (lookups > 0).then_some(SchemaLookupCount { outcome, lookups })
            })
            .collect()
    }

    /// 业务作用：导出当前累计控制面请求结局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只含非零操作与结局组合的累计次数；读取不清零。
    fn control_snapshot(&self) -> Vec<SchemaControlCount> {
        SCHEMA_CONTROL_OPERATIONS
            .into_iter()
            .flat_map(|operation| {
                SCHEMA_CONTROL_OUTCOMES
                    .into_iter()
                    .filter_map(move |outcome| {
                        let requests = self.control_requests[operation as usize][outcome as usize]
                            .load(Ordering::Relaxed);
                        (requests > 0).then_some(SchemaControlCount {
                            operation,
                            outcome,
                            requests,
                        })
                    })
            })
            .collect()
    }
}

/// 一次数据面查询的完成守卫；future 在返回前被丢弃时由析构路径补记取消结局。
struct SchemaLookupAccounting<'a> {
    metrics: Option<&'a SchemaRegistryMetrics>,
}

impl<'a> SchemaLookupAccounting<'a> {
    /// 业务作用：在查询第一次被 poll 时取得唯一记账责任，使完成与取消路径共同守恒。
    ///
    /// 参数说明：
    /// - `metrics`: 本次查询所属 client 的累计观测状态。
    ///
    /// 返回：持有唯一记账责任的守卫；未显式完成时析构为 `Cancelled`。
    fn new(metrics: &'a SchemaRegistryMetrics) -> Self {
        Self {
            metrics: Some(metrics),
        }
    }

    /// 业务作用：发布正常返回路径的数据面结局，并解除析构路径的取消记账责任。
    ///
    /// 参数说明：
    /// - `outcome`: 缓存或远端查询已经得出的封闭结局。
    ///
    /// 返回：无；本次查询只发布一次结局。
    fn finish(mut self, outcome: SchemaLookupOutcome) {
        if let Some(metrics) = self.metrics.take() {
            metrics.record(outcome);
        }
    }
}

impl Drop for SchemaLookupAccounting<'_> {
    /// 业务作用：在查询 future 完成前被丢弃时发布取消结局，避免已开始的远端工作从观测面消失。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无；仅在正常完成尚未解除责任时记录一次数据面取消结局。
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.take() {
            metrics.record(SchemaLookupOutcome::Cancelled);
        }
    }
}

/// 一次控制面调用的完成守卫；按操作维度为提前丢弃的 future 补记取消结局。
struct SchemaControlAccounting<'a> {
    metrics: Option<&'a SchemaRegistryMetrics>,
    operation: SchemaControlOperation,
}

impl<'a> SchemaControlAccounting<'a> {
    /// 业务作用：在控制面调用第一次被 poll 时取得指定操作的唯一记账责任。
    ///
    /// 参数说明：
    /// - `metrics`: 本次调用所属 client 的累计观测状态。
    /// - `operation`: 兼容性检查或注册操作。
    ///
    /// 返回：持有唯一记账责任的守卫；未显式完成时析构为 `Cancelled`。
    fn new(metrics: &'a SchemaRegistryMetrics, operation: SchemaControlOperation) -> Self {
        Self {
            metrics: Some(metrics),
            operation,
        }
    }

    /// 业务作用：发布正常返回路径的控制面结局，并解除析构路径的取消记账责任。
    ///
    /// 参数说明：
    /// - `outcome`: 本地门禁或远端调用已经得出的封闭结局。
    ///
    /// 返回：无；本次控制面调用只发布一次结局。
    fn finish(mut self, outcome: SchemaControlOutcome) {
        if let Some(metrics) = self.metrics.take() {
            metrics.record_control(self.operation, outcome);
        }
    }
}

impl Drop for SchemaControlAccounting<'_> {
    /// 业务作用：在控制面 future 完成前被丢弃时按原操作发布取消结局。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无；仅在正常完成尚未解除责任时记录一次对应操作的取消结局。
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.take() {
            metrics.record_control(self.operation, SchemaControlOutcome::Cancelled);
        }
    }
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
    /// 业务作用：输出稳定错误分类，不附带 endpoint、认证信息或 schema 正文。
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
    /// 业务作用：创建空缓存；容量已由 adapter 构造器校验为正。
    fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    /// 业务作用：读取未过期条目并刷新 LRU；外层 `Option` 表示未命中，内层表示正/负结果。
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

    /// 业务作用：覆盖写入正或负结果，并按 LRU 淘汰到冻结容量以内。
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

    /// 业务作用：将已命中的 ID 移到 LRU 队尾。
    fn touch(&mut self, id: SchemaId) {
        if let Some(position) = self.lru.iter().position(|value| *value == id) {
            self.lru.remove(position);
        }
        self.lru.push_back(id);
    }

    /// 业务作用：同时删除条目与 LRU 索引，保持两份结构一致。
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
    metrics: SchemaRegistryMetrics,
}

impl ConfluentSchemaRegistry {
    /// 业务作用：读取 client 自构造以来的数据面查询、控制面请求与缓存事实，供业务接入自己的指标目录。
    ///
    /// 分母非零时，完成查询的正命中率按 `cache_hit / (cache_hit + fetched)` 计算；`cancelled`
    /// 没有查询结论，不进入该分母。命中率偏低且缓存占用接近容量时，表示数据面受容量驱逐影响。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只含非零结局的数据面与控制面累计次数、当前缓存条目数与容量上限；读取不清零。
    pub fn metrics_snapshot(&self) -> SchemaRegistrySnapshot {
        let cached_entries = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .len() as u64;
        SchemaRegistrySnapshot {
            lookups: self.metrics.snapshot(),
            cached_entries,
            cache_capacity: self.options.cache_capacity as u64,
            control_requests: self.metrics.control_snapshot(),
        }
    }

    /// 业务作用：校验配置并构造 adapter。
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
            metrics: SchemaRegistryMetrics::new(),
            options,
        })
    }

    /// 业务作用：在保留 endpoint 基础路径的前提下安全追加已校验的 Registry 路径段。
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

    /// 业务作用：按冻结认证配置附加 Authorization header，敏感中间字符串由 `Zeroizing` 承载。
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

    /// 业务作用：在 Content-Length 与流式累计两层限制下读取并反序列化 JSON 响应。
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
    /// 业务作用：优先读取正/负缓存，未命中时按全局 ID 拉取并缓存 Registry schema。
    ///
    /// 参数说明：
    /// - `id`: Registry 分配且已经过正数校验的全局 schema ID。
    ///
    /// 返回：正缓存命中或远端返回合法 schema 时返回共享 schema；负缓存命中、远端缺失、
    /// 传输失败或应答不符合合同时返回封闭错误，并为每次调用记录且只记录一个查询结局。
    async fn schema_by_id(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError> {
        let accounting = SchemaLookupAccounting::new(&self.metrics);
        let attempt: Result<
            (Arc<RegisteredSchema>, SchemaLookupOutcome),
            (SchemaRegistryError, SchemaLookupOutcome),
        > = async {
            let cached = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(id, Instant::now());
            if let Some(value) = cached {
                // 正负缓存都未访问 Registry，但负命中表示上游持续按不存在的 ID 解码，
                // 必须与正命中分开观测，才能区分健康复用和无效流量。
                return match value {
                    Some(schema) => Ok((schema, SchemaLookupOutcome::CacheHit)),
                    None => Err((
                        SchemaRegistryError::SchemaNotFound(id),
                        SchemaLookupOutcome::NegativeCacheHit,
                    )),
                };
            }

            let id_text = id.get().to_string();
            let url = self
                .url(&["schemas", "ids", &id_text])
                .map_err(|error| (error, SchemaLookupOutcome::FetchFailed))?;
            let response = self
                .authenticate(self.client.get(url))
                .send()
                .await
                .map_err(|_| {
                    (
                        SchemaRegistryError::Transport,
                        SchemaLookupOutcome::FetchFailed,
                    )
                })?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                self.cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(
                        id,
                        CachedSchema::Miss,
                        Instant::now() + self.options.negative_cache_ttl,
                    );
                return Err((
                    SchemaRegistryError::SchemaNotFound(id),
                    SchemaLookupOutcome::FetchedMissing,
                ));
            }
            if !response.status().is_success() {
                return Err((
                    SchemaRegistryError::RemoteStatus(response.status().as_u16()),
                    SchemaLookupOutcome::FetchFailed,
                ));
            }
            let body: SchemaByIdResponse = self
                .bounded_json(response)
                .await
                .map_err(|error| (error, SchemaLookupOutcome::FetchFailed))?;
            let schema_type = match body.schema_type.as_deref().unwrap_or("AVRO") {
                "AVRO" => RegistrySchemaType::Avro,
                "PROTOBUF" => RegistrySchemaType::Protobuf,
                "JSON" => RegistrySchemaType::Json,
                _ => {
                    return Err((
                        SchemaRegistryError::InvalidResponse,
                        SchemaLookupOutcome::FetchFailed,
                    ));
                }
            };
            if body.schema.is_empty() {
                return Err((
                    SchemaRegistryError::InvalidResponse,
                    SchemaLookupOutcome::FetchFailed,
                ));
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
            Ok((schema, SchemaLookupOutcome::Fetched))
        }
        .await;

        let (result, outcome) = match attempt {
            Ok((schema, outcome)) => (Ok(schema), outcome),
            Err((error, outcome)) => (Err(error), outcome),
        };
        // 正常返回与析构取消共享唯一记账责任，既不让提前返回漏记，也不让完成路径重复计数。
        accounting.finish(outcome);
        result
    }

    /// 业务作用：向 Registry 查询候选 schema 对指定 subject/version 的兼容性。
    ///
    /// 参数说明：
    /// - `subject`: 已由业务选择的 Registry subject。
    /// - `version`: 正整数文本或 `latest`。
    /// - `schema_type`: 候选 schema 的协议类型。
    /// - `schema`: 待检查且受响应体同一容量上限约束的 schema 文本。
    ///
    /// 返回：远端成功应答时返回兼容性结论；本地门禁、传输、状态码或应答合同失败时返回封闭错误，
    /// 并把结局计入控制面观测。
    async fn is_compatible(
        &self,
        subject: &str,
        version: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<bool, SchemaRegistryError> {
        let accounting =
            SchemaControlAccounting::new(&self.metrics, SchemaControlOperation::Compatibility);
        // 记账包住整个控制面调用，使本地门禁、URL 派生、传输与应答解析共享唯一出口；
        // 后续新增提前返回也不会让使用期请求从观测面消失。
        let result: Result<bool, SchemaRegistryError> = async {
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
        .await;
        accounting.finish(schema_control_outcome(&result));
        result
    }

    /// 业务作用：在显式开启自动注册后提交候选 schema，并校验返回的正 ID。
    ///
    /// 参数说明：
    /// - `subject`: 新修订所属的 Registry subject。
    /// - `schema_type`: 待注册 schema 的协议类型。
    /// - `schema`: 待注册且受响应体同一容量上限约束的 schema 文本。
    ///
    /// 返回：写入授权开启且远端返回正 ID 时成功；本地门禁、传输、状态码或应答合同失败时返回
    /// 封闭错误，并把结局计入控制面观测。
    async fn register(
        &self,
        subject: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<SchemaId, SchemaRegistryError> {
        let accounting =
            SchemaControlAccounting::new(&self.metrics, SchemaControlOperation::Register);
        let result: Result<SchemaId, SchemaRegistryError> = async {
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
        .await;
        accounting.finish(schema_control_outcome(&result));
        result
    }
}

/// 业务作用：把控制面调用结果折叠成稳定低基数结局，避免 subject、状态码或正文进入指标。
///
/// 参数说明：
/// - `result`: 兼容性检查或注册的完整调用结果。
///
/// 返回：成功、门禁拒绝、传输、远端状态或应答合同五类之一。
fn schema_control_outcome<T>(result: &Result<T, SchemaRegistryError>) -> SchemaControlOutcome {
    match result {
        Ok(_) => SchemaControlOutcome::Success,
        Err(SchemaRegistryError::Transport) => SchemaControlOutcome::Transport,
        Err(SchemaRegistryError::SchemaNotFound(_) | SchemaRegistryError::RemoteStatus(_)) => {
            SchemaControlOutcome::RemoteStatus
        }
        Err(
            SchemaRegistryError::ResponseTooLarge
            | SchemaRegistryError::InvalidResponse
            | SchemaRegistryError::InvalidSchemaId(_),
        ) => SchemaControlOutcome::InvalidResponse,
        Err(
            SchemaRegistryError::InvalidConfiguration
            | SchemaRegistryError::InvalidEnvelope
            | SchemaRegistryError::UnsupportedMagic(_)
            | SchemaRegistryError::UnapprovedSchemaId(_)
            | SchemaRegistryError::PayloadTooLarge { .. }
            | SchemaRegistryError::SchemaTooLarge { .. }
            | SchemaRegistryError::InvalidSubject
            | SchemaRegistryError::AutoRegisterDisabled,
        ) => SchemaControlOutcome::Rejected,
    }
}

impl ConfluentSchemaRegistry {
    /// 业务作用：复用响应体上限约束待提交 schema 文本，避免构造无界 JSON 请求。
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

/// 业务作用：校验 subject 路径安全性、version 规范形式以及非空 schema。
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

/// 业务作用：限制认证材料长度与字符集，确保后续 header 构造不会接收控制字符或无界输入。
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
