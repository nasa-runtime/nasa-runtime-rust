//! 有界 Confluent-compatible Schema Registry client 与 wire envelope。
//!
//! 该模块提供可独立使用的 Kafka codec 子能力，不启动 registry 服务端；命名受管实例的生命周期
//! 由 Application 适配层承担。生产默认禁止
//! 自动注册；业务数据面只按已批准 schema ID 拉取并使用有界正/负缓存。
//!
//! 数据面先校验 Confluent magic byte、payload 上限与 `ApprovedSchemaIds`，再按 ID 读取缓存或
//! Registry；控制面的兼容性检查与注册独立记账，不会稀释数据面命中率。endpoint、响应体、缓存、
//! timeout 与 credential 均有边界，错误不携带 endpoint、subject、schema 正文或认证信息。
//!
//! 本模块不生成 Avro/Protobuf/JSON codec，不决定 subject 命名、兼容级别、发布审批、ACL 或灾备。
//! 调用取消后远端结果未知，写入方必须依靠 Registry 去重语义和自己的发布流程安全重试。

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
    /// 参数说明：无。
    /// 返回：固定协议值，不采用 Rust 枚举的显示文本。
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
    /// 参数说明：value 为 Registry 的有符号 32 位 ID。
    /// 返回：正值包装为合法 ID；零或负值返回 InvalidSchemaId。
    pub fn new(value: i32) -> Result<Self, SchemaRegistryError> {
        if value <= 0 {
            return Err(SchemaRegistryError::InvalidSchemaId(value));
        }
        Ok(Self(value))
    }

    /// 业务作用：返回 wire 上的 i32 ID。
    /// 参数说明：无。
    /// 返回：已经过正值校验的 ID。
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
    /// 参数说明：无。
    /// 返回：默认不批准任何 schema 的集合。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：加入一个已批准 ID。
    /// 参数说明：id 为控制面批准的 Registry ID。
    /// 返回：首次加入为 true；重复加入不改变批准集合。
    pub fn insert(&mut self, id: SchemaId) -> bool {
        self.ids.insert(id)
    }

    /// 业务作用：判断 ID 是否已批准。
    /// 参数说明：id 为消息声明的 Registry ID。
    /// 返回：只有受信集合明确包含该 ID 时为 true。
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
    /// 参数说明：wire 为完整线路字节，max_payload_bytes 为正文上限，approved 为受信白名单。
    /// 返回：借用正文的 envelope；非法头、超限或未批准 ID 在任何网络访问前失败。
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
/// 参数说明：schema_id 为正 ID，payload 为 codec 正文，max_payload_bytes 为正文上限。
/// 返回：包含 magic byte 与大端 ID 的独立字节；超限不编码。
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
    /// 参数说明：formatter 为格式化目标。
    /// 返回：脱敏诊断；目标写入失败时传播格式化错误。
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

mod tls;
pub use tls::SchemaRegistryTls;

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
    /// 正/负缓存总容量，同时限制不同冷查询 ID 与实际并发 HTTP 请求数。
    pub cache_capacity: usize,
    /// 同时等待 schema 查询结果的业务调用上限，包含同 ID 的等待者；最大 65536。
    pub max_pending_lookups: usize,
    /// 成功 schema 的缓存时长。
    pub cache_ttl: Duration,
    /// 404 的负缓存时长。
    pub negative_cache_ttl: Duration,
    /// 是否允许运行时自动注册；生产缺省 false。
    pub auto_register: bool,
}

impl ConfluentRegistryOptions {
    /// 业务作用：创建生产保守缺省。
    /// 参数说明：endpoint 为受信 Registry 基址，构造 client 时继续验证。
    /// 返回：禁用自动注册、请求与正负缓存均有界的配置声明。
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth: None,
            request_timeout: Duration::from_secs(3),
            max_response_bytes: 1024 * 1024,
            cache_capacity: 256,
            max_pending_lookups: 4096,
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
    /// 参数说明：id 为所属 Registry 内的正 ID；业务使用前仍须验证角色白名单。
    /// 返回：共享 schema 或缺失、网络、协议等封闭错误。
    async fn schema_by_id(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError>;

    /// 业务作用：检查候选对 subject/version 的兼容性。
    /// 参数说明：subject/version 为控制目标，schema_type/schema 为候选协议与正文。
    /// 返回：兼容布尔值或封闭错误，不创建修订。
    async fn is_compatible(
        &self,
        subject: &str,
        version: &str,
        schema_type: RegistrySchemaType,
        schema: &str,
    ) -> Result<bool, SchemaRegistryError>;

    /// 业务作用：注册新的 schema 修订；adapter 必须显式启用 `auto_register`。
    /// 参数说明：subject 为目标，schema_type/schema 为候选协议与正文。
    /// 返回：远端分配的正 ID；超时或取消不能证明远端没有注册。
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
    /// 宿主已经关闭该代次客户端的新调用准入。
    Closed,
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
    /// 不同在途 ID 或 HTTP 请求已达到客户端容量边界。
    Capacity,
}

impl fmt::Display for SchemaRegistryError {
    /// 业务作用：输出稳定错误分类，不附带 endpoint、认证信息或 schema 正文。
    /// 参数说明：formatter 为诊断输出目标。
    /// 返回：仅写入封闭分类和非敏感数值；写入失败传播格式化错误。
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
    /// 参数说明：capacity 为正负缓存共用的条目上限。
    /// 返回：尚未持有 schema 或缺失证据的缓存。
    fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    /// 业务作用：读取未过期条目并刷新 LRU；外层 `Option` 表示未命中，内层表示正/负结果。
    /// 参数说明：id 为查询目标，now 为当前单调时刻。
    /// 返回：过期或无条目为 None，负命中为 Some(None)，正命中共享已缓存 schema。
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
    /// 参数说明：id 为身份，value 为正负结果，expires_at 为该条目的单调过期时刻。
    /// 返回：更新缓存与索引；必要时驱逐最久未访问条目。
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
    /// 参数说明：id 为已存在且仍有效的缓存条目。
    /// 返回：该条目成为最近访问项，不改变其过期时间。
    fn touch(&mut self, id: SchemaId) {
        if let Some(position) = self.lru.iter().position(|value| *value == id) {
            self.lru.remove(position);
        }
        self.lru.push_back(id);
    }

    /// 业务作用：同时删除条目与 LRU 索引，保持两份结构一致。
    /// 参数说明：id 为过期、被替代或需要驱逐的条目。
    /// 返回：不存在时无副作用，存在时释放缓存引用与索引占用。
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
    options: Arc<ConfluentRegistryOptions>,
    cache: Arc<Mutex<SchemaCache>>,
    /// 冷缓存 singleflight:同一 schema ID 的并发未命中只放行一个 leader 去访问 Registry,
    /// 其余调用等待该轮结束后重查缓存。防止冷启动或 Registry 故障时,解码并发被原样放大成
    /// 对 Registry 的请求风暴;leader 失败时等待者逐个接替(串行重试),不回到并行放大。
    inflight: Arc<Mutex<HashMap<SchemaId, tokio::sync::watch::Receiver<()>>>>,
    /// 数据查询与控制面共用 HTTP 准入额度，调用方取消等待不释放客户端持有的实际请求责任。
    http_permits: Arc<tokio::sync::Semaphore>,
    lookup_permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<SchemaRegistryMetrics>,
    closed: tokio::sync::watch::Sender<bool>,
}

/// 单个 schema ID 的一轮 singleflight 权限，由实际请求任务持有；
/// Drop 时先摘除在飞登记再释放 watch sender，等待者只在请求轮次结束后重新检查缓存。
struct InflightRound {
    registry: ConfluentSchemaRegistry,
    id: SchemaId,
    _sender: tokio::sync::watch::Sender<()>,
}

impl Drop for InflightRound {
    /// 业务作用：在客户端持有的请求轮次完成、到期或关闭时摘除登记并唤醒等待者。
    /// 调用方取消等待不析构此守卫，不能让其它等待者重复发起仍在执行的同 ID 请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；移除登记后 sender 随守卫释放，等待者可重查缓存或竞争下一轮。
    fn drop(&mut self) {
        self.registry
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
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
    ///
    /// 参数说明：
    /// - `options`：Registry endpoint、超时、响应上限与正负缓存策略。
    ///
    /// 返回：配置全部有界且 endpoint 可作 HTTP(S) 基址时返回 adapter；否则返回稳定配置错误。
    pub fn new(options: ConfluentRegistryOptions) -> Result<Self, SchemaRegistryError> {
        Self::build(options, None)
    }

    /// 业务作用：创建使用私有信任锚或 mTLS 身份的 Registry 客户端。
    /// 参数说明：options 为请求与缓存合同，tls 为独立 TLS 材料，endpoint 必须为 HTTPS。
    /// 返回：尚未请求远端的客户端；材料或 endpoint 非法时拒绝，证书及主机名验证始终开启。
    pub fn new_with_tls(
        options: ConfluentRegistryOptions,
        tls: SchemaRegistryTls,
    ) -> Result<Self, SchemaRegistryError> {
        Self::build(options, Some(tls))
    }

    /// 业务作用：统一默认与私有 TLS 客户端的边界校验及缓存初始化。
    /// 参数说明：options 为请求合同，tls 为可选独立 TLS 材料。
    /// 返回：完整构造或固定配置错误，不泄露原生解析文本。
    fn build(
        options: ConfluentRegistryOptions,
        tls: Option<SchemaRegistryTls>,
    ) -> Result<Self, SchemaRegistryError> {
        if options.cache_capacity == 0
            || options.max_pending_lookups == 0
            || options.max_pending_lookups > 65536
            || options.cache_capacity > tokio::sync::Semaphore::MAX_PERMITS
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
        if tls.is_some() && endpoint.scheme() != "https" {
            return Err(SchemaRegistryError::InvalidConfiguration);
        }
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
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(options.request_timeout)
            .min_tls_version(reqwest::tls::Version::TLS_1_2);
        if let Some(tls) = tls {
            builder = tls.apply(builder)?;
        }
        let client = builder
            .build()
            .map_err(|_| SchemaRegistryError::InvalidConfiguration)?;
        Ok(Self {
            endpoint,
            client,
            cache: Arc::new(Mutex::new(SchemaCache::new(options.cache_capacity))),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            http_permits: Arc::new(tokio::sync::Semaphore::new(options.cache_capacity)),
            lookup_permits: Arc::new(tokio::sync::Semaphore::new(options.max_pending_lookups)),
            metrics: Arc::new(SchemaRegistryMetrics::new()),
            closed: tokio::sync::watch::channel(false).0,
            options: Arc::new(options),
        })
    }

    /// 业务作用：为客户端接纳的有界请求保留资源责任，不随某个业务等待者退出而丢失。
    /// 参数说明：无。
    /// 返回：共享同一缓存、准入、关闭门禁及观测的内部句柄，不创建连接池。
    fn shared(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            options: self.options.clone(),
            cache: self.cache.clone(),
            inflight: self.inflight.clone(),
            http_permits: self.http_permits.clone(),
            lookup_permits: self.lookup_permits.clone(),
            metrics: self.metrics.clone(),
            closed: self.closed.clone(),
        }
    }

    /// 业务作用：查询客户端是否已经永久关闭新请求准入。
    /// 参数说明：无。
    /// 返回：关闭后恒为 true，不表示远端注册被撤回。
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// 业务作用：观测尚未归还客户端额度的实际 HTTP 请求责任。
    /// 参数说明：无。
    /// 返回：接纳后至请求任务退出的数量，不含同 ID 等待者；取消等待不减少该数量。
    pub fn in_flight(&self) -> usize {
        self.options.cache_capacity - self.http_permits.available_permits()
    }

    /// 业务作用：观测尚未完成或取消的 schema 业务等待数。
    /// 参数说明：无。
    /// 返回：包含 leader 和同 ID 等待者，与客户端仍持有的实际 HTTP 请求数量分开。
    pub fn pending_lookups(&self) -> usize {
        self.options.max_pending_lookups - self.lookup_permits.available_permits()
    }

    /// 业务作用：永久关闭新准入并通知已接纳请求停止本地读取。
    /// 参数说明：无。
    /// 返回：清空缓存并使旧句柄拒绝新请求；远端注册可能已发生，关闭不提供撤销保证。
    pub fn close(&self) {
        // 先收回准入，再通知请求和清空缓存，等待者醒来不能重新发起外部副作用。
        self.http_permits.close();
        self.lookup_permits.close();
        self.closed.send_replace(true);
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.entries.clear();
        cache.lru.clear();
    }

    /// 业务作用：在共享停止预算内等待实际 HTTP 责任全部归还。
    /// 参数说明：deadline 为单调时钟截止点；调用方通常先执行 close 阻止新准入。
    /// 返回：true 证明当前请求及查询轮次均已退出；false 表示预算耗尽，不能据此推定远端没有写入。
    pub async fn await_idle(&self, deadline: Instant) -> bool {
        loop {
            if self.in_flight() == 0
                && self
                    .inflight
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty()
            {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep_until(
                std::cmp::min(deadline, Instant::now() + Duration::from_millis(5)).into(),
            )
            .await;
        }
    }

    /// 业务作用：把已接纳 HTTP 的生命周期绑定到客户端，避免取消业务等待释放真实在途额度。
    /// 参数说明：work 为拥有输入与资源引用的一次请求，不能自动重试外部写操作；round 为可选查询权限，必须在 HTTP 额度归还后唤醒后继。
    /// 返回：请求结果、关闭或固定传输失败；等待者取消时任务仍受同一超时与关闭门禁约束。
    async fn detached<T, F>(
        &self,
        work: F,
        round: Option<InflightRound>,
    ) -> Result<T, SchemaRegistryError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, SchemaRegistryError>> + Send + 'static,
    {
        if self.is_closed() {
            return Err(SchemaRegistryError::Closed);
        }
        let permit = self.http_permits.clone().try_acquire_owned().map_err(|_| {
            if self.is_closed() {
                SchemaRegistryError::Closed
            } else {
                SchemaRegistryError::Capacity
            }
        })?;
        let mut closed = self.closed.subscribe();
        let deadline = tokio::time::Instant::now() + self.options.request_timeout;
        // 额度在派发前取得并由实际任务归还；JoinHandle 被丢弃只撤回等待，不中断任务。
        tokio::spawn(async move {
            // 后声明的额度守卫先释放，等待者被轮次退出唤醒时不会看到上一请求仍占用名额。
            let _round = round;
            let _permit = permit;
            if *closed.borrow() { return Err(SchemaRegistryError::Closed); }
            tokio::select! {
                biased;
                _ = closed.changed() => Err(SchemaRegistryError::Closed),
                result = tokio::time::timeout_at(deadline, work) => result.unwrap_or(Err(SchemaRegistryError::Transport)),
            }
        }).await.map_err(|_| SchemaRegistryError::Transport)?
    }

    /// 业务作用：在共享查询轮次的实际责任内读取并发布正负缓存。
    /// 参数说明：id 为已经取得 singleflight 权限的正 schema ID。
    /// 返回：合法定义或封闭错误；仅成功与真实 404 入缓存，关闭后不再发布缓存。
    async fn fetch_schema(
        &self,
        id: SchemaId,
    ) -> Result<Arc<RegisteredSchema>, SchemaRegistryError> {
        let url = self.url(&["schemas", "ids", &id.get().to_string()])?;
        let response = self
            .authenticate(self.client.get(url))
            .send()
            .await
            .map_err(|_| SchemaRegistryError::Transport)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.is_closed() {
                return Err(SchemaRegistryError::Closed);
            }
            cache.insert(
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
        // 未提供引用解析器时拒绝依赖外部定义的 schema，不能以不完整定义授权业务解码。
        if !body.references.is_empty() {
            return Err(SchemaRegistryError::InvalidResponse);
        }
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
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 关闭与缓存发布通过同一锁排序，停止后完成的旧请求不能重新填充缓存。
        if self.is_closed() {
            return Err(SchemaRegistryError::Closed);
        }
        cache.insert(
            id,
            CachedSchema::Hit(schema.clone()),
            Instant::now() + self.options.cache_ttl,
        );
        Ok(schema)
    }

    /// 业务作用：让控制面的 HTTP 请求与查询共用有界责任和关闭门禁。
    /// 参数说明：request 为已拥有候选正文和认证头的单次请求。
    /// 返回：有界 JSON 结果或固定失败；调用方取消不重试、不释放仍在执行的请求额度。
    async fn request_json<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, SchemaRegistryError> {
        let client = self.shared();
        self.detached(
            async move {
                let response = request
                    .send()
                    .await
                    .map_err(|_| SchemaRegistryError::Transport)?;
                if !response.status().is_success() {
                    return Err(SchemaRegistryError::RemoteStatus(
                        response.status().as_u16(),
                    ));
                }
                client.bounded_json(response).await
            },
            None,
        )
        .await
    }

    /// 业务作用：在保留 endpoint 基础路径的前提下安全追加已校验的 Registry 路径段。
    /// 参数说明：segments 为已验证的控制路径段。
    /// 返回：目标仍属于原 endpoint 的 URL；不可构造路径时返回配置错误。
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
    /// 参数说明：request 为目标已经限定到配置 endpoint 的请求。
    /// 返回：附有本认证域凭据的请求，不读取 Kafka 认证材料或消息内容。
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
    /// 参数说明：response 为已通过状态码校验的 HTTP 应答。
    /// 返回：符合结构的 JSON 投影；超限、传输或反序列化失败返回封闭错误。
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
    #[serde(default)]
    references: Vec<serde_json::Value>,
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
    /// 业务作用：优先读取正/负缓存，未命中时经 singleflight 按全局 ID 拉取并缓存 Registry schema。
    ///
    /// 同一 ID 的并发未命中只放行一个 leader 访问 Registry;其余调用等待该轮结束后重查缓存,
    /// leader 成功即全体命中,失败则等待者逐个接替(串行而非并行重试)。冷启动与 Registry 故障
    /// 期间对 Registry 的请求量因此以"每 ID 同时至多一个"为上界,不随解码并发放大。
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
        // 请求合并只限制远端并发，不能限制等待者内存；在登记轮次前另行取得业务等待额度。
        let _lookup_permit = match self.lookup_permits.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                accounting.finish(SchemaLookupOutcome::FetchFailed);
                return Err(if self.is_closed() {
                    SchemaRegistryError::Closed
                } else {
                    SchemaRegistryError::Capacity
                });
            }
        };
        let attempt: Result<
            (Arc<RegisteredSchema>, SchemaLookupOutcome),
            (SchemaRegistryError, SchemaLookupOutcome),
        > = async {
            // singleflight 领导权轮次:守卫存在期间同 ID 的其它调用都在等待本轮;必须在取得
            // 领导权后立刻持有，接纳后移交实际请求任务；拒绝路径释放权限，取消等待不结束请求轮次。
            let _round: InflightRound;
            if self.is_closed() {
                return Err((
                    SchemaRegistryError::Closed,
                    SchemaLookupOutcome::FetchFailed,
                ));
            }
            loop {
                if self.is_closed() {
                    return Err((
                        SchemaRegistryError::Closed,
                        SchemaLookupOutcome::FetchFailed,
                    ));
                }
                let cached = self
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(id, Instant::now());
                if let Some(value) = cached {
                    // 正负缓存都未访问 Registry，但负命中表示上游持续按不存在的 ID 解码，
                    // 必须与正命中分开观测，才能区分健康复用和无效流量。
                    // singleflight 等待后到达这里的调用同样按缓存结局记账——它没有访问 Registry。
                    return match value {
                        Some(schema) => Ok((schema, SchemaLookupOutcome::CacheHit)),
                        None => Err((
                            SchemaRegistryError::SchemaNotFound(id),
                            SchemaLookupOutcome::NegativeCacheHit,
                        )),
                    };
                }
                // 领导权裁决在锁内完成,守卫构造与等待都在锁外——map 锁绝不跨 await,
                // 守卫的 Drop(会再取同一把锁)也绝不在持锁路径上构造或析构。
                let joined = {
                    let mut inflight = self
                        .inflight
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match inflight.get(&id) {
                        Some(receiver) => Err(receiver.clone()),
                        None => {
                            // 高基数输入也必须有界；不能只限制完成后的缓存而放任冷请求占用内存。
                            if inflight.len() >= self.options.cache_capacity {
                                return Err((
                                    SchemaRegistryError::Capacity,
                                    SchemaLookupOutcome::FetchFailed,
                                ));
                            }
                            // 无在飞轮次:本调用成为 leader。先登记再发请求,窗口内到达的并发
                            // 调用都会订阅本轮;watch sender 随守卫在任何离开路径释放。
                            let (sender, receiver) = tokio::sync::watch::channel(());
                            inflight.insert(id, receiver);
                            Ok(sender)
                        }
                    }
                };
                match joined {
                    Ok(sender) => {
                        _round = InflightRound {
                            registry: self.shared(),
                            id,
                            _sender: sender,
                        };
                        break;
                    }
                    Err(mut receiver) => {
                        // 等待当前轮次结束(changed 在 sender drop 时以 Err 返回,即轮次结束
                        // 信号),然后回到循环开头重查缓存:leader 成功→命中;失败→接替为新 leader。
                        let _ = receiver.changed().await;
                    }
                }
            }

            // 缓存检查与轮次竞争分属两把锁；前一请求可能在两者之间完成，取得权限后必须复验，避免重复拉取。
            if let Some(cached) = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(id, Instant::now())
            {
                return match cached {
                    Some(schema) => Ok((schema, SchemaLookupOutcome::CacheHit)),
                    None => Err((
                        SchemaRegistryError::SchemaNotFound(id),
                        SchemaLookupOutcome::NegativeCacheHit,
                    )),
                };
            }
            let client = self.shared();
            let result = self
                .detached(async move { client.fetch_schema(id).await }, Some(_round))
                .await;
            match result {
                Ok(schema) => Ok((schema, SchemaLookupOutcome::Fetched)),
                Err(error @ SchemaRegistryError::SchemaNotFound(_)) => {
                    Err((error, SchemaLookupOutcome::FetchedMissing))
                }
                Err(error) => Err((error, SchemaLookupOutcome::FetchFailed)),
            }
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
            if self.is_closed() {
                return Err(SchemaRegistryError::Closed);
            }
            validate_subject(subject, version, schema)?;
            self.validate_schema_size(schema)?;
            let url = self.url(&["compatibility", "subjects", subject, "versions", version])?;
            let request = self
                .authenticate(self.client.post(url))
                .json(&SchemaRequest {
                    schema,
                    schema_type: schema_type.confluent_name(),
                });
            let body: CompatibilityResponse = self.request_json(request).await?;
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
            if self.is_closed() {
                return Err(SchemaRegistryError::Closed);
            }
            if !self.options.auto_register {
                return Err(SchemaRegistryError::AutoRegisterDisabled);
            }
            validate_subject(subject, "latest", schema)?;
            self.validate_schema_size(schema)?;
            let url = self.url(&["subjects", subject, "versions"])?;
            let request = self
                .authenticate(self.client.post(url))
                .json(&SchemaRequest {
                    schema,
                    schema_type: schema_type.confluent_name(),
                });
            let body: RegisterResponse = self.request_json(request).await?;
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
            SchemaRegistryError::Closed
            | SchemaRegistryError::InvalidConfiguration
            | SchemaRegistryError::InvalidEnvelope
            | SchemaRegistryError::UnsupportedMagic(_)
            | SchemaRegistryError::UnapprovedSchemaId(_)
            | SchemaRegistryError::PayloadTooLarge { .. }
            | SchemaRegistryError::SchemaTooLarge { .. }
            | SchemaRegistryError::InvalidSubject
            | SchemaRegistryError::AutoRegisterDisabled
            | SchemaRegistryError::Capacity,
        ) => SchemaControlOutcome::Rejected,
    }
}

impl ConfluentSchemaRegistry {
    /// 业务作用：复用响应体上限约束待提交 schema 文本，避免构造无界 JSON 请求。
    /// 参数说明：schema 为待检查或注册的 UTF-8 正文。
    /// 返回：未超限时成功；超限在网络请求前拒绝。
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
/// 参数说明：subject 为目标，version 为正整数文本或 latest，schema 为候选正文。
/// 返回：完整声明合法时成功；路径歧义或空正文返回 InvalidSubject。
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
/// 参数说明：auth 为独立 Registry 域的 Basic 或 Bearer 凭据。
/// 返回：认证材料合法时成功；空值、越界或控制字符返回配置错误。
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
