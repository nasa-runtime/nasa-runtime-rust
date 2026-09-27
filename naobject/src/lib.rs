//! 有界对象存储合同与 S3-compatible SigV4 adapter。
//!
//! 当前只接受有硬上限的单对象缓冲，不提供 multipart 或无限流式上传。独立 adapter 的生命周期
//! 由调用方负责；Application 通过 object_stores 命名计划装配凭据、健康、关闭门禁与在途等待。
//!
//! `ObjectStore` 封闭业务读写语义，`S3ObjectStore` 依次执行本地 key/容量门禁、path-style SigV4、
//! 禁止重定向的有界 HTTP 请求和 SHA-256 metadata 复核。`CreateOnly` 由远端条件写裁决，取消只表示
//! 调用方停止等待，不证明远端未执行；重试写入必须使用稳定 key、条件创建或业务幂等协议。
//!
//! 本 crate 不提供 multipart、流式/range/list、presigned URL、STS 自动刷新、服务端加密策略或
//! 对象版本治理，需要这些能力的业务应选择专用 client。

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use hmac::{Hmac, Mac as _};
use nasecret::SecretBytes;
#[cfg(feature = "metrics")]
pub mod metrics;
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

/// 当前 adapter 会完整缓冲对象，因此用框架硬上限阻止配置把“有界”退化成 `usize::MAX`。
pub const MAX_BUFFERED_OBJECT_BYTES: usize = 256 * 1024 * 1024;
/// 单次对象存储请求允许的最大总时长。
pub const MAX_OBJECT_REQUEST_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

type HmacSha256 = Hmac<Sha256>;

/// 已校验、相对于 bucket 根的对象 key。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(Arc<str>);

impl ObjectKey {
    /// 业务作用：校验并构造 key。拒绝空 key、绝对路径、控制字符、`.`/`..` 段、空段与超过 1024 字节。
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, ObjectStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 1024
            || value.starts_with('/')
            || value.chars().any(char::is_control)
            || value
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
        {
            return Err(ObjectStoreError::InvalidKey);
        }
        Ok(Self(value))
    }

    /// 业务作用：返回原始相对 key。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// PUT 的覆盖语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutMode {
    /// 允许创建或覆盖。
    Overwrite,
    /// 仅当 key 不存在时创建，对 S3 使用 `If-None-Match: *`。
    CreateOnly,
}

/// 有界单对象上传请求。
#[derive(Debug)]
pub struct PutObject {
    /// 目标 key。
    pub key: ObjectKey,
    /// 完整对象字节。
    pub body: Vec<u8>,
    /// 有界 Content-Type。
    pub content_type: Option<String>,
    /// 覆盖语义。
    pub mode: PutMode,
}

/// 对象元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// 对象 key。
    pub key: ObjectKey,
    /// 字节数。
    pub size: u64,
    /// 服务端 ETag；不作为内容校验摘要。
    pub etag: Option<String>,
    /// 客户端写入并在读取时复核的 SHA-256 hex。
    pub sha256: Option<String>,
}

/// 完整下载结果。
#[derive(Debug)]
pub struct GetObject {
    /// 已验证元数据。
    pub metadata: ObjectMetadata,
    /// 完整对象字节。
    pub body: Vec<u8>,
}

/// provider-neutral 对象存储合同。
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// 业务作用：上传一个有界对象。
    async fn put(&self, request: PutObject) -> Result<ObjectMetadata, ObjectStoreError>;
    /// 业务作用：下载一个有界对象。
    async fn get(&self, key: &ObjectKey) -> Result<GetObject, ObjectStoreError>;
    /// 业务作用：只读取元数据。
    async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata, ObjectStoreError>;
    /// 业务作用：幂等删除；不存在也算成功。
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectStoreError>;
}

/// S3 credential；Debug 永不输出任何 credential。
pub struct S3Credentials {
    /// Access key ID。
    pub access_key_id: SecretBytes,
    /// Secret access key。
    pub secret_access_key: SecretBytes,
    /// 可选 STS session token。
    pub session_token: Option<SecretBytes>,
}

impl fmt::Debug for S3Credentials {
    /// 业务作用：只展示 credential 字段是否存在，永不输出实际认证字节。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3Credentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// S3-compatible adapter 配置。
#[derive(Debug)]
pub struct S3Options {
    /// S3 根 endpoint，可包含部署前缀。
    pub endpoint: String,
    /// path-style bucket。
    pub bucket: String,
    /// SigV4 region。
    pub region: String,
    /// credential。
    pub credentials: S3Credentials,
    /// 单请求总超时。
    pub request_timeout: Duration,
    /// 上传/下载对象硬上限。
    pub max_object_bytes: usize,
    /// GET 时是否要求并复核 `x-amz-meta-sha256`。
    pub require_checksum: bool,
}

impl S3Options {
    /// 业务作用：创建生产保守缺省：10 秒、16 MiB、强制 SHA-256 metadata。
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        credentials: S3Credentials,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            region: region.into(),
            credentials,
            request_timeout: Duration::from_secs(10),
            max_object_bytes: 16 * 1024 * 1024,
            require_checksum: true,
        }
    }
}

/// 对象存储错误；不读取或转发远端错误正文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreError {
    /// 宿主已关闭当前 client 的新工作准入。
    Closed,
    /// key 不合法。
    InvalidKey,
    /// adapter 配置不合法。
    InvalidConfiguration,
    /// 上传/下载超过硬上限。
    ObjectTooLarge {
        /// 实际字节数。
        actual: u64,
        /// 上限。
        max: usize,
    },
    /// 对象不存在。
    NotFound,
    /// CreateOnly 的 key 已存在。
    AlreadyExists,
    /// 传输或请求超时。
    Transport,
    /// 远端非预期状态。
    RemoteStatus(u16),
    /// 响应元数据非法。
    InvalidResponse,
    /// 缺失强制校验摘要。
    MissingChecksum,
    /// 内容 SHA-256 不匹配。
    ChecksumMismatch,
}

impl fmt::Display for ObjectStoreError {
    /// 业务作用：输出稳定错误分类，不附带 endpoint、credential、key 或远端正文。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "object store error: {self:?}")
    }
}

impl std::error::Error for ObjectStoreError {}

/// 对象存储的四类操作；label 值封闭，不随 bucket、key 或对端扩张。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectOperation {
    /// 上传一个有界对象。
    Put,
    /// 下载一个有界对象。
    Get,
    /// 只读取元数据。
    Head,
    /// 幂等删除。
    Delete,
}

impl ObjectOperation {
    /// 业务作用：返回操作的稳定低基数 label 值，供导出面使用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：与枚举一一对应的固定字符串。
    pub const fn label(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Get => "get",
            Self::Head => "head",
            Self::Delete => "delete",
        }
    }
}

/// 一次操作的封闭结局分类。
///
/// 远端状态码不进 label：`RemoteStatus` 统一折叠为 `remote_status`，避免把 0..=599
/// 的取值域变成指标基数。需要具体状态码时读错误本身，不读指标。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectOutcome {
    /// 操作成功。
    Success,
    /// 对象不存在。
    NotFound,
    /// 对象超过配置的缓冲上限；上传可在本地拒绝，下载可在远端应答后停止读取。
    TooLarge,
    /// `CreateOnly` 上传被远端确认对象已经存在。
    AlreadyExists,
    /// key、配置或参数在本地即被拒绝，请求未发出。
    Rejected,
    /// 传输层失败，结果不确定。
    Transport,
    /// 远端返回非成功状态码。
    RemoteStatus,
    /// 远端应答缺少必需字段或无法解析。
    InvalidResponse,
    /// 校验和缺失或与本地计算不一致。
    Checksum,
    /// 调用 future 在返回业务结果前被丢弃；远端是否收到或完成请求未知。
    Cancelled,
}

impl ObjectOutcome {
    /// 业务作用：返回结局的稳定低基数 label 值，供导出面使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与枚举一一对应的固定字符串。
    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::TooLarge => "too_large",
            Self::AlreadyExists => "already_exists",
            Self::Rejected => "rejected",
            Self::Transport => "transport",
            Self::RemoteStatus => "remote_status",
            Self::InvalidResponse => "invalid_response",
            Self::Checksum => "checksum",
            Self::Cancelled => "cancelled",
        }
    }

    /// 业务作用：把封闭错误分类折叠成导出用结局，保证 label 取值域有限。
    ///
    /// 参数说明：
    /// - `error`: adapter 返回的封闭错误。
    ///
    /// 返回：对应的结局分类；新增错误分类时必须在此显式归类。
    const fn from_error(error: &ObjectStoreError) -> Self {
        match error {
            ObjectStoreError::NotFound => Self::NotFound,
            ObjectStoreError::ObjectTooLarge { .. } => Self::TooLarge,
            ObjectStoreError::AlreadyExists => Self::AlreadyExists,
            ObjectStoreError::Closed
            | ObjectStoreError::InvalidKey
            | ObjectStoreError::InvalidConfiguration => Self::Rejected,
            ObjectStoreError::Transport => Self::Transport,
            ObjectStoreError::RemoteStatus(_) => Self::RemoteStatus,
            ObjectStoreError::InvalidResponse => Self::InvalidResponse,
            ObjectStoreError::MissingChecksum | ObjectStoreError::ChecksumMismatch => {
                Self::Checksum
            }
        }
    }
}

/// 全部操作与结局组合的固定顺序；导出面据此生成稳定序列，不因运行期取值变化而增删。
const OBJECT_OPERATIONS: [ObjectOperation; 4] = [
    ObjectOperation::Put,
    ObjectOperation::Get,
    ObjectOperation::Head,
    ObjectOperation::Delete,
];
const OBJECT_OUTCOMES: [ObjectOutcome; 10] = [
    ObjectOutcome::Success,
    ObjectOutcome::NotFound,
    ObjectOutcome::TooLarge,
    ObjectOutcome::AlreadyExists,
    ObjectOutcome::Rejected,
    ObjectOutcome::Transport,
    ObjectOutcome::RemoteStatus,
    ObjectOutcome::InvalidResponse,
    ObjectOutcome::Checksum,
    ObjectOutcome::Cancelled,
];
/// 对象存储时延直方图边界（秒）；覆盖本地 MinIO 到跨区对象存储的常见区间。
pub const OBJECT_DURATION_BOUNDS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// 一个 (操作, 结局) 组合的累计请求数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectRequestCount {
    /// 操作类别。
    pub operation: ObjectOperation,
    /// 结局分类。
    pub outcome: ObjectOutcome,
    /// 累计请求数。
    pub requests: u64,
}

/// 一个操作已返回业务结局的时延分布快照。
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectDurationSample {
    /// 操作类别。
    pub operation: ObjectOperation,
    /// 每个有限边界及 `+Inf` 的非累积桶计数。
    pub buckets: Vec<u64>,
    /// 观测秒数总和。
    pub sum_seconds: f64,
    /// 观测总数。
    pub count: u64,
}

/// adapter 自启动以来的累计观测事实。
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectStoreSnapshot {
    /// 按 (操作, 结局) 的累计请求数，只包含非零组合。
    pub requests: Vec<ObjectRequestCount>,
    /// 按操作的完整返回时延分布，只包含有观测的操作，不含调用方取消；只有取消时无对应样本。
    pub durations: Vec<ObjectDurationSample>,
    /// 成功上传的对象字节总数。
    pub uploaded_bytes: u64,
    /// 成功下载的对象字节总数。
    pub downloaded_bytes: u64,
}

/// adapter 内部的原子计数；读取不清零，导出面按需取快照。
#[derive(Debug)]
struct ObjectStoreMetrics {
    requests: [[AtomicU64; OBJECT_OUTCOMES.len()]; OBJECT_OPERATIONS.len()],
    duration_buckets: [[AtomicU64; OBJECT_DURATION_BOUNDS.len() + 1]; OBJECT_OPERATIONS.len()],
    duration_nanos: [AtomicU64; OBJECT_OPERATIONS.len()],
    uploaded_bytes: AtomicU64,
    downloaded_bytes: AtomicU64,
}

impl ObjectStoreMetrics {
    /// 业务作用：创建全部计数为零的 adapter 观测状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由并发请求写入、导出面并发读取的原子状态。
    fn new() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            duration_buckets: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            duration_nanos: std::array::from_fn(|_| AtomicU64::new(0)),
            uploaded_bytes: AtomicU64::new(0),
            downloaded_bytes: AtomicU64::new(0),
        }
    }

    /// 业务作用：记录一次操作的结局，并为已返回业务结局的调用记录耗时与成功传输字节。
    ///
    /// 耗时总和先写、bucket 最后发布；快照直接以所有 bucket 的饱和总和作为 count，
    /// 因而并发抓取不会得到 `sum(buckets) != count` 并被统一指标目录拒绝。
    ///
    /// 参数说明：
    /// - `operation`: 本次操作类别。
    /// - `outcome`: 本次操作的封闭结局。
    /// - `elapsed`: 从进入 adapter 到当前记账点的单调耗时；取消路径不写入时延分布。
    /// - `bytes`: 成功时的对象字节数；失败或无载荷时为 0。
    ///
    /// 返回：无；所有结局都累加请求数，只有完整返回的调用累加时延，成功传输另行累加字节。
    fn record(
        &self,
        operation: ObjectOperation,
        outcome: ObjectOutcome,
        elapsed: Duration,
        bytes: u64,
    ) {
        let op = operation as usize;
        self.requests[op][outcome as usize].fetch_add(1, Ordering::Relaxed);
        // 取消耗时由调用方预算决定且没有完整业务结局；只保留请求结局，避免短超时压低完成调用的分位数。
        if outcome == ObjectOutcome::Cancelled {
            return;
        }
        // 本地边界拒绝可能短于 1 微秒；纳秒精度保证 `_sum / _count` 与桶位置描述同一批耗时，
        // 不把已观测请求截断成零时延。
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.duration_nanos[op].fetch_add(nanos, Ordering::Relaxed);
        let seconds = elapsed.as_secs_f64();
        let index = OBJECT_DURATION_BOUNDS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(OBJECT_DURATION_BOUNDS.len());
        self.duration_buckets[op][index].fetch_add(1, Ordering::Release);
        if outcome == ObjectOutcome::Success && bytes > 0 {
            match operation {
                ObjectOperation::Put => self.uploaded_bytes.fetch_add(bytes, Ordering::Relaxed),
                ObjectOperation::Get => self.downloaded_bytes.fetch_add(bytes, Ordering::Relaxed),
                ObjectOperation::Head | ObjectOperation::Delete => 0,
            };
        }
    }

    /// 业务作用：导出当前累计事实，供业务接入自己的指标目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只含非零组合的请求计数与完整返回时延分布；只有取消的操作不产生时延样本，读取不清零。
    fn snapshot(&self) -> ObjectStoreSnapshot {
        let mut requests = Vec::new();
        let mut durations = Vec::new();
        for operation in OBJECT_OPERATIONS {
            // 操作与结局两轴都以枚举判别值寻址，展示数组调序不会把计数或时延标成其它业务类别。
            let op = operation as usize;
            for outcome in OBJECT_OUTCOMES {
                let value = self.requests[op][outcome as usize].load(Ordering::Relaxed);
                if value > 0 {
                    requests.push(ObjectRequestCount {
                        operation,
                        outcome,
                        requests: value,
                    });
                }
            }
            let buckets: Vec<u64> = self.duration_buckets[op]
                .iter()
                .map(|bucket| bucket.load(Ordering::Acquire))
                .collect();
            // count 与导出的非累积桶必须来自同一组读值；独立原子计数会在并发写入间隙
            // 领先或落后于桶，使统一目录按形状非法丢弃整条 histogram。
            let count = buckets.iter().copied().fold(0_u64, u64::saturating_add);
            if count == 0 {
                continue;
            }
            durations.push(ObjectDurationSample {
                operation,
                buckets,
                sum_seconds: self.duration_nanos[op].load(Ordering::Relaxed) as f64
                    / 1_000_000_000_f64,
                count,
            });
        }
        ObjectStoreSnapshot {
            requests,
            durations,
            uploaded_bytes: self.uploaded_bytes.load(Ordering::Relaxed),
            downloaded_bytes: self.downloaded_bytes.load(Ordering::Relaxed),
        }
    }
}

/// 一次对象调用的完成守卫；future 在返回前被丢弃时由析构路径补记取消结局。
struct ObjectCallAccounting<'a> {
    metrics: Option<&'a ObjectStoreMetrics>,
    operation: ObjectOperation,
    started: Instant,
}

impl<'a> ObjectCallAccounting<'a> {
    /// 业务作用：在调用第一次被 poll 时建立记账所有权，使正常完成与取消共享同一计数责任。
    ///
    /// 参数说明：
    /// - `metrics`: 本次调用所属 adapter 的累计观测状态。
    /// - `operation`: 本次对象操作类别。
    ///
    /// 返回：持有唯一记账责任的守卫；未显式完成时析构为 `Cancelled`。
    fn new(metrics: &'a ObjectStoreMetrics, operation: ObjectOperation) -> Self {
        Self {
            metrics: Some(metrics),
            operation,
            started: Instant::now(),
        }
    }

    /// 业务作用：发布正常返回路径的封闭结局，并解除析构路径的取消记账责任。
    ///
    /// 参数说明：
    /// - `outcome`: 内层实现已经得出的业务结局。
    /// - `bytes`: 成功上传或下载的完整对象字节数；其它结局为 0。
    ///
    /// 返回：无；本次调用只发布一次请求、完整返回耗时与成功字节事实。
    fn finish(mut self, outcome: ObjectOutcome, bytes: u64) {
        if let Some(metrics) = self.metrics.take() {
            metrics.record(self.operation, outcome, self.started.elapsed(), bytes);
        }
    }
}

impl Drop for ObjectCallAccounting<'_> {
    /// 业务作用：在调用 future 完成前被丢弃时发布取消结局，避免已开始的远端工作从观测面消失。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无；仅在正常完成尚未解除责任时记录一次取消请求，不计入完整返回时延与成功字节。
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.take() {
            metrics.record(
                self.operation,
                ObjectOutcome::Cancelled,
                self.started.elapsed(),
                0,
            );
        }
    }
}

/// S3-compatible path-style SigV4 adapter。
pub struct S3ObjectStore {
    endpoint: reqwest::Url,
    client: reqwest::Client,
    options: S3Options,
    metrics: ObjectStoreMetrics,
}

impl S3ObjectStore {
    /// 业务作用：以签名的 HEAD bucket 证明当前凭据可访问配置目标，不创建或删除对象。
    /// 参数说明：无。
    /// 返回：远端确认成功时就绪；权限不足、目标不存在或网络失败原样归类，不返回远端正文。
    pub async fn health_check(&self) -> Result<(), ObjectStoreError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| ObjectStoreError::InvalidConfiguration)?
            .pop_if_empty()
            .push(&self.options.bucket);
        let response = self
            .signed(
                reqwest::Method::HEAD,
                url,
                &hex::encode(Sha256::digest([])),
                None,
            )?
            .send()
            .await
            .map_err(|_| ObjectStoreError::Transport)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(ObjectStoreError::RemoteStatus(response.status().as_u16()))
        }
    }

    /// 业务作用：校验配置并构造 adapter。
    pub fn new(options: S3Options) -> Result<Self, ObjectStoreError> {
        if !valid_bucket(&options.bucket)
            || options.region.is_empty()
            || options.region.len() > 64
            || options.max_object_bytes == 0
            || options.max_object_bytes > MAX_BUFFERED_OBJECT_BYTES
            || options.request_timeout.is_zero()
            || options.request_timeout > MAX_OBJECT_REQUEST_TIMEOUT
            || !options
                .region
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(ObjectStoreError::InvalidConfiguration);
        }
        let endpoint = reqwest::Url::parse(&options.endpoint)
            .map_err(|_| ObjectStoreError::InvalidConfiguration)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.cannot_be_a_base()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(ObjectStoreError::InvalidConfiguration);
        }
        if endpoint.scheme() == "http"
            && !endpoint
                .host_str()
                .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"))
        {
            return Err(ObjectStoreError::InvalidConfiguration);
        }
        let access_key_id = std::str::from_utf8(options.credentials.access_key_id.expose())
            .map_err(|_| ObjectStoreError::InvalidConfiguration)?;
        if access_key_id.is_empty()
            || access_key_id.len() > 128
            || !access_key_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || options.credentials.secret_access_key.is_empty()
            || options.credentials.secret_access_key.len() > 4096
            || options
                .credentials
                .session_token
                .as_ref()
                .is_some_and(|token| {
                    token.is_empty()
                        || token.len() > 8192
                        || !token.expose().iter().all(|byte| byte.is_ascii_graphic())
                })
        {
            return Err(ObjectStoreError::InvalidConfiguration);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(options.request_timeout)
            .build()
            .map_err(|_| ObjectStoreError::InvalidConfiguration)?;
        Ok(Self {
            endpoint,
            client,
            options,
            metrics: ObjectStoreMetrics::new(),
        })
    }

    /// 业务作用：在保留 endpoint 前缀的前提下，以独立 path segment 追加 bucket 与已校验对象 key。
    fn object_url(&self, key: &ObjectKey) -> Result<reqwest::Url, ObjectStoreError> {
        let mut url = self.endpoint.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| ObjectStoreError::InvalidConfiguration)?;
        segments.pop_if_empty();
        segments.push(&self.options.bucket);
        for segment in key.as_str().split('/') {
            segments.push(segment);
        }
        drop(segments);
        Ok(url)
    }

    /// 业务作用：构造 SigV4 canonical request、派生签名密钥并附加全部认证 header。
    fn signed(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
        payload_hash: &str,
        metadata_sha256: Option<&str>,
    ) -> Result<reqwest::RequestBuilder, ObjectStoreError> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short_date = now.format("%Y%m%d").to_string();
        let host = match (url.host_str(), url.port()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_owned(),
            _ => return Err(ObjectStoreError::InvalidConfiguration),
        };
        let session_token = self
            .options
            .credentials
            .session_token
            .as_ref()
            .map(|token| {
                std::str::from_utf8(token.expose())
                    .map_err(|_| ObjectStoreError::InvalidConfiguration)
            })
            .transpose()?;
        let access_key_id = std::str::from_utf8(self.options.credentials.access_key_id.expose())
            .expect("S3 access key ID was validated during construction");
        if session_token.is_some_and(|token| token.chars().any(char::is_control)) {
            return Err(ObjectStoreError::InvalidConfiguration);
        }
        let mut headers = BTreeMap::from([
            ("host", host.as_str()),
            ("x-amz-content-sha256", payload_hash),
            ("x-amz-date", amz_date.as_str()),
        ]);
        if let Some(checksum) = metadata_sha256 {
            headers.insert("x-amz-meta-sha256", checksum);
        }
        if let Some(token) = session_token {
            headers.insert("x-amz-security-token", token);
        }
        let canonical_headers = Zeroizing::new(
            headers
                .iter()
                .map(|(name, value)| format!("{name}:{}\n", value.trim()))
                .collect::<String>(),
        );
        let signed_headers = headers.keys().copied().collect::<Vec<_>>().join(";");
        let canonical_request = Zeroizing::new(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            url.path(),
            url.query().unwrap_or_default(),
            canonical_headers.as_str(),
            signed_headers,
            payload_hash
        ));
        let scope = format!("{short_date}/{}/s3/aws4_request", self.options.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let mut root_key = Zeroizing::new(Vec::with_capacity(
            4 + self.options.credentials.secret_access_key.len(),
        ));
        root_key.extend_from_slice(b"AWS4");
        root_key.extend_from_slice(self.options.credentials.secret_access_key.expose());
        let date_key = hmac(&root_key, short_date.as_bytes())?;
        let region_key = hmac(&date_key, self.options.region.as_bytes())?;
        let service_key = hmac(&region_key, b"s3")?;
        let signing_key = hmac(&service_key, b"aws4_request")?;
        let signature = Zeroizing::new(hex::encode(hmac(&signing_key, string_to_sign.as_bytes())?));
        let authorization = Zeroizing::new(format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            access_key_id,
            scope,
            signed_headers,
            signature.as_str()
        ));
        let mut request = self
            .client
            .request(method, url)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header(reqwest::header::AUTHORIZATION, authorization.as_str());
        if let Some(checksum) = metadata_sha256 {
            request = request.header("x-amz-meta-sha256", checksum);
        }
        if let Some(token) = session_token {
            request = request.header("x-amz-security-token", token);
        }
        Ok(request)
    }

    /// 业务作用：从响应头提取并校验有界 ETag、SHA-256 与已知对象大小。
    fn metadata(
        &self,
        key: &ObjectKey,
        headers: &reqwest::header::HeaderMap,
        size: u64,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let etag = optional_header(headers, reqwest::header::ETAG)?;
        let sha256 = optional_header_name(headers, "x-amz-meta-sha256")?;
        if etag
            .as_ref()
            .is_some_and(|value| value.len() > 256 || value.chars().any(char::is_control))
        {
            return Err(ObjectStoreError::InvalidResponse);
        }
        if sha256.as_ref().is_some_and(|value| {
            value.len() != 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        }) {
            return Err(ObjectStoreError::InvalidResponse);
        }
        Ok(ObjectMetadata {
            key: key.clone(),
            size,
            etag,
            sha256,
        })
    }
}

/// 内层实现：只负责协议与校验，结局记账由 `ObjectStore` 包装层统一完成。
impl S3ObjectStore {
    /// 业务作用：校验对象与 Content-Type 上限，写入内容摘要并执行覆盖或 CreateOnly 上传。
    async fn put_inner(&self, request: PutObject) -> Result<ObjectMetadata, ObjectStoreError> {
        let object_size = request.body.len();
        if object_size > self.options.max_object_bytes {
            return Err(ObjectStoreError::ObjectTooLarge {
                actual: object_size as u64,
                max: self.options.max_object_bytes,
            });
        }
        if request.content_type.as_ref().is_some_and(|value| {
            value.is_empty()
                || value.len() > 255
                || value.chars().any(char::is_control)
                || reqwest::header::HeaderValue::from_str(value).is_err()
        }) {
            return Err(ObjectStoreError::InvalidConfiguration);
        }
        let checksum = hex::encode(Sha256::digest(&request.body));
        let url = self.object_url(&request.key)?;
        let mut http = self.signed(reqwest::Method::PUT, url, &checksum, Some(&checksum))?;
        if let Some(content_type) = &request.content_type {
            http = http.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        if request.mode == PutMode::CreateOnly {
            http = http.header(reqwest::header::IF_NONE_MATCH, "*");
        }
        let response = http
            .body(request.body)
            .send()
            .await
            .map_err(|_| ObjectStoreError::Transport)?;
        match response.status() {
            status if status.is_success() => {
                let mut metadata =
                    self.metadata(&request.key, response.headers(), object_size as u64)?;
                metadata.sha256 = Some(checksum);
                Ok(metadata)
            }
            reqwest::StatusCode::PRECONDITION_FAILED => Err(ObjectStoreError::AlreadyExists),
            status => Err(ObjectStoreError::RemoteStatus(status.as_u16())),
        }
    }

    /// 业务作用：在 Content-Length 与流式累计两层限制下下载对象，并按配置复核 SHA-256。
    async fn get_inner(&self, key: &ObjectKey) -> Result<GetObject, ObjectStoreError> {
        let url = self.object_url(key)?;
        let response = self
            .signed(reqwest::Method::GET, url, &empty_sha256(), None)?
            .send()
            .await
            .map_err(|_| ObjectStoreError::Transport)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ObjectStoreError::NotFound);
        }
        if !response.status().is_success() {
            return Err(ObjectStoreError::RemoteStatus(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|size| size > self.options.max_object_bytes as u64)
        {
            return Err(ObjectStoreError::ObjectTooLarge {
                actual: response.content_length().unwrap_or_default(),
                max: self.options.max_object_bytes,
            });
        }
        let headers = response.headers().clone();
        let mut response = response;
        let initial_capacity = response
            .content_length()
            .and_then(|size| usize::try_from(size).ok())
            .unwrap_or_default()
            .min(self.options.max_object_bytes);
        let mut body = Vec::with_capacity(initial_capacity);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ObjectStoreError::Transport)?
        {
            let actual = body.len().saturating_add(chunk.len());
            if actual > self.options.max_object_bytes {
                return Err(ObjectStoreError::ObjectTooLarge {
                    actual: actual as u64,
                    max: self.options.max_object_bytes,
                });
            }
            body.extend_from_slice(&chunk);
        }
        let metadata = self.metadata(key, &headers, body.len() as u64)?;
        match &metadata.sha256 {
            Some(expected) if *expected == hex::encode(Sha256::digest(&body)) => {}
            Some(_) => return Err(ObjectStoreError::ChecksumMismatch),
            None if self.options.require_checksum => return Err(ObjectStoreError::MissingChecksum),
            None => {}
        }
        Ok(GetObject { metadata, body })
    }

    /// 业务作用：发送签名 HEAD 请求并把远端元数据投影为经过边界校验的对象摘要。
    async fn head_inner(&self, key: &ObjectKey) -> Result<ObjectMetadata, ObjectStoreError> {
        let url = self.object_url(key)?;
        let response = self
            .signed(reqwest::Method::HEAD, url, &empty_sha256(), None)?
            .send()
            .await
            .map_err(|_| ObjectStoreError::Transport)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ObjectStoreError::NotFound);
        }
        if !response.status().is_success() {
            return Err(ObjectStoreError::RemoteStatus(response.status().as_u16()));
        }
        let size = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(ObjectStoreError::InvalidResponse)?;
        if size > self.options.max_object_bytes as u64 {
            return Err(ObjectStoreError::ObjectTooLarge {
                actual: size,
                max: self.options.max_object_bytes,
            });
        }
        self.metadata(key, response.headers(), size)
    }

    /// 业务作用：发送签名 DELETE；成功与不存在都映射为幂等成功。
    async fn delete_inner(&self, key: &ObjectKey) -> Result<(), ObjectStoreError> {
        let url = self.object_url(key)?;
        let response = self
            .signed(reqwest::Method::DELETE, url, &empty_sha256(), None)?
            .send()
            .await
            .map_err(|_| ObjectStoreError::Transport)?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(ObjectStoreError::RemoteStatus(response.status().as_u16()))
        }
    }

    /// 业务作用：读取 adapter 自启动以来的累计观测事实，供业务接入自己的指标目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 (操作, 结局) 的请求计数、按操作的完整返回时延与成功传输字节；只有取消的操作不产生
    /// 时延样本，读取不清零。
    pub fn metrics_snapshot(&self) -> ObjectStoreSnapshot {
        self.metrics.snapshot()
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    /// 业务作用：上传一个有界对象，并把结局、耗时与字节数记入唯一记账点。
    ///
    /// 参数说明：
    /// - `request`: 已带 key、载荷与可选内容类型的上传请求。
    ///
    /// 返回：远端确认并通过校验和核对时返回对象元数据；任一封闭失败分类都同时被记账。
    async fn put(&self, request: PutObject) -> Result<ObjectMetadata, ObjectStoreError> {
        // 守卫在首次 poll 后取得唯一记账责任；正常返回走 finish，调用方取消则由 Drop 留下结局。
        let accounting = ObjectCallAccounting::new(&self.metrics, ObjectOperation::Put);
        let bytes = request.body.len() as u64;
        let result = self.put_inner(request).await;
        accounting.finish(object_outcome(&result), bytes);
        result
    }

    /// 业务作用：下载一个有界对象，并把结局、耗时与字节数记入唯一记账点。
    ///
    /// 参数说明：
    /// - `key`: 已校验、相对于 bucket 根的对象 key。
    ///
    /// 返回：远端返回且校验和一致时返回对象内容；失败分类同时被记账。
    async fn get(&self, key: &ObjectKey) -> Result<GetObject, ObjectStoreError> {
        let accounting = ObjectCallAccounting::new(&self.metrics, ObjectOperation::Get);
        let result = self.get_inner(key).await;
        let bytes = result
            .as_ref()
            .map(|object| object.body.len() as u64)
            .unwrap_or(0);
        accounting.finish(object_outcome(&result), bytes);
        result
    }

    /// 业务作用：只读取对象元数据，并把结局与耗时记入唯一记账点。
    ///
    /// 参数说明：
    /// - `key`: 已校验、相对于 bucket 根的对象 key。
    ///
    /// 返回：对象存在时返回元数据；不存在与其它失败分类分别记账。
    async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata, ObjectStoreError> {
        let accounting = ObjectCallAccounting::new(&self.metrics, ObjectOperation::Head);
        let result = self.head_inner(key).await;
        accounting.finish(object_outcome(&result), 0);
        result
    }

    /// 业务作用：幂等删除对象，并把结局与耗时记入唯一记账点。
    ///
    /// 参数说明：
    /// - `key`: 已校验、相对于 bucket 根的对象 key。
    ///
    /// 返回：删除成功或对象本就不存在时成功；两者都记为成功结局，与幂等语义一致。
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectStoreError> {
        let accounting = ObjectCallAccounting::new(&self.metrics, ObjectOperation::Delete);
        let result = self.delete_inner(key).await;
        accounting.finish(object_outcome(&result), 0);
        result
    }
}

/// 业务作用：把一次操作结果折叠成封闭结局分类，保证导出 label 取值域有限。
///
/// 参数说明：
/// - `result`: adapter 内层实现的返回值。
///
/// 返回：成功返回 `Success`，失败按错误分类映射。
fn object_outcome<T>(result: &Result<T, ObjectStoreError>) -> ObjectOutcome {
    match result {
        Ok(_) => ObjectOutcome::Success,
        Err(error) => ObjectOutcome::from_error(error),
    }
}

/// 业务作用：按 S3 DNS-compatible 规则校验 bucket 名，额外拒绝 IPv4 字面量。
fn valid_bucket(bucket: &str) -> bool {
    (3..=63).contains(&bucket.len())
        && bucket.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && bucket
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !bucket.contains("..")
        && bucket.parse::<std::net::Ipv4Addr>().is_err()
}

/// 业务作用：计算一轮 HMAC-SHA256，并用可清零缓冲承载 SigV4 派生密钥。
fn hmac(key: &[u8], data: &[u8]) -> Result<Zeroizing<Vec<u8>>, ObjectStoreError> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|_| ObjectStoreError::InvalidConfiguration)?;
    mac.update(data);
    Ok(Zeroizing::new(mac.finalize().into_bytes().to_vec()))
}

/// 业务作用：返回空请求体的 SHA-256 hex，用于 GET/HEAD/DELETE 的 SigV4 payload hash。
fn empty_sha256() -> String {
    hex::encode(Sha256::digest([]))
}

/// 业务作用：读取可选标准 header，并拒绝非文本值。
fn optional_header(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Result<Option<String>, ObjectStoreError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| ObjectStoreError::InvalidResponse)
        })
        .transpose()
}

/// 业务作用：读取可选扩展 header，并拒绝非文本值。
fn optional_header_name(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
) -> Result<Option<String>, ObjectStoreError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| ObjectStoreError::InvalidResponse)
        })
        .transpose()
}
