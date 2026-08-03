//! 实验性对象存储合同与 S3-compatible SigV4 adapter。
//!
//! 第一版只接受有硬上限的单对象缓冲，不伪装成 multipart/无限流式上传。adapter 不拥有应用生命周期；
//! 业务可把实例注册为 managed resource。稳定公共合同仍需两个真实上传/导出/归档项目收敛。

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use hmac::{Hmac, Mac as _};
use nasecret::SecretBytes;
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

/// S3-compatible path-style SigV4 adapter。
pub struct S3ObjectStore {
    endpoint: reqwest::Url,
    client: reqwest::Client,
    options: S3Options,
}

impl S3ObjectStore {
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

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    /// 业务作用：校验对象与 Content-Type 上限，写入内容摘要并执行覆盖或 CreateOnly 上传。
    async fn put(&self, request: PutObject) -> Result<ObjectMetadata, ObjectStoreError> {
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
    async fn get(&self, key: &ObjectKey) -> Result<GetObject, ObjectStoreError> {
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
    async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata, ObjectStoreError> {
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
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectStoreError> {
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
