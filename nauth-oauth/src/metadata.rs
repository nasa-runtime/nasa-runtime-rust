//! RFC 8414 Authorization Server Metadata 的有界发现客户端。

use std::collections::BTreeSet;
use std::time::Duration;

use serde::Deserialize;

/// Metadata 文档的框架硬上限；调用方只能进一步收紧。
pub const MAX_METADATA_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// 单次 metadata 请求允许的最大总时长。
pub const MAX_METADATA_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Resource Server 需要的最小 metadata 投影。
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AuthorizationServerMetadata {
    /// 必须与配置 issuer 精确相等。
    pub issuer: String,
    /// JWT access token 的 JWK Set URL。
    pub jwks_uri: String,
    /// 可选 token endpoint；只作为元数据暴露，不由 Resource Server 调用。
    #[serde(default)]
    pub token_endpoint: Option<String>,
}

/// metadata/JWKS URL 与响应边界。
#[derive(Debug, Clone)]
pub struct MetadataOptions {
    /// 单次 GET 总超时。
    pub timeout: Duration,
    /// 响应体硬上限。
    pub max_response_bytes: usize,
    /// metadata 和返回的 jwks_uri 主机白名单。
    pub allowed_hosts: BTreeSet<String>,
}

impl Default for MetadataOptions {
    /// 业务作用：使用 3 秒、256 KiB 且不扩展跨 host 信任的保守缺省。
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3),
            max_response_bytes: 256 * 1024,
            allowed_hosts: BTreeSet::new(),
        }
    }
}

/// 安全、脱敏的 metadata 错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataError {
    /// 配置 URL/issuer/limit 非法。
    InvalidConfiguration,
    /// HTTP 或响应边界失败。
    FetchFailed,
    /// JSON 结构失败。
    InvalidDocument,
    /// 返回 issuer 不精确匹配。
    IssuerMismatch,
    /// 返回 jwks_uri 不满足 scheme/host policy。
    InvalidJwksUri,
}

impl std::fmt::Display for MetadataError {
    /// 业务作用：输出稳定错误分类，不附带 issuer、URL 或远端响应正文。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "authorization server metadata error: {self:?}")
    }
}

impl std::error::Error for MetadataError {}

/// 拒绝重定向、限制 host/HTTPS/body/time 的 RFC 8414 客户端。
pub struct MetadataClient {
    expected_issuer: String,
    metadata_uri: reqwest::Url,
    options: MetadataOptions,
    client: reqwest::Client,
}

impl MetadataClient {
    /// 业务作用：创建客户端；非 loopback 的 HTTP、userinfo、fragment 和未允许 host 均被拒。
    pub fn new(
        expected_issuer: impl Into<String>,
        metadata_uri: &str,
        options: MetadataOptions,
    ) -> Result<Self, MetadataError> {
        let expected_issuer = expected_issuer.into();
        if expected_issuer.trim().is_empty()
            || options.timeout.is_zero()
            || options.timeout > MAX_METADATA_TIMEOUT
            || options.max_response_bytes == 0
            || options.max_response_bytes > MAX_METADATA_RESPONSE_BYTES
        {
            return Err(MetadataError::InvalidConfiguration);
        }
        let metadata_uri =
            reqwest::Url::parse(metadata_uri).map_err(|_| MetadataError::InvalidConfiguration)?;
        validate_url(&metadata_uri, &options.allowed_hosts)
            .map_err(|_| MetadataError::InvalidConfiguration)?;
        let client = reqwest::Client::builder()
            .timeout(options.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| MetadataError::InvalidConfiguration)?;
        Ok(Self {
            expected_issuer,
            metadata_uri,
            options,
            client,
        })
    }

    /// 业务作用：拉取、限长、解析并校验 issuer/jwks_uri。
    pub async fn fetch(&self) -> Result<AuthorizationServerMetadata, MetadataError> {
        let mut response = self
            .client
            .get(self.metadata_uri.clone())
            .send()
            .await
            .map_err(|_| MetadataError::FetchFailed)?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > self.options.max_response_bytes as u64)
        {
            return Err(MetadataError::FetchFailed);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| MetadataError::FetchFailed)?
        {
            if chunk.len() > self.options.max_response_bytes.saturating_sub(body.len()) {
                return Err(MetadataError::FetchFailed);
            }
            body.extend_from_slice(&chunk);
        }
        let metadata: AuthorizationServerMetadata =
            serde_json::from_slice(&body).map_err(|_| MetadataError::InvalidDocument)?;
        if metadata.issuer != self.expected_issuer {
            return Err(MetadataError::IssuerMismatch);
        }
        let jwks_uri =
            reqwest::Url::parse(&metadata.jwks_uri).map_err(|_| MetadataError::InvalidJwksUri)?;
        validate_url(&jwks_uri, &self.options.allowed_hosts)
            .map_err(|_| MetadataError::InvalidJwksUri)?;
        if self.options.allowed_hosts.is_empty()
            && jwks_uri.host_str() != self.metadata_uri.host_str()
        {
            return Err(MetadataError::InvalidJwksUri);
        }
        Ok(metadata)
    }
}

/// 业务作用：校验 metadata/JWKS URL 的 scheme、host、userinfo、fragment 与 SSRF 白名单。
fn validate_url(url: &reqwest::Url, allowed_hosts: &BTreeSet<String>) -> Result<(), ()> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(());
    }
    let host = url.host_str().ok_or(())?;
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1");
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(());
    }
    if !allowed_hosts.is_empty() && !allowed_hosts.contains(host) {
        return Err(());
    }
    Ok(())
}
