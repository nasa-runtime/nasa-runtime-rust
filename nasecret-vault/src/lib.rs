//! Vault/OpenBao KV v2 secret provider。请求有界、拒绝重定向，错误不携带响应正文或 token。

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::time::Duration;

use nasecret::{SecretBytes, SecretProvider, SecretProviderError};
use zeroize::Zeroizing;

/// Vault JSON 响应的框架硬上限。
pub const MAX_VAULT_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// 单次 Vault 请求允许的最大总时长。
pub const MAX_VAULT_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// provider key 的总长度与路径段数上限；直接调用 `SecretProvider::read` 也不能绕过 `SecretSpec`。
const MAX_VAULT_KEY_BYTES: usize = 512;
const MAX_VAULT_PATH_SEGMENTS: usize = 32;

/// Vault/OpenBao HTTP 读取边界。
#[derive(Debug, Clone)]
pub struct VaultOptions {
    /// 单次读取总超时。
    pub timeout: Duration,
    /// JSON 响应体硬上限。
    pub max_response_bytes: usize,
    /// endpoint/JWKS 类 SSRF 主机白名单；空表示只接受配置 URL 自身的 host。
    pub allowed_hosts: BTreeSet<String>,
}

impl Default for VaultOptions {
    /// 业务作用：使用 3 秒、256 KiB 且只信任 endpoint 自身 host 的保守缺省。
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3),
            max_response_bytes: 256 * 1024,
            allowed_hosts: BTreeSet::new(),
        }
    }
}

/// 构造期安全配置错误；不含 token。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VaultConfigError {
    /// endpoint URL 非法。
    InvalidEndpoint,
    /// 非 HTTPS 且不是 loopback。
    InsecureEndpoint,
    /// host 不在白名单。
    HostNotAllowed,
    /// mount 非单个安全路径段。
    InvalidMount,
    /// timeout/size 为零。
    InvalidLimit,
}

/// Vault/OpenBao KV v2 adapter。
///
/// `SecretProvider::read` 的 key 形如 `team/service#field`：`#` 前是 KV path，后面是
/// `data.data` 下的字段名。路径与字段只用于定位，绝不写入错误正文。
pub struct VaultKvV2Provider {
    endpoint: reqwest::Url,
    mount: String,
    token: SecretBytes,
    client: reqwest::Client,
    max_response_bytes: usize,
}

impl std::fmt::Debug for VaultKvV2Provider {
    /// 业务作用：仅展示 host、mount 与响应上限，固定隐藏 bootstrap token。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultKvV2Provider")
            .field("host", &self.endpoint.host_str())
            .field("mount", &self.mount)
            .field("token", &"<redacted>")
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

impl VaultKvV2Provider {
    /// 业务作用：创建 adapter；bootstrap token 必须来自 env/file/root-of-trust，不能由该 provider 自身解析。
    pub fn new(
        endpoint: &str,
        mount: impl Into<String>,
        token: SecretBytes,
        options: VaultOptions,
    ) -> Result<Self, VaultConfigError> {
        let endpoint =
            reqwest::Url::parse(endpoint).map_err(|_| VaultConfigError::InvalidEndpoint)?;
        let host = endpoint
            .host_str()
            .ok_or(VaultConfigError::InvalidEndpoint)?;
        let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1");
        if endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && loopback) {
            return Err(VaultConfigError::InsecureEndpoint);
        }
        if !options.allowed_hosts.is_empty()
            && !options
                .allowed_hosts
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(host))
        {
            return Err(VaultConfigError::HostNotAllowed);
        }
        if endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err(VaultConfigError::InvalidEndpoint);
        }
        if endpoint.cannot_be_a_base()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(VaultConfigError::InvalidEndpoint);
        }
        let mount = mount.into();
        if !safe_segment(&mount) {
            return Err(VaultConfigError::InvalidMount);
        }
        if options.timeout.is_zero()
            || options.timeout > MAX_VAULT_TIMEOUT
            || options.max_response_bytes == 0
            || options.max_response_bytes > MAX_VAULT_RESPONSE_BYTES
        {
            return Err(VaultConfigError::InvalidLimit);
        }
        let client = reqwest::Client::builder()
            .timeout(options.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| VaultConfigError::InvalidEndpoint)?;
        Ok(Self {
            endpoint,
            mount,
            token,
            client,
            max_response_bytes: options.max_response_bytes,
        })
    }

    /// 业务作用：校验 `path#field`、执行有界 KV v2 请求并只提取目标字符串字段。
    async fn read_inner(&self, key: &str) -> Result<SecretBytes, SecretProviderError> {
        if key.len() > MAX_VAULT_KEY_BYTES {
            return Err(SecretProviderError);
        }
        let (path, field) = key.split_once('#').ok_or(SecretProviderError)?;
        let path_segments = path.split('/').collect::<Vec<_>>();
        if path_segments.is_empty()
            || path_segments.len() > MAX_VAULT_PATH_SEGMENTS
            || path_segments.iter().any(|segment| !safe_segment(segment))
            || !safe_segment(field)
        {
            return Err(SecretProviderError);
        }
        let mut url = self.endpoint.clone();
        {
            let mut segments = url.path_segments_mut().map_err(|_| SecretProviderError)?;
            segments.pop_if_empty();
            segments.push("v1");
            segments.push(&self.mount);
            segments.push("data");
            for segment in path_segments {
                segments.push(segment);
            }
        }
        let token = std::str::from_utf8(self.token.expose()).map_err(|_| SecretProviderError)?;
        let mut response = self
            .client
            .get(url)
            .header("x-vault-token", token)
            .send()
            .await
            .map_err(|_| SecretProviderError)?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(SecretProviderError);
        }
        let mut bytes = Zeroizing::new(Vec::new());
        while let Some(chunk) = response.chunk().await.map_err(|_| SecretProviderError)? {
            if chunk.len() > self.max_response_bytes.saturating_sub(bytes.len()) {
                return Err(SecretProviderError);
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut document: VaultResponse =
            serde_json::from_slice(&bytes).map_err(|_| SecretProviderError)?;
        let value = document
            .data
            .data
            .remove(field)
            .ok_or(SecretProviderError)?;
        if value.is_empty() {
            return Err(SecretProviderError);
        }
        Ok(SecretBytes::new(value.as_bytes().to_vec()))
    }
}

#[async_trait::async_trait]
impl SecretProvider for VaultKvV2Provider {
    /// 业务作用：通过统一 provider 合同读取一个 Vault KV v2 字段。
    async fn read(&self, key: &str) -> Result<SecretBytes, SecretProviderError> {
        self.read_inner(key).await
    }
}

/// 业务作用：限制 mount、path 与 field 为单个有界 ASCII 路径段。
fn safe_segment(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// Vault KV v2 响应的最小外层投影。
#[derive(serde::Deserialize)]
struct VaultResponse {
    data: VaultData,
}

/// Vault `data.data` 字段的字符串映射投影。
#[derive(serde::Deserialize)]
struct VaultData {
    data: std::collections::BTreeMap<String, SecretJsonString>,
}

/// 反序列化后在 Drop 时清零的 JSON secret 字符串。
struct SecretJsonString(Zeroizing<String>);

impl SecretJsonString {
    /// 业务作用：判断返回字段是否为空。
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 业务作用：借用 UTF-8 secret 字节，避免额外普通字符串复制。
    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<'de> serde::Deserialize<'de> for SecretJsonString {
    /// 业务作用：使用自定义 visitor 直接把 JSON 字符串放入可清零容器。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// 只接受 JSON string 的可清零反序列化 visitor。
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SecretJsonString;

            /// 业务作用：描述反序列化器期望的 secret 字符串类型。
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a secret string")
            }

            /// 业务作用：复制借用字符串到可清零所有权容器。
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SecretJsonString(Zeroizing::new(value.to_owned())))
            }

            /// 业务作用：直接接管已有 String 并包装为可清零容器。
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SecretJsonString(Zeroizing::new(value)))
            }
        }

        deserializer.deserialize_string(Visitor)
    }
}
