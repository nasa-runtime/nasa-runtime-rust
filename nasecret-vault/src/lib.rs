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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回三秒请求总超时、256 KiB 响应上限与空主机白名单；不读取 token 或连接 Vault。
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
    ///
    /// 参数说明：
    /// - `formatter`：接收提供方配置摘要的格式化器。
    ///
    /// 返回：只展示 endpoint 主机、mount、响应容量并将 token 替换为固定占位符；格式化写出失败返回错误。
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
    ///
    /// 参数说明：
    /// - `endpoint`：可信配置中的 Vault 根 URL，可带部署路径前缀，不得含凭据、查询或片段。
    /// - `mount`：KV 挂载的单个安全路径段。
    /// - `token`：由独立引导流程提供的 Vault 访问 token 材料。
    /// - `options`：请求超时、有界响应和可选主机白名单。
    ///
    /// 返回：endpoint 协议与主机策略、单段 mount、超时和容量合法且 HTTP 客户端构建成功时返回提供方；非法时返回配置分类错误。不验证 token 可用性或发起请求，token 的文本可编码性延迟到 read 检查。
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
    ///
    /// 参数说明：
    /// - `key`：采用 path/to/secret#field 形态的查询标识，路径与字段各段须通过安全字符规则。
    ///
    /// 返回：key 为有界安全路径加 #字段且远端成功返回符合 KV 响应结构的非空字符串时，返回其 UTF-8 材料清零容器；路径、token、状态、超限、网络、JSON 或字段异常均返回不带正文的单位错误。不刷新 token、不跟随重定向。
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
    ///
    /// 参数说明：
    /// - `key`：带 #字段选择器的稳定 Vault 查询标识。
    ///
    /// 返回：返回 read_inner 读取到的清零容器或脱敏单位错误，不另行缓存、重试或推导其它字段。
    async fn read(&self, key: &str) -> Result<SecretBytes, SecretProviderError> {
        self.read_inner(key).await
    }
}

/// 业务作用：限制 mount、path 与 field 为单个有界 ASCII 路径段。
///
/// 参数说明：
/// - `value`：待用于 mount、路径段或字段选择器的原标识。
///
/// 返回：非空、非 . 或 ..、最多 128 字节且只含 ASCII 字母数字、连字符、下划线或点时返回 true，否则 false，不解码 URL 转义。
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
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层敏感字符串长度为零时返回 true，不输出内容。
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 业务作用：借用 UTF-8 secret 字节，避免额外普通字符串复制。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回生命周期受当前清零字符串约束的 UTF-8 字节借用，不复制材料。
    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<'de> serde::Deserialize<'de> for SecretJsonString {
    /// 业务作用：使用自定义 visitor 直接把 JSON 字符串放入可清零容器。
    ///
    /// 参数说明：
    /// - `deserializer`：提供 Vault 字段字符串的 serde 来源。
    ///
    /// 返回：反序列化为拥有的清零字符串；只接受目标反序列化器可提供的字符串表示，非法类型返回其错误，不解析数字或复合值为材料。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// 只接受 JSON string 的可清零反序列化 visitor。
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SecretJsonString;

            /// 业务作用：描述反序列化器期望的 secret 字符串类型。
            ///
            /// 参数说明：
            /// - `formatter`：接收反序列化类型说明的格式化器。
            ///
            /// 返回：写出固定的字符串类型期望或传播格式化错误，不包含实际字段材料。
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a secret string")
            }

            /// 业务作用：复制借用字符串到可清零所有权容器。
            ///
            /// 参数说明：
            /// - `value`：由反序列化器临时借用的 secret 文本。
            ///
            /// 返回：复制借用字符串并包入 Zeroizing 后成功返回，不修改或清除来源缓冲，也不拒绝空字符串。
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SecretJsonString(Zeroizing::new(value.to_owned())))
            }

            /// 业务作用：直接接管已有 String 并包装为可清零容器。
            ///
            /// 参数说明：
            /// - `value`：由反序列化器移交的拥有 secret 文本。
            ///
            /// 返回：直接接管拥有字符串并包入 Zeroizing 后成功返回，不复制、不验证非空；非空约束由读取字段后检查。
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
