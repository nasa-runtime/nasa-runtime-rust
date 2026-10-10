//! 可选的有限 HTTP 控制面传输。禁止隐式重定向和环境代理，错误不携带响应正文。

use crate::managed::{DiscoveryError, ErrorCode, Result};
pub use reqwest::{
    header::{HeaderMap, HeaderValue},
    Method, RequestBuilder, Url,
};
use std::{sync::Mutex, time::Duration};

pub struct HttpTransport {
    client: Mutex<Option<reqwest::Client>>,
    max_bytes: usize,
}
pub struct HttpResponse {
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl HttpTransport {
    /// 业务作用：创建独立连接池并限定响应大小及单次请求期限。
    /// 参数说明：`timeout` 是传输上限，`max_bytes` 是解码前字节上限，`ca_pem` 为可选受信 CA。
    /// 返回：TLS 证书校验开启的传输；非法 CA 或预算失败。
    pub fn new(timeout: Duration, max_bytes: usize, ca_pem: Option<&[u8]>) -> Result<Self> {
        Self::with_identity(timeout, max_bytes, ca_pem, None)
    }

    /// 业务作用：为需要双向 TLS 的控制面配置显式客户端证书。
    /// 参数说明：`timeout`、`max_bytes` 限制操作，`ca_pem` 为服务端 CA，`identity_pem` 包含客户端证书和私钥。
    /// 返回：只使用指定认证材料的连接池，证书无效时拒绝。
    pub fn with_identity(
        timeout: Duration,
        max_bytes: usize,
        ca_pem: Option<&[u8]>,
        identity_pem: Option<&[u8]>,
    ) -> Result<Self> {
        if timeout.is_zero() || max_bytes == 0 || max_bytes > 64 * 1024 * 1024 {
            return Err(error(
                ErrorCode::InvalidConfig,
                "invalid HTTP transport limits",
            ));
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .connect_timeout(timeout)
            .pool_max_idle_per_host(4)
            .pool_idle_timeout(Duration::from_secs(15));
        if let Some(pem) = ca_pem {
            let roots = reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|_| error(ErrorCode::InvalidConfig, "invalid CA certificate"))?;
            if roots.is_empty() {
                return Err(error(
                    ErrorCode::InvalidConfig,
                    "CA certificate bundle is empty",
                ));
            }
            // 显式 CA 定义本连接完整信任边界，不能另行接受环境或系统中的其他根。
            builder = builder.tls_built_in_root_certs(false);
            for root in roots {
                builder = builder.add_root_certificate(root);
            }
        }
        if let Some(pem) = identity_pem {
            builder = builder.identity(
                reqwest::Identity::from_pem(pem)
                    .map_err(|_| error(ErrorCode::InvalidConfig, "invalid TLS client identity"))?,
            );
        }
        let client = builder
            .build()
            .map_err(|_| error(ErrorCode::InvalidConfig, "cannot create HTTP transport"))?;
        Ok(Self {
            client: Mutex::new(Some(client)),
            max_bytes,
        })
    }

    /// 业务作用：构造一次有限控制面请求，关闭后不再发放连接池引用。
    /// 参数说明：`method` 与 `url` 是 adapter 已确定的控制面操作。
    /// 返回：请求构造器；不得记录包含秘密的请求对象。
    pub fn request(&self, method: Method, url: Url) -> Result<RequestBuilder> {
        let client = self
            .client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(client
            .as_ref()
            .ok_or_else(|| error(ErrorCode::OwnerClosed, "HTTP transport is closed"))?
            .request(method, url))
    }

    /// 业务作用：读取有限响应并将 HTTP 错误映射为稳定类别。
    /// 参数说明：`request` 是已经配置作用域及凭据的请求。
    /// 返回：完整响应字节；超过上限、截断或权限失败不返回部分成功。
    pub async fn send(&self, request: RequestBuilder) -> Result<HttpResponse> {
        self.send_admitted(request, None).await
    }

    /// 业务作用：在实际请求首次交给传输时核对写许可，避免鉴权等待后越过撤权。
    /// 参数说明：`request` 是最终请求，`admission` 为受管写许可；只读操作为空。
    /// 返回：完整有限响应；撤权发生在提交前时不发出请求。
    pub async fn send_admitted(
        &self,
        request: RequestBuilder,
        admission: Option<&dyn crate::managed::WriteAdmission>,
    ) -> Result<HttpResponse> {
        self.send_inner(request, admission, None)
            .await
            .map(|(_, response)| response)
    }

    /// 业务作用：保留只读查询的 404 状态和有限正文，供 adapter 区分后端明确缺失与接口不可用。
    /// 参数说明：`request` 是已限定作用域和凭据的查询。
    /// 返回：成功或 404 的状态码及完整有限响应；404 必须由调用方核对协议码，其他 HTTP 错误仍拒绝。
    pub async fn send_allow_not_found(
        &self,
        request: RequestBuilder,
    ) -> Result<(u16, HttpResponse)> {
        self.send_inner(request, None, Some(404)).await
    }

    /// 业务作用：保留只读查询的 400 状态和有限正文，供 adapter 识别后端定义的业务缺失码。
    /// 参数说明：`request` 是已限定作用域和凭据的查询。
    /// 返回：成功或 400 的状态码及完整有限响应；400 必须复验协议码，权限、截断与容量错误仍拒绝。
    pub async fn send_allow_bad_request(
        &self,
        request: RequestBuilder,
    ) -> Result<(u16, HttpResponse)> {
        self.send_inner(request, None, Some(400)).await
    }

    /// 业务作用：在提交许可与响应限额内执行控制面请求，默认拒绝所有非成功状态。
    /// 参数说明：`request` 为最终请求，`admission` 为可选写许可，`evidence_status` 仅保留只读入口指定的待协议复验状态。
    /// 返回：状态码及完整响应；拒绝、截断与超限均不发布部分证据。
    async fn send_inner(
        &self,
        request: RequestBuilder,
        admission: Option<&dyn crate::managed::WriteAdmission>,
        evidence_status: Option<u16>,
    ) -> Result<(u16, HttpResponse)> {
        let mut future = std::pin::pin!(request.send());
        let mut submitted = false;
        let response = std::future::poll_fn(|cx| {
            use std::future::Future;
            if submitted {
                return future.as_mut().poll(cx).map(Ok);
            }
            let mut output = std::task::Poll::Pending;
            let mut submit = || {
                submitted = true;
                output = future.as_mut().poll(cx);
            };
            if let Some(admission) = admission {
                if let Err(error) = admission.submit(&mut submit) {
                    return std::task::Poll::Ready(Err(error));
                }
            } else {
                submit();
            }
            output.map(Ok)
        })
        .await?;
        let mut response = response.map_err(|err| {
            if err.is_timeout() {
                error(
                    ErrorCode::DeadlineExceeded,
                    "control-plane request timed out",
                )
            } else {
                error(ErrorCode::Unavailable, "control-plane transport failed")
            }
        })?;
        let status = response.status().as_u16();
        // 仅显式查询入口允许读取指定错误正文，不能让协议特例放宽写侧确认或权限门禁。
        if !(response.status().is_success() || evidence_status == Some(status)) {
            let code = match response.status().as_u16() {
                401 => ErrorCode::Unauthenticated,
                403 => ErrorCode::PermissionDenied,
                404 => ErrorCode::NoInstances,
                409 => ErrorCode::Conflict,
                429 => ErrorCode::RateLimited,
                400 | 405 | 422 | 501 => ErrorCode::Unsupported,
                _ => ErrorCode::Unavailable,
            };
            return Err(error(code, "control-plane request was rejected"));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_bytes as u64)
        {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "control-plane response exceeds byte limit",
            ));
        }
        let headers = response.headers().clone();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            error(
                ErrorCode::IncompleteSnapshot,
                "control-plane response was interrupted",
            )
        })? {
            if body.len().saturating_add(chunk.len()) > self.max_bytes {
                return Err(error(
                    ErrorCode::CapacityExceeded,
                    "control-plane response exceeds byte limit",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, HttpResponse { headers, body }))
    }

    /// 业务作用：释放连接池持有权，禁止新的控制面操作。
    /// 参数说明：无。
    /// 返回：已取得的请求仍须由 owner 取消或等待到截止点。
    pub fn close(&self) {
        self.client
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    /// 业务作用：等待一条 Kubernetes 对象变更，不把 bookmark 或连接存活当健康证据。
    /// 参数说明：`request` 是带作用域及游标的 watch 请求；行大小受传输字节限额保护。
    /// 返回：首个对象变更触发重读；410、EOF、错误帧与超大事件返回失败。
    pub async fn wait_json_event(&self, request: RequestBuilder) -> Result<()> {
        let mut response = request
            .send()
            .await
            .map_err(|_| error(ErrorCode::Unavailable, "control-plane watch failed"))?;
        if !response.status().is_success() {
            let code = match response.status().as_u16() {
                401 => ErrorCode::Unauthenticated,
                403 => ErrorCode::PermissionDenied,
                _ => ErrorCode::IncompleteSnapshot,
            };
            return Err(error(code, "control-plane watch was rejected"));
        }
        let mut buffer = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            error(
                ErrorCode::IncompleteSnapshot,
                "watch stream was interrupted",
            )
        })? {
            for byte in chunk {
                if buffer.len() >= self.max_bytes {
                    return Err(error(
                        ErrorCode::CapacityExceeded,
                        "watch event exceeds byte limit",
                    ));
                }
                if byte != b'\n' {
                    buffer.push(byte);
                    continue;
                }
                if buffer.is_empty() {
                    continue;
                }
                let event: serde_json::Value = serde_json::from_slice(&buffer)
                    .map_err(|_| error(ErrorCode::IncompleteSnapshot, "invalid watch event"))?;
                buffer.clear();
                match event["type"].as_str() {
                    Some("BOOKMARK") => {}
                    Some("ADDED" | "MODIFIED" | "DELETED") => return Ok(()),
                    _ => {
                        return Err(error(
                            ErrorCode::IncompleteSnapshot,
                            "watch synchronization was lost",
                        ))
                    }
                }
            }
        }
        Err(error(ErrorCode::IncompleteSnapshot, "watch stream ended"))
    }
}

impl HttpResponse {
    /// 业务作用：只对通过字节限额的完整响应执行结构化解码。
    /// 参数说明：目标类型由 adapter 确定，不输出解析错误中的响应片段。
    /// 返回：完整数据或脱敏的协议错误。
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).map_err(|_| {
            error(
                ErrorCode::IncompleteSnapshot,
                "invalid control-plane JSON response",
            )
        })
    }
}

/// 业务作用：拒绝含用户信息、查询串或片段的控制面根地址。
/// 参数说明：`value` 为显式 HTTP(S) 地址，允许部署要求的路径前缀。
/// 返回：规范化 URL，不从环境推测代理或其他后端。
pub fn base_url(value: &str) -> Result<Url> {
    let url = Url::parse(value)
        .map_err(|_| error(ErrorCode::InvalidConfig, "invalid control-plane URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(error(
            ErrorCode::InvalidConfig,
            "unsupported control-plane URL",
        ));
    }
    Ok(url)
}

/// 业务作用：提供不会携带服务器响应正文的固定错误。
/// 参数说明：`code` 是稳定类别，`reason` 为固定原因。
/// 返回：适配器可直接传给 owner 的失败事实。
pub fn error(code: ErrorCode, reason: &'static str) -> DiscoveryError {
    DiscoveryError::new(code, reason)
}
