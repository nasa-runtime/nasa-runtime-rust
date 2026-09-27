//! 随 ConfigView 同点发布的命名 HTTP/TLS 材料与有界调用入口。

use crate::{
    Application, ApplicationError, ApplicationPhase, ApplicationResult, ComponentId, ConfigView,
    PrepareContext, WeakApplication,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    #[serde(default)]
    enabled: bool,
    base_url: Option<String>,
    certificate: Option<String>,
    private_key: Option<String>,
    trust: Option<String>,
    request_timeout_ms: Option<u64>,
    max_body_bytes: Option<usize>,
}

pub(crate) struct PreparedHttp {
    snapshot: Arc<nasecret_http::TlsHttpClientSnapshot>,
    base_url: reqwest::Url,
    max_body_bytes: usize,
}

impl std::fmt::Debug for PreparedHttp {
    /// 业务作用：配置诊断只展示代次，不输出目标地址和安全材料。
    /// 参数说明：`formatter` 为格式化目标。
    /// 返回：代次和固定隐藏标记。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedHttp")
            .field("generation", &self.snapshot.generation())
            .finish_non_exhaustive()
    }
}

/// 业务作用：构造与秘密同代的完整 HTTP/TLS 集合，不安装独立轮换指针。
/// 参数说明：`tree` 为配置声明；`secrets` 为本候选已经解析的材料。
/// 返回：最多 64 个完整 client，任一配置或 PEM 失败拒绝整个候选。
pub(crate) fn prepare(
    tree: &Value,
    secrets: &nasecret::SecretSnapshot,
) -> Result<BTreeMap<String, PreparedHttp>, crate::secret::SecretResolveError> {
    let plans: BTreeMap<String, Plan> = tree
        .get("http_clients")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|_| material_error())?
        .unwrap_or_default();
    if plans.len() > 64 {
        return Err(material_error());
    }
    let mut clients = BTreeMap::new();
    for (name, plan) in plans {
        if !plan.enabled {
            continue;
        }
        let base = reqwest::Url::parse(plan.base_url.as_deref().ok_or_else(material_error)?)
            .map_err(|_| material_error())?;
        if !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.host_str().is_none()
            || base.scheme() != "https"
        {
            return Err(material_error());
        }
        let mut config = nasecret_http::TlsHttpClientConfig::new(Arc::<str>::from(name.as_str()));
        config.identity = match (plan.certificate, plan.private_key) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(nasecret::TlsIdentityRef {
                certificate_chain: reference(&cert)?,
                private_key: reference(&key)?,
            }),
            _ => return Err(material_error()),
        };
        config.trust = plan
            .trust
            .map(|value| {
                reference(&value).map(|certificates| nasecret::TrustBundleRef { certificates })
            })
            .transpose()?;
        let timeout = plan.request_timeout_ms.unwrap_or(10_000);
        let max = plan.max_body_bytes.unwrap_or(16 * 1024 * 1024);
        if timeout == 0 || timeout > 300_000 || max == 0 || max > 64 * 1024 * 1024 {
            return Err(material_error());
        }
        config.request_timeout = Duration::from_millis(timeout);
        let snapshot = nasecret_http::RotatingTlsHttpClient::new(secrets, config)
            .map_err(|_| material_error())?
            .current();
        clients.insert(
            name,
            PreparedHttp {
                snapshot,
                base_url: base,
                max_body_bytes: max,
            },
        );
    }
    Ok(clients)
}

/// 业务作用：从明确的 secret locator 取得稳定身份，不接受内联 PEM。
/// 参数说明：`value` 为配置引用。
/// 返回：非空身份；其它形式拒绝。
fn reference(value: &str) -> Result<Arc<str>, crate::secret::SecretResolveError> {
    value
        .strip_prefix("secret://")
        .filter(|value| !value.is_empty())
        .map(Arc::from)
        .ok_or_else(material_error)
}

/// 业务作用：对候选拒绝仅给出固定 TLS 分类，隐藏证书和 endpoint。
/// 参数说明：无。
/// 返回：材料准备错误。
fn material_error() -> crate::secret::SecretResolveError {
    crate::secret::SecretResolveError::MalformedSpec {
        id: Arc::from("http_clients"),
        detail: "invalid named HTTP/TLS configuration or material".into(),
    }
}

/// 命名 HTTP 入口；每次请求从一个 ConfigView 固定 client、配置与秘密代次。
#[derive(Clone)]
pub struct ManagedHttpClient {
    application: WeakApplication,
    name: Arc<str>,
    owner: Arc<crate::managed_adapters::ManagedAdapter<()>>,
}

/// 完整有界 HTTP 响应；配置视图是本次请求实际使用的一代。
pub struct ManagedHttpResponse {
    /// 远端 HTTP 状态码；完整响应不等于业务操作成功。
    pub status: u16,
    /// 与完整正文对应的响应头，不包含后续独立请求的状态。
    pub headers: reqwest::header::HeaderMap,
    /// 已按命名客户端容量与调用预算读取完成的响应正文。
    pub body: Vec<u8>,
    /// 本请求实际使用的配置与凭据视图，便于按同代策略解释结果。
    pub config: Arc<ConfigView>,
}

impl ManagedHttpClient {
    /// 业务作用：在永久关闭门禁内完成一次 HTTP 请求与有界正文读取。
    /// 参数说明：`method` 为显式 HTTP 方法；`path` 是相对目标路径；`body` 为有界正文；`budget` 为可选调用预算。
    /// 返回：完整响应及实际代次；取消只停止本地等待，不证明远端未执行，写操作不自动重放。
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Vec<u8>,
        budget: Option<&nabudget::RequestBudget>,
    ) -> ApplicationResult<ManagedHttpResponse> {
        let _call = self
            .owner
            .enter()
            .await
            .map_err(|_| error("HTTP client is closed"))?;
        let app = self
            .application
            .upgrade()
            .ok_or_else(|| error("HTTP application is unavailable"))?;
        let view = app.config_view();
        let client = view
            .http_clients
            .get(self.name.as_ref())
            .ok_or_else(|| error("HTTP client is disabled in current configuration"))?;
        let url = client
            .base_url
            .join(path)
            .map_err(|_| error("invalid relative HTTP path"))?;
        if url.origin() != client.base_url.origin()
            || !url.username().is_empty()
            || url.password().is_some()
            || body.len() > client.max_body_bytes
        {
            return Err(error(
                "HTTP request exceeds its declared target or body boundary",
            ));
        }
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| error("invalid HTTP method"))?;
        let call = async {
            let mut response = client
                .snapshot
                .client()
                .request(method, url)
                .body(body)
                .send()
                .await
                .map_err(|_| error("HTTP transport failed; remote execution may be unknown"))?;
            if response
                .content_length()
                .is_some_and(|length| length > client.max_body_bytes as u64)
            {
                return Err(error("HTTP response body exceeds its limit"));
            }
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| error("HTTP response body failed"))?
            {
                if chunk.len() > client.max_body_bytes.saturating_sub(body.len()) {
                    return Err(error("HTTP response body exceeds its limit"));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(ManagedHttpResponse {
                status,
                headers,
                body,
                config: view.clone(),
            })
        };
        match budget {
            Some(budget) => budget
                .run(call)
                .await
                .map_err(|_| error("HTTP request budget ended; remote execution may be unknown"))?,
            None => call.await,
        }
    }
}

/// 业务作用：把首代命名 client 绑定到标准资源表和调用排干 owner。
/// 参数说明：`context` 提供准备阶段的资源登记与清理链。
/// 返回：每个名称只装配一次；后续材料通过 ConfigView 原子切换，旧句柄不得绕过关闭门禁。
pub(crate) fn install(context: &mut PrepareContext<'_>) -> ApplicationResult<()> {
    let app = context.application().clone();
    for name in app.config_view().http_clients.keys() {
        let owner = crate::managed_adapters::ManagedAdapter::new(Arc::new(()));
        context.activate(Box::new(crate::managed_adapters::AdapterShutdown(
            owner.clone(),
        )));
        context.register_resource(
            Some(name),
            ManagedHttpClient {
                application: app.downgrade(),
                name: Arc::from(name.as_str()),
                owner,
            },
        )?;
    }
    Ok(())
}

impl Application {
    /// 业务作用：取得已装配名称的受管 HTTP client，不暴露可绕过关闭门禁的原始 client。
    /// 参数说明：`name` 为 http_clients 中启用的稳定名称。
    /// 返回：共享调用 owner 的句柄；未知或应用关闭时拒绝。
    pub async fn http_client(&self, name: &str) -> ApplicationResult<ManagedHttpClient> {
        Ok(self
            .named_resource::<ManagedHttpClient>(name)
            .await?
            .clone())
    }
}

/// 业务作用：以固定原因汇报受管 HTTP 边界，不包含 URL、正文或证书。
/// 参数说明：`message` 为稳定错误摘要。
/// 返回：运行阶段的安全错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Application, ApplicationPhase::Running, message)
}
