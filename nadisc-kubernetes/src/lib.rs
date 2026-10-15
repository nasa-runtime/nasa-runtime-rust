//! 只读取显式 namespace 内的 Service 与 EndpointSlice；不注册实例，不读取 Pod、Node 或 Secret。
//! 公共 owner 按 qualifier 隔离集群身份，默认连接名为 primary。
//! Service 与 EndpointSlice 联合复验决定候选；权限失效立即撤销旧发送资格。
//! 显式 kubeconfig 由 naml 有界读取，原始字段不执行环境表达式或 YAML 文件包含。
//! 正文和累计文本分别最多一 MiB，重复键、未知标签或超限拒绝整个认证配置，错误不回显正文。

mod auth;
pub use auth::Authentication;

use nadisc::{
    http::{error, HeaderValue, HttpTransport, Method, RequestBuilder, Url},
    managed::*,
    Instance,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};

/// 单个集群的显式认证、namespace 范围与有限读取预算。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesConfig {
    /// 认证材料的唯一来源；不执行 kubeconfig 中的外部认证程序。
    pub auth: Authentication,
    /// 允许访问的 namespace 白名单，不自动枚举其他作用域。
    pub namespaces: Vec<String>,
    /// 按逻辑服务名绑定的目标，不接受重复键。
    pub services: BTreeMap<String, ServiceBinding>,
    /// 一次请求的最大耗时，单位为毫秒。
    #[serde(default = "request_timeout")]
    pub request_timeout_ms: u64,
    /// 单次 watch 的等待上限，单位为毫秒。
    #[serde(default = "watch_timeout")]
    pub watch_timeout_ms: u64,
    /// 单次后端响应允许占用的最大字节数。
    #[serde(default = "byte_limit")]
    pub max_response_bytes: usize,
    /// 单次完整服务观察允许包含的实例数量上限。
    #[serde(default = "instance_limit")]
    pub max_instances: usize,
    /// Kubernetes 分页读取期望的单页条目数。
    #[serde(default = "page_size")]
    pub page_size: usize,
}

/// 逻辑服务到具体 Service 及命名端口的显式绑定。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceBinding {
    /// 目标所在的命名空间。
    pub namespace: String,
    /// 目标后端中的服务名称。
    pub name: String,
    /// Service 中用于发现实例端口的命名端口。
    pub port_name: String,
    /// 允许路由的实例地址族。
    #[serde(default)]
    pub address_family: AddressFamily,
}

/// 实例地址族筛选策略。
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressFamily {
    /// 只接受 IPv4 地址。
    Ipv4,
    /// 只接受 IPv6 地址。
    Ipv6,
    /// 接受 IPv4 和 IPv6 地址。
    #[default]
    Both,
}

/// 只读取 Service 与控制器所属 EndpointSlice 的发现适配器。
pub struct KubernetesProvider {
    config: KubernetesConfig,
    server: Url,
    transport: HttpTransport,
    credential: auth::Credential,
    versions: Mutex<BTreeMap<String, (String, String)>>,
    closed: AtomicBool,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    uid: String,
    #[serde(default)]
    resource_version: String,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    owner_references: Vec<Owner>,
    #[serde(default)]
    deletion_timestamp: Option<String>,
}
#[derive(Deserialize)]
struct Owner {
    uid: String,
    kind: String,
    #[serde(rename = "apiVersion")]
    api_version: String,
    #[serde(default)]
    controller: bool,
}
#[derive(Deserialize)]
struct Service {
    metadata: Metadata,
    spec: Spec,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    selector: HashMap<String, String>,
    #[serde(default)]
    publish_not_ready_addresses: bool,
    ports: Vec<ServicePort>,
}
#[derive(Deserialize)]
struct ServicePort {
    #[serde(default)]
    name: String,
    protocol: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Slice {
    metadata: Metadata,
    address_type: String,
    #[serde(deserialize_with = "deserialize_slice_list")]
    ports: Vec<SlicePort>,
    #[serde(deserialize_with = "deserialize_slice_list")]
    endpoints: Vec<Endpoint>,
}

/// 业务作用：接受控制器以 null 或数组表达的空 EndpointSlice 字段。
/// 参数说明：`deserializer` 读取完整响应中的端点或端口列表。
/// 返回：null 成为空列表；缺席字段、错误类型或非法成员仍使整份响应解码失败。
fn deserialize_slice_list<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}
#[derive(Deserialize)]
struct SlicePort {
    name: Option<String>,
    port: Option<u16>,
    protocol: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Endpoint {
    addresses: Vec<String>,
    conditions: Conditions,
    #[serde(default)]
    target_ref: Option<ObjectRef>,
}
#[derive(Deserialize)]
struct ObjectRef {
    #[serde(default)]
    uid: String,
}
#[derive(Deserialize)]
struct Conditions {
    ready: Option<bool>,
    serving: Option<bool>,
    terminating: Option<bool>,
}
#[derive(Deserialize)]
struct SliceList {
    metadata: ListMetadata,
    items: Vec<Slice>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListMetadata {
    resource_version: String,
    #[serde(default, rename = "continue")]
    continuation: String,
}

impl KubernetesConfig {
    /// 业务作用：在认证文件和网络访问前校验完整 Service 作用域及预算。
    /// 参数说明：无。
    /// 返回：静态配置有效时成功，非法绑定和容量拒绝构造。
    pub fn validate(&self) -> Result<()> {
        if self.services.is_empty()
            || self.services.len() > 1024
            || self.namespaces.is_empty()
            || self.request_timeout_ms == 0
            || self.request_timeout_ms > 86_400_000
            || self.watch_timeout_ms < 1000
            || self.watch_timeout_ms > 300_000
            || self.max_instances == 0
            || self.max_instances > 65536
            || self.page_size == 0
            || self.page_size > 1000
            || self.max_response_bytes == 0
            || self.max_response_bytes > 64 * 1024 * 1024
        {
            return Err(error(
                ErrorCode::InvalidConfig,
                "invalid Kubernetes discovery limits",
            ));
        }
        let namespaces: BTreeSet<_> = self.namespaces.iter().collect();
        if namespaces.len() != self.namespaces.len() {
            return Err(error(
                ErrorCode::InvalidConfig,
                "duplicate Kubernetes namespace",
            ));
        }
        for (alias, service) in &self.services {
            if alias.is_empty()
                || alias.len() > 512
                || !namespaces.contains(&service.namespace)
                || !dns_label(&service.namespace)
                || !dns_label(&service.name)
                || !dns_label(&service.port_name)
            {
                return Err(error(
                    ErrorCode::InvalidConfig,
                    "invalid Kubernetes service binding",
                ));
            }
        }
        Ok(())
    }
}

impl KubernetesProvider {
    /// 业务作用：在首个网络操作前校验作用域并加载显式认证材料。
    /// 参数说明：`config` 包含精确 Service/端口映射，认证来源不得隐式回退。
    /// 返回：无后台任务的只读 adapter；不安全认证模式和非法范围拒绝。
    pub async fn new(config: KubernetesConfig) -> Result<Self> {
        config.validate()?;
        let material = tokio::time::timeout(
            Duration::from_millis(config.request_timeout_ms),
            auth::load(&config.auth),
        )
        .await
        .map_err(|_| {
            error(
                ErrorCode::DeadlineExceeded,
                "Kubernetes authentication loading timed out",
            )
        })??;
        let transport = HttpTransport::with_identity(
            Duration::from_millis(config.request_timeout_ms),
            config.max_response_bytes,
            Some(&material.ca),
            material.identity.as_deref(),
        )?;
        Ok(Self {
            config,
            server: material.server,
            transport,
            credential: material.credential,
            versions: Mutex::new(BTreeMap::new()),
            closed: AtomicBool::new(false),
        })
    }

    /// 业务作用：为每次请求读取既定 token 来源，支持 ServiceAccount 文件轮换。
    /// 参数说明：`url` 是已限制 scope 的 API 路径。
    /// 返回：包含当前认证材料的只读请求；既定 token 来源不可用返回认证失败，凭据不进入诊断或查询串。
    async fn request(&self, url: Url) -> Result<RequestBuilder> {
        if self.closed.load(Ordering::Acquire) {
            return Err(error(
                ErrorCode::OwnerClosed,
                "Kubernetes provider is closed",
            ));
        }
        let token = match &self.credential {
            auth::Credential::Token(token) => Some(token.clone()),
            auth::Credential::TokenFile(path) => Some(
                // 运行期文件类型、容量或读取失败都使当前认证来源失效，旧身份不能继续使用路由宽限。
                // read_file 同时服务启动配置加载，在此按 token 来源语义分类，不改变 kubeconfig/CA 校验。
                String::from_utf8(auth::read_file(path).await.map_err(|_| {
                    error(
                        ErrorCode::Unauthenticated,
                        "Kubernetes token file is unavailable or invalid",
                    )
                })?)
                .map_err(|_| {
                    error(
                        ErrorCode::Unauthenticated,
                        "Kubernetes token encoding is invalid",
                    )
                })?
                .trim()
                .into(),
            ),
            auth::Credential::Certificate => None,
        };
        let mut request = self.transport.request(Method::GET, url)?;
        if let Some(token) = token {
            if token.is_empty() || token.len() > 65536 {
                return Err(error(
                    ErrorCode::Unauthenticated,
                    "Kubernetes token is empty or oversized",
                ));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                error(
                    ErrorCode::Unauthenticated,
                    "invalid Kubernetes authorization header",
                )
            })?;
            value.set_sensitive(true);
            request = request.header("Authorization", value);
        }
        Ok(request)
    }

    /// 业务作用：构造只读 namespaced 资源地址，不扩大到集群目录。
    /// 参数说明：`binding` 是明确 Service，`slices` 选择 EndpointSlice 集合，`collection` 选择 Service 集合 watch。
    /// 返回：已经逐段转义的 API URL。
    fn url(&self, binding: &ServiceBinding, slices: bool, collection: bool) -> Result<Url> {
        let mut url = self.server.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Kubernetes API URL"))?;
        segments.pop_if_empty();
        if slices {
            segments.extend([
                "apis",
                "discovery.k8s.io",
                "v1",
                "namespaces",
                &binding.namespace,
                "endpointslices",
            ]);
        } else {
            segments.extend(["api", "v1", "namespaces", &binding.namespace, "services"]);
            if !collection {
                segments.push(&binding.name);
            }
        }
        drop(segments);
        Ok(url)
    }

    /// 业务作用：取得严格限定作用域的服务映射。
    /// 参数说明：`service` 是本地服务别名。
    /// 返回：明确 namespace、Service、端口及地址族。
    fn binding(&self, service: &str) -> Result<&ServiceBinding> {
        self.config.services.get(service).ok_or_else(|| {
            error(
                ErrorCode::InvalidConfig,
                "unknown Kubernetes service binding",
            )
        })
    }

    /// 业务作用：读取 Service UID 和接流政策，避免同名重建后使用旧 EndpointSlice。
    /// 参数说明：`binding` 是明确 Service 目标。
    /// 返回：当前 Service 或真实 404 缺失；权限失败不当作空服务。
    async fn service(&self, binding: &ServiceBinding) -> Result<Option<Service>> {
        let request = self.request(self.url(binding, false, false)?).await?;
        match self.transport.send(request).await {
            Ok(response) => response.json().map(Some),
            Err(failure) if failure.code == ErrorCode::NoInstances => Ok(None),
            Err(failure) => Err(failure),
        }
    }

    /// 业务作用：原子收集完整分页结果，限制累计字节并核对列表游标一致性。
    /// 参数说明：`binding` 限定 namespace 和 Service 标签。
    /// 返回：全部 EndpointSlice 与原始 opaque resourceVersion；分页错误不返回部分候选。
    async fn slices(&self, binding: &ServiceBinding) -> Result<(Vec<Slice>, String)> {
        let mut continuation = String::new();
        let mut seen = BTreeSet::new();
        let mut slices = Vec::new();
        let mut version = String::new();
        let mut bytes = 0usize;
        loop {
            let mut url = self.url(binding, true, true)?;
            url.query_pairs_mut()
                .append_pair(
                    "labelSelector",
                    &format!("kubernetes.io/service-name={}", binding.name),
                )
                .append_pair("limit", &self.config.page_size.to_string());
            if !continuation.is_empty() {
                url.query_pairs_mut().append_pair("continue", &continuation);
            }
            let response = self.transport.send(self.request(url).await?).await?;
            bytes = bytes.saturating_add(response.body.len());
            if bytes > self.config.max_response_bytes {
                return Err(error(
                    ErrorCode::CapacityExceeded,
                    "Kubernetes snapshot exceeds cumulative byte limit",
                ));
            }
            let page: SliceList = response.json()?;
            if page.metadata.resource_version.is_empty()
                || (!version.is_empty() && version != page.metadata.resource_version)
            {
                return Err(error(
                    ErrorCode::IncompleteSnapshot,
                    "Kubernetes pagination revision changed",
                ));
            }
            version = page.metadata.resource_version;
            slices.extend(page.items);
            if slices.len() > self.config.max_instances {
                return Err(error(
                    ErrorCode::CapacityExceeded,
                    "Kubernetes slice capacity exceeded",
                ));
            }
            continuation = page.metadata.continuation;
            if continuation.is_empty() {
                return Ok((slices, version));
            }
            if !seen.insert(continuation.clone()) || continuation.len() > 65536 {
                return Err(error(
                    ErrorCode::IncompleteSnapshot,
                    "Kubernetes pagination did not advance",
                ));
            }
        }
    }

    /// 业务作用：把 Service 所有权、端口和 EndpointSlice 条件转换为保守接流集合。
    /// 参数说明：`binding` 提供受信端口与地址族，`service` 是前后复验的 Service，`slices` 是完整列表。
    /// 返回：去重后的实例；已确认 Service 身份、来源或端口不符时撤销旧证据，重复记录资格冲突使用更严格状态。
    fn project(
        &self,
        binding: &ServiceBinding,
        service: &Service,
        slices: Vec<Slice>,
    ) -> Result<Vec<Instance>> {
        if service.metadata.uid.is_empty()
            || service.metadata.resource_version.is_empty()
            || service.metadata.namespace != binding.namespace
            || service.metadata.name != binding.name
            || service.spec.kind == "ExternalName"
            || service.spec.selector.is_empty()
        {
            // 完整复验已确认当前 Service 不再是受信来源，不能把该负面事实当作同步失败沿用旧端点。
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "Kubernetes Service is not a controller-managed endpoint source",
            ));
        }
        if service.metadata.deletion_timestamp.is_some() {
            return Ok(Vec::new());
        }
        if service
            .spec
            .ports
            .iter()
            .filter(|port| port.name == binding.port_name && port.protocol == "TCP")
            .count()
            != 1
        {
            // 命名 TCP 绑定已被移除、改协议或变得含混，即使旧 Slice 尚在也不再授予旧端点发送权。
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "Kubernetes service port binding is absent or ambiguous",
            ));
        }
        let mut instances: BTreeMap<(IpAddr, u16), Instance> = BTreeMap::new();
        let mut examined = 0usize;
        for slice in slices {
            if slice.metadata.namespace != binding.namespace
                || slice.metadata.deletion_timestamp.is_some()
                || slice.metadata.labels.get("kubernetes.io/service-name") != Some(&binding.name)
                || !slice.metadata.owner_references.iter().any(|owner| {
                    owner.uid == service.metadata.uid
                        && owner.kind == "Service"
                        && owner.api_version == "v1"
                        && owner.controller
                })
            {
                continue;
            }
            if !matches!(slice.address_type.as_str(), "IPv4" | "IPv6") {
                return Err(error(
                    ErrorCode::Unsupported,
                    "Kubernetes endpoint address type is unsupported",
                ));
            }
            // 归属和地址类型已核对的空 Slice 是完整撤权事实，不需要端口即可撤下旧候选。
            // 非空 Slice 仍必须提供唯一的命名 TCP 端口，不能把缺少路由信息当作空目录。
            if slice.endpoints.is_empty() {
                continue;
            }
            let mut ports = slice.ports.iter().filter(|port| {
                port.name.as_deref() == Some(&binding.port_name)
                    && port.protocol.as_deref() == Some("TCP")
            });
            let Some(port) = ports
                .next()
                .and_then(|port| port.port)
                .filter(|port| *port != 0)
            else {
                return Err(error(
                    ErrorCode::IncompleteSnapshot,
                    "Kubernetes endpoint port is absent",
                ));
            };
            if ports.next().is_some() {
                return Err(error(
                    ErrorCode::Conflict,
                    "Kubernetes endpoint port is ambiguous",
                ));
            }
            for endpoint in slice.endpoints {
                let healthy = endpoint.conditions.ready == Some(true)
                    && endpoint.conditions.terminating != Some(true)
                    && endpoint.conditions.serving != Some(false)
                    && (!service.spec.publish_not_ready_addresses
                        || endpoint.conditions.serving == Some(true));
                for address in endpoint.addresses {
                    examined += 1;
                    if examined > self.config.max_instances {
                        return Err(error(
                            ErrorCode::CapacityExceeded,
                            "Kubernetes endpoint capacity exceeded",
                        ));
                    }
                    let address: IpAddr = address.parse().map_err(|_| {
                        error(
                            ErrorCode::InvalidSnapshot,
                            "Kubernetes endpoint IP is invalid",
                        )
                    })?;
                    if address.is_unspecified()
                        || address.is_multicast()
                        || (address.is_ipv4() != (slice.address_type == "IPv4"))
                    {
                        return Err(error(
                            ErrorCode::InvalidSnapshot,
                            "Kubernetes endpoint address family is inconsistent",
                        ));
                    }
                    if matches!(binding.address_family, AddressFamily::Ipv4) && address.is_ipv6()
                        || matches!(binding.address_family, AddressFamily::Ipv6)
                            && address.is_ipv4()
                    {
                        continue;
                    }
                    let candidate = Instance::new(address.to_string(), port)
                        .with_healthy(healthy)
                        .with_metadata("nasa.kubernetes.service_uid", &service.metadata.uid)
                        .with_metadata(
                            "nasa.kubernetes.target_uid",
                            endpoint
                                .target_ref
                                .as_ref()
                                .map_or("", |target| &target.uid),
                        );
                    match instances.get_mut(&(address, port)) {
                        Some(previous) => {
                            previous.healthy &=
                                candidate.healthy && previous.metadata == candidate.metadata;
                        }
                        None => {
                            instances.insert((address, port), candidate);
                        }
                    }
                }
            }
        }
        Ok(instances.into_values().collect())
    }

    /// 业务作用：监听已观察资源的后续变化，只用于触发完整重读。
    /// 参数说明：`binding` 是固定目标，`slices` 选择资源类型，`version` 是后端 opaque 游标。
    /// 返回：对象变化或同步失败；bookmark 不更新健康期限。
    async fn watch(&self, binding: &ServiceBinding, slices: bool, version: &str) -> Result<()> {
        let mut url = self.url(binding, slices, true)?;
        let (key, selector) = if slices {
            (
                "labelSelector",
                format!("kubernetes.io/service-name={}", binding.name),
            )
        } else {
            ("fieldSelector", format!("metadata.name={}", binding.name))
        };
        url.query_pairs_mut()
            .append_pair(key, &selector)
            .append_pair("watch", "true")
            .append_pair("allowWatchBookmarks", "true")
            .append_pair("resourceVersion", version)
            .append_pair(
                "timeoutSeconds",
                &(self.config.watch_timeout_ms / 1000).max(1).to_string(),
            );
        let request = self.request(url).await?.timeout(Duration::from_millis(
            self.config.watch_timeout_ms + self.config.request_timeout_ms,
        ));
        self.transport.wait_json_event(request).await
    }
}

#[async_trait::async_trait]
impl ManagedProvider for KubernetesProvider {
    /// 业务作用：以真实 API 与查询过滤条件识别服务集合，使本地别名不改变后端身份。
    /// 参数说明：`service` 是已声明的服务别名。
    /// 返回：包含规范化 API 地址、namespace、Service、命名端口和地址族的键；未知别名返回错误，不包含认证材料。
    fn service_key(&self, service: &str) -> Result<String> {
        let binding = self.binding(service)?;
        Ok(format!(
            "kubernetes:{:?}",
            (
                self.server
                    .as_str()
                    .strip_suffix('/')
                    .unwrap_or(self.server.as_str()),
                &binding.namespace,
                &binding.name,
                &binding.port_name,
                binding.address_family,
            )
        ))
    }

    /// 业务作用：保留 Service 与目标对象的代次，避免地址复用使新旧端点共享公开身份。
    /// 参数说明：`instance` 来自本 provider 已验证的 TCP EndpointSlice 投影。
    /// 返回：包含 API 地址、Service UID、目标 UID、地址族、规范化地址、端口及 TCP 协议的稳定键；无目标 UID 时显式采用 Service 代次内的地址身份，不能区分该代次内无 UID 的地址复用。
    fn instance_key(&self, instance: &Instance) -> String {
        let service_uid = instance
            .metadata
            .get("nasa.kubernetes.service_uid")
            .map(String::as_str)
            .unwrap_or("");
        let target_uid = instance
            .metadata
            .get("nasa.kubernetes.target_uid")
            .filter(|uid| !uid.is_empty());
        let address = instance.ip.parse::<IpAddr>();
        let family = match address {
            Ok(IpAddr::V4(_)) => "IPv4",
            Ok(IpAddr::V6(_)) => "IPv6",
            Err(_) => "invalid",
        };
        let address = address.map_or_else(|_| instance.ip.clone(), |address| address.to_string());
        // 未提供 targetRef.uid 的合法端点不能伪造 Pod 身份；显式标记退化边界，并保留 Service UID 隔离同名重建。
        let target_kind = if target_uid.is_some() {
            "target-uid"
        } else {
            "service-scoped-address"
        };
        format!(
            "kubernetes:{:?}",
            (
                self.server
                    .as_str()
                    .strip_suffix('/')
                    .unwrap_or(self.server.as_str()),
                service_uid,
                target_kind,
                target_uid,
                family,
                address,
                instance.port,
                "TCP",
            )
        )
    }

    /// 业务作用：声明只读后端健康发现，禁止业务误以为可以注册 Kubernetes 实例。
    /// 参数说明：无。
    /// 返回：本 adapter 不提供注册能力。
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            registration: false,
            enumeration: true,
            evidence: EvidenceKind::BackendHealth,
        }
    }

    /// 业务作用：在精确服务范围验证 API 权限，不申请 Pod、Node、Secret 访问。
    /// 参数说明：无。
    /// 返回：每个绑定可完整读取；目标暂时不存在也可以准备完成。
    async fn prepare(&self) -> Result<()> {
        for service in self.config.services.keys() {
            self.observe(service).await?;
        }
        Ok(())
    }

    /// 业务作用：读取 Service、分页端点并复验 Service 代次，防止同名重建混用。
    /// 参数说明：`service` 是明确服务别名。
    /// 返回：完整候选；分页未完成、权限不足及 Service 变化均拒绝发布。
    async fn observe(&self, service: &str) -> Result<ProviderSnapshot> {
        let binding = self.binding(service)?;
        let Some(before) = self.service(binding).await? else {
            self.versions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(service);
            return Ok(ProviderSnapshot {
                instances: Vec::new(),
                evidence: EvidenceKind::BackendHealth,
            });
        };
        let (slices, version) = self.slices(binding).await?;
        let after = self.service(binding).await?.ok_or_else(|| {
            error(
                ErrorCode::InvalidSnapshot,
                "Kubernetes Service changed during observation",
            )
        })?;
        // Service 代次已经改变是明确身份撤销，不能用上一个 Service 的端点度过同步宽限。
        if before.metadata.uid != after.metadata.uid {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "Kubernetes Service identity changed during observation",
            ));
        }
        if before.metadata.resource_version != after.metadata.resource_version {
            return Err(error(
                ErrorCode::IncompleteSnapshot,
                "Kubernetes Service changed during observation",
            ));
        }
        let instances = self.project(binding, &after, slices)?;
        self.versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(service.into(), (after.metadata.resource_version, version));
        Ok(ProviderSnapshot {
            instances,
            evidence: EvidenceKind::BackendHealth,
        })
    }

    /// 业务作用：提供不占用普通请求信号量的长观察上限。
    /// 参数说明：无。
    /// 返回：watch 和完整重读分别计时。
    fn change_wait_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(
            self.config.watch_timeout_ms + self.config.request_timeout_ms,
        ))
    }

    /// 业务作用：同时观察 Service 和 EndpointSlice 时按两个实际长连接计费。
    /// 参数说明：无。
    /// 返回：两个额度；额度不足时 owner 仍可通过周期完整查询取得证据。
    fn change_wait_streams(&self) -> u32 {
        2
    }

    /// 业务作用：同时感知 Service 政策与 EndpointSlice 成员变化。
    /// 参数说明：`service` 必须与已发布完整观察使用同一绑定。
    /// 返回：任一资源变化后退出并取消另一 watch，交给 owner 全量复验。
    async fn wait_for_change(&self, service: &str) -> Result<()> {
        let binding = self.binding(service)?;
        let versions = self
            .versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(service)
            .cloned()
            .unwrap_or_default();
        tokio::select! {
            result = self.watch(binding, false, &versions.0) => result,
            result = self.watch(binding, true, &versions.1) => result,
        }
    }

    /// 业务作用：枚举本连接声明的服务别名，不扫描 namespace 中其他资源。
    /// 参数说明：无。
    /// 返回：冻结的显式绑定列表。
    async fn services(&self) -> Result<Vec<String>> {
        Ok(self.config.services.keys().cloned().collect())
    }

    /// 业务作用：关闭 API 传输并清除 watch 游标。
    /// 参数说明：无。
    /// 返回：owner 已取消在途观察，后续请求拒绝。
    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.transport.close();
        self.versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        Ok(())
    }
}

/// 业务作用：限制 namespaced 绑定中的资源与端口名称，拒绝路径语义字符。
/// 参数说明：`value` 是 Kubernetes DNS label。
/// 返回：仅接纳长度有限且首尾为字母数字的名称。
fn dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}
/// 业务作用：设置一次 API 请求的默认期限。
/// 参数说明：无。
/// 返回：三秒预算。
fn request_timeout() -> u64 {
    3000
}
/// 业务作用：设置 watch 默认最大连续等待。
/// 参数说明：无。
/// 返回：三十秒预算，不作为健康证明期限。
fn watch_timeout() -> u64 {
    30000
}
/// 业务作用：限制完整分页同步的累计字节。
/// 参数说明：无。
/// 返回：四 MiB 上限。
fn byte_limit() -> usize {
    4 * 1024 * 1024
}
/// 业务作用：限制一项服务同步期间检查的地址数。
/// 参数说明：无。
/// 返回：四千零九十六条地址。
fn instance_limit() -> usize {
    4096
}
/// 业务作用：要求 API 分页，避免单页无界增长。
/// 参数说明：无。
/// 返回：每页二百五十个 EndpointSlice。
fn page_size() -> usize {
    250
}
