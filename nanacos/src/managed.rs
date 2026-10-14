//! Nacos HTTP 受管命名适配；发现使用 client API，注册所有权使用服务范围的 admin API 证据。

use nadisc::{
    http::{base_url, error, HttpTransport, Method, Url},
    managed::*,
    Instance,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{sync::Mutex, time::Instant};

/// Nacos 受管发现连接的作用域、凭据和有限读取预算。
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ManagedNacosConfig {
    /// 显式 Nacos 服务端地址，可按受支持的分隔方式声明多个地址。
    pub server_addr: String,
    /// 目标所在的命名空间。
    pub namespace: String,
    /// 目标服务或配置所属的分组。
    pub group: String,
    /// Nacos 登录用户名，不写入诊断。
    pub username: String,
    /// Nacos 登录密码，不写入诊断。
    pub password: String,
    /// 按逻辑服务名绑定的目标，不接受重复键。
    pub services: BTreeMap<String, NacosService>,
    /// 一次请求的最大耗时，单位为毫秒。
    pub request_timeout_ms: u64,
    /// 单次后端响应允许占用的最大字节数。
    pub max_response_bytes: usize,
    /// 单次完整服务观察允许包含的实例数量上限。
    pub max_instances: usize,
    /// HTTPS 证书验证使用的附加 CA PEM；不关闭系统信任校验。
    pub ca_pem: Option<String>,
}

/// 逻辑服务到 Nacos 服务名及可选集群的显式绑定。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NacosService {
    /// 目标后端中的服务名称。
    pub name: String,
    /// 可选 Nacos 集群范围；省略时不按集群筛选。
    #[serde(default)]
    pub cluster: Option<String>,
}

impl Default for ManagedNacosConfig {
    /// 业务作用：提供不包含任何凭据或隐式服务枚举的连接基线。
    /// 参数说明：无。
    /// 返回：服务端地址和服务绑定需要显式配置。
    fn default() -> Self {
        Self {
            server_addr: String::new(),
            namespace: String::new(),
            group: "DEFAULT_GROUP".into(),
            username: String::new(),
            password: String::new(),
            services: BTreeMap::new(),
            request_timeout_ms: 3000,
            max_response_bytes: 4 * 1024 * 1024,
            max_instances: 4096,
            ca_pem: None,
        }
    }
}

struct Token {
    value: String,
    expires: Instant,
}

/// 受管 Nacos HTTP 适配器，隔离连接身份并验证注册所有权。
pub struct ManagedNacos {
    config: ManagedNacosConfig,
    servers: Vec<Url>,
    transport: HttpTransport,
    cursor: AtomicUsize,
    token: Mutex<Option<Token>>,
}

#[derive(Deserialize)]
struct Envelope<T> {
    code: i64,
    data: Option<T>,
}

#[derive(Deserialize)]
struct Login {
    #[serde(rename = "accessToken")]
    token: String,
    #[serde(rename = "tokenTtl")]
    ttl: u64,
}

#[derive(Deserialize)]
struct RemoteInstance {
    ip: String,
    port: u16,
    weight: f64,
    healthy: bool,
    enabled: bool,
    ephemeral: bool,
    #[serde(rename = "clusterName")]
    cluster: String,
    metadata: HashMap<String, String>,
}

impl RemoteInstance {
    /// 业务作用：保留后端实例的管理属性和所有权，转换为中性发现记录。
    /// 参数说明：无。
    /// 返回：健康、启用、持久性、cluster 和 metadata 均来自同一远端记录。
    fn into_instance(self) -> Instance {
        Instance::new(self.ip, self.port)
            .with_weight(self.weight)
            .with_healthy(self.healthy)
            .with_enabled(self.enabled)
            .with_ephemeral(self.ephemeral)
            .with_cluster(self.cluster)
            .with_metadata_map(self.metadata)
    }
}

impl ManagedNacos {
    /// 业务作用：校验并创建独立命名连接，凭据、namespace 和 group 不与其他连接共享。
    /// 参数说明：`config` 使用 Nacos HTTP API，server_addr 多项必须属于同一集群；空 namespace 等同 public。
    /// 返回：没有后台任务或网络副作用的 adapter；不支持的配置在构造时拒绝。
    pub fn new(mut config: ManagedNacosConfig) -> Result<Self> {
        // 后端将空 namespace 和 public 指向同一空间，必须在生成请求与身份键之前统一，避免重复监督同一实例。
        if config.namespace.is_empty() {
            config.namespace = "public".into();
        }
        if config.request_timeout_ms == 0
            || config.request_timeout_ms > 86_400_000
            || config.max_instances == 0
            || config.max_instances > 65536
            || config.group.is_empty()
            || config.group.len() > 128
            || config.username.is_empty() != config.password.is_empty()
            || config.services.is_empty()
            || config.services.len() > 1024
        {
            return Err(error(
                ErrorCode::InvalidConfig,
                "invalid managed Nacos configuration",
            ));
        }
        for (alias, binding) in &config.services {
            if alias.is_empty()
                || alias.len() > 512
                || binding.name.is_empty()
                || binding.name.len() > 512
                || binding
                    .cluster
                    .as_ref()
                    .is_some_and(|value| value.is_empty() || value.contains(','))
            {
                return Err(error(
                    ErrorCode::InvalidConfig,
                    "invalid Nacos service binding",
                ));
            }
        }
        let mut servers: Vec<_> = config
            .server_addr
            .split(',')
            .map(|address| {
                let mut url = base_url(address.trim())?;
                if !url.path().ends_with('/') {
                    url.set_path(&format!("{}/", url.path()));
                }
                Ok(url)
            })
            .collect::<Result<_>>()?;
        if servers.is_empty() || servers.len() > 16 {
            return Err(error(ErrorCode::InvalidConfig, "invalid Nacos server list"));
        }
        // 服务器顺序和重复项不改变注册作用域，请求轮换与两类身份必须共用同一规范化集合。
        servers.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        servers.dedup();
        let transport = HttpTransport::new(
            Duration::from_millis(config.request_timeout_ms),
            config.max_response_bytes,
            config.ca_pem.as_deref().map(str::as_bytes),
        )?;
        Ok(Self {
            config,
            servers,
            transport,
            cursor: AtomicUsize::new(0),
            token: Mutex::new(None),
        })
    }

    /// 业务作用：选择同集群下一个控制面地址，不把不同业务环境混作高可用节点。
    /// 参数说明：`path` 为相对于显式 Nacos 根路径的 API 路径。
    /// 返回：使用同一 namespace/group 的请求 URL。
    fn url(&self, path: &str) -> Result<Url> {
        let index = self.cursor.fetch_add(1, Ordering::Relaxed) % self.servers.len();
        self.servers[index]
            .join(path)
            .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Nacos API path"))
    }

    /// 业务作用：按需取得本连接认证信息，避免另起脱离 owner 的定时刷新。
    /// 参数说明：无。
    /// 返回：有效 token 或认证错误；缓存命中不产生新的服务健康证据。
    async fn token(&self) -> Result<Option<String>> {
        if self.config.username.is_empty() {
            return Ok(None);
        }
        let mut token = self.token.lock().await;
        if let Some(token) = token
            .as_ref()
            .filter(|token| token.expires > Instant::now())
        {
            return Ok(Some(token.value.clone()));
        }
        let request = self
            .transport
            .request(Method::POST, self.url("v3/auth/user/login")?)?
            .form(&[
                ("username", &self.config.username),
                ("password", &self.config.password),
            ]);
        let response = self.transport.send(request).await.map_err(|failure| {
            if matches!(
                failure.code,
                ErrorCode::PermissionDenied | ErrorCode::Unauthenticated
            ) {
                error(
                    ErrorCode::Unauthenticated,
                    "Nacos authentication was rejected",
                )
            } else {
                failure
            }
        })?;
        let login: Login = response.json()?;
        if login.token.is_empty()
            || login.token.len() > 16384
            || login.ttl < 2
            || login.ttl > 31_536_000
        {
            return Err(error(
                ErrorCode::IncompleteSnapshot,
                "invalid Nacos token response",
            ));
        }
        let value = login.token.clone();
        *token = Some(Token {
            value: login.token,
            expires: Instant::now() + Duration::from_secs(login.ttl * 4 / 5),
        });
        Ok(Some(value))
    }

    /// 业务作用：发送一次有作用域的 Nacos 操作，并对已失效 token 撤销本地缓存。
    /// 参数说明：`method`、`path`、`params` 是确定的 API 请求，`admission` 只用于注册及续约写入。
    /// 返回：经字节限额检查的 data；错误响应不向上层暴露正文。
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Vec<(&str, String)>,
        admission: Option<&dyn WriteAdmission>,
    ) -> Result<T> {
        let token = self.token().await?;
        let mut request = self.transport.request(method.clone(), self.url(path)?)?;
        if let Some(token) = token {
            let mut header = nadisc::http::HeaderValue::from_str(&token)
                .map_err(|_| error(ErrorCode::Unauthenticated, "invalid Nacos token encoding"))?;
            header.set_sensitive(true);
            request = request.header("accessToken", header);
        }
        request = if method == Method::POST {
            request.form(&params)
        } else {
            request.query(&params)
        };
        let response = match self.transport.send_admitted(request, admission).await {
            Ok(response) => response,
            Err(failure) => {
                if matches!(
                    failure.code,
                    ErrorCode::Unauthenticated | ErrorCode::PermissionDenied
                ) {
                    self.token.lock().await.take();
                }
                return Err(failure);
            }
        };
        let envelope: Envelope<T> = response.json()?;
        if envelope.code != 0 {
            let code = match envelope.code {
                401 => ErrorCode::Unauthenticated,
                403 => ErrorCode::PermissionDenied,
                21003 => ErrorCode::NotReady,
                429 => ErrorCode::RateLimited,
                _ => ErrorCode::Unavailable,
            };
            if matches!(
                code,
                ErrorCode::Unauthenticated | ErrorCode::PermissionDenied
            ) {
                self.token.lock().await.take();
            }
            return Err(error(code, "Nacos operation was rejected"));
        }
        envelope
            .data
            .ok_or_else(|| error(ErrorCode::IncompleteSnapshot, "Nacos response has no data"))
    }

    /// 业务作用：将本地服务别名映射到固定后端名和 cluster 条件。
    /// 参数说明：`service` 是连接中明确声明的服务别名。
    /// 返回：不可跨 namespace/group 的服务绑定。
    fn binding(&self, service: &str) -> Result<&NacosService> {
        self.config
            .services
            .get(service)
            .ok_or_else(|| error(ErrorCode::InvalidConfig, "unknown Nacos service binding"))
    }

    /// 业务作用：建立当前连接的公共查询作用域。
    /// 参数说明：`service` 是已确认的后端服务名。
    /// 返回：namespace、group、service 三个明确参数。
    fn scope(&self, service: &str) -> Vec<(&'static str, String)> {
        vec![
            ("namespaceId", self.config.namespace.clone()),
            ("groupName", self.config.group.clone()),
            ("serviceName", service.into()),
        ]
    }

    /// 业务作用：按绑定服务读取流量发现实例，不将隐藏的禁用实例解释为注册缺失。
    /// 参数说明：`service` 是本地绑定别名。
    /// 返回：client API 确认的流量集合；权限、协议与容量错误均拒绝，空集合不提供写侧所有权证据。
    async fn instances(&self, service: &str) -> Result<Vec<Instance>> {
        let binding = self.binding(service)?;
        let mut params = self.scope(&binding.name);
        if let Some(cluster) = &binding.cluster {
            params.push(("clusterName", cluster.clone()));
        }
        params.push(("healthyOnly", "false".into()));
        let rows: Vec<RemoteInstance> = self
            .call(Method::GET, "v3/client/ns/instance/list", params, None)
            .await?;
        if rows.len() > self.config.max_instances {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "Nacos instance limit exceeded",
            ));
        }
        Ok(rows
            .into_iter()
            .map(RemoteInstance::into_instance)
            .collect())
    }

    /// 业务作用：读取服务范围内的完整管理证据，保留供上层复验的特定缺失状态。
    /// 参数说明：`path` 与 `params` 限定已绑定服务，`service_detail` 指定服务详情的 400 协议入口。
    /// 返回：HTTP 状态和有界协议响应；鉴权失效撤销 token，权限或传输失败不能作为空证据。
    async fn admin_evidence(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
        service_detail: bool,
    ) -> Result<(u16, Envelope<serde_json::Value>)> {
        let token = self.token().await?;
        let mut request = self
            .transport
            .request(Method::GET, self.url(path)?)?
            .query(&params);
        if let Some(token) = token {
            let mut header = nadisc::http::HeaderValue::from_str(&token)
                .map_err(|_| error(ErrorCode::Unauthenticated, "invalid Nacos token encoding"))?;
            header.set_sensitive(true);
            request = request.header("accessToken", header);
        }
        let result = if service_detail {
            self.transport.send_allow_bad_request(request).await
        } else {
            self.transport.send_allow_not_found(request).await
        };
        if result.as_ref().err().is_some_and(|failure| {
            matches!(
                failure.code,
                ErrorCode::Unauthenticated | ErrorCode::PermissionDenied
            )
        }) {
            self.token.lock().await.take();
        }
        let (status, response) = result?;
        let envelope: Envelope<serde_json::Value> = response.json()?;
        if envelope.code == 401 || envelope.code == 403 {
            self.token.lock().await.take();
            return Err(error(
                if envelope.code == 401 {
                    ErrorCode::Unauthenticated
                } else {
                    ErrorCode::PermissionDenied
                },
                "Nacos instance evidence was denied",
            ));
        }
        Ok((status, envelope))
    }

    /// 业务作用：定位原 cluster 的精确注册记录，并拒绝清理或续约其他进程的实例。
    /// 参数说明：`plan` 包含 owner 写入的所有权 metadata。
    /// 返回：完整管理属性的自有实例或协议明确缺失；端点、实例类型或所有权不符为冲突，取证失败原样返回。
    async fn owned_instance(&self, plan: &RegistrationPlan) -> Result<Option<Instance>> {
        // client 列表会隐藏禁用实例；写侧必须读取管理事实，取证失败不能放行写入或宣称已注销。
        let (status, envelope) = self
            .admin_evidence("v3/admin/ns/instance", self.instance_params(plan)?, false)
            .await?;
        // 精确 cluster 缺席只证明该记录不存在，首次注册仍须核对其他 cluster 的同端点写入冲突。
        if status == 404 && envelope.code == 20004 {
            return Ok(None);
        }
        if status == 404 || envelope.code != 0 {
            return Err(error(
                ErrorCode::IncompleteSnapshot,
                "Nacos instance evidence is not authoritative",
            ));
        }
        let row: RemoteInstance = serde_json::from_value(envelope.data.ok_or_else(|| {
            error(
                ErrorCode::IncompleteSnapshot,
                "Nacos instance evidence is missing",
            )
        })?)
        .map_err(|_| {
            error(
                ErrorCode::IncompleteSnapshot,
                "invalid Nacos instance evidence",
            )
        })?;
        let row = row.into_instance();
        // 精确查询仍须复验返回身份，防止端点或持久性变化使心跳与删除作用于另一条记录。
        if row.ip != plan.instance.ip
            || row.port != plan.instance.port
            || row.cluster_name.as_deref()
                != Some(plan.instance.cluster_name.as_deref().unwrap_or("DEFAULT"))
            || row.ephemeral != plan.instance.ephemeral
            || row.metadata.get("nasa.discovery.owner")
                != plan.instance.metadata.get("nasa.discovery.owner")
        {
            return Err(error(
                ErrorCode::Conflict,
                "Nacos registration identity or ownership differs",
            ));
        }
        Ok(Some(row))
    }

    /// 业务作用：首次发布前核验 HTTP 写入可能覆盖的全部 cluster，防止同服务端点被另一 owner 接管。
    /// 参数说明：`plan` 指定绑定服务和拟发布端点。
    /// 返回：完整管理列表中端点不存在，或服务被协议明确确认为缺失时通过；冲突、权限、畸形和超限均拒绝。
    async fn ensure_registration_vacant(&self, plan: &RegistrationPlan) -> Result<()> {
        let scope = self.scope(&self.binding(&plan.service)?.name);
        let mut params = scope.clone();
        // HTTP client 的写入身份不包含 cluster；不能带发现过滤，也不能隐藏禁用或不健康的已有记录。
        params.push(("healthyOnly", "false".into()));
        let (status, envelope) = self
            .admin_evidence("v3/admin/ns/instance/list", params, false)
            .await?;
        if status == 404 && envelope.code == 30000 {
            // 列表的通用错误码不能证明空服务；只有服务详情的专用缺失码才允许首次创建。
            let (status, service) = self
                .admin_evidence("v3/admin/ns/service", scope, true)
                .await?;
            return if status == 400 && service.code == 21008 {
                Ok(())
            } else {
                Err(error(
                    ErrorCode::IncompleteSnapshot,
                    "Nacos service absence is not authoritative",
                ))
            };
        }
        if status != 200 || envelope.code != 0 {
            return Err(error(
                ErrorCode::IncompleteSnapshot,
                "Nacos registration scope is not authoritative",
            ));
        }
        let rows: Vec<RemoteInstance> = serde_json::from_value(envelope.data.ok_or_else(|| {
            error(
                ErrorCode::IncompleteSnapshot,
                "Nacos registration scope is missing",
            )
        })?)
        .map_err(|_| {
            error(
                ErrorCode::IncompleteSnapshot,
                "invalid Nacos registration scope",
            )
        })?;
        if rows.len() > self.config.max_instances {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "Nacos instance limit exceeded",
            ));
        }
        // 即使 owner 相同也不能重放首次注册覆盖管理属性；实例类型差异不授予接管权限。
        if rows
            .iter()
            .any(|row| row.ip == plan.instance.ip && row.port == plan.instance.port)
        {
            return Err(error(
                ErrorCode::Conflict,
                "Nacos registration endpoint already exists",
            ));
        }
        Ok(())
    }

    /// 业务作用：生成只包含精确注册键的操作参数。
    /// 参数说明：`plan` 是冻结的最终注册计划。
    /// 返回：不携带旧管理属性的注册键参数。
    fn instance_params(&self, plan: &RegistrationPlan) -> Result<Vec<(&'static str, String)>> {
        let mut params = self.scope(&self.binding(&plan.service)?.name);
        params.extend([
            ("ip", plan.instance.ip.clone()),
            ("port", plan.instance.port.to_string()),
            ("ephemeral", plan.instance.ephemeral.to_string()),
            (
                "clusterName",
                plan.instance
                    .cluster_name
                    .clone()
                    .unwrap_or_else(|| "DEFAULT".into()),
            ),
        ]);
        Ok(params)
    }
}

#[async_trait::async_trait]
impl ManagedProvider for ManagedNacos {
    /// 业务作用：声明 Nacos 命名读写与后端健康证据能力。
    /// 参数说明：无。
    /// 返回：服务枚举仅限显式绑定。
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            registration: true,
            enumeration: true,
            evidence: EvidenceKind::BackendHealth,
        }
    }

    /// 业务作用：用真实服务作用域查询验证认证及访问能力。
    /// 参数说明：无。
    /// 返回：空服务可以成功；不申请全局服务目录权限。
    async fn prepare(&self) -> Result<()> {
        for alias in self.config.services.keys() {
            self.instances(alias).await?;
        }
        Ok(())
    }

    /// 业务作用：取得同一服务最新完整后端证据。
    /// 参数说明：`service` 是显式服务绑定。
    /// 返回：有实例、空实例和失败分别表达。
    async fn observe(&self, service: &str) -> Result<ProviderSnapshot> {
        Ok(ProviderSnapshot {
            instances: self.instances(service).await?,
            evidence: EvidenceKind::BackendHealth,
        })
    }

    /// 业务作用：枚举本连接允许使用的服务别名。
    /// 参数说明：无。
    /// 返回：配置绑定列表，不执行管理 API 全目录扫描。
    async fn services(&self) -> Result<Vec<String>> {
        Ok(self.config.services.keys().cloned().collect())
    }

    /// 业务作用：阻止未绑定服务、过滤范围外 cluster 及不支持的写侧设置。
    /// 参数说明：`plan` 是最终注册事实。
    /// 返回：本 adapter 可以完整表达该计划时通过。
    fn validate_registration(&self, plan: &RegistrationPlan) -> Result<()> {
        let binding = self.binding(&plan.service)?;
        if !plan.attributes.is_empty()
            || !plan.instance.metadata.contains_key("nasa.discovery.owner")
            || binding.cluster.as_ref().is_some_and(|cluster| {
                cluster != plan.instance.cluster_name.as_deref().unwrap_or("DEFAULT")
            })
            || plan.instance.metadata.len() > 128
            || plan
                .instance
                .metadata
                .iter()
                .any(|(key, value)| key.len() + value.len() > 4096)
        {
            return Err(error(
                ErrorCode::InvalidConfig,
                "unsupported Nacos registration attributes",
            ));
        }
        Ok(())
    }

    /// 业务作用：预检不同连接别名是否指向同一个真实注册键。
    /// 参数说明：`plan` 为已校验注册。
    /// 返回：按规范化服务器集合、namespace、group、service 和端点组成的写入身份；cluster 不构成独立写入范围。
    fn registration_key(&self, plan: &RegistrationPlan) -> String {
        let servers: Vec<_> = self.servers.iter().map(Url::as_str).collect();
        // 字符串字段独立转义并保留边界，合法名称中的分隔符不能合并不同的外部注册作用域。
        format!(
            "nacos:{:?}",
            (
                servers,
                &self.config.namespace,
                &self.config.group,
                &self.config.services[&plan.service].name,
                &plan.instance.ip,
                plan.instance.port,
            )
        )
    }

    /// 业务作用：识别指向同一 Nacos 查询集合的别名，避免同一进程发布相互矛盾的协议端点。
    /// 参数说明：`service` 是明确服务绑定。
    /// 返回：含服务器、namespace、group、服务和 cluster 过滤的安全查询键。
    fn service_key(&self, service: &str) -> Result<String> {
        let binding = self.binding(service)?;
        let servers: Vec<_> = self.servers.iter().map(Url::as_str).collect();
        Ok(format!(
            "nacos:{:?}",
            (
                servers,
                &self.config.namespace,
                &self.config.group,
                &binding.name,
                &binding.cluster
            )
        ))
    }

    /// 业务作用：仅发布无冲突的新实例，不重放旧管理属性覆盖已有记录。
    /// 参数说明：`plan` 是冻结注册，`admission` 在鉴权及核对后复验发布权。
    /// 返回：明确接纳或失败；传输结果不明由 owner 保留。
    async fn register(
        &self,
        plan: &RegistrationPlan,
        admission: &dyn WriteAdmission,
    ) -> Result<()> {
        if self.owned_instance(plan).await?.is_some() {
            return Err(error(
                ErrorCode::Conflict,
                "Nacos registration key already exists",
            ));
        }
        self.ensure_registration_vacant(plan).await?;
        let mut params = self.instance_params(plan)?;
        params.extend([
            ("healthy", plan.instance.healthy.to_string()),
            ("enabled", plan.instance.enabled.to_string()),
            ("weight", plan.instance.weight.to_string()),
            (
                "metadata",
                serde_json::to_string(&plan.instance.metadata)
                    .map_err(|_| error(ErrorCode::InvalidConfig, "invalid instance metadata"))?,
            ),
        ]);
        let _: String = self
            .call(
                Method::POST,
                "v3/client/ns/instance",
                params,
                Some(admission),
            )
            .await?;
        Ok(())
    }

    /// 业务作用：续约既有临时实例，使用 heartBeat 避免覆盖 enabled、weight 和 metadata。
    /// 参数说明：`plan` 为已有实例，`ready` 为本地资格，`admission` 保护实际心跳提交。
    /// 返回：缺失、禁用或零权重时拒绝续约，不自动重新注册。
    async fn renew(
        &self,
        plan: &RegistrationPlan,
        ready: bool,
        admission: &dyn WriteAdmission,
    ) -> Result<()> {
        let row = self
            .owned_instance(plan)
            .await?
            .ok_or_else(|| error(ErrorCode::NotReady, "Nacos registration is missing"))?;
        if !ready || !row.enabled || row.weight <= 0.0 {
            return Err(error(
                ErrorCode::NotReady,
                "Nacos registration is not eligible",
            ));
        }
        if !plan.instance.ephemeral {
            return Ok(());
        }
        let mut params = self.instance_params(plan)?;
        params.push(("heartBeat", "true".into()));
        let _: String = self
            .call(
                Method::POST,
                "v3/client/ns/instance",
                params,
                Some(admission),
            )
            .await?;
        Ok(())
    }

    /// 业务作用：为临时实例请求受管保活，持久实例不冒充心跳所有权。
    /// 参数说明：`plan` 决定实例类型。
    /// 返回：临时实例每五秒续约，持久实例由服务端健康机制管理。
    fn renewal_interval(&self, plan: &RegistrationPlan) -> Option<Duration> {
        plan.instance.ephemeral.then(|| Duration::from_secs(5))
    }

    /// 业务作用：允许原临时实例重复发送相同心跳，恢复短暂响应丢失后的保活。
    /// 参数说明：`plan` 必须保持原实例身份与临时实例属性。
    /// 返回：临时实例允许协调续约；每次仍复验所有权和管理资格，不覆盖注册属性。
    fn supports_idempotent_renewal(&self, plan: &RegistrationPlan) -> bool {
        plan.instance.ephemeral
    }

    /// 业务作用：区分不存在、不健康与管理端摘流。
    /// 参数说明：`plan` 指向精确实例所有权。
    /// 返回：完整状态事实，查询错误不伪装成缺失。
    async fn inspect(&self, plan: &RegistrationPlan) -> Result<RegistrationEvidence> {
        Ok(match self.owned_instance(plan).await? {
            None => RegistrationEvidence::Missing,
            Some(row) if !row.enabled || row.weight <= 0.0 => RegistrationEvidence::Disabled,
            Some(row) if row.healthy => RegistrationEvidence::Healthy,
            Some(_) => RegistrationEvidence::Unhealthy,
        })
    }

    /// 业务作用：核对当前所有权后删除精确实例，不清理其他 owner 的记录。
    /// 参数说明：`plan` 保留原注册身份。
    /// 返回：已缺失或删除成功；Nacos 无条件删除 API 不提供跨进程 CAS 保证。
    async fn deregister(&self, plan: &RegistrationPlan) -> Result<()> {
        if self.owned_instance(plan).await?.is_none() {
            return Ok(());
        }
        let _: String = self
            .call(
                Method::DELETE,
                "v3/client/ns/instance",
                self.instance_params(plan)?,
                None,
            )
            .await?;
        Ok(())
    }

    /// 业务作用：释放本连接 HTTP 池和 token，不留下独立续约任务。
    /// 参数说明：无。
    /// 返回：后续请求被传输层拒绝；在途操作已由 owner 取消。
    async fn close(&self) -> Result<()> {
        self.transport.close();
        self.token.lock().await.take();
        Ok(())
    }
}
