//! Consul agent 注册与健康目录适配。每个注册使用 owner 专属 Service ID，健康来自完整 checks。
//! 多个 Consul 环境由公共 owner 的 qualifier 隔离；默认连接名为 primary。
//! 只写固定 agent，完整检查与权限证据决定可接流资格，不提供后端 CAS。

mod duration;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use nadisc::{
    http::{base_url, error, HeaderValue, HttpResponse, HttpTransport, Method, Url},
    managed::*,
    Instance,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsulConfig {
    pub api_addr: String,
    pub agent_addr: Option<String>,
    pub datacenter: String,
    pub token: String,
    pub ca_pem: Option<String>,
    pub services: BTreeMap<String, String>,
    pub request_timeout_ms: u64,
    pub max_response_bytes: usize,
    pub max_instances: usize,
    pub blocking_wait_ms: u64,
}

impl Default for ConsulConfig {
    /// 业务作用：提供仅包含有限预算、不猜测后端地址或凭据的配置。
    /// 参数说明：无。
    /// 返回：使用者必须声明 datacenter 和服务绑定。
    fn default() -> Self {
        Self {
            api_addr: String::new(),
            agent_addr: None,
            datacenter: String::new(),
            token: String::new(),
            ca_pem: None,
            services: BTreeMap::new(),
            request_timeout_ms: 3000,
            max_response_bytes: 4 * 1024 * 1024,
            max_instances: 4096,
            blocking_wait_ms: 10000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum HealthCheck {
    Ttl {
        ttl_ms: u64,
        report_interval_ms: u64,
    },
    Http {
        url: String,
        interval_ms: u64,
        timeout_ms: u64,
    },
}

impl HealthCheck {
    /// 业务作用：将具体健康模式写入通用计划，避免把 Nacos 属性用于 Consul。
    /// 参数说明：`plan` 为尚未冻结的注册计划。
    /// 返回：设置唯一 Consul 检查定义，不增加全局注册副作用。
    pub fn apply(&self, plan: &mut RegistrationPlan) -> Result<()> {
        let value = serde_json::to_string(self)
            .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Consul check"))?;
        plan.attributes.insert("consul.check".into(), value);
        Ok(())
    }
}

pub struct ConsulProvider {
    config: ConsulConfig,
    api: Url,
    agent: Option<Url>,
    transport: HttpTransport,
    indexes: Mutex<BTreeMap<String, u64>>,
    agent_identity: Mutex<Option<String>>,
    closed: AtomicBool,
}

/// 业务作用：把中性 metadata 键投影到 Consul 允许的字符集，保留点号及大小写语义。
/// 参数说明：`metadata` 是原始中性键值，不对业务值进行改写。
/// 返回：可逆的命名键；超过 Consul 长度约束时拒绝注册。
fn encode_metadata(metadata: &HashMap<String, String>) -> Result<HashMap<String, String>> {
    let mut encoded = HashMap::new();
    for (key, value) in metadata {
        let key = format!("nasa_meta_{}", URL_SAFE_NO_PAD.encode(key.as_bytes()));
        if key.len() > 128 || value.len() > 512 {
            return Err(error(
                ErrorCode::InvalidConfig,
                "metadata exceeds Consul key or value limit",
            ));
        }
        encoded.insert(key, value.clone());
    }
    Ok(encoded)
}

/// 业务作用：恢复本 adapter 的键投影，同时允许外部普通服务使用原生 Consul metadata。
/// 参数说明：`metadata` 为完整后端元数据。
/// 返回：原始中性键；含混重复和非法编码拒绝，不覆盖已有同名字段。
fn decode_metadata(metadata: HashMap<String, String>) -> Result<HashMap<String, String>> {
    let mut decoded = HashMap::new();
    for (key, value) in metadata {
        let key = if let Some(encoded) = key.strip_prefix("nasa_meta_") {
            String::from_utf8(URL_SAFE_NO_PAD.decode(encoded).map_err(|_| {
                error(
                    ErrorCode::IncompleteSnapshot,
                    "invalid Consul metadata key encoding",
                )
            })?)
            .map_err(|_| {
                error(
                    ErrorCode::IncompleteSnapshot,
                    "invalid Consul metadata key encoding",
                )
            })?
        } else {
            key
        };
        if decoded.insert(key, value).is_some() {
            return Err(error(ErrorCode::Conflict, "ambiguous Consul metadata key"));
        }
    }
    Ok(decoded)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Service {
    #[serde(rename = "ID")]
    id: String,
    service: String,
    address: String,
    port: u16,
    #[serde(default)]
    weights: Option<Weights>,
    #[serde(default, deserialize_with = "deserialize_metadata")]
    meta: HashMap<String, String>,
}

/// 业务作用：统一 Consul 对空 metadata 的缺席、null 与空对象表达。
/// 参数说明：`deserializer` 读取 agent 或健康目录中的 Meta 字段。
/// 返回：空值成为空映射，错误类型或非字符串值仍使整份响应解码失败。
fn deserialize_metadata<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<HashMap<String, String>>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AgentCheck {
    #[serde(rename = "CheckID")]
    id: String,
    #[serde(rename = "ServiceID")]
    service_id: String,
    service_name: String,
    #[serde(rename = "Type")]
    kind: String,
    interval: Option<String>,
    timeout: Option<String>,
    // 健康状态与检查写入权分别取证；状态不可读不能阻止仍归本 owner 的 TTL 恢复。
    #[serde(default)]
    status: Value,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Weights {
    passing: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Node {
    address: String,
    #[serde(rename = "ID")]
    id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Check {
    #[serde(rename = "CheckID")]
    id: String,
    #[serde(rename = "ServiceID")]
    service_id: String,
    status: String,
    #[serde(default, rename = "Type")]
    kind: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Entry {
    node: Node,
    service: Service,
    checks: Vec<Check>,
}

impl ConsulProvider {
    /// 业务作用：建立独立 ACL/TLS 作用域，注册 agent 与查询 API 明确分开。
    /// 参数说明：`config` 的 agent_addr 必须直连固定 agent，不能使用轮询负载均衡地址。
    /// 返回：无后台任务的 adapter；缺失绑定或不合理预算立即拒绝。
    pub fn new(config: ConsulConfig) -> Result<Self> {
        if config.datacenter.is_empty()
            || config.datacenter.len() > 128
            || config.services.is_empty()
            || config.services.len() > 1024
            || config.services.iter().any(|(alias, service)| {
                alias.is_empty() || service.is_empty() || service.len() > 512
            })
            || config.request_timeout_ms == 0
            || config.request_timeout_ms > 86_400_000
            || config.max_instances == 0
            || config.max_instances > 65536
            || config.blocking_wait_ms == 0
            || config.blocking_wait_ms > 300000
        {
            return Err(error(
                ErrorCode::InvalidConfig,
                "invalid Consul configuration",
            ));
        }
        let api = base_url(&config.api_addr)?;
        let agent = config.agent_addr.as_deref().map(base_url).transpose()?;
        let transport = HttpTransport::new(
            Duration::from_millis(config.request_timeout_ms),
            config.max_response_bytes,
            config.ca_pem.as_deref().map(str::as_bytes),
        )?;
        Ok(Self {
            config,
            api,
            agent,
            transport,
            indexes: Mutex::new(BTreeMap::new()),
            agent_identity: Mutex::new(None),
            closed: AtomicBool::new(false),
        })
    }

    /// 业务作用：构造已转义路径，防止服务名或 Service ID 改变 API 路由。
    /// 参数说明：`agent` 选择固定注册端，`segments` 是逐段路径。
    /// 返回：保持显式根地址前缀的 URL。
    fn url(&self, agent: bool, segments: &[&str]) -> Result<Url> {
        if self.closed.load(Ordering::Acquire) {
            return Err(error(ErrorCode::OwnerClosed, "Consul provider is closed"));
        }
        let mut url = if agent {
            self.agent.as_ref().ok_or_else(|| {
                error(
                    ErrorCode::Unsupported,
                    "Consul registration agent is not configured",
                )
            })?
        } else {
            &self.api
        }
        .clone();
        url.path_segments_mut()
            .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Consul URL"))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    /// 业务作用：发送只向指定 Consul 地址携带 ACL 的有限请求。
    /// 参数说明：`method`、`url`、`body` 指定操作，`admission` 保护发布与 passing。
    /// 返回：完整响应；ACL 截断不作为成功空集合。
    async fn request(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        admission: Option<&dyn WriteAdmission>,
    ) -> Result<HttpResponse> {
        let blocking = url.query_pairs().any(|(key, _)| key == "wait");
        let mut request = self.transport.request(method, url)?;
        if blocking {
            request = request.timeout(Duration::from_millis(
                self.config.blocking_wait_ms.saturating_mul(2) + self.config.request_timeout_ms,
            ));
        }
        if !self.config.token.is_empty() {
            let mut token = HeaderValue::from_str(&self.config.token)
                .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Consul token encoding"))?;
            token.set_sensitive(true);
            request = request.header("X-Consul-Token", token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = self.transport.send_admitted(request, admission).await?;
        if response
            .headers
            .get("X-Consul-Results-Filtered-By-ACLs")
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"true"))
        {
            return Err(error(
                ErrorCode::PermissionFiltered,
                "Consul response was filtered by ACL",
            ));
        }
        Ok(response)
    }

    /// 业务作用：解析显式服务绑定，禁止连接内猜测别名或跨 datacenter 回退。
    /// 参数说明：`service` 是本地别名。
    /// 返回：该别名冻结的 Consul 服务名。
    fn binding(&self, service: &str) -> Result<&str> {
        self.config
            .services
            .get(service)
            .map(String::as_str)
            .ok_or_else(|| error(ErrorCode::InvalidConfig, "unknown Consul service binding"))
    }

    /// 业务作用：获取有一致性保证的完整服务健康记录。
    /// 参数说明：`service` 是本地绑定别名。
    /// 返回：未截断的 node/service/checks 事实，不能用 catalog 条目代替。
    async fn entries(&self, service: &str) -> Result<Vec<Entry>> {
        let mut url = self.url(false, &["v1", "health", "service", self.binding(service)?])?;
        url.query_pairs_mut()
            .append_pair("dc", &self.config.datacenter)
            .append_pair("consistent", "");
        let response = self.request(Method::GET, url, None, None).await?;
        let index = response
            .headers
            .get("X-Consul-Index")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                error(
                    ErrorCode::IncompleteSnapshot,
                    "Consul query index is absent",
                )
            })?;
        self.indexes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(service.into(), index);
        let rows: Vec<Entry> = response.json()?;
        if rows.len() > self.config.max_instances {
            return Err(error(
                ErrorCode::CapacityExceeded,
                "Consul instance capacity exceeded",
            ));
        }
        Ok(rows)
    }

    /// 业务作用：要求节点和本服务的所有检查均为 passing，且确实存在服务检查。
    /// 参数说明：`entry` 为一条未过滤健康记录。
    /// 返回：无检查、维护态、warning 或 critical 都不能接流。
    fn healthy(entry: &Entry) -> bool {
        entry
            .checks
            .iter()
            .any(|check| check.service_id == entry.service.id)
            && entry.checks.iter().any(|check| check.service_id.is_empty())
            && entry.checks.iter().all(|check| check.status == "passing")
    }

    /// 业务作用：用 owner 身份隔离 Consul 服务键，防止重启后的旧注销删除新进程记录。
    /// 参数说明：`plan` 包含 owner 保留的 metadata 与本地注册名。
    /// 返回：稳定且属于本 owner 的 Service ID。
    fn service_id(plan: &RegistrationPlan) -> Result<String> {
        let owner = plan
            .instance
            .metadata
            .get("nasa.discovery.owner")
            .ok_or_else(|| error(ErrorCode::InvalidConfig, "missing registration ownership"))?;
        Ok(format!("nasa-{owner}-{}", plan.id))
    }

    /// 业务作用：解码通用注册计划中的唯一 Consul 检查定义。
    /// 参数说明：`plan` 是冻结注册。
    /// 返回：HTTP 或 TTL 检查；缺失和多余属性拒绝。
    fn check(plan: &RegistrationPlan) -> Result<HealthCheck> {
        if plan.attributes.len() != 1 {
            return Err(error(
                ErrorCode::InvalidConfig,
                "Consul requires one explicit health check",
            ));
        }
        serde_json::from_str(
            plan.attributes
                .get("consul.check")
                .ok_or_else(|| error(ErrorCode::InvalidConfig, "missing Consul health check"))?,
        )
        .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Consul health check"))
    }

    /// 业务作用：验证固定 agent 的身份和 datacenter，避免向另一 agent 更新同名检查。
    /// 参数说明：无。
    /// 返回：首次冻结身份，后续变化拒绝写入；agent-read 权限不足明确失败。
    async fn verify_agent(&self) -> Result<()> {
        let value: Value = self
            .request(
                Method::GET,
                self.url(true, &["v1", "agent", "self"])?,
                None,
                None,
            )
            .await?
            .json()?;
        let config = &value["Config"];
        let id = config["NodeID"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                error(
                    ErrorCode::IncompleteSnapshot,
                    "Consul agent identity is absent",
                )
            })?;
        if config["Datacenter"].as_str() != Some(&self.config.datacenter) {
            return Err(error(
                ErrorCode::Conflict,
                "Consul agent datacenter differs from query scope",
            ));
        }
        let mut identity = self
            .agent_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match identity.as_ref() {
            Some(previous) if previous != id => Err(error(
                ErrorCode::Conflict,
                "Consul registration agent changed",
            )),
            _ => {
                *identity = Some(id.into());
                Ok(())
            }
        }
    }

    /// 业务作用：查询本 agent 精确 Service ID 并核对所有权。
    /// 参数说明：`plan` 是本 owner 的计划。
    /// 返回：存在或缺失；不接管其他 owner 已有服务。
    async fn owned_service(&self, plan: &RegistrationPlan) -> Result<Option<Service>> {
        self.verify_agent().await?;
        let response = self
            .request(
                Method::GET,
                self.url(true, &["v1", "agent", "service", &Self::service_id(plan)?])?,
                None,
                None,
            )
            .await;
        let mut service: Service = match response {
            Ok(response) => response.json()?,
            Err(failure) if failure.code == ErrorCode::NoInstances => return Ok(None),
            Err(failure) => return Err(failure),
        };
        service.meta = decode_metadata(service.meta)?;
        if service.meta.get("nasa.discovery.owner")
            != plan.instance.metadata.get("nasa.discovery.owner")
        {
            return Err(error(
                ErrorCode::Conflict,
                "Consul service belongs to another owner",
            ));
        }
        Ok(Some(service))
    }

    /// 业务作用：核对本注册的固定服务身份与发布端点，避免把管理端改写后的记录认作自身。
    /// 参数说明：`service` 是 agent 或健康目录的在册记录，`plan` 是冻结注册计划。
    /// 返回：身份和端点全部一致才接受；不一致返回 Conflict，不覆盖远端记录。
    fn verify_registration(&self, service: &Service, plan: &RegistrationPlan) -> Result<()> {
        // owner metadata 可以被保留，仍须独立确认在册端点指向冻结计划声明的宿主。
        if service.id != Self::service_id(plan)?
            || service.service != self.binding(&plan.service)?
            || service.address != plan.instance.ip
            || service.port != plan.instance.port
        {
            return Err(error(
                ErrorCode::Conflict,
                "Consul registration identity or endpoint changed",
            ));
        }
        Ok(())
    }

    /// 业务作用：读取冻结计划要求的后端检查类型，供写入与健康证据使用同一模式约束。
    /// 参数说明：`plan` 包含冻结的健康模式。
    /// 返回：Consul 的 TTL 或 HTTP 类型标识；非法计划拒绝。
    fn check_kind(plan: &RegistrationPlan) -> Result<&'static str> {
        Ok(match Self::check(plan)? {
            HealthCheck::Ttl { .. } => "ttl",
            HealthCheck::Http { .. } => "http",
        })
    }

    /// 业务作用：核对固定 agent 的检查归属、冻结模式及可读 HTTP 时限，防止认证已改变的健康定义。
    /// 参数说明：`plan` 是冻结注册计划，`id` 是由该计划生成的自有 CheckID。
    /// 返回：归属及定义核验后的检查；缺席为 NotReady，归属、类型或 HTTP 时限改变为 Conflict，缺失或非法时限拒绝；状态留给健康取证判断。
    async fn verify_check(&self, plan: &RegistrationPlan, id: &str) -> Result<AgentCheck> {
        let mut url = self.url(true, &["v1", "agent", "checks"])?;
        let literal = serde_json::to_string(id)
            .map_err(|_| error(ErrorCode::InvalidConfig, "invalid Consul check identity"))?;
        url.query_pairs_mut()
            .append_pair("filter", &format!("CheckID == {literal}"));
        let mut checks: HashMap<String, AgentCheck> =
            self.request(Method::GET, url, None, None).await?.json()?;
        // 服务仍在册不代表自有检查仍存在，不能用原 Service ID 推断当前 CheckID 的写入权。
        let check = checks
            .remove(id)
            .ok_or_else(|| error(ErrorCode::NotReady, "Consul registration check is missing"))?;
        if !checks.is_empty()
            || check.id != id
            || check.service_id != Self::service_id(plan)?
            || check.service_name != self.binding(&plan.service)?
            || check.kind != Self::check_kind(plan)?
        {
            return Err(error(
                ErrorCode::Conflict,
                "Consul registration check ownership or type changed",
            ));
        }
        if let HealthCheck::Http {
            interval_ms,
            timeout_ms,
            ..
        } = Self::check(plan)?
        {
            let interval = duration::read(check.interval.as_deref())?;
            let timeout = duration::read(check.timeout.as_deref())?;
            // 延长检查周期会让旧 passing 长期存续；必须先核对时限才能授予新的健康期限。
            if interval != Duration::from_millis(interval_ms)
                || timeout != Duration::from_millis(timeout_ms)
            {
                return Err(error(
                    ErrorCode::Conflict,
                    "Consul HTTP check interval or timeout changed",
                ));
            }
        }
        // critical 可以仅由 TTL 到期产生，归属有效时允许恢复；该 API 不提供 TTL 时长或读写 CAS。
        Ok(check)
    }
}

#[async_trait::async_trait]
impl ManagedProvider for ConsulProvider {
    /// 业务作用：按是否配置固定 agent 声明写侧能力。
    /// 参数说明：无。
    /// 返回：只发现连接不能自注册。
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            registration: self.agent.is_some(),
            enumeration: true,
            evidence: EvidenceKind::BackendHealth,
        }
    }

    /// 业务作用：验证声明作用域内健康读取及固定注册 agent 身份。
    /// 参数说明：无。
    /// 返回：所需访问能力均成立时完成准备。
    async fn prepare(&self) -> Result<()> {
        if self.agent.is_some() {
            self.verify_agent().await?;
        }
        for service in self.config.services.keys() {
            self.entries(service).await?;
        }
        Ok(())
    }

    /// 业务作用：把完整 Consul 健康记录映射为中性实例。
    /// 参数说明：`service` 是显式服务绑定。
    /// 返回：健康属性保留完整检查结果，实例身份存入独立 provider metadata。
    async fn observe(&self, service: &str) -> Result<ProviderSnapshot> {
        let rows = self.entries(service).await?;
        let instances = rows
            .into_iter()
            .map(|row| {
                let healthy = Self::healthy(&row);
                let address = if row.service.address.is_empty() {
                    row.node.address
                } else {
                    row.service.address
                };
                Ok(Instance::new(address, row.service.port)
                    .with_healthy(healthy)
                    .with_weight(
                        row.service
                            .weights
                            .map_or(1.0, |weights| f64::from(weights.passing)),
                    )
                    .with_metadata_map(decode_metadata(row.service.meta)?)
                    .with_metadata("nasa.consul.instance_id", row.service.id)
                    .with_metadata("nasa.consul.node_id", row.node.id))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ProviderSnapshot {
            instances,
            evidence: EvidenceKind::BackendHealth,
        })
    }

    /// 业务作用：区分同一地址上的不同 Consul 节点与服务记录，保留各自检查及生命周期。
    /// 参数说明：`instance` 是本 adapter 已封闭写入 provider 标识的候选。
    /// 返回：datacenter、Node.ID 与 Service.ID 共同组成实例身份。
    fn instance_key(&self, instance: &Instance) -> String {
        match (
            instance.metadata.get("nasa.consul.node_id"),
            instance.metadata.get("nasa.consul.instance_id"),
        ) {
            (Some(node), Some(service)) if !node.is_empty() && !service.is_empty() => {
                format!("{}:{node}:{service}", self.config.datacenter)
            }
            _ => String::new(),
        }
    }

    /// 业务作用：声明 blocking query 的完整 HTTP 预算，包含服务端附加等待和传输余量。
    /// 参数说明：无。
    /// 返回：有限等待上限；不占用普通注册写额度。
    fn change_wait_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(
            self.config.blocking_wait_ms.saturating_mul(2) + self.config.request_timeout_ms,
        ))
    }

    /// 业务作用：使用 Consul query index 感知变化，索引回退后要求重新读取完整事实。
    /// 参数说明：`service` 是精确绑定服务，不扩大 ACL 作用域。
    /// 返回：变化或等待到期后通知 owner 重新观察；此响应本身不刷新健康有效期。
    async fn wait_for_change(&self, service: &str) -> Result<()> {
        let index = self
            .indexes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(service)
            .copied()
            .unwrap_or(0)
            .max(1);
        let mut url = self.url(false, &["v1", "health", "service", self.binding(service)?])?;
        url.query_pairs_mut()
            .append_pair("dc", &self.config.datacenter)
            .append_pair("consistent", "")
            .append_pair("index", &index.to_string())
            .append_pair("wait", &format!("{}ms", self.config.blocking_wait_ms));
        let response = self.request(Method::GET, url, None, None).await?;
        let next = response
            .headers
            .get("X-Consul-Index")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                error(
                    ErrorCode::IncompleteSnapshot,
                    "Consul blocking query index is absent",
                )
            })?;
        if next < index {
            self.indexes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(service.into(), 0);
        }
        Ok(())
    }

    /// 业务作用：仅枚举配置内的服务别名。
    /// 参数说明：无。
    /// 返回：不使用全 catalog 扫描。
    async fn services(&self) -> Result<Vec<String>> {
        Ok(self.config.services.keys().cloned().collect())
    }

    /// 业务作用：拒绝不能表达的 Nacos 属性及不安全检查周期。
    /// 参数说明：`plan` 是全部冻结前的候选计划。
    /// 返回：精确可表达的 HTTP/TTL 模式才能发布。
    fn validate_registration(&self, plan: &RegistrationPlan) -> Result<()> {
        self.binding(&plan.service)?;
        Self::service_id(plan)?;
        encode_metadata(&plan.instance.metadata)?;
        if self.agent.is_none()
            || plan.instance.cluster_name.is_some()
            || !plan.instance.enabled
            || !plan.instance.healthy
            || plan.instance.weight != 1.0
            || !plan.instance.ephemeral
            || plan.instance.metadata.len() > 64
        {
            return Err(error(
                ErrorCode::InvalidConfig,
                "unsupported Consul registration attributes",
            ));
        }
        match Self::check(plan)? {
            HealthCheck::Ttl {
                ttl_ms,
                report_interval_ms,
            } => {
                if report_interval_ms == 0
                    || ttl_ms > 86_400_000
                    || report_interval_ms
                        .saturating_add(self.config.request_timeout_ms)
                        .saturating_add(1000)
                        >= ttl_ms
                {
                    return Err(error(
                        ErrorCode::InvalidConfig,
                        "Consul TTL does not cover report interval and timeout",
                    ));
                }
            }
            HealthCheck::Http {
                url,
                interval_ms,
                timeout_ms,
            } => {
                base_url(&url)?;
                if timeout_ms == 0 || timeout_ms >= interval_ms || interval_ms > 86_400_000 {
                    return Err(error(
                        ErrorCode::InvalidConfig,
                        "invalid Consul HTTP check interval",
                    ));
                }
            }
        }
        Ok(())
    }

    /// 业务作用：预检明确 agent 作用域内的 Service ID 重复。
    /// 参数说明：`plan` 为已校验计划。
    /// 返回：包含 agent 地址和 owner 专属 Service ID 的注册键。
    fn registration_key(&self, plan: &RegistrationPlan) -> String {
        format!(
            "consul:{:?}:{}",
            self.agent,
            Self::service_id(plan).unwrap_or_default()
        )
    }

    /// 业务作用：识别同 datacenter 的服务查询别名，不把注册 agent 身份冒充查询隔离。
    /// 参数说明：`service` 是明确服务绑定。
    /// 返回：含 API 地址、datacenter 和服务名的安全查询键。
    fn service_key(&self, service: &str) -> Result<String> {
        Ok(format!(
            "consul:{:?}:{:?}:{:?}",
            self.api,
            self.config.datacenter,
            self.binding(service)?
        ))
    }

    /// 业务作用：创建 owner 独占服务和初始 critical 检查，不替换既有检查。
    /// 参数说明：`plan` 是精确端点和健康模式，`admission` 在所有前置读取后检查发布权。
    /// 返回：注册接纳；服务健康仍由后续回查确认。
    async fn register(
        &self,
        plan: &RegistrationPlan,
        admission: &dyn WriteAdmission,
    ) -> Result<()> {
        if self.owned_service(plan).await?.is_some() {
            return Err(error(
                ErrorCode::Conflict,
                "Consul Service ID already exists",
            ));
        }
        let id = Self::service_id(plan)?;
        let mut check = json!({"CheckID": format!("{id}-health"), "Name": "application eligibility", "Status": "critical"});
        match Self::check(plan)? {
            HealthCheck::Ttl { ttl_ms, .. } => {
                check["TTL"] = json!(format!("{ttl_ms}ms"));
            }
            HealthCheck::Http {
                url,
                interval_ms,
                timeout_ms,
            } => {
                check["HTTP"] = json!(url);
                check["Interval"] = json!(format!("{interval_ms}ms"));
                check["Timeout"] = json!(format!("{timeout_ms}ms"));
                check["DisableRedirects"] = json!(true);
                check["TLSSkipVerify"] = json!(false);
            }
        }
        let body = json!({"ID": id, "Name": self.binding(&plan.service)?, "Address": plan.instance.ip,
            "Port": plan.instance.port, "Meta": encode_metadata(&plan.instance.metadata)?, "Check": check, "EnableTagOverride": false});
        self.request(
            Method::PUT,
            self.url(true, &["v1", "agent", "service", "register"])?,
            Some(body),
            Some(admission),
        )
        .await?;
        Ok(())
    }

    /// 业务作用：只更新本 owner 的 TTL 检查，不清除服务或节点维护状态。
    /// 参数说明：`plan` 指定检查，`ready` 为本地资格，`admission` 保护 passing 提交。
    /// 返回：服务或检查缺席拒绝；身份、端点、检查归属或类型不符时返回冲突，HTTP 模式不主动上报。
    async fn renew(
        &self,
        plan: &RegistrationPlan,
        ready: bool,
        admission: &dyn WriteAdmission,
    ) -> Result<()> {
        if !matches!(Self::check(plan)?, HealthCheck::Ttl { .. }) {
            return Ok(());
        }
        // 已删除的服务不再授予检查写入权，不能凭本地计划继续延长同名检查。
        let Some(service) = self.owned_service(plan).await? else {
            return Err(error(ErrorCode::NotReady, "Consul service is missing"));
        };
        self.verify_registration(&service, plan)?;
        let id = format!("{}-health", Self::service_id(plan)?);
        self.verify_check(plan, &id).await?;
        let state = if ready {
            "pass"
        } else {
            "fail"
        };
        // 归属读取完成后仍须在提交处复验宿主资格；Consul 不保证外部改写与此更新互斥。
        self.request(
            Method::PUT,
            self.url(true, &["v1", "agent", "check", state, &id])?,
            None,
            Some(admission),
        )
        .await?;
        Ok(())
    }

    /// 业务作用：将 TTL 上报周期交给公共 owner，HTTP 检查由 agent 调度。
    /// 参数说明：`plan` 是已校验检查模式。
    /// 返回：TTL 间隔或无主动续约要求。
    fn renewal_interval(&self, plan: &RegistrationPlan) -> Option<Duration> {
        match Self::check(plan).ok()? {
            HealthCheck::Ttl {
                report_interval_ms, ..
            } => Some(Duration::from_millis(report_interval_ms)),
            _ => None,
        }
    }

    /// 业务作用：允许对原有 TTL 检查重复提交相同健康状态，不重建服务或检查。
    /// 参数说明：`plan` 保持固定 agent、服务与自有检查身份。
    /// 返回：只有 TTL 模式允许协调续约；HTTP 检查继续由 agent 执行。
    fn supports_idempotent_renewal(&self, plan: &RegistrationPlan) -> bool {
        matches!(Self::check(plan), Ok(HealthCheck::Ttl { .. }))
    }

    /// 业务作用：按固定 agent 节点、Service ID 与 owner 选择自注册证据，不借用其他节点的同名身份。
    /// 参数说明：`plan` 定位本 owner 的服务名、冻结端点和健康模式。
    /// 返回：目录或自有检查缺席、agent 明确负面状态表示不健康；未知或非法状态不能提供健康证据；身份、端点、检查类型或 HTTP 时限不符返回冲突，agent 缺失与目录维护状态分别报告。
    async fn inspect(&self, plan: &RegistrationPlan) -> Result<RegistrationEvidence> {
        let Some(owned) = self.owned_service(plan).await? else {
            return Ok(RegistrationEvidence::Missing);
        };
        let id = Self::service_id(plan)?;
        let service = self.binding(&plan.service)?;
        self.verify_registration(&owned, plan)?;
        let own_check = format!("{id}-health");
        // passing 不能替代冻结定义：agent 已改类型或 HTTP 时限时，目录旧健康不得重新开放注册就绪。
        let check = match self.verify_check(plan, &own_check).await {
            Err(failure) if failure.code == ErrorCode::NotReady => {
                return Ok(RegistrationEvidence::Unhealthy);
            }
            result => result?,
        };
        // 本地检查已否定健康时立即撤销证据，不能让目录旧 passing 或后续查询失败的宽限掩盖负面事实。
        match check.status.as_str() {
            Some("passing") => {}
            Some("critical" | "warning") => return Ok(RegistrationEvidence::Unhealthy),
            _ => {
                return Err(error(
                    ErrorCode::IncompleteSnapshot,
                    "Consul registration agent check status is invalid",
                ));
            }
        }
        let node = self
            .agent_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                error(
                    ErrorCode::Conflict,
                    "Consul registration agent is not identified",
                )
            })?;
        let rows = self.entries(&plan.service).await?;
        // Service ID 只在节点内唯一；本节点暂未进入目录时，其他节点的同 ID 不能满足启动或就绪。
        let mut candidates = rows
            .into_iter()
            .filter(|row| row.node.id == node && row.service.id == id);
        let Some(mut row) = candidates.next() else {
            return Ok(RegistrationEvidence::Unhealthy);
        };
        if candidates.next().is_some() || row.service.service != service {
            return Err(error(
                ErrorCode::Conflict,
                "Consul registration directory identity is ambiguous",
            ));
        }
        // agent 与目录存在传播时差，必须分别核对端点，不能借任一侧的旧事实维持自身就绪。
        self.verify_registration(&row.service, plan)?;
        let metadata = decode_metadata(std::mem::take(&mut row.service.meta)).map_err(|_| {
            error(
                ErrorCode::Conflict,
                "Consul registration directory metadata is invalid",
            )
        })?;
        if metadata.get("nasa.discovery.owner")
            != plan.instance.metadata.get("nasa.discovery.owner")
        {
            // agent 的所有权不能替代目录记录的所有权，传播中的旧记录不得被认证成当前注册。
            return Err(error(
                ErrorCode::Conflict,
                "Consul registration directory owner differs",
            ));
        }
        let kind = Self::check_kind(plan)?;
        let mut checks = std::collections::HashSet::new();
        for check in &row.checks {
            if !checks.insert(&check.id)
                || (!check.service_id.is_empty() && check.service_id != id)
                || (check.id == own_check && (check.service_id != id || check.kind != kind))
            {
                // 目录也须确认自有检查模式，不能用传播中的另一类型或重复检查证明冻结计划健康。
                return Err(error(
                    ErrorCode::Conflict,
                    "Consul registration check identity or type differs",
                ));
            }
        }
        if row.checks.iter().any(|check| {
            check.id.starts_with("_service_maintenance:") || check.id == "_node_maintenance"
        }) {
            return Ok(RegistrationEvidence::Disabled);
        }
        Ok(
            if row
                .checks
                .iter()
                .any(|check| check.id == own_check && check.service_id == id)
                && Self::healthy(&row)
            {
                RegistrationEvidence::Healthy
            } else {
                RegistrationEvidence::Unhealthy
            },
        )
    }

    /// 业务作用：从原 agent 删除本 owner 的服务及其附属检查。
    /// 参数说明：`plan` 保持初次注册的 Service ID，不能改用 catalog 全局删除。
    /// 返回：缺失或删除成功；服务上附着的检查随该实例一起删除。
    async fn deregister(&self, plan: &RegistrationPlan) -> Result<()> {
        if self.owned_service(plan).await?.is_none() {
            return Ok(());
        }
        self.request(
            Method::PUT,
            self.url(
                true,
                &[
                    "v1",
                    "agent",
                    "service",
                    "deregister",
                    &Self::service_id(plan)?,
                ],
            )?,
            None,
            None,
        )
        .await?;
        Ok(())
    }

    /// 业务作用：关闭当前 ACL/TLS 连接池，不留下自行运行的 TTL 任务。
    /// 参数说明：无。
    /// 返回：之后的新操作均被拒绝。
    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.transport.close();
        Ok(())
    }
}
