//! 多后端类型化装配。输入已经完成占位符解析，公共组件不依赖任何应用配置框架。

use crate::{
    config::{invalid, validate_name, ConnectionPolicy, Limits, RegistrationPolicy},
    providers::LocalProvider,
    ConnectionSpec, DiscoveryOwner, Eligibility, OwnerBuilder, RegistrationSpec,
};
use nadisc::{
    managed::{ManagedProvider, RegistrationPlan, Result},
    DnsService, Instance,
};
use serde::{
    de::{Error, MapAccess, Visitor},
    Deserialize, Serialize,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub enabled: bool,
    pub eligibility_probe: bool,
    /// 兼容显式默认引用；省略时只采用名为 primary 的连接，不按数量或顺序推断。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_connection: Option<String>,
    pub limits: Limits,
    #[serde(deserialize_with = "unique_map")]
    pub connections: BTreeMap<String, ConnectionConfig>,
    #[serde(deserialize_with = "unique_map")]
    pub registrations: BTreeMap<String, RegistrationConfig>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectionConfig {
    pub provider: String,
    pub startup_required: bool,
    pub request_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub idle_timeout_ms: u64,
    pub snapshot: SnapshotPolicy,
    pub retry: RetryPolicy,
    #[serde(deserialize_with = "optional_unique_value")]
    pub nacos: Option<serde_json::Value>,
    #[serde(deserialize_with = "optional_unique_value")]
    pub consul: Option<serde_json::Value>,
    #[serde(deserialize_with = "optional_unique_value")]
    pub kubernetes: Option<serde_json::Value>,
    #[serde(rename = "static")]
    pub static_config: Option<StaticConfig>,
    pub dns: Option<DnsConfig>,
    pub rest: Option<RestPolicy>,
}

/// 宿主 REST 桥的独立调用策略；共享发现时效仅由 snapshot 策略裁决。
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RestPolicy {
    pub request_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub lb_strategy: String,
    pub instance_scheme: String,
    pub retry_get_head_on_transport_error: bool,
    pub retry_get_head_on_retryable_status: bool,
    pub max_attempts: usize,
}
impl Default for RestPolicy {
    /// 业务作用：给各连接使用独立有限的 HTTP 调用预算，默认不重试。
    /// 参数说明：无。
    /// 返回：十秒总预算、两秒连接预算和轮询分配策略。
    fn default() -> Self {
        Self {
            request_timeout_ms: 10000,
            connect_timeout_ms: 2000,
            lb_strategy: "round_robin".into(),
            instance_scheme: "http".into(),
            retry_get_head_on_transport_error: false,
            retry_get_head_on_retryable_status: false,
            max_attempts: 1,
        }
    }
}
impl RestPolicy {
    /// 业务作用：在连接准备前拒绝非法调用时长、协议、分配策略和重试次数。
    /// 参数说明：无。
    /// 返回：策略有效时成功；不开放任意 HTTP 方法的自动重试。
    pub fn validate(&self) -> Result<()> {
        if self.request_timeout_ms == 0
            || self.request_timeout_ms > 86_400_000
            || self.connect_timeout_ms == 0
            || self.connect_timeout_ms > self.request_timeout_ms
            || !matches!(self.lb_strategy.as_str(), "round_robin" | "weighted")
            || !matches!(self.instance_scheme.as_str(), "http" | "https")
            || !(1..=16).contains(&self.max_attempts)
        {
            return Err(invalid("invalid per-connection REST policy"));
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SnapshotPolicy {
    pub max_age_ms: u64,
    pub stale_if_error_ms: u64,
}
impl Default for SnapshotPolicy {
    /// 业务作用：默认禁止异常后继续使用旧路由。
    /// 参数说明：无。
    /// 返回：十五秒最大证据年龄和零异常宽限。
    fn default() -> Self {
        Self {
            max_age_ms: 15000,
            stale_if_error_ms: 0,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryPolicy {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}
impl Default for RetryPolicy {
    /// 业务作用：对不可达连接限制恢复重试频率。
    /// 参数说明：无。
    /// 返回：一秒起步、三十秒封顶的退避。
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
        }
    }
}

impl Default for ConnectionConfig {
    /// 业务作用：默认连接必须准备成功，但没有指定 provider 时不猜测后端。
    /// 参数说明：无。
    /// 返回：有限请求和观察预算的候选配置。
    fn default() -> Self {
        Self {
            provider: String::new(),
            startup_required: true,
            request_timeout_ms: 3000,
            poll_interval_ms: 5000,
            idle_timeout_ms: 30000,
            snapshot: SnapshotPolicy::default(),
            retry: RetryPolicy::default(),
            nacos: None,
            consul: None,
            kubernetes: None,
            static_config: None,
            dns: None,
            rest: None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StaticConfig {
    #[serde(deserialize_with = "unique_map")]
    pub services: BTreeMap<String, Vec<Instance>>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DnsConfig {
    #[serde(deserialize_with = "unique_map")]
    pub services: BTreeMap<String, DnsTarget>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DnsTarget {
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RegistrationConfig {
    /// 发布目标的 qualifier；Rust 字段保留兼容名称，配置序列化使用 qualifier。
    #[serde(rename = "qualifier", alias = "connection_ref")]
    pub connection_ref: String,
    pub service_name: String,
    pub ip: Option<String>,
    pub port: Option<u16>,
    pub listener_ref: Option<String>,
    pub eligibility_ref: Option<String>,
    pub metadata: HashMap<String, String>,
    pub startup_required: bool,
    pub readiness_critical: bool,
    pub monitor_interval_ms: u64,
    pub observation_grace_ms: u64,
    pub deregister_timeout_ms: u64,
    pub nacos: Option<NacosRegistration>,
    #[serde(deserialize_with = "optional_unique_value")]
    pub consul: Option<serde_json::Value>,
}

impl Default for RegistrationConfig {
    /// 业务作用：注册目标省略 qualifier 时使用 primary，默认要求首次注册确认。
    /// 参数说明：无。
    /// 返回：服务与地址待显式配置或宿主解析的注册项；兼容默认连接覆盖不改变此处的 primary。
    fn default() -> Self {
        Self {
            connection_ref: "primary".into(),
            service_name: String::new(),
            ip: None,
            port: None,
            listener_ref: None,
            eligibility_ref: None,
            metadata: HashMap::new(),
            startup_required: true,
            readiness_critical: false,
            monitor_interval_ms: 5000,
            observation_grace_ms: 15000,
            deregister_timeout_ms: 3000,
            nacos: None,
            consul: None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NacosRegistration {
    pub ephemeral: bool,
    pub cluster_name: String,
    pub weight: f64,
}
impl Default for NacosRegistration {
    /// 业务作用：显式提供 Nacos 临时实例和默认集群属性。
    /// 参数说明：无。
    /// 返回：默认权重为一，不影响其他后端。
    fn default() -> Self {
        Self {
            ephemeral: true,
            cluster_name: "DEFAULT".into(),
            weight: 1.0,
        }
    }
}

/// 宿主已绑定 listener 的事实；代理映射地址由 registration 的 ip/port 显式覆盖。
#[derive(Clone)]
pub struct ListenerEndpoint {
    pub ip: String,
    pub port: u16,
}

impl DiscoveryConfig {
    /// 业务作用：校验所有引用与 provider 块，必需注册提升所属连接的启动要求。
    /// 参数说明：无。
    /// 返回：配置可用于独立宿主或 napp；禁用配置不产生后端连接，引用与 provider 拒绝携带字段路径。
    pub fn validate(&self) -> Result<()> {
        self.limits
            .validate()
            .map_err(|error| error.at_config_path("discovery.limits"))?;
        if !self.enabled
            && (!self.connections.is_empty()
                || !self.registrations.is_empty()
                || self.default_connection.is_some()
                || self.eligibility_probe)
        {
            return Err(invalid(
                "disabled discovery cannot declare connections, registrations or probes",
            ));
        }
        if self.connections.len() > self.limits.max_connections
            || self.registrations.len() > self.limits.max_registrations
        {
            return Err(invalid("discovery configuration exceeds capacity"));
        }
        if let Some(name) = &self.default_connection {
            if !self.connections.contains_key(name) {
                return Err(invalid("unknown default connection")
                    .at_config_path("discovery.default_connection"));
            }
        }
        for (name, connection) in &self.connections {
            validate_name(name).map_err(|error| error.at_config_path("discovery.connections"))?;
            connection
                .validate()
                .map_err(|error| error.at_config_path(format!("discovery.connections.{name}")))?;
        }
        for (name, registration) in &self.registrations {
            validate_name(name).map_err(|error| error.at_config_path("discovery.registrations"))?;
            let connection = self
                .connections
                .get(&registration.connection_ref)
                .ok_or_else(|| {
                    invalid("unknown registration connection")
                        .at_config_path(format!("discovery.registrations.{name}.qualifier"))
                })?;
            if !connection.has_service(&registration.service_name) {
                return Err(invalid("unknown registration service binding")
                    .at_config_path(format!("discovery.registrations.{name}.service_name")));
            }
            if registration.service_name.is_empty()
                || registration.service_name.len() > 512
                || registration
                    .ip
                    .as_ref()
                    .is_some_and(|ip| ip.is_empty() || ip.trim() != ip)
                || registration.port == Some(0)
                || registration.metadata.len() > 128
                || registration
                    .metadata
                    .iter()
                    .any(|(key, value)| key.len() > 256 || value.len() > 4096)
            {
                return Err(invalid(
                    "invalid registration service, endpoint or metadata",
                ));
            }
            registration.policy().validate(&connection.policy())?;
            match connection.provider.as_str() {
                "nacos" if registration.consul.is_none() => {}
                "consul" if registration.nacos.is_none() && registration.consul.is_some() => {}
                _ => {
                    return Err(invalid(
                        "registration attributes do not match a writable provider",
                    ))
                }
            }
        }
        Ok(())
    }

    /// 业务作用：构造各身份隔离的 provider 和公共 owner，不启动注册或网络轮询。
    /// 参数说明：无；配置值应已完成宿主占位符解析。
    /// 返回：唯一关闭 owner；只启用已编入的 provider。
    pub async fn build(&self) -> Result<DiscoveryOwner> {
        self.validate()?;
        let mut builder = OwnerBuilder::default().limits(self.limits.clone());
        if !self.enabled {
            return builder.build();
        }
        if let Some(default) = &self.default_connection {
            builder = builder.default_connection(default);
        }
        for (name, config) in &self.connections {
            let mut policy = config.policy();
            policy.startup_required |= self.registrations.values().any(|registration| {
                registration.connection_ref == *name && registration.startup_required
            });
            builder = builder.connection(ConnectionSpec {
                name: name.clone(),
                provider: config.build().await.map_err(|error| {
                    error.at_config_path(format!("discovery.connections.{name}"))
                })?,
                policy,
            })?;
        }
        builder.build()
    }

    /// 业务作用：在全部监听器已绑定后解析一次完整注册集合。
    /// 参数说明：`listeners` 是实际端点，`eligibility` 是宿主本地资格源；单项时允许省略对应引用。
    /// 返回：全部可解析的计划；不执行外部注册，不生成业务专用 metadata。
    pub fn resolve(
        &self,
        listeners: &BTreeMap<String, ListenerEndpoint>,
        eligibility: &BTreeMap<String, Eligibility>,
    ) -> Result<Vec<RegistrationSpec>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        self.validate()?;
        let mut plans = Vec::new();
        for (name, registration) in &self.registrations {
            let listener = match &registration.listener_ref {
                Some(name) => Some(
                    listeners
                        .get(name)
                        .ok_or_else(|| invalid("unknown registration listener"))?,
                ),
                None if listeners.len() == 1 => listeners.values().next(),
                None => None,
            };
            let inferred_listener = registration
                .listener_ref
                .as_ref()
                .or_else(|| (listeners.len() == 1).then(|| listeners.keys().next().unwrap()));
            let eligibility = match &registration.eligibility_ref {
                Some(name) => eligibility.get(name),
                None if inferred_listener.is_some() => eligibility.get(inferred_listener.unwrap()),
                None if eligibility.len() == 1 => eligibility.values().next(),
                None => None,
            }
            .cloned()
            .ok_or_else(|| invalid("registration eligibility source is absent or ambiguous"))?;
            let ip = registration
                .ip
                .as_ref()
                .or_else(|| listener.map(|listener| &listener.ip))
                .ok_or_else(|| invalid("registration IP is absent"))?;
            let port = registration
                .port
                .or_else(|| listener.map(|listener| listener.port))
                .ok_or_else(|| invalid("registration port is absent"))?;
            let mut instance =
                Instance::new(ip.clone(), port).with_metadata_map(registration.metadata.clone());
            if let Some(nacos) = &registration.nacos {
                instance = instance
                    .with_cluster(&nacos.cluster_name)
                    .with_ephemeral(nacos.ephemeral)
                    .with_weight(nacos.weight);
            }
            #[allow(unused_mut)]
            let mut plan = RegistrationPlan {
                id: name.clone(),
                service: registration.service_name.clone(),
                instance,
                attributes: BTreeMap::new(),
            };
            if let Some(consul) = &registration.consul {
                #[cfg(feature = "consul")]
                {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct CheckConfig {
                        check: nadisc_consul::HealthCheck,
                    }
                    let config: CheckConfig = serde_json::from_value(consul.clone())
                        .map_err(|_| invalid("invalid Consul registration configuration"))?;
                    config.check.apply(&mut plan)?;
                }
                #[cfg(not(feature = "consul"))]
                {
                    let _ = consul;
                    return Err(invalid("Consul provider feature is disabled"));
                }
            }
            plans.push(RegistrationSpec {
                connection_ref: registration.connection_ref.clone(),
                plan,
                policy: registration.policy(),
                eligibility,
            });
        }
        Ok(plans)
    }
}

impl RegistrationConfig {
    /// 业务作用：将配置中的启动与运行期要求转换成公共注册策略。
    /// 参数说明：无。
    /// 返回：两个必需性维度保持独立。
    pub fn policy(&self) -> RegistrationPolicy {
        RegistrationPolicy {
            startup_required: self.startup_required,
            readiness_critical: self.readiness_critical,
            monitor_interval_ms: self.monitor_interval_ms,
            observation_grace_ms: self.observation_grace_ms,
            deregister_timeout_ms: self.deregister_timeout_ms,
        }
    }
}

impl ConnectionConfig {
    /// 业务作用：在网络准备之前核对宿主引用是否属于该连接的显式服务目录。
    /// 参数说明：`service` 是本地服务别名，不能替代后端服务名或跨连接查询。
    /// 返回：静态、DNS 或已配置 provider 服务表包含该精确别名时为 true。
    pub fn has_service(&self, service: &str) -> bool {
        match self.provider.as_str() {
            "static" => self
                .static_config
                .as_ref()
                .is_some_and(|config| config.services.contains_key(service)),
            "dns" => self
                .dns
                .as_ref()
                .is_some_and(|config| config.services.contains_key(service)),
            "nacos" | "consul" | "kubernetes" => {
                let value = match self.provider.as_str() {
                    "nacos" => &self.nacos,
                    "consul" => &self.consul,
                    _ => &self.kubernetes,
                };
                value
                    .as_ref()
                    .and_then(|value| value.get("services"))
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|services| services.contains_key(service))
            }
            _ => false,
        }
    }

    /// 业务作用：把公开配置转换为 owner 的冻结时效预算。
    /// 参数说明：无。
    /// 返回：不依赖具体传输默认值的公共策略。
    pub fn policy(&self) -> ConnectionPolicy {
        ConnectionPolicy {
            startup_required: self.startup_required,
            request_timeout_ms: self.request_timeout_ms,
            poll_interval_ms: self.poll_interval_ms,
            max_age_ms: self.snapshot.max_age_ms,
            stale_if_error_ms: self.snapshot.stale_if_error_ms,
            idle_timeout_ms: self.idle_timeout_ms,
            initial_backoff_ms: self.retry.initial_backoff_ms,
            max_backoff_ms: self.retry.max_backoff_ms,
        }
    }

    /// 业务作用：拒绝同时填写多个 provider 块或未编入的运行时能力。
    /// 参数说明：无。
    /// 返回：恰好一个匹配 provider 块通过。
    pub fn validate(&self) -> Result<()> {
        self.policy().validate()?;
        if let Some(rest) = &self.rest {
            rest.validate()?;
        }
        let blocks = [
            self.nacos.is_some(),
            self.consul.is_some(),
            self.kubernetes.is_some(),
            self.static_config.is_some(),
            self.dns.is_some(),
        ];
        if blocks.into_iter().filter(|present| *present).count() != 1 {
            return Err(invalid("one provider configuration block is required"));
        }
        let supported = match self.provider.as_str() {
            "static" => self.static_config.is_some(),
            "dns" => self.dns.is_some(),
            "nacos" => self.nacos.is_some() && cfg!(feature = "nacos"),
            "consul" => self.consul.is_some() && cfg!(feature = "consul"),
            "kubernetes" => self.kubernetes.is_some() && cfg!(feature = "kubernetes"),
            _ => false,
        };
        if !supported {
            return Err(
                invalid("provider configuration or compiled feature is unavailable")
                    .at_config_path("provider"),
            );
        }
        // 全部 provider 的配置先通过类型检查，再允许 build 读取任何认证材料。
        if let Some(config) = &self.static_config {
            LocalProvider::static_services(config.services.clone())?;
        }
        if let Some(config) = &self.dns {
            LocalProvider::dns_services(
                config
                    .services
                    .iter()
                    .map(|(name, target)| {
                        (
                            name.clone(),
                            DnsService {
                                host: target.host.clone(),
                                port: target.port,
                            },
                        )
                    })
                    .collect(),
            )?;
        }
        #[cfg(feature = "nacos")]
        if let Some(value) = &self.nacos {
            let mut config: nanacos::managed::ManagedNacosConfig =
                serde_json::from_value(value.clone())
                    .map_err(|_| invalid("invalid Nacos provider configuration"))?;
            config.request_timeout_ms = self.request_timeout_ms;
            nanacos::managed::ManagedNacos::new(config)?;
        }
        #[cfg(feature = "consul")]
        if let Some(value) = &self.consul {
            let mut config: nadisc_consul::ConsulConfig = serde_json::from_value(value.clone())
                .map_err(|_| invalid("invalid Consul provider configuration"))?;
            config.request_timeout_ms = self.request_timeout_ms;
            nadisc_consul::ConsulProvider::new(config)?;
        }
        #[cfg(feature = "kubernetes")]
        if let Some(value) = &self.kubernetes {
            let mut config: nadisc_kubernetes::KubernetesConfig =
                serde_json::from_value(value.clone())
                    .map_err(|_| invalid("invalid Kubernetes provider configuration"))?;
            config.request_timeout_ms = self.request_timeout_ms;
            config.validate()?;
        }
        Ok(())
    }

    /// 业务作用：构造只属于当前连接的后端，不复用其他账号会话。
    /// 参数说明：无。
    /// 返回：中性 provider；认证文件读取受其 adapter 自身有限预算约束。
    async fn build(&self) -> Result<Arc<dyn ManagedProvider>> {
        self.validate()?;
        match self.provider.as_str() {
            "static" => Ok(Arc::new(LocalProvider::static_services(
                self.static_config.as_ref().unwrap().services.clone(),
            )?)),
            "dns" => Ok(Arc::new(LocalProvider::dns_services(
                self.dns
                    .as_ref()
                    .unwrap()
                    .services
                    .iter()
                    .map(|(name, target)| {
                        (
                            name.clone(),
                            DnsService {
                                host: target.host.clone(),
                                port: target.port,
                            },
                        )
                    })
                    .collect(),
            )?)),
            #[cfg(feature = "nacos")]
            "nacos" => {
                let mut config: nanacos::managed::ManagedNacosConfig =
                    serde_json::from_value(self.nacos.clone().unwrap())
                        .map_err(|_| invalid("invalid Nacos provider configuration"))?;
                config.request_timeout_ms = self.request_timeout_ms;
                Ok(Arc::new(nanacos::managed::ManagedNacos::new(config)?))
            }
            #[cfg(feature = "consul")]
            "consul" => {
                let mut config: nadisc_consul::ConsulConfig =
                    serde_json::from_value(self.consul.clone().unwrap())
                        .map_err(|_| invalid("invalid Consul provider configuration"))?;
                config.request_timeout_ms = self.request_timeout_ms;
                Ok(Arc::new(nadisc_consul::ConsulProvider::new(config)?))
            }
            #[cfg(feature = "kubernetes")]
            "kubernetes" => {
                let mut config: nadisc_kubernetes::KubernetesConfig =
                    serde_json::from_value(self.kubernetes.clone().unwrap())
                        .map_err(|_| invalid("invalid Kubernetes provider configuration"))?;
                config.request_timeout_ms = self.request_timeout_ms;
                Ok(Arc::new(
                    nadisc_kubernetes::KubernetesProvider::new(config).await?,
                ))
            }
            _ => Err(invalid("provider is unavailable")),
        }
    }
}

/// 业务作用：保存按需编入的后端配置前递归拒绝重复键，防止秘密或作用域字段被覆盖。
/// 参数说明：`deserializer` 必须来自尚未折叠重复项的原始输入。
/// 返回：完整唯一键值树；重复项错误不包含原始值。
fn optional_unique_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<serde_json::Value>, D::Error> {
    Option::<UniqueValue>::deserialize(deserializer).map(|value| value.map(|value| value.0))
}

struct UniqueValue(serde_json::Value);
impl<'de> Deserialize<'de> for UniqueValue {
    /// 业务作用：在容纳后端扩展字段时保留原始重复键校验。
    /// 参数说明：`deserializer` 提供原始序列化事件。
    /// 返回：完整 JSON 兼容树；不接受非有限数值和重复字段。
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Unique;
        impl<'de> Visitor<'de> for Unique {
            type Value = UniqueValue;
            /// 业务作用：返回不泄露配置内容的输入类型诊断。
            /// 参数说明：`formatter` 为诊断目标。
            /// 返回：固定类型描述。
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a configuration value with unique fields")
            }
            /// 业务作用：校验映射每个字段只出现一次。
            /// 参数说明：`map` 是未折叠输入。
            /// 返回：唯一字段对象，重复输入整体失败。
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some((key, value)) = map.next_entry::<String, UniqueValue>()? {
                    if values.insert(key, value.0).is_some() {
                        return Err(A::Error::custom("duplicate discovery field"));
                    }
                }
                Ok(UniqueValue(serde_json::Value::Object(values)))
            }
            /// 业务作用：对配置数组中的每个对象继续执行字段唯一性检查。
            /// 参数说明：`seq` 是输入数组。
            /// 返回：完整数组，不截断元素。
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(serde_json::Value::Array(values)))
            }
            /// 业务作用：保留文本配置的原始内容供后续 provider 校验。
            /// 参数说明：`value` 是字符串值。
            /// 返回：拥有内容的 JSON 字符串，不输出诊断日志。
            fn visit_str<E: Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            /// 业务作用：保留布尔配置，不转换为字符串。
            /// 参数说明：`value` 是布尔值。
            /// 返回：对应 JSON 布尔值。
            fn visit_bool<E: Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            /// 业务作用：保留无符号配置数值的范围。
            /// 参数说明：`value` 是无符号整数。
            /// 返回：对应 JSON 数字。
            fn visit_u64<E: Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            /// 业务作用：保留有符号配置供后续范围校验。
            /// 参数说明：`value` 是有符号整数。
            /// 返回：对应 JSON 数字。
            fn visit_i64<E: Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            /// 业务作用：拒绝不能表达确定权重或预算的非有限数值。
            /// 参数说明：`value` 是浮点输入。
            /// 返回：有限 JSON 数字或配置错误。
            fn visit_f64<E: Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|value| UniqueValue(value.into()))
                    .ok_or_else(|| E::custom("non-finite discovery value"))
            }
            /// 业务作用：保留显式空值以供类型化字段判断是否允许缺省。
            /// 参数说明：无。
            /// 返回：JSON 空值。
            fn visit_unit<E: Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(serde_json::Value::Null))
            }
        }
        deserializer.deserialize_any(Unique)
    }
}

/// 业务作用：在映射落地前拒绝重复名称，避免反序列化覆盖掩盖含混连接和注册。
/// 参数说明：`deserializer` 提供原始映射事件，不接受预先丢失重复项的输入作为证明。
/// 返回：唯一键映射；错误不输出可能包含秘密的值。
fn unique_map<'de, D, T>(deserializer: D) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Unique<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for Unique<T> {
        type Value = BTreeMap<String, T>;
        /// 业务作用：报告固定的映射类型要求。
        /// 参数说明：`formatter` 接收无配置值的类型描述。
        /// 返回：安全的反序列化诊断。
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map with unique names")
        }
        /// 业务作用：逐项检查名称唯一性后才保存配置。
        /// 参数说明：`map` 是尚未合并的原始映射。
        /// 返回：完整唯一键集合或重复键错误。
        fn visit_map<A: MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut values = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, T>()? {
                if values.insert(key, value).is_some() {
                    return Err(A::Error::custom("duplicate discovery name"));
                }
            }
            Ok(values)
        }
    }
    deserializer.deserialize_map(Unique(std::marker::PhantomData))
}
