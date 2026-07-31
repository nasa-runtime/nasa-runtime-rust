//! 服务发现 provider-neutral 中性类型、规则与接口。
//!
//! 这里只放**与具体注册中心无关**的数据类型与规则,让 `nacos` / 以后 `eureka` 等后端 crate 复用同一份 [`Instance`]
//! 与同一套[流量过滤规则](is_traffic_instance)(而彼此不依赖)。
//!
//! [`DiscoveryClient`] / [`ServiceRegistry`] 等抽象也定义在这里,后端 crate 各自实现;`RestDiscovery` 等上层只面向本 crate,
//! 不绑定 `NacosDiscoveryClient` 之类具体后端类型。

use std::collections::HashMap;

mod providers;

pub use providers::{DnsDiscovery, DnsService, StaticDiscovery};

/// 一个服务实例(注册/发现的中性表示;不绑定任何后端 SDK 类型)。
/// `PartialEq` 便于缓存/差异计算(无 `Eq`:`weight` 是 `f64`)。serde 暂不加(YAGNI;需要 admin/快照序列化时再上 optional feature)。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Instance {
    /// 实例 IP 或可直连主机地址。
    pub ip: String,
    /// 实例服务端口。
    pub port: u16,
    /// 负载权重(默认 1.0)。`0` = 已注册但不承载流量(平滑摘流/预注册);[`is_traffic_instance`] 会把它过滤掉。
    pub weight: f64,
    /// 是否健康(discover 读到的值;register 时通常保持 true)。
    pub healthy: bool,
    /// 是否启用(默认 true)。`false` = 被管理端【禁用】:仍在注册中心,但不应承载流量。
    /// 映射自注册中心的 enabled 状态;`discover_all` 会如实暴露,[`is_traffic_instance`] 会过滤掉。
    pub enabled: bool,
    /// 临时实例(默认 true):靠心跳保活、断连/超时自动摘除;false = 持久实例。
    pub ephemeral: bool,
    /// 集群名(None = 默认集群)。
    pub cluster_name: Option<String>,
    /// 自定义元数据(版本/区域/灰度标等)。
    pub metadata: HashMap<String, String>,
}

impl Default for Instance {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            ip: String::new(),
            port: 0,
            weight: 1.0,
            healthy: true,
            enabled: true,
            ephemeral: true,
            cluster_name: None,
            metadata: HashMap::new(),
        }
    }
}

impl Instance {
    /// 起手式:必填 `ip` + `port`,其余取默认(weight 1.0 / healthy / enabled / ephemeral);再链式 `with_*`。
    /// 推荐用它替代结构体字面量:以后 `Instance` 加字段时构造代码不必跟着改。
    ///
    /// # 参数
    ///
    /// - `ip`: 实例对调用方暴露的 IP 或主机地址。
    /// - `port`: 实例对调用方暴露的服务端口。
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self {
            ip: ip.into(),
            port,
            ..Default::default()
        }
    }

    /// 负载权重(0 = 已注册但不承载流量)。
    ///
    /// # 参数
    ///
    /// - `weight`: 负载均衡权重；`0` 或非有限值不会被视为可承载流量。
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// 健康标志(register 一般保持 true)。
    ///
    /// # 参数
    ///
    /// - `healthy`: 注册中心或调用方观察到的健康状态。
    pub fn with_healthy(mut self, healthy: bool) -> Self {
        self.healthy = healthy;
        self
    }

    /// 启用标志(false = 注册但禁用,不承载流量)。
    ///
    /// # 参数
    ///
    /// - `enabled`: 管理侧是否允许该实例承载业务流量。
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// 临时实例(true,默认)/ 持久实例(false)。
    ///
    /// # 参数
    ///
    /// - `ephemeral`: 是否按临时实例注册，通常用于心跳保活和断连自动摘除。
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = ephemeral;
        self
    }

    /// 集群名。
    ///
    /// # 参数
    ///
    /// - `cluster_name`: 注册中心内的集群名称或分组名称。
    pub fn with_cluster(mut self, cluster_name: impl Into<String>) -> Self {
        self.cluster_name = Some(cluster_name.into());
        self
    }

    /// 可选集群名。
    ///
    /// # 参数
    ///
    /// - `cluster_name`: 可选集群名称；`None` 表示清空为默认集群。
    pub fn with_cluster_opt(mut self, cluster_name: Option<impl Into<String>>) -> Self {
        self.cluster_name = cluster_name.map(Into::into);
        self
    }

    /// 追加一条元数据(可链式多次)。
    ///
    /// # 参数
    ///
    /// - `key`: 元数据键，例如版本、区域或灰度标签名。
    /// - `value`: 元数据值。
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// 替换整张元数据表。
    ///
    /// # 参数
    ///
    /// - `metadata`: 完整元数据表，会替换实例上已有的元数据。
    pub fn with_metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

/// **provider-neutral 的"可承载流量"判定**:统一各后端(nacos/eureka/…)对"这个实例能不能转流量"的口径,
/// 不依赖具体 SDK 的过滤行为。`discover` / `subscribe_channel` 等"给负载均衡用"的入口应只返回它为 `true` 的实例;
/// `discover_all`(管理/诊断)则不应用本规则、如实返回全部。
///
/// 规则:启用 + 健康 + 权重为正且有限 + ip 非空且无首尾空白 + port 非 0。
///
/// # 参数
///
/// - `inst`: 待判定是否可用于业务流量的服务实例。
pub fn is_traffic_instance(inst: &Instance) -> bool {
    inst.enabled
        && inst.healthy
        && inst.weight.is_finite()
        && inst.weight > 0.0
        && !inst.ip.is_empty()
        && inst.ip == inst.ip.trim()
        && inst.port != 0
}

// ════════════════════════════════════════════════════════════════════════════
// provider-neutral 接口:RestDiscovery 等上层只面向这些 trait,后端(nacos/eureka/...)实现它们。
// 后端具体类型(NacosDiscoveryClient 等)不应扩散到上层。
// ════════════════════════════════════════════════════════════════════════════

/// 服务发现【读侧】抽象:列服务、查实例(可承载流量/全部)、watch 实例变化。
/// 客户端负载均衡(RestDiscovery)持 `Arc<dyn DiscoveryClient>`,不绑定具体后端。
#[async_trait::async_trait]
pub trait DiscoveryClient: Send + Sync + 'static {
    /// 列出全部服务名(去重)。
    async fn list_services(&self) -> anyhow::Result<Vec<String>>;

    /// 某服务**可承载流量**的实例(后端用 [`is_traffic_instance`] 口径过滤)。
    ///
    /// # 参数
    ///
    /// - `service`: 要查询实例的服务名。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>>;

    /// 某服务**全部**实例(含不健康/禁用/零权重),供管理/诊断。
    ///
    /// # 参数
    ///
    /// - `service`: 要查询全部实例的服务名。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>>;

    /// 订阅某服务实例变化:返回 [`ServiceWatch`](初值=当前可承载流量快照 + 后续推送)。
    ///
    /// # 参数
    ///
    /// - `service`: 要订阅实例变化的服务名。
    async fn watch(&self, service: &str) -> anyhow::Result<ServiceWatch> {
        self.watch_with_options(service, WatchOptions::default())
            .await
    }

    /// 同 [`watch`](Self::watch),但允许上层设置 provider-neutral watch 选项。
    ///
    /// # 参数
    ///
    /// - `service`: 要订阅实例变化的服务名。
    /// - `options`: watch 轮询兜底等中性选项。
    async fn watch_with_options(
        &self,
        service: &str,
        options: WatchOptions,
    ) -> anyhow::Result<ServiceWatch>;
}

/// provider-neutral watch 选项。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WatchOptions {
    /// 后端用于校正服务实例快照的轮询间隔。具体后端可用订阅推送降低延迟,但应以该间隔作为最终一致兜底。
    pub poll_interval: std::time::Duration,
}

impl Default for WatchOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            poll_interval: std::time::Duration::from_secs(5),
        }
    }
}

impl WatchOptions {
    /// 起手式:默认 watch 选项。
    pub fn new() -> Self {
        Self::default()
    }

    /// 后端用于校正服务实例快照的轮询间隔。
    ///
    /// # 参数
    ///
    /// - `poll_interval`: 后端校正实例快照的轮询兜底间隔，不能为 `0`。
    pub fn with_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// 校验 watch 选项。后端实现应先调用它,再映射到自己的 provider-specific options。
    ///
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.poll_interval.is_zero(),
            "discovery: watch poll_interval 不能为 0"
        );
        Ok(())
    }
}

/// [`DiscoveryClient::watch`] 的返回:RAII 取消句柄 + 最新实例快照接收端。两者都要持有。
#[must_use = "ServiceWatch 必须持有:一旦 drop,服务订阅会被取消;receiver 也不会再收到更新"]
pub struct ServiceWatch {
    /// drop 即取消订阅;也可显式 `guard.unsubscribe().await`。
    pub guard: Box<dyn ServiceWatchGuard>,
    /// 最新"可承载流量实例"快照(`changed().await` 等下次变化)。
    pub receiver: tokio::sync::watch::Receiver<Vec<Instance>>,
}

/// watch 订阅句柄抽象(后端的 SubscribeGuard 实现它)。
#[async_trait::async_trait]
pub trait ServiceWatchGuard: Send {
    /// 显式取消订阅(优雅路径);不调则 drop 时兜底取消。
    ///
    /// # 参数
    ///
    /// - `self`: 被消费的订阅句柄，调用后不再继续接收实例变化。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()>;
}

/// 服务【注册侧】抽象:注册本实例,返回 [`Registration`] 句柄。
#[async_trait::async_trait]
pub trait ServiceRegistry: Send + Sync + 'static {
    /// 执行 register 操作；用于维护服务实例生命周期。
    ///
    /// # 参数
    ///
    /// - `service`: 要注册到的服务名。
    /// - `instance`: 当前进程对外暴露的服务实例信息。
    async fn register(
        &self,
        service: &str,
        instance: Instance,
    ) -> anyhow::Result<Box<dyn Registration>>;
}

/// 注册句柄抽象(后端的 RegistrationGuard 实现它):drop 即 best-effort 下线,也可显式 `deregister().await`。
#[async_trait::async_trait]
pub trait Registration: Send {
    /// 执行 deregister 操作；用于维护服务实例生命周期。
    ///
    /// # 参数
    ///
    /// - `self`: 被消费的注册句柄，调用后实例应从注册中心下线。
    async fn deregister(self: Box<Self>) -> anyhow::Result<()>;
}
