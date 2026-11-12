//! 服务发现 provider-neutral 中性类型、规则与接口。
//! qualifier 与 primary 默认选择由公共 owner 管理；本层不维护应用连接目录。
//!
//! Nacos、Consul、Kubernetes 和 Static/DNS 复用同一份 [`Instance`] 与
//! [流量过滤规则](is_traffic_instance)，各后端之间不互相依赖。
//!
//! [`DiscoveryClient`] / [`ServiceRegistry`] 等抽象也定义在这里,后端 crate 各自实现;`RestDiscovery` 等上层只面向本 crate,
//! 不绑定 `NacosDiscoveryClient` 之类具体后端类型。
//! [`managed`] 定义完整观察、注册所有权、写入许可和后端关闭合同；生命周期调度由独立的
//! `nadisc-runtime` owner 承担，不在本 crate 中创建全局运行时。

use std::collections::HashMap;

#[cfg(feature = "http")]
pub mod http;
pub mod managed;
mod providers;

pub use providers::{DnsDiscovery, DnsService, StaticDiscovery};

/// 一个服务实例(注册/发现的中性表示;不绑定任何后端 SDK 类型)。
/// `PartialEq` 用于比较完整实例属性；可选 `serde` feature 用于类型化配置。
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
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
    /// 业务作用：构造尚未指定地址的实例基线，供显式设置端点后发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回空地址、零端口、权重 1、健康且启用的临时实例；该默认端点不能通过流量资格判断。
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
    /// 业务作用：按给定地址和端口构造待注册或发现的实例描述。
    ///
    /// 参数说明：
    /// - `ip`：调用方可直连的 IP 或主机地址。
    /// - `port`：对应的服务端口。
    ///
    /// 返回：返回带默认健康、启用、权重和临时属性的实例，不执行端点校验或网络注册。
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self {
            ip: ip.into(),
            port,
            ..Default::default()
        }
    }

    /// 业务作用：设置供流量筛选及负载均衡使用的实例权重。
    ///
    /// 参数说明：
    /// - `weight`：原样保存的权重，流量资格要求其有限且大于零。
    ///
    /// 返回：返回设置后的实例，不在此拒绝非法权重或改变注册状态。
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// 业务作用：设置实例描述中的健康事实。
    ///
    /// 参数说明：
    /// - `healthy`：后端或调用方观察到的健康状态。
    ///
    /// 返回：返回更新后的实例；false 会使公共流量筛选拒绝该实例，不主动通知后端。
    pub fn with_healthy(mut self, healthy: bool) -> Self {
        self.healthy = healthy;
        self
    }

    /// 业务作用：设置实例是否被管理侧允许承接流量。
    ///
    /// 参数说明：
    /// - `enabled`：false 表示保留实例描述但禁用流量资格。
    ///
    /// 返回：返回更新后的实例，不发送后端启停命令。
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// 业务作用：选择实例描述所表达的临时或持久注册属性。
    ///
    /// 参数说明：
    /// - `ephemeral`：true 表示临时实例，具体保活语义由后端实现。
    ///
    /// 返回：返回更新后的实例，不启动心跳或执行注册。
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = ephemeral;
        self
    }

    /// 业务作用：为实例设置显式集群归属。
    ///
    /// 参数说明：
    /// - `cluster_name`：按后端命名约定解释的集群名称。
    ///
    /// 返回：返回集群属性为 Some 的实例，不校验后端支持情况。
    pub fn with_cluster(mut self, cluster_name: impl Into<String>) -> Self {
        self.cluster_name = Some(cluster_name.into());
        self
    }

    /// 业务作用：设置或清空实例的可选集群归属。
    ///
    /// 参数说明：
    /// - `cluster_name`：Some 提供集群名，None 清空显式集群属性。
    ///
    /// 返回：返回更新后的实例，默认集群的实际解释由后端决定。
    pub fn with_cluster_opt(mut self, cluster_name: Option<impl Into<String>>) -> Self {
        self.cluster_name = cluster_name.map(Into::into);
        self
    }

    /// 业务作用：追加或覆盖实例的一项业务元数据。
    ///
    /// 参数说明：
    /// - `key`：区域、灰度等元数据键。
    /// - `value`：对应的元数据文本值。
    ///
    /// 返回：返回更新后的实例；同名键覆盖旧值，不合并旧值内容。
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// 业务作用：用完整元数据表替换实例已有元数据。
    ///
    /// 参数说明：
    /// - `metadata`：新的完整键值表。
    ///
    /// 返回：返回更新后的实例，原元数据中未出现在新表的键不再保留。
    pub fn with_metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

/// 业务作用：统一判断中性实例描述是否可作为业务流量候选。
///
/// 参数说明：
/// - `inst`：待检查健康、启用、权重与端点字段的实例。
///
/// 返回：仅启用、健康、有限正权重、地址非空且无首尾空白、端口非零时返回 true；不探测网络健康。
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
    /// 业务作用：声明调用方是否必须在每次传输提交前复验受管路由。
    ///
    /// 参数说明：无。
    ///
    /// 返回：旧独立 provider 默认不提供这一附加保证；受管句柄返回 true。
    fn requires_admission(&self) -> bool {
        false
    }

    /// 业务作用：在实现提供的候选资格保护范围内同步提交传输。
    ///
    /// 参数说明：
    /// - `_service`：用于复验候选来源的服务名，默认实现不读取。
    /// - `_instance`：待复验实例，默认实现不读取。
    /// - `submit`：只能同步启动传输、不得阻塞或重入发现接口的回调。
    ///
    /// 返回：默认实现直接调用回调并成功，不提供撤权门禁；要求 admission 的实现应在拒绝时不调用回调。
    fn submit_if_current(
        &self,
        _service: &str,
        _instance: &Instance,
        submit: &mut dyn FnMut(),
    ) -> anyhow::Result<()> {
        submit();
        Ok(())
    }

    /// 业务作用：提供发现后端允许枚举的去重服务名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回当前可枚举名称，失败返回后端错误；返回空列表与枚举失败应区分。
    async fn list_services(&self) -> anyhow::Result<Vec<String>>;

    /// 业务作用：读取指定服务中满足公共流量资格的实例。
    ///
    /// 参数说明：
    /// - `service`：后端作用域内的目标服务名。
    ///
    /// 返回：成功返回可承接流量的实例列表，完整空列表表示当前没有候选；查询失败返回错误。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>>;

    /// 业务作用：读取服务完整实例视图供管理和观察使用。
    ///
    /// 参数说明：
    /// - `service`：要查询全部实例的目标服务名。
    ///
    /// 返回：成功返回包括禁用、不健康及零权重实例的列表；查询失败返回错误，不以流量过滤后的空集代替完整视图。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>>;

    /// 业务作用：用默认选项建立服务流量候选订阅。
    ///
    /// 参数说明：
    /// - `service`：要持续观察实例变化的服务名。
    ///
    /// 返回：成功返回初始快照接收端及必须持有的取消句柄；建立失败返回错误，取消语义由后端实现。
    async fn watch(&self, service: &str) -> anyhow::Result<ServiceWatch> {
        self.watch_with_options(service, WatchOptions::default())
            .await
    }

    /// 业务作用：按显式订阅策略建立服务流量候选观察。
    ///
    /// 参数说明：
    /// - `service`：目标服务名。
    /// - `options`：订阅轮询兜底等中性选项。
    ///
    /// 返回：成功返回初始快照和后续最新值接收端及取消句柄；配置或建立失败返回错误，调用方应持有句柄维持订阅。
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
    /// 业务作用：提供服务实例订阅的默认轮询兜底间隔。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回轮询间隔为五秒的选项，不启动订阅。
    fn default() -> Self {
        Self {
            poll_interval: std::time::Duration::from_secs(5),
        }
    }
}

impl WatchOptions {
    /// 业务作用：构造可继续设置的默认订阅策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回默认五秒轮询间隔的选项，不启动任务。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：设置后端校正实例快照的轮询间隔。
    ///
    /// 参数说明：
    /// - `poll_interval`：预期非零的轮询间隔，后续由 validate 检查。
    ///
    /// 返回：返回更新后的选项，不立即验证间隔或重置已有订阅。
    pub fn with_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// 业务作用：拒绝无法建立周期校正的零轮询间隔。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：间隔非零时成功，否则返回配置错误；不检查具体后端的其它预算。
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
    /// 业务作用：消费订阅句柄并显式停止对应服务观察。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：后端取消完成时成功，失败返回错误；调用后句柄不再可用，Drop 提供后端定义的取消兜底。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()>;
}

/// 服务【注册侧】抽象:注册本实例,返回 [`Registration`] 句柄。
#[async_trait::async_trait]
pub trait ServiceRegistry: Send + Sync + 'static {
    /// 业务作用：向后端注册本实例并取得其生命周期句柄。
    ///
    /// 参数说明：
    /// - `service`：后端作用域内的注册服务名。
    /// - `instance`：本进程对外暴露的端点、健康和元数据。
    ///
    /// 返回：成功返回必须持有的注册句柄；校验或注册失败返回错误。具体续约、注销和 Drop 兜底语义由后端实现。
    async fn register(
        &self,
        service: &str,
        instance: Instance,
    ) -> anyhow::Result<Box<dyn Registration>>;
}

/// 注册句柄抽象(后端的 RegistrationGuard 实现它):drop 即 best-effort 下线,也可显式 `deregister().await`。
#[async_trait::async_trait]
pub trait Registration: Send {
    /// 业务作用：消费注册句柄并显式下线对应后端实例。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：后端注销完成时成功，失败返回错误；调用后句柄不再可用，失败不应被当作远端记录已删除。
    async fn deregister(self: Box<Self>) -> anyhow::Result<()>;
}
