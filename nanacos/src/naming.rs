//! 注册中心客户端 [`NacosDiscoveryClient`]:`register`(+ SDK 自动心跳 + drop best-effort deregister)、`discover`/`discover_all`、
//! `subscribe`(+ cluster 过滤 / channel 变体)。重点是 [`RegistrationGuard`] 的 **drop best-effort deregister**,防优雅停机/panic 后留僵尸实例。
//! 只建 NamingService(不碰配置服务)。

use crate::client::{self, NacosProps};
// 服务实例 + 流量过滤规则用 provider-neutral 中性类型(与 eureka 等后端共享;定义在 discovery crate)。
use nadisc::Instance;
// is_traffic_instance 只在真实 feature 的 discover/subscribe 过滤里用;stub 下不引入避免 unused。
#[cfg(feature = "nacos")]
use nadisc::is_traffic_instance;

/// 注册中心客户端:内含 NamingService(由 SDK 维护其连接 + 临时实例心跳)。持有到不再需要注册/发现为止(drop 即断连)。
/// feature 关时为空壳(永不被构造,因为 [`connect`](Self::connect) 已先 bail)。
pub struct NacosDiscoveryClient {
    #[cfg(feature = "nacos")]
    naming: nacos_sdk::api::naming::NamingService,
    /// 默认分组(注册/发现用)。
    #[cfg(feature = "nacos")]
    group: String,
    /// 非空时作为注册中心实例的对外 IP,覆盖调用方传入的 `Instance.ip`。
    #[cfg(feature = "nacos")]
    discovery_ip: Option<String>,
}

impl std::fmt::Debug for NacosDiscoveryClient {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("NacosDiscoveryClient");
        #[cfg(feature = "nacos")]
        s.field("group", &self.group)
            .field("discovery_ip", &self.discovery_ip.as_deref().unwrap_or(""));
        s.finish_non_exhaustive()
    }
}

/// 校验 validate service 约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `service`: 服务名,用于服务发现或注册中心查询。
#[cfg(feature = "nacos")]
fn validate_service(service: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!service.trim().is_empty(), "nacos: service 不能为空");
    client::ensure_no_outer_ws("service", service)?;
    Ok(())
}

/// 分页参数校验(共享层 fail-fast,胜过把非法分页交给 SDK)。
///
/// # 参数
/// - `page_no`: Nacos 分页查询的页码。
/// - `page_size`: 分页查询单页大小。
#[cfg(feature = "nacos")]
fn validate_page(page_no: i32, page_size: i32) -> anyhow::Result<()> {
    anyhow::ensure!(page_no >= 1, "nacos: page_no 必须 >= 1,当前 {page_no}");
    anyhow::ensure!(
        page_size >= 1,
        "nacos: page_size 必须 >= 1,当前 {page_size}"
    );
    anyhow::ensure!(
        page_size <= 1000,
        "nacos: page_size 过大(>1000),当前 {page_size}"
    );
    Ok(())
}

/// 校验 validate cluster name 约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `field`: Hash 字段名或业务字段名,用于定位 key 内的子项。
/// - `cluster`: Nacos 实例所属的集群名称。
fn validate_cluster_name(field: &str, cluster: &str) -> anyhow::Result<()> {
    client::ensure_no_outer_ws(field, cluster)?;
    anyhow::ensure!(!cluster.is_empty(), "nacos: {field} 不能为空");
    anyhow::ensure!(
        cluster
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
        "nacos: {field} 只能含 0-9a-zA-Z-.,当前 {cluster:?}"
    );
    Ok(())
}

/// 校验 validate clusters 约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `clusters`: 服务实例所属集群列表。
fn validate_clusters(clusters: &[String]) -> anyhow::Result<()> {
    for cluster in clusters {
        validate_cluster_name("clusters[]", cluster)?;
    }
    Ok(())
}

/// 校验 validate instance 约束；用于在进入运行流程前 fail-fast。
///
/// # 参数
/// - `inst`: 服务发现返回的实例信息。
#[cfg(feature = "nacos")]
fn validate_instance(inst: &Instance) -> anyhow::Result<()> {
    anyhow::ensure!(!inst.ip.trim().is_empty(), "nacos: instance ip 不能为空");
    client::ensure_no_outer_ws("instance.ip", &inst.ip)?;
    anyhow::ensure!(inst.port != 0, "nacos: instance port 不能为 0");
    // weight 允许 0(已实测 Nacos 接受 weight=0 注册):0 = 已注册但不承载流量(平滑摘流/预注册);
    // discover/subscribe_channel 会用 is_traffic_instance 过滤掉 weight≤0,discover_all 保留。
    anyhow::ensure!(
        inst.weight.is_finite() && inst.weight >= 0.0,
        "nacos: instance weight 必须为非负且有限,当前 {}",
        inst.weight
    );
    // cluster_name 只能 0-9a-zA-Z-.(Nacos 服务端规则;不挡则错误推迟到服务端、文案与本层脱节)。
    if let Some(c) = &inst.cluster_name {
        validate_cluster_name("cluster_name", c)?;
    }
    Ok(())
}

/// 转换为 sdk 表示；用于对接下游接口。
///
/// # 参数
/// - `inst`: 服务发现返回的实例信息。
#[cfg(feature = "nacos")]
fn to_sdk(inst: &Instance) -> nacos_sdk::api::naming::ServiceInstance {
    nacos_sdk::api::naming::ServiceInstance {
        instance_id: None,
        ip: inst.ip.clone(),
        port: inst.port as i32,
        weight: inst.weight,
        healthy: inst.healthy,
        enabled: inst.enabled,
        ephemeral: inst.ephemeral,
        cluster_name: inst.cluster_name.clone(),
        service_name: None,
        metadata: inst.metadata.clone(),
    }
}

/// SDK 实例 → 中性 Instance。端口越界(<0 或 >65535)→ 返回 None 并 warn(不静默截断后交给业务负载均衡)。
///
/// # 参数
/// - `si`: 服务实例结构,用于完成注册、续约或列表解析。
#[cfg(feature = "nacos")]
fn try_from_sdk(si: &nacos_sdk::api::naming::ServiceInstance) -> Option<Instance> {
    if si.port < 0 || si.port > u16::MAX as i32 {
        tracing::warn!(ip = %si.ip, port = si.port, "nacos: 实例端口越界,已跳过");
        return None;
    }
    Some(
        Instance::new(si.ip.clone(), si.port as u16)
            .with_weight(si.weight)
            .with_healthy(si.healthy)
            .with_enabled(si.enabled)
            .with_ephemeral(si.ephemeral)
            .with_cluster_opt(si.cluster_name.clone())
            .with_metadata_map(si.metadata.clone()),
    )
}

/// 把实例列表按稳定 key `(cluster_name, ip, port)` 排序——用于 `subscribe_channel` 比较前归一化,
/// 避免 Nacos 返回同一批实例但顺序抖动时(`Vec` 的 `PartialEq` 顺序敏感)产生伪变更、触发订阅方无谓重建。
///
/// # 参数
/// - `v`: 待转换的值。
#[cfg(feature = "nacos")]
fn sort_instances(v: &mut [Instance]) {
    v.sort_by(|a, b| {
        a.cluster_name
            .cmp(&b.cluster_name)
            .then_with(|| a.ip.cmp(&b.ip))
            .then_with(|| a.port.cmp(&b.port))
    });
}

/// 规范化 `discovery_ip`:trim 后空串视为未配置(`None`)。connect 时把 `NacosProps.discovery_ip` 收进 client。
///
/// # 参数
/// - `raw`: 待解析的原始字符串、字节或配置值。
#[cfg(feature = "nacos")]
fn normalize_discovery_ip(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|ip| !ip.is_empty())
        .map(ToOwned::to_owned)
}

/// 把 client 的 `discovery_ip`(已规范化)应用到待注册实例:`Some` 覆盖 `Instance.ip`,`None` 保持原值。
/// 用于服务监听 `0.0.0.0`、本机多网卡、VPN/容器网卡等场景,避免注册不可拨号地址。
///
/// # 参数
/// - `discovery_ip`: 注册到发现中心的实例 IP 地址。
/// - `inst`: 服务发现返回的实例信息。
#[cfg(feature = "nacos")]
fn apply_discovery_ip(discovery_ip: Option<&str>, mut inst: Instance) -> Instance {
    if let Some(ip) = discovery_ip {
        inst.ip = ip.to_string();
    }
    inst
}

/// 最终注册 IP 是否为「未指定地址」(`0.0.0.0` / `::`,或空)——这类地址注册上去消费者无法拨号。
/// 非 IP 字面量(如主机名)按已指定处理(不拦,留给确实想注册 hostname 的场景)。
///
/// # 参数
/// - `ip`: 服务实例或目标节点的 IP 地址。
#[cfg(feature = "nacos")]
fn is_unspecified_ip(ip: &str) -> bool {
    let ip = ip.trim();
    ip.is_empty()
        || ip
            .parse::<std::net::IpAddr>()
            .map(|addr| addr.is_unspecified())
            .unwrap_or(false)
}

/// 去重保序:保留首次出现的顺序、丢弃重复(分页拉服务名跨页可能重复)。`list_services` 用。
///
/// # 参数
/// - `names`: 需要合并或解析的名称列表。
#[cfg(feature = "nacos")]
fn dedup_preserve_order(names: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    names
        .into_iter()
        .filter(|n| seen.insert(n.clone()))
        .collect()
}

/// 按总量和页大小计算最大页数；用于分页拉取实例列表。
///
/// # 参数
/// - `total`: 服务端返回的总记录数。
/// - `page_size`: 分页查询单页大小。
#[cfg(feature = "nacos")]
fn max_pages_from_total(total: i32, page_size: i32) -> i32 {
    let total = i64::from(total.max(0));
    let page_size = i64::from(page_size.max(1));
    ((total + page_size - 1) / page_size).max(1) as i32
}

/// `subscribe_channel_with_options` 的选项:cluster 过滤 + 轮询兜底间隔(默认 5s)。
///
/// `poll_interval` 决定"删除最后实例 / SDK 漏推 / 订阅吞错误"的最终一致延迟上界:
/// 低延迟场景(网关/LB/撮合)可调小(如 500ms),压低 Nacos 压力可调大。
///
/// 注意:cluster 不是 group;默认 group 常见为 `DEFAULT_GROUP`,但默认 cluster 是 `DEFAULT`。
/// 本实现按 Nacos cluster_name 规则校验,不允许 `_`。
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SubscribeOptions {
    /// 只订阅这些 cluster(空 = 全部)。cluster 不是 group;默认 cluster 通常是 `DEFAULT`。
    pub clusters: Vec<String>,
    /// discover 轮询兜底间隔(默认 5s)。
    pub poll_interval: std::time::Duration,
}

impl Default for SubscribeOptions {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            clusters: Vec::new(),
            poll_interval: std::time::Duration::from_secs(5),
        }
    }
}

impl SubscribeOptions {
    /// 起手式:默认订阅选项。
    pub fn new() -> Self {
        Self::default()
    }

    /// 只订阅这些 cluster(空 = 全部)。注意 cluster 不是 group;默认 cluster 通常是 `DEFAULT`。
    ///
    /// # 参数
    /// - `clusters`: 要订阅的 Nacos cluster 名称集合;空集合表示全部 cluster。
    pub fn with_clusters(mut self, clusters: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.clusters = clusters.into_iter().map(Into::into).collect();
        self
    }

    /// discover 轮询兜底间隔。
    ///
    /// # 参数
    /// - `poll_interval`: 后台 discover 校正快照的时间间隔,必须大于 0。
    pub fn with_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// 校验 cluster 名称和轮询间隔;builder 保持轻量,使用点 fail-fast。
    ///
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_clusters(&self.clusters)?;
        anyhow::ensure!(
            !self.poll_interval.is_zero(),
            "nacos: subscribe poll_interval 不能为 0"
        );
        Ok(())
    }
}

impl NacosDiscoveryClient {
    /// 连接注册中心(只建 NamingService,含 HTTP 鉴权)。feature 关 → `Err`;开 → 校验参数后连接。
    ///
    /// # 参数
    /// - `props`: Nacos 连接参数,提供服务端地址、命名空间、默认分组、鉴权和可选对外 IP。
    pub async fn connect(props: &NacosProps) -> anyhow::Result<NacosDiscoveryClient> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = props;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            use nacos_sdk::api::naming::NamingServiceBuilder;
            client::validate_props(props)?;
            tracing::info!(server_addr = %props.server_addr, namespace = %props.namespace, group = %props.group, "connecting nacos naming service");
            let naming = NamingServiceBuilder::new(client::sdk_client_props(props))
                .enable_auth_plugin_http()
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("nacos: build NamingService failed: {e}"))?;
            Ok(NacosDiscoveryClient {
                naming,
                group: props.group.clone(),
                discovery_ip: normalize_discovery_ip(props.discovery_ip.as_deref()),
            })
        }
    }

    /// 注册本实例(临时实例由 SDK 自动维持心跳);返回的 [`RegistrationGuard`]。
    /// [`RegistrationGuard`] drop 时只做 best-effort deregister;优雅停机必须显式 `deregister().await`。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要注册到 Nacos 的服务名。
    /// - `inst`: 本节点实例信息,包含 IP、端口、权重、健康和 metadata。
    pub async fn register(
        &self,
        service: &str,
        inst: Instance,
    ) -> anyhow::Result<RegistrationGuard> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (service, inst);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_service(service)?;
            let inst = apply_discovery_ip(self.discovery_ip.as_deref(), inst);
            validate_instance(&inst)?;
            // 门面层兜底告警:最终对外 IP 是未指定地址 → 消费者拨不通(直接用门面、没有 app 侧检查的服务也能收到提醒)。
            if is_unspecified_ip(&inst.ip) {
                tracing::warn!(
                    service,
                    ip = %inst.ip,
                    "nacos: 注册的对外 IP 是未指定地址(0.0.0.0/::),消费者将无法拨号;请设置 NacosProps.discovery_ip 为可路由地址"
                );
            }
            let si = to_sdk(&inst);
            let group = Some(self.group.clone());
            self.naming
                .register_instance(service.to_string(), group.clone(), si.clone())
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "nacos: register {service} {}:{} failed: {e}",
                        inst.ip,
                        inst.port
                    )
                })?;
            tracing::info!(service, ip = %inst.ip, port = inst.port, ephemeral = inst.ephemeral, "nacos instance registered");
            Ok(RegistrationGuard {
                naming: self.naming.clone(),
                service: service.to_string(),
                group,
                inst: si,
                active: true,
                handle: tokio::runtime::Handle::current(),
            })
        }
    }

    /// 返回本 client 配置的注册中心对外 IP。为空表示由调用方传入的 `Instance.ip` 决定。
    pub fn discovery_ip(&self) -> Option<&str> {
        #[cfg(not(feature = "nacos"))]
        {
            None
        }
        #[cfg(feature = "nacos")]
        {
            self.discovery_ip.as_deref()
        }
    }

    /// 查询某服务**当前可承载流量的实例**:共享层用 [`nadisc::is_traffic_instance`] 统一过滤
    /// (启用 + 健康 + 权重正且有限 + ip 非空 + port≠0),不依赖 SDK 行为。即客户端负载均衡的"可用实例集"。
    /// 要含不健康/禁用/零权重实例做管理/诊断,用 [`discover_all`](Self::discover_all)。不自动订阅、全 cluster。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要查询的 Nacos 服务名。
    pub async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = service;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            self.discover_core(service, Vec::new()).await
        }
    }

    /// 同 [`discover`](Self::discover),但只查指定 cluster(空 = 全部)。注意 cluster 不是 group:
    /// 默认 group 常见为 `DEFAULT_GROUP`,默认 cluster 是 `DEFAULT`,且 cluster 不允许 `_`。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要查询的 Nacos 服务名。
    /// - `clusters`: 要限制的 cluster 名称集合;空集合表示全部 cluster。
    pub async fn discover_with_clusters(
        &self,
        service: &str,
        clusters: impl IntoIterator<Item = impl Into<String>>,
    ) -> anyhow::Result<Vec<Instance>> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = service;
            let _ = clusters.into_iter().count();
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            let cl: Vec<String> = clusters.into_iter().map(Into::into).collect();
            self.discover_core(service, cl).await
        }
    }

    /// 查询 discover core 信息；用于获取服务和实例快照。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `clusters`: 服务实例所属集群列表。
    #[cfg(feature = "nacos")]
    async fn discover_core(
        &self,
        service: &str,
        clusters: Vec<String>,
    ) -> anyhow::Result<Vec<Instance>> {
        validate_service(service)?;
        validate_clusters(&clusters)?;
        let group = Some(self.group.clone());
        let list = self
            .naming
            .select_instances(service.to_string(), group, clusters, false, true)
            .await
            .map_err(|e| anyhow::anyhow!("nacos: discover {service} failed: {e}"))?;
        // 共享层统一用 is_traffic_instance 过滤(不靠 SDK 行为),保证 discover 只吐"可承载流量"实例。
        Ok(list
            .iter()
            .filter_map(try_from_sdk)
            .filter(is_traffic_instance)
            .collect())
    }

    /// 查询某服务**全部实例**(含 `healthy=false` / `enabled=false` / `weight=0`;全 cluster,不自动订阅)。供管理面/调试/灰度排查;
    /// 客户端负载均衡请用 [`discover`](Self::discover)。每个实例的 `healthy`/`enabled`/`weight` 如实反映注册中心状态。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要查询的 Nacos 服务名。
    pub async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = service;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            self.discover_all_core(service, Vec::new()).await
        }
    }

    /// 同 [`discover_all`](Self::discover_all),但只查指定 cluster(空 = 全部)。注意 cluster 不是 group:
    /// 默认 group 常见为 `DEFAULT_GROUP`,默认 cluster 是 `DEFAULT`,且 cluster 不允许 `_`。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要查询的 Nacos 服务名。
    /// - `clusters`: 要限制的 cluster 名称集合;空集合表示全部 cluster。
    pub async fn discover_all_with_clusters(
        &self,
        service: &str,
        clusters: impl IntoIterator<Item = impl Into<String>>,
    ) -> anyhow::Result<Vec<Instance>> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = service;
            let _ = clusters.into_iter().count();
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            let cl: Vec<String> = clusters.into_iter().map(Into::into).collect();
            self.discover_all_core(service, cl).await
        }
    }

    /// 查询 discover all core 信息；用于获取服务和实例快照。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `clusters`: 服务实例所属集群列表。
    #[cfg(feature = "nacos")]
    async fn discover_all_core(
        &self,
        service: &str,
        clusters: Vec<String>,
    ) -> anyhow::Result<Vec<Instance>> {
        validate_service(service)?;
        validate_clusters(&clusters)?;
        let group = Some(self.group.clone());
        let list = self
            .naming
            .get_all_instances(service.to_string(), group, clusters, false)
            .await
            .map_err(|e| anyhow::anyhow!("nacos: discover_all {service} failed: {e}"))?;
        // discover_all 不做流量过滤:如实返回全部(含 healthy=false / enabled=false / weight=0),供管理/诊断。
        Ok(list.iter().filter_map(try_from_sdk).collect())
    }

    /// 列出当前 group 下【一页】服务名 + 总数(`page_no` 从 1 起;供自定义分页)。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `page_no`: 页码,从 1 开始。
    /// - `page_size`: 每页服务名数量,必须大于 0。
    pub async fn list_services_page(
        &self,
        page_no: i32,
        page_size: i32,
    ) -> anyhow::Result<(Vec<String>, i32)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (page_no, page_size);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_page(page_no, page_size)?;
            self.naming
                .get_service_list(page_no, page_size, Some(self.group.clone()))
                .await
                .map_err(|e| anyhow::anyhow!("nacos: list_services page {page_no} failed: {e}"))
        }
    }

    /// 列出当前 group 下【全部】服务名(循环分页直到取满 total;去重保序,保留 Nacos 原始服务名)。
    /// 供 RestDiscovery 等上层构建服务索引(大小写归一化在上层做)。feature 关 → `Err`。
    pub async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        #[cfg(not(feature = "nacos"))]
        {
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            const PAGE_SIZE: i32 = 100;
            let mut all: Vec<String> = Vec::new();
            let mut page_no = 1;
            let mut max_pages = 1;
            loop {
                let (names, total) = self.list_services_page(page_no, PAGE_SIZE).await?;
                if page_no == 1 {
                    max_pages = max_pages_from_total(total, PAGE_SIZE);
                }
                let got = names.len();
                all.extend(names);
                // 取满 total 或某页空 → 结束(空页兜底防 total 不一致时死循环)。
                if got == 0 || all.len() as i32 >= total || page_no >= max_pages {
                    break;
                }
                page_no += 1;
            }
            Ok(dedup_preserve_order(all))
        }
    }

    /// **【低层原始 SDK 事件入口,不适合客户端负载均衡】** 订阅某服务实例变化(全 cluster):每次推送把回调 callback 一遍(原始事件实例列表)。
    /// 返回 [`SubscribeGuard`];drop 只做 best-effort 取消订阅,也可显式 `guard.unsubscribe().await`。feature 关 → `Err`。
    ///
    /// ⚠️ **SDK 限制**(已核实 nacos-sdk 0.8 源码):① 订阅【最后一个实例被删 → 变空】的事件会被 SDK 的 empty-push 过滤掉
    /// (`is_empty_or_error_push`:`hosts.is_none()`),callback **收不到"变空"**;② SDK 的 `subscribe` 内部吞掉错误
    /// (`let _ = subscribe_async(..).await; Ok(())`),本方法返回 `Ok` 不保证订阅真的建立。
    /// → **客户端负载均衡请用 [`subscribe_channel`](Self::subscribe_channel)**:它用 discover 轮询兜底,能可靠反映"变空"与漏推;
    ///    或自行定期 [`discover`](Self::discover) 校正。本 callback 版仅作低层原始事件入口。
    ///
    /// # 参数
    /// - `service`: 要订阅的 Nacos 服务名。
    /// - `on_change`: SDK 推送实例变化时执行的同步回调。
    pub async fn subscribe<F>(&self, service: &str, on_change: F) -> anyhow::Result<SubscribeGuard>
    where
        F: Fn(Vec<Instance>) + Send + Sync + 'static,
    {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (service, on_change);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            self.subscribe_core(service, Vec::new(), on_change).await
        }
    }

    /// 同 [`subscribe`](Self::subscribe),但只订阅指定 cluster(空 = 全部)。注意 cluster 不是 group:
    /// 默认 group 常见为 `DEFAULT_GROUP`,默认 cluster 是 `DEFAULT`,且 cluster 不允许 `_`。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要订阅的 Nacos 服务名。
    /// - `clusters`: 要限制的 cluster 名称集合;空集合表示全部 cluster。
    /// - `on_change`: SDK 推送实例变化时执行的同步回调。
    pub async fn subscribe_with_clusters<F>(
        &self,
        service: &str,
        clusters: impl IntoIterator<Item = impl Into<String>>,
        on_change: F,
    ) -> anyhow::Result<SubscribeGuard>
    where
        F: Fn(Vec<Instance>) + Send + Sync + 'static,
    {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (service, on_change);
            let _ = clusters.into_iter().count();
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            let cl: Vec<String> = clusters.into_iter().map(Into::into).collect();
            self.subscribe_core(service, cl, on_change).await
        }
    }

    /// 建立 subscribe core 监听；用于接收后续变更并保持状态同步。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `clusters`: 服务实例所属集群列表。
    /// - `on_change`: 监听配置或实例列表变化时触发的回调。
    #[cfg(feature = "nacos")]
    async fn subscribe_core<F>(
        &self,
        service: &str,
        clusters: Vec<String>,
        on_change: F,
    ) -> anyhow::Result<SubscribeGuard>
    where
        F: Fn(Vec<Instance>) + Send + Sync + 'static,
    {
        use std::sync::Arc;
        validate_service(service)?;
        validate_clusters(&clusters)?;
        let listener: Arc<dyn nacos_sdk::api::naming::NamingEventListener> =
            Arc::new(EvtListener(on_change));
        let group = Some(self.group.clone());
        self.naming
            .subscribe(
                service.to_string(),
                group.clone(),
                clusters.clone(),
                Arc::clone(&listener),
            )
            .await
            .map_err(|e| anyhow::anyhow!("nacos: subscribe {service} failed: {e}"))?;
        tracing::info!(service, "nacos service subscribed");
        Ok(SubscribeGuard {
            naming: self.naming.clone(),
            service: service.to_string(),
            group,
            clusters,
            listener,
            armed: true,
            handle: tokio::runtime::Handle::current(),
            // 仅 subscribe_channel* 会挂轮询兜底任务;callback 版无
            poll: None,
        })
    }

    /// channel 变体:返回 [`SubscribeGuard`] + `watch::Receiver<Vec<Instance>>`,便于 app 用 `select!` 统一处理。
    /// **顺序:先 subscribe 建监听,再 discover 播种当前(健康)快照**——避免"订阅前已有实例、订阅后无变更"时 Receiver 长期空列表
    /// (把"无实例"和"还没收到事件"混成一种状态)。返回前已把初值设为当前快照。guard 与 rx 都要持有。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要订阅并维护健康实例快照的 Nacos 服务名。
    pub async fn subscribe_channel(
        &self,
        service: &str,
    ) -> anyhow::Result<(SubscribeGuard, tokio::sync::watch::Receiver<Vec<Instance>>)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = service;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            self.subscribe_channel_core(service, SubscribeOptions::default())
                .await
        }
    }

    /// 同 [`subscribe_channel`](Self::subscribe_channel),但只订阅指定 cluster(空 = 全部)。注意 cluster 不是 group:
    /// 默认 group 常见为 `DEFAULT_GROUP`,默认 cluster 是 `DEFAULT`,且 cluster 不允许 `_`。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要订阅并维护健康实例快照的 Nacos 服务名。
    /// - `clusters`: 要限制的 cluster 名称集合;空集合表示全部 cluster。
    pub async fn subscribe_channel_with_clusters(
        &self,
        service: &str,
        clusters: impl IntoIterator<Item = impl Into<String>>,
    ) -> anyhow::Result<(SubscribeGuard, tokio::sync::watch::Receiver<Vec<Instance>>)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = service;
            let _ = clusters.into_iter().count();
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            let cl: Vec<String> = clusters.into_iter().map(Into::into).collect();
            self.subscribe_channel_core(service, SubscribeOptions::new().with_clusters(cl))
                .await
        }
    }

    /// 同 [`subscribe_channel`](Self::subscribe_channel),但可配置 cluster 过滤 + **轮询兜底间隔**([`SubscribeOptions`])。
    /// 注意 cluster 不是 group:默认 group 常见为 `DEFAULT_GROUP`,默认 cluster 是 `DEFAULT`,且 cluster 不允许 `_`。
    /// 低延迟场景调小 `poll_interval`(删实例/漏推的最终一致延迟随之降低),压低 Nacos 压力调大。feature 关 → `Err`。
    ///
    /// # 参数
    /// - `service`: 要订阅并维护健康实例快照的 Nacos 服务名。
    /// - `options`: 订阅选项,包含 cluster 过滤和 discover 轮询兜底间隔。
    pub async fn subscribe_channel_with_options(
        &self,
        service: &str,
        options: SubscribeOptions,
    ) -> anyhow::Result<(SubscribeGuard, tokio::sync::watch::Receiver<Vec<Instance>>)> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = (service, options);
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            self.subscribe_channel_core(service, options).await
        }
    }

    /// 建立 subscribe channel core 监听；用于接收后续变更并保持状态同步。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `options`: 运行选项,用于控制客户端或调度器行为。
    #[cfg(feature = "nacos")]
    async fn subscribe_channel_core(
        &self,
        service: &str,
        options: SubscribeOptions,
    ) -> anyhow::Result<(SubscribeGuard, tokio::sync::watch::Receiver<Vec<Instance>>)> {
        use std::sync::Arc;
        // channel 变体的可靠性关键:nacos-sdk 对【空/无效推送】(最后一个实例被删 → hosts 为空)会过滤掉,
        // 订阅 callback **收不到"变空"事件**(实测确认 + SDK service_info_observable::is_empty_or_error_push)。
        // 故 channel 用【discover 轮询】作真相源:SDK 事件仅作低延迟"唤醒"信号(notify),由轮询统一 discover(healthy)→ 推快照;
        // 这样无论加/删(含删到空)都会被反映,且 channel 值始终是一致的"健康实例快照"(比较前归一化排序,避免顺序抖动伪变更)。
        validate_service(service)?;
        // poll_interval=0 会让后台 tokio::time::interval panic(且发生在 spawn 任务里,调用方已拿到 Ok → 轮询兜底静默失效)。
        // SubscribeOptions 自身 fail-fast,并统一校验 cluster 名称。
        options.validate()?;
        let SubscribeOptions {
            clusters,
            poll_interval,
            ..
        } = options;
        let (tx, rx) = tokio::sync::watch::channel(Vec::new());
        let wake = Arc::new(tokio::sync::Notify::new());
        let wake_cb = Arc::clone(&wake);
        // SDK 订阅:回调只唤醒轮询(忽略其原始实例列表,避免"全实例 vs 健康"口径不一致)。
        let mut guard = self
            .subscribe_with_clusters(service, clusters.clone(), move |_v| wake_cb.notify_one())
            .await?;
        // 播种当前健康快照(归一化后;返回前 Receiver 即最新)。
        // 订阅已建立:若播种 discover 失败,显式 unsubscribe 清理(不只靠 Drop best-effort),再返回原始错误。
        let mut seed = match self.discover_with_clusters(service, clusters.clone()).await {
            Ok(seed) => seed,
            Err(e) => {
                if let Err(ce) = guard.unsubscribe().await {
                    tracing::warn!(service, error = %ce, "nacos: subscribe_channel 播种失败,unsubscribe 清理也失败");
                }
                return Err(e);
            }
        };
        sort_instances(&mut seed);
        let _ = tx.send(seed.clone());
        // 轮询兜底任务:被 SDK 事件唤醒或每 poll_interval 触发,discover→归一化→有变化才推。
        let naming = self.naming.clone();
        let group = Some(self.group.clone());
        let svc = service.to_string();
        let poll = tokio::spawn(async move {
            let mut last = seed;
            let mut ticker = tokio::time::interval(poll_interval);
            // 吃掉立即触发的首拍(初值已播种)
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = wake.notified() => {}
                    _ = ticker.tick() => {}
                }
                match naming
                    .select_instances(svc.clone(), group.clone(), clusters.clone(), false, true)
                    .await
                {
                    Ok(list) => {
                        // 与 discover 同口径:只保留可承载流量的实例(is_traffic_instance)。
                        let mut insts: Vec<Instance> = list
                            .iter()
                            .filter_map(try_from_sdk)
                            .filter(is_traffic_instance)
                            .collect();
                        // 归一化后再比较,避免顺序抖动伪变更
                        sort_instances(&mut insts);
                        if insts != last {
                            last = insts.clone();
                            // 接收端全 drop → 退出
                            if tx.send(insts).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => tracing::debug!("nacos subscribe poll discover failed: {e}"),
                }
            }
        });
        guard.poll = Some(poll.abort_handle());
        Ok((guard, rx))
    }
}

/// 注册句柄:drop 做 best-effort deregister(SIGTERM/panic 兜底,防僵尸实例)。
/// 优雅停机推荐显式 `deregister().await`(确定性、可等结果);Drop 是兜底(用创建时保存的 runtime handle 投递),
/// 运行时关闭阶段不保证一定完成。
#[must_use = "register 返回的 guard 必须持有到进程结束;drop 只会 best-effort 注销,优雅停机请显式 deregister().await"]
pub struct RegistrationGuard {
    #[cfg(feature = "nacos")]
    naming: nacos_sdk::api::naming::NamingService,
    #[cfg(feature = "nacos")]
    service: String,
    #[cfg(feature = "nacos")]
    group: Option<String>,
    #[cfg(feature = "nacos")]
    inst: nacos_sdk::api::naming::ServiceInstance,
    #[cfg(feature = "nacos")]
    active: bool,
    #[cfg(feature = "nacos")]
    handle: tokio::runtime::Handle,
}

impl RegistrationGuard {
    /// 原位刷新同一个已注册实例的权重、健康状态或 metadata，不先注销，避免配置换版造成注册表闪断。
    ///
    /// `service/group/ip/port/cluster/ephemeral` 是注册身份，必须保持不变；身份变化仍应显式注销旧
    /// guard 后重新 [`NacosDiscoveryClient::register`]。刷新成功后同步替换 guard 内保存的实例，
    /// 后续显式注销或 Drop 兜底会使用最新 payload。
    ///
    /// # 参数
    /// - `inst`: 与当前注册身份相同的新实例 payload。
    #[allow(unused_mut, unused_variables)]
    pub async fn refresh(&mut self, inst: Instance) -> anyhow::Result<()> {
        #[cfg(not(feature = "nacos"))]
        {
            let _ = inst;
            anyhow::bail!(client::DISABLED_MSG);
        }
        #[cfg(feature = "nacos")]
        {
            validate_instance(&inst)?;
            let next = to_sdk(&inst);
            anyhow::ensure!(
                self.inst.ip == next.ip
                    && self.inst.port == next.port
                    && self.inst.cluster_name == next.cluster_name
                    && self.inst.ephemeral == next.ephemeral,
                "nacos: refresh 只允许更新同一注册身份的 payload"
            );
            self.naming
                .register_instance(self.service.clone(), self.group.clone(), next.clone())
                .await
                .map_err(|e| anyhow::anyhow!("nacos: refresh {} failed: {e}", self.service))?;
            self.inst = next;
            tracing::debug!(service = %self.service, "nacos instance registration refreshed in place");
            Ok(())
        }
    }

    /// 显式注销(优雅路径:可 await 等结果)。**先注销成功**再标记不再 Drop 兜底;失败时 `active` 仍为 true,
    /// self drop 时还会走一次 best-effort 注销(不至于因一次失败彻底关掉兜底)。
    /// ⚠️ 优雅停机若担心 Nacos 半开/卡住,用 `tokio::time::timeout(d, guard.deregister())` 包裹:超时丢弃本 future →
    ///    self 随之 drop → Drop 的 best-effort 投递接力(非阻塞),不会拖死停机流程。
    ///
    #[allow(unused_mut)]
    pub async fn deregister(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            self.naming
                .deregister_instance(self.service.clone(), self.group.clone(), self.inst.clone())
                .await
                .map_err(|e| anyhow::anyhow!("nacos: deregister {} failed: {e}", self.service))?;
            self.active = false;
            tracing::info!(service = %self.service, "nacos instance deregistered");
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for RegistrationGuard {
    /// 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let (naming, service, group, inst) = (
            self.naming.clone(),
            self.service.clone(),
            self.group.clone(),
            self.inst.clone(),
        );
        self.handle.spawn(async move {
            if let Err(e) = naming.deregister_instance(service, group, inst).await {
                tracing::warn!("nacos deregister on drop failed: {e}");
            }
        });
    }
}

#[cfg(feature = "nacos")]
impl std::fmt::Debug for RegistrationGuard {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationGuard")
            .field("service", &self.service)
            .field("group", &self.group)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

#[cfg(not(feature = "nacos"))]
impl std::fmt::Debug for RegistrationGuard {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationGuard").finish_non_exhaustive()
    }
}

/// 订阅句柄:drop 做 best-effort 取消订阅;也可显式 [`unsubscribe`](Self::unsubscribe)。
/// 优雅路径请显式 `unsubscribe().await`;Drop 只做 best-effort 兜底,运行时关闭阶段不保证一定完成。
#[must_use = "subscribe 返回的 guard 必须持有;drop 只会 best-effort 取消订阅,优雅路径请显式 unsubscribe().await"]
pub struct SubscribeGuard {
    #[cfg(feature = "nacos")]
    naming: nacos_sdk::api::naming::NamingService,
    #[cfg(feature = "nacos")]
    service: String,
    #[cfg(feature = "nacos")]
    group: Option<String>,
    #[cfg(feature = "nacos")]
    clusters: Vec<String>,
    #[cfg(feature = "nacos")]
    listener: std::sync::Arc<dyn nacos_sdk::api::naming::NamingEventListener>,
    #[cfg(feature = "nacos")]
    armed: bool,
    #[cfg(feature = "nacos")]
    handle: tokio::runtime::Handle,
    /// channel 变体的 discover 轮询兜底任务句柄;Drop/unsubscribe 时 abort。callback 版为 None。
    #[cfg(feature = "nacos")]
    poll: Option<tokio::task::AbortHandle>,
}

impl SubscribeGuard {
    /// 显式取消订阅(优雅路径:可 await)。成功后关闭 Drop 兜底;失败保留 Drop 兜底。
    /// ⚠️ 担心卡住同 [`RegistrationGuard::deregister`]:用 `tokio::time::timeout` 包裹,超时由 Drop best-effort 接力。
    ///
    #[allow(unused_mut)]
    pub async fn unsubscribe(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            // 先停轮询兜底任务
            if let Some(h) = self.poll.take() {
                h.abort();
            }
            self.naming
                .unsubscribe(
                    self.service.clone(),
                    self.group.clone(),
                    self.clusters.clone(),
                    std::sync::Arc::clone(&self.listener),
                )
                .await
                .map_err(|e| anyhow::anyhow!("nacos: unsubscribe {} failed: {e}", self.service))?;
            self.armed = false;
            tracing::info!(service = %self.service, "nacos service unsubscribed");
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for SubscribeGuard {
    /// 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        // 停轮询兜底任务(无论 armed 与否)
        if let Some(h) = &self.poll {
            h.abort();
        }
        if !self.armed {
            return;
        }
        let (naming, service, group, clusters, listener) = (
            self.naming.clone(),
            self.service.clone(),
            self.group.clone(),
            self.clusters.clone(),
            std::sync::Arc::clone(&self.listener),
        );
        self.handle.spawn(async move {
            if let Err(e) = naming.unsubscribe(service, group, clusters, listener).await {
                tracing::warn!("nacos unsubscribe on drop failed: {e}");
            }
        });
    }
}

#[cfg(feature = "nacos")]
impl std::fmt::Debug for SubscribeGuard {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscribeGuard")
            .field("service", &self.service)
            .field("group", &self.group)
            .field("clusters", &self.clusters)
            .field("armed", &self.armed)
            .finish_non_exhaustive()
    }
}

#[cfg(not(feature = "nacos"))]
impl std::fmt::Debug for SubscribeGuard {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscribeGuard").finish_non_exhaustive()
    }
}

/// 把用户回调包成 SDK 的 `NamingEventListener`:实例列表变化时把最新列表交给回调(端口越界的实例跳过)。
#[cfg(feature = "nacos")]
struct EvtListener<F: Fn(Vec<Instance>) + Send + Sync + 'static>(F);

#[cfg(feature = "nacos")]
impl<F: Fn(Vec<Instance>) + Send + Sync + 'static> nacos_sdk::api::naming::NamingEventListener
    for EvtListener<F>
{
    /// 接收实例变更事件并发布最新快照；用于驱动服务订阅刷新。
    ///
    /// # 参数
    /// - `event`: 注册中心推送的服务实例变更事件。
    fn event(&self, event: std::sync::Arc<nacos_sdk::api::naming::NamingChangeEvent>) {
        let insts = event
            .instances
            .as_ref()
            .map(|v| v.iter().filter_map(try_from_sdk).collect())
            .unwrap_or_default();
        (self.0)(insts);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 实现 discovery 的 provider-neutral 接口:让 RestDiscovery 等面向 `nadisc::DiscoveryClient`
// 抽象使用本后端(`Arc<NacosDiscoveryClient> as Arc<dyn DiscoveryClient>`),不绑定 nacos 具体类型。
// 内部一律走【inherent 同名方法】(用 `NacosDiscoveryClient::xxx(self, ..)` 显式消歧,避免 trait 自递归)。
// 这些 inherent 方法两 feature 态都在(stub 下运行期 bail),故 impl 不需 cfg-gate。
// ════════════════════════════════════════════════════════════════════════════

#[async_trait::async_trait]
impl nadisc::DiscoveryClient for NacosDiscoveryClient {
    /// 查询 list services 信息；用于获取服务和实例快照。
    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        NacosDiscoveryClient::list_services(self).await
    }

    /// 查询 discover 信息；用于获取服务和实例快照。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        NacosDiscoveryClient::discover(self, service).await
    }

    /// 查询 discover all 信息；用于获取服务和实例快照。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        NacosDiscoveryClient::discover_all(self, service).await
    }

    /// 建立 watch with options 监听；用于接收后续变更并保持状态同步。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `options`: 运行选项,用于控制客户端或调度器行为。
    async fn watch_with_options(
        &self,
        service: &str,
        options: nadisc::WatchOptions,
    ) -> anyhow::Result<nadisc::ServiceWatch> {
        options.validate()?;
        let (guard, receiver) = self
            .subscribe_channel_with_options(
                service,
                SubscribeOptions::new().with_poll_interval(options.poll_interval),
            )
            .await?;
        Ok(nadisc::ServiceWatch {
            guard: Box::new(guard),
            receiver,
        })
    }
}

#[async_trait::async_trait]
impl nadisc::ServiceWatchGuard for SubscribeGuard {
    /// 建立 unsubscribe 监听；用于接收后续变更并保持状态同步。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        SubscribeGuard::unsubscribe(*self).await
    }
}

#[async_trait::async_trait]
impl nadisc::ServiceRegistry for NacosDiscoveryClient {
    /// 执行 register 操作；用于维护服务实例生命周期。
    ///
    /// # 参数
    /// - `service`: 服务名,用于服务发现或注册中心查询。
    /// - `instance`: 需要注册、转换或写入索引的服务实例。
    async fn register(
        &self,
        service: &str,
        instance: Instance,
    ) -> anyhow::Result<Box<dyn nadisc::Registration>> {
        let guard = NacosDiscoveryClient::register(self, service, instance).await?;
        Ok(Box::new(guard))
    }
}

#[async_trait::async_trait]
impl nadisc::Registration for RegistrationGuard {
    /// 执行 deregister 操作；用于维护服务实例生命周期。
    async fn deregister(self: Box<Self>) -> anyhow::Result<()> {
        RegistrationGuard::deregister(*self).await
    }
}
