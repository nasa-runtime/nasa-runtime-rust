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
    /// 业务作用：展示发现客户端的默认分组和对外 IP 配置。
    ///
    /// 参数说明：
    /// - `f`：接收结构化展示内容的格式化器。
    ///
    /// 返回：输出配置摘要；写出失败时返回格式化错误，不展示 SDK 连接内部状态。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("NacosDiscoveryClient");
        #[cfg(feature = "nacos")]
        s.field("group", &self.group)
            .field("discovery_ip", &self.discovery_ip.as_deref().unwrap_or(""));
        s.finish_non_exhaustive()
    }
}

/// 业务作用：在访问注册中心前拒绝空白服务名及首尾空白。
///
/// 参数说明：
/// - `service`：保留原始大小写的 Nacos 服务名。
///
/// 返回：非空且无首尾空白时成功；否则返回错误，不发起网络请求。
#[cfg(feature = "nacos")]
fn validate_service(service: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!service.trim().is_empty(), "nacos: service 不能为空");
    client::ensure_no_outer_ws("service", service)?;
    Ok(())
}

/// 业务作用：限制服务名分页的页码与单页规模，避免非法或过大的查询。
///
/// 参数说明：
/// - `page_no`：从 1 开始的页码。
/// - `page_size`：每页数量，允许 1 至 1000。
///
/// 返回：分页参数处于允许范围时成功；否则返回错误。
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

/// 业务作用：拒绝空集群名、首尾空白和当前集群标识不支持的字符。
///
/// 参数说明：
/// - `field`：错误中标识该集群参数的位置名称。
/// - `cluster`：仅允许 ASCII 字母、数字、连字符和点的集群名称。
///
/// 返回：名称符合约束时成功；否则返回定位到该参数的错误。
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

/// 业务作用：在注册中心调用前逐项验证集群过滤条件。
///
/// 参数说明：
/// - `clusters`：待验证的集群名称列表；空列表表示不限制集群。
///
/// 返回：全部名称合法或列表为空时成功；遇到首个非法名称返回错误。
fn validate_clusters(clusters: &[String]) -> anyhow::Result<()> {
    for cluster in clusters {
        validate_cluster_name("clusters[]", cluster)?;
    }
    Ok(())
}

/// 业务作用：在注册或刷新前检查实例地址、端口、权重和集群标识。
///
/// 参数说明：
/// - `inst`：准备提交的实例；零权重表示不承载流量。
///
/// 返回：IP 非空且无首尾空白、端口非零、权重非负且有限、可选集群名合法时成功；不验证地址是否可拨通，失败时返回错误。
#[cfg(feature = "nacos")]
fn validate_instance(inst: &Instance) -> anyhow::Result<()> {
    anyhow::ensure!(!inst.ip.trim().is_empty(), "nacos: instance ip 不能为空");
    client::ensure_no_outer_ws("instance.ip", &inst.ip)?;
    anyhow::ensure!(inst.port != 0, "nacos: instance port 不能为 0");
    // weight 允许为 0，表示实例已注册但不承载流量，用于平滑摘流或预注册。
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

/// 业务作用：将中性实例转换为 Nacos 注册载荷，保留流量属性和元数据。
///
/// 参数说明：
/// - `inst`：由调用方负责校验的实例。
///
/// 返回：返回新的 SDK 实例；服务名和实例 ID 留空，实际服务身份由注册调用提供。
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

/// 业务作用：将 SDK 实例转换为中性实例，避免端口窄化时发生回绕。
///
/// 参数说明：
/// - `si`：注册中心返回的原始实例。
///
/// 返回：端口处于 0 至 65535 时返回实例；越界时记录警告并返回 None，端口 0 和其它非流量实例交由后续过滤。
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

/// 业务作用：按集群、IP 和端口规范实例顺序，减少订阅快照因排列变化产生的通知。
///
/// 参数说明：
/// - `v`：待原位排序的实例切片。
///
/// 返回：原位完成排序；不去重，也不校验实例流量资格。
#[cfg(feature = "nacos")]
fn sort_instances(v: &mut [Instance]) {
    v.sort_by(|a, b| {
        a.cluster_name
            .cmp(&b.cluster_name)
            .then_with(|| a.ip.cmp(&b.ip))
            .then_with(|| a.port.cmp(&b.port))
    });
}

/// 业务作用：规范客户端配置的对外 IP 覆盖值。
///
/// 参数说明：
/// - `raw`：可选的原始对外地址。
///
/// 返回：去除首尾空白后非空则返回新字符串；缺省或空白返回 None，不验证地址可达性。
#[cfg(feature = "nacos")]
fn normalize_discovery_ip(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|ip| !ip.is_empty())
        .map(ToOwned::to_owned)
}

/// 业务作用：在注册前将客户端对外地址覆盖到实例载荷。
///
/// 参数说明：
/// - `discovery_ip`：已规范化的可选地址覆盖值。
/// - `inst`：调用方提供的待注册实例。
///
/// 返回：配置存在时替换 IP 后返回实例；否则保留原 IP，其余字段不变。
#[cfg(feature = "nacos")]
fn apply_discovery_ip(discovery_ip: Option<&str>, mut inst: Instance) -> Instance {
    if let Some(ip) = discovery_ip {
        inst.ip = ip.to_string();
    }
    inst
}

/// 业务作用：识别不适合作为消费者拨号目标的空地址和未指定 IP。
///
/// 参数说明：
/// - `ip`：待判断的实例地址。
///
/// 返回：空白或可解析的未指定 IP 返回 true；其它 IP 和非 IP 字面量返回 false，不证明可达性。
#[cfg(feature = "nacos")]
fn is_unspecified_ip(ip: &str) -> bool {
    let ip = ip.trim();
    ip.is_empty()
        || ip
            .parse::<std::net::IpAddr>()
            .map(|addr| addr.is_unspecified())
            .unwrap_or(false)
}

/// 业务作用：合并分页服务名时保留名称首次出现的次序。
///
/// 参数说明：
/// - `names`：可能跨页重复的原始服务名。
///
/// 返回：返回去重列表，保留大小写及首次出现的原始名称。
#[cfg(feature = "nacos")]
fn dedup_preserve_order(names: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    names
        .into_iter()
        .filter(|n| seen.insert(n.clone()))
        .collect()
}

/// 业务作用：为分页枚举建立基于首批总数的终止上限。
///
/// 参数说明：
/// - `total`：服务端报告的总记录数，负值按零处理。
/// - `page_size`：预期页大小，小于 1 时按 1 处理。
///
/// 返回：返回向上取整且至少为 1 的页数；使用宽整数计算避免加法溢出。
#[cfg(feature = "nacos")]
fn max_pages_from_total(total: i32, page_size: i32) -> i32 {
    let total = i64::from(total.max(0));
    let page_size = i64::from(page_size.max(1));
    ((total + page_size - 1) / page_size).max(1) as i32
}

/// `subscribe_channel_with_options` 的选项:cluster 过滤 + 轮询兜底间隔(默认 5s)。
///
/// `poll_interval` 控制删除最后实例、SDK 漏推或订阅未生效时的查询频率；实际可见延迟还受查询耗时与失败影响:
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
    /// 业务作用：创建不限集群、每五秒轮询一次的实例订阅选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回选项值；不建立订阅，也不发起服务发现。
    fn default() -> Self {
        Self {
            clusters: Vec::new(),
            poll_interval: std::time::Duration::from_secs(5),
        }
    }
}

impl SubscribeOptions {
    /// 业务作用：创建不限集群、每五秒轮询一次的实例订阅选项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回选项值；不建立订阅，也不发起服务发现。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：设置实例订阅的集群过滤条件。
    ///
    /// 参数说明：
    /// - `clusters`：要订阅的集群名称；空集合表示全部集群。
    ///
    /// 返回：返回替换集群列表后的选项；名称合法性延迟到 validate 检查。
    pub fn with_clusters(mut self, clusters: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.clusters = clusters.into_iter().map(Into::into).collect();
        self
    }

    /// 业务作用：设置 SDK 推送之外的服务发现轮询间隔。
    ///
    /// 参数说明：
    /// - `poll_interval`：轮询间隔，使用时必须非零。
    ///
    /// 返回：返回更新后的选项；本方法不校验，也不启动定时任务。
    pub fn with_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// 业务作用：在启动订阅前检查集群过滤条件和轮询间隔。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：名称合法且间隔非零时成功；否则返回错误，防止后台定时器因零间隔展开。
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
    /// 业务作用：建立仅用于注册与发现的 Nacos NamingService，并保存默认分组及对外地址。
    ///
    /// 参数说明：
    /// - `props`：服务端、命名空间、分组、鉴权和可选对外 IP 配置。
    ///
    /// 返回：启用 nacos 且配置校验和 SDK 构建成功时返回客户端；配置或构建失败、特性关闭时返回错误。
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

    /// 业务作用：应用对外 IP 配置并向默认分组注册实例，临时实例的心跳交由 SDK 管理。
    ///
    /// 参数说明：
    /// - `service`：待注册的服务名。
    /// - `inst`：实例地址、端口、流量属性、集群和元数据。
    ///
    /// 返回：SDK 注册成功后返回必须持有的注销句柄；配置或注册失败、特性关闭时返回错误。失败不证明远端未登记；Drop 只尽力投递注销，优雅退出应显式等待 deregister。
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

    /// 业务作用：读取注册时使用的客户端对外地址覆盖配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回已规范化地址的借用；未配置或 nacos 特性关闭时返回 None。
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

    /// 业务作用：查询默认分组中可供客户端负载均衡的实例。
    ///
    /// 参数说明：
    /// - `service`：待查询的服务名。
    ///
    /// 返回：成功时返回可承载流量的实例列表，可以为空；地址、端口和权重等由中性流量规则过滤，不建立订阅。参数非法或 SDK 查询失败时返回错误。 nacos 特性关闭时返回错误。
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

    /// 业务作用：查询默认分组中可供客户端负载均衡的实例。
    ///
    /// 参数说明：
    /// - `service`：待查询的服务名。
    /// - `clusters`：限定的集群名称，空集合表示全部集群。
    ///
    /// 返回：成功时返回可承载流量的实例列表，可以为空；地址、端口和权重等由中性流量规则过滤，不建立订阅。参数非法或 SDK 查询失败时返回错误。 nacos 特性关闭时返回错误。
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

    /// 业务作用：查询默认分组中可供客户端负载均衡的实例。
    ///
    /// 参数说明：
    /// - `service`：待校验并查询的服务名。
    /// - `clusters`：待校验的集群过滤列表，空列表表示全部集群。
    ///
    /// 返回：成功时返回可承载流量的实例列表，可以为空；地址、端口和权重等由中性流量规则过滤，不建立订阅。参数非法或 SDK 查询失败时返回错误。
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

    /// 业务作用：查询默认分组中的实例状态，供管理面观察非流量实例。
    ///
    /// 参数说明：
    /// - `service`：待查询的服务名。
    ///
    /// 返回：成功时返回可转换的全部实例，包括禁用、不健康或零权重实例；端口越界的 SDK 实例被跳过。参数非法或 SDK 查询失败时返回错误，不建立订阅。 nacos 特性关闭时返回错误。
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

    /// 业务作用：查询默认分组中的实例状态，供管理面观察非流量实例。
    ///
    /// 参数说明：
    /// - `service`：待查询的服务名。
    /// - `clusters`：限定的集群名称，空集合表示全部集群。
    ///
    /// 返回：成功时返回可转换的全部实例，包括禁用、不健康或零权重实例；端口越界的 SDK 实例被跳过。参数非法或 SDK 查询失败时返回错误，不建立订阅。 nacos 特性关闭时返回错误。
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

    /// 业务作用：查询默认分组中的实例状态，供管理面观察非流量实例。
    ///
    /// 参数说明：
    /// - `service`：待校验并查询的服务名。
    /// - `clusters`：待校验的集群过滤列表，空列表表示全部集群。
    ///
    /// 返回：成功时返回可转换的全部实例，包括禁用、不健康或零权重实例；端口越界的 SDK 实例被跳过。参数非法或 SDK 查询失败时返回错误，不建立订阅。
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

    /// 业务作用：读取默认分组中的一页服务名及服务端报告的总数。
    ///
    /// 参数说明：
    /// - `page_no`：从 1 开始的页码。
    /// - `page_size`：每页数量，允许 1 至 1000。
    ///
    /// 返回：成功时原样返回名称列表与总数；分页非法、SDK 查询失败或 nacos 特性关闭时返回错误，不保证多页间具有快照一致性。
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

    /// 业务作用：分页枚举默认分组的服务名，供上层建立服务索引。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按首批总数的页数上限、当前总数或空页停止，并对已取名称去重保序；任一页失败或特性关闭时返回错误。服务并发增删时不保证完整一致快照。
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

    /// 业务作用：订阅原始实例事件并同步调用业务回调，供需要 SDK 事件语义的调用方使用。
    ///
    /// 参数说明：
    /// - `service`：待订阅的服务名。
    /// - `on_change`：接收原始实例列表的同步回调，不执行流量过滤。
    ///
    /// 返回：返回持有原始 SDK 监听器的句柄；参数或 SDK 调用失败时返回错误。SDK 可能过滤空推送或内部订阅错误，因此成功不保证监听已在远端生效，也不保证事件能反映删空。 nacos 特性关闭时返回错误。
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

    /// 业务作用：为指定集群保存 SDK 实例监听器及取消订阅所需的身份。
    ///
    /// 参数说明：
    /// - `service`：待订阅的服务名。
    /// - `clusters`：集群过滤列表，空集合表示全部集群。
    /// - `on_change`：收到 SDK 事件时同步调用的实例列表回调。
    ///
    /// 返回：返回持有原始 SDK 监听器的句柄；参数或 SDK 调用失败时返回错误。SDK 可能过滤空推送或内部订阅错误，因此成功不保证监听已在远端生效，也不保证事件能反映删空。 nacos 特性关闭时返回错误。
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

    /// 业务作用：为指定集群保存 SDK 实例监听器及取消订阅所需的身份。
    ///
    /// 参数说明：
    /// - `service`：待订阅的服务名。
    /// - `clusters`：集群过滤列表，空集合表示全部集群。
    /// - `on_change`：收到 SDK 事件时同步调用的实例列表回调。
    ///
    /// 返回：返回持有原始 SDK 监听器的句柄；参数或 SDK 调用失败时返回错误。SDK 可能过滤空推送或内部订阅错误，因此成功不保证监听已在远端生效，也不保证事件能反映删空。
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

    /// 业务作用：以默认轮询选项维护可承载流量的实例快照。
    ///
    /// 参数说明：
    /// - `service`：待订阅并播种当前快照的服务名。
    ///
    /// 返回：成功时返回订阅句柄与已播种的 watch 接收端；后续由事件唤醒或定时查询更新有变化的流量实例快照。查询失败保留旧值，不保证快照时效上限；句柄取消只请求中止轮询，不等待任务退出。初始化失败时尝试退订并返回原错误。 特性关闭时返回错误。
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

    /// 业务作用：为指定集群维护可承载流量的实例快照。
    ///
    /// 参数说明：
    /// - `service`：待订阅的服务名。
    /// - `clusters`：集群过滤列表，空集合表示全部集群。
    ///
    /// 返回：成功时返回订阅句柄与已播种的 watch 接收端；后续由事件唤醒或定时查询更新有变化的流量实例快照。查询失败保留旧值，不保证快照时效上限；句柄取消只请求中止轮询，不等待任务退出。初始化失败时尝试退订并返回原错误。 特性关闭时返回错误。
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

    /// 业务作用：先登记事件监听器，再播种发现快照并启动轮询校正。
    ///
    /// 参数说明：
    /// - `service`：待订阅的服务名。
    /// - `options`：集群过滤和非零轮询间隔。
    ///
    /// 返回：成功时返回订阅句柄与已播种的 watch 接收端；后续由事件唤醒或定时查询更新有变化的流量实例快照。查询失败保留旧值，不保证快照时效上限；句柄取消只请求中止轮询，不等待任务退出。初始化失败时尝试退订并返回原错误。 特性关闭时返回错误。
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

    /// 业务作用：先登记事件监听器，再播种发现快照并启动轮询校正。
    ///
    /// 参数说明：
    /// - `service`：待订阅的服务名。
    /// - `options`：集群过滤和非零轮询间隔。
    ///
    /// 返回：成功时返回订阅句柄与已播种的 watch 接收端；后续由事件唤醒或定时查询更新有变化的流量实例快照。查询失败保留旧值，不保证快照时效上限；句柄取消只请求中止轮询，不等待任务退出。初始化失败时尝试退订并返回原错误。
    #[cfg(feature = "nacos")]
    async fn subscribe_channel_core(
        &self,
        service: &str,
        options: SubscribeOptions,
    ) -> anyhow::Result<(SubscribeGuard, tokio::sync::watch::Receiver<Vec<Instance>>)> {
        use std::sync::Arc;
        // channel 变体的可靠性关键:nacos-sdk 对【空/无效推送】(最后一个实例被删 → hosts 为空)会过滤掉,
        // SDK 的空推送过滤可能使回调收不到删空事件，不能单靠推送维护可路由快照。
        // 故 channel 用【discover 轮询】作真相源:SDK 事件仅作低延迟"唤醒"信号(notify),由轮询统一 discover(healthy)→ 推快照;
        // 成功查询可校正新增与删空；查询失败保留旧快照，排序后比较可减少仅由排列变化造成的通知。
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
        // 在启动后台查询前发布本次初始查询结果，避免接收端把尚未查询误认为确实无实例。
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
    /// 业务作用：在注册身份不变时重新登记流量属性和元数据，避免先注销造成发现空窗。
    ///
    /// 参数说明：
    /// - `inst`：新载荷；IP、端口、集群及临时实例标记必须与当前注册相同。
    ///
    /// 返回：SDK 确认成功后替换句柄保存的载荷；身份变化、非法实例、调用失败或特性关闭时返回错误。失败保留旧本地载荷，但不证明远端没有接受新值。
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
            // 只有 SDK 确认后才替换本地注销载荷；调用失败不能推断远端未接受。
            self.inst = next;
            tracing::debug!(service = %self.service, "nacos instance registration refreshed in place");
            Ok(())
        }
    }

    /// 业务作用：消费注册句柄并显式等待注销，成功后关闭 Drop 兜底。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：SDK 成功时解除本地注销责任；失败或等待被取消时保留 Drop 投递机会，不保证运行时关闭后任务完成。特性关闭的空句柄直接成功。
    #[allow(unused_mut)]
    pub async fn deregister(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            self.naming
                .deregister_instance(self.service.clone(), self.group.clone(), self.inst.clone())
                .await
                .map_err(|e| anyhow::anyhow!("nacos: deregister {} failed: {e}", self.service))?;
            // 远端确认注销后才解除本地责任，失败或取消仍保留 Drop 兜底。
            self.active = false;
            tracing::info!(service = %self.service, "nacos instance deregistered");
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for RegistrationGuard {
    /// 业务作用：为仍承担注册责任的句柄尽力投递注销任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未激活时不操作；否则向创建句柄时的运行时投递，立即返回且不等待结果，任务失败仅记录警告。
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
    /// 业务作用：展示注册句柄的服务、分组和激活状态摘要。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要；格式化写出失败时返回错误，不展开 SDK 连接和监听器内部状态。
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
    /// 业务作用：展示注册句柄的类型摘要。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要；格式化写出失败时返回错误，不展开 SDK 连接和监听器内部状态。
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
    /// 业务作用：消费订阅句柄并停止轮询，再等待 SDK 取消监听。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功后关闭 Drop 兜底；失败或取消等待仍保留尽力退订机会。轮询仅发出中止请求，不等待退出；特性关闭的空句柄直接成功。
    #[allow(unused_mut)]
    pub async fn unsubscribe(mut self) -> anyhow::Result<()> {
        #[cfg(feature = "nacos")]
        {
            // 先请求中止轮询，避免退订期间继续发布快照；abort 不等待在途任务退出。
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
            // SDK 确认退订后才关闭兜底，避免一次失败直接遗失监听清理责任。
            self.armed = false;
            tracing::info!(service = %self.service, "nacos service unsubscribed");
        }
        Ok(())
    }
}

#[cfg(feature = "nacos")]
impl Drop for SubscribeGuard {
    /// 业务作用：请求中止订阅轮询，并为仍持有监听责任的句柄尽力投递退订任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无论是否仍需退订均请求中止轮询；未解除责任时投递退订，不等待完成，失败仅记录警告。
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
    /// 业务作用：展示订阅句柄的服务、分组、集群和待退订状态摘要。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要；格式化写出失败时返回错误，不展开 SDK 连接和监听器内部状态。
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
    /// 业务作用：展示订阅句柄的类型摘要。
    ///
    /// 参数说明：
    /// - `f`：接收摘要的格式化器。
    ///
    /// 返回：输出摘要；格式化写出失败时返回错误，不展开 SDK 连接和监听器内部状态。
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
    /// 业务作用：把 SDK 实例事件交给同步回调，保留原始事件的非流量状态。
    ///
    /// 参数说明：
    /// - `event`：SDK 推送的实例变更事件。
    ///
    /// 返回：实例缺省时传递空列表，越界端口被跳过；同步执行回调，不排序、不执行流量过滤，回调展开会向上传播。
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
    /// 业务作用：通过中性发现接口转发服务名枚举。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按首批总数的页数上限、当前总数或空页停止，并对已取名称去重保序；任一页失败或特性关闭时返回错误。服务并发增删时不保证完整一致快照。
    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        NacosDiscoveryClient::list_services(self).await
    }

    /// 业务作用：通过中性发现接口查询可承载流量的实例。
    ///
    /// 参数说明：
    /// - `service`：待查询的服务名。
    ///
    /// 返回：成功时返回可承载流量的实例列表，可以为空；地址、端口和权重等由中性流量规则过滤，不建立订阅。参数非法或 SDK 查询失败时返回错误。 特性关闭时返回错误。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        NacosDiscoveryClient::discover(self, service).await
    }

    /// 业务作用：通过中性发现接口查询包含非流量状态的全部实例。
    ///
    /// 参数说明：
    /// - `service`：待查询的服务名。
    ///
    /// 返回：成功时返回可转换的全部实例，包括禁用、不健康或零权重实例；端口越界的 SDK 实例被跳过。参数非法或 SDK 查询失败时返回错误，不建立订阅。 特性关闭时返回错误。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        NacosDiscoveryClient::discover_all(self, service).await
    }

    /// 业务作用：把中性监听选项转换为 Nacos 轮询订阅并封装生命周期句柄。
    ///
    /// 参数说明：
    /// - `service`：待监听的服务名。
    /// - `options`：中性监听选项；经校验后使用其中的轮询间隔。
    ///
    /// 返回：成功时返回已播种的接收端与装箱句柄；选项校验或初始化失败时返回错误，后续发现失败保留旧快照。
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
    /// 业务作用：消费中性监听句柄并委托具体 Nacos 退订流程。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回显式退订结果；轮询请求中止但不等待退出，失败仍由具体句柄保留 Drop 兜底。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        SubscribeGuard::unsubscribe(*self).await
    }
}

#[async_trait::async_trait]
impl nadisc::ServiceRegistry for NacosDiscoveryClient {
    /// 业务作用：通过中性注册接口登记 Nacos 实例并装箱注销句柄。
    ///
    /// 参数说明：
    /// - `service`：待注册的服务名。
    /// - `instance`：待注册的实例载荷。
    ///
    /// 返回：SDK 注册成功后返回必须持有的注销句柄；配置或注册失败、特性关闭时返回错误。失败不证明远端未登记；Drop 只尽力投递注销，优雅退出应显式等待 deregister。
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
    /// 业务作用：消费中性注册句柄并等待具体 Nacos 注销流程。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回注销结果；失败或取消等待时具体句柄仍尽力投递注销，不保证远端已删除。
    async fn deregister(self: Box<Self>) -> anyhow::Result<()> {
        RegistrationGuard::deregister(*self).await
    }
}
