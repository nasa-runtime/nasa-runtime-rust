use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use crate::{
    is_traffic_instance, DiscoveryClient, Instance, ServiceWatch, ServiceWatchGuard, WatchOptions,
};

type Watchers = HashMap<String, HashMap<u64, tokio::sync::watch::Sender<Vec<Instance>>>>;

/// 静态发现表及其订阅者的共享可变状态；替换服务与注册 watch 采用固定锁序。
struct StaticInner {
    services: RwLock<BTreeMap<String, Vec<Instance>>>,
    watchers: Mutex<Watchers>,
    next_watcher: AtomicU64,
}

/// 可原子替换、失败保 last-good 的静态服务发现 provider。
#[derive(Clone)]
pub struct StaticDiscovery {
    inner: Arc<StaticInner>,
}

impl StaticDiscovery {
    /// 业务作用：从完整服务表创建可替换的静态发现后端。
    ///
    /// 参数说明：
    /// - `services`：服务名到完整实例列表的静态映射，可包含空服务列表。
    ///
    /// 返回：全部名称、端点、权重及同服务地址唯一性有效时返回后端；否则整体拒绝，不发布部分表。
    pub fn new(services: BTreeMap<String, Vec<Instance>>) -> anyhow::Result<Self> {
        validate_services(&services)?;
        Ok(Self {
            inner: Arc::new(StaticInner {
                services: RwLock::new(services),
                watchers: Mutex::new(HashMap::new()),
                next_watcher: AtomicU64::new(1),
            }),
        })
    }

    /// 业务作用：在固定锁序下替换单个服务并同步发布其流量投影。
    ///
    /// 参数说明：
    /// - `service`：要替换或新建的服务名。
    /// - `instances`：该服务新的完整实例列表。
    ///
    /// 返回：候选合法时更新快照并通知当前订阅者；非法时保留旧表和旧通知状态，不等待订阅者消费。
    pub fn replace(
        &self,
        service: impl Into<String>,
        instances: Vec<Instance>,
    ) -> anyhow::Result<()> {
        let service = service.into();
        validate_service_name(&service)?;
        validate_instances(&instances)?;
        let traffic = traffic_instances(&instances);
        // services→watchers 固定锁序，与 watch 注册共用同一线性化窗口，避免“读到旧初值后恰好错过
        // replace 推送”的订阅丢更新竞态。
        let mut services = self
            .inner
            .services
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        services.insert(service.clone(), instances);
        let mut watchers = self
            .inner
            .watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(service_watchers) = watchers.get_mut(&service) {
            service_watchers.retain(|_, sender| sender.send(traffic.clone()).is_ok());
        }
        drop(watchers);
        drop(services);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DiscoveryClient for StaticDiscovery {
    /// 业务作用：列出当前静态表中的全部服务名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回按字典序排列的名称，包括实例列表为空的服务，不访问网络。
    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .inner
            .services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect())
    }

    /// 业务作用：从静态完整快照中筛选可承接流量的实例。
    ///
    /// 参数说明：
    /// - `service`：目标静态服务名。
    ///
    /// 返回：返回公共资格过滤后的列表；未知服务为空，名称非法返回错误。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        Ok(traffic_instances(&self.discover_all(service).await?))
    }

    /// 业务作用：读取静态表中的完整服务实例快照。
    ///
    /// 参数说明：
    /// - `service`：需先通过名称校验的服务名。
    ///
    /// 返回：返回包含非流量实例的克隆快照，未知服务返回空列表；名称非法返回错误。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        validate_service_name(service)?;
        Ok(self
            .inner
            .services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(service)
            .cloned()
            .unwrap_or_default())
    }

    /// 业务作用：在读取初值的同一锁序窗口登记静态服务订阅。
    ///
    /// 参数说明：
    /// - `service`：要观察的静态服务名。
    /// - `options`：需通过校验的订阅策略，静态后端不启动轮询。
    ///
    /// 返回：返回当前流量快照及取消句柄，后续 replace 推送最新值；名称或选项非法时不登记。
    async fn watch_with_options(
        &self,
        service: &str,
        options: WatchOptions,
    ) -> anyhow::Result<ServiceWatch> {
        options.validate()?;
        validate_service_name(service)?;
        let services = self
            .inner
            .services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let initial = services
            .get(service)
            .map_or_else(Vec::new, |instances| traffic_instances(instances));
        let (sender, receiver) = tokio::sync::watch::channel(initial);
        let id = self.inner.next_watcher.fetch_add(1, Ordering::Relaxed);
        self.inner
            .watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(service.to_owned())
            .or_default()
            .insert(id, sender);
        drop(services);
        Ok(ServiceWatch {
            guard: Box::new(StaticWatchGuard {
                inner: Arc::downgrade(&self.inner),
                service: service.to_owned(),
                id,
            }),
            receiver,
        })
    }
}

/// 静态发现订阅的拥有式注销凭据，Drop 与显式 unsubscribe 共用同一删除逻辑。
struct StaticWatchGuard {
    inner: Weak<StaticInner>,
    service: String,
    id: u64,
}

impl StaticWatchGuard {
    /// 业务作用：移除当前静态 watcher，并回收空订阅桶。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：对应 sender 被移除；后端已释放或记录已不存在时无操作，可重复调用。
    fn remove(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut watchers = inner
            .watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(service) = watchers.get_mut(&self.service) {
            service.remove(&self.id);
            if service.is_empty() {
                watchers.remove(&self.service);
            }
        }
    }
}

impl Drop for StaticWatchGuard {
    /// 业务作用：在静态订阅句柄释放时撤销共享表中的订阅记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同步移除当前订阅，接收端不再得到该 sender 的新值。
    fn drop(&mut self) {
        self.remove();
    }
}

#[async_trait::async_trait]
impl ServiceWatchGuard for StaticWatchGuard {
    /// 业务作用：显式撤销静态订阅并消费其句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：移除订阅后成功；随后的 Drop 重复移除可安全无操作，不等待其它订阅者。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        self.remove();
        Ok(())
    }
}

/// 一个服务的 DNS A/AAAA 查询合同。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsService {
    /// 要解析的 hostname。
    pub host: String,
    /// 连接端口。
    pub port: u16,
}

/// 使用 Tokio resolver 的 DNS/static-host provider；watch 以有界轮询发布变化并在错误时保 last-good。
#[derive(Clone)]
pub struct DnsDiscovery {
    services: Arc<BTreeMap<String, DnsService>>,
}

impl DnsDiscovery {
    /// 业务作用：冻结显式声明的 DNS 服务和目标端口映射。
    ///
    /// 参数说明：
    /// - `services`：非空服务表，每项提供主机名及非零服务端口。
    ///
    /// 返回：名称和目标基本字段合法时返回后端；空表或非法字段返回错误，不提前执行 DNS 查询。
    pub fn new(services: BTreeMap<String, DnsService>) -> anyhow::Result<Self> {
        anyhow::ensure!(!services.is_empty(), "dns discovery requires services");
        for (service, target) in &services {
            validate_service_name(service)?;
            anyhow::ensure!(
                !target.host.is_empty()
                    && target.host == target.host.trim()
                    && target.host.len() <= 253
                    && target.port != 0,
                "dns discovery target is invalid"
            );
        }
        Ok(Self {
            services: Arc::new(services),
        })
    }

    /// 业务作用：解析 DNS 目标并生成按地址和端口排序去重的实例。
    ///
    /// 参数说明：
    /// - `target`：待解析的主机名和服务端口。
    ///
    /// 返回：解析得到非空地址时返回带 source=dns 元数据的实例；解析失败或无地址返回错误，不提供后端健康证据。
    async fn resolve(target: &DnsService) -> anyhow::Result<Vec<Instance>> {
        let addresses = tokio::net::lookup_host((target.host.as_str(), target.port)).await?;
        let mut endpoints = addresses
            .map(|address| (address.ip().to_string(), address.port()))
            .collect::<Vec<_>>();
        endpoints.sort();
        endpoints.dedup();
        anyhow::ensure!(!endpoints.is_empty(), "DNS returned no addresses");
        Ok(endpoints
            .into_iter()
            .map(|(ip, port)| Instance::new(ip, port).with_metadata("source", "dns"))
            .collect())
    }
}

#[async_trait::async_trait]
impl DiscoveryClient for DnsDiscovery {
    /// 业务作用：列出冻结 DNS 服务表中的配置名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回按字典序排列的名称，不发起 DNS 查询。
    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.services.keys().cloned().collect())
    }

    /// 业务作用：对指定 DNS 服务执行一次地址发现。
    ///
    /// 参数说明：
    /// - `service`：冻结服务表中的逻辑名称。
    ///
    /// 返回：与完整视图使用同一解析结果，返回排序去重地址；未配置、解析失败或无地址返回错误。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        self.discover_all(service).await
    }

    /// 业务作用：读取指定 DNS 服务的完整地址解析结果。
    ///
    /// 参数说明：
    /// - `service`：必须存在于冻结表中的服务名。
    ///
    /// 返回：成功返回本次解析的地址实例列表；未配置或解析失败返回错误，不保证业务端点健康。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        let target = self
            .services
            .get(service)
            .ok_or_else(|| anyhow::anyhow!("DNS service is not configured"))?;
        Self::resolve(target).await
    }

    /// 业务作用：建立 DNS 初始快照并周期解析，成功且变化时推送候选。
    ///
    /// 参数说明：
    /// - `service`：已配置的逻辑 DNS 服务名。
    /// - `options`：要求非零周期的订阅选项。
    ///
    /// 返回：初次解析成功后返回接收端及任务取消句柄；后续解析失败保留最近成功值。选项、服务或初次解析失败不启动轮询。
    async fn watch_with_options(
        &self,
        service: &str,
        options: WatchOptions,
    ) -> anyhow::Result<ServiceWatch> {
        options.validate()?;
        let target = self
            .services
            .get(service)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("DNS service is not configured"))?;
        let initial = Self::resolve(&target).await?;
        let (sender, receiver) = tokio::sync::watch::channel(initial.clone());
        let task = tokio::spawn(async move {
            let mut last_good = initial;
            let mut interval = tokio::time::interval(options.poll_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Ok(candidate) = DnsDiscovery::resolve(&target).await {
                    if candidate != last_good {
                        last_good = candidate.clone();
                        if sender.send(candidate).is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Ok(ServiceWatch {
            guard: Box::new(DnsWatchGuard {
                task: task.abort_handle(),
            }),
            receiver,
        })
    }
}

/// DNS 轮询任务的拥有式取消句柄。
struct DnsWatchGuard {
    task: tokio::task::AbortHandle,
}

impl Drop for DnsWatchGuard {
    /// 业务作用：在 DNS 订阅句柄释放时请求终止其轮询任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发送任务 abort，不同步等待任务退出，不影响其它订阅。
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl ServiceWatchGuard for DnsWatchGuard {
    /// 业务作用：显式请求终止当前 DNS 轮询并消费订阅句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发送 abort 后立即成功，不等待任务完成；Drop 的重复终止请求安全。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        self.task.abort();
        Ok(())
    }
}

/// 业务作用：在构造静态后端前整体检查服务表。
///
/// 参数说明：
/// - `services`：待发布的完整静态服务到实例列表映射。
///
/// 返回：每项名称和实例合法时成功；首次非法项返回错误，不改变原表。空表允许。
fn validate_services(services: &BTreeMap<String, Vec<Instance>>) -> anyhow::Result<()> {
    for (service, instances) in services {
        validate_service_name(service)?;
        validate_instances(instances)?;
    }
    Ok(())
}

/// 业务作用：限制服务名为有界且无空白的 ASCII 标识。
///
/// 参数说明：
/// - `service`：要用于服务表索引的名称。
///
/// 返回：名称长度为 1 到 128 字节且只含字母、数字、连字符、下划线或点时成功，否则返回错误。
fn validate_service_name(service: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !service.is_empty()
            && service.len() <= 128
            && service
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "discovery service name is invalid"
    );
    Ok(())
}

/// 业务作用：检查静态实例端点、有限非负权重及服务内端点唯一性。
///
/// 参数说明：
/// - `instances`：同一服务将要整体发布的实例列表。
///
/// 返回：全部地址非空且无首尾空白、端口非零、权重有限非负并无重复地址端口时成功；否则错误。允许空列表及不健康、禁用实例。
fn validate_instances(instances: &[Instance]) -> anyhow::Result<()> {
    let mut endpoints = std::collections::BTreeSet::new();
    for instance in instances {
        anyhow::ensure!(
            !instance.ip.is_empty()
                && instance.ip == instance.ip.trim()
                && instance.port != 0
                && instance.weight.is_finite()
                && instance.weight >= 0.0,
            "static discovery instance is invalid"
        );
        anyhow::ensure!(
            endpoints.insert((instance.ip.clone(), instance.port)),
            "static discovery contains a duplicate endpoint"
        );
    }
    Ok(())
}

/// 业务作用：从完整实例表投影出公共规则允许承接流量的节点。
///
/// 参数说明：
/// - `instances`：含健康及管理属性的完整快照。
///
/// 返回：返回保留原顺序的合格实例克隆，不修改原快照或探测网络。
fn traffic_instances(instances: &[Instance]) -> Vec<Instance> {
    instances
        .iter()
        .filter(|instance| is_traffic_instance(instance))
        .cloned()
        .collect()
}
