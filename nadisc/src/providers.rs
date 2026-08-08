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
    /// 业务作用：从完整静态表构造；服务名、endpoint 或重复地址非法时整体拒绝。
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

    /// 业务作用：原子替换单个服务；候选非法时当前快照与 watch 均不变化。
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
    /// 业务作用：返回当前静态表中按字典序保存的全部服务名。
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

    /// 业务作用：返回指定服务当前可接流量的实例，过滤 disabled 或不健康节点。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        Ok(traffic_instances(&self.discover_all(service).await?))
    }

    /// 业务作用：返回指定服务的完整静态实例快照，包括暂不接流量的节点。
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

    /// 业务作用：注册一个带初始快照的服务订阅，并用 guard 保证取消时移除 sender。
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
    /// 业务作用：从共享订阅表移除当前 watcher，并清理已经为空的服务桶。
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
    /// 业务作用：调用方丢弃 watch 时立即注销，避免 sender 和服务桶泄漏。
    fn drop(&mut self) {
        self.remove();
    }
}

#[async_trait::async_trait]
impl ServiceWatchGuard for StaticWatchGuard {
    /// 业务作用：显式注销当前静态订阅；重复 Drop 由幂等删除安全承接。
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
    /// 业务作用：创建不可变 DNS 服务表。
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

    /// 业务作用：解析目标的 A/AAAA 地址并生成去重、稳定排序的实例快照。
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
    /// 业务作用：返回配置中声明的全部 DNS 服务名。
    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.services.keys().cloned().collect())
    }

    /// 业务作用：解析指定服务的当前 DNS 地址；DNS provider 不区分流量与管理视图。
    async fn discover(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        self.discover_all(service).await
    }

    /// 业务作用：对指定服务执行一次 DNS 查询并返回完整地址快照。
    async fn discover_all(&self, service: &str) -> anyhow::Result<Vec<Instance>> {
        let target = self
            .services
            .get(service)
            .ok_or_else(|| anyhow::anyhow!("DNS service is not configured"))?;
        Self::resolve(target).await
    }

    /// 业务作用：启动有界周期 DNS 轮询，仅在成功快照变化时向订阅者发布。
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
    /// 业务作用：watch 离开作用域时终止 DNS 轮询，避免 detached task。
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl ServiceWatchGuard for DnsWatchGuard {
    /// 业务作用：显式终止 DNS 轮询任务。
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        self.task.abort();
        Ok(())
    }
}

/// 业务作用：整体验证静态服务表的名称、实例字段和 endpoint 唯一性。
fn validate_services(services: &BTreeMap<String, Vec<Instance>>) -> anyhow::Result<()> {
    for (service, instances) in services {
        validate_service_name(service)?;
        validate_instances(instances)?;
    }
    Ok(())
}

/// 业务作用：限制服务名为有界 ASCII 标识，阻止空白、控制字符和无界 key。
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

/// 业务作用：校验实例 endpoint、权重及同服务内地址唯一性。
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

/// 业务作用：从完整管理视图投影出当前健康且启用的流量实例。
fn traffic_instances(instances: &[Instance]) -> Vec<Instance> {
    instances
        .iter()
        .filter(|instance| is_traffic_instance(instance))
        .cloned()
        .collect()
}
