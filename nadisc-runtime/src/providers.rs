//! 本地静态表与 DNS 的受管桥，不把地址解析提升为后端健康证据。

use nadisc::managed::{
    Capabilities, DiscoveryError, ErrorCode, EvidenceKind, ManagedProvider, ProviderSnapshot,
    Result,
};
use nadisc::{DiscoveryClient, DnsDiscovery, DnsService, Instance, StaticDiscovery};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

enum Backend {
    Static(StaticDiscovery),
    Dns(DnsDiscovery),
}

pub struct LocalProvider {
    backend: Backend,
    closed: AtomicBool,
}

impl LocalProvider {
    /// 业务作用：冻结普通服务的静态实例表，不产生自注册副作用。
    /// 参数说明：`services` 为服务到完整实例列表的显式映射。
    /// 返回：合法静态表对应的只读 provider。
    pub fn static_services(services: BTreeMap<String, Vec<Instance>>) -> Result<Self> {
        let backend = StaticDiscovery::new(services)
            .map_err(|_| crate::config::invalid("invalid static discovery table"))?;
        Ok(Self {
            backend: Backend::Static(backend),
            closed: AtomicBool::new(false),
        })
    }

    /// 业务作用：复用 A/AAAA 地址解析，只允许显式服务绑定。
    /// 参数说明：`services` 为服务别名与主机端口映射。
    /// 返回：只读 DNS provider，不承诺 SRV、DNS TTL 或业务健康。
    pub fn dns_services(services: BTreeMap<String, DnsService>) -> Result<Self> {
        if services.is_empty() {
            return Err(crate::config::invalid(
                "DNS service bindings cannot be empty",
            ));
        }
        let backend = DnsDiscovery::new(services)
            .map_err(|_| crate::config::invalid("invalid DNS discovery table"))?;
        Ok(Self {
            backend: Backend::Dns(backend),
            closed: AtomicBool::new(false),
        })
    }

    /// 业务作用：统一拒绝已关闭本地 provider 的新请求。
    /// 参数说明：无。
    /// 返回：仍开放时成功，关闭后拒绝。
    fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(crate::closed())
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl ManagedProvider for LocalProvider {
    /// 业务作用：准确声明本地后端只读与证据边界。
    /// 参数说明：无。
    /// 返回：Static 或 Address 证据，永不声明远端健康。
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            registration: false,
            enumeration: true,
            evidence: match self.backend {
                Backend::Static(_) => EvidenceKind::Static,
                Backend::Dns(_) => EvidenceKind::Address,
            },
        }
    }

    /// 业务作用：确认本地配置 provider 仍开放。
    /// 参数说明：无。
    /// 返回：不把 DNS 配置有效误报为远端服务健康。
    async fn prepare(&self) -> Result<()> {
        self.check()
    }

    /// 业务作用：读取完整静态表或执行一次 DNS 地址解析。
    /// 参数说明：`service` 是显式服务别名。
    /// 返回：完整候选或地址解析失败，不使用旧 DNS 缓存伪造新证据。
    async fn observe(&self, service: &str) -> Result<ProviderSnapshot> {
        self.check()?;
        let instances = match &self.backend {
            Backend::Static(provider) => provider.discover_all(service).await,
            Backend::Dns(provider) => provider.discover_all(service).await,
        }
        .map_err(|_| {
            DiscoveryError::new(ErrorCode::Unavailable, "local provider observation failed")
        })?;
        self.check()?;
        Ok(ProviderSnapshot {
            instances,
            evidence: self.capabilities().evidence,
        })
    }

    /// 业务作用：只枚举显式配置服务，不执行全局目录扫描。
    /// 参数说明：无。
    /// 返回：本连接配置的服务名。
    async fn services(&self) -> Result<Vec<String>> {
        self.check()?;
        match &self.backend {
            Backend::Static(provider) => provider.list_services().await,
            Backend::Dns(provider) => provider.list_services().await,
        }
        .map_err(|_| {
            DiscoveryError::new(ErrorCode::Unavailable, "local service enumeration failed")
        })
    }

    /// 业务作用：禁止该本地适配器接受新观察。
    /// 参数说明：无。
    /// 返回：重复关闭幂等，没有独立后台循环。
    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}
