//! `RestDiscovery` 全局门面。
//!
//! 业务在 main 里 `init_*` 一次,任意位置 `RestDiscovery::get()` 拿共享 client。
//!
//! 全局存储用 `std::sync::RwLock<Option<Arc<RemoteRuntime>>>`:
//! init 写一次(已存在 → `AlreadyInitialized`),`get/try_get` 读一次。
//! 这与文档示意的 `tokio::sync::OnceCell` 语义等价(get 未初始化 panic、try_get Err、重复 init 报错)。

use std::sync::{Arc, RwLock};

use nadisc::DiscoveryClient;

use crate::client::RestDiscoveryClient;
use crate::error::{RestDiscoveryError, Result};
use crate::lb::LoadBalancer;
use crate::options::RestDiscoveryOptions;
use crate::runtime::RemoteRuntime;

/// 进程级唯一运行时。`None` = 尚未 init。
static RUNTIME: RwLock<Option<Arc<RemoteRuntime>>> = RwLock::new(None);

/// `nasa::nadisc::RestDiscovery` 全局门面(零字段标记类型)。
pub struct RestDiscovery;

impl RestDiscovery {
    /// 业务作用：把已经由宿主持有的运行态发布给声明式客户端。
    ///
    /// 参数说明：
    /// - `runtime`：为唯一受管运行态。
    ///
    /// 返回：空槽安装成功；存在其它 owner 时拒绝，不覆盖现有客户端。
    pub fn install_runtime(runtime: Arc<RemoteRuntime>) -> Result<()> {
        install(runtime)
    }
    /// 业务作用：构造发现及负载均衡客户端并尝试安装进程共享入口。
    ///
    /// 参数说明：
    /// - `discovery`：已构造的共享服务发现后端。
    /// - `options`：客户端及后台观察策略。
    ///
    /// 返回：客户端连接初始化和全局安装成功时完成；配置、后端或已初始化错误传播，不覆盖已有运行时。
    pub async fn init_with_discovery(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
    ) -> Result<()> {
        // connect() 按 heuristic_http 决定是否同步首拉 list_services + 启动索引刷新(首拉失败可致启动失败)。
        let rest = Arc::new(RestDiscoveryClient::connect(discovery.clone(), options).await?);
        let runtime = Arc::new(RemoteRuntime::new(rest, Some(discovery)));
        install(runtime)
    }

    /// 业务作用：用显式选址算法构造发现客户端并安装共享入口。
    ///
    /// 参数说明：
    /// - `discovery`：已构造的发现后端。
    /// - `options`：运行策略，其中内置 lb_strategy 不用于选址。
    /// - `load_balancer`：三类内部调用共同使用的自定义选址算法。
    ///
    /// 返回：初始化和安装成功时完成；失败返回对应错误，不覆盖已有全局入口。
    pub async fn init_with_discovery_and_load_balancer(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
        load_balancer: Arc<dyn LoadBalancer>,
    ) -> Result<()> {
        let rest = Arc::new(
            RestDiscoveryClient::connect_with_load_balancer(
                discovery.clone(),
                options,
                load_balancer,
            )
            .await?,
        );
        let runtime = Arc::new(RemoteRuntime::new(rest, Some(discovery)));
        install(runtime)
    }

    /// 业务作用：构造只支持外部 HTTP 的客户端并安装共享入口。
    ///
    /// 参数说明：
    /// - `options`：外部请求使用的超时、重试及保护选项。
    ///
    /// 返回：构造及安装成功时完成；非法配置或已初始化返回错误。此模式没有发现后端，显式内部调用保持拒绝语义。
    pub async fn init_external_only(options: RestDiscoveryOptions) -> Result<()> {
        // fail-fast:非法 options 返回 InvalidOptions,不 panic(init_with_discovery 经 connect() 同样 fail-fast)。
        let rest = Arc::new(RestDiscoveryClient::try_external_only(options)?);
        let runtime = Arc::new(RemoteRuntime::new(rest, None));
        install(runtime)
    }

    /// 业务作用：从进程共享入口取得 REST 客户端，供已完成初始化的业务调用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已初始化返回共享客户端；未初始化或全局锁中毒时 panic，需要可恢复错误的调用方应使用 try_get。
    pub fn get() -> Arc<RestDiscoveryClient> {
        Self::try_get().expect(
            "RestDiscovery 未初始化:请先在 main 调用 RestDiscovery::init_with_discovery 或 init_external_only",
        )
    }

    /// 业务作用：从进程共享入口读取客户端并显式报告未初始化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在运行时返回客户端 Arc 克隆，否则返回 NotInitialized；全局锁中毒仍会 panic。
    pub fn try_get() -> Result<Arc<RestDiscoveryClient>> {
        RUNTIME
            .read()
            .expect("rest-discovery: RUNTIME 锁被毒化")
            .as_ref()
            .map(|rt| rt.rest())
            .ok_or(RestDiscoveryError::NotInitialized)
    }

    /// 业务作用：取下当前进程共享运行时并请求停止其后台任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：确有运行时被取下时返回 true，空槽返回 false；不等待任务退出，全局锁中毒会 panic。持有外部 Arc 不阻止取下及后台停止。
    pub fn shutdown() -> bool {
        let taken = RUNTIME
            .write()
            .expect("rest-discovery: RUNTIME 锁被毒化")
            .take();
        match taken {
            Some(runtime) => {
                stop_background(&runtime);
                true
            }
            None => false,
        }
    }

    /// 业务作用：仅在指针身份仍匹配时撤下调用方安装的运行时。
    ///
    /// 参数说明：
    /// - `runtime`：期望仍占用全局槽的运行时 Arc。
    ///
    /// 返回：身份不匹配返回 false；匹配时先取下、释放全局锁，再停止后台并返回 true，避免旧句柄关闭新运行时。
    pub fn shutdown_if_current(runtime: &Arc<RemoteRuntime>) -> bool {
        let mut guard = RUNTIME.write().expect("rest-discovery: RUNTIME 锁被毒化");
        let is_current = guard
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, runtime));
        if !is_current {
            return false;
        }
        let taken = guard.take();
        drop(guard);
        if let Some(taken) = taken {
            stop_background(&taken);
        }
        true
    }

    /// 业务作用：取得进程共享运行时的拥有式引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已安装返回 Arc 克隆，未安装返回 NotInitialized；全局锁中毒时 panic，不转移全局槽本身的持有权。
    pub fn runtime() -> Result<Arc<RemoteRuntime>> {
        RUNTIME
            .read()
            .expect("rest-discovery: RUNTIME 锁被毒化")
            .clone()
            .ok_or(RestDiscoveryError::NotInitialized)
    }
}

/// 业务作用：请求停止已取下运行时的后台观察工作。
///
/// 参数说明：
/// - `runtime`：需要终止后台任务的共享运行时。
///
/// 返回：同步发出关闭及终止请求，不等待异步任务退出；重复调用由客户端关闭逻辑承接。
fn stop_background(runtime: &Arc<RemoteRuntime>) {
    runtime.rest().shutdown_background();
}

/// 业务作用：在全局槽为空时发布已构造完成的运行时。
///
/// 参数说明：
/// - `runtime`：需要安装的共享运行时。
///
/// 返回：空槽写入成功；已有运行时返回 AlreadyInitialized 并保留旧槽，全局锁中毒时 panic。
fn install(runtime: Arc<RemoteRuntime>) -> Result<()> {
    let mut guard = RUNTIME.write().expect("rest-discovery: RUNTIME 锁被毒化");
    if guard.is_some() {
        return Err(RestDiscoveryError::AlreadyInitialized);
    }
    // 只有空槽才发布新实例，防止并发初始化覆盖另一条仍承担请求责任的运行时。
    *guard = Some(runtime);
    Ok(())
}
