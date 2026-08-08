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
    /// 业务作用：discovery 启用:用已连接好的 provider(`Arc<dyn DiscoveryClient>`)装配内部模式。
    /// `service_request`/`lb://` 走服务发现 + LB。
    ///
    /// # 参数
    ///
    /// - `discovery`: 已构造好的服务发现 provider。
    /// - `options`: rest-discovery 运行选项。
    pub async fn init_with_discovery(
        discovery: Arc<dyn DiscoveryClient>,
        options: RestDiscoveryOptions,
    ) -> Result<()> {
        // connect() 按 heuristic_http 决定是否同步首拉 list_services + 启动索引刷新(首拉失败可致启动失败)。
        let rest = Arc::new(RestDiscoveryClient::connect(discovery.clone(), options).await?);
        let runtime = Arc::new(RemoteRuntime::new(rest, Some(discovery)));
        install(runtime)
    }

    /// 业务作用：同 [`init_with_discovery`](Self::init_with_discovery),但**注入自定义 [`LoadBalancer`]**:
    /// `options.lb_strategy` 被忽略,三类内部调用(`service_request`/`lb://`/启发式命中)共享传入算法。
    ///
    /// # 参数
    ///
    /// - `discovery`: 已构造好的服务发现 provider。
    /// - `options`: rest-discovery 运行选项。
    /// - `load_balancer`: 调用方注入的选址算法。
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

    /// 业务作用：discovery 禁用(`rest_discovery.enabled=false`):发布 external-only client。
    /// 普通 http(s) 可用;显式内部调用返回 `DiscoveryDisabledForInternalCall`,**不退化成 DNS**。
    ///
    /// # 参数
    ///
    /// - `options`: external-only HTTP client 运行选项。
    pub async fn init_external_only(options: RestDiscoveryOptions) -> Result<()> {
        // fail-fast:非法 options 返回 InvalidOptions,不 panic(init_with_discovery 经 connect() 同样 fail-fast)。
        let rest = Arc::new(RestDiscoveryClient::try_external_only(options)?);
        let runtime = Arc::new(RemoteRuntime::new(rest, None));
        install(runtime)
    }

    /// 业务作用：业务便利入口:未初始化直接 panic 并提示先 init。库代码建议用 [`try_get`](Self::try_get)。
    pub fn get() -> Arc<RestDiscoveryClient> {
        Self::try_get().expect(
            "RestDiscovery 未初始化:请先在 main 调用 RestDiscovery::init_with_discovery 或 init_external_only",
        )
    }

    /// 业务作用：取共享 client;未初始化返回 `Err(NotInitialized)`。
    pub fn try_get() -> Result<Arc<RestDiscoveryClient>> {
        RUNTIME
            .read()
            .expect("rest-discovery: RUNTIME 锁被毒化")
            .as_ref()
            .map(|rt| rt.rest())
            .ok_or(RestDiscoveryError::NotInitialized)
    }

    /// 业务作用：幂等取下并显式关闭当前进程级运行时。
    ///
    /// 只 drop 手上的 `Arc` 是不够的:全局槽自身持有一份强引用,不取下就永远不会触发 `RemoteRuntime::drop`,
    /// 索引刷新与 watch pump 会活到进程退出。应用停机必须能显式收回这条生命周期。
    ///
    /// # 参数
    ///
    /// 本函数无参数;槽为空时返回 `false`,重复调用安全。
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

    /// 业务作用：仅当全局槽仍指向给定运行时时才取下并关闭它。
    ///
    /// 用指针相等做 clear-if-current:迁移期若已经装上了另一个实例,旧持有者不能把它误关。
    ///
    /// # 参数
    ///
    /// - `runtime`: 调用方自己安装、期望关闭的运行时句柄。
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

    /// 业务作用：取底层运行时(高级用途);未初始化返回 `Err(NotInitialized)`。
    pub fn runtime() -> Result<Arc<RemoteRuntime>> {
        RUNTIME
            .read()
            .expect("rest-discovery: RUNTIME 锁被毒化")
            .clone()
            .ok_or(RestDiscoveryError::NotInitialized)
    }
}

/// 业务作用：主动终止运行时的后台任务,不等待最后一个 `Arc` 释放。
///
/// 业务可能仍持有 `RestDiscovery::get()` 返回的 client 句柄;abort 是幂等的,
/// 因此这里显式停一次不会与随后的 `RemoteRuntime::drop` 冲突。
///
/// # 参数
/// - `runtime`: 已经从全局槽取下的运行时。
fn stop_background(runtime: &Arc<RemoteRuntime>) {
    runtime.rest().shutdown_background();
}

/// 业务作用：把运行时写入全局:已存在 → `AlreadyInitialized`(不覆盖)。
///
/// # 参数
/// - `runtime`: REST 发现客户端共享的运行时状态。
fn install(runtime: Arc<RemoteRuntime>) -> Result<()> {
    let mut guard = RUNTIME.write().expect("rest-discovery: RUNTIME 锁被毒化");
    if guard.is_some() {
        return Err(RestDiscoveryError::AlreadyInitialized);
    }
    *guard = Some(runtime);
    Ok(())
}
