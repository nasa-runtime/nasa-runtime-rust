//! `RemoteRuntime`:承载 discovery provider + rest client + 后台任务生命周期。
//!
//! drop 时统一 abort 后台 watch pump,避免运行时 reset / 热重启留下泄漏任务。

use std::sync::Arc;

use nadisc::DiscoveryClient;

use crate::client::RestDiscoveryClient;

/// 全局门面内部持有的轻量运行时。发布到全局前必须是「可用状态」(provider 已连、rest 已建)。
pub struct RemoteRuntime {
    rest: Arc<RestDiscoveryClient>,
    /// 与当前 REST 实例共同持有发现 provider，避免仍有任务时提前释放。
    _discovery: Option<Arc<dyn DiscoveryClient>>,
}

impl RemoteRuntime {
    /// 业务作用：共同持有 REST 客户端及可选发现后端的生命周期。
    ///
    /// 参数说明：
    /// - `rest`：已由调用方构造好的共享 REST 客户端。
    /// - `discovery`：需与该客户端共同存活的可选发现后端。
    ///
    /// 返回：返回持有两者的运行时，不检查关联身份或执行网络初始化；释放运行时时会请求停止客户端后台任务。
    pub fn new(
        rest: Arc<RestDiscoveryClient>,
        discovery: Option<Arc<dyn DiscoveryClient>>,
    ) -> Self {
        Self {
            rest,
            _discovery: discovery,
        }
    }

    /// 业务作用：取得当前运行时持有的共享 REST 客户端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回客户端 Arc 的克隆，不新建连接池或复制运行状态。
    pub fn rest(&self) -> Arc<RestDiscoveryClient> {
        self.rest.clone()
    }
}

impl std::fmt::Debug for RemoteRuntime {
    /// 业务作用：展示运行时是否持有发现后端而不输出其内部对象。
    ///
    /// 参数说明：
    /// - `f`：接收调试表示的格式化目标。
    ///
    /// 返回：写入类型及 has_discovery 布尔值，格式化失败传播，不输出端点或认证配置。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRuntime")
            .field("has_discovery", &self._discovery.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for RemoteRuntime {
    /// 业务作用：在运行时释放时关闭 REST 客户端的后台观察责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：请求停止关联后台任务，不等待异步完成，也不直接调用发现后端的注销或关闭接口。
    fn drop(&mut self) {
        // 停掉所有后台 watch pump(reset/热重启不泄漏任务)。
        self.rest.shutdown_background();
    }
}
