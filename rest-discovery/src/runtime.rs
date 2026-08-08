//! `RemoteRuntime`:承载 discovery provider + rest client + 后台任务生命周期。
//!
//! drop 时统一 abort 后台 watch pump,避免运行时 reset / 热重启留下泄漏任务。

use std::sync::Arc;

use nadisc::DiscoveryClient;

use crate::client::RestDiscoveryClient;

/// 全局门面内部持有的轻量运行时。发布到全局前必须是「可用状态」(provider 已连、rest 已建)。
pub struct RemoteRuntime {
    rest: Arc<RestDiscoveryClient>,
    /// 保留 provider 句柄:第三阶段的 service-list 刷新任务会用到;第一阶段仅持有。
    _discovery: Option<Arc<dyn DiscoveryClient>>,
}

impl RemoteRuntime {
    /// 业务作用：构造新实例；用于集中初始化内部字段和默认状态。
    pub(crate) fn new(
        rest: Arc<RestDiscoveryClient>,
        discovery: Option<Arc<dyn DiscoveryClient>>,
    ) -> Self {
        Self {
            rest,
            _discovery: discovery,
        }
    }

    /// 业务作用：取共享的 HTTP LB client。
    pub fn rest(&self) -> Arc<RestDiscoveryClient> {
        self.rest.clone()
    }
}

impl std::fmt::Debug for RemoteRuntime {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRuntime")
            .field("has_discovery", &self._discovery.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for RemoteRuntime {
    /// 业务作用：释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        // 停掉所有后台 watch pump(reset/热重启不泄漏任务)。
        self.rest.shutdown_background();
    }
}
