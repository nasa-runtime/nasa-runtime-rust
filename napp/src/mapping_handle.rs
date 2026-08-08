//! Application 容器发布的只读 mapping 运行时能力。

use std::sync::Arc;

use crate::capabilities::ComponentLifecycleState;
use crate::state::StateCell;

/// Web Ready 成功后可从 Application 取得的只读 mapping 句柄。
///
/// 句柄只暴露健康摘要和生命周期，不交出密钥、可变路由图或停机权限。
#[derive(Clone)]
pub struct MappingHandle {
    runtime: Arc<naweb::MappingRuntime>,
    application_state: Arc<StateCell>,
}

impl MappingHandle {
    /// 业务作用：建立共享 MappingRuntime 的只读能力句柄。
    pub(crate) fn new(
        runtime: Arc<naweb::MappingRuntime>,
        application_state: Arc<StateCell>,
    ) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：返回不含路由、Token、密钥和后端地址的运行时健康摘要。
    pub fn health(&self) -> naweb::MappingRuntimeHealth {
        self.runtime.health()
    }

    /// 业务作用：返回与 Application 统一状态机一致的只读生命周期状态。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}
