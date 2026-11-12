//! Application 容器发布的只读 mapping 运行时能力。

use std::sync::Arc;

use crate::capabilities::ComponentLifecycleState;
use crate::state::StateCell;

/// Web Ready 成功后可从 Application 取得的只读 mapping 句柄。
///
/// 句柄只暴露健康摘要和生命周期，不交出密钥、可变路由图或停机权限。
#[derive(Clone)]
pub struct MappingHandle {
    /// 可跨热更读取健康摘要的原运行时实例，外部只能通过只读接口使用。
    runtime: Arc<naweb::MappingRuntime>,
    /// 当前应用的共享生命周期状态；保留句柄不会阻止应用进入停机阶段。
    application_state: Arc<StateCell>,
}

impl MappingHandle {
    /// 业务作用：建立共享 MappingRuntime 的只读能力句柄。
    ///
    /// 参数说明：
    /// - `runtime`：当前应用已装配的路由运行时；此处共享原实例，不重新创建路由或安全材料。
    /// - `application_state`：同一应用的生命周期状态，供句柄持有者观察停机等变化。
    ///
    /// 返回：共享运行时与状态的句柄；构造本身不校验就绪，也不授予修改或停机权限。
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前快照代次、快照年龄和最近未恢复刷新失败的年龄；不执行后端探测。
    /// 快照和失败时间分别读取，并非统一原子快照；该摘要不代表应用整体就绪或每条路由可用。
    pub fn health(&self) -> naweb::MappingRuntimeHealth {
        self.runtime.health()
    }

    /// 业务作用：返回与 Application 统一状态机一致的只读生命周期状态。
    ///
    /// 参数说明：无。
    ///
    /// 返回：读取当时的应用阶段，Stopping 映射为 Draining、Stopped 映射为 Closed。
    /// 此读数独立于健康摘要，不授予后续调用资格，也不阻止读后发生停机。
    pub fn lifecycle(&self) -> ComponentLifecycleState {
        self.application_state.load().into()
    }
}
