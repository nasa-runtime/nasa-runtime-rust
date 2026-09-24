use std::{future::Future, pin::Pin};

use crate::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};

/// 组件、资源和 action 使用的 object-safe 异步结果。
pub type ApplicationFuture<'a, T = ()> =
    Pin<Box<dyn Future<Output = ApplicationResult<T>> + Send + 'a>>;

/// 生命周期扩展值的唯一释放权；借用轮询和所有权移交均不能越过析构隔离。
pub(crate) struct StartupCleanup<T> {
    value: Option<T>,
    component: ComponentId,
    phase: ApplicationPhase,
    operation: &'static str,
}

impl<T> StartupCleanup<T> {
    /// 业务作用：在可能拒绝、等待或暂存之前接管扩展值，保持生命周期回滚和停机的控制权。
    /// 参数说明：`value` 为 future、实例或工厂；`component`、`phase` 和 `operation` 为固定释放归因。
    /// 返回：不运行用户代码的所有权守卫，未显式释放时由 Drop 承担最后边界。
    pub(crate) fn new(
        value: T,
        component: ComponentId,
        phase: ApplicationPhase,
        operation: &'static str,
    ) -> Self {
        Self {
            value: Some(value),
            component,
            phase,
            operation,
        }
    }

    /// 业务作用：借出扩展值用于读取元数据与执行无状态校验，保留唯一释放权。
    /// 参数说明：无。
    /// 返回：尚未释放值的共享借用；移交或释放后再次借用属于内部状态错误。
    pub(crate) fn value(&self) -> &T {
        self.value
            .as_ref()
            .expect("extension ownership is retained")
    }

    /// 业务作用：借出扩展值供受保护轮询，所有权仍留在守卫中。
    /// 参数说明：无。
    /// 返回：尚未释放值的可变借用；移交或释放后再次借用属于内部状态错误。
    pub(crate) fn value_mut(&mut self) -> &mut T {
        self.value
            .as_mut()
            .expect("extension ownership is retained")
    }

    /// 业务作用：把唯一所有权交给紧邻的受保护调用或接管者。
    /// 参数说明：无。
    /// 返回：原值；调用方必须立即建立下一释放边界，原守卫不再析构它。
    pub(crate) fn take(&mut self) -> T {
        self.value.take().expect("extension ownership is retained")
    }

    /// 业务作用：在独立展开边界内释放生命周期扩展，防止取消与拒绝截断异步回滚。
    /// 参数说明：无。
    /// 返回：正常或已移交时为空；单次析构展开转为固定阶段错误，异常正文不参与诊断。
    pub(crate) fn release(&mut self) -> Option<ApplicationError> {
        // 先撤销释放权，异常后也不会重复析构；payload 的析构单独隔离，避免二次异常越过此边界。
        let value = self.value.take();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
            Ok(()) => None,
            Err(payload) => {
                crate::shutdown::release_shutdown_panic_payload(payload);
                Some(ApplicationError::new(
                    self.component,
                    self.phase,
                    format!("extension ownership panicked while {}", self.operation),
                ))
            }
        }
    }
}

impl<T> Drop for StartupCleanup<T> {
    /// 业务作用：在外层取消或尚未显式移交时隔离释放，允许其余暂存项和 active stack 继续清理。
    /// 参数说明：无。
    /// 返回：无返回值；无法返回的次要释放错误以固定告警报告，不覆盖首次停止原因。
    fn drop(&mut self) {
        if let Some(error) = self.release() {
            crate::report::report_shutdown(&error);
        }
    }
}
