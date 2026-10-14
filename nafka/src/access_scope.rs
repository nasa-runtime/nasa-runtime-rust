//! 宿主导出句柄的单向准入边界，与底层资源的实际退出分别管理。

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::{NafkaError, Result};

/// 可附加到 Admin 和 producer lane 的宿主准入域。
///
/// 关闭永久拒绝该域的新操作，但不取消已准入责任，也不表示底层资源已退出。
/// 克隆共享同一边界；句柄可叠加多个域，任一域关闭都拒绝准入。
#[derive(Clone, Default)]
pub struct KafkaAccessScope {
    closed: Arc<AtomicBool>,
}

impl KafkaAccessScope {
    /// 业务作用：创建一个尚未关闭的宿主准入域。
    /// 参数说明：无。
    /// 返回：独立的新域；不会创建连接、线程或取得源关闭权。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：永久封闭本域导出句柄的新准入。
    /// 参数说明：无。
    /// 返回：所有克隆观察到同一关闭边界；已准入工作仍由源负责排干。
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// 业务作用：读取本域是否已永久拒绝新操作。
    /// 参数说明：无。
    /// 返回：真仅表示准入关闭，不证明客户端析构或业务完成。
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// 业务作用：在业务操作取得底层资源之前复验宿主权威。
    /// 参数说明：无。
    /// 返回：开放时通过；域关闭后永久返回 Lifecycle。
    pub(crate) fn ensure_open(&self) -> Result<()> {
        // 宿主清理预算耗尽也不能使旧句柄恢复准入。
        if self.is_closed() {
            return Err(NafkaError::Lifecycle("Kafka 句柄所属准入域已关闭".into()));
        }
        Ok(())
    }

    /// 业务作用：叠加准入边界且不解除既有边界。
    /// 参数说明：scopes 为当前句柄已有的域集合。
    /// 返回：原地加入尚未登记的域；重复绑定同一域不增加存储。
    pub(crate) fn attach(&self, scopes: &mut Vec<Self>) {
        if !scopes
            .iter()
            .any(|scope| Arc::ptr_eq(&scope.closed, &self.closed))
        {
            scopes.push(self.clone());
        }
    }
}
