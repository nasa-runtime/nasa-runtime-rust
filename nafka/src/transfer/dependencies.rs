//! 目标先预登记，来源后绑定；双方停机均等待同一实际任务。

use super::{TransferError, TransferResult};
use crate::replay::runtime::ReplayParticipant;
use crate::{KafkaProxyInner, ReplayHandle};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(crate) struct TransferRegistry {
    state: Mutex<RegistryState>,
}
#[derive(Default)]
struct RegistryState {
    closed: bool,
    links: Vec<Arc<TransferLink>>,
}
pub(crate) struct TransferLink {
    owner: Weak<KafkaProxyInner>,
    name: String,
    state: Mutex<LinkState>,
    changed: Condvar,
    inspection: Arc<super::TargetInspection>,
}
#[derive(Default)]
struct LinkState {
    handle: Option<ReplayHandle>,
    stopped: bool,
    finished: bool,
}

impl TransferRegistry {
    /// 业务作用：在来源执行之前占用目标的有界依赖责任，与停机共享准入门。
    /// 参数说明：owner 为目标内部状态，name 为来源与任务的稳定诊断身份，inspection 为必须先销毁再归还任务责任的目标探测资源。
    /// 返回：尚未激活的票据；关闭或十六项容量耗尽时拒绝。
    pub(crate) fn reserve(
        &self,
        owner: &Arc<KafkaProxyInner>,
        name: String,
        inspection: Arc<super::TargetInspection>,
    ) -> TransferResult<Arc<TransferLink>> {
        let mut state = lock(&self.state);
        if state.closed
            || matches!(
                owner.lifecycle(),
                crate::Lifecycle::ShuttingDown | crate::Lifecycle::Stopped
            )
        {
            return Err(TransferError::Closed);
        }
        if state.links.len() >= 16 {
            return Err(TransferError::Capacity);
        }
        let link = Arc::new(TransferLink {
            owner: Arc::downgrade(owner),
            name,
            state: Mutex::new(LinkState::default()),
            changed: Condvar::new(),
            inspection,
        });
        state.links.push(Arc::clone(&link));
        Ok(link)
    }
    /// 业务作用：先永久关闭新依赖，再通知包括尚未绑定的全部责任。
    /// 参数说明：无。返回：本轮完整依赖快照，不等待实际 I/O。
    pub(crate) fn stop(&self) -> Vec<Arc<TransferLink>> {
        let mut state = lock(&self.state);
        state.closed = true;
        for link in &state.links {
            link.stop();
        }
        state.links.clone()
    }
    /// 业务作用：在超时诊断中保留尚未退出的目标依赖，包括尚未绑定来源句柄者。
    /// 参数说明：无。返回：稳定来源任务名，不触发取消或 I/O。
    pub(crate) fn pending_names(&self) -> Vec<String> {
        lock(&self.state)
            .links
            .iter()
            .map(|link| link.name.clone())
            .collect()
    }
    /// 业务作用：实际责任退出后只移除对应票据，不影响其它同名来源。
    /// 参数说明：link 为实际完成的票据。返回：归还一个目标容量槽。
    fn release(&self, link: &TransferLink) {
        lock(&self.state)
            .links
            .retain(|current| !std::ptr::eq(current.as_ref(), link));
    }
}
impl TransferLink {
    /// 业务作用：连接双方已经登记的责任并打开激活等待者；早到的停机必须先传递给来源。
    /// 参数说明：handle 为来源唯一真实任务。返回：停止状态不能因绑定而重新开放。
    pub(crate) fn bind(&self, handle: ReplayHandle) {
        let mut state = lock(&self.state);
        if state.stopped || state.finished {
            handle.cancel();
        }
        if !state.finished {
            state.handle = Some(handle);
        }
        self.changed.notify_all();
    }
    /// 业务作用：记住目标停止请求，即使来源句柄仍在构造也不能丢失取消。
    /// 参数说明：无。返回：仅关闭准入，实际责任继续保留。
    fn stop(&self) {
        let mut state = lock(&self.state);
        state.stopped = true;
        if let Some(handle) = &state.handle {
            handle.cancel();
        }
        self.changed.notify_all();
    }
}
impl ReplayParticipant for TransferLink {
    /// 业务作用：在完整绑定前阻止取得持久所有权，同时响应任一端先到的停止。
    /// 参数说明：stopping 为来源停止查询。返回：绑定成功且双方仍开放才允许执行。
    fn activate(&self, stopping: &dyn Fn() -> bool) -> bool {
        let mut state = lock(&self.state);
        while state.handle.is_none() && !state.stopped && !state.finished && !stopping() {
            state = self
                .changed
                .wait_timeout(state, Duration::from_millis(100))
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        state.handle.is_some() && !state.stopped && !state.finished && !stopping()
    }
    /// 业务作用：真实 consumer、处理和 lease 退出后解除循环句柄并归还目标登记。
    /// 参数说明：无。返回：唤醒停机等待者；不在票据锁内取得目录锁。
    fn finished(&self) {
        // worker 已全部退出才销毁复用的探测连接；保留任务句柄不能延长原生资源寿命。
        // 原生析构返回前不发布 finished，目标停机仍能观察并等待这份责任。
        self.inspection.close();
        {
            let mut state = lock(&self.state);
            state.finished = true;
            state.handle.take();
            self.changed.notify_all();
        }
        // stop 的锁顺序是目录到票据，归还前先释放票据锁，避免双方关闭时相互等待。
        if let Some(owner) = self.owner.upgrade() {
            owner.transfers.release(self);
        }
    }
}
/// 业务作用：目标停机等待全部实际依赖，超时不删除票据或释放来源 lease。
/// 参数说明：links 为已通知快照，deadline 为目标共用总预算。返回：尚未完成的稳定来源任务名。
pub(crate) async fn drain(links: &[Arc<TransferLink>], deadline: Instant) -> Vec<String> {
    loop {
        let pending: Vec<_> = links
            .iter()
            .filter(|link| !lock(&link.state).finished)
            .map(|link| link.name.clone())
            .collect();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if pending.is_empty() || remaining.is_zero() {
            return pending;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
    }
}
/// 业务作用：继续保留内部责任状态，即使持锁线程异常退出也不能静默遗失依赖。
/// 参数说明：mutex 为内部状态锁。返回：包含原状态的互斥保护。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
