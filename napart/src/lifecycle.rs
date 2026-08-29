//! Runner 生命周期监督基础设施。
//!
//! 本模块只维护控制权、停止模式和 Tokio 子任务句柄，不包含分区业务状态。调用 Future
//! 被取消时，未完成 join 的句柄通过 RAII 自动回到注册表，后续 supervisor 可以继续收口。

#![allow(dead_code)]

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle as RuntimeHandle;
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinError, JoinHandle};

const MODE_RUNNING: u8 = 0;
const MODE_GRACEFUL_STOP: u8 = 1;
const MODE_FORCE_STOP: u8 = 2;

/// 当前 generation 的停止模式；状态只能向右单调升级。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleMode {
    /// 正常运行并允许业务数据面工作。
    Running,
    /// 已请求无损停止，等待既有任务排空。
    GracefulStop,
    /// 已请求有损停止，允许中止和冻结未收敛任务。
    ForceStop,
}

/// 受监督 Tokio 子任务类别，用于收口报告和稳定指标分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildTaskKind {
    /// 分区唯一 worker。
    Worker,
    /// Runner 集中观察任务。
    Observer,
    /// Runner 私有定时驱动。
    Timer,
    /// 已开始执行的业务 Future。
    Business,
    /// 在阻塞线程池释放未执行业务载荷并发布终态的清理任务。
    Cleanup,
}

/// 子任务在当前 generation 内的不可复用标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ChildTaskId(u64);

impl ChildTaskId {
    /// 业务作用：读取子任务标识的原始值，供诊断和停机报告关联同一条句柄记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 内不可复用的数值。
    pub(crate) fn get(self) -> u64 {
        self.0
    }
}

/// 子任务登记失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterError {
    /// 当前 generation 的任务标识已经耗尽。
    IdExhausted,
    /// 生命周期控制权已经损坏，不能再接纳新的后台任务。
    AuthorityFailed,
}

/// supervisor 启动失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SupervisorStartError {
    /// 当前 generation 尚有未 join 的 supervisor 句柄。
    PreviousNotJoined,
    /// 无法取得唯一 supervisor epoch。
    AuthorityFailed,
    /// 当前线程不在 Tokio runtime 上，无法创建受监督任务。
    RuntimeUnavailable,
}

/// supervisor 等待失败。
#[derive(Debug)]
pub(crate) enum SupervisorWaitError {
    /// Tokio 任务取消或异常退出。
    Join(JoinError),
    /// 句柄槽位与 lease 不一致，无法提供可靠退出证明。
    AuthorityLost,
}

/// 业务作用：把受监督子任务的类型、可等待句柄与强制中止权绑定到同一 lease 槽位。
struct ChildHandleEntry {
    kind: ChildTaskKind,
    handle: Option<JoinHandle<()>>,
    abort: AbortHandle,
}

/// 当前 generation 的生命周期控制核心。
pub(crate) struct LifecycleCore {
    generation: u64,
    mode: AtomicU8,
    authority_failed: AtomicBool,
    supervisor_active: AtomicBool,
    supervisor_epoch: AtomicU64,
    revision: AtomicU64,
    next_child_id: AtomicU64,
    children: Mutex<BTreeMap<ChildTaskId, ChildHandleEntry>>,
    wake: Notify,
}

impl LifecycleCore {
    /// 业务作用：创建一个 generation 独占的生命周期核心，停止模式从 Running 开始且尚无
    /// supervisor 与子任务。
    ///
    /// 参数说明：
    /// - `generation`: Runner 当前代次；旧代核心不得在重启时复位复用。
    ///
    /// 返回：可由控制句柄和后台监督任务共享的新核心。
    pub(crate) fn new(generation: u64) -> Arc<Self> {
        Arc::new(Self {
            generation,
            mode: AtomicU8::new(MODE_RUNNING),
            authority_failed: AtomicBool::new(false),
            supervisor_active: AtomicBool::new(false),
            supervisor_epoch: AtomicU64::new(0),
            revision: AtomicU64::new(0),
            next_child_id: AtomicU64::new(0),
            children: Mutex::new(BTreeMap::new()),
            wake: Notify::new(),
        })
    }

    /// 业务作用：读取本核心所属 generation，供迟到 supervisor 和子任务复验控制权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的代次。
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：读取当前停止模式；未知内部值按 ForceStop 处理，避免损坏状态继续开放路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Running、GracefulStop 或 ForceStop。
    pub(crate) fn mode(&self) -> LifecycleMode {
        match self.mode.load(Ordering::Acquire) {
            MODE_RUNNING => LifecycleMode::Running,
            MODE_GRACEFUL_STOP => LifecycleMode::GracefulStop,
            _ => LifecycleMode::ForceStop,
        }
    }

    /// 业务作用：把生命周期单调推进到无损停止；已经进入有损停止时不能降级。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次调用首次改变模式时返回 true。
    pub(crate) fn request_graceful_stop(&self) -> bool {
        let changed = self
            .mode
            .compare_exchange(
                MODE_RUNNING,
                MODE_GRACEFUL_STOP,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if changed {
            self.signal_change();
        }
        changed
    }

    /// 业务作用：把生命周期单调推进到有损停止，使 supervisor 和业务任务立即观察强制
    /// 收口权威。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次调用把模式从更低等级升级时返回 true。
    pub(crate) fn request_force_stop(&self) -> bool {
        let previous = self.mode.swap(MODE_FORCE_STOP, Ordering::AcqRel);
        let changed = previous != MODE_FORCE_STOP;
        if changed {
            self.signal_change();
        }
        changed
    }

    /// 业务作用：把无法维持唯一控制权的情况登记为权威失败，并同步升级为有损停止。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次调用首次登记失败时返回 true。
    pub(crate) fn fail_authority(&self) -> bool {
        let first = !self.authority_failed.swap(true, Ordering::AcqRel);
        self.request_force_stop();
        first
    }

    /// 业务作用：读取生命周期控制权是否已经损坏，供公开入口拒绝创建新 operation。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已经失去正常控制权时返回 true。
    pub(crate) fn authority_failed(&self) -> bool {
        self.authority_failed.load(Ordering::Acquire)
    }

    /// 业务作用：竞争当前 generation 的唯一 supervisor 权威；已有 supervisor 活跃时拒绝
    /// 第二个清理者。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时返回带新 epoch 的 RAII 权威 guard；竞争失败返回 None。
    pub(crate) fn claim_supervisor(self: &Arc<Self>) -> Option<SupervisorGuard> {
        if self
            .supervisor_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let epoch =
            match self
                .supervisor_epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_add(1)
                }) {
                Ok(previous) => previous + 1,
                Err(_) => {
                    self.supervisor_active.store(false, Ordering::Release);
                    self.fail_authority();
                    return None;
                }
            };
        Some(SupervisorGuard {
            core: self.clone(),
            epoch,
            active: true,
        })
    }

    /// 业务作用：读取当前 supervisor epoch，供控制回调拒绝迟到旧监督者的发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最近一次成功取得权威的 epoch；尚未取得时为零。
    pub(crate) fn supervisor_epoch(&self) -> u64 {
        self.supervisor_epoch.load(Ordering::Acquire)
    }

    /// 业务作用：登记一个 Tokio 子任务及其唯一 JoinHandle，并永久保留可克隆 AbortHandle，
    /// 使有损停止在 JoinHandle 被借出等待时仍能发出中止请求。
    ///
    /// 参数说明：
    /// - `kind`: 子任务类别。
    /// - `handle`: 新 spawn 子任务的唯一 join 权威。
    ///
    /// 返回：成功时返回当前 generation 内的任务标识；失权或标识耗尽时先 abort 再拒绝。
    pub(crate) fn register_child(
        &self,
        kind: ChildTaskKind,
        handle: JoinHandle<()>,
    ) -> Result<ChildTaskId, RegisterError> {
        if self.authority_failed() {
            handle.abort();
            return Err(RegisterError::AuthorityFailed);
        }
        let id =
            match self
                .next_child_id
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_add(1)
                }) {
                Ok(value) => ChildTaskId(value),
                Err(_) => {
                    handle.abort();
                    self.fail_authority();
                    return Err(RegisterError::IdExhausted);
                }
            };
        let abort = handle.abort_handle();
        let mut children = self.children.lock().unwrap_or_else(|e| e.into_inner());
        match children.entry(id) {
            Entry::Vacant(slot) => {
                slot.insert(ChildHandleEntry {
                    kind,
                    handle: Some(handle),
                    abort,
                });
            }
            Entry::Occupied(_) => {
                drop(children);
                handle.abort();
                self.fail_authority();
                return Err(RegisterError::AuthorityFailed);
            }
        }
        drop(children);
        self.signal_change();
        Ok(id)
    }

    /// 业务作用：临时取出一个子任务的唯一 JoinHandle 以等待退出；句柄不存在或正在被另一
    /// supervisor 等待时拒绝重复租用。
    ///
    /// 参数说明：
    /// - `id`: 目标子任务标识。
    ///
    /// 返回：成功时返回取消安全的 HandleLease；不存在或已租出时返回 None。
    pub(crate) fn lease_child(self: &Arc<Self>, id: ChildTaskId) -> Option<HandleLease> {
        let mut children = self.children.lock().unwrap_or_else(|e| e.into_inner());
        let entry = children.get_mut(&id)?;
        let handle = entry.handle.take()?;
        Some(HandleLease {
            core: self.clone(),
            id,
            kind: entry.kind,
            handle: Some(handle),
        })
    }

    /// 业务作用：向全部已登记子任务发出中止请求；AbortHandle 永久留在注册表，因此不受
    /// JoinHandle 当前是否被 lease 影响。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次发出中止请求的子任务数。
    pub(crate) fn abort_all(&self) -> usize {
        let handles = self
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|entry| entry.abort.clone())
            .collect::<Vec<_>>();
        for handle in &handles {
            handle.abort();
        }
        handles.len()
    }

    /// 业务作用：只中止指定类别的子任务，使有损停止能够保留 worker 继续清扫物理队列。
    ///
    /// 参数说明：
    /// - `kind`: 需要发出中止请求的子任务类别。
    ///
    /// 返回：本次找到并请求中止的句柄数。
    pub(crate) fn abort_kind(&self, kind: ChildTaskKind) -> usize {
        let handles = self
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|entry| entry.kind == kind)
            .map(|entry| entry.abort.clone())
            .collect::<Vec<_>>();
        for handle in &handles {
            handle.abort();
        }
        handles.len()
    }

    /// 业务作用：读取指定类别尚未取得 join 证明的任务数，供停止报告区分业务与控制任务。
    ///
    /// 参数说明：
    /// - `kind`: 需要统计的子任务类别。
    ///
    /// 返回：包含已结束但尚未 join、正在运行和正在被 lease 的全部条目数。
    pub(crate) fn child_count_by_kind(&self, kind: ChildTaskKind) -> usize {
        self.children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|entry| entry.kind == kind)
            .count()
    }

    /// 业务作用：读取尚未取得 join 证明的子任务总数，供停止收敛和资源指标使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：句柄注册表当前条目数，包含正在被 lease 等待的任务。
    pub(crate) fn child_count(&self) -> usize {
        self.children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// 业务作用：读取当前被 supervisor 借出等待的 JoinHandle 数，供诊断取消安全是否归还。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：handle 暂不在注册表槽位中的任务数。
    pub(crate) fn leased_count(&self) -> usize {
        self.children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|entry| entry.handle.is_none())
            .count()
    }

    /// 业务作用：枚举全部尚未取得 join 证明的子任务标识，供停止 supervisor 逐项租用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 generation 内标识排序的快照；返回后任务集合仍可能变化。
    pub(crate) fn child_ids(&self) -> Vec<ChildTaskId> {
        self.children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// 业务作用：枚举已经停止运行但尚未 join 的子任务，使常驻 supervisor 能及时清理业务
    /// 句柄而不阻塞等待仍活动的 worker。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 JoinHandle 可用且 `is_finished` 的任务标识快照。
    pub(crate) fn finished_child_ids(&self) -> Vec<ChildTaskId> {
        self.children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(|(id, entry)| {
                entry
                    .handle
                    .as_ref()
                    .is_some_and(JoinHandle::is_finished)
                    .then_some(*id)
            })
            .collect()
    }

    /// 业务作用：显式通知 supervisor 数据面或控制面发生进展，覆盖没有子任务登记变化的
    /// 队列排空与计数归零事件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；变化序号先发布再唤醒等待者。
    pub(crate) fn notify_progress(&self) {
        self.signal_change();
    }

    /// 业务作用：读取生命周期变化序号，供 supervisor 在检查条件前保存观察点并避免丢失
    /// 检查与等待之间发生的通知。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前单调变化序号。
    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    /// 业务作用：等待变化序号离开调用方观察值；先登记 Notify 再复验序号，覆盖检查与 await
    /// 之间的竞争窗口。
    ///
    /// 参数说明：
    /// - `observed`: 调用方检查收敛条件前读取的变化序号。
    ///
    /// 返回：已经发生或随后发生任一生命周期变化时返回；调用方必须重新读取全部权威状态。
    pub(crate) async fn changed_since(&self, observed: u64) {
        loop {
            let notified = self.wake.notified();
            if self.revision() != observed {
                return;
            }
            crate::shield_future(notified).await;
        }
    }

    /// 业务作用：提交一次生命周期变化序号并唤醒 supervisor；序号耗尽时关闭正常权威，避免
    /// 回绕后旧观察点被误判为当前状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；变化序号发布先于通知。
    fn signal_change(&self) {
        if self
            .revision
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .is_err()
        {
            self.authority_failed.store(true, Ordering::Release);
            self.mode.store(MODE_FORCE_STOP, Ordering::Release);
        }
        self.wake.notify_waiters();
        self.wake.notify_one();
    }
}

/// 业务作用：保存 supervisor 当前 epoch 的等待与中止句柄，以及最后取得退出证明的 epoch。
struct SupervisorSlot {
    epoch: Option<u64>,
    handle: Option<JoinHandle<()>>,
    abort: Option<AbortHandle>,
    joined_epoch: u64,
}

/// 当前 Runner generation 的控制对象；supervisor 自身句柄与普通子任务注册表分开保存。
pub(crate) struct GenerationControl {
    generation: u64,
    lifecycle: Arc<LifecycleCore>,
    supervisor: Mutex<SupervisorSlot>,
}

impl GenerationControl {
    /// 业务作用：创建全新 generation 控制对象；重启必须构造新实例，不能复位旧停止模式或
    /// 复用旧句柄注册表。
    ///
    /// 参数说明：
    /// - `generation`: Runner 的新代次。
    ///
    /// 返回：尚未启动 supervisor、停止模式为 Running 的共享控制对象。
    pub(crate) fn new(generation: u64) -> Arc<Self> {
        Arc::new(Self {
            generation,
            lifecycle: LifecycleCore::new(generation),
            supervisor: Mutex::new(SupervisorSlot {
                epoch: None,
                handle: None,
                abort: None,
                joined_epoch: 0,
            }),
        })
    }

    /// 业务作用：读取本控制对象所属 generation，供 RunnerControl 安装与摘除时执行身份 CAS。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的代次。
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：取得当前 generation 独占的生命周期核心，供 worker、timer 和 observer
    /// 登记句柄与读取停止模式。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享同一代停止权威和子任务注册表的 Arc。
    pub(crate) fn lifecycle(&self) -> Arc<LifecycleCore> {
        self.lifecycle.clone()
    }

    /// 业务作用：为当前 generation 启动唯一 supervisor 并保存其 JoinHandle 与 AbortHandle；
    /// 未取得上一 supervisor 的 join 证明前拒绝替换。
    ///
    /// 参数说明：
    /// - `future`: supervisor 主循环；不得强持有本 `GenerationControl`，避免与句柄槽形成环。
    ///
    /// 返回：成功返回新 supervisor epoch；无 runtime、上一句柄未 join 或权威失败时明确拒绝。
    pub(crate) fn spawn_supervisor(
        self: &Arc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<u64, SupervisorStartError> {
        let runtime =
            RuntimeHandle::try_current().map_err(|_| SupervisorStartError::RuntimeUnavailable)?;
        let mut slot = self.supervisor.lock().unwrap_or_else(|e| e.into_inner());
        if slot.epoch.is_some() || slot.handle.is_some() || slot.abort.is_some() {
            return Err(SupervisorStartError::PreviousNotJoined);
        }
        let guard = self
            .lifecycle
            .claim_supervisor()
            .ok_or(SupervisorStartError::AuthorityFailed)?;
        let epoch = guard.epoch();
        let handle = runtime.spawn(async move {
            let _authority = guard;
            future.await;
        });
        let abort = handle.abort_handle();
        slot.epoch = Some(epoch);
        slot.handle = Some(handle);
        slot.abort = Some(abort);
        drop(slot);
        self.lifecycle.signal_change();
        Ok(epoch)
    }

    /// 业务作用：临时取得 supervisor 的唯一 JoinHandle；等待 Future 被取消时 lease 会自动
    /// 归还句柄，后续停止调用仍能取得退出证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在尚未 join 且未被其它等待者租用的 supervisor 时返回 lease，否则返回 None。
    pub(crate) fn lease_supervisor(self: &Arc<Self>) -> Option<SupervisorLease> {
        let mut slot = self.supervisor.lock().unwrap_or_else(|e| e.into_inner());
        let epoch = slot.epoch?;
        let handle = slot.handle.take()?;
        Some(SupervisorLease {
            control: self.clone(),
            epoch,
            handle: Some(handle),
        })
    }

    /// 业务作用：向当前 supervisor 发出中止请求；AbortHandle 始终保留在槽位，因此等待者
    /// 租出 JoinHandle 时仍可执行最后兜底。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在当前 supervisor 并已发出请求时返回 true。
    pub(crate) fn abort_supervisor(&self) -> bool {
        let abort = self
            .supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .abort
            .clone();
        if let Some(abort) = abort {
            abort.abort();
            true
        } else {
            false
        }
    }

    /// 业务作用：读取当前尚未 join 的 supervisor epoch，供停止调用复验等待对象属于本代。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：有待取得退出证明的 supervisor 时返回其 epoch，否则返回 None。
    pub(crate) fn supervisor_epoch(&self) -> Option<u64> {
        self.supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .epoch
    }

    /// 业务作用：判断现役 supervisor 是否已经停止运行但尚未取得 join 证明，使公开停止
    /// 等待者能接回异常离场句柄并建立唯一替代者。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：句柄仍在槽位且 Tokio 已标记完成时返回 true；被其它等待者租出时返回 false。
    pub(crate) fn supervisor_finished(&self) -> bool {
        self.supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
    }

    /// 业务作用：读取最近已经取得 join 证明的 supervisor epoch，供并发等待者复用终局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未 join 任何 supervisor 时为零，否则为最近已收口 epoch。
    pub(crate) fn joined_supervisor_epoch(&self) -> u64 {
        self.supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .joined_epoch
    }
}

impl Drop for GenerationControl {
    /// 业务作用：最后一个 generation 控制引用离场时先进入有损保护态，再中止 supervisor 与
    /// 全部登记子任务；Drop 不等待退出证明，正常停止必须显式 lease 并 join。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；所有可达的监督任务都收到中止请求。
    fn drop(&mut self) {
        // 先关闭正常数据面权威，再发出异步中止，避免后台任务在看到请求前继续接纳新副作用。
        self.lifecycle.request_force_stop();
        let slot = self.supervisor.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(abort) = &slot.abort {
            abort.abort();
        }
        self.lifecycle.abort_all();
    }
}

/// supervisor JoinHandle 的取消安全租约。
pub(crate) struct SupervisorLease {
    control: Arc<GenerationControl>,
    epoch: u64,
    handle: Option<JoinHandle<()>>,
}

impl SupervisorLease {
    /// 业务作用：读取本 lease 关联的 supervisor epoch，供等待者拒绝跨代或替代者混淆。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：租出 JoinHandle 时冻结的 epoch。
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// 业务作用：等待 supervisor 真实退出并提交 join 证明；等待 Future 被取消时 Drop 会归还
    /// 句柄，不会把仍运行或已完成但未证明的任务 detach。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：正常退出返回 Ok；Tokio 取消/异常返回 Join，槽位失权返回 AuthorityLost。
    pub(crate) async fn join(mut self) -> Result<(), SupervisorWaitError> {
        let result = match self.handle.as_mut() {
            Some(handle) => handle.await,
            None => {
                self.control.lifecycle.fail_authority();
                return Err(SupervisorWaitError::AuthorityLost);
            }
        };
        self.handle.take();
        let mut slot = self
            .control
            .supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.epoch != Some(self.epoch) || slot.handle.is_some() || slot.abort.is_none() {
            drop(slot);
            self.control.lifecycle.fail_authority();
            return Err(SupervisorWaitError::AuthorityLost);
        }
        slot.epoch = None;
        slot.abort = None;
        slot.joined_epoch = self.epoch;
        drop(slot);
        self.control.lifecycle.signal_change();
        result.map_err(SupervisorWaitError::Join)
    }
}

impl Drop for SupervisorLease {
    /// 业务作用：supervisor join 等待被取消或异常展开时归还唯一 JoinHandle；槽位不一致时
    /// 中止租出的任务并关闭本 generation 权威，不能覆盖另一句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；句柄正常归还或在权威不一致时中止并关门。
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let mut slot = self
            .control
            .supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.epoch != Some(self.epoch) || slot.handle.is_some() || slot.abort.is_none() {
            drop(slot);
            handle.abort();
            self.control.lifecycle.fail_authority();
            return;
        }
        slot.handle = Some(handle);
        drop(slot);
        self.control.lifecycle.signal_change();
    }
}

impl Drop for LifecycleCore {
    /// 业务作用：控制核心最终释放时向仍登记的子任务发出中止请求，避免 JoinHandle 的默认
    /// detach 语义让执行域在失去控制句柄后继续运行；Drop 不等待退出证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；每个仍登记的子任务都收到中止请求。
    fn drop(&mut self) {
        let children = self.children.get_mut().unwrap_or_else(|e| e.into_inner());
        for entry in children.values() {
            entry.abort.abort();
        }
    }
}

/// 当前 generation 的唯一 supervisor 权威。
pub(crate) struct SupervisorGuard {
    core: Arc<LifecycleCore>,
    epoch: u64,
    active: bool,
}

impl SupervisorGuard {
    /// 业务作用：读取本 supervisor 的权威 epoch，供每次外部副作用前复验仍是现役控制者。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得权威时冻结的 supervisor epoch。
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// 业务作用：复验本 guard 仍是当前 generation 的现役 supervisor，防止迟到旧任务发布
    /// 生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：epoch 相同且权威仍激活时返回 true。
    pub(crate) fn is_current(&self) -> bool {
        self.active
            && self.core.supervisor_active.load(Ordering::Acquire)
            && self.core.supervisor_epoch() == self.epoch
    }
}

impl Drop for SupervisorGuard {
    /// 业务作用：supervisor 正常或异常离场时释放唯一权威，使替代者能够通过新 epoch 接管。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；仅现役 epoch 能解除权威并发布进展通知。
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if self.core.supervisor_epoch() == self.epoch {
            self.core.supervisor_active.store(false, Ordering::Release);
            self.core.signal_change();
        }
        self.active = false;
    }
}

/// 子任务 JoinHandle 的取消安全租约。
pub(crate) struct HandleLease {
    core: Arc<LifecycleCore>,
    id: ChildTaskId,
    kind: ChildTaskKind,
    handle: Option<JoinHandle<()>>,
}

impl HandleLease {
    /// 业务作用：读取租约对应的子任务类别，供 supervisor 生成分类停止报告。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：登记时冻结的任务类别。
    pub(crate) fn kind(&self) -> ChildTaskKind {
        self.kind
    }

    /// 业务作用：等待子任务取得真实退出结果；等待 Future 被取消时本 lease 的 Drop 会把
    /// JoinHandle 放回注册表，后续 supervisor 可以继续等待。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Tokio 子任务的 join 结果；无论正常、取消或 panic，返回前都从注册表删除该任务。
    pub(crate) async fn join(mut self) -> Result<(), JoinError> {
        let result = match self.handle.as_mut() {
            Some(handle) => handle.await,
            None => {
                self.core.fail_authority();
                return Ok(());
            }
        };
        self.handle.take();
        let removed = self
            .core
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
        if removed.is_none() {
            self.core.fail_authority();
        }
        self.core.signal_change();
        result
    }
}

impl Drop for HandleLease {
    /// 业务作用：join 等待被取消或异常展开时把唯一 JoinHandle 归还原注册槽位，防止任务
    /// detach 后失去退出证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；句柄正常归还原槽位，槽位失权时中止任务并关闭权威。
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let mut children = self.core.children.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = children.get_mut(&self.id) else {
            drop(children);
            handle.abort();
            self.core.fail_authority();
            return;
        };
        if entry.handle.is_some() {
            drop(children);
            handle.abort();
            self.core.fail_authority();
            return;
        }
        entry.handle = Some(handle);
        drop(children);
        self.core.signal_change();
    }
}
