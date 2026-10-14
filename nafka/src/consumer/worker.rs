//! 消费组共享的有界持久业务线程；调用完成与确认权威相互独立。

use crate::error::{NafkaError, Result};
use crate::types::Tp;
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{sync_channel, SyncSender},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Instant,
};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// 一个已准入调用的唯一完成槽，不增加独立无界结果队列。
struct Completion<T> {
    outcome: Mutex<Option<(Result<T>, Instant)>>,
    finished: Arc<AtomicBool>,
}

/// 完成票据持续承担输入责任；逻辑超时不会使票据自动完成。
pub(crate) struct WorkerTicket<T>(Arc<Completion<T>>);

impl<T> WorkerTicket<T> {
    /// 业务作用：判定业务及其捕获资源是否已实际退出。
    /// 参数说明：无。
    /// 返回：结果及业务资源析构均结束后为 true。
    pub(crate) fn is_finished(&self) -> bool {
        self.0.finished.load(Ordering::Acquire)
    }

    /// 业务作用：把已完成解码或业务调用的结论移交唯一 owner。
    /// 参数说明：无。
    /// 返回：结论或隔离的执行异常及实际完成时刻；只能在完成后调用，不能据逻辑超时提前释放名额。
    pub(crate) fn finish(self) -> (Result<T>, Instant) {
        self.0
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("实际完成后才收回业务票据")
    }
}

/// 每个线程至多接纳一个责任，不使用预排队扩大业务输入占用。
struct Slot {
    sender: SyncSender<WorkItem>,
    busy: Arc<AtomicBool>,
    join: JoinHandle<()>,
}

/// 调用责任及其唯一 owner 的完成通知；通知本身不携带确认权威。
struct WorkItem {
    job: Job,
    finished: Arc<AtomicBool>,
    permit: HandlerPermit,
    owner: std::thread::Thread,
}

/// 全组共用原生线程与执行额度，空闲 owner 不单独保留线程份额。
struct SharedPool {
    capacity: usize,
    admission: Mutex<Admission>,
    slots: Mutex<Vec<Slot>>,
}

#[derive(Default)]
struct Admission {
    reserved: usize,
    domains: BTreeMap<Tp, Domain>,
}

#[derive(Default)]
struct Domain {
    exclusive: bool,
    keys: BTreeSet<Arc<[u8]>>,
}

/// owner 的共享线程入口及本槽实际责任；异常收尾只等待本槽，不等待其它健康槽。
pub(crate) struct HandlerPool {
    shared: Arc<SharedPool>,
    active: Arc<AtomicUsize>,
}

/// 已准入但尚未实际退出的调用额度；超时或票据丢弃不能归还它。
pub(crate) struct HandlerPermit {
    shared: Arc<SharedPool>,
    active: Arc<AtomicUsize>,
    domain: Option<(Tp, Option<Arc<[u8]>>)>,
}

impl Drop for HandlerPermit {
    /// 业务作用：实际闭包及其捕获资源退出后归还组执行额度和本槽关闭责任。
    /// 参数说明：无。
    /// 返回：只减少本凭证的一份占用，不改变业务确认结果。
    fn drop(&mut self) {
        let mut admission = self
            .shared
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.reserved -= 1;
        if let Some((tp, key)) = &self.domain {
            let domain = admission.domains.get_mut(tp).expect("调用拥有有效顺序凭证");
            if let Some(key) = key {
                domain.keys.remove(key);
            } else {
                domain.exclusive = false;
            }
            if !domain.exclusive && domain.keys.is_empty() {
                admission.domains.remove(tp);
            }
        }
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl HandlerPool {
    /// 业务作用：冻结消费组的共享业务执行上限，不在构造时创建空闲线程。
    /// 参数说明：capacity 为整个组允许同时执行的调用数。
    /// 返回：空池，首个实际调用触发线程创建；owner 入口复用同一上限。
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            shared: Arc::new(SharedPool {
                capacity,
                admission: Mutex::new(Admission::default()),
                slots: Mutex::new(Vec::new()),
            }),
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 业务作用：为一个 owner 建立独立关闭责任，同时借用组内所有空闲执行额度。
    /// 参数说明：无。
    /// 返回：同一共享池的新入口；不增加组上限，也不创建线程。
    pub(crate) fn owner(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 业务作用：在移动业务输入前原子预留全组执行名额，防止各槽同时超额派发。
    /// 参数说明：domain 为业务分区及原始 key；空 key 独占分区，整个参数为 None 时仅用于无业务副作用的框架准备。
    /// 返回：实际责任凭证；容量或旧 owner 的顺序责任阻挡时返回 None，owner 应继续协调等待。
    pub(crate) fn reserve(&self, domain: Option<(&Tp, Option<&[u8]>)>) -> Option<HandlerPermit> {
        let mut admission = self
            .shared
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.reserved >= self.shared.capacity {
            return None;
        }
        let mut owned_domain = None;
        if let Some((tp, key)) = domain {
            // assignment 改变不会终止旧调用，组内接管必须等原顺序域的真实副作用责任退出。
            if admission.domains.get(tp).is_some_and(|state| {
                state.exclusive || key.is_none() || key.is_some_and(|key| state.keys.contains(key))
            }) {
                return None;
            }
            let state = admission.domains.entry(tp.clone()).or_default();
            let key = key.map(Arc::<[u8]>::from);
            if let Some(key) = &key {
                state.keys.insert(Arc::clone(key));
            } else {
                state.exclusive = true;
            }
            // 顺序域表与退出凭证共享同一完整键，不额外复制原始字节。
            owned_domain = Some((tp.clone(), key));
        }
        admission.reserved += 1;
        self.active.fetch_add(1, Ordering::AcqRel);
        Some(HandlerPermit {
            shared: Arc::clone(&self.shared),
            active: Arc::clone(&self.active),
            domain: owned_domain,
        })
    }

    /// 业务作用：会话异常退出时仍等待实际调用归还，防止新会话与旧外部副作用重叠。
    /// 参数说明：无。
    /// 返回：全部业务闭包已退出；无法强制取消的调用会持续保留关闭责任。
    pub(crate) fn await_idle(&self) {
        loop {
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
    }

    /// 业务作用：复用空闲线程执行解码或业务调用，保持非 Send future 在同一线程构造和完成。
    /// 参数说明：permit 为本入口预留的全组额度；job 为拥有输入的闭包，业务 future 必须在闭包内创建。
    /// 返回：实际完成票据，panic 被隔离为执行失败；凭证不匹配或线程创建失败时不执行新工作。
    pub(crate) fn submit<T: Send + 'static>(
        &self,
        permit: HandlerPermit,
        job: impl FnOnce() -> T + Send + 'static,
    ) -> Result<WorkerTicket<T>> {
        if !Arc::ptr_eq(&permit.shared, &self.shared) || !Arc::ptr_eq(&permit.active, &self.active)
        {
            return Err(NafkaError::Lifecycle("业务执行凭证不属于当前 owner".into()));
        }
        let mut slots = self
            .shared
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reusable = slots
            .iter()
            .position(|slot| !slot.busy.load(Ordering::Acquire));
        let index = if let Some(index) = reusable {
            index
        } else {
            // 线程不能作为溢出队列；owner 必须先收割既有责任，再授权后继调用。
            if slots.len() >= self.shared.capacity {
                return Err(NafkaError::Lifecycle("业务线程额度已满".into()));
            }
            let (sender, receiver) = sync_channel::<WorkItem>(1);
            let busy = Arc::new(AtomicBool::new(false));
            let activity = Arc::clone(&busy);
            let join = std::thread::Builder::new()
                .name("nafka-handler".into())
                .spawn(move || {
                    while let Ok(WorkItem {
                        job,
                        finished,
                        permit,
                        owner,
                    }) = receiver.recv()
                    {
                        job();
                        // 业务闭包及其上下文先退出，再交还额度并发布完成；迟到确认仍由调用票据的权威拒绝。
                        activity.store(false, Ordering::Release);
                        // 先让共享线程可复用，再归还准入凭证，避免新 owner 获额后看见所有线程仍忙。
                        drop(permit);
                        finished.store(true, Ordering::Release);
                        // 完成事实发布后才唤醒 owner；仅缩短本地等待，不中断 native 提交或改变确认门禁。
                        owner.unpark();
                    }
                })
                .map_err(|_| NafkaError::Lifecycle("无法创建业务线程".into()))?;
            slots.push(Slot { sender, busy, join });
            slots.len() - 1
        };
        let completion = Arc::new(Completion {
            outcome: Mutex::new(None),
            finished: Arc::new(AtomicBool::new(false)),
        });
        let result = Arc::clone(&completion);
        let task: Job = Box::new(move || {
            let outcome = catch_unwind(AssertUnwindSafe(job))
                .map_err(|_| NafkaError::Lifecycle("业务或解码调用异常退出".into()));
            *result
                .outcome
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((outcome, Instant::now()));
        });
        let slot = &slots[index];
        slot.busy.store(true, Ordering::Release);
        if let Err(failed) = slot.sender.try_send(WorkItem {
            job: task,
            finished: Arc::clone(&completion.finished),
            permit,
            owner: std::thread::current(),
        }) {
            slot.busy.store(false, Ordering::Release);
            drop(failed);
            return Err(NafkaError::Lifecycle("业务线程邮箱不可用".into()));
        }
        Ok(WorkerTicket(completion))
    }
}

impl Drop for SharedPool {
    /// 业务作用：全部 owner 及实际调用退出后关闭共享线程，防止反复启动消费组积累后台资源。
    /// 参数说明：无。
    /// 返回：全部池线程退出；仍有业务票据时由 owner 保留池的生命周期。
    fn drop(&mut self) {
        let slots = self
            .slots
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for Slot { sender, join, .. } in slots.drain(..) {
            drop(sender);
            // 异常析构若发生于最后一个业务闭包，当前线程在邮箱断开后自行退出，不能同步等待自身。
            if join.thread().id() != std::thread::current().id() {
                let _ = join.join();
            }
        }
    }
}
