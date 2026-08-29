//! 每个 Runner generation 独占的延迟索引和唯一驱动任务。

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::runner::RunnerInner;

/// 业务作用：保存当前 generation 的延迟任务索引、有效条目与接纳封口状态。
struct TimerState {
    heap: BinaryHeap<Reverse<(Instant, u64)>>,
    active: HashMap<u64, TimerEntry>,
    closed: bool,
}

#[derive(Debug, Clone, Copy)]
/// 业务作用：记录延迟任务本段截止时刻及超长延迟尚未进入计时器的剩余时长。
struct TimerEntry {
    deadline: Instant,
    remaining: Duration,
}

const MAX_TIMER_SEGMENT: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Runner 私有延迟索引；业务载荷保存在 Runner delayed 表，不进入定时堆。
pub(crate) struct RunnerTimer {
    state: Mutex<TimerState>,
    wake: Notify,
    cancelled: AtomicU64,
    compactions: AtomicU64,
}

impl RunnerTimer {
    /// 业务作用：创建尚未登记任务且开放接纳的 generation 私有定时器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由唯一驱动任务和并发 producer 共享的定时器。
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(TimerState {
                heap: BinaryHeap::new(),
                active: HashMap::new(),
                closed: false,
            }),
            wake: Notify::new(),
            cancelled: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
        })
    }

    /// 业务作用：把已进入 delayed 表的任务标识登记到绝对截止点，重复或关门登记必须拒绝。
    ///
    /// 参数说明：
    /// - `id`: generation 内不可复用提交标识。
    /// - `delay`: 从登记时刻开始计算的持续时间；超长值会分段等待，绝不提前触发。
    ///
    /// 返回：首次登记成功返回 true；关闭或重复标识返回 false。
    pub(crate) fn register(&self, id: u64, delay: Duration) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed || state.active.contains_key(&id) {
            return false;
        }
        let segment = delay.min(MAX_TIMER_SEGMENT);
        let deadline = Instant::now()
            .checked_add(segment)
            .unwrap_or_else(Instant::now);
        state.active.insert(
            id,
            TimerEntry {
                deadline,
                remaining: delay.saturating_sub(segment),
            },
        );
        state.heap.push(Reverse((deadline, id)));
        drop(state);
        self.wake.notify_one();
        true
    }

    /// 业务作用：立即取消尚未到期的逻辑项，并按几何阈值压缩惰性物理槽位；最后一项
    /// 离场时直接清空堆，使突发取消保持摊销线性且物理存量有界。
    ///
    /// 参数说明：
    /// - `id`: 需要撤销的提交标识。
    ///
    /// 返回：本次实际移除逻辑项返回 true；重复取消返回 false 且不增加累计量。
    pub(crate) fn cancel(&self, id: u64) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let removed = state.active.remove(&id).is_some();
        if removed {
            let stale = state.heap.len().saturating_sub(state.active.len());
            if state.active.is_empty() {
                state.heap.clear();
                self.compactions.fetch_add(1, Ordering::Relaxed);
            } else if stale >= state.active.len().max(64) {
                state.heap = state
                    .active
                    .iter()
                    .map(|(active_id, entry)| Reverse((entry.deadline, *active_id)))
                    .collect();
                self.compactions.fetch_add(1, Ordering::Relaxed);
            }
            self.cancelled.fetch_add(1, Ordering::Relaxed);
            drop(state);
            self.wake.notify_one();
        }
        removed
    }

    /// 业务作用：关闭新登记并取走全部未到期逻辑标识，停止路径据此逐笔发布拒绝终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：关门时仍活动的任务标识；重复关闭返回空列表。
    pub(crate) fn close_and_drain(&self) -> Vec<u64> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        let ids = state.active.drain().map(|(id, _)| id).collect();
        state.heap.clear();
        drop(state);
        self.wake.notify_waiters();
        self.wake.notify_one();
        ids
    }

    /// 业务作用：读取逻辑延迟项数量，取消后立即下降。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：active 索引当前大小。
    pub(crate) fn logical_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .len()
    }

    /// 业务作用：读取定时堆物理槽位数量，用于观测惰性删除的堆压缩边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前堆元素数，可能短暂大于逻辑项数。
    pub(crate) fn physical_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .heap
            .len()
    }

    /// 业务作用：读取 generation 内通过索引完成物理移除的定时取消累计量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功取消并移除物理槽位的次数。
    pub(crate) fn cancelled_count(&self) -> u64 {
        self.cancelled.load(Ordering::Acquire)
    }

    /// 业务作用：读取为保持定时物理槽位有界而重建堆的累计次数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：generation 内堆压缩次数。
    pub(crate) fn compaction_count(&self) -> u64 {
        self.compactions.load(Ordering::Acquire)
    }

    /// 业务作用：驱动唯一到期循环，删除取消槽、压缩堆并把到期标识交还 Runner 结算。
    ///
    /// 参数说明：
    /// - `inner`: 当前 generation Runner 内核；停止通过受监督句柄取得完整退出证明。
    ///
    /// 返回：关门且逻辑项为空，或 Runner generation 已释放时退出。
    pub(crate) async fn run(self: Arc<Self>, inner: Arc<RunnerInner>) {
        loop {
            let next = {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.closed && state.active.is_empty() {
                    return;
                }
                state.heap.peek().copied()
            };
            let notified = self.wake.notified();
            match next {
                Some(Reverse((deadline, _))) => {
                    let sleep = tokio::time::sleep_until(deadline);
                    tokio::pin!(sleep);
                    tokio::select! {
                        _ = &mut sleep => {}
                        _ = notified => continue,
                    }
                }
                None => notified.await,
            }
            let expired = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                let mut expired = Vec::new();
                while let Some(Reverse((deadline, id))) = state.heap.peek().copied() {
                    if deadline > now {
                        break;
                    }
                    state.heap.pop();
                    let Some(entry) = state.active.get(&id).copied() else {
                        continue;
                    };
                    if entry.deadline != deadline {
                        continue;
                    }
                    if entry.remaining.is_zero() {
                        state.active.remove(&id);
                        expired.push(id);
                    } else {
                        let segment = entry.remaining.min(MAX_TIMER_SEGMENT);
                        // 每段都从上一绝对边界推进；runtime 时钟一次跨过多段时，循环会连续
                        // 消耗已流逝段，避免把调度延迟重新加到业务截止时间上。
                        let next_deadline = entry
                            .deadline
                            .checked_add(segment)
                            .unwrap_or_else(|| now + MAX_TIMER_SEGMENT);
                        state.active.insert(
                            id,
                            TimerEntry {
                                deadline: next_deadline,
                                remaining: entry.remaining.saturating_sub(segment),
                            },
                        );
                        state.heap.push(Reverse((next_deadline, id)));
                    }
                }
                expired
            };
            for id in expired {
                inner.expire_delayed(id);
            }
        }
    }
}
