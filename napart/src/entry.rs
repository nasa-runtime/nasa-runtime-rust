//! 任务载荷、权威条目与公开提交句柄。
//!
//! 队列和盗洞只移动 `Arc<TaskEnvelope>`。业务 Future、owner、物理 retention、容量许可与
//! 终态都保存在唯一 `TaskEntry` 中，业务代码不能读写框架所有权。

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use crate::route::TypeState;
use crate::runner::RunnerInner;

mod task;

pub(crate) use task::{
    CancelDecision, EntryState, LogicalContainer, LogicalOwner, MoveStage, MoveTicket, Owner,
    PhysicalRetention, TaskAuthorityError, TaskEntry,
};

/// 类型擦除且固定 Pin 的业务 Future。
pub(crate) type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// 任务在公开句柄中的生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskStatus {
    /// 延迟任务已登记，尚未进入类型物理队列。
    Delayed,
    /// 已受理并等待严格次序或业务执行权。
    Queued,
    /// 业务 Future 正在执行。
    Running,
    /// 业务 Future 正常完成。
    Completed,
    /// 调用方在任务运行前取得取消权。
    Cancelled,
    /// 任务未完成物理受理。
    Rejected,
    /// 已受理任务失去安全推进条件或业务 Future 展开。
    Failed,
}

/// 队列中的任务投影；严格序号跨主队列、stock、incremental 与 staging 保持不变。
pub(crate) struct TaskEnvelope {
    authority: Arc<TaskEntry<Job>>,
    type_state: Arc<TypeState>,
    strict_sequence: OnceLock<u64>,
    queued_accounted: AtomicBool,
}

/// 延迟任务与物理任务投影之间的单次绑定；到期前只保留 TaskEntry，不创建 TypeState。
pub(crate) type EnvelopeBinding = Arc<OnceLock<Arc<TaskEnvelope>>>;

impl TaskEnvelope {
    /// 业务作用：把任务权威、固定类型状态和严格受理序号绑定为可在物理容器间移动的单元。
    ///
    /// 参数说明：
    /// - `authority`: 唯一任务权威。
    /// - `type_state`: 固定 `(home, TaskType)` 状态。
    /// - `strict_sequence`: 严格类型的受理序号；非严格类型为 None。
    ///
    /// 返回：不复制业务 Future 的共享任务投影。
    pub(crate) fn new(
        authority: Arc<TaskEntry<Job>>,
        type_state: Arc<TypeState>,
        strict_sequence: Option<u64>,
    ) -> Arc<Self> {
        let sequence = OnceLock::new();
        if let Some(value) = strict_sequence {
            let _ = sequence.set(value);
        }
        type_state.increment_queued();
        Arc::new(Self {
            authority,
            type_state,
            strict_sequence: sequence,
            queued_accounted: AtomicBool::new(true),
        })
    }

    /// 业务作用：取得任务权威引用，供唯一 consumer 复验 owner、retention 与状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务权威共享引用。
    pub(crate) fn authority(&self) -> &Arc<TaskEntry<Job>> {
        &self.authority
    }

    /// 业务作用：取得不可变类型状态，供执行顺序和终态计数结算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定类型状态共享引用。
    pub(crate) fn type_state(&self) -> &Arc<TypeState> {
        &self.type_state
    }

    /// 业务作用：读取严格受理序号，跨物理移动时不得重新签发。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：严格类型返回序号，非严格类型返回 None。
    pub(crate) fn strict_sequence(&self) -> Option<u64> {
        self.strict_sequence.get().copied()
    }

    /// 业务作用：恰好撤销一次已受理排队投影，使容量等待者与真实任务指标保持隔离。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次或重复结算且底层计数有效时返回 true；底层类型计数下溢时返回 false。
    pub(crate) fn settle_queued(&self) -> bool {
        if !self.queued_accounted.swap(false, Ordering::AcqRel) {
            return true;
        }
        self.type_state.decrement_queued()
    }
}

/// 分区任务的稳定终态句柄。
///
/// 句柄保留 `TaskEntry` 的权威 Arc，但不拥有 Runner 控制权。丢弃句柄不取消任务；取消必须
/// 显式调用 [`Submission::cancel`]。
pub struct Submission {
    pub(crate) entry: Arc<TaskEntry<Job>>,
    pub(crate) envelope: EnvelopeBinding,
    pub(crate) inner: Weak<RunnerInner>,
    pub(crate) delayed_id: Option<u64>,
}

impl Submission {
    /// 业务作用：读取任务当前公开状态；稳定终态发布前的结算窗口仍按活动状态展示。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时状态快照；非终态返回后仍可能变化。
    pub fn status(&self) -> TaskStatus {
        match self.entry.state() {
            Ok(EntryState::Delayed) => TaskStatus::Delayed,
            Ok(EntryState::Enqueueing | EntryState::Queued | EntryState::Moving) => {
                TaskStatus::Queued
            }
            Ok(EntryState::Running | EntryState::Terminating) => TaskStatus::Running,
            Ok(EntryState::Completed) => TaskStatus::Completed,
            Ok(EntryState::Cancelled) => TaskStatus::Cancelled,
            Ok(EntryState::Rejected) => TaskStatus::Rejected,
            Ok(EntryState::Failed) | Err(_) => TaskStatus::Failed,
        }
    }

    /// 业务作用：在任务开始执行前竞争取消权；Moving 期间只登记协作意图，由现役移动发布者
    /// 在新容器可见前完成终态收口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次请求取得取消权或首次登记移动协作意图时返回 true；true 只表示意图已被
    /// 权威接纳，载荷仍在物理容器或受监督清理任务中时需通过 `await_outcome` 等待稳定终态；
    /// 已经运行或终态返回 false。
    pub fn cancel(&self) -> bool {
        match self.entry.request_cancel() {
            CancelDecision::Accepted => {
                let inner = self.inner.upgrade();
                if let (Some(inner), Some(id)) = (&inner, self.delayed_id) {
                    inner.retire_delayed(id);
                }
                if self.delayed_id.is_some() && self.envelope.get().is_none() {
                    if let Some(owner) = &inner {
                        // 未绑定物理容器的延迟任务没有 consumer 可代为释放载荷；先把清理
                        // JoinHandle 纳入本代监督，再让公开取消返回，避免不可信析构阻塞调用线程。
                        if owner.schedule_delayed_cancel(self.entry.clone(), self.envelope.clone())
                        {
                            return true;
                        }
                    }
                }
                let envelope = self.envelope.get().cloned();
                let result = self.entry.finalize_termination_with_accounting(
                    |logical| {
                        if let Some(envelope) = &envelope {
                            if let Some(inner) = &inner {
                                inner.commit_logical(envelope.type_state(), logical);
                            } else {
                                let _ = envelope.type_state().decrement_logical();
                            }
                        }
                    },
                    |terminal| {
                        if let Some(envelope) = &envelope {
                            if let Some(inner) = &inner {
                                inner.settle_queued_projection(envelope);
                            } else {
                                let _ = envelope.settle_queued();
                            }
                        }
                        if let Some(inner) = &inner {
                            inner.commit_terminal_accounting(terminal, false, false);
                        }
                    },
                );
                if result.is_ok() {
                    if let Some(envelope) = &envelope {
                        envelope
                            .type_state()
                            .settle_strict(envelope.strict_sequence());
                    }
                    if let Some(inner) = inner {
                        inner.lifecycle().notify_progress();
                    }
                    true
                } else if matches!(
                    result,
                    Err(TaskAuthorityError::RetentionMismatch | TaskAuthorityError::SettlementOwned)
                ) && self
                    .entry
                    .state()
                    .is_ok_and(|state| state == EntryState::Terminating || state.is_terminal())
                {
                    // 已发布队列的任务只能由唯一 consumer 解除物理持有；取消方只唤醒责任
                    // slot；另一结算者已取得终态 owner 时也由其完成账本，不能重复结算。
                    if let (Some(inner), Some(envelope)) = (inner, envelope) {
                        inner.notify_pending_terminal(&envelope);
                    }
                    true
                } else {
                    if let Some(inner) = inner {
                        if let Some(envelope) = envelope {
                            inner.record_authority_failure(envelope, "cancel_settlement_failed");
                        } else {
                            inner.fail_unbound_delayed("cancel_settlement_failed");
                        }
                    }
                    false
                }
            }
            CancelDecision::DeferredMoving => true,
            CancelDecision::AlreadyDeferred | CancelDecision::TooLate => false,
        }
    }

    /// 业务作用：读取拒绝、取消或失败终态的稳定原因码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：带原因终态返回静态原因文本；活动状态与正常完成返回 None。
    pub fn reason(&self) -> Option<&'static str> {
        self.entry.reason()
    }

    /// 业务作用：取消安全地等待任务发布稳定终态，调用 Future 被丢弃不改变任务执行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Completed、Cancelled、Rejected 或 Failed；内部状态损坏按 Failed 返回。
    pub async fn await_outcome(&self) -> TaskStatus {
        match self.entry.await_terminal().await {
            Ok(EntryState::Completed) => TaskStatus::Completed,
            Ok(EntryState::Cancelled) => TaskStatus::Cancelled,
            Ok(EntryState::Rejected) => TaskStatus::Rejected,
            Ok(EntryState::Failed) | Err(_) => TaskStatus::Failed,
            Ok(_) => TaskStatus::Failed,
        }
    }
}

impl std::fmt::Debug for Submission {
    /// 业务作用：只渲染任务身份、状态和稳定原因，不输出业务 Future 或内部路由引用。
    ///
    /// 参数说明：
    /// - `f`: 接收脱敏调试字段的格式化器。
    ///
    /// 返回：全部安全字段成功写入时返回 Ok；底层写入失败时返回格式错误。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Submission")
            .field("id", &self.entry.id())
            .field("status", &self.status())
            .field("reason", &self.reason())
            .finish()
    }
}
