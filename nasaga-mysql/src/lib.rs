//! NASA Saga 的 MySQL store：状态、timer、审计、配额持久化与数据库 CAS。
//!
//! `nasaga-core` 负责封闭状态机与身份派生等纯裁决，本 crate 负责把裁决结果以正确的
//! 事务边界与唯一键落库。Orchestrator
//! 的推进事务以"单一本地事务"为前提——Saga 表必须与 Orchestrator 自身的 Inbox/Outbox
//! **同库**，本 crate 不提供跨库变体。
//!
//! # 事务合同（调用方必须遵守）
//!
//! - **全部写路径要求 ambient `natx` 事务**（`natx::run`/`#[transactional]`），事务缺失
//!   直接报错，绝不静默 autocommit。这正是"事务内 transition + Outbox"的成立方式：
//!   Inbox claim、CAS 推进、transition 审计行、命令 Outbox（`naoutbox-mysql`）、Audit
//!   在**同一 COMMIT** 内生效或一起消失。
//! - **唯一例外是 [`MySqlSagaStore::claim_due_timers`]**：租约领取必须独立提交、立即
//!   对其它副本可见，因此强制在事务外调用。该入口消费不可复制的
//!   [`TimerFencingToken`] 并返回 [`TimerClaimBatch`]，禁止裸字符串或跨轮复用同一权威。
//! - [`CasOutcome::Conflict`]/[`CasOutcome::DuplicateTrigger`] 与 [`TimerFencing::Lost`]
//!   都要求调用方**放弃提交当前事务**：失去 CAS/租约权威后继续写 Outbox 是脑裂写入。
//!
//! # 身份与去重面
//!
//! - `effect_id` 跨 attempt 稳定、`command_id` 每 attempt 变化（`nasaga-core` 派生）；
//!   `UNIQUE(command_id)` 建在统一 attempt journal 上，`saga_step` 的 phase 列只是投影。
//! - 创建幂等靠 `UNIQUE(tenant_id, workflow_name, business_key)`；同一触发只推进一次
//!   靠 `UNIQUE(saga_id, trigger_kind, trigger_id)`；`transition_seq` 直接取 CAS 推进后
//!   的 `version`，不存在第二个序列来源。
//! - 在飞实例配额在创建事务内预留、终态事务内释放；变更类管理动作预算与对应动作同事务提交。
//!   存量账本必须先按非终态事实对账并置初始化标记，不能把空账本当成真实零用量。
//! - `MANUALLY_CLOSED` 的唯一入边要求同事务管理审计；旧读者尚未全部退出前，部署不得开启产生
//!   该终态的能力。
//!
//! 错误一律脱敏为 [`SagaStoreError`]（不回显 SQL/凭据/业务键/payload）。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod action_rate;
mod audit;
mod error;
mod instance;
mod metrics;
mod participant;
mod quota;
mod row;
mod schema;
mod stepjournal;
mod timer;

pub use audit::{
    AttemptConflictFact, SagaConflictFactRow, SagaConflictKind, SagaControlAuditRow,
    SagaManagementAuditRow, SagaTransitionAuditRow,
};
pub use error::SagaStoreError;

/// 人工关闭动作在管理审计表中的稳定 action 名。
///
/// `MANUALLY_CLOSED` 唯一入边的同事务证据检查与管理入口的审计写入必须使用同一常量,
/// 防止两侧字符串漂移让合法关闭被拒或伪造审计被放行。
pub const MANUAL_CLOSE_ACTION: &str = "manual_close";
pub use action_rate::ActionRateReservation;
pub use instance::{
    CasOutcome, ControlCasOutcome, ControlTransitionSpec, ManagementAuditOutcome, NewSagaInstance,
    SagaCreation, SagaInstanceQuery, TransitionSpec,
};
pub use metrics::SagaStoreMetrics;
pub use participant::{
    CancelAdjudication, CompensationAdmission, ExecuteAdmission, ExternalCancelAdmission,
    ParticipantGateKey, ResolutionAdmission, ResolutionTarget,
};
pub use quota::QuotaReservation;
pub use row::{SagaInstanceRow, SagaInstanceSummary, SagaStepAttemptRow, SagaStepRow};
pub use stepjournal::{AttemptOutcomeRecord, AttemptStart, StepJournalPatch};
pub use timer::{
    SagaTimerRow, TimerClaimBatch, TimerFencing, TimerFencingToken, TimerFencingTokenIssuer,
    TimerReschedule, TimerSchedule, TimerScope, TimerSpec, TimerState,
};

/// 业务作用：无状态 MySQL Saga store；每次操作经 `natx` 取连接，自动感知 ambient 事务。
///
/// 无状态设计（对齐 `MySqlInbox`/`MySqlOutbox`）使多副本 Orchestrator 不共享任何进程内
/// 可变状态：并发控制完全由数据库 CAS、唯一键与 fencing token 承担。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlSagaStore;

impl MySqlSagaStore {
    /// 业务作用：创建 store，不建连。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接使用的无状态 store。
    pub fn new() -> Self {
        Self
    }
}
