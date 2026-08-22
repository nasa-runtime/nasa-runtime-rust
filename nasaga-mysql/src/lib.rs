//! NASA Saga 的 MySQL store：状态、timer、审计、配额持久化与数据库 CAS。
//!
//! `nasaga-core` 负责封闭状态机与身份派生等纯裁决，本 crate 负责把裁决结果以正确的
//! 事务边界与唯一键落库。Orchestrator
//! 的推进事务以"单一本地事务"为前提——Saga 表必须与 Orchestrator 自身的 Inbox/Outbox
//! **同库**，本 crate 不提供跨库变体。
//!
//! # 事务合同（调用方必须遵守）
//!
//! - **全部写路径要求同源 ambient `natx` 事务**（`natx::run_for`/带 datasource 的
//!   `#[transactional]`），事务缺失或 datasource 不一致
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
mod backend;
mod error;
mod instance;
mod metrics;
mod participant;
mod quota;
mod row;
mod schema;
mod stepjournal;
mod timer;

pub use error::SagaStoreError;
pub use nasaga_backend::{
    ActionRateReservation, AttemptConflictFact, AttemptOutcomeRecord, AttemptStart,
    CancelAdjudication, CasOutcome, CompensationAdmission, ControlCasOutcome,
    ControlTransitionSpec, ExecuteAdmission, ExternalCancelAdmission, ManagementAuditOutcome,
    NewSagaInstance, ParticipantGateKey, QuotaReservation, ResolutionAdmission, ResolutionTarget,
    SagaConflictFactRow, SagaConflictKind, SagaControlAuditRow, SagaCreation, SagaInstanceQuery,
    SagaInstanceRow, SagaInstanceSummary, SagaManagementAuditRow, SagaStepAttemptRow, SagaStepRow,
    SagaStoreMetrics, SagaTimerRow, SagaTransitionAuditRow, StepJournalPatch, TimerClaimBatch,
    TimerFencing, TimerFencingToken, TimerFencingTokenIssuer, TimerReschedule, TimerSchedule,
    TimerScope, TimerSpec, TimerState, TransitionSpec,
};

/// 人工关闭动作在管理审计表中的稳定 action 名。
///
/// `MANUALLY_CLOSED` 唯一入边的同事务证据检查与管理入口的审计写入必须使用同一常量,
/// 防止两侧字符串漂移让合法关闭被拒或伪造审计被放行。
pub use nasaga_backend::MANUAL_CLOSE_ACTION;
/// 业务作用：以不可变 datasource 身份统一 MySQL Saga 实例、timer、审计和配额操作。
///
/// 轻量句柄只保存 datasource qualifier；多副本 Orchestrator 不共享进程内可变业务状态，
/// 并发控制完全由数据库 CAS、唯一键与 fencing token 承担。
#[derive(Debug, Clone)]
pub struct MySqlSagaStore {
    datasource: natx::DatasourceRef,
}

impl Default for MySqlSagaStore {
    /// 业务作用：以兼容语义构造绑定默认 datasource 的 Saga store。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`MySqlSagaStore::new`] 相同的轻量句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl MySqlSagaStore {
    /// 业务作用：创建 store，不建连。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接使用的默认 datasource 轻量 store。
    pub fn new() -> Self {
        Self {
            datasource: natx::DatasourceRef::default(),
        }
    }

    /// 业务作用：创建绑定命名 datasource 的 Saga store，阻止 Saga 持久化隐式跨库。
    ///
    /// 参数说明：`datasource` 为启动期已注册的数据源名称。
    ///
    /// 返回：所有后续 Saga 操作使用该 datasource；名称非法时在 I/O 前失败。
    pub fn with_datasource(datasource: impl AsRef<str>) -> anyhow::Result<Self> {
        Ok(Self {
            datasource: natx::DatasourceRef::new(datasource)?,
        })
    }

    /// 业务作用：读取该 store 全部实例、步骤、timer、审计与配额操作绑定的 datasource。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不可变、不含连接信息的 qualifier 引用。
    pub fn datasource_ref(&self) -> &natx::DatasourceRef {
        &self.datasource
    }
}
