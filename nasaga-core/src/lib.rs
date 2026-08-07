//! NASA Saga 核心：provider-neutral 的 definition、身份、结果分类与封闭状态机。
//!
//! 本 crate **不依赖 MySQL、Kafka 或任何运行时**：store 与 transport 由上层通过 trait 注入，
//! 这里只保留可在无 I/O 环境下判定的纯逻辑，使同一套安全裁决被 Orchestrator、参与方
//! adapter 与启动预检共用，而不是各自实现一份近似规则。
//!
//! # 本层保证的安全不变量
//!
//! - **身份分离**：每个 phase 的 `effect_id` 跨全部 attempt 稳定，`command_id` 只标识单次
//!   投递；cancel/resolve 另携带原 execute/compensate 的 `target_effect_id`。混用会让重试形成
//!   新副作用，或让查询稳定命中错误的远端请求。
//! - **definition 不可变**：内容摘要覆盖全部流程语义，同一版本改内容必然改摘要，
//!   从而被启动预检拦截，杜绝“用新内容驱动旧实例”。
//! - **pivot 闭合**：不可补偿步骤只能位于流程末端，保证进入补偿时计划内不存在
//!   已成功却无法撤销的步骤，`COMPENSATED` 才能真正表示“计划内全部已撤销”。
//! - **状态机封闭**：合法迁移穷尽枚举；超时先经 `Cancelling` 取消屏障、未知先经
//!   `WaitingResolution`，绝不把“结果未知”当作“执行失败”直接补偿。
//! - **计划冻结**：补偿计划一次性冻结并带内容摘要，之后只允许幂等完成计划内步骤，
//!   计划外迟到成功必须转人工重建而不是动态插队。
//!
//! # 本层不保证什么
//!
//! 不提供跨服务 ACID，也**不提供隔离性**：已提交步骤的中间状态对并发 Saga 与旁路读者
//! 立即可见，补偿前的读者可能已经消费了将被撤销的事实。这类问题必须由业务层用语义锁、
//! 条件更新或可交换操作自行防御。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod context;
mod definition;
mod error;
mod identity;
mod journal;
mod outcome;
mod plan;
mod state;
mod step;

pub use context::SagaContext;
pub use definition::{
    CancelMode, Compensation, ResolutionMode, ResolutionSpec, StepDefinition, TimeoutPolicy,
    WorkflowDefinition,
};
pub use error::{
    ContractResult, ContractViolation, CODE_COUNTER_OVERFLOW, CODE_COUNTER_ZERO,
    CODE_DEFINITION_BUDGET_ZERO, CODE_DEFINITION_DUPLICATE_STEP, CODE_DEFINITION_EMPTY,
    CODE_DEFINITION_MODE_CONFLICT, CODE_DEFINITION_PIVOT_NOT_LAST,
    CODE_DEFINITION_RESOLUTION_MISSING, CODE_IDENTIFIER_CHARSET, CODE_IDENTIFIER_EMPTY,
    CODE_IDENTIFIER_TOO_LONG, CODE_IDENTIFIER_UNTRIMMED, CODE_PLAN_DUPLICATE_STEP,
    CODE_PLAN_MISSING_STEP, CODE_PLAN_SUCCEEDED_PIVOT, CODE_PLAN_UNKNOWN_STEP,
    CODE_PLAN_UNSETTLED_STEP, CODE_TRANSITION_FORWARD_AFTER_COMPENSATION, CODE_TRANSITION_ILLEGAL,
};
pub use identity::{
    AttemptNo, BusinessKey, CommandId, DefinitionVersion, EffectId, SagaId, ServiceIdentity,
    StepName, StepPhase, TenantId, WorkflowName,
};
pub use journal::{
    StepAttemptStatus, StepCancelStatus, StepCompensationStatus, StepResolutionStatus, TriggerKind,
};
pub use outcome::{
    CancelOutcome, CancelResult, CompensationOutcome, CompensationResult, SagaExecutionError,
    SagaOutcome, SagaResult, TerminalForwardOutcome,
};
pub use plan::{
    freeze_compensation_plan, CompensationEntry, CompensationPlan, StepForwardStatus,
    StepJournalEntry,
};
pub use state::{check_transition, ControlState, Direction, SagaStatus, TransitionGuard};
pub use step::{SagaCancelStep, SagaResolveStep, SagaStep};
