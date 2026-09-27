//! Saga 后端中立持久合同。
//!
//! 本 crate 只承载数据库无关的行模型、封闭结果与分组能力，不依赖 SQLx driver。
//! schema、migration、SQLSTATE 解析和连接生命周期属于具体 adapter。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod audit;
mod error;
mod governance;
mod instance;
mod journal;
mod metrics;
mod participant;
mod row;
mod timer;
mod traits;

pub use audit::{
    AttemptConflictFact, SagaAttemptAuditCursor, SagaAttemptAuditRow, SagaAuditEventCursor,
    SagaAuditEventRecord, SagaAuditEventRow, SagaConflictFactRow, SagaConflictKind,
    SagaControlAuditRow, SagaManagementAuditRow, SagaTimedAuditCursor, SagaTransitionAuditRow,
};
pub use error::{SagaBackendError, SagaBackendErrorKind};
pub use governance::{ActionRateReservation, QuotaReservation, MANUAL_CLOSE_ACTION};
pub use instance::{
    validate_saga_instance_query, CasOutcome, ControlCasOutcome, ControlTransitionSpec,
    ManagementAuditOutcome, NewSagaInstance, SagaCreation, SagaInstanceQuery,
    SagaInstanceQueryParameterError, TransitionSpec, SAGA_INSTANCE_TIME_MAX_MS,
    SAGA_INSTANCE_TIME_MIN_MS,
};
pub use journal::{AttemptOutcomeRecord, AttemptStart, StepJournalPatch};
pub use metrics::{SagaLifecycleQuantiles, SagaStoreMetrics};
pub use participant::{
    CancelAdjudication, CompensationAdmission, ExecuteAdmission, ExternalCancelAdmission,
    ParticipantGateKey, ResolutionAdmission, ResolutionTarget,
};
pub use row::{SagaInstanceRow, SagaInstanceSummary, SagaStepAttemptRow, SagaStepRow};
pub use timer::{
    SagaTimerRow, TimerClaimBatch, TimerFencing, TimerFencingToken, TimerFencingTokenIssuer,
    TimerReschedule, TimerSchedule, TimerScope, TimerSpec, TimerState,
};
pub use traits::{
    SagaAuditStore, SagaBackend, SagaBackendFactory, SagaGovernanceStore, SagaInstanceStore,
    SagaJournalStore, SagaParticipantStore, SagaStore, SagaTimerStore, SagaTransactionFuture,
    SagaTransactionRunError, SagaTransactionRunner,
};
