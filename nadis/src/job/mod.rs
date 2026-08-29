//! RedisJob 分布式任务运行时：调度、执行器注册、租约、Fanout 与有预算停机。
//!
//! 所有节点都可以扫描，没有全局 leader；重复观察由 Lua CAS、Run 状态、owner、attempt token 与
//! assignment epoch 收敛。本模块的 key、标识与摘要是持久线协议，多实现共享同一 Redis 数据时必须字节
//! 稳定，否则同一逻辑 Run/Fanout 会被路由或识别成不同批次。

mod api;
pub mod config;
pub mod coordinator;
pub mod definition;
pub mod dispatcher;
mod error;
pub mod fanout;
mod fanout_dispatcher;
pub mod handler;
pub mod identifiers;
pub mod keyspace;
pub mod metrics;
pub mod model;
pub mod monitor;
mod names;
pub mod payload;
pub mod plan;
mod pubsub;
pub mod registry;
pub mod repository;
pub mod run;
pub mod runtime;
pub mod scanner;
mod script;
pub mod source;
pub mod trigger;

pub use api::{
    JobControl, JobQuery, JobSourceHealthReason, JobSourceHealthSnapshot, JobSourceHealthState,
    NamespaceGovernanceReport,
};
pub use config::{JobConfig, JobConfigBuilder, JobSourceConfig, JobSourceConfigBuilder};
pub use coordinator::{FanoutBuilder, FanoutCoordinator, FanoutRootContext, FanoutSubmitOutcome};
pub use definition::{JobDefinition, JobDefinitionBuilder};
pub use dispatcher::JobDispatcher;
pub use error::JobError;
pub use fanout::{
    DueIndexScan, FanoutAcceptOutcome, FanoutAddShardsOutcome, FanoutAggregateOutcome,
    FanoutBeginOutcome, FanoutCancelBatchOutcome, FanoutCapabilityCursorOutcome,
    FanoutCleanupOutcome, FanoutCommitOutcome, FanoutContract, FanoutDeliverOutcome,
    FanoutDueScans, FanoutFailCreatingOutcome, FanoutMarkReconciledOutcome,
    FanoutReadyDeferOutcome, FanoutReadyPromoteOutcome, FanoutReassignOutcome,
    FanoutReceiptRetryOutcome, FanoutRepository, FanoutRoot, FanoutShard, FanoutWatchRootOutcome,
};
pub use handler::{
    FanoutContext, IntoJobHandlerResult, JobContext, JobExecution, JobHandler, JobHandlerFuture,
    JobOutcome, JobResult,
};
pub use keyspace::JobKeyspace;
pub use metrics::{
    JobFireResult, JobMetricsSnapshot, JobSupervisorLoop, JobSupervisorMetricsSnapshot,
    JobSupervisorResult,
};
pub use model::{
    JobConcurrency, JobDefinitionState, JobExecutorState, JobFanoutFailurePolicy, JobMisfire,
    JobPubSubMode, JobResultCode, JobScheduleType, JobSerialOverflowPolicy, JobState, JobTrigger,
    JobWireCodec,
};
pub use monitor::FanoutMonitor;
pub use payload::{decode_json_payload, validate_json_payload, JobParameter, JobPayload};
pub use plan::{JobRuntimeHandle, PreparedJobRuntime, RedisJobPlan, RunningJobRuntime};
pub use registry::{
    ClusterSnapshot, ExecutorIdentity, ExecutorMember, ExecutorRegistry, HeartbeatOutcome,
    RecordEvidenceOutcome, RegisterCapabilityOutcome, RegistryGc, SnapshotOutcome,
    UnregisterOutcome,
};
pub use repository::{
    CancelOutcome, CompletionTrimOutcome, DeferOutcome, DefinitionControlOutcome, DeleteOutcome,
    DueEntry, FailWaitingOutcome, FinishFanoutRootOutcome, FinishOutcome, FireDueBatchItem,
    FireDueBatchResult, FireDueOutcome, JobDefinitionRecord, JobReapOutcome, JobRegisterOutcome,
    JobRepository, ManualFireOutcome, NamespaceShardSnapshot, NamespaceStateOutcome,
    PrepareFanoutRootOutcome, Promotion, RecoverOutcome, RenewBatchItem, RenewBatchResult,
    RenewOutcome, ScheduleDueScan, StartOutcome,
};
pub use run::JobRun;
pub use scanner::JobScanner;
pub use source::{JobSourceId, RedisJobSources};
pub use trigger::{
    first_fire_at_after, next_fire_at, validate_cron_compatibility, CRON_SEMANTICS_ID,
    CRON_TZDB_VERSION,
};
