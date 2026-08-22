//! NASA Saga 运行时：可恢复的 Orchestrator、参与方事务 adapter 与 transport 裁决。
//!
//! 职责边界：`nasaga-core` 是纯裁决，`nasaga-mysql` 是持久化与 CAS，
//! 本 crate 把二者组装成可运行的推进引擎——创建、结果推进、durable timer、有界重试、
//! 崩溃恢复（durable timer + at-least-once Outbox 天然承担）、租户治理与管理命令。宿主层
//! （`napp` 托管组件）负责把所选 transport 消费循环、timer 轮询循环接到这里，本 crate 不
//! 自行 `tokio::spawn` 任何无限循环。
//!
//! # 端到端架构
//!
//! Orchestrator 在一个本地事务内提交结果 Inbox、实例 CAS、attempt/transition、durable timer
//! 与下一 command Outbox；参与方在自己的本地事务内提交 command Inbox、gate、业务事实与
//! result Outbox。两端通过至少一次 transport 连接，`effect_id` 跨重投稳定，重复由 Inbox 和
//! 目标业务幂等键吸收。进程崩溃后由数据库事实恢复，不依赖内存队列续跑。
//!
//! Kafka 与 Redis Streams feature 提供完整消费裁决；HTTP 入口提供认证/重放构件；gRPC feature
//! 提供框架 generated command/result client/server、mTLS leaf principal 绑定与封闭收据。Application
//! 入站计划把 service 自动登记进唯一 `nagrpc` registry；独立宿主仍显式拥有 listener、deadline 与
//! drain。`TraceContext` 只作为已验证的显式输入传播，不读取 ambient 状态，也不是投递前置条件。
//!
//! # 能力范围与明确不承诺
//!
//! - Orchestration、带不可变版本的严格串行步骤、MySQL store、Outbox/Inbox 可靠通道；
//! - transport 层 producer 认证（topic-to-owner、mTLS 或端到端签名）在 connector
//!   边界落地，本层仍做**身份复验**（派生比对）兜底；
//! - 公开保证只能是"本地 ACID + Outbox 至少一次 + Inbox 幂等 + 持久化状态机与显式
//!   补偿 = 最终一致性"；不承诺物理 exactly-once、跨服务 ACID 或并发 Saga 隔离性。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod backend;
mod participant;
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
mod transport_shared;

pub use backend::{MySqlSagaBackend, MySqlSagaTransactionRunner};
pub use nasaga_runtime_core::{
    derive_result_event_id, HandleOutcome, OrchestratorConfig, SagaAuditTrail, SagaCommandEnvelope,
    SagaManagementContext, SagaManagementPermission, SagaOperationalMetrics, SagaResultEnvelope,
    StartOutcome, StartSagaRequest, TenantActionRate, TimerOutcome, VerifiedIdentity,
    COMMAND_EVENT_TYPE, RESULT_EVENT_TYPE, SAGA_AGGREGATE_TYPE,
};
/// 既有 MySQL Orchestrator 入口；全部状态裁决由 `nasaga-runtime-core` 执行。
pub type Orchestrator = nasaga_runtime_core::Orchestrator<MySqlSagaBackend>;
/// 既有 MySQL Participant 入口；业务代码不需要增加泛型参数。
pub type ParticipantRuntime = nasaga_runtime_core::ParticipantRuntime<MySqlSagaBackend>;
/// 既有已认证 MySQL Participant 能力视图。
pub type AuthenticatedParticipantRuntime<'a> =
    nasaga_runtime_core::AuthenticatedParticipantRuntime<'a, MySqlSagaBackend>;
pub use nasaga_runtime_core::{
    render_saga_http_command_dlt_metric, SagaHttpMessageAuthError, SagaHttpMessageAuthFailure,
    SagaHttpMessageAuthenticator, SagaHttpReplayGuard, SagaHttpReplayMetricAggregate,
    SagaHttpReplayMetrics, SagaHttpReplayPlane, SagaHttpSignedMessage,
};

pub use nasaga_runtime_core::{
    classify_command_delivery_error, classify_result_delivery_error, command_dead_letter_reason,
    CommandDeliveryDisposition, CommandDeliveryPolicy, ResultDeliveryDisposition,
    ResultDeliveryPolicy, SagaCommandProcessingError, SagaResultProcessingError,
    SAGA_COMMAND_DEAD_LETTER_REASONS,
};

// 检索查询/摘要与 fencing 类型同为公开管理面输入输出,经本 crate 统一再导出。
pub use nasaga_mysql::{
    SagaInstanceQuery, SagaInstanceSummary, TimerFencingToken, TimerFencingTokenIssuer,
};
// 链路上下文的公开类型:发起入口与 transport 收据以它显式传递 trace,宏展开也经本
// 重导出引用,避免业务/宏直接依赖 natelemetry 坐标。
pub use nasaga_core::ServiceIdentity;
pub use nasaga_runtime_core::{
    derive_scheduled_business_key, derive_timer_id, DefinitionRegistry, ParticipantCommandTrust,
    ParticipantHandled, SagaTransactionError, ScheduledBatchReport, ScheduledBatchSpec,
    ScheduledItem, KIND_CANCEL_TIMEOUT, KIND_COMPENSATE_TIMEOUT,
    KIND_COMPENSATION_RESOLUTION_BUDGET, KIND_FORWARD_RESOLUTION_BUDGET, KIND_INSTANCE_DEADLINE,
    KIND_RESOLUTION_BUDGET, KIND_RESOLVE_TIMEOUT, KIND_STEP_TIMEOUT,
};
#[cfg(feature = "kafka")]
pub use nasaga_runtime_core::{
    SagaCommandRoute, SagaKafkaCommandConsumer, SagaKafkaCommandConsumerConfig,
    SagaKafkaResultConsumerConfig,
};
pub use natelemetry::TraceContext;
pub use participant::SagaCommandService;
/// 既有 MySQL Kafka result consumer；默认 handler 保持为 MySQL Orchestrator。
#[cfg(feature = "kafka")]
pub type SagaKafkaResultConsumer<H = Orchestrator> =
    nasaga_runtime_core::SagaKafkaResultConsumer<H>;
#[cfg(feature = "grpc-transport")]
pub use nasaga_runtime_core::{
    grpc_proto, outbox_disposition_of, SagaGrpcBindingError, SagaGrpcCommandServer,
    SagaGrpcCommandTransportService, SagaGrpcPeerBinding, SagaGrpcPeerIdentity, SagaGrpcReceipt,
    SagaGrpcResultServer, SagaGrpcResultTransportService,
};
#[cfg(feature = "redis-stream")]
pub use nasaga_runtime_core::{
    publisher_duplicate_hints_total, safe_trim_by_group_frontier, stream_group_backlog,
    verify_stream_transport_ready, SagaRedisStreamCommandConsumer, SagaRedisStreamPublisher,
    SagaRedisStreamResultConsumer, SagaStreamAuth, SagaStreamConsumerConfig, SagaStreamPoller,
    StreamPollReport,
};
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
pub use nasaga_runtime_core::{SagaCommandHandler, SagaResultHandler};
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
pub use transport_shared::ParticipantCommandHandler;

pub use nasaga_runtime_core::{verify_descriptors, SagaStepDescriptor, COLLECTED_SAGA_STEPS};

/// 宏展开专用的内部再导出；业务代码不应直接使用。
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use linkme;
    pub use nasaga_core as core;
    pub use nasaga_runtime_core as runtime_core;
}
