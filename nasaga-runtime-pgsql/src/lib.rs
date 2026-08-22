//! NASA Saga PostgreSQL 运行包装。
//!
//! 唯一状态机位于 `nasaga-runtime-core`；本 crate 只把 PostgreSQL Saga store、Inbox、
//! Outbox 与 ambient transaction 组合为同源后端。运行边界是本地 ACID、至少一次投递、
//! Inbox 幂等和显式补偿，不提供跨服务 ACID 或跨 datasource 原子事务。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod backend;

pub use backend::{PgSagaBackend, PgSagaTransactionRunner};
pub use nasaga_core::ServiceIdentity;
pub use nasaga_pgsql::{
    SagaInstanceQuery, SagaInstanceSummary, TimerFencingToken, TimerFencingTokenIssuer,
};
pub use nasaga_runtime_core::{
    classify_command_delivery_error, classify_result_delivery_error, command_dead_letter_reason,
    derive_result_event_id, derive_scheduled_business_key, derive_timer_id, verify_descriptors,
    CommandDeliveryDisposition, CommandDeliveryPolicy, DefinitionRegistry, HandleOutcome,
    OrchestratorConfig, ParticipantCommandTrust, ParticipantHandled, ResultDeliveryDisposition,
    ResultDeliveryPolicy, SagaAuditTrail, SagaCommandEnvelope, SagaCommandProcessingError,
    SagaManagementContext, SagaManagementPermission, SagaOperationalMetrics, SagaResultEnvelope,
    SagaResultProcessingError, SagaStepDescriptor, SagaTransactionError, ScheduledBatchReport,
    ScheduledBatchSpec, ScheduledItem, StartOutcome, StartSagaRequest, TenantActionRate,
    TimerOutcome, VerifiedIdentity, COLLECTED_SAGA_STEPS, COMMAND_EVENT_TYPE, KIND_CANCEL_TIMEOUT,
    KIND_COMPENSATE_TIMEOUT, KIND_COMPENSATION_RESOLUTION_BUDGET, KIND_FORWARD_RESOLUTION_BUDGET,
    KIND_INSTANCE_DEADLINE, KIND_RESOLUTION_BUDGET, KIND_RESOLVE_TIMEOUT, KIND_STEP_TIMEOUT,
    RESULT_EVENT_TYPE, SAGA_AGGREGATE_TYPE, SAGA_COMMAND_DEAD_LETTER_REASONS,
};
pub use nasaga_runtime_core::{
    render_saga_http_command_dlt_metric, SagaHttpMessageAuthError, SagaHttpMessageAuthFailure,
    SagaHttpMessageAuthenticator, SagaHttpReplayGuard, SagaHttpReplayMetricAggregate,
    SagaHttpReplayMetrics, SagaHttpReplayPlane, SagaHttpSignedMessage,
};
pub use natelemetry::TraceContext;

#[cfg(feature = "kafka")]
pub use nasaga_runtime_core::{
    SagaCommandRoute, SagaKafkaCommandConsumer, SagaKafkaCommandConsumerConfig,
    SagaKafkaResultConsumerConfig,
};
/// PostgreSQL Kafka result consumer；默认 handler 绑定 PostgreSQL Orchestrator。
#[cfg(feature = "kafka")]
pub type SagaKafkaResultConsumer<H = PgOrchestrator> =
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

/// PostgreSQL Orchestrator 入口；全部裁决由后端中立状态机执行。
pub type PgOrchestrator = nasaga_runtime_core::Orchestrator<PgSagaBackend>;
/// PostgreSQL-only 宿主使用的 Orchestrator 兼容名称。
pub type Orchestrator = PgOrchestrator;
/// PostgreSQL Participant 入口；业务代码不需要直接提供后端泛型。
pub type PgParticipantRuntime = nasaga_runtime_core::ParticipantRuntime<PgSagaBackend>;
/// 已认证 PostgreSQL Participant 能力视图。
pub type PgAuthenticatedParticipantRuntime<'a> =
    nasaga_runtime_core::AuthenticatedParticipantRuntime<'a, PgSagaBackend>;
/// PostgreSQL-only 宿主使用的已认证参与方兼容名称。
pub type AuthenticatedParticipantRuntime<'a> = PgAuthenticatedParticipantRuntime<'a>;
/// PostgreSQL 宏展开使用的参与方运行时兼容名称。
#[doc(hidden)]
pub type ParticipantRuntime = PgParticipantRuntime;

/// 业务作用：抽象宏生成的 PostgreSQL 类型化步骤分发入口，供受管 command consumer 绑定 Service。
pub trait PgSagaCommandService: Send + Sync + 'static {
    /// 业务作用：把已认证命令交给 PostgreSQL Participant 的完整本地事务。
    ///
    /// 参数说明：`runtime` 是 PostgreSQL 参与方运行时，`envelope` 是命令，`producer` 是可信来源映射的身份。
    ///
    /// 返回：本地事务提交后返回可确认结论；合同、业务或基础设施失败返回错误。
    fn handle_saga_command<'a>(
        &'a self,
        runtime: &'a PgParticipantRuntime,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
    ) -> impl std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a;

    /// 业务作用：在同一参与方事务中携带已验证收据链路上下文。
    ///
    /// 参数说明：前三项与无 trace 入口一致，`receipt_trace` 是可选受信收据上下文。
    ///
    /// 返回：语义与 [`PgSagaCommandService::handle_saga_command`] 一致。
    fn handle_saga_command_traced<'a>(
        &'a self,
        runtime: &'a PgParticipantRuntime,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
        receipt_trace: Option<&'a TraceContext>,
    ) -> impl std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a {
        let _ = receipt_trace;
        self.handle_saga_command(runtime, envelope, producer)
    }
}

/// PostgreSQL 宏展开使用的 Service 分发合同兼容名称。
#[doc(hidden)]
pub use PgSagaCommandService as SagaCommandService;

/// 业务作用：把 PostgreSQL Participant runtime 与宏生成 Service 组装为后端中立 command handler。
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
pub struct ParticipantCommandHandler<S> {
    runtime: std::sync::Arc<PgParticipantRuntime>,
    service: std::sync::Arc<S>,
}

#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
impl<S> ParticipantCommandHandler<S> {
    /// 业务作用：绑定 PostgreSQL 参与方运行时与类型化 Saga Service，不启动消费任务。
    ///
    /// 参数说明：`runtime` 持有同源事务能力，`service` 持有精确步骤分发合同。
    ///
    /// 返回：可交给任一已编入 connector 的轻量 handler。
    pub fn new(runtime: std::sync::Arc<PgParticipantRuntime>, service: std::sync::Arc<S>) -> Self {
        Self { runtime, service }
    }
}

#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
impl<S: PgSagaCommandService> nasaga_runtime_core::SagaCommandHandler
    for ParticipantCommandHandler<S>
{
    /// 业务作用：把已认证 command 交给 PostgreSQL 参与方的完整本地事务。
    async fn handle_authenticated_command(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled> {
        self.service
            .handle_saga_command(&self.runtime, envelope, producer)
            .await
    }

    /// 业务作用：把已验证收据 trace 与 command 一并交给 PostgreSQL 参与方事务。
    async fn handle_authenticated_command_traced(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
    ) -> anyhow::Result<ParticipantHandled> {
        self.service
            .handle_saga_command_traced(&self.runtime, envelope, producer, receipt_trace)
            .await
    }
}

/// 宏展开专用内部再导出。
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use nasaga_core as core;
    pub use nasaga_runtime_core as runtime_core;
    pub use nasaga_runtime_core::__private::linkme;
}
