//! NASA Saga PostgreSQL 运行包装。
//!
//! 唯一状态机位于 `nasaga-runtime-core`；本 crate 只把 PostgreSQL Saga store、Inbox、
//! Outbox 与 ambient transaction 组合为同源后端。运行边界是本地 ACID、至少一次投递、
//! Inbox 幂等和显式补偿，不提供跨服务 ACID 或跨 datasource 原子事务。
//!
//! # 运行架构与可靠发起
//!
//! Orchestrator 在一个 PostgreSQL 事务中提交 result Inbox、实例 CAS、journal、timer 和 command
//! Outbox；参与方在自己的事务中提交 command Inbox、gate、业务事实和 result Outbox。`napp` 拥有
//! 受管角色装配、Ready、listener、后台循环与停机，本 crate 不独立启动这些生命周期动作。
//! 受管可靠 client 的业务事实、start-intent 与 dispatcher 固定使用 `saga.client.datasource_ref`；
//! 显式 `outbox.datasource_ref` 冲突时在 Ready 前拒绝，省略该字段不改变扫描目标。事务内追加返回的
//! 事件身份不是外层提交证明；本地已受理后，远端不可用或收据丢失仍保留原事件重投，不宣称流程已完成。
//!
//! 受管结果入口把独立 Catalog 资格传入共享状态机。command route 缺席时，已经提交的原 result
//! 仍可在共享 Catalog、冻结 definition 与 producer 信任有效的前提下收敛，但新 Start、timer claim
//! 和 Ready 保持关闭。请求冻结期限、撤销身份、安全发布代际与合同摘要；等待实例锁后失权会回滚
//! 整笔 PostgreSQL 事务并保留原事件重投，A→B→A 不恢复旧资格。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod backend;
mod catalog;

pub use backend::{PgSagaBackend, PgSagaTransactionRunner};
pub use catalog::{
    acknowledge_catalog_generation_for, activate_definition_for,
    activate_definition_with_operation_for, catalog_generation_fully_acknowledged_for,
    deprecate_definition_for, deprecate_definition_with_operation_for, load_definition_for,
    load_dynamic_catalog_for, publish_definition_for, register_capability_for,
    retire_catalog_replica_for, retire_definition_with_operation_for,
};
pub use nasaga_backend::SagaInstanceRow;
pub use nasaga_core::ServiceIdentity;
pub use nasaga_pgsql::{
    validate_saga_instance_query, SagaInstanceQuery, SagaInstanceQueryParameterError,
    SagaInstanceSummary, TimerFencingToken, TimerFencingTokenIssuer, SAGA_INSTANCE_TIME_MAX_MS,
    SAGA_INSTANCE_TIME_MIN_MS,
};
pub use nasaga_runtime_core::{
    classify_command_delivery_error, classify_result_delivery_error, collect_workflow_definitions,
    command_dead_letter_reason, derive_result_event_id, derive_scheduled_business_key,
    derive_timer_id, select_capability_route, select_capability_routes,
    validate_capability_route_contract, validate_redis_stream_route, verify_descriptors,
    CapabilityDescriptor, CapabilityReceipt, CommandDeliveryDisposition, CommandDeliveryPolicy,
    DefinitionActivationGate, DefinitionArtifact, DefinitionCatalogError, DefinitionLifecycle,
    DefinitionLifecycleOperation, DefinitionPublishDisposition, DefinitionRecord,
    DefinitionRegistry, DynamicCatalogSnapshot, HandleOutcome, OrchestratorConfig,
    ParticipantCommandTrust, ParticipantHandled, RegisteredCapability, ResultDeliveryDisposition,
    ResultDeliveryPolicy, SagaAuditPage, SagaAuditPageCursor, SagaAuditRecord, SagaAuditTrail,
    SagaCommandEnvelope, SagaCommandProcessingError, SagaConcurrencyError, SagaManagementContext,
    SagaManagementError, SagaManagementExpectation, SagaManagementPermission,
    SagaOperationalMetrics, SagaResultEnvelope, SagaResultProcessingError, SagaStepDescriptor,
    SagaTransactionError, SagaWorkflowDescriptor, ScheduledBatchReport, ScheduledBatchSpec,
    ScheduledItem, StartOutcome, StartSagaError, StartSagaRequest, TenantActionRate, TimerOutcome,
    VerifiedIdentity, COLLECTED_SAGA_STEPS, COLLECTED_SAGA_WORKFLOWS, COMMAND_EVENT_TYPE,
    KIND_CANCEL_TIMEOUT, KIND_COMPENSATE_TIMEOUT, KIND_COMPENSATION_RESOLUTION_BUDGET,
    KIND_FORWARD_RESOLUTION_BUDGET, KIND_INSTANCE_DEADLINE, KIND_RESOLUTION_BUDGET,
    KIND_RESOLVE_TIMEOUT, KIND_STEP_TIMEOUT, RESULT_EVENT_TYPE, SAGA_AGGREGATE_TYPE,
    SAGA_COMMAND_DEAD_LETTER_REASONS,
};
pub use nasaga_runtime_core::{
    render_saga_http_command_dlt_metric, SagaHttpCredentialSource, SagaHttpCredentials,
    SagaHttpMessageAuthError, SagaHttpMessageAuthFailure, SagaHttpMessageAuthenticator,
    SagaHttpReplayGuard, SagaHttpReplayMetricAggregate, SagaHttpReplayMetrics, SagaHttpReplayPlane,
    SagaHttpSignedMessage,
};
pub use nasaga_runtime_core::{
    render_saga_latency_metrics, saga_latency_snapshot, SagaLatencySnapshot, SAGA_LATENCY_BUCKETS,
};
pub use nasaga_runtime_core::{SagaPayload, SagaPayloadContract, SagaPayloadError};
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
    grpc_proto, orchestrator_proto, outbox_disposition_of, SagaGrpcBindingError,
    SagaGrpcCommandServer, SagaGrpcCommandTransportService, SagaGrpcPeerBinding,
    SagaGrpcPeerIdentity, SagaGrpcReceipt, SagaGrpcResultServer, SagaGrpcResultTransportService,
};
#[cfg(feature = "redis-stream")]
pub use nasaga_runtime_core::{
    publisher_duplicate_hints_total, safe_trim_by_group_frontier, stream_group_backlog,
    verify_stream_transport_ready, SagaRedisStreamCommandConsumer, SagaRedisStreamPublisher,
    SagaRedisStreamResultConsumer, SagaStreamAuth, SagaStreamConsumerConfig, SagaStreamPoller,
    SagaStreamVerificationKey, StreamPollReport,
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

/// 业务作用：在指定 PostgreSQL 事务域中建立并复验 Orchestrator 独占的 Saga、result Inbox 与 command Outbox 持久结构。
///
/// 参数说明：`datasource` 是 Application 已冻结的 PostgreSQL datasource qualifier。
///
/// 返回：全部结构可用时成功；连接、DDL 或最终结构门禁失败时返回错误。
pub async fn ensure_orchestrator_schema_for(datasource: &str) -> anyhow::Result<()> {
    let mut lock_connection = acquire_schema_lock(datasource).await?;
    nasaga_pgsql::PgSagaStore::ensure_schema_on_connection(&mut lock_connection).await?;
    nainbox_pgsql::PgInbox::ensure_schema_on_connection(&mut lock_connection).await?;
    naoutbox_pgsql::PgOutbox::ensure_schema_on_connection(&mut lock_connection).await?;
    ensure_http_replay_schema(&mut lock_connection).await?;
    catalog::ensure_schema(&mut lock_connection).await?;
    release_schema_lock(&mut lock_connection).await?;
    Ok(())
}

/// 业务作用：在指定 PostgreSQL 事务域中建立并复验 participant gate、command Inbox 与 result Outbox 持久结构。
///
/// 参数说明：`datasource` 是 participant binding 唯一绑定的 PostgreSQL datasource qualifier。
///
/// 返回：全部本地事务结构可用时成功；任一结构不可用时返回错误。
pub async fn ensure_participant_schema_for(datasource: &str) -> anyhow::Result<()> {
    let mut lock_connection = acquire_schema_lock(datasource).await?;
    nasaga_pgsql::PgSagaStore::ensure_participant_schema_on_connection(&mut lock_connection)
        .await?;
    nainbox_pgsql::PgInbox::ensure_schema_on_connection(&mut lock_connection).await?;
    naoutbox_pgsql::PgOutbox::ensure_schema_on_connection(&mut lock_connection).await?;
    ensure_http_replay_schema(&mut lock_connection).await?;
    release_schema_lock(&mut lock_connection).await?;
    Ok(())
}

/// 业务作用：在角色事务域中建立跨副本共享的 HTTP nonce 一次性裁决表。
///
/// 参数说明：`connection` 是持有当前 database/schema 自举锁的连接。
///
/// 返回：表与过期索引可用时成功；DDL 权限或结构冲突返回错误并阻止 Ready。
async fn ensure_http_replay_schema(connection: &mut natx_pgsql::PgConn) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nasa_saga_http_replay_claim (\
             producer VARCHAR(128) NOT NULL,\
             nonce CHAR(32) NOT NULL,\
             expires_at_ms BIGINT NOT NULL,\
             claimed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\
             PRIMARY KEY (producer, nonce)\
         )",
    )
    .execute(connection.as_mut())
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_nasa_saga_http_replay_expiry \
         ON nasa_saga_http_replay_claim (expires_at_ms)",
    )
    .execute(connection.as_mut())
    .await?;
    let identity =
        sqlx::query("SELECT current_database() AS database_name, current_schema() AS schema_name")
            .fetch_one(connection.as_mut())
            .await?;
    let database_name: String = sqlx::Row::try_get(&identity, "database_name")?;
    let schema_name: String = sqlx::Row::try_get(&identity, "schema_name")?;
    anyhow::ensure!(
        !database_name.is_empty() && !schema_name.is_empty(),
        "Saga HTTP replay database identity is unavailable"
    );
    let valid_columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = current_schema() \
         AND table_name = 'nasa_saga_http_replay_claim' AND is_nullable = 'NO' AND (\
           (column_name = 'producer' AND udt_name = 'varchar' AND character_maximum_length = 128 AND collation_name IS NULL) OR \
           (column_name = 'nonce' AND udt_name = 'bpchar' AND character_maximum_length = 32 AND collation_name IS NULL) OR \
           (column_name = 'expires_at_ms' AND udt_name = 'int8') OR \
           (column_name = 'claimed_at' AND udt_name = 'timestamptz'))",
    )
    .fetch_one(connection.as_mut())
    .await?;
    let table_valid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() \
         AND table_name = 'nasa_saga_http_replay_claim' AND table_type = 'BASE TABLE'",
    )
    .fetch_one(connection.as_mut())
    .await?;
    let primary_valid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (SELECT i.indisunique, ARRAY_AGG(a.attname ORDER BY keys.ordinality)::TEXT[] AS columns \
         FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace \
         JOIN pg_index i ON i.indrelid = t.oid JOIN pg_class ix ON ix.oid = i.indexrelid \
         CROSS JOIN LATERAL UNNEST(i.indkey) WITH ORDINALITY AS keys(attnum, ordinality) \
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = keys.attnum \
         WHERE n.nspname = current_schema() AND t.relname = 'nasa_saga_http_replay_claim' \
         AND ix.relname = 'nasa_saga_http_replay_claim_pkey' GROUP BY i.indisunique) indexes \
         WHERE indisunique AND columns = ARRAY['producer','nonce']::TEXT[]",
    )
    .fetch_one(connection.as_mut())
    .await?;
    let expiry_valid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (SELECT i.indisunique, ARRAY_AGG(a.attname ORDER BY keys.ordinality)::TEXT[] AS columns \
         FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace \
         JOIN pg_index i ON i.indrelid = t.oid JOIN pg_class ix ON ix.oid = i.indexrelid \
         CROSS JOIN LATERAL UNNEST(i.indkey) WITH ORDINALITY AS keys(attnum, ordinality) \
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = keys.attnum \
         WHERE n.nspname = current_schema() AND t.relname = 'nasa_saga_http_replay_claim' \
         AND ix.relname = 'idx_nasa_saga_http_replay_expiry' GROUP BY i.indisunique) indexes \
         WHERE NOT indisunique AND columns = ARRAY['expires_at_ms']::TEXT[]",
    )
    .fetch_one(connection.as_mut())
    .await?;
    anyhow::ensure!(
        valid_columns == 4 && table_valid == 1 && primary_valid == 1 && expiry_valid == 1,
        "Saga HTTP replay schema contract is invalid"
    );
    Ok(())
}

/// 业务作用：在共享 PostgreSQL 事务域中原子占用已验真的 producer/nonce，阻止其它副本再次受理同一请求。
///
/// 参数说明：
/// - `datasource`: 接收角色唯一绑定的数据源。
/// - `producer`: 已通过 HMAC 的逻辑发送方。
/// - `nonce`: 已通过格式与时间窗校验的一次性值。
/// - `now_ms`: 接收端当前 Unix 毫秒。
/// - `expires_at_ms`: 本 claim 必须保留到的时间边界。
///
/// 返回：首次占用返回真；时间窗内重复返回假；数据库失败返回错误且不得进入业务处理。
pub async fn claim_http_replay_for(
    datasource: &str,
    producer: &str,
    nonce: &str,
    now_ms: i64,
    expires_at_ms: i64,
) -> anyhow::Result<bool> {
    let mut connection = natx_pgsql::conn_for(datasource).await?;
    // PostgreSQL 用索引挑选小批过期行，避免清理工作随历史总量无界增长。
    sqlx::query(
        "DELETE FROM nasa_saga_http_replay_claim WHERE ctid IN (\
             SELECT ctid FROM nasa_saga_http_replay_claim \
             WHERE expires_at_ms < $1 ORDER BY expires_at_ms LIMIT 256\
         )",
    )
    .bind(now_ms)
    .execute(connection.as_mut())
    .await?;
    let inserted: Option<i32> = sqlx::query_scalar(
        "INSERT INTO nasa_saga_http_replay_claim (producer, nonce, expires_at_ms) \
         VALUES ($1, $2, $3) ON CONFLICT (producer, nonce) DO NOTHING RETURNING 1",
    )
    .bind(producer)
    .bind(nonce)
    .bind(expires_at_ms)
    .fetch_optional(connection.as_mut())
    .await?;
    Ok(inserted.is_some())
}

/// 业务作用：在目标 PostgreSQL database/schema 范围内取得跨进程 advisory lock，使多副本 DDL 串行化。
///
/// 参数说明：`datasource` 是已冻结的 PostgreSQL qualifier。
///
/// 返回：取得 database/schema 级 session lock 时返回持锁连接；连接或锁操作失败时返回错误。
async fn acquire_schema_lock(datasource: &str) -> anyhow::Result<natx_pgsql::PgConn> {
    let mut connection = natx_pgsql::conn_for(datasource).await?;
    // PostgreSQL advisory lock 属于会话；把连接预先置为丢弃态，确保任务取消、DDL 失败
    // 或解锁结果不确定时以物理断连释放权威，不让持锁会话重新进入连接池。
    connection.close_on_drop()?;
    sqlx::query(
        "SELECT pg_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0))",
    )
    .execute(connection.as_mut())
    .await?;
    Ok(connection)
}

/// 业务作用：在全部结构复验成功后显式释放 PostgreSQL schema 自举权。
///
/// 参数说明：`connection` 是取得 advisory lock 的同一 session。
///
/// 返回：服务端确认释放时成功；持有权已丢失或往返失败时返回错误。
async fn release_schema_lock(connection: &mut natx_pgsql::PgConn) -> anyhow::Result<()> {
    let released: bool = sqlx::query_scalar(
        "SELECT pg_advisory_unlock(hashtextextended(current_database() || ':' || current_schema(), 0))",
    )
    .fetch_one(connection.as_mut())
    .await?;
    anyhow::ensure!(released, "Saga schema lock ownership was lost");
    Ok(())
}

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

/// 业务作用：为受管 transport 擦除具体业务 Service 类型，同时保留 PostgreSQL Participant 的事务提交边界。
pub trait ManagedSagaCommandHandler: Send + Sync + 'static {
    /// 业务作用：把已认证命令交给宏生成的精确步骤适配器。
    ///
    /// 参数说明：
    /// - `envelope`: 已通过路径、签名和 producer 门禁的命令。
    /// - `producer`: 从受信凭据映射出的 Orchestrator 身份。
    /// - `receipt_trace`: 收据携带的可选链路上下文。
    ///
    /// 返回：参与方本地事务提交后返回可确认结论；未提交时返回错误。
    fn handle<'a>(
        &'a self,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
        receipt_trace: Option<&'a TraceContext>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a>,
    >;
}

/// 业务作用：描述一个允许 managed 模式构造的本地 PostgreSQL 步骤 Service 工厂。
#[derive(Clone, Copy)]
pub struct ManagedSagaStepFactory {
    /// workflow 名称。
    pub workflow: &'static str,
    /// definition 版本。
    pub definition_version: u32,
    /// 步骤名称。
    pub step: &'static str,
    /// 多数据源参与方使用的本地事务域绑定；单数据源参与方保持为空。
    pub binding: Option<&'static str>,
    /// 使用已冻结 Participant runtime 构造类型擦除 handler。
    pub factory:
        fn(std::sync::Arc<ParticipantRuntime>) -> std::sync::Arc<dyn ManagedSagaCommandHandler>,
    /// 声明位置，仅用于 Ready 冲突定位。
    pub source: &'static str,
}

/// 当前二进制中显式允许受管构造的全部 PostgreSQL 步骤工厂。
#[nasaga_runtime_core::__private::linkme::distributed_slice]
pub static COLLECTED_MANAGED_SAGA_STEPS: [ManagedSagaStepFactory];

/// 业务作用：把 `Default` 构造的业务 Service 与 PostgreSQL Participant runtime 绑定为受管 handler。
///
/// 参数说明：`runtime` 持有同源本地事务能力，`service` 是宏声明的业务步骤实现。
///
/// 返回：不暴露具体 Service 类型的共享 handler。
#[doc(hidden)]
pub fn managed_saga_step_handler<S>(
    runtime: std::sync::Arc<ParticipantRuntime>,
    service: S,
) -> std::sync::Arc<dyn ManagedSagaCommandHandler>
where
    S: PgSagaCommandService,
{
    std::sync::Arc::new(ManagedSagaCommandHandlerAdapter { runtime, service })
}

/// 业务作用：保存受管 PostgreSQL Participant runtime 与单个类型化业务 Service。
struct ManagedSagaCommandHandlerAdapter<S> {
    runtime: std::sync::Arc<ParticipantRuntime>,
    service: S,
}

impl<S> ManagedSagaCommandHandler for ManagedSagaCommandHandlerAdapter<S>
where
    S: PgSagaCommandService,
{
    /// 业务作用：经宏生成适配器执行完整 Inbox、gate、业务事实与结果 Outbox 事务序。
    ///
    /// 参数说明：
    /// - `envelope`: 已认证命令。
    /// - `producer`: 受信 Orchestrator 身份。
    /// - `receipt_trace`: 可选链路上下文。
    ///
    /// 返回：本地提交明确时返回可确认结论，否则返回错误并保留源事件。
    fn handle<'a>(
        &'a self,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
        receipt_trace: Option<&'a TraceContext>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a>,
    > {
        Box::pin(self.service.handle_saga_command_traced(
            &self.runtime,
            envelope,
            producer,
            receipt_trace,
        ))
    }
}

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
