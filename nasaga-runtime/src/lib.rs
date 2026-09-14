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
mod catalog;
mod participant;
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
mod transport_shared;

pub use backend::{MySqlSagaBackend, MySqlSagaTransactionRunner};
pub use catalog::{
    acknowledge_catalog_generation_for, activate_definition_for,
    activate_definition_with_operation_for, catalog_generation_fully_acknowledged_for,
    deprecate_definition_for, deprecate_definition_with_operation_for, load_definition_for,
    load_dynamic_catalog_for, publish_definition_for, register_capability_for,
    retire_catalog_replica_for, retire_definition_with_operation_for,
};
pub use nasaga_backend::SagaInstanceRow;
pub use nasaga_runtime_core::{
    derive_result_event_id, select_capability_route, select_capability_routes,
    validate_capability_route_contract, validate_redis_stream_route, CapabilityDescriptor,
    CapabilityReceipt, DefinitionActivationGate, DefinitionArtifact, DefinitionCatalogError,
    DefinitionLifecycle, DefinitionLifecycleOperation, DefinitionPublishDisposition,
    DefinitionRecord, DynamicCatalogSnapshot, HandleOutcome, OrchestratorConfig,
    RegisteredCapability, SagaAuditPage, SagaAuditPageCursor, SagaAuditRecord, SagaAuditTrail,
    SagaCommandEnvelope, SagaConcurrencyError, SagaManagementContext, SagaManagementError,
    SagaManagementExpectation, SagaManagementPermission, SagaOperationalMetrics,
    SagaResultEnvelope, StartOutcome, StartSagaError, StartSagaRequest, TenantActionRate,
    TimerOutcome, VerifiedIdentity, COMMAND_EVENT_TYPE, RESULT_EVENT_TYPE, SAGA_AGGREGATE_TYPE,
};
pub use nasaga_runtime_core::{
    render_saga_latency_metrics, saga_latency_snapshot, SagaLatencySnapshot, SAGA_LATENCY_BUCKETS,
};
pub use nasaga_runtime_core::{SagaPayload, SagaPayloadContract, SagaPayloadError};
/// 既有 MySQL Orchestrator 入口；全部状态裁决由 `nasaga-runtime-core` 执行。
pub type Orchestrator = nasaga_runtime_core::Orchestrator<MySqlSagaBackend>;
/// 既有 MySQL Participant 入口；业务代码不需要增加泛型参数。
pub type ParticipantRuntime = nasaga_runtime_core::ParticipantRuntime<MySqlSagaBackend>;
/// 既有已认证 MySQL Participant 能力视图。
pub type AuthenticatedParticipantRuntime<'a> =
    nasaga_runtime_core::AuthenticatedParticipantRuntime<'a, MySqlSagaBackend>;
pub use nasaga_runtime_core::{
    render_saga_http_command_dlt_metric, SagaHttpCredentialSource, SagaHttpCredentials,
    SagaHttpMessageAuthError, SagaHttpMessageAuthFailure, SagaHttpMessageAuthenticator,
    SagaHttpReplayGuard, SagaHttpReplayMetricAggregate, SagaHttpReplayMetrics, SagaHttpReplayPlane,
    SagaHttpSignedMessage,
};

pub use nasaga_runtime_core::{
    classify_command_delivery_error, classify_result_delivery_error, command_dead_letter_reason,
    CommandDeliveryDisposition, CommandDeliveryPolicy, ResultDeliveryDisposition,
    ResultDeliveryPolicy, SagaCommandProcessingError, SagaResultProcessingError,
    SAGA_COMMAND_DEAD_LETTER_REASONS,
};

// 检索查询/摘要与 fencing 类型同为公开管理面输入输出,经本 crate 统一再导出。
pub use nasaga_mysql::{
    validate_saga_instance_query, SagaInstanceQuery, SagaInstanceQueryParameterError,
    SagaInstanceSummary, TimerFencingToken, TimerFencingTokenIssuer, SAGA_INSTANCE_TIME_MAX_MS,
    SAGA_INSTANCE_TIME_MIN_MS,
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
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
pub use transport_shared::ParticipantCommandHandler;

pub use nasaga_runtime_core::{
    collect_workflow_definitions, verify_descriptors, SagaStepDescriptor, SagaWorkflowDescriptor,
    COLLECTED_SAGA_STEPS, COLLECTED_SAGA_WORKFLOWS,
};

/// 业务作用：为受管 transport 擦除具体业务 Service 类型，同时保留 MySQL Participant 的事务提交边界。
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

/// 业务作用：描述一个允许 managed 模式构造的本地步骤 Service 工厂。
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

/// 当前二进制中显式允许受管构造的全部 MySQL 步骤工厂。
#[linkme::distributed_slice]
pub static COLLECTED_MANAGED_SAGA_STEPS: [ManagedSagaStepFactory];

/// 业务作用：把 `Default` 构造的业务 Service 与 MySQL Participant runtime 绑定为受管 handler。
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
    S: SagaCommandService,
{
    std::sync::Arc::new(ManagedSagaCommandHandlerAdapter { runtime, service })
}

/// 业务作用：保存受管 MySQL Participant runtime 与单个类型化业务 Service。
struct ManagedSagaCommandHandlerAdapter<S> {
    runtime: std::sync::Arc<ParticipantRuntime>,
    service: S,
}

impl<S> ManagedSagaCommandHandler for ManagedSagaCommandHandlerAdapter<S>
where
    S: SagaCommandService,
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

/// 业务作用：在指定 MySQL 事务域中建立并复验 Orchestrator 独占的 Saga、result Inbox 与 command Outbox 持久结构。
///
/// 参数说明：`datasource` 是 Application 已冻结的 MySQL datasource qualifier。
///
/// 返回：全部结构可用时成功；连接、DDL 或最终结构门禁失败时返回错误。
pub async fn ensure_orchestrator_schema_for(datasource: &str) -> anyhow::Result<()> {
    let mut lock_connection = acquire_schema_lock(datasource).await?;
    nasaga_mysql::MySqlSagaStore::ensure_schema_on_connection(&mut lock_connection).await?;
    nainbox_mysql::MySqlInbox::ensure_schema_on_connection(&mut lock_connection).await?;
    naoutbox_mysql::MySqlOutbox::ensure_schema_on_connection(&mut lock_connection).await?;
    ensure_http_replay_schema(&mut lock_connection).await?;
    catalog::ensure_schema(&mut lock_connection).await?;
    release_schema_lock(&mut lock_connection).await?;
    Ok(())
}

/// 业务作用：在指定 MySQL 事务域中建立并复验 participant gate、command Inbox 与 result Outbox 持久结构。
///
/// 参数说明：`datasource` 是 participant binding 唯一绑定的 MySQL datasource qualifier。
///
/// 返回：全部本地事务结构可用时成功；任一结构不可用时返回错误。
pub async fn ensure_participant_schema_for(datasource: &str) -> anyhow::Result<()> {
    let mut lock_connection = acquire_schema_lock(datasource).await?;
    nasaga_mysql::MySqlSagaStore::ensure_participant_schema_on_connection(&mut lock_connection)
        .await?;
    nainbox_mysql::MySqlInbox::ensure_schema_on_connection(&mut lock_connection).await?;
    naoutbox_mysql::MySqlOutbox::ensure_schema_on_connection(&mut lock_connection).await?;
    ensure_http_replay_schema(&mut lock_connection).await?;
    release_schema_lock(&mut lock_connection).await?;
    Ok(())
}

/// 业务作用：在角色事务域中建立跨副本共享的 HTTP nonce 一次性裁决表。
///
/// 参数说明：`connection` 是持有当前 database schema 自举锁的连接。
///
/// 返回：表与过期索引可用时成功；DDL 权限或结构冲突返回错误并阻止 Ready。
async fn ensure_http_replay_schema(connection: &mut natx::Conn) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nasa_saga_http_replay_claim (\
             producer VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             nonce CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             expires_at_ms BIGINT NOT NULL,\
             claimed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\
             PRIMARY KEY (producer, nonce),\
             INDEX idx_nasa_saga_http_replay_expiry (expires_at_ms)\
         ) ENGINE=InnoDB",
    )
    .execute(connection.as_mut())
    .await?;
    let valid_columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() \
         AND table_name = 'nasa_saga_http_replay_claim' AND is_nullable = 'NO' AND (\
           (column_name = 'producer' AND column_type = 'varchar(128)' AND collation_name = 'ascii_bin') OR \
           (column_name = 'nonce' AND column_type = 'char(32)' AND collation_name = 'ascii_bin') OR \
           (column_name = 'expires_at_ms' AND column_type = 'bigint') OR \
           (column_name = 'claimed_at' AND column_type = 'timestamp(6)'))",
    )
    .fetch_one(connection.as_mut())
    .await?;
    let table_valid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() \
         AND table_name = 'nasa_saga_http_replay_claim' AND engine = 'InnoDB' \
         AND table_collation = @@collation_database",
    )
    .fetch_one(connection.as_mut())
    .await?;
    let primary_columns: Option<String> = sqlx::query_scalar(
        "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') \
         FROM information_schema.statistics WHERE table_schema = DATABASE() \
         AND table_name = 'nasa_saga_http_replay_claim' AND index_name = 'PRIMARY' \
         AND non_unique = 0 GROUP BY index_name",
    )
    .fetch_optional(connection.as_mut())
    .await?;
    let expiry_columns: Option<String> = sqlx::query_scalar(
        "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') \
         FROM information_schema.statistics WHERE table_schema = DATABASE() \
         AND table_name = 'nasa_saga_http_replay_claim' \
         AND index_name = 'idx_nasa_saga_http_replay_expiry' GROUP BY index_name",
    )
    .fetch_optional(connection.as_mut())
    .await?;
    anyhow::ensure!(
        valid_columns == 4
            && table_valid == 1
            && primary_columns.as_deref() == Some("producer,nonce")
            && expiry_columns.as_deref() == Some("expires_at_ms"),
        "Saga HTTP replay schema contract is invalid"
    );
    Ok(())
}

/// 业务作用：在共享 MySQL 事务域中原子占用已验真的 producer/nonce，阻止其它副本再次受理同一请求。
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
    let mut connection = natx::conn_for(datasource).await?;
    // 过期证据只影响容量，不影响正确性；有界清理避免请求路径一次删除无上限历史行。
    sqlx::query(
        "DELETE FROM nasa_saga_http_replay_claim WHERE expires_at_ms < ? ORDER BY expires_at_ms LIMIT 256",
    )
    .bind(now_ms)
    .execute(connection.as_mut())
    .await?;
    let result = sqlx::query(
        "INSERT IGNORE INTO nasa_saga_http_replay_claim (producer, nonce, expires_at_ms) VALUES (?, ?, ?)",
    )
    .bind(producer)
    .bind(nonce)
    .bind(expires_at_ms)
    .execute(connection.as_mut())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// 业务作用：在目标 MySQL database 范围内取得跨进程 schema 自举互斥权，使多副本 DDL 串行化。
///
/// 参数说明：`datasource` 是已冻结的 MySQL qualifier。
///
/// 返回：三十秒内取得 database 级 named lock 时返回持锁连接；超时或连接失败时返回错误。
async fn acquire_schema_lock(datasource: &str) -> anyhow::Result<natx::Conn> {
    let mut connection = natx::conn_for(datasource).await?;
    // 会话锁从请求发出起就可能已在服务端生效；把连接预先置为丢弃态，确保任务取消、
    // DDL 失败或解锁结果不确定时通过物理断连释放权威，不把持锁会话归还连接池。
    connection.close_on_drop()?;
    // MySQL 把 named lock 限制为 64 字节；database 名可占满自身上限，因此以稳定摘要
    // 保留逐库互斥域，避免合法长库名绕过后续结构门禁。
    let acquired: Option<i64> =
        sqlx::query_scalar("SELECT GET_LOCK(CONCAT('nasa:saga:', MD5(DATABASE())), 30)")
            .fetch_one(connection.as_mut())
            .await?;
    anyhow::ensure!(acquired == Some(1), "Saga schema lock is unavailable");
    Ok(connection)
}

/// 业务作用：在全部结构复验成功后显式释放 MySQL schema 自举权。
///
/// 参数说明：`connection` 是取得 named lock 的同一 session。
///
/// 返回：服务端确认释放时成功；丢失持有权或往返失败时返回错误。
async fn release_schema_lock(connection: &mut natx::Conn) -> anyhow::Result<()> {
    let released: Option<i64> =
        sqlx::query_scalar("SELECT RELEASE_LOCK(CONCAT('nasa:saga:', MD5(DATABASE())))")
            .fetch_one(connection.as_mut())
            .await?;
    anyhow::ensure!(released == Some(1), "Saga schema lock ownership was lost");
    Ok(())
}

/// 宏展开专用的内部再导出；业务代码不应直接使用。
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use linkme;
    pub use nasaga_core as core;
    pub use nasaga_runtime_core as runtime_core;
}
