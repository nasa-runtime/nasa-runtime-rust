//! Saga backend 中立状态机与 wrapper 共享的进程级运行状态。
//!
//! Orchestrator、Participant、timer、补偿、恢复与 fencing 裁决只在本 crate
//! 保留一份，MySQL 与 PostgreSQL wrapper 只注入 [`nasaga_backend::SagaBackend`]。
//! 数据库事实由各 backend 读取；transport、配额与管理动作指标全进程只计一次。
//!
//! [`Orchestrator::handle_authenticated_result_authorized_traced`] 让宿主把冻结的结果资格带入状态事务。
//! 同一次资格在异步恢复、实例锁后及事务交还前持续复验；失权回滚完整事务并以
//! [`SagaResultProcessingError::AuthorityUnavailable`] 保留原事件重投。宿主必须用不可复用的安全
//! 发布代际识别 A→B→A，不能只比较当前材料摘要。COMMIT 已发出后的结局仍按 backend 收据裁决。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use nasaga_core::{SagaPayload, SagaPayloadContract, SagaPayloadError};
mod latency;
pub use latency::{
    render_saga_latency_metrics, saga_latency_snapshot, SagaLatencySnapshot, SAGA_LATENCY_BUCKETS,
};
mod catalog;
mod envelope;
mod management;
mod observability;
mod orchestrator;
mod participant;
mod registry;
mod scheduled;
mod timers;
mod transaction;
#[cfg(feature = "kafka")]
mod transport;
mod transport_auth;
#[cfg(feature = "grpc-transport")]
mod transport_grpc;
#[cfg(feature = "redis-stream")]
mod transport_redis;
#[cfg(any(
    feature = "kafka",
    feature = "redis-stream",
    feature = "grpc-transport"
))]
mod transport_shared;

pub use catalog::{
    select_capability_route, select_capability_routes, validate_capability_route_contract,
    validate_redis_stream_route, CapabilityDescriptor, CapabilityReceipt, DefinitionActivationGate,
    DefinitionArtifact, DefinitionCancelMode, DefinitionCatalogError, DefinitionCompensation,
    DefinitionLifecycle, DefinitionLifecycleOperation, DefinitionPublishDisposition,
    DefinitionRecord, DefinitionResolution, DefinitionResolutionMode, DefinitionStepArtifact,
    DefinitionTimeoutPolicy, DynamicCatalogSnapshot, RegisteredCapability,
    CATALOG_OBJECT_KEY_MAX_LEN,
};
pub use envelope::{
    canonical_bytes, derive_result_event_id, SagaCommandEnvelope, SagaResultEnvelope,
    VerifiedIdentity, COMMAND_EVENT_TYPE, RESULT_EVENT_TYPE, SAGA_AGGREGATE_TYPE,
};
pub use management::{
    SagaAuditPage, SagaAuditPageCursor, SagaAuditRecord, SagaAuditTrail, SagaManagementContext,
    SagaManagementError, SagaManagementExpectation, SagaManagementPermission,
};
pub use observability::SagaOperationalMetrics;
pub use orchestrator::{
    HandleOutcome, Orchestrator, OrchestratorConfig, SagaConcurrencyError, StartOutcome,
    StartSagaError, StartSagaRequest, TenantActionRate, TimerOutcome,
};
pub use participant::{
    AuthenticatedParticipantRuntime, ParticipantCommandTrust, ParticipantHandled,
    ParticipantRuntime, SagaCommandService,
};
pub use registry::DefinitionRegistry;
pub use scheduled::{
    derive_scheduled_business_key, ScheduledBatchReport, ScheduledBatchSpec, ScheduledItem,
};
pub use timers::{
    derive_timer_id, KIND_CANCEL_TIMEOUT, KIND_COMPENSATE_TIMEOUT,
    KIND_COMPENSATION_RESOLUTION_BUDGET, KIND_FORWARD_RESOLUTION_BUDGET, KIND_INSTANCE_DEADLINE,
    KIND_RESOLUTION_BUDGET, KIND_RESOLVE_TIMEOUT, KIND_STEP_TIMEOUT,
};
pub use transaction::SagaTransactionError;
#[cfg(feature = "kafka")]
pub use transport::{
    SagaCommandRoute, SagaKafkaCommandConsumer, SagaKafkaCommandConsumerConfig,
    SagaKafkaResultConsumer, SagaKafkaResultConsumerConfig,
};
pub use transport_auth::{
    render_saga_http_command_dlt_metric, SagaHttpCredentialSource, SagaHttpCredentials,
    SagaHttpMessageAuthError, SagaHttpMessageAuthFailure, SagaHttpMessageAuthenticator,
    SagaHttpReplayGuard, SagaHttpReplayMetricAggregate, SagaHttpReplayMetrics, SagaHttpReplayPlane,
    SagaHttpSignedMessage,
};
#[cfg(feature = "grpc-transport")]
pub use transport_grpc::{
    orchestrator_proto, outbox_disposition_of, proto as grpc_proto, SagaGrpcBindingError,
    SagaGrpcCommandServer, SagaGrpcCommandTransportService, SagaGrpcPeerBinding,
    SagaGrpcPeerIdentity, SagaGrpcReceipt, SagaGrpcResultServer, SagaGrpcResultTransportService,
};
#[cfg(feature = "redis-stream")]
pub use transport_redis::{
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
pub use transport_shared::{ParticipantCommandHandler, SagaCommandHandler, SagaResultHandler};

use nasaga_core::{CancelMode, Compensation, ResolutionMode};

/// 业务作用：用封闭类型标记 Saga command 的不可恢复投递错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SagaCommandProcessingError {
    /// transport 认证出的 producer 不在参与方信任投影中。
    ProducerUnauthorized,
    /// envelope 身份、派生身份或编码不合法。
    IdentityInvalid,
    /// command route 无权承载目标 workflow 步骤。
    RouteUnauthorized,
    /// phase、取消能力或 payload 违反已发布步骤合同。
    ContractInvalid,
}

impl SagaCommandProcessingError {
    /// 业务作用：返回可安全写入 DLT header 的稳定原因码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含 envelope、payload 或底层错误的低基数原因码。
    pub const fn dead_letter_reason(self) -> &'static str {
        match self {
            Self::ProducerUnauthorized => "saga_command_producer_unauthorized",
            Self::IdentityInvalid => "saga_command_identity_invalid",
            Self::RouteUnauthorized => "saga_command_route_unauthorized",
            Self::ContractInvalid => "saga_command_contract_invalid",
        }
    }

    /// 业务作用：把未受信原因码收窄为封闭 command 错误。
    ///
    /// 参数说明：`reason` 是 connector 收到的候选原因码。
    ///
    /// 返回：精确命中白名单时返回对应类型；未知文本返回 `None`。
    pub fn from_dead_letter_reason(reason: &str) -> Option<Self> {
        match reason {
            "saga_command_producer_unauthorized" => Some(Self::ProducerUnauthorized),
            "saga_command_identity_invalid" => Some(Self::IdentityInvalid),
            "saga_command_route_unauthorized" => Some(Self::RouteUnauthorized),
            "saga_command_contract_invalid" => Some(Self::ContractInvalid),
            _ => None,
        }
    }
}

impl std::fmt::Display for SagaCommandProcessingError {
    /// 业务作用：输出稳定、脱敏的 command 错误分类。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：分类文本写入成功返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ProducerUnauthorized => "saga command producer is unauthorized",
            Self::IdentityInvalid => "saga command identity is invalid",
            Self::RouteUnauthorized => "saga command route is unauthorized",
            Self::ContractInvalid => "saga command contract is invalid",
        })
    }
}

impl std::error::Error for SagaCommandProcessingError {}

/// command DLT 接收端可信的全部稳定原因码。
pub const SAGA_COMMAND_DEAD_LETTER_REASONS: &[&str] = &[
    "saga_command_producer_unauthorized",
    "saga_command_identity_invalid",
    "saga_command_route_unauthorized",
    "saga_command_contract_invalid",
];

/// 业务作用：用封闭类型标记 Saga result 的可信投递分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SagaResultProcessingError {
    /// 实例处于 `PAUSED`，恢复后必须继续处理同一结果。
    Paused,
    /// 本次结果操作的运行资格已经撤销或到期，须保留原事件等待重新准入。
    AuthorityUnavailable,
    /// transport 认证出的 producer 无权为目标步骤作证。
    ProducerUnauthorized,
    /// envelope 自报身份、派生身份或编码不合法。
    IdentityInvalid,
    /// envelope 与固定实例和 definition 合同不一致。
    ContractInvalid,
}

impl SagaResultProcessingError {
    /// 业务作用：返回可进入 DLT 的稳定 result 原因码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：暂停或失权返回 `None`；确定性协议错误返回脱敏原因码。
    pub const fn dead_letter_reason(self) -> Option<&'static str> {
        match self {
            Self::Paused | Self::AuthorityUnavailable => None,
            Self::ProducerUnauthorized => Some("saga_result_producer_unauthorized"),
            Self::IdentityInvalid => Some("saga_result_identity_invalid"),
            Self::ContractInvalid => Some("saga_result_contract_invalid"),
        }
    }
}

impl std::fmt::Display for SagaResultProcessingError {
    /// 业务作用：输出稳定、脱敏的 result 错误分类。
    ///
    /// 参数说明：`formatter` 是标准格式化目标。
    ///
    /// 返回：分类文本写入成功返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Paused => "saga result processing deferred while instance is paused",
            Self::AuthorityUnavailable => "saga result processing authority is unavailable",
            Self::ProducerUnauthorized => "saga result producer is unauthorized",
            Self::IdentityInvalid => "saga result identity is invalid",
            Self::ContractInvalid => "saga result contract is invalid",
        })
    }
}

impl std::error::Error for SagaResultProcessingError {}

/// 业务作用：把 Participant command 失败收敛为 transport 可执行的保留、持续收敛或隔离动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDeliveryDisposition {
    /// 普通瞬态失败，保留消息并消耗有界重试预算。
    Retry,
    /// 提交或回滚结果不确定，保留消息且不消耗隔离预算。
    Defer,
    /// 来源、身份或合同确定性非法，允许进入耐久 DLT。
    DeadLetter,
}

/// 业务作用：限制同一 command 的普通瞬态重试次数，不影响结果不确定分支的持续收敛。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandDeliveryPolicy {
    /// 普通失败可重投的最大序号。
    pub max_retries: u32,
}

impl Default for CommandDeliveryPolicy {
    /// 业务作用：提供有界的默认 command 重投预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最多允许十六次普通失败重投的策略。
    fn default() -> Self {
        Self { max_retries: 16 }
    }
}

impl CommandDeliveryPolicy {
    /// 业务作用：结合失败类别与本次普通重试序号裁决 command 投递动作。
    ///
    /// 参数说明：`error` 是完整错误链，`retry_attempt` 是从一开始的普通失败序号。
    ///
    /// 返回：确定性合同错误立即隔离，普通失败超预算后隔离，关键事务阶段持续保留。
    pub fn decide(self, error: &anyhow::Error, retry_attempt: u32) -> CommandDeliveryDisposition {
        let disposition = classify_command_delivery_error(error);
        if disposition == CommandDeliveryDisposition::Retry && retry_attempt > self.max_retries {
            CommandDeliveryDisposition::DeadLetter
        } else {
            disposition
        }
    }
}

/// 业务作用：从 command 错误链提取可安全跨 transport 传播的封闭 DLT 原因码。
///
/// 参数说明：`error` 是 Participant 或入口门禁返回的完整错误链。
///
/// 返回：命中类型化确定性错误时返回低基数原因码；其它失败返回 `None`。
pub fn command_dead_letter_reason(error: &anyhow::Error) -> Option<&'static str> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<SagaCommandProcessingError>()
            .map(|error| error.dead_letter_reason())
    })
}

/// 业务作用：为 hosted command consumer 形成不依赖错误文本的重试与隔离分类。
///
/// 参数说明：`error` 是 transport 身份门禁或 Participant 事务错误。
///
/// 返回：合同错误隔离，事务关键阶段持续保留，其它失败有界重试。
pub fn classify_command_delivery_error(error: &anyhow::Error) -> CommandDeliveryDisposition {
    if command_dead_letter_reason(error).is_some() {
        CommandDeliveryDisposition::DeadLetter
    } else if error.chain().any(|cause| {
        cause
            .downcast_ref::<SagaTransactionError>()
            .is_some_and(|error| error.requires_unbounded_redelivery())
    }) {
        CommandDeliveryDisposition::Defer
    } else {
        CommandDeliveryDisposition::Retry
    }
}

/// 业务作用：把 Orchestrator 结果处理失败收敛为 transport 可执行动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultDeliveryDisposition {
    /// 普通瞬态失败，保留消息并消耗有界预算。
    Retry,
    /// 暂停或事务结果不确定，保留消息且不消耗隔离预算。
    Defer,
    /// 确定性协议错误，允许保留原文后进入 DLT。
    DeadLetter,
}

/// 业务作用：限制 result 普通瞬态重试次数，同时保留暂停与事务不确定的无限收敛语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultDeliveryPolicy {
    /// 普通失败可重投的最大序号。
    pub max_retries: u32,
}

impl Default for ResultDeliveryPolicy {
    /// 业务作用：提供有界的默认 result 重投预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：最多允许十六次普通失败重投的策略。
    fn default() -> Self {
        Self { max_retries: 16 }
    }
}

impl ResultDeliveryPolicy {
    /// 业务作用：结合失败类别与普通失败序号裁决 result 投递动作。
    ///
    /// 参数说明：`error` 是完整错误链，`retry_attempt` 是从一开始的普通失败序号。
    ///
    /// 返回：暂停、失权和事务不确定持续保留，普通失败超预算后隔离。
    pub fn decide(self, error: &anyhow::Error, retry_attempt: u32) -> ResultDeliveryDisposition {
        let disposition = classify_result_delivery_error(error);
        if disposition == ResultDeliveryDisposition::Retry && retry_attempt > self.max_retries {
            ResultDeliveryDisposition::DeadLetter
        } else {
            disposition
        }
    }
}

/// 业务作用：为 hosted result consumer 形成不依赖错误文本的重试、延后与隔离分类。
///
/// 参数说明：`error` 是 Orchestrator 结果处理返回的完整错误链。
///
/// 返回：暂停、失权与事务关键阶段延后，确定性协议错误隔离，其它失败有界重试。
pub fn classify_result_delivery_error(error: &anyhow::Error) -> ResultDeliveryDisposition {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<SagaTransactionError>()
            .is_some_and(|error| error.requires_unbounded_redelivery())
    }) {
        return ResultDeliveryDisposition::Defer;
    }
    let classified = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<SagaResultProcessingError>().copied());
    if matches!(
        classified,
        Some(SagaResultProcessingError::Paused | SagaResultProcessingError::AuthorityUnavailable)
    ) {
        ResultDeliveryDisposition::Defer
    } else if classified.is_some_and(|error| error.dead_letter_reason().is_some()) {
        ResultDeliveryDisposition::DeadLetter
    } else {
        ResultDeliveryDisposition::Retry
    }
}

/// 业务作用：`#[saga]` 为每个本地步骤生成的静态合同投影。
#[derive(Debug, Clone, Copy)]
pub struct SagaStepDescriptor {
    /// 本 handler 接受的规范媒体类型。
    pub payload_content_type: &'static str,
    /// 本 handler 支持的稳定 schema 身份。
    pub payload_schema_id: &'static str,

    /// workflow 名称。
    pub workflow: &'static str,
    /// definition 版本。
    pub definition_version: u32,
    /// 步骤名称。
    pub step: &'static str,
    /// 多事务域参与方使用的显式 datasource binding；单事务域可省略。
    pub binding: Option<&'static str>,
    /// 业务 Service 类型名。
    pub service_type: &'static str,
    /// 是否可补偿。
    pub compensation: Compensation,
    /// 取消形态。
    pub cancel_mode: CancelMode,
    /// 是否允许返回 `Unknown`。
    pub allow_unknown: bool,
    /// 本地托管的解决能力。
    pub resolution_mode: Option<ResolutionMode>,
    /// 源码位置，只用于启动拒绝时定位重复合同。
    pub source: &'static str,
}

/// 当前 binary 内 `#[saga]` 收集到的全部本地步骤合同。
#[linkme::distributed_slice]
pub static COLLECTED_SAGA_STEPS: [SagaStepDescriptor];

/// 业务作用：描述一个由 `#[saga_workflow]` 登记的只读流程定义工厂。
///
/// 工厂只返回业务合同，不持有 Orchestrator、transport 或 Application 生命周期；来源位置只在
/// Ready 拒绝时帮助定位同一二进制内的冲突声明。
#[derive(Clone, Copy)]
pub struct SagaWorkflowDescriptor {
    /// 返回一份已经完成业务字段构造、仍需进入运行时注册校验的流程定义。
    pub factory: fn() -> anyhow::Result<nasaga_core::WorkflowDefinition>,
    /// 声明所在源码位置，只用于启动拒绝的低敏定位。
    pub source: &'static str,
}

/// 当前二进制内 `#[saga_workflow]` 收集到的全部流程定义工厂。
#[linkme::distributed_slice]
pub static COLLECTED_SAGA_WORKFLOWS: [SagaWorkflowDescriptor];

/// 业务作用：调用当前二进制内全部流程工厂，并构造不可变、按定义键去重的注册表快照。
///
/// 收集顺序不具备业务含义；每份 definition 自身携带完整步骤顺序。同键同摘要按幂等声明处理，
/// 同键不同摘要拒绝启动，不能由链接顺序决定哪一份合同生效。
///
/// 参数说明: 无。
///
/// 返回：全部工厂和 definition 校验通过时返回注册表；构造失败或摘要冲突时返回带声明位置的错误。
pub fn collect_workflow_definitions() -> anyhow::Result<DefinitionRegistry> {
    let mut registry = DefinitionRegistry::new();
    for descriptor in COLLECTED_SAGA_WORKFLOWS {
        let definition = (descriptor.factory)().map_err(|error| {
            anyhow::anyhow!(
                "saga workflow definition factory failed at {}: {}",
                descriptor.source,
                error
            )
        })?;
        registry.register(definition).map_err(|error| {
            anyhow::anyhow!(
                "saga workflow definition is invalid at {}: {}",
                descriptor.source,
                error
            )
        })?;
    }
    Ok(registry)
}

/// 业务作用：在 Ready 前复验本地步骤合同唯一且与 definition 一致。
///
/// 参数说明：`registry` 是启动期冻结的 workflow definition 集合。
///
/// 返回：本地投影无重复且已注册 definition 不漂移时返回 `Ok`；否则拒绝 Ready。
pub fn verify_descriptors(registry: &DefinitionRegistry) -> anyhow::Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for descriptor in COLLECTED_SAGA_STEPS {
        nasaga_core::SagaPayloadContract {
            content_type: descriptor.payload_content_type.to_owned(),
            schema_id: descriptor.payload_schema_id.to_owned(),
        }
        .validate()?;

        let key = (
            descriptor.workflow,
            descriptor.definition_version,
            descriptor.step,
        );
        // 同一 binary 不能让两个 handler 竞争同一步骤的业务效果。
        if !seen.insert(key) {
            anyhow::bail!(
                "duplicate Saga step `{}` v{} `{}` ({})",
                descriptor.workflow,
                descriptor.definition_version,
                descriptor.step,
                descriptor.source
            );
        }
        let workflow = nasaga_core::WorkflowName::new(descriptor.workflow)
            .map_err(|value| anyhow::anyhow!("bad descriptor workflow: {}", value.code()))?;
        let version = nasaga_core::DefinitionVersion::new(descriptor.definition_version)
            .map_err(|value| anyhow::anyhow!("bad descriptor version: {}", value.code()))?;
        let Some(definition) = registry.get(&workflow, version) else {
            continue;
        };
        let step = nasaga_core::StepName::new(descriptor.step)
            .map_err(|value| anyhow::anyhow!("bad descriptor step: {}", value.code()))?;
        let Some(step_definition) = definition.step(&step) else {
            anyhow::bail!(
                "descriptor step `{}` is absent from workflow `{}` v{} ({})",
                descriptor.step,
                descriptor.workflow,
                descriptor.definition_version,
                descriptor.source
            );
        };
        // 合同漂移在 Ready 前拒绝，避免已运行实例被不同的补偿或取消语义驱动。
        if step_definition.payload_contract().content_type != descriptor.payload_content_type
            || step_definition.payload_contract().schema_id != descriptor.payload_schema_id
            || step_definition.compensation() != descriptor.compensation
            || step_definition.cancel_mode() != descriptor.cancel_mode
            || step_definition.resolution().allow_unknown() != descriptor.allow_unknown
            || step_definition.resolution().mode() != descriptor.resolution_mode
        {
            anyhow::bail!(
                "descriptor contract drift on step `{}` of `{}` v{} ({})",
                descriptor.step,
                descriptor.workflow,
                descriptor.definition_version,
                descriptor.source
            );
        }
    }
    Ok(())
}

/// 宏展开专用的内部再导出。
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    pub use linkme;
    pub use nasaga_core as core;
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static KAFKA_RESULT_PROCESSING_TOTAL: AtomicU64 = AtomicU64::new(0);
static QUOTA_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static ACTION_RATE_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_PROCESSING_MICROS: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_DLT_REQUESTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_ACK_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_DUPLICATE_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_PROCESSING_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_PROCESSING_MICROS: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_DLT_REQUESTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_ACK_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_DUPLICATE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// 业务作用：保存同一进程内只允许存在一份的 Saga transport 与治理计数快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SagaProcessMetrics {
    /// 按租户配额拒绝的创建请求累计数。
    pub quota_rejections_total: u64,
    /// 按租户频率拒绝的管理动作累计数。
    pub action_rate_rejections_total: u64,
    /// Kafka result handler 调用累计数。
    pub kafka_result_processing_total: u64,
    /// Kafka result handler 累计耗时微秒数。
    pub kafka_result_processing_micros_sum: u64,
    /// Kafka result 保留 offset 重投累计数。
    pub kafka_result_retry_total: u64,
    /// Kafka result 请求进入 durability-first DLT 的累计数。
    pub kafka_result_dlt_requested_total: u64,
    /// Kafka result 在本地提交后 ACK 的累计数。
    pub kafka_result_ack_total: u64,
    /// Kafka result 被 Inbox 吸收后 ACK 的累计数。
    pub kafka_result_duplicate_total: u64,
    /// Kafka command handler 调用累计数。
    pub kafka_command_processing_total: u64,
    /// Kafka command handler 累计耗时微秒数。
    pub kafka_command_processing_micros_sum: u64,
    /// Kafka command 因本地事务未提交而重投的累计数。
    pub kafka_command_retry_total: u64,
    /// Kafka command 请求进入 durability-first DLT 的累计数。
    pub kafka_command_dlt_requested_total: u64,
    /// Kafka command 在参与方提交后 ACK 的累计数。
    pub kafka_command_ack_total: u64,
    /// Kafka command 被 Inbox 吸收后 ACK 的累计数。
    pub kafka_command_duplicate_total: u64,
}

/// 业务作用：读取 MySQL 与 PostgreSQL wrapper 共同维护的 process metrics 快照。
///
/// 参数说明: 无。
///
/// 返回：调用时刻各原子计数的低基数快照；不包含任何数据库事实或业务身份。
pub fn process_metrics_snapshot() -> SagaProcessMetrics {
    SagaProcessMetrics {
        quota_rejections_total: QUOTA_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        action_rate_rejections_total: ACTION_RATE_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        kafka_result_processing_total: KAFKA_RESULT_PROCESSING_TOTAL.load(Ordering::Relaxed),
        kafka_result_processing_micros_sum: KAFKA_RESULT_PROCESSING_MICROS.load(Ordering::Relaxed),
        kafka_result_retry_total: KAFKA_RESULT_RETRY_TOTAL.load(Ordering::Relaxed),
        kafka_result_dlt_requested_total: KAFKA_RESULT_DLT_REQUESTED_TOTAL.load(Ordering::Relaxed),
        kafka_result_ack_total: KAFKA_RESULT_ACK_TOTAL.load(Ordering::Relaxed),
        kafka_result_duplicate_total: KAFKA_RESULT_DUPLICATE_TOTAL.load(Ordering::Relaxed),
        kafka_command_processing_total: KAFKA_COMMAND_PROCESSING_TOTAL.load(Ordering::Relaxed),
        kafka_command_processing_micros_sum: KAFKA_COMMAND_PROCESSING_MICROS
            .load(Ordering::Relaxed),
        kafka_command_retry_total: KAFKA_COMMAND_RETRY_TOTAL.load(Ordering::Relaxed),
        kafka_command_dlt_requested_total: KAFKA_COMMAND_DLT_REQUESTED_TOTAL
            .load(Ordering::Relaxed),
        kafka_command_ack_total: KAFKA_COMMAND_ACK_TOTAL.load(Ordering::Relaxed),
        kafka_command_duplicate_total: KAFKA_COMMAND_DUPLICATE_TOTAL.load(Ordering::Relaxed),
    }
}

/// 业务作用：累计一次按租户配额拒绝的创建请求。
///
/// 参数说明: 无。
///
/// 返回：无；原子计数不阻塞业务线程。
pub fn record_quota_rejection() {
    QUOTA_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：累计一次按租户频率拒绝的变更类管理动作。
///
/// 参数说明: 无。
///
/// 返回：无；原子计数不阻塞业务线程。
pub fn record_action_rate_rejection() {
    ACTION_RATE_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录一次 Kafka Saga result handler 的单调耗时。
///
/// 参数说明：`elapsed` 是从 transport 门禁到本地事务结束的单调耗时。
///
/// 返回：无；次数原子递增，耗时使用饱和累计。
pub fn record_kafka_processing(elapsed: Duration) {
    KAFKA_RESULT_PROCESSING_TOTAL.fetch_add(1, Ordering::Relaxed);
    saturating_add_duration(&KAFKA_RESULT_PROCESSING_MICROS, elapsed);
}

/// 业务作用：记录 Saga result 因本地事务未提交而保留 offset 重投。
///
/// 参数说明: 无。
///
/// 返回：无；原子计数不阻塞 transport。
pub fn record_kafka_retry() {
    KAFKA_RESULT_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Saga result 已请求进入 durability-first DLT。
///
/// 参数说明: 无。
///
/// 返回：无；broker 最终持久结果由 transport 自身指标证明。
pub fn record_kafka_dlt_requested() {
    KAFKA_RESULT_DLT_REQUESTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Saga result 在本地提交后完成 ACK，并区分 Inbox 重复吸收。
///
/// 参数说明：`duplicate` 表示本次是否未重复执行业务效果。
///
/// 返回：无；ACK 与重复计数原子更新。
pub fn record_kafka_ack(duplicate: bool) {
    KAFKA_RESULT_ACK_TOTAL.fetch_add(1, Ordering::Relaxed);
    if duplicate {
        KAFKA_RESULT_DUPLICATE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// 业务作用：记录一次 Kafka Saga command handler 的单调耗时。
///
/// 参数说明：`elapsed` 是从 transport 门禁到参与方事务结束的单调耗时。
///
/// 返回：无；次数原子递增，耗时使用饱和累计。
pub fn record_kafka_command_processing(elapsed: Duration) {
    KAFKA_COMMAND_PROCESSING_TOTAL.fetch_add(1, Ordering::Relaxed);
    saturating_add_duration(&KAFKA_COMMAND_PROCESSING_MICROS, elapsed);
}

/// 业务作用：记录 Saga command 因参与方事务未提交而保留 offset 重投。
///
/// 参数说明: 无。
///
/// 返回：无；原子计数不阻塞 transport。
pub fn record_kafka_command_retry() {
    KAFKA_COMMAND_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Saga command 已请求进入 durability-first DLT。
///
/// 参数说明: 无。
///
/// 返回：无；broker 最终持久结果由 transport 自身指标证明。
pub fn record_kafka_command_dlt_requested() {
    KAFKA_COMMAND_DLT_REQUESTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Saga command 在参与方提交后完成 ACK，并区分 Inbox 重复吸收。
///
/// 参数说明：`duplicate` 表示本次是否未重复执行业务效果。
///
/// 返回：无；ACK 与重复计数原子更新。
pub fn record_kafka_command_ack(duplicate: bool) {
    KAFKA_COMMAND_ACK_TOTAL.fetch_add(1, Ordering::Relaxed);
    if duplicate {
        KAFKA_COMMAND_DUPLICATE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// 业务作用：把单次耗时以微秒饱和累加到进程计数器。
///
/// 参数说明：`target` 是目标原子计数，`elapsed` 是单调耗时。
///
/// 返回：无；溢出时保持 `u64::MAX`，不允许回绕成较小值。
fn saturating_add_duration(target: &AtomicU64, elapsed: Duration) {
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(micros))
    });
}
