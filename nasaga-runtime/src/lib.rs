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
//! 只提供封闭收据与服务端裁决器，listener、mTLS 身份、deadline 和 drain 仍由宿主显式拥有。
//! `TraceContext` 只作为已验证的显式输入传播，不读取 ambient 状态，也不是投递前置条件。
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

pub use envelope::{
    derive_result_event_id, SagaCommandEnvelope, SagaResultEnvelope, VerifiedIdentity,
    COMMAND_EVENT_TYPE, RESULT_EVENT_TYPE, SAGA_AGGREGATE_TYPE,
};
pub use management::{SagaAuditTrail, SagaManagementContext, SagaManagementPermission};
pub use observability::SagaOperationalMetrics;
pub use orchestrator::{
    HandleOutcome, Orchestrator, OrchestratorConfig, StartOutcome, StartSagaRequest,
    TenantActionRate, TimerOutcome,
};
pub use transport_auth::{
    render_saga_http_command_dlt_metric, SagaHttpMessageAuthError, SagaHttpMessageAuthFailure,
    SagaHttpMessageAuthenticator, SagaHttpReplayGuard, SagaHttpReplayMetricAggregate,
    SagaHttpReplayMetrics, SagaHttpReplayPlane, SagaHttpSignedMessage,
};

/// 业务作用：把 Participant command 处理错误映射为 transport 可执行的重试或隔离动作。
///
/// command 没有 Orchestrator `PAUSED` 延后语义；本地事务未提交只能有界重试，来源、身份或
/// 合同错误则立即进入 durability-first DLT，绝不能占用 Participant Inbox。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDeliveryDisposition {
    /// 本地事务未提交，保留 Kafka offset 做有预算重投。
    Retry,
    /// COMMIT/回滚结果需要持续重投收敛，保留 offset 且不消耗 DLT 预算。
    Defer,
    /// 消息违反不可恢复的来源、身份或步骤合同，应进入 DLT。
    DeadLetter,
}

// 枚举、线协议原因码、反向解析和脱敏文本必须从同一声明生成；新增错误类型时禁止接收端白名单漏改。
macro_rules! define_saga_command_processing_errors {
    (
        $(
            $(#[$variant_meta:meta])*
            $variant:ident => ($reason:literal, $display:literal)
        ),+ $(,)?
    ) => {
        /// 业务作用：用封闭类型标记 Saga command 的不可恢复投递错误，禁止 transport 解析错误文本。
        ///
        /// 未带本标记的数据库、事务与业务 `Retryable` 错误一律按瞬态错误处理；只有 transport
        /// 身份入口、宏生成的精确步骤门禁和 payload codec 可以构造这些稳定结论。
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum SagaCommandProcessingError {
            $(
                $(#[$variant_meta])*
                $variant,
            )+
        }

        /// 业务作用：公布 HTTP/Kafka connector 共同接受的全部稳定 command DLT 原因码。
        ///
        /// 接收端不得复制一份字符串 `match`；应调用
        /// [`SagaCommandProcessingError::from_dead_letter_reason`] 完成封闭解析。
        pub const SAGA_COMMAND_DEAD_LETTER_REASONS: &[&str] = &[$($reason),+];

        impl SagaCommandProcessingError {
            /// 业务作用：返回可安全写入 DLT header、HTTP 正文和低基数日志的稳定原因码。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：不包含 envelope、payload 或底层错误细节的原因码。
            pub const fn dead_letter_reason(self) -> &'static str {
                match self {
                    $(Self::$variant => $reason,)+
                }
            }

            /// 业务作用：把 connector 收到的原因码反向解析为封闭协议错误，拒绝未知文本控制 DLT。
            ///
            /// 参数说明：
            /// - `reason`: HTTP 正文或 DLT header 中未经信任的候选原因码。
            ///
            /// 返回：精确命中已发布白名单时返回对应错误类型；大小写、空白或未知值返回 `None`。
            pub fn from_dead_letter_reason(reason: &str) -> Option<Self> {
                match reason {
                    $($reason => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl std::fmt::Display for SagaCommandProcessingError {
            /// 业务作用：输出稳定、脱敏的 command 错误分类，供事务回滚链和运维日志识别。
            ///
            /// 参数说明：
            /// - `formatter`: 标准格式化输出目标。
            ///
            /// 返回：分类文本写入成功返回 `Ok`，底层格式化失败返回对应错误。
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(match self {
                    $(Self::$variant => $display,)+
                })
            }
        }

        impl std::error::Error for SagaCommandProcessingError {}
    };
}

define_saga_command_processing_errors! {
    /// transport 认证出的 producer 不在 Participant 冻结的 Orchestrator 信任表中。
    ProducerUnauthorized => (
        "saga_command_producer_unauthorized",
        "saga command producer is unauthorized"
    ),
    /// envelope 自报身份、派生身份或编码不合法。
    IdentityInvalid => (
        "saga_command_identity_invalid",
        "saga command identity is invalid"
    ),
    /// command topic 未获授权承载 envelope 指向的 workflow/version/step。
    RouteUnauthorized => (
        "saga_command_route_unauthorized",
        "saga command route is unauthorized"
    ),
    /// phase、取消能力或业务 payload 违反已发布步骤合同。
    ContractInvalid => (
        "saga_command_contract_invalid",
        "saga command contract is invalid"
    ),
}

/// 业务作用：限制同一 Kafka command 的瞬态重试次数，避免故障业务或数据库长期阻塞分区。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandDeliveryPolicy {
    /// 普通失败可重投的最大序号；下一次普通失败自动升级为 `DeadLetter`。
    pub max_retries: u32,
}

impl Default for CommandDeliveryPolicy {
    /// 业务作用：提供有界的默认命令重投预算，避免单条故障消息无限阻塞分区。
    ///
    /// 参数说明：无。
    ///
    /// 返回：最多允许十六次普通失败重投的策略。
    fn default() -> Self {
        Self { max_retries: 16 }
    }
}

impl CommandDeliveryPolicy {
    /// 业务作用：结合本次普通失败预算序号给出 command 最终投递动作。
    ///
    /// 参数说明：
    /// - `error`: Participant command 处理错误。
    /// - `retry_attempt`: `ConsumeCtx` 的普通失败预算序号，从 1 开始。
    ///
    /// 返回：类型化不可恢复错误立即隔离；普通错误在预算内重试，超预算
    /// 进入 DLT；COMMIT/回滚关键阶段始终 `Defer`。
    pub fn decide(self, error: &anyhow::Error, retry_attempt: u32) -> CommandDeliveryDisposition {
        let disposition = classify_command_delivery_error(error);
        if disposition == CommandDeliveryDisposition::Retry && retry_attempt > self.max_retries {
            CommandDeliveryDisposition::DeadLetter
        } else {
            disposition
        }
    }
}

/// 业务作用：为 Saga command hosted consumer 提供稳定的 Retry/DLT 分类。
///
/// 参数说明：
/// - `error`: transport 身份门禁或 Participant 事务返回的错误。
///
/// 返回：类型化来源、身份与合同错误进入 `DeadLetter`；不可 ACK 的事务阶段
/// 进入无 DLT 预算 `Defer`；任意其它错误保守进入有预算 `Retry`。
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

/// 业务作用：从完整错误链中提取可安全跨 transport 传播的 command DLT 原因码。
///
/// HTTP、Kafka 与其它 connector 必须复用这一个封闭分类，避免同一确定性协议错误在不同通道中
/// 分别退化成无限重试或依赖错误文本的脆弱判定。
///
/// 参数说明：
/// - `error`: Participant command 处理返回的完整错误链。
///
/// 返回：命中 [`SagaCommandProcessingError`] 时返回稳定、低基数原因码；事务或业务瞬态错误返回
/// `None`，由 transport 保留原消息并执行有界重试。
pub fn command_dead_letter_reason(error: &anyhow::Error) -> Option<&'static str> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<SagaCommandProcessingError>()
            .map(|error| error.dead_letter_reason())
    })
}

/// 业务作用：把 Orchestrator 结果处理错误映射为 transport 层可执行的投递动作。
///
/// 宿主 Kafka consumer 必须依据该分类决定延后、重试或进入 DLT；ACK 只由成功的
/// [`HandleOutcome`] 驱动，错误分支没有推断提交成功的权限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultDeliveryDisposition {
    /// 本地事务未提交，保留 Kafka offset 做有预算重投。
    Retry,
    /// 控制态暂不允许消费，保留 Kafka offset 且不消耗毒消息预算。
    Defer,
    /// 消息违反不可恢复的协议合同，应保留原文后进入 DLT。
    DeadLetter,
}

/// 业务作用：用封闭类型标记 Saga result 的可信投递分类，禁止 transport 解析错误文本
/// 决定 offset 是否前移。
///
/// 未带本标记的数据库、事务和未知错误一律按有预算瞬态错误处理；只有协议入口和暂停
/// 门禁可以构造这些稳定结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SagaResultProcessingError {
    /// 实例处于 `PAUSED`，恢复后必须继续处理同一结果，不能因等待时间进入 DLT。
    Paused,
    /// transport 认证出的 producer 无权为目标步骤作证。
    ProducerUnauthorized,
    /// envelope 自报身份、派生身份或编码不合法。
    IdentityInvalid,
    /// envelope 与固定实例/definition 的封闭合同不一致。
    ContractInvalid,
}

impl SagaResultProcessingError {
    /// 业务作用：返回可安全写入 DLT header 和低基数日志的稳定原因码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：暂停态没有 DLT 原因码；协议错误返回不含 envelope 或底层错误细节的原因码。
    pub fn dead_letter_reason(self) -> Option<&'static str> {
        match self {
            Self::Paused => None,
            Self::ProducerUnauthorized => Some("saga_result_producer_unauthorized"),
            Self::IdentityInvalid => Some("saga_result_identity_invalid"),
            Self::ContractInvalid => Some("saga_result_contract_invalid"),
        }
    }
}

impl std::fmt::Display for SagaResultProcessingError {
    /// 业务作用：输出稳定、脱敏的错误分类，供事务回滚链和运维日志识别。
    ///
    /// 参数说明：
    /// - `formatter`: 标准格式化输出目标。
    ///
    /// 返回：分类文本写入成功返回 `Ok`，底层格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Paused => "saga result processing deferred while instance is paused",
            Self::ProducerUnauthorized => "saga result producer is unauthorized",
            Self::IdentityInvalid => "saga result identity is invalid",
            Self::ContractInvalid => "saga result contract is invalid",
        })
    }
}

impl std::error::Error for SagaResultProcessingError {}

/// 业务作用：限制同一 Kafka 消息的瞬态重试次数，避免单条故障消息形成无限重试风暴。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultDeliveryPolicy {
    /// 普通失败可重投的最大序号；下一次普通失败自动升级为 `DeadLetter`。
    pub max_retries: u32,
}

impl Default for ResultDeliveryPolicy {
    /// 业务作用：提供有界的默认结果重投预算，同时保留提交不确定分支的持续收敛语义。
    ///
    /// 参数说明：无。
    ///
    /// 返回：最多允许十六次普通失败重投的策略。
    fn default() -> Self {
        Self { max_retries: 16 }
    }
}

impl ResultDeliveryPolicy {
    /// 业务作用：结合当前普通失败预算序号给出最终投递动作。
    ///
    /// 参数说明：
    /// - `error`: Saga 结果处理错误。
    /// - `retry_attempt`: `ConsumeCtx` 中排除 `HandlerDeferred` 的本次普通失败预算序号，从 1 开始。
    ///
    /// 返回：未超过预算时保留基础分类；超过预算的瞬态错误进入 DLT；`Defer` 不消耗
    /// 毒消息预算，确保长时间暂停不会改变业务投递语义。
    pub fn decide(self, error: &anyhow::Error, retry_attempt: u32) -> ResultDeliveryDisposition {
        let disposition = classify_result_delivery_error(error);
        if disposition == ResultDeliveryDisposition::Retry && retry_attempt > self.max_retries {
            ResultDeliveryDisposition::DeadLetter
        } else {
            disposition
        }
    }
}

/// 业务作用：为 Saga 结果消费宿主提供稳定的 ACK/重试/DLT 分类。
///
/// 参数说明：
/// - `error`: `handle_result` 返回的错误。
///
/// 返回：类型化协议错误进入 `DeadLetter`；暂停进入不受毒消息预算影响的 `Defer`；
/// 其余错误（包括任意文本相同的外部错误）都保守进入有预算 `Retry`。
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
    match classified {
        Some(SagaResultProcessingError::Paused) => ResultDeliveryDisposition::Defer,
        Some(
            SagaResultProcessingError::ProducerUnauthorized
            | SagaResultProcessingError::IdentityInvalid
            | SagaResultProcessingError::ContractInvalid,
        ) => ResultDeliveryDisposition::DeadLetter,
        None => ResultDeliveryDisposition::Retry,
    }
}

// 检索查询/摘要与 fencing 类型同为公开管理面输入输出,经本 crate 统一再导出。
pub use nasaga_mysql::{
    SagaInstanceQuery, SagaInstanceSummary, TimerFencingToken, TimerFencingTokenIssuer,
};
// 链路上下文的公开类型:发起入口与 transport 收据以它显式传递 trace,宏展开也经本
// 重导出引用,避免业务/宏直接依赖 natelemetry 坐标。
pub use natelemetry::TraceContext;
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
#[cfg(feature = "grpc-transport")]
pub use transport_grpc::{
    outbox_disposition_of, SagaGrpcCommandServer, SagaGrpcPeerIdentity, SagaGrpcReceipt,
    SagaGrpcResultServer,
};
#[cfg(feature = "redis-stream")]
pub use transport_redis::{
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
pub use transport_shared::{ParticipantCommandHandler, SagaCommandHandler, SagaResultHandler};

use nasaga_core::{CancelMode, Compensation, ResolutionMode};

/// 业务作用：`#[saga]` 宏为每个本地步骤生成的静态 descriptor。
///
/// descriptor 只描述**本地** handler 的合同投影，不能充当全局 workflow definition；
/// 启动预检把它与注册表中的 definition 对齐，不一致时拒绝 Ready。
#[derive(Debug, Clone, Copy)]
pub struct SagaStepDescriptor {
    /// workflow 名称。
    pub workflow: &'static str,
    /// definition 版本。
    pub definition_version: u32,
    /// 步骤名称。
    pub step: &'static str,
    /// 业务 Service 类型名，用于诊断与资源容器解析。
    pub service_type: &'static str,
    /// 是否可补偿；与 definition 的 `Compensation` 声明比对。
    pub compensation: Compensation,
    /// 取消形态；与 definition 的 `CancelMode` 声明比对。
    pub cancel_mode: CancelMode,
    /// 是否允许返回 `Unknown`；与 definition 的 `ResolutionSpec` 比对。
    pub allow_unknown: bool,
    /// 本地托管的解决能力；`Some(Poll)` 表示 descriptor 同时绑定类型化查询 handler。
    pub resolution_mode: Option<ResolutionMode>,
    /// 源码位置（file:line），用于重复/漂移诊断。
    pub source: &'static str,
}

/// 本 binary 内 `#[saga]` 收集到的全部本地 step descriptor。
///
/// `linkme` 只能收集当前 binary 的本地 handler，不能跨部署发现完整业务链——
/// 全局 definition 始终由 Orchestrator 注册表持有。
#[linkme::distributed_slice]
pub static COLLECTED_SAGA_STEPS: [SagaStepDescriptor];

/// 业务作用：校验本 binary 收集的 descriptor 集合自洽，并与注册表中的 definition 对齐。
///
/// 拒绝：同一 `(workflow, version, step)` 重复注册（两个 handler 抢同一步骤）；
/// descriptor 引用的 definition 已注册但步骤缺失或合同字段漂移（补偿能力、取消形态、
/// `allow_unknown` 不一致）。definition 未注册的 descriptor 允许存在——参与方 binary
/// 可能只托管步骤而不托管 Orchestrator，其对齐由受信 contract projection 分发承担。
///
/// 参数说明：
/// - `registry`: definition 注册表。
///
/// 返回：全部一致返回 `Ok`；任一冲突返回携带步骤定位的错误，调用方拒绝 Ready。
pub fn verify_descriptors(registry: &DefinitionRegistry) -> anyhow::Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for descriptor in COLLECTED_SAGA_STEPS {
        let key = (
            descriptor.workflow,
            descriptor.definition_version,
            descriptor.step,
        );
        // 同一 binary 内同一步骤只允许一个 handler:两个实现会对同一命令产生
        // 两份互相竞争的业务效果。
        if !seen.insert(key) {
            anyhow::bail!(
                "duplicate #[saga] step `{}` v{} `{}` (second registration at {})",
                descriptor.workflow,
                descriptor.definition_version,
                descriptor.step,
                descriptor.source
            );
        }
        let workflow =
            nasaga_core::WorkflowName::new(descriptor.workflow).map_err(|violation| {
                anyhow::anyhow!("bad descriptor workflow: {}", violation.code())
            })?;
        let version = nasaga_core::DefinitionVersion::new(descriptor.definition_version)
            .map_err(|violation| anyhow::anyhow!("bad descriptor version: {}", violation.code()))?;
        let Some(definition) = registry.get(&workflow, version) else {
            continue;
        };
        let step = nasaga_core::StepName::new(descriptor.step)
            .map_err(|violation| anyhow::anyhow!("bad descriptor step: {}", violation.code()))?;
        let Some(step_def) = definition.step(&step) else {
            anyhow::bail!(
                "descriptor step `{}` is absent from workflow `{}` v{} ({})",
                descriptor.step,
                descriptor.workflow,
                descriptor.definition_version,
                descriptor.source
            );
        };
        // descriptor 与 definition 漂移必须在 Ready 前失败,不能等运行期毒消息暴露。
        if step_def.compensation() != descriptor.compensation
            || step_def.cancel_mode() != descriptor.cancel_mode
            || step_def.resolution().allow_unknown() != descriptor.allow_unknown
            || step_def.resolution().mode() != descriptor.resolution_mode
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

/// 宏展开专用的内部再导出；业务代码不得直接使用。
pub mod __private {
    pub use anyhow;
    pub use linkme;
    pub use nasaga_core as core;
}
