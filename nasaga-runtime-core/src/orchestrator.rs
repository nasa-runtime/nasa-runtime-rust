//! Orchestrator：Saga 的创建、结果推进、durable timer 裁决与管理命令。
//!
//! 每次推进都是一个"单一本地事务"：Inbox claim、attempt journal
//! 记账、CAS + transition 审计行、下一条命令 Outbox 与 timer 的生死在同一 COMMIT 内
//! 生效或一起消失。CAS 输掉竞争时整个事务回滚且不 ACK，重投后基于新快照重新裁决——
//! 绝不用旧快照写 Outbox。
//!
//! 安全裁决的三条主线：
//! - **超时不是失败证据**：可取消步骤先进入 `CANCELLING` 建屏障；`resolve_only` 步骤
//!   直接进入 `WAITING_RESOLUTION`。两者都必须等待真实裁决，不能把 timeout 伪造成拒绝。
//! - **未知必须收敛**：`Unknown` 进入 `WAITING_RESOLUTION`，靠 resolve 命令/回调/人工
//!   得到唯一裁决；解决预算耗尽升级人工介入，绝不自动降级为 `Rejected`。
//! - **补偿计划冻结**：进入 `COMPENSATING` 前由已提交 journal 一次性冻结计划并落库，
//!   之后只幂等完成计划内步骤。

use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use nainbox_core::{InboxClaim, InboxStore};
use naoutbox_core::DurableOutboxAppend;
use nasaga_backend::{
    AttemptConflictFact, AttemptOutcomeRecord, CasOutcome, ControlCasOutcome,
    ControlTransitionSpec, ManagementAuditOutcome, NewSagaInstance, SagaAuditEventCursor,
    SagaAuditEventRecord, SagaAuditStore, SagaBackend, SagaBackendErrorKind, SagaBackendFactory,
    SagaConflictKind, SagaCreation, SagaGovernanceStore, SagaInstanceQuery, SagaInstanceRow,
    SagaInstanceStore, SagaInstanceSummary, SagaJournalStore, SagaStepRow, SagaTimerRow,
    SagaTimerStore, StepJournalPatch, TimerFencing, TimerFencingToken, TimerFencingTokenIssuer,
    TimerScope, TimerSpec, TransitionSpec,
};
use nasaga_core::{
    freeze_compensation_plan, AttemptNo, BusinessKey, CancelMode, CommandId, DefinitionVersion,
    Direction, EffectId, ResolutionMode, SagaId, SagaStatus, ServiceIdentity, StepAttemptStatus,
    StepCancelStatus, StepCompensationStatus, StepDefinition, StepForwardStatus, StepJournalEntry,
    StepName, StepPhase, StepResolutionStatus, TenantId, TimeoutPolicy, TriggerKind,
    WorkflowDefinition, WorkflowName,
};
use natelemetry::TraceContext;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::envelope::{canonical_bytes, SagaCommandEnvelope, SagaResultEnvelope, VerifiedIdentity};
use crate::management::{
    SagaAuditPage, SagaAuditPageCursor, SagaAuditRecord, SagaAuditTrail, SagaManagementContext,
    SagaManagementError, SagaManagementExpectation, SagaManagementPermission,
};
use crate::observability::{
    record_action_rate_rejection, record_quota_rejection, SagaOperationalMetrics,
};
use crate::registry::DefinitionRegistry;
use crate::timers::{
    derive_timer_id, parse_step_scope, phase_timeout_kind, resolution_budget_kind,
    KIND_CANCEL_TIMEOUT, KIND_COMPENSATE_TIMEOUT, KIND_COMPENSATION_RESOLUTION_BUDGET,
    KIND_FORWARD_RESOLUTION_BUDGET, KIND_INSTANCE_DEADLINE, KIND_RESOLUTION_BUDGET,
    KIND_RESOLVE_TIMEOUT, KIND_STEP_TIMEOUT,
};

/// 派生补偿立即收敛 trigger 的固定命名空间（ASCII `nasasaga-v1-cvns`）。
///
/// 使用定长 UUID 而不是在外部 cause id 后追加文本，避免合法的 190 字节 trigger
/// 在 `#converge` 后缀处越过数据库索引上限并永久卡在 `COMPENSATING`。
const CONVERGE_TRIGGER_NAMESPACE: Uuid = Uuid::from_bytes(*b"nasasaga-v1-cvns");

/// 业务作用：Orchestrator 的运行参数；全部预算有界，自动状态不允许无限滞留。
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Orchestrator result Inbox 的小写 canonical consumer 名称。
    pub inbox_consumer: String,
    /// 取消命令的最大 attempt 数；耗尽升级人工介入。
    pub cancel_max_attempts: u32,
    /// 补偿命令的最大 attempt 数；耗尽升级人工介入，不伪造 `COMPENSATED`。
    pub compensate_max_attempts: u32,
    /// 解决查询的最大 attempt 数；耗尽升级人工介入，不自动转 `Rejected`。
    pub resolve_max_attempts: u32,
    /// 单轮 timer 领取上限。
    pub timer_claim_limit: u32,
    /// timer 租约时长（毫秒）。
    pub timer_lease_ms: i64,
    /// 实例暂停时 timer 的轮询退避（毫秒）；只是轮询节奏，不顺延业务 deadline。
    pub pause_backoff_ms: i64,
    /// 启动预检扫描非终态实例的上限。
    pub startup_scan_limit: u32,
    /// 每租户在飞实例上限（冻结表）。
    ///
    /// 未列出的租户不设限但仍精确记账（"先观测,不拒绝"档);列出的租户在创建事务内
    /// 原子预留,超限以稳定原因码拒绝新建、不影响已在飞实例收敛。上限只作用于创建,
    /// 配额拒绝与系统故障可区分,且不泄漏其它租户用量。
    pub tenant_quotas: std::collections::BTreeMap<String, u64>,
    /// 每租户变更类管理动作的速率上限（冻结表）。
    ///
    /// 覆盖 pause/resume/retry-compensation/retry-resolution/manual-close 五个会向恢复
    /// 通道注入外部副作用的动作；只读动作（检索、审计、用量查询）不占预算。未列出的
    /// 租户不限速且不写账本；`max_actions = 0` 表示完全封禁该租户的变更类管理动作。
    /// 预算与动作事务同提交:动作失败回滚时预算退还,拒绝携带稳定原因码并与系统
    /// 故障可区分,不泄漏其它租户用量。
    pub tenant_action_rates: std::collections::BTreeMap<String, TenantActionRate>,
    /// 人工关闭（`MANUALLY_CLOSED`）管理动作的受信能力开关。
    ///
    /// 滚动升级门禁：新终态一旦落库就不可回退，必须先让**全部**副本升级为能解析
    /// `MANUALLY_CLOSED` 的读者，再由部署方打开本开关放行管理动作。默认关闭——
    /// 未显式开启的部署是"可读不产生"的兼容读者。
    pub enable_manual_close: bool,
}

impl Default for OrchestratorConfig {
    /// 业务作用：给出保守默认预算，保证未显式配置的部署也有有界收敛行为。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认配置。
    fn default() -> Self {
        Self {
            inbox_consumer: "nasaga-orchestrator".to_string(),
            cancel_max_attempts: 5,
            compensate_max_attempts: 5,
            resolve_max_attempts: 10,
            timer_claim_limit: 32,
            timer_lease_ms: 60_000,
            pause_backoff_ms: 30_000,
            startup_scan_limit: 1_000,
            tenant_quotas: std::collections::BTreeMap::new(),
            tenant_action_rates: std::collections::BTreeMap::new(),
            // 默认保持"可读不产生":人工关闭动作须在全部副本可解析新终态后显式开启。
            enable_manual_close: false,
        }
    }
}

/// 业务作用：单租户变更类管理动作的固定窗口速率上限。
///
/// 窗口边界由**数据库时钟**对齐,多副本 Orchestrator 共享同一套窗口;预算只按首次
/// 提交的新 operation 计，完全相同的已提交 operation 重放不重复计数，失败回滚不消耗。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantActionRate {
    /// 单窗口允许提交的动作数;0 表示完全封禁。
    pub max_actions: u64,
    /// 窗口长度（毫秒）,必须为正;非法值在动作路径上报配置错误而不是稳定拒绝。
    pub window_ms: i64,
}

/// 业务作用：区分创建入口的两种结果；`AlreadyExists` 表示业务幂等键命中，无新副作用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartOutcome {
    /// 实例已创建，首步命令、timer 与初始 transition 已同事务提交。
    Started(SagaInstanceRow),
    /// 同一业务意图已有实例；返回其当前快照。
    AlreadyExists(SagaInstanceRow),
}

/// 业务作用：封闭 Saga 创建的确定性拒绝原因，供协议适配器按类型映射稳定状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartSagaError {
    /// 指定 workflow/version 当前不允许创建新实例。
    DefinitionInactive,
    /// 正文格式、schema 或双重输入来源不符合冻结步骤合同。
    InvalidPayload,
    /// 已发布 definition 没有可执行步骤。
    DefinitionEmpty,
    /// Saga 身份或业务幂等槽已被不同启动请求占用。
    RequestConflict,
    /// 租户在飞实例配额已耗尽。
    TenantQuotaExceeded,
}

impl StartSagaError {
    /// 业务作用：从完整错误链中提取协议层可以安全公开的 Saga 创建拒绝类别。
    ///
    /// 参数说明：`error` 是运行核心返回且可能附加调用上下文的完整错误链。
    ///
    /// 返回：包含封闭创建拒绝时返回对应类别；基础设施或未知失败返回 `None`。
    pub fn from_error(error: &anyhow::Error) -> Option<Self> {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<Self>().copied())
    }
}

impl std::fmt::Display for StartSagaError {
    /// 业务作用：输出不含租户用量、请求正文或持久化细节的稳定创建拒绝摘要。
    ///
    /// 参数说明：`formatter` 是标准格式化输出目标。
    ///
    /// 返回：摘要写入成功时返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPayload => "Saga payload contract is invalid",
            Self::DefinitionInactive => "Saga workflow definition is not active",
            Self::DefinitionEmpty => "Saga workflow definition has no steps",
            Self::RequestConflict => "Saga identity is already bound to a different start request",
            Self::TenantQuotaExceeded => "saga_tenant_quota_exceeded",
        })
    }
}

impl std::error::Error for StartSagaError {}

/// 业务作用：保留状态迁移 CAS 未生效的确定性并发裁决，禁止协议层解析错误文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SagaConcurrencyError {
    /// 调用方使用的实例状态或版本快照已经过期。
    StaleSnapshot,
    /// 相同触发身份已经提交过另一条迁移。
    DuplicateTrigger,
}

impl SagaConcurrencyError {
    /// 业务作用：从事务与调用上下文包裹的错误链中提取 CAS 并发裁决。
    ///
    /// 参数说明：`error` 是状态迁移调用返回的完整错误链。
    ///
    /// 返回：包含确定性并发裁决时返回对应类别；其它失败返回 `None`。
    pub fn from_error(error: &anyhow::Error) -> Option<Self> {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<Self>().copied())
    }
}

impl std::fmt::Display for SagaConcurrencyError {
    /// 业务作用：输出不携带实例身份与业务数据的稳定 CAS 裁决摘要。
    ///
    /// 参数说明：`formatter` 是标准格式化输出目标。
    ///
    /// 返回：摘要写入成功时返回 `Ok`；格式化失败返回对应错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::StaleSnapshot => "Saga state snapshot is stale",
            Self::DuplicateTrigger => "Saga transition trigger was already applied",
        })
    }
}

impl std::error::Error for SagaConcurrencyError {}

/// 业务作用：区分结果处理的两种可 ACK 结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOutcome {
    /// 已推进（或迟到结果已记账）；`status` 为提交后的实例状态。
    Applied {
        /// 事务提交后的实例业务状态。
        status: SagaStatus,
    },
    /// Inbox 命中重复结果事件；无新副作用，直接 ACK。
    Duplicate,
}

/// 业务作用：区分单个 timer 的处理结论，驱动领取循环决定交还还是继续。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerOutcome {
    /// timer 已消费并完成对应裁决。
    Applied {
        /// 事务提交后的实例业务状态。
        status: SagaStatus,
    },
    /// 实例处于 `PAUSED`：不消费，由调用方按轮询退避交还。
    SkippedPaused,
    /// fencing 失败：租约已被其它副本接管，旧 owner 立即停止。
    SkippedFencingLost,
    /// timer 语义已过时（状态/步骤已迁移），已按已消费吸收，无业务动作。
    SkippedStale,
}

/// 业务作用：描述一次 Saga 创建请求。
#[derive(Debug, Clone)]
pub struct StartSagaRequest<'a> {
    /// 实例身份，由调用方生成（如 UUID 文本）。
    pub saga_id: &'a SagaId,
    /// 规范化租户；无租户部署传固定 system tenant。
    pub tenant: &'a TenantId,
    /// workflow 名称，必须已在注册表注册。
    pub workflow: &'a WorkflowName,
    /// definition 版本。
    pub version: DefinitionVersion,
    /// 业务幂等键。
    pub business_key: &'a BusinessKey,
    /// 实例级业务 deadline（epoch 毫秒）；为空则不设全局期限。
    pub deadline_at_ms: Option<i64>,
    /// 初始 transition 的触发来源。
    pub trigger_kind: TriggerKind,
    /// 初始 transition 的触发身份（event id 或管理 operation id）。
    pub trigger_id: &'a str,
    /// 首步 execute 命令携带的业务输入；后续步骤由参与方本地事实解析。
    pub first_command_payload: Option<serde_json::Value>,
    /// 原始首步正文；与兼容 JSON 输入互斥，媒体类型与 schema 纳入请求摘要。
    pub first_command_raw_payload: Option<nasaga_core::SagaPayload>,
    /// 当前时刻（epoch 毫秒），由调用方注入统一时钟。
    pub now_ms: i64,
}

/// 业务作用：保留调用方选择的首步正文合同，兼容 JSON envelope 与原始字节 envelope 不相互转换。
enum CommandInput {
    Json(serde_json::Value),
    Raw(nasaga_core::SagaPayload),
}

/// 业务作用：Saga Orchestrator——所有推进都以数据库 CAS 与单一本地事务落库。
///
/// 无进程内可变业务状态（仅 fencing token 发行器）：多副本并发安全完全由 store 的
/// CAS、唯一键与 fencing token 承担，任何副本崩溃后另一副本可无缝接管。
pub struct Orchestrator<B: SagaBackend> {
    backend: B,
    registry: RwLock<Arc<DefinitionRegistry>>,
    config: OrchestratorConfig,
    /// 每个 runtime 实例独立的 fencing token 发行域；不承载业务状态或租约归属。
    fencing_tokens: TimerFencingTokenIssuer,
}

/// 业务作用：一次结果推进的裁决上下文，聚合已证身份与已提交快照。
struct ResultContext<'a> {
    instance: &'a SagaInstanceRow,
    definition: &'a WorkflowDefinition,
    step_def: &'a StepDefinition,
    identity: &'a VerifiedIdentity,
    status: StepAttemptStatus,
    terminal: Option<StepForwardStatus>,
    reason_code: Option<&'a str>,
    event_id: &'a str,
    now_ms: i64,
}

/// 业务作用：在 Orchestrator 获得任何自动推进能力前校验 Inbox 命名空间与全部预算。
///
/// 参数说明：
/// - `config`: 待装载的运行参数。
///
/// 返回：consumer 满足小写 canonical Inbox key 且预算有界时返回 `Ok`；否则拒绝构造。
fn validate_config(config: &OrchestratorConfig) -> anyhow::Result<()> {
    if config.inbox_consumer.is_empty()
        || config.inbox_consumer.trim() != config.inbox_consumer
        || config.inbox_consumer.len() > 128
        || config.inbox_consumer.contains('\0')
        || !config.inbox_consumer.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
        || config.cancel_max_attempts == 0
        || config.compensate_max_attempts == 0
        || config.resolve_max_attempts == 0
        || config.timer_claim_limit == 0
        || config.timer_lease_ms <= 0
        || config.pause_backoff_ms <= 0
        || config.startup_scan_limit == 0
    {
        anyhow::bail!("orchestrator config violates the Inbox key contract or positive budgets");
    }
    // 冻结表在构造期整体校验:非法租户键会静默成为永不匹配的配置,窗口非法则要到
    // 首次管理动作才以系统错误暴露——不合理值必须在 Ready 之前失败。
    for tenant in config.tenant_quotas.keys() {
        if TenantId::new(tenant.as_str()).is_err() {
            anyhow::bail!("tenant quota key is not a valid tenant id");
        }
    }
    for (tenant, rate) in &config.tenant_action_rates {
        if TenantId::new(tenant.as_str()).is_err() {
            anyhow::bail!("tenant action rate key is not a valid tenant id");
        }
        if rate.window_ms <= 0 {
            anyhow::bail!("tenant action rate window_ms must be positive");
        }
    }
    Ok(())
}

/// 业务作用：把 definition 中的 Duration 安全转换成数据库 epoch 毫秒增量。
///
/// 参数说明：
/// - `duration`: 步骤超时或解决预算。
///
/// 返回：可由 i64 表示且大于零的毫秒数；截断为零或溢出时返回错误并停止自动调度。
fn duration_millis(duration: Duration) -> anyhow::Result<i64> {
    let millis = i64::try_from(duration.as_millis())
        .map_err(|_| anyhow::anyhow!("saga duration exceeds the supported millisecond range"))?;
    if millis <= 0 {
        anyhow::bail!("saga duration must be at least one millisecond");
    }
    Ok(millis)
}

/// 业务作用：以 checked arithmetic 计算 timer 到期时刻，防止溢出后立即或负时间触发。
///
/// 参数说明：
/// - `now_ms`: 当前 epoch 毫秒。
/// - `delay_ms`: 正数延迟毫秒。
///
/// 返回：安全的到期时刻；延迟非正或加法溢出时返回错误并回滚当前推进事务。
fn checked_deadline(now_ms: i64, delay_ms: i64) -> anyhow::Result<i64> {
    if delay_ms <= 0 {
        anyhow::bail!("saga timer delay must be positive");
    }
    now_ms
        .checked_add(delay_ms)
        .ok_or_else(|| anyhow::anyhow!("saga timer deadline overflow"))
}

/// 业务作用：为管理状态门禁保留脱敏诊断，同时让协议层从错误链按类型分类。
///
/// 参数说明：`message` 是不含实例身份与业务数据的稳定前置条件摘要。
///
/// 返回：外层显示具体门禁、内层保留 [`SagaManagementError::PreconditionFailed`] 的错误。
fn management_precondition(message: &'static str) -> anyhow::Error {
    anyhow::Error::new(SagaManagementError::PreconditionFailed).context(message)
}

/// 业务作用：把 JSON 值按类型、数组顺序和排序后的对象键写入摘要，消除对象插入顺序差异。
///
/// 参数说明：
/// - `hasher`: 当前启动请求摘要状态。
/// - `value`: 首步命令 payload 的一个 JSON 节点。
///
/// 返回：无；节点的类型边界和内容被追加到摘要状态。
fn hash_canonical_json(hasher: &mut Sha256, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => hasher.update(canonical_bytes(&[b"null"])),
        serde_json::Value::Bool(value) => {
            hasher.update(canonical_bytes(&[b"bool", &[u8::from(*value)]]));
        }
        serde_json::Value::Number(value) => {
            hasher.update(canonical_bytes(&[b"number", value.to_string().as_bytes()]));
        }
        serde_json::Value::String(value) => {
            hasher.update(canonical_bytes(&[b"string", value.as_bytes()]));
        }
        serde_json::Value::Array(values) => {
            hasher.update(canonical_bytes(&[b"array", &values.len().to_be_bytes()]));
            for value in values {
                hash_canonical_json(hasher, value);
            }
        }
        serde_json::Value::Object(values) => {
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_unstable();
            hasher.update(canonical_bytes(&[b"object", &keys.len().to_be_bytes()]));
            for key in keys {
                hasher.update(canonical_bytes(&[b"key", key.as_bytes()]));
                hash_canonical_json(hasher, &values[key]);
            }
        }
    }
}

/// 业务作用：为 business-key 幂等入口生成不含敏感明文的 canonical 请求摘要。
///
/// 摘要覆盖会改变 Saga 合同或首个业务效果的全部输入；不覆盖 `saga_id`、transport
/// trigger 与 `now_ms`，因此同一业务请求的传输重试可使用新的请求身份而仍命中幂等结果。
///
/// 参数说明：
/// - `request`: 调用方提交的启动请求。
/// - `definition_digest`: 已注册 definition 的固定内容摘要。
/// - `first_step`: definition 的首步身份。
///
/// 返回：六十四位小写十六进制 SHA-256 摘要。
fn derive_legacy_start_request_digest(
    request: &StartSagaRequest<'_>,
    definition_digest: &str,
    first_step: &StepName,
) -> String {
    let version = request.version.get().to_be_bytes();
    let deadline = request.deadline_at_ms.unwrap_or_default().to_be_bytes();
    let deadline_present = [u8::from(request.deadline_at_ms.is_some())];
    let payload_present = [u8::from(request.first_command_payload.is_some())];
    let mut hasher = Sha256::new();
    hasher.update(canonical_bytes(&[
        request.tenant.as_str().as_bytes(),
        request.workflow.as_str().as_bytes(),
        &version,
        definition_digest.as_bytes(),
        request.business_key.as_str().as_bytes(),
        first_step.as_str().as_bytes(),
        &deadline_present,
        &deadline,
        &payload_present,
    ]));
    if let Some(payload) = request.first_command_payload.as_ref() {
        hash_canonical_json(&mut hasher, payload);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用：以规范领域字段和原始正文构造跨协议相同的创建摘要。
///
/// 参数说明：request 固定业务身份，definition_digest 与 first_step 固定执行合同，payload 保留原始字节。
///
/// 返回：有正文时把媒体、schema 和正文纳入独立摘要域；无正文沿用既有空输入摘要。
fn derive_start_request_digest(
    request: &StartSagaRequest<'_>,
    definition_digest: &str,
    first_step: &StepName,
    payload: Option<&nasaga_core::SagaPayload>,
) -> String {
    let mut empty = request.clone();
    empty.first_command_payload = None;
    empty.first_command_raw_payload = None;
    let domain = derive_legacy_start_request_digest(&empty, definition_digest, first_step);
    let Some(payload) = payload else {
        return domain;
    };
    let mut hasher = Sha256::new();
    hasher.update(canonical_bytes(&[
        b"saga-start-payload",
        domain.as_bytes(),
        payload.content_type.as_bytes(),
        payload.schema_id.as_bytes(),
        &payload.body,
    ]));
    hex::encode(hasher.finalize())
}

/// 业务作用：从任意有界 cause id 派生定长补偿收敛 trigger，保持同一原因重试身份稳定。
///
/// 参数说明：
/// - `cause_id`: 引发进入补偿或完成最后一项补偿的触发身份。
///
/// 返回：确定性的 UUIDv5 文本，不会超过 store 的 trigger_id 长度上限。
fn derive_converge_trigger_id(cause_id: &str) -> String {
    Uuid::new_v5(
        &CONVERGE_TRIGGER_NAMESPACE,
        &canonical_bytes(&[cause_id.as_bytes(), b"converge"]),
    )
    .to_string()
}

/// 业务作用：把 timer 领取后的单调流逝时间叠加到注入的 epoch 时钟，供租约复验使用。
///
/// 使用 `Instant` 而不是再次读取墙上时钟，可避免 NTP 回拨让已经消耗的租约时间倒退；
/// 注入的 `base_now_ms` 同时承担确定裁决时刻与统一业务时钟的语义。
///
/// 参数说明：
/// - `base_now_ms`: 领取开始时注入的 epoch 毫秒。
/// - `claimed_at`: 领取开始前记录的单调时刻。
///
/// 返回：叠加后的当前 epoch 毫秒；流逝时间或加法超出 i64 时返回错误并停止使用租约。
fn elapsed_epoch_ms(base_now_ms: i64, claimed_at: Instant) -> anyhow::Result<i64> {
    let elapsed_ms = i64::try_from(claimed_at.elapsed().as_millis())
        .map_err(|_| anyhow::anyhow!("timer lease elapsed duration overflow"))?;
    base_now_ms
        .checked_add(elapsed_ms)
        .ok_or_else(|| anyhow::anyhow!("timer lease clock overflow"))
}

impl<B> Orchestrator<B>
where
    B: SagaBackend + SagaBackendFactory,
{
    /// 业务作用：构造 Orchestrator。
    ///
    /// 参数说明：
    /// - `registry`: 启动阶段构建完成的 definition 注册表（运行期只读）。
    /// - `config`: 运行参数。
    ///
    /// 返回：配置预算全部为正时返回 Orchestrator；零/负预算返回错误并拒绝启动。
    pub fn new(registry: DefinitionRegistry, config: OrchestratorConfig) -> anyhow::Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            backend: B::default_backend()?,
            registry: RwLock::new(Arc::new(registry)),
            config,
            fencing_tokens: TimerFencingTokenIssuer::new(),
        })
    }

    /// 业务作用：构造绑定命名 datasource 的 Orchestrator，使状态、Inbox、Outbox、timer 与审计共享一次本地提交。
    ///
    /// 参数说明：
    /// - `registry`: 启动期已冻结的 definition 注册表。
    /// - `config`: Orchestrator 运行预算与租户门禁。
    /// - `datasource`: 所有 Saga 本地持久与事务边界共用的数据源。
    ///
    /// 返回：配置与 datasource 合法时返回同源 Orchestrator；否则在任何数据库 I/O 前拒绝。
    pub fn with_datasource(
        registry: DefinitionRegistry,
        config: OrchestratorConfig,
        datasource: impl AsRef<str>,
    ) -> anyhow::Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            backend: B::backend_for(datasource.as_ref())?,
            registry: RwLock::new(Arc::new(registry)),
            config,
            fencing_tokens: TimerFencingTokenIssuer::new(),
        })
    }

    /// 业务作用：读取 Orchestrator 全部本地持久与事务链绑定的 datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：store、Inbox、Outbox 与 timer 共用的不可变 qualifier。
    pub fn datasource_ref(&self) -> &natx_core::DatasourceRef {
        self.backend.datasource_ref()
    }

    /// 业务作用：取得当前完整 registry generation 的共享快照，使一次状态机操作不会跨代读取定义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前不可变快照；锁中毒时仍读取最后一次完整发布值。
    fn registry_snapshot(&self) -> Arc<DefinitionRegistry> {
        self.registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// 业务作用：原子发布已经完成 Catalog、历史实例与 capability 门禁的新 registry generation。
    ///
    /// 参数说明：`registry` 同时包含 active definition 和旧实例仍需的 deprecated definition。
    ///
    /// 返回：新快照成为后续操作唯一入口后完成；进行中的操作继续持有旧快照直到事务结束。
    pub fn replace_registry(&self, registry: DefinitionRegistry) {
        *self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(registry);
    }

    /// 业务作用：在动态 Catalog 快照发布前确认全部非终态实例仍能找到相同摘要的 definition。
    ///
    /// 参数说明：`registry` 是 watcher 刚从共享 generation 完整装载的候选快照。
    ///
    /// 返回：所有在途实例均可安全继续时成功；缺失或摘要漂移时拒绝切换。
    pub async fn verify_registry_snapshot(
        &self,
        registry: &DefinitionRegistry,
    ) -> anyhow::Result<()> {
        registry
            .verify_non_terminal(self.backend.store(), self.config.startup_scan_limit)
            .await
    }

    /// 业务作用：启动预检——非终态实例引用的 definition 必须可用且摘要一致，否则拒绝 Ready。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预检通过返回 `Ok`；缺失或摘要漂移返回错误，调用方不得宣告 Ready。
    pub async fn verify_startup(&self) -> anyhow::Result<()> {
        // 本进程收集的步骤合同必须先于历史实例扫描校验；否则同一 binary 内重复 handler 或
        // 合同漂移可能在数据库事实通过后才随首条命令暴露，形成已 Ready 但不可安全推进的窗口。
        let registry = self.registry_snapshot();
        crate::verify_descriptors(&registry)?;
        // 设限租户的配额账本必须已完成初始化回填:存量非终态实例未入账时,其终态释放
        // 会扣掉新实例的名额、上限被静默穿透。回填只能在全部写入方都运行记账版本后
        // 经 reconcile 的受锁窗口执行——这里 fail-fast,不把部署错误拖到首次预留。
        for tenant in self.config.tenant_quotas.keys() {
            let tenant = TenantId::new(tenant.as_str())
                .map_err(|_| anyhow::anyhow!("tenant quota key is not a valid tenant id"))?;
            if !self
                .backend
                .store()
                .tenant_quota_initialized(&tenant)
                .await?
            {
                anyhow::bail!(
                    "tenant quota ledger for a capped tenant is not initialized; \
                     run reconcile_tenant_quota after every writer runs the accounting binary"
                );
            }
        }
        registry
            .verify_non_terminal(self.backend.store(), self.config.startup_scan_limit)
            .await
    }

    /// 业务作用：读取当前 registry 快照中指定 definition 的 canonical 摘要，供协议兼容门禁在创建前校验。
    ///
    /// 参数说明：`workflow` 与 `version` 共同定位不可变流程版本。
    ///
    /// 返回：当前快照已注册时返回摘要；未知或未激活版本返回空。
    pub fn definition_digest(
        &self,
        workflow: &WorkflowName,
        version: DefinitionVersion,
    ) -> Option<String> {
        self.registry_snapshot()
            .get(workflow, version)
            .map(WorkflowDefinition::digest)
    }

    /// 业务作用：按租户读取当前可用于运行期复验的 definition 摘要。
    ///
    /// 参数说明：`tenant`、`workflow` 与 `version` 共同定位租户专属流程版本。
    ///
    /// 返回：active、deprecated 或静态全局定义存在时返回摘要；未知版本为空。
    pub fn definition_digest_for_tenant(
        &self,
        tenant: &TenantId,
        workflow: &WorkflowName,
        version: DefinitionVersion,
    ) -> Option<String> {
        self.registry_snapshot()
            .get_for_tenant(tenant, workflow, version)
            .map(WorkflowDefinition::digest)
    }

    /// 业务作用：为管理面读取指定租户拥有的 Saga 快照，阻止仅凭 saga_id 跨租户枚举。
    ///
    /// 参数说明：
    /// - `tenant`: 已认证管理主体被授权访问的租户。
    /// - `saga_id`: 实例身份。
    ///
    /// 返回：实例存在且属于该租户时返回快照；不存在或属于其他租户均返回
    /// `None`，防止 saga_id 成为跨租户存在性 oracle。
    pub async fn load_instance(
        &self,
        tenant: &TenantId,
        saga_id: &SagaId,
    ) -> anyhow::Result<Option<SagaInstanceRow>> {
        Ok(self
            .backend
            .store()
            .load_instance(saga_id)
            .await?
            .filter(|instance| &instance.tenant == tenant))
    }

    /// 业务作用：按租户和 `saga.audit.read` 权限读取一份有界、可恢复的完整审计视图。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体与权限快照；reason 用于宿主管理访问日志。
    /// - `tenant`: 主体获授权访问的租户。
    /// - `saga_id`: 目标实例。
    /// - `after_transition_seq`: 业务迁移分页游标。
    /// - `after_control_seq`: 控制迁移分页游标。
    /// - `limit`: 每类事实最大返回数，范围 1..=1000。
    ///
    /// 返回：实例存在且属于租户时返回 attempt/transition/control/management/conflict
    /// 聚合视图；无权限、跨租户、上限非法或持久层失败返回错误。
    pub async fn load_audit_trail(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        after_transition_seq: u64,
        after_control_seq: u64,
        limit: u32,
    ) -> anyhow::Result<SagaAuditTrail> {
        // 审计包含 actor、触发身份与业务效果 id，必须先鉴权再检查实例是否存在。
        management.require(SagaManagementPermission::ReadAudit)?;
        self.backend
            .store()
            .load_instance(saga_id)
            .await?
            .filter(|instance| &instance.tenant == tenant)
            .ok_or(SagaManagementError::NotFound)?;
        Ok(SagaAuditTrail {
            attempts: self
                .backend
                .store()
                .load_attempt_audit(saga_id, None, limit)
                .await?
                .into_iter()
                .map(|row| row.attempt)
                .collect(),
            transitions: self
                .backend
                .store()
                .load_transition_audit(saga_id, after_transition_seq, limit)
                .await?,
            controls: self
                .backend
                .store()
                .load_control_audit(saga_id, after_control_seq, limit)
                .await?,
            management_operations: self
                .backend
                .store()
                .load_management_audit(saga_id, None, limit)
                .await?,
            conflicts: self
                .backend
                .store()
                .load_conflict_audit(saga_id, None, limit)
                .await?,
        })
    }

    /// 业务作用：按数据库全局序号读取不可变审计事件，使运行中新增事实和 attempt 终态变化
    /// 都能出现在既有游标之后。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体与审计读取权限。
    /// - `tenant`: 主体获授权访问的租户。
    /// - `saga_id`: 目标实例。
    /// - `cursor`: 上一页末项的全局序号；`None` 表示从事件流起点开始。
    /// - `limit`: 单页最大事实数，范围 1..=1000。
    ///
    /// 返回：按 `audit_seq` 返回至多 `limit` 条事实；非空页同时返回末项 checkpoint，
    /// 供历史遍历或后续追加事实续读。越权、跨租户或持久化失败返回错误。
    pub async fn load_audit_page(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        cursor: Option<&SagaAuditPageCursor>,
        limit: u32,
    ) -> anyhow::Result<SagaAuditPage> {
        if limit == 0 || limit > 1_000 {
            return Err(SagaManagementError::InvalidContext.into());
        }
        // 审计事实含业务身份和人工主体，必须在解析游标和查询目标之前完成权限门禁。
        management.require(SagaManagementPermission::ReadAudit)?;
        self.backend
            .store()
            .load_instance(saga_id)
            .await?
            .filter(|instance| &instance.tenant == tenant)
            .ok_or(SagaManagementError::NotFound)?;
        let after = SagaAuditEventCursor {
            audit_seq: cursor.map_or(0, |cursor| cursor.audit_seq),
        };
        let events = self
            .backend
            .store()
            .load_audit_events(saga_id, after, limit)
            .await?;
        // 非满页也必须签发末项 checkpoint；运行中 Saga 可在读取瞬间的尾部之后继续
        // 追加事实，调用方据此断线续读，不必重新遍历或依赖类别内排序。
        let next_cursor = events.last().map(|event| SagaAuditPageCursor {
            audit_seq: event.audit_seq,
        });
        let records = events
            .into_iter()
            .map(|event| match event.record {
                SagaAuditEventRecord::Attempt(row) => SagaAuditRecord::Attempt(row),
                SagaAuditEventRecord::Transition(row) => SagaAuditRecord::Transition(row),
                SagaAuditEventRecord::Control(row) => SagaAuditRecord::Control(row),
                SagaAuditEventRecord::Management(row) => SagaAuditRecord::Management(row),
                SagaAuditEventRecord::Conflict(row) => SagaAuditRecord::Conflict(row),
            })
            .collect();
        Ok(SagaAuditPage {
            records,
            next_cursor,
        })
    }

    /// 业务作用：租户受限的实例只读检索——按状态与创建时间窗过滤、keyset 分页，让运维
    /// 无需直连数据库即可定位待处置对象（如全部 `MANUAL_INTERVENTION` 实例）。
    ///
    /// 检索是纯读动作，使用与写动作分离的 `saga.instance.list` 权限；响应只含身份、
    /// 状态、当前步骤、版本、稳定原因码与时间戳，不携带业务 payload。租户过滤由查询
    /// 条件强制，不同租户实例的存在性不经本入口泄漏。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体与权限快照。
    /// - `query`: 租户、状态集合、时间窗、cursor 与页大小。
    ///
    /// 返回：无时间条件时按 saga_id 升序；有时间条件时按创建时刻、saga_id 升序；无权限、
    /// 参数非法或持久层失败返回错误。
    pub async fn list_instances(
        &self,
        management: &SagaManagementContext,
        query: &SagaInstanceQuery<'_>,
    ) -> anyhow::Result<Vec<SagaInstanceSummary>> {
        // 检索响应含实例身份与状态,必须先鉴权;权限独立于任何写动作。
        management.require(SagaManagementPermission::ListInstances)?;
        Ok(self.backend.store().list_instances(query).await?)
    }

    /// 业务作用：读取单个租户的精确在飞配额用量——只经受鉴权管理查询返回,不进指标标签。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体与权限快照（要求 `saga.audit.read`）。
    /// - `tenant`: 目标租户。
    ///
    /// 返回：账本记录的在飞实例数;无权限返回错误。
    pub async fn tenant_quota_usage(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
    ) -> anyhow::Result<u64> {
        management.require(SagaManagementPermission::ReadAudit)?;
        Ok(self.backend.store().tenant_quota_usage(tenant).await?)
    }

    /// 业务作用：读取单个租户当前窗口的管理动作用量——只经受鉴权管理查询返回,不进指标标签。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体与权限快照（要求 `saga.audit.read`）。
    /// - `tenant`: 目标租户。
    ///
    /// 返回：`Some((窗口起点毫秒, 已用动作数))`;该租户未配置速率时返回 `None`。
    pub async fn tenant_action_rate_usage(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
    ) -> anyhow::Result<Option<(u64, u64)>> {
        management.require(SagaManagementPermission::ReadAudit)?;
        let Some(rate) = self.config.tenant_action_rates.get(tenant.as_str()) else {
            return Ok(None);
        };
        Ok(Some(
            self.backend
                .store()
                .tenant_action_rate_usage(tenant, rate.window_ms)
                .await?,
        ))
    }

    /// 业务作用：在变更类管理动作事务内按租户预留一次速率预算——单租户刷重试类动作
    /// 不能挤占其它租户的恢复通道。
    ///
    /// 未配置速率的租户直接放行且不写账本;超限以稳定原因码拒绝并使动作事务回滚,
    /// 与系统故障可区分。必须在 ambient 事务内调用:动作失败回滚时预算一并退还。
    ///
    /// 参数说明：
    /// - `tenant`: 已认证管理主体被授权访问的租户。
    ///
    /// 返回：预算可用返回 `Ok`;超限或底层失败返回错误。
    async fn reserve_action_rate_budget(&self, tenant: &TenantId) -> anyhow::Result<()> {
        let Some(rate) = self.config.tenant_action_rates.get(tenant.as_str()) else {
            return Ok(());
        };
        if matches!(
            self.backend
                .store()
                .reserve_tenant_action_rate(tenant, rate.max_actions, rate.window_ms)
                .await?,
            nasaga_backend::ActionRateReservation::Exceeded
        ) {
            record_action_rate_rejection();
            // 类型化拒绝使协议层无需读取错误文本，也不会暴露其它租户的用量。
            return Err(SagaManagementError::RateLimitExceeded.into());
        }
        Ok(())
    }

    /// 业务作用：读取一份不含业务身份标签的 Saga 运行指标快照。
    ///
    /// 生命周期与状态计数来自当前 Saga 后端的已提交事实；Kafka 处理指标来自当前进程。
    /// 宿主应将该入口放在受保护的运维 `/metrics` 端口，不要与业务 API 公网暴露。
    ///
    /// 参数说明：
    /// - `now_ms`: 当前 epoch 毫秒，用于统计已到期 durable timer。
    ///
    /// 返回：数据库聚合成功时返回可渲染 Prometheus 文本的快照；连接或查询失败返回错误。
    pub async fn load_operational_metrics(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<SagaOperationalMetrics> {
        Ok(self
            .backend
            .store()
            .load_operational_metrics(now_ms)
            .await?
            .into())
    }

    /// 业务作用：在占用 Orchestrator Inbox 身份前，把 transport 认证出的 producer
    /// 与 definition 登记的步骤 owner 绑定，阻止共享 result topic 上的越权作证。
    ///
    /// 该校验只接受 transport 信任策略给出的逻辑身份；envelope 自报字段会先完成派生复验，
    /// 随后再定位固定版本的 definition。失败时调用方必须隔离消息，不得进入 Inbox claim，
    /// 否则攻击者可以用合法 event id 抢先污染真实 owner 的去重身份。
    ///
    /// 参数说明：
    /// - `envelope`: transport 已解码但尚未进入持久化路径的结果 envelope。
    /// - `producer`: mTLS/SASL principal 或独占 topic 经信任策略映射出的逻辑服务身份。
    ///
    /// 返回：producer 与步骤 owner 精确一致时返回成功；身份、definition、步骤或 owner
    /// 任一不一致时返回协议错误，且没有任何数据库副作用。
    pub fn verify_result_producer(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<()> {
        let identity = envelope.verified_for_delivery()?;
        let registry = self.registry_snapshot();
        let definition = registry
            .get_for_tenant(
                &identity.tenant,
                &identity.workflow,
                identity.definition_version,
            )
            .ok_or(crate::SagaResultProcessingError::ContractInvalid)?;
        if definition.digest() != identity.definition_digest {
            return Err(crate::SagaResultProcessingError::ContractInvalid.into());
        }
        let step = definition
            .step(&identity.step)
            .ok_or(crate::SagaResultProcessingError::ContractInvalid)?;
        // 能写共享 topic 不代表有权为任意步骤作证；owner 绑定必须先于 Inbox claim。
        if step.owner() != producer {
            return Err(crate::SagaResultProcessingError::ProducerUnauthorized.into());
        }
        Ok(())
    }

    /// 业务作用：消费一条已经获得 transport producer 身份的结果，并在认证通过后推进 Saga。
    ///
    /// 参数说明：
    /// - `envelope`: 参与方结果 envelope。
    /// - `producer`: transport 信任策略解析出的逻辑服务身份。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：认证和本地事务均成功时返回可 ACK 结论；认证失败无持久化副作用，推进失败则
    /// 事务回滚，两者都返回错误供 transport 选择 DLT 或保留 offset 重投。
    pub async fn handle_authenticated_result(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        now_ms: i64,
    ) -> anyhow::Result<HandleOutcome> {
        self.handle_authenticated_result_traced(envelope, producer, None, now_ms)
            .await
    }

    /// 业务作用：携带 transport 收据链路上下文消费结果——参与方到编排端的 trace 由此续接。
    ///
    /// `receipt_trace` 必须来自 transport 收据（如 Kafka 消息 header）的显式解析，框架不
    /// 读取 ambient 状态。推进真实生效时，该上下文在同一事务内写入实例作为最新因果上下文，
    /// 本事务内发出的下一条命令与之后的 timer/恢复命令都从它派生 child span。
    ///
    /// 参数说明：
    /// - `envelope`: 参与方结果 envelope。
    /// - `producer`: transport 信任策略解析出的逻辑服务身份。
    /// - `receipt_trace`: 收据中已校验的链路上下文；`None` 表示收据未携带。
    /// - `now_ms`: 当前 epoch 毫秒。
    ///
    /// 返回：语义与 [`handle_authenticated_result`](Self::handle_authenticated_result)
    /// 完全一致；trace 不改变认证、去重与推进边界。
    pub async fn handle_authenticated_result_traced(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
        now_ms: i64,
    ) -> anyhow::Result<HandleOutcome> {
        self.handle_authenticated_result_authorized_traced(
            envelope,
            producer,
            receipt_trace,
            now_ms,
            &|| Ok(()),
        )
        .await
    }

    /// 业务作用：在宿主冻结的资格下接收已认证结果，数据库等待期间失权时回滚全部推进事实。
    ///
    /// 参数说明：`envelope` 是原结果，`producer` 是认证身份，`receipt_trace` 是可选受信链路，
    /// `now_ms` 是裁决时刻，`authorize` 同步复验同一次操作的期限、撤销代际和安全合同。
    ///
    /// 返回：资格持续有效且事务提交后才返回应用或重复收据；失权回滚，COMMIT 已发出后的不确定性保持原分类。
    /// 加入已有同源事务时，调用方仍须在外层最终提交前复验自己的执行资格。
    pub async fn handle_authenticated_result_authorized_traced(
        &self,
        envelope: &SagaResultEnvelope,
        producer: &ServiceIdentity,
        receipt_trace: Option<&TraceContext>,
        now_ms: i64,
        authorize: &(dyn Fn() -> anyhow::Result<()> + Send + Sync),
    ) -> anyhow::Result<HandleOutcome> {
        // 失效操作不能先占用 Inbox 身份，也不能读取新 registry 后借新的确认资格继续。
        authorize()?;
        // producer 权限必须在 Inbox claim 之前复验，防止越权消息抢占 event id。
        self.verify_result_producer(envelope, producer)?;
        let _latency =
            crate::latency::LatencyGuard::new(crate::latency::LatencyStage::TransitionTransaction);
        self.handle_verified_result(envelope, receipt_trace, now_ms, authorize)
            .await
    }

    /// 业务作用：以受保护事务幂等创建 Saga 并发布首步命令。
    ///
    /// 创建事务同时提交：`version/transition_seq = 1` 的 `NONE -> RUNNING`、步骤骨架、
    /// 首步 execute 命令 Outbox、step 超时 timer 与（可选）实例 deadline timer。
    /// 业务幂等键冲突时返回既有实例，不产生任何新副作用。
    ///
    /// 参数说明：
    /// - `request`: 创建请求。
    ///
    /// 返回：真实创建返回 [`StartOutcome::Started`]；幂等命中返回 `AlreadyExists`；
    /// 确定性拒绝可下转为 [`StartSagaError`]；事务失败保留回滚、提交结果不明或回滚失败的封闭分类。
    pub async fn start_saga(&self, request: &StartSagaRequest<'_>) -> anyhow::Result<StartOutcome> {
        self.start_saga_traced(request, None).await
    }

    /// 业务作用：携带显式链路上下文创建 Saga——发起入口的调用链由此接入 Saga 全程追踪。
    ///
    /// 框架不读取线程或任务 ambient 状态：调用方（Web 入口、调度任务）显式传入已校验的
    /// [`TraceContext`]，其 canonical `traceparent` 随实例同事务持久化；首步命令与之后
    /// 每一条自动命令（含 timer、崩溃恢复发出的）都从已提交实例读取同一 trace 并派生
    /// 新 child span。缺少上下文时传 `None`，实例照常创建与投递。
    ///
    /// 参数说明：
    /// - `request`: 创建请求。
    /// - `trace`: 发起入口已校验的链路上下文；`None` 表示调用链未提供。
    ///
    /// 返回：语义与 [`start_saga`](Self::start_saga) 完全一致；trace 不改变创建、幂等
    /// 与拒绝边界。
    pub async fn start_saga_traced(
        &self,
        request: &StartSagaRequest<'_>,
        trace: Option<&TraceContext>,
    ) -> anyhow::Result<StartOutcome> {
        self.start_saga_authorized_traced(request, trace, &|| Ok(()))
            .await
    }

    /// 业务作用：在调用方冻结的执行资格下裁决 Saga 创建与重复收据，连接池、行锁和事务等待不能延长权限。
    ///
    /// 参数说明：`request` 是创建事实，`trace` 是受信链路上下文，`authorize` 同步复验本次操作的资格与期限。
    ///
    /// 返回：资格有效且创建事务提交后返回创建或重复收据；失权走事务回滚，底层提交不确定分类保持不变。
    /// deprecated 定义仍按持久请求摘要裁决重复与冲突；只有真正的新建适用 active 门禁。
    /// 此门禁覆盖本次创建操作；加入已有同源事务时，外层事务还须在其最终提交前复验自己的执行资格。
    pub async fn start_saga_authorized_traced(
        &self,
        request: &StartSagaRequest<'_>,
        trace: Option<&TraceContext>,
        authorize: &(dyn Fn() -> anyhow::Result<()> + Send + Sync),
    ) -> anyhow::Result<StartOutcome> {
        // 快照必须在执行资格复验之后读取，认证时持有的旧资格不能授权当前创建。
        authorize()?;
        let registry = self.registry_snapshot();
        // 停用只撤销新建资格，已提交请求仍需用同一不可变定义计算摘要，不能失去持久提交的确认能力。
        let definition = registry
            .get_for_tenant(request.tenant, request.workflow, request.version)
            .ok_or(StartSagaError::DefinitionInactive)?;
        let digest = definition.digest();
        let first_step = definition
            .steps()
            .first()
            .ok_or(StartSagaError::DefinitionEmpty)?;
        let _latency =
            crate::latency::LatencyGuard::new(crate::latency::LatencyStage::StartTransaction);
        if request.first_command_payload.is_some() && request.first_command_raw_payload.is_some() {
            return Err(StartSagaError::InvalidPayload.into());
        }
        let payload = request.first_command_raw_payload.clone().or_else(|| {
            request
                .first_command_payload
                .clone()
                .map(nasaga_core::SagaPayload::json)
        });
        if let Some(payload) = &payload {
            payload
                .validate()
                .map_err(|_| StartSagaError::InvalidPayload)?;
            if &payload.contract() != first_step.payload_contract() {
                return Err(StartSagaError::InvalidPayload.into());
            }
        }
        let start_request_digest =
            derive_start_request_digest(request, &digest, first_step.name(), payload.as_ref());
        let legacy_start_request_digest = match payload.as_ref() {
            Some(payload) if payload.contract() == nasaga_core::SagaPayloadContract::default() => {
                let mut legacy = request.clone();
                legacy.first_command_payload = Some(
                    serde_json::from_slice(&payload.body)
                        .map_err(|_| StartSagaError::InvalidPayload)?,
                );
                Some(derive_legacy_start_request_digest(
                    &legacy,
                    &digest,
                    first_step.name(),
                ))
            }
            _ => None,
        };
        let canonical_trace = trace.map(TraceContext::to_traceparent);

        let outcome = crate::transaction::run_authorized_for(&self.backend, authorize, async {
            let creation = self
                .backend
                .store()
                .create_instance(&NewSagaInstance {
                    saga_id: request.saga_id,
                    tenant: request.tenant,
                    workflow: request.workflow,
                    business_key: request.business_key,
                    definition_version: request.version,
                    definition_digest: &digest,
                    start_request_digest: &start_request_digest,
                    legacy_start_request_digest: legacy_start_request_digest.as_deref(),
                    deadline_at_ms: request.deadline_at_ms,
                    // 创建即定位首步:step 超时裁决要求 current_step 与 timer 步骤一致。
                    current_step: Some(first_step.name()),
                    trigger_kind: request.trigger_kind,
                    trigger_id: request.trigger_id,
                    // 因果上下文与实例同事务落库:首步命令在同一事务内从创建回读的
                    // 实例行取 trace,发起入口的调用链从第一跳起就不断开。
                    traceparent: canonical_trace.as_deref(),
                })
                .await
                .map_err(|error| {
                    if error.kind() == SagaBackendErrorKind::Conflict {
                        anyhow::Error::new(StartSagaError::RequestConflict)
                    } else {
                        error.into()
                    }
                })?;
            let instance = match creation {
                // 幂等命中:绝不重复发布首步命令、timer 或初始 transition。
                SagaCreation::Existing(row) => return Ok(StartOutcome::AlreadyExists(row)),
                SagaCreation::Created(row) => row,
            };
            // 唯一键与摘要已在同一事务中裁决；新建遇到停用必须回滚实例及初始 transition，
            // 在此之前不预留配额、不发布命令或 timer，不能让拒绝的启动留下可见事实。
            if registry
                .get_for_start(request.tenant, request.workflow, request.version)
                .is_none()
            {
                return Err(StartSagaError::DefinitionInactive.into());
            }
            // 配额预留只对真实创建执行且与创建同事务:幂等命中不占额,创建回滚时预留
            // 一并回滚。条件自增在租户行锁上串行化并发创建——无锁计数会穿透上限。
            let cap = self
                .config
                .tenant_quotas
                .get(request.tenant.as_str())
                .copied();
            if matches!(
                self.backend
                    .store()
                    .reserve_tenant_quota(request.tenant, cap)
                    .await?,
                nasaga_backend::QuotaReservation::Exceeded
            ) {
                record_quota_rejection();
                // 类型化拒绝使协议层无需读取错误文本，也不会暴露其它租户的用量。
                return Err(StartSagaError::TenantQuotaExceeded.into());
            }
            self.backend
                .store()
                .register_steps(request.saga_id, definition)
                .await?;
            self.issue_command(
                &instance,
                first_step,
                StepPhase::Execute,
                AttemptNo::FIRST,
                request
                    .first_command_raw_payload
                    .clone()
                    .map(CommandInput::Raw)
                    .or_else(|| {
                        request
                            .first_command_payload
                            .clone()
                            .map(CommandInput::Json)
                    }),
                None,
                request.now_ms,
                instance.version,
            )
            .await?;
            // 实例级 deadline 与创建同事务:实例一旦可见即受全局期限保护。
            if let Some(deadline) = request.deadline_at_ms {
                self.schedule_timer(
                    request.saga_id,
                    TimerScope::Instance,
                    KIND_INSTANCE_DEADLINE,
                    AttemptNo::FIRST,
                    deadline,
                    instance.version,
                )
                .await?;
            }
            Ok(StartOutcome::Started(instance))
        })
        .await?;
        match &outcome {
            StartOutcome::Started(instance) => tracing::info!(
                saga_id = instance.saga_id.as_str(),
                tenant = instance.tenant.as_str(),
                workflow = instance.workflow.as_str(),
                definition_version = instance.definition_version.get(),
                saga_status = instance.status.as_str(),
                "Saga 创建事务已提交"
            ),
            StartOutcome::AlreadyExists(instance) => tracing::info!(
                saga_id = instance.saga_id.as_str(),
                tenant = instance.tenant.as_str(),
                workflow = instance.workflow.as_str(),
                saga_status = instance.status.as_str(),
                "Saga 启动重放已幂等吸收"
            ),
        }
        Ok(outcome)
    }

    /// 业务作用：消费一条参与方结果事件并推进状态机，是 Orchestrator 的主推进入口。
    ///
    /// 事务序固定为：Inbox claim → 合同交叉校验 → attempt journal 记账（幂等吸收）→
    /// 按阶段路由裁决（CAS + transition + 下一命令 Outbox + timer）→ COMMIT。
    /// CAS 冲突时返回错误使事务回滚且**不得 ACK**，重投后基于新快照重新裁决。
    ///
    /// 参数说明：
    /// - `envelope`: 已通过 transport 层 producer 认证的结果 envelope。
    /// - `now_ms`: 当前时刻（epoch 毫秒）。
    /// - `authorize`: 本次结果操作固定的执行资格，等待与提交前必须保持有效。
    ///
    /// 返回：推进成功返回 [`HandleOutcome::Applied`]（COMMIT 已确认，调用方可 ACK）；
    /// 重复事件返回 `Duplicate`（可 ACK）；身份复验失败、合同不匹配、矛盾 outcome 或
    /// CAS 冲突返回错误（已回滚，不得 ACK）。
    async fn handle_verified_result(
        &self,
        envelope: &SagaResultEnvelope,
        receipt_trace: Option<&TraceContext>,
        now_ms: i64,
        authorize: &(dyn Fn() -> anyhow::Result<()> + Send + Sync),
    ) -> anyhow::Result<HandleOutcome> {
        // 身份复验在任何持久化动作之前:伪造 envelope 不允许占用去重身份。
        let identity = envelope.verified_for_delivery()?;
        let status = envelope
            .parsed_status()
            .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;
        let terminal = envelope
            .parsed_terminal()
            .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;
        let registry = self.registry_snapshot();

        let outcome = crate::transaction::run_authorized_for(&self.backend, authorize, async {
            if matches!(
                self.backend
                    .inbox()
                    .claim(&self.config.inbox_consumer, &envelope.event_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(HandleOutcome::Duplicate);
            }
            let instance = self
                .backend
                .store()
                .load_instance(&identity.saga_id)
                .await?
                .ok_or(crate::SagaResultProcessingError::ContractInvalid)?;
            verify_instance_contract(&instance, &identity)
                .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;
            // 实例读取或前序 Inbox 竞争可能已经等待；任何 journal 与后续状态写入前复验原资格。
            authorize()?;
            if !instance.control_state.allows_automatic_actions() {
                // PAUSED 的核心语义是不发出任何自动动作；结果也不能先被 Inbox 吞掉再悬空。
                // 类型化延后使同事务 Inbox claim 回滚且不消耗毒消息预算；恢复后再裁决。
                return Err(crate::SagaResultProcessingError::Paused.into());
            }
            let definition = registry
                .get_for_tenant(
                    &instance.tenant,
                    &instance.workflow,
                    instance.definition_version,
                )
                .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;
            let step_def = definition.step(&identity.step).ok_or_else(|| {
                anyhow::Error::from(crate::SagaResultProcessingError::ContractInvalid)
            })?;

            // 真实 outcome 必须先落 journal:即使实例已在人工介入,迟到事实也不许丢
            // (拒收会造成恢复时重复补偿)。
            let recorded = self
                .backend
                .store()
                .record_attempt_outcome(
                    &identity.saga_id,
                    &identity.step,
                    identity.phase,
                    identity.attempt,
                    status,
                    Some(&envelope.event_id),
                )
                .await?;
            match recorded {
                // 同一 attempt 的相同结论已经处理过(换了 event id 的 DLT/管理重放绕过了
                // Inbox 去重):journal 与 Inbox 都已留证,此处必须停止一切路由副作用,
                // 否则重放会重复推进状态机并重发业务命令。
                AttemptOutcomeRecord::AlreadyRecorded => {
                    return Ok(HandleOutcome::Applied {
                        status: instance.status,
                    });
                }
                AttemptOutcomeRecord::Conflicting { existing } => {
                    // 后到互斥事实不能覆盖 journal 先到结论，也不能通过返回错误让 Inbox 与
                    // 冲突证据一起回滚形成永久毒消息；先留存双方状态，再同事务冻结自动动作。
                    self.backend
                        .store()
                        .record_attempt_conflict(&AttemptConflictFact {
                            saga_id: &identity.saga_id,
                            step: &identity.step,
                            phase: identity.phase,
                            attempt: identity.attempt,
                            existing_status: existing,
                            incoming_status: status,
                            incoming_event_id: &envelope.event_id,
                            conflict_kind: SagaConflictKind::AttemptTerminal,
                        })
                        .await?;
                    let status = if instance.status.is_terminal()
                        || instance.status == SagaStatus::ManualIntervention
                    {
                        // 终态是不可重开的已提交结论；冲突事实仍与 Inbox 同事务
                        // 留存，但不得尝试非法的终态 -> Manual 迁移导致证据整体回滚。
                        instance.status
                    } else {
                        self.escalate_manual(
                            &instance,
                            TriggerKind::Event,
                            &envelope.event_id,
                            "attempt_outcome_conflict",
                        )
                        .await?
                    };
                    return Ok(HandleOutcome::Applied { status });
                }
                AttemptOutcomeRecord::Recorded => {}
            }

            // 已认证结果是实例最新的因果事实:在同一推进事务内落库,并刷新内存快照——
            // 本事务随后发出的下一条命令、之后的 timer 与崩溃恢复命令都从同一 trace
            // 派生 child。推进输掉 CAS 时整个事务回滚,上下文不会先于推进生效。
            let instance = match receipt_trace {
                Some(trace) => {
                    let canonical = trace.to_traceparent();
                    self.backend
                        .store()
                        .update_trace_context(&identity.saga_id, &canonical)
                        .await?;
                    SagaInstanceRow {
                        traceparent: Some(canonical),
                        ..instance
                    }
                }
                None => instance,
            };

            let context = ResultContext {
                instance: &instance,
                definition,
                step_def,
                identity: &identity,
                status,
                terminal,
                reason_code: envelope.reason_code.as_deref(),
                event_id: &envelope.event_id,
                now_ms,
            };
            // 路由分支各自内嵌多层持久化 future,合并成单个状态机会让 handle_result
            // 的 poll 栈占用以 MB 计(debug 态)并压垮调用方线程栈;逐分支装箱,
            // 让大状态机落在堆上。
            let new_status = match identity.phase {
                StepPhase::Execute => Box::pin(self.on_execute_result(&context)).await?,
                StepPhase::Cancel => Box::pin(self.on_cancel_result(&context)).await?,
                StepPhase::Compensate => Box::pin(self.on_compensate_result(&context)).await?,
                StepPhase::Resolve => Box::pin(self.on_resolve_result(&context)).await?,
            };
            Ok(HandleOutcome::Applied { status: new_status })
        })
        .await?;
        match &outcome {
            HandleOutcome::Applied { status } => tracing::info!(
                saga_id = identity.saga_id.as_str(),
                workflow = identity.workflow.as_str(),
                step = identity.step.as_str(),
                phase = identity.phase.as_str(),
                attempt = identity.attempt.get(),
                saga_status = status.as_str(),
                "Saga 结果事务已提交"
            ),
            HandleOutcome::Duplicate => tracing::info!(
                saga_id = identity.saga_id.as_str(),
                workflow = identity.workflow.as_str(),
                step = identity.step.as_str(),
                phase = identity.phase.as_str(),
                attempt = identity.attempt.get(),
                "Saga 重复结果已由 Inbox 吸收"
            ),
        }
        Ok(outcome)
    }

    /// 业务作用：领取到期 timer 并逐个裁决，多副本可并发竞争。
    ///
    /// **必须在 ambient 事务之外调用**：租约领取独立提交；每个 timer 的裁决各自开启
    /// 单一本地事务。实例处于 `PAUSED` 时不消费，按轮询退避交还（只是轮询节奏，
    /// 不顺延业务 deadline）。
    ///
    /// 参数说明：
    /// - `owner`: 本副本稳定标识。
    /// - `now_ms`: 当前时刻（epoch 毫秒）。
    ///
    /// 返回：本轮实际完成业务裁决的 timer 数；领取或裁决基础设施失败返回错误
    /// （已领取未处理的 timer 由租约到期自动回收）。
    pub async fn run_due_timers(&self, owner: &str, now_ms: i64) -> anyhow::Result<u32> {
        // 单调起点必须早于领取 SQL：数据库等待和后续逐条处理都消耗同一份租约预算，
        // 不能让整批 timer 永远复用领取瞬间的旧 now_ms。
        let claimed_at = Instant::now();
        // token 额外绑定不可注入的 runtime nonce：即使两个副本误配同一 owner，旧副本也无法
        // 派生新持有者的 token 完成过期租约，不能把防脑裂正确性押在部署配置上。
        let token = self.fencing_tokens.issue(owner, now_ms);
        let claimed = self
            .backend
            .store()
            .claim_due_timers(
                owner,
                token,
                now_ms,
                self.config.timer_lease_ms,
                self.config.timer_claim_limit,
            )
            .await?;
        let mut applied = 0u32;
        for timer in claimed.timers() {
            match self
                .fire_timer_inner(timer, claimed.token(), now_ms, Some(claimed_at))
                .await?
            {
                TimerOutcome::Applied { status } => {
                    applied += 1;
                    tracing::info!(
                        saga_id = timer.saga_id.as_str(),
                        timer_kind = %timer.kind,
                        timer_scope = %timer.scope_kind,
                        attempt = timer.attempt.get(),
                        saga_status = status.as_str(),
                        "Saga durable timer 裁决事务已提交"
                    );
                }
                TimerOutcome::SkippedPaused => {
                    // 暂停只停止发出新动作;timer 保留原语义,按退避节奏交还等待恢复。
                    // 交还前重新读取流逝时间；租约到期的旧 owner 必须认输，不能把
                    // 即将由新 owner 接管的 timer 又推回未来。
                    let release_now = elapsed_epoch_ms(now_ms, claimed_at)?;
                    let _ = self
                        .backend
                        .store()
                        .release_timer(
                            &timer.timer_id,
                            claimed.token(),
                            release_now,
                            checked_deadline(release_now, self.config.pause_backoff_ms)?,
                        )
                        .await?;
                }
                TimerOutcome::SkippedFencingLost => tracing::warn!(
                    saga_id = timer.saga_id.as_str(),
                    timer_kind = %timer.kind,
                    "Saga timer 已失去 fencing 权威，旧 owner 停止动作"
                ),
                TimerOutcome::SkippedStale => tracing::debug!(
                    saga_id = timer.saga_id.as_str(),
                    timer_kind = %timer.kind,
                    "Saga 过期 timer 已幂等吸收"
                ),
            }
        }
        Ok(applied)
    }

    /// 业务作用：裁决单个已领取 timer；消费与它触发的状态迁移同一事务。
    ///
    /// 参数说明：
    /// - `timer`: 已领取的 timer 行。
    /// - `token`: 本轮 fencing token。
    /// - `now_ms`: 当前时刻（epoch 毫秒）。
    ///
    /// 返回：完成裁决返回 [`TimerOutcome::Applied`]；暂停/失权/语义过时返回对应跳过
    /// 结论；CAS 冲突返回错误（事务回滚，timer 留在租约内等待重试或回收）。
    pub async fn fire_timer(
        &self,
        timer: &SagaTimerRow,
        token: &TimerFencingToken,
        now_ms: i64,
    ) -> anyhow::Result<TimerOutcome> {
        self.fire_timer_inner(timer, token, now_ms, None).await
    }

    /// 业务作用：执行单个 timer 的实际裁决，并在批量领取路径中按单调时钟复验租约。
    ///
    /// 参数说明：
    /// - `timer`: 已领取的 timer 行。
    /// - `token`: 本轮 fencing token。
    /// - `base_now_ms`: 领取时注入的 epoch 毫秒，同时作为本批次裁决时刻。
    /// - `claimed_at`: 批量领取的单调起点；为空表示调用方已传入最新裁决时刻。
    ///
    /// 返回：完成裁决返回 [`TimerOutcome::Applied`]；暂停/失权/语义过时返回对应跳过
    /// 结论；基础设施或状态机失败返回错误并回滚 timer 消费事务。
    async fn fire_timer_inner(
        &self,
        timer: &SagaTimerRow,
        token: &TimerFencingToken,
        base_now_ms: i64,
        claimed_at: Option<Instant>,
    ) -> anyhow::Result<TimerOutcome> {
        let _latency =
            crate::latency::LatencyGuard::new(crate::latency::LatencyStage::TransitionTransaction);
        let step_scope = parse_step_scope(&timer.scope_kind, &timer.scope_key)?;
        let registry = self.registry_snapshot();
        crate::transaction::run_for(&self.backend, async {
            let Some(instance) = self.backend.store().load_instance(&timer.saga_id).await? else {
                // 实例已被归档清理的孤儿 timer:消费吸收,不再触发。
                let fencing_now = match claimed_at {
                    Some(started) => elapsed_epoch_ms(base_now_ms, started)?,
                    None => base_now_ms,
                };
                return Ok(
                    match self
                        .backend
                        .store()
                        .complete_timer(&timer.timer_id, token, fencing_now)
                        .await?
                    {
                        TimerFencing::Applied => TimerOutcome::SkippedStale,
                        TimerFencing::Lost => TimerOutcome::SkippedFencingLost,
                    },
                );
            };
            if !instance.control_state.allows_automatic_actions() {
                return Ok(TimerOutcome::SkippedPaused);
            }
            let now_ms = match claimed_at {
                Some(started) => elapsed_epoch_ms(base_now_ms, started)?,
                None => base_now_ms,
            };
            // 消费必须与迁移同事务:先以 fencing 占住消费权,失权立即停止推进。
            if matches!(
                self.backend
                    .store()
                    .complete_timer(&timer.timer_id, token, now_ms)
                    .await?,
                TimerFencing::Lost
            ) {
                return Ok(TimerOutcome::SkippedFencingLost);
            }
            crate::latency::observe(
                crate::latency::LatencyStage::TimerLateness,
                Duration::from_millis(now_ms.saturating_sub(timer.due_at_ms).max(0) as u64),
            );
            // attempt 级 timeout 只对调度它的实例版本有效；版本已推进说明相应结果或
            // 另一计时器先完成了裁决，旧 timer 必须被吸收，绝不能再次发命令或改状态。
            // 实例 deadline 与解决总预算跨越多个实例版本，故意不采用该精确版本条件。
            if matches!(
                timer.kind.as_str(),
                KIND_STEP_TIMEOUT
                    | KIND_CANCEL_TIMEOUT
                    | KIND_COMPENSATE_TIMEOUT
                    | KIND_RESOLVE_TIMEOUT
            ) && timer.expected_saga_version != instance.version
            {
                return Ok(TimerOutcome::SkippedStale);
            }
            let definition = registry
                .get_for_tenant(
                    &instance.tenant,
                    &instance.workflow,
                    instance.definition_version,
                )
                .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;

            // 与 handle_result 同理:超时裁决分支装箱,防止 fire_timer 状态机撑爆调用栈。
            let status = match timer.kind.as_str() {
                KIND_STEP_TIMEOUT => {
                    Box::pin(self.on_step_timeout(
                        &instance,
                        definition,
                        step_scope.as_ref(),
                        timer,
                        now_ms,
                    ))
                    .await?
                }
                KIND_CANCEL_TIMEOUT => {
                    Box::pin(self.on_phase_retry_timeout(
                        &instance,
                        definition,
                        step_scope.as_ref(),
                        timer,
                        StepPhase::Cancel,
                        SagaStatus::Cancelling,
                        self.config.cancel_max_attempts,
                        "cancel_attempts_exhausted",
                        now_ms,
                    ))
                    .await?
                }
                KIND_COMPENSATE_TIMEOUT => {
                    Box::pin(self.on_phase_retry_timeout(
                        &instance,
                        definition,
                        step_scope.as_ref(),
                        timer,
                        StepPhase::Compensate,
                        SagaStatus::Compensating,
                        self.config.compensate_max_attempts,
                        "compensation_attempts_exhausted",
                        now_ms,
                    ))
                    .await?
                }
                KIND_RESOLVE_TIMEOUT => {
                    Box::pin(self.on_phase_retry_timeout(
                        &instance,
                        definition,
                        step_scope.as_ref(),
                        timer,
                        StepPhase::Resolve,
                        SagaStatus::WaitingResolution,
                        self.config.resolve_max_attempts,
                        "resolve_attempts_exhausted",
                        now_ms,
                    ))
                    .await?
                }
                KIND_RESOLUTION_BUDGET
                | KIND_FORWARD_RESOLUTION_BUDGET
                | KIND_COMPENSATION_RESOLUTION_BUDGET => {
                    // 解决预算耗尽只升级人工介入并告警:绝不自动降级为 Rejected,
                    // 否则支付已成功但响应丢失时会同时退款与继续成功流程。
                    if instance.status == SagaStatus::WaitingResolution {
                        Some(
                            self.escalate_manual(
                                &instance,
                                TriggerKind::Timer,
                                &timer.timer_id,
                                "resolution_budget_exhausted",
                            )
                            .await?,
                        )
                    } else {
                        None
                    }
                }
                KIND_INSTANCE_DEADLINE => {
                    Box::pin(self.on_instance_deadline(&instance, definition, timer, now_ms))
                        .await?
                }
                _ => anyhow::bail!("unknown timer kind"),
            };
            Ok(match status {
                Some(status) => TimerOutcome::Applied { status },
                // 语义已过时(状态或步骤已迁移):timer 已消费,无业务动作。
                None => TimerOutcome::SkippedStale,
            })
        })
        .await
    }

    /// 业务作用：管理面暂停实例——只停止自动动作，不改业务状态、不停业务时钟。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体、原因与 `saga.pause` 权限快照。
    /// - `tenant`: 已认证管理主体被授权访问的租户。
    /// - `saga_id`: 实例身份。
    /// - `operation_id`: 稳定管理 operation id；与控制态切换同事务写入本地幂等审计链。
    ///
    /// 返回：暂停生效返回 `Ok`；实例版本或控制状态过期返回错误，调用方重读后重试。
    pub async fn pause(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
    ) -> anyhow::Result<()> {
        self.pause_with_expectation(
            management,
            tenant,
            saga_id,
            operation_id,
            SagaManagementExpectation::default(),
        )
        .await
    }

    /// 业务作用：在事务内复验调用方版本后暂停实例，避免旧管理快照覆盖并发状态或控制迁移。
    ///
    /// 参数说明：管理上下文、租户、实例和操作身份定位动作，`expectation` 约束事务内实例版本。
    ///
    /// 返回：版本匹配且暂停提交时成功；初始控制态不允许动作返回管理前置条件错误，
    /// 提交 CAS 失去竞争返回可重试快照过期，权限或持久化失败返回对应错误。
    pub async fn pause_with_expectation(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        expectation: SagaManagementExpectation,
    ) -> anyhow::Result<()> {
        // 权限门禁必须早于实例查询，避免无权主体利用存在性与租户错误枚举 saga_id。
        management.require(SagaManagementPermission::Pause)?;
        crate::transaction::run_for(&self.backend, async {
            let instance = self
                .backend
                .store()
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or(SagaManagementError::NotFound)?;
            // 先让持久层识别完全相同的 operation；首次动作即使暂时完成 UPDATE，也仍在
            // 当前事务内，随后 expected version 失配会整体回滚，不能绕过并发门禁。
            match self
                .backend
                .store()
                .set_control_state(&ControlTransitionSpec {
                    saga_id,
                    expected_version: instance.version,
                    expected_control_version: instance.control_version,
                    from: nasaga_core::ControlState::Active,
                    to: nasaga_core::ControlState::Paused,
                    operation_id,
                    actor: management.actor(),
                    reason: management.reason(),
                })
                .await?
            {
                ControlCasOutcome::Applied => {
                    expectation.verify(instance.version, instance.control_version)?;
                    // 只有首次 operation 才占用动作预算；若额度不足，控制态和审计与
                    // 本次预留在同一事务回滚。已提交重放在上面的持久事实分支直接返回。
                    self.reserve_action_rate_budget(tenant).await?;
                    Ok(())
                }
                ControlCasOutcome::AlreadyApplied => Ok(()),
                ControlCasOutcome::Conflict => {
                    expectation.verify(instance.version, instance.control_version)?;
                    if instance.control_state == nasaga_core::ControlState::Active {
                        // 初次快照允许暂停而 CAS 未命中，说明提交前已经失去控制权威；
                        // 返回可重试并发类别，让协议层要求调用方重新加载。
                        Err(anyhow::Error::new(SagaConcurrencyError::StaleSnapshot)
                            .context("pause lost the control-state race; reload and retry"))
                    } else {
                        Err(management_precondition(
                            "pause requires an active control state",
                        ))
                    }
                }
            }
        })
        .await?;
        tracing::info!(
            saga_id = saga_id.as_str(),
            tenant = tenant.as_str(),
            actor = management.actor(),
            operation_id,
            control_state = "PAUSED",
            "Saga 暂停操作已提交"
        );
        Ok(())
    }

    /// 业务作用：管理面恢复实例；已逾期的 deadline 由 timer 在恢复后立即合并触发一次，
    /// 不重置到未来，也不逐 tick 补烧。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体、原因与 `saga.resume` 权限快照。
    /// - `tenant`: 已认证管理主体被授权访问的租户。
    /// - `saga_id`: 实例身份。
    /// - `operation_id`: 稳定管理 operation id；重放已提交操作不会再次改变控制态。
    ///
    /// 返回：恢复生效返回 `Ok`；实例版本或控制状态过期返回错误。
    pub async fn resume(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
    ) -> anyhow::Result<()> {
        self.resume_with_expectation(
            management,
            tenant,
            saga_id,
            operation_id,
            SagaManagementExpectation::default(),
        )
        .await
    }

    /// 业务作用：在事务内复验调用方版本后恢复实例，并保持原业务 deadline 语义。
    ///
    /// 参数说明：管理上下文、租户、实例和操作身份定位动作，`expectation` 约束事务内实例版本。
    ///
    /// 返回：版本匹配且恢复提交时成功；初始控制态不允许动作返回管理前置条件错误，
    /// 提交 CAS 失去竞争返回可重试快照过期，权限或持久化失败返回对应错误。
    pub async fn resume_with_expectation(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        expectation: SagaManagementExpectation,
    ) -> anyhow::Result<()> {
        // 与 pause 相同，先鉴权再读实例，防止管理读侧成为跨租户枚举 oracle。
        management.require(SagaManagementPermission::Resume)?;
        crate::transaction::run_for(&self.backend, async {
            let instance = self
                .backend
                .store()
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or(SagaManagementError::NotFound)?;
            // 持久 operation 事实优先于调用方旧快照；首次恢复仍在同一事务内复验版本，
            // 失配会连同控制态 UPDATE 一起回滚，已提交重放则不再次唤醒 timer。
            match self
                .backend
                .store()
                .set_control_state(&ControlTransitionSpec {
                    saga_id,
                    expected_version: instance.version,
                    expected_control_version: instance.control_version,
                    from: nasaga_core::ControlState::Paused,
                    to: nasaga_core::ControlState::Active,
                    operation_id,
                    actor: management.actor(),
                    reason: management.reason(),
                })
                .await?
            {
                ControlCasOutcome::Applied => {
                    expectation.verify(instance.version, instance.control_version)?;
                    // 新恢复动作才消耗预算；超限会回滚刚完成的控制态 CAS 与审计，
                    // 完全相同的已提交重放不改变速率账本。
                    self.reserve_action_rate_budget(tenant).await?;
                    // 恢复不重置业务期限：只清除暂停产生的 available_at 退避，
                    // 已逾期 timer 会在下一轮立即可领，未来 timer 仍等待原 due_at。
                    self.backend.store().wake_saga_timers(saga_id).await?;
                    Ok(())
                }
                ControlCasOutcome::AlreadyApplied => Ok(()),
                ControlCasOutcome::Conflict => {
                    expectation.verify(instance.version, instance.control_version)?;
                    if instance.control_state == nasaga_core::ControlState::Paused {
                        // 初次快照允许恢复而 CAS 未命中，说明提交前已经失去控制权威；
                        // 返回可重试并发类别，让协议层要求调用方重新加载。
                        Err(anyhow::Error::new(SagaConcurrencyError::StaleSnapshot)
                            .context("resume lost the control-state race; reload and retry"))
                    } else {
                        Err(management_precondition(
                            "resume requires a paused control state",
                        ))
                    }
                }
            }
        })
        .await?;
        tracing::info!(
            saga_id = saga_id.as_str(),
            tenant = tenant.as_str(),
            actor = management.actor(),
            operation_id,
            control_state = "ACTIVE",
            "Saga 恢复操作已提交"
        );
        Ok(())
    }

    /// 业务作用：管理面在人工介入后重试补偿——基于持久化事实继续冻结计划内的补偿。
    ///
    /// 只允许 `MANUAL_INTERVENTION -> COMPENSATING` 且实例已有冻结计划；计划首项为
    /// `HALTED` 时，同事务管理审计会显式重开 Orchestrator 投影，并把 operation id 写入
    /// command 供 Participant 二次门禁。`Unknown` 仍禁止直接重做；无剩余计划项时收敛。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体、原因与 `saga.retry_compensation` 权限快照。
    /// - `tenant`: 已认证管理主体被授权访问的租户。
    /// - `saga_id`: 实例身份。
    /// - `operation_id`: 稳定管理 operation id，作为迁移触发身份进入审计链。
    /// - `now_ms`: 当前时刻（epoch 毫秒）。
    ///
    /// 返回：推进后的实例状态；PAUSED、实例不在人工介入、无冻结计划或 CAS 冲突返回错误。
    pub async fn retry_compensation(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        now_ms: i64,
    ) -> anyhow::Result<SagaStatus> {
        self.retry_compensation_with_expectation(
            management,
            tenant,
            saga_id,
            operation_id,
            now_ms,
            SagaManagementExpectation::default(),
        )
        .await
    }

    /// 业务作用：在事务内命中调用方版本后重开冻结补偿计划，禁止旧快照发布新的外部命令。
    ///
    /// 参数说明：管理上下文、实例、操作身份和时刻定位动作，`expectation` 约束事务内实例版本。
    ///
    /// 返回：提交后的状态；过期快照、权限、恢复前置条件或持久化失败返回错误。
    pub async fn retry_compensation_with_expectation(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        now_ms: i64,
        expectation: SagaManagementExpectation,
    ) -> anyhow::Result<SagaStatus> {
        // 人工恢复会再次发布外部补偿命令，必须先做最小权限校验，再接触实例数据。
        management.require(SagaManagementPermission::RetryCompensation)?;
        let registry = self.registry_snapshot();
        let status = crate::transaction::run_for(&self.backend, async {
            let instance = self
                .backend
                .store()
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or(SagaManagementError::NotFound)?;
            match self
                .backend
                .store()
                .record_management_operation(
                    saga_id,
                    operation_id,
                    "retry_frozen_compensation",
                    management.actor(),
                    management.reason(),
                )
                .await?
            {
                // 同一人工操作已与状态迁移、命令 Outbox 一起提交；重放必须直接返回
                // 当前状态，绝不能再分配新 attempt 或发第二条补偿命令。
                ManagementAuditOutcome::AlreadyRecorded => return Ok(instance.status),
                ManagementAuditOutcome::Recorded => {}
            }
            // operation 审计先区分首次调用与已提交重放；仅首次调用预留额度，后续任何
            // 前置条件或命令发布失败都会让审计与预算一起回滚。
            self.reserve_action_rate_budget(tenant).await?;
            // 新 operation 只有命中调用方观察到的版本才可发布外部补偿命令；失配时本事务
            // 会回滚刚写入的管理审计，原 operation id 仍可由调用方更正后使用。
            expectation.verify(instance.version, instance.control_version)?;
            if !instance.control_state.allows_automatic_actions() {
                return Err(management_precondition(
                    "retry_compensation requires ACTIVE control state; resume first",
                ));
            }
            if instance.status != SagaStatus::ManualIntervention {
                return Err(management_precondition(
                    "retry_compensation requires MANUAL_INTERVENTION",
                ));
            }
            if instance.compensation_plan_version.is_none() {
                return Err(management_precondition(
                    "no frozen compensation plan to retry",
                ));
            }
            let definition = registry
                .get_for_tenant(
                    &instance.tenant,
                    &instance.workflow,
                    instance.definition_version,
                )
                .ok_or_else(|| management_precondition("instance definition is not registered"))?;
            let steps = self.backend.store().load_steps(saga_id).await?;
            match next_pending_compensation(&instance, definition, &steps, true)? {
                Some(step) => {
                    let recovering_halted = steps.iter().any(|row| {
                        row.step == step
                            && row.compensation_status == StepCompensationStatus::Halted
                    });
                    if recovering_halted {
                        // HALTED 只能由本事务已写入的管理审计解除；普通投影补丁和 timer
                        // 均不能访问该路径，参与方还会复验命令上的 recovery operation。
                        self.backend
                            .store()
                            .reopen_halted_compensation(saga_id, &step, operation_id)
                            .await?;
                    }
                    let version = self
                        .advance(
                            &instance,
                            SagaStatus::Compensating,
                            Direction::Compensating,
                            Some(&step),
                            TriggerKind::Admin,
                            operation_id,
                            None,
                            None,
                        )
                        .await?;
                    let step_def = definition.step(&step).ok_or_else(|| {
                        management_precondition("plan step absent from definition")
                    })?;
                    let attempt = self
                        .next_attempt(saga_id, &step, StepPhase::Compensate)
                        .await?;
                    self.issue_command(
                        &instance,
                        step_def,
                        StepPhase::Compensate,
                        attempt,
                        None,
                        Some(operation_id),
                        now_ms,
                        version,
                    )
                    .await?;
                    Ok(SagaStatus::Compensating)
                }
                None => {
                    // 计划内已无待补偿项:经 COMPENSATING 立即收敛为 COMPENSATED,
                    // 两次迁移共享同一管理操作的派生触发身份。
                    let version = self
                        .advance(
                            &instance,
                            SagaStatus::Compensating,
                            Direction::Compensating,
                            None,
                            TriggerKind::Admin,
                            operation_id,
                            None,
                            None,
                        )
                        .await?;
                    self.converge_compensated(&instance, version, TriggerKind::Admin, operation_id)
                        .await?;
                    Ok(SagaStatus::Compensated)
                }
            }
        })
        .await?;
        tracing::info!(
            saga_id = saga_id.as_str(),
            tenant = tenant.as_str(),
            actor = management.actor(),
            operation_id,
            saga_status = status.as_str(),
            "Saga 人工补偿恢复操作已提交"
        );
        Ok(status)
    }

    /// 业务作用：在 Unknown 解决硬预算耗尽后，由经授权的操作员恢复一次查询周期。
    ///
    /// 本入口只重发 typed resolve；若查询投影为 `HALTED`，只在同事务审计成立后重开查询，
    /// `UNKNOWN/HALTED` 补偿事实本身绝不被直接改回 `PENDING`，不会二次退款。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体、原因与 `saga.retry_resolution` 权限快照。
    /// - `tenant`: 已授权租户。
    /// - `saga_id`: 人工介入实例身份。
    /// - `operation_id`: 稳定管理操作身份，重放不再发新命令。
    /// - `now_ms`: 当前 epoch 毫秒，用于新的 resolve timeout 与硬预算。
    ///
    /// 返回：成功返回 `WAITING_RESOLUTION`；PAUSED、非人工态、当前没有 Unknown 事实、
    /// 步骤不支持 Poll、租户/权限不匹配或 CAS 冲突返回错误。
    pub async fn retry_resolution(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        now_ms: i64,
    ) -> anyhow::Result<SagaStatus> {
        self.retry_resolution_with_expectation(
            management,
            tenant,
            saga_id,
            operation_id,
            now_ms,
            SagaManagementExpectation::default(),
        )
        .await
    }

    /// 业务作用：在事务内命中调用方版本后重开 Unknown 解决周期，禁止旧快照发布新的查询命令。
    ///
    /// 参数说明：管理上下文、实例、操作身份和时刻定位动作，`expectation` 约束事务内实例版本。
    ///
    /// 返回：提交后的状态；过期快照、权限、解决前置条件或持久化失败返回错误。
    pub async fn retry_resolution_with_expectation(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        now_ms: i64,
        expectation: SagaManagementExpectation,
    ) -> anyhow::Result<SagaStatus> {
        management.require(SagaManagementPermission::RetryResolution)?;
        let registry = self.registry_snapshot();
        let status = crate::transaction::run_for(&self.backend, async {
            let instance = self
                .backend
                .store()
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or(SagaManagementError::NotFound)?;
            match self
                .backend
                .store()
                .record_management_operation(
                    saga_id,
                    operation_id,
                    "retry_resolution",
                    management.actor(),
                    management.reason(),
                )
                .await?
            {
                ManagementAuditOutcome::AlreadyRecorded => return Ok(instance.status),
                ManagementAuditOutcome::Recorded => {}
            }
            // 已提交 operation 不再占用额度；首次操作的审计、预算、HALTED 重开和
            // resolve command 仍处于同一事务，要么全部提交，要么全部撤销。
            self.reserve_action_rate_budget(tenant).await?;
            // 已提交重放直接返回；首次操作必须在同一事务内命中 expected version，
            // 才能解除 HALTED 门禁并产生新的 resolve command。
            expectation.verify(instance.version, instance.control_version)?;
            if !instance.control_state.allows_automatic_actions() {
                return Err(management_precondition(
                    "retry_resolution requires ACTIVE control state; resume first",
                ));
            }
            if instance.status != SagaStatus::ManualIntervention {
                return Err(management_precondition(
                    "retry_resolution requires MANUAL_INTERVENTION",
                ));
            }
            let step = instance
                .current_step
                .as_ref()
                .ok_or_else(|| management_precondition("manual resolution has no current step"))?;
            let definition = registry
                .get_for_tenant(
                    &instance.tenant,
                    &instance.workflow,
                    instance.definition_version,
                )
                .ok_or_else(|| management_precondition("instance definition is not registered"))?;
            let step_def = definition
                .step(step)
                .ok_or_else(|| management_precondition("resolution step absent from definition"))?;
            if step_def.resolution().mode() != Some(ResolutionMode::Poll) {
                return Err(management_precondition(
                    "retry_resolution requires a Poll resolver",
                ));
            }
            let row = self
                .backend
                .store()
                .load_steps(saga_id)
                .await?
                .into_iter()
                .find(|row| row.step == *step)
                .ok_or_else(|| management_precondition("resolution step journal is missing"))?;
            let has_unknown_target = match instance.direction {
                Direction::Forward => row.forward_status == StepForwardStatus::Unknown,
                Direction::Compensating => {
                    row.compensation_status == StepCompensationStatus::Unknown
                }
            };
            if !has_unknown_target
                || !matches!(
                    row.resolution_status,
                    StepResolutionStatus::Pending | StepResolutionStatus::Halted
                )
            {
                return Err(management_precondition(
                    "retry_resolution requires an unresolved Unknown effect",
                ));
            }

            let attempt = self.next_attempt(saga_id, step, StepPhase::Resolve).await?;
            if row.resolution_status == StepResolutionStatus::Halted {
                // 普通单调投影故意拒绝 HALTED→PENDING；这里只允许同事务中已经落审计的
                // 管理操作重开，避免自动 timeout 或迟到结果解除人工冻结。
                self.backend
                    .store()
                    .reopen_halted_resolution(saga_id, step, operation_id)
                    .await?;
            }
            let version = self
                .advance(
                    &instance,
                    SagaStatus::WaitingResolution,
                    instance.direction,
                    Some(step),
                    TriggerKind::Admin,
                    operation_id,
                    None,
                    None,
                )
                .await?;
            self.schedule_timer(
                saga_id,
                TimerScope::Step(step),
                resolution_budget_kind(instance.direction),
                attempt,
                checked_deadline(now_ms, duration_millis(step_def.resolution().budget())?)?,
                version,
            )
            .await?;
            self.issue_command(
                &instance,
                step_def,
                StepPhase::Resolve,
                attempt,
                None,
                Some(operation_id),
                now_ms,
                version,
            )
            .await?;
            Ok(SagaStatus::WaitingResolution)
        })
        .await?;
        tracing::info!(
            saga_id = saga_id.as_str(),
            tenant = tenant.as_str(),
            actor = management.actor(),
            operation_id,
            saga_status = status.as_str(),
            "Saga 人工解决查询恢复操作已提交"
        );
        Ok(status)
    }

    /// 业务作用：系统外人工处置完成后关闭自动化——`MANUAL_INTERVENTION -> MANUALLY_CLOSED`
    /// 的唯一管理入口，实例由此离开非终态集合，`nasaga_manual_intervention` 回落。
    ///
    /// 关闭只记录"自动化已由人工关闭"这一事实：不写 `COMPLETED`/`COMPENSATED`、不伪造
    /// 未经系统证明的业务结果；未完成的补偿计划仍以快照保留。动作与
    /// [`MANUAL_CLOSE_ACTION`](nasaga_backend::MANUAL_CLOSE_ACTION) 审计同事务提交，
    /// 状态机受限出边只放行携带该同事务证据的迁移，自动 worker 无法触发。
    ///
    /// 参数说明：
    /// - `management`: 已认证主体、强制原因与 `saga.manual_close` 权限快照。
    /// - `tenant`: 已认证管理主体被授权访问的租户。
    /// - `saga_id`: 实例身份。
    /// - `operation_id`: 一次性管理 operation 身份；重放按幂等成功返回，不产生第二次迁移。
    ///
    /// 返回：关闭后（或幂等重放时）的实例状态。能力开关未开启、无权限、实例不在
    /// `MANUAL_INTERVENTION`、PAUSED 或 CAS 冲突返回错误。
    pub async fn manual_close(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
    ) -> anyhow::Result<SagaStatus> {
        self.manual_close_with_expectation(
            management,
            tenant,
            saga_id,
            operation_id,
            SagaManagementExpectation::default(),
        )
        .await
    }

    /// 业务作用：在事务内命中调用方版本后人工关闭自动化，避免旧快照终结已并发变化的实例。
    ///
    /// 参数说明：管理上下文、租户、实例和操作身份定位动作，`expectation` 约束事务内实例版本。
    ///
    /// 返回：关闭后或幂等重放状态；过期快照、权限、状态或持久化失败返回错误。
    pub async fn manual_close_with_expectation(
        &self,
        management: &SagaManagementContext,
        tenant: &TenantId,
        saga_id: &SagaId,
        operation_id: &str,
        expectation: SagaManagementExpectation,
    ) -> anyhow::Result<SagaStatus> {
        management.require(SagaManagementPermission::ManualClose)?;
        // 滚动升级门禁:新终态一旦落库不可回退,旧副本读到未知状态会按数据损坏停止推进。
        // 必须先全量部署可解析 MANUALLY_CLOSED 的读者,再由部署方开启本能力。
        if !self.config.enable_manual_close {
            return Err(management_precondition(
                "manual close is disabled: deploy MANUALLY_CLOSED-aware readers to every replica, then enable it explicitly",
            ));
        }
        let status = crate::transaction::run_for(&self.backend, async {
            let instance = self
                .backend
                .store()
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or(SagaManagementError::NotFound)?;
            // 审计先行且与迁移同事务:状态机对本边的放行证据就是这行审计,先写审计
            // 再 CAS 的顺序保证证据与迁移一起提交或一起消失。
            match self
                .backend
                .store()
                .record_management_operation(
                    saga_id,
                    operation_id,
                    nasaga_backend::MANUAL_CLOSE_ACTION,
                    management.actor(),
                    management.reason(),
                )
                .await?
            {
                // 同一 operation 已提交过关闭;重放直接返回当前状态,不再迁移。
                ManagementAuditOutcome::AlreadyRecorded => return Ok(instance.status),
                ManagementAuditOutcome::Recorded => {}
            }
            // 人工关闭的同一 operation 重放直接返回；只有新关闭事实才消耗额度。
            // 后续门禁或状态迁移失败会使审计和预算共同回滚。
            self.reserve_action_rate_budget(tenant).await?;
            // 先识别已提交 operation，保证丢失响应后的原请求可重放；新关闭动作仍必须
            // 命中调用方版本和 ACTIVE 控制态，否则审计插入随事务一起回滚。
            expectation.verify(instance.version, instance.control_version)?;
            if !instance.control_state.allows_automatic_actions() {
                return Err(management_precondition(
                    "manual_close requires ACTIVE control state; resume first",
                ));
            }
            if instance.status != SagaStatus::ManualIntervention {
                return Err(management_precondition(
                    "manual_close requires MANUAL_INTERVENTION",
                ));
            }
            self.advance(
                &instance,
                SagaStatus::ManuallyClosed,
                instance.direction,
                None,
                TriggerKind::Admin,
                operation_id,
                None,
                None,
            )
            .await?;
            // 终态后实例级 deadline 不再有业务含义;残留 timer 由 stale 吸收,但在此
            // 作废可让 due_timer 观测面立即回落。
            self.backend
                .store()
                .cancel_scope_timers(saga_id, TimerScope::Instance, None)
                .await?;
            Ok(SagaStatus::ManuallyClosed)
        })
        .await?;
        tracing::info!(
            saga_id = saga_id.as_str(),
            tenant = tenant.as_str(),
            actor = management.actor(),
            operation_id,
            saga_status = status.as_str(),
            "Saga 人工关闭操作已提交"
        );
        Ok(status)
    }

    // ---- 结果路由 ----

    /// 业务作用：实例已离开结果所属阶段时，只投影当前 attempt 的迟到事实，不自动推进状态机。
    ///
    /// 迟到事实仍会影响人工恢复与冻结计划；完全只写 attempt journal 会使
    /// `retry_compensation` 看不到已经成功的补偿。旧 attempt 则只能留在完整 journal 中，
    /// 不得覆盖较新 attempt 的步骤投影。
    ///
    /// 参数说明：
    /// - `context`: 已完成身份、状态和 attempt journal 校验的结果上下文。
    ///
    /// 返回：当前 attempt 的投影与对应 timeout 同事务更新后返回实例状态；旧 attempt
    /// 保持原状态；跨 phase 强事实矛盾时保留旧投影并把非终态实例提交到人工介入。
    /// 步骤骨架缺失或持久化失败返回错误并回滚 Inbox claim。
    async fn project_deferred_result(
        &self,
        context: &ResultContext<'_>,
    ) -> anyhow::Result<SagaStatus> {
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        let row = self
            .backend
            .store()
            .load_steps(saga_id)
            .await?
            .into_iter()
            .find(|row| row.step == *step)
            .ok_or_else(|| anyhow::anyhow!("step journal row is missing"))?;
        let current_attempt = match context.identity.phase {
            StepPhase::Execute => row.execute_attempt,
            StepPhase::Cancel => row.cancel_attempt,
            StepPhase::Compensate => row.compensate_attempt,
            StepPhase::Resolve => row.resolve_attempt,
        };
        if current_attempt != Some(context.identity.attempt.get()) {
            return Ok(context.instance.status);
        }

        let mut patch = StepJournalPatch {
            last_error_code: context.reason_code,
            ..Default::default()
        };
        let timeout_kind = match context.identity.phase {
            StepPhase::Execute => {
                patch.forward_status = Some(match context.status {
                    StepAttemptStatus::Succeeded => StepForwardStatus::Succeeded,
                    StepAttemptStatus::Rejected => StepForwardStatus::Rejected,
                    StepAttemptStatus::Unknown => StepForwardStatus::Unknown,
                    StepAttemptStatus::Halted => StepForwardStatus::Halted,
                    _ => anyhow::bail!("invalid deferred execute status"),
                });
                patch.mark_finished = !matches!(context.status, StepAttemptStatus::Unknown);
                KIND_STEP_TIMEOUT
            }
            StepPhase::Cancel => {
                match context.status {
                    StepAttemptStatus::CancelConfirmed => {
                        patch.forward_status = Some(StepForwardStatus::Cancelled);
                        patch.cancel_status = Some(StepCancelStatus::Confirmed);
                        patch.mark_finished = true;
                    }
                    StepAttemptStatus::AlreadyTerminal => {
                        patch.forward_status = context.terminal;
                        patch.cancel_status = Some(StepCancelStatus::AlreadyTerminal);
                        patch.mark_finished = true;
                    }
                    StepAttemptStatus::ResolutionPending => {
                        patch.cancel_status = Some(StepCancelStatus::ResolutionPending);
                    }
                    _ => anyhow::bail!("invalid deferred cancel status"),
                }
                KIND_CANCEL_TIMEOUT
            }
            StepPhase::Compensate => {
                patch.compensation_status = Some(match context.status {
                    StepAttemptStatus::Succeeded => StepCompensationStatus::Succeeded,
                    StepAttemptStatus::Unknown => StepCompensationStatus::Unknown,
                    StepAttemptStatus::Halted => StepCompensationStatus::Halted,
                    _ => anyhow::bail!("invalid deferred compensation status"),
                });
                KIND_COMPENSATE_TIMEOUT
            }
            StepPhase::Resolve => {
                patch.resolution_status = Some(match context.status {
                    StepAttemptStatus::Succeeded => StepResolutionStatus::Succeeded,
                    StepAttemptStatus::Rejected => StepResolutionStatus::Rejected,
                    StepAttemptStatus::Unknown => StepResolutionStatus::Pending,
                    StepAttemptStatus::Halted => StepResolutionStatus::Halted,
                    _ => anyhow::bail!("invalid deferred resolution status"),
                });
                if context.instance.direction == Direction::Forward {
                    patch.forward_status = match context.status {
                        StepAttemptStatus::Succeeded => Some(StepForwardStatus::Succeeded),
                        StepAttemptStatus::Rejected => Some(StepForwardStatus::Rejected),
                        StepAttemptStatus::Halted => Some(StepForwardStatus::Halted),
                        StepAttemptStatus::Unknown => Some(StepForwardStatus::Unknown),
                        _ => None,
                    };
                } else {
                    patch.compensation_status = match context.status {
                        StepAttemptStatus::Succeeded => Some(StepCompensationStatus::Succeeded),
                        StepAttemptStatus::Rejected => Some(StepCompensationStatus::Pending),
                        StepAttemptStatus::Halted => Some(StepCompensationStatus::Halted),
                        StepAttemptStatus::Unknown => Some(StepCompensationStatus::Unknown),
                        _ => None,
                    };
                }
                KIND_RESOLVE_TIMEOUT
            }
        };

        // 补偿方向的 Resolve=Rejected 只能发生在当前补偿尚未确认生效时；若投影已经
        // 是 Succeeded，说明两个 phase 给出了不可同时成立的事实，不能把成功补偿降回
        // Pending，否则恢复器会重复执行外部副作用。
        let resolve_after_compensation_success = context.identity.phase == StepPhase::Resolve
            && context.instance.direction == Direction::Compensating
            && context.status == StepAttemptStatus::Rejected
            && row.compensation_status == StepCompensationStatus::Succeeded
            && patch.compensation_status == Some(StepCompensationStatus::Pending);
        if resolve_after_compensation_success
            || merge_deferred_projection(&row, &mut patch).is_err()
        {
            // attempt 事实已经先记入统一 journal；这里不能返回错误让它随 Inbox 一起
            // 回滚成永久毒消息。先保留冲突双方证据，再将非终态实例同事务
            // 转人工等待核验；两者不得拆成两次 COMMIT，否则崩溃窗口会产生无证据 Manual。
            let existing = conflicting_projection_status(&row, context)
                .ok_or_else(|| anyhow::anyhow!("conflicting projection has no settled fact"))?;
            self.backend
                .store()
                .record_attempt_conflict(&AttemptConflictFact {
                    saga_id,
                    step,
                    phase: context.identity.phase,
                    attempt: context.identity.attempt,
                    existing_status: existing,
                    incoming_status: context.status,
                    incoming_event_id: context.event_id,
                    conflict_kind: SagaConflictKind::CrossPhaseFact,
                })
                .await?;
            if !context.instance.status.is_terminal()
                && context.instance.status != SagaStatus::ManualIntervention
            {
                return self
                    .escalate_manual(
                        context.instance,
                        TriggerKind::Event,
                        context.event_id,
                        "cross_phase_fact_conflict",
                    )
                    .await;
            }
            return Ok(context.instance.status);
        }
        self.backend
            .store()
            .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(timeout_kind))
            .await?;
        self.patch_step(saga_id, step, patch).await?;
        Ok(context.instance.status)
    }

    /// 业务作用：在当前阶段的热路径中单调合并 step 投影，防止后到 phase 改写强事实。
    ///
    /// attempt outcome 已由调用方先落统一 journal。投影互斥时，本函数在同一
    /// 事务留存冲突证据并冻结非终态实例，不返回错误导致 Inbox/证据回滚。
    ///
    /// 参数说明：
    /// - `context`: 当前结果、实例快照与 phase 身份。
    /// - `patch`: 热路径准备写入的阶段投影。
    ///
    /// 返回：单调合并并写入成功返回 `None`；冲突已留证并收敛时返回
    /// `Some(status)`，调用方必须立即停止路由；持久化或内部不变量失败返回错误。
    async fn apply_monotonic_projection(
        &self,
        context: &ResultContext<'_>,
        mut patch: StepJournalPatch<'_>,
    ) -> anyhow::Result<Option<SagaStatus>> {
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        let row = self
            .backend
            .store()
            .load_steps(saga_id)
            .await?
            .into_iter()
            .find(|row| row.step == *step)
            .ok_or_else(|| anyhow::anyhow!("step journal row is missing"))?;

        // Resolve 否定补偿发生与已确认的补偿成功不可并存；若改回 Pending，
        // 恢复器会再次执行同一外部撤销，因此必须作为跨 phase 冲突留证。
        let resolve_after_compensation_success = context.identity.phase == StepPhase::Resolve
            && context.instance.direction == Direction::Compensating
            && context.status == StepAttemptStatus::Rejected
            && row.compensation_status == StepCompensationStatus::Succeeded
            && patch.compensation_status == Some(StepCompensationStatus::Pending);
        if resolve_after_compensation_success
            || merge_deferred_projection(&row, &mut patch).is_err()
        {
            let existing = conflicting_projection_status(&row, context)
                .ok_or_else(|| anyhow::anyhow!("conflicting projection has no settled fact"))?;
            self.backend
                .store()
                .record_attempt_conflict(&AttemptConflictFact {
                    saga_id,
                    step,
                    phase: context.identity.phase,
                    attempt: context.identity.attempt,
                    existing_status: existing,
                    incoming_status: context.status,
                    incoming_event_id: context.event_id,
                    conflict_kind: SagaConflictKind::CrossPhaseFact,
                })
                .await?;
            if !context.instance.status.is_terminal()
                && context.instance.status != SagaStatus::ManualIntervention
            {
                let status = self
                    .escalate_manual(
                        context.instance,
                        TriggerKind::Event,
                        context.event_id,
                        "cross_phase_fact_conflict",
                    )
                    .await?;
                return Ok(Some(status));
            }
            return Ok(Some(context.instance.status));
        }
        self.patch_step(saga_id, step, patch).await?;
        Ok(None)
    }

    /// 业务作用：裁决 execute 阶段结果，驱动正向推进、进补偿或进解决通道。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态；协议违规或 CAS 冲突返回错误。
    async fn on_execute_result(&self, context: &ResultContext<'_>) -> anyhow::Result<SagaStatus> {
        // 迟到结果(实例已离开 RUNNING):journal 已记账,不绕过当前状态推进——
        // 取消屏障或解决通道会基于 journal 事实给出裁决。
        if context.instance.status != SagaStatus::Running
            || context.instance.current_step.as_ref() != Some(&context.identity.step)
        {
            return self.project_deferred_result(context).await;
        }
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        match context.status {
            StepAttemptStatus::Succeeded => {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        forward_status: Some(StepForwardStatus::Succeeded),
                        mark_finished: true,
                        ..Default::default()
                    },
                )
                .await?;
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_STEP_TIMEOUT))
                    .await?;
                self.proceed_forward(context).await
            }
            StepAttemptStatus::Rejected => {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        forward_status: Some(StepForwardStatus::Rejected),
                        last_error_code: context.reason_code,
                        mark_finished: true,
                        ..Default::default()
                    },
                )
                .await?;
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_STEP_TIMEOUT))
                    .await?;
                // 确定性拒绝已可靠提交,串行定义保证无在途未知命令:可直接进补偿。
                self.enter_compensation(context).await
            }
            StepAttemptStatus::Unknown => {
                self.enter_waiting_resolution(context, Direction::Forward)
                    .await
            }
            StepAttemptStatus::Halted => {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        forward_status: Some(StepForwardStatus::Halted),
                        last_error_code: context.reason_code,
                        mark_finished: true,
                        ..Default::default()
                    },
                )
                .await?;
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Step(step), None)
                    .await?;
                self.escalate_manual(
                    context.instance,
                    TriggerKind::Event,
                    context.event_id,
                    "step_halted",
                )
                .await
            }
            _ => anyhow::bail!("cancel-vocabulary status on an execute result"),
        }
    }

    /// 业务作用：裁决取消屏障结果——只有屏障裁决完成后才允许进入补偿。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态；缺失终态、协议违规或 CAS 冲突返回错误。
    async fn on_cancel_result(&self, context: &ResultContext<'_>) -> anyhow::Result<SagaStatus> {
        if context.instance.status != SagaStatus::Cancelling
            || context.instance.current_step.as_ref() != Some(&context.identity.step)
        {
            return self.project_deferred_result(context).await;
        }
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        match context.status {
            StepAttemptStatus::CancelConfirmed => {
                // admission fence 已建立,该步骤零正向效果:不纳入补偿集合。
                if let Some(status) = self
                    .apply_monotonic_projection(
                        context,
                        StepJournalPatch {
                            forward_status: Some(StepForwardStatus::Cancelled),
                            cancel_status: Some(StepCancelStatus::Confirmed),
                            mark_finished: true,
                            ..Default::default()
                        },
                    )
                    .await?
                {
                    return Ok(status);
                }
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_CANCEL_TIMEOUT))
                    .await?;
                self.enter_compensation(context).await
            }
            StepAttemptStatus::AlreadyTerminal => {
                let terminal = context.terminal.ok_or_else(|| {
                    anyhow::anyhow!("ALREADY_TERMINAL requires a terminal status")
                })?;
                if let Some(status) = self
                    .apply_monotonic_projection(
                        context,
                        StepJournalPatch {
                            forward_status: Some(terminal),
                            cancel_status: Some(StepCancelStatus::AlreadyTerminal),
                            mark_finished: true,
                            ..Default::default()
                        },
                    )
                    .await?
                {
                    return Ok(status);
                }
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_CANCEL_TIMEOUT))
                    .await?;
                match terminal {
                    StepForwardStatus::Succeeded => match context.step_def.timeout_policy() {
                        // 迟到成功按成功收敛:回正向或直接完成,绝不补偿成功的 pivot 上游。
                        TimeoutPolicy::AcceptLateSuccess => self.proceed_forward(context).await,
                        // 业务期限已过:成功效果按失效处理,纳入冻结计划撤销。
                        TimeoutPolicy::CompensateLateSuccess => {
                            self.enter_compensation(context).await
                        }
                    },
                    StepForwardStatus::Rejected => self.enter_compensation(context).await,
                    StepForwardStatus::Halted => {
                        self.escalate_manual(
                            context.instance,
                            TriggerKind::Event,
                            context.event_id,
                            "step_halted",
                        )
                        .await
                    }
                    _ => anyhow::bail!("ALREADY_TERMINAL carried a non-terminal forward status"),
                }
            }
            StepAttemptStatus::ResolutionPending => {
                // 取消不能证明外部效果未发生:进入解决通道,不谎报取消成功。
                if let Some(status) = self
                    .apply_monotonic_projection(
                        context,
                        StepJournalPatch {
                            cancel_status: Some(StepCancelStatus::ResolutionPending),
                            ..Default::default()
                        },
                    )
                    .await?
                {
                    return Ok(status);
                }
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_CANCEL_TIMEOUT))
                    .await?;
                self.enter_waiting_resolution(context, Direction::Forward)
                    .await
            }
            _ => anyhow::bail!("non-cancel status on a cancel result"),
        }
    }

    /// 业务作用：裁决补偿阶段结果，推进冻结计划或收敛终态。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态；协议违规或 CAS 冲突返回错误。
    async fn on_compensate_result(
        &self,
        context: &ResultContext<'_>,
    ) -> anyhow::Result<SagaStatus> {
        if context.instance.status != SagaStatus::Compensating
            || context.instance.current_step.as_ref() != Some(&context.identity.step)
        {
            return self.project_deferred_result(context).await;
        }
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        self.backend
            .store()
            .cancel_scope_timers(
                saga_id,
                TimerScope::Step(step),
                Some(KIND_COMPENSATE_TIMEOUT),
            )
            .await?;
        match context.status {
            StepAttemptStatus::Succeeded => {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        compensation_status: Some(StepCompensationStatus::Succeeded),
                        ..Default::default()
                    },
                )
                .await?;
                self.continue_compensation(context).await
            }
            StepAttemptStatus::Unknown => {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        compensation_status: Some(StepCompensationStatus::Unknown),
                        ..Default::default()
                    },
                )
                .await?;
                self.enter_waiting_resolution(context, Direction::Compensating)
                    .await
            }
            StepAttemptStatus::Halted => {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        compensation_status: Some(StepCompensationStatus::Halted),
                        last_error_code: context.reason_code,
                        ..Default::default()
                    },
                )
                .await?;
                self.escalate_manual(
                    context.instance,
                    TriggerKind::Event,
                    context.event_id,
                    "compensation_halted",
                )
                .await
            }
            _ => anyhow::bail!("invalid status on a compensate result"),
        }
    }

    /// 业务作用：在消费解决应答前复验步骤投影，让已经提交的正向或补偿成功优先于更早查询形成的迟到应答。
    ///
    /// resolve handler 可能先读取到未知事实，随后原业务 attempt 才提交成功，最终两个结果按相反顺序
    /// 到达 Orchestrator。attempt journal 会保留两条原始结论；步骤投影必须以已提交副作用为准，不能因
    /// 迟到的 `Halted` 或 `Rejected` 把真实成功冻结成人工介入。
    ///
    /// 参数说明：
    /// - `context`: 当前解决应答、实例快照、步骤合同与事件身份。
    ///
    /// 返回：发现已提交成功时写入单调解决投影、作废解决 timer，并返回推进后的状态；尚无成功事实时
    /// 返回 `None` 交给正常解决分支；投影冲突、CAS 或持久化失败返回错误。
    async fn settle_resolution_from_committed_success(
        &self,
        context: &ResultContext<'_>,
    ) -> anyhow::Result<Option<SagaStatus>> {
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        let row = self
            .backend
            .store()
            .load_steps(saga_id)
            .await?
            .into_iter()
            .find(|row| row.step == *step)
            .ok_or_else(|| anyhow::anyhow!("step journal row is missing"))?;
        let committed_success = match context.instance.direction {
            Direction::Forward => row.forward_status == StepForwardStatus::Succeeded,
            Direction::Compensating => row.compensation_status == StepCompensationStatus::Succeeded,
        };
        if !committed_success {
            return Ok(None);
        }

        let mut patch = StepJournalPatch {
            resolution_status: Some(StepResolutionStatus::Succeeded),
            ..Default::default()
        };
        match context.instance.direction {
            Direction::Forward => {
                patch.forward_status = Some(StepForwardStatus::Succeeded);
                patch.mark_finished = true;
            }
            Direction::Compensating => {
                patch.compensation_status = Some(StepCompensationStatus::Succeeded);
            }
        }
        if let Some(status) = self.apply_monotonic_projection(context, patch).await? {
            return Ok(Some(status));
        }

        // 业务成功事实和解决投影必须先在本事务固定，再作废重查预算并推进；否则下一轮 timer
        // 可能在状态迁移前再次发布查询，把已经收敛的副作用重新暴露给旧应答竞态。
        self.cancel_resolution_timers(saga_id, step).await?;
        let status = match context.instance.direction {
            Direction::Forward => match context.step_def.timeout_policy() {
                TimeoutPolicy::AcceptLateSuccess => self.proceed_forward(context).await?,
                TimeoutPolicy::CompensateLateSuccess => self.enter_compensation(context).await?,
            },
            Direction::Compensating => self.continue_compensation(context).await?,
        };
        Ok(Some(status))
    }

    /// 业务作用：裁决解决通道结果，按方向把唯一裁决送回正向或补偿推进。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态；协议违规或 CAS 冲突返回错误。
    async fn on_resolve_result(&self, context: &ResultContext<'_>) -> anyhow::Result<SagaStatus> {
        if context.instance.status != SagaStatus::WaitingResolution
            || context.instance.current_step.as_ref() != Some(&context.identity.step)
        {
            return self.project_deferred_result(context).await;
        }
        if let Some(status) = self
            .settle_resolution_from_committed_success(context)
            .await?
        {
            return Ok(status);
        }
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        match context.instance.direction {
            Direction::Forward => match context.status {
                StepAttemptStatus::Succeeded => {
                    if let Some(status) = self
                        .apply_monotonic_projection(
                            context,
                            StepJournalPatch {
                                forward_status: Some(StepForwardStatus::Succeeded),
                                resolution_status: Some(StepResolutionStatus::Succeeded),
                                mark_finished: true,
                                ..Default::default()
                            },
                        )
                        .await?
                    {
                        return Ok(status);
                    }
                    self.cancel_resolution_timers(saga_id, step).await?;
                    self.proceed_forward(context).await
                }
                StepAttemptStatus::Rejected => {
                    // 判 Rejected 的前提(排除迟到生效)由 resolve handler 保证;
                    // 这里只消费其唯一裁决。
                    if let Some(status) = self
                        .apply_monotonic_projection(
                            context,
                            StepJournalPatch {
                                forward_status: Some(StepForwardStatus::Rejected),
                                resolution_status: Some(StepResolutionStatus::Rejected),
                                last_error_code: context.reason_code,
                                mark_finished: true,
                                ..Default::default()
                            },
                        )
                        .await?
                    {
                        return Ok(status);
                    }
                    self.cancel_resolution_timers(saga_id, step).await?;
                    self.enter_compensation(context).await
                }
                StepAttemptStatus::Unknown => self.reissue_resolve(context).await,
                StepAttemptStatus::Halted => {
                    if let Some(status) = self
                        .apply_monotonic_projection(
                            context,
                            StepJournalPatch {
                                resolution_status: Some(StepResolutionStatus::Halted),
                                last_error_code: context.reason_code,
                                ..Default::default()
                            },
                        )
                        .await?
                    {
                        return Ok(status);
                    }
                    self.escalate_manual(
                        context.instance,
                        TriggerKind::Event,
                        context.event_id,
                        "resolution_halted",
                    )
                    .await
                }
                _ => anyhow::bail!("invalid status on a resolve result"),
            },
            Direction::Compensating => match context.status {
                StepAttemptStatus::Succeeded => {
                    // 裁决:补偿效果已真实发生。
                    if let Some(status) = self
                        .apply_monotonic_projection(
                            context,
                            StepJournalPatch {
                                compensation_status: Some(StepCompensationStatus::Succeeded),
                                resolution_status: Some(StepResolutionStatus::Succeeded),
                                ..Default::default()
                            },
                        )
                        .await?
                    {
                        return Ok(status);
                    }
                    self.cancel_resolution_timers(saga_id, step).await?;
                    self.continue_compensation(context).await
                }
                StepAttemptStatus::Rejected => {
                    // 裁决:补偿并未发生——重新排队补偿该步骤,新 attempt 复用同一 effect。
                    if let Some(status) = self
                        .apply_monotonic_projection(
                            context,
                            StepJournalPatch {
                                compensation_status: Some(StepCompensationStatus::Pending),
                                resolution_status: Some(StepResolutionStatus::Rejected),
                                ..Default::default()
                            },
                        )
                        .await?
                    {
                        return Ok(status);
                    }
                    self.cancel_resolution_timers(saga_id, step).await?;
                    let attempt = self
                        .next_attempt(saga_id, step, StepPhase::Compensate)
                        .await?;
                    if attempt.get() > self.config.compensate_max_attempts {
                        return self
                            .escalate_manual(
                                context.instance,
                                TriggerKind::Event,
                                context.event_id,
                                "compensation_attempts_exhausted",
                            )
                            .await;
                    }
                    let version = self
                        .advance(
                            context.instance,
                            SagaStatus::Compensating,
                            Direction::Compensating,
                            Some(step),
                            TriggerKind::Event,
                            context.event_id,
                            None,
                            None,
                        )
                        .await?;
                    self.issue_command(
                        context.instance,
                        context.step_def,
                        StepPhase::Compensate,
                        attempt,
                        None,
                        None,
                        context.now_ms,
                        version,
                    )
                    .await?;
                    Ok(SagaStatus::Compensating)
                }
                StepAttemptStatus::Unknown => self.reissue_resolve(context).await,
                StepAttemptStatus::Halted => {
                    if let Some(status) = self
                        .apply_monotonic_projection(
                            context,
                            StepJournalPatch {
                                resolution_status: Some(StepResolutionStatus::Halted),
                                last_error_code: context.reason_code,
                                ..Default::default()
                            },
                        )
                        .await?
                    {
                        return Ok(status);
                    }
                    self.escalate_manual(
                        context.instance,
                        TriggerKind::Event,
                        context.event_id,
                        "resolution_halted",
                    )
                    .await
                }
                _ => anyhow::bail!("invalid status on a resolve result"),
            },
        }
    }

    // ---- timer 裁决 ----

    /// 业务作用：step 超时裁决——可取消步骤先建屏障，resolve-only 步骤直接进入有界查询。
    ///
    /// 参数说明：
    /// - `instance`: 已提交实例快照。
    /// - `definition`: 实例固定版本的定义。
    /// - `step_scope`: timer 的步骤作用域。
    /// - `timer`: timer 行。
    /// - `now_ms`: 当前时刻。
    ///
    /// 返回：裁决生效返回新状态；timer 语义过时返回 `None`。
    async fn on_step_timeout(
        &self,
        instance: &SagaInstanceRow,
        definition: &WorkflowDefinition,
        step_scope: Option<&StepName>,
        timer: &SagaTimerRow,
        now_ms: i64,
    ) -> anyhow::Result<Option<SagaStatus>> {
        let Some(step) = step_scope else {
            anyhow::bail!("step-timeout timer without a step scope");
        };
        // 只有实例仍停在该步骤的正向执行上,超时才有效;否则结果已经到达,timer 过时。
        if instance.status != SagaStatus::Running || instance.current_step.as_ref() != Some(step) {
            return Ok(None);
        }
        let step_def = definition
            .step(step)
            .ok_or_else(|| anyhow::anyhow!("timer step absent from definition"))?;
        if step_def.cancel_mode() == CancelMode::ResolveOnly {
            // resolve-only 无法建立取消屏障；误发 cancel 只会让实例在无实现的通道耗尽重试。
            // 因此先发布 Unknown/解决投影，再进入带硬期限的查询通道，绝不因超时直接补偿。
            self.patch_step(
                &instance.saga_id,
                step,
                StepJournalPatch {
                    forward_status: Some(StepForwardStatus::Unknown),
                    resolution_status: Some(StepResolutionStatus::Pending),
                    ..Default::default()
                },
            )
            .await?;
            let version = self
                .advance(
                    instance,
                    SagaStatus::WaitingResolution,
                    Direction::Forward,
                    Some(step),
                    TriggerKind::Timer,
                    &timer.timer_id,
                    None,
                    None,
                )
                .await?;
            let resolution = step_def.resolution();
            self.schedule_timer(
                &instance.saga_id,
                TimerScope::Step(step),
                resolution_budget_kind(Direction::Forward),
                timer.attempt,
                checked_deadline(now_ms, duration_millis(resolution.budget())?)?,
                version,
            )
            .await?;
            if resolution.mode() == Some(ResolutionMode::Poll) {
                let attempt = self
                    .next_attempt(&instance.saga_id, step, StepPhase::Resolve)
                    .await?;
                self.issue_command(
                    instance,
                    step_def,
                    StepPhase::Resolve,
                    attempt,
                    None,
                    None,
                    now_ms,
                    version,
                )
                .await?;
            }
            return Ok(Some(SagaStatus::WaitingResolution));
        }

        // 可取消步骤的超时只说明结果未知：标 TIMED_OUT 后先建取消屏障，由参与方真实裁决，
        // 绝不能直接补偿并与可能迟到生效的外部效果并存。
        self.patch_step(
            &instance.saga_id,
            step,
            StepJournalPatch {
                forward_status: Some(StepForwardStatus::TimedOut),
                ..Default::default()
            },
        )
        .await?;
        let version = self
            .advance(
                instance,
                SagaStatus::Cancelling,
                Direction::Forward,
                Some(step),
                TriggerKind::Timer,
                &timer.timer_id,
                None,
                None,
            )
            .await?;
        let attempt = self
            .next_attempt(&instance.saga_id, step, StepPhase::Cancel)
            .await?;
        self.issue_command(
            instance,
            step_def,
            StepPhase::Cancel,
            attempt,
            None,
            None,
            now_ms,
            version,
        )
        .await?;
        Ok(Some(SagaStatus::Cancelling))
    }

    /// 业务作用：cancel/compensate/resolve 命令的有界重发裁决；预算耗尽升级人工介入。
    ///
    /// 参数说明：
    /// - `instance`: 已提交实例快照。
    /// - `definition`: 实例固定版本的定义。
    /// - `step_scope`: timer 的步骤作用域。
    /// - `timer`: timer 行。
    /// - `phase`: 重发的命令阶段。
    /// - `required_status`: 该 timer 语义有效所要求的实例状态。
    /// - `max_attempts`: 阶段预算上限。
    /// - `exhausted_code`: 预算耗尽时写入的稳定失败码。
    /// - `now_ms`: 当前时刻。
    ///
    /// 返回：重发或升级后返回新状态；timer 语义过时返回 `None`。
    #[allow(clippy::too_many_arguments)]
    async fn on_phase_retry_timeout(
        &self,
        instance: &SagaInstanceRow,
        definition: &WorkflowDefinition,
        step_scope: Option<&StepName>,
        timer: &SagaTimerRow,
        phase: StepPhase,
        required_status: SagaStatus,
        max_attempts: u32,
        exhausted_code: &str,
        now_ms: i64,
    ) -> anyhow::Result<Option<SagaStatus>> {
        let Some(step) = step_scope else {
            anyhow::bail!("phase retry timer without a step scope");
        };
        if instance.status != required_status || instance.current_step.as_ref() != Some(step) {
            return Ok(None);
        }
        let step_def = definition
            .step(step)
            .ok_or_else(|| anyhow::anyhow!("timer step absent from definition"))?;
        let exhausted = if phase == StepPhase::Resolve {
            // resolve attempt 号跨正向/补偿全局单调，但预算必须按当前业务方向计数；
            // 否则正向用完十次查询后，补偿方向第一次 timeout 会被误判耗尽。
            self.backend
                .store()
                .count_resolution_attempts(&instance.saga_id, step, instance.direction)
                .await?
                >= max_attempts
        } else {
            self.next_attempt(&instance.saga_id, step, phase)
                .await?
                .get()
                > max_attempts
        };
        // 有界重发:同一 effect 下新 attempt/new command;预算耗尽即冻结转人工,
        // 自动状态不允许无限滞留,也不伪造终态。
        if exhausted {
            return Ok(Some(
                self.escalate_manual(
                    instance,
                    TriggerKind::Timer,
                    &timer.timer_id,
                    exhausted_code,
                )
                .await?,
            ));
        }
        // 方向预算裁决完成后再分配全局单调 attempt；身份序号不能按方向复用。
        let attempt = self.next_attempt(&instance.saga_id, step, phase).await?;
        let version = self
            .advance(
                instance,
                required_status,
                instance.direction,
                Some(step),
                TriggerKind::Timer,
                &timer.timer_id,
                None,
                None,
            )
            .await?;
        self.issue_command(
            instance, step_def, phase, attempt, None, None, now_ms, version,
        )
        .await?;
        Ok(Some(required_status))
    }

    /// 业务作用：实例级 deadline 裁决——FORWARD 段走取消屏障，其余段只升级人工介入。
    ///
    /// 参数说明：
    /// - `instance`: 已提交实例快照。
    /// - `definition`: 实例固定版本的定义。
    /// - `timer`: timer 行。
    /// - `now_ms`: 当前时刻。
    ///
    /// 返回：裁决生效返回新状态；终态/人工介入下 timer 过时返回 `None`。
    async fn on_instance_deadline(
        &self,
        instance: &SagaInstanceRow,
        definition: &WorkflowDefinition,
        timer: &SagaTimerRow,
        now_ms: i64,
    ) -> anyhow::Result<Option<SagaStatus>> {
        match instance.status {
            // FORWARD 段:对当前步骤建取消屏障,后续按 timeout policy 收敛。
            SagaStatus::Running => {
                let Some(step) = instance.current_step.clone() else {
                    return Ok(None);
                };
                self.on_step_timeout(instance, definition, Some(&step), timer, now_ms)
                    .await
            }
            // 屏障/解决/补偿段:只升级告警与人工介入,不向执行器伪造"已取消",
            // 也不抹掉已经发出的取消、查询或补偿。
            SagaStatus::Cancelling | SagaStatus::WaitingResolution | SagaStatus::Compensating => {
                Ok(Some(
                    self.escalate_manual(
                        instance,
                        TriggerKind::Timer,
                        &timer.timer_id,
                        "deadline_exceeded",
                    )
                    .await?,
                ))
            }
            _ => Ok(None),
        }
    }

    // ---- 推进原语 ----

    /// 业务作用：正向收敛——推进到下一 PENDING 步骤或完成实例。
    ///
    /// CAS 的 from 取自已提交实例快照（Running 自身、取消屏障迟到成功回正向、
    /// 解决确认成功回正向三条入口共用）。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态。
    async fn proceed_forward(&self, context: &ResultContext<'_>) -> anyhow::Result<SagaStatus> {
        let saga_id = &context.identity.saga_id;
        let steps = self.backend.store().load_steps(saga_id).await?;
        match next_pending_forward(context.definition, &steps)? {
            Some(next) => {
                let version = self
                    .advance(
                        context.instance,
                        SagaStatus::Running,
                        Direction::Forward,
                        Some(&next),
                        TriggerKind::Event,
                        context.event_id,
                        None,
                        None,
                    )
                    .await?;
                let next_def = context
                    .definition
                    .step(&next)
                    .ok_or_else(|| anyhow::anyhow!("next step absent from definition"))?;
                let attempt = self
                    .next_attempt(saga_id, &next, StepPhase::Execute)
                    .await?;
                // 后续步骤命令只携带身份:业务输入由参与方按 saga_id + step 从本地事实解析。
                self.issue_command(
                    context.instance,
                    next_def,
                    StepPhase::Execute,
                    attempt,
                    None,
                    None,
                    context.now_ms,
                    version,
                )
                .await?;
                Ok(SagaStatus::Running)
            }
            None => {
                self.advance(
                    context.instance,
                    SagaStatus::Completed,
                    Direction::Forward,
                    None,
                    TriggerKind::Event,
                    context.event_id,
                    None,
                    None,
                )
                .await?;
                // 终态后实例级 deadline 不再有意义,同事务作废。
                self.backend
                    .store()
                    .cancel_scope_timers(saga_id, TimerScope::Instance, None)
                    .await?;
                Ok(SagaStatus::Completed)
            }
        }
    }

    /// 业务作用：冻结补偿计划并进入 `COMPENSATING`；空计划立即收敛为 `COMPENSATED`。
    ///
    /// 冻结、CAS 迁移、计划标记与首条补偿命令在同一事务提交，保证"计划已冻结"与
    /// "成员已标记"不可分割。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态；journal 存在未裁决步骤或成功 pivot 时冻结被 core
    /// 拒绝——按不变量破坏升级人工介入，绝不让该消息在重投里热循环。
    async fn enter_compensation(&self, context: &ResultContext<'_>) -> anyhow::Result<SagaStatus> {
        let saga_id = &context.identity.saga_id;
        let steps = self.backend.store().load_steps(saga_id).await?;
        let journal: Vec<StepJournalEntry> = steps
            .iter()
            .map(|row| StepJournalEntry::new(row.step.clone(), row.forward_status))
            .collect();
        // 冻结被拒 = 不变量已破坏(未裁决步骤/已成功 pivot):这不是可重试故障,
        // 回滚重投只会热循环;唯一安全出路是提交人工介入冻结并 ACK。
        let plan = match freeze_compensation_plan(context.definition, &journal) {
            Ok(plan) => plan,
            Err(violation) => {
                return self
                    .escalate_manual(
                        context.instance,
                        TriggerKind::Event,
                        context.event_id,
                        violation.code(),
                    )
                    .await;
            }
        };
        let first = plan.entries().first().map(|entry| entry.step().clone());
        let version = self
            .advance(
                context.instance,
                SagaStatus::Compensating,
                Direction::Compensating,
                first.as_ref(),
                TriggerKind::Event,
                context.event_id,
                None,
                Some(plan.version()),
            )
            .await?;
        self.backend
            .store()
            .mark_compensation_plan(saga_id, &plan)
            .await?;
        match first {
            Some(step) => {
                let step_def = context
                    .definition
                    .step(&step)
                    .ok_or_else(|| anyhow::anyhow!("plan step absent from definition"))?;
                let attempt = self
                    .next_attempt(saga_id, &step, StepPhase::Compensate)
                    .await?;
                self.issue_command(
                    context.instance,
                    step_def,
                    StepPhase::Compensate,
                    attempt,
                    None,
                    None,
                    context.now_ms,
                    version,
                )
                .await?;
                Ok(SagaStatus::Compensating)
            }
            None => {
                // 首步即拒绝/取消时计划为空:COMPENSATING 立即收敛,不引入额外终态。
                self.converge_compensated(
                    context.instance,
                    version,
                    TriggerKind::Event,
                    context.event_id,
                )
                .await?;
                Ok(SagaStatus::Compensated)
            }
        }
    }

    /// 业务作用：补偿计划推进——发出下一计划项或收敛为 `COMPENSATED`。
    ///
    /// 关键顺序：**先**从已提交 journal 找出下一计划项，**再**以它作为 `current_step`
    /// 执行 CAS 迁移——`current_step` 与在途补偿命令必须一致，否则该命令的
    /// compensate-timeout 会被语义校验判为过时，补偿失去超时保护而无限滞留。
    /// 补偿结果与解决通道确认补偿成功两条入口共用（from 分别为 Compensating 与
    /// WaitingResolution，均为合法边）。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态。
    async fn continue_compensation(
        &self,
        context: &ResultContext<'_>,
    ) -> anyhow::Result<SagaStatus> {
        let saga_id = &context.identity.saga_id;
        let steps = self.backend.store().load_steps(saga_id).await?;
        let next =
            match next_pending_compensation(context.instance, context.definition, &steps, false) {
                Ok(next) => next,
                Err(_) => {
                    // 冻结计划在执行中漂移时绝不能继续发补偿命令；先保留本次真实结果，
                    // 再同事务冻结人工介入，避免错误计划扩大副作用面。
                    return self
                        .escalate_manual(
                            context.instance,
                            TriggerKind::Event,
                            context.event_id,
                            "compensation_plan_drift",
                        )
                        .await;
                }
            };
        let version = self
            .advance(
                context.instance,
                SagaStatus::Compensating,
                Direction::Compensating,
                next.as_ref(),
                TriggerKind::Event,
                context.event_id,
                None,
                None,
            )
            .await?;
        match next {
            Some(step) => {
                let step_def = context
                    .definition
                    .step(&step)
                    .ok_or_else(|| anyhow::anyhow!("plan step absent from definition"))?;
                let attempt = self
                    .next_attempt(saga_id, &step, StepPhase::Compensate)
                    .await?;
                self.issue_command(
                    context.instance,
                    step_def,
                    StepPhase::Compensate,
                    attempt,
                    None,
                    None,
                    context.now_ms,
                    version,
                )
                .await?;
                Ok(SagaStatus::Compensating)
            }
            None => {
                self.converge_compensated_at(
                    saga_id,
                    version,
                    TriggerKind::Event,
                    context.event_id,
                )
                .await?;
                Ok(SagaStatus::Compensated)
            }
        }
    }

    /// 业务作用：进入 `WAITING_RESOLUTION` 并布置解决预算与查询通道。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    /// - `direction`: 解决完成后应回到的推进方向。
    ///
    /// 返回：提交后的实例状态；definition 不允许 Unknown 时升级人工介入。
    async fn enter_waiting_resolution(
        &self,
        context: &ResultContext<'_>,
        direction: Direction,
    ) -> anyhow::Result<SagaStatus> {
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        let resolution = context.step_def.resolution();
        // 纯本地步骤不允许 Unknown:参与方本应提交 Halted;走到这里说明 descriptor
        // 与 definition 漂移,按合同违规冻结转人工,不能继续正向推进。
        if !resolution.allow_unknown() {
            if context.identity.phase == StepPhase::Execute {
                self.patch_step(
                    saga_id,
                    step,
                    StepJournalPatch {
                        forward_status: Some(StepForwardStatus::Unknown),
                        ..Default::default()
                    },
                )
                .await?;
            }
            // compensation/cancel 的 Unknown 已由各自阶段列留证；绝不能把已确认的
            // forward=SUCCEEDED 覆盖成 UNKNOWN，否则人工恢复会失去“确有待撤销效果”的证明。
            return self
                .escalate_manual(
                    context.instance,
                    TriggerKind::Event,
                    context.event_id,
                    "unknown_not_allowed",
                )
                .await;
        }
        if context.identity.phase == StepPhase::Execute {
            self.patch_step(
                saga_id,
                step,
                StepJournalPatch {
                    forward_status: Some(StepForwardStatus::Unknown),
                    resolution_status: Some(StepResolutionStatus::Pending),
                    ..Default::default()
                },
            )
            .await?;
        } else {
            self.patch_step(
                saga_id,
                step,
                StepJournalPatch {
                    resolution_status: Some(StepResolutionStatus::Pending),
                    ..Default::default()
                },
            )
            .await?;
        }
        self.backend
            .store()
            .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_STEP_TIMEOUT))
            .await?;
        let version = self
            .advance(
                context.instance,
                SagaStatus::WaitingResolution,
                direction,
                Some(step),
                TriggerKind::Event,
                context.event_id,
                None,
                None,
            )
            .await?;
        // Unknown 不是可以永久停留的状态:解决预算是硬期限,超期升级人工介入。
        self.schedule_timer(
            saga_id,
            TimerScope::Step(step),
            resolution_budget_kind(direction),
            context.identity.attempt,
            checked_deadline(context.now_ms, duration_millis(resolution.budget())?)?,
            version,
        )
        .await?;
        if resolution.mode() == Some(ResolutionMode::Poll) {
            let attempt = self.next_attempt(saga_id, step, StepPhase::Resolve).await?;
            self.issue_command(
                context.instance,
                context.step_def,
                StepPhase::Resolve,
                attempt,
                None,
                None,
                context.now_ms,
                version,
            )
            .await?;
        }
        Ok(SagaStatus::WaitingResolution)
    }

    /// 业务作用：解决结果仍为 Unknown 时的有界重查；预算耗尽升级人工介入。
    ///
    /// 参数说明：
    /// - `context`: 结果裁决上下文。
    ///
    /// 返回：提交后的实例状态。
    async fn reissue_resolve(&self, context: &ResultContext<'_>) -> anyhow::Result<SagaStatus> {
        let saga_id = &context.identity.saga_id;
        let step = &context.identity.step;
        let used = self
            .backend
            .store()
            .count_resolution_attempts(saga_id, step, context.instance.direction)
            .await?;
        if used >= self.config.resolve_max_attempts {
            return self
                .escalate_manual(
                    context.instance,
                    TriggerKind::Event,
                    context.event_id,
                    "resolve_attempts_exhausted",
                )
                .await;
        }
        // attempt 序号仍在 resolve phase 内全局单调，用于 command/effect 身份；
        // 预算则依据上面的方向计数，不得把正向查询次数扣到补偿上。
        let attempt = self.next_attempt(saga_id, step, StepPhase::Resolve).await?;
        let version = self
            .advance(
                context.instance,
                SagaStatus::WaitingResolution,
                context.instance.direction,
                Some(step),
                TriggerKind::Event,
                context.event_id,
                None,
                None,
            )
            .await?;
        self.issue_command(
            context.instance,
            context.step_def,
            StepPhase::Resolve,
            attempt,
            None,
            None,
            context.now_ms,
            version,
        )
        .await?;
        Ok(SagaStatus::WaitingResolution)
    }

    /// 业务作用：升级人工介入——停止发出新的自动命令，冻结待人工裁决。
    ///
    /// 参数说明：
    /// - `instance`: 已提交实例快照。
    /// - `trigger_kind`: 触发来源。
    /// - `trigger_id`: 触发身份。
    /// - `failure_code`: 稳定失败原因码。
    ///
    /// 返回：`ManualIntervention`；CAS 冲突返回错误。
    async fn escalate_manual(
        &self,
        instance: &SagaInstanceRow,
        trigger_kind: TriggerKind,
        trigger_id: &str,
        failure_code: &str,
    ) -> anyhow::Result<SagaStatus> {
        self.advance(
            instance,
            SagaStatus::ManualIntervention,
            instance.direction,
            instance.current_step.as_ref(),
            trigger_kind,
            trigger_id,
            Some(failure_code),
            None,
        )
        .await?;
        Ok(SagaStatus::ManualIntervention)
    }

    /// 业务作用：把已处于 `COMPENSATING` 的实例收敛为 `COMPENSATED` 终态。
    ///
    /// 参数说明：
    /// - `instance`: 迁入 `COMPENSATING` 前的实例快照（saga_id 取自此处）。
    /// - `version`: 当前（`COMPENSATING`）实例版本。
    /// - `cause_id`: 引发收敛的触发身份；派生定长 UUID 保持触发唯一键不冲突。
    ///
    /// 返回：收敛成功返回 `Ok`；CAS 冲突返回错误。
    async fn converge_compensated(
        &self,
        instance: &SagaInstanceRow,
        version: u64,
        trigger_kind: TriggerKind,
        cause_id: &str,
    ) -> anyhow::Result<()> {
        self.converge_compensated_at(&instance.saga_id, version, trigger_kind, cause_id)
            .await
    }

    /// 业务作用：`converge_compensated` 的按身份变体，供推进后没有完整快照的路径使用。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `version`: 当前（`COMPENSATING`）实例版本。
    /// - `cause_id`: 引发收敛的触发身份。
    ///
    /// 返回：收敛成功返回 `Ok`；CAS 冲突返回错误。
    async fn converge_compensated_at(
        &self,
        saga_id: &SagaId,
        version: u64,
        trigger_kind: TriggerKind,
        cause_id: &str,
    ) -> anyhow::Result<()> {
        // COMPENSATED 是不可逆业务声明；迁移前必须从已提交 journal 重建原计划，
        // 并逐项证明计划摘要、顺序和成功状态一致，不能仅凭“没有 PENDING 行”推断完成。
        let definition_version = self
            .verify_compensation_completion(saga_id, version)
            .await?;
        // 同一原因引发两次迁移(进入补偿 + 立即收敛):第二次使用确定性 UUID，
        // 既保住"同一触发只推进一次"，也不会让接近索引上限的 cause id 拼接后越界。
        let converge_trigger = derive_converge_trigger_id(cause_id);
        let outcome = self
            .backend
            .store()
            .advance(
                saga_id,
                version,
                SagaStatus::Compensating,
                &TransitionSpec {
                    to_status: SagaStatus::Compensated,
                    direction: Direction::Compensating,
                    current_step: None,
                    trigger_kind,
                    trigger_id: &converge_trigger,
                    definition_version,
                    failure_code: None,
                    compensation_plan_version: None,
                },
            )
            .await?;
        let _ = ensure_applied(outcome)?;
        self.backend
            .store()
            .cancel_scope_timers(saga_id, TimerScope::Instance, None)
            .await?;
        Ok(())
    }

    /// 业务作用：在写入 `COMPENSATED` 前证明冻结计划未漂移且每个计划成员都已补偿成功。
    ///
    /// 参数说明：
    /// - `saga_id`: 待收敛实例身份。
    /// - `expected_version`: 调用方刚推进到 `COMPENSATING` 后持有的实例版本。
    ///
    /// 返回：全部证据成立时返回 definition 版本；状态、版本、计划摘要、成员顺序或成员终态
    /// 任一不一致时返回错误并回滚当前事务，绝不发布假 `COMPENSATED` 终态。
    async fn verify_compensation_completion(
        &self,
        saga_id: &SagaId,
        expected_version: u64,
    ) -> anyhow::Result<DefinitionVersion> {
        let instance = self
            .backend
            .store()
            .load_instance(saga_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("saga instance not found while converging"))?;
        if instance.status != SagaStatus::Compensating || instance.version != expected_version {
            anyhow::bail!("compensation completion proof used a stale instance snapshot");
        }
        let frozen_version = instance
            .compensation_plan_version
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("compensating instance has no frozen plan"))?;
        let registry = self.registry_snapshot();
        let definition = registry
            .get_for_tenant(
                &instance.tenant,
                &instance.workflow,
                instance.definition_version,
            )
            .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;
        let steps = self.backend.store().load_steps(saga_id).await?;
        let journal: Vec<StepJournalEntry> = steps
            .iter()
            .map(|row| StepJournalEntry::new(row.step.clone(), row.forward_status))
            .collect();
        let rebuilt = freeze_compensation_plan(definition, &journal)
            .map_err(|violation| anyhow::anyhow!("{}", violation.code()))?;
        if rebuilt.version() != frozen_version {
            anyhow::bail!("frozen compensation plan no longer matches durable forward facts");
        }
        for entry in rebuilt.entries() {
            let row = steps
                .iter()
                .find(|row| row.step == *entry.step())
                .ok_or_else(|| anyhow::anyhow!("frozen compensation member is missing"))?;
            if row.compensation_plan_version.as_deref() != Some(frozen_version)
                || row.compensation_order != Some(entry.order())
                || row.compensation_status != StepCompensationStatus::Succeeded
            {
                anyhow::bail!("frozen compensation member is not durably succeeded");
            }
        }
        let marked_count = steps
            .iter()
            .filter(|row| row.compensation_plan_version.as_deref() == Some(frozen_version))
            .count();
        if marked_count != rebuilt.entries().len() {
            anyhow::bail!("durable compensation plan contains unexpected members");
        }
        Ok(instance.definition_version)
    }

    /// 业务作用：以实例快照的既有状态为预期执行一次 CAS 推进。
    ///
    /// 参数说明：
    /// - `instance`: 已提交实例快照（version/status 作为 CAS 预期）。
    /// - `to`: 目标状态。
    /// - `direction`: 推进后的方向。
    /// - `current_step`: 推进后的当前步骤。
    /// - `trigger_kind`/`trigger_id`: 触发身份。
    /// - `failure_code`: 稳定失败码；为空保留既有值。
    /// - `plan_version`: 冻结计划摘要；为空保留既有值。
    ///
    /// 返回：推进后的实例版本；CAS 冲突或重复触发返回错误（事务回滚重新裁决）。
    #[allow(clippy::too_many_arguments)]
    async fn advance(
        &self,
        instance: &SagaInstanceRow,
        to: SagaStatus,
        direction: Direction,
        current_step: Option<&StepName>,
        trigger_kind: TriggerKind,
        trigger_id: &str,
        failure_code: Option<&str>,
        plan_version: Option<&str>,
    ) -> anyhow::Result<u64> {
        self.advance_from(
            instance,
            instance.status,
            to,
            direction,
            current_step,
            trigger_kind,
            trigger_id,
            failure_code,
            plan_version,
        )
        .await
    }

    /// 业务作用：`advance` 的显式 from 变体，供裁决入口已知快照状态的路径复用。
    ///
    /// 参数说明：同 [`Orchestrator::advance`]，另有
    /// - `from`: CAS 预期的当前状态。
    ///
    /// 返回：推进后的实例版本；CAS 冲突或重复触发返回错误。
    #[allow(clippy::too_many_arguments)]
    async fn advance_from(
        &self,
        instance: &SagaInstanceRow,
        from: SagaStatus,
        to: SagaStatus,
        direction: Direction,
        current_step: Option<&StepName>,
        trigger_kind: TriggerKind,
        trigger_id: &str,
        failure_code: Option<&str>,
        plan_version: Option<&str>,
    ) -> anyhow::Result<u64> {
        let outcome = self
            .backend
            .store()
            .advance(
                &instance.saga_id,
                instance.version,
                from,
                &TransitionSpec {
                    to_status: to,
                    direction,
                    current_step,
                    trigger_kind,
                    trigger_id,
                    definition_version: instance.definition_version,
                    failure_code,
                    compensation_plan_version: plan_version,
                },
            )
            .await?;
        ensure_applied(outcome)
    }

    /// 业务作用：登记 attempt、写命令 Outbox 并布置阶段超时 timer——命令发出的原子三件套。
    ///
    /// 参数说明：
    /// - `instance`: 实例快照（身份与 definition 绑定来源）。
    /// - `step_def`: 目标步骤定义。
    /// - `phase`: 命令阶段。
    /// - `attempt`: 尝试序号。
    /// - `payload`: 已校验的业务输入及原始输入形式；仅首步 execute 携带。
    /// - `recovery_operation_id`: 已审计人工恢复身份；普通自动命令为空。
    /// - `now_ms`: 当前时刻。
    /// - `expected_version`: timer 登记的实例版本（推进后的版本）。
    ///
    /// 返回：三件套全部落库返回 `Ok`；任一失败返回错误使整个事务回滚。
    #[allow(clippy::too_many_arguments)]
    async fn issue_command(
        &self,
        instance: &SagaInstanceRow,
        step_def: &StepDefinition,
        phase: StepPhase,
        attempt: AttemptNo,
        payload: Option<CommandInput>,
        recovery_operation_id: Option<&str>,
        now_ms: i64,
        expected_version: u64,
    ) -> anyhow::Result<()> {
        let step = step_def.name();
        let effect = EffectId::derive(&instance.saga_id, instance.definition_version, step, phase);
        let command = CommandId::derive(&effect, attempt);
        // journal 先占身份,再写命令:结果事件永远能找到去重锚点。
        self.backend
            .store()
            .record_attempt_started(&instance.saga_id, step, phase, attempt, &effect, &command)
            .await?;
        // 兼容输入继续写原有 JSON 字段；显式 bytes 输入只写 raw_payload，不能丢失字节或 schema。
        let (payload, raw_payload) = match payload {
            Some(CommandInput::Json(value)) => (Some(value), None),
            Some(CommandInput::Raw(value)) => (None, Some(value)),
            None => (None, None),
        };
        let envelope = SagaCommandEnvelope {
            saga_id: instance.saga_id.as_str().to_string(),
            tenant_id: instance.tenant.as_str().to_string(),
            workflow: instance.workflow.as_str().to_string(),
            definition_version: instance.definition_version.get(),
            definition_digest: instance.definition_digest.clone(),
            step: step.as_str().to_string(),
            phase: phase.as_str().to_string(),
            attempt: attempt.get(),
            effect_id: effect.to_string(),
            command_id: command.to_string(),
            recovery_operation_id: recovery_operation_id.map(str::to_owned),
            payload,
            raw_payload,
        };
        // 命令继承实例已提交的因果上下文并派生新 span:同一 trace-id 串联发起入口、
        // 编排端与参与方,每一跳都是新的 child;列值损坏或缺失时按无上下文投递——
        // trace 是观测面,不阻塞业务命令。
        let event = match instance
            .traceparent
            .as_deref()
            .and_then(TraceContext::parse_traceparent)
        {
            Some(base) => envelope
                .to_outbox_event()?
                .with_traceparent(base.child(natelemetry::random_span_id()).to_traceparent()),
            None => envelope.to_outbox_event()?,
        };
        // 关键双写:命令进 Outbox 必须与状态迁移同事务,事务外 autocommit 被拒绝。
        // 租户归因取已提交实例身份(受信写入上下文),供 Outbox 层配额与对账使用;
        // 绝不从命令 payload 解析租户。
        let write_context = naoutbox_core::OutboxWriteContext::new(instance.tenant.as_str())
            .map_err(|reason| anyhow::anyhow!("outbox write context rejected: {reason}"))?;
        self.backend
            .outbox()
            .append_transactional_with_context(&write_context, &event)
            .await?;
        // 命令与它的超时保护同生:写命令的事务同时写 timer,步骤不会无限滞留。
        let timeout_ms = duration_millis(step_def.timeout())?;
        self.schedule_timer(
            &instance.saga_id,
            TimerScope::Step(step),
            phase_timeout_kind(phase),
            attempt,
            checked_deadline(now_ms, timeout_ms)?,
            expected_version,
        )
        .await?;
        Ok(())
    }

    /// 业务作用：以确定性派生的 timer 身份调度 durable timer。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `scope`: timer 作用域。
    /// - `kind`: timer 种类。
    /// - `attempt`: 关联尝试序号。
    /// - `due_at_ms`: 到期时刻。
    /// - `expected_version`: 调度时刻的实例版本。
    ///
    /// 返回：调度或幂等命中返回 `Ok`；底层失败返回错误。
    async fn schedule_timer(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: &str,
        attempt: AttemptNo,
        due_at_ms: i64,
        expected_version: u64,
    ) -> anyhow::Result<()> {
        let timer_id = derive_timer_id(saga_id, &scope, kind, attempt);
        self.backend
            .store()
            .schedule_timer(&TimerSpec {
                timer_id: &timer_id,
                saga_id,
                scope,
                kind,
                due_at_ms,
                attempt,
                expected_saga_version: expected_version,
            })
            .await?;
        Ok(())
    }

    /// 业务作用：作废某步骤的解决类 timer（预算 + 重查），在解决收敛后调用。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    ///
    /// 返回：作废完成返回 `Ok`。
    async fn cancel_resolution_timers(
        &self,
        saga_id: &SagaId,
        step: &StepName,
    ) -> anyhow::Result<()> {
        // 滚动升级期可能同时存在旧版无方向 kind 与新版双方向 kind；终态提交前必须
        // 全部作废，否则旧预算会在后续补偿阶段误把实例升级人工介入。
        for kind in [
            KIND_RESOLUTION_BUDGET,
            KIND_FORWARD_RESOLUTION_BUDGET,
            KIND_COMPENSATION_RESOLUTION_BUDGET,
        ] {
            self.backend
                .store()
                .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(kind))
                .await?;
        }
        self.backend
            .store()
            .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(KIND_RESOLVE_TIMEOUT))
            .await?;
        Ok(())
    }

    /// 业务作用：更新 step journal 投影的便捷入口。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `patch`: 投影补丁。
    ///
    /// 返回：更新成功返回 `Ok`。
    async fn patch_step(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        patch: StepJournalPatch<'_>,
    ) -> anyhow::Result<()> {
        self.backend
            .store()
            .update_step_journal(saga_id, step, &patch)
            .await?;
        Ok(())
    }

    /// 业务作用：由 attempt journal 推导某阶段的下一尝试序号。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `phase`: 阶段。
    ///
    /// 返回：journal 无该阶段记录返回 1；否则返回最大序号加一。
    async fn next_attempt(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
    ) -> anyhow::Result<AttemptNo> {
        let attempts = self.backend.store().load_attempts(saga_id, step).await?;
        let max = attempts
            .iter()
            .filter(|row| row.phase == phase)
            .map(|row| row.attempt.get())
            .max();
        match max {
            None => Ok(AttemptNo::FIRST),
            Some(value) => AttemptNo::new(value)
                .and_then(|attempt| attempt.next())
                .map_err(|violation| anyhow::anyhow!("attempt overflow: {}", violation.code())),
        }
    }
}

/// 业务作用：校验结果 envelope 与实例合同一致（tenant/workflow/version/digest）。
///
/// tenant 不匹配的 envelope 即使 producer 已认证也不得推进：认证只证明"谁在说话"，
/// 不证明它有权把既有 saga_id 切换到另一租户。
///
/// 参数说明：
/// - `instance`: 已提交实例快照。
/// - `identity`: envelope 已证身份。
///
/// 返回：一致返回 `Ok`；任一字段不匹配返回错误，调用方走隔离/DLT 路径。
fn verify_instance_contract(
    instance: &SagaInstanceRow,
    identity: &VerifiedIdentity,
) -> anyhow::Result<()> {
    if instance.tenant != identity.tenant {
        anyhow::bail!("envelope tenant does not match the instance tenant");
    }
    if instance.workflow != identity.workflow
        || instance.definition_version != identity.definition_version
    {
        anyhow::bail!("envelope workflow/version does not match the instance");
    }
    if instance.definition_digest != identity.definition_digest {
        anyhow::bail!("envelope definition digest does not match the instance");
    }
    Ok(())
}

/// 业务作用：从步骤投影中提取与迟到结果互斥的已提交强事实。
///
/// 返回值使用 attempt 状态稳定词汇，让同 attempt 与跨 phase 冲突能进入
/// 同一审计结构。只有确定事实才能作为 `existing_status`；Pending 类弱投影
/// 返回 `None`，由调用方按内部不变量破坏处理。
///
/// 参数说明：
/// - `row`: 写入前的步骤投影。
/// - `context`: 迟到结果所属 phase 与当前补偿方向。
///
/// 返回：找到冲突强事实时返回对应稳定状态；仅有弱投影时返回 `None`。
fn conflicting_projection_status(
    row: &SagaStepRow,
    context: &ResultContext<'_>,
) -> Option<StepAttemptStatus> {
    let forward = || match row.forward_status {
        StepForwardStatus::Succeeded => Some(StepAttemptStatus::Succeeded),
        StepForwardStatus::Rejected => Some(StepAttemptStatus::Rejected),
        StepForwardStatus::Cancelled => Some(StepAttemptStatus::CancelConfirmed),
        StepForwardStatus::Halted => Some(StepAttemptStatus::Halted),
        StepForwardStatus::Pending | StepForwardStatus::Unknown | StepForwardStatus::TimedOut => {
            None
        }
    };
    let cancellation = || match row.cancel_status {
        StepCancelStatus::Confirmed => Some(StepAttemptStatus::CancelConfirmed),
        StepCancelStatus::AlreadyTerminal => Some(StepAttemptStatus::AlreadyTerminal),
        StepCancelStatus::None | StepCancelStatus::ResolutionPending => None,
    };
    let compensation = || match row.compensation_status {
        StepCompensationStatus::Succeeded => Some(StepAttemptStatus::Succeeded),
        StepCompensationStatus::Halted => Some(StepAttemptStatus::Halted),
        StepCompensationStatus::None
        | StepCompensationStatus::Pending
        | StepCompensationStatus::Unknown => None,
    };
    let resolution = || match row.resolution_status {
        StepResolutionStatus::Succeeded => Some(StepAttemptStatus::Succeeded),
        StepResolutionStatus::Rejected => Some(StepAttemptStatus::Rejected),
        StepResolutionStatus::Halted => Some(StepAttemptStatus::Halted),
        StepResolutionStatus::None | StepResolutionStatus::Pending => None,
    };

    match context.identity.phase {
        StepPhase::Execute => forward().or_else(cancellation),
        StepPhase::Cancel => cancellation().or_else(forward),
        StepPhase::Compensate => compensation().or_else(resolution),
        StepPhase::Resolve => resolution().or_else(|| {
            if context.instance.direction == Direction::Compensating {
                compensation()
            } else {
                forward()
            }
        }),
    }
}

/// 业务作用：把迟到 result 合并进步骤投影，保证跨 phase 投影只能单调增强而不能改写强事实。
///
/// `PENDING/UNKNOWN/RESOLUTION_PENDING` 等弱事实可以被确定终态增强；确定终态收到更晚
/// 的弱事实时保持不动；两个互斥确定终态同时存在则返回冲突，由调用方保留 attempt 证据并
/// 把非终态 Saga 转人工，而不是选择“最后写入者获胜”。
///
/// 参数说明：
/// - `row`: 写入前的步骤强类型投影。
/// - `patch`: 根据迟到 result 构造的候选更新；本函数会清除无需写入的回退/重复字段。
///
/// 返回：投影可单调合并返回 `Ok`；发现两个互斥强事实返回 `Err`，调用方不得执行该 patch。
fn merge_deferred_projection(
    row: &SagaStepRow,
    patch: &mut StepJournalPatch<'_>,
) -> Result<(), ()> {
    if let Some(next) = patch.forward_status {
        patch.forward_status = if row.forward_status == next {
            None
        } else {
            match (row.forward_status, next) {
                (
                    StepForwardStatus::Pending
                    | StepForwardStatus::Unknown
                    | StepForwardStatus::TimedOut,
                    _,
                ) => Some(next),
                // 晚到的未知/超时只是较弱证据，不能把已确认终态降级回不确定。
                (_, StepForwardStatus::Unknown | StepForwardStatus::TimedOut) => None,
                _ => return Err(()),
            }
        };
    }

    if let Some(next) = patch.cancel_status {
        patch.cancel_status = if row.cancel_status == next {
            None
        } else {
            match (row.cancel_status, next) {
                (StepCancelStatus::None | StepCancelStatus::ResolutionPending, _) => Some(next),
                // 已完成取消裁决后到达 ResolutionPending 不能抹掉屏障证明。
                (_, StepCancelStatus::ResolutionPending) => None,
                _ => return Err(()),
            }
        };
    }

    if let Some(next) = patch.compensation_status {
        patch.compensation_status = if row.compensation_status == next {
            None
        } else {
            match (row.compensation_status, next) {
                (
                    StepCompensationStatus::None
                    | StepCompensationStatus::Pending
                    | StepCompensationStatus::Unknown,
                    _,
                ) => Some(next),
                // resolve 已确认补偿终态后，迟到 Unknown/Pending 只保留 attempt 事实。
                (_, StepCompensationStatus::Unknown | StepCompensationStatus::Pending) => None,
                _ => return Err(()),
            }
        };
    }

    if let Some(next) = patch.resolution_status {
        patch.resolution_status = if row.resolution_status == next {
            None
        } else {
            match (row.resolution_status, next) {
                (StepResolutionStatus::None | StepResolutionStatus::Pending, _) => Some(next),
                // 最终 resolution 已提交后，迟到 Pending 不得重新打开解决通道。
                (_, StepResolutionStatus::Pending) => None,
                _ => return Err(()),
            }
        };
    }
    Ok(())
}

/// 业务作用：把 CAS 结果收敛为"必须放弃本事务"的错误语义。
///
/// `Conflict` 与 `DuplicateTrigger` 都要求回滚：前者重投后按新快照重新裁决，
/// 后者说明该触发已生效，回滚后必须重新加载已提交结论；调用方不能据此直接 ACK 未确认状态。
///
/// 参数说明：
/// - `outcome`: store CAS 结果。
///
/// 返回：`Applied` 返回数据库确认的新实例版本；其余返回携带稳定原因的错误。
fn ensure_applied(outcome: CasOutcome) -> anyhow::Result<u64> {
    match outcome {
        CasOutcome::Applied { new_version } => Ok(new_version),
        CasOutcome::Conflict => Err(SagaConcurrencyError::StaleSnapshot.into()),
        CasOutcome::DuplicateTrigger => Err(SagaConcurrencyError::DuplicateTrigger.into()),
    }
}

/// 业务作用：按 definition 顺序找出下一个待执行的正向步骤。
///
/// 参数说明：
/// - `definition`: 实例固定版本的定义。
/// - `steps`: 已提交 step journal 投影。
///
/// 返回：第一个 `PENDING` 步骤；全部步骤均 `SUCCEEDED` 返回 `None`；journal 缺行、
/// 顺序漂移或混入其它状态时返回错误，禁止错误发布 `COMPLETED`。
fn next_pending_forward(
    definition: &WorkflowDefinition,
    steps: &[SagaStepRow],
) -> anyhow::Result<Option<StepName>> {
    if steps.len() != definition.steps().len() {
        anyhow::bail!("step journal does not completely cover the workflow definition");
    }
    let mut next = None;
    for (index, step_def) in definition.steps().iter().enumerate() {
        let row = &steps[index];
        if row.step != *step_def.name() || row.ordinal != index as u32 + 1 {
            anyhow::bail!("step journal order does not match the workflow definition");
        }
        match (next.is_some(), row.forward_status) {
            (false, StepForwardStatus::Succeeded) => {}
            (false, StepForwardStatus::Pending) => next = Some(row.step.clone()),
            (true, StepForwardStatus::Pending) => {}
            // 严格串行流程只能是 SUCCEEDED 前缀 + PENDING 后缀；若在首个 PENDING
            // 前看到拒绝/未知，或在其后看到已成功步骤，继续发命令会越过断裂依赖。
            _ => anyhow::bail!("forward journal is not a succeeded-prefix/pending-suffix"),
        }
    }
    Ok(next)
}

/// 业务作用：复验冻结计划的摘要、成员与顺序后，找出下一个待补偿步骤。
///
/// 参数说明：
/// - `instance`: 持有冻结计划摘要的实例快照。
/// - `definition`: 实例固定版本的 definition。
/// - `steps`: 已提交 step journal 投影。
/// - `allow_halted_recovery`: 仅管理入口已落审计时为真，允许选择首个 HALTED 成员。
///
/// 返回：冻结证据完整且成员形成 `SUCCEEDED` 前缀 + `PENDING` 后缀时返回首个待补偿
/// 步骤；管理恢复可把首个 `HALTED` 作为待恢复项。全部成功返回 `None`；摘要漂移、计划外
/// 成员、顺序断裂或自动补偿态混入 Unknown/Halted 时返回错误，调用方不得发出下一命令。
fn next_pending_compensation(
    instance: &SagaInstanceRow,
    definition: &WorkflowDefinition,
    steps: &[SagaStepRow],
    allow_halted_recovery: bool,
) -> anyhow::Result<Option<StepName>> {
    let frozen_version = instance
        .compensation_plan_version
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("compensating instance has no frozen plan"))?;
    let journal: Vec<StepJournalEntry> = steps
        .iter()
        .map(|row| StepJournalEntry::new(row.step.clone(), row.forward_status))
        .collect();
    let rebuilt = freeze_compensation_plan(definition, &journal)
        .map_err(|violation| anyhow::anyhow!("{}", violation.code()))?;
    if rebuilt.version() != frozen_version {
        anyhow::bail!("frozen compensation plan digest drifted");
    }

    let mut next = None;
    for entry in rebuilt.entries() {
        let row = steps
            .iter()
            .find(|row| row.step == *entry.step())
            .ok_or_else(|| anyhow::anyhow!("frozen compensation member is missing"))?;
        if row.compensation_plan_version.as_deref() != Some(frozen_version)
            || row.compensation_order != Some(entry.order())
        {
            anyhow::bail!("durable compensation member metadata drifted");
        }
        match (next.is_some(), row.compensation_status) {
            (false, StepCompensationStatus::Succeeded) => {}
            (false, StepCompensationStatus::Pending) => next = Some(row.step.clone()),
            (false, StepCompensationStatus::Halted) if allow_halted_recovery => {
                next = Some(row.step.clone())
            }
            (true, StepCompensationStatus::Pending) => {}
            _ => anyhow::bail!("compensation journal is not a succeeded-prefix/pending-suffix"),
        }
    }
    let marked_count = steps
        .iter()
        .filter(|row| row.compensation_plan_version.as_deref() == Some(frozen_version))
        .count();
    if marked_count != rebuilt.entries().len() {
        anyhow::bail!("durable compensation plan contains unexpected members");
    }
    Ok(next)
}
