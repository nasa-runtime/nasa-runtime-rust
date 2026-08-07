//! Orchestrator：Saga 的创建、结果推进、durable timer 裁决与管理命令。
//!
//! 每次推进都是一个"单一本地事务"（§11.5.4/11.5.5）：Inbox claim、attempt journal
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

use std::time::{Duration, Instant};

use nainbox_core::InboxClaim;
use nainbox_mysql::MySqlInbox;
use naoutbox_mysql::MySqlOutbox;
use nasaga_core::{
    freeze_compensation_plan, AttemptNo, BusinessKey, CancelMode, CommandId, DefinitionVersion,
    Direction, EffectId, ResolutionMode, SagaId, SagaStatus, ServiceIdentity, StepAttemptStatus,
    StepCancelStatus, StepCompensationStatus, StepDefinition, StepForwardStatus, StepJournalEntry,
    StepName, StepPhase, StepResolutionStatus, TenantId, TimeoutPolicy, TriggerKind,
    WorkflowDefinition, WorkflowName,
};
use nasaga_mysql::{
    AttemptConflictFact, AttemptOutcomeRecord, CasOutcome, ControlCasOutcome,
    ControlTransitionSpec, ManagementAuditOutcome, MySqlSagaStore, NewSagaInstance,
    SagaConflictKind, SagaCreation, SagaInstanceRow, SagaStepRow, SagaTimerRow, StepJournalPatch,
    TimerFencing, TimerFencingToken, TimerFencingTokenIssuer, TimerScope, TimerSpec,
    TransitionSpec,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::envelope::{canonical_bytes, SagaCommandEnvelope, SagaResultEnvelope, VerifiedIdentity};
use crate::management::{SagaAuditTrail, SagaManagementContext, SagaManagementPermission};
use crate::observability::SagaOperationalMetrics;
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
        }
    }
}

/// 业务作用：区分创建入口的两种结果；`AlreadyExists` 表示业务幂等键命中，无新副作用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartOutcome {
    /// 实例已创建，首步命令、timer 与初始 transition 已同事务提交。
    Started(SagaInstanceRow),
    /// 同一业务意图已有实例；返回其当前快照。
    AlreadyExists(SagaInstanceRow),
}

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
    /// 当前时刻（epoch 毫秒），由调用方注入统一时钟。
    pub now_ms: i64,
}

/// 业务作用：Saga Orchestrator——所有推进都以数据库 CAS 与单一本地事务落库。
///
/// 无进程内可变业务状态（仅 fencing token 发行器）：多副本并发安全完全由 store 的
/// CAS、唯一键与 fencing token 承担，任何副本崩溃后另一副本可无缝接管。
pub struct Orchestrator {
    store: MySqlSagaStore,
    inbox: MySqlInbox,
    outbox: MySqlOutbox,
    registry: DefinitionRegistry,
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
fn derive_start_request_digest(
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

impl Orchestrator {
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
            store: MySqlSagaStore::new(),
            inbox: MySqlInbox::new(),
            outbox: MySqlOutbox::new(),
            registry,
            config,
            fencing_tokens: TimerFencingTokenIssuer::new(),
        })
    }

    /// 业务作用：启动预检——非终态实例引用的 definition 必须可用且摘要一致，否则拒绝 Ready。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预检通过返回 `Ok`；缺失或摘要漂移返回错误，调用方不得宣告 Ready。
    pub async fn verify_startup(&self) -> anyhow::Result<()> {
        self.registry
            .verify_non_terminal(&self.store, self.config.startup_scan_limit)
            .await
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
            .store
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
        self.store
            .load_instance(saga_id)
            .await?
            .filter(|instance| &instance.tenant == tenant)
            .ok_or_else(|| anyhow::anyhow!("saga instance not found"))?;
        Ok(SagaAuditTrail {
            attempts: self.store.load_attempt_audit(saga_id, limit).await?,
            transitions: self
                .store
                .load_transition_audit(saga_id, after_transition_seq, limit)
                .await?,
            controls: self
                .store
                .load_control_audit(saga_id, after_control_seq, limit)
                .await?,
            management_operations: self.store.load_management_audit(saga_id, limit).await?,
            conflicts: self.store.load_conflict_audit(saga_id, limit).await?,
        })
    }

    /// 业务作用：读取一份不含业务身份标签的 Saga 运行指标快照。
    ///
    /// 生命周期与状态计数来自 MySQL 已提交事实；Kafka 处理指标来自当前进程。
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
        Ok(self.store.load_operational_metrics(now_ms).await?.into())
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
        let definition = self
            .registry
            .get(&identity.workflow, identity.definition_version)
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
        // producer 权限必须在 Inbox claim 之前复验，防止越权消息抢占 event id。
        self.verify_result_producer(envelope, producer)?;
        self.handle_verified_result(envelope, now_ms).await
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
    /// workflow 未注册、definition 为空或底层失败返回错误（事务回滚）。
    pub async fn start_saga(&self, request: &StartSagaRequest<'_>) -> anyhow::Result<StartOutcome> {
        let definition = self
            .registry
            .get(request.workflow, request.version)
            .ok_or_else(|| anyhow::anyhow!("workflow definition is not registered"))?;
        let digest = definition.digest();
        let first_step = definition
            .steps()
            .first()
            .ok_or_else(|| anyhow::anyhow!("definition has no steps"))?;
        let start_request_digest = derive_start_request_digest(request, &digest, first_step.name());

        let outcome = crate::transaction::run(async {
            let creation = self
                .store
                .create_instance(&NewSagaInstance {
                    saga_id: request.saga_id,
                    tenant: request.tenant,
                    workflow: request.workflow,
                    business_key: request.business_key,
                    definition_version: request.version,
                    definition_digest: &digest,
                    start_request_digest: &start_request_digest,
                    deadline_at_ms: request.deadline_at_ms,
                    // 创建即定位首步:step 超时裁决要求 current_step 与 timer 步骤一致。
                    current_step: Some(first_step.name()),
                    trigger_kind: request.trigger_kind,
                    trigger_id: request.trigger_id,
                })
                .await?;
            let instance = match creation {
                // 幂等命中:绝不重复发布首步命令、timer 或初始 transition。
                SagaCreation::Existing(row) => return Ok(StartOutcome::AlreadyExists(row)),
                SagaCreation::Created(row) => row,
            };
            self.store
                .register_steps(request.saga_id, definition)
                .await?;
            self.issue_command(
                &instance,
                first_step,
                StepPhase::Execute,
                AttemptNo::FIRST,
                request.first_command_payload.clone(),
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
    ///
    /// 返回：推进成功返回 [`HandleOutcome::Applied`]（COMMIT 已确认，调用方可 ACK）；
    /// 重复事件返回 `Duplicate`（可 ACK）；身份复验失败、合同不匹配、矛盾 outcome 或
    /// CAS 冲突返回错误（已回滚，不得 ACK）。
    async fn handle_verified_result(
        &self,
        envelope: &SagaResultEnvelope,
        now_ms: i64,
    ) -> anyhow::Result<HandleOutcome> {
        // 身份复验在任何持久化动作之前:伪造 envelope 不允许占用去重身份。
        let identity = envelope.verified_for_delivery()?;
        let status = envelope
            .parsed_status()
            .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;
        let terminal = envelope
            .parsed_terminal()
            .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;

        let outcome = crate::transaction::run(async {
            if matches!(
                self.inbox
                    .claim(&self.config.inbox_consumer, &envelope.event_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(HandleOutcome::Duplicate);
            }
            let instance = self
                .store
                .load_instance(&identity.saga_id)
                .await?
                .ok_or(crate::SagaResultProcessingError::ContractInvalid)?;
            verify_instance_contract(&instance, &identity)
                .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;
            if !instance.control_state.allows_automatic_actions() {
                // PAUSED 的核心语义是不发出任何自动动作；结果也不能先被 Inbox 吞掉再悬空。
                // 类型化延后使同事务 Inbox claim 回滚且不消耗毒消息预算；恢复后再裁决。
                return Err(crate::SagaResultProcessingError::Paused.into());
            }
            let definition = self
                .registry
                .get(&instance.workflow, instance.definition_version)
                .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;
            let step_def = definition.step(&identity.step).ok_or_else(|| {
                anyhow::Error::from(crate::SagaResultProcessingError::ContractInvalid)
            })?;

            // 真实 outcome 必须先落 journal:即使实例已在人工介入,迟到事实也不许丢
            // (拒收会造成恢复时重复补偿)。
            let recorded = self
                .store
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
                    self.store
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
            .store
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
                        .store
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
        let step_scope = parse_step_scope(&timer.scope_kind, &timer.scope_key)?;
        crate::transaction::run(async {
            let Some(instance) = self.store.load_instance(&timer.saga_id).await? else {
                // 实例已被归档清理的孤儿 timer:消费吸收,不再触发。
                let fencing_now = match claimed_at {
                    Some(started) => elapsed_epoch_ms(base_now_ms, started)?,
                    None => base_now_ms,
                };
                return Ok(
                    match self
                        .store
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
                self.store
                    .complete_timer(&timer.timer_id, token, now_ms)
                    .await?,
                TimerFencing::Lost
            ) {
                return Ok(TimerOutcome::SkippedFencingLost);
            }
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
            let definition = self
                .registry
                .get(&instance.workflow, instance.definition_version)
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
        // 权限门禁必须早于实例查询，避免无权主体利用存在性与租户错误枚举 saga_id。
        management.require(SagaManagementPermission::Pause)?;
        crate::transaction::run(async {
            let instance = self
                .store
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or_else(|| anyhow::anyhow!("saga instance not found"))?;
            match self
                .store
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
                ControlCasOutcome::Applied | ControlCasOutcome::AlreadyApplied => Ok(()),
                ControlCasOutcome::Conflict => {
                    anyhow::bail!("pause lost the control-state race; reload and retry")
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
        // 与 pause 相同，先鉴权再读实例，防止管理读侧成为跨租户枚举 oracle。
        management.require(SagaManagementPermission::Resume)?;
        crate::transaction::run(async {
            let instance = self
                .store
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or_else(|| anyhow::anyhow!("saga instance not found"))?;
            match self
                .store
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
                    // 恢复不重置业务期限：只清除暂停产生的 available_at 退避，
                    // 已逾期 timer 会在下一轮立即可领，未来 timer 仍等待原 due_at。
                    self.store.wake_saga_timers(saga_id).await?;
                    Ok(())
                }
                ControlCasOutcome::AlreadyApplied => Ok(()),
                ControlCasOutcome::Conflict => {
                    anyhow::bail!("resume lost the control-state race; reload and retry")
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
        // 人工恢复会再次发布外部补偿命令，必须先做最小权限校验，再接触实例数据。
        management.require(SagaManagementPermission::RetryCompensation)?;
        let status = crate::transaction::run(async {
            let instance = self
                .store
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or_else(|| anyhow::anyhow!("saga instance not found"))?;
            if !instance.control_state.allows_automatic_actions() {
                // 人工恢复会立即发布外部命令；PAUSED 下先写审计再由 advance 冲突回滚，
                // 既给出误导错误也无法留下审计。必须要求显式 resume 后使用新 operation。
                anyhow::bail!("retry_compensation requires ACTIVE control state; resume first");
            }
            match self
                .store
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
            if instance.status != SagaStatus::ManualIntervention {
                anyhow::bail!("retry_compensation requires MANUAL_INTERVENTION");
            }
            if instance.compensation_plan_version.is_none() {
                anyhow::bail!("no frozen compensation plan to retry");
            }
            let definition = self
                .registry
                .get(&instance.workflow, instance.definition_version)
                .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;
            let steps = self.store.load_steps(saga_id).await?;
            match next_pending_compensation(&instance, definition, &steps, true)? {
                Some(step) => {
                    let recovering_halted = steps.iter().any(|row| {
                        row.step == step
                            && row.compensation_status == StepCompensationStatus::Halted
                    });
                    if recovering_halted {
                        // HALTED 只能由本事务已写入的管理审计解除；普通投影补丁和 timer
                        // 均不能访问该路径，参与方还会复验命令上的 recovery operation。
                        self.store
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
                    let step_def = definition
                        .step(&step)
                        .ok_or_else(|| anyhow::anyhow!("plan step absent from definition"))?;
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
        management.require(SagaManagementPermission::RetryResolution)?;
        let status = crate::transaction::run(async {
            let instance = self
                .store
                .load_instance(saga_id)
                .await?
                .filter(|instance| &instance.tenant == tenant)
                .ok_or_else(|| anyhow::anyhow!("saga instance not found"))?;
            if !instance.control_state.allows_automatic_actions() {
                anyhow::bail!("retry_resolution requires ACTIVE control state; resume first");
            }
            match self
                .store
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
            if instance.status != SagaStatus::ManualIntervention {
                anyhow::bail!("retry_resolution requires MANUAL_INTERVENTION");
            }
            let step = instance
                .current_step
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("manual resolution has no current step"))?;
            let definition = self
                .registry
                .get(&instance.workflow, instance.definition_version)
                .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;
            let step_def = definition
                .step(step)
                .ok_or_else(|| anyhow::anyhow!("resolution step absent from definition"))?;
            if step_def.resolution().mode() != Some(ResolutionMode::Poll) {
                anyhow::bail!("retry_resolution requires a Poll resolver");
            }
            let row = self
                .store
                .load_steps(saga_id)
                .await?
                .into_iter()
                .find(|row| row.step == *step)
                .ok_or_else(|| anyhow::anyhow!("resolution step journal is missing"))?;
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
                anyhow::bail!("retry_resolution requires an unresolved Unknown effect");
            }

            let attempt = self.next_attempt(saga_id, step, StepPhase::Resolve).await?;
            if row.resolution_status == StepResolutionStatus::Halted {
                // 普通单调投影故意拒绝 HALTED→PENDING；这里只允许同事务中已经落审计的
                // 管理操作重开，避免自动 timeout 或迟到结果解除人工冻结。
                self.store
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
            .store
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
            && row.compensation_status == StepCompensationStatus::Succeeded;
        if resolve_after_compensation_success
            || merge_deferred_projection(&row, &mut patch).is_err()
        {
            // attempt 事实已经先记入统一 journal；这里不能返回错误让它随 Inbox 一起
            // 回滚成永久毒消息。先保留冲突双方证据，再将非终态实例同事务
            // 转人工等待核验；两者不得拆成两次 COMMIT，否则崩溃窗口会产生无证据 Manual。
            let existing = conflicting_projection_status(&row, context)
                .ok_or_else(|| anyhow::anyhow!("conflicting projection has no settled fact"))?;
            self.store
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
        self.store
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
            .store
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
            && row.compensation_status == StepCompensationStatus::Succeeded;
        if resolve_after_compensation_success
            || merge_deferred_projection(&row, &mut patch).is_err()
        {
            let existing = conflicting_projection_status(&row, context)
                .ok_or_else(|| anyhow::anyhow!("conflicting projection has no settled fact"))?;
            self.store
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
                self.store
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
                self.store
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
                self.store
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
                self.store
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
                self.store
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
                self.store
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
        self.store
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
            self.store
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
        let steps = self.store.load_steps(saga_id).await?;
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
                self.store
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
        let steps = self.store.load_steps(saga_id).await?;
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
        self.store.mark_compensation_plan(saga_id, &plan).await?;
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
        let steps = self.store.load_steps(saga_id).await?;
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
        self.store
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
            .store
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
            .store
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
        self.store
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
            .store
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
        let definition = self
            .registry
            .get(&instance.workflow, instance.definition_version)
            .ok_or_else(|| anyhow::anyhow!("instance definition is not registered"))?;
        let steps = self.store.load_steps(saga_id).await?;
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
            .store
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
    /// - `payload`: 业务输入；仅首步 execute 携带。
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
        payload: Option<serde_json::Value>,
        recovery_operation_id: Option<&str>,
        now_ms: i64,
        expected_version: u64,
    ) -> anyhow::Result<()> {
        let step = step_def.name();
        let effect = EffectId::derive(&instance.saga_id, instance.definition_version, step, phase);
        let command = CommandId::derive(&effect, attempt);
        // journal 先占身份,再写命令:结果事件永远能找到去重锚点。
        self.store
            .record_attempt_started(&instance.saga_id, step, phase, attempt, &effect, &command)
            .await?;
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
        };
        // 关键双写:命令进 Outbox 必须与状态迁移同事务,事务外 autocommit 被拒绝。
        self.outbox
            .append_transactional(&envelope.to_outbox_event()?)
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
        self.store
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
            self.store
                .cancel_scope_timers(saga_id, TimerScope::Step(step), Some(kind))
                .await?;
        }
        self.store
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
        self.store
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
        let attempts = self.store.load_attempts(saga_id, step).await?;
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
        CasOutcome::Conflict => anyhow::bail!("cas conflict: snapshot is stale; redeliver"),
        CasOutcome::DuplicateTrigger => {
            anyhow::bail!("duplicate trigger: transition already applied; rollback and reload")
        }
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
