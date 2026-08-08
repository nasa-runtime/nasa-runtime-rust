//! Saga command/result envelope：跨服务传输的受保护 header 与身份复验。
//!
//! envelope 至少携带 `saga_id`、workflow、definition version/digest、step、phase、
//! attempt、`effect_id`、`command_id` 与 tenant。**身份不可信自报**：
//! 消费方必须用 `verified()` 把文本字段解析回封闭类型，并重新派生 effect/command 身份
//! 与自报值比对——派生是确定性的，比对失败即说明 envelope 被伪造、损坏或派生规则漂移，
//! 一律拒绝进入业务路径。producer 身份认证（topic-to-owner 映射/端到端签名）发生在
//! transport 接入层，本层假定 envelope 已通过该认证。

use naoutbox_core::OutboxEvent;
use nasaga_core::{
    AttemptNo, CommandId, DefinitionVersion, EffectId, SagaId, StepAttemptStatus,
    StepForwardStatus, StepName, StepPhase, TenantId, WorkflowName,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// command envelope 的 Outbox event type；下游按此路由到 command topic。
pub const COMMAND_EVENT_TYPE: &str = "saga.command";

/// result envelope 的 Outbox event type；下游按此路由到 result topic。
pub const RESULT_EVENT_TYPE: &str = "saga.result";

/// Saga 事件的聚合类型；`aggregate_id = saga_id` 兼作 Kafka partition key——
/// 顺序只是优化，正确性只依赖状态机、Inbox 与 CAS。
pub const SAGA_AGGREGATE_TYPE: &str = "saga";

/// 稳定原因码的最大字节数；与 `saga_instance.failure_code`、
/// `saga_step.last_error_code` 的 VARCHAR(64) 持久化合同一致。
const REASON_CODE_MAX_LEN: usize = 64;

/// 业务作用：校验原因码能否安全进入持久化列、指标维度和跨服务 result envelope。
///
/// 参数说明：
/// - `code`: 业务 handler 返回的稳定原因码。
///
/// 返回：非空、至多 64 字节且只含可移植标识符字符时返回真。
pub(crate) fn valid_reason_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= REASON_CODE_MAX_LEN
        && code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// 派生 result 事件身份的固定命名空间（ASCII `nasasaga-v1-rsns`）。
///
/// 发布后**不得修改**：同一 command 的 result 事件身份必须跨进程稳定，
/// 参与方事务重试才不会为同一结果占用两个 Inbox 去重身份。
const RESULT_EVENT_NAMESPACE: Uuid = Uuid::from_bytes(*b"nasasaga-v1-rsns");

/// 业务作用：以长度前缀方式拼接字段，消除字段边界歧义（与 nasaga-core 同规则）。
///
/// 参数说明：
/// - `fields`: 按固定顺序给出的字段字节切片。
///
/// 返回：可送入 UUIDv5 的规范字节串。
pub(crate) fn canonical_bytes(fields: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for field in fields {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    encoded
}

/// 业务作用：从命令身份确定性派生 result 事件身份，使参与方崩溃重试不产生第二个结果事件身份。
///
/// 参数说明：
/// - `command_id`: 触发该结果的命令身份文本。
///
/// 返回：跨进程一致的 result 事件 id（UUID 文本）。
pub fn derive_result_event_id(command_id: &str) -> String {
    Uuid::new_v5(
        &RESULT_EVENT_NAMESPACE,
        &canonical_bytes(&[command_id.as_bytes(), b"result"]),
    )
    .to_string()
}

/// 业务作用：Orchestrator 发往参与方的 execute/cancel/compensate/resolve 命令 envelope。
///
/// 字段说明：`payload` 只在首步 execute 由启动请求携带；其余命令只携带 Saga 与 step
/// 身份，参与方按 `saga_id + step` 从本地事实解析业务输入，天然避免敏感载荷跨服务复制。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaCommandEnvelope {
    /// 实例身份。
    pub saga_id: String,
    /// 规范化租户；无租户部署为固定 system tenant。
    pub tenant_id: String,
    /// workflow 名称。
    pub workflow: String,
    /// 实例固定的 definition 版本。
    pub definition_version: u32,
    /// 实例固定的 definition 摘要。
    pub definition_digest: String,
    /// 步骤名称。
    pub step: String,
    /// 执行阶段稳定名（execute/cancel/compensate/resolve）。
    pub phase: String,
    /// 尝试序号。
    pub attempt: u32,
    /// 跨 attempt 稳定的业务效果身份。
    pub effect_id: String,
    /// 本次尝试的命令身份；同一 attempt 传输重投复用。
    pub command_id: String,
    /// 仅由已审计管理操作签发的恢复身份；普通自动命令必须为空。
    ///
    /// Participant 只允许携带该字段的新命令重新打开 `HALTED` 查询或补偿 gate；因此
    /// 自动 timeout/new attempt 不能解除人工冻结。旧格式 envelope 缺字段时按 `None`
    /// 解码，支持滚动升级期间继续消费已发布消息。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_operation_id: Option<String>,
    /// 首步 execute 的业务输入；其余命令为空。
    pub payload: Option<serde_json::Value>,
}

/// 业务作用：参与方发回 Orchestrator 的结果 envelope。
///
/// 字段说明：`status` 是 attempt journal 词汇（四个 phase 终态的并集）；
/// `terminal_status` 仅在取消屏障裁决 `ALREADY_TERMINAL` 时携带正向真实终态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaResultEnvelope {
    /// 结果事件身份，进入 Orchestrator Inbox 去重。
    pub event_id: String,
    /// 实例身份。
    pub saga_id: String,
    /// 规范化租户。
    pub tenant_id: String,
    /// workflow 名称。
    pub workflow: String,
    /// definition 版本。
    pub definition_version: u32,
    /// definition 摘要。
    pub definition_digest: String,
    /// 步骤名称。
    pub step: String,
    /// 执行阶段稳定名。
    pub phase: String,
    /// 尝试序号。
    pub attempt: u32,
    /// 效果身份。
    pub effect_id: String,
    /// 命令身份。
    pub command_id: String,
    /// attempt 终态稳定名（SUCCEEDED/REJECTED/…/RESOLUTION_PENDING）。
    pub status: String,
    /// 取消裁决 `ALREADY_TERMINAL` 携带的正向真实终态稳定名。
    pub terminal_status: Option<String>,
    /// 稳定原因码；禁止携带敏感明细。
    pub reason_code: Option<String>,
}

/// 业务作用：envelope 通过身份复验后的强类型视图，业务路径只消费本类型。
#[derive(Debug, Clone)]
pub struct VerifiedIdentity {
    /// 实例身份。
    pub saga_id: SagaId,
    /// 租户身份。
    pub tenant: TenantId,
    /// workflow 名称。
    pub workflow: WorkflowName,
    /// definition 版本。
    pub definition_version: DefinitionVersion,
    /// definition 摘要。
    pub definition_digest: String,
    /// 步骤名称。
    pub step: StepName,
    /// 执行阶段。
    pub phase: StepPhase,
    /// 尝试序号。
    pub attempt: AttemptNo,
    /// 复验一致的效果身份。
    pub effect: EffectId,
    /// 复验一致的命令身份。
    pub command: CommandId,
}

/// 业务作用：解析并复验 envelope 公共身份字段，把"自报身份"升级为"已证身份"。
///
/// 关键步骤：effect/command 由 `(saga, version, step, phase, attempt)` **重新派生**后与
/// 自报值比对——不一致说明伪造、损坏或派生漂移，绝不进入 Inbox claim 或业务 handler，
/// 否则攻击者可用自报 id 预先污染合法命令的去重身份。
///
/// 参数说明：
/// - `saga_id`/`tenant_id`/`workflow`/`definition_version`/`definition_digest`/`step`/
///   `phase`/`attempt`/`effect_id`/`command_id`: envelope 自报的身份字段。
///
/// 返回：全部字段合法且派生一致时返回已证身份；否则返回携带稳定原因的错误。
#[allow(clippy::too_many_arguments)]
fn verify_identity(
    saga_id: &str,
    tenant_id: &str,
    workflow: &str,
    definition_version: u32,
    definition_digest: &str,
    step: &str,
    phase: &str,
    attempt: u32,
    effect_id: &str,
    command_id: &str,
) -> anyhow::Result<VerifiedIdentity> {
    let saga_id = SagaId::new(saga_id).map_err(|v| anyhow::anyhow!("bad saga id: {}", v.code()))?;
    let tenant =
        TenantId::new(tenant_id).map_err(|v| anyhow::anyhow!("bad tenant: {}", v.code()))?;
    let workflow =
        WorkflowName::new(workflow).map_err(|v| anyhow::anyhow!("bad workflow: {}", v.code()))?;
    let version = DefinitionVersion::new(definition_version)
        .map_err(|v| anyhow::anyhow!("bad version: {}", v.code()))?;
    let step = StepName::new(step).map_err(|v| anyhow::anyhow!("bad step: {}", v.code()))?;
    let phase = StepPhase::parse(phase).ok_or_else(|| anyhow::anyhow!("bad phase"))?;
    let attempt =
        AttemptNo::new(attempt).map_err(|v| anyhow::anyhow!("bad attempt: {}", v.code()))?;
    // definition digest 会参与实例合同复验；先限制为 SHA-256 小写十六进制，
    // 防止大小写或非规范编码把同一摘要扩成多个传输身份。
    if definition_digest.len() != 64
        || !definition_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        anyhow::bail!("bad definition digest: expected 64 lowercase hex characters");
    }
    let effect = EffectId::derive(&saga_id, version, &step, phase);
    let command = CommandId::derive(&effect, attempt);
    // 身份复验是伪造与漂移的最后防线:自报 id 与确定性派生不一致的 envelope
    // 不允许占用任何去重身份或触达业务 handler。
    if effect.to_string() != effect_id || command.to_string() != command_id {
        anyhow::bail!("envelope identity mismatch: derived ids differ from self-reported ids");
    }
    Ok(VerifiedIdentity {
        saga_id,
        tenant,
        workflow,
        definition_version: version,
        definition_digest: definition_digest.to_string(),
        step,
        phase,
        attempt,
        effect,
        command,
    })
}

/// 业务作用：校验结果状态与 phase、event identity、terminal 附加字段之间的协议组合。
///
/// 参数说明：
/// - `envelope`: 尚未进入 Inbox 的结果 envelope。
/// - `identity`: 已完成公共身份派生复验的强类型视图。
///
/// 返回：组合属于封闭协议时返回 `Ok`；伪造 event id、跨 phase 状态或非法原因码返回错误，
/// 调用方必须隔离消息且不得占用 Inbox 去重身份。
fn verify_result_contract(
    envelope: &SagaResultEnvelope,
    identity: &VerifiedIdentity,
) -> anyhow::Result<()> {
    if envelope.event_id != derive_result_event_id(&envelope.command_id) {
        anyhow::bail!("result event identity does not match command identity");
    }
    let status = envelope.parsed_status()?;
    let phase_allows_status = match identity.phase {
        StepPhase::Execute => matches!(
            status,
            StepAttemptStatus::Succeeded
                | StepAttemptStatus::Rejected
                | StepAttemptStatus::Unknown
                | StepAttemptStatus::Halted
        ),
        StepPhase::Cancel => matches!(
            status,
            StepAttemptStatus::CancelConfirmed
                | StepAttemptStatus::AlreadyTerminal
                | StepAttemptStatus::ResolutionPending
        ),
        StepPhase::Compensate => matches!(
            status,
            StepAttemptStatus::Succeeded | StepAttemptStatus::Unknown | StepAttemptStatus::Halted
        ),
        StepPhase::Resolve => matches!(
            status,
            StepAttemptStatus::Succeeded
                | StepAttemptStatus::Rejected
                | StepAttemptStatus::Unknown
                | StepAttemptStatus::Halted
        ),
    };
    if !phase_allows_status {
        anyhow::bail!("result status is not valid for its phase");
    }

    match (status, envelope.parsed_terminal()?) {
        (
            StepAttemptStatus::AlreadyTerminal,
            Some(
                StepForwardStatus::Succeeded
                | StepForwardStatus::Rejected
                | StepForwardStatus::Halted,
            ),
        ) => {}
        (StepAttemptStatus::AlreadyTerminal, _) => {
            anyhow::bail!("ALREADY_TERMINAL requires a settled terminal forward status")
        }
        (_, Some(_)) => anyhow::bail!("terminal_status is only valid for ALREADY_TERMINAL"),
        (_, None) => {}
    }

    if let Some(code) = envelope.reason_code.as_deref() {
        if !valid_reason_code(code) {
            anyhow::bail!("reason_code is not a stable bounded identifier");
        }
    }
    Ok(())
}

impl SagaCommandEnvelope {
    /// 业务作用：解析并复验命令 envelope 的身份字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：复验通过返回已证身份；否则返回错误，调用方走隔离路径而不是业务路径。
    pub fn verified(&self) -> anyhow::Result<VerifiedIdentity> {
        let identity = verify_identity(
            &self.saga_id,
            &self.tenant_id,
            &self.workflow,
            self.definition_version,
            &self.definition_digest,
            &self.step,
            &self.phase,
            self.attempt,
            &self.effect_id,
            &self.command_id,
        )?;
        if let Some(operation_id) = self.recovery_operation_id.as_deref() {
            if operation_id.is_empty()
                || operation_id.trim() != operation_id
                || operation_id.len() > 190
                || operation_id.chars().any(char::is_control)
                || !matches!(identity.phase, StepPhase::Compensate | StepPhase::Resolve)
            {
                anyhow::bail!("recovery operation identity is invalid for the command phase");
            }
        }
        Ok(identity)
    }

    /// 业务作用：在 Kafka 投递边界把命令身份错误转换成封闭分类。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份合法时返回已证身份；否则只返回稳定的类型化分类，使 transport 不解析
    /// 可能包含不可信字段的错误文本，也不让非法命令占用 Participant Inbox。
    #[cfg(feature = "kafka")]
    pub(crate) fn verified_for_delivery(&self) -> anyhow::Result<VerifiedIdentity> {
        self.verified()
            .map_err(|_| crate::SagaCommandProcessingError::IdentityInvalid.into())
    }

    /// 业务作用：把命令 envelope 序列化为 Outbox 事件，与触发它的状态迁移同事务发布。
    ///
    /// `event_id` 直接使用 `command_id`：同一 attempt 的传输重投必须复用同一投递身份，
    /// dispatcher 重发不会产生新的去重身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可交给 `naoutbox` 的事件；序列化失败返回错误。
    pub fn to_outbox_event(&self) -> anyhow::Result<OutboxEvent> {
        // producer 也必须执行与 consumer 相同的身份复验；否则本地代码可以先把非法
        // envelope 提交到 Outbox，直到远端消费时才形成永久重投/隔离毒消息。
        self.verified()?;
        Ok(OutboxEvent {
            event_id: self.command_id.clone(),
            aggregate_type: SAGA_AGGREGATE_TYPE.to_string(),
            aggregate_id: self.saga_id.clone(),
            event_type: COMMAND_EVENT_TYPE.to_string(),
            payload: serde_json::to_vec(self)?,
            traceparent: None,
        })
    }
}

impl SagaResultEnvelope {
    /// 业务作用：解析并复验结果 envelope 的身份字段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：复验通过返回已证身份；否则返回错误，调用方走隔离路径。
    pub fn verified(&self) -> anyhow::Result<VerifiedIdentity> {
        let identity = verify_identity(
            &self.saga_id,
            &self.tenant_id,
            &self.workflow,
            self.definition_version,
            &self.definition_digest,
            &self.step,
            &self.phase,
            self.attempt,
            &self.effect_id,
            &self.command_id,
        )?;
        verify_result_contract(self, &identity)?;
        Ok(identity)
    }

    /// 业务作用：在 Kafka 投递边界把结果身份错误与合同错误转换成封闭分类。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份和结果合同都合法时返回已证身份；否则只返回稳定的类型化分类，
    /// 使 transport 无需解析可能含不可信字段的错误文本。
    pub(crate) fn verified_for_delivery(&self) -> anyhow::Result<VerifiedIdentity> {
        let identity = verify_identity(
            &self.saga_id,
            &self.tenant_id,
            &self.workflow,
            self.definition_version,
            &self.definition_digest,
            &self.step,
            &self.phase,
            self.attempt,
            &self.effect_id,
            &self.command_id,
        )
        .map_err(|_| crate::SagaResultProcessingError::IdentityInvalid)?;
        verify_result_contract(self, &identity)
            .map_err(|_| crate::SagaResultProcessingError::ContractInvalid)?;
        Ok(identity)
    }

    /// 业务作用：解析结果状态列，收敛回 attempt journal 封闭词汇。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：合法且非 `STARTED` 时返回状态；否则返回错误——结果必须是已裁决状态。
    pub fn parsed_status(&self) -> anyhow::Result<StepAttemptStatus> {
        let status = StepAttemptStatus::parse(&self.status)
            .ok_or_else(|| anyhow::anyhow!("unknown result status"))?;
        if status.is_in_flight() {
            anyhow::bail!("result status must be settled");
        }
        Ok(status)
    }

    /// 业务作用：解析取消裁决携带的正向真实终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`ALREADY_TERMINAL` 结果返回其终态；其它结果返回 `None`；文本非法返回错误。
    pub fn parsed_terminal(&self) -> anyhow::Result<Option<StepForwardStatus>> {
        self.terminal_status
            .as_deref()
            .map(|raw| {
                StepForwardStatus::parse(raw)
                    .ok_or_else(|| anyhow::anyhow!("unknown terminal status"))
            })
            .transpose()
    }

    /// 业务作用：把结果 envelope 序列化为 Outbox 事件（参与方侧使用）。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可交给 `naoutbox` 的事件；序列化失败返回错误。
    pub fn to_outbox_event(&self) -> anyhow::Result<OutboxEvent> {
        // reason_code、phase/status 和确定性 event id 在发布前即封闭，避免生产者制造
        // 自己的消费者必然拒绝、且已经可靠落入 Outbox 的毒消息。
        self.verified()?;
        Ok(OutboxEvent {
            event_id: self.event_id.clone(),
            aggregate_type: SAGA_AGGREGATE_TYPE.to_string(),
            aggregate_id: self.saga_id.clone(),
            event_type: RESULT_EVENT_TYPE.to_string(),
            payload: serde_json::to_vec(self)?,
            traceparent: None,
        })
    }
}
