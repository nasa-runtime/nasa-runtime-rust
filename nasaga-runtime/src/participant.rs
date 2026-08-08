//! 参与方 adapter 支撑：`#[saga]` 宏生成代码调用的完整本地事务 wrapper。
//!
//! 固定事务序：身份复验（envelope 伪造不进业务路径）→ 开启本地事务 →
//! Inbox claim(`command_id`) → 按 `effect_id` 竞争 participant step gate → 业务 handler
//! → 本地业务结果 + gate 落账 → 结果 Outbox → COMMIT → transport 在 COMMIT 后 ACK。
//! `Retryable` 与任何基础设施失败返回 `Err` 使事务回滚且**不得 ACK**。
//!
//! 当前覆盖 `local_fenceable` 的 execute/cancel/compensate、`resolve_only` Poll，以及
//! `externally_cancellable` 的类型化 cancel + Poll resolve。外部取消必须以稳定 `effect_id`
//! 调用远端幂等接口；本地提交失败后的重投不能形成第二份外部效果。

use nainbox_core::InboxClaim;
use nainbox_mysql::MySqlInbox;
use naoutbox_mysql::MySqlOutbox;
use std::collections::{BTreeMap, BTreeSet};

use nasaga_core::{
    CancelOutcome, CompensationOutcome, DefinitionVersion, EffectId, SagaCancelStep, SagaContext,
    SagaExecutionError, SagaOutcome, SagaResolveStep, SagaStep, ServiceIdentity, StepAttemptStatus,
    StepCancelStatus, StepCompensationStatus, StepForwardStatus, StepPhase, StepResolutionStatus,
    TerminalForwardOutcome, WorkflowName,
};
use nasaga_mysql::{
    CancelAdjudication, CompensationAdmission, ExecuteAdmission, ExternalCancelAdmission,
    MySqlSagaStore, ParticipantGateKey, ResolutionAdmission,
};
use serde::de::DeserializeOwned;

use crate::envelope::{
    derive_result_event_id, valid_reason_code, SagaCommandEnvelope, SagaResultEnvelope,
};

/// 业务作用：抽象宏生成的类型化步骤分发入口，供 hosted command consumer 绑定具体 Service。
///
/// 实现只能由 `#[saga]` 按已发布 descriptor 生成；它必须先执行精确 workflow/version/step
/// 门禁，再进入 [`ParticipantRuntime`] 的 Inbox、gate、业务和结果 Outbox 事务序。
pub trait SagaCommandService: Send + Sync + 'static {
    /// 业务作用：把一条已由 transport 认证并绑定目标 route 的命令交给完整参与方事务 wrapper。
    ///
    /// 参数说明：
    /// - `runtime`: 本服务的参与方运行时。
    /// - `envelope`: 已认证且 route 精确匹配的 command envelope。
    /// - `producer`: transport 从可信来源映射出的 Orchestrator 逻辑身份。
    ///
    /// 返回：本地事务 COMMIT 后返回可 ACK 结论；合同、业务瞬态或基础设施错误返回错误。
    fn handle_saga_command<'a>(
        &'a self,
        runtime: &'a ParticipantRuntime,
        envelope: &'a SagaCommandEnvelope,
        producer: &'a ServiceIdentity,
    ) -> impl std::future::Future<Output = anyhow::Result<ParticipantHandled>> + Send + 'a;
}

/// 业务作用：参与方命令处理的可 ACK 结论。
///
/// 全部分支都表示"事务已 COMMIT，可以 ACK"；`Retryable` 与基础设施失败不会出现在
/// 这里——它们以 `Err` 返回并回滚。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantHandled {
    /// 业务 handler 真实执行并提交了结论。
    Executed(StepAttemptStatus),
    /// Inbox 命中重复命令；零副作用。
    Duplicate,
    /// 取消屏障已建立，execute 被本地状态拒绝；零正向效果，无结果事件。
    Suppressed,
    /// 该效果已有终态（新 attempt 重投）；已按既有终态重发结果事件，零业务效果。
    Replayed(StepAttemptStatus),
    /// 补偿合同违规（本地无正向成功效果）；已提交 `Halted` 违规事实并需告警。
    ContractViolation,
}

/// 业务作用：冻结一个 workflow definition 可向本 Participant 发命令的逻辑 Orchestrator。
///
/// 信任投影同时锚定 workflow、version 与 digest；共享 Participant 的多个业务域不能只凭
/// “某个已登记 Orchestrator”身份跨流程发起 execute、cancel、compensate 或 resolve。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantCommandTrust {
    producer: ServiceIdentity,
    workflow: WorkflowName,
    definition_version: DefinitionVersion,
    definition_digest: String,
}

impl ParticipantCommandTrust {
    /// 业务作用：从受信 deployment/definition projection 构造一条精确 command 授权边。
    ///
    /// 参数说明：
    /// - `producer`: 该 definition 的逻辑 Orchestrator 身份。
    /// - `workflow`: 投影绑定的 workflow 名称。
    /// - `definition_version`: 投影绑定的不可变 definition 版本。
    /// - `definition_digest`: 投影绑定的 64 位小写十六进制摘要。
    ///
    /// 返回：摘要编码合法时返回冻结投影；非法摘要在 consumer Ready 前拒绝启动。
    pub fn new(
        producer: ServiceIdentity,
        workflow: WorkflowName,
        definition_version: DefinitionVersion,
        definition_digest: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let definition_digest = definition_digest.into();
        if !valid_definition_digest(&definition_digest) {
            anyhow::bail!("participant command trust requires a canonical definition digest");
        }
        Ok(Self {
            producer,
            workflow,
            definition_version,
            definition_digest,
        })
    }
}

/// 业务作用：参与方运行时——持有 store/inbox/outbox 句柄与本服务的 Inbox consumer 名。
///
/// 无进程内业务状态；并发安全由 gate 行锁与 Inbox 唯一键承担。
pub struct ParticipantRuntime {
    store: MySqlSagaStore,
    inbox: MySqlInbox,
    outbox: MySqlOutbox,
    consumer: String,
    trusted_command_producers: BTreeSet<ServiceIdentity>,
    command_trust: BTreeMap<(WorkflowName, DefinitionVersion, String), ServiceIdentity>,
}

/// 业务作用：表示已命中至少一条 Participant 信任投影的 command producer 能力。
///
/// 非 Kafka transport 应在校验 mTLS、签名或独占通道后创建本视图；其后所有 phase 调用都复用
/// 同一受信身份，避免某个分支遗漏 producer 参数又退回未认证入口。
pub struct AuthenticatedParticipantRuntime<'a> {
    runtime: &'a ParticipantRuntime,
    producer: &'a ServiceIdentity,
}

impl ParticipantRuntime {
    /// 业务作用：构造参与方运行时。
    ///
    /// 参数说明：
    /// - `consumer`: 本服务的小写 canonical Inbox consumer 名称（各服务唯一，去重命名空间）。
    /// - `command_trust`: 从受信 definition/deployment 投影得到的精确 Orchestrator 授权边。
    ///
    /// 返回：consumer 满足 128 字节小写 canonical Inbox key 时返回不建连的运行时；
    /// 非法去重命名空间或空信任投影返回错误，避免未认证 command 获得业务入口。
    pub fn new(
        consumer: impl Into<String>,
        command_trust: impl IntoIterator<Item = ParticipantCommandTrust>,
    ) -> anyhow::Result<Self> {
        let consumer = consumer.into();
        if consumer.is_empty()
            || consumer.trim() != consumer
            || consumer.len() > 128
            || consumer.contains('\0')
            || !consumer.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'_' | b'-' | b'.')
            })
        {
            anyhow::bail!("participant consumer must satisfy the Inbox key contract");
        }
        let mut trusted_command_producers = BTreeSet::new();
        let mut projected_trust = BTreeMap::new();
        for trust in command_trust {
            trusted_command_producers.insert(trust.producer.clone());
            let key = (
                trust.workflow,
                trust.definition_version,
                trust.definition_digest,
            );
            if projected_trust.insert(key, trust.producer).is_some() {
                anyhow::bail!(
                    "participant command trust contains a duplicate definition projection"
                );
            }
        }
        if projected_trust.is_empty() {
            anyhow::bail!("participant requires at least one command trust projection");
        }
        Ok(Self {
            store: MySqlSagaStore::new(),
            inbox: MySqlInbox::new(),
            outbox: MySqlOutbox::new(),
            consumer,
            trusted_command_producers,
            command_trust: projected_trust,
        })
    }

    /// 业务作用：把 transport 已认证 producer 收窄成只能调用 authenticated phase API 的能力视图。
    ///
    /// 参数说明：
    /// - `producer`: transport 从 mTLS/SASL principal、独占 topic 或签名 key 映射出的身份。
    ///
    /// 返回：身份至少拥有一条启动投影时返回能力视图；否则在解析 envelope 与接触数据库前拒绝；
    /// 每个 phase 仍会按该 envelope 的 workflow/version/digest 再做精确授权。
    pub fn authenticate<'a>(
        &'a self,
        producer: &'a ServiceIdentity,
    ) -> anyhow::Result<AuthenticatedParticipantRuntime<'a>> {
        if !self.trusted_command_producers.contains(producer) {
            return Err(crate::SagaCommandProcessingError::ProducerUnauthorized.into());
        }
        Ok(AuthenticatedParticipantRuntime {
            runtime: self,
            producer,
        })
    }

    /// 业务作用：为自定义 connector 预检 command envelope 与 producer 的精确授权投影。
    ///
    /// 该预检不替代 phase handler 内的强制复验；它用于在解析业务 payload、获取连接或触碰 Inbox
    /// 之前给 HTTP/gRPC adapter 一个统一的 fail-closed 入口。
    ///
    /// 参数说明：
    /// - `envelope`: transport 已校验完整性、但尚未获业务授权的原始 command envelope。
    /// - `producer`: transport 从签名、mTLS 或独占 route 映射出的逻辑 Orchestrator。
    ///
    /// 返回：身份派生与 workflow/version/digest/producer 投影全部匹配时成功；否则类型化拒绝。
    pub fn verify_authenticated_command(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<()> {
        self.verified_command_identity(envelope, producer).map(drop)
    }

    /// 业务作用：execute 命令的完整本地事务 wrapper——gate 准入、业务执行、结果发布。
    ///
    /// 参数说明：
    /// - `service`: 业务步骤实现。
    /// - `envelope`: 已通过 transport 层 producer 认证的命令 envelope。
    /// - `producer`: transport 认证并映射出的 Orchestrator 逻辑身份。
    /// - `allow_unknown`: descriptor 声明的 `allow_unknown`；纯本地步骤返回 `Unknown`
    ///   会按合同违规提交 `Halted`（不能进入无界等待）。
    ///
    /// 返回：可 ACK 的处理结论（COMMIT 已确认）；`Retryable`、身份复验失败或基础设施
    /// 失败返回错误（已回滚，不得 ACK）。
    pub async fn handle_authenticated_execute<S>(
        &self,
        service: &S,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
        allow_unknown: bool,
    ) -> anyhow::Result<ParticipantHandled>
    where
        S: SagaStep,
        S::Command: DeserializeOwned,
    {
        let identity = self.verified_command_identity(envelope, producer)?;
        if identity.phase != StepPhase::Execute {
            return Err(crate::SagaCommandProcessingError::ContractInvalid.into());
        }
        crate::transaction::run(async {
            if matches!(
                self.inbox
                    .claim(&self.consumer, &envelope.command_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(ParticipantHandled::Duplicate);
            }
            let gate = ParticipantGateKey {
                saga_id: &identity.saga_id,
                step: &identity.step,
                tenant: &identity.tenant,
                workflow: &identity.workflow,
                definition_version: identity.definition_version,
                definition_digest: &envelope.definition_digest,
            };
            match self.store.admit_execute(&gate, &identity.effect).await? {
                // 取消屏障已建立:只提交 Inbox claim,零正向效果;不伪造 StepRejected,
                // 取消事实由先前同事务写出的 CancelConfirmed Outbox 证明。
                ExecuteAdmission::Suppressed => Ok(ParticipantHandled::Suppressed),
                // 新 attempt 重投命中既有终态:原响应可能丢失,按既有终态以本 attempt
                // 的身份重发结果事件,零业务效果。
                ExecuteAdmission::AlreadyTerminal(terminal) => {
                    let status = forward_to_attempt_status(terminal)?;
                    self.emit_result(envelope, status, None, None).await?;
                    Ok(ParticipantHandled::Replayed(status))
                }
                ExecuteAdmission::Admitted => {
                    let context = self.context_of(&identity, envelope)?;
                    let command: S::Command =
                        serde_json::from_value(envelope.payload.clone().unwrap_or_default())
                            .map_err(|_| crate::SagaCommandProcessingError::ContractInvalid)?;
                    match service.execute(&context, command).await {
                        Ok(SagaOutcome::Succeeded(_)) => {
                            self.store
                                .settle_execute(
                                    &identity.saga_id,
                                    &identity.step,
                                    StepForwardStatus::Succeeded,
                                )
                                .await?;
                            self.emit_result(envelope, StepAttemptStatus::Succeeded, None, None)
                                .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Succeeded))
                        }
                        Ok(SagaOutcome::Rejected { code }) => {
                            // 确定性拒绝是可提交结论:拒绝事实与 Inbox、结果事件原子提交。
                            self.store
                                .settle_execute(
                                    &identity.saga_id,
                                    &identity.step,
                                    StepForwardStatus::Rejected,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Rejected,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Rejected))
                        }
                        Ok(SagaOutcome::Unknown { code }) => {
                            if allow_unknown {
                                self.store
                                    .settle_execute(
                                        &identity.saga_id,
                                        &identity.step,
                                        StepForwardStatus::Unknown,
                                    )
                                    .await?;
                                self.emit_result(
                                    envelope,
                                    StepAttemptStatus::Unknown,
                                    None,
                                    Some(safe_reason_code(code)),
                                )
                                .await?;
                                Ok(ParticipantHandled::Executed(StepAttemptStatus::Unknown))
                            } else {
                                // 纯本地步骤谎称 Unknown = 合同违规:提交 Halted 冻结事实
                                // 并告警,不能让"未知"进入无界等待。
                                self.store
                                    .settle_execute(
                                        &identity.saga_id,
                                        &identity.step,
                                        StepForwardStatus::Halted,
                                    )
                                    .await?;
                                self.emit_result(
                                    envelope,
                                    StepAttemptStatus::Halted,
                                    None,
                                    Some("unknown_not_allowed"),
                                )
                                .await?;
                                Ok(ParticipantHandled::Executed(StepAttemptStatus::Halted))
                            }
                        }
                        Ok(SagaOutcome::Halted { code }) => {
                            self.store
                                .settle_execute(
                                    &identity.saga_id,
                                    &identity.step,
                                    StepForwardStatus::Halted,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Halted,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Halted))
                        }
                        // 本次尝试没有可提交结论:回滚全部本地写并保留输入消息重投。
                        Err(SagaExecutionError::Retryable { code }) => {
                            anyhow::bail!("retryable execution error: {code}")
                        }
                    }
                }
            }
        })
        .await
    }

    /// 业务作用：`local_fenceable` 步骤的取消命令 wrapper——在 gate 行锁上完成取消屏障裁决。
    ///
    /// 参数说明：
    /// - `envelope`: 已认证的取消命令 envelope。
    /// - `producer`: transport 认证并映射出的 Orchestrator 逻辑身份。
    ///
    /// 返回：可 ACK 的裁决结论（Confirmed/AlreadyTerminal/ResolutionPending 之一已
    /// 连同结果事件提交）；基础设施失败返回错误（已回滚，不得 ACK）。
    pub async fn handle_authenticated_cancel_local(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled> {
        let identity = self.verified_command_identity(envelope, producer)?;
        if identity.phase != StepPhase::Cancel {
            return Err(crate::SagaCommandProcessingError::ContractInvalid.into());
        }
        crate::transaction::run(async {
            if matches!(
                self.inbox
                    .claim(&self.consumer, &envelope.command_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(ParticipantHandled::Duplicate);
            }
            let gate = ParticipantGateKey {
                saga_id: &identity.saga_id,
                step: &identity.step,
                tenant: &identity.tenant,
                workflow: &identity.workflow,
                definition_version: identity.definition_version,
                definition_digest: &envelope.definition_digest,
            };
            // 取消与 execute 竞争同一效果身份:execute effect 由参与方本地派生,
            // 不信任命令自带的任何执行身份。
            let execute_effect = EffectId::derive(
                &identity.saga_id,
                identity.definition_version,
                &identity.step,
                StepPhase::Execute,
            );
            let (status, terminal) = match self
                .store
                .adjudicate_cancel(&gate, &execute_effect, &identity.effect)
                .await?
            {
                CancelAdjudication::Confirmed => (StepAttemptStatus::CancelConfirmed, None),
                CancelAdjudication::AlreadyTerminal(forward) => {
                    (StepAttemptStatus::AlreadyTerminal, Some(forward))
                }
                CancelAdjudication::ResolutionPending => {
                    (StepAttemptStatus::ResolutionPending, None)
                }
            };
            self.emit_result(envelope, status, terminal, None).await?;
            Ok(ParticipantHandled::Executed(status))
        })
        .await
    }

    /// 业务作用：externally-cancellable 步骤的类型化取消事务 wrapper。
    ///
    /// 固定顺序为 Inbox claim → gate 行锁/取消准入 → 外部 cancel handler → 真实裁决落账 →
    /// result Outbox → COMMIT。handler 调用期间 gate 权威持续持有，使并发 execute 不可能越过
    /// 已确认取消；`ResolutionPending` 如实保留 Unknown，交 Poll resolver 收敛。
    ///
    /// 参数说明：
    /// - `service`: 同时托管步骤取消能力的业务 Service。
    /// - `envelope`: 已通过 transport owner/route 认证的 cancel command。
    /// - `producer`: transport 认证并映射出的 Orchestrator 逻辑身份。
    ///
    /// 返回：真实裁决与结果事件 COMMIT 后返回可 ACK 结论；`Retryable`、payload 合同、失权
    /// 或基础设施失败返回错误并回滚，transport 不得 ACK。
    pub async fn handle_authenticated_cancel_external<C>(
        &self,
        service: &C,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled>
    where
        C: SagaCancelStep,
        C::Command: DeserializeOwned,
    {
        let identity = self.verified_command_identity(envelope, producer)?;
        if identity.phase != StepPhase::Cancel {
            return Err(crate::SagaCommandProcessingError::ContractInvalid.into());
        }
        crate::transaction::run(async {
            if matches!(
                self.inbox
                    .claim(&self.consumer, &envelope.command_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(ParticipantHandled::Duplicate);
            }
            let gate = ParticipantGateKey {
                saga_id: &identity.saga_id,
                step: &identity.step,
                tenant: &identity.tenant,
                workflow: &identity.workflow,
                definition_version: identity.definition_version,
                definition_digest: &envelope.definition_digest,
            };
            let execute_effect = EffectId::derive(
                &identity.saga_id,
                identity.definition_version,
                &identity.step,
                StepPhase::Execute,
            );
            match self
                .store
                .admit_external_cancel(&gate, &execute_effect, &identity.effect)
                .await?
            {
                ExternalCancelAdmission::AlreadyAdjudicated(adjudication) => {
                    // 已提交裁决只能重放 result；再次调用外部取消接口可能把幂等缺陷放大为
                    // 误取消别的业务请求，因此 gate 事实优先于新 delivery attempt。
                    let (status, terminal) = cancel_adjudication_result(adjudication);
                    self.emit_result(envelope, status, terminal, None).await?;
                    Ok(ParticipantHandled::Replayed(status))
                }
                ExternalCancelAdmission::Admitted => {
                    // cancel 操作自己的 effect 用于取消请求幂等；目标 effect 必须指向原
                    // execute，否则业务 API 会稳定地取消一条不存在的 cancel-phase 请求。
                    let context = self
                        .context_of(&identity, envelope)?
                        .with_target_phase(StepPhase::Execute);
                    let command: C::Command = serde_json::from_value(
                        envelope.payload.clone().unwrap_or(serde_json::Value::Null),
                    )
                    .map_err(|_| crate::SagaCommandProcessingError::ContractInvalid)?;
                    match service.cancel(&context, command).await {
                        Ok(outcome) => {
                            let (forward, cancel, status, terminal, reason) =
                                external_cancel_result(outcome);
                            let reason = reason.map(safe_reason_code);
                            // 外部真实裁决、gate 与 result Outbox 同事务提交；先发 result 会让
                            // Orchestrator 相信一个在参与方崩溃后并不存在的取消屏障。
                            self.store
                                .settle_external_cancel(
                                    &identity.saga_id,
                                    &identity.step,
                                    forward,
                                    cancel,
                                )
                                .await?;
                            self.emit_result(envelope, status, terminal, reason).await?;
                            Ok(ParticipantHandled::Executed(status))
                        }
                        Err(SagaExecutionError::Retryable { code }) => {
                            // 未获真实裁决时回滚 PENDING gate、Inbox 与所有业务写，让同一
                            // command 保持可重投；绝不能把调用失败当作取消成功。
                            anyhow::bail!("retryable external cancel error: {code}")
                        }
                    }
                }
            }
        })
        .await
    }

    /// 业务作用：补偿命令的完整本地事务 wrapper——按 `effect_id` 幂等撤销正向效果。
    ///
    /// 参数说明：
    /// - `service`: 业务步骤实现。
    /// - `envelope`: 已认证的补偿命令 envelope。
    /// - `producer`: transport 认证并映射出的 Orchestrator 逻辑身份。
    ///
    /// 返回：可 ACK 的处理结论；补偿已成功时按幂等成功重发结果；本地无正向成功效果
    /// 时提交 `Halted` 合同违规事实（[`ParticipantHandled::ContractViolation`]）；
    /// `Retryable` 或基础设施失败返回错误（已回滚，不得 ACK）。
    pub async fn handle_authenticated_compensate<S>(
        &self,
        service: &S,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled>
    where
        S: SagaStep,
        S::Compensation: DeserializeOwned,
    {
        let identity = self.verified_command_identity(envelope, producer)?;
        if identity.phase != StepPhase::Compensate {
            return Err(crate::SagaCommandProcessingError::ContractInvalid.into());
        }
        crate::transaction::run(async {
            if matches!(
                self.inbox
                    .claim(&self.consumer, &envelope.command_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(ParticipantHandled::Duplicate);
            }
            let gate = ParticipantGateKey {
                saga_id: &identity.saga_id,
                step: &identity.step,
                tenant: &identity.tenant,
                workflow: &identity.workflow,
                definition_version: identity.definition_version,
                definition_digest: &envelope.definition_digest,
            };
            match self
                .store
                .admit_compensation(
                    &gate,
                    &identity.effect,
                    envelope.recovery_operation_id.as_deref(),
                )
                .await?
            {
                // 既有补偿事实必须原样重放。尤其 UNKNOWN 可能是退款已生效但响应丢失，
                // 自动再次调用 handler 会把“查询未知”升级成二次退款事故。
                CompensationAdmission::AlreadySettled(terminal) => {
                    let status = compensation_to_attempt_status(terminal)?;
                    let reason = match terminal {
                        StepCompensationStatus::Unknown => Some("compensation_outcome_unknown"),
                        StepCompensationStatus::Halted => Some("compensation_already_halted"),
                        _ => None,
                    };
                    self.emit_result(envelope, status, None, reason).await?;
                    Ok(ParticipantHandled::Replayed(status))
                }
                // 协议破坏:补偿只会对已记账成功的步骤发出。提交违规事实并告警,
                // 不自旋等待正向命令,也不进入无限重试。
                CompensationAdmission::MissingForwardEffect => {
                    self.emit_result(
                        envelope,
                        StepAttemptStatus::Halted,
                        None,
                        Some("compensation_contract_violation"),
                    )
                    .await?;
                    Ok(ParticipantHandled::ContractViolation)
                }
                CompensationAdmission::Admitted => {
                    let context = self.context_of(&identity, envelope)?;
                    // 补偿输入来自持久化事实(默认形态 (a)):命令只带身份,
                    // 业务按 saga_id + step 自行解析要撤销的内容。
                    let command: S::Compensation =
                        serde_json::from_value(envelope.payload.clone().unwrap_or_default())
                            .map_err(|_| crate::SagaCommandProcessingError::ContractInvalid)?;
                    match service.compensate(&context, command).await {
                        Ok(CompensationOutcome::Succeeded) => {
                            self.store
                                .settle_compensation(
                                    &identity.saga_id,
                                    &identity.step,
                                    StepCompensationStatus::Succeeded,
                                )
                                .await?;
                            self.emit_result(envelope, StepAttemptStatus::Succeeded, None, None)
                                .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Succeeded))
                        }
                        Ok(CompensationOutcome::Unknown { code }) => {
                            self.store
                                .settle_compensation(
                                    &identity.saga_id,
                                    &identity.step,
                                    StepCompensationStatus::Unknown,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Unknown,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Unknown))
                        }
                        Ok(CompensationOutcome::Halted { code }) => {
                            self.store
                                .settle_compensation(
                                    &identity.saga_id,
                                    &identity.step,
                                    StepCompensationStatus::Halted,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Halted,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Halted))
                        }
                        Err(SagaExecutionError::Retryable { code }) => {
                            anyhow::bail!("retryable compensation error: {code}")
                        }
                    }
                }
            }
        })
        .await
    }

    /// 业务作用：运行 `poll` 解决命令的类型化本地事务，把未知外部效果收敛为唯一裁决。
    ///
    /// Inbox claim、gate 方向裁决、查询结论与结果 Outbox 在同一事务提交；`Unknown` 只保持
    /// `PENDING` 供下一有界 attempt 继续查询，确定终态以后新 attempt 只能重放，避免重复调用
    /// 可能带 void/cancel-inquiry 副作用的裁决型接口。
    ///
    /// 参数说明：
    /// - `service`: 实现 [`SagaResolveStep`] 的类型化查询服务。
    /// - `envelope`: 已通过 transport producer 认证的 resolve 命令。
    /// - `producer`: transport 认证并映射出的 Orchestrator 逻辑身份。
    ///
    /// 返回：事务提交后返回可 ACK 结论；`Retryable`、身份复验、方向合同或基础设施失败
    /// 返回错误并回滚，transport 不得 ACK。
    pub async fn handle_authenticated_resolve<R>(
        &self,
        service: &R,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<ParticipantHandled>
    where
        R: SagaResolveStep,
        R::Command: DeserializeOwned,
    {
        let identity = self.verified_command_identity(envelope, producer)?;
        if identity.phase != StepPhase::Resolve {
            return Err(crate::SagaCommandProcessingError::ContractInvalid.into());
        }
        crate::transaction::run(async {
            if matches!(
                self.inbox
                    .claim(&self.consumer, &envelope.command_id)
                    .await?,
                InboxClaim::Duplicate
            ) {
                return Ok(ParticipantHandled::Duplicate);
            }
            let gate = ParticipantGateKey {
                saga_id: &identity.saga_id,
                step: &identity.step,
                tenant: &identity.tenant,
                workflow: &identity.workflow,
                definition_version: identity.definition_version,
                definition_digest: &envelope.definition_digest,
            };
            match self
                .store
                .admit_resolution(
                    &gate,
                    &identity.effect,
                    envelope.recovery_operation_id.as_deref(),
                )
                .await?
            {
                ResolutionAdmission::AlreadySettled { status, .. } => {
                    let attempt_status = resolution_to_attempt_status(status)?;
                    self.emit_result(envelope, attempt_status, None, None)
                        .await?;
                    Ok(ParticipantHandled::Replayed(attempt_status))
                }
                ResolutionAdmission::MissingUnknownEffect => {
                    // 没有未知业务事实时不得调用查询 handler：凭空查询可能命中其它请求，
                    // 也会掩盖 command 链断裂。提交 Halted 供 Orchestrator 转人工审计。
                    self.emit_result(
                        envelope,
                        StepAttemptStatus::Halted,
                        None,
                        Some("resolution_contract_violation"),
                    )
                    .await?;
                    Ok(ParticipantHandled::ContractViolation)
                }
                ResolutionAdmission::Admitted(target) => {
                    // gate 已判定本次查询裁决正向还是补偿；把目标 phase/effect 明确交给
                    // resolver，禁止业务仅凭 resolve effect 猜测并查错远端请求。
                    let target_phase = match target {
                        nasaga_mysql::ResolutionTarget::Forward => StepPhase::Execute,
                        nasaga_mysql::ResolutionTarget::Compensation => StepPhase::Compensate,
                    };
                    let context = self
                        .context_of(&identity, envelope)?
                        .with_target_phase(target_phase);
                    let command: R::Command =
                        serde_json::from_value(envelope.payload.clone().unwrap_or_default())
                            .map_err(|_| crate::SagaCommandProcessingError::ContractInvalid)?;
                    match service.resolve(&context, command).await {
                        Ok(SagaOutcome::Succeeded(_)) => {
                            self.store
                                .settle_resolution(
                                    &identity.saga_id,
                                    &identity.step,
                                    target,
                                    StepResolutionStatus::Succeeded,
                                )
                                .await?;
                            self.emit_result(envelope, StepAttemptStatus::Succeeded, None, None)
                                .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Succeeded))
                        }
                        Ok(SagaOutcome::Rejected { code }) => {
                            // Rejected 只能由 handler 在排除迟到生效后给出；本层原样可靠提交，
                            // 不把普通“查无记录”自行升级成确定拒绝。
                            self.store
                                .settle_resolution(
                                    &identity.saga_id,
                                    &identity.step,
                                    target,
                                    StepResolutionStatus::Rejected,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Rejected,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Rejected))
                        }
                        Ok(SagaOutcome::Unknown { code }) => {
                            self.store
                                .settle_resolution(
                                    &identity.saga_id,
                                    &identity.step,
                                    target,
                                    StepResolutionStatus::Pending,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Unknown,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Unknown))
                        }
                        Ok(SagaOutcome::Halted { code }) => {
                            self.store
                                .settle_resolution(
                                    &identity.saga_id,
                                    &identity.step,
                                    target,
                                    StepResolutionStatus::Halted,
                                )
                                .await?;
                            self.emit_result(
                                envelope,
                                StepAttemptStatus::Halted,
                                None,
                                Some(safe_reason_code(code)),
                            )
                            .await?;
                            Ok(ParticipantHandled::Executed(StepAttemptStatus::Halted))
                        }
                        Err(SagaExecutionError::Retryable { code }) => {
                            // 没有裁决时回滚 Inbox 与 PENDING 写入，原 command 保持可重投。
                            anyhow::bail!("retryable resolution error: {code}")
                        }
                    }
                }
            }
        })
        .await
    }

    /// 业务作用：复验 command 身份并把 producer 绑定到该 definition 的精确信任投影。
    ///
    /// 这里必须先完成身份派生，再按 workflow/version/digest 查授权；任一失败都发生在获取数据库
    /// 连接和 Inbox claim 前，避免跨业务域 Orchestrator 触发未授权外部效果。
    ///
    /// 参数说明：
    /// - `envelope`: 待复验的 command envelope。
    /// - `producer`: transport 认证得到的逻辑 Orchestrator。
    ///
    /// 返回：envelope 身份和精确信任投影一致时返回已证身份；否则返回类型化拒绝。
    fn verified_command_identity(
        &self,
        envelope: &SagaCommandEnvelope,
        producer: &ServiceIdentity,
    ) -> anyhow::Result<crate::envelope::VerifiedIdentity> {
        let identity = envelope
            .verified()
            .map_err(|_| crate::SagaCommandProcessingError::IdentityInvalid)?;
        self.verify_command_producer(producer, &identity)?;
        Ok(identity)
    }

    /// 业务作用：把 transport 认证身份绑定到 envelope 指向的 workflow definition 投影。
    ///
    /// 参数说明：
    /// - `producer`: mTLS/SASL principal、独占 topic 或端到端签名映射出的逻辑身份。
    /// - `identity`: 已完成派生复验的 command 身份。
    ///
    /// 返回：producer 与精确 projection 一致时成功；不存在或被另一 Orchestrator 所有时类型化拒绝。
    fn verify_command_producer(
        &self,
        producer: &ServiceIdentity,
        identity: &crate::envelope::VerifiedIdentity,
    ) -> anyhow::Result<()> {
        let key = (
            identity.workflow.clone(),
            identity.definition_version,
            identity.definition_digest.clone(),
        );
        if self.command_trust.get(&key) != Some(producer) {
            return Err(crate::SagaCommandProcessingError::ProducerUnauthorized.into());
        }
        Ok(())
    }

    /// 业务作用：由已证身份构造只读业务上下文，身份由框架派生而不是业务自造。
    ///
    /// 参数说明：
    /// - `identity`: envelope 已证身份。
    /// - `envelope`: 原始 envelope（提供 digest 文本）。
    ///
    /// 返回：可传给业务 handler 的上下文。
    fn context_of(
        &self,
        identity: &crate::envelope::VerifiedIdentity,
        envelope: &SagaCommandEnvelope,
    ) -> anyhow::Result<SagaContext> {
        Ok(SagaContext::new(
            identity.saga_id.clone(),
            identity.tenant.clone(),
            identity.workflow.clone(),
            identity.definition_version,
            envelope.definition_digest.clone(),
            identity.step.clone(),
            identity.phase,
            identity.attempt,
        ))
    }

    /// 业务作用：把处理结论以稳定身份写入结果 Outbox，与业务写同事务发布。
    ///
    /// result 事件 id 由命令身份确定性派生：参与方事务重试不会为同一结果占用两个
    /// 去重身份。
    ///
    /// 参数说明：
    /// - `envelope`: 触发本结果的命令 envelope。
    /// - `status`: 处理结论。
    /// - `terminal`: 取消裁决 `ALREADY_TERMINAL` 携带的正向真实终态。
    /// - `reason_code`: 稳定原因码。
    ///
    /// 返回：写入成功返回 `Ok`。
    async fn emit_result(
        &self,
        envelope: &SagaCommandEnvelope,
        status: StepAttemptStatus,
        terminal: Option<StepForwardStatus>,
        reason_code: Option<&str>,
    ) -> anyhow::Result<()> {
        let result = SagaResultEnvelope {
            event_id: derive_result_event_id(&envelope.command_id),
            saga_id: envelope.saga_id.clone(),
            tenant_id: envelope.tenant_id.clone(),
            workflow: envelope.workflow.clone(),
            definition_version: envelope.definition_version,
            definition_digest: envelope.definition_digest.clone(),
            step: envelope.step.clone(),
            phase: envelope.phase.clone(),
            attempt: envelope.attempt,
            effect_id: envelope.effect_id.clone(),
            command_id: envelope.command_id.clone(),
            status: status.as_str().to_string(),
            terminal_status: terminal.map(|value| value.as_str().to_string()),
            reason_code: reason_code.map(str::to_string),
        };
        // 业务 reason 已在 handler 分支收敛为安全码；这里仍用完整 producer 合同
        // 拦截框架状态/身份漂移。这类确定性编程错误必须回滚并隔离，不得
        // 伪装成数据库瞬态故障耗尽重试预算。
        let event = result
            .to_outbox_event()
            .map_err(|_| crate::SagaCommandProcessingError::ContractInvalid)?;
        self.outbox.append_transactional(&event).await?;
        Ok(())
    }
}

impl AuthenticatedParticipantRuntime<'_> {
    /// 业务作用：以已绑定 producer 执行 execute 的完整本地事务。
    ///
    /// 参数说明：
    /// - `service`: 业务步骤实现。
    /// - `envelope`: 已由同一 transport 收据认证的命令。
    /// - `allow_unknown`: definition 是否允许正向 Unknown。
    ///
    /// 返回：事务提交后返回可 ACK 结论；否则返回拒绝或可重试错误。
    pub async fn execute<S>(
        &self,
        service: &S,
        envelope: &SagaCommandEnvelope,
        allow_unknown: bool,
    ) -> anyhow::Result<ParticipantHandled>
    where
        S: SagaStep,
        S::Command: DeserializeOwned,
    {
        self.runtime
            .handle_authenticated_execute(service, envelope, self.producer, allow_unknown)
            .await
    }

    /// 业务作用：以已绑定 producer 执行本地可围栏取消裁决。
    ///
    /// 参数说明：
    /// - `envelope`: 已由同一 transport 收据认证的取消命令。
    ///
    /// 返回：取消屏障与结果提交后返回可 ACK 结论；否则返回拒绝或基础设施错误。
    pub async fn cancel_local(
        &self,
        envelope: &SagaCommandEnvelope,
    ) -> anyhow::Result<ParticipantHandled> {
        self.runtime
            .handle_authenticated_cancel_local(envelope, self.producer)
            .await
    }

    /// 业务作用：以已绑定 producer 执行类型化外部取消裁决。
    ///
    /// 参数说明：
    /// - `service`: 外部取消服务实现。
    /// - `envelope`: 已认证取消命令。
    ///
    /// 返回：外部裁决、gate 与结果原子提交后返回可 ACK 结论；否则返回错误。
    pub async fn cancel_external<C>(
        &self,
        service: &C,
        envelope: &SagaCommandEnvelope,
    ) -> anyhow::Result<ParticipantHandled>
    where
        C: SagaCancelStep,
        C::Command: DeserializeOwned,
    {
        self.runtime
            .handle_authenticated_cancel_external(service, envelope, self.producer)
            .await
    }

    /// 业务作用：以已绑定 producer 执行补偿或受审计 HALTED 恢复。
    ///
    /// 参数说明：
    /// - `service`: 补偿业务实现。
    /// - `envelope`: 已认证补偿命令。
    ///
    /// 返回：补偿事实与结果提交后返回可 ACK 结论；否则返回拒绝或可重试错误。
    pub async fn compensate<S>(
        &self,
        service: &S,
        envelope: &SagaCommandEnvelope,
    ) -> anyhow::Result<ParticipantHandled>
    where
        S: SagaStep,
        S::Compensation: DeserializeOwned,
    {
        self.runtime
            .handle_authenticated_compensate(service, envelope, self.producer)
            .await
    }

    /// 业务作用：以已绑定 producer 执行 Unknown 查询或受审计 HALTED resolve 恢复。
    ///
    /// 参数说明：
    /// - `service`: 类型化查询实现。
    /// - `envelope`: 已认证 resolve 命令。
    ///
    /// 返回：查询裁决与结果提交后返回可 ACK 结论；否则返回拒绝或可重试错误。
    pub async fn resolve<R>(
        &self,
        service: &R,
        envelope: &SagaCommandEnvelope,
    ) -> anyhow::Result<ParticipantHandled>
    where
        R: SagaResolveStep,
        R::Command: DeserializeOwned,
    {
        self.runtime
            .handle_authenticated_resolve(service, envelope, self.producer)
            .await
    }
}

/// 业务作用：校验信任投影中的 definition digest 使用唯一的 canonical 文本表示。
///
/// 参数说明：
/// - `digest`: 待校验的摘要文本。
///
/// 返回：恰好 64 字符且仅含小写十六进制字符时为 `true`。
fn valid_definition_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// 业务作用：把业务 handler 返回的非规范原因码收敛为稳定低敏码，避免已发生外部效果后
/// 因 result 合同校验失败回滚 gate/Inbox，继而在下一 attempt 重复调用外部系统。
///
/// 参数说明：
/// - `code`: handler 返回的静态原因码；不得记录其原文，避免把误填敏感文本写入日志。
///
/// 返回：合法时原样返回；非法时返回可持久化的统一合同违规码。
fn safe_reason_code(code: &'static str) -> &'static str {
    if valid_reason_code(code) {
        code
    } else {
        tracing::error!(
            reason_code_valid = false,
            "Saga 参与方原因码不符合传输合同，已安全降级并保留业务裁决"
        );
        "participant_reason_code_invalid"
    }
}

/// 业务作用：把 gate 的正向终态映射为 attempt 结果词汇，用于终态重放。
///
/// 参数说明：
/// - `terminal`: gate 行的正向终态。
///
/// 返回：对应的 attempt 状态；`PENDING`/`CANCELLED`/`TIMED_OUT` 不是可重放终态，返回错误。
fn forward_to_attempt_status(terminal: StepForwardStatus) -> anyhow::Result<StepAttemptStatus> {
    match terminal {
        StepForwardStatus::Succeeded => Ok(StepAttemptStatus::Succeeded),
        StepForwardStatus::Rejected => Ok(StepAttemptStatus::Rejected),
        StepForwardStatus::Unknown => Ok(StepAttemptStatus::Unknown),
        StepForwardStatus::Halted => Ok(StepAttemptStatus::Halted),
        _ => anyhow::bail!("gate terminal status is not replayable"),
    }
}

/// 业务作用：把 store 中既有取消裁决映射为可重发的 result envelope 字段。
///
/// 参数说明：
/// - `adjudication`: gate 已提交的取消裁决。
///
/// 返回：对应 attempt 状态与 `ALREADY_TERMINAL` 正向终态。
fn cancel_adjudication_result(
    adjudication: CancelAdjudication,
) -> (StepAttemptStatus, Option<StepForwardStatus>) {
    match adjudication {
        CancelAdjudication::Confirmed => (StepAttemptStatus::CancelConfirmed, None),
        CancelAdjudication::AlreadyTerminal(forward) => {
            (StepAttemptStatus::AlreadyTerminal, Some(forward))
        }
        CancelAdjudication::ResolutionPending => (StepAttemptStatus::ResolutionPending, None),
    }
}

/// 业务作用：把类型化外部取消真实裁决投影为 participant gate 与 result 协议字段。
///
/// 参数说明：
/// - `outcome`: `SagaCancelStep` 返回的封闭裁决。
///
/// 返回：一致的 forward/cancel 状态、attempt 结果、可选正向终态与稳定原因码。
#[allow(clippy::type_complexity)]
fn external_cancel_result(
    outcome: CancelOutcome,
) -> (
    StepForwardStatus,
    StepCancelStatus,
    StepAttemptStatus,
    Option<StepForwardStatus>,
    Option<&'static str>,
) {
    match outcome {
        CancelOutcome::CancelConfirmed => (
            StepForwardStatus::Cancelled,
            StepCancelStatus::Confirmed,
            StepAttemptStatus::CancelConfirmed,
            None,
            None,
        ),
        CancelOutcome::AlreadyTerminal(terminal) => {
            let (forward, reason) = match terminal {
                TerminalForwardOutcome::Succeeded => (StepForwardStatus::Succeeded, None),
                TerminalForwardOutcome::Rejected { code } => {
                    (StepForwardStatus::Rejected, Some(code))
                }
                TerminalForwardOutcome::Halted { code } => (StepForwardStatus::Halted, Some(code)),
            };
            (
                forward,
                StepCancelStatus::AlreadyTerminal,
                StepAttemptStatus::AlreadyTerminal,
                Some(forward),
                reason,
            )
        }
        CancelOutcome::ResolutionPending { code } => (
            StepForwardStatus::Unknown,
            StepCancelStatus::ResolutionPending,
            StepAttemptStatus::ResolutionPending,
            None,
            Some(code),
        ),
    }
}

/// 业务作用：把参与方 gate 的补偿既有事实映射为 attempt 结果，用于安全重放。
///
/// 参数说明：
/// - `terminal`: gate 中已提交的补偿状态。
///
/// 返回：`Succeeded/Unknown/Halted` 返回同语义 attempt 状态；`None/Pending` 不是可重放
/// 事实，返回错误并拒绝伪造结果。
fn compensation_to_attempt_status(
    terminal: StepCompensationStatus,
) -> anyhow::Result<StepAttemptStatus> {
    match terminal {
        StepCompensationStatus::Succeeded => Ok(StepAttemptStatus::Succeeded),
        StepCompensationStatus::Unknown => Ok(StepAttemptStatus::Unknown),
        StepCompensationStatus::Halted => Ok(StepAttemptStatus::Halted),
        StepCompensationStatus::None | StepCompensationStatus::Pending => {
            anyhow::bail!("compensation gate status is not replayable")
        }
    }
}

/// 业务作用：把参与方 gate 的解决终态映射为 attempt 结果，供确定裁决幂等重放。
///
/// 参数说明：
/// - `status`: gate 中已提交的解决状态。
///
/// 返回：`Succeeded/Rejected/Halted` 返回同语义 attempt 状态；`None/Pending` 尚未形成
/// 确定裁决，返回错误并拒绝伪造结果。
fn resolution_to_attempt_status(status: StepResolutionStatus) -> anyhow::Result<StepAttemptStatus> {
    match status {
        StepResolutionStatus::Succeeded => Ok(StepAttemptStatus::Succeeded),
        StepResolutionStatus::Rejected => Ok(StepAttemptStatus::Rejected),
        StepResolutionStatus::Halted => Ok(StepAttemptStatus::Halted),
        StepResolutionStatus::None | StepResolutionStatus::Pending => {
            anyhow::bail!("resolution gate status is not replayable")
        }
    }
}
