//! 交给业务 handler 的 Saga 执行上下文。
//!
//! 上下文由框架构造并只读传入：业务不能自行编造 Saga 身份或命令身份，
//! 否则外部幂等键与 Inbox 去重都会失去可追溯的来源。

use crate::identity::{
    AttemptNo, CommandId, DefinitionVersion, EffectId, SagaId, StepName, StepPhase, TenantId,
    WorkflowName,
};

/// 业务作用：承载一次步骤调用的完整身份与定义版本绑定，供业务实现做幂等、审计与外部幂等键使用。
///
/// 字段说明：`effect_id` 是当前 cancel/execute/compensate/resolve 操作的稳定身份；
/// `target_effect_id` 是该操作正在裁决的业务效果，普通阶段与自身相同，external cancel
/// 指向 execute，resolve 指向 execute 或 compensate。`command_id` 只标识本次投递尝试。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaContext {
    saga_id: SagaId,
    tenant_id: TenantId,
    workflow: WorkflowName,
    definition_version: DefinitionVersion,
    definition_digest: String,
    step: StepName,
    phase: StepPhase,
    attempt: AttemptNo,
    effect_id: EffectId,
    target_phase: StepPhase,
    target_effect_id: EffectId,
    command_id: CommandId,
}

impl SagaContext {
    /// 业务作用：构造一次步骤调用的上下文，并在内部确定性派生业务效果与命令身份。
    ///
    /// 身份由框架统一派生而不是由调用方传入，保证同一 `(saga, version, step, phase)`
    /// 在任何进程、任何重试中都得到同一个 `effect_id`；崩溃恢复后重新构造上下文
    /// 也不会得到不同的 `command_id`，因此不会为同一尝试重复占用 Inbox 去重身份。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `tenant_id`: 租户身份，用于隔离与审计关联。
    /// - `workflow`: workflow 名称。
    /// - `definition_version`: 固定到实例的 definition 版本。
    /// - `definition_digest`: 该版本的 canonical 内容摘要，用于校验参与方合同未漂移。
    /// - `step`: 步骤名称。
    /// - `phase`: 本次调用所处阶段。
    /// - `attempt`: 本次投递尝试序号。
    ///
    /// 返回：可只读传给业务 handler 的上下文；不产生任何副作用。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        saga_id: SagaId,
        tenant_id: TenantId,
        workflow: WorkflowName,
        definition_version: DefinitionVersion,
        definition_digest: impl Into<String>,
        step: StepName,
        phase: StepPhase,
        attempt: AttemptNo,
    ) -> Self {
        let effect_id = EffectId::derive(&saga_id, definition_version, &step, phase);
        let command_id = CommandId::derive(&effect_id, attempt);
        Self {
            saga_id,
            tenant_id,
            workflow,
            definition_version,
            definition_digest: definition_digest.into(),
            step,
            phase,
            attempt,
            effect_id,
            target_phase: phase,
            target_effect_id: effect_id,
            command_id,
        }
    }

    /// 业务作用：为取消/解决 adapter 绑定“当前操作正在裁决的原始业务阶段”。
    ///
    /// 当前操作的 `effect_id` 保持不变，用于取消或查询本身的幂等；本方法只派生目标阶段的
    /// `target_effect_id`，让业务查询命中原 execute/compensate 请求而不是 resolve 请求。
    ///
    /// 参数说明：
    /// - `target_phase`: external cancel 固定为 Execute；resolve 由 gate 裁决为 Execute 或 Compensate。
    ///
    /// 返回：携带稳定目标阶段与目标效果身份的新上下文；不产生副作用。
    pub fn with_target_phase(mut self, target_phase: StepPhase) -> Self {
        self.target_effect_id = EffectId::derive(
            &self.saga_id,
            self.definition_version,
            &self.step,
            target_phase,
        );
        self.target_phase = target_phase;
        self
    }

    /// 业务作用：读取实例身份，用于业务侧关联本地事实与审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：实例身份引用。
    pub fn saga_id(&self) -> &SagaId {
        &self.saga_id
    }

    /// 业务作用：读取租户身份，用于业务侧的数据隔离与权限校验。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：租户身份引用。
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// 业务作用：读取 workflow 名称，用于业务分支与可观测标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：workflow 名称引用。
    pub fn workflow(&self) -> &WorkflowName {
        &self.workflow
    }

    /// 业务作用：读取 definition 版本，业务据此选择与该版本一致的处理逻辑。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：definition 版本。
    pub fn definition_version(&self) -> DefinitionVersion {
        self.definition_version
    }

    /// 业务作用：读取 definition 内容摘要，用于参与方核对本地合同未发生漂移。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：definition 摘要字符串切片。
    pub fn definition_digest(&self) -> &str {
        &self.definition_digest
    }

    /// 业务作用：读取步骤名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：步骤名称引用。
    pub fn step(&self) -> &StepName {
        &self.step
    }

    /// 业务作用：读取当前执行阶段，业务据此区分正向、取消、补偿与解决语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：执行阶段。
    pub fn phase(&self) -> StepPhase {
        self.phase
    }

    /// 业务作用：读取本次投递尝试序号，用于诊断与退避观测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尝试序号。
    pub fn attempt(&self) -> AttemptNo {
        self.attempt
    }

    /// 业务作用：读取当前阶段操作跨 attempt 稳定的业务效果身份。
    ///
    /// execute/compensate 用它作为外部请求幂等键；cancel/resolve 用它标识取消/查询操作自身，
    /// 并用 [`Self::target_effect_id`] 定位被裁决的原始业务效果。改用 [`Self::command_id`]
    /// 会让每次新 attempt 都被外部系统当成一次全新请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：业务效果身份。
    pub fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    /// 业务作用：读取当前取消/查询正在裁决的原始业务阶段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：普通 handler 与 [`Self::phase`] 相同；external cancel 为 Execute；resolve 为
    /// gate 已判定的 Execute 或 Compensate。
    pub fn target_phase(&self) -> StepPhase {
        self.target_phase
    }

    /// 业务作用：读取当前操作正在裁决的原始业务效果身份。
    ///
    /// 外部 cancel/resolve 必须用本值查找原 execute/compensate 请求；使用 resolve 自己的
    /// [`Self::effect_id`] 会稳定地查询错误记录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由 saga/version/step/target_phase 确定性派生、跨 attempt 稳定的目标效果身份。
    pub fn target_effect_id(&self) -> EffectId {
        self.target_effect_id
    }

    /// 业务作用：读取本次投递尝试的命令身份，用于 Inbox 去重与投递审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：命令身份。
    pub fn command_id(&self) -> CommandId {
        self.command_id
    }
}
