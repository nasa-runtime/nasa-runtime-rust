//! Saga 已提交实例、步骤和 attempt 的数据库无关快照。

use nasaga_core::{
    AttemptNo, BusinessKey, ControlState, DefinitionVersion, Direction, SagaId, SagaStatus,
    StepAttemptStatus, StepCancelStatus, StepCompensationStatus, StepForwardStatus, StepName,
    StepPhase, StepResolutionStatus, TenantId, WorkflowName,
};

/// 业务作用：保存 Saga 实例的已提交快照，作为 CAS、deadline 和方向裁决输入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaInstanceRow {
    /// 实例身份。
    pub saga_id: SagaId,
    /// 租户身份。
    pub tenant: TenantId,
    /// workflow 名称。
    pub workflow: WorkflowName,
    /// 业务幂等键。
    pub business_key: BusinessKey,
    /// 固定到实例的 definition 版本。
    pub definition_version: DefinitionVersion,
    /// 固定到实例的 definition 内容摘要。
    pub definition_digest: String,
    /// 创建请求 canonical 摘要；迁移前实例可能为空。
    pub start_request_digest: Option<String>,
    /// 业务状态。
    pub status: SagaStatus,
    /// 与业务状态正交的管理控制状态。
    pub control_state: ControlState,
    /// 控制态独立 CAS generation。
    pub control_version: u64,
    /// 推进方向。
    pub direction: Direction,
    /// 当前步骤；实例尚未定位时为空。
    pub current_step: Option<StepName>,
    /// 进入补偿时冻结的计划摘要。
    pub compensation_plan_version: Option<String>,
    /// 乐观并发版本，从一开始单调递增。
    pub version: u64,
    /// 实例级业务 deadline。
    pub deadline_at_ms: Option<i64>,
    /// 最近一次失败的稳定原因码。
    pub failure_code: Option<String>,
    /// 实例最新 canonical W3C traceparent。
    pub traceparent: Option<String>,
}

/// 业务作用：提供不携带 payload 的租户受限实例检索摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaInstanceSummary {
    /// 实例身份。
    pub saga_id: SagaId,
    /// 租户身份。
    pub tenant: TenantId,
    /// workflow 名称。
    pub workflow: WorkflowName,
    /// 业务幂等键。
    pub business_key: BusinessKey,
    /// 固定到实例的 definition 版本。
    pub definition_version: DefinitionVersion,
    /// 业务状态。
    pub status: SagaStatus,
    /// 管理控制状态。
    pub control_state: ControlState,
    /// 推进方向。
    pub direction: Direction,
    /// 当前步骤。
    pub current_step: Option<StepName>,
    /// 乐观并发版本。
    pub version: u64,
    /// 最近一次失败的稳定原因码。
    pub failure_code: Option<String>,
    /// 创建时刻。
    pub created_at_ms: i64,
    /// 最近更新时刻。
    pub updated_at_ms: i64,
}

/// 业务作用：保存 Orchestrator step journal 的当前投影快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaStepRow {
    /// 步骤名称。
    pub step: StepName,
    /// 步骤在 definition 中从一开始的序号。
    pub ordinal: u32,
    /// 正向阶段状态。
    pub forward_status: StepForwardStatus,
    /// 取消屏障裁决状态。
    pub cancel_status: StepCancelStatus,
    /// 补偿阶段状态。
    pub compensation_status: StepCompensationStatus,
    /// 解决阶段状态。
    pub resolution_status: StepResolutionStatus,
    /// execute 当前 attempt 的 effect id 投影。
    pub execute_effect_id: Option<String>,
    /// execute 当前 attempt 的 command id 投影。
    pub execute_command_id: Option<String>,
    /// execute 当前 attempt 序号投影。
    pub execute_attempt: Option<u32>,
    /// cancel 当前 attempt 的 effect id 投影。
    pub cancel_effect_id: Option<String>,
    /// cancel 当前 attempt 的 command id 投影。
    pub cancel_command_id: Option<String>,
    /// cancel 当前 attempt 序号投影。
    pub cancel_attempt: Option<u32>,
    /// compensate 当前 attempt 的 effect id 投影。
    pub compensate_effect_id: Option<String>,
    /// compensate 当前 attempt 的 command id 投影。
    pub compensate_command_id: Option<String>,
    /// compensate 当前 attempt 序号投影。
    pub compensate_attempt: Option<u32>,
    /// resolve 当前 attempt 的 effect id 投影。
    pub resolve_effect_id: Option<String>,
    /// resolve 当前 attempt 的 command id 投影。
    pub resolve_command_id: Option<String>,
    /// resolve 当前 attempt 序号投影。
    pub resolve_attempt: Option<u32>,
    /// 纳入冻结补偿计划时写入的计划摘要。
    pub compensation_plan_version: Option<String>,
    /// 冻结计划内的稳定逆序位置。
    pub compensation_order: Option<u32>,
    /// 最近一次失败的稳定原因码。
    pub last_error_code: Option<String>,
}

/// 业务作用：保存 attempt journal 的完整事实，作为命令去重和迟到结果裁决依据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaStepAttemptRow {
    /// 步骤名称。
    pub step: StepName,
    /// 执行阶段。
    pub phase: StepPhase,
    /// 尝试序号。
    pub attempt: AttemptNo,
    /// 跨 attempt 稳定的业务效果身份。
    pub effect_id: String,
    /// 本次尝试的命令身份。
    pub command_id: String,
    /// 尝试生命周期状态。
    pub status: StepAttemptStatus,
    /// 产生终态的 outcome 事件 id。
    pub outcome_event_id: Option<String>,
}
