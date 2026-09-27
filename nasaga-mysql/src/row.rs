//! 持久化行到强类型快照的解析。
//!
//! 读回路径把稳定文本列解析回 `nasaga-core` 的封闭枚举：解析失败说明数据被绕过 store
//! 写入或发生损坏，必须立即失败并停止基于该实例的自动推进，而不是猜测语义继续执行——
//! 在损坏状态上继续推进可能把已补偿实例重新当作正向实例驱动。

pub(crate) use nasaga_backend::{
    SagaInstanceRow, SagaInstanceSummary, SagaStepAttemptRow, SagaStepRow,
};
use nasaga_core::{
    AttemptNo, BusinessKey, ControlState, DefinitionVersion, Direction, SagaId, SagaStatus,
    StepAttemptStatus, StepCancelStatus, StepCompensationStatus, StepForwardStatus, StepName,
    StepPhase, StepResolutionStatus, TenantId, WorkflowName,
};
use sqlx::{mysql::MySqlRow, Row as _};

use crate::error::{corrupt, map_database, SagaStoreError};

/// 业务作用：从查询结果解析实例快照，把稳定文本列收敛回封闭枚举。
///
/// 参数说明：
/// - `row`: `SELECT` 出的实例行。
///
/// 返回：解析成功返回强类型快照；任一列不在封闭词汇表内或身份非法时返回
/// 标记列名的数据损坏错误，调用方必须停止基于该实例的自动推进。
pub(crate) fn parse_instance_row(row: &MySqlRow) -> Result<SagaInstanceRow, SagaStoreError> {
    let saga_id: String = row.try_get("saga_id").map_err(map_database)?;
    let tenant: String = row.try_get("tenant_id").map_err(map_database)?;
    let workflow: String = row.try_get("workflow_name").map_err(map_database)?;
    let business_key: String = row.try_get("business_key").map_err(map_database)?;
    let definition_version: u32 = row.try_get("definition_version").map_err(map_database)?;
    let status: String = row.try_get("status").map_err(map_database)?;
    let control_state: String = row.try_get("control_state").map_err(map_database)?;
    let direction: String = row.try_get("direction").map_err(map_database)?;
    let current_step: Option<String> = row.try_get("current_step").map_err(map_database)?;
    Ok(SagaInstanceRow {
        saga_id: SagaId::new(saga_id).map_err(|_| corrupt("saga_id"))?,
        tenant: TenantId::new(tenant).map_err(|_| corrupt("tenant_id"))?,
        workflow: WorkflowName::new(workflow).map_err(|_| corrupt("workflow_name"))?,
        business_key: BusinessKey::new(business_key).map_err(|_| corrupt("business_key"))?,
        definition_version: DefinitionVersion::new(definition_version)
            .map_err(|_| corrupt("definition_version"))?,
        definition_digest: row.try_get("definition_digest").map_err(map_database)?,
        start_request_digest: row.try_get("start_request_digest").map_err(map_database)?,
        status: SagaStatus::parse(&status).ok_or_else(|| corrupt("status"))?,
        control_state: ControlState::parse(&control_state)
            .ok_or_else(|| corrupt("control_state"))?,
        control_version: row.try_get("control_version").map_err(map_database)?,
        direction: Direction::parse(&direction).ok_or_else(|| corrupt("direction"))?,
        current_step: current_step
            .map(|step| StepName::new(step).map_err(|_| corrupt("current_step")))
            .transpose()?,
        compensation_plan_version: row
            .try_get("compensation_plan_version")
            .map_err(map_database)?,
        version: row.try_get("version").map_err(map_database)?,
        deadline_at_ms: row.try_get("deadline_at").map_err(map_database)?,
        failure_code: row.try_get("failure_code").map_err(map_database)?,
        traceparent: row.try_get("traceparent").map_err(map_database)?,
    })
}

/// 业务作用：从检索查询结果解析实例摘要行，稳定文本列收敛回封闭枚举。
///
/// 参数说明：
/// - `row`: 检索 `SELECT` 出的摘要行。
///
/// 返回：解析成功返回摘要；任一列不在封闭词汇表内返回标记列名的数据损坏错误。
pub(crate) fn parse_instance_summary(
    row: &MySqlRow,
) -> Result<SagaInstanceSummary, SagaStoreError> {
    let saga_id: String = row.try_get("saga_id").map_err(map_database)?;
    let tenant: String = row.try_get("tenant_id").map_err(map_database)?;
    let workflow: String = row.try_get("workflow_name").map_err(map_database)?;
    let business_key: String = row.try_get("business_key").map_err(map_database)?;
    let definition_version: u32 = row.try_get("definition_version").map_err(map_database)?;
    let status: String = row.try_get("status").map_err(map_database)?;
    let control_state: String = row.try_get("control_state").map_err(map_database)?;
    let direction: String = row.try_get("direction").map_err(map_database)?;
    let current_step: Option<String> = row.try_get("current_step").map_err(map_database)?;
    Ok(SagaInstanceSummary {
        definition_digest: row.try_get("definition_digest").map_err(map_database)?,
        control_version: row.try_get("control_version").map_err(map_database)?,
        deadline_at_ms: row.try_get("deadline_at_ms").map_err(map_database)?,
        traceparent: row.try_get("traceparent").map_err(map_database)?,
        saga_id: SagaId::new(saga_id).map_err(|_| corrupt("saga_id"))?,
        tenant: TenantId::new(tenant).map_err(|_| corrupt("tenant_id"))?,
        workflow: WorkflowName::new(workflow).map_err(|_| corrupt("workflow_name"))?,
        business_key: BusinessKey::new(business_key).map_err(|_| corrupt("business_key"))?,
        definition_version: DefinitionVersion::new(definition_version)
            .map_err(|_| corrupt("definition_version"))?,
        status: SagaStatus::parse(&status).ok_or_else(|| corrupt("status"))?,
        control_state: ControlState::parse(&control_state)
            .ok_or_else(|| corrupt("control_state"))?,
        direction: Direction::parse(&direction).ok_or_else(|| corrupt("direction"))?,
        current_step: current_step
            .map(|step| StepName::new(step).map_err(|_| corrupt("current_step")))
            .transpose()?,
        version: row.try_get("version").map_err(map_database)?,
        failure_code: row.try_get("failure_code").map_err(map_database)?,
        created_at_ms: row.try_get("created_at_ms").map_err(map_database)?,
        created_at_cursor_us: row.try_get("created_at_cursor_us").map_err(map_database)?,
        updated_at_ms: row.try_get("updated_at_ms").map_err(map_database)?,
    })
}

/// 业务作用：从查询结果解析 step journal 投影行。
///
/// 参数说明：
/// - `row`: `SELECT` 出的 step 行。
///
/// 返回：解析成功返回投影快照；状态列不在封闭词汇表内时返回标记列名的数据损坏错误。
pub(crate) fn parse_step_row(row: &MySqlRow) -> Result<SagaStepRow, SagaStoreError> {
    let step: String = row.try_get("step_name").map_err(map_database)?;
    let forward_status: String = row.try_get("forward_status").map_err(map_database)?;
    let cancel_status: String = row.try_get("cancel_status").map_err(map_database)?;
    let compensation_status: String = row.try_get("compensation_status").map_err(map_database)?;
    let resolution_status: String = row.try_get("resolution_status").map_err(map_database)?;
    Ok(SagaStepRow {
        step: StepName::new(step).map_err(|_| corrupt("step_name"))?,
        ordinal: row.try_get("ordinal").map_err(map_database)?,
        forward_status: StepForwardStatus::parse(&forward_status)
            .ok_or_else(|| corrupt("forward_status"))?,
        cancel_status: StepCancelStatus::parse(&cancel_status)
            .ok_or_else(|| corrupt("cancel_status"))?,
        compensation_status: StepCompensationStatus::parse(&compensation_status)
            .ok_or_else(|| corrupt("compensation_status"))?,
        resolution_status: StepResolutionStatus::parse(&resolution_status)
            .ok_or_else(|| corrupt("resolution_status"))?,
        execute_effect_id: row.try_get("execute_effect_id").map_err(map_database)?,
        execute_command_id: row.try_get("execute_command_id").map_err(map_database)?,
        execute_attempt: row.try_get("execute_attempt").map_err(map_database)?,
        cancel_effect_id: row.try_get("cancel_effect_id").map_err(map_database)?,
        cancel_command_id: row.try_get("cancel_command_id").map_err(map_database)?,
        cancel_attempt: row.try_get("cancel_attempt").map_err(map_database)?,
        compensate_effect_id: row.try_get("compensate_effect_id").map_err(map_database)?,
        compensate_command_id: row.try_get("compensate_command_id").map_err(map_database)?,
        compensate_attempt: row.try_get("compensate_attempt").map_err(map_database)?,
        resolve_effect_id: row.try_get("resolve_effect_id").map_err(map_database)?,
        resolve_command_id: row.try_get("resolve_command_id").map_err(map_database)?,
        resolve_attempt: row.try_get("resolve_attempt").map_err(map_database)?,
        compensation_plan_version: row
            .try_get("compensation_plan_version")
            .map_err(map_database)?,
        compensation_order: row.try_get("compensation_order").map_err(map_database)?,
        last_error_code: row.try_get("last_error_code").map_err(map_database)?,
    })
}

/// 业务作用：从查询结果解析 attempt journal 行。
///
/// 参数说明：
/// - `row`: `SELECT` 出的 attempt 行。
///
/// 返回：解析成功返回事实行；phase/status 不在封闭词汇表内或 attempt 序号非法时
/// 返回标记列名的数据损坏错误。
pub(crate) fn parse_attempt_row(row: &MySqlRow) -> Result<SagaStepAttemptRow, SagaStoreError> {
    let step: String = row.try_get("step_name").map_err(map_database)?;
    let phase: String = row.try_get("phase").map_err(map_database)?;
    let attempt: u32 = row.try_get("attempt_no").map_err(map_database)?;
    let status: String = row.try_get("status").map_err(map_database)?;
    Ok(SagaStepAttemptRow {
        step: StepName::new(step).map_err(|_| corrupt("step_name"))?,
        phase: StepPhase::parse(&phase).ok_or_else(|| corrupt("phase"))?,
        attempt: AttemptNo::new(attempt).map_err(|_| corrupt("attempt_no"))?,
        effect_id: row.try_get("effect_id").map_err(map_database)?,
        command_id: row.try_get("command_id").map_err(map_database)?,
        status: StepAttemptStatus::parse(&status).ok_or_else(|| corrupt("status"))?,
        outcome_event_id: row.try_get("outcome_event_id").map_err(map_database)?,
    })
}
