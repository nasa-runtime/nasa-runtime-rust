//! Saga attempt、状态迁移、控制操作、人工恢复与冲突事实的只读审计查询。

use nasaga_core::{AttemptNo, SagaId, StepAttemptStatus, StepName, StepPhase};
use sqlx::Row as _;

use crate::error::{corrupt, map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::row::{parse_attempt_row, SagaStepAttemptRow};
use crate::MySqlSagaStore;

/// 业务作用：区分结果事实冲突的稳定类别，便于审计、告警与恢复工具按原因聚合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaConflictKind {
    /// 同一 attempt 收到两个互斥终态。
    AttemptTerminal,
    /// 迟到 phase 结果与其它 phase 已提交强事实互斥。
    CrossPhaseFact,
}

impl SagaConflictKind {
    /// 业务作用：返回冲突类别的稳定持久化名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可用于数据库、日志和告警标签的低基数名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AttemptTerminal => "attempt_terminal_conflict",
            Self::CrossPhaseFact => "cross_phase_fact_conflict",
        }
    }
}

/// 业务作用：表示一条业务状态迁移审计，序号与实例 version 同源且严格单调。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaTransitionAuditRow {
    /// 状态迁移序号。
    pub transition_seq: u64,
    /// 迁移前状态；初始创建为 `NONE`。
    pub from_state: String,
    /// 迁移后状态。
    pub to_state: String,
    /// event/timer/admin 等触发类别。
    pub trigger_kind: String,
    /// 稳定触发身份。
    pub trigger_id: String,
    /// 驱动迁移的 definition 版本。
    pub definition_version: u32,
    /// 数据库格式化的 UTC 时间文本，保留微秒精度。
    pub occurred_at: String,
}

/// 业务作用：表示一次 pause/resume 控制态 CAS 与不可抵赖主体审计。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaControlAuditRow {
    /// 独立 control generation。
    pub control_seq: u64,
    /// 切换前控制状态。
    pub from_state: String,
    /// 切换后控制状态。
    pub to_state: String,
    /// 管理请求幂等身份。
    pub operation_id: String,
    /// 认证主体稳定 id。
    pub actor: String,
    /// 工单或事故原因。
    pub reason: String,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
}

/// 业务作用：表示一次会改变业务状态或发布命令的人工恢复操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaManagementAuditRow {
    /// 管理请求幂等身份。
    pub operation_id: String,
    /// 稳定低基数动作名。
    pub action: String,
    /// 认证主体稳定 id。
    pub actor: String,
    /// 工单或事故原因。
    pub reason: String,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
}

/// 业务作用：表示同一 attempt 收到两种互斥终态的人工介入证据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaConflictFactRow {
    /// 后到矛盾结果的 event id，也是冲突记录幂等身份。
    pub incoming_event_id: String,
    /// 发生冲突的步骤。
    pub step: StepName,
    /// 发生冲突的阶段。
    pub phase: StepPhase,
    /// 发生冲突的 attempt。
    pub attempt: AttemptNo,
    /// journal 中先到、不可覆盖的终态。
    pub existing_status: StepAttemptStatus,
    /// incoming envelope 携带的互斥终态。
    pub incoming_status: StepAttemptStatus,
    /// 稳定冲突类别。
    pub conflict_kind: String,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
}

/// 业务作用：聚合一条互斥结果证据的稳定身份与双方裁决，避免调用方错位传参污染审计链。
#[derive(Debug, Clone, Copy)]
pub struct AttemptConflictFact<'a> {
    /// Saga 实例身份。
    pub saga_id: &'a SagaId,
    /// 冲突步骤。
    pub step: &'a StepName,
    /// 冲突阶段。
    pub phase: StepPhase,
    /// 冲突 attempt。
    pub attempt: AttemptNo,
    /// journal 已提交的先到终态。
    pub existing_status: StepAttemptStatus,
    /// 后到 envelope 携带的互斥终态。
    pub incoming_status: StepAttemptStatus,
    /// 后到结果事件身份，也是冲突写入幂等键。
    pub incoming_event_id: &'a str,
    /// 稳定冲突类别。
    pub conflict_kind: SagaConflictKind,
}

impl MySqlSagaStore {
    /// 业务作用：记录同一 attempt 或跨 phase 的互斥结果事实，作为升级人工介入的恢复依据。
    ///
    /// 参数说明：
    /// - `fact`: 冲突 attempt 身份、双方终态、incoming event 与稳定类别。
    ///
    /// 返回：写入成功返回 `Ok`；字段非法、事务缺失或数据库失败返回错误。
    pub async fn record_attempt_conflict(
        &self,
        fact: &AttemptConflictFact<'_>,
    ) -> Result<(), SagaStoreError> {
        if fact.existing_status.is_in_flight() || fact.incoming_status.is_in_flight() {
            return Err(SagaStoreError::new(
                "attempt conflict requires settled statuses",
            ));
        }
        validate_event_id(fact.incoming_event_id)?;
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        sqlx::query(
            "INSERT INTO saga_conflict_fact \
             (saga_id, incoming_event_id, step_name, phase, attempt_no, existing_status, \
              incoming_status, conflict_kind) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(fact.saga_id.as_str())
        .bind(fact.incoming_event_id)
        .bind(fact.step.as_str())
        .bind(fact.phase.as_str())
        .bind(fact.attempt.get())
        .bind(fact.existing_status.as_str())
        .bind(fact.incoming_status.as_str())
        .bind(fact.conflict_kind.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(())
    }

    /// 业务作用：按步骤、阶段、attempt 顺序读取实例的 attempt 事实。
    pub async fn load_attempt_audit(
        &self,
        saga_id: &SagaId,
        limit: u32,
    ) -> Result<Vec<SagaStepAttemptRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT step_name, phase, attempt_no, effect_id, command_id, status, outcome_event_id \
             FROM saga_step_attempt WHERE saga_id = ? \
             ORDER BY step_name, phase, attempt_no LIMIT ?",
        )
        .bind(saga_id.as_str())
        .bind(limit)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_attempt_row).collect()
    }

    /// 业务作用：从指定序号后分页读取业务状态迁移审计链。
    pub async fn load_transition_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaTransitionAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT transition_seq, from_state, to_state, trigger_kind, trigger_id, \
             definition_version, DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at \
             FROM saga_transition WHERE saga_id = ? AND transition_seq > ? \
             ORDER BY transition_seq LIMIT ?",
        )
        .bind(saga_id.as_str())
        .bind(after_seq)
        .bind(limit)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_transition).collect()
    }

    /// 业务作用：从指定 control generation 后分页读取 pause/resume 主体审计。
    pub async fn load_control_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaControlAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT control_seq, from_state, to_state, operation_id, actor, reason, \
             DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at \
             FROM saga_control_transition WHERE saga_id = ? AND control_seq > ? \
             ORDER BY control_seq LIMIT ?",
        )
        .bind(saga_id.as_str())
        .bind(after_seq)
        .bind(limit)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_control).collect()
    }

    /// 业务作用：读取人工恢复业务动作的 actor、reason 与 operation 幂等证据。
    pub async fn load_management_audit(
        &self,
        saga_id: &SagaId,
        limit: u32,
    ) -> Result<Vec<SagaManagementAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT operation_id, action, actor, reason, \
             DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at \
             FROM saga_management_audit WHERE saga_id = ? ORDER BY occurred_at, operation_id LIMIT ?",
        )
        .bind(saga_id.as_str())
        .bind(limit)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_management).collect()
    }

    /// 业务作用：读取不可覆盖的互斥结果事实，供人工恢复前比对双方证据。
    pub async fn load_conflict_audit(
        &self,
        saga_id: &SagaId,
        limit: u32,
    ) -> Result<Vec<SagaConflictFactRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT incoming_event_id, step_name, phase, attempt_no, existing_status, \
             incoming_status, conflict_kind, \
             DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at \
             FROM saga_conflict_fact WHERE saga_id = ? ORDER BY occurred_at, incoming_event_id LIMIT ?",
        )
        .bind(saga_id.as_str())
        .bind(limit)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_conflict).collect()
    }
}

/// 业务作用：校验审计查询上限，防止管理请求把全历史一次加载进内存。
fn validate_limit(limit: u32) -> Result<(), SagaStoreError> {
    if limit == 0 || limit > 1_000 {
        return Err(SagaStoreError::new("audit limit must be in 1..=1000"));
    }
    Ok(())
}

/// 业务作用：校验冲突 event id 适配持久化主键且不含控制字符。
fn validate_event_id(event_id: &str) -> Result<(), SagaStoreError> {
    if event_id.is_empty()
        || event_id.len() > 190
        || event_id.trim() != event_id
        || event_id.chars().any(char::is_control)
    {
        return Err(SagaStoreError::new("invalid conflict event id"));
    }
    Ok(())
}

/// 业务作用：解析业务状态迁移审计行。
fn parse_transition(row: &sqlx::mysql::MySqlRow) -> Result<SagaTransitionAuditRow, SagaStoreError> {
    Ok(SagaTransitionAuditRow {
        transition_seq: row.try_get("transition_seq").map_err(map_database)?,
        from_state: row.try_get("from_state").map_err(map_database)?,
        to_state: row.try_get("to_state").map_err(map_database)?,
        trigger_kind: row.try_get("trigger_kind").map_err(map_database)?,
        trigger_id: row.try_get("trigger_id").map_err(map_database)?,
        definition_version: row.try_get("definition_version").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
    })
}

/// 业务作用：解析控制态主体审计行。
fn parse_control(row: &sqlx::mysql::MySqlRow) -> Result<SagaControlAuditRow, SagaStoreError> {
    Ok(SagaControlAuditRow {
        control_seq: row.try_get("control_seq").map_err(map_database)?,
        from_state: row.try_get("from_state").map_err(map_database)?,
        to_state: row.try_get("to_state").map_err(map_database)?,
        operation_id: row.try_get("operation_id").map_err(map_database)?,
        actor: row.try_get("actor").map_err(map_database)?,
        reason: row.try_get("reason").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
    })
}

/// 业务作用：解析人工恢复动作审计行。
fn parse_management(row: &sqlx::mysql::MySqlRow) -> Result<SagaManagementAuditRow, SagaStoreError> {
    Ok(SagaManagementAuditRow {
        operation_id: row.try_get("operation_id").map_err(map_database)?,
        action: row.try_get("action").map_err(map_database)?,
        actor: row.try_get("actor").map_err(map_database)?,
        reason: row.try_get("reason").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
    })
}

/// 业务作用：解析互斥 attempt 事实并收敛回封闭身份/状态类型。
fn parse_conflict(row: &sqlx::mysql::MySqlRow) -> Result<SagaConflictFactRow, SagaStoreError> {
    let step: String = row.try_get("step_name").map_err(map_database)?;
    let phase: String = row.try_get("phase").map_err(map_database)?;
    let attempt: u32 = row.try_get("attempt_no").map_err(map_database)?;
    let existing: String = row.try_get("existing_status").map_err(map_database)?;
    let incoming: String = row.try_get("incoming_status").map_err(map_database)?;
    Ok(SagaConflictFactRow {
        incoming_event_id: row.try_get("incoming_event_id").map_err(map_database)?,
        step: StepName::new(step).map_err(|_| corrupt("step_name"))?,
        phase: StepPhase::parse(&phase).ok_or_else(|| corrupt("phase"))?,
        attempt: AttemptNo::new(attempt).map_err(|_| corrupt("attempt_no"))?,
        existing_status: StepAttemptStatus::parse(&existing)
            .ok_or_else(|| corrupt("existing_status"))?,
        incoming_status: StepAttemptStatus::parse(&incoming)
            .ok_or_else(|| corrupt("incoming_status"))?,
        conflict_kind: row.try_get("conflict_kind").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
    })
}
