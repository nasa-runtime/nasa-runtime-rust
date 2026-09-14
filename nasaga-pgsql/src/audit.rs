//! Saga attempt、状态迁移、控制操作、人工恢复与冲突事实的只读审计查询。

use nasaga_backend::{
    AttemptConflictFact, SagaAttemptAuditCursor, SagaAttemptAuditRow, SagaAuditEventCursor,
    SagaAuditEventRecord, SagaAuditEventRow, SagaConflictFactRow, SagaControlAuditRow,
    SagaManagementAuditRow, SagaTimedAuditCursor, SagaTransitionAuditRow,
};
use nasaga_core::{AttemptNo, SagaId, StepAttemptStatus, StepName, StepPhase};
use sqlx::Row as _;

use crate::error::{
    corrupt, map_connection, map_database, pg_u64, row_u32, row_u64, SagaStoreError,
};
use crate::instance::require_ambient_transaction;
use crate::row::parse_attempt_row;
use crate::PgSagaStore;

impl PgSagaStore {
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
        let mut connection = natx_pgsql::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        sqlx::query(
            "INSERT INTO saga_conflict_fact \
             (saga_id, incoming_event_id, step_name, phase, attempt_no, existing_status, \
              incoming_status, conflict_kind) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(fact.saga_id.as_str())
        .bind(fact.incoming_event_id)
        .bind(fact.step.as_str())
        .bind(fact.phase.as_str())
        .bind(i64::from(fact.attempt.get()))
        .bind(fact.existing_status.as_str())
        .bind(fact.incoming_status.as_str())
        .bind(fact.conflict_kind.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(())
    }

    /// 业务作用：按数据库全局序号读取跨类别不可变审计事件，支持运行中持续追加。
    ///
    /// 参数说明：实例、最后已交付序号与上限共同限定下一页。
    ///
    /// 返回：严格按 `audit_seq` 递增的事实快照；参数、数据损坏或读取失败返回错误。
    pub async fn load_audit_events(
        &self,
        saga_id: &SagaId,
        after: SagaAuditEventCursor,
        limit: u32,
    ) -> Result<Vec<SagaAuditEventRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT audit_seq, record_kind, step_name, phase, attempt_no, effect_id, command_id, \
             attempt_status AS status, outcome_event_id, transition_seq, from_state, to_state, \
             trigger_kind, trigger_id, definition_version, control_seq, operation_id, action, \
             actor, reason, incoming_event_id, existing_status, incoming_status, conflict_kind, \
             to_char(occurred_at AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
             FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
             FROM saga_audit_event WHERE saga_id = $1 AND audit_seq > $2 \
             ORDER BY audit_seq LIMIT $3",
        )
        .bind(saga_id.as_str())
        .bind(pg_u64(after.audit_seq)?)
        .bind(i64::from(limit))
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_audit_event).collect()
    }

    /// 业务作用：按步骤、阶段、attempt 顺序读取实例的 attempt 事实。
    pub async fn load_attempt_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaAttemptAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaAttemptAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT step_name, phase, attempt_no, effect_id, command_id, status, outcome_event_id, \
                 to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
                 FLOOR(EXTRACT(EPOCH FROM started_at) * 1000)::BIGINT AS occurred_at_ms \
                 FROM saga_step_attempt WHERE saga_id = $1 AND \
                 (step_name, phase, attempt_no) > ($2, $3, $4) \
                 ORDER BY step_name, phase, attempt_no LIMIT $5",
            )
            .bind(saga_id.as_str())
            .bind(after.step.as_str())
            .bind(after.phase.as_str())
            .bind(i64::from(after.attempt.get()))
            .bind(i64::from(limit))
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query(
                "SELECT step_name, phase, attempt_no, effect_id, command_id, status, outcome_event_id, \
                 to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
                 FLOOR(EXTRACT(EPOCH FROM started_at) * 1000)::BIGINT AS occurred_at_ms \
                 FROM saga_step_attempt WHERE saga_id = $1 \
                 ORDER BY step_name, phase, attempt_no LIMIT $2",
            )
            .bind(saga_id.as_str())
            .bind(i64::from(limit))
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        };
        rows.iter()
            .map(|row| {
                Ok(SagaAttemptAuditRow {
                    attempt: parse_attempt_row(row)?,
                    occurred_at: row.try_get("occurred_at").map_err(map_database)?,
                    occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
                })
            })
            .collect()
    }

    /// 业务作用：从指定序号后分页读取业务状态迁移审计链。
    pub async fn load_transition_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaTransitionAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT transition_seq, from_state, to_state, trigger_kind, trigger_id, \
             definition_version, to_char(occurred_at AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
             FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
             FROM saga_transition WHERE saga_id = $1 AND transition_seq > $2 \
             ORDER BY transition_seq LIMIT $3",
        )
        .bind(saga_id.as_str())
        .bind(pg_u64(after_seq)?)
        .bind(i64::from(limit))
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
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT control_seq, from_state, to_state, operation_id, actor, reason, \
             to_char(occurred_at AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
             FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
             FROM saga_control_transition WHERE saga_id = $1 AND control_seq > $2 \
             ORDER BY control_seq LIMIT $3",
        )
        .bind(saga_id.as_str())
        .bind(pg_u64(after_seq)?)
        .bind(i64::from(limit))
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_control).collect()
    }

    /// 业务作用：读取人工恢复业务动作的 actor、reason 与 operation 幂等证据。
    pub async fn load_management_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaManagementAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT operation_id, action, actor, reason, \
                 to_char(occurred_at AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
                 FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
                 FROM saga_management_audit WHERE saga_id = $1 AND \
                 (to_char(occurred_at AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), operation_id) > ($2, $3) \
                 ORDER BY occurred_at, operation_id LIMIT $4",
            )
            .bind(saga_id.as_str())
            .bind(&after.occurred_at)
            .bind(&after.identity)
            .bind(i64::from(limit))
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query(
                "SELECT operation_id, action, actor, reason, \
                 to_char(occurred_at AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
                 FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
                 FROM saga_management_audit WHERE saga_id = $1 ORDER BY occurred_at, operation_id LIMIT $2",
            )
            .bind(saga_id.as_str())
            .bind(i64::from(limit))
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        };
        rows.iter().map(parse_management).collect()
    }

    /// 业务作用：读取不可覆盖的互斥结果事实，供人工恢复前比对双方证据。
    pub async fn load_conflict_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaConflictFactRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx_pgsql::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT incoming_event_id, step_name, phase, attempt_no, existing_status, \
                 incoming_status, conflict_kind, \
                 to_char(occurred_at AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
                 FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
                 FROM saga_conflict_fact WHERE saga_id = $1 AND \
                 (to_char(occurred_at AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), incoming_event_id) > ($2, $3) \
                 ORDER BY occurred_at, incoming_event_id LIMIT $4",
            )
            .bind(saga_id.as_str())
            .bind(&after.occurred_at)
            .bind(&after.identity)
            .bind(i64::from(limit))
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query(
                "SELECT incoming_event_id, step_name, phase, attempt_no, existing_status, \
                 incoming_status, conflict_kind, \
                 to_char(occurred_at AT TIME ZONE 'UTC', \
                 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS occurred_at, \
                 FLOOR(EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT AS occurred_at_ms \
                 FROM saga_conflict_fact WHERE saga_id = $1 ORDER BY occurred_at, incoming_event_id LIMIT $2",
            )
            .bind(saga_id.as_str())
            .bind(i64::from(limit))
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        };
        rows.iter().map(parse_conflict).collect()
    }
}

/// 业务作用：把统一事件表的一行收敛为封闭审计类别，并拒绝列组合损坏。
///
/// 参数说明：`row` 是按 `audit_seq` 读取的事件快照。
///
/// 返回：类别与必填字段一致时返回类型化事件；未知类别或坏值返回错误。
fn parse_audit_event(row: &sqlx::postgres::PgRow) -> Result<SagaAuditEventRow, SagaStoreError> {
    let record_kind: String = row.try_get("record_kind").map_err(map_database)?;
    let occurred_at = row.try_get("occurred_at").map_err(map_database)?;
    let occurred_at_ms = row.try_get("occurred_at_ms").map_err(map_database)?;
    let record = match record_kind.as_str() {
        "attempt" => SagaAuditEventRecord::Attempt(SagaAttemptAuditRow {
            attempt: parse_attempt_row(row)?,
            occurred_at,
            occurred_at_ms,
        }),
        "transition" => SagaAuditEventRecord::Transition(parse_transition(row)?),
        "control" => SagaAuditEventRecord::Control(parse_control(row)?),
        "management" => SagaAuditEventRecord::Management(parse_management(row)?),
        "conflict" => SagaAuditEventRecord::Conflict(parse_conflict(row)?),
        _ => return Err(corrupt("record_kind")),
    };
    Ok(SagaAuditEventRow {
        audit_seq: row_u64(row, "audit_seq")?,
        record,
    })
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
fn parse_transition(row: &sqlx::postgres::PgRow) -> Result<SagaTransitionAuditRow, SagaStoreError> {
    Ok(SagaTransitionAuditRow {
        transition_seq: row_u64(row, "transition_seq")?,
        from_state: row.try_get("from_state").map_err(map_database)?,
        to_state: row.try_get("to_state").map_err(map_database)?,
        trigger_kind: row.try_get("trigger_kind").map_err(map_database)?,
        trigger_id: row.try_get("trigger_id").map_err(map_database)?,
        definition_version: row_u32(row, "definition_version")?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
        occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
    })
}

/// 业务作用：解析控制态主体审计行。
fn parse_control(row: &sqlx::postgres::PgRow) -> Result<SagaControlAuditRow, SagaStoreError> {
    Ok(SagaControlAuditRow {
        control_seq: row_u64(row, "control_seq")?,
        from_state: row.try_get("from_state").map_err(map_database)?,
        to_state: row.try_get("to_state").map_err(map_database)?,
        operation_id: row.try_get("operation_id").map_err(map_database)?,
        actor: row.try_get("actor").map_err(map_database)?,
        reason: row.try_get("reason").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
        occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
    })
}

/// 业务作用：解析人工恢复动作审计行。
fn parse_management(row: &sqlx::postgres::PgRow) -> Result<SagaManagementAuditRow, SagaStoreError> {
    Ok(SagaManagementAuditRow {
        operation_id: row.try_get("operation_id").map_err(map_database)?,
        action: row.try_get("action").map_err(map_database)?,
        actor: row.try_get("actor").map_err(map_database)?,
        reason: row.try_get("reason").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
        occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
    })
}

/// 业务作用：解析互斥 attempt 事实并收敛回封闭身份/状态类型。
fn parse_conflict(row: &sqlx::postgres::PgRow) -> Result<SagaConflictFactRow, SagaStoreError> {
    let step: String = row.try_get("step_name").map_err(map_database)?;
    let phase: String = row.try_get("phase").map_err(map_database)?;
    let attempt = row_u32(row, "attempt_no")?;
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
        occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
    })
}
