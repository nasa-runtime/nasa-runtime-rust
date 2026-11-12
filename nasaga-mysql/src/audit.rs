//! Saga attempt、状态迁移、控制操作、人工恢复与冲突事实查询，以及事务内冲突事实记录。

use nasaga_backend::{
    AttemptConflictFact, SagaAttemptAuditCursor, SagaAttemptAuditRow, SagaAuditEventCursor,
    SagaAuditEventRecord, SagaAuditEventRow, SagaConflictFactRow, SagaControlAuditRow,
    SagaManagementAuditRow, SagaTimedAuditCursor, SagaTransitionAuditRow,
};
use nasaga_core::{AttemptNo, SagaId, StepAttemptStatus, StepName, StepPhase};
use sqlx::Row as _;

use crate::error::{corrupt, map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::row::parse_attempt_row;
use crate::MySqlSagaStore;

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
        // 只有已确定的双方结果才能作为冲突证据；在途状态不能据此升级人工介入。
        if fact.existing_status.is_in_flight() || fact.incoming_status.is_in_flight() {
            return Err(SagaStoreError::new(
                "attempt conflict requires settled statuses",
            ));
        }
        validate_event_id(fact.incoming_event_id)?;
        // 冲突事实必须随调用方的实例决策一同提交，禁止在缺少事务时独立落库。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
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

    /// 业务作用：按数据库全局序号读取跨类别不可变审计事件，支持运行中持续追加。
    ///
    /// 参数说明：
    /// - `saga_id`：查询所属的精确 Saga 实例身份。
    /// - `after`：最后已交付的全局审计序号，下一页不再包含该序号。
    /// - `limit`：单页条数，必须位于 1..=1000。
    ///
    /// 返回：返回 audit_seq 严格递增且大于游标的本页事实，可能为空；上限、连接、数据库或行解析失败返回错误。分页不建立跨调用的一致快照。
    pub async fn load_audit_events(
        &self,
        saga_id: &SagaId,
        after: SagaAuditEventCursor,
        limit: u32,
    ) -> Result<Vec<SagaAuditEventRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT audit_seq, record_kind, step_name, phase, attempt_no, effect_id, command_id, \
             attempt_status AS status, outcome_event_id, transition_seq, from_state, to_state, \
             trigger_kind, trigger_id, definition_version, control_seq, operation_id, action, \
             actor, reason, incoming_event_id, existing_status, incoming_status, conflict_kind, \
             DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
             CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
             FROM saga_audit_event WHERE saga_id = ? AND audit_seq > ? \
             ORDER BY audit_seq LIMIT ?",
        )
        .bind(saga_id.as_str())
        .bind(after.audit_seq)
        .bind(limit)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_audit_event).collect()
    }

    /// 业务作用：按步骤、阶段、attempt 顺序读取实例的 attempt 事实。
    ///
    /// 参数说明：
    /// - `saga_id`：查询所属的精确 Saga 实例身份。
    /// - `after`：最后已交付的步骤、阶段和 attempt 组合；None 从头读取。
    /// - `limit`：单页条数，必须位于 1..=1000。
    ///
    /// 返回：返回按 step_name、phase、attempt_no 升序且严格位于游标后的本页 attempt，可能为空；上限、连接、查询或行身份解析失败返回错误。该表包含可变 attempt 当前状态，不等同于不可变事件流。
    pub async fn load_attempt_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaAttemptAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaAttemptAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT step_name, phase, attempt_no, effect_id, command_id, status, outcome_event_id, \
                 DATE_FORMAT(started_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
                 CAST(UNIX_TIMESTAMP(started_at) * 1000 AS SIGNED) AS occurred_at_ms \
                 FROM saga_step_attempt WHERE saga_id = ? AND \
                 (step_name, phase, attempt_no) > (?, ?, ?) \
                 ORDER BY step_name, phase, attempt_no LIMIT ?",
            )
            .bind(saga_id.as_str())
            .bind(after.step.as_str())
            .bind(after.phase.as_str())
            .bind(after.attempt.get())
            .bind(limit)
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query(
                "SELECT step_name, phase, attempt_no, effect_id, command_id, status, outcome_event_id, \
                 DATE_FORMAT(started_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
                 CAST(UNIX_TIMESTAMP(started_at) * 1000 AS SIGNED) AS occurred_at_ms \
                 FROM saga_step_attempt WHERE saga_id = ? \
                 ORDER BY step_name, phase, attempt_no LIMIT ?",
            )
            .bind(saga_id.as_str())
            .bind(limit)
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
    ///
    /// 参数说明：
    /// - `saga_id`：查询所属的精确 Saga 实例身份。
    /// - `after_seq`：最后已交付的业务迁移序号，不包含该序号。
    /// - `limit`：单页条数，必须位于 1..=1000。
    ///
    /// 返回：返回 transition_seq 严格递增且大于游标的本页迁移记录，可能为空；参数、连接、查询或行转换失败返回错误。
    pub async fn load_transition_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaTransitionAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT transition_seq, from_state, to_state, trigger_kind, trigger_id, \
             definition_version, DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
             CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
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
    ///
    /// 参数说明：
    /// - `saga_id`：查询所属的精确 Saga 实例身份。
    /// - `after_seq`：最后已交付的控制序号，不包含该序号。
    /// - `limit`：单页条数，必须位于 1..=1000。
    ///
    /// 返回：返回 control_seq 严格递增且大于游标的本页控制记录，可能为空；参数、连接、查询或行转换失败返回错误。
    pub async fn load_control_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaControlAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT control_seq, from_state, to_state, operation_id, actor, reason, \
             DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
             CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
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
    ///
    /// 参数说明：
    /// - `saga_id`：查询所属的精确 Saga 实例身份。
    /// - `after`：最后已交付的 occurred_at 文本与 operation_id；None 从头读取。
    /// - `limit`：单页条数，必须位于 1..=1000。
    ///
    /// 返回：按 occurred_at、operation_id 升序返回本页记录；有游标时按格式化时间文本和 operation_id 严格续读。上限、连接、查询或行转换失败时报错，不在此处校验游标时间格式或身份内容。
    pub async fn load_management_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaManagementAuditRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT operation_id, action, actor, reason, \
                 DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
                 CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
                 FROM saga_management_audit WHERE saga_id = ? AND \
                 (DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ'), operation_id) > (?, ?) \
                 ORDER BY occurred_at, operation_id LIMIT ?",
            )
            .bind(saga_id.as_str())
            .bind(&after.occurred_at)
            .bind(&after.identity)
            .bind(limit)
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query(
                "SELECT operation_id, action, actor, reason, \
                 DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
                 CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
                 FROM saga_management_audit WHERE saga_id = ? ORDER BY occurred_at, operation_id LIMIT ?",
            )
            .bind(saga_id.as_str())
            .bind(limit)
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        };
        rows.iter().map(parse_management).collect()
    }

    /// 业务作用：读取不可覆盖的互斥结果事实，供人工恢复前比对双方证据。
    ///
    /// 参数说明：
    /// - `saga_id`：查询所属的精确 Saga 实例身份。
    /// - `after`：最后已交付的 occurred_at 文本与 incoming_event_id；None 从头读取。
    /// - `limit`：单页条数，必须位于 1..=1000。
    ///
    /// 返回：按 occurred_at、incoming_event_id 升序返回本页事实；有游标时按格式化时间文本和事件 ID 严格续读。上限、连接、查询或身份、阶段和状态转换失败时报错，不验证游标时间格式。
    pub async fn load_conflict_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaConflictFactRow>, SagaStoreError> {
        validate_limit(limit)?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT incoming_event_id, step_name, phase, attempt_no, existing_status, \
                 incoming_status, conflict_kind, \
                 DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
                 CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
                 FROM saga_conflict_fact WHERE saga_id = ? AND \
                 (DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ'), incoming_event_id) > (?, ?) \
                 ORDER BY occurred_at, incoming_event_id LIMIT ?",
            )
            .bind(saga_id.as_str())
            .bind(&after.occurred_at)
            .bind(&after.identity)
            .bind(limit)
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query(
                "SELECT incoming_event_id, step_name, phase, attempt_no, existing_status, \
                 incoming_status, conflict_kind, \
                 DATE_FORMAT(occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at, \
                 CAST(UNIX_TIMESTAMP(occurred_at) * 1000 AS SIGNED) AS occurred_at_ms \
                 FROM saga_conflict_fact WHERE saga_id = ? ORDER BY occurred_at, incoming_event_id LIMIT ?",
            )
            .bind(saga_id.as_str())
            .bind(limit)
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?
        };
        rows.iter().map(parse_conflict).collect()
    }
}

/// 业务作用：把统一事件表的一行收敛为封闭审计类别，并拒绝列组合损坏。
///
/// 参数说明：
/// - `row`：是按 `audit_seq` 读取的事件快照。
///
/// 返回：类别与必填字段一致时返回类型化事件；未知类别或坏值返回错误。
fn parse_audit_event(row: &sqlx::mysql::MySqlRow) -> Result<SagaAuditEventRow, SagaStoreError> {
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
        audit_seq: row.try_get("audit_seq").map_err(map_database)?,
        record,
    })
}

/// 业务作用：校验审计查询上限，防止管理请求把全历史一次加载进内存。
///
/// 参数说明：
/// - `limit`：单页条数，必须位于 1..=1000。
///
/// 返回：1..=1000 时成功，其它值返回固定错误，不改写上限。
fn validate_limit(limit: u32) -> Result<(), SagaStoreError> {
    if limit == 0 || limit > 1_000 {
        return Err(SagaStoreError::new("audit limit must be in 1..=1000"));
    }
    Ok(())
}

/// 业务作用：校验冲突 event id 适配持久化主键且不含控制字符。
///
/// 参数说明：
/// - `event_id`：待持久化的冲突输入事件身份原文。
///
/// 返回：非空、不超 190 UTF-8 字节、与 trim 后相同且无控制字符时成功，否则报错；不改写 ID，也不证明该 ID 已存在。
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
///
/// 参数说明：
/// - `row`：包含迁移事实列与标准化发生时间的查询行。
///
/// 返回：所需列读取与类型转换成功时返回迁移事实；缺列、类型损坏时返回错误，不校验业务状态名或迁移是否合法。
fn parse_transition(row: &sqlx::mysql::MySqlRow) -> Result<SagaTransitionAuditRow, SagaStoreError> {
    Ok(SagaTransitionAuditRow {
        transition_seq: row.try_get("transition_seq").map_err(map_database)?,
        from_state: row.try_get("from_state").map_err(map_database)?,
        to_state: row.try_get("to_state").map_err(map_database)?,
        trigger_kind: row.try_get("trigger_kind").map_err(map_database)?,
        trigger_id: row.try_get("trigger_id").map_err(map_database)?,
        definition_version: row.try_get("definition_version").map_err(map_database)?,
        occurred_at: row.try_get("occurred_at").map_err(map_database)?,
        occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
    })
}

/// 业务作用：解析控制态主体审计行。
///
/// 参数说明：
/// - `row`：包含控制序号、operation、actor、reason 与时间的查询行。
///
/// 返回：所需列读取与类型转换成功时返回控制事实，失败返回错误；不校验 action 语义或重新取得控制权。
fn parse_control(row: &sqlx::mysql::MySqlRow) -> Result<SagaControlAuditRow, SagaStoreError> {
    Ok(SagaControlAuditRow {
        control_seq: row.try_get("control_seq").map_err(map_database)?,
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
///
/// 参数说明：
/// - `row`：包含人工动作身份、类型、操作者、理由与时间的查询行。
///
/// 返回：所需列读取与类型转换成功时返回人工动作事实，失败返回错误；不执行动作、验证 actor 权限或清洗字符串。
fn parse_management(row: &sqlx::mysql::MySqlRow) -> Result<SagaManagementAuditRow, SagaStoreError> {
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
///
/// 参数说明：
/// - `row`：包含冲突事件身份、attempt、双方状态、类别与时间的查询行。
///
/// 返回：列读取成功且 step、phase、attempt、双方状态能转换为封闭类型时返回事实，否则报错；不在此处再次证明双方为 settled 或真正互斥。
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
        occurred_at_ms: row.try_get("occurred_at_ms").map_err(map_database)?,
    })
}
