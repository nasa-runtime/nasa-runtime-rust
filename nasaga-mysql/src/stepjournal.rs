//! Orchestrator step journal 与统一 attempt journal 的持久化。
//!
//! attempt journal 是"每条投递命令全局去重"与真实 outcome 留证的最终事实源
//! （`UNIQUE(command_id)` 建在这里）；`saga_step` 的 phase 列只是当前 attempt 的投影。
//! 冻结补偿计划、恢复裁决与人工介入都必须从这两张表的已提交事实出发。

use nasaga_core::{
    AttemptNo, CommandId, CompensationPlan, EffectId, SagaId, StepAttemptStatus, StepCancelStatus,
    StepCompensationStatus, StepForwardStatus, StepName, StepPhase, StepResolutionStatus,
    WorkflowDefinition,
};
use sqlx::Row as _;

use crate::error::{is_unique_violation, map_connection, map_database, SagaStoreError};
use crate::instance::{require_ambient_transaction, validate_reason_code};
use crate::row::{parse_attempt_row, parse_step_row, SagaStepAttemptRow, SagaStepRow};
use crate::MySqlSagaStore;

/// 业务作用：区分 attempt 登记的两种合法结果，使创建/推进事务的崩溃重试可被幂等吸收。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStart {
    /// 本事务真实登记了该 attempt。
    Recorded,
    /// 同一 attempt 已以相同身份登记过（崩溃重试）；无新副作用。
    AlreadyRecorded,
}

/// 业务作用：区分 outcome 记账、幂等重放与互斥事实冲突，使矛盾结果能同事务转人工留证。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcomeRecord {
    /// 本事务真实写入了该终态。
    Recorded,
    /// 同一 attempt 已记录完全相同的终态（重复结果事件）；无新副作用。
    AlreadyRecorded,
    /// 同一 attempt 已有不同终态；调用方必须记录 incoming 冲突事实并升级人工介入。
    Conflicting {
        /// journal 中已经提交的真实终态。
        existing: StepAttemptStatus,
    },
}

/// 业务作用：描述一次 step journal 投影更新；为空的字段保留列上既有值。
///
/// 字段说明：全部字段都是"设置为该值"而非"清空"——journal 状态只允许被显式改写，
/// 不存在把已裁决状态抹回未知的合法业务路径。
#[derive(Debug, Clone, Copy, Default)]
pub struct StepJournalPatch<'a> {
    /// 正向阶段状态；为空保留既有值。
    pub forward_status: Option<StepForwardStatus>,
    /// 取消屏障裁决状态；为空保留既有值。
    pub cancel_status: Option<StepCancelStatus>,
    /// 补偿阶段状态；为空保留既有值。
    pub compensation_status: Option<StepCompensationStatus>,
    /// 解决阶段状态；为空保留既有值。
    pub resolution_status: Option<StepResolutionStatus>,
    /// 稳定失败原因码；为空保留既有值。
    pub last_error_code: Option<&'a str>,
    /// 为真时把 finished_at 定格为当前时刻（已有值则保留首次定格）。
    pub mark_finished: bool,
}

/// execute 阶段的投影更新语句；`COALESCE(execute_attempt, 0) <= ?` 保证投影单调，
/// 旧 attempt 的迟到写入不能覆盖新 attempt 的投影。
const PROJECT_EXECUTE_SQL: &str = "UPDATE saga_step SET execute_effect_id = ?, \
     execute_command_id = ?, execute_attempt = ?, \
     started_at = COALESCE(started_at, CURRENT_TIMESTAMP(6)) \
     WHERE saga_id = ? AND step_name = ? AND COALESCE(execute_attempt, 0) <= ?";

/// cancel 阶段的投影更新语句，单调性约束同 execute。
const PROJECT_CANCEL_SQL: &str = "UPDATE saga_step SET cancel_effect_id = ?, \
     cancel_command_id = ?, cancel_attempt = ?, \
     started_at = COALESCE(started_at, CURRENT_TIMESTAMP(6)) \
     WHERE saga_id = ? AND step_name = ? AND COALESCE(cancel_attempt, 0) <= ?";

/// compensate 阶段的投影更新语句，单调性约束同 execute。
const PROJECT_COMPENSATE_SQL: &str = "UPDATE saga_step SET compensate_effect_id = ?, \
     compensate_command_id = ?, compensate_attempt = ?, \
     started_at = COALESCE(started_at, CURRENT_TIMESTAMP(6)) \
     WHERE saga_id = ? AND step_name = ? AND COALESCE(compensate_attempt, 0) <= ?";

/// resolve 阶段的投影更新语句，单调性约束同 execute。
const PROJECT_RESOLVE_SQL: &str = "UPDATE saga_step SET resolve_effect_id = ?, \
     resolve_command_id = ?, resolve_attempt = ?, \
     started_at = COALESCE(started_at, CURRENT_TIMESTAMP(6)) \
     WHERE saga_id = ? AND step_name = ? AND COALESCE(resolve_attempt, 0) <= ?";

impl MySqlSagaStore {
    /// 业务作用：在创建事务内按 definition 注册全部步骤行，建立 journal 的行骨架。
    ///
    /// 使用 `INSERT IGNORE` 幂等：创建事务的崩溃重试不产生重复行，也不会把已推进的
    /// 步骤状态重置回初始值。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `definition`: 实例绑定版本的流程定义，提供步骤集合与顺序。
    ///
    /// 返回：全部步骤行就位返回 `Ok`；事务缺失或底层失败返回错误。
    pub async fn register_steps(
        &self,
        saga_id: &SagaId,
        definition: &WorkflowDefinition,
    ) -> Result<(), SagaStoreError> {
        // 步骤骨架必须与实例创建同事务:实例可见但步骤缺行会让恢复扫描误判"无步骤可补偿"。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        for (index, step) in definition.steps().iter().enumerate() {
            sqlx::query(
                "INSERT IGNORE INTO saga_step (saga_id, step_name, ordinal, forward_status, \
                 cancel_status, compensation_status, resolution_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(saga_id.as_str())
            .bind(step.name().as_str())
            .bind(index as u32 + 1)
            .bind(StepForwardStatus::Pending.as_str())
            .bind(StepCancelStatus::None.as_str())
            .bind(StepCompensationStatus::None.as_str())
            .bind(StepResolutionStatus::None.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        Ok(())
    }

    /// 业务作用：按 ordinal 顺序加载实例的全部 step journal 投影，供冻结补偿计划与恢复裁决。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    ///
    /// 返回：按定义顺序排列的投影行；读回列损坏或底层失败返回错误。
    pub async fn load_steps(&self, saga_id: &SagaId) -> Result<Vec<SagaStepRow>, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT step_name, ordinal, forward_status, cancel_status, compensation_status, \
             resolution_status, execute_effect_id, execute_command_id, execute_attempt, \
             cancel_effect_id, cancel_command_id, cancel_attempt, compensate_effect_id, \
             compensate_command_id, compensate_attempt, resolve_effect_id, resolve_command_id, \
             resolve_attempt, compensation_plan_version, compensation_order, last_error_code \
             FROM saga_step WHERE saga_id = ? ORDER BY ordinal ASC",
        )
        .bind(saga_id.as_str())
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_step_row).collect()
    }

    /// 业务作用：判断实例是否已有任一步骤补偿成功，为"人工介入后禁止恢复正向"的
    /// 业务门禁提供已提交事实。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    ///
    /// 返回：存在补偿成功步骤返回真；底层失败返回错误。
    pub async fn any_compensation_succeeded(
        &self,
        saga_id: &SagaId,
    ) -> Result<bool, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM saga_step WHERE saga_id = ? \
             AND compensation_status = ?) AS present",
        )
        .bind(saga_id.as_str())
        .bind(StepCompensationStatus::Succeeded.as_str())
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        let present: i64 = row.try_get("present").map_err(map_database)?;
        Ok(present != 0)
    }

    /// 业务作用：在命令写入 Outbox 的同一事务内登记一次投递尝试，占用其全局去重身份。
    ///
    /// journal 行以 `STARTED` 落库并同步刷新 `saga_step` 的当前 attempt 投影；
    /// `UNIQUE(command_id)` 在此保证所有 phase 的投递命令全局唯一。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `phase`: 执行阶段。
    /// - `attempt`: 尝试序号。
    /// - `effect`: 跨 attempt 稳定的业务效果身份。
    /// - `command`: 本次尝试的命令身份。
    ///
    /// 返回：真实登记返回 [`AttemptStart::Recorded`]；同一 attempt 已以**相同身份**登记
    /// 过返回 `AlreadyRecorded`（崩溃重试幂等）。同一 attempt 已存在但身份不一致说明
    /// 身份派生被破坏，返回错误；事务缺失或底层失败返回错误。
    pub async fn record_attempt_started(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        effect: &EffectId,
        command: &CommandId,
    ) -> Result<AttemptStart, SagaStoreError> {
        // attempt 登记必须与命令 Outbox 同事务:journal 有行而命令未发,重试会补发;
        // 命令已发而 journal 无行,结果事件将找不到去重锚点。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let effect_text = effect.to_string();
        let command_text = command.to_string();

        let inserted = sqlx::query(
            "INSERT INTO saga_step_attempt (saga_id, step_name, phase, attempt_no, effect_id, \
             command_id, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(phase.as_str())
        .bind(attempt.get())
        .bind(&effect_text)
        .bind(&command_text)
        .bind(StepAttemptStatus::Started.as_str())
        .execute(connection.as_mut())
        .await;

        match inserted {
            Ok(_) => {}
            Err(error) if is_unique_violation(&error) => {
                let existing = sqlx::query(
                    "SELECT effect_id, command_id FROM saga_step_attempt \
                     WHERE saga_id = ? AND step_name = ? AND phase = ? AND attempt_no = ?",
                )
                .bind(saga_id.as_str())
                .bind(step.as_str())
                .bind(phase.as_str())
                .bind(attempt.get())
                .fetch_optional(connection.as_mut())
                .await
                .map_err(map_database)?;
                let Some(existing) = existing else {
                    // 本 attempt 无行却仍冲突,说明 effect/command 身份被其它步骤占用:
                    // 确定性派生保证不同输入不同身份,出现共享即身份体系被破坏。
                    return Err(SagaStoreError::new(
                        "attempt identity collides with a different step attempt",
                    ));
                };
                let existing_effect: String =
                    existing.try_get("effect_id").map_err(map_database)?;
                let existing_command: String =
                    existing.try_get("command_id").map_err(map_database)?;
                if existing_effect != effect_text || existing_command != command_text {
                    // 同一 attempt 出现两套身份意味着重派生结果漂移,继续执行会让
                    // 外部幂等键失效,必须显式失败转人工。
                    return Err(SagaStoreError::new(
                        "attempt already recorded with different identities",
                    ));
                }
                return Ok(AttemptStart::AlreadyRecorded);
            }
            Err(error) => return Err(map_database(error)),
        }

        let project_sql = match phase {
            StepPhase::Execute => PROJECT_EXECUTE_SQL,
            StepPhase::Cancel => PROJECT_CANCEL_SQL,
            StepPhase::Compensate => PROJECT_COMPENSATE_SQL,
            StepPhase::Resolve => PROJECT_RESOLVE_SQL,
        };
        // 投影只允许单调前进:journal 是事实源,旧 attempt 的迟到投影写入直接丢弃。
        sqlx::query(project_sql)
            .bind(&effect_text)
            .bind(&command_text)
            .bind(attempt.get())
            .bind(saga_id.as_str())
            .bind(step.as_str())
            .bind(attempt.get())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        Ok(AttemptStart::Recorded)
    }

    /// 业务作用：把参与方结果事件记账为 attempt 终态，重复结果幂等吸收。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `phase`: 执行阶段。
    /// - `attempt`: 尝试序号。
    /// - `status`: 裁决出的终态，不允许传 `Started`。
    /// - `outcome_event_id`: 产生该终态的结果事件 id；为空保留既有值。
    ///
    /// 返回：真实记账返回 [`AttemptOutcomeRecord::Recorded`]；同一 attempt 已记录**相同**
    /// 终态返回 `AlreadyRecorded`；已记录不同终态返回 `Conflicting`，供调用方在同一事务
    /// 留存 incoming 证据并升级人工介入。attempt 不存在仍属协议违规并返回错误。
    pub async fn record_attempt_outcome(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        status: StepAttemptStatus,
        outcome_event_id: Option<&str>,
    ) -> Result<AttemptOutcomeRecord, SagaStoreError> {
        if status.is_in_flight() {
            return Err(SagaStoreError::new(
                "attempt outcome must be a settled status",
            ));
        }
        // 结果记账必须与 Inbox claim 及后续状态推进同事务,否则重复结果事件会二次记账。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_step_attempt SET status = ?, \
             outcome_event_id = COALESCE(?, outcome_event_id), \
             finished_at = CURRENT_TIMESTAMP(6) \
             WHERE saga_id = ? AND step_name = ? AND phase = ? AND attempt_no = ? \
             AND status = ?",
        )
        .bind(status.as_str())
        .bind(outcome_event_id)
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(phase.as_str())
        .bind(attempt.get())
        .bind(StepAttemptStatus::Started.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 1 {
            return Ok(AttemptOutcomeRecord::Recorded);
        }

        let existing = sqlx::query(
            "SELECT status FROM saga_step_attempt \
             WHERE saga_id = ? AND step_name = ? AND phase = ? AND attempt_no = ?",
        )
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(phase.as_str())
        .bind(attempt.get())
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;
        let Some(existing) = existing else {
            // 结果先于命令登记到达说明协议被破坏:结果只可能由已登记命令产生。
            return Err(SagaStoreError::new(
                "attempt outcome arrived for an unrecorded attempt",
            ));
        };
        let existing_status: String = existing.try_get("status").map_err(map_database)?;
        if existing_status == status.as_str() {
            return Ok(AttemptOutcomeRecord::AlreadyRecorded);
        }
        let existing = StepAttemptStatus::parse(&existing_status)
            .ok_or_else(|| SagaStoreError::new("corrupt attempt status"))?;
        // 不覆盖先到事实，也不返回错误让 incoming 证据随事务回滚；Orchestrator 会把
        // 两个互斥状态写入 conflict 表并在同一 COMMIT 中升级人工介入。
        Ok(AttemptOutcomeRecord::Conflicting { existing })
    }

    /// 业务作用：按阶段与尝试序号加载步骤的全部 attempt 事实，供恢复与人工裁决审阅。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    ///
    /// 返回：按 phase、attempt_no 排序的事实行；读回列损坏或底层失败返回错误。
    pub async fn load_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
    ) -> Result<Vec<SagaStepAttemptRow>, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows = sqlx::query(
            "SELECT step_name, phase, attempt_no, effect_id, command_id, status, \
             outcome_event_id FROM saga_step_attempt \
             WHERE saga_id = ? AND step_name = ? ORDER BY phase ASC, attempt_no ASC",
        )
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        rows.iter().map(parse_attempt_row).collect()
    }

    /// 业务作用：按正向/补偿方向统计 resolve attempt，使两个未知效果拥有独立预算。
    ///
    /// 严格串行 Saga 在首个 compensate attempt 之前的 resolve 全属于正向，之后全属于
    /// 补偿。该分界直接来自已提交 attempt journal，不依赖进程内计数器。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤身份。
    /// - `direction`: 要统计的未知效果方向。
    ///
    /// 返回：该方向已发布的 resolve attempt 数；聚合类型漂移或数据库失败返回错误。
    pub async fn count_resolution_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        direction: nasaga_core::Direction,
    ) -> Result<u32, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let count: i64 = if direction == nasaga_core::Direction::Forward {
            // 尚未进入补偿时子查询为 NULL；此时全部 resolve 都属于正向预算。
            sqlx::query_scalar(
                "SELECT CAST(COUNT(*) AS SIGNED) FROM saga_step_attempt AS resolution \
                 WHERE resolution.saga_id = ? AND resolution.step_name = ? \
                 AND BINARY resolution.phase = 'resolve' AND ( \
                   (SELECT MIN(compensation.started_at) FROM saga_step_attempt AS compensation \
                    WHERE compensation.saga_id = resolution.saga_id \
                    AND compensation.step_name = resolution.step_name \
                    AND BINARY compensation.phase = 'compensate') IS NULL \
                   OR resolution.started_at < (SELECT MIN(compensation.started_at) \
                    FROM saga_step_attempt AS compensation \
                    WHERE compensation.saga_id = resolution.saga_id \
                    AND compensation.step_name = resolution.step_name \
                    AND BINARY compensation.phase = 'compensate'))",
            )
            .bind(saga_id.as_str())
            .bind(step.as_str())
            .fetch_one(connection.as_mut())
            .await
            .map_err(map_database)?
        } else {
            sqlx::query_scalar(
                "SELECT CAST(COUNT(*) AS SIGNED) FROM saga_step_attempt AS resolution \
                 WHERE resolution.saga_id = ? AND resolution.step_name = ? \
                 AND BINARY resolution.phase = 'resolve' AND resolution.started_at >= \
                 (SELECT MIN(compensation.started_at) FROM saga_step_attempt AS compensation \
                  WHERE compensation.saga_id = resolution.saga_id \
                  AND compensation.step_name = resolution.step_name \
                  AND BINARY compensation.phase = 'compensate')",
            )
            .bind(saga_id.as_str())
            .bind(step.as_str())
            .fetch_one(connection.as_mut())
            .await
            .map_err(map_database)?
        };
        u32::try_from(count).map_err(|_| crate::error::corrupt("resolution attempt count"))
    }

    /// 业务作用：更新一个步骤的 journal 投影状态（forward/cancel/compensation/resolution）。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `patch`: 要写入的状态子集；为空的字段保留既有值。
    ///
    /// 返回：步骤行存在并完成更新返回 `Ok`；步骤行缺失（骨架未注册或被绕过 store 删除）、
    /// 事务缺失或底层失败返回错误。
    pub async fn update_step_journal(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        patch: &StepJournalPatch<'_>,
    ) -> Result<(), SagaStoreError> {
        if let Some(code) = patch.last_error_code {
            validate_reason_code(code)?;
        }
        // 状态投影与触发它的结果记账/状态推进同事务,防止"attempt 已终态但步骤仍 PENDING"
        // 的持久化撕裂被恢复扫描读到。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_step SET \
             forward_status = COALESCE(?, forward_status), \
             cancel_status = COALESCE(?, cancel_status), \
             compensation_status = COALESCE(?, compensation_status), \
             resolution_status = COALESCE(?, resolution_status), \
             last_error_code = COALESCE(?, last_error_code), \
             finished_at = IF(?, COALESCE(finished_at, CURRENT_TIMESTAMP(6)), finished_at) \
             WHERE saga_id = ? AND step_name = ?",
        )
        .bind(patch.forward_status.map(StepForwardStatus::as_str))
        .bind(patch.cancel_status.map(StepCancelStatus::as_str))
        .bind(
            patch
                .compensation_status
                .map(StepCompensationStatus::as_str),
        )
        .bind(patch.resolution_status.map(StepResolutionStatus::as_str))
        .bind(patch.last_error_code)
        .bind(patch.mark_finished)
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Err(SagaStoreError::new("step journal row is missing"));
        }
        Ok(())
    }

    /// 业务作用：在已审计管理操作的同一事务内重新打开一个 `HALTED` 补偿投影。
    ///
    /// 本方法不接受普通状态补丁：它会反查 `saga_management_audit` 中完全相同的
    /// `retry_frozen_compensation` operation，确保自动 timer 或未审计调用方不能把冻结态
    /// 改回 `PENDING`。参与方仍需看到命令上的同一恢复身份后才会重新执行外部补偿。
    ///
    /// 参数说明：
    /// - `saga_id`: 人工介入实例身份。
    /// - `step`: 冻结计划中的补偿步骤。
    /// - `operation_id`: 已由管理入口原子写入审计表的稳定操作身份。
    ///
    /// 返回：唯一 HALTED 计划成员成功重开返回 `Ok`；审计缺失、状态/计划不匹配、事务
    /// 缺失或数据库失败返回错误并回滚整个管理操作。
    pub async fn reopen_halted_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaStoreError> {
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        // 审计证据必须先存在于本事务；先改投影再补审计会留下可被崩溃窗口越权利用的
        // PENDING 补偿，并让自动 worker 在人工授权尚未成立时发出外部请求。
        let updated = sqlx::query(
            "UPDATE saga_step AS step_row JOIN saga_management_audit AS audit_row \
             ON audit_row.saga_id = step_row.saga_id AND audit_row.operation_id = ? \
             AND BINARY audit_row.action = 'retry_frozen_compensation' \
             SET step_row.compensation_status = ?, step_row.resolution_status = ? \
             WHERE step_row.saga_id = ? AND step_row.step_name = ? \
             AND step_row.compensation_plan_version IS NOT NULL \
             AND step_row.compensation_status = ?",
        )
        .bind(operation_id)
        .bind(StepCompensationStatus::Pending.as_str())
        .bind(StepResolutionStatus::None.as_str())
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(StepCompensationStatus::Halted.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() != 1 {
            return Err(SagaStoreError::new(
                "halted compensation recovery requires a matching management audit and frozen plan",
            ));
        }
        Ok(())
    }

    /// 业务作用：在已审计管理操作的同一事务内重新打开 `HALTED` 的 Unknown 查询投影。
    ///
    /// 参数说明：
    /// - `saga_id`: 人工介入实例身份。
    /// - `step`: 仍保留 Unknown 业务事实的步骤。
    /// - `operation_id`: 已写入 `retry_resolution` 审计行的稳定操作身份。
    ///
    /// 返回：HALTED 查询唯一重开返回 `Ok`；审计或 Unknown 事实缺失、事务缺失及数据库
    /// 失败返回错误，禁止只迁移实例却不开放参与方查询 gate。
    pub async fn reopen_halted_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaStoreError> {
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        // 只保留正向或补偿 Unknown 的行有资格重开；其它 HALTED 代表协议破坏，必须继续冻结。
        let updated = sqlx::query(
            "UPDATE saga_step AS step_row JOIN saga_management_audit AS audit_row \
             ON audit_row.saga_id = step_row.saga_id AND audit_row.operation_id = ? \
             AND BINARY audit_row.action = 'retry_resolution' \
             SET step_row.resolution_status = ? \
             WHERE step_row.saga_id = ? AND step_row.step_name = ? \
             AND step_row.resolution_status = ? \
             AND (step_row.forward_status = ? OR step_row.compensation_status = ?)",
        )
        .bind(operation_id)
        .bind(StepResolutionStatus::Pending.as_str())
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(StepResolutionStatus::Halted.as_str())
        .bind(StepForwardStatus::Unknown.as_str())
        .bind(StepCompensationStatus::Unknown.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() != 1 {
            return Err(SagaStoreError::new(
                "halted resolution recovery requires a matching management audit and unknown fact",
            ));
        }
        Ok(())
    }

    /// 业务作用：把冻结的补偿计划落到 step journal，标记计划成员与稳定逆序位置。
    ///
    /// 计划成员的 `compensation_status` 从 `NONE` 置为 `PENDING`（已推进的保持原状），
    /// 与实例进入 `COMPENSATING` 的 CAS 同事务提交，保证"计划已冻结"与"成员已标记"
    /// 不可分割。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `plan`: 冻结出的补偿计划。
    ///
    /// 返回：全部计划成员标记完成返回 `Ok`；计划引用缺失步骤（计划与 journal 出自
    /// 不同事实）、事务缺失或底层失败返回错误。
    pub async fn mark_compensation_plan(
        &self,
        saga_id: &SagaId,
        plan: &CompensationPlan,
    ) -> Result<(), SagaStoreError> {
        // 计划标记与进入 COMPENSATING 的 CAS 必须同事务:只冻结实例摘要而不标记成员,
        // 恢复时将无法从持久化事实还原"哪些步骤在计划内"。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        for entry in plan.entries() {
            let updated = sqlx::query(
                "UPDATE saga_step SET compensation_plan_version = ?, compensation_order = ?, \
                 compensation_status = IF(compensation_status = ?, ?, compensation_status) \
                 WHERE saga_id = ? AND step_name = ?",
            )
            .bind(plan.version())
            .bind(entry.order())
            .bind(StepCompensationStatus::None.as_str())
            .bind(StepCompensationStatus::Pending.as_str())
            .bind(saga_id.as_str())
            .bind(entry.step().as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
            if updated.rows_affected() == 0 {
                return Err(SagaStoreError::new(
                    "compensation plan references a missing step",
                ));
            }
        }
        Ok(())
    }
}
