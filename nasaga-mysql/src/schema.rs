//! Saga store 的表结构与演示自举。
//!
//! 生产部署应由 migration 拥有 schema；`ensure_schema` 只服务受控自举环境。
//! 唯一键集合是本 store 的正确性支柱，与列同级维护：
//!
//! - `saga_instance` `UNIQUE(tenant_id, workflow_name, business_key)` —— 创建幂等，
//!   同一业务意图只允许一个实例；三列全部 `NOT NULL`，因为 MySQL 的 nullable unique
//!   允许多行 NULL 绕过约束。
//! - `saga_step_attempt` `PRIMARY KEY(saga_id, step_name, phase, attempt_no)` +
//!   `UNIQUE(effect_id, attempt_no)` + `UNIQUE(command_id)` —— 业务效果身份与投递尝试
//!   身份分离后的稳定去重面；`UNIQUE(command_id)` 建在统一 attempt journal 上，
//!   `saga_step` 中的 command 列只是当前 attempt 的投影。
//! - `saga_transition` `UNIQUE(saga_id, trigger_kind, trigger_id)` —— 同一触发在每个
//!   Saga 内只推进一次；`transition_seq` 直接取 CAS 推进后的 `version`，不引入第二个序列。
//! - `saga_timer` `PRIMARY KEY(saga_id, scope_kind, scope_key, kind, attempt_no)` ——
//!   同一次 attempt 的调度事务重试不产生重复计时器；重排（resume/deadline 重算）复用
//!   同一行并递增 `generation`，而不是插入新行。

use crate::error::{map_connection, map_database, SagaStoreError};
use crate::MySqlSagaStore;

/// saga_instance 建表语句。`version` 是乐观并发版本，也是 transition_seq 的唯一来源。
const CREATE_INSTANCE_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_instance ( \
     saga_id VARCHAR(256) NOT NULL, \
     tenant_id VARCHAR(256) NOT NULL, \
     workflow_name VARCHAR(128) NOT NULL, \
     business_key VARCHAR(256) NOT NULL, \
     definition_version INT UNSIGNED NOT NULL, \
     definition_digest CHAR(64) NOT NULL, \
     start_request_digest CHAR(64) NOT NULL, \
     status VARCHAR(32) NOT NULL, \
     control_state VARCHAR(16) NOT NULL, \
     control_version BIGINT UNSIGNED NOT NULL, \
     direction VARCHAR(16) NOT NULL, \
     current_step VARCHAR(128) NULL, \
     compensation_plan_version CHAR(64) NULL, \
     version BIGINT UNSIGNED NOT NULL, \
     deadline_at BIGINT NULL, \
     failure_code VARCHAR(64) NULL, \
     paused_at TIMESTAMP(6) NULL, \
     created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id), \
     UNIQUE KEY uk_business (tenant_id, workflow_name, business_key), \
     KEY idx_status (status), \
     KEY idx_status_lifecycle (status, created_at, updated_at) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// saga_step 建表语句。四个 phase 的 effect/command/attempt 列都是当前 attempt 的投影，
/// 完整历史以 saga_step_attempt journal 为准。
const CREATE_STEP_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_step ( \
     saga_id VARCHAR(256) NOT NULL, \
     step_name VARCHAR(128) NOT NULL, \
     ordinal INT UNSIGNED NOT NULL, \
     forward_status VARCHAR(32) NOT NULL, \
     cancel_status VARCHAR(32) NOT NULL, \
     compensation_status VARCHAR(32) NOT NULL, \
     resolution_status VARCHAR(32) NOT NULL, \
     execute_effect_id CHAR(36) NULL, \
     execute_command_id CHAR(36) NULL, \
     execute_attempt INT UNSIGNED NULL, \
     cancel_effect_id CHAR(36) NULL, \
     cancel_command_id CHAR(36) NULL, \
     cancel_attempt INT UNSIGNED NULL, \
     compensate_effect_id CHAR(36) NULL, \
     compensate_command_id CHAR(36) NULL, \
     compensate_attempt INT UNSIGNED NULL, \
     resolve_effect_id CHAR(36) NULL, \
     resolve_command_id CHAR(36) NULL, \
     resolve_attempt INT UNSIGNED NULL, \
     compensation_plan_version CHAR(64) NULL, \
     compensation_order INT UNSIGNED NULL, \
     last_error_code VARCHAR(64) NULL, \
     started_at TIMESTAMP(6) NULL, \
     finished_at TIMESTAMP(6) NULL, \
     PRIMARY KEY (saga_id, step_name), \
     KEY idx_saga_ordinal (saga_id, ordinal) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// saga_step_attempt 建表语句：统一 attempt journal，所有 phase 的投递命令在此全局去重。
const CREATE_ATTEMPT_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_step_attempt ( \
     saga_id VARCHAR(256) NOT NULL, \
     step_name VARCHAR(128) NOT NULL, \
     phase VARCHAR(16) NOT NULL, \
     attempt_no INT UNSIGNED NOT NULL, \
     effect_id CHAR(36) NOT NULL, \
     command_id CHAR(36) NOT NULL, \
     status VARCHAR(32) NOT NULL, \
     outcome_event_id VARCHAR(190) NULL, \
     started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     finished_at TIMESTAMP(6) NULL, \
     PRIMARY KEY (saga_id, step_name, phase, attempt_no), \
     UNIQUE KEY uk_effect_attempt (effect_id, attempt_no), \
     UNIQUE KEY uk_command (command_id), \
     KEY idx_attempt_status_finished (status, finished_at), \
     KEY idx_attempt_retry (attempt_no) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// saga_transition 建表语句：状态迁移审计链，`transition_seq` 即 CAS 推进后的实例 version。
const CREATE_TRANSITION_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_transition ( \
     saga_id VARCHAR(256) NOT NULL, \
     transition_seq BIGINT UNSIGNED NOT NULL, \
     from_state VARCHAR(32) NOT NULL, \
     to_state VARCHAR(32) NOT NULL, \
     trigger_kind VARCHAR(8) NOT NULL, \
     trigger_id VARCHAR(190) NOT NULL, \
     definition_version INT UNSIGNED NOT NULL, \
     occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id, transition_seq), \
     UNIQUE KEY uk_trigger (saga_id, trigger_kind, trigger_id), \
     KEY idx_transition_state_time (to_state, occurred_at) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 管理控制操作审计表。`operation_id` 是 pause/resume 的稳定幂等身份；独立
/// `control_seq` 对齐操作提交后的 control_version，不占用业务 transition_seq。
const CREATE_CONTROL_TRANSITION_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_control_transition ( \
     saga_id VARCHAR(256) NOT NULL, \
     control_seq BIGINT UNSIGNED NOT NULL, \
     from_state VARCHAR(16) NOT NULL, \
     to_state VARCHAR(16) NOT NULL, \
     operation_id VARCHAR(190) NOT NULL, \
     actor VARCHAR(128) NOT NULL, \
     reason VARCHAR(512) NOT NULL, \
     occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id, control_seq), \
     UNIQUE KEY uk_control_operation (saga_id, operation_id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 人工恢复业务动作的审计表。pause/resume 已由 control transition 记录；会改变业务状态、
/// 发布命令或裁决人工介入的动作写入本表，并以 operation_id 保证管理请求幂等。
const CREATE_MANAGEMENT_AUDIT_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_management_audit ( \
     saga_id VARCHAR(256) NOT NULL, \
     operation_id VARCHAR(190) NOT NULL, \
     action VARCHAR(64) NOT NULL, \
     actor VARCHAR(128) NOT NULL, \
     reason VARCHAR(512) NOT NULL, \
     occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id, operation_id), \
     KEY idx_management_actor_time (actor, occurred_at) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 同一 attempt 出现互斥结果时的不可覆盖冲突事实；incoming event 先留证，再把实例
/// 与该记录在同一事务内升级人工介入，避免错误返回回滚后永久丢失矛盾证据。
const CREATE_CONFLICT_FACT_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_conflict_fact ( \
     saga_id VARCHAR(256) NOT NULL, \
     incoming_event_id VARCHAR(190) NOT NULL, \
     step_name VARCHAR(128) NOT NULL, \
     phase VARCHAR(16) NOT NULL, \
     attempt_no INT UNSIGNED NOT NULL, \
     existing_status VARCHAR(32) NOT NULL, \
     incoming_status VARCHAR(32) NOT NULL, \
     conflict_kind VARCHAR(64) NOT NULL, \
     occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id, incoming_event_id), \
     KEY idx_conflict_saga_time (saga_id, occurred_at) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// saga_timer 建表语句。`due_at` 是业务期限，`available_at` 只控制轮询退避；二者与
/// `claimed_until` 都使用调用方注入的 epoch 毫秒，避免依赖数据库会话时区；
/// `idx_due` 按领取可用时刻服务 claim 扫描。
const CREATE_TIMER_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_timer ( \
     saga_id VARCHAR(256) NOT NULL, \
     scope_kind VARCHAR(8) NOT NULL, \
     scope_key VARCHAR(128) NOT NULL, \
     kind VARCHAR(64) NOT NULL, \
     attempt_no INT UNSIGNED NOT NULL, \
     timer_id VARCHAR(190) NOT NULL, \
     due_at BIGINT NOT NULL, \
     available_at BIGINT NOT NULL, \
     state VARCHAR(16) NOT NULL, \
     expected_saga_version BIGINT UNSIGNED NOT NULL, \
     generation INT UNSIGNED NOT NULL, \
     owner VARCHAR(128) NULL, \
     fencing_token VARCHAR(64) NULL, \
     claimed_until BIGINT NULL, \
     created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id, scope_kind, scope_key, kind, attempt_no), \
     UNIQUE KEY uk_timer_id (timer_id), \
     KEY idx_due (state, available_at, due_at) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

impl MySqlSagaStore {
    /// 业务作用：创建全部 Saga 表。生产环境应由 migration 拥有 schema，
    /// 此方法只供受控环境自举。需先 `natx::init`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部建表语句执行成功返回 `Ok`；连接不可用或 DDL 失败返回脱敏错误。
    pub async fn ensure_schema() -> Result<(), SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        for statement in [
            CREATE_INSTANCE_SQL,
            CREATE_STEP_SQL,
            CREATE_ATTEMPT_SQL,
            CREATE_TRANSITION_SQL,
            CREATE_CONTROL_TRANSITION_SQL,
            CREATE_MANAGEMENT_AUDIT_SQL,
            CREATE_CONFLICT_FACT_SQL,
            CREATE_TIMER_SQL,
        ] {
            sqlx::query(statement)
                .execute(connection.as_mut())
                .await
                .map_err(map_database)?;
        }
        // 旧演示库可能已经由早期 Saga 原型建表；available_at 把“轮询退避”与不可变
        // 业务 deadline 分离。只在列缺失时做向前兼容，生产环境仍应使用正式 migration。
        let has_available_at: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_timer' \
             AND COLUMN_NAME = 'available_at'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if has_available_at == 0 {
            sqlx::query(
                "ALTER TABLE saga_timer ADD COLUMN available_at BIGINT NOT NULL DEFAULT 0 \
                 AFTER due_at",
            )
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
            sqlx::query("UPDATE saga_timer SET available_at = due_at WHERE available_at = 0")
                .execute(connection.as_mut())
                .await
                .map_err(map_database)?;
        }
        // 控制态使用独立 generation 防止 ACTIVE→PAUSED→ACTIVE 后，旧 pause 请求
        // 仅凭未变化的业务 version 再次命中（ABA）。生产环境应以正式 migration 添加。
        let has_control_version: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_instance' \
             AND COLUMN_NAME = 'control_version'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if has_control_version == 0 {
            sqlx::query(
                "ALTER TABLE saga_instance ADD COLUMN control_version BIGINT UNSIGNED \
                 NOT NULL DEFAULT 1 AFTER control_state",
            )
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        // 创建请求摘要防止同一 business_key 被带着不同 payload/deadline/definition
        // 静默吸收到旧实例。历史演示行无法重建原 payload，只能保留 NULL 并在重复创建时
        // fail-closed；新实例写路径始终提供 64 位摘要。
        let has_start_request_digest: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_instance' \
             AND COLUMN_NAME = 'start_request_digest'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if has_start_request_digest == 0 {
            sqlx::query(
                "ALTER TABLE saga_instance ADD COLUMN start_request_digest CHAR(64) NULL \
                 AFTER definition_digest",
            )
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        // actor/reason 是控制操作的不可抵赖证据；历史演示行只能标记为 legacy，不能伪造
        // 当时不存在的主体。生产库由正式 migration 一次性完成同样升级。
        for (column, statement) in [
            (
                "actor",
                "ALTER TABLE saga_control_transition ADD COLUMN actor VARCHAR(128) \
                 NOT NULL DEFAULT 'legacy-unknown' AFTER operation_id",
            ),
            (
                "reason",
                "ALTER TABLE saga_control_transition ADD COLUMN reason VARCHAR(512) \
                 NOT NULL DEFAULT 'legacy operation predates actor audit' AFTER actor",
            ),
        ] {
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_control_transition' \
                 AND COLUMN_NAME = ?",
            )
            .bind(column)
            .fetch_one(connection.as_mut())
            .await
            .map_err(map_database)?;
            if exists == 0 {
                sqlx::query(statement)
                    .execute(connection.as_mut())
                    .await
                    .map_err(map_database)?;
            }
        }
        Ok(())
    }
}
