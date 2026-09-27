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
use sqlx::{Connection as _, Row as _};

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
     traceparent VARCHAR(55) NULL, \
     paused_at TIMESTAMP(6) NULL, \
     created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (saga_id), \
     UNIQUE KEY uk_business (tenant_id, workflow_name, business_key), \
     KEY idx_tenant_saga (tenant_id, saga_id), \
     KEY idx_tenant_workflow_saga (tenant_id, workflow_name, saga_id), \
     KEY idx_tenant_created_saga (tenant_id, created_at, saga_id), \
     KEY idx_tenant_workflow_created_saga (tenant_id, workflow_name, created_at, saga_id), \
     KEY idx_tenant_status_saga (tenant_id, status, saga_id), \
     KEY idx_tenant_workflow_status_saga (tenant_id, workflow_name, status, saga_id), \
     KEY idx_tenant_status_created_saga (tenant_id, status, created_at, saga_id), \
     KEY idx_tenant_workflow_status_created_saga (tenant_id, workflow_name, status, created_at, saga_id), \
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

/// 同一 Saga 审计事件的事务提交栅栏。`generation` 只用于制造持久行变更并持锁到提交，
/// 公开游标仍使用事件表的 `audit_seq`。
const CREATE_AUDIT_STREAM_GUARD_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_audit_stream_guard ( \
     saga_id VARCHAR(256) NOT NULL, \
     generation BIGINT UNSIGNED NOT NULL, \
     PRIMARY KEY (saga_id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 跨类别审计事件表。每个事实插入与 attempt 终态变化都形成不可变快照，`audit_seq`
/// 由数据库全局分配，公开分页只按该序号前进，不再依赖类别或可变业务键。
const CREATE_AUDIT_EVENT_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_audit_event ( \
     audit_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, \
     saga_id VARCHAR(256) NOT NULL, \
     record_kind VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, \
     event_identity_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, \
     event_revision_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, \
     step_name VARCHAR(128) NULL, phase VARCHAR(16) NULL, attempt_no INT UNSIGNED NULL, \
     effect_id CHAR(36) NULL, command_id CHAR(36) NULL, attempt_status VARCHAR(32) NULL, \
     outcome_event_id VARCHAR(190) NULL, transition_seq BIGINT UNSIGNED NULL, \
     from_state VARCHAR(32) NULL, to_state VARCHAR(32) NULL, trigger_kind VARCHAR(8) NULL, \
     trigger_id VARCHAR(190) NULL, definition_version INT UNSIGNED NULL, \
     control_seq BIGINT UNSIGNED NULL, operation_id VARCHAR(190) NULL, action VARCHAR(64) NULL, \
     actor VARCHAR(128) NULL, reason VARCHAR(512) NULL, incoming_event_id VARCHAR(190) NULL, \
     existing_status VARCHAR(32) NULL, incoming_status VARCHAR(32) NULL, \
     conflict_kind VARCHAR(64) NULL, occurred_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     PRIMARY KEY (audit_seq), \
     UNIQUE KEY uk_saga_audit_event_revision \
       (saga_id, record_kind, event_identity_digest, event_revision_digest), \
     KEY idx_saga_audit_event_stream (saga_id, audit_seq) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

// trigger 先递增所属 Saga 的 guard，再分配自增序号；同一 Saga 的较高序号因此不能先于
// 较低序号事务可见，而不同 Saga 不共享该行锁。
const CREATE_ATTEMPT_INSERT_AUDIT_TRIGGER_SQL: &str = "CREATE TRIGGER nasaga_audit_attempt_insert \
     AFTER INSERT ON saga_step_attempt FOR EACH ROW \
     BEGIN \
     INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1) \
       ON DUPLICATE KEY UPDATE generation = generation + 1; \
     INSERT IGNORE INTO saga_audit_event \
       (saga_id, record_kind, event_identity_digest, event_revision_digest, step_name, phase, \
        attempt_no, effect_id, command_id, attempt_status, outcome_event_id, occurred_at) \
     VALUES (NEW.saga_id, 'attempt', \
       SHA2(CONCAT_WS(CHAR(31), NEW.step_name, NEW.phase, NEW.attempt_no), 256), \
       SHA2('started', 256), NEW.step_name, NEW.phase, NEW.attempt_no, NEW.effect_id, \
       NEW.command_id, NEW.status, NEW.outcome_event_id, NEW.started_at); \
     END";

const CREATE_ATTEMPT_UPDATE_AUDIT_TRIGGER_SQL: &str = "CREATE TRIGGER nasaga_audit_attempt_update \
     AFTER UPDATE ON saga_step_attempt FOR EACH ROW \
     BEGIN \
     INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1) \
       ON DUPLICATE KEY UPDATE generation = generation + 1; \
     INSERT IGNORE INTO saga_audit_event \
       (saga_id, record_kind, event_identity_digest, event_revision_digest, step_name, phase, \
        attempt_no, effect_id, command_id, attempt_status, outcome_event_id, occurred_at) \
     VALUES (NEW.saga_id, 'attempt', \
       SHA2(CONCAT_WS(CHAR(31), NEW.step_name, NEW.phase, NEW.attempt_no), 256), \
       SHA2(CONCAT_WS(CHAR(31), 'status', NEW.status, COALESCE(NEW.outcome_event_id, '')), 256), \
       NEW.step_name, NEW.phase, NEW.attempt_no, NEW.effect_id, NEW.command_id, NEW.status, \
       NEW.outcome_event_id, COALESCE(NEW.finished_at, CURRENT_TIMESTAMP(6))); \
     END";

const CREATE_TRANSITION_AUDIT_TRIGGER_SQL: &str = "CREATE TRIGGER nasaga_audit_transition_insert \
     AFTER INSERT ON saga_transition FOR EACH ROW \
     BEGIN \
     INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1) \
       ON DUPLICATE KEY UPDATE generation = generation + 1; \
     INSERT IGNORE INTO saga_audit_event \
       (saga_id, record_kind, event_identity_digest, event_revision_digest, transition_seq, \
        from_state, to_state, trigger_kind, trigger_id, definition_version, occurred_at) \
     VALUES (NEW.saga_id, 'transition', SHA2(CAST(NEW.transition_seq AS CHAR), 256), \
       SHA2('fact', 256), NEW.transition_seq, NEW.from_state, NEW.to_state, NEW.trigger_kind, \
       NEW.trigger_id, NEW.definition_version, NEW.occurred_at); \
     END";

const CREATE_CONTROL_AUDIT_TRIGGER_SQL: &str = "CREATE TRIGGER nasaga_audit_control_insert \
     AFTER INSERT ON saga_control_transition FOR EACH ROW \
     BEGIN \
     INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1) \
       ON DUPLICATE KEY UPDATE generation = generation + 1; \
     INSERT IGNORE INTO saga_audit_event \
       (saga_id, record_kind, event_identity_digest, event_revision_digest, control_seq, \
        from_state, to_state, operation_id, actor, reason, occurred_at) \
     VALUES (NEW.saga_id, 'control', SHA2(NEW.operation_id, 256), SHA2('fact', 256), \
       NEW.control_seq, NEW.from_state, NEW.to_state, NEW.operation_id, NEW.actor, NEW.reason, \
       NEW.occurred_at); \
     END";

const CREATE_MANAGEMENT_AUDIT_TRIGGER_SQL: &str = "CREATE TRIGGER nasaga_audit_management_insert \
     AFTER INSERT ON saga_management_audit FOR EACH ROW \
     BEGIN \
     INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1) \
       ON DUPLICATE KEY UPDATE generation = generation + 1; \
     INSERT IGNORE INTO saga_audit_event \
       (saga_id, record_kind, event_identity_digest, event_revision_digest, operation_id, \
        action, actor, reason, occurred_at) \
     VALUES (NEW.saga_id, 'management', SHA2(NEW.operation_id, 256), SHA2('fact', 256), \
       NEW.operation_id, NEW.action, NEW.actor, NEW.reason, NEW.occurred_at); \
     END";

const CREATE_CONFLICT_AUDIT_TRIGGER_SQL: &str = "CREATE TRIGGER nasaga_audit_conflict_insert \
     AFTER INSERT ON saga_conflict_fact FOR EACH ROW \
     BEGIN \
     INSERT INTO saga_audit_stream_guard (saga_id, generation) VALUES (NEW.saga_id, 1) \
       ON DUPLICATE KEY UPDATE generation = generation + 1; \
     INSERT IGNORE INTO saga_audit_event \
       (saga_id, record_kind, event_identity_digest, event_revision_digest, incoming_event_id, \
        step_name, phase, attempt_no, existing_status, incoming_status, conflict_kind, occurred_at) \
     VALUES (NEW.saga_id, 'conflict', SHA2(NEW.incoming_event_id, 256), SHA2('fact', 256), \
       NEW.incoming_event_id, NEW.step_name, NEW.phase, NEW.attempt_no, NEW.existing_status, \
       NEW.incoming_status, NEW.conflict_kind, NEW.occurred_at); \
     END";

const AUDIT_GUARD_UPSERT_SQL: &str = "INSERT INTO saga_audit_stream_guard \
     (saga_id, generation) VALUES (?, 1) \
     ON DUPLICATE KEY UPDATE generation = generation + 1";

const AUDIT_TRIGGER_GUARD_BODY_SQL: &str = "INSERT INTO saga_audit_stream_guard \
     (saga_id, generation) VALUES (NEW.saga_id, 1) \
     ON DUPLICATE KEY UPDATE generation = generation + 1";

#[derive(Clone, Copy)]
struct AuditTriggerContract {
    name: &'static str,
    event: &'static str,
    table: &'static str,
    drop_statement: &'static str,
    create_statement: &'static str,
}

const AUDIT_TRIGGER_CONTRACTS: [AuditTriggerContract; 6] = [
    AuditTriggerContract {
        name: "nasaga_audit_attempt_insert",
        event: "INSERT",
        table: "saga_step_attempt",
        drop_statement: "DROP TRIGGER nasaga_audit_attempt_insert",
        create_statement: CREATE_ATTEMPT_INSERT_AUDIT_TRIGGER_SQL,
    },
    AuditTriggerContract {
        name: "nasaga_audit_attempt_update",
        event: "UPDATE",
        table: "saga_step_attempt",
        drop_statement: "DROP TRIGGER nasaga_audit_attempt_update",
        create_statement: CREATE_ATTEMPT_UPDATE_AUDIT_TRIGGER_SQL,
    },
    AuditTriggerContract {
        name: "nasaga_audit_transition_insert",
        event: "INSERT",
        table: "saga_transition",
        drop_statement: "DROP TRIGGER nasaga_audit_transition_insert",
        create_statement: CREATE_TRANSITION_AUDIT_TRIGGER_SQL,
    },
    AuditTriggerContract {
        name: "nasaga_audit_control_insert",
        event: "INSERT",
        table: "saga_control_transition",
        drop_statement: "DROP TRIGGER nasaga_audit_control_insert",
        create_statement: CREATE_CONTROL_AUDIT_TRIGGER_SQL,
    },
    AuditTriggerContract {
        name: "nasaga_audit_management_insert",
        event: "INSERT",
        table: "saga_management_audit",
        drop_statement: "DROP TRIGGER nasaga_audit_management_insert",
        create_statement: CREATE_MANAGEMENT_AUDIT_TRIGGER_SQL,
    },
    AuditTriggerContract {
        name: "nasaga_audit_conflict_insert",
        event: "INSERT",
        table: "saga_conflict_fact",
        drop_statement: "DROP TRIGGER nasaga_audit_conflict_insert",
        create_statement: CREATE_CONFLICT_AUDIT_TRIGGER_SQL,
    },
];

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
        Self::ensure_schema_for(natx::DEFAULT_DATASOURCE).await
    }

    /// 业务作用：在指定 datasource 上创建并复验全部 Saga 持久结构。
    ///
    /// 参数说明：`datasource` 是启动期已注册的数据源名称。
    ///
    /// 返回：结构完整时成功；名称、连接或结构升级失败时返回脱敏错误。
    pub async fn ensure_schema_for(datasource: impl AsRef<str>) -> Result<(), SagaStoreError> {
        let datasource = natx::DatasourceRef::new(datasource).map_err(map_connection)?;
        let mut connection = natx::conn_for(&datasource).await.map_err(map_connection)?;
        Self::ensure_schema_on_connection(&mut connection).await
    }

    /// 业务作用：复用调用方已持有的 MySQL 连接创建并复验全部 Saga 持久结构，使 schema
    /// 互斥权与全部 DDL 位于同一数据库会话。
    ///
    /// 参数说明：`connection` 是调用方已取得 schema 互斥权的连接。
    ///
    /// 返回：结构完整时成功；DDL、历史收敛或最终结构门禁失败时返回脱敏错误。
    pub async fn ensure_schema_on_connection(
        connection: &mut natx::Conn,
    ) -> Result<(), SagaStoreError> {
        for statement in [
            CREATE_INSTANCE_SQL,
            CREATE_STEP_SQL,
            CREATE_ATTEMPT_SQL,
            CREATE_TRANSITION_SQL,
            CREATE_CONTROL_TRANSITION_SQL,
            CREATE_MANAGEMENT_AUDIT_SQL,
            CREATE_CONFLICT_FACT_SQL,
            CREATE_AUDIT_STREAM_GUARD_SQL,
            CREATE_AUDIT_EVENT_SQL,
            CREATE_TIMER_SQL,
            crate::quota::CREATE_QUOTA_SQL,
            crate::action_rate::CREATE_ACTION_RATE_SQL,
        ] {
            sqlx::query(statement)
                .execute(connection.as_mut())
                .await
                .map_err(map_database)?;
        }
        // 旧演示库的配额账本补初始化标记列;生产环境由正式 migration 拥有该变更。
        let _ = sqlx::query(
            "ALTER TABLE saga_tenant_quota ADD COLUMN initialized TINYINT NOT NULL DEFAULT 0",
        )
        .execute(connection.as_mut())
        .await;
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
        // 实例因果上下文列:创建入口显式 trace 在此落库,result 推进同事务更新,timer 与
        // 崩溃恢复读取它保持链路连续。生产库由 saga_instance_trace_context 迁移添加。
        let has_traceparent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_instance' \
             AND COLUMN_NAME = 'traceparent'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if has_traceparent == 0 {
            sqlx::query(
                "ALTER TABLE saga_instance ADD COLUMN traceparent VARCHAR(55) NULL \
                 AFTER failure_code",
            )
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
        ensure_instance_query_index(
            connection,
            "idx_tenant_saga",
            &["tenant_id", "saga_id"],
            "CREATE INDEX idx_tenant_saga ON saga_instance (tenant_id, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_workflow_saga",
            &["tenant_id", "workflow_name", "saga_id"],
            "CREATE INDEX idx_tenant_workflow_saga \
             ON saga_instance (tenant_id, workflow_name, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_created_saga",
            &["tenant_id", "created_at", "saga_id"],
            "CREATE INDEX idx_tenant_created_saga \
             ON saga_instance (tenant_id, created_at, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_workflow_created_saga",
            &["tenant_id", "workflow_name", "created_at", "saga_id"],
            "CREATE INDEX idx_tenant_workflow_created_saga \
             ON saga_instance (tenant_id, workflow_name, created_at, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_status_saga",
            &["tenant_id", "status", "saga_id"],
            "CREATE INDEX idx_tenant_status_saga \
             ON saga_instance (tenant_id, status, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_workflow_status_saga",
            &["tenant_id", "workflow_name", "status", "saga_id"],
            "CREATE INDEX idx_tenant_workflow_status_saga \
             ON saga_instance (tenant_id, workflow_name, status, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_status_created_saga",
            &["tenant_id", "status", "created_at", "saga_id"],
            "CREATE INDEX idx_tenant_status_created_saga \
             ON saga_instance (tenant_id, status, created_at, saga_id)",
        )
        .await?;
        ensure_instance_query_index(
            connection,
            "idx_tenant_workflow_status_created_saga",
            &[
                "tenant_id",
                "workflow_name",
                "status",
                "created_at",
                "saga_id",
            ],
            "CREATE INDEX idx_tenant_workflow_status_created_saga \
             ON saga_instance (tenant_id, workflow_name, status, created_at, saga_id)",
        )
        .await?;
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
        // trigger 收敛和历史回填都会触碰不可变审计流；先确认 guard、事件表与逻辑
        // 唯一键完整，避免拒绝 Ready 之前已经复制无法自动判定去留的事实。
        verify_audit_storage_schema(connection).await?;
        for contract in AUDIT_TRIGGER_CONTRACTS {
            ensure_audit_trigger(connection, contract).await?;
        }
        backfill_audit_events(connection).await?;
        verify_audit_schema(connection).await?;
        Ok(())
    }
}

/// 业务作用：为实例管理查询建立并复验稳定的 keyset 索引，拒绝采用同名但列顺序不同的结构。
///
/// 参数说明：
/// - `connection`：执行自举且持有 schema 互斥权的连接。
/// - `name`：索引合同中的稳定名称。
/// - `columns`：按前导顺序排列的完整列合同。
/// - `create_statement`：索引不存在时执行的确定性 DDL。
///
/// 返回：索引不存在时创建后复验，合同匹配时成功；同名漂移或数据库失败时返回脱敏错误。
async fn ensure_instance_query_index(
    connection: &mut natx::Conn,
    name: &str,
    columns: &[&str],
    create_statement: &'static str,
) -> Result<(), SagaStoreError> {
    let mut rows = sqlx::query(
        "SELECT COLUMN_NAME, NON_UNIQUE, SUB_PART FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_instance' AND INDEX_NAME = ? \
         ORDER BY SEQ_IN_INDEX",
    )
    .bind(name)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.is_empty() {
        sqlx::query(create_statement)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        rows = sqlx::query(
            "SELECT COLUMN_NAME, NON_UNIQUE, SUB_PART FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_instance' AND INDEX_NAME = ? \
             ORDER BY SEQ_IN_INDEX",
        )
        .bind(name)
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
    }
    if rows.len() != columns.len() {
        return Err(SagaStoreError::new(
            "Saga instance management index contract is invalid",
        ));
    }
    for (row, expected) in rows.iter().zip(columns) {
        let actual: String = row.try_get("COLUMN_NAME").map_err(map_database)?;
        let non_unique: i64 = row.try_get("NON_UNIQUE").map_err(map_database)?;
        let prefix: Option<i64> = row.try_get("SUB_PART").map_err(map_database)?;
        if actual != *expected || non_unique != 1 || prefix.is_some() {
            return Err(SagaStoreError::new(
                "Saga instance management index contract is invalid",
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct AuditTriggerDefinition {
    event: String,
    table: String,
    timing: String,
    orientation: String,
    body: String,
}

#[derive(Clone, Copy)]
struct AuditColumnContract {
    name: &'static str,
    data_type: &'static str,
    column_type: &'static str,
    nullable: bool,
    collation: Option<&'static str>,
    default: Option<&'static str>,
    extra: &'static str,
}

const AUDIT_GUARD_COLUMNS: [AuditColumnContract; 2] = [
    AuditColumnContract {
        name: "saga_id",
        data_type: "varchar",
        column_type: "varchar(256)",
        nullable: false,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "generation",
        data_type: "bigint",
        column_type: "bigint unsigned",
        nullable: false,
        collation: None,
        default: None,
        extra: "",
    },
];

const AUDIT_EVENT_COLUMNS: [AuditColumnContract; 28] = [
    AuditColumnContract {
        name: "audit_seq",
        data_type: "bigint",
        column_type: "bigint unsigned",
        nullable: false,
        collation: None,
        default: None,
        extra: "auto_increment",
    },
    AuditColumnContract {
        name: "saga_id",
        data_type: "varchar",
        column_type: "varchar(256)",
        nullable: false,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "record_kind",
        data_type: "varchar",
        column_type: "varchar(16)",
        nullable: false,
        collation: Some("ascii_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "event_identity_digest",
        data_type: "char",
        column_type: "char(64)",
        nullable: false,
        collation: Some("ascii_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "event_revision_digest",
        data_type: "char",
        column_type: "char(64)",
        nullable: false,
        collation: Some("ascii_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "step_name",
        data_type: "varchar",
        column_type: "varchar(128)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "phase",
        data_type: "varchar",
        column_type: "varchar(16)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "attempt_no",
        data_type: "int",
        column_type: "int unsigned",
        nullable: true,
        collation: None,
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "effect_id",
        data_type: "char",
        column_type: "char(36)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "command_id",
        data_type: "char",
        column_type: "char(36)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "attempt_status",
        data_type: "varchar",
        column_type: "varchar(32)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "outcome_event_id",
        data_type: "varchar",
        column_type: "varchar(190)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "transition_seq",
        data_type: "bigint",
        column_type: "bigint unsigned",
        nullable: true,
        collation: None,
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "from_state",
        data_type: "varchar",
        column_type: "varchar(32)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "to_state",
        data_type: "varchar",
        column_type: "varchar(32)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "trigger_kind",
        data_type: "varchar",
        column_type: "varchar(8)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "trigger_id",
        data_type: "varchar",
        column_type: "varchar(190)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "definition_version",
        data_type: "int",
        column_type: "int unsigned",
        nullable: true,
        collation: None,
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "control_seq",
        data_type: "bigint",
        column_type: "bigint unsigned",
        nullable: true,
        collation: None,
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "operation_id",
        data_type: "varchar",
        column_type: "varchar(190)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "action",
        data_type: "varchar",
        column_type: "varchar(64)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "actor",
        data_type: "varchar",
        column_type: "varchar(128)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "reason",
        data_type: "varchar",
        column_type: "varchar(512)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "incoming_event_id",
        data_type: "varchar",
        column_type: "varchar(190)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "existing_status",
        data_type: "varchar",
        column_type: "varchar(32)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "incoming_status",
        data_type: "varchar",
        column_type: "varchar(32)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "conflict_kind",
        data_type: "varchar",
        column_type: "varchar(64)",
        nullable: true,
        collation: Some("utf8mb4_bin"),
        default: None,
        extra: "",
    },
    AuditColumnContract {
        name: "occurred_at",
        data_type: "timestamp",
        column_type: "timestamp(6)",
        nullable: false,
        collation: None,
        default: Some("current_timestamp(6)"),
        extra: "DEFAULT_GENERATED",
    },
];

/// 业务作用：确保统一审计 trigger 满足当前语义；只允许缺失对象或已知旧定义收敛，
/// 未知同名对象拒绝进入 Ready，避免事实提交后没有对应审计事件。
///
/// 参数说明：`connection` 是自举连接，`contract` 描述 trigger 的目标表、事件和执行体。
///
/// 返回：对象符合当前合同或从已知旧定义升级成功时返回 `Ok`；未知结构或数据库失败返回错误。
async fn ensure_audit_trigger(
    connection: &mut natx::Conn,
    contract: AuditTriggerContract,
) -> Result<(), SagaStoreError> {
    let Some(definition) = load_audit_trigger(connection, contract.name).await? else {
        create_audit_trigger(connection, contract.create_statement).await?;
        return Ok(());
    };
    if trigger_metadata_matches(&definition, contract)
        && normalize_mysql_sql(&definition.body)
            == expected_trigger_body(contract.create_statement)?
    {
        return Ok(());
    }
    if trigger_metadata_matches(&definition, contract)
        && normalize_mysql_sql(&definition.body)
            == expected_pre_guard_trigger_body(contract.create_statement)?
    {
        // 仅识别不含 guard 的早期事件写入定义；删除与创建语句均为编译期常量，
        // 未知执行体不会被覆盖，以免掩盖人工对象或不完整迁移。
        sqlx::raw_sql(contract.drop_statement)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        create_audit_trigger(connection, contract.create_statement).await?;
        return Ok(());
    }
    Err(SagaStoreError::new(
        "Saga audit trigger contract is invalid",
    ))
}

/// 业务作用：读取 trigger 的权威元数据，供自举和最终合同复验共享同一判断依据。
///
/// 参数说明：`connection` 是目标数据库连接，`name` 是编译期固定的 trigger 名称。
///
/// 返回：对象存在时返回完整定义，不存在时返回 `None`；元数据读取失败返回错误。
async fn load_audit_trigger(
    connection: &mut natx::Conn,
    name: &str,
) -> Result<Option<AuditTriggerDefinition>, SagaStoreError> {
    let row = sqlx::query(
        "SELECT EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_TIMING, ACTION_ORIENTATION, \
                ACTION_STATEMENT \
         FROM information_schema.TRIGGERS \
         WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = ?",
    )
    .bind(name)
    .fetch_optional(connection.as_mut())
    .await
    .map_err(map_database)?;
    row.map(|row| {
        Ok(AuditTriggerDefinition {
            event: row.try_get("EVENT_MANIPULATION").map_err(map_database)?,
            table: row.try_get("EVENT_OBJECT_TABLE").map_err(map_database)?,
            timing: row.try_get("ACTION_TIMING").map_err(map_database)?,
            orientation: row.try_get("ACTION_ORIENTATION").map_err(map_database)?,
            body: row.try_get("ACTION_STATEMENT").map_err(map_database)?,
        })
    })
    .transpose()
}

/// 业务作用：使用受控静态 DDL 创建审计 trigger，保证 prepared protocol 限制不改变自举语义。
///
/// 参数说明：`connection` 是目标连接，`statement` 只能来自编译期 trigger 合同。
///
/// 返回：创建成功返回 `Ok`；DDL 被拒绝或连接失败返回脱敏错误。
async fn create_audit_trigger(
    connection: &mut natx::Conn,
    statement: &'static str,
) -> Result<(), SagaStoreError> {
    sqlx::raw_sql(statement)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
    Ok(())
}

/// 业务作用：核对 trigger 的挂载位置与行级触发语义，防止正确名称绑定到错误事实表或事件。
///
/// 参数说明：`definition` 是数据库对象，`contract` 是当前静态合同。
///
/// 返回：目标表、事件、时机和行级语义全部一致时返回 `true`。
fn trigger_metadata_matches(
    definition: &AuditTriggerDefinition,
    contract: AuditTriggerContract,
) -> bool {
    definition.event.eq_ignore_ascii_case(contract.event)
        && definition.table == contract.table
        && definition.timing.eq_ignore_ascii_case("AFTER")
        && definition.orientation.eq_ignore_ascii_case("ROW")
}

/// 业务作用：把 MySQL 格式化差异归一化，同时保留字符串字面量内容，用于比较 trigger 业务语义。
///
/// 参数说明：`sql` 是数据库返回或编译期定义中的 SQL 片段。
///
/// 返回：移除非字面量空白与标识符引号、统一关键字大小写后的稳定文本。
fn normalize_mysql_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut in_literal = false;
    let mut escaped = false;
    for character in sql.chars() {
        if in_literal {
            normalized.push(character);
            if character == '\'' && !escaped {
                in_literal = false;
            }
            escaped = character == '\\' && !escaped;
            if character != '\\' {
                escaped = false;
            }
        } else if character == '\'' {
            in_literal = true;
            normalized.push(character);
        } else if !character.is_whitespace() && character != '`' {
            normalized.extend(character.to_lowercase());
        }
    }
    while normalized.ends_with(';') {
        normalized.pop();
    }
    normalized
}

/// 业务作用：从静态 CREATE TRIGGER 提取规范化执行体，避免把对象名称和格式化差异纳入语义摘要。
///
/// 参数说明：`statement` 是当前编译期 trigger DDL。
///
/// 返回：DDL 合法时返回规范化执行体；静态合同不完整时返回内部结构错误。
fn expected_trigger_body(statement: &'static str) -> Result<String, SagaStoreError> {
    statement
        .split_once(" FOR EACH ROW ")
        .map(|(_, body)| normalize_mysql_sql(body))
        .ok_or_else(|| SagaStoreError::new("Saga audit trigger contract is invalid"))
}

/// 业务作用：从当前 trigger 合同推导唯一可升级的旧执行体，即写入同一事件但尚未取得 Saga guard。
///
/// 参数说明：`statement` 是当前编译期 trigger DDL。
///
/// 返回：合同可解析时返回已知旧执行体摘要；静态定义不符合包装约束时返回内部结构错误。
fn expected_pre_guard_trigger_body(statement: &'static str) -> Result<String, SagaStoreError> {
    let current = expected_trigger_body(statement)?;
    let prefix = format!(
        "begin{};",
        normalize_mysql_sql(AUDIT_TRIGGER_GUARD_BODY_SQL)
    );
    current
        .strip_prefix(&prefix)
        .and_then(|body| body.strip_suffix(";end"))
        .map(str::to_owned)
        .ok_or_else(|| SagaStoreError::new("Saga audit trigger contract is invalid"))
}

const MISSING_AUDIT_SAGA_SQL: &str = "SELECT saga_id FROM ( \
     SELECT attempt.saga_id FROM saga_step_attempt attempt \
      WHERE NOT EXISTS (SELECT 1 FROM saga_audit_event event \
       WHERE event.saga_id = attempt.saga_id AND event.record_kind = 'attempt' \
       AND event.event_identity_digest = SHA2(CONCAT_WS(CHAR(31), attempt.step_name, \
           attempt.phase, attempt.attempt_no), 256) \
       AND event.event_revision_digest = SHA2(IF(attempt.status = 'STARTED', 'started', \
           CONCAT_WS(CHAR(31), 'status', attempt.status, COALESCE(attempt.outcome_event_id, ''))), 256)) \
     UNION SELECT transition.saga_id FROM saga_transition transition \
      WHERE NOT EXISTS (SELECT 1 FROM saga_audit_event event \
       WHERE event.saga_id = transition.saga_id AND event.record_kind = 'transition' \
       AND event.event_identity_digest = SHA2(CAST(transition.transition_seq AS CHAR), 256) \
       AND event.event_revision_digest = SHA2('fact', 256)) \
     UNION SELECT control.saga_id FROM saga_control_transition control \
      WHERE NOT EXISTS (SELECT 1 FROM saga_audit_event event \
       WHERE event.saga_id = control.saga_id AND event.record_kind = 'control' \
       AND event.event_identity_digest = SHA2(control.operation_id, 256) \
       AND event.event_revision_digest = SHA2('fact', 256)) \
     UNION SELECT management.saga_id FROM saga_management_audit management \
      WHERE NOT EXISTS (SELECT 1 FROM saga_audit_event event \
       WHERE event.saga_id = management.saga_id AND event.record_kind = 'management' \
       AND event.event_identity_digest = SHA2(management.operation_id, 256) \
       AND event.event_revision_digest = SHA2('fact', 256)) \
     UNION SELECT conflict.saga_id FROM saga_conflict_fact conflict \
      WHERE NOT EXISTS (SELECT 1 FROM saga_audit_event event \
       WHERE event.saga_id = conflict.saga_id AND event.record_kind = 'conflict' \
       AND event.event_identity_digest = SHA2(conflict.incoming_event_id, 256) \
       AND event.event_revision_digest = SHA2('fact', 256)) \
     ) missing WHERE (? IS NULL OR saga_id > ?) ORDER BY saga_id LIMIT 128";

const AUDIT_BACKFILL_SQL: [&str; 5] = [
    "INSERT IGNORE INTO saga_audit_event \
         (saga_id, record_kind, event_identity_digest, event_revision_digest, step_name, phase, \
          attempt_no, effect_id, command_id, attempt_status, outcome_event_id, occurred_at) \
         SELECT saga_id, 'attempt', SHA2(CONCAT_WS(CHAR(31), step_name, phase, attempt_no), 256), \
          SHA2(IF(status = 'STARTED', 'started', CONCAT_WS(CHAR(31), 'status', status, \
          COALESCE(outcome_event_id, ''))), 256), step_name, phase, attempt_no, effect_id, \
          command_id, status, outcome_event_id, COALESCE(finished_at, started_at) \
         FROM saga_step_attempt WHERE saga_id = ?",
        "INSERT IGNORE INTO saga_audit_event \
         (saga_id, record_kind, event_identity_digest, event_revision_digest, transition_seq, \
          from_state, to_state, trigger_kind, trigger_id, definition_version, occurred_at) \
         SELECT saga_id, 'transition', SHA2(CAST(transition_seq AS CHAR), 256), SHA2('fact', 256), \
          transition_seq, from_state, to_state, trigger_kind, trigger_id, definition_version, \
          occurred_at FROM saga_transition WHERE saga_id = ?",
        "INSERT IGNORE INTO saga_audit_event \
         (saga_id, record_kind, event_identity_digest, event_revision_digest, control_seq, \
          from_state, to_state, operation_id, actor, reason, occurred_at) \
         SELECT saga_id, 'control', SHA2(operation_id, 256), SHA2('fact', 256), control_seq, \
          from_state, to_state, operation_id, actor, reason, occurred_at \
         FROM saga_control_transition WHERE saga_id = ?",
        "INSERT IGNORE INTO saga_audit_event \
         (saga_id, record_kind, event_identity_digest, event_revision_digest, operation_id, \
          action, actor, reason, occurred_at) \
         SELECT saga_id, 'management', SHA2(operation_id, 256), SHA2('fact', 256), operation_id, \
          action, actor, reason, occurred_at FROM saga_management_audit WHERE saga_id = ?",
        "INSERT IGNORE INTO saga_audit_event \
         (saga_id, record_kind, event_identity_digest, event_revision_digest, incoming_event_id, \
          step_name, phase, attempt_no, existing_status, incoming_status, conflict_kind, occurred_at) \
         SELECT saga_id, 'conflict', SHA2(incoming_event_id, 256), SHA2('fact', 256), \
          incoming_event_id, step_name, phase, attempt_no, existing_status, incoming_status, \
          conflict_kind, occurred_at FROM saga_conflict_fact WHERE saga_id = ?",
];

/// 业务作用：为统一事件表启用前已存在的审计事实生成当前快照，并让同一 Saga 的回填
/// 与在线写入共用提交 guard，保证公开游标不会越过迟提交的较小序号。
///
/// 参数说明：`connection` 指向完成建表与 trigger 自举的 datasource。
///
/// 返回：全部缺失事实已在 guard 保护下幂等映射时返回 `Ok`；任一事务失败返回错误。
async fn backfill_audit_events(connection: &mut natx::Conn) -> Result<(), SagaStoreError> {
    let mut after_saga_id: Option<String> = None;
    loop {
        let saga_ids: Vec<String> = sqlx::query_scalar(MISSING_AUDIT_SAGA_SQL)
            .bind(after_saga_id.as_deref())
            .bind(after_saga_id.as_deref())
            .fetch_all(connection.as_mut())
            .await
            .map_err(map_database)?;
        if saga_ids.is_empty() {
            return Ok(());
        }
        for saga_id in &saga_ids {
            let mut transaction = connection.as_mut().begin().await.map_err(map_database)?;
            // guard 必须先于历史事实读取并持有至提交；若在线事务已分配较小序号，
            // 回填会等待其提交，之后才生成更大的可见序号。
            sqlx::query(AUDIT_GUARD_UPSERT_SQL)
                .bind(saga_id)
                .execute(&mut *transaction)
                .await
                .map_err(map_database)?;
            for statement in AUDIT_BACKFILL_SQL {
                sqlx::query(statement)
                    .bind(saga_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(map_database)?;
            }
            transaction.commit().await.map_err(map_database)?;
        }
        after_saga_id = saga_ids.last().cloned();
    }
}

/// 业务作用：复验统一审计流的 guard、事件表及逻辑唯一键，为 trigger 收敛和历史回填提供只读前置门禁。
///
/// 参数说明：`connection` 是持有 schema 互斥权的目标连接。
///
/// 返回：两张表的列、引擎、排序规则和索引完整时返回 `Ok`；任何漂移或查询失败返回错误。
async fn verify_audit_storage_schema(connection: &mut natx::Conn) -> Result<(), SagaStoreError> {
    verify_audit_table(
        connection,
        "saga_audit_stream_guard",
        &AUDIT_GUARD_COLUMNS,
        &[("PRIMARY", false, "BTREE", "saga_id")],
    )
    .await?;
    verify_audit_table(
        connection,
        "saga_audit_event",
        &AUDIT_EVENT_COLUMNS,
        &[
            ("PRIMARY", false, "BTREE", "audit_seq"),
            (
                "idx_saga_audit_event_stream",
                true,
                "BTREE",
                "saga_id,audit_seq",
            ),
            (
                "uk_saga_audit_event_revision",
                false,
                "BTREE",
                "saga_id,record_kind,event_identity_digest,event_revision_digest",
            ),
        ],
    )
    .await?;
    Ok(())
}

/// 业务作用：在回填完成后独立复验统一审计表、索引与全部 trigger，确保 Ready 对应完整语义合同。
///
/// 参数说明：`connection` 是完成自举和历史映射的目标连接。
///
/// 返回：全部审计对象精确符合合同返回 `Ok`；缺列、错索引、错执行体或查询失败返回错误。
async fn verify_audit_schema(connection: &mut natx::Conn) -> Result<(), SagaStoreError> {
    verify_audit_storage_schema(connection).await?;
    for contract in AUDIT_TRIGGER_CONTRACTS {
        let definition = load_audit_trigger(connection, contract.name)
            .await?
            .ok_or_else(|| SagaStoreError::new("Saga audit trigger contract is invalid"))?;
        if !trigger_metadata_matches(&definition, contract)
            || normalize_mysql_sql(&definition.body)
                != expected_trigger_body(contract.create_statement)?
        {
            return Err(SagaStoreError::new(
                "Saga audit trigger contract is invalid",
            ));
        }
    }
    Ok(())
}

/// 业务作用：复验审计表的列顺序、类型、默认值、引擎、排序规则与完整索引集合。
///
/// 参数说明：连接和表名定位对象，`columns` 与 `indexes` 给出允许进入 Ready 的唯一结构。
///
/// 返回：结构逐项一致返回 `Ok`；任何漂移或元数据读取失败返回错误。
async fn verify_audit_table(
    connection: &mut natx::Conn,
    table: &str,
    columns: &[AuditColumnContract],
    indexes: &[(&str, bool, &str, &str)],
) -> Result<(), SagaStoreError> {
    let table_row = sqlx::query(
        "SELECT ENGINE, TABLE_COLLATION FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
    )
    .bind(table)
    .fetch_optional(connection.as_mut())
    .await
    .map_err(map_database)?
    .ok_or_else(|| SagaStoreError::new("Saga audit table contract is invalid"))?;
    let engine: String = table_row.try_get("ENGINE").map_err(map_database)?;
    let collation: String = table_row.try_get("TABLE_COLLATION").map_err(map_database)?;
    if !engine.eq_ignore_ascii_case("InnoDB") || collation != "utf8mb4_bin" {
        return Err(SagaStoreError::new("Saga audit table contract is invalid"));
    }

    let actual_columns = sqlx::query(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLLATION_NAME, \
                COLUMN_DEFAULT, EXTRA \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
    )
    .bind(table)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if actual_columns.len() != columns.len() {
        return Err(SagaStoreError::new("Saga audit table contract is invalid"));
    }
    for (row, expected) in actual_columns.iter().zip(columns) {
        let name: String = row.try_get("COLUMN_NAME").map_err(map_database)?;
        let data_type: String = row.try_get("DATA_TYPE").map_err(map_database)?;
        let column_type: String = row.try_get("COLUMN_TYPE").map_err(map_database)?;
        let nullable: String = row.try_get("IS_NULLABLE").map_err(map_database)?;
        let actual_collation: Option<String> =
            row.try_get("COLLATION_NAME").map_err(map_database)?;
        let actual_default: Option<String> = row.try_get("COLUMN_DEFAULT").map_err(map_database)?;
        let extra: String = row.try_get("EXTRA").map_err(map_database)?;
        if name != expected.name
            || !data_type.eq_ignore_ascii_case(expected.data_type)
            || !column_type.eq_ignore_ascii_case(expected.column_type)
            || (nullable == "YES") != expected.nullable
            || actual_collation.as_deref() != expected.collation
            || actual_default
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref()
                != expected.default
            || !(extra.eq_ignore_ascii_case(expected.extra)
                || (expected.extra == "DEFAULT_GENERATED" && extra.is_empty()))
        {
            return Err(SagaStoreError::new("Saga audit table contract is invalid"));
        }
    }

    let index_rows = sqlx::query(
        "SELECT INDEX_NAME, MIN(NON_UNIQUE) AS NON_UNIQUE, MIN(INDEX_TYPE) AS INDEX_TYPE, \
                GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS INDEX_COLUMNS \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? GROUP BY INDEX_NAME",
    )
    .bind(table)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if index_rows.len() != indexes.len() {
        return Err(SagaStoreError::new("Saga audit table contract is invalid"));
    }
    let mut actual_indexes = Vec::with_capacity(index_rows.len());
    for row in &index_rows {
        let name: String = row.try_get("INDEX_NAME").map_err(map_database)?;
        let non_unique: i64 = row.try_get("NON_UNIQUE").map_err(map_database)?;
        let index_type: String = row.try_get("INDEX_TYPE").map_err(map_database)?;
        let index_columns: String = row.try_get("INDEX_COLUMNS").map_err(map_database)?;
        actual_indexes.push((
            name,
            non_unique != 0,
            index_type.to_ascii_uppercase(),
            index_columns,
        ));
    }
    actual_indexes.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected_indexes: Vec<_> = indexes
        .iter()
        .map(|(name, non_unique, index_type, columns)| {
            (
                (*name).to_owned(),
                *non_unique,
                index_type.to_ascii_uppercase(),
                (*columns).to_owned(),
            )
        })
        .collect();
    expected_indexes.sort_by(|left, right| left.0.cmp(&right.0));
    if actual_indexes != expected_indexes {
        return Err(SagaStoreError::new("Saga audit table contract is invalid"));
    }
    Ok(())
}
