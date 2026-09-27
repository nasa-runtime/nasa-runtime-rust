//! Saga PostgreSQL 持久结构与 standalone 自举入口。
//!
//! 生产部署应由 migration 拥有 schema；本模块只在新建 standalone 数据库中
//! 创建当前完整结构，不推断或改写历史结构。

use crate::error::{map_connection, map_database, SagaStoreError};
use crate::PgSagaStore;
use sqlx::Row as _;

const CREATE_SAGA_SCHEMA_SQL: &str = include_str!("../migrations/create_saga.sql");

impl PgSagaStore {
    /// 业务作用：在默认 PostgreSQL datasource 创建当前完整 Saga 结构。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 DDL 明确成功时返回 `Ok`；连接或 DDL 失败返回脱敏错误。
    pub async fn ensure_schema() -> Result<(), SagaStoreError> {
        Self::ensure_schema_for(natx_pgsql::DEFAULT_DATASOURCE).await
    }

    /// 业务作用：在命名 PostgreSQL datasource 创建当前完整 Saga 结构。
    ///
    /// 参数说明：`datasource` 是已注册的 PostgreSQL qualifier。
    ///
    /// 返回：表、索引与 updated-at trigger 全部可用时返回 `Ok`；任一步失败不宣称结构可用。
    pub async fn ensure_schema_for(datasource: impl AsRef<str>) -> Result<(), SagaStoreError> {
        let datasource = natx_pgsql::DatasourceRef::new(datasource).map_err(map_connection)?;
        let mut connection = natx_pgsql::conn_for(&datasource)
            .await
            .map_err(map_connection)?;
        Self::ensure_schema_on_connection(&mut connection).await
    }

    /// 业务作用：复用调用方已持有的 PostgreSQL 连接创建当前完整 Saga 结构，使 advisory
    /// lock 与全部结构操作位于同一会话。
    ///
    /// 参数说明：`connection` 是调用方已取得 schema 互斥权的连接。
    ///
    /// 返回：表、索引、函数与 trigger 达到运行合同时成功；任一步失败时拒绝宣称结构可用。
    pub async fn ensure_schema_on_connection(
        connection: &mut natx_pgsql::PgConn,
    ) -> Result<(), SagaStoreError> {
        if audit_event_table_exists(connection).await? {
            // 统一审计结构一旦存在，任何未知漂移都必须在 DDL 与回填前阻止启动；否则
            // `ON CONFLICT` 可能在失去逻辑唯一性后静默复制历史事实。
            verify_audit_schema(connection).await?;
        }
        // 自举与生产迁移必须读取同一份结构合同，避免 DDL 在两条维护路径中漂移。
        sqlx::raw_sql(CREATE_SAGA_SCHEMA_SQL)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        // 回填提交后再次读取数据库实际合同；只有列、唯一性、stream 索引、函数和 trigger
        // 均与运行时假设一致，调用方才允许开放业务路由。
        verify_audit_schema(connection).await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_saga_idx",
            &["tenant_id", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_workflow_saga_idx",
            &["tenant_id", "workflow_name", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_created_saga_idx",
            &["tenant_id", "created_at", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_workflow_created_saga_idx",
            &["tenant_id", "workflow_name", "created_at", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_status_saga_idx",
            &["tenant_id", "status", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_workflow_status_saga_idx",
            &["tenant_id", "workflow_name", "status", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_status_created_saga_idx",
            &["tenant_id", "status", "created_at", "saga_id"],
        )
        .await?;
        verify_named_index(
            connection,
            "saga_instance",
            "saga_instance_tenant_workflow_status_created_saga_idx",
            &[
                "tenant_id",
                "workflow_name",
                "status",
                "created_at",
                "saga_id",
            ],
        )
        .await?;
        verify_instance_query_statistics(connection).await?;
        populate_instance_query_statistics_if_missing(connection).await?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct AuditColumnContract {
    name: &'static str,
    udt_name: &'static str,
    length: Option<i64>,
    nullable: bool,
    identity: bool,
    default: Option<&'static str>,
}

const AUDIT_GUARD_COLUMNS: [AuditColumnContract; 2] = [
    AuditColumnContract {
        name: "saga_id",
        udt_name: "varchar",
        length: Some(256),
        nullable: false,
        identity: false,
        default: None,
    },
    AuditColumnContract {
        name: "generation",
        udt_name: "int8",
        length: None,
        nullable: false,
        identity: false,
        default: None,
    },
];

const AUDIT_EVENT_COLUMNS: [AuditColumnContract; 28] = [
    AuditColumnContract {
        name: "audit_seq",
        udt_name: "int8",
        length: None,
        nullable: false,
        identity: true,
        default: None,
    },
    pg_varchar("saga_id", 256, false),
    pg_varchar("record_kind", 16, false),
    pg_varchar("event_identity", 512, false),
    pg_varchar("event_revision", 256, false),
    pg_varchar("step_name", 128, true),
    pg_varchar("phase", 16, true),
    pg_bigint("attempt_no", true),
    pg_char("effect_id", 36, true),
    pg_char("command_id", 36, true),
    pg_varchar("attempt_status", 32, true),
    pg_varchar("outcome_event_id", 190, true),
    pg_bigint("transition_seq", true),
    pg_varchar("from_state", 32, true),
    pg_varchar("to_state", 32, true),
    pg_varchar("trigger_kind", 8, true),
    pg_varchar("trigger_id", 190, true),
    pg_bigint("definition_version", true),
    pg_bigint("control_seq", true),
    pg_varchar("operation_id", 190, true),
    pg_varchar("action", 64, true),
    pg_varchar("actor", 128, true),
    pg_varchar("reason", 512, true),
    pg_varchar("incoming_event_id", 190, true),
    pg_varchar("existing_status", 32, true),
    pg_varchar("incoming_status", 32, true),
    pg_varchar("conflict_kind", 64, true),
    AuditColumnContract {
        name: "occurred_at",
        udt_name: "timestamptz",
        length: None,
        nullable: false,
        identity: false,
        default: Some("clock_timestamp()"),
    },
];

const AUDIT_ROUTINES: [&str; 6] = [
    "nasaga_audit_attempt_insert",
    "nasaga_audit_attempt_update",
    "nasaga_audit_transition_insert",
    "nasaga_audit_control_insert",
    "nasaga_audit_management_insert",
    "nasaga_audit_conflict_insert",
];

const AUDIT_TRIGGERS: [(&str, &str, &str, i16); 6] = [
    (
        "nasaga_audit_attempt_insert",
        "saga_step_attempt",
        "nasaga_audit_attempt_insert",
        5,
    ),
    (
        "nasaga_audit_attempt_update",
        "saga_step_attempt",
        "nasaga_audit_attempt_update",
        17,
    ),
    (
        "nasaga_audit_transition_insert",
        "saga_transition",
        "nasaga_audit_transition_insert",
        5,
    ),
    (
        "nasaga_audit_control_insert",
        "saga_control_transition",
        "nasaga_audit_control_insert",
        5,
    ),
    (
        "nasaga_audit_management_insert",
        "saga_management_audit",
        "nasaga_audit_management_insert",
        5,
    ),
    (
        "nasaga_audit_conflict_insert",
        "saga_conflict_fact",
        "nasaga_audit_conflict_insert",
        5,
    ),
];

/// 业务作用：以紧凑常量表达 PostgreSQL `VARCHAR` 审计列合同，避免列清单重复填写类型属性。
///
/// 参数说明：`name` 是列名，`length` 是字符上限，`nullable` 表示是否允许空值。
///
/// 返回：无 identity 与默认值的 `VARCHAR` 列合同。
const fn pg_varchar(name: &'static str, length: i64, nullable: bool) -> AuditColumnContract {
    AuditColumnContract {
        name,
        udt_name: "varchar",
        length: Some(length),
        nullable,
        identity: false,
        default: None,
    }
}

/// 业务作用：以紧凑常量表达 PostgreSQL `CHAR` 审计列合同，保持固定长度身份字段定义一致。
///
/// 参数说明：`name` 是列名，`length` 是固定字符长度，`nullable` 表示是否允许空值。
///
/// 返回：无 identity 与默认值的 `CHAR` 列合同。
const fn pg_char(name: &'static str, length: i64, nullable: bool) -> AuditColumnContract {
    AuditColumnContract {
        name,
        udt_name: "bpchar",
        length: Some(length),
        nullable,
        identity: false,
        default: None,
    }
}

/// 业务作用：以紧凑常量表达 PostgreSQL `BIGINT` 审计列合同，保持序号与版本字段定义一致。
///
/// 参数说明：`name` 是列名，`nullable` 表示是否允许空值。
///
/// 返回：无 identity 与默认值的 `BIGINT` 列合同。
const fn pg_bigint(name: &'static str, nullable: bool) -> AuditColumnContract {
    AuditColumnContract {
        name,
        udt_name: "int8",
        length: None,
        nullable,
        identity: false,
        default: None,
    }
}

/// 业务作用：判断统一审计结构是否已经进入数据库，以区分明确支持的首次创建与存量契约复验。
///
/// 参数说明：`connection` 是执行 schema gate 的 PostgreSQL 连接。
///
/// 返回：当前 schema 存在事件表时为 `true`；目录读取失败时返回脱敏错误。
async fn audit_event_table_exists(
    connection: &mut natx_pgsql::PgConn,
) -> Result<bool, SagaStoreError> {
    sqlx::query_scalar(
        "SELECT to_regclass(format('%I.%I', current_schema(), 'saga_audit_event')) IS NOT NULL",
    )
    .fetch_one(connection.as_mut())
    .await
    .map_err(map_database)
}

/// 业务作用：复验 PostgreSQL 统一审计的完整数据库合同，确保分页唯一性与同 Saga 提交顺序门禁成立。
///
/// 参数说明：`connection` 是执行 schema gate 的 PostgreSQL 连接。
///
/// 返回：两张表、关键约束、stream 索引、六组函数及 trigger 全部匹配时成功；任何漂移均拒绝 Ready。
async fn verify_audit_schema(connection: &mut natx_pgsql::PgConn) -> Result<(), SagaStoreError> {
    verify_columns(connection, "saga_audit_stream_guard", &AUDIT_GUARD_COLUMNS).await?;
    verify_columns(connection, "saga_audit_event", &AUDIT_EVENT_COLUMNS).await?;
    verify_constraint(
        connection,
        "saga_audit_stream_guard",
        "saga_audit_stream_guard_pkey",
        "p",
        &["saga_id"],
    )
    .await?;
    verify_constraint(
        connection,
        "saga_audit_event",
        "saga_audit_event_pkey",
        "p",
        &["audit_seq"],
    )
    .await?;
    verify_constraint(
        connection,
        "saga_audit_event",
        "saga_audit_event_revision",
        "u",
        &["saga_id", "record_kind", "event_identity", "event_revision"],
    )
    .await?;
    let audit_unique_constraints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint AS constraint_contract \
         JOIN pg_class AS target ON target.oid = constraint_contract.conrelid \
         JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace \
         WHERE namespace.nspname = current_schema() AND target.relname = 'saga_audit_event' \
           AND constraint_contract.contype IN ('p', 'u')",
    )
    .fetch_one(connection.as_mut())
    .await
    .map_err(map_database)?;
    if audit_unique_constraints != 2 {
        return Err(invalid_audit_contract());
    }
    verify_named_index(
        connection,
        "saga_audit_event",
        "saga_audit_event_stream_idx",
        &["saga_id", "audit_seq"],
    )
    .await?;
    let audit_indexes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_index AS index_contract \
         JOIN pg_class AS target ON target.oid = index_contract.indrelid \
         JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace \
         WHERE namespace.nspname = current_schema() AND target.relname = 'saga_audit_event'",
    )
    .fetch_one(connection.as_mut())
    .await
    .map_err(map_database)?;
    if audit_indexes != 3 {
        return Err(invalid_audit_contract());
    }
    for name in AUDIT_ROUTINES {
        verify_routine(connection, name).await?;
    }
    for (name, table, routine, trigger_type) in AUDIT_TRIGGERS {
        verify_trigger(connection, name, table, routine, trigger_type).await?;
    }
    Ok(())
}

/// 业务作用：逐列核对审计表的顺序、类型、长度、可空性、identity 与默认值。
///
/// 参数说明：`connection` 是目录读取连接；`table` 是目标表；`expected` 是完整列合同。
///
/// 返回：实际列与合同完全一致时成功；缺列、增列或属性漂移时返回稳定错误。
async fn verify_columns(
    connection: &mut natx_pgsql::PgConn,
    table: &str,
    expected: &[AuditColumnContract],
) -> Result<(), SagaStoreError> {
    let rows = sqlx::query(
        "SELECT column_name, udt_name, character_maximum_length, is_nullable, column_default, \
                is_identity, identity_generation \
         FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.len() != expected.len() {
        return Err(invalid_audit_contract());
    }
    for (row, contract) in rows.iter().zip(expected) {
        let name: String = row.try_get("column_name").map_err(map_database)?;
        let udt_name: String = row.try_get("udt_name").map_err(map_database)?;
        let length: Option<i32> = row
            .try_get("character_maximum_length")
            .map_err(map_database)?;
        let nullable: String = row.try_get("is_nullable").map_err(map_database)?;
        let default: Option<String> = row.try_get("column_default").map_err(map_database)?;
        let identity: String = row.try_get("is_identity").map_err(map_database)?;
        let identity_generation: Option<String> =
            row.try_get("identity_generation").map_err(map_database)?;
        if name != contract.name
            || udt_name != contract.udt_name
            || length.map(i64::from) != contract.length
            || (nullable == "YES") != contract.nullable
            || default.as_deref() != contract.default
            || (identity == "YES") != contract.identity
            || identity_generation.as_deref() != contract.identity.then_some("BY DEFAULT")
        {
            return Err(invalid_audit_contract());
        }
    }
    Ok(())
}

/// 业务作用：核对命名主键或唯一约束的类别、有效状态与列顺序，避免同名对象掩盖逻辑唯一性漂移。
///
/// 参数说明：连接与表定位实际目录对象，`name`、`kind`、`columns` 描述完整约束合同。
///
/// 返回：唯一一个已验证约束完全匹配时成功；不存在、重复或定义漂移时返回稳定错误。
async fn verify_constraint(
    connection: &mut natx_pgsql::PgConn,
    table: &str,
    name: &str,
    kind: &str,
    columns: &[&str],
) -> Result<(), SagaStoreError> {
    let rows = sqlx::query(
        "SELECT constraint_contract.contype::TEXT AS constraint_type, \
                constraint_contract.convalidated, \
                string_agg(attribute.attname, ',' ORDER BY key_column.ordinality) AS columns \
         FROM pg_constraint AS constraint_contract \
         JOIN pg_class AS target ON target.oid = constraint_contract.conrelid \
         JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace \
         CROSS JOIN LATERAL unnest(constraint_contract.conkey) WITH ORDINALITY \
             AS key_column(attnum, ordinality) \
         JOIN pg_attribute AS attribute ON attribute.attrelid = target.oid \
             AND attribute.attnum = key_column.attnum \
         WHERE namespace.nspname = current_schema() AND target.relname = $1 \
             AND constraint_contract.conname = $2 \
         GROUP BY constraint_contract.oid, constraint_contract.contype, \
             constraint_contract.convalidated",
    )
    .bind(table)
    .bind(name)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.len() != 1 {
        return Err(invalid_audit_contract());
    }
    let actual_kind: String = rows[0].try_get("constraint_type").map_err(map_database)?;
    let validated: bool = rows[0].try_get("convalidated").map_err(map_database)?;
    let actual_columns: String = rows[0].try_get("columns").map_err(map_database)?;
    if actual_kind != kind || !validated || actual_columns != columns.join(",") {
        return Err(invalid_audit_contract());
    }
    Ok(())
}

/// 业务作用：核对服务 keyset 或审计 stream 的命名 B-tree 索引，保证前导列和可用状态满足查询合同。
///
/// 参数说明：连接与表定位目录对象，`name` 是索引名称，`columns` 是完整 key 列顺序。
///
/// 返回：唯一一个普通、无表达式、无谓词、已就绪索引匹配时成功；其它结构返回稳定错误。
async fn verify_named_index(
    connection: &mut natx_pgsql::PgConn,
    table: &str,
    name: &str,
    columns: &[&str],
) -> Result<(), SagaStoreError> {
    let rows = sqlx::query(
        "SELECT index_contract.indisunique, index_contract.indisvalid, index_contract.indisready, \
                index_contract.indnkeyatts, index_contract.indnatts, access_method.amname, \
                pg_get_expr(index_contract.indpred, index_contract.indrelid) AS predicate, \
                pg_get_expr(index_contract.indexprs, index_contract.indrelid) AS expressions, \
                string_agg(attribute.attname, ',' ORDER BY key_column.ordinality) AS columns \
         FROM pg_index AS index_contract \
         JOIN pg_class AS target ON target.oid = index_contract.indrelid \
         JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace \
         JOIN pg_class AS index_object ON index_object.oid = index_contract.indexrelid \
         JOIN pg_am AS access_method ON access_method.oid = index_object.relam \
         CROSS JOIN LATERAL unnest(index_contract.indkey) WITH ORDINALITY \
             AS key_column(attnum, ordinality) \
         JOIN pg_attribute AS attribute ON attribute.attrelid = target.oid \
             AND attribute.attnum = key_column.attnum \
         WHERE namespace.nspname = current_schema() AND target.relname = $1 \
             AND index_object.relname = $2 \
         GROUP BY index_contract.indexrelid, index_contract.indisunique, \
             index_contract.indisvalid, index_contract.indisready, index_contract.indnkeyatts, \
             index_contract.indnatts, access_method.amname, index_contract.indpred, \
             index_contract.indrelid, index_contract.indexprs",
    )
    .bind(table)
    .bind(name)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.len() != 1 {
        return Err(invalid_audit_contract());
    }
    let row = &rows[0];
    let unique: bool = row.try_get("indisunique").map_err(map_database)?;
    let valid: bool = row.try_get("indisvalid").map_err(map_database)?;
    let ready: bool = row.try_get("indisready").map_err(map_database)?;
    let key_count: i16 = row.try_get("indnkeyatts").map_err(map_database)?;
    let total_count: i16 = row.try_get("indnatts").map_err(map_database)?;
    let method: String = row.try_get("amname").map_err(map_database)?;
    let predicate: Option<String> = row.try_get("predicate").map_err(map_database)?;
    let expressions: Option<String> = row.try_get("expressions").map_err(map_database)?;
    let actual_columns: String = row.try_get("columns").map_err(map_database)?;
    if unique
        || !valid
        || !ready
        || i32::from(key_count) != columns.len() as i32
        || key_count != total_count
        || method != "btree"
        || predicate.is_some()
        || expressions.is_some()
        || actual_columns != columns.join(",")
    {
        return Err(invalid_audit_contract());
    }
    Ok(())
}

/// 业务作用：核对实例状态查询的跨列统计合同，使 planner 能识别 tenant、workflow 与 status 的相关分布。
///
/// 参数说明：`connection` 是执行 Ready schema gate 的 PostgreSQL 连接。
///
/// 返回：两列与三列统计对象均覆盖精确列序且同时启用 MCV 与 dependencies 时成功；
/// 缺失或同名漂移时返回稳定错误。
async fn verify_instance_query_statistics(
    connection: &mut natx_pgsql::PgConn,
) -> Result<(), SagaStoreError> {
    let rows = sqlx::query(
        "SELECT statistics_contract.stxname AS statistics_name, \
                cardinality(statistics_contract.stxkind)::INTEGER AS kind_count, \
                'm'::\"char\" = ANY(statistics_contract.stxkind) AS has_mcv, \
                'f'::\"char\" = ANY(statistics_contract.stxkind) AS has_dependencies, \
                string_agg(attribute.attname, ',' ORDER BY key_column.ordinality) AS columns \
         FROM pg_statistic_ext AS statistics_contract \
         JOIN pg_class AS target ON target.oid = statistics_contract.stxrelid \
         JOIN pg_namespace AS namespace ON namespace.oid = statistics_contract.stxnamespace \
         CROSS JOIN LATERAL unnest(statistics_contract.stxkeys) WITH ORDINALITY \
             AS key_column(attnum, ordinality) \
         JOIN pg_attribute AS attribute ON attribute.attrelid = target.oid \
             AND attribute.attnum = key_column.attnum \
         WHERE namespace.nspname = current_schema() AND target.relname = 'saga_instance' \
             AND statistics_contract.stxname IN \
                 ('saga_instance_tenant_status_stats', \
                  'saga_instance_tenant_workflow_status_stats') \
         GROUP BY statistics_contract.oid, statistics_contract.stxkind",
    )
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.len() != 2 {
        return Err(invalid_instance_query_contract());
    }
    let expected = [
        ("saga_instance_tenant_status_stats", "tenant_id,status"),
        (
            "saga_instance_tenant_workflow_status_stats",
            "tenant_id,workflow_name,status",
        ),
    ]
    .into_iter()
    .collect::<std::collections::BTreeMap<_, _>>();
    for row in rows {
        let name: String = row.try_get("statistics_name").map_err(map_database)?;
        let kind_count: i32 = row.try_get("kind_count").map_err(map_database)?;
        let has_mcv: bool = row.try_get("has_mcv").map_err(map_database)?;
        let has_dependencies: bool = row.try_get("has_dependencies").map_err(map_database)?;
        let columns: String = row.try_get("columns").map_err(map_database)?;
        if kind_count != 2
            || !has_mcv
            || !has_dependencies
            || expected.get(name.as_str()).copied() != Some(columns.as_str())
        {
            return Err(invalid_instance_query_contract());
        }
    }
    Ok(())
}

/// 业务作用：在首次引入跨列统计时通过 owner 可读视图判断数据是否生成，避免等待 autovacuum
/// 期间 planner 仍按 tenant、workflow 与 status 独立分布估算管理查询。
///
/// 参数说明：`connection` 是持有 schema 自举权的 PostgreSQL 会话。
///
/// 返回：两个统计对象均已有公开数据行时不重复扫描；缺失时完成目标列 ANALYZE，数据库拒绝时返回错误。
async fn populate_instance_query_statistics_if_missing(
    connection: &mut natx_pgsql::PgConn,
) -> Result<(), SagaStoreError> {
    let populated: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_stats_ext \
         WHERE schemaname = current_schema() AND tablename = 'saga_instance' \
         AND statistics_name IN ('saga_instance_tenant_status_stats', \
                                 'saga_instance_tenant_workflow_status_stats')",
    )
    .fetch_one(connection.as_mut())
    .await
    .map_err(map_database)?;
    if populated != 2 {
        // 公开统计视图对普通 database owner 可读；目标列 ANALYZE 会同时填充两列和三列对象。
        sqlx::query("ANALYZE saga_instance (tenant_id, workflow_name, status)")
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
    }
    Ok(())
}

/// 业务作用：核对审计 trigger function 的执行体与安全属性，禁止采用同名不同义的数据库代码。
///
/// 参数说明：`connection` 是目录读取连接；`name` 是零参数 trigger function 名称。
///
/// 返回：当前 schema 中唯一函数与迁移合同一致时成功；缺失、重载或定义漂移时返回稳定错误。
async fn verify_routine(
    connection: &mut natx_pgsql::PgConn,
    name: &str,
) -> Result<(), SagaStoreError> {
    let rows = sqlx::query(
        "SELECT routine.prosrc, routine.provolatile::TEXT AS volatility, routine.prosecdef, \
                routine.proleakproof, routine.prokind::TEXT AS routine_kind, \
                pg_get_function_result(routine.oid) AS result, language.lanname \
         FROM pg_proc AS routine \
         JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace \
         JOIN pg_language AS language ON language.oid = routine.prolang \
         WHERE namespace.nspname = current_schema() AND routine.proname = $1 \
             AND routine.pronargs = 0",
    )
    .bind(name)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.len() != 1 {
        return Err(invalid_audit_contract());
    }
    let row = &rows[0];
    let body: String = row.try_get("prosrc").map_err(map_database)?;
    let volatility: String = row.try_get("volatility").map_err(map_database)?;
    let security_definer: bool = row.try_get("prosecdef").map_err(map_database)?;
    let leakproof: bool = row.try_get("proleakproof").map_err(map_database)?;
    let routine_kind: String = row.try_get("routine_kind").map_err(map_database)?;
    let result: String = row.try_get("result").map_err(map_database)?;
    let language: String = row.try_get("lanname").map_err(map_database)?;
    let expected = expected_routine_body(name).ok_or_else(invalid_audit_contract)?;
    if normalize_routine_body(&body) != normalize_routine_body(expected)
        || volatility != "v"
        || security_definer
        || leakproof
        || routine_kind != "f"
        || result != "trigger"
        || language != "plpgsql"
    {
        return Err(invalid_audit_contract());
    }
    Ok(())
}

/// 业务作用：核对 trigger 的目标表、事件时机、行级属性、启用状态与执行函数。
///
/// 参数说明：连接与名称定位 trigger，`table`、`routine`、`trigger_type` 描述完整触发合同。
///
/// 返回：唯一 trigger 完全匹配时成功；禁用、带条件或同名不同义时返回稳定错误。
async fn verify_trigger(
    connection: &mut natx_pgsql::PgConn,
    name: &str,
    table: &str,
    routine: &str,
    trigger_type: i16,
) -> Result<(), SagaStoreError> {
    let rows = sqlx::query(
        "SELECT trigger_contract.tgtype, trigger_contract.tgenabled::TEXT AS enabled, \
                trigger_contract.tgisinternal, \
                pg_get_expr(trigger_contract.tgqual, trigger_contract.tgrelid) AS condition, \
                function_contract.proname AS routine_name \
         FROM pg_trigger AS trigger_contract \
         JOIN pg_class AS target ON target.oid = trigger_contract.tgrelid \
         JOIN pg_namespace AS namespace ON namespace.oid = target.relnamespace \
         JOIN pg_proc AS function_contract ON function_contract.oid = trigger_contract.tgfoid \
         WHERE namespace.nspname = current_schema() AND target.relname = $1 \
             AND trigger_contract.tgname = $2",
    )
    .bind(table)
    .bind(name)
    .fetch_all(connection.as_mut())
    .await
    .map_err(map_database)?;
    if rows.len() != 1 {
        return Err(invalid_audit_contract());
    }
    let actual_type: i16 = rows[0].try_get("tgtype").map_err(map_database)?;
    let enabled: String = rows[0].try_get("enabled").map_err(map_database)?;
    let internal: bool = rows[0].try_get("tgisinternal").map_err(map_database)?;
    let condition: Option<String> = rows[0].try_get("condition").map_err(map_database)?;
    let actual_routine: String = rows[0].try_get("routine_name").map_err(map_database)?;
    if actual_type != trigger_type
        || enabled != "O"
        || internal
        || condition.is_some()
        || actual_routine != routine
    {
        return Err(invalid_audit_contract());
    }
    Ok(())
}

/// 业务作用：从生产迁移合同提取指定函数体，使受控自举与正式迁移共享单一数据库代码来源。
///
/// 参数说明：`name` 是审计函数的稳定名称。
///
/// 返回：迁移中存在完整函数体时返回其正文；合同缺失或边界不完整时返回 `None`。
fn expected_routine_body(name: &str) -> Option<&'static str> {
    let marker = format!("CREATE OR REPLACE FUNCTION {name}()");
    let (_, definition) = CREATE_SAGA_SCHEMA_SQL.split_once(&marker)?;
    let (_, body) = definition.split_once("AS $$")?;
    body.split_once("\n$$;").map(|(body, _)| body)
}

/// 业务作用：消除数据库目录与迁移文件之间无业务含义的空白差异，同时保留标识符和字面量大小写。
///
/// 参数说明：`body` 是函数正文。
///
/// 返回：以单空格连接的稳定比较文本。
fn normalize_routine_body(body: &str) -> String {
    body.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// 业务作用：生成不暴露数据库细节的统一审计契约错误，供启动门禁稳定分类。
///
/// 参数说明: 无。
///
/// 返回：说明审计数据库合同不可采用的存储错误。
fn invalid_audit_contract() -> SagaStoreError {
    SagaStoreError::new("PostgreSQL Saga audit schema contract is invalid")
}

/// 业务作用：生成不暴露数据库细节的实例管理查询结构错误，供启动门禁稳定分类。
///
/// 参数说明: 无。
///
/// 返回：说明 tenant/status 索引或统计合同不可采用的存储错误。
fn invalid_instance_query_contract() -> SagaStoreError {
    SagaStoreError::new("PostgreSQL Saga instance query schema contract is invalid")
}
