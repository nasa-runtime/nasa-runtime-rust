//! MySQL Definition Catalog 与参与方 capability 的共享持久化实现。

use nasaga_core::{ServiceIdentity, TenantId};
use nasaga_runtime_core::{
    validate_capability_route_contract, validate_redis_stream_route, CapabilityDescriptor,
    CapabilityReceipt, DefinitionActivationGate, DefinitionArtifact, DefinitionCatalogError,
    DefinitionLifecycle, DefinitionLifecycleOperation, DefinitionPublishDisposition,
    DefinitionRecord, DefinitionRegistry, DynamicCatalogSnapshot, RegisteredCapability,
    CATALOG_OBJECT_KEY_MAX_LEN,
};
use sqlx::{Connection as _, Row as _};

const MIN_CAPABILITY_LEASE_MS: u64 = 5_000;
const MAX_CAPABILITY_LEASE_MS: u64 = 120_000;

/// 业务作用：在已持有 schema 串行权威的连接上建立 Catalog、capability、generation 与审计结构。
///
/// 参数说明：`connection` 是 Orchestrator schema coordinator 持有的同一 MySQL session。
///
/// 返回：全部控制面结构可用时成功；DDL 权限或结构冲突返回错误。
pub(crate) async fn ensure_schema(connection: &mut natx::Conn) -> anyhow::Result<()> {
    migrate_predecessor_schema(connection).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nasa_saga_catalog_state (\
             singleton TINYINT NOT NULL PRIMARY KEY,\
             generation BIGINT UNSIGNED NOT NULL,\
             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\
             CONSTRAINT chk_nasa_saga_catalog_singleton CHECK (singleton = 1)\
         ) ENGINE=InnoDB",
    )
    .execute(connection.as_mut())
    .await?;
    sqlx::query("INSERT IGNORE INTO nasa_saga_catalog_state (singleton, generation) VALUES (1, 0)")
        .execute(connection.as_mut())
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nasa_saga_definition_catalog (\
             tenant VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\
             workflow VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             definition_version INT UNSIGNED NOT NULL,\
             workflow_owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             definition_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             artifact_json JSON NOT NULL,\
             lifecycle VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             catalog_generation BIGINT UNSIGNED NOT NULL,\
             activated_at TIMESTAMP(6) NULL,\
             created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\
             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\
             PRIMARY KEY (tenant, workflow, definition_version),\
             INDEX idx_nasa_saga_definition_lifecycle (lifecycle, catalog_generation),\
             CONSTRAINT chk_nasa_saga_definition_version CHECK (definition_version > 0),\
             CONSTRAINT chk_nasa_saga_definition_lifecycle CHECK (lifecycle IN ('candidate','active','deprecated','retired'))\
         ) ENGINE=InnoDB",
    )
    .execute(connection.as_mut())
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nasa_saga_capability_registry (\
             tenant VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\
             owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             replica_identity VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             workflow VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             definition_version INT UNSIGNED NOT NULL,\
             step VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             capability_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             descriptor_json JSON NOT NULL,\
             route_generation BIGINT UNSIGNED NOT NULL,\
             accepted_until_ms BIGINT NOT NULL,\
             catalog_generation BIGINT UNSIGNED NOT NULL,\
             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\
             PRIMARY KEY (tenant, owner, replica_identity, workflow, definition_version, step),\
             INDEX idx_nasa_saga_capability_lookup (tenant, workflow, definition_version, step, accepted_until_ms),\
             INDEX idx_nasa_saga_capability_lease (accepted_until_ms),\
             CONSTRAINT chk_nasa_saga_capability_version CHECK (definition_version > 0),\
             CONSTRAINT chk_nasa_saga_capability_route_generation CHECK (route_generation > 0),\
             CONSTRAINT chk_nasa_saga_capability_lease CHECK (accepted_until_ms >= 0)\
         ) ENGINE=InnoDB",
    )
    .execute(connection.as_mut())
    .await?;
    let create_audit = format!(
        "CREATE TABLE IF NOT EXISTS nasa_saga_catalog_audit (\
             id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\
             catalog_generation BIGINT UNSIGNED NOT NULL,\
             actor VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             action VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             object_key VARCHAR({CATALOG_OBJECT_KEY_MAX_LEN}) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\
             object_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             operation_id VARCHAR(190) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,\
             reason VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,\
             created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\
             INDEX idx_nasa_saga_catalog_audit_generation (catalog_generation, id),\
             UNIQUE INDEX uk_nasa_saga_catalog_audit_operation (actor, action, operation_id)\
         ) ENGINE=InnoDB"
    );
    sqlx::query(sqlx::AssertSqlSafe(create_audit))
        .execute(connection.as_mut())
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nasa_saga_catalog_replica (\
             service_identity VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             replica_identity VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             snapshot_generation BIGINT UNSIGNED NOT NULL,\
             snapshot_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             activation_contract_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,\
             ready TINYINT(1) NOT NULL,\
             lease_until_ms BIGINT NOT NULL,\
             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\
             PRIMARY KEY (service_identity, replica_identity),\
             INDEX idx_nasa_saga_catalog_replica_lease (service_identity, ready, lease_until_ms),\
             CONSTRAINT chk_nasa_saga_catalog_replica_generation CHECK (snapshot_generation >= 0),\
             CONSTRAINT chk_nasa_saga_catalog_replica_lease CHECK (lease_until_ms >= 0)\
         ) ENGINE=InnoDB",
    )
    .execute(connection.as_mut())
    .await?;
    verify_schema(connection).await?;
    Ok(())
}

/// 业务作用：在创建当前对象前把可识别的直接前驱 Catalog 有序迁移到 tenant、审计和文本合同。
///
/// 参数说明：`connection` 是持有 database 级自举锁的同一 MySQL session。
///
/// 返回：空库或当前结构直接成功；前驱结构安全回填后成功；租户歧义、数据不一致或 DDL 失败时拒绝 Ready。
async fn migrate_predecessor_schema(connection: &mut natx::Conn) -> anyhow::Result<()> {
    if mysql_table_exists(connection, "nasa_saga_definition_catalog").await? {
        if !mysql_column_exists(connection, "nasa_saga_definition_catalog", "activated_at").await? {
            sqlx::query(
                "ALTER TABLE nasa_saga_definition_catalog ADD COLUMN activated_at TIMESTAMP(6) NULL \
                 AFTER catalog_generation",
            )
            .execute(connection.as_mut())
            .await?;
        }
        // MySQL DDL 会独立提交，因此回填不能依赖“本次刚加列”；每次启动都让已识别
        // 中间态继续收敛，避免进程退出后把非 candidate 永久留成无激活时间。
        sqlx::query(
            "UPDATE nasa_saga_definition_catalog SET activated_at = updated_at \
             WHERE lifecycle <> 'candidate' AND activated_at IS NULL",
        )
        .execute(connection.as_mut())
        .await?;
    }

    migrate_mysql_capability_tenant(connection).await?;

    if mysql_table_exists(connection, "nasa_saga_catalog_audit").await? {
        let object_key_predecessor: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() \
             AND table_name = 'nasa_saga_catalog_audit' AND column_name = 'object_key' \
             AND column_type = 'varchar(640)' AND is_nullable = 'NO' \
             AND collation_name = 'utf8mb4_bin'",
        )
        .fetch_one(connection.as_mut())
        .await?;
        if object_key_predecessor == 1 {
            // 该列只保存由合法身份拼成的审计键；容量按同一身份上限公式扩展，不截断既有事实。
            let alter_object_key = format!(
                "ALTER TABLE nasa_saga_catalog_audit MODIFY COLUMN object_key \
                 VARCHAR({CATALOG_OBJECT_KEY_MAX_LEN}) CHARACTER SET utf8mb4 \
                 COLLATE utf8mb4_bin NOT NULL"
            );
            sqlx::query(sqlx::AssertSqlSafe(alter_object_key))
                .execute(connection.as_mut())
                .await?;
        }
        let missing_operation =
            !mysql_column_exists(connection, "nasa_saga_catalog_audit", "operation_id").await?;
        let missing_reason =
            !mysql_column_exists(connection, "nasa_saga_catalog_audit", "reason").await?;
        if missing_operation {
            sqlx::query(
                "ALTER TABLE nasa_saga_catalog_audit ADD COLUMN operation_id VARCHAR(190) \
                 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL AFTER object_digest",
            )
            .execute(connection.as_mut())
            .await?;
        }
        if missing_reason {
            sqlx::query(
                "ALTER TABLE nasa_saga_catalog_audit ADD COLUMN reason VARCHAR(512) \
                 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL AFTER operation_id",
            )
            .execute(connection.as_mut())
            .await?;
        }
        // 加列与建索引之间同样可能中断；索引目标必须按自身结构事实独立确保。
        if !mysql_index_exists(
            connection,
            "nasa_saga_catalog_audit",
            "uk_nasa_saga_catalog_audit_operation",
        )
        .await?
        {
            sqlx::query(
                "ALTER TABLE nasa_saga_catalog_audit ADD UNIQUE INDEX \
                 uk_nasa_saga_catalog_audit_operation (actor, action, operation_id)",
            )
            .execute(connection.as_mut())
            .await?;
        }
    }
    migrate_mysql_catalog_replica_contract(connection).await?;
    Ok(())
}

/// 业务作用：把未记录运行时路由合同的 Catalog 副本表收敛到当前确认语义。
///
/// 参数说明：`connection` 是持有 database 级自举锁的同一 MySQL session。
///
/// 返回：当前表或空库直接成功；前驱确认全部撤销并补齐非空摘要列后成功；DDL 失败时拒绝 Ready。
async fn migrate_mysql_catalog_replica_contract(connection: &mut natx::Conn) -> anyhow::Result<()> {
    if !mysql_table_exists(connection, "nasa_saga_catalog_replica").await? {
        return Ok(());
    }
    if !mysql_column_exists(
        connection,
        "nasa_saga_catalog_replica",
        "activation_contract_digest",
    )
    .await?
    {
        sqlx::query(
            "ALTER TABLE nasa_saga_catalog_replica ADD COLUMN activation_contract_digest \
             CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER snapshot_digest",
        )
        .execute(connection.as_mut())
        .await?;
    }
    let nullable: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() \
         AND table_name = 'nasa_saga_catalog_replica' \
         AND column_name = 'activation_contract_digest'",
    )
    .fetch_one(connection.as_mut())
    .await?;
    if nullable == "YES" {
        // 前驱确认没有证明各副本使用相同发布边界，必须先撤销 Ready，再补入不可命中的占位摘要。
        sqlx::query(
            "UPDATE nasa_saga_catalog_replica SET activation_contract_digest = REPEAT('0', 64), \
             ready = 0, lease_until_ms = 0 WHERE activation_contract_digest IS NULL",
        )
        .execute(connection.as_mut())
        .await?;
        sqlx::query(
            "ALTER TABLE nasa_saga_catalog_replica MODIFY COLUMN activation_contract_digest \
             CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
        )
        .execute(connection.as_mut())
        .await?;
    }
    Ok(())
}

/// 业务作用：把无 tenant 的直接前驱 capability 安全绑定到唯一 definition 租户并重算持久摘要。
///
/// 参数说明：`connection` 是 schema 锁内连接。
///
/// 返回：租户可唯一推导时完成列、JSON、摘要、主键和查询索引迁移；歧义时拒绝 Ready。
async fn migrate_mysql_capability_tenant(connection: &mut natx::Conn) -> anyhow::Result<()> {
    if !mysql_table_exists(connection, "nasa_saga_capability_registry").await? {
        return Ok(());
    }
    if !mysql_column_exists(connection, "nasa_saga_capability_registry", "tenant").await? {
        sqlx::query(
            "ALTER TABLE nasa_saga_capability_registry ADD COLUMN tenant VARCHAR(256) \
             CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL FIRST",
        )
        .execute(connection.as_mut())
        .await?;
    }
    let tenant_nullable: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() \
         AND table_name = 'nasa_saga_capability_registry' AND column_name = 'tenant'",
    )
    .fetch_one(connection.as_mut())
    .await?;
    if tenant_nullable == "YES" {
        let rows = sqlx::query(
            "SELECT tenant, owner, replica_identity, workflow, definition_version, step, \
             descriptor_json FROM nasa_saga_capability_registry ORDER BY owner, replica_identity, \
             workflow, definition_version, step",
        )
        .fetch_all(connection.as_mut())
        .await?;
        for row in rows {
            let owner: String = row.try_get("owner")?;
            let replica: String = row.try_get("replica_identity")?;
            let workflow: String = row.try_get("workflow")?;
            let version: u32 = row.try_get("definition_version")?;
            let step: String = row.try_get("step")?;
            let tenant = match row.try_get::<Option<String>, _>("tenant")? {
                Some(tenant) => tenant,
                None => {
                    let tenants: Vec<String> = sqlx::query_scalar(
                        "SELECT tenant FROM nasa_saga_definition_catalog WHERE workflow = ? \
                         AND definition_version = ? ORDER BY tenant",
                    )
                    .bind(&workflow)
                    .bind(version)
                    .fetch_all(connection.as_mut())
                    .await?;
                    anyhow::ensure!(
                        tenants.len() == 1,
                        "Saga Catalog predecessor capability tenant cannot be inferred uniquely"
                    );
                    tenants.into_iter().next().expect("one tenant was verified")
                }
            };
            let mut document: serde_json::Value = row.try_get("descriptor_json")?;
            let object = document.as_object_mut().ok_or_else(|| {
                anyhow::anyhow!("Saga Catalog predecessor capability document is invalid")
            })?;
            object.insert(
                "tenant".to_owned(),
                serde_json::Value::String(tenant.clone()),
            );
            let descriptor: CapabilityDescriptor = serde_json::from_value(document.clone())?;
            anyhow::ensure!(
                descriptor.owner == owner
                    && descriptor.replica_identity == replica
                    && descriptor.workflow == workflow
                    && descriptor.definition_version == version
                    && descriptor.step == step
                    && descriptor.tenant == tenant,
                "Saga Catalog predecessor capability identity is inconsistent"
            );
            let digest = descriptor.digest();
            sqlx::query(
                "UPDATE nasa_saga_capability_registry SET tenant = ?, descriptor_json = ?, \
                 capability_digest = ? WHERE owner = ? AND replica_identity = ? AND workflow = ? \
                 AND definition_version = ? AND step = ?",
            )
            .bind(&tenant)
            .bind(document)
            .bind(digest)
            .bind(&owner)
            .bind(&replica)
            .bind(&workflow)
            .bind(version)
            .bind(&step)
            .execute(connection.as_mut())
            .await?;
        }
        sqlx::query(
            "ALTER TABLE nasa_saga_capability_registry MODIFY COLUMN tenant VARCHAR(256) \
             CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
        )
        .execute(connection.as_mut())
        .await?;
    }
    let primary_columns =
        mysql_index_columns(connection, "nasa_saga_capability_registry", "PRIMARY").await?;
    let current_primary = "tenant,owner,replica_identity,workflow,definition_version,step";
    if primary_columns == "owner,replica_identity,workflow,definition_version,step" {
        // tenant 已完成回填后在单条 DDL 中替换直接前驱主键，减少可观察中间态。
        sqlx::query(
            "ALTER TABLE nasa_saga_capability_registry DROP PRIMARY KEY, ADD PRIMARY KEY \
             (tenant, owner, replica_identity, workflow, definition_version, step)",
        )
        .execute(connection.as_mut())
        .await?;
    } else if primary_columns.is_empty() {
        // DROP 已提交而 ADD 未执行时主键为空；tenant 已是 NOT NULL，直接补齐当前键即可续跑。
        sqlx::query(
            "ALTER TABLE nasa_saga_capability_registry ADD PRIMARY KEY \
             (tenant, owner, replica_identity, workflow, definition_version, step)",
        )
        .execute(connection.as_mut())
        .await?;
    } else if primary_columns != current_primary {
        // 未识别键不在迁移白名单内，保留原结构并交给最终门禁拒绝 Ready。
    }
    let lookup = mysql_index_columns(
        connection,
        "nasa_saga_capability_registry",
        "idx_nasa_saga_capability_lookup",
    )
    .await?;
    if lookup == "workflow,definition_version,step,accepted_until_ms" {
        sqlx::query(
            "ALTER TABLE nasa_saga_capability_registry \
             DROP INDEX idx_nasa_saga_capability_lookup, \
             ADD INDEX idx_nasa_saga_capability_lookup \
             (tenant, workflow, definition_version, step, accepted_until_ms)",
        )
        .execute(connection.as_mut())
        .await?;
    }
    Ok(())
}

/// 业务作用：判断当前 database 是否存在指定 Catalog 普通表。
///
/// 参数说明：连接与表名定位当前 database 对象。
///
/// 返回：存在时为真；查询失败返回错误。
async fn mysql_table_exists(connection: &mut natx::Conn, table: &str) -> anyhow::Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() \
         AND table_name = ? AND table_type = 'BASE TABLE'",
    )
    .bind(table)
    .fetch_one(connection.as_mut())
    .await?;
    Ok(count == 1)
}

/// 业务作用：判断目标表是否已有指定列，支持 MySQL 隐式提交后的迁移续跑。
///
/// 参数说明：连接、表和列定位结构事实。
///
/// 返回：列存在时为真；查询失败返回错误。
async fn mysql_column_exists(
    connection: &mut natx::Conn,
    table: &str,
    column: &str,
) -> anyhow::Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() \
         AND table_name = ? AND column_name = ?",
    )
    .bind(table)
    .bind(column)
    .fetch_one(connection.as_mut())
    .await?;
    Ok(count == 1)
}

/// 业务作用：判断目标表是否已有命名索引，避免重复 DDL。
///
/// 参数说明：连接、表与索引名定位结构事实。
///
/// 返回：索引存在时为真；查询失败返回错误。
async fn mysql_index_exists(
    connection: &mut natx::Conn,
    table: &str,
    index: &str,
) -> anyhow::Result<bool> {
    Ok(!mysql_index_columns(connection, table, index)
        .await?
        .is_empty())
}

/// 业务作用：读取 MySQL 命名索引的完整列序，供迁移和最终合同使用同一事实来源。
///
/// 参数说明：连接、表与索引名定位当前 database 索引。
///
/// 返回：逗号分隔列序；索引不存在返回空文本。
async fn mysql_index_columns(
    connection: &mut natx::Conn,
    table: &str,
    index: &str,
) -> anyhow::Result<String> {
    let columns: Option<String> = sqlx::query_scalar(
        "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') \
         FROM information_schema.statistics WHERE table_schema = DATABASE() \
         AND table_name = ? AND index_name = ?",
    )
    .bind(table)
    .bind(index)
    .fetch_one(connection.as_mut())
    .await?;
    Ok(columns.unwrap_or_default())
}

/// 业务作用：复验 Catalog 全部表、列、collation、索引、唯一性与 CHECK 合同，结构漂移时拒绝 Ready。
///
/// 参数说明：`connection` 是仍持有 schema 串行权威且绑定目标 database 的同一会话。
///
/// 返回：服务端最终结构与运行期 SQL 假设完全一致时成功；任一缺失或类型漂移返回错误。
async fn verify_schema(connection: &mut natx::Conn) -> anyhow::Result<()> {
    let database: Option<String> = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(connection.as_mut())
        .await?;
    anyhow::ensure!(
        database.is_some_and(|value| !value.is_empty()),
        "Saga Catalog database identity is unavailable"
    );
    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() \
         AND table_name IN ('nasa_saga_catalog_state','nasa_saga_definition_catalog',\
         'nasa_saga_capability_registry','nasa_saga_catalog_audit','nasa_saga_catalog_replica') \
         AND engine = 'InnoDB' AND table_collation = @@collation_database",
    )
    .fetch_one(connection.as_mut())
    .await?;
    anyhow::ensure!(
        table_count == 5,
        "Saga Catalog table identity, engine or collation is invalid"
    );
    let rows = sqlx::query(
        "SELECT table_name AS checked_table, column_name AS checked_column, \
         column_type AS checked_type, is_nullable AS checked_nullable, \
         collation_name AS checked_collation \
         FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name IN \
         ('nasa_saga_catalog_state','nasa_saga_definition_catalog','nasa_saga_capability_registry',\
          'nasa_saga_catalog_audit','nasa_saga_catalog_replica')",
    )
    .fetch_all(connection.as_mut())
    .await?;
    let mut columns = std::collections::BTreeMap::new();
    for row in rows {
        columns.insert(
            (
                row.try_get::<String, _>("checked_table")?,
                row.try_get::<String, _>("checked_column")?,
            ),
            (
                row.try_get::<String, _>("checked_type")?
                    .to_ascii_lowercase(),
                row.try_get::<String, _>("checked_nullable")? == "YES",
                row.try_get::<Option<String>, _>("checked_collation")?,
            ),
        );
    }
    let audit_object_key_type = format!("varchar({CATALOG_OBJECT_KEY_MAX_LEN})");
    let expected: [(&str, &str, &str, bool, Option<&str>); 43] = [
        (
            "nasa_saga_catalog_state",
            "singleton",
            "tinyint",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_state",
            "generation",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_state",
            "updated_at",
            "timestamp(6)",
            false,
            None,
        ),
        (
            "nasa_saga_definition_catalog",
            "tenant",
            "varchar(256)",
            false,
            Some("utf8mb4_bin"),
        ),
        (
            "nasa_saga_definition_catalog",
            "workflow",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_definition_catalog",
            "definition_version",
            "int unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_definition_catalog",
            "workflow_owner",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_definition_catalog",
            "definition_digest",
            "char(64)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_definition_catalog",
            "artifact_json",
            "json",
            false,
            None,
        ),
        (
            "nasa_saga_definition_catalog",
            "lifecycle",
            "varchar(16)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_definition_catalog",
            "catalog_generation",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_definition_catalog",
            "activated_at",
            "timestamp(6)",
            true,
            None,
        ),
        (
            "nasa_saga_definition_catalog",
            "created_at",
            "timestamp(6)",
            false,
            None,
        ),
        (
            "nasa_saga_definition_catalog",
            "updated_at",
            "timestamp(6)",
            false,
            None,
        ),
        (
            "nasa_saga_capability_registry",
            "tenant",
            "varchar(256)",
            false,
            Some("utf8mb4_bin"),
        ),
        (
            "nasa_saga_capability_registry",
            "owner",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_capability_registry",
            "replica_identity",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_capability_registry",
            "workflow",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_capability_registry",
            "definition_version",
            "int unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_capability_registry",
            "step",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_capability_registry",
            "capability_digest",
            "char(64)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_capability_registry",
            "descriptor_json",
            "json",
            false,
            None,
        ),
        (
            "nasa_saga_capability_registry",
            "route_generation",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_capability_registry",
            "accepted_until_ms",
            "bigint",
            false,
            None,
        ),
        (
            "nasa_saga_capability_registry",
            "catalog_generation",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_capability_registry",
            "updated_at",
            "timestamp(6)",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_audit",
            "id",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_audit",
            "catalog_generation",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_audit",
            "actor",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_audit",
            "action",
            "varchar(32)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_audit",
            "object_key",
            audit_object_key_type.as_str(),
            false,
            Some("utf8mb4_bin"),
        ),
        (
            "nasa_saga_catalog_audit",
            "object_digest",
            "char(64)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_audit",
            "operation_id",
            "varchar(190)",
            true,
            Some("utf8mb4_bin"),
        ),
        (
            "nasa_saga_catalog_audit",
            "reason",
            "varchar(512)",
            true,
            Some("utf8mb4_bin"),
        ),
        (
            "nasa_saga_catalog_audit",
            "created_at",
            "timestamp(6)",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_replica",
            "service_identity",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_replica",
            "replica_identity",
            "varchar(128)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_replica",
            "snapshot_generation",
            "bigint unsigned",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_replica",
            "snapshot_digest",
            "char(64)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_replica",
            "activation_contract_digest",
            "char(64)",
            false,
            Some("ascii_bin"),
        ),
        (
            "nasa_saga_catalog_replica",
            "ready",
            "tinyint(1)",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_replica",
            "lease_until_ms",
            "bigint",
            false,
            None,
        ),
        (
            "nasa_saga_catalog_replica",
            "updated_at",
            "timestamp(6)",
            false,
            None,
        ),
    ];
    for (table, column, column_type, nullable, expected_collation) in expected {
        let actual = columns
            .get(&(table.to_owned(), column.to_owned()))
            .ok_or_else(|| anyhow::anyhow!("Saga Catalog required column is missing"))?;
        anyhow::ensure!(
            actual.0 == column_type && actual.1 == nullable,
            "Saga Catalog column type or nullability is invalid"
        );
        if let Some(expected_collation) = expected_collation {
            anyhow::ensure!(
                actual.2.as_deref() == Some(expected_collation),
                "Saga Catalog text collation is invalid"
            );
        }
    }
    for (table, index, unique, expected_columns) in [
        ("nasa_saga_catalog_state", "PRIMARY", true, "singleton"),
        (
            "nasa_saga_definition_catalog",
            "PRIMARY",
            true,
            "tenant,workflow,definition_version",
        ),
        (
            "nasa_saga_definition_catalog",
            "idx_nasa_saga_definition_lifecycle",
            false,
            "lifecycle,catalog_generation",
        ),
        (
            "nasa_saga_capability_registry",
            "PRIMARY",
            true,
            "tenant,owner,replica_identity,workflow,definition_version,step",
        ),
        (
            "nasa_saga_capability_registry",
            "idx_nasa_saga_capability_lookup",
            false,
            "tenant,workflow,definition_version,step,accepted_until_ms",
        ),
        (
            "nasa_saga_capability_registry",
            "idx_nasa_saga_capability_lease",
            false,
            "accepted_until_ms",
        ),
        (
            "nasa_saga_catalog_audit",
            "idx_nasa_saga_catalog_audit_generation",
            false,
            "catalog_generation,id",
        ),
        (
            "nasa_saga_catalog_audit",
            "uk_nasa_saga_catalog_audit_operation",
            true,
            "actor,action,operation_id",
        ),
        (
            "nasa_saga_catalog_replica",
            "PRIMARY",
            true,
            "service_identity,replica_identity",
        ),
        (
            "nasa_saga_catalog_replica",
            "idx_nasa_saga_catalog_replica_lease",
            false,
            "service_identity,ready,lease_until_ms",
        ),
    ] {
        verify_index(connection, table, index, unique, expected_columns).await?;
    }
    let check_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = DATABASE() \
         AND constraint_type = 'CHECK' AND constraint_name IN \
         ('chk_nasa_saga_catalog_singleton','chk_nasa_saga_definition_version',\
          'chk_nasa_saga_definition_lifecycle','chk_nasa_saga_capability_version',\
          'chk_nasa_saga_capability_route_generation','chk_nasa_saga_capability_lease',\
          'chk_nasa_saga_catalog_replica_generation','chk_nasa_saga_catalog_replica_lease')",
    )
    .fetch_one(connection.as_mut())
    .await?;
    anyhow::ensure!(
        check_count == 8,
        "Saga Catalog CHECK constraints are incomplete"
    );
    verify_mysql_check_contracts(connection).await?;
    verify_mysql_column_behaviors(connection).await?;
    verify_mysql_activation_facts(connection).await?;
    Ok(())
}

/// 业务作用：复验已离开 candidate 的 definition 都具有可公开的首次激活时间证据。
///
/// 参数说明：`connection` 是 database 锁内连接。
///
/// 返回：历史事实完整时成功；任一非 candidate 行缺少 `activated_at` 时拒绝 Ready。
async fn verify_mysql_activation_facts(connection: &mut natx::Conn) -> anyhow::Result<()> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM nasa_saga_definition_catalog \
         WHERE lifecycle <> 'candidate' AND activated_at IS NULL",
    )
    .fetch_one(connection.as_mut())
    .await?;
    anyhow::ensure!(
        invalid == 0,
        "Saga Catalog non-candidate definition is missing activated_at"
    );
    Ok(())
}

/// 业务作用：逐项核对 MySQL 命名 CHECK 的真实表达式，阻止同名弱约束通过 Ready。
///
/// 参数说明：`connection` 是 database 锁内连接。
///
/// 返回：八项表达式与当前合同一致时成功；缺失或表达式漂移返回错误。
async fn verify_mysql_check_contracts(connection: &mut natx::Conn) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "SELECT tc.table_name AS checked_table, tc.constraint_name AS checked_constraint, \
         cc.check_clause AS checked_clause FROM information_schema.table_constraints tc \
         JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema \
         AND cc.constraint_name = tc.constraint_name WHERE tc.constraint_schema = DATABASE() \
         AND tc.constraint_type = 'CHECK'",
    )
    .fetch_all(connection.as_mut())
    .await?;
    let mut actual = std::collections::BTreeMap::new();
    for row in rows {
        actual.insert(
            (
                row.try_get::<String, _>("checked_table")?,
                row.try_get::<String, _>("checked_constraint")?,
            ),
            normalize_mysql_sql_contract(&row.try_get::<String, _>("checked_clause")?),
        );
    }
    for (table, constraint, expression) in [
        (
            "nasa_saga_catalog_state",
            "chk_nasa_saga_catalog_singleton",
            "(singleton = 1)",
        ),
        (
            "nasa_saga_definition_catalog",
            "chk_nasa_saga_definition_version",
            "(definition_version > 0)",
        ),
        (
            "nasa_saga_definition_catalog",
            "chk_nasa_saga_definition_lifecycle",
            "(lifecycle IN ('candidate','active','deprecated','retired'))",
        ),
        (
            "nasa_saga_capability_registry",
            "chk_nasa_saga_capability_version",
            "(definition_version > 0)",
        ),
        (
            "nasa_saga_capability_registry",
            "chk_nasa_saga_capability_route_generation",
            "(route_generation > 0)",
        ),
        (
            "nasa_saga_capability_registry",
            "chk_nasa_saga_capability_lease",
            "(accepted_until_ms >= 0)",
        ),
        (
            "nasa_saga_catalog_replica",
            "chk_nasa_saga_catalog_replica_generation",
            "(snapshot_generation >= 0)",
        ),
        (
            "nasa_saga_catalog_replica",
            "chk_nasa_saga_catalog_replica_lease",
            "(lease_until_ms >= 0)",
        ),
    ] {
        let found = actual
            .get(&(table.to_owned(), constraint.to_owned()))
            .ok_or_else(|| {
                anyhow::anyhow!("Saga Catalog CHECK `{table}.{constraint}` is missing")
            })?;
        let expected = normalize_mysql_sql_contract(expression);
        anyhow::ensure!(
            found == &expected,
            "Saga Catalog CHECK `{table}.{constraint}` expression is invalid: expected `{expected}`, found `{found}`"
        );
    }
    Ok(())
}

/// 业务作用：复验 MySQL 时间默认值、ON UPDATE 与 audit 自增行为。
///
/// 参数说明：`connection` 是 database 锁内连接。
///
/// 返回：默认值和额外行为精确命中当前合同时成功；漂移时返回错误。
async fn verify_mysql_column_behaviors(connection: &mut natx::Conn) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "SELECT table_name AS checked_table, column_name AS checked_column, \
         column_default AS checked_default, extra AS checked_extra FROM information_schema.columns \
         WHERE table_schema = DATABASE()",
    )
    .fetch_all(connection.as_mut())
    .await?;
    let mut actual = std::collections::BTreeMap::new();
    for row in rows {
        actual.insert(
            (
                row.try_get::<String, _>("checked_table")?,
                row.try_get::<String, _>("checked_column")?,
            ),
            (
                row.try_get::<Option<String>, _>("checked_default")?
                    .map(|value| normalize_mysql_sql_contract(&value)),
                normalize_mysql_sql_contract(&row.try_get::<String, _>("checked_extra")?),
            ),
        );
    }
    for (table, column, default, extra) in [
        (
            "nasa_saga_catalog_state",
            "updated_at",
            Some("current_timestamp(6)"),
            "default_generatedonupdatecurrent_timestamp(6)",
        ),
        ("nasa_saga_definition_catalog", "activated_at", None, ""),
        (
            "nasa_saga_definition_catalog",
            "created_at",
            Some("current_timestamp(6)"),
            "default_generated",
        ),
        (
            "nasa_saga_definition_catalog",
            "updated_at",
            Some("current_timestamp(6)"),
            "default_generatedonupdatecurrent_timestamp(6)",
        ),
        (
            "nasa_saga_capability_registry",
            "updated_at",
            Some("current_timestamp(6)"),
            "default_generatedonupdatecurrent_timestamp(6)",
        ),
        ("nasa_saga_catalog_audit", "id", None, "auto_increment"),
        (
            "nasa_saga_catalog_audit",
            "created_at",
            Some("current_timestamp(6)"),
            "default_generated",
        ),
        (
            "nasa_saga_catalog_replica",
            "updated_at",
            Some("current_timestamp(6)"),
            "default_generatedonupdatecurrent_timestamp(6)",
        ),
    ] {
        let found = actual
            .get(&(table.to_owned(), column.to_owned()))
            .ok_or_else(|| {
                anyhow::anyhow!("Saga Catalog behavior column `{table}.{column}` is missing")
            })?;
        let expected_default = default.map(normalize_mysql_sql_contract);
        anyhow::ensure!(
            found.0 == expected_default && found.1 == normalize_mysql_sql_contract(extra),
            "Saga Catalog column `{table}.{column}` default or extra behavior is invalid"
        );
    }
    Ok(())
}

/// 业务作用：把 MySQL 结构表达式压缩为可精确比较的稳定文本。
///
/// 参数说明：`value` 是 information_schema 输出或源码静态期望。
///
/// 返回：小写且移除 ASCII 空白、标识符反引号、展示转义和等价字符集 introducer 的文本。
fn normalize_mysql_sql_contract(value: &str) -> String {
    let normalized = value
        .chars()
        .filter(|character| {
            !character.is_ascii_whitespace() && *character != '`' && *character != '\\'
        })
        .flat_map(char::to_lowercase)
        .collect::<String>();
    ["_utf8mb4", "_utf8mb3", "_utf8", "_ascii", "_latin1"]
        .into_iter()
        .fold(normalized, |text, introducer| {
            text.replace(&format!("{introducer}'"), "'")
        })
}

/// 业务作用：复验一个命名索引的唯一性和完整前导列顺序，防止同名残缺索引绕过 Ready 门禁。
///
/// 参数说明：连接、表、索引名、唯一性与逗号分隔列序共同描述执行合同。
///
/// 返回：服务端索引精确命中时成功；缺失、列序或唯一性漂移返回错误。
async fn verify_index(
    connection: &mut natx::Conn,
    table: &str,
    index: &str,
    unique: bool,
    expected_columns: &str,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT MIN(non_unique) AS checked_non_unique, \
         GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS checked_columns \
         FROM information_schema.statistics WHERE table_schema = DATABASE() \
         AND table_name = ? AND index_name = ? GROUP BY index_name",
    )
    .bind(table)
    .bind(index)
    .fetch_optional(connection.as_mut())
    .await?
    .ok_or_else(|| anyhow::anyhow!("Saga Catalog required index `{table}.{index}` is missing"))?;
    let non_unique: i64 = row.try_get("checked_non_unique")?;
    let columns_text: String = row.try_get("checked_columns")?;
    anyhow::ensure!(
        (non_unique == 0) == unique && columns_text == expected_columns,
        "Saga Catalog index `{table}.{index}` contract is invalid"
    );
    Ok(())
}

/// 业务作用：读取同一 MySQL generation 的候选与运行 definition，以及当前有效能力 route。
///
/// 参数说明：`datasource` 是 Orchestrator 明确绑定的 Catalog datasource。
///
/// 返回：所有持久行与 seal 复验通过时返回不可变快照；漂移或数据库失败返回错误。
pub async fn load_dynamic_catalog_for(datasource: &str) -> anyhow::Result<DynamicCatalogSnapshot> {
    let mut connection = natx::conn_for(datasource).await?;
    let generation: u64 =
        sqlx::query_scalar("SELECT generation FROM nasa_saga_catalog_state WHERE singleton = 1")
            .fetch_one(connection.as_mut())
            .await?;
    let rows = sqlx::query(
        "SELECT tenant, workflow, definition_version, definition_digest, artifact_json, \
         lifecycle, catalog_generation, \
         CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) AS published_at_ms, \
         CAST(UNIX_TIMESTAMP(activated_at) * 1000 AS SIGNED) AS activated_at_ms \
         FROM nasa_saga_definition_catalog \
         WHERE lifecycle IN ('candidate', 'active', 'deprecated') \
         ORDER BY tenant, workflow, definition_version",
    )
    .fetch_all(connection.as_mut())
    .await?;
    let mut registry = DefinitionRegistry::new();
    let mut definition_records = Vec::with_capacity(rows.len());
    for row in rows {
        let tenant: String = row.try_get("tenant")?;
        let workflow: String = row.try_get("workflow")?;
        let version: u32 = row.try_get("definition_version")?;
        let digest: String = row.try_get("definition_digest")?;
        let artifact_json: serde_json::Value = row.try_get("artifact_json")?;
        let lifecycle: String = row.try_get("lifecycle")?;
        let row_generation: u64 = row.try_get("catalog_generation")?;
        anyhow::ensure!(
            row_generation <= generation,
            "definition generation is ahead of catalog state"
        );
        let artifact: DefinitionArtifact = serde_json::from_value(artifact_json)?;
        anyhow::ensure!(
            artifact.tenant == tenant
                && artifact.workflow == workflow
                && artifact.definition_version == version
                && artifact.seal == digest,
            "definition catalog row does not match its sealed artifact"
        );
        let definition = artifact.to_definition()?;
        let lifecycle = parse_lifecycle(&lifecycle)?;
        if matches!(
            lifecycle,
            DefinitionLifecycle::Active | DefinitionLifecycle::Deprecated
        ) {
            registry.register_for_tenant(
                TenantId::new(&tenant).map_err(|error| anyhow::anyhow!(error.code()))?,
                definition,
                lifecycle == DefinitionLifecycle::Active,
            )?;
        }
        definition_records.push(DefinitionRecord {
            artifact,
            lifecycle,
            catalog_generation: row_generation,
            published_at_ms: row.try_get("published_at_ms")?,
            activated_at_ms: row.try_get("activated_at_ms")?,
        });
    }
    let now_ms = database_now_ms(connection.as_mut()).await?;
    let rows = sqlx::query(
        "SELECT tenant, capability_digest, descriptor_json, accepted_until_ms, catalog_generation \
         FROM nasa_saga_capability_registry WHERE accepted_until_ms >= ? \
         ORDER BY tenant, owner, replica_identity, workflow, definition_version, step",
    )
    .bind(now_ms)
    .fetch_all(connection.as_mut())
    .await?;
    let mut capabilities = Vec::with_capacity(rows.len());
    for row in rows {
        let capability_digest: String = row.try_get("capability_digest")?;
        let descriptor_json: serde_json::Value = row.try_get("descriptor_json")?;
        let accepted_until_ms: i64 = row.try_get("accepted_until_ms")?;
        let row_generation: u64 = row.try_get("catalog_generation")?;
        anyhow::ensure!(
            row_generation <= generation,
            "capability generation is ahead of catalog state"
        );
        let descriptor: CapabilityDescriptor = serde_json::from_value(descriptor_json)?;
        anyhow::ensure!(
            descriptor.tenant == row.try_get::<String, _>("tenant")?
                && descriptor.digest() == capability_digest,
            "capability digest does not match stored content"
        );
        capabilities.push(RegisteredCapability {
            descriptor,
            accepted_until_ms,
            capability_digest,
        });
    }
    let confirmed_generation: u64 =
        sqlx::query_scalar("SELECT generation FROM nasa_saga_catalog_state WHERE singleton = 1")
            .fetch_one(connection.as_mut())
            .await?;
    anyhow::ensure!(
        confirmed_generation == generation,
        "catalog generation changed while the snapshot was loading"
    );
    Ok(DynamicCatalogSnapshot::with_definition_records(
        generation,
        registry,
        definition_records,
        capabilities,
    ))
}

/// 业务作用：按完整 definition key 读取 Catalog 记录，供协议层在租户授权后返回当前生命周期。
///
/// 参数说明：`datasource` 固定 Catalog，tenant/workflow/version 共同定位不可变定义。
///
/// 返回：记录存在且持久字段与 seal 一致时返回；不存在返回空，漂移或数据库失败返回错误。
pub async fn load_definition_for(
    datasource: &str,
    tenant: &str,
    workflow: &str,
    version: u32,
) -> anyhow::Result<Option<DefinitionRecord>> {
    let mut connection = natx::conn_for(datasource).await?;
    let row = sqlx::query(
        "SELECT tenant, workflow, definition_version, definition_digest, artifact_json, \
         lifecycle, catalog_generation, \
         CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) AS published_at_ms, \
         CAST(UNIX_TIMESTAMP(activated_at) * 1000 AS SIGNED) AS activated_at_ms \
         FROM nasa_saga_definition_catalog \
         WHERE tenant = ? AND workflow = ? AND definition_version = ?",
    )
    .bind(tenant)
    .bind(workflow)
    .bind(version)
    .fetch_optional(connection.as_mut())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let stored_tenant: String = row.try_get("tenant")?;
    let stored_workflow: String = row.try_get("workflow")?;
    let stored_version: u32 = row.try_get("definition_version")?;
    let digest: String = row.try_get("definition_digest")?;
    let artifact_json: serde_json::Value = row.try_get("artifact_json")?;
    let lifecycle: String = row.try_get("lifecycle")?;
    let artifact: DefinitionArtifact = serde_json::from_value(artifact_json)?;
    anyhow::ensure!(
        artifact.tenant == stored_tenant
            && artifact.workflow == stored_workflow
            && artifact.definition_version == stored_version
            && artifact.seal == digest,
        "definition catalog row does not match its sealed artifact"
    );
    artifact.to_definition()?;
    let lifecycle = match lifecycle.as_str() {
        "candidate" => DefinitionLifecycle::Candidate,
        "active" => DefinitionLifecycle::Active,
        "deprecated" => DefinitionLifecycle::Deprecated,
        "retired" => DefinitionLifecycle::Retired,
        _ => anyhow::bail!("definition lifecycle is invalid"),
    };
    Ok(Some(DefinitionRecord {
        artifact,
        lifecycle,
        catalog_generation: row.try_get("catalog_generation")?,
        published_at_ms: row.try_get("published_at_ms")?,
        activated_at_ms: row.try_get("activated_at_ms")?,
    }))
}

/// 业务作用：登记 Orchestrator 副本已完整装载并复验某个 Catalog 快照，同时续租 Ready 资格。
///
/// 参数说明：数据源和两个身份定位副本，`generation`/`digest` 是已装载快照，
/// `activation_contract_digest` 绑定本副本实际发布边界，`lease_ms` 是有界租期。
///
/// 返回：代际一致且确认提交时返回数据库时钟下的租约截止毫秒；代际变化、参数非法或持久化失败时不授予租约。
pub async fn acknowledge_catalog_generation_for(
    datasource: &str,
    service_identity: &ServiceIdentity,
    replica_identity: &str,
    generation: u64,
    digest: &str,
    activation_contract_digest: &str,
    lease_ms: u64,
) -> anyhow::Result<i64> {
    ServiceIdentity::new(replica_identity).map_err(|error| anyhow::anyhow!(error.code()))?;
    anyhow::ensure!(
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "catalog snapshot digest is invalid"
    );
    anyhow::ensure!(
        activation_contract_digest.len() == 64
            && activation_contract_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "catalog activation contract digest is invalid"
    );
    anyhow::ensure!(
        (5_000..=60_000).contains(&lease_ms),
        "catalog replica lease is outside the supported range"
    );
    let mut connection = natx::conn_for(datasource).await?;
    let mut transaction = connection.as_mut().begin().await?;
    let current: u64 = sqlx::query_scalar(
        "SELECT generation FROM nasa_saga_catalog_state WHERE singleton = 1 FOR UPDATE",
    )
    .fetch_one(&mut *transaction)
    .await?;
    anyhow::ensure!(
        current == generation,
        "catalog generation changed before replica acknowledgement"
    );
    // UPSERT 可能等待已经存在的副本行；先在本事务内占有该行，再开始租约计时。
    // 临时非 Ready 状态不独立提交，任何后续失败都会回滚，不会发布半份确认。
    sqlx::query(
        "INSERT INTO nasa_saga_catalog_replica \
         (service_identity, replica_identity, snapshot_generation, snapshot_digest, \
          activation_contract_digest, ready, lease_until_ms) \
         VALUES (?, ?, ?, ?, ?, 0, 0) AS incoming \
         ON DUPLICATE KEY UPDATE snapshot_generation = incoming.snapshot_generation, \
         snapshot_digest = incoming.snapshot_digest, \
         activation_contract_digest = incoming.activation_contract_digest, \
         ready = 0, lease_until_ms = 0",
    )
    .bind(service_identity.as_str())
    .bind(replica_identity)
    .bind(generation)
    .bind(digest)
    .bind(activation_contract_digest)
    .execute(&mut *transaction)
    .await?;
    let now_ms = database_now_ms(&mut *transaction).await?;
    let lease_until_ms = now_ms
        .checked_add(i64::try_from(lease_ms)?)
        .ok_or_else(|| anyhow::anyhow!("catalog replica lease overflow"))?;
    // 两类控制行锁都已持有，完整快照与有效期限必须在同一次提交中成为共享确认事实。
    sqlx::query(
        "UPDATE nasa_saga_catalog_replica SET ready = 1, lease_until_ms = ? \
         WHERE service_identity = ? AND replica_identity = ?",
    )
    .bind(lease_until_ms)
    .bind(service_identity.as_str())
    .bind(replica_identity)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(lease_until_ms)
}

/// 业务作用：判断同一逻辑 Orchestrator 的全部有效 Ready 副本是否确认了当前完整快照。
///
/// 参数说明：身份限定共享控制面，`generation`/`digest` 是待激活候选依赖的当前快照，
/// `activation_contract_digest` 是本进程实际运行时路由合同。
///
/// 返回：至少存在一个有效副本且全部确认相同代际和摘要时返回真；读取失败返回错误。
pub async fn catalog_generation_fully_acknowledged_for(
    datasource: &str,
    service_identity: &ServiceIdentity,
    generation: u64,
    digest: &str,
    activation_contract_digest: &str,
) -> anyhow::Result<bool> {
    let mut connection = natx::conn_for(datasource).await?;
    let now_ms = database_now_ms(connection.as_mut()).await?;
    let row = sqlx::query(
        "SELECT CAST(COUNT(*) AS SIGNED) AS replica_count, \
         CAST(COALESCE(SUM(CASE WHEN snapshot_generation = ? AND snapshot_digest = ? \
         AND activation_contract_digest = ? THEN 0 ELSE 1 END), 0) AS SIGNED) AS mismatch_count \
         FROM nasa_saga_catalog_replica WHERE service_identity = ? AND ready = 1 AND lease_until_ms >= ?",
    )
    .bind(generation)
    .bind(digest)
    .bind(activation_contract_digest)
    .bind(service_identity.as_str())
    .bind(now_ms)
    .fetch_one(connection.as_mut())
    .await?;
    let count: i64 = row.try_get("replica_count")?;
    let mismatches: i64 = row.try_get("mismatch_count")?;
    Ok(count > 0 && mismatches == 0)
}

/// 业务作用：在副本排空时撤销其 Catalog Ready 资格，使后续激活不等待已失权进程。
///
/// 参数说明：数据源和两个身份精确定位本副本租约。
///
/// 返回：对应记录不存在也视为幂等成功；数据库失败返回错误。
pub async fn retire_catalog_replica_for(
    datasource: &str,
    service_identity: &ServiceIdentity,
    replica_identity: &str,
) -> anyhow::Result<()> {
    let mut connection = natx::conn_for(datasource).await?;
    sqlx::query(
        "UPDATE nasa_saga_catalog_replica SET ready = 0, lease_until_ms = 0 \
         WHERE service_identity = ? AND replica_identity = ?",
    )
    .bind(service_identity.as_str())
    .bind(replica_identity)
    .execute(connection.as_mut())
    .await?;
    Ok(())
}

/// 业务作用：以 workflow owner 身份幂等发布不可变 candidate，并在同事务推进 Catalog generation 与审计。
///
/// 参数说明：`datasource` 是共享 Catalog，`actor` 是认证主体，`artifact` 是完整 definition seal。
///
/// 返回：首次发布或同摘要重放的类型化结论与持久记录；同键异摘要、越权或数据库失败返回错误。
pub async fn publish_definition_for(
    datasource: &str,
    actor: &ServiceIdentity,
    artifact: &DefinitionArtifact,
) -> anyhow::Result<(DefinitionPublishDisposition, DefinitionRecord)> {
    validate_definition_actor(actor, artifact)?;
    artifact
        .to_definition()
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    let artifact_json = serde_json::to_value(artifact)?;
    let mut connection = natx::conn_for(datasource).await?;
    let mut transaction = connection.as_mut().begin().await?;
    let generation: u64 = sqlx::query_scalar(
        "SELECT generation FROM nasa_saga_catalog_state WHERE singleton = 1 FOR UPDATE",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if let Some(row) = sqlx::query(
        "SELECT definition_digest, artifact_json, lifecycle, catalog_generation, \
         CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) AS published_at_ms, \
         CAST(UNIX_TIMESTAMP(activated_at) * 1000 AS SIGNED) AS activated_at_ms \
         FROM nasa_saga_definition_catalog WHERE tenant = ? AND workflow = ? \
         AND definition_version = ? FOR UPDATE",
    )
    .bind(&artifact.tenant)
    .bind(&artifact.workflow)
    .bind(artifact.definition_version)
    .fetch_optional(&mut *transaction)
    .await?
    {
        let digest: String = row.try_get("definition_digest")?;
        let stored: DefinitionArtifact = serde_json::from_value(row.try_get("artifact_json")?)?;
        if digest != artifact.seal || stored != *artifact {
            return Err(DefinitionCatalogError::DigestConflict.into());
        }
        let lifecycle = parse_lifecycle(row.try_get::<String, _>("lifecycle")?.as_str())?;
        let record = DefinitionRecord {
            artifact: stored,
            lifecycle,
            catalog_generation: row.try_get("catalog_generation")?,
            published_at_ms: row.try_get("published_at_ms")?,
            activated_at_ms: row.try_get("activated_at_ms")?,
        };
        transaction.commit().await?;
        return Ok((DefinitionPublishDisposition::Duplicate, record));
    }
    let next = generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("catalog generation overflow"))?;
    sqlx::query("UPDATE nasa_saga_catalog_state SET generation = ? WHERE singleton = 1")
        .bind(next)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "INSERT INTO nasa_saga_definition_catalog \
         (tenant, workflow, definition_version, workflow_owner, definition_digest, artifact_json, lifecycle, catalog_generation) \
         VALUES (?, ?, ?, ?, ?, ?, 'candidate', ?)",
    )
    .bind(&artifact.tenant)
    .bind(&artifact.workflow)
    .bind(artifact.definition_version)
    .bind(&artifact.workflow_owner)
    .bind(&artifact.seal)
    .bind(artifact_json)
    .bind(next)
    .execute(&mut *transaction)
    .await?;
    insert_audit(
        &mut transaction,
        next,
        actor.as_str(),
        "publish_definition",
        &definition_key(artifact),
        &artifact.seal,
        None,
    )
    .await?;
    let published_at_ms: i64 = sqlx::query_scalar(
        "SELECT CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) \
         FROM nasa_saga_definition_catalog WHERE tenant = ? AND workflow = ? \
         AND definition_version = ?",
    )
    .bind(&artifact.tenant)
    .bind(&artifact.workflow)
    .bind(artifact.definition_version)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok((
        DefinitionPublishDisposition::Published,
        DefinitionRecord {
            artifact: artifact.clone(),
            lifecycle: DefinitionLifecycle::Candidate,
            catalog_generation: next,
            published_at_ms,
            activated_at_ms: None,
        },
    ))
}

/// 业务作用：在行锁下验证全部步骤存在有效 capability 后原子激活 candidate。
///
/// 参数说明：`datasource` 是共享 Catalog，`actor` 是获权主体，`gate` 锁定
/// 已被全部 Ready 副本确认的同代摘要与数据面协议，其余字段定位不可变定义。
///
/// 返回：激活成功或 active 幂等重放时返回记录；缺能力、状态非法或数据库失败返回错误。
pub async fn activate_definition_for(
    datasource: &str,
    actor: &ServiceIdentity,
    gate: &DefinitionActivationGate,
    tenant: &str,
    workflow: &str,
    version: u32,
) -> anyhow::Result<DefinitionRecord> {
    change_definition_lifecycle(
        datasource,
        actor,
        DefinitionLifecycleChange {
            tenant,
            workflow,
            version,
            target: DefinitionLifecycle::Active,
            activation_gate: Some(gate),
            operation: None,
        },
    )
    .await
}

/// 业务作用：以持久幂等身份和事务内 seal 前置条件激活 candidate definition。
///
/// 参数说明：数据源、主体、门禁和定义键定位目标，`operation` 携带 CAS、操作身份与审计原因。
///
/// 返回：首次提交或同参数重放返回 active 记录；摘要或操作身份冲突时不改变 Catalog。
pub async fn activate_definition_with_operation_for(
    datasource: &str,
    actor: &ServiceIdentity,
    gate: &DefinitionActivationGate,
    tenant: &str,
    workflow: &str,
    version: u32,
    operation: &DefinitionLifecycleOperation,
) -> anyhow::Result<DefinitionRecord> {
    change_definition_lifecycle(
        datasource,
        actor,
        DefinitionLifecycleChange {
            tenant,
            workflow,
            version,
            target: DefinitionLifecycle::Active,
            activation_gate: Some(gate),
            operation: Some(operation),
        },
    )
    .await
}

/// 业务作用：原子把 active definition 标记为 deprecated，只关闭新实例入口。
///
/// 参数说明：`datasource` 是共享 Catalog，`actor` 是获权主体，其余字段定位不可变定义。
///
/// 返回：变更成功或 deprecated 幂等重放时返回记录；状态非法或数据库失败返回错误。
pub async fn deprecate_definition_for(
    datasource: &str,
    actor: &ServiceIdentity,
    tenant: &str,
    workflow: &str,
    version: u32,
) -> anyhow::Result<DefinitionRecord> {
    change_definition_lifecycle(
        datasource,
        actor,
        DefinitionLifecycleChange {
            tenant,
            workflow,
            version,
            target: DefinitionLifecycle::Deprecated,
            activation_gate: None,
            operation: None,
        },
    )
    .await
}

/// 业务作用：以持久幂等身份和事务内 seal 前置条件弃用 active definition。
///
/// 参数说明：数据源、主体和定义键定位目标，`operation` 携带 CAS、操作身份与审计原因。
///
/// 返回：首次提交或同参数重放返回 deprecated 记录；摘要或操作身份冲突时不改变 Catalog。
pub async fn deprecate_definition_with_operation_for(
    datasource: &str,
    actor: &ServiceIdentity,
    tenant: &str,
    workflow: &str,
    version: u32,
    operation: &DefinitionLifecycleOperation,
) -> anyhow::Result<DefinitionRecord> {
    change_definition_lifecycle(
        datasource,
        actor,
        DefinitionLifecycleChange {
            tenant,
            workflow,
            version,
            target: DefinitionLifecycle::Deprecated,
            activation_gate: None,
            operation: Some(operation),
        },
    )
    .await
}

/// 业务作用：以持久幂等身份和事务内 seal 前置条件退休无持久引用的 deprecated definition。
///
/// 参数说明：数据源、主体和定义键定位目标，`operation` 携带 CAS、操作身份与审计原因。
///
/// 返回：首次提交或同参数重放返回 retired 记录；摘要或操作身份冲突时不改变 Catalog。
pub async fn retire_definition_with_operation_for(
    datasource: &str,
    actor: &ServiceIdentity,
    tenant: &str,
    workflow: &str,
    version: u32,
    operation: &DefinitionLifecycleOperation,
) -> anyhow::Result<DefinitionRecord> {
    change_definition_lifecycle(
        datasource,
        actor,
        DefinitionLifecycleChange {
            tenant,
            workflow,
            version,
            target: DefinitionLifecycle::Retired,
            activation_gate: None,
            operation: Some(operation),
        },
    )
    .await
}

/// 业务作用：以 owner 认证身份登记或续租一个逐实例 capability，并在数据库行锁内分配单调 route generation。
///
/// 参数说明：`datasource` 是共享目录，`actor` 来自认证边界，`descriptor` 是当前实例能力。
///
/// 返回：服务端数据库时钟确定的有界租约、有效路由代际、内容摘要和 Catalog generation；越权或持久事实漂移返回错误。
pub async fn register_capability_for(
    datasource: &str,
    actor: &ServiceIdentity,
    descriptor: &CapabilityDescriptor,
) -> anyhow::Result<CapabilityReceipt> {
    validate_capability_actor(actor, descriptor)?;
    let route_contract_digest = descriptor.route_contract_digest();
    let lease_ms = descriptor
        .requested_lease_ms
        .clamp(MIN_CAPABILITY_LEASE_MS, MAX_CAPABILITY_LEASE_MS);
    let mut connection = natx::conn_for(datasource).await?;
    let mut transaction = connection.as_mut().begin().await?;
    // 先取得目录与能力行的控制权，再建立租约，避免锁等待消耗尚未发布的有效期。
    let generation: u64 = sqlx::query_scalar(
        "SELECT generation FROM nasa_saga_catalog_state WHERE singleton = 1 FOR UPDATE",
    )
    .fetch_one(&mut *transaction)
    .await?;
    // retired 是不可逆的 handler 停用边界；后到的续租不能重新开放旧定义路由。
    let lifecycle: Option<String> = sqlx::query_scalar(
        "SELECT lifecycle FROM nasa_saga_definition_catalog WHERE tenant = ? AND workflow = ? AND definition_version = ?")
        .bind(&descriptor.tenant).bind(&descriptor.workflow).bind(descriptor.definition_version)
        .fetch_optional(&mut *transaction).await?;
    if lifecycle.as_deref() == Some("retired") {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    let existing = sqlx::query(
        "SELECT capability_digest, descriptor_json, route_generation FROM nasa_saga_capability_registry \
         WHERE tenant = ? AND owner = ? AND replica_identity = ? AND workflow = ? AND definition_version = ? \
         AND step = ? FOR UPDATE",
    )
    .bind(&descriptor.tenant)
    .bind(&descriptor.owner)
    .bind(&descriptor.replica_identity)
    .bind(&descriptor.workflow)
    .bind(descriptor.definition_version)
    .bind(&descriptor.step)
    .fetch_optional(&mut *transaction)
    .await?;
    let mut effective_descriptor = descriptor.clone();
    let content_changed = match existing {
        Some(row) => {
            let old_digest: String = row.try_get("capability_digest")?;
            let old_route_generation: u64 = row.try_get("route_generation")?;
            let old_descriptor: CapabilityDescriptor =
                serde_json::from_value(row.try_get("descriptor_json")?)?;
            anyhow::ensure!(
                old_descriptor.route_generation == old_route_generation
                    && old_descriptor.digest() == old_digest,
                "Saga Catalog capability persistence is inconsistent"
            );
            if old_descriptor.route_contract_digest() != route_contract_digest {
                // 内容变化必须在持有 capability 行锁和全局 generation 锁时分配下一代，
                // 进程时钟与调用方自报值均不能推进或回退数据面路由权威。
                effective_descriptor.route_generation = old_route_generation
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("capability route generation overflow"))?;
                true
            } else {
                // 相同路由的重启或不确定重报沿用已提交代际，使响应丢失不会制造虚假变更。
                effective_descriptor.route_generation = old_route_generation;
                false
            }
        }
        None => {
            effective_descriptor.route_generation = 1;
            true
        }
    };
    let digest = effective_descriptor.digest();
    let descriptor_json = serde_json::to_value(&effective_descriptor)?;
    let catalog_generation = if content_changed {
        let next = generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("catalog generation overflow"))?;
        sqlx::query("UPDATE nasa_saga_catalog_state SET generation = ? WHERE singleton = 1")
            .bind(next)
            .execute(&mut *transaction)
            .await?;
        next
    } else {
        generation
    };
    let now_ms = database_now_ms(&mut *transaction).await?;
    let accepted_until_ms = now_ms
        .checked_add(i64::try_from(lease_ms)?)
        .ok_or_else(|| anyhow::anyhow!("capability lease deadline overflow"))?;
    sqlx::query(
        "INSERT INTO nasa_saga_capability_registry \
         (tenant, owner, replica_identity, workflow, definition_version, step, capability_digest, descriptor_json, route_generation, accepted_until_ms, catalog_generation) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS incoming \
         ON DUPLICATE KEY UPDATE capability_digest = incoming.capability_digest, \
         descriptor_json = incoming.descriptor_json, route_generation = incoming.route_generation, \
         accepted_until_ms = incoming.accepted_until_ms, catalog_generation = incoming.catalog_generation",
    )
    .bind(&effective_descriptor.tenant)
    .bind(&effective_descriptor.owner)
    .bind(&effective_descriptor.replica_identity)
    .bind(&effective_descriptor.workflow)
    .bind(effective_descriptor.definition_version)
    .bind(&effective_descriptor.step)
    .bind(&digest)
    .bind(descriptor_json)
    .bind(effective_descriptor.route_generation)
    .bind(accepted_until_ms)
    .bind(catalog_generation)
    .execute(&mut *transaction)
    .await?;
    if content_changed {
        insert_audit(
            &mut transaction,
            catalog_generation,
            actor.as_str(),
            "register_capability",
            &capability_key(&effective_descriptor),
            &digest,
            None,
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(CapabilityReceipt {
        accepted_until_ms,
        capability_digest: digest,
        route_generation: effective_descriptor.route_generation,
        catalog_generation,
    })
}

/// 业务作用：冻结一次 definition 生命周期变更的目标键、门禁与人工操作合同。
struct DefinitionLifecycleChange<'a> {
    tenant: &'a str,
    workflow: &'a str,
    version: u32,
    target: DefinitionLifecycle,
    activation_gate: Option<&'a DefinitionActivationGate>,
    operation: Option<&'a DefinitionLifecycleOperation>,
}

/// 业务作用：执行 definition 到目标生命周期的封闭迁移，并将 generation 与审计共同提交。
///
/// 参数说明：`datasource` 与 `actor` 定位数据库权威，`change` 绑定定义键、目标状态、激活门禁与人工操作。
///
/// 返回：迁移后的完整记录；能力不足、越权、状态不合法或数据库失败返回错误。
async fn change_definition_lifecycle(
    datasource: &str,
    actor: &ServiceIdentity,
    change: DefinitionLifecycleChange<'_>,
) -> anyhow::Result<DefinitionRecord> {
    let DefinitionLifecycleChange {
        tenant,
        workflow,
        version,
        target,
        activation_gate,
        operation,
    } = change;
    TenantId::new(tenant).map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    let mut connection = natx::conn_for(datasource).await?;
    let mut transaction = connection.as_mut().begin().await?;
    let generation: u64 = sqlx::query_scalar(
        "SELECT generation FROM nasa_saga_catalog_state WHERE singleton = 1 FOR UPDATE",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let row = sqlx::query(
        "SELECT workflow_owner, definition_digest, artifact_json, lifecycle, catalog_generation, \
         CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) AS published_at_ms, \
         CAST(UNIX_TIMESTAMP(activated_at) * 1000 AS SIGNED) AS activated_at_ms \
         FROM nasa_saga_definition_catalog WHERE tenant = ? AND workflow = ? \
         AND definition_version = ? FOR UPDATE",
    )
    .bind(tenant)
    .bind(workflow)
    .bind(version)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(DefinitionCatalogError::NotFound)?;
    let owner: String = row.try_get("workflow_owner")?;
    let actor_is_owner = owner == actor.as_str();
    let actor_is_orchestrator = target == DefinitionLifecycle::Active
        && activation_gate.is_some_and(|gate| gate.orchestrator_service_identity() == actor);
    if !actor_is_owner && !actor_is_orchestrator {
        return Err(DefinitionCatalogError::PermissionDenied.into());
    }
    let current = parse_lifecycle(row.try_get::<String, _>("lifecycle")?.as_str())?;
    let artifact: DefinitionArtifact = serde_json::from_value(row.try_get("artifact_json")?)?;
    let digest: String = row.try_get("definition_digest")?;
    let published_at_ms: i64 = row.try_get("published_at_ms")?;
    let activated_at_ms: Option<i64> = row.try_get("activated_at_ms")?;
    let action = match target {
        DefinitionLifecycle::Active => "activate_definition",
        DefinitionLifecycle::Deprecated => "deprecate_definition",
        DefinitionLifecycle::Retired => "retire_definition",
        DefinitionLifecycle::Candidate => {
            return Err(DefinitionCatalogError::FailedPrecondition.into())
        }
    };
    if let Some(operation) = operation {
        if let Some(audit) = sqlx::query(
            "SELECT catalog_generation, object_key, object_digest, reason \
             FROM nasa_saga_catalog_audit WHERE actor = ? AND action = ? AND operation_id = ? FOR UPDATE",
        )
        .bind(actor.as_str())
        .bind(action)
        .bind(operation.operation_id())
        .fetch_optional(&mut *transaction)
        .await?
        {
            let same = audit.try_get::<String, _>("object_key")? == definition_key(&artifact)
                && audit.try_get::<String, _>("object_digest")? == digest
                && operation.expected_sha256() == digest
                && audit.try_get::<Option<String>, _>("reason")?.as_deref()
                    == Some(operation.reason());
            if !same {
                return Err(DefinitionCatalogError::OperationConflict.into());
            }
            let record = DefinitionRecord {
                artifact,
                lifecycle: target,
                catalog_generation: audit.try_get("catalog_generation")?,
                published_at_ms,
                activated_at_ms,
            };
            transaction.commit().await?;
            return Ok(record);
        }
        if digest != operation.expected_sha256() {
            return Err(DefinitionCatalogError::PreconditionFailed.into());
        }
    }
    if current == target {
        if let Some(operation) = operation {
            insert_audit(
                &mut transaction,
                row.try_get("catalog_generation")?,
                actor.as_str(),
                action,
                &definition_key(&artifact),
                &digest,
                Some(operation),
            )
            .await?;
        }
        let record = DefinitionRecord {
            artifact,
            lifecycle: current,
            catalog_generation: row.try_get("catalog_generation")?,
            published_at_ms,
            activated_at_ms,
        };
        transaction.commit().await?;
        return Ok(record);
    }
    if !matches!(
        (current, target),
        (DefinitionLifecycle::Candidate, DefinitionLifecycle::Active)
            | (DefinitionLifecycle::Active, DefinitionLifecycle::Deprecated)
            | (
                DefinitionLifecycle::Deprecated,
                DefinitionLifecycle::Retired
            )
    ) {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    if target == DefinitionLifecycle::Retired {
        // generation 行锁与所有受管推进共用；引用检查完成前不能发布退休状态。
        ensure_definition_unreferenced(&mut transaction, &artifact).await?;
    }
    if target == DefinitionLifecycle::Active {
        let gate = activation_gate.ok_or(DefinitionCatalogError::FailedPrecondition)?;
        if generation != gate.catalog_generation() {
            return Err(DefinitionCatalogError::FailedPrecondition.into());
        }
        validate_live_capabilities(&mut transaction, &artifact, gate).await?;
        ensure_catalog_generation_acknowledged(&mut transaction, generation, gate).await?;
    }
    let next = generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("catalog generation overflow"))?;
    sqlx::query("UPDATE nasa_saga_catalog_state SET generation = ? WHERE singleton = 1")
        .bind(next)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "UPDATE nasa_saga_definition_catalog SET lifecycle = ?, catalog_generation = ?, \
         activated_at = CASE WHEN ? = 'active' THEN COALESCE(activated_at, CURRENT_TIMESTAMP(6)) ELSE activated_at END \
         WHERE tenant = ? AND workflow = ? AND definition_version = ?",
    )
    .bind(target.as_str())
    .bind(next)
    .bind(target.as_str())
    .bind(tenant)
    .bind(workflow)
    .bind(version)
    .execute(&mut *transaction)
    .await?;
    insert_audit(
        &mut transaction,
        next,
        actor.as_str(),
        action,
        &definition_key(&artifact),
        &digest,
        operation,
    )
    .await?;
    let activated_at_ms: Option<i64> = sqlx::query_scalar(
        "SELECT CAST(UNIX_TIMESTAMP(activated_at) * 1000 AS SIGNED) \
         FROM nasa_saga_definition_catalog WHERE tenant = ? AND workflow = ? AND definition_version = ?",
    )
    .bind(tenant)
    .bind(workflow)
    .bind(version)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(DefinitionRecord {
        artifact,
        lifecycle: target,
        catalog_generation: next,
        published_at_ms,
        activated_at_ms,
    })
}

/// 业务作用：在 Catalog 权威锁内证明定义不再被实例、审计或待投递资料引用。
///
/// 参数说明：`transaction` 固定当前 generation，`artifact` 定位租户与不可变定义。
///
/// 返回：全部引用释放时成功；保留的终态实例同样阻止退休，资料清理必须由独立保留流程完成。
async fn ensure_definition_unreferenced(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
    artifact: &DefinitionArtifact,
) -> anyhow::Result<()> {
    let version = artifact.definition_version;
    // 终态仍能接收迟到结果、DLT 重放和审计查询，不能仅凭 status 判断已经没有引用。
    let instance = sqlx::query("SELECT saga_id FROM saga_instance WHERE tenant_id = ? AND workflow_name = ? AND definition_version = ? LIMIT 1 FOR UPDATE")
        .bind(&artifact.tenant)
        .bind(&artifact.workflow)
        .bind(version)
        .fetch_optional(&mut **transaction)
        .await?;
    if instance.is_some() {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    // 实例资料若被外部保留流程部分删除，孤立的迁移记录仍然需要原定义；无法归属时保守拒绝。
    let audit = sqlx::query("SELECT saga_id FROM saga_transition WHERE definition_version = ? AND NOT EXISTS (SELECT 1 FROM saga_instance i WHERE i.saga_id = saga_transition.saga_id) LIMIT 1")
        .bind(version)
        .fetch_optional(&mut **transaction)
        .await?;
    if audit.is_some() {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    // 无实例归属的审计链不能被误当作已清理，保守保留所需 handler 和定义解释能力。
    let orphan = sqlx::query("SELECT saga_id FROM saga_audit_event WHERE (definition_version = ? OR definition_version IS NULL) AND NOT EXISTS (SELECT 1 FROM saga_instance i WHERE i.saga_id = saga_audit_event.saga_id) LIMIT 1")
        .bind(version).fetch_optional(&mut **transaction).await?;
    if orphan.is_some() {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    // 在途能力租约意味着旧参与方仍可接收迟到消息，必须先排空并停止续租。
    let now_ms = database_now_ms(&mut **transaction).await?;
    let live = sqlx::query("SELECT owner FROM nasa_saga_capability_registry WHERE tenant = ? AND workflow = ? AND definition_version = ? AND accepted_until_ms >= ? LIMIT 1")
        .bind(&artifact.tenant).bind(&artifact.workflow).bind(version).bind(now_ms)
        .fetch_optional(&mut **transaction).await?;
    if live.is_some() {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    // Outbox 保留行包括已发布事件与死信；解析失败意味着无法证明不引用此定义。
    let events: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM outbox_event WHERE tenant = ? AND aggregate_type = 'saga'",
    )
    .bind(&artifact.tenant)
    .fetch_all(&mut **transaction)
    .await?;
    for payload in events {
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|_| DefinitionCatalogError::FailedPrecondition)?;
        if value
            .get("workflow")
            .and_then(serde_json::Value::as_str)
            .is_none()
            || value
                .get("definition_version")
                .and_then(serde_json::Value::as_u64)
                .is_none()
        {
            return Err(DefinitionCatalogError::FailedPrecondition.into());
        }
        if value.get("workflow").and_then(serde_json::Value::as_str)
            == Some(artifact.workflow.as_str())
            && value
                .get("definition_version")
                .and_then(serde_json::Value::as_u64)
                == Some(u64::from(version))
        {
            return Err(DefinitionCatalogError::FailedPrecondition.into());
        }
    }
    Ok(())
}

/// 业务作用：证明候选 definition 的每个步骤至少存在一个租约内、语义相符且能被实际
/// publisher 接受的能力 route。
///
/// 参数说明：`transaction` 持有 generation 行锁，`artifact` 是待激活完整流程，
/// `gate` 绑定当前 Orchestrator 实际选用的数据面协议与 Redis 专属 route 上下文。
///
/// 返回：HTTP/gRPC 每步至少一个合法实例、消息队列每步只有一个 route key 时成功；
/// 缺失、非法、歧义或仅有漂移能力时拒绝激活。
async fn validate_live_capabilities(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
    artifact: &DefinitionArtifact,
    gate: &DefinitionActivationGate,
) -> anyhow::Result<()> {
    let transport = gate.transport();
    let definition = artifact.to_definition()?;
    let now_ms = database_now_ms(&mut **transaction).await?;
    for step in definition.steps() {
        let rows = sqlx::query(
            "SELECT descriptor_json FROM nasa_saga_capability_registry \
             WHERE tenant = ? AND owner = ? AND workflow = ? AND definition_version = ? AND step = ? \
             AND accepted_until_ms >= ?",
        )
        .bind(&artifact.tenant)
        .bind(step.owner().as_str())
        .bind(definition.name().as_str())
        .bind(definition.version().get())
        .bind(step.name().as_str())
        .bind(now_ms)
        .fetch_all(&mut **transaction)
        .await?;
        let mut matched = false;
        let mut selected_route: Option<(String, Option<String>)> = None;
        for row in rows {
            let descriptor: CapabilityDescriptor =
                serde_json::from_value(row.try_get("descriptor_json")?)?;
            if descriptor.transport == transport
                && descriptor.matches_step(&artifact.tenant, &definition, step)
            {
                // 历史持久行也必须在生命周期改变前按当前 publisher 合同复验；否则先发布 active
                // 状态、再由数据面拒绝 route，会留下无法投递的新实例入口。
                validate_capability_route_contract(&descriptor)
                    .map_err(|_| DefinitionCatalogError::FailedPrecondition)?;
                if transport == "kafka"
                    && descriptor.result_contract_digest.as_deref() != gate.result_backend_digest()
                {
                    // 激活事务必须在行锁内再次绑定 result 后端，避免预检后能力换租到另一套 broker。
                    return Err(DefinitionCatalogError::FailedPrecondition.into());
                }
                if transport != "kafka"
                    && !descriptor
                        .result_contract_digest
                        .as_deref()
                        .is_some_and(|digest| {
                            gate.accepts_result_contract(&descriptor.owner, digest)
                        })
                {
                    // owner 名称与 route 正确仍不足以接收结果；凭据合同必须在同一事务快照中命中。
                    return Err(DefinitionCatalogError::FailedPrecondition.into());
                }
                if transport == "redis-stream" {
                    let key_tag = gate
                        .redis_key_tag()
                        .ok_or(DefinitionCatalogError::FailedPrecondition)?;
                    // Redis activation 必须先证明 stream 与运行配置同槽；否则 active 发布后
                    // publisher 才拒绝 route，会开放一个无法发送 command 的新实例入口。
                    validate_redis_stream_route(&descriptor.endpoint, Some(key_tag))
                        .map_err(|_| DefinitionCatalogError::FailedPrecondition)?;
                }
                if !matches!(transport, "http" | "grpc") {
                    let route = (
                        descriptor.endpoint.clone(),
                        descriptor.effective_saga_base_path.clone(),
                    );
                    if selected_route
                        .as_ref()
                        .is_some_and(|current| current != &route)
                    {
                        return Err(DefinitionCatalogError::FailedPrecondition.into());
                    }
                    selected_route.get_or_insert(route);
                }
                matched = true;
            }
        }
        if !matched {
            return Err(DefinitionCatalogError::FailedPrecondition.into());
        }
    }
    Ok(())
}

/// 业务作用：在 definition 行仍被锁定时证明同一逻辑 Orchestrator 的全部有效 Ready 副本确认了精确快照。
///
/// 参数说明：`transaction` 持有 generation 权威，`generation` 是当前代际，`gate` 固定服务身份与摘要。
///
/// 返回：至少一个副本且全部命中同代同摘要时成功；缺失、落后或分歧时拒绝激活。
async fn ensure_catalog_generation_acknowledged(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
    generation: u64,
    gate: &DefinitionActivationGate,
) -> anyhow::Result<()> {
    let now_ms = database_now_ms(&mut **transaction).await?;
    let row = sqlx::query(
        "SELECT CAST(COUNT(*) AS SIGNED) AS replica_count, \
         CAST(COALESCE(SUM(CASE WHEN snapshot_generation = ? AND snapshot_digest = ? \
         AND activation_contract_digest = ? \
         THEN 0 ELSE 1 END), 0) AS SIGNED) AS mismatch_count \
         FROM nasa_saga_catalog_replica WHERE service_identity = ? AND ready = 1 \
         AND lease_until_ms >= ?",
    )
    .bind(generation)
    .bind(gate.snapshot_digest())
    .bind(gate.activation_contract_digest())
    .bind(gate.orchestrator_service_identity().as_str())
    .bind(now_ms)
    .fetch_one(&mut **transaction)
    .await?;
    let replicas: i64 = row.try_get("replica_count")?;
    let mismatches: i64 = row.try_get("mismatch_count")?;
    if replicas <= 0 || mismatches != 0 {
        return Err(DefinitionCatalogError::FailedPrecondition.into());
    }
    Ok(())
}

/// 业务作用：复验 definition 发布主体和 artifact owner 一致，避免请求字段自行授予权限。
///
/// 参数说明：`actor` 来自认证层，`artifact` 携带声明 owner 与租户。
///
/// 返回：身份与租户合法且相等时成功；否则在数据库写入前拒绝。
fn validate_definition_actor(
    actor: &ServiceIdentity,
    artifact: &DefinitionArtifact,
) -> anyhow::Result<()> {
    TenantId::new(&artifact.tenant).map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    let owner = ServiceIdentity::new(&artifact.workflow_owner)
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    if &owner != actor {
        return Err(DefinitionCatalogError::PermissionDenied.into());
    }
    Ok(())
}

/// 业务作用：复验 capability 的 owner、步骤键、transport、完整 route 与租约建议均来自合法封闭值。
///
/// 参数说明：`actor` 来自认证层，`descriptor` 是参与方请求体。
///
/// 返回：认证 owner 与描述完全一致且字段有界时成功；否则不接触数据库。
fn validate_capability_actor(
    actor: &ServiceIdentity,
    descriptor: &CapabilityDescriptor,
) -> anyhow::Result<()> {
    TenantId::new(&descriptor.tenant).map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    let owner = ServiceIdentity::new(&descriptor.owner)
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    if &owner != actor {
        return Err(DefinitionCatalogError::PermissionDenied.into());
    }
    ServiceIdentity::new(&descriptor.replica_identity)
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    nasaga_core::WorkflowName::new(&descriptor.workflow)
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    nasaga_core::DefinitionVersion::new(descriptor.definition_version)
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    nasaga_core::StepName::new(&descriptor.step)
        .map_err(|_| DefinitionCatalogError::InvalidArgument)?;
    if !matches!(
        descriptor.transport.as_str(),
        "http" | "grpc" | "kafka" | "redis-stream"
    ) || descriptor.endpoint.is_empty()
        || descriptor.route_generation == 0
        || descriptor.requested_lease_ms == 0
    {
        return Err(DefinitionCatalogError::InvalidArgument.into());
    }
    validate_capability_route_contract(descriptor)?;
    Ok(())
}

/// 业务作用：读取 MySQL 服务端当前毫秒，所有租约与激活判断使用同一时钟权威。
///
/// 参数说明：`connection` 是当前连接或事务。
///
/// 返回：非负 Unix 毫秒；数据库失败或时钟异常返回错误。
async fn database_now_ms<'e, E>(connection: E) -> anyhow::Result<i64>
where
    E: sqlx::Executor<'e, Database = sqlx::MySql>,
{
    let now_ms: i64 =
        sqlx::query_scalar("SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)")
            .fetch_one(connection)
            .await?;
    anyhow::ensure!(now_ms >= 0, "database clock is outside the supported range");
    Ok(now_ms)
}

/// 业务作用：把数据库生命周期文本恢复为封闭枚举，拒绝未知持久状态。
///
/// 参数说明：`value` 是 Catalog 行中的 lifecycle。
///
/// 返回：已知状态返回枚举；未知文本返回结构漂移错误。
fn parse_lifecycle(value: &str) -> anyhow::Result<DefinitionLifecycle> {
    match value {
        "candidate" => Ok(DefinitionLifecycle::Candidate),
        "active" => Ok(DefinitionLifecycle::Active),
        "deprecated" => Ok(DefinitionLifecycle::Deprecated),
        "retired" => Ok(DefinitionLifecycle::Retired),
        _ => anyhow::bail!("definition catalog contains an unknown lifecycle"),
    }
}

/// 业务作用：构造 definition 审计对象键，不包含 payload 或凭据。
///
/// 参数说明：`artifact` 提供租户、workflow 与版本。
///
/// 返回：可稳定检索的对象键。
fn definition_key(artifact: &DefinitionArtifact) -> String {
    format!(
        "{}/{}/{}",
        artifact.tenant, artifact.workflow, artifact.definition_version
    )
}

/// 业务作用：构造 capability 审计对象键，区分 owner 副本和步骤版本。
///
/// 参数说明：`descriptor` 提供能力主键字段。
///
/// 返回：不含 endpoint 的稳定对象键。
fn capability_key(descriptor: &CapabilityDescriptor) -> String {
    format!(
        "{}/{}/{}/{}/{}/{}",
        descriptor.tenant,
        descriptor.owner,
        descriptor.replica_identity,
        descriptor.workflow,
        descriptor.definition_version,
        descriptor.step
    )
}

/// 业务作用：把控制面变更审计与其 generation 状态变化写入同一事务。
///
/// 参数说明：事务、generation、主体、动作、对象键与摘要共同形成不可分审计事实。
///
/// 返回：审计行写入成功时完成；失败使外层控制面事务整体回滚。
async fn insert_audit(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
    generation: u64,
    actor: &str,
    action: &str,
    object_key: &str,
    digest: &str,
    operation: Option<&DefinitionLifecycleOperation>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO nasa_saga_catalog_audit \
         (catalog_generation, actor, action, object_key, object_digest, operation_id, reason) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(generation)
    .bind(actor)
    .bind(action)
    .bind(object_key)
    .bind(digest)
    .bind(operation.map(DefinitionLifecycleOperation::operation_id))
    .bind(operation.map(DefinitionLifecycleOperation::reason))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
