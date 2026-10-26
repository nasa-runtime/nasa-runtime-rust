//! SQLite 受管数据源的数据库迁移门禁。
//!
//! validate 只读比较 SQLx 历史；apply 在同一个 IMMEDIATE 事务中执行全部待处理脚本和历史写入。
//! 计划或历史不一致时停止，既不重放脚本，也不接受非事务迁移。
//! napp 在同一个 `db` 组件的 Prepare 阶段使用与 Mapper 相同的 datasource 和文件 owner；
//! Service 可在 UserHook 登记，Batch 通过静态计划登记，门禁失败不执行工作负载。
//! 业务显式嵌入 SQLx Migrator，配置不自动发现脚本；计划自身 SQL/checksum 一致性先于数据库访问校验。
//! 有限锁预算和批次执行预算不代替真实原生收口，跨 datasource 不提供原子迁移。

use std::time::{Duration, Instant};

use natx_sqlite::{MigrationSession, SqliteDataSource};
pub use sqlx::migrate::Migrator;

pub use namigrate_core::{MigrationError, MigrationMode, MigrationReport, MigrationSettings};

/// 业务作用：在默认有限执行预算内校验或应用文件来源的迁移计划。
/// 参数说明：
/// - `source`：与业务 Mapper 相同的受管来源。
/// - `migrator`：业务嵌入的 SQLx 计划。
/// - `settings`：门禁模式和锁预算。
/// 返回：确认完成的迁移摘要或不包含 SQL、路径的稳定错误。
pub async fn run_gate(
    source: &SqliteDataSource,
    migrator: &Migrator,
    settings: &MigrationSettings,
) -> Result<MigrationReport, MigrationError> {
    run_gate_with_timeout(source, migrator, settings, 30_000).await
}

/// 业务作用：以独立总执行预算保证整批迁移原子提交，并严格验证计划与历史的前缀关系。
/// 参数说明：
/// - `source`：业务来源句柄。
/// - `migrator`：只执行 up 项的嵌入计划。
/// - `settings`：validate、apply 或 disabled 及有限锁预算。
/// - `execution_timeout_ms`：取得迁移事务后整个批次的有限预算。
/// 返回：计划不一致时在数据库操作前拒绝；disabled 返回跳过摘要，其余模式确认历史与计划一致并物理收口后成功，失败不自动重试。
pub async fn run_gate_with_timeout(
    source: &SqliteDataSource,
    migrator: &Migrator,
    settings: &MigrationSettings,
    execution_timeout_ms: u64,
) -> Result<MigrationReport, MigrationError> {
    settings.validate()?;
    if !(1..=3_600_000).contains(&settings.lock_timeout_ms) {
        return Err(MigrationError::InvalidLockTimeout(settings.lock_timeout_ms));
    }
    if !(1..=3_600_000).contains(&execution_timeout_ms) {
        return Err(backend("invalid SQLite migration execution timeout"));
    }
    // 固定历史与整批事务是门禁权威，不能静默忽略调用方要求跳过锁、历史或事务的选项。
    if migrator.no_tx
        || migrator.ignore_missing
        || !migrator.locking
        || migrator.table_name.as_ref() != "_sqlx_migrations"
        || !migrator.create_schemas.is_empty()
    {
        return Err(backend("unsupported SQLite migrator options"));
    }
    let migrations: Vec<_> = migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .collect();
    let mut previous = None;
    let mut bytes = 0usize;
    for migration in &migrations {
        if migration.version <= 0 || previous.is_some_and(|previous| migration.version <= previous)
        {
            return Err(backend(
                "SQLite migration IDs must be positive, unique and increasing",
            ));
        }
        if migration.no_tx {
            return Err(MigrationError::NonTransactionalEvidenceRequired(
                migration.version,
            ));
        }
        previous = Some(migration.version);
        bytes = bytes.saturating_add(migration.sql.as_str().len());
    }
    if migrations.len() > 1024 || bytes > 16 * 1024 * 1024 {
        return Err(backend("SQLite migration plan exceeds supported bounds"));
    }
    // 先限制输入体积，再由 SQLx 同一构造入口重算原文摘要；公开字段不能让执行正文与历史证据分离。
    for migration in &migrations {
        let expected = sqlx::migrate::Migration::new(
            migration.version,
            migration.description.clone(),
            migration.migration_type,
            migration.sql.clone(),
            migration.no_tx,
        );
        if expected.checksum != migration.checksum {
            return Err(backend("SQLite migration SQL/checksum mismatch"));
        }
    }
    let mut report = MigrationReport {
        mode: settings.mode,
        embedded: migrations.len(),
        applied: 0,
    };
    if settings.mode == MigrationMode::Disabled {
        return Ok(report);
    }
    let apply = settings.mode == MigrationMode::Apply;
    let opened = MigrationSession::open(
        source,
        apply,
        Duration::from_millis(settings.lock_timeout_ms),
        Duration::from_millis(execution_timeout_ms),
    )
    .await;
    let mut session = opened.map_err(|_| backend("SQLite migration session acquisition failed"))?;
    let history = session
        .history()
        .await
        .map_err(|_| backend("SQLite migration history is incompatible"))?;
    let applied: Vec<_> = history
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|record| namigrate_core::AppliedMigration {
            version: record.version,
            checksum: record.checksum.clone(),
            success: record.success,
        })
        .collect();
    let embedded: Vec<_> = migrations
        .iter()
        .map(|migration| namigrate_core::EmbeddedMigration {
            version: migration.version,
            checksum: migration.checksum.to_vec(),
            reversible: false,
            transactional: true,
        })
        .collect();
    let comparison = namigrate_core::compare_migrations(&embedded, &applied);
    comparison.ensure_applicable(false)?;
    // 历史只能是当前声明的连续前缀；在已经应用的新项前补插旧迁移必须阻断。
    if applied.iter().enumerate().any(|(index, record)| {
        embedded
            .get(index)
            .is_none_or(|expected| expected.version != record.version)
    }) {
        return Err(backend(
            "SQLite migration history is not a prefix of the plan",
        ));
    }
    if !apply {
        comparison.ensure_valid(false)?;
        session
            .finish_validation()
            .await
            .map_err(|_| backend("SQLite migration validation close failed"))?;
        return Ok(report);
    }
    if history.is_none() {
        session
            .create_history()
            .await
            .map_err(|_| backend("SQLite migration history creation failed"))?;
    }
    for migration in migrations.iter().skip(applied.len()) {
        let started = Instant::now();
        session
            .execute_script(migration.sql.as_str())
            .await
            .map_err(|_| backend("SQLite migration script failed; batch was not committed"))?;
        let elapsed = i64::try_from(started.elapsed().as_nanos())
            .map_err(|_| backend("SQLite migration duration overflow"))?;
        session
            .record(
                migration.version,
                &migration.description,
                &migration.checksum,
                elapsed,
            )
            .await
            .map_err(|_| {
                backend("SQLite migration history write failed; batch was not committed")
            })?;
        report.applied += 1;
    }
    let final_history = session
        .history()
        .await
        .map_err(|_| backend("SQLite migration final history validation failed"))?
        .ok_or_else(|| backend("SQLite migration history disappeared"))?;
    if final_history.len() != embedded.len()
        || final_history
            .iter()
            .zip(&embedded)
            .any(|(actual, expected)| {
                !actual.success
                    || actual.version != expected.version
                    || actual.checksum != expected.checksum
            })
    {
        return Err(backend(
            "SQLite migration final history does not match the plan",
        ));
    }
    session.commit().await.map_err(|_| {
        backend("SQLite migration commit was not confirmed; inspect history before retrying")
    })?;
    Ok(report)
}

/// 业务作用：生成稳定迁移失败摘要，不暴露数据库文件路径或 SQL 正文。
/// 参数说明：
/// - `reason`：固定阶段或边界说明。
/// 返回：后端中立错误中的 SQLite 失败原因。
fn backend(reason: &str) -> MigrationError {
    MigrationError::Backend(reason.to_owned())
}
