//! DB migration 版本门禁。
//!
//! 业务 migration 属**业务 schema**,不放进共享 runtime;本 crate 提供 MySQL session 上的**门禁**:
//! 给定业务嵌入的 [`Migrator`](`sqlx::migrate!("./migrations")` 或运行期 `Migrator::new(path)`)与配置,
//! 在 listener Ready **之前**按 mode 裁决:
//!
//! - `disabled`:跳过(既不校验也不应用)。
//! - `validate`(生产默认):**只读校验**——嵌入的每条 up migration 必须已按相同 checksum 应用;有未应用
//!   或 checksum 不符即失败,**绝不改 schema**。用于生产 Pod:schema 由专门 migration Job/本地 apply 推进,
//!   业务实例只确认版本一致。
//! - `apply`:应用未决 migration(仅本地/单实例/专门 Job)。
//!
//! 失败只输出**版本号与稳定 reason**,不输出 SQL 正文。多 datasource 由调用方分别登记、限制顺序。
//! 池获取按 `purpose=migration` 记录，取得连接即结束等待计时，不包含锁竞争、catalog 查询或 DDL。
//! [`run_gate_for`] 保留命名数据源身份；等待通知不改变门禁结果或原始锁预算。

#![forbid(unsafe_code)]

use sqlx::{pool::PoolConnection, MySql, MySqlConnection, MySqlPool, Row as _};

pub use namigrate_core::{
    AppliedMigration, EmbeddedMigration, MigrationComparison, MigrationError, MigrationMode,
    MigrationReport, MigrationSettings, MAX_MIGRATION_LOCK_TIMEOUT_MS,
};

/// 业务嵌入式 migrator 类型(`sqlx::migrate::Migrator` 的重导出)。
///
/// 业务用 `sqlx::migrate!("./migrations")` 构造它并经门面 `Application::configure_migrations`
/// 登记;`napp`/`nasa` 只需按名字接收本类型,不必各自再直依赖 `sqlx`(第三方类型只经本 crate
/// 与门面收敛穿透一次)。它是嵌入式常量数据,`Send + Sync + 'static`,可跨阶段存放。
pub use sqlx::migrate::Migrator;

/// 业务作用: 把任意数据库细节收敛为不泄露 SQL、schema 或凭据的稳定错误。
fn backend<E>(_error: E) -> MigrationError {
    MigrationError::Backend("database error".to_owned())
}

/// 业务作用: 提取不含 SQL 正文的 MySQL up migration 描述供 core 执行一致性比较。
///
/// # 参数
/// - `migrator`: 当前业务嵌入式 migrator。
///
/// 返回: 按版本升序排列的版本、checksum、可逆性与事务属性。
fn embedded_ups(migrator: &Migrator) -> Vec<EmbeddedMigration> {
    let mut ups: Vec<EmbeddedMigration> = migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| EmbeddedMigration {
            version: migration.version,
            checksum: migration.checksum.to_vec(),
            reversible: migration.migration_type.is_reversible(),
            transactional: !(migrator.no_tx || migration.no_tx),
        })
        .collect();
    ups.sort_by_key(|migration| migration.version);
    ups
}

/// 业务作用: 查询 MySQL 已应用与未完成 migration；catalog 不存在时保持首次启动语义。
///
/// # 参数
/// - `connection`: 当前 datasource 的专有 MySQL 连接。
///
/// 返回: 按版本排序的 catalog 记录；表不存在返回空集合，查询失败返回脱敏错误。
async fn applied_state(
    connection: &mut MySqlConnection,
) -> Result<Vec<AppliedMigration>, MigrationError> {
    // 表不存在(从未 apply 过)→ 返回空表,交由上层按"全部未应用"处理。
    let exists: i64 = sqlx::query(
        "SELECT COUNT(*) AS n FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name = '_sqlx_migrations'",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(backend)?
    .try_get("n")
    .map_err(backend)?;
    if exists == 0 {
        return Ok(Vec::new());
    }

    let rows =
        sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await
            .map_err(backend)?;
    let mut applied = Vec::with_capacity(rows.len());
    for row in rows {
        let version: i64 = row.try_get("version").map_err(backend)?;
        let checksum: Vec<u8> = row.try_get("checksum").map_err(backend)?;
        let success: bool = row.try_get("success").map_err(backend)?;
        applied.push(AppliedMigration {
            version,
            checksum,
            success,
        });
    }
    Ok(applied)
}

/// 业务作用: 与 SQLx MySQL migrator 使用同一算法计算 advisory lock ID。
///
/// SQLx 内部是 `format!("{:x}", 0x3d32ad9e * CRC32(database_name))`；在服务端计算可避免复制
/// 私有 Rust helper，同时保证其它直接使用 SQLx migrator 的进程与本门禁互斥。
async fn sqlx_lock_id(connection: &mut MySqlConnection) -> Result<String, MigrationError> {
    sqlx::query_scalar("SELECT LOWER(HEX(1026731422 * CRC32(DATABASE())))")
        .fetch_one(connection)
        .await
        .map_err(backend)
}

/// 持有 migration advisory lock 的专用池连接。
///
/// 正常完成显式释放后连接回池；错误、panic 或 future cancellation 关闭物理连接，确保 session lock
/// 不会随着 pooled connection 留在池里。
struct MigrationLock {
    connection: Option<PoolConnection<MySql>>,
    lock_id: String,
}

impl MigrationLock {
    /// 业务作用: 在单一端到端预算内取得池连接、计算 SQLx lock ID 并竞争 MySQL advisory lock。
    /// 参数说明：`pool` 为目标池，`datasource` 为冻结目录身份，`timeout_ms` 为完整取锁预算。
    /// 返回：持有专有 session 的锁；获取失败保持稳定门禁分类。
    async fn acquire(
        pool: &MySqlPool,
        datasource: &str,
        timeout_ms: u64,
    ) -> Result<Self, MigrationError> {
        let deadline = (timeout_ms != 0)
            .then(|| tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms));
        // `lock_timeout_ms` 覆盖连接池获取、握手、lock ID 计算和 GET_LOCK 全链路；若只限制
        // GET_LOCK，连接池耗尽或握手缓慢仍可能在数据库锁计时开始前无限等待。
        let connection = acquire_connection(pool, datasource, deadline, timeout_ms).await?;
        let mut guard = Self {
            connection: Some(connection),
            lock_id: String::new(),
        };
        guard.lock_id = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, sqlx_lock_id(guard.connection()))
                .await
                .map_err(|_| MigrationError::LockTimeout(timeout_ms))??,
            None => sqlx_lock_id(guard.connection()).await?,
        };
        // MySQL GET_LOCK 以秒计且接受小数；显式 0 保留旧合同，使用底层无限等待。
        let timeout_seconds = match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(MigrationError::LockTimeout(timeout_ms));
                }
                remaining.as_secs_f64()
            }
            None => -1.0,
        };
        let lock_id = guard.lock_id.clone();
        let acquire_lock = async {
            sqlx::query_scalar("SELECT GET_LOCK(?, ?)")
                .bind(lock_id)
                .bind(timeout_seconds)
                .fetch_one(guard.connection())
                .await
                .map_err(backend)
        };
        let acquired: Option<i64> = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, acquire_lock)
                .await
                .map_err(|_| MigrationError::LockTimeout(timeout_ms))??,
            None => acquire_lock.await?,
        };
        if acquired == Some(1) {
            Ok(guard)
        } else {
            guard.disarm();
            Err(MigrationError::LockTimeout(timeout_ms))
        }
    }

    /// 业务作用: 借用仍由 lock guard 独占的底层 MySQL 连接。
    fn connection(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_mut()
            .expect("migration lock connection must exist until release")
    }

    /// 业务作用: 移走连接使 Drop 不再执行 close-on-drop；仅用于确认未取得 session lock 的路径。
    fn disarm(mut self) {
        let _ = self.connection.take();
    }

    /// 业务作用: 显式释放 advisory lock；只有服务端确认释放后才允许连接安全回池。
    async fn release(mut self) {
        let lock_id = self.lock_id.clone();
        let released: Result<Option<i64>, _> = sqlx::query_scalar("SELECT RELEASE_LOCK(?)")
            .bind(lock_id)
            .fetch_one(self.connection())
            .await;
        if matches!(released, Ok(Some(1))) {
            let _ = self.connection.take();
        }
    }
}

impl Drop for MigrationLock {
    /// 业务作用: 未确认 RELEASE_LOCK 的路径关闭物理连接，防止 session lock 随池连接泄漏。
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

/// 业务作用: 复制业务 migrator 的完整设置，仅关闭 SQLx 自带的无限等待 lock；外层已经取得同 ID 的有界锁。
fn unlocked_migrator(migrator: &Migrator) -> Migrator {
    Migrator {
        migrations: migrator.migrations.clone(),
        ignore_missing: migrator.ignore_missing,
        locking: false,
        no_tx: migrator.no_tx,
        table_name: migrator.table_name.clone(),
        create_schemas: migrator.create_schemas.clone(),
    }
}

/// 业务作用: 按 `settings.mode` 运行 migration 门禁。见 crate 文档。
///
/// # 参数
/// - `pool`:目标 datasource 连接池。
/// - `migrator`:业务嵌入的 [`Migrator`]。
/// - `settings`:门禁配置。
///
/// 返回: 状态与模式要求一致时返回摘要；差异、锁超时或数据库失败时返回稳定分类。
pub async fn run_gate(
    pool: &MySqlPool,
    migrator: &Migrator,
    settings: &MigrationSettings,
) -> Result<MigrationReport, MigrationError> {
    run_gate_for(pool, "default", migrator, settings).await
}

/// 业务作用：按数据源身份运行 MySQL migration 门禁，并把真实池等待归入 migration 用途。
/// 参数说明：`pool` 是目标池，`datasource` 为冻结目录身份，`migrator` 为业务迁移，`settings` 为门禁策略。
/// 返回：门禁摘要或原有稳定失败；观测不会改变门禁结果，advisory lock 等待不计入池等待。
pub async fn run_gate_for(
    pool: &MySqlPool,
    datasource: &str,
    migrator: &Migrator,
    settings: &MigrationSettings,
) -> Result<MigrationReport, MigrationError> {
    settings.validate()?;
    let ups = embedded_ups(migrator);
    match settings.mode {
        MigrationMode::Disabled => Ok(MigrationReport {
            mode: MigrationMode::Disabled,
            embedded: ups.len(),
            applied: 0,
        }),
        MigrationMode::Validate => {
            let mut connection = acquire_connection(pool, datasource, None, 0).await?;
            let state = applied_state(&mut connection).await?;
            namigrate_core::compare_migrations(&ups, &state)
                .ensure_valid(migrator.ignore_missing)?;
            Ok(MigrationReport {
                mode: MigrationMode::Validate,
                embedded: ups.len(),
                applied: 0,
            })
        }
        MigrationMode::Apply => {
            // 先取得 SQLx-compatible 有界 advisory lock；状态读取、apply 与记录都复用同一连接。
            let mut lock =
                MigrationLock::acquire(pool, datasource, settings.lock_timeout_ms).await?;
            let state = applied_state(lock.connection()).await?;
            let comparison = namigrate_core::compare_migrations(&ups, &state);
            comparison.ensure_applicable(migrator.ignore_missing)?;
            let to_apply = comparison.pending.len();
            // 外层已锁，复制完整 migrator 设置后仅关闭 SQLx 内建的无限等待 lock。
            unlocked_migrator(migrator)
                .run_direct(None, lock.connection(), false)
                .await
                .map_err(map_migrate_err)?;
            lock.release().await;
            Ok(MigrationReport {
                mode: MigrationMode::Apply,
                embedded: ups.len(),
                applied: to_apply,
            })
        }
    }
}

/// 业务作用：在擦除 SQLx 错误前记录 migration 的真实连接池等待，保留外层锁预算。
/// 参数说明：`pool`、`datasource` 指定连接身份；`deadline`、`timeout_ms` 表达可选端到端预算。
/// 返回：池连接或脱敏门禁错误；取消由观测守卫记录，取得连接后立即结束等待计时。
async fn acquire_connection(
    pool: &MySqlPool,
    datasource: &str,
    deadline: Option<tokio::time::Instant>,
    timeout_ms: u64,
) -> Result<PoolConnection<MySql>, MigrationError> {
    use natx_core::observability::{connection_slot, AcquireOutcome, ConnectionPurpose};
    let mut observation = connection_slot(natx_core::DatabaseDriver::MySql, datasource)
        .acquire(ConnectionPurpose::Migration);
    let result = match deadline {
        Some(deadline) => match tokio::time::timeout_at(deadline, pool.acquire()).await {
            Ok(result) => result,
            Err(_) => {
                // 完整锁预算在池等待阶段耗尽，不能将它误记为 advisory lock 竞争或取消。
                observation.finish(AcquireOutcome::Timeout);
                return Err(MigrationError::LockTimeout(timeout_ms));
            }
        },
        None => pool.acquire().await,
    };
    observation.finish(match &result {
        Ok(_) => AcquireOutcome::Ok,
        Err(sqlx::Error::PoolTimedOut) => AcquireOutcome::Timeout,
        Err(sqlx::Error::PoolClosed) => AcquireOutcome::Closed,
        Err(sqlx::Error::WorkerCrashed) => AcquireOutcome::Worker,
        Err(_) => AcquireOutcome::Other,
    });
    result.map_err(backend)
}

/// 业务作用: 把 sqlx `MigrateError` 脱敏映射(checksum 漂移单列,其余归 Backend)。
fn map_migrate_err(error: sqlx::migrate::MigrateError) -> MigrationError {
    match error {
        sqlx::migrate::MigrateError::VersionMismatch(version) => {
            MigrationError::ChecksumMismatch(version)
        }
        sqlx::migrate::MigrateError::Dirty(version) => MigrationError::Dirty(version),
        _ => MigrationError::Backend("migration apply failed".to_owned()),
    }
}
