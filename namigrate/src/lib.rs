//! DB migration 版本门禁。
//!
//! 业务 migration 属**业务 schema**,不放进共享 runtime;本 crate 只提供 provider-neutral 的**门禁**:
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

#![forbid(unsafe_code)]

use serde::Deserialize;
use sqlx::{pool::PoolConnection, MySql, MySqlConnection, MySqlPool, Row as _};

/// 有界 advisory-lock 等待允许的最大毫秒数；`0` 仍保留显式无限等待合同。
pub const MAX_MIGRATION_LOCK_TIMEOUT_MS: u64 = 365 * 24 * 60 * 60 * 1000;

/// 业务嵌入式 migrator 类型(`sqlx::migrate::Migrator` 的重导出)。
///
/// 业务用 `sqlx::migrate!("./migrations")` 构造它并经门面 `Application::configure_migrations`
/// 登记;`napp`/`nasa` 只需按名字接收本类型,不必各自再直依赖 `sqlx`(第三方类型只经本 crate
/// 与门面收敛穿透一次)。它是嵌入式常量数据,`Send + Sync + 'static`,可跨阶段存放。
pub use sqlx::migrate::Migrator;

/// migration 门禁模式(配置 `database.migrations.mode`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MigrationMode {
    /// 跳过(不校验不应用)。
    Disabled,
    /// 只读校验:嵌入 migration 必须已全部应用且 checksum 一致(生产默认)。
    #[default]
    Validate,
    /// 应用未决 migration(本地/单实例/专门 Job)。
    Apply,
}

/// migration 配置(`database.migrations`)。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MigrationSettings {
    /// 门禁模式;默认 `validate`(生产安全)。
    pub mode: MigrationMode,
    /// 获取 migration 锁的等待上限毫秒(apply 时用;`0` 表示用底层默认)。
    pub lock_timeout_ms: u64,
    /// 是否允许在 dirty(上次 apply 中断)状态下继续;默认否。
    pub allow_dirty: bool,
}

impl Default for MigrationSettings {
    /// 业务作用: 使用生产保守的 validate 模式、30 秒锁预算并拒绝 dirty override。
    fn default() -> Self {
        Self {
            mode: MigrationMode::default(),
            lock_timeout_ms: 30_000,
            allow_dirty: false,
        }
    }
}

impl MigrationSettings {
    /// 业务作用: 校验安全合同。
    ///
    /// `allow_dirty=true` 不能通用地解释成“从失败处继续”：MySQL DDL 可能已经部分提交，runtime
    /// 无法推断 schema 的真实修复点。该旋钮保留用于给旧配置稳定报错，调用方必须先人工修复并删除
    /// dirty 记录，而不是让框架盲目续跑。
    ///
    /// # 错误
    ///
    /// dirty override 或无法安全表示为绝对 deadline 的锁等待配置会被拒绝。
    pub fn validate(&self) -> Result<(), MigrationError> {
        if self.allow_dirty {
            return Err(MigrationError::DirtyOverrideUnsupported);
        }
        if self.lock_timeout_ms > MAX_MIGRATION_LOCK_TIMEOUT_MS {
            return Err(MigrationError::InvalidLockTimeout(self.lock_timeout_ms));
        }
        Ok(())
    }
}

/// 门禁结果(稳定摘要,不含 SQL)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// 实际执行的模式。
    pub mode: MigrationMode,
    /// 嵌入的 up migration 总数。
    pub embedded: usize,
    /// 本次应用的 migration 数(apply 模式;validate/disabled 为 0)。
    pub applied: usize,
}

/// migration 门禁失败(只含版本与稳定 reason,不含 SQL 正文)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationError {
    /// `validate`:存在未应用的 migration(附缺失版本号,升序)。
    Pending(Vec<i64>),
    /// `validate`/`apply`:某版本已应用记录的 checksum 与嵌入不符(schema 漂移)。
    ChecksumMismatch(i64),
    /// migration 表存在失败记录；必须先人工检查/修复部分 DDL。
    Dirty(i64),
    /// `apply` 未在配置上限内取得 SQLx 兼容的数据库 advisory lock。
    LockTimeout(u64),
    /// `allow_dirty=true` 不具备可通用证明的安全语义，明确拒绝而不是静默忽略。
    DirtyOverrideUnsupported,
    /// 有界锁等待超出框架可安全表示的 deadline。
    InvalidLockTimeout(u64),
    /// 底层 DB/migrator 错误(脱敏,不含 SQL)。
    Backend(String),
}

impl std::fmt::Display for MigrationError {
    /// 业务作用: 输出版本号与稳定失败分类，不包含 migration SQL 或数据库连接信息。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Pending(versions) => {
                write!(
                    formatter,
                    "migrations not applied (validate): versions {versions:?}"
                )
            }
            MigrationError::ChecksumMismatch(version) => write!(
                formatter,
                "migration checksum mismatch at version {version} (schema drift)"
            ),
            MigrationError::Dirty(version) => write!(
                formatter,
                "migration {version} is partially applied (dirty); repair it before startup"
            ),
            MigrationError::LockTimeout(timeout_ms) => write!(
                formatter,
                "migration lock was not acquired within {timeout_ms}ms"
            ),
            MigrationError::DirtyOverrideUnsupported => write!(
                formatter,
                "allow_dirty=true is unsafe and unsupported; repair the dirty migration explicitly"
            ),
            MigrationError::InvalidLockTimeout(timeout_ms) => write!(
                formatter,
                "migration lock timeout {timeout_ms}ms exceeds the framework hard limit"
            ),
            MigrationError::Backend(reason) => {
                write!(formatter, "migration backend error: {reason}")
            }
        }
    }
}

impl std::error::Error for MigrationError {}

/// 业务作用: 把任意数据库细节收敛为不泄露 SQL、schema 或凭据的稳定错误。
fn backend<E>(_error: E) -> MigrationError {
    MigrationError::Backend("database error".to_owned())
}

/// 业务作用: 嵌入的 up migration:`(version, checksum)`,按 version 升序。
fn embedded_ups(migrator: &Migrator) -> Vec<(i64, Vec<u8>)> {
    let mut ups: Vec<(i64, Vec<u8>)> = migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| (migration.version, migration.checksum.to_vec()))
        .collect();
    ups.sort_by_key(|(version, _)| *version);
    ups
}

/// 数据库当前 migration 状态。
struct AppliedState {
    applied: std::collections::HashMap<i64, Vec<u8>>,
    dirty: Option<i64>,
}

/// 业务作用: 查询已应用/dirty migration；`_sqlx_migrations` 不存在视为空。
async fn applied_state(connection: &mut MySqlConnection) -> Result<AppliedState, MigrationError> {
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
        return Ok(AppliedState {
            applied: std::collections::HashMap::new(),
            dirty: None,
        });
    }

    let rows =
        sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await
            .map_err(backend)?;
    let mut applied = std::collections::HashMap::with_capacity(rows.len());
    let mut dirty = None;
    for row in rows {
        let version: i64 = row.try_get("version").map_err(backend)?;
        let checksum: Vec<u8> = row.try_get("checksum").map_err(backend)?;
        let success: bool = row.try_get("success").map_err(backend)?;
        if success {
            applied.insert(version, checksum);
        } else if dirty.is_none() {
            dirty = Some(version);
        }
    }
    Ok(AppliedState { applied, dirty })
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
    async fn acquire(pool: &MySqlPool, timeout_ms: u64) -> Result<Self, MigrationError> {
        let deadline = (timeout_ms != 0)
            .then(|| tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms));
        // `lock_timeout_ms` 覆盖连接池获取、握手、lock ID 计算和 GET_LOCK 全链路；若只限制
        // GET_LOCK，连接池耗尽或握手缓慢仍可能在数据库锁计时开始前无限等待。
        let connection = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, pool.acquire())
                .await
                .map_err(|_| MigrationError::LockTimeout(timeout_ms))?
                .map_err(backend)?,
            None => pool.acquire().await.map_err(backend)?,
        };
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
pub async fn run_gate(
    pool: &MySqlPool,
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
            let mut connection = pool.acquire().await.map_err(backend)?;
            let state = applied_state(&mut connection).await?;
            if let Some(version) = state.dirty {
                return Err(MigrationError::Dirty(version));
            }
            let mut pending = Vec::new();
            for (version, checksum) in &ups {
                match state.applied.get(version) {
                    None => pending.push(*version),
                    Some(existing) if existing != checksum => {
                        return Err(MigrationError::ChecksumMismatch(*version));
                    }
                    Some(_) => {}
                }
            }
            if pending.is_empty() {
                Ok(MigrationReport {
                    mode: MigrationMode::Validate,
                    embedded: ups.len(),
                    applied: 0,
                })
            } else {
                pending.sort_unstable();
                Err(MigrationError::Pending(pending))
            }
        }
        MigrationMode::Apply => {
            // 先取得 SQLx-compatible 有界 advisory lock；状态读取、apply 与记录都复用同一连接。
            let mut lock = MigrationLock::acquire(pool, settings.lock_timeout_ms).await?;
            let state = applied_state(lock.connection()).await?;
            if let Some(version) = state.dirty {
                return Err(MigrationError::Dirty(version));
            }
            let to_apply = ups
                .iter()
                .filter(|(version, _)| !state.applied.contains_key(version))
                .count();
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
