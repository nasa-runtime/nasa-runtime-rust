//! PostgreSQL migration 门禁。
//!
//! advisory lock、catalog、apply/validate 和 unlock 始终绑定同一物理 session。池入口只适用于直连或
//! 会话级池化；事务级池化必须使用独立的 session-affine endpoint，并在取锁前通过
//! [`verify_target_identity`] 确认它与业务 pool 的 database/schema 身份一致。

#![forbid(unsafe_code)]

use async_trait::async_trait;
use sqlx::migrate::Migrate as _;
use sqlx::pool::PoolConnection;
use sqlx::{Connection as _, PgConnection, PgPool, Postgres, Row as _};

pub use namigrate_core::{
    AppliedMigration, EmbeddedMigration, MigrationComparison, MigrationError, MigrationMode,
    MigrationReport, MigrationSettings, MAX_MIGRATION_LOCK_TIMEOUT_MS,
};
pub use sqlx::migrate::Migrator;

/// 非事务 migration 的完成证据合同。
///
/// 典型实现查询 `pg_index.indisvalid` 或等价 catalog 事实。探针必须只读，并精确证明该版本声明的全部
/// 外部副作用已经完成；仅证明对象存在不足以确认并发索引可用。
#[async_trait]
pub trait NonTransactionalEvidence: Send + Sync {
    /// 业务作用: 返回本探针负责裁决的 migration 版本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 与嵌入式 migration 完全一致的版本号。
    fn version(&self) -> i64;

    /// 业务作用: 返回该非事务阶段独立于 advisory lock 的最大执行毫秒数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 大于零且不超过公共硬上限的执行预算。
    fn timeout_ms(&self) -> u64;

    /// 业务作用: 从 PostgreSQL catalog 证明该版本的全部外部副作用已经完整可用。
    ///
    /// # 参数
    /// - `connection`: 与 advisory lock、migration 执行相同的专有物理连接。
    ///
    /// 返回: 证据完整时为 `true`；证据不足为 `false`；查询失败返回脱敏门禁错误。
    async fn is_complete(&self, connection: &mut PgConnection) -> Result<bool, MigrationError>;
}

/// 业务作用: 使用稳定 FNV-1a 算法从 database 与 schema 身份生成 PostgreSQL advisory lock key。
///
/// # 参数
/// - `database`: PostgreSQL `current_database()` 返回值。
/// - `schema`: 已通过有界 identifier 校验的受管 schema。
///
/// 返回: 跨进程、跨重启稳定的有符号 64 位 lock key。
pub fn advisory_lock_key(database: &str, schema: &str) -> i64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in b"nasa-runtime:namigrate-pgsql:v1\0"
        .iter()
        .chain(database.as_bytes())
        .chain(std::iter::once(&0_u8))
        .chain(schema.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::from_ne_bytes(hash.to_ne_bytes())
}

/// 业务作用: 通过池中一条专有连接执行 PostgreSQL migration 门禁。
///
/// 调用方必须保证 pool 直连 PostgreSQL 或经过会话级池化代理；事务级池化不满足 session advisory lock
/// 的持有语义。函数在 unlock 回包未确认、取消或 panic 时关闭物理连接，不让可能持锁的 session 回池。
///
/// # 参数
/// - `pool`: 目标 PostgreSQL datasource 连接池。
/// - `schema`: migration 表与业务对象所属的受管 schema。
/// - `migrator`: 业务嵌入式 SQLx migrator。
/// - `settings`: 门禁模式与锁等待预算。
///
/// 返回: 校验或应用成功时给出不含 SQL 的摘要；失败返回稳定分类。
pub async fn run_gate(
    pool: &PgPool,
    schema: &str,
    migrator: &Migrator,
    settings: &MigrationSettings,
) -> Result<MigrationReport, MigrationError> {
    run_gate_with_evidence(pool, schema, migrator, settings, &[]).await
}

/// 业务作用: 使用显式非事务完成证据执行 PostgreSQL migration 门禁。
///
/// # 参数
/// - `pool`: 只允许直连或会话级池化的 PostgreSQL pool。
/// - `schema`: 受管 schema。
/// - `migrator`: 业务嵌入式 migrator。
/// - `settings`: 门禁配置。
/// - `evidence`: 按版本提供的非事务完成探针。
///
/// 返回: 所有状态与完成证据成立时返回摘要；证据缺失或不成立时阻断。
pub async fn run_gate_with_evidence(
    pool: &PgPool,
    schema: &str,
    migrator: &Migrator,
    settings: &MigrationSettings,
    evidence: &[&dyn NonTransactionalEvidence],
) -> Result<MigrationReport, MigrationError> {
    settings.validate()?;
    validate_schema(schema)?;
    if settings.mode == MigrationMode::Disabled {
        return Ok(disabled_report(migrator));
    }

    let deadline = deadline(settings.lock_timeout_ms);
    let connection = wait_until(deadline, settings.lock_timeout_ms, pool.acquire()).await?;
    let mut session = PooledSession::new(connection);
    let mut safe_to_reuse = false;
    let result = run_on_session(
        session.connection(),
        schema,
        migrator,
        settings,
        evidence,
        deadline,
        &mut safe_to_reuse,
    )
    .await;
    if safe_to_reuse {
        session.return_to_pool();
    }
    result
}

/// 业务作用: 消费一条专有 PostgreSQL 连接执行 migration 门禁并在结束后关闭该 session。
///
/// 该入口适合事务级业务代理旁路出的直连或会话级 migration endpoint。连接不会加入业务 PgPool，
/// database/schema 身份应由调用方在进入本函数前与业务 pool 复验一致。
///
/// # 参数
/// - `connection`: 已建立的专有 session-affine PostgreSQL 连接。
/// - `schema`: 与业务 datasource 一致的受管 schema。
/// - `migrator`: 业务嵌入式 migrator。
/// - `settings`: 门禁配置。
/// - `evidence`: 非事务 migration 完成探针。
///
/// 返回: 门禁结果；无论结果如何，函数都消费并关闭传入连接。
pub async fn run_gate_on_connection(
    mut connection: PgConnection,
    schema: &str,
    migrator: &Migrator,
    settings: &MigrationSettings,
    evidence: &[&dyn NonTransactionalEvidence],
) -> Result<MigrationReport, MigrationError> {
    settings.validate()?;
    validate_schema(schema)?;
    if settings.mode == MigrationMode::Disabled {
        connection.close().await.map_err(backend)?;
        return Ok(disabled_report(migrator));
    }
    let deadline = deadline(settings.lock_timeout_ms);
    let mut unlocked = false;
    let result = run_on_session(
        &mut connection,
        schema,
        migrator,
        settings,
        evidence,
        deadline,
        &mut unlocked,
    )
    .await;
    let close_result = connection.close().await.map_err(backend);
    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(report), Ok(())) => Ok(report),
    }
}

/// 业务作用: 对专用 endpoint 与业务 pool 的 database/schema 身份执行取锁前复验。
///
/// # 参数
/// - `business_pool`: 正常承载业务请求的 PostgreSQL pool。
/// - `migration_connection`: 直连或会话级 migration endpoint 的专有连接。
/// - `schema`: 两侧都应使用的受管 schema。
///
/// 返回: 两侧 database 和 schema 均一致时成功；任何差异均在 advisory lock 前拒绝。
pub async fn verify_target_identity(
    business_pool: &PgPool,
    migration_connection: &mut PgConnection,
    schema: &str,
) -> Result<(), MigrationError> {
    validate_schema(schema)?;
    let mut business = business_pool.acquire().await.map_err(backend)?;
    let business_identity = target_identity(&mut business, schema).await?;
    let migration_identity = target_identity(migration_connection, schema).await?;
    if business_identity == migration_identity {
        Ok(())
    } else {
        Err(MigrationError::TargetMismatch)
    }
}

/// 池连接守卫；只有 unlock 被服务端确认后才允许连接回池。
struct PooledSession {
    connection: Option<PoolConnection<Postgres>>,
}

impl PooledSession {
    /// 业务作用: 把新取得的池连接置于默认 close-on-drop 保护态。
    ///
    /// # 参数
    /// - `connection`: 尚未执行 advisory lock 的专用池连接。
    ///
    /// 返回: 未显式确认安全前不会把连接还回池的守卫。
    fn new(connection: PoolConnection<Postgres>) -> Self {
        Self {
            connection: Some(connection),
        }
    }

    /// 业务作用: 借用锁、catalog 和执行共同使用的物理 PostgreSQL 连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 守卫当前独占的可变连接引用。
    fn connection(&mut self) -> &mut PgConnection {
        self.connection
            .as_mut()
            .expect("migration session must own its connection")
    }

    /// 业务作用: 在服务端确认 unlock 后解除 close-on-drop，让健康连接正常回池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 无；连接所有权交回 SQLx pool。
    fn return_to_pool(mut self) {
        let _ = self.connection.take();
    }
}

impl Drop for PooledSession {
    /// 业务作用: 在取消、panic 或 unlock 未确认时关闭物理连接，依靠 session 终止释放 advisory lock。
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

/// 业务作用: 在同一 session 内完成 identity、锁、catalog、裁决、执行和 unlock 的完整时序。
///
/// # 参数
/// - `connection`: 全流程独占的 PostgreSQL 连接。
/// - `schema`: 受管 schema。
/// - `migrator`: 业务 migrator。
/// - `settings`: 门禁配置。
/// - `evidence`: 非事务完成探针。
/// - `deadline`: 获取连接前开始计算的统一锁预算。
/// - `safe_to_reuse`: 仅在服务端确认 unlock 后置为 `true`。
///
/// 返回: 门禁摘要或稳定失败分类。
async fn run_on_session(
    connection: &mut PgConnection,
    schema: &str,
    migrator: &Migrator,
    settings: &MigrationSettings,
    evidence: &[&dyn NonTransactionalEvidence],
    deadline: Option<tokio::time::Instant>,
    safe_to_reuse: &mut bool,
) -> Result<MigrationReport, MigrationError> {
    ensure_default_table(migrator)?;
    let (database, actual_schema) = target_identity(connection, schema).await?;
    let original_search_path = sqlx::query_scalar::<_, String>("SHOW search_path")
        .fetch_one(&mut *connection)
        .await
        .map_err(backend)?;
    sqlx::query_scalar::<_, String>("SELECT set_config('search_path', $1, false)")
        .bind(schema)
        .fetch_one(&mut *connection)
        .await
        .map_err(backend)?;
    let lock_key = advisory_lock_key(&database, &actual_schema);
    acquire_lock(connection, lock_key, deadline, settings.lock_timeout_ms).await?;

    let body = match settings.mode {
        MigrationMode::Disabled => {
            unreachable!("disabled mode returns before acquiring a connection")
        }
        MigrationMode::Validate => validate_on_connection(connection, migrator).await,
        MigrationMode::Apply => apply_on_connection(connection, migrator, evidence).await,
    };

    if matches!(body, Err(MigrationError::NonTransactionalTimeout(_, _))) {
        // 非事务 DDL 超时后服务端是否仍在执行无法由客户端证明；继续复用 session 或排队发送 unlock
        // 都会把独立执行预算变成无界等待。保持保护态并关闭物理连接，由 session 终止释放锁；
        // 下次启动只依据显式 catalog 证据决定补记或续作。
        return body;
    }
    // 只有服务端明确返回 `true` 才能把 session 视为无锁；回包丢失与 `false` 都保持保护态并关闭连接。
    let unlocked = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .fetch_one(&mut *connection)
        .await
        .map_err(backend)?;
    if !unlocked {
        return Err(MigrationError::Backend(
            "advisory unlock was not confirmed".to_owned(),
        ));
    }
    // pool 连接在返回业务流量前恢复调用方原 search_path，避免 migration schema 污染后续请求。
    sqlx::query_scalar::<_, String>("SELECT set_config('search_path', $1, false)")
        .bind(original_search_path)
        .fetch_one(&mut *connection)
        .await
        .map_err(backend)?;
    *safe_to_reuse = true;
    body
}

/// 业务作用: 读取 catalog 并执行只读 migration 一致性裁决。
///
/// # 参数
/// - `connection`: 已持有 advisory lock 的 PostgreSQL 连接。
/// - `migrator`: 当前二进制携带的 migration 声明。
///
/// 返回: 完全一致时返回 validate 摘要；差异按 core 稳定分类返回。
async fn validate_on_connection(
    connection: &mut PgConnection,
    migrator: &Migrator,
) -> Result<MigrationReport, MigrationError> {
    let embedded = embedded_ups(migrator);
    let applied = applied_state(connection).await?;
    namigrate_core::compare_migrations(&embedded, &applied)
        .ensure_valid(migrator.ignore_missing)?;
    Ok(MigrationReport {
        mode: MigrationMode::Validate,
        embedded: embedded.len(),
        applied: 0,
    })
}

/// 业务作用: 在已持锁 session 中按版本顺序执行事务 migration，并用显式证据裁决非事务阶段。
///
/// # 参数
/// - `connection`: 已持有 advisory lock 的 PostgreSQL 连接。
/// - `migrator`: 当前业务 migrator。
/// - `evidence`: 非事务版本对应的完成探针。
///
/// 返回: 所有未决版本完成并登记后返回 apply 摘要；中断或证据不足时保持可恢复状态并失败。
async fn apply_on_connection(
    connection: &mut PgConnection,
    migrator: &Migrator,
    evidence: &[&dyn NonTransactionalEvidence],
) -> Result<MigrationReport, MigrationError> {
    for schema in migrator.create_schemas.iter() {
        connection
            .create_schema_if_not_exists(schema)
            .await
            .map_err(map_migrate_err)?;
    }
    connection
        .ensure_migrations_table(&migrator.table_name)
        .await
        .map_err(map_migrate_err)?;

    let embedded = embedded_ups(migrator);
    let initial = applied_state(connection).await?;
    let comparison = namigrate_core::compare_migrations(&embedded, &initial);
    comparison.ensure_applicable(migrator.ignore_missing)?;
    let expected_applied = comparison.pending.len();

    for migration in migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
    {
        let current = applied_state(connection).await?;
        if current
            .iter()
            .any(|applied| applied.version == migration.version && applied.success)
        {
            continue;
        }
        if migrator.no_tx || migration.no_tx {
            apply_non_transactional(connection, migration, evidence).await?;
        } else {
            connection
                .apply(&migrator.table_name, migration)
                .await
                .map_err(map_migrate_err)?;
        }
    }

    Ok(MigrationReport {
        mode: MigrationMode::Apply,
        embedded: embedded.len(),
        applied: expected_applied,
    })
}

/// 业务作用: 执行可由 catalog 证据恢复的非事务 migration，并在证据成立后登记 checksum。
///
/// # 参数
/// - `connection`: 已持有 advisory lock 的专有 session。
/// - `migration`: 当前非事务 migration，SQL 只交给数据库执行，不进入错误输出。
/// - `evidence`: 调用方提供的版本化完成探针集合。
///
/// 返回: 既有或新产生的完成证据成立并成功登记时成功；缺证据、超时或执行失败时阻断。
async fn apply_non_transactional(
    connection: &mut PgConnection,
    migration: &sqlx::migrate::Migration,
    evidence: &[&dyn NonTransactionalEvidence],
) -> Result<(), MigrationError> {
    let probe = evidence
        .iter()
        .copied()
        .find(|probe| probe.version() == migration.version)
        .ok_or(MigrationError::NonTransactionalEvidenceRequired(
            migration.version,
        ))?;
    let timeout_ms = probe.timeout_ms();
    if timeout_ms == 0 || timeout_ms > MAX_MIGRATION_LOCK_TIMEOUT_MS {
        return Err(MigrationError::InvalidLockTimeout(timeout_ms));
    }

    let stage = async {
        if !probe.is_complete(connection).await? {
            sqlx::raw_sql(migration.sql.clone())
                .execute(&mut *connection)
                .await
                .map_err(backend)?;
            if !probe.is_complete(connection).await? {
                return Err(MigrationError::CompletionEvidenceMissing(migration.version));
            }
        }
        // 完成证据先于 migration 记录发布；进程在两者之间退出时，下次运行可依证据安全补记。
        connection
            .skip("_sqlx_migrations", migration)
            .await
            .map_err(map_migrate_err)?;
        Ok(())
    };
    tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), stage)
        .await
        .map_err(|_| MigrationError::NonTransactionalTimeout(migration.version, timeout_ms))?
}

/// 业务作用: 查询当前 schema 下的 SQLx migration catalog；表不存在视为尚未应用。
///
/// # 参数
/// - `connection`: 已设置受管 `search_path` 的 PostgreSQL session。
///
/// 返回: 按版本排序的数据库执行记录，不包含 SQL 正文。
async fn applied_state(
    connection: &mut PgConnection,
) -> Result<Vec<AppliedMigration>, MigrationError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (\
           SELECT 1 FROM pg_catalog.pg_class c \
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
           WHERE n.nspname = current_schema() AND c.relname = '_sqlx_migrations'\
         )",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(backend)?;
    if !exists {
        return Ok(Vec::new());
    }
    let rows =
        sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await
            .map_err(backend)?;
    rows.into_iter()
        .map(|row| {
            Ok(AppliedMigration {
                version: row.try_get("version").map_err(backend)?,
                checksum: row.try_get("checksum").map_err(backend)?,
                success: row.try_get("success").map_err(backend)?,
            })
        })
        .collect()
}

/// 业务作用: 读取当前 database 并确认受管 schema 在该 database 中存在。
///
/// # 参数
/// - `connection`: 需要检查目标身份的 PostgreSQL session。
/// - `schema`: 已通过普通 identifier 约束的 schema。
///
/// 返回: 服务端确认的 `(database, schema)` 身份；schema 不存在时拒绝且不改变 session 配置。
async fn target_identity(
    connection: &mut PgConnection,
    schema: &str,
) -> Result<(String, String), MigrationError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)",
    )
    .bind(schema)
    .fetch_one(&mut *connection)
    .await
    .map_err(backend)?;
    if !exists {
        return Err(MigrationError::SchemaNotFound);
    }
    let row = sqlx::query("SELECT current_database() AS database")
        .fetch_one(connection)
        .await
        .map_err(backend)?;
    let database: String = row.try_get("database").map_err(backend)?;
    Ok((database, schema.to_owned()))
}

/// 业务作用: 在统一绝对 deadline 内循环竞争 session advisory lock。
///
/// # 参数
/// - `connection`: 后续 catalog 和执行也会复用的物理连接。
/// - `lock_key`: database/schema 稳定身份生成的锁 key。
/// - `deadline`: 获取 pool 连接前开始计算的绝对截止时间。
/// - `timeout_ms`: 用于稳定超时错误的原始配置值。
///
/// 返回: 服务端确认持锁时成功；预算耗尽时返回 `LockTimeout`。
async fn acquire_lock(
    connection: &mut PgConnection,
    lock_key: i64,
    deadline: Option<tokio::time::Instant>,
    timeout_ms: u64,
) -> Result<(), MigrationError> {
    loop {
        let acquired = wait_until(
            deadline,
            timeout_ms,
            sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
                .bind(lock_key)
                .fetch_one(&mut *connection),
        )
        .await?;
        if acquired {
            return Ok(());
        }
        match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(
                    deadline,
                    tokio::time::sleep(std::time::Duration::from_millis(25)),
                )
                .await
                .map_err(|_| MigrationError::LockTimeout(timeout_ms))?;
            }
            None => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
        }
    }
}

/// 业务作用: 在可选绝对 deadline 内等待单个连接或数据库动作。
///
/// # 参数
/// - `deadline`: `None` 表示调用方显式选择无截止时间。
/// - `timeout_ms`: 超时时进入稳定错误的配置值。
/// - `future`: 当前需要受统一预算约束的异步动作。
///
/// 返回: 动作结果；deadline 到达时统一映射为 `LockTimeout`。
async fn wait_until<F, T, E>(
    deadline: Option<tokio::time::Instant>,
    timeout_ms: u64,
    future: F,
) -> Result<T, MigrationError>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| MigrationError::LockTimeout(timeout_ms))?
            .map_err(backend),
        None => future.await.map_err(backend),
    }
}

/// 业务作用: 从锁等待配置建立连接取得与锁竞争共用的绝对 deadline。
///
/// # 参数
/// - `timeout_ms`: `0` 表示无显式截止时间，其它值按当前时刻计算。
///
/// 返回: 有界配置对应绝对时刻，无界配置返回 `None`。
fn deadline(timeout_ms: u64) -> Option<tokio::time::Instant> {
    (timeout_ms != 0)
        .then(|| tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms))
}

/// 业务作用: 校验 schema 可安全作为单一 PostgreSQL identifier 与 lock 身份。
///
/// # 参数
/// - `schema`: 外部配置提供的受管 schema 名称。
///
/// 返回: ASCII 普通 identifier 且不超过 PostgreSQL 63 字节上限时成功。
fn validate_schema(schema: &str) -> Result<(), MigrationError> {
    let mut bytes = schema.bytes();
    let Some(first) = bytes.next() else {
        return Err(MigrationError::InvalidSchema);
    };
    if schema.len() > 63
        || !(first == b'_' || first.is_ascii_alphabetic())
        || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
    {
        return Err(MigrationError::InvalidSchema);
    }
    Ok(())
}

/// 业务作用: 拒绝当前 PostgreSQL adapter 无法安全 catalog 查询的自定义 migration 表名。
///
/// # 参数
/// - `migrator`: 需要执行的 SQLx migrator。
///
/// 返回: 使用默认 `_sqlx_migrations` 时成功；其它表名在网络副作用前拒绝。
fn ensure_default_table(migrator: &Migrator) -> Result<(), MigrationError> {
    if migrator.table_name.as_ref() == "_sqlx_migrations" {
        Ok(())
    } else {
        Err(MigrationError::Backend(
            "custom migration table is unsupported".to_owned(),
        ))
    }
}

/// 业务作用: 提取不含 SQL 正文的 up migration 描述供 core 比较。
///
/// # 参数
/// - `migrator`: 当前业务嵌入式 migrator。
///
/// 返回: 按版本升序排列的版本、checksum、可逆性和事务属性。
fn embedded_ups(migrator: &Migrator) -> Vec<EmbeddedMigration> {
    let mut migrations = migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| EmbeddedMigration {
            version: migration.version,
            checksum: migration.checksum.to_vec(),
            reversible: migration.migration_type.is_reversible(),
            transactional: !(migrator.no_tx || migration.no_tx),
        })
        .collect::<Vec<_>>();
    migrations.sort_by_key(|migration| migration.version);
    migrations
}

/// 业务作用: 为 disabled 模式生成不接触数据库的准确摘要。
///
/// # 参数
/// - `migrator`: 当前业务 migrator。
///
/// 返回: 嵌入数量准确且 applied 为零的 disabled 报告。
fn disabled_report(migrator: &Migrator) -> MigrationReport {
    MigrationReport {
        mode: MigrationMode::Disabled,
        embedded: embedded_ups(migrator).len(),
        applied: 0,
    }
}

/// 业务作用: 把连接与 catalog 细节收敛为不泄露 SQL、schema 或 endpoint 的稳定错误。
///
/// # 参数
/// - `_error`: 仅用于触发映射的底层错误，不进入公开文本。
///
/// 返回: 固定数据库错误分类。
fn backend<E>(_error: E) -> MigrationError {
    MigrationError::Backend("database error".to_owned())
}

/// 业务作用: 把 SQLx migration 错误映射为 core 稳定分类并移除 SQL 与连接细节。
///
/// # 参数
/// - `error`: SQLx migration 执行错误。
///
/// 返回: checksum、dirty 或通用 apply 失败分类。
fn map_migrate_err(error: sqlx::migrate::MigrateError) -> MigrationError {
    match error {
        sqlx::migrate::MigrateError::VersionMismatch(version) => {
            MigrationError::ChecksumMismatch(version)
        }
        sqlx::migrate::MigrateError::Dirty(version) => MigrationError::Dirty(version),
        _ => MigrationError::Backend("migration apply failed".to_owned()),
    }
}
