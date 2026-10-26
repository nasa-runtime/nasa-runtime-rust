use std::time::Duration;

use sqlx::Row;
use tokio::time::Instant;

use crate::executor::{Control, SqliteConn};
use crate::native::ExecutionPolicy;
use crate::{SqliteDataSource, TransactionMode};

/// 数据库中已经确认的迁移记录。
pub struct MigrationRecord {
    pub version: i64,
    pub checksum: Vec<u8>,
    pub success: bool,
}

/// 仅授予迁移脚本和固定历史操作的会话，不提供任意内部 SQL executor。
pub struct MigrationSession {
    connection: SqliteConn,
    writable: bool,
}

impl MigrationSession {
    /// 业务作用：在同源维护互斥下建立一致快照，apply 使用 IMMEDIATE 固定整批写入权。
    /// 参数说明：
    /// - `source`：迁移与业务共用的数据源句柄。
    /// - `apply`：是否需要应用待执行脚本。
    /// - `lock_timeout`：维护互斥、借用和 BEGIN 的共同预算。
    /// - `execution_timeout`：取得迁移事务后整个批次的预算。
    /// 返回：已经开始事务的专有会话，取消时以连接关闭回滚。
    pub async fn open(
        source: &SqliteDataSource,
        apply: bool,
        lock_timeout: Duration,
        execution_timeout: Duration,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            crate::current_driver().is_none(),
            "SQLite migration cannot join a business transaction"
        );
        anyhow::ensure!(
            !apply || !source.is_read_only(),
            "SQLite migration apply requires a writable datasource"
        );
        let lock_deadline = Instant::now() + lock_timeout;
        let maintenance =
            tokio::time::timeout_at(lock_deadline, source.0.maintenance.clone().lock_owned())
                .await?;
        let lease = source.acquire(lock_deadline).await?;
        // 取消 BEGIN 或脚本不代表原生事务已结束；维护门交给连接状态直到确认结束或物理关闭。
        *lease
            .state()
            .maintenance
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(maintenance);
        lease
            .control(
                Control::BeginMigration {
                    mode: if apply {
                        TransactionMode::Immediate
                    } else {
                        TransactionMode::Deferred
                    },
                    execution_timeout,
                },
                lock_deadline,
            )
            .await?;
        // 锁预算在 BEGIN 确认后结束；后续 SQL 和空闲监督共用 worker 固定的执行期限。
        let deadline = lease
            .state()
            .transaction_deadline()
            .ok_or_else(|| anyhow::anyhow!("SQLite migration transaction has no deadline"))?;
        Ok(Self {
            connection: SqliteConn {
                lease,
                deadline: Instant::from_std(deadline),
                ticket: None,
            },
            writable: apply,
        })
    }

    /// 业务作用：只读核验真实历史表布局和记录，不以同名 view 或有触发器表作为迁移权威。
    /// 参数说明：无。
    /// 返回：缺表时 None，结构和记录合法时返回有序历史，否则阻断迁移。
    pub async fn history(&mut self) -> anyhow::Result<Option<Vec<MigrationRecord>>> {
        let objects = sqlx::query("SELECT type, name, tbl_name FROM main.sqlite_schema WHERE name = '_sqlx_migrations' COLLATE NOCASE OR tbl_name = '_sqlx_migrations' COLLATE NOCASE").fetch_all(self.connection.framework_executor(ExecutionPolicy::History)).await?;
        if objects.is_empty() {
            return Ok(None);
        }
        let mut tables = 0;
        for object in objects {
            let kind: String = object.try_get("type")?;
            let name: String = object.try_get("name")?;
            match kind.as_str() {
                "table" if name.eq_ignore_ascii_case("_sqlx_migrations") => tables += 1,
                "index" if name.starts_with("sqlite_autoindex_") => {}
                _ => anyhow::bail!("SQLite migration history contains unsupported schema objects"),
            }
        }
        anyhow::ensure!(tables == 1, "SQLite migration history is not a table");
        let columns = sqlx::query("PRAGMA main.table_xinfo('_sqlx_migrations')")
            .fetch_all(self.connection.framework_executor(ExecutionPolicy::History))
            .await?;
        let expected = [
            ("version", "BIGINT"),
            ("description", "TEXT"),
            ("installed_on", "TIMESTAMP"),
            ("success", "BOOLEAN"),
            ("checksum", "BLOB"),
            ("execution_time", "BIGINT"),
        ];
        anyhow::ensure!(
            columns.len() == expected.len(),
            "SQLite migration history layout differs from SQLx"
        );
        for (index, (column, (name, kind))) in columns.iter().zip(expected).enumerate() {
            let actual_name: String = column.try_get("name")?;
            let actual_type: String = column.try_get("type")?;
            let primary: i64 = column.try_get("pk")?;
            let hidden: i64 = column.try_get("hidden")?;
            let not_null: i64 = column.try_get("notnull")?;
            anyhow::ensure!(
                actual_name.eq_ignore_ascii_case(name)
                    && actual_type.eq_ignore_ascii_case(kind)
                    && primary == i64::from(index == 0)
                    && hidden == 0
                    && (index == 0 || not_null == 1),
                "SQLite migration history column is incompatible"
            );
        }
        let invalid: i64 = sqlx::query_scalar("SELECT count(*) FROM main._sqlx_migrations WHERE typeof(version) <> 'integer' OR version <= 0 OR typeof(success) <> 'integer' OR success NOT IN (0, 1) OR typeof(checksum) <> 'blob' OR length(checksum) <> 48 OR typeof(description) <> 'text' OR typeof(installed_on) <> 'text' OR typeof(execution_time) <> 'integer' OR execution_time < -1").fetch_one(self.connection.framework_executor(ExecutionPolicy::History)).await?;
        anyhow::ensure!(
            invalid == 0,
            "SQLite migration history contains invalid records"
        );
        let rows = sqlx::query(
            "SELECT version, checksum, success FROM main._sqlx_migrations ORDER BY version",
        )
        .fetch_all(self.connection.framework_executor(ExecutionPolicy::History))
        .await?;
        let mut history = Vec::with_capacity(rows.len());
        let mut previous = None;
        for row in rows {
            let version: i64 = row.try_get("version")?;
            anyhow::ensure!(
                previous.is_none_or(|previous| version > previous),
                "SQLite migration history contains duplicate versions"
            );
            previous = Some(version);
            history.push(MigrationRecord {
                version,
                checksum: row.try_get("checksum")?,
                success: row.try_get("success")?,
            });
        }
        Ok(Some(history))
    }

    /// 业务作用：在 apply 的同一事务内首次创建 SQLx 历史表。
    /// 参数说明：无。
    /// 返回：明确缺表且可写时创建；已有未知结构不会被静默接受。
    pub async fn create_history(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.writable && self.history().await?.is_none(),
            "SQLite migration history creation is not allowed"
        );
        sqlx::query("CREATE TABLE main._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, success BOOLEAN NOT NULL, checksum BLOB NOT NULL, execution_time BIGINT NOT NULL)").execute(self.connection.framework_executor(ExecutionPolicy::History)).await?;
        Ok(())
    }

    /// 业务作用：在原生 authorizer 约束下执行业务迁移脚本，脚本不取得事务控制或历史写入权。
    /// 参数说明：
    /// - `sql`：当前迁移的完整 SQL 脚本。
    /// 返回：全部语句完成且事务仍连续才成功，失败保留整批回滚责任。
    pub async fn execute_script(&mut self, sql: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.writable,
            "SQLite migration validate cannot execute scripts"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(
                self.connection
                    .framework_executor(ExecutionPolicy::Migration),
            )
            .await?;
        anyhow::ensure!(
            !self
                .connection
                .lease
                .state()
                .poisoned
                .load(std::sync::atomic::Ordering::Acquire),
            "SQLite migration transaction was interrupted"
        );
        Ok(())
    }

    /// 业务作用：把脚本完成事实与 checksum 写入同一未提交事务。
    /// 参数说明：
    /// - `version`：业务 schema 序号。
    /// - `description`：迁移的业务描述。
    /// - `checksum`：SQLx 对 SQL 计算的 SHA384。
    /// - `elapsed_ns`：已完成脚本的纳秒耗时。
    /// 返回：唯一成功记录写入后完成，不在提交后补记历史。
    pub async fn record(
        &mut self,
        version: i64,
        description: &str,
        checksum: &[u8],
        elapsed_ns: i64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.writable && version > 0 && checksum.len() == 48 && elapsed_ns >= 0,
            "SQLite migration record is invalid"
        );
        sqlx::query("INSERT INTO main._sqlx_migrations(version, description, success, checksum, execution_time) VALUES (?, ?, 1, ?, ?)").bind(version).bind(description).bind(checksum).bind(elapsed_ns).execute(self.connection.framework_executor(ExecutionPolicy::History)).await?;
        Ok(())
    }

    /// 业务作用：提交前核验外键完整性并等待原生事务收据。
    /// 参数说明：无。
    /// 返回：完整提交确认才成功；结果未知时隔离来源缓存，不重放脚本。
    pub async fn commit(mut self) -> anyhow::Result<()> {
        let invalid = sqlx::query("PRAGMA main.foreign_key_check")
            .fetch_optional(self.connection.framework_executor(ExecutionPolicy::History))
            .await?;
        anyhow::ensure!(
            invalid.is_none(),
            "SQLite migration foreign key integrity check failed"
        );
        let result = self
            .connection
            .lease
            .control(Control::Commit, self.connection.deadline)
            .await;
        if result.is_err() {
            self.connection.lease.source.quarantine_cache();
        } else {
            self.connection
                .lease
                .state()
                .maintenance
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
        }
        result
    }

    /// 业务作用：结束只读校验快照，不创建或更新迁移历史。
    /// 参数说明：无。
    /// 返回：原生回滚确认后释放快照，失败连接关闭。
    pub async fn finish_validation(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.writable,
            "SQLite apply session requires explicit commit"
        );
        let result = self
            .connection
            .lease
            .control(Control::Rollback, self.connection.deadline)
            .await;
        if result.is_ok() {
            self.connection
                .lease
                .state()
                .maintenance
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
        }
        result
    }
}
