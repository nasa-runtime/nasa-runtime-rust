use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use libsqlite3_sys as ffi;
use tokio::time::Instant;

use crate::{DataSourceConfig, JournalMode, SqliteDataSource, SqliteOptions};

/// 维护失败的证据归属，目标或发布失败不能据此撤销健康源库的业务权威。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceErrorOrigin {
    /// 已确认属于源数据库，可参与来源健康与隔离判断。
    Source,
    /// 属于目标文件准备或写入，不代表源数据库失效。
    Target,
    /// 目标内容持久发布失败，调用方不能认定备份已可用。
    Publication,
    /// 证据不足以确定归属，不将失败归因于源数据库。
    Unknown,
}

/// 保留维护阶段、故障归属和原生扩展码；可从 anyhow 错误链中读取。
#[derive(Debug)]
pub struct MaintenanceError {
    /// 失败发生的固定维护阶段，不包含业务数据或文件路径。
    pub operation: &'static str,
    /// 错误证据归属，决定是否参与源数据库隔离判断。
    pub origin: MaintenanceErrorOrigin,
    /// 原生扩展结果码；文件系统等外部失败没有原生码。
    pub code: Option<i32>,
    cause: Option<anyhow::Error>,
}

impl std::fmt::Display for MaintenanceError {
    /// 业务作用：输出固定阶段与原生码，不将源文件路径或业务内容写入错误分类。
    ///
    /// 参数说明：
    /// - `formatter`：错误文本输出目标。
    ///
    /// 返回：保留故障归属和原生码的诊断文本。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SQLite {} failed ({:?}, code {:?})",
            self.operation, self.origin, self.code
        )?;
        if let Some(cause) = &self.cause {
            write!(formatter, ": {cause}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MaintenanceError {
    /// 业务作用：保留文件发布或权限拒绝的底层原因，供调用方核对持久化事实。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有外部原因时返回其错误链，纯原生码失败为空。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|cause| cause.as_ref() as _)
    }
}

/// 业务作用：在原生句柄状态改变前固定维护失败证据。
///
/// 参数说明：
/// - `operation`：发生失败的固定阶段。
/// - `origin`：已确认的故障归属。
/// - `code`：原生扩展结果码。
///
/// 返回：可供来源健康链路与调用方分别处理的结构化错误。
fn native_failure(
    operation: &'static str,
    origin: MaintenanceErrorOrigin,
    code: i32,
) -> anyhow::Error {
    MaintenanceError {
        operation,
        origin,
        code: Some(code),
        cause: None,
    }
    .into()
}

/// 业务作用：将路径与发布失败保留为目标侧证据，避免误触发源库隔离。
///
/// 参数说明：
/// - `operation`：目标准备或发布阶段。
/// - `origin`：目标或发布归属。
/// - `cause`：保持原有失败后果的底层错误。
///
/// 返回：可从错误链读取原因的维护失败。
fn external_failure(
    operation: &'static str,
    origin: MaintenanceErrorOrigin,
    cause: anyhow::Error,
) -> anyhow::Error {
    MaintenanceError {
        operation,
        origin,
        code: None,
        cause: Some(cause),
    }
    .into()
}

/// 手动 WAL 维护策略；默认不等待读者让出快照。
#[derive(Clone, Copy, Debug, Default)]
pub enum CheckpointMode {
    /// 请求 PASSIVE checkpoint，不等待活跃读者释放快照。
    #[default]
    Passive,
    /// 请求 FULL checkpoint，等待仍受操作预算约束。
    Full,
    /// 请求 RESTART checkpoint，允许日志在满足引擎条件后重新使用。
    Restart,
    /// 请求 TRUNCATE checkpoint，满足引擎条件后截短 WAL 文件。
    Truncate,
}

/// 帧数不可用时使用 None，未完成 checkpoint 不撤销已确认的提交。
#[derive(Clone, Copy, Debug)]
pub struct CheckpointResult {
    /// 原生引擎是否因忙碌未完成所请求的 checkpoint。
    pub busy: bool,
    /// 引擎报告的 WAL 总帧数；未提供时为空。
    pub wal_frames: Option<u32>,
    /// 引擎报告的已回写帧数；未提供时为空，不等同于新提交消息数。
    pub checkpointed_frames: Option<u32>,
}

/// 仅在原生复制、校验、关闭和持久发布全部确认后产生。
#[derive(Clone, Debug)]
pub struct BackupReport {
    /// 已完成持久发布的备份文件路径。
    pub path: PathBuf,
    /// 原生备份复制的数据库页数。
    pub pages: u32,
    /// 已发布备份文件的实际字节长度。
    pub bytes: u64,
    /// 已核对的备份文件日志模式。
    pub journal_mode: JournalMode,
}

/// 固定来源的维护能力；不暴露原生连接或任意 PRAGMA。
#[derive(Clone)]
pub struct SqliteMaintenance {
    source: SqliteDataSource,
}

impl SqliteDataSource {
    /// 业务作用：为当前来源取得受同一生命周期和串行门保护的维护能力。
    ///
    /// 参数说明：无。
    ///
    /// 返回：只能执行固定维护操作的句柄，停机后不能发起新操作。
    pub fn maintenance(&self) -> SqliteMaintenance {
        SqliteMaintenance {
            source: self.clone(),
        }
    }
}

/// 固定完整性检查通过后读取的持久 schema 标识，不提供任意 PRAGMA 能力。
pub struct DatabaseInspection {
    /// 完整性检查通过后读取的 user_version，用于识别持久 schema 格式。
    pub user_version: u32,
}

pub(crate) enum Operation {
    Inspect,
    Checkpoint(CheckpointMode),
    Backup(PathBuf, JournalMode),
}

pub(crate) enum ResultValue {
    Inspection(DatabaseInspection),
    Checkpoint(CheckpointResult),
    Backup(BackupReport),
}

impl SqliteMaintenance {
    /// 业务作用：核对数据库物理结构、外键引用并读取旧存储格式标识，供启动迁移门禁使用。
    ///
    /// 参数说明：
    /// - `budget`：维护排队、数据库检查和结果交付共同遵守的预算。
    ///
    /// 返回：完整性检查通过后的 user_version；损坏、外键缺失或来源失效时拒绝。
    pub async fn inspect(
        &self,
        budget: &nabudget::RequestBudget,
    ) -> anyhow::Result<DatabaseInspection> {
        match self.execute(Operation::Inspect, budget).await? {
            ResultValue::Inspection(result) => Ok(result),
            _ => unreachable!("inspection result"),
        }
    }

    /// 业务作用：有界执行同文件串行的 WAL checkpoint，不删除活动日志。
    ///
    /// 参数说明：
    /// - `mode`：读者等待与 WAL 截断策略。
    /// - `budget`：获取维护权、借用连接和原生执行共同遵守的预算。
    ///
    /// 返回：真实 busy 与帧数；只读来源、环境事务或停机状态拒绝。
    pub async fn checkpoint(
        &self,
        mode: CheckpointMode,
        budget: &nabudget::RequestBudget,
    ) -> anyhow::Result<CheckpointResult> {
        anyhow::ensure!(
            !self.source.is_read_only(),
            "SQLite read-only datasource cannot checkpoint"
        );
        match self.execute(Operation::Checkpoint(mode), budget).await? {
            ResultValue::Checkpoint(result) => Ok(result),
            _ => unreachable!("checkpoint result"),
        }
    }

    /// 业务作用：用 SQLite 原生增量备份生成独立、经过校验的 DELETE 数据库。
    ///
    /// 参数说明：
    /// - `destination`：必须不存在且父目录受保护的新路径。
    /// - `budget`：整个维护与发布过程的剩余预算。
    ///
    /// 返回：确认发布的备份；取消或完成未确认时不得当作可直接重试的成功。
    pub async fn backup_to(
        &self,
        destination: impl AsRef<Path>,
        budget: &nabudget::RequestBudget,
    ) -> anyhow::Result<BackupReport> {
        match self
            .execute(
                Operation::Backup(destination.as_ref().to_owned(), JournalMode::Delete),
                budget,
            )
            .await?
        {
            ResultValue::Backup(result) => Ok(result),
            _ => unreachable!("backup result"),
        }
    }

    /// 业务作用：把维护门与原生执行责任一起交给 worker，取消不会提前放开同文件维护。
    ///
    /// 参数说明：
    /// - `operation`：固定维护动作。
    /// - `budget`：调用者剩余时间与取消信号。
    ///
    /// 返回：worker 最终确认的维护结果。
    async fn execute(
        &self,
        operation: Operation,
        budget: &nabudget::RequestBudget,
    ) -> anyhow::Result<ResultValue> {
        anyhow::ensure!(
            crate::current_driver().is_none(),
            "SQLite maintenance is forbidden in a transaction"
        );
        crate::check_source(&self.source)?;
        budget.check()?;
        let deadline = budget.deadline();
        let work = async {
            let gate =
                tokio::time::timeout_at(deadline, self.source.0.maintenance.clone().lock_owned())
                    .await?;
            let lease = self.source.acquire(deadline).await?;
            lease.maintain(operation, gate, deadline).await
        };
        budget.run(work).await?
    }
}

/// 业务作用：独占只读备份源后恢复到新路径，不覆盖活动数据库或保留旧 sidecar。
///
/// 参数说明：
/// - `backup`：已关闭、使用 DELETE 模式的完整备份文件。
/// - `destination`：尚不存在的新目标。
/// - `journal_mode`：恢复后目标的明确部署模式。
/// - `budget`：打开、校验、复制及发布共同遵守的预算。
///
/// 返回：新文件已发布且源已关闭；失败保留真实发布事实，不覆盖重试。
pub async fn restore_to_new(
    backup: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    journal_mode: JournalMode,
    budget: &nabudget::RequestBudget,
) -> anyhow::Result<BackupReport> {
    anyhow::ensure!(
        crate::current_driver().is_none(),
        "SQLite restore is forbidden in a transaction"
    );
    budget.check()?;
    let owner = budget
        .run(crate::open_owned(
            "restore",
            DataSourceConfig {
                sqlite: SqliteOptions {
                    path: backup.as_ref().to_owned(),
                    read_only: true,
                    journal_mode: JournalMode::Delete,
                    ..SqliteOptions::default()
                },
                max_connections: 1,
                ..DataSourceConfig::default()
            },
        ))
        .await??;
    let result = owner
        .datasource()
        .maintenance()
        .execute(
            Operation::Backup(destination.as_ref().to_owned(), journal_mode),
            budget,
        )
        .await;
    // 即使业务预算结束，源关闭仍有独立有限清理预算；超时不会解除原生连接的文件锁。
    owner.close(Instant::now() + Duration::from_secs(5)).await?;
    match result? {
        ResultValue::Backup(report) => Ok(report),
        _ => unreachable!("restore result"),
    }
}

/// 业务作用：在 actor 阻塞域锁住 SQLx 原生句柄，顺序完成维护与结果确认。
///
/// 参数说明：
/// - `connection`：来源唯一借用的连接。
/// - `datasource`：持续提供排空截止点和隔离状态的来源。
/// - `operation`：已通过入口门禁的固定动作。
/// - `deadline`：绝对截止点。
/// - `cancel`：等待方取消信号。
///
/// 返回：没有原始句柄逃逸的维护结果。
pub(crate) async fn execute_native(
    connection: &mut sqlx::SqliteConnection,
    datasource: &SqliteDataSource,
    operation: Operation,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
) -> anyhow::Result<ResultValue> {
    let mut locked = connection.lock_handle().await?;
    let source = locked.as_raw_handle().as_ptr();
    let budget = NativeBudget {
        deadline,
        cancel,
        source: datasource.clone(),
    };
    budget.check()?;
    match operation {
        Operation::Inspect => {
            let mut rows: Vec<String> = Vec::new();
            for sql in [
                c"PRAGMA quick_check",
                c"PRAGMA foreign_key_check",
                c"PRAGMA user_version",
            ] {
                rows.clear();
                budget.check()?;
                let result = unsafe {
                    ffi::sqlite3_exec(
                        source,
                        sql.as_ptr(),
                        Some(collect_row),
                        &mut rows as *mut Vec<String> as *mut c_void,
                        std::ptr::null_mut(),
                    )
                };
                if result != ffi::SQLITE_OK {
                    return Err(native_failure(
                        "inspection",
                        MaintenanceErrorOrigin::Source,
                        unsafe { ffi::sqlite3_extended_errcode(source) },
                    ));
                }
                budget.check()?;
                // 原生结构异常与业务外键缺失都必须阻断启动，不能以可读取部分记录代替完整性。
                if sql == c"PRAGMA quick_check" && rows.as_slice() != ["ok"] {
                    return Err(native_failure(
                        "inspection",
                        MaintenanceErrorOrigin::Source,
                        ffi::SQLITE_CORRUPT,
                    ));
                }
                anyhow::ensure!(
                    sql != c"PRAGMA foreign_key_check" || rows.is_empty(),
                    "SQLite foreign key inspection failed"
                );
            }
            anyhow::ensure!(rows.len() == 1, "SQLite schema identity is unavailable");
            Ok(ResultValue::Inspection(DatabaseInspection {
                user_version: rows[0].parse()?,
            }))
        }
        Operation::Checkpoint(mode) => {
            let mode = match mode {
                CheckpointMode::Passive => ffi::SQLITE_CHECKPOINT_PASSIVE,
                CheckpointMode::Full => ffi::SQLITE_CHECKPOINT_FULL,
                CheckpointMode::Restart => ffi::SQLITE_CHECKPOINT_RESTART,
                CheckpointMode::Truncate => ffi::SQLITE_CHECKPOINT_TRUNCATE,
            };
            let mut log = -1;
            let mut checkpointed = -1;
            let result = unsafe {
                ffi::sqlite3_wal_checkpoint_v2(
                    source,
                    c"main".as_ptr(),
                    mode,
                    &mut log,
                    &mut checkpointed,
                )
            };
            if result != ffi::SQLITE_OK && result != ffi::SQLITE_BUSY {
                return Err(native_failure(
                    "checkpoint",
                    MaintenanceErrorOrigin::Source,
                    result,
                ));
            }
            Ok(ResultValue::Checkpoint(CheckpointResult {
                busy: result == ffi::SQLITE_BUSY,
                wal_frames: u32::try_from(log).ok(),
                checkpointed_frames: u32::try_from(checkpointed).ok(),
            }))
        }
        Operation::Backup(destination, mode) => {
            backup_native(source, &destination, mode, &budget).map(ResultValue::Backup)
        }
    }
}

struct NativeBudget {
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    source: SqliteDataSource,
}

impl NativeBudget {
    /// 业务作用：让复制、锁等待、目标校验及发布共同服从维护预算与来源生命周期。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未取消、未超期且来源仍可排空时成功；隔离或来源关闭后拒绝后续普通维护步骤。
    fn check(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.cancel.load(Ordering::Acquire) && Instant::now() < self.deadline,
            "SQLite maintenance cancelled or deadline exceeded"
        );
        // 已接纳维护允许在排空期限内结束；借用入口永久关闭后只保留 finish/关闭与临时材料清理。
        // semaphore 的关闭态是单一终结证据，避免把接纳态切换到排空态的中间窗口误判为终止。
        anyhow::ensure!(
            !self.source.business_expired() && !self.source.0.borrowers.is_closed(),
            "SQLite maintenance source is closed, isolated, or drain deadline exceeded"
        );
        Ok(())
    }
}

struct NativeDatabase(*mut ffi::sqlite3);

impl Drop for NativeDatabase {
    /// 业务作用：失败路径同步关闭目标连接，临时文件清理不得先于原生退出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：该模块未保留 statement 时立即关闭目标。
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                ffi::sqlite3_close(self.0);
            }
        }
    }
}

/// 业务作用：增量复制受控源并在 DELETE 目标校验后按明确模式持久发布。
///
/// 参数说明：
/// - `source`：整个调用期间被 SQLx handle lock 独占的有效连接。
/// - `destination`：不存在的新目标路径。
/// - `mode`：最终目标模式。
/// - `budget`：同步调用期间持续有效的取消与时间边界。
///
/// 返回：只有 DONE、finish、校验、关闭、同步全部成功才返回报告。
fn backup_native(
    source: *mut ffi::sqlite3,
    destination: &Path,
    mode: JournalMode,
    budget: &NativeBudget,
) -> anyhow::Result<BackupReport> {
    budget.check()?;
    let publication = crate::publication::Publication::reserve(destination).map_err(|error| {
        external_failure(
            "backup target reservation",
            MaintenanceErrorOrigin::Target,
            error,
        )
    })?;
    let path = CString::new(
        publication
            .temporary
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("SQLite backup path must be UTF-8"))?,
    )?;
    budget.check()?;
    let mut target = NativeDatabase(std::ptr::null_mut());
    let opened = unsafe {
        ffi::sqlite3_open_v2(
            path.as_ptr(),
            &mut target.0,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_PRIVATECACHE | ffi::SQLITE_OPEN_NOMUTEX,
            std::ptr::null(),
        )
    };
    if opened != ffi::SQLITE_OK {
        return Err(native_failure(
            "backup target open",
            MaintenanceErrorOrigin::Target,
            opened,
        ));
    }
    // 目标与源是两个真实连接，目标回调引用的预算在关闭前始终有效。
    unsafe {
        ffi::sqlite3_progress_handler(
            target.0,
            1000,
            Some(target_progress),
            budget as *const NativeBudget as *mut c_void,
        );
        ffi::sqlite3_busy_handler(
            target.0,
            Some(target_busy),
            budget as *const NativeBudget as *mut c_void,
        );
    }
    execute_target(
        target.0,
        budget,
        c"PRAGMA trusted_schema=OFF; PRAGMA synchronous=EXTRA; PRAGMA journal_mode=DELETE",
        None,
    )?;
    if cfg!(target_os = "macos") {
        execute_target(
            target.0,
            budget,
            c"PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON",
            None,
        )?;
    }
    let source_io = crate::maintenance_io::FileIo::install(source, true)?;
    let target_io = crate::maintenance_io::FileIo::install(target.0, false)?;
    let backup =
        unsafe { ffi::sqlite3_backup_init(target.0, c"main".as_ptr(), source, c"main".as_ptr()) };
    if backup.is_null() {
        return Err(native_failure(
            "backup initialization",
            MaintenanceErrorOrigin::Target,
            unsafe { ffi::sqlite3_extended_errcode(target.0) },
        ));
    }
    // 每次成功 init 必须恰好 finish；即使 finish 返回 OK，未见 DONE 也不能宣告复制完成。
    let mut completed = false;
    let mut pages = 0;
    let copy_result = (|| -> anyhow::Result<()> {
        loop {
            budget.check()?;
            let result = unsafe { ffi::sqlite3_backup_step(backup, 128) };
            if result == ffi::SQLITE_DONE {
                completed = true;
                pages = unsafe { ffi::sqlite3_backup_pagecount(backup) }.max(0) as u32;
                return Ok(());
            }
            match result {
                ffi::SQLITE_OK => {}
                ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => {
                    std::thread::sleep(Duration::from_millis(1))
                }
                _ => {
                    // finish 会把源读取错误写入目标 errcode；必须在此依据源文件证据固定归属。
                    let source_code = source_io.error_code();
                    let target_code = target_io.error_code();
                    let origin = if target_code.is_some() && source_code.is_none() {
                        MaintenanceErrorOrigin::Target
                    } else if source_code.is_some()
                        || matches!(result & 255, ffi::SQLITE_CORRUPT | ffi::SQLITE_NOTADB)
                    {
                        MaintenanceErrorOrigin::Source
                    } else if matches!(
                        result & 255,
                        ffi::SQLITE_IOERR
                            | ffi::SQLITE_FULL
                            | ffi::SQLITE_READONLY
                            | ffi::SQLITE_CANTOPEN
                    ) {
                        MaintenanceErrorOrigin::Target
                    } else {
                        MaintenanceErrorOrigin::Unknown
                    };
                    return Err(native_failure(
                        "backup step",
                        origin,
                        source_code.or(target_code).unwrap_or(result),
                    ));
                }
            }
        }
    })();
    let finished = unsafe { ffi::sqlite3_backup_finish(backup) };
    drop(source_io);
    drop(target_io);
    copy_result?;
    if finished != ffi::SQLITE_OK {
        return Err(native_failure(
            "backup finish",
            MaintenanceErrorOrigin::Target,
            finished,
        ));
    }
    anyhow::ensure!(
        completed && finished == ffi::SQLITE_OK,
        "SQLite backup completion is unconfirmed"
    );
    budget.check()?;
    let mut journal = Vec::new();
    execute_target(
        target.0,
        budget,
        c"PRAGMA journal_mode=DELETE",
        Some(&mut journal),
    )?;
    anyhow::ensure!(
        journal == ["delete"],
        "SQLite backup journal normalization failed"
    );
    let mut check = Vec::new();
    execute_target(target.0, budget, c"PRAGMA quick_check", Some(&mut check))?;
    anyhow::ensure!(check == ["ok"], "SQLite backup content validation failed");
    let mut foreign_keys = Vec::new();
    execute_target(
        target.0,
        budget,
        c"PRAGMA foreign_key_check",
        Some(&mut foreign_keys),
    )?;
    anyhow::ensure!(
        foreign_keys.is_empty(),
        "SQLite backup foreign key validation failed"
    );
    if mode == JournalMode::Wal {
        let mut journal = Vec::new();
        execute_target(
            target.0,
            budget,
            c"PRAGMA journal_mode=WAL",
            Some(&mut journal),
        )?;
        anyhow::ensure!(
            journal == ["wal"],
            "SQLite restore journal normalization failed"
        );
    }
    budget.check()?;
    let closed = unsafe { ffi::sqlite3_close(target.0) };
    if closed != ffi::SQLITE_OK {
        return Err(native_failure(
            "backup target close",
            MaintenanceErrorOrigin::Target,
            closed,
        ));
    }
    target.0 = std::ptr::null_mut();
    budget.check()?;
    let bytes = std::fs::metadata(&publication.temporary)?.len();
    let path = publication.publish(|| budget.check()).map_err(|error| {
        external_failure(
            "backup publication",
            MaintenanceErrorOrigin::Publication,
            error,
        )
    })?;
    Ok(BackupReport {
        path,
        pages,
        bytes,
        journal_mode: mode,
    })
}

/// 业务作用：在私有目标上执行固定校验语句，收集有界首列结果。
///
/// 参数说明：
/// - `database`：目标有效连接。
/// - `budget`：包含来源排空与隔离边界的维护预算。
/// - `sql`：框架固定 SQL。
/// - `rows`：需要验证的首列结果集合。
///
/// 返回：执行结束的原生状态；不接受业务 SQL。
fn execute_target(
    database: *mut ffi::sqlite3,
    budget: &NativeBudget,
    sql: &CStr,
    rows: Option<&mut Vec<String>>,
) -> anyhow::Result<()> {
    // 短语句不一定触发 progress 回调，执行边界仍需显式复验来源资格。
    budget.check()?;
    let context = rows.map_or(std::ptr::null_mut(), |rows| {
        rows as *mut Vec<String> as *mut c_void
    });
    let result = unsafe {
        ffi::sqlite3_exec(
            database,
            sql.as_ptr(),
            Some(collect_row),
            context,
            std::ptr::null_mut(),
        )
    };
    if result != ffi::SQLITE_OK {
        return Err(native_failure(
            "backup target validation",
            MaintenanceErrorOrigin::Target,
            unsafe { ffi::sqlite3_extended_errcode(database) },
        ));
    }
    budget.check()
}

/// 业务作用：限制目标校验返回的诊断数量，避免损坏文件驱动无界内存增长。
///
/// 参数说明：
/// - `context`：当前 execute_target 调用持有的可选结果集合。
/// - `count`：本行列数。
/// - `values`：SQLite 当前行的临时字符串。
/// - `_names`：SQLite 当前行列名。
///
/// 返回：达到上限时中止；只拷贝有界首列，不保留原生指针。
unsafe extern "C" fn collect_row(
    context: *mut c_void,
    count: c_int,
    values: *mut *mut c_char,
    _names: *mut *mut c_char,
) -> c_int {
    if context.is_null() || count == 0 {
        return 0;
    }
    let rows = unsafe { &mut *(context as *mut Vec<String>) };
    if rows.len() >= 16 {
        return 1;
    }
    let value = unsafe { *values };
    if !value.is_null() {
        rows.push(
            unsafe { CStr::from_ptr(value) }
                .to_string_lossy()
                .chars()
                .take(128)
                .collect(),
        );
    }
    0
}

/// 业务作用：目标校验 VM 服从维护取消与截止点。
///
/// 参数说明：
/// - `context`：整个原生维护期间有效的预算。
///
/// 返回：预算终止时中断 VM。
unsafe extern "C" fn target_progress(context: *mut c_void) -> c_int {
    i32::from(
        unsafe { &*(context as *const NativeBudget) }
            .check()
            .is_err(),
    )
}

/// 业务作用：目标锁竞争只在剩余预算内进行有限退避。
///
/// 参数说明：
/// - `context`：维护预算。
/// - `_attempts`：SQLite 本次锁等待的调用次数。
///
/// 返回：剩余预算存在时继续，否则停止。
unsafe extern "C" fn target_busy(context: *mut c_void, _attempts: c_int) -> c_int {
    if unsafe { target_progress(context) } != 0 {
        return 0;
    }
    std::thread::sleep(Duration::from_millis(1));
    1
}
