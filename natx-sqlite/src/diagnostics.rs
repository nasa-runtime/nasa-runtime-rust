use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};

use crate::SqliteDataSource;

/// 原生错误分类不承诺重试安全；是否重试仍取决于事务提交事实。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum SqliteErrorCategory {
    Busy,
    BusySnapshot,
    Locked,
    Constraint,
    ReadOnly,
    Io,
    Full,
    Interrupted,
    Corrupt,
    Other,
}

/// 业务作用：从 SQLx 原生 code 提取稳定类别，不匹配 SQL 或服务端错误文本。
/// 参数说明：
/// - `error`：尚未擦除的 SQLx 错误。
/// 返回：固定低基数类别，扩展 BUSY_SNAPSHOT 与普通 BUSY 保持区别。
pub fn classify_sqlite_error(error: &sqlx::Error) -> SqliteErrorCategory {
    if matches!(error, sqlx::Error::Io(_)) {
        return SqliteErrorCategory::Io;
    }
    let code = error
        .as_database_error()
        .and_then(|error| error.code())
        .and_then(|code| code.parse::<i32>().ok());
    classify_code(code)
}

/// 业务作用：让受控 SQL 与原生维护使用相同的稳定错误类别。
/// 参数说明：
/// - `code`：已确认的原生扩展结果码；缺失时不猜测存储故障。
/// 返回：用于隔离判断和固定指标的错误类别。
pub(crate) fn classify_code(code: Option<i32>) -> SqliteErrorCategory {
    use libsqlite3_sys as ffi;
    if code == Some(ffi::SQLITE_BUSY_SNAPSHOT) {
        return SqliteErrorCategory::BusySnapshot;
    }
    match code.map(|code| code & 255) {
        Some(ffi::SQLITE_BUSY) => SqliteErrorCategory::Busy,
        Some(ffi::SQLITE_LOCKED) => SqliteErrorCategory::Locked,
        Some(ffi::SQLITE_CONSTRAINT) => SqliteErrorCategory::Constraint,
        Some(ffi::SQLITE_READONLY) => SqliteErrorCategory::ReadOnly,
        Some(ffi::SQLITE_IOERR | ffi::SQLITE_CANTOPEN) => SqliteErrorCategory::Io,
        Some(ffi::SQLITE_FULL) => SqliteErrorCategory::Full,
        Some(ffi::SQLITE_INTERRUPT) => SqliteErrorCategory::Interrupted,
        Some(ffi::SQLITE_CORRUPT | ffi::SQLITE_NOTADB) => SqliteErrorCategory::Corrupt,
        _ => SqliteErrorCategory::Other,
    }
}

/// 业务作用：返回当前进程实际链接的 SQLite 引擎标识。
/// 参数说明：无。
/// 返回：SQLite 的整数版本编码，用于启动诊断而不扩展每条 SQL 的指标标签。
pub fn engine_version_number() -> i32 {
    unsafe { libsqlite3_sys::sqlite3_libversion_number() }
}

pub(crate) struct Diagnostics {
    errors: [AtomicU64; 10],
    pub(crate) checkpoint_wal: AtomicI64,
    pub(crate) checkpoint_done: AtomicI64,
    pub(crate) checkpoint_busy: AtomicI64,
    pub(crate) backup_ok: AtomicU64,
    pub(crate) backup_failed: AtomicU64,
    pub(crate) backup_millis: AtomicU64,
}

impl Default for Diagnostics {
    /// 业务作用：为一个 datasource 创建固定规模诊断单元，不保存 SQL 和文件路径。
    /// 参数说明：无。
    /// 返回：计数为零，尚未取得 checkpoint 统计以负一表示。
    fn default() -> Self {
        Self {
            errors: [const { AtomicU64::new(0) }; 10],
            checkpoint_wal: AtomicI64::new(-1),
            checkpoint_done: AtomicI64::new(-1),
            checkpoint_busy: AtomicI64::new(-1),
            backup_ok: AtomicU64::new(0),
            backup_failed: AtomicU64::new(0),
            backup_millis: AtomicU64::new(0),
        }
    }
}

impl Diagnostics {
    /// 业务作用：只记录已确认的原生执行错误，避免取消等待被误记为数据库拒绝。
    /// 参数说明：
    /// - `error`：worker 已收到的原始 SQLx 错误。
    /// 返回：固定类别计数饱和递增。
    pub(crate) fn record_error(&self, error: &sqlx::Error) {
        self.record_category(classify_sqlite_error(error));
    }

    /// 业务作用：按已确认类别累加源库错误，供原生维护复用同一诊断序列。
    /// 参数说明：
    /// - `category`：实际源库故障类别。
    /// 返回：对应固定类别的计数饱和递增。
    pub(crate) fn record_category(&self, category: SqliteErrorCategory) {
        nametrics_core::atomic::add(&self.errors[category as usize], 1);
    }
}

static ERRORS: MetricDescriptor = MetricDescriptor {
    name: "natx_sqlite_errors_total",
    help: "SQLite 受控执行已确认错误次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["datasource", "category"],
    histogram_bounds: &[],
};
static STATUS: MetricDescriptor = MetricDescriptor {
    name: "natx_sqlite_maintenance_state",
    help: "SQLite 最近完成的维护状态，负值表示统计不可用。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["datasource", "state"],
    histogram_bounds: &[],
};
static BACKUPS: MetricDescriptor = MetricDescriptor {
    name: "natx_sqlite_backups_total",
    help: "SQLite 原生备份执行结束次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["datasource", "outcome"],
    histogram_bounds: &[],
};

static DESCRIPTORS: &[&MetricDescriptor] = &[&ERRORS, &STATUS, &BACKUPS];

/// 固定来源集合的 SQLite 诊断出口，不在 scrape 时执行 checkpoint 或文件 I/O。
pub struct SqliteMetricsSource {
    sources: Vec<SqliteDataSource>,
}

impl SqliteMetricsSource {
    /// 业务作用：冻结 Application 已安装的 SQLite 来源集合与观测基数。
    /// 参数说明：
    /// - `sources`：每个来源只出现一次的使用句柄集合。
    /// 返回：重复名称拒绝，成功后不再动态增加指标标签。
    pub fn new(sources: Vec<SqliteDataSource>) -> anyhow::Result<Self> {
        let mut names = std::collections::BTreeSet::new();
        for source in &sources {
            anyhow::ensure!(
                names.insert(source.name()),
                "duplicate SQLite metrics datasource"
            );
        }
        Ok(Self {
            sources,
        })
    }

    /// 业务作用：在指标注册之前预留该冻结来源表的最大序列数。
    /// 参数说明：无。
    /// 返回：每个来源十种错误、十一种状态和两种备份结果的精确上限。
    pub fn series_budget(&self) -> usize {
        self.sources.len() * 23
    }
}

impl LegacyMetricsSource for SqliteMetricsSource {
    /// 业务作用：登记固定的 SQLite 维护与错误指标族。
    /// 参数说明：无。
    /// 返回：与结构化快照一致的 descriptor 集合。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        DESCRIPTORS
    }

    /// 业务作用：导出原子维护事实，不在指标出口触发数据库副作用。
    /// 参数说明：无。
    /// 返回：固定上限的来源快照，未取得的帧数保持负值。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut samples = Vec::with_capacity(self.series_budget());
        for source in &self.sources {
            let diagnostics = &source.0.diagnostics;
            for (category, count) in [
                "busy",
                "busy_snapshot",
                "locked",
                "constraint",
                "read_only",
                "io",
                "full",
                "interrupted",
                "corrupt",
                "other",
            ]
            .iter()
            .zip(&diagnostics.errors)
            {
                samples.push(MetricSample {
                    name: ERRORS.name,
                    labels: vec![
                        ("datasource", source.name().to_owned()),
                        ("category", (*category).to_owned()),
                    ],
                    value: MetricValue::Counter(count.load(Ordering::Relaxed)),
                });
            }
            for (state, value) in [
                (
                    "checkpoint_wal_frames",
                    diagnostics.checkpoint_wal.load(Ordering::Relaxed) as f64,
                ),
                (
                    "checkpoint_done_frames",
                    diagnostics.checkpoint_done.load(Ordering::Relaxed) as f64,
                ),
                (
                    "checkpoint_busy",
                    diagnostics.checkpoint_busy.load(Ordering::Relaxed) as f64,
                ),
                (
                    "backup_duration_ms",
                    diagnostics.backup_millis.load(Ordering::Relaxed) as f64,
                ),
                ("cache_quarantined", f64::from(!source.cache_safe())),
                (
                    "source_isolated",
                    f64::from(source.isolation_reason().is_some()),
                ),
                (
                    "isolated_connections",
                    source
                        .0
                        .health
                        .connections
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .iter()
                        .filter_map(std::sync::Weak::upgrade)
                        .filter(|state| {
                            state.poisoned.load(Ordering::Acquire)
                                && !state.closed.load(Ordering::Acquire)
                        })
                        .count() as f64,
                ),
                (
                    "wal_bytes",
                    source.0.health.wal_bytes.load(Ordering::Relaxed) as f64,
                ),
                (
                    "available_bytes",
                    source.0.health.available_bytes.load(Ordering::Relaxed) as f64,
                ),
                (
                    "storage_pressure",
                    f64::from(source.0.health.pressure.load(Ordering::Acquire)),
                ),
                (
                    "oldest_read_ms",
                    source
                        .0
                        .health
                        .reads
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .values()
                        .map(|started| started.elapsed().as_millis() as f64)
                        .fold(0.0, f64::max),
                ),
            ] {
                samples.push(MetricSample {
                    name: STATUS.name,
                    labels: vec![
                        ("datasource", source.name().to_owned()),
                        ("state", state.to_owned()),
                    ],
                    value: MetricValue::Gauge(value),
                });
            }
            for (outcome, count) in [
                ("ok", &diagnostics.backup_ok),
                ("failed", &diagnostics.backup_failed),
            ] {
                samples.push(MetricSample {
                    name: BACKUPS.name,
                    labels: vec![
                        ("datasource", source.name().to_owned()),
                        ("outcome", outcome.to_owned()),
                    ],
                    value: MetricValue::Counter(count.load(Ordering::Relaxed)),
                });
            }
        }
        Some(samples)
    }

    /// 业务作用：把文本编码交给统一 MetricHub，保持出口口径一致。
    /// 参数说明：
    /// - `_output`：兼容旧文本出口的缓冲区。
    /// 返回：无直接输出，所有样本来自结构化快照。
    fn render_prometheus(&self, _output: &mut String) {}
}
