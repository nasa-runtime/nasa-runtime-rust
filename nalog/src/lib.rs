//! 统一日志初始化、文件滚动和格式化支持。
//!
//! 提供 `tracing` 订阅器装配、运行期日志级别切换、按大小滚动的 info/error 文件输出，
//! 以及可由配置模块驱动的日志管理入口。
// 全局订阅器只注册一次；运行期通过原子 writer 槽和 filter handle 切换文件输出与日志级别。
// LogGuard 必须由唯一生命周期 owner 持有到停机 flush 完成，避免后台刷盘线程提前退出。
#![forbid(unsafe_code)]

use std::fs::{create_dir_all, read_dir, rename, File, OpenOptions};
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::writer::{EitherWriter, MakeWriter};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// 应用侧日志配置抽象(`LogConfig`/`ByteSize`/`LogContext`/`LogManager` 等),把
/// `LogConfig → FileLogConfig` 映射从各业务 main 收敛到此，保持配置与文件生命周期只有一个 owner。
mod config;
pub use config::*;

/// `LOG_PATTERN` 配置的解析与渲染。
mod pattern;
pub use pattern::{CompiledLogPattern, LogPatternError, DEFAULT_LOG_PATTERN};

// ────────────────────────────────────────────────────────────────────────────
// 运行期可热插的文件 writer 槽(info 全量 + error 仅 ERROR)
// ────────────────────────────────────────────────────────────────────────────
// tracing 的全局订阅器只能 init 一次,没法"先控制台、之后重建带文件"。
// 解法:init 时就把【文件 layer】装进订阅器,但 writer 初始指向空槽(写入丢弃),
//       于是 nacos 之前日志只落控制台;nacos 之后把真实文件 writer 原子塞进槽,
//       后续日志就同时写文件。ArcSwapOption 提供无锁原子热替换。
static INFO_FILE_WRITER: ArcSwapOption<NonBlocking> = ArcSwapOption::const_empty();
static ERROR_FILE_WRITER: ArcSwapOption<NonBlocking> = ArcSwapOption::const_empty();
// 文件 layer 是否给内容上色，由 FileLogConfig.color 决定;由 enable_file_logging_with 设置,
// LogFormatter 每条日志读它决定文件输出是否带 ANSI(控制台另由 TTY 决定,与此无关)。
static FILE_COLOR: AtomicBool = AtomicBool::new(false);

// 三个 layer 共用同一个可热替换格式槽，避免控制台与文件使用不同配置。
// None = 未配置 → 用 DEFAULT_LOG_PATTERN(惰性编译一次)。LogConfig/LogManager::apply 解析后塞入。
static LOG_PATTERN: ArcSwapOption<CompiledLogPattern> = ArcSwapOption::const_empty();
static DEFAULT_PATTERN: OnceLock<CompiledLogPattern> = OnceLock::new();

/// 业务作用：惰性取得日志事件的默认已编译格式。
///
/// 参数说明: 无。
///
/// 返回：返回进程内共享的默认格式，首次解析常量失败时 panic，不修改运行期自定义格式槽。
fn default_pattern() -> &'static CompiledLogPattern {
    DEFAULT_PATTERN.get_or_init(|| {
        CompiledLogPattern::parse(DEFAULT_LOG_PATTERN).expect("DEFAULT_LOG_PATTERN must compile")
    })
}

/// 业务作用：发布已经编译完成的后续事件格式。
///
/// 参数说明：
/// - `pattern`：调用方已解析校验的共享格式树。
///
/// 返回：替换全局格式槽，后续读取使用新树；已取得旧树的渲染仍可完成，不在此重新校验或输出日志。
pub(crate) fn set_log_pattern(pattern: std::sync::Arc<CompiledLogPattern>) {
    LOG_PATTERN.store(Some(pattern));
}

// 默认值由公开配置合同统一定义。
const DEFAULT_MAX_FILE_SIZE: u64 = 500 * 1024 * 1024; // maxFileSize=500MB
const DEFAULT_MAX_HISTORY_DAYS: i64 = 30; // maxHistory=30
const DEFAULT_TOTAL_SIZE_CAP: u64 = 30 * 1024 * 1024 * 1024; // totalSizeCap=30GB

/// 按日期和大小滚动的文件日志配置。
///
/// `dir` 指定输出目录；`LegacyDefault` 路径策略使用 `/usr/local/logs/{application.name}`，按服务名分目录。
/// 文件基名固定为 `info`/`error`，业务无需再指定文件名。
#[derive(Clone, Debug)]
pub struct FileLogConfig {
    /// 日志输出目录。
    pub dir: String,
    /// 单文件大小上限(字节),超过即滚动（默认 500MB）。
    pub max_file_size: u64,
    /// 归档保留天数,更早的归档删除（默认 30）。
    pub max_history_days: i64,
    /// 归档总量上限(字节),超出按最旧优先删除（默认 30GB）。
    pub total_size_cap: u64,
    /// 启动时立即清理过期/超量历史（默认 true）。
    pub clean_history_on_start: bool,
    /// 额外写一份仅 ERROR 的 `error.log`（默认 true）。
    pub split_error_file: bool,
    /// 文件内容是否带 ANSI 颜色(默认 false=纯文本,grep/wc 友好)。
    /// 置 true 时 info.log/error.log 写入彩色转义码，终端查看可显示颜色；文本处理工具会读到
    /// `\x1b[..` 转义码，文件体积也会增加。
    /// 注意:**控制台**的颜色与本项无关——控制台按 stdout 是否 TTY 自动上色(重定向时自动关)。
    pub color: bool,
}

impl FileLogConfig {
    /// 业务作用：为显式目录构造文件日志轮转及保留策略。
    ///
    /// 参数说明：
    /// - `dir`：应用指定的日志目录，构造时不创建或验证。
    ///
    /// 返回：返回单文件 500 MiB、保留三十天、归档总量三十 GiB、启动清理和独立错误文件开启、颜色关闭的配置。
    pub fn new(dir: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            max_history_days: DEFAULT_MAX_HISTORY_DAYS,
            total_size_cap: DEFAULT_TOTAL_SIZE_CAP,
            clean_history_on_start: true,
            split_error_file: true,
            color: false,
        }
    }

    // ── 链式覆盖(便于直接喂 yml 的 `Option<...>`:None 保持默认/上一次值,Some 才覆盖)──
    // 把"MB→字节换算 + 有值才覆盖"的样板从各下游 main 收敛到这里(下游只读自己的配置字段传入)。

    /// 业务作用：按 MiB 设置触发单文件轮转的大小。
    ///
    /// 参数说明：
    /// - `mb`：Some 提供 MiB 阈值，None 保留既有设置。
    ///
    /// 返回：返回更新后的配置，字节换算溢出时饱和到 u64 上限，不校验零或直接执行轮转。
    pub fn with_max_file_size_mb(mut self, mb: Option<u64>) -> Self {
        if let Some(mb) = mb {
            self.max_file_size = mb.saturating_mul(1024 * 1024);
        }
        self
    }

    /// 业务作用：设置归档的日期保留窗口。
    ///
    /// 参数说明：
    /// - `days`：Some 保存天数，负值关闭按天清理，None 保留原设置。
    ///
    /// 返回：返回更新后的配置，不检查日期算术可表示性，也不立即删除归档。
    pub fn with_max_history_days(mut self, days: Option<i64>) -> Self {
        if let Some(d) = days {
            self.max_history_days = d;
        }
        self
    }

    /// 业务作用：按 MiB 设置每类文件归档的累计容量阈值。
    ///
    /// 参数说明：
    /// - `mb`：Some 提供 MiB 阈值，零表示关闭容量清理，None 保留原值。
    ///
    /// 返回：返回更新后的配置，字节乘法饱和，不执行目录扫描或删除。
    pub fn with_total_size_cap_mb(mut self, mb: Option<u64>) -> Self {
        if let Some(mb) = mb {
            self.total_size_cap = mb.saturating_mul(1024 * 1024);
        }
        self
    }

    /// 业务作用：选择文件日志是否渲染 ANSI 颜色。
    ///
    /// 参数说明：
    /// - `color`：文件输出是否启用颜色，独立于控制台终端判断。
    ///
    /// 返回：返回更新后的配置，仅在配置被发布后影响后续文件事件。
    pub fn with_color(mut self, color: bool) -> Self {
        self.color = color;
        self
    }
}

/// 业务作用：取得本地时区下用于日志归档的当前日期。
///
/// 参数说明: 无。
///
/// 返回：返回 YYYY-MM-DD 文本，日期随系统墙钟和本地时区变化，不是单调时间。
fn today() -> String {
    chrono::Local::now().format("%Y-%m-%d").to_string()
}

// ────────────────────────────────────────────────────────────────────────────
// 日期边界与文件大小共同决定滚动，避免单日高流量形成无限增长的文件。
// ────────────────────────────────────────────────────────────────────────────
// 活动文件固定 `{dir}/{base}.log`;跨天或超过 max_file_size 时,把它改名为
// `{dir}/{base}_{日期}.{序号i}.log` 归档,再开新的活动文件。每次归档后跑保留清理。
// 由 tracing_appender::non_blocking 的【单一后台线程】独占写入 → 内部无需加锁。
// 一个归档文件的元信息:(日期 YYYY-MM-DD, 当日序号 i, 路径, 字节大小)。
type Archive = (String, u32, PathBuf, u64);

/// 维护滚动日志文件状态；用于写入当前文件并按日期归档。
struct RollingFile {
    dir: PathBuf,
    base: String, // 文件基名:"info" / "error"
    date: String, // 当前活动文件归属日期
    index: u32,   // 当前日期下一个归档序号(%i)
    written: u64, // 当前活动文件已写字节(用于按大小滚动)
    max_file_size: u64,
    max_history_days: i64,
    total_size_cap: u64,
    file: File,
    first_write: bool,
    clean_history_on_start: bool,
}

impl RollingFile {
    /// 业务作用：在指定目录下派生活动日志文件名。
    ///
    /// 参数说明：
    /// - `dir`：文件所在目录。
    /// - `base`：由调用方限定的文件类别名，例如 info 或 error。
    ///
    /// 返回：返回 base.log 路径，不创建文件或校验类别名是否含路径分隔符。
    fn active_path(dir: &Path, base: &str) -> PathBuf {
        dir.join(format!("{base}.log"))
    }

    /// 业务作用：组合日志类别、归档日期和序号的文件路径。
    ///
    /// 参数说明：
    /// - `dir`：归档所在目录。
    /// - `base`：调用方限定的日志类别。
    /// - `date`：调用方提供的归档日期文本。
    /// - `index`：同日同类别的归档序号。
    ///
    /// 返回：返回 base_date.index.log 路径，不检查日期或目标存在性。
    fn archive_path(dir: &Path, base: &str, date: &str, index: u32) -> PathBuf {
        dir.join(format!("{base}_{date}.{index}.log"))
    }

    /// 业务作用：验证日志目录与活动文件可写，准备阶段不归档或删除现有日志。
    ///
    /// 参数说明：
    /// - `cfg`：为文件容量和保留策略
    /// - `base`：为固定日志文件类别。
    ///
    /// 返回：已打开但尚未使用的 writer；路径不可写时失败且不改变当前输出选择。
    fn open(cfg: &FileLogConfig, base: &str) -> io::Result<Self> {
        let dir = PathBuf::from(&cfg.dir);
        create_dir_all(&dir)?;
        let active = Self::active_path(&dir, base);
        let file = OpenOptions::new().create(true).append(true).open(&active)?;
        Ok(Self {
            index: 0,
            dir,
            base: base.to_string(),
            date: today(),
            written: 0,
            max_file_size: cfg.max_file_size,
            max_history_days: cfg.max_history_days,
            total_size_cap: cfg.total_size_cap,
            file,
            first_write: true,
            clean_history_on_start: cfg.clean_history_on_start,
        })
    }

    // 扫描既有 `{base}_{date}.{i}.log`,返回该日期下一个可用序号(max+1,无则 0)。
    /// 业务作用：扫描同类别同日归档并计算下一编号。
    ///
    /// 参数说明：
    /// - `dir`：需要扫描的日志目录。
    /// - `base`：要匹配的日志类别。
    /// - `date`：限定本次编号范围的日期文本。
    ///
    /// 返回：无可解析编号或目录不可读时返回零，否则返回最大 u32 编号加一；达到编号上限时算术可能溢出，不在此返回错误。
    fn next_index(dir: &Path, base: &str, date: &str) -> u32 {
        let prefix = format!("{base}_{date}.");
        let mut max_idx: Option<u32> = None;
        if let Ok(rd) = read_dir(dir) {
            for ent in rd.flatten() {
                let name = ent.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if let Some(rest) = name.strip_prefix(&prefix) {
                    if let Some(num) = rest.strip_suffix(".log") {
                        if let Ok(i) = num.parse::<u32>() {
                            max_idx = Some(max_idx.map_or(i, |m| m.max(i)));
                        }
                    }
                }
            }
        }
        max_idx.map_or(0, |m| m + 1)
    }

    // 需要时滚动:跨天 → 归档旧日期并重置序号;同日超限 → 归档当前序号并 +1。
    /// 业务作用：在首次写入同步文件状态，并按日期或预计大小执行轮转。
    ///
    /// 参数说明：
    /// - `incoming`：本次准备写入的字节数。
    ///
    /// 返回：无需轮转或重命名及重开成功时完成，元数据及文件操作失败返回 IO 错误。先归档再重开，中途失败不回滚；空文件允许首批字节超过大小阈值，归档清理按尽力原则执行。
    fn maybe_roll(&mut self, incoming: usize) -> io::Result<()> {
        if self.first_write {
            // 候选准备时旧 writer 仍可能滚动；首次真正使用时重新取得活动文件，避免写入过时 inode。
            self.file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(Self::active_path(&self.dir, &self.base))?;
            let metadata = self.file.metadata()?;
            self.written = metadata.len();
            self.date = metadata
                .modified()
                .map(|value| {
                    chrono::DateTime::<chrono::Local>::from(value)
                        .format("%Y-%m-%d")
                        .to_string()
                })
                .unwrap_or_else(|_| today());
            self.index = Self::next_index(&self.dir, &self.base, &self.date);
            self.first_write = false;
            if self.clean_history_on_start {
                self.cleanup();
            }
        }
        let now = today();
        let day_changed = now != self.date;
        let size_exceeded = self.written > 0 && self.written + incoming as u64 > self.max_file_size;
        if !day_changed && !size_exceeded {
            return Ok(());
        }
        // 当前活动文件归属 self.date / self.index → 归档
        let active = Self::active_path(&self.dir, &self.base);
        let archived = Self::archive_path(&self.dir, &self.base, &self.date, self.index);
        self.file.flush().ok();
        rename(&active, &archived)?;
        if day_changed {
            self.date = now;
            self.index = 0;
        } else {
            self.index += 1;
        }
        self.file = OpenOptions::new().create(true).append(true).open(&active)?;
        self.written = 0;
        self.cleanup();
        Ok(())
    }

    /// 业务作用：按归档命名模式执行日期及累计容量清理。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：扫描同类别且日期外形匹配的归档，先删除过期项再从最旧项削减容量；读取或删除失败被忽略，不保证最终满足阈值。不检查文件来源或日期的日历有效性，调用方应使用专门日志目录。
    fn cleanup(&self) {
        let prefix = format!("{}_", self.base);
        // 保存归档日期、滚动序号、完整路径和字节数，以便按保留策略选择删除目标。
        let mut archives: Vec<Archive> = Vec::new();
        let Ok(rd) = read_dir(&self.dir) else {
            return;
        };
        for ent in rd.flatten() {
            let name = ent.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(rest) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Some(rest) = rest.strip_suffix(".log") else {
                continue;
            };
            // rest = "YYYY-MM-DD.i"(日期内无 '.','.' 之后是序号)
            let Some((date, idx)) = rest.split_once('.') else {
                continue;
            };
            let Ok(idx) = idx.parse::<u32>() else {
                continue;
            };
            if !is_archive_date(date) {
                continue; // 非 YYYY-MM-DD,跳过(只 len==10 会误删同名外部文件,如 info_XXXXXXXXXX.0.log)
            }
            let size = ent.metadata().map(|m| m.len()).unwrap_or(0);
            archives.push((date.to_string(), idx, ent.path(), size));
        }

        // 1) maxHistory:删除早于 cutoff 的(YYYY-MM-DD 字典序即时间序)
        if self.max_history_days >= 0 {
            let cutoff = (chrono::Local::now() - chrono::Duration::days(self.max_history_days))
                .format("%Y-%m-%d")
                .to_string();
            archives.retain(|(date, _, path, _)| {
                if date.as_str() < cutoff.as_str() {
                    let _ = std::fs::remove_file(path);
                    false
                } else {
                    true
                }
            });
        }

        // 2) totalSizeCap:总量超限则最旧优先删除
        if self.total_size_cap > 0 {
            archives.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1))); // (date, index) 升序=最旧在前
            let mut total: u64 = archives.iter().map(|a| a.3).sum();
            for (_, _, path, size) in &archives {
                if total <= self.total_size_cap {
                    break;
                }
                if std::fs::remove_file(path).is_ok() {
                    total = total.saturating_sub(*size);
                }
            }
        }
    }
}

/// 业务作用：按数字和连字符位置识别归档日期的固定外形。
///
/// 参数说明：
/// - `s`：待识别的文件名日期片段。
///
/// 返回：恰为四位数字、连字符、两位数字、连字符及两位数字时 true；不检查真实年月日范围。
fn is_archive_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
}

impl Write for RollingFile {
    /// 业务作用：在轮转尝试后将字节写入当前持有的日志文件句柄。
    ///
    /// 参数说明：
    /// - `buf`：本次底层输出的字节缓冲区，不保证对应完整事件。
    ///
    /// 返回：返回实际写入字节数并累计计数，文件写入错误传播；轮转错误只输出到 stderr，仍尝试当前句柄写入，不承诺归档失败时路径状态已恢复。
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // 滚动失败不影响写入:记一行 stderr,继续写当前文件
        if let Err(e) = self.maybe_roll(buf.len()) {
            eprintln!("[log] roll failed: {e}");
        }
        let n = self.file.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    /// 业务作用：将活动日志文件的用户态缓冲交给底层文件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：返回底层 flush 成功或 IO 错误，不执行 fsync 或保证断电持久性。
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// 自定义 MakeWriter:写日志时看 info 槽——有真实 writer 就写文件,没有就丢弃(io::sink)。
#[derive(Clone, Copy)]
struct InfoFileWriter;
impl<'a> MakeWriter<'a> for InfoFileWriter {
    type Writer = EitherWriter<NonBlocking, io::Sink>;
    /// 业务作用：为普通文件输出取得当前非阻塞写入器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：文件槽已启用时返回写入器克隆，否则返回丢弃输出的 sink；已取得克隆不因随后关闭槽而自动失效。
    fn make_writer(&'a self) -> Self::Writer {
        match INFO_FILE_WRITER.load_full() {
            Some(nb) => EitherWriter::A((*nb).clone()),
            None => EitherWriter::B(io::sink()),
        }
    }
}

/// 同上,但看 error 槽(仅 ERROR 事件会进到此 layer)。
#[derive(Clone, Copy)]
struct ErrorFileWriter;
impl<'a> MakeWriter<'a> for ErrorFileWriter {
    type Writer = EitherWriter<NonBlocking, io::Sink>;
    /// 业务作用：为独立错误文件输出取得当前非阻塞写入器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：错误文件槽存在时返回克隆，否则 sink；是否只接收 ERROR 由日志 layer 的过滤器控制。
    fn make_writer(&'a self) -> Self::Writer {
        match ERROR_FILE_WRITER.load_full() {
            Some(nb) => EitherWriter::A((*nb).clone()),
            None => EitherWriter::B(io::sink()),
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 公开入口
// ────────────────────────────────────────────────────────────────────────────

/// 业务作用：以 info 作为环境配置缺失时的默认过滤表达式安装全局日志订阅器。
///
/// 参数说明: 无。
///
/// 返回：成功后后续 tracing 事件进入控制台及可选文件层；全局订阅器已经安装等初始化失败会 panic，不自动开启文件日志。
pub fn init() {
    init_with_default("info");
}

// 运行期重置日志级别的"重置器":捕获 reload handle 的闭包(避免命名 handle 的复杂类型)。
type LevelReloader = Box<dyn Fn(EnvFilter) -> Result<EnvFilter, String> + Send + Sync>;
static LEVEL_RELOADER: OnceLock<LevelReloader> = OnceLock::new();

/// 业务作用：以显式默认过滤表达式初始化全局日志输出。
///
/// 参数说明：
/// - `default_filter`：环境 RUST_LOG 解析失败时使用的表达式。
///
/// 返回：安装控制台及动态文件层，不自动开启文件；重复安装全局订阅器会 panic。
pub fn init_with_default(default_filter: &str) {
    init_with_optional_layer(default_filter, None);
}

/// 业务作用：初始化默认日志输出并接入调用方提供的附加采集层。
///
/// 参数说明：
/// - `default_filter`：环境过滤器不可用时使用的默认表达式。
/// - `layer`：随全局 Registry 安装的附加 layer。
///
/// 返回：订阅器安装成功后统一处理事件；附加层的内部生命周期由其实现决定，重复全局初始化会 panic。
pub fn init_with_default_and_layer<L>(default_filter: &str, layer: L)
where
    L: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    init_with_optional_layer(default_filter, Some(layer.boxed()));
}

type OptionalRegistryLayer = Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;

/// 业务作用：组装可重载过滤器、可选采集层、控制台和动态文件输出。
///
/// 参数说明：
/// - `default_filter`：RUST_LOG 不可解析时采用的过滤表达式。
/// - `extra_layer`：可选的全局 Registry 附加层。
///
/// 返回：安装成功时保存过滤重载入口，控制台颜色按终端判断、文件颜色按槽设置；订阅器安装失败 panic，不创建文件目录。
fn init_with_optional_layer(default_filter: &str, extra_layer: Option<OptionalRegistryLayer>) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    // 把 filter 包成 reload::Layer:拿到 handle 后能在运行期 reload 成新级别。
    let (filter_layer, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    let _ = LEVEL_RELOADER.set(Box::new(move |mut filter| {
        reload_handle
            .modify(|current| std::mem::swap(current, &mut filter))
            .map_err(|error| error.to_string())?;
        Ok(filter)
    }));

    // 控制台是否上色:stdout 是 TTY 才上色(颜色只在终端,重定向/管道时自动关,
    // 不会把转义码写进重定向文件);文件 layer 永远纯文本(除非 FileLogConfig.color 显式开)。
    let console_ansi = std::io::stdout().is_terminal();
    let console_color = if console_ansi {
        Colorize::Always
    } else {
        Colorize::Never
    };

    tracing_subscriber::registry()
        // Option<Layer> 的 None 分支是零行为；Some 用于 OTel 等进程级扩展，避免应用另建
        // 第二个 subscriber（tracing 全局 subscriber 只能安装一次）。
        .with(extra_layer)
        // 共享过滤句柄使各输出层同时采用当前日志级别。
        .with(filter_layer)
        // 控制台 layer 持续输出，TTY 环境按配置显示颜色。
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(console_ansi)
                .event_format(LogFormatter {
                    color: console_color,
                }),
        )
        // info 文件 layer 接收过滤后的各级别事件；安装前丢弃写入，文件颜色默认关闭。
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(InfoFileWriter)
                .event_format(LogFormatter {
                    color: Colorize::FileSlot,
                }),
        )
        // error 文件 layer 仅接收 ERROR 事件。
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(ErrorFileWriter)
                .event_format(LogFormatter {
                    color: Colorize::FileSlot,
                })
                .with_filter(filter_fn(|meta| *meta.level() == Level::ERROR)),
        )
        .init();
}

/// 业务作用：尝试用新过滤表达式更新已安装的日志过滤层。
///
/// 参数说明：
/// - `level`：tracing EnvFilter 支持的级别或目标过滤表达式。
///
/// 返回：有效且重载成功后影响后续过滤；无效表达式、未初始化或重载失败被忽略，不返回执行结果。
pub fn set_level(level: &str) {
    if let Ok(filter) = EnvFilter::try_new(level) {
        let _ = install_level(filter);
    }
}

/// 业务作用：安装已解析的日志过滤器，不在提交阶段重新解析配置。
///
/// 参数说明：
/// - `filter`：为已验证的过滤器。
///
/// 返回：已初始化且过滤器可替换时成功；否则不改变文件输出。
fn install_level(filter: EnvFilter) -> Result<(), String> {
    replace_level(filter).map(|_| ())
}

/// 业务作用：切换过滤器并保留精确旧值，使配置事务可在不重新解析的情况下恢复。
///
/// 参数说明：
/// - `filter`：已完成解析的候选过滤器。
///
/// 返回：成功返回先前过滤器；句柄不可用时不改变过滤状态。
fn replace_level(filter: EnvFilter) -> Result<EnvFilter, String> {
    LEVEL_RELOADER
        .get()
        .ok_or_else(|| "log subscriber is not initialized".to_owned())?(filter)
}

/// 接入文件日志的守卫。**必须持有到进程结束**,否则一 drop,tracing-appender
/// 的后台刷盘线程停止、缓冲日志丢失。
#[must_use = "LogGuard 一旦 drop 后台刷盘线程即停止、缓冲日志丢失,务必持有到进程结束"]
pub struct LogGuard(#[allow(dead_code)] Vec<WorkerGuard>);

/// 业务作用：用默认文件策略按显式目录尝试开启日志输出。
///
/// 参数说明：
/// - `path`：可选目录文本，None 或仅含空白时不开启。
///
/// 返回：成功返回需持有的后台写入 guard；未提供目录或打开失败返回 None，打开失败可写 stderr。已有全局 writer 仅在准备成功后被替换。
pub fn enable_file_logging(path: Option<&str>) -> Option<LogGuard> {
    let dir = path?;
    if dir.trim().is_empty() {
        return None;
    }
    enable_file_logging_with(&FileLogConfig::new(dir))
}

/// 接入文件日志时的目录与文件打开错误。
#[derive(Debug)]
pub enum LogOpenError {
    /// 目录为空字符串。
    EmptyDir,
    /// 打开 `info`/`error` 滚动文件失败(目录不可建/不可写/被普通文件占用等)。
    Open {
        /// 出错的 appender:`"info"` 或 `"error"`。
        appender: &'static str,
        /// 目标目录。
        dir: String,
        /// 底层 IO 错误。
        source: io::Error,
    },
}

impl std::fmt::Display for LogOpenError {
    /// 业务作用：展示文件日志打开失败的类别及文件系统原因。
    ///
    /// 参数说明：
    /// - `f`：接收错误文本的格式化目标。
    ///
    /// 返回：写入空目录提示或类别、目录及 IO 原因，格式化失败传播；目录文本不在此脱敏。
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::EmptyDir => write!(f, "log dir is empty"),
            Self::Open {
                appender,
                dir,
                source,
            } => write!(f, "open {appender} log under '{dir}' failed: {source}"),
        }
    }
}

impl std::error::Error for LogOpenError {
    /// 业务作用：提供日志打开错误对应的底层 IO 原因链。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Open 返回底层 IO 错误引用，EmptyDir 返回 None，不复制或记录错误。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Open {
                source,
                ..
            } => Some(source),
            Self::EmptyDir => None,
        }
    }
}

/// 业务作用：准备全部文件输出资源后发布新文件日志槽。
///
/// 参数说明：
/// - `cfg`：目录、轮转、清理及独立错误文件配置。
///
/// 返回：成功返回调用方必须持有的 guard；空目录或任一文件打开失败返回 LogOpenError 并保留旧 writer 槽。已创建的目录或文件不会因此删除，多槽发布不是同一原子操作。
pub fn try_enable_file_logging_with(cfg: &FileLogConfig) -> Result<LogGuard, LogOpenError> {
    Ok(prepare_file_logging(cfg)?.install())
}

/// 文件已打开且刷盘线程已建立的候选，尚未写入全局输出槽。
struct PreparedFileLogging {
    info: NonBlocking,
    error: Option<NonBlocking>,
    guards: LogGuard,
    color: bool,
}

impl PreparedFileLogging {
    /// 业务作用：将已准备的文件 writer 切换为当前输出，并移交刷盘线程责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：必须持有到输出被替换后才释放的日志守卫；不执行文件打开或刷盘等待。
    fn install(self) -> LogGuard {
        FILE_COLOR.store(self.color, Ordering::Relaxed);
        // 文件和 worker 已全部准备后才发布 writer，打开失败不能替换仍可用的旧槽；各槽依次切换。
        INFO_FILE_WRITER.store(Some(Arc::new(self.info)));
        ERROR_FILE_WRITER.store(self.error.map(Arc::new));
        self.guards
    }
}

/// 业务作用：在发布锁之外准备文件和后台 writer，失败不改变已生效的输出。
///
/// 参数说明：
/// - `cfg`：为文件日志配置。
///
/// 返回：尚未安装的候选；目录或文件无法打开时返回错误。
fn prepare_file_logging(cfg: &FileLogConfig) -> Result<PreparedFileLogging, LogOpenError> {
    if cfg.dir.trim().is_empty() {
        return Err(LogOpenError::EmptyDir);
    }
    // ── 准备阶段:先把需要的 roller 全部打开,任一失败即返回 Err,此时尚未触碰任何全局 writer 槽 ──
    let info = RollingFile::open(cfg, "info").map_err(|source| LogOpenError::Open {
        appender: "info",
        dir: cfg.dir.clone(),
        source,
    })?;
    let (info_nb, info_guard) = tracing_appender::non_blocking(info);
    let error_pair = if cfg.split_error_file {
        let error = RollingFile::open(cfg, "error").map_err(|source| LogOpenError::Open {
            appender: "error",
            dir: cfg.dir.clone(),
            source,
        })?;
        Some(tracing_appender::non_blocking(error))
    } else {
        None
    };

    let mut guards = vec![info_guard];
    let error = error_pair.map(|(writer, guard)| {
        guards.push(guard);
        writer
    });
    Ok(PreparedFileLogging {
        info: info_nb,
        error,
        guards: LogGuard(guards),
        color: cfg.color,
    })
}

/// 业务作用：用当前日志格式记录已启用文件输出的配置状态。
///
/// 参数说明：
/// - `cfg`：已经成功安装的文件配置。
///
/// 返回：提交 info 级配置状态事件，是否实际输出由当前过滤和写入器决定，不提供落盘确认。
pub(crate) fn emit_file_logging_enabled(cfg: &FileLogConfig) {
    tracing::info!(
        "file logging enabled (post-nacos): {}/info.log{} (max_file_size={}B, max_history_days={}, total_size_cap={}B, color={})",
        cfg.dir,
        if cfg.split_error_file {
            " + error.log"
        } else {
            ""
        },
        cfg.max_file_size,
        cfg.max_history_days,
        cfg.total_size_cap,
        cfg.color
    );
}

/// 业务作用：按配置尝试开启文件日志并记录启用状态。
///
/// 参数说明：
/// - `cfg`：明确指定目录和轮转保留策略的配置。
///
/// 返回：准备及安装成功返回 guard，失败写 stderr 并返回 None；启用事件不代表已经完成持久化。
pub fn enable_file_logging_with(cfg: &FileLogConfig) -> Option<LogGuard> {
    match try_enable_file_logging_with(cfg) {
        Ok(g) => {
            emit_file_logging_enabled(cfg); // 用当前全局 pattern 写状态行
            Some(g)
        }
        Err(e) => {
            eprintln!("[log] {e}");
            None
        }
    }
}

/// 业务作用：从全局输出槽撤下普通和错误文件写入器。
///
/// 参数说明: 无。
///
/// 返回：后续从空槽取得的 writer 丢弃输出；已经取得的写入器及排队字节不会立即撤销，后台资源仍由各 guard 持有和释放。
pub fn disable_file_logging() {
    INFO_FILE_WRITER.store(None);
    ERROR_FILE_WRITER.store(None);
}

// ────────────────────────────────────────────────────────────────────────────
// 按 `LOG_PATTERN` 模板格式化，字段展开由 pattern 模块负责。
// ────────────────────────────────────────────────────────────────────────────
// 输出形如:
//   2026-05-27 12:34:56.789 [INFO ] [tokio-runtime-worker] a.s.service::main[260] - 数据库连接已建立
//
// 默认格式由 DEFAULT_LOG_PATTERN 定义，包含时间、级别、线程与调用位置。
// 业务可经 LogConfig.pattern 覆盖;颜色是否输出由 `color` 决定(控制台=TTY,文件=FILE_COLOR)。

/// 颜色来源:控制台固定(Always/Never,按 TTY 在 init 时定);文件读运行期 FILE_COLOR。
#[derive(Clone, Copy)]
enum Colorize {
    Always,
    Never,
    FileSlot,
}

/// 保存已编译的日志格式器；用于把事件渲染成最终文本。
pub struct LogFormatter {
    color: Colorize,
}

impl LogFormatter {
    /// 业务作用：根据当前渲染器策略确定是否输出 ANSI 颜色。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定策略直接返回开关；文件槽策略读取当前文件颜色标志，不修改输出配置。
    fn ansi(&self) -> bool {
        match self.color {
            Colorize::Always => true,
            Colorize::Never => false,
            Colorize::FileSlot => FILE_COLOR.load(Ordering::Relaxed),
        }
    }
}

impl<S, N> FormatEvent<S, N> for LogFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    /// 业务作用：用本次取得的格式树和颜色策略渲染一个 tracing 事件。
    ///
    /// 参数说明：
    /// - `ctx`：提供 span 及事件字段格式化上下文。
    /// - `writer`：接收当前事件文本的目标写入器。
    /// - `event`：要按当前格式渲染的 tracing 事件。
    ///
    /// 返回：优先使用已发布格式，否则使用惰性默认格式；格式化失败传播，不在此执行事件内容脱敏或文件持久化确认。
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        // 取运行期 pattern(未配置则用惰性编译的 DEFAULT_LOG_PATTERN),三个 layer 共用同一 pattern。
        let guard = LOG_PATTERN.load();
        let pattern = guard.as_deref().unwrap_or_else(|| default_pattern());
        pattern.render(ctx, writer, event, self.ansi())
    }
}
