//! 统一日志初始化、文件滚动和格式化支持。
//!
//! 提供 `tracing` 订阅器装配、运行期日志级别切换、按大小滚动的 info/error 文件输出，
//! 以及可由配置模块驱动的日志管理入口。
// ============================================================================
// nlog —— NASA 日志(对照 原实现 原工具包 的 logback-原框架.xml)
//
// 业务经门面引用:`use nasa::log;` → `log::init();`(一行完成全局注册)。
// 实现 = tracing + tracing-subscriber(≈ SLF4J + Logback)。
//
// 【对照 logback-原框架.xml(原框架-boot-starter-原工具包)】
//   - PatternLayout 风格 formatter:`yyyy-MM-dd HH:mm:ss.SSS [LEVEL] [thread] logger[line] - msg`
//   - 独立 error.log:仅 ERROR(对照 error appender + ThresholdFilter ERROR)
//   - 按天 + 按大小滚动:`{base}.log` → 归档 `{base}_{yyyy-MM-dd}.{i}.log`
//     (对照 SizeAndTimeBasedRollingPolicy + maxFileSize)
//   - 保留清理:maxHistory(天)+ totalSizeCap(归档总量)+ cleanHistoryOnStart
//     (原 rust-simple-mvc 版**缺这三项,会涨爆磁盘**——本次补齐)
//   - 运行期级别热切(reload):配合 nacos——先控制台,配置就绪后热接文件 + 改级别
//
// 【推荐用法:配置抽象(配合 nacos)】业务只声明 `pub log: nasa::log::LogConfig` 并反序列化 YAML/Nacos,
//   由 nlog 负责 LogConfig→FileLogConfig 映射 / 默认值 / 单位解析 / 路径策略 / 启停。
//     use nasa::log::{LogContext, LogManager};
//     let mut log_manager = LogManager::bootstrap(boot.log.as_ref());   // 最早期:仅控制台
//     // … 拉取 nacos 配置 …
//     let ctx = LogContext::with_app_name(&cfg.server.name);            // 缺 path = 只控制台(Rust 现状)
//     // 想对齐 原实现 /usr/local/logs/{app}:LogContext::原实现_default(&cfg.server.name)
//     log_manager.apply(&cfg.log, &ctx)?;                              // set_level + 接文件
//     // log_manager 必须持有到进程结束(同 LogGuard);Nacos 热更新复用同一 manager 再 apply。
//
// 【底层用法(无需配置抽象)】
//     use nasa::log;
//     log::init_with_default("info");            // 最早期:仅控制台
//     log::set_level("info,my_app=debug");        // 热切级别
//     let _g = log::enable_file_logging(Some("/usr/local/logs/my-app")); // 接文件
//     // _g(LogGuard)必须持有到进程结束,否则后台刷盘线程停止、缓冲日志丢失
//     // 关闭文件日志回到只控制台:log::disable_file_logging() + drop 旧 guard
// ============================================================================
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
/// `LogConfig → FileLogConfig` 映射从各业务 main 收敛到此(对照 原实现 logback-原框架.xml 的应用映射)。
mod config;
pub use config::*;

/// logback `LOG_PATTERN` 子集的解析 + 渲染(对照 logback PatternLayout)。
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
// 文件 layer 是否给内容上色(对照 FileLogConfig.color);由 enable_file_logging_with 设置,
// LogFormatter 每条日志读它决定文件输出是否带 ANSI(控制台另由 TTY 决定,与此无关)。
static FILE_COLOR: AtomicBool = AtomicBool::new(false);

// 运行期可热替换的输出 pattern 槽(对照 logback LOG_PATTERN property,被三个 layer 共用)。
// None = 未配置 → 用 DEFAULT_LOG_PATTERN(惰性编译一次)。LogConfig/LogManager::apply 解析后塞入。
static LOG_PATTERN: ArcSwapOption<CompiledLogPattern> = ArcSwapOption::const_empty();
static DEFAULT_PATTERN: OnceLock<CompiledLogPattern> = OnceLock::new();

/// 业务作用：返回默认日志格式模板；用于未配置格式时保持统一输出。
fn default_pattern() -> &'static CompiledLogPattern {
    DEFAULT_PATTERN.get_or_init(|| {
        CompiledLogPattern::parse(DEFAULT_LOG_PATTERN).expect("DEFAULT_LOG_PATTERN must compile")
    })
}

/// 业务作用：【运行期】热替换全局输出 pattern(被 [`config::LogManager::apply`]/[`config::apply_config`] 提交)。
pub(crate) fn set_log_pattern(pattern: std::sync::Arc<CompiledLogPattern>) {
    LOG_PATTERN.store(Some(pattern));
}

// ── logback 默认值(对照 logback-原框架.xml 的 property)──
const DEFAULT_MAX_FILE_SIZE: u64 = 500 * 1024 * 1024; // maxFileSize=500MB
const DEFAULT_MAX_HISTORY_DAYS: i64 = 30; // maxHistory=30
const DEFAULT_TOTAL_SIZE_CAP: u64 = 30 * 1024 * 1024 * 1024; // totalSizeCap=30GB

/// 文件日志配置(对照 logback `RollingFileAppender` + `SizeAndTimeBasedRollingPolicy`)。
///
/// `dir` 对照 logback `LOG_PATH`(默认 `/usr/local/logs/${原框架.application.name}`,即按服务名分目录);
/// 文件基名固定 `info`/`error`(对照 logback 两个 appender),业务无需传服务名。
#[derive(Clone, Debug)]
pub struct FileLogConfig {
    /// 日志目录(对照 logback `LOG_PATH`)。
    pub dir: String,
    /// 单文件大小上限(字节),超过即滚动(对照 `maxFileSize`,默认 500MB)。
    pub max_file_size: u64,
    /// 归档保留天数,更早的归档删除(对照 `maxHistory`,默认 30)。
    pub max_history_days: i64,
    /// 归档总量上限(字节),超出按最旧优先删除(对照 `totalSizeCap`,默认 30GB)。
    pub total_size_cap: u64,
    /// 启动时立即清理过期/超量历史(对照 `cleanHistoryOnStart`,默认 true)。
    pub clean_history_on_start: bool,
    /// 额外写一份仅 ERROR 的 `error.log`(对照 error appender + ThresholdFilter,默认 true)。
    pub split_error_file: bool,
    /// 文件内容是否带 ANSI 颜色(默认 false=纯文本,grep/wc 友好)。
    /// 置 true → info.log/error.log 写入彩色转义码,`tail` 时也能看到颜色(对照 logback
    /// 把 `%highlight` 写进文件的行为);代价是 grep/awk 会看到 `\x1b[..` 转义码、文件略大。
    /// 注意:**控制台**的颜色与本项无关——控制台按 stdout 是否 TTY 自动上色(重定向时自动关)。
    pub color: bool,
}

impl FileLogConfig {
    /// 业务作用：用 logback 默认参数构造:`max_file_size=500MB`、`max_history_days=30`、
    /// `total_size_cap=30GB`、`clean_history_on_start=true`、`split_error_file=true`、`color=false`。
    ///
    /// # 参数
    /// - `dir`: 文件日志目录,会在其下写入 `info.log` 和可选的 `error.log`。
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

    /// 业务作用：单文件上限按 **MB** 设置(`None` 不改,保持当前值)。对照 logback `maxFileSize`。
    ///
    /// # 参数
    /// - `mb`: 单个滚动日志文件的 MB 上限;`None` 表示保留当前配置。
    pub fn with_max_file_size_mb(mut self, mb: Option<u64>) -> Self {
        if let Some(mb) = mb {
            self.max_file_size = mb.saturating_mul(1024 * 1024);
        }
        self
    }

    /// 业务作用：归档保留天数(`None` 不改)。对照 logback `maxHistory`。
    ///
    /// # 参数
    /// - `days`: 归档文件保留天数;`None` 表示保留当前配置。
    pub fn with_max_history_days(mut self, days: Option<i64>) -> Self {
        if let Some(d) = days {
            self.max_history_days = d;
        }
        self
    }

    /// 业务作用：归档总量上限按 **MB** 设置(`None` 不改)。对照 logback `totalSizeCap`。
    ///
    /// # 参数
    /// - `mb`: 归档总容量 MB 上限;`None` 表示保留当前配置。
    pub fn with_total_size_cap_mb(mut self, mb: Option<u64>) -> Self {
        if let Some(mb) = mb {
            self.total_size_cap = mb.saturating_mul(1024 * 1024);
        }
        self
    }

    /// 业务作用：文件是否带 ANSI 颜色(`tail` 可见色,代价 grep 见转义码)。
    ///
    /// # 参数
    /// - `color`: `true` 表示文件日志写入 ANSI 颜色转义码,`false` 表示纯文本。
    pub fn with_color(mut self, color: bool) -> Self {
        self.color = color;
        self
    }
}

/// 业务作用：返回当前本地日期字符串 `YYYY-MM-DD`。
///
/// 文件滚动命名与日志时间戳都使用 Local 时区,确保归档日期和业务看到的日志日期一致。
fn today() -> String {
    chrono::Local::now().format("%Y-%m-%d").to_string()
}

// ────────────────────────────────────────────────────────────────────────────
// 按天 + 按大小滚动文件(logback SizeAndTimeBasedRollingPolicy 风格)
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
}

impl RollingFile {
    /// 业务作用：计算当前日志文件路径；用于确定正在写入的目标文件。
    ///
    /// # 参数
    /// - `dir`: 日志、存储或配置文件所在目录。
    /// - `base`: 归档、配置或路径拼接使用的基础名称。
    fn active_path(dir: &Path, base: &str) -> PathBuf {
        dir.join(format!("{base}.log"))
    }

    /// 业务作用：计算归档日志文件路径；用于滚动时生成带日期和序号的文件名。
    ///
    /// # 参数
    /// - `dir`: 日志、存储或配置文件所在目录。
    /// - `base`: 归档、配置或路径拼接使用的基础名称。
    /// - `date`: 日志归档日期字符串。
    /// - `index`: 列表下标、归档序号或字段位置。
    fn archive_path(dir: &Path, base: &str, date: &str, index: u32) -> PathBuf {
        dir.join(format!("{base}_{date}.{index}.log"))
    }

    /// 业务作用：打开 open 资源；用于准备后续读写操作。
    ///
    /// # 参数
    /// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
    /// - `base`: 归档、配置或路径拼接使用的基础名称。
    fn open(cfg: &FileLogConfig, base: &str) -> io::Result<Self> {
        let dir = PathBuf::from(&cfg.dir);
        create_dir_all(&dir)?;
        let active = Self::active_path(&dir, base);
        let today = today();

        // 重启跨天:活动文件存在且最后修改日期非今天 → 先归档(避免昨天尾巴混进今天)。
        if let Ok(meta) = std::fs::metadata(&active) {
            if let Ok(modified) = meta.modified() {
                let fdate = chrono::DateTime::<chrono::Local>::from(modified)
                    .format("%Y-%m-%d")
                    .to_string();
                if fdate != today {
                    let idx = Self::next_index(&dir, base, &fdate);
                    let _ = rename(&active, Self::archive_path(&dir, base, &fdate, idx));
                }
            }
        }

        // append 打开:同一天内进程重启续写同一活动文件,不覆盖
        let file = OpenOptions::new().create(true).append(true).open(&active)?;
        let written = file.metadata().map(|m| m.len()).unwrap_or(0);
        let rf = Self {
            index: Self::next_index(&dir, base, &today),
            dir,
            base: base.to_string(),
            date: today,
            written,
            max_file_size: cfg.max_file_size,
            max_history_days: cfg.max_history_days,
            total_size_cap: cfg.total_size_cap,
            file,
        };
        if cfg.clean_history_on_start {
            rf.cleanup();
        }
        Ok(rf)
    }

    // 扫描既有 `{base}_{date}.{i}.log`,返回该日期下一个可用序号(max+1,无则 0)。
    /// 业务作用：扫描既有 `{base}_{date}.{i}.log`,返回该日期下一个可用序号(max+1,无则 0)。
    /// # 参数
    /// - `dir`: 日志、存储或配置文件所在目录。
    /// - `base`: 归档、配置或路径拼接使用的基础名称。
    /// - `date`: 日志归档日期字符串。
    fn next_index(dir: &Path, base: &str, date: &str) -> u32 {
        let prefix = format!("{base}_{date}.");
        let mut max_idx: Option<u32> = None;
        if let Ok(rd) = read_dir(dir) {
            for ent in rd.flatten() {
                let name = ent.file_name();
                let Some(name) = name.to_str() else { continue };
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
    /// 业务作用：需要时滚动:跨天 → 归档旧日期并重置序号;同日超限 → 归档当前序号并 +1。
    /// # 参数
    /// - `incoming`: 即将写入日志文件的字节数。
    fn maybe_roll(&mut self, incoming: usize) -> io::Result<()> {
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

    /// 业务作用：清理超出保留策略的归档日志。
    ///
    /// 对照 logback `maxHistory + totalSizeCap`:只删除形如 `{base}_YYYY-MM-DD.{i}.log` 的归档,
    /// 先按日期淘汰,再按总大小从最旧归档开始删除；活动文件 `{base}.log` 不匹配归档前缀,不会被误删。
    fn cleanup(&self) {
        let prefix = format!("{}_", self.base);
        // (date, index, path, size)
        let mut archives: Vec<Archive> = Vec::new();
        let Ok(rd) = read_dir(&self.dir) else { return };
        for ent in rd.flatten() {
            let name = ent.file_name();
            let Some(name) = name.to_str() else { continue };
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

/// 业务作用：校验归档文件名的日期段严格为 `YYYY-MM-DD`(4 位数-2 位数-2 位数),而非仅 `len()==10`。
/// 用于保留清理只删真正的归档,不误删同名外部文件(如 `info_helloworld.0.log`)。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
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
    /// 业务作用：写入 write 内容；用于输出数据或持久化状态。
    ///
    /// # 参数
    /// - `buf`: 需要写入目标日志 writer 的字节缓冲。
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // 滚动失败不影响写入:记一行 stderr,继续写当前文件
        if let Err(e) = self.maybe_roll(buf.len()) {
            eprintln!("[log] roll failed: {e}");
        }
        let n = self.file.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    /// 业务作用：刷新缓冲内容；用于确保已写数据及时落地。
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// 自定义 MakeWriter:写日志时看 info 槽——有真实 writer 就写文件,没有就丢弃(io::sink)。
#[derive(Clone, Copy)]
struct InfoFileWriter;
impl<'a> MakeWriter<'a> for InfoFileWriter {
    type Writer = EitherWriter<NonBlocking, io::Sink>;
    /// 业务作用：创建 writer；用于向调用方提供临时工作对象。
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
    /// 业务作用：创建 writer；用于向调用方提供临时工作对象。
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

/// 业务作用：初始化全局日志(仅控制台,默认 `info` 级别)。
pub fn init() {
    init_with_default("info");
}

// 运行期重置日志级别的"重置器":捕获 reload handle 的闭包(避免命名 handle 的复杂类型)。
type LevelReloader = Box<dyn Fn(&str) + Send + Sync>;
static LEVEL_RELOADER: OnceLock<LevelReloader> = OnceLock::new();

/// 业务作用：初始化全局日志:原实现 logback 风格 formatter + `RUST_LOG`/默认级别过滤。
///
/// # 参数
/// - `default_filter`: 未设置 `RUST_LOG` 时使用的默认 EnvFilter 表达式。
///
/// 在 main 最早期调用(nacos 之前)。此时:
///   - 级别 = `default_filter`(或 `RUST_LOG`),且【可被 [`set_level`] 运行期重置】
///   - info/error 文件 layer 已挂载但写入丢弃 → 实际只输出控制台
///
/// 待 nacos 配置就绪后,再调用 [`set_level`] + [`enable_file_logging`] 接入最终配置。
pub fn init_with_default(default_filter: &str) {
    init_with_optional_layer(default_filter, None);
}

/// 业务作用：初始化全局日志并附加一个 `tracing` layer。
///
/// 该入口供 OpenTelemetry 等与日志共用同一 subscriber 栈的组件使用；额外 layer 仍位于全局
/// reload filter 之下，不会绕过运行期日志级别。初始化必须且只能在进程启动早期调用一次。
pub fn init_with_default_and_layer<L>(default_filter: &str, layer: L)
where
    L: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    init_with_optional_layer(default_filter, Some(layer.boxed()));
}

type OptionalRegistryLayer = Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;

/// 业务作用：组装一次性全局 subscriber、可热更新过滤器、控制台层、文件层及可选扩展层。
fn init_with_optional_layer(default_filter: &str, extra_layer: Option<OptionalRegistryLayer>) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    // 把 filter 包成 reload::Layer:拿到 handle 后能在运行期 reload 成新级别。
    let (filter_layer, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    let _ = LEVEL_RELOADER.set(Box::new(move |level: &str| {
        match EnvFilter::try_new(level) {
            Ok(f) => {
                let _ = reload_handle.reload(f);
            }
            Err(e) => eprintln!("[log] invalid level '{level}': {e}"),
        }
    }));

    // 控制台是否上色:stdout 是 TTY 才上色(对照 原实现——颜色只在终端,重定向/管道时自动关,
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
        // 可重置的全局级别过滤(对照 logback <root level>)
        .with(filter_layer)
        // 控制台 layer(始终生效,对照 STDOUT appender;TTY 时彩色,对照 %highlight/%magenta/%cyan)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(console_ansi)
                .event_format(LogFormatter {
                    color: console_color,
                }),
        )
        // info 文件 layer:全量级别(对照 info appender),writer 初始丢弃;颜色随 FILE_COLOR(默认关)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(InfoFileWriter)
                .event_format(LogFormatter {
                    color: Colorize::FileSlot,
                }),
        )
        // error 文件 layer:仅 ERROR(对照 error appender + ThresholdFilter ERROR)
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

/// 业务作用：【nacos 之后调用】把日志级别热切到最终配置(如 `"info,my_app=debug"`)。
/// 在 [`init`]/[`init_with_default`] 之前调用无效(reloader 尚未就绪)。
///
/// # 参数
/// - `level`: 新的 EnvFilter 表达式,例如全局级别或模块级别组合。
pub fn set_level(level: &str) {
    if let Some(reloader) = LEVEL_RELOADER.get() {
        reloader(level);
    }
}

/// 接入文件日志的守卫。**必须持有到进程结束**,否则一 drop,tracing-appender
/// 的后台刷盘线程停止、缓冲日志丢失。
#[must_use = "LogGuard 一旦 drop 后台刷盘线程即停止、缓冲日志丢失,务必持有到进程结束"]
pub struct LogGuard(#[allow(dead_code)] Vec<WorkerGuard>);

/// 业务作用：【nacos 之后调用】按 logback 默认参数接入文件日志(`info.log` + `error.log`)。
///
/// - `path` 为 `None`/空 → 不接文件(保持只控制台),返回 `None`
/// - `path` 有值 → 在该目录下创建滚动文件 + 非阻塞写,原子塞进全局槽,此后日志同时落文件
///
/// 等价 `enable_file_logging_with(&FileLogConfig::new(path))`。
///
/// # 参数
/// - `path`: 日志输出目录;为空或 `None` 时不启用文件日志。
pub fn enable_file_logging(path: Option<&str>) -> Option<LogGuard> {
    let dir = path?;
    if dir.trim().is_empty() {
        return None;
    }
    enable_file_logging_with(&FileLogConfig::new(dir))
}

/// 接入文件日志的运行期打开错误(对照 logback appender 启动失败)。
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
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
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
    /// 业务作用：返回底层错误来源；用于错误链追踪。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Open { source, .. } => Some(source),
            Self::EmptyDir => None,
        }
    }
}

/// 业务作用：【nacos 之后调用】按完整配置接入文件日志(滚动 + 保留清理 + 可选 error.log),**严格版**:
/// 任一需要的文件打不开 → 返回 `Err`,**不改任何 writer 槽**(避免半更新);全部就绪后才原子替换。
///
/// # 参数
/// - `cfg`: 文件日志配置,包括目录、滚动大小、保留天数、总容量、error.log 和颜色设置。
///
/// 对照运行期 IO 失败必须可见,不能吞成"成功"。`LogManager::apply` 用本函数;
/// 旧 [`enable_file_logging_with`] 降级调用本函数(IO 失败 → `eprintln` + `None`,保持兼容)。
/// 返回的 [`LogGuard`] **必须被持有到进程结束**(见其文档)。
pub fn try_enable_file_logging_with(cfg: &FileLogConfig) -> Result<LogGuard, LogOpenError> {
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

    // ── 提交阶段:全部就绪,原子替换 writer 槽 ──
    // 文件是否上色(默认 false=纯文本,对照 原实现 文件无转义码);控制台另由 TTY 决定。
    FILE_COLOR.store(cfg.color, Ordering::Relaxed);
    INFO_FILE_WRITER.store(Some(Arc::new(info_nb)));
    let mut guards = vec![info_guard];
    // **运行期重配置必须覆盖旧槽**:`split_error_file=false` 时清空 `ERROR_FILE_WRITER`,
    // 否则上一代 error writer 残留、继续往旧目录写。
    match error_pair {
        Some((error_nb, error_guard)) => {
            ERROR_FILE_WRITER.store(Some(Arc::new(error_nb)));
            guards.push(error_guard);
        }
        None => ERROR_FILE_WRITER.store(None),
    }

    // 注:**不在此处**写 "file logging enabled" 状态行——此时 writer 已替换但新 pattern 可能
    // 尚未 store,旧 pattern 会污染新文件首行。状态行由调用方在 set_log_pattern 之后经 emit_file_logging_enabled 写。
    Ok(LogGuard(guards))
}

/// 业务作用：写一条 "file logging enabled" 状态行(用**当前**全局 pattern)。`LogManager::apply`/`enable_file_logging_with`
/// 在 writer 接入 + pattern 提交**之后**调用,确保状态行也遵循新 pattern。
pub(crate) fn emit_file_logging_enabled(cfg: &FileLogConfig) {
    tracing::info!(
        "file logging enabled (post-nacos): {}/info.log{} (max_file_size={}B, max_history_days={}, total_size_cap={}B, color={})",
        cfg.dir,
        if cfg.split_error_file { " + error.log" } else { "" },
        cfg.max_file_size,
        cfg.max_history_days,
        cfg.total_size_cap,
        cfg.color
    );
}

/// 业务作用：【nacos 之后调用】按完整配置接入文件日志(滚动 + 保留清理 + 可选 error.log)。best-effort 版:
/// 打开失败 → `eprintln` + 返回 `None`(**全有或全无**,失败时不改 writer 槽)。需失败可见请用
/// [`try_enable_file_logging_with`]。返回的 [`LogGuard`] **必须被持有到进程结束**。
///
/// # 参数
/// - `cfg`: 文件日志配置,包括目录、滚动大小、保留天数、总容量、error.log 和颜色设置。
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

/// 业务作用：【运行期】关闭文件日志,回到**只控制台**:清空 info/error 两个 writer 槽(此后文件 layer 写入丢弃)。
///
/// 用于 Nacos 热更新把 `path` 置空时真正停掉文件输出(`enable_file_logging(None)` 是 no-op、不会关已接入的文件,
/// 故另设此显式入口)。**调用方仍应 drop 旧 [`LogGuard`]**,以停止旧的后台刷盘线程(否则线程空转到进程结束)。
pub fn disable_file_logging() {
    INFO_FILE_WRITER.store(None);
    ERROR_FILE_WRITER.store(None);
}

// ────────────────────────────────────────────────────────────────────────────
// 原实现 风格 formatter(对照 logback PatternLayout)—— 委托给可配置 `LOG_PATTERN`(见 `pattern` 模块)
// ────────────────────────────────────────────────────────────────────────────
// 输出形如:
//   2026-05-27 12:34:56.789 [INFO ] [tokio-runtime-worker] r.s.mvc::main[260] - mysql connection ok
//
// 默认 pattern 逐字对照 logback:`%d{yyyy-MM-dd HH:mm:ss.SSS} %highlight([%-5level]) ...`(见 DEFAULT_LOG_PATTERN)。
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
    /// 业务作用：读取 ansi 状态；用于向调用方暴露当前运行信息。
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
    /// 业务作用：格式化单条日志事件；用于写入控制台或文件前生成文本。
    ///
    /// # 参数
    /// - `ctx`: 本次格式化、日志或运行阶段的上下文。
    /// - `writer`: 日志或格式化内容的目标 writer。
    /// - `event`: 当前 tracing 日志事件,由 pattern 渲染器读取字段和元数据。
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
