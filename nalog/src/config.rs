//! 日志配置抽象：把应用侧 `LogConfig → FileLogConfig` 映射、默认值、单位解析、路径策略和启停重配置
//! 收敛到 nalog。业务只需声明 `pub log: nasa::log::LogConfig` 并反序列化 YAML/Nacos；nalog 不读取
//! YAML/Nacos/env，只消费已反序列化的 `LogConfig`。
//!
//! ```ignore
//! use nasa::log::{LogContext, LogManager};
//! let mut mgr = LogManager::bootstrap(boot.log.as_ref()); // 早期:只控制台
//! let ctx = LogContext::with_app_name(&cfg.server.name);    // 缺 path = 只控制台(Rust 现状)
//! mgr.apply(&cfg.log, &ctx)?;                               // 最终:set_level + 接文件
//! // 需要历史默认目录时使用 LogContext::legacy_default(&cfg.server.name)
//! ```

use crate::{
    disable_file_logging, emit_file_logging_enabled, init_with_default, set_level, set_log_pattern,
    try_enable_file_logging_with, CompiledLogPattern, FileLogConfig, LogGuard, LogOpenError,
    LogPatternError, DEFAULT_LOG_PATTERN, DEFAULT_MAX_FILE_SIZE, DEFAULT_MAX_HISTORY_DAYS,
    DEFAULT_TOTAL_SIZE_CAP,
};
pub use nabase::{ByteSize, ByteSizeError};
use serde::Deserialize;
use std::fmt;
use std::sync::Arc;

// ────────────────────────────────────────────────────────────────────────────
// LogConfigError
// ────────────────────────────────────────────────────────────────────────────

/// 日志配置解析/转换错误(不引入 anyhow 到共享库公共 API)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogConfigError {
    /// 非法 size 字符串(空、含小数、未知单位等)。
    InvalidSize(String),
    /// size 单位换算溢出 `u64`。
    SizeOverflow(String),
    /// `max_file_size == 0`(会导致几乎每条日志都滚动)。
    ZeroMaxFileSize,
    /// `total_size_cap == 0` 不构成有效归档容量。
    ZeroTotalSizeCap,
    /// `max_history_days < 0` 不构成有效保留天数。
    NegativeMaxHistoryDays(i64),
    /// `MissingPathPolicy::LegacyDefault` 路径策略但未提供 `app_name`。
    MissingAppNameForLegacyDefaultPath,
    /// `pattern` 解析失败，不静默退回默认格式。
    InvalidPattern(LogPatternError),
    /// 日志过滤器不符合语法或当前 subscriber 不允许安装。
    InvalidLevel(String),
}

impl fmt::Display for LogConfigError {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidSize(s) => write!(f, "invalid size: {s:?}"),
            Self::SizeOverflow(s) => write!(f, "size overflow: {s:?}"),
            Self::ZeroMaxFileSize => write!(f, "max_file_size must be > 0"),
            Self::ZeroTotalSizeCap => write!(f, "total_size_cap must be > 0"),
            Self::NegativeMaxHistoryDays(d) => write!(f, "max_history_days must be >= 0, got {d}"),
            Self::MissingAppNameForLegacyDefaultPath => {
                write!(f, "LegacyDefault path policy requires app_name")
            }
            Self::InvalidPattern(e) => write!(f, "invalid log pattern: {e}"),
            Self::InvalidLevel(reason) => write!(f, "invalid log filter: {reason}"),
        }
    }
}

impl std::error::Error for LogConfigError {
    /// 业务作用：返回底层错误来源；用于错误链追踪。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidPattern(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ByteSizeError> for LogConfigError {
    /// 业务作用：把公共容量错误映射到日志配置错误。
    ///
    /// # 参数
    /// - `e`: 错误对象或外部错误值。
    fn from(e: ByteSizeError) -> Self {
        match e {
            ByteSizeError::Invalid(s) => Self::InvalidSize(s),
            ByteSizeError::Overflow(s) => Self::SizeOverflow(s),
        }
    }
}

/// `LogManager::apply` / `apply_config` 的错误:**配置解析错误**与**运行期文件打开错误**分开,
/// 目录创建失败、文件被占用或权限不足均返回文件错误，不作为成功应用处理。
#[derive(Debug)]
pub enum LogApplyError {
    /// 配置解析/转换错误。
    Config(LogConfigError),
    /// 接入文件日志时打开失败(此时**未改变**已生效的旧文件日志)。
    Io(LogOpenError),
}

impl fmt::Display for LogApplyError {
    /// 业务作用：实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Config(e) => write!(f, "log config error: {e}"),
            Self::Io(e) => write!(f, "log apply io error: {e}"),
        }
    }
}

impl std::error::Error for LogApplyError {
    /// 业务作用：返回底层错误来源；用于错误链追踪。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

impl From<LogConfigError> for LogApplyError {
    /// 业务作用：执行类型转换；用于把外部值统一转成本模块类型。
    ///
    /// # 参数
    /// - `e`: 错误对象或外部错误值。
    fn from(e: LogConfigError) -> Self {
        Self::Config(e)
    }
}
impl From<LogOpenError> for LogApplyError {
    /// 业务作用：执行类型转换；用于把外部值统一转成本模块类型。
    ///
    /// # 参数
    /// - `e`: 错误对象或外部错误值。
    fn from(e: LogOpenError) -> Self {
        Self::Io(e)
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 应用侧日志配置声明。
// ────────────────────────────────────────────────────────────────────────────

/// 应用侧日志配置。直接把 YAML/Nacos 的 `log:` 段反序列化进来;`None` 字段在 [`resolve`](LogConfig::resolve)
/// 时取组件默认值。`Option` 区分“省略并使用默认值”与“明确配值”。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// EnvFilter 表达式(根级别与各模块覆盖)。默认 `"info"`,例 `"info,my_app=debug"`。
    pub level: String,
    /// 日志目录。`Some`/非空 → 接 `info.log`/`error.log`;`None`/空 → 按 [`MissingPathPolicy`] 处理。
    pub path: Option<String>,
    /// 兼容旧 YAML:单文件上限,单位 MB。
    pub max_file_size_mb: Option<u64>,
    /// 单文件上限支持 `"500MB"`、`"1GB"` 或字节整数；与 `max_file_size_mb` 并存时本字段优先。
    #[serde(alias = "maxFileSize")]
    pub max_file_size: Option<ByteSize>,
    /// 兼容旧 YAML:归档总量上限,单位 MB。
    pub total_size_cap_mb: Option<u64>,
    /// 归档总量上限支持 `"30GB"` 等容量值；与 `total_size_cap_mb` 并存时本字段优先。
    #[serde(alias = "totalSizeCap")]
    pub total_size_cap: Option<ByteSize>,
    /// 归档保留天数。`None` → 30。
    #[serde(alias = "maxHistory")]
    pub max_history_days: Option<i64>,
    /// 启动时清理历史。`None` → true。
    #[serde(alias = "cleanHistoryOnStart")]
    pub clean_history_on_start: Option<bool>,
    /// 是否写独立 `error.log`。`None` → true。
    pub split_error_file: Option<bool>,
    /// 文件是否带 ANSI 颜色。`None` → false。
    pub color: Option<bool>,
    /// 日志输出格式；`None` 使用 [`DEFAULT_LOG_PATTERN`]。
    /// 非法 pattern 在 `resolve` 时返 `Err(InvalidPattern)`,**不静默退回默认**。
    #[serde(alias = "log_pattern", alias = "logPattern", alias = "LOG_PATTERN")]
    pub pattern: Option<String>,
}

impl Default for LogConfig {
    /// 业务作用：返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            path: None,
            max_file_size_mb: None,
            max_file_size: None,
            total_size_cap_mb: None,
            total_size_cap: None,
            max_history_days: None,
            clean_history_on_start: None,
            split_error_file: None,
            color: None,
            pattern: None,
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 路径策略 / 解析结果
// ────────────────────────────────────────────────────────────────────────────

/// 缺失 `path` 时的策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingPathPolicy {
    /// 缺 path = 不接文件,只控制台(兼容 rust-simple-mvc 现状)。
    ConsoleOnly,
    /// legacy default = 沿用历史日志目录规则;只影响缺省 path 的补全,不代表整套运行协议。
    /// 缺 path 且有 `app_name` = `{default_log_root}/{app_name}`。
    LegacyDefault,
}

/// 解析路径所需的运行上下文(应用名 / 默认根 / 缺失策略)。
#[derive(Debug, Clone)]
pub struct LogContext {
    /// 应用名；`LegacyDefault` 缺少 path 时用于拼接默认目录。
    pub app_name: Option<String>,
    /// 默认日志根为 `/usr/local/logs`。
    pub default_log_root: String,
    /// 缺失 path 的处理策略。
    pub missing_path_policy: MissingPathPolicy,
}

impl Default for LogContext {
    /// 业务作用：返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            app_name: None,
            default_log_root: "/usr/local/logs".to_string(),
            missing_path_policy: MissingPathPolicy::ConsoleOnly,
        }
    }
}

impl LogContext {
    /// 业务作用：缺 path = 只控制台(无 app_name)。
    pub fn console_only() -> Self {
        Self::default()
    }

    /// 业务作用：显式选择缺省文件日志目录；缺 path 时使用 `{default_log_root}/{app_name}`。
    ///
    /// # 参数
    /// - `app_name`: 应用名,用于在未显式配置日志目录时拼出默认文件日志目录。
    pub fn legacy_default(app_name: impl Into<String>) -> Self {
        Self {
            app_name: Some(app_name.into()),
            missing_path_policy: MissingPathPolicy::LegacyDefault,
            ..Self::default()
        }
    }

    /// 业务作用：记录 app_name 并保持控制台输出(缺 path = 只控制台)。
    ///
    /// # 参数
    /// - `app_name`: 应用名,仅记录到上下文中,不会自动启用文件日志目录。
    pub fn with_app_name(app_name: impl Into<String>) -> Self {
        Self {
            app_name: Some(app_name.into()),
            ..Self::default()
        }
    }

    /// 业务作用：覆盖默认日志根(本地开发常用临时目录,避免写 `/usr/local/logs`)。
    ///
    /// # 参数
    /// - `root`: 缺失 path 且使用 legacy 策略时的日志根目录。
    pub fn with_default_log_root(mut self, root: impl Into<String>) -> Self {
        self.default_log_root = root.into();
        self
    }

    /// 业务作用：覆盖缺失 path 策略。
    ///
    /// # 参数
    /// - `p`: 未配置日志 path 时的处理策略,决定只打控制台还是拼 legacy 默认目录。
    pub fn with_missing_path_policy(mut self, p: MissingPathPolicy) -> Self {
        self.missing_path_policy = p;
        self
    }
}

/// 解析得到的日志目录来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPathSource {
    /// 来自 `cfg.path`。
    Explicit,
    /// 来自 `MissingPathPolicy::LegacyDefault` 拼接的默认目录。
    LegacyDefault,
    /// 无文件日志(只控制台)。
    Disabled,
}

/// `LogConfig` 解析后的只读结果(便于打印/诊断)。
#[derive(Debug, Clone)]
pub struct ResolvedLogConfig {
    /// 最终 EnvFilter 级别表达式。
    pub level: String,
    /// 文件配置;`None` = 只控制台。
    pub file: Option<FileLogConfig>,
    /// 目录来源。
    pub path_source: LogPathSource,
    /// 编译后的输出 pattern(`None` 时为 [`DEFAULT_LOG_PATTERN`] 编译结果),由 `apply`/`apply_config` 提交到全局槽。
    pub pattern: Arc<CompiledLogPattern>,
}

impl LogConfig {
    /// 业务作用：启动期级别(`level` 空 → `"info"`;**返回 trim 后的值**,避免 `" info "` 传给 `EnvFilter` 失败)。
    pub fn bootstrap_level(&self) -> &str {
        let l = self.level.trim();
        if l.is_empty() {
            "info"
        } else {
            l
        }
    }

    /// 业务作用：解析路径:`(目录, 来源)`;`Disabled` 时目录为 `None`。
    ///
    /// # 参数
    /// - `ctx`: 本次格式化、日志或运行阶段的上下文。
    fn resolve_path(
        &self,
        ctx: &LogContext,
    ) -> Result<(Option<String>, LogPathSource), LogConfigError> {
        let explicit = self
            .path
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(p) = explicit {
            return Ok((Some(p.to_string()), LogPathSource::Explicit));
        }
        match ctx.missing_path_policy {
            MissingPathPolicy::ConsoleOnly => Ok((None, LogPathSource::Disabled)),
            // MissingPathPolicy::LegacyDefault 缺 path 时拼 {root}/{app};app_name 缺失**或空白**都视为缺失(空服务名不应悄悄落到根目录)。
            MissingPathPolicy::LegacyDefault => {
                match ctx
                    .app_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    Some(name) => {
                        let dir = format!("{}/{name}", ctx.default_log_root.trim_end_matches('/'));
                        Ok((Some(dir), LogPathSource::LegacyDefault))
                    }
                    None => Err(LogConfigError::MissingAppNameForLegacyDefaultPath),
                }
            }
        }
    }

    /// 业务作用：解析 resolved max file size 结果；用于确定最终运行参数。
    fn resolved_max_file_size(&self) -> Result<u64, LogConfigError> {
        let bytes = match (self.max_file_size, self.max_file_size_mb) {
            (Some(bs), _) => bs.0,
            (None, Some(mb)) => mb
                .checked_mul(1024 * 1024)
                .ok_or_else(|| LogConfigError::SizeOverflow(format!("{mb}MB")))?,
            (None, None) => DEFAULT_MAX_FILE_SIZE,
        };
        if bytes == 0 {
            return Err(LogConfigError::ZeroMaxFileSize);
        }
        Ok(bytes)
    }

    /// 业务作用：解析 resolved total size cap 结果；用于确定最终运行参数。
    fn resolved_total_size_cap(&self) -> Result<u64, LogConfigError> {
        let bytes = match (self.total_size_cap, self.total_size_cap_mb) {
            (Some(bs), _) => bs.0,
            (None, Some(mb)) => mb
                .checked_mul(1024 * 1024)
                .ok_or_else(|| LogConfigError::SizeOverflow(format!("{mb}MB")))?,
            (None, None) => DEFAULT_TOTAL_SIZE_CAP,
        };
        if bytes == 0 {
            return Err(LogConfigError::ZeroTotalSizeCap);
        }
        Ok(bytes)
    }

    /// 业务作用：构建 build file cfg 结果；用于把配置和上下文组装成可执行对象。
    ///
    /// # 参数
    /// - `dir`: 日志、存储或配置文件所在目录。
    fn build_file_cfg(&self, dir: String) -> Result<FileLogConfig, LogConfigError> {
        let max_history_days = self.max_history_days.unwrap_or(DEFAULT_MAX_HISTORY_DAYS);
        if max_history_days < 0 {
            return Err(LogConfigError::NegativeMaxHistoryDays(max_history_days));
        }
        Ok(FileLogConfig {
            dir,
            max_file_size: self.resolved_max_file_size()?,
            max_history_days,
            total_size_cap: self.resolved_total_size_cap()?,
            clean_history_on_start: self.clean_history_on_start.unwrap_or(true),
            split_error_file: self.split_error_file.unwrap_or(true),
            color: self.color.unwrap_or(false),
        })
    }

    /// 业务作用：解析为 `Option<FileLogConfig>`(`None` = 只控制台)。
    ///
    /// # 参数
    /// - `ctx`: 运行期日志上下文,提供应用名、默认日志根和缺失 path 策略。
    pub fn resolve_file(&self, ctx: &LogContext) -> Result<Option<FileLogConfig>, LogConfigError> {
        match self.resolve_path(ctx)? {
            (Some(dir), _) => Ok(Some(self.build_file_cfg(dir)?)),
            (None, _) => Ok(None),
        }
    }

    /// 业务作用：编译输出 pattern(`None` → [`DEFAULT_LOG_PATTERN`]);非法 → `Err(InvalidPattern)`,不静默退回默认。
    fn resolve_pattern(&self) -> Result<Arc<CompiledLogPattern>, LogConfigError> {
        let s = self.pattern.as_deref().unwrap_or(DEFAULT_LOG_PATTERN);
        CompiledLogPattern::parse(s)
            .map(Arc::new)
            .map_err(LogConfigError::InvalidPattern)
    }

    /// 业务作用：解析为完整 [`ResolvedLogConfig`](级别 + 文件配置 + 路径来源 + 编译 pattern)。
    ///
    /// # 参数
    /// - `ctx`: 运行期日志上下文,用于解析缺失 path、legacy 默认目录和应用名回退。
    pub fn resolve(&self, ctx: &LogContext) -> Result<ResolvedLogConfig, LogConfigError> {
        let (dir, source) = self.resolve_path(ctx)?;
        let file = match dir {
            Some(dir) => Some(self.build_file_cfg(dir)?),
            None => None,
        };
        let pattern = self.resolve_pattern()?;
        Ok(ResolvedLogConfig {
            level: self.bootstrap_level().to_string(),
            file,
            path_source: source,
            pattern,
        })
    }
}

// ────────────────────────────────────────────────────────────────────────────
// 运行期管理:LogManager(推荐)+ apply_config(轻量)
// ────────────────────────────────────────────────────────────────────────────

/// 推荐的日志生命周期管理器:持有 `LogGuard`,封装 bootstrap / apply / disable(尤其配合 Nacos 热更新)。
///
/// **必须持有到进程结束**,否则文件日志后台刷盘线程停止、缓冲日志丢失。
#[must_use = "LogManager 必须持有到进程结束,否则文件日志后台刷盘线程会停止"]
pub struct LogManager {
    guard: Option<LogGuard>,
    reclaim: LogReclaimer,
}

/// 锁外完成 I/O 和过滤器解析的日志候选；旧 guard 在安装后保留到候选被锁外释放。
pub struct PreparedLogConfig {
    resolved: ResolvedLogConfig,
    filter: Option<tracing_subscriber::EnvFilter>,
    file: Option<crate::PreparedFileLogging>,
    retired: Option<LogGuard>,
    reclaim: Option<ReclaimPermit>,
}

/// 每个日志 owner 只保留固定数量的候选和回收工作，避免坏目录或慢刷盘累积后台线程。
struct LogReclaimer {
    sender: Option<std::sync::mpsc::Sender<ReclaimWork>>,
    worker: Option<std::thread::JoinHandle<()>>,
    pending: Arc<std::sync::atomic::AtomicUsize>,
}

enum ReclaimWork {
    Guards(Vec<LogGuard>, Arc<std::sync::atomic::AtomicUsize>),
    Barrier(std::sync::mpsc::SyncSender<()>),
}

struct ReclaimPermit {
    sender: std::sync::mpsc::Sender<ReclaimWork>,
    pending: Arc<std::sync::atomic::AtomicUsize>,
}

impl LogReclaimer {
    /// 业务作用：建立一个有界日志资源回收 owner，刷盘等待不占配置发布线程。
    /// 参数说明：无。
    /// 返回：拥有独立回收线程的队列；线程无法建立时不允许启动日志 owner。
    fn new() -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("log-reclaim".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    match work {
                        ReclaimWork::Guards(guards, pending) => {
                            drop(guards);
                            pending.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                        }
                        ReclaimWork::Barrier(done) => {
                            let _ = done.send(());
                        }
                    }
                }
            })
            .expect("log resource reclaimer thread is required");
        Self {
            sender: Some(sender),
            worker: Some(worker),
            pending: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// 业务作用：在打开候选文件前预留回收容量，慢回收时拒绝继续积累候选。
    /// 参数说明：无。
    /// 返回：最多两个在途候选/回收项中的一个许可；容量不足时返回配置应用错误。
    fn reserve(&self) -> Result<ReclaimPermit, LogApplyError> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| LogConfigError::InvalidLevel("log owner is closed".into()))?;
        self.pending
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |pending| (pending < 2).then_some(pending + 1),
            )
            .map_err(|_| {
                LogConfigError::InvalidLevel("log resource retirement capacity exhausted".into())
            })?;
        Ok(ReclaimPermit {
            sender: sender.clone(),
            pending: self.pending.clone(),
        })
    }

    /// 业务作用：在停机的阻塞清理线程中确认已提交回收的旧 writer 全部完成。
    /// 参数说明：无。
    /// 返回：此前已交给回收 owner 的资源完成刷盘后返回。
    fn drain(&self) {
        let (done, wait) = std::sync::mpsc::sync_channel(0);
        if self
            .sender
            .as_ref()
            .is_some_and(|sender| sender.send(ReclaimWork::Barrier(done)).is_ok())
        {
            let _ = wait.recv();
        }
    }

    /// 业务作用：关闭候选准入并等待有界回收线程退出。
    /// 参数说明：无。
    /// 返回：所有持有候选归还后完成，调用方须在受停机预算监督的阻塞线程中执行。
    fn shutdown(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for ReclaimPermit {
    /// 业务作用：准备失败而尚未形成候选时归还容量。
    /// 参数说明：无。
    /// 返回：减少在途准备计数，不等待日志资源。
    fn drop(&mut self) {
        self.pending
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl Drop for PreparedLogConfig {
    /// 业务作用：将未安装或已退役的 writer 移交给有界回收 owner。
    /// 参数说明：无。
    /// 返回：本方法不等待刷盘；回收完成后容量归还。
    fn drop(&mut self) {
        let Some(permit) = self.reclaim.take() else {
            return;
        };
        let mut guards = Vec::with_capacity(2);
        if let Some(file) = self.file.take() {
            guards.push(file.guards);
        }
        if let Some(retired) = self.retired.take() {
            guards.push(retired);
        }
        if guards.is_empty() {
            return;
        }
        // 候选在 I/O 之前已取得许可，队列长度由许可总量约束；不在发布临界区执行 flush/join。
        let pending = permit.pending.clone();
        pending.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Err(error) = permit
            .sender
            .send(ReclaimWork::Guards(guards, pending.clone()))
        {
            drop(error);
            pending.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

impl LogManager {
    /// 业务作用：早期启动(严格版):**先提交 boot 配置的 pattern**
    /// 再仅初始化控制台(文件 writer 槽仍空)。`init_with_default` 全局只能调一次。pattern 非法 → `Err`,不静默退默认。
    ///
    /// # 参数
    /// - `cfg`: 启动期可选日志配置,用于决定初始级别和控制台输出 pattern。
    pub fn try_bootstrap(cfg: Option<&LogConfig>) -> Result<Self, LogConfigError> {
        if let Some(c) = cfg {
            set_log_pattern(c.resolve_pattern()?); // 早期控制台即用配置 pattern
        }
        init_with_default(cfg.map(LogConfig::bootstrap_level).unwrap_or("info"));
        Ok(Self {
            guard: None,
            reclaim: LogReclaimer::new(),
        })
    }

    /// 业务作用：早期启动:仅初始化控制台(文件 writer 槽仍空)。便捷 fail-fast 版,boot pattern 非法直接 panic。
    /// 需可恢复请用 [`LogManager::try_bootstrap`]。
    ///
    /// # 参数
    /// - `cfg`: 启动期可选日志配置,用于决定初始级别和控制台输出 pattern。
    pub fn bootstrap(cfg: Option<&LogConfig>) -> Self {
        Self::try_bootstrap(cfg).expect("invalid bootstrap log config (pattern)")
    }

    /// 业务作用：在发布前准备日志文件、writer 和过滤器，当前输出保持可用。
    /// 参数说明：`cfg` 为完整日志配置；`ctx` 为稳定的应用日志上下文。
    /// 返回：尚未安装的候选；任何解析或文件准备失败都不修改日志运行态。
    pub fn prepare(
        &self,
        cfg: &LogConfig,
        ctx: &LogContext,
    ) -> Result<PreparedLogConfig, LogApplyError> {
        let reclaim = self.reclaim.reserve()?;
        let resolved = cfg.resolve(ctx)?;
        let filter = tracing_subscriber::EnvFilter::try_new(&resolved.level)
            .map_err(|error| LogConfigError::InvalidLevel(error.to_string()))?;
        let file = resolved
            .file
            .as_ref()
            .map(crate::prepare_file_logging)
            .transpose()?;
        Ok(PreparedLogConfig {
            resolved,
            filter: Some(filter),
            file,
            retired: None,
            reclaim: Some(reclaim),
        })
    }

    /// 业务作用：在宿主发布临界区安装已经准备的候选，不执行文件 I/O 或旧 writer 等待。
    /// 参数说明：`prepared` 为尚未安装的候选，调用方必须在发布锁外释放它。
    /// 返回：过滤器安装成功后切换输出；失败保留旧输出，候选仍负责未安装资源的清理。
    pub fn install(&mut self, prepared: &mut PreparedLogConfig) -> Result<(), LogApplyError> {
        let filter = prepared.filter.take().ok_or_else(|| {
            LogConfigError::InvalidLevel("candidate was already installed".into())
        })?;
        // 先完成唯一可拒绝的过滤器安装，再无失败地移交文件输出与 guard。
        crate::install_level(filter).map_err(LogConfigError::InvalidLevel)?;
        set_log_pattern(prepared.resolved.pattern.clone());
        let next_guard = match prepared.file.take() {
            Some(file) => Some(file.install()),
            None => {
                disable_file_logging();
                None
            }
        };
        prepared.retired = std::mem::replace(&mut self.guard, next_guard);
        Ok(())
    }

    /// 业务作用：独立使用时完成准备、安装和旧输出回收。
    /// 参数说明：`cfg` 为完整配置；`ctx` 为应用日志上下文。
    /// 返回：成功返回生效配置；失败保留原输出；此同步入口可能等待文件 I/O，不能在宿主发布锁内调用。
    pub fn apply(
        &mut self,
        cfg: &LogConfig,
        ctx: &LogContext,
    ) -> Result<ResolvedLogConfig, LogApplyError> {
        let mut prepared = self.prepare(cfg, ctx)?;
        self.install(&mut prepared)?;
        if let Some(file) = &prepared.resolved.file {
            emit_file_logging_enabled(file);
        }
        Ok(prepared.resolved.clone())
    }

    /// 业务作用：关闭文件日志并终止候选资源回收 owner，供宿主最终停机使用。
    /// 参数说明：无。
    /// 返回：当前及退役 writer 刷盘、回收线程退出后返回；关闭后不再接受新候选。
    pub fn shutdown(&mut self) {
        self.disable_file();
        self.reclaim.shutdown();
    }

    /// 业务作用：手动关闭文件日志,回到只控制台。
    pub fn disable_file(&mut self) {
        disable_file_logging();
        self.guard = None;
        self.reclaim.drain();
    }
}

/// 业务作用：轻量入口(不强制用 `LogManager`):接入/关闭文件日志 + `set_level`,返回新 `LogGuard`(调用方须持有)。
/// 失败语义同 [`LogManager::apply`]:文件打开失败返 `Err(LogApplyError::Io)`,不静默吞。有 Nacos 热更新时建议用
/// [`LogManager`] 管理 guard 生命周期。
///
/// # 参数
/// - `cfg`: 最终日志配置,包含级别、文件滚动参数和输出 pattern。
/// - `ctx`: 运行期日志上下文,用于解析缺失 path 和 legacy 默认目录。
pub fn apply_config(cfg: &LogConfig, ctx: &LogContext) -> Result<Option<LogGuard>, LogApplyError> {
    let resolved = cfg.resolve(ctx)?;
    match resolved.file {
        Some(file_cfg) => {
            let guard = try_enable_file_logging_with(&file_cfg)?;
            set_log_pattern(resolved.pattern.clone());
            set_level(&resolved.level);
            emit_file_logging_enabled(&file_cfg); // 同 LogManager::apply:pattern 提交后写状态行
            Ok(Some(guard))
        }
        None => {
            set_log_pattern(resolved.pattern.clone());
            set_level(&resolved.level);
            disable_file_logging();
            Ok(None)
        }
    }
}
