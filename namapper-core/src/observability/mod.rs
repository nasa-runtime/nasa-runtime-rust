//! 静态方法身份与原子观测。完成路径不分配标签、不写 MetricHub、不调用用户 observer。

pub mod config;
pub mod console;

use config::{EffectivePolicy, LogLevel};
use nametrics_core::atomic::{add, AtomicHistogram, LATENCY_SECONDS, STREAM_LIFETIME_SECONDS};
use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};
use nanotify_core::{
    AlertRoute, EventKind, Notification, NotificationFields, NotificationIdentity,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};

macro_rules! domain {
    ($name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[repr(usize)]
        pub enum $name { $($variant),+ }
        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            /// 业务作用：返回固定词表中的公开标签，不从错误文本或参数派生。
            /// 参数说明：无。
            /// 返回：编译期静态标签。
            pub const fn label(self) -> &'static str { match self { $(Self::$variant => $label),+ } }
        }
    };
}
domain!(CallPath { Cache => "cache", Database => "database", PreExecution => "pre_execution" });
domain!(CallOutcome { Ok => "ok", NotFound => "not_found", Error => "error", Cancelled => "cancelled", Panic => "panic" });
domain!(DbOutcome {
    Ok => "ok", NotFound => "not_found", Constraint => "constraint", Database => "database",
    Configuration => "configuration", InvalidArgument => "invalid_argument", Io => "io", Tls => "tls",
    Protocol => "protocol", Encode => "encode", Decode => "decode", Schema => "schema", Driver => "driver",
    Cancelled => "cancelled", Panic => "panic", Other => "other"
});
domain!(StreamOutcome { Completed => "completed", Error => "error", Cancelled => "cancelled", CancelledBeforePoll => "cancelled_before_poll", Panic => "panic" });
domain!(Status { Success => "success", Failure => "failure", Cancelled => "cancelled" });

impl DbOutcome {
    /// 业务作用：将详细数据库结果映射为低基数延迟分布状态。
    /// 参数说明：无。
    /// 返回：未找到属于成功，取消单列，其它失败不能稀释成功分位数。
    pub const fn status(self) -> Status {
        match self {
            Self::Ok | Self::NotFound => Status::Success,
            Self::Cancelled => Status::Cancelled,
            _ => Status::Failure,
        }
    }
    /// 业务作用：区分执行失败与未找到、取消、展开等生命周期结果。
    /// 参数说明：无。
    /// 返回：显式数据库失败时允许输出 execution_error 事件。
    pub const fn is_execution_error(self) -> bool {
        !matches!(
            self,
            Self::Ok | Self::NotFound | Self::Cancelled | Self::Panic
        )
    }
}

/// 宏生成的静态方法身份与进程生命周期单元；不会在旧 future 或 stream 存活期间释放。
pub struct MapperMethodMeta {
    pub method: &'static str,
    pub mapper: &'static str,
    pub driver: &'static str,
    pub datasource: &'static str,
    pub operation: &'static str,
    pub tx_mode: &'static str,
    pub sql_template: &'static str,
    cells: MethodCells,
    policy: OnceLock<EffectivePolicy>,
    record_rows: AtomicBool,
    alerts: OnceLock<MethodAlerts>,
}

struct MethodAlerts {
    identity: Arc<NotificationIdentity>,
    slow_route: Option<AlertRoute>,
    error_route: Option<AlertRoute>,
    slow_next: AtomicU64,
    error_next: [AtomicU64; 16],
}

#[linkme::distributed_slice]
pub static MAPPER_METHOD_META: [&'static MapperMethodMeta];

static DEFAULT_POLICY: LazyLock<EffectivePolicy> = LazyLock::new(EffectivePolicy::default);
static CLOCK: LazyLock<Instant> = LazyLock::new(Instant::now);
static NOTIFICATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct MethodCells {
    calls: [AtomicU64; 15],
    call_duration: [AtomicHistogram; 3],
    calls_open: AtomicU64,
    operations: [AtomicU64; 16],
    db_duration: [AtomicHistogram; 3],
    db_open: AtomicU64,
    rows: AtomicU64,
    slow: AtomicU64,
    streams: [AtomicU64; 5],
    ttfr: AtomicHistogram,
    lifetime: [AtomicHistogram; 3],
    streams_open: AtomicU64,
    slow_log_next: AtomicU64,
    error_log_next: [AtomicU64; 16],
}
impl MethodCells {
    /// 业务作用：为编译期方法预留全部封闭结果与固定桶，避免完成路径懒建状态。
    /// 参数说明：无。
    /// 返回：零值原子单元。
    const fn new() -> Self {
        Self {
            calls: [const { AtomicU64::new(0) }; 15],
            call_duration: [const { AtomicHistogram::new(LATENCY_SECONDS) }; 3],
            calls_open: AtomicU64::new(0),
            operations: [const { AtomicU64::new(0) }; 16],
            db_duration: [const { AtomicHistogram::new(LATENCY_SECONDS) }; 3],
            db_open: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            slow: AtomicU64::new(0),
            streams: [const { AtomicU64::new(0) }; 5],
            ttfr: AtomicHistogram::new(LATENCY_SECONDS),
            lifetime: [const { AtomicHistogram::new(STREAM_LIFETIME_SECONDS) }; 3],
            streams_open: AtomicU64::new(0),
            slow_log_next: AtomicU64::new(0),
            error_log_next: [const { AtomicU64::new(0) }; 16],
        }
    }
}

impl MapperMethodMeta {
    /// 业务作用：把完整方法身份、事务边界与 SQL 模板绑定到静态观测单元。
    /// 参数说明：参数均为宏确定的静态合同；sql_template 只供受控诊断，不作指标标签。
    /// 返回：尚未安装应用策略、但已可安全采集基础指标的静态元数据。
    pub const fn new(
        method: &'static str,
        mapper: &'static str,
        driver: &'static str,
        datasource: &'static str,
        operation: &'static str,
        tx_mode: &'static str,
        sql_template: &'static str,
    ) -> Self {
        Self {
            method,
            mapper,
            driver,
            datasource,
            operation,
            tx_mode,
            sql_template,
            cells: MethodCells::new(),
            policy: OnceLock::new(),
            record_rows: AtomicBool::new(true),
            alerts: OnceLock::new(),
        }
    }

    /// 业务作用：在业务 I/O 前一次冻结有效策略，禁止旧 Stream 与新配置交叉解释。
    /// 参数说明：`policy` 为逐叶合并结果，`record_rows` 决定行数附加指标。
    /// 返回：首次安装成功；重复安装相同策略幂等，不同策略要求重启。
    pub fn configure(
        &'static self,
        policy: EffectivePolicy,
        record_rows: bool,
    ) -> Result<(), &'static str> {
        policy.validate()?;
        if let Some(existing) = self.policy.get() {
            return if existing == &policy && self.record_rows.load(Ordering::Relaxed) == record_rows
            {
                Ok(())
            } else {
                Err("SQL observation policy is frozen; restart required")
            };
        }
        self.record_rows.store(record_rows, Ordering::Relaxed);
        self.policy
            .set(policy)
            .map_err(|_| "SQL observation policy installation raced")
    }

    /// 业务作用：读取不含敏感数据的冻结策略，独立使用时采用产品默认。
    /// 参数说明：无。
    /// 返回：与方法单元同生命周期的策略引用。
    pub fn policy(&self) -> &EffectivePolicy {
        self.policy.get().unwrap_or_else(|| &DEFAULT_POLICY)
    }

    /// 业务作用：在访问可能分配字符串的 driver 错误码之前确认错误日志确实需要该字段。
    /// 参数说明：无。
    /// 返回：错误日志开启、允许数据库码且日志过滤器可见时为真。
    pub fn database_code_enabled(&self) -> bool {
        let policy = &self.policy().execution_error;
        policy.log_enabled
            && policy.include_database_code
            && diagnostic(|| match policy.log_level {
                LogLevel::Warn => tracing::enabled!(tracing::Level::WARN),
                _ => tracing::enabled!(tracing::Level::ERROR),
            })
            .unwrap_or(false)
    }

    /// 业务作用：在启动期把冻结规则绑定到框架队列句柄，执行路径不调用 provider。
    /// 参数说明：`identity` 为进程身份，`slow_route` 与 `error_route` 为已验证的有界投递路由。
    /// 返回：首次绑定成功；重复绑定拒绝以免运行中的调用切换渠道。
    pub fn configure_alerts(
        &'static self,
        identity: Arc<NotificationIdentity>,
        slow_route: Option<AlertRoute>,
        error_route: Option<AlertRoute>,
    ) -> Result<(), &'static str> {
        if slow_route.is_none() && error_route.is_none() {
            return Ok(());
        }
        self.alerts
            .set(MethodAlerts {
                identity,
                slow_route,
                error_route,
                slow_next: AtomicU64::new(0),
                error_next: [const { AtomicU64::new(0) }; 16],
            })
            .map_err(|_| "SQL notification routes are frozen; restart required")
    }

    /// 业务作用：仅在规则命中且取得冷却许可后构造拥有型通知，拥塞不影响 SQL 返回。
    /// 参数说明：`outcome`、`duration` 为执行事实，`sql` 为可选 prepared 模板。
    /// 返回：无；只执行具体队列 try_send，禁用时不分配文本。
    fn notify(&self, outcome: DbOutcome, duration: Duration, sql: &str) {
        let Some(routes) = self.alerts.get() else {
            return;
        };
        let policy = self.policy();
        let slow = policy.slow_sql.is_slow(duration);
        let (route, rule, next, event) = if outcome.is_execution_error()
            && policy.alerts.execution_error.enabled
            && routes.error_route.is_some()
        {
            (
                &routes.error_route,
                &policy.alerts.execution_error,
                &routes.error_next[outcome as usize],
                EventKind::ExecutionError,
            )
        } else if slow {
            (
                &routes.slow_route,
                &policy.alerts.slow_sql,
                &routes.slow_next,
                EventKind::SlowSql,
            )
        } else {
            return;
        };
        let Some(route) = route else {
            return;
        };
        if !rule.enabled || !cooldown(next, rule.cooldown_ms) {
            return;
        }
        let sequence = NOTIFICATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let occurred_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let identity = &routes.identity;
        use std::hash::{Hash, Hasher};
        let mut instance_hash = std::collections::hash_map::DefaultHasher::new();
        identity.instance.hash(&mut instance_hash);
        let notification = Notification::new(NotificationFields {
            id: format!(
                "mapper-{:x}-{occurred_at_unix_ms}-{sequence}",
                instance_hash.finish()
            ),
            event,
            severity: rule.severity,
            occurred_at_unix_ms,
            service: identity.service.clone(),
            instance: identity.instance.clone(),
            environment: identity.environment.clone(),
            cluster: identity.cluster.clone(),
            driver: self.driver.to_owned(),
            datasource: self.datasource.to_owned(),
            operation: self.operation.to_owned(),
            method: Some(self.method.to_owned()),
            duration,
            outcome: outcome.label().to_owned(),
            error_kind: outcome
                .is_execution_error()
                .then(|| outcome.label().to_owned()),
            prepared_sql: rule
                .include_sql
                .then(|| clean_text(sql, rule.max_sql_chars)),
            slow,
            ..NotificationFields::default()
        });
        // 入队结果只由通知指标记录；不能把观测侧背压转化为数据库失败或事务回滚。
        let _ = route.try_send(notification);
    }

    /// 业务作用：为指标出口构建静态身份标签，业务执行路径不调用该函数。
    /// 参数说明：`operation` 决定是否附带操作维度。
    /// 返回：顺序与公开 descriptor 一致的拥有型标签。
    fn labels(&self, operation: bool) -> Vec<(&'static str, String)> {
        let mut labels = vec![
            ("method", self.method.to_owned()),
            ("driver", self.driver.to_owned()),
            ("datasource", self.datasource.to_owned()),
        ];
        if operation {
            labels.push(("operation", self.operation.to_owned()));
        }
        labels
    }

    /// 业务作用：终止一次真实数据库调用并独立记录结果、时长、行数和慢阈值。
    /// 参数说明：`outcome` 是擦除前分类，`duration` 为客户端活跃时长，`rows` 为准确行数。
    /// 返回：是否达到慢阈值；基础事实不受出口配置影响。
    fn record_db(&self, outcome: DbOutcome, duration: Duration, rows: u64) -> bool {
        add(&self.cells.operations[outcome as usize], 1);
        self.cells.db_duration[outcome.status() as usize].observe(duration);
        self.cells.db_open.fetch_sub(1, Ordering::Relaxed);
        if self.record_rows.load(Ordering::Relaxed) {
            add(&self.cells.rows, rows);
        }
        let slow = self.policy().slow_sql.is_slow(duration);
        if slow {
            add(&self.cells.slow, 1);
        }
        slow
    }

    /// 业务作用：在显式终态选择唯一慢操作或执行失败日志，原始错误和 bind 值不进入事件。
    /// 参数说明：`outcome`、`duration`、`rows` 为观测事实，`sql` 是可选诊断模板，`code` 是结构化数据库码。
    /// 返回：无；日志关闭或冷却时不构造文本，日志处理异常不改变数据库结果。
    fn emit(
        &self,
        outcome: DbOutcome,
        duration: Duration,
        rows: u64,
        sql: &str,
        code: Option<&str>,
    ) {
        self.notify(outcome, duration, sql);
        let policy = self.policy();
        let slow = policy.slow_sql.is_slow(duration);
        let selected = if outcome.is_execution_error() && policy.execution_error.log_enabled {
            let p = &policy.execution_error;
            Some((
                "execution_error",
                p.log_level,
                p.include_sql,
                p.max_sql_chars,
                p.include_database_code,
                cooldown(
                    &self.cells.error_log_next[outcome as usize],
                    p.log_cooldown_ms,
                ),
            ))
        } else if slow && policy.slow_sql.log_enabled {
            let p = &policy.slow_sql;
            Some((
                "slow_sql",
                p.log_level,
                p.include_sql,
                p.max_sql_chars,
                false,
                cooldown(&self.cells.slow_log_next, p.log_cooldown_ms),
            ))
        } else {
            None
        };
        let Some((event, level, include_sql, max_chars, include_code, true)) = selected else {
            return;
        };
        diagnostic(|| {
            // subscriber 的过滤与输出都可能执行外部代码；整个诊断边界必须晚于原子终态且不能传播展开。
            let enabled = match level {
                LogLevel::Info => tracing::enabled!(tracing::Level::INFO),
                LogLevel::Warn => tracing::enabled!(tracing::Level::WARN),
                _ => tracing::enabled!(tracing::Level::ERROR),
            };
            if !enabled {
                return;
            }
            let sql = include_sql.then(|| clean_text(sql, max_chars));
            let code = if include_code {
                code.filter(|c| {
                    c.len() <= 32 && c.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
            } else {
                None
            };
            macro_rules! event {
                ($level:expr) => {
                    tracing::event!(
                        $level,
                        component = "mapper",
                        event,
                        method = self.method,
                        driver = self.driver,
                        datasource = self.datasource,
                        operation = self.operation,
                        path = "database",
                        outcome = outcome.label(),
                        error_kind = outcome.label(),
                        duration_ms = duration.as_secs_f64() * 1000.0,
                        rows,
                        slow,
                        sql = sql.as_deref(),
                        database_code = code,
                        "数据库客户端操作"
                    )
                };
            }
            match level {
                LogLevel::Info => event!(tracing::Level::INFO),
                LogLevel::Warn => event!(tracing::Level::WARN),
                _ => event!(tracing::Level::ERROR),
            }
        });
    }
}

/// 业务作用：隔离诊断过滤、格式化和日志回调的展开，保持已取得的数据库结果。
/// 参数说明：`action` 只承载已命中的诊断工作，不得包裹数据库调用或事务裁决。
/// 返回：完成时保留诊断值；展开时丢弃本次诊断，连同异常载荷的析构也不传播到业务。
fn diagnostic<T>(action: impl FnOnce() -> T) -> Option<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
        Ok(value) => Some(value),
        Err(payload) => {
            // 自定义载荷析构也可能展开；只保留二次展开的载荷，避免递归析构再次影响业务。
            if let Err(nested) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
            {
                std::mem::forget(nested);
            }
            None
        }
    }
}

/// 业务作用：对允许输出的诊断文本按 Unicode 字符截断并替换控制字符。
/// 参数说明：`value` 为非凭据文本，`limit` 为字符上限。
/// 返回：有界拥有型文本；只在出口已命中时调用。
pub fn clean_text(value: &str, limit: usize) -> String {
    value
        .chars()
        .take(limit)
        .map(|c| {
            if c.is_control() {
                ' '
            } else {
                c
            }
        })
        .collect()
}

/// 业务作用：以单调时钟和原子比较控制单进程重复日志，竞争失败不等待。
/// 参数说明：`next` 为静态冷却单元，`millis` 为冷却间隔。
/// 返回：本次取得输出许可时为真；零间隔总是允许。
pub fn cooldown(next: &AtomicU64, millis: u64) -> bool {
    if millis == 0 {
        return true;
    }
    let now = CLOCK.elapsed().as_millis().min((u64::MAX - 1) as u128) as u64 + 1;
    let expected = next.load(Ordering::Relaxed);
    expected <= now
        && next
            .compare_exchange(
                expected,
                now.saturating_add(millis),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
}

/// 跨缓存提前返回的逻辑调用守卫，第一次 poll 才由宏创建。
pub struct MapperCallGuard {
    meta: &'static MapperMethodMeta,
    start: Instant,
    path: AtomicU8,
    done: bool,
}
impl MapperCallGuard {
    /// 业务作用：开始逻辑调用计时并占用一个在途名额。
    /// 参数说明：`meta` 为该方法的静态身份。
    /// 返回：负责显式完成、取消与 unwind 的唯一守卫。
    pub fn start(meta: &'static MapperMethodMeta) -> Self {
        add(&meta.cells.calls_open, 1);
        Self {
            meta,
            start: Instant::now(),
            path: AtomicU8::new(CallPath::PreExecution as u8),
            done: false,
        }
    }
    /// 业务作用：记录缓存、连接前或真实执行路径，不因缓存提前返回丢失调用归属。
    /// 参数说明：`path` 为封闭阶段。
    /// 返回：更新当前路径，允许 single-flight loader 共享不可变守卫引用。
    pub fn path(&self, path: CallPath) {
        self.path.store(path as u8, Ordering::Relaxed);
    }
    /// 业务作用：终止逻辑调用，数据库已成功但缓存失败仍单独记录方法失败。
    /// 参数说明：`outcome` 为方法最终结果。
    /// 返回：仅记录一次完成量与时长并归还在途名额。
    pub fn finish(&mut self, outcome: CallOutcome) {
        if self.done {
            return;
        }
        self.done = true;
        let status = match outcome {
            CallOutcome::Ok | CallOutcome::NotFound => Status::Success,
            CallOutcome::Cancelled => Status::Cancelled,
            _ => Status::Failure,
        };
        add(
            &self.meta.cells.calls
                [self.path.load(Ordering::Relaxed) as usize * 5 + outcome as usize],
            1,
        );
        self.meta.cells.call_duration[status as usize].observe(self.start.elapsed());
        self.meta.cells.calls_open.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Drop for MapperCallGuard {
    /// 业务作用：把已开始但未完成的调用记为取消或展开，避免永久遗留在途数。
    /// 参数说明：无。
    /// 返回：只更新原子事实，不输出日志或通知。
    fn drop(&mut self) {
        if !self.done {
            self.finish(if std::thread::panicking() {
                CallOutcome::Panic
            } else {
                CallOutcome::Cancelled
            });
        }
    }
}

/// 只覆盖已取得连接后的数据库客户端 Future。
pub struct MapperDbGuard {
    meta: &'static MapperMethodMeta,
    start: Instant,
    done: bool,
}
impl MapperDbGuard {
    /// 业务作用：开始数据库客户端计时，连接等待不在此区间。
    /// 参数说明：`meta` 为调用的静态方法身份。
    /// 返回：数据库调用唯一终态守卫。
    pub fn start(meta: &'static MapperMethodMeta) -> Self {
        add(&meta.cells.db_open, 1);
        Self {
            meta,
            start: Instant::now(),
            done: false,
        }
    }
    /// 业务作用：在 SQLx 结果被类型擦除前记录分类，再裁决脱敏日志。
    /// 参数说明：`outcome` 为稳定分类，`rows` 为准确行数，`sql` 为 prepared SQL，`code` 为结构化数据库码。
    /// 返回：无；不修改或替换原 SQL 结果。
    pub fn finish(&mut self, outcome: DbOutcome, rows: u64, sql: &str, code: Option<&str>) {
        if self.done {
            return;
        }
        self.done = true;
        let duration = self.start.elapsed();
        self.meta.record_db(outcome, duration, rows);
        self.meta.emit(outcome, duration, rows, sql, code);
    }
}
impl Drop for MapperDbGuard {
    /// 业务作用：取消或展开时归还数据库在途数，只记录已观察到的活跃时长。
    /// 参数说明：无。
    /// 返回：不格式化 SQL、不调用用户代码、不发送网络请求。
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.meta.record_db(
                if std::thread::panicking() {
                    DbOutcome::Panic
                } else {
                    DbOutcome::Cancelled
                },
                self.start.elapsed(),
                0,
            );
        }
    }
}

/// owned Stream 的独立状态机；消费者停顿只进入 lifetime，不进入 active fetch。
pub struct MapperStreamGuard {
    meta: &'static MapperMethodMeta,
    constructed: Instant,
    first_poll: Option<Instant>,
    active_since: Option<Instant>,
    active: Duration,
    rows: u64,
    done: bool,
    sql: Option<String>,
}
impl MapperStreamGuard {
    /// 业务作用：让流适配器在擦除 SQLx 错误前判断是否需要获取数据库码。
    /// 参数说明：无。
    /// 返回：与该流创建时冻结的方法日志策略一致的开关。
    pub fn database_code_enabled(&self) -> bool {
        self.meta.database_code_enabled()
    }

    /// 业务作用：在连接交给 Stream 时记录连接占用，包含从未 poll 的生命周期。
    /// 参数说明：`meta` 为静态身份，`sql` 只在显式允许诊断时复制。
    /// 返回：持有进程静态单元的 Stream 守卫。
    pub fn new(meta: &'static MapperMethodMeta, sql: &str) -> Self {
        add(&meta.cells.streams_open, 1);
        let p = meta.policy();
        let keep_sql = p.slow_sql.log_enabled && p.slow_sql.include_sql
            || p.execution_error.log_enabled && p.execution_error.include_sql
            || p.alerts.slow_sql.enabled && p.alerts.slow_sql.include_sql
            || p.alerts.execution_error.enabled && p.alerts.execution_error.include_sql;
        Self {
            meta,
            constructed: Instant::now(),
            first_poll: None,
            active_since: None,
            active: Duration::ZERO,
            rows: 0,
            done: false,
            sql: keep_sql.then(|| {
                clean_text(
                    sql,
                    p.slow_sql
                        .max_sql_chars
                        .max(p.execution_error.max_sql_chars)
                        .max(p.alerts.slow_sql.max_sql_chars)
                        .max(p.alerts.execution_error.max_sql_chars),
                )
            }),
        }
    }
    /// 业务作用：开始或继续一次底层取行等待；Pending 之间的等待持续计入活跃时长。
    /// 参数说明：无。
    /// 返回：首次 poll 才增加数据库在途数，重复 poll 不重复计数。
    pub fn poll_started(&mut self) {
        if self.done {
            return;
        }
        let now = Instant::now();
        if self.first_poll.is_none() {
            self.first_poll = Some(now);
            add(&self.meta.cells.db_open, 1);
        }
        if self.active_since.is_none() {
            self.active_since = Some(now);
        }
    }
    /// 业务作用：记录一次实际交给消费者的行，停止活跃计时直到下次请求下一行。
    /// 参数说明：无。
    /// 返回：首行只记录一次 TTFR；行数留在本地直到终止。
    pub fn row(&mut self) {
        if self.done {
            return;
        }
        self.end_active();
        if self.rows == 0 {
            if let Some(start) = self.first_poll {
                self.meta.cells.ttfr.observe(start.elapsed());
            }
        }
        self.rows = self.rows.saturating_add(1);
    }
    /// 业务作用：将当前底层 await 的时间并入本地累计，排除消费者处理间隔。
    /// 参数说明：无。
    /// 返回：清空当前活跃段，重复调用不重复累加。
    fn end_active(&mut self) {
        if let Some(start) = self.active_since.take() {
            self.active = self.active.saturating_add(start.elapsed());
        }
    }
    /// 业务作用：在 EOF 或 SQLx 失败时终止 Stream，方法构造结果不被回写。
    /// 参数说明：`outcome` 为数据库执行结果，`code` 为受限数据库码。
    /// 返回：记录 Stream 与数据库终态，并选择唯一日志事件。
    pub fn finish(&mut self, outcome: DbOutcome, code: Option<&str>) {
        if self.done {
            return;
        }
        self.end_active();
        self.terminate(
            if matches!(outcome, DbOutcome::Ok) {
                StreamOutcome::Completed
            } else {
                StreamOutcome::Error
            },
            outcome,
        );
        self.meta.emit(
            outcome,
            self.active,
            self.rows,
            self.sql.as_deref().unwrap_or(""),
            code,
        );
    }
    /// 业务作用：统一归还 Stream 和数据库名额，未 poll 的对象不产生数据库调用。
    /// 参数说明：`stream` 为流终态，`database` 为已开始数据库调用的终态。
    /// 返回：只记录一次完成；取消与 unwind 不执行日志格式化。
    fn terminate(&mut self, stream: StreamOutcome, database: DbOutcome) {
        if self.done {
            return;
        }
        self.done = true;
        let status = match stream {
            StreamOutcome::Completed => Status::Success,
            StreamOutcome::Error | StreamOutcome::Panic => Status::Failure,
            _ => Status::Cancelled,
        };
        add(&self.meta.cells.streams[stream as usize], 1);
        self.meta.cells.lifetime[status as usize].observe(self.constructed.elapsed());
        self.meta.cells.streams_open.fetch_sub(1, Ordering::Relaxed);
        if self.first_poll.is_some() {
            self.meta.record_db(database, self.active, self.rows);
        }
    }
}
impl Drop for MapperStreamGuard {
    /// 业务作用：区分未 poll 丢弃、进行中取消和展开，保证连接占用观测最终收口。
    /// 参数说明：无。
    /// 返回：只更新原子事实，仍存活的静态单元不会悬空。
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.end_active();
        let panic = std::thread::panicking();
        let stream = if panic {
            StreamOutcome::Panic
        } else if self.first_poll.is_none() {
            StreamOutcome::CancelledBeforePoll
        } else {
            StreamOutcome::Cancelled
        };
        self.terminate(
            stream,
            if panic {
                DbOutcome::Panic
            } else {
                DbOutcome::Cancelled
            },
        );
    }
}

macro_rules! descriptor {
    ($id:ident, $name:literal, $kind:ident, $help:literal, $labels:expr, $bounds:expr) => {
        static $id: MetricDescriptor = MetricDescriptor {
            name: $name,
            kind: MetricKind::$kind,
            help: $help,
            unit: if matches!(MetricKind::$kind, MetricKind::Histogram) {
                "seconds"
            } else {
                ""
            },
            label_names: $labels,
            histogram_bounds: $bounds,
        };
    };
}
descriptor!(
    CALLS,
    "namapper_method_calls_total",
    Counter,
    "Mapper 逻辑调用完成量。",
    &[
        "method",
        "driver",
        "datasource",
        "operation",
        "path",
        "outcome"
    ],
    &[]
);
descriptor!(
    CALL_DURATION,
    "namapper_method_duration_seconds",
    Histogram,
    "Mapper 逻辑调用客户端时长。",
    &["method", "driver", "datasource", "operation", "status"],
    LATENCY_SECONDS
);
descriptor!(
    CALL_OPEN,
    "namapper_method_in_flight",
    Gauge,
    "已经开始且尚未完成的逻辑调用。",
    &["method", "driver", "datasource", "operation"],
    &[]
);
descriptor!(
    DB_CALLS,
    "namapper_db_client_operations_total",
    Counter,
    "真实数据库客户端调用完成量。",
    &["method", "driver", "datasource", "operation", "outcome"],
    &[]
);
descriptor!(
    DB_DURATION,
    "namapper_db_client_duration_seconds",
    Histogram,
    "数据库客户端活跃调用时长，不含连接和消费间隔。",
    &["method", "driver", "datasource", "operation", "status"],
    LATENCY_SECONDS
);
descriptor!(
    DB_OPEN,
    "namapper_db_client_in_flight",
    Gauge,
    "尚未终止的真实数据库调用。",
    &["method", "driver", "datasource", "operation"],
    &[]
);
descriptor!(
    ROWS,
    "namapper_rows_total",
    Counter,
    "交付行或协议确认影响行累计量。",
    &["method", "driver", "datasource", "operation", "kind"],
    &[]
);
descriptor!(
    SLOW,
    "namapper_slow_operations_total",
    Counter,
    "达到冻结慢阈值的数据库调用。",
    &["method", "driver", "datasource", "operation"],
    &[]
);
descriptor!(
    STREAMS,
    "namapper_streams_total",
    Counter,
    "结果流终止量。",
    &["method", "driver", "datasource", "outcome"],
    &[]
);
descriptor!(
    TTFR,
    "namapper_stream_time_to_first_row_seconds",
    Histogram,
    "首次 poll 到首行交付的客户端时长。",
    &["method", "driver", "datasource"],
    LATENCY_SECONDS
);
descriptor!(
    LIFETIME,
    "namapper_stream_lifetime_seconds",
    Histogram,
    "从构造到终止的结果流连接占用时长。",
    &["method", "driver", "datasource", "status"],
    STREAM_LIFETIME_SECONDS
);
descriptor!(
    STREAM_OPEN,
    "namapper_stream_open",
    Gauge,
    "已构造且尚未终止的结果流。",
    &["method", "driver", "datasource"],
    &[]
);
static DESCRIPTORS: [&MetricDescriptor; 12] = [
    &CALLS,
    &CALL_DURATION,
    &CALL_OPEN,
    &DB_CALLS,
    &DB_DURATION,
    &DB_OPEN,
    &ROWS,
    &SLOW,
    &STREAMS,
    &TTFR,
    &LIFETIME,
    &STREAM_OPEN,
];

/// 进程静态 Mapper 目录的统一指标快照源。
pub struct MapperMetricsSource;
impl MapperMetricsSource {
    /// 业务作用：精确计算静态方法目录的最坏公开时间序列数。
    /// 参数说明：无。
    /// 返回：包含无限桶、sum/count、封闭 outcome 和实际确定标签组合的上界。
    pub fn worst_case_series(&self) -> usize {
        MAPPER_METHOD_META
            .iter()
            .map(|meta| {
                15 + 3 * (LATENCY_SECONDS.len() + 3)
                    + 1
                    + 16
                    + 3 * (LATENCY_SECONDS.len() + 3)
                    + 1
                    + usize::from(meta.record_rows.load(Ordering::Relaxed))
                    + 1
                    + if meta.operation == "stream" {
                        5 + LATENCY_SECONDS.len() + 3 + 3 * (STREAM_LIFETIME_SECONDS.len() + 3) + 1
                    } else {
                        0
                    }
            })
            .sum()
    }
}
impl LegacyMetricsSource for MapperMetricsSource {
    /// 业务作用：提供固定指标合同供启动冲突校验和出口共用。
    /// 参数说明：无。
    /// 返回：方法、数据库和 Stream 的静态 descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &DESCRIPTORS
    }
    /// 业务作用：在出口侧一次读取全部方法单元，不让标签构造进入 SQL 完成路径。
    /// 参数说明：无。
    /// 返回：支持 Prometheus 与 OTLP 的同源结构化快照。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut out = Vec::new();
        for meta in MAPPER_METHOD_META {
            let c = &meta.cells;
            let mut push = |d: &'static MetricDescriptor,
                            stream: bool,
                            extra: &[(&'static str, &str)],
                            value| {
                let mut labels = meta.labels(!stream);
                labels.extend(extra.iter().map(|(key, value)| (*key, (*value).to_owned())));
                out.push(MetricSample {
                    name: d.name,
                    labels,
                    value,
                });
            };
            for path in CallPath::ALL {
                for outcome in CallOutcome::ALL {
                    push(
                        &CALLS,
                        false,
                        &[("path", path.label()), ("outcome", outcome.label())],
                        MetricValue::Counter(
                            c.calls[*path as usize * 5 + *outcome as usize].load(Ordering::Relaxed),
                        ),
                    );
                }
            }
            for status in Status::ALL {
                push(
                    &CALL_DURATION,
                    false,
                    &[("status", status.label())],
                    c.call_duration[*status as usize].snapshot(),
                );
                push(
                    &DB_DURATION,
                    false,
                    &[("status", status.label())],
                    c.db_duration[*status as usize].snapshot(),
                );
            }
            for outcome in DbOutcome::ALL {
                push(
                    &DB_CALLS,
                    false,
                    &[("outcome", outcome.label())],
                    MetricValue::Counter(c.operations[*outcome as usize].load(Ordering::Relaxed)),
                );
            }
            push(
                &CALL_OPEN,
                false,
                &[],
                MetricValue::Gauge(c.calls_open.load(Ordering::Relaxed) as f64),
            );
            push(
                &DB_OPEN,
                false,
                &[],
                MetricValue::Gauge(c.db_open.load(Ordering::Relaxed) as f64),
            );
            push(
                &SLOW,
                false,
                &[],
                MetricValue::Counter(c.slow.load(Ordering::Relaxed)),
            );
            if meta.record_rows.load(Ordering::Relaxed) {
                push(
                    &ROWS,
                    false,
                    &[(
                        "kind",
                        if matches!(meta.operation, "query" | "stream") {
                            "returned"
                        } else {
                            "affected"
                        },
                    )],
                    MetricValue::Counter(c.rows.load(Ordering::Relaxed)),
                );
            }
            if meta.operation == "stream" {
                for outcome in StreamOutcome::ALL {
                    push(
                        &STREAMS,
                        true,
                        &[("outcome", outcome.label())],
                        MetricValue::Counter(c.streams[*outcome as usize].load(Ordering::Relaxed)),
                    );
                }
                for status in Status::ALL {
                    push(
                        &LIFETIME,
                        true,
                        &[("status", status.label())],
                        c.lifetime[*status as usize].snapshot(),
                    );
                }
                push(&TTFR, true, &[], c.ttfr.snapshot());
                push(
                    &STREAM_OPEN,
                    true,
                    &[],
                    MetricValue::Gauge(c.streams_open.load(Ordering::Relaxed) as f64),
                );
            }
        }
        Some(out)
    }
    /// 业务作用：为独立使用方渲染同一结构化快照，避免多个 exporter 口径不同。
    /// 参数说明：`output` 接收 Prometheus 文本。
    /// 返回：追加当前快照，不注册或改变指标。
    fn render_prometheus(&self, output: &mut String) {
        nametrics_core::atomic::render_snapshot(
            &DESCRIPTORS,
            &self.snapshot().unwrap_or_default(),
            output,
        );
    }
}

/// 业务作用：为非 Application 宿主提供可显式注册的统一 Mapper 指标源。
/// 参数说明：无。
/// 返回：读取进程静态方法目录的共享 source；不创建 worker 或 exporter。
pub fn metrics_source() -> Arc<MapperMetricsSource> {
    Arc::new(MapperMetricsSource)
}
