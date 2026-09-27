//! SQL 观测的递归默认、严格字段合同与逐叶覆盖。

pub use nanotify_core::{DispatcherConfig, Severity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 观测日志的封闭级别；各事件进一步约束允许范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// 最细粒度的执行轨迹，适用于显式开启的语句输出。
    Trace,
    /// 开发诊断信息，作为语句输出的默认级别。
    Debug,
    /// 常规运行信息；具体事件仍校验允许的级别。
    Info,
    /// 需要关注的延迟或运行异常。
    Warn,
    /// 执行失败等需要处理的错误。
    Error,
}

macro_rules! policy {
    ($(#[$meta:meta])* $name:ident, $(#[$pmeta:meta])* $patch:ident { $($(#[$fmeta:meta])* $field:ident: $ty:ty = $default:expr),* $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
        #[serde(default, deny_unknown_fields)]
        pub struct $name { $($(#[$fmeta])* pub $field: $ty),* }
        impl Default for $name {
            /// 业务作用：提供该策略稳定的递归缺省值。
            /// 参数说明：无。
            /// 返回：未显式配置时的完整策略，不沿用历史进程状态。
            fn default() -> Self { Self { $($field: $default),* } }
        }
        $(#[$pmeta])*
        #[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
        #[serde(default, deny_unknown_fields)]
        pub struct $patch { $($(#[$fmeta])* #[serde(skip_serializing_if = "Option::is_none")] pub $field: Option<$ty>),* }
        impl $name {
            /// 业务作用：仅覆盖显式叶子，空子树保持上层业务策略。
            /// 参数说明：`patch` 为下层声明的可选叶子。
            /// 返回：更新当前策略，不重新套用全局默认。
            pub fn apply(&mut self, patch: &$patch) { $(if let Some(value) = &patch.$field { self.$field = value.clone(); })* }
        }
    };
}

policy!(
    /// 显式 SQL 语句输出策略；参数输出需单独开启并限制大小。
    ConsolePolicy,
    /// 数据源的语句输出覆盖；省略的字段继承全局策略。
    ConsolePatch {
        /// 是否启用逐条语句输出。
        enabled: bool = false,
        /// 逐条语句输出级别，仅接受 Debug 或 Trace。
        statement_level: LogLevel = LogLevel::Debug,
        /// 是否输出绑定参数；生产环境应保持关闭。
        include_parameters: bool = false,
        /// 每个参数可输出的最大字符数。
        max_parameter_chars: usize = 256,
        /// 一条语句可输出的最大参数个数。
        max_parameters: usize = 64,
    }
);
policy!(
    /// 慢 SQL 判定与日志策略，使用数据库客户端活跃时长。
    SlowPolicy,
    /// 慢 SQL 策略的逐叶覆盖；省略字段继承上层值。
    SlowPatch {
        /// 慢 SQL 阈值，单位毫秒，达到阈值即命中。
        threshold_ms: u64 = 1000,
        /// 是否输出命中的慢 SQL 日志。
        log_enabled: bool = true,
        /// 慢 SQL 日志级别，接受 Info、Warn 或 Error。
        log_level: LogLevel = LogLevel::Warn,
        /// 同一方法连续慢 SQL 日志的最小间隔，单位毫秒；零表示不冷却。
        log_cooldown_ms: u64 = 0,
        /// 是否在日志中附带 SQL 模板，不包含绑定参数。
        include_sql: bool = false,
        /// 日志中 SQL 模板的最大字符数。
        max_sql_chars: usize = 2048,
    }
);

impl SlowPolicy {
    /// 业务作用：为慢 SQL 指标、日志与通知使用同一原始时长门禁，防止出口间边界不一致。
    /// 参数说明：`duration` 为数据库客户端活跃时长，不含连接等待、缓存或 Stream 消费者停顿。
    /// 返回：时长达到或超过配置阈值时为真；直接比较 Duration，不截断为整数毫秒。
    pub fn is_slow(&self, duration: std::time::Duration) -> bool {
        duration >= std::time::Duration::from_millis(self.threshold_ms)
    }
}

policy!(
    /// 数据库执行失败的日志策略，不将未找到或取消视为执行错误。
    ErrorPolicy,
    /// 执行错误日志的逐叶覆盖；省略字段继承上层值。
    ErrorPatch {
        /// 是否输出数据库执行错误日志。
        log_enabled: bool = true,
        /// 执行错误日志级别，仅接受 Warn 或 Error。
        log_level: LogLevel = LogLevel::Error,
        /// 同一方法和错误分类的日志最小间隔，单位毫秒。
        log_cooldown_ms: u64 = 0,
        /// 是否在错误日志中附带 SQL 模板，不包含绑定参数。
        include_sql: bool = false,
        /// 是否输出驱动提供的数据库错误码。
        include_database_code: bool = true,
        /// 错误日志中 SQL 模板的最大字符数。
        max_sql_chars: usize = 2048,
    }
);
policy!(
    /// 连接获取或事务槽等待的延迟日志策略。
    WaitPolicy,
    /// 等待日志的逐叶覆盖；省略字段继承上层值。
    WaitPatch {
        /// 等待达到该毫秒数时命中延迟规则。
        threshold_ms: u64 = 250,
        /// 是否输出等待超阈值日志。
        log_enabled: bool = false,
        /// 等待日志级别，接受 Info 或 Warn。
        log_level: LogLevel = LogLevel::Warn,
        /// 同一等待观测单元的日志最小间隔，单位毫秒。
        log_cooldown_ms: u64 = 60000,
    }
);

policy!(
    /// 慢 SQL 或执行错误的异步通知策略。
    SqlAlertPolicy,
    /// SQL 通知的逐叶覆盖；省略字段继承上层值。
    SqlAlertPatch {
    /// 是否向通知分发器投递命中事件。
    enabled: bool = false,
    /// 已配置通知 provider 的引用名称；启用通知时必须能够解析。
    provider_ref: Option<String> = None,
    /// 通知携带的业务严重度。
    severity: Severity = Severity::Warning,
    /// 同一规则观测单元的通知最小间隔，单位毫秒。
    cooldown_ms: u64 = 60000,
    /// 是否在通知中附带 SQL 模板，不包含绑定参数。
    include_sql: bool = false,
    /// 通知中 SQL 模板的最大字符数。
    max_sql_chars: usize = 1024,
});
policy!(
    /// 连接获取超时的通知策略，可按用途限定触发范围。
    AcquireAlertPolicy,
    /// 连接超时通知的逐叶覆盖；省略字段继承上层值。
    AcquireAlertPatch {
    /// 是否投递连接获取超时通知。
    enabled: bool = false,
    /// 已配置通知 provider 的引用名称。
    provider_ref: Option<String> = None,
    /// 超时通知携带的业务严重度。
    severity: Severity = Severity::Error,
    /// 连接超时通知的最小间隔，单位毫秒。
    cooldown_ms: u64 = 60000,
    /// 允许触发通知的连接用途，默认只包含 mapper。
    purposes: Vec<String> = vec!["mapper".to_owned()],
});

/// 独立控制慢 SQL、执行错误和连接超时的通知；默认均关闭。
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Alerts {
    /// 慢 SQL 通知规则。
    pub slow_sql: SqlAlertPolicy,
    #[serde(deserialize_with = "deserialize_execution_alert")]
    /// 数据库执行错误通知规则。
    pub execution_error: SqlAlertPolicy,
    /// 连接获取超时通知规则。
    pub acquire_timeout: AcquireAlertPolicy,
}

/// 业务作用：让执行错误告警的空对象和缺省父级继承同一组错误级别默认值。
/// 参数说明：`deserializer` 提供显式声明的告警对象。
/// 返回：在错误默认值上逐叶覆盖的策略；未知字段仍由严格 patch 拒绝。
fn deserialize_execution_alert<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<SqlAlertPolicy, D::Error> {
    let patch = SqlAlertPatch::deserialize(deserializer)?;
    let mut value = SqlAlertPolicy {
        severity: Severity::Error,
        cooldown_ms: 30000,
        ..SqlAlertPolicy::default()
    };
    value.apply(&patch);
    Ok(value)
}
impl Default for Alerts {
    /// 业务作用：关闭全部离散通知，同时保留各事件独立的默认严重度与冷却预算。
    /// 参数说明：无。
    /// 返回：没有 provider 引用、不创建队列的策略。
    fn default() -> Self {
        Self {
            slow_sql: SqlAlertPolicy::default(),
            execution_error: SqlAlertPolicy {
                severity: Severity::Error,
                cooldown_ms: 30000,
                ..SqlAlertPolicy::default()
            },
            acquire_timeout: AcquireAlertPolicy::default(),
        }
    }
}

/// 数据源通知规则的逐叶覆盖，省略字段继承全局策略。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AlertsPatch {
    /// 慢 SQL 通知规则。
    pub slow_sql: SqlAlertPatch,
    /// 数据库执行错误通知规则。
    pub execution_error: SqlAlertPatch,
    /// 连接获取超时通知规则。
    pub acquire_timeout: AcquireAlertPatch,
}
/// 方法级 SQL 通知覆盖，不改变数据源连接获取规则。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MethodAlertsPatch {
    /// 慢 SQL 通知规则。
    pub slow_sql: SqlAlertPatch,
    /// 数据库执行错误通知规则。
    pub execution_error: SqlAlertPatch,
}

/// 按数据源覆盖全局观测策略，空子树不重置已继承的值。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatasourcePatch {
    /// 逐条 SQL 语句输出策略，参数输出受独立开关和预算限制。
    pub console: ConsolePatch,
    /// 按数据库活跃时长判定慢 SQL 的策略。
    pub slow_sql: SlowPatch,
    /// 数据库执行错误的日志策略。
    pub execution_error: ErrorPatch,
    /// 连接获取等待的日志策略。
    pub acquire_wait: WaitPatch,
    /// 事务槽等待的日志策略。
    pub transaction_slot_wait: WaitPatch,
    /// SQL 事件与连接超时的异步通知策略。
    pub alerts: AlertsPatch,
}
/// 按完整方法身份覆盖慢 SQL、执行错误及其通知策略。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MethodPatch {
    /// 按数据库活跃时长判定慢 SQL 的策略。
    pub slow_sql: SlowPatch,
    /// 数据库执行错误的日志策略。
    pub execution_error: ErrorPatch,
    /// SQL 事件与连接超时的异步通知策略。
    pub alerts: MethodAlertsPatch,
}

/// 方法或数据源的有效策略，不含 SQL、凭据、URL 或错误原文。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct EffectivePolicy {
    /// 逐条 SQL 语句输出策略，参数输出受独立开关和预算限制。
    pub console: ConsolePolicy,
    /// 按数据库活跃时长判定慢 SQL 的策略。
    pub slow_sql: SlowPolicy,
    /// 数据库执行错误的日志策略。
    pub execution_error: ErrorPolicy,
    /// 连接获取等待的日志策略。
    pub acquire_wait: WaitPolicy,
    /// 事务槽等待的日志策略。
    pub transaction_slot_wait: WaitPolicy,
    /// SQL 事件与连接超时的异步通知策略。
    pub alerts: Alerts,
}

/// 附加 SQL 指标开关；基础调用次数和时长始终采集。
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsPolicy {
    /// 是否记录准确行数，启动后随观测策略冻结。
    pub record_rows: bool,
}
impl Default for MetricsPolicy {
    /// 业务作用：默认统计准确行数，同时始终保持基础调用与时长采集。
    /// 参数说明：无。
    /// 返回：启用行数附加指标的策略。
    fn default() -> Self {
        Self { record_rows: true }
    }
}

/// 根配置；缺省与空对象均使用同一默认树。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SqlConfig {
    /// 全局 SQL 观测策略及数据源、方法覆盖。
    pub observability: ObservabilitySettings,
}

/// SQL 观测配置，按全局、数据源、方法顺序逐叶合并后冻结。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilitySettings {
    /// 逐条 SQL 语句输出策略，参数输出受独立开关和预算限制。
    pub console: ConsolePolicy,
    /// 行数等附加指标的采集策略。
    pub metrics: MetricsPolicy,
    /// 按数据库活跃时长判定慢 SQL 的策略。
    pub slow_sql: SlowPolicy,
    /// 数据库执行错误的日志策略。
    pub execution_error: ErrorPolicy,
    /// 连接获取等待的日志策略。
    pub acquire_wait: WaitPolicy,
    /// 事务槽等待的日志策略。
    pub transaction_slot_wait: WaitPolicy,
    /// SQL 事件与连接超时的异步通知策略。
    pub alerts: Alerts,
    /// 异步通知队列与投递资源预算。
    pub dispatcher: DispatcherConfig,
    /// 以数据源目录名称为键的策略覆盖。
    pub datasource_overrides: BTreeMap<String, DatasourcePatch>,
    /// 以完整静态方法身份为键的策略覆盖，优先于数据源策略。
    pub method_overrides: BTreeMap<String, MethodPatch>,
}

impl SqlConfig {
    /// 业务作用：从 SQL 子树解析严格递归配置，在产生 I/O 前拒绝未知字段与显式空值。
    /// 参数说明：`value` 为可缺省的 sql 子树。
    /// 返回：完整策略；错误只含配置合同，不回显用户输入。
    pub fn parse(value: Option<&serde_json::Value>) -> Result<Self, &'static str> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        reject_null(value)?;
        let config: Self = serde_json::from_value(value.clone())
            .map_err(|_| "invalid sql.observability schema")?;
        config.observability.validate()?;
        Ok(config)
    }
}

/// 业务作用：协调逐源 SQLx 事件开关与进程日志过滤器，保留未配置 console 时的旧指令入口。
/// 参数说明：`root` 为完整配置树，`datasource` 为待构造连接的目录名称。
/// 返回：该源应产生的语句级别；None 表示连接不产生逐条 SQL 事件。
pub fn statement_level(
    root: &serde_json::Value,
    datasource: &str,
) -> Result<Option<LogLevel>, &'static str> {
    let config = SqlConfig::parse(root.get("sql"))?;
    let policy = config.observability.effective(datasource, None).console;
    if console_is_explicit(root) {
        Ok(policy.enabled.then_some(policy.statement_level))
    } else {
        Ok(legacy_statement_level(root))
    }
}

/// 业务作用：派生容纳所有已启用数据源语句的进程过滤指令，并拒绝双入口冲突。
/// 参数说明：`root` 为完整启动或候选配置。
/// 返回：固定 target 指令；未启用 console 时显式关闭受管语句 target。
pub fn console_directive(root: &serde_json::Value) -> Result<String, &'static str> {
    let settings = SqlConfig::parse(root.get("sql"))?.observability;
    let mut level = statement_level(root, "default")?;
    for datasource in settings.datasource_overrides.keys() {
        let candidate = statement_level(root, datasource)?;
        if candidate == Some(LogLevel::Trace) || level.is_none() {
            level = candidate;
        }
    }
    if console_is_explicit(root)
        && root
            .pointer("/log/level")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|filter| {
                filter
                    .split(',')
                    .any(|entry| entry.trim().starts_with("sqlx::query="))
            })
    {
        return Err("use sql.observability.console instead of simultaneous sqlx::query directives");
    }
    Ok(format!(
        "sqlx::query={}",
        match level {
            Some(LogLevel::Trace) => "trace",
            Some(_) => "debug",
            None => "off",
        }
    ))
}

/// 业务作用：识别业务是否选择了 console 配置入口，避免默认树遮蔽兼容指令。
/// 参数说明：`root` 为尚未补齐默认值的配置树。
/// 返回：全局或任一数据源出现 console 子树时为真。
fn console_is_explicit(root: &serde_json::Value) -> bool {
    let Some(settings) = root.pointer("/sql/observability") else {
        return false;
    };
    settings.get("console").is_some()
        || settings
            .get("datasource_overrides")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|sources| {
                sources
                    .values()
                    .any(|source| source.get("console").is_some())
            })
}

/// 业务作用：只识别旧配置中精确 SQLx target 的 debug/trace 指令，不扩大其它日志目标。
/// 参数说明：`root` 为原始配置树。
/// 返回：最后一项受支持的兼容级别；其它级别不启用逐条输出。
fn legacy_statement_level(root: &serde_json::Value) -> Option<LogLevel> {
    root.pointer("/log/level")
        .and_then(serde_json::Value::as_str)?
        .split(',')
        .filter_map(|entry| entry.trim().strip_prefix("sqlx::query="))
        .next_back()
        .and_then(|level| match level.trim() {
            "debug" => Some(LogLevel::Debug),
            "trace" => Some(LogLevel::Trace),
            _ => None,
        })
}

/// 业务作用：统一拒绝把 null 当作缺省或覆盖重置，保留字段省略的唯一默认语义。
/// 参数说明：`value` 为尚未反序列化的观测配置子树。
/// 返回：无 null 时成功；显式 null 返回固定配置错误。
pub fn reject_null(value: &serde_json::Value) -> Result<(), &'static str> {
    match value {
        serde_json::Value::Null => Err("observability configuration does not accept null"),
        serde_json::Value::Object(map) => map.values().try_for_each(reject_null),
        serde_json::Value::Array(values) => values.iter().try_for_each(reject_null),
        _ => Ok(()),
    }
}

impl ObservabilitySettings {
    /// 业务作用：按 global、datasource、method 逐叶组合冻结策略。
    /// 参数说明：`datasource` 是目录名称，`method` 是可选完整静态方法身份。
    /// 返回：脱敏有效视图；空覆盖不重置继承值。
    pub fn effective(&self, datasource: &str, method: Option<&str>) -> EffectivePolicy {
        let mut out = EffectivePolicy {
            console: self.console.clone(),
            slow_sql: self.slow_sql.clone(),
            execution_error: self.execution_error.clone(),
            acquire_wait: self.acquire_wait.clone(),
            transaction_slot_wait: self.transaction_slot_wait.clone(),
            alerts: self.alerts.clone(),
        };
        if let Some(patch) = self.datasource_overrides.get(datasource) {
            out.console.apply(&patch.console);
            out.slow_sql.apply(&patch.slow_sql);
            out.execution_error.apply(&patch.execution_error);
            out.acquire_wait.apply(&patch.acquire_wait);
            out.transaction_slot_wait
                .apply(&patch.transaction_slot_wait);
            out.alerts.slow_sql.apply(&patch.alerts.slow_sql);
            out.alerts
                .execution_error
                .apply(&patch.alerts.execution_error);
            out.alerts
                .acquire_timeout
                .apply(&patch.alerts.acquire_timeout);
        }
        if let Some(patch) = method.and_then(|name| self.method_overrides.get(name)) {
            out.slow_sql.apply(&patch.slow_sql);
            out.execution_error.apply(&patch.execution_error);
            out.alerts.slow_sql.apply(&patch.alerts.slow_sql);
            out.alerts
                .execution_error
                .apply(&patch.alerts.execution_error);
        }
        out
    }

    /// 业务作用：在编译策略前验证阈值、字符上限、事件级别与通知预算。
    /// 参数说明：无。
    /// 返回：各层合并后合法时成功；目录存在性由宿主针对最终目录校验。
    pub fn validate(&self) -> Result<(), &'static str> {
        self.effective("", None).validate()?;
        for name in self.datasource_overrides.keys() {
            self.effective(name, None).validate()?;
        }
        for name in self.method_overrides.keys() {
            let mut policy = self.effective("", Some(name));
            // 方法所属数据源只有静态目录知道；这里检查数值，最终 provider 引用在完整继承后验证。
            policy.alerts.slow_sql.enabled = false;
            policy.alerts.execution_error.enabled = false;
            policy.validate()?;
        }
        let d = &self.dispatcher;
        if !(1..=65536).contains(&d.queue_capacity)
            || !(1..=32).contains(&d.max_in_flight)
            || !(100..=30000).contains(&d.delivery_timeout_ms)
            || d.shutdown_drain_timeout_ms > 30000
            || !(1..=5).contains(&d.max_attempts)
            || !(50..=5000).contains(&d.retry_initial_backoff_ms)
            || !(d.retry_initial_backoff_ms..=10000).contains(&d.retry_max_backoff_ms)
        {
            return Err("invalid sql.observability.dispatcher budget");
        }
        Ok(())
    }
}

impl EffectivePolicy {
    /// 业务作用：确认继承后的实际策略可以安全用于有界日志和非阻塞告警。
    /// 参数说明：无。
    /// 返回：全部值合法时成功；不执行凭据解析或网络动作。
    pub fn validate(&self) -> Result<(), &'static str> {
        use LogLevel::*;
        if !matches!(self.console.statement_level, Debug | Trace)
            || !(16..=4096).contains(&self.console.max_parameter_chars)
            || !(1..=256).contains(&self.console.max_parameters)
        {
            return Err("invalid SQL console policy");
        }
        if !(1..=300000).contains(&self.slow_sql.threshold_ms)
            || !matches!(self.slow_sql.log_level, Info | Warn | Error)
            || self.slow_sql.log_cooldown_ms > 86400000
            || !(128..=16384).contains(&self.slow_sql.max_sql_chars)
            || !matches!(self.execution_error.log_level, Warn | Error)
            || self.execution_error.log_cooldown_ms > 86400000
            || !(128..=16384).contains(&self.execution_error.max_sql_chars)
        {
            return Err("invalid SQL event log policy");
        }
        for wait in [&self.acquire_wait, &self.transaction_slot_wait] {
            if !(1..=300000).contains(&wait.threshold_ms)
                || !matches!(wait.log_level, Info | Warn)
                || wait.log_cooldown_ms > 86400000
            {
                return Err("invalid SQL wait policy");
            }
        }
        for alert in [&self.alerts.slow_sql, &self.alerts.execution_error] {
            if alert.cooldown_ms > 86400000 || !(128..=4096).contains(&alert.max_sql_chars) {
                return Err("invalid SQL alert budget");
            }
            if alert
                .provider_ref
                .as_deref()
                .is_some_and(|name| !valid_provider_id(name))
            {
                return Err("SQL alert provider_ref must be a valid identifier when supplied");
            }
        }
        let acquire = &self.alerts.acquire_timeout;
        if acquire.cooldown_ms > 86400000
            || acquire.purposes.is_empty()
            || acquire.purposes.len() > 4
            || acquire.purposes.iter().enumerate().any(|(i, purpose)| {
                !matches!(
                    purpose.as_str(),
                    "mapper" | "migration" | "probe" | "direct"
                ) || acquire.purposes[..i].contains(purpose)
            })
            || acquire
                .provider_ref
                .as_deref()
                .is_some_and(|name| !valid_provider_id(name))
        {
            return Err("invalid acquire_timeout alert policy");
        }
        Ok(())
    }
}

/// 业务作用：将 provider 名限制为启动时可冻结的低基数标识。
/// 参数说明：`name` 为配置中的 provider 引用。
/// 返回：长度与 ASCII 字符均符合合同则为真。
pub fn valid_provider_id(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name.as_bytes()[0].is_ascii_alphabetic()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}
