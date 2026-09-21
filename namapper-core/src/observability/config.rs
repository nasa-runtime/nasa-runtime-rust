//! SQL 观测的递归默认、严格字段合同与逐叶覆盖。

pub use nanotify_core::{DispatcherConfig, Severity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 观测日志的封闭级别；各事件进一步约束允许范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

macro_rules! policy {
    ($name:ident, $patch:ident { $($field:ident: $ty:ty = $default:expr),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
        #[serde(default, deny_unknown_fields)]
        pub struct $name { $(pub $field: $ty),* }
        impl Default for $name {
            /// 业务作用：提供该策略稳定的递归缺省值。
            /// 参数说明：无。
            /// 返回：未显式配置时的完整策略，不沿用历史进程状态。
            fn default() -> Self { Self { $($field: $default),* } }
        }
        #[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
        #[serde(default, deny_unknown_fields)]
        pub struct $patch { $(#[serde(skip_serializing_if = "Option::is_none")] pub $field: Option<$ty>),* }
        impl $name {
            /// 业务作用：仅覆盖显式叶子，空子树保持上层业务策略。
            /// 参数说明：`patch` 为下层声明的可选叶子。
            /// 返回：更新当前策略，不重新套用全局默认。
            pub fn apply(&mut self, patch: &$patch) { $(if let Some(value) = &patch.$field { self.$field = value.clone(); })* }
        }
    };
}

policy!(
    ConsolePolicy,
    ConsolePatch {
        enabled: bool = false,
        statement_level: LogLevel = LogLevel::Debug,
        include_parameters: bool = false,
        max_parameter_chars: usize = 256,
        max_parameters: usize = 64,
    }
);
policy!(
    SlowPolicy,
    SlowPatch {
        threshold_ms: u64 = 1000,
        log_enabled: bool = true,
        log_level: LogLevel = LogLevel::Warn,
        log_cooldown_ms: u64 = 0,
        include_sql: bool = false,
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
    ErrorPolicy,
    ErrorPatch {
        log_enabled: bool = true,
        log_level: LogLevel = LogLevel::Error,
        log_cooldown_ms: u64 = 0,
        include_sql: bool = false,
        include_database_code: bool = true,
        max_sql_chars: usize = 2048,
    }
);
policy!(
    WaitPolicy,
    WaitPatch {
        threshold_ms: u64 = 250,
        log_enabled: bool = false,
        log_level: LogLevel = LogLevel::Warn,
        log_cooldown_ms: u64 = 60000,
    }
);

policy!(SqlAlertPolicy, SqlAlertPatch {
    enabled: bool = false,
    provider_ref: Option<String> = None,
    severity: Severity = Severity::Warning,
    cooldown_ms: u64 = 60000,
    include_sql: bool = false,
    max_sql_chars: usize = 1024,
});
policy!(AcquireAlertPolicy, AcquireAlertPatch {
    enabled: bool = false,
    provider_ref: Option<String> = None,
    severity: Severity = Severity::Error,
    cooldown_ms: u64 = 60000,
    purposes: Vec<String> = vec!["mapper".to_owned()],
});

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Alerts {
    pub slow_sql: SqlAlertPolicy,
    #[serde(deserialize_with = "deserialize_execution_alert")]
    pub execution_error: SqlAlertPolicy,
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

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AlertsPatch {
    pub slow_sql: SqlAlertPatch,
    pub execution_error: SqlAlertPatch,
    pub acquire_timeout: AcquireAlertPatch,
}
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MethodAlertsPatch {
    pub slow_sql: SqlAlertPatch,
    pub execution_error: SqlAlertPatch,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatasourcePatch {
    pub console: ConsolePatch,
    pub slow_sql: SlowPatch,
    pub execution_error: ErrorPatch,
    pub acquire_wait: WaitPatch,
    pub transaction_slot_wait: WaitPatch,
    pub alerts: AlertsPatch,
}
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MethodPatch {
    pub slow_sql: SlowPatch,
    pub execution_error: ErrorPatch,
    pub alerts: MethodAlertsPatch,
}

/// 方法或数据源的有效策略，不含 SQL、凭据、URL 或错误原文。
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct EffectivePolicy {
    pub console: ConsolePolicy,
    pub slow_sql: SlowPolicy,
    pub execution_error: ErrorPolicy,
    pub acquire_wait: WaitPolicy,
    pub transaction_slot_wait: WaitPolicy,
    pub alerts: Alerts,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsPolicy {
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
    pub observability: ObservabilitySettings,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilitySettings {
    pub console: ConsolePolicy,
    pub metrics: MetricsPolicy,
    pub slow_sql: SlowPolicy,
    pub execution_error: ErrorPolicy,
    pub acquire_wait: WaitPolicy,
    pub transaction_slot_wait: WaitPolicy,
    pub alerts: Alerts,
    pub dispatcher: DispatcherConfig,
    pub datasource_overrides: BTreeMap<String, DatasourcePatch>,
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
