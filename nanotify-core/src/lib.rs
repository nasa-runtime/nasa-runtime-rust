//! 有界、渠道中立的离散事件通知合同。
//!
//! 队列生产端只向固定容量队列尝试入队，不调用第三方代码；宿主负责 worker 生命周期、
//! 超时、并发和停止。文本在构造通知时清理控制字符并限制长度，不接收数据库对象或任意 JSON。
//! 业务主动实现 [`Notify`] 并自行选择通知微服务的通信方式；本组件不提供渠道客户端、协议或机器人凭据配置。
//! 业务用 [`init`] 主动安装进程实现；[`notify`] 在未初始化时忽略消息，有实现时直接调用。
//! 直接调用不隐式获得队列、超时或异常隔离。受管 SQL 通知由宿主 worker 调用业务实现，
//! [`NotifyReceipt::accepted`] 为真才表示适配器接受，不代表最终收件；有界队列不提供持久送达或跨副本去重。

mod queue;
pub use queue::*;
mod registry;
pub use registry::*;

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// 进程级通知调度预算；未启用通知时仅保留配置，不创建运行资源。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DispatcherConfig {
    pub queue_capacity: usize,
    pub max_in_flight: usize,
    pub delivery_timeout_ms: u64,
    pub shutdown_drain_timeout_ms: u64,
    pub max_attempts: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

impl Default for DispatcherConfig {
    /// 业务作用：提供所有宿主共用的有限队列与投递预算。
    /// 参数说明：无。
    /// 返回：关闭无限等待和无限重试的稳定默认值。
    fn default() -> Self {
        Self {
            queue_capacity: 512,
            max_in_flight: 4,
            delivery_timeout_ms: 3000,
            shutdown_drain_timeout_ms: 3000,
            max_attempts: 1,
            retry_initial_backoff_ms: 250,
            retry_max_backoff_ms: 2000,
        }
    }
}

impl DispatcherConfig {
    /// 业务作用：在分配队列或调用渠道前验证调度资源和时间上限。
    /// 参数说明：无。
    /// 返回：配置合法时成功，否则只返回静态字段名。
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (valid, field) in [
            (
                (1..=65536).contains(&self.queue_capacity),
                "dispatcher.queue_capacity",
            ),
            (
                (1..=32).contains(&self.max_in_flight),
                "dispatcher.max_in_flight",
            ),
            (
                (100..=30000).contains(&self.delivery_timeout_ms),
                "dispatcher.delivery_timeout_ms",
            ),
            (
                self.shutdown_drain_timeout_ms <= 30000,
                "dispatcher.shutdown_drain_timeout_ms",
            ),
            (
                (1..=5).contains(&self.max_attempts),
                "dispatcher.max_attempts",
            ),
            (
                (50..=5000).contains(&self.retry_initial_backoff_ms),
                "dispatcher.retry_initial_backoff_ms",
            ),
            (
                (self.retry_initial_backoff_ms..=10000).contains(&self.retry_max_backoff_ms),
                "dispatcher.retry_max_backoff_ms",
            ),
        ] {
            if !valid {
                return Err(ConfigError(field));
            }
        }
        Ok(())
    }
}

/// 只携带静态配置路径的安全错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigError(pub &'static str);

impl fmt::Display for ConfigError {
    /// 业务作用：报告失败字段而不显示配置材料。
    /// 参数说明：`formatter` 接收脱敏诊断。
    /// 返回：格式化成功或格式化器失败。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid notification configuration: {}", self.0)
    }
}
impl std::error::Error for ConfigError {}

/// 固定离散事件集合，也是通知指标的低基数维度。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum EventKind {
    #[default]
    SlowSql,
    ExecutionError,
    AcquireTimeout,
}

impl EventKind {
    /// 业务作用：返回稳定事件名称供通知与指标共享。
    /// 参数说明：无。
    /// 返回：不包含调用参数的静态名称。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SlowSql => "slow_sql",
            Self::ExecutionError => "execution_error",
            Self::AcquireTimeout => "acquire_timeout",
        }
    }
}

/// 渠道中立的消息严重程度。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    #[default]
    Warning,
    Error,
    Critical,
}

/// 启动期冻结的通知来源身份；与指标出口使用同一进程标识。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotificationIdentity {
    pub service: String,
    pub instance: String,
    pub environment: Option<String>,
    pub cluster: Option<String>,
}

impl Severity {
    /// 业务作用：返回跨渠道一致的严重程度名称。
    /// 参数说明：无。
    /// 返回：固定的纯文本标识。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

/// 通知输入；构造后所有文本均由 [`Notification::new`] 施加统一边界。
#[derive(Clone, Default)]
pub struct NotificationFields {
    pub id: String,
    pub event: EventKind,
    pub severity: Severity,
    pub occurred_at_unix_ms: u64,
    pub service: String,
    pub instance: String,
    pub environment: Option<String>,
    pub cluster: Option<String>,
    pub driver: String,
    pub datasource: String,
    pub operation: String,
    pub method: Option<String>,
    pub purpose: Option<String>,
    pub duration: Duration,
    pub outcome: String,
    pub error_kind: Option<String>,
    pub prepared_sql: Option<String>,
    pub trace_id: Option<String>,
    pub slow: bool,
}

/// 拥有型且长度受限的通知；不实现输出消息内容的诊断格式化。
#[derive(Clone)]
pub struct Notification(NotificationFields);

impl Notification {
    /// 业务作用：在通知跨越异步队列前建立文本长度和控制字符边界。
    /// 参数说明：`fields` 是仅在规则命中后构造的通知输入，不得包含凭据或绑定参数。
    /// 返回：标识最多 128 字符、方法最多 512 字符、SQL 最多 4096 字符的只读通知。
    pub fn new(mut fields: NotificationFields) -> Self {
        for text in [
            &mut fields.id,
            &mut fields.service,
            &mut fields.instance,
            &mut fields.driver,
            &mut fields.datasource,
            &mut fields.operation,
            &mut fields.outcome,
        ] {
            *text = bounded_text(text, 128);
        }
        for value in [
            &mut fields.environment,
            &mut fields.cluster,
            &mut fields.purpose,
            &mut fields.error_kind,
            &mut fields.trace_id,
        ]
        .into_iter()
        .flatten()
        {
            *value = bounded_text(value, 128);
        }
        if let Some(value) = &mut fields.method {
            *value = bounded_text(value, 512);
        }
        if let Some(value) = &mut fields.prepared_sql {
            *value = bounded_text(value, 4096);
        }
        Self(fields)
    }

    /// 业务作用：允许 transport 读取已经通过边界约束的消息。
    /// 参数说明：无。
    /// 返回：不可修改的通知内容，调用方不得写入普通日志。
    pub fn fields(&self) -> &NotificationFields {
        &self.0
    }
}

/// 业务作用：按 Unicode 边界限制长度并消除控制与方向覆盖字符，防止消息结构注入。
/// 参数说明：`text` 为待投递内容，`max_chars` 为字符数硬上限。
/// 返回：长度有界的纯文本；控制字符替换为空格，截断结尾以省略号标示。
pub fn bounded_text(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut chars = text.chars();
    let mut output = String::with_capacity(text.len().min(max_chars.saturating_mul(4)));
    for _ in 0..max_chars {
        let Some(ch) = chars.next() else {
            return output;
        };
        output.push(
            if ch.is_control() || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') {
                ' '
            } else {
                ch
            },
        );
    }
    if chars.next().is_some() {
        output.pop();
        output.push('…');
    }
    output
}

/// 渠道受理回执，不携带原始响应正文；未受理不等同于 trait 返回错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyReceipt {
    /// 业务适配器提供的可选消息标识，不应包含渠道凭据或响应正文。
    pub message_id: Option<String>,
    /// true 表示下游受理，false 在受管 dispatcher 中记为拒绝；均不证明最终用户收件。
    pub accepted: bool,
}

/// 稳定失败分类；原始响应与 URL 不进入错误链。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum NotifyErrorKind {
    Timeout,
    Unavailable,
    RateLimited,
    Rejected,
    Authentication,
    InvalidRequest,
    Other,
}

impl NotifyErrorKind {
    /// 业务作用：把渠道失败映射为低基数诊断名称。
    /// 参数说明：无。
    /// 返回：稳定的静态失败名称。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::RateLimited => "rate_limited",
            Self::Rejected => "rejected",
            Self::Authentication => "authentication",
            Self::InvalidRequest => "invalid_request",
            Self::Other => "other",
        }
    }
}

/// 明确的重试证据；结果未知不得自行重发。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrySafety {
    Never,
    Safe { retry_after: Option<Duration> },
}

/// 脱敏错误，只描述稳定分类及重试证据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotifyError {
    pub kind: NotifyErrorKind,
    pub retry: RetrySafety,
}

impl NotifyError {
    /// 业务作用：构造不能安全重发的通知失败。
    /// 参数说明：`kind` 是不含原始响应的分类。
    /// 返回：默认禁止重试的错误。
    pub const fn new(kind: NotifyErrorKind) -> Self {
        Self {
            kind,
            retry: RetrySafety::Never,
        }
    }
    /// 业务作用：声明连接未发送或服务明确拒绝后的安全重试。
    /// 参数说明：`kind` 为分类，`retry_after` 为服务要求的最小等待。
    /// 返回：携带重试证据的错误，仍受宿主总预算限制。
    pub const fn retryable(kind: NotifyErrorKind, retry_after: Option<Duration>) -> Self {
        Self {
            kind,
            retry: RetrySafety::Safe { retry_after },
        }
    }
}

impl fmt::Display for NotifyError {
    /// 业务作用：避免渠道凭据或响应正文进入普通错误链。
    /// 参数说明：`formatter` 接收稳定分类。
    /// 返回：格式化器的写入结果。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.as_str())
    }
}
impl std::error::Error for NotifyError {}

/// 受管 SQL 通知只在宿主管理的异步 worker 内调用，不得在 SQL 生产路径调用。
/// 业务也可直接调用进程通知接口，此时由业务负责超时、异常隔离与执行边界。
/// 业务实现负责调用通知微服务；协议、鉴权和下游消息发送由业务拥有，框架不连接具体消息渠道。
/// Future 必须协作让出执行权且取消安全；同步阻塞或自行派生的后台任务不受宿主时间预算保护。
#[async_trait::async_trait]
pub trait Notify: Send + Sync {
    /// 业务作用：投递一条已完成裁决和脱敏的通知，不参与业务事务。
    /// 参数说明：`notification` 是只读有界消息。
    /// 返回：包含是否受理的回执或稳定失败；受理不表示最终用户收到，未知结果不得声明为安全重试。
    async fn notify(&self, notification: &Notification) -> Result<NotifyReceipt, NotifyError>;
}
