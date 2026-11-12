//! NASA 业务审计事件。
//!
//! 用统一事件记录执行者、发生时间、资源、动作与结局。调用方负责核实事实和脱敏，
//! 本组件不提供签名或防篡改存储。需要与业务写原子提交时使用 [`TransactionalAuditSink`]，
//! 并将写入错误传播至业务事务边界；事务适配器与后续投递端共同承担持久投递。
//! [`OutboxAuditSink`] 仅适配同步写入端，其持久性与事务保证取决于具体 [`OutboxWriter`] 实现。
//!
//! 本 crate **不依赖 `napp`**;时间由调用方传入(可使用 `nabase::date::UtcClock`),context 只放**已脱敏**字段。

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use naoutbox_core::{OutboxEvent, OutboxWriter};
use serde::Serialize;

/// 持久审计写入失败；只保存稳定、脱敏的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditWriteError {
    /// 不含 SQL、凭据或审计 payload 的稳定原因。
    pub reason: String,
}

impl AuditWriteError {
    /// 业务作用：保存供业务调用方处理的稳定审计失败原因，不自动过滤传入文本。
    ///
    /// 参数说明：
    /// - `reason`：调用方已脱敏的失败分类，不得包含 SQL、凭据或审计正文。
    ///
    /// 返回：持有该原因的错误值，不执行日志输出或审计写入。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for AuditWriteError {
    /// 业务作用：以固定前缀输出保存的失败原因，脱敏由错误构造方保证。
    ///
    /// 参数说明：
    /// - `formatter`：接收错误摘要的格式化输出目标。
    ///
    /// 返回：摘要写入成功时返回 `Ok`，目标拒绝写入时传播格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "audit write failed: {}", self.reason)
    }
}

impl std::error::Error for AuditWriteError {}

/// 审计结局。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// 操作成功。
    Success,
    /// 操作失败/被拒。
    Failure,
}

/// 一条业务审计事件。字段只承载**对审计安全**的信息(不含 secret/token/payload 明文)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEvent {
    /// 执行者(subject/client id 等稳定标识)。
    pub actor: String,
    /// 动作(如 `order.create`、`user.role.grant`)。
    pub action: String,
    /// 被作用资源(如 `order:42`)。
    pub resource: String,
    /// 结局。
    pub outcome: AuditOutcome,
    /// 发生时刻(epoch 毫秒;调用方从 UtcClock 取)。
    pub occurred_at_millis: u64,
    /// 附加脱敏上下文(如 tenant、request_id);不放敏感值。
    pub context: BTreeMap<String, String>,
}

impl AuditEvent {
    /// 业务作用：组装审计事实的主体、动作与结果；真实性、脱敏和字段有效性由调用方保证。
    ///
    /// 参数说明：
    /// - `actor`：已认证执行者的稳定标识，不携带认证凭据。
    /// - `action`：业务动作名称，用作 Outbox 事件类型。
    /// - `resource`：被操作资源的安全标识。
    /// - `outcome`：本次业务操作的实际结局。
    /// - `occurred_at_millis`：业务事实发生时的 Unix 时间戳，单位为毫秒。
    ///
    /// 返回：附加上下文为空的事件；不校验空值、不读取时钟，也不持久化。
    pub fn new(
        actor: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
        outcome: AuditOutcome,
        occurred_at_millis: u64,
    ) -> Self {
        Self {
            actor: actor.into(),
            action: action.into(),
            resource: resource.into(),
            outcome,
            occurred_at_millis,
            context: BTreeMap::new(),
        }
    }

    /// 业务作用：为审计事实附加调用方已脱敏的上下文，供后续检索和关联。
    ///
    /// 参数说明：
    /// - `key`：上下文名称，相同名称覆盖原值。
    /// - `value`：可安全存储的上下文值，不得传入令牌或敏感正文。
    ///
    /// 返回：更新后的事件；不自动脱敏，也不改变事件的主体、动作或发生时间。
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// 业务作用：按统一映射将审计事实编码为 Outbox 事件，供同步写入端和持久事务适配器共用。
    ///
    /// 参数说明：无。
    ///
    /// 返回：聚合类型为 `Audit`、聚合标识为执行者、事件类型为动作的事件，正文为审计 JSON。
    /// 新事件生成独立事件 ID，使用默认系统租户；转换本身不建立事务或执行投递。
    /// JSON 编码返回错误时正文为空，调用方无法通过本接口取得编码错误。
    pub fn into_outbox_event(self) -> OutboxEvent {
        // 当前字段仅含 JSON 可表示值；仍保留编码错误时的空正文分支，不将编码错误向业务传播。
        let payload = serde_json::to_vec(&self).unwrap_or_default();
        OutboxEvent::new("Audit", self.actor, self.action, payload)
    }
}

/// 审计投递端。
pub trait AuditSink {
    /// 业务作用：将审计事实交给同步记录端，存储介质与投递保证由实现决定。
    ///
    /// 参数说明：
    /// - `event`：已核实并脱敏的审计事件。
    ///
    /// 返回：实现完成同步调用后返回，不提供持久化确认或可传播的错误结果。
    fn record(&self, event: AuditEvent);
}

/// 必须加入调用方业务事务的异步持久审计端。
///
/// 实现不得在缺少 ambient transaction 时退化成 autocommit，否则会重新制造“业务回滚但审计已落库”
/// 或“业务提交但审计失败”的双写窗口。
#[async_trait::async_trait]
pub trait TransactionalAuditSink: Send + Sync {
    /// 业务作用：将审计事实加入当前业务事务，要求实现拒绝脱离该事务的独立提交。
    ///
    /// 参数说明：
    /// - `event`：需要与业务写入共同提交或回滚的审计事件。
    ///
    /// 返回：加入事务成功时返回 `Ok`，此时事务尚未由本接口提交；缺少有效事务或写入失败
    /// 返回脱敏错误，调用方应将错误传播至业务事务边界以阻止不完整提交。
    async fn record_transactional(&self, event: AuditEvent) -> Result<(), AuditWriteError>;
}

/// 将审计 JSON 交给同步 Outbox 写入端的适配器，不额外提供持久性或事务保证。
///
/// `OutboxEvent` 字段:aggregate_type=`Audit`、aggregate_id=actor、event_type=action、payload=事件 JSON。
pub struct OutboxAuditSink<W: OutboxWriter> {
    /// 承担实际存储与投递保证的同步写入端。
    writer: W,
}

impl<W: OutboxWriter> OutboxAuditSink<W> {
    /// 业务作用：绑定同步 Outbox 写入端，使审计事件沿用该写入端的存储与投递方式。
    ///
    /// 参数说明：
    /// - `writer`：接收转换后事件的写入端，其实现决定是否持久化及是否加入业务事务。
    ///
    /// 返回：持有该写入端的适配器；构造时不写入事件，不创建数据库事务。
    pub fn new(writer: W) -> Self {
        Self {
            writer,
        }
    }
}

impl<W: OutboxWriter> AuditSink for OutboxAuditSink<W> {
    /// 业务作用：将审计事实转换后同步交给绑定的 Outbox 写入端。
    ///
    /// 参数说明：
    /// - `event`：已核实并脱敏的审计事实，转换后使用执行者作为聚合标识。
    ///
    /// 返回：底层追加调用完成后返回，不独立确认事务提交或下游投递，也不捕获写入端 panic。
    fn record(&self, event: AuditEvent) {
        // 与异步持久路径共用同一映射约定(单一来源,见 `AuditEvent::into_outbox_event`)。
        self.writer.append(event.into_outbox_event());
    }
}
