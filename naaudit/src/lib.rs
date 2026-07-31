//! NASA 业务审计事件。
//!
//! 日志是运维诊断,审计是**不可抵赖的业务事实**:谁(actor)在何时(occurred_at)对什么(resource)
//! 做了什么(action)、结果如何(outcome)。审计事件必须**可靠投递**——[`OutboxAuditSink`] 把事件写进
//! Outbox(可靠投递复用 Outbox),与业务写同事务落库、由 dispatcher/CDC 发出,避免"业务成功但
//! 审计丢失"。
//!
//! 本 crate **不依赖 `napp`**;时间由调用方传入(用 `nadate::UtcClock`),context 只放**已脱敏**字段。

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
    /// 创建脱敏错误。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for AuditWriteError {
    /// 输出不包含 SQL、凭据或审计载荷的稳定错误摘要。
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
    /// 用必填字段创建审计事件(无附加 context)。
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

    /// 追加一条脱敏上下文键值。
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// 按统一映射约定转成 outbox 事件:aggregate_type=`Audit`、aggregate_id=actor、event_type=action、
    /// payload=事件 JSON。
    ///
    /// [`OutboxAuditSink`](同步 `OutboxWriter` 路径)与**异步持久 outbox**(如 `MySqlOutbox::append`,在
    /// `natx` 事务内与业务写同提交)共用本转换——异步后端不实现同步 `OutboxWriter`,业务在事务内直接
    /// `outbox.append(&event.into_outbox_event()).await` 即可,映射口径与 sink 完全一致。
    pub fn into_outbox_event(self) -> OutboxEvent {
        // 序列化恒成功(全字段可序列化);极端失败降级为空 payload,不因审计序列化 panic 业务。
        let payload = serde_json::to_vec(&self).unwrap_or_default();
        OutboxEvent::new("Audit", self.actor, self.action, payload)
    }
}

/// 审计投递端。
pub trait AuditSink {
    /// 记录一条审计事件(实现负责可靠投递)。
    fn record(&self, event: AuditEvent);
}

/// 必须加入调用方业务事务的异步持久审计端。
///
/// 实现不得在缺少 ambient transaction 时退化成 autocommit，否则会重新制造“业务回滚但审计已落库”
/// 或“业务提交但审计失败”的双写窗口。
#[async_trait::async_trait]
pub trait TransactionalAuditSink: Send + Sync {
    /// 在当前业务事务内记录事件。
    async fn record_transactional(&self, event: AuditEvent) -> Result<(), AuditWriteError>;
}

/// 经 Outbox 可靠投递的审计 sink:事件序列化为 JSON 写入 outbox,与业务写同事务落库。
///
/// `OutboxEvent` 字段:aggregate_type=`Audit`、aggregate_id=actor、event_type=action、payload=事件 JSON。
pub struct OutboxAuditSink<W: OutboxWriter> {
    writer: W,
}

impl<W: OutboxWriter> OutboxAuditSink<W> {
    /// 用一个 outbox writer 构造。
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: OutboxWriter> AuditSink for OutboxAuditSink<W> {
    /// 把审计事实映射为 outbox 事件，交由同步 writer 加入当前可靠投递路径。
    fn record(&self, event: AuditEvent) {
        // 与异步持久路径共用同一映射约定(单一来源,见 `AuditEvent::into_outbox_event`)。
        self.writer.append(event.into_outbox_event());
    }
}
