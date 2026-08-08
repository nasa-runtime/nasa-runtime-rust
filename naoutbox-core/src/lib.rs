//! NASA Outbox 核心。
//!
//! **只负责在业务 DB 事务内写 outbox 事件**(`outbox-core` 只依赖 `natx`)。轮询发往 Kafka 的
//! dispatcher、或 CDC(Debezium)读取,都是本层之上的可选增量;应用不为 CDC 强制启用 Kafka。
//!
//! 事件字段对齐 Debezium Outbox Event Router 约定:aggregate type/id、event type、payload、
//! 可选 trace context。

#![forbid(unsafe_code)]

use std::sync::Mutex;

/// 一条 outbox 事件(Debezium Outbox Event Router 兼容字段)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    /// 全局唯一事件 id；至少一次投递时消费者必须按此字段去重。
    pub event_id: String,
    /// 聚合根类型(路由/topic 依据),如 `Order`。
    pub aggregate_type: String,
    /// 聚合根 id(常作分区 key),如 `42`。
    pub aggregate_id: String,
    /// 事件类型,如 `OrderCreated`。
    pub event_type: String,
    /// 事件负载(通常 JSON 字节)。
    pub payload: Vec<u8>,
    /// 可选 W3C `traceparent`,供跨 DB→Kafka 传播 trace。
    pub traceparent: Option<String>,
}

impl OutboxEvent {
    /// 业务作用：用必填字段创建事件(无 trace context)。
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        event_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            event_type: event_type.into(),
            payload,
            traceparent: None,
        }
    }

    /// 业务作用：附加 W3C trace context(链路穿透)。
    pub fn with_traceparent(mut self, traceparent: impl Into<String>) -> Self {
        self.traceparent = Some(traceparent.into());
        self
    }
}

/// 在业务 DB 事务内追加 outbox 事件的写入端(与业务写同事务,保证原子)。
///
/// 持久实现可在 `natx` 事务上下文里 INSERT；trait 让业务与不同 provider 解耦。
pub trait OutboxWriter {
    /// 业务作用：追加一条事件(应与当前业务事务同提交)。
    fn append(&self, event: OutboxEvent);
}

/// 借用透传:`&W` 也是 writer,方便把同一个 outbox 借给审计等多个 sink 而无需 Arc。
impl<W: OutboxWriter + ?Sized> OutboxWriter for &W {
    /// 业务作用：将借用 writer 的调用透明转发到原实现。
    fn append(&self, event: OutboxEvent) {
        (**self).append(event);
    }
}

/// 进程内 outbox，适用于允许进程重启后丢失待投事件的非持久场景。
#[derive(Default)]
pub struct InMemoryOutbox {
    events: Mutex<Vec<OutboxEvent>>,
    dispatch: tokio::sync::Mutex<()>,
}

impl InMemoryOutbox {
    /// 业务作用：创建空 outbox。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：取走全部已追加事件(FIFO),清空缓冲——模拟 dispatcher 一批取出投递。
    pub fn drain(&self) -> Vec<OutboxEvent> {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *events)
    }

    /// 业务作用：当前缓冲的事件数。
    pub fn len(&self) -> usize {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// 业务作用：是否为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl OutboxWriter for InMemoryOutbox {
    /// 业务作用：按调用顺序把事件追加到进程内 FIFO。
    fn append(&self, event: OutboxEvent) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
    }
}

// ───────────────────────────── dispatcher(轮询投递核心)─────────────────────────────

/// 投递失败原因(脱敏;不含 broker 地址/凭据/payload)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxPublishError {
    /// 稳定脱敏原因(如 "kafka publish failed")。
    pub reason: String,
}

impl OutboxPublishError {
    /// 业务作用：用脱敏原因构造。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for OutboxPublishError {
    /// 业务作用：输出稳定脱敏原因，不包含 broker、topic 或 payload。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "outbox publish error: {}", self.reason)
    }
}

impl std::error::Error for OutboxPublishError {}

/// 把 outbox 事件投递到下游(如 Kafka topic)的发布端。
///
/// provider-neutral:Kafka 适配器 impl 本 trait(经 nafka 的已配置 producer lane 发 `aggregate_type`
/// 对应 topic、`aggregate_id` 作 key);进程内捕获实现。**至少一次**:失败即由 dispatcher 保留重试,
/// 故下游消费者需按 `event_id` 幂等去重；聚合类型/id/事件类型不是事件唯一键。
#[async_trait::async_trait]
pub trait OutboxPublisher {
    /// 业务作用：投递一条事件;失败返回 [`OutboxPublishError`](该事件将被保留、下轮重试)。
    async fn publish(&self, event: &OutboxEvent) -> Result<(), OutboxPublishError>;
}

/// 一批投递的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchReport {
    /// 本批成功投递的事件数(保序前缀)。
    pub published: usize,
    /// 本批待投递事件总数。
    pub total: usize,
    /// 首个失败的脱敏原因;全部成功则为 `None`。
    pub first_error: Option<OutboxPublishError>,
}

impl DispatchReport {
    /// 业务作用：是否整批投递完成。
    pub fn is_complete(&self) -> bool {
        self.published == self.total
    }
}

/// 业务作用：**保序至少一次**投递一批事件:按序逐条 `publish`,遇首个失败即停(不跳过,保投递顺序),
/// 已成功的前缀记入 `published`。调用方据 `published` 移除已投递事件、保留未投递的下轮重试。
///
/// 停在首个失败(而非继续后续)是为保证同聚合根事件的**顺序**:跳过失败项会让后发事件先落地。
pub async fn dispatch_in_order<P>(events: &[OutboxEvent], publisher: &P) -> DispatchReport
where
    P: OutboxPublisher + ?Sized,
{
    let mut published = 0;
    let mut first_error = None;
    for event in events {
        match publisher.publish(event).await {
            Ok(()) => published += 1,
            Err(error) => {
                first_error = Some(error);
                break;
            }
        }
    }
    DispatchReport {
        published,
        total: events.len(),
        first_error,
    }
}

impl InMemoryOutbox {
    /// 业务作用：一次投递轮:取走全部事件 → 保序投递 → **把未投递的按原序放回**(下轮重试)。返回本轮报告。
    ///
    /// 与 `drain` 不同:失败的事件不丢,保留在 outbox 里。用于把 [`InMemoryOutbox`] 直接当 dispatcher
    /// 的事件源做端到端演示。
    pub async fn dispatch_once<P>(&self, publisher: &P) -> DispatchReport
    where
        P: OutboxPublisher + ?Sized,
    {
        // 同一 source 只能有一个投递轮；否则第一轮 drain 后追加的新事件可能被第二轮抢先发布，
        // 破坏本类型承诺的 FIFO。等待 gate 被取消时尚未 drain，不需要恢复。
        let _dispatch = self.dispatch.lock().await;
        // 把本批所有权放进 RAII 守卫：若调用者在 publish await 中取消本 future，守卫会把整批
        // 放回队首。正常完成后只放回未发布后缀。取消点前一次 publish 是否已抵达下游不可知，
        // 因而恢复整批是正确的“至少一次”选择（可能重复，但绝不静默丢失）。
        let mut batch = DrainedBatch::new(self, self.drain());
        let report = dispatch_in_order(&batch.events, publisher).await;
        batch.published = report.published;
        report
    }
}

/// 已从进程内 outbox 取出的临时批次；任何提前退出都会把尚未确认的事件恢复到队首。
struct DrainedBatch<'a> {
    outbox: &'a InMemoryOutbox,
    events: Vec<OutboxEvent>,
    published: usize,
}

impl<'a> DrainedBatch<'a> {
    /// 业务作用：创建尚未确认任何发布前缀的批次恢复守卫。
    fn new(outbox: &'a InMemoryOutbox, events: Vec<OutboxEvent>) -> Self {
        Self {
            outbox,
            events,
            published: 0,
        }
    }
}

impl Drop for DrainedBatch<'_> {
    /// 业务作用：将未确认后缀恢复到当前队列之前，维持取消安全与原始 FIFO。
    fn drop(&mut self) {
        let published = self.published.min(self.events.len());
        let mut remaining = self.events.split_off(published);
        if remaining.is_empty() {
            return;
        }
        let mut current = self
            .outbox
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        remaining.append(&mut current);
        *current = remaining;
    }
}
