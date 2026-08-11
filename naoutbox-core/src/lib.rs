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
    /// 受信租户归因。写入权威是受信入口的 [`OutboxWriteContext`](crate::OutboxWriteContext)
    /// 与持久 `tenant` 列;从存储读出的事件由读取路径回填该列值。发布端与归档端据此
    /// 选择租户隔离空间、加密键与授权,禁止从 payload、`aggregate_id` 或自报 header
    /// 另行推导身份。构造入口未声明租户时固定为 [`SYSTEM_TENANT`]。
    pub tenant: String,
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
            tenant: SYSTEM_TENANT.to_string(),
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

// ───────────────────────────── 受信写入上下文与租户配额 ─────────────────────────────

/// 未走受信写入上下文的历史 append 路径映射到的固定租户。
pub const SYSTEM_TENANT: &str = "system";

/// 每租户在飞事件配额拒绝的稳定原因码；调用方以此与系统故障区分,不得改写。
pub const TENANT_QUOTA_EXCEEDED_REASON: &str = "outbox_tenant_quota_exceeded";

/// 业务作用：受信的 Outbox 写入上下文——租户身份只能由已认证的业务上下文填充。
///
/// `outbox_event` 的租户列是配额与归因的依据,绝不允许从 payload、`aggregate_id` 或
/// 自报 header 解析:那会把配额边界交给消息内容,任何能构造 payload 的调用方都能
/// 冒用他租户额度。未携带上下文的历史 append 路径固定映射 [`SYSTEM_TENANT`]。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxWriteContext {
    tenant: String,
}

impl OutboxWriteContext {
    /// 业务作用：从已认证的租户身份构造写入上下文。
    ///
    /// 参数说明：
    /// - `tenant`: 已认证租户;要求非空、长度 ≤256 且不含控制字符——上界与 Saga
    ///   `TenantId` 公开合同及租户列宽一致,合法租户不允许在 Outbox 双写处被窄化拒绝。
    ///
    /// 返回：合法时返回上下文;越界或含控制字符返回不透出内容的静态原因。
    pub fn new(tenant: impl Into<String>) -> Result<Self, &'static str> {
        let tenant = tenant.into();
        if tenant.is_empty() || tenant.len() > 256 {
            return Err("outbox tenant must be 1..=256 bytes");
        }
        if tenant.chars().any(char::is_control) {
            return Err("outbox tenant must not contain control characters");
        }
        Ok(Self { tenant })
    }

    /// 业务作用：读取已认证租户身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：租户字符串。
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

// ───────────────────────────── retention(保留、归档与清理合同)─────────────────────────────

/// 业务作用：冻结一份 Outbox 保留清理策略——执行器绝不从"开启了 Outbox"推断保留期，
/// 没有已批准策略就没有任何删除。
///
/// 行分类合同：`dispatched = 1` 的已投递行达到最小保留期后才可归档/删除；`dead = 1`
/// 死信默认不清理，只有独立批准（`dead_approval`）、最小年龄与归档收据齐备才可清理；
/// `dispatched = 0 AND dead = 0` 的待投递行**永不**进入保留清理，无论年龄。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRetentionPolicy {
    /// 已投递行的最小保留期（毫秒），到龄才成为清理候选；必须为正。
    pub dispatched_min_age_ms: i64,
    /// 单批候选行上限；每批立即提交，禁止无上限 DELETE。
    pub batch_limit: u32,
    /// 单轮时间预算（毫秒）；有效范围 1..=3_600_000，超出即停止本轮，未处理候选
    /// 留给下一轮。
    pub round_time_budget_ms: i64,
    /// 是否要求"归档收据在手才可删除源行"；开启后无收据零删除。
    pub archive_required: bool,
    /// 是否清理死信；默认关闭。开启必须同时给出 `dead_min_age_ms` 与 `dead_approval`，
    /// 且死信删除始终要求归档收据（不受 `archive_required` 影响）。
    pub delete_dead: bool,
    /// 死信最小保留期（毫秒）；`delete_dead` 时必填且为正。
    pub dead_min_age_ms: Option<i64>,
    /// 死信清理的独立批准标识（工单/审批号）；`delete_dead` 时必填非空。
    pub dead_approval: Option<String>,
}

impl OutboxRetentionPolicy {
    /// 业务作用：Ready 前校验策略值自洽；不合理配置直接拒绝启动，不做"自动修正"。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部值有界自洽返回 `Ok`；否则返回稳定的拒绝原因文本。
    pub fn validate(&self) -> Result<(), String> {
        if self.dispatched_min_age_ms <= 0 {
            return Err("dispatched_min_age_ms must be positive".to_string());
        }
        if self.batch_limit == 0 || self.batch_limit > 10_000 {
            return Err("batch_limit must be within 1..=10000".to_string());
        }
        if !(1..=3_600_000).contains(&self.round_time_budget_ms) {
            return Err("round_time_budget_ms must be within 1..=3600000".to_string());
        }
        if self.delete_dead {
            match self.dead_min_age_ms {
                Some(age) if age > 0 => {}
                _ => return Err("delete_dead requires a positive dead_min_age_ms".to_string()),
            }
            match self.dead_approval.as_deref() {
                // 上限与处置事实表 `outbox_dead_disposal.approval VARCHAR(128)` 对齐:
                // 校验放行而列装不下的配置会通过启动门禁、却在首批死信清理写处置
                // 事实时持续失败,属于不可执行配置,必须在这里拦下。
                Some(approval)
                    if !approval.is_empty()
                        && approval.trim() == approval
                        && approval.len() <= 128
                        && !approval.chars().any(char::is_control) => {}
                _ => {
                    return Err(
                        "delete_dead requires a non-empty bounded dead_approval identifier"
                            .to_string(),
                    )
                }
            }
        }
        Ok(())
    }
}

/// 归档端返回的可复验收据；删除源行前必须持有并可重查。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveReceipt {
    /// 归档端确认已幂等落地的事件身份。
    pub event_id: String,
}

/// 归档失败原因(脱敏;不含归档端地址/凭据/payload)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxArchiveError {
    /// 稳定脱敏原因。
    pub reason: String,
}

impl OutboxArchiveError {
    /// 业务作用：用脱敏原因构造归档错误。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for OutboxArchiveError {
    /// 业务作用：输出不含归档端身份或事件正文的稳定原因。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "outbox archive error: {}", self.reason)
    }
}

impl std::error::Error for OutboxArchiveError {}

/// 把待清理事件写入归档端的合同。
///
/// **删除的前置是收据**：`archive` 必须按 `event_id` 幂等——重复归档同一事件不产生第二份
/// 记录；回包丢失时清理执行器用 `receipt_of` 重查而不是盲目重写，也绝不在无收据时删源行。
/// 归档 payload 沿用原分类加密与授权，不进日志。
#[async_trait::async_trait]
pub trait OutboxArchive {
    /// 业务作用：按事件身份幂等写入归档端。
    ///
    /// 参数说明：
    /// - `event`：待归档的完整事件（含 payload 与 trace 元数据）。
    ///
    /// 返回：归档端确认落地后返回收据；不确定或失败返回错误（调用方停止本轮并重查）。
    async fn archive(&self, event: &OutboxEvent) -> Result<ArchiveReceipt, OutboxArchiveError>;

    /// 业务作用：重查某事件的归档收据，服务回包丢失后的确定性恢复。
    ///
    /// 参数说明：
    /// - `event_id`：事件身份。
    ///
    /// 返回：已归档返回收据；未归档返回 `None`；归档端不可达返回错误。
    async fn receipt_of(
        &self,
        event_id: &str,
    ) -> Result<Option<ArchiveReceipt>, OutboxArchiveError>;
}

// ───────────────────────────── dispatcher(轮询投递核心)─────────────────────────────

/// 投递失败原因(脱敏;不含 broker 地址/凭据/payload)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxPublishError {
    /// 稳定脱敏原因(如 "kafka publish failed")。
    pub reason: String,
    /// 失败类别:是否允许死信预算裁决。
    pub class: PublishErrorClass,
}

/// 业务作用：发布失败的封闭类别——决定 dispatcher 的死信预算是否适用。
///
/// 结果不确定(deadline、断连、回包丢失)与基础设施瞬态失败绝不允许因重投预算耗尽
/// 被标死并越过:远端可能已经提交,离开投递流等于放弃后续收敛与重查。只有携带稳定
/// 原因码的确定性拒绝才可以进入死信裁决。类别必须由发布端在构造错误时声明并一路
/// 传到存储裁决,不能在 dispatcher 侧靠字符串猜测。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishErrorClass {
    /// 确定性失败:重投永远得到同一拒绝,允许按预算进入死信集合。
    Terminal,
    /// 瞬态或结果不确定:必须保留重投,不消耗死信预算,永不自动标死。
    Transient,
}

impl OutboxPublishError {
    /// 业务作用：用脱敏原因构造确定性失败(默认类别,保持既有毒丸预算语义)。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            class: PublishErrorClass::Terminal,
        }
    }

    /// 业务作用：用脱敏原因构造瞬态/结果不确定失败——保留重投,豁免死信预算。
    pub fn transient(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            class: PublishErrorClass::Transient,
        }
    }

    /// 业务作用：判断本失败是否豁免死信预算。
    pub fn is_transient(&self) -> bool {
        self.class == PublishErrorClass::Transient
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
