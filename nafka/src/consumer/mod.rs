//! 消费者公共契约、注册入口与内部 owner 模块树。

pub(crate) mod ack;
pub(crate) mod erased;
mod passthrough;
pub(crate) mod registry;

pub(crate) mod assign;
pub(crate) mod control;
pub(crate) mod dlt;
pub(crate) mod offset_state;
pub(crate) mod poll_task;
pub(crate) mod retry;
pub(crate) mod supervisor;

use crate::error::Result;
use crate::types::{KafkaHeaders, Tp};
use crate::wire::DecodePayload;
use crate::HEADER_TRACEPARENT;

pub use ack::AckToken;
pub use passthrough::{
    BorrowedKafkaRecord, InvalidRecordReason, KafkaHeaderRef, KafkaHeadersRef, PassthroughConsumer,
    PassthroughDeadLetterReason, PassthroughDisposition, PassthroughFailure,
    PassthroughFailureAction, PassthroughFailureReason, PassthroughFatalReason,
    PassthroughHaltReason, PassthroughRetryReason, PassthroughSkipReason,
};
pub use registry::{ConsumerRegistryBuilder, ConsumerRuntime};

use ack::RecordAck;

/// consumer route 的最终 group 解析规则。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupSpec {
    /// 使用顶层配置的默认 group id。
    Default,
    /// 使用显式 group id。
    Named(String),
    /// 为本进程实例派生独占广播 group。
    Broadcast(String),
}

/// route 级确认模式；两种模式都由 owner 统一提交连续安全水位。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AckMode {
    /// handler 只有返回成功才自动确认整个连续 run。
    #[default]
    Auto,
    /// handler 必须在回调期内显式确认，且最终仍需返回成功。
    Manual,
}

/// 拥有型消费上下文；可安全跨异步 handler 持有。
#[derive(Clone, Debug)]
pub struct ConsumeCtx {
    /// topic 名。
    pub topic: String,
    /// 分区号。
    pub partition: i32,
    /// 当前记录 offset。
    pub offset: i64,
    /// 当前 owner 对该 offset 的投递序号，从一开始；失败退避重投时单调增加。
    ///
    /// 该值由框架覆盖来源侧同名 header 后生成，用于实际投递审计与 DLT 证据；consumer
    /// 会话重建后可能重新从一开始，因此业务幂等仍必须依赖消息身份和 Inbox，不能依赖此计数。
    pub delivery_attempt: u32,
    /// 当前普通失败预算中的尝试序号，从一开始；`HandlerDeferred` 不推进该序号。
    ///
    /// 该值用于把外部暂停门禁与毒消息预算隔离。它与 `delivery_attempt` 一样只在当前
    /// consumer 会话内可信，不能充当跨重启幂等键。
    pub retry_attempt: u32,
    /// broker 消息时间戳毫秒；缺失时由内部约定值表达。
    pub timestamp: i64,
    /// UTF-8 key；非 UTF-8 key 在 typed 路由前处理阶段拒绝。
    pub key: Option<String>,
    /// 保序且允许同名重复的消息 headers。
    pub headers: KafkaHeaders,
    /// 从透传 header 解析得到的 JSON Map；解析失败时为空。
    pub passthrough: serde_json::Map<String, serde_json::Value>,
}

impl ConsumeCtx {
    /// 业务作用：返回当前记录的 topic-partition 自有值。
    pub fn topic_partition(&self) -> Tp {
        Tp::new(self.topic.clone(), self.partition)
    }

    /// 业务作用：解析 producer 写入的 W3C trace context；缺失、非 UTF-8 或非法值返回 `None`。
    pub fn trace_context(&self) -> Option<natelemetry::TraceContext> {
        self.headers
            .last(HEADER_TRACEPARENT)
            .and_then(|header| header.value.as_deref())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(natelemetry::TraceContext::parse_traceparent)
    }
}

/// 类型化拥有型消费记录。
pub struct KafkaRecord<T> {
    /// 已解码业务值。
    pub value: T,
    /// 消费位置、key、header 与透传上下文。
    pub ctx: ConsumeCtx,
    /// 私有确认能力，业务不能替换或伪造。
    ack: RecordAck,
}

impl<T> KafkaRecord<T> {
    /// 业务作用：由内部 adapter 构造带确认模式的记录。
    ///
    /// # 参数
    ///
    /// - `value`: 已解码业务值。
    /// - `ctx`: 拥有型消费上下文。
    /// - `ack`: 本次回调的确认能力。
    pub(crate) fn new(value: T, ctx: ConsumeCtx, ack: RecordAck) -> Self {
        Self { value, ctx, ack }
    }

    /// 业务作用：在手动模式下同步记录本地确认。
    ///
    /// # 错误
    ///
    /// 自动模式返回确认模式错误；回调结束后返回确认过期。
    pub fn ack(&self) -> Result<()> {
        self.ack.acknowledge()
    }

    /// 业务作用：拆出业务值与上下文，不携带手动确认能力。
    pub fn into_parts(self) -> (T, ConsumeCtx) {
        (self.value, self.ctx)
    }

    /// 业务作用：拆出业务值、上下文和仍受回调期约束的手动 token。
    ///
    /// # 错误
    ///
    /// 自动模式没有手动 token，返回确认模式错误。
    pub fn into_manual_parts(self) -> Result<(T, ConsumeCtx, AckToken)> {
        Ok((self.value, self.ctx, self.ack.into_token()?))
    }

    /// 业务作用：返回当前记录的 topic-partition 自有值。
    pub fn topic_partition(&self) -> Tp {
        self.ctx.topic_partition()
    }
}

/// 为手动批消费提供连续全确认便利方法。
pub trait AckBatchExt {
    /// 业务作用：按输入顺序确认全部记录；遇到首个错误立即停止。
    ///
    /// # 错误
    ///
    /// route 为自动模式或任一 token 已过期时返回错误。
    fn ack_all(&self) -> Result<()>;
}

impl<T> AckBatchExt for [KafkaRecord<T>] {
    /// 业务作用：顺序确认 slice 中全部记录，保持首错即停语义。
    fn ack_all(&self) -> Result<()> {
        self.iter().try_for_each(KafkaRecord::ack)
    }
}

/// 单条类型化消费者契约。
pub trait SingleConsumer: Send + Sync + 'static {
    /// 业务消息类型及其固定 codec。
    type Message: DecodePayload;

    /// 业务作用：返回订阅 topic 列表；注册时读取一次并冻结。
    fn topics(&self) -> Vec<String>;

    /// 业务作用：返回路由事件名；注册时读取一次并冻结。
    fn event(&self) -> String;

    /// 业务作用：返回 group 解析规则。
    fn group(&self) -> GroupSpec {
        GroupSpec::Default
    }

    /// 业务作用：返回稳定 handler id。
    fn id(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 业务作用：返回 route 级确认模式。
    fn ack_mode(&self) -> AckMode {
        AckMode::Auto
    }

    /// 业务作用：处理单条记录。
    ///
    /// # 参数
    ///
    /// - `record`: 已解码记录及其回调期确认能力。
    ///
    /// # 错误
    ///
    /// 返回错误时本次调用中的任何临时确认全部作废，自动模式也绝不确认。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()>;
}

/// 连续 run 类型化批消费者契约。
pub trait BatchConsumer: Send + Sync + 'static {
    /// 业务消息类型及其固定 codec。
    type Message: DecodePayload;

    /// 业务作用：返回订阅 topic 列表；注册时读取一次并冻结。
    fn topics(&self) -> Vec<String>;

    /// 业务作用：返回路由事件名；注册时读取一次并冻结。
    fn event(&self) -> String;

    /// 业务作用：返回 group 解析规则。
    fn group(&self) -> GroupSpec {
        GroupSpec::Default
    }

    /// 业务作用：返回稳定 handler id。
    fn id(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 业务作用：返回 route 级确认模式。
    fn ack_mode(&self) -> AckMode {
        AckMode::Auto
    }

    /// 业务作用：处理同一 route 的连续记录 run。
    ///
    /// # 参数
    ///
    /// - `records`: 已解码且按分区 offset 连续排列的记录。
    ///
    /// # 错误
    ///
    /// 返回错误时整次调用的临时确认作废，自动模式也绝不确认。
    async fn consume_batch(&self, records: Vec<KafkaRecord<Self::Message>>) -> Result<()>;
}
