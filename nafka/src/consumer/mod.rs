//! 消费者公共契约、注册入口与内部 owner 模块树。

pub(crate) mod ack;
pub(crate) mod erased;
mod passthrough;
pub(crate) mod registry;

pub(crate) mod assign;
pub(crate) mod capacity;
pub(crate) mod control;
pub(crate) mod dlt;
pub(crate) mod group;
pub(crate) mod metadata_capacity;
pub(crate) mod offset_state;
pub(crate) mod poll_task;
mod retained_payload;
pub(crate) mod retry;
pub(crate) mod source_capacity;
pub(crate) mod supervisor;
pub(crate) mod worker;

use crate::error::Result;
use crate::types::{KafkaHeaders, Tp};
use crate::wire::DecodePayload;

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
    /// 使用原文 group id；须非纯空白、无 NUL 且为1至1024个 UTF-8 字节，首尾不含 U+0000–0020，有效原文不裁剪。
    Named(String),
    /// 为本进程实例派生独占广播 group。
    Broadcast(String),
    /// 使用共同身份编码派生广播组；既有 Broadcast 保留原组算法，迁移必须显式选择。
    StableBroadcast {
        /// 明确业务作用域，不使用 handler 路径。
        scope: String,
        /// 稳定实例身份；None 沿用 behavior.broadcast_instance_id、环境变量或进程临时身份。
        instance_id: Option<String>,
    },
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

/// 消费执行的顺序域；提交始终受分区连续安全前缀约束。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    /// 分区内按实际交付顺序串行执行，支持单条和连续批次。
    #[default]
    PartitionOrdered,
    /// 同分区的不同 key 可并发，同 key 串行；null key 是分区屏障，仅支持单条。
    KeyOrdered,
}

/// 拥有型消费上下文；可安全跨异步 handler 持有。
#[derive(Clone, Debug)]
pub struct ConsumeCtx {
    /// 原始 Kafka value 是否为 null；与解码得到的 JSON null 或空字节明确区分。
    pub tombstone: bool,
    /// 稳定逻辑源名，对应 KafkaConfig.client_name，不代表物理 cluster ID。
    pub source_id: String,
    /// 实际消费组，默认组和广播组均已经解析。
    pub group: String,
    /// 本次派发的分区权威代次；旧代次不能确认新分配。
    pub assignment_epoch: u64,
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
    /// UTF-8 key 便利视图；null 或非 UTF-8 key 返回 None，顺序域以 raw_key 为准。
    pub key: Option<String>,
    /// 原始可空 key 字节，保留非 UTF-8 key 与 null/空字节的差异。
    pub raw_key: Option<Vec<u8>>,
    /// 保序且允许同名重复的消息 headers。
    pub headers: KafkaHeaders,
    /// 从透传 header 解析得到的 JSON Map；解析失败时为空。
    pub passthrough: serde_json::Map<String, serde_json::Value>,
}

impl ConsumeCtx {
    /// 业务作用：返回当前记录的 topic-partition 自有值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：复制 topic 名的分区坐标，不包含 offset 或分配权威，不能单独作为确认凭证。
    pub fn topic_partition(&self) -> Tp {
        Tp::new(self.topic.clone(), self.partition)
    }

    /// 业务作用：解析唯一规范的W3C上下文，观测头错误不会使合法业务消息失效。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：缺失、重复、非UTF-8或非法值均为None；有效值不作为授权证据。
    pub fn trace_context(&self) -> Option<natelemetry::TraceContext> {
        crate::extract_trace_context(&self.headers).ok().flatten()
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
    /// 参数说明：
    ///
    /// - `value`: 已解码业务值。
    /// - `ctx`: 拥有型消费上下文。
    /// - `ack`: 本次回调的确认能力。
    ///
    /// 返回：接管业务值、上下文及确认能力的记录；不解码、复制正文或执行确认。
    pub(crate) fn new(value: T, ctx: ConsumeCtx, ack: RecordAck) -> Self {
        Self {
            value,
            ctx,
            ack,
        }
    }

    /// 业务作用：在手动模式下同步记录本地确认。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前槽可确认且权威有效时幂等记录临时确认；模式不符或已失效时返回错误，成功仍依赖回调最终成功及连续水位。
    ///
    /// # 错误
    ///
    /// 自动模式返回确认模式错误；回调结束后返回确认过期。
    pub fn ack(&self) -> Result<()> {
        self.ack.acknowledge()
    }

    /// 业务作用：拆出业务值与上下文，不携带手动确认能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原值与上下文的所有权，确认句柄随记录拆分而释放；本操作不会代替业务确认。
    pub fn into_parts(self) -> (T, ConsumeCtx) {
        (self.value, self.ctx)
    }

    /// 业务作用：拆出业务值、上下文和仍受回调期约束的手动 token。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：手动模式转移三者所有权，不延长 token 有效期；自动模式返回模式错误并释放已消耗的记录。
    ///
    /// # 错误
    ///
    /// 自动模式没有手动 token，返回确认模式错误。
    pub fn into_manual_parts(self) -> Result<(T, ConsumeCtx, AckToken)> {
        Ok((self.value, self.ctx, self.ack.into_token()?))
    }

    /// 业务作用：返回当前记录的 topic-partition 自有值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：由上下文复制的分区坐标；不消费业务值或转移确认能力。
    pub fn topic_partition(&self) -> Tp {
        self.ctx.topic_partition()
    }
}

/// 为手动批消费提供连续全确认便利方法。
pub trait AckBatchExt {
    /// 业务作用：按输入顺序确认全部记录；遇到首个错误立即停止。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部本地确认被接受时成功；首错之前的临时确认不回滚，整次回调若返回错误则由框架统一作废。
    ///
    /// # 错误
    ///
    /// route 为自动模式或任一 token 已过期时返回错误。
    fn ack_all(&self) -> Result<()>;
}

impl<T> AckBatchExt for [KafkaRecord<T>] {
    /// 业务作用：顺序确认 slice 中全部记录，保持首错即停语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空切片或全部记录确认成功时完成；遇错保留此前临时确认并停止后继确认，不直接推进 broker 提交。
    fn ack_all(&self) -> Result<()> {
        self.iter().try_for_each(KafkaRecord::ack)
    }
}

/// 单条类型化消费者契约。
pub trait SingleConsumer: Send + Sync + 'static {
    /// 业务消息类型及其固定 codec。
    type Message: DecodePayload;

    /// 业务作用：返回订阅 topic 列表；注册时读取一次并冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：非空且无重复的精确 topic 名集合；注册入口拒绝空名及首尾空白，不静默裁剪。
    fn topics(&self) -> Vec<String>;

    /// 业务作用：返回路由事件名；注册时读取一次并冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：参与精确匹配的原始事件名；注册入口拒绝纯空白，合法名字不因边缘空格被改写。
    fn event(&self) -> String;

    /// 业务作用：返回 group 解析规则。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认引用源配置中的 group；命名或广播规则可由实现覆盖，最终身份在启动前解析并校验。
    fn group(&self) -> GroupSpec {
        GroupSpec::Default
    }

    /// 业务作用：返回稳定 handler id。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认使用具体 Rust 类型名，供注册排序和重复检测；需要跨类型重命名保持身份时应显式覆盖。
    fn id(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 业务作用：返回 route 级确认模式。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认 Auto，仅回调成功才确认；覆盖为 Manual 后还须在回调有效期内调用记录的确认接口。
    fn ack_mode(&self) -> AckMode {
        AckMode::Auto
    }

    /// 业务作用：冻结单条路由的执行顺序域，同组所有路由必须一致。
    ///
    /// 参数说明：无。
    ///
    /// 返回：默认分区串行；按键并发须显式返回 `ExecutionMode::KeyOrdered`。
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::PartitionOrdered
    }

    /// 业务作用：处理单条记录。
    ///
    /// 参数说明：
    ///
    /// - `record`: 已解码记录及其回调期确认能力。
    ///
    /// # 错误
    ///
    /// 返回错误时本次调用中的任何临时确认全部作废，自动模式也绝不确认。
    ///
    /// 返回：成功表示业务处理完成，框架再结合确认模式与当前权威决定可提交前缀；错误进入失败策略，不代替业务副作用回滚。
    async fn consume(&self, record: KafkaRecord<Self::Message>) -> Result<()>;
}

/// 连续 run 类型化批消费者契约。
pub trait BatchConsumer: Send + Sync + 'static {
    /// 业务消息类型及其固定 codec。
    type Message: DecodePayload;

    /// 业务作用：返回订阅 topic 列表；注册时读取一次并冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：批路由覆盖的非空 topic 名集合；注册入口拒绝重复、空名及首尾空白，冻结后不随调用变更。
    fn topics(&self) -> Vec<String>;

    /// 业务作用：返回路由事件名；注册时读取一次并冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：批路由的精确事件身份；纯空白声明被拒绝，不允许同一次批调用混入其它路由的记录。
    fn event(&self) -> String;

    /// 业务作用：返回 group 解析规则。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认复用源配置的消费组声明；可覆盖命名或广播规则，由启动入口生成并验证最终组身份。
    fn group(&self) -> GroupSpec {
        GroupSpec::Default
    }

    /// 业务作用：返回稳定 handler id。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认具体类型名，供批消费者登记及重复检测；需要脱离 Rust 类型路径的稳定标识时显式覆盖。
    fn id(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 业务作用：返回 route 级确认模式。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认 Auto，整次回调成功才确认整批；Manual 只认可成功回调中的有效连续确认前缀。
    fn ack_mode(&self) -> AckMode {
        AckMode::Auto
    }

    /// 业务作用：声明批路由的执行顺序域，在注册时拒绝与单条按键责任不兼容的组合。
    ///
    /// 参数说明：无。
    ///
    /// 返回：默认分区串行；返回 `KeyOrdered` 会导致注册失败。
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::PartitionOrdered
    }

    /// 业务作用：处理同一 route 的连续记录 run。
    ///
    /// 参数说明：
    ///
    /// - `records`：同分区、同路由且按实际交付顺序连续的已解码记录；数值 offset 可能存在 broker 自身的空洞。
    ///
    /// # 错误
    ///
    /// 返回错误时整次调用的临时确认作废，自动模式也绝不确认。
    ///
    /// 返回：成功允许框架按确认模式计算安全前缀；失败使本次确认全部作废，已发生的外部业务副作用须由业务幂等约束处理。
    async fn consume_batch(&self, records: Vec<KafkaRecord<Self::Message>>) -> Result<()>;
}
