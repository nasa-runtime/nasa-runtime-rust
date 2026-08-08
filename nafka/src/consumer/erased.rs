//! 类型化消费者的隐藏擦除边界；宏只负责构造公开 trait 实现。

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::FutureExt;

use crate::consumer::ack::{AckIdentity, ManualAckSet, RecordAck};
use crate::consumer::{AckMode, BatchConsumer, ConsumeCtx, GroupSpec, KafkaRecord, SingleConsumer};
use crate::error::{NafkaError, Result};
use crate::wire::{DecodePayload, PayloadCodec};

/// owner 从底层借用消息物化出的有界拥有型记录。
#[doc(hidden)]
pub struct RawRecord {
    /// 原始可空 payload；`None` 表示 tombstone。
    pub payload: Option<Vec<u8>>,
    /// 已解析的拥有型上下文。
    pub ctx: ConsumeCtx,
    /// owner 在物化记录时观察到的 assignment epoch。
    pub assignment_epoch: u64,
}

impl RawRecord {
    /// 业务作用：返回本记录保留的主要字节数，供容量 permit 计量。
    pub fn retained_bytes(&self) -> usize {
        self.payload.as_ref().map_or(0, Vec::len)
            + self.ctx.key.as_ref().map_or(0, String::len)
            + self
                .ctx
                .headers
                .iter()
                .map(|header| header.name.len() + header.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
    }
}

/// 已完成 codec 解码但尚未恢复具体业务类型的记录。
#[doc(hidden)]
pub struct ErasedRecord {
    /// 具体业务值。
    pub value: Box<dyn Any + Send>,
    /// 消费上下文。
    pub ctx: ConsumeCtx,
    /// 与原始记录绑定的 assignment epoch。
    pub assignment_epoch: u64,
}

/// 一次 handler run 的框架结果。
#[doc(hidden)]
pub enum InvocationOutcome {
    /// handler 返回成功；自动模式确认全部，手动模式只确认连续前缀。
    Completed {
        /// 从 run 首条开始可进入安全水位的记录数。
        acked_prefix: usize,
        /// 首个确认空洞之后仍被业务确认的记录数；这些确认不会推进安全前沿。
        acked_after_gap: usize,
        /// 本次 run 总记录数。
        total: usize,
    },
    /// handler 返回错误或发生 panic；本次调用内临时确认全部作废。
    Failed(NafkaError),
    /// 擦除边界或输入形态破坏了框架不变量，不能归因给业务消息并重试。
    Fatal(NafkaError),
}

/// owner 线程本地驱动的非 `Send` handler future。
#[doc(hidden)]
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = InvocationOutcome> + 'a>>;

/// 类型化消费者一次调用所接收的记录形态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum ConsumerShape {
    /// 每次调用严格接收一条记录。
    Single,
    /// 每次调用接收同分区、同 route 的连续交付 run。
    Batch,
}

/// 注册时冻结的消费者元数据。
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct ConsumerMeta {
    /// 稳定 handler id。
    pub id: &'static str,
    /// 规范化前即完成重复检查的 topic 列表。
    pub topics: Vec<String>,
    /// 业务事件名。
    pub event: String,
    /// 尚未解析的 group 规则。
    pub group: GroupSpec,
    /// route 级确认模式。
    pub ack_mode: AckMode,
    /// handler 的单条或批量调用形态。
    pub shape: ConsumerShape,
    /// 消息 codec。
    pub codec: PayloadCodec,
    /// 注册来源诊断位置。
    pub source: &'static str,
}

/// 运行时内部的类型擦除消费者。
#[doc(hidden)]
pub trait ErasedConsumer: Send + Sync {
    /// 业务作用：返回注册时冻结的元数据。
    fn meta(&self) -> &ConsumerMeta;

    /// 业务作用：返回消息类型固定 codec。
    fn codec(&self) -> PayloadCodec;

    /// 业务作用：逐条解码原始记录，以便精确归属失败 offset。
    ///
    /// # 参数
    ///
    /// - `raw`: 已受容量约束的拥有型原始记录。
    ///
    /// # 错误
    ///
    /// tombstone、codec 不匹配或 payload 解码失败时返回错误。
    fn decode(&self, raw: &RawRecord) -> Result<ErasedRecord>;

    /// 业务作用：恢复具体业务类型并调用 handler。
    ///
    /// # 参数
    ///
    /// - `records`: 已逐条解码的连续 run。
    fn invoke<'a>(&'a self, records: Vec<ErasedRecord>) -> HandlerFuture<'a>;
}

/// 属性宏放入分布式切片的静态注册项。
#[doc(hidden)]
pub struct CollectedConsumer {
    /// 用于稳定排序和重复检测的 handler id。
    pub id: &'static str,
    /// `file:line:column` 来源位置。
    pub source: &'static str,
    /// 目标 Kafka 客户端实例名。
    pub client: &'static str,
    /// 构造擦除消费者的无捕获函数。
    pub build: fn() -> Result<Arc<dyn ErasedConsumer>>,
}

/// 单条消费者的泛型擦除 adapter。
struct SingleAdapter<C> {
    /// 业务消费者。
    consumer: C,
    /// 冻结元数据。
    meta: ConsumerMeta,
    /// 单调递增的本进程 invocation id。
    next_invocation: AtomicU64,
}

/// 批消费者的泛型擦除 adapter。
struct BatchAdapter<C> {
    /// 业务消费者。
    consumer: C,
    /// 冻结元数据。
    meta: ConsumerMeta,
    /// 单调递增的本进程 invocation id。
    next_invocation: AtomicU64,
}

/// 业务作用：将公开单条消费者实例擦除为内部 trait object。
///
/// # 参数
///
/// - `consumer`: 已完成业务依赖注入的消费者实例。
///
/// # 错误
///
/// id、event、topic 列表非法或存在重复 topic 时返回注册错误。
#[doc(hidden)]
pub fn erase_single<C: SingleConsumer>(consumer: C) -> Result<Arc<dyn ErasedConsumer>> {
    let meta = single_meta(&consumer)?;
    Ok(Arc::new(SingleAdapter {
        consumer,
        meta,
        next_invocation: AtomicU64::new(1),
    }))
}

/// 业务作用：将公开批消费者实例擦除为内部 trait object。
///
/// # 参数
///
/// - `consumer`: 已完成业务依赖注入的消费者实例。
///
/// # 错误
///
/// id、event、topic 列表非法或存在重复 topic 时返回注册错误。
#[doc(hidden)]
pub fn erase_batch<C: BatchConsumer>(consumer: C) -> Result<Arc<dyn ErasedConsumer>> {
    let meta = batch_meta(&consumer)?;
    Ok(Arc::new(BatchAdapter {
        consumer,
        meta,
        next_invocation: AtomicU64::new(1),
    }))
}

/// 业务作用：冻结单条消费者元数据。
///
/// # 参数
///
/// - `consumer`: 待读取一次元数据的消费者。
///
/// # 错误
///
/// 元数据非法时返回注册错误。
fn single_meta<C: SingleConsumer>(consumer: &C) -> Result<ConsumerMeta> {
    build_meta(
        consumer.id(),
        consumer.topics(),
        consumer.event(),
        consumer.group(),
        consumer.ack_mode(),
        ConsumerShape::Single,
        C::Message::CODEC,
        std::any::type_name::<C>(),
    )
}

/// 业务作用：冻结批消费者元数据。
///
/// # 参数
///
/// - `consumer`: 待读取一次元数据的消费者。
///
/// # 错误
///
/// 元数据非法时返回注册错误。
fn batch_meta<C: BatchConsumer>(consumer: &C) -> Result<ConsumerMeta> {
    build_meta(
        consumer.id(),
        consumer.topics(),
        consumer.event(),
        consumer.group(),
        consumer.ack_mode(),
        ConsumerShape::Batch,
        C::Message::CODEC,
        std::any::type_name::<C>(),
    )
}

/// 业务作用：拒绝带首尾空白的元数据值。
///
/// 校验点必须在 `build_meta`：宏面的 `deny_padded` 只覆盖 `#[kafka_consumer]`，
/// 而 `ConsumerRegistry::register` / `register_batch` 与手写 `impl SingleConsumer`
/// 是同等的公开入口，走的正是这里。此前这两条路只做 `trim().is_empty()` 判空，
/// 却**存未 trim 的值**，于是 `event: " created "` 编译通过、运行期与 header 永不匹配，
/// `GroupSpec::Named(" g ")` 则直接换成另一个 group.id（孤儿 offset + 按 reset 重置）。
///
/// 刻意报错而非静默 trim：静默 trim 会与用户写下的意图不符，且掩盖上游的拼接 bug。
///
/// # 参数
///
/// - `id`: 所属 consumer id，用于定位。
/// - `value`: 待校验的值。
/// - `field`: 出错时提示的字段名。
///
/// # 错误
///
/// 值带首尾空白时返回注册错误。
fn deny_padded(id: &str, value: &str, field: &str) -> Result<()> {
    if value != value.trim() {
        return Err(NafkaError::Registry(format!(
            "consumer `{id}` {field} 不能带首尾空白：该值会被原样用于匹配或拼接 group.id，\
             带空白会导致静默不生效"
        )));
    }
    Ok(())
}

/// 业务作用：校验并构造通用消费者元数据。
///
/// # 参数
///
/// - `id`: 稳定 handler id。
/// - `topics`: topic 列表。
/// - `event`: 事件名。
/// - `group`: group 规则。
/// - `ack_mode`: route 确认模式。
/// - `shape`: handler 的单条或批量调用形态。
/// - `codec`: 消息 codec。
/// - `source`: 来源诊断位置。
///
/// # 错误
///
/// 任一字符串为空或 topic 重复时返回注册错误。
#[allow(clippy::too_many_arguments)]
fn build_meta(
    id: &'static str,
    topics: Vec<String>,
    event: String,
    group: GroupSpec,
    ack_mode: AckMode,
    shape: ConsumerShape,
    codec: PayloadCodec,
    source: &'static str,
) -> Result<ConsumerMeta> {
    if id.trim().is_empty() {
        return Err(NafkaError::Registry("consumer id 不能为空".into()));
    }
    deny_padded(id, id, "id")?;
    if event.trim().is_empty() {
        return Err(NafkaError::Registry(format!(
            "consumer `{id}` event 不能为空"
        )));
    }
    deny_padded(id, &event, "event")?;
    match &group {
        GroupSpec::Default => {}
        GroupSpec::Named(value) => deny_padded(id, value, "group")?,
        GroupSpec::Broadcast(value) => deny_padded(id, value, "broadcast scope")?,
    }
    if topics.is_empty() {
        return Err(NafkaError::Registry(format!(
            "consumer `{id}` topics 不能为空"
        )));
    }
    let mut sorted = topics;
    if sorted.iter().any(|topic| topic.trim().is_empty()) {
        return Err(NafkaError::Registry(format!(
            "consumer `{id}` topic 不能为空"
        )));
    }
    for topic in &sorted {
        deny_padded(id, topic, "topic")?;
    }
    sorted.sort();
    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(NafkaError::Registry(format!(
            "consumer `{id}` topics 存在重复项"
        )));
    }
    Ok(ConsumerMeta {
        id,
        topics: sorted,
        event,
        group,
        ack_mode,
        shape,
        codec,
        source,
    })
}

impl<C: SingleConsumer> ErasedConsumer for SingleAdapter<C> {
    /// 业务作用：返回单条消费者冻结元数据。
    fn meta(&self) -> &ConsumerMeta {
        &self.meta
    }

    /// 业务作用：返回单条消息类型固定 codec。
    fn codec(&self) -> PayloadCodec {
        self.meta.codec
    }

    /// 业务作用：解码一条拥有型原始记录。
    ///
    /// - `raw`: 已受字节 permit 约束的记录。
    fn decode(&self, raw: &RawRecord) -> Result<ErasedRecord> {
        decode_record::<C::Message>(raw)
    }

    /// 业务作用：调用一次且只含一条记录的单条 handler run。
    ///
    /// - `records`: dispatcher 提供的单记录向量。
    fn invoke<'a>(&'a self, records: Vec<ErasedRecord>) -> HandlerFuture<'a> {
        Box::pin(async move {
            if records.len() != 1 {
                return InvocationOutcome::Fatal(NafkaError::Lifecycle(format!(
                    "single adapter 收到非单条 run: {}",
                    records.len()
                )));
            }
            let invocation_id = self.next_invocation.fetch_add(1, Ordering::Relaxed);
            let erased = records.into_iter().next().expect("长度已检查");
            let ack_set = manual_ack_set(
                self.meta.ack_mode,
                invocation_id,
                [(&erased.ctx, erased.assignment_epoch)],
            );
            let ack = record_ack(self.meta.ack_mode, ack_set.as_ref(), 0);
            let value = match erased.value.downcast::<C::Message>() {
                Ok(value) => *value,
                Err(_) => {
                    seal_failed(ack_set.as_ref());
                    return InvocationOutcome::Fatal(NafkaError::Lifecycle(
                        "single adapter 类型恢复失败".into(),
                    ));
                }
            };
            let result = AssertUnwindSafe(
                self.consumer
                    .consume(KafkaRecord::new(value, erased.ctx, ack)),
            )
            .catch_unwind()
            .await;
            finish_invocation(result, ack_set.as_ref(), 1, self.meta.ack_mode)
        })
    }
}

impl<C: BatchConsumer> ErasedConsumer for BatchAdapter<C> {
    /// 业务作用：返回批消费者冻结元数据。
    fn meta(&self) -> &ConsumerMeta {
        &self.meta
    }

    /// 业务作用：返回批消息类型固定 codec。
    fn codec(&self) -> PayloadCodec {
        self.meta.codec
    }

    /// 业务作用：逐条解码拥有型原始记录。
    ///
    /// - `raw`: 已受字节 permit 约束的记录。
    fn decode(&self, raw: &RawRecord) -> Result<ErasedRecord> {
        decode_record::<C::Message>(raw)
    }

    /// 业务作用：恢复连续 run 的业务类型并调用一次批 handler。
    ///
    /// - `records`: 同 route、同分区且 offset 连续的记录。
    fn invoke<'a>(&'a self, records: Vec<ErasedRecord>) -> HandlerFuture<'a> {
        Box::pin(async move {
            let total = records.len();
            let invocation_id = self.next_invocation.fetch_add(1, Ordering::Relaxed);
            let ack_set = manual_ack_set(
                self.meta.ack_mode,
                invocation_id,
                records
                    .iter()
                    .map(|record| (&record.ctx, record.assignment_epoch)),
            );
            let mut typed = Vec::with_capacity(total);
            for (index, erased) in records.into_iter().enumerate() {
                let value = match erased.value.downcast::<C::Message>() {
                    Ok(value) => *value,
                    Err(_) => {
                        seal_failed(ack_set.as_ref());
                        return InvocationOutcome::Fatal(NafkaError::Lifecycle(
                            "batch adapter 类型恢复失败".into(),
                        ));
                    }
                };
                typed.push(KafkaRecord::new(
                    value,
                    erased.ctx,
                    record_ack(self.meta.ack_mode, ack_set.as_ref(), index),
                ));
            }
            let result = AssertUnwindSafe(self.consumer.consume_batch(typed))
                .catch_unwind()
                .await;
            finish_invocation(result, ack_set.as_ref(), total, self.meta.ack_mode)
        })
    }
}

/// 业务作用：将原始 payload 解码为具体消息类型。
///
/// # 参数
///
/// - `raw`: 原始拥有型记录。
///
/// # 错误
///
/// tombstone 或 codec 解码失败时返回错误。
fn decode_record<T: DecodePayload>(raw: &RawRecord) -> Result<ErasedRecord> {
    let payload = raw.payload.as_deref().ok_or(NafkaError::MissingPayload)?;
    // 解码跑在 owner 线程上，且在每次调用的 catch_unwind **之外**：这里 panic 会一路穿到
    // 会话级 catch_unwind 把 group 打成 Crashed，而 offset 从未推进 —— operator restart 后
    // 同一条记录再次 panic，形成永久毒消息循环。
    //
    // handler 的 panic 早已被接住并按重试/DLT 处置；解码同样是在处理外部字节，
    // 必须享有同等待遇：panic 归一成"确定无效记录"，交给 invalid_record_policy。
    // 触发面既包括业务自定义解码器的缺陷，也包括第三方 codec 对畸形 payload 的越界访问。
    let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| T::decode(payload)))
        .map_err(|_payload| {
            // **刻意丢弃 panic payload 文本**：该错误会进 `GroupHealth.last_error`(公开 API,
            // 不截断)与 DLT reason header。而解码器常写成 `from_slice::<T>(p).unwrap()`,
            // serde 的 panic 文本里**嵌着出错的输入片段**(`invalid type: string "sk-live-…"`),
            // 转交原始错误会泄露凭据；napp/src/panic_hook.rs 同样拒绝读取 payload。
            // 定位靠固定字面量 + 具体消息类型名(编译期常量,不含任何记录内容)。
            NafkaError::Codec(format!(
                "payload 解码 panic(内容已抑制,类型 {})",
                std::any::type_name::<T>()
            ))
        })?;
    let value = decoded?;
    Ok(ErasedRecord {
        value: Box::new(value),
        ctx: raw.ctx.clone(),
        assignment_epoch: raw.assignment_epoch,
    })
}

/// 业务作用：按确认模式创建本次 run 的手动确认槽。
///
/// # 参数
///
/// - `mode`: route 确认模式。
/// - `invocation_id`: 本次 handler 调用 id。
/// - `contexts`: 与输入顺序一致的记录上下文和 assignment epoch。
fn manual_ack_set<'a>(
    mode: AckMode,
    invocation_id: u64,
    contexts: impl IntoIterator<Item = (&'a ConsumeCtx, u64)>,
) -> Option<ManualAckSet> {
    (mode == AckMode::Manual).then(|| {
        ManualAckSet::new(
            contexts
                .into_iter()
                .map(|(ctx, assignment_epoch)| AckIdentity {
                    invocation_id,
                    assignment_epoch,
                    topic: ctx.topic.clone(),
                    partition: ctx.partition,
                    offset: ctx.offset,
                })
                .collect(),
        )
    })
}

/// 业务作用：为指定记录生成确认能力。
///
/// # 参数
///
/// - `mode`: route 确认模式。
/// - `set`: 手动模式的确认槽集合。
/// - `index`: 输入下标。
fn record_ack(mode: AckMode, set: Option<&ManualAckSet>, index: usize) -> RecordAck {
    match mode {
        AckMode::Auto => RecordAck::Auto,
        AckMode::Manual => set.expect("手动模式必须创建确认槽").record_ack(index),
    }
}

/// 业务作用：失败路径封存全部 token。
///
/// # 参数
///
/// - `set`: 可选手动确认集合。
fn seal_failed(set: Option<&ManualAckSet>) {
    if let Some(set) = set {
        let _ = set.finish(false);
    }
}

/// 业务作用：把 handler future 结果转换为严格确认矩阵。
///
/// # 参数
///
/// - `result`: 捕获 panic 后的 handler 结果。
/// - `set`: 手动确认集合。
/// - `total`: 本次 run 总记录数。
/// - `mode`: route 确认模式。
fn finish_invocation(
    result: std::result::Result<Result<()>, Box<dyn Any + Send>>,
    set: Option<&ManualAckSet>,
    total: usize,
    mode: AckMode,
) -> InvocationOutcome {
    match result {
        Ok(Ok(())) => {
            let (acked_prefix, acked_after_gap) = match mode {
                AckMode::Auto => (total, 0),
                AckMode::Manual => {
                    let summary = set.expect("手动模式必须创建确认槽").finish(true);
                    (summary.prefix, summary.acked.saturating_sub(summary.prefix))
                }
            };
            InvocationOutcome::Completed {
                acked_prefix,
                acked_after_gap,
                total,
            }
        }
        Ok(Err(error)) => {
            seal_failed(set);
            InvocationOutcome::Failed(error)
        }
        Err(payload) => {
            seal_failed(set);
            InvocationOutcome::Failed(NafkaError::HandlerPanic(panic_message(payload)))
        }
    }
}

/// 业务作用：将 panic payload 转为不暴露业务数据的简短文本。
///
/// # 参数
///
/// - `payload`: `catch_unwind` 捕获的 payload。
fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "非字符串 panic payload".into()
    }
}
