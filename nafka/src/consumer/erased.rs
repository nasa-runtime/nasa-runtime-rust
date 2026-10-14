//! 类型化消费者的隐藏擦除边界；宏只负责构造公开 trait 实现。

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures::FutureExt;

use crate::consumer::ack::{AckIdentity, ManualAckSet, RecordAck};
use crate::consumer::{
    AckMode, BatchConsumer, ConsumeCtx, ExecutionMode, GroupSpec, KafkaRecord, SingleConsumer,
};
use crate::error::{NafkaError, Result};
use crate::wire::{DecodePayload, PayloadCodec};

/// owner 与执行线程共享的原始输入，克隆只增加引用，不复制 payload、key 或 headers。
#[doc(hidden)]
#[derive(Clone)]
pub struct RawRecord {
    inner: Arc<RawRecordData>,
}

/// 已物化且在派发期间冻结的原始记录内容。
#[doc(hidden)]
pub struct RawRecordData {
    /// 实际原文引用持有的责任；账本撤销本身不能归还仍被执行方使用的额度。
    input_permit: Arc<crate::consumer::capacity::RecordPermit>,
    /// 原始可空 payload；`None` 表示 tombstone。
    pub payload: Option<Vec<u8>>,
    /// 已解析的拥有型上下文。
    pub ctx: ConsumeCtx,
    /// owner 在物化记录时观察到的 assignment epoch。
    pub assignment_epoch: u64,
}

impl RawRecord {
    /// 业务作用：接管已经准入的原文字节，使裁决方与执行方共享同一份内容。
    /// 参数说明：payload 为可空正文；ctx 为来源上下文；assignment_epoch 为物化时的分配权威；input_permit 为物化前取得的输入责任。
    /// 返回：独占初始引用；派发后克隆不再增加原文字节副本。
    pub(crate) fn new(
        payload: Option<Vec<u8>>,
        ctx: ConsumeCtx,
        assignment_epoch: u64,
        input_permit: Arc<crate::consumer::capacity::RecordPermit>,
    ) -> Self {
        Self {
            inner: Arc::new(RawRecordData {
                payload,
                ctx,
                assignment_epoch,
                input_permit,
            }),
        }
    }

    /// 业务作用：将输入额度随实际调用延续到业务结束，避免主循环作废旧分配时提前释放。
    /// 参数说明：无。
    /// 返回：同一原始输入凭证的引用，不重复计费，也不增加确认权限。
    pub(crate) fn input_permit(&self) -> Arc<crate::consumer::capacity::RecordPermit> {
        Arc::clone(&self.inner.input_permit)
    }

    /// 业务作用：仅在独占原始记录时发布可信投递次数，防止更改已交给业务线程的输入。
    /// 参数说明：无。
    /// 返回：独占内容；仍有执行方引用时拒绝隐式复制或更改共同上下文。
    pub(crate) fn exclusive(&mut self) -> &mut RawRecordData {
        Arc::get_mut(&mut self.inner).expect("可信投递次数只能在派发前更新")
    }

    /// 业务作用：返回本记录保留的主要字节数，供容量 permit 计量。
    ///
    /// 参数说明：无。
    /// 返回：payload、原始 key 和有序 header 的字节总量，不把非 UTF-8 key 当作空键。
    pub fn retained_bytes(&self) -> usize {
        self.payload.as_ref().map_or(0, Vec::len)
            + self.ctx.raw_key.as_ref().map_or(0, Vec::len)
            + self
                .ctx
                .headers
                .iter()
                .map(|header| header.name.len() + header.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
    }
}

impl std::ops::Deref for RawRecord {
    type Target = RawRecordData;

    /// 业务作用：读取共享且在派发期间冻结的原文与来源上下文。
    /// 参数说明：无。
    /// 返回：绑定当前引用生命周期的只读内容，不创建字节副本。
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// 已完成 codec 解码但尚未恢复具体业务类型的记录。
#[doc(hidden)]
pub struct ErasedRecord {
    /// 由并发 owner 控制的实时确认许可，业务不能通过消费上下文重新授权。
    pub(crate) authority: Option<Arc<AtomicBool>>,
    /// 源停止先于控制邮箱执行时，仍须同步撤销手动确认。
    pub(crate) stopping: Option<Arc<AtomicBool>>,
    /// 具体业务值。
    pub value: Box<dyn Any + Send>,
    /// 消费上下文。
    pub ctx: ConsumeCtx,
    /// 与原始记录绑定的 assignment epoch。
    pub assignment_epoch: u64,
}

/// 一次 handler run 的框架结果。
#[doc(hidden)]
#[derive(Clone)]
pub enum InvocationOutcome {
    /// 本次业务未执行，指定 offset 的解码失败由 owner 按输入顺序治理。
    DecodeFailed {
        /// 非空失败集合；每个条目保留同分区真实 offset 和有界原因。
        failures: Vec<(i64, String)>,
    },
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
    /// 注册时冻结的顺序域；同组不能混合不同执行模式。
    pub execution_mode: ExecutionMode,
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
/// # 参数说明
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

/// 业务作用：在登记时读取一次单条消费者声明，防止运行中改变路由身份。
/// 参数说明：consumer 为待冻结元数据的消费者实例。
/// 返回：完整冻结声明；组身份非法时为 Config，其它非法元数据为 Registry。
fn single_meta<C: SingleConsumer>(consumer: &C) -> Result<ConsumerMeta> {
    build_meta(
        consumer.id(),
        consumer.topics(),
        consumer.event(),
        consumer.group(),
        consumer.ack_mode(),
        consumer.execution_mode(),
        ConsumerShape::Single,
        C::Message::CODEC,
        std::any::type_name::<C>(),
    )
}

/// 业务作用：在登记时读取一次批消费者声明，并拒绝无法表达批次责任的按键模式。
/// 参数说明：consumer 为待冻结元数据的消费者实例。
/// 返回：完整冻结声明；组身份非法时为 Config，其它非法元数据或按键批模式为 Registry。
fn batch_meta<C: BatchConsumer>(consumer: &C) -> Result<ConsumerMeta> {
    build_meta(
        consumer.id(),
        consumer.topics(),
        consumer.event(),
        consumer.group(),
        consumer.ack_mode(),
        consumer.execution_mode(),
        ConsumerShape::Batch,
        C::Message::CODEC,
        std::any::type_name::<C>(),
    )
}

/// 业务作用：在手写和宏声明共用的注册入口拒绝身份字段的首尾空白。
///
/// consumer id、broadcast scope 与 topic 参与身份比较或资源选择，
/// 不得通过静默裁剪改写调用方声明。event 与普通 group 另行校验各自的精确文本边界，有效身份保留原文。
///
/// 参数说明：id 为所属 consumer，value 为待校验的身份值，field 为诊断中的字段名。
/// 返回：无首尾空白时通过，否则返回注册错误，不发布消费路由。
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
/// - `execution_mode`: 同组一致的执行顺序域。
/// - `shape`: handler 的单条或批量调用形态。
/// - `codec`: 消息 codec。
/// - `source`: 来源诊断位置。
///
/// 返回：合法元数据的冻结副本；组身份非法时为 Config，其它非法声明或按键批模式为 Registry。
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
    execution_mode: ExecutionMode,
    shape: ConsumerShape,
    codec: PayloadCodec,
    source: &'static str,
) -> Result<ConsumerMeta> {
    // 批次确认无法表达不同 key 的独立失败责任，必须在开放消费路由之前拒绝。
    if execution_mode == ExecutionMode::KeyOrdered && shape == ConsumerShape::Batch {
        return Err(NafkaError::Registry(
            "KEY_ORDERED 不支持 batch consumer".into(),
        ));
    }
    if id.trim().is_empty() {
        return Err(NafkaError::Registry("consumer id 不能为空".into()));
    }
    deny_padded(id, id, "id")?;
    if crate::wire::is_blank_event(&event) {
        return Err(NafkaError::Registry(format!(
            "consumer `{id}` event 不能为空"
        )));
    }
    // event 是精确线路身份；非空白原文可含边缘空格，注册时不得裁剪或收窄发布合同。
    match &group {
        GroupSpec::Default => {}
        GroupSpec::Named(value) => crate::wire::validate_group(value)?,
        GroupSpec::Broadcast(value) => deny_padded(id, value, "broadcast scope")?,
        GroupSpec::StableBroadcast { scope, instance_id } => {
            // 共同编码独立验证精确原文，不沿用旧广播净化或裁剪规则。
            crate::broadcast_group(scope, instance_id.as_deref().unwrap_or("pending"))?;
        }
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
        execution_mode,
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
    ///
    /// 参数说明：records 携带已解码值、来源身份和 owner 的实时确认许可。
    /// 返回：回调期受管的业务 future；成功仍须服从 Manual Ack 和有效权威门禁。
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
                [(
                    &erased.ctx,
                    erased.assignment_epoch,
                    erased.authority.clone(),
                    erased.stopping.clone(),
                )],
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
    ///
    /// 参数说明：records 为按交付顺序排列的连续 run 及其确认许可。
    /// 返回：整个调用成功时仅确认有效连续前缀，错误或取消封存本次全部 token。
    fn invoke<'a>(&'a self, records: Vec<ErasedRecord>) -> HandlerFuture<'a> {
        Box::pin(async move {
            let total = records.len();
            let invocation_id = self.next_invocation.fetch_add(1, Ordering::Relaxed);
            let ack_set = manual_ack_set(
                self.meta.ack_mode,
                invocation_id,
                records.iter().map(|record| {
                    (
                        &record.ctx,
                        record.assignment_epoch,
                        record.authority.clone(),
                        record.stopping.clone(),
                    )
                }),
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

/// 业务作用：按业务类型解码拥有型记录，并保留 Kafka null 与普通正文的区别。
/// 参数说明：raw 为保留原始 null 身份的记录。
/// 返回：业务消息值；类型不接受 tombstone 或 codec 解码失败时返回错误。
fn decode_record<T: DecodePayload>(raw: &RawRecord) -> Result<ErasedRecord> {
    // codec 在有界业务线程中处理外部字节；panic 归为本条解码失败，由 owner 按输入顺序
    // 应用 invalid_record_policy 与连续失败保护，不能越过尚未接管的前缀改变分区状态。
    let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        match raw.payload.as_deref() {
            Some(payload) => T::decode(payload),
            None => T::decode_tombstone(),
        }
    }))
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
        authority: None,
        stopping: None,
        value: Box::new(value),
        ctx: raw.ctx.clone(),
        assignment_epoch: raw.assignment_epoch,
    })
}

/// 业务作用：按确认模式为本次调用建立受 owner 权威约束的临时确认槽。
/// 参数说明：mode 为确认模式；invocation_id 为本次调用身份；contexts 按输入顺序携带坐标、分配代次、实时权威与源停止门。
/// 返回：Manual 模式的确认集合；Auto 模式不创建确认槽，旧调用不能确认后来分配的输入。
fn manual_ack_set<'a>(
    mode: AckMode,
    invocation_id: u64,
    contexts: impl IntoIterator<
        Item = (
            &'a ConsumeCtx,
            u64,
            Option<Arc<AtomicBool>>,
            Option<Arc<AtomicBool>>,
        ),
    >,
) -> Option<ManualAckSet> {
    (mode == AckMode::Manual).then(|| {
        ManualAckSet::new(
            contexts
                .into_iter()
                .map(|(ctx, assignment_epoch, authority, stopping)| AckIdentity {
                    invocation_id,
                    assignment_epoch,
                    authority,
                    stopping,
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
