//! 同步借用式消费入口，用于 payload/header 少拷贝转发链路。

use std::fmt::Display;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::consumer::ack::RecordAck;
use crate::consumer::{AckMode, GroupSpec};
use crate::error::Result;

/// 借用 Kafka header 视图；值保持 null 与空 slice 的区别。
#[derive(Clone, Copy, Debug)]
pub struct KafkaHeaderRef<'a> {
    /// header 名。
    pub name: &'a str,
    /// header 值；`None` 为协议 null，`Some(&[])` 为空字节串。
    pub value: Option<&'a [u8]>,
}

/// 有序多值借用 header 视图。
///
/// 内部表示不属于公共 ABI，可替换为直接游标而不改变业务调用方式。
pub struct KafkaHeadersRef<'a> {
    /// 当前消息按 wire 顺序排列的 header 描述符。
    inner: &'a [KafkaHeaderRef<'a>],
}

impl<'a> KafkaHeadersRef<'a> {
    /// 业务作用：从调用方持有的有序 header slice 构造只读视图。
    ///
    /// 该方法用于协议适配器和自定义同步组合；构造不会复制 header 名和值。
    ///
    /// 参数说明：
    ///
    /// - `inner`: 与返回视图同生命周期的 header 描述符。
    ///
    /// 返回：借用原描述符切片的视图，保留顺序、重复项以及 null/空值差异；不校验字段语义。
    pub fn from_slice(inner: &'a [KafkaHeaderRef<'a>]) -> Self {
        Self {
            inner,
        }
    }

    /// 业务作用：从内部借用 header 描述符构造视图。
    ///
    /// 参数说明：
    ///
    /// - `inner`: 与当前消息同生命周期的有序 header 描述符。
    ///
    /// 返回：与原始消息共享生命周期的只读头视图，不复制头名和值或取得消息所有权。
    #[allow(dead_code)]
    pub(crate) fn new(inner: &'a [KafkaHeaderRef<'a>]) -> Self {
        Self::from_slice(inner)
    }

    /// 业务作用：按 wire 顺序遍历全部 header。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含同名重复项的迭代器，仅复制借用描述符，不复制名称和值字节。
    pub fn iter(&self) -> impl Iterator<Item = KafkaHeaderRef<'a>> + '_ {
        self.inner.iter().copied()
    }

    /// 业务作用：返回指定名字最后一个 header。
    ///
    /// 参数说明：
    ///
    /// - `name`: 大小写敏感 header 名。
    ///
    /// 返回：最后一个精确同名头的借用描述符，缺失时为 None；找到 null 值仍返回 Some，不验证重复项一致性。
    pub fn last(&self, name: &str) -> Option<KafkaHeaderRef<'a>> {
        self.inner
            .iter()
            .rev()
            .find(|header| header.name == name)
            .copied()
    }

    /// 业务作用：按 wire 顺序遍历指定名字的全部 header。
    ///
    /// 参数说明：
    ///
    /// - `name`: 大小写敏感 header 名。
    ///
    /// 返回：保留原始顺序和 null 值的同名头迭代器，缺失时为空；不去重或合并多个值。
    pub fn all<'b>(&'b self, name: &'b str) -> impl Iterator<Item = KafkaHeaderRef<'a>> + 'b {
        self.inner
            .iter()
            .filter(move |header| header.name == name)
            .copied()
    }

    /// 业务作用：返回 header 总条数，包含同名重复项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原始描述符数量，null 值也占一条；不按唯一头名或字节数计量。
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 业务作用：判断消息是否没有 header。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：描述符切片为空时为 true；存在头但其值为空或 null 时仍为 false。
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// owner callback 栈内有效的借用消息。
///
/// 本类型显式为 `!Send + !Sync`，payload、key 与 headers 不能逃逸到线程、任务或 channel。
///
/// ```compile_fail
/// use nafka::BorrowedKafkaRecord;
///
/// fn move_to_thread(record: BorrowedKafkaRecord<'_>) {
///     std::thread::spawn(move || drop(record));
/// }
/// ```
pub struct BorrowedKafkaRecord<'a> {
    /// topic 名。
    pub topic: &'a str,
    /// 分区号。
    pub partition: i32,
    /// 当前记录 offset。
    pub offset: i64,
    /// broker 消息时间戳毫秒。
    pub timestamp: i64,
    /// 原始可空 key 字节。
    pub key: Option<&'a [u8]>,
    /// 有序多值借用 headers。
    pub headers: KafkaHeadersRef<'a>,
    /// 原始可空 payload；空 slice 与 tombstone 不同。
    pub payload: Option<&'a [u8]>,
    /// 与 typed 路径共用的确认能力。
    ack: RecordAck,
    /// 禁止跨线程移动和共享的零大小标记。
    _not_send_sync: PhantomData<Rc<()>>,
}

impl<'a> BorrowedKafkaRecord<'a> {
    /// 业务作用：在借用调用内解析唯一规范trace，返回的固定身份不持有原文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：缺失或非法观测为空，不改变业务处置结果。
    pub fn trace_context(&self) -> Option<natelemetry::TraceContext> {
        crate::trace::extract_fields(self.headers.iter().map(|h| (h.name, h.value)))
            .ok()
            .flatten()
    }
    /// 业务作用：由 owner callback 构造借用消息。
    ///
    /// 参数说明：
    ///
    /// - `topic`: topic 名。
    /// - `partition`: 分区号。
    /// - `offset`: 当前 offset。
    /// - `timestamp`: broker 时间戳毫秒。
    /// - `key`: 原始可空 key。
    /// - `headers`: 借用 header 视图。
    /// - `payload`: 原始可空 payload。
    /// - `ack`: 当前 route 确认能力。
    ///
    /// 返回：限定于当前借用生命周期且不可跨线程的记录，正文、key 和头保持原引用；构造不解码或确认消息。
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub(crate) fn new(
        topic: &'a str,
        partition: i32,
        offset: i64,
        timestamp: i64,
        key: Option<&'a [u8]>,
        headers: KafkaHeadersRef<'a>,
        payload: Option<&'a [u8]>,
        ack: RecordAck,
    ) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp,
            key,
            headers,
            payload,
            ack,
            _not_send_sync: PhantomData,
        }
    }

    /// 业务作用：在手动模式下同步记录本地确认。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：有效手动槽接受幂等临时确认时成功；自动模式或失效权限返回错误，成功不表示业务结果已提交至 Kafka。
    ///
    /// # 错误
    ///
    /// 自动模式返回确认模式错误；回调已结束时返回确认过期。
    pub fn ack(&self) -> Result<()> {
        self.ack.acknowledge()
    }
}

/// passthrough 未产生本地出站任务时的安全跳过原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughSkipReason {
    /// 路由在本节点没有在线目标。
    NoLocalTarget,
    /// 显式目标节点与本节点不匹配。
    TargetNodeMismatch,
    /// 消息来源会形成回环。
    Loopback,
    /// 路由来源 epoch 已过期。
    StaleSource,
}

/// passthrough 同步回调的成功结果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughDisposition {
    /// 全部目标已经成功进入本地有界 outbox。
    Handled {
        /// 成功入队的唯一 session 数。
        queued: usize,
    },
    /// 按安全规则跳过且无需重试。
    Skipped {
        /// 跳过原因。
        reason: PassthroughSkipReason,
    },
    /// 显式 best-effort 模式下允许部分丢弃。
    BestEffortDropped {
        /// 成功入队数。
        queued: usize,
        /// outbox 满导致的丢弃数。
        dropped: usize,
        /// session 已关闭导致的失败数。
        closed: usize,
    },
}

/// passthrough 失败进入框架状态机的动作分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughFailureAction {
    /// 临时失败，进入业务尝试次数与退避状态机。
    Retry,
    /// 确定坏消息，应用 invalid record 策略。
    ApplyInvalidRecordPolicy,
    /// adapter 显式要求写入 DLT。
    DeadLetter,
    /// 容量或安全不变量被破坏，立即硬暂停。
    Halt,
    /// 框架不可能状态，group 进入崩溃状态。
    Fatal,
}

/// 可重试 passthrough 失败原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughRetryReason {
    /// 本地 socket outbox 暂时无法满足入队策略。
    SocketEnqueue,
    /// 临时依赖不可用。
    TemporaryDependency,
}

/// 确定无效记录原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidRecordReason {
    /// 路由 header 缺失、重复冲突或格式非法。
    MalformedRoute,
    /// 数据面消息是 tombstone。
    MissingPayload,
    /// 消息模式不受目标协议支持。
    UnsupportedMessageMode,
}

/// 显式进入 DLT 的 passthrough 原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughDeadLetterReason {
    /// 路由展开目标超过配置上限。
    RouteExpansionTooLarge,
}

/// 立即硬暂停的 passthrough 原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughHaltReason {
    /// 路由展开目标超过配置上限。
    RouteExpansionTooLarge,
    /// 最终 socket frame 超过目标上限。
    FrameTooLarge,
}

/// group 进入崩溃状态的 passthrough 原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughFatalReason {
    /// adapter 或框架内部不变量被破坏。
    InternalInvariant,
}

/// passthrough 失败的强类型原因联合。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassthroughFailureReason {
    /// 临时失败。
    Retryable(PassthroughRetryReason),
    /// 确定无效记录。
    Invalid(InvalidRecordReason),
    /// 显式 DLT。
    ExplicitDeadLetter(PassthroughDeadLetterReason),
    /// 立即硬暂停。
    Halt(PassthroughHaltReason),
    /// 框架致命错误。
    Fatal(PassthroughFatalReason),
}

/// passthrough 回调失败；detail 在构造时统一清洗和截断。
#[derive(Clone, Debug)]
pub struct PassthroughFailure {
    /// 强类型失败原因。
    reason: PassthroughFailureReason,
    /// 不含 payload、完整路由或凭据的安全诊断文本。
    safe_detail: Option<String>,
}

impl PassthroughFailure {
    /// 业务作用：构造临时可重试失败。
    ///
    /// 参数说明：
    ///
    /// - `reason`: 可重试原因。
    /// - `detail`：调用方已排除正文和凭据的诊断；仅替换控制字符并限制为 256 个字符，不自动脱敏。
    ///
    /// 返回：动作固定为 Retry 的失败值；这里只构造原因，不调度重试或延长消费期限。
    pub fn retryable(reason: PassthroughRetryReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Retryable(reason), detail)
    }

    /// 业务作用：构造确定无效记录失败。
    ///
    /// 参数说明：
    ///
    /// - `reason`: 无效记录原因。
    /// - `detail`：调用方保证不含敏感内容的诊断，控制字符及长度由构造入口约束。
    ///
    /// 返回：请求应用 invalid_record_policy 的失败值；不会在此选择跳过、死信或暂停，也不执行对应副作用。
    pub fn invalid(reason: InvalidRecordReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Invalid(reason), detail)
    }

    /// 业务作用：构造显式 DLT 失败。
    ///
    /// 参数说明：
    ///
    /// - `reason`: DLT 原因。
    /// - `detail`：调用方已脱敏的诊断，构造入口仅控制文本格式和长度。
    ///
    /// 返回：动作固定为 DeadLetter 的失败值；死信准入与实际交付由 owner 后续执行，不代表失败记录已经保存。
    pub fn dead_letter(reason: PassthroughDeadLetterReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::ExplicitDeadLetter(reason), detail)
    }

    /// 业务作用：构造硬暂停失败。
    ///
    /// 参数说明：
    ///
    /// - `reason`: 硬暂停原因。
    /// - `detail`：不含业务正文或凭据的诊断，由调用方保证内容安全。
    ///
    /// 返回：请求硬暂停的失败值及受长度约束的说明；此函数不直接改变分区状态或推进提交。
    pub fn halt(reason: PassthroughHaltReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Halt(reason), detail)
    }

    /// 业务作用：构造框架致命失败。
    ///
    /// 参数说明：
    ///
    /// - `reason`: 致命原因。
    /// - `detail`：调用方已排除敏感内容的框架不变量诊断。
    ///
    /// 返回：动作固定为 Fatal 的失败值；不将框架不变量错误降为可重试业务失败，状态迁移由 owner 处理。
    pub fn fatal(reason: PassthroughFatalReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Fatal(reason), detail)
    }

    /// 业务作用：返回强类型失败原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的原因副本，不从诊断字符串推断动作，文本变化不会改变失败分类。
    pub fn reason(&self) -> PassthroughFailureReason {
        self.reason
    }

    /// 业务作用：从强类型原因唯一推导框架动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与原因变体一一对应的重试、无效记录策略、死信、暂停或致命动作；不解析说明文本或执行动作。
    pub fn action(&self) -> PassthroughFailureAction {
        match self.reason {
            PassthroughFailureReason::Retryable(_) => PassthroughFailureAction::Retry,
            PassthroughFailureReason::Invalid(_) => {
                PassthroughFailureAction::ApplyInvalidRecordPolicy
            }
            PassthroughFailureReason::ExplicitDeadLetter(_) => PassthroughFailureAction::DeadLetter,
            PassthroughFailureReason::Halt(_) => PassthroughFailureAction::Halt,
            PassthroughFailureReason::Fatal(_) => PassthroughFailureAction::Fatal,
        }
    }

    /// 业务作用：返回清洗后的可选诊断文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：借用替换控制字符并最多保留 256 个字符的说明；纯空白为 None，业务敏感内容必须由构造调用方排除。
    pub fn safe_detail(&self) -> Option<&str> {
        self.safe_detail.as_deref()
    }

    /// 业务作用：统一构造失败并清洗诊断文本。
    ///
    /// 参数说明：
    ///
    /// - `reason`: 强类型原因。
    /// - `detail`：调用方已排除秘密的原始诊断，其 Display 实现会先完整生成字符串。
    ///
    /// 返回：保留强类型原因并将控制字符替换为空格、取前 256 个字符的失败值；纯空白转为 None，不执行语义脱敏。
    fn new(reason: PassthroughFailureReason, detail: impl Display) -> Self {
        let detail = detail.to_string();
        let cleaned: String = detail
            .chars()
            .map(|ch| {
                if ch.is_control() {
                    ' '
                } else {
                    ch
                }
            })
            .take(256)
            .collect();
        let safe_detail = (!cleaned.trim().is_empty()).then_some(cleaned);
        Self {
            reason,
            safe_detail,
        }
    }
}

/// 同步借用式消费者契约。
pub trait PassthroughConsumer: Send + Sync + 'static {
    /// 业务作用：返回订阅 topic 列表；注册时读取一次并冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同步路由覆盖的非空精确 topic 名集合；注册入口拒绝重复及纯空白名字，其余原文不裁剪，底层名称约束仍由客户端验证。
    fn topics(&self) -> Vec<String>;

    /// 业务作用：返回路由事件名；注册时读取一次并冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与线路事件精确匹配的原文，纯空白被拒绝，合法边缘空格不裁剪；不使用正文反序列化推断路由。
    fn event(&self) -> String;

    /// 业务作用：返回 group 解析规则。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认沿用源配置中的组声明；覆盖命名或广播规则后，最终组身份仍在启动前统一校验。
    fn group(&self) -> GroupSpec {
        GroupSpec::Default
    }

    /// 业务作用：返回稳定 handler id。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认具体 Rust 类型名，供登记和冲突检查；需要不随类型路径变化的身份时可显式覆盖。
    fn id(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 业务作用：返回 route 级确认模式。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认 Auto；Manual 要求回调有效期内显式确认，任何模式都不能越过 owner 的连续水位与分配权威检查。
    fn ack_mode(&self) -> AckMode {
        AckMode::Auto
    }

    /// 业务作用：在 owner callback 栈内同步处理借用消息。
    ///
    /// 参数说明：
    ///
    /// - `record`: 不可跨线程和异步边界逃逸的借用记录。
    ///
    /// # 错误
    ///
    /// 返回强类型失败，由框架按其唯一动作进入重试、坏消息、DLT、暂停或崩溃状态。
    ///
    /// 返回：本地交付、允许的跳过或显式部分丢弃结果；失败按原因分类处理，成功不等于远端接收完成，借用内容不能逃逸回调。
    fn consume_borrowed(
        &self,
        record: BorrowedKafkaRecord<'_>,
    ) -> std::result::Result<PassthroughDisposition, PassthroughFailure>;
}
