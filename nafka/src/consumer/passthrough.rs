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
    /// # 参数
    ///
    /// - `inner`: 与返回视图同生命周期的 header 描述符。
    pub fn from_slice(inner: &'a [KafkaHeaderRef<'a>]) -> Self {
        Self { inner }
    }

    /// 业务作用：从内部借用 header 描述符构造视图。
    ///
    /// # 参数
    ///
    /// - `inner`: 与当前消息同生命周期的有序 header 描述符。
    #[allow(dead_code)]
    pub(crate) fn new(inner: &'a [KafkaHeaderRef<'a>]) -> Self {
        Self::from_slice(inner)
    }

    /// 业务作用：按 wire 顺序遍历全部 header。
    pub fn iter(&self) -> impl Iterator<Item = KafkaHeaderRef<'a>> + '_ {
        self.inner.iter().copied()
    }

    /// 业务作用：返回指定名字最后一个 header。
    ///
    /// # 参数
    ///
    /// - `name`: 大小写敏感 header 名。
    pub fn last(&self, name: &str) -> Option<KafkaHeaderRef<'a>> {
        self.inner
            .iter()
            .rev()
            .find(|header| header.name == name)
            .copied()
    }

    /// 业务作用：按 wire 顺序遍历指定名字的全部 header。
    ///
    /// # 参数
    ///
    /// - `name`: 大小写敏感 header 名。
    pub fn all<'b>(&'b self, name: &'b str) -> impl Iterator<Item = KafkaHeaderRef<'a>> + 'b {
        self.inner
            .iter()
            .filter(move |header| header.name == name)
            .copied()
    }

    /// 业务作用：返回 header 总条数，包含同名重复项。
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 业务作用：判断消息是否没有 header。
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
    /// 业务作用：由 owner callback 构造借用消息。
    ///
    /// # 参数
    ///
    /// - `topic`: topic 名。
    /// - `partition`: 分区号。
    /// - `offset`: 当前 offset。
    /// - `timestamp`: broker 时间戳毫秒。
    /// - `key`: 原始可空 key。
    /// - `headers`: 借用 header 视图。
    /// - `payload`: 原始可空 payload。
    /// - `ack`: 当前 route 确认能力。
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
    /// # 参数
    ///
    /// - `reason`: 可重试原因。
    /// - `detail`: 将被清洗截断的安全诊断文本。
    pub fn retryable(reason: PassthroughRetryReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Retryable(reason), detail)
    }

    /// 业务作用：构造确定无效记录失败。
    ///
    /// # 参数
    ///
    /// - `reason`: 无效记录原因。
    /// - `detail`: 将被清洗截断的安全诊断文本。
    pub fn invalid(reason: InvalidRecordReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Invalid(reason), detail)
    }

    /// 业务作用：构造显式 DLT 失败。
    ///
    /// # 参数
    ///
    /// - `reason`: DLT 原因。
    /// - `detail`: 将被清洗截断的安全诊断文本。
    pub fn dead_letter(reason: PassthroughDeadLetterReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::ExplicitDeadLetter(reason), detail)
    }

    /// 业务作用：构造硬暂停失败。
    ///
    /// # 参数
    ///
    /// - `reason`: 硬暂停原因。
    /// - `detail`: 将被清洗截断的安全诊断文本。
    pub fn halt(reason: PassthroughHaltReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Halt(reason), detail)
    }

    /// 业务作用：构造框架致命失败。
    ///
    /// # 参数
    ///
    /// - `reason`: 致命原因。
    /// - `detail`: 将被清洗截断的安全诊断文本。
    pub fn fatal(reason: PassthroughFatalReason, detail: impl Display) -> Self {
        Self::new(PassthroughFailureReason::Fatal(reason), detail)
    }

    /// 业务作用：返回强类型失败原因。
    pub fn reason(&self) -> PassthroughFailureReason {
        self.reason
    }

    /// 业务作用：从强类型原因唯一推导框架动作。
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
    pub fn safe_detail(&self) -> Option<&str> {
        self.safe_detail.as_deref()
    }

    /// 业务作用：统一构造失败并清洗诊断文本。
    ///
    /// # 参数
    ///
    /// - `reason`: 强类型原因。
    /// - `detail`: 原始诊断文本。
    fn new(reason: PassthroughFailureReason, detail: impl Display) -> Self {
        let detail = detail.to_string();
        let cleaned: String = detail
            .chars()
            .map(|ch| if ch.is_control() { ' ' } else { ch })
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

    /// 业务作用：在 owner callback 栈内同步处理借用消息。
    ///
    /// # 参数
    ///
    /// - `record`: 不可跨线程和异步边界逃逸的借用记录。
    ///
    /// # 错误
    ///
    /// 返回强类型失败，由框架按其唯一动作进入重试、坏消息、DLT、暂停或崩溃状态。
    fn consume_borrowed(
        &self,
        record: BorrowedKafkaRecord<'_>,
    ) -> std::result::Result<PassthroughDisposition, PassthroughFailure>;
}
