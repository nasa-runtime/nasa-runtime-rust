//! 每 group 单 owner 线程、借用路由、严格确认与同步安全提交。

mod handler;
mod key_ordered;
mod route_headers;
use key_ordered::KeyCompletions;
use route_headers::RouteHeaders;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant, SystemTime};

use tracing::Instrument;

use super::group::OwnerRuntime;
use crate::consumer::ack::{AckIdentity, ManualAckSet, RecordAck};
use crate::consumer::dlt::{DltDispatcher, DltEnvelope, DltJob, DltRecord};
use crate::consumer::erased::{ConsumerShape, ErasedRecord, InvocationOutcome, RawRecord};
use crate::consumer::offset_state::{AckSource, GroupOffsetState, RecordOutcome, SkipReason};
use crate::consumer::passthrough::{
    BorrowedKafkaRecord, KafkaHeaderRef, KafkaHeadersRef, PassthroughDisposition,
    PassthroughFailureAction,
};
use crate::consumer::supervisor::{
    update_state, CommandReply, GroupPlan, OwnerCommand, OwnerCommandKind, OwnerHandle, OwnerMode,
    StartupGate,
};
use crate::consumer::{AckMode, ConsumeCtx, ExecutionMode};
use crate::error::{NafkaError, Result};
use crate::health::{GroupHealth, GroupState, HaltReason, PauseReason};
use crate::rd::consumer::{
    ConsumerClient, RdMessage, RdPollError, RdRebalanceEvent, RdRebalanceKind,
};
use crate::types::{KafkaHeader, KafkaHeaders, StartOffset, Tp};
use crate::wire::{
    PayloadCodec, DEFAULT_EVENT, HEADER_DELIVERY_ATTEMPT, HEADER_EVENT, HEADER_PASSTHROUGH,
    HEADER_PAYLOAD_CODEC, HEADER_PAYLOAD_MODE, PAYLOAD_CODEC_PROTOCOL_BYTES,
};
use crate::KafkaProxyInner;

/// progress 反压门禁连续多少轮（每轮含一次 50ms 维护 poll）没释放容量后升级为 Degraded。
///
/// 反压本身是合法的自动恢复态，但长期不恢复必须让运维看见并可 `restart_group`，
/// 不能停留在一个外部看不出异常的"正常门禁"里。
const PROGRESS_GATE_IDLE_ALERT: u32 = 200;

/// 单条处理结束后 owner 应采取的动作。
enum ProcessAction {
    /// 当前 offset 已得到可审计的安全处理结果。
    Safe(RecordOutcome),
    /// 当前 offset 必须回退重试。
    Retry(String),
    /// 当前记录必须先成功写入 DLT 才能越过。
    DeadLetter(String),
    /// 当前分区需要人工恢复。
    Halt(HaltReason, String),
    /// group 不可继续运行。
    Fatal(String),
}

/// 应用一次处理结果后对当前逻辑批的控制决策。
#[derive(Clone, Debug, PartialEq, Eq)]
enum ActionFlow {
    /// 当前分区可以继续派发更高 offset。
    Continue,
    /// 当前分区出现空洞，本批不再派发其更高 offset。
    StopPartition,
    /// progress permit 暂时不足，当前逻辑批全部回退并进入自动恢复门禁。
    ///
    /// 必须携带触发记录自身的位置：它没有进入 progress，回退 seek 必须覆盖到它，
    /// 否则批内"后续未派发记录"的回退会把它跳过去并被后续安全前沿提交越过。
    StopBatch {
        /// 触发容量不足的分区。
        tp: Tp,
        /// 触发容量不足的记录 offset（失败 run 取首 offset）。
        offset: i64,
    },
    /// owner 必须退出当前底层会话。
    StopOwner,
}

/// 业务作用：把可信投递序号与毒消息预算分开计数，避免控制态延后伪造投递次数或耗尽 DLT 预算。
struct RetryAttemptState {
    /// 当前 offset 已经完成但未成功的 handler 投递次数，包含控制态延后。
    failed_deliveries: u32,
    /// 当前 offset 已消耗的普通失败次数，不包含控制态延后。
    bounded_failures: u32,
    /// 与本条失败计数同寿命，移除或清空状态时归还组级额度。
    _permit: super::metadata_capacity::RetryPermit,
}

/// 业务作用：整批登记尚未保存次数的失败记录，保证多 owner 共用上限且批次无部分准入。
/// 参数说明：runtime 为共享组容量；attempts 为本槽状态；identities 为本次失败的完整坐标。
/// 返回：全部坐标都有凭证时为 true；容量不足不改变状态。
fn admit_retry_entries(
    runtime: &Arc<OwnerRuntime>,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    identities: impl IntoIterator<Item = (Tp, i64)>,
) -> bool {
    let missing: BTreeSet<_> = identities
        .into_iter()
        .filter(|key| !attempts.contains_key(key))
        .collect();
    let Some(permits) = runtime.metadata.reserve_retries(missing.len()) else {
        return false;
    };
    for (identity, permit) in missing.into_iter().zip(permits) {
        attempts.insert(
            identity,
            RetryAttemptState {
                failed_deliveries: 0,
                bounded_failures: 0,
                _permit: permit,
            },
        );
    }
    true
}

/// 业务作用：读取逐记录投递历史，防止重试批次合并把旧失败次数转移给新输入。
/// 参数说明：attempts 为失败账本；identity 为来源坐标。
/// 返回：已失败投递数与毒消息预算消耗；未失败记录均为零。
fn retry_counts(
    attempts: &BTreeMap<(Tp, i64), RetryAttemptState>,
    identity: &(Tp, i64),
) -> (u32, u32) {
    attempts.get(identity).map_or((0, 0), |state| {
        (state.failed_deliveries, state.bounded_failures)
    })
}

/// owner 应用处理结果时共享的可变状态引用集合。
struct ActionContext<'a> {
    /// 所属运行时。
    runtime: &'a Arc<OwnerRuntime>,
    /// owner 唯一 consumer。
    consumer: &'a ConsumerClient,
    /// group 健康快照。
    health: &'a Arc<RwLock<GroupHealth>>,
    /// resolved group id。
    group: &'a str,
    /// owner 控制命令接收端。
    commands: &'a std::sync::mpsc::Receiver<OwnerCommand>,
    /// 当前订阅方式。
    mode: &'a mut OwnerMode,
    /// 本进程失败尝试表；投递序号与 DLT 预算在值内独立推进。
    attempts: &'a mut BTreeMap<(Tp, i64), RetryAttemptState>,
    /// 每分区非阻塞重试退避状态。
    retries: &'a mut BTreeMap<Tp, PartitionRetry>,
    /// 每分区至多一个在途 DLT 任务。
    dlt_pending: &'a mut BTreeMap<Tp, PendingDlt>,
    /// DLT completion 有界发送端。
    dlt_sender: &'a tokio::sync::mpsc::Sender<DltCompletion>,
    /// dispatcher 暂无 permit 时各失败分区的有界坐标等待。
    dlt_admission: &'a mut BTreeMap<Tp, PendingDltAdmission>,
    /// 连续安全水位状态。
    progress: &'a mut GroupOffsetState,
    /// 最近一次成功提交时间。
    last_commit: &'a mut Instant,
    /// 维护性 poll 捕获的临时错误，交由外层自动重建预算处理。
    transient_exit: &'a mut Option<String>,
    /// 已认领但因 DLT 未收尾而推迟的 Stop；owner 主循环在 DLT 清空后了结它。
    deferred_stop: &'a mut Option<OwnerCommand>,
    /// 当前 assignment epoch。
    assignment_epoch: u64,
}

/// 可被 DLT 构造器读取的原始消息视图。
trait DeadLetterSource {
    /// 业务作用：返回来源 topic。
    fn source_topic(&self) -> &str;

    /// 业务作用：返回来源分区。
    fn source_partition(&self) -> i32;

    /// 业务作用：返回来源 offset。
    fn source_offset(&self) -> i64;

    /// 业务作用：返回来源时间戳。
    fn source_timestamp(&self) -> i64;

    /// 业务作用：返回原始 key。
    fn source_key(&self) -> Option<&[u8]>;

    /// 业务作用：返回原始 payload。
    fn source_payload(&self) -> Option<&[u8]>;

    /// 业务作用：复制原始有序 headers。
    fn source_headers(&self) -> KafkaHeaders;

    /// 业务作用：返回进入 DLT 前已经发生的可信业务投递次数。
    fn delivery_attempts(&self) -> u32 {
        1
    }
}

impl DeadLetterSource for RdMessage<'_> {
    /// 业务作用：返回底层借用消息 topic。
    fn source_topic(&self) -> &str {
        self.topic()
    }

    /// 业务作用：返回底层借用消息分区。
    fn source_partition(&self) -> i32 {
        self.partition()
    }

    /// 业务作用：返回底层借用消息 offset。
    fn source_offset(&self) -> i64 {
        self.offset()
    }

    /// 业务作用：返回底层借用消息时间戳。
    fn source_timestamp(&self) -> i64 {
        self.timestamp()
    }

    /// 业务作用：返回底层借用消息 key。
    fn source_key(&self) -> Option<&[u8]> {
        self.key()
    }

    /// 业务作用：返回底层借用消息 payload。
    fn source_payload(&self) -> Option<&[u8]> {
        self.payload()
    }

    /// 业务作用：仅在 DLT 路径复制底层 headers。
    fn source_headers(&self) -> KafkaHeaders {
        self.headers()
            .map(|headers| owned_headers(&headers))
            .unwrap_or_default()
    }
}

/// typed 逻辑批中的拥有型原始消息；不会进入 passthrough 借用链路。
struct OwnedTypedMessage {
    /// 原文跨 poll、准备和派发队列转移时持续保留原始输入额度。
    input_permit: Arc<super::capacity::RecordPermit>,
    /// 来源 topic。
    topic: String,
    /// 来源分区。
    partition: i32,
    /// 来源 offset。
    offset: i64,
    /// broker 时间戳。
    timestamp: i64,
    /// 原始可空 key 字节。
    key: Option<Vec<u8>>,
    /// 原始有序 headers。
    headers: KafkaHeaders,
    /// 底层 header 名不是 UTF-8；记录必须走无效消息策略，不能进入 route handler。
    malformed_headers: bool,
    /// 原始可空 payload。
    payload: Option<Vec<u8>>,
}

impl OwnedTypedMessage {
    /// 业务作用：在底层借用消息仍有效时立即物化有界 typed 记录。
    ///
    /// # 参数
    ///
    /// - `message`: 已通过单记录字节上限检查的借用消息。
    /// - `parsed`: 在借用期解析的只读头描述符。
    /// - `input_permit`: 原文物化前取得且必须随数据持续保留的责任。
    ///
    /// 返回：独立原文与其额度；额度不会因外层逻辑批结束而提前归还。
    fn copy_from(
        message: &RdMessage<'_>,
        parsed: &Result<Vec<crate::rd::consumer::RdHeaderRef<'_>>>,
        input_permit: Arc<super::capacity::RecordPermit>,
    ) -> Self {
        let (headers, malformed_headers) = match parsed {
            Ok(headers) => (owned_headers(headers), false),
            Err(_) => (KafkaHeaders::default(), true),
        };
        Self {
            input_permit,
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
            timestamp: message.timestamp(),
            key: message.key().map(<[u8]>::to_vec),
            headers,
            malformed_headers,
            payload: message.payload().map(<[u8]>::to_vec),
        }
    }

    /// 业务作用：返回当前消息的 topic-partition。
    fn tp(&self) -> Tp {
        Tp::new(self.topic.clone(), self.partition)
    }
}

impl DeadLetterSource for OwnedTypedMessage {
    /// 业务作用：返回拥有型来源 topic。
    fn source_topic(&self) -> &str {
        &self.topic
    }

    /// 业务作用：返回拥有型来源分区。
    fn source_partition(&self) -> i32 {
        self.partition
    }

    /// 业务作用：返回拥有型来源 offset。
    fn source_offset(&self) -> i64 {
        self.offset
    }

    /// 业务作用：返回拥有型来源时间戳。
    fn source_timestamp(&self) -> i64 {
        self.timestamp
    }

    /// 业务作用：返回拥有型来源 key。
    fn source_key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// 业务作用：返回拥有型来源 payload。
    fn source_payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// 业务作用：克隆拥有型来源 headers 供 DLT 独立持有。
    fn source_headers(&self) -> KafkaHeaders {
        self.headers.clone()
    }
}

impl DeadLetterSource for RawRecord {
    /// 业务作用：返回已解析 typed 原始记录的 topic。
    fn source_topic(&self) -> &str {
        &self.ctx.topic
    }

    /// 业务作用：返回已解析 typed 原始记录的分区。
    fn source_partition(&self) -> i32 {
        self.ctx.partition
    }

    /// 业务作用：返回已解析 typed 原始记录的 offset。
    fn source_offset(&self) -> i64 {
        self.ctx.offset
    }

    /// 业务作用：返回已解析 typed 原始记录的时间戳。
    fn source_timestamp(&self) -> i64 {
        self.ctx.timestamp
    }

    /// 业务作用：让 DLT 保留来源 key 的完整字节身份。
    ///
    /// 参数说明：无。
    /// 返回：原始可空字节，不通过 UTF-8 便利视图重编码。
    fn source_key(&self) -> Option<&[u8]> {
        self.ctx.raw_key.as_deref()
    }

    /// 业务作用：返回已解析 typed 原始记录的 payload。
    fn source_payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// 业务作用：克隆已解析 typed 原始记录的 headers 供 DLT 独立持有。
    fn source_headers(&self) -> KafkaHeaders {
        self.ctx.headers.clone()
    }

    /// 业务作用：返回 owner 在本次 handler 调用前写入的可信投递序号。
    fn delivery_attempts(&self) -> u32 {
        self.ctx.delivery_attempt
    }
}

/// typed 逻辑批内的一项；超限项只保留定位与计量，不复制业务字节。
enum TypedBatchItem {
    /// 已通过单记录上限并完成物化的消息。
    Owned(OwnedTypedMessage),
    /// 超过单记录上限、必须硬暂停且绝不复制的消息。
    Oversize {
        /// 来源 topic-partition。
        tp: Tp,
        /// 来源 offset。
        offset: i64,
        /// 实际 retained bytes。
        retained_bytes: usize,
    },
}

impl TypedBatchItem {
    /// 业务作用：返回当前项的 topic-partition。
    fn tp(&self) -> Tp {
        match self {
            Self::Owned(message) => message.tp(),
            Self::Oversize { tp, .. } => tp.clone(),
        }
    }

    /// 业务作用：返回当前项的真实 offset。
    fn offset(&self) -> i64 {
        match self {
            Self::Owned(message) => message.offset,
            Self::Oversize { offset, .. } => *offset,
        }
    }
}

/// 一次有界 typed 逻辑 poll 批。
struct LogicalTypedBatch {
    /// 按 poll 交付顺序收集的记录。
    items: Vec<TypedBatchItem>,
    /// 已物化消息的累计 retained bytes。
    retained_bytes: usize,
    /// 批次提前回退或失权时，原文仍存活到本批退出，不能只清理账本就提前归还计费。
    _permits: Vec<Arc<super::capacity::RecordPermit>>,
}

/// 一个失败分区的非阻塞退避状态。
struct PartitionRetry {
    /// 恢复前必须再次 seek 的失败单元首 offset。
    retry_from: i64,
    /// 允许恢复派发的单调时钟时刻。
    ready_at: Instant,
    /// 按键输入仍保留在有界队列中，退避只阻止失败 key，无需再次物化原文。
    in_place: bool,
}

/// owner 持有的单分区在途 DLT 身份与取消句柄。
struct PendingDlt {
    /// 任务所属 assignment epoch。
    assignment_epoch: u64,
    /// 失败单元首 offset。
    first_offset: i64,
    /// 失权后关闭新交付；已发出的请求及其配额等待真实终态。
    cancel: Arc<AtomicBool>,
}

/// 异步 DLT worker 返回 owner 的有界 completion。
struct DltCompletion {
    /// 来源 topic-partition。
    tp: Tp,
    /// 调度时的 assignment epoch。
    assignment_epoch: u64,
    /// 失败单元按交付顺序排列的真实 offsets。
    offsets: Vec<i64>,
    /// delivery 成功或最终错误。
    result: Result<()>,
}

/// dispatcher permit 暂不可得时只保留坐标和计量，不持有额外原文副本。
struct PendingDltAdmission {
    /// 已校验的完整信封字节上界；取入原坐标时仍须原子复验额度。
    required_bytes: usize,
    /// 来源 topic-partition。
    tp: Tp,
    /// 调度时 assignment epoch。
    assignment_epoch: u64,
    /// 失败单元按交付顺序排列的 offsets。
    offsets: Vec<i64>,
}

/// typed 记录完成框架路由后的派发形态；自定义解码与业务共用实际调用责任。
enum PreparedTypedItem {
    /// 尚未构造 typed 上下文时已经得到框架动作。
    ImmediateOwned {
        /// 保留 DLT 所需原始字节的来源记录。
        source: OwnedTypedMessage,
        /// 无需调用业务 handler 的动作。
        action: ProcessAction,
    },
    /// 已发现解码失败，但尚未进入按分区顺序的失败治理。
    DecodeFailure {
        /// 保留 DLT 与进度身份的 typed 原始记录。
        source: RawRecord,
        /// 冻结的处理器身份；只有 owner 实际接管本条失败才发出指标。
        consumer_id: &'static str,
        /// 用于后续策略裁决的解码原因。
        reason: String,
    },
    /// 已匹配 route、尚待执行或已有完成证据的记录。
    Routed {
        /// 保留失败重试与 DLT 所需原始数据。
        source: RawRecord,
        /// 匹配的冻结 route。
        route: Arc<dyn crate::consumer::erased::ErasedConsumer>,
    },
    /// 未复制业务字节的超限记录。
    Oversize {
        /// 来源 topic-partition。
        tp: Tp,
        /// 来源 offset。
        offset: i64,
        /// 实际 retained bytes。
        retained_bytes: usize,
    },
}

impl PreparedTypedItem {
    /// 业务作用：返回尚未派发项的 topic-partition 与真实 offset，用于批次提前退出时回退 fetch position。
    fn location(&self) -> (Tp, i64) {
        match self {
            Self::ImmediateOwned { source, .. } => (source.tp(), source.offset),
            Self::DecodeFailure { source, .. } | Self::Routed { source, .. } => {
                (source.ctx.topic_partition(), source.ctx.offset)
            }
            Self::Oversize { tp, offset, .. } => (tp.clone(), *offset),
        }
    }
}

/// 单次底层 consumer 会话退出原因；崩溃会保留 owner 控制邮箱等待显式恢复。
enum OwnerSessionExit {
    /// 收到 Stop 或启动延迟期间停止，owner 可以终止线程。
    Stopped,
    /// 临时底层错误；supervisor 在预算内退避后重建 consumer。
    Transient(String),
    /// fatal、不变量破坏或内部 panic，owner 必须保留邮箱等待 Restart/Stop。
    Crashed,
}

/// 自动重建退避期间收到的控制结果。
enum RestartBackoffOutcome {
    /// 退避自然到期，按预算自动重建。
    Elapsed,
    /// operator 显式要求立即重建并清零预算。
    Restarted,
    /// 收到 Stop 或控制端断开。
    Stopped,
}

/// 业务作用：计算带确定性抖动的 consumer 指数重建退避。
///
/// # 参数
///
/// - `runtime`: 提供初始值与封顶值。
/// - `attempt`: 当前滑动窗口内第几次自动重建，从一开始。
/// - `seed`: resolved group 派生的稳定种子。
fn restart_delay(runtime: &Arc<OwnerRuntime>, attempt: u32, seed: u64) -> Duration {
    restart_delay_values(
        runtime.config.behavior.consume_restart_delay_ms,
        runtime.config.behavior.consume_restart_max_delay_ms,
        attempt,
        seed,
    )
}

/// 业务作用：用显式数值计算 consumer 指数退避，使边界计算不依赖 broker 状态。
///
/// # 参数
///
/// - `base`: 初始退避毫秒。
/// - `max`: 最大退避毫秒。
/// - `attempt`: 当前窗口内第几次重建。
/// - `seed`: 稳定抖动种子。
fn restart_delay_values(base: u64, max: u64, attempt: u32, seed: u64) -> Duration {
    let exponential = capped_exponential_delay(base, max, attempt);
    let spread = exponential / 5;
    let jitter = if spread == 0 {
        0
    } else {
        seed.wrapping_add(u64::from(attempt)) % (spread + 1)
    };
    Duration::from_millis(
        exponential
            .saturating_sub(spread / 2)
            .saturating_add(jitter),
    )
}

/// 业务作用：计算不会因左移丢高位而回绕为零的指数退避基值。
///
/// `checked_shl` 只校验 shift 位数，不校验有效位被移出；必须先构造 2^shift，再用 `saturating_mul` 吸收数值溢出。
///
/// # 参数
///
/// - `base`: 首次退避毫秒。
/// - `max`: 封顶毫秒。
/// - `attempt`: 从一开始的尝试次数。
fn capped_exponential_delay(base: u64, max: u64, attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(63);
    let factor = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    base.saturating_mul(factor).min(max)
}

/// 业务作用：在滑动窗口内登记一次自动重建；先淘汰过期样本，再原子判定预算。
///
/// # 参数
///
/// - `history`: 已发生自动重建的单调时钟序列。
/// - `now`: 当前单调时钟。
/// - `window`: 滑动窗口长度。
/// - `budget`: 窗口内允许的自动重建次数。
fn register_restart(
    history: &mut VecDeque<Instant>,
    now: Instant,
    window: Duration,
    budget: u32,
) -> bool {
    while history
        .front()
        .is_some_and(|started| now.duration_since(*started) >= window)
    {
        history.pop_front();
    }
    if history.len() >= budget as usize {
        false
    } else {
        history.push_back(now);
        true
    }
}

/// 业务作用：从非敏感稳定文本生成 FNV-1a 种子。
///
/// # 参数
///
/// - `value`: resolved group id。
fn stable_text_seed(value: &str) -> u64 {
    value
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
}

/// 业务作用：自动重建退避期间维持 Stop/Restart 控制响应，不持有底层 consumer。
///
/// # 参数
///
/// - `commands`: 跨会话 owner 控制邮箱。
/// - `health`: group 健康快照。
/// - `delay`: 本次退避时长。
fn wait_restart_backoff(
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    health: &Arc<RwLock<GroupHealth>>,
    delay: Duration,
) -> RestartBackoffOutcome {
    let deadline = Instant::now() + delay;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return RestartBackoffOutcome::Elapsed;
        }
        match commands.recv_timeout(remaining.min(Duration::from_millis(100))) {
            Ok(command) if matches!(command.kind, OwnerCommandKind::Stop) => {
                // 同上：被取消的 Stop 不得产生"停止 owner"这个副作用。
                if !OwnerHandle::begin_command(&command) {
                    continue;
                }
                update_state(health, GroupState::Stopped, None);
                OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                return RestartBackoffOutcome::Stopped;
            }
            Ok(command) if matches!(command.kind, OwnerCommandKind::Restart) => {
                if OwnerHandle::begin_command(&command) {
                    OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                    return RestartBackoffOutcome::Restarted;
                }
            }
            Ok(command) => {
                if OwnerHandle::begin_command(&command) {
                    OwnerHandle::reject_command(
                        command,
                        "consumer 正在自动重建退避，仅允许 restart_group 或 stop",
                    );
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return RestartBackoffOutcome::Stopped;
            }
        }
    }
}

/// 业务作用：owner 线程主循环。
///
/// # 参数
///
/// - `runtime`: 所属运行时。
/// - `plan`: 冻结 group 计划。
/// - `commands`: 有界控制命令接收端。
/// - `health`: 共享健康快照。
/// - `startup_gate`: 同一启动批次的统一放行门。
/// - `startup_reply`: owner 本地 consumer 构造结果。
/// - `global_error`: 与 supervisor 共享的 broker 级错误单槽,跨会话重建复用。
pub(crate) fn run_owner(
    runtime: Arc<OwnerRuntime>,
    mut plan: GroupPlan,
    commands: std::sync::mpsc::Receiver<OwnerCommand>,
    health: Arc<RwLock<GroupHealth>>,
    startup_gate: Arc<StartupGate>,
    startup_reply: tokio::sync::oneshot::Sender<Result<()>>,
    global_error: Arc<std::sync::Mutex<Option<String>>>,
) {
    let fixed_offsets = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let initial_consumer = match ConsumerClient::create(
        &runtime.config,
        &plan.group,
        runtime.metrics.clone(),
        Arc::clone(&global_error),
        Arc::clone(&fixed_offsets),
    ) {
        Ok(consumer) => consumer,
        Err(error) => {
            update_state(
                &health,
                GroupState::Crashed,
                Some(format!(
                    "group={} 本地 consumer 构造失败: {error}",
                    plan.group
                )),
            );
            let _ = startup_reply.send(Err(error));
            return;
        }
    };
    if startup_reply.send(Ok(())).is_err() {
        update_state(
            &health,
            GroupState::Stopped,
            Some("owner 启动调用方已离开".into()),
        );
        return;
    }
    if !wait_startup_gate(&commands, &health, &startup_gate) {
        return;
    }

    let mut attempts: BTreeMap<(Tp, i64), RetryAttemptState> = BTreeMap::new();
    let mut restart_history = VecDeque::new();
    let mut initial_consumer = Some(initial_consumer);
    // 对外发布的 assignment_epoch 必须跨会话单调：底层 epoch 计数器随 ConsumerClient
    // 重建从 0 起，直接对外发布会让调用方在会话重建后看到相同数值，误判"没发生过 rebalance"。
    let epoch_base = Arc::new(std::sync::atomic::AtomicU64::new(0));
    loop {
        // 每一代底层会话开始前先撤销上一代 ready 租约。否则 broker 全停且旧 consumer 已销毁时，
        // await_group_ready 仍会凭旧 assignment 立即放行。
        invalidate_ready_snapshot(&runtime, &plan.group, &health);
        note_owner_session_start(&health);
        let session_counts = health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owner_session_started;
        runtime.counter("owner_session_started_total", 1, &[("group", &plan.group)]);
        if session_counts > 1 {
            runtime.counter(
                "owner_session_restarted_total",
                1,
                &[("group", &plan.group)],
            );
        }
        let session_started = Instant::now();
        let session = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run_owner_session(
                &runtime,
                &mut plan,
                &commands,
                &health,
                &mut attempts,
                initial_consumer.take(),
                &epoch_base,
                &global_error,
                &fixed_offsets,
            )
        }));
        // 会话已经结束（consumer 已析构）：立刻撤销 ready 租约，不能等到下一代入口。
        // 否则整个重建退避窗口（默认 3s，退避上限 60s）里 await_group_ready 仍会凭
        // 上一代 assignment 立即放行，而此刻底层什么都没在消费。
        invalidate_ready_snapshot(&runtime, &plan.group, &health);
        // 即使会话因 panic 提前展开栈，也必须先等实际闭包退出，才允许重建或报告停止。
        runtime.handlers.await_idle();
        runtime.records.clear(None);
        // 停止意图跨会话保留；停止收口期间的网络错误不能重新开放另一个消费会话。
        if runtime.stop_requested.load(Ordering::Acquire) {
            update_state(&health, GroupState::Stopped, None);
            return;
        }
        let exit = match session {
            Ok(exit) => exit,
            Err(payload) => {
                let detail = if let Some(value) = payload.downcast_ref::<&str>() {
                    (*value).to_owned()
                } else if let Some(value) = payload.downcast_ref::<String>() {
                    value.clone()
                } else {
                    "非字符串 panic payload".into()
                };
                update_state(
                    &health,
                    GroupState::Crashed,
                    Some(format!("owner panic: {detail}")),
                );
                OwnerSessionExit::Crashed
            }
        };
        match exit {
            OwnerSessionExit::Stopped => return,
            OwnerSessionExit::Transient(detail) => {
                let window =
                    Duration::from_millis(runtime.config.behavior.consume_restart_budget_window_ms);
                if session_started.elapsed() >= window {
                    restart_history.clear();
                }
                let now = Instant::now();
                if !register_restart(
                    &mut restart_history,
                    now,
                    window,
                    runtime.config.behavior.consume_restart_budget_times,
                ) {
                    update_state(
                        &health,
                        GroupState::Crashed,
                        Some(format!("consumer 自动重建预算耗尽: {detail}")),
                    );
                    if !wait_for_crashed_command(&commands, &health) {
                        return;
                    }
                    attempts.clear();
                    restart_history.clear();
                    continue;
                }
                let delay = restart_delay(
                    &runtime,
                    restart_history.len() as u32,
                    stable_text_seed(&plan.group),
                );
                update_state(
                    &health,
                    GroupState::Recovering,
                    Some(format!(
                        "consumer 临时错误，等待自动重建 delay_ms={}: {detail}",
                        delay.as_millis()
                    )),
                );
                match wait_restart_backoff(&commands, &health, delay) {
                    RestartBackoffOutcome::Elapsed => {
                        update_state(&health, GroupState::Starting, None);
                    }
                    RestartBackoffOutcome::Restarted => {
                        attempts.clear();
                        restart_history.clear();
                        update_state(&health, GroupState::Starting, None);
                    }
                    RestartBackoffOutcome::Stopped => return,
                }
            }
            OwnerSessionExit::Crashed => {
                if !wait_for_crashed_command(&commands, &health) {
                    return;
                }
                attempts.clear();
                restart_history.clear();
            }
        }
    }
}

/// 业务作用：在 registry/assign 原子登记 owner 前等待统一放行，并保持 Stop 命令可响应。
///
/// # 参数
///
/// - `commands`: owner 控制命令接收端。
/// - `health`: 共享健康快照。
/// - `startup_gate`: 同一启动批次的共享门。
///
/// # 返回
///
/// 门已打开时返回 true；启动取消、Stop 或发送端断开时返回 false。
fn wait_startup_gate(
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    health: &Arc<RwLock<GroupHealth>>,
    startup_gate: &StartupGate,
) -> bool {
    loop {
        if startup_gate.is_open() {
            return true;
        }
        if startup_gate.is_aborted() {
            update_state(health, GroupState::Stopped, None);
            return false;
        }
        match commands.recv_timeout(Duration::from_millis(10)) {
            Ok(command) => {
                if !OwnerHandle::begin_command(&command) {
                    continue;
                }
                if matches!(&command.kind, OwnerCommandKind::Stop) {
                    update_state(health, GroupState::Stopped, None);
                    OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                    return false;
                }
                OwnerHandle::reject_command(command, "consumer owner 尚未通过 startup gate");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                update_state(health, GroupState::Stopped, None);
                return false;
            }
        }
    }
}

/// 业务作用：构造并运行一次底层 consumer 会话，退出后把 owner 控制邮箱留给后续会话。
///
/// # 参数
///
/// - `runtime`: 所属运行时与冻结配置。
/// - `plan`: group 路由计划；动态订阅会更新其中的 mode。
/// - `commands`: 跨会话保留的有界控制命令接收端。
/// - `health`: 跨会话保留的健康快照。
/// - `initial_consumer`: 首次会话由 startup handshake 预先构造的 consumer；后续重建为 None。
/// - `epoch_base`: 跨会话递增的消费世代基线。
/// - `global_error`: 跨会话共享的最近错误单槽。
/// - `fixed_offsets`: 固定分配已经解析、定位或确认提交的跨会话恢复坐标。
///
/// # 返回
///
/// 返回本次会话的退出原因，由 owner 决定停止、重建或保持故障状态。
#[allow(clippy::too_many_arguments)] // 启动握手、控制邮箱、故障证据和恢复坐标分别承担跨会话责任。
fn run_owner_session(
    runtime_ref: &Arc<OwnerRuntime>,
    plan: &mut GroupPlan,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    health_ref: &Arc<RwLock<GroupHealth>>,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    initial_consumer: Option<ConsumerClient>,
    epoch_base: &std::sync::atomic::AtomicU64,
    global_error: &Arc<std::sync::Mutex<Option<String>>>,
    fixed_offsets: &Arc<std::sync::Mutex<BTreeMap<Tp, i64>>>,
) -> OwnerSessionExit {
    let runtime = Arc::clone(runtime_ref);
    let _assignment_session = runtime.metadata.session(runtime.slot);
    let health = Arc::clone(health_ref);
    let consumer = match initial_consumer {
        Some(consumer) => consumer,
        None => {
            match ConsumerClient::create(
                &runtime.config,
                &plan.group,
                runtime.metrics.clone(),
                Arc::clone(global_error),
                Arc::clone(fixed_offsets),
            ) {
                Ok(consumer) => consumer,
                Err(error) => return classify_session_error(&health, &plan.group, error),
            }
        }
    };
    let delay = Duration::from_millis(runtime.config.behavior.start_consume_delay_ms);
    if !delay.is_zero() && !delay_with_commands(&consumer, commands, &health, delay) {
        update_state(&health, GroupState::Stopped, None);
        return OwnerSessionExit::Stopped;
    }
    // 固定分配先取得完整组范围，不能在原生 assign 后才发现超限并留下可派发窗口。
    if let OwnerMode::Assign { partitions, .. } = &plan.mode {
        if let Err(actual) = runtime.metadata.assign(runtime.slot, partitions) {
            return classify_session_error(
                &health,
                &plan.group,
                NafkaError::AssignmentTooLarge {
                    group: plan.group.clone(),
                    actual,
                    max: runtime.config.consumer.max_assignment_partitions,
                },
            );
        }
    }
    // 启动延迟必须发生在 subscribe/assign 之前；否则维护性 poll 会取走尚未派发的记录。
    if let Err(error) = initialize_owner(
        &consumer,
        &plan.mode,
        &runtime.config.consumer.auto_offset_reset,
    ) {
        return classify_session_error(&health, &plan.group, error);
    }
    update_state(&health, GroupState::Joining, None);

    let mut assignment = match &plan.mode {
        OwnerMode::Assign { partitions, .. } => partitions.clone(),
        OwnerMode::Subscribe { .. } => Vec::new(),
    };
    let mut assignment_epoch = if assignment.is_empty() {
        0
    } else {
        1
    };
    // 固定 assign 也必须等一次成功 poll 之后才发布 ready：本地 assign 不代表 broker 可达，
    // 会话入口就发布等于把刚撤销的 ready 租约当场恢复，await_group_ready 便再也
    // 反映不出 broker 中断。
    let mut pending_ready_epoch = if assignment_epoch == 0 {
        None
    } else {
        reapply_persistent_pauses(&consumer, &health, &assignment);
        Some(assignment_epoch)
    };
    let mut retries: BTreeMap<Tp, PartitionRetry> = BTreeMap::new();
    let mut dlt_pending: BTreeMap<Tp, PendingDlt> = BTreeMap::new();
    let mut dlt_admission: BTreeMap<Tp, PendingDltAdmission> = BTreeMap::new();
    let (dlt_sender, mut dlt_receiver) =
        tokio::sync::mpsc::channel(runtime.config.behavior.dlt_max_in_flight.saturating_add(1));
    let mut progress = GroupOffsetState::new(
        runtime.config.behavior.max_progress_records,
        runtime.config.behavior.max_group_progress_records,
    );
    let mut last_commit = Instant::now();
    let poll_timeout = Duration::from_millis(runtime.config.behavior.poll_timeout_ms);
    let poll_cadence = poll_timeout.min(Duration::from_millis(25));
    let mut last_typed_poll = Instant::now() - poll_cadence;
    let commit_interval = Duration::from_millis(runtime.config.behavior.commit_interval_ms);
    let mut partition_cursor = 0_usize;
    let mut deferred_stop: Option<OwnerCommand> = None;
    // 按分区隔离的连续解码失败计数（解码整体故障熔断，见 decode_failure_action）。
    // owner 单线程独占，成功即移除条目，规模受 assignment 约束。
    let mut decode_streaks: BTreeMap<Tp, u64> = BTreeMap::new();
    // 后续 key 的完成证据跨本地重投保留，避免前缀失败令已成功的业务调用重复执行。
    // 该证据仅在当前 assignment 内有效，不能跨失权、人工定位或 owner 会话恢复复用。
    let mut key_completions = KeyCompletions::default();
    // 未裁决输入跨 poll 保留；实际调用不能阻塞其它分区的主循环。
    let mut queued_typed: BTreeMap<Tp, VecDeque<PreparedTypedItem>> = BTreeMap::new();
    let mut transient_exit = None;
    // progress 反压门禁连续多少轮没能释放容量；只用于把长期反压升级成运维可见的 Degraded。
    let mut progress_gate_idle: u32 = 0;
    // 触发本次反压的分区；解除条件只看它自身容量，避免被无关的卡住分区拖成全组死锁。
    let mut progress_gate_trigger: Option<Tp> = None;
    let mut stopping = false;
    // 仪表采样节流：主循环每轮都跑，但 sink 是用户实现，不能按 poll 频率打。
    let mut last_gauge_sample = Instant::now() - GAUGE_SAMPLE_INTERVAL;
    while !stopping {
        runtime.records.prune_committed(&progress);
        resume_input_partitions(&consumer, &health, &runtime.records.resumable());
        key_completions.prune_committed(&progress);
        key_completions.collect_finished(
            &consumer,
            assignment_epoch,
            runtime.stop_requested.load(Ordering::Acquire) || deferred_stop.is_some(),
            &health,
        );
        key_completions.resume_idle(&consumer, &health);
        if let Some(error) = key_completions.fatal_error() {
            // 致命框架结论不等待更早的慢 key，先关闭新派发再收尾真实调用。
            update_state(&health, GroupState::Crashed, Some(error));
            break;
        }

        // 暂停行与 ready 租约可在多个分支变化，按固定节奏统一采样，避免仪表停留在旧状态。
        if last_gauge_sample.elapsed() >= GAUGE_SAMPLE_INTERVAL {
            publish_health_gauges(&runtime, &plan.group, &health);
            last_gauge_sample = Instant::now();
        }
        {
            let mut context = ActionContext {
                runtime: &runtime,
                consumer: &consumer,
                health: &health,
                group: &plan.group,
                commands,
                mode: &mut plan.mode,
                attempts,
                retries: &mut retries,
                dlt_pending: &mut dlt_pending,
                dlt_sender: &dlt_sender,
                dlt_admission: &mut dlt_admission,
                progress: &mut progress,
                last_commit: &mut last_commit,
                transient_exit: &mut transient_exit,
                deferred_stop: &mut deferred_stop,
                assignment_epoch,
            };
            stopping |= drain_dlt_completions(&mut context, &mut dlt_receiver);
        }
        if stopping {
            break;
        }
        if deferred_stop.is_some()
            && dlt_pending.is_empty()
            && dlt_admission.is_empty()
            && !key_completions.has_active()
        {
            OwnerHandle::finish_command(
                deferred_stop.take().expect("停止命令存在"),
                Ok(CommandReply::Unit),
            );
            stopping = true;
            continue;
        }
        activate_due_retries(
            &runtime,
            &consumer,
            &health,
            &mut retries,
            runtime.config.behavior.consume_retry_backoff_ms,
        );
        while let Ok(command) = commands.try_recv() {
            // 先 CAS 认领命令：调用方已超时取消时直接丢弃，绝不让一个不会被执行的命令
            // 产生任何副作用。
            if !OwnerHandle::begin_command(&command) {
                continue;
            }
            if deferred_stop.is_some() {
                OwnerHandle::reject_command(command, "group 正在等待 DLT 收尾");
                continue;
            }
            if let Some(error) = command_epoch_error(&consumer, &health, &command) {
                OwnerHandle::finish_command(command, Err(error));
                continue;
            }
            // 全部拒绝理由集中在 admission 裁决里，且必须早于 commit/cancel/clear 这些
            // 破坏性副作用：契约是"返回错误即未执行"，不能出现"拒绝了但状态已被摧毁"。
            if let Some(error) = command_admission_error(
                &command.kind,
                &plan.mode,
                &health,
                &dlt_pending,
                &dlt_admission,
            ) {
                tracing::debug!(error = %error, "控制命令未通过副作用准入");
                let id = command.id;
                OwnerHandle::finish_command(command, Err(NafkaError::ControlNotApplied(id)));
                continue;
            }
            let invalidates = command_invalidates_progress(&command.kind);
            // 定位不能清掉仍在队列或执行中的输入；停止等待独立的真实退出责任。
            let targets = super::group::partition_targets(&command.kind);
            let input_busy = targets
                .as_ref()
                .filter(|targets| !targets.is_empty())
                .map_or_else(
                    || !queued_typed.is_empty() || key_completions.has_active(),
                    |targets| {
                        targets.iter().any(|tp| {
                            queued_typed.contains_key(tp) || key_completions.active_partition(tp)
                        })
                    },
                );
            if invalidates && !matches!(command.kind, OwnerCommandKind::Stop) && input_busy {
                let id = command.id;
                OwnerHandle::finish_command(command, Err(NafkaError::ControlNotApplied(id)));
                continue;
            }
            if invalidates && progress.observed_total() != 0 {
                stopping |= commit_until_resolved(
                    &consumer,
                    &mut progress,
                    &health,
                    commands,
                    &mut plan.mode,
                    attempts,
                    &mut last_commit,
                    runtime.config.behavior.consume_retry_backoff_ms,
                    &mut transient_exit,
                    !dlt_pending.is_empty() || !dlt_admission.is_empty(),
                    &mut deferred_stop,
                );
                if stopping {
                    break;
                }
            }
            // 提交等待可能处理了新的协调事件；清理旧责任前必须再次复验命令权威。
            if let Some(error) = command_epoch_error(&consumer, &health, &command) {
                OwnerHandle::finish_command(command, Err(error));
                continue;
            }
            if invalidates
                && super::group::partition_targets(&command.kind).is_some_and(|targets| {
                    targets.iter().any(|tp| {
                        progress.has_partition_observations(tp) || retries.contains_key(tp)
                    })
                })
            {
                // 定位不能抹去目标尚未完成的输入或退避责任，必须先由原业务形成安全提交前缀。
                let id = command.id;
                OwnerHandle::finish_command(command, Err(NafkaError::ControlNotApplied(id)));
                continue;
            }
            if invalidates {
                cancel_retry_states(&consumer, &health, &mut retries, &command.kind);
                if !matches!(command.kind, OwnerCommandKind::Stop) {
                    cancel_dlt_states(
                        &runtime,
                        &consumer,
                        &health,
                        &mut dlt_pending,
                        &mut dlt_admission,
                        &command.kind,
                    );
                }
            }
            if matches!(command.kind, OwnerCommandKind::Stop)
                && (!dlt_pending.is_empty()
                    || !dlt_admission.is_empty()
                    || key_completions.has_active())
            {
                update_state(&health, GroupState::Stopping, None);
                deferred_stop = Some(command);
                break;
            }
            let changed_targets = super::group::partition_targets(&command.kind);
            let previous_subscriptions = if matches!(
                command.kind,
                OwnerCommandKind::Subscribe { .. } | OwnerCommandKind::Unsubscribe
            ) {
                match &plan.mode {
                    OwnerMode::Subscribe { topics } => Some(topics.clone()),
                    _ => None,
                }
            } else {
                None
            };
            stopping = execute_command(
                &consumer,
                &mut plan.mode,
                attempts,
                &health,
                &runtime.config.consumer.auto_offset_reset,
                command,
            );
            if previous_subscriptions.is_some_and(|previous| matches!(&plan.mode, OwnerMode::Subscribe { topics } if *topics != previous)) {
                // 订阅真正改变后的 Ready 必须来自后续分配；相同集合不丢弃原本有效的初始化证据。
                pending_ready_epoch = None;
            }
            if matches!(&plan.mode, OwnerMode::Subscribe { topics } if topics.is_empty()) {
                let _ = runtime.metadata.assign(runtime.slot, &[]);
            }
            if invalidates {
                if let Some(targets) = changed_targets.filter(|targets| !targets.is_empty()) {
                    // 人工定位只建立目标分区的新坐标，其它分区的完成证据和安全前沿继续有效。
                    runtime.records.clear(Some(&targets));
                    for tp in targets {
                        key_completions.clear_partition(&tp);
                        progress.clear_partition(&tp);
                    }
                } else {
                    runtime.records.clear(None);
                    key_completions.clear();
                    progress.clear();
                }
            }
            if stopping {
                break;
            }
        }
        if stopping {
            break;
        }
        resume_key_retry_inputs(&consumer, &health, &retries);
        {
            let mut context = ActionContext {
                runtime: &runtime,
                consumer: &consumer,
                health: &health,
                group: &plan.group,
                commands,
                mode: &mut plan.mode,
                attempts,
                retries: &mut retries,
                dlt_pending: &mut dlt_pending,
                dlt_sender: &dlt_sender,
                dlt_admission: &mut dlt_admission,
                progress: &mut progress,
                last_commit: &mut last_commit,
                transient_exit: &mut transient_exit,
                deferred_stop: &mut deferred_stop,
                assignment_epoch,
            };
            service_dlt_admission(&mut context)
        };
        if deferred_stop.is_some() {
            if let Err(error) = consumer.poll(Duration::from_millis(50)) {
                record_maintenance_poll_error(&health, &mut transient_exit, error);
                break;
            }
            health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last_poll_at = Some(SystemTime::now());
            continue;
        }
        // 门禁的判据是 pause 事实而不是 health.state：state 会被 note_success（每次成功
        // commit / DLT 完成）、halt_partition、activate_due_retries 覆写成 Running/Degraded，
        // 若以它为守卫，门禁内部第一次成功 commit 就会把自己的旗子擦掉，导致全部分区
        // 永久 pause 而健康面显示 Running。
        if progress_gate_engaged(&health) {
            stopping |= commit_until_resolved(
                &consumer,
                &mut progress,
                &health,
                commands,
                &mut plan.mode,
                attempts,
                &mut last_commit,
                runtime.config.behavior.consume_retry_backoff_ms,
                &mut transient_exit,
                !dlt_pending.is_empty() || !dlt_admission.is_empty(),
                &mut deferred_stop,
            );
            if stopping {
                continue;
            }
            runtime.records.prune_committed(&progress);
            if progress.has_dispatch_capacity(progress_gate_trigger.as_ref()) {
                let assignment = consumer.assignment().unwrap_or_default();
                let resumable =
                    clear_pause_reason(&health, &assignment, &PauseReason::ProgressBackpressure);
                let _ = consumer.resume(&resumable);
                progress_gate_idle = 0;
                progress_gate_trigger = None;
                note_success(&health);
            } else {
                // 容量没释放时保持门禁可见：上面那些路径可能已经把 state 改回 Running。
                if !progress.has_pending_gap() {
                    // 规则 5：满、无可提交前沿、也没有任何已知空洞 → 框架不变量破坏。
                    // 这里不能静默空转，必须变成运维可见且可 restart_group 的状态。
                    update_state(
                        &health,
                        GroupState::Degraded,
                        Some("progress 容量耗尽但无任何待收尾空洞(ProgressInvariant)".into()),
                    );
                } else {
                    progress_gate_idle = progress_gate_idle.saturating_add(1);
                    if progress_gate_idle >= PROGRESS_GATE_IDLE_ALERT {
                        update_state(
                            &health,
                            GroupState::Degraded,
                            Some("progress 反压长时间未释放，等待重试/DLT 收尾".into()),
                        );
                    } else {
                        update_state(&health, GroupState::ProgressBackpressure, None);
                    }
                }
                if let Err(error) = consumer.poll(Duration::from_millis(50)) {
                    record_maintenance_poll_error(&health, &mut transient_exit, error);
                    stopping = true;
                }
                continue;
            }
        }
        if !plan.typed.is_empty() {
            let local_work = key_completions.has_active() || !queued_typed.is_empty();
            // 本地已有输入或调用时只做非阻塞 native 维护，完成票据唤醒后及时收割，不给每条业务附加网络等待。
            let wait = if local_work {
                Duration::ZERO
            } else {
                poll_timeout
            };
            // 连续完成独立推进；网络维护按有界周期进行，让已接纳输入在下一次拉取前获得释放额度的机会。
            let polled = !local_work || last_typed_poll.elapsed() >= poll_cadence;
            let incoming = if polled {
                let value = collect_typed_batch(
                    &consumer,
                    &runtime,
                    &health,
                    wait,
                    &queued_typed,
                    &progress,
                    assignment_epoch,
                );
                last_typed_poll = Instant::now();
                health
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .last_poll_at = Some(SystemTime::now());
                value
            } else {
                Ok(LogicalTypedBatch {
                    items: Vec::new(),
                    retained_bytes: 0,
                    _permits: Vec::new(),
                })
            };
            let batch = match incoming {
                Ok(batch) => batch,
                Err(RdPollError::Transient(error)) => {
                    update_state(&health, GroupState::Recovering, Some(error.clone()));
                    transient_exit = Some(error);
                    break;
                }
                Err(RdPollError::Fatal(error)) => {
                    update_state(&health, GroupState::Crashed, Some(error));
                    break;
                }
            };
            if let Some(event) = consumer.take_rebalance() {
                key_completions.clear();
                let mut unstarted = queued_typed
                    .iter()
                    .filter_map(|(tp, items)| {
                        items.front().map(|item| (tp.clone(), item.location().1))
                    })
                    .collect::<BTreeMap<_, _>>();
                queued_typed.clear();
                // Assigned 分支会 prime_assignment_positions，把每个分区重置回 committed；
                // 那之后再按"本批最小 offset"回退等于把位置往**前**推，会跳过
                // committed 与批首之间那些已观察但随 reset_for_assignment 一起作废的记录。
                let repriced = matches!(event.kind, RdRebalanceKind::Assigned);
                if !apply_rebalance(
                    &consumer,
                    &plan.group,
                    &runtime,
                    &health,
                    &mut assignment,
                    &mut assignment_epoch,
                    &mut progress,
                    attempts,
                    &mut retries,
                    &mut dlt_pending,
                    &mut dlt_admission,
                    &mut pending_ready_epoch,
                    event,
                ) {
                    break;
                }
                if !batch.items.is_empty() || !unstarted.is_empty() {
                    // 本批已经从 broker 取走但一条都没派发。只有在**没有重新 prime**的事件
                    // （`Error`：不改 assignment 不 prime；cooperative 的 revoke-only：保留分区
                    // 不 prime）上才需要按批首回退，否则丢批等于让后续安全前沿跨过未处理记录。
                    if !repriced {
                        let rewind =
                            batch
                                .items
                                .iter()
                                .fold(BTreeMap::<Tp, i64>::new(), |mut acc, item| {
                                    acc.entry(item.tp())
                                        .and_modify(|current| {
                                            *current = (*current).min(item.offset());
                                        })
                                        .or_insert_with(|| item.offset());
                                    acc
                                });
                        for (tp, offset) in rewind {
                            unstarted
                                .entry(tp)
                                .and_modify(|current| *current = (*current).min(offset))
                                .or_insert(offset);
                        }
                        let failed = rewind_input_offsets(&runtime, &consumer, &health, &unstarted);
                        halt_failed_rewinds(&consumer, &health, &failed);
                    }
                    continue;
                }
            } else if polled {
                // 本地调度轮次不构成初始化后的 broker 维护证据；必须等实际 poll 才开放新的 Ready。
                if let Some(ready_epoch) = pending_ready_epoch.take() {
                    publish_assignment_health(
                        &runtime,
                        &plan.group,
                        &health,
                        &assignment,
                        external_epoch(epoch_base, ready_epoch),
                    );
                }
            }
            if batch.items.is_empty()
                && progress.observed_total() != 0
                && last_commit.elapsed() >= commit_interval
            {
                stopping |= commit_until_resolved(
                    &consumer,
                    &mut progress,
                    &health,
                    commands,
                    &mut plan.mode,
                    attempts,
                    &mut last_commit,
                    runtime.config.behavior.consume_retry_backoff_ms,
                    &mut transient_exit,
                    !dlt_pending.is_empty() || !dlt_admission.is_empty(),
                    &mut deferred_stop,
                );
            }
            debug_assert!(
                batch.retained_bytes <= runtime.config.behavior.max_batch_bytes
                    || batch.items.len() == 1
            );
            stopping |= process_logical_typed_batch(
                &runtime,
                &consumer,
                plan,
                &health,
                commands,
                attempts,
                &mut retries,
                &mut dlt_pending,
                &dlt_sender,
                &mut dlt_admission,
                &mut progress,
                &mut last_commit,
                assignment_epoch,
                batch,
                &mut partition_cursor,
                &mut transient_exit,
                &mut progress_gate_trigger,
                &mut deferred_stop,
                &mut decode_streaks,
                &mut key_completions,
                &mut queued_typed,
            );
            if !stopping
                && queued_typed.is_empty()
                && !key_completions.has_active()
                && retries.is_empty()
                && dlt_pending.is_empty()
                && dlt_admission.is_empty()
                && progress.observed_total() != 0
                && !progress.has_pending_gap()
            {
                // 本地责任已经形成完整安全前缀，尾批无需等待定时阈值；未知提交仍进入原有保护和退避流程。
                stopping |= commit_until_resolved(
                    &consumer,
                    &mut progress,
                    &health,
                    commands,
                    &mut plan.mode,
                    attempts,
                    &mut last_commit,
                    runtime.config.behavior.consume_retry_backoff_ms,
                    &mut transient_exit,
                    false,
                    &mut deferred_stop,
                );
            }
            if (key_completions.has_active() || !queued_typed.is_empty())
                && !stopping
                && !key_completions.has_finished()
            {
                // unpark 保留先到的完成通知；等待仍有上限，使 broker 维护、重试和失权检查持续推进。
                std::thread::park_timeout(poll_cadence.saturating_sub(last_typed_poll.elapsed()));
            }
            continue;
        }
        let message = match consumer.poll(poll_timeout) {
            Ok(message) => message,
            Err(RdPollError::Transient(error)) => {
                update_state(&health, GroupState::Recovering, Some(error.clone()));
                transient_exit = Some(error);
                break;
            }
            Err(RdPollError::Fatal(error)) => {
                update_state(&health, GroupState::Crashed, Some(error));
                break;
            }
        };
        {
            let mut snapshot = health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            snapshot.last_poll_at = Some(SystemTime::now());
        }
        if let Some(event) = consumer.take_rebalance() {
            let repriced = matches!(event.kind, RdRebalanceKind::Assigned);
            if !apply_rebalance(
                &consumer,
                &plan.group,
                &runtime,
                &health,
                &mut assignment,
                &mut assignment_epoch,
                &mut progress,
                attempts,
                &mut retries,
                &mut dlt_pending,
                &mut dlt_admission,
                &mut pending_ready_epoch,
                event,
            ) {
                break;
            }
            // 与 typed 路径同样的规则：本次 poll 取到的记录属于 rebalance 之前的 generation，
            // 不能用新 epoch 处理它（分区可能已被 revoke，progress 也已按新 assignment 重置）。
            // 丢弃后重新拉取；只有在没有重新 prime 的事件上才需要显式回退位置，
            // Assigned 已把位置重置回 committed，再按本条 offset 回退会把位置往前推。
            if let Some(message) = &message {
                if !repriced {
                    let tp = Tp::new(message.topic(), message.partition());
                    let _ = consumer.seek_offsets(&BTreeMap::from([(tp, message.offset())]));
                }
                continue;
            }
        } else if let Some(ready_epoch) = pending_ready_epoch.take() {
            publish_assignment_health(
                &runtime,
                &plan.group,
                &health,
                &assignment,
                external_epoch(epoch_base, ready_epoch),
            );
        }
        let Some(message) = message else {
            if progress.observed_total() != 0 && last_commit.elapsed() >= commit_interval {
                stopping |= commit_until_resolved(
                    &consumer,
                    &mut progress,
                    &health,
                    commands,
                    &mut plan.mode,
                    attempts,
                    &mut last_commit,
                    runtime.config.behavior.consume_retry_backoff_ms,
                    &mut transient_exit,
                    !dlt_pending.is_empty() || !dlt_admission.is_empty(),
                    &mut deferred_stop,
                );
            }
            continue;
        };
        let tp = Tp::new(message.topic(), message.partition());
        if let Some(expected) = runtime
            .records
            .accept_dead_letter_position(&tp, message.offset())
        {
            halt_partition(
                &consumer,
                &health,
                &tp,
                HaltReason::ProgressInvariant,
                format!(
                    "DLT 原坐标 {expected} 未重新交付，实际位置 {}",
                    message.offset()
                ),
            );
            continue;
        }
        let bytes = message.retained_bytes();
        let _input_permit = if bytes
            <= runtime
                .config
                .behavior
                .max_record_bytes
                .min(runtime.records.maximum_record_bytes())
        {
            match runtime.records.retain(
                &tp,
                message.offset(),
                super::capacity::input_reservation_bytes(bytes),
                message.key(),
            ) {
                Some(permit) => Some(permit),
                None => {
                    // 借用消费同样受完整责任额度约束；未执行业务的记录必须从其原坐标重新交付。
                    let failed =
                        consumer.rewind_offsets(&BTreeMap::from([(tp.clone(), message.offset())]));
                    halt_failed_rewinds(&consumer, &health, &failed);
                    enter_input_backpressure(&consumer, &health, &tp);
                    continue;
                }
            }
        } else {
            None
        };
        let action = process_message(&runtime, plan, assignment_epoch, &message);
        let flow = {
            let mut context = ActionContext {
                runtime: &runtime,
                consumer: &consumer,
                health: &health,
                group: &plan.group,
                commands,
                mode: &mut plan.mode,
                attempts,
                retries: &mut retries,
                dlt_pending: &mut dlt_pending,
                dlt_sender: &dlt_sender,
                dlt_admission: &mut dlt_admission,
                progress: &mut progress,
                last_commit: &mut last_commit,
                transient_exit: &mut transient_exit,
                deferred_stop: &mut deferred_stop,
                assignment_epoch,
            };
            apply_process_action(&mut context, &message, action)
        };
        match flow {
            ActionFlow::StopOwner => stopping = true,
            // 单消息(passthrough)路径同样要回退触发记录：它没有进入 progress，
            // 若不 seek 回去，门禁解除后就从下一条继续，安全前沿会把它提交越过。
            ActionFlow::StopBatch { tp, offset } => {
                let _ = consumer.seek_offsets(&BTreeMap::from([(tp.clone(), offset)]));
                progress_gate_trigger = Some(tp);
            }
            ActionFlow::Continue | ActionFlow::StopPartition => {}
        }
    }
    if progress.observed_total() != 0 && !consumer.assignment_lost() {
        if let Ok(offsets) = progress.committable_offsets() {
            if !offsets.is_empty() {
                match consumer.commit_offsets(&offsets) {
                    Ok(()) => {
                        note_commit_result(&health, true);
                        let _ = progress.confirm_committed(&offsets);
                    }
                    Err(_) => note_commit_result(&health, false),
                }
            }
        }
    }
    key_completions.clear();
    queued_typed.clear();
    // 退出先撤销旧调用，再等实际资源归还；短关闭等待不能授权另一调用越过它。
    while key_completions.has_active() {
        key_completions.collect_finished(&consumer, assignment_epoch, true, &health);
        if key_completions.has_active() {
            let _ = consumer.poll(Duration::from_millis(25));
        }
    }
    // 已认领（EXECUTING）的 deferred Stop 必须在这里得到确定结果：直接丢弃会让调用方
    // 收到 ControlOutcomeUnknown，而 stop_until 把它当成"已停止"不再重发，
    // 于是本会话按 Transient 自动重建、group 在 ShuttingDown 下继续消费，shutdown 只能超时。
    let had_deferred_stop = deferred_stop.is_some();
    if let Some(command) = deferred_stop.take() {
        let result = if dlt_pending.is_empty() {
            Ok(CommandReply::Unit)
        } else {
            Err(NafkaError::Lifecycle(
                "owner 会话在 DLT 收尾完成前退出，死信未确认".into(),
            ))
        };
        OwnerHandle::finish_command(command, result);
    }
    for (_, pending) in dlt_pending {
        pending.cancel.store(true, Ordering::Release);
    }
    drop(dlt_admission);
    consumer.unsubscribe();
    if let Some(detail) = transient_exit {
        if had_deferred_stop {
            // 已经答应过停止：即使本次是瞬时错误也不能自动重建，否则 group 会在
            // ShuttingDown 之后继续消费。
            tracing::warn!(group = %plan.group, detail, "owner 在停止收尾期间遇到瞬时错误，直接结束会话");
            return OwnerSessionExit::Stopped;
        }
        return OwnerSessionExit::Transient(detail);
    }
    let crashed = health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .state
        == GroupState::Crashed;
    if crashed {
        OwnerSessionExit::Crashed
    } else {
        update_state(&health, GroupState::Stopped, None);
        OwnerSessionExit::Stopped
    }
}

/// 业务作用：在 Crashed 状态只接受 Restart 或 Stop，避免其他控制操作获得虚假成功。
///
/// # 参数
///
/// - `commands`: 保留下来的 owner 控制邮箱。
/// - `health`: 当前 Crashed 健康快照。
///
/// # 返回
///
/// 收到有效 Restart 时返回 true；Stop 或发送端断开时返回 false。
fn wait_for_crashed_command(
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    health: &Arc<RwLock<GroupHealth>>,
) -> bool {
    loop {
        let Ok(command) = commands.recv() else {
            update_state(health, GroupState::Stopped, None);
            return false;
        };
        if !OwnerHandle::begin_command(&command) {
            continue;
        }
        match &command.kind {
            OwnerCommandKind::Restart => {
                update_state(health, GroupState::Starting, None);
                OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                return true;
            }
            OwnerCommandKind::Stop => {
                update_state(health, GroupState::Stopped, None);
                OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                return false;
            }
            _ => OwnerHandle::reject_command(
                command,
                "group 已 Crashed，仅允许 restart_group 或 stop",
            ),
        }
    }
}

/// 业务作用：初始化 subscribe 或固定 assign。
///
/// # 参数
///
/// - `consumer`: owner 唯一 consumer。
/// - `mode`: 冻结订阅方式。
/// - `auto_offset_reset`: fixed Committed 无历史位点时的回退策略。
///
/// # 错误
///
/// 底层订阅或 assign 失败时返回错误。
fn initialize_owner(
    consumer: &ConsumerClient,
    mode: &OwnerMode,
    auto_offset_reset: &str,
) -> Result<()> {
    match mode {
        OwnerMode::Subscribe { topics } if topics.is_empty() => {
            consumer.unsubscribe();
            Ok(())
        }
        OwnerMode::Subscribe { topics } => consumer.subscribe(topics),
        OwnerMode::Assign { partitions, starts } => {
            if partitions.len() != starts.len()
                || partitions.iter().any(|tp| !starts.contains_key(tp))
            {
                return Err(NafkaError::Lifecycle(
                    "assign 分区与起始位点表不一致".into(),
                ));
            }
            consumer.assign(starts, auto_offset_reset)
        }
    }
}

/// 业务作用：把 consumer 构造/初始化错误集中映射为会话退出分类。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `group`: resolved group id。
/// - `error`: 公共错误域中的底层初始化错误。
fn classify_session_error(
    health: &Arc<RwLock<GroupHealth>>,
    group: &str,
    error: NafkaError,
) -> OwnerSessionExit {
    match error {
        NafkaError::Broker(message) => {
            update_state(health, GroupState::Recovering, Some(message.clone()));
            OwnerSessionExit::Transient(message)
        }
        NafkaError::GroupFatal { message, .. } => {
            update_state(health, GroupState::Crashed, Some(message));
            OwnerSessionExit::Crashed
        }
        other => {
            let message = format!("group={group}: {other}");
            update_state(health, GroupState::Crashed, Some(message));
            OwnerSessionExit::Crashed
        }
    }
}

/// 业务作用：启动延迟期间维持命令响应和 consumer 心跳。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `commands`: 命令队列。
/// - `health`: 健康快照。
/// - `delay`: 延迟时长。
///
/// # 返回
///
/// 收到 Stop 时返回 false。
fn delay_with_commands(
    consumer: &ConsumerClient,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    health: &Arc<RwLock<GroupHealth>>,
    delay: Duration,
) -> bool {
    let deadline = Instant::now() + delay;
    while Instant::now() < deadline {
        if let Ok(command) = commands.try_recv() {
            if matches!(command.kind, OwnerCommandKind::Stop) {
                // 副作用必须在 CAS 之内：调用方已超时并把命令置为 CANCELLED 时，
                // 保证「返回 ControlNotApplied 即绝不执行」，这里不能照样停掉 owner。
                if !OwnerHandle::begin_command(&command) {
                    continue;
                }
                OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                return false;
            }
            if OwnerHandle::begin_command(&command) {
                OwnerHandle::reject_command(command, "consumer 尚在启动延迟阶段");
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _ = consumer.poll(remaining.min(Duration::from_millis(50)));
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.last_poll_at = Some(SystemTime::now());
    }
    true
}

/// 业务作用：拉取一次有界 typed 逻辑批：首条等待，后续使用零等待 drain。
///
/// 达到字节上限时，已经被底层返回但尚未复制的边界记录会立即 seek 回原 offset，留给下一批；
/// 因而 owner 不需要持有一个额外的超上限 lookahead。
///
/// # 参数说明
///
/// - `consumer`: owner 唯一 consumer。
/// - `runtime`: 提供记录数、字节数与单记录上限。
/// - `health`: 最近 poll 时间的健康快照。
/// - `first_timeout`: 首条消息最长等待时间。
/// - `queued`: 已跨 poll 保留的输入；确实取到新记录后才建立身份索引，维护回退后不重复物化原文。
/// - `progress`、`assignment_epoch`: 当前处理证据及其派发权威，用于排除已经安全处理的重复 fetch。
///
/// # 返回
///
/// 返回受记录数和字节数约束的输入批；poll、边界 seek 或字节计量失败时返回错误。
fn collect_typed_batch(
    consumer: &ConsumerClient,
    runtime: &Arc<OwnerRuntime>,
    health: &Arc<RwLock<GroupHealth>>,
    first_timeout: Duration,
    queued: &BTreeMap<Tp, VecDeque<PreparedTypedItem>>,
    progress: &GroupOffsetState,
    assignment_epoch: u64,
) -> std::result::Result<LogicalTypedBatch, RdPollError> {
    let mut batch = LogicalTypedBatch {
        items: Vec::new(),
        retained_bytes: 0,
        _permits: Vec::new(),
    };
    let mut known: Option<BTreeSet<(Tp, i64)>> = None;
    loop {
        let timeout = if batch.items.is_empty() {
            first_timeout
        } else {
            Duration::ZERO
        };
        let Some(message) = consumer.poll(timeout)? else {
            break;
        };
        health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_poll_at = Some(SystemTime::now());
        let position_tp = Tp::new(message.topic(), message.partition());
        // 提交恢复会回退 native 位置，而本地批次可能已继续完成；先核对处理证据，
        // 防止已释放的完成票据令迟到 fetch 再次执行业务。人工定位会清除目标证据，
        // 失权会作废未提交结果，因此这里不会吞掉需要重新处理的失败或新会话输入。
        if progress.already_safe(&position_tp, assignment_epoch, message.offset()) {
            continue;
        }
        // 本地已保留坐标不能因维护回退再物化一份未独立计费的原文。
        if known
            .get_or_insert_with(|| {
                queued
                    .values()
                    .flat_map(|items| items.iter().map(PreparedTypedItem::location))
                    .collect()
            })
            .contains(&(position_tp.clone(), message.offset()))
        {
            continue;
        }
        if let Some(expected) = runtime
            .records
            .accept_dead_letter_position(&position_tp, message.offset())
        {
            halt_partition(
                consumer,
                health,
                &position_tp,
                HaltReason::ProgressInvariant,
                format!(
                    "DLT 原坐标 {expected} 未重新交付，实际位置 {}",
                    message.offset()
                ),
            );
            continue;
        }
        // headers 只解析一次：底层对非 UTF-8 header 名会 panic（由 RdMessage::headers 兜住），
        // 重复调用等于对同一条坏记录反复 panic/unwind，既有噪声也有额外开销。
        let headers = message.headers();
        let retained = message.retained_bytes_with(headers.as_ref().ok());
        if retained
            > runtime
                .config
                .behavior
                .max_record_bytes
                .min(runtime.records.maximum_record_bytes())
        {
            batch.items.push(TypedBatchItem::Oversize {
                tp: Tp::new(message.topic(), message.partition()),
                offset: message.offset(),
                retained_bytes: retained,
            });
        } else {
            let next_bytes = batch
                .retained_bytes
                .checked_add(retained)
                .ok_or_else(|| RdPollError::Fatal("typed 逻辑批字节计量溢出".into()))?;
            if !batch.items.is_empty() && next_bytes > runtime.config.behavior.max_batch_bytes {
                consumer
                    .seek_offsets(&BTreeMap::from([(
                        Tp::new(message.topic(), message.partition()),
                        message.offset(),
                    )]))
                    .map_err(|error| RdPollError::Transient(error.to_string()))?;
                break;
            }
            let tp = Tp::new(message.topic(), message.partition());
            let Some(permit) = runtime.records.retain(
                &tp,
                message.offset(),
                super::capacity::input_reservation_bytes(retained),
                message.key(),
            ) else {
                // 尚未物化或派发的边界记录保留重读位置；已有输入仍负责完成并释放共享额度。
                consumer
                    .seek_offsets(&BTreeMap::from([(tp.clone(), message.offset())]))
                    .map_err(|error| RdPollError::Transient(error.to_string()))?;
                enter_input_backpressure(consumer, health, &tp);
                break;
            };
            batch._permits.push(Arc::clone(&permit));
            batch.retained_bytes = next_bytes;
            batch
                .items
                .push(TypedBatchItem::Owned(OwnedTypedMessage::copy_from(
                    &message, &headers, permit,
                )));
            // 此处尚未跨过下一条原生记录：额度趋满时先处理当前批，避免仅因预取过快而回退并重复解压。
            // 这只是批次收口提示，周期性 poll 仍继续，其它 key/分区的实际准入始终重新检查各层额度。
            if !runtime.records.may_accept_similar(
                &tp,
                super::capacity::input_reservation_bytes(retained),
                message.key(),
            ) {
                break;
            }
        }
        if batch.items.len() >= runtime.config.behavior.max_batch_records
            || consumer.has_rebalance()
        {
            break;
        }
    }
    Ok(batch)
}

/// 业务作用：执行一个通过 fencing 的 owner 控制命令。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `mode`: 当前订阅方式，动态 subscribe 会替换它。
/// - `attempts`: owner 本地失败尝试表，seek/restart 时清理。
/// - `health`: 健康快照。
/// - `auto_offset_reset`: fixed Committed 重启时的无位点回退策略。
/// - `command`: 命令信封。
///
/// # 返回
///
/// Stop 命令返回 true。
fn execute_command(
    consumer: &ConsumerClient,
    mode: &mut OwnerMode,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    health: &Arc<RwLock<GroupHealth>>,
    auto_offset_reset: &str,
    command: OwnerCommand,
) -> bool {
    // 维护窗口也可以执行查询和暂停；最终 native 副作用前统一复验分配代次与目标归属。
    if let Some(error) = command_epoch_error(consumer, health, &command) {
        OwnerHandle::finish_command(command, Err(error));
        return false;
    }
    let stop = matches!(&command.kind, OwnerCommandKind::Stop);
    let command_id = command.id;
    let result = match &command.kind {
        OwnerCommandKind::Stop => {
            update_state(health, GroupState::Stopping, None);
            Ok(CommandReply::Unit)
        }
        OwnerCommandKind::Seek { offsets } => consumer.seek_control_offsets(offsets).map(|()| {
            clear_attempts(attempts, offsets.keys());
            clear_operator_pauses(consumer, health, offsets.keys());
            CommandReply::Unit
        }),
        OwnerCommandKind::SeekBeginning { tps } => {
            consumer.seek_special(tps, StartOffset::Earliest).map(|()| {
                clear_attempts(attempts, tps.iter());
                clear_operator_pauses(consumer, health, tps.iter());
                CommandReply::Unit
            })
        }
        OwnerCommandKind::SeekEnd { tps } => {
            consumer.seek_special(tps, StartOffset::Latest).map(|()| {
                clear_attempts(attempts, tps.iter());
                clear_operator_pauses(consumer, health, tps.iter());
                CommandReply::Unit
            })
        }
        // Committed 必须先查 broker 再 seek 绝对 offset：框架强制关闭本地 offset store
        //，底层 Offset::Stored 在本框架下恒为空。
        OwnerCommandKind::SeekCommitted { tps } => {
            consumer.seek_to_committed(tps, "none").map(|()| {
                clear_attempts(attempts, tps.iter());
                clear_operator_pauses(consumer, health, tps.iter());
                CommandReply::Unit
            })
        }
        OwnerCommandKind::SeekTimestamp { times } => consumer.seek_times(times).map(|()| {
            clear_attempts(attempts, times.keys());
            clear_operator_pauses(consumer, health, times.keys());
            CommandReply::Unit
        }),
        OwnerCommandKind::Pause { tps } => consumer.pause(tps).map(|targets| {
            set_user_paused(health, &targets, true);
            CommandReply::Unit
        }),
        OwnerCommandKind::Resume { tps } => {
            let requested = if tps.is_empty() {
                consumer.assignment()
            } else {
                Ok(tps.clone())
            };
            let targets = resumable_user_targets(consumer, health, tps);
            requested.and_then(|requested| {
                consumer.resume(&targets).map(|_| {
                    // 解除用户意图与 native 可恢复范围不同；内部保护仍有效，但不应留下已撤销的 User 原因。
                    set_user_paused(health, &requested, false);
                    CommandReply::Unit
                })
            })
        }
        OwnerCommandKind::Position { tps } => consumer.position(tps).map(CommandReply::Positions),
        OwnerCommandKind::Assignment => consumer.assignment().map(CommandReply::Assignment),
        OwnerCommandKind::Subscribe { topics, change } => match mode {
            OwnerMode::Subscribe { topics: current } => {
                use super::supervisor::SubscriptionChange;
                let mut desired: BTreeSet<String> = current.iter().cloned().collect();
                match change {
                    SubscriptionChange::Replace => desired = topics.iter().cloned().collect(),
                    SubscriptionChange::Add => desired.extend(topics.iter().cloned()),
                    SubscriptionChange::Remove => desired.retain(|topic| !topics.contains(topic)),
                }
                let desired: Vec<String> = desired.into_iter().collect();
                if desired == *current {
                    // 同集合不触发新 join，不能撤销 Ready 后等待一个不会发生的分配事件。
                    Ok(CommandReply::Unit)
                } else {
                    let result = if desired.is_empty() {
                        consumer.unsubscribe();
                        Ok(())
                    } else {
                        consumer.subscribe(&desired)
                    };
                    // native 接受修改后再更新恢复声明、撤销旧 Ready，最后才回报控制成功。
                    result.map(|()| {
                        *current = desired;
                        attempts.clear();
                        invalidate_subscription_health(health, current.is_empty());
                        CommandReply::Unit
                    })
                }
            }
            OwnerMode::Assign { .. } => Err(NafkaError::Lifecycle(
                "固定 assign group 不支持 subscribe_group".into(),
            )),
        },
        OwnerCommandKind::Unsubscribe => match mode {
            OwnerMode::Subscribe { topics } => {
                consumer.unsubscribe();
                topics.clear();
                attempts.clear();
                invalidate_subscription_health(health, true);
                Ok(CommandReply::Unit)
            }
            OwnerMode::Assign { .. } => Err(NafkaError::Lifecycle(
                "固定 assign group 不支持 unsubscribe_group".into(),
            )),
        },
        // 状态门已在 command_admission_error 裁决过；这里只负责执行。
        OwnerCommandKind::Restart => {
            initialize_owner(consumer, mode, auto_offset_reset).map(|()| {
                attempts.clear();
                let assignment = consumer.assignment().unwrap_or_default();
                let _ = consumer.resume(&assignment);
                // 只清框架自身的瞬时暂停；业务显式 pause 的分区不因运维 restart 被静默放开
                // （与自动会话重建的 reapply_persistent_pauses 保持同一语义）。
                {
                    let mut snapshot = health
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    snapshot
                        .paused
                        .retain(|(_, reason)| *reason == PauseReason::User);
                }
                reapply_persistent_pauses(consumer, health, &assignment);
                update_state(health, GroupState::Joining, None);
                CommandReply::Unit
            })
        }
    };
    OwnerHandle::finish_command(command, result);
    tracing::debug!(command_id, "consumer owner 控制命令已完成");
    stop
}

/// 业务作用：订阅控制成功后先撤销旧 Ready，再交付控制回执。
/// 参数说明：health 为共享快照，empty 表示已退订全部 topic。
/// 返回：旧分配不再满足接流量条件；后续原生分配事件才能重新开放。
fn invalidate_subscription_health(health: &Arc<RwLock<GroupHealth>>, empty: bool) {
    // native 接受订阅意图不等于完成 join；旧快照不能在控制成功后继续授权接流量。
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.ready_assignment_epoch = None;
    snapshot.state = GroupState::Joining;
    snapshot.last_error = None;
    if empty {
        snapshot.assignment.clear();
    }
}

/// 业务作用：在产生任何副作用之前裁决一条控制命令能否执行。
///
/// 全部拒绝理由必须集中在这里： 的契约是"返回错误即未执行"，因此裁决必须早于
/// commit、cancel_retry_states、cancel_dlt_states 和 progress.clear() 这些破坏性动作。
///
/// # 参数
///
/// - `kind`: 待执行命令。
/// - `mode`: 当前订阅方式；固定 assign 不接受动态订阅命令。
/// - `health`: 共享健康快照；restart 的状态门读它。
/// - `pending`: 已准入的每分区 DLT worker。
/// - `admission`: 各分区的 DLT 准入等待。
///
/// # 返回
///
/// 可以执行时返回 `None`；否则返回应原样回给调用方的错误。
fn command_admission_error(
    kind: &OwnerCommandKind,
    mode: &OwnerMode,
    health: &Arc<RwLock<GroupHealth>>,
    pending: &BTreeMap<Tp, PendingDlt>,
    admission: &BTreeMap<Tp, PendingDltAdmission>,
) -> Option<NafkaError> {
    if seek_conflicts_with_dlt(kind, pending, admission) {
        return Some(NafkaError::Lifecycle(
            "存在 in-flight DLT，seek 被拒绝以保护 durability-first 结果".into(),
        ));
    }
    match kind {
        OwnerCommandKind::Subscribe { .. } if matches!(mode, OwnerMode::Assign { .. }) => Some(
            NafkaError::Lifecycle("固定 assign group 不支持 subscribe_group".into()),
        ),
        OwnerCommandKind::Unsubscribe if matches!(mode, OwnerMode::Assign { .. }) => Some(
            NafkaError::Lifecycle("固定 assign group 不支持 unsubscribe_group".into()),
        ),
        OwnerCommandKind::Restart => {
            let state = health
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .state;
            (!state.permits_operator_restart()).then(|| {
                NafkaError::Lifecycle(format!(
                    "group 处于 {state:?}，restart_group 仅允许 {}",
                    GroupState::operator_restart_states()
                ))
            })
        }
        _ => None,
    }
}

/// 业务作用：阻止组级路由后的命令作用于已经转移的分区或后来的 assignment。
/// 参数说明：consumer 为唯一 native owner；health 为本槽观测；command 为带入队代次的命令。
/// 返回：权威和归属都匹配时 None，否则返回确定未执行，不向其它槽自动重试。
fn command_epoch_error(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    command: &OwnerCommand,
) -> Option<NafkaError> {
    let targets = super::group::partition_targets(&command.kind)?;
    let current = health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .ready_assignment_epoch;
    if command.expected_epoch.is_none()
        || current != command.expected_epoch
        || consumer.has_rebalance()
        || consumer.assignment_lost()
    {
        return Some(NafkaError::ControlOwnerChanged(command.id));
    }
    match consumer.assignment() {
        Ok(assigned) if targets.iter().all(|tp| assigned.contains(tp)) => None,
        _ => Some(NafkaError::ControlOwnerChanged(command.id)),
    }
}

/// 业务作用：判断 seek 是否会覆盖尚未得到 durability-first 终态的 DLT 分区。
///
/// - `kind`: 待执行控制命令；只检查五种 seek。
/// - `pending`: 已准入并由 worker 投递的 DLT。
/// - `admission`: 尚在等待源级 permit 的各分区 DLT。
fn seek_conflicts_with_dlt(
    kind: &OwnerCommandKind,
    pending: &BTreeMap<Tp, PendingDlt>,
    admission: &BTreeMap<Tp, PendingDltAdmission>,
) -> bool {
    let targets = match kind {
        OwnerCommandKind::Seek { offsets } => Some(offsets.keys().collect::<BTreeSet<_>>()),
        OwnerCommandKind::SeekBeginning { tps }
        | OwnerCommandKind::SeekEnd { tps }
        | OwnerCommandKind::SeekCommitted { tps } => Some(tps.iter().collect::<BTreeSet<_>>()),
        OwnerCommandKind::SeekTimestamp { times } => Some(times.keys().collect::<BTreeSet<_>>()),
        _ => return false,
    };
    let targets = targets.expect("seek 分支必须提供目标集合");
    let all = targets.is_empty();
    pending.keys().any(|tp| all || targets.contains(tp))
        || admission.keys().any(|tp| all || targets.contains(tp))
}

/// 业务作用：清理指定分区的本地失败尝试状态。
///
/// # 参数
///
/// - `attempts`: owner 尝试表。
/// - `tps`: 已被 operator 改变位置的分区。
fn clear_attempts<'a>(
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    tps: impl IntoIterator<Item = &'a Tp>,
) {
    let targets: BTreeSet<Tp> = tps.into_iter().cloned().collect();
    attempts.retain(|(tp, _), _| !targets.contains(tp));
}

/// 业务作用：operator seek 后移除框架安全暂停，并在没有 User 原因时恢复底层分区。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `health`: 健康快照。
/// - `tps`: 已显式改变位置的分区。
fn clear_operator_pauses<'a>(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    tps: impl IntoIterator<Item = &'a Tp>,
) {
    let targets: BTreeSet<Tp> = tps.into_iter().cloned().collect();
    let resumable = {
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot
            .paused
            .retain(|(tp, reason)| !targets.contains(tp) || *reason == PauseReason::User);
        targets
            .iter()
            .filter(|tp| !snapshot.paused.iter().any(|(current, _)| current == *tp))
            .cloned()
            .collect::<Vec<_>>()
    };
    let _ = consumer.resume(&resumable);
}

/// 业务作用：只返回移除 User 原因后没有任何框架暂停原因的分区。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `health`: 健康快照。
/// - `requested`: 调用方目标；空列表表示当前 assignment。
fn resumable_user_targets(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    requested: &[Tp],
) -> Vec<Tp> {
    let targets = if requested.is_empty() {
        consumer.assignment().unwrap_or_default()
    } else {
        requested.to_vec()
    };
    let snapshot = health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    targets
        .into_iter()
        .filter(|tp| {
            !snapshot
                .paused
                .iter()
                .any(|(current, reason)| current == tp && *reason != PauseReason::User)
        })
        .collect()
}

/// 业务作用：把会话内 epoch 换算成跨会话单调的对外 epoch。
///
/// 内部 epoch 只用于同一底层会话内的 fencing，随 consumer 重建从 0 起；
/// 对外快照必须单调，否则调用方无法区分"同一 assignment"与"重建后的新 generation"。
///
/// # 参数
///
/// - `base`: 本 owner 线程累积的基线。
/// - `session_epoch`: 当前会话内 epoch。
fn external_epoch(base: &std::sync::atomic::AtomicU64, session_epoch: u64) -> u64 {
    let value = base
        .load(std::sync::atomic::Ordering::Acquire)
        .saturating_add(session_epoch);
    // 立刻把已经对外发布过的值写回基线：会话若因 panic 被 catch_unwind 截断，
    // 收尾代码不会执行，下一代会话就会从更小的值重新发布，让对外 epoch 倒退。
    base.fetch_max(value, std::sync::atomic::Ordering::AcqRel);
    value
}

/// group 仪表重采样间隔，限制标签快照与指标队列准入的频率。
const GAUGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// 业务作用：发布一次真实 assign 后的健康与 ready 快照。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `assignment`: callback 或固定 assign 给出的分区。
/// - `epoch`: 本次 assignment epoch。
fn publish_assignment_health(
    runtime: &Arc<OwnerRuntime>,
    group: &str,
    health: &Arc<RwLock<GroupHealth>>,
    assignment: &[Tp],
    epoch: u64,
) {
    {
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.assignment = assignment.to_vec();
        snapshot.ready_assignment_epoch = Some(epoch);
        snapshot.last_assignment_at = Some(SystemTime::now());
        snapshot.state = GroupState::Running;
        snapshot.last_error = None;
    }
    // ready 必须由 owner 的真实状态驱动。若只在 await_group_ready 里写，典型用法是启动时
    // 调一次，之后该 gauge 就永远停在 1——group 已 Crashed 或丢掉 assignment 也看不出来，
    // 比没有这个指标更危险。
    // 先释放健康锁再构造指标快照，避免观测准入延长控制面状态锁的占用。
    publish_health_gauges(runtime, group, health);
}

/// 业务作用：从 health 快照采样一次并发布 group 级仪表。
///
/// ready 租约与暂停分区统一从同一健康快照采样，使撤销、重新加入和暂停变化都反映到指标。
///
/// 语义定义（写在这里是因为它必须唯一）：
/// - `group_ready` = 全部槽持有有效 assignment 权威且处于 Running。空 assignment 也可算 1，
///   那是竞争组 standby 的合法形态；要区分 standby 看 `assigned_partitions`。
/// - `paused_partitions` 数的是**分区数**不是暂停行数：同一分区可同时挂
///   `User` 与 `Halt` 两行，按行数会翻倍。
///
/// # 参数
///
/// - `runtime`: 指标出口。
/// - `group`: group.id 标签。
/// - `health`: 共享健康快照。
///
/// 返回：无；读取快照后释放锁，再向有界出口提交仪表，用户代码仅在导出线程执行。
fn publish_health_gauges(
    runtime: &Arc<OwnerRuntime>,
    group: &str,
    health: &Arc<RwLock<GroupHealth>>,
) {
    // 先释放健康锁，再向组级聚合和有界观测队列传递快照。
    let snapshot = health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    runtime.publish_health(group, snapshot);
}

/// 业务作用：撤销旧底层会话发布的 ready 快照。
///
/// 参数说明：
/// - `runtime`: 提供指标出口的冻结资源。
/// - `group`: 失去会话权威的消费组。
/// - `health`: 跨会话共享的 group 健康状态。
///
/// 返回：无；只在实际调用已排空后清除旧 assignment 与容量观测。
fn invalidate_ready_snapshot(
    runtime: &Arc<OwnerRuntime>,
    group: &str,
    health: &Arc<RwLock<GroupHealth>>,
) {
    {
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.assignment.clear();
        snapshot.ready_assignment_epoch = None;
        snapshot.active_handlers = 0;
        snapshot.key_active_handlers = 0;
        snapshot.key_pending_records = 0;
        snapshot.key_pending_bytes = 0;
        snapshot.last_assignment_at = Some(SystemTime::now());
    }
    publish_health_gauges(runtime, group, health);
}

/// 业务作用：把跨 rebalance/会话保留的 User/Halt 事实重新下发到底层 consumer。
///
/// - `consumer`: 当前新建或刚完成 assignment 的 owner consumer。
/// - `health`: 保存 operator 暂停事实的共享快照。
/// - `assignment`: 当前实际拥有的分区；不向已 revoke 分区下发 pause。
fn reapply_persistent_pauses(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    assignment: &[Tp],
) {
    let assigned = assignment.iter().collect::<BTreeSet<_>>();
    let targets = health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .paused
        .iter()
        .filter(|(tp, reason)| {
            assigned.contains(tp) && matches!(reason, PauseReason::User | PauseReason::Halt(_))
        })
        .map(|(tp, _)| tp.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if !targets.is_empty() {
        let _ = consumer.pause(&targets);
    }
}

/// 业务作用：应用 owner mailbox 中的一次实际 rebalance 结果。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `group`: resolved group id。
/// - `runtime`: 冻结配置来源。
/// - `health`: group 健康快照。
/// - `assignment`: owner 当前 assignment。
/// - `epoch`: owner 当前 epoch。
/// - `progress`: 连续水位状态。
/// - `attempts`: 进程内重试计数。
/// - `retries`: 每分区非阻塞退避状态。
/// - `dlt_pending`: 每分区已准入 DLT worker。
/// - `dlt_admission`: owner-local DLT 准入任务。
/// - `pending_ready_epoch`: assignment 后等待一次 position 初始化 poll 的 epoch。
/// - `event`: callback 合并事件。
///
/// # 返回
///
/// assignment 超限等致命错误时返回 false。
#[allow(clippy::too_many_arguments)]
fn apply_rebalance(
    consumer: &ConsumerClient,
    group: &str,
    runtime: &Arc<OwnerRuntime>,
    health: &Arc<RwLock<GroupHealth>>,
    assignment: &mut Vec<Tp>,
    epoch: &mut u64,
    progress: &mut GroupOffsetState,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    dlt_pending: &mut BTreeMap<Tp, PendingDlt>,
    dlt_admission: &mut BTreeMap<Tp, PendingDltAdmission>,
    pending_ready_epoch: &mut Option<u64>,
    event: RdRebalanceEvent,
) -> bool {
    // 分配变更会丢弃旧本地队列，保留分区的重试重新依据 broker 坐标恢复。
    for state in retries.values_mut() {
        state.in_place = false;
    }
    match event.kind {
        RdRebalanceKind::Assigned => {
            runtime.counter("rebalance_assigned_total", 1, &[("group", group)]);
            let max = runtime.config.consumer.max_assignment_partitions;
            // 全组并集准入先于 Ready 与本地权威发布，容量拒绝时关闭本槽业务路由。
            if let Err(actual) = runtime.metadata.assign(runtime.slot, &event.partitions) {
                let _ = consumer.unassign();
                update_state(
                    health,
                    GroupState::Crashed,
                    Some(
                        NafkaError::AssignmentTooLarge {
                            group: group.to_owned(),
                            actual,
                            max,
                        }
                        .to_string(),
                    ),
                );
                return false;
            }
            *assignment = event.partitions;
            *epoch = event.epoch;
            if let Err(error) = consumer
                .prime_assignment_positions(assignment, &runtime.config.consumer.auto_offset_reset)
            {
                update_state(
                    health,
                    GroupState::Crashed,
                    Some(format!("assignment 初始位置解析失败: {error}")),
                );
                return false;
            }
            progress.reset_for_assignment(assignment);
            runtime.records.clear(None);
            publish_commit_snapshot(consumer, progress);
            let assigned: BTreeSet<&Tp> = assignment.iter().collect();
            // librdkafka 的 pause 是 consumer-local 状态；同一分区在 rebalance 后重新
            // assign 时，旧 epoch 的 CommitBlocked/DLTBackpressure 等瞬时暂停仍可能残留。
            // 先把新 assignment 恢复为可拉取基线，再在下方重放 User/Halt 与有效 retry，
            // 避免 health 已清空瞬时原因、底层却永久保持 paused 的状态分裂。
            if let Err(error) = consumer.resume(assignment) {
                update_state(
                    health,
                    GroupState::Crashed,
                    Some(format!("assignment 清理旧 epoch 暂停状态失败: {error}")),
                );
                return false;
            }
            health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .paused
                .retain(|(tp, reason)| {
                    assigned.contains(tp)
                        && matches!(reason, PauseReason::User | PauseReason::Halt(_))
                });
            reapply_persistent_pauses(consumer, health, assignment);
            attempts.retain(|(tp, _), _| assigned.contains(tp));
            retries.retain(|tp, _| assigned.contains(tp));
            // completion 只授权调度时的代次；新分配关闭旧任务的新发送并从 committed 重新交付。
            // 旧任务已经发出的请求继续收口，允许重复死信，但旧完成不能推进新代次来源位点。
            let stale_dlt = dlt_pending
                .iter()
                .filter(|(tp, pending)| {
                    !assigned.contains(tp) || pending.assignment_epoch != event.epoch
                })
                .map(|(tp, _)| tp.clone())
                .collect::<Vec<_>>();
            for tp in stale_dlt {
                if let Some(pending) = dlt_pending.remove(&tp) {
                    pending.cancel.store(true, Ordering::Release);
                }
            }
            // 新分配已回到 broker 起点，旧等待意图不能自行开放新权威。
            dlt_admission.retain(|_, pending| pending.assignment_epoch == event.epoch);
            for (tp, state) in retries.iter() {
                let _ = consumer.pause(std::slice::from_ref(tp));
                let _ = consumer.seek_offsets(&BTreeMap::from([(tp.clone(), state.retry_from)]));
                add_pause_reason(health, std::slice::from_ref(tp), PauseReason::RetryBackoff);
            }
            // 写锁必须在调用 update_state 之前释放：std RwLock 不可重入，
            // 在持有写锁时再 write() 会直接死锁（owner 线程挂死且**握着** health 写锁，
            // 随后每个 group_health()/await_group_ready()/shutdown() 全部阻塞）。
            {
                let mut snapshot = health
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                snapshot.assignment = assignment.clone();
                snapshot.ready_assignment_epoch = None;
                snapshot.last_assignment_at = Some(SystemTime::now());
                snapshot.state = GroupState::Joining;
                snapshot.last_error = None;
            }
            *pending_ready_epoch = Some(*epoch);
            if !retries.is_empty() {
                update_state(health, GroupState::Retrying, None);
            }
            true
        }
        RdRebalanceKind::Revoked => {
            if !event.committed.is_empty() {
                let _ = progress.confirm_committed(&event.committed);
            }
            if let Some(error) = &event.error {
                tracing::warn!(group, error, "revoke 安全前沿提交未满足预算");
            }
            let revoked: BTreeSet<Tp> = event.partitions.into_iter().collect();
            assignment.retain(|tp| !revoked.contains(tp));
            let _ = runtime.metadata.assign(runtime.slot, assignment);
            for tp in &revoked {
                progress.reset_partition(tp);
            }
            runtime
                .records
                .clear(Some(&revoked.iter().cloned().collect::<Vec<_>>()));
            publish_commit_snapshot(consumer, progress);
            attempts.retain(|(tp, _), _| !revoked.contains(tp));
            retries.retain(|tp, _| !revoked.contains(tp));
            // 组代次已改变，保留分区也不能接纳旧 DLT completion；先撤销新发送，再恢复未确认坐标。
            // 已发送请求继续持有信封与并行额度直到真实终态，来源重投允许产生重复死信。
            let mut dlt_rewind = BTreeMap::new();
            for (tp, pending) in std::mem::take(dlt_pending) {
                pending.cancel.store(true, Ordering::Release);
                if !revoked.contains(&tp) {
                    dlt_rewind.insert(tp, pending.first_offset);
                }
            }
            for (_, pending) in std::mem::take(dlt_admission) {
                if !revoked.contains(&pending.tp) {
                    dlt_rewind.insert(pending.tp, pending.offsets[0]);
                }
            }
            if !dlt_rewind.is_empty() {
                for (tp, offset) in &dlt_rewind {
                    if runtime.records.dead_letter_intent(tp, *offset).is_some() {
                        runtime.records.expect_dead_letter(tp, *offset);
                    }
                }
                let failed = rewind_input_offsets(runtime, consumer, health, &dlt_rewind);
                halt_failed_rewinds(consumer, health, &failed);
            }
            // 这些门禁由已作废的任务拥有；只解除 DLT 原因，用户暂停与 Halt 继续阻止业务。
            for reason in [
                PauseReason::DltPending,
                PauseReason::DltRetryDegraded,
                PauseReason::DltBackpressure,
            ] {
                let resumable = clear_pause_reason(health, assignment, &reason);
                let _ = consumer.resume(&resumable);
            }
            // cooperative 撤权不保证后续出现 Assigned；保留分区的提交和进度门禁必须在此复位。
            // 仅清理这些框架瞬态原因，其它保护仍独立阻止 native 取入。
            let retained = assignment.clone();
            let stale_transient = {
                let snapshot = health
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                snapshot
                    .paused
                    .iter()
                    .filter(|(tp, reason)| {
                        retained.contains(tp)
                            && matches!(
                                reason,
                                PauseReason::CommitBlocked | PauseReason::ProgressBackpressure
                            )
                    })
                    .map(|(tp, _)| tp.clone())
                    .collect::<Vec<_>>()
            };
            if !stale_transient.is_empty() {
                let resumable =
                    clear_pause_reason(health, &stale_transient, &PauseReason::CommitBlocked);
                let mut resumable = resumable;
                resumable.extend(clear_pause_reason(
                    health,
                    &stale_transient,
                    &PauseReason::ProgressBackpressure,
                ));
                resumable.sort();
                resumable.dedup();
                let _ = consumer.resume(&resumable);
            }
            health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .paused
                .retain(|(tp, reason)| {
                    !revoked.contains(tp)
                        || matches!(reason, PauseReason::User | PauseReason::Halt(_))
                });
            *epoch = event.epoch;
            // cooperative 只撤权时也要由下一次成功 poll 重建保留分区的 Ready 证据。
            *pending_ready_epoch = (!assignment.is_empty()).then_some(*epoch);
            let mut snapshot = health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            snapshot.assignment = assignment.clone();
            snapshot.ready_assignment_epoch = None;
            snapshot.last_assignment_at = Some(SystemTime::now());
            snapshot.state = GroupState::Joining;
            true
        }
        RdRebalanceKind::Error => {
            *pending_ready_epoch = None;
            update_state(
                health,
                GroupState::Degraded,
                event.error.or_else(|| Some("rebalance 失败".into())),
            );
            true
        }
    }
}

/// 业务作用：判断控制命令是否会让当前本地 progress 失效。
///
/// # 参数
///
/// - `kind`: 待执行命令。
fn command_invalidates_progress(kind: &OwnerCommandKind) -> bool {
    matches!(
        kind,
        OwnerCommandKind::Stop
            | OwnerCommandKind::Seek { .. }
            | OwnerCommandKind::SeekBeginning { .. }
            | OwnerCommandKind::SeekEnd { .. }
            | OwnerCommandKind::SeekCommitted { .. }
            | OwnerCommandKind::SeekTimestamp { .. }
            | OwnerCommandKind::Subscribe { .. }
            | OwnerCommandKind::Unsubscribe
            | OwnerCommandKind::Restart
    )
}

/// 业务作用：控制命令改变 position/assignment 前清理其覆盖范围内的退避状态。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `retries`: owner 每分区退避表。
/// - `kind`: 即将执行的控制命令。
fn cancel_retry_states(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    kind: &OwnerCommandKind,
) {
    let targets = match kind {
        OwnerCommandKind::Seek { offsets } => {
            Some(offsets.keys().cloned().collect::<BTreeSet<_>>())
        }
        OwnerCommandKind::SeekBeginning { tps }
        | OwnerCommandKind::SeekEnd { tps }
        | OwnerCommandKind::SeekCommitted { tps } => {
            Some(tps.iter().cloned().collect::<BTreeSet<_>>())
        }
        OwnerCommandKind::SeekTimestamp { times } => {
            Some(times.keys().cloned().collect::<BTreeSet<_>>())
        }
        OwnerCommandKind::Stop
        | OwnerCommandKind::Subscribe { .. }
        | OwnerCommandKind::Unsubscribe
        | OwnerCommandKind::Restart => None,
        OwnerCommandKind::Pause { .. }
        | OwnerCommandKind::Resume { .. }
        | OwnerCommandKind::Position { .. }
        | OwnerCommandKind::Assignment => return,
    };
    let cancelled = match &targets {
        Some(targets) => {
            let hit = retries
                .keys()
                .filter(|tp| targets.contains(*tp))
                .cloned()
                .collect::<Vec<_>>();
            retries.retain(|tp, _| !targets.contains(tp));
            hit
        }
        None => {
            let hit = retries.keys().cloned().collect::<Vec<_>>();
            retries.clear();
            hit
        }
    };
    // 取消 RetryBackoff 必须连底层 pause 一起解除：activate_due_retries 遍历的是刚被清空的
    // retries 表，不 resume 就再没有任何路径会恢复这些分区，且 health 里也不再有记录
    // ——控制面显示"未暂停"而数据面永久停流。
    let resumable = clear_pause_reason(health, &cancelled, &PauseReason::RetryBackoff);
    if !resumable.is_empty() {
        let _ = consumer.resume(&resumable);
    }
}

/// 业务作用：position/assignment 控制命令覆盖旧处理事实前取消对应 DLT worker 与 local pending。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `pending`: 已准入的每分区 DLT worker。
/// - `admission`: owner-local 准入任务。
/// - `kind`: 即将执行的控制命令。
fn cancel_dlt_states(
    _runtime: &OwnerRuntime,
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    pending: &mut BTreeMap<Tp, PendingDlt>,
    admission: &mut BTreeMap<Tp, PendingDltAdmission>,
    kind: &OwnerCommandKind,
) {
    let targets = match kind {
        OwnerCommandKind::Seek { offsets } => {
            Some(offsets.keys().cloned().collect::<BTreeSet<_>>())
        }
        OwnerCommandKind::SeekBeginning { tps }
        | OwnerCommandKind::SeekEnd { tps }
        | OwnerCommandKind::SeekCommitted { tps } => {
            Some(tps.iter().cloned().collect::<BTreeSet<_>>())
        }
        OwnerCommandKind::SeekTimestamp { times } => {
            Some(times.keys().cloned().collect::<BTreeSet<_>>())
        }
        OwnerCommandKind::Subscribe { .. }
        | OwnerCommandKind::Unsubscribe
        | OwnerCommandKind::Restart => None,
        OwnerCommandKind::Stop
        | OwnerCommandKind::Pause { .. }
        | OwnerCommandKind::Resume { .. }
        | OwnerCommandKind::Position { .. }
        | OwnerCommandKind::Assignment => return,
    };
    let cancelled = pending
        .keys()
        .filter(|tp| targets.as_ref().is_none_or(|targets| targets.contains(*tp)))
        .cloned()
        .collect::<Vec<_>>();
    for tp in &cancelled {
        if let Some(task) = pending.remove(tp) {
            task.cancel.store(true, Ordering::Release);
        }
    }
    let gate_cleared = admission
        .keys()
        .filter(|tp| targets.as_ref().is_none_or(|targets| targets.contains(*tp)))
        .cloned()
        .collect::<Vec<_>>();
    for tp in &gate_cleared {
        admission.remove(tp);
    }
    // 摘掉 health 的 DLT 暂停行必须同步 resume，否则控制面显示未暂停而分区在底层永久停流。
    let mut resumable = clear_pause_reason(health, &cancelled, &PauseReason::DltPending);
    resumable.extend(clear_pause_reason(
        health,
        &gate_cleared,
        &PauseReason::DltPending,
    ));
    resumable.extend(clear_pause_reason(
        health,
        &cancelled,
        &PauseReason::DltRetryDegraded,
    ));
    if !gate_cleared.is_empty() {
        resumable.extend(clear_pause_reason(
            health,
            &gate_cleared,
            &PauseReason::DltBackpressure,
        ));
    }
    resumable.sort();
    resumable.dedup();
    if !resumable.is_empty() {
        let _ = consumer.resume(&resumable);
    }
}

/// 业务作用：记录安全处理或提交进展，保留仍生效的分区保护和会话关闭状态。
/// 参数说明：health 为 group 健康快照。
/// 返回：更新最近成功时间；其它分区成功或旧安全前缀提交不能解除硬暂停、失权或停止。
fn note_success(health: &Arc<RwLock<GroupHealth>>) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.last_success_at = Some(SystemTime::now());
    // 成功仅证明该项责任完成，不代表其它分区门禁已经解除；停机与致命状态更不能被覆盖。
    if matches!(
        snapshot.state,
        GroupState::Stopping | GroupState::Stopped | GroupState::Crashed | GroupState::Recovering
    ) || snapshot.ready_assignment_epoch.is_none()
    {
        return;
    }
    snapshot.state =
        if snapshot.paused.iter().any(|(_, reason)| {
            matches!(reason, PauseReason::Halt(_) | PauseReason::DltRetryDegraded)
        }) {
            GroupState::Degraded
        } else if snapshot
            .paused
            .iter()
            .any(|(_, reason)| *reason == PauseReason::CommitBlocked)
        {
            GroupState::CommitBlocked
        } else if snapshot
            .paused
            .iter()
            .any(|(_, reason)| *reason == PauseReason::DltBackpressure)
        {
            GroupState::DltBackpressure
        } else if snapshot.paused.iter().any(|(_, reason)| {
            matches!(
                reason,
                PauseReason::ProgressBackpressure | PauseReason::InputBackpressure
            )
        }) {
            GroupState::ProgressBackpressure
        } else if snapshot
            .paused
            .iter()
            .any(|(_, reason)| *reason == PauseReason::RetryBackoff)
        {
            GroupState::Retrying
        } else {
            snapshot.last_error = None;
            GroupState::Running
        };
}

/// 业务作用：累计一次底层 consumer 会话创建；首次之后的尝试同时计入重建次数。
///
/// # 参数
///
/// - `health`: group 健康快照。
fn note_owner_session_start(health: &Arc<RwLock<GroupHealth>>) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.owner_session_started = snapshot.owner_session_started.saturating_add(1);
    if snapshot.owner_session_started > 1 {
        snapshot.owner_session_restarted = snapshot.owner_session_restarted.saturating_add(1);
    }
}

/// 业务作用：累计一次真实 broker commit 结果，供长稳与吞吐门禁计算 commits/record。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `succeeded`: broker 是否确认本次 commit 成功。
fn note_commit_result(health: &Arc<RwLock<GroupHealth>>, succeeded: bool) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.commit_attempted = snapshot.commit_attempted.saturating_add(1);
    if succeeded {
        snapshot.commit_succeeded = snapshot.commit_succeeded.saturating_add(1);
    } else {
        snapshot.commit_failed = snapshot.commit_failed.saturating_add(1);
    }
}

/// 业务作用：把 owner 当前安全前沿发布给 pre-rebalance callback。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `progress`: 当前连续水位状态。
fn publish_commit_snapshot(consumer: &ConsumerClient, progress: &GroupOffsetState) {
    if let Ok(offsets) = progress.committable_offsets() {
        consumer.set_rebalance_offsets(&offsets);
    }
}

/// 业务作用：记录维护性 poll 的会话级错误，并保留自动重建或显式恢复所需分类。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `transient_exit`: 当前会话待上报的临时退出原因。
/// - `error`: 底层集中分类后的 poll 错误。
fn record_maintenance_poll_error(
    health: &Arc<RwLock<GroupHealth>>,
    transient_exit: &mut Option<String>,
    error: RdPollError,
) {
    match error {
        RdPollError::Transient(message) => {
            update_state(health, GroupState::Recovering, Some(message.clone()));
            *transient_exit = Some(message);
        }
        RdPollError::Fatal(message) => {
            update_state(health, GroupState::Crashed, Some(message));
        }
    }
}

/// 业务作用：同步提交当前全部安全前沿；失败期间停止业务派发并维护心跳。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `progress`: 待提交且提交成功前不可裁剪的状态。
/// - `health`: group 健康快照。
/// - `commands`: 控制命令队列。
/// - `mode`: 当前订阅方式。
/// - `attempts`: 失败尝试表。
/// - `last_commit`: 最近一次成功提交时间。
/// - `backoff_ms`: commit 失败重试退避。
/// - `transient_exit`: 维护性 poll 捕获的临时会话错误。
///
/// # 返回
///
/// CommitBlocked 期间收到 Stop 时返回 true。
#[allow(clippy::too_many_arguments)]
fn commit_until_resolved(
    consumer: &ConsumerClient,
    progress: &mut GroupOffsetState,
    health: &Arc<RwLock<GroupHealth>>,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    mode: &mut OwnerMode,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    last_commit: &mut Instant,
    backoff_ms: u64,
    transient_exit: &mut Option<String>,
    dlt_outstanding: bool,
    deferred_stop: &mut Option<OwnerCommand>,
) -> bool {
    // 单一出口：无论从哪个分支返回，维护性 poll 取走的记录都必须回退，
    // 否则它们的 fetch position 已前移却无人处理，会被后续安全前沿提交越过。
    let mut discarded: BTreeMap<Tp, i64> = BTreeMap::new();
    let stopping = commit_until_resolved_inner(
        consumer,
        progress,
        health,
        commands,
        mode,
        attempts,
        last_commit,
        backoff_ms,
        transient_exit,
        dlt_outstanding,
        deferred_stop,
        &mut discarded,
    );
    if !discarded.is_empty() {
        let failed = consumer.rewind_offsets(&discarded);
        halt_failed_rewinds(consumer, health, &failed);
    }
    stopping
}

/// 业务作用：[`commit_until_resolved`] 的实际循环体；维护性 poll 取出的记录记入 `discarded`。
#[allow(clippy::too_many_arguments)]
fn commit_until_resolved_inner(
    consumer: &ConsumerClient,
    progress: &mut GroupOffsetState,
    health: &Arc<RwLock<GroupHealth>>,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    mode: &mut OwnerMode,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    last_commit: &mut Instant,
    backoff_ms: u64,
    transient_exit: &mut Option<String>,
    dlt_outstanding: bool,
    deferred_stop: &mut Option<OwnerCommand>,
    discarded: &mut BTreeMap<Tp, i64>,
) -> bool {
    let mut blocked = false;
    loop {
        let offsets = match progress.committable_offsets() {
            Ok(offsets) => offsets,
            Err(error) => {
                update_state(health, GroupState::Crashed, Some(error.to_string()));
                return true;
            }
        };
        if offsets.is_empty() {
            return false;
        }
        if consumer.assignment_lost() {
            progress.clear();
            publish_commit_snapshot(consumer, progress);
            update_state(
                health,
                GroupState::Joining,
                Some("assignment 已丢失，放弃旧 epoch commit".into()),
            );
            return false;
        }
        let group = health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .group
            .clone();
        let commit_started = Instant::now();
        let span = tracing::info_span!(
            "kafka.commit",
            group = %group,
            partition_count = offsets.len(),
            latency_ms = tracing::field::Empty
        );
        let commit_result = span.in_scope(|| consumer.commit_offsets(&offsets));
        span.record(
            "latency_ms",
            u64::try_from(commit_started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        consumer.counter("commit_attempted_total", 1);
        match commit_result {
            Ok(()) => {
                note_commit_result(health, true);
                consumer.counter("commit_succeeded_total", 1);
                if let Err(error) = progress.confirm_committed(&offsets) {
                    update_state(health, GroupState::Crashed, Some(error.to_string()));
                    return true;
                }
                publish_commit_snapshot(consumer, progress);
                *last_commit = Instant::now();
                if blocked {
                    // commit 失败期间维护性 poll 可能把本地 position 前移；成功后回到安全前沿再恢复。
                    if let Err(error) = consumer.seek_offsets(&offsets) {
                        update_state(health, GroupState::Crashed, Some(error.to_string()));
                        return true;
                    }
                    // 把安全前沿并进 discarded 并取 min：否则外层单一出口会拿"维护性 poll 观测到的
                    // offset"再 seek 一次，而该值恒 >= 前沿（前沿是连续已 ack 前缀，position 只要
                    // 有待 ack 记录就更靠前）。那一步会把分区**向前**seek，抹掉这里刚做的回退，
                    // 前沿与它之间的记录永不重投——前沿冻结→ProgressBackpressure 永久停摆。
                    for (tp, offset) in &offsets {
                        discarded
                            .entry(tp.clone())
                            .and_modify(|current| *current = (*current).min(*offset))
                            .or_insert(*offset);
                    }
                    clear_commit_blocked(consumer, health);
                }
                note_success(health);
                return false;
            }
            Err(error) => {
                note_commit_result(health, false);
                consumer.counter("commit_failed_total", 1);
                update_state(health, GroupState::CommitBlocked, Some(error.to_string()));
                if !blocked {
                    blocked = true;
                    if let Ok(targets) = consumer.pause(&[]) {
                        add_pause_reason(health, &targets, PauseReason::CommitBlocked);
                    }
                }
            }
        }

        while let Ok(command) = commands.try_recv() {
            if matches!(command.kind, OwnerCommandKind::Stop) {
                if !OwnerHandle::begin_command(&command) {
                    continue;
                }
                update_state(health, GroupState::Stopping, None);
                if dlt_outstanding && deferred_stop.is_none() {
                    // 步骤 4/5：停机不能成为跳过 DLT 的理由。这里若直接回 Ok 并退出会话，
                    // 会话收尾会 abort 掉全部 in-flight DLT worker，shutdown 却报成功——死信被丢弃。
                    // 推迟该 Stop，让 owner 主循环继续收割 DltCompletion，收尾后再了结它。
                    *deferred_stop = Some(command);
                    return false;
                }
                OwnerHandle::finish_command(command, Ok(CommandReply::Unit));
                return true;
            }
            if OwnerHandle::begin_command(&command) {
                OwnerHandle::reject_command(command, "commit 未确认期间只接受停止命令");
            }
        }
        if consumer.has_rebalance() {
            // owner 外层会在处理下一条消息前取走 callback 事件；旧 generation 的 snapshot 只能丢弃并重放。
            progress.clear();
            publish_commit_snapshot(consumer, progress);
            update_state(health, GroupState::Joining, None);
            return false;
        }
        let wait = Duration::from_millis(backoff_ms.max(1)).min(Duration::from_millis(100));
        match consumer.poll(wait) {
            Err(error) => {
                record_maintenance_poll_error(health, transient_exit, error);
                return true;
            }
            // 维护性 poll 取出的记录必须记账并回退：CommitBlocked 期间竖起的 pause 只是
            // best-effort（`pause(&[])` 在 assignment 为空或查询失败时什么都没暂停），
            // 一旦真的取到记录，直接丢弃就等于让它的 fetch position 前移而无人处理，
            // 随后的安全前沿会把它提交越过（静默丢失）。
            Ok(Some(message)) => {
                let tp = Tp::new(message.topic(), message.partition());
                discarded
                    .entry(tp)
                    .and_modify(|current| *current = (*current).min(message.offset()))
                    .or_insert_with(|| message.offset());
            }
            Ok(None) => {}
        }
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.last_poll_at = Some(SystemTime::now());
        drop(snapshot);
        let _ = mode;
        let _ = attempts;
    }
}

/// 业务作用：接纳单条 outcome；容量不足时先提交安全前沿，仍不足则进入可自动恢复的 group 门禁。
///
/// - `context`: owner 本轮共享状态。
/// - `tp`: 记录分区。
/// - `offset`: 当前真实 offset，也是门禁回退点。
/// - `outcome`: 待写入的轻量处理结果。
fn observe_one_with_recovery(
    context: &mut ActionContext<'_>,
    tp: &Tp,
    offset: i64,
    outcome: RecordOutcome,
) -> std::result::Result<(), ActionFlow> {
    match context
        .progress
        .observe(tp, context.assignment_epoch, offset, outcome)
    {
        Ok(()) => return Ok(()),
        Err(error) if error.is_capacity() => {}
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
            return Err(ActionFlow::StopPartition);
        }
    }
    if commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    ) {
        return Err(ActionFlow::StopOwner);
    }
    match context
        .progress
        .observe(tp, context.assignment_epoch, offset, outcome)
    {
        Ok(()) => Ok(()),
        Err(error) if error.is_capacity() => {
            enter_progress_backpressure(context.consumer, context.health, &error);
            Err(ActionFlow::StopBatch {
                tp: tp.clone(),
                offset,
            })
        }
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
            Err(ActionFlow::StopPartition)
        }
    }
}

/// 业务作用：原子接纳同分区 run，并复用单条路径的 commit-first 容量恢复语义。
///
/// - `context`: owner 本轮共享状态。
/// - `tp`: run 所属分区。
/// - `outcomes`: 非空、按 offset 递增的处理结果。
fn observe_run_with_recovery(
    context: &mut ActionContext<'_>,
    tp: &Tp,
    outcomes: &[(i64, RecordOutcome)],
) -> std::result::Result<(), ActionFlow> {
    match context
        .progress
        .observe_run(tp, context.assignment_epoch, outcomes)
    {
        Ok(()) => return Ok(()),
        Err(error) if error.is_capacity() => {}
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
            return Err(ActionFlow::StopPartition);
        }
    }
    if commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    ) {
        return Err(ActionFlow::StopOwner);
    }
    match context
        .progress
        .observe_run(tp, context.assignment_epoch, outcomes)
    {
        Ok(()) => Ok(()),
        Err(error) if error.is_capacity() => {
            let offset = outcomes.first().map_or(0, |(offset, _)| *offset);
            enter_progress_backpressure(context.consumer, context.health, &error);
            Err(ActionFlow::StopBatch {
                tp: tp.clone(),
                offset,
            })
        }
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
            Err(ActionFlow::StopPartition)
        }
    }
}

/// 业务作用：处理批级回退失败：仍被本 owner 持有的分区位置已不可信，必须显式 Halt。
///
/// 这是把「回退没生效」从**静默丢失**变成**可见停摆**的通用防线：
/// 回退表里既有已 revoke（失败无害）也有仍持有的分区，后者一旦回退失败，
/// 那些已 poll 未派发的记录就再也不会被交付，而后续安全前沿会把它们提交越过。
/// 与其逐个审计回退点，不如在这里统一兜底。
///
/// # 参数
///
/// - `consumer`: owner consumer；用于确认分区是否仍在 assignment 内。
/// - `health`: group 健康快照。
/// - `failed`: 回退未生效的分区集合。
fn halt_failed_rewinds(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    failed: &[Tp],
) {
    if failed.is_empty() {
        return;
    }
    if consumer.has_rebalance() {
        // 有未处理的 rebalance 事件时，`assignment()` 报告的分区可能刚由 assign 落进
        // pending/queried，OffsetFetch 尚未回来——此时 `rd_kafka_seek` 必然回 `_STATE`
        // (fetch_state 未达 FETCH_OFFSET_QUERY)。nafka 未设 partition.assignment.strategy,
        // librdkafka 默认 **eager**：每次 rebalance 全撤全分，一批 in-flight 记录足以让
        // 整个 assignment 集体命中该窗口。把这判成 ProgressInvariant 就是把一次常规
        // rebalance 变成"全分区 Halt 待人工"。这些记录会在新 epoch 从 committed 正确重投。
        tracing::warn!(
            partitions = failed.len(),
            "回退失败但有 rebalance 待处理，按新 epoch 重投处理，不判定为位置不可信"
        );
        return;
    }
    let assigned = consumer
        .assignment()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    for tp in failed {
        // 已 revoke 的分区回退失败是预期的：新 owner 会从 broker committed 重新开始。
        if !assigned.contains(tp) {
            continue;
        }
        halt_partition(
            consumer,
            health,
            tp,
            HaltReason::ProgressInvariant,
            "批级回退未生效，分区位置不可信".into(),
        );
    }
}

/// 业务作用：判断 progress 反压门禁是否仍然竖着。
///
/// 判据是 health 里的 `ProgressBackpressure` 暂停行——它与底层 pause 一一对应，
/// 且不会被 `note_success`/`update_state` 这类状态写入覆盖。
///
/// - `health`: 共享健康快照。
fn progress_gate_engaged(health: &Arc<RwLock<GroupHealth>>) -> bool {
    health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .paused
        .iter()
        .any(|(_, reason)| *reason == PauseReason::ProgressBackpressure)
}

/// 业务作用：暂停当前 assignment 并进入容量门禁，等待 commit/DLT 收尾释放 permit。
///
/// - `consumer`: owner consumer。
/// - `health`: group 健康快照。
/// - `error`: 稳定容量分类，不包含 payload。
fn enter_progress_backpressure(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    error: &crate::consumer::offset_state::OffsetStateError,
) {
    let assignment = consumer.assignment().unwrap_or_default();
    let _ = consumer.pause(&assignment);
    add_pause_reason(health, &assignment, PauseReason::ProgressBackpressure);
    // 位置回退统一由 StopBatch 的批级回退完成：那里会把触发记录与本批全部未派发记录
    // 一起取 min 后只 seek 一次，避免两次 seek 互相覆盖。
    update_state(
        health,
        GroupState::ProgressBackpressure,
        Some(error.to_string()),
    );
}

/// 业务作用：将单条框架动作接入连续水位、重试、DLT 与健康状态机。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `source`: 保留原始 DLT 数据的消息视图。
/// - `action`: handler 或路由前处理产生的动作。
fn apply_process_action<S: DeadLetterSource + ?Sized>(
    context: &mut ActionContext<'_>,
    source: &S,
    action: ProcessAction,
) -> ActionFlow {
    let tp = Tp::new(source.source_topic(), source.source_partition());
    let offset = source.source_offset();
    match action {
        ProcessAction::Safe(outcome) => {
            if let Err(flow) = observe_one_with_recovery(context, &tp, offset, outcome) {
                return flow;
            }
            publish_commit_snapshot(context.consumer, context.progress);
            context.attempts.remove(&(tp, offset));
            note_success(context.health);
            if context.progress.observed_total()
                >= context.runtime.config.behavior.commit_batch_records
                && commit_until_resolved(
                    context.consumer,
                    context.progress,
                    context.health,
                    context.commands,
                    context.mode,
                    context.attempts,
                    context.last_commit,
                    context.runtime.config.behavior.consume_retry_backoff_ms,
                    context.transient_exit,
                    !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
                    context.deferred_stop,
                )
            {
                ActionFlow::StopOwner
            } else {
                ActionFlow::Continue
            }
        }
        ProcessAction::Retry(error) => apply_retry_action(context, source, &tp, offset, error),
        ProcessAction::DeadLetter(reason) => {
            apply_direct_dlt_action(context, source, &tp, offset, reason)
        }
        ProcessAction::Halt(reason, detail) => {
            halt_partition(context.consumer, context.health, &tp, reason, detail);
            ActionFlow::StopPartition
        }
        ProcessAction::Fatal(detail) => {
            update_state(context.health, GroupState::Crashed, Some(detail));
            ActionFlow::StopOwner
        }
    }
}

/// 业务作用：登记单条普通 handler 失败，推进总投递与毒消息预算，并在耗尽时转入 DLT。
///
/// 参数说明：
/// - `context`: owner 本轮共享状态。
/// - `source`: 原始消息视图。
/// - `tp`: 来源 topic-partition。
/// - `offset`: 失败 offset。
/// - `error`: 已脱敏失败原因。
///
/// 返回：已安排退避、DLT 或安全停分区后的 owner 流程动作；容量或持久化不变量失败时停止
/// 当前分区/owner，绝不越过失败 offset。
fn apply_retry_action<S: DeadLetterSource + ?Sized>(
    context: &mut ActionContext<'_>,
    source: &S,
    tp: &Tp,
    offset: i64,
    error: String,
) -> ActionFlow {
    if let Err(flow) = observe_one_with_recovery(context, tp, offset, RecordOutcome::Failed) {
        return flow;
    }
    publish_commit_snapshot(context.consumer, context.progress);
    if commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    ) {
        return ActionFlow::StopOwner;
    }
    context.progress.reset_partition(tp);
    publish_commit_snapshot(context.consumer, context.progress);
    if !admit_retry_entries(context.runtime, context.attempts, [(tp.clone(), offset)]) {
        halt_partition(
            context.consumer,
            context.health,
            tp,
            HaltReason::ProgressInvariant,
            "消费重试状态表达到硬上限".into(),
        );
        return ActionFlow::StopPartition;
    }
    let state = context
        .attempts
        .get_mut(&(tp.clone(), offset))
        .expect("已准入失败记录");
    state.failed_deliveries = state.failed_deliveries.saturating_add(1);
    state.bounded_failures = state.bounded_failures.saturating_add(1);
    let exhausted = context.runtime.config.behavior.max_consume_attempts != 0
        && state.bounded_failures >= context.runtime.config.behavior.max_consume_attempts;
    if exhausted {
        if context
            .runtime
            .config
            .behavior
            .dead_letter_topic_suffix
            .trim()
            .is_empty()
        {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::DltDisabled,
                "消费重试已耗尽，但 DLT 已关闭".into(),
            );
            return ActionFlow::StopPartition;
        }
        match schedule_single_dlt(context, source, &error) {
            Ok(()) => return ActionFlow::StopPartition,
            Err(NafkaError::RecursiveDlt)
                if context.runtime.config.behavior.dead_letter_required =>
            {
                halt_partition(
                    context.consumer,
                    context.health,
                    tp,
                    HaltReason::RecursiveDlt,
                    "禁止递归 DLT".into(),
                );
                return ActionFlow::StopPartition;
            }
            Err(NafkaError::RecursiveDlt) => {
                return apply_availability_skip(context, tp, offset);
            }
            Err(dlt_error) if !context.runtime.config.behavior.dead_letter_required => {
                tracing::error!(
                    group = context.group,
                    topic = %tp.topic,
                    partition = tp.partition,
                    offset,
                    error = %dlt_error,
                    "可用性优先模式跳过 DLT 失败记录"
                );
                return apply_availability_skip(context, tp, offset);
            }
            Err(dlt_error) => {
                halt_partition(
                    context.consumer,
                    context.health,
                    tp,
                    HaltReason::DltInfrastructure,
                    dlt_error.to_string(),
                );
                return ActionFlow::StopPartition;
            }
        }
    }
    schedule_retry_flow(context, tp, offset)
}

/// 业务作用：应用确定坏消息的直接 DLT 动作，不消耗 handler 尝试次数。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `source`: 原始消息视图。
/// - `tp`: 来源 topic-partition。
/// - `offset`: 来源 offset。
/// - `reason`: 已脱敏原因。
fn apply_direct_dlt_action<S: DeadLetterSource + ?Sized>(
    context: &mut ActionContext<'_>,
    source: &S,
    tp: &Tp,
    offset: i64,
    reason: String,
) -> ActionFlow {
    if context
        .runtime
        .config
        .behavior
        .dead_letter_topic_suffix
        .trim()
        .is_empty()
    {
        halt_partition(
            context.consumer,
            context.health,
            tp,
            HaltReason::DltDisabled,
            "当前记录要求 DLT，但 DLT 已关闭".into(),
        );
        return ActionFlow::StopPartition;
    }
    if let Err(flow) = observe_one_with_recovery(context, tp, offset, RecordOutcome::Failed) {
        return flow;
    }
    if commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    ) {
        return ActionFlow::StopOwner;
    }
    context.progress.reset_partition(tp);
    publish_commit_snapshot(context.consumer, context.progress);
    match schedule_single_dlt(context, source, &reason) {
        Ok(()) => ActionFlow::StopPartition,
        Err(NafkaError::RecursiveDlt) if context.runtime.config.behavior.dead_letter_required => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::RecursiveDlt,
                "禁止递归 DLT".into(),
            );
            ActionFlow::StopPartition
        }
        Err(NafkaError::RecursiveDlt) => apply_availability_skip(context, tp, offset),
        Err(error) if !context.runtime.config.behavior.dead_letter_required => {
            tracing::error!(
                group = context.group,
                topic = %tp.topic,
                partition = tp.partition,
                offset,
                error = %error,
                "可用性优先模式跳过 DLT 失败记录"
            );
            apply_availability_skip(context, tp, offset)
        }
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::DltInfrastructure,
                error.to_string(),
            );
            ActionFlow::StopPartition
        }
    }
}

/// 业务作用：在显式 availability-first 模式下记录安全跳过并提交该边界。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `tp`: 来源 topic-partition。
/// - `offset`: 来源 offset。
fn apply_availability_skip(context: &mut ActionContext<'_>, tp: &Tp, offset: i64) -> ActionFlow {
    if let Err(flow) = observe_one_with_recovery(
        context,
        tp,
        offset,
        RecordOutcome::Skipped(SkipReason::AvailabilityFirst),
    ) {
        return flow;
    }
    publish_commit_snapshot(context.consumer, context.progress);
    if commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    ) {
        ActionFlow::StopOwner
    } else {
        ActionFlow::Continue
    }
}

/// 业务作用：原子记录一个成功 handler run 的安全前缀。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `sources`: 同分区、同 route 且按交付顺序排列的安全记录。
/// - `source`: 自动成功或手动确认来源。
fn observe_safe_run(
    context: &mut ActionContext<'_>,
    sources: &[RawRecord],
    source: AckSource,
) -> ActionFlow {
    let Some(first) = sources.first() else {
        return ActionFlow::Continue;
    };
    let tp = first.ctx.topic_partition();
    let outcomes = sources
        .iter()
        .map(|record| (record.ctx.offset, RecordOutcome::Acknowledged(source)))
        .collect::<Vec<_>>();
    if let Err(flow) = observe_run_with_recovery(context, &tp, &outcomes) {
        return flow;
    }
    for record in sources {
        context.attempts.remove(&(tp.clone(), record.ctx.offset));
    }
    publish_commit_snapshot(context.consumer, context.progress);
    note_success(context.health);
    if context.progress.observed_total() >= context.runtime.config.behavior.commit_batch_records
        && commit_until_resolved(
            context.consumer,
            context.progress,
            context.health,
            context.commands,
            context.mode,
            context.attempts,
            context.last_commit,
            context.runtime.config.behavior.consume_retry_backoff_ms,
            context.transient_exit,
            !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
            context.deferred_stop,
        )
    {
        ActionFlow::StopOwner
    } else {
        ActionFlow::Continue
    }
}

/// 业务作用：把 single/batch handler 结论映射为安全前缀、普通失败预算或控制态延后。
///
/// 参数说明：
/// - `context`: owner 本轮共享状态。
/// - `route`: 本次调用的冻结 route。
/// - `sources`: 与 handler 输入一一对应的原始记录。
/// - `outcome`: 擦除 adapter 返回的严格确认结果。
///
/// 返回：已提交安全前缀、安排退避/DLT 或停止会话后的 owner 流程动作；非法 handler 形态
/// 返回停止动作并发布不变量错误。
fn apply_invocation_outcome(
    context: &mut ActionContext<'_>,
    route: &Arc<dyn crate::consumer::erased::ErasedConsumer>,
    sources: &[RawRecord],
    outcome: InvocationOutcome,
) -> ActionFlow {
    match outcome {
        InvocationOutcome::DecodeFailed { .. } => {
            // 解码结局必须先拆回输入策略，不能伪装成已执行的 handler 或越过对应 offset。
            update_state(
                context.health,
                GroupState::Crashed,
                Some("解码结局尚未归还消息策略".into()),
            );
            ActionFlow::StopOwner
        }
        InvocationOutcome::Completed {
            acked_prefix,
            acked_after_gap,
            total,
        } => {
            if total != sources.len() || acked_prefix > total {
                update_state(
                    context.health,
                    GroupState::Crashed,
                    Some(format!(
                        "handler outcome 形态非法: total={total}, sources={}, acked_prefix={acked_prefix}",
                        sources.len()
                    )),
                );
                return ActionFlow::StopOwner;
            }
            let ack_source = match route.meta().ack_mode {
                AckMode::Auto => AckSource::AutoOnSuccess,
                AckMode::Manual => AckSource::Manual,
            };
            let labels = [("group", context.group), ("consumer_id", route.meta().id)];
            if route.meta().ack_mode == AckMode::Manual {
                context.runtime.counter(
                    "manual_acked_total",
                    u64::try_from(acked_prefix).unwrap_or(u64::MAX),
                    &labels,
                );
                context.runtime.counter(
                    "manual_ack_missing_total",
                    u64::try_from(total.saturating_sub(acked_prefix)).unwrap_or(u64::MAX),
                    &labels,
                );
                context.runtime.counter(
                    "manual_ack_after_gap_total",
                    u64::try_from(acked_after_gap).unwrap_or(u64::MAX),
                    &labels,
                );
            }
            context.runtime.counter(
                "consumed_total",
                u64::try_from(acked_prefix).unwrap_or(u64::MAX),
                &labels,
            );
            let prefix_flow = observe_safe_run(context, &sources[..acked_prefix], ack_source);
            if prefix_flow != ActionFlow::Continue {
                return prefix_flow;
            }
            if acked_prefix == total {
                ActionFlow::Continue
            } else {
                apply_retry_run(
                    context,
                    &sources[acked_prefix..],
                    "手动确认未覆盖连续 run 后缀".into(),
                    RetryBudget::Bounded,
                    route.meta().execution_mode == ExecutionMode::KeyOrdered,
                )
            }
        }
        InvocationOutcome::Failed(NafkaError::HandlerDeadLetter(reason)) => {
            context.runtime.counter(
                "handler_dead_letter_total",
                1,
                &[("group", context.group), ("consumer_id", route.meta().id)],
            );
            let Some(source) = sources.first() else {
                update_state(
                    context.health,
                    GroupState::Crashed,
                    Some("请求 DLT 的 handler run 不能为空".into()),
                );
                return ActionFlow::StopOwner;
            };
            let tp = source.ctx.topic_partition();
            // DLT 与来源 offset 的提交由同一 owner 状态机串联：只有 broker 已确认死信后，
            // completion 才把该 offset 标为安全并提交，进程在两者之间崩溃只会重放原消息。
            if let Err(error) = remember_dlt_run(context, sources, &reason) {
                update_state(context.health, GroupState::Crashed, Some(error.to_string()));
                return ActionFlow::StopOwner;
            }
            apply_direct_dlt_action(context, source, &tp, source.ctx.offset, reason)
        }
        InvocationOutcome::Failed(NafkaError::HandlerDeferred(reason)) => {
            context.runtime.counter(
                "handler_deferred_total",
                1,
                &[("group", context.group), ("consumer_id", route.meta().id)],
            );
            // 延后仍执行与普通失败相同的顺序域退避，并推进可信投递序号；
            // 但它不推进毒消息预算，恢复控制态后必须重新交给同一 handler。
            apply_retry_run(
                context,
                sources,
                reason,
                RetryBudget::Deferred,
                route.meta().execution_mode == ExecutionMode::KeyOrdered,
            )
        }
        InvocationOutcome::Failed(error) => {
            context.runtime.counter(
                "handler_failed_total",
                1,
                &[("group", context.group), ("consumer_id", route.meta().id)],
            );
            apply_retry_run(
                context,
                sources,
                error.to_string(),
                RetryBudget::Bounded,
                route.meta().execution_mode == ExecutionMode::KeyOrdered,
            )
        }
        InvocationOutcome::Fatal(error) => {
            context.runtime.counter(
                "handler_failed_total",
                1,
                &[("group", context.group), ("consumer_id", route.meta().id)],
            );
            update_state(context.health, GroupState::Crashed, Some(error.to_string()));
            ActionFlow::StopOwner
        }
    }
}

/// 业务作用：区分普通毒消息预算与可恢复控制态延后，防止两层重试策略互相覆盖。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryBudget {
    /// 计入 `max_consume_attempts`，耗尽后进入 DLT。
    Bounded,
    /// 只做顺序域退避，不计入 DLT 预算。
    Deferred,
}

/// 业务作用：应用 typed consumer 的连续失败 run，并按门禁类型决定是否消耗 DLT 预算。
///
/// 参数说明：
/// - `context`: owner 本轮共享状态。
/// - `sources`: 同分区且按交付顺序排列的非空失败后缀。
/// - `error`: 已脱敏失败原因。
/// - `budget`: 是否计入底层毒消息预算。
/// - `in_place`: 按键原文仍保留在本地队列，退避期间不回退 native 取入位置。
///
/// 返回：已安排退避重投、DLT 或分区停止后的 owner 流程动作；空 run 或跨分区 run
/// 返回停止动作并发布不变量错误。
fn apply_retry_run(
    context: &mut ActionContext<'_>,
    sources: &[RawRecord],
    error: String,
    budget: RetryBudget,
    in_place: bool,
) -> ActionFlow {
    let Some(first) = sources.first() else {
        update_state(
            context.health,
            GroupState::Crashed,
            Some("失败 run 不能为空".into()),
        );
        return ActionFlow::StopOwner;
    };
    let tp = first.ctx.topic_partition();
    let first_offset = first.ctx.offset;
    if sources.iter().any(|record| {
        record.ctx.topic != first.ctx.topic || record.ctx.partition != first.ctx.partition
    }) {
        update_state(
            context.health,
            GroupState::Crashed,
            Some("失败 run 跨越 topic-partition".into()),
        );
        return ActionFlow::StopOwner;
    }
    if let Err(flow) = observe_one_with_recovery(context, &tp, first_offset, RecordOutcome::Failed)
    {
        return flow;
    }
    publish_commit_snapshot(context.consumer, context.progress);
    if commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    ) {
        return ActionFlow::StopOwner;
    }
    context.progress.reset_partition(&tp);
    publish_commit_snapshot(context.consumer, context.progress);
    context.runtime.counter(
        "retried_total",
        u64::try_from(sources.len()).unwrap_or(u64::MAX),
        &[("group", context.group), ("topic", tp.topic.as_str())],
    );
    // 所有失败记录都需要独立历史；整批容量不足时保护分区，不能只保存首条次数。
    if !admit_retry_entries(
        context.runtime,
        context.attempts,
        sources.iter().map(|source| (tp.clone(), source.ctx.offset)),
    ) {
        halt_partition(
            context.consumer,
            context.health,
            &tp,
            HaltReason::ProgressInvariant,
            "消费重试状态表达到硬上限".into(),
        );
        return ActionFlow::StopPartition;
    }
    for source in sources {
        let state = context
            .attempts
            .get_mut(&(tp.clone(), source.ctx.offset))
            .expect("已准入失败记录");
        state.failed_deliveries = state.failed_deliveries.saturating_add(1);
        if budget != RetryBudget::Deferred {
            state.bounded_failures = state.bounded_failures.saturating_add(1);
        }
    }
    if budget == RetryBudget::Deferred {
        return schedule_typed_retry(context, &tp, first_offset, in_place);
    }
    let state = &context.attempts[&(tp.clone(), first_offset)];
    let exhausted = context.runtime.config.behavior.max_consume_attempts != 0
        && state.bounded_failures >= context.runtime.config.behavior.max_consume_attempts;
    if exhausted {
        if context
            .runtime
            .config
            .behavior
            .dead_letter_topic_suffix
            .trim()
            .is_empty()
        {
            halt_partition(
                context.consumer,
                context.health,
                &tp,
                HaltReason::DltDisabled,
                "消费重试已耗尽，但 DLT 已关闭".into(),
            );
            return ActionFlow::StopPartition;
        }
        match schedule_dlt_run(context, sources, &error) {
            Ok(()) => return ActionFlow::StopPartition,
            Err(NafkaError::RecursiveDlt)
                if context.runtime.config.behavior.dead_letter_required =>
            {
                halt_partition(
                    context.consumer,
                    context.health,
                    &tp,
                    HaltReason::RecursiveDlt,
                    "禁止递归 DLT".into(),
                );
                return ActionFlow::StopPartition;
            }
            Err(NafkaError::RecursiveDlt) => return apply_availability_skip_run(context, sources),
            Err(dlt_error) if !context.runtime.config.behavior.dead_letter_required => {
                tracing::error!(
                    group = context.group,
                    topic = %tp.topic,
                    partition = tp.partition,
                    first_offset,
                    last_offset = sources.last().map_or(first_offset, |record| record.ctx.offset),
                    error = %dlt_error,
                    "可用性优先模式跳过 DLT 失败 run"
                );
                return apply_availability_skip_run(context, sources);
            }
            Err(dlt_error) => update_state(
                context.health,
                GroupState::Degraded,
                Some(dlt_error.to_string()),
            ),
        }
        halt_partition(
            context.consumer,
            context.health,
            &tp,
            HaltReason::DltInfrastructure,
            "DLT run 调度失败".into(),
        );
        return ActionFlow::StopPartition;
    }
    schedule_typed_retry(context, &tp, first_offset, in_place)
}

/// 业务作用：为保留原文的按键失败设置独立退避，避免回退 native 后再次交付已经完成的输入。
/// 参数说明：context 为当前责任表；tp/offset 为失败前缀；in_place 指示原文由有界本地队列持续保留。
/// 返回：已登记退避后停止裁决本前缀；非按键输入复用分区回退语义。
fn schedule_typed_retry(
    context: &mut ActionContext<'_>,
    tp: &Tp,
    offset: i64,
    in_place: bool,
) -> ActionFlow {
    if !in_place {
        return schedule_retry_flow(context, tp, offset);
    }
    // 原文仍在队列时不能 seek：迟到的旧 fetch 会在本地成功提交后再次物化同一记录。
    context.retries.insert(
        tp.clone(),
        PartitionRetry {
            retry_from: offset,
            ready_at: Instant::now()
                + Duration::from_millis(context.runtime.config.behavior.consume_retry_backoff_ms),
            in_place: true,
        },
    );
    add_pause_reason(
        context.health,
        std::slice::from_ref(tp),
        PauseReason::RetryBackoff,
    );
    update_state(context.health, GroupState::Retrying, None);
    ActionFlow::StopPartition
}

/// 业务作用：把单条消息构造成 DLT job 并接入非阻塞准入/worker 流程。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `source`: 原始消息视图。
/// - `reason`: 已脱敏失败原因。
///
/// # 错误
///
/// 递归 DLT、job 构造、pause/seek 或 dispatcher 准入失败时返回错误。
fn schedule_single_dlt<S: DeadLetterSource + ?Sized>(
    context: &mut ActionContext<'_>,
    source: &S,
    reason: &str,
) -> Result<()> {
    let tp = Tp::new(source.source_topic(), source.source_partition());
    let offset = source.source_offset();
    let suffix = &context.runtime.config.behavior.dead_letter_topic_suffix;
    if !suffix.is_empty() && source.source_topic().ends_with(suffix) {
        return Err(NafkaError::RecursiveDlt);
    }
    if !context.runtime.records.remember_dead_letter(
        &tp,
        offset,
        reason,
        source_delivery_attempts(source),
    ) {
        return Err(NafkaError::Lifecycle("DLT 原输入不在责任账本中".into()));
    }
    let (intent, raw_bytes) = context
        .runtime
        .records
        .dead_letter_intent(&tp, offset)
        .ok_or_else(|| NafkaError::Lifecycle("DLT 原输入意图已失效".into()))?;
    // 固定余量覆盖1024字节原因、2048字节源/集群、字段名与数字；原头全部保守保留计量。
    let required_bytes = raw_bytes
        .checked_add(tp.topic.len())
        .and_then(|n| n.checked_add(context.group.len()))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| NafkaError::Config("DLT 字节计量溢出".into()))?;
    if context.dlt_pending.contains_key(&tp) || context.dlt_admission.contains_key(&tp) {
        return Err(NafkaError::Lifecycle(
            "DLT 分区或本地准入责任尚未退出".into(),
        ));
    }
    let dispatcher = context
        .runtime
        .dlt_dispatcher
        .get()
        .ok_or_else(|| NafkaError::Lifecycle("DLT dispatcher 尚未初始化".into()))?
        .clone();
    // 先取得完整额度再复制原文；等待只有意图和坐标，不能把额外信封藏在本地队列。
    let reservation = dispatcher.try_reserve(1, required_bytes)?;
    context.consumer.pause(std::slice::from_ref(&tp))?;
    add_pause_reason(
        context.health,
        std::slice::from_ref(&tp),
        PauseReason::DltPending,
    );
    context
        .consumer
        .seek_offsets(&BTreeMap::from([(tp.clone(), offset)]))?;
    if let Some(reservation) = reservation {
        let record = build_dlt_record(
            suffix,
            context.group,
            source,
            &intent.reason,
            intent.attempts,
        )?;
        let envelope = reservation.attach(DltJob::from_records(vec![record])?)?;
        spawn_dlt_worker(context, dispatcher, envelope, tp, vec![offset]);
    } else {
        // 信封容量只阻止该分区的失败责任，健康分区继续取入与提交。
        add_pause_reason(
            context.health,
            std::slice::from_ref(&tp),
            PauseReason::DltBackpressure,
        );
        update_state(context.health, GroupState::DltBackpressure, None);
        context.dlt_admission.insert(
            tp.clone(),
            PendingDltAdmission {
                required_bytes,
                tp,
                assignment_epoch: context.assignment_epoch,
                offsets: vec![offset],
            },
        );
    }
    Ok(())
}

/// 业务作用：冻结整个失败批的逐记录死信意图，按来源顺序在有界信封池中逐条推进。
/// 参数说明：context 为当前控制权威；sources 为同分区失败输入；reason 为失败原因。
/// 返回：仅首条开始交付或等待；后继重读直接执行原死信意图，不重复执行业务。
fn schedule_dlt_run(
    context: &mut ActionContext<'_>,
    sources: &[RawRecord],
    reason: &str,
) -> Result<()> {
    let first = sources
        .first()
        .ok_or_else(|| NafkaError::Lifecycle("DLT 失败 run 不能为空".into()))?;
    remember_dlt_run(context, sources, reason)?;
    schedule_single_dlt(context, first, reason)
}

/// 业务作用：把一批已失败业务的意图逐条绑定到原输入凭证，允许小信封池分段交付。
/// 参数说明：context 为会话账本；sources 为已准入的同分区记录；reason 为有界失败说明。
/// 返回：全部原坐标仍属于本会话时登记成功；缺失责任时拒绝交付，不能静默丢弃后继。
fn remember_dlt_run(
    context: &ActionContext<'_>,
    sources: &[RawRecord],
    reason: &str,
) -> Result<()> {
    for source in sources {
        if !context.runtime.records.remember_dead_letter(
            &source.ctx.topic_partition(),
            source.ctx.offset,
            reason,
            source_delivery_attempts(source),
        ) {
            return Err(NafkaError::Lifecycle("失败批的原输入责任已失效".into()));
        }
    }
    Ok(())
}

/// 业务作用：启动一个已经取得全局 permit 的 DLT worker，并登记分区取消身份。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `dispatcher`: 全运行时共享 dispatcher。
/// - `envelope`: 已取得记录数与字节 permit 的任务信封。
/// - `tp`: 来源分区。
/// - `offsets`: 失败单元真实 offsets。
fn spawn_dlt_worker(
    context: &mut ActionContext<'_>,
    dispatcher: DltDispatcher,
    envelope: DltEnvelope,
    tp: Tp,
    offsets: Vec<i64>,
) {
    let assignment_epoch = context.assignment_epoch;
    let first_offset = offsets.first().copied().unwrap_or_default();
    let required = context.runtime.config.behavior.dead_letter_required;
    let initial_ms = context.runtime.config.behavior.dlt_retry_initial_ms;
    let max_ms = context.runtime.config.behavior.dlt_retry_max_ms;
    let degraded_after = context.runtime.config.behavior.dlt_degraded_after_attempts;
    let ready_epoch = context
        .health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .ready_assignment_epoch;
    let sender = context.dlt_sender.clone();
    let health = Arc::clone(context.health);
    let completion_tp = tp.clone();
    let runtime = Arc::clone(context.runtime);
    emit_dlt_queue_usage(context.runtime, &dispatcher);
    let failure_group = context.group.to_owned();
    let span = tracing::info_span!(
        "kafka.dlt",
        origin_topic = %tp.topic,
        partition = tp.partition,
        offset = first_offset,
        reason = "handler_or_invalid"
    );
    let cancel = Arc::new(AtomicBool::new(false));
    let task_cancel = Arc::clone(&cancel);
    let responsibility = dispatcher.worker(context.group);
    context.runtime.runtime.spawn(
        async move {
            // 分配表可以先撤销旧任务，但源级停止必须等待这个实际 future 退出。
            let _responsibility = responsibility;
            let stopped = || {
                task_cancel.load(Ordering::Acquire)
                    || runtime.stop_requested.load(Ordering::Acquire)
            };
            let result = deliver_dlt_envelope(
                &dispatcher,
                &envelope,
                required,
                initial_ms,
                max_ms,
                degraded_after,
                first_offset,
                &health,
                &completion_tp,
                ready_epoch,
                &stopped,
                || {
                    runtime.counter(
                        "dlt_attempt_failed_total",
                        offsets.len() as u64,
                        &[("group", &failure_group), ("topic", &completion_tp.topic)],
                    )
                },
            )
            .await;
            drop(envelope);
            let (jobs, bytes) = dispatcher.queue_usage();
            runtime.gauge(
                "dlt_queue_jobs",
                i64::try_from(jobs).unwrap_or(i64::MAX),
                &[],
            );
            runtime.gauge(
                "dlt_queue_bytes",
                i64::try_from(bytes).unwrap_or(i64::MAX),
                &[],
            );
            let _ = sender
                .send(DltCompletion {
                    tp: completion_tp,
                    assignment_epoch,
                    offsets,
                    result,
                })
                .await;
        }
        .instrument(span),
    );
    context.dlt_pending.insert(
        tp,
        PendingDlt {
            assignment_epoch,
            first_offset,
            cancel,
        },
    );
}

/// 业务作用：发布全运行时 DLT 有界队列的瞬时占用。
fn emit_dlt_queue_usage(runtime: &KafkaProxyInner, dispatcher: &DltDispatcher) {
    let (jobs, bytes) = dispatcher.queue_usage();
    runtime.gauge(
        "dlt_queue_jobs",
        i64::try_from(jobs).unwrap_or(i64::MAX),
        &[],
    );
    runtime.gauge(
        "dlt_queue_bytes",
        i64::try_from(bytes).unwrap_or(i64::MAX),
        &[],
    );
}

/// 业务作用：执行可靠 DLT 交付与退避，停止或失权后只收口已发送请求。
///
/// # 参数说明
///
/// - `dispatcher`：源级共享交付资源。
/// - `envelope`：已取得记录和字节额度的任务信封。
/// - `required`：是否必须取得 DLT 确认才允许来源推进。
/// - `initial_ms`、`max_ms`：指数退避初始值与上限。
/// - `degraded_after`：连续失败进入 Degraded 的次数。
/// - `seed`：稳定抖动种子。
/// - `health`：组健康快照。
/// - `tp`、`ready_epoch`：来源分区与分配代次，失权后不能写入旧降级原因。
/// - `stop_requested`：源停止与任务失权的准入门，不代表已发请求退出。
/// - `failed_attempt`：非停止失败的观测回调，不决定来源提交。
///
/// 返回：全部交付确认才成功；非必需交付失败、永久关闭或停止返回错误，停止不推定业务成功。可靠模式的重试次数不限，退避间隔有上限。
#[allow(clippy::too_many_arguments)]
async fn deliver_dlt_envelope(
    dispatcher: &DltDispatcher,
    envelope: &DltEnvelope,
    required: bool,
    initial_ms: u64,
    max_ms: u64,
    degraded_after: u32,
    seed: i64,
    health: &Arc<RwLock<GroupHealth>>,
    tp: &Tp,
    ready_epoch: Option<u64>,
    stop_requested: &impl Fn() -> bool,
    failed_attempt: impl Fn(),
) -> Result<()> {
    let mut attempt = 0_u32;
    loop {
        attempt = attempt.saturating_add(1);
        match dispatcher.deliver(envelope, stop_requested).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                // 原输入仍未提交，可以交回重投；停止后不再制造无限 DLT 轮次阻止资源退出。
                if stop_requested() {
                    return Err(error);
                }
                // 保留责任的重试也必须可观测；终结失败仍由 owner 的独立 completion 计数承担。
                failed_attempt();
                if !required {
                    return Err(error);
                }
                if attempt >= degraded_after {
                    mark_dlt_degraded(
                        health,
                        tp,
                        ready_epoch,
                        format!("DLT delivery 连续失败 attempt={attempt}: {error}"),
                    );
                }
                let deadline = tokio::time::Instant::now()
                    + dlt_retry_delay_values(initial_ms, max_ms, attempt, seed);
                while tokio::time::Instant::now() < deadline {
                    if stop_requested() {
                        return Err(error);
                    }
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + Duration::from_millis(25)),
                    )
                    .await;
                }
            }
        }
    }
}

/// 业务作用：将持续 DLT 失败绑定到仍有效的分区交付责任，防止其它成功覆盖其降级状态。
/// 参数说明：health 为槽快照；tp/ready_epoch 为启动交付时的分区和代次；detail 为脱敏失败诊断。
/// 返回：仅当前分配仍拥有 DltPending 时增加降级原因；失权、关闭和终结后的迟到失败不改变新状态。
fn mark_dlt_degraded(
    health: &Arc<RwLock<GroupHealth>>,
    tp: &Tp,
    ready_epoch: Option<u64>,
    detail: String,
) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // 后台交付不能把旧代次故障重新发布到新 assignment，也不能重新开放停机中的状态。
    if ready_epoch.is_none()
        || snapshot.ready_assignment_epoch != ready_epoch
        || matches!(
            snapshot.state,
            GroupState::Stopping
                | GroupState::Stopped
                | GroupState::Crashed
                | GroupState::Recovering
        )
        || !snapshot
            .paused
            .iter()
            .any(|(current, reason)| current == tp && *reason == PauseReason::DltPending)
    {
        return;
    }
    if !snapshot
        .paused
        .iter()
        .any(|(current, reason)| current == tp && *reason == PauseReason::DltRetryDegraded)
    {
        snapshot
            .paused
            .push((tp.clone(), PauseReason::DltRetryDegraded));
    }
    snapshot.state = GroupState::Degraded;
    snapshot.last_error = Some(detail);
}

/// 业务作用：优先收割异步 DLT completion，并在 delivery 成功后生成终态 outcome 与同步提交。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `receiver`: 本 owner 的有界 completion 接收端。
///
/// # 返回
///
/// commit 收尾期间收到 Stop 或框架不变量失败时返回 true。
fn drain_dlt_completions(
    context: &mut ActionContext<'_>,
    receiver: &mut tokio::sync::mpsc::Receiver<DltCompletion>,
) -> bool {
    // 原生 callback 已撤销旧权威时，旧结果不能趁 owner 尚未处理 mailbox 越过 fencing。
    if context.consumer.has_rebalance() || context.consumer.assignment_lost() {
        return false;
    }
    while let Ok(completion) = receiver.try_recv() {
        let first_offset = completion.offsets.first().copied().unwrap_or_default();
        let matches_pending = context
            .dlt_pending
            .get(&completion.tp)
            .is_some_and(|pending| {
                pending.assignment_epoch == completion.assignment_epoch
                    && pending.first_offset == first_offset
            });
        if !matches_pending {
            continue;
        }
        context.dlt_pending.remove(&completion.tp);
        if completion.assignment_epoch != context.assignment_epoch {
            continue;
        }
        let outcome = match completion.result {
            Ok(()) => {
                context.runtime.counter(
                    "dlt_total",
                    u64::try_from(completion.offsets.len()).unwrap_or(u64::MAX),
                    &[
                        ("group", context.group),
                        ("topic", completion.tp.topic.as_str()),
                    ],
                );
                RecordOutcome::DeadLettered
            }
            // 停止使尚未发送的信封也会返回；不能将取消准入解释成 availability-first 的成功跳过。
            Err(_) if context.runtime.stop_requested.load(Ordering::Acquire) => continue,
            Err(error) if !context.runtime.config.behavior.dead_letter_required => {
                context.runtime.counter(
                    "dlt_failed_total",
                    u64::try_from(completion.offsets.len()).unwrap_or(u64::MAX),
                    &[
                        ("group", context.group),
                        ("topic", completion.tp.topic.as_str()),
                    ],
                );
                tracing::error!(
                    group = context.group,
                    topic = %completion.tp.topic,
                    partition = completion.tp.partition,
                    first_offset,
                    error = %error,
                    "可用性优先模式跳过异步 DLT 失败单元"
                );
                RecordOutcome::Skipped(SkipReason::AvailabilityFirst)
            }
            Err(error) => {
                context.runtime.counter(
                    "dlt_failed_total",
                    u64::try_from(completion.offsets.len()).unwrap_or(u64::MAX),
                    &[
                        ("group", context.group),
                        ("topic", completion.tp.topic.as_str()),
                    ],
                );
                halt_partition(
                    context.consumer,
                    context.health,
                    &completion.tp,
                    HaltReason::DltInfrastructure,
                    error.to_string(),
                );
                continue;
            }
        };
        let outcomes = completion
            .offsets
            .iter()
            .map(|offset| (*offset, outcome))
            .collect::<Vec<_>>();
        if let Err(flow) = observe_run_with_recovery(context, &completion.tp, &outcomes) {
            if flow == ActionFlow::StopOwner {
                return true;
            }
            // 容量不足(StopBatch)：此时 dlt_pending 条目已被移除，若只是 continue，
            // 该分区会留着一条 DltPending 暂停行却**再没有任何 owner**——
            // 门禁解除只清 ProgressBackpressure 行，clear_pause_reason 也不会 resume
            // 仍带其它原因的分区，于是永久停摆且健康面看着"正常在收尾"。
            // DLT 本身已投递成功，这里把它转成可见的 Halt 等待人工裁决。
            halt_partition(
                context.consumer,
                context.health,
                &completion.tp,
                HaltReason::ProgressInvariant,
                "DLT 已完成但 progress 容量不足，无法接纳终态结果".into(),
            );
            continue;
        }
        publish_commit_snapshot(context.consumer, context.progress);
        if commit_until_resolved(
            context.consumer,
            context.progress,
            context.health,
            context.commands,
            context.mode,
            context.attempts,
            context.last_commit,
            context.runtime.config.behavior.consume_retry_backoff_ms,
            context.transient_exit,
            !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
            context.deferred_stop,
        ) {
            return true;
        }
        let Some(next_offset) = completion
            .offsets
            .last()
            .and_then(|offset| offset.checked_add(1))
        else {
            halt_partition(
                context.consumer,
                context.health,
                &completion.tp,
                HaltReason::ProgressInvariant,
                "DLT completion next-offset 溢出".into(),
            );
            continue;
        };
        if let Some(expected) = context
            .runtime
            .records
            .next_dead_letter(&completion.tp, next_offset)
        {
            context
                .runtime
                .records
                .expect_dead_letter(&completion.tp, expected);
        }
        if let Err(error) = context
            .consumer
            .seek_offsets(&BTreeMap::from([(completion.tp.clone(), next_offset)]))
        {
            halt_partition(
                context.consumer,
                context.health,
                &completion.tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
            continue;
        }
        context
            .attempts
            .remove(&(completion.tp.clone(), first_offset));
        // DLT 后继可能已在原逻辑批中计费；其重投不依赖新记录额度，也不能被该门禁永久挡住。
        let targets = context
            .runtime
            .records
            .replaying(&BTreeMap::from([(completion.tp.clone(), next_offset)]));
        resume_input_partitions(context.consumer, context.health, &targets);
        let resumable = clear_pause_reason(
            context.health,
            std::slice::from_ref(&completion.tp),
            &PauseReason::DltPending,
        );
        let mut resumable = resumable;
        resumable.extend(clear_pause_reason(
            context.health,
            std::slice::from_ref(&completion.tp),
            &PauseReason::DltRetryDegraded,
        ));
        let _ = context.consumer.resume(&resumable);
        note_success(context.health);
    }
    false
}

/// 业务作用：非阻塞复验各失败分区的 DLT 准入条件，健康分区继续由主循环处理。
/// 参数说明：context 为当前 owner 的分配权威、等待表和暂停事实。
/// 返回：仅开放容量已恢复的原坐标；停止或失权释放未发出的等待，不宣称来源已完成。
fn service_dlt_admission(context: &mut ActionContext<'_>) {
    if context.consumer.has_rebalance() || context.consumer.assignment_lost() {
        return;
    }
    let stopping = context.runtime.stop_requested.load(Ordering::Acquire);
    let dispatcher = context.runtime.dlt_dispatcher.get().cloned();
    let targets = context
        .dlt_admission
        .iter()
        .filter(|(_, pending)| {
            stopping
                || pending.assignment_epoch != context.assignment_epoch
                || dispatcher
                    .as_ref()
                    .is_none_or(|dispatcher| dispatcher.can_reserve(pending.required_bytes))
        })
        .map(|(tp, _)| tp.clone())
        .collect::<Vec<_>>();
    for tp in targets {
        let pending = context.dlt_admission.remove(&tp).expect("已选中的等待责任");
        if !stopping && pending.assignment_epoch == context.assignment_epoch {
            if dispatcher.is_none() {
                halt_partition(
                    context.consumer,
                    context.health,
                    &tp,
                    HaltReason::DltInfrastructure,
                    "DLT dispatcher 尚未初始化".into(),
                );
            } else {
                // 只恢复失败原坐标；信封复制仍须在再次取入时原子取得全额预算。
                let offset = pending.offsets[0];
                context.runtime.records.expect_dead_letter(&tp, offset);
                let failed = rewind_input_offsets(
                    context.runtime,
                    context.consumer,
                    context.health,
                    &BTreeMap::from([(tp.clone(), offset)]),
                );
                halt_failed_rewinds(context.consumer, context.health, &failed);
            }
        }
        clear_pause_reason(
            context.health,
            std::slice::from_ref(&tp),
            &PauseReason::DltBackpressure,
        );
        let resumable = clear_pause_reason(
            context.health,
            std::slice::from_ref(&tp),
            &PauseReason::DltPending,
        );
        if !stopping {
            let _ = context.consumer.resume(&resumable);
        }
    }
}

/// 业务作用：availability-first 地把整个失败 run 标为安全跳过并提交末尾边界。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `sources`: 同分区、按交付顺序排列的失败 run。
fn apply_availability_skip_run(
    context: &mut ActionContext<'_>,
    sources: &[RawRecord],
) -> ActionFlow {
    let Some(first) = sources.first() else {
        return ActionFlow::Continue;
    };
    let tp = first.ctx.topic_partition();
    let first_offset = first.ctx.offset;
    let outcomes = sources
        .iter()
        .map(|record| {
            (
                record.ctx.offset,
                RecordOutcome::Skipped(SkipReason::AvailabilityFirst),
            )
        })
        .collect::<Vec<_>>();
    if let Err(flow) = observe_run_with_recovery(context, &tp, &outcomes) {
        return flow;
    }
    publish_commit_snapshot(context.consumer, context.progress);
    let stopping = commit_until_resolved(
        context.consumer,
        context.progress,
        context.health,
        context.commands,
        context.mode,
        context.attempts,
        context.last_commit,
        context.runtime.config.behavior.consume_retry_backoff_ms,
        context.transient_exit,
        !context.dlt_pending.is_empty() || !context.dlt_admission.is_empty(),
        context.deferred_stop,
    );
    context.attempts.remove(&(tp, first_offset));
    if stopping {
        ActionFlow::StopOwner
    } else {
        ActionFlow::Continue
    }
}

/// 业务作用：为目标分区添加一个不重复的框架暂停原因。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `targets`: 目标分区。
/// - `reason`: 暂停原因。
fn add_pause_reason(health: &Arc<RwLock<GroupHealth>>, targets: &[Tp], reason: PauseReason) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for tp in targets {
        if !snapshot
            .paused
            .iter()
            .any(|(current, current_reason)| current == tp && *current_reason == reason)
        {
            snapshot.paused.push((tp.clone(), reason.clone()));
        }
    }
}

/// 业务作用：移除 CommitBlocked 原因并恢复没有其他暂停原因的分区。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `health`: group 健康快照。
fn clear_commit_blocked(consumer: &ConsumerClient, health: &Arc<RwLock<GroupHealth>>) {
    let resumable = {
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot
            .paused
            .retain(|(_, reason)| *reason != PauseReason::CommitBlocked);
        snapshot
            .assignment
            .iter()
            .filter(|tp| !snapshot.paused.iter().any(|(current, _)| current == *tp))
            .cloned()
            .collect::<Vec<_>>()
    };
    let _ = consumer.resume(&resumable);
}

/// 业务作用：为拥有型输入解析事件路由、校验 codec 声明并构造来源上下文。
/// 参数说明：runtime 提供无匹配及无效记录策略；plan 为冻结组路由；assignment_epoch 为本批权威；message 为已受条数和字节预算约束的输入。
/// 返回：可派发原始记录或消息策略；此处不执行自定义 codec，不改变解码失败计数或保护状态。
fn prepare_typed_message(
    runtime: &Arc<OwnerRuntime>,
    plan: &GroupPlan,
    assignment_epoch: u64,
    message: OwnedTypedMessage,
) -> PreparedTypedItem {
    if let Some((intent, _)) = runtime
        .records
        .dead_letter_intent(&message.tp(), message.offset)
    {
        return PreparedTypedItem::ImmediateOwned {
            source: message,
            action: ProcessAction::DeadLetter(intent.reason),
        };
    }
    if message.malformed_headers {
        return PreparedTypedItem::ImmediateOwned {
            source: message,
            action: invalid_action(
                runtime,
                crate::consumer::InvalidRecordReason::MalformedRoute,
                "header 名不是 UTF-8",
            ),
        };
    }
    let declaration = match RouteHeaders::parse(
        message
            .headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_deref())),
        runtime.config.behavior.strict_headers,
    ) {
        Ok(declaration) => declaration,
        Err(reason) => {
            return PreparedTypedItem::ImmediateOwned {
                source: message,
                action: invalid_action(
                    runtime,
                    crate::consumer::InvalidRecordReason::MalformedRoute,
                    reason,
                ),
            };
        }
    };
    let event = declaration.event;
    let Some(route) = plan
        .typed
        .iter()
        .find(|route| {
            route.meta().event == event
                && route
                    .meta()
                    .topics
                    .iter()
                    .any(|topic| topic == &message.topic)
        })
        .cloned()
    else {
        let action = match runtime.config.behavior.unmatched_policy {
            crate::config::UnmatchedPolicy::Skip => {
                ProcessAction::Safe(RecordOutcome::Skipped(SkipReason::UnmatchedRoute))
            }
            crate::config::UnmatchedPolicy::DeadLetter => {
                ProcessAction::DeadLetter("unmatched route".into())
            }
            crate::config::UnmatchedPolicy::Halt => ProcessAction::Halt(
                HaltReason::UnmatchedRoute,
                format!("未匹配 route event={event}"),
            ),
        };
        return PreparedTypedItem::ImmediateOwned {
            source: message,
            action,
        };
    };
    if message.payload.is_none() {
        use crate::config::TombstonePolicy;
        let action = match runtime.config.behavior.tombstone_policy {
            TombstonePolicy::Skip => Some(ProcessAction::Safe(RecordOutcome::Skipped(
                SkipReason::Tombstone,
            ))),
            TombstonePolicy::DeadLetter => Some(ProcessAction::DeadLetter("Tombstone".into())),
            TombstonePolicy::Halt => Some(ProcessAction::Halt(
                HaltReason::InvalidRecord(crate::consumer::InvalidRecordReason::MissingPayload),
                "Tombstone".into(),
            )),
            TombstonePolicy::Deliver => None,
        };
        // 只有明确的 Deliver 才进入类型解码；跳过、DLT 和暂停不会伪造一次业务调用。
        if let Some(action) = action {
            return PreparedTypedItem::ImmediateOwned {
                source: message,
                action,
            };
        }
    }
    // tombstone 没有编码格式；Deliver 使用类型的空值入口，其余载荷必须匹配 codec/mode。
    if let Err(error) = if message.payload.is_some() {
        declaration.validate_codec(route.meta().codec)
    } else {
        Ok(())
    } {
        return PreparedTypedItem::ImmediateOwned {
            source: message,
            action: invalid_or_dlt(runtime, error.to_string()),
        };
    }
    let passthrough = match message.headers.last(HEADER_PASSTHROUGH) {
        Some(header) => match header.value.as_deref() {
            Some(bytes) => match serde_json::from_slice(bytes) {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        topic = %message.topic,
                        partition = message.partition,
                        offset = message.offset,
                        error = %error,
                        "透传上下文解码失败，按兼容契约置为空"
                    );
                    serde_json::Map::new()
                }
            },
            None => serde_json::Map::new(),
        },
        None => serde_json::Map::new(),
    };
    let OwnedTypedMessage {
        input_permit,
        topic,
        partition,
        offset,
        timestamp,
        key,
        headers,
        malformed_headers: _,
        payload,
    } = message;
    let raw_key = key;
    let key = raw_key
        .as_deref()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .map(str::to_owned);
    let mut headers = headers;
    // 来源 producer 无权自报投递次数；首次交付也必须先覆盖同名值，避免伪造 header
    // 影响 Saga 等有界重试消费者的 ACK/DLT 裁决。
    headers.replace_framework_value(HEADER_DELIVERY_ATTEMPT, b"1".to_vec());
    let tombstone = payload.is_none();
    let raw = RawRecord::new(
        payload,
        ConsumeCtx {
            tombstone,
            source_id: runtime.config.client_name.clone(),
            group: plan.group.clone(),
            assignment_epoch,
            topic,
            partition,
            offset,
            delivery_attempt: 1,
            retry_attempt: 1,
            timestamp,
            key,
            raw_key,
            headers,
            passthrough,
        },
        assignment_epoch,
        input_permit,
    );
    if route.meta().execution_mode == ExecutionMode::KeyOrdered {
        raw.input_permit().mark_keyed();
    }
    PreparedTypedItem::Routed { source: raw, route }
}

/// 业务作用：在每次调用业务 handler 前，同步可信总投递序号与独立的普通失败预算序号。
///
/// 同一 offset 退避后会重新解码为新的记录；这里仍在最终调用点覆盖一次，确保 batch 合并、
/// 预取与失败后缀裁剪都不会把旧序号带入本次业务裁决。来源侧同名 header 被全部删除，
/// 防止生产者伪造“已耗尽预算”诱导消息跳过正常重试。
///
/// 参数说明：
/// - `sources`: DLT 与 offset 裁决保留的原始记录。
/// - `delivery_attempt`: 包含控制态延后的本次总投递序号。
/// - `retry_attempt`: 排除控制态延后的本次普通失败预算序号。
///
/// 返回：原始上下文携带可信序号，worker 解码时继承同一份业务身份。
fn annotate_delivery_attempt(sources: &mut [RawRecord], delivery_attempt: u32, retry_attempt: u32) {
    let value = delivery_attempt.to_string().into_bytes();
    for source in sources {
        // 实际 worker 共享原文；只有派发前的独占引用可以改变可信次数。
        let original = source.exclusive();
        original.ctx.delivery_attempt = delivery_attempt;
        original.ctx.retry_attempt = retry_attempt;
        original
            .ctx
            .headers
            .replace_framework_value(HEADER_DELIVERY_ATTEMPT, value.clone());
    }
}

/// 业务作用：驱动一次类型擦除 single/batch handler future，并应用统一超时边界。
///
/// # 参数
///
/// - `runtime`: 提供 owner 外部异步运行时与 handler 超时。
/// - `route`: 本次调用的冻结 route。
/// - `records`: 同分区、同 route 的非空连续交付 run。
/// - `handler_ambient`：从消息传播信息派生的 handler 环境 trace。
/// - `deadline`：执行准入时冻结、已由解码消耗的共同截止时刻。
///
/// 返回：handler 的完成结局；超过冻结预算时返回稳定超时失败。
fn invoke_typed_handler(
    runtime: &Arc<OwnerRuntime>,
    route: &Arc<dyn crate::consumer::erased::ErasedConsumer>,
    records: Vec<ErasedRecord>,
    handler_ambient: Option<natelemetry::TraceContext>,
    deadline: Option<Instant>,
) -> InvocationOutcome {
    // 解码已经消耗同一预算，不能在没有剩余时间时首次轮询业务 future。
    if deadline.is_some_and(|end| Instant::now() >= end) {
        return InvocationOutcome::Failed(NafkaError::Broker("consumer handler timeout".into()));
    }
    // handler 全程处于消息链路的环境作用域:业务在处理中发起的出站调用(REST/Kafka)未显式绑定
    // 上下文时自动延续本条消息的 trace,与 HTTP 入站中间件的作用域语义一致。
    let links = if records.len() > 1 {
        crate::KafkaTraceLinks::from_contexts(records.iter().map(|r| &r.ctx))
    } else {
        crate::KafkaTraceLinks::default()
    };
    let future = links.scope(natelemetry::with_ambient(
        handler_ambient,
        route.invoke(records),
    ));
    if let Some(deadline) = deadline {
        runtime.runtime.block_on(async {
            match tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), future)
                .await
            {
                Ok(outcome) if Instant::now() < deadline => outcome,
                // 同步阻塞的 handler 可能直到返回才归还执行权；实际退出后仍按总预算失败，
                // 不能让其迟到成功越过输入位点，也不能在它退出前开放同顺序域的后继调用。
                Ok(_) | Err(_) => {
                    InvocationOutcome::Failed(NafkaError::Broker("consumer handler timeout".into()))
                }
            }
        })
    } else {
        runtime.runtime.block_on(future)
    }
}

/// 业务作用：按 partition-first 与轮转起点处理 typed 逻辑批，并在最终调用点注入双重可信序号。
///
/// 参数说明：
/// - `runtime`: 所属运行时。
/// - `consumer`: owner 唯一 consumer。
/// - `plan`: 当前 group 冻结计划。
/// - `health`: group 健康快照。
/// - `commands`: owner 控制命令接收端。
/// - `attempts`: 本进程失败尝试表。
/// - `retries`: 按分区保留的退避和重新交付责任。
/// - `dlt_pending`、`dlt_sender`、`dlt_admission`: 已准入 DLT、完成邮箱与待准入门禁。
/// - `progress`: 连续安全水位。
/// - `last_commit`: 最近一次成功提交时间。
/// - `assignment_epoch`: 当前 assignment epoch。
/// - `batch`: 已完成 records/bytes 双门禁的逻辑批。
/// - `partition_cursor`: 跨批轮转分区起点。
/// - `transient_exit`: 维护性 poll 捕获的临时会话错误。
/// - `gate_trigger`: 触发进度容量保护的分区。
/// - `deferred_stop`: 已接纳但仍等待实际调用或 DLT 排空的停止命令。
/// - `decode_streaks`: 各分区连续解码失败的保护计数。
/// - `key_completions`: 当前 assignment 内尚未接入连续前缀的有界按键完成证据。
/// - `queued`: 跨 poll 保留的分区有序输入，实际 worker 结束后按原边界裁决。
///
/// 返回：
/// owner 必须停止当前会话时返回 true。
#[allow(clippy::too_many_arguments)]
fn process_logical_typed_batch(
    runtime: &Arc<OwnerRuntime>,
    consumer: &ConsumerClient,
    plan: &mut GroupPlan,
    health: &Arc<RwLock<GroupHealth>>,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    attempts: &mut BTreeMap<(Tp, i64), RetryAttemptState>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    dlt_pending: &mut BTreeMap<Tp, PendingDlt>,
    dlt_sender: &tokio::sync::mpsc::Sender<DltCompletion>,
    dlt_admission: &mut BTreeMap<Tp, PendingDltAdmission>,
    progress: &mut GroupOffsetState,
    last_commit: &mut Instant,
    assignment_epoch: u64,
    batch: LogicalTypedBatch,
    partition_cursor: &mut usize,
    transient_exit: &mut Option<String>,
    gate_trigger: &mut Option<Tp>,
    deferred_stop: &mut Option<OwnerCommand>,
    decode_streaks: &mut BTreeMap<Tp, u64>,
    key_completions: &mut KeyCompletions,
    queued: &mut BTreeMap<Tp, VecDeque<PreparedTypedItem>>,
) -> bool {
    let mut partitions: BTreeMap<Tp, Vec<TypedBatchItem>> = BTreeMap::new();
    for item in batch.items {
        partitions.entry(item.tp()).or_default().push(item);
    }
    for items in partitions.values_mut() {
        items.sort_by_key(TypedBatchItem::offset);
    }
    for (tp, items) in std::mem::take(&mut partitions) {
        if items
            .windows(2)
            .any(|pair| pair[0].offset() >= pair[1].offset())
        {
            halt_partition(
                consumer,
                health,
                &tp,
                HaltReason::ProgressInvariant,
                "同一逻辑批出现非递增 offset".into(),
            );
            continue;
        }
        // 这里只解析已受批字节上限约束的框架头和冻结路由，不等待业务执行额度。
        // 自定义 codec 与 handler 仍在实际调用 worker 内运行，池饱和不能阻塞完成裁决。
        queued
            .entry(tp)
            .or_default()
            .extend(items.into_iter().map(|item| match item {
                TypedBatchItem::Owned(message) => {
                    prepare_typed_message(runtime, plan, assignment_epoch, message)
                }
                TypedBatchItem::Oversize {
                    tp,
                    offset,
                    retained_bytes,
                } => PreparedTypedItem::Oversize {
                    tp,
                    offset,
                    retained_bytes,
                },
            }));
    }
    key_completions.collect_finished(
        consumer,
        assignment_epoch,
        runtime.stop_requested.load(Ordering::Acquire) || deferred_stop.is_some(),
        health,
    );
    if let Some(error) = key_completions.fatal_error() {
        // 更高 offset 的框架致命结论也先关闭新派发，不能等慢前缀后才开始保护。
        update_state(health, GroupState::Crashed, Some(error));
        return true;
    }
    if consumer.has_rebalance()
        || consumer.assignment_lost()
        || deferred_stop.is_some()
        || runtime.stop_requested.load(Ordering::Acquire)
    {
        return false;
    }
    let mut order = queued.keys().cloned().collect::<Vec<_>>();
    if !order.is_empty() {
        let start = *partition_cursor % order.len();
        order.rotate_left(start);
        *partition_cursor = (*partition_cursor + 1) % order.len();
    }
    for tp in order {
        let Some(mut prepared) = queued.remove(&tp) else {
            continue;
        };
        while !prepared.is_empty() {
            if matches!(prepared.front(), Some(PreparedTypedItem::Routed { route, .. }) if route.meta().execution_mode == ExecutionMode::KeyOrdered)
            {
                let retry_offset = retries
                    .get(&tp)
                    .filter(|state| state.in_place)
                    .map(|state| state.retry_from);
                key_completions.launch_round(
                    runtime,
                    attempts,
                    &mut prepared,
                    health,
                    retry_offset,
                );
                if let Some(error) = key_completions.fatal_error() {
                    update_state(health, GroupState::Crashed, Some(error));
                    return true;
                }
                let first = prepared.front().expect("队列非空").location();
                if key_completions.outcome(&first.0, first.1).is_none() {
                    break;
                }
            }
            let first = prepared.front().expect("队列非空").location();
            // 用户暂停只关闭新派发，已经实际完成的前缀仍须接入账本并提交，避免责任永久滞留。
            if key_completions.active_at(&first.0, first.1)
                || (handler::user_paused(health, &tp)
                    && key_completions.outcome(&first.0, first.1).is_none())
            {
                break;
            }
            let item = prepared.pop_front().expect("前置检查保证有记录");
            let flow = match item {
                PreparedTypedItem::ImmediateOwned { source, action } => {
                    let mut context = ActionContext {
                        runtime,
                        consumer,
                        health,
                        group: &plan.group,
                        commands,
                        mode: &mut plan.mode,
                        attempts,
                        retries,
                        dlt_pending,
                        dlt_sender,
                        dlt_admission,
                        progress,
                        last_commit,
                        transient_exit,
                        deferred_stop,
                        assignment_epoch,
                    };
                    apply_process_action(&mut context, &source, action)
                }
                PreparedTypedItem::DecodeFailure {
                    source,
                    consumer_id,
                    reason,
                } => {
                    let tp = source.ctx.topic_partition();
                    let previous = decode_streaks.get(&tp).copied();
                    // 批次尾部可能随前缀重试被整体丢弃；只有走到本条输入时才决定连续失败保护。
                    let action = decode_failure_action(runtime, decode_streaks, &tp, reason);
                    let mut context = ActionContext {
                        runtime,
                        consumer,
                        health,
                        group: &plan.group,
                        commands,
                        mode: &mut plan.mode,
                        attempts,
                        retries,
                        dlt_pending,
                        dlt_sender,
                        dlt_admission,
                        progress,
                        last_commit,
                        transient_exit,
                        deferred_stop,
                        assignment_epoch,
                    };
                    let flow = apply_process_action(&mut context, &source, action);
                    if matches!(flow, ActionFlow::StopBatch { .. }) {
                        // 容量不足尚未接管失败责任，回退后重新取入不能再次消耗失败阈值。
                        if let Some(value) = previous {
                            decode_streaks.insert(tp, value);
                        } else {
                            decode_streaks.remove(&tp);
                        }
                    } else {
                        runtime.counter(
                            "decode_failed_total",
                            1,
                            &[("group", &plan.group), ("consumer_id", consumer_id)],
                        );
                    }
                    flow
                }
                PreparedTypedItem::Oversize {
                    tp,
                    offset,
                    retained_bytes,
                } => {
                    halt_partition(
                        consumer,
                        health,
                        &tp,
                        HaltReason::OversizeRecord,
                        format!("record offset={offset} bytes={retained_bytes} 超过上限"),
                    );
                    ActionFlow::StopPartition
                }
                PreparedTypedItem::Routed { source, route } => {
                    let mut sources = vec![source];
                    let mut source_bytes = sources[0].retained_bytes();
                    let batch_retry_counts =
                        retry_counts(attempts, &(tp.clone(), sources[0].ctx.offset));
                    let completed_count =
                        key_completions.completed_count(&tp, sources[0].ctx.offset);
                    if route.meta().shape == ConsumerShape::Batch {
                        while sources.len()
                            < completed_count.unwrap_or(runtime.config.behavior.max_batch_records)
                            && matches!(prepared.front(), Some(PreparedTypedItem::Routed { source: next_source, route: next }) if Arc::ptr_eq(&route, next) && (completed_count.is_some() || (source_bytes.saturating_add(next_source.retained_bytes()) <= runtime.config.behavior.max_batch_bytes && retry_counts(attempts, &(tp.clone(), next_source.ctx.offset)) == batch_retry_counts)))
                        {
                            let Some(PreparedTypedItem::Routed { source, .. }) =
                                prepared.pop_front()
                            else {
                                unreachable!("前置形态检查已确认 routed item");
                            };
                            source_bytes = source_bytes.saturating_add(source.retained_bytes());
                            sources.push(source);
                        }
                    }
                    let first_offset = sources[0].ctx.offset;
                    // 每条原文携带自己的可信历史；分批边界变化不能重置尾部或把首条次数借给新输入。
                    for source in &mut sources {
                        let (deliveries, failures) =
                            retry_counts(attempts, &(tp.clone(), source.ctx.offset));
                        annotate_delivery_attempt(
                            std::slice::from_mut(source),
                            deliveries.saturating_add(1),
                            failures.saturating_add(1),
                        );
                    }
                    let cached = key_completions.outcome(&tp, first_offset);
                    let outcome = if let Some(outcome) = cached.clone() {
                        outcome
                    } else {
                        match key_completions.start_partition(runtime, &route, sources.clone()) {
                            Ok(_) => {
                                // 启动或等待额度都保留前缀，立即轮转到其它分区。
                                for source in sources.into_iter().rev() {
                                    prepared.push_front(PreparedTypedItem::Routed {
                                        source,
                                        route: Arc::clone(&route),
                                    });
                                }
                                break;
                            }
                            Err(outcome) => outcome,
                        }
                    };
                    if let InvocationOutcome::DecodeFailed { failures } = outcome {
                        // 解码失败不算一次业务调用：按原 offset 拆回消息策略，正常前后缀保留再次准入资格。
                        // 只有主循环真正接管失败项时才更新连续失败计数，预备尾部不能提前重置保护。
                        let mut failures = failures.into_iter().collect::<BTreeMap<_, _>>();
                        if failures.is_empty()
                            || failures.keys().any(|offset| {
                                !sources.iter().any(|source| source.ctx.offset == *offset)
                            })
                        {
                            update_state(
                                health,
                                GroupState::Crashed,
                                Some("解码结局与本次输入身份不一致".into()),
                            );
                            return true;
                        }
                        for source in sources.into_iter().rev() {
                            let item = if let Some(reason) = failures.remove(&source.ctx.offset) {
                                PreparedTypedItem::DecodeFailure {
                                    source,
                                    consumer_id: route.meta().id,
                                    reason,
                                }
                            } else {
                                PreparedTypedItem::Routed {
                                    source,
                                    route: Arc::clone(&route),
                                }
                            };
                            prepared.push_front(item);
                        }
                        if cached.is_some() {
                            key_completions.remove(&tp, first_offset);
                        }
                        // 拆回消息策略后仍先处理可能到达的失权，不能绕过逻辑批末尾的分配门禁。
                        if consumer.has_rebalance() || consumer.assignment_lost() {
                            let rewind = unprocessed_offsets(&prepared, &partitions);
                            let failed = rewind_input_offsets(runtime, consumer, health, &rewind);
                            halt_failed_rewinds(consumer, health, &failed);
                            return false;
                        }
                        continue;
                    }
                    note_decode_success(decode_streaks, &tp);
                    let mut context = ActionContext {
                        runtime,
                        consumer,
                        health,
                        group: &plan.group,
                        commands,
                        mode: &mut plan.mode,
                        attempts,
                        retries,
                        dlt_pending,
                        dlt_sender,
                        dlt_admission,
                        progress,
                        last_commit,
                        transient_exit,
                        deferred_stop,
                        assignment_epoch,
                    };
                    let flow = apply_invocation_outcome(&mut context, &route, &sources, outcome);
                    if cached.is_some() && !matches!(flow, ActionFlow::StopBatch { .. }) {
                        key_completions.remove(&tp, first_offset);
                    }
                    if flow == ActionFlow::StopPartition
                        && route.meta().execution_mode == ExecutionMode::KeyOrdered
                        && retries.contains_key(&tp)
                    {
                        // 同 key 的失败前缀留在队列占有顺序域；其它 key 可继续使用空闲额度。
                        retries.get_mut(&tp).expect("已登记按键退避").in_place = true;
                        for source in sources.into_iter().rev() {
                            prepared.push_front(PreparedTypedItem::Routed {
                                source,
                                route: Arc::clone(&route),
                            });
                        }
                        resume_key_retry_inputs(consumer, health, retries);
                        ActionFlow::Continue
                    } else {
                        flow
                    }
                }
            };
            match flow {
                ActionFlow::Continue => {}
                ActionFlow::StopPartition => {
                    // 失败坐标重读前等待本分区旧调用退出，防止同源字节和副作用重叠；其它分区继续。
                    if key_completions.active_partition(&tp) {
                        let _ = consumer.pause(std::slice::from_ref(&tp));
                        add_pause_reason(
                            health,
                            std::slice::from_ref(&tp),
                            PauseReason::HandlerInFlight,
                        );
                    }
                    prepared.clear();
                    break;
                }
                ActionFlow::StopBatch { tp, offset } => {
                    // 触发记录本身没有进入 progress，必须与本批未派发记录一起回退；
                    // 取 min 保证不会被"下一条记录"的回退把它跳过去。
                    let mut rewind = unprocessed_offsets(&prepared, &partitions);
                    rewind
                        .entry(tp.clone())
                        .and_modify(|current| *current = (*current).min(offset))
                        .or_insert(offset);
                    let failed = rewind_input_offsets(runtime, consumer, health, &rewind);
                    halt_failed_rewinds(consumer, health, &failed);
                    *gate_trigger = Some(tp);
                    return false;
                }
                ActionFlow::StopOwner => return true,
            }
            if consumer.has_rebalance() {
                // post-rebalance mailbox 已经就绪时，本逻辑批仍可能持有 cooperative 协议保留
                // 分区的已 poll 记录。先回退全部未派发项；已 revoke 的 seek 失败无害，保留
                // 分区则可从原 offset 重新交付。
                let rewind = unprocessed_offsets(&prepared, &partitions);
                if !rewind.is_empty() {
                    let failed = rewind_input_offsets(runtime, consumer, health, &rewind);
                    halt_failed_rewinds(consumer, health, &failed);
                }
                return false;
            }
        }
        if !prepared.is_empty() {
            queued.insert(tp, prepared);
        }
    }
    key_completions.publish_load(health);
    false
}

/// 业务作用：汇总逻辑批中尚未派发的每分区最小 offset。
///
/// - `prepared`: 当前分区剩余记录。
/// - `partitions`: 尚未轮转到的分区记录。
fn unprocessed_offsets(
    prepared: &VecDeque<PreparedTypedItem>,
    partitions: &BTreeMap<Tp, Vec<TypedBatchItem>>,
) -> BTreeMap<Tp, i64> {
    let mut offsets: BTreeMap<Tp, i64> = BTreeMap::new();
    let mut insert = |tp: Tp, offset: i64| {
        offsets
            .entry(tp)
            .and_modify(|current| *current = (*current).min(offset))
            .or_insert(offset);
    };
    for item in prepared {
        let (tp, offset) = item.location();
        insert(tp, offset);
    }
    for item in partitions.values().flatten() {
        insert(item.tp(), item.offset());
    }
    offsets
}

/// 业务作用：路由并处理单条借用消息。
///
/// # 参数
///
/// - `runtime`: 所属运行时。
/// - `plan`: group 路由计划。
/// - `assignment_epoch`: 当前 owner epoch。
/// - `message`: poll 借用消息。
fn process_message(
    runtime: &Arc<OwnerRuntime>,
    plan: &GroupPlan,
    assignment_epoch: u64,
    message: &RdMessage<'_>,
) -> ProcessAction {
    if message.retained_bytes()
        > runtime
            .config
            .behavior
            .max_record_bytes
            .min(runtime.records.maximum_record_bytes())
    {
        return ProcessAction::Halt(
            HaltReason::OversizeRecord,
            format!("record bytes={} 超过上限", message.retained_bytes()),
        );
    }
    if let Some((intent, _)) = runtime.records.dead_letter_intent(
        &Tp::new(message.topic(), message.partition()),
        message.offset(),
    ) {
        return ProcessAction::DeadLetter(intent.reason);
    }
    let headers = match message.headers() {
        Ok(headers) => headers,
        Err(_) => {
            return invalid_action(
                runtime,
                crate::consumer::InvalidRecordReason::MalformedRoute,
                "header 名不是 UTF-8",
            );
        }
    };
    let declaration = match RouteHeaders::parse(
        headers.iter().map(|header| (header.name, header.value)),
        runtime.config.behavior.strict_headers,
    ) {
        Ok(declaration) => declaration,
        Err(reason) => {
            return invalid_action(
                runtime,
                crate::consumer::InvalidRecordReason::MalformedRoute,
                reason,
            );
        }
    };
    let event = declaration.event;
    // 单消息路径只服务 passthrough-only group：registry 禁止 typed 与 passthrough 混入同一
    // group，而 typed 非空时 owner 主循环走逻辑批路径，不会到达这里。此处不做 typed 路由，
    // 避免维护一份永远不会执行、却会随 ack/指标语义演进而漂移的第二实现。
    debug_assert!(
        plan.typed.is_empty(),
        "单消息路径不该出现 typed route：registry 应已拒绝混合注册"
    );
    if let Some(route) = plan.passthrough.iter().find(|route| {
        route.meta.event == event
            && route
                .meta
                .topics
                .iter()
                .any(|topic| topic == message.topic())
    }) {
        return process_passthrough(
            runtime,
            &plan.group,
            route,
            assignment_epoch,
            message,
            &headers,
        );
    }
    match runtime.config.behavior.unmatched_policy {
        crate::config::UnmatchedPolicy::Skip => {
            ProcessAction::Safe(RecordOutcome::Skipped(SkipReason::UnmatchedRoute))
        }
        crate::config::UnmatchedPolicy::DeadLetter => {
            ProcessAction::DeadLetter("unmatched route".into())
        }
        crate::config::UnmatchedPolicy::Halt => ProcessAction::Halt(
            HaltReason::UnmatchedRoute,
            format!("未匹配 route event={event}"),
        ),
    }
}

/// 业务作用：在 owner callback 栈内执行同步少拷贝 route。
///
/// # 参数
///
/// - `runtime`: 所属运行时及无效记录策略。
/// - `route`: 匹配 passthrough route。
/// - `assignment_epoch`: 当前 epoch。
/// - `message`: 借用消息。
/// - `headers`: 底层借用 headers。
fn process_passthrough(
    runtime: &Arc<OwnerRuntime>,
    group: &str,
    route: &crate::consumer::registry::RegisteredPassthrough,
    assignment_epoch: u64,
    message: &RdMessage<'_>,
    headers: &[crate::rd::consumer::RdHeaderRef<'_>],
) -> ProcessAction {
    let public_headers: Vec<KafkaHeaderRef<'_>> = headers
        .iter()
        .map(|header| KafkaHeaderRef {
            name: header.name,
            value: header.value,
        })
        .collect();
    let ack_set = (route.meta.ack_mode == AckMode::Manual).then(|| {
        ManualAckSet::new(vec![AckIdentity {
            authority: None,
            stopping: Some(Arc::clone(&runtime.stop_requested)),
            invocation_id: 0,
            assignment_epoch,
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
        }])
    });
    let ack = match &ack_set {
        Some(set) => set.record_ack(0),
        None => RecordAck::Auto,
    };
    let record = BorrowedKafkaRecord::new(
        message.topic(),
        message.partition(),
        message.offset(),
        message.timestamp(),
        message.key(),
        KafkaHeadersRef::new(&public_headers),
        message.payload(),
        ack,
    );
    let span = tracing::info_span!(
        "kafka.consume",
        group,
        consumer_id = route.meta.id,
        topic = message.topic(),
        partition = message.partition(),
        offset = message.offset(),
        event = %route.meta.event,
        attempt = 1_u32,
        trace_id = tracing::field::Empty
    );
    let incoming = crate::trace::extract_fields(public_headers.iter().map(|h| (h.name, h.value)));
    if let Err(reason) = incoming {
        runtime.counter(
            "trace_invalid_total",
            1,
            &[("group", group), ("reason", reason.as_str())],
        );
    }
    let (trace, context) =
        crate::trace::consume_span(runtime.span_recorder.as_ref(), incoming.ok().flatten());
    let callback_started = Instant::now();
    let outcome = span.in_scope(|| {
        natelemetry::with_ambient_sync(Some(context), || {
            std::panic::catch_unwind(AssertUnwindSafe(|| route.handler.consume_borrowed(record)))
        })
    });
    if let Some(trace) = trace {
        let _ = trace.finish(None);
    }
    runtime.counter("passthrough_calls_total", 1, &[("group", group)]);
    runtime.counter(
        "passthrough_duration_micros_total",
        u64::try_from(callback_started.elapsed().as_micros()).unwrap_or(u64::MAX),
        &[("group", group)],
    );
    match outcome {
        Ok(Ok(disposition)) => {
            finish_passthrough_success(disposition, route.meta.ack_mode, ack_set.as_ref())
        }
        Ok(Err(error)) => {
            let detail = format!(
                "{:?}: {}",
                error.reason(),
                error.safe_detail().unwrap_or("无诊断文本")
            );
            match error.action() {
                PassthroughFailureAction::Retry => ProcessAction::Retry(detail),
                PassthroughFailureAction::ApplyInvalidRecordPolicy => invalid_action(
                    runtime,
                    match error.reason() {
                        crate::consumer::PassthroughFailureReason::Invalid(reason) => reason,
                        _ => crate::consumer::InvalidRecordReason::MalformedRoute,
                    },
                    &detail,
                ),
                PassthroughFailureAction::DeadLetter => ProcessAction::DeadLetter(detail),
                PassthroughFailureAction::Halt => ProcessAction::Halt(
                    match error.reason() {
                        crate::consumer::PassthroughFailureReason::Halt(
                            crate::consumer::PassthroughHaltReason::RouteExpansionTooLarge,
                        ) => HaltReason::RouteExpansionTooLarge,
                        _ => HaltReason::FrameTooLarge,
                    },
                    detail,
                ),
                PassthroughFailureAction::Fatal => ProcessAction::Fatal(detail),
            }
        }
        Err(_) => ProcessAction::Retry("passthrough handler panic".into()),
    }
}

/// 业务作用：把 passthrough 成功返回值与 route 确认模式合并成唯一 owner 动作。
///
/// # 参数
///
/// - `disposition`: 同步 adapter 返回的类型化成功结果。
/// - `ack_mode`: 注册时冻结的 route 确认模式。
/// - `ack_set`: 手动模式的回调期确认槽；自动模式为 `None`。
fn finish_passthrough_success(
    disposition: PassthroughDisposition,
    ack_mode: AckMode,
    ack_set: Option<&ManualAckSet>,
) -> ProcessAction {
    match disposition {
        PassthroughDisposition::Skipped { reason } => {
            // 安全跳过由框架分类事实确认，不要求业务再用 Manual token 重复确认。
            if let Some(set) = ack_set {
                let _ = set.finish(false);
            }
            ProcessAction::Safe(RecordOutcome::Skipped(SkipReason::Passthrough(reason)))
        }
        PassthroughDisposition::Handled { .. }
        | PassthroughDisposition::BestEffortDropped { .. } => {
            if ack_set.is_none_or(|set| set.finish(true).prefix == 1) {
                ProcessAction::Safe(RecordOutcome::Acknowledged(match ack_mode {
                    AckMode::Auto => AckSource::AutoOnSuccess,
                    AckMode::Manual => AckSource::Manual,
                }))
            } else {
                ProcessAction::Retry("passthrough 手动确认缺失".into())
            }
        }
    }
}

/// 业务作用：按显式无效输入策略选择跳过、可靠死信或保留位点暂停。
/// 参数说明：runtime 为所属运行时，reason 为脱敏原因。
/// 返回：只在显式 Skip 下放弃输入；默认 DLT 仍须完成交付后才产生安全水位。
fn invalid_or_dlt(runtime: &Arc<OwnerRuntime>, reason: String) -> ProcessAction {
    match runtime.config.behavior.invalid_record_policy {
        // 跳过必须来自显式策略，不能由 DLT 不可用或解码失败自动降级产生。
        crate::config::InvalidRecordPolicy::Skip => {
            ProcessAction::Safe(RecordOutcome::Skipped(SkipReason::InvalidRecord))
        }
        crate::config::InvalidRecordPolicy::Halt => ProcessAction::Halt(
            HaltReason::InvalidRecord(crate::consumer::InvalidRecordReason::MalformedRoute),
            reason,
        ),
        crate::config::InvalidRecordPolicy::DeadLetter => ProcessAction::DeadLetter(reason),
    }
}

/// 业务作用：在应用单条无效输入策略前检查本分区连续解码保护，防止系统性解码失败持续排干来源。
/// 参数说明：runtime提供冻结阈值；streaks为本owner按分区隔离的连续失败计数；tp为失败分区；reason为脱敏原因。
/// 返回：正阈值耗尽时关闭该分区准入；未配置或阈值为零时只应用invalid_record_policy，不影响其它分区计数。
fn decode_failure_action(
    runtime: &Arc<OwnerRuntime>,
    streaks: &mut BTreeMap<Tp, u64>,
    tp: &Tp,
    reason: String,
) -> ProcessAction {
    let streak = record_decode_failure(streaks, tp);
    if let Some(limit) = runtime
        .config
        .behavior
        .decode_failure_halt_streak
        .filter(|limit| *limit > 0)
    {
        if streak >= limit {
            // Halt 后本分区不再派发；保留计数无害且下次成功解码即清零。
            return ProcessAction::Halt(
                HaltReason::InvalidRecord(crate::consumer::InvalidRecordReason::MalformedRoute),
                format!("分区连续 {streak} 条解码失败，疑似解码器整体故障；最近一条：{reason}"),
            );
        }
    }
    invalid_or_dlt(runtime, reason)
}

/// 业务作用：记录本分区一次解码失败并返回累计的连续失败次数。
///
/// 纯函数（不触碰 runtime），集中维护"分区之间互不影响"这条关键隔离语义。
///
/// - `streaks`: owner 会话局部的按分区连续解码失败表。
/// - `tp`: 触发失败的分区。
fn record_decode_failure(streaks: &mut BTreeMap<Tp, u64>, tp: &Tp) -> u64 {
    let entry = streaks.entry(tp.clone()).or_insert(0);
    *entry = entry.saturating_add(1);
    *entry
}

/// 业务作用：记录本分区一次解码成功，清零该分区的连续失败计数。
///
/// 工作正常的分区解码成功后立即从表中移除，因此该表只保留"正在连续失败"的分区，
/// 规模受当前 assignment 约束（远小于分区总数）。
///
/// - `streaks`: owner 会话局部的按分区连续解码失败表。
/// - `tp`: 解码成功的分区。
fn note_decode_success(streaks: &mut BTreeMap<Tp, u64>, tp: &Tp) {
    streaks.remove(tp);
}

/// 业务作用：构造不依赖消息所有权的无效路由动作。
///
/// 参数说明：runtime 为所属运行时，invalid_reason 为强类型原因，reason 为脱敏文本。
/// 返回：明确的安全跳过、DLT 或硬暂停；普通业务失败预算不改变此策略。
fn invalid_action(
    runtime: &Arc<OwnerRuntime>,
    invalid_reason: crate::consumer::InvalidRecordReason,
    reason: &str,
) -> ProcessAction {
    match runtime.config.behavior.invalid_record_policy {
        // 只有显式接受放弃的源才可把无效记录记作安全跳过。
        crate::config::InvalidRecordPolicy::Skip => {
            ProcessAction::Safe(RecordOutcome::Skipped(SkipReason::InvalidRecord))
        }
        crate::config::InvalidRecordPolicy::Halt => {
            ProcessAction::Halt(HaltReason::InvalidRecord(invalid_reason), reason.into())
        }
        crate::config::InvalidRecordPolicy::DeadLetter => ProcessAction::DeadLetter(reason.into()),
    }
}

/// 业务作用：在已取得信封额度后复制原文并写入当前来源证据。
/// 参数说明：suffix/group 为冻结目标与来源组，message 为原始视图；reason 为有界失败原因，delivery_attempts 为首次失败意图冻结的可信次数。
/// 返回：独立拥有的死信原文；真实来源已经是 DLT 时返回递归错误，不接受伪造来源头作为权威。
fn build_dlt_record<S: DeadLetterSource + ?Sized>(
    suffix: &str,
    group: &str,
    message: &S,
    reason: &str,
    delivery_attempts: u32,
) -> Result<DltRecord> {
    // 来源 topic 是实际 broker 身份，来源侧可伪造的旧死信头不能授予或阻止本次留证。
    if !suffix.is_empty() && message.source_topic().ends_with(suffix) {
        return Err(NafkaError::RecursiveDlt);
    }
    let headers = crate::wire::dead_letter_headers(
        &message.source_headers(),
        group,
        message.source_topic(),
        message.source_partition(),
        message.source_offset(),
        reason,
        delivery_attempts,
    );
    Ok(DltRecord {
        topic: format!("{}{suffix}", message.source_topic()),
        payload: message.source_payload().map(<[u8]>::to_vec),
        key: message.source_key().map(<[u8]>::to_vec),
        headers,
        timestamp: (message.source_timestamp() >= 0).then(|| message.source_timestamp()),
    })
}

/// 业务作用：把 DLT 投递次数收敛为从一开始的安全值，避免异常实现写出零次死信证据。
///
/// 参数说明：
/// - `source`: 待隔离的原始消息视图。
///
/// 返回：至少为一的可信投递次数。
fn source_delivery_attempts<S: DeadLetterSource + ?Sized>(source: &S) -> u32 {
    source.delivery_attempts().max(1)
}

/// 业务作用：用显式数值计算 DLT 指数退避，使边界计算不依赖运行时状态。
///
/// # 参数
///
/// - `base`: 初始退避毫秒，必须大于零。
/// - `max`: 指数增长的封顶毫秒，必须不小于 `base`。
/// - `attempt`: 已失败的 delivery 次数，从一开始。
/// - `seed`: 不含业务内容的稳定抖动种子。
fn dlt_retry_delay_values(base: u64, max: u64, attempt: u32, seed: i64) -> Duration {
    let exponential = capped_exponential_delay(base, max, attempt);
    let spread = exponential / 5;
    let jitter = if spread == 0 {
        0
    } else {
        seed.unsigned_abs().wrapping_add(u64::from(attempt)) % (spread + 1)
    };
    Duration::from_millis(
        exponential
            .saturating_sub(spread / 2)
            .saturating_add(jitter),
    )
}

/// 业务作用：安排失败分区的非阻塞退避，并把错误映射成 owner 控制流。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `tp`: 失败分区。
/// - `offset`: 失败单元首 offset。
fn schedule_retry_flow(context: &mut ActionContext<'_>, tp: &Tp, offset: i64) -> ActionFlow {
    match schedule_partition_retry(
        context.consumer,
        context.health,
        context.retries,
        tp,
        offset,
        context.runtime.config.behavior.consume_retry_backoff_ms,
    ) {
        Ok(()) => ActionFlow::StopPartition,
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
            ActionFlow::StopPartition
        }
    }
}

/// 业务作用：暂停单个失败分区、回到失败 offset，并登记单调时钟恢复时刻。
///
/// # 参数
///
/// - `consumer`: owner 唯一 consumer。
/// - `health`: group 健康快照。
/// - `retries`: owner 的每分区退避表。
/// - `tp`: 失败分区。
/// - `offset`: 失败单元首 offset。
/// - `backoff_ms`: 本次退避毫秒。
///
/// # 错误
///
/// pause 或 seek 失败时返回错误，调用方必须保持分区不可派发。
fn schedule_partition_retry(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    tp: &Tp,
    offset: i64,
    backoff_ms: u64,
) -> Result<()> {
    consumer.pause(std::slice::from_ref(tp))?;
    add_pause_reason(health, std::slice::from_ref(tp), PauseReason::RetryBackoff);
    if let Err(error) = consumer.seek_offsets(&BTreeMap::from([(tp.clone(), offset)])) {
        let _ = clear_pause_reason(health, std::slice::from_ref(tp), &PauseReason::RetryBackoff);
        return Err(error);
    }
    retries.insert(
        tp.clone(),
        PartitionRetry {
            retry_from: offset,
            ready_at: Instant::now() + Duration::from_millis(backoff_ms),
            in_place: false,
        },
    );
    update_state(health, GroupState::Retrying, None);
    Ok(())
}

/// 业务作用：恢复退避到期的调用；保留原文的按键责任直接重派，其余分区先恢复 broker 坐标。
///
/// # 参数
///
/// - `consumer`: owner 唯一 consumer。
/// - `health`: group 健康快照。
/// - `retries`: owner 的每分区退避表。
/// - `retry_delay_ms`: seek 临时失败后的再次尝试间隔。
/// - `runtime`: 原输入责任表，已计费重试不会重新申请共享容量。
///
/// 返回：到期且定位成功的分区移除退避；定位失败继续保留输入和退避责任。
fn activate_due_retries(
    runtime: &OwnerRuntime,
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    retry_delay_ms: u64,
) {
    let now = Instant::now();
    let due = retries
        .iter()
        .filter(|(_, state)| state.ready_at <= now)
        .map(|(tp, state)| (tp.clone(), state.retry_from, state.in_place))
        .collect::<Vec<_>>();
    for (tp, offset, in_place) in due {
        let positioned = if in_place {
            Ok(())
        } else {
            consumer.seek_offsets(&BTreeMap::from([(tp.clone(), offset)]))
        };
        match positioned {
            Ok(()) => {
                // 失败前缀已经拥有输入额度，不能被后续新记录触发的容量暂停挡住。
                let targets = runtime
                    .records
                    .replaying(&BTreeMap::from([(tp.clone(), offset)]));
                resume_input_partitions(consumer, health, &targets);
                retries.remove(&tp);
                let resumable = clear_pause_reason(
                    health,
                    std::slice::from_ref(&tp),
                    &PauseReason::RetryBackoff,
                );
                let _ = consumer.resume(&resumable);
            }
            Err(error) => {
                if let Some(state) = retries.get_mut(&tp) {
                    state.ready_at = Instant::now() + Duration::from_millis(retry_delay_ms.max(1));
                }
                update_state(health, GroupState::Degraded, Some(error.to_string()));
            }
        }
    }
    if retries.is_empty() {
        let has_framework_pause = health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .paused
            .iter()
            .any(|(_, reason)| *reason != PauseReason::User);
        if !has_framework_pause {
            update_state(health, GroupState::Running, None);
        }
    }
}

/// 业务作用：按键退避只阻止对应顺序域，允许同分区其它 key 继续取入。
/// 参数说明：consumer/health 为本槽取入权威与暂停原因；retries 为仍在保留原文的退避责任。
/// 返回：只恢复没有 User、容量、DLT 或硬保护原因的分区，保留 RetryBackoff 观测与期限。
fn resume_key_retry_inputs(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    retries: &BTreeMap<Tp, PartitionRetry>,
) {
    let snapshot = health
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let targets = retries
        .iter()
        .filter(|(tp, state)| {
            state.in_place
                && !snapshot
                    .paused
                    .iter()
                    .any(|(current, reason)| current == *tp && *reason != PauseReason::RetryBackoff)
        })
        .map(|(tp, _)| tp.clone())
        .collect::<Vec<_>>();
    drop(snapshot);
    let _ = consumer.resume(&targets);
}

/// 业务作用：新输入无共享额度时只保护触发分区，保留其它分区释放责任的机会。
/// 参数说明：consumer/health 为当前 owner；tp 为已定位回未取入记录的分区。
/// 返回：登记输入暂停；暂停失败时保留可见错误，下一次取入仍会再次检查额度。
fn enter_input_backpressure(consumer: &ConsumerClient, health: &Arc<RwLock<GroupHealth>>, tp: &Tp) {
    // 原文尚未复制，先保持 broker 重投位置，再禁止无额度的新业务进入。
    let error = consumer
        .pause(std::slice::from_ref(tp))
        .err()
        .map(|error| error.to_string());
    add_pause_reason(
        health,
        std::slice::from_ref(tp),
        PauseReason::InputBackpressure,
    );
    update_state(health, GroupState::ProgressBackpressure, error);
}

/// 业务作用：额度可用或已计费输入重新交付时，只解除输入容量门禁。
/// 参数说明：consumer/health 为当前 owner；offsets 为已获得重新准入资格的分区及原重读坐标。
/// 返回：仅恢复仍属本会话且无其它保护原因的分区；恢复失败转为硬暂停，避免状态与实际路由分离。
fn resume_input_partitions(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    offsets: &BTreeMap<Tp, i64>,
) {
    if offsets.is_empty() {
        return;
    }
    let targets = offsets.keys().cloned().collect::<Vec<_>>();
    let assigned = consumer.assignment().unwrap_or_default();
    let resumable = clear_pause_reason(health, &targets, &PauseReason::InputBackpressure)
        .into_iter()
        .filter(|tp| assigned.contains(tp))
        .collect::<Vec<_>>();
    if resumable.is_empty() {
        return;
    }
    // 容量可用不是 User、RetryBackoff、Halt 等独立控制权威的解除证据。
    let resumed = (|| {
        let offsets = resumable
            .iter()
            .map(|tp| (tp.clone(), offsets[tp]))
            .collect();
        consumer.resume(&resumable)?;
        // 原生 resume 仅解除暂停标记，空闲 broker 线程可能仍在等待；以原坐标重新激活 fetch。
        // 这里不运行 poll、不改变恢复水位，不越过未接纳输入；任一步失败都在后续派发前重新保护分区。
        consumer.seek_offsets(&offsets)
    })();
    if let Err(error) = resumed {
        for tp in &resumable {
            halt_partition(
                consumer,
                health,
                tp,
                HaltReason::ProgressInvariant,
                error.to_string(),
            );
        }
    } else {
        note_success(health);
    }
}

/// 业务作用：未完成输入回退后恢复其原额度的使用权，避免新输入门禁阻断已计费责任。
/// 参数说明：runtime 提供责任账本；consumer/health 为当前会话；offsets 为应重新交付的最早坐标。
/// 返回：实际回退失败的分区；失败目标不解除任何输入保护。
fn rewind_input_offsets(
    runtime: &OwnerRuntime,
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    offsets: &BTreeMap<Tp, i64>,
) -> Vec<Tp> {
    let failed = consumer.rewind_offsets(offsets);
    let accepted = offsets
        .iter()
        .filter(|(tp, _)| !failed.contains(tp))
        .map(|(tp, offset)| (tp.clone(), *offset))
        .collect();
    let targets = runtime.records.replaying(&accepted);
    resume_input_partitions(consumer, health, &targets);
    failed
}

/// 业务作用：移除一组目标上的指定框架暂停原因，并返回已经没有任何暂停原因的分区。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `targets`: 本次框架流程曾暂停的分区。
/// - `reason`: 只移除这一种原因，保留业务暂停和其他安全门禁。
fn clear_pause_reason(
    health: &Arc<RwLock<GroupHealth>>,
    targets: &[Tp],
    reason: &PauseReason,
) -> Vec<Tp> {
    let targets: BTreeSet<Tp> = targets.iter().cloned().collect();
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot
        .paused
        .retain(|(tp, current_reason)| !targets.contains(tp) || current_reason != reason);
    targets
        .into_iter()
        .filter(|tp| !snapshot.paused.iter().any(|(current, _)| current == tp))
        .collect()
}

/// 业务作用：暂停单个分区并写入 Halt 原因。
///
/// # 参数
///
/// - `consumer`: owner consumer。
/// - `health`: 健康快照。
/// - `tp`: 目标分区。
/// - `reason`: 强类型 Halt 原因。
/// - `detail`: 已脱敏诊断。
fn halt_partition(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    tp: &Tp,
    reason: HaltReason,
    detail: String,
) {
    let _ = consumer.pause(std::slice::from_ref(tp));
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.state = GroupState::Degraded;
    snapshot.last_error = Some(detail);
    // 只覆盖框架自身的瞬时暂停原因；业务显式 pause 必须保留，否则后续 sanctioned seek
    // 恢复 Halt 时会把业务本要暂停的分区一并放开（clear_operator_pauses 保 User，
    // 但这里若把 User 行删掉就什么都不剩了）。
    snapshot
        .paused
        .retain(|(current, current_reason)| current != tp || *current_reason == PauseReason::User);
    snapshot
        .paused
        .push((tp.clone(), PauseReason::Halt(reason)));
}

/// 业务作用：增加或移除一组分区的 User 暂停原因。
///
/// # 参数
///
/// - `health`: 健康快照。
/// - `targets`: 目标分区。
/// - `paused`: true 为增加，false 为移除。
fn set_user_paused(health: &Arc<RwLock<GroupHealth>>, targets: &[Tp], paused: bool) {
    let targets: BTreeSet<Tp> = targets.iter().cloned().collect();
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot
        .paused
        .retain(|(tp, reason)| !(targets.contains(tp) && *reason == PauseReason::User));
    if paused {
        snapshot
            .paused
            .extend(targets.into_iter().map(|tp| (tp, PauseReason::User)));
    }
}

/// 业务作用：复制底层 headers 为公共有序拥有型集合。
///
/// # 参数
///
/// - `headers`: 底层借用 headers。
fn owned_headers(headers: &[crate::rd::consumer::RdHeaderRef<'_>]) -> KafkaHeaders {
    KafkaHeaders::from_vec(
        headers
            .iter()
            .map(|header| KafkaHeader {
                name: header.name.to_owned(),
                value: header.value.map(<[u8]>::to_vec),
            })
            .collect(),
    )
}
