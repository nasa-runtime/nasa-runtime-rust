//! 每 group 单 owner 线程、借用路由、严格确认与同步安全提交。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use tracing::Instrument;

use crate::consumer::ack::{AckIdentity, ManualAckSet, RecordAck};
use crate::consumer::dlt::{DltAdmission, DltDispatcher, DltEnvelope, DltJob, DltRecord};
use crate::consumer::erased::{ConsumerShape, ErasedRecord, InvocationOutcome, RawRecord};
use crate::consumer::offset_state::{AckSource, GroupOffsetState, RecordOutcome, SkipReason};
use crate::consumer::passthrough::{
    BorrowedKafkaRecord, KafkaHeaderRef, KafkaHeadersRef, PassthroughDisposition,
    PassthroughFailureAction,
};
use crate::consumer::supervisor::{
    update_state, CommandReply, GroupHandle, GroupPlan, OwnerCommand, OwnerCommandKind, OwnerMode,
    StartupGate,
};
use crate::consumer::{AckMode, ConsumeCtx};
use crate::error::{NafkaError, Result};
use crate::health::{GroupHealth, GroupState, HaltReason, PauseReason};
use crate::rd::consumer::{
    ConsumerClient, RdMessage, RdPollError, RdRebalanceEvent, RdRebalanceKind,
};
use crate::types::{KafkaHeader, KafkaHeaders, StartOffset, Tp};
use crate::wire::{
    PayloadCodec, DEFAULT_EVENT, HEADER_DLT_ORIGIN_OFFSET, HEADER_DLT_ORIGIN_PARTITION,
    HEADER_DLT_ORIGIN_TOPIC, HEADER_DLT_REASON, HEADER_EVENT, HEADER_PASSTHROUGH,
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

/// 维护性 poll 后 owner 主循环应采取的会话级动作。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaintenanceFlow {
    /// 当前门禁已经解除，可以恢复业务派发。
    Ready,
    /// 门禁仍然存在，本轮只维护心跳。
    Blocked,
    /// poll 已报告会话级临时或致命错误，必须退出当前会话。
    StopOwner,
}

/// owner 应用处理结果时共享的可变状态引用集合。
struct ActionContext<'a> {
    /// 所属运行时。
    runtime: &'a Arc<KafkaProxyInner>,
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
    /// 本进程失败尝试表。
    attempts: &'a mut BTreeMap<(Tp, i64), u32>,
    /// 每分区非阻塞重试退避状态。
    retries: &'a mut BTreeMap<Tp, PartitionRetry>,
    /// 每分区至多一个在途 DLT 任务。
    dlt_pending: &'a mut BTreeMap<Tp, PendingDlt>,
    /// DLT completion 有界发送端。
    dlt_sender: &'a tokio::sync::mpsc::Sender<DltCompletion>,
    /// dispatcher 暂无 permit 时每 owner 唯一的本地准入任务。
    dlt_admission: &'a mut Option<PendingDltAdmission>,
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
    /// 返回来源 topic。
    fn source_topic(&self) -> &str;

    /// 返回来源分区。
    fn source_partition(&self) -> i32;

    /// 返回来源 offset。
    fn source_offset(&self) -> i64;

    /// 返回来源时间戳。
    fn source_timestamp(&self) -> i64;

    /// 返回原始 key。
    fn source_key(&self) -> Option<&[u8]>;

    /// 返回原始 payload。
    fn source_payload(&self) -> Option<&[u8]>;

    /// 复制原始有序 headers。
    fn source_headers(&self) -> KafkaHeaders;
}

impl DeadLetterSource for RdMessage<'_> {
    /// 返回底层借用消息 topic。
    fn source_topic(&self) -> &str {
        self.topic()
    }

    /// 返回底层借用消息分区。
    fn source_partition(&self) -> i32 {
        self.partition()
    }

    /// 返回底层借用消息 offset。
    fn source_offset(&self) -> i64 {
        self.offset()
    }

    /// 返回底层借用消息时间戳。
    fn source_timestamp(&self) -> i64 {
        self.timestamp()
    }

    /// 返回底层借用消息 key。
    fn source_key(&self) -> Option<&[u8]> {
        self.key()
    }

    /// 返回底层借用消息 payload。
    fn source_payload(&self) -> Option<&[u8]> {
        self.payload()
    }

    /// 仅在 DLT 路径复制底层 headers。
    fn source_headers(&self) -> KafkaHeaders {
        self.headers()
            .map(|headers| owned_headers(&headers))
            .unwrap_or_default()
    }
}

/// typed 逻辑批中的拥有型原始消息；不会进入 passthrough 借用链路。
struct OwnedTypedMessage {
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
    /// 在底层借用消息仍有效时立即物化有界 typed 记录。
    ///
    /// # 参数
    ///
    /// - `message`: 已通过单记录字节上限检查的借用消息。
    fn copy_from(
        message: &RdMessage<'_>,
        parsed: &Result<Vec<crate::rd::consumer::RdHeaderRef<'_>>>,
    ) -> Self {
        let (headers, malformed_headers) = match parsed {
            Ok(headers) => (owned_headers(headers), false),
            Err(_) => (KafkaHeaders::default(), true),
        };
        Self {
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

    /// 返回当前消息的 topic-partition。
    fn tp(&self) -> Tp {
        Tp::new(self.topic.clone(), self.partition)
    }
}

impl DeadLetterSource for OwnedTypedMessage {
    /// 返回拥有型来源 topic。
    fn source_topic(&self) -> &str {
        &self.topic
    }

    /// 返回拥有型来源分区。
    fn source_partition(&self) -> i32 {
        self.partition
    }

    /// 返回拥有型来源 offset。
    fn source_offset(&self) -> i64 {
        self.offset
    }

    /// 返回拥有型来源时间戳。
    fn source_timestamp(&self) -> i64 {
        self.timestamp
    }

    /// 返回拥有型来源 key。
    fn source_key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// 返回拥有型来源 payload。
    fn source_payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// 克隆拥有型来源 headers 供 DLT 独立持有。
    fn source_headers(&self) -> KafkaHeaders {
        self.headers.clone()
    }
}

impl DeadLetterSource for RawRecord {
    /// 返回已解析 typed 原始记录的 topic。
    fn source_topic(&self) -> &str {
        &self.ctx.topic
    }

    /// 返回已解析 typed 原始记录的分区。
    fn source_partition(&self) -> i32 {
        self.ctx.partition
    }

    /// 返回已解析 typed 原始记录的 offset。
    fn source_offset(&self) -> i64 {
        self.ctx.offset
    }

    /// 返回已解析 typed 原始记录的时间戳。
    fn source_timestamp(&self) -> i64 {
        self.ctx.timestamp
    }

    /// 返回已解析 typed 原始记录的 UTF-8 key 字节。
    fn source_key(&self) -> Option<&[u8]> {
        self.ctx.key.as_deref().map(str::as_bytes)
    }

    /// 返回已解析 typed 原始记录的 payload。
    fn source_payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// 克隆已解析 typed 原始记录的 headers 供 DLT 独立持有。
    fn source_headers(&self) -> KafkaHeaders {
        self.ctx.headers.clone()
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
    /// 返回当前项的 topic-partition。
    fn tp(&self) -> Tp {
        match self {
            Self::Owned(message) => message.tp(),
            Self::Oversize { tp, .. } => tp.clone(),
        }
    }

    /// 返回当前项的真实 offset。
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
}

/// 一个失败分区的非阻塞退避状态。
struct PartitionRetry {
    /// 恢复前必须再次 seek 的失败单元首 offset。
    retry_from: i64,
    /// 允许恢复派发的单调时钟时刻。
    ready_at: Instant,
}

/// owner 持有的单分区在途 DLT 身份与取消句柄。
struct PendingDlt {
    /// 任务所属 assignment epoch。
    assignment_epoch: u64,
    /// 失败单元首 offset。
    first_offset: i64,
    /// owner 会话退出或 rebalance 失效时的任务取消句柄。
    abort: tokio::task::AbortHandle,
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

/// dispatcher permit 暂不可得时 owner 唯一允许保留的本地 DLT job。
struct PendingDltAdmission {
    /// 尚未取得全局 permit 的原始任务。
    job: DltJob,
    /// 来源 topic-partition。
    tp: Tp,
    /// 调度时 assignment epoch。
    assignment_epoch: u64,
    /// 失败单元按交付顺序排列的 offsets。
    offsets: Vec<i64>,
    /// group 级维护性 poll 已取出但未派发的每分区最小 offset。
    discarded: BTreeMap<Tp, i64>,
}

/// typed 记录完成路由前处理与逐条解码后的派发形态。
enum PreparedTypedItem {
    /// 尚未构造 typed 上下文时已经得到框架动作。
    ImmediateOwned {
        /// 保留 DLT 所需原始字节的来源记录。
        source: OwnedTypedMessage,
        /// 无需调用业务 handler 的动作。
        action: ProcessAction,
    },
    /// 构造 typed 上下文后解码失败得到的框架动作。
    ImmediateRaw {
        /// 保留 DLT 与进度身份的 typed 原始记录。
        source: RawRecord,
        /// 无需调用业务 handler 的动作。
        action: ProcessAction,
    },
    /// 已匹配 route 且逐条解码成功的记录。
    Routed {
        /// 保留失败重试与 DLT 所需原始数据。
        source: RawRecord,
        /// 匹配的冻结 route。
        route: Arc<dyn crate::consumer::erased::ErasedConsumer>,
        /// 已恢复前仍保持擦除的业务值。
        decoded: ErasedRecord,
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
    /// 返回尚未派发项的 topic-partition 与真实 offset，用于批次提前退出时回退 fetch position。
    fn location(&self) -> (Tp, i64) {
        match self {
            Self::ImmediateOwned { source, .. } => (source.tp(), source.offset),
            Self::ImmediateRaw { source, .. } | Self::Routed { source, .. } => {
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

/// 计算带确定性抖动的 consumer 指数重建退避。
///
/// # 参数
///
/// - `runtime`: 提供初始值与封顶值。
/// - `attempt`: 当前滑动窗口内第几次自动重建，从一开始。
/// - `seed`: resolved group 派生的稳定种子。
fn restart_delay(runtime: &Arc<KafkaProxyInner>, attempt: u32, seed: u64) -> Duration {
    restart_delay_values(
        runtime.config.behavior.consume_restart_delay_ms,
        runtime.config.behavior.consume_restart_max_delay_ms,
        attempt,
        seed,
    )
}

/// 用显式数值计算 consumer 指数退避，便于无 broker 边界验证。
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

/// 计算不会因左移丢高位而回绕为零的指数退避基值。
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

/// 在滑动窗口内登记一次自动重建；先淘汰过期样本，再原子判定预算。
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

/// 从非敏感稳定文本生成 FNV-1a 种子。
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

/// 自动重建退避期间维持 Stop/Restart 控制响应，不持有底层 consumer。
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
                if !GroupHandle::begin_command(&command) {
                    continue;
                }
                update_state(health, GroupState::Stopped, None);
                GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                return RestartBackoffOutcome::Stopped;
            }
            Ok(command) if matches!(command.kind, OwnerCommandKind::Restart) => {
                if GroupHandle::begin_command(&command) {
                    GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                    return RestartBackoffOutcome::Restarted;
                }
            }
            Ok(command) => {
                if GroupHandle::begin_command(&command) {
                    GroupHandle::finish_command(
                        command,
                        Err(NafkaError::Lifecycle(
                            "consumer 正在自动重建退避，仅允许 restart_group 或 stop".into(),
                        )),
                    );
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return RestartBackoffOutcome::Stopped
            }
        }
    }
}

/// owner 线程主循环。
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
    runtime: Arc<KafkaProxyInner>,
    mut plan: GroupPlan,
    commands: std::sync::mpsc::Receiver<OwnerCommand>,
    health: Arc<RwLock<GroupHealth>>,
    startup_gate: Arc<StartupGate>,
    startup_reply: tokio::sync::oneshot::Sender<Result<()>>,
    global_error: Arc<std::sync::Mutex<Option<String>>>,
) {
    let initial_consumer = match ConsumerClient::create(
        &runtime.config,
        &plan.group,
        Arc::clone(&runtime.metrics),
        Arc::clone(&global_error),
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

    let mut attempts: BTreeMap<(Tp, i64), u32> = BTreeMap::new();
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
            )
        }));
        // 会话已经结束（consumer 已析构）：立刻撤销 ready 租约，不能等到下一代入口。
        // 否则整个重建退避窗口（默认 3s，退避上限 60s）里 await_group_ready 仍会凭
        // 上一代 assignment 立即放行，而此刻底层什么都没在消费。
        invalidate_ready_snapshot(&runtime, &plan.group, &health);
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
                    GroupState::Degraded,
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

/// 在 registry/assign 原子登记 owner 前等待统一放行，并保持 Stop 命令可响应。
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
                if !GroupHandle::begin_command(&command) {
                    continue;
                }
                if matches!(&command.kind, OwnerCommandKind::Stop) {
                    update_state(health, GroupState::Stopped, None);
                    GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                    return false;
                }
                GroupHandle::finish_command(
                    command,
                    Err(NafkaError::Lifecycle(
                        "consumer owner 尚未通过 startup gate".into(),
                    )),
                );
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                update_state(health, GroupState::Stopped, None);
                return false;
            }
        }
    }
}

/// 构造并运行一次底层 consumer 会话，退出后不消费 owner 控制邮箱的所有权。
///
/// # 参数
///
/// - `runtime`: 所属运行时与冻结配置。
/// - `plan`: group 路由计划；动态订阅会更新其中的 mode。
/// - `commands`: 跨会话保留的有界控制命令接收端。
/// - `health`: 跨会话保留的健康快照。
/// - `initial_consumer`: 首轮由 startup handshake 预先构造的 consumer；重建轮次为 None。
#[allow(clippy::too_many_arguments)] // owner 会话参数本就逐项显式;第 8 个是 共享错误单槽,合并会造隐式上下文
fn run_owner_session(
    runtime_ref: &Arc<KafkaProxyInner>,
    plan: &mut GroupPlan,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    health_ref: &Arc<RwLock<GroupHealth>>,
    attempts: &mut BTreeMap<(Tp, i64), u32>,
    initial_consumer: Option<ConsumerClient>,
    epoch_base: &std::sync::atomic::AtomicU64,
    global_error: &Arc<std::sync::Mutex<Option<String>>>,
) -> OwnerSessionExit {
    let runtime = Arc::clone(runtime_ref);
    let health = Arc::clone(health_ref);
    let consumer = match initial_consumer {
        Some(consumer) => consumer,
        None => {
            match ConsumerClient::create(
                &runtime.config,
                &plan.group,
                Arc::clone(&runtime.metrics),
                Arc::clone(global_error),
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
    let mut assignment_epoch = if assignment.is_empty() { 0 } else { 1 };
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
    let mut dlt_admission: Option<PendingDltAdmission> = None;
    let (dlt_sender, mut dlt_receiver) =
        tokio::sync::mpsc::channel(runtime.config.behavior.dlt_max_in_flight.saturating_add(1));
    let mut progress = GroupOffsetState::new(
        runtime.config.behavior.max_progress_records,
        runtime.config.behavior.max_group_progress_records,
    );
    let mut last_commit = Instant::now();
    let poll_timeout = Duration::from_millis(runtime.config.behavior.poll_timeout_ms);
    let commit_interval = Duration::from_millis(runtime.config.behavior.commit_interval_ms);
    let mut partition_cursor = 0_usize;
    let mut deferred_stop: Option<OwnerCommand> = None;
    // 按分区隔离的连续解码失败计数（解码整体故障熔断，见 decode_failure_action）。
    // owner 单线程独占，成功即移除条目，规模受 assignment 约束。
    let mut decode_streaks: BTreeMap<Tp, u64> = BTreeMap::new();
    let mut transient_exit = None;
    // progress 反压门禁连续多少轮没能释放容量；只用于把长期反压升级成运维可见的 Degraded。
    let mut progress_gate_idle: u32 = 0;
    // 触发本次反压的分区；解除条件只看它自身容量，避免被无关的卡住分区拖成全组死锁。
    let mut progress_gate_trigger: Option<Tp> = None;
    let mut stopping = false;
    // 仪表采样节流：主循环每轮都跑，但 sink 是用户实现，不能按 poll 频率打。
    let mut last_gauge_sample = Instant::now() - GAUGE_SAMPLE_INTERVAL;
    while !stopping {
        // 暂停行、ready 租约都在循环内被十几处改写，逐处补发必然漏（此前只有 rebalance
        // 那一个采样点，恰恰是"什么都没暂停"的时刻）。这里按固定节奏统一重采一次。
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
        if deferred_stop.is_some() && dlt_pending.is_empty() && dlt_admission.is_none() {
            GroupHandle::finish_command(
                deferred_stop.take().expect("停止命令存在"),
                Ok(CommandReply::Unit),
            );
            stopping = true;
            continue;
        }
        activate_due_retries(
            &consumer,
            &health,
            &mut retries,
            runtime.config.behavior.consume_retry_backoff_ms,
        );
        while let Ok(command) = commands.try_recv() {
            // 先 CAS 认领命令：调用方已超时取消时直接丢弃，绝不让一个不会被执行的命令
            // 产生任何副作用。
            if !GroupHandle::begin_command(&command) {
                continue;
            }
            if deferred_stop.is_some() {
                GroupHandle::finish_command(
                    command,
                    Err(NafkaError::Lifecycle("group 正在等待 DLT 收尾".into())),
                );
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
                GroupHandle::finish_command(command, Err(error));
                continue;
            }
            let invalidates = command_invalidates_progress(&command.kind);
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
                    !dlt_pending.is_empty() || dlt_admission.is_some(),
                    &mut deferred_stop,
                );
                if stopping {
                    break;
                }
            }
            if invalidates {
                cancel_retry_states(&consumer, &health, &mut retries, &command.kind);
                if !matches!(command.kind, OwnerCommandKind::Stop) {
                    cancel_dlt_states(
                        &consumer,
                        &health,
                        &mut dlt_pending,
                        &mut dlt_admission,
                        &command.kind,
                    );
                }
            }
            if matches!(command.kind, OwnerCommandKind::Stop)
                && (!dlt_pending.is_empty() || dlt_admission.is_some())
            {
                update_state(&health, GroupState::Stopping, None);
                deferred_stop = Some(command);
                break;
            }
            stopping = execute_command(
                &consumer,
                &mut plan.mode,
                attempts,
                &health,
                &runtime.config.consumer.auto_offset_reset,
                command,
            );
            if invalidates {
                progress.clear();
            }
            if stopping {
                break;
            }
        }
        if stopping {
            break;
        }
        let admission_flow = {
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
        match admission_flow {
            MaintenanceFlow::Ready => {}
            MaintenanceFlow::Blocked => continue,
            MaintenanceFlow::StopOwner => {
                stopping = true;
                continue;
            }
        }
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
                !dlt_pending.is_empty() || dlt_admission.is_some(),
                &mut deferred_stop,
            );
            if stopping {
                continue;
            }
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
            let batch = match collect_typed_batch(&consumer, &runtime, &health, poll_timeout) {
                Ok(batch) => batch,
                Err(RdPollError::Transient(error)) => {
                    update_state(&health, GroupState::Degraded, Some(error.clone()));
                    transient_exit = Some(error);
                    break;
                }
                Err(RdPollError::Fatal(error)) => {
                    update_state(&health, GroupState::Crashed, Some(error));
                    break;
                }
            };
            health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last_poll_at = Some(SystemTime::now());
            if let Some(event) = consumer.take_rebalance() {
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
                if !batch.items.is_empty() {
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
                        let failed = consumer.rewind_offsets(&rewind);
                        halt_failed_rewinds(&consumer, &health, &failed);
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
            if batch.items.is_empty() {
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
                        !dlt_pending.is_empty() || dlt_admission.is_some(),
                        &mut deferred_stop,
                    );
                }
                continue;
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
            );
            continue;
        }
        let message = match consumer.poll(poll_timeout) {
            Ok(message) => message,
            Err(RdPollError::Transient(error)) => {
                update_state(&health, GroupState::Degraded, Some(error.clone()));
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
                    !dlt_pending.is_empty() || dlt_admission.is_some(),
                    &mut deferred_stop,
                );
            }
            continue;
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
        GroupHandle::finish_command(command, result);
    }
    for (_, pending) in dlt_pending {
        pending.abort.abort();
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

/// 在 Crashed 状态只接受 Restart 或 Stop，避免其他控制操作获得虚假成功。
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
        if !GroupHandle::begin_command(&command) {
            continue;
        }
        match &command.kind {
            OwnerCommandKind::Restart => {
                update_state(health, GroupState::Starting, None);
                GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                return true;
            }
            OwnerCommandKind::Stop => {
                update_state(health, GroupState::Stopped, None);
                GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                return false;
            }
            _ => GroupHandle::finish_command(
                command,
                Err(NafkaError::Lifecycle(
                    "group 已 Crashed，仅允许 restart_group 或 stop".into(),
                )),
            ),
        }
    }
}

/// 初始化 subscribe 或固定 assign。
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

/// 把 consumer 构造/初始化错误集中映射为会话退出分类。
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
            update_state(health, GroupState::Degraded, Some(message.clone()));
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

/// 启动延迟期间维持命令响应和 consumer 心跳。
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
                if !GroupHandle::begin_command(&command) {
                    continue;
                }
                GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                return false;
            }
            if GroupHandle::begin_command(&command) {
                GroupHandle::finish_command(
                    command,
                    Err(NafkaError::Lifecycle("consumer 尚在启动延迟阶段".into())),
                );
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

/// 拉取一次有界 typed 逻辑批：首条等待，后续使用零等待 drain。
///
/// 达到字节上限时，已经被底层返回但尚未复制的边界记录会立即 seek 回原 offset，留给下一批；
/// 因而 owner 不需要持有一个额外的超上限 lookahead。
///
/// # 参数
///
/// - `consumer`: owner 唯一 consumer。
/// - `runtime`: 提供记录数、字节数与单记录上限。
/// - `health`: 最近 poll 时间的健康快照。
/// - `first_timeout`: 首条消息最长等待时间。
///
/// # 错误
///
/// poll、边界 seek 或字节计量失败时返回错误。
fn collect_typed_batch(
    consumer: &ConsumerClient,
    runtime: &Arc<KafkaProxyInner>,
    health: &Arc<RwLock<GroupHealth>>,
    first_timeout: Duration,
) -> std::result::Result<LogicalTypedBatch, RdPollError> {
    let mut batch = LogicalTypedBatch {
        items: Vec::with_capacity(runtime.config.behavior.max_batch_records),
        retained_bytes: 0,
    };
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
        // headers 只解析一次：底层对非 UTF-8 header 名会 panic（由 RdMessage::headers 兜住），
        // 重复调用等于对同一条坏记录反复 panic/unwind，既有噪声也有额外开销。
        let headers = message.headers();
        let retained = message.retained_bytes_with(headers.as_ref().ok());
        if retained > runtime.config.behavior.max_record_bytes {
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
            batch.retained_bytes = next_bytes;
            batch
                .items
                .push(TypedBatchItem::Owned(OwnedTypedMessage::copy_from(
                    &message, &headers,
                )));
        }
        if batch.items.len() >= runtime.config.behavior.max_batch_records
            || consumer.has_rebalance()
        {
            break;
        }
    }
    Ok(batch)
}

/// 执行一个通过 fencing 的 owner 控制命令。
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
    attempts: &mut BTreeMap<(Tp, i64), u32>,
    health: &Arc<RwLock<GroupHealth>>,
    auto_offset_reset: &str,
    command: OwnerCommand,
) -> bool {
    // 调用方已在 owner 主循环里完成 begin_command 与 admission 裁决；到这里的命令一定会执行。
    let stop = matches!(&command.kind, OwnerCommandKind::Stop);
    let command_id = command.id;
    let result = match &command.kind {
        OwnerCommandKind::Stop => {
            update_state(health, GroupState::Stopping, None);
            Ok(CommandReply::Unit)
        }
        OwnerCommandKind::Seek { offsets } => consumer.seek_offsets(offsets).map(|()| {
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
        OwnerCommandKind::SeekCommitted { tps } => consumer
            .seek_to_committed(tps, auto_offset_reset)
            .map(|()| {
                clear_attempts(attempts, tps.iter());
                clear_operator_pauses(consumer, health, tps.iter());
                CommandReply::Unit
            }),
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
            let targets = resumable_user_targets(consumer, health, tps);
            consumer.resume(&targets).map(|resumed| {
                set_user_paused(health, &resumed, false);
                CommandReply::Unit
            })
        }
        OwnerCommandKind::Position { tps } => consumer.position(tps).map(CommandReply::Positions),
        OwnerCommandKind::Assignment => consumer.assignment().map(CommandReply::Assignment),
        OwnerCommandKind::Subscribe { topics } => match mode {
            OwnerMode::Subscribe { .. } => consumer.subscribe(topics).map(|()| {
                *mode = OwnerMode::Subscribe {
                    topics: topics.clone(),
                };
                attempts.clear();
                CommandReply::Unit
            }),
            OwnerMode::Assign { .. } => Err(NafkaError::Lifecycle(
                "固定 assign group 不支持 subscribe_group".into(),
            )),
        },
        OwnerCommandKind::Unsubscribe => match mode {
            OwnerMode::Subscribe { .. } => {
                consumer.unsubscribe();
                attempts.clear();
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
    GroupHandle::finish_command(command, result);
    tracing::debug!(command_id, "consumer owner 控制命令已完成");
    stop
}

/// 在产生任何副作用之前裁决一条控制命令能否执行。
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
/// - `admission`: 唯一 owner-local DLT 准入任务。
///
/// # 返回
///
/// 可以执行时返回 `None`；否则返回应原样回给调用方的错误。
fn command_admission_error(
    kind: &OwnerCommandKind,
    mode: &OwnerMode,
    health: &Arc<RwLock<GroupHealth>>,
    pending: &BTreeMap<Tp, PendingDlt>,
    admission: &Option<PendingDltAdmission>,
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

/// 判断 seek 是否会覆盖尚未得到 durability-first 终态的 DLT 分区。
///
/// - `kind`: 待执行控制命令；只检查五种 seek。
/// - `pending`: 已准入并由 worker 投递的 DLT。
/// - `admission`: 尚在等待全局 permit 的唯一 DLT。
fn seek_conflicts_with_dlt(
    kind: &OwnerCommandKind,
    pending: &BTreeMap<Tp, PendingDlt>,
    admission: &Option<PendingDltAdmission>,
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
        || admission
            .as_ref()
            .is_some_and(|item| all || targets.contains(&item.tp))
}

/// 清理指定分区的本地失败尝试状态。
///
/// # 参数
///
/// - `attempts`: owner 尝试表。
/// - `tps`: 已被 operator 改变位置的分区。
fn clear_attempts<'a>(
    attempts: &mut BTreeMap<(Tp, i64), u32>,
    tps: impl IntoIterator<Item = &'a Tp>,
) {
    let targets: BTreeSet<Tp> = tps.into_iter().cloned().collect();
    attempts.retain(|(tp, _), _| !targets.contains(tp));
}

/// operator seek 后移除框架安全暂停，并在没有 User 原因时恢复底层分区。
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

/// 只返回移除 User 原因后没有任何框架暂停原因的分区。
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

/// 把会话内 epoch 换算成跨会话单调的对外 epoch。
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

/// group 仪表重采样间隔；下限由用户 MetricsSink 的实现成本决定，不按 poll 频率打。
const GAUGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// 发布一次真实 assign 后的健康与 ready 快照。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `assignment`: callback 或固定 assign 给出的分区。
/// - `epoch`: 本次 assignment epoch。
fn publish_assignment_health(
    runtime: &Arc<KafkaProxyInner>,
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
    // 先取值再释放锁：gauge 会调用**用户实现**的 MetricsSink，绝不能在持有 health 锁时执行。
    // 慢实现会卡住所有 group_health() 查询；若它回调进来再取读锁，写偏好的 RwLock 下
    // 还可能与排队中的写者形成死锁。
    publish_health_gauges(runtime, group, health);
}

/// 从 health 快照采样一次并发布 group 级仪表。
///
/// **所有** ready 租约与暂停行的观测都必须走这里，原因是各写各的必然漂移：
/// 此前 4 个改 ready 租约的点只有 2 个碰 gauge（`Revoked` 分支清了租约却不发 0，
/// eager 撤销后若重新 join 卡住，gauge 就永远停在 1）；而 `paused_partitions`
/// 唯一的采样点在 assignment 发布内，即"什么都没暂停"的那一刻——
/// 稳态下 halt 掉 12 个分区也永远报的是若干天前采到的 0。
///
/// 语义定义（写在这里是因为它必须唯一）：
/// - `group_ready` = 是否持有有效 assignment 租约。空 assignment 也算 1，
///   那是竞争组 standby 的合法形态；要区分 standby 看 `assigned_partitions`。
/// - `paused_partitions` 数的是**分区数**不是暂停行数：同一分区可同时挂
///   `User` 与 `Halt` 两行，按行数会翻倍。
///
/// # 参数
///
/// - `runtime`: 指标出口。
/// - `group`: group.id 标签。
/// - `health`: 共享健康快照。
fn publish_health_gauges(
    runtime: &Arc<KafkaProxyInner>,
    group: &str,
    health: &Arc<RwLock<GroupHealth>>,
) {
    // 先取值再释放锁：gauge 会调用**用户实现**的 MetricsSink，绝不能在持有 health 锁时执行。
    // 慢实现会卡住所有 group_health() 查询；若它回调进来再取读锁，写偏好的 RwLock 下
    // 还可能与排队中的写者形成死锁。
    let (ready, assigned, paused) = {
        let snapshot = health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let paused: BTreeSet<&Tp> = snapshot.paused.iter().map(|(tp, _)| tp).collect();
        (
            i64::from(snapshot.ready_assignment_epoch.is_some()),
            snapshot.assignment.len(),
            paused.len(),
        )
    };
    let labels = [("group", group)];
    runtime.gauge("group_ready", ready, &labels);
    runtime.gauge(
        "assigned_partitions",
        i64::try_from(assigned).unwrap_or(i64::MAX),
        &labels,
    );
    runtime.gauge(
        "paused_partitions",
        i64::try_from(paused).unwrap_or(i64::MAX),
        &labels,
    );
}

/// 撤销旧底层会话发布的 ready 快照。
///
/// - `health`: 跨会话共享的 group 健康状态。
fn invalidate_ready_snapshot(
    runtime: &Arc<KafkaProxyInner>,
    group: &str,
    health: &Arc<RwLock<GroupHealth>>,
) {
    {
        let mut snapshot = health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshot.assignment.clear();
        snapshot.ready_assignment_epoch = None;
        snapshot.last_assignment_at = Some(SystemTime::now());
    }
    publish_health_gauges(runtime, group, health);
}

/// 把跨 rebalance/会话保留的 User/Halt 事实重新下发到底层 consumer。
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

/// 应用 owner mailbox 中的一次实际 rebalance 结果。
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
    runtime: &Arc<KafkaProxyInner>,
    health: &Arc<RwLock<GroupHealth>>,
    assignment: &mut Vec<Tp>,
    epoch: &mut u64,
    progress: &mut GroupOffsetState,
    attempts: &mut BTreeMap<(Tp, i64), u32>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    dlt_pending: &mut BTreeMap<Tp, PendingDlt>,
    dlt_admission: &mut Option<PendingDltAdmission>,
    pending_ready_epoch: &mut Option<u64>,
    event: RdRebalanceEvent,
) -> bool {
    match event.kind {
        RdRebalanceKind::Assigned => {
            let max = runtime.config.consumer.max_assignment_partitions;
            if event.partitions.len() > max {
                let _ = consumer.unassign();
                update_state(
                    health,
                    GroupState::Crashed,
                    Some(
                        NafkaError::AssignmentTooLarge {
                            group: group.to_owned(),
                            actual: event.partitions.len(),
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
            // DLT worker 的 completion 用 (tp, assignment_epoch, first_offset) 三元组配对，
            // 因此跨 epoch 保留 worker 会让 completion 永远配不上、分区卡死在 DltPending。
            // 这里仍然按旧规则作废：记录会在新 epoch 下从 committed 位置重新消费并重投 DLT，
            // 代价是可能出现重复死信，换取状态机自洽。
            let stale_dlt = dlt_pending
                .iter()
                .filter(|(tp, pending)| {
                    !assigned.contains(tp) || pending.assignment_epoch != event.epoch
                })
                .map(|(tp, _)| tp.clone())
                .collect::<Vec<_>>();
            for tp in stale_dlt {
                if let Some(pending) = dlt_pending.remove(&tp) {
                    pending.abort.abort();
                }
            }
            if dlt_admission
                .as_ref()
                .is_some_and(|pending| pending.assignment_epoch != event.epoch)
            {
                // 门禁记录的回退 offset 属于**其它仍持有的分区**（门禁竖起时已 poll 未派发），
                // 只有 finish_dlt_admission_gate 会 seek 它们；直接丢结构体等于让这些记录被
                // 后续安全前沿跨过。作废前先把回退落到底层。
                if let Some(pending) = dlt_admission.take() {
                    let retained = pending
                        .discarded
                        .into_iter()
                        .filter(|(tp, _)| assigned.contains(tp))
                        .collect::<BTreeMap<_, _>>();
                    if !retained.is_empty() {
                        let failed = consumer.rewind_offsets(&retained);
                        halt_failed_rewinds(consumer, health, &failed);
                    }
                }
            }
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
            for tp in &revoked {
                progress.reset_partition(tp);
            }
            publish_commit_snapshot(consumer, progress);
            attempts.retain(|(tp, _), _| !revoked.contains(tp));
            retries.retain(|tp, _| !revoked.contains(tp));
            for tp in &revoked {
                if let Some(pending) = dlt_pending.remove(tp) {
                    pending.abort.abort();
                }
            }
            if dlt_admission
                .as_ref()
                .is_some_and(|pending| revoked.contains(&pending.tp))
            {
                // 同 Assigned 分支：丢弃门禁前把仍持有分区的回退 offset 落到底层。
                if let Some(pending) = dlt_admission.take() {
                    let retained = pending
                        .discarded
                        .into_iter()
                        .filter(|(tp, _)| !revoked.contains(tp))
                        .collect::<BTreeMap<_, _>>();
                    if !retained.is_empty() {
                        let failed = consumer.rewind_offsets(&retained);
                        halt_failed_rewinds(consumer, health, &failed);
                    }
                }
            }
            // revoke-only 的 rebalance（cooperative 协议）不会跟随 Assigned，
            // 保留分区上残留的框架瞬时暂停（CommitBlocked / DltBackpressure /
            // ProgressBackpressure）没有任何其它清除路径，会造成"health 显示 Running、
            // 底层永久 paused、committed 冻结"。这里对**保留分区**做一次恢复基线。
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
            *pending_ready_epoch = None;
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

/// 判断控制命令是否会让当前本地 progress 失效。
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

/// 控制命令改变 position/assignment 前清理其覆盖范围内的退避状态。
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

/// position/assignment 控制命令覆盖旧处理事实前取消对应 DLT worker 与 local pending。
///
/// # 参数
///
/// - `health`: group 健康快照。
/// - `pending`: 已准入的每分区 DLT worker。
/// - `admission`: owner-local 准入任务。
/// - `kind`: 即将执行的控制命令。
fn cancel_dlt_states(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    pending: &mut BTreeMap<Tp, PendingDlt>,
    admission: &mut Option<PendingDltAdmission>,
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
            task.abort.abort();
        }
    }
    let mut gate_cleared = Vec::new();
    if admission.as_ref().is_some_and(|item| {
        targets
            .as_ref()
            .is_none_or(|targets| targets.contains(&item.tp))
    }) {
        // 丢弃门禁前先把它记录的回退 offset 落到底层：这些是**其它仍持有分区**在门禁竖起时
        // 已 poll 未派发的记录，只有 finish_dlt_admission_gate 会 seek 它们，直接丢结构体
        // 等于把那些记录跳过去（与 的"解除反压前按安全前沿重新 seek"冲突）。
        if let Some(item) = admission.take() {
            if !item.discarded.is_empty() {
                let failed = consumer.rewind_offsets(&item.discarded);
                halt_failed_rewinds(consumer, health, &failed);
            }
            gate_cleared = consumer.assignment().unwrap_or_default();
        }
    }
    // 摘掉 health 的 DLT 暂停行必须同步 resume，否则控制面显示未暂停而分区在底层永久停流。
    let mut resumable = clear_pause_reason(health, &cancelled, &PauseReason::DltPending);
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

/// 记录一次业务安全处理完成，但不声称 broker 已提交。
///
/// # 参数
///
/// - `health`: group 健康快照。
fn note_success(health: &Arc<RwLock<GroupHealth>>) {
    let mut snapshot = health
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.state = GroupState::Running;
    snapshot.last_success_at = Some(SystemTime::now());
    snapshot.last_error = None;
}

/// 累计一次底层 consumer 会话创建；首次之后的尝试同时计入重建次数。
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

/// 累计一次真实 broker commit 结果，供长稳与吞吐门禁计算 commits/record。
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

/// 把 owner 当前安全前沿发布给 pre-rebalance callback。
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

/// 记录维护性 poll 的会话级错误，并保留自动重建或显式恢复所需分类。
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
            update_state(health, GroupState::Degraded, Some(message.clone()));
            *transient_exit = Some(message);
        }
        RdPollError::Fatal(message) => {
            update_state(health, GroupState::Crashed, Some(message));
        }
    }
}

/// 同步提交当前全部安全前沿；失败期间停止业务派发并维护心跳。
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
    attempts: &mut BTreeMap<(Tp, i64), u32>,
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

/// [`commit_until_resolved`] 的实际循环体；维护性 poll 取出的记录记入 `discarded`。
#[allow(clippy::too_many_arguments)]
fn commit_until_resolved_inner(
    consumer: &ConsumerClient,
    progress: &mut GroupOffsetState,
    health: &Arc<RwLock<GroupHealth>>,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    mode: &mut OwnerMode,
    attempts: &mut BTreeMap<(Tp, i64), u32>,
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
                if !GroupHandle::begin_command(&command) {
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
                GroupHandle::finish_command(command, Ok(CommandReply::Unit));
                return true;
            }
            if GroupHandle::begin_command(&command) {
                GroupHandle::finish_command(
                    command,
                    Err(NafkaError::Lifecycle(
                        "commit 未确认期间只接受停止命令".into(),
                    )),
                );
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

/// 接纳单条 outcome；容量不足时先提交安全前沿，仍不足则进入可自动恢复的 group 门禁。
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
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

/// 原子接纳同分区 run，并复用单条路径的 commit-first 容量恢复语义。
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
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

/// 处理批级回退失败：仍被本 owner 持有的分区位置已不可信，必须显式 Halt。
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

/// 判断 progress 反压门禁是否仍然竖着。
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

/// 暂停当前 assignment 并进入容量门禁，等待 commit/DLT 收尾释放 permit。
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

/// 将单条框架动作接入连续水位、重试、DLT 与健康状态机。
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
                    !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
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

/// 应用单条 handler 可重试失败，并在耗尽时转入 DLT。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `source`: 原始消息视图。
/// - `tp`: 来源 topic-partition。
/// - `offset`: 失败 offset。
/// - `error`: 已脱敏失败原因。
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
        context.deferred_stop,
    ) {
        return ActionFlow::StopOwner;
    }
    context.progress.reset_partition(tp);
    publish_commit_snapshot(context.consumer, context.progress);
    if !context.attempts.contains_key(&(tp.clone(), offset))
        && context.attempts.len() >= context.runtime.config.behavior.max_retry_entries
    {
        halt_partition(
            context.consumer,
            context.health,
            tp,
            HaltReason::ProgressInvariant,
            "消费重试状态表达到硬上限".into(),
        );
        return ActionFlow::StopPartition;
    }
    let count = context.attempts.entry((tp.clone(), offset)).or_insert(0);
    *count = count.saturating_add(1);
    let exhausted = context.runtime.config.behavior.max_consume_attempts != 0
        && *count >= context.runtime.config.behavior.max_consume_attempts;
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

/// 应用确定坏消息的直接 DLT 动作，不消耗 handler 尝试次数。
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
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

/// 在显式 availability-first 模式下记录安全跳过并提交该边界。
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
        context.deferred_stop,
    ) {
        ActionFlow::StopOwner
    } else {
        ActionFlow::Continue
    }
}

/// 原子记录一个成功 handler run 的安全前缀。
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
            !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
            context.deferred_stop,
        )
    {
        ActionFlow::StopOwner
    } else {
        ActionFlow::Continue
    }
}

/// 将一次 single/batch handler 调用结果映射为安全前缀或连续失败后缀。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `route`: 本次调用的冻结 route。
/// - `sources`: 与 handler 输入一一对应的原始记录。
/// - `outcome`: 擦除 adapter 返回的严格确认结果。
fn apply_invocation_outcome(
    context: &mut ActionContext<'_>,
    route: &Arc<dyn crate::consumer::erased::ErasedConsumer>,
    sources: &[RawRecord],
    outcome: InvocationOutcome,
) -> ActionFlow {
    match outcome {
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
                )
            }
        }
        InvocationOutcome::Failed(error) => {
            context.runtime.counter(
                "handler_failed_total",
                1,
                &[("group", context.group), ("consumer_id", route.meta().id)],
            );
            apply_retry_run(context, sources, error.to_string())
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

/// 应用 BatchConsumer 的连续失败 run；尝试次数、seek 和 DLT 都绑定首 offset。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `sources`: 同分区且按交付顺序排列的非空失败后缀。
/// - `error`: 已脱敏失败原因。
fn apply_retry_run(
    context: &mut ActionContext<'_>,
    sources: &[RawRecord],
    error: String,
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
        context.deferred_stop,
    ) {
        return ActionFlow::StopOwner;
    }
    context.progress.reset_partition(&tp);
    publish_commit_snapshot(context.consumer, context.progress);
    if !context.attempts.contains_key(&(tp.clone(), first_offset))
        && context.attempts.len() >= context.runtime.config.behavior.max_retry_entries
    {
        halt_partition(
            context.consumer,
            context.health,
            &tp,
            HaltReason::ProgressInvariant,
            "消费重试状态表达到硬上限".into(),
        );
        return ActionFlow::StopPartition;
    }
    let count = context
        .attempts
        .entry((tp.clone(), first_offset))
        .or_insert(0);
    *count = count.saturating_add(1);
    context.runtime.counter(
        "retried_total",
        u64::try_from(sources.len()).unwrap_or(u64::MAX),
        &[("group", context.group), ("topic", tp.topic.as_str())],
    );
    let exhausted = context.runtime.config.behavior.max_consume_attempts != 0
        && *count >= context.runtime.config.behavior.max_consume_attempts;
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
    schedule_retry_flow(context, &tp, first_offset)
}

/// 把单条消息构造成 DLT job 并接入非阻塞准入/worker 流程。
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
    let record = build_dlt_record(context.runtime, context.group, source, reason)?;
    schedule_dlt_job(
        context,
        tp,
        vec![offset],
        DltJob::from_records(vec![record])?,
    )
}

/// 把一个 typed 连续失败 run 构造成单个 DLT job 并非阻塞调度。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `sources`: 同分区、按交付顺序排列的失败 run。
/// - `reason`: 已脱敏失败原因。
///
/// # 错误
///
/// run 为空、递归 DLT、job 构造、pause/seek 或准入失败时返回错误。
fn schedule_dlt_run(
    context: &mut ActionContext<'_>,
    sources: &[RawRecord],
    reason: &str,
) -> Result<()> {
    let first = sources
        .first()
        .ok_or_else(|| NafkaError::Lifecycle("DLT 失败 run 不能为空".into()))?;
    let records = sources
        .iter()
        .map(|source| build_dlt_record(context.runtime, context.group, source, reason))
        .collect::<Result<Vec<_>>>()?;
    let offsets = sources
        .iter()
        .map(|source| source.ctx.offset)
        .collect::<Vec<_>>();
    schedule_dlt_job(
        context,
        first.ctx.topic_partition(),
        offsets,
        DltJob::from_records(records)?,
    )
}

/// 对已构造 job 执行无等待全局准入；容量不足时只留下一个 owner-local pending。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `tp`: 来源分区。
/// - `offsets`: 失败单元真实 offsets。
/// - `job`: 已有界构造的 DLT job。
///
/// # 错误
///
/// 同分区已有任务、owner 已持有 local pending、pause/seek 或 dispatcher 失败时返回错误。
fn schedule_dlt_job(
    context: &mut ActionContext<'_>,
    tp: Tp,
    offsets: Vec<i64>,
    job: DltJob,
) -> Result<()> {
    let first_offset = *offsets
        .first()
        .ok_or_else(|| NafkaError::Lifecycle("DLT offsets 不能为空".into()))?;
    if context.dlt_pending.contains_key(&tp)
        || context
            .dlt_admission
            .as_ref()
            .is_some_and(|pending| pending.tp == tp)
    {
        return Err(NafkaError::Lifecycle(format!(
            "分区已有 DLT 任务: {}:{}",
            tp.topic, tp.partition
        )));
    }
    context.consumer.pause(std::slice::from_ref(&tp))?;
    add_pause_reason(
        context.health,
        std::slice::from_ref(&tp),
        PauseReason::DltPending,
    );
    context
        .consumer
        .seek_offsets(&BTreeMap::from([(tp.clone(), first_offset)]))?;
    let dispatcher = context
        .runtime
        .dlt_dispatcher
        .get()
        .ok_or_else(|| NafkaError::Lifecycle("DLT dispatcher 尚未初始化".into()))?
        .clone();
    match dispatcher.try_admit(job)? {
        DltAdmission::Admitted(envelope) => {
            spawn_dlt_worker(context, dispatcher, envelope, tp, offsets);
        }
        DltAdmission::Backpressured(job) => {
            if context.dlt_admission.is_some() {
                return Err(NafkaError::Lifecycle(
                    "owner 已有一个 DLT local pending".into(),
                ));
            }
            let all = context
                .consumer
                .assignment()
                .unwrap_or_else(|_| vec![tp.clone()]);
            context.consumer.pause(&all)?;
            add_pause_reason(context.health, &all, PauseReason::DltBackpressure);
            update_state(context.health, GroupState::DltBackpressure, None);
            *context.dlt_admission = Some(PendingDltAdmission {
                job,
                tp,
                assignment_epoch: context.assignment_epoch,
                offsets,
                discarded: BTreeMap::new(),
            });
        }
    }
    Ok(())
}

/// 启动一个已经取得全局 permit 的 DLT worker，并登记分区取消身份。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `dispatcher`: 全运行时共享 dispatcher。
/// - `envelope`: 已取得 job/byte permit 的任务信封。
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
    let sender = context.dlt_sender.clone();
    let health = Arc::clone(context.health);
    let completion_tp = tp.clone();
    let runtime = Arc::clone(context.runtime);
    emit_dlt_queue_usage(context.runtime, &dispatcher);
    let span = tracing::info_span!(
        "kafka.dlt",
        origin_topic = %tp.topic,
        partition = tp.partition,
        offset = first_offset,
        reason = "handler_or_invalid"
    );
    let task = context.runtime.runtime.spawn(
        async move {
            let result = deliver_dlt_envelope(
                &dispatcher,
                &envelope,
                required,
                initial_ms,
                max_ms,
                degraded_after,
                first_offset,
                &health,
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
            abort: task.abort_handle(),
        },
    );
}

/// 发布全运行时 DLT 有界队列的瞬时占用。
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

/// 在异步 worker 内执行 DLT delivery；durability-first 模式使用同一信封无限外层重试。
///
/// # 参数
///
/// - `dispatcher`: 全运行时共享 dispatcher。
/// - `envelope`: 已取得全局 permit 的任务信封。
/// - `required`: 是否 durability-first。
/// - `initial_ms`: 指数退避初始毫秒。
/// - `max_ms`: 指数退避封顶毫秒。
/// - `degraded_after`: 连续失败进入 Degraded 的次数。
/// - `seed`: 稳定抖动种子。
/// - `health`: group 健康快照。
///
/// # 错误
///
/// availability-first 的首次 delivery 失败，或 dispatcher 永久关闭时返回错误。
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
) -> Result<()> {
    let mut attempt = 0_u32;
    loop {
        attempt = attempt.saturating_add(1);
        match dispatcher.deliver(envelope).await {
            Ok(()) => return Ok(()),
            Err(error) if !required => return Err(error),
            Err(error) => {
                if attempt >= degraded_after {
                    update_state(
                        health,
                        GroupState::Degraded,
                        Some(format!("DLT delivery 连续失败 attempt={attempt}: {error}")),
                    );
                }
                tokio::time::sleep(dlt_retry_delay_values(initial_ms, max_ms, attempt, seed)).await;
            }
        }
    }
}

/// 优先收割异步 DLT completion，并在 delivery 成功后生成终态 outcome 与同步提交。
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
            !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
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
        let resumable = clear_pause_reason(
            context.health,
            std::slice::from_ref(&completion.tp),
            &PauseReason::DltPending,
        );
        let _ = context.consumer.resume(&resumable);
        note_success(context.health);
    }
    false
}

/// 推进 owner 唯一的 DLT local pending 准入；未取得 permit 时只做一次短维护 poll。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
///
/// # 返回
///
/// 返回门禁是否解除，或维护性 poll 是否要求结束当前会话。
fn service_dlt_admission(context: &mut ActionContext<'_>) -> MaintenanceFlow {
    let Some(mut pending) = context.dlt_admission.take() else {
        return MaintenanceFlow::Ready;
    };
    if pending.assignment_epoch != context.assignment_epoch {
        finish_dlt_admission_gate(context, &pending.discarded);
        return MaintenanceFlow::Ready;
    }
    let dispatcher = match context.runtime.dlt_dispatcher.get() {
        Some(dispatcher) => dispatcher.clone(),
        None => {
            halt_partition(
                context.consumer,
                context.health,
                &pending.tp,
                HaltReason::DltInfrastructure,
                "DLT dispatcher 尚未初始化".into(),
            );
            finish_dlt_admission_gate(context, &pending.discarded);
            return MaintenanceFlow::Ready;
        }
    };
    match dispatcher.try_admit(pending.job) {
        Ok(DltAdmission::Admitted(envelope)) => {
            finish_dlt_admission_gate(context, &pending.discarded);
            spawn_dlt_worker(context, dispatcher, envelope, pending.tp, pending.offsets);
            MaintenanceFlow::Ready
        }
        Ok(DltAdmission::Backpressured(job)) => {
            pending.job = job;
            match context.consumer.poll(Duration::from_millis(50)) {
                Ok(Some(message)) => {
                    let tp = Tp::new(message.topic(), message.partition());
                    pending
                        .discarded
                        .entry(tp)
                        .and_modify(|offset| *offset = (*offset).min(message.offset()))
                        .or_insert(message.offset());
                }
                Ok(None) => {}
                Err(error) => {
                    record_maintenance_poll_error(context.health, context.transient_exit, error);
                    *context.dlt_admission = Some(pending);
                    return MaintenanceFlow::StopOwner;
                }
            }
            context
                .health
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last_poll_at = Some(SystemTime::now());
            *context.dlt_admission = Some(pending);
            MaintenanceFlow::Blocked
        }
        Err(error) => {
            halt_partition(
                context.consumer,
                context.health,
                &pending.tp,
                HaltReason::DltInfrastructure,
                error.to_string(),
            );
            finish_dlt_admission_gate(context, &pending.discarded);
            MaintenanceFlow::Ready
        }
    }
}

/// 清理 group 级 DLT 准入门禁，同时恢复维护性 poll 取出的记录位置。
///
/// # 参数
///
/// - `context`: owner 本轮共享状态。
/// - `discarded`: 门禁期间每分区已取出但未派发的最小 offset。
fn finish_dlt_admission_gate(context: &mut ActionContext<'_>, discarded: &BTreeMap<Tp, i64>) {
    if !discarded.is_empty() {
        let failed = context.consumer.rewind_offsets(discarded);
        halt_failed_rewinds(context.consumer, context.health, &failed);
    }
    let assignment = context.consumer.assignment().unwrap_or_default();
    let resumable = clear_pause_reason(context.health, &assignment, &PauseReason::DltBackpressure);
    let _ = context.consumer.resume(&resumable);
}

/// availability-first 地把整个失败 run 标为安全跳过并提交末尾边界。
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
        !context.dlt_pending.is_empty() || context.dlt_admission.is_some(),
        context.deferred_stop,
    );
    context.attempts.remove(&(tp, first_offset));
    if stopping {
        ActionFlow::StopOwner
    } else {
        ActionFlow::Continue
    }
}

/// 为目标分区添加一个不重复的框架暂停原因。
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

/// 移除 CommitBlocked 原因并恢复没有其他暂停原因的分区。
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

/// 对一条拥有型 typed 消息完成 event 路由、codec 校验、上下文构造与逐条解码。
///
/// # 参数
///
/// - `runtime`: 提供无匹配和无效记录策略。
/// - `plan`: 当前 resolved group 的冻结 route 计划。
/// - `assignment_epoch`: 本次逻辑批所属 assignment epoch。
/// - `decode_streaks`: owner 会话局部的按分区连续解码失败表（解码整体故障熔断，见
///   `decode_failure_action`）。
/// - `message`: 已受 records/bytes 双上限约束的拥有型消息。
fn prepare_typed_message(
    runtime: &Arc<KafkaProxyInner>,
    plan: &GroupPlan,
    assignment_epoch: u64,
    decode_streaks: &mut BTreeMap<Tp, u64>,
    message: OwnedTypedMessage,
) -> PreparedTypedItem {
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
    let event = match message.headers.last(HEADER_EVENT) {
        None => DEFAULT_EVENT,
        Some(header) => match header.value.as_deref() {
            None | Some([]) => DEFAULT_EVENT,
            Some(value) => match std::str::from_utf8(value) {
                Ok(value) => value,
                Err(_) => {
                    return PreparedTypedItem::ImmediateOwned {
                        source: message,
                        action: invalid_action(
                            runtime,
                            crate::consumer::InvalidRecordReason::MalformedRoute,
                            "event header 不是 UTF-8",
                        ),
                    }
                }
            },
        },
    };
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
        return PreparedTypedItem::ImmediateOwned {
            source: message,
            action: ProcessAction::Safe(RecordOutcome::Skipped(SkipReason::Tombstone)),
        };
    }
    if let Err(error) = validate_owned_codec(route.codec(), &message.headers) {
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
        topic,
        partition,
        offset,
        timestamp,
        key,
        headers,
        malformed_headers: _,
        payload,
    } = message;
    let key = match key {
        Some(bytes) => match String::from_utf8(bytes) {
            Ok(value) => Some(value),
            Err(error) => {
                return PreparedTypedItem::ImmediateOwned {
                    source: OwnedTypedMessage {
                        topic,
                        partition,
                        offset,
                        timestamp,
                        key: Some(error.into_bytes()),
                        headers,
                        malformed_headers: false,
                        payload,
                    },
                    action: invalid_or_dlt(runtime, "key 不是 UTF-8".into()),
                };
            }
        },
        None => None,
    };
    let raw = RawRecord {
        payload,
        ctx: ConsumeCtx {
            topic,
            partition,
            offset,
            timestamp,
            key,
            headers,
            passthrough,
        },
        assignment_epoch,
    };
    let tp = Tp::new(&raw.ctx.topic, raw.ctx.partition);
    match route.decode(&raw) {
        Ok(decoded) => {
            note_decode_success(decode_streaks, &tp);
            PreparedTypedItem::Routed {
                source: raw,
                route,
                decoded,
            }
        }
        Err(error) => {
            runtime.counter(
                "decode_failed_total",
                1,
                &[("group", &plan.group), ("consumer_id", route.meta().id)],
            );
            PreparedTypedItem::ImmediateRaw {
                source: raw,
                action: decode_failure_action(runtime, decode_streaks, &tp, error.to_string()),
            }
        }
    }
}

/// 从透传上下文提取低基数 trace id；缺失或非字符串时返回空串。
fn consume_trace_id(ctx: &ConsumeCtx) -> &str {
    ctx.passthrough
        .get("traceId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

/// 驱动一次类型擦除 single/batch handler future，并应用统一超时边界。
///
/// # 参数
///
/// - `runtime`: 提供 owner 外部异步运行时与 handler 超时。
/// - `route`: 本次调用的冻结 route。
/// - `records`: 同分区、同 route 的非空连续交付 run。
fn invoke_typed_handler(
    runtime: &Arc<KafkaProxyInner>,
    route: &Arc<dyn crate::consumer::erased::ErasedConsumer>,
    records: Vec<ErasedRecord>,
) -> InvocationOutcome {
    let future = route.invoke(records);
    if let Some(timeout) = runtime.config.behavior.handler_timeout_ms {
        runtime.runtime.block_on(async {
            match tokio::time::timeout(Duration::from_millis(timeout), future).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    InvocationOutcome::Failed(NafkaError::Broker("consumer handler timeout".into()))
                }
            }
        })
    } else {
        runtime.runtime.block_on(future)
    }
}

/// 按 partition-first 与轮转起点处理一次 typed 逻辑批。
///
/// # 参数
///
/// - `runtime`: 所属运行时。
/// - `consumer`: owner 唯一 consumer。
/// - `plan`: 当前 group 冻结计划。
/// - `health`: group 健康快照。
/// - `commands`: owner 控制命令接收端。
/// - `attempts`: 本进程失败尝试表。
/// - `progress`: 连续安全水位。
/// - `last_commit`: 最近一次成功提交时间。
/// - `assignment_epoch`: 当前 assignment epoch。
/// - `batch`: 已完成 records/bytes 双门禁的逻辑批。
/// - `partition_cursor`: 跨批轮转分区起点。
/// - `transient_exit`: 维护性 poll 捕获的临时会话错误。
///
/// # 返回
///
/// owner 必须停止当前会话时返回 true。
#[allow(clippy::too_many_arguments)]
fn process_logical_typed_batch(
    runtime: &Arc<KafkaProxyInner>,
    consumer: &ConsumerClient,
    plan: &mut GroupPlan,
    health: &Arc<RwLock<GroupHealth>>,
    commands: &std::sync::mpsc::Receiver<OwnerCommand>,
    attempts: &mut BTreeMap<(Tp, i64), u32>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    dlt_pending: &mut BTreeMap<Tp, PendingDlt>,
    dlt_sender: &tokio::sync::mpsc::Sender<DltCompletion>,
    dlt_admission: &mut Option<PendingDltAdmission>,
    progress: &mut GroupOffsetState,
    last_commit: &mut Instant,
    assignment_epoch: u64,
    batch: LogicalTypedBatch,
    partition_cursor: &mut usize,
    transient_exit: &mut Option<String>,
    gate_trigger: &mut Option<Tp>,
    deferred_stop: &mut Option<OwnerCommand>,
    decode_streaks: &mut BTreeMap<Tp, u64>,
) -> bool {
    let mut partitions: BTreeMap<Tp, Vec<TypedBatchItem>> = BTreeMap::new();
    for item in batch.items {
        partitions.entry(item.tp()).or_default().push(item);
    }
    for items in partitions.values_mut() {
        items.sort_by_key(TypedBatchItem::offset);
    }
    let mut order = partitions.keys().cloned().collect::<Vec<_>>();
    if !order.is_empty() {
        let start = *partition_cursor % order.len();
        order.rotate_left(start);
        *partition_cursor = (*partition_cursor + 1) % order.len();
    }
    for tp in order {
        let Some(items) = partitions.remove(&tp) else {
            continue;
        };
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
        let mut prepared = items
            .into_iter()
            .map(|item| match item {
                TypedBatchItem::Owned(message) => {
                    prepare_typed_message(runtime, plan, assignment_epoch, decode_streaks, message)
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
            })
            .collect::<VecDeque<_>>();
        while let Some(item) = prepared.pop_front() {
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
                PreparedTypedItem::ImmediateRaw { source, action } => {
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
                PreparedTypedItem::Routed {
                    source,
                    route,
                    decoded,
                } => {
                    let mut sources = vec![source];
                    let mut decoded_records = vec![decoded];
                    if route.meta().shape == ConsumerShape::Batch {
                        while matches!(prepared.front(), Some(PreparedTypedItem::Routed { route: next, .. }) if Arc::ptr_eq(&route, next))
                        {
                            let Some(PreparedTypedItem::Routed {
                                source, decoded, ..
                            }) = prepared.pop_front()
                            else {
                                unreachable!("前置形态检查已确认 routed item");
                            };
                            sources.push(source);
                            decoded_records.push(decoded);
                        }
                    }
                    let first = &sources[0].ctx;
                    let last_offset = sources
                        .last()
                        .map_or(first.offset, |source| source.ctx.offset);
                    let attempt = attempts
                        .get(&(tp.clone(), first.offset))
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(1);
                    let trace_id = consume_trace_id(first);
                    let consumer_span = first.trace_context().and_then(|parent| {
                        runtime.span_recorder.as_ref().map(|recorder| {
                            recorder.start(
                                "Kafka consume",
                                &parent,
                                natelemetry::SpanKind::Consumer,
                            )
                        })
                    });
                    let outcome = if route.meta().shape == ConsumerShape::Batch {
                        let span = tracing::info_span!(
                            "kafka.consume_batch",
                            group = %plan.group,
                            consumer_id = route.meta().id,
                            topic = %first.topic,
                            partition = first.partition,
                            first_offset = first.offset,
                            last_offset,
                            count = sources.len(),
                            event = %route.meta().event,
                            attempt,
                            trace_id
                        );
                        span.in_scope(|| invoke_typed_handler(runtime, &route, decoded_records))
                    } else {
                        let span = tracing::info_span!(
                            "kafka.consume",
                            group = %plan.group,
                            consumer_id = route.meta().id,
                            topic = %first.topic,
                            partition = first.partition,
                            offset = first.offset,
                            event = %route.meta().event,
                            attempt,
                            trace_id
                        );
                        span.in_scope(|| invoke_typed_handler(runtime, &route, decoded_records))
                    };
                    if let Some(span) = consumer_span {
                        let _ = span.finish(None);
                    }
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
                    apply_invocation_outcome(&mut context, &route, &sources, outcome)
                }
            };
            if dlt_admission.is_some() {
                record_unprocessed_for_dlt_gate(dlt_admission, &prepared, &partitions);
                return false;
            }
            match flow {
                ActionFlow::Continue => {}
                ActionFlow::StopPartition => break,
                ActionFlow::StopBatch { tp, offset } => {
                    // 触发记录本身没有进入 progress，必须与本批未派发记录一起回退；
                    // 取 min 保证不会被"下一条记录"的回退把它跳过去。
                    let mut rewind = unprocessed_offsets(&prepared, &partitions);
                    rewind
                        .entry(tp.clone())
                        .and_modify(|current| *current = (*current).min(offset))
                        .or_insert(offset);
                    let failed = consumer.rewind_offsets(&rewind);
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
                    let failed = consumer.rewind_offsets(&rewind);
                    halt_failed_rewinds(consumer, health, &failed);
                }
                return false;
            }
        }
    }
    false
}

/// 把当前逻辑批尚未派发的全部分区记录并入 DLT group 级门禁回退表。
///
/// - `admission`: 当前唯一 local pending DLT；存在时持有门禁回退表。
/// - `prepared`: 当前分区尚未派发的记录。
/// - `partitions`: 本批轮转顺序中尚未开始派发的其它分区。
fn record_unprocessed_for_dlt_gate(
    admission: &mut Option<PendingDltAdmission>,
    prepared: &VecDeque<PreparedTypedItem>,
    partitions: &BTreeMap<Tp, Vec<TypedBatchItem>>,
) {
    let Some(pending) = admission.as_mut() else {
        return;
    };
    for (tp, offset) in unprocessed_offsets(prepared, partitions) {
        pending
            .discarded
            .entry(tp)
            .and_modify(|current| *current = (*current).min(offset))
            .or_insert(offset);
    }
}

/// 汇总逻辑批中尚未派发的每分区最小 offset。
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

/// 路由并处理单条借用消息。
///
/// # 参数
///
/// - `runtime`: 所属运行时。
/// - `plan`: group 路由计划。
/// - `assignment_epoch`: 当前 owner epoch。
/// - `message`: poll 借用消息。
fn process_message(
    runtime: &Arc<KafkaProxyInner>,
    plan: &GroupPlan,
    assignment_epoch: u64,
    message: &RdMessage<'_>,
) -> ProcessAction {
    if message.retained_bytes() > runtime.config.behavior.max_record_bytes {
        return ProcessAction::Halt(
            HaltReason::OversizeRecord,
            format!("record bytes={} 超过上限", message.retained_bytes()),
        );
    }
    let headers = match message.headers() {
        Ok(headers) => headers,
        Err(_) => {
            return invalid_action(
                runtime,
                crate::consumer::InvalidRecordReason::MalformedRoute,
                "header 名不是 UTF-8",
            )
        }
    };
    let event = match last_header(&headers, HEADER_EVENT) {
        None | Some(None) => DEFAULT_EVENT,
        Some(Some(value)) => match std::str::from_utf8(value) {
            Ok("") => DEFAULT_EVENT,
            Ok(value) => value,
            Err(_) => {
                return invalid_action(
                    runtime,
                    crate::consumer::InvalidRecordReason::MalformedRoute,
                    "event header 不是 UTF-8",
                )
            }
        },
    };
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

/// 在 owner callback 栈内执行同步少拷贝 route。
///
/// # 参数
///
/// - `runtime`: 所属运行时及无效记录策略。
/// - `route`: 匹配 passthrough route。
/// - `assignment_epoch`: 当前 epoch。
/// - `message`: 借用消息。
/// - `headers`: 底层借用 headers。
fn process_passthrough(
    runtime: &Arc<KafkaProxyInner>,
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
    let outcome = span.in_scope(|| {
        std::panic::catch_unwind(AssertUnwindSafe(|| route.handler.consume_borrowed(record)))
    });
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

/// 把 passthrough 成功返回值与 route 确认模式合并成唯一 owner 动作。
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

/// 校验拥有型 typed 消息的 codec headers 与 route 声明一致。
///
/// # 参数
///
/// - `codec`: route 编译期固定 codec。
/// - `headers`: 已物化但仍保持有序多值语义的 headers。
///
/// # 错误
///
/// codec 或 payload mode 缺失、非法、不匹配时返回错误。
fn validate_owned_codec(codec: PayloadCodec, headers: &KafkaHeaders) -> Result<()> {
    let actual_codec = headers
        .last(HEADER_PAYLOAD_CODEC)
        .and_then(|header| header.value.as_deref());
    match codec {
        PayloadCodec::Json if actual_codec.is_none() => Ok(()),
        PayloadCodec::Json => Err(NafkaError::Codec(
            "JSON route 收到带二进制 codec header 的记录".into(),
        )),
        PayloadCodec::Proto(mode) => {
            if actual_codec != Some(PAYLOAD_CODEC_PROTOCOL_BYTES.as_bytes()) {
                return Err(NafkaError::Codec(
                    "ProtocolBytes codec header 不匹配".into(),
                ));
            }
            let actual_mode = headers
                .last(HEADER_PAYLOAD_MODE)
                .and_then(|header| header.value.as_deref());
            if let Some(actual_mode) = actual_mode {
                let actual_mode = std::str::from_utf8(actual_mode)
                    .ok()
                    .and_then(crate::wire::mode_from_wire_name);
                if actual_mode != Some(mode) {
                    return Err(NafkaError::Codec("ProtocolBytes mode header 不匹配".into()));
                }
            }
            Ok(())
        }
    }
}

/// 按配置把确定无效记录映射为 DLT/重试或 Halt。
///
/// # 参数
///
/// - `runtime`: 所属运行时。
/// - `message`: 原消息。
/// - `reason`: 已脱敏原因。
fn invalid_or_dlt(runtime: &Arc<KafkaProxyInner>, reason: String) -> ProcessAction {
    match runtime.config.behavior.invalid_record_policy {
        crate::config::InvalidRecordPolicy::Halt => ProcessAction::Halt(
            HaltReason::InvalidRecord(crate::consumer::InvalidRecordReason::MalformedRoute),
            reason,
        ),
        crate::config::InvalidRecordPolicy::DeadLetter => ProcessAction::DeadLetter(reason),
    }
}

/// 解码失败的处置：先过**本分区**连续失败熔断，再落到 `invalid_record_policy`。
///
/// 单条畸形 payload 走 DeadLetter 是对的；但"解码器整体坏了"在这条路径上长得一模一样，
/// 而后者会让整个 topic 满速排进 DLT、offset 一路前进、group 仍报 Running。
/// 连续失败达到 `decode_failure_halt_streak` 即 Halt 分区，交由人工判定。
///
/// 计数**必须按分区隔离**：进程级单计数会被同进程其它 group/route 的正常解码成功清零
/// （socket-center 的 data + control 双 group 共用一个 KafkaProxy 就是典型形态），
/// 使这道守卫在最该生效的多路场景里被架空。这里以 owner 会话局部的 `streaks` 表按 `tp`
/// 累计，与 Halt 的分区粒度一致；owner 单线程独占该表，无需原子/锁。
///
/// # 参数
///
/// - `runtime`: 所属运行时（提供阈值配置）。
/// - `streaks`: owner 会话局部的按分区连续解码失败表。
/// - `tp`: 触发失败的分区。
/// - `reason`: 已脱敏原因。
fn decode_failure_action(
    runtime: &Arc<KafkaProxyInner>,
    streaks: &mut BTreeMap<Tp, u64>,
    tp: &Tp,
    reason: String,
) -> ProcessAction {
    let streak = record_decode_failure(streaks, tp);
    if let Some(limit) = runtime.config.behavior.decode_failure_halt_streak {
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

/// 记录本分区一次解码失败并返回累计的连续失败次数。
///
/// 纯函数（不触碰 runtime），便于单测覆盖"分区之间互不影响"这条关键隔离语义。
///
/// - `streaks`: owner 会话局部的按分区连续解码失败表。
/// - `tp`: 触发失败的分区。
fn record_decode_failure(streaks: &mut BTreeMap<Tp, u64>, tp: &Tp) -> u64 {
    let entry = streaks.entry(tp.clone()).or_insert(0);
    *entry = entry.saturating_add(1);
    *entry
}

/// 记录本分区一次解码成功，清零该分区的连续失败计数。
///
/// 工作正常的分区解码成功后立即从表中移除，因此该表只保留"正在连续失败"的分区，
/// 规模受当前 assignment 约束（远小于分区总数）。
///
/// - `streaks`: owner 会话局部的按分区连续解码失败表。
/// - `tp`: 解码成功的分区。
fn note_decode_success(streaks: &mut BTreeMap<Tp, u64>, tp: &Tp) {
    streaks.remove(tp);
}

/// 构造不依赖消息所有权的无效路由动作。
///
/// # 参数
///
/// - `runtime`: 所属运行时。
/// - `reason`: 已脱敏原因。
fn invalid_action(
    runtime: &Arc<KafkaProxyInner>,
    invalid_reason: crate::consumer::InvalidRecordReason,
    reason: &str,
) -> ProcessAction {
    match runtime.config.behavior.invalid_record_policy {
        crate::config::InvalidRecordPolicy::Halt => {
            ProcessAction::Halt(HaltReason::InvalidRecord(invalid_reason), reason.into())
        }
        crate::config::InvalidRecordPolicy::DeadLetter => ProcessAction::DeadLetter(reason.into()),
    }
}

/// 判断一个 header 名是否属于框架写入的 DLT 来源标记。
///
/// 转发前必须剥掉来源记录上的同名 header：它们可能由外部生产者伪造，
/// 与框架追加的真实值共存会让 DLT 侧去重读到歧义值。
///
/// # 参数
///
/// - `name`: 待判定的 header 名。
fn is_dlt_origin_header(name: &str) -> bool {
    name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_TOPIC)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_PARTITION)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_OFFSET)
}

/// 从原始消息构造保留来源身份的单条死信记录。
///
/// # 参数
///
/// - `runtime`: 提供冻结的 DLT 后缀。
/// - `group`: 来源 group。
/// - `message`: 原始消息视图。
/// - `reason`: 已脱敏失败原因。
///
/// # 错误
///
/// 原记录已经来自 DLT 时返回递归错误。
fn build_dlt_record<S: DeadLetterSource + ?Sized>(
    runtime: &Arc<KafkaProxyInner>,
    group: &str,
    message: &S,
    reason: &str,
) -> Result<DltRecord> {
    let original = message.source_headers();
    let suffix = runtime.config.behavior.dead_letter_topic_suffix.as_str();
    // 递归判定只能用**不可伪造**的信号：记录是否真的来自死信 topic。
    //
    // 规范 原文写的是"原记录已带 X-Nasa-DLT-Origin-Topic 时"判定为递归，但该 header
    // 由生产者任意填写：任何有写权限的客户端只要发一条「合法 event + 伪造 origin header +
    // 解不出来的 payload」，就能让该分区进入 Halt(RecursiveDlt) 并停止消费，直到人工介入
    // ——等于把"一条消息瘫痪一个分区"的能力交给了外部（已实测复现）。它同时堵死了
    // "把 DLT 记录重放回业务 topic"这一正常运维流程：重放记录天然带着 origin header，
    // 再次失败时本应重新进 DLT，却会把分区打成 Halt。
    //
    // 来源 topic 名由框架自己拼（`<topic><suffix>`），生产者无法伪造，且完全满足
    // 的意图——避免 `topic.DLT.DLT` 链。因此以它为唯一判据。
    let already_dead_lettered = !suffix.is_empty() && message.source_topic().ends_with(suffix);
    if already_dead_lettered {
        return Err(NafkaError::RecursiveDlt);
    }
    // 剥掉来源侧可能携带的同名 origin header，避免伪造值与框架写入值共存导致
    // DLT 消费者按 的 "origin topic + partition + offset 去重" 时读到歧义值。
    let mut headers = original
        .iter()
        .filter(|header| !is_dlt_origin_header(&header.name))
        .cloned()
        .collect::<Vec<_>>();
    headers.extend([
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_TOPIC.into(),
            value: Some(message.source_topic().as_bytes().to_vec()),
        },
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_PARTITION.into(),
            value: Some(message.source_partition().to_string().into_bytes()),
        },
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_OFFSET.into(),
            value: Some(message.source_offset().to_string().into_bytes()),
        },
        KafkaHeader {
            name: HEADER_DLT_REASON.into(),
            value: Some(reason.chars().take(1024).collect::<String>().into_bytes()),
        },
        KafkaHeader {
            name: "X-Nasa-Dlt-Origin-Group".into(),
            value: Some(group.as_bytes().to_vec()),
        },
    ]);
    Ok(DltRecord {
        topic: format!("{}{suffix}", message.source_topic()),
        payload: message.source_payload().map(<[u8]>::to_vec),
        key: message.source_key().map(<[u8]>::to_vec),
        headers: KafkaHeaders::from_vec(headers),
        timestamp: (message.source_timestamp() >= 0).then(|| message.source_timestamp()),
    })
}

/// 用显式数值计算 DLT 指数退避，保持算法可做无运行时依赖的边界验证。
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

/// 安排失败分区的非阻塞退避，并把错误映射成 owner 控制流。
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

/// 暂停单个失败分区、回到失败 offset，并登记单调时钟恢复时刻。
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
        },
    );
    update_state(health, GroupState::Retrying, None);
    Ok(())
}

/// 恢复所有退避到期分区；每个分区恢复前再次 seek，消除底层预取位置漂移。
///
/// # 参数
///
/// - `consumer`: owner 唯一 consumer。
/// - `health`: group 健康快照。
/// - `retries`: owner 的每分区退避表。
/// - `retry_delay_ms`: seek 临时失败后的再次尝试间隔。
fn activate_due_retries(
    consumer: &ConsumerClient,
    health: &Arc<RwLock<GroupHealth>>,
    retries: &mut BTreeMap<Tp, PartitionRetry>,
    retry_delay_ms: u64,
) {
    let now = Instant::now();
    let due = retries
        .iter()
        .filter(|(_, state)| state.ready_at <= now)
        .map(|(tp, state)| (tp.clone(), state.retry_from))
        .collect::<Vec<_>>();
    for (tp, offset) in due {
        match consumer.seek_offsets(&BTreeMap::from([(tp.clone(), offset)])) {
            Ok(()) => {
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

/// 移除一组目标上的指定框架暂停原因，并返回已经没有任何暂停原因的分区。
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

/// 暂停单个分区并写入 Halt 原因。
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

/// 增加或移除一组分区的 User 暂停原因。
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

/// 读取指定名字的最后一个底层 header。
///
/// # 参数
///
/// - `headers`: 底层 header 描述符。
/// - `name`: 大小写敏感名称。
fn last_header<'a>(
    headers: &'a [crate::rd::consumer::RdHeaderRef<'a>],
    name: &str,
) -> Option<Option<&'a [u8]>> {
    headers
        .iter()
        .rev()
        .find(|header| header.name == name)
        .map(|header| header.value)
}

/// 复制底层 headers 为公共有序拥有型集合。
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
