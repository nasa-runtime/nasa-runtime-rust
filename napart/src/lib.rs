//! 带保序任务窃取的分 lane 串行/并行执行器。
//!
//! # 核心价值：保序任务窃取
//!
//! 顺序边界是「原始分区 + 任务类型」：同一 key 的严格类型任务按提交顺序串行落地；不同 key
//! 若落入同一原始分区且类型相同，也共享该严格 lane。不同类型不共享严格顺序门禁，非严格 lane
//! 可由多个空闲分区并发消费。空闲分区通过一次执行权 CAS 接管其它分区的严格 lane，不移动
//! 队列数据，FIFO 由队列自身保证。
//!
//! 窃取只改变严格 lane 的执行权：队列留在原 lane，提交仍写入原始分区。worker 取得执行门禁后
//! 会复验执行权，再把 pop 与业务执行整体置于门禁内，因此原持有方与接管方不能并发执行同一
//! lane。非严格 lane 无独占执行权，可由空闲 worker 直接代服务。被接管 lane 排空、接管方的
//! 原籍 lane 出现积压或持有达到上界时，执行权会归还原分区。
//!
//! 任务窃取不会并行化单条严格 lane。兼容 `submit*` 入口在每个原始分区只有一条保留严格 lane；
//! 要让同分区的独立业务类型绕过队首阻塞，应使用不同的稳定 [`TaskType`] 拆分 lane。
//!
//! # 运行架构
//!
//! 1. key 经进程内 hash 映射到原始分区，`(原始分区, TaskType)` 唯一定位 lane；分区号不作为
//!    跨进程或跨程序版本的持久化标识。
//! 2. 每个分区有一个常驻 worker，公平轮转本分区 lane 与已接管的外分区 lane。
//! 3. 空闲 worker 在 lane 注册表的无锁快照上寻找积压；严格 lane 通过 `owner` CAS 接管，
//!    非严格 lane 直接加入临时代服务集合。
//! 4. 接管数量、持有周期与重扫周期均有界；持续有进展时也会重新裁决，避免严格热点长期饥饿。
//! 5. worker 异常退出时冻结其负责的严格 lane 并保留证据，不在执行权不明时盲目推进。
//!
//! 兼容入口 [`PartitionExecutor::submit`] / [`PartitionExecutor::submit_sync`] /
//! [`PartitionExecutor::submit_async`] 保持既有语义：同 key 严格 FIFO、有界背压、被拒可见。
//! 类型化入口 [`PartitionExecutor::submit_typed`] 额外提供任务句柄（状态查询 / 取消 / 等待
//! 终态）、延迟提交与跨分区执行权流动。
//!
//! 准入分两层：每 lane 深度上限约束排队量（worker 取走任务即腾出），全局在飞预算约束
//! 「排队 + 执行中」总量（任务到达终态即归还，含取消）。lane 总数受配置上限约束
//! （[`PartitionExecutor::with_max_lanes`]）。任一层满载都以明确错误拒绝，不静默丢弃。
//!
//! 停机在独立受监督任务中驱动：调用方 future 被取消不影响停机推进；返回时 worker、在途
//! 任务与延迟定时器均凭 join 证明退出，有损项（冻结/强制中止）全部计入报告与证据。
//!
//! 必须在 Tokio 运行时内构造（内部 spawn 常驻 worker）；`shutdown` 为 async。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod entry;
mod lane;
mod worker;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{
    fence, AtomicBool, AtomicI64, AtomicU64, AtomicU8, AtomicUsize,
    Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;

pub use entry::{Submission, TaskStatus};
pub use lane::{TaskOrdering, TaskSpec, TaskType};

use entry::{
    reason_code, reason_text, DepthReservation, EntryPayload, EntryShared, Job, STATE_DELAYED,
    STATE_QUEUED, STATE_REJECTED,
};
use lane::{FrozenEntry, Lane, LaneCreateError, LaneRegistry, LEGACY_TYPE};

// ── 生命周期 ────────────────────────────────────────────────────────────────

/// 生命周期：开放提交。
pub(crate) const PHASE_ACCEPTING: u8 = 0;
/// 生命周期：停机中（拒新、排空旧）。
pub(crate) const PHASE_STOPPING: u8 = 1;
/// 生命周期：已停止。
pub(crate) const PHASE_STOPPED: u8 = 2;

/// 每 lane 默认深度上限。满则由提交入口返回满载错误形成可见背压，而非无界积压。
const DEFAULT_QUEUE_CAPACITY: usize = 65_536;
/// lane 总数默认上限。`TaskType` 是开放的 u32，注册表与按类型导出的指标序列只增不减，
/// 必须有配置上界，否则动态类型可绕过全局内存预算造成单调增长。
const DEFAULT_MAX_LANES: usize = 4_096;
/// 停机 worker 收口默认上界；超时强制中止在途任务并把未排空载荷冻结为证据。
const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(2);
/// 停机收口上界的最大可表示值（365 天）。`Instant + Duration` 对极大时长会溢出并在唯一
/// 停机驱动内展开，驱动死亡后生命周期永久停在 Stopping；超出的合法输入一律钳制到本值。
const MAX_STOP_TIMEOUT: Duration = Duration::from_secs(31_536_000);
/// 停机等待 producer 收口的告警间隔：提交临界区无 await、纳秒级完成，等待超过此间隔说明
/// 存在进程级停摆（OS 抢占、分配器停顿），周期性告警但继续等待——producer 在场时发布
/// STOPPED 会让终局报告被迟到提交改写。
const PRODUCER_DRAIN_WARN: Duration = Duration::from_secs(2);
/// 延迟定时的单段等待上界（365 天）。底层定时器对不可表示的截止点（`Instant + Duration`
/// 溢出）不报错也不保留原时长，而是退化为约 30 年后的近似值；延迟按不超过本值的分段
/// 消耗，任何合法 `delay`（含 `Duration::MAX`）都绝不早于请求时长到期。
const DELAY_SLEEP_SEGMENT: Duration = Duration::from_secs(31_536_000);

// ── 错误 ────────────────────────────────────────────────────────────────────

/// 兼容提交入口被拒的原因（对调用方可见可监控，不静默丢任务）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// 执行器已 shutdown / 正在停机，拒收新任务。
    ShuttingDown,
    /// 目标分区已失去安全执行条件（worker 异常死亡等），该方向永久拒收。
    PartitionDead,
    /// 目标队列已满（有界背压）。非阻塞提交专有；可退避重试或改用 `submit_async` 等容量。
    QueueFull,
}

impl std::fmt::Display for SubmitError {
    /// 业务作用：把拒绝原因格式化为稳定文本，供调用方记录与分类处理。
    ///
    /// 参数说明：
    /// - `f`: 标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubmitError::ShuttingDown => write!(f, "partition executor is shutting down"),
            SubmitError::PartitionDead => write!(f, "target partition worker is dead"),
            SubmitError::QueueFull => write!(f, "target partition queue is full"),
        }
    }
}

impl std::error::Error for SubmitError {}

/// 类型化提交入口被拒的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubmitRejection {
    /// 执行器正在停机或已停止。
    ShuttingDown,
    /// 目标 lane 深度已满（排队上限）。
    QueueFull,
    /// 全局在飞预算耗尽（排队 + 执行中总量上限）。
    Overloaded,
    /// 同一「分区 + 任务类型」已以不同顺序要求存在，严格承诺不允许被混用稀释。
    OrderingConflict,
    /// 目标 lane 已冻结（结构性异常后拒收，任务不进黑洞）。
    LaneFailed,
    /// 类型化 lane 总数已达配置上限（`with_max_lanes`）；类型基数受控，动态类型不能无界
    /// 扩张注册表与指标序列。兼容入口的保留 lane 豁免，不会收到此拒绝。
    LaneLimitExceeded,
    /// 任务类型使用了保留值 `TaskType(u32::MAX)`。该值是兼容入口保留 lane 的内部身份，
    /// 若放行，typed 提交可改写保留 lane 的顺序要求或借用其上限豁免，让兼容 `submit`
    /// 在对应分区永久失效——入口身份不能同时充当业务输入。
    ReservedTaskType,
}

impl std::fmt::Display for SubmitRejection {
    /// 业务作用：把类型化拒绝原因格式化为稳定文本，供调用方记录与分类处理。
    ///
    /// 参数说明：
    /// - `f`: 标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            SubmitRejection::ShuttingDown => "executor is shutting down",
            SubmitRejection::QueueFull => "lane queue is full",
            SubmitRejection::Overloaded => "global in-flight budget exhausted",
            SubmitRejection::OrderingConflict => "task type ordering conflict",
            SubmitRejection::LaneFailed => "lane is frozen",
            SubmitRejection::LaneLimitExceeded => "lane count limit reached",
            SubmitRejection::ReservedTaskType => "task type value is reserved",
        };
        f.write_str(text)
    }
}

impl std::error::Error for SubmitRejection {}

/// 业务作用：把类型化拒绝映射为兼容入口的三值错误，保持既有调用方的错误处理不变。
///
/// 参数说明：
/// - `rejection`: 类型化拒绝原因。
///
/// 返回：兼容错误。`Overloaded` 归入 `QueueFull`（同为容量背压，可退避重试）；
/// `OrderingConflict`/`LaneFailed`/`LaneLimitExceeded`/`ReservedTaskType` 归入
/// `PartitionDead`（同为该方向的持久性拒收；兼容入口不携带任务类型，实际不会产生
/// `ReservedTaskType`）。
fn to_legacy_error(rejection: SubmitRejection) -> SubmitError {
    match rejection {
        SubmitRejection::ShuttingDown => SubmitError::ShuttingDown,
        SubmitRejection::QueueFull | SubmitRejection::Overloaded => SubmitError::QueueFull,
        SubmitRejection::OrderingConflict
        | SubmitRejection::LaneFailed
        | SubmitRejection::LaneLimitExceeded
        | SubmitRejection::ReservedTaskType => SubmitError::PartitionDead,
    }
}

// ── 内核 ────────────────────────────────────────────────────────────────────

/// 延迟登记：停机时主动拒绝未到期任务、并等待定时任务退出证明所需的句柄。
struct DelayedReg {
    shared: Arc<EntryShared>,
    /// 定时任务的 JoinHandle：停机路径 abort 后必须 await 它，"定时器已退出"才有证明，
    /// 否则停机返回后仍可能有本执行器的定时任务在后台存活。
    handle: JoinHandle<()>,
}

/// 垂死收容所首次触发清剪的长度水位。
const DYING_PRUNE_FLOOR: usize = 64;

/// 垂死定时任务收容所：已离开登记表、可能尚未析构的定时任务句柄。
struct DyingTimers {
    handles: Vec<JoinHandle<()>>,
    /// 清剪水位：长度达到水位才全表剪一次已结束句柄，随后按存活量翻倍重置。摊销后每次
    /// 插入 O(1)——同步批量取消（单线程 runtime 内被 abort 的定时任务在取消循环让出前
    /// 一个也不会结束）不会形成逐次全表扫描的平方退化；均匀负载下清剪把列表压回 O(1)，
    /// 长驻进程不累积。
    prune_at: usize,
}

/// 执行器共享内核：worker、提交入口与停机流程共同持有。
pub(crate) struct Inner {
    /// 生命周期阶段。
    ///
    /// 内存序约定:阶段写与 producer 计数构成 Dekker 型判定——提交方"先登记 producer、
    /// 再验阶段",停机方"先切阶段、再读 producer"。两侧都必须 SeqCst:仅用 AcqRel/Acquire
    /// 时,两个不同地址上的读可以各自看到对方写之前的旧值(store-buffer 交错),出现
    /// "停机认为无 producer、producer 认为仍在接收"的双盲,迟到 push 落进无人服务的队列。
    phase: AtomicU8,
    /// producer 已收口信号：worker 只有在此为 true 后才允许按"已排空"退出，
    /// 否则迟到 push 会落进无人服务的队列。
    pub(crate) drain_ok: AtomicBool,
    /// 在途 producer 计数（提交路径存续期间 +1）；与 `phase` 构成 Dekker 判定，见其注释。
    producers: AtomicI64,
    /// 停机强制中止信号：worker 见到后停止取新载荷，并中止正在等待的业务任务。
    force_abort: AtomicBool,
    /// 强制中止的广播唤醒点（`notify_waiters` 语义，配合 force_abort 复查免丢唤醒）。
    abort_wake: Notify,
    /// 终局停机报告：驱动任务在切到 STOPPED 前写入，所有 shutdown 调用方读取同一份。
    report: OnceLock<ShutdownReport>,
    /// 全局在飞预算。
    global: Arc<Semaphore>,
    /// lane 注册表。
    pub(crate) registry: LaneRegistry,
    /// lane 总数上限（构造后可经 builder 调整；只约束新建）。
    max_lanes: AtomicUsize,
    /// 每分区 worker 的唤醒点。
    notifies: Box<[Notify]>,
    /// 分区路由掩码。
    mask: usize,
    /// 每 lane 深度上限。
    lane_cap: u32,
    /// 延迟登记表。
    delayed: Mutex<HashMap<u64, DelayedReg>>,
    /// 已离开登记表但可能尚未析构的定时任务（被取消 abort 的、自然到期正在收尾的）。
    /// 停机必须逐个 join 这里的句柄才算拿到"无本执行器定时任务在后台"的证明——abort 只是
    /// 取消请求，Drop JoinHandle 是 detach，都不构成退出证明。清剪按水位摊销执行（见
    /// `DyingTimers`），长驻进程不累积。锁序固定为 delayed -> dying，与停机 drain 一致。
    dying: Mutex<DyingTimers>,
    /// 延迟登记序号。
    delayed_seq: AtomicU64,
    /// 冻结证据容器（含未执行冻结与执行中被停机中止两类残留）。
    frozen: Mutex<Vec<FrozenEntry>>,

    // ── 计数（低基数、只增） ──
    submitted: AtomicU64,
    pub(crate) completed: AtomicU64,
    /// 成功取消总数（终态 Cancelled 的真实次数，含到期前取消的延迟任务）。
    pub(crate) cancelled_total: AtomicU64,
    /// 已取消载荷被物理丢弃数（pop 或清扫时点），用于停机报告的清理口径。
    pub(crate) cancelled_discarded: AtomicU64,
    pub(crate) task_panics: AtomicU64,
    /// 停机强制中止的执行中任务数（有损：任务可能已部分执行）。
    aborted_running: AtomicU64,
    rej_shutdown: AtomicU64,
    rej_queue_full: AtomicU64,
    rej_overloaded: AtomicU64,
    rej_ordering: AtomicU64,
    rej_lane_failed: AtomicU64,
    rej_lane_limit: AtomicU64,
    rej_reserved: AtomicU64,
    pub(crate) steal_attempts: AtomicU64,
    pub(crate) steal_success: AtomicU64,
    pub(crate) releases: AtomicU64,
    frozen_count: AtomicU64,
    failed_lanes: AtomicU64,
    pub(crate) dead_workers: AtomicI64,
    // ── 停机期归账(报告口径) ──
    // 报告不用"终值 - 停机起点基线"的差值:基线采样发生在停机驱动首次被调度时,与 phase
    // 切换之间存在不受控的调度/分配延迟,切相后、采样前完成的处置会被基线吞掉。归账采用
    // 事件 guard 协议(OutcomeSection):每笔完成/丢弃/冻结先登记事件、再读 SeqCst phase
    // 裁决归属,发布与记账都在同一事件段内完成;停机驱动在推进前等待切相时已登记的事件
    // 收口。于是裁决为"非停机"的事件必然整体先于停机推进,切相前已公开的终态不可能被
    // 后置的 phase 读取误归入停机期,反向也不漏计。
    /// 已登记的归账事件总数（只增）。
    outcome_started: AtomicU64,
    /// 已收口的归账事件总数（只增）；started 与 finished 之差为在途事件。
    outcome_finished: AtomicU64,
    /// 停机开始后正常执行完成的任务数（报告 drained）。
    drained_stop: AtomicU64,
    /// 停机开始后物理丢弃的已取消载荷数（报告 cancelled）。
    discarded_stop: AtomicU64,
    /// 停机开始后被冻结为未执行证据的任务数（报告 frozen）。
    frozen_stop: AtomicU64,
}

/// 业务作用：在完整的不可信边界内执行可能运行外部代码的动作，并保证本函数自身不展开。
/// 边界覆盖三层外部代码：动作本身（外部 Waker）、panic payload 的析构（`panic_any` 可携带
/// 析构再度 panic 的载荷）、以及异常路径的错误日志（`tracing` Subscriber 是集成方提供的
/// 同步回调，安全实现同样可以 panic——若不隔离，日志展开会跳过调用方剩余的结算步骤或
/// 沿 worker/停机驱动调用栈传播）。记录失败后不再经同一日志链重试；二次捕获所得载荷不再
/// 触发析构（泄漏是唯一不继续展开的出路，且仅发生在故意的对抗载荷上）。
///
/// 参数说明：
/// - `context`: 记入错误日志的动作说明。
/// - `action`: 可能同步调用外部代码的动作。
///
/// 返回：无返回值；任何展开都被吸收，本函数从不向调用方展开。
pub(crate) fn run_isolated(context: &str, action: impl FnOnce()) {
    let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) else {
        return;
    };
    // 先在独立边界内消费原始 payload,再记录:日志回调也可能展开,不能让它把载荷处置
    // 一并跳过。
    swallow_panic_payload(payload);
    log_isolated(|| {
        tracing::error!("napart {context} 阶段被外部代码 panic 中断");
    });
}

/// 业务作用：产品诊断日志的唯一入口。`tracing` Subscriber 是集成方提供的同步回调，安全
/// 实现即可 panic；任何状态迁移、计数、唤醒或收口路径都不得因诊断展开而中断——普通
/// 服务路径会被放大成 worker 死亡与 lane 冻结，停机驱动会永久丢失，Drop 清理路径会升级
/// 为进程中止。日志展开被吸收后不重试、不经同一日志链再记录。
///
/// 参数说明：
/// - `record`: 日志记录动作。
///
/// 返回：无返回值；本函数从不向调用方展开。
pub(crate) fn log_isolated(record: impl FnOnce()) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(record)) {
        swallow_panic_payload(payload);
    }
}

/// 业务作用：在独立 panic 边界内析构一个不可信 panic payload；析构自身再度展开时放弃析构，
/// 不向调用方传播。
///
/// 参数说明：
/// - `payload`: 捕获到的 panic 载荷。
///
/// 返回：无返回值。
fn swallow_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    if let Err(second) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        drop(payload);
    })) {
        std::mem::forget(second);
    }
}

/// 防护罩 waker：把调用方真实 Waker 的**完整生命周期**包在不展开边界内。
///
/// 共享唤醒设施（lane 容量 Notify、全局预算信号量、终态通知）以批量循环调用等待者的
/// Waker；Waker 是调用方提供的外部安全代码，单个 panic 会中断批量循环，让同批其它正常
/// 等待者丢失调度、或沿停机驱动/信号量关闭路径展开。框架把注册进共享设施的 waker 全部
/// 换成本防护罩，异常在等待者自己的 waker 层被吸收，批量唤醒继续推进。
///
/// 不展开保证必须覆盖两个面：唤醒调用，以及真实 Waker 的**析构**——`Waker::from(Arc<W>)`
/// 的最后一次释放会运行 `W` 的 Drop，同样是外部安全代码，可在消费式唤醒、等待 future
/// 取消、内部 Waker 替换与共享设施清理等任何路径上展开。析构隔离由本类型的 Drop 完成，
/// 因此无论防护罩在哪里离场，内层 Waker 的销毁都不越过边界。
struct ShieldWaker(Option<std::task::Waker>);

impl std::task::Wake for ShieldWaker {
    /// 业务作用：消费式唤醒委托引用转发；`Arc` 随后在本调用栈上释放，内层真实 Waker 的
    /// 析构由 Drop 的隔离边界承接，不向共享设施的批量循环展开。
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// 业务作用：按引用转发唤醒并吸收外部 Waker 调用的展开。
    fn wake_by_ref(self: &Arc<Self>) {
        run_isolated("等待者唤醒转发", || {
            if let Some(waker) = &self.0 {
                waker.wake_by_ref();
            }
        });
    }
}

impl Drop for ShieldWaker {
    /// 业务作用：在不展开边界内消费内层真实 Waker——它的 Drop 是外部安全代码，析构展开
    /// 若外泄会在批量唤醒中截断同批正常等待者，或沿停机驱动/设施清理路径传播。
    fn drop(&mut self) {
        if let Some(waker) = self.0.take() {
            run_isolated("等待者真实 Waker 析构", move || drop(waker));
        }
    }
}

/// 经防护罩注册等待的 future 适配器；内部 future 固定装箱以避免自引用投影。
struct ShieldedWait<F: Future> {
    inner: std::pin::Pin<Box<F>>,
}

impl<F: Future> Future for ShieldedWait<F> {
    type Output = F::Output;

    /// 业务作用：以防护罩 waker 轮询内部 future，使其注册进共享唤醒设施的是不展开的
    /// 转发 waker，而不是调用方的裸 Waker。
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let shielded = std::task::Waker::from(Arc::new(ShieldWaker(Some(cx.waker().clone()))));
        let mut shielded_cx = std::task::Context::from_waker(&shielded);
        self.inner.as_mut().poll(&mut shielded_cx)
    }
}

/// 业务作用：把一次共享设施等待包进防护罩——批量唤醒中任一外部 Waker 的 panic 都不会
/// 中断批次或沿框架调用栈展开。
///
/// 参数说明：
/// - `fut`: 将在共享唤醒设施（Notify / 信号量）上注册等待的 future。
///
/// 返回：输出与原 future 一致的防护 future。
pub(crate) fn await_shielded<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    ShieldedWait {
        inner: Box::pin(fut),
    }
}

/// 终态归账事件段：构造即登记事件并裁决停机归属，Drop 即收口。
///
/// 归属由构造时刻的一次 SeqCst phase 读取唯一裁决，事件内的终态发布与计数都使用该裁决；
/// 段内不得跨越 `.await`（停机驱动会等待在途事件收口，跨 await 会把等待放大成业务时长）。
pub(crate) struct OutcomeSection<'a> {
    inner: &'a Inner,
    /// 本事件是否归属停机期。
    pub(crate) stopping: bool,
}

impl Drop for OutcomeSection<'_> {
    /// 业务作用：收口归账事件；线程处于 panic 展开时仍完成计数，使停机驱动不因异常路径永久
    /// 等待。收口计数无条件先行——本 Drop 可能已处于展开路径，诊断若直接调用可展开的日志
    /// 回调，会在析构清理期形成二次展开并中止整个进程；诊断因此放在安全不变量之后、且只经
    /// 不展开入口执行。`thread::panicking` 无法区分段内新异常与进入本段前已经存在的外层展开，
    /// 该日志只表示风险信号，不单独证明框架内部异常。
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        self.inner.outcome_finished.fetch_add(1, SeqCst);
        if unwinding {
            log_isolated(|| {
                tracing::error!("napart 归账事件段被 panic 展开,对应账本提交可能不完整");
            });
        }
    }
}

impl Inner {
    /// 业务作用：读取当前生命周期阶段，提交入口与 worker 据此裁决拒收或排空退出。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前阶段字节。SeqCst 与 producer 计数构成 Dekker 判定（见字段注释），
    /// 弱化为 Acquire 会在弱内存序目标上打开"双盲"窗口。
    pub(crate) fn phase(&self) -> u8 {
        self.phase.load(SeqCst)
    }

    /// 业务作用：读取停机强制中止信号，worker 据此停止取新载荷并中止在途等待。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已进入强制中止阶段返回 true。
    pub(crate) fn force_aborting(&self) -> bool {
        self.force_abort.load(Acquire)
    }

    /// 业务作用：挂起等待停机强制中止信号；worker 在等待业务任务完成时以 select 监听它，
    /// 使停机方不必 abort worker 本身就能触发在途任务的受控中止。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：信号已置位时返回；先登记等待再复查，信号置位与唤醒之间不存在丢失窗口
    /// （`Notified` 创建后即可接收 `notify_waiters`）。
    pub(crate) async fn force_abort_signal(&self) {
        loop {
            if self.force_abort.load(Acquire) {
                return;
            }
            let notified = self.abort_wake.notified();
            if self.force_abort.load(Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// 业务作用：开启一段终态归账事件——先登记事件、再以一次 SeqCst phase 读取裁决停机
    /// 归属。与停机驱动的"等待已登记事件收口"构成 Dekker 配对：裁决为"非停机"的事件在
    /// 停机推进前必然完成发布与记账，终态公开时序与报告归属不会互相矛盾。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：携带归属裁决的事件段 guard；Drop 即收口。
    pub(crate) fn outcome_section(&self) -> OutcomeSection<'_> {
        // 先登记、后裁决:若裁决读到 ACCEPTING,则该读取先于切相,登记更先于切相,
        // 停机驱动的收口等待必然覆盖本事件。
        self.outcome_started.fetch_add(1, SeqCst);
        let stopping = self.phase() != PHASE_ACCEPTING;
        OutcomeSection {
            inner: self,
            stopping,
        }
    }

    /// 业务作用：登记一笔任务正常执行完成；归属由所在归账事件段裁决，停机期完成计入
    /// 报告 drained。
    ///
    /// 参数说明：
    /// - `stopping`: 所在事件段的停机归属裁决。
    ///
    /// 返回：无返回值。
    pub(crate) fn count_completed(&self, stopping: bool) {
        self.completed.fetch_add(1, Relaxed);
        if stopping {
            self.drained_stop.fetch_add(1, Relaxed);
        }
    }

    /// 业务作用：登记已取消载荷被物理丢弃；归属由所在归账事件段裁决，停机期丢弃计入
    /// 报告 cancelled。
    ///
    /// 参数说明：
    /// - `n`: 本次物理丢弃的载荷数。
    /// - `stopping`: 所在事件段的停机归属裁决。
    ///
    /// 返回：无返回值。
    pub(crate) fn count_discarded(&self, n: u64, stopping: bool) {
        if n == 0 {
            return;
        }
        self.cancelled_discarded.fetch_add(n, Relaxed);
        if stopping {
            self.discarded_stop.fetch_add(n, Relaxed);
        }
    }

    /// 业务作用：登记一笔被停机强制中止的执行中任务——计入有损计数并留进证据容器，
    /// 使报告与审计不会把"中止了一笔在途任务"误报成完全优雅停机。
    ///
    /// 参数说明：
    /// - `lane`: 任务所在 lane（提供类型与分区归属）。
    /// - `shared`: 任务共享状态（终态 Failed/aborted_during_shutdown 已发布）。
    ///
    /// 返回：无返回值。
    pub(crate) fn record_aborted(&self, lane: &Arc<Lane>, shared: Arc<EntryShared>) {
        self.aborted_running.fetch_add(1, Relaxed);
        let mut frozen = self.frozen.lock().unwrap_or_else(|e| e.into_inner());
        frozen.push(FrozenEntry {
            shared,
            ty: lane.ty,
            home: lane.home,
        });
    }

    /// 业务作用：取指定分区 worker 的唤醒点，新任务入队与执行权归还时定向唤醒。
    ///
    /// 参数说明：
    /// - `index`: 分区号。
    ///
    /// 返回：该分区的唤醒句柄。
    pub(crate) fn notify_of(&self, index: u32) -> &Notify {
        &self.notifies[index as usize]
    }

    /// 业务作用：唤醒全部 worker；停机各阶段与冻结事件用它保证没有 worker 停在旧状态上。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值。
    pub(crate) fn wake_all(&self) {
        for n in self.notifies.iter() {
            n.notify_waiters();
            n.notify_one();
        }
    }

    /// 业务作用：摘除一条延迟登记并把其定时任务句柄移交垂死收容所；取消路径与到期路径共用，
    /// 重复摘除是无害幂等。
    ///
    /// 摘除与移交必须在登记表锁内一并完成：停机 drain 先取登记表锁、后取收容所锁（同一锁序），
    /// 因此任何先于 drain 完成的摘除，其句柄必然已在收容所里对 drain 可见——不存在"两把锁
    /// 之间"的窗口让定时任务同时逃出登记表与收容所、失去被 join 的机会。
    ///
    /// 参数说明：
    /// - `id`: 登记序号。
    ///
    /// 返回：无返回值。
    pub(crate) fn retire_delayed(&self, id: u64) {
        let mut map = self.delayed.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(reg) = map.remove(&id) {
            let mut dying = self.dying.lock().unwrap_or_else(|e| e.into_inner());
            dying.handles.push(reg.handle);
            // 水位式摊销清剪,不逐次全表扫描:同步取消突发里被 abort 的定时任务拿不到调度、
            // 一个也不会结束,逐次 retain 会让第 i 次取消扫描前 i-1 个句柄,`cancel()` 这个
            // 同步调用退化成 O(N²) 的 runtime 阻塞。达到水位才剪一次,剪后按存活量翻倍
            // 重置水位:全部存活时清剪总量与插入量同阶,全部速死时水位落回下限周期回收。
            if dying.handles.len() >= dying.prune_at {
                dying.handles.retain(|handle| !handle.is_finished());
                dying.prune_at = (dying.handles.len() * 2).max(DYING_PRUNE_FLOOR);
            }
        }
    }

    /// 业务作用：冻结一条 lane 并把清扫出的未执行载荷登记为证据，同步更新健康计数。
    ///
    /// 参数说明：
    /// - `lane`: 目标 lane。
    /// - `code`: 稳定原因码（封闭集合）。
    ///
    /// 返回：无返回值；重复冻结是无害幂等。
    pub(crate) fn freeze_lane(&self, lane: &Arc<Lane>, code: u8) {
        // 冻结清扫整体作为一个归账事件:清扫内的终态发布与随后的计数共用同一归属裁决。
        let section = self.outcome_section();
        if let Some((evidence, cancelled)) = lane.fail(code) {
            self.failed_lanes.fetch_add(1, AcqRel);
            self.merge_sweep(lane, evidence, cancelled, section.stopping);
        }
    }

    /// 业务作用：提交方在 push 之后观察到 lane 已冻结时的补清扫——冻结方的排空可能发生在
    /// 本次 push 落队之前，若无人再清扫，该载荷会以 Queued 永久滞留、句柄悬挂且无证据。
    /// 补清扫沿用首次冻结的原因码，保证证据口径一致。
    ///
    /// 参数说明：
    /// - `lane`: 已冻结的目标 lane。
    ///
    /// 返回：无返回值；与冻结方或其它提交方并发清扫安全（终态由每条目 CAS 唯一裁决）。
    pub(crate) fn resweep_frozen_lane(&self, lane: &Arc<Lane>) {
        // 冻结方在 swap(failed) 与发布原因码之间只有纳秒级窗口;自旋等原因码就绪,
        // 避免同一条 lane 的证据出现两种原因。
        let code = loop {
            if let Some(code) = lane.fail_code.get() {
                break *code;
            }
            std::hint::spin_loop();
        };
        let section = self.outcome_section();
        let (evidence, cancelled) = lane.sweep_frozen(code);
        if evidence.is_empty() && cancelled == 0 {
            return;
        }
        self.merge_sweep(lane, evidence, cancelled, section.stopping);
    }

    /// 业务作用：把一次冻结清扫的产出并入全局计数与证据容器，首扫与补扫共用同一记账口径。
    ///
    /// 参数说明：
    /// - `lane`: 清扫来源 lane。
    /// - `evidence`: 本次转为失败证据的条目。
    /// - `cancelled`: 本次物理丢弃的已取消载荷数。
    /// - `stopping`: 所在归账事件段的停机归属裁决（运行期 worker 死亡的冻结不属于停机
    ///   损耗，停机清扫与迟到补清扫计入报告 frozen）。
    ///
    /// 返回：无返回值。
    fn merge_sweep(
        &self,
        lane: &Arc<Lane>,
        evidence: Vec<Arc<EntryShared>>,
        cancelled: u64,
        stopping: bool,
    ) {
        self.count_discarded(cancelled, stopping);
        if !evidence.is_empty() {
            self.frozen_count.fetch_add(evidence.len() as u64, Relaxed);
            if stopping {
                self.frozen_stop.fetch_add(evidence.len() as u64, Relaxed);
            }
        }
        let mut frozen = self.frozen.lock().unwrap_or_else(|e| e.into_inner());
        for shared in evidence {
            frozen.push(FrozenEntry {
                shared,
                ty: lane.ty,
                home: lane.home,
            });
        }
    }
}

/// 在途 producer guard：提交路径存续期间占一个名额，Drop 归还。
///
/// 停机流程先关入口、再等本计数归零，才允许 worker 按"已排空"退出——保证不存在
/// 「push 落进已被判定为空的队列」的窗口。
struct ProducerGuard {
    inner: Arc<Inner>,
}

impl ProducerGuard {
    /// 业务作用：登记一个在途 producer，使停机流程能等到本次提交完全离场。
    ///
    /// 参数说明：
    /// - `inner`: 执行器内核。
    ///
    /// 返回：Drop 时自动离场的 guard。
    fn new(inner: Arc<Inner>) -> Self {
        // SeqCst:与停机方"切阶段后读 producer"构成 Dekker 判定,弱序会让双方同时读到
        // 对方动作前的旧值(见 Inner::phase 字段注释)。
        inner.producers.fetch_add(1, SeqCst);
        Self { inner }
    }
}

impl Drop for ProducerGuard {
    /// 业务作用：提交路径离场；任何提前返回与 panic 都不遗留虚假在途计数。
    fn drop(&mut self) {
        self.inner.producers.fetch_sub(1, SeqCst);
    }
}

// ── 执行器 ──────────────────────────────────────────────────────────────────

/// 分 lane 串行/并行执行器。
pub struct PartitionExecutor {
    inner: Arc<Inner>,
    /// worker 句柄：shutdown 取走并 join（Option 实现只 join 一次）。
    workers: Mutex<Option<Vec<JoinHandle<()>>>>,
    /// 停机 worker 收口上界。
    stop_timeout: Duration,
}

/// 停机结果报告：调用方据此审计"停机是否有损"。
///
/// 完全优雅的判定是 `frozen == 0 && aborted == 0`：frozen 是未执行的损耗，aborted 是
/// 已开始执行却被中止的损耗，二者缺一个都不能宣称无损。
///
/// 各计数按事件发生时刻观察到的生命周期阶段归账（与阶段切换同一全局序），而不是
/// 用"终值减停机起点基线"推导——阶段切换对外可见后、停机驱动被调度前完成的处置
/// 同样准确计入，报告即终局事实。
#[derive(Debug, Clone, Copy)]
pub struct ShutdownReport {
    /// 停机开始后仍被正常执行完成的任务数（排空产出）。
    pub drained: u64,
    /// 停机开始后物理丢弃的已取消载荷数。
    pub cancelled: u64,
    /// 收口超时被强制中止的执行中任务数（有损；任务可能已产生部分业务效果，终态
    /// `Failed(aborted_during_shutdown)`，同样进入证据容器）。
    pub aborted: u64,
    /// 被强制冻结为未执行证据的任务数（有损；终态 `Failed(shutdown_frozen)`）。
    pub frozen: u64,
    /// 收口超时后仍有残留而被冻结清扫的 lane 数。
    pub timed_out_lanes: u32,
}

impl PartitionExecutor {
    /// 业务作用：按默认分区数（2 × CPU，向上取 2 的幂）与默认深度上限构造并启动。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已启动、可立即提交的执行器。
    pub fn new() -> Self {
        Self::with_partitions(default_partitions())
    }

    /// 业务作用：指定分区数（向上取整为 2 的幂）构造并启动，深度上限取默认值。
    ///
    /// 参数说明：
    /// - `n`: 期望分区数，至少取 1。
    ///
    /// 返回：已启动的执行器。
    pub fn with_partitions(n: usize) -> Self {
        Self::with_partitions_and_capacity(n, DEFAULT_QUEUE_CAPACITY)
    }

    /// 业务作用：指定分区数与每 lane 深度上限构造并启动；全局在飞预算取
    /// `分区数 × (深度上限 + 1)`，与「每分区一条队列 + 一个执行位」的兼容语义对齐。
    ///
    /// 参数说明：
    /// - `n`: 期望分区数，至少取 1，向上取整为 2 的幂。
    /// - `queue_capacity`: 每 lane 深度上限，至少取 1。
    ///
    /// 返回：已启动的执行器。
    pub fn with_partitions_and_capacity(n: usize, queue_capacity: usize) -> Self {
        let n = n.max(1).next_power_of_two();
        let cap = queue_capacity.max(1);
        let budget = n.saturating_mul(cap.saturating_add(1));
        Self::with_limits(n, cap, budget)
    }

    /// 业务作用：完整构造入口——分区数、每 lane 深度上限与全局在飞预算全部显式指定。
    ///
    /// 全局预算约束「排队 + 执行中」总量：类型化负载下 lane 数可远超分区数，仅靠每 lane
    /// 上限会形成 `lane 数 × 深度` 的不可控内存上界，必须由本预算封顶。
    ///
    /// 参数说明：
    /// - `n`: 期望分区数，至少取 1，向上取整为 2 的幂。
    /// - `queue_capacity`: 每 lane 深度上限，至少取 1。
    /// - `global_inflight`: 全局在飞预算，至少取 1。
    ///
    /// 返回：已启动的执行器。
    pub fn with_limits(n: usize, queue_capacity: usize, global_inflight: usize) -> Self {
        let n = n.max(1).next_power_of_two();
        let cap = queue_capacity.max(1);
        let budget = global_inflight.clamp(1, Semaphore::MAX_PERMITS);
        let inner = Arc::new(Inner {
            phase: AtomicU8::new(PHASE_ACCEPTING),
            drain_ok: AtomicBool::new(false),
            producers: AtomicI64::new(0),
            force_abort: AtomicBool::new(false),
            abort_wake: Notify::new(),
            report: OnceLock::new(),
            global: Arc::new(Semaphore::new(budget)),
            registry: LaneRegistry::new(n),
            // 上限不得低于分区数,否则兼容入口连每分区一条的保留 lane 都建不满。
            max_lanes: AtomicUsize::new(DEFAULT_MAX_LANES.max(n)),
            notifies: (0..n).map(|_| Notify::new()).collect::<Vec<_>>().into(),
            mask: n - 1,
            lane_cap: u32::try_from(cap).unwrap_or(u32::MAX),
            delayed: Mutex::new(HashMap::new()),
            dying: Mutex::new(DyingTimers {
                handles: Vec::new(),
                prune_at: DYING_PRUNE_FLOOR,
            }),
            delayed_seq: AtomicU64::new(1),
            frozen: Mutex::new(Vec::new()),
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            cancelled_total: AtomicU64::new(0),
            cancelled_discarded: AtomicU64::new(0),
            task_panics: AtomicU64::new(0),
            aborted_running: AtomicU64::new(0),
            rej_shutdown: AtomicU64::new(0),
            rej_queue_full: AtomicU64::new(0),
            rej_overloaded: AtomicU64::new(0),
            rej_ordering: AtomicU64::new(0),
            rej_lane_failed: AtomicU64::new(0),
            rej_lane_limit: AtomicU64::new(0),
            rej_reserved: AtomicU64::new(0),
            steal_attempts: AtomicU64::new(0),
            steal_success: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            frozen_count: AtomicU64::new(0),
            failed_lanes: AtomicU64::new(0),
            dead_workers: AtomicI64::new(0),
            outcome_started: AtomicU64::new(0),
            outcome_finished: AtomicU64::new(0),
            drained_stop: AtomicU64::new(0),
            discarded_stop: AtomicU64::new(0),
            frozen_stop: AtomicU64::new(0),
        });
        let handles = (0..n as u32)
            .map(|index| {
                let state = worker::WorkerState::new(inner.clone(), index);
                tokio::spawn(worker::run(state))
            })
            .collect();
        log_isolated(|| tracing::info!("napart executor started: partitions={n}"));
        Self {
            inner,
            workers: Mutex::new(Some(handles)),
            stop_timeout: DEFAULT_STOP_TIMEOUT,
        }
    }

    /// 业务作用：设置停机 worker 收口上界（默认 2s）。调大更耐心等排空；调小更快放弃挂住
    /// 的在途任务并把残留冻结为证据。
    ///
    /// 参数说明：
    /// - `d`: 收口上界；超过 365 天按 365 天生效（极大时长在期限加法中不可表示，不能让
    ///   合法配置值破坏停机推进）。
    ///
    /// 返回：链式返回自身。
    pub fn with_stop_timeout(mut self, d: Duration) -> Self {
        self.stop_timeout = d.min(MAX_STOP_TIMEOUT);
        self
    }

    /// 业务作用：设置类型化 lane 总数上限（默认 4096）。`TaskType` 是开放的 u32，注册表与
    /// 按类型导出的指标序列只增不减；上限把类型基数封顶为显式配置，超限提交返回
    /// [`SubmitRejection::LaneLimitExceeded`]，不再静默扩张内存。
    ///
    /// 兼容入口（`submit`/`submit_sync`/`submit_async`）的保留 lane 豁免本上限：typed 类型
    /// 先到先得占满额度不会让兼容提交失去容量。豁免部分每分区至多一条，进程内 lane 总数
    /// 上界为 `上限 + 分区数`。
    ///
    /// 参数说明：
    /// - `n`: 类型化 lane 总数上限；低于分区数时按分区数生效，保证至少一个任务类型可以在
    ///   全部分区展开。
    ///
    /// 返回：链式返回自身。
    pub fn with_max_lanes(self, n: usize) -> Self {
        let floor = self.inner.mask + 1;
        self.inner.max_lanes.store(n.max(floor), Relaxed);
        self
    }

    // ── 观测 ──

    /// 业务作用：判断执行器是否健康——仍在运行、无 worker 异常死亡、无 lane 冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：健康返回 true；任一结构性异常发生后返回 false，监控据此告警。
    pub fn is_healthy(&self) -> bool {
        self.inner.phase() == PHASE_ACCEPTING
            && self.inner.dead_workers.load(Acquire) == 0
            && self.inner.failed_lanes.load(Acquire) == 0
    }

    /// 业务作用：返回异常死亡的 worker 数（0 = 全部存活），供健康面板与熔断决策使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：异常死亡 worker 计数。
    pub fn dead_partitions(&self) -> i64 {
        self.inner.dead_workers.load(Acquire)
    }

    /// 业务作用：返回分区数，供容量规划与路由诊断使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分区数量。
    pub fn partitions(&self) -> usize {
        self.inner.mask + 1
    }

    /// 业务作用：返回执行器是否仍接受任务，用于准入检查与运行状态探测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：开放提交返回 true。
    pub fn is_started(&self) -> bool {
        self.inner.phase() == PHASE_ACCEPTING
    }

    /// 业务作用：返回当前 lane 总数，供容量监控观察类型分布规模。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已创建的 lane 数量。
    pub fn lanes(&self) -> usize {
        self.inner.registry.all_lanes().len()
    }

    /// 业务作用：返回已冻结 lane 数，非零表示存在拒收方向，需要运维介入或重建执行器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：冻结 lane 计数。
    pub fn failed_lanes(&self) -> u64 {
        self.inner.failed_lanes.load(Acquire)
    }

    /// 业务作用：导出低基数运行指标快照，供健康面板与告警接入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前各计数的一致性快照（逐项原子读取，不加全局锁）。
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let inner = &self.inner;
        let queued_depth = inner
            .registry
            .all_lanes()
            .values()
            .map(|l| u64::from(l.queued_hint()))
            .sum();
        MetricsSnapshot {
            submitted: inner.submitted.load(Relaxed),
            completed: inner.completed.load(Relaxed),
            cancelled: inner.cancelled_total.load(Relaxed),
            task_panics: inner.task_panics.load(Relaxed),
            aborted: inner.aborted_running.load(Relaxed),
            rejected_shutting_down: inner.rej_shutdown.load(Relaxed),
            rejected_queue_full: inner.rej_queue_full.load(Relaxed),
            rejected_overloaded: inner.rej_overloaded.load(Relaxed),
            rejected_ordering_conflict: inner.rej_ordering.load(Relaxed),
            rejected_lane_failed: inner.rej_lane_failed.load(Relaxed),
            rejected_lane_limit: inner.rej_lane_limit.load(Relaxed),
            rejected_reserved_type: inner.rej_reserved.load(Relaxed),
            steal_attempts: inner.steal_attempts.load(Relaxed),
            steal_successes: inner.steal_success.load(Relaxed),
            releases: inner.releases.load(Relaxed),
            frozen: inner.frozen_count.load(Relaxed),
            lanes: self.lanes() as u64,
            failed_lanes: inner.failed_lanes.load(Acquire),
            dead_workers: inner.dead_workers.load(Acquire),
            queued_depth,
            admit_available: inner.global.available_permits() as u64,
            delayed_pending: inner
                .delayed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len() as u64,
        }
    }

    /// 业务作用：导出停机与冻结的证据清单（不含业务载荷），供审计与人工对账。清单包含两类
    /// 残留：未执行即被冻结的任务（`shutdown_frozen`/`worker_died` 等）与执行中被停机强制
    /// 中止的任务（`aborted_during_shutdown`），按原因码区分。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：每条证据的任务类型、原始分区、终态与稳定原因码快照。
    pub fn frozen_evidence(&self) -> Vec<FrozenEvidence> {
        self.inner
            .frozen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|entry| {
                let (state, code) = entry.shared.snapshot();
                FrozenEvidence {
                    task_type: entry.ty,
                    partition: entry.home,
                    status: entry::status_of(state),
                    reason: reason_text(code),
                }
            })
            .collect()
    }

    // ── 兼容提交入口 ──

    /// 业务作用：提交一个异步任务（非阻塞）：同 key 严格按提交顺序串行，不同 key 并发。
    ///
    /// 参数说明：
    /// - `key`: 路由 key；同 key 保证串行。
    /// - `f`: 异步业务任务。
    ///
    /// 返回：入队成功返回 `Ok`；停机返回 `ShuttingDown`，容量满返回 `QueueFull`（可退避
    /// 重试或改用 [`submit_async`](Self::submit_async)），目标方向失去执行条件返回
    /// `PartitionDead`。被拒对调用方可见，不静默丢。
    pub fn submit<K, F, Fut>(&self, key: K, f: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let job: Job = Box::pin(async move { f().await });
        self.submit_entry(hash_of(key), LEGACY_TYPE, TaskOrdering::Strict, job)
            .map(|_| ())
            .map_err(to_legacy_error)
    }

    /// 业务作用：提交一个同步短任务的便捷版（非阻塞，返回语义同 [`submit`](Self::submit)）。
    ///
    /// 参数说明：
    /// - `key`: 路由 key；同 key 保证串行。
    /// - `f`: 同步业务任务。
    ///
    /// 返回：同 [`submit`](Self::submit)。
    pub fn submit_sync<K, F>(&self, key: K, f: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() + Send + 'static,
    {
        let job: Job = Box::pin(async move { f() });
        self.submit_entry(hash_of(key), LEGACY_TYPE, TaskOrdering::Strict, job)
            .map(|_| ())
            .map_err(to_legacy_error)
    }

    /// 业务作用：提交异步任务并等待容量（真背压）：先在不占任何全局资源的前提下等待 lane
    /// 深度，取得深度预留后再等待全局预算，永不返回 `QueueFull`。等待中的调用不占用全局
    /// 在飞名额，不会把其它 lane 的准入封死。
    ///
    /// **再入死锁警告**：业务任务内不得调用本方法并 `await`——不论 key、任务类型或原始
    /// 分区。运行中的任务在终态前持有一个全局许可，任务内的等待型提交要再取一份；预算贴满
    /// 时即使目标是其它分区的空闲 lane 也会互等（全局许可不分 lane）。任务内派生工作请用
    /// 非阻塞 [`submit`](Self::submit) / [`submit_typed`](Self::submit_typed) 并处理满载，
    /// 或使用独立执行器。
    ///
    /// 参数说明：
    /// - `key`: 路由 key；同 key 保证串行。
    /// - `f`: 异步业务任务。
    ///
    /// 返回：入队成功返回 `Ok`；仅在停机（`ShuttingDown`）或目标方向失去执行条件
    /// （`PartitionDead`）时返回 `Err`。
    pub async fn submit_async<K, F, Fut>(&self, key: K, f: F) -> Result<(), SubmitError>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let job: Job = Box::pin(async move { f().await });
        self.submit_waiting(hash_of(key), LEGACY_TYPE, TaskOrdering::Strict, job)
            .await
            .map(|_| ())
            .map_err(to_legacy_error)
    }

    // ── 类型化提交入口 ──

    /// 业务作用：按任务类型提交并取得稳定句柄，可查询状态、取消与等待终态。
    ///
    /// 严格类型在「原始分区 + 类型」内按提交顺序串行；非严格类型允许任意空闲分区并发执行，
    /// 每个任务仍保证至多执行一次。
    ///
    /// 参数说明：
    /// - `key`: 路由 key，决定原始分区。
    /// - `spec`: 任务类型与顺序要求；同一「分区 + 类型」的顺序要求不得混用。
    /// - `f`: 异步业务任务。
    ///
    /// 返回：受理成功返回句柄；停机、满载、预算耗尽、顺序要求冲突或 lane 已冻结时返回
    /// 对应拒绝原因。
    pub fn submit_typed<K, F, Fut>(
        &self,
        key: K,
        spec: TaskSpec,
        f: F,
    ) -> Result<Submission, SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.reject_reserved_type(spec)?;
        let job: Job = Box::pin(async move { f().await });
        let shared = self.submit_entry(hash_of(key), spec.ty.0, spec.ordering, job)?;
        Ok(Submission {
            shared,
            timer: None,
            inner: Arc::downgrade(&self.inner),
            delayed_id: None,
        })
    }

    /// 业务作用：按任务类型提交 fire-and-forget 任务（无句柄、无查询），语义同
    /// [`submit_typed`](Self::submit_typed)。
    ///
    /// 参数说明：
    /// - `key`: 路由 key。
    /// - `spec`: 任务类型与顺序要求。
    /// - `f`: 异步业务任务。
    ///
    /// 返回：受理成功返回 `Ok(())`；拒绝原因同 [`submit_typed`](Self::submit_typed)。
    pub fn exec_typed<K, F, Fut>(&self, key: K, spec: TaskSpec, f: F) -> Result<(), SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.reject_reserved_type(spec)?;
        let job: Job = Box::pin(async move { f().await });
        self.submit_entry(hash_of(key), spec.ty.0, spec.ordering, job)
            .map(|_| ())
    }

    /// 业务作用：登记延迟任务，登记时计算 key 的进程内路由，到期后才查找或创建 lane 并占用
    /// lane 深度；到期前仅占一个全局名额。
    ///
    /// 参数说明：
    /// - `key`: 路由 key；登记时完成 hash，分区数在执行器生命周期内不变。
    /// - `delay`: 延迟时长，无上限；超出底层定时器可表示范围的时长按有界分段消耗，任务
    ///   绝不早于 `登记时刻 + delay` 到期（`Duration::MAX` 语义上即"永不到期"，仍可取消，
    ///   停机时照常置为 `Rejected(shutdown_before_expiry)`）。零延迟不走定时器，与
    ///   [`submit_typed`](Self::submit_typed) 完全等价——同一容量状态下受理/拒绝边界一致，
    ///   满载同步返回 `QueueFull` 而不是先受理再异步 `Rejected`。
    /// - `spec`: 任务类型与顺序要求。
    /// - `f`: 异步业务任务。
    ///
    /// 返回：登记成功返回可在到期前取消的句柄；停机拒绝登记，全局预算耗尽返回 `Overloaded`。
    /// 到期时若已停机或 lane 满载，任务转入 `Rejected` 终态并携带稳定原因码
    /// （`shutdown_before_expiry` / `queue_full_at_expiry` 等），不静默消失。
    pub fn submit_after<K, F, Fut>(
        &self,
        key: K,
        delay: Duration,
        spec: TaskSpec,
        f: F,
    ) -> Result<Submission, SubmitRejection>
    where
        K: Hash,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.reject_reserved_type(spec)?;
        // 零延迟等价于立即提交:绕过定时器,让"同一容量状态得到同一受理结论"成立——
        // 经定时器会把同步的 QueueFull 变成"先 Delayed 后异步 Rejected",错误处理口径漂移。
        if delay.is_zero() {
            let job: Job = Box::pin(async move { f().await });
            let shared = self.submit_entry(hash_of(key), spec.ty.0, spec.ordering, job)?;
            return Ok(Submission {
                shared,
                timer: None,
                inner: Arc::downgrade(&self.inner),
                delayed_id: None,
            });
        }
        if self.inner.phase() != PHASE_ACCEPTING {
            self.inner.rej_shutdown.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        let _producer = ProducerGuard::new(self.inner.clone());
        // 登记 producer 后复验停机:停机方以"切阶段后 producer 归零"为界排空延迟登记表,
        // 本次登记要么在此让路,要么整体先于登记表排空完成,不存在漏拒的登记。
        if self.inner.phase() != PHASE_ACCEPTING {
            self.inner.rej_shutdown.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        // 延迟任务从登记起就占用全局名额:否则大量未到期任务可绕过预算,到期瞬间冲垮执行器。
        let permit = match self.inner.global.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(tokio::sync::TryAcquireError::Closed) => {
                self.inner.rej_shutdown.fetch_add(1, Relaxed);
                return Err(SubmitRejection::ShuttingDown);
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.inner.rej_overloaded.fetch_add(1, Relaxed);
                return Err(SubmitRejection::Overloaded);
            }
        };
        let shared = EntryShared::new(STATE_DELAYED, permit);
        // 载荷从登记起就交由析构护卫持有:取消 abort、停机 abort、执行器直接 Drop、到期
        // 各拒绝分支与定时任务异常离场都经护卫在隔离边界内析构,与排队载荷丢弃同一口径。
        let job = DelayedJobGuard::new(Box::pin(async move { f().await }));
        let id = self.inner.delayed_seq.fetch_add(1, Relaxed);
        let key_hash = hash_of(key);
        let inner = self.inner.clone();
        let timer_shared = shared.clone();
        // 先发布登记、再放行到期回调:两步都在登记表锁内完成。定时任务的第一步 remove 会
        // 阻塞在同一把锁上,因此不存在"到期回调先 remove、登记后 insert"的窗口——那种
        // 交错会在登记表里留下永不摘除的僵尸记录。锁内只做 spawn 与 insert,无 await。
        {
            let mut map = self.inner.delayed.lock().unwrap_or_else(|e| e.into_inner());
            // 起算时点在登记提交的同一同步临界区内锁定:spawn 只是入调度队列,定时任务
            // 首次 poll 可被停顿任意推迟;若在任务体内才取起点,停顿会在到期等待上整段
            // 重复一次,"任务在登记时刻 + delay 到期"的公开合同失效。
            let registered_at = tokio::time::Instant::now();
            let handle = tokio::spawn(async move {
                sleep_full_delay(registered_at, delay).await;
                // 到期先摘除登记并把自身句柄移交收容所:停机拒绝路径与本回调对同一登记至多
                // 一方处理,且无论谁处理,句柄都留有可被停机 join 的退出证明。
                inner.retire_delayed(id);
                expire_delayed(inner, timer_shared, key_hash, spec, job);
            });
            let abort = handle.abort_handle();
            map.insert(
                id,
                DelayedReg {
                    shared: shared.clone(),
                    handle,
                },
            );
            drop(map);
            // 登记返回句柄即受理:submitted 在此计入,与"占用全局名额、可取消、可等终态"的
            // 生命周期口径一致;到期入队不再重复计数,否则延迟任务被计两次或(取消时)零次,
            // 公开累计量会出现 cancelled > submitted 的倒挂。
            self.inner.submitted.fetch_add(1, Relaxed);
            Ok(Submission {
                shared,
                timer: Some(abort),
                inner: Arc::downgrade(&self.inner),
                delayed_id: Some(id),
            })
        }
    }

    // ── 内部提交路径 ──

    /// 业务作用：同步无 await 的核心提交路径——生命周期、路由、双层准入、入队、唤醒一次完成。
    ///
    /// 全程无 `.await` 即无取消点：不存在"预留了深度却在入队前被取消"的半完成窗口。
    ///
    /// 参数说明：
    /// - `key_hash`: 已计算的路由 hash。
    /// - `ty`: 任务类型原始值。
    /// - `ordering`: 顺序要求。
    /// - `job`: 业务任务 future。
    ///
    /// 返回：受理成功返回任务共享状态；否则返回具体拒绝原因并同步计数。
    fn submit_entry(
        &self,
        key_hash: u64,
        ty: u32,
        ordering: TaskOrdering,
        job: Job,
    ) -> Result<Arc<EntryShared>, SubmitRejection> {
        // 停机快速短路:不进任何准入,让停机流程尽快观察到 producer 归零。
        if self.inner.phase() != PHASE_ACCEPTING {
            self.inner.rej_shutdown.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        let _producer = ProducerGuard::new(self.inner.clone());
        // 二次检查:停机可能在登记 producer 之后才切阶段;此时主动退出,让 worker 排空的
        // 是一个不再增长的队列集合。
        if self.inner.phase() != PHASE_ACCEPTING {
            self.inner.rej_shutdown.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        let home = (spread(key_hash) & self.inner.mask) as u32;
        let lane = self.lookup_lane(home, ty, ordering)?;
        if lane.is_failed() {
            self.inner.rej_lane_failed.fetch_add(1, Relaxed);
            return Err(SubmitRejection::LaneFailed);
        }
        let permit = match self.inner.global.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(tokio::sync::TryAcquireError::Closed) => {
                self.inner.rej_shutdown.fetch_add(1, Relaxed);
                return Err(SubmitRejection::ShuttingDown);
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.inner.rej_overloaded.fetch_add(1, Relaxed);
                return Err(SubmitRejection::Overloaded);
            }
        };
        if !lane.try_reserve_depth() {
            self.inner.rej_queue_full.fetch_add(1, Relaxed);
            return Err(SubmitRejection::QueueFull);
        }
        let reservation = DepthReservation::new(lane.clone());
        let shared = EntryShared::new(STATE_QUEUED, permit);
        lane.queue.push(EntryPayload {
            shared: shared.clone(),
            job,
        });
        // push 成功,深度记账移交队列(此后由 pop 侧归还)。
        reservation.commit();
        confirm_push(&self.inner, &lane);
        self.inner.submitted.fetch_add(1, Relaxed);
        // 定向唤醒当前执行权持有方:严格 lane 可能已被窃取,唤醒 home 会空转一轮。
        let owner = lane.owner.load(Acquire);
        let target = if (owner as usize) <= self.inner.mask {
            owner
        } else {
            home
        };
        self.inner.notify_of(target).notify_one();
        Ok(shared)
    }

    /// 业务作用：拒绝保留任务类型值。`u32::MAX` 是兼容入口保留 lane 的内部身份：放行会让
    /// 公开 typed 输入以不同顺序要求首建保留 lane（该分区兼容 `submit` 从此永久
    /// `PartitionDead`），或借用保留 lane 的上限豁免绕过类型基数约束。文档声明"保留"不能
    /// 约束安全 Rust 的公开输入，必须在入口 fail-fast。
    ///
    /// 参数说明：
    /// - `spec`: 调用方提交的任务规格。
    ///
    /// 返回：类型合法返回 `Ok(())`；使用保留值返回 `ReservedTaskType` 并计数。
    fn reject_reserved_type(&self, spec: TaskSpec) -> Result<(), SubmitRejection> {
        if spec.ty.0 == LEGACY_TYPE {
            self.inner.rej_reserved.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ReservedTaskType);
        }
        Ok(())
    }

    /// 业务作用：提交入口的 lane 查找/创建，并把创建失败翻译为对调用方可见的拒绝与计数。
    ///
    /// 参数说明：
    /// - `home`: 原始分区号。
    /// - `ty`: 任务类型原始值。
    /// - `ordering`: 顺序要求。
    ///
    /// 返回：lane 或对应拒绝（`OrderingConflict` / `LaneLimitExceeded`）。
    fn lookup_lane(
        &self,
        home: u32,
        ty: u32,
        ordering: TaskOrdering,
    ) -> Result<Arc<Lane>, SubmitRejection> {
        match self.inner.registry.get_or_create(
            home,
            ty,
            ordering,
            self.inner.lane_cap,
            self.inner.max_lanes.load(Relaxed),
        ) {
            Ok(lane) => Ok(lane),
            Err(LaneCreateError::OrderingConflict) => {
                self.inner.rej_ordering.fetch_add(1, Relaxed);
                Err(SubmitRejection::OrderingConflict)
            }
            Err(LaneCreateError::LimitExceeded) => {
                self.inner.rej_lane_limit.fetch_add(1, Relaxed);
                Err(SubmitRejection::LaneLimitExceeded)
            }
        }
    }

    /// 业务作用：等待容量的提交路径——全局预算与 lane 深度都可等待腾出，用于真背压场景。
    ///
    /// 等待只发生在准入之前；一旦开始「预留深度 → 入队」的序列即同步完成，不跨越任何
    /// 取消点。
    ///
    /// 参数说明：
    /// - `key_hash`: 已计算的路由 hash。
    /// - `ty`: 任务类型原始值。
    /// - `ordering`: 顺序要求。
    /// - `job`: 业务任务 future。
    ///
    /// 返回：受理成功返回任务共享状态；停机、顺序冲突或 lane 冻结返回对应拒绝。
    async fn submit_waiting(
        &self,
        key_hash: u64,
        ty: u32,
        ordering: TaskOrdering,
        job: Job,
    ) -> Result<Arc<EntryShared>, SubmitRejection> {
        if self.inner.phase() != PHASE_ACCEPTING {
            self.inner.rej_shutdown.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        let home = (spread(key_hash) & self.inner.mask) as u32;
        let lane = self.lookup_lane(home, ty, ordering)?;
        // 双资源等待顺序:先等 lane 深度、后等全局许可。等待深度期间不持有任何全局资源——
        // 尚未受理的等待者若先占全局许可,一个热点 lane 的等待队列就能耗尽全局预算,把其它
        // lane 的非阻塞准入全部封死(跨 lane 头阻塞)。等待期间也不持有 producer 计数:
        // producer 收口语义是"短同步提交临界区",若跨 await 持有,批量唤醒中被吸收的异常
        // Waker 会让本 future 失去复验调度,停机驱动的无界收口等待随之永久悬挂。等待经
        // 防护罩注册,单个等待者的 Waker 异常不会中断共享设施的批量唤醒。
        loop {
            if lane.is_failed() {
                self.inner.rej_lane_failed.fetch_add(1, Relaxed);
                return Err(SubmitRejection::LaneFailed);
            }
            if self.inner.phase() != PHASE_ACCEPTING {
                self.inner.rej_shutdown.fetch_add(1, Relaxed);
                return Err(SubmitRejection::ShuttingDown);
            }
            if lane.try_reserve_depth() {
                break;
            }
            let notified = lane.space.notified();
            if lane.try_reserve_depth() {
                break;
            }
            // 登记等待者之后必须再验一次停机:停机唤醒清扫只覆盖清扫时刻已登记的等待者,
            // 漏掉这一验会让本调用永久滞留在无人再唤醒的容量等待上。
            if self.inner.phase() != PHASE_ACCEPTING {
                self.inner.rej_shutdown.fetch_add(1, Relaxed);
                return Err(SubmitRejection::ShuttingDown);
            }
            await_shielded(notified).await;
        }
        // 持深度等全局:预留由 guard 的 Drop 兜底,本调用被取消或停机拒绝都会回滚深度并唤醒
        // 容量等待者,不泄漏。持有的只是目标 lane 自身的队列容量,背压绑定在调用方自己选择的
        // key 上,不外溢到其它 lane。死锁不可能:等待方只按"深度→全局"单一顺序持有并等待,
        // 非阻塞提交对两种资源都只 try 不等,不存在反向持有等待的环。
        let reservation = DepthReservation::new(lane.clone());
        // 等待全局预算:停机会 close 信号量,等待者以 ShuttingDown 退出而不是永久挂起;
        // 关闭的批量唤醒同样经防护罩,异常等待者不拖累同批正常等待者。
        let permit = match await_shielded(self.inner.global.clone().acquire_owned()).await {
            Ok(p) => p,
            Err(_) => {
                self.inner.rej_shutdown.fetch_add(1, Relaxed);
                return Err(SubmitRejection::ShuttingDown);
            }
        };
        // 双资源齐备后进入短同步提交临界区:登记 producer、复验停机与冻结,任何失败路径的
        // 资源回滚由两个 guard 的 Drop 完成。停机方以"切阶段后等 producer 归零"为界,
        // 本临界区无 await,收口有界。
        let _producer = ProducerGuard::new(self.inner.clone());
        if self.inner.phase() != PHASE_ACCEPTING {
            self.inner.rej_shutdown.fetch_add(1, Relaxed);
            return Err(SubmitRejection::ShuttingDown);
        }
        if lane.is_failed() {
            self.inner.rej_lane_failed.fetch_add(1, Relaxed);
            return Err(SubmitRejection::LaneFailed);
        }
        let shared = EntryShared::new(STATE_QUEUED, permit);
        lane.queue.push(EntryPayload {
            shared: shared.clone(),
            job,
        });
        reservation.commit();
        confirm_push(&self.inner, &lane);
        self.inner.submitted.fetch_add(1, Relaxed);
        let owner = lane.owner.load(Acquire);
        let target = if (owner as usize) <= self.inner.mask {
            owner
        } else {
            home
        };
        self.inner.notify_of(target).notify_one();
        Ok(shared)
    }

    // ── 停机 ──

    /// 业务作用：优雅停机（兼容入口）。返回时所有已受理任务要么执行完毕、要么已进入带
    /// 原因的失败终态并留证记录错误日志；worker、在途业务任务与延迟定时器均已凭 join
    /// 证明退出，不再有本执行器任务在后台运行。幂等。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；需要审计停机损耗时改用
    /// [`shutdown_with_report`](Self::shutdown_with_report)。
    pub async fn shutdown(&self) {
        let report = self.shutdown_with_report().await;
        if report.frozen > 0 || report.aborted > 0 {
            log_isolated(|| {
                tracing::error!(
                    frozen = report.frozen,
                    aborted = report.aborted,
                    timed_out_lanes = report.timed_out_lanes,
                    "napart shutdown 有损:未排空任务已冻结为证据,在途任务被强制中止"
                );
            });
        }
        log_isolated(|| tracing::info!("napart executor shutdown complete"));
    }

    /// 业务作用：分阶段停机并产出可审计报告——关入口、等 producer 收口、拒延迟登记、排空、
    /// 超时强制中止在途任务并冻结残留。
    ///
    /// 停机流程运行在独立的受监督任务里：本方法的调用 future 被 timeout/select 取消不会把
    /// 生命周期卡死在停机中；后续任何调用都会继续等待并取回同一份终局报告。
    ///
    /// 完成边界：返回时全部 worker、被强制中止的业务任务与延迟定时器都已按 join 结果确认
    /// 退出，而不是仅发出取消请求。被中止的任务在其下一个让出点结束——从不让出的业务
    /// future 无法被任何方式取消，会推迟停机完成，这是协作式调度的固有边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：所有调用方（含并发与事后调用）都获得同一份终局损耗报告。`frozen > 0` 或
    /// `aborted > 0` 表示有损停机，对应任务以 `Failed` 终态与稳定原因码保留在证据容器中。
    pub async fn shutdown_with_report(&self) -> ShutdownReport {
        // 阶段 CAS 保证只有首个调用者启动停机驱动;并发调用者等待完成即可,不重复清扫。
        if self
            .inner
            .phase
            .compare_exchange(PHASE_ACCEPTING, PHASE_STOPPING, SeqCst, SeqCst)
            .is_ok()
        {
            let handles = self
                .workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            // 驱动放进独立任务且与阶段 CAS 之间无 await:调用方 future 随后无论被怎样
            // 取消,驱动都已存在并终将把阶段推进到 STOPPED,生命周期不会滞留在 Stopping。
            tokio::spawn(drive_shutdown(
                self.inner.clone(),
                handles,
                self.stop_timeout,
            ));
        }
        while self.inner.phase() != PHASE_STOPPED {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // 驱动先写报告、后发布 STOPPED,观察到 STOPPED 即保证报告可读。
        self.inner.report.get().copied().unwrap_or(ShutdownReport {
            drained: 0,
            cancelled: 0,
            aborted: 0,
            frozen: 0,
            timed_out_lanes: 0,
        })
    }
}

/// 业务作用：停机驱动——在独立受监督任务中完成关入口、producer 收口、拒延迟登记、排空、
/// 强制中止与冻结清扫的全部阶段，并在发布 STOPPED 前写入终局报告。
///
/// 参数说明：
/// - `inner`: 执行器内核。
/// - `handles`: worker JoinHandle 集合（首个停机者取走；重复停机为 None）。
/// - `stop_timeout`: 排空阶段的收口上界。
///
/// 返回：无返回值；返回时阶段已是 STOPPED 且报告已写入。
async fn drive_shutdown(
    inner: Arc<Inner>,
    handles: Option<Vec<JoinHandle<()>>>,
    stop_timeout: Duration,
) {
    // 报告不在此采样任何基线:停机期损耗与排空产出由各归账事件段(OutcomeSection)在
    // 发生处裁决归属。驱动被调度的早晚只影响推进节奏,不影响报告归属。
    //
    // 等待切相时已登记的归账事件收口:凡归属裁决读到 ACCEPTING 的事件,其登记先于切相,
    // 必然落在本快照内;等它们收口后,"切相前公开的终态"已全部按非停机记账完毕,后续
    // 停机推进不会与公开时序矛盾。快照之后新登记的事件读到的必是 STOPPING,无需等待。
    let registered_before_stop = inner.outcome_started.load(SeqCst);
    while inner.outcome_finished.load(SeqCst) < registered_before_stop {
        tokio::time::sleep(Duration::from_micros(50)).await;
    }

    // 关闭全局预算信号量:等待预算的 submit_async 立即以 ShuttingDown 退出。批量唤醒的
    // 等待者 waker 已由防护罩转发,单个异常不越过驱动边界;这里再以隔离边界执行,双保险。
    run_isolated("停机预算关闭", || inner.global.close());
    inner.wake_all();
    for lane in inner.registry.all_lanes().values() {
        // 容量等待者的 Waker 可能是调用方手工 poll 的外部安全代码,panic(含其 payload 的
        // 析构 panic)不得沿停机驱动展开——驱动一旦死亡,生命周期永远到不了 STOPPED。
        run_isolated("停机容量唤醒", || lane.space.notify_waiters());
    }

    // 等 producer 收口:在此之前不能允许 worker 按"已排空"退出,否则迟到 push 无人服务。
    // 无界等待:提交临界区是无 await 的短同步段,唯一能拖长它的是 OS 抢占、分配器停顿这类
    // 进程级停摆。在 producer 仍可能完成 push 时发布 STOPPED,会让终局报告被返回后的迟到
    // 提交改写(报告 frozen=0 而实际产生新冻结)——宁可等待并周期告警,不可谎报停止。
    let mut waited = Duration::ZERO;
    while inner.producers.load(SeqCst) > 0 {
        tokio::time::sleep(Duration::from_micros(50)).await;
        waited += Duration::from_micros(50);
        if waited >= PRODUCER_DRAIN_WARN {
            waited = Duration::ZERO;
            log_isolated(|| {
                tracing::error!(
                    producers = inner.producers.load(SeqCst),
                    "napart shutdown 等待 producer 收口超时,继续等待离场"
                );
            });
        }
    }

    // 拒绝全部未到期延迟任务:到期回调与本路径对同一登记的处置由状态 CAS 决出唯一胜者。
    // abort 之后必须 await JoinHandle——"定时器已退出"要有 join 证明,否则停机返回后
    // 仍可能有本执行器的定时任务在后台存活。
    let delayed: Vec<DelayedReg> = {
        let mut map = inner.delayed.lock().unwrap_or_else(|e| e.into_inner());
        map.drain().map(|(_, reg)| reg).collect()
    };
    for reg in delayed {
        // 拒绝计数作为终态胜者的提交闭包执行:败给取消/到期竞争的登记已由对方计入各自
        // 口径,外部 Waker panic 也拆不开终态与计数。
        let _ = reg.shared.finish(
            STATE_DELAYED,
            STATE_REJECTED,
            reason_code::SHUTDOWN_BEFORE_EXPIRY,
            || {
                inner.rej_shutdown.fetch_add(1, Relaxed);
            },
        );
        reg.handle.abort();
        let _ = reg.handle.await;
    }

    // join 垂死收容所:已取消(abort 在途)与自然到期收尾中的定时任务都在这里留有句柄。
    // 登记表 drain 完成后不会再有新条目(余下登记全部已进终态,retire 的前置 CAS 必败),
    // 因此一次 take 即完整;逐个 await 拿到全部定时任务的析构证明。
    let dying: Vec<JoinHandle<()>> = {
        let mut list = inner.dying.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut list.handles)
    };
    for handle in dying {
        handle.abort();
        let _ = handle.await;
    }

    // producer 已收口:放行 worker 的"排空即退出"判定。
    inner.drain_ok.store(true, Release);
    inner.wake_all();

    // 有界排空:先给业务任务 stop_timeout 的机会正常跑完;超时后进入强制中止,
    // 但不 abort worker 本身——worker 收到信号后自行中止在途任务并干净退出。
    if let Some(handles) = handles {
        // checked_add 兜底:期限不可表示时按上界钳制,溢出展开会杀死唯一停机驱动。
        let deadline = Instant::now()
            .checked_add(stop_timeout)
            .unwrap_or_else(|| Instant::now() + MAX_STOP_TIMEOUT);
        let mut undrained = Vec::new();
        for mut handle in handles {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remaining, &mut handle).await.is_err() {
                undrained.push(handle);
            }
        }
        if !undrained.is_empty() {
            // 推进所需状态先于诊断:本驱动是唯一且未保留 JoinHandle 的停机执行者,诊断
            // 回调若在置位强制中止信号之前展开,驱动死亡后生命周期将永久停在 Stopping。
            inner.force_abort.store(true, Release);
            inner.abort_wake.notify_waiters();
            inner.wake_all();
            log_isolated(|| {
                tracing::error!(
                    timeout_ms = stop_timeout.as_millis() as u64,
                    workers = undrained.len(),
                    "napart shutdown 排空超时,强制中止在途任务"
                );
            });
            // 无界等待退出证明:abort 在任务下一个让出点生效,任何会让出的业务 future 都
            // 很快退出;在这里返回"已停止"而实际任务仍在跑,上层就会在旧任务仍可写副作用
            // 时关闭资源或发布新代际——宁可等待,不可谎报。
            for handle in undrained {
                let _ = handle.await;
            }
        }
    }

    // 冻结清扫:仍有残留载荷的 lane 全部冻结,载荷转为证据。worker 已全部凭 join 退出,
    // 清扫与消费不存在并发;producer 已收口,不存在未转正的容量预留。
    let mut timed_out_lanes = 0u32;
    for lane in inner.registry.all_lanes().values() {
        if lane.queued_hint() > 0 {
            timed_out_lanes += 1;
            inner.freeze_lane(lane, reason_code::SHUTDOWN_FROZEN);
        }
    }

    // 发布 STOPPED 前等全部在途归账事件收口:清扫后所有条目已终态,此刻仍在途的事件只剩
    // 已胜出终态 CAS、但累计量尚未提交的外部取消调用(以及注定失败的迟到取消尝试,均为
    // 纳秒级)。不等它们,STOPPED 之后公开 cancelled 仍会被旧调用改写,README 的"排空后
    // 守恒""报告即终局事实"就不成立。
    while inner.outcome_finished.load(SeqCst) < inner.outcome_started.load(SeqCst) {
        tokio::time::sleep(Duration::from_micros(50)).await;
    }

    let report = ShutdownReport {
        drained: inner.drained_stop.load(Relaxed),
        cancelled: inner.discarded_stop.load(Relaxed),
        aborted: inner.aborted_running.load(Relaxed),
        frozen: inner.frozen_stop.load(Relaxed),
        timed_out_lanes,
    };
    // 先写报告、再发布 STOPPED:等待方以阶段为可见性门槛读取报告。
    let _ = inner.report.set(report);
    inner.phase.store(PHASE_STOPPED, SeqCst);
    inner.wake_all();
}

impl Default for PartitionExecutor {
    /// 业务作用：返回默认配置执行器，用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PartitionExecutor {
    /// 业务作用：所有者析构时的非阻塞尽力收口。直接 Drop 执行器若不做任何事，常驻 worker
    /// 会带着整个内核（lane 注册表、指标、队列）被永久分离成后台泄漏——JoinHandle 的
    /// Drop 只是 detach，不取消任务。这里拒新、终态化全部排队载荷、拒延迟登记并向全部
    /// 常驻任务发出中止与唤醒请求；协作式任务在下一个让出点退出，poll 不返回的非协作
    /// 业务 future 无法被抢占、可能继续存活。Drop 无法 await，不提供退出证明与损耗报告，
    /// 需要终局报告与 join 证明必须显式调用 [`shutdown`](Self::shutdown)。已进入停机流程
    /// （含已停止）时不做任何事，停机驱动持有唯一收口权。
    fn drop(&mut self) {
        if self
            .inner
            .phase
            .compare_exchange(PHASE_ACCEPTING, PHASE_STOPPING, SeqCst, SeqCst)
            .is_err()
        {
            return;
        }
        // 拒新并唤醒全部等待者:预算/容量等待者立即以 ShuttingDown 退出,不残留挂起调用。
        run_isolated("析构预算关闭", || self.inner.global.close());
        // worker 在循环顶与业务等待 select 中都观察强制中止信号,协作退出不需要 abort
        // JoinHandle;信号先于唤醒发布。非协作业务 future(poll 不返回)无法被抢占,其
        // 承载任务在该 future 让出前继续存活,这是协作式调度的公开边界。
        self.inner.force_abort.store(true, Release);
        self.inner.abort_wake.notify_waiters();
        self.inner.wake_all();
        for lane in self.inner.registry.all_lanes().values() {
            run_isolated("析构容量唤醒", || lane.space.notify_waiters());
        }
        // 终态化全部排队载荷:worker 收到强制中止信号后不再 pop,残留 Queued 句柄若无人
        // 发布终态将永久悬挂。无条件冻结每条 lane——空 lane 冻结无副作用,且让本清扫之后
        // 才落队的迟到 push 经提交方复验自行转为证据,不存在二次滞留窗口。
        for lane in self.inner.registry.all_lanes().values() {
            self.inner.freeze_lane(lane, reason_code::SHUTDOWN_FROZEN);
        }
        // 拒绝全部延迟登记并请求中止定时任务:句柄进入带原因终态,被中止的定时任务由
        // 运行时在其让出点回收,不作为分离任务残留。Drop 不 join,退出证明由显式停机提供。
        let delayed: Vec<DelayedReg> = {
            let mut map = self.inner.delayed.lock().unwrap_or_else(|e| e.into_inner());
            map.drain().map(|(_, reg)| reg).collect()
        };
        for reg in delayed {
            let _ = reg.shared.finish(
                STATE_DELAYED,
                STATE_REJECTED,
                reason_code::SHUTDOWN_BEFORE_EXPIRY,
                || {
                    self.inner.rej_shutdown.fetch_add(1, Relaxed);
                },
            );
            reg.handle.abort();
        }
        let dying: Vec<JoinHandle<()>> = {
            let mut list = self.inner.dying.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut list.handles)
        };
        for handle in dying {
            handle.abort();
        }
    }
}

/// 单条冻结证据的只读快照（不含业务载荷）。
#[derive(Debug, Clone, Copy)]
pub struct FrozenEvidence {
    /// 任务类型原始值（`u32::MAX` 为兼容入口）。
    pub task_type: u32,
    /// 原始分区号。
    pub partition: u32,
    /// 证据条目的终态。
    pub status: TaskStatus,
    /// 稳定原因码。
    pub reason: Option<&'static str>,
}

/// 低基数运行指标快照。
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct MetricsSnapshot {
    /// 已受理任务总数。受理即计数：立即提交在入队时点，延迟提交在登记返回句柄时点——
    /// 延迟任务从登记起就占用预算、可取消、可等终态，不计入会让公开累计量出现
    /// `cancelled > submitted` 的倒挂。
    pub submitted: u64,
    /// 已执行完成总数。
    pub completed: u64,
    /// 成功取消总数（终态 Cancelled 的真实次数，含到期前取消的延迟任务；取消发生即计数，
    /// 不等待载荷被物理丢弃）。
    pub cancelled: u64,
    /// 任务 panic 总数（单笔失败，不冻结 lane）。
    pub task_panics: u64,
    /// 停机强制中止的执行中任务总数（有损停机的另一半口径，与 frozen 并列）。
    pub aborted: u64,
    /// 因停机被拒总数（含同步拒绝、延迟到期时已停机与停机主动拒绝未到期登记）。
    pub rejected_shutting_down: u64,
    /// 因 lane 深度满被拒总数（含同步拒绝与延迟到期时满载 `queue_full_at_expiry`）。
    pub rejected_queue_full: u64,
    /// 因全局预算耗尽被拒总数。
    pub rejected_overloaded: u64,
    /// 因顺序要求冲突被拒总数（含延迟到期变体）。
    pub rejected_ordering_conflict: u64,
    /// 因 lane 冻结被拒总数（含延迟到期变体）。
    pub rejected_lane_failed: u64,
    /// 因类型化 lane 总数达配置上限被拒总数（含延迟到期变体）。
    pub rejected_lane_limit: u64,
    /// 因使用保留任务类型值 `TaskType(u32::MAX)` 被拒总数。
    pub rejected_reserved_type: u64,
    /// 严格 lane 执行权窃取尝试总数（含失败竞争）。
    pub steal_attempts: u64,
    /// 严格 lane 执行权窃取成功总数。
    pub steal_successes: u64,
    /// 严格 lane 执行权归还总数。
    pub releases: u64,
    /// 被冻结为未执行证据的任务总数。
    pub frozen: u64,
    /// 当前 lane 总数。
    pub lanes: u64,
    /// 当前已冻结 lane 数。
    pub failed_lanes: u64,
    /// 异常死亡 worker 数。
    pub dead_workers: i64,
    /// 当前全部 lane 排队深度之和。
    pub queued_depth: u64,
    /// 全局预算剩余许可数。
    pub admit_available: u64,
    /// 当前延迟登记数（未到期且未取消）。长驻进程该值应随负载波动而非单调增长。
    pub delayed_pending: u64,
}

// ── 延迟到期 ────────────────────────────────────────────────────────────────

/// 业务作用：把任意时长的延迟消耗为若干可表示的有界 Sleep，保证任务在
/// `registered_at + delay` 到期——绝不提前，调度停顿后也不把停顿重复叠加成额外迟到。
/// 底层定时器对不可表示的截止点不报错也不保留原时长，而是静默退化为约 30 年后的
/// 近似值；若把 `delay` 原样交给单次 Sleep，`Duration::MAX` 这类"近似永不到期"的
/// 合法输入会被截短并提前执行，违反延迟提交的核心时间合同。
///
/// 参数说明：
/// - `registered_at`: 起算时点，必须在同步登记临界区内取得。定时任务经 spawn 入队后
///   首次 poll 可被 runtime 停顿任意推迟，若在任务体内重取起点，整段停顿会在到期
///   等待上重复一次；runtime 恢复后应依据原登记截止点立即到期。
/// - `delay`: 请求的延迟时长，无上限；超出单段上界的部分按剩余量分段继续等待。
///
/// 返回：时钟自 `registered_at` 起累计推进不少于 `delay` 后返回（起算点已过期则立即
/// 返回）；每段等待都是取消点，定时任务被中止时立即退出。
async fn sleep_full_delay(registered_at: tokio::time::Instant, delay: Duration) {
    // 按"起算点至今已推进时长"结算剩余量而非按段递减:时钟一次性大步前进或发生休眠补偿时
    // 不重复等待已流逝的部分,各段的调度滞后也不会随段数累加。
    loop {
        let elapsed = registered_at.elapsed();
        let Some(remaining) = delay.checked_sub(elapsed) else {
            return;
        };
        if remaining.is_zero() {
            return;
        }
        tokio::time::sleep(remaining.min(DELAY_SLEEP_SEGMENT)).await;
    }
}

/// 业务作用：延迟任务业务载荷的析构护卫——把 `Job` 在定时任务内的全部未执行结局
/// （到期前取消 abort、停机 abort、执行器直接 Drop、到期各拒绝分支、定时任务异常离场）
/// 收进与排队载荷丢弃同一口径的隔离边界。业务 future 的析构是外部代码，裸随定时任务
/// 析构会把单次 panic 变成无人读取的 task 失败，公开审计无法区分"正常未执行析构"与
/// "载荷析构展开"。到期成功入队时显式取走所有权转交 lane 条目，护卫置空、不重复析构。
struct DelayedJobGuard(Option<Job>);

impl DelayedJobGuard {
    /// 业务作用：登记时捕获业务载荷，此后载荷的所有权路径唯一（入队转交或护卫析构）。
    ///
    /// 参数说明：
    /// - `job`: 业务任务 future。
    ///
    /// 返回：持有载荷的护卫。
    fn new(job: Job) -> Self {
        Self(Some(job))
    }

    /// 业务作用：到期成功入队时取走载荷所有权转交 lane 条目；每个护卫仅在此消费一次，
    /// 此后护卫析构为空操作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：登记时捕获的业务载荷。
    fn take_for_enqueue(&mut self) -> Job {
        self.0
            .take()
            .expect("延迟载荷护卫在入队前必然持有载荷:构造即填充,取走仅入队一处")
    }
}

impl Drop for DelayedJobGuard {
    fn drop(&mut self) {
        if let Some(job) = self.0.take() {
            // 未执行结局统一在此收口:载荷 Drop 的单次 panic 由隔离边界捕获并结构化记录,
            // 不升级为不可观察的定时任务失败,也不沿停机/取消路径展开。
            run_isolated("延迟载荷析构", move || drop(job));
        }
    }
}

/// 业务作用：延迟任务到期回调——按当时生命周期与路由完成受理，或以稳定原因进入拒绝终态。
///
/// 参数说明：
/// - `inner`: 执行器内核。
/// - `shared`: 任务共享状态（DELAYED，托管登记时取得的全局名额）。
/// - `key_hash`: 登记时计算的路由 hash。
/// - `spec`: 任务类型与顺序要求。
/// - `job`: 业务载荷析构护卫；拒绝分支返回时载荷留在护卫内，由其在隔离边界内析构。
///
/// 返回：无返回值；任何不能入队的分支都发布带原因的终态（终态发布即归还全局名额），
/// 任务不静默消失，未入队载荷经护卫在隔离边界内析构。
fn expire_delayed(
    inner: Arc<Inner>,
    shared: Arc<EntryShared>,
    key_hash: u64,
    spec: TaskSpec,
    mut job: DelayedJobGuard,
) {
    // 到期拒绝与同步拒绝共用同一组公开计数:任务已计入 submitted(登记时点),终态 Rejected
    // 若不进拒绝指标,到期满载/停机拒绝对告警完全不可见。finish 胜出才计数,败给取消竞争的
    // 不重复计入。
    // 停机在前:未到期任务不得在停机排空判定之后再进入队列。
    if inner.phase() != PHASE_ACCEPTING {
        let _ = shared.finish(
            STATE_DELAYED,
            STATE_REJECTED,
            reason_code::SHUTDOWN_BEFORE_EXPIRY,
            || {
                inner.rej_shutdown.fetch_add(1, Relaxed);
            },
        );
        return;
    }
    // 与取消竞争唯一受理权:CAS 失败即已被取消/拒绝,载荷留在护卫内随离场于隔离边界
    // 析构(名额已由终态方归还;业务 future 的析构是外部代码,不得沿定时任务栈裸展开)。
    if !shared.transition(STATE_DELAYED, STATE_QUEUED) {
        return;
    }
    let _producer = ProducerGuard::new(inner.clone());
    // 登记 producer 之后必须复验停机:停机流程可能已在首验与登记之间完成 producer 归零判定,
    // 此时入队会落进已被排空放行、无人再服务的队列,任务永久滞留在 Queued。
    if inner.phase() != PHASE_ACCEPTING {
        let _ = shared.finish(
            STATE_QUEUED,
            STATE_REJECTED,
            reason_code::SHUTDOWN_BEFORE_EXPIRY,
            || {
                inner.rej_shutdown.fetch_add(1, Relaxed);
            },
        );
        return;
    }
    let home = (spread(key_hash) & inner.mask) as u32;
    let lane = match inner.registry.get_or_create(
        home,
        spec.ty.0,
        spec.ordering,
        inner.lane_cap,
        inner.max_lanes.load(Relaxed),
    ) {
        Ok(lane) => lane,
        Err(LaneCreateError::OrderingConflict) => {
            let _ = shared.finish(
                STATE_QUEUED,
                STATE_REJECTED,
                reason_code::ORDERING_CONFLICT_AT_EXPIRY,
                || {
                    inner.rej_ordering.fetch_add(1, Relaxed);
                },
            );
            return;
        }
        Err(LaneCreateError::LimitExceeded) => {
            let _ = shared.finish(
                STATE_QUEUED,
                STATE_REJECTED,
                reason_code::LANE_LIMIT_AT_EXPIRY,
                || {
                    inner.rej_lane_limit.fetch_add(1, Relaxed);
                },
            );
            return;
        }
    };
    if lane.is_failed() {
        let _ = shared.finish(
            STATE_QUEUED,
            STATE_REJECTED,
            reason_code::LANE_FAILED_AT_EXPIRY,
            || {
                inner.rej_lane_failed.fetch_add(1, Relaxed);
            },
        );
        return;
    }
    // 延迟任务到期不等待容量:等待会让"延迟 N 毫秒"退化成无界延迟,拒绝并留证更可预期。
    if !lane.try_reserve_depth() {
        let _ = shared.finish(
            STATE_QUEUED,
            STATE_REJECTED,
            reason_code::QUEUE_FULL_AT_EXPIRY,
            || {
                inner.rej_queue_full.fetch_add(1, Relaxed);
            },
        );
        return;
    }
    let reservation = DepthReservation::new(lane.clone());
    lane.queue.push(EntryPayload {
        shared: shared.clone(),
        job: job.take_for_enqueue(),
    });
    reservation.commit();
    confirm_push(&inner, &lane);
    // submitted 已在登记受理时计入,到期入队不重复计数。
    let owner = lane.owner.load(Acquire);
    let target = if (owner as usize) <= inner.mask {
        owner
    } else {
        home
    };
    inner.notify_of(target).notify_one();
}

/// 业务作用：push 之后的冻结复验——闭合"提交方读到未冻结、冻结方清扫后才入队"的竞争窗口。
/// 没有这一步,该载荷会以 Queued 永久滞留:句柄悬挂、深度与全局许可永不归还、证据缺失,
/// 违反"已受理任务必有结局或明确失败证据"。
///
/// 参数说明：
/// - `inner`: 执行器内核。
/// - `lane`: 刚完成 push 的 lane。
///
/// 返回：无返回值；观察到冻结时由提交方补清扫，任务以 `Failed` 终态留证（提交入口仍返回
/// 已受理的句柄，损耗经句柄与证据可见）。
fn confirm_push(inner: &Arc<Inner>, lane: &Arc<Lane>) {
    // SeqCst fence 与 Lane::fail 里 swap(failed) 之后的 fence 配对:全局序中必有一侧后行,
    // 后行的提交方在此看到 failed 并补清扫,后行的冻结方在排空中看到本次 push 的载荷。
    // 双方都用弱序时可以互相都看不见(store-buffer 交错),载荷静默滞留。
    fence(SeqCst);
    if lane.is_failed() {
        inner.resweep_frozen_lane(lane);
    }
}

// ── 工具 ────────────────────────────────────────────────────────────────────

/// 业务作用：计算 key 的进程内路由 hash，同一执行器内相同 key 恒定落到同一分区；算法不作为
/// 跨进程或跨程序版本的持久化分片协议。
///
/// 参数说明：
/// - `key`: 业务路由 key。
///
/// 返回：64 位 hash。
fn hash_of<K: Hash>(key: K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// 业务作用：返回默认分区数（2 × 可用并行度），未配置时的稳定基线。
///
/// 参数说明: 无。
///
/// 返回：默认分区数。
fn default_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        * 2
}

/// 业务作用：扩散哈希高低位，降低业务 key 分布造成的槽位偏斜。
///
/// 参数说明：
/// - `hash`: 原始哈希值。
///
/// 返回：扩散后的非负槽位基数。
fn spread(hash: u64) -> usize {
    ((hash ^ (hash >> 16)) & 0x7fff_ffff) as usize
}
