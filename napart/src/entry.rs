//! 任务条目：共享状态、唯一载荷、提交句柄与准入 guard。
//!
//! 条目拆成两半：`EntryShared` 是句柄与执行侧共同持有的原子状态；`EntryPayload` 唯一持有业务
//! future，在队列中以移动语义传递。"同一任务同时出现在两条队列"由所有权系统排除，不需要运行期
//! 一致性复验。
//!
//! 状态与原因码打包在同一个原子字里一次发布：任何读者（轮询或经通知唤醒）观察到终态时，
//! 原因码必然同时可见；竞争败方的 CAS 失败也不可能把自己的原因附到别人的终态上。

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{
    AtomicU32,
    Ordering::{AcqRel, Acquire, Relaxed, Release},
};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OwnedSemaphorePermit};

use crate::lane::Lane;

/// 类型擦除、堆上 Pin 住的业务任务 future。
pub(crate) type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

// ── 任务状态机 ──────────────────────────────────────────────────────────────
// 七态。终态为 COMPLETED / CANCELLED / REJECTED / FAILED,进入终态后不再迁移。
// 执行权与取消权竞争同一个 CAS(QUEUED -> RUNNING vs QUEUED -> CANCELLED),
// 因此至多一方成功,不需要独立的 owner 字段。
// 原子字布局:低 8 位状态,次 8 位原因码;非终态恒为无原因,期望字因此总是 (state, NONE)。

/// 延迟任务已登记定时器，尚未进入任何 lane。
pub(crate) const STATE_DELAYED: u8 = 0;
/// 已入队等待执行。
pub(crate) const STATE_QUEUED: u8 = 1;
/// 已取得执行权，业务 future 正在运行。
pub(crate) const STATE_RUNNING: u8 = 2;
/// 业务 future 正常返回。
pub(crate) const STATE_COMPLETED: u8 = 3;
/// 在开始执行前被取消。
pub(crate) const STATE_CANCELLED: u8 = 4;
/// 未被受理（到期时停机/满载，或延迟登记被停机拒绝）。
pub(crate) const STATE_REJECTED: u8 = 5;
/// 已受理但失去安全推进条件，作为未执行（或未完成）证据保留。
pub(crate) const STATE_FAILED: u8 = 6;
/// 内部结算态：终态结算权已被唯一竞得，框架累计量、登记迁移与许可归还尚未全部完成。
/// 不对外映射——公开读取解码回原状态（原状态编码在原子字第三字节），因此调用方观察到
/// 任何终态时，对应的框架提交与预算归还必然已经完成；结算期间的并发迁移与取消尝试都会
/// 因期望字不匹配而竞争失败。
pub(crate) const STATE_TERMINATING: u8 = 7;

/// 稳定原因码的封闭集合：与终态同字发布，公开面翻译为固定文本。
pub(crate) mod reason_code {
    /// 无原因（非终态与正常完成）。
    pub(crate) const NONE: u8 = 0;
    /// 调用方在执行前取消。
    pub(crate) const CANCELLED_BY_CALLER: u8 = 1;
    /// 业务 future panic。
    pub(crate) const TASK_PANICKED: u8 = 2;
    /// 停机 abort 时任务正在执行，被连带取消。
    pub(crate) const ABORTED_DURING_SHUTDOWN: u8 = 3;
    /// 延迟任务到期前执行器已停机。
    pub(crate) const SHUTDOWN_BEFORE_EXPIRY: u8 = 4;
    /// 延迟任务到期时 lane 深度已满。
    pub(crate) const QUEUE_FULL_AT_EXPIRY: u8 = 5;
    /// 延迟任务到期时顺序要求与既有 lane 冲突。
    pub(crate) const ORDERING_CONFLICT_AT_EXPIRY: u8 = 6;
    /// 延迟任务到期时目标 lane 已冻结。
    pub(crate) const LANE_FAILED_AT_EXPIRY: u8 = 7;
    /// worker 异常死亡，严格 lane 冻结。
    pub(crate) const WORKER_DIED: u8 = 8;
    /// 停机收口超时，残留任务冻结为证据。
    pub(crate) const SHUTDOWN_FROZEN: u8 = 9;
    /// 延迟任务到期时 lane 总数已达配置上限。
    pub(crate) const LANE_LIMIT_AT_EXPIRY: u8 = 10;
}

/// 业务作用：把封闭原因码翻译为稳定文本，供句柄与证据导出使用；文本集合即公开合同。
///
/// 参数说明：
/// - `code`: 原因码。
///
/// 返回：有原因时返回稳定文本；`NONE` 或未知值返回 None（未知值表示内部不变量已失效，
/// 宁可不提供原因也不返回错误文本）。
pub(crate) fn reason_text(code: u8) -> Option<&'static str> {
    match code {
        reason_code::CANCELLED_BY_CALLER => Some("cancelled_by_caller"),
        reason_code::TASK_PANICKED => Some("task_panicked"),
        reason_code::ABORTED_DURING_SHUTDOWN => Some("aborted_during_shutdown"),
        reason_code::SHUTDOWN_BEFORE_EXPIRY => Some("shutdown_before_expiry"),
        reason_code::QUEUE_FULL_AT_EXPIRY => Some("queue_full_at_expiry"),
        reason_code::ORDERING_CONFLICT_AT_EXPIRY => Some("ordering_conflict_at_expiry"),
        reason_code::LANE_FAILED_AT_EXPIRY => Some("lane_failed_at_expiry"),
        reason_code::WORKER_DIED => Some("worker_died"),
        reason_code::SHUTDOWN_FROZEN => Some("shutdown_frozen"),
        reason_code::LANE_LIMIT_AT_EXPIRY => Some("lane_limit_at_expiry"),
        _ => None,
    }
}

/// 提交后任务的生命周期状态。
///
/// `Failed` 是与 `Rejected` 不同的第四种结局：任务已被受理，但所在 lane 失去安全推进条件
/// （worker 异常死亡、停机超时强制冻结、任务 panic 等），条目作为证据保留、业务闭包不再执行
/// 或不再重试。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskStatus {
    /// 延迟任务已登记，到期后才进入路由。
    Delayed,
    /// 已入队，等待所属 lane 的执行者取出。
    Queued,
    /// 业务 future 正在执行。
    Running,
    /// 执行完成。
    Completed,
    /// 在执行开始前被调用方取消。
    Cancelled,
    /// 未被受理；`reason` 给出稳定原因码。
    Rejected,
    /// 已受理但被冻结为失败证据；`reason` 给出稳定原因码。
    Failed,
}

/// 业务作用：把内部状态字节翻译为公开状态枚举，供句柄查询与运维判断任务是否仍可能执行。
///
/// 参数说明：
/// - `raw`: 内部状态字节。
///
/// 返回：对应的公开枚举；内部新增状态未同步映射时按保守的 `Failed` 处理，不让调用方误判任务
/// 仍会执行。
pub(crate) fn status_of(raw: u8) -> TaskStatus {
    match raw {
        STATE_DELAYED => TaskStatus::Delayed,
        STATE_QUEUED => TaskStatus::Queued,
        STATE_RUNNING => TaskStatus::Running,
        STATE_COMPLETED => TaskStatus::Completed,
        STATE_CANCELLED => TaskStatus::Cancelled,
        STATE_REJECTED => TaskStatus::Rejected,
        _ => TaskStatus::Failed,
    }
}

/// 业务作用：把状态与原因码编码为单个原子字，使二者只能一起发布、一起被观察。
///
/// 参数说明：
/// - `state`: 状态字节。
/// - `code`: 原因码。
///
/// 返回：编码后的字。
#[inline]
fn word(state: u8, code: u8) -> u32 {
    u32::from(state) | (u32::from(code) << 8)
}

/// 业务作用：编码结算态字——第三字节保留原状态，公开读取据此解码回结算前的可见状态。
///
/// 参数说明：
/// - `origin`: 结算前的状态字节。
/// - `code`: 结算完成后将随终态发布的原因码。
///
/// 返回：编码后的结算字。
#[inline]
fn settling_word(origin: u8, code: u8) -> u32 {
    u32::from(STATE_TERMINATING) | (u32::from(code) << 8) | (u32::from(origin) << 16)
}

/// 业务作用：解码原子字为对外可见的（状态, 原因码）——结算态映射回原状态且无原因，
/// 保证"观察到终态即框架提交与预算归还已完成"的公开合同。
///
/// 参数说明：
/// - `w`: 原子字。
///
/// 返回：（对外状态字节, 原因码）。
#[inline]
fn decode(w: u32) -> (u8, u8) {
    let state = (w & 0xFF) as u8;
    if state == STATE_TERMINATING {
        (((w >> 16) & 0xFF) as u8, reason_code::NONE)
    } else {
        (state, ((w >> 8) & 0xFF) as u8)
    }
}

/// 句柄与执行侧共同持有的任务共享状态。
pub(crate) struct EntryShared {
    /// 状态 + 原因码的打包原子字；全部迁移都是对整字的 CAS。
    word: AtomicU32,
    /// 终态唤醒点：`Submission::await_outcome` 在此等待。
    pub(crate) done: Notify,
    /// 全局在飞许可。挂在共享状态而不是队列载荷上，是为了让"发布终态"与"归还全局预算"
    /// 由同一个 CAS 裁决：取消成功的任务即使载荷仍滞留队列，也立即腾出全局名额，不会被
    /// 前面的长任务拖成整个执行器的预算黑洞（不变量 B2：全局许可在到达终态时归还）。
    admit: Mutex<Option<OwnedSemaphorePermit>>,
}

impl EntryShared {
    /// 业务作用：以指定初始状态创建任务共享状态，作为句柄查询、取消与执行竞争的唯一权威，
    /// 并托管该任务的全局在飞许可直至终态。
    ///
    /// 参数说明：
    /// - `initial`: 初始状态（立即提交为 QUEUED，延迟提交为 DELAYED）。
    /// - `permit`: 提交准入时取得的全局许可，终态发布时归还。
    ///
    /// 返回：无原因码、无等待者的新共享状态。
    pub(crate) fn new(initial: u8, permit: OwnedSemaphorePermit) -> Arc<Self> {
        Arc::new(Self {
            word: AtomicU32::new(word(initial, reason_code::NONE)),
            done: Notify::new(),
            admit: Mutex::new(Some(permit)),
        })
    }

    /// 业务作用：归还全局在飞许可；只有首次调用真正归还，之后为无害幂等。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；许可在锁外 Drop，避免持锁唤醒全局预算等待者。
    fn release_admit(&self) {
        let permit = self.admit.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(permit);
    }

    /// 业务作用：非终态之间的原子迁移（取得执行权、延迟到期受理），失败即竞争败北。
    ///
    /// 参数说明：
    /// - `from`: 期望的当前状态（非终态恒无原因，期望字确定）。
    /// - `to`: 目标状态。
    ///
    /// 返回：CAS 成功返回 true；当前状态已被其它路径改变返回 false。
    pub(crate) fn transition(&self, from: u8, to: u8) -> bool {
        self.word
            .compare_exchange(
                word(from, reason_code::NONE),
                word(to, reason_code::NONE),
                AcqRel,
                Acquire,
            )
            .is_ok()
    }

    /// 业务作用：以两阶段结算发布终态。唯一胜者先把状态字 CAS 到内部结算态（对外仍解码为
    /// 原状态），完成框架提交与全局许可归还后，才以 Release 发布最终状态并唤醒等待者。
    /// 因此任何读者——无论轮询还是经通知唤醒——观察到终态时，累计量、登记迁移与预算归还
    /// 必然已经完成；状态与原因码同字发布，不存在"无原因终态"窗口，竞争败方也不可能把
    /// 自己的原因附到别人的终态上。
    ///
    /// 参数说明：
    /// - `expected`: 期望的当前状态。
    /// - `terminal`: 要发布的终态。
    /// - `code`: 稳定原因码；正常完成用 `NONE`。
    /// - `commit`: 结算权胜者的框架提交动作；仅胜者恰好执行一次，先于终态对外可见。
    ///
    /// 返回：取得结算权并完成发布返回 true；当前状态已被其它路径改变返回 false
    /// （`commit` 不执行）。
    pub(crate) fn finish(
        &self,
        expected: u8,
        terminal: u8,
        code: u8,
        commit: impl FnOnce(),
    ) -> bool {
        // 结算权竞争:胜者独占从原状态到终态的整个结算窗口,并发的执行/取消/到期迁移都
        // 因期望字不匹配而败北;窗口内对外可见状态仍是原状态,调用方不会把结算中的任务
        // 当成稳定终态提前行动。
        if self
            .word
            .compare_exchange(
                word(expected, reason_code::NONE),
                settling_word(expected, code),
                AcqRel,
                Acquire,
            )
            .is_err()
        {
            return false;
        }
        // 结算权一旦取得,终态发布就不允许被任何异常跳过:guard 的 Drop 负责发布与通知,
        // 即使结算窗口内的某一步意外展开,任务也不会永久滞留在对外映射为原状态的结算态
        // (那将是一笔无载荷、无证据、永无终态的已受理任务)。
        let publication = SettlementPublication {
            shared: self,
            value: word(terminal, code),
        };
        commit();
        // 许可归还会同步调用预算等待者的 Waker——外部安全代码,可以 panic,其 panic
        // payload 的析构与异常日志同样可以 panic;整体隔离在结算窗口内,不改变发布顺序。
        crate::run_isolated("终态结算的许可归还", || self.release_admit());
        // 框架提交与预算归还全部完成后才发布终态:观察到终态即观察到一致账本。
        drop(publication);
        true
    }

    /// 业务作用：读取对外可见的状态字节，供状态查询与竞争前的快速短路；结算态解码回原
    /// 状态，终态可见即代表结算已完成。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：以 Acquire 语义读取并解码的状态字节。
    pub(crate) fn load(&self) -> u8 {
        decode(self.word.load(Acquire)).0
    }

    /// 业务作用：一次读取对外可见状态与原因码的一致快照，供句柄与证据导出使用；结算态
    /// 解码回原状态且无原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：（状态字节, 原因码）。
    pub(crate) fn snapshot(&self) -> (u8, u8) {
        decode(self.word.load(Acquire))
    }
}

/// 结算发布 guard：持有已竞得的终态结算权，Drop 时发布最终状态并唤醒等待者。
///
/// 发布走 Drop 而不是顺序语句，是为了让"取得结算权 ⇒ 终态必然发布"在异常展开下也成立：
/// 结算窗口内任何一步意外展开时，栈展开仍会执行本 guard，任务不会永久滞留在结算态。
struct SettlementPublication<'a> {
    shared: &'a EntryShared,
    value: u32,
}

impl Drop for SettlementPublication<'_> {
    /// 业务作用：发布最终状态并唤醒 `await_outcome` 等待者；通知经不展开的隔离边界执行，
    /// 等待者的 Waker 异常不会阻断发布链或向外传播。
    fn drop(&mut self) {
        self.shared.word.store(self.value, Release);
        crate::run_isolated("终态通知", || self.shared.done.notify_waiters());
    }
}

/// lane 容量预留 guard：push 成功前的中途失败（含等待全局许可时被取消）自动回滚预留。
///
/// 预留占用的是 reserved 份额——只参与容量封顶，不冒充队列载荷：公开排队深度、窃取与
/// 排空判定都不统计它。push 成功后经 `commit` 原子转为 queued 份额（此后由 pop 侧归还）；
/// push 前任何失败路径由本 guard 的 Drop 回滚，保证容量不会因半完成提交而永久假满。
pub(crate) struct DepthReservation {
    lane: Arc<Lane>,
    armed: bool,
}

impl DepthReservation {
    /// 业务作用：记录一次已成功的容量预留，在 push 完成前保护性持有回滚责任。
    ///
    /// 参数说明：
    /// - `lane`: 已完成 reserved 份额预留的 lane。
    ///
    /// 返回：armed 状态的预留 guard；不 `commit` 则 Drop 时回滚预留。
    pub(crate) fn new(lane: Arc<Lane>) -> Self {
        Self { lane, armed: true }
    }

    /// 业务作用：载荷已成功进入队列，把 reserved 份额原子转为 queued 份额（容量总占用
    /// 不变，此后由 pop 侧归还），并解除本 guard 的回滚责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；调用后 Drop 不再回滚。
    pub(crate) fn commit(mut self) {
        self.lane.commit_reserved();
        self.armed = false;
    }
}

impl Drop for DepthReservation {
    /// 业务作用：push 前的失败路径回滚容量预留并唤醒容量等待者，防止预留因取消/停机滞留
    /// 造成永久假满。
    fn drop(&mut self) {
        if self.armed {
            self.lane.release_reserved();
        }
    }
}

/// 队列中的唯一载荷：业务 future 与其共享状态引用。
///
/// 全局许可不在载荷上：它托管在 `EntryShared` 里、随终态发布归还。载荷被物理丢弃
/// （已取消项被 pop、冻结清扫）时终态必然已发布或正由丢弃方发布，不存在许可泄漏路径。
pub(crate) struct EntryPayload {
    /// 与句柄共享的状态权威。
    pub(crate) shared: Arc<EntryShared>,
    /// 业务 future；执行时被取出。
    pub(crate) job: Job,
}

/// 分区任务的稳定提交句柄。
///
/// 句柄只读共享状态，不持有业务 future；Drop 句柄不影响任务执行（fire-and-forget 等价于
/// 提交后立即丢弃句柄）。
pub struct Submission {
    pub(crate) shared: Arc<EntryShared>,
    /// 延迟任务的定时器中止句柄；立即提交为 None。
    pub(crate) timer: Option<tokio::task::AbortHandle>,
    /// 执行器内核弱引用：取消成功时计入公开取消指标（含延迟登记摘除）。Weak 不延长执行器
    /// 生命周期；执行器已 Drop 时取消仍正确生效，只是不再计数。
    pub(crate) inner: std::sync::Weak<crate::Inner>,
    /// 延迟登记序号：取消成功时立即清理登记表，防止被取消的延迟任务滞留成长期泄漏。
    pub(crate) delayed_id: Option<u64>,
}

impl Submission {
    /// 业务作用：读取任务当前生命周期状态，供调用方判断任务是否仍可能执行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前状态快照；返回后状态仍可能被并发推进，终态除外。
    pub fn status(&self) -> TaskStatus {
        status_of(self.shared.load())
    }

    /// 业务作用：在任务开始执行前竞争取消权，成功者负责发布终态并唤醒等待者。
    ///
    /// 取消不从队列摘除载荷：载荷仍留在原位，由消费者取出后按已取消状态物理丢弃。因此取消成功
    /// 后 lane 深度不会立即腾出，"取消一批后立即重提"仍可能 `QueueFull`，这是有意的设计；
    /// 但全局在飞许可随终态立即归还，不会因取消项滞留队列而封死其它 lane 的准入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次调用把未执行任务成功取消返回 true；任务已运行、已终态或已被其它方取消返回
    /// false。
    pub fn cancel(&self) -> bool {
        // 稳定终态与运行态不可能再赢得取消权:先做无副作用快速返回,不登记归账事件——
        // 持有终态句柄的调用方高频重试取消时,若每次都登记事件,停机的动态收口门禁会被
        // 持续到来的空事件饿死,外部一个句柄就能拖延 STOPPED。结算窗口(对外映射为原
        // 状态)仍会进入下方竞争并以 CAS 失败告终,其事件段生命周期只有纳秒级。
        if !matches!(self.shared.load(), STATE_QUEUED | STATE_DELAYED) {
            return false;
        }
        // 整个取消尝试作为一段终态归账事件:在竞争终态 CAS 之前登记事件,CAS 胜出后的
        // 终态发布(含全局许可归还,可能同步唤醒预算等待者)、登记摘除与累计量提交都在
        // 事件段内完成。停机驱动在发布 STOPPED 前等待在途事件收口,因此不存在"句柄已
        // 公开 Cancelled、执行器已 STOPPED,公开 cancelled 累计量事后才改写"的终局漂移。
        // CAS 之前登记而不是胜出后登记:终态 CAS 与事件登记之间的窗口同样可被抢占卡开。
        let inner = self.inner.upgrade();
        let _section = inner.as_deref().map(crate::Inner::outcome_section);
        // 先竞争 QUEUED:立即提交的主路径。执行侧的 QUEUED -> RUNNING 与这里的
        // QUEUED -> CANCELLED 竞争同一原子字,至多一方成功,不存在"取消成功但仍被执行"。
        // 累计量作为终态 CAS 胜者的提交动作,在许可归还/终态通知(可运行外部 Waker)之前
        // 完成——外部 panic 拆不开"公开 Cancelled"与"cancelled 累计"。
        if self.shared.finish(
            STATE_QUEUED,
            STATE_CANCELLED,
            reason_code::CANCELLED_BY_CALLER,
            || {
                if let Some(inner) = &inner {
                    inner.cancelled_total.fetch_add(1, Relaxed);
                }
            },
        ) {
            return true;
        }
        // 再竞争 DELAYED:延迟任务在到期前取消。到期回调的 DELAYED -> QUEUED 与这里竞争,
        // 败方(到期回调)会观察到已取消并直接丢弃,不会二次入队。定时器中止、垂死收容所
        // 移交(清登记表防僵尸、留 JoinHandle 给停机 join 退出证明)与累计量同为胜者的
        // 提交动作,先于外部可展开的发布阶段。
        if self.shared.finish(
            STATE_DELAYED,
            STATE_CANCELLED,
            reason_code::CANCELLED_BY_CALLER,
            || {
                if let Some(timer) = &self.timer {
                    timer.abort();
                }
                if let Some(inner) = &inner {
                    if let Some(id) = self.delayed_id {
                        inner.retire_delayed(id);
                    }
                    inner.cancelled_total.fetch_add(1, Relaxed);
                }
            },
        ) {
            return true;
        }
        false
    }

    /// 业务作用：返回拒绝、失败或取消的稳定原因码，帮助调用方区分停机、满载、策略冲突与冻结。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已进入带原因终态时返回稳定原因码；运行中或正常完成返回 None。原因码与状态同字
    /// 发布，观察到终态即保证原因可见。
    pub fn reason(&self) -> Option<&'static str> {
        let (_, code) = self.shared.snapshot();
        reason_text(code)
    }

    /// 业务作用：等待任务进入终态，免去调用方轮询状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务的终态（Completed / Cancelled / Rejected / Failed）；若等待期间执行器被停机
    /// 强制冻结，返回 Failed。
    pub async fn await_outcome(&self) -> TaskStatus {
        loop {
            let status = status_of(self.shared.load());
            if matches!(
                status,
                TaskStatus::Completed
                    | TaskStatus::Cancelled
                    | TaskStatus::Rejected
                    | TaskStatus::Failed
            ) {
                return status;
            }
            // 先登记等待再复查状态,避免"复查后、await 前"窗口内的终态通知丢失。
            let notified = crate::await_shielded(self.shared.done.notified());
            let status = status_of(self.shared.load());
            if matches!(
                status,
                TaskStatus::Completed
                    | TaskStatus::Cancelled
                    | TaskStatus::Rejected
                    | TaskStatus::Failed
            ) {
                return status;
            }
            notified.await;
        }
    }
}

/// 业务作用：把执行结果发布为终态并唤醒等待者；执行侧专用。
///
/// 参数说明：
/// - `shared`: 任务共享状态。
/// - `terminal`: 要发布的终态（COMPLETED 或 FAILED）。
/// - `code`: 失败时的稳定原因码；正常完成用 `NONE`。
/// - `commit`: 终态胜者的框架提交动作（计数等），先于外部可展开的发布阶段执行。
///
/// 返回：无返回值；当前状态不是 RUNNING 时不覆盖（说明已被停机中止路径先行发布，
/// `commit` 不执行）。
pub(crate) fn publish_execution_outcome(
    shared: &EntryShared,
    terminal: u8,
    code: u8,
    commit: impl FnOnce(),
) {
    let _ = shared.finish(STATE_RUNNING, terminal, code, commit);
}

/// 业务作用：为"执行中被停机 abort"的路径兜底发布失败终态，保证等待者不会永久挂起。
///
/// worker 在等待业务 future 期间可能被停机超时 abort，此时任务既不会完成也不会有人发布终态；
/// 本 guard 随执行栈展开触发，把 RUNNING 收敛为 FAILED 并唤醒等待者。
pub(crate) struct RunningBackstop {
    pub(crate) shared: Arc<EntryShared>,
    pub(crate) armed: bool,
}

impl Drop for RunningBackstop {
    /// 业务作用：执行路径异常展开时把 RUNNING 收敛为失败证据，防止句柄等待者永久挂起。
    fn drop(&mut self) {
        if self.armed {
            // 兜底路径无独立计数;finish 把 Waker 调用与其 panic payload 的析构整体隔离在
            // 发布边界内,本 Drop 即使已处于展开路径也不会叠加成双重展开。
            let _ = self.shared.finish(
                STATE_RUNNING,
                STATE_FAILED,
                reason_code::ABORTED_DURING_SHUTDOWN,
                || {},
            );
        }
    }
}

impl std::fmt::Debug for Submission {
    /// 业务作用：以状态与原因码渲染句柄，供日志与断言使用；不输出业务载荷。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Submission")
            .field("status", &self.status())
            .field("reason", &self.reason())
            .finish()
    }
}
