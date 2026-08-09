//! worker：每分区一个常驻任务，轮转服务本分区 lane 与已窃取 lane。
//!
//! 顺序保证的唯一来源：严格 lane 的 pop 与执行整体位于门禁内，且 `owner` 的复验发生在取得
//! 门禁之后。窃取只做一次 `owner` CAS、不移动数据，因此不需要与门禁协调——原持有方下一轮取得
//! 门禁后会发现执行权已失去并立即退出服务。

use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::Arc;
use std::time::Duration;

use crate::entry::{
    publish_execution_outcome, reason_code, RunningBackstop, STATE_COMPLETED, STATE_FAILED,
    STATE_QUEUED, STATE_RUNNING,
};
use crate::lane::{Lane, TaskOrdering, GATE_IDLE, GATE_SERVING};
use crate::{Inner, PHASE_ACCEPTING};

/// 空闲 park 上限：既是窃取机会的重扫周期，也是停机信号的兜底观察周期。
const IDLE_PARK: Duration = Duration::from_millis(1);
/// 被窃 lane 的最短持有时长：短于此不归还，抑制空转 ping-pong。
const MIN_HOLD_NANOS: u64 = 1_000_000;
/// 被窃 lane 的最长持有时长：超过必归还，防止执行权长期漂移让归属诊断失真。
const MAX_HOLD_NANOS: u64 = 20_000_000;
/// 单个 worker 同时持有的被窃 lane 上限：防止一个空闲 worker 囤积全部热点执行权。
const MAX_STOLEN: usize = 4;
/// 单轮窃取扫描为严格候选保留的执行权 CAS 尝试上限：非严格 lane 可即时借入，若按
/// "先遇到先受理"早退，大量繁忙非严格 lane 会让严格热点（home worker 被占死、唯一出路
/// 是执行权接管）长期得不到尝试机会；有界预算保证每轮都有严格候选的公平窗口，同时
/// 防止在一长串瞬态竞争失败的严格候选上耗尽空闲周期。
const STRICT_STEAL_ATTEMPTS: usize = 3;
/// 持续有进展时的强制再裁决周期（按服务轮数）：worker 借入持续有载荷的非严格 lane 后
/// 每轮都有进展，永远不会走"无进展才窃取"的路径，后来出现的严格热点将无限饥饿。每隔
/// 本周期强制执行一次窃取裁决（严格候选优先），给公平性一个确定性上界。
const STEAL_RESCAN_ROUNDS: u32 = 8;

/// worker 运行态：本 worker 的私有状态 + 异常死亡兜底。
pub(crate) struct WorkerState {
    pub(crate) inner: Arc<Inner>,
    pub(crate) index: u32,
    /// 已窃取且仍持有的严格 lane。
    stolen: Vec<Arc<Lane>>,
    /// 轮转起点，保证 lane 间公平。
    rr: usize,
    /// 窃取扫描的随机起点种子（确定性 xorshift，不引入随机数依赖）。
    rng: u64,
    /// 距上次窃取裁决的服务轮数；达到 `STEAL_RESCAN_ROUNDS` 即强制再裁决，防止持续有
    /// 进展的借入服务垄断本 worker、让新严格热点饥饿。
    rounds_since_steal: u32,
    /// 正常退出标记：Drop 时区分优雅停机与异常死亡。
    clean: bool,
}

impl WorkerState {
    /// 业务作用：创建 worker 私有状态并挂上异常死亡兜底。
    ///
    /// 参数说明：
    /// - `inner`: 执行器共享内核。
    /// - `index`: 本 worker 服务的分区号。
    ///
    /// 返回：可进入主循环的运行态。
    pub(crate) fn new(inner: Arc<Inner>, index: u32) -> Self {
        Self {
            inner,
            index,
            stolen: Vec::new(),
            rr: 0,
            rng: 0x9E37_79B9_7F4A_7C15 ^ u64::from(index).wrapping_mul(0xA24B_AED4_963E_E407),
            rounds_since_steal: 0,
            clean: false,
        }
    }
}

impl Drop for WorkerState {
    /// 业务作用：worker 异常死亡（循环 panic / 被非停机路径 abort）时冻结其执行权范围内的
    /// lane 并计入健康面板，防止任务静默滞留成黑洞。
    fn drop(&mut self) {
        if self.clean {
            return;
        }
        // 停机路径的 abort 不算异常死亡:残留载荷由停机清扫统一转为冻结证据并计入报告,
        // 这里再冻结会与清扫重复计数。
        if self.inner.phase() != PHASE_ACCEPTING {
            return;
        }
        self.inner.dead_workers.fetch_add(1, AcqRel);
        // 只冻结严格 lane:它们失去唯一执行者后无法再证明顺序,拒收并保留证据比静默滞留安全。
        // 非严格 lane(含代服务借入的)仍可被任意存活 worker 消费,不构成黑洞,冻结属过度失败。
        for lane in self.inner.registry.home_lanes(self.index).iter() {
            if lane.ordering == TaskOrdering::Strict {
                self.inner.freeze_lane(lane, reason_code::WORKER_DIED);
            }
        }
        for lane in &self.stolen {
            if lane.ordering == TaskOrdering::Strict {
                self.inner.freeze_lane(lane, reason_code::WORKER_DIED);
            }
        }
        // 唤醒先于诊断:兜底路径本就处于异常退出,诊断回调再展开也不能耽误其它 worker
        // 观察到冻结与健康计数。
        self.inner.wake_all();
        crate::log_isolated(|| {
            tracing::error!(
                partition = self.index,
                "napart worker 异常退出——本分区 lane 已冻结并拒收新任务"
            );
        });
    }
}

/// 业务作用：worker 主循环——轮转服务、空闲窃取、停机排空，直至排空完成（或收到强制中止
/// 信号）正常退出。
///
/// 参数说明：
/// - `state`: worker 运行态。
///
/// 返回：无返回值；返回即本 worker 已停止消费且不再持有任何被窃执行权，停机流程可以把
/// 「本 worker 已 join」当作其服务范围内不再有并发消费的证明。
pub(crate) async fn run(mut state: WorkerState) {
    loop {
        // 强制中止优先于一切服务:停机收口超时后继续 pop 新任务只会制造更多"刚启动就被
        // 中止"的损耗,必须立即停止消费并交还执行权,让清扫阶段接管残留载荷。
        if state.inner.force_aborting() {
            break;
        }
        let stopping = state.inner.phase() != PHASE_ACCEPTING;

        // 手中被窃 lane 先做一次维护:执行权已被第三方接走或已冻结的,移出轮转。
        // 维护轮转集合:严格 lane 以执行权归属为准(被第三方接管即移出);非严格 lane 只是
        // 临时代服务,排空即移出。
        let index = state.index;
        state.stolen.retain(|lane| {
            !lane.is_failed()
                && match lane.ordering {
                    TaskOrdering::Strict => lane.owner.load(Acquire) == index,
                    TaskOrdering::Relaxed => lane.queued_hint() > 0,
                }
        });

        let home = state.inner.registry.home_lanes(state.index);
        let mut progressed = false;

        // 轮转服务:home lane 与被窃 lane 共用一个公平游标,避免固定顺序饿死尾部 lane。
        let total = home.len() + state.stolen.len();
        if total > 0 {
            state.rr = state.rr.wrapping_add(1);
            for step in 0..total {
                let pick = (state.rr + step) % total;
                let lane = if pick < home.len() {
                    home[pick].clone()
                } else {
                    state.stolen[pick - home.len()].clone()
                };
                progressed |= serve_once(&state.inner, state.index, &lane).await;
                if pick >= home.len() {
                    maybe_release(&state.inner, state.index, &lane, &home);
                }
            }
        }

        if stopping {
            // 停机退出门禁:必须等 producer 收口信号(drain_ok)之后才允许按"已排空"退出,
            // 否则迟到的 push 会落进无人服务的队列,被清扫成冻结证据(本可正常执行)。
            if state.inner.drain_ok.load(Acquire) {
                let stolen_empty = state.stolen.iter().all(|l| l.queued_hint() == 0);
                let home_empty = home.iter().all(|l| l.queued_hint() == 0);
                if stolen_empty && home_empty {
                    break;
                }
            }
            if !progressed {
                // 停机排空期间无新提交,靠短 park 推进而不是紧转空烧 CPU。
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            continue;
        }

        if !progressed {
            state.rounds_since_steal = 0;
            if try_steal(&mut state) {
                continue;
            }
            // 有界 park:唤醒信号覆盖新提交与停机;超时兜底覆盖"victim 积压增长但无人通知"
            // 的窃取机会重扫。
            let _ = tokio::time::timeout(IDLE_PARK, state.inner.notify_of(state.index).notified())
                .await;
        } else {
            // 持续有进展也必须周期性再裁决:借入的非严格 lane 一直有载荷时,本 worker 每轮
            // 都进展、永远不会走上面的空闲窃取路径,后来出现的严格热点(home worker 被占死)
            // 将无人接管。按服务轮数强制重扫给公平性一个确定性上界。
            state.rounds_since_steal += 1;
            if state.rounds_since_steal >= STEAL_RESCAN_ROUNDS {
                state.rounds_since_steal = 0;
                let _ = try_steal(&mut state);
            }
        }
    }
    // 统一退出收口:交还全部被窃执行权,让归属回到原分区,清扫阶段的归属判定不再依赖
    // 本 worker;排空退出与强制中止退出共用同一条路径,不存在带着执行权消失的出口。
    for lane in state.stolen.drain(..) {
        let _ = lane
            .owner
            .compare_exchange(state.index, lane.home, AcqRel, Relaxed);
    }
    state.clean = true;
    crate::log_isolated(|| tracing::debug!(partition = state.index, "napart worker 正常退出"));
}

/// 业务作用：服务一条 lane 一次——严格 lane 在门禁内完成 pop 与执行，非严格 lane 直接并发取。
///
/// 参数说明：
/// - `inner`: 执行器共享内核。
/// - `index`: 当前 worker 分区号。
/// - `lane`: 目标 lane。
///
/// 返回：本轮取得并处置了一个载荷（执行或物理丢弃已取消项）返回 true；无事可做返回 false。
pub(crate) async fn serve_once(inner: &Arc<Inner>, index: u32, lane: &Arc<Lane>) -> bool {
    // 已冻结 lane 不再服务:残留载荷由冻结方清扫,这里继续取会与证据清扫竞争同一批载荷。
    if lane.is_failed() {
        return false;
    }
    // 强制中止已发出时不再取新载荷:pop 出来也只会立即被中止,徒增有损计数;残留载荷
    // 留在队列里由停机清扫统一转为冻结证据。
    if inner.force_aborting() {
        return false;
    }

    let strict = lane.ordering == TaskOrdering::Strict;
    let _gate = if strict {
        // 门禁是顺序保证的全部:pop 与执行必须整体互斥,否则两个 worker 各取一个任务并发执行,
        // 提交顺序即被打破。取不到门禁说明另一 worker 正在服务,本轮直接让出。
        if lane
            .gate
            .compare_exchange(GATE_IDLE, GATE_SERVING, Acquire, Relaxed)
            .is_err()
        {
            return false;
        }
        let guard = GateGuard { lane: lane.clone() };
        // 执行权复验必须在取得门禁之后:窃取方只改 owner 不碰门禁,先验后取门禁会留下
        // "验过之后、取门禁之前被窃取"的窗口,导致失权方继续消费。
        if lane.owner.load(Acquire) != index {
            return false; // guard 归还门禁
        }
        Some(guard)
    } else {
        None
    };

    let Some(payload) = lane.queue.pop() else {
        return false;
    };
    // 载荷离开队列,queued 份额立即归还:队列容量语义是"排队中的任务数",执行中的任务由
    // 全局许可约束。
    lane.release_queued();

    // 取消与执行竞争同一个状态 CAS:这里失败即取消方已胜出,本方只负责物理丢弃载荷。
    // 丢弃作为归账事件段处理:登记、裁决归属、计数一体,与停机切相不出现交错矛盾。
    if !payload.shared.transition(STATE_QUEUED, STATE_RUNNING) {
        {
            let section = inner.outcome_section();
            inner.count_discarded(1, section.stopping);
        }
        // 业务 future 的析构是外部代码:在隔离边界内丢弃载荷,单次析构 panic 不得沿
        // worker 调用栈展开、把一笔取消放大成整条严格 lane 冻结。
        crate::run_isolated("已取消载荷析构", move || drop(payload));
        return true;
    }

    // spawn + await 提供 panic 隔离:业务 panic 只终结这一笔任务,不杀 worker。
    // AbortOnDrop 兜底:worker 内部异常展开或运行时整体拆除时连带取消
    // 业务任务,不留无人认领的后台执行。
    let shared = payload.shared.clone();
    let mut backstop = RunningBackstop {
        shared: shared.clone(),
        armed: true,
    };
    let mut forced = false;
    let mut join = tokio::spawn(payload.job);
    let abort = AbortOnDrop(Some(join.abort_handle()));
    // 等待业务完成的同时监听停机强制中止信号:停机方不 abort worker 本身,而是让 worker
    // 主动中止业务任务并 await 其 JoinHandle——这样"任务确实已退出"有 join 证明,停机
    // 返回后不存在仍在后台推进的业务 future，停机完成边界以实际退出证明为准。
    let outcome = {
        let raced = tokio::select! {
            r = &mut join => Some(r),
            () = inner.force_abort_signal() => None,
        };
        match raced {
            Some(result) => result,
            None => {
                // abort 在任务下一个让出点生效;await 拿到的是任务已析构的确定性证明,
                // 而不是"已发出取消请求"。记录中止事实:后续归因以本控制面裁决为准。
                forced = true;
                join.abort();
                join.await
            }
        }
    };
    drop(abort.disarmed());
    backstop.armed = false;

    // 终态发布与归账整体作为一个事件段:归属在发布前裁决,停机驱动等待切相前已登记的
    // 事件收口——切相前已公开的 Completed 不可能被后置 phase 读取误计为 drained,反向
    // 也不漏计。计数作为终态 CAS 胜者的提交闭包在外部可展开的发布阶段之前执行,
    // await_outcome 等待者的 Waker panic 拆不开终态与账本、也到不了 worker 调用栈。
    // 段内全程同步,不跨 await。
    let section = inner.outcome_section();
    match outcome {
        Ok(()) => {
            publish_execution_outcome(&shared, STATE_COMPLETED, reason_code::NONE, || {
                inner.count_completed(section.stopping);
            });
        }
        Err(join_error) if forced || join_error.is_cancelled() => {
            // 停机强制中止是先行发生的控制面事实:被中止 future 在析构中再度 panic 时,
            // Tokio 会返回 panic 类型的 JoinError,若按错误类型归因会把有损停机改记成
            // 普通任务 panic、报告宣称零损耗。归因以本方是否发出中止裁决,析构异常仅作
            // 附加诊断。
            publish_execution_outcome(
                &shared,
                STATE_FAILED,
                reason_code::ABORTED_DURING_SHUTDOWN,
                || {
                    inner.record_aborted(lane, shared.clone());
                },
            );
            let destructor_panicked = !join_error.is_cancelled();
            crate::log_isolated(|| {
                if destructor_panicked {
                    tracing::error!(
                        partition = index,
                        task_type = lane.ty,
                        "napart 停机强制中止在途任务,其析构另行 panic"
                    );
                } else {
                    tracing::error!(
                        partition = index,
                        task_type = lane.ty,
                        "napart 停机强制中止在途任务"
                    );
                }
            });
        }
        Err(join_error) => {
            // 任务级失败不冻结 lane:panic 只归属于当前业务任务,后续任务照常服务;
            // 冻结整条 lane 会把一笔坏数据放大成整类停摆。
            publish_execution_outcome(&shared, STATE_FAILED, reason_code::TASK_PANICKED, || {
                inner.task_panics.fetch_add(1, Relaxed);
            });
            // 单笔任务失败的归账已在终态提交闭包内完成;诊断经不展开入口执行,日志回调
            // 异常不得把单笔失败放大成 worker 死亡与整条 lane 冻结。
            crate::log_isolated(|| {
                tracing::error!(
                    partition = index,
                    task_type = lane.ty,
                    "napart 任务执行失败: {join_error}"
                );
            });
        }
    }
    // 载荷在此 Drop;全局许可已随终态发布归还,深度已在 pop 时归还。
    true
}

/// 严格 lane 门禁 guard：任何退出路径（含 panic 展开）都归还门禁，否则 lane 永久不可服务。
struct GateGuard {
    lane: Arc<Lane>,
}

impl Drop for GateGuard {
    /// 业务作用：释放执行门禁；Release 语义保证本轮执行的全部效果对下一个取得者可见。
    fn drop(&mut self) {
        self.lane.gate.store(GATE_IDLE, Release);
    }
}

/// 在途任务中止 guard：worker 内部异常展开或运行时整体拆除时连带取消
/// 已 spawn 的业务任务。停机主路径不依赖它——强制中止由 worker 在 select 分支里主动
/// abort 并 await 退出证明，本 guard 只是最后兜底。
struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl AbortOnDrop {
    /// 业务作用：正常完成路径解除中止责任，任务终态已定不再需要兜底。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：解除后的 guard（Drop 不再触发 abort）。
    fn disarmed(mut self) -> Self {
        self.0 = None;
        self
    }
}

impl Drop for AbortOnDrop {
    /// 业务作用：执行栈异常展开时取消在途业务任务，停机后不留后台残余执行。
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

/// 业务作用：空闲 worker 的窃取尝试——先直接代服务外分区的非严格 lane，再对严格 lane 做
/// 执行权 CAS 接管。
///
/// 参数说明：
/// - `state`: worker 运行态。
///
/// 返回：本轮取得了任何进展（代服务成功或接管成功）返回 true。
pub(crate) fn try_steal(state: &mut WorkerState) -> bool {
    let all = state.inner.registry.all_lanes();
    if all.is_empty() {
        return false;
    }
    // 确定性 xorshift 选随机起点:窃取压力均匀落在各 victim,不引入随机数依赖。
    state.rng ^= state.rng << 13;
    state.rng ^= state.rng >> 7;
    state.rng ^= state.rng << 17;
    let lanes: Vec<&Arc<Lane>> = all.values().collect();
    let start = (state.rng as usize) % lanes.len();

    // 严格候选优先于非严格借入裁决:非严格 lane 任何 worker 都能服务,晚一轮无碍;
    // 严格热点的 home worker 被占死时,执行权接管是唯一推进出路。若按"先遇到先受理"
    // 早退,繁忙非严格 lane 会把扫描截断,严格候选长期得不到 CAS 机会。本轮先给严格
    // 候选至多 STRICT_STEAL_ATTEMPTS 次尝试,全部失败或用尽预算后才落回非严格借入。
    // 严格接管上限只统计严格 lane:非严格借入是临时代服务而非执行权转移,若与严格共享
    // 同一上限,借满非严格 lane 的 worker 将永远无法接管任何严格热点。
    let strict_owned = state
        .stolen
        .iter()
        .filter(|l| l.ordering == TaskOrdering::Strict)
        .count();
    let mut strict_attempts = 0usize;
    let mut relaxed_candidate: Option<&Arc<Lane>> = None;
    for i in 0..lanes.len() {
        let lane = lanes[(start + i) % lanes.len()];
        if lane.home == state.index || lane.is_failed() || lane.queued_hint() == 0 {
            continue;
        }
        match lane.ordering {
            TaskOrdering::Relaxed => {
                // 只记首个非严格退路,消费在严格裁决完成后统一进行。
                if relaxed_candidate.is_none() {
                    relaxed_candidate = Some(lane);
                }
            }
            TaskOrdering::Strict => {
                if strict_owned >= MAX_STOLEN || strict_attempts >= STRICT_STEAL_ATTEMPTS {
                    continue;
                }
                strict_attempts += 1;
                // 只窃取执行权仍在原分区手里的 lane:owner 已是第三方时接管链会变成
                // 不可诊断的漂移;等它归还后再窃。CAS 失败不重试同一条,继续扫下一候选。
                state.inner.steal_attempts.fetch_add(1, Relaxed);
                if lane
                    .owner
                    .compare_exchange(lane.home, state.index, AcqRel, Relaxed)
                    .is_ok()
                {
                    state.inner.steal_success.fetch_add(1, Relaxed);
                    lane.held_since.store(now_nanos(), Release);
                    state.stolen.push(lane.clone());
                    return true;
                }
            }
        }
        // 双向都已出结果即停:已有非严格退路,且严格预算用尽(或本轮不可能再接管),
        // 继续扫描没有收益。
        if relaxed_candidate.is_some()
            && (strict_attempts >= STRICT_STEAL_ATTEMPTS || strict_owned >= MAX_STOLEN)
        {
            break;
        }
    }
    if let Some(lane) = relaxed_candidate {
        // 非严格 lane 不需要窃取协议:执行权由任务级状态 CAS 裁决,登记进本 worker 的
        // 临时轮转即时代服务("谁空闲谁消费")。
        return true_if_served_relaxed(state, lane);
    }
    false
}

/// 业务作用：把"发现可代服务的非严格 lane"翻译为一次进展信号；实际消费交由主循环的
/// 轮转路径完成会损失一轮延迟，因此这里直接登记该 lane 进入本 worker 的临时轮转。
///
/// 参数说明：
/// - `state`: worker 运行态。
/// - `lane`: 有积压的外分区非严格 lane。
///
/// 返回：恒为 true（发现即进展；消费在下一轮 serve_once 完成）。
fn true_if_served_relaxed(state: &mut WorkerState, lane: &Arc<Lane>) -> bool {
    // 非严格 lane 无所有权可转移,借用 stolen 轮转位即可;maybe_release 对非严格 lane
    // 的 owner CAS 恒失败,退出条件由 retain(queued==0 时下一轮维护自然移除)承担。
    // 借入上限只统计非严格 lane,不挤占严格接管的配额。
    let borrowed = state
        .stolen
        .iter()
        .filter(|l| l.ordering == TaskOrdering::Relaxed)
        .count();
    if borrowed < MAX_STOLEN && !state.stolen.iter().any(|l| Arc::ptr_eq(l, lane)) {
        state.stolen.push(lane.clone());
    }
    true
}

/// 业务作用：裁决被窃严格 lane 是否归还执行权——排空即还、原籍积压即还、超时必还，
/// 短持有阻尼防止 ping-pong。
///
/// 参数说明：
/// - `inner`: 执行器共享内核。
/// - `index`: 当前持有方分区号。
/// - `lane`: 被窃 lane。
/// - `home`: 当前持有方自己的 home lane 快照（判断原籍是否积压）。
///
/// 返回：无返回值；归还成功计入指标并唤醒原分区 worker。
fn maybe_release(inner: &Arc<Inner>, index: u32, lane: &Arc<Lane>, home: &Arc<Vec<Arc<Lane>>>) {
    if lane.ordering != TaskOrdering::Strict {
        // 非严格 lane 只是临时代服务,排空后自然从轮转移除,无执行权可归还。
        return;
    }
    let held = now_nanos().saturating_sub(lane.held_since.load(Acquire));
    // 阻尼:刚接管就归还会形成窃取-归还空转,吃掉双方的调度周期却不推进任何任务。
    if held < MIN_HOLD_NANOS {
        return;
    }
    let should_release = lane.queued_hint() == 0
        || home.iter().any(|l| l.queued_hint() > 0)
        || held > MAX_HOLD_NANOS;
    if !should_release {
        return;
    }
    // 归还必须以自己为期望值:执行权可能已被冻结路径或第三方处置,期望不符时放弃而不是
    // 强写,否则会把别人的执行权覆盖掉。
    if lane
        .owner
        .compare_exchange(index, lane.home, AcqRel, Relaxed)
        .is_ok()
    {
        inner.releases.fetch_add(1, Relaxed);
        inner.notify_of(lane.home).notify_one();
    }
}

/// 业务作用：读取单调时钟纳秒，供持有时长裁决；墙钟跳变不影响归还节奏。
///
/// 参数说明: 无。
///
/// 返回：进程内单调递增的纳秒计数。
fn now_nanos() -> u64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
