//! lane：顺序与准入的唯一一等对象。
//!
//! 每个「原始分区 + 任务类型」对应一条 lane。严格 lane 由 `owner` + `gate` 保证同一时刻至多
//! 一个执行者；非严格 lane 无 owner 无门禁，任何 worker 可并发取任务。窃取只转移 `owner`，
//! 不移动队列数据，FIFO 由队列自身保证。

use std::collections::HashMap;
use std::sync::atomic::{
    fence, AtomicBool, AtomicU32, AtomicU64, AtomicU8,
    Ordering::{AcqRel, Acquire, Relaxed, SeqCst},
};
use std::sync::Arc;
use std::sync::OnceLock;

use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use tokio::sync::Notify;

use crate::entry::{EntryPayload, STATE_FAILED, STATE_QUEUED};

/// 任务类型：严格顺序与统计的索引，由业务定义并保持稳定。
///
/// `u32::MAX` 保留给未分型的兼容提交入口；类型化入口对该值 fail-fast 拒绝
/// （`ReservedTaskType`），公开输入不能改写保留 lane 的顺序要求或借用其上限豁免。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskType(pub u32);

/// 兼容入口（`submit`/`submit_sync`/`submit_async`）使用的保留任务类型。
pub(crate) const LEGACY_TYPE: u32 = u32::MAX;

/// 任务的顺序要求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOrdering {
    /// 在「原始分区 + 任务类型」范围内严格按提交顺序执行。
    Strict,
    /// 允许任务粒度并发与重排；仍保证每个任务至多执行一次。
    Relaxed,
}

/// 提交规格：任务类型 + 顺序要求。同一「分区 + 类型」的顺序要求进入框架后不得改变。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSpec {
    pub(crate) ty: TaskType,
    pub(crate) ordering: TaskOrdering,
}

impl TaskSpec {
    /// 业务作用：声明一个严格保序的任务类型，同分区同类型的任务按提交顺序串行落地。
    ///
    /// 参数说明：
    /// - `ty`: 业务任务类型。
    ///
    /// 返回：严格顺序的提交规格。
    pub fn strict(ty: TaskType) -> Self {
        Self {
            ty,
            ordering: TaskOrdering::Strict,
        }
    }

    /// 业务作用：声明一个允许并发执行的任务类型，放弃顺序换取多 worker 吞吐。
    ///
    /// 参数说明：
    /// - `ty`: 业务任务类型。
    ///
    /// 返回：非严格顺序的提交规格。
    pub fn relaxed(ty: TaskType) -> Self {
        Self {
            ty,
            ordering: TaskOrdering::Relaxed,
        }
    }
}

/// 严格 lane 门禁：空闲。
pub(crate) const GATE_IDLE: u8 = 0;
/// 严格 lane 门禁：有 worker 正在取任务或执行。
pub(crate) const GATE_SERVING: u8 = 1;

/// 单条 lane：一个「原始分区 + 任务类型」的队列与执行权。
pub(crate) struct Lane {
    /// 原始分区号；路由与归还的固定目标。
    pub(crate) home: u32,
    /// 任务类型原始值（`LEGACY_TYPE` 为兼容入口）。
    pub(crate) ty: u32,
    /// 顺序要求；lane 创建后不可变。
    pub(crate) ordering: TaskOrdering,
    /// 载荷队列。物理无界，容量由 `depth` 准入约束。
    pub(crate) queue: SegQueue<EntryPayload>,
    /// 双份额打包字：低 32 位为队列中的真实载荷数（queued），高 32 位为已取得容量但尚未
    /// push 的预留数（reserved）。准入按两者之和封顶；公开排队深度、窃取与排空判定只读
    /// queued——未受理的容量预留没有共享终态、没有载荷、不计受理量，把它冒充排队任务会
    /// 制造幽灵深度并驱动 worker 对空队列高频窃取。
    depth: AtomicU64,
    /// 深度上限。
    pub(crate) cap: u32,
    /// 深度腾出时唤醒容量等待者（`submit_async` 背压路径）。
    pub(crate) space: Notify,
    /// 当前有权服务本 lane 的分区（仅严格 lane 有意义）。
    pub(crate) owner: AtomicU32,
    /// 执行门禁（仅严格 lane 有意义）：pop 与执行整体在门禁内。
    pub(crate) gate: AtomicU8,
    /// 被窃取后的持有起点（单调纳秒），归还节奏据此裁决。
    pub(crate) held_since: AtomicU64,
    /// lane 已冻结：结构性异常后拒绝新提交。
    pub(crate) failed: AtomicBool,
    /// 首次冻结的稳定原因码；提交方补清扫时沿用同一原因，证据口径一致。
    pub(crate) fail_code: OnceLock<u8>,
}

impl Lane {
    /// 业务作用：创建一条空 lane，严格 lane 的初始执行权归属原始分区。
    ///
    /// 参数说明：
    /// - `home`: 原始分区号。
    /// - `ty`: 任务类型原始值。
    /// - `ordering`: 顺序要求。
    /// - `cap`: 深度上限。
    ///
    /// 返回：可立即接受提交的 lane。
    pub(crate) fn new(home: u32, ty: u32, ordering: TaskOrdering, cap: u32) -> Self {
        Self {
            home,
            ty,
            ordering,
            queue: SegQueue::new(),
            depth: AtomicU64::new(0),
            cap,
            space: Notify::new(),
            owner: AtomicU32::new(home),
            gate: AtomicU8::new(GATE_IDLE),
            held_since: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            fail_code: OnceLock::new(),
        }
    }

    /// 业务作用：为一次提交预留 lane 容量（reserved 份额），作为 per-lane 有界背压的准入点。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预留成功返回 true；reserved + queued 已达上限返回 false（调用方据此返回满载
    /// 错误或等待容量）。
    pub(crate) fn try_reserve_depth(&self) -> bool {
        // fetch_update 保证"检查上限 + 占位"原子完成;先读后加会让并发提交突破上限。
        // 上限按 reserved + queued 之和封顶:等待全局许可的预留者仍占容量,后来的提交
        // 不能把它的位置抢走。
        self.depth
            .fetch_update(AcqRel, Acquire, |packed| {
                let reserved = packed >> 32;
                let queued = packed & 0xFFFF_FFFF;
                (reserved + queued < u64::from(self.cap)).then_some(packed + (1_u64 << 32))
            })
            .is_ok()
    }

    /// 业务作用：载荷成功 push 时把一个 reserved 份额原子转为 queued 份额——容量占用总量
    /// 不变，但从"未受理预留"变为"真实排队载荷"，此后对窃取、排空与公开深度可见。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值。
    pub(crate) fn commit_reserved(&self) {
        // 单次原子操作完成转移:reserved - 1、queued + 1,即整字减 (2^32 - 1)。
        self.depth.fetch_sub((1_u64 << 32) - 1, AcqRel);
    }

    /// 业务作用：回滚一个未 push 的 reserved 份额并唤醒一个容量等待者；等待全局许可被取消、
    /// 停机拒绝与 push 前失败路径共用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；容量腾出对 `submit_async` 等待者立即可见。
    pub(crate) fn release_reserved(&self) {
        self.depth.fetch_sub(1_u64 << 32, AcqRel);
        self.notify_space();
    }

    /// 业务作用：唤醒一个容量等待者并把其 Waker 的整个不可信边界隔离——等待者可能是被
    /// 调用方手工 poll 的 `submit_async` future，其 Waker 是外部安全代码，调用与 panic
    /// payload 的析构都可能展开；容量记账已在唤醒前完成，展开不得沿 worker 调用栈传播。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；展开被整体吸收并记录。
    fn notify_space(&self) {
        crate::run_isolated("lane 容量唤醒", || self.space.notify_one());
    }

    /// 业务作用：归还一个 queued 份额并唤醒一个容量等待者；pop 与冻结清扫共用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；容量腾出对 `submit_async` 等待者立即可见。
    pub(crate) fn release_queued(&self) {
        // 载荷 push 与 reserved→queued 转正是两次独立发布,消费方可能在两者之间 pop 到
        // 载荷。此时低位仍为 0,无条件减一会从 reserved 高位借位,把公开 queued 炸成
        // u32::MAX、让"和封顶"准入误判为满、驱动空队列窃取。pop 成功即证明对应的
        // commit_reserved 必然在途:自旋等待转正可见后再 CAS 递减,绝不借位。窗口正常为
        // 数条指令;producer 被 OS 抢占时等待随之延长,但公开状态始终不被污染。
        let mut packed = self.depth.load(Acquire);
        loop {
            if packed & 0xFFFF_FFFF == 0 {
                std::hint::spin_loop();
                packed = self.depth.load(Acquire);
                continue;
            }
            match self
                .depth
                .compare_exchange_weak(packed, packed - 1, AcqRel, Acquire)
            {
                Ok(_) => break,
                Err(current) => packed = current,
            }
        }
        self.notify_space();
    }

    /// 业务作用：读取队列中的真实载荷数，供窃取启发、排空判定与公开排队深度使用；未受理的
    /// 容量预留不计入，避免幽灵排队与空队列窃取。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 queued 快照；仅作提示，最终以 pop 结果为准。
    pub(crate) fn queued_hint(&self) -> u32 {
        (self.depth.load(Relaxed) & 0xFFFF_FFFF) as u32
    }

    /// 业务作用：判断 lane 是否已冻结，冻结后新提交必须被明确拒绝而不是静默滞留。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已冻结返回 true。
    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Acquire)
    }

    /// 业务作用：冻结 lane 并把队列中未执行载荷转为失败证据，阻止后续提交进入黑洞。
    ///
    /// 冻结是单向的：一旦失去安全推进条件（worker 异常死亡等），恢复只能通过重建执行器完成，
    /// 不提供运行期解冻，避免半恢复状态下顺序无法证明。
    ///
    /// 参数说明：
    /// - `code`: 稳定原因码（封闭集合）。
    ///
    /// 返回：本次调用完成首次冻结时返回（证据条目, 已取消丢弃数）；已冻结过返回 None。
    pub(crate) fn fail(&self, code: u8) -> Option<(Vec<Arc<crate::entry::EntryShared>>, u64)> {
        // swap 保证只有一个冻结者执行首次清扫与 failed_lanes 计数,并发冻结不会重复。
        if self.failed.swap(true, AcqRel) {
            return None;
        }
        let _ = self.fail_code.set(code);
        // SeqCst fence 与提交路径 push 之后的 fence 配对,闭合"清扫后入队"窗口:提交方
        // 与冻结方在全局序中必有一方后行——后行的提交方会观察到 failed 并自行补清扫,
        // 后行的冻结方会在下面的排空中观察到已入队载荷。缺少该 fence 时,双方可以各自
        // 只看到对方动作之前的状态(store-buffer 交错),载荷会永久滞留在无人服务的队列。
        fence(SeqCst);
        Some(self.sweep_frozen(code))
    }

    /// 业务作用：排空已冻结 lane 的队列，把未执行载荷转为失败证据；首次冻结与提交方补清扫
    /// 共用，可安全并发调用（队列 pop 原子，终态由每条目的状态 CAS 唯一裁决）。
    ///
    /// 参数说明：
    /// - `code`: 冻结原因码，写入每条证据的终态。
    ///
    /// 返回：（本次清扫出的证据条目, 本次物理丢弃的已取消载荷数）。
    pub(crate) fn sweep_frozen(&self, code: u8) -> (Vec<Arc<crate::entry::EntryShared>>, u64) {
        let mut evidence = Vec::new();
        let mut cancelled = 0u64;
        // 清扫队列:仍排队的载荷发布 FAILED 并保留共享状态作证据;已取消的按取消物理丢弃。
        // 终态发布即归还全局许可;queued 份额逐条归还,保证准入计数与队列长度同步归零。
        // 证据与计数按批在调用方合并,单条 finish 无独立提交;finish 内部已隔离外部
        // Waker panic,清扫不会被等待者代码展开中断。
        while let Some(payload) = self.queue.pop() {
            self.release_queued();
            if payload
                .shared
                .finish(STATE_QUEUED, STATE_FAILED, code, || {})
            {
                evidence.push(payload.shared.clone());
            } else {
                // 非 QUEUED 只可能是已取消(执行中的载荷不在队列里),取消方已发布终态。
                cancelled += 1;
            }
            // 业务 future 的析构是外部代码:清扫可能运行在唯一停机驱动的调用栈上,析构
            // panic 若外泄,驱动死亡后生命周期将永久停在 Stopping。
            crate::run_isolated("冻结清扫载荷析构", move || drop(payload));
        }
        (evidence, cancelled)
    }
}

/// lane 主键：高 32 位分区，低 32 位类型。
///
/// 业务作用：把「分区 + 类型」编码为注册表键，保证同一组合全局只有一条 lane。
///
/// 参数说明：
/// - `home`: 原始分区号。
/// - `ty`: 任务类型原始值。
///
/// 返回：注册表键。
pub(crate) fn lane_key(home: u32, ty: u32) -> u64 {
    (u64::from(home) << 32) | u64::from(ty)
}

/// lane 创建失败的原因，由提交入口翻译为对调用方可见的拒绝。
pub(crate) enum LaneCreateError {
    /// 同一「分区 + 类型」已以不同顺序要求存在。
    OrderingConflict,
    /// lane 总数已达配置上限，禁止再扩展类型基数。
    LimitExceeded,
}

/// lane 注册表：读侧无锁快照，写侧串行创建。
pub(crate) struct LaneRegistry {
    /// 全量 lane：窃取扫描与最终清扫的遍历入口。
    all: ArcSwap<HashMap<u64, Arc<Lane>>>,
    /// 按原始分区分组的 lane 列表：worker 轮转的快速入口。
    by_home: Box<[ArcSwap<Vec<Arc<Lane>>>]>,
    /// lane 创建串行锁：创建是低频路径，串行化换取读侧完全无锁。
    create: std::sync::Mutex<()>,
}

impl LaneRegistry {
    /// 业务作用：创建空注册表，容量按分区数固定。
    ///
    /// 参数说明：
    /// - `partitions`: 分区数。
    ///
    /// 返回：无任何 lane 的注册表。
    pub(crate) fn new(partitions: usize) -> Self {
        let by_home = (0..partitions)
            .map(|_| ArcSwap::from_pointee(Vec::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            all: ArcSwap::from_pointee(HashMap::new()),
            by_home,
            create: std::sync::Mutex::new(()),
        }
    }

    /// 业务作用：查找或创建「分区 + 类型」的 lane，校验顺序要求未被混用，并按配置上限约束
    /// 类型化 lane 总数——`TaskType` 是开放的 u32，若无上限，动态类型可让注册表、指标序列与
    /// 每次创建的全量快照复制无界增长，绕过全局内存预算。
    ///
    /// 兼容入口的保留 lane（`LEGACY_TYPE`，每分区至多一条）豁免上限：兼容 `submit` 承诺
    /// "默认语义与既有实现一致"，不能因 typed 类型先到先得占满额度而失去容量。豁免部分
    /// 天然有界（≤ 分区数），进程内 lane 总数上界为 `max_lanes + 分区数`。
    ///
    /// 参数说明：
    /// - `home`: 原始分区号。
    /// - `ty`: 任务类型原始值。
    /// - `ordering`: 本次提交声明的顺序要求。
    /// - `cap`: 新建 lane 的深度上限。
    /// - `max_lanes`: 类型化 lane 总数上限；只约束新建，已存在的 lane 恒可命中。
    ///
    /// 返回：顺序要求一致且未超上限时返回 lane；混用顺序要求返回 `OrderingConflict`（混用会
    /// 让"严格"承诺静默失效，必须在提交入口 fail-fast），达到上限返回 `LimitExceeded`。
    pub(crate) fn get_or_create(
        &self,
        home: u32,
        ty: u32,
        ordering: TaskOrdering,
        cap: u32,
        max_lanes: usize,
    ) -> Result<Arc<Lane>, LaneCreateError> {
        let key = lane_key(home, ty);
        if let Some(lane) = self.all.load().get(&key) {
            if lane.ordering != ordering {
                return Err(LaneCreateError::OrderingConflict);
            }
            return Ok(lane.clone());
        }
        let guard = self.create.lock().unwrap_or_else(|e| e.into_inner());
        // 持锁复查:两个提交者并发首建同一 lane 时,后进入者必须复用先建的实例,
        // 否则会出现两条同键 lane、顺序保证被拆成两半。
        let snapshot = self.all.load();
        if let Some(lane) = snapshot.get(&key) {
            if lane.ordering != ordering {
                return Err(LaneCreateError::OrderingConflict);
            }
            return Ok(lane.clone());
        }
        // 上限判定在持锁复查之后:并发首建同一键不受上限影响,只有真正扩基数才被拒。
        // 保留 lane 豁免;typed lane 按非保留计数判定,先到先得不侵占兼容入口。
        if ty != LEGACY_TYPE
            && snapshot.values().filter(|l| l.ty != LEGACY_TYPE).count() >= max_lanes
        {
            return Err(LaneCreateError::LimitExceeded);
        }
        let lane = Arc::new(Lane::new(home, ty, ordering, cap));
        let mut next_all = HashMap::clone(&snapshot);
        next_all.insert(key, lane.clone());
        self.all.store(Arc::new(next_all));
        let mut next_home = Vec::clone(&self.by_home[home as usize].load());
        next_home.push(lane.clone());
        self.by_home[home as usize].store(Arc::new(next_home));
        drop(guard);
        Ok(lane)
    }

    /// 业务作用：读取某分区的 lane 列表快照，供 worker 轮转服务。
    ///
    /// 参数说明：
    /// - `home`: 分区号。
    ///
    /// 返回：该分区当前全部 lane 的共享快照。
    pub(crate) fn home_lanes(&self, home: u32) -> Arc<Vec<Arc<Lane>>> {
        self.by_home[home as usize].load_full()
    }

    /// 业务作用：读取全量 lane 快照，供窃取扫描、指标汇总与停机清扫遍历。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 lane 的共享快照。
    pub(crate) fn all_lanes(&self) -> Arc<HashMap<u64, Arc<Lane>>> {
        self.all.load_full()
    }
}

/// 冻结证据：未执行（或执行中被停机中止）任务的可审计残留（不含业务载荷）。
pub(crate) struct FrozenEntry {
    /// 任务共享状态（含终态与原因码）。
    pub(crate) shared: Arc<crate::entry::EntryShared>,
    /// 所属任务类型原始值。
    pub(crate) ty: u32,
    /// 原始分区。
    pub(crate) home: u32,
}

// 内存序约定集中说明:depth 预留/归还用 AcqRel 与 push/pop 建立顺序;owner/gate 的转移语义
// 在 worker 模块的各 CAS 处逐点注释;failed 用 swap(AcqRel) 保证唯一冻结者,并以 SeqCst
// fence 与提交路径的 push 后复验配对(见 fail 内注释),fail_code 的可见性由 OnceLock 自身
// 保证。LEGACY_TYPE(u32::MAX)只会经兼容入口进入本注册表:类型化入口在提交前拒绝该保留
// 值,因此"ty == LEGACY_TYPE 即兼容保留 lane"的豁免判定不会被公开输入冒用。
