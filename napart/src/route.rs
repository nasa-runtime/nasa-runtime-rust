//! 任务类型状态与严格路由权威。
//!
//! `TypeState` 只表达固定 `(home, TaskType)` 的业务顺序、资源计数和当前路由，不拥有
//! `PartitionSlot`。物理队列始终由 slot 或盗洞持有，避免形成 slot 与类型状态之间的强引用环。

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, Semaphore};

use crate::tunnel::{NonStrictTunnel, StrictTunnel};

/// 任务类型：严格顺序、路由和低基数指标的稳定索引。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskType(pub u32);

/// standalone 兼容入口独占的保留任务类型。
pub(crate) const LEGACY_TYPE: TaskType = TaskType(u32::MAX);

/// 任务顺序要求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOrdering {
    /// 在同一 Runner、原始分区和任务类型内严格按受理序号执行。
    Strict,
    /// 允许多个 worker 并发执行和重排，但每笔任务仍至多执行一次。
    Relaxed,
}

/// 提交规格：稳定任务类型与不可变顺序要求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSpec {
    pub(crate) ty: TaskType,
    pub(crate) ordering: TaskOrdering,
}

impl TaskSpec {
    /// 业务作用：声明一个严格保序任务类型。
    ///
    /// 参数说明：
    /// - `ty`: 当前 Runner 内稳定、低基数的任务类型。
    ///
    /// 返回：严格顺序提交规格。
    pub fn strict(ty: TaskType) -> Self {
        Self {
            ty,
            ordering: TaskOrdering::Strict,
        }
    }

    /// 业务作用：声明一个允许并发执行的任务类型。
    ///
    /// 参数说明：
    /// - `ty`: 当前 Runner 内稳定、低基数的任务类型。
    ///
    /// 返回：非严格顺序提交规格。
    pub fn relaxed(ty: TaskType) -> Self {
        Self {
            ty,
            ordering: TaskOrdering::Relaxed,
        }
    }
}

/// 严格类型的现役路由阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrictRouteState {
    /// 新旧任务都进入原始 slot。
    Local,
    /// 源 worker 正在封存主队列存量并建立唯一盗洞。
    Migrating,
    /// 新任务直接进入盗洞，源存量进入 stock FIFO。
    Stolen,
    /// 新任务先进入 staging，等待旧直投 producer 边界收口。
    ReturnPrepare,
    /// 盗洞内容正在回写原始 slot，不能撤销归还。
    Returning,
    /// 原始 slot 正在按归还边界追赶暂存任务。
    LocalCatchup,
    /// 可逆归还条件消失，暂存任务正在恢复到盗洞。
    StolenCatchup,
    /// 当前类型已失去顺序或物理所有权证明。
    Failed,
}

/// 严格类型不可变路由快照。
#[derive(Clone)]
pub(crate) struct StrictRoute {
    /// 路由代次；每次状态变化严格递增。
    pub(crate) epoch: u64,
    /// 当前迁移或归还阶段。
    pub(crate) state: StrictRouteState,
    /// 现役唯一盗洞；Local 与 Failed 不携带盗洞。
    pub(crate) tunnel: Option<Arc<StrictTunnel>>,
}

impl fmt::Debug for StrictRoute {
    /// 业务作用：输出路由代次、阶段和盗洞身份，不遍历任务队列或业务载荷。
    ///
    /// 参数说明：
    /// - `f`: 接收脱敏路由字段的格式化器。
    ///
    /// 返回：写入成功返回 Ok；底层写入失败返回格式错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StrictRoute")
            .field("epoch", &self.epoch)
            .field("state", &self.state)
            .field("tunnel", &self.tunnel.as_ref().map(|value| value.id()))
            .finish()
    }
}

/// 非严格类型的活动盗洞快照。
#[derive(Clone, Default)]
pub(crate) struct NonStrictTunnelGroup {
    tunnels: Arc<[Arc<NonStrictTunnel>]>,
    cursor: Arc<AtomicUsize>,
}

impl NonStrictTunnelGroup {
    /// 业务作用：创建空活动快照，非严格任务在没有盗洞时仍进入原始 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含目标的快照。
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// 业务作用：从现有快照增加一个不同目标盗洞，保持旧 producer 可安全使用旧 Arc 快照。
    ///
    /// 参数说明：
    /// - `tunnel`: 已在目标 slot 登记的活动盗洞。
    ///
    /// 返回：目标尚不存在时返回新快照；重复目标返回 None。
    pub(crate) fn with_tunnel(&self, tunnel: Arc<NonStrictTunnel>) -> Option<Self> {
        if self
            .tunnels
            .iter()
            .any(|current| current.target() == tunnel.target())
        {
            return None;
        }
        let mut tunnels = self.tunnels.to_vec();
        tunnels.push(tunnel);
        Some(Self {
            tunnels: tunnels.into(),
            cursor: self.cursor.clone(),
        })
    }

    /// 业务作用：轮转选择仍可接纳的盗洞，使源 slot 保留本地份额且多个目标都有直投机会。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在活动目标时返回其中一个盗洞；全部关闭或当前轮保留本地份额时返回 None。
    pub(crate) fn select(&self) -> Option<Arc<NonStrictTunnel>> {
        if self.tunnels.is_empty() {
            return None;
        }
        let turn = self.cursor.fetch_add(1, Ordering::Relaxed);
        // 多一个本地虚拟目标，避免热点全部搬离后源 worker 永久失去执行份额。
        let pick = turn % (self.tunnels.len() + 1);
        if pick == self.tunnels.len() {
            return None;
        }
        self.tunnels
            .get(pick)
            .filter(|tunnel| tunnel.accepting())
            .cloned()
    }

    /// 业务作用：读取活动盗洞列表快照，供 observer 收租和停止路径关闭目标。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时冻结的共享列表。
    pub(crate) fn tunnels(&self) -> Arc<[Arc<NonStrictTunnel>]> {
        self.tunnels.clone()
    }

    /// 业务作用：从新快照摘除已关闭盗洞，旧 producer 持有的 Arc 仍可完成既有发布责任。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已取得排空证明的盗洞标识。
    ///
    /// 返回：找到并摘除时返回新快照；不存在返回 None。
    pub(crate) fn without_tunnel(&self, tunnel_id: u64) -> Option<Self> {
        if !self.tunnels.iter().any(|tunnel| tunnel.id() == tunnel_id) {
            return None;
        }
        let tunnels = self
            .tunnels
            .iter()
            .filter(|tunnel| tunnel.id() != tunnel_id)
            .cloned()
            .collect::<Vec<_>>();
        Some(Self {
            tunnels: tunnels.into(),
            cursor: self.cursor.clone(),
        })
    }
}

struct StrictOrder {
    next_issue: AtomicU64,
    state: Mutex<StrictOrderState>,
    changed: Notify,
}

struct StrictOrderState {
    next_run: u64,
    settled_ahead: BTreeSet<u64>,
}

impl StrictOrder {
    /// 业务作用：创建从零开始的严格受理序号域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未签发或结算序号的门禁。
    fn new() -> Self {
        Self {
            next_issue: AtomicU64::new(0),
            state: Mutex::new(StrictOrderState {
                next_run: 0,
                settled_ahead: BTreeSet::new(),
            }),
            changed: Notify::new(),
        }
    }

    /// 业务作用：签发不可复用的严格受理序号，作为跨主队列和盗洞的最终 FIFO 权威。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回下一序号；耗尽时返回 None，当前类型必须拒收。
    fn issue(&self) -> Option<u64> {
        self.next_issue
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .ok()
    }

    /// 业务作用：等待本序号成为严格执行队头；取消或失败的前序通过 `settle` 连续推进。
    ///
    /// 参数说明：
    /// - `sequence`: 提交时冻结的严格序号。
    ///
    /// 返回：轮到本序号时返回；若调用方任务已经终态，调用方应先结算再退出。
    async fn wait_turn(&self, sequence: u64) {
        loop {
            let notified = self.changed.notified();
            if self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .next_run
                >= sequence
            {
                return;
            }
            notified.await;
        }
    }

    /// 业务作用：同步判断严格序号是否已经取得执行资格，使 slot worker 能把尚未轮到的
    /// Handling 任务留在私有 deferred 槽，不占用业务执行许可等待前序。
    ///
    /// 参数说明：
    /// - `sequence`: 提交时冻结的严格序号。
    ///
    /// 返回：前序已经全部结算时返回 true。
    fn turn_ready(&self, sequence: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .next_run
            >= sequence
    }

    /// 业务作用：登记某个严格序号已经进入终态，并只跨过连续前缀，禁止后继越过未决任务。
    ///
    /// 参数说明：
    /// - `sequence`: 已完成、取消、拒绝或失败的序号。
    ///
    /// 返回：本次登记推进的连续序号数量；重复或旧序号返回零。
    fn settle(&self, sequence: u64) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if sequence < state.next_run || !state.settled_ahead.insert(sequence) {
            return 0;
        }
        let mut advanced = 0;
        loop {
            let next = state.next_run;
            if !state.settled_ahead.remove(&next) {
                break;
            }
            state.next_run += 1;
            advanced += 1;
        }
        drop(state);
        if advanced > 0 {
            self.changed.notify_waiters();
        }
        advanced
    }
}

/// 固定 `(home, TaskType)` 的顺序、容量与路由状态。
pub(crate) struct TypeState {
    runner_id: u64,
    generation: u64,
    home: u32,
    task_type: TaskType,
    ordering: TaskOrdering,
    logical_count: AtomicUsize,
    queued_count: AtomicUsize,
    queued_budget: Arc<Semaphore>,
    failed: AtomicBool,
    strict_execution: AtomicBool,
    strict_execution_changed: Notify,
    strict_handling: AtomicUsize,
    strict_order: Option<StrictOrder>,
    strict_route: Mutex<Option<StrictRoute>>,
    non_strict_group: Mutex<NonStrictTunnelGroup>,
    low_load_observations: AtomicUsize,
    return_activity_epoch: AtomicU64,
}

impl TypeState {
    /// 业务作用：创建当前 generation 内不可驱逐的类型状态，冻结顺序策略和类型排队预算。
    ///
    /// 参数说明：
    /// - `runner_id`: 所属 Runner 不可复用标识。
    /// - `generation`: 所属 Runner 代次。
    /// - `home`: 原始 slot。
    /// - `task_type`: 业务任务类型。
    /// - `ordering`: 首次创建时冻结的顺序要求。
    /// - `queue_capacity`: 本类型排队许可上限。
    ///
    /// 返回：逻辑计数为零、路由位于本地的新类型状态。
    pub(crate) fn new(
        runner_id: u64,
        generation: u64,
        home: u32,
        task_type: TaskType,
        ordering: TaskOrdering,
        queue_capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            runner_id,
            generation,
            home,
            task_type,
            ordering,
            logical_count: AtomicUsize::new(0),
            queued_count: AtomicUsize::new(0),
            queued_budget: Arc::new(Semaphore::new(queue_capacity)),
            failed: AtomicBool::new(false),
            strict_execution: AtomicBool::new(false),
            strict_execution_changed: Notify::new(),
            strict_handling: AtomicUsize::new(0),
            strict_order: (ordering == TaskOrdering::Strict).then(StrictOrder::new),
            strict_route: Mutex::new((ordering == TaskOrdering::Strict).then_some(StrictRoute {
                epoch: 0,
                state: StrictRouteState::Local,
                tunnel: None,
            })),
            non_strict_group: Mutex::new(NonStrictTunnelGroup::empty()),
            low_load_observations: AtomicUsize::new(0),
            return_activity_epoch: AtomicU64::new(u64::MAX),
        })
    }

    /// 业务作用：读取所属 Runner 标识，供盗洞安装拒绝跨执行域引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的 Runner 标识。
    pub(crate) fn runner_id(&self) -> u64 {
        self.runner_id
    }

    /// 业务作用：读取所属 generation，供迟到观察与定时回调拒绝跨代动作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时冻结的代次。
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：读取原始 slot，严格归还和诊断始终以此为稳定边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原始 slot 下标。
    pub(crate) fn home(&self) -> u32 {
        self.home
    }

    /// 业务作用：读取稳定任务类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次创建时冻结的类型。
    pub(crate) fn task_type(&self) -> TaskType {
        self.task_type
    }

    /// 业务作用：读取不可变顺序策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Strict 或 Relaxed。
    pub(crate) fn ordering(&self) -> TaskOrdering {
        self.ordering
    }

    /// 业务作用：读取排队许可池，提交取消时由 RAII 自动归还。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前类型独占的信号量。
    pub(crate) fn queued_budget(&self) -> Arc<Semaphore> {
        self.queued_budget.clone()
    }

    /// 业务作用：登记一笔已取得全部准入许可、尚未进入 Running 的任务投影。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：增加后的已受理排队任务数。
    pub(crate) fn increment_queued(&self) -> usize {
        self.queued_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// 业务作用：在任务进入 Running 或提前终结时恰好撤销一次已受理排队投影。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：正常减计返回 true；下溢表示内部账本失权并返回 false。
    pub(crate) fn decrement_queued(&self) -> bool {
        self.queued_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_ok()
    }

    /// 业务作用：读取已经完整受理且尚未进入 Running 的任务数量，容量等待者不计入该值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：入队、迁移、盗洞和 worker 私有暂存中的任务总数快照。
    pub(crate) fn queued_count(&self) -> usize {
        self.queued_count.load(Ordering::Acquire)
    }

    /// 业务作用：增加一笔已受理任务的逻辑计数，覆盖排队、迁移和执行直到终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：增加后的任务数。
    pub(crate) fn increment_logical(&self) -> usize {
        self.logical_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// 业务作用：结算一笔终态任务的逻辑计数；下溢表示重复结算并冻结当前类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：正常时返回减少后的任务数；下溢返回 false 并关闭本类型。
    pub(crate) fn decrement_logical(&self) -> bool {
        let result =
            self.logical_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_sub(1)
                });
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
            return false;
        }
        true
    }

    /// 业务作用：读取当前类型全部活动任务数，供集中观察选择热点。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：逻辑任务数快照。
    pub(crate) fn logical_count(&self) -> usize {
        self.logical_count.load(Ordering::Acquire)
    }

    /// 业务作用：为严格任务签发跨物理容器保持单调的受理序号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：严格类型返回新序号；非严格类型返回 None。
    pub(crate) fn issue_strict_sequence(&self) -> Option<u64> {
        self.strict_order.as_ref().and_then(StrictOrder::issue)
    }

    /// 业务作用：等待严格任务成为现役队头；非严格任务无需等待。
    ///
    /// 参数说明：
    /// - `sequence`: 提交时取得的严格序号。
    ///
    /// 返回：轮到本任务执行时返回。
    pub(crate) async fn wait_strict_turn(&self, sequence: u64) {
        if let Some(order) = &self.strict_order {
            order.wait_turn(sequence).await;
        }
    }

    /// 业务作用：同步复验严格任务是否已经成为现役队头，避免后继任务持有 slot 执行位
    /// 等待位于其它物理容器中的前序任务。
    ///
    /// 参数说明：
    /// - `sequence`: 提交时取得的严格序号。
    ///
    /// 返回：严格任务已经取得执行资格时返回 true；非严格类型始终返回 true。
    pub(crate) fn strict_turn_ready(&self, sequence: u64) -> bool {
        self.strict_order
            .as_ref()
            .is_none_or(|order| order.turn_ready(sequence))
    }

    /// 业务作用：为已经轮到的严格任务竞争类型级执行权；该门禁只隔离同一
    /// `(home, TaskType)`，其它类型的长任务不能阻止热点迁移。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次取得执行权时返回 RAII lease；门禁已占用或非严格类型返回 None。
    pub(crate) fn try_acquire_strict_execution(self: &Arc<Self>) -> Option<StrictExecutionLease> {
        if self.ordering != TaskOrdering::Strict {
            return None;
        }
        self.strict_execution
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| StrictExecutionLease {
                state: self.clone(),
            })
    }

    /// 业务作用：等待并取得严格类型执行权；等待期间不占用 slot 执行许可，因此归还到同一
    /// slot 的前序任务仍能取得推进机会。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得权威时返回 RAII lease；类型已经失败时返回 None。
    pub(crate) async fn acquire_strict_execution(self: &Arc<Self>) -> Option<StrictExecutionLease> {
        loop {
            if self.failed() {
                return None;
            }
            let notified = self.strict_execution_changed.notified();
            if let Some(lease) = self.try_acquire_strict_execution() {
                return Some(lease);
            }
            notified.await;
        }
    }

    /// 业务作用：登记严格任务已经脱离物理容器并由受监督业务边界负责，路由转换在该责任
    /// 释放前不得切换执行位置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回 RAII handling lease；计数耗尽或非严格类型返回 None。
    pub(crate) fn begin_strict_handling(self: &Arc<Self>) -> Option<StrictHandlingLease> {
        if self.ordering != TaskOrdering::Strict {
            return None;
        }
        self.strict_handling
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .ok()
            .map(|_| StrictHandlingLease {
                state: self.clone(),
            })
    }

    /// 业务作用：判断当前严格类型是否位于业务任务边界，迁移与归还只能在没有现役执行者
    /// 时改变路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：非严格类型或严格执行权空闲时返回 true。
    pub(crate) fn strict_execution_idle(&self) -> bool {
        self.ordering != TaskOrdering::Strict
            || (self.strict_handling.load(Ordering::Acquire) == 0
                && !self.strict_execution.load(Ordering::Acquire))
    }

    /// 业务作用：把严格任务终态纳入连续前缀，允许下一序号取得执行权。
    ///
    /// 参数说明：
    /// - `sequence`: 当前任务严格序号。
    ///
    /// 返回：本次推进的连续序号数；非严格类型返回零。
    pub(crate) fn settle_strict(&self, sequence: Option<u64>) -> u64 {
        match (&self.strict_order, sequence) {
            (Some(order), Some(sequence)) => order.settle(sequence),
            _ => 0,
        }
    }

    /// 业务作用：读取类型是否已失去安全推进条件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：失败状态返回 true。
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// 业务作用：单向冻结类型并关闭排队等待者，防止任务进入无消费者或顺序不明的方向。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次冻结返回 true。
    pub(crate) fn fail(&self) -> bool {
        let first = !self.failed.swap(true, Ordering::AcqRel);
        if first {
            self.queued_budget.close();
            self.strict_execution_changed.notify_waiters();
        }
        first
    }

    /// 业务作用：为非严格类型增加已登记目标盗洞，旧快照仍可由在途 producer 安全完成。
    ///
    /// 参数说明：
    /// - `tunnel`: 已完成目标入站登记的盗洞。
    ///
    /// 返回：成功安装返回 true；顺序类型不符或目标重复返回 false。
    pub(crate) fn add_non_strict_tunnel(&self, tunnel: Arc<NonStrictTunnel>) -> bool {
        if self.ordering != TaskOrdering::Relaxed
            || tunnel.runner_id() != self.runner_id
            || tunnel.generation() != self.generation
        {
            return false;
        }
        let mut group = self
            .non_strict_group
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(next) = group.with_tunnel(tunnel) else {
            return false;
        };
        *group = next;
        true
    }

    /// 业务作用：轮转选择非严格直投盗洞，并保留固定本地份额。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次应直投时返回活动盗洞，否则进入原始 slot。
    pub(crate) fn select_non_strict_tunnel(&self) -> Option<Arc<NonStrictTunnel>> {
        self.non_strict_group
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .select()
    }

    /// 业务作用：源 slot 没有本地执行位时选择任一活动目标，避免“保留本地份额”把主队头
    /// 留在无执行能力的位置并阻塞后续存量迁移。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首个仍接纳任务的非严格盗洞；没有活动目标时返回 None。
    pub(crate) fn select_non_strict_tunnel_for_move(&self) -> Option<Arc<NonStrictTunnel>> {
        self.non_strict_group
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tunnels()
            .iter()
            .find(|tunnel| tunnel.accepting())
            .cloned()
    }

    /// 业务作用：取得盗洞排空证明后从源类型活动快照撤销目标，迟到旧快照不能重新开放租约。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已关闭且排空的盗洞标识。
    ///
    /// 返回：现役快照包含并移除该盗洞时返回 true。
    pub(crate) fn remove_non_strict_tunnel(&self, tunnel_id: u64) -> bool {
        let mut group = self
            .non_strict_group
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(next) = group.without_tunnel(tunnel_id) else {
            return false;
        };
        *group = next;
        true
    }

    /// 业务作用：判断非严格类型是否已经连接指定目标，源 worker 选择候选时避免重复安装。
    ///
    /// 参数说明：
    /// - `target`: 候选目标 slot。
    ///
    /// 返回：活动快照中存在该目标时返回 true。
    pub(crate) fn has_non_strict_target(&self, target: u32) -> bool {
        self.non_strict_group
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tunnels()
            .iter()
            .any(|tunnel| tunnel.target() == target)
    }

    /// 业务作用：读取严格路由快照，producer 在物理发布前必须仍持有本次快照对应的路由锁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：严格类型返回当前快照；非严格类型返回 None。
    pub(crate) fn strict_route(&self) -> Option<StrictRoute> {
        self.strict_route
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// 业务作用：在源 worker 权威下安装唯一严格盗洞，路由代次单调增加并关闭本地新直投。
    ///
    /// 参数说明：
    /// - `tunnel`: 已在目标 slot 登记且身份一致的严格盗洞。
    ///
    /// 返回：Local 状态成功进入 Stolen 返回 true；已有迁移或身份不一致返回 false。
    pub(crate) fn install_strict_tunnel(&self, tunnel: Arc<StrictTunnel>) -> bool {
        if self.ordering != TaskOrdering::Strict
            || tunnel.runner_id() != self.runner_id
            || tunnel.generation() != self.generation
            || tunnel.source() != self.home
        {
            return false;
        }
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state != StrictRouteState::Local {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        // 先关闭 Local 新直投并发布 Migrating；源 worker 只有在旧 producer 归零且主队列
        // consumer 到达冻结边界后才发布 Stolen，目标不能提前越过 stock 前缀。
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::Migrating,
            tunnel: Some(tunnel.clone()),
        });
        true
    }

    /// 业务作用：源 consumer 已分类旧 Local 边界后把严格路由从 Migrating 发布为 Stolen。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已取得 source boundary 证明的盗洞标识。
    ///
    /// 返回：身份和阶段一致时返回 true；迟到或代次耗尽返回 false。
    pub(crate) fn finish_strict_migration(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state != StrictRouteState::Migrating
            || current.tunnel.as_ref().map(|tunnel| tunnel.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(stolen_epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        *route = Some(StrictRoute {
            epoch: stolen_epoch,
            state: StrictRouteState::Stolen,
            tunnel: current.tunnel.clone(),
        });
        true
    }

    /// 业务作用：在严格路由锁内选择新任务物理落点，防止归还切相与 producer 发布交错。
    ///
    /// 参数说明：
    /// - `publish`: 接收当前快照并完成 reservation 与物理发布的闭包。
    ///
    /// 返回：闭包返回值；非严格类型返回 None。
    pub(crate) fn with_strict_route<R>(
        &self,
        publish: impl FnOnce(&StrictRoute) -> R,
    ) -> Option<R> {
        let route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        route.as_ref().map(publish)
    }

    /// 业务作用：累计严格类型连续低负载观察，任何恢复热点的观察都会清零归还证据。
    ///
    /// 参数说明：
    /// - `low_load`: 当前观察是否满足归还条件。
    /// - `activity_epoch`: 当前盗洞活动代次。
    ///
    /// 返回：连续低负载观察次数。
    pub(crate) fn observe_return_candidate(&self, low_load: bool, activity_epoch: u64) -> usize {
        let previous = self
            .return_activity_epoch
            .swap(activity_epoch, Ordering::AcqRel);
        if low_load && (previous == activity_epoch || previous == u64::MAX) {
            self.low_load_observations.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            self.low_load_observations.store(0, Ordering::Release);
            0
        }
    }

    /// 业务作用：把严格路由切入 ReturnPrepare，使后续 producer 只进入无 consumer 的 staging。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：从 Stolen 成功进入准备态时返回现役盗洞。
    pub(crate) fn begin_strict_return(&self) -> Option<Arc<StrictTunnel>> {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let current = route.as_ref()?;
        if current.state != StrictRouteState::Stolen {
            return None;
        }
        let tunnel = current.tunnel.clone()?;
        *route = Some(StrictRoute {
            epoch: current.epoch.checked_add(1)?,
            state: StrictRouteState::ReturnPrepare,
            tunnel: Some(tunnel.clone()),
        });
        Some(tunnel)
    }

    /// 业务作用：归还准备期出现新活动且尚未发生物理回写时切入 StolenCatchup，使 staging
    /// 由目标重新接管而不丢失顺序。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 当前 ReturnPrepare 盗洞标识。
    ///
    /// 返回：身份与阶段一致时返回 true；已进入不可逆阶段时拒绝。
    pub(crate) fn cancel_strict_return(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state != StrictRouteState::ReturnPrepare
            || current.tunnel.as_ref().map(|tunnel| tunnel.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::StolenCatchup,
            tunnel: current.tunnel.clone(),
        });
        true
    }

    /// 业务作用：目标排空可逆归还 staging 后重新发布 Stolen，后续 producer 与 consumer
    /// 继续使用同一盗洞。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已完成 staging 追赶的盗洞标识。
    ///
    /// 返回：身份与阶段一致时返回 true。
    pub(crate) fn finish_stolen_catchup(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state != StrictRouteState::StolenCatchup
            || current.tunnel.as_ref().map(|tunnel| tunnel.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::Stolen,
            tunnel: current.tunnel.clone(),
        });
        self.low_load_observations.store(0, Ordering::Release);
        true
    }

    /// 业务作用：旧直投 producer 与可执行 FIFO 收口后发布不可逆 Returning 阶段。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 当前已冻结直投边界的盗洞标识。
    ///
    /// 返回：ReturnPrepare 身份一致时返回 true；迟到或代次耗尽返回 false。
    pub(crate) fn mark_strict_returning(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state != StrictRouteState::ReturnPrepare
            || current.tunnel.as_ref().map(|tunnel| tunnel.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::Returning,
            tunnel: current.tunnel.clone(),
        });
        true
    }

    /// 业务作用：首次归还动作确定不可撤销后切换 LocalCatchup，使新 producer 进入源主队列。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 当前 Returning 盗洞标识。
    ///
    /// 返回：身份和阶段一致时返回 true。
    pub(crate) fn begin_local_catchup(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state != StrictRouteState::Returning
            || current.tunnel.as_ref().map(|tunnel| tunnel.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::LocalCatchup,
            tunnel: current.tunnel.clone(),
        });
        true
    }

    /// 业务作用：在 LocalCatchup 全部暂存任务回写后发布 Local，禁止归还后旧目标复活。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已取得排空证明的现役盗洞标识。
    ///
    /// 返回：身份和阶段一致时完成归还并返回 true。
    pub(crate) fn finish_strict_return(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if !matches!(current.state, StrictRouteState::LocalCatchup)
            || current.tunnel.as_ref().map(|value| value.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            self.fail();
            return false;
        };
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::Local,
            tunnel: None,
        });
        self.low_load_observations.store(0, Ordering::Release);
        true
    }

    /// 业务作用：严格盗洞失去队列或 owner 证明时发布保留历史上下文的 Failed 路由并拒收。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 发生失权的现役盗洞标识。
    ///
    /// 返回：首次把匹配路由切到 Failed 时返回 true。
    pub(crate) fn fail_strict_route(&self, tunnel_id: u64) -> bool {
        let mut route = self.strict_route.lock().unwrap_or_else(|e| e.into_inner());
        let Some(current) = route.as_ref() else {
            return false;
        };
        if current.state == StrictRouteState::Failed
            || current.tunnel.as_ref().map(|tunnel| tunnel.id()) != Some(tunnel_id)
        {
            return false;
        }
        let Some(epoch) = current.epoch.checked_add(1) else {
            drop(route);
            return self.fail();
        };
        *route = Some(StrictRoute {
            epoch,
            state: StrictRouteState::Failed,
            tunnel: current.tunnel.clone(),
        });
        drop(route);
        self.fail()
    }
}

/// 严格类型单任务执行权；任务离开受监督业务边界时自动开放后继与路由转换。
pub(crate) struct StrictExecutionLease {
    state: Arc<TypeState>,
}

impl Drop for StrictExecutionLease {
    /// 业务作用：释放同一类型的严格执行门禁，使后继任务或迁移控制能够继续推进。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；执行权释放后唤醒同类型等待者。
    fn drop(&mut self) {
        self.state.strict_execution.store(false, Ordering::Release);
        self.state.strict_execution_changed.notify_waiters();
    }
}

/// 严格任务脱离物理容器后的受监督责任；等待执行顺序期间仍阻止路由切相。
pub(crate) struct StrictHandlingLease {
    state: Arc<TypeState>,
}

impl Drop for StrictHandlingLease {
    /// 业务作用：任务进入稳定终态或失败收口后释放严格处理责任，使迁移和归还重新可观察。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；责任计数下溢时关闭当前类型。
    fn drop(&mut self) {
        if self
            .state
            .strict_handling
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_err()
        {
            self.state.fail();
        }
    }
}
