//! 分区槽位与唯一 worker 主循环。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::entry::TaskEnvelope;
use crate::observer::StealRequest;
use crate::queue::{
    channel, ConsumerPoll, ProducerBoundary, Reservation, ReserveError, SequencedReceiver,
    SequencedSender,
};
use crate::route::{StrictRouteState, TaskType, TypeState};
use crate::runner::RunnerInner;
use crate::tunnel::{NonStrictTunnel, StrictTunnel};

/// worker 独占的两个 consumer；构造后不再进入共享 `PartitionSlot`。
pub(crate) struct SlotConsumers {
    main: SequencedReceiver<Arc<TaskEnvelope>>,
    control: SequencedReceiver<Arc<StealRequest>>,
    deferred: Option<DeferredTask>,
    deferred_control: Option<Arc<StealRequest>>,
}

enum DeferredTask {
    Main {
        sequence: u64,
        envelope: Arc<TaskEnvelope>,
    },
    Tunnel(Arc<TaskEnvelope>),
}

impl DeferredTask {
    /// 业务作用：读取 worker 已从唯一物理 consumer 摘取但尚未派发的任务投影，供有损
    /// 收口在不猜测来源容器的前提下发布终态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 worker 独占 Handling 责任的任务引用。
    fn envelope(&self) -> &Arc<TaskEnvelope> {
        match self {
            Self::Main { envelope, .. } | Self::Tunnel(envelope) => envelope,
        }
    }
}

/// 单 slot 的低基数队列、producer 与严格路由观测值。
pub(crate) struct SlotMetrics {
    pub(crate) main_depth: u64,
    pub(crate) control_depth: u64,
    pub(crate) reserved_heads: u64,
    pub(crate) producer_inflight: u64,
    pub(crate) stock_depth: u64,
    pub(crate) incremental_depth: u64,
    pub(crate) staging_depth: u64,
    pub(crate) non_strict_open: u64,
    pub(crate) non_strict_draining: u64,
    pub(crate) strict_routes: [u64; 8],
}

/// 当前 generation 的分区槽位。
pub(crate) struct PartitionSlot {
    runner_id: u64,
    generation: u64,
    index: u32,
    main: SequencedSender<Arc<TaskEnvelope>>,
    control: SequencedSender<Arc<StealRequest>>,
    pub(crate) type_states: DashMap<TaskType, Arc<TypeState>>,
    inbound_non_strict: DashMap<u64, Arc<NonStrictTunnel>>,
    inbound_strict: DashMap<u64, Arc<StrictTunnel>>,
    max_inbound_tunnels: usize,
    inbound_tunnel_count: AtomicUsize,
    pub(crate) local_task_count: AtomicUsize,
    pub(crate) tunnel_task_count: AtomicUsize,
    main_depth: AtomicUsize,
    control_depth: AtomicUsize,
    main_reserved_head: AtomicBool,
    control_reserved_head: AtomicBool,
    execution_budget: Arc<Semaphore>,
    accepting: AtomicBool,
    failed: AtomicBool,
    wake: Arc<Notify>,
}

impl PartitionSlot {
    /// 业务作用：创建一个主 MPSC、一个控制 MPSC 与唯一 consumer 对，slot 初始开放且无类型状态。
    ///
    /// 参数说明：
    /// - `runner_id`: 所属 Runner 标识。
    /// - `generation`: 所属代次。
    /// - `index`: 分区下标。
    /// - `max_inbound_tunnels`: 非严格与严格入站盗洞合计上限。
    ///
    /// 返回：共享 slot 与只能交给本 slot worker 的 consumer。
    pub(crate) fn new(
        runner_id: u64,
        generation: u64,
        index: u32,
        max_inbound_tunnels: usize,
    ) -> (Arc<Self>, SlotConsumers) {
        let (main, main_rx) = channel();
        let (control, control_rx) = channel();
        (
            Arc::new(Self {
                runner_id,
                generation,
                index,
                main,
                control,
                type_states: DashMap::new(),
                inbound_non_strict: DashMap::new(),
                inbound_strict: DashMap::new(),
                max_inbound_tunnels,
                inbound_tunnel_count: AtomicUsize::new(0),
                local_task_count: AtomicUsize::new(0),
                tunnel_task_count: AtomicUsize::new(0),
                main_depth: AtomicUsize::new(0),
                control_depth: AtomicUsize::new(0),
                main_reserved_head: AtomicBool::new(false),
                control_reserved_head: AtomicBool::new(false),
                execution_budget: Arc::new(Semaphore::new(1)),
                accepting: AtomicBool::new(true),
                failed: AtomicBool::new(false),
                wake: Arc::new(Notify::new()),
            }),
            SlotConsumers {
                main: main_rx,
                control: control_rx,
                deferred: None,
                deferred_control: None,
            },
        )
    }

    /// 业务作用：读取所属 Runner 标识，所有盗洞安装与任务接纳都必须一致。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建 slot 时冻结的 Runner ID。
    pub(crate) fn runner_id(&self) -> u64 {
        self.runner_id
    }

    /// 业务作用：读取所属 generation，拒绝重启后的迟到路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建 slot 时冻结的 generation。
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：读取分区下标，作为物理 owner 的非负编码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 分区数组中的稳定下标。
    pub(crate) fn index(&self) -> u32 {
        self.index
    }

    /// 业务作用：判断 slot 是否仍能接纳任务和控制请求。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：准入门禁开放且 slot 未失败时返回 true。
    pub(crate) fn accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire) && !self.failed.load(Ordering::Acquire)
    }

    /// 业务作用：预留主队列序号，使 producer 可以先建立任务权威再发布物理载荷。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：slot 开放时返回已计入深度的 reservation；关门或队列失权时返回
    /// 物理队列原因。
    pub(crate) fn reserve_main(&self) -> Result<Reservation<Arc<TaskEnvelope>>, ReserveError> {
        if !self.accepting() {
            return Err(ReserveError::Closed);
        }
        let reservation = self.main.reserve()?;
        // 未消费槽位必须在 reservation 逃离本调用前计入，防止已发布载荷
        // 被 worker 先行取得后减少尚未增加的深度。
        self.main_depth.fetch_add(1, Ordering::AcqRel);
        // reservation 析构会发布 poison 但不能直接持有 slot 唤醒点，预留时先唤醒
        // worker，使其在槽位未完成前保持控制时钟，避免孤立 poison 长期滞留。
        self.wake.notify_one();
        Ok(reservation)
    }

    /// 业务作用：为已经受理且正在严格归还的任务预留源主队列位置；Runner 关门后外部提交
    /// 已被 accepting 门禁拒绝，但不可逆归还仍必须完成物理回写才能保持顺序证明。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：主队列仍具备序号权威时返回 reservation；内部队列失效时返回 Closed 或 Failed。
    pub(crate) fn reserve_main_transfer(
        &self,
    ) -> Result<Reservation<Arc<TaskEnvelope>>, ReserveError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(ReserveError::Closed);
        }
        let reservation = self.main.reserve()?;
        self.main_depth.fetch_add(1, Ordering::AcqRel);
        self.wake.notify_one();
        Ok(reservation)
    }

    /// 业务作用：主队列 reservation 发布后唤醒唯一 worker；物理深度已在预留点
    /// 提交。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；唤醒信号与已发布载荷一起保持到 worker 观察。
    pub(crate) fn commit_main(&self) {
        self.wake.notify_one();
    }

    /// 业务作用：主队列 reservation 在物理发布前失去权威时撤销未消费深度，
    /// 并隔离无法继续保证序号一致性的 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；slot 关门并唤醒现役 worker 进入失败收口。
    pub(crate) fn abort_main_reserved(&self) {
        let decremented = self
            .main_depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_ok();
        debug_assert!(decremented, "main queue depth authority lost");
        self.fail();
    }

    /// 业务作用：读取源主队列 producer 是否全部离开 reservation 临界区，严格迁移据此冻结边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前主队列在途 producer 数。
    pub(crate) fn main_producer_inflight(&self) -> usize {
        self.main.producer_inflight()
    }

    /// 业务作用：读取源主队列下一个尚未分配序号，作为严格旧 Local 路由排他上界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：单调 producer 边界。
    pub(crate) fn main_producer_boundary(&self) -> ProducerBoundary {
        self.main.boundary()
    }

    /// 业务作用：向源 worker 控制队列发布盗洞请求，观察任务自身不得改写源路由。
    ///
    /// 参数说明：
    /// - `request`: 已冻结目标 slot 与观察 epoch 的一次性安装请求。
    ///
    /// 返回：请求已完成物理发布时返回 true；slot 关门、队列失权或发布未完成时
    /// 返回 false，失权路径同时隔离 slot。
    pub(crate) fn offer_control(&self, request: Arc<StealRequest>) -> bool {
        if !self.accepting() {
            return false;
        }
        let Ok(reservation) = self.control.reserve() else {
            return false;
        };
        // 控制队列与主队列共享“预留先计数、消费后归还”的物理槽位不变量。
        self.control_depth.fetch_add(1, Ordering::AcqRel);
        if reservation.publish(request).is_err() {
            let _ = self
                .control_depth
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_sub(1)
                });
            self.fail();
            return false;
        }
        self.wake.notify_one();
        true
    }

    /// 业务作用：在目标 worker 开始轮询前登记非严格入站盗洞，并限制重复 ID。
    ///
    /// 参数说明：
    /// - `tunnel`: 已冻结 Runner、generation、源、目标与类型的候选盗洞。
    ///
    /// 返回：身份一致、slot 开放、入站名额可用且 ID 首次登记时返回 true；其他情况
    /// 不改变现有入站表并返回 false。
    pub(crate) fn register_non_strict(&self, tunnel: Arc<NonStrictTunnel>) -> bool {
        if !self.accepting()
            || tunnel.runner_id() != self.runner_id
            || tunnel.generation() != self.generation
            || tunnel.target() != self.index
        {
            return false;
        }
        if !self.reserve_inbound_tunnel() {
            return false;
        }
        if self
            .inbound_non_strict
            .insert(tunnel.id(), tunnel)
            .is_none()
        {
            true
        } else {
            self.release_inbound_tunnel();
            false
        }
    }

    /// 业务作用：在目标 worker 开始轮询前登记唯一严格入站盗洞，并限制重复 ID。
    ///
    /// 参数说明：
    /// - `tunnel`: 已冻结 Runner、generation、源、目标与严格类型的候选盗洞。
    ///
    /// 返回：身份一致、slot 开放、入站名额可用且 ID 首次登记时返回 true；其他情况
    /// 不改变现有入站表并返回 false。
    pub(crate) fn register_strict(&self, tunnel: Arc<StrictTunnel>) -> bool {
        if !self.accepting()
            || tunnel.runner_id() != self.runner_id
            || tunnel.generation() != self.generation
            || tunnel.target() != self.index
        {
            return false;
        }
        if !self.reserve_inbound_tunnel() {
            return false;
        }
        if self.inbound_strict.insert(tunnel.id(), tunnel).is_none() {
            true
        } else {
            self.release_inbound_tunnel();
            false
        }
    }

    /// 业务作用：盗洞关闭且排空后撤销目标入站引用，释放队列和源类型历史快照。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已取得排空证明的非严格盗洞标识。
    ///
    /// 返回：目标表实际移除一项时返回 true。
    pub(crate) fn unregister_non_strict(&self, tunnel_id: u64) -> bool {
        if self.inbound_non_strict.remove(&tunnel_id).is_some() {
            self.release_inbound_tunnel();
            true
        } else {
            false
        }
    }

    /// 业务作用：严格归还完成或失败清理后撤销目标入站引用，旧队列不跨代保留。
    ///
    /// 参数说明：
    /// - `tunnel_id`: 已完成收口的严格盗洞标识。
    ///
    /// 返回：目标表实际移除一项时返回 true。
    pub(crate) fn unregister_strict(&self, tunnel_id: u64) -> bool {
        if self.inbound_strict.remove(&tunnel_id).is_some() {
            self.release_inbound_tunnel();
            true
        } else {
            false
        }
    }

    /// 业务作用：原子预留非严格与严格共享的入站盗洞名额，阻止多个源并发越过总上限。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前名额未满且本次成功占用时返回 true。
    fn reserve_inbound_tunnel(&self) -> bool {
        self.inbound_tunnel_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < self.max_inbound_tunnels).then_some(value + 1)
            })
            .is_ok()
    }

    /// 业务作用：目标登记被撤销或安装回滚时归还一个入站盗洞名额；下溢会隔离 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：计数正常减少时返回 true；重复归还返回 false。
    fn release_inbound_tunnel(&self) -> bool {
        if self
            .inbound_tunnel_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            true
        } else {
            self.fail();
            false
        }
    }

    /// 业务作用：读取当前严格入站盗洞快照，目标 worker 只定向推进自身控制状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时仍登记的盗洞 Arc 列表。
    pub(crate) fn strict_tunnels(&self) -> Vec<Arc<StrictTunnel>> {
        self.inbound_strict
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// 业务作用：判断本 slot 是否已有来自指定源的活动盗洞，observer 可在同一方向复用
    /// 已有执行能力，为后来出现的严格热点保留独立迁移机会。
    ///
    /// 参数说明：
    /// - `source`: 候选对端 slot 下标。
    ///
    /// 返回：任一严格或非严格入站盗洞来自该源时返回 true。
    pub(crate) fn has_inbound_from(&self, source: u32) -> bool {
        self.inbound_non_strict
            .iter()
            .any(|tunnel| tunnel.source() == source)
            || self
                .inbound_strict
                .iter()
                .any(|tunnel| tunnel.source() == source)
    }

    /// 业务作用：读取仍由本 slot 作为逻辑 owner 的任务数量，集中 observer 据此识别
    /// 原始分区热点，不让已经借出的盗洞积压反向冒充新的热点源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地逻辑容器当前承担的任务数近似快照。
    pub(crate) fn local_task_load(&self) -> usize {
        self.local_task_count.load(Ordering::Acquire)
    }

    /// 业务作用：读取当前主队列深度，供集中观察选择空闲目标。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含未发布 reservation 和 poison 的主队列未消费槽位数。
    pub(crate) fn main_depth(&self) -> usize {
        self.main_depth.load(Ordering::Acquire)
    }

    /// 业务作用：读取当前主队列、盗洞和运行任务的近似总负载。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：主队列物理槽位、盗洞逻辑任务和本地逻辑任务的饱和求和。
    pub(crate) fn load(&self) -> usize {
        self.main_depth()
            .saturating_add(self.tunnel_task_count.load(Ordering::Acquire))
            .saturating_add(self.local_task_count.load(Ordering::Acquire))
    }

    /// 业务作用：汇总本 slot 的物理队列、在途 producer、盗洞与严格路由状态，供 Runner
    /// 导出低基数观测而不泄露内部调度对象。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时刻的非事务性资源快照。
    pub(crate) fn metrics_snapshot(&self) -> SlotMetrics {
        let strict_tunnels = self
            .inbound_strict
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        let mut stock_depth = 0_u64;
        let mut incremental_depth = 0_u64;
        let mut staging_depth = 0_u64;
        let mut producer_inflight =
            self.main
                .producer_inflight()
                .saturating_add(self.main.outstanding_reservations())
                .saturating_add(self.control.producer_inflight())
                .saturating_add(self.control.outstanding_reservations()) as u64;
        for tunnel in &strict_tunnels {
            let (stock, incremental, staging) = tunnel.queue_depths();
            stock_depth = stock_depth.saturating_add(stock as u64);
            incremental_depth = incremental_depth.saturating_add(incremental as u64);
            staging_depth = staging_depth.saturating_add(staging as u64);
            producer_inflight = producer_inflight.saturating_add(tunnel.producer_inflight() as u64);
        }
        let non_strict_tunnels = self
            .inbound_non_strict
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        let mut non_strict_open = 0_u64;
        let mut non_strict_draining = 0_u64;
        for tunnel in &non_strict_tunnels {
            producer_inflight = producer_inflight.saturating_add(tunnel.producer_inflight() as u64);
            if tunnel.accepting() {
                non_strict_open = non_strict_open.saturating_add(1);
            } else if !tunnel.drained() {
                non_strict_draining = non_strict_draining.saturating_add(1);
            }
        }
        let mut strict_routes = [0_u64; 8];
        for state in &self.type_states {
            let Some(route) = state.strict_route() else {
                continue;
            };
            let index = match route.state {
                StrictRouteState::Local => 0,
                StrictRouteState::Migrating => 1,
                StrictRouteState::Stolen => 2,
                StrictRouteState::ReturnPrepare => 3,
                StrictRouteState::Returning => 4,
                StrictRouteState::LocalCatchup => 5,
                StrictRouteState::StolenCatchup => 6,
                StrictRouteState::Failed => 7,
            };
            strict_routes[index] = strict_routes[index].saturating_add(1);
        }
        SlotMetrics {
            main_depth: self.main_depth.load(Ordering::Acquire) as u64,
            control_depth: self.control_depth.load(Ordering::Acquire) as u64,
            reserved_heads: self.main_reserved_head.load(Ordering::Acquire) as u64
                + self.control_reserved_head.load(Ordering::Acquire) as u64,
            producer_inflight,
            stock_depth,
            incremental_depth,
            staging_depth,
            non_strict_open,
            non_strict_draining,
            strict_routes,
        }
    }

    /// 业务作用：判断本 slot 当前是否有业务执行位，worker 据此避免提前摘取无法迁移的任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：独占执行 semaphore 尚有许可时返回 true。
    pub(crate) fn execution_available(&self) -> bool {
        self.execution_budget.available_permits() > 0
    }

    /// 业务作用：判断当前 worker 是否需要控制时钟兜底唤醒；完全空闲且没有迁移、盗洞或
    /// reservation 时只等待真实通知，避免分区数量放大空转 CPU。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：存在入站盗洞、源侧严格迁移或队列 producer 临界区时返回 true。
    pub(crate) fn needs_control_tick(&self) -> bool {
        if !self.inbound_non_strict.is_empty()
            || !self.inbound_strict.is_empty()
            || self.main_depth.load(Ordering::Acquire) > 0
            || self.control_depth.load(Ordering::Acquire) > 0
            || self.main.producer_inflight() > 0
            || self.main.outstanding_reservations() > 0
            || self.control.producer_inflight() > 0
            || self.control.outstanding_reservations() > 0
        {
            return true;
        }
        self.type_states.iter().any(|state| {
            state
                .strict_route()
                .is_some_and(|route| matches!(route.state, StrictRouteState::Migrating))
        })
    }

    /// 业务作用：为一笔即将派发的业务任务取得 slot 独占执行位，使同 slot 长任务不会被新任务绕过。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：执行位空闲时返回 RAII 许可；已被占用时返回 None。
    pub(crate) fn try_acquire_execution(&self) -> Option<OwnedSemaphorePermit> {
        self.execution_budget.clone().try_acquire_owned().ok()
    }

    /// 业务作用：等待取得 slot 独占执行位；严格任务已在类型层等待到队头后才调用，避免
    /// 后继占位阻塞其它物理容器中的前序。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得执行位返回许可；仅在 semaphore 被关闭时返回失败。
    pub(crate) async fn acquire_execution(
        &self,
    ) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.execution_budget.clone().acquire_owned().await
    }

    /// 业务作用：关闭外部接纳、控制队列和全部入站盗洞 producer；主队列保留仅供已经受理
    /// 任务完成不可逆内部回写，外部 reserve 仍由 accepting 门禁拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；关门单向保持并唤醒所有等待 worker 进入收口。
    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        self.control.close();
        for tunnel in &self.inbound_non_strict {
            tunnel.close();
        }
        for tunnel in &self.inbound_strict {
            tunnel.close_all();
        }
        self.wake.notify_waiters();
        self.wake.notify_one();
    }

    /// 业务作用：单向隔离失败 slot，先关物理入口再由 Runner 更新健康与收口策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次隔离时返回 true；已隔离时幂等返回 false。
    pub(crate) fn fail(&self) -> bool {
        let first = !self.failed.swap(true, Ordering::AcqRel);
        if first {
            self.close();
        }
        first
    }

    /// 业务作用：判断所有共享物理容器是否已经排空，作为 worker 无损退出条件之一。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：主/控制队列深度、在途 producer、reservation 与全部入站盗洞都取得排空
    /// 证明时返回 true。
    pub(crate) fn queues_empty(&self) -> bool {
        self.main_depth.load(Ordering::Acquire) == 0
            && self.main.producer_inflight() == 0
            && self.main.outstanding_reservations() == 0
            && self.control_depth.load(Ordering::Acquire) == 0
            && self.control.producer_inflight() == 0
            && self.control.outstanding_reservations() == 0
            && self.inbound_non_strict.iter().all(|entry| entry.drained())
            && self.inbound_strict.iter().all(|entry| {
                entry.depth() == 0
                    && entry.direct_producers_stopped()
                    && entry.staging_producers_stopped()
            })
    }

    /// 业务作用：取得 worker 唤醒点 Arc，使 observer 和提交方不借用 slot 即可等待。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享同一丢失抵抗唤醒点的 `Arc<Notify>`。
    pub(crate) fn wake_handle(&self) -> Arc<Notify> {
        self.wake.clone()
    }
}

/// 业务作用：运行一个 slot 的唯一 consumer，公平穿插控制请求、主队列和入站盗洞。
///
/// 参数说明：
/// - `inner`: 当前 generation Runner 内核；受监督任务退出前保持代次可达。
/// - `slot`: 本 worker 独占服务的 slot。
/// - `consumers`: 构造时取得的主队列与控制队列唯一 consumer。
///
/// 返回：关门且全部物理容器排空，或 generation 被有损停止后退出。
pub(crate) async fn run(
    inner: Arc<RunnerInner>,
    slot: Arc<PartitionSlot>,
    mut consumers: SlotConsumers,
) {
    let mut exit_guard = WorkerExitGuard {
        inner: inner.clone(),
        slot: slot.clone(),
        clean: false,
    };
    loop {
        let runner = inner.clone();
        let mut progressed = false;
        let batch = runner.config().drain_batch;
        if runner.lifecycle_mode() == crate::lifecycle::LifecycleMode::ForceStop {
            if let Some(deferred) = consumers.deferred.take() {
                runner.fail_deferred(deferred.envelope(), "shutdown_frozen");
                progressed = true;
            }
        }
        // 业务任务释放执行位后，先让上轮保留的控制请求取得任务边界；若先恢复 deferred，
        // 单一严格类型会立即重新占用执行门禁，使仍有积压的类型永远错过合法迁移窗口。
        // 有损停止仍在上方优先结算 deferred，控制公平性不能延迟强制收口。
        if let Some(request) = consumers.deferred_control.take() {
            if runner.lifecycle_mode() == crate::lifecycle::LifecycleMode::ForceStop {
                request.complete();
                progressed = true;
            } else if slot.execution_available() {
                if !runner.handle_steal_request(&slot, request.clone()) {
                    consumers.deferred_control = Some(request);
                }
                progressed = true;
            } else {
                consumers.deferred_control = Some(request);
            }
        }
        for _ in 0..batch.min(8) {
            if consumers.deferred_control.is_some() {
                break;
            }
            let control_poll = consumers.control.try_recv();
            slot.control_reserved_head.store(
                matches!(control_poll, ConsumerPoll::Reserved { .. }),
                Ordering::Release,
            );
            match control_poll {
                ConsumerPoll::Item { value, .. } => {
                    let decremented = slot
                        .control_depth
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                            value.checked_sub(1)
                        })
                        .is_ok();
                    debug_assert!(decremented, "control queue depth authority lost");
                    if value.pending() && !runner.handle_steal_request(&slot, value.clone()) {
                        // 请求只缺当前严格任务边界时保留 pending 权威，
                        // 不反复入队，也不让后续目标请求越过本轮公平机会。
                        consumers.deferred_control = Some(value);
                    }
                    progressed = true;
                }
                ConsumerPoll::Poisoned { .. } => {
                    let decremented = slot
                        .control_depth
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                            value.checked_sub(1)
                        })
                        .is_ok();
                    debug_assert!(decremented, "control queue depth authority lost");
                    progressed = true;
                }
                ConsumerPoll::Failed => {
                    runner.fail_slot(&slot, "control_queue_failed");
                    break;
                }
                ConsumerPoll::Empty | ConsumerPoll::Reserved { .. } | ConsumerPoll::Closed => break,
            }
        }
        if let Some(deferred) = consumers.deferred.take() {
            consumers.deferred = match deferred {
                DeferredTask::Main { sequence, envelope } => runner
                    .resume_main_handling(&slot, sequence, envelope)
                    .map(|envelope| DeferredTask::Main { sequence, envelope }),
                DeferredTask::Tunnel(envelope) => runner
                    .resume_handling(slot.index(), envelope)
                    .map(DeferredTask::Tunnel),
            };
            progressed |= consumers.deferred.is_none();
        }
        for _ in 0..batch {
            if consumers.deferred.is_some() {
                break;
            }
            let main_poll = consumers.main.try_recv();
            slot.main_reserved_head.store(
                matches!(main_poll, ConsumerPoll::Reserved { .. }),
                Ordering::Release,
            );
            match main_poll {
                ConsumerPoll::Item { sequence, value } => {
                    let decremented = slot
                        .main_depth
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                            value.checked_sub(1)
                        })
                        .is_ok();
                    debug_assert!(decremented, "main queue depth authority lost");
                    consumers.deferred = runner
                        .consume_main(&slot, sequence, value)
                        .map(|envelope| DeferredTask::Main { sequence, envelope });
                    progressed = true;
                }
                ConsumerPoll::Poisoned { .. } => {
                    let decremented = slot
                        .main_depth
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                            value.checked_sub(1)
                        })
                        .is_ok();
                    debug_assert!(decremented, "main queue depth authority lost");
                    progressed = true;
                }
                ConsumerPoll::Failed => {
                    runner.fail_slot(&slot, "main_queue_failed");
                    break;
                }
                ConsumerPoll::Empty | ConsumerPoll::Reserved { .. } | ConsumerPoll::Closed => break,
            }
        }
        runner.progress_migrations(&slot, consumers.main.boundary());
        let strict_tunnels = slot
            .inbound_strict
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for tunnel in strict_tunnels {
            for _ in 0..batch {
                if consumers.deferred.is_some()
                    || (runner.lifecycle_mode() != crate::lifecycle::LifecycleMode::ForceStop
                        && !slot.execution_available())
                {
                    break;
                }
                match tunnel.poll_executable() {
                    ConsumerPoll::Item { sequence, value } => {
                        consumers.deferred = runner
                            .consume_tunnel(&slot, sequence, value)
                            .map(DeferredTask::Tunnel);
                        progressed = true;
                    }
                    ConsumerPoll::Poisoned { .. } => progressed = true,
                    ConsumerPoll::Failed => {
                        runner.fail_strict_tunnel(&slot, &tunnel, "strict_tunnel_queue_failed");
                        break;
                    }
                    ConsumerPoll::Empty | ConsumerPoll::Reserved { .. } | ConsumerPoll::Closed => {
                        break
                    }
                }
            }
        }
        let non_strict_tunnels = slot
            .inbound_non_strict
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for tunnel in non_strict_tunnels {
            tunnel.close_if_expired();
            for _ in 0..batch {
                if consumers.deferred.is_some()
                    || (runner.lifecycle_mode() != crate::lifecycle::LifecycleMode::ForceStop
                        && !slot.execution_available())
                {
                    break;
                }
                match tunnel.poll() {
                    ConsumerPoll::Item { sequence, value } => {
                        consumers.deferred = runner
                            .consume_tunnel(&slot, sequence, value)
                            .map(DeferredTask::Tunnel);
                        progressed = true;
                    }
                    ConsumerPoll::Poisoned { .. } => progressed = true,
                    ConsumerPoll::Failed => {
                        tunnel.close();
                        break;
                    }
                    ConsumerPoll::Empty | ConsumerPoll::Reserved { .. } | ConsumerPoll::Closed => {
                        break
                    }
                }
            }
            runner.retire_non_strict_tunnel(&slot, &tunnel);
        }
        runner.progress_returns(&slot);
        if runner.worker_should_exit(&slot)
            && slot.queues_empty()
            && consumers.deferred.is_none()
            && consumers.deferred_control.is_none()
        {
            break;
        }
        if !progressed {
            let wake = slot.wake_handle();
            let needs_control_tick =
                slot.needs_control_tick() || consumers.deferred_control.is_some();
            let control_tick = runner.config().control_tick;
            drop(runner);
            if needs_control_tick {
                let _ = tokio::time::timeout(control_tick, wake.notified()).await;
            } else {
                wake.notified().await;
            }
        }
    }
    exit_guard.clean = true;
}

/// 业务作用：确保 slot worker 未取得排空证明就离场时把最小故障域转入关闭状态。
struct WorkerExitGuard {
    inner: Arc<RunnerInner>,
    slot: Arc<PartitionSlot>,
    clean: bool,
}

impl Drop for WorkerExitGuard {
    /// 业务作用：worker 在未取得排空证明前异常离场时立即关闭 slot，并把故障交给 Runner
    /// 健康与停止权威；正常退出不产生额外状态变化。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；非正常标记会关闭最小 slot 故障域。
    fn drop(&mut self) {
        if !self.clean {
            self.inner
                .fail_slot(&self.slot, "worker_exited_without_drain_proof");
        }
    }
}
