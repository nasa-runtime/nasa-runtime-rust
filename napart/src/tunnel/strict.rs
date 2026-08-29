//! 严格迁移与归还的多 FIFO 盗洞。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::entry::TaskEnvelope;
use crate::queue::{
    channel, ConsumerBoundary, ConsumerPoll, ProducerBoundary, Reservation, ReserveError,
    SequencedReceiver, SequencedSender,
};
use crate::route::TaskType;

/// 严格盗洞中的物理队列类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrictQueue {
    /// 源主队列在迁移声明前已经存在的任务。
    Stock,
    /// Stolen 路由发布后由新 producer 直接提交的任务。
    Incremental,
    /// ReturnPrepare 后暂时没有 consumer 的新任务。
    Staging,
}

/// 业务作用：为严格迁移的单一阶段保存独立 FIFO 与精确深度账目。
struct StrictFifo {
    sender: SequencedSender<Arc<TaskEnvelope>>,
    receiver: Mutex<SequencedReceiver<Arc<TaskEnvelope>>>,
    depth: AtomicUsize,
}

impl StrictFifo {
    /// 业务作用：创建严格阶段专属的单 consumer 序号 FIFO。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：生产端开放、消费边界为零且深度为零的队列。
    fn new() -> Self {
        let (sender, receiver) = channel();
        Self {
            sender,
            receiver: Mutex::new(receiver),
            depth: AtomicUsize::new(0),
        }
    }

    /// 业务作用：预留严格 FIFO 序号并登记未消费物理槽位，发布前保持
    /// RAII poison 责任。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：队列开放时返回已计入深度的 reservation；关门、序号耗尽或队列
    /// 失权时返回对应原因。
    fn reserve(&self) -> Result<Reservation<Arc<TaskEnvelope>>, ReserveError> {
        let reservation = self.sender.reserve()?;
        // 深度必须在 reservation 移交给调用方前发布，否则 consumer 可能先看到
        // 载荷或 poison 并把尚未增加的计数减到下溢。
        self.depth.fetch_add(1, Ordering::AcqRel);
        Ok(reservation)
    }

    /// 业务作用：reservation 发布完成后读取已在预留点提交的物理深度。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含当前载荷、未发布 reservation 和 poison 的未消费槽位数。
    fn commit_reserved(&self) -> usize {
        self.depth.load(Ordering::Acquire)
    }

    /// 业务作用：reservation 在物理发布前失去队列权威时撤销深度责任，该槽位
    /// 不再能由 consumer 安全取得。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；预留深度被撤销且 FIFO 关门。
    fn abort_reserved(&self) {
        let decremented = self
            .depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_ok();
        debug_assert!(decremented, "strict FIFO depth authority lost");
        self.sender.close();
    }

    /// 业务作用：由唯一目标 worker 按序取得任务或 poison，并归还预留时
    /// 登记的物理深度。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前队头载荷、poison、保留、空队列、关闭或失权状态；只有载荷与
    /// poison 会推进 consumer 并归还深度。
    fn poll(&self) -> ConsumerPoll<Arc<TaskEnvelope>> {
        let result = self
            .receiver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_recv();
        if matches!(
            result,
            ConsumerPoll::Item { .. } | ConsumerPoll::Poisoned { .. }
        ) {
            let decremented = self
                .depth
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_sub(1)
                })
                .is_ok();
            debug_assert!(decremented, "strict FIFO depth authority lost");
        }
        result
    }

    /// 业务作用：关闭新 producer，使停止或归还路径能够取得稳定边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；新 reservation 不再被接纳，已预留前缀仍可排空。
    fn close(&self) {
        self.sender.close();
    }

    /// 业务作用：读取当前尚未由 consumer 取得的预留槽位数，其中包含尚未
    /// 发布的 reservation 与待跨过的 poison。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时刻的非事务性物理槽位快照。
    fn depth(&self) -> usize {
        self.depth.load(Ordering::Acquire)
    }

    /// 业务作用：读取已取得序号但尚未完成发布或 poison 的 producer 数，冻结边界前必须归零。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 reservation 临界区数量。
    fn producer_inflight(&self) -> usize {
        self.sender
            .producer_inflight()
            .saturating_add(self.sender.outstanding_reservations())
    }
}

/// 严格类型唯一盗洞；目标必须先消费 stock，再消费 incremental，归还期间 staging 不被目标执行。
pub(crate) struct StrictTunnel {
    id: u64,
    runner_id: u64,
    generation: u64,
    source: u32,
    target: u32,
    task_type: TaskType,
    accepting: AtomicBool,
    target_execution: AtomicBool,
    activity_epoch: AtomicU64,
    stock: StrictFifo,
    incremental: StrictFifo,
    staging: StrictFifo,
    source_boundary: OnceLock<ProducerBoundary>,
    migration_complete: AtomicBool,
    failure_reason: OnceLock<&'static str>,
}

impl StrictTunnel {
    /// 业务作用：创建同代、单源、单目标的严格盗洞和三条独立序号 FIFO。
    ///
    /// 参数说明：
    /// - `id`: generation 内不可复用盗洞标识。
    /// - `runner_id`: 所属 Runner 标识。
    /// - `generation`: 所属代次。
    /// - `source`: 严格类型原始 slot。
    /// - `target`: 唯一盗洞 consumer。
    /// - `task_type`: 源 slot 内的稳定类型索引。
    ///
    /// 返回：全部 FIFO 为空且开放直投的新盗洞。
    pub(crate) fn new(
        id: u64,
        runner_id: u64,
        generation: u64,
        source: u32,
        target: u32,
        task_type: TaskType,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            runner_id,
            generation,
            source,
            target,
            task_type,
            accepting: AtomicBool::new(true),
            target_execution: AtomicBool::new(true),
            activity_epoch: AtomicU64::new(0),
            stock: StrictFifo::new(),
            incremental: StrictFifo::new(),
            staging: StrictFifo::new(),
            source_boundary: OnceLock::new(),
            migration_complete: AtomicBool::new(false),
            failure_reason: OnceLock::new(),
        })
    }

    /// 业务作用：冻结首次使严格盗洞失去推进证明的原因，供残留任务形成可诊断终态。
    ///
    /// 参数说明：
    /// - `reason`: 调用方已经复验的静态失败原因码。
    ///
    /// 返回：首次登记返回 true；后续调用保留原始原因并返回 false。
    pub(crate) fn record_failure(&self, reason: &'static str) -> bool {
        self.failure_reason.set(reason).is_ok()
    }

    /// 业务作用：读取严格盗洞首次失权原因，使排空路径不会用宽泛原因覆盖根因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：盗洞已经转入失败收口时返回首次原因，否则返回 None。
    pub(crate) fn failure_reason(&self) -> Option<&'static str> {
        self.failure_reason.get().copied()
    }

    /// 业务作用：读取不可复用盗洞标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 内不可复用的盗洞标识。
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// 业务作用：读取所属 Runner 标识，供安装与消费复验隔离域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建盗洞时冻结的 Runner ID。
    pub(crate) fn runner_id(&self) -> u64 {
        self.runner_id
    }

    /// 业务作用：读取所属 generation，供重启后拒绝迟到引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建盗洞时冻结的 generation。
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：读取严格类型原始 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：拥有类型状态与归还落点的源 slot 下标。
    pub(crate) fn source(&self) -> u32 {
        self.source
    }

    /// 业务作用：读取唯一 consumer slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已登记入站盗洞的目标 slot 下标。
    pub(crate) fn target(&self) -> u32 {
        self.target
    }

    /// 业务作用：读取盗洞绑定的稳定任务类型，使目标 worker 无需扫描其它类型即可定位源状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：安装时冻结的 TaskType。
    pub(crate) fn task_type(&self) -> TaskType {
        self.task_type
    }

    /// 业务作用：为指定严格阶段 FIFO 预留唯一物理序号，归还准备后禁止新增 stock/incremental。
    ///
    /// 参数说明：
    /// - `queue`: 当前路由阶段要求的 FIFO。
    ///
    /// 返回：成功返回自动 poison 的 reservation；关闭或失权返回原因。
    pub(crate) fn reserve(
        &self,
        queue: StrictQueue,
    ) -> Result<Reservation<Arc<TaskEnvelope>>, ReserveError> {
        if !self.accepting.load(Ordering::Acquire) && queue != StrictQueue::Staging {
            return Err(ReserveError::Closed);
        }
        match queue {
            StrictQueue::Stock => self.stock.reserve(),
            StrictQueue::Incremental => self.incremental.reserve(),
            StrictQueue::Staging => self.staging.reserve(),
        }
    }

    /// 业务作用：指定 FIFO 的 reservation 已发布后登记活动进展，物理深度
    /// 已在预留线性化点提交。
    ///
    /// 参数说明：
    /// - `queue`: 已完成发布的 FIFO。
    ///
    /// 返回：当前该 FIFO 深度。
    pub(crate) fn commit_reserved(&self, queue: StrictQueue) -> usize {
        self.activity_epoch.fetch_add(1, Ordering::AcqRel);
        match queue {
            StrictQueue::Stock => self.stock.commit_reserved(),
            StrictQueue::Incremental => self.incremental.commit_reserved(),
            StrictQueue::Staging => self.staging.commit_reserved(),
        }
    }

    /// 业务作用：物理发布失权时撤销指定 FIFO 的预留深度并关闭严格直投，
    /// 避免无法消费的槽位被当作存量继续等待。
    ///
    /// 参数说明：
    /// - `queue`: 未完成物理发布的 FIFO。
    ///
    /// 返回：无；对应 FIFO 关门，上层必须转入类型失败收口。
    pub(crate) fn abort_reserved(&self, queue: StrictQueue) {
        match queue {
            StrictQueue::Stock => self.stock.abort_reserved(),
            StrictQueue::Incremental => self.incremental.abort_reserved(),
            StrictQueue::Staging => self.staging.abort_reserved(),
        }
        self.close_all();
    }

    /// 业务作用：由目标 worker 按 stock 优先于 incremental 的顺序取得可执行任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前可执行队头；stock 有保留槽时不越过到 incremental。
    pub(crate) fn poll_executable(&self) -> ConsumerPoll<Arc<TaskEnvelope>> {
        if !self.target_execution.load(Ordering::Acquire) {
            return ConsumerPoll::Empty;
        }
        match self.stock.poll() {
            ConsumerPoll::Empty | ConsumerPoll::Closed
                if self.migration_complete.load(Ordering::Acquire) =>
            {
                self.incremental.poll()
            }
            ConsumerPoll::Empty | ConsumerPoll::Closed => ConsumerPoll::Empty,
            other => other,
        }
    }

    /// 业务作用：旧 Local producer 全部离场后冻结源主队列边界，迟到帮助者不能替换该值。
    ///
    /// 参数说明：
    /// - `boundary`: 所有可能按旧 Local 路由发布的排他上界。
    ///
    /// 返回：首次安装或与既有边界一致时返回 true；不一致返回 false。
    pub(crate) fn install_source_boundary(&self, boundary: ProducerBoundary) -> bool {
        match self.source_boundary.set(boundary) {
            Ok(()) => true,
            Err(current) => current == boundary,
        }
    }

    /// 业务作用：源 consumer 到达冻结边界后发布迁移完成，目标才能从 incremental 执行。
    ///
    /// 参数说明：
    /// - `consumer`: 源主队列当前 consumer 边界。
    ///
    /// 返回：边界已安装且 consumer 到达时发布完成并返回 true。
    pub(crate) fn complete_migration(&self, consumer: ConsumerBoundary) -> bool {
        let Some(boundary) = self.source_boundary.get().copied() else {
            return false;
        };
        if !consumer.reached(boundary) {
            return false;
        }
        self.migration_complete.store(true, Ordering::Release);
        true
    }

    /// 业务作用：由归还责任方从指定 FIFO 取得任务；每条 FIFO 仍只有一个现役 consumer。
    ///
    /// 参数说明：
    /// - `queue`: 归还阶段正在排空的 FIFO。
    ///
    /// 返回：对应队头状态。
    pub(crate) fn poll_return(&self, queue: StrictQueue) -> ConsumerPoll<Arc<TaskEnvelope>> {
        match queue {
            StrictQueue::Stock => self.stock.poll(),
            StrictQueue::Incremental => self.incremental.poll(),
            StrictQueue::Staging => self.staging.poll(),
        }
    }

    /// 业务作用：类型失败或有损停止后按 stock、incremental、staging 的固定次序取得残留
    /// 条目，唯一目标 worker 据此发布失败终态而不是让关闭队列永久阻塞 join。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次检查的 FIFO 类别与队头状态；前一 FIFO 有 reservation 时不会越过。
    pub(crate) fn poll_failure(&self) -> (StrictQueue, ConsumerPoll<Arc<TaskEnvelope>>) {
        let stock = self.stock.poll();
        match stock {
            ConsumerPoll::Empty | ConsumerPoll::Closed => {}
            other => return (StrictQueue::Stock, other),
        }
        let incremental = self.incremental.poll();
        match incremental {
            ConsumerPoll::Empty | ConsumerPoll::Closed => {}
            other => return (StrictQueue::Incremental, other),
        }
        (StrictQueue::Staging, self.staging.poll())
    }

    /// 业务作用：进入归还准备后关闭 stock 与 incremental 新 producer，staging 保持接纳现役路由任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；旧直投方向关门，已预留前缀仍可排空。
    pub(crate) fn begin_return(&self) {
        self.accepting.store(false, Ordering::Release);
        self.stock.close();
        self.incremental.close();
    }

    /// 业务作用：进入可逆归还准备时先关闭目标消费权，让旧直投前缀保持可冻结；此时不关闭
    /// direct producer，撤销归还仍可继续使用原盗洞。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；调用方必须已经发布 ReturnPrepare 路由。
    pub(crate) fn begin_return_prepare(&self) {
        self.target_execution.store(false, Ordering::Release);
    }

    /// 业务作用：可逆归还撤销且 staging 已追赶完毕后恢复目标消费权，重新开放 Stolen 执行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；direct producer 始终保持开放。
    pub(crate) fn resume_stolen_execution(&self) {
        self.target_execution.store(true, Ordering::Release);
    }

    /// 业务作用：路由已切到 LocalCatchup 后关闭 staging 新 producer，随后等待现有 reservation 离场。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；已发布 staging 前缀仍由归还责任方排空。
    pub(crate) fn begin_staging_drain(&self) {
        self.staging.close();
    }

    /// 业务作用：停止或完成归还时关闭全部 FIFO producer。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；stock、incremental 与 staging 都不再接纳新 reservation。
    pub(crate) fn close_all(&self) {
        self.accepting.store(false, Ordering::Release);
        self.stock.close();
        self.incremental.close();
        self.staging.close();
    }

    /// 业务作用：读取三条 FIFO 的总物理深度，供停止与归还收敛判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：stock、incremental 与 staging 未消费槽位数之和。
    pub(crate) fn depth(&self) -> usize {
        self.stock.depth() + self.incremental.depth() + self.staging.depth()
    }

    /// 业务作用：分别读取严格存量、增量和归还暂存 FIFO 的物理深度，观测侧不得据此
    /// 改写路由或消费任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 stock、incremental、staging 顺序排列的深度。
    pub(crate) fn queue_depths(&self) -> (usize, usize, usize) {
        (
            self.stock.depth(),
            self.incremental.depth(),
            self.staging.depth(),
        )
    }

    /// 业务作用：汇总三条严格 FIFO 已登记但尚未离场的 producer，用于证明冻结边界前不存在
    /// 未计入的合法发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：stock、incremental 与 staging producer 临界区数量之和。
    pub(crate) fn producer_inflight(&self) -> usize {
        self.stock
            .producer_inflight()
            .saturating_add(self.incremental.producer_inflight())
            .saturating_add(self.staging.producer_inflight())
    }

    /// 业务作用：判断目标可执行的 stock 与 incremental 是否已经排空，staging 不参与目标消费。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两条目标执行 FIFO 深度都为零时返回 true。
    pub(crate) fn executable_empty(&self) -> bool {
        self.stock.depth() == 0 && self.incremental.depth() == 0
    }

    /// 业务作用：确认 stock 与 incremental 已无在途 producer，目标可冻结旧直投边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两条旧直投 FIFO 的 reservation 全部离场时返回 true。
    pub(crate) fn direct_producers_stopped(&self) -> bool {
        self.stock.producer_inflight() == 0 && self.incremental.producer_inflight() == 0
    }

    /// 业务作用：确认 staging 已关门且全部旧 producer 完成发布，可开始稳定排空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：staging reservation 全部离场时返回 true。
    pub(crate) fn staging_producers_stopped(&self) -> bool {
        self.staging.producer_inflight() == 0
    }

    /// 业务作用：读取 staging 当前物理深度，归还准备据此发现新活动并撤销尚未回写的归还。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未由唯一归还 consumer 取得的暂存任务数。
    pub(crate) fn staging_depth(&self) -> usize {
        self.staging.depth()
    }

    /// 业务作用：读取盗洞发布活动代次，连续空闲观察之间出现快速任务也会改变该值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 内单调活动序号。
    pub(crate) fn activity_epoch(&self) -> u64 {
        self.activity_epoch.load(Ordering::Acquire)
    }
}
