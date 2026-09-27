//! 非严格租约盗洞。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::entry::TaskEnvelope;
use crate::queue::{
    channel, ConsumerPoll, Reservation, ReserveError, SequencedReceiver, SequencedSender,
};
use crate::route::TaskType;

/// 非严格任务从一个源 slot 流向一个目标 slot 的租约通道。
pub(crate) struct NonStrictTunnel {
    id: u64,
    runner_id: u64,
    generation: u64,
    source: u32,
    target: u32,
    task_type: TaskType,
    sender: SequencedSender<Arc<TaskEnvelope>>,
    receiver: Mutex<SequencedReceiver<Arc<TaskEnvelope>>>,
    accepting: AtomicBool,
    depth: AtomicUsize,
    lease_nanos: u64,
    expires_at_nanos: AtomicU64,
    clock_origin: Instant,
}

impl NonStrictTunnel {
    /// 业务作用：创建同代、单目标、单 consumer 的非严格盗洞，并从当前时刻启动租约。
    ///
    /// 参数说明：
    /// - `id`: generation 内不可复用盗洞标识。
    /// - `runner_id`: 所属 Runner 标识。
    /// - `generation`: 所属代次。
    /// - `source`: 热点来源 slot。
    /// - `target`: 唯一 consumer slot。
    /// - `task_type`: 源 slot 内不可变任务类型。
    /// - `lease`: 无进展时允许保持开放的时长。
    ///
    /// 返回：可由 producer 和目标 worker 共享的新盗洞。
    pub(crate) fn new(
        id: u64,
        runner_id: u64,
        generation: u64,
        source: u32,
        target: u32,
        task_type: TaskType,
        lease: Duration,
    ) -> Arc<Self> {
        let (sender, receiver) = channel();
        let lease_nanos = u64::try_from(lease.as_nanos()).unwrap_or(u64::MAX);
        Arc::new(Self {
            id,
            runner_id,
            generation,
            source,
            target,
            task_type,
            sender,
            receiver: Mutex::new(receiver),
            accepting: AtomicBool::new(true),
            depth: AtomicUsize::new(0),
            lease_nanos,
            expires_at_nanos: AtomicU64::new(lease_nanos),
            clock_origin: Instant::now(),
        })
    }

    /// 业务作用：读取不可复用盗洞标识，供目标入站表和指标关联。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 generation 内不可复用的盗洞标识。
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// 业务作用：读取所属 Runner 标识，阻止跨执行域安装。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建盗洞时冻结的 Runner ID。
    pub(crate) fn runner_id(&self) -> u64 {
        self.runner_id
    }

    /// 业务作用：读取所属代次，阻止重启后的迟到 producer 发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：创建盗洞时冻结的 generation。
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// 业务作用：读取热点来源 slot。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：拥有类型状态与出站路由的源 slot 下标。
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

    /// 业务作用：读取本盗洞绑定的稳定任务类型，关闭时只撤销对应源路由。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：安装时冻结的 TaskType。
    pub(crate) fn task_type(&self) -> TaskType {
        self.task_type
    }

    /// 业务作用：判断盗洞是否仍接纳新任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：producer 门禁开放且租约未到期时返回 true。
    pub(crate) fn accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire) && !self.expired()
    }

    /// 业务作用：预留目标 FIFO 序号并登记未消费槽位，使任务 owner、逻辑计数
    /// 与稳定状态能在物理发布前完成。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：盗洞开放时返回自动 poison 的 reservation；关闭或失权返回队列原因。
    pub(crate) fn reserve(&self) -> Result<Reservation<Arc<TaskEnvelope>>, ReserveError> {
        if !self.accepting() {
            return Err(ReserveError::Closed);
        }
        let reservation = self.sender.reserve()?;
        // 深度在 reservation 可被另一线程发布或析构前先可见，保证 consumer 对
        // 载荷和 poison 的每次减计都有唯一对应的预留增计。
        self.depth.fetch_add(1, Ordering::AcqRel);
        Ok(reservation)
    }

    /// 业务作用：reservation 已携带任务发布后登记租约进展，物理深度已在
    /// 预留线性化点提交。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前物理深度。
    pub(crate) fn commit_reserved(&self) -> usize {
        self.renew();
        self.depth.load(Ordering::Acquire)
    }

    /// 业务作用：reservation 在物理发布前失去队列权威时撤销深度责任并关门，
    /// 避免无法消费的槽位阻塞盗洞退役。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；调用后不再接纳新 producer。
    pub(crate) fn abort_reserved(&self) {
        let decremented = self
            .depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .is_ok();
        debug_assert!(decremented, "non-strict FIFO depth authority lost");
        self.close();
    }

    /// 业务作用：由目标 worker 按序取得一笔任务；只有目标 worker 可以调用本方法。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：任务、等待状态或队列失败结果。
    pub(crate) fn poll(&self) -> ConsumerPoll<Arc<TaskEnvelope>> {
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
            debug_assert!(decremented, "non-strict FIFO depth authority lost");
            self.renew();
        }
        result
    }

    /// 业务作用：关闭新 producer；已发布前缀仍由目标 worker 排空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；关门状态单向保持。
    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        self.sender.close();
    }

    /// 业务作用：租约到期后单向关闭 producer；已有队列前缀仍由目标唯一 consumer 排空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本次观察后盗洞已关闭时返回 true。
    pub(crate) fn close_if_expired(&self) -> bool {
        if self.expired() {
            self.close();
        }
        !self.accepting.load(Ordering::Acquire)
    }

    /// 业务作用：判断关闭盗洞是否已经没有在途 producer 和物理任务，可安全撤销两侧引用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已关闭、深度为零且 producer reservation 全部离场时返回 true。
    pub(crate) fn drained(&self) -> bool {
        !self.accepting.load(Ordering::Acquire)
            && self.depth() == 0
            && self.sender.producer_inflight() == 0
            && self.sender.outstanding_reservations() == 0
    }

    /// 业务作用：读取当前未消费预留槽位数快照，其中包含待发布槽位与
    /// poison，供租约关闭和停机收敛判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时刻的非事务性物理槽位数。
    pub(crate) fn depth(&self) -> usize {
        self.depth.load(Ordering::Acquire)
    }

    /// 业务作用：读取已进入发布临界区但尚未完整离场的 producer 数，关闭与指标路径据此
    /// 区分空队列和仍可能出现合法迟到发布的队列。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：序号分配临界区与跨调用 reservation 的合计数量。
    pub(crate) fn producer_inflight(&self) -> usize {
        self.sender
            .producer_inflight()
            .saturating_add(self.sender.outstanding_reservations())
    }

    /// 业务作用：在真实发布或消费进展后延长租约，防止活跃盗洞被观察任务误关。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；到期刻度更新为当前单调时间加租期。
    fn renew(&self) {
        let now = self.elapsed_nanos();
        self.expires_at_nanos
            .store(now.saturating_add(self.lease_nanos), Ordering::Release);
    }

    /// 业务作用：判断租约是否到期；到期只阻止新发布，存量仍必须排空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前单调刻度已达到或超过到期刻度时返回 true。
    fn expired(&self) -> bool {
        self.elapsed_nanos() >= self.expires_at_nanos.load(Ordering::Acquire)
    }

    /// 业务作用：把单调时钟转换为本盗洞局部纳秒刻度，溢出时保持最大值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：自盗洞创建起的纳秒数，不可表示时为 `u64::MAX`。
    fn elapsed_nanos(&self) -> u64 {
        u64::try_from(self.clock_origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}
