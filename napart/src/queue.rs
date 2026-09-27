//! 单 consumer 分段序号 MPSC。
//!
//! producer 先取得不可复用的全局序号，再在对应槽位发布载荷或 poison。consumer 只能按
//! 序号前进，遇到尚未发布的保留槽必须停止，不能越过后继任务破坏顺序边界。

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

const SEGMENT_LEN: u64 = 64;

const SLOT_EMPTY: u8 = 0;
const SLOT_RESERVED: u8 = 1;
const SLOT_WRITING: u8 = 2;
const SLOT_PUBLISHED: u8 = 3;
const SLOT_POISONED: u8 = 4;
const SLOT_TAKEN: u8 = 5;

/// 唯一 consumer 对当前队头的无副作用状态快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerHeadState {
    /// 尚无 producer 取得当前序号。
    Empty,
    /// 当前序号正在预留或写入，不能越过。
    Reserved,
    /// 当前序号已经发布业务载荷。
    Published,
    /// 当前序号由离场 producer 发布为 poison。
    Poisoned,
    /// 队列已关闭且全部已分配序号都已消费。
    Closed,
    /// 槽位或队列权威损坏，正常消费必须停止。
    Failed,
}

/// 关闭 producer 后排空队列的结果摘要。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainSummary {
    items: u64,
    poisoned: u64,
}

impl DrainSummary {
    /// 业务作用：读取本轮物理排空交给清理者的业务载荷数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功消费并调用清理闭包的载荷数。
    pub(crate) fn items(self) -> u64 {
        self.items
    }

    /// 业务作用：读取本轮物理排空跨过的 poison 槽位数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：没有业务载荷但占用过全局序号的槽位数。
    pub(crate) fn poisoned(self) -> u64 {
        self.poisoned
    }
}

/// 关门排空尚不能取得完整物理收口证明的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainError {
    /// 仍有 producer 位于接纳复验或序号预留临界区。
    ProducersActive { count: usize },
    /// 已取得序号的 reservation 尚未发布载荷或 poison。
    HeadReserved { sequence: u64 },
    /// 槽位内容与单 consumer 游标不一致。
    Failed { sequence: u64 },
}

/// producer 边界：`exclusive` 之前的序号都已经被某个 producer 取得。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerBoundary {
    exclusive: u64,
}

impl ProducerBoundary {
    /// 业务作用：读取边界的排他序号，用于迁移和归还流程封存 producer 前缀。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：边界之后第一个尚未被纳入前缀的序号。
    pub(crate) fn exclusive(self) -> u64 {
        self.exclusive
    }
}

/// consumer 边界：`next` 是唯一 consumer 下一次必须处理的序号。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConsumerBoundary {
    next: u64,
}

impl ConsumerBoundary {
    /// 业务作用：读取 consumer 游标，供严格迁移判断指定 producer 前缀是否已经完整消费。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：下一条尚未消费的序号。
    pub(crate) fn next(self) -> u64 {
        self.next
    }

    /// 业务作用：判断 consumer 是否已经跨过指定 producer 排他边界。
    ///
    /// 参数说明：
    /// - `boundary`: 需要完整消费的 producer 前缀。
    ///
    /// 返回：consumer 已处理边界前全部槽位时返回 true。
    pub(crate) fn reached(self, boundary: ProducerBoundary) -> bool {
        self.next >= boundary.exclusive
    }
}

/// producer 预留失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReserveError {
    /// 队列已经关闭，不再接受新 producer。
    Closed,
    /// 全局序号耗尽；继续回绕会把旧槽位误当成新任务。
    SequenceExhausted,
    /// 队列内部状态已经失去唯一性证明。
    Failed,
}

/// 发布失败，携带尚未交给队列的业务载荷。
pub(crate) struct PublishError<T> {
    value: T,
}

impl<T> PublishError<T> {
    /// 业务作用：取回未发布载荷，使调用方能够在拒绝路径安全结算业务对象。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：从未进入物理队列的原始载荷。
    pub(crate) fn into_inner(self) -> T {
        self.value
    }
}

/// 唯一 consumer 对队头的观察结果。
pub(crate) enum ConsumerPoll<T> {
    /// 当前没有已预留序号；队列仍可能接收后续任务。
    Empty,
    /// 队头序号已经预留但尚未发布，consumer 不得越过。
    Reserved { sequence: u64 },
    /// 取得按序发布的业务载荷。
    Item { sequence: u64, value: T },
    /// producer 在发布前离场，poison 使 consumer 可以安全跨过该序号。
    Poisoned { sequence: u64 },
    /// 队列关闭且全部已预留序号均已处理。
    Closed,
    /// 内部状态冲突，所属故障域必须关闭并转入失败收口。
    Failed,
}

/// 业务作用：保存单个全局序号的发布状态与至多一个业务载荷。
struct Slot<T> {
    state: AtomicU8,
    value: Mutex<Option<T>>,
}

impl<T> Slot<T> {
    /// 业务作用：创建尚未被任何 producer 取得的物理槽位。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：状态为 EMPTY、载荷未初始化的槽位。
    fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            value: Mutex::new(None),
        }
    }
}

/// 业务作用：把连续序号映射到固定数量槽位，支持队列按段回收而不移动存量任务。
struct Segment<T> {
    base: u64,
    slots: Box<[Slot<T>]>,
}

impl<T> Segment<T> {
    /// 业务作用：创建覆盖一个连续序号区间的分段，避免为每条任务单独分配队列节点。
    ///
    /// 参数说明：
    /// - `base`: 分段起始序号，必须按 `SEGMENT_LEN` 对齐。
    ///
    /// 返回：全部槽位为空的新分段。
    fn new(base: u64) -> Self {
        debug_assert_eq!(base % SEGMENT_LEN, 0);
        let slots = (0..SEGMENT_LEN)
            .map(|_| Slot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { base, slots }
    }

    /// 业务作用：按全局序号定位本分段中的唯一物理槽位。
    ///
    /// 参数说明：
    /// - `sequence`: 必须属于本分段的全局序号。
    ///
    /// 返回：对应槽位的共享引用。
    fn slot(&self, sequence: u64) -> &Slot<T> {
        debug_assert!(sequence >= self.base && sequence < self.base + SEGMENT_LEN);
        &self.slots[(sequence - self.base) as usize]
    }
}

/// 业务作用：集中拥有序号分配、producer 临界计数、分段索引与单向失败状态。
struct QueueCore<T> {
    next_sequence: AtomicU64,
    accepting: AtomicBool,
    producer_inflight: AtomicUsize,
    reservations: AtomicUsize,
    senders: AtomicUsize,
    failed: AtomicBool,
    tail: ArcSwap<Segment<T>>,
    segments: Mutex<BTreeMap<u64, Arc<Segment<T>>>>,
}

impl<T> QueueCore<T> {
    /// 业务作用：创建序号从零开始、允许 producer 接入的队列内核。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：含首个空分段的共享内核。
    fn new() -> Self {
        let first = Arc::new(Segment::new(0));
        let mut segments = BTreeMap::new();
        segments.insert(0, first.clone());
        Self {
            next_sequence: AtomicU64::new(0),
            accepting: AtomicBool::new(true),
            producer_inflight: AtomicUsize::new(0),
            reservations: AtomicUsize::new(0),
            senders: AtomicUsize::new(1),
            failed: AtomicBool::new(false),
            tail: ArcSwap::from(first),
            segments: Mutex::new(segments),
        }
    }

    /// 业务作用：为一次提交线性化分配不可复用的全局序号。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：队列开放时返回新序号；关闭、失权或序号耗尽时明确拒绝。
    fn allocate_sequence(&self) -> Result<u64, ReserveError> {
        loop {
            if self.failed.load(Ordering::Acquire) {
                return Err(ReserveError::Failed);
            }
            if !self.accepting.load(Ordering::SeqCst) {
                return Err(ReserveError::Closed);
            }
            let current = self.next_sequence.load(Ordering::Acquire);
            if current == u64::MAX {
                self.failed.store(true, Ordering::Release);
                self.accepting.store(false, Ordering::SeqCst);
                return Err(ReserveError::SequenceExhausted);
            }
            if self
                .next_sequence
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(current);
            }
        }
    }

    /// 业务作用：登记一次序号预留临界区，并在关门竞争下复验接纳权，保证关闭方能够等待
    /// 已越过首检的 producer 全部离场。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：接纳权仍有效时返回自动离场 guard；已经关闭时返回 Closed。
    fn enter_producer(&self) -> Result<ProducerSection<'_, T>, ReserveError> {
        if !self.accepting.load(Ordering::SeqCst) {
            return Err(ReserveError::Closed);
        }
        // 接纳位与 inflight 位于不同原子上，使用同一全局序防止关门方和 producer 各自
        // 只观察到对方动作之前的值，进而把迟到序号遗漏在最终边界之外。
        self.producer_inflight.fetch_add(1, Ordering::SeqCst);
        if !self.accepting.load(Ordering::SeqCst) {
            self.producer_inflight.fetch_sub(1, Ordering::SeqCst);
            return Err(ReserveError::Closed);
        }
        Ok(ProducerSection { core: self })
    }

    /// 业务作用：序号槽位进入 RESERVED 后登记跨调用 reservation 责任；关门方必须同时
    /// 等待分配临界区和这些未 publish/poison 的能力离场。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：计数仍可表示时成功；耗尽时关闭队列并返回 Failed。
    fn register_reservation(&self) -> Result<(), ReserveError> {
        if self
            .reservations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .is_err()
        {
            self.fail();
            return Err(ReserveError::Failed);
        }
        Ok(())
    }

    /// 业务作用：reservation 完成 publish 或 poison 后恰好归还一次跨调用责任；下溢表示
    /// 队列权威损坏并立即关门。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：正常归还返回 true；重复归还返回 false。
    fn finish_reservation(&self) -> bool {
        if self
            .reservations
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

    /// 业务作用：取得序号所属分段；并发跨段时只创建一个同 base 分段并原子推进 tail。
    ///
    /// 参数说明：
    /// - `sequence`: 已经线性化取得的 producer 序号。
    ///
    /// 返回：覆盖该序号的唯一共享分段。
    fn segment_for_producer(&self, sequence: u64) -> Arc<Segment<T>> {
        let base = sequence / SEGMENT_LEN * SEGMENT_LEN;
        let tail = self.tail.load_full();
        if tail.base == base {
            return tail;
        }

        let mut segments = self.segments.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(segment) = segments.get(&base) {
            return segment.clone();
        }
        let segment = Arc::new(Segment::new(base));
        segments.insert(base, segment.clone());
        let current_tail = self.tail.load_full();
        if base > current_tail.base {
            self.tail.store(segment.clone());
        }
        segment
    }

    /// 业务作用：为唯一 consumer 查找当前序号分段；producer 尚未建立跨段结构时保持等待，
    /// 不能越过该序号。
    ///
    /// 参数说明：
    /// - `sequence`: consumer 下一条必须处理的序号。
    ///
    /// 返回：分段已存在时返回共享引用，否则返回 None 表示队头仍保留中。
    fn segment_for_consumer(&self, sequence: u64) -> Option<Arc<Segment<T>>> {
        let base = sequence / SEGMENT_LEN * SEGMENT_LEN;
        let tail = self.tail.load_full();
        if tail.base == base {
            return Some(tail);
        }
        self.segments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&base)
            .cloned()
    }

    /// 业务作用：回收 consumer 已完整跨过且不再作为当前 tail 的旧分段，限制物理队列内存
    /// 与活动序号跨度一致。
    ///
    /// 参数说明：
    /// - `consumer_sequence`: consumer 下一条序号；此前分段已没有未处理槽位。
    ///
    /// 返回：无；不影响当前 tail 与尚未消费分段。
    fn reclaim_before(&self, consumer_sequence: u64) {
        if consumer_sequence == 0 || !consumer_sequence.is_multiple_of(SEGMENT_LEN) {
            return;
        }
        let tail_base = self.tail.load().base;
        let mut segments = self.segments.lock().unwrap_or_else(|e| e.into_inner());
        segments.retain(|base, _| *base >= consumer_sequence || *base == tail_base);
    }

    /// 业务作用：把无法解释的槽位竞争升级为队列失败并关闭新 producer，避免继续产生
    /// 无法按序证明的任务。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；失败状态单向保持。
    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.accepting.store(false, Ordering::SeqCst);
    }
}

/// 业务作用：用 RAII 表示一次 producer 序号预留临界区，保证关闭方最终观察到零在途提交。
struct ProducerSection<'a, T> {
    core: &'a QueueCore<T>,
}

impl<T> Drop for ProducerSection<'_, T> {
    /// 业务作用：序号预留路径离场时归还 producer 临界计数，使关闭方能够取得最终边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；对应临界计数恰好归还一次。
    fn drop(&mut self) {
        self.core.producer_inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 可克隆的 MPSC producer 句柄。
pub(crate) struct SequencedSender<T> {
    core: Arc<QueueCore<T>>,
}

impl<T> Clone for SequencedSender<T> {
    /// 业务作用：创建同一队列的 producer 句柄并登记 producer 生命周期。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享同一序号域的新 sender。
    fn clone(&self) -> Self {
        self.core.senders.fetch_add(1, Ordering::AcqRel);
        Self {
            core: self.core.clone(),
        }
    }
}

impl<T> Drop for SequencedSender<T> {
    /// 业务作用：producer 句柄离场；最后一个 sender 关闭新预留，使 consumer 排空后观察
    /// 到稳定 Closed。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；最后一个 sender 离场时单向关闭新预留。
    fn drop(&mut self) {
        if self.core.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.core.accepting.store(false, Ordering::SeqCst);
        }
    }
}

impl<T> SequencedSender<T> {
    /// 业务作用：预留下一全局序号并把槽位标记为 RESERVED，供需要跨步骤发布的 producer
    /// 建立取消安全边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功时返回析构自动 poison 的 reservation；队列关闭或失权时返回明确原因。
    pub(crate) fn reserve(&self) -> Result<Reservation<T>, ReserveError> {
        let producer = self.core.enter_producer()?;
        let sequence = self.core.allocate_sequence()?;
        let segment = self.core.segment_for_producer(sequence);
        let slot = segment.slot(sequence);
        if slot
            .state
            .compare_exchange(
                SLOT_EMPTY,
                SLOT_RESERVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.core.fail();
            return Err(ReserveError::Failed);
        }
        if let Err(error) = self.core.register_reservation() {
            slot.state.store(SLOT_POISONED, Ordering::Release);
            return Err(error);
        }
        drop(producer);
        Ok(Reservation {
            core: self.core.clone(),
            segment,
            sequence,
            active: true,
        })
    }

    /// 业务作用：在单次调用内完成序号预留和载荷发布，供不需要跨步骤控制的 producer 使用。
    ///
    /// 参数说明：
    /// - `value`: 需要按全局序号交给唯一 consumer 的载荷。
    ///
    /// 返回：成功时返回分配序号；预留失败或内部失权时返回原载荷与原因。
    pub(crate) fn offer(&self, value: T) -> Result<u64, OfferError<T>> {
        let reservation = match self.reserve() {
            Ok(reservation) => reservation,
            Err(reason) => return Err(OfferError { reason, value }),
        };
        reservation.publish(value).map_err(|failure| OfferError {
            reason: ReserveError::Failed,
            value: failure.into_inner(),
        })
    }

    /// 业务作用：封存当前 producer 排他边界，供严格迁移等待该前缀完整发布和消费。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：调用时已经取得序号的 producer 前缀边界。
    pub(crate) fn boundary(&self) -> ProducerBoundary {
        ProducerBoundary {
            exclusive: self.core.next_sequence.load(Ordering::Acquire),
        }
    }

    /// 业务作用：读取正在序号预留临界区内的 producer 数，供关闭方在封存最终边界前等待
    /// 已越过首检的提交全部离场。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前序号分配临界区数量；跨调用 reservation 由
    /// `outstanding_reservations` 独立报告。
    pub(crate) fn producer_inflight(&self) -> usize {
        self.core.producer_inflight.load(Ordering::SeqCst)
    }

    /// 业务作用：读取已经取得物理序号但尚未 publish 或 poison 的 reservation 数；撤销
    /// 盗洞或冻结最终物理边界前必须等待该值归零。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前跨调用 reservation 责任数。
    pub(crate) fn outstanding_reservations(&self) -> usize {
        self.core.reservations.load(Ordering::Acquire)
    }

    /// 业务作用：关闭新 producer 预留；已经取得序号的 reservation 仍必须发布或 poison。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；关闭状态不可逆。
    pub(crate) fn close(&self) {
        self.core.accepting.store(false, Ordering::SeqCst);
    }

    /// 业务作用：读取队列是否已经关闭或失败，供提交路径提前返回稳定拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不再接受新 reservation 时返回 true。
    pub(crate) fn is_closed(&self) -> bool {
        !self.core.accepting.load(Ordering::SeqCst)
    }

    /// 业务作用：读取当前活动物理分段数，用于观测分段队列的资源边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分段注册表中的当前条目数。
    pub(crate) fn segment_count(&self) -> usize {
        self.core
            .segments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

/// offer 失败，保留拒绝原因与未发布业务载荷。
pub(crate) struct OfferError<T> {
    reason: ReserveError,
    value: T,
}

impl<T> OfferError<T> {
    /// 业务作用：读取提交被拒的稳定原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：关闭、序号耗尽或队列失权原因。
    pub(crate) fn reason(&self) -> ReserveError {
        self.reason
    }

    /// 业务作用：取回从未进入队列的业务载荷。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原始载荷。
    pub(crate) fn into_inner(self) -> T {
        self.value
    }
}

/// 已取得序号但尚未发布的 producer 权威；离场时自动 poison 对应槽位。
pub(crate) struct Reservation<T> {
    core: Arc<QueueCore<T>>,
    segment: Arc<Segment<T>>,
    sequence: u64,
    active: bool,
}

impl<T> Reservation<T> {
    /// 业务作用：读取本 reservation 的全局序号，用于构造迁移描述符和边界证据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不可复用的全局序号。
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    /// 业务作用：把载荷写入唯一槽位并以 Release 语义发布，使 consumer 观察状态后可以安全读取。
    ///
    /// 参数说明：
    /// - `value`: 与本 reservation 序号绑定的业务载荷。
    ///
    /// 返回：成功时返回序号；reservation 已失权时返回从未发布的载荷。
    pub(crate) fn publish(mut self, value: T) -> Result<u64, PublishError<T>> {
        let slot = self.segment.slot(self.sequence);
        if slot
            .state
            .compare_exchange(
                SLOT_RESERVED,
                SLOT_WRITING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.active = false;
            self.core.finish_reservation();
            self.core.fail();
            return Err(PublishError { value });
        }
        // 本 reservation 是该槽位唯一写者，WRITING 阻止 consumer 读取；每槽位互斥锁
        // 只保护载荷移动，不参与队列全局排序，因此并发 producer 不共享热锁。
        *slot.value.lock().unwrap_or_else(|e| e.into_inner()) = Some(value);
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);
        self.active = false;
        self.core.finish_reservation();
        Ok(self.sequence)
    }
}

impl<T> Drop for Reservation<T> {
    /// 业务作用：producer 在发布前取消或展开离场时发布 poison，使唯一 consumer 能跨过已
    /// 分配序号而不越过未知状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；活动 reservation 发布 poison 并归还责任，已 publish 的对象不再动作。
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let slot = self.segment.slot(self.sequence);
        if slot
            .state
            .compare_exchange(
                SLOT_RESERVED,
                SLOT_POISONED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            // poison 发布失败表示槽位权威已经被其它路径改写；继续消费会失去一次性证明。
            self.core.fail();
        }
        self.core.finish_reservation();
        self.active = false;
    }
}

/// 唯一 consumer 句柄；类型不实现 Clone，从接口层阻止多个 consumer 并发推进游标。
pub(crate) struct SequencedReceiver<T> {
    core: Arc<QueueCore<T>>,
    next_sequence: u64,
}

impl<T> SequencedReceiver<T> {
    /// 业务作用：按序观察并推进唯一 consumer；保留槽未发布时必须停止，poison 可以安全跨过。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：队头载荷、保留/poison/关闭状态或队列失权结果。
    pub(crate) fn try_recv(&mut self) -> ConsumerPoll<T> {
        if self.core.failed.load(Ordering::Acquire) {
            return ConsumerPoll::Failed;
        }
        let reserved = self.core.next_sequence.load(Ordering::Acquire);
        if self.next_sequence >= reserved {
            return if self.core.accepting.load(Ordering::SeqCst) {
                ConsumerPoll::Empty
            } else if self.core.producer_inflight.load(Ordering::SeqCst) > 0 {
                ConsumerPoll::Reserved {
                    sequence: self.next_sequence,
                }
            } else {
                ConsumerPoll::Closed
            };
        }

        self.core.reclaim_before(self.next_sequence);
        let Some(segment) = self.core.segment_for_consumer(self.next_sequence) else {
            return ConsumerPoll::Reserved {
                sequence: self.next_sequence,
            };
        };
        let sequence = self.next_sequence;
        let slot = segment.slot(sequence);
        match slot.state.load(Ordering::Acquire) {
            SLOT_EMPTY | SLOT_RESERVED | SLOT_WRITING => ConsumerPoll::Reserved { sequence },
            SLOT_PUBLISHED => {
                // PUBLISHED 的 Acquire 已观察 producer 对载荷的初始化。唯一 consumer 从
                // 对应槽位移出载荷，再推进为 TAKEN，分段析构不会重复释放。
                let Some(value) = slot.value.lock().unwrap_or_else(|e| e.into_inner()).take()
                else {
                    self.core.fail();
                    return ConsumerPoll::Failed;
                };
                slot.state.store(SLOT_TAKEN, Ordering::Release);
                self.next_sequence += 1;
                ConsumerPoll::Item { sequence, value }
            }
            SLOT_POISONED => {
                slot.state.store(SLOT_TAKEN, Ordering::Release);
                self.next_sequence += 1;
                ConsumerPoll::Poisoned { sequence }
            }
            _ => {
                self.core.fail();
                ConsumerPoll::Failed
            }
        }
    }

    /// 业务作用：读取唯一 consumer 当前边界，供迁移与归还判断前缀是否已经完整跨过。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：下一条必须处理的序号。
    pub(crate) fn boundary(&self) -> ConsumerBoundary {
        ConsumerBoundary {
            next: self.next_sequence,
        }
    }

    /// 业务作用：无副作用观察当前队头，供 worker 决定继续消费、让出、等待或关闭故障域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前 consumer 序号对应的封闭状态快照。
    pub(crate) fn head_state(&self) -> ConsumerHeadState {
        if self.core.failed.load(Ordering::Acquire) {
            return ConsumerHeadState::Failed;
        }
        let reserved = self.core.next_sequence.load(Ordering::Acquire);
        if self.next_sequence >= reserved {
            return if self.core.accepting.load(Ordering::SeqCst) {
                ConsumerHeadState::Empty
            } else if self.core.producer_inflight.load(Ordering::SeqCst) > 0 {
                ConsumerHeadState::Reserved
            } else {
                ConsumerHeadState::Closed
            };
        }
        let Some(segment) = self.core.segment_for_consumer(self.next_sequence) else {
            return ConsumerHeadState::Reserved;
        };
        match segment
            .slot(self.next_sequence)
            .state
            .load(Ordering::Acquire)
        {
            SLOT_EMPTY | SLOT_RESERVED | SLOT_WRITING => ConsumerHeadState::Reserved,
            SLOT_PUBLISHED => ConsumerHeadState::Published,
            SLOT_POISONED => ConsumerHeadState::Poisoned,
            _ => ConsumerHeadState::Failed,
        }
    }

    /// 业务作用：关闭新 producer 后按全局序号物理排空剩余载荷；即使队列已标记 Failed，
    /// 仍允许收口者释放能够安全辨认的已发布前缀，但绝不越过未完成 reservation。
    ///
    /// 参数说明：
    /// - `consumer`: 对每个唯一载荷恰好调用一次的终态或失败清理动作。
    ///
    /// 返回：完整跨过关门边界时返回载荷与 poison 计数；仍有 producer、保留队头或槽位
    /// 权威不一致时返回具体阻断位置，调用方可等待后重试。
    pub(crate) fn drain_after_producers_stop(
        &mut self,
        mut consumer: impl FnMut(T),
    ) -> Result<DrainSummary, DrainError> {
        self.core.accepting.store(false, Ordering::SeqCst);
        let producers = self.core.producer_inflight.load(Ordering::SeqCst);
        if producers != 0 {
            return Err(DrainError::ProducersActive { count: producers });
        }
        let boundary = self.core.next_sequence.load(Ordering::Acquire);
        let mut summary = DrainSummary {
            items: 0,
            poisoned: 0,
        };
        while self.next_sequence < boundary {
            self.core.reclaim_before(self.next_sequence);
            let sequence = self.next_sequence;
            let Some(segment) = self.core.segment_for_consumer(sequence) else {
                return Err(DrainError::HeadReserved { sequence });
            };
            let slot = segment.slot(sequence);
            match slot.state.load(Ordering::Acquire) {
                SLOT_PUBLISHED => {
                    let Some(value) = slot.value.lock().unwrap_or_else(|e| e.into_inner()).take()
                    else {
                        self.core.fail();
                        return Err(DrainError::Failed { sequence });
                    };
                    slot.state.store(SLOT_TAKEN, Ordering::Release);
                    self.next_sequence += 1;
                    summary.items += 1;
                    consumer(value);
                }
                SLOT_POISONED => {
                    slot.state.store(SLOT_TAKEN, Ordering::Release);
                    self.next_sequence += 1;
                    summary.poisoned += 1;
                }
                SLOT_EMPTY | SLOT_RESERVED | SLOT_WRITING => {
                    return Err(DrainError::HeadReserved { sequence });
                }
                _ => {
                    self.core.fail();
                    return Err(DrainError::Failed { sequence });
                }
            }
        }
        self.core.reclaim_before(self.next_sequence);
        Ok(summary)
    }

    /// 业务作用：关闭新 producer；consumer 仍可继续排空已经取得序号的槽位。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；关闭状态不可逆。
    pub(crate) fn close(&self) {
        self.core.accepting.store(false, Ordering::SeqCst);
    }

    /// 业务作用：读取当前活动物理分段数，供消费侧验证跨段回收已经发生。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：分段注册表中的当前条目数。
    pub(crate) fn segment_count(&self) -> usize {
        self.core
            .segments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

impl<T> Drop for SequencedReceiver<T> {
    /// 业务作用：唯一 consumer 离场时关闭新 producer，避免后续任务进入无人消费的队列。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；队列新预留门禁单向关闭。
    fn drop(&mut self) {
        self.core.accepting.store(false, Ordering::SeqCst);
    }
}

/// 业务作用：创建共享序号域的多 producer、单 consumer 队列。
///
/// 参数说明: 无。
///
/// 返回：一个可克隆 sender 和一个不可克隆 receiver；序号从零开始。
pub(crate) fn channel<T>() -> (SequencedSender<T>, SequencedReceiver<T>) {
    let core = Arc::new(QueueCore::new());
    (
        SequencedSender { core: core.clone() },
        SequencedReceiver {
            core,
            next_sequence: 0,
        },
    )
}
