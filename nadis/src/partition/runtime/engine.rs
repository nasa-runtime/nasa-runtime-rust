//! 共享记录账本与业务键门禁，各执行域独立推进业务和提交监督。

use super::{fencing, identity::BatchIdentity, GroupRuntime};
use crate::partition::{
    plan::{ConsumerPlan, DecodeFailure, Payload, PlanMap},
    Envelope, PartitionLimits, RecordIdentity,
};
use crate::{
    error::{NasaRedisError, Result},
    lock::HoldStatus,
};
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

#[repr(u8)]
enum SourcePhase {
    Active,
    Quiescing,
    Releasing,
    Lost,
}

/// 一个持锁期的不可变权威；停止根准入不撤销既有成功结果的提交权限。
pub(in crate::partition) struct SourceAuthority {
    pub domain: usize,
    pub epoch: u64,
    pub partition: u32,
    pub holder: String,
    pub lock_key: String,
    pub stamp: Option<fencing::FencingStamp>,
    pub group: Weak<GroupRuntime>,
    phase: AtomicU8,
    pub io: AtomicUsize,
    pub blocked: AtomicBool,
    pub parked: AtomicBool,
    pub uncertain: AtomicBool,
    pub sweep_requested: AtomicBool,
    pub oversized: AtomicBool,
    pub protocol_error: AtomicBool,
    pub unreleased: AtomicBool,
    pub lost_watch: tokio::sync::watch::Receiver<bool>,
    pub lost_cancel: tokio_util::sync::CancellationToken,
}

impl SourceAuthority {
    /// 业务作用：判断来源生命周期是否仍允许读取监督存活，Park 不等于释放 Claim。
    /// 参数说明: 无。
    /// 返回：Active 且未失权时为 true。
    pub(super) fn active(&self) -> bool {
        self.phase.load(Ordering::Acquire) == SourcePhase::Active as u8 && self.can_commit()
    }
    /// 业务作用：冻结当前持锁期凭据；相同物理分区重新持锁必须创建新 epoch。
    /// 参数说明：`rt` 为物理组；`partition` 为分区；`identity` 为任期；`guard` 提供锁凭据；`stamp` 为 fencing 戳。
    /// 返回：Active 来源权威，尚未开放恢复后的新消息读取。
    pub(super) fn new(
        rt: &Arc<GroupRuntime>,
        partition: u32,
        identity: BatchIdentity,
        guard: &crate::lock::LockGuard,
        stamp: Option<fencing::FencingStamp>,
    ) -> Arc<Self> {
        Arc::new(Self {
            domain: rt.core.executions.domain(&rt.layout.prefix, partition),
            epoch: identity.claim_epoch,
            partition,
            holder: guard.holder().into(),
            lock_key: guard.lock_key().into(),
            stamp,
            group: Arc::downgrade(rt),
            phase: AtomicU8::new(SourcePhase::Active as u8),
            io: AtomicUsize::new(0),
            blocked: AtomicBool::new(false),
            parked: AtomicBool::new(false),
            uncertain: AtomicBool::new(false),
            sweep_requested: AtomicBool::new(false),
            oversized: AtomicBool::new(false),
            protocol_error: AtomicBool::new(false),
            unreleased: AtomicBool::new(true),
            lost_watch: guard.lost(),
            lost_cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// 业务作用：检查是否仍允许开始下一条业务，隔离失锁、停机与人工冻结。
    /// 参数说明: 无。
    /// 返回：仅 Active 且持锁观察未失效、未 Park 时为 true。
    pub(in crate::partition) fn can_start(&self) -> bool {
        self.phase.load(Ordering::Acquire) == SourcePhase::Active as u8
            && self.can_commit()
            && !self.parked.load(Ordering::Acquire)
    }

    /// 业务作用：区分正常排干与失权，允许正常停止期间提交既有成功事实。
    /// 参数说明: 无。
    /// 返回：Active/Quiescing 且看门狗未确认失锁时为 true。
    pub(in crate::partition) fn can_commit(&self) -> bool {
        self.phase.load(Ordering::Acquire) < SourcePhase::Releasing as u8
            && !*self.lost_watch.borrow()
    }

    /// 业务作用：先关闭下一条业务准入，再等待已开始任务和提交责任收口。
    /// 参数说明: 无。
    /// 返回：Active 单向进入 Quiescing，不撤销已成功业务的提交权。
    pub(in crate::partition) fn quiesce(&self) {
        let _ = self.phase.compare_exchange(
            SourcePhase::Active as u8,
            SourcePhase::Quiescing as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// 业务作用：全部来源责任取得退出证明后封闭最后提交窗口，再开始释放租约。
    /// 参数说明: 无。
    /// 返回：Quiescing 进入 Releasing，Lost 不会恢复为可释放的有效权威。
    pub(super) fn begin_release(&self) {
        let _ = self.phase.compare_exchange(
            SourcePhase::Quiescing as u8,
            SourcePhase::Releasing as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// 业务作用：失去租约时立即撤销所有新业务及 Redis 修改权限。
    /// 参数说明: 无。
    /// 返回：来源进入不可恢复的 Lost；新持锁必须创建另一权威。
    pub(in crate::partition) fn lose(&self) {
        self.phase.store(SourcePhase::Lost as u8, Ordering::Release);
        self.lost_cancel.cancel();
    }
}

/// 带完整物理坐标的本地记录索引；任期不在主键中，避免新 Claim 覆盖旧责任。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(in crate::partition) struct Coordinate {
    pub group: Arc<str>,
    pub identity: RecordIdentity,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct GateKey {
    plan: u32,
    route: napart::RouteHash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BusinessState {
    Open,
    Executing,
    Blocked,
    Invalidating,
}

struct OrderedGate {
    token: u64,
    business: BusinessState,
    queue: VecDeque<Coordinate>,
    executing: Option<u64>,
}

/// 独占责任在同一账本锁内转移；Commit 永远不会转回需要执行 handler 的状态。
enum RecordOwner {
    Ready,
    Parked,
    Task(u64),
    Commit {
        unknown: bool,
        confirmed: bool,
        next: Instant,
    },
    Retry {
        next: Instant,
        operation: String,
        executed: bool,
    },
    Unroutable {
        malformed: bool,
        next: Instant,
    },
}

struct Record {
    source: Arc<SourceAuthority>,
    batch: u64,
    revision: u64,
    owner: RecordOwner,
    plan: Option<Arc<ConsumerPlan>>,
    gate: Option<GateKey>,
    bytes: usize,
    attempts: u32,
    // 同一 revision 的 CAS 意图在本地保留，响应丢失和远端 marker 到期均不能重算 delivery。
    retry_intent: Option<crate::partition::retryop::RetryIntent>,
}

type TaskPayloads = Arc<Mutex<Option<Vec<(Coordinate, Payload)>>>>;

struct RetiredPayload<'a> {
    _payload: Payload,
    _budget: RetiredPayloadBudget<'a>,
}

struct RetiredPayloadBudget<'a> {
    core: &'a RedisPartitionRuntime,
    source: Arc<SourceAuthority>,
    bytes: usize,
}

impl Drop for RetiredPayloadBudget<'_> {
    /// 业务作用：未执行对象析构完成后归还容量和来源责任，防止析构期间提前解锁。
    /// 参数说明: 无。
    /// 返回：移交中的记录、正文与 I/O 引用归零；析构展开同样释放 guard。
    fn drop(&mut self) {
        let mut state = self.core.state.lock().expect("ledger");
        state.bytes -= self.bytes;
        state.reserved_records -= 1;
        state.domains[self.source.domain].bytes -= self.bytes;
        state.domains[self.source.domain].records -= 1;
        self.source.io.fetch_sub(1, Ordering::AcqRel);
        self.core.changed.notify_waiters();
    }
}

#[derive(Clone)]
struct ConsumeOutcome {
    succeeded: Vec<Coordinate>,
    failed: Vec<Coordinate>,
    deferred: Vec<Coordinate>,
}

/// 可重复读取的执行事实；Notify 只加速监督者，不承载记录所有权。
struct OutcomeCell {
    value: Mutex<Option<ConsumeOutcome>>,
    changed: Notify,
}

struct TaskTicket {
    source: Arc<SourceAuthority>,
    records: Vec<Coordinate>,
    gate: Option<(GateKey, u64)>,
    submission: Option<napart::Submission>,
    outcome: Arc<OutcomeCell>,
}

#[derive(Default)]
struct State {
    domains: Vec<DomainUsage>,
    sources: HashMap<u64, Weak<SourceAuthority>>,
    abandoned: HashMap<u64, Arc<SourceAuthority>>,
    abandoned_readers: Vec<tokio::task::JoinHandle<()>>,
    records: HashMap<Coordinate, Record>,
    prepared: HashMap<Coordinate, Payload>,
    tasks: BTreeMap<u64, TaskTicket>,
    gates: HashMap<GateKey, OrderedGate>,
    waiters: VecDeque<(u64, usize)>,
    reads: HashMap<u64, (usize, usize)>,
    next_identity: u64,
    bytes: usize,
    reserved_records: usize,
    record_debt: usize,
    retained: u64,
    committed: u64,
    protocol_closed: bool,
}

#[derive(Default, Clone)]
struct DomainUsage {
    records: usize,
    bytes: usize,
    batches: usize,
}

/// 启动时冻结的执行域及其当前份额，不包含业务键或消息坐标。
#[derive(Debug, Clone)]
pub struct ExecutionDomainSnapshot {
    /// 按完整配置拓扑冻结的本实例域索引，再平衡不重新编号。
    pub id: usize,
    /// source 模式为 None；其它模式为逻辑组 ID，默认组使用空字符串。
    pub group: Option<String>,
    /// 仅 stream 模式设置，表示组内物理 Stream 编号。
    pub partition: Option<u32>,
    /// 本域固定记录份额，受源级记录及后继责任上限共同约束。
    pub record_capacity: usize,
    /// 本域固定估算正文份额，包含线格式与解码对象的保留权重。
    pub payload_capacity: usize,
    /// 本域可同时持有的读取批次预留。
    pub batch_capacity: usize,
    /// 本域可交给异步删除 owner 的 ID 上限。
    pub delete_capacity: usize,
    /// 尚未终结的记录责任数，包含读取前预留。
    pub inflight_records: usize,
    /// 当前保留的估算正文与解码对象字节数，包含读取预留。
    pub payload_bytes: usize,
    /// 当前持有读取预留的批次数。
    pub active_batches: usize,
    /// 账本中尚未收口的消费任务数。
    pub inflight_tasks: usize,
    /// 本执行域尚未完成的异步删除责任数。
    pub async_delete_pending: usize,
    /// 执行监督是否已进入降级状态。
    pub runner_degraded: bool,
    /// 本执行域 Runner 是否已确认进入 Stopped 状态。
    pub runner_stopped: bool,
}

/// 共享消费状态的低基数快照，不输出 entry id、业务键或 gate token。
#[derive(Debug, Clone, Default)]
pub struct PartitionSnapshot {
    /// 本实例冻结的本地执行域划分方式。
    pub executor_scope: crate::partition::PartitionExecutorScope,
    /// record、payload、batch、task 在账本锁内采样；删除与 Runner 状态独立读取。
    pub execution_domains: Vec<ExecutionDomainSnapshot>,
    /// 消费器本地准入状态，业务须显式接入 Application readiness。
    pub ready: bool,
    /// 执行监督是否已进入降级状态。
    pub runner_degraded: bool,
    /// 尚未终结的记录责任数，包含读取前预留。
    pub inflight_records: usize,
    /// 当前保留的估算正文与解码对象字节数，包含读取预留。
    pub payload_bytes: usize,
    /// 当前持有读取预留的批次数。
    pub active_batches: usize,
    /// 等待共享读取预算的请求数。
    pub read_waiters: usize,
    /// 账本中尚未收口的消费任务数。
    pub inflight_tasks: usize,
    /// 业务已完成但提交责任尚未终结的记录数。
    pub pending_commits: usize,
    /// pending_commits 中服务端提交结果尚不确定的记录数。
    pub unknown_commits: usize,
    /// 由重试责任持有的记录数。
    pub retry_tickets: usize,
    /// 当前保留的业务顺序键门禁数。
    pub ordered_keys: usize,
    /// 因前驱尚未解决而处于 Blocked 状态的顺序键数。
    pub blocked_keys: usize,
    /// 已经就绪、仍由顺序键门禁管理的待执行记录数。
    pub deferred_records: usize,
    /// 当前无法解码或匹配消费计划、仍保留处置责任的记录数。
    pub unroutable_records: usize,
    /// 已暂停推进、等待显式处置的记录数。
    pub parked_records: usize,
    /// 因未解决记录而关闭新业务准入的物理来源数。
    pub blocked_sources: usize,
    /// 进入 Park 保护态的物理来源数。
    pub parked_sources: usize,
    /// 因单条原始 Envelope 超过 max_record_bytes 而受保护的物理来源数。
    pub oversized_sources: usize,
    /// 因协议异常而受保护的物理来源数。
    pub protocol_error_sources: usize,
    /// Redis 响应记录数超过读取预留形成的数量欠额，非零时关闭根准入。
    pub record_debt: usize,
    /// 当前正文预算占用超过源级上限的字节数。
    pub byte_debt: usize,
    /// 确认服务端提交并从本地账本移除的累计记录数。
    pub committed_records: u64,
    /// 未确认提交而结束本地责任的累计记录数，不代表已经消费成功。
    pub retained_records: u64,
    /// 尚未确认释放或原 holder 已失权的来源锁数。
    pub unconfirmed_locks: usize,
    /// 在途任务中 Runner 当前报告为 Running 的数量。
    pub running_tasks: usize,
    /// 各来源仍持有的 I/O 与载荷清理责任总数。
    pub source_io: usize,
    /// 转交共享监督后尚未 join 回收的读取任务数。
    pub unjoined_readers: usize,
    /// 异步删除未确认成功、保留远端记录的累计数量。
    pub delete_retained_records: u64,
}

pub(in crate::partition) struct RedisPartitionRuntime {
    state: Mutex<State>,
    pub limits: PartitionLimits,
    pub executions: crate::partition::execution::ExecutionDomains,
    pub changed: Notify,
    pub roots_closed: AtomicBool,
    pub degraded: AtomicBool,
    pub async_delete_records: AtomicUsize,
    pub delete_retained_records: std::sync::atomic::AtomicU64,
    stopping: AtomicBool,
    force_io: tokio_util::sync::CancellationToken,
    supervisors: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl RedisPartitionRuntime {
    /// 业务作用：接管异常退出来源的读取句柄，取消请求不能替代真实 join 证明。
    /// 参数说明：`reader` 为已请求中止但尚未等待退出的唯一读取任务。
    /// 返回：句柄纳入共享监督与停机报告，直到任务终结后被 await。
    pub(super) fn abandoned_reader(&self, reader: tokio::task::JoinHandle<()>) {
        self.state
            .lock()
            .expect("ledger")
            .abandoned_readers
            .push(reader);
        self.changed.notify_waiters();
    }

    /// 业务作用：缺少完整退出证明时登记停止续租的锁，直到服务端确认原 holder 不再持有。
    /// 参数说明：`source` 为失权或显式强停的原始来源。
    /// 返回：保留有界租约对账责任，不发送提前 unlock。
    pub(super) fn abandoned_source(&self, source: &Arc<SourceAuthority>) {
        self.state
            .lock()
            .expect("ledger")
            .abandoned
            .insert(source.epoch, source.clone());
        self.changed.notify_waiters();
    }

    /// 业务作用：人工处置完成后重新接管本地未执行后缀，未知计划仍保持来源保护。
    /// 参数说明：`source` 为收到处置完成通知的现役任期。
    /// 返回：可判定后缀进入精确读取，原来源随后通过 PEL 空页重新开放新消息。
    pub(super) fn resume_source(&self, source: &Arc<SourceAuthority>) {
        let mut state = self.state.lock().expect("ledger");
        // 管理处置可能是重投，也可能已 ACK；保留原 gate，先按精确 PEL 事实决定下一责任。
        for (coordinate, record) in state.records.iter_mut() {
            if record.source.epoch == source.epoch && matches!(record.owner, RecordOwner::Parked) {
                record.owner = RecordOwner::Retry {
                    next: Instant::now(),
                    operation: format!("park:{}:{}", source.epoch, coordinate.identity.id),
                    executed: false,
                };
                record.retry_intent = None;
            }
        }
        let blocked = state.records.values().any(|record| {
            record.source.epoch == source.epoch
                && matches!(record.owner, RecordOwner::Unroutable { .. })
                && record.plan.is_none()
        });
        if !blocked
            && !source.protocol_error.load(Ordering::Acquire)
            && !source.oversized.load(Ordering::Acquire)
        {
            for (coordinate, record) in state.records.iter_mut() {
                if record.source.epoch == source.epoch
                    && matches!(
                        record.owner,
                        RecordOwner::Unroutable {
                            malformed: false,
                            ..
                        }
                    )
                {
                    record.owner = RecordOwner::Retry {
                        next: Instant::now(),
                        operation: format!("resume:{}:{}", source.epoch, coordinate.identity.id),
                        executed: false,
                    };
                }
            }
            source.blocked.store(false, Ordering::Release);
        }
        source.uncertain.store(true, Ordering::Release);
        source.parked.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }
    /// 业务作用：显式强停时撤销全部持锁任期的新副作用权限。
    /// 参数说明: 无。
    /// 返回：所有来源单向进入 Lost，不主动解锁。
    pub(in crate::partition) fn revoke_all(&self) {
        self.roots_closed.store(true, Ordering::Release);
        self.force_io.cancel();
        for source in self
            .state
            .lock()
            .expect("ledger")
            .sources
            .values()
            .filter_map(Weak::upgrade)
        {
            source.lose();
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：空闲来源周期对账 PEL，仍由原来源唯一读者负责网络执行。
    /// 参数说明：`group` 为要巡检的物理组。
    /// 返回：没有本地记录和读责任的来源设置恢复标志，其它来源不受影响。
    pub(in crate::partition) fn request_recovery(&self, group: &str) {
        let state = self.state.lock().expect("ledger");
        for source in state.sources.values().filter_map(Weak::upgrade) {
            if source
                .group
                .upgrade()
                .is_some_and(|rt| rt.layout.prefix == group)
                && !state
                    .records
                    .values()
                    .any(|r| r.source.epoch == source.epoch)
                && !state.reads.contains_key(&source.epoch)
            {
                source.sweep_requested.store(true, Ordering::Release);
            }
        }
    }
    /// 业务作用：同步建立本源全部执行域和共享账本，供激活事务先取得清理责任。
    /// 参数说明：`limits` 为源级预算；`plan` 为已校验的域表和配额。
    /// 返回：未启动任何后台任务的运行时；注册失败不产生消费副作用。
    pub(in crate::partition) fn new(
        limits: PartitionLimits,
        plan: crate::partition::execution::ExecutionPlan,
    ) -> Result<Arc<Self>> {
        let executions = crate::partition::execution::ExecutionDomains::new(plan)?;
        let core = Arc::new(Self {
            state: Mutex::new(State {
                next_identity: 1,
                domains: vec![DomainUsage::default(); executions.domains.len()],
                ..Default::default()
            }),
            limits,
            executions,
            changed: Notify::new(),
            roots_closed: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            async_delete_records: AtomicUsize::new(0),
            delete_retained_records: std::sync::atomic::AtomicU64::new(0),
            stopping: AtomicBool::new(false),
            force_io: tokio_util::sync::CancellationToken::new(),
            supervisors: Mutex::new(Vec::new()),
        });
        Ok(core)
    }

    /// 业务作用：在激活事务已持有全部清理责任后启动 Runner 和每域单飞监督。
    /// 参数说明：`gate` 为全组共同的消费激活屏障。
    /// 返回：全部执行器就绪时成功；任一步失败或取消均由既有激活事务排干。
    pub(in crate::partition) async fn start(
        self: &Arc<Self>,
        gate: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        for domain in &self.executions.domains {
            domain
                .runner
                .start()
                .await
                .map_err(|e| NasaRedisError::Config(e.to_string()))?;
        }
        for domain in 0..self.executions.domains.len() {
            let owner = self.clone();
            let gate = gate.clone();
            let handle = tokio::spawn(async move {
                loop {
                    // 先登记停止通知再检查状态，回滚不能在状态检查与挂起之间丢失唤醒。
                    let changed = owner.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if gate.is_cancelled() || owner.stopping.load(Ordering::Acquire) {
                        break;
                    }
                    tokio::select! {
                        _ = gate.cancelled() => {
                        }
                        _ = changed => {
                        }
                    }
                }
                let result = std::panic::AssertUnwindSafe(owner.clone().supervise(domain))
                    .catch_unwind()
                    .await;
                if result.is_err() {
                    // 核心监督失败后保留账本与锁，不把尚未归约的成功事实伪装为已排干。
                    owner.degraded.store(true, Ordering::Release);
                    owner.close_roots();
                }
            });
            self.supervisors.lock().expect("supervisors").push(handle);
        }
        Ok(())
    }

    /// 业务作用：在首次读 PEL 前登记来源，所有后继责任始终引用同一不可变任期。
    /// 参数说明：`source` 为新持锁期权威。
    /// 返回：来源进入共享索引；同物理坐标的旧记录仍保留原 owner。
    pub(in crate::partition) fn register_source(&self, source: &Arc<SourceAuthority>) {
        let mut state = self.state.lock().expect("ledger");
        if self.roots_closed.load(Ordering::Acquire)
            || self.executions.domains[source.domain]
                .degraded
                .load(Ordering::Acquire)
        {
            source.quiesce();
        }
        state.sources.retain(|_, weak| weak.strong_count() != 0);
        state.sources.insert(source.epoch, Arc::downgrade(source));
    }

    /// 业务作用：按跨组 FIFO 原子预留批次、记录、正文及最坏后继容量。
    /// 参数说明：`source` 为读取来源；`count` 为本次 Redis COUNT。
    /// 返回：队首且容量充足时取得唯一读取票据；否则保持至多一个有界等待项。
    pub(in crate::partition) fn reserve(
        self: &Arc<Self>,
        source: &Arc<SourceAuthority>,
        count: usize,
    ) -> Option<ReadPermit> {
        let mut state = self.state.lock().expect("ledger");
        if self.roots_closed.load(Ordering::Acquire)
            || self.degraded.load(Ordering::Acquire)
            || self.executions.domains[source.domain]
                .degraded
                .load(Ordering::Acquire)
            || state.protocol_closed
            || state.record_debt != 0
            || !source.can_start()
            || source.blocked.load(Ordering::Acquire)
            || state.reads.contains_key(&source.epoch)
        {
            return None;
        }
        let group = source.group.upgrade()?;
        let group_reads = state
            .reads
            .keys()
            .filter(|epoch| {
                state
                    .sources
                    .get(epoch)
                    .and_then(Weak::upgrade)
                    .and_then(|source| source.group.upgrade())
                    .is_some_and(|current| current.layout.prefix == group.layout.prefix)
            })
            .count();
        if group_reads >= group.stream_cfg.inflight_max
            || state
                .records
                .values()
                .filter(|record| record.source.epoch == source.epoch)
                .count()
                .checked_add(count)?
                > self.limits.max_blocked_keys_per_source
        {
            // 来源自己的上限不能占住全局 FIFO 队首，阻止其它仍有准入资格的组前进。
            state.waiters.retain(|(epoch, _)| *epoch != source.epoch);
            return None;
        }
        let bytes = count.checked_mul(self.limits.max_record_bytes)?;
        let quota = &self.executions.domains[source.domain].spec.quota;
        let usage = &state.domains[source.domain];
        if usage.records.checked_add(count)? > quota.records
            || usage.bytes.checked_add(bytes)? > quota.bytes
            || usage.batches >= quota.batches
        {
            // 本域固定份额耗尽不能占住跨域队首；其他域的最低份额始终保留。
            state.waiters.retain(|(epoch, _)| *epoch != source.epoch);
            return None;
        }
        let valid: std::collections::HashSet<u64> = state
            .sources
            .iter()
            .filter_map(|(id, weak)| {
                weak.upgrade()
                    .filter(|s| s.can_start() && !s.blocked.load(Ordering::Acquire))
                    .map(|_| *id)
            })
            .collect();
        let eligible: std::collections::HashSet<u64> = state
            .waiters
            .iter()
            .filter_map(|(epoch, count)| {
                let source = state.sources.get(epoch)?.upgrade()?;
                let usage = &state.domains[source.domain];
                let quota = &self.executions.domains[source.domain].spec.quota;
                (usage.records.checked_add(*count)? <= quota.records
                    && usage
                        .bytes
                        .checked_add(count.checked_mul(self.limits.max_record_bytes)?)?
                        <= quota.bytes
                    && usage.batches < quota.batches)
                    .then_some(*epoch)
            })
            .collect();
        state
            .waiters
            .retain(|(id, _)| valid.contains(id) && eligible.contains(id));
        if !state.waiters.iter().any(|(id, _)| *id == source.epoch) {
            if state.waiters.len() >= self.limits.max_read_waiters {
                return None;
            }
            state.waiters.push_back((source.epoch, count));
        }
        if state.waiters.front().map(|(id, _)| *id) != Some(source.epoch) {
            return None;
        }
        if state.reads.len() >= self.limits.max_active_batches
            || state
                .records
                .len()
                .checked_add(state.reserved_records)?
                .checked_add(count)?
                > self.limits.record_limit()
            || state.bytes.checked_add(bytes)? > self.limits.max_inflight_payload_bytes
        {
            return None;
        }
        state.waiters.pop_front();
        state.reads.insert(source.epoch, (count, bytes));
        state.reserved_records += count;
        state.bytes += bytes;
        state.domains[source.domain].records += count;
        state.domains[source.domain].bytes += bytes;
        state.domains[source.domain].batches += 1;
        source.io.fetch_add(1, Ordering::AcqRel);
        Some(ReadPermit {
            core: self.clone(),
            source: source.clone(),
            released: false,
        })
    }

    /// 业务作用：判断来源本地责任是否已全部交回，作为 unlock 的必要门禁。
    /// 参数说明：`source` 为待释放持锁期。
    /// 返回：记录、任务、读取和在途 Redis I/O 全部为零时为 true。
    pub(in crate::partition) fn drained(&self, source: &SourceAuthority) -> bool {
        let state = self.state.lock().expect("ledger");
        source.io.load(Ordering::Acquire) == 0
            && !state.reads.contains_key(&source.epoch)
            && !state
                .records
                .values()
                .any(|r| r.source.epoch == source.epoch)
            && !state.tasks.values().any(|t| t.source.epoch == source.epoch)
    }

    /// 业务作用：停止读者与下一条业务，同时保留正在运行任务的成功后继发布窗口。
    /// 参数说明: 无。
    /// 返回：全部来源进入 Quiescing；排队任务被取消，已运行 Future 继续受到监督。
    pub(in crate::partition) fn close_roots(&self) {
        let mut state = self.state.lock().expect("ledger");
        self.roots_closed.store(true, Ordering::Release);
        state.waiters.clear();
        for source in state.sources.values().filter_map(Weak::upgrade) {
            source.quiesce();
        }
        for task in state.tasks.values() {
            if let Some(submission) = &task.submission {
                submission.cancel();
            }
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：从固定计数和状态轴构造就绪与容量观测，不暴露高基数业务身份。
    /// 参数说明: 无。
    /// 返回：记录、正文、批次与任务在同一账本锁下采样；来源、删除与 Runner 状态独立读取，
    /// 不构成全部字段在同一时刻的原子证明。
    pub(in crate::partition) fn snapshot(&self) -> PartitionSnapshot {
        let state = self.state.lock().expect("ledger");
        let mut result = PartitionSnapshot {
            executor_scope: self.executions.scope,
            execution_domains: self
                .executions
                .domains
                .iter()
                .enumerate()
                .map(|(id, domain)| {
                    let usage = &state.domains[id];
                    ExecutionDomainSnapshot {
                        id,
                        group: domain.spec.group.clone(),
                        partition: domain.spec.partition,
                        record_capacity: domain.spec.quota.records,
                        payload_capacity: domain.spec.quota.bytes,
                        batch_capacity: domain.spec.quota.batches,
                        delete_capacity: domain.spec.quota.deletes,
                        inflight_records: usage.records,
                        payload_bytes: usage.bytes,
                        active_batches: usage.batches,
                        inflight_tasks: state
                            .tasks
                            .values()
                            .filter(|t| t.source.domain == id)
                            .count(),
                        async_delete_pending: domain.delete_records.load(Ordering::Acquire),
                        runner_degraded: domain.degraded.load(Ordering::Acquire),
                        runner_stopped: domain.runner.health() == napart::RunnerHealth::Stopped,
                    }
                })
                .collect(),
            ready: !self.roots_closed.load(Ordering::Acquire)
                && !self.degraded.load(Ordering::Acquire)
                && !state.protocol_closed
                && state.record_debt == 0,
            runner_degraded: self.degraded.load(Ordering::Acquire),
            inflight_records: state.records.len() + state.reserved_records,
            payload_bytes: state.bytes,
            active_batches: state.reads.len(),
            read_waiters: state.waiters.len(),
            inflight_tasks: state.tasks.len(),
            ordered_keys: state.gates.len(),
            record_debt: state.record_debt,
            byte_debt: state
                .bytes
                .saturating_sub(self.limits.max_inflight_payload_bytes),
            committed_records: state.committed,
            retained_records: state.retained,
            unconfirmed_locks: state
                .sources
                .values()
                .filter_map(Weak::upgrade)
                .filter(|source| source.unreleased.load(Ordering::Acquire))
                .count(),
            running_tasks: state
                .tasks
                .values()
                .filter(|t| {
                    t.submission
                        .as_ref()
                        .is_some_and(|s| s.status() == napart::TaskStatus::Running)
                })
                .count(),
            source_io: state
                .sources
                .values()
                .filter_map(Weak::upgrade)
                .map(|s| s.io.load(Ordering::Acquire))
                .sum(),
            unjoined_readers: state.abandoned_readers.len(),
            delete_retained_records: self.delete_retained_records.load(Ordering::Acquire),
            ..Default::default()
        };
        result.runner_degraded |= result.execution_domains.iter().any(|d| d.runner_degraded);
        result.ready &= !result.runner_degraded;
        result.blocked_sources = state
            .sources
            .values()
            .filter_map(Weak::upgrade)
            .filter(|s| s.blocked.load(Ordering::Acquire))
            .count();
        result.parked_sources = state
            .sources
            .values()
            .filter_map(Weak::upgrade)
            .filter(|source| source.parked.load(Ordering::Acquire))
            .count();
        result.ready &= result.blocked_sources == 0 && result.parked_sources == 0;
        result.oversized_sources = state
            .sources
            .values()
            .filter_map(Weak::upgrade)
            .filter(|source| source.oversized.load(Ordering::Acquire))
            .count();
        result.protocol_error_sources = state
            .sources
            .values()
            .filter_map(Weak::upgrade)
            .filter(|source| source.protocol_error.load(Ordering::Acquire))
            .count();
        for record in state.records.values() {
            match record.owner {
                RecordOwner::Commit { unknown, .. } => {
                    result.pending_commits += 1;
                    result.unknown_commits += usize::from(unknown);
                }
                RecordOwner::Retry { .. } => result.retry_tickets += 1,
                RecordOwner::Parked => result.parked_records += 1,
                RecordOwner::Unroutable { .. } => result.unroutable_records += 1,
                RecordOwner::Ready if record.gate.is_some() => result.deferred_records += 1,
                _ => {}
            }
        }
        result.blocked_keys = state
            .gates
            .values()
            .filter(|g| g.business == BusinessState::Blocked)
            .count();
        result
    }

    /// 业务作用：冻结整批路由后一次性发布记录责任；任意不可判定路由使整批停在来源保护态。
    /// 参数说明：`permit` 为读取预留；`batch` 为来源批次；`rows` 为明确区分墓碑的原始记录；`plans` 为不可变路由。
    /// 返回：已登记坐标不会被新 epoch 覆盖；COUNT 越界永久关闭读取，超大正文冻结当前来源。
    pub(in crate::partition) fn dispatch(
        &self,
        mut permit: ReadPermit,
        batch: u64,
        rows: Vec<(String, Option<Vec<u8>>)>,
        plans: &PlanMap,
    ) {
        let source = permit.source.clone();
        let Some(rt) = source.group.upgrade() else {
            return;
        };
        let mut prepared = Vec::with_capacity(rows.len());
        let mut unroutable = false;
        let mut payload_overflow = false;
        for (id, body) in rows {
            let coordinate = Coordinate {
                group: Arc::from(rt.layout.group()),
                identity: RecordIdentity {
                    stream: Arc::from(rt.layout.stream(source.partition)),
                    id: Arc::from(id),
                },
            };
            let raw_bytes = body.as_ref().map_or(0, Vec::len);
            let tombstone = body.is_none();
            let mut plan = None;
            let mut decoded = None;
            let mut failure = None;
            if let Some(body) = body {
                if body.len() > self.limits.max_record_bytes {
                    source.oversized.store(true, Ordering::Release);
                    failure = Some(DecodeFailure::Internal);
                } else {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let env = serde_json::from_slice::<Envelope>(&body)
                            .map_err(|_| DecodeFailure::Malformed)?;
                        let candidate = plans
                            .get(&(env.topic.clone(), env.event.clone()))
                            .ok_or(DecodeFailure::Internal)?
                            .clone();
                        plan = Some(candidate.clone());
                        let value = (candidate.decode)(coordinate.identity.clone(), env)?;
                        Ok::<_, DecodeFailure>((candidate, value))
                    }));
                    match result {
                        Ok(Ok((p, value))) => {
                            plan = Some(p);
                            decoded = Some(value);
                        }
                        Ok(Err(reason)) => failure = Some(reason),
                        Err(_) => failure = Some(DecodeFailure::Internal),
                    }
                }
            }
            // 无路由或内部回调异常意味着顺序键未知，整批不得抢先执行可解码的后缀。
            if failure.is_some()
                && !(matches!(failure, Some(DecodeFailure::Malformed))
                    && plan.as_ref().is_some_and(|p| p.legacy))
            {
                unroutable = true;
            }
            let bytes = raw_bytes.checked_add(decoded.as_ref().map_or(0, |d| d.weight));
            payload_overflow |= bytes.is_none();
            let bytes = bytes.unwrap_or(usize::MAX);
            if matches!(failure, Some(DecodeFailure::Internal)) {
                plan = None;
            }
            prepared.push((coordinate, plan, decoded, bytes, tombstone, failure));
        }
        let mut state = self.state.lock().expect("ledger");
        let (reserved, reserved_bytes) =
            state.reads.remove(&source.epoch).expect("read reservation");
        state.reserved_records -= reserved;
        state.bytes -= reserved_bytes;
        state.domains[source.domain].records -= reserved;
        state.domains[source.domain].bytes -= reserved_bytes;
        state.domains[source.domain].batches -= 1;
        source.io.fetch_sub(1, Ordering::AcqRel);
        permit.released = true;
        if prepared.len() > reserved {
            state.record_debt = state.record_debt.saturating_add(prepared.len() - reserved);
            unroutable = true;
        }
        // 线格式上限只约束原始 Envelope；解码权重在任何 Task 发布前统一尝试扩大共享预算。
        let actual = prepared.iter().try_fold(0usize, |n, r| n.checked_add(r.3));
        let payload_wait = payload_overflow
            || actual
                .and_then(|actual| state.bytes.checked_add(actual))
                .is_none_or(|total| total > self.limits.max_inflight_payload_bytes)
            || actual
                .and_then(|actual| state.domains[source.domain].bytes.checked_add(actual))
                .is_none_or(|total| {
                    total > self.executions.domains[source.domain].spec.quota.bytes
                });
        let mut gate_counts: HashMap<GateKey, usize> = HashMap::new();
        for (_, plan, decoded, _, _, _) in &prepared {
            if let (Some(plan), Some(decoded)) = (plan, decoded) {
                let route = if plan.legacy {
                    Some(napart::RouteHash::from_key(&source.epoch))
                } else {
                    decoded.route
                };
                if let Some(route) = route {
                    *gate_counts
                        .entry(GateKey {
                            plan: plan.id,
                            route,
                        })
                        .or_default() += 1;
                }
            }
        }
        if gate_counts.iter().any(|(key, count)| {
            state
                .gates
                .get(key)
                .map_or(0, |g| g.queue.len())
                .saturating_add(*count)
                > self.limits.max_deferred_records_per_key
        }) {
            unroutable = true;
        }
        if state
            .next_identity
            .checked_add(gate_counts.len() as u64)
            .is_none()
        {
            state.protocol_closed = true;
            unroutable = true;
        }
        if unroutable || payload_wait {
            source.blocked.store(true, Ordering::Release);
        }
        // 未受理对象留在准备集合中，账本锁释放后再析构，避免应用 Drop 重入运行状态。
        for (coordinate, plan, decoded, bytes, tombstone, failure) in &mut prepared {
            // 精确坐标仍有旧责任时，不允许新任期读结果替换旧任务或提交事实。
            if state.records.contains_key(coordinate) {
                continue;
            }
            let gate = if unroutable {
                None
            } else {
                plan.as_ref().and_then(|p| {
                    decoded.as_ref().and_then(|d| {
                        let route = if p.legacy {
                            Some(napart::RouteHash::from_key(&source.epoch))
                        } else {
                            d.route
                        };
                        route.map(|route| GateKey { plan: p.id, route })
                    })
                })
            };
            if let Some(key) = gate {
                if !state.gates.contains_key(&key) {
                    let Some(token) = issue(&mut state) else {
                        source.blocked.store(true, Ordering::Release);
                        return;
                    };
                    state.gates.insert(
                        key,
                        OrderedGate {
                            token,
                            business: BusinessState::Open,
                            queue: VecDeque::new(),
                            executing: None,
                        },
                    );
                }
                state
                    .gates
                    .get_mut(&key)
                    .expect("gate")
                    .queue
                    .push_back(coordinate.clone());
            }
            let owner = if *tombstone {
                RecordOwner::Commit {
                    unknown: false,
                    confirmed: false,
                    next: Instant::now(),
                }
            } else if unroutable || failure.is_some() {
                RecordOwner::Unroutable {
                    malformed: matches!(failure, Some(DecodeFailure::Malformed)),
                    next: Instant::now(),
                }
            } else if payload_wait {
                // 正文可以精确重读，容量不足只转移原坐标与 gate，不能把合法记录升级为毒消息。
                RecordOwner::Retry {
                    next: Instant::now() + Duration::from_millis(200),
                    operation: format!("capacity:{}:{}", source.epoch, coordinate.identity.id),
                    executed: false,
                }
            } else {
                RecordOwner::Ready
            };
            let bytes = if unroutable || payload_wait || *tombstone || failure.is_some() {
                0
            } else {
                *bytes
            };
            let payload = if unroutable || payload_wait || *tombstone || failure.is_some() {
                None
            } else {
                decoded.take().map(|d| d.payload)
            };
            if let Some(payload) = payload {
                state.prepared.insert(coordinate.clone(), payload);
            }
            state.bytes = state.bytes.saturating_add(bytes);
            state.domains[source.domain].records += 1;
            state.domains[source.domain].bytes =
                state.domains[source.domain].bytes.saturating_add(bytes);
            state.records.insert(
                coordinate.clone(),
                Record {
                    source: source.clone(),
                    batch,
                    revision: 0,
                    owner,
                    plan: plan.clone(),
                    gate,
                    bytes,
                    attempts: 0,
                    retry_intent: None,
                },
            );
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：归约稳定 napart 终态并转移已预留的提交、重试与未执行责任。
    /// 参数说明：`state` 为唯一账本写锁。
    /// 返回：只有 Completed 且业务事实完整时生成 Commit；其它终态均保留 PEL。
    fn settle_tasks(&self, state: &mut State) {
        let done: Vec<_> = state
            .tasks
            .iter()
            .filter_map(|(id, task)| {
                let status = task
                    .submission
                    .as_ref()
                    .map_or(napart::TaskStatus::Rejected, |s| s.status());
                matches!(
                    status,
                    napart::TaskStatus::Completed
                        | napart::TaskStatus::Cancelled
                        | napart::TaskStatus::Rejected
                        | napart::TaskStatus::Failed
                )
                .then_some((*id, status))
            })
            .collect();
        for (id, status) in done {
            let task = state.tasks.remove(&id).expect("task ticket");
            let outcome = task
                .outcome
                .value
                .lock()
                .expect("outcome")
                .clone()
                .filter(|outcome| {
                    status == napart::TaskStatus::Completed
                        && outcome.succeeded.len() + outcome.failed.len() + outcome.deferred.len()
                            == task.records.len()
                        && task.records.iter().all(|coordinate| {
                            outcome
                                .succeeded
                                .iter()
                                .chain(&outcome.failed)
                                .chain(&outcome.deferred)
                                .filter(|candidate| *candidate == coordinate)
                                .count()
                                == 1
                        })
                });
            if let Some((key, token)) = task.gate {
                if let Some(gate) = state.gates.get_mut(&key) {
                    if gate.token == token && gate.executing == Some(id) {
                        gate.executing = None;
                    }
                }
            }
            for coordinate in &task.records {
                let Some(record) = state.records.get_mut(coordinate) else {
                    continue;
                };
                if !matches!(record.owner, RecordOwner::Task(current) if current == id) {
                    continue;
                }
                state.bytes -= record.bytes;
                state.domains[record.source.domain].bytes -= record.bytes;
                record.bytes = 0;
                let success = outcome
                    .as_ref()
                    .is_some_and(|o| o.succeeded.contains(coordinate));
                let failed = outcome
                    .as_ref()
                    .is_some_and(|o| o.failed.contains(coordinate));
                let Some(revision) = record.revision.checked_add(1) else {
                    record.source.lose();
                    continue;
                };
                record.revision = revision;
                record.retry_intent = None;
                // 成功事实先登记为提交责任，网络响应不确定只能在此责任内部对账。
                record.owner = if success {
                    RecordOwner::Commit {
                        unknown: false,
                        confirmed: false,
                        next: Instant::now(),
                    }
                } else {
                    RecordOwner::Retry {
                        next: Instant::now() + Duration::from_millis(200),
                        operation: format!(
                            "{}:{}:{}:{}",
                            task.source.epoch,
                            record.batch,
                            record.revision,
                            coordinate.identity.id
                        ),
                        executed: failed,
                    }
                };
                if failed {
                    record.attempts = record.attempts.saturating_add(1);
                }
            }
        }
    }

    /// 业务作用：在提交屏障内先登记任务、门禁与 OutcomeCell，再交给来源所属执行域。
    /// 参数说明：`state` 为共享账本；`payload_holds` 将拒绝任务的析构责任保持到账本锁外。
    /// 返回：受理成功保存 Submission；拒绝保留相同队头并进入精确重试。
    fn submit_ready(self: &Arc<Self>, state: &mut State, payload_holds: &mut Vec<TaskPayloads>) {
        if self.roots_closed.load(Ordering::Acquire) || self.degraded.load(Ordering::Acquire) {
            return;
        }
        let mut candidates: Vec<_> = state
            .records
            .iter()
            .filter(|(_, r)| matches!(r.owner, RecordOwner::Ready) && r.source.can_start())
            .map(|(c, r)| (r.batch, c.clone()))
            .collect();
        candidates.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| stream_id_cmp(&a.1.identity.id, &b.1.identity.id))
        });
        for (_, coordinate) in candidates {
            let Some(record) = state.records.get(&coordinate) else {
                continue;
            };
            if !matches!(record.owner, RecordOwner::Ready) {
                continue;
            }
            let source = record.source.clone();
            let plan = record.plan.as_ref().expect("ready plan").clone();
            let batch = record.batch;
            if plan.legacy
                && state.records.values().any(|candidate| {
                    candidate.source.epoch == source.epoch
                        && candidate.batch == batch
                        && candidate
                            .plan
                            .as_ref()
                            .is_some_and(|candidate_plan| candidate_plan.id == plan.id)
                        && matches!(candidate.owner, RecordOwner::Retry { .. })
                })
            {
                // 兼容入口必须等同一失败 Vec 的精确正文全部重建，不能把一次批量重投拆成逐条业务调用。
                continue;
            }
            let gate_key = record.gate;
            let mut records = vec![coordinate.clone()];
            let mut gate_token = None;
            if let Some(key) = gate_key {
                let gate = state.gates.get(&key).expect("record gate");
                // 同 key 必须等前一任务稳定终态及其全部提交结论，不凭唤醒信号开放后继。
                if gate.executing.is_some()
                    || gate.queue.front() != Some(&coordinate)
                    || gate.queue.iter().any(|c| {
                        state
                            .records
                            .get(c)
                            .is_some_and(|r| matches!(r.owner, RecordOwner::Commit { .. }))
                    })
                {
                    continue;
                }
                records = gate
                    .queue
                    .iter()
                    .take_while(|c| {
                        state.records.get(*c).is_some_and(|r| {
                            matches!(r.owner, RecordOwner::Ready)
                                && r.source.epoch == source.epoch
                                && r.batch == batch
                        })
                    })
                    .cloned()
                    .collect();
                gate_token = Some((key, gate.token));
            }
            let Some(id) = issue(state) else {
                return;
            };
            let outcome = Arc::new(OutcomeCell {
                value: Mutex::new(None),
                changed: Notify::new(),
            });
            let mut payloads = Vec::with_capacity(records.len());
            for coordinate in &records {
                let record = state.records.get_mut(coordinate).expect("record");
                record.owner = RecordOwner::Task(id);
                payloads.push((
                    coordinate.clone(),
                    state.prepared.remove(coordinate).expect("ready payload"),
                ));
            }
            if let Some((key, _)) = gate_token {
                let gate = state.gates.get_mut(&key).expect("gate");
                gate.executing = Some(id);
                gate.business = BusinessState::Executing;
            }
            state.tasks.insert(
                id,
                TaskTicket {
                    source: source.clone(),
                    records: records.clone(),
                    gate: gate_token,
                    submission: None,
                    outcome: outcome.clone(),
                },
            );
            let core = self.clone();
            let domain = source.domain;
            let spec = plan.spec(gate_key.is_some());
            let route = gate_key.map_or_else(napart::RouteHash::unordered, |g| g.route);
            // submit 拒绝会同步释放闭包；额外持有载荷到锁外，防止应用析构锁住整个账本。
            let payloads = Arc::new(Mutex::new(Some(payloads)));
            payload_holds.push(payloads.clone());
            let submission = self.executions.domains[source.domain]
                .runner
                .submit_routed_typed(route, spec, move || async move {
                    let items = payloads
                        .lock()
                        .expect("task payload")
                        .take()
                        .expect("payload owner");
                    core.execute(id, source, plan, items, outcome).await;
                });
            match submission {
                Ok(submission) => {
                    state
                        .tasks
                        .get_mut(&id)
                        .expect("registered task")
                        .submission = Some(submission);
                }
                Err(napart::SubmitRejection::QueueFull | napart::SubmitRejection::Overloaded) => {}
                Err(_) => {
                    // 类型合同或执行域已失效时关闭本域，其他域不继承该 Runner 的失效权限。
                    self.executions.domains[domain]
                        .degraded
                        .store(true, Ordering::Release);
                    for source in state.sources.values().filter_map(Weak::upgrade) {
                        if source.domain == domain {
                            source.quiesce();
                        }
                    }
                }
            }
        }
    }

    /// 业务作用：业务调用前复验票据、来源和 gate 的同一所有者，阻止迟到执行越过权威撤销。
    /// 参数说明：`id` 为任务身份；`source` 为提交时冻结的持锁期。
    /// 返回：票据仍拥有全部坐标且 gate token 未变化时允许开始下一条业务。
    fn task_can_start(&self, id: u64, source: &SourceAuthority) -> bool {
        let state = self.state.lock().expect("ledger");
        source.can_start()
            && state.tasks.get(&id).is_some_and(|task| {
                task.source.epoch == source.epoch
                    && task.records.iter().all(|coordinate| {
                        state.records.get(coordinate).is_some_and(|record| {
                        record.source.epoch == source.epoch
                            && matches!(record.owner, RecordOwner::Task(current) if current == id)
                    })
                    })
                    && task.gate.is_none_or(|(key, token)| {
                        state
                            .gates
                            .get(&key)
                            .is_some_and(|gate| gate.token == token && gate.executing == Some(id))
                    })
            })
    }

    /// 业务作用：在 napart 受监督任务内顺序执行已冻结的同 key 记录，并先发布门禁事实再发布业务结果。
    /// 参数说明：`id` 为任务票据；`source` 为原始权威；`plan` 为计划；`items` 为单次解码结果；`cell` 为可重读结果。
    /// 返回：成功前缀、失败头和未执行后缀完整写入结果；本函数不 ACK。
    async fn execute(
        self: Arc<Self>,
        id: u64,
        source: Arc<SourceAuthority>,
        plan: Arc<ConsumerPlan>,
        items: Vec<(Coordinate, Payload)>,
        cell: Arc<OutcomeCell>,
    ) {
        let mut outcome = ConsumeOutcome {
            succeeded: Vec::new(),
            failed: Vec::new(),
            deferred: Vec::new(),
        };
        let Some(rt) = source.group.upgrade() else {
            return;
        };
        let all: Vec<_> = items.iter().map(|(c, _)| c.clone()).collect();
        // 首次外部业务前必须复验持锁；Unknown 不代表执行许可。
        let held = source.can_start()
            && rt.lock.holds_status(&source.lock_key, &source.holder).await == HoldStatus::Held;
        if !held || !self.task_can_start(id, &source) {
            outcome.deferred = all;
        } else if plan.legacy {
            let args = items
                .into_iter()
                .map(|(c, payload)| (c.identity.id.to_string(), payload))
                .collect();
            let result = tokio::time::timeout(
                Duration::from_millis(rt.cfg.handler_timeout_ms),
                std::panic::AssertUnwindSafe(async { (plan.invoke)(args).await }).catch_unwind(),
            )
            .await;
            match result {
                Ok(Ok(failed)) => {
                    for coordinate in all {
                        if failed
                            .iter()
                            .any(|id| id == coordinate.identity.id.as_ref())
                        {
                            outcome.failed.push(coordinate);
                        } else {
                            outcome.succeeded.push(coordinate);
                        }
                    }
                }
                _ => outcome.failed = all,
            }
        } else {
            let mut stopped = false;
            for (coordinate, payload) in items {
                // 停机或失权后不开始下一条；已经完成的前缀仍由任务票据持有。
                if stopped || !self.task_can_start(id, &source) {
                    outcome.deferred.push(coordinate);
                    continue;
                }
                let entry_id = coordinate.identity.id.to_string();
                let result = tokio::time::timeout(
                    Duration::from_millis(rt.cfg.handler_timeout_ms),
                    std::panic::AssertUnwindSafe(async {
                        (plan.invoke)(vec![(entry_id, payload)]).await
                    })
                    .catch_unwind(),
                )
                .await;
                if matches!(result, Ok(Ok(ref failed)) if failed.is_empty()) {
                    outcome.succeeded.push(coordinate);
                } else {
                    stopped = true;
                    outcome.failed.push(coordinate);
                }
            }
        }
        {
            let mut state = self.state.lock().expect("ledger");
            let gate_ticket = state.tasks.get(&id).and_then(|task| task.gate);
            if let Some((key, token)) = gate_ticket {
                if let Some(gate) = state.gates.get_mut(&key) {
                    if gate.token == token && gate.executing == Some(id) {
                        gate.business = if !source.can_commit() {
                            BusinessState::Invalidating
                        } else if !outcome.failed.is_empty() || !outcome.deferred.is_empty() {
                            BusinessState::Blocked
                        } else {
                            BusinessState::Open
                        };
                    }
                }
            }
            // 门禁先观察业务事实，OutcomeCell 后发布；稳定 Completed 是监督者第三道 ACK 门禁。
            *cell.value.lock().expect("outcome") = Some(outcome);
        }
        cell.changed.notify_waiters();
        self.changed.notify_waiters();
    }

    /// 业务作用：周期扫描权威注册表，确保通知遗漏和取消不会遗失完成或提交责任。
    /// 参数说明：`domain` 为唯一归属的执行域。
    /// 返回：停止且所有记录及任务清空后退出；同域 Redis I/O 单飞，跨域不等待彼此网络返回。
    async fn supervise(self: Arc<Self>, domain: usize) {
        loop {
            let readers = {
                let mut state = self.state.lock().expect("ledger");
                let mut finished = Vec::new();
                let mut index = 0;
                while index < state.abandoned_readers.len() {
                    if state.abandoned_readers[index].is_finished() {
                        finished.push(state.abandoned_readers.swap_remove(index));
                    } else {
                        index += 1;
                    }
                }
                finished
            };
            for reader in readers {
                let _ = reader.await;
            }
            if matches!(
                self.executions.domains[domain].runner.health(),
                napart::RunnerHealth::Degraded | napart::RunnerHealth::Failed
            ) {
                // Runner 的局部失败只关闭所属来源；提交监督继续收口既有成功事实。
                self.executions.domains[domain]
                    .degraded
                    .store(true, Ordering::Release);
                for source in self
                    .state
                    .lock()
                    .expect("ledger")
                    .sources
                    .values()
                    .filter_map(Weak::upgrade)
                {
                    if source.domain == domain {
                        source.quiesce();
                    }
                }
            }
            let mut retired_payloads = Vec::new();
            let mut payload_holds = Vec::new();
            let action = {
                let mut state = self.state.lock().expect("ledger");
                self.settle_tasks(&mut state);
                let retained: Vec<_> = state
                    .records
                    .iter()
                    .filter(|(_, r)| {
                        !matches!(r.owner, RecordOwner::Task(_))
                            && (!r.source.can_commit()
                                || (!r.source.active()
                                    && !matches!(r.owner, RecordOwner::Commit { .. })))
                    })
                    .map(|(c, _)| c.clone())
                    .collect();
                for coordinate in retained {
                    if let Some(payload) = retire(&self, &mut state, &coordinate, false) {
                        retired_payloads.push(payload);
                    }
                }
                self.submit_ready(&mut state, &mut payload_holds);
                if self.stopping.load(Ordering::Acquire)
                    && state.records.is_empty()
                    && state.tasks.is_empty()
                    && state.reads.is_empty()
                    && state.abandoned.is_empty()
                    && state.abandoned_readers.is_empty()
                {
                    break;
                }
                next_io(&state, domain)
            };
            drop(retired_payloads);
            drop(payload_holds);
            if let Some(action) = action {
                let lost = action.source.lost_cancel.clone();
                tokio::select! {
                    biased;
                    _ = self.force_io.cancelled() => {
                    }
                    _ = lost.cancelled() => {
                    }
                    _ = self.run_io(action) => {
                    }
                }
                tokio::task::yield_now().await;
            } else {
                let abandoned = self
                    .state
                    .lock()
                    .expect("ledger")
                    .abandoned
                    .values()
                    .find(|source| source.domain == domain)
                    .cloned();
                if let Some(source) = abandoned {
                    if let Some(rt) = source.group.upgrade() {
                        if rt.lock.holds_status(&source.lock_key, &source.holder).await
                            == HoldStatus::Lost
                        {
                            source.unreleased.store(false, Ordering::Release);
                            self.state
                                .lock()
                                .expect("ledger")
                                .abandoned
                                .remove(&source.epoch);
                        }
                    }
                }
                tokio::select! {
                    _ = self.changed.notified() => {
                    }
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                    }
                }
            }
        }
    }

    /// 业务作用：在全部来源已经排干后关闭后继监督，并停止本实例拥有的 runner。
    /// 参数说明: 无。
    /// 返回：监督与 runner 取得退出证明后返回；不通过替换 runner 恢复降级执行域。
    pub(in crate::partition) async fn stop(&self) {
        self.close_roots();
        self.stopping.store(true, Ordering::Release);
        self.changed.notify_waiters();
        let handles = std::mem::take(&mut *self.supervisors.lock().expect("supervisors"));
        for handle in handles {
            let _ = handle.await;
        }
        for domain in &self.executions.domains {
            domain.runner.request_stop();
        }
        futures::future::join_all(self.executions.domains.iter().map(|domain| async move {
            loop {
                if domain
                    .runner
                    .stop(Instant::now() + Duration::from_secs(30))
                    .await
                    .is_ok()
                {
                    break;
                }
            }
        }))
        .await;
    }
}

/// 唯一读取票据；调用取消时只归还本地预留，来源仍需通过 PEL 恢复确认服务端结果。
pub(in crate::partition) struct ReadPermit {
    core: Arc<RedisPartitionRuntime>,
    source: Arc<SourceAuthority>,
    released: bool,
}

impl Drop for ReadPermit {
    /// 业务作用：取消或空响应时归还尚未移交记录的全部预留，不影响已登记责任。
    /// 参数说明: 无。
    /// 返回：释放本次来源读取占位和 I/O 引用，并唤醒公平队列。
    fn drop(&mut self) {
        if !self.released {
            let mut state = self.core.state.lock().expect("ledger");
            if let Some((records, bytes)) = state.reads.remove(&self.source.epoch) {
                state.reserved_records -= records;
                state.bytes -= bytes;
                state.domains[self.source.domain].records -= records;
                state.domains[self.source.domain].bytes -= bytes;
                state.domains[self.source.domain].batches -= 1;
                self.source.io.fetch_sub(1, Ordering::AcqRel);
            }
            self.core.changed.notify_waiters();
        }
    }
}

/// 业务作用：签发所有本地 task/gate 身份，耗尽时永久关闭根准入。
/// 参数说明：`state` 为权威写锁。
/// 返回：不复用身份；溢出时不产生部分身份。
fn issue(state: &mut State) -> Option<u64> {
    let id = state.next_identity;
    match id.checked_add(1) {
        Some(next) => {
            state.next_identity = next;
            Some(id)
        }
        None => {
            state.protocol_closed = true;
            None
        }
    }
}

/// 业务作用：释放终态记录的全部本地引用并维护同 key 队头，Retained 不形成永久墓碑缓存。
/// 参数说明：`core` 负责延迟归还；`state` 为账本；`coordinate` 为精确坐标；`committed` 表示服务端提交已确认。
/// 返回：记录从账本移除；未执行载荷连同容量和来源责任由调用方在账本锁外释放。
fn retire<'a>(
    core: &'a RedisPartitionRuntime,
    state: &mut State,
    coordinate: &Coordinate,
    committed: bool,
) -> Option<RetiredPayload<'a>> {
    detach_gate(state, coordinate);
    if let Some(record) = state.records.remove(coordinate) {
        let payload = state.prepared.remove(coordinate);
        if committed {
            state.committed = state.committed.saturating_add(1);
        } else {
            state.retained = state.retained.saturating_add(1);
        }
        if let Some(payload) = payload {
            // 析构仍可能执行应用逻辑，先保留容量与来源引用，再离开账本临界区。
            state.reserved_records += 1;
            record.source.io.fetch_add(1, Ordering::AcqRel);
            return Some(RetiredPayload {
                _payload: payload,
                _budget: RetiredPayloadBudget {
                    core,
                    source: record.source,
                    bytes: record.bytes,
                },
            });
        }
        state.bytes -= record.bytes;
        state.domains[record.source.domain].records -= 1;
        state.domains[record.source.domain].bytes -= record.bytes;
    }
    None
}

/// 业务作用：ACK 确认或责任交回后解除该记录的顺序依赖，删除队列背压不撤销确认事实。
/// 参数说明：`state` 为账本；`coordinate` 为已取得明确终态的记录。
/// 返回：记录退出 gate，仍可由 Commit 持有删除交接与容量责任。
fn detach_gate(state: &mut State, coordinate: &Coordinate) {
    if let Some(key) = state
        .records
        .get_mut(coordinate)
        .and_then(|record| record.gate.take())
    {
        if let Some(gate) = state.gates.get_mut(&key) {
            gate.queue.retain(|candidate| candidate != coordinate);
            if gate.queue.is_empty() && gate.executing.is_none() {
                state.gates.remove(&key);
            }
        }
    }
}

/// 业务作用：按 Redis Stream ID 数值顺序比较，同毫秒的序号也保持稳定顺序。
/// 参数说明：`left`、`right` 为服务端记录 ID。
/// 返回：毫秒与序号二元组的顺序，非法 ID 落固定最小值并由协议层拒绝。
fn stream_id_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    let parse = |value: &str| {
        value
            .split_once('-')
            .and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))
            .unwrap_or_default()
    };
    parse(left).cmp(&parse(right))
}

#[path = "engine_io.rs"]
mod io;
use io::next_io;
