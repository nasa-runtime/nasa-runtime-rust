//! 与线上消费组隔离的有限原文处理，checkpoint lease 覆盖实际 worker 寿命。

use super::{ReplayError, ReplayPlan, ReplayResult};
use crate::{
    CheckpointError, CheckpointLease, CheckpointStore, KafkaConfig, KafkaHeader, KafkaHeaders,
    KafkaProxy, KafkaProxyInner, ReplayCheckpoint, ReplayPosition,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

const OPERATION_BUDGET: Duration = Duration::from_secs(10);

/// 框架内部任务依赖，激活前不允许任何来源或目的副作用。
pub(crate) trait ReplayParticipant: Send + Sync {
    /// 业务作用：等待双方登记完整，停止请求优先于开放执行。
    /// 参数说明：stopping 为来源当前取消状态。返回：仅双方允许时开放，否则取消本实例。
    fn activate(&self, stopping: &dyn Fn() -> bool) -> bool;
    /// 业务作用：在真实 worker 与持久 lease 全部退出后归还另一来源的依赖责任。
    /// 参数说明：无。返回：移除匹配实例，不能提前释放 native 责任。
    fn finished(&self);
}

/// 业务作用：让所有回放共用停机截止时间，超时不丢弃真实责任。
/// 参数说明：handles 为已通知停止的集合，deadline 为来源总截止时间。
/// 返回：仍未排干的稳定任务名，实际任务仍留在来源目录。
pub(crate) async fn drain(handles: &[ReplayHandle], deadline: Instant) -> Vec<String> {
    let mut pending = Vec::new();
    for handle in handles {
        if !handle
            .await_drained(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            pending.push(handle.status().task_id);
        }
    }
    pending
}

/// 真实资源目录；topic 删除重建必须改变 generation，名称和分区数不能代替代次。
pub trait ReplayIdentityProvider: Send + Sync {
    /// 业务作用：在执行准入前取得当前资源代次证据。
    /// 参数说明：cluster_id 为观测到的物理来源，topics 为完整白名单，timeout 为剩余预算。
    /// 返回：当前完整映射；目录不可观测时必须失败，不能以旧缓存冒充当前证据。
    fn generations(
        &self,
        cluster_id: &str,
        topics: &BTreeSet<String>,
        timeout: Duration,
    ) -> ReplayResult<BTreeMap<String, String>>;
}

/// 同步成功合同；不同分区可并发调用，返回前必须等待本条全部实际副作用结束。
pub trait ReplayProcessor: Send + Sync {
    /// 业务作用：固定业务转换、schema、目的地和成功条件的共同合同。
    /// 参数说明：无。返回：与计划相同的 SHA-256 小写十六进制指纹。
    fn fingerprint(&self) -> String;
    /// 业务作用：在独占任务责任内完成本条真实处理，不允许派生未等待的后台副作用。
    /// 参数说明：record 为拥有独立原文字节的来源记录。
    /// 返回：只有全部实际副作用成功才返回 Ok；失败或 panic 不推进本条 checkpoint。
    fn process(&self, record: ReplayRecord) -> ReplayResult<()>;
}

/// 原文及稳定来源坐标；目标 offset 不能替代来源幂等身份。
#[derive(Clone, Debug)]
pub struct ReplayRecord {
    /// 物理集群 ID。
    pub cluster_id: String,
    /// 来源 topic。
    pub topic: String,
    /// 来源分区。
    pub partition: i32,
    /// 来源 offset。
    pub offset: i64,
    /// Kafka 毫秒时间；缺失为 -1。
    pub timestamp: i64,
    /// 原始 key，None 与空字节不同。
    pub key: Option<Vec<u8>>,
    /// 原始正文，None 是 tombstone。
    pub payload: Option<Vec<u8>>,
    /// 保留顺序、重复项与 null 值的消息头。
    pub headers: KafkaHeaders,
}

/// 控制请求与实际责任分离的任务状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayState {
    /// 正在验证来源和取得独占权。
    Starting,
    /// 可以接受新处理。
    Running,
    /// 已关闭新准入，尚有处理在途。
    Pausing,
    /// 已暂停且没有正在执行的处理。
    Paused,
    /// 已取消或失败，实际资源尚未全部退出。
    Stopping,
    /// 冻结范围全部持久确认且资源退出。
    Completed,
    /// 取消后实际资源退出。
    Cancelled,
    /// 失败后实际资源退出。
    Failed,
}

/// 不进行 I/O 的即时责任与持久进度快照。
#[derive(Clone, Debug)]
pub struct ReplayStatus {
    /// 固定任务身份。
    pub task_id: String,
    /// 本实例阶段。
    pub state: ReplayState,
    /// 最后确认持久进度；未接管为 None。
    pub checkpoint: Option<ReplayCheckpoint>,
    /// 首个脱敏执行失败。
    pub failure: Option<ReplayError>,
    /// 存储失败的独立类别。
    pub checkpoint_failure: Option<CheckpointError>,
    /// 框架当前驻留记录数。
    pub in_flight_records: usize,
    /// 框架按原文及固定元数据计算的驻留额度。
    pub in_flight_bytes: u64,
    /// 本实例处理且持久确认的条数。
    pub processed_records: u64,
    /// 尚未退出的分区 worker。
    pub workers: usize,
}

impl ReplayStatus {
    /// 业务作用：区分终态与仅停止准入的控制请求。
    /// 参数说明：无。返回：只有实际资源全部退出的终态为 true。
    pub fn drained(&self) -> bool {
        matches!(
            self.state,
            ReplayState::Completed | ReplayState::Cancelled | ReplayState::Failed
        )
    }
}

/// 独立任务的控制句柄；丢弃或取消等待均不释放真实任务责任。
#[derive(Clone)]
pub struct ReplayHandle {
    task: Arc<Task>,
}

impl ReplayHandle {
    /// 业务作用：暂停新处理，已进入的实际回调继续完成。
    /// 参数说明：无。返回：Paused 或 Pausing；终态与停止不能重新开放。
    pub fn pause(&self) -> ReplayStatus {
        self.task.pause(true)
    }
    /// 业务作用：恢复尚未停止的任务准入。
    /// 参数说明：无。返回：恢复后的快照，取消和失败保持关闭。
    pub fn resume(&self) -> ReplayStatus {
        self.task.pause(false)
    }
    /// 业务作用：永久关闭本实例的新准入，不打断已经发生的实际处理。
    /// 参数说明：无。返回：停止阶段；不意味着 lease 已归还。
    pub fn cancel(&self) -> ReplayStatus {
        self.task.cancel()
    }
    /// 业务作用：无 I/O 读取持久进度与在途责任。
    /// 参数说明：无。返回：不包含消息正文或连接凭据的快照。
    pub fn status(&self) -> ReplayStatus {
        self.task.status()
    }
    /// 业务作用：异步等待实际任务退出，超时或取消等待不会取消底层责任。
    /// 参数说明：timeout 为本次等待预算，零仅检查当前状态。
    /// 返回：实际排干为 true；预算耗尽为 false。
    pub async fn await_drained(&self, timeout: Duration) -> bool {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        loop {
            if self.status().drained() {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
        }
    }
}

/// 每来源固定任务和线程额度，已停止后不可重新开启。
#[derive(Default)]
pub(crate) struct ReplayManager {
    state: Mutex<ManagerState>,
}
#[derive(Default)]
struct ManagerState {
    tasks: BTreeMap<String, Arc<Task>>,
    workers: usize,
    closed: bool,
}

impl ReplayManager {
    /// 业务作用：在停机预算已耗尽时仍报告真实未退出的任务身份。
    /// 参数说明：无。返回：稳定任务名，不触发 I/O 或关闭资源。
    pub(crate) fn pending_names(&self) -> Vec<String> {
        lock(&self.state).tasks.keys().cloned().collect()
    }

    /// 业务作用：关闭准入并先通知全部任务，再由调用方等待实际退出。
    /// 参数说明：无。返回：本轮完整责任集合；任务名称在真实退出前继续占用。
    pub(crate) fn stop(&self) -> Vec<ReplayHandle> {
        let mut state = lock(&self.state);
        state.closed = true;
        state
            .tasks
            .values()
            .map(|task| {
                task.cancel();
                ReplayHandle {
                    task: Arc::clone(task),
                }
            })
            .collect()
    }
    /// 业务作用：仅在实际资源关闭后移除匹配实例并归还线程额度。
    /// 参数说明：task 为本次已退出实例。返回：不影响同名后续任务。
    fn finished(&self, task: &Arc<Task>) {
        let mut state = lock(&self.state);
        if state
            .tasks
            .get(&task.plan.task_id)
            .is_some_and(|current| Arc::ptr_eq(current, task))
        {
            state.tasks.remove(&task.plan.task_id);
            state.workers -= task.worker_limit;
        }
    }
}

impl KafkaProxy {
    /// 业务作用：在当前来源登记有界回放责任，不加入或提交线上消费组。
    /// 参数说明：plan 为冻结合同，store 为持久后端，identity 为真实代次目录，processor 为同步成功合同。
    /// 返回：异步准备的控制句柄；关闭、合同不符、同名活动任务或预算不足立即拒绝。
    pub fn replay(
        &self,
        plan: ReplayPlan,
        store: Arc<dyn CheckpointStore>,
        identity: Arc<dyn ReplayIdentityProvider>,
        processor: Arc<dyn ReplayProcessor>,
    ) -> ReplayResult<ReplayHandle> {
        self.replay_participating(plan, store, identity, processor, None)
    }

    /// 业务作用：登记可选框架依赖，在任务开放前完成双方生命周期绑定。
    /// 参数说明：plan/store/identity/processor 为完整回放合同，participant 为内置目标依赖。
    /// 返回：受管句柄；任何同步登记失败都不会启动未登记的任务。
    pub(crate) fn replay_participating(
        &self,
        plan: ReplayPlan,
        store: Arc<dyn CheckpointStore>,
        identity: Arc<dyn ReplayIdentityProvider>,
        processor: Arc<dyn ReplayProcessor>,
        participant: Option<Arc<dyn ReplayParticipant>>,
    ) -> ReplayResult<ReplayHandle> {
        plan.validate()?;
        let fingerprint =
            isolate(|| processor.fingerprint()).map_err(|_| ReplayError::ProcessingFailed)?;
        if plan.source_id != self.config().client_name || plan.pipeline_fingerprint != fingerprint {
            return Err(ReplayError::ContractMismatch);
        }
        let mut manager = lock(&self.inner.replays.state);
        // 与 stop 共用登记门，保证关闭快照之外不会出现新的实际任务。
        if manager.closed
            || matches!(
                self.inner.lifecycle(),
                crate::Lifecycle::ShuttingDown | crate::Lifecycle::Stopped
            )
        {
            return Err(ReplayError::Closed);
        }
        if manager.tasks.contains_key(&plan.task_id) {
            return Err(ReplayError::Busy);
        }
        let count = (plan.limits.concurrency as usize).min(plan.ranges.len());
        if manager.tasks.len() >= 16 || manager.workers + count > 64 {
            return Err(ReplayError::Capacity);
        }
        let record_limit = (self.config().behavior.max_record_bytes as u64)
            .min(plan.limits.max_in_flight_bytes / count as u64);
        if record_limit == 0 {
            return Err(ReplayError::InvalidConfiguration);
        }
        let task = Arc::new(Task {
            source: Arc::downgrade(&self.inner),
            config: self.config().clone(),
            topics: plan.topic_generations.keys().cloned().collect(),
            plan,
            store,
            identity,
            processor,
            participant,
            worker_limit: count,
            record_limit,
            control: Mutex::new(Control::default()),
            changed: Condvar::new(),
            persistence: Mutex::new(Persistence::default()),
            cursor: AtomicUsize::new(0),
        });
        manager
            .tasks
            .insert(task.plan.task_id.clone(), Arc::clone(&task));
        manager.workers += count;
        let running = Arc::clone(&task);
        if std::thread::Builder::new()
            .name("nasa-replay-coordinator".into())
            .spawn(move || running.run())
            .is_err()
        {
            manager.tasks.remove(&task.plan.task_id);
            manager.workers -= count;
            return Err(ReplayError::Capacity);
        }
        Ok(ReplayHandle { task })
    }
}

#[derive(Default)]
struct Control {
    initialized: bool,
    paused: bool,
    cancelled: bool,
    done: bool,
    failure: Option<ReplayError>,
    checkpoint_failure: Option<CheckpointError>,
    published: Option<ReplayCheckpoint>,
    in_flight: usize,
    bytes: u64,
    active: usize,
    workers: usize,
    processed: u64,
    next_record: Option<Instant>,
    next_bytes: Option<Instant>,
}
#[derive(Default)]
struct Persistence {
    lease: Option<Box<dyn CheckpointLease>>,
    blocked: bool,
}
struct Task {
    source: Weak<KafkaProxyInner>,
    config: KafkaConfig,
    plan: ReplayPlan,
    topics: BTreeSet<String>,
    store: Arc<dyn CheckpointStore>,
    identity: Arc<dyn ReplayIdentityProvider>,
    processor: Arc<dyn ReplayProcessor>,
    participant: Option<Arc<dyn ReplayParticipant>>,
    worker_limit: usize,
    record_limit: u64,
    control: Mutex<Control>,
    changed: Condvar,
    persistence: Mutex<Persistence>,
    cursor: AtomicUsize,
}

impl Task {
    /// 业务作用：生成控制锁内一致的任务状态。
    /// 参数说明：state 为当前控制与责任计数。返回：仅真实退出后显示终态。
    fn snapshot(&self, state: &Control) -> ReplayStatus {
        let phase = if state.done {
            if state.failure.is_some() {
                ReplayState::Failed
            } else if state
                .published
                .as_ref()
                .is_some_and(ReplayCheckpoint::complete)
            {
                ReplayState::Completed
            } else {
                ReplayState::Cancelled
            }
        } else if state.cancelled || state.failure.is_some() {
            ReplayState::Stopping
        } else if !state.initialized {
            ReplayState::Starting
        } else if state.paused {
            if state.active == 0 {
                ReplayState::Paused
            } else {
                ReplayState::Pausing
            }
        } else {
            ReplayState::Running
        };
        ReplayStatus {
            task_id: self.plan.task_id.clone(),
            state: phase,
            checkpoint: state.published.clone(),
            failure: state.failure,
            checkpoint_failure: state.checkpoint_failure,
            in_flight_records: state.in_flight,
            in_flight_bytes: state.bytes,
            processed_records: state.processed,
            workers: state.workers,
        }
    }
    /// 业务作用：读取本实例的即时责任。参数说明：无。返回：不触发外部操作的快照。
    fn status(&self) -> ReplayStatus {
        self.snapshot(&lock(&self.control))
    }
    /// 业务作用：以同一门线性化暂停和处理准入。参数说明：value 为暂停状态。返回：不能重开已停止实例。
    fn pause(&self, value: bool) -> ReplayStatus {
        let mut state = lock(&self.control);
        if !stopping(&state) {
            state.paused = value;
        }
        self.changed.notify_all();
        self.snapshot(&state)
    }
    /// 业务作用：永久关闭新准入，保留在途回调和 lease。参数说明：无。返回：停止状态。
    fn cancel(&self) -> ReplayStatus {
        let mut state = lock(&self.control);
        if !state.done {
            state.cancelled = true;
        }
        self.changed.notify_all();
        self.snapshot(&state)
    }
    /// 业务作用：保留首个稳定失败并唤醒全部等待者。参数说明：error 为执行类别，checkpoint 为可选存储类别。返回：新准入关闭，实际责任继续排干。
    fn fail(&self, error: ReplayError, checkpoint: Option<CheckpointError>) {
        let mut state = lock(&self.control);
        state.failure.get_or_insert(error);
        if state.checkpoint_failure.is_none() {
            state.checkpoint_failure = checkpoint;
        }
        self.changed.notify_all();
    }
    /// 业务作用：取得持久独占权后启动固定 worker，全部真实退出后才释放所有权和登记。
    /// 参数说明：无。返回：关闭失败期间保持 Stopping 并持续持有任务额度。
    fn run(self: Arc<Self>) {
        let mut workers = Vec::new();
        let result =
            isolate(|| -> ReplayResult<()> {
                // 目标停机可能先于来源句柄绑定；未完整登记前不得取得 lease 或发送第一条消息。
                if self.participant.as_ref().is_some_and(|dependency| {
                    !dependency.activate(&|| stopping(&lock(&self.control)))
                }) {
                    self.cancel();
                    return Ok(());
                }
                let initial = self.plan.initial_checkpoint()?;
                let previous = self
                    .checkpoint_call(|| self.store.load(&self.plan.task_id))?
                    .unwrap_or_else(|| initial.clone());
                self.verify(&previous, None)?;
                if stopping(&lock(&self.control)) {
                    return Ok(());
                }
                // 验证来源不等于取得写权；claim 持久化代次成功后才允许业务副作用。
                let lease = self.checkpoint_call(|| self.store.claim(&initial))?;
                lock(&self.persistence).lease = Some(lease);
                let checkpoint = self.checkpoint_call(|| {
                    Ok(lock(&self.persistence).lease.as_ref().unwrap().checkpoint())
                })?;
                lock(&self.control).published = Some(checkpoint.clone());
                self.verify(&checkpoint, None)?;
                lock(&self.control).initialized = true;
                if checkpoint.complete() || stopping(&lock(&self.control)) {
                    return Ok(());
                }
                for _ in 0..self.worker_limit {
                    lock(&self.control).workers += 1;
                    let worker = Arc::clone(&self);
                    match std::thread::Builder::new()
                        .name("nasa-replay-partition".into())
                        .spawn(move || worker.worker())
                    {
                        Ok(handle) => workers.push(handle),
                        Err(_) => {
                            lock(&self.control).workers -= 1;
                            return Err(ReplayError::Capacity);
                        }
                    }
                }
                Ok(())
            })
            .unwrap_or(Err(ReplayError::ObservationFailed));
        if let Err(error) = result {
            self.fail(error, None);
        }
        for worker in workers {
            if worker.join().is_err() {
                self.fail(ReplayError::ProcessingFailed, None);
            }
        }
        // 超时或取消等待不能代替真实退出；close 失败也不能把任务交给新执行者。
        loop {
            let result = isolate(|| {
                let mut persistence = lock(&self.persistence);
                persistence
                    .lease
                    .as_mut()
                    .map_or(Ok(()), |lease| lease.close())
            })
            .unwrap_or(Err(CheckpointError::OutcomeUnknown));
            match result {
                Ok(()) => break,
                Err(error) => self.fail(ReplayError::CheckpointFailed, Some(error)),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Some(dependency) = &self.participant {
            dependency.finished();
        }
        if let Some(source) = self.source.upgrade() {
            source.replays.finished(&self);
        }
        lock(&self.control).done = true;
        self.changed.notify_all();
    }
    /// 业务作用：隔离存储插件 panic 并保留存储失败类别。参数说明：call 为真实同步存储操作。返回：确认结果；失败关闭准入。
    fn checkpoint_call<T>(
        &self,
        call: impl FnOnce() -> crate::CheckpointResult<T>,
    ) -> ReplayResult<T> {
        match isolate(call).unwrap_or(Err(CheckpointError::OutcomeUnknown)) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.fail(ReplayError::CheckpointFailed, Some(error));
                Err(ReplayError::CheckpointFailed)
            }
        }
    }
    /// 业务作用：领取不重叠的分区，在每个分区内维持严格串行。
    /// 参数说明：无。返回：范围耗尽或停止后退出；panic 同样关闭新准入。
    fn worker(&self) {
        let result = isolate(|| -> ReplayResult<()> {
            while !stopping(&lock(&self.control)) {
                let index = self.cursor.fetch_add(1, Ordering::Relaxed);
                if index >= self.plan.ranges.len() {
                    break;
                }
                let range =
                    lock(&self.control).published.as_ref().unwrap().positions[index].clone();
                if range.next_offset < range.end_offset {
                    self.consume(index, range)?;
                }
            }
            Ok(())
        })
        .unwrap_or(Err(ReplayError::ProcessingFailed));
        if let Err(error) = result {
            self.fail(error, None);
        }
        lock(&self.control).workers -= 1;
        self.changed.notify_all();
    }
    /// 业务作用：在副作用或无记录边界推进前重新证明来源和保留范围。
    /// 参数说明：checkpoint 为最后确认进度，consumer 为已有分区客户端，协调阶段传 None。返回：物理身份、代次、分区和范围全部吻合才通过。
    fn verify(
        &self,
        checkpoint: &ReplayCheckpoint,
        consumer: Option<&BaseConsumer>,
    ) -> ReplayResult<()> {
        let deadline = Instant::now() + OPERATION_BUDGET;
        let observed = match consumer {
            Some(consumer) => {
                super::inspect::inspect_consumer(consumer, &self.topics, None, deadline)?
            }
            None => {
                super::inspect::inspect(self.config.clone(), self.topics.clone(), None, deadline)?
            }
        };
        let generations =
            self.identity
                .generations(&observed.cluster_id, &self.topics, remaining(deadline)?)?;
        remaining(deadline)?;
        self.plan
            .validate_resume(&observed, &generations, checkpoint)
    }
    /// 业务作用：将后端权威复验与分区持久推进串行，已知失权后停止新的业务派发。
    /// 参数说明：无。返回：当前 lease 权威仍成立；任何后端失败或 panic 关闭整个任务的推进门禁。
    fn verify_authority(&self) -> ReplayResult<()> {
        let mut persistence = lock(&self.persistence);
        if persistence.blocked {
            return Err(ReplayError::CheckpointFailed);
        }
        let result = isolate(|| {
            persistence
                .lease
                .as_mut()
                .ok_or(CheckpointError::Closed)?
                .verify()
        })
        .unwrap_or(Err(CheckpointError::Fenced));
        if let Err(error) = result {
            // 不能依赖自定义后端自行停止其它分区；先保护整个任务再发布失败，保留已在途副作用的责任。
            persistence.blocked = true;
            self.fail(ReplayError::CheckpointFailed, Some(error));
            return Err(ReplayError::CheckpointFailed);
        }
        Ok(())
    }
    /// 业务作用：合并分区完成证据并先持久化再公开，写入未知后禁止后续写入。
    /// 参数说明：index 为冻结分区下标，next 为已成功的下一坐标。返回：持久确认后的进度；失败保持原公开快照。
    fn advance(&self, index: usize, next: i64) -> ReplayResult<()> {
        let mut persistence = lock(&self.persistence);
        // 插件可能未自行封禁；执行器必须阻止其它 worker 在不确定写入之后继续推进。
        if persistence.blocked {
            return Err(ReplayError::CheckpointFailed);
        }
        let result = isolate(|| {
            let lease = persistence.lease.as_mut().ok_or(CheckpointError::Closed)?;
            let current = lease.checkpoint();
            if next <= current.positions[index].next_offset {
                return Ok(current);
            }
            let mut positions = current.positions;
            positions[index].next_offset = next;
            lease.advance(current.revision, positions)
        })
        .unwrap_or(Err(CheckpointError::OutcomeUnknown));
        match result {
            Ok(checkpoint) => {
                lock(&self.control).published = Some(checkpoint);
                Ok(())
            }
            Err(error) => {
                persistence.blocked = true;
                self.fail(ReplayError::CheckpointFailed, Some(error));
                Err(ReplayError::CheckpointFailed)
            }
        }
    }
    /// 业务作用：暂停期间停止拉取，取消或失败后永久拒绝继续。
    /// 参数说明：无。返回：允许读取时 true，退出时 false。
    fn await_unpaused(&self) -> bool {
        let mut state = lock(&self.control);
        while state.paused && !stopping(&state) {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        !stopping(&state)
    }
    /// 业务作用：用同一门实施全任务速率并授权真实处理。
    /// 参数说明：size 为驻留原文额度。返回：取得处理权为 true；取消或失败为 false，最多一条初始突发。
    fn admit(&self, size: u64) -> ReplayResult<bool> {
        let mut state = lock(&self.control);
        while !stopping(&state) {
            let now = Instant::now();
            let wait = state
                .next_record
                .map_or(Duration::ZERO, |at| at.saturating_duration_since(now))
                .max(
                    state
                        .next_bytes
                        .map_or(Duration::ZERO, |at| at.saturating_duration_since(now)),
                );
            if !state.paused && wait.is_zero() {
                state.next_record = now.checked_add(delay(1, self.plan.limits.records_per_second));
                state.next_bytes = now.checked_add(delay(size, self.plan.limits.bytes_per_second));
                if state.next_record.is_none() || state.next_bytes.is_none() {
                    return Err(ReplayError::Capacity);
                }
                state.active += 1;
                return Ok(true);
            }
            state = self
                .changed
                .wait_timeout(
                    state,
                    if wait.is_zero() {
                        Duration::from_millis(100)
                    } else {
                        wait.min(Duration::from_millis(100))
                    },
                )
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        Ok(false)
    }
    /// 业务作用：以独立单分区 consumer 执行冻结原文范围，跳过事务控制记录而不移动线上 group。
    /// 参数说明：index 为持久进度位置，range 为本分区恢复范围。返回：全部完成或真实停止，消费资源随函数退出释放。
    fn consume(&self, index: usize, range: ReplayPosition) -> ReplayResult<()> {
        let mut native = crate::rd::config::consumer_config(
            &self.config,
            &format!("nasa-replay-{}", self.plan.fingerprint()?),
        )
        .map_err(|_| ReplayError::InvalidConfiguration)?;
        // 原生 fetch 总量不能小于 message.max.bytes，协议帧另需空间；框架记录额度仍在交付前独立检查。
        let native_limit = self.record_limit.clamp(1024, 1_000_000_000);
        native
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("isolation.level", "read_committed")
            .set("allow.auto.create.topics", "false")
            .set("auto.offset.reset", "error")
            .set("enable.partition.eof", "true")
            .set("queued.min.messages", "1")
            .set(
                "queued.max.messages.kbytes",
                self.record_limit
                    .div_ceil(1024)
                    .min(i32::MAX as u64)
                    .to_string(),
            )
            .set("fetch.message.max.bytes", native_limit.to_string())
            .set("message.max.bytes", native_limit.to_string())
            .set("fetch.max.bytes", native_limit.to_string())
            .set(
                "receive.message.max.bytes",
                (native_limit + 512).max(1_000_000).to_string(),
            );
        native.remove("group.instance.id");
        let consumer: BaseConsumer = native
            .create()
            .map_err(|_| ReplayError::InvalidConfiguration)?;
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(
                &range.topic,
                range.partition,
                Offset::Offset(range.next_offset),
            )
            .map_err(|_| ReplayError::InvalidConfiguration)?;
        consumer
            .assign(&assignment)
            .map_err(|_| ReplayError::ObservationFailed)?;
        while self.await_unpaused() {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(message)) => {
                    let checkpoint = lock(&self.control).published.clone().unwrap();
                    if message.offset() < checkpoint.positions[index].next_offset {
                        return Err(ReplayError::ObservationFailed);
                    }
                    if message.offset() >= range.end_offset {
                        self.verify(&checkpoint, Some(&consumer))?;
                        self.advance(index, range.end_offset)?;
                        return Ok(());
                    }
                    let size = record_size(&message);
                    if size > self.record_limit {
                        return Err(ReplayError::Capacity);
                    }
                    {
                        let mut state = lock(&self.control);
                        state.in_flight += 1;
                        state.bytes += size;
                    }
                    let mut residence = Residence {
                        task: self,
                        size,
                        active: false,
                    };
                    if !self.admit(size)? {
                        return Ok(());
                    }
                    residence.active = true;
                    // 等待暂停或速率额度可能跨越资源变化，准入后必须取得新证据再调用业务。
                    let current = lock(&self.control).published.clone().unwrap();
                    self.verify(&current, Some(&consumer))?;
                    if self.processor.fingerprint() != self.plan.pipeline_fingerprint {
                        return Err(ReplayError::ContractMismatch);
                    }
                    // 来源查询和速率等待之后复验 lease，已失去推进权时不能再开启业务副作用。
                    self.verify_authority()?;
                    let input = ReplayRecord {
                        cluster_id: self.plan.cluster_id.clone(),
                        topic: message.topic().to_owned(),
                        partition: message.partition(),
                        offset: message.offset(),
                        timestamp: message.timestamp().to_millis().unwrap_or(-1),
                        key: message.key().map(<[u8]>::to_vec),
                        payload: message.payload().map(<[u8]>::to_vec),
                        headers: KafkaHeaders::from_vec(
                            message
                                .headers()
                                .map(|headers| {
                                    headers
                                        .iter()
                                        .map(|header| KafkaHeader {
                                            name: header.key.to_owned(),
                                            value: header.value.map(<[u8]>::to_vec),
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                        ),
                    };
                    isolate(|| self.processor.process(input))
                        .map_err(|_| ReplayError::ProcessingFailed)?
                        .map_err(|_| ReplayError::ProcessingFailed)?;
                    // 已接纳处理的成功证据可在取消后持久化；等待取消不能把真实成功变成没有发生。
                    self.advance(index, message.offset() + 1)?;
                    lock(&self.control).processed += 1;
                }
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) | None => {}
                Some(Err(error)) => {
                    if error
                        .rdkafka_error_code()
                        .is_some_and(crate::rd::errors::authorization)
                    {
                        return Err(ReplayError::Authorization);
                    }
                    return Err(
                        if error.rdkafka_error_code()
                            == Some(rdkafka::error::RDKafkaErrorCode::OffsetOutOfRange)
                        {
                            ReplayError::DataGap
                        } else {
                            ReplayError::ObservationFailed
                        },
                    );
                }
            }
            if stopping(&lock(&self.control)) {
                return Ok(());
            }
            // position 仅在本轮交付全部成功后使用；控制标记和空洞不要求存在最后一条业务记录。
            let positions = consumer
                .position()
                .map_err(|_| ReplayError::ObservationFailed)?;
            if positions.find_partition(&range.topic, range.partition).is_some_and(|position| matches!(position.offset(), Offset::Offset(value) if value >= range.end_offset)) {
                let checkpoint = lock(&self.control).published.clone().unwrap();
                self.verify(&checkpoint, Some(&consumer))?; self.advance(index, range.end_offset)?; return Ok(());
            }
        }
        Ok(())
    }
}

struct Residence<'a> {
    task: &'a Task,
    size: u64,
    active: bool,
}
impl Drop for Residence<'_> {
    /// 业务作用：只有实际回调和持久操作退出后才归还驻留额度，panic 路径同样保留此顺序。
    /// 参数说明：无。返回：更新实际计数并唤醒控制等待。
    fn drop(&mut self) {
        let mut state = lock(&self.task.control);
        if self.active {
            state.active -= 1;
        }
        state.in_flight -= 1;
        state.bytes -= self.size;
        self.task.changed.notify_all();
    }
}

/// 业务作用：使状态锁 panic 后仍可执行保护与排干。参数说明：mutex 为受控内部状态。返回：已有状态，后续业务门仍显式复验。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
/// 业务作用：统一新处理停止门。参数说明：state 为当前控制状态。返回：取消、失败或终态为 true。
fn stopping(state: &Control) -> bool {
    state.cancelled || state.failure.is_some() || state.done
}
/// 业务作用：为来源查询和目录证明共用总预算。参数说明：deadline 为单调时钟截止值。返回：正剩余时间；耗尽拒绝。
fn remaining(deadline: Instant) -> ReplayResult<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(ReplayError::Timeout)
    } else {
        Ok(left)
    }
}
/// 业务作用：用共同原文与固定元数据公式计算单条驻留额度。参数说明：message 为原生记录。返回：保留 null 与空值语义的计费字节数。
fn record_size(message: &rdkafka::message::BorrowedMessage<'_>) -> u64 {
    128 + message.topic().len() as u64
        + message.key().map_or(0, |value| value.len() as u64)
        + message.payload().map_or(0, |value| value.len() as u64)
        + message.headers().map_or(0, |headers| {
            headers
                .iter()
                .map(|header| {
                    16 + header.key.len() as u64
                        + header.value.map_or(0, |value| value.len() as u64)
                })
                .sum::<u64>()
        })
}
/// 业务作用：向上取整速率间隔，零代表不节流。参数说明：amount 为条数或字节，rate 为每秒额度。返回：无乘法溢出的非负等待。
fn delay(amount: u64, rate: u64) -> Duration {
    if rate == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(
            ((amount as u128 * 1_000_000_000).div_ceil(rate as u128)).min(u64::MAX as u128) as u64,
        )
    }
}

/// 业务作用：隔离业务与存储插件的 panic，避免异常载荷析构再次中断实际责任收尾。
/// 参数说明：operation 为同步扩展调用。返回：正常结果或无敏感正文的失败；二次异常载荷保留而不继续析构。
pub(crate) fn isolate<T>(operation: impl FnOnce() -> T) -> std::result::Result<T, ()> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(value) => Ok(value),
        Err(payload) => {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(secondary);
            }
            Err(())
        }
    }
}
