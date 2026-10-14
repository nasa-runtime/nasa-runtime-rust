//! 事务专属身份目录、整批权威和实际资源寿命。

use super::{
    TransactionError as Error, TransactionPlan, TransactionProcessor, TransactionResult as Result,
};
use crate::{KafkaConfig, KafkaProxy, KafkaProxyInner, Tp};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::{Duration, Instant},
};

/// 专属事务 owner 的业务阶段。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionState {
    /// 正在核对集群与初始化稳定 producer 身份。
    Starting,
    /// 已初始化，等待已分配的输入。
    Idle,
    /// 执行同步整批回调并暂存输出。
    Processing,
    /// 使用专属 producer 发送事务内部输出。
    Sending,
    /// 将本批完整输入位点登记到事务。
    Preparing,
    /// commit 已进入完成过程，不能因取消切换到 abort。
    Committing,
    /// 等待整批 abort 的实际确认。
    Aborting,
    /// 已确认中止，等待有界重试退避。
    Backoff,
    /// 新准入已关闭，实际资源尚未完全退出。
    Stopping,
    /// 取消或正常关闭后的实际资源终态。
    Stopped,
    /// 实际资源已退出，保留固定失败与输入证据。
    Failed,
}

/// 真实执行状态；drained 不代表未知 broker 事务已被认定成功或中止。
#[derive(Clone, Debug)]
pub struct TransactionStatus {
    /// 双端共同派生的稳定 producer 身份。
    pub transactional_id: String,
    /// 本 owner 专属消费组。
    pub group: String,
    /// 当前事务阶段。
    pub state: TransactionState,
    /// 分配权威的本地代次。
    pub assignment_epoch: u64,
    /// 当前实际分配集合。
    pub assignment: BTreeSet<Tp>,
    /// 初始化、有效分配和业务准入同时成立的快照。
    pub ready: bool,
    /// 回调、producer 与 consumer 全部实际退出。
    pub drained: bool,
    /// 固定失败分类，成功或仅取消时为空。
    pub failure: Option<Error>,
    /// commit 已确认的批次数。
    pub committed_batches: u64,
    /// commit 已确认的输入数。
    pub committed_inputs: u64,
    /// commit 已确认的输出数。
    pub committed_outputs: u64,
    /// abort 已确认的批次数。
    pub aborted_batches: u64,
    /// 当前或最后批次的来源起点，未知提交时保留。
    pub input_starts: BTreeMap<Tp, i64>,
    /// 当前或最后批次每分区的下一消费位点。
    pub input_ends: BTreeMap<Tp, i64>,
}

/// 绑定真实执行实例的事务控制句柄。
#[derive(Clone)]
pub struct TransactionHandle {
    pub(crate) task: Arc<Task>,
}

impl TransactionHandle {
    /// 业务作用：取得当前阶段、权威与已确认成功计数。
    /// 参数说明：无。返回：不发起网络请求的独立快照。
    pub fn status(&self) -> TransactionStatus {
        self.task.status()
    }

    /// 业务作用：关闭新批次并撤销尚未 commit 的实际业务权威。
    /// 参数说明：无。返回：即时快照，在途 commit 仍等待真实结果。
    pub fn cancel(&self) -> TransactionStatus {
        self.task.cancel();
        self.status()
    }

    /// 业务作用：在共同绝对期限内等待当前有效分配和事务初始化。
    /// 参数说明：deadline 为本次等待的绝对截止时刻。返回：就绪快照；失败、取消或超时拒绝。
    pub async fn await_ready(&self, deadline: Instant) -> Result<TransactionStatus> {
        loop {
            let notified = self.task.changed.notified();
            let status = self.status();
            if status.ready {
                return Ok(status);
            }
            if let Some(error) = status.failure {
                return Err(error);
            }
            if self.task.stopping() || status.drained {
                return Err(Error::Closed);
            }
            tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), notified)
                .await
                .map_err(|_| Error::TransactionTimeout)?;
        }
    }

    /// 业务作用：等待实际资源退出，调用方取消等待不解除旧 owner 的责任。
    /// 参数说明：deadline 为所有收尾共用的绝对截止时刻。返回：全部退出为 true，超时为 false。
    pub async fn await_drained(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.task.changed.notified();
            if self.status().drained {
                return true;
            }
            if tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), notified)
                .await
                .is_err()
            {
                return false;
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Manager {
    state: Mutex<Directory>,
}
#[derive(Default)]
struct Directory {
    closed: bool,
    tasks: BTreeMap<String, Arc<Task>>,
}
static IDENTITIES: OnceLock<Mutex<BTreeSet<(String, String)>>> = OnceLock::new();

impl Manager {
    /// 业务作用：读取尚未实际归还的事务资源数，不改变 owner 准入。
    /// 参数说明：无。
    /// 返回：目录当前责任数，业务结局继续从 TransactionHandle 查询。
    pub(crate) fn active_count(&self) -> usize {
        lock(&self.state).tasks.len()
    }
    /// 业务作用：封闭新事务准入并同时通知所有 owner。
    /// 参数说明：无。返回：关闭流程必须排干的真实实例集合。
    pub(crate) fn stop(&self) -> Vec<TransactionHandle> {
        let mut state = lock(&self.state);
        state.closed = true;
        state
            .tasks
            .values()
            .map(|task| {
                task.cancel();
                TransactionHandle {
                    task: Arc::clone(task),
                }
            })
            .collect()
    }

    /// 业务作用：阻止普通消费组注册与现有事务组共享提交责任。
    /// 参数说明：group 为拟注册的完整组名。返回：该组是否仍有实际事务 owner。
    pub(crate) fn contains(&self, group: &str) -> bool {
        lock(&self.state).tasks.contains_key(group)
    }
}

impl KafkaProxy {
    /// 业务作用：登记同集群消费、输出与组位点的专属原子 owner。
    /// 参数说明：plan 为完整固定合同，processor 为同步有界处理入口。返回：已纳入两段停机的句柄，最多16个实际事务 owner；非法组或配置返回 InvalidConfiguration，隔离冲突为 IsolationMismatch。不支持手动 assign 或异步按键处理。
    pub async fn transact(
        &self,
        plan: TransactionPlan,
        processor: Arc<dyn TransactionProcessor>,
    ) -> Result<TransactionHandle> {
        // 公开计划可由业务修改，必须在占用组与事务身份之前再次验证，不能依赖调用方曾执行过预检。
        plan.validate()?;
        if plan.source_id != self.config().client_name {
            return Err(Error::InvalidConfiguration);
        }
        // 与普通消费注册和停机共用生命周期门，不允许两类提交机制在快照外竞争同一组。
        let _lifecycle = self.inner.consumer_lifecycle_gate.lock().await;
        let mut directory = lock(&self.inner.transactions.state);
        if directory.closed
            || matches!(
                self.inner.lifecycle(),
                crate::Lifecycle::ShuttingDown | crate::Lifecycle::Stopped
            )
        {
            return Err(Error::Closed);
        }
        if directory.tasks.contains_key(&plan.group)
            || self
                .inner
                .groups
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&plan.group)
        {
            return Err(Error::Busy);
        }
        if directory.tasks.len() >= 16 {
            return Err(Error::Capacity);
        }
        crate::rd::transaction::validate(self.config(), &plan)?;
        let id = plan.identity.transactional_id()?;
        let identity = (plan.cluster_id.clone(), id.clone());
        if !lock(IDENTITIES.get_or_init(Mutex::default)).insert(identity.clone()) {
            return Err(Error::Busy);
        }
        let task = Arc::new(Task::new(
            Arc::downgrade(&self.inner),
            self.config().clone(),
            plan,
            processor,
            id,
            self.inner.runtime.clone(),
        ));
        directory
            .tasks
            .insert(task.plan.group.clone(), Arc::clone(&task));
        let worker = Arc::clone(&task);
        if std::thread::Builder::new()
            .name("nafka-transaction-poll".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::rd::transaction::run(&worker)
                }));
                if let Err(error) = result.unwrap_or(Err(Error::BrokerError)) {
                    worker.fail(error);
                }
                worker.finish();
            })
            .is_err()
        {
            directory.tasks.remove(&task.plan.group);
            lock(IDENTITIES.get_or_init(Mutex::default)).remove(&identity);
            return Err(Error::Capacity);
        }
        Ok(TransactionHandle { task })
    }
}

pub(crate) struct Task {
    source: Weak<KafkaProxyInner>,
    pub(crate) config: KafkaConfig,
    pub(crate) plan: Arc<TransactionPlan>,
    pub(crate) processor: Arc<dyn TransactionProcessor>,
    pub(crate) runtime: tokio::runtime::Handle,
    state: Mutex<Control>,
    changed: tokio::sync::Notify,
}
struct Control {
    status: TransactionStatus,
    cancelled: bool,
    initialized: bool,
    active: Option<Active>,
}
struct Active {
    epoch: u64,
    deadline: Instant,
    handler_deadline: Instant,
    rejection: Option<Error>,
}

impl Task {
    /// 业务作用：为本批死信保留门禁复用源级管理资源，服从最终关闭边界。
    /// 参数说明：无。返回：真实源仍存活时的共享Admin；退出或不可构造时拒绝，不创建业务句柄。
    pub(crate) fn dlt_admin(&self) -> Result<Arc<crate::rd::admin::AdminHandle>> {
        let source = self.source.upgrade().ok_or(Error::Closed)?;
        crate::admin::shared_client(&source).map_err(|_| Error::BrokerError)
    }
    /// 业务作用：固定任务依赖和初始观测，不在构造时建立原生事务。
    /// 参数说明：source 为生命周期弱引用，config/plan 为冻结配置，processor 为业务，id 为稳定事务 ID，runtime 用于等待原生交付。返回：尚未初始化的责任实例。
    fn new(
        source: Weak<KafkaProxyInner>,
        config: KafkaConfig,
        plan: TransactionPlan,
        processor: Arc<dyn TransactionProcessor>,
        id: String,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let status = TransactionStatus {
            transactional_id: id,
            group: plan.group.clone(),
            state: TransactionState::Starting,
            assignment_epoch: 0,
            assignment: BTreeSet::new(),
            ready: false,
            drained: false,
            failure: None,
            committed_batches: 0,
            committed_inputs: 0,
            committed_outputs: 0,
            aborted_batches: 0,
            input_starts: BTreeMap::new(),
            input_ends: BTreeMap::new(),
        };
        Self {
            source,
            config,
            plan: Arc::new(plan),
            processor,
            runtime,
            state: Mutex::new(Control {
                status,
                cancelled: false,
                initialized: false,
                active: None,
            }),
            changed: tokio::sync::Notify::new(),
        }
    }

    /// 业务作用：计算每次查询时仍有效的 Ready，不锁存第一次成功。
    /// 参数说明：无。返回：当前业务权威和资源状态。
    pub(crate) fn status(&self) -> TransactionStatus {
        let state = lock(&self.state);
        let mut status = state.status.clone();
        status.ready = state.initialized
            && !state.cancelled
            && status.failure.is_none()
            && !status.assignment.is_empty()
            && state.active.as_ref().is_none_or(|a| a.rejection.is_none());
        status
    }

    /// 业务作用：撤销尚未进入 commit 的批次；已进入 commit 的操作继续承担实际责任。
    /// 参数说明：无。返回：永久关闭新准入并通知等待方。
    pub(crate) fn cancel(&self) {
        let mut state = lock(&self.state);
        state.cancelled = true;
        if state.status.state != TransactionState::Committing {
            if let Some(active) = &mut state.active {
                active.rejection.get_or_insert(Error::Cancelled);
            }
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：读取取消与固定失败共同形成的永久准入门。
    /// 参数说明：无。返回：不能开始新批次时为 true。
    pub(crate) fn stopping(&self) -> bool {
        let state = lock(&self.state);
        state.cancelled || state.status.failure.is_some()
    }

    /// 业务作用：确认中止后的原生 producer 重建期间撤销业务就绪，但保留稳定身份和实际资源责任。
    /// 参数说明：无。返回：新 producer 完成集群复验与初始化前不能开放下一批。
    pub(crate) fn recovering_producer(&self) {
        let mut state = lock(&self.state);
        state.initialized = false;
        state.status.state = TransactionState::Starting;
        self.changed.notify_waiters();
    }

    /// 业务作用：在稳定事务身份初始化成功后开放分配观测。
    /// 参数说明：无。返回：进入 Idle，仍需实际 assignment 才能 Ready。
    pub(crate) fn initialized(&self) {
        let mut state = lock(&self.state);
        state.initialized = true;
        state.status.state = TransactionState::Idle;
        self.changed.notify_waiters();
    }

    /// 业务作用：使新的定位代次立即撤销旧批次的组 metadata 权威。
    /// 参数说明：partitions 为变更集合，add 表示取得或撤回。返回：更新分配与代次，在途 commit 不被改为 abort。
    pub(crate) fn assignment_changed(&self, partitions: BTreeSet<Tp>, add: bool) {
        let mut state = lock(&self.state);
        state.status.assignment_epoch += 1;
        if add {
            state.status.assignment.extend(partitions);
        } else {
            for tp in partitions {
                state.status.assignment.remove(&tp);
            }
        }
        if state.status.state != TransactionState::Committing {
            if let Some(active) = &mut state.active {
                active.rejection.get_or_insert(Error::Rebalanced);
            }
        }
        self.changed.notify_waiters();
    }

    /// 业务作用：将完整输入责任固定到当前分配代次。
    /// 参数说明：starts/ends 为批次各分区完整输入范围。返回：本批定位代次与不可续期的事务截止时间。
    pub(crate) fn begin(
        &self,
        starts: BTreeMap<Tp, i64>,
        ends: BTreeMap<Tp, i64>,
    ) -> Result<(u64, Instant)> {
        let mut state = lock(&self.state);
        if state.cancelled {
            return Err(Error::Cancelled);
        }
        if !starts.keys().all(|tp| state.status.assignment.contains(tp)) {
            return Err(Error::Rebalanced);
        }
        let epoch = state.status.assignment_epoch;
        let now = Instant::now();
        let deadline = now + Duration::from_millis(self.plan.limits.transaction_timeout_ms.into());
        state.status.input_starts = starts;
        state.status.input_ends = ends;
        state.status.state = TransactionState::Processing;
        state.active = Some(Active {
            epoch,
            deadline,
            handler_deadline: now
                + Duration::from_millis(self.plan.limits.handler_timeout_ms.into()),
            rejection: None,
        });
        Ok((epoch, deadline))
    }

    /// 业务作用：以同一控制锁复验当前批次并转换副作用阶段，commit 准入不能与失权通知交错。
    /// 参数说明：epoch 为冻结代次，stage 为可选下一阶段。返回：取消、超时或权威变化拒绝新副作用。
    pub(crate) fn check(&self, epoch: u64, stage: Option<TransactionState>) -> Result<()> {
        let mut state = lock(&self.state);
        let active = state.active.as_ref().ok_or(Error::ContextClosed)?;
        if let Some(error) = active.rejection {
            return Err(error);
        }
        if state.cancelled {
            return Err(Error::Cancelled);
        }
        if active.epoch != epoch || state.status.assignment_epoch != epoch {
            return Err(Error::Rebalanced);
        }
        if Instant::now() >= active.deadline {
            return Err(Error::TransactionTimeout);
        }
        if state.status.state == TransactionState::Processing
            && Instant::now() >= active.handler_deadline
        {
            return Err(Error::HandlerTimeout);
        }
        if let Some(stage) = stage {
            state.status.state = stage;
        }
        Ok(())
    }

    /// 业务作用：让持续 poll 的 owner 撤销超时回调，不通过中断伪装回调已经退出。
    /// 参数说明：无。返回：超时立即令 Ready 失效，实际线程继续受管。
    pub(crate) fn expire(&self) {
        let mut state = lock(&self.state);
        let stage = state.status.state;
        if matches!(
            stage,
            TransactionState::Committing | TransactionState::Aborting
        ) {
            return;
        }
        if let Some(active) = &mut state.active {
            if Instant::now() >= active.deadline {
                active.rejection.get_or_insert(Error::TransactionTimeout);
            } else if stage == TransactionState::Processing
                && Instant::now() >= active.handler_deadline
            {
                active.rejection.get_or_insert(Error::HandlerTimeout);
            }
        }
    }

    /// 业务作用：发布已经取得原生确认的 commit 计数。
    /// 参数说明：inputs/outputs 为完整批次条数。返回：只有真实 commit 成功才增加可见成功量。
    pub(crate) fn committed(&self, inputs: usize, outputs: usize) {
        let mut state = lock(&self.state);
        state.status.committed_batches += 1;
        state.status.committed_inputs += inputs as u64;
        state.status.committed_outputs += outputs as u64;
    }

    /// 业务作用：将整批带入合法中止阶段，不开放后续事务。
    /// 参数说明：无。返回：开始等待原生 abort 确认。
    pub(crate) fn aborting(&self) {
        lock(&self.state).status.state = TransactionState::Aborting;
    }

    /// 业务作用：登记已经确认作废的完整事务。
    /// 参数说明：无。返回：增加已确认中止数，不推进任何输入成功位点。
    pub(crate) fn aborted(&self) {
        lock(&self.state).status.aborted_batches += 1;
    }

    /// 业务作用：实际批次线程退出后归还单批槽位。
    /// 参数说明：backoff 表示是否进入已确认 abort 后的退避。返回：清理当前权威，保留最后批次输入证据。
    pub(crate) fn settled(&self, backoff: bool) {
        let mut state = lock(&self.state);
        state.active = None;
        state.status.state = if backoff {
            TransactionState::Backoff
        } else {
            TransactionState::Idle
        };
        self.changed.notify_waiters();
    }

    /// 业务作用：保留首个固定失败并封闭 owner 的后续工作。
    /// 参数说明：error 为脱敏分类。返回：立即撤销 Ready，实际资源由原生 owner 排干。
    pub(crate) fn fail(&self, error: Error) {
        let mut state = lock(&self.state);
        state.status.failure.get_or_insert(error);
        state.cancelled = true;
        self.changed.notify_waiters();
    }

    /// 业务作用：只有原生运行函数和其批次线程全部返回后才释放稳定身份。
    /// 参数说明：无。返回：目录和实际身份归还，等待方可观察 drained。
    fn finish(&self) {
        let id = self.status().transactional_id;
        if let Some(source) = self.source.upgrade() {
            lock(&source.transactions.state)
                .tasks
                .remove(&self.plan.group);
        }
        lock(IDENTITIES.get_or_init(Mutex::default)).remove(&(self.plan.cluster_id.clone(), id));
        let mut state = lock(&self.state);
        state.initialized = false;
        state.status.assignment.clear();
        state.status.drained = true;
        state.status.state = if state.status.failure.is_some() {
            TransactionState::Failed
        } else {
            TransactionState::Stopped
        };
        self.changed.notify_waiters();
    }
}

/// 业务作用：让两段停机共享同一绝对期限，不因事务数量增加总预算。
/// 参数说明：tasks 为关闭时冻结的责任，deadline 为总截止时间。返回：尚未实际退出的稳定事务任务名。
pub(crate) async fn drain(tasks: &[TransactionHandle], deadline: Instant) -> Vec<String> {
    let mut pending = Vec::new();
    for task in tasks {
        if !task.await_drained(deadline).await {
            pending.push(format!("transaction:{}", task.status().group));
        }
    }
    pending
}

/// 业务作用：保护受管权威状态，panic 后仍保留并排干实际责任。
/// 参数说明：mutex 为内部状态锁。返回：取得原状态，不因 poison 丢失目录或身份。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
