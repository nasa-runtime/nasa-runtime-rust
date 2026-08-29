//! 运行时编排：把执行器注册、心跳、调度扫描、Dispatch 消费与可见性提升组合成受监督的后台任务集合。
//!
//! 启动时登记本节点全部 Worker 能力；每个活跃调度分片一个循环，依次扫描触发、消费执行与提升可见成员；
//! 心跳循环单独续期存活。停机先停止接受新工作、等待各循环退出，再注销本执行器，避免半注销状态进入 Fanout 快照。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::error::Result;
use crate::job::api::{classify_error, JobSourceHealthReason, SourceHealth};
use crate::job::config::JobConfig;
use crate::job::coordinator::{FanoutCoordinator, FanoutService};
use crate::job::definition::JobDefinition;
use crate::job::dispatcher::{JobDispatcher, JobDispatcherPollError};
use crate::job::fanout::FanoutRepository;
use crate::job::fanout_dispatcher::{FanoutDispatcher, FanoutPollError};
use crate::job::handler::JobHandler;
use crate::job::keyspace::JobKeyspace;
use crate::job::metrics::{JobMetrics, JobSupervisorLoop};
use crate::job::model::{JobConcurrency, JobExecutorState};
use crate::job::monitor::FanoutMonitor;
use crate::job::registry::{
    ExecutorIdentity, ExecutorRegistry, HeartbeatOutcome, RegisterCapabilityOutcome,
};
use crate::job::repository::JobRepository;
use crate::job::scanner::JobScanner;

const SUPERVISOR_MAX_RESTARTS: usize = 5;
const SUPERVISOR_RESTART_WINDOW: Duration = Duration::from_secs(60);
const SUPERVISOR_BACKOFF_BASE: Duration = Duration::from_millis(100);
const SUPERVISOR_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// 单个循环实例的有界重启预算；失败窗口和退避只存在本地，不进入持久调度协议。
struct LoopRestartBudget {
    failures: VecDeque<std::time::Instant>,
    consecutive: u32,
    recovering: bool,
}

impl LoopRestartBudget {
    /// 业务作用：建立尚未消耗预算的首代循环监督状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空失败窗口、非恢复态的预算。
    fn new() -> Self {
        Self {
            failures: VecDeque::new(),
            consecutive: 0,
            recovering: false,
        }
    }

    /// 业务作用：登记一次完整退出后的失败，按固定窗口限制重启并计算有上限的指数退避。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预算尚存时给出退避时长与是否首次进入 Degraded；预算耗尽返回 `None`。
    fn failed(&mut self) -> Option<(Duration, bool)> {
        let now = std::time::Instant::now();
        while self
            .failures
            .front()
            .is_some_and(|instant| now.duration_since(*instant) > SUPERVISOR_RESTART_WINDOW)
        {
            self.failures.pop_front();
        }
        if self.failures.len() >= SUPERVISOR_MAX_RESTARTS {
            return None;
        }
        self.failures.push_back(now);
        self.consecutive = self.consecutive.saturating_add(1);
        let shift = self.consecutive.saturating_sub(1).min(16);
        let multiplier = 1_u32 << shift;
        let delay = SUPERVISOR_BACKOFF_BASE
            .checked_mul(multiplier)
            .unwrap_or(SUPERVISOR_BACKOFF_MAX)
            .min(SUPERVISOR_BACKOFF_MAX);
        let first_failure = !self.recovering;
        self.recovering = true;
        Some((delay, first_failure))
    }

    /// 业务作用：在新代次完成一次成功推进后结束当前恢复窗口并重置连续退避级别。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：此前确实处于恢复态时为 true，调用方据此清除本循环的 Degraded 事实。
    fn succeeded(&mut self) -> bool {
        self.consecutive = 0;
        std::mem::replace(&mut self.recovering, false)
    }
}

/// 心跳权威的本地保守截止与跨调用恢复状态；截止只由服务端成功回包推进。
struct HeartbeatAuthority {
    state: std::sync::Mutex<HeartbeatAuthorityState>,
}

/// 业务作用：保存心跳成功确认的本地保守截止、服务端 revision 与连续恢复状态。
struct HeartbeatAuthorityState {
    deadline: Instant,
    heartbeat_revision: i64,
    consecutive_failures: u32,
    recovering: bool,
}

impl HeartbeatAuthority {
    /// 业务作用：以启动登记成功回包折算的本地保守截止建立心跳权威窗口。
    ///
    /// 参数说明：
    /// - `deadline`：不晚于 Redis 已确认的执行器过期时刻。
    /// - `heartbeat_revision`：同一回包确认的心跳修订号，后续续租据此 fencing。
    ///
    /// 返回：尚未进入恢复态的共享权威窗口。
    fn new(deadline: Instant, heartbeat_revision: i64) -> Self {
        Self {
            state: std::sync::Mutex::new(HeartbeatAuthorityState {
                deadline,
                heartbeat_revision,
                consecutive_failures: 0,
                recovering: false,
            }),
        }
    }

    /// 业务作用：读取距离最后一次已确认执行器过期时刻的剩余本地预算。
    ///
    /// 参数说明：`now` 为调用方同一单调时钟的当前时刻。
    ///
    /// 返回：截止已到时为零，否则返回保守剩余时间。
    fn remaining(&self, now: Instant) -> Duration {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .deadline
            .saturating_duration_since(now)
    }

    /// 业务作用：读取最后一次服务端成功回包确认的心跳修订号，作为下一次续租的 fencing 依据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前本地已确认的 `heartbeatRevision`。
    fn heartbeat_revision(&self) -> i64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .heartbeat_revision
    }

    /// 业务作用：用成功心跳的 Redis 权威时刻推进本地截止，并结束当前传输恢复窗口。
    ///
    /// 参数说明：
    /// - `attempt_started`：发起 Redis 命令前的本地单调时刻，用于给网络往返留出保守余量。
    /// - `redis_now`：脚本执行时的 Redis 毫秒时刻。
    /// - `expire_at`：脚本确认的新执行器过期毫秒时刻。
    /// - `heartbeat_revision`：脚本确认的新心跳修订号。
    ///
    /// 返回：此前处于传输恢复态时为真，调用方据此发布 recovered 状态。
    fn succeeded(
        &self,
        attempt_started: Instant,
        redis_now: i64,
        expire_at: i64,
        heartbeat_revision: i64,
    ) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.deadline = conservative_authority_deadline(attempt_started, redis_now, expire_at);
        state.heartbeat_revision = heartbeat_revision;
        state.consecutive_failures = 0;
        std::mem::replace(&mut state.recovering, false)
    }

    /// 业务作用：登记一次截止前的心跳传输失败，并计算不越过硬截止的指数退避基值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：退避时长与是否首次进入恢复态；次数不单独终止权威，硬截止才是最终门禁。
    fn failed(&self) -> (Duration, bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        let shift = state.consecutive_failures.saturating_sub(1).min(16);
        let multiplier = 1_u32 << shift;
        let delay = SUPERVISOR_BACKOFF_BASE
            .checked_mul(multiplier)
            .unwrap_or(SUPERVISOR_BACKOFF_MAX)
            .min(SUPERVISOR_BACKOFF_MAX);
        let first_failure = !state.recovering;
        state.recovering = true;
        (delay, first_failure)
    }
}

/// 业务作用：把 Redis 返回的权威租约窗口折算为不晚于服务端截止的本地单调时刻。
///
/// 参数说明：
/// - `attempt_started`：命令发起前的本地时刻。
/// - `redis_now`：脚本执行时的 Redis 毫秒时刻。
/// - `expire_at`：脚本确认的过期毫秒时刻。
///
/// 返回：异常或溢出输入收敛到命令开始时刻；正常输入返回开始时刻加服务端剩余窗口。
fn conservative_authority_deadline(
    attempt_started: Instant,
    redis_now: i64,
    expire_at: i64,
) -> Instant {
    let remaining_ms = expire_at
        .checked_sub(redis_now)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0);
    attempt_started
        .checked_add(Duration::from_millis(remaining_ms))
        .unwrap_or(attempt_started)
}

/// 业务作用：声明心跳在取得串行发送权后应采用的状态来源，避免排队任务携带过期状态覆盖新权威。
#[derive(Clone, Copy)]
enum HeartbeatStateIntent {
    /// 周期续期读取锁内最新准入状态。
    CurrentAdmission,
    /// 订阅失联明确发布保护态。
    Draining,
    /// 新订阅 ACK 后发布 ACTIVE，并在同一锁内开放周期心跳状态。
    RecoverActive,
}

/// 业务作用：把本地 Fanout 准入状态与本次心跳意图绑定，确保二者只能在线性化锁内解释。
#[derive(Clone, Copy)]
struct HeartbeatStateRequest<'a> {
    fanout_ready: &'a AtomicBool,
    intent: HeartbeatStateIntent,
}

/// 业务作用：在最后一次已确认执行器截止内重试心跳传输错误，并区分真实失权与短暂不可达。
///
/// 参数说明：
/// - `registry`：执行器持久注册表。
/// - `heartbeat_lock`：串行化不同运行循环发出的心跳命令。
/// - `authority`：仅由成功回包推进的共享权威窗口。
/// - `health`：发布 Degraded、recovered 与监督代次的 source 健康账目。
/// - `stop`：正常停机或其它终态的取消信号。
/// - `state_request`：取得心跳锁后才能解释的本地准入状态与业务意图。
/// - `inflight`：当前在途执行数。
///
/// 返回：成功或 `NotFound` 返回封闭结局；正常停机返回空；协议错误或已确认截止耗尽返回错误。
async fn heartbeat_with_authority(
    registry: &ExecutorRegistry,
    heartbeat_lock: &tokio::sync::Mutex<()>,
    authority: &HeartbeatAuthority,
    health: &SourceHealth,
    stop: &CancellationToken,
    state_request: HeartbeatStateRequest<'_>,
    inflight: u32,
) -> Result<Option<HeartbeatOutcome>> {
    let lock_started = Instant::now();
    let lock_remaining = authority.remaining(lock_started);
    if lock_remaining.is_zero() {
        return Err(crate::job::JobError::ExecutionStopped(
            "RedisJob executor 已超过最后一次确认的心跳存活截止".to_owned(),
        )
        .into());
    }
    // 同一执行器只能有一个逻辑心跳处于重发窗口，否则其它循环推进 revision 后，旧请求可能再次续租。
    let _heartbeat = tokio::select! {
        _ = stop.cancelled() => return Ok(None),
        result = tokio::time::timeout(lock_remaining, heartbeat_lock.lock()) => match result {
            Ok(guard) => guard,
            Err(_) => {
                return Err(crate::job::JobError::ExecutionStopped(
                    "RedisJob executor 未能在最后一次确认的存活截止前取得心跳发送权".to_owned(),
                ).into());
            }
        }
    };
    // 锁与取消同时就绪时 select 不保证选择取消分支；取得发送权后必须再复验单调终态。
    if stop.is_cancelled() {
        return Ok(None);
    }
    let state = match state_request.intent {
        HeartbeatStateIntent::CurrentAdmission => {
            if state_request.fanout_ready.load(Ordering::Acquire) {
                JobExecutorState::Active
            } else {
                JobExecutorState::Draining
            }
        }
        HeartbeatStateIntent::Draining => JobExecutorState::Draining,
        HeartbeatStateIntent::RecoverActive => JobExecutorState::Active,
    };
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let expected_revision = authority.heartbeat_revision();
    loop {
        let attempt_started = Instant::now();
        let remaining = authority.remaining(attempt_started);
        if remaining.is_zero() {
            return Err(crate::job::JobError::ExecutionStopped(
                "RedisJob executor 已超过最后一次确认的心跳存活截止".to_owned(),
            )
            .into());
        }
        let attempt =
            registry.heartbeat_idempotent(state, inflight, &request_id, expected_revision);
        let result = tokio::select! {
            _ = stop.cancelled() => return Ok(None),
            result = tokio::time::timeout(remaining, attempt) => match result {
                Ok(result) => result,
                Err(_) => {
                    return Err(crate::job::JobError::ExecutionStopped(
                        "RedisJob executor 心跳未能在最后一次确认的存活截止前完成".to_owned(),
                    ).into());
                }
            }
        };
        match result {
            Ok(
                outcome @ HeartbeatOutcome::Ok {
                    redis_now,
                    expire_at,
                    heartbeat_revision,
                },
            ) => {
                let Some(next_revision) = expected_revision.checked_add(1) else {
                    return Err(crate::job::JobError::FencingRegression(
                        "RedisJob executor heartbeatRevision 已达到数值上限".to_owned(),
                    )
                    .into());
                };
                if heartbeat_revision != next_revision {
                    return Err(crate::job::JobError::FencingRegression(format!(
                        "RedisJob executor 心跳修订号不连续: expected={next_revision}, actual={heartbeat_revision}"
                    ))
                    .into());
                }
                let recovered =
                    authority.succeeded(attempt_started, redis_now, expire_at, heartbeat_revision);
                if authority.remaining(Instant::now()).is_zero() {
                    return Err(crate::job::JobError::ExecutionStopped(
                        "RedisJob executor 心跳回包晚于可安全采用的存活截止".to_owned(),
                    )
                    .into());
                }
                if recovered {
                    health.recovered(JobSupervisorLoop::Heartbeat);
                }
                if matches!(state_request.intent, HeartbeatStateIntent::RecoverActive) {
                    // ACTIVE 确认与本地 ready 必须在同一心跳锁内发布，排队的周期心跳随后只能读到新状态。
                    state_request.fanout_ready.store(true, Ordering::Release);
                    // 取消可能与本次 Redis 回包并发；先写再复验，取消方和本分支至少一方会把 ready 压回 false。
                    if stop.is_cancelled() {
                        state_request.fanout_ready.store(false, Ordering::Release);
                        return Ok(None);
                    }
                }
                return Ok(Some(outcome));
            }
            Ok(HeartbeatOutcome::NotFound) => return Ok(Some(HeartbeatOutcome::NotFound)),
            Err(error) if classify_error(&error) == JobSourceHealthReason::RedisTransport => {
                let remaining = authority.remaining(Instant::now());
                if remaining.is_zero() {
                    return Err(crate::job::JobError::ExecutionStopped(
                        "RedisJob executor 心跳传输失败且已超过最后一次确认的存活截止".to_owned(),
                    )
                    .into());
                }
                let (delay, first_failure) = authority.failed();
                health.restart_scheduled(
                    JobSupervisorLoop::Heartbeat,
                    JobSourceHealthReason::RedisTransport,
                    first_failure,
                );
                if !wait_backoff(stop, delay.min(remaining)).await {
                    return Ok(None);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

/// 业务作用：等待扫描周期或监督退避，同时让 source 取消立即打断等待。
///
/// 参数说明：`stop` 为当前 source 的取消令牌，`delay` 为本轮等待时长。
///
/// 返回：完整等到时限为 true；source 已取消为 false。
async fn wait_backoff(stop: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// 业务作用：把可恢复循环错误归入健康快照允许公开的封闭原因，不泄露 endpoint 或业务字段。
///
/// 参数说明：`error` 为本代循环返回的内部错误。
///
/// 返回：稳定、低基数的恢复原因。
/// 运行时构建器：收集连接、配置、启动代次与本地 (定义, Handler) 登记，启动时组装后台任务。
pub struct RedisJobRuntime {
    client: Arc<RedisClient>,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
    startup_id: String,
    registrations: Vec<(JobDefinition, Arc<dyn JobHandler>)>,
    metrics: Arc<JobMetrics>,
}

impl RedisJobRuntime {
    /// 业务作用：以连接、键模型、配置与本次启动代次创建运行时构建器。
    ///
    /// 参数说明：
    /// - `client`/`keyspace`/`config`: 连接、冻结键模型与已校验配置。
    /// - `startup_id`: 本次进程启动的唯一标识，参与执行器身份。
    ///
    /// 返回：尚未启动的运行时构建器。
    pub fn new(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        startup_id: impl Into<String>,
    ) -> Self {
        let metrics = Arc::new(JobMetrics::new(keyspace.qualifier()));
        client.attach_job_metrics(&metrics);
        Self {
            client,
            keyspace,
            config,
            startup_id: startup_id.into(),
            registrations: Vec::new(),
            metrics,
        }
    }

    /// 业务作用：登记一个任务定义与其 Worker 能力的本地 Handler，启动后由调度与消费循环处理。
    ///
    /// 参数说明：
    /// - `definition`: 任务定义。
    /// - `handler`: 该 Worker 能力的本地 Handler。
    ///
    /// 返回：无返回值。
    pub fn register(&mut self, definition: JobDefinition, handler: Arc<dyn JobHandler>) {
        self.registrations.push((definition, handler));
    }

    /// 业务作用：记录准备阶段观测到的定义冲突，使同 source generation 的固定指标能解释启动拒绝原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；只累计预登记 counter，不改变定义状态。
    pub(crate) fn record_definition_conflict(&self) {
        self.metrics.add("redis_job_definition_conflict_total", 1);
    }

    /// 业务作用：登记本节点执行器与全部定义，仅为 FANOUT_ONLY 发布能力，再按活跃分片启动后台循环与心跳。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：注册成功后返回可停机的运行时句柄；执行器身份非法或能力登记被拒时返回错误。
    pub(crate) async fn start(self) -> Result<SourceRunningJobRuntime> {
        if self.registrations.is_empty() {
            return Err(crate::job::JobError::Config(
                "RedisJob source 至少需要一条本地定义才能建立执行器心跳权威".to_owned(),
            )
            .into());
        }
        let identity = ExecutorIdentity::from_config(&self.config, &self.startup_id)?;
        let executor_id = identity.executor_id().to_owned();
        let node_identity = identity.node_identity().to_owned();
        let metrics = self.metrics.clone();
        let health = Arc::new(SourceHealth::new(
            self.keyspace.qualifier(),
            metrics.clone(),
        ));
        // Fanout 频道必须在能力记录发布 fanoutReady 之前全部取得 ACK，避免快照选中尚无法接收定向投递的节点。
        let mut fanout_dispatcher = FanoutDispatcher::start(
            self.client.clone(),
            self.keyspace.clone(),
            self.config.clone(),
            executor_id.clone(),
            node_identity,
            metrics.clone(),
        )
        .await?;
        let registry = Arc::new(ExecutorRegistry::new(
            self.client.clone(),
            self.keyspace.clone(),
            self.config.clone(),
            identity,
        ));
        // 先登记执行器事实与 FANOUT_ONLY 能力，再开放调度与消费，避免半登记成员进入 Fanout 快照。
        // 每次成功登记同时返回服务端存活截止与 revision；取最后一次回包作为周期心跳的初始权威证据。
        let mut initial_authority = None;
        for (definition, _) in &self.registrations {
            let register_started = Instant::now();
            let registration = registry
                .register(definition, JobExecutorState::Active)
                .await;
            let outcome = match registration {
                Ok(outcome) => outcome,
                Err(error) => {
                    // Pub/Sub 已经开放连接，且此前能力可能已经提交；返回启动失败前必须先关本地准入并
                    // 尽力全量注销，known_workers 会覆盖回包丢失的能力坐标。
                    fanout_dispatcher.stop_accepting();
                    let _ = registry.unregister().await;
                    return Err(error);
                }
            };
            match outcome {
                RegisterCapabilityOutcome::Ok {
                    redis_now,
                    expire_at,
                    heartbeat_revision,
                } => {
                    initial_authority = Some((
                        conservative_authority_deadline(register_started, redis_now, expire_at),
                        heartbeat_revision,
                    ));
                }
                RegisterCapabilityOutcome::WorkerKeyConflict => {
                    fanout_dispatcher.stop_accepting();
                    let _ = registry.unregister().await;
                    return Err(crate::job::JobError::Protocol(format!(
                        "RedisJob Worker key 与既有能力冲突: {}",
                        definition.worker_name()
                    ))
                    .into());
                }
                RegisterCapabilityOutcome::ContractMismatch => {
                    fanout_dispatcher.stop_accepting();
                    let _ = registry.unregister().await;
                    return Err(crate::job::JobError::ContractMismatch(format!(
                        "RedisJob Worker 合同与既有能力冲突: {}",
                        definition.worker_name()
                    ))
                    .into());
                }
            }
        }

        let mut scanner = JobScanner::new(
            self.client.clone(),
            self.keyspace.clone(),
            self.config.clone(),
        );
        let mut dispatcher = JobDispatcher::new_with_metrics(
            self.client.clone(),
            self.keyspace.clone(),
            self.config.clone(),
            executor_id.clone(),
            metrics.clone(),
        );
        let mut monitor = FanoutMonitor::new_with_metrics(
            self.client.clone(),
            self.keyspace.clone(),
            self.config.clone(),
            registry.clone(),
            metrics.clone(),
        );
        let mut active_shards: Vec<u32> = Vec::new();
        let mut definitions = HashMap::new();
        for (definition, handler) in &self.registrations {
            if definition.trigger() == crate::job::model::JobTrigger::FanoutOnly {
                fanout_dispatcher.register_handler(definition.clone(), handler.clone());
            } else {
                scanner.register_definition(definition.clone());
                dispatcher.register_handler(definition.clone(), handler.clone());
                let shard = self.keyspace.schedule_shard(definition.name());
                if !active_shards.contains(&shard) {
                    active_shards.push(shard);
                }
            }
            monitor.register_definition(definition.clone());
            definitions.insert(definition.name().to_owned(), definition.clone());
        }
        let definitions = Arc::new(definitions);
        let fanout_service = FanoutService::new(
            registry.clone(),
            FanoutCoordinator::new_with_metrics(
                self.client.clone(),
                self.keyspace.clone(),
                self.config.clone(),
                executor_id.clone(),
                metrics.clone(),
            ),
            definitions.clone(),
            self.config.clone(),
            executor_id.clone(),
        );
        dispatcher.set_fanout_service(fanout_service);
        let scanner = Arc::new(scanner);
        let dispatcher = Arc::new(dispatcher);
        let monitor = Arc::new(monitor);
        let fanout_dispatcher = Arc::new(fanout_dispatcher);
        let repository = JobRepository::new(
            self.client.clone(),
            self.keyspace.clone(),
            self.config.clone(),
        );

        let stop = CancellationToken::new();
        let fanout_ready = Arc::new(AtomicBool::new(true));
        let heartbeat_lock = Arc::new(tokio::sync::Mutex::new(()));
        // 心跳只能继承成功登记返回的服务端存活窗口；证据缺失时拒绝启动，不能伪造本地截止后
        // 再让后台循环以失权形式退出，否则装配错误会被误报成运行期权威丢失。
        let (initial_authority_deadline, initial_heartbeat_revision) = initial_authority
            .ok_or_else(|| {
                crate::job::JobError::Config(
                    "RedisJob source 未取得执行器登记返回的初始心跳权威".to_owned(),
                )
            })?;
        let heartbeat_authority = Arc::new(HeartbeatAuthority::new(
            initial_authority_deadline,
            initial_heartbeat_revision,
        ));
        let mut handles = Vec::new();

        // 定向 Push 只负责低延迟定位；持久 receipt/ready 索引会补偿丢失通知，订阅代次断开则关闭 source 准入。
        {
            let fanout_dispatcher = fanout_dispatcher.clone();
            let registry = registry.clone();
            let stop = stop.clone();
            let health = health.clone();
            let fanout_ready = fanout_ready.clone();
            let heartbeat_lock = heartbeat_lock.clone();
            let heartbeat_authority = heartbeat_authority.clone();
            let metrics = metrics.clone();
            handles.push(tokio::spawn(async move {
                let mut budget = LoopRestartBudget::new();
                while !stop.is_cancelled() {
                    match fanout_dispatcher.poll().await {
                        Ok(()) => {
                            if budget.succeeded() {
                                health.recovered(JobSupervisorLoop::Subscription);
                            }
                        }
                        Err(FanoutPollError::Authority(error)) => {
                            stop.cancel();
                            fanout_ready.store(false, Ordering::Release);
                            fanout_dispatcher.stop_accepting();
                            health.authority_lost(JobSupervisorLoop::Dispatcher, &error);
                            return Err(error);
                        }
                        Err(FanoutPollError::Subscription(mut error)) => {
                            let recovery_started = std::time::Instant::now();
                            // 断线代次必须先退出快照，心跳循环通过同一 gate 不会把 ready 提前改回 true。
                            fanout_ready.store(false, Ordering::Release);
                            match heartbeat_with_authority(
                                &registry,
                                &heartbeat_lock,
                                &heartbeat_authority,
                                &health,
                                &stop,
                                HeartbeatStateRequest {
                                    fanout_ready: &fanout_ready,
                                    intent: HeartbeatStateIntent::Draining,
                                },
                                0,
                            )
                            .await
                            {
                                Ok(Some(HeartbeatOutcome::Ok { .. })) => {}
                                Ok(Some(HeartbeatOutcome::NotFound)) => {
                                    let authority = crate::error::NasaRedisError::from(
                                        crate::job::JobError::ExecutionStopped(
                                            "RESP3 断线时 executor 主记录已失效".to_owned(),
                                        ),
                                    );
                                    health.authority_lost(JobSupervisorLoop::Heartbeat, &authority);
                                    stop.cancel();
                                    return Err(authority);
                                }
                                Ok(None) => return Ok(()),
                                Err(authority) => {
                                    health.authority_lost(JobSupervisorLoop::Heartbeat, &authority);
                                    stop.cancel();
                                    return Err(authority);
                                }
                            }
                            loop {
                                let Some((delay, first_failure)) = budget.failed() else {
                                    health.budget_exhausted(JobSupervisorLoop::Subscription);
                                    fanout_dispatcher.stop_accepting();
                                    stop.cancel();
                                    return Err(error);
                                };
                                health.restart_scheduled(
                                    JobSupervisorLoop::Subscription,
                                    JobSourceHealthReason::SubscriptionNotReady,
                                    first_failure,
                                );
                                if !wait_backoff(&stop, delay).await {
                                    return Ok(());
                                }
                                match fanout_dispatcher.restart_subscription().await {
                                    Ok(_) => {
                                        match heartbeat_with_authority(
                                            &registry,
                                            &heartbeat_lock,
                                            &heartbeat_authority,
                                            &health,
                                            &stop,
                                            HeartbeatStateRequest {
                                                fanout_ready: &fanout_ready,
                                                intent: HeartbeatStateIntent::RecoverActive,
                                            },
                                            0,
                                        )
                                        .await
                                        {
                                            Ok(Some(HeartbeatOutcome::Ok { .. })) => {
                                                let _ = budget.succeeded();
                                                health.recovered(JobSupervisorLoop::Subscription);
                                                metrics.set(
                                                    "redis_job_pubsub_recovery_window_ms",
                                                    i64::try_from(
                                                        recovery_started.elapsed().as_millis(),
                                                    )
                                                    .unwrap_or(i64::MAX),
                                                );
                                                break;
                                            }
                                            Ok(Some(HeartbeatOutcome::NotFound)) => {
                                                let authority = crate::error::NasaRedisError::from(
                                                    crate::job::JobError::ExecutionStopped(
                                                        "RESP3 恢复后 executor 主记录已失效"
                                                            .to_owned(),
                                                    ),
                                                );
                                                health.authority_lost(
                                                    JobSupervisorLoop::Heartbeat,
                                                    &authority,
                                                );
                                                stop.cancel();
                                                return Err(authority);
                                            }
                                            Ok(None) => return Ok(()),
                                            Err(authority) => {
                                                health.authority_lost(
                                                    JobSupervisorLoop::Heartbeat,
                                                    &authority,
                                                );
                                                stop.cancel();
                                                return Err(authority);
                                            }
                                        }
                                    }
                                    Err(next_error) => error = next_error,
                                }
                            }
                        }
                    }
                }
                Ok(())
            }));
        }

        // 心跳循环：周期续期执行器与能力存活。
        {
            let registry = registry.clone();
            let dispatcher = dispatcher.clone();
            let fanout_dispatcher = fanout_dispatcher.clone();
            let stop = stop.clone();
            let health = health.clone();
            let fanout_ready = fanout_ready.clone();
            let heartbeat_lock = heartbeat_lock.clone();
            let heartbeat_authority = heartbeat_authority.clone();
            let heartbeat_metrics = metrics.clone();
            let executor_capacity = self.config.executor_capacity;
            let period = Duration::from_millis(self.config.heartbeat_ms.max(1));
            handles.push(tokio::spawn(async move {
                let result = async {
                    while !stop.is_cancelled() {
                        let ordinary = dispatcher.inflight_count().await;
                        let fanout = fanout_dispatcher.inflight_count().await;
                        let inflight =
                            u32::try_from(ordinary.saturating_add(fanout)).unwrap_or(u32::MAX);
                        heartbeat_metrics.set("redis_job_running", i64::from(inflight));
                        heartbeat_metrics.set(
                            "redis_job_executor_capacity",
                            i64::from(executor_capacity.saturating_sub(inflight)),
                        );
                        match heartbeat_with_authority(
                            &registry,
                            &heartbeat_lock,
                            &heartbeat_authority,
                            &health,
                            &stop,
                            HeartbeatStateRequest {
                                fanout_ready: &fanout_ready,
                                intent: HeartbeatStateIntent::CurrentAdmission,
                            },
                            inflight,
                        )
                        .await?
                        {
                            Some(HeartbeatOutcome::Ok { .. }) => {}
                            Some(HeartbeatOutcome::NotFound) => {
                                return Err(crate::job::JobError::ExecutionStopped(
                                    "RedisJob executor 主记录已失效，当前 source 不再具备续期权威"
                                        .to_owned(),
                                )
                                .into());
                            }
                            None => break,
                        }
                        tokio::select! {
                            _ = stop.cancelled() => break,
                            _ = tokio::time::sleep(period) => {}
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = &result {
                    // 心跳失效后关闭整个 source 的新领取，避免已不可证明存活的执行器继续扩大在途工作。
                    stop.cancel();
                    fanout_ready.store(false, Ordering::Release);
                    fanout_dispatcher.stop_accepting();
                    health.authority_lost(JobSupervisorLoop::Heartbeat, error);
                }
                result
            }));
        }

        // Fanout 对账循环：逐桶扫描到期看门狗根，终态回填根 Run。
        {
            let monitor = monitor.clone();
            let stop = stop.clone();
            let health = health.clone();
            let fanout_ready = fanout_ready.clone();
            let fanout_dispatcher = fanout_dispatcher.clone();
            let bucket_count = self.config.fanout_bucket_count;
            let period = Duration::from_millis(self.config.min_scan_interval_ms.max(1));
            let registry_gc_period = Duration::from_millis(self.config.max_scan_interval_ms.max(1));
            let registry = registry.clone();
            let fanout_metrics = metrics.clone();
            handles.push(tokio::spawn(async move {
                let mut budget = LoopRestartBudget::new();
                let mut next_registry_gc = tokio::time::Instant::now();
                while !stop.is_cancelled() {
                    let result = async {
                        for bucket in 0..bucket_count {
                            monitor.reconcile_bucket(bucket).await?;
                        }
                        if tokio::time::Instant::now() >= next_registry_gc {
                            let outcome = registry.registry_gc().await?;
                            fanout_metrics.add(
                                "redis_job_registry_gc_total",
                                outcome.deleted.saturating_add(outcome.newly_expired),
                            );
                            next_registry_gc = tokio::time::Instant::now() + registry_gc_period;
                        }
                        Ok::<(), crate::error::NasaRedisError>(())
                    }
                    .await;
                    match result {
                        Ok(()) => {
                            if budget.succeeded() {
                                health.recovered(JobSupervisorLoop::FanoutMonitor);
                            }
                            if !wait_backoff(&stop, period).await {
                                break;
                            }
                        }
                        Err(error) => {
                            let Some((delay, first_failure)) = budget.failed() else {
                                health.budget_exhausted(JobSupervisorLoop::FanoutMonitor);
                                stop.cancel();
                                fanout_ready.store(false, Ordering::Release);
                                fanout_dispatcher.stop_accepting();
                                return Err(error);
                            };
                            health.restart_scheduled(
                                JobSupervisorLoop::FanoutMonitor,
                                classify_error(&error),
                                first_failure,
                            );
                            if !wait_backoff(&stop, delay).await {
                                break;
                            }
                        }
                    }
                }
                Ok(())
            }));
        }

        // 调度/恢复与阻塞消费拆成独立循环，长 Handler 不得拖住同分片租约恢复或可见性重建。
        let period = Duration::from_millis(self.config.min_scan_interval_ms.max(1));
        // Completion 在没有新完成事件时仍需按时间界回收；独立 control loop 同时覆盖普通分片与全部 Fanout 桶。
        {
            let completion_repository = repository.clone();
            let completion_monitor = monitor.clone();
            let completion_shard_count = self.config.shard_count;
            let completion_buckets = self.config.fanout_bucket_count;
            let completion_stop = stop.clone();
            let completion_health = health.clone();
            let completion_metrics = metrics.clone();
            let completion_fanout_ready = fanout_ready.clone();
            let completion_fanout_dispatcher = fanout_dispatcher.clone();
            let completion_period = Duration::from_millis(self.config.max_scan_interval_ms.max(1));
            handles.push(tokio::spawn(async move {
                let mut budget = LoopRestartBudget::new();
                while !completion_stop.is_cancelled() {
                    let result = async {
                        for shard in 0..completion_shard_count {
                            // 单轮每个分片只推进有限批次，积压再大也不能长期占用 control lane。
                            for _ in 0..10 {
                                let outcome = completion_repository.reap_deleted(shard).await?;
                                for request in &outcome.fanout_requests {
                                    completion_monitor.drive_deleted_fanout(request).await?;
                                }
                                if !outcome.active {
                                    break;
                                }
                            }
                            completion_repository.cleanup_tombstones(shard).await?;
                            let outcome = completion_repository.trim_completion(shard).await?;
                            completion_metrics.record_completion_trim(
                                outcome.length_trimmed,
                                outcome.retention_trimmed,
                                outcome.oldest_id.as_deref(),
                                outcome.redis_now,
                            );
                        }
                        for bucket in 0..completion_buckets {
                            let outcome =
                                completion_repository.trim_fanout_completion(bucket).await?;
                            completion_metrics.record_completion_trim(
                                outcome.length_trimmed,
                                outcome.retention_trimmed,
                                outcome.oldest_id.as_deref(),
                                outcome.redis_now,
                            );
                        }
                        Ok::<(), crate::error::NasaRedisError>(())
                    }
                    .await;
                    match result {
                        Ok(()) => {
                            if budget.succeeded() {
                                completion_health.recovered(JobSupervisorLoop::Completion);
                            }
                            if !wait_backoff(&completion_stop, completion_period).await {
                                break;
                            }
                        }
                        Err(error) => {
                            let Some((delay, first_failure)) = budget.failed() else {
                                completion_health.budget_exhausted(JobSupervisorLoop::Completion);
                                completion_stop.cancel();
                                completion_fanout_ready.store(false, Ordering::Release);
                                completion_fanout_dispatcher.stop_accepting();
                                return Err(error);
                            };
                            completion_health.restart_scheduled(
                                JobSupervisorLoop::Completion,
                                classify_error(&error),
                                first_failure,
                            );
                            if !wait_backoff(&completion_stop, delay).await {
                                break;
                            }
                        }
                    }
                }
                Ok(())
            }));
        }
        for shard in active_shards {
            let scanner = scanner.clone();
            let repository = repository.clone();
            let definitions = definitions.clone();
            let scan_stop = stop.clone();
            let scan_health = health.clone();
            let scan_metrics = metrics.clone();
            let scan_fanout_ready = fanout_ready.clone();
            let scan_fanout_dispatcher = fanout_dispatcher.clone();
            handles.push(tokio::spawn(async move {
                let mut budget = LoopRestartBudget::new();
                while !scan_stop.is_cancelled() {
                    let result = async {
                        let scan = scanner.scan_once_observed(shard).await?;
                        scan_metrics.add("redis_job_due_scan_total", 1);
                        scan_metrics.set_shard_gauge(
                            "redis_job_schedule_lag_ms",
                            shard,
                            scan.schedule_lag_ms,
                        );
                        for (result, total) in scan.fire_results {
                            scan_metrics.record_fire(result, total);
                        }
                        repository.promote_visible(shard).await?;
                        scan_metrics.set_shard_gauge(
                            "redis_job_visible_size",
                            shard,
                            repository.visible_size(shard).await?,
                        );
                        let mut serial_wait_depth = 0_i64;
                        for definition in definitions.values().filter(|definition| {
                            definition.concurrency() == JobConcurrency::SerialQueue
                                && repository.schedule_shard(definition.name()) == shard
                        }) {
                            serial_wait_depth = serial_wait_depth.saturating_add(
                                repository.serial_wait_depth(definition.name()).await?,
                            );
                        }
                        scan_metrics.set_shard_gauge(
                            "redis_job_serial_wait_depth",
                            shard,
                            serial_wait_depth,
                        );
                        for entry in repository.scan_leases_due(shard).await?.entries {
                            if let Some(run) =
                                repository.read_run_at_shard(shard, &entry.member).await?
                            {
                                if let Some(definition) = definitions.get(&run.job_name) {
                                    if matches!(
                                        repository
                                            .recover_expired(definition, &entry.member)
                                            .await?,
                                        crate::job::repository::RecoverOutcome::Ok { .. }
                                    ) {
                                        scan_metrics.add("redis_job_lease_expired_total", 1);
                                    }
                                }
                            }
                        }
                        for entry in repository.scan_waiting_due(shard).await?.entries {
                            if let Some(run) =
                                repository.read_run_at_shard(shard, &entry.member).await?
                            {
                                if let Some(definition) = definitions.get(&run.job_name) {
                                    repository
                                        .fail_waiting_creation(definition, &entry.member)
                                        .await?;
                                }
                            }
                        }
                        Ok::<(), crate::error::NasaRedisError>(())
                    }
                    .await;
                    match result {
                        Ok(()) => {
                            if budget.succeeded() {
                                scan_health.recovered(JobSupervisorLoop::Scanner);
                            }
                            if !wait_backoff(&scan_stop, period).await {
                                break;
                            }
                        }
                        Err(error) => {
                            let Some((delay, first_failure)) = budget.failed() else {
                                scan_health.budget_exhausted(JobSupervisorLoop::Scanner);
                                scan_stop.cancel();
                                scan_fanout_ready.store(false, Ordering::Release);
                                scan_fanout_dispatcher.stop_accepting();
                                return Err(error);
                            };
                            scan_health.restart_scheduled(
                                JobSupervisorLoop::Scanner,
                                classify_error(&error),
                                first_failure,
                            );
                            if !wait_backoff(&scan_stop, delay).await {
                                break;
                            }
                        }
                    }
                }
                Ok(())
            }));

            let dispatcher = dispatcher.clone();
            let dispatch_stop = stop.clone();
            let dispatch_health = health.clone();
            let dispatch_fanout_ready = fanout_ready.clone();
            let dispatch_fanout_dispatcher = fanout_dispatcher.clone();
            handles.push(tokio::spawn(async move {
                let mut budget = LoopRestartBudget::new();
                while !dispatch_stop.is_cancelled() {
                    match dispatcher.poll_shard_supervised(shard).await {
                        Ok(_processed) => {
                            if budget.succeeded() {
                                dispatch_health.recovered(JobSupervisorLoop::Dispatcher);
                            }
                        }
                        Err(JobDispatcherPollError::Recoverable(error)) => {
                            let Some((delay, first_failure)) = budget.failed() else {
                                dispatch_health.budget_exhausted(JobSupervisorLoop::Dispatcher);
                                dispatch_stop.cancel();
                                dispatch_fanout_ready.store(false, Ordering::Release);
                                dispatch_fanout_dispatcher.stop_accepting();
                                return Err(error);
                            };
                            dispatch_health.restart_scheduled(
                                JobSupervisorLoop::Dispatcher,
                                classify_error(&error),
                                first_failure,
                            );
                            if !wait_backoff(&dispatch_stop, delay).await {
                                break;
                            }
                        }
                        Err(JobDispatcherPollError::Authority(error)) => {
                            dispatch_stop.cancel();
                            dispatch_fanout_ready.store(false, Ordering::Release);
                            dispatch_fanout_dispatcher.stop_accepting();
                            dispatch_health.authority_lost(JobSupervisorLoop::Dispatcher, &error);
                            return Err(error);
                        }
                    }
                }
                Ok(())
            }));
        }

        Ok(SourceRunningJobRuntime {
            stop,
            handles,
            registry,
            dispatcher,
            fanout_dispatcher,
            fanout_ready,
            heartbeat_lock,
            fanout_repository: FanoutRepository::new(self.client, self.keyspace, self.config),
            health,
            executor_id,
        })
    }
}

/// 运行中的运行时句柄；停机时先停各循环再注销执行器。
pub(crate) struct SourceRunningJobRuntime {
    stop: CancellationToken,
    handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    registry: Arc<ExecutorRegistry>,
    dispatcher: Arc<JobDispatcher>,
    fanout_dispatcher: Arc<FanoutDispatcher>,
    fanout_ready: Arc<AtomicBool>,
    heartbeat_lock: Arc<tokio::sync::Mutex<()>>,
    fanout_repository: FanoutRepository,
    health: Arc<SourceHealth>,
    executor_id: String,
}

impl SourceRunningJobRuntime {
    /// 业务作用：返回本进程执行器身份，供观测与外部关联。参数说明: 无。返回：执行器身份。
    pub(crate) fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// 业务作用：返回绑定当前 source 的 Fanout 查询与控制仓库副本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享底层连接、但不共享可变生命周期所有权的仓库。
    pub(crate) fn fanout_repository(&self) -> FanoutRepository {
        self.fanout_repository.clone()
    }

    /// 业务作用：返回当前 source generation 的共享健康状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：供 JobQuery 构造逐 source 快照的共享句柄。
    pub(crate) fn health(&self) -> Arc<SourceHealth> {
        self.health.clone()
    }

    /// 业务作用：返回当前 source generation 的执行器注册表，供受管控制面在定义删除后撤销本地能力。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享同一心跳 Worker 集合与执行器身份的注册表句柄。
    pub(crate) fn registry(&self) -> Arc<ExecutorRegistry> {
        self.registry.clone()
    }

    /// 业务作用：关闭本 source 的新工作准入并通知全部后台循环退出；等待与注销由后续停机阶段完成。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；调用后 scanner、dispatcher 与监视器不会开启下一轮领取。
    pub(crate) fn stop_accepting(&self) {
        self.health.stop_accepting();
        // 取消令牌是不可逆终态；先发布它，再压低 ready，任何并发恢复分支复验后都只能保持关闭。
        self.stop.cancel();
        self.fanout_ready.store(false, Ordering::Release);
        self.fanout_dispatcher.stop_accepting();
        // 关闭阻塞 lane，确保 XREADGROUP 不会把 drain 延迟到 BLOCK 时限或继续占有旧 transport。
        self.dispatcher.close_blocking_lanes();
    }

    /// 业务作用：在停止本地准入后把 executor 状态发布为 Draining，使新 Fanout 快照立即排除当前节点。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：主记录存在且 Draining 心跳成功时完成；记录已失效或 Redis 不可达时返回错误。
    pub(crate) async fn publish_draining(&self) -> Result<()> {
        self.fanout_ready.store(false, Ordering::Release);
        let ordinary = self.dispatcher.inflight_count().await;
        let fanout = self.fanout_dispatcher.inflight_count().await;
        let inflight = u32::try_from(ordinary.saturating_add(fanout)).unwrap_or(u32::MAX);
        let _heartbeat = self.heartbeat_lock.lock().await;
        match self
            .registry
            .heartbeat(JobExecutorState::Draining, inflight)
            .await?
        {
            HeartbeatOutcome::Ok { .. } => Ok(()),
            HeartbeatOutcome::NotFound => Err(crate::job::JobError::ExecutionStopped(
                "RedisJob drain 前 executor 主记录已失效".to_owned(),
            )
            .into()),
        }
    }

    /// 业务作用：等待已停止准入的 source 循环退出并注销执行器，超过统一截止时终止本地任务以停止续期。
    ///
    /// 参数说明：
    /// - `deadline`: 全量停机共享的绝对截止时刻。
    ///
    /// 返回：循环与注销在截止前完成时成功；超时或注销失败返回错误。
    pub(crate) async fn shutdown(mut self, deadline: tokio::time::Instant) -> Result<()> {
        // 先置停止标志并等待各循环退出，确保不再有循环在注销后继续消费或提升。
        self.stop.cancel();
        self.fanout_ready.store(false, Ordering::Release);
        self.health.stop_accepting();
        // shutdown 也独立执行关闭，覆盖调用方未先经过 stop_accepting 的底层运行时用法。
        self.dispatcher.close_blocking_lanes();
        let mut first_error = None;
        match tokio::time::timeout_at(deadline, self.publish_draining()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => first_error = Some(error),
            Err(_) => {
                first_error = Some(
                    crate::job::JobError::ShutdownDeadline(
                        "RedisJob executor 未在停机截止前发布 Draining".to_owned(),
                    )
                    .into(),
                );
            }
        }
        for mut handle in self.handles.drain(..) {
            match tokio::time::timeout_at(deadline, &mut handle).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) if first_error.is_none() => first_error = Some(error),
                Ok(Err(join_error)) if first_error.is_none() => {
                    first_error = Some(
                        crate::job::JobError::ExecutionUnknown(format!(
                            "RedisJob source 后台任务异常退出: {join_error}"
                        ))
                        .into(),
                    );
                }
                Err(_) => {
                    // 截止后终止本地循环并停止续期；其余 handle 仍逐一处理，不能让一个超时遮蔽其它 source 的收口。
                    handle.abort();
                    // abort 只提交取消；必须等待 JoinHandle 封闭，才能证明 future 内的权威守卫和连接句柄已析构。
                    let _ = handle.await;
                    if first_error.is_none() {
                        first_error = Some(
                            crate::job::JobError::ShutdownDeadline(
                                "RedisJob source 未在停机截止前收口，已停止本地续期".to_owned(),
                            )
                            .into(),
                        );
                    }
                }
                _ => {}
            }
        }
        // Pub/Sub 循环退出后再等待已接纳 Fanout Handler，避免 executor 注销后仍续租或提交旧 assignment。
        if let Err(error) = self.fanout_dispatcher.shutdown(deadline).await {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
        match tokio::time::timeout_at(deadline, self.registry.unregister()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
            Err(_) if first_error.is_none() => {
                first_error = Some(
                    crate::job::JobError::ShutdownDeadline(
                        "RedisJob executor 注销超过停机截止".to_owned(),
                    )
                    .into(),
                );
            }
            _ => {}
        }
        first_error.map_or(Ok(()), Err)
    }
}
