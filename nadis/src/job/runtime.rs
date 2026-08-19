//! 运行时编排：把执行器注册、心跳、调度扫描、Dispatch 消费与可见性提升组合成受监督的后台任务集合。
//!
//! 启动时登记本节点全部 Worker 能力；每个活跃调度分片一个循环，依次扫描触发、消费执行与提升可见成员；
//! 心跳循环单独续期存活。停机先停止接受新工作、等待各循环退出，再注销本执行器，避免半注销状态进入 Fanout 快照。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::client::RedisClient;
use crate::error::Result;
use crate::job::api::{JobSourceHealthReason, SourceHealth};
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
fn recoverable_reason(error: &crate::error::NasaRedisError) -> JobSourceHealthReason {
    match error {
        crate::error::NasaRedisError::Redis(_)
        | crate::error::NasaRedisError::ConnectProbe { .. } => {
            JobSourceHealthReason::RedisTransport
        }
        crate::error::NasaRedisError::JobProtocol(_) => JobSourceHealthReason::ProtocolContract,
        crate::error::NasaRedisError::ExecutionUnknown(_) => {
            JobSourceHealthReason::ExecutionUnknown
        }
        crate::error::NasaRedisError::Config(_) => JobSourceHealthReason::RuntimeConfiguration,
        crate::error::NasaRedisError::Job(error) => match error {
            crate::job::JobError::ExecutionUnknown(_) => JobSourceHealthReason::ExecutionUnknown,
            crate::job::JobError::ExecutionStopped(_)
            | crate::job::JobError::StaleOwner(_)
            | crate::job::JobError::StaleAssignment(_)
            | crate::job::JobError::FencingRegression(_) => {
                JobSourceHealthReason::ExecutionAuthority
            }
            crate::job::JobError::Protocol(_)
            | crate::job::JobError::SourceMismatch(_)
            | crate::job::JobError::ContractMismatch(_)
            | crate::job::JobError::InvalidPayload(_) => JobSourceHealthReason::ProtocolContract,
            _ => JobSourceHealthReason::RuntimeConfiguration,
        },
        _ => JobSourceHealthReason::CriticalRuntime,
    }
}

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
        for (definition, _) in &self.registrations {
            match registry
                .register(definition, JobExecutorState::Active)
                .await?
            {
                RegisterCapabilityOutcome::Ok { .. } => {}
                RegisterCapabilityOutcome::WorkerKeyConflict => {
                    let _ = registry.unregister().await;
                    return Err(crate::job::JobError::Protocol(format!(
                        "RedisJob Worker key 与既有能力冲突: {}",
                        definition.worker_name()
                    ))
                    .into());
                }
                RegisterCapabilityOutcome::ContractMismatch => {
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
        let mut handles = Vec::new();

        // 定向 Push 只负责低延迟定位；持久 receipt/ready 索引会补偿丢失通知，订阅代次断开则关闭 source 准入。
        {
            let fanout_dispatcher = fanout_dispatcher.clone();
            let registry = registry.clone();
            let stop = stop.clone();
            let health = health.clone();
            let fanout_ready = fanout_ready.clone();
            let heartbeat_lock = heartbeat_lock.clone();
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
                            fanout_ready.store(false, Ordering::Release);
                            fanout_dispatcher.stop_accepting();
                            health.authority_lost(JobSupervisorLoop::Dispatcher, &error);
                            stop.cancel();
                            return Err(error);
                        }
                        Err(FanoutPollError::Subscription(mut error)) => {
                            let recovery_started = std::time::Instant::now();
                            // 断线代次必须先退出快照，心跳循环通过同一 gate 不会把 ready 提前改回 true。
                            fanout_ready.store(false, Ordering::Release);
                            {
                                let _heartbeat = heartbeat_lock.lock().await;
                                match registry.heartbeat(JobExecutorState::Draining, 0).await {
                                    Ok(HeartbeatOutcome::Ok { .. }) => {}
                                    Ok(HeartbeatOutcome::NotFound) => {
                                        let authority = crate::error::NasaRedisError::from(
                                            crate::job::JobError::ExecutionStopped(
                                                "RESP3 断线时 executor 主记录已失效".to_owned(),
                                            ),
                                        );
                                        health.authority_lost(
                                            JobSupervisorLoop::Heartbeat,
                                            &authority,
                                        );
                                        stop.cancel();
                                        return Err(authority);
                                    }
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
                                        let _heartbeat = heartbeat_lock.lock().await;
                                        match registry.heartbeat(JobExecutorState::Active, 0).await
                                        {
                                            Ok(HeartbeatOutcome::Ok { .. }) => {
                                                // 只有新订阅 ACK 和 Active 心跳都成功后才重新允许周期心跳发布 ready。
                                                fanout_ready.store(true, Ordering::Release);
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
                                            Ok(HeartbeatOutcome::NotFound) => {
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
                        let _heartbeat = heartbeat_lock.lock().await;
                        let state = if fanout_ready.load(Ordering::Acquire) {
                            JobExecutorState::Active
                        } else {
                            JobExecutorState::Draining
                        };
                        match registry.heartbeat(state, inflight).await? {
                            HeartbeatOutcome::Ok { .. } => {}
                            HeartbeatOutcome::NotFound => {
                                return Err(crate::job::JobError::ExecutionStopped(
                                    "RedisJob executor 主记录已失效，当前 source 不再具备续期权威"
                                        .to_owned(),
                                )
                                .into());
                            }
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
                    fanout_ready.store(false, Ordering::Release);
                    fanout_dispatcher.stop_accepting();
                    health.authority_lost(JobSupervisorLoop::Heartbeat, error);
                    stop.cancel();
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
                                fanout_ready.store(false, Ordering::Release);
                                fanout_dispatcher.stop_accepting();
                                stop.cancel();
                                return Err(error);
                            };
                            health.restart_scheduled(
                                JobSupervisorLoop::FanoutMonitor,
                                recoverable_reason(&error),
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
                                completion_fanout_ready.store(false, Ordering::Release);
                                completion_fanout_dispatcher.stop_accepting();
                                completion_stop.cancel();
                                return Err(error);
                            };
                            completion_health.restart_scheduled(
                                JobSupervisorLoop::Completion,
                                recoverable_reason(&error),
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
                                scan_fanout_ready.store(false, Ordering::Release);
                                scan_fanout_dispatcher.stop_accepting();
                                scan_stop.cancel();
                                return Err(error);
                            };
                            scan_health.restart_scheduled(
                                JobSupervisorLoop::Scanner,
                                recoverable_reason(&error),
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
                                dispatch_fanout_ready.store(false, Ordering::Release);
                                dispatch_fanout_dispatcher.stop_accepting();
                                dispatch_stop.cancel();
                                return Err(error);
                            };
                            dispatch_health.restart_scheduled(
                                JobSupervisorLoop::Dispatcher,
                                recoverable_reason(&error),
                                first_failure,
                            );
                            if !wait_backoff(&dispatch_stop, delay).await {
                                break;
                            }
                        }
                        Err(JobDispatcherPollError::Authority(error)) => {
                            dispatch_fanout_ready.store(false, Ordering::Release);
                            dispatch_fanout_dispatcher.stop_accepting();
                            dispatch_health.authority_lost(JobSupervisorLoop::Dispatcher, &error);
                            dispatch_stop.cancel();
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
        self.fanout_ready.store(false, Ordering::Release);
        self.fanout_dispatcher.stop_accepting();
        // 先关闭阻塞 lane 再广播停止，确保 XREADGROUP 不会把 drain 延迟到 BLOCK 时限或继续占有旧 transport。
        self.dispatcher.close_blocking_lanes();
        self.stop.cancel();
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
