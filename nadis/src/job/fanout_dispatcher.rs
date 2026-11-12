//! Fanout 定向执行：Push 只提供定位，接收、领取、续期与完成全部复验持久 shard 状态。

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use base64::Engine;
use futures::FutureExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::fanout::{
    FanoutAcceptOutcome, FanoutReadyDeferOutcome, FanoutRepository, FanoutShard,
};
use crate::job::handler::{
    FanoutContext, JobExecution, JobExecutionAuthority, JobHandler, JobOutcome,
};
use crate::job::keyspace::JobKeyspace;
use crate::job::metrics::JobMetrics;
use crate::job::model::JobResultCode;
use crate::job::pubsub::{FanoutSignal, JobPubSubLane};
use crate::job::repository::{FinishOutcome, RenewOutcome, StartOutcome};

/// 稳定节点定向 inbox 的消费组名，与持久 start 和清理脚本使用同一合同。
const INBOX_GROUP: &str = "redis-job-fanout";

/// Fanout 接收循环的失败边界；订阅代次可重建，已接纳执行的权威失败不得原地重启。
pub(crate) enum FanoutPollError {
    /// RESP3 连接或 Push 协议失效，持久索引允许建立新订阅代次。
    Subscription(NasaRedisError),
    /// 通知已进入 accept/start/renew/finish 路径，结局不确定时必须关闭 source 准入。
    Authority(NasaRedisError),
}

/// 每个 source 独占的 Fanout 接收器；通知队列和 Handler 容量分别有界。
pub(crate) struct FanoutDispatcher {
    /// 当前 source 连接，提供持久控制和通知订阅所需通道。
    client: Arc<RedisClient>,
    /// 冻结的来源、命名空间与桶布局。
    keyspace: JobKeyspace,
    /// 当前 source 的容量、租约和扫描期限配置。
    config: Arc<JobConfig>,
    /// 负责接收、领取、续期与完成脚本的持久仓库。
    repository: FanoutRepository,
    /// 当前进程执行器身份，作为 shard 的接收方和 owner。
    executor_id: String,
    /// 跨进程重启保持稳定的节点身份，用于定向 inbox 和频道。
    node_identity: String,
    /// 按 Worker 能力名冻结的本地合同目录。
    definitions: HashMap<String, JobDefinition>,
    /// 与定义对应的业务处理器，不在运行中替换。
    handlers: HashMap<String, Arc<dyn JobHandler>>,
    /// 当前已确认频道订阅的 RESP3 通道代次。
    pubsub: Mutex<JobPubSubLane>,
    /// 限制已接纳的通知处理责任，饱和时由持久索引再次唤醒。
    notification_capacity: Arc<Semaphore>,
    /// 覆盖领取后业务执行与完成提交的并发许可。
    handler_capacity: Arc<Semaphore>,
    /// 已领取的批次、序号与 token 身份，阻止同一尝试并发进入 Handler。
    in_flight: Mutex<HashSet<String>>,
    /// 已派生通知任务的句柄，供停机等待和取消。
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// 把后台执行的权威错误送回 source 监督循环。
    error_sender: tokio::sync::mpsc::UnboundedSender<NasaRedisError>,
    /// 监督循环独占消费的后台错误入口。
    error_receiver: Mutex<tokio::sync::mpsc::UnboundedReceiver<NasaRedisError>>,
    /// 本地订阅代次，拒绝已被替换通道派生的旧通知。
    subscription_generation: AtomicU64,
    /// 当前 source 共享的固定维度运行指标。
    metrics: Arc<JobMetrics>,
}

impl FanoutDispatcher {
    /// 业务作用：先建立全部固定桶 RESP3 订阅，再返回可登记 Worker 的 Fanout 接收器。
    ///
    /// 参数说明：
    /// - `client`：当前 source 连接，用于持久控制和订阅。
    /// - `keyspace`：冻结的来源、命名空间及桶布局。
    /// - `config`：已经校验的订阅模式、通知容量与执行配置。
    /// - `executor_id`：当前进程执行器身份，承担 shard owner 责任。
    /// - `node_identity`：稳定节点身份，定位本节点定向频道和 inbox。
    /// - `metrics`：当前 source 唯一共享的指标容器。
    ///
    /// 返回：全部 SUBSCRIBE/SSUBSCRIBE 确认后返回接收器；任一能力或 ACL 不成立时拒绝启动。
    pub(crate) async fn start(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        executor_id: impl Into<String>,
        node_identity: impl Into<String>,
        metrics: Arc<JobMetrics>,
    ) -> Result<Self> {
        let executor_id = executor_id.into();
        let node_identity = node_identity.into();
        // 全部频道确认前不交付接收器，避免业务能力已开放却没有可用通知通道。
        let pubsub = JobPubSubLane::start(
            client.clone(),
            &keyspace,
            config.pubsub_mode,
            &node_identity,
            &executor_id,
        )
        .await?;
        let repository = FanoutRepository::new(client.clone(), keyspace.clone(), config.clone());
        let (error_sender, error_receiver) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            client,
            keyspace,
            config: config.clone(),
            repository,
            executor_id,
            node_identity,
            definitions: HashMap::new(),
            handlers: HashMap::new(),
            pubsub: Mutex::new(pubsub),
            notification_capacity: Arc::new(Semaphore::new(
                (config.handler_capacity as usize).saturating_mul(4).max(16),
            )),
            handler_capacity: Arc::new(Semaphore::new(config.handler_capacity as usize)),
            in_flight: Mutex::new(HashSet::new()),
            tasks: Mutex::new(Vec::new()),
            error_sender,
            error_receiver: Mutex::new(error_receiver),
            subscription_generation: AtomicU64::new(1),
            metrics,
        })
    }

    /// 业务作用：登记本节点可执行的 Worker 合同，通知确认前必须精确匹配该定义。
    ///
    /// 参数说明：
    /// - `definition`：冻结计划中的 Worker 定义，用于持久合同复验。
    /// - `handler`：与该能力绑定的业务处理器。
    ///
    /// 返回：无返回值；同 Worker 名只保留计划冻结后的唯一项。
    pub(crate) fn register_handler(
        &mut self,
        definition: JobDefinition,
        handler: Arc<dyn JobHandler>,
    ) {
        self.handlers
            .insert(definition.worker_name().to_owned(), handler);
        self.definitions
            .insert(definition.worker_name().to_owned(), definition);
    }

    /// 业务作用：等待下一条 Push 或后台执行错误；通知处理被有界转交，receipt 仅作低延迟唤醒。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功接纳或安全丢弃一条非权威信号时成功；订阅断线返回订阅错误，后台任务异常或执行控制失败返回权威错误。
    pub(crate) async fn poll(self: &Arc<Self>) -> std::result::Result<(), FanoutPollError> {
        let completed = {
            let mut tasks = self.tasks.lock().await;
            let mut completed = Vec::new();
            let mut index = 0;
            while index < tasks.len() {
                if tasks[index].is_finished() {
                    completed.push(tasks.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            completed
        };
        // 创建业务 Future 时也可能异常退出，不能仅依赖返回值错误通道；取得退出证据后才释放句柄。
        // 任一异常归入权威失败，由监督器关闭 source，禁止原地重新进入已领取的 attempt。
        for task in completed {
            task.await.map_err(|_| {
                FanoutPollError::Authority(
                    crate::job::JobError::ExecutionUnknown(
                        "Fanout 后台任务异常退出，执行结局不可确认".to_owned(),
                    )
                    .into(),
                )
            })?;
        }
        let signal = async { self.pubsub.lock().await.next_signal().await };
        let background_error = async {
            self.error_receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| {
                    NasaRedisError::from(crate::job::JobError::ExecutionUnknown(
                        "Fanout 后台错误通道意外关闭".to_owned(),
                    ))
                })
        };
        tokio::select! {
            result = background_error => Err(FanoutPollError::Authority(
                result.map_err(FanoutPollError::Authority)?,
            )),
            _ = tokio::time::sleep(std::time::Duration::from_millis(
                self.config.min_scan_interval_ms.max(1),
            )) => Ok(()),
            result = signal => {
                match result.map_err(FanoutPollError::Subscription)? {
                    FanoutSignal::Receipt(_) => Ok(()),
                    FanoutSignal::Notification(envelope) => {
                        let Ok(permit) = self.notification_capacity.clone().try_acquire_owned() else {
                            // 通知队列饱和时不确认 shard；receipt/ready 持久索引会重发定位信号。
                            return Ok(());
                        };
                        let dispatcher = self.clone();
                        let generation = self.subscription_generation.load(Ordering::Acquire);
                        let task = tokio::spawn(async move {
                            let result = dispatcher
                                .handle_notification(generation, &envelope)
                                .await;
                            drop(permit);
                            if let Err(error) = result {
                                let _ = dispatcher.error_sender.send(error);
                            }
                        });
                        let mut tasks = self.tasks.lock().await;
                        tasks.push(task);
                        Ok(())
                    }
                }
            }
        }
    }

    /// 业务作用：为失效 RESP3 通道建立替代订阅，全部固定频道确认后替换本地代次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功返回新本地代次；连接、ACL 或协议失败时不替换原引用，监督器继续按失效状态处理。
    pub(crate) async fn restart_subscription(&self) -> Result<u64> {
        let replacement = JobPubSubLane::start(
            self.client.clone(),
            &self.keyspace,
            self.config.pubsub_mode,
            &self.node_identity,
            &self.executor_id,
        )
        .await?;
        // 新连接全部 ACK 后才发布引用并提升代次，避免半订阅状态被轮询使用。
        *self.pubsub.lock().await = replacement;
        Ok(self
            .subscription_generation
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1))
    }

    /// 业务作用：同步关闭 Fanout 通知与 Handler 新准入，使停机或权威失效后不再接纳新 shard。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；已取得许可的任务仍由 shutdown 截止负责收口。
    pub(crate) fn stop_accepting(&self) {
        // 先封闭两个入口，已取得许可的责任由统一停机流程等待，不允许停机途中补入新执行。
        self.notification_capacity.close();
        self.handler_capacity.close();
    }

    /// 业务作用：读取当前 Fanout Handler 在途数量，供 executor 心跳发布容量观测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地集合仍保留的 shard/token 身份数；取消任务可能保留身份，不能据此推断活跃 Handler 数。
    pub(crate) async fn inflight_count(&self) -> usize {
        self.in_flight.lock().await.len()
    }

    /// 业务作用：用通知定位持久 shard，复验 assignment 与合同后确认接收，并尝试取得执行权。
    ///
    /// 参数说明：
    /// - `generation`：产生此通知的本地订阅代次，开始处理时须仍为当前代次。
    /// - `envelope`：只携带 fanoutId、seq、assignment epoch 和 inbox message id 的定位字节。
    ///
    /// 返回：陈旧通知或本地容量不足时安全返回；source/协议错误或 Redis 失败向监督循环返回。
    async fn handle_notification(self: &Arc<Self>, generation: u64, envelope: &[u8]) -> Result<()> {
        // 已替换通道的排队通知不能作为当前接纳依据；后续领取仍由持久 assignment 复验。
        if generation != self.subscription_generation.load(Ordering::Acquire) {
            return Ok(());
        }
        let (fanout_id, seq, assignment_epoch, message_id) =
            parse_envelope(envelope).inspect_err(|_| {
                self.metrics.add("redis_job_invalid_payload_total", 1);
            })?;
        let Some(shard) = self.repository.read_shard(&fanout_id, seq).await? else {
            return Ok(());
        };
        // Push 只负责定位，目标、epoch 和消息必须与持久记录一致，旧通知不得领取新分配。
        if shard.target_node_identity != self.node_identity
            || shard.assignment_epoch != assignment_epoch
            || shard.inbox_message_id != message_id
        {
            return Ok(());
        }
        // 本地能力已撤销时不确认接收，保留持久恢复责任供监视器收敛。
        let Some(definition) = self.definitions.get(&shard.worker_name) else {
            return Ok(());
        };
        let Some(handler) = self.handlers.get(&shard.worker_name) else {
            return Ok(());
        };
        self.ensure_inbox_group(&fanout_id).await?;
        match self
            .repository
            .accept_shard(
                definition,
                &fanout_id,
                seq,
                &self.node_identity,
                &self.executor_id,
                assignment_epoch,
                &shard.origin_executor_id,
            )
            .await?
        {
            FanoutAcceptOutcome::Ok {
                ..
            }
            | FanoutAcceptOutcome::Adopted => {}
            FanoutAcceptOutcome::SourceMismatch => {
                return Err(crate::job::JobError::SourceMismatch(
                    "Fanout shard 来源与当前 source 不一致".to_owned(),
                )
                .into());
            }
            _ => return Ok(()),
        }
        let Ok(permit) = self.handler_capacity.clone().try_acquire_owned() else {
            // 已确认接收足以证明目标在线；显式延后容量背压，避免普通唤醒次数耗尽后把健康节点降为失联。
            match self.repository.defer_ready_for_capacity(&shard).await? {
                FanoutReadyDeferOutcome::Deferred {
                    ..
                } => {
                    self.metrics.add("redis_job_fanout_capacity_defer_total", 1);
                }
                FanoutReadyDeferOutcome::CapacityExhausted {
                    ..
                } => {
                    self.metrics
                        .add("redis_job_fanout_capacity_exhausted_total", 1);
                }
                FanoutReadyDeferOutcome::StaleAssignment => {
                    self.metrics
                        .add("redis_job_fanout_stale_assignment_total", 1);
                }
                FanoutReadyDeferOutcome::Stale => {}
            }
            return Ok(());
        };
        // 在领取前冻结单调起点，后续网络和本地排队不能增加已获得租约的有效跨度。
        let request_started = tokio::time::Instant::now();
        let (attempt, attempt_token, redis_now, lease_until) = match self
            .repository
            .start_shard(
                definition,
                &fanout_id,
                seq,
                &self.node_identity,
                &self.executor_id,
                assignment_epoch,
                &message_id,
            )
            .await?
        {
            StartOutcome::Started {
                attempt,
                attempt_token,
                redis_now,
                lease_until,
            }
            | StartOutcome::Adopted {
                attempt,
                attempt_token,
                redis_now,
                lease_until,
            } => (attempt, attempt_token, redis_now, lease_until),
            StartOutcome::SourceMismatch => {
                return Err(crate::job::JobError::SourceMismatch(
                    "Fanout start 来源与当前 source 不一致".to_owned(),
                )
                .into());
            }
            _ => return Ok(()),
        };
        let execution_id = format!("{fanout_id}:{seq}:{attempt_token}");
        // 脚本可能幂等采用当前 attempt；本地仍须禁止同一 token 并发进入业务处理器。
        if !self.in_flight.lock().await.insert(execution_id.clone()) {
            return Ok(());
        }
        let dispatcher = self.clone();
        let definition = definition.clone();
        let handler = handler.clone();
        let result = dispatcher
            .execute(
                definition,
                handler,
                shard,
                attempt,
                attempt_token,
                redis_now,
                lease_until,
                request_started,
                execution_id.clone(),
                permit,
            )
            .await;
        dispatcher.in_flight.lock().await.remove(&execution_id);
        result
    }

    /// 业务作用：关闭本 source 的 Fanout 通知与 Handler 新准入，并在统一截止前等待全部已接纳任务退出。
    ///
    /// 参数说明：
    /// - `deadline`：source 停机各责任共享的绝对截止时刻，不为单个任务延长。
    ///
    /// 返回：全部通知处理与 Handler 收口时成功；task 异常或超过截止时返回第一个错误并终止剩余本地 task。
    pub(crate) async fn shutdown(&self, deadline: tokio::time::Instant) -> Result<()> {
        self.stop_accepting();
        let mut tasks = {
            let mut tracked = self.tasks.lock().await;
            std::mem::take(&mut *tracked)
        };
        let mut first_error = None;
        for mut task in tasks.drain(..) {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if first_error.is_none() => {
                    first_error = Some(
                        crate::job::JobError::ExecutionUnknown(format!(
                            "Fanout 本地 task 异常退出: {error}"
                        ))
                        .into(),
                    );
                }
                Err(_) => {
                    // 截止到达后必须撤销本地 future；AuthorityGuard 会同步关闭 attempt 门禁，后续完成提交被拒绝。
                    task.abort();
                    // 取消请求不代表 future 已退出；等待句柄后才确认本地权威已撤销、许可已释放，在途身份不保证清除。
                    let _ = task.await;
                    if first_error.is_none() {
                        first_error = Some(
                            crate::job::JobError::ShutdownDeadline(
                                "Fanout Handler 未在停机截止前收口".to_owned(),
                            )
                            .into(),
                        );
                    }
                }
                _ => {}
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// 业务作用：运行一个已取得执行权的 Fanout shard，在本地保守租约内续期并提交唯一终态出口。
    ///
    /// 参数说明：
    /// - `definition`：冻结的 Worker 定义，提供执行期限和重试规则。
    /// - `handler`：对应能力的本地业务处理器。
    /// - `shard`：领取前读回的持久分片，提供载荷、合同和 assignment 身份。
    /// - `attempt`：领取响应确认的当前尝试次数。
    /// - `attempt_token`：当前尝试的 fencing token，用于续期和完成 CAS。
    /// - `redis_now`：领取脚本返回的服务端毫秒时间。
    /// - `lease_until`：同一响应的租约截止，与服务端时间共同形成保守本地期限。
    /// - `request_started`：领取请求发出前的本地单调起点，截止折算包含响应等待与本地排队。
    /// - `_execution_id`：调用方登记的本地执行身份；当前函数保留该参数，不操作在途集合。
    /// - `_permit`：覆盖业务 Future 和完成提交的本地容量许可，退出时释放。
    ///
    /// 返回：业务结局确定后先关闭本地权威并释放业务 Future，再提交持久结果；提交成功时返回。
    /// 续期失权、状态提交未知或持久合同错误时返回错误，本地权威不会随迟到响应重新开放。
    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        definition: JobDefinition,
        handler: Arc<dyn JobHandler>,
        shard: FanoutShard,
        attempt: i64,
        attempt_token: i64,
        redis_now: i64,
        lease_until: i64,
        request_started: tokio::time::Instant,
        _execution_id: String,
        _permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        let payload = base64::engine::general_purpose::STANDARD
            .decode(shard.parameter_payload.as_bytes())
            .map_err(|_| {
                self.metrics.add("redis_job_invalid_payload_total", 1);
                NasaRedisError::from(crate::job::JobError::InvalidPayload(
                    "Fanout payload Base64 非法".to_owned(),
                ))
            })?;
        let authority = JobExecutionAuthority::new(
            redis_now,
            lease_until,
            self.config
                .renew_rtt_allowance_ms
                .saturating_add(self.config.clock_drift_allowance_ms),
            request_started,
        );
        let execution = JobExecution {
            qualifier: self.keyspace.qualifier().to_owned(),
            namespace: self.keyspace.namespace().to_owned(),
            run_id: crate::job::identifiers::shard_run_id(&shard.fanout_id, shard.seq),
            job_name: shard.worker_name.clone(),
            worker_name: shard.worker_name.clone(),
            logical_fire_at: 0,
            triggered_at: 0,
            attempt,
            attempt_token,
            schema_id: shard.schema_id.clone(),
            wire_codec: shard.wire_codec.clone(),
            parameter_payload: payload,
            fanout: Some(FanoutContext {
                fanout_id: shard.fanout_id.clone(),
                root_run_id: shard.root_run_id.clone(),
                snapshot_id: shard.snapshot_id.clone(),
                shard_index: shard.shard_index,
                shard_total: shard.shard_total,
                seq: shard.seq,
                execution_key: shard.execution_key.clone(),
                target_node_identity: shard.target_node_identity.clone(),
                assignment_epoch: shard.assignment_epoch,
            }),
            authority: authority.clone(),
            fanout_service: None,
        };
        let _guard = FanoutAuthorityGuard(authority.clone());
        // 持久领取响应可能已经迟到，创建业务 Future 前必须复验扣除实际等待后的本地窗口。
        execution.checkpoint()?;
        self.metrics.add("redis_job_started_total", 1);
        let mut handler_future =
            Box::pin(AssertUnwindSafe(handler.handle(&execution)).catch_unwind());
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(
            definition
                .timeout_ms()
                .min(self.config.max_run_duration_ms)
                .max(1),
        ));
        tokio::pin!(timeout);
        let mut renew = tokio::time::interval_at(
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(self.config.lease_renew_ms),
            std::time::Duration::from_millis(self.config.lease_renew_ms),
        );
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let outcome = loop {
            tokio::select! {
                result = &mut handler_future => {
                    break match result {
                        Ok(outcome) => outcome,
                        Err(_) => JobOutcome::retry("Fanout Handler panic，当前 attempt 按可重试失败收口"),
                    };
                }
                _ = &mut timeout => {
                    break JobOutcome {
                        code: JobResultCode::Timeout,
                        summary: "Fanout Handler 超过任务执行时限".to_owned(),
                    };
                }
                _ = renew.tick() => {
                    let request_started = tokio::time::Instant::now();
                    let renewed = match self.repository.renew_shard(
                        &shard.fanout_id,
                        shard.seq,
                        &self.executor_id,
                        attempt_token,
                    ).await {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            // 未取得续期证据时立即撤销本地 shard 执行，避免网络分区中的旧 owner 继续副作用。
                            self.metrics.add("redis_job_self_fence_total", 1);
                            return Err(error);
                        }
                    };
                    match renewed {
                        RenewOutcome::Ok { redis_now, deadline, cancel_requested: true } => {
                            let _ = (redis_now, deadline);
                            authority.revoke(true);
                            break JobOutcome {
                                code: JobResultCode::Cancelled,
                                summary: "Fanout shard 收到持久取消请求".to_owned(),
                            };
                        }
                        RenewOutcome::Ok { redis_now, deadline, cancel_requested: false } => {
                            if !authority.renew(
                                redis_now,
                                deadline,
                                self.config.renew_rtt_allowance_ms
                                    .saturating_add(self.config.clock_drift_allowance_ms),
                                request_started,
                            ) {
                                self.metrics.add("redis_job_self_fence_total", 1);
                                return Err(crate::job::JobError::ExecutionStopped(
                                    "Fanout shard 本地权威门禁已关闭".to_owned(),
                                )
                                .into());
                            }
                        }
                        RenewOutcome::StateMismatch | RenewOutcome::StaleOwner => {
                            self.metrics.add("redis_job_self_fence_total", 1);
                            return Err(crate::job::JobError::StaleOwner(
                                "Fanout Handler 已失去 attempt 权威，禁止完成提交".to_owned(),
                            )
                            .into());
                        }
                    }
                }
            }
        };
        // 业务已经结束或被超时停止，完成请求的网络等待不能延长其外部副作用权限。
        // 先关闭所有上下文副本的门禁，再释放 Future，使析构期间也不能继续持权写入。
        authority.revoke(false);
        drop(handler_future);
        let finish = self
            .repository
            .finish_shard(
                &shard.fanout_id,
                shard.seq,
                &shard.worker_name,
                &self.executor_id,
                attempt_token,
                shard.assignment_epoch,
                &definition,
                outcome.code,
                &outcome.summary,
            )
            .await?;
        match finish {
            FinishOutcome::Ok {
                next_state,
                ..
            } => {
                self.record_finished(next_state);
                Ok(())
            }
            FinishOutcome::StateMismatch | FinishOutcome::StaleOwner => {
                self.metrics.add("redis_job_stale_finish_total", 1);
                Err(crate::job::JobError::StaleOwner(
                    "Fanout shard 完成提交已失去当前状态或 owner 权威".to_owned(),
                )
                .into())
            }
            FinishOutcome::StaleAssignment => {
                self.metrics
                    .add("redis_job_fanout_stale_assignment_total", 1);
                Err(crate::job::JobError::StaleAssignment(
                    "Fanout shard 完成提交的 assignment epoch 已失效".to_owned(),
                )
                .into())
            }
            FinishOutcome::IdentityMismatch => {
                self.metrics.add("redis_job_fencing_regression_total", 1);
                Err(crate::job::JobError::FencingRegression(
                    "Fanout shard 完成提交的稳定执行身份不一致".to_owned(),
                )
                .into())
            }
        }
    }

    /// 业务作用：按 Lua 已提交的 Fanout shard 下一状态累计唯一终态或重试结局。
    ///
    /// 参数说明：
    /// - `next_state`：`finish_shard` 已提交的封闭状态，不以业务 Future 的本地结果替代。
    ///
    /// 返回：无；每次成功完成提交只更新一组封闭计数。
    fn record_finished(&self, next_state: crate::job::model::JobState) {
        use crate::job::model::JobState;
        match next_state {
            JobState::Succeeded => self.metrics.add("redis_job_success_total", 1),
            JobState::RetryWait => self.metrics.add("redis_job_retry_total", 1),
            JobState::Dead => {
                self.metrics.add("redis_job_failed_total", 1);
                self.metrics.add("redis_job_dead_total", 1);
            }
            JobState::Failed => self.metrics.add("redis_job_failed_total", 1),
            JobState::Skipped => self.metrics.add("redis_job_skipped_total", 1),
            JobState::Created
            | JobState::Queued
            | JobState::Blocked
            | JobState::Running
            | JobState::FanoutCreating
            | JobState::WaitingChildren
            | JobState::AwaitingCapability
            | JobState::AwaitingReceipt
            | JobState::Received
            | JobState::Cancelled => {}
        }
    }

    /// 业务作用：为稳定节点 inbox 建立共享消费组，使 start 脚本可原子确认并删除消息。
    ///
    /// 参数说明：
    /// - `fanout_id`：与本节点稳定身份共同定位定向 inbox 的批次标识。
    ///
    /// 返回：新建或已存在均成功；其它 Redis 错误向上返回。
    async fn ensure_inbox_group(&self, fanout_id: &str) -> Result<()> {
        let inbox = self.keyspace.fanout_inbox(fanout_id, &self.node_identity);
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("CREATE")
            .arg(inbox)
            .arg(INBOX_GROUP)
            .arg("0")
            .arg("MKSTREAM");
        let mut connection = self.client.conn();
        match cmd.query_async::<()>(&mut connection).await {
            Ok(()) => Ok(()),
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
            Err(error) => Err(NasaRedisError::Redis(error)),
        }
    }
}

/// Handler future 结束或被停止时永久关闭 Fanout attempt 的本地副作用门禁。
struct FanoutAuthorityGuard(
    /// 随业务执行作用域退出而撤销的共享本地权威。
    Arc<JobExecutionAuthority>,
);

impl Drop for FanoutAuthorityGuard {
    /// 业务作用：确保所有执行出口都撤销本地 attempt 权威。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；Drop 后 checkpoint 永久拒绝。
    fn drop(&mut self) {
        self.0.revoke(false);
    }
}

/// 业务作用：解析 Fanout 定位信封，不允许 Push 携带或覆盖持久业务字段。
///
/// 参数说明：
/// - `envelope`：按 `fanoutId|seq|assignmentEpoch|messageId` 编码的 UTF-8 定位字节。
///
/// 返回：四个定位字段；字段数、UTF-8 或数值非法时返回协议错误。
fn parse_envelope(envelope: &[u8]) -> Result<(String, i64, i64, String)> {
    let text = std::str::from_utf8(envelope)
        .map_err(|_| crate::job::JobError::Protocol("Fanout 通知信封不是 UTF-8".to_owned()))?;
    let fields = text.split('|').collect::<Vec<_>>();
    if fields.len() != 4 || fields[0].is_empty() || fields[3].is_empty() {
        return Err(
            crate::job::JobError::Protocol("Fanout 通知信封字段不符合定位合同".to_owned()).into(),
        );
    }
    let seq = fields[1]
        .parse::<i64>()
        .map_err(|_| crate::job::JobError::Protocol("Fanout 通知 seq 非法".to_owned()))?;
    let assignment_epoch = fields[2].parse::<i64>().map_err(|_| {
        crate::job::JobError::Protocol("Fanout 通知 assignment epoch 非法".to_owned())
    })?;
    Ok((
        fields[0].to_owned(),
        seq,
        assignment_epoch,
        fields[3].to_owned(),
    ))
}
