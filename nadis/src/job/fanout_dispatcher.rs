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
    client: Arc<RedisClient>,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
    repository: FanoutRepository,
    executor_id: String,
    node_identity: String,
    definitions: HashMap<String, JobDefinition>,
    handlers: HashMap<String, Arc<dyn JobHandler>>,
    pubsub: Mutex<JobPubSubLane>,
    notification_capacity: Arc<Semaphore>,
    handler_capacity: Arc<Semaphore>,
    in_flight: Mutex<HashSet<String>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    error_sender: tokio::sync::mpsc::UnboundedSender<NasaRedisError>,
    error_receiver: Mutex<tokio::sync::mpsc::UnboundedReceiver<NasaRedisError>>,
    subscription_generation: AtomicU64,
    metrics: Arc<JobMetrics>,
}

impl FanoutDispatcher {
    /// 业务作用：先建立全部固定桶 RESP3 订阅，再返回可登记 Worker 的 Fanout 接收器。
    ///
    /// 参数说明：连接、键模型、配置与执行器双层身份共同限定唯一 source subscription generation。
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
    /// 参数说明：`definition` 与 `handler` 来自冻结 plan。
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
    /// 返回：成功接纳或安全丢弃一条非权威信号时成功；订阅断线、source 不匹配或执行控制错误向上返回。
    pub(crate) async fn poll(self: &Arc<Self>) -> std::result::Result<(), FanoutPollError> {
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
                        tasks.retain(|task| !task.is_finished());
                        tasks.push(task);
                        Ok(())
                    }
                }
            }
        }
    }

    /// 业务作用：丢弃失效 RESP3 lane，重放全部固定频道并等待 ACK 后建立下一订阅代次。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：所有频道在新连接上确认后成功；连接、ACL 或协议失败返回错误，旧 lane 保持不可用。
    pub(crate) async fn restart_subscription(&self) -> Result<u64> {
        let replacement = JobPubSubLane::start(
            self.client.clone(),
            &self.keyspace,
            self.config.pubsub_mode,
            &self.node_identity,
            &self.executor_id,
        )
        .await?;
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
        self.notification_capacity.close();
        self.handler_capacity.close();
    }

    /// 业务作用：读取当前 Fanout Handler 在途数量，供 executor 心跳发布容量观测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已取得 shard attempt 且尚未完成提交的本地执行数。
    pub(crate) async fn inflight_count(&self) -> usize {
        self.in_flight.lock().await.len()
    }

    /// 业务作用：用通知定位持久 shard，复验 assignment 与合同后确认接收，并尝试取得执行权。
    ///
    /// 参数说明：`envelope` 只允许携带 fanoutId、seq、assignment epoch 与 inbox message id。
    ///
    /// 返回：陈旧通知或本地容量不足时安全返回；source/协议错误或 Redis 失败向监督循环返回。
    async fn handle_notification(self: &Arc<Self>, generation: u64, envelope: &[u8]) -> Result<()> {
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
        if shard.target_node_identity != self.node_identity
            || shard.assignment_epoch != assignment_epoch
            || shard.inbox_message_id != message_id
        {
            return Ok(());
        }
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
            FanoutAcceptOutcome::Ok { .. } | FanoutAcceptOutcome::Adopted => {}
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
                FanoutReadyDeferOutcome::Deferred { .. } => {
                    self.metrics.add("redis_job_fanout_capacity_defer_total", 1);
                }
                FanoutReadyDeferOutcome::CapacityExhausted { .. } => {
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
                execution_id.clone(),
                permit,
            )
            .await;
        dispatcher.in_flight.lock().await.remove(&execution_id);
        result
    }

    /// 业务作用：关闭本 source 的 Fanout 通知与 Handler 新准入，并在统一截止前等待全部已接纳任务退出。
    ///
    /// 参数说明：`deadline` 为 source 停机共享的绝对截止时刻。
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
                    // 取消请求本身不代表 future 已退出；等待 JoinHandle 后，in-flight guard 与容量许可才确定释放。
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
    /// 参数说明：定义、Handler、持久 shard、attempt 权威响应和容量许可共同覆盖整个业务 future。
    ///
    /// 返回：完成提交成功时返回；续期失权、状态提交未知或持久合同错误时返回错误。
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
            FinishOutcome::Ok { next_state, .. } => {
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
    /// 参数说明：`next_state` 来自 `finish_shard` 的封闭返回。
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
    /// 参数说明：`fanout_id` 定位与当前节点身份组合的 inbox Stream。
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
struct FanoutAuthorityGuard(Arc<JobExecutionAuthority>);

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
/// 参数说明：`envelope` 必须是 `fanoutId|seq|assignmentEpoch|messageId` UTF-8 字节。
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
