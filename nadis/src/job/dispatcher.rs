//! Dispatch 消费：从任务的 Dispatch Stream 拉取消息，取得 Redis 执行权后调用本地 Handler，再提交唯一状态出口。
//!
//! 消费者只做定位：真正的执行权、参数与合同以 `start_run` 与 Run 记录为准，Stream 消息丢失由可见性提升重建。
//! 同一 attempt 最多调用一次 Handler；无本地 Handler 或路由不兼容的消息延后放回可见索引，等待兼容节点。

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use base64::Engine;
use futures::{FutureExt, StreamExt};
use redis::streams::{StreamAutoClaimReply, StreamId, StreamReadReply};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::client::{Conn, RedisClient};
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::coordinator::FanoutService;
use crate::job::definition::JobDefinition;
use crate::job::handler::{JobExecution, JobExecutionAuthority, JobHandler, JobOutcome};
use crate::job::keyspace::JobKeyspace;
use crate::job::metrics::JobMetrics;
use crate::job::model::JobResultCode;
use crate::job::repository::{FinishOutcome, JobRepository, RenewOutcome, StartOutcome};

/// Dispatch 循环的失败边界；读取 lane 可重建，进入 attempt 后的结局不确定必须关闭 source。
pub(crate) enum JobDispatcherPollError {
    /// 尚未取得新 attempt 的 Stream/transport 失败，可在完整返回后启动下一循环代次。
    Recoverable(NasaRedisError),
    /// 已处理消息或执行 Handler 后失败，可能存在活动权威，不允许原地重启。
    Authority(NasaRedisError),
}

/// 单分片阻塞读取 lane；连接只由本对象持有，关闭令牌用于在停机时打断未返回的 BLOCK 请求。
struct BlockingLane {
    conn: Mutex<Conn>,
    close: CancellationToken,
}

/// 单节点 Dispatch 消费器；持有连接、键模型、配置、仓库、执行器身份与本地定义/Handler 注册表。
pub struct JobDispatcher {
    client: Arc<RedisClient>,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
    repository: JobRepository,
    executor_id: String,
    definitions: HashMap<String, JobDefinition>,
    handlers: HashMap<String, Arc<dyn JobHandler>>,
    capacity: Arc<Semaphore>,
    handler_capacities: HashMap<String, Arc<Semaphore>>,
    in_flight: Mutex<HashSet<String>>,
    blocking_lanes: std::sync::Mutex<HashMap<u32, Arc<BlockingLane>>>,
    fanout_service: Option<Arc<FanoutService>>,
    metrics: Arc<JobMetrics>,
}

impl JobDispatcher {
    /// 业务作用：绑定连接、键模型、配置与执行器身份，创建 Dispatch 消费器。
    ///
    /// 参数说明：
    /// - `client`: 承载消费连接的 RedisClient。
    /// - `keyspace`: 冻结的 Job 键模型。
    /// - `config`: 已校验的 Job 配置，提供消费组名与批量。
    /// - `executor_id`: 本进程执行器身份，作为消费者名与 Run owner。
    ///
    /// 返回：可注册定义/Handler 并轮询处理的消费器。
    pub fn new(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        executor_id: impl Into<String>,
    ) -> Self {
        let metrics = Arc::new(JobMetrics::new(keyspace.qualifier()));
        Self::new_with_metrics(client, keyspace, config, executor_id, metrics)
    }

    /// 业务作用：使用 source 运行时的唯一指标容器建立 Dispatch 消费器。
    ///
    /// 参数说明：连接、键模型、配置、执行器身份与 `metrics` 必须同属一个 source generation。
    ///
    /// 返回：可装配 Handler 并把执行结局写入逐 source 快照的消费器。
    pub(crate) fn new_with_metrics(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        executor_id: impl Into<String>,
        metrics: Arc<JobMetrics>,
    ) -> Self {
        let repository = JobRepository::new(client.clone(), keyspace.clone(), config.clone());
        let executor_capacity = config.executor_capacity as usize;
        Self {
            client,
            keyspace,
            config,
            repository,
            executor_id: executor_id.into(),
            definitions: HashMap::new(),
            handlers: HashMap::new(),
            capacity: Arc::new(Semaphore::new(executor_capacity)),
            handler_capacities: HashMap::new(),
            in_flight: Mutex::new(HashSet::new()),
            blocking_lanes: std::sync::Mutex::new(HashMap::new()),
            fanout_service: None,
            metrics,
        }
    }

    /// 业务作用：在运行时启动前注入同 source Fanout 服务，使普通 Handler 可显式创建批次。
    ///
    /// 参数说明：`service` 必须与当前 dispatcher 使用同一连接、keyspace、配置与执行器身份。
    ///
    /// 返回：无返回值；冻结后每个 JobContext 克隆共享服务。
    pub(crate) fn set_fanout_service(&mut self, service: Arc<FanoutService>) {
        self.fanout_service = Some(service);
    }

    /// 业务作用：读取当前普通 Handler 在途数量，供 executor 心跳发布容量观测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已取得本地 in-flight 门禁且尚未完成提交的 Run 数量。
    pub(crate) async fn inflight_count(&self) -> usize {
        self.in_flight.lock().await.len()
    }

    /// 业务作用：登记一个任务定义与其 Worker 能力的本地 Handler，供后续轮询处理该任务的 Dispatch 消息。
    ///
    /// 参数说明：
    /// - `definition`: 任务定义（提供任务名与 Worker 能力名）。
    /// - `handler`: 该 Worker 能力的本地 Handler。
    ///
    /// 返回：无返回值；同名任务重复登记覆盖既有项。
    pub fn register_handler(&mut self, definition: JobDefinition, handler: Arc<dyn JobHandler>) {
        self.handler_capacities
            .entry(definition.worker_name().to_owned())
            .or_insert_with(|| Arc::new(Semaphore::new(self.config.handler_capacity as usize)));
        self.handlers.insert(definition.name().to_owned(), handler);
        self.definitions
            .insert(definition.name().to_owned(), definition);
    }

    /// 业务作用：为一个调度分片内全部已登记 Worker 拉取并处理待办 Dispatch 消息，逐条取得执行权后调用 Handler 并提交结果。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片。
    ///
    /// 返回：本轮成功取得执行权并调用 Handler 的 Run 数；连接或脚本失败时向上返回错误。
    /// 每条消息在 `start_run` 内一次性 ACK/删除；没有本地容量时不拉取，PEL 消息先接管再与新消息共同受
    /// `handler_capacity` 限制。
    pub async fn poll_shard(&self, shard: u32) -> Result<usize> {
        self.poll_shard_supervised(shard)
            .await
            .map_err(|error| match error {
                JobDispatcherPollError::Recoverable(error)
                | JobDispatcherPollError::Authority(error) => error,
            })
    }

    /// 业务作用：按“领取前可恢复、领取后权威失败”分类轮询结局，供 source 监督器选择重建或停源。
    ///
    /// 参数说明：`shard` 为目标调度分片。
    ///
    /// 返回：成功时返回实际调用 Handler 的数量；读取失败为 `Recoverable`，消息处理失败为 `Authority`。
    pub(crate) async fn poll_shard_supervised(
        &self,
        shard: u32,
    ) -> std::result::Result<usize, JobDispatcherPollError> {
        let group = self.config.dispatch_group.clone();
        let mut streams: Vec<String> = self
            .definitions
            .values()
            .filter(|definition| self.keyspace.schedule_shard(definition.name()) == shard)
            .map(|definition| self.keyspace.dispatch(shard, definition.worker_name()))
            .collect();
        streams.sort();
        streams.dedup();
        for stream in &streams {
            self.ensure_group(stream, &group)
                .await
                .map_err(JobDispatcherPollError::Recoverable)?;
        }
        if streams.is_empty() || self.capacity.available_permits() == 0 {
            return Ok(0);
        }

        let mut entries: Vec<(String, StreamId)> = Vec::new();
        let mut remaining = self
            .capacity
            .available_permits()
            .min(self.config.scan_batch_size as usize);
        // PEL 接管优先于读取新消息，防止消费者退出后留下的消息被持续的新流量饿死。
        for stream in &streams {
            if remaining == 0 {
                break;
            }
            let claimed = self
                .claim_group(shard, stream, &group, remaining)
                .await
                .map_err(JobDispatcherPollError::Recoverable)?;
            remaining = remaining.saturating_sub(claimed.claimed.len());
            entries.extend(
                claimed
                    .claimed
                    .into_iter()
                    .map(|entry| (stream.clone(), entry)),
            );
        }
        if remaining > 0 {
            let reply = self
                .read_group(shard, &streams, &group, remaining)
                .await
                .map_err(JobDispatcherPollError::Recoverable)?;
            for key in reply.keys {
                entries.extend(key.ids.into_iter().map(|entry| (key.key.clone(), entry)));
            }
        }

        let mut seen = HashSet::new();
        let mut work = Vec::new();
        for (stream, entry) in entries {
            if !seen.insert((stream.clone(), entry.id.clone())) {
                continue;
            }
            let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
                break;
            };
            work.push((stream, entry, permit));
        }
        let results = futures::stream::iter(work)
            .map(|(stream, entry, permit)| async move {
                self.process_entry(&stream, entry, permit).await
            })
            .buffer_unordered(self.config.executor_capacity as usize)
            .collect::<Vec<_>>()
            .await;
        let mut processed = 0;
        let mut first_error = None;
        for result in results {
            match result {
                Ok(true) => processed += 1,
                Ok(false) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        match first_error {
            Some(error) => Err(JobDispatcherPollError::Authority(error)),
            None => Ok(processed),
        }
    }

    /// 业务作用：确保 Dispatch Stream 上存在共享消费组；组已存在时按幂等处理。
    ///
    /// 参数说明：
    /// - `stream`/`group`: 目标 Stream key 与消费组名。
    ///
    /// 返回：创建成功或组已存在返回成功；其它错误向上返回。
    async fn ensure_group(&self, stream: &str, group: &str) -> Result<()> {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("0")
            .arg("MKSTREAM");
        let mut conn = self.client.conn();
        match cmd.query_async::<()>(&mut conn).await {
            Ok(()) => Ok(()),
            // 组已存在是并发建组的正常结果，按幂等成功处理。
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
            Err(error) => Err(NasaRedisError::Redis(error)),
        }
    }

    /// 业务作用：从同一调度分片的多个 Worker Stream 一次阻塞拉取本消费者尚未处理的新消息。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片，用于绑定独立阻塞 lane。
    /// - `streams`/`group`: 同 slot 的目标 Stream 集合与消费组名。
    /// - `limit`: 本轮仍可接纳的消息数。
    ///
    /// 返回：本批新消息；无新消息时返回空回复。
    async fn read_group(
        &self,
        shard: u32,
        streams: &[String],
        group: &str,
        limit: usize,
    ) -> Result<StreamReadReply> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(group)
            .arg(&self.executor_id)
            .arg("COUNT")
            .arg(limit)
            .arg("BLOCK")
            .arg(self.config.min_scan_interval_ms.max(1))
            .arg("STREAMS")
            .arg(streams);
        for _ in streams {
            cmd.arg(">");
        }
        self.query_blocking(shard, &mut cmd).await
    }

    /// 业务作用：把闲置超过门限的 PEL 消息转移给当前执行器，使读取后退出的消费者不会永久占有消息。
    ///
    /// 参数说明：
    /// - `shard`/`stream`/`group`: 分片、实际 Stream 与共享消费组。
    /// - `limit`: 本轮最大接管数量。
    ///
    /// 返回：已接管消息与下一扫描游标；Redis 不支持或命令失败时拒绝继续消费。
    async fn claim_group(
        &self,
        shard: u32,
        stream: &str,
        group: &str,
        limit: usize,
    ) -> Result<StreamAutoClaimReply> {
        let mut cmd = redis::cmd("XAUTOCLAIM");
        cmd.arg(stream)
            .arg(group)
            .arg(&self.executor_id)
            .arg(self.config.xautoclaim_min_idle_ms)
            .arg("0-0")
            .arg("COUNT")
            .arg(limit);
        self.query_blocking(shard, &mut cmd).await
    }

    /// 业务作用：在分片独占连接上执行读取命令，并让停机或 lane 换代立即打断未返回的阻塞请求。
    ///
    /// 参数说明：
    /// - `shard`: 调度分片下标。
    /// - `cmd`: 只允许当前分片键的 Stream 读取命令。
    ///
    /// 返回：命令成功时返回类型化响应；关闭时返回停止错误，传输失败时淘汰本代连接后返回底层错误。
    async fn query_blocking<T: redis::FromRedisValue>(
        &self,
        shard: u32,
        cmd: &mut redis::Cmd,
    ) -> Result<T> {
        let lane = self.blocking_lane(shard).await?;
        let mut conn = tokio::select! {
            _ = lane.close.cancelled() => {
                return Err(crate::job::JobError::ExecutionStopped(
                    format!("Dispatch 分片 {shard} 的阻塞读取已关闭"),
                ).into());
            }
            conn = lane.conn.lock() => conn,
        };
        let result = tokio::select! {
            _ = lane.close.cancelled() => {
                return Err(crate::job::JobError::ExecutionStopped(
                    format!("Dispatch 分片 {shard} 的阻塞读取已关闭"),
                ).into());
            }
            result = cmd.query_async::<T>(&mut *conn) => result,
        };
        drop(conn);
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                // 读取失败后的 transport 可能仍携带未决响应，必须整代淘汰，下一轮才能建立干净的连接边界。
                self.retire_blocking_lane(shard, &lane);
                Err(NasaRedisError::Redis(error))
            }
        }
    }

    /// 业务作用：为每个活跃分片惰性建立单一所有者的阻塞连接，避免 BLOCK 请求与控制面或其它分片共享 transport。
    ///
    /// 参数说明：`shard` 为调度分片下标。
    ///
    /// 返回：该分片当前 generation 的共享 lane 外壳；连接本身不会被克隆。
    async fn blocking_lane(&self, shard: u32) -> Result<Arc<BlockingLane>> {
        if let Some(lane) = self
            .blocking_lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard)
            .cloned()
        {
            return Ok(lane);
        }
        let conn = self
            .client
            .dedicated_conn("Job Dispatch blocking lane")
            .await?;
        let candidate = Arc::new(BlockingLane {
            conn: Mutex::new(conn),
            close: CancellationToken::new(),
        });
        let mut lanes = self
            .blocking_lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(lanes.entry(shard).or_insert(candidate).clone())
    }

    /// 业务作用：只淘汰调用方实际使用的 lane generation，避免旧请求失败时误关并发建立的新连接。
    ///
    /// 参数说明：`shard` 为分片，`lane` 为待核对的 generation。
    ///
    /// 返回：无；仍是当前 generation 时取消并移除，否则保持新 generation 不变。
    fn retire_blocking_lane(&self, shard: u32, lane: &Arc<BlockingLane>) {
        let mut lanes = self
            .blocking_lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lanes
            .get(&shard)
            .is_some_and(|current| Arc::ptr_eq(current, lane))
        {
            lane.close.cancel();
            lanes.remove(&shard);
        }
    }

    /// 业务作用：关闭全部 Dispatch 阻塞 lane，使停机不必等待 Redis BLOCK 时限并立即释放连接所有权。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；后续若仍允许轮询，会按分片建立新的 generation。
    pub(crate) fn close_blocking_lanes(&self) {
        let mut lanes = self
            .blocking_lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for lane in lanes.values() {
            lane.close.cancel();
        }
        lanes.clear();
    }

    /// 业务作用：校验消息信封、保留本地容量许可并确保同一 Run 在本进程内只有一个 Handler future。
    ///
    /// 参数说明：
    /// - `stream`: 实际读取消息的 Dispatch Stream。
    /// - `entry`: Stream 消息及字段。
    /// - `permit`: 覆盖领取、Handler、续租和完成提交的容量许可。
    ///
    /// 返回：实际调用 Handler 返回 `true`；消息被延后或本地已有同 Run future 返回 `false`。
    async fn process_entry(
        &self,
        stream: &str,
        entry: StreamId,
        _permit: OwnedSemaphorePermit,
    ) -> Result<bool> {
        let job_name = field(&entry.map, "jobName");
        let run_id = field(&entry.map, "runId");
        if job_name.is_empty() || run_id.is_empty() {
            self.metrics.add("redis_job_invalid_payload_total", 1);
            return Err(crate::job::JobError::InvalidPayload(
                "Dispatch 消息缺少 jobName 或 runId".to_owned(),
            )
            .into());
        }
        let source = field(&entry.map, "schedulerQualifier");
        if source != self.keyspace.qualifier() {
            return Err(crate::job::JobError::SourceMismatch(format!(
                "expected={} observed={}",
                self.keyspace.qualifier(),
                source
            ))
            .into());
        }
        {
            let mut in_flight = self.in_flight.lock().await;
            if !in_flight.insert(run_id.clone()) {
                return Ok(false);
            }
        }
        let result = self.process(stream, &job_name, &run_id, &entry.id).await;
        let mut in_flight = self.in_flight.lock().await;
        in_flight.remove(&run_id);
        result
    }

    /// 业务作用：处理一条 Dispatch 消息：取得执行权后调用本地 Handler 并提交结果；无本地 Handler 或路由不兼容则延后。
    ///
    /// 参数说明：
    /// - `stream`: 实际读取消息的 Dispatch Stream。
    /// - `job_name`/`run_id`: 消息定位的任务名与 Run 标识。
    /// - `message_id`: Stream 消息 ID，随执行权提交一并 ACK/删除。
    ///
    /// 返回：成功取得执行权并调用 Handler 返回 `true`；延后或未取得执行权返回 `false`。
    async fn process(
        &self,
        stream: &str,
        job_name: &str,
        run_id: &str,
        message_id: &str,
    ) -> Result<bool> {
        let definition = self.definitions.get(job_name);
        let handler = self.handlers.get(job_name);
        let (definition, handler) = match (definition, handler) {
            (Some(def), Some(handler)) => (def, handler),
            // 无本地 Handler 时确认消息并放回可见索引，等待兼容节点，而非在本地丢弃。
            (Some(def), None) => {
                self.repository.defer_run(def, run_id, message_id).await?;
                return Ok(false);
            }
            _ => {
                self.repository
                    .defer_stream(job_name, run_id, message_id, stream)
                    .await?;
                return Ok(false);
            }
        };
        let Some(handler_capacity) = self.handler_capacities.get(definition.worker_name()) else {
            self.repository
                .defer_run(definition, run_id, message_id)
                .await?;
            return Ok(false);
        };
        let Ok(_handler_permit) = handler_capacity.clone().try_acquire_owned() else {
            // 共享 Worker 达到独立并发上限时把 Run 放回可见索引，不占用全局执行容量等待。
            self.repository
                .defer_run(definition, run_id, message_id)
                .await?;
            return Ok(false);
        };
        let (attempt, attempt_token, redis_now, lease_until) = match self
            .repository
            .start_run(definition, run_id, &self.executor_id, message_id)
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
                    "Run 或定义声明与当前运行时不一致".to_owned(),
                )
                .into());
            }
            // 其它结局意味着未取得执行权（已终态、被他人领取、延后等），消息已在脚本内处置。
            _ => return Ok(false),
        };
        let run = self
            .repository
            .read_run(job_name, run_id)
            .await?
            .ok_or_else(|| crate::job::JobError::Protocol("领取后 Run 记录缺失".to_owned()))?;
        // 参数以 Base64 持久，交给 Handler 前解码为原始字节。
        let payload = base64::engine::general_purpose::STANDARD
            .decode(run.parameter_payload.as_bytes())
            .map_err(|_| {
                self.metrics.add("redis_job_invalid_payload_total", 1);
                NasaRedisError::from(crate::job::JobError::InvalidPayload(
                    "Run 参数 Base64 非法".to_owned(),
                ))
            })?;
        let execution = JobExecution {
            qualifier: self.keyspace.qualifier().to_owned(),
            namespace: self.keyspace.namespace().to_owned(),
            run_id: run_id.to_owned(),
            job_name: job_name.to_owned(),
            worker_name: run.worker_name,
            logical_fire_at: run.logical_fire_at,
            triggered_at: run.triggered_at,
            attempt,
            attempt_token,
            schema_id: run.schema_id,
            wire_codec: run.wire_codec,
            parameter_payload: payload,
            fanout: None,
            authority: JobExecutionAuthority::new(
                redis_now,
                lease_until,
                self.config
                    .renew_rtt_allowance_ms
                    .saturating_add(self.config.clock_drift_allowance_ms),
            ),
            fanout_service: self.fanout_service.clone(),
        };
        self.metrics.add("redis_job_started_total", 1);
        let outcome = self
            .run_with_lease(definition, run_id, attempt_token, handler, &execution)
            .await?;
        // 根已进入持久 Fanout 状态机后，唯一完成出口属于对账流程；普通 finish 会与合法移交竞争并误判失权。
        if execution.authority.is_fanout_transferred() {
            return Ok(true);
        }
        let finish = self
            .repository
            .finish_run(
                definition,
                run_id,
                &self.executor_id,
                attempt_token,
                outcome.code,
                &outcome.summary,
            )
            .await?;
        match finish {
            FinishOutcome::Ok { next_state, .. } => {
                self.record_finished(next_state);
                Ok(true)
            }
            FinishOutcome::StateMismatch
            | FinishOutcome::StaleOwner
            | FinishOutcome::StaleAssignment => {
                self.metrics.add("redis_job_stale_finish_total", 1);
                Err(crate::job::JobError::StaleOwner(
                    "attempt 完成提交已失去当前状态或 owner 权威".to_owned(),
                )
                .into())
            }
            FinishOutcome::IdentityMismatch => {
                self.metrics.add("redis_job_fencing_regression_total", 1);
                Err(crate::job::JobError::FencingRegression(
                    "attempt 完成提交的稳定执行身份不一致".to_owned(),
                )
                .into())
            }
        }
    }

    /// 业务作用：按 Lua 已提交的下一状态累计唯一终态或重试结局。
    ///
    /// 参数说明：`next_state` 来自 `finish_run` 的封闭返回，不使用 Handler 未提交的本地猜测。
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

    /// 业务作用：在 Handler 整个生命周期内续期当前 attempt，并把超时、取消和 panic 收敛为封闭结果码。
    ///
    /// 参数说明：
    /// - `definition`/`run_id`/`attempt_token`: 续期 CAS 所需的任务与当前执行权身份。
    /// - `handler`/`execution`: 业务处理器与冻结上下文。
    ///
    /// 返回：Handler 正常、超时、取消或 panic 时返回可提交结果；续期失败或已失权时返回错误并禁止完成提交。
    async fn run_with_lease(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        attempt_token: i64,
        handler: &Arc<dyn JobHandler>,
        execution: &JobExecution,
    ) -> Result<JobOutcome> {
        let authority = execution.authority.clone();
        let _authority_guard = AuthorityGuard(authority.clone());
        let mut handler_future =
            Box::pin(AssertUnwindSafe(handler.handle(execution)).catch_unwind());
        let run_timeout_ms = definition
            .timeout_ms()
            .min(self.config.max_run_duration_ms)
            .max(1);
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(run_timeout_ms));
        tokio::pin!(timeout);
        let first_renew = tokio::time::Instant::now()
            + std::time::Duration::from_millis(self.config.lease_renew_ms);
        let mut renew = tokio::time::interval_at(
            first_renew,
            std::time::Duration::from_millis(self.config.lease_renew_ms),
        );
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                result = &mut handler_future => {
                    return Ok(match result {
                        Ok(outcome) => outcome,
                        Err(_) => JobOutcome::retry("Handler panic，当前 attempt 按可重试失败收口"),
                    });
                }
                _ = &mut timeout => {
                    return Ok(JobOutcome {
                        code: JobResultCode::Timeout,
                        summary: "Handler 超过任务执行时限".to_owned(),
                    });
                }
                _ = renew.tick() => {
                    // Fanout prepare 已接管根时立即结束普通续期；后续 begin/commit 由持久看门狗收敛。
                    if execution.authority.is_fanout_transferred() {
                        return Ok(JobOutcome::success());
                    }
                    let renewed = match self.repository
                        .renew_run(definition, run_id, &self.executor_id, attempt_token)
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            // 未取得续期证据时立即终止 Handler future，不能把传输失败解释为仍持有租约。
                            self.metrics.add("redis_job_self_fence_total", 1);
                            return Err(error);
                        }
                    };
                    match renewed {
                        RenewOutcome::Ok { cancel_requested: true, .. } => {
                            authority.revoke(true);
                            return Ok(JobOutcome {
                                code: JobResultCode::Cancelled,
                                summary: "收到持久取消请求".to_owned(),
                            });
                        }
                        RenewOutcome::Ok {
                            redis_now,
                            deadline,
                            cancel_requested: false,
                        } => {
                            let updated = authority.renew(
                                redis_now,
                                deadline,
                                self.config
                                    .renew_rtt_allowance_ms
                                    .saturating_add(self.config.clock_drift_allowance_ms),
                            );
                            if !updated {
                                self.metrics.add("redis_job_self_fence_total", 1);
                                return Err(crate::job::JobError::ExecutionStopped(
                                    "当前 attempt 的本地权威门禁已关闭".to_owned(),
                                )
                                .into());
                            }
                        }
                        RenewOutcome::StateMismatch | RenewOutcome::StaleOwner => {
                            if execution.authority.is_fanout_transferred() {
                                return Ok(JobOutcome::success());
                            }
                            self.metrics.add("redis_job_self_fence_total", 1);
                            return Err(crate::job::JobError::StaleOwner(
                                "Handler 执行期间已失去 attempt 权威，已停止本地 future 且禁止完成提交".to_owned(),
                            )
                            .into());
                        }
                    }
                }
            }
        }
    }
}

/// Handler future 的权威关闭守卫；正常返回、续期错误、超时或 task abort 都会永久关闭 checkpoint。
struct AuthorityGuard(Arc<JobExecutionAuthority>);

impl Drop for AuthorityGuard {
    /// 业务作用：在 Handler 执行作用域结束时关闭 attempt 的本地副作用门禁。
    fn drop(&mut self) {
        self.0.revoke(false);
    }
}

/// 业务作用：从 Stream 消息字段表读取一个字段的文本；缺失或非字符串取空串。
///
/// 参数说明：
/// - `map`: 消息字段名到值的映射。
/// - `key`: 字段名。
///
/// 返回：字段文本或空串。
fn field(map: &HashMap<String, redis::Value>, key: &str) -> String {
    match map.get(key) {
        Some(redis::Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        Some(redis::Value::SimpleString(text)) => text.clone(),
        _ => String::new(),
    }
}
