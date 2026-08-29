//! Fanout 监视：驱动跨 slot 对账与看门狗。把桶内到期的 receipt/ready/租约/清理索引分派到对应写脚本，并在批次
//! 终态时把结果回填根 Run、标记已对账。
//!
//! 监视只做证据驱动：真正的重发、唤醒、重分配、恢复与收敛都由各写脚本按 Redis 权威状态复验，监视器丢帧或重复
//! 扫描不会破坏一次性语义。根 Run 的完成权威在批次终态后由 `finish_fanout_root` 回填，使等待子任务的根离开等待态。

use std::collections::HashMap;
use std::sync::Arc;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::fanout::{
    FanoutDeliverOutcome, FanoutReadyDeferOutcome, FanoutReadyPromoteOutcome,
    FanoutReassignOutcome, FanoutReceiptRetryOutcome, FanoutRepository, FanoutRoot, FanoutShard,
    FanoutWatchRootOutcome,
};
use crate::job::keyspace::JobKeyspace;
use crate::job::metrics::JobMetrics;
use crate::job::model::{JobFanoutFailurePolicy, JobState};
use crate::job::registry::{ExecutorMember, ExecutorRegistry, SnapshotOutcome};
use crate::job::repository::{FinishFanoutRootOutcome, JobReapFanoutRequest, JobRepository};

/// 判定 Fanout 根终态的状态集合；只有终态才回填根 Run 并标记对账。
const TERMINAL_ROOT_STATES: [&str; 4] = ["SUCCEEDED", "PARTIAL_FAILED", "FAILED", "CANCELLED"];

/// Fanout 监视器；持有连接、仓库、Fanout 数据模型、键模型与本地定义，驱动对账与看门狗。
pub struct FanoutMonitor {
    client: Arc<RedisClient>,
    repository: JobRepository,
    fanout: FanoutRepository,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
    registry: Arc<ExecutorRegistry>,
    root_definitions: HashMap<String, JobDefinition>,
    worker_definitions: HashMap<String, JobDefinition>,
    metrics: Option<Arc<JobMetrics>>,
}

impl FanoutMonitor {
    /// 业务作用：绑定连接、键模型与配置，创建 Fanout 监视器。
    ///
    /// 参数说明：
    /// - `client`/`keyspace`/`config`: 连接、冻结键模型与已校验配置。
    /// - `registry`: 同一 source 的执行器注册表，用于失联证据与兼容能力快照。
    ///
    /// 返回：可登记根定义并执行对账的监视器。
    pub fn new(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        registry: Arc<ExecutorRegistry>,
    ) -> Self {
        let repository = JobRepository::new(client.clone(), keyspace.clone(), config.clone());
        let fanout = FanoutRepository::new(client.clone(), keyspace.clone(), config.clone());
        Self {
            client,
            repository,
            fanout,
            keyspace,
            config,
            registry,
            root_definitions: HashMap::new(),
            worker_definitions: HashMap::new(),
            metrics: None,
        }
    }

    /// 业务作用：为受管运行时创建共享同一 source 指标容器的 Fanout 监视器。
    ///
    /// 参数说明：连接、布局、配置与 registry 同 `new`，`metrics` 属于同一 source generation。
    ///
    /// 返回：看门狗推进时同步发布固定观测的监视器。
    pub(crate) fn new_with_metrics(
        client: Arc<RedisClient>,
        keyspace: JobKeyspace,
        config: Arc<JobConfig>,
        registry: Arc<ExecutorRegistry>,
        metrics: Arc<JobMetrics>,
    ) -> Self {
        let mut monitor = Self::new(client, keyspace, config, registry);
        monitor.metrics = Some(metrics);
        monitor
    }

    /// 业务作用：登记一个根任务定义，使对账终态回填时可按根任务名找到定义与串行队列。
    ///
    /// 参数说明：
    /// - `definition`: 根任务定义。
    ///
    /// 返回：无返回值；定义名用于根回填，Worker 名用于 shard 租约恢复。
    pub fn register_definition(&mut self, definition: JobDefinition) {
        self.worker_definitions
            .insert(definition.worker_name().to_owned(), definition.clone());
        self.root_definitions
            .insert(definition.name().to_owned(), definition);
    }

    /// 业务作用：读取一个 Fanout 根状态；若已终态，则把结果回填对应根 Run 并标记已对账，使根离开等待子任务态。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：根不存在返回 `None`；存在返回其状态；终态且已登记根定义时同步回填根 Run 与标记对账。
    pub async fn reconcile_root(&self, fanout_id: &str) -> Result<Option<String>> {
        let (state, root_run_id, root_attempt, root_job_name, cancel_reason, error_type) =
            match self.fanout.watch_root(fanout_id).await? {
                FanoutWatchRootOutcome::NotFound => return Ok(None),
                FanoutWatchRootOutcome::Ok {
                    state,
                    root_run_id,
                    root_attempt,
                    root_job_name,
                    cancel_reason,
                    error_type,
                    ..
                } => (
                    state,
                    root_run_id,
                    root_attempt,
                    root_job_name,
                    cancel_reason,
                    error_type,
                ),
            };
        if !TERMINAL_ROOT_STATES.contains(&state.as_str()) {
            return Ok(Some(state));
        }
        // 只有登记了根定义（拥有其串行队列与分片）的节点才回填根 Run，避免无定义节点误动状态。
        let Some(definition) = self.root_definitions.get(&root_job_name) else {
            return Ok(Some(state));
        };
        let summary = if cancel_reason.is_empty() {
            String::new()
        } else {
            cancel_reason.clone()
        };
        let terminal_error = if cancel_reason == "JOB_DELETED" {
            "JOB_DELETED"
        } else {
            error_type.as_str()
        };
        // 把桶内终态回填普通根 Run；终态时释放串行槽并唤醒下一队首。
        match self
            .repository
            .finish_fanout_root(
                definition,
                &root_run_id,
                fanout_id,
                root_attempt,
                &state,
                terminal_error,
                &summary,
            )
            .await?
        {
            FinishFanoutRootOutcome::Ok { .. }
            | FinishFanoutRootOutcome::Adopted
            | FinishFanoutRootOutcome::AlreadyCompleted { .. } => {}
            other => return Err(protocol(&format!("finish_fanout_root 回填拒绝: {other:?}"))),
        }
        // 回填后标记已对账，进入有界清理候选。
        let _ = self.fanout.mark_reconciled(fanout_id).await?;
        Ok(Some(state))
    }

    /// 业务作用：推进删除收敛脚本返回的跨 slot Fanout 根，在关闭桶内新执行权后回填普通根。
    ///
    /// 参数说明：`request` 携带普通根与 Fanout 桶的持久关联证据。
    ///
    /// 返回：桶不存在或已终态时完成普通根回填；桶未终态时发布一批协作式取消；定位冲突或 Redis 失败时返回错误。
    pub(crate) async fn drive_deleted_fanout(&self, request: &JobReapFanoutRequest) -> Result<()> {
        let watched = self.fanout.watch_root(&request.fanout_id).await?;
        let FanoutWatchRootOutcome::Ok {
            state,
            root_run_id,
            root_attempt,
            root_job_name,
            cancel_reason,
            error_type,
            ..
        } = watched
        else {
            // 桶从未建立或已被清理时已无跨 slot 执行权，普通根可直接按删除终态收口。
            return self
                .finish_deleted_root(request, "CANCELLED", "JOB_DELETED", "")
                .await;
        };
        if root_run_id != request.run_id
            || root_attempt != request.root_attempt
            || root_job_name != request.job_name
        {
            // Fanout 标识与普通根证据冲突时拒绝取消，避免关闭不属于该删除任务的桶。
            return Err(protocol("删除收敛的 Fanout 根定位证据冲突"));
        }
        if TERMINAL_ROOT_STATES.contains(&state.as_str()) {
            let terminal_error = if cancel_reason == "JOB_DELETED" {
                "JOB_DELETED"
            } else {
                error_type.as_str()
            };
            self.finish_deleted_root(request, &state, terminal_error, &cancel_reason)
                .await?;
            // 普通根已采用桶内终态后才能撤销桶的对账门禁，避免先清桶再丢失唯一终态证据。
            let _ = self.fanout.mark_reconciled(&request.fanout_id).await?;
            return Ok(());
        }
        let Some(root) = self.fanout.read_root(&request.fanout_id).await? else {
            return Ok(());
        };
        // 先把根发布为 CANCELLING，再按固定批次撤销 shard；后续 control 周期会持续推进到桶终态。
        let _ = self.fanout.cancel_batch(&root, "JOB_DELETED").await?;
        Ok(())
    }

    /// 业务作用：用删除收敛携带的任务名回填普通根，不依赖本节点仍保留对应定义对象。
    ///
    /// 参数说明：`request` 定位普通根；`state`、`error_type` 与 `summary` 来自桶终态或缺桶结局。
    ///
    /// 返回：回填成功、幂等采用或根已终态时成功；其它状态拒绝时返回协议错误。
    async fn finish_deleted_root(
        &self,
        request: &JobReapFanoutRequest,
        state: &str,
        error_type: &str,
        summary: &str,
    ) -> Result<()> {
        match self
            .repository
            .finish_fanout_root_for_job(
                &request.job_name,
                &request.run_id,
                &request.fanout_id,
                request.root_attempt,
                state,
                error_type,
                summary,
            )
            .await?
        {
            FinishFanoutRootOutcome::Ok { .. }
            | FinishFanoutRootOutcome::Adopted
            | FinishFanoutRootOutcome::AlreadyCompleted { .. } => Ok(()),
            other => Err(protocol(&format!(
                "删除收敛的 finish_fanout_root 回填拒绝: {other:?}"
            ))),
        }
    }

    /// 业务作用：扫描一个 Fanout 桶内到期看门狗根与四类到期索引，对根执行对账、对分片驱动 receipt/ready/租约/清理看门狗。
    ///
    /// 参数说明：
    /// - `bucket`: 目标 Fanout 桶下标。
    ///
    /// 返回：本轮观察到的到期根数量；扫描或对账失败时向上返回错误。分片级看门狗按各自脚本证据推进，NOT_DUE 项不动。
    pub async fn reconcile_bucket(&self, bucket: u32) -> Result<usize> {
        let due = self.due_roots(bucket).await?;
        let count = due.len();
        for fanout_id in due {
            self.advance_root(&fanout_id).await?;
            self.reconcile_root(&fanout_id).await?;
        }
        self.sweep_watchdogs(bucket).await?;
        Ok(count)
    }

    /// 业务作用：按桶批量扫描四类到期索引，把 receipt/ready/租约/清理到期项分派到对应写脚本。
    ///
    /// 参数说明：
    /// - `bucket`: 目标 Fanout 桶下标。
    ///
    /// 返回：全部到期项处理成功返回；读取或写脚本失败时向上返回错误。
    async fn sweep_watchdogs(&self, bucket: u32) -> Result<()> {
        let scans = self.fanout.scan_due(bucket).await?;
        if let Some(metrics) = &self.metrics {
            metrics.set_shard_gauge(
                "redis_job_fanout_delivery_pending",
                bucket,
                scans.root_count,
            );
            metrics.set_shard_gauge(
                "redis_job_fanout_gc_pending",
                bucket,
                i64::try_from(scans.gc.members.len()).unwrap_or(i64::MAX),
            );
        }
        // receipt 截止到期：重发当前 assignment 的接收通知并推进下一截止。
        for (member, _score) in &scans.receipts.members {
            let Some((fanout_id, seq)) = parse_member(member) else {
                continue;
            };
            if let (Some(root), Some(shard)) = self.root_and_shard(&fanout_id, seq).await? {
                match self.fanout.retry_receipt(&root, &shard).await? {
                    FanoutReceiptRetryOutcome::Ok { .. } => {
                        self.add_metric("redis_job_fanout_receipt_timeout_total", 1);
                        self.add_metric("redis_job_fanout_receipt_retry_total", 1);
                    }
                    FanoutReceiptRetryOutcome::RetryExhausted { .. } => {
                        self.add_metric("redis_job_fanout_receipt_timeout_total", 1);
                        self.handle_unavailable(&root, &shard, "RECEIPT_TIMEOUT")
                            .await?;
                    }
                    FanoutReceiptRetryOutcome::StaleAssignment => {
                        self.add_metric("redis_job_fanout_stale_assignment_total", 1);
                    }
                    FanoutReceiptRetryOutcome::Stale | FanoutReceiptRetryOutcome::NotDue { .. } => {
                    }
                }
            }
        }
        // ready 截止到期：重新唤醒已接收未 start 的分片。
        for (member, _score) in &scans.ready.members {
            let Some((fanout_id, seq)) = parse_member(member) else {
                continue;
            };
            if let (Some(root), Some(shard)) = self.root_and_shard(&fanout_id, seq).await? {
                match self.fanout.promote_ready(&root, &shard).await? {
                    FanoutReadyPromoteOutcome::Ok { .. } => {
                        self.add_metric("redis_job_fanout_ready_wakeup_total", 1);
                    }
                    FanoutReadyPromoteOutcome::CapacityExhausted { .. } => {
                        // 容量证据表明目标仍在线；先尝试切换兼容节点，避免把满载误判为失联并烧尽恢复配额。
                        self.add_metric("redis_job_fanout_ready_wakeup_total", 1);
                        self.handle_capacity_pressure(&root, &shard).await?;
                    }
                    FanoutReadyPromoteOutcome::WakeupExhausted { .. } => {
                        self.add_metric("redis_job_fanout_ready_wakeup_total", 1);
                        self.handle_unavailable(&root, &shard, "START_TIMEOUT")
                            .await?;
                    }
                    FanoutReadyPromoteOutcome::StaleAssignment => {
                        self.add_metric("redis_job_fanout_stale_assignment_total", 1);
                    }
                    FanoutReadyPromoteOutcome::Stale | FanoutReadyPromoteOutcome::NotDue { .. } => {
                    }
                }
            }
        }
        // 租约到期：撤销旧 owner 权威并按失败策略推进重试、重分配或收敛。
        for (member, _score) in &scans.leases.members {
            let Some((fanout_id, seq)) = parse_member(member) else {
                continue;
            };
            if let (Some(root), Some(shard)) = self.root_and_shard(&fanout_id, seq).await? {
                let recovered =
                    if let Some(definition) = self.worker_definitions.get(&shard.worker_name) {
                        self.fanout.recover_shard(&root, &shard, definition).await?
                    } else if root.state == "CANCELLING" {
                        // 删除会撤销本地 Worker 契约；取消专用模式只收敛崩溃 owner，不借用其它配置创建新 attempt。
                        self.fanout.recover_cancelling_shard(&root, &shard).await?
                    } else {
                        continue;
                    };
                if matches!(recovered, crate::job::repository::RecoverOutcome::Ok { .. }) {
                    self.add_metric("redis_job_lease_expired_total", 1);
                }
            }
        }
        // 清理到期：对已对账终态 Fanout 有界回收 shard 与索引。
        for (member, _score) in &scans.gc.members {
            if let Some(root) = self.fanout.read_root(member).await? {
                self.fanout.cleanup(&root).await?;
            }
        }
        Ok(())
    }

    /// 业务作用：恢复中断的 Fanout 建立、首次投递、取消传播与等待能力轮询，使根索引能够最终收敛。
    ///
    /// 参数说明：
    /// - `fanout_id`: 看门狗索引给出的 Fanout 标识。
    ///
    /// 返回：根不存在或已终态时幂等成功；任一权威读取或状态脚本失败时返回错误。
    async fn advance_root(&self, fanout_id: &str) -> Result<()> {
        let Some(root) = self.fanout.read_root(fanout_id).await? else {
            return Ok(());
        };
        if TERMINAL_ROOT_STATES.contains(&root.state.as_str()) {
            return Ok(());
        }
        match root.state.as_str() {
            "CREATING" => {
                self.fanout.fail_creating(fanout_id).await?;
                return Ok(());
            }
            "CANCELLING" => {
                let reason = if root.cancel_reason.trim().is_empty() {
                    "CANCEL_REQUESTED"
                } else {
                    root.cancel_reason.as_str()
                };
                self.fanout.cancel_batch(&root, reason).await?;
                return Ok(());
            }
            "COMMITTED" | "WAITING_CHILDREN" => {}
            _ => return Err(protocol("Fanout 根状态不属于封闭状态集合")),
        }
        if root.delivery_cursor < root.shard_total {
            let count = (root.shard_total - root.delivery_cursor)
                .min(self.config.fanout_delivery_batch_size as i64)
                .max(0);
            let mut shards = Vec::with_capacity(count as usize);
            for seq in root.delivery_cursor..root.delivery_cursor + count {
                let shard = self
                    .fanout
                    .read_shard(fanout_id, seq)
                    .await?
                    .ok_or_else(|| protocol("首次投递恢复缺少已提交的 shard"))?;
                shards.push(shard);
            }
            match self
                .fanout
                .deliver_assignments(fanout_id, &shards, true)
                .await?
            {
                FanoutDeliverOutcome::Ok { .. } => {}
                FanoutDeliverOutcome::NotCommitted => {
                    return Err(protocol("Fanout 根投递时不再处于已提交状态"));
                }
            }
        }
        self.recover_capabilities(&root).await
    }

    /// 业务作用：对已确认无法由当前 assignment 启动的 shard 应用根冻结的失败策略。
    ///
    /// 参数说明：
    /// - `root`/`shard`: 当前根与 assignment 的权威投影。
    /// - `reason`: 进入收敛路径的稳定结果摘要。
    ///
    /// 返回：严格快照保持原 assignment；尽力模式聚合 SKIPPED；重分配模式切换兼容目标或进入等待能力状态。
    async fn handle_unavailable(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        reason: &str,
    ) -> Result<()> {
        let policy = JobFanoutFailurePolicy::parse(&root.failure_policy)
            .ok_or_else(|| protocol("Fanout 根包含未知失败策略"))?;
        if policy == JobFanoutFailurePolicy::StrictSnapshot {
            return Ok(());
        }
        // 证据必须绑定目标启动代次与心跳修订号，陈旧 assignment 不能降低新进程的可用状态。
        self.registry
            .record_fanout_evidence(
                &shard.target_node_identity,
                &shard.target_startup_id,
                shard.target_heartbeat_revision,
                &root.fanout_id,
            )
            .await?;
        if policy == JobFanoutFailurePolicy::BestEffort {
            self.fanout
                .aggregate(
                    &root.fanout_id,
                    shard.seq,
                    JobState::Skipped,
                    "SKIPPED",
                    reason,
                )
                .await?;
            return Ok(());
        }
        let candidates = self.compatible_members(root).await?;
        let target = candidates
            .iter()
            .find(|candidate| candidate.node_identity != shard.target_node_identity);
        self.reassign_and_deliver(root, shard, target).await
    }

    /// 业务作用：在 CAS 撤销旧 inbox 与期限索引后切换 assignment，并只对成功切换的新代次建立投递。
    ///
    /// 参数说明：`root`/`shard` 为旧 assignment；`target` 为空时持久进入等待兼容能力状态。
    ///
    /// 返回：竞争陈旧、执行中或达到上限均按幂等收敛；新 assignment 建立后完成持久投递。
    async fn reassign_and_deliver(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        target: Option<&ExecutorMember>,
    ) -> Result<()> {
        self.reassign_and_deliver_mode(root, shard, target, "FAILURE")
            .await
    }

    /// 业务作用：按指定关闭模式 CAS 切换 Fanout shard assignment，并只为成功的新代次投递一次。
    ///
    /// 参数说明：
    /// - `root`：当前 Fanout 根事实。
    /// - `shard`：需要撤销旧 inbox 并重分配的 shard。
    /// - `target`：兼容时选定的新执行器；为空时持久进入等待状态。
    /// - `mode`：传给共享 Lua 的封闭重分配模式。
    ///
    /// 返回：旧代次已经收敛或新 assignment 完成持久投递时成功；Redis 协议与传输失败返回错误。
    async fn reassign_and_deliver_mode(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        target: Option<&ExecutorMember>,
        mode: &str,
    ) -> Result<()> {
        match self
            .fanout
            .reassign_shard_mode(root, shard, target, mode)
            .await?
        {
            FanoutReassignOutcome::Ok { .. } => {
                self.add_metric("redis_job_fanout_reassign_total", 1);
                let reassigned = self
                    .fanout
                    .read_shard(&root.fanout_id, shard.seq)
                    .await?
                    .ok_or_else(|| protocol("重分配后 shard 不存在"))?;
                match self
                    .fanout
                    .deliver_assignments(&root.fanout_id, &[reassigned], false)
                    .await?
                {
                    FanoutDeliverOutcome::Ok { .. } => Ok(()),
                    FanoutDeliverOutcome::NotCommitted => {
                        Err(protocol("重分配投递时 Fanout 根不再处于已提交状态"))
                    }
                }
            }
            FanoutReassignOutcome::StaleAssignment => {
                self.add_metric("redis_job_fanout_stale_assignment_total", 1);
                Ok(())
            }
            FanoutReassignOutcome::Busy | FanoutReassignOutcome::NoCapableExecutor => Ok(()),
            FanoutReassignOutcome::StaleCapacity => Ok(()),
            FanoutReassignOutcome::CapacityRouteExhausted => {
                match self
                    .fanout
                    .defer_ready_for_capacity_mode(shard, true)
                    .await?
                {
                    FanoutReadyDeferOutcome::Deferred { .. }
                    | FanoutReadyDeferOutcome::CapacityExhausted { .. }
                    | FanoutReadyDeferOutcome::Stale
                    | FanoutReadyDeferOutcome::StaleAssignment => Ok(()),
                }
            }
        }
    }

    /// 业务作用：轮转等待兼容能力的 shard，让后来加入的执行器能够以新 assignment 接管。
    ///
    /// 参数说明：`root` 为本轮读取的非终态根投影。
    ///
    /// 返回：有界检查完成并 CAS 推进游标；合同冲突按当前无候选处理，其它 Redis 错误向上返回。
    async fn recover_capabilities(&self, root: &FanoutRoot) -> Result<()> {
        if root.shard_total <= 0 {
            return Ok(());
        }
        let start = root.capability_cursor.rem_euclid(root.shard_total);
        let count = root
            .shard_total
            .min(self.config.fanout_delivery_batch_size as i64)
            .max(0);
        let candidates = self.compatible_members(root).await?;
        for offset in 0..count {
            let seq = (start + offset).rem_euclid(root.shard_total);
            let Some(shard) = self.fanout.read_shard(&root.fanout_id, seq).await? else {
                continue;
            };
            if shard.state != JobState::AwaitingCapability.wire_name() {
                continue;
            }
            let target = candidates
                .iter()
                .find(|candidate| candidate.node_identity != shard.target_node_identity);
            if target.is_some() {
                self.reassign_and_deliver(root, &shard, target).await?;
            }
        }
        let next = (start + count).rem_euclid(root.shard_total);
        self.fanout
            .advance_capability_cursor(&root.fanout_id, root.capability_cursor, next)
            .await?;
        Ok(())
    }

    /// 业务作用：处理持续容量压力；可切换目标时改派，配额耗尽或无候选时开启新的容量等待窗口。
    ///
    /// 参数说明：`root` 与 `shard` 为当前扫描得到的 Fanout 投影。
    ///
    /// 返回：改派或重新排队成功返回；状态、代次变化时忽略陈旧裁决，Redis 错误向上返回。
    async fn handle_capacity_pressure(&self, root: &FanoutRoot, shard: &FanoutShard) -> Result<()> {
        let target = if shard.assignment_count < i64::from(self.config.fanout_max_assignments) {
            self.compatible_members(root)
                .await?
                .into_iter()
                .find(|member| member.node_identity != shard.target_node_identity)
        } else {
            None
        };
        if let Some(target) = target {
            match self
                .reassign_and_deliver_mode(root, shard, Some(&target), "CAPACITY")
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => Err(error),
            }
        } else {
            match self
                .fanout
                .defer_ready_for_capacity_mode(shard, true)
                .await?
            {
                FanoutReadyDeferOutcome::Deferred { .. }
                | FanoutReadyDeferOutcome::CapacityExhausted { .. }
                | FanoutReadyDeferOutcome::Stale
                | FanoutReadyDeferOutcome::StaleAssignment => Ok(()),
            }
        }
    }

    /// 业务作用：读取与根冻结合同精确兼容且已发布 Fanout Ready 的执行器候选。
    ///
    /// 参数说明：`root` 提供 Worker、契约修订、Schema 与唯一 codec。
    ///
    /// 返回：兼容快照成员；合同冲突视为空集合，不把不兼容执行器选为新目标。
    async fn compatible_members(&self, root: &FanoutRoot) -> Result<Vec<ExecutorMember>> {
        let members = match self
            .registry
            .snapshot(
                &root.worker_name,
                root.contract_revision,
                &root.schema_id,
                &root.wire_codec,
            )
            .await?
        {
            SnapshotOutcome::Ok(snapshot) => snapshot.members,
            SnapshotOutcome::ContractMismatch => Vec::new(),
        };
        if let Some(metrics) = &self.metrics {
            metrics.set(
                "redis_job_capable_executor_count",
                i64::try_from(members.len()).unwrap_or(i64::MAX),
            );
        }
        Ok(members)
    }

    /// 业务作用：向受管 source 的固定指标全集累计值；独立低层使用时保持零开销。
    ///
    /// 参数说明：`name` 为固定 family，`delta` 为本轮非负增量。
    ///
    /// 返回：无返回值。
    fn add_metric(&self, name: &'static str, delta: i64) {
        if let Some(metrics) = &self.metrics {
            metrics.add(name, delta);
        }
    }

    /// 业务作用：读取一个 Fanout 根与其某分片的投影，供分片级看门狗构造参数。
    ///
    /// 参数说明：
    /// - `fanout_id`/`seq`: Fanout 标识与分片序号。
    ///
    /// 返回：根与分片各自存在与否；任一缺失即视为陈旧证据由调用方跳过。
    #[allow(clippy::type_complexity)]
    async fn root_and_shard(
        &self,
        fanout_id: &str,
        seq: i64,
    ) -> Result<(
        Option<crate::job::fanout::FanoutRoot>,
        Option<crate::job::fanout::FanoutShard>,
    )> {
        let root = self.fanout.read_root(fanout_id).await?;
        let shard = self.fanout.read_shard(fanout_id, seq).await?;
        Ok((root, shard))
    }

    /// 业务作用：读取一个 Fanout 桶内看门狗到期的根标识集合。
    ///
    /// 参数说明：
    /// - `bucket`: 目标 Fanout 桶下标。
    ///
    /// 返回：score 不晚于当前的根标识；扫描失败时向上返回错误。
    async fn due_roots(&self, bucket: u32) -> Result<Vec<String>> {
        // 看门狗 ZSET score 为下一看门时刻；取不晚于当前时刻的到期根，成员即 fanoutId。
        let time = redis::cmd("TIME");
        let time_reply: (i64, i64) = {
            let mut conn = self.client.conn();
            time.query_async(&mut conn)
                .await
                .map_err(NasaRedisError::Redis)?
        };
        let now = time_reply.0 * 1000 + time_reply.1 / 1000;
        let mut cmd = redis::cmd("ZRANGEBYSCORE");
        cmd.arg(self.keyspace.fanout_roots_at(bucket))
            .arg("-inf")
            .arg(now)
            .arg("LIMIT")
            .arg(0)
            .arg(self.config.scan_batch_size);
        let mut conn = self.client.conn();
        cmd.query_async::<Vec<String>>(&mut conn)
            .await
            .map_err(NasaRedisError::Redis)
    }
}

/// 业务作用：把 receipt/ready/租约索引成员 `fanoutId:seq` 拆为标识与分片序号。
///
/// 参数说明：
/// - `member`: 索引成员文本。`fanoutId` 为十六进制无冒号，`seq` 为十进制。
///
/// 返回：格式合法返回 `(fanoutId, seq)`；缺分隔符或 seq 非数值返回 `None` 由调用方跳过。
fn parse_member(member: &str) -> Option<(String, i64)> {
    let (fanout_id, seq) = member.rsplit_once(':')?;
    let seq = seq.parse::<i64>().ok()?;
    Some((fanout_id.to_owned(), seq))
}

/// 业务作用：构造 Job 协议错误。参数说明：`message` 摘要。返回：协议错误。
fn protocol(message: &str) -> NasaRedisError {
    crate::job::JobError::Protocol(message.to_owned()).into()
}
