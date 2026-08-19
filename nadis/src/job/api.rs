//! RedisJob 业务控制与查询门面：调用方显式选择 source 后，只提交结构化请求，不接触 Lua 或裸返回数组。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{NasaRedisError, Result};
use crate::job::definition::JobDefinition;
use crate::job::fanout::{FanoutRepository, FanoutRoot, FanoutShard};
use crate::job::metrics::{JobMetrics, JobMetricsSnapshot, JobSupervisorLoop};
use crate::job::model::JobDefinitionState;
use crate::job::names::require_name;
use crate::job::payload::JobPayload;
use crate::job::registry::ExecutorRegistry;
use crate::job::repository::{
    CancelOutcome, DefinitionControlOutcome, DeleteOutcome, JobDefinitionRecord, JobRepository,
    ManualFireOutcome, NamespaceStateOutcome,
};
use crate::job::run::JobRun;

/// 单个 Redis source 的聚合运行状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSourceHealthState {
    /// 全部关键循环处于当前正常代次并接受新工作。
    Up,
    /// 可恢复循环正在预算内重建，当前 source 暂时降级。
    Degraded,
    /// 权威失效或重启预算耗尽，source 已关闭准入。
    NotReady,
    /// 正常停机已关闭准入，正在收口已有执行。
    Draining,
}

/// source 健康原因的封闭分类；不包含 endpoint、任务名、payload 或底层自由文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSourceHealthReason {
    /// Redis transport、路由或命令响应暂时不可用。
    RedisTransport,
    /// 持久线协议、脚本返回或 source 绑定不一致。
    ProtocolContract,
    /// 写出后的控制动作结局无法确认。
    ExecutionUnknown,
    /// attempt、executor 或心跳权威已经失效。
    ExecutionAuthority,
    /// 冻结运行配置不再满足安全门禁。
    RuntimeConfiguration,
    /// RESP3 订阅代次未就绪。
    SubscriptionNotReady,
    /// 受管指标 descriptor 或最坏 series 容量未能在 Prepare 阶段预留。
    MetricsReservation,
    /// Cron 表达式、时区规则或跨语言语义证明不兼容。
    CronSemantics,
    /// 可恢复循环耗尽有界重启预算。
    RestartBudgetExhausted,
    /// 未归入更具体类别的关键运行失败。
    CriticalRuntime,
}

impl JobSourceHealthReason {
    /// 业务作用：返回健康与受管 readiness 使用的稳定原因文本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不携带运行期任意值的封闭名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RedisTransport => "redis_transport",
            Self::ProtocolContract => "protocol_contract",
            Self::ExecutionUnknown => "execution_unknown",
            Self::ExecutionAuthority => "execution_authority",
            Self::RuntimeConfiguration => "runtime_configuration",
            Self::SubscriptionNotReady => "subscription_not_ready",
            Self::MetricsReservation => "metrics_reservation",
            Self::CronSemantics => "cron_semantics",
            Self::RestartBudgetExhausted => "restart_budget_exhausted",
            Self::CriticalRuntime => "critical_runtime",
        }
    }
}

/// 单个 Redis source 的运行健康快照；原因只描述封闭基础设施类别。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSourceHealthSnapshot {
    /// canonical source id。
    pub qualifier: String,
    /// 是否仍接受新通知与领取。
    pub accepting: bool,
    /// 关键后台循环是否仍保持运行。
    pub healthy: bool,
    /// 聚合状态，区分预算内恢复、权威失效与正常 drain。
    pub state: JobSourceHealthState,
    /// 当前聚合原因；完全健康或正常 drain 时为空。
    pub reason: Option<JobSourceHealthReason>,
    /// 兼容文本投影；内容来自 `reason` 的封闭名称。
    pub failure_reason: Option<String>,
}

/// 运行时内部共享的 source 健康状态。
pub(crate) struct SourceHealth {
    qualifier: String,
    accepting: AtomicBool,
    draining: AtomicBool,
    terminal_reason: Mutex<Option<JobSourceHealthReason>>,
    degraded_loops: Mutex<BTreeMap<JobSupervisorLoop, (JobSourceHealthReason, u32)>>,
    metrics: Arc<JobMetrics>,
}

impl SourceHealth {
    /// 业务作用：为一个已冻结 source 建立初始健康状态。
    ///
    /// 参数说明：`qualifier` 为 canonical source id。
    ///
    /// 返回：初始接受准入且无关键失败的状态。
    pub(crate) fn new(qualifier: impl Into<String>, metrics: Arc<JobMetrics>) -> Self {
        Self {
            qualifier: qualifier.into(),
            accepting: AtomicBool::new(true),
            draining: AtomicBool::new(false),
            terminal_reason: Mutex::new(None),
            degraded_loops: Mutex::new(BTreeMap::new()),
            metrics,
        }
    }

    /// 业务作用：记录可恢复循环的一次失败并推进监督代次，source 在预算内保持可服务但标记 Degraded。
    ///
    /// 参数说明：`loop_name` 为封闭循环身份，`reason` 为稳定失败类别。
    ///
    /// 返回：下一次将启动的单调 generation。
    pub(crate) fn restart_scheduled(
        &self,
        loop_name: JobSupervisorLoop,
        reason: JobSourceHealthReason,
        first_failure: bool,
    ) -> u64 {
        if first_failure {
            let mut degraded = self
                .degraded_loops
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = degraded.entry(loop_name).or_insert((reason, 0));
            entry.0 = reason;
            entry.1 = entry.1.saturating_add(1);
        }
        self.metrics.restart_scheduled(loop_name)
    }

    /// 业务作用：标记一个可恢复循环的新代次已经成功推进，并仅清除该循环的降级事实。
    ///
    /// 参数说明：`loop_name` 为封闭循环身份。
    ///
    /// 返回：无；其它循环或 terminal 失败不被覆盖。
    pub(crate) fn recovered(&self, loop_name: JobSupervisorLoop) {
        let mut degraded = self
            .degraded_loops
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, count)) = degraded.get_mut(&loop_name) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                degraded.remove(&loop_name);
            }
        }
        drop(degraded);
        self.metrics.recovered(loop_name);
    }

    /// 业务作用：在可恢复循环耗尽预算时关闭 source 准入并保留终态原因。
    ///
    /// 参数说明：`loop_name` 为耗尽预算的循环。
    ///
    /// 返回：无；其它 source 不受影响。
    pub(crate) fn budget_exhausted(&self, loop_name: JobSupervisorLoop) {
        self.accepting.store(false, Ordering::Release);
        self.metrics.budget_exhausted(loop_name);
        let mut terminal = self
            .terminal_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        terminal.get_or_insert(JobSourceHealthReason::RestartBudgetExhausted);
    }

    /// 业务作用：记录 attempt 或 executor 权威循环失效，立即关闭当前 source 的新工作准入。
    ///
    /// 参数说明：`loop_name` 为失效循环，`error` 仅用于归入封闭原因。
    ///
    /// 返回：无；首个 terminal 原因稳定保留。
    pub(crate) fn authority_lost(&self, loop_name: JobSupervisorLoop, error: &NasaRedisError) {
        self.accepting.store(false, Ordering::Release);
        self.metrics.authority_lost(loop_name);
        let reason = classify_error(error);
        let mut terminal = self
            .terminal_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        terminal.get_or_insert(reason);
    }

    /// 业务作用：在正常停机或失败停机开始时关闭新任务准入。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无返回值；既有失败原因保持不变。
    pub(crate) fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
        self.draining.store(true, Ordering::Release);
    }

    /// 业务作用：返回当前 source 的共享本地指标容器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 source generation 同生命周期的指标句柄。
    pub(crate) fn metrics(&self) -> Arc<JobMetrics> {
        self.metrics.clone()
    }

    /// 业务作用：生成一致的逐 source 健康快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前准入、关键循环状态与首个失败原因。
    pub(crate) fn snapshot(&self) -> JobSourceHealthSnapshot {
        let terminal = *self
            .terminal_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let degraded = self
            .degraded_loops
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reason = terminal.or_else(|| degraded.values().next().map(|(reason, _)| *reason));
        let state = if terminal.is_some() {
            JobSourceHealthState::NotReady
        } else if self.draining.load(Ordering::Acquire) {
            JobSourceHealthState::Draining
        } else if degraded.is_empty() {
            JobSourceHealthState::Up
        } else {
            JobSourceHealthState::Degraded
        };
        JobSourceHealthSnapshot {
            qualifier: self.qualifier.clone(),
            accepting: self.accepting.load(Ordering::Acquire),
            healthy: terminal.is_none(),
            state,
            reason,
            failure_reason: reason.map(|value| value.as_str().to_owned()),
        }
    }
}

/// 业务作用：把内部错误归入健康快照允许公开的封闭类别。
///
/// 参数说明：`error` 为 source 循环返回的错误，不会把其自由文本写入快照。
///
/// 返回：稳定、低基数的健康原因。
fn classify_error(error: &NasaRedisError) -> JobSourceHealthReason {
    match error {
        NasaRedisError::Redis(_) | NasaRedisError::ConnectProbe { .. } => {
            JobSourceHealthReason::RedisTransport
        }
        NasaRedisError::JobProtocol(_) => JobSourceHealthReason::ProtocolContract,
        NasaRedisError::ExecutionUnknown(_) => JobSourceHealthReason::ExecutionUnknown,
        NasaRedisError::JobExecutionStopped(_) => JobSourceHealthReason::ExecutionAuthority,
        NasaRedisError::Config(_) => JobSourceHealthReason::RuntimeConfiguration,
        NasaRedisError::Job(error) => match error {
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

/// 受管控制面持有的本地定义引用与待撤销能力；不拥有运行时停机权。
pub(crate) struct ManagedJobControlState {
    registry: Arc<ExecutorRegistry>,
    definitions: Mutex<BTreeMap<String, Option<String>>>,
    pending_withdrawals: Mutex<std::collections::BTreeSet<String>>,
    completed_deletions: Mutex<BTreeMap<(String, i64), DeleteOutcome>>,
}

impl ManagedJobControlState {
    /// 业务作用：冻结当前 source 的本地任务到 Worker 引用表，使删除最后一个定义时能撤销对应能力。
    ///
    /// 参数说明：`registry` 属于同一 source generation，`definitions` 为任务名到可选 Fanout Worker 的映射。
    ///
    /// 返回：可由多个控制句柄共享、按 Worker 引用归零撤销能力的状态。
    pub(crate) fn new(
        registry: Arc<ExecutorRegistry>,
        definitions: BTreeMap<String, Option<String>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            definitions: Mutex::new(definitions),
            pending_withdrawals: Mutex::new(std::collections::BTreeSet::new()),
            completed_deletions: Mutex::new(BTreeMap::new()),
        })
    }

    /// 业务作用：确认控制动作只删除当前 generation 实际登记的本地定义，避免任意节点撤销无 Handler 的定义。
    ///
    /// 参数说明：`job_name` 为待删除任务名。
    ///
    /// 返回：本地定义存在时为真；已删除或从未登记时为假。
    fn contains_definition(&self, job_name: &str) -> bool {
        self.definitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(job_name)
    }

    /// 业务作用：读取当前 generation 已提交的定义删除结局，使能力撤销失败后的业务重试保持幂等。
    ///
    /// 参数说明：`job_name` 与 `revision` 必须和此前已写入 tombstone 的控制请求完全一致。
    ///
    /// 返回：本地已确认 tombstone 时返回首次权威结局；尚未提交删除时返回 `None`。
    fn completed_deletion(&self, job_name: &str, revision: i64) -> Option<DeleteOutcome> {
        self.completed_deletions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(job_name.to_owned(), revision))
            .cloned()
    }

    /// 业务作用：在持久删除成功后移除本地定义引用，并把引用归零的 Worker 放入能力撤销队列。
    ///
    /// 参数说明：`job_name`/`revision` 已由 Redis tombstone 确认删除，`outcome` 为首次权威删除结局。
    ///
    /// 返回：无；删除结局保留到 source generation 结束，共享 Worker 仍有其它本地定义时保留能力。
    fn definition_deleted(&self, job_name: &str, revision: i64, outcome: DeleteOutcome) {
        self.completed_deletions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry((job_name.to_owned(), revision))
            .or_insert(outcome);
        let worker = {
            let mut definitions = self
                .definitions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(worker) = definitions.remove(job_name) else {
                return;
            };
            let Some(worker) = worker else {
                return;
            };
            if definitions
                .values()
                .any(|other| other.as_ref() == Some(&worker))
            {
                return;
            }
            worker
        };
        self.pending_withdrawals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(worker);
    }

    /// 业务作用：重试全部已归零 Worker 的原子能力撤销，成功项才从待办集合移除。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部待办完成时成功；首个传输或脚本错误立即返回并保留未确认项供相同控制调用重试。
    async fn withdraw_pending(&self) -> Result<()> {
        let workers: Vec<String> = self
            .pending_withdrawals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        for worker in workers {
            self.registry.remove_capability(&worker).await?;
            self.pending_withdrawals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&worker);
        }
        Ok(())
    }
}

/// 一个已绑定 source 的结构化控制入口；网络鉴权仍由业务边界负责。
#[derive(Clone)]
pub struct JobControl {
    repository: JobRepository,
    fanout: FanoutRepository,
    health: Option<Arc<SourceHealth>>,
    managed: Option<Arc<ManagedJobControlState>>,
}

impl JobControl {
    /// 业务作用：把普通 Run 控制仓库与同 source Fanout 仓库组合为唯一业务控制入口。
    ///
    /// 参数说明：两个仓库必须绑定同一 qualifier、namespace 与配置。
    ///
    /// 返回：可执行结构化控制动作的门面。
    pub fn new(repository: JobRepository, fanout: FanoutRepository) -> Self {
        Self {
            repository,
            fanout,
            health: None,
            managed: None,
        }
    }

    /// 业务作用：建立绑定 source 生命周期准入的控制门面，使已克隆句柄在 drain 后也不能提交新动作。
    ///
    /// 参数说明：仓库、Fanout 仓库、`health` 与 `managed` 必须属于同一 source generation；后者维护定义能力引用。
    ///
    /// 返回：每次控制调用都会复验当前准入状态的门面。
    pub(crate) fn new_with_health(
        repository: JobRepository,
        fanout: FanoutRepository,
        health: Arc<SourceHealth>,
        managed: Arc<ManagedJobControlState>,
    ) -> Self {
        Self {
            repository,
            fanout,
            health: Some(health),
            managed: Some(managed),
        }
    }

    /// 业务作用：在任何控制面副作用前复验 source generation 仍接受新动作，防止停机前取得的句柄越过 drain。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未绑定生命周期或 source 仍接受准入时成功；停止后返回 `JobError::ExecutionStopped`。
    fn ensure_accepting(&self) -> Result<()> {
        let Some(health) = &self.health else {
            return Ok(());
        };
        let snapshot = health.snapshot();
        if snapshot.accepting {
            Ok(())
        } else {
            Err(crate::job::JobError::ExecutionStopped(format!(
                "source {} 已关闭控制准入",
                snapshot.qualifier
            ))
            .into())
        }
    }

    /// 业务作用：以 requestId 幂等触发一个手工 Run。
    ///
    /// 参数说明：`definition` 为冻结定义，`request_id` 为业务幂等标识，`payload` 为显式编码载荷。
    ///
    /// 返回：首次创建、采用、定义缺失或状态拒绝的封闭结局。
    pub async fn trigger(
        &self,
        definition: &JobDefinition,
        request_id: &str,
        payload: &JobPayload,
    ) -> Result<ManualFireOutcome> {
        self.ensure_accepting()?;
        self.repository
            .manual_fire(definition, request_id, payload)
            .await
    }

    /// 业务作用：暂停一个定义的新自动触发，不撤销已有执行权。
    ///
    /// 参数说明：`job_name` 为目标任务名。
    ///
    /// 返回：动作生效、定义缺失或状态拒绝的封闭结局。
    pub async fn pause(&self, job_name: &str) -> Result<DefinitionControlOutcome> {
        self.ensure_accepting()?;
        self.repository.pause(job_name).await
    }

    /// 业务作用：从调用方计算的下一逻辑时刻恢复一个定义。
    ///
    /// 参数说明：`job_name` 为任务名，`next_fire_at` 为权威毫秒时刻，零表示无自动时刻。
    ///
    /// 返回：动作生效、定义缺失或状态拒绝的封闭结局。
    pub async fn resume(
        &self,
        job_name: &str,
        next_fire_at: i64,
    ) -> Result<DefinitionControlOutcome> {
        self.ensure_accepting()?;
        self.repository.resume(job_name, next_fire_at).await
    }

    /// 业务作用：最终一致地关闭当前 source 全部调度分片的普通 Run 新执行权门禁，不影响其它 Redis source。
    ///
    /// 参数说明：`actor` 为进入持久审计字段的操作来源，按名称合同规范化。
    ///
    /// 返回：全部分片发布 `PAUSED` 后成功；中途失败返回错误，已发布分片保持关闭，调用方可用同一 API 幂等续作。
    pub async fn pause_namespace(&self, actor: &str) -> Result<()> {
        self.set_namespace_state(JobDefinitionState::Paused, actor)
            .await
    }

    /// 业务作用：最终一致地重新开放当前 source 全部调度分片的普通 Run 新执行权门禁，不影响其它 Redis source。
    ///
    /// 参数说明：`actor` 为进入持久审计字段的操作来源，按名称合同规范化。
    ///
    /// 返回：全部分片发布 `ENABLED` 后成功；中途失败返回错误，调用方可重试以收敛部分发布状态。
    pub async fn resume_namespace(&self, actor: &str) -> Result<()> {
        self.set_namespace_state(JobDefinitionState::Enabled, actor)
            .await
    }

    /// 业务作用：先完成控制参数校验，再逐分片传播命名空间门禁，避免非法参数造成部分状态变更。
    ///
    /// 参数说明：`state` 仅由暂停/恢复入口传入，`actor` 为规范化前的操作来源。
    ///
    /// 返回：全部分片接受状态时成功；传输失败保留已完成分片供幂等重试，脚本拒绝已知状态时按协议不一致返回。
    async fn set_namespace_state(&self, state: JobDefinitionState, actor: &str) -> Result<()> {
        self.ensure_accepting()?;
        let actor = require_name(actor, "actor")?;
        for shard in 0..self.repository.shard_count() {
            if matches!(
                self.repository
                    .set_namespace_state(shard, state, &actor)
                    .await?,
                NamespaceStateOutcome::Invalid
            ) {
                return Err(crate::job::JobError::Protocol(
                    "namespace_set_state 拒绝内部已校验状态".to_owned(),
                )
                .into());
            }
        }
        Ok(())
    }

    /// 业务作用：请求取消普通 Run，并在它已移交 Fanout 时同步把批次根切入协作式取消流程。
    ///
    /// 参数说明：`definition` 与 `run_id` 精确定位当前 source 的 Run。
    ///
    /// 返回：普通 Run 的封闭取消结局；Fanout 首批取消提交失败时返回错误并由持久看门狗继续处理。
    pub async fn request_cancel(
        &self,
        definition: &JobDefinition,
        run_id: &str,
    ) -> Result<CancelOutcome> {
        self.ensure_accepting()?;
        let run = self.repository.read_run(definition.name(), run_id).await?;
        let outcome = self.repository.request_cancel(definition, run_id).await?;
        if matches!(outcome, CancelOutcome::Ok { .. }) {
            if let Some(run) = run.filter(|run| !run.fanout_id.is_empty()) {
                if let Some(root) = self.fanout.read_root(&run.fanout_id).await? {
                    self.fanout.cancel_batch(&root, "CANCEL_REQUESTED").await?;
                }
            }
        }
        Ok(outcome)
    }

    /// 业务作用：以单调修订号删除定义并留下 tombstone。
    ///
    /// 参数说明：`job_name` 为任务名，`revision` 不得低于当前持久修订号。
    ///
    /// 返回：删除、缺失或陈旧修订号的封闭结局。
    pub async fn delete_definition(&self, job_name: &str, revision: i64) -> Result<DeleteOutcome> {
        self.ensure_accepting()?;
        if let Some(managed) = &self.managed {
            // 先补偿此前已归零但 Redis 回执失败的能力，避免心跳集合已关闭而能力成员长期残留。
            managed.withdraw_pending().await?;
            if let Some(outcome) = managed.completed_deletion(job_name, revision) {
                return Ok(outcome);
            }
            if !managed.contains_definition(job_name) {
                return Err(crate::job::JobError::Config(format!(
                    "当前 source 未登记本地定义 {job_name}"
                ))
                .into());
            }
        }
        let outcome = self.repository.delete(job_name, revision).await?;
        if matches!(outcome, DeleteOutcome::Ok { .. }) {
            if let Some(managed) = &self.managed {
                // tombstone 先于能力撤销落库，保证新调度已封闭；引用归零后再让 Fanout 快照停止选择本节点。
                managed.definition_deleted(job_name, revision, outcome.clone());
                managed.withdraw_pending().await?;
            }
        }
        Ok(outcome)
    }

    /// 业务作用：显式选择定义摘要解除 CONFLICT，并恢复到给定下一触发时刻。
    ///
    /// 参数说明：`definition` 为管理者选定定义，`next_fire_at` 为恢复后的逻辑时刻。
    ///
    /// 返回：动作生效、缺失、状态不符或摘要陈旧的封闭结局。
    pub async fn resolve_conflict(
        &self,
        definition: &JobDefinition,
        next_fire_at: i64,
    ) -> Result<DefinitionControlOutcome> {
        self.ensure_accepting()?;
        self.repository
            .resolve_conflict(definition, next_fire_at)
            .await
    }
}

/// 一个已绑定 source 的只读查询入口；所有定位字段都由调用方显式提供。
#[derive(Clone)]
pub struct JobQuery {
    repository: JobRepository,
    fanout: FanoutRepository,
    health: Arc<SourceHealth>,
    metrics: Arc<JobMetrics>,
}

impl JobQuery {
    /// 业务作用：组合普通 Run、Fanout 与 source 健康视图。
    ///
    /// 参数说明：所有依赖必须属于同一 source generation。
    ///
    /// 返回：不执行跨 source 猜测的查询门面。
    pub(crate) fn new(
        repository: JobRepository,
        fanout: FanoutRepository,
        health: Arc<SourceHealth>,
        metrics: Arc<JobMetrics>,
    ) -> Self {
        Self {
            repository,
            fanout,
            health,
            metrics,
        }
    }

    /// 业务作用：读取定义的持久核心事实。
    ///
    /// 参数说明：`job_name` 为当前 source 的任务名。
    ///
    /// 返回：定义存在时返回投影，不存在返回 `None`。
    pub async fn definition(&self, job_name: &str) -> Result<Option<JobDefinitionRecord>> {
        self.repository.definition_record(job_name).await
    }

    /// 业务作用：读取一个普通 Run 的只读投影。
    ///
    /// 参数说明：`job_name` 决定分片，`run_id` 定位记录。
    ///
    /// 返回：Run 存在时返回投影，不存在返回 `None`。
    pub async fn run(&self, job_name: &str, run_id: &str) -> Result<Option<JobRun>> {
        self.repository.read_run(job_name, run_id).await
    }

    /// 业务作用：读取 Fanout 根的权威投影。
    ///
    /// 参数说明：`fanout_id` 为批次标识。
    ///
    /// 返回：根存在时返回投影，不存在返回 `None`。
    pub async fn fanout_root(&self, fanout_id: &str) -> Result<Option<FanoutRoot>> {
        self.fanout.read_root(fanout_id).await
    }

    /// 业务作用：读取 Fanout shard 的合同、assignment 与执行状态。
    ///
    /// 参数说明：`fanout_id` 与 `seq` 精确定位 shard。
    ///
    /// 返回：shard 存在时返回投影，不存在返回 `None`。
    pub async fn fanout_shard(&self, fanout_id: &str, seq: i64) -> Result<Option<FanoutShard>> {
        self.fanout.read_shard(fanout_id, seq).await
    }

    /// 业务作用：读取当前 source generation 的准入与关键循环健康状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不会合并或覆盖其它 source 原因的独立快照。
    pub fn health(&self) -> JobSourceHealthSnapshot {
        self.health.snapshot()
    }

    /// 业务作用：读取当前 source 的完整本地指标快照，不触发 Redis I/O。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定指标全集、监督代次与 Completion 裁剪观测。
    pub fn metrics(&self) -> JobMetricsSnapshot {
        self.metrics.snapshot()
    }
}
