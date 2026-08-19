//! RedisJob 类型状态入口：计划只收集本地定义，prepare 冻结 source 与布局，start 才开放后台消费。

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::api::{JobControl, JobQuery, JobSourceHealthSnapshot, ManagedJobControlState};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::handler::{handler_from_fn, IntoJobHandlerResult, JobContext, JobHandler};
use crate::job::keyspace::JobKeyspace;
use crate::job::model::{JobScheduleType, JobTrigger};
use crate::job::repository::{JobRegisterOutcome, JobRepository};
use crate::job::runtime::{RedisJobRuntime, SourceRunningJobRuntime};
use crate::job::source::{JobSourceId, RedisJobSources};
use crate::job::trigger::next_fire_at;

/// 一个尚未接触 Redis 的本地定义与 Handler 登记。
struct Registration {
    definition: JobDefinition,
    handler: Arc<dyn JobHandler>,
}

/// RedisJob 拥有式计划；prepare 消耗自身，之后不存在继续修改定义集合的入口。
#[derive(Default)]
pub struct RedisJobPlan {
    registrations: Vec<Registration>,
}

impl RedisJobPlan {
    /// 业务作用：创建不绑定连接的空计划，供独立项目或受管组件登记定义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未接触 Redis 的空计划。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：返回计划实际引用的 canonical source 集合，供受管容器在 prepare 前精确解析 Redis 资源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：去重并按 source id 稳定排序的列表；不暴露 Handler 或允许修改计划。
    pub fn source_ids(&self) -> Result<Vec<JobSourceId>> {
        let mut sources = BTreeMap::new();
        for registration in &self.registrations {
            let id = JobSourceId::new(registration.definition.qualifier())?;
            sources.insert(id.clone(), ());
        }
        Ok(sources.into_keys().collect())
    }

    /// 业务作用：登记一个定义与异步 Handler，并在本地拒绝同一 source 的重复任务名。
    ///
    /// 参数说明：
    /// - `definition`: 已冻结定义，包含强路由 qualifier。
    /// - `handler`: 接收拥有式 JobContext 的异步业务函数。
    ///
    /// 返回：登记成功返回更新后的拥有式计划；重复 `(qualifier, name)` 返回错误且不接触 Redis。
    pub fn register<F, Fut, R>(mut self, definition: JobDefinition, handler: F) -> Result<Self>
    where
        F: Fn(JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoJobHandlerResult + Send + 'static,
    {
        self.ensure_unique(&definition)?;
        self.registrations.push(Registration {
            definition,
            handler: handler_from_fn(handler),
        });
        Ok(self)
    }

    /// 业务作用：登记已经实现 `JobHandler` 的共享对象，供底层集成和动态适配器复用同一计划门禁。
    ///
    /// 参数说明：
    /// - `definition`: 已冻结定义。
    /// - `handler`: 共享 Handler trait object。
    ///
    /// 返回：登记成功返回更新后的计划；组内重名返回错误。
    pub fn register_handler(
        mut self,
        definition: JobDefinition,
        handler: Arc<dyn JobHandler>,
    ) -> Result<Self> {
        self.ensure_unique(&definition)?;
        self.registrations.push(Registration {
            definition,
            handler,
        });
        Ok(self)
    }

    /// 业务作用：把另一份尚未接触 Redis 的计划并入当前计划，供静态声明与 UserHook 动态声明统一冻结。
    ///
    /// 参数说明：`other` 为待合并的拥有式计划。
    ///
    /// 返回：全部 `(qualifier, name)` 唯一时返回合并计划；任一冲突时整体拒绝且不接触 Redis。
    pub fn merge(mut self, other: RedisJobPlan) -> Result<Self> {
        for registration in other.registrations {
            self.ensure_unique(&registration.definition)?;
            self.registrations.push(registration);
        }
        Ok(self)
    }

    /// 业务作用：以单个 `primary` RedisClient 准备计划；发现其它 qualifier 时在任何 Redis 写入前拒绝。
    ///
    /// 参数说明：
    /// - `client`: 默认 source 客户端。
    /// - `config`: Job 根级配置。
    ///
    /// 返回：全部布局和定义门禁通过后返回 Prepared 类型；空计划、非 primary 定义或配置冲突返回错误。
    pub async fn prepare(
        self,
        client: Arc<RedisClient>,
        config: JobConfig,
    ) -> Result<PreparedJobRuntime> {
        if self
            .registrations
            .iter()
            .any(|entry| entry.definition.qualifier() != "primary")
        {
            return Err(crate::job::JobError::Config(
                "单 source prepare 只接受 qualifier=primary 的定义".to_owned(),
            )
            .into());
        }
        self.prepare_sources(RedisJobSources::primary(client)?, config)
            .await
    }

    /// 业务作用：按 qualifier 分组并冻结全部 source，逐源确认 layout marker 后登记定义，不开放任务领取。
    ///
    /// 参数说明：
    /// - `sources`: canonical source 到已托管 RedisClient 的冻结映射。
    /// - `config`: 根级默认与逐 source 稀疏覆盖。
    ///
    /// 返回：全部 source 完成真实控制面往返后返回 Prepared 类型；任一未知、禁用、布局或定义门禁失败则不启动后台任务。
    pub async fn prepare_sources(
        self,
        sources: RedisJobSources,
        config: JobConfig,
    ) -> Result<PreparedJobRuntime> {
        if self.registrations.is_empty() {
            return Err(crate::job::JobError::Config("RedisJob plan 不能为空".to_owned()).into());
        }
        let mut grouped: BTreeMap<JobSourceId, Vec<Registration>> = BTreeMap::new();
        for registration in self.registrations {
            grouped
                .entry(JobSourceId::new(registration.definition.qualifier())?)
                .or_default()
                .push(registration);
        }

        let startup_id = uuid::Uuid::new_v4().to_string();
        let mut prepared = BTreeMap::new();
        for (source_id, registrations) in grouped {
            let client = sources.get(&source_id).ok_or_else(|| {
                crate::job::JobError::UnknownSource(source_id.as_str().to_owned())
            })?;
            let source_config =
                Arc::new(config.resolve_source(source_id.as_str(), &client.config().namespace)?);
            let keyspace = JobKeyspace::new(
                source_id.as_str(),
                &source_config.namespace,
                source_config.shard_count,
                source_config.fanout_bucket_count,
            )?;
            if client.qualifier() != keyspace.qualifier() {
                return Err(crate::job::JobError::SourceMismatch(format!(
                    "keyspace={} client={}",
                    keyspace.qualifier(),
                    client.qualifier()
                ))
                .into());
            }
            let repository =
                JobRepository::new(client.clone(), keyspace.clone(), source_config.clone());
            // marker 是加入调度数据面的第一道外部副作用；不一致时必须在定义和执行器写入前停止。
            repository.confirm_layout().await?;
            // 全 master 能力与 ACL 必须在定义 CAS 和 consumer group 之前闭合，不能让部分节点启动后才发现数据面缺命令。
            repository.probe_capabilities().await?;
            let redis_now = redis_now(&client).await?;
            let mut runtime =
                RedisJobRuntime::new(client, keyspace, source_config, startup_id.clone());
            let mut local_definitions = BTreeMap::new();
            for registration in registrations {
                let next_fire = match registration.definition.schedule_type() {
                    JobScheduleType::Manual | JobScheduleType::FanoutOnly => 0,
                    _ => next_fire_at(&registration.definition, redis_now)?,
                };
                match repository
                    .register(&registration.definition, next_fire)
                    .await?
                {
                    JobRegisterOutcome::Registered { .. } | JobRegisterOutcome::Adopted { .. } => {}
                    JobRegisterOutcome::SourceMismatch { current_source } => {
                        return Err(crate::job::JobError::SourceMismatch(format!(
                            "定义 {} 已绑定 source {current_source}",
                            registration.definition.name()
                        ))
                        .into());
                    }
                    outcome => {
                        runtime.record_definition_conflict();
                        return Err(crate::job::JobError::ContractMismatch(format!(
                            "定义 {} 登记被拒绝: {outcome:?}",
                            registration.definition.name()
                        ))
                        .into());
                    }
                }
                local_definitions.insert(
                    registration.definition.name().to_owned(),
                    (registration.definition.trigger() == JobTrigger::FanoutOnly)
                        .then(|| registration.definition.worker_name().to_owned()),
                );
                runtime.register(registration.definition, registration.handler);
            }
            prepared.insert(
                source_id,
                PreparedSourceRuntime {
                    runtime,
                    repository,
                    local_definitions,
                },
            );
        }
        Ok(PreparedJobRuntime { sources: prepared })
    }

    /// 业务作用：复验计划内任务身份，并阻止 FANOUT_ONLY 能力与其它定义共享 Worker 名造成处理器歧义。
    ///
    /// 参数说明：
    /// - `definition`: 待加入的定义。
    ///
    /// 返回：任务名唯一且 Fanout Worker 无歧义时成功；普通定义可按协议共享 Worker Stream 与容量池。
    fn ensure_unique(&self, definition: &JobDefinition) -> Result<()> {
        if self.registrations.iter().any(|entry| {
            entry.definition.qualifier() == definition.qualifier()
                && entry.definition.name() == definition.name()
        }) {
            return Err(crate::job::JobError::Config(format!(
                "RedisJob 定义重复: ({}, {})",
                definition.qualifier(),
                definition.name()
            ))
            .into());
        }
        if self.registrations.iter().any(|entry| {
            entry.definition.qualifier() == definition.qualifier()
                && entry.definition.worker_name() == definition.worker_name()
                && (entry.definition.trigger() == crate::job::model::JobTrigger::FanoutOnly
                    || definition.trigger() == crate::job::model::JobTrigger::FanoutOnly)
        }) {
            return Err(crate::job::JobError::Config(format!(
                "RedisJob FANOUT_ONLY Worker 与其它定义重名: ({}, {})",
                definition.qualifier(),
                definition.worker_name()
            ))
            .into());
        }
        Ok(())
    }
}

/// 已准备的单 source 运行时及其控制面仓库。
struct PreparedSourceRuntime {
    runtime: RedisJobRuntime,
    repository: JobRepository,
    local_definitions: BTreeMap<String, Option<String>>,
}

/// 已完成全部 Redis 门禁、尚未开放后台消费的类型状态。
pub struct PreparedJobRuntime {
    sources: BTreeMap<JobSourceId, PreparedSourceRuntime>,
}

impl PreparedJobRuntime {
    /// 业务作用：按稳定 source 顺序登记能力并启动后台运行时；任一失败会收口此前已启动 source。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部 source 启动成功后返回唯一运行句柄；部分启动失败时先回滚已启动项再返回原错误。
    pub async fn start(self) -> Result<RunningJobRuntime> {
        let mut running = BTreeMap::new();
        for (source_id, prepared) in self.sources {
            let repository = prepared.repository;
            let local_definitions = prepared.local_definitions;
            match prepared.runtime.start().await {
                Ok(runtime) => {
                    let fanout = runtime.fanout_repository();
                    let health = runtime.health();
                    let metrics = health.metrics();
                    let managed =
                        ManagedJobControlState::new(runtime.registry(), local_definitions);
                    let control = JobControl::new_with_health(
                        repository.clone(),
                        fanout.clone(),
                        health.clone(),
                        managed,
                    );
                    let query = JobQuery::new(repository.clone(), fanout, health, metrics);
                    running.insert(
                        source_id,
                        RunningSourceRuntime {
                            runtime,
                            control,
                            query,
                        },
                    );
                }
                Err(error) => {
                    for source in running.values() {
                        source.runtime.stop_accepting();
                    }
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                    while let Some((_, source)) = running.pop_last() {
                        let _ = source.runtime.shutdown(deadline).await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(RunningJobRuntime { sources: running })
    }
}

/// 一个运行中 source 的后台句柄与只读控制面引用。
struct RunningSourceRuntime {
    runtime: SourceRunningJobRuntime,
    control: JobControl,
    query: JobQuery,
}

/// 多 source 运行时的唯一所有者；Drop 只关闭准入，显式 shutdown 才等待与注销。
pub struct RunningJobRuntime {
    sources: BTreeMap<JobSourceId, RunningSourceRuntime>,
}

/// 多 source RedisJob 的可克隆业务门面；只持有控制与查询句柄，不拥有后台任务或停机权。
#[derive(Clone)]
pub struct JobRuntimeHandle {
    sources: BTreeMap<JobSourceId, JobRuntimeSourceHandle>,
}

/// 单 source 的业务门面投影；字段不公开，调用方必须显式按 qualifier 选择。
#[derive(Clone)]
struct JobRuntimeSourceHandle {
    executor_id: String,
    control: JobControl,
    query: JobQuery,
}

impl JobRuntimeHandle {
    /// 业务作用：取得指定 source 的结构化控制 API，不按唯一成员或默认 source 回退。
    ///
    /// 参数说明：`qualifier` 为 canonical source id。
    ///
    /// 返回：source 存在且仍接受新工作时返回可克隆控制门面；未知或停止准入时返回结构化错误。
    pub fn control(&self, qualifier: &str) -> Result<JobControl> {
        let source = self.source(qualifier)?;
        let health = source.query.health();
        if !health.accepting {
            return Err(crate::job::JobError::ExecutionStopped(format!(
                "source {} 已关闭控制准入",
                health.qualifier
            ))
            .into());
        }
        Ok(source.control.clone())
    }

    /// 业务作用：取得指定 source 的只读查询 API，使同名任务不会跨数据源猜测。
    ///
    /// 参数说明：`qualifier` 为 canonical source id。
    ///
    /// 返回：source 存在时返回可克隆查询门面；未知 source 返回结构化错误。
    pub fn query(&self, qualifier: &str) -> Result<JobQuery> {
        Ok(self.source(qualifier)?.query.clone())
    }

    /// 业务作用：读取指定 source 的执行器身份，供控制面审计关联当前运行代次。
    ///
    /// 参数说明：`qualifier` 为 canonical source id。
    ///
    /// 返回：source 存在时返回稳定执行器身份副本；未知 source 返回结构化错误。
    pub fn executor_id(&self, qualifier: &str) -> Result<String> {
        Ok(self.source(qualifier)?.executor_id.clone())
    }

    /// 业务作用：读取指定 source 的独立健康快照，不用其它 source 的后续成功覆盖当前事实。
    ///
    /// 参数说明：`qualifier` 为 canonical source id。
    ///
    /// 返回：source 存在时返回准入、状态和封闭原因；未知 source 返回结构化错误。
    pub fn source_health(&self, qualifier: &str) -> Result<JobSourceHealthSnapshot> {
        Ok(self.source(qualifier)?.query.health())
    }

    /// 业务作用：定位一个已冻结 source 门面，不实施别名、唯一成员或默认值推断。
    ///
    /// 参数说明：`qualifier` 为调用方显式选择的 source id。
    ///
    /// 返回：存在时返回内部投影；未知 source 返回 `JobError::UnknownSource`。
    fn source(&self, qualifier: &str) -> Result<&JobRuntimeSourceHandle> {
        let id = JobSourceId::new(qualifier)?;
        self.sources
            .get(&id)
            .ok_or_else(|| crate::job::JobError::UnknownSource(id.as_str().to_owned()).into())
    }
}

impl RunningJobRuntime {
    /// 业务作用：生成不拥有停机权的可克隆业务门面，供受管容器在 Ready 后原子发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：包含全部冻结 source 控制、查询和执行器身份的只读路由表。
    pub fn handle(&self) -> JobRuntimeHandle {
        JobRuntimeHandle {
            sources: self
                .sources
                .iter()
                .map(|(id, source)| {
                    (
                        id.clone(),
                        JobRuntimeSourceHandle {
                            executor_id: source.runtime.executor_id().to_owned(),
                            control: source.control.clone(),
                            query: source.query.clone(),
                        },
                    )
                })
                .collect(),
        }
    }
    /// 业务作用：克隆全部逐 source 查询门面，供受管生命周期持续观测关键后台循环。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 canonical source 顺序排列的查询门面；克隆不转移 shutdown 所有权。
    pub fn health_queries(&self) -> Vec<JobQuery> {
        self.sources
            .values()
            .map(|source| source.query.clone())
            .collect()
    }

    /// 业务作用：返回指定 source 的本进程执行器身份，供低基数观测与控制面关联。
    ///
    /// 参数说明：
    /// - `qualifier`: canonical source id。
    ///
    /// 返回：source 存在时返回 executor id；未知 source 返回 `JobError::UnknownSource`。
    pub fn executor_id(&self, qualifier: &str) -> Result<&str> {
        let id = JobSourceId::new(qualifier)?;
        self.sources
            .get(&id)
            .map(|source| source.runtime.executor_id())
            .ok_or_else(|| crate::job::JobError::UnknownSource(id.as_str().to_owned()).into())
    }

    /// 业务作用：取得指定 source 的控制面仓库，不按唯一成员或默认 source 回退。
    ///
    /// 参数说明：
    /// - `qualifier`: canonical source id。
    ///
    /// 返回：source 存在时返回结构化控制门面；未知 source 返回 `JobError::UnknownSource`。
    pub fn control(&self, qualifier: &str) -> Result<&JobControl> {
        let id = JobSourceId::new(qualifier)?;
        self.sources
            .get(&id)
            .map(|source| &source.control)
            .ok_or_else(|| crate::job::JobError::UnknownSource(id.as_str().to_owned()).into())
    }

    /// 业务作用：取得指定 source 的查询仓库，使同名任务始终在调用方指定的数据源中查询。
    ///
    /// 参数说明：
    /// - `qualifier`: canonical source id。
    ///
    /// 返回：source 存在时返回结构化查询门面；未知 source 返回 `JobError::UnknownSource`。
    pub fn query(&self, qualifier: &str) -> Result<&JobQuery> {
        let id = JobSourceId::new(qualifier)?;
        self.sources
            .get(&id)
            .map(|source| &source.query)
            .ok_or_else(|| crate::job::JobError::UnknownSource(id.as_str().to_owned()).into())
    }

    /// 业务作用：读取指定 source 的独立健康快照，不用其它 source 的最后观测覆盖其失败原因。
    ///
    /// 参数说明：`qualifier` 为 canonical source id。
    ///
    /// 返回：source 存在时返回准入与关键循环状态；未知 source 返回配置错误。
    pub fn source_health(&self, qualifier: &str) -> Result<JobSourceHealthSnapshot> {
        Ok(self.query(qualifier)?.health())
    }

    /// 业务作用：显式排空并关闭一个独立 Redis source，同时保持其它 source 的领取、续租和控制准入。
    ///
    /// 参数说明：
    /// - `qualifier`: 待关闭的 canonical source id。
    /// - `budget`: 该 source 发布 Draining、等待 Handler、注销执行器和关闭连接可使用的总预算。
    ///
    /// 返回：目标 source 完整收口时成功并从运行表移除；未知 source、预算耗尽或注销失败时返回结构化错误，
    /// 已移除 source 不会重新开放，其它 source 仍由当前 `RunningJobRuntime` 持有。
    pub async fn shutdown_source(&mut self, qualifier: &str, budget: Duration) -> Result<()> {
        let id = JobSourceId::new(qualifier)?;
        let source = self
            .sources
            .remove(&id)
            .ok_or_else(|| crate::job::JobError::UnknownSource(id.as_str().to_owned()))?;

        // 先永久关闭本 source 的业务准入，确保此前克隆的控制句柄也不能越过 drain 继续写控制面。
        source.runtime.stop_accepting();
        let deadline = tokio::time::Instant::now() + budget;
        let mut first_error =
            match tokio::time::timeout_at(deadline, source.runtime.publish_draining()).await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(_) => Some(
                    crate::job::JobError::ShutdownDeadline(format!(
                        "RedisJob source {} 未在截止前发布 Draining",
                        id.as_str()
                    ))
                    .into(),
                ),
            };

        // 即使 Draining 发布失败也继续执行本地收口，避免一个 source 的外部故障拖住其它 source 的所有权。
        if let Err(error) = source.runtime.shutdown(deadline).await {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// 业务作用：同时关闭全部 source 准入，再按反向稳定顺序在共享预算内等待、注销并继续收口其它 source。
    ///
    /// 参数说明：
    /// - `budget`: 全量 Job 停机可使用的最大时长。
    ///
    /// 返回：全部 source 完成时成功；任一超时或注销失败时在其余 source 收口后返回第一个错误。
    pub async fn shutdown(mut self, budget: Duration) -> Result<()> {
        for source in self.sources.values() {
            source.runtime.stop_accepting();
        }
        let deadline = tokio::time::Instant::now() + budget;
        let mut first_error = None;
        // 所有 source 先并发发布 Draining，避免按顺序等待前一 source 时后续 source 仍进入新 Fanout 快照。
        let publish = futures::future::join_all(
            self.sources
                .values()
                .map(|source| source.runtime.publish_draining()),
        );
        match tokio::time::timeout_at(deadline, publish).await {
            Ok(results) => {
                for result in results {
                    if let Err(error) = result {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            Err(_) => {
                first_error = Some(
                    crate::job::JobError::ShutdownDeadline(
                        "RedisJob source 未在共享截止前全部发布 Draining".to_owned(),
                    )
                    .into(),
                );
            }
        }
        while let Some((_, source)) = self.sources.pop_last() {
            if let Err(error) = source.runtime.shutdown(deadline).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for RunningJobRuntime {
    /// 业务作用：未显式 shutdown 时只关闭全部 source 的新领取，不伪报 Handler 已完成或 executor 已注销。
    fn drop(&mut self) {
        for source in self.sources.values() {
            source.runtime.stop_accepting();
        }
    }
}

/// 业务作用：读取 Redis 权威当前毫秒，作为该 source 首个逻辑触发时刻的统一基准。
///
/// 参数说明：
/// - `client`: 当前 source 客户端。
///
/// 返回：权威毫秒时刻；连接或解析失败返回错误。
async fn redis_now(client: &RedisClient) -> Result<i64> {
    let mut connection = client.conn();
    let (seconds, micros): (i64, i64) = redis::cmd("TIME")
        .query_async(&mut connection)
        .await
        .map_err(NasaRedisError::Redis)?;
    Ok(seconds * 1_000 + micros / 1_000)
}
