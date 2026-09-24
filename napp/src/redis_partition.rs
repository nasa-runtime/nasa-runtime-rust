//! Redis 来源独立的分区计划、启动屏障与聚合排干责任。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult, ComponentId,
    PrepareContext, ShutdownAction, ShutdownContext,
};
use nadis::partition::{
    PartitionShutdownReport, PartitionSnapshot, PreparedPartition, RunningPartition,
};

type Register = Box<dyn FnOnce(&mut PreparedPartition) -> Result<(), nadis::NasaRedisError> + Send>;

struct Plan {
    register: Register,
    critical: bool,
}

struct Source {
    runtime: Arc<RunningPartition>,
    health: ReadinessContributor,
    critical: bool,
}

/// 不包含租约或业务键的单来源观察值；各来源独立采样。
#[derive(Debug, Clone)]
pub struct RedisPartitionObservation {
    pub source: String,
    pub sampled_at: Instant,
    pub partition: PartitionSnapshot,
}

/// 多来源共享期限的停机结果，未收口来源继续由领域 owner 持有。
#[derive(Debug, Clone)]
pub struct RedisPartitionStopResult {
    pub source: String,
    pub report: PartitionShutdownReport,
}

/// 单个 Application 的计划和运行态；与通用 partition 注册表完全独立。
#[derive(Default)]
pub(crate) struct RedisPartitionState {
    plans: Mutex<(bool, BTreeMap<String, Plan>)>,
    sources: Mutex<BTreeMap<String, Source>>,
    reports: Mutex<Vec<RedisPartitionStopResult>>,
}

impl RedisPartitionState {
    /// 业务作用：记录指定来源的唯一 handler 集合，不创建连接或后台任务。
    /// 参数说明：`source` 为 Redis qualifier；`critical` 控制运行健康失败；`register` 冻结路由。
    /// 返回：首次合法登记成功；封口、重复或容量不足时拒绝。
    pub(crate) fn configure<F>(
        &self,
        source: &str,
        critical: bool,
        register: F,
    ) -> ApplicationResult<()>
    where
        F: FnOnce(&mut PreparedPartition) -> Result<(), nadis::NasaRedisError> + Send + 'static,
    {
        let source = if source == "default" {
            "primary"
        } else {
            source
        };
        let mut plans = self.plans.lock().expect("redis partition plans");
        if plans.0
            || plans.1.len() >= 64
            || source.is_empty()
            || source.trim() != source
            || source.len() > 256
            || plans.1.contains_key(source)
        {
            return Err(error(
                ApplicationPhase::UserHook,
                "Redis partition plan is closed, invalid or duplicated",
            ));
        }
        plans.1.insert(
            source.to_owned(),
            Plan {
                register: Box::new(register),
                critical,
            },
        );
        Ok(())
    }

    /// 业务作用：准备已声明来源的独立消费运行态，全部保持消费屏障关闭。
    /// 参数说明：`context` 提供受管 Redis、健康与清理登记。
    /// 返回：路由、来源和执行域全部通过校验；任意失败保留聚合清理责任。
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        context: &mut PrepareContext<'_>,
    ) -> ApplicationResult<()> {
        let app = context.application().clone();
        let mut plans = {
            let mut state = self.plans.lock().expect("redis partition plans");
            state.0 = true;
            std::mem::take(&mut state.1)
        };
        let configs = crate::redis::read_redis_configs(&app)?;
        let enabled = configs
            .into_iter()
            .filter(|(_, config)| config.partition.enabled)
            .collect::<Vec<_>>();
        if enabled.is_empty() && plans.is_empty() {
            return Ok(());
        }
        if app.info().mode() != crate::ApplicationMode::Service {
            return Err(error(
                ApplicationPhase::Prepare,
                "Redis partition consumers require Service mode",
            ));
        }
        // 在首次可取消的领域启动之前登记唯一聚合 owner，部分成功也有相同收口路径。
        context.activate(Box::new(PartitionShutdown(self.clone())));
        for (source, _) in enabled {
            let plan = plans.remove(&source).ok_or_else(|| {
                error(
                    ApplicationPhase::Prepare,
                    "enabled Redis partition source requires a handler plan",
                )
            })?;
            // 健康登记可能失败，必须先完成登记再启动任务，运行态建立后立即交给聚合 owner。
            let health = app.register_readiness(
                ComponentId::Redis,
                Arc::<str>::from(format!("redis-partition:{source}")),
                ReadinessPolicy {
                    affects_ready: plan.critical,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: Some(Duration::from_secs(15)),
                },
            )?;
            let client = crate::redis::redis_handle(&app, &source).await?;
            let lock = Arc::new(nadis::lock::DistributedLock::new(client.clone()));
            let mut prepared = PreparedPartition::prepare(client, lock)
                .await
                .map_err(domain_error)?;
            (plan.register)(&mut prepared).map_err(domain_error)?;
            let runtime = Arc::new(prepared.start_suspended().await.map_err(domain_error)?);
            health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            self.sources
                .lock()
                .expect("redis partition sources")
                .insert(
                    source,
                    Source {
                        runtime,
                        health,
                        critical: plan.critical,
                    },
                );
        }
        if !plans.is_empty() {
            return Err(error(
                ApplicationPhase::Prepare,
                "Redis partition plan references an unknown or disabled source",
            ));
        }
        Ok(())
    }

    /// 业务作用：在全局 Ready 放行后激活各来源，并汇报运行态健康。
    /// 参数说明：无。
    /// 返回：首次调用打开已有域；关键来源异常返回错误，非关键来源只降级自身贡献项。
    pub(crate) fn observe(&self) -> ApplicationResult<()> {
        for source in self
            .sources
            .lock()
            .expect("redis partition sources")
            .values()
        {
            source.runtime.activate();
            let snapshot = source.runtime.snapshot();
            let healthy = snapshot.ready && !snapshot.runner_degraded;
            source.health.observe(
                if healthy {
                    DependencyState::Ready
                } else {
                    DependencyState::Degraded
                },
                if healthy {
                    reason::HEALTHY
                } else {
                    reason::DEGRADED
                },
                Instant::now(),
            );
            if source.critical && !healthy {
                return Err(error(
                    ApplicationPhase::Ready,
                    "critical Redis partition source is unhealthy",
                ));
            }
        }
        Ok(())
    }

    /// 业务作用：取得固定来源的运行句柄，保持底层路由及发布失败语义。
    /// 参数说明：`source` 为受管 Redis qualifier，default 是 primary 的查询别名。
    /// 返回：已准备来源；不存在时不创建新的执行域。
    pub(crate) fn runtime(&self, source: &str) -> ApplicationResult<Arc<RunningPartition>> {
        let source = if source == "default" {
            "primary"
        } else {
            source
        };
        self.sources
            .lock()
            .expect("redis partition sources")
            .get(source)
            .map(|source| source.runtime.clone())
            .ok_or_else(|| {
                error(
                    ApplicationPhase::Ready,
                    "Redis partition source is not prepared",
                )
            })
    }

    /// 业务作用：读取有界来源集合的既有计数，不访问后端或记录业务身份。
    /// 参数说明：无。
    /// 返回：每个来源独立标记采样时间，不承诺跨源原子快照。
    pub(crate) fn observations(&self) -> Vec<RedisPartitionObservation> {
        self.sources
            .lock()
            .expect("redis partition sources")
            .iter()
            .map(|(name, source)| RedisPartitionObservation {
                source: name.clone(),
                sampled_at: Instant::now(),
                partition: source.runtime.snapshot(),
            })
            .collect()
    }

    /// 业务作用：同步关闭全部来源的发布和消费准入。
    /// 参数说明：无。
    /// 返回：只提出一次排干请求，不等待某个慢来源。
    pub(crate) fn begin_shutdown(&self) {
        for source in self
            .sources
            .lock()
            .expect("redis partition sources")
            .values()
        {
            source
                .health
                .observe(DependencyState::NotReady, reason::NOT_READY, Instant::now());
            source.runtime.begin_shutdown();
        }
    }

    /// 业务作用：并发等待全部来源使用同一绝对截止点排干。
    /// 参数说明：`deadline` 为宿主授予聚合动作的剩余期限。
    /// 返回：逐来源保留退出证明；不自动升级为有损停止。
    pub(crate) async fn shutdown(&self, deadline: Instant) -> ApplicationResult<()> {
        self.begin_shutdown();
        let sources = self
            .sources
            .lock()
            .expect("redis partition sources")
            .iter()
            .map(|(name, source)| (name.clone(), source.runtime.clone()))
            .collect::<Vec<_>>();
        let reports = futures_util::future::join_all(sources.into_iter().map(
            |(source, runtime)| async move {
                RedisPartitionStopResult {
                    source,
                    report: runtime.shutdown_until(deadline).await,
                }
            },
        ))
        .await;
        let converged = reports.iter().all(|result| result.report.converged);
        *self.reports.lock().expect("redis partition reports") = reports;
        if converged {
            Ok(())
        } else {
            Err(error(
                ApplicationPhase::Stopping,
                "Redis partition sources have unfinished drain responsibilities",
            ))
        }
    }

    /// 业务作用：保留未排干运行态供宿主移交剩余依赖所有权。
    /// 参数说明：无。
    /// 返回：具有在途工作或尚未停止执行域的来源，不把停止请求当成完成。
    pub(crate) fn unfinished(&self) -> Vec<Arc<RunningPartition>> {
        self.sources
            .lock()
            .expect("redis partition sources")
            .values()
            .filter(|source| !source.runtime.shutdown_report().converged)
            .map(|source| source.runtime.clone())
            .collect()
    }

    /// 业务作用：返回最近一次聚合停机等待的逐源证明。
    /// 参数说明：无。
    /// 返回：有界结果副本；未开始等待时为空。
    pub(crate) fn reports(&self) -> Vec<RedisPartitionStopResult> {
        self.reports
            .lock()
            .expect("redis partition reports")
            .clone()
    }
}

struct PartitionShutdown(Arc<RedisPartitionState>);

impl ShutdownAction for PartitionShutdown {
    /// 业务作用：提供聚合清理动作的固定身份。
    /// 参数说明：无。
    /// 返回：不含来源配置的稳定名称。
    fn label(&self) -> &'static str {
        "redis-partitions"
    }

    /// 业务作用：先关闭所有来源，再在同一期限等待其领域收口证明。
    /// 参数说明：`context` 为宿主共享停机预算。
    /// 返回：全部收口才成功；未完成责任保留在逐来源报告。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.begin_shutdown();
        Box::pin(async move { self.0.shutdown(context.deadline()).await })
    }
}

impl Drop for PartitionShutdown {
    /// 业务作用：外层取消或部分启动失败时同步收回全部消费准入。
    /// 参数说明：无。
    /// 返回：复用已经登记的领域排干操作，不声称异步收口完成。
    fn drop(&mut self) {
        self.0.begin_shutdown();
    }
}

/// 业务作用：归类聚合消费器的阶段性拒绝。
/// 参数说明：`phase` 为生命周期阶段；`message` 为不含凭据的固定说明。
/// 返回：Redis 子能力错误。
fn error(phase: ApplicationPhase, message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Redis, phase, message)
}

/// 业务作用：保留底层准备原因，同时给出稳定的宿主阶段归属。
/// 参数说明：`error` 为领域准备错误。
/// 返回：阻止工作负载激活的 Prepare 错误。
fn domain_error(error: nadis::NasaRedisError) -> ApplicationError {
    ApplicationError::with_source(
        ComponentId::Redis,
        ApplicationPhase::Prepare,
        "Redis partition preparation failed",
        error,
    )
}
