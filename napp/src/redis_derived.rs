//! 普通 Stream、共享组 Proxy 和出站微批的命名装配、激活与关闭责任。

use crate::managed_adapters::ManagedAdapter;
use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationError, ApplicationFuture, ApplicationMode, ApplicationPhase,
    ApplicationResult, ComponentId, PrepareContext, ShutdownAction, ShutdownContext,
};
use nadis::{
    pipeline::{AutoPipeline, AutoPipelineState, MicroBatchCfg},
    proxy::{
        PreparedProxy, ProxyCfg, ProxyCleanup, ProxyStartOffset, ProxyStopReport, RunningProxy,
    },
    stream::{StreamSubscribeCfg, StreamSubscriber, StreamSubscription, StreamTaskState},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub(crate) type StreamSetup = Box<dyn FnOnce(StreamSubscriber) -> StreamSubscriber + Send>;
pub(crate) type ProxySetup = Box<dyn FnOnce(&mut PreparedProxy) + Send>;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamPlan {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
    stream: Option<String>,
    #[serde(default)]
    config: StreamSubscribeCfg,
    #[serde(default)]
    critical: bool,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyPlan {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
    stream: Option<String>,
    group: Option<String>,
    #[serde(default)]
    config: ProxyCfg,
    #[serde(default)]
    start_offset: ProxyStartOffset,
    #[serde(default)]
    critical: bool,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PipelinePlan {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
    window_ms: Option<u64>,
    max_batch: Option<usize>,
    queue_capacity: Option<usize>,
    max_command_bytes: Option<usize>,
    max_batch_bytes: Option<usize>,
    #[serde(default)]
    critical: bool,
}

#[derive(Default)]
struct Registrations {
    sealed: bool,
    streams: BTreeMap<String, StreamSetup>,
    proxies: BTreeMap<String, ProxySetup>,
}

#[derive(Clone)]
enum Runtime {
    Stream(Arc<StreamSubscription>),
    Proxy(Arc<RunningProxy>, Arc<ManagedAdapter<()>>),
    Pipeline(Arc<AutoPipeline>, Arc<ManagedAdapter<()>>),
}
struct Entry {
    name: String,
    runtime: Runtime,
    health: Option<ReadinessContributor>,
    critical: bool,
}

/// 命名 Redis 派生能力的有界只读观测。
#[derive(Debug, Clone)]
pub struct RedisDerivedObservation {
    /// 冻结的计划名称。
    pub name: String,
    /// 固定能力类别。
    pub kind: &'static str,
    /// 任务仍由本代 owner 持有并运行。
    pub running: bool,
    /// 已关闭新业务准入。
    pub closing: bool,
    /// 微批队列占用；消费计划为零。
    pub queued: usize,
    /// 任务存活、参数字节、最近进展与本地未完成责任，不表示远端 PEL 或逐消息成功。
    pub activity: nadis::RedisTaskObservation,
    /// Proxy 的退出和清理事实；其余能力为 None，terminated 为 false 时尚未完成关闭。
    pub proxy_stop: Option<ProxyStopReport>,
}

#[derive(Default)]
pub(crate) struct RedisDerived {
    registrations: Mutex<Registrations>,
    entries: Mutex<Vec<Entry>>,
    ready: CancellationToken,
    closed: CancellationToken,
}

/// 受宿主准入保护的自动微批入口；业务不拥有最终关闭权。
#[derive(Clone)]
pub struct ManagedRedisPipeline {
    pipeline: Arc<AutoPipeline>,
    calls: Arc<ManagedAdapter<()>>,
    ready: CancellationToken,
}
impl ManagedRedisPipeline {
    /// 业务作用：在当前宿主代次内提交并等待单命令回执。
    /// 参数说明：`command` 为领域准入允许的命令。
    /// 返回：确定响应或领域错误；关闭后明确未发送，不自动重放。
    pub async fn execute<T: nadis::FromRedisValue>(
        &self,
        command: nadis::RedisCommand,
    ) -> nadis::Result<T> {
        let _call = self
            .calls
            .enter()
            .await
            .map_err(|_| nadis::NasaRedisError::NotExecuted("受管微批已关闭".into()))?;
        if !self.ready.is_cancelled() {
            return Err(nadis::NasaRedisError::NotExecuted(
                "受管微批尚未激活".into(),
            ));
        }
        self.pipeline.execute(command).await
    }
    /// 业务作用：接纳命令后返回，由 flusher 承担后续发送责任。
    /// 参数说明：`command` 为待入队命令。
    /// 返回：成功仅表示入队，调用方放弃逐命令回执；关闭或超限时拒绝。
    pub async fn submit(&self, command: nadis::RedisCommand) -> nadis::Result<()> {
        let _call = self
            .calls
            .enter()
            .await
            .map_err(|_| nadis::NasaRedisError::NotExecuted("受管微批已关闭".into()))?;
        if !self.ready.is_cancelled() {
            return Err(nadis::NasaRedisError::NotExecuted(
                "受管微批尚未激活".into(),
            ));
        }
        self.pipeline.submit(command).await
    }
}

/// 共享组 Proxy 的出站发布入口，保持领域信封和未知结果语义。
#[derive(Clone)]
pub struct ManagedRedisProxy {
    proxy: Arc<RunningProxy>,
    calls: Arc<ManagedAdapter<()>>,
    ready: CancellationToken,
}
impl ManagedRedisProxy {
    /// 业务作用：通过受管共享组发布领域信封并登记在途责任。
    /// 参数说明：`topic` 与 `event` 选择路由；`data` 为信封正文。
    /// 返回：XADD entry ID，不代表消费成功；关闭后明确拒绝。
    pub async fn publish<T: serde::Serialize>(
        &self,
        topic: &str,
        event: &str,
        data: &T,
    ) -> nadis::Result<String> {
        let _call = self
            .calls
            .enter()
            .await
            .map_err(|_| nadis::NasaRedisError::NotExecuted("受管 Proxy 已关闭".into()))?;
        if !self.ready.is_cancelled() {
            return Err(nadis::NasaRedisError::NotExecuted(
                "受管 Proxy 尚未激活".into(),
            ));
        }
        self.proxy.publish(topic, event, data).await
    }
}

impl RedisDerived {
    /// 业务作用：保留固定关键目录的健康句柄，供取得所有领域保护后检查新鲜度。
    /// 参数说明：无。
    /// 返回：准备阶段封存的关键证据集合，不包含可选依赖。
    pub(crate) fn critical_health(&self) -> Vec<ReadinessContributor> {
        self.entries
            .lock()
            .expect("Redis derived entries")
            .iter()
            .filter(|entry| entry.critical)
            .filter_map(|entry| entry.health.clone())
            .collect()
    }
    /// 业务作用：在关键消费及微批 owner 的任务责任仍完整时统一发布接流许可。
    /// 参数说明：`publish` 为后续领域裁决和宿主状态提交，不得执行业务或阻塞操作。
    /// 返回：已退出、关闭或证据过期时拒绝启动，保护范围延续至发布完成。
    pub(crate) fn publish_ready(
        &self,
        publish: &mut dyn FnMut() -> ApplicationResult<()>,
    ) -> ApplicationResult<()> {
        let entries = self.entries.lock().expect("Redis derived entries");
        // 收口后不能通过尚存的领域句柄再次开放共享许可。
        if self.closed.is_cancelled() {
            return Err(ApplicationError::new(
                ComponentId::Redis,
                ApplicationPhase::Ready,
                "Redis derived owner already closing",
            ));
        }
        Self::guard_entries(&entries, &mut || {
            if entries.iter().any(|entry| {
                entry.critical
                    && entry
                        .health
                        .as_ref()
                        .is_some_and(|health| !health.ready_now())
            }) {
                return Err(ApplicationError::new(
                    ComponentId::Redis,
                    ApplicationPhase::Ready,
                    "Redis derived readiness evidence expired",
                ));
            }
            publish()
        })
    }

    /// 业务作用：按封存目录嵌套任务存活保护，避免逐项复验后再无保护发布。
    /// 参数说明：`entries` 为固定上限的计划；`publish` 为最终发布操作。
    /// 返回：任何关键 owner 不再完整时拒绝后续执行，可选 owner 只贡献降级状态。
    fn guard_entries(
        entries: &[Entry],
        publish: &mut dyn FnMut() -> ApplicationResult<()>,
    ) -> ApplicationResult<()> {
        let Some((entry, rest)) = entries.split_first() else {
            return publish();
        };
        if entry.critical {
            let mut next = || {
                if let Some(health) = &entry.health {
                    health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
                }
                Self::guard_entries(rest, publish)
            };
            // 任务退出和发布共享同一责任锁，已归还的任务不能沿用 Prepare 时的 Ready 事实。
            entry.runtime.with_running(&mut next).unwrap_or_else(|| {
                if let Some(health) = &entry.health {
                    health.observe(DependencyState::NotReady, reason::DEGRADED, Instant::now());
                }
                Err(ApplicationError::new(
                    ComponentId::Redis,
                    ApplicationPhase::Ready,
                    "critical Redis derived owner exited before Ready",
                ))
            })
        } else {
            if let Some(health) = &entry.health {
                let healthy = entry.runtime.running();
                health.observe(
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
            }
            Self::guard_entries(rest, publish)
        }
    }
    /// 业务作用：将本应用的 Redis 派生入口绑定到统一启动许可。
    /// 参数说明：`ready` 为 Service 的公共许可或 Batch 的独立出站许可。
    /// 返回：尚未登记计划或创建任务的领域 owner。
    pub(crate) fn new(ready: CancellationToken) -> Self {
        Self {
            ready,
            ..Self::default()
        }
    }
    /// 业务作用：登记普通 Stream 的有界事件目录，连接由 Prepare 创建。
    /// 参数说明：`name` 匹配命名配置；`setup` 只登记事件处理策略。
    /// 返回：名称有效且未封口时成功，重复或超量拒绝。
    pub(crate) fn register_stream(&self, name: &str, setup: StreamSetup) -> ApplicationResult<()> {
        let mut plans = self.registrations.lock().expect("Redis derived plans");
        if plans.sealed
            || !valid_name(name)
            || plans.streams.len() >= 64
            || plans.streams.contains_key(name)
        {
            return Err(error("invalid or duplicate Stream registration"));
        }
        plans.streams.insert(name.into(), setup);
        Ok(())
    }
    /// 业务作用：登记 Proxy 的主题与事件策略，不能借登记启动消费。
    /// 参数说明：`name` 匹配命名配置；`setup` 登记类型化 handler。
    /// 返回：有效唯一登记成功；封口后拒绝。
    pub(crate) fn register_proxy(&self, name: &str, setup: ProxySetup) -> ApplicationResult<()> {
        let mut plans = self.registrations.lock().expect("Redis derived plans");
        if plans.sealed
            || !valid_name(name)
            || plans.proxies.len() >= 64
            || plans.proxies.contains_key(name)
        {
            return Err(error("invalid or duplicate Proxy registration"));
        }
        plans.proxies.insert(name.into(), setup);
        Ok(())
    }
    /// 业务作用：根据已冻结配置准备命名资源，先登记清理责任再创建后台任务。
    /// 参数说明：`context` 提供受管 Redis 来源、资源表及回滚栈。
    /// 返回：全部计划可激活后成功；任一计划失败均阻止业务放行。
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        context: &mut PrepareContext<'_>,
    ) -> ApplicationResult<()> {
        let app = context.application().clone();
        let tree = app.config();
        let streams: BTreeMap<String, StreamPlan> = plans(tree.value(), "redis_streams")?;
        let proxies: BTreeMap<String, ProxyPlan> = plans(tree.value(), "redis_proxies")?;
        let pipelines: BTreeMap<String, PipelinePlan> = plans(tree.value(), "redis_pipelines")?;
        if streams.len() + proxies.len() + pipelines.len() > 64 {
            return Err(error("Redis derived plan count exceeds 64"));
        }
        let (mut stream_handlers, mut proxy_handlers) = {
            let mut plans = self.registrations.lock().expect("Redis derived plans");
            plans.sealed = true;
            (
                std::mem::take(&mut plans.streams),
                std::mem::take(&mut plans.proxies),
            )
        };
        let batch = app.info().mode() == ApplicationMode::Batch;
        if batch && (streams.values().any(|p| p.enabled) || proxies.values().any(|p| p.enabled)) {
            return Err(error("Stream and Proxy consumption require Service mode"));
        }
        let mut identities = BTreeSet::new();
        for (name, plan) in streams.iter().filter(|(_, p)| p.enabled) {
            validate_name(name)?;
            required(&plan.stream)?;
            if !stream_handlers.contains_key(name) || plan.config.handler_timeout_ms == 0 {
                return Err(error(
                    "Stream requires a handler and finite handler timeout",
                ));
            }
            if let nadis::stream::StreamMode::Group {
                group, consumer, ..
            } = &plan.config.mode
            {
                if !identities.insert((
                    crate::redis::canonical_qualifier(
                        plan.redis_ref.as_deref().unwrap_or("default"),
                    ),
                    plan.stream.clone(),
                    group.clone(),
                    consumer.clone(),
                )) {
                    return Err(error("duplicate Stream consumer identity"));
                }
            }
        }
        for (name, plan) in proxies.iter().filter(|(_, p)| p.enabled) {
            validate_name(name)?;
            required(&plan.stream)?;
            required(&plan.group)?;
            if !proxy_handlers.contains_key(name) {
                return Err(error("Proxy requires registered handlers"));
            }
            if identities.iter().any(|(source, stream, group, _)| {
                *source
                    == crate::redis::canonical_qualifier(
                        plan.redis_ref.as_deref().unwrap_or("default"),
                    )
                    && stream == &plan.stream
                    && Some(group) == plan.group.as_ref()
            }) {
                return Err(error("incompatible Stream and Proxy group protocols"));
            }
        }
        let mut bytes = 0usize;
        for (name, plan) in pipelines.iter().filter(|(_, p)| p.enabled) {
            validate_name(name)?;
            let cfg = pipeline_config(plan)?;
            bytes = bytes
                .checked_add(
                    cfg.queue_capacity
                        .checked_mul(cfg.max_command_bytes)
                        .ok_or_else(|| error("Redis pipeline capacity overflow"))?,
                )
                .ok_or_else(|| error("Redis pipeline capacity overflow"))?;
        }
        if bytes > 256 * 1024 * 1024 {
            return Err(error(
                "Redis pipeline queue parameter budget exceeds 256 MiB",
            ));
        }
        // 回滚入口必须先于任何后台任务出现，准备 future 被取消后仍会关闭已登记 owner。
        context.activate(Box::new(DerivedShutdown(self.clone())));
        if batch {
            self.ready.cancel();
        }
        for (name, plan) in streams {
            if !plan.enabled {
                continue;
            }
            let client =
                crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default"))
                    .await?;
            let setup = stream_handlers
                .remove(&name)
                .expect("validated Stream registration");
            let subscriber = setup(client.subscribe(required(&plan.stream)?)).with_cfg(plan.config);
            if !(1..=256).contains(&subscriber.handler_count()) {
                return Err(error("Stream event directory must contain 1..256 handlers"));
            }
            let subscription = subscriber
                .start_suspended_with_activation(self.ready.clone())
                .await
                .map_err(|_| error("Stream preparation failed"))?;
            let runtime = Runtime::Stream(Arc::new(subscription));
            self.add(&app, name, runtime, plan.critical, batch)?;
        }
        for (name, plan) in proxies {
            if !plan.enabled {
                continue;
            }
            let client =
                crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default"))
                    .await?;
            let mut prepared = PreparedProxy::prepare_with_offset(
                client,
                required(&plan.stream)?,
                required(&plan.group)?,
                plan.config,
                plan.start_offset,
            )
            .await
            .map_err(|_| error("Proxy preparation failed"))?;
            proxy_handlers
                .remove(&name)
                .expect("validated Proxy registration")(&mut prepared);
            if !(1..=256).contains(&prepared.handler_count()) {
                return Err(error("Proxy route directory must contain 1..256 handlers"));
            }
            let proxy = Arc::new(
                prepared
                    .start_suspended_with_activation(self.ready.clone())
                    .await
                    .map_err(|_| error("Proxy task preparation failed"))?,
            );
            let calls = ManagedAdapter::new(Arc::new(()));
            self.add(
                &app,
                name.clone(),
                Runtime::Proxy(proxy.clone(), calls.clone()),
                plan.critical,
                batch,
            )?;
            context.register_resource(
                Some(&name),
                ManagedRedisProxy {
                    proxy,
                    calls,
                    ready: self.ready.clone(),
                },
            )?;
        }
        for (name, plan) in pipelines {
            if !plan.enabled {
                continue;
            }
            let client =
                crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default"))
                    .await?;
            let pipeline = AutoPipeline::start(client, pipeline_config(&plan)?);
            let calls = ManagedAdapter::new(Arc::new(()));
            self.add(
                &app,
                name.clone(),
                Runtime::Pipeline(pipeline.clone(), calls.clone()),
                plan.critical,
                batch,
            )?;
            context.register_resource(
                Some(&name),
                ManagedRedisPipeline {
                    pipeline,
                    calls,
                    ready: self.ready.clone(),
                },
            )?;
        }
        if !stream_handlers.is_empty() || !proxy_handlers.is_empty() {
            return Err(error(
                "Redis handler references an unknown or disabled plan",
            ));
        }
        Ok(())
    }
    /// 业务作用：纳入聚合关闭责任后登记就绪观测，避免观测失败遗留无人管理任务。
    /// 参数说明：`app` 提供健康注册；`name`、`runtime`、`critical` 为固定计划；`batch` 区分工作负载模式。
    /// 返回：任务已归属且 Service 健康已登记，错误由准备回滚处理。
    fn add(
        &self,
        app: &Application,
        name: String,
        runtime: Runtime,
        critical: bool,
        batch: bool,
    ) -> ApplicationResult<()> {
        let mut entries = self.entries.lock().expect("Redis derived entries");
        entries.push(Entry {
            name: name.clone(),
            runtime,
            health: None,
            critical,
        });
        if !batch {
            let health = app.register_readiness(
                ComponentId::Redis,
                Arc::<str>::from(format!(
                    "redis-derived:{name}:{}",
                    entries.last().unwrap().runtime.kind()
                )),
                ReadinessPolicy {
                    affects_ready: critical,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: Some(Duration::from_secs(15)),
                },
            )?;
            // 这里只证明任务已准备；消费放行不依赖先消费成功，避免 Ready 环。
            health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            entries.last_mut().unwrap().health = Some(health);
        }
        Ok(())
    }
    /// 业务作用：宿主 Ready 后持续裁决后台意外退出并更新健康事实。
    /// 参数说明：无。
    /// 返回：关键任务失效时要求监督器停止应用；关闭后不重新激活。
    pub(crate) fn observe(&self) -> ApplicationResult<()> {
        if self.closed.is_cancelled() {
            return Ok(());
        }
        for entry in self.entries.lock().expect("Redis derived entries").iter() {
            let healthy = entry.runtime.running();
            if let Some(health) = &entry.health {
                health.observe(
                    if healthy {
                        DependencyState::Ready
                    } else if entry.critical {
                        DependencyState::NotReady
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
            }
            if !healthy && entry.critical {
                return Err(ApplicationError::new(
                    ComponentId::Redis,
                    ApplicationPhase::Running,
                    "critical Redis derived task exited",
                ));
            }
        }
        Ok(())
    }
    /// 业务作用：生成冻结目录范围内的只读状态，观察本身不创建连接或业务请求。
    /// 参数说明：无。
    /// 返回：至多 64 个命名计划的当前本地状态。
    pub(crate) fn observations(&self) -> Vec<RedisDerivedObservation> {
        self.entries
            .lock()
            .expect("Redis derived entries")
            .iter()
            .map(|entry| RedisDerivedObservation {
                name: entry.name.clone(),
                kind: entry.runtime.kind(),
                running: entry.runtime.running(),
                closing: self.closed.is_cancelled(),
                queued: match &entry.runtime {
                    Runtime::Pipeline(p, _) => p.queued(),
                    _ => 0,
                },
                activity: match &entry.runtime {
                    Runtime::Pipeline(p, _) => p.observation(),
                    Runtime::Stream(s) => s.observation(),
                    Runtime::Proxy(p, _) => p.observation(),
                },
                proxy_stop: match &entry.runtime {
                    Runtime::Proxy(p, _) => Some(p.stop_report()),
                    _ => None,
                },
            })
            .collect()
    }
    /// 业务作用：先关闭全部计划的准入，再由聚合等待并发排干。
    /// 参数说明：`deadline` 为共用宿主截止点。
    /// 返回：所有旧句柄永久关闭，领域 owner 保留剩余责任。
    fn close(&self, deadline: tokio::time::Instant) {
        self.closed.cancel();
        for entry in self.entries.lock().expect("Redis derived entries").iter() {
            match &entry.runtime {
                Runtime::Stream(s) => s.begin_shutdown(),
                Runtime::Proxy(p, c) => {
                    c.close();
                    p.begin_shutdown_until(deadline);
                }
                Runtime::Pipeline(p, c) => {
                    c.close();
                    p.begin_shutdown();
                }
            }
        }
    }
    /// 业务作用：同时等待各领域真实退出及已登记的出站调用归还资源。
    /// 参数说明：无。
    /// 返回：所有计划正常退出时成功，强制或异常结果明确拒绝成功结论。
    async fn drain(&self) -> ApplicationResult<()> {
        let runtimes: Vec<_> = self
            .entries
            .lock()
            .expect("Redis derived entries")
            .iter()
            .map(|e| e.runtime.clone())
            .collect();
        let results = futures_util::future::join_all(runtimes.iter().map(|runtime| async move {
            match runtime {
                Runtime::Stream(s) => s.wait_closed().await == StreamTaskState::Stopped,
                Runtime::Proxy(p, c) => {
                    let r = p.wait_closed().await;
                    c.drain().await;
                    // 任务退出不能替代清理结局；证据不足或请求超时必须保留为次要停机失败。
                    r.terminated
                        && !r.forced
                        && !r.failed
                        && matches!(r.cleanup, ProxyCleanup::Complete | ProxyCleanup::Pending)
                }
                Runtime::Pipeline(p, c) => {
                    let r = p.shutdown_result().await;
                    c.drain().await;
                    r.is_ok()
                }
            }
        }))
        .await;
        if results.into_iter().all(|ok| ok) {
            Ok(())
        } else {
            Err(ApplicationError::new(
                ComponentId::Redis,
                ApplicationPhase::Stopping,
                "Redis derived shutdown was not graceful",
            ))
        }
    }
    /// 业务作用：宿主预算耗尽时请求终止可中止任务，实际 join 由领域 owner 保留。
    /// 参数说明：无。
    /// 返回：已发出终止请求，不能据此报告排干完成。
    fn abort(&self) {
        for entry in self.entries.lock().expect("Redis derived entries").iter() {
            match &entry.runtime {
                Runtime::Stream(s) => s.abort(),
                Runtime::Pipeline(p, _) => p.abort(),
                Runtime::Proxy(_, _) => {}
            }
        }
    }
}
impl Runtime {
    /// 业务作用：以领域自身的任务责任保护最终接流裁决。
    /// 参数说明：`publish` 为短小同步的宿主发布操作，不得重入领域句柄。
    /// 返回：任务责任完整时返回发布结果，否则不执行。
    fn with_running(
        &self,
        publish: &mut dyn FnMut() -> ApplicationResult<()>,
    ) -> Option<ApplicationResult<()>> {
        match self {
            Self::Stream(s) => s.with_running(publish),
            Self::Proxy(p, _) => p.with_running(publish),
            Self::Pipeline(p, _) => p.with_running(publish),
        }
    }
    /// 业务作用：提供封闭的领域类别，避免配置内容进入指标身份。
    /// 参数说明：无。
    /// 返回：固定类别名。
    fn kind(&self) -> &'static str {
        match self {
            Self::Stream(_) => "stream",
            Self::Proxy(_, _) => "proxy",
            Self::Pipeline(_, _) => "pipeline",
        }
    }
    /// 业务作用：按实际领域任务状态判断本代是否仍在运行。
    /// 参数说明：无。
    /// 返回：任务仍持有运行责任时为 true。
    fn running(&self) -> bool {
        match self {
            Self::Stream(s) => s.state() == StreamTaskState::Running,
            Self::Proxy(p, _) => {
                let report = p.stop_report();
                !report.terminated && !report.failed
            }
            Self::Pipeline(p, _) => p.state() == AutoPipelineState::Running,
        }
    }
}
struct DerivedShutdown(Arc<RedisDerived>);
impl ShutdownAction for DerivedShutdown {
    /// 业务作用：标识 Redis 派生消费与微批聚合关闭动作。
    /// 参数说明：无。
    /// 返回：固定动作名称。
    fn label(&self) -> &'static str {
        "redis-stream-proxy-pipeline"
    }
    /// 业务作用：关闭全部准入后按同一剩余预算并发等待，避免慢来源延迟其它来源停止。
    /// 参数说明：`context` 提供宿主统一关闭期限。
    /// 返回：正常排干成功；超时请求终止并报告未完成。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.close(context.deadline().into());
        Box::pin(async move {
            match tokio::time::timeout_at(context.deadline().into(), self.0.drain()).await {
                Ok(result) => result,
                Err(_) => {
                    self.0.abort();
                    Err(ApplicationError::new(
                        ComponentId::Redis,
                        ApplicationPhase::Stopping,
                        "Redis derived shutdown deadline exceeded",
                    ))
                }
            }
        })
    }
}
impl Drop for DerivedShutdown {
    /// 业务作用：准备回滚或等待取消时收回全部业务准入。
    /// 参数说明：无。
    /// 返回：领域 owner 继续持有实际退出责任。
    fn drop(&mut self) {
        self.0
            .close(tokio::time::Instant::now() + Duration::from_secs(10));
    }
}

/// 业务作用：解析封闭命名计划集合，拒绝无效形状且不输出配置材料。
/// 参数说明：`tree` 是配置树；`key` 是固定段名。
/// 返回：类型化计划，结构错误阻止准备。
fn plans<T: serde::de::DeserializeOwned>(
    tree: &serde_json::Value,
    key: &str,
) -> ApplicationResult<T> {
    serde_json::from_value(tree.get(key).cloned().unwrap_or(serde_json::json!({})))
        .map_err(|_| error("invalid Redis derived configuration"))
}
/// 业务作用：验证命名目录的长度与规范形式。
/// 参数说明：`name` 为固定计划名。
/// 返回：非空、无首尾空白且长度有界时为 true。
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128 && name.trim() == name
}
/// 业务作用：将非法计划名称转换为稳定配置拒绝。
/// 参数说明：`name` 为待检查的计划名称。
/// 返回：合法时成功。
fn validate_name(name: &str) -> ApplicationResult<()> {
    if valid_name(name) {
        Ok(())
    } else {
        Err(error("invalid Redis derived name"))
    }
}
/// 业务作用：验证启用计划所需的后端身份，禁用计划不要求填写独占参数。
/// 参数说明：`value` 为可选 stream 或 group。
/// 返回：合法有界身份，否则拒绝准备。
fn required(value: &Option<String>) -> ApplicationResult<&str> {
    value
        .as_deref()
        .filter(|v| !v.is_empty() && v.len() <= 512 && v.trim() == *v)
        .ok_or_else(|| error("missing or invalid Redis derived identity"))
}
/// 业务作用：要求受管微批具备有限容量，保留独立领域入口的默认行为。
/// 参数说明：`plan` 为显式启用的微批配置。
/// 返回：参数字节软上限配置，缺失、零或过大值拒绝。
fn pipeline_config(plan: &PipelinePlan) -> ApplicationResult<MicroBatchCfg> {
    let cfg = MicroBatchCfg {
        window: Duration::from_millis(plan.window_ms.unwrap_or(1)),
        max_batch: plan.max_batch.unwrap_or(1000),
        queue_capacity: plan.queue_capacity.unwrap_or(4096),
        max_command_bytes: plan.max_command_bytes.unwrap_or(0),
        max_batch_bytes: plan.max_batch_bytes.unwrap_or(0),
    };
    if cfg.window > Duration::from_secs(60)
        || !(1..=65536).contains(&cfg.max_batch)
        || !(1..=65536).contains(&cfg.queue_capacity)
        || !(1..=16 * 1024 * 1024).contains(&cfg.max_command_bytes)
        || !(1..=64 * 1024 * 1024).contains(&cfg.max_batch_bytes)
    {
        return Err(error("Redis pipeline requires finite bounded capacity"));
    }
    Ok(cfg)
}
/// 业务作用：生成不回显后端地址或凭据的固定拒绝原因。
/// 参数说明：`message` 为静态错误类别。
/// 返回：Redis 准备阶段错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Redis, ApplicationPhase::Prepare, message)
}
