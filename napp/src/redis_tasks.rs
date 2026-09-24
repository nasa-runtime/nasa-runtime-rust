//! 独立竞选与订阅的来源绑定、Ready 屏障和退出责任。

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult,
    ComponentId, PrepareContext, ShutdownAction, ShutdownContext,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub(crate) type Handler =
    Arc<dyn Fn(nadis::pubsub::Message) -> ApplicationFuture<'static> + Send + Sync>;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderPlan {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
    key: String,
    period_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionPlan {
    #[serde(default)]
    enabled: bool,
    redis_ref: Option<String>,
    #[serde(default)]
    channels: Vec<String>,
    max_payload_bytes: Option<usize>,
    #[serde(default)]
    critical: bool,
}

struct Observation {
    health: ReadinessContributor,
    failed: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    leader: Option<Arc<nadis::leader::Leader>>,
    critical: bool,
}

struct SubscriptionExit {
    cancel: CancellationToken,
    failed: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
}

impl Drop for SubscriptionExit {
    /// 业务作用：即使 handler panic，也将实际退出反映到任务健康，避免长期保留虚假的连接状态。
    /// 参数说明：无。
    /// 返回：清除连接观测；未收到停机请求的退出被标记为失败。
    fn drop(&mut self) {
        self.connected.store(false, Ordering::Release);
        if !self.cancel.is_cancelled() {
            self.failed.store(true, Ordering::Release);
        }
    }
}

#[derive(Default)]
pub(crate) struct RedisTasks {
    handlers: Mutex<(bool, BTreeMap<String, Handler>)>,
    observations: Mutex<Vec<Observation>>,
    leaders: Mutex<Vec<ManagedRedisLeader>>,
    ready: CancellationToken,
    cancel: CancellationToken,
    tasks: TaskTracker,
}

/// 独立竞选的受管业务入口；没有领导权时跳过，失权取消不代表远端副作用未发生。
#[derive(Clone)]
pub struct ManagedRedisLeader {
    leader: Arc<nadis::leader::Leader>,
    calls: Arc<crate::managed_adapters::ManagedAdapter<()>>,
    ready: CancellationToken,
    cancel: CancellationToken,
}

impl ManagedRedisLeader {
    /// 业务作用：读取当前来源的本地任期观测，Ready 前或关闭后不授予业务权威。
    /// 参数说明：无。
    /// 返回：处于开放生命周期且领域 leader 仍有效时为 true；不能代替外部 fencing。
    pub fn is_leader(&self) -> bool {
        self.ready.is_cancelled() && !self.cancel.is_cancelled() && self.leader.is_leader()
    }

    /// 业务作用：在受管调用责任内执行可响应失权的幂等任务。
    /// 参数说明：`work` 接受当前任期的取消信号，不得派生未登记任务。
    /// 返回：任内完成时 Some；未开放、非 leader 或失权时 None；关闭时拒绝。
    pub async fn run<F, Fut, T>(&self, work: F) -> ApplicationResult<Option<T>>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _call = self
            .calls
            .enter()
            .await
            .map_err(|_| error("Redis leader is closed"))?;
        if !self.is_leader() {
            return Ok(None);
        }
        Ok(self.leader.run_if_leader_cancellable(work).await)
    }
}

impl RedisTasks {
    /// 业务作用：登记启动期订阅策略，容量按命名计划固定。
    /// 参数说明：`name` 为稳定计划名；`handler` 为业务处理函数。
    /// 返回：未封口且名称唯一时成功，不创建任务。
    pub(crate) fn register(&self, name: &str, handler: Handler) -> ApplicationResult<()> {
        let mut handlers = self.handlers.lock().expect("Redis subscription plans");
        if handlers.0
            || handlers.1.len() >= 64
            || !valid_name(name)
            || handlers.1.contains_key(name)
        {
            return Err(error(
                "Redis subscription plan is invalid, duplicated or closed",
            ));
        }
        handlers.1.insert(name.to_owned(), handler);
        Ok(())
    }

    /// 业务作用：准备独立 Redis 长期能力并立即登记唯一聚合清理责任。
    /// 参数说明：`context` 提供已连接来源和资源目录。
    /// 返回：启用计划与 handler 全部匹配；任何错误阻止 Ready。
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        context: &mut PrepareContext<'_>,
    ) -> ApplicationResult<()> {
        let app = context.application().clone();
        let view = app.config();
        let leaders: BTreeMap<String, LeaderPlan> = serde_json::from_value(
            view.value()
                .get("redis_leaders")
                .cloned()
                .unwrap_or(serde_json::json!({})),
        )
        .map_err(|_| error("invalid redis_leaders declaration"))?;
        let subscriptions: BTreeMap<String, SubscriptionPlan> = serde_json::from_value(
            view.value()
                .get("redis_subscriptions")
                .cloned()
                .unwrap_or(serde_json::json!({})),
        )
        .map_err(|_| error("invalid redis_subscriptions declaration"))?;
        let mut handlers = {
            let mut handlers = self.handlers.lock().expect("Redis subscription plans");
            handlers.0 = true;
            std::mem::take(&mut handlers.1)
        };
        if leaders.len() > 64 || subscriptions.len() > 64 {
            return Err(error("Redis derived plan count exceeds 64"));
        }
        let enabled = leaders.values().any(|plan| plan.enabled)
            || subscriptions.values().any(|plan| plan.enabled);
        if enabled && app.info().mode() != crate::ApplicationMode::Service {
            return Err(error(
                "Redis leader and subscription plans require Service mode",
            ));
        }
        context.activate(Box::new(RedisTasksShutdown(self.clone())));
        for (name, plan) in leaders {
            if !plan.enabled {
                continue;
            }
            let period = plan.period_ms.unwrap_or(1000);
            if !valid_name(&name)
                || plan.key.is_empty()
                || plan.key.len() > 512
                || !(1..=60_000).contains(&period)
            {
                return Err(error("invalid Redis leader plan"));
            }
            let client =
                crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default"))
                    .await?;
            let health = app.register_readiness(
                ComponentId::Redis,
                Arc::<str>::from(format!("redis-leader:{name}")),
                policy(false),
            )?;
            let leader = nadis::leader::Leader::elect(
                Arc::new(nadis::lock::DistributedLock::new(client)),
                plan.key,
                Duration::from_millis(period),
            );
            let handle = ManagedRedisLeader {
                leader: leader.clone(),
                calls: crate::managed_adapters::ManagedAdapter::new(Arc::new(())),
                ready: self.ready.clone(),
                cancel: self.cancel.clone(),
            };
            self.leaders
                .lock()
                .expect("Redis leaders")
                .push(handle.clone());
            self.observations
                .lock()
                .expect("Redis task health")
                .push(Observation {
                    health,
                    failed: Arc::new(AtomicBool::new(false)),
                    connected: Arc::new(AtomicBool::new(true)),
                    leader: Some(leader),
                    critical: false,
                });
            context.register_resource(Some(&name), handle)?;
        }
        for (name, plan) in subscriptions {
            if !plan.enabled {
                continue;
            }
            let limit = plan.max_payload_bytes.unwrap_or(1024 * 1024);
            if !valid_name(&name)
                || plan.channels.is_empty()
                || plan.channels.len() > 128
                || plan
                    .channels
                    .iter()
                    .any(|channel| channel.is_empty() || channel.len() > 512)
                || !(1..=16 * 1024 * 1024).contains(&limit)
            {
                return Err(error("invalid Redis subscription plan"));
            }
            let handler = handlers
                .remove(&name)
                .ok_or_else(|| error("enabled Redis subscription requires a handler"))?;
            let client =
                crate::redis::redis_handle(&app, plan.redis_ref.as_deref().unwrap_or("default"))
                    .await?;
            let health = app.register_readiness(
                ComponentId::Redis,
                Arc::<str>::from(format!("redis-subscription:{name}")),
                policy(plan.critical),
            )?;
            let subscription = client
                .sub(&plan.channels.iter().map(String::as_str).collect::<Vec<_>>())
                .await
                .map_err(|_| error("Redis subscription preparation failed"))?;
            let failed = Arc::new(AtomicBool::new(false));
            let connected = Arc::new(AtomicBool::new(true));
            health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
            self.observations
                .lock()
                .expect("Redis task health")
                .push(Observation {
                    health,
                    failed: failed.clone(),
                    connected: connected.clone(),
                    leader: None,
                    critical: plan.critical,
                });
            let ready = self.ready.clone();
            let cancel = self.cancel.clone();
            self.tasks.spawn(async move {
                let _exit = SubscriptionExit {
                    cancel: cancel.clone(),
                    failed: failed.clone(),
                    connected: connected.clone(),
                };
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = ready.cancelled() => {},
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {},
                    _ = consume(subscription, handler, limit, connected) => { failed.store(true, Ordering::Release); },
                }
            });
        }
        if !handlers.is_empty() {
            return Err(error(
                "Redis subscription handler references an unknown or disabled plan",
            ));
        }
        Ok(())
    }

    /// 业务作用：在宿主 Ready 后放行订阅，并向既有 Redis monitor 提供任务健康。
    /// 参数说明：无。
    /// 返回：关键任务退出时拒绝继续运行；来源连接健康仍由 Redis 自身探测。
    pub(crate) fn observe(&self) -> ApplicationResult<()> {
        if self.cancel.is_cancelled() {
            return Ok(());
        }
        self.ready.cancel();
        for observation in self.observations.lock().expect("Redis task health").iter() {
            let failed = observation.failed.load(Ordering::Acquire);
            // 构造完成不代表竞选已运行；任务异常退出后不能由来源探针反复刷新旧的成功状态。
            let healthy = !failed
                && observation.connected.load(Ordering::Acquire)
                && observation
                    .leader
                    .as_ref()
                    .is_none_or(|leader| leader.is_running());
            observation.health.observe(
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
            if failed && observation.critical {
                return Err(error("critical Redis subscription task exited"));
            }
        }
        Ok(())
    }

    /// 业务作用：先同步关闭全部订阅与领导权调用，再等待退出。
    /// 参数说明：无。
    /// 返回：准入永久关闭，后续 Ready 观测不能重开。
    fn close(&self) {
        self.cancel.cancel();
        self.tasks.close();
        for leader in self.leaders.lock().expect("Redis leaders").iter() {
            leader.calls.close();
            leader.leader.begin_shutdown();
        }
    }

    /// 业务作用：并发取得所有派生任务与领导权调用的退出证明。
    /// 参数说明：无。
    /// 返回：全部调用 future、订阅和竞选退出后完成。
    async fn drain(&self) {
        let leaders = self.leaders.lock().expect("Redis leaders").clone();
        futures_util::future::join_all(leaders.iter().map(|leader| async {
            leader.leader.shutdown().await;
            let _exclusive = leader.calls.drain().await;
        }))
        .await;
        self.tasks.wait().await;
    }
}

struct RedisTasksShutdown(Arc<RedisTasks>);
impl ShutdownAction for RedisTasksShutdown {
    /// 业务作用：标识独立 Redis 长期能力聚合清理。
    /// 参数说明：无。
    /// 返回：固定动作名。
    fn label(&self) -> &'static str {
        "redis-derived-tasks"
    }
    /// 业务作用：按同一期限停止所有独立竞选与订阅。
    /// 参数说明：`context` 为宿主剩余预算。
    /// 返回：全部退出时成功；超时明确报告未完成。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.close();
        Box::pin(async move {
            tokio::time::timeout_at(context.deadline().into(), self.0.drain())
                .await
                .map_err(|_| error("Redis derived tasks did not stop"))?;
            Ok(())
        })
    }
}
impl Drop for RedisTasksShutdown {
    /// 业务作用：启动失败或取消等待时收回全部派生能力准入。
    /// 参数说明：无。
    /// 返回：提出停止请求，实际收口仍由任务 owner 持有。
    fn drop(&mut self) {
        self.0.close();
    }
}

/// 业务作用：顺序处理有界消息，断线后有界退避重订阅；不承诺补回 Pub/Sub 缺口。
/// 参数说明：`subscription` 持有来源连接；`handler` 为策略；`limit` 限制交给业务的正文；`connected` 报告链路状态。
/// 返回：handler 失败或超大消息时退出，由聚合 owner 分类；取消直接释放本次 future。
async fn consume(
    mut subscription: nadis::pubsub::Subscription,
    handler: Handler,
    limit: usize,
    connected: Arc<AtomicBool>,
) {
    loop {
        match subscription.next_message().await {
            Some(message) => {
                if message.payload.len() > limit || handler(message).await.is_err() {
                    return;
                }
            }
            None => {
                connected.store(false, Ordering::Release);
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if subscription
                        .reconnect_timeout(Duration::from_secs(5))
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
                connected.store(true, Ordering::Release);
            }
        }
    }
}

impl Application {
    /// 业务作用：取得按来源绑定的命名竞选入口。
    /// 参数说明：`name` 为 redis_leaders 计划名称。
    /// 返回：受 Ready 与关闭门禁保护的句柄；不存在时拒绝。
    pub async fn redis_leader(&self, name: &str) -> ApplicationResult<ManagedRedisLeader> {
        Ok(self
            .named_resource::<ManagedRedisLeader>(name)
            .await?
            .clone())
    }
}

/// 业务作用：保持派生任务名称有界，避免资源与指标身份无限增长。
/// 参数说明：`name` 为声明名称。
/// 返回：合法稳定名称为 true。
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128 && name.trim() == name
}

/// 业务作用：按明确关键性设置独立长期任务健康。
/// 参数说明：`critical` 指示该能力是否影响服务可用。
/// 返回：固定阈值与过期窗口的策略。
fn policy(critical: bool) -> ReadinessPolicy {
    ReadinessPolicy {
        affects_ready: critical,
        failure_threshold: 1,
        recovery_threshold: 1,
        stale_after: Some(Duration::from_secs(15)),
    }
}

/// 业务作用：返回不包含 key、channel 和凭据的稳定错误。
/// 参数说明：`message` 为固定原因。
/// 返回：Redis 子能力错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Redis, ApplicationPhase::Prepare, message)
}
