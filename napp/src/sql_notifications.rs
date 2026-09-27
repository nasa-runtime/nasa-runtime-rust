//! SQL 离散告警的渠道装配与有限异步投递。
//!
//! Start 只建立内存队列；业务通过 nanotify-core 初始化默认实现，也可在 UserHook 登记命名渠道；Prepare 封口路由；
//! Ready 才由宿主管理的 worker 投递。通知结果从不影响 SQL、事务裁决或数据库 readiness。

use anyhow::{anyhow, bail, Result};
use futures_util::stream::{FuturesUnordered, StreamExt};
use nametrics_core::LegacyMetricsSource;
use nanotify_core::{
    valid_provider_id, DeliveryOutcome, DispatcherConfig, Envelope, NotificationProducer,
    NotificationQueue, NotificationReceiver, Notify, QueueMetrics, RetrySafety,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// 可选命名通知配置；默认通知直接引用 nanotify-core 的业务实现，不要求本配置存在。
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationsConfig {
    pub providers: BTreeMap<String, ProviderConfig>,
}

/// 业务渠道类型声明；框架只接收业务注册的 Notify，不拥有渠道协议。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderConfig {
    Custom(CustomProviderConfig),
}

impl ProviderConfig {
    /// 业务作用：取得渠道声明的启用门禁，不创建任何运行资源。
    /// 参数说明：无。
    /// 返回：是否允许被启用的告警引用。
    pub fn enabled(&self) -> bool {
        match self {
            Self::Custom(value) => value.enabled,
        }
    }
}

/// 业务渠道只声明启用门禁，transport 特有配置由业务独立配置根持有。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CustomProviderConfig {
    pub enabled: bool,
}

impl Default for CustomProviderConfig {
    /// 业务作用：让声明的 custom provider 默认可被规则引用。
    /// 参数说明：无。
    /// 返回：启用声明；业务未提供实现时通知被忽略。
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl NotificationsConfig {
    /// 业务作用：严格解析独立通知配置树，不允许 null 隐式恢复默认。
    /// 参数说明：`value` 是已合并环境变量和远端覆盖的 notifications 子树；缺省传 None。
    /// 返回：共享默认或完整配置；错误不输出输入值、凭据或原始解析文本。
    pub fn parse(value: Option<&serde_json::Value>) -> Result<Self> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        reject_null(value)?;
        let settings: Self = serde_json::from_value(value.clone())
            .map_err(|_| anyhow!("notifications: invalid schema"))?;
        if settings.providers.len() > 32
            || settings.providers.keys().any(|id| !valid_provider_id(id))
            || settings
                .providers
                .contains_key(nanotify_core::DEFAULT_PROVIDER_ID)
        {
            bail!("notifications.providers: invalid provider catalog");
        }
        Ok(settings)
    }

    /// 业务作用：在 Start 分配队列前验证被引用渠道的类型、启用和资源边界。
    /// 参数说明：`references` 是全部生效规则引用，`dispatcher` 为进程唯一总预算。
    /// 返回：默认路由不要求声明，其它引用必须已声明且启用；不要求已有实现，不创建 client。
    pub fn validate_references(
        &self,
        references: &BTreeSet<String>,
        dispatcher: &DispatcherConfig,
    ) -> Result<()> {
        dispatcher.validate()?;
        if self.providers.len() > 32
            || self.providers.keys().any(|id| !valid_provider_id(id))
            || self
                .providers
                .contains_key(nanotify_core::DEFAULT_PROVIDER_ID)
        {
            bail!("notifications.providers: invalid provider catalog");
        }
        for reference in references {
            if reference == nanotify_core::DEFAULT_PROVIDER_ID {
                continue;
            }
            let provider = self
                .providers
                .get(reference)
                .ok_or_else(|| anyhow!("notifications: referenced provider is undeclared"))?;
            if !provider.enabled() {
                bail!("notifications: referenced provider is disabled");
            }
        }
        Ok(())
    }
}

/// 业务作用：拒绝显式 null，避免 optional 字段吞掉类型错误。
/// 参数说明：`value` 为严格配置树中的任意节点。
/// 返回：全部节点有效时成功；不暴露节点内容。
fn reject_null(value: &serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::Null => bail!("notifications: null is not a default value"),
        serde_json::Value::Array(items) => {
            for item in items {
                reject_null(item)?;
            }
        }
        serde_json::Value::Object(items) => {
            for item in items.values() {
                reject_null(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct Registry {
    sealed: bool,
    prepared: bool,
    taken: bool,
    custom: BTreeMap<String, Arc<dyn Notify>>,
    providers: Vec<ProviderBinding>,
    receiver: Option<NotificationReceiver>,
}

#[derive(Clone)]
enum ProviderBinding {
    Global,
    Named(Option<Arc<dyn Notify>>),
}

/// Application 拥有的队列生命周期；默认实现由 nanotify-core 管理，命名 provider 只在受控窗口登记。
pub struct NotificationRuntime {
    config: NotificationsConfig,
    dispatcher: DispatcherConfig,
    references: BTreeSet<String>,
    producer: Option<NotificationProducer>,
    registry: Mutex<Registry>,
}

impl NotificationRuntime {
    /// 业务作用：在 Start 建立有界生产能力，使 UserHook 中的 SQL 可计数和入队但不能触发网络。
    /// 参数说明：`config` 为严格配置，`dispatcher` 为总预算，`references` 包含所有生效 alert 的渠道。
    /// 返回：可安装的运行状态；没有启用 alert 时不创建队列或任务；任何情况均不创建渠道 client。
    pub fn start(
        config: NotificationsConfig,
        dispatcher: DispatcherConfig,
        references: BTreeSet<String>,
    ) -> Result<Self> {
        config.validate_references(&references, &dispatcher)?;
        let (producer, receiver) = if references.is_empty() {
            (None, None)
        } else {
            let (producer, receiver) = NotificationQueue::bounded(
                references.iter().cloned().collect(),
                dispatcher.queue_capacity,
            )?;
            (Some(producer), Some(receiver))
        };
        Ok(Self {
            config,
            dispatcher,
            references,
            producer,
            registry: Mutex::new(Registry {
                sealed: false,
                prepared: false,
                taken: false,
                custom: BTreeMap::new(),
                providers: Vec::new(),
                receiver,
            }),
        })
    }

    /// 业务作用：交出启动期 producer，用于编译每个方法和 datasource 的具体路由。
    /// 参数说明：无。
    /// 返回：启用通知时的生产端；无规则时为 None。
    pub fn producer(&self) -> Option<NotificationProducer> {
        self.producer.clone()
    }

    /// 业务作用：把通知队列与投递指标纳入 Application 启动注册事务。
    /// 参数说明：无。
    /// 返回：每个 source 及其精确最坏系列数；未启用通知时为空。
    pub fn metrics_sources(&self) -> Vec<(Arc<dyn LegacyMetricsSource>, usize)> {
        let mut sources: Vec<(Arc<dyn LegacyMetricsSource>, usize)> = Vec::new();
        if let Some(producer) = &self.producer {
            let source = producer.metrics();
            let budget = source.series_budget();
            sources.push((source, budget));
        }
        sources
    }

    /// 业务作用：在 UserHook 窗口登记配置声明的 custom provider，避免运行期新增无界渠道。
    /// 参数说明：`provider_id` 为声明名称，`provider` 为只在 worker 中调用的异步实现。
    /// 返回：唯一登记成功；未声明、类型不匹配、禁用、重复或封口后均拒绝。
    pub fn register_custom(&self, provider_id: &str, provider: Arc<dyn Notify>) -> Result<()> {
        if provider_id == nanotify_core::DEFAULT_PROVIDER_ID {
            bail!(
                "notifications: initialize the default implementation through nanotify_core::init"
            );
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow!("notifications: registry unavailable"))?;
        if registry.sealed {
            bail!("notifications: provider registration is sealed");
        }
        if !matches!(self.config.providers.get(provider_id), Some(ProviderConfig::Custom(settings)) if settings.enabled)
        {
            bail!("notifications: custom provider is not declared and enabled");
        }
        if registry.custom.contains_key(provider_id) {
            bail!("notifications: custom provider is already registered");
        }
        registry.custom.insert(provider_id.into(), provider);
        Ok(())
    }

    /// 业务作用：在 Prepare 冻结命名路由，默认路由在使用时读取 nanotify-core 的进程实现。
    /// 参数说明：无。
    /// 返回：发布准备状态；没有实现不阻止启动，使用时忽略，不读取渠道凭据或执行网络。
    pub fn prepare(&self) -> Result<()> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow!("notifications: registry unavailable"))?;
        if registry.sealed {
            bail!("notifications: provider graph is already sealed");
        }
        // 命名路由先封口再发布；默认路由只引用外部初始化槽，不把暂未初始化解释成启动错误。
        registry.sealed = true;
        registry.providers = self
            .references
            .iter()
            .map(|reference| {
                if reference == nanotify_core::DEFAULT_PROVIDER_ID {
                    ProviderBinding::Global
                } else {
                    ProviderBinding::Named(registry.custom.get(reference).cloned())
                }
            })
            .collect();
        registry.prepared = true;
        Ok(())
    }

    /// 业务作用：Ready 时把唯一消费权交给 Application Supervisor，不自行启动脱离宿主的任务。
    /// 参数说明：无。
    /// 返回：启用通知时的 dispatcher，未启用时为 None；Prepare 前或重复领取拒绝。
    pub fn take_dispatcher(&self) -> Result<Option<NotificationDispatcher>> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow!("notifications: registry unavailable"))?;
        if !registry.prepared || registry.taken {
            bail!("notifications: dispatcher is unprepared or already taken");
        }
        registry.taken = true;
        let Some(receiver) = registry.receiver.take() else {
            return Ok(None);
        };
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow!("notifications: producer unavailable"))?
            .clone();
        Ok(Some(NotificationDispatcher {
            receiver,
            providers: registry.providers.clone(),
            metrics: producer.metrics(),
            producer,
            settings: self.dispatcher.clone(),
        }))
    }

    /// 业务作用：业务停流后立即撤销所有告警生产权，排空由 worker 在独立预算内完成。
    /// 参数说明：无。
    /// 返回：无；旧 guard/Stream 的后续终止只会记录 stopping drop。
    pub fn stop_accepting(&self) {
        if let Some(producer) = &self.producer {
            producer.close();
        }
    }
}

/// 通知 worker 的唯一所有权；被取消或丢弃时不保留脱离宿主的投递任务。
pub struct NotificationDispatcher {
    receiver: NotificationReceiver,
    providers: Vec<ProviderBinding>,
    producer: NotificationProducer,
    metrics: Arc<QueueMetrics>,
    settings: DispatcherConfig,
}

impl NotificationDispatcher {
    /// 业务作用：在宿主监督下限制共享并发、隔离 provider，并在停止后按预算排空。
    /// 参数说明：`stop` 是 Application 的停止信号；发出前业务应先停止接受新流量。
    /// 返回：队列排空或停止预算耗尽后结束；渠道失败不会返回业务错误。
    pub async fn run(mut self, stop: CancellationToken) {
        let mut pending = FuturesUnordered::new();
        let mut draining = false;
        let mut exhausted = false;
        let mut drain_deadline = None;
        loop {
            if exhausted && pending.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _ = stop.cancelled(), if !draining => {
                    // 先关闭生产门禁再计算排空预算，防止持续失败的 SQL 延长停机。
                    self.producer.close(); self.receiver.close(); draining = true;
                    drain_deadline = Some(tokio::time::Instant::now() + Duration::from_millis(self.settings.shutdown_drain_timeout_ms));
                }
                _ = wait_deadline(drain_deadline), if draining => {
                    // 超预算请求同步取消；每条投递独立隔离 Future 析构，单个渠道不能阻止其它请求收口。
                    self.receiver.discard_remaining(); pending.clear(); break;
                }
                result = pending.next(), if !pending.is_empty() => { let _ = result; }
                message = self.receiver.recv(), if !exhausted && pending.len() < self.settings.max_in_flight => {
                    match message {
                        Some(message) => {
                            let provider = match &self.providers[message.provider] {
                                ProviderBinding::Global => nanotify_core::get().cloned(),
                                ProviderBinding::Named(provider) => provider.clone(),
                            };
                            // 未提供实现时直接忽略，不制造失败、重试或等待业务稍后注册。
                            let Some(provider) = provider else { continue; };
                            pending.push(deliver(message, provider, self.metrics.clone(), self.settings.clone()));
                        }
                        None => exhausted = true,
                    }
                }
            }
        }
        self.producer.close();
    }
}

/// 业务作用：提供只在停止排空期间生效的等待分支。
/// 参数说明：`deadline` 是单调时钟预算终点。
/// 返回：到期后完成；无终点时一直挂起且不创建后台任务。
async fn wait_deadline(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

struct DeliveryGuard {
    metrics: Arc<QueueMetrics>,
    provider: usize,
    event: nanotify_core::EventKind,
    start: Instant,
    finished: bool,
}

impl DeliveryGuard {
    /// 业务作用：记录正常结束的唯一最终结果，防止 Drop 重复计数。
    /// 参数说明：`outcome` 为全部有限尝试后的终态。
    /// 返回：无；只写入固定原子单元。
    fn finish(&mut self, outcome: DeliveryOutcome) {
        self.metrics
            .record_delivery(self.provider, self.event, outcome, self.start.elapsed());
        self.finished = true;
    }
}
impl Drop for DeliveryGuard {
    /// 业务作用：停机取消在途 Future 时登记未完成投递，不把取消误记成接受。
    /// 参数说明：无。
    /// 返回：无；通知取消不影响业务事务。
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.record_delivery(
                self.provider,
                self.event,
                DeliveryOutcome::Shutdown,
                self.start.elapsed(),
            );
        }
    }
}

/// 投递 Future 与终态守卫分开持有，确保取消析构完成后才裁决唯一结果。
struct IsolatedDelivery<F> {
    future: Option<Pin<Box<F>>>,
    guard: DeliveryGuard,
}

impl<F> IsolatedDelivery<F> {
    /// 业务作用：在非展开状态下销毁渠道 Future，覆盖超时、停机与宿主取消。
    /// 参数说明：无。
    /// 返回：析构展开时返回 true；Future 所有权先撤销，不会再次销毁同一对象。
    fn discard(&mut self) -> bool {
        self.future
            .take()
            .is_some_and(|future| isolate_provider(|| drop(future)).is_err())
    }
}

impl<F: Future<Output = DeliveryOutcome>> Future for IsolatedDelivery<F> {
    type Output = ();

    /// 业务作用：隔离渠道轮询和完成后的销毁，先释放 Future 再记录投递终态。
    /// 参数说明：`context` 提供当前宿主的任务唤醒上下文。
    /// 返回：未完成时 Pending；完成或展开时只记录一次结果并返回 Ready。
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(future) = this.future.as_mut() else {
            return Poll::Ready(());
        };
        let outcome = match isolate_provider(|| future.as_mut().poll(context)) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(outcome)) => outcome,
            Err(()) => DeliveryOutcome::Panic,
        };
        // 已返回的结果仍需经过渠道析构边界；析构展开不能被提前记录为 accepted 或 timeout。
        let outcome = if this.discard() {
            DeliveryOutcome::Panic
        } else {
            outcome
        };
        this.guard.finish(outcome);
        Poll::Ready(())
    }
}

impl<F> Drop for IsolatedDelivery<F> {
    /// 业务作用：宿主取消时同步清空渠道 Future，并隔离析构展开。
    /// 参数说明：无。
    /// 返回：析构展开记录 panic；正常取消由终态守卫记录 shutdown，不留下独立投递任务。
    fn drop(&mut self) {
        if self.discard() {
            self.guard.finish(DeliveryOutcome::Panic);
        }
    }
}

/// 业务作用：限制渠道轮询、析构及异常载荷销毁的展开边界，不将渠道异常交给宿主。
/// 参数说明：`action` 为本次渠道操作，不在 SQL 生产路径执行。
/// 返回：正常返回操作值；展开返回空错误，异常对象不进入日志或错误链。
fn isolate_provider<T>(action: impl FnOnce() -> T) -> Result<T, ()> {
    std::panic::catch_unwind(AssertUnwindSafe(action)).map_err(|payload| {
        // 异常载荷本身也可能运行渠道析构；第二次展开不再递归销毁未知对象。
        if let Err(nested) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            std::mem::forget(nested);
        }
    })
}

/// 业务作用：隔离单个 provider 的失败、panic、重试和整个投递预算。
/// 参数说明：`envelope` 为一次告警，`provider` 为冻结实现，`metrics` 记录终态，`settings` 限定时间与次数。
/// 返回：无；所有渠道结果只更新通知指标。
fn deliver(
    envelope: Envelope,
    provider: Arc<dyn Notify>,
    metrics: Arc<QueueMetrics>,
    settings: DispatcherConfig,
) -> impl Future<Output = ()> + Send {
    let start = Instant::now();
    let guard = DeliveryGuard {
        metrics,
        provider: envelope.provider,
        event: envelope.notification.fields().event,
        start,
        finished: false,
    };
    let future = async move {
        let future = async {
            let mut backoff = settings.retry_initial_backoff_ms;
            for attempt in 0..settings.max_attempts {
                let result = provider.notify(&envelope.notification).await;
                match result {
                    Ok(receipt) => {
                        return if receipt.accepted {
                            DeliveryOutcome::Accepted
                        } else {
                            DeliveryOutcome::Rejected
                        }
                    }
                    Err(error) => {
                        let retry_after = match error.retry {
                            RetrySafety::Never => return error.kind.into(),
                            RetrySafety::Safe { retry_after } => retry_after,
                        };
                        if attempt + 1 >= settings.max_attempts {
                            return error.kind.into();
                        }
                        // 只有 provider 声明尚未投递或服务明确拒绝时才重试；未知结果不能制造重复通知。
                        let pause =
                            Duration::from_millis(backoff).max(retry_after.unwrap_or_default());
                        let remaining = Duration::from_millis(settings.delivery_timeout_ms)
                            .saturating_sub(start.elapsed());
                        if pause >= remaining {
                            // 服务要求的最小等待已经超出本条预算；不能缩短它重发，也不构造溢出时钟的定时器。
                            return error.kind.into();
                        }
                        tokio::time::sleep(pause).await;
                        backoff = backoff.saturating_mul(2).min(settings.retry_max_backoff_ms);
                    }
                }
            }
            DeliveryOutcome::Other
        };
        match tokio::time::timeout(Duration::from_millis(settings.delivery_timeout_ms), future)
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => DeliveryOutcome::Timeout,
        }
    };
    IsolatedDelivery {
        future: Some(Box::pin(future)),
        guard,
    }
}
