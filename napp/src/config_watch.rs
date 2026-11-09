//! 本地配置与秘密文件的有界观察；事件只唤醒既有候选处理 owner。

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ComponentId, ReadyContext, ShutdownAction, ShutdownContext, StartContext,
};
use naml::{watch::YmlWatcher, YmlLoader};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// 文件事件使用单个合并信号；每次候选最多持有新旧两套观察集合。
pub(crate) struct LocalWatch {
    pub(crate) signal: Arc<Notify>,
    watcher: Option<LocalObserver>,
    source: Option<Arc<crate::config_source::StrictSource>>,
    pub(crate) interval_ms: u64,
    pub(crate) enabled: bool,
}

pub(crate) struct PreparedWatch {
    observer: PreparedObserver,
    settings: WatchSettings,
}

pub(crate) struct LocalObserver {
    _legacy: Option<YmlWatcher>,
    _strict: Option<naml::strict::ConfigWatcher>,
    fingerprint: Option<[u8; 32]>,
}

pub(crate) enum PreparedObserver {
    Replace(Option<Box<LocalObserver>>),
    Unchanged,
}

/// 候选准备只持有观察策略快照，旧 watcher 继续由异步驱动持有，阻塞任务不能替换它。
struct WatchPreparation {
    source: Option<Arc<crate::config_source::StrictSource>>,
    signal: Arc<Notify>,
    current_fingerprint: Option<[u8; 32]>,
    current_healthy: bool,
    current_directories: usize,
}

impl LocalWatch {
    /// 业务作用：建立无任务的文件观察 owner，未显式启用时不创建操作系统 watcher。
    /// 参数说明：`raw` 为原始合并树，秘密声明尚未被脱敏；`source` 固定显式严格来源。
    /// 返回：已启用时建立当前依赖观察；目录挂载失败阻止启动。
    pub(crate) async fn new(
        raw: Value,
        source: Option<Arc<crate::config_source::StrictSource>>,
    ) -> ApplicationResult<Self> {
        // 来源复验和操作系统 watcher 建立都可能等待文件系统，不能占用业务调度线程。
        crate::read_config_candidate(move || {
            let mut owner = Self {
                signal: Arc::new(Notify::new()),
                watcher: None,
                source,
                interval_ms: crate::DEFAULT_CONFIG_POLL_INTERVAL_MS,
                enabled: false,
            };
            let prepared = owner.preparation().prepare(&raw)?;
            drop(owner.install(prepared, false));
            Ok::<_, ApplicationError>(owner)
        })
        .await?
    }

    /// 业务作用：复制候选所需的观察身份和目录预算，不转移或关闭当前 watcher。
    /// 参数说明：无。
    /// 返回：仅含内存状态及共享来源的准备输入，旧监听在后台准备期间仍有效。
    fn preparation(&self) -> WatchPreparation {
        let current = self.watcher.as_ref();
        let health = current
            .and_then(|observer| observer._strict.as_ref())
            .map(|watcher| watcher.health());
        WatchPreparation {
            source: self.source.clone(),
            signal: self.signal.clone(),
            current_fingerprint: current.and_then(|observer| observer.fingerprint),
            current_healthy: health.as_ref().is_some_and(|health| health.backend_ok),
            current_directories: health.map_or(0, |health| health.watched_directories),
        }
    }

    /// 业务作用：在线程池准备完整候选监听，使文件系统等待不占用业务线程。
    /// 参数说明：
    /// - `raw`：尚未发布的完整候选树。
    /// 返回：独立新监听或复用决定；失败不改变当前监听，后台准备没有安装权限。
    pub(crate) async fn prepare(&mut self, raw: &Value) -> ApplicationResult<PreparedWatch> {
        let preparation = self.preparation();
        let raw = raw.clone();
        let prepared = crate::read_config_candidate(move || preparation.prepare(&raw)).await??;
        // 阻塞准备排队期间后端仍可能失效；复用旧观察前再次确认健康，不能发布已经失联的观察证据。
        if matches!(prepared.observer, PreparedObserver::Unchanged)
            && !self
                .watcher
                .as_ref()
                .and_then(|observer| observer._strict.as_ref())
                .is_some_and(|watcher| watcher.health().backend_ok)
        {
            return Err(error(
                "configuration observer became unavailable during preparation",
            ));
        }
        Ok(prepared)
    }
}

impl WatchPreparation {
    /// 业务作用：在候选安装前建立独立的观察集合，失败不撤销当前有效监听。
    /// 参数说明：`raw` 为待解析材料所属候选。
    /// 返回：至多追加 128 个材料依赖的新 watcher；严格来源另受共享目录预算约束，任一活跃目录不可观察时拒绝候选。
    fn prepare(&self, raw: &Value) -> ApplicationResult<PreparedWatch> {
        let settings = settings(raw)?;
        let observer = self.prepare_observer(raw, settings.enabled)?;
        Ok(PreparedWatch { observer, settings })
    }

    /// 业务作用：在当前观察仍有效时准备候选的目录和秘密依赖，失败不撤销旧监听。
    /// 参数说明：
    /// - `raw`：已校验观察设置的完整候选树。
    /// - `enabled`：候选是否启用本地文件观察。
    /// 返回：可在候选提交后安装的观察集合；任何目录或来源异常均拒绝候选。
    fn prepare_observer(&self, raw: &Value, enabled: bool) -> ApplicationResult<PreparedObserver> {
        if !enabled {
            return Ok(PreparedObserver::Replace(None));
        }
        let specs = crate::secret::active_specs(raw)
            .map_err(|_| error("invalid secret file declarations"))?;
        let mut paths = std::collections::BTreeSet::new();
        let needed_providers = specs
            .iter()
            .flat_map(|spec| &spec.fragments)
            .filter_map(|fragment| match fragment {
                nasecret::SecretFragmentRef::Provider { provider, .. } => Some(provider.clone()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        // 文件观察与解析共享活跃消费者；禁用计划不能因缺失挂载而阻止无关业务启动。
        for spec in specs {
            for fragment in spec.fragments {
                if let nasecret::SecretFragmentRef::File(path) = fragment {
                    paths.insert(path);
                }
            }
        }
        // provider 引导 token 与业务 secret 同属候选依赖，轮换文件必须走同一发布边界。
        for (name, provider) in raw
            .get("secret_providers")
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|providers| providers.iter())
        {
            if needed_providers.contains(name.as_str())
                && provider.get("enabled").and_then(Value::as_bool) == Some(true)
            {
                if let Some(path) = provider.pointer("/token/file").and_then(Value::as_str) {
                    paths.insert(std::path::PathBuf::from(path));
                }
            }
        }
        if paths.len() > 128 {
            return Err(error("configuration watch dependency count exceeds 128"));
        }
        let signal = self.signal.clone();
        if let Some(source) = &self.source {
            let mut plan = source.watch_plan();
            plan.dependencies.extend(paths);
            let fingerprint = plan.fingerprint();
            if self.current_fingerprint == Some(fingerprint) && self.current_healthy {
                // 来源身份和目录集合均未变化时复用现有观察，避免补读不断重建操作系统资源。
                plan.revalidate(source.loader.load_policy()).map_err(|_| {
                    error("strict configuration sources changed before publication")
                })?;
                return Ok(PreparedObserver::Unchanged);
            }
            let mut policy = source.loader.load_policy().clone();
            // 独立新旧 watcher 同时存在，重复目录也占两份后端资源，先扣除仍有效的旧观察预算。
            policy.limits.watch_directories = policy
                .limits
                .watch_directories
                .saturating_sub(self.current_directories);
            let watcher =
                naml::strict::ConfigWatcher::new(&plan, policy, move |_| signal.notify_one())
                    .map_err(|_| error("cannot observe strict configuration sources"))?;
            // 观察安装可能等待后端；提交之前再次验证来源，拒绝已知发生变化的候选。
            plan.revalidate(source.loader.load_policy())
                .map_err(|_| error("strict configuration sources changed before publication"))?;
            return Ok(PreparedObserver::Replace(Some(Box::new(LocalObserver {
                _legacy: None,
                _strict: Some(watcher),
                fingerprint: Some(fingerprint),
            }))));
        }
        let sources = YmlLoader::standard()
            .local_sources()
            .map_err(|_| error("cannot resolve local configuration sources"))?;
        YmlWatcher::new(
            &sources,
            &paths.into_iter().collect::<Vec<_>>(),
            move |_| signal.notify_one(),
        )
        .map(|watcher| {
            PreparedObserver::Replace(Some(Box::new(LocalObserver {
                _legacy: Some(watcher),
                _strict: None,
                fingerprint: None,
            })))
        })
        .map_err(|_| error("cannot observe configuration dependencies"))
    }
}

impl LocalWatch {
    /// 业务作用：在发布完成后替换有效观察集合，旧集合始终保留到新候选可用。
    /// 参数说明：
    /// - `next`：`next` 为安装前已建立的观察。
    /// - `published`：`published` 表示有新视图发布。
    ///
    /// 返回：已替换的旧 watcher 交由调用方在线程池释放；发布后补读覆盖准备期间的材料变化。
    fn install(&mut self, next: PreparedWatch, published: bool) -> Option<LocalObserver> {
        // 周期和监听归属同一候选，只有完整配置提交后才更新有效观察策略。
        self.interval_ms = next.settings.poll_interval_ms;
        self.enabled = next.settings.enabled;
        let retired = match next.observer {
            PreparedObserver::Replace(next) => {
                std::mem::replace(&mut self.watcher, next.map(|watcher| *watcher))
            }
            PreparedObserver::Unchanged => None,
        };
        if let Some(source) = &self.source {
            source.commit_observation();
        }
        // 关闭观察的已提交候选不再申请补读，避免撤销监听后仍消费自身留下的通知。
        if published && self.enabled {
            self.signal.notify_one();
        }
        retired
    }

    /// 业务作用：按已完成的配置事务结果保留或替换监听，并在线程池回收不再使用的观察资源。
    /// 参数说明：
    /// - `next`：尚未安装的候选监听。
    /// - `published`：调用方完成配置发布或拒绝后的结果。
    /// 返回：保留原发布结果；拒绝时旧监听和周期不变，成功时先更新策略再等待旧监听释放。
    pub(crate) async fn finish(
        &mut self,
        next: PreparedWatch,
        published: ApplicationResult<Option<u64>>,
    ) -> ApplicationResult<Option<u64>> {
        match published {
            Ok(version) => {
                // 配置已经提交，策略切换不能因回收等待被取消而留下新配置与旧周期的组合。
                let retired = self.install(next, version.is_some());
                if let Some(retired) = retired {
                    crate::read_config_candidate(move || drop(retired)).await?;
                }
                Ok(version)
            }
            Err(error) => {
                // 准备的观察从未取得安装资格；只关闭候选，旧观察仍负责接收后续合法变更。
                if let PreparedObserver::Replace(Some(observer)) = next.observer {
                    crate::read_config_candidate(move || drop(observer)).await?;
                }
                Err(error)
            }
        }
    }

    /// 业务作用：按已提交周期限定成功观察的有效期，避免长周期正常等待被误判为观察失联。
    /// 参数说明：
    /// - `health`：独立本地配置观察的健康贡献项。
    /// 返回：启用时发布有期限的 Ready；关闭时清除观察期限并记录关闭状态，不把主动关闭误判为失联。
    fn observe_ready(&self, health: &ReadinessContributor) {
        if !self.enabled {
            // 关闭策略已提交且监听已撤销，不再要求周期观察证据；保留健康项以表明当前策略。
            health.observe(DependencyState::Ready, "disabled", Instant::now());
            return;
        }
        let freshness = Duration::from_millis(self.interval_ms)
            .saturating_mul(3)
            .max(Duration::from_secs(45));
        let now = Instant::now();
        // 使用已安装周期；拒绝的候选不能延长有效期，驱动阻塞也不能自行维持健康。
        // 单调时间无法表示期限时提交过期证据，不以时间溢出中断正在运行的应用。
        health.observe_ready_until(now.checked_add(freshness).unwrap_or(now));
    }
}

#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WatchSettings {
    enabled: bool,
    poll_interval_ms: u64,
}

impl Default for WatchSettings {
    /// 业务作用：缺省关闭文件监听，显式开启后使用一秒周期补读。
    /// 参数说明：无。
    /// 返回：无需创建观察资源的默认策略。
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_ms: crate::DEFAULT_CONFIG_POLL_INTERVAL_MS,
        }
    }
}

/// 业务作用：校验本地观察策略后取得显式开关，避免编入 feature 就自动监听文件。
/// 参数说明：
/// - `tree`：`tree` 为候选配置。
///
/// 返回：段缺失等同禁用；未知字段或错误类型拒绝。
pub(crate) fn enabled(tree: &Value) -> ApplicationResult<bool> {
    settings(tree).map(|settings| settings.enabled)
}

/// 业务作用：在任何观察或配置副作用前绑定并校验整份补读策略。
/// 参数说明：
/// - `tree`：本地和远端合并后的候选树。
/// 返回：缺省策略或完整合法配置；未知字段、错误类型及不可用周期拒绝候选。
fn settings(tree: &Value) -> ApplicationResult<WatchSettings> {
    let settings = tree
        .get("config_watch")
        .map(|value| {
            naml::strict::bind::<WatchSettings>(value.clone())
                .map_err(|_| error("invalid config_watch declaration"))
        })
        .unwrap_or_else(|| Ok(WatchSettings::default()))?;
    crate::ConfigPoller::validate_interval(settings.poll_interval_ms)?;
    Ok(settings)
}

pub(crate) struct LocalConfigComponent {
    components: Vec<ComponentId>,
    health: Option<ReadinessContributor>,
    task: Option<ApplicationFuture<'static>>,
}

impl LocalConfigComponent {
    /// 业务作用：为没有活动 Nacos 驱动的应用准备来源中立的监听组件。
    /// 参数说明：`components` 为声明目标集合。
    /// 返回：尚未读取文件或创建线程的组件。
    pub(crate) fn new(components: Vec<ComponentId>) -> Self {
        Self {
            components,
            health: None,
            task: None,
        }
    }
}

impl ApplicationComponent for LocalConfigComponent {
    /// 业务作用：归属现有 Config 内部身份，不增加业务组件字符串。
    /// 参数说明：无。
    /// 返回：固定配置身份。
    fn id(&self) -> ComponentId {
        ComponentId::Config
    }

    /// 业务作用：只为独立本地 watch 注册健康；活动 Nacos 由其已有驱动统一接收文件事件。
    /// 参数说明：`context` 提供最终初始配置和健康目录。
    /// 返回：开关合法且模式支持时成功；Batch 不开放持续配置消费。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let app = context.application();
            let view = app.config();
            if !enabled(view.value())? {
                return Ok(());
            }
            if app.info().mode() == crate::ApplicationMode::Batch {
                return Err(error("config_watch requires Service mode"));
            }
            if self.components.contains(&ComponentId::NacosConfig)
                && view
                    .value()
                    .pointer("/nacos/enabled")
                    .and_then(Value::as_bool)
                    == Some(true)
            {
                return Ok(());
            }
            self.health = Some(app.register_readiness(
                ComponentId::Config,
                Arc::<str>::from("config-watch:local"),
                ReadinessPolicy {
                    affects_ready: false,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    // 有效期随已提交的补读周期变化，由每次成功观察携带绝对期限。
                    stale_after: None,
                },
            )?);
            Ok(())
        })
    }

    /// 业务作用：建立当前监听集合并暂存唯一配置候选任务，等待宿主整体 Ready。
    /// 参数说明：`context` 提供既有配置 applier 及停机责任登记。
    /// 返回：全部目录可观察时成功；尚未开放候选安装。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(health) = self.health.take() else {
                return Ok(());
            };
            let source = context.application().strict_source();
            let raw = if source.is_some() {
                context.application().config().value().clone()
            } else {
                crate::read_config_candidate(|| {
                    YmlLoader::standard()
                        .load_tree()
                        .map_err(|_| error("cannot read initial watched configuration"))
                })
                .await??
            };
            let watcher = LocalWatch::new(raw, source).await?;
            let app = context.application().clone();
            let publisher = crate::config_reload::CandidatePublisher::new(
                app.clone(),
                self.components.clone(),
                app.config().value().get("application").cloned(),
            );
            let cancel = CancellationToken::new();
            context.activate(Box::new(WatchShutdown(cancel.clone())));
            watcher.observe_ready(&health);
            self.task = Some(Box::pin(run(app, watcher, publisher, health, cancel)));
            Ok(())
        })
    }

    /// 业务作用：移交唯一配置驱动给宿主监督器。
    /// 参数说明：无。
    /// 返回：首次返回任务；未启用或重复取得时为空。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.task.take().map(|task| ("local-config-watch", task))
    }
}

/// 业务作用：按单个合并信号和有效配置周期处理本地候选，不为事件风暴无限延迟刷新。
/// 参数说明：
/// - `app`：`app` 为宿主。
/// - `watcher`：`watcher` 为当前观察。
/// - `publisher`：`publisher` 保管实际投影。
/// - `health`：`health` 汇报结果。
/// - `cancel`：`cancel` 关闭驱动。
///
/// 返回：取消后退出；坏候选只降级并保留旧配置与周期。观察关闭后仅等待停机，不再读取本地候选。
async fn run(
    app: Application,
    mut watcher: LocalWatch,
    mut publisher: crate::config_reload::CandidatePublisher,
    health: ReadinessContributor,
    cancel: CancellationToken,
) -> ApplicationResult<()> {
    let mut poll = crate::ConfigPoller::new(watcher.interval_ms)?;
    loop {
        let signal = watcher.signal.clone();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = app.cancellation_token().cancelled_owned() => return Ok(()),
            // 已提交的开关同时约束定期补读和积压通知；关闭后仍保留关键任务等待停机。
            _ = poll.tick(), if watcher.enabled => {},
            _ = signal.notified(), if watcher.enabled => {},
        }
        // 固定短窗口合并频繁 rename；不随新事件重置，因此始终有机会处理最新候选。
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
        let result = async {
            let source = app.strict_source();
            let raw = poll
                .read(move || {
                    if let Some(source) = source {
                        source.load_local()
                    } else {
                        YmlLoader::standard()
                            .load_tree()
                            .map_err(|_| error("cannot read watched configuration"))
                    }
                })
                .await??;
            // 同步读取不能被强制中断；完成后先复验停机，不能据读取开始时的资格继续发布。
            if cancel.is_cancelled() || app.cancellation_token().is_cancelled() {
                return Ok(());
            }
            let prepared_watch = watcher.prepare(&raw).await?;
            let published = publisher.publish(raw, Vec::new(), &cancel).await;
            watcher.finish(prepared_watch, published).await?;
            poll.set_interval(watcher.interval_ms)?;
            Ok::<_, ApplicationError>(())
        }
        .await;
        match result {
            Ok(()) => watcher.observe_ready(&health),
            Err(error) => {
                // 拒绝立即显示降级，但旧配置、监听和周期仍然有效，等待后续合法候选。
                health.observe(DependencyState::Degraded, reason::DEGRADED, Instant::now());
                tracing::warn!(reason = error.message(), "configuration candidate rejected");
            }
        }
    }
}

struct WatchShutdown(CancellationToken);
impl ShutdownAction for WatchShutdown {
    /// 业务作用：标识本地配置驱动的停止请求。
    /// 参数说明：无。
    /// 返回：固定名称。
    fn label(&self) -> &'static str {
        "local-config-watch"
    }
    /// 业务作用：关闭候选准入，任务退出由宿主监督器取得证明。
    /// 参数说明：`_context` 为宿主剩余预算。
    /// 返回：已请求停止；不把请求本身当作任务退出。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.cancel();
        Box::pin(async { Ok(()) })
    }
}
impl Drop for WatchShutdown {
    /// 业务作用：启动回滚或执行器释放时收回候选准入。
    /// 参数说明：无。
    /// 返回：同步通知唯一任务停止。
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// 业务作用：按固定原因归类文件观察或结构错误，避免输出材料路径和值。
/// 参数说明：`message` 为稳定错误摘要。
/// 返回：配置组件错误。
fn error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Config, ApplicationPhase::Running, message)
}
