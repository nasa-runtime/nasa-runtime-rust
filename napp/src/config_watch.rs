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
    watcher: Option<YmlWatcher>,
}

impl LocalWatch {
    /// 业务作用：建立无任务的文件观察 owner，未显式启用时不创建操作系统 watcher。
    /// 参数说明：`raw` 为原始合并树，秘密声明尚未被脱敏。
    /// 返回：已启用时建立当前依赖观察；目录挂载失败阻止启动。
    pub(crate) fn new(raw: &Value) -> ApplicationResult<Self> {
        let signal = Arc::new(Notify::new());
        let mut owner = Self {
            signal,
            watcher: None,
        };
        owner.watcher = owner.prepare(raw)?;
        Ok(owner)
    }

    /// 业务作用：在候选安装前建立独立的观察集合，失败不撤销当前有效监听。
    /// 参数说明：`raw` 为待解析材料所属候选。
    /// 返回：至多包含 128 个活跃文件依赖的新 watcher；未启用时为空，任一活跃目录不可观察时拒绝候选。
    pub(crate) fn prepare(&self, raw: &Value) -> ApplicationResult<Option<YmlWatcher>> {
        if !enabled(raw)? {
            return Ok(None);
        }
        let sources = YmlLoader::standard()
            .local_sources()
            .map_err(|_| error("cannot resolve local configuration sources"))?;
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
        YmlWatcher::new(
            &sources,
            &paths.into_iter().collect::<Vec<_>>(),
            move |_| signal.notify_one(),
        )
        .map(Some)
        .map_err(|_| error("cannot observe configuration dependencies"))
    }

    /// 业务作用：在发布完成后替换有效观察集合，旧集合始终保留到新候选可用。
    /// 参数说明：`next` 为安装前已建立的观察；`published` 表示有新视图发布。
    /// 返回：旧 watcher 在发布锁外释放；发布后补一次重读覆盖准备期间的材料变化。
    pub(crate) fn install(&mut self, next: Option<YmlWatcher>, published: bool) {
        self.watcher = next;
        if published {
            self.signal.notify_one();
        }
    }
}

#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WatchSettings {
    #[serde(default)]
    enabled: bool,
}

/// 业务作用：只解析本地观察的显式开关，避免编入 feature 就自动监听文件。
/// 参数说明：`tree` 为候选配置。
/// 返回：段缺失等同禁用；未知字段或错误类型拒绝。
pub(crate) fn enabled(tree: &Value) -> ApplicationResult<bool> {
    tree.get("config_watch")
        .map(|value| {
            serde_json::from_value::<WatchSettings>(value.clone())
                .map(|settings| settings.enabled)
                .map_err(|_| error("invalid config_watch declaration"))
        })
        .unwrap_or(Ok(false))
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
                    stale_after: Some(Duration::from_secs(45)),
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
            let raw = YmlLoader::standard()
                .load_tree()
                .map_err(|_| error("cannot read initial watched configuration"))?;
            let watcher = LocalWatch::new(&raw)?;
            let app = context.application().clone();
            let publisher = crate::config_reload::CandidatePublisher::new(
                app.clone(),
                self.components.clone(),
                app.config().value().get("application").cloned(),
            );
            let cancel = CancellationToken::new();
            context.activate(Box::new(WatchShutdown(cancel.clone())));
            health.observe(DependencyState::Ready, reason::HEALTHY, Instant::now());
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

/// 业务作用：按单个合并信号和固定补读间隔处理本地候选，不为事件风暴无限延迟刷新。
/// 参数说明：`app` 为宿主；`watcher` 为当前观察；`publisher` 保管实际投影；`health` 汇报结果；`cancel` 关闭驱动。
/// 返回：取消后退出；坏候选只降级并保留旧态，下一固定补读可恢复新增文件依赖。
async fn run(
    app: Application,
    mut watcher: LocalWatch,
    mut publisher: crate::config_reload::CandidatePublisher,
    health: ReadinessContributor,
    cancel: CancellationToken,
) -> ApplicationResult<()> {
    let mut poll = tokio::time::interval(Duration::from_secs(15));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let signal = watcher.signal.clone();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = app.cancellation_token().cancelled_owned() => return Ok(()),
            _ = poll.tick() => {},
            _ = signal.notified() => {},
        }
        // 固定短窗口合并频繁 rename；不随新事件重置，因此始终有机会处理最新候选。
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
        let result = async {
            let raw = YmlLoader::standard()
                .load_tree()
                .map_err(|_| error("cannot read watched configuration"))?;
            let prepared_watch = watcher.prepare(&raw)?;
            let published = publisher.publish(raw, Vec::new(), &cancel).await?;
            watcher.install(prepared_watch, published.is_some());
            Ok::<_, ApplicationError>(())
        }
        .await;
        health.observe(
            if result.is_ok() {
                DependencyState::Ready
            } else {
                DependencyState::Degraded
            },
            if result.is_ok() {
                reason::HEALTHY
            } else {
                reason::DEGRADED
            },
            Instant::now(),
        );
        if let Err(error) = result {
            tracing::warn!(reason = error.message(), "configuration candidate rejected");
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
