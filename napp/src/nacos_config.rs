use std::{sync::Arc, time::Duration};

use config_boot::{
    connect_config_client, nacos_refs_for_bootstrap, resolve_imports,
    resolve_ordered_overlays_for_bootstrap, validate_imports_enabled, NacosBootstrap,
};
use naml::{YmlImport, YmlLoader};
use nanacos::{ConfigBundle, MultiWatchGuard, NacosConfigClient};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, BootstrapContext, ComponentId, ConfigSource, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

/// 配置中心主动 freshness 探针间隔；同时兜底补拉可能漏掉的 watch 事件。
const NACOS_CONFIG_PROBE_INTERVAL: Duration = Duration::from_secs(15);
/// 三个探针周期没有任何观测时，readiness 将当前 last-good 标为 stale。
const NACOS_CONFIG_STALE_AFTER: Duration = Duration::from_secs(45);

/// Nacos 配置中心组件：合并远端 overlay 生成最终配置树，并热刷新到订阅者。
///
/// 只负责配置中心；服务注册由独立的 `nacos-discovery` 组件负责。`enabled=false` 时本地树即
/// 最终配置，组件为纯校验空操作；`enabled=true` 时在 Bootstrap 合并并重发布 version=1，在 Ready 启动
/// watch driver 作为受监督关键任务。
pub(crate) struct NacosConfigComponent {
    /// 已声明组件，用于首拉校验和后续目标热应用状态登记。
    components: Vec<ComponentId>,
    /// preflight 固定的原始 application 段，不允许首拉覆盖运行时启动约束。
    pinned_application: Option<Value>,
    /// Bootstrap 首拉成功后持有、Ready 建立 watch 时取走的连接上下文。
    connection: Option<NacosConnection>,
    /// 准备完成后向 Runner 一次性移交的配置驱动任务。
    critical_task: Option<ApplicationFuture<'static>>,
    /// enabled+connected 时 Start(封口前)登记、Ready observe 的配置中心就绪贡献句柄。
    /// 首拉在 Bootstrap 已成功(否则 fail-closed 不启动),watch 在 Ready 启动后 observe Ready。
    readiness_contributor: Option<ReadinessContributor>,
}

/// Bootstrap 建立、供 Ready 启动 watch 的配置中心连接上下文。
struct NacosConnection {
    /// 首拉使用的共享客户端，Ready 继续用于监听和定期补拉。
    client: Arc<NacosConfigClient>,
    /// 启动阶段解析的导入顺序，供非严格来源按相同覆盖关系重建配置。
    imports: Vec<YmlImport>,
    /// 启动时固定的连接与引用解析规则，在线候选不替换此连接身份。
    boot: NacosBootstrap,
}

/// 启动阶段为跨 feature `Send` 边界临时 spawn 的任务；阶段 future 一旦被取消，立即 abort，
/// 不能采用 JoinHandle 普通 Drop 的 detached 语义。
struct AbortOnDropTask<T> {
    /// 必须随阶段取消请求终止的任务，避免句柄直接释放后任务脱离管理。
    handle: JoinHandle<T>,
}

impl<T> AbortOnDropTask<T> {
    /// 业务作用：包装一个必须由当前生命周期阶段负责 join 或 abort 的任务。
    ///
    /// 参数说明：
    /// - `handle`：已经启动的任务，交由此 guard 在等待结束或阶段取消时管理。
    ///
    /// 返回：持有任务句柄的 guard，不创建新任务或等待当前任务完成。
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle,
        }
    }

    /// 业务作用：等待阶段拥有的任务完成，并在等待被取消时保留终止任务的责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：任务正常结束时交还其输出，任务 panic 或被取消时返回 JoinError。
    /// guard 随等待结束或取消而析构并调用 abort；已完成任务不受该调用影响。
    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    /// 业务作用：guard 释放时请求终止尚未结束的任务，避免阶段取消后任务脱离宿主继续运行。
    ///
    /// 参数说明：无。
    ///
    /// 返回：同步发出 abort 请求，不等待任务实际结束；不能抢占不让出执行权的同步代码。
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl NacosConfigComponent {
    /// 业务作用：创建尚未连接配置中心的组件。
    ///
    /// 参数说明：
    ///
    /// - `components`：声明的组件列表，用于热刷新时构造同版本 reload 状态表。
    /// - `pinned_application`：同步 preflight 预读到的原始 `application.*`，作为 bootstrap-only 判定基准。
    ///
    /// 返回：尚无连接、watch 任务或就绪贡献项的组件；保存输入供后续生命周期使用。
    pub(crate) fn new(components: Vec<ComponentId>, pinned_application: Option<Value>) -> Self {
        Self {
            components,
            pinned_application,
            connection: None,
            critical_task: None,
            readiness_contributor: None,
        }
    }
}

impl ApplicationComponent for NacosConfigComponent {
    /// 业务作用：返回配置中心组件稳定身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：NacosConfig 身份，供 Runner 归类错误和生命周期顺序，不表示配置中心当前可用。
    fn id(&self) -> ComponentId {
        ComponentId::NacosConfig
    }

    /// 业务作用：读取本地 `nacos` 段；enabled=true 时连接、合并并重发布最终配置树。
    ///
    /// 参数说明：
    ///
    /// - `context`：提供本地初始配置视图的 Bootstrap 上下文。
    ///
    /// 返回：禁用时发布禁用状态并保留本地树；启用时在首拉、合并与校验成功后先发布配置，再发布客户端。
    /// 来源冲突、连接或配置失败阻止启动；配置发布后的后续失败由宿主停止启动，不承诺恢复旧快照。
    fn bootstrap<'a>(&'a mut self, context: &'a mut BootstrapContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let boot = read_nacos_bootstrap(context.application())?;
            if !boot.enabled {
                // 本地树已经是 preflight 写入的 version=1 最终配置，配置中心组件无需动作。
                if !context
                    .application()
                    .nacos_config_runtime()
                    .publish_disabled()
                {
                    return Err(nacos_error(
                        ApplicationPhase::Bootstrap,
                        "config center capability state was already published",
                    ));
                }
                return Ok(());
            }

            let strict_source = context.application().strict_source();
            let (local_tree, imports, merged, client) = if let Some(source) = &strict_source {
                let plan = source.prepare(true)?;
                let imports = plan.imports().to_vec();
                let local_tree = context.application().config().value().clone();
                let client = connect_config_client(&boot).await.map_err(|_| {
                    nacos_error(
                        ApplicationPhase::Bootstrap,
                        "cannot connect strict config source",
                    )
                })?;
                let loaded = config_boot::strict::load(plan, &client, &boot)
                    .await
                    .map_err(|_| {
                        nacos_error(
                            ApplicationPhase::Bootstrap,
                            "strict remote configuration rejected",
                        )
                    })?;
                (local_tree, imports, source.accept(loaded), client)
            } else {
                let loader = YmlLoader::standard();
                let local_tree = loader.load_tree().map_err(|error| {
                    nacos_error_src(
                        ApplicationPhase::Bootstrap,
                        "cannot reload local config tree",
                        error,
                    )
                })?;
                // application 段必须与预读的 JSON 值一致，避免运行时参数与配置快照使用不同启动约束。
                if local_tree.get("application") != self.pinned_application.as_ref() {
                    return Err(ApplicationError::new(
                        ComponentId::NacosConfig,
                        ApplicationPhase::Bootstrap,
                        "local `application.*` changed between preflight and bootstrap; \
                     restart the process instead of running with two different bootstrap values",
                    ));
                }
                let base_dir = loader.base_file_dir().to_path_buf();
                let imports = resolve_imports(&local_tree, &base_dir, &boot).map_err(|error| {
                    nacos_error_src(
                        ApplicationPhase::Bootstrap,
                        "cannot resolve nacos imports",
                        error,
                    )
                })?;
                validate_imports_enabled(&boot, &imports).map_err(|error| {
                    nacos_error_src(
                        ApplicationPhase::Bootstrap,
                        "invalid nacos import declaration",
                        error,
                    )
                })?;
                let client = connect_config_client(&boot).await.map_err(|error| {
                nacos_error_src(
                    ApplicationPhase::Bootstrap,
                    "cannot connect nacos config center (enable nasa feature `nacos-sdk` for the real backend, or set nacos.enabled=false)",
                    error,
                )
            })?;
                let overlays = resolve_ordered_overlays_for_bootstrap(&client, &imports, &boot)
                    .await
                    .map_err(|error| {
                        nacos_error_src(
                            ApplicationPhase::Bootstrap,
                            "cannot pull nacos overlays",
                            error,
                        )
                    })?;
                let merged = loader.load_tree_with_overlays(&overlays).map_err(|error| {
                    nacos_error_src(
                        ApplicationPhase::Bootstrap,
                        "cannot merge nacos overlays",
                        error,
                    )
                })?;

                (local_tree, imports, merged, client)
            };

            // 连接配置中心之前必须已有独立 provider 信任根；远端 overlay 不能补造其自身认证的前置依赖。
            if merged.get("secret_providers") != local_tree.get("secret_providers") {
                return Err(nacos_error(
                    ApplicationPhase::Bootstrap,
                    "secret provider bootstrap declarations must come from local configuration",
                ));
            }

            // bootstrap-only 冲突：远端首拉不允许改写 application.*。比较基准是同步预读固定的
            // 原始 section，而不是刚刚重新读到的本地树——否则本地文件被同时改动就能绕过该判定。
            // 该比较必须早于任何组件段校验，未知字段的改写才不会先被 serde 错误遮蔽。
            if merged.get("application") != self.pinned_application.as_ref() {
                return Err(ApplicationError::new(
                    ComponentId::NacosConfig,
                    ApplicationPhase::Bootstrap,
                    "bootstrap-only configuration conflict: nacos overlays changed `application.*`",
                ));
            }
            crate::sections::validate_declared_sections(
                &self.components,
                &merged,
                ApplicationPhase::Bootstrap,
            )?;

            context
                .application()
                .set_bootstrap_config(merged, nacos_sources(&imports, &boot))
                .await?;
            if let Some(source) = context.application().strict_source() {
                source.commit_observation();
            }
            let client = Arc::new(client);
            if !context
                .application()
                .nacos_config_runtime()
                .publish_client(&client)
            {
                return Err(nacos_error(
                    ApplicationPhase::Bootstrap,
                    "config center capability state was already published",
                ));
            }
            self.connection = Some(NacosConnection {
                client,
                imports,
                boot,
            });
            Ok(())
        })
    }

    /// 业务作用：enabled+connected(Bootstrap 已成功首拉)时登记配置中心就绪 contributor。
    ///
    /// 必须在 UserHook 封口前(Start)登记;首拉失败已在 Bootstrap fail-closed,故走到这里即首拉成功。
    /// Ready 启动 watch 后再 observe Ready。`enabled=false`(本地树)无远端可监控,不登记。
    ///
    /// 参数说明：
    ///
    /// - `context`：提供 Application(据此登记 readiness)的 Start 上下文。
    ///
    /// 返回：有连接时登记仅供观测的 freshness 贡献项，登记失败返回错误；无连接时不登记。
    /// 运行期短暂失败标记降级，45 秒无观测可变为 stale，但此贡献项不阻断应用整体就绪。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            if self.connection.is_some() {
                let contributor = context.application().register_readiness(
                    ComponentId::NacosConfig,
                    Arc::<str>::from("nacos-config:default"),
                    // 首拉是关键(失败即 fail-closed 不启动)；运行期短暂探针失败保留 last-good 并降级。
                    ReadinessPolicy {
                        affects_ready: false,
                        failure_threshold: 1,
                        recovery_threshold: 1,
                        stale_after: Some(NACOS_CONFIG_STALE_AFTER),
                    },
                )?;
                self.readiness_contributor = Some(contributor);
            }
            Ok(())
        })
    }

    /// 业务作用：enabled 时启动 watch driver 作为受监督关键任务，并登记 guard 关闭动作。
    ///
    /// 参数说明：
    ///
    /// - `context`：提供 Application 与 action 激活能力的 Ready 上下文。
    ///
    /// 返回：无连接时直接完成；有连接时建立 watch、记录首次健康事实、保存 driver 并登记停机动作。
    /// 建立、就绪登记或本地监听准备失败则拒绝 Ready；driver 仍须由 Runner 取出并调度。
    /// 阶段等待取消时请求终止建立任务，不把终止请求当作已经等待其退出。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let Some(connection) = self.connection.take() else {
                return Ok(());
            };
            let refs = nacos_refs_for_bootstrap(&connection.imports, &connection.boot).map_err(
                |error| {
                    nacos_error_src(
                        ApplicationPhase::Ready,
                        "cannot build nacos watch refs",
                        error,
                    )
                },
            )?;
            let NacosConnection {
                client,
                imports,
                boot,
            } = connection;
            // 建立 watch 的 future 内部持有 `&NacosConfigClient`，而真实 SDK 后端下它只对**具体**生命周期
            // 实现 `Send`。组件是 trait object，其阶段 future 必须 `for<'a> Send`，直接 await 会编译失败
            // （只有开 `nacos-config-sdk` 才暴露）。放进一个用**拥有的** client 的 `'static` 任务里完成建立，
            // 再把句柄取回。AbortOnDropTask 保证 Ready future 被启动 deadline 取消时任务也随之中止，
            // 不会因 JoinHandle 的默认 detach 语义带着 client/watch 在回滚后继续运行。
            let watch_refs = refs.clone();
            let started = AbortOnDropTask::new(tokio::spawn(async move {
                let (guard, receiver) = client.watch_many_channel(watch_refs).await?;
                Ok::<_, anyhow::Error>((guard, receiver, client))
            }))
            .join()
            .await
            .map_err(|error| {
                nacos_error_src(
                    ApplicationPhase::Ready,
                    "nacos config watch setup task failed",
                    error,
                )
            })?;
            let (guard, receiver, client) = started.map_err(|error| {
                nacos_error_src(
                    ApplicationPhase::Ready,
                    "cannot start nacos config watch",
                    error,
                )
            })?;
            let contributor = self.readiness_contributor.take().ok_or_else(|| {
                nacos_error(
                    ApplicationPhase::Ready,
                    "nacos config readiness contributor was not registered",
                )
            })?;
            contributor.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            );

            let cancel = CancellationToken::new();
            let publisher = crate::config_reload::CandidatePublisher::new(
                context.application().clone(),
                self.components.clone(),
                self.pinned_application.clone(),
            );
            let driver = WatchDriver {
                receiver,
                source: context.application().strict_source(),
                client: Arc::clone(&client),
                refs,

                imports,
                boot,
                publisher,
                #[cfg(feature = "config-watch")]
                local: crate::config_watch::LocalWatch::new(
                    context.application().config().value().clone(),
                    context.application().strict_source(),
                )
                .await?,
                readiness: contributor,
                cancel: cancel.clone(),
            };
            self.critical_task = Some(Box::pin(driver.run()));
            // guard 与 client 由 shutdown action 持有到停机；先压栈再交任务，保证可回滚。
            context.activate(Box::new(NacosWatchShutdown {
                guard: Some(guard),
                _client: client,
                cancel,
            }));
            Ok(())
        })
    }

    /// 业务作用：把 watch driver 交给 Runner 关键任务监督集合。
    ///
    /// 参数说明：无。
    ///
    /// 返回：首次取出已准备任务时返回固定名称和 future；禁用、未准备或已取出时为 None。
    /// 取出动作不启动任务，由 Runner 负责调度并判断退出是否符合生命周期预期。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("nacos-config-watch", task))
    }
}

/// 持有 watch guard 与配置 client 的停机动作，不负责回退已发布配置。
struct NacosWatchShutdown {
    /// 关闭时一次性取走的监听集合；释放 guard 仍按底层契约执行尽力清理。
    guard: Option<MultiWatchGuard>,
    /// 保持配置客户端可用直至监听关闭动作释放。
    _client: Arc<NacosConfigClient>,
    /// driver 的停止信号，发出取消不等于已等待 driver 退出。
    cancel: CancellationToken,
}

impl ShutdownAction for NacosWatchShutdown {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：固定的 nacos-config-watch 名称，不包含文档名称、配置值或连接凭据。
    fn label(&self) -> &'static str {
        "nacos-config-watch"
    }

    /// 业务作用：同步通知 driver 停止，再移交 watch guard 的显式关闭过程。
    ///
    /// 参数说明：
    ///
    /// - `_context`：宿主提供的共享停机上下文，本函数未直接读取其截止点，外层负责限制等待。
    ///
    /// 返回：guard 已取走或 close 成功时完成，close 返回错误则归类为停机错误。
    /// 当前 MultiWatchGuard 对子监听器注销失败仅记日志，因此成功不证明全部远端注销成功；此处也不 join driver。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.cancel.cancel();
        Box::pin(async move {
            if let Some(guard) = self.guard.take() {
                guard.close().await.map_err(|error| {
                    nacos_error_src(
                        ApplicationPhase::Stopping,
                        "nacos watch guard close failed",
                        error,
                    )
                })?;
            }
            Ok(())
        })
    }
}

/// 一次 watch 循环运行所需的全部拥有状态。
struct WatchDriver {
    /// 可选严格来源，负责候选准备、来源复验与接受后的观察记录。
    source: Option<Arc<crate::config_source::StrictSource>>,
    /// 仅保留最新全量 bundle 的通知通道，不保存完整变更历史。
    receiver: watch::Receiver<ConfigBundle>,
    /// 主动 freshness 探针与漏事件补拉使用的同一个受管 client。
    client: Arc<NacosConfigClient>,
    /// 与 watch 注册完全相同、顺序固定的配置引用。
    refs: Vec<nanacos::ConfigRef>,
    /// 本地文件变化信号及监听安装状态，和远端候选共用同一发布流程。
    #[cfg(feature = "config-watch")]
    local: crate::config_watch::LocalWatch,
    /// 非严格来源重建 overlay 时使用的固定导入顺序。
    imports: Vec<YmlImport>,
    /// 启动固定的 Nacos 引用与加载规则。
    boot: NacosBootstrap,
    /// 串行验证、准备、安装和发布候选的入口，维护各目标最后成功状态。
    publisher: crate::config_reload::CandidatePublisher,
    /// 运行期 last-good freshness 贡献项。
    readiness: ReadinessContributor,
    /// 停止驱动循环及候选准备的信号，不抢占已经开始的同步安装。
    cancel: CancellationToken,
}

impl WatchDriver {
    /// 业务作用：结合远端通知、定期全量补拉与可选本地通知，串行提交候选配置并报告 freshness。
    ///
    /// 参数说明：无。
    ///
    /// 返回：取消或通知通道关闭时正常返回，由 Runner 按应用阶段判断关键任务退出是否异常。
    /// 拉取或候选失败记录降级并继续处理；发布是否成功由 publisher 裁决，无变化不增加版本。
    /// watch 只保存最新 bundle，不能作为逐次变更日志；已进入的拉取或发布不会被外层 select 立即抢占。
    async fn run(mut self) -> ApplicationResult<()> {
        let mut probe = tokio::time::interval(NACOS_CONFIG_PROBE_INTERVAL);
        probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // watch 建立过程已经完成一次全量拉取；跳过 interval 的立即首 tick，避免 Ready 后重复请求。
        probe.tick().await;
        #[cfg(feature = "config-watch")]
        let mut local_poll = crate::ConfigPoller::new(self.local.interval_ms)?;
        #[cfg(feature = "config-watch")]
        local_poll.tick().await;
        let mut latest_bundle = self.receiver.borrow().clone();
        loop {
            #[cfg(feature = "config-watch")]
            let local_signal = self.local.signal.clone();
            let local_event = async {
                #[cfg(feature = "config-watch")]
                tokio::select! {
                    // 本地观察关闭后积压通知也失去加载资格；远端通知和 freshness 补拉仍由外层驱动负责。
                    _ = local_signal.notified(), if self.local.enabled => {},
                    _ = local_poll.tick(), if self.local.enabled => {},
                    else => std::future::pending::<()>().await,
                }
                #[cfg(not(feature = "config-watch"))]
                std::future::pending::<()>().await;
            };
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    self.readiness.observe(
                        DependencyState::NotReady,
                        reason::NOT_READY,
                        std::time::Instant::now(),
                    );
                    return Ok(());
                },
                _ = local_event => {
                    let result = self.reload_once(&latest_bundle).await;
                    self.readiness.observe(
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
                        std::time::Instant::now(),
                    );
                    // 固定短间隔限制事件风暴，远端和定期补拉在下一轮公平竞争。
                    tokio::select! {
                        _ = self.cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                    }
                },
                _ = probe.tick() => {
                    let now = std::time::Instant::now();
                    match self.client.fetch_many(&self.refs).await {
                        Ok(bundle) => {
                            latest_bundle = bundle.clone();
                            match self.reload_once(&bundle).await {
                                Ok(Some(version)) => {
                                    tracing::info!(
                                        "nacos config freshness probe hot-reloaded version {version}"
                                    );
                                    self.readiness.observe(DependencyState::Ready, reason::HEALTHY, now);
                                }
                                Ok(None) => {
                                    self.readiness.observe(DependencyState::Ready, reason::HEALTHY, now);
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        "nacos config freshness candidate rejected, keeping previous snapshot: {}",
                                        error.message()
                                    );
                                    self.readiness.observe(
                                        DependencyState::Degraded,
                                        reason::DEGRADED,
                                        now,
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                "nacos config freshness probe failed, keeping previous snapshot: {error}"
                            );
                            self.readiness.observe(
                                DependencyState::Degraded,
                                reason::DEGRADED,
                                now,
                            );
                        }
                    }
                },
                changed = self.receiver.changed() => {
                    if changed.is_err() {
                        // sender 关闭：正常 shutdown 会先 cancel；此处返回由 reap 时的 group 状态决定归类。
                        self.readiness.observe(
                            DependencyState::NotReady,
                            reason::NOT_READY,
                            std::time::Instant::now(),
                        );
                        return Ok(());
                    }
                    let bundle = self.receiver.borrow_and_update().clone();
                    latest_bundle = bundle.clone();
                    let now = std::time::Instant::now();
                    match self.reload_once(&bundle).await {
                        Ok(Some(version)) => {
                            tracing::info!("nacos config hot-reloaded to version {version}");
                            self.readiness.observe(DependencyState::Ready, reason::HEALTHY, now);
                        }
                        Ok(None) => {
                            tracing::debug!(
                                "nacos config bundle re-pulled without changes; keeping current version"
                            );
                            self.readiness.observe(DependencyState::Ready, reason::HEALTHY, now);
                        }
                        Err(error) => {
                            tracing::warn!(
                                "nacos config reload rejected, keeping previous snapshot: {}",
                                error.message()
                            );
                            self.readiness.observe(
                                DependencyState::Degraded,
                                reason::DEGRADED,
                                now,
                            );
                        }
                    }
                }
            }
            #[cfg(feature = "config-watch")]
            local_poll.set_interval(self.local.interval_ms)?;
        }
    }

    /// 业务作用：对一帧 bundle 执行合并、校验并按需发布新配置版本。
    ///
    /// 参数说明：
    /// - `bundle`：本轮全量重拉的逐文档原文。
    ///
    /// 返回：整批提交新配置时为 Some(version)，内容与秘密未变化时为 None；取消时拒绝候选。
    /// 合并、校验、准备或安装失败均不发布候选；不支持热应用的字段要求重启且保留当前完整配置。
    /// publisher 完成整批提交后才更新本地监听和来源观察。
    async fn reload_once(&mut self, bundle: &ConfigBundle) -> ApplicationResult<Option<u64>> {
        let merged = if let Some(source) = &self.source {
            let source = source.clone();
            let bundle = bundle.clone();
            let boot = self.boot.clone();
            crate::read_config_candidate(move || {
                let plan = source.prepare(false)?;
                let loaded = config_boot::strict::assemble(plan, &bundle, &boot).map_err(|_| {
                    nacos_error(
                        ApplicationPhase::Running,
                        "strict configuration candidate rejected",
                    )
                })?;
                Ok::<_, crate::ApplicationError>(source.accept(loaded))
            })
            .await??
        } else {
            let overlays = config_boot::assemble_overlays_from_bundle_for_bootstrap(
                &self.imports,
                bundle,
                &self.boot,
            )
            .await
            .map_err(|error| {
                nacos_error_src(
                    ApplicationPhase::Running,
                    "cannot assemble nacos overlays",
                    error,
                )
            })?;
            crate::read_config_candidate(move || {
                YmlLoader::standard()
                    .load_tree_with_overlays(&overlays)
                    .map_err(|error| {
                        nacos_error_src(
                            ApplicationPhase::Running,
                            "cannot merge nacos overlays",
                            error,
                        )
                    })
            })
            .await??
        };
        #[cfg(feature = "config-watch")]
        let watch = self.local.prepare(&merged).await?;
        let published = self
            .publisher
            .publish(
                merged,
                nacos_sources(&self.imports, &self.boot),
                &self.cancel,
            )
            .await;
        #[cfg(feature = "config-watch")]
        let published = self.local.finish(watch, published).await?;
        #[cfg(not(feature = "config-watch"))]
        let published = published?;
        if let Some(source) = &self.source {
            source.commit_observation();
        }
        Ok(published)
    }
}

/// 业务作用：读取本地 `nacos` 段；段缺失时返回 `enabled=false` 的默认引导配置。
///
/// 参数说明：
///
/// - `application`：提供当前不可变配置快照的共享上下文。
///
/// 返回：缺省段使用禁用配置；启用时从独立秘密来源解析用户名和密码中的 secret:// 引用。
/// 结构非法、秘密缺失或秘密不是 UTF-8 文本时拒绝；不建立连接，不允许远端配置补造自身信任根。
fn read_nacos_bootstrap(application: &Application) -> ApplicationResult<NacosBootstrap> {
    let snapshot = application.config();
    if snapshot.value().get("nacos").is_none() {
        return naml::strict::bind(Value::Object(Default::default())).map_err(|error| {
            nacos_error_src(
                ApplicationPhase::Bootstrap,
                "cannot construct default nacos bootstrap",
                error,
            )
        });
    }
    let mut boot = snapshot.section::<NacosBootstrap>("nacos")?;
    if boot.enabled {
        let secrets = application.secrets();
        for value in [&mut boot.username, &mut boot.password] {
            if let Some(id) = value.strip_prefix("secret://") {
                let material = secrets.get(id).ok_or_else(|| {
                    nacos_error(
                        ApplicationPhase::Bootstrap,
                        "Nacos bootstrap credential is unavailable from independent secret roots",
                    )
                })?;
                *value = std::str::from_utf8(material.expose())
                    .map_err(|_| {
                        nacos_error(
                            ApplicationPhase::Bootstrap,
                            "Nacos bootstrap credential is not valid text",
                        )
                    })?
                    .to_owned();
            }
        }
    }
    Ok(boot)
}

/// 业务作用：从 import 列表派生本轮**远端全量重拉**所含文档的来源摘要。
///
/// 只包含 Nacos 文档：本地 base 文件不是可重拉来源，混进去会让订阅者误判本轮变更范围。
/// nanacos 的 bundle 也不提供“究竟哪个 dataId 触发”的信息，因此这里如实列出本轮包含的全部文档。
///
/// 参数说明：
///
/// - `imports`：已解析的配置导入列表。
/// - `boot`：提供默认分组和文件扩展名的引导配置。
///
/// 返回：本轮全部远端引用的 data_id 与 group 摘要，不识别实际触发变化的单一文档。
/// 引用解析失败时返回空列表，调用方须在前置准备阶段完成有效性校验，不能把空列表当作配置合法的证据。
fn nacos_sources(imports: &[YmlImport], boot: &NacosBootstrap) -> Vec<ConfigSource> {
    let Ok(refs) = nacos_refs_for_bootstrap(imports, boot) else {
        return Vec::new();
    };
    refs.into_iter()
        .map(|config_ref| {
            ConfigSource::nacos(
                Arc::from(config_ref.data_id.as_str()),
                config_ref.group.map(|group| Arc::from(group.as_str())),
            )
        })
        .collect()
}

/// 业务作用：在不建立任何连接的前提下校验候选配置树中的 `nacos` 段。
///
/// 供配置热刷新在发布候选前使用；段缺失等价于 `enabled=false`，只有非法结构才拒绝整帧。
///
/// 参数说明：
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
///
/// 返回：段缺失或能严格绑定为 NacosBootstrap 时成功；未知字段、类型或绑定约束不符时返回指定阶段错误。
/// 不验证远端可达性、账号权限或导入文档是否存在。
pub(crate) fn validate_nacos_section(
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("nacos") else {
        return Ok(());
    };
    naml::strict::bind::<NacosBootstrap>(section.clone())
        .map(|_| ())
        .map_err(|error| nacos_error_src(phase, "invalid `nacos` configuration section", error))
}

/// 业务作用：创建不携带底层错误链的配置中心生命周期错误。
///
/// 参数说明：
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含配置值和凭据的稳定摘要。
///
/// 返回：归属于 NacosConfig 及指定阶段的错误，不附加底层错误链，也不自动脱敏输入摘要。
fn nacos_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::NacosConfig, phase, message)
}

/// 业务作用：创建带底层错误链的配置中心错误。
///
/// 参数说明：
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含配置值和密码的稳定摘要。
/// - `source`：保留在错误链中的底层错误，调用方须避免引入凭据或敏感配置内容。
///
/// 返回：归属于 NacosConfig 及指定阶段的带来源错误；本函数原样保留原因，不执行脱敏或输出。
fn nacos_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::NacosConfig, phase, message, source)
}
