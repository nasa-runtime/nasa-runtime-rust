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
    reload::ConfigApplier, Application, ApplicationComponent, ApplicationError, ApplicationFuture,
    ApplicationPhase, ApplicationResult, BootstrapContext, ComponentId, ConfigSource, ConfigView,
    ReadyContext, ReloadStatus, ReloadTarget, ShutdownAction, ShutdownContext, StartContext,
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
    components: Vec<ComponentId>,
    pinned_application: Option<Value>,
    connection: Option<NacosConnection>,
    critical_task: Option<ApplicationFuture<'static>>,
    /// enabled+connected 时 Start(封口前)登记、Ready observe 的配置中心就绪贡献句柄。
    /// 首拉在 Bootstrap 已成功(否则 fail-closed 不启动),watch 在 Ready 启动后 observe Ready。
    readiness_contributor: Option<ReadinessContributor>,
}

/// Bootstrap 建立、供 Ready 启动 watch 的配置中心连接上下文。
struct NacosConnection {
    client: Arc<NacosConfigClient>,
    imports: Vec<YmlImport>,
    boot: NacosBootstrap,
}

/// 启动阶段为跨 feature `Send` 边界临时 spawn 的任务；阶段 future 一旦被取消，立即 abort，
/// 不能采用 JoinHandle 普通 Drop 的 detached 语义。
struct AbortOnDropTask<T> {
    handle: JoinHandle<T>,
}

impl<T> AbortOnDropTask<T> {
    /// 包装一个必须由当前生命周期阶段负责 join 或 abort 的任务。
    fn new(handle: JoinHandle<T>) -> Self {
        Self { handle }
    }

    /// 等待任务完成；消费 guard 后不会再触发 Drop abort。
    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    /// 阶段 future 取消时立即 abort，禁止 JoinHandle 默认 detach。
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl NacosConfigComponent {
    /// 创建尚未连接配置中心的组件。
    ///
    /// # 参数
    ///
    /// - `components`：声明的组件列表，用于热刷新时构造同版本 reload 状态表。
    /// - `pinned_application`：同步 preflight 预读到的原始 `application.*`，作为 bootstrap-only 判定基准。
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
    /// 返回配置中心组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 用它归类配置相关错误与顺序约束。
    fn id(&self) -> ComponentId {
        ComponentId::NacosConfig
    }

    /// 读取本地 `nacos` 段；enabled=true 时连接、合并并重发布最终配置树。
    ///
    /// # 参数
    ///
    /// - `context`：提供本地初始配置视图的 Bootstrap 上下文。
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

            let loader = YmlLoader::standard();
            let local_tree = loader.load_tree().map_err(|error| {
                nacos_error_src(
                    ApplicationPhase::Bootstrap,
                    "cannot reload local config tree",
                    error,
                )
            })?;
            // 二次读取必须与同步预读逐字节一致：否则 runtime 已按 A 创建、ConfigStore 却会记录 B。
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
                .set_bootstrap_config(merged, nacos_sources(&imports, &boot))?;
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

    /// enabled+connected(Bootstrap 已成功首拉)时登记配置中心就绪 contributor。
    ///
    /// 必须在 UserHook 封口前(Start)登记;首拉失败已在 Bootstrap fail-closed,故走到这里即首拉成功。
    /// Ready 启动 watch 后再 observe Ready。`enabled=false`(本地树)无远端可监控,不登记。
    ///
    /// # 参数
    ///
    /// - `context`：提供 Application(据此登记 readiness)的 Start 上下文。
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

    /// enabled 时启动 watch driver 作为受监督关键任务，并登记 guard 关闭动作。
    ///
    /// # 参数
    ///
    /// - `context`：提供 Application 与 action 激活能力的 Ready 上下文。
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
            let appliers = context
                .application()
                .config_appliers()
                .into_iter()
                .map(|applier| (applier.component(), applier))
                .collect();
            let driver = WatchDriver {
                receiver,
                client: Arc::clone(&client),
                refs,
                application: context.application().clone(),
                imports,
                boot,
                components: self.components.clone(),
                pinned_application: self.pinned_application.clone(),
                appliers,
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

    /// 把 watch driver 交给 Runner 关键任务监督集合。
    ///
    /// # 参数
    ///
    /// 本方法无参数；driver 只允许被取出一次。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("nacos-config-watch", task))
    }
}

/// 持有 watch guard 与配置 client 直到停机的可逆 action。
struct NacosWatchShutdown {
    guard: Option<MultiWatchGuard>,
    _client: Arc<NacosConfigClient>,
    cancel: CancellationToken,
}

impl ShutdownAction for NacosWatchShutdown {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不含配置值。
    fn label(&self) -> &'static str {
        "nacos-config-watch"
    }

    /// 先取消 driver，再显式关闭 MultiWatchGuard（Drop 只 abort，不算优雅关闭）。
    ///
    /// # 参数
    ///
    /// - `_context`：共享停机预算，本 action 自身耗时极短。
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
    receiver: watch::Receiver<ConfigBundle>,
    /// 主动 freshness 探针与漏事件补拉使用的同一个受管 client。
    client: Arc<NacosConfigClient>,
    /// 与 watch 注册完全相同、顺序固定的配置引用。
    refs: Vec<nanacos::ConfigRef>,
    application: Application,
    imports: Vec<YmlImport>,
    boot: NacosBootstrap,
    components: Vec<ComponentId>,
    pinned_application: Option<Value>,
    /// 可热刷组件在 Start 阶段登记的重应用句柄，按组件身份索引。
    ///
    /// 在 Ready 构造驱动时取一次快照即可：登记只发生在组件启动阶段，此后不再变化。
    appliers: std::collections::HashMap<ComponentId, Arc<dyn ConfigApplier>>,
    /// 运行期 last-good freshness 贡献项。
    readiness: ReadinessContributor,
    cancel: CancellationToken,
}

impl WatchDriver {
    /// 循环消费最新 bundle：合并失败保留旧快照，成功则发布新版本。
    ///
    /// 作为关键任务：取消时正常返回（停机阶段 reap 归类为预期）；receiver 意外关闭时返回
    /// 让 Runner 在 Running 阶段 reap 归类为故障。
    ///
    /// # 参数
    ///
    /// 本方法消费 self；内部持有的 receiver 与 Application 均为拥有值。
    async fn run(mut self) -> ApplicationResult<()> {
        let mut probe = tokio::time::interval(NACOS_CONFIG_PROBE_INTERVAL);
        probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // watch 建立过程已经完成一次全量拉取；跳过 interval 的立即首 tick，避免 Ready 后重复请求。
        probe.tick().await;
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    self.readiness.observe(
                        DependencyState::NotReady,
                        reason::NOT_READY,
                        std::time::Instant::now(),
                    );
                    return Ok(());
                },
                _ = probe.tick() => {
                    let now = std::time::Instant::now();
                    match self.client.fetch_many(&self.refs).await {
                        Ok(bundle) => {
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
        }
    }

    /// 对一帧 bundle 执行合并、校验并按需发布新配置版本。
    ///
    /// 返回 `None` 表示合并结果与当前快照逐字节相同、本帧未发布：注册 watch 时 SDK 会先投一次
    /// 初始通知，重连和无关文档变更也会触发全量重拉，若照单发布就会出现"版本号涨了但配置没变"的
    /// 噪声，令 `applied_version` 与快照版本的差值失去诊断意义。
    ///
    /// # 参数
    ///
    /// - `bundle`：本轮全量重拉的逐文档原文。
    async fn reload_once(&self, bundle: &ConfigBundle) -> ApplicationResult<Option<u64>> {
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
        let loader = YmlLoader::standard();
        let merged = loader.load_tree_with_overlays(&overlays).map_err(|error| {
            nacos_error_src(
                ApplicationPhase::Running,
                "cannot merge nacos overlays",
                error,
            )
        })?;
        // 先对候选整帧做内置组件段校验：任一段非法都不发布，旧快照继续有效。
        // `application` 段被有意排除——它 bootstrap-only，只做原始 section 比较并记 RestartRequired，
        // 否则远端新增/拼错 application 字段会因 deny_unknown_fields 把整帧候选一起否掉。
        crate::sections::validate_declared_sections(
            &self.components,
            &merged,
            ApplicationPhase::Running,
        )?;

        let current = self.application.config_view();
        // 无变化判断比较**原始**候选的私有 fingerprint,而不是拿原始 `merged` 与上一帧发布的
        // `<redacted>` 树直接比较——后者因脱敏差异每次 watch 都会误判有变、误增版本。
        if crate::secret::candidate_fingerprint(&merged) == current.candidate_fingerprint() {
            return Ok(None);
        }
        // 状态表必须与本次发布的版本号严格同版本，因此先算出候选版本再逐目标判定；
        // 可热刷组件的 apply 也发生在这一步——先 apply 后发布，
        // 订阅者看到新视图时，声明为 Applied 的运行态已经切换完成。
        let current_version = current.snapshot().version();
        let next_version = current_version.checked_add(1).ok_or_else(|| {
            nacos_error(
                ApplicationPhase::Running,
                "config snapshot version reached its maximum value",
            )
        })?;
        let statuses = self.apply_and_collect_statuses(&current, &merged, next_version);
        let version = self.application.publish_reloaded_config(
            current_version,
            merged,
            nacos_sources(&self.imports, &self.boot),
            statuses,
        )?;
        debug_assert_eq!(version, next_version);
        Ok(Some(version))
    }

    /// 对可热刷组件执行重应用，并构造与本次发布严格同版本的目标状态表。
    ///
    /// 判定规则：
    /// - `application.*`：bootstrap-only，与预读 pin 一致则推进 applied_version，被远端改写则记
    ///   `RestartRequired` 并保留原 applied_version——本次 runtime 不会改变应用名、模式或 deadline。
    /// - 各已声明组件：相关配置段未变化则推进 applied_version；变化且登记过重应用句柄则调用它——
    ///   成功记 `Applied`，失败保留 last-known-good 版本并记 `ApplyFailed`（早先成功的组件不回滚，
    ///   明确不是跨组件事务）；变化但没有句柄则如实记 `RestartRequired`。
    ///
    /// # 参数
    ///
    /// - `current`：当前已发布的同版本视图，提供各目标的上一次成功 apply 版本。
    /// - `candidate`：尚未发布的候选配置树。
    /// - `next_version`：本次即将发布的快照版本。
    fn apply_and_collect_statuses(
        &self,
        current: &ConfigView,
        candidate: &Value,
        next_version: u64,
    ) -> std::collections::HashMap<ReloadTarget, ReloadStatus> {
        let current_tree = current.snapshot().value();
        let mut statuses = std::collections::HashMap::new();

        let application_status = if candidate.get("application") == self.pinned_application.as_ref()
        {
            ReloadStatus::applied(next_version)
        } else {
            ReloadStatus::restart_required(
                applied_version_of(current, &ReloadTarget::Application),
                "remote overlays changed bootstrap-only `application.*`",
            )
        };
        statuses.insert(ReloadTarget::Application, application_status);

        for component in &self.components {
            let target = ReloadTarget::Component(*component);
            let changed = crate::sections::sections_changed(*component, current_tree, candidate);
            let status = if !changed {
                ReloadStatus::applied(next_version)
            } else if let Some(applier) = self.appliers.get(component) {
                match applier.apply(candidate) {
                    Ok(()) => {
                        tracing::info!(
                            "component `{component}` hot-applied config version {next_version}"
                        );
                        ReloadStatus::applied(next_version)
                    }
                    Err(error) => {
                        //：状态表里的 summary 与主报告使用同一 redactor；
                        // 运行态保留 last-known-good，applied_version 不推进。
                        let summary = crate::report::redact(&crate::report::error_chain(&error));
                        tracing::warn!(
                            "component `{component}` hot apply failed, keeping last-known-good: {summary}"
                        );
                        ReloadStatus::apply_failed(applied_version_of(current, &target), summary)
                    }
                }
            } else {
                ReloadStatus::restart_required(
                    applied_version_of(current, &target),
                    "component configuration changed but this runtime build cannot hot-apply it",
                )
            };
            statuses.insert(target, status);
        }
        statuses
    }
}

/// 读取某个目标在当前视图中最后一次成功 apply 的版本。
///
/// 未出现在当前状态表中的目标按初始版本 1 处理，保证 `RestartRequired` 一定携带可比较的版本号。
///
/// # 参数
///
/// - `current`：当前已发布的同版本配置视图。
/// - `target`：需要查询的配置应用目标。
fn applied_version_of(current: &ConfigView, target: &ReloadTarget) -> u64 {
    current
        .reload_statuses()
        .get(target)
        .map(|status| status.applied_version)
        .unwrap_or(1)
}

/// 读取本地 `nacos` 段；段缺失时返回 `enabled=false` 的默认引导配置。
///
/// # 参数
///
/// - `application`：提供当前不可变配置快照的共享上下文。
fn read_nacos_bootstrap(application: &Application) -> ApplicationResult<NacosBootstrap> {
    let snapshot = application.config();
    if snapshot.value().get("nacos").is_none() {
        return serde_json::from_value(Value::Object(Default::default())).map_err(|error| {
            nacos_error_src(
                ApplicationPhase::Bootstrap,
                "cannot construct default nacos bootstrap",
                error,
            )
        });
    }
    snapshot.section::<NacosBootstrap>("nacos")
}

/// 从 import 列表派生本轮**远端全量重拉**所含文档的来源摘要。
///
/// 只包含 Nacos 文档：本地 base 文件不是可重拉来源，混进去会让订阅者误判本轮变更范围。
/// nanacos 的 bundle 也不提供“究竟哪个 dataId 触发”的信息，因此这里如实列出本轮包含的全部文档。
///
/// # 参数
///
/// - `imports`：已解析的配置导入列表。
/// - `boot`：提供默认分组和文件扩展名的引导配置。
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

/// 在不建立任何连接的前提下校验候选配置树中的 `nacos` 段。
///
/// 供配置热刷新在发布候选前使用；段缺失等价于 `enabled=false`，只有非法结构才拒绝整帧。
///
/// # 参数
///
/// - `tree`：合并、插值完成但尚未发布的候选配置树。
/// - `phase`：本次无副作用校验所属的生命周期阶段。
pub(crate) fn validate_nacos_section(
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("nacos") else {
        return Ok(());
    };
    serde_json::from_value::<NacosBootstrap>(section.clone())
        .map(|_| ())
        .map_err(|error| nacos_error_src(phase, "invalid `nacos` configuration section", error))
}

/// 创建不携带底层错误链的配置中心生命周期错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含配置值和凭据的稳定摘要。
fn nacos_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::NacosConfig, phase, message)
}

/// 创建带底层错误链的配置中心错误。
///
/// # 参数
///
/// - `phase`：故障被观察到的生命周期阶段。
/// - `message`：不含配置值和密码的稳定摘要。
/// - `source`：只供诊断、输出前统一脱敏的底层错误。
fn nacos_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::NacosConfig, phase, message, source)
}
