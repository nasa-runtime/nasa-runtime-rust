//! 具名多注册中心的应用装配。监听器放行、本地资格与远端注册确认分别管理。

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
#[cfg(any(feature = "web", feature = "grpc"))]
use crate::Application;
use crate::{
    ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase, ApplicationResult,
    ApplicationState, ComponentId, ReadyContext, ShutdownAction, ShutdownContext, StartContext,
};
use nadisc_runtime::{
    assembly::{DiscoveryConfig, ListenerEndpoint},
    DiscoveryHandle, DiscoveryOwner, Eligibility, OwnerPhase, RegistrationState,
    RegistrationStatus,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::sync::watch;

/// 应用多注册中心的只读能力。目录不延长 owner 生命周期，也不授予关闭权限。
#[derive(Clone)]
pub struct DiscoveryDirectory {
    owner: Weak<DiscoveryOwner>,
    #[cfg(feature = "rest")]
    rest: Arc<Mutex<BTreeMap<String, Arc<rest_discovery::RestDiscoveryClient>>>>,
}

impl DiscoveryDirectory {
    /// 业务作用：按 qualifier 选择独立发现环境，省略时采用 primary 或兼容配置的显式默认引用。
    /// 参数说明：`qualifier` 是冻结连接目录中的精确名称。
    /// 返回：受管发现句柄；未知连接或 owner 已关闭时失败。
    pub fn client(&self, qualifier: Option<&str>) -> ApplicationResult<DiscoveryHandle> {
        self.owner
            .upgrade()
            .ok_or_else(|| error(ApplicationPhase::Running, "discovery owner is unavailable"))?
            .client(qualifier)
            .map_err(|_| {
                error(
                    ApplicationPhase::Running,
                    "discovery connection is unavailable",
                )
            })
    }

    /// 业务作用：查询注册事实供业务运维展示，不输出凭据和 metadata。
    /// 参数说明：无。
    /// 返回：资源仍存在时的状态集合，否则返回错误。
    pub fn registrations(&self) -> ApplicationResult<Vec<RegistrationStatus>> {
        Ok(self
            .owner
            .upgrade()
            .ok_or_else(|| error(ApplicationPhase::Running, "discovery owner is unavailable"))?
            .registrations())
    }

    /// 业务作用：建立直接注入 generated client 的动态 gRPC Service，连接与正文归同一 owner。
    /// 参数说明：`qualifier` 选择连接，`service` 选择绑定服务，`options` 冻结容量与 TLS 身份政策。
    /// 返回：无独立注册后台的调用器；clone 共享其连接池。
    #[cfg(feature = "grpc-discovery")]
    pub fn grpc(
        &self,
        qualifier: Option<&str>,
        service: &str,
        options: nagrpc::discovery::DiscoveryClientOptions,
    ) -> ApplicationResult<nagrpc::discovery::DiscoveryGrpcService> {
        nagrpc::discovery::DiscoveryGrpcService::new(self.client(qualifier)?, service, options)
            .map_err(|_| {
                error(
                    ApplicationPhase::Running,
                    "invalid gRPC discovery client configuration",
                )
            })
    }

    /// 业务作用：取得同一具名连接共享的 REST 调用器，避免每个业务对象重复创建后台资源。
    /// 参数说明：`qualifier` 选择连接，不跨环境自动回退。
    /// 返回：由 Application 停机阶段关闭的 REST 客户端；响应正文应使用 send_managed 接口。
    #[cfg(feature = "rest")]
    pub fn rest(
        &self,
        qualifier: Option<&str>,
    ) -> ApplicationResult<Arc<rest_discovery::RestDiscoveryClient>> {
        if qualifier.is_none() {
            if let Some(external) = self
                .rest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get("")
                .cloned()
            {
                return Ok(external);
            }
        }
        let handle = self.client(qualifier)?;
        self.rest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(handle.name())
            .cloned()
            .ok_or_else(|| {
                error(
                    ApplicationPhase::Running,
                    "REST discovery client is unavailable",
                )
            })
    }

    /// 业务作用：为声明式 REST 宏提供以 qualifier 选择的共享客户端集合。
    /// 参数说明：无。
    /// 返回：当前连接的调用能力及配置默认项；owner 已释放、已关闭或目录非法时拒绝，不新建连接池和监督任务。
    #[cfg(feature = "rest")]
    pub fn rest_clients(&self) -> ApplicationResult<Arc<rest_discovery::RestClientSet>> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| error(ApplicationPhase::Running, "discovery owner is unavailable"))?;
        // 纯外部 URL 目录没有具名项可逐项复验，也必须拒绝从已关闭的 owner 派生调用入口。
        if owner.phase() == OwnerPhase::Closed {
            return Err(error(
                ApplicationPhase::Running,
                "discovery owner is closed",
            ));
        }
        let clients = self
            .rest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(external) = clients.get("") {
            return Ok(Arc::new(rest_discovery::RestClientSet::from_client(
                external.clone(),
            )));
        }
        // 目录快照只共享原客户端；逐项复验 owner 生命周期，不能从已关闭的目录生成新的调用入口。
        for qualifier in clients.keys() {
            owner.client(Some(qualifier)).map_err(|_| {
                error(
                    ApplicationPhase::Running,
                    "discovery connection is unavailable",
                )
            })?;
        }
        let default = owner
            .observer()
            .snapshot()
            .and_then(|snapshot| snapshot.default_connection);
        rest_discovery::RestClientSet::new(clients, default.as_deref())
            .map(Arc::new)
            .map_err(|_| error(ApplicationPhase::Running, "invalid REST qualifier set"))
    }
}

struct Endpoints {
    sealed: bool,
    listeners: BTreeMap<String, ListenerEndpoint>,
    eligibility: BTreeMap<String, Eligibility>,
}

pub(crate) struct RuntimeState {
    directory: OnceLock<DiscoveryDirectory>,
    endpoints: Mutex<Endpoints>,
    pub(crate) startup: watch::Sender<bool>,
    qualification: Mutex<BTreeMap<String, Eligibility>>,
}

impl RuntimeState {
    /// 业务作用：建立无网络活动的资源槽，默认不增加启动确认门禁。
    /// 参数说明：无。
    /// 返回：Start 才安装 owner，Ready 封存端点目录。
    pub(crate) fn new() -> Self {
        Self {
            directory: OnceLock::new(),
            endpoints: Mutex::new(Endpoints {
                sealed: false,
                listeners: BTreeMap::new(),
                eligibility: BTreeMap::new(),
            }),
            startup: watch::channel(true).0,
            qualification: Mutex::new(BTreeMap::new()),
        }
    }

    /// 业务作用：读取已经安装的受管目录。
    /// 参数说明：无。
    /// 返回：尚未安装时拒绝，不隐式构造另一个 owner。
    pub(crate) fn directory(&self) -> ApplicationResult<DiscoveryDirectory> {
        self.directory.get().cloned().ok_or_else(|| {
            error(
                ApplicationPhase::UserHook,
                "declare discovery before requesting its directory",
            )
        })
    }

    /// 业务作用：登记自定义入站端点，确保最终注册地址有本地资格来源。
    /// 参数说明：`name` 是唯一引用，`endpoint`、`eligibility` 属于同一个业务入口。
    /// 返回：目录未封存且名称合法唯一时成功。
    pub(crate) fn endpoint(
        &self,
        name: &str,
        endpoint: ListenerEndpoint,
        eligibility: Eligibility,
    ) -> ApplicationResult<()> {
        let mut endpoints = self
            .endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.directory.get().is_none()
            || endpoints.sealed
            || name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|ch| ch.is_ascii_alphanumeric() || b"_-".contains(&ch))
            || matches!(name, "web" | "grpc" | "saga-recovery")
            || endpoints.listeners.contains_key(name)
            || endpoint.port == 0
        {
            return Err(error(
                ApplicationPhase::UserHook,
                "discovery endpoint is invalid, duplicated or sealed",
            ));
        }
        endpoints.listeners.insert(name.into(), endpoint);
        endpoints.eligibility.insert(name.into(), eligibility);
        Ok(())
    }

    /// 业务作用：为独立资格探针读取注册目标绑定的本地事实。
    /// 参数说明：`registration` 是已经冻结的目标名称。
    /// 返回：目标存在且本地业务可接流才返回 true，不读取远端注册状态。
    #[cfg(feature = "web")]
    pub(crate) fn qualified(&self, registration: &str) -> bool {
        self.qualification
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(registration)
            .is_some_and(Eligibility::is_ready)
    }
}

mod metrics;

pub(crate) struct DiscoveryComponent {
    owner: Option<Arc<DiscoveryOwner>>,
    config: Option<DiscoveryConfig>,
    startup: Option<ReadinessContributor>,
    contributors: BTreeMap<String, ReadinessContributor>,
    task: Option<ApplicationFuture<'static>>,
}

impl DiscoveryComponent {
    /// 业务作用：建立尚未连接的中性装配组件。
    /// 参数说明：无。
    /// 返回：连接、注册和关闭权均在生命周期阶段内取得。
    pub(crate) fn new() -> Self {
        Self {
            owner: None,
            config: None,
            startup: None,
            contributors: BTreeMap::new(),
            task: None,
        }
    }
}

impl ApplicationComponent for DiscoveryComponent {
    /// 业务作用：标识中性服务发现组件。
    /// 参数说明：无。
    /// 返回：固定组件身份。
    fn id(&self) -> ComponentId {
        ComponentId::Discovery
    }

    /// 业务作用：先登记资源清理权，再在共享启动期限内准备出站发现连接。
    /// 参数说明：`context` 提供最终配置、启动预算和反向清理栈。
    /// 返回：所有启动必需连接准备完成；不发布入站实例。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_config(context.application().config().value())?;
            #[cfg(not(feature = "rest"))]
            if config
                .connections
                .values()
                .any(|connection| connection.rest.is_some())
            {
                return Err(error(
                    ApplicationPhase::Start,
                    "REST policy requires REST feature",
                ));
            }
            let owner = Arc::new(config.build().await.map_err(|_| {
                error(
                    ApplicationPhase::Start,
                    "cannot build discovery connections",
                )
            })?);
            let state = context.application().discovery_runtime();
            #[cfg(feature = "rest")]
            let rest = Arc::new(Mutex::new(BTreeMap::new()));
            #[cfg(feature = "rest")]
            let global = Arc::new(Mutex::new(None));
            context.activate(Box::new(RuntimeShutdown {
                owner: owner.clone(),
                #[cfg(feature = "rest")]
                rest: rest.clone(),
                #[cfg(feature = "rest")]
                global: global.clone(),
            }));
            state
                .directory
                .set(DiscoveryDirectory {
                    owner: Arc::downgrade(&owner),
                    #[cfg(feature = "rest")]
                    rest: rest.clone(),
                })
                .map_err(|_| {
                    error(
                        ApplicationPhase::Start,
                        "discovery owner was already installed",
                    )
                })?;
            if config.enabled {
                // 每个固定标签组合都预留 series，运行期间的服务输入不能扩大指标目录。
                context
                    .application()
                    .metrics_hub()
                    .register_legacy_source_reserved(
                        Arc::new(metrics::DiscoveryMetrics(owner.observer())),
                        6 + config.connections.len() * 7 + config.registrations.len() * 4,
                    )
                    .map_err(|_| {
                        error(
                            ApplicationPhase::Start,
                            "discovery metric capacity is unavailable",
                        )
                    })?;
                self.startup = Some(context.application().register_readiness(
                    ComponentId::Discovery,
                    "discovery:startup",
                    ReadinessPolicy::critical_immediate(),
                )?);
                state.startup.send_replace(false);
                for (name, registration) in &config.registrations {
                    let mut policy = ReadinessPolicy::critical_immediate();
                    policy.affects_ready = registration.readiness_critical;
                    let contributor = context.application().register_readiness(
                        ComponentId::Discovery,
                        format!("discovery:registration:{name}"),
                        policy,
                    )?;
                    self.contributors.insert(name.clone(), contributor);
                }
                #[cfg(feature = "rest")]
                for name in config.connections.keys() {
                    let handle = owner.client(Some(name)).map_err(|_| {
                        error(ApplicationPhase::Start, "unknown discovery connection")
                    })?;
                    let client = rest_discovery::RestDiscoveryClient::try_new(
                        Arc::new(handle),
                        rest_options(config.connections[name].rest.as_ref()),
                    )
                    .map_err(|_| {
                        error(
                            ApplicationPhase::Start,
                            "cannot construct REST discovery client",
                        )
                    })?;
                    rest.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(name.clone(), Arc::new(client));
                }
            }
            self.owner = Some(owner.clone());
            self.config = Some(config);
            owner.prepare(tokio::time::Instant::from_std(context.deadline())).await
                .map_err(|_| error(ApplicationPhase::Start, "required discovery connections did not prepare before the startup deadline"))?;
            #[cfg(feature = "rest")]
            {
                let default = owner.client(None).ok();
                // 有具名连接但没有 primary 时只开放显式选择，不能伪造外部默认项绕过缺省选择错误。
                if default.is_none()
                    && !self
                        .config
                        .as_ref()
                        .expect("discovery config is installed")
                        .connections
                        .is_empty()
                {
                    return Ok(());
                }
                let client = if let Some(handle) = &default {
                    rest.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(handle.name())
                        .cloned()
                        .ok_or_else(|| {
                            error(ApplicationPhase::Start, "default REST client is absent")
                        })?
                } else {
                    let client = Arc::new(
                        rest_discovery::RestDiscoveryClient::try_external_only(Default::default())
                            .map_err(|_| {
                                error(
                                    ApplicationPhase::Start,
                                    "external REST client could not be constructed",
                                )
                            })?,
                    );
                    rest.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(String::new(), client.clone());
                    client
                };
                let runtime = Arc::new(rest_discovery::RemoteRuntime::new(
                    client,
                    default.map(|handle| Arc::new(handle) as Arc<dyn nadisc::DiscoveryClient>),
                ));
                // 默认门面只有一个明确 owner；已有槽位不能被具名连接的安装顺序覆盖。
                rest_discovery::RestDiscovery::install_runtime(runtime.clone()).map_err(|_| {
                    error(
                        ApplicationPhase::Start,
                        "default REST runtime is already owned",
                    )
                })?;
                *global
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(runtime);
            }
            Ok(())
        })
    }

    /// 业务作用：以已绑定 listener 和独立本地资格冻结所有计划，外部注册等待统一放行。
    /// 参数说明：`context` 提供实际监听地址和原启动截止点。
    /// 返回：计划和摘流所有权一次性登记；任一非法计划阻止所有注册。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let owner = self.owner.clone().ok_or_else(|| {
                error(ApplicationPhase::Ready, "discovery owner was not prepared")
            })?;
            let config = self.config.take().ok_or_else(|| {
                error(
                    ApplicationPhase::Ready,
                    "discovery configuration is unavailable",
                )
            })?;
            let application = context.application().clone();
            let runtime = application.discovery_runtime();
            let mut endpoints = runtime
                .endpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            #[cfg(feature = "web")]
            if let Some(address) = application.web_addr() {
                endpoints.listeners.insert(
                    "web".into(),
                    ListenerEndpoint {
                        ip: address.ip().to_string(),
                        port: address.port(),
                    },
                );
                let app = application.clone();
                endpoints.eligibility.insert(
                    "web".into(),
                    Eligibility::from_check(move || local_ready(&app)),
                );
            }
            #[cfg(feature = "grpc")]
            if let Some(endpoint) = application.grpc_runtime().endpoint_registration() {
                endpoints.listeners.insert(
                    "grpc".into(),
                    ListenerEndpoint {
                        ip: endpoint.authority.unwrap_or_default(),
                        port: endpoint.port,
                    },
                );
                let app = application.clone();
                endpoints.eligibility.insert(
                    "grpc".into(),
                    Eligibility::from_check(move || {
                        local_ready(&app)
                            && app.grpc_runtime().observer().is_ok_and(|observer| {
                                matches!(observer.state(), nagrpc::GrpcServerState::Running)
                            })
                    }),
                );
            }
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            if config.registrations.values().any(|registration| {
                registration.eligibility_ref.as_deref() == Some("saga-recovery")
            }) {
                let app = application.clone();
                // 恢复入口不等待待续租的业务目录，但全局停机与 Saga 生命周期仍同步撤销资格。
                endpoints.eligibility.insert(
                    "saga-recovery".into(),
                    Eligibility::from_check(move || {
                        app.state() == ApplicationState::Ready
                            && app.saga_runtime().ensure_running().is_ok()
                    }),
                );
            }
            endpoints.sealed = true;
            let mut plans = config
                .resolve(&endpoints.listeners, &endpoints.eligibility)
                .map_err(|_| {
                    error(
                        ApplicationPhase::Ready,
                        "discovery registration endpoint or eligibility reference is invalid",
                    )
                })?;
            #[cfg(feature = "grpc")]
            let implicit_grpc_listener =
                endpoints.listeners.len() == 1 && endpoints.listeners.contains_key("grpc");
            drop(endpoints);
            #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
            let mut saga_projections = BTreeMap::new();
            for plan in &mut plans {
                if plan.plan.instance.metadata.keys().any(|key| {
                    key.starts_with("nasa.grpc.")
                        || key.starts_with("nasa.saga.")
                        || key.starts_with("nasa.discovery.")
                }) {
                    return Err(error(
                        ApplicationPhase::Ready,
                        "framework registration metadata is reserved",
                    ));
                }
                // 协议投影必须归属于计划选中的 listener；自定义端点不能继承另一入口的 TLS 与协议声明。
                #[cfg(feature = "grpc")]
                if config.registrations[&plan.plan.id].listener_ref.as_deref() == Some("grpc")
                    || (config.registrations[&plan.plan.id].listener_ref.is_none()
                        && implicit_grpc_listener)
                {
                    let endpoint = application
                        .grpc_runtime()
                        .endpoint_registration()
                        .ok_or_else(|| {
                            error(ApplicationPhase::Ready, "gRPC listener is unavailable")
                        })?;
                    plan.plan.instance.metadata.extend([
                        ("nasa.grpc.protocol".into(), "grpc".into()),
                        ("nasa.grpc.port".into(), plan.plan.instance.port.to_string()),
                        ("nasa.grpc.tls_mode".into(), endpoint.tls_mode.into()),
                        ("nasa.grpc.authority".into(), plan.plan.instance.ip.clone()),
                    ]);
                }
                #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
                for (key, value) in crate::saga::discovery_registration_metadata(
                    &application,
                    &plan.plan.instance.ip,
                    plan.plan.instance.port,
                    Some(
                        if plan
                            .plan
                            .instance
                            .metadata
                            .contains_key("nasa.grpc.protocol")
                        {
                            "grpc"
                        } else {
                            "http"
                        },
                    ),
                )? {
                    plan.plan.instance.metadata.insert(key, value);
                }
                #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
                if plan
                    .plan
                    .instance
                    .metadata
                    .contains_key("nasa.saga.incarnation")
                {
                    let key = owner
                        .service_key(&plan.connection_ref, &plan.plan.service)
                        .map_err(|_| {
                            error(
                                ApplicationPhase::Ready,
                                "Saga registration service scope is unavailable",
                            )
                        })?;
                    let projection: BTreeMap<_, _> = plan
                        .plan
                        .instance
                        .metadata
                        .iter()
                        .filter(|(key, _)| key.starts_with("nasa.saga."))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    // 同一 incarnation 在同一查询集合不能出现不同 origin；不同环境可各自映射可达端点。
                    if saga_projections
                        .insert(key, projection.clone())
                        .is_some_and(|previous| previous != projection)
                    {
                        return Err(error(
                            ApplicationPhase::Ready,
                            "Saga registration projections conflict in the same service scope",
                        ));
                    }
                }
                runtime
                    .qualification
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(plan.plan.id.clone(), plan.eligibility.clone());
            }
            owner.freeze_registrations(plans).map_err(|_| {
                error(
                    ApplicationPhase::Ready,
                    "discovery registration plans failed preflight",
                )
            })?;
            let cancel = tokio_util::sync::CancellationToken::new();
            context.activate(Box::new(RegistrationShutdown {
                owner: owner.clone(),
                cancel: cancel.clone(),
            }));
            let deadline = tokio::time::Instant::from_std(context.deadline());
            let startup = self.startup.take();
            let contributors = std::mem::take(&mut self.contributors);
            self.task = Some(Box::pin(async move {
                let mut phase = application.subscribe_state();
                while application.state() == ApplicationState::Starting {
                    phase.changed().await.map_err(|_| {
                        error(ApplicationPhase::Ready, "application startup state closed")
                    })?;
                }
                if application.state() != ApplicationState::Ready {
                    cancel.cancelled().await;
                    return Ok(());
                }
                // listener 已放行，但普通就绪仍由启动确认贡献项阻断；沿用原期限等待远端确认。
                owner.publish(deadline).await.map_err(|_| error(ApplicationPhase::Ready, "required discovery registrations were not confirmed before the startup deadline"))?;
                if let Some(startup) = startup {
                    startup.observe(
                        DependencyState::Ready,
                        reason::HEALTHY,
                        std::time::Instant::now(),
                    );
                }
                runtime.startup.send_replace(true);
                let mut changes = owner.changes();
                let cancellation = cancel;
                loop {
                    if matches!(
                        application.state(),
                        ApplicationState::Stopping
                            | ApplicationState::Stopped
                            | ApplicationState::Failed
                    ) {
                        cancellation.cancelled().await;
                        return Ok(());
                    }
                    if owner.phase() != OwnerPhase::Serving {
                        return Err(error(
                            ApplicationPhase::Running,
                            "discovery supervision is unavailable",
                        ));
                    }
                    for status in owner.registrations() {
                        if let Some(contributor) = contributors.get(&status.id) {
                            if status.is_ready() {
                                if let Some(until) = status.valid_until {
                                    contributor.observe_ready_until(until.into_std());
                                }
                            } else {
                                contributor.observe(
                                    DependencyState::NotReady,
                                    reason::NOT_READY,
                                    std::time::Instant::now(),
                                );
                            }
                        }
                    }
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(()),
                        _ = changes.changed() => {},
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                    }
                }
            }));
            Ok(())
        })
    }

    /// 业务作用：移交持续监督任务，使异常进入 Runner 的统一停机流程。
    /// 参数说明：无。
    /// 返回：只移交一次的任务，未装配时为空。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.task.take().map(|task| ("discovery-supervision", task))
    }
}

struct RegistrationShutdown {
    owner: Arc<DiscoveryOwner>,
    cancel: tokio_util::sync::CancellationToken,
}
impl ShutdownAction for RegistrationShutdown {
    /// 业务作用：提供固定的注册摘流动作名称。
    /// 参数说明：无。
    /// 返回：不含实例地址的标识。
    fn label(&self) -> &'static str {
        "discovery-registration"
    }
    /// 业务作用：先撤销注册许可并摘流，出站发现保留供业务排空。
    /// 参数说明：`context` 是全局停机预算。
    /// 返回：所有目标明确移除才成功，残留或未知结果进入停机摘要。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.cancel.cancel();
        let ticket = self
            .owner
            .begin_quiesce(tokio::time::Instant::from_std(context.deadline()));
        Box::pin(async move {
            let statuses = ticket
                .wait()
                .await
                .map_err(|_| error(ApplicationPhase::Stopping, "discovery withdrawal failed"))?;
            if statuses
                .iter()
                .any(|status| status.state != RegistrationState::Removed)
            {
                return Err(error(
                    ApplicationPhase::Stopping,
                    "discovery registrations may remain at a provider",
                ));
            }
            Ok(())
        })
    }
}

struct RuntimeShutdown {
    owner: Arc<DiscoveryOwner>,
    #[cfg(feature = "rest")]
    rest: Arc<Mutex<BTreeMap<String, Arc<rest_discovery::RestDiscoveryClient>>>>,
    #[cfg(feature = "rest")]
    global: Arc<Mutex<Option<Arc<rest_discovery::RemoteRuntime>>>>,
}
impl ShutdownAction for RuntimeShutdown {
    /// 业务作用：提供固定的出站资源清理动作名称。
    /// 参数说明：无。
    /// 返回：不含配置的标识。
    fn label(&self) -> &'static str {
        "discovery-runtime"
    }
    /// 业务作用：业务排空后关闭全部出站发送和共享观察，并等待有限清理。
    /// 参数说明：`context` 延续摘流阶段的同一停机截止点。
    /// 返回：注销和 provider 均关闭才成功，超时与残留保留为错误。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let ticket = self.owner.begin_close(deadline);
        Box::pin(async move {
            #[cfg(feature = "rest")]
            let mut rest_failed = false;
            #[cfg(feature = "rest")]
            {
                if let Some(runtime) = self
                    .global
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    rest_discovery::RestDiscovery::shutdown_if_current(&runtime);
                }
                let clients: Vec<_> = self
                    .rest
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect();
                for client in &clients {
                    client.shutdown_background();
                }
                for client in clients {
                    rest_failed |= client.shutdown(deadline).await.is_err();
                }
            }
            let report = ticket.wait().await.map_err(|_| {
                error(
                    ApplicationPhase::Stopping,
                    "discovery resource close failed",
                )
            })?;
            #[cfg(feature = "rest")]
            if rest_failed {
                return Err(error(
                    ApplicationPhase::Stopping,
                    "REST response tasks did not finish before discovery shutdown",
                ));
            }
            if report.deadline_elapsed
                || !report.provider_errors.is_empty()
                || report
                    .registrations
                    .iter()
                    .any(|status| status.state != RegistrationState::Removed)
            {
                return Err(error(
                    ApplicationPhase::Stopping,
                    "discovery close has residual or incomplete resources",
                ));
            }
            Ok(())
        })
    }
}

impl Drop for RegistrationShutdown {
    /// 业务作用：宿主提前释放清理动作时同步关闭注册监督许可。
    /// 参数说明：无。
    /// 返回：触发有限摘流；已存在的停机期限不会延长。
    fn drop(&mut self) {
        self.cancel.cancel();
        self.owner
            .begin_quiesce(tokio::time::Instant::now() + Duration::from_secs(3));
    }
}

impl Drop for RuntimeShutdown {
    /// 业务作用：宿主被丢弃时清除本次安装的默认槽位，并关闭受管出站资源。
    /// 参数说明：无。
    /// 返回：旧 action 不会关闭另一个 owner 的默认门面，清理任务由 owner 保留。
    fn drop(&mut self) {
        #[cfg(feature = "rest")]
        {
            if let Some(runtime) = self
                .global
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                rest_discovery::RestDiscovery::shutdown_if_current(&runtime);
            }
            for client in self
                .rest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
            {
                client.shutdown_background();
            }
        }
        self.owner
            .begin_close(tokio::time::Instant::now() + Duration::from_secs(3));
    }
}

/// 业务作用：判断普通 listener 的本地业务资格，排除远端注册结果以避免健康检查循环。
/// 参数说明：`application` 提供本地生命周期和已登记业务依赖。
/// 返回：已放行且所有本地关键依赖可用时为 true。
#[cfg(any(feature = "web", feature = "grpc"))]
fn local_ready(application: &Application) -> bool {
    application.state() == ApplicationState::Ready
        && application
            .readiness_snapshot()
            .entries
            .iter()
            .filter(|entry| entry.component.as_ref() != "discovery" && entry.affects_ready)
            .all(|entry| {
                matches!(
                    entry.state,
                    DependencyState::Ready | DependencyState::Degraded
                )
            })
}

/// 业务作用：在候选配置发布前校验中性发现配置和引用。
/// 参数说明：`tree` 是最终配置树，`phase` 是当前校验阶段。
/// 返回：配置有效时成功；不产生网络副作用。
pub(crate) fn validate_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    read_config(tree)
        .map_err(|_| error(phase, "invalid discovery configuration"))?
        .validate()
        .map_err(|_| error(phase, "invalid discovery configuration"))
}

/// 业务作用：从完整配置中读取发现投影，不把敏感值带入错误链。
/// 参数说明：`tree` 已完成占位符与导入处理。
/// 返回：缺省时禁用，未知字段或非法结构返回稳定错误。
fn read_config(tree: &serde_json::Value) -> ApplicationResult<DiscoveryConfig> {
    tree.get("discovery").map_or_else(
        || Ok(DiscoveryConfig::default()),
        |value| {
            serde_json::from_value(value.clone())
                .map_err(|_| error(ApplicationPhase::Start, "invalid discovery configuration"))
        },
    )
}

/// 业务作用：创建不携带 provider 内容的生命周期错误。
/// 参数说明：`phase` 是失败阶段，`message` 是静态安全说明。
/// 返回：归属于中性发现组件的错误。
fn error(phase: ApplicationPhase, message: &'static str) -> ApplicationError {
    ApplicationError::new(ComponentId::Discovery, phase, message)
}

/// 业务作用：仅在显式启用时为 HTTP listener 保留本地资格探针。
/// 参数说明：`application` 提供启动阶段冻结配置。
/// 返回：禁用 discovery 或未声明探针时为 false。
#[cfg(feature = "web")]
pub(crate) fn probe_enabled(application: &Application) -> bool {
    read_config(application.config().value())
        .is_ok_and(|config| config.enabled && config.eligibility_probe)
}

/// 业务作用：将当前连接的 HTTP 策略投影到独立客户端，发现缓存与资格继续由共同 owner 裁决。
/// 参数说明：`policy` 已由公共配置校验，省略时使用有限默认值。
/// 返回：没有全量服务扫描或额外发现缓存宽限的调用选项。
#[cfg(feature = "rest")]
fn rest_options(
    policy: Option<&nadisc_runtime::assembly::RestPolicy>,
) -> rest_discovery::RestDiscoveryOptions {
    use rest_discovery::{InstanceScheme, LbStrategy, RestDiscoveryOptions};
    let policy = policy.cloned().unwrap_or_default();
    let mut options = RestDiscoveryOptions::default();
    options.http.timeout = Duration::from_millis(policy.request_timeout_ms);
    options.http.connect_timeout = Duration::from_millis(policy.connect_timeout_ms);
    options.lb_strategy = if policy.lb_strategy == "weighted" {
        LbStrategy::Weighted
    } else {
        LbStrategy::RoundRobin
    };
    options.default_instance_scheme = if policy.instance_scheme == "https" {
        InstanceScheme::Https
    } else {
        InstanceScheme::Http
    };
    options.retry.retry_get_head_on_transport_error = policy.retry_get_head_on_transport_error;
    options.retry.retry_get_head_on_retryable_status = policy.retry_get_head_on_retryable_status;
    options.retry.max_attempts = policy.max_attempts;
    options
}
