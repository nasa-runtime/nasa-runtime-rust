use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex as StdMutex, OnceLock,
    },
    time::{Instant, SystemTime},
};

use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{ConfigStore, ConfigView},
    global,
    resources::ResourceRegistry,
    state::{StateCell, TerminalCell, TerminalIntent},
    supervisor::{ManagedTaskFuture, SupervisorClient, TaskKind, TaskSupervisor},
    ApplicationError, ApplicationMode, ApplicationPhase, ApplicationResult, ApplicationState,
    ComponentId, ConfigSnapshot, ManagedResource, ResourceRef, TaskId,
};

/// `configure_router` 登记的一次路由定制。
///
/// 定制作用在**尚未补齐状态**的 `Router<Application>` 上：业务在这里挂手写路由、全局中间件和
/// per-route 层，框架随后才 merge 探针、`with_state` 并 nest context path。
#[cfg(feature = "web")]
pub type RouterTransform =
    Box<dyn FnOnce(axum::Router<Application>) -> axum::Router<Application> + Send>;

/// `configure_mapping` 登记的一次类型化拦截器/安全运行时计划变换。
///
/// 该队列与 Router 逃生舱分离：这里登记的手动 binding 会与 `global = true` 自动 binding 一起参与
/// mapping 的作用域合并、auth-before-decrypt 排序和监听前审计。
#[cfg(feature = "web")]
pub type MappingTransform = Box<
    dyn FnOnce(
            naweb::MappingPlan<Application>,
        ) -> Result<naweb::MappingPlan<Application>, naweb::MappingBuildError>
        + Send,
>;

/// `configure_ws` 登记的一次长连接服务定制。
///
/// 鉴权回调、endpoint 事件表和集群 notifier 都是业务闭包，配置文件表达不了；定制在声明式配置
/// 之后应用，因此既能补齐这些闭包，也能覆盖 YAML 里的取值。
#[cfg(feature = "ws")]
pub type WsCustomization = Box<dyn FnOnce(naws::ServerBuilder) -> naws::ServerBuilder + Send>;

/// `configure_kafka` 登记的一次消费者注册表定制。
///
/// 闭包在 Kafka Ready 阶段按登记顺序取得并归还 builder；业务依赖已经在 UserHook
/// 构造完成，因此不需要运行时反射或把原始 KafkaProxy 暴露给资源容器。
#[cfg(feature = "kafka")]
pub(crate) type KafkaCustomization = Box<
    dyn FnOnce(nafka::ConsumerRegistryBuilder) -> nafka::Result<nafka::ConsumerRegistryBuilder>
        + Send,
>;

/// `configure_migrations` 登记的一次数据源迁移门禁项:`(数据源名, 业务嵌入 migrator)`。
///
/// migrator 由业务 `sqlx::migrate!("./migrations")` 在 UserHook 构造并登记;DB 组件在 Ready
/// 阶段(监听器就绪、Web/Kafka consumer 接流之前)按数据源逐项取出,依 `database.migrations.mode`
/// 运行 [`namigrate::run_gate`]。migrator 是嵌入式常量数据,`Send + 'static`,可跨阶段存放。
#[cfg(feature = "db")]
pub(crate) type MigrationRegistration = (String, namigrate::Migrator);

#[derive(Debug, Clone)]
/// 启动期固定的应用身份、profile、运行模式和计时基准。
pub struct ApplicationInfo {
    name: Arc<str>,
    profile: Option<Arc<str>>,
    mode: ApplicationMode,
    started_at: SystemTime,
    started_instant: Instant,
}

impl ApplicationInfo {
    /// 校验并创建不会在运行期热替换的应用元数据。
    ///
    /// # 参数
    ///
    /// - `name`：配置名或编译期包名缺省值，trim 后必须非空。
    /// - `profile`：显式启用的本地 profile；空白值按未启用处理。
    /// - `mode`：preflight 已解析完成的 Service 或 Batch 模式。
    pub fn new(
        name: impl AsRef<str>,
        profile: Option<&str>,
        mode: ApplicationMode,
    ) -> ApplicationResult<Self> {
        let name = name.as_ref().trim();
        if name.is_empty() {
            return Err(ApplicationError::new(
                ComponentId::Application,
                ApplicationPhase::Bootstrap,
                "application name cannot be empty",
            ));
        }
        Ok(Self {
            name: Arc::from(name),
            profile: profile
                .map(str::trim)
                .filter(|profile| !profile.is_empty())
                .map(Arc::from),
            mode,
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
        })
    }

    /// 返回稳定应用名。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回借用与当前元数据共同存活。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 返回显式启用的本地 profile。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未设置或只包含空白时返回 `None`。
    pub fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    /// 返回 preflight 固定的运行模式。
    ///
    /// # 参数
    ///
    /// 本方法无参数；运行期配置刷新不会改变该值。
    pub fn mode(&self) -> ApplicationMode {
        self.mode
    }

    /// 返回用于展示的墙上时钟启动时间。
    ///
    /// # 参数
    ///
    /// 本方法无参数；持续时间计算应改用 `uptime`。
    pub fn started_at(&self) -> SystemTime {
        self.started_at
    }

    /// 使用单调时钟返回进程已运行时长。
    ///
    /// # 参数
    ///
    /// 本方法无参数；结果不受系统时间校正影响。
    pub fn uptime(&self) -> std::time::Duration {
        self.started_instant.elapsed()
    }
}

/// Application 共享的所有权根，集中保存配置、资源、状态和控制通道。
pub(crate) struct ApplicationInner {
    info: ApplicationInfo,
    config: ConfigStore,
    resources: ResourceRegistry,
    state: Arc<StateCell>,
    terminal: TerminalCell,
    shutdown_requested: CancellationToken,
    supervisor: SupervisorClient,
    /// 业务启动 Hook 是否仍允许登记资源、任务和定制。
    ///
    /// Runner 在开始轮询 Hook 前打开，在观察到任一完成、失败、取消或超时事件后立即关闭。
    /// 该原子门把公共扩展 API 的有效期绑定到真实 Hook 生命周期，而不是宽松地延长到 Ready。
    user_hook_open: AtomicBool,
    /// 资源与同步定制登记共用的 Hook 边界线性化锁。
    ///
    /// 公共同步登记先持锁再复查 `user_hook_open`；Runner 持同一把锁关闭门。因此一次登记要么完整
    /// 发生在关闭之前，要么确定性失败，不会出现“检查时开放、写入时 Hook 已结束”的竞态。
    user_registration_gate: StdMutex<()>,
    /// Runner 已装配组件的位集合，是全部强类型能力入口判断“是否声明”的唯一来源。
    ///
    /// 组件表只在运行前构造，之后只读；用原子位集合可以让同步与异步 getter 共用同一判断，且无需
    /// 让能力句柄反向持有 Runner 或组件 trait object。
    declared_components: AtomicU32,
    /// 汇总运行依赖动态健康的通用就绪注册表；没有贡献项时保持既有语义。
    readiness: Arc<crate::readiness::ReadinessRegistry>,
    /// UserHook 登记的数据源迁移门禁队列;DB 组件 Ready 取走后封口。
    ///
    /// 语义与 `ws_customizations`/`router_transforms` 一致:`None` 表示队列已被 DB 组件在 Ready
    /// 阶段一次性取走,此后再登记不可能生效,必须报阶段错误而非静默丢弃。同步互斥锁只保护一次
    /// push/take,不跨 await 持有。
    #[cfg(feature = "db")]
    migrations: StdMutex<Option<Vec<MigrationRegistration>>>,
    /// 遥测组件 Start 发布的有界 span 导出器;span 生产者(如 Web trace 中间件)据此非阻塞入队。
    #[cfg(feature = "telemetry")]
    telemetry_exporter: OnceLock<std::sync::Arc<natelemetry::BoundedSpanExporter>>,
    web_addr: OnceLock<SocketAddr>,
    /// Web 组件一次写入、能力句柄只读访问的运行时元数据和请求计数器。
    #[cfg(feature = "web")]
    web_runtime: Arc<crate::web_handle::WebRuntimeState>,
    /// 长连接组件发布发送器与真实监听地址、能力句柄只读访问的运行时状态。
    #[cfg(feature = "ws")]
    ws_runtime: Arc<crate::capabilities::WsRuntimeState>,
    /// UserHook 登记的长连接服务定制队列；语义与路由定制队列一致，取走即封口。
    #[cfg(feature = "ws")]
    ws_customizations: StdMutex<Option<Vec<WsCustomization>>>,
    /// UserHook 登记的路由定制队列。
    ///
    /// `None` 表示队列已被 Web 组件在构造 Router 时一次性取走：此后再登记的定制不可能生效，
    /// 必须报阶段错误而不是静默丢弃。同步互斥锁只保护一次 push/take，不跨 await 持有。
    #[cfg(feature = "web")]
    router_transforms: StdMutex<Option<Vec<RouterTransform>>>,
    /// UserHook 登记的 mapping 计划变换；Ready 取走后永久封口。
    #[cfg(feature = "web")]
    mapping_transforms: StdMutex<Option<Vec<MappingTransform>>>,
    /// Web Ready 审计成功后发布的只读 MappingRuntime。
    #[cfg(feature = "web")]
    mapping_runtime: OnceLock<Arc<naweb::MappingRuntime>>,
    /// 可热刷组件在 Start 阶段登记的配置重应用句柄。
    ///
    /// 登记只发生在组件启动阶段（配置热刷新驱动尚不存在），驱动创建于 Ready、此后只读；
    /// 同步互斥锁只保护登记与整表克隆，不跨 await 持有。
    #[cfg(any(feature = "log", feature = "nacos-config"))]
    config_appliers: StdMutex<Vec<Arc<dyn crate::reload::ConfigApplier>>>,
    /// 日志组件的初始化发布状态，不包含日志管理器或刷盘 guard。
    #[cfg(feature = "log")]
    log_runtime: Arc<crate::capabilities::LogRuntimeState>,
    /// 配置中心底层客户端的弱引用发布状态。
    #[cfg(feature = "nacos-config")]
    nacos_config_runtime: Arc<crate::capabilities::NacosConfigRuntimeState>,
    /// 服务发现会话的弱引用发布状态。
    #[cfg(feature = "nacos-discovery")]
    nacos_discovery_runtime: Arc<crate::capabilities::NacosDiscoveryRuntimeState>,
    /// 调度器启动结果和可选选主对象的只读发布状态。
    #[cfg(feature = "scheduling")]
    scheduling_runtime: Arc<crate::capabilities::SchedulingRuntimeState>,
    /// Kafka client 能力、UserHook consumer 定制和指标桥的受控发布状态。
    #[cfg(feature = "kafka")]
    kafka_runtime: Arc<crate::kafka::KafkaRuntimeState>,
    /// 进程级统一指标注册表:各领域按 descriptor 记录到同一 hub。
    ///
    /// nafka 域为原生记录(fan-out 到本 hub,取代业务手工 sink);naweb 域经
    /// `NawebMetricsSource` 兼容源并入。nafana 迁移属门面层增量,不在 napp。
    #[cfg(any(feature = "kafka", feature = "web"))]
    metrics_hub: Arc<nametrics_core::MetricHub>,
    /// 业务经 UserHook 注入的幂等 store;注入后 Web 组件在 Ready 装配幂等中间件。
    ///
    /// `OnceLock` 保证只注入一次;未注入则不启用幂等层(默认零行为)。
    #[cfg(feature = "web")]
    idempotency_store: OnceLock<crate::idempotency::SharedIdempotencyStore>,
    /// 业务经 UserHook 注入的授权策略注册表;注入后 Web 组件在 Ready 装配授权中间件。
    ///
    /// `OnceLock` 保证只注入一次;未注入则不启用授权层(默认零行为)。
    #[cfg(feature = "web")]
    authz_registry: OnceLock<crate::authz::SharedPolicyRegistry>,
    /// 对象级授权 provider 与固定调用预算；请求边界冻结进 RequestSecurityContext。
    #[cfg(feature = "web")]
    object_authorizer: OnceLock<(crate::authz::SharedObjectAuthorizer, std::time::Duration)>,
    /// 业务经 UserHook 注入的认证器;注入后 Web 组件在 Ready 装配 authentication 中间件。
    ///
    /// `OnceLock` 保证只注入一次;未注入则不启用认证层(受保护 route 因无 Principal 被 authz 拒)。
    #[cfg(feature = "web")]
    authenticator: OnceLock<crate::authn::SharedAuthenticator>,
}

#[derive(Clone)]
/// 业务和组件共享的应用上下文；克隆只增加共享所有权，不复制资源或状态。
pub struct Application {
    pub(crate) inner: Arc<ApplicationInner>,
}

impl Application {
    /// 创建 Application 与其唯一 TaskSupervisor 所有者。
    ///
    /// # 参数
    ///
    /// - `info`：同步 preflight 已固定的应用元数据。
    /// - `initial_config`：版本为 1 的不可变配置视图。
    pub(crate) fn create(
        info: ApplicationInfo,
        initial_config: Arc<ConfigView>,
    ) -> (Self, TaskSupervisor) {
        let (supervisor, task_supervisor) = TaskSupervisor::channel();
        let application = Self {
            inner: Arc::new(ApplicationInner {
                info,
                config: ConfigStore::new(initial_config),
                resources: ResourceRegistry::new(),
                state: Arc::new(StateCell::new()),
                terminal: TerminalCell::new(),
                shutdown_requested: CancellationToken::new(),
                supervisor,
                user_hook_open: AtomicBool::new(false),
                user_registration_gate: StdMutex::new(()),
                declared_components: AtomicU32::new(0),
                readiness: Arc::new(crate::readiness::ReadinessRegistry::new()),
                #[cfg(feature = "db")]
                migrations: StdMutex::new(Some(Vec::new())),
                #[cfg(feature = "telemetry")]
                telemetry_exporter: OnceLock::new(),
                web_addr: OnceLock::new(),
                #[cfg(feature = "web")]
                web_runtime: Arc::new(crate::web_handle::WebRuntimeState::new()),
                #[cfg(feature = "ws")]
                ws_runtime: Arc::new(crate::capabilities::WsRuntimeState::new()),
                #[cfg(feature = "ws")]
                ws_customizations: StdMutex::new(Some(Vec::new())),
                #[cfg(feature = "web")]
                router_transforms: StdMutex::new(Some(Vec::new())),
                #[cfg(feature = "web")]
                mapping_transforms: StdMutex::new(Some(Vec::new())),
                #[cfg(feature = "web")]
                mapping_runtime: OnceLock::new(),
                #[cfg(any(feature = "log", feature = "nacos-config"))]
                config_appliers: StdMutex::new(Vec::new()),
                #[cfg(feature = "log")]
                log_runtime: Arc::new(crate::capabilities::LogRuntimeState::new()),
                #[cfg(feature = "nacos-config")]
                nacos_config_runtime: Arc::new(crate::capabilities::NacosConfigRuntimeState::new()),
                #[cfg(feature = "nacos-discovery")]
                nacos_discovery_runtime: Arc::new(
                    crate::capabilities::NacosDiscoveryRuntimeState::new(),
                ),
                #[cfg(feature = "scheduling")]
                scheduling_runtime: Arc::new(crate::capabilities::SchedulingRuntimeState::new()),
                #[cfg(feature = "kafka")]
                kafka_runtime: Arc::new(crate::kafka::KafkaRuntimeState::new()),
                #[cfg(any(feature = "kafka", feature = "web"))]
                metrics_hub: Arc::new(nametrics_core::MetricHub::new()),
                #[cfg(feature = "web")]
                idempotency_store: OnceLock::new(),
                #[cfg(feature = "web")]
                authz_registry: OnceLock::new(),
                #[cfg(feature = "web")]
                object_authorizer: OnceLock::new(),
                #[cfg(feature = "web")]
                authenticator: OnceLock::new(),
            }),
        };
        (application, task_supervisor)
    }

    /// 返回启动期固定的应用元数据。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回借用不能越过当前 Application 句柄。
    pub fn info(&self) -> &ApplicationInfo {
        &self.inner.info
    }

    /// 以 Acquire 语义读取公开生命周期状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；读取到 Stopping 后即可安全观察已先提交的首次终态。
    pub fn state(&self) -> ApplicationState {
        self.inner.state.load()
    }

    /// 判断应用是否已经完成所有 Ready action且全部动态运行依赖当前可用。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Stopping、Stopped、Failed 或任一 readiness contributor 为 false 时返回 false。
    pub fn is_ready(&self) -> bool {
        self.state() == ApplicationState::Ready && self.inner.readiness.all_ready()
    }

    /// 读取各依赖只读就绪快照(管理端读取):按名有序的 name/state/reason/affects_ready,以及聚合
    /// `ready`/`degraded`。无 I/O、无锁竞争地读内存已发布状态,供管理端诊断「谁未就绪/谁降级」。
    ///
    /// # 参数
    ///
    /// 本方法无参数;快照反映读取时刻各 contributor 的已发布状态(含 stale 裁决)。
    pub fn readiness_snapshot(&self) -> crate::readiness::ReadinessSnapshot {
        self.inner.readiness.snapshot(std::time::Instant::now())
    }

    /// 封口就绪注册表:此后组件/业务不能再注册 readiness contributor。
    ///
    /// 由 Runner 在 UserHook 成功、资源封口之后、Ready 之前调用(与 `resources().seal()` 同处),
    /// 防止运行期无界新增贡献项名称。组件必须在 Start 或 UserHook 开放期完成注册。
    ///
    /// # 参数
    ///
    /// 本方法无参数;幂等,重复调用无副作用。
    pub(crate) fn seal_readiness(&self) {
        self.inner.readiness.seal();
    }

    /// 返回 Web 监听器实际绑定的地址。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Web Ready 尚未成功或应用未声明 Web 时返回 `None`。
    pub fn web_addr(&self) -> Option<SocketAddr> {
        self.inner.web_addr.get().copied()
    }

    /// 获取当前配置视图中的不可变期望快照。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回的 `Arc` 保证一次业务读取始终停留在同一版本。
    pub fn config(&self) -> Arc<ConfigSnapshot> {
        self.inner.config.load().snapshot().clone()
    }

    /// 获取包含快照和各目标应用状态的同版本配置视图。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值可跨 await 保持一致。
    pub fn config_view(&self) -> Arc<ConfigView> {
        self.inner.config.load()
    }

    /// 获取与当前 config 同 generation 的 secret 快照。
    ///
    /// # 参数
    ///
    /// 本方法无参数;返回与 `config()` 同代的 secret 集合。真实值只能经其 `get(id).expose()` 取得,
    /// 不进普通 `config()` 树(树里 fragment 已 `<redacted>`)。secret 消费者据此取 material 并用
    /// `changed_ids` 判轮换。
    pub fn secrets(&self) -> Arc<nasecret::SecretSnapshot> {
        Arc::clone(self.inner.config.load().secrets())
    }

    /// 从当前单一快照反序列化完整强类型配置。
    ///
    /// # 参数
    ///
    /// 本方法无显式参数；目标类型 `T` 必须拥有反序列化结果。
    pub fn config_as<T: DeserializeOwned>(&self) -> ApplicationResult<T> {
        self.config().deserialize()
    }

    /// 从当前单一快照读取并反序列化指定配置段。
    ///
    /// # 参数
    ///
    /// - `path`：使用 `.` 或 `/` 分隔的配置节点路径。
    pub fn config_section<T: DeserializeOwned>(&self, path: &str) -> ApplicationResult<T> {
        self.config().section(path)
    }

    /// 订阅配置视图的原子版本更新。
    ///
    /// # 参数
    ///
    /// 本方法无参数；新 receiver 的初值就是调用时的完整当前视图。
    pub fn subscribe_config(&self) -> watch::Receiver<Arc<ConfigView>> {
        self.inner.config.subscribe()
    }

    /// 在 UserHook 开放阶段登记一个无 qualifier 的业务资源。
    ///
    /// # 参数
    ///
    /// - `value`：把所有权交给 Application 的线程安全业务资源。
    pub fn register<T>(&self, value: T) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("resource registration")?;
        self.inner.resources.register(value)
    }

    /// 在同一资源类型下登记一个带 qualifier 的业务资源。
    ///
    /// # 参数
    ///
    /// - `qualifier`：区分同类型实例的非空稳定名称。
    /// - `value`：把所有权交给 Application 的线程安全业务资源。
    pub fn register_named<T>(&self, qualifier: impl AsRef<str>, value: T) -> ApplicationResult<()>
    where
        T: Send + Sync + 'static,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("resource registration")?;
        self.inner.resources.register_named(qualifier, value)
    }

    /// 登记一个需要显式异步 shutdown 的业务资源。
    ///
    /// # 参数
    ///
    /// - `value`：实现 ManagedResource 且 Drop 保持非阻塞的资源所有权。
    pub fn register_managed<T>(&self, value: T) -> ApplicationResult<()>
    where
        T: ManagedResource,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("managed resource registration")?;
        self.inner.resources.register_managed(value)
    }

    /// 登记一个带 qualifier 且需要显式异步 shutdown 的业务资源。
    ///
    /// # 参数
    ///
    /// - `qualifier`：区分同类型受管资源的非空稳定名称。
    /// - `value`：实现 ManagedResource 且 Drop 保持非阻塞的资源所有权。
    pub fn register_named_managed<T>(
        &self,
        qualifier: impl AsRef<str>,
        value: T,
    ) -> ApplicationResult<()>
    where
        T: ManagedResource,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("managed resource registration")?;
        self.inner
            .resources
            .register_named_managed(qualifier, value)
    }

    /// 异步借用一个无 qualifier 的已登记资源。
    ///
    /// # 参数
    ///
    /// 本方法无显式参数；类型 `T` 参与资源 key，并把返回守卫生命周期绑定到当前 Application 借用。
    pub async fn resource<T>(&self) -> ApplicationResult<ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner.resources.get().await
    }

    /// 异步借用一个指定 qualifier 的已登记资源。
    ///
    /// # 参数
    ///
    /// - `qualifier`：登记时使用的非空稳定名称。
    pub async fn named_resource<T>(
        &self,
        qualifier: impl AsRef<str>,
    ) -> ApplicationResult<ResourceRef<'_, T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner.resources.get_named(qualifier).await
    }

    /// 登记一个异常退出只进入报告、不主动终止 Service 的后台任务。
    ///
    /// # 参数
    ///
    /// - `name`：同一任务组内唯一且非空的稳定任务名。
    /// - `task`：接收组级子取消令牌并返回拥有所有捕获值的 future 的工厂。
    pub async fn spawn_background<N, F, Fut>(&self, name: N, task: F) -> ApplicationResult<TaskId>
    where
        N: Into<Arc<str>>,
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.spawn_task(name.into(), TaskKind::Background, task)
            .await
    }

    /// 登记一个在 Running 状态提前退出就触发失败停机的关键任务。
    ///
    /// # 参数
    ///
    /// - `name`：同一任务组内唯一且非空的稳定任务名。
    /// - `task`：接收组级子取消令牌并返回拥有所有捕获值的 future 的工厂。
    pub async fn spawn_critical<N, F, Fut>(&self, name: N, task: F) -> ApplicationResult<TaskId>
    where
        N: Into<Arc<str>>,
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if self.info().mode() == ApplicationMode::Batch {
            return Err(ApplicationError::new(
                ComponentId::Supervisor,
                ApplicationPhase::UserHook,
                "critical managed tasks are not allowed in batch mode",
            ));
        }
        self.spawn_task(name.into(), TaskKind::Critical, task).await
    }

    /// 按名称获取数据源连接池句柄。
    ///
    /// 返回 clone handle 是 `MySqlPool` 自身的共享语义（内部就是 `Arc` 池），不等于把资源移出容器；
    /// 单库配置 `database` 注册在 `default` 名下。
    ///
    /// # 参数
    ///
    /// - `name`：数据源 qualifier；单库配置固定使用 `default`。
    #[cfg(feature = "db")]
    pub async fn datasource(&self, name: &str) -> ApplicationResult<natx::MySqlPool> {
        self.ensure_component_declared(
            ComponentId::Db,
            ApplicationPhase::Running,
            "datasource access",
        )?;
        crate::db::datasource_handle(self, name).await
    }

    /// 为某数据源登记一组业务嵌入 migration,交由 DB 组件在监听器 Ready 前运行门禁。
    ///
    /// migration 属**业务 schema**,不进共享 runtime;业务在 UserHook 用
    /// `sqlx::migrate!("./migrations")` 构造 [`Migrator`](crate::Migrator) 并按数据源名登记。
    /// 具体裁决(disabled/validate/apply)由配置 `database.migrations.mode`(多库为
    /// `datasources.<名>.migrations.mode`)决定,生产默认 `validate`(只读校验版本一致,绝不改
    /// schema)。门禁在 DB 组件 Ready 阶段执行——因 `db` 声明序在 `web`/`kafka` 之前,故迁移在任何
    /// 监听器接流、任何 Kafka consumer 启动之前完成;校验失败(未应用、checksum 漂移)直接 fail
    /// startup,监听端口从不对外服务。
    ///
    /// 不新增 `"migration"` 组件字符串:它是已声明 `db` 组件的 Ready 前子阶段。
    ///
    /// # 参数
    ///
    /// - `datasource`:目标数据源 qualifier;单库配置固定 `default`,须与已配置数据源同名。
    /// - `migrator`:业务 `sqlx::migrate!(...)` 生成的嵌入式 migrator。
    ///
    /// # 错误
    ///
    /// 非 UserHook 阶段、未声明 `db` 组件、数据源名为空、同一数据源重复登记,或 Ready 已封口时返回
    /// 阶段错误。门禁运行期的未应用/漂移/后端错误在 DB 组件 Ready 阶段转成启动失败(只含版本与稳定
    /// reason,不含 SQL 正文)。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// app.configure_migrations("default", sqlx::migrate!("./migrations"))?;
    /// ```
    #[cfg(feature = "db")]
    pub fn configure_migrations(
        &self,
        datasource: &str,
        migrator: namigrate::Migrator,
    ) -> ApplicationResult<()> {
        // 与 ws/router 定制登记同一把 Hook 边界线性化锁:先持锁再复查门,消除"检查时开放、写入时
        // Hook 已结束"的竞态。
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("migration registration")?;
        self.ensure_component_declared(
            ComponentId::Db,
            ApplicationPhase::UserHook,
            "migration registration",
        )?;
        // 门禁在 DB 组件 Ready 阶段执行,而 Batch 模式不执行 Ready:登记会被静默丢弃。
        // 与其静默漏跑,不如直接拒绝并指向显式 API——Batch 任务可在 Hook 里自行调用
        // `nasa::run_gate`(经 `nasa::MigrationSettings` 构造设置)完成一次性校验/应用。
        if self.inner.info.mode() == ApplicationMode::Batch {
            return Err(ApplicationError::new(
                ComponentId::Db,
                ApplicationPhase::UserHook,
                "configure_migrations requires Service mode; the migration gate runs in the DB Ready sub-phase, which Batch does not execute. Run namigrate::run_gate explicitly in a batch hook instead",
            ));
        }
        let datasource = datasource.trim();
        if datasource.is_empty() {
            return Err(ApplicationError::new(
                ComponentId::Db,
                ApplicationPhase::UserHook,
                "configure_migrations datasource name cannot be empty",
            ));
        }
        let mut slot = self
            .inner
            .migrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(registrations) = slot.as_mut() else {
            return Err(ApplicationError::new(
                ComponentId::Db,
                ApplicationPhase::Ready,
                "migration registration is closed; configure_migrations is only available while the application startup hook is running",
            ));
        };
        // 同一数据源只能登记一组 migration:门禁按数据源执行一次,重复登记会产生歧义。
        if registrations.iter().any(|(name, _)| name == datasource) {
            return Err(ApplicationError::new(
                ComponentId::Db,
                ApplicationPhase::UserHook,
                format!("datasource `{datasource}` already has a registered migration set"),
            ));
        }
        registrations.push((datasource.to_owned(), migrator));
        Ok(())
    }

    /// 由 `TelemetryComponent` 在 Start 阶段发布配置驱动的有界 span 导出器。
    ///
    /// 早于流量入口 Ready 发布,使 Web 等生产者在其 Ready 装配时即可取到 exporter 入队。重复发布返回
    /// 带 Telemetry 组件身份的错误。
    ///
    /// # 参数
    ///
    /// - `exporter`:遥测组件按 `telemetry.queue_capacity` 创建的有界导出器。
    #[cfg(feature = "telemetry")]
    pub(crate) fn publish_telemetry_exporter(
        &self,
        exporter: std::sync::Arc<natelemetry::BoundedSpanExporter>,
    ) -> ApplicationResult<()> {
        self.inner.telemetry_exporter.set(exporter).map_err(|_| {
            ApplicationError::new(
                ComponentId::Telemetry,
                ApplicationPhase::Start,
                "telemetry exporter was already published",
            )
        })
    }

    /// 获取遥测组件发布的有界 span 导出器;未声明遥测组件或未启用时返回 `None`。
    ///
    /// # 参数
    ///
    /// 本方法无参数;span 生产者用它非阻塞入队,拿不到即表示遥测未激活,直接跳过入队。
    /// 仅在同时启用 `web` 时编译:目前唯一生产者是 Web trace 中间件。
    #[cfg(feature = "telemetry")]
    pub(crate) fn telemetry_exporter(
        &self,
    ) -> Option<std::sync::Arc<natelemetry::BoundedSpanExporter>> {
        self.inner.telemetry_exporter.get().cloned()
    }

    /// 返回只写 span 记录器，不暴露 exporter 接收端、flush 或关闭权限。
    #[cfg(feature = "telemetry")]
    pub fn span_recorder(&self) -> Option<natelemetry::SpanRecorder> {
        self.telemetry_exporter()
            .map(natelemetry::SpanRecorder::new)
    }

    /// 返回遥测有界队列/丢弃计数摘要；组件未启用时返回 `None`。
    ///
    /// 摘要不包含 OTLP endpoint、span 名或属性正文，可安全用于受保护的管理端。
    #[cfg(feature = "telemetry")]
    pub fn telemetry_snapshot(&self) -> Option<natelemetry::ExporterSnapshot> {
        self.inner
            .telemetry_exporter
            .get()
            .map(|exporter| exporter.snapshot())
    }

    /// 显式记录一个业务子 span。
    ///
    /// 给 db/redis/kafka 等**非 web 调用点**用的显式插桩入口:业务把 handler 里拿到的
    /// [`TraceContext`](natelemetry::TraceContext)(Web trace 中间件写入请求扩展)**显式传入**,本方法
    /// 以其为父派生一个新 span-id 的子 span 并非阻塞入队——同一 trace-id 串起 Web 服务端 span 与业务
    /// 子操作 span。不做任何隐式 ambient 上下文透传:拿不到/不想传上下文就不产 span,纪律与全框架一致。
    ///
    /// 遥测组件未声明/未启用时为无副作用空操作;队列满时丢弃并计数,绝不阻塞业务。`name` 必须低基数
    /// (如 `"db select order"`),不得含用户输入或主键值。
    ///
    /// # 参数
    ///
    /// - `name`:低基数 span 名。
    /// - `trace`:父链路上下文(通常来自请求扩展),子 span 沿用其 trace-id。
    #[cfg(feature = "telemetry")]
    pub fn record_span(&self, name: impl Into<String>, trace: &natelemetry::TraceContext) {
        let Some(exporter) = self.inner.telemetry_exporter.get() else {
            return;
        };
        // child() 把新 span-id 写进 parent_id 段,故 parent_id_hex() 即本子 span 的 span-id(与 Web
        // trace 中间件 `trace_context_export` 的取法一致)。
        let child = trace.child(natelemetry::random_span_id());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        let _ = exporter.export(natelemetry::SpanRecord {
            name: name.into(),
            trace_id_hex: child.trace_id_hex(),
            span_id_hex: child.parent_id_hex(),
            parent_span_id_hex: Some(trace.parent_id_hex()),
            // 业务子操作是进程内 span,不得冒充服务端 span(SERVER 由 Web trace 中间件产)。
            kind: natelemetry::SpanKind::Internal,
            start_unix_nano: now,
            end_unix_nano: now,
            http_status_code: None,
        });
    }

    /// 取走并封口 UserHook 登记的全部迁移门禁项(DB 组件 Ready 阶段调用一次)。
    ///
    /// # 参数
    ///
    /// 本方法无参数;取走后队列置 `None`,此后 `configure_migrations` 一律返回封口错误。
    #[cfg(feature = "db")]
    pub(crate) fn take_migrations(&self) -> Vec<MigrationRegistration> {
        self.inner
            .migrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .unwrap_or_default()
    }

    /// 按名称获取 Redis 客户端句柄。
    ///
    /// # 参数
    ///
    /// - `name`：Redis 实例 qualifier；单实例配置固定使用 `default`。
    #[cfg(feature = "redis")]
    pub async fn redis(&self, name: &str) -> ApplicationResult<Arc<nadis::RedisClient>> {
        self.ensure_component_declared(
            ComponentId::Redis,
            ApplicationPhase::Running,
            "redis client access",
        )?;
        crate::redis::redis_handle(self, name).await
    }

    /// 按 client name 获取 Kafka 的受控发布、健康和运行控制句柄。
    ///
    /// 句柄不会暴露 `KafkaProxy`、consumer registry 或 shutdown 权；最终关闭始终由
    /// Application active stack 执行。
    ///
    /// # 参数
    ///
    /// - `name`：`kafka.client_name` 或 `kafkas` map key；必须非空且已经在 Start 发布。
    ///
    /// # 返回
    ///
    /// 返回共享底层 client 的轻量受控句柄，克隆不会新建连接。
    ///
    /// # 错误
    ///
    /// 组件未声明、client 不存在或尚未完成 Start 发布时返回 Kafka 组件错误。
    #[cfg(feature = "kafka")]
    pub fn kafka(&self, name: &str) -> ApplicationResult<crate::capabilities::KafkaHandle> {
        self.ensure_component_declared(
            ComponentId::Kafka,
            ApplicationPhase::Running,
            "kafka capability access",
        )?;
        let capability = self.inner.kafka_runtime.client(name).ok_or_else(|| {
            ApplicationError::new(
                ComponentId::Kafka,
                ApplicationPhase::Running,
                format!("kafka client `{name}` is not configured or has not completed Start"),
            )
        })?;
        Ok(crate::capabilities::KafkaHandle::new(
            capability,
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取日志组件的只读运行时能力句柄。
    ///
    /// 日志底层是进程级订阅器而不是实例客户端；句柄用于确认容器管理的初始化与生命周期，日志事件
    /// 继续通过公开日志门面写入，配置重应用和文件关闭权不对业务开放。
    ///
    /// # 参数
    ///
    /// 本方法无参数；应用未声明日志组件时返回带日志组件身份的错误。
    #[cfg(feature = "log")]
    pub fn log(&self) -> ApplicationResult<crate::capabilities::LogHandle> {
        self.ensure_component_declared(
            ComponentId::Log,
            ApplicationPhase::Running,
            "log capability access",
        )?;
        Ok(crate::capabilities::LogHandle::new(
            Arc::clone(&self.inner.log_runtime),
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取配置中心组件的受控拉取能力句柄。
    ///
    /// 句柄复用组件已经建立的连接，只开放读取远端原文；发布、删除、监听注册和关闭仍由容器统一管理。
    ///
    /// # 参数
    ///
    /// 本方法无参数；应用未声明配置中心组件时返回明确错误。
    #[cfg(feature = "nacos-config")]
    pub fn nacos_config(&self) -> ApplicationResult<crate::capabilities::NacosConfigHandle> {
        self.ensure_component_declared(
            ComponentId::NacosConfig,
            ApplicationPhase::Running,
            "config center capability access",
        )?;
        Ok(crate::capabilities::NacosConfigHandle::new(
            Arc::clone(&self.inner.nacos_config_runtime),
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取 Web 组件的只读运行时能力句柄。
    ///
    /// 句柄可以查询真实监听地址、上下文前缀、预检路由清单、就绪状态和请求指标；它不持有也不返回
    /// 路由服务图、监听器、服务任务或资源容器，路由修改仍只能在 UserHook 使用 `configure_router`。
    ///
    /// # 参数
    ///
    /// 本方法无参数；应用未声明 Web 组件时返回带 Web 组件身份的明确错误。
    #[cfg(feature = "web")]
    pub fn web(&self) -> ApplicationResult<crate::web_handle::WebHandle> {
        self.ensure_component_declared(
            ComponentId::Web,
            ApplicationPhase::Running,
            "web capability access",
        )?;
        Ok(crate::web_handle::WebHandle::new(
            Arc::clone(&self.inner.web_runtime),
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取两级缓存运行时的只读能力句柄。
    ///
    /// 句柄只提供 generation、装配摘要和只读健康探针，不持有停机 owner，也不能替换后端。
    #[cfg(feature = "cache")]
    pub fn cache(&self) -> ApplicationResult<cacheable::CacheHandle> {
        self.ensure_component_declared(
            ComponentId::Cache,
            ApplicationPhase::Running,
            "cache capability access",
        )?;
        Ok(cacheable::cache_handle())
    }

    /// 从 Ready 阶段发布的静态业务路由事实生成 OpenAPI 3.1 文档。
    ///
    /// `configure_router` 追加的不透明路由没有可审计的类型元数据，刻意不进入文档。授权标记同时合并
    /// 端点声明和当前 route policy 快照，避免把运行时收紧的路由误报成公开接口。
    #[cfg(feature = "web")]
    pub fn openapi_document(
        &self,
        title: &str,
        version: &str,
    ) -> ApplicationResult<serde_json::Value> {
        let web = self.web()?;
        let context_path = web.context_path().trim_end_matches('/');
        let policy_set = self.authz_registry().map(|registry| registry.current());
        let manifest = web.routes();
        let routes = manifest
            .iter()
            .filter(|route| matches!(route.origin(), crate::web_handle::WebRouteOrigin::Business))
            .map(|route| {
                let path = if context_path.is_empty() {
                    route.path().to_owned()
                } else {
                    format!("{context_path}{}", route.path())
                };
                let route_id = format!("{} {path}", route.method());
                // Axum 0.8 的 catch-all 写作 `{*tail}`，OpenAPI 路径模板仍是 `{tail}`。
                // route_id 保留真实 Axum 形式用于授权查找，只有文档路径去掉 `*`。
                let openapi_path = path
                    .split('/')
                    .map(|segment| {
                        segment
                            .strip_prefix("{*")
                            .and_then(|tail| tail.strip_suffix('}'))
                            .map_or_else(|| segment.to_owned(), |name| format!("{{{name}}}"))
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                let mut parameters = Vec::new();
                for (location, factory) in [
                    (
                        naopenapi::ParameterLocation::Query,
                        route.query_parameters(),
                    ),
                    (
                        naopenapi::ParameterLocation::Header,
                        route.header_parameters(),
                    ),
                ] {
                    if let Some(factory) = factory {
                        parameters.extend(factory().iter().map(|parameter| {
                            let schema = (parameter.schema)();
                            naopenapi::ParameterContract {
                                name: parameter.name.to_owned(),
                                location,
                                required: parameter.required,
                                schema: naopenapi::SchemaContract {
                                    name: schema.name.to_owned(),
                                    json_schema: schema.json_schema.to_owned(),
                                },
                            }
                        }));
                    }
                }
                let additional_responses = route
                    .additional_responses()
                    .map(|factory| {
                        factory()
                            .iter()
                            .map(|response| naopenapi::ResponseContract {
                                status: response.status,
                                description: response.description.to_owned(),
                                media_type: response.produces.map(str::to_owned),
                                schema: response.schema.map(|factory| {
                                    let schema = factory();
                                    naopenapi::SchemaContract {
                                        name: schema.name.to_owned(),
                                        json_schema: schema.json_schema.to_owned(),
                                    }
                                }),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                naopenapi::RouteContract {
                    method: route.method().to_owned(),
                    path: openapi_path,
                    operation_id: route.handler().to_owned(),
                    consumes: route.consumes().map(str::to_owned),
                    produces: route.produces().map(str::to_owned),
                    request_schema: route.request_schema().map(|factory| {
                        let schema = factory();
                        naopenapi::SchemaContract {
                            name: schema.name.to_owned(),
                            json_schema: schema.json_schema.to_owned(),
                        }
                    }),
                    response_schema: route.response_schema().map(|factory| {
                        let schema = factory();
                        naopenapi::SchemaContract {
                            name: schema.name.to_owned(),
                            json_schema: schema.json_schema.to_owned(),
                        }
                    }),
                    parameters,
                    success_status: route.success_status(),
                    additional_responses,
                    streaming: route.streaming(),
                    auth_required: route.auth_required()
                        || policy_set
                            .as_ref()
                            .is_some_and(|policies| policies.is_protected(&route_id)),
                }
            });
        naopenapi::generate(title, version, routes).map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Web,
                ApplicationPhase::Running,
                "OpenAPI document generation failed",
                error,
            )
        })
    }

    /// 获取 Web Ready 发布的只读 mapping 安全运行时句柄。
    ///
    /// # 返回
    ///
    /// Web 已完成路由计划审计时返回句柄；尚未发布或未声明 Web 组件时返回阶段错误。
    #[cfg(feature = "web")]
    pub fn mapping(&self) -> ApplicationResult<crate::MappingHandle> {
        self.ensure_component_declared(
            ComponentId::Web,
            ApplicationPhase::Running,
            "mapping capability access",
        )?;
        let runtime = self.mapping_runtime().ok_or_else(|| {
            ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Running,
                "mapping runtime has not completed Web Ready publication",
            )
        })?;
        Ok(crate::MappingHandle::new(
            runtime,
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取长连接组件的发送与只读运行状态句柄。
    ///
    /// 句柄可取得底层广播发送器和真实监听地址，但不返回 server、builder、监听器或关闭入口。
    ///
    /// # 参数
    ///
    /// 本方法无参数；应用未声明长连接组件时返回明确错误。
    #[cfg(feature = "ws")]
    pub fn ws(&self) -> ApplicationResult<crate::capabilities::WsHandle> {
        self.ensure_component_declared(
            ComponentId::Ws,
            ApplicationPhase::Running,
            "ws capability access",
        )?;
        Ok(crate::capabilities::WsHandle::new(
            Arc::clone(&self.inner.ws_runtime),
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取服务发现组件的出站请求与注册状态句柄。
    ///
    /// 句柄可取得底层带负载均衡能力的 HTTP 客户端；注册、摘流和关闭 provider 仍由生命周期组件执行。
    ///
    /// # 参数
    ///
    /// 本方法无参数；应用未声明服务发现组件时返回明确错误。
    #[cfg(feature = "nacos-discovery")]
    pub fn nacos_discovery(&self) -> ApplicationResult<crate::capabilities::NacosDiscoveryHandle> {
        self.ensure_component_declared(
            ComponentId::NacosDiscovery,
            ApplicationPhase::Running,
            "discovery capability access",
        )?;
        Ok(crate::capabilities::NacosDiscoveryHandle::new(
            Arc::clone(&self.inner.nacos_discovery_runtime),
            Arc::clone(&self.inner.state),
        ))
    }

    /// 获取调度组件的只读底层运行时句柄。
    ///
    /// 句柄提供运行状态、任务数量和可选选主状态，不开放停止或重启调度器的能力。
    ///
    /// # 参数
    ///
    /// 本方法无参数；应用未声明调度组件时返回明确错误。
    #[cfg(feature = "scheduling")]
    pub fn scheduling(&self) -> ApplicationResult<crate::capabilities::SchedulingHandle> {
        self.ensure_component_declared(
            ComponentId::Scheduling,
            ApplicationPhase::Running,
            "scheduling capability access",
        )?;
        Ok(crate::capabilities::SchedulingHandle::new(
            Arc::clone(&self.inner.scheduling_runtime),
            Arc::clone(&self.inner.state),
        ))
    }

    /// 在 UserHook 阶段为指定 Kafka client 登记一次有状态 consumer 装配。
    ///
    /// 自动收集的属性 consumer 会先进入 builder，本闭包随后按登记顺序执行；闭包只能
    /// 注册业务 consumer，不能取得原始运行时或关闭权。
    ///
    /// # 参数
    ///
    /// - `name`：目标 Kafka client 的稳定名称，必须已经在 Start 配置并发布。
    /// - `configure`：取得并归还 `ConsumerRegistryBuilder` 的一次性闭包；捕获的 DB、Redis
    ///   等业务依赖必须拥有所有权并可跨线程移动。
    ///
    /// # 返回
    ///
    /// 闭包成功进入该 client 的有序定制队列时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// UserHook 已关闭、Kafka 组件未声明、client 不存在或 Ready 已取走队列时返回错误。
    #[cfg(feature = "kafka")]
    pub fn configure_kafka<F>(&self, name: &str, configure: F) -> ApplicationResult<()>
    where
        F: FnOnce(nafka::ConsumerRegistryBuilder) -> nafka::Result<nafka::ConsumerRegistryBuilder>
            + Send
            + 'static,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("kafka consumer configuration")?;
        self.ensure_component_declared(
            ComponentId::Kafka,
            ApplicationPhase::UserHook,
            "kafka consumer configuration",
        )?;
        self.inner
            .kafka_runtime
            .push_customization(name, Box::new(configure))
    }

    /// 在 UserHook 阶段为指定 Kafka client 安装一次后端无关指标接收端。
    ///
    /// sink 会通过 Start 时创建的内部桥接层立即接管后续指标；它必须无阻塞、不可 panic，
    /// 且不能持有需要在 consumer 之前关闭的业务资源。
    ///
    /// # 参数
    ///
    /// - `name`：目标 Kafka client 的稳定名称。
    /// - `sink`：业务提供的共享指标出口；同一 client 只允许安装一次。
    ///
    /// # 返回
    ///
    /// 指标桥首次安装成功时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// UserHook 已关闭、组件/client 不存在、重复安装或 bridge 已在 Ready 封口时返回错误。
    #[cfg(feature = "kafka")]
    pub fn configure_kafka_metrics(
        &self,
        name: &str,
        sink: Arc<dyn nafka::MetricsSink>,
    ) -> ApplicationResult<()> {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("kafka metrics configuration")?;
        self.ensure_component_declared(
            ComponentId::Kafka,
            ApplicationPhase::UserHook,
            "kafka metrics configuration",
        )?;
        self.inner.kafka_runtime.install_metrics(name, sink)
    }

    /// 在 UserHook 阶段登记一次原始 Axum Router 变换。
    ///
    /// 这是自动 mapping 端点之外的 Web 逃生舱，适合完成下列工作：
    ///
    /// - 用 `route` 增加手写 HTTP 端点，例如运维接口、流式响应、Webhook 或未使用 mapping 宏的接口；
    /// - 用 `merge`/`nest` 接入第三方或独立模块构造的 Router；
    /// - 用 `layer` 给当前已经存在的自动端点和手写端点增加普通 Tower/Axum 中间件，例如
    ///   CORS、压缩、响应头、业务日志或限流；
    /// - 挂载无法用声明式 mapping 表达的静态资源、监控导出或协议适配端点。
    ///
    /// 本入口**不参与** [`Self::configure_mapping`] 的 effective plan、interceptor 排序、auth/crypto
    /// 安全审计，也不会把手写路由补进 `Application::web().routes()` 的自动端点清单。需要身份阶段、
    /// 解密前后定位或启动前安全审计的接口，应使用 mapping 端点和 `configure_mapping`，不能用一个
    /// 原始 Axum layer 冒充类型化安全拦截器。
    ///
    /// # 装配顺序与覆盖范围
    ///
    /// Web Ready 的固定顺序是：`手动 mapping plan -> 自动 global binding -> 自动端点 ->
    /// configure_router -> 框架探针 -> with_state(Application) -> context path ->
    /// 框架 body-limit/trace/metrics 外层`。
    /// 因此：
    ///
    /// - transform 收到的是已经包含全部自动端点、但尚未补齐 State 和 context path 的
    ///   `Router<Application>`；手写路径应写相对路径，最终会统一套用 `server.context_path`；
    /// - 业务 `layer` 不覆盖随后才加入的 `/healthz`、`/readyz`，避免探针被业务鉴权、限流或统计拦截；
    /// - 框架的 body limit、HTTP trace 和 Web 指标位于更外层，仍会覆盖手写路由、探针和 404；
    /// - 多次登记按登记顺序执行。Axum 的 `layer` 只作用于当时已经存在的路由，因此先挂 layer、
    ///   后由另一个 transform 新增的路由，不保证被前一个 layer 覆盖。
    ///
    /// 该 API 只在启动 Hook 期间开放。Web 组件在 Ready 构造 Router 时一次性取走队列，之后再调用
    /// 返回阶段错误，而不是静默丢弃定制。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// use nasa::web::{from_fn, get};
    ///
    /// app.configure_router(|router| {
    ///     router
    ///         .route("/ops/version", get(version))
    ///         .merge(metrics_router())
    ///         .layer(from_fn(add_business_header))
    /// })?;
    /// ```
    ///
    /// # 参数
    ///
    /// - `transform`：接收尚未补齐状态的路由并返回改造结果的一次性闭包；它必须能跨线程移动，
    ///   且捕获值的所有权全部移入闭包，因为它会在 Ready 阶段由 Runner 线程执行。
    ///
    /// # 错误
    ///
    /// 未声明 `web` 组件、调用时已离开 UserHook，或 Ready 已取走定制队列时返回阶段错误。
    /// transform 自身若在 Ready 构造路由时 panic，在 `panic = "unwind"` 构建中会被转换成 Web
    /// 启动错误；启用框架探针时，重复占用其保留路径也会拒绝启动。
    #[cfg(feature = "web")]
    pub fn configure_router<F>(&self, transform: F) -> ApplicationResult<()>
    where
        F: FnOnce(axum::Router<Application>) -> axum::Router<Application> + Send + 'static,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("router configuration")?;
        self.ensure_component_declared(
            ComponentId::Web,
            ApplicationPhase::UserHook,
            "router configuration",
        )?;
        let mut slot = self
            .inner
            .router_transforms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(transforms) = slot.as_mut() else {
            return Err(ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "router configuration is closed; configure_router is only available while the application startup hook is running",
            ));
        };
        transforms.push(Box::new(transform));
        Ok(())
    }

    /// 在 UserHook 阶段登记一次自动 mapping 端点的拦截器与安全运行时计划变换。
    ///
    /// 它是 mapping 端点的类型化装配入口，可以完成下列工作：
    ///
    /// - `#[interceptor(..., global = true)]` 会由 `mvc_router!` 自动收集并全局装配，无需在本入口
    ///   重复登记；缺省 `global = false` 的声明仍按以下方式手动激活；
    /// - `plan.global(binding)`：给全部匹配安全合同的自动 mapping 业务端点安装全局 interceptor；
    /// - `plan.scope("/orders", binding)`：只给指定静态路径前缀及其子路径安装 interceptor；
    /// - `binding.when_route(...)`：按每条端点的静态 `RoutePolicy` 再筛选 HTTP 方法、路由 ID、
    ///   auth/crypto 合同等；selector 在启动期求值，不读取请求 Header 或动态路径参数；
    /// - `plan.with_runtime(runtime)`：注入同一个共享 [`naweb::MappingRuntime`]，让端点、
    ///   interceptor、认证 provider、密钥环、重放防护、指标和 last-good 热更新读取同一代快照；
    /// - 把 `#[interceptor]` 声明的业务拦截器放进三个固定阶段：
    ///   - `edge`：最外层，可观察原始请求和最终响应，适合关联 ID、审计、统一响应观察；
    ///   - `auth`：位于 `AuthContext` 门禁和请求解密之前，适合 Token、签名、设备身份认证；
    ///   - `plaintext`：位于请求解密之后、响应加密之前，适合必须面对明文语义的业务校验；
    /// - 将自动 global、手动 global、由外到内的 scope、端点 `interceptors(...)` 合并成每条路由
    ///   唯一的 effective plan，再依据 interceptor 的 `order`、`before`、`after` 做同阶段内排序；
    /// - 在监听端口之前统一审计重复 ID、缺失依赖、循环/跨阶段依赖、required/optional 身份门禁、
    ///   auth provider/condition、crypto key、replay guard 等合同；错误会直接阻止应用进入 Ready。
    ///
    /// `public`/未声明 auth 的路由会自动排除 global/scope 的 auth-stage binding，避免把公开端点
    /// 意外变成隐式鉴权端点；端点若显式绑定 auth interceptor 却未声明 required/optional，则拒绝启动。
    ///
    /// 请求入站的安全顺序固定为
    /// `edge -> auth -> AuthContext gate -> request decrypt/replay -> plaintext -> handler`，
    /// 响应按 Tower 套层反向返回，因此 plaintext 位于响应加密之前，edge 能看到最终响应。
    /// 业务只能调整同一固定阶段、同一 global/scope/endpoint 边界内的先后关系，不能用
    /// `order`/`before`/`after` 跨越安全边界。
    ///
    /// # 与 configure_router 的边界
    ///
    /// 本入口只影响 `mvc_router!` 收集的自动 mapping 端点，不新增 HTTP 路由，也不覆盖框架
    /// `/healthz`、`/readyz`。手写 Axum 路由、Router 合并和普通全局 layer 应使用
    /// [`Self::configure_router`]；反过来，原始 Router layer 不会进入上述排序和安全审计。
    /// `global = true` 同样只覆盖自动 mapping 端点，并只支持无 State 或根 `State<Application>`；
    /// 需要自定义窄 State、scope、`when_route` 或动态配置开关时，应继续使用本入口。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// app.configure_mapping(move |plan| {
    ///     plan.with_runtime(mapping_runtime)
    ///         .global(request_audit::binding())
    ///         .scope(
    ///             "/orders",
    ///             account_auth::binding_with::<nasa::Application>(auth_state),
    ///         )
    /// })?;
    /// ```
    ///
    /// # 参数
    ///
    /// - `transform`：接收当前计划并返回新计划；调用顺序与登记顺序一致。闭包必须拥有捕获值并
    ///   可跨线程移动，因为真正的计划构建发生在 Runner 的 Ready 阶段。
    ///
    /// # 错误
    ///
    /// 非 UserHook 阶段、未声明 `web` 组件或 Ready 已封口时返回阶段错误。transform 返回的
    /// [`naweb::MappingBuildError`]、transform panic、effective plan 排序失败或安全运行时审计失败，
    /// 都会转换成带 Web/Ready 阶段信息的启动错误；此时监听端口尚未对外服务。
    #[cfg(feature = "web")]
    pub fn configure_mapping<F>(&self, transform: F) -> ApplicationResult<()>
    where
        F: FnOnce(
                naweb::MappingPlan<Application>,
            ) -> Result<naweb::MappingPlan<Application>, naweb::MappingBuildError>
            + Send
            + 'static,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("mapping configuration")?;
        self.ensure_component_declared(
            ComponentId::Web,
            ApplicationPhase::UserHook,
            "mapping configuration",
        )?;
        let mut slot = self
            .inner
            .mapping_transforms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(transforms) = slot.as_mut() else {
            return Err(ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "mapping configuration is closed; configure_mapping is only available while the application startup hook is running",
            ));
        };
        transforms.push(Box::new(transform));
        Ok(())
    }

    /// 在 UserHook 阶段登记一次独立 TCP/WebSocket 长连接服务的 builder 定制。
    ///
    /// 该入口操作的是 `naws::ServerBuilder`，不是 HTTP Web 组件的 Axum Router。它可以完成：
    ///
    /// - `authorize(...)`：提供握手鉴权回调，根据认证请求和连接上下文返回服务端可信 UID；
    /// - `endpoint(...)`：按长连接 path/namespace 注册事件表，并配置 `on_connect`、
    ///   `on_disconnect`、不可阻塞的 `on_event_inline` 和可 await 的 `on_event_async`；
    /// - `policy(...)`：设置客户端入站消息是否允许触发自动 relay 的授权策略；
    /// - `payload_bridge(...)`：替换 socket.io JSON 参数与内部二进制 payload 的转换桥；
    /// - 覆盖监听和保护参数：TCP/WebSocket 地址、鉴权/心跳超时、全局与单连接 handler 并发、
    ///   活跃/未认证连接上限、frame/message 上限、出站队列容量与字节上限、背压策略；
    /// - `cluster(...)`：注入稳定 node ID 与跨节点 notifier；还可配置独立 data publisher、
    ///   incarnation fencing token、集群初始就绪超时和未就绪时是否降级启动。
    ///
    /// 底层 builder 没有鉴权回调就直接拒绝 `build()`，因此**声明 `ws` 组件的应用必须在所有
    /// configure_ws 定制合并后至少提供一个 `authorize`**；这不是可选装饰，而是长连接服务的
    /// 最小可用契约。endpoint 内的业务身份必须采用鉴权结果，不能信任客户端自报 UID。
    ///
    /// # 装配顺序与覆盖规则
    ///
    /// Ready 阶段固定执行 `application.yml 的 ws 配置 -> configure_ws 登记顺序 -> build ->
    /// 发布 Sender -> bind`。因此业务定制可以覆盖 YAML 已预填的 builder 值；多次调用按登记顺序
    /// 执行，后一次设置同一标量项会覆盖前一次，endpoint 则持续累加。`Sender` 在 bind 前发布，
    /// 所以连接开始触发 handler 时，业务已经可以通过 [`Self::ws_sender`] 广播消息。
    ///
    /// 该 API 只在启动 Hook 期间开放：组件在 Ready 构建服务时一次性取走队列，之后再调用返回
    /// 阶段错误，而不是把定制静默丢弃。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// use nasa::ws::{AuthResult, Endpoint};
    ///
    /// app.configure_ws(|builder| {
    ///     builder
    ///         .authorize(|request, _context| async move {
    ///             authenticate(request).await
    ///         })
    ///         .endpoint(
    ///             Endpoint::builder("/ws/orders")
    ///                 .on_connect(on_connect)
    ///                 .on_disconnect(on_disconnect)
    ///                 .on_event_async("submit", handle_submit)
    ///                 .build(),
    ///         )
    /// })?;
    /// ```
    ///
    /// # 参数
    ///
    /// - `customize`：接收预填好声明式配置的 builder 并返回改造结果的一次性闭包；它在 Ready 阶段由
    ///   Runner 线程执行，因此必须拥有全部捕获值并可跨线程移动。
    ///
    /// # 错误
    ///
    /// 未声明 `ws` 组件、调用时已离开 UserHook，或 Ready 已取走定制队列时返回阶段错误。定制合并后
    /// 缺少 `authorize`、集群参数不完整、限额非法或监听失败，会在 Ready/build/bind 阶段拒绝启动。
    ///
    /// # Panics
    ///
    /// `customize` 在 Ready 执行时不得 panic。底层 endpoint builder 把空事件名、重复事件名和重复
    /// endpoint path 视为配置期编程错误并会 panic，因此这些名称应在启动前保持非空且唯一。
    #[cfg(feature = "ws")]
    pub fn configure_ws<F>(&self, customize: F) -> ApplicationResult<()>
    where
        F: FnOnce(naws::ServerBuilder) -> naws::ServerBuilder + Send + 'static,
    {
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_user_hook_open("ws configuration")?;
        self.ensure_component_declared(
            ComponentId::Ws,
            ApplicationPhase::UserHook,
            "ws configuration",
        )?;
        let mut slot = self
            .inner
            .ws_customizations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(customizations) = slot.as_mut() else {
            return Err(ApplicationError::new(
                ComponentId::Ws,
                ApplicationPhase::Ready,
                "ws configuration is closed; configure_ws is only available while the application startup hook is running",
            ));
        };
        customizations.push(Box::new(customize));
        Ok(())
    }

    /// 返回长连接服务实际绑定的 TCP 地址。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未声明 ws 组件或尚未 bind 时返回 `None`。
    #[cfg(feature = "ws")]
    pub fn ws_addr(&self) -> Option<SocketAddr> {
        self.ws().ok().and_then(|handle| handle.local_addr())
    }

    /// 返回长连接服务实际绑定的 WebSocket 地址。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未启用独立 WebSocket 端口时返回 `None`。
    #[cfg(feature = "ws")]
    pub fn ws_websocket_addr(&self) -> Option<SocketAddr> {
        self.ws().ok().and_then(|handle| handle.websocket_addr())
    }

    /// 获取长连接广播发送器。
    ///
    /// 发送器在 ws 组件 Ready 构建服务时发布；业务事件处理只会在服务开始接受连接之后被触发，
    /// 因此 handler 内调用一定能取到。
    ///
    /// # 参数
    ///
    /// 本方法无参数；未声明 ws 组件或尚未完成 Ready 时返回错误。
    #[cfg(feature = "ws")]
    pub fn ws_sender(&self) -> ApplicationResult<Arc<naws::Sender>> {
        self.ws()?.sender()
    }

    /// Service 模式请求优雅停机；重复调用为 no-op，Batch 模式返回阶段错误。
    ///
    /// # 参数
    ///
    /// 本方法无参数；这里只唤醒 Runner，不直接取消各任务组。
    pub fn shutdown(&self) -> ApplicationResult<()> {
        if self.info().mode() == ApplicationMode::Batch {
            return Err(ApplicationError::new(
                ComponentId::Application,
                ApplicationPhase::Running,
                "shutdown requests are not supported in batch mode",
            ));
        }
        // 这里只发请求；terminal intent 只能由单线程 Runner 提交。
        self.inner.shutdown_requested.cancel();
        Ok(())
    }

    /// 尝试升级迁移期全局 Weak 槽。
    ///
    /// 返回 `Result` 而不是 `Option`：调用点通常在遗留静态入口里用 `?` 传播，错误值需要携带组件与阶段，
    /// 才能把“容器尚未发布/已进入 Closing”和普通业务错误区分开。框架不提供会 panic 的 `global()`。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Batch、UserHook 尚未封存或业务资源进入 Closing 后都会失败。
    pub fn try_global() -> ApplicationResult<Self> {
        global::get().ok_or_else(|| {
            ApplicationError::new(
                ComponentId::Application,
                ApplicationPhase::Running,
                "no application is published for global access; \
                 pass the Application explicitly or check the lifecycle phase",
            )
        })
    }

    /// 在 Service sealed 后发布迁移期全局 Weak 槽。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Runner 是唯一调用者。
    pub(crate) fn install_global(&self) -> ApplicationResult<()> {
        global::install(self)
    }

    /// 仅在槽仍指向当前 Application 时清除全局 Weak。
    ///
    /// # 参数
    ///
    /// 本方法无参数；指针比较避免误删另一个实例。
    pub(crate) fn clear_global(&self) {
        global::clear(self);
    }

    /// 记录 Runner 组件表中出现的一个组件身份。
    ///
    /// # 参数
    ///
    /// - `component`：即将由当前 Runner 托管生命周期的稳定组件身份。
    pub(crate) fn mark_component_declared(&self, component: ComponentId) {
        self.inner
            .declared_components
            .fetch_or(component_mask(component), Ordering::Release);
    }

    /// 校验某个强类型能力入口对应的组件确实存在于 Runner 组件表。
    ///
    /// 该检查必须先于资源查找：否则“组件未声明”会被误报成通用资源缺失，调用方无法区分漏写属性组件
    /// 与组件已经声明但仍未发布底层对象这两类问题。
    ///
    /// # 参数
    ///
    /// - `component`：能力入口所属的稳定组件身份。
    /// - `phase`：本次访问所在阶段，用于形成可定位的统一错误。
    /// - `capability`：不含业务输入的能力名称，用于说明哪一次访问被拒绝。
    #[cfg(any(
        feature = "log",
        feature = "nacos-config",
        feature = "db",
        feature = "redis",
        feature = "cache",
        feature = "kafka",
        feature = "web",
        feature = "ws",
        feature = "nacos-discovery",
        feature = "scheduling"
    ))]
    pub(crate) fn ensure_component_declared(
        &self,
        component: ComponentId,
        phase: ApplicationPhase,
        capability: &'static str,
    ) -> ApplicationResult<()> {
        let declared = self.inner.declared_components.load(Ordering::Acquire);
        if declared & component_mask(component) != 0 {
            return Ok(());
        }
        Err(ApplicationError::new(
            component,
            phase,
            format!(
                "{capability} requires the `{component}` component, but it is not declared for this application"
            ),
        ))
    }

    /// 取走全部路由定制并关闭登记入口。
    ///
    /// 取走即封口：这是"定制能否生效"的线性化点，之后的 `configure_router` 一律得到阶段错误。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只有 Web 组件在 Ready 构造 Router 时调用一次。
    #[cfg(feature = "web")]
    pub(crate) fn take_router_transforms(&self) -> Vec<RouterTransform> {
        self.inner
            .router_transforms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .unwrap_or_default()
    }

    /// 取走全部 mapping 计划变换并关闭登记入口。
    ///
    /// Web Ready 必须先封口并构建计划，再调用自动路由工厂；这样监听开始后不会出现
    /// 路由图或拦截器顺序漂移。
    #[cfg(feature = "web")]
    pub(crate) fn take_mapping_transforms(&self) -> Vec<MappingTransform> {
        self.inner
            .mapping_transforms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .unwrap_or_default()
    }

    /// 发布已经通过路由审计的共享 MappingRuntime。
    #[cfg(feature = "web")]
    pub(crate) fn publish_mapping_runtime(
        &self,
        runtime: Arc<naweb::MappingRuntime>,
    ) -> ApplicationResult<()> {
        self.inner.mapping_runtime.set(runtime).map_err(|_| {
            ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "mapping runtime was already published",
            )
        })
    }

    /// 返回 Web Ready 发布的只读 MappingRuntime 句柄。
    #[cfg(feature = "web")]
    pub(crate) fn mapping_runtime(&self) -> Option<Arc<naweb::MappingRuntime>> {
        self.inner.mapping_runtime.get().cloned()
    }

    /// 返回 Web 请求观测中间件使用的共享运行时状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不包含 Application、路由服务图、监听器或清理动作。
    #[cfg(feature = "web")]
    pub(crate) fn web_runtime(&self) -> Arc<crate::web_handle::WebRuntimeState> {
        Arc::clone(&self.inner.web_runtime)
    }

    /// 返回日志组件写入初始化结果的共享运行时状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；状态不包含日志管理器和文件刷盘 guard。
    #[cfg(feature = "log")]
    pub(crate) fn log_runtime(&self) -> Arc<crate::capabilities::LogRuntimeState> {
        Arc::clone(&self.inner.log_runtime)
    }

    /// 返回配置中心组件发布底层客户端弱引用的共享状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；能力句柄不会通过该状态取得关闭所有权。
    #[cfg(feature = "nacos-config")]
    pub(crate) fn nacos_config_runtime(&self) -> Arc<crate::capabilities::NacosConfigRuntimeState> {
        Arc::clone(&self.inner.nacos_config_runtime)
    }

    /// 返回服务发现组件发布会话弱引用的共享状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；强引用仍只存在于组件和清理动作中。
    #[cfg(feature = "nacos-discovery")]
    pub(crate) fn nacos_discovery_runtime(
        &self,
    ) -> Arc<crate::capabilities::NacosDiscoveryRuntimeState> {
        Arc::clone(&self.inner.nacos_discovery_runtime)
    }

    /// 返回调度组件发布启动和选主状态的共享单元。
    ///
    /// # 参数
    ///
    /// 本方法无参数；底层关闭入口不在该状态中。
    #[cfg(feature = "scheduling")]
    pub(crate) fn scheduling_runtime(&self) -> Arc<crate::capabilities::SchedulingRuntimeState> {
        Arc::clone(&self.inner.scheduling_runtime)
    }

    /// 返回 Kafka 组件发布 client 与 UserHook 定制队列的共享状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不把原始 proxy 或 shutdown action 暴露给业务资源容器。
    #[cfg(feature = "kafka")]
    pub(crate) fn kafka_runtime(&self) -> Arc<crate::kafka::KafkaRuntimeState> {
        Arc::clone(&self.inner.kafka_runtime)
    }

    /// 返回进程级统一指标 hub:各领域按 descriptor 记录到此。
    ///
    /// # 参数
    ///
    /// 本方法无参数;返回共享 hub 的 Arc 副本,克隆只增加所有权不复制注册表。
    #[cfg(any(feature = "kafka", feature = "web"))]
    pub(crate) fn metrics_hub(&self) -> Arc<nametrics_core::MetricHub> {
        Arc::clone(&self.inner.metrics_hub)
    }

    /// 把一个兼容领域源(自持 registry、自渲染 Prometheus)并入进程级统一 hub。
    ///
    /// 供 `nasa` 门面层把 **nafana** 等 napp 不直接依赖的领域接入同一 `/metrics`:门面构造领域源后
    /// 在业务 UserHook 调用本方法,其族即随框架 `/metrics` 一并渲染,并纳入 descriptor 冲突审计。
    ///
    /// # 参数
    ///
    /// - `source`:领域自渲染源;其 descriptor 会并入统一 catalog,值仍由源自渲染。
    ///
    /// # 错误
    ///
    /// 源的任一 descriptor 与已注册项语义冲突时返回阶段错误。
    #[cfg(any(feature = "kafka", feature = "web"))]
    pub fn register_metrics_source(
        &self,
        source: Arc<dyn nametrics_core::LegacyMetricsSource>,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("metrics source registration")?;
        self.inner
            .metrics_hub
            .register_legacy_source(source)
            .map_err(|conflict| {
                ApplicationError::new(
                    ComponentId::Web,
                    ApplicationPhase::UserHook,
                    format!(
                        "metric source descriptor `{}` conflicts with an existing registration",
                        conflict.name
                    ),
                )
            })
    }

    /// 注入业务幂等 store,在 UserHook 阶段调用一次。
    ///
    /// 注入后 Web 组件在 Ready 装配阶段把幂等中间件接进请求路径(最内层包裹 handler,重放短路)。
    /// 未注入则不启用幂等层。重复注入返回 UserHook 阶段错误。
    ///
    /// # 参数
    ///
    /// - `store`:幂等 store 句柄(内存/DB/Redis 实现同一 trait)。
    #[cfg(feature = "web")]
    pub fn set_idempotency_store(
        &self,
        store: crate::idempotency::SharedIdempotencyStore,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("idempotency store injection")?;
        self.inner.idempotency_store.set(store).map_err(|_| {
            ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::UserHook,
                "idempotency store has already been injected".to_string(),
            )
        })
    }

    /// 注入业务授权策略注册表,在 UserHook 阶段调用一次。
    ///
    /// 注入后 Web 组件在 Ready 装配阶段把授权中间件接进请求路径(授权在幂等之外——未授权请求
    /// 不进入幂等/handler)。未注入则不启用授权层。重复注入返回 UserHook 阶段错误。
    ///
    /// # 参数
    ///
    /// - `registry`:授权策略注册表(热更新保 last-good)。
    #[cfg(feature = "web")]
    pub fn set_authz_registry(
        &self,
        registry: crate::authz::SharedPolicyRegistry,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("authz registry injection")?;
        self.inner.authz_registry.set(registry).map_err(|_| {
            ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::UserHook,
                "authz policy registry has already been injected".to_string(),
            )
        })
    }

    /// 已注入的幂等 store(Web 装配用);未注入返回 `None`。
    #[cfg(feature = "web")]
    pub(crate) fn idempotency_store(&self) -> Option<crate::idempotency::SharedIdempotencyStore> {
        self.inner.idempotency_store.get().cloned()
    }

    /// 已注入的授权策略注册表(Web 装配用);未注入返回 `None`。
    #[cfg(feature = "web")]
    pub(crate) fn authz_registry(&self) -> Option<crate::authz::SharedPolicyRegistry> {
        self.inner.authz_registry.get().cloned()
    }

    /// 在 UserHook 注入对象级授权 provider；provider 错误/超时均 fail closed。
    #[cfg(feature = "web")]
    pub fn set_object_authorizer(
        &self,
        provider: crate::authz::SharedObjectAuthorizer,
        timeout: std::time::Duration,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("object authorizer injection")?;
        if timeout.is_zero() || timeout > crate::runner::MAX_LIFECYCLE_TIMEOUT {
            return Err(ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::UserHook,
                "object authorizer timeout must be within (0, 365 days]",
            ));
        }
        self.inner
            .object_authorizer
            .set((provider, timeout))
            .map_err(|_| {
                ApplicationError::new(
                    ComponentId::Web,
                    ApplicationPhase::UserHook,
                    "object authorizer has already been injected",
                )
            })
    }

    /// 返回已冻结的对象授权 provider 与请求预算，供 Web Ready 阶段装配授权中间件。
    #[cfg(feature = "web")]
    pub(crate) fn object_authorizer(
        &self,
    ) -> Option<(crate::authz::SharedObjectAuthorizer, std::time::Duration)> {
        self.inner.object_authorizer.get().cloned()
    }

    /// 注入业务认证器,在 UserHook 阶段调用一次。
    ///
    /// 注入后 Web 组件在 Ready 装配阶段把 authentication 中间件接进请求路径(装在授权之外——认证永远
    /// 早于授权)。未注入则不启用认证层。重复注入返回 UserHook 阶段错误。
    ///
    /// # 参数
    ///
    /// - `authenticator`:JWKS 注册表 + token 校验策略的组合句柄。
    #[cfg(feature = "web")]
    pub fn set_authenticator(
        &self,
        authenticator: crate::authn::SharedAuthenticator,
    ) -> ApplicationResult<()> {
        self.ensure_user_hook_open("authenticator injection")?;
        self.inner.authenticator.set(authenticator).map_err(|_| {
            ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::UserHook,
                "authenticator has already been injected".to_string(),
            )
        })
    }

    /// 已注入的认证器(Web 装配用);未注入返回 `None`。
    #[cfg(feature = "web")]
    pub(crate) fn authenticator(&self) -> Option<crate::authn::SharedAuthenticator> {
        self.inner.authenticator.get().cloned()
    }

    /// 由 `AuthComponent` 在 Ready 阶段发布配置驱动的认证器。
    ///
    /// 与业务 UserHook 的 `set_authenticator` 写同一槽位:若业务已手动注入,则冲突并返回带 Auth 组件
    /// 身份的错误——同一进程只能二选一(声明 `auth` 组件由配置驱动,或业务手动注入),不允许双写。
    ///
    /// # 参数
    ///
    /// - `authenticator`:`AuthComponent` 由 `auth` 配置 warmup 出的 JWKS + 策略组合句柄。
    #[cfg(feature = "web")]
    pub(crate) fn publish_authenticator_from_component(
        &self,
        authenticator: crate::authn::SharedAuthenticator,
    ) -> ApplicationResult<()> {
        self.inner.authenticator.set(authenticator).map_err(|_| {
            ApplicationError::new(
                ComponentId::Auth,
                ApplicationPhase::Ready,
                "authenticator already injected; declare the `auth` component or call set_authenticator, not both",
            )
        })
    }

    /// 为一个运行依赖注册初始未就绪的动态贡献项。
    ///
    /// # 参数
    ///
    /// - `component`：负责更新该贡献项并承担错误归因的组件身份。
    /// - `name`：框架生成的非空稳定名称，不得包含地址、凭据或业务负载。
    ///
    /// # 返回
    ///
    /// 首次注册成功时返回该组件独占的原子更新句柄。
    ///
    /// # 参数
    ///
    /// - `component`:贡献项归属的稳定组件身份,用于形成可定位错误。
    /// - `name`:框架构造的非空稳定名称(如 `db:default`),不含配置秘密。
    /// - `policy`:该依赖影响全局就绪的策略(关键/非关键、失败/恢复阈值、stale 窗)。
    ///
    /// # 错误
    ///
    /// 名称为空、重名、封口后或阈值非法时返回 Start 阶段错误。
    #[cfg(any(
        feature = "kafka",
        feature = "db",
        feature = "redis",
        feature = "nacos-config",
        feature = "nacos-discovery",
        feature = "telemetry",
        feature = "cache",
        feature = "web"
    ))]
    pub(crate) fn register_readiness(
        &self,
        component: ComponentId,
        name: impl Into<Arc<str>>,
        policy: crate::readiness::ReadinessPolicy,
    ) -> ApplicationResult<crate::readiness::ReadinessContributor> {
        self.inner
            .readiness
            .register_component(Arc::<str>::from(component.to_string()), name, policy)
            .map_err(|err| {
                let detail = match err {
                    crate::readiness::RegisterError::EmptyName => {
                        "readiness contributor name is empty"
                    }
                    crate::readiness::RegisterError::Duplicate => {
                        "readiness contributor name is already registered"
                    }
                    crate::readiness::RegisterError::Sealed => {
                        "readiness registry is sealed; contributors must register before user-hook completes"
                    }
                    crate::readiness::RegisterError::InvalidPolicy => {
                        "readiness policy thresholds must be greater than zero"
                    }
                };
                ApplicationError::new(component, ApplicationPhase::Start, detail)
            })
    }

    /// 发布 Start 阶段已经校验通过的 Web 上下文前缀。
    ///
    /// # 参数
    ///
    /// - `context_path`：本次进程固定使用的统一路径前缀。
    #[cfg(feature = "web")]
    pub(crate) fn set_web_context_path(&self, context_path: Arc<str>) -> ApplicationResult<()> {
        self.inner
            .web_runtime
            .set_context_path(context_path)
            .map_err(|_| {
                ApplicationError::new(
                    ComponentId::Web,
                    ApplicationPhase::Start,
                    "web context path is already published",
                )
            })
    }

    /// 发布长连接服务 bind 成功后的真实监听地址。
    ///
    /// # 参数
    ///
    /// - `addr`：TCP 监听地址，端口可能由系统分配。
    /// - `websocket`：独立 WebSocket 监听地址；未启用时为 `None`。
    #[cfg(feature = "ws")]
    pub(crate) fn set_ws_addrs(
        &self,
        addr: SocketAddr,
        websocket: Option<SocketAddr>,
    ) -> ApplicationResult<()> {
        if self.inner.ws_runtime.publish_addrs(addr, websocket) {
            return Ok(());
        }
        Err(ApplicationError::new(
            ComponentId::Ws,
            ApplicationPhase::Ready,
            "ws listener address is already published",
        ))
    }

    /// 发布长连接广播发送器。
    ///
    /// # 参数
    ///
    /// - `sender`：ws 组件构建服务后取得的共享发送器句柄。
    #[cfg(feature = "ws")]
    pub(crate) fn set_ws_sender(&self, sender: Arc<naws::Sender>) -> ApplicationResult<()> {
        if self.inner.ws_runtime.publish_sender(&sender) {
            return Ok(());
        }
        Err(ApplicationError::new(
            ComponentId::Ws,
            ApplicationPhase::Ready,
            "ws sender is already published",
        ))
    }

    /// 取走全部长连接定制并关闭登记入口。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只有 ws 组件在 Ready 构建服务时调用一次。
    #[cfg(feature = "ws")]
    pub(crate) fn take_ws_customizations(&self) -> Vec<WsCustomization> {
        self.inner
            .ws_customizations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .unwrap_or_default()
    }

    /// 登记一个组件的配置重应用句柄。
    ///
    /// 只允许可热刷组件在自身 Start 阶段调用：驱动创建于 Ready，因此登记与消费天然分段，
    /// 不需要额外的阶段封口协议。
    ///
    /// # 参数
    ///
    /// - `applier`：持有该组件运行态共享句柄的重应用实现。
    // 目前只有 log 是可热刷组件；驱动侧 feature 单独打开时该入口空置属于预期形态。
    #[cfg(any(feature = "log", feature = "nacos-config"))]
    #[cfg_attr(not(feature = "log"), allow(dead_code))]
    pub(crate) fn register_config_applier(&self, applier: Arc<dyn crate::reload::ConfigApplier>) {
        self.inner
            .config_appliers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(applier);
    }

    /// 返回全部已登记的配置重应用句柄。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只有配置热刷新驱动在 Ready 阶段读取一次。
    #[cfg(any(feature = "log", feature = "nacos-config"))]
    #[cfg_attr(not(feature = "nacos-config"), allow(dead_code))]
    pub(crate) fn config_appliers(&self) -> Vec<Arc<dyn crate::reload::ConfigApplier>> {
        self.inner
            .config_appliers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// 返回只用于唤醒 Runner 的停机请求令牌。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该令牌不是用户任务组的父令牌。
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.inner.shutdown_requested.clone()
    }

    /// 在 Runner 即将轮询业务启动 Hook 时开放公共登记入口。
    ///
    /// # 参数
    ///
    /// 本方法无参数；每个 Application 生命周期只允许调用一次。
    pub(crate) fn open_user_hook(&self) {
        let was_open = self.inner.user_hook_open.swap(true, Ordering::AcqRel);
        debug_assert!(!was_open, "user hook registration gate must open only once");
    }

    /// 在 Runner 首次观察到 Hook 终止事件后关闭公共登记入口。
    ///
    /// 关闭先于任务通道收口、资源封存和反向清理。这样仍在运行的业务 future 即使晚一步获得调度，
    /// 也只能得到阶段错误，不能在清理开始后继续增加资源或任务。
    ///
    /// # 参数
    ///
    /// 本方法无参数；重复关闭安全。
    pub(crate) fn close_user_hook(&self) {
        // 关闭与全部同步登记共用一个临界区；取得锁之后没有已通过检查但尚未写入的调用。
        let _gate = self
            .inner
            .user_registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.inner.user_hook_open.store(false, Ordering::Release);
        // 队列锁仍需在门关闭后短暂取得，确保后续 Ready 的一次性 take 不与末次合法 push 交叠。
        #[cfg(feature = "web")]
        drop(
            self.inner
                .router_transforms
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        #[cfg(feature = "ws")]
        drop(
            self.inner
                .ws_customizations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }

    /// 返回 Runner 和组件使用的资源注册表。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回借用不暴露内部容器所有权。
    pub(crate) fn resources(&self) -> &ResourceRegistry {
        &self.inner.resources
    }

    /// 原子发布已经校验完成的下一配置视图。
    ///
    /// # 参数
    ///
    /// - `next`：版本、快照和目标状态已经保持一致的新视图。
    pub(crate) fn publish_config(&self, next: Arc<ConfigView>) {
        self.inner.config.publish(next);
    }

    /// 用 Nacos 合并后的最终配置树替换初始 bootstrap 视图。
    ///
    /// 保持 version=1 与既有 reload 状态：初始合并仍属版本 1，此刻尚无订阅者（Bootstrap 阶段）。
    ///
    /// # 参数
    ///
    /// - `value`：本地树与远端 overlay 合并、插值后的完整最终树。
    /// - `sources`：形成该版本的来源摘要（本地文件 + Nacos 文档）。
    #[cfg(feature = "nacos-config")]
    pub(crate) fn set_bootstrap_config(
        &self,
        value: serde_json::Value,
        sources: Vec<crate::ConfigSource>,
    ) -> ApplicationResult<()> {
        let current = self.config_view();
        let version = current.snapshot().version();
        // secret 在脱敏前解析;Bootstrap 合并失败即 fail-closed。
        let view =
            crate::config::resolve_view(version, value, sources, current.reload_statuses().clone())
                .map_err(|error| {
                    ApplicationError::with_source(
                        ComponentId::Config,
                        ApplicationPhase::Bootstrap,
                        "secret resolution failed while merging bootstrap config",
                        error,
                    )
                })?;
        self.publish_config(view);
        Ok(())
    }

    /// 发布一个热刷新配置版本：版本自增并携带新的目标应用状态表。
    ///
    /// # 参数
    ///
    /// - `expected_current_version`：驱动计算状态表时读取到的当前版本，用于拒绝并发覆盖。
    /// - `value`：本轮全量重拉合并后的完整树。
    /// - `sources`：本轮包含的来源摘要。
    /// - `statuses`：该版本对应的各目标应用状态。
    #[cfg(feature = "nacos-config")]
    pub(crate) fn publish_reloaded_config(
        &self,
        expected_current_version: u64,
        value: serde_json::Value,
        sources: Vec<crate::ConfigSource>,
        statuses: std::collections::HashMap<crate::ReloadTarget, crate::ReloadStatus>,
    ) -> ApplicationResult<u64> {
        let current = self.config_view();
        if current.snapshot().version() != expected_current_version {
            return Err(ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "config view changed while a reload candidate was being prepared",
            ));
        }
        let next = expected_current_version.checked_add(1).ok_or_else(|| {
            ApplicationError::new(
                ComponentId::Config,
                ApplicationPhase::Running,
                "config snapshot version reached its maximum value",
            )
        })?;
        // secret 在脱敏前解析;解析失败保留 last-good(不发布,对外 generation 不变)。
        let view =
            crate::config::resolve_view(next, value, sources, statuses).map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Config,
                    ApplicationPhase::Running,
                    "secret resolution failed while publishing a reloaded config",
                    error,
                )
            })?;
        self.publish_config(view);
        Ok(next)
    }

    /// 一次性发布 Web Ready 阶段得到的运行时身份。
    ///
    /// 能力句柄使用同一个 OnceLock 同时观察监听地址和路由清单；兼容地址入口随后复制地址值。
    /// 同一 Application 不允许替换监听器身份或路由清单。
    ///
    /// # 参数
    ///
    /// - `address`：监听器 bind 成功后读取的实际本地地址，端口可以来自系统分配。
    /// - `routes`：已经完成冲突预检并按稳定顺序排列的不可变路由清单。
    #[cfg(feature = "web")]
    pub(crate) fn publish_web_runtime(
        &self,
        address: SocketAddr,
        routes: Arc<[crate::web_handle::RouteInfo]>,
    ) -> ApplicationResult<()> {
        if self.inner.web_addr.get().is_some() || self.inner.web_runtime.is_published() {
            return Err(ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "web runtime information is already published",
            ));
        }
        // 能力句柄首先得到不可分割的地址与清单记录；兼容地址槽随后只复制一次 Copy 地址值。
        if !self.inner.web_runtime.publish(address, routes) {
            return Err(ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "web runtime information is already published",
            ));
        }
        self.inner.web_addr.set(address).map_err(|_| {
            ApplicationError::new(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "web listener address is already published",
            )
        })
    }

    /// 发布 Starting 到 Ready 的唯一合法转换。
    ///
    /// # 参数
    ///
    /// 本方法无参数；所有 Ready action 成功后才能调用。
    pub(crate) fn mark_ready(&self) -> ApplicationResult<()> {
        self.inner
            .state
            .transition(ApplicationState::Starting, ApplicationState::Ready)
    }

    /// 发布 Ready 到 Stopping 的转换。
    ///
    /// # 参数
    ///
    /// 本方法无参数；首次终态必须先于该状态写入。
    pub(crate) fn mark_stopping(&self) -> ApplicationResult<()> {
        self.inner
            .state
            .transition(ApplicationState::Ready, ApplicationState::Stopping)
    }

    /// 发布 Starting 到 Stopping 的启动失败、启动信号或 Batch 完成转换。
    ///
    /// # 参数
    ///
    /// 本方法无参数；首次终态必须先于该状态写入。
    pub(crate) fn mark_startup_stopping(&self) -> ApplicationResult<()> {
        self.inner
            .state
            .transition(ApplicationState::Starting, ApplicationState::Stopping)
    }

    /// 在清理完成后发布正常终态 Stopped。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只允许从 Stopping 转换。
    pub(crate) fn mark_stopped(&self) -> ApplicationResult<()> {
        self.inner
            .state
            .transition(ApplicationState::Stopping, ApplicationState::Stopped)
    }

    /// 在清理完成后发布失败终态 Failed。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只允许从 Stopping 转换，禁止发现故障时提前写入。
    pub(crate) fn mark_failed(&self) -> ApplicationResult<()> {
        self.inner
            .state
            .transition(ApplicationState::Stopping, ApplicationState::Failed)
    }

    /// 尝试一次提交首次终态。
    ///
    /// # 参数
    ///
    /// - `intent`：Runner 已经分类的非空终止意图；后到事件不能覆盖它。
    pub(crate) fn set_terminal(&self, intent: TerminalIntent) -> bool {
        self.inner.terminal.try_set(intent)
    }

    /// 以 Acquire 语义读取首次终态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；信号控制面只在观察到 Stopping 后使用结果。
    pub(crate) fn terminal_intent(&self) -> TerminalIntent {
        self.inner.terminal.load()
    }

    /// 把公共任务工厂装箱后送入 bounded Supervisor 队列并等待 ACK。
    ///
    /// # 参数
    ///
    /// - `name`：已经转换为共享字符串的任务名。
    /// - `kind`：决定提前退出是否触发主失败的任务角色。
    /// - `task`：接收独立子取消令牌的任务 future 工厂。
    async fn spawn_task<N, F, Fut>(
        &self,
        name: N,
        kind: TaskKind,
        task: F,
    ) -> ApplicationResult<TaskId>
    where
        N: Into<Arc<str>>,
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.ensure_user_hook_open("managed task registration")?;
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ApplicationError::new(
                ComponentId::Supervisor,
                ApplicationPhase::UserHook,
                "managed task name cannot be empty",
            ));
        }
        let token = self.inner.supervisor.task_token();
        let future: ManagedTaskFuture = Box::pin(task(token));
        self.inner.supervisor.register(name, kind, future).await
    }

    /// 校验当前调用仍发生在业务启动 Hook 的有效登记窗口内。
    ///
    /// # 参数
    ///
    /// - `operation`：只包含 API 类别、不包含业务输入的稳定诊断名称。
    fn ensure_user_hook_open(&self, operation: &str) -> ApplicationResult<()> {
        if self.inner.user_hook_open.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(ApplicationError::new(
            ComponentId::UserHook,
            ApplicationPhase::UserHook,
            format!(
                "{operation} is closed; it is only available while the application startup hook is running"
            ),
        ))
    }
}

/// 把稳定组件身份映射为声明位集合中的唯一 bit。
///
/// # 参数
///
/// - `component`：需要编码到 Runner 声明集合中的组件身份。
const fn component_mask(component: ComponentId) -> u32 {
    // 位集合升到 u32:14 个现有 offset(0..=13)之外仍余 18 位,容纳后续 Cache、Telemetry、
    // Auth 等组件。显式 match 而非 `component as u32` 位移：新增内部身份时不会悄悄改变
    // 已分配 bit 或超出位宽，唯一性由编译期断言保证。
    let offset = component_bit_offset(component);
    1_u32 << offset
}

/// 每个 `ComponentId` 到声明位集合中唯一 bit 偏移的显式映射。
///
/// 新增组件时必须在此追加一个此前未用、且小于 32 的偏移;唯一性与上界由发布门禁校验。
const fn component_bit_offset(component: ComponentId) -> u32 {
    match component {
        ComponentId::Application => 0,
        ComponentId::Config => 1,
        ComponentId::Log => 2,
        ComponentId::NacosConfig => 3,
        ComponentId::Db => 4,
        ComponentId::Redis => 5,
        ComponentId::Web => 6,
        ComponentId::Ws => 7,
        ComponentId::NacosDiscovery => 8,
        ComponentId::Scheduling => 9,
        ComponentId::Resources => 10,
        ComponentId::Supervisor => 11,
        ComponentId::UserHook => 12,
        ComponentId::Kafka => 13,
        ComponentId::Auth => 14,
        ComponentId::Telemetry => 15,
        ComponentId::Cache => 16,
    }
}

/// 编译期断言:所有 `ComponentId` 的 bit 偏移唯一且小于 32。
///
/// 新增 `ComponentId` 变体时必须同步扩充 `component_bit_offset` 的 match(exhaustive,漏写
/// 直接编译失败)与下面的 `ALL` 列表;偏移重复或越界会在编译期报错,不会退化成运行期误判。
const _: () = {
    const ALL: [ComponentId; 17] = [
        ComponentId::Application,
        ComponentId::Config,
        ComponentId::Log,
        ComponentId::NacosConfig,
        ComponentId::Db,
        ComponentId::Redis,
        ComponentId::Web,
        ComponentId::Ws,
        ComponentId::NacosDiscovery,
        ComponentId::Scheduling,
        ComponentId::Resources,
        ComponentId::Supervisor,
        ComponentId::UserHook,
        ComponentId::Kafka,
        ComponentId::Auth,
        ComponentId::Telemetry,
        ComponentId::Cache,
    ];
    let mut i = 0;
    while i < ALL.len() {
        assert!(
            component_bit_offset(ALL[i]) < 32,
            "component bit offset must be < 32 for the u32 declared-components bitmap"
        );
        let mut j = i + 1;
        while j < ALL.len() {
            assert!(
                component_bit_offset(ALL[i]) != component_bit_offset(ALL[j]),
                "component bit offsets must be unique"
            );
            j += 1;
        }
        i += 1;
    }
};
