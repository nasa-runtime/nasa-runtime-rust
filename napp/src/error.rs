use std::fmt;

/// 运行时内置组件标识，用于稳定归类配置、启动、运行和停机错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComponentId {
    /// 应用生命周期核心。
    Application,
    /// 配置加载、快照和热更新。
    Config,
    /// 日志初始化和刷新。
    Log,
    /// Nacos 配置中心接入。
    NacosConfig,
    /// 数据库连接与事务资源。
    Db,
    /// Redis 连接和协议能力。
    Redis,
    /// Trace 生产、导出和停机刷新。
    Telemetry,
    /// 进程内及分布式缓存。
    Cache,
    /// Kafka 发布、消费和健康状态。
    Kafka,
    /// 事务型 Outbox 持久化投递循环。
    Outbox,
    /// Saga 编排、参与方能力与 durable timer。
    Saga,
    /// 身份认证运行时。
    Auth,
    /// HTTP 路由和治理入口。
    Web,
    /// TCP/WebSocket 长连接入口。
    Ws,
    /// Nacos 服务注册与发现。
    NacosDiscovery,
    /// 异步任务和定时调度。
    Scheduling,
    /// 受管业务资源注册表。
    Resources,
    /// 后台任务监督器。
    Supervisor,
    /// 业务启动钩子。
    UserHook,
}

impl fmt::Display for ComponentId {
    /// 业务作用：写出不会包含配置值的稳定组件名称，供生命周期错误安全归因。
    ///
    /// 参数说明：
    /// - `f`：接收组件名称的格式化缓冲区。
    ///
    /// 返回：名称写入成功时返回 `Ok`，底层格式化缓冲区失败时透传错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Application => "application",
            Self::Config => "config",
            Self::Log => "log",
            Self::NacosConfig => "nacos-config",
            Self::Db => "db",
            Self::Redis => "redis",
            Self::Telemetry => "telemetry",
            Self::Cache => "cache",
            Self::Kafka => "kafka",
            Self::Outbox => "outbox",
            Self::Saga => "saga",
            Self::Auth => "auth",
            Self::Web => "web",
            Self::Ws => "ws",
            Self::NacosDiscovery => "nacos-discovery",
            Self::Scheduling => "scheduling",
            Self::Resources => "resources",
            Self::Supervisor => "supervisor",
            Self::UserHook => "user-hook",
        };
        f.write_str(value)
    }
}

/// 错误或组件动作发生时所属的生命周期阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplicationPhase {
    /// 读取并验证最初配置的引导阶段。
    Bootstrap,
    /// 组件创建资源但尚未对外就绪的阶段。
    Start,
    /// 执行业务注册和装配钩子的阶段。
    UserHook,
    /// 在业务初始化前执行 migration 和出站依赖门禁的阶段。
    Prepare,
    /// 按全局屏障执行业务 initializer 的阶段。
    Initialization,
    /// 关闭初始化期登记并封存资源与 readiness 的阶段。
    Seal,
    /// 完成外部探针并发布就绪资源的阶段。
    Ready,
    /// 应用已经对外服务的阶段。
    Running,
    /// 停止入口并按预算释放资源的阶段。
    Stopping,
    /// 所有受管资源均已退出的终态。
    Stopped,
}

impl fmt::Display for ApplicationPhase {
    /// 业务作用：写出稳定的小写阶段名称。
    ///
    /// # 参数
    ///
    /// - `f`：接收阶段名称的格式化缓冲区。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Bootstrap => "bootstrap",
            Self::Start => "start",
            Self::UserHook => "user-hook",
            Self::Prepare => "prepare",
            Self::Initialization => "initialization",
            Self::Seal => "seal",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        };
        f.write_str(value)
    }
}

/// 所有框架层失败统一使用该错误形状，避免丢失组件和生命周期阶段。
#[derive(Debug, thiserror::Error)]
#[error("{component} failed during {phase}: {message}")]
pub struct ApplicationError {
    component: ComponentId,
    phase: ApplicationPhase,
    message: String,
    #[source]
    source: Option<anyhow::Error>,
    /// 主失败是否已经在组件清理前写入统一诊断通道。
    reported: bool,
}

impl ApplicationError {
    /// 业务作用：创建没有底层错误链的框架错误。
    ///
    /// # 参数
    ///
    /// - `component`：负责归类本次失败的组件。
    /// - `phase`：失败被观察到的生命周期阶段。
    /// - `message`：不包含业务秘密的稳定摘要。
    pub fn new(
        component: ComponentId,
        phase: ApplicationPhase,
        message: impl Into<String>,
    ) -> Self {
        Self {
            component,
            phase,
            message: message.into(),
            source: None,
            reported: false,
        }
    }

    /// 业务作用：创建保留底层错误链的框架错误。
    ///
    /// # 参数
    ///
    /// - `component`：负责归类本次失败的组件。
    /// - `phase`：失败被观察到的生命周期阶段。
    /// - `message`：不包含业务秘密的稳定摘要。
    /// - `source`：只供程序化诊断使用、输出前必须经过统一脱敏的底层错误。
    pub fn with_source(
        component: ComponentId,
        phase: ApplicationPhase,
        message: impl Into<String>,
        source: impl Into<anyhow::Error>,
    ) -> Self {
        Self {
            component,
            phase,
            message: message.into(),
            source: Some(source.into()),
            reported: false,
        }
    }

    /// 业务作用：返回错误所属组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值可用于稳定指标标签。
    pub fn component(&self) -> ComponentId {
        self.component
    }

    /// 业务作用：返回错误所属生命周期阶段。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不依赖错误文本解析。
    pub fn phase(&self) -> ApplicationPhase {
        self.phase
    }

    /// 业务作用：返回框架生成的稳定错误摘要。
    ///
    /// # 参数
    ///
    /// 本方法无参数；底层 source 不会被拼接进该字符串。
    pub fn message(&self) -> &str {
        &self.message
    }

    /// 业务作用：标记该主失败已经由 Runner 在反向清理前输出。
    ///
    /// # 参数
    ///
    /// 本方法无参数；仅生命周期执行器用于避免同步入口重复报告同一个错误。
    pub(crate) fn mark_reported(&mut self) {
        self.reported = true;
    }

    /// 业务作用：判断该错误是否已经作为唯一主失败输出。
    ///
    /// # 参数
    ///
    /// 本方法无参数；同步入口据此只补报未经过常规 Runner 失败出口的内部错误。
    pub(crate) fn was_reported(&self) -> bool {
        self.reported
    }
}

/// 应用生命周期 API 统一使用的结果别名。
pub type ApplicationResult<T> = Result<T, ApplicationError>;
