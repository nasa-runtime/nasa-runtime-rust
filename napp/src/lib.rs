//! NASA 应用生命周期运行时核心。
//!
//! 本 crate 统一拥有配置快照、组件启动与反向停机、受管任务以及 Ready 发布。业务 initializer
//! 是 `Prepare` 与 `Seal` 之间的固定屏障：静态属性入口与 Service 启动 Hook 的运行时入口先合并
//! 冻结为同一份依赖图；migration 和出站依赖准备完成后再严格执行全部 `before`、全部
//! `initialize`、全部 `after`。三轮全部成功前不会开放入站监听、消费循环或服务发现。
//!
//! 依赖边优先于数值 `order`；失败、panic、启动超时或取消会阻止 Ready，并让已经取得所有权的
//! action、资源和任务沿 active stack 逆序关闭。外部系统中已经提交的事实不属于本地回滚能力，
//! initializer 必须通过事务或稳定幂等键保证可安全重跑。
//!
//! 启用 Saga 组件时，Application 隐式拥有 DB 与 Outbox 生命周期，在 Ready 前校验 definition、
//! descriptor、历史非终态实例和参与方信任投影，再监督 durable timer 与所选受管消费循环。
//! Application 只负责资源所有权和启停顺序；Saga 的 CAS、Inbox/Outbox 与补偿正确性仍由
//! `nasaga-runtime` 的持久合同承担。
//!
//! 启用 gRPC 组件时，UserHook 只登记统一 codegen 生成的业务 service，Prepare 永久封口 registry，
//! 全部 initializer 成功后的 Ready 才自动装配 Router、health、可选 reflection 并绑定 listener。
//! 组件独占 shutdown，持续 accept 失败会摘除 readiness 并保持有界恢复，serve 所有权丢失则触发
//! 统一失败停机。启用 Saga gRPC 入站计划时，generated command/result service 也在同一封口前自动
//! 登记，不建立第二个 Router、身份解析或停机 owner。
//!
//! 启用 Web 组件时，Application 在 Ready 阶段独占明文 TCP listener 与路由服务图。默认只接受
//! HTTP/1；`server.http2.enabled=true` 后同一端口按固定前言接受 h2c prior knowledge，并继续兼容
//! HTTP/1。连接、stream、流控与报文边界在启动期冻结；停机先关闭 accept，再分别关闭 HTTP/1
//! keep-alive 或向 HTTP/2 发送 GOAWAY。该 listener 不实现 `Upgrade: h2c`，也不终止 TLS。
//!
//! 本 crate 由 `nasa` 门面重导出；业务应用不应直接依赖实现 crate。

#![forbid(unsafe_code)]

mod application;
/// 两级缓存组件:配置驱动装配 L2 + 失效广播,托管 CacheRuntimeGuard 生命周期。
#[cfg(feature = "cache")]
mod cache;
mod capabilities;
mod component;
mod config;
#[cfg(feature = "db")]
mod db;
/// 稳定 gRPC listener：UserHook service registry、Ready 绑定、关键监督与有界排空。
#[cfg(feature = "grpc")]
mod grpc;
/// 保序分 lane 执行器组件：UserHook 计划、Prepare 发布、动态健康与有界停机。
#[cfg(feature = "partition")]
mod partition;
/// OpenTelemetry traces 组件:配置驱动的有界 span 导出管道 + 受管 drainer + 停机 flush。
#[cfg(feature = "telemetry")]
mod telemetry;
/// 业务构造迁移门禁所需的 `namigrate` 公共类型再导出;经 `nasa::application::*` 一并透出。
///
/// 业务仍需自身直依赖 `sqlx` 以调用 `sqlx::migrate!("./migrations")` 生成 [`Migrator`]
/// (嵌入式 migration 与 sqlx 天然耦合,这是唯一被门面放行的第三方类型穿透点);本组再导出让
/// 业务能命名门禁的配置/报告/错误类型,并在 `Application::configure_migrations` 登记后由 DB 组件
/// 在 initializer 之前的 Prepare 阶段按 `database.migrations.mode` 执行门禁。
#[cfg(feature = "db")]
pub use namigrate::{
    run_gate, MigrationError, MigrationMode, MigrationReport, MigrationSettings, Migrator,
};
#[cfg(feature = "telemetry")]
pub use telemetry::OtlpMetricsSnapshot;
#[cfg(feature = "nacos-discovery")]
mod discovery;
mod error;
mod future;
mod global;
mod initialization;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "log")]
mod log;
#[cfg(feature = "mapper-cache")]
mod mapper_cache;
#[cfg(feature = "web")]
mod mapping_handle;
#[cfg(any(feature = "kafka", feature = "web-security"))]
mod metrics;

/// 统一指标接入所需的 nametrics-core 公共类型再导出。
///
/// `nasa` 门面据此构造 nafana、对象存储等**领域兼容源**并经
/// [`Application::register_metrics_source`] 并入进程级 hub,无需业务或门面直接依赖
/// `nametrics-core`。该组类型是指标目录的公开合同，与具体领域无关，因此不随 kafka/web
/// 能力开关变化；否则不启用这两项的领域将无法为统一目录贡献指标。
pub use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricFamilySnapshot, MetricKind, MetricSample,
    MetricValue,
};

mod secret;

/// RFC 9457 `application/problem+json` 统一 Web 错误契约。
#[cfg(feature = "web")]
mod problem;
#[cfg(feature = "web")]
pub use problem::{ApiProblem, FieldViolation};

/// Web 生产治理中间件:request ID 等按 顺序装配的韧性层。
#[cfg(feature = "web")]
mod governance;
#[cfg(feature = "web")]
pub use governance::{ClientIp, RequestBudget, RequestId};

/// 跨副本分布式业务配额:`RateLimitProvider` 抽象 + nadis Redis 固定窗口后端。
#[cfg(feature = "rate-limit")]
mod ratelimit;
#[cfg(all(feature = "rate-limit", feature = "web"))]
pub use ratelimit::{distributed_rate_limit, DistributedRateLimit};
#[cfg(feature = "rate-limit")]
pub use ratelimit::{
    RateLimitOutcome, RateLimitProvider, RedisRateLimitProvider, SharedRateLimitProvider,
};

/// 输入校验提取器与统一错误:`ValidatedJson/Query/Path` + `ValidateRequest`。
#[cfg(feature = "web")]
mod validate;
#[cfg(feature = "web")]
pub use validate::{ValidateRequest, ValidatedJson, ValidatedPath, ValidatedQuery};

/// 幂等请求中间件:把 naidempotency 状态机接到 Web 请求路径。
#[cfg(feature = "web")]
mod idempotency;
#[cfg(feature = "web")]
pub use idempotency::{idempotency, IdempotencyLayerState, SharedIdempotencyStore};
/// 业务构造幂等 store 所需的 naidempotency 公共类型再导出(经门面即可用,无需直依赖)。
#[cfg(feature = "web")]
pub use naidempotency::{
    ExecutionLease, IdempotencyError, IdempotencyKey, IdempotencyOutcome, IdempotencyStore,
    InMemoryIdempotencyStore, RequestFingerprint, StoredHeader, StoredResponse,
};

/// route 级授权中间件:把 naauthz 策略决策接到 Web 请求路径。
#[cfg(feature = "web")]
mod authz;
#[cfg(feature = "web")]
pub use authz::{authorize, AuthorizationLayerState, SharedObjectAuthorizer, SharedPolicyRegistry};
/// 业务构造授权策略所需的 naauthz 公共类型再导出。
#[cfg(feature = "web")]
pub use naauthz::{
    AuthzDecision, DenyReason, ObjectAuthorizationError, ObjectAuthorizationRequest,
    ObjectAuthorizer, ObjectDecision, ObjectProviderError, PolicyError, PolicyRegistry, PolicySet,
    Principal, RequestSecurityContext, RequireMode, RoutePolicy,
};

/// OAuth Resource Server / JWKS 认证组件:配置驱动 warmup JWKS,Ready 发布 Authenticator。
#[cfg(feature = "web")]
mod auth;

/// authentication 中间件:校验 Bearer JWT → 写入已验证 Principal(供 authz 判定)。
#[cfg(feature = "web")]
mod authn;
#[cfg(feature = "web")]
pub use authn::{authenticate, Authenticator, SharedAuthenticator};
/// 业务构造认证器所需的 nauth-oauth 公共类型再导出(经门面即可用,无需直依赖)。
#[cfg(feature = "web")]
pub use nauth_oauth::{
    AccessTokenClaims, Jwk, JwkSet, JwksError, JwksRegistry, TokenError, TokenPolicy,
};

/// W3C Trace Context 传播中间件:把 natelemetry 链路上下文接到 Web 入口。
#[cfg(feature = "web")]
mod trace;
/// 业务/下游透传所需的 natelemetry 链路类型再导出。
#[cfg(feature = "web")]
pub use natelemetry::TraceContext;
#[cfg(feature = "web")]
pub use trace::trace_context;

#[cfg(feature = "nacos-config")]
mod nacos_config;
#[cfg(feature = "outbox")]
mod outbox;
mod panic_hook;
mod preflight;
mod process;
mod readiness;

/// 只读就绪快照类型(管理端读取):供业务经 [`Application::readiness_snapshot`] 读取各依赖的聚合
/// 状态。只暴露**读取**——注册/观测/封口等 owner 权限仍只在框架内部,不做成业务 API。
pub use readiness::{
    DependencySnapshot, DependencyState, ReadinessContributor, ReadinessPolicy, ReadinessSnapshot,
};

#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "redis-job")]
mod redis_job;
#[cfg(feature = "redis-job")]
pub use redis_job::{RedisJobDescriptor, COLLECTED_REDIS_JOBS};
#[cfg(any(feature = "log", feature = "nacos-config"))]
mod reload;
mod report;
mod resources;
mod runner;
#[cfg(feature = "saga")]
mod saga;
#[cfg(feature = "scheduling")]
mod scheduling;
mod sections;
mod shutdown;
mod signal;
mod spec;
mod state;
mod supervisor;
#[cfg(feature = "web")]
mod web;
#[cfg(feature = "web")]
mod web_handle;
#[cfg(feature = "ws")]
mod ws;

#[cfg(feature = "ws")]
pub use application::WsCustomization;
pub use application::{Application, ApplicationInfo, WeakApplication};
#[cfg(feature = "web")]
pub use application::{MappingTransform, RouterTransform};
pub use capabilities::ComponentLifecycleState;
#[cfg(feature = "log")]
pub use capabilities::LogHandle;
#[cfg(feature = "nacos-config")]
pub use capabilities::NacosConfigHandle;
#[cfg(feature = "nacos-discovery")]
pub use capabilities::NacosDiscoveryHandle;
#[cfg(feature = "scheduling")]
pub use capabilities::SchedulingHandle;
#[cfg(feature = "ws")]
pub use capabilities::WsHandle;
#[cfg(all(feature = "nacos-discovery", feature = "grpc"))]
pub use capabilities::{GrpcDiscoveredEndpoint, GrpcDiscoveredTlsMode};
#[cfg(feature = "kafka")]
pub use capabilities::{KafkaHandle, KafkaReadinessSnapshot};
pub use component::{
    ApplicationComponent, BootstrapContext, PrepareContext, ReadyContext, ShutdownAction,
    StartContext,
};
pub use config::{
    ConfigProvider, ConfigSnapshot, ConfigSource, ConfigStore, ConfigView, ReloadState,
    ReloadStatus, ReloadTarget,
};
pub use error::{ApplicationError, ApplicationPhase, ApplicationResult, ComponentId};
pub use future::ApplicationFuture;
pub use initialization::{
    Initialization, InitializationContext, InitializerDescriptor, InitializerFailure,
    InitializerFailureKind, InitializerKind, InitializerSpec, InitializerStage,
    COLLECTED_INITIALIZERS, DEFAULT_INITIALIZER_ORDER, MAX_INITIALIZERS, MAX_INITIALIZER_REQUIRES,
};
#[cfg(feature = "web")]
pub use mapping_handle::MappingHandle;
#[cfg(feature = "outbox")]
pub use outbox::{
    OutboxApplicationPlan, OutboxChannelPlan, OutboxHandle, OutboxPoisonPolicy,
    OutboxRetentionPlan, OutboxSnapshot,
};
#[cfg(feature = "partition")]
pub use partition::{PartitionApplicationHandle, PartitionApplicationPlan};
pub use process::run;
pub use resources::{ManagedResource, ResourcePhase, ResourceRef, ResourceRegistry};
pub use runner::{ApplicationExit, ApplicationExitReason, ApplicationRunner};
#[cfg(feature = "saga-redis-stream")]
pub use saga::SagaRedisTransportPlan;
#[cfg(feature = "saga")]
pub use saga::{SagaApplicationPlan, SagaHandle};
pub use shutdown::{ShutdownContext, ShutdownReason, ShutdownSignal};
pub use spec::ApplicationSpec;
#[cfg(feature = "web")]
pub use spec::{RouteMeta, WebBuildContext, WebRouteMetaFactory, WebRouterFactory};
pub use state::{ApplicationMode, ApplicationState};
pub use supervisor::{TaskId, SUPERVISOR_QUEUE_CAPACITY};
/// 受管任务接收的协作式取消令牌；业务服务应在排空入口等待它，而不是重复监听进程信号。
pub use tokio_util::sync::CancellationToken;
#[cfg(feature = "web")]
pub use web_handle::{RouteInfo, WebHandle, WebMetricsSnapshot, WebReadinessState, WebRouteOrigin};

/// 属性入口用于在业务 crate 编译期核对组件能力是否已经启用的探测点。
///
/// 每个子模块只在对应运行能力编入时存在。属性展开代码引用其中的常量，缺失能力会直接形成
/// 指向组件名的编译错误，不会把问题延后到进程启动阶段。
pub mod components {
    /// 日志组件的编译期能力探测点。
    #[cfg(feature = "log")]
    pub mod log {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 配置中心组件的编译期能力探测点。
    #[cfg(feature = "nacos-config")]
    pub mod nacos_config {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 数据源组件的编译期能力探测点。
    #[cfg(feature = "db")]
    pub mod db {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// Redis 组件的编译期能力探测点。
    #[cfg(feature = "redis")]
    pub mod redis {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// RedisJob 长生命周期组件的编译期能力探测点。
    #[cfg(feature = "redis-job")]
    pub mod redis_job {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// Web 组件的编译期能力探测点。
    #[cfg(feature = "web")]
    pub mod web {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 认证组件的编译期能力探测点(auth 能力随 Web 编入,声明 `auth` 需同时声明 `web`)。
    #[cfg(feature = "web")]
    pub mod auth {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 遥测组件的编译期能力探测点。
    #[cfg(feature = "telemetry")]
    pub mod telemetry {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 保序分 lane 执行器的编译期能力探测点。
    #[cfg(feature = "partition")]
    pub mod partition {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 受管 gRPC listener 的编译期能力探测点。
    #[cfg(feature = "grpc")]
    pub mod grpc {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 缓存组件的编译期能力探测点。
    #[cfg(feature = "cache")]
    pub mod cache {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// Kafka 组件的编译期能力探测点。
    #[cfg(feature = "kafka")]
    pub mod kafka {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// Outbox dispatcher 组件的编译期能力探测点。
    #[cfg(feature = "outbox")]
    pub mod outbox {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// Saga 生命周期组件的编译期能力探测点。
    #[cfg(feature = "saga")]
    pub mod saga {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 长连接组件的编译期能力探测点。
    #[cfg(feature = "ws")]
    pub mod ws {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 服务发现组件的编译期能力探测点。
    #[cfg(feature = "nacos-discovery")]
    pub mod nacos_discovery {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// 调度组件的编译期能力探测点。
    #[cfg(feature = "scheduling")]
    pub mod scheduling {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }
}

/// 属性入口展开代码使用的依赖桥；不属于业务稳定接口。
#[doc(hidden)]
pub mod __private {
    pub use anyhow;
    #[cfg(feature = "web")]
    pub use axum;
    pub use linkme;
    #[cfg(feature = "redis-job")]
    pub use nadis;
    #[cfg(feature = "web")]
    pub use naweb;
    #[cfg(feature = "redis-job")]
    pub use prost;
    #[cfg(feature = "redis-job")]
    pub use serde_json;
}
