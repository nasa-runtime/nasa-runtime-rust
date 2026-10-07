//! NASA 应用生命周期运行时核心。
//!
//! 严格配置可由 `ApplicationSpec::with_config_loader` 在 preflight 前显式选择，固定环境和来源权限。
//! naml 负责嵌套默认值、有序文件模式和字段解释；宿主负责材料、观察集、配置视图及组件应用状态。
//! 同值来源变化可推进 `Application::config_observation`，不增加业务配置版本，也不表示组件重试成功。
//!
//! Redis Stream、Proxy、AutoPipeline、出站 TCP 帧客户端和 hystrix 可由命名配置接入，
//! 框架负责准备、业务激活、健康观测以及关闭后的真实退出等待。
//!
//! 命名 REST、幂等 store、事务审计、对象存储、Schema Registry、TLS HTTP、缓存与 Redis 派生任务
//! 通过 Application 的显式配置和启动期计划装配。宿主持有准入、健康和关闭 owner；默认 feature
//! 为空，未选择的能力不建连。可用入口随对应 feature 导出，持久提交与租约仍由各自后端裁决。
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
//! # 消费、出站与隔离命令
//!
//! `redis` 提供命名 Stream、Proxy 与 AutoPipeline；`ws-client` 提供原生 TCP 帧 Client，
//! 不需要入站 listener。Service 的消费、发送与回调和宿主终端共用启动许可；initializer 可以
//! 取得句柄，但发送入口在放行前拒绝调用。Batch 在工作负载前开放 Pipeline 与发送 Client，
//! 不接受长期消费或回调计划。TCP Client 不提供 ws/wss、TLS、消息重放或远端送达保证。
//!
//! `hystrix` 的显式受管配置在 Prepare 安装本代目录、固定规则与一个周期观测任务，供后续初始化、
//! 业务调用和收尾使用。属性宏只缓存带代次的弱引用；关闭先拒绝新调用，等待在途责任后撤销全局
//! 入口，旧 Command 永久返回 503。上述命名配置与认证材料变化报告 `RestartRequired`。
//!
//! # SQL 观测与统一放行
//!
//! `mapper-observability` 在 DB 建连前冻结目录与 YAML 策略，自动登记方法、连接、Pool 和通知源。
//! 业务主动安装进程 `Notify`，缺失则忽略；慢 SQL 原始耗时达到阈值且告警开启时尝试入队，逐条通知
//! 需关闭默认冷却。worker 调用业务通知微服务适配器，不在 SQL 路径执行用户代码或网络请求。
//! `observability` 从同一 MetricHub 导出，失败不改变 SQL、事务或数据库 readiness。
//!
//! Service 完成全部 Ready 装配与 initializer 任务工厂后，执行只读静态检查。在关键领域的本地
//! 状态保护内复验任务责任、认证连接、健康证据与共享启动期限，再提交 Ready 与统一启动许可。
//! 公开 Ready 时受管终端和领域入口已获许可；保护不覆盖尚未被观察到的远端故障。Batch 在工作
//! 负载前开放所选出站资源与观测任务，不发布 Service Ready。通知队列非持久、容量有限，
//! 不提供绝对送达或跨副本去重。
//! UserHook 中普通 `spawn_background` / `spawn_critical` 不隐式等待 Ready；自管 listener 应使用
//! `serve_when_ready`。initializer 任务工厂只构造 future，不自行开放入口或派生脱离屏障的任务。
//!
//! # 业务优雅停机
//!
//! Service 与 Batch 的 UserHook 可通过 [`Application::register_graceful_shutdown`] 移交一次性收尾
//! future。运行时在受监督任务收口之后、UserHook 业务资源释放之前，按 priority 升序和同级
//! 登记顺序执行。Service 在任务前清理 initializer；Batch 在业务任务和业务资源之后清理静态
//! initializer。任务组公平分配剩余预算并预留资源清理时间，单项错误不覆盖首次终止原因。
//! 尚未清理的资源可在任务执行时查找；局部 owner 清理不会提前关闭其它资源的查找。
//!
//! 已接管任务从登记到执行持续隔离单次析构展开。直接取消 Runner 时同步关闭登记，将 Starting/Ready
//! 置为 Stopping，沿实际激活栈逆序释放剩余 action、停机任务和所属资源，保持上述 Service/Batch 顺序，
//! 若经过任务门时仍有受监督 future 存活，立即撤销新借用与全局入口，将剩余清理交给任务存活守卫，
//! 最后一个 future 析构后才释放尾部；否则在栈清理后撤销入口。已有 Stopped/Failed 不变。
//! 保留句柄不能再借用资源，已借出的资源随借用归还释放；此前复制的外部客户端句柄不在撤销范围内。
//! Stopping 不表示异步收尾完成；直接取消不保证执行异步收尾或生成退出报告。
//! 同步阻塞、非协作 poll、`panic=abort` 和析构自身展开期间
//! 的未隔离再次 panic 不在保护范围内；一个对象只能有一个最终关闭 owner。
//!
//! # Saga 角色与同源投递
//!
//! 启用 Saga 组件时，Application 按角色构造运行能力；数据角色在 Ready 前校验 definition、
//! descriptor、历史非终态实例和参与方信任投影，再监督 durable timer 与所选受管消费循环。
//! direct client 不创建本地数据库或 Outbox。可靠 client 在 `saga.client.datasource_ref` 的业务事务内
//! 追加 start-intent，并把 dispatcher 固定绑定同一数据源；显式 `outbox.datasource_ref` 不一致时
//! 在 Ready 前拒绝，省略该字段或只配置轮询预算不改变绑定。`enqueue_start` 返回事件身份仅表示
//! 事务内追加成功，外层事务提交后才可表示本地已受理；远端收据不明时保留原事件，Ready 不代表流程完成。
//! Application 只负责资源所有权和启停顺序；Saga 的 CAS、Inbox/Outbox 与补偿正确性仍由
//! 当前数据库对应的 `nasaga-runtime` 或 `nasaga-runtime-pgsql` 持久合同承担。
//! 已提交 result 使用独立 Catalog 资格：command route 缺席不会单独阻断原事件，也不会开放新
//! Start、timer claim 或 Ready。请求冻结期限、撤销身份、安全发布代际与合同摘要；受管
//! HTTP、gRPC、Kafka 和 Redis Streams 在状态事务中持续复验同一次资格，失权完整回滚并保留重投。
//!
//! 数据库 YAML 中的 migration 段只定义执行策略。Service UserHook 通过
//! `Application::configure_migrations` 登记业务嵌入的 migrator，DB Prepare 在 initializer 和入站
//! listener 之前执行门禁；Batch 通过 `MIGRATION_PLANS` 静态工厂登记，工作负载前执行。PostgreSQL 独立 session endpoint 还会在 advisory lock 前复验目标身份。
//!
//! `rate-limit` 提供共享 Redis 原子计数的跨副本业务配额。业务显式从受管 Redis source 构造 provider；
//! 本能力不增加组件字符串或配置根，默认后端错误采用 fail-open。与 `web` 组合时可安装 IP 中间件，
//! tenant、subject 或 API key 配额则直接使用 provider 合同。
//!
//! Web 授权边界在认证后冻结 Principal、route 策略、未命中缺省与 generation。显式策略始终优先，
//! 公开 route 或健康探针豁免只能作用于没有显式策略的 route；registry 入口和 handler 请求上下文
//! 使用同一完整快照。对象授权缺失、拒绝、错误或超时均 fail-closed。
//!
//! 合法上游 Trace Context 严格继承 sampled 位；没有上游时由受管 telemetry exporter 的 sampler
//! 唯一裁决新根。未启用 telemetry 的 Web 仍传播未采样上下文，不会替下游声明已采样。调度 span
//! 只在 leader 与 claim 门禁通过后建立。
//!
//! 启用 gRPC 组件时，UserHook 只登记统一 codegen 生成的业务 service，Prepare 永久封口 registry，
//! 全部 initializer 成功后的 Ready 才自动装配 Router、health、可选 reflection 并绑定 listener。
//! 绑定只发布 Bound；全部任务工厂与最终检查通过、Application Ready 发布后统一放行接流，
//! 随后才允许服务发现注册。注册确认前动态 readiness 保持不可用。
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

mod diagnostics;
mod managed_adapters;
#[cfg(feature = "kafka-schema-registry")]
mod schema_registry;
pub use diagnostics::{ConfigStatusSummary, DiagnosticSnapshot, Sampled};
pub use nabudget::{BudgetError, RequestBudget};
#[cfg(any(feature = "db", feature = "db-pgsql"))]
mod migrations;
#[cfg(any(feature = "db", feature = "db-pgsql"))]
pub use migrations::{MigrationPlanFactory, MIGRATION_PLANS};
mod grouped_cache;
mod object_store;
#[cfg(feature = "rest")]
mod rest;
#[cfg(all(feature = "cache", feature = "redis"))]
pub use grouped_cache::ManagedGroupedCache;
#[cfg(feature = "secret-http")]
mod tls_http;
#[cfg(feature = "secret-http")]
pub use tls_http::{ManagedHttpClient, ManagedHttpResponse};
#[cfg(feature = "redis")]
mod redis_partition;
#[cfg(feature = "redis")]
pub use redis_partition::{RedisPartitionObservation, RedisPartitionStopResult};

mod application;
/// 两级缓存组件:配置驱动装配 L2 + 失效广播,托管 CacheRuntimeGuard 生命周期。
#[cfg(feature = "cache")]
mod cache;
mod capabilities;
mod component;
mod config;
#[cfg(any(feature = "nacos-config", feature = "config-watch"))]
mod config_reload;
#[cfg(feature = "config-watch")]
mod config_watch;
#[cfg(all(feature = "db", not(feature = "db-pgsql")))]
mod db;
#[cfg(feature = "db-pgsql")]
#[path = "db_multi.rs"]
mod db;
/// 稳定 gRPC listener：UserHook service registry、Ready 绑定、关键监督与有界排空。
#[cfg(feature = "grpc")]
mod grpc;
/// 命名分区 Runner 组件：严格 FIFO 的保序任务窃取、独立容量、Prepare 原子发布、逐域健康与有界停机。
#[cfg(feature = "partition")]
mod partition;
/// OpenTelemetry traces 组件:配置驱动的有界 span 导出管道 + 受管 drainer + 停机 flush。
#[cfg(feature = "telemetry")]
mod telemetry;
/// 业务构造迁移门禁所需的 `namigrate` 公共类型再导出;经 `nasa::application::*` 一并透出。
///
/// 业务仍需自身直依赖 `sqlx` 并启用 `macros,migrate`，以调用
/// `sqlx::migrate!("./migrations")` 生成 [`Migrator`]
/// (嵌入式 migration 与 sqlx 天然耦合,这是唯一被门面放行的第三方类型穿透点);本组再导出让
/// 业务能命名门禁的配置/报告/错误类型,并在 `Application::configure_migrations` 登记后由 DB 组件
/// 在 initializer 之前的 Prepare 阶段按 `database.migrations.mode` 执行门禁。
#[cfg(feature = "db")]
pub use namigrate::{
    run_gate, MigrationError, MigrationMode, MigrationReport, MigrationSettings, Migrator,
};
#[cfg(all(not(feature = "db"), feature = "db-pgsql"))]
pub use namigrate_pgsql::{
    MigrationError, MigrationMode, MigrationReport, MigrationSettings, Migrator,
};
#[cfg(feature = "telemetry")]
pub use telemetry::OtlpMetricsSnapshot;
#[cfg(feature = "nacos-discovery")]
mod discovery;
mod error;
mod future;
mod global;
/// Inbox 去重标记的显式保留计划、受管串行清理与统一观测。
#[cfg(any(feature = "inbox", feature = "inbox-pgsql"))]
mod inbox;
mod initialization;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(any(feature = "inbox", feature = "inbox-pgsql"))]
pub use inbox::{InboxRetentionPlan, InboxRetentionSnapshot};
#[cfg(feature = "log")]
mod log;
#[cfg(any(feature = "mapper-cache", feature = "mapper-cache-pgsql"))]
mod mapper_cache;
#[cfg(any(
    feature = "mapper-cache",
    feature = "mapper-cache-pgsql",
    feature = "mapper-observability"
))]
mod mapper_defaults;
#[cfg(feature = "web")]
mod mapping_handle;
#[cfg(any(feature = "kafka", feature = "web-auth", feature = "web-crypto"))]
mod metrics;
#[cfg(feature = "observability")]
mod observability;
#[cfg(feature = "mapper-observability")]
mod sql_notifications;
#[cfg(feature = "mapper-observability")]
mod sql_observability;

/// 与 SQL 执行结果隔离的通知 provider 合同。
/// 业务通过 init 安装进程默认实现，未安装时忽略；默认不要求命名 provider 配置。
#[cfg(feature = "mapper-observability")]
pub mod notifications {
    pub use nanotify_core::*;
}

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
pub use governance::{ClientIp, RequestId};

/// 跨副本分布式业务配额:`RateLimitProvider` 抽象 + nadis Redis 固定窗口后端。
#[cfg(feature = "rate-limit")]
mod ratelimit;
#[cfg(all(feature = "rate-limit", feature = "web"))]
pub use ratelimit::{
    distributed_rate_limit, DistributedRateLimit, MissingSubjectPolicy, QuotaSubject,
};
#[cfg(feature = "rate-limit")]
pub use ratelimit::{
    rate_limit_counters, RateLimitFailurePolicy, RateLimitOutcome, RateLimitProvider,
    RedisRateLimitProvider, SharedRateLimitProvider,
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
pub use authz::{
    authorize, unmatched_denied_total, unmatched_observed_total, AuthorizationLayerState,
    SharedObjectAuthorizer, SharedPolicyRegistry,
};
/// 业务构造授权策略所需的 naauthz 公共类型再导出。
#[cfg(feature = "web")]
pub use naauthz::{
    AuthzDecision, DenyReason, ObjectAuthorizationError, ObjectAuthorizationRequest,
    ObjectAuthorizer, ObjectDecision, ObjectProviderError, PolicyCoverageAudit,
    PolicyDecisionSnapshot, PolicyError, PolicyRegistry, PolicySet, Principal,
    RequestSecurityContext, RequireMode, RoutePolicy, UnmatchedRoutePolicy,
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

mod config_source;
pub use config_source::ConfigObservation;
#[cfg(feature = "nacos-config")]
mod nacos_config;
#[cfg(any(feature = "outbox", feature = "outbox-pgsql"))]
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
#[cfg(feature = "redis")]
mod redis_derived;
#[cfg(feature = "redis")]
mod redis_tasks;
#[cfg(feature = "redis")]
pub use redis_derived::{ManagedRedisPipeline, ManagedRedisProxy, RedisDerivedObservation};
#[cfg(any(feature = "log", feature = "nacos-config", feature = "config-watch"))]
mod reload;
#[cfg(feature = "redis")]
pub use redis_tasks::ManagedRedisLeader;
mod report;
mod resources;
mod runner;
#[cfg(any(feature = "saga", feature = "saga-pgsql"))]
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
#[cfg(any(feature = "ws-redis", feature = "ws-kafka"))]
mod ws_cluster;

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
#[cfg(any(feature = "outbox", feature = "outbox-pgsql"))]
pub use outbox::{
    OutboxApplicationPlan, OutboxChannelPlan, OutboxHandle, OutboxPoisonPolicy,
    OutboxRetentionPlan, OutboxSnapshot,
};
#[cfg(feature = "partition")]
pub use partition::{PartitionApplicationHandle, PartitionApplicationPlan};
pub use process::run;
pub use resources::{ManagedResource, ResourcePhase, ResourceRef, ResourceRegistry};
pub use runner::{ApplicationExit, ApplicationExitReason, ApplicationRunner};
#[cfg(any(feature = "saga-redis-stream", feature = "saga-redis-stream-pgsql"))]
pub use saga::SagaRedisTransportPlan;
#[cfg(any(feature = "saga", feature = "saga-pgsql"))]
pub use saga::{
    SagaApplicationPlan, SagaHandle, SagaOrchestratorHandle, SagaPlanMode, SagaRemoteClient,
    SagaRemoteSnapshot, SagaRemoteStartDisposition, SagaRemoteStartReceipt, SagaRemoteStartRequest,
    SagaRole,
};
pub use shutdown::{ShutdownContext, ShutdownReason, ShutdownSignal, ShutdownTaskOutput};
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
    #[cfg(any(feature = "db", feature = "db-pgsql"))]
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

    /// 命名分区 Runner 的编译期能力探测点。
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
    #[cfg(any(feature = "outbox", feature = "outbox-pgsql"))]
    pub mod outbox {
        /// 组件能力已编入时可被属性展开代码引用的零大小标记。
        pub const FEATURE_CHECK: () = ();
    }

    /// Saga 生命周期组件的编译期能力探测点。
    #[cfg(any(feature = "saga", feature = "saga-pgsql"))]
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

#[cfg(feature = "ws-client")]
mod ws_client;
#[cfg(feature = "ws-client")]
pub use ws_client::ManagedWsClient;

#[cfg(feature = "hystrix")]
mod hystrix_managed;
