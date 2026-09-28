//! 面向 Rust 服务端应用的受管生命周期与可靠业务执行门面。
//!
//! `nasa-runtime-rust` 将配置校验、资源装配、业务初始化、接流许可和有序停机纳入同一生命周期，
//! 并通过事务、Inbox/Outbox、Saga 与有序消息处理支持可恢复业务。应用按 feature 选择能力；
//! 持久事实、事务提交和租约权威仍由所选数据库与消息系统承载。
//! [中文指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/README.md) 与
//! [English guide](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/README.en.md)
//! 提供快速开始、架构和接入说明。
//!
//! 命名 REST、幂等 store、事务审计、对象存储、Schema Registry、TLS HTTP、缓存与 Redis 派生任务
//! 通过 Application 的显式配置和启动期计划装配。宿主持有准入、健康和关闭 owner；默认 feature
//! 为空，未选择的能力不建连。可用入口随对应 feature 导出，持久提交与租约仍由各自后端裁决。
//!
//! 业务项目优先依赖本 crate，并通过 feature 选择需要的应用生命周期、MySQL/PostgreSQL 事务、
//! Inbox、Outbox、Saga、消息传输、缓存、跨副本业务配额、路由、调度、配置、发现和工具模块。
//!
//! # 消费、出站与隔离命令的受管入口
//!
//! `application,redis` 配合 Redis 组件提供命名 Stream、Proxy 与 AutoPipeline；
//! `application,ws-client` 提供无需入站 listener 的原生 TCP Client。Service 的消费、发送与回调
//! 和宿主终端共用启动许可，关键任务、认证连接与健康证据在本地状态保护内复验，公开 Ready 时
//! 入口已获许可。此边界不保证远端送达或未来调用成功。Batch 支持 Pipeline 和 Client 发送，
//! 不接受长期消费或回调计划；Client 不支持 ws/wss、TLS 或透明重发。
//!
//! `application,hystrix` 的显式配置安装本代命令目录、固定规则与集中周期观测，属性宏使用代次
//! 感知的弱引用缓存。业务收尾后先关闭命令准入，再等待在途调用并撤销目录；旧 Command 返回 503。
//! 这些资源的名称、容量与认证材料冻结到启动，变化报告 `RestartRequired`。
//!
//! # Redis 分区消费
//!
//! `redis` 提供 `PreparedPartition`、`PartitionRecord` 和 `RunningPartition`。Redis 租约与 PEL
//! 负责持久接管，实例私有的 napart Runner 集合负责本地执行。不同 Redis 源始终独立，同源通过
//! `partition.executor.scope` 选择 source、group 或 stream；域内容量来自源级总额的固定份额。
//! 同计划同业务键的顺序跨域覆盖 handler、ACK 和精确重试；ACK 不确定保留提交责任，不重跑成功
//! handler。交付仍为至少一次，不提供跨进程业务键锁或共享 Redis 后端隔离。
//! 消费器不复用 Application 的命名 Runner，由创建它的业务在 Redis 客户端关闭前等待排干报告；
//! 普通等待超时可继续等待同一操作，显式强停才请求有损中止。
//!
//! # SQL 观测与业务通知
//!
//! `application` 与 `mapper`/`mapper-pgsql` 自动装配 SQL、连接与 Pool 指标；YAML 控制开发日志、
//! 阈值通知与统一出口。原始执行耗时达到或超过阈值即命中，不计连接等待和 Stream 消费者处理。
//! 业务通过 `nasa::application::notifications::init` 安装 `Notify`，未安装则忽略；逐条通知需启用
//! 慢告警并设 `cooldown_ms=0`。SQL 路径只尝试入队，worker 调用业务适配器，框架不指定通知微服务
//! 协议或连接机器人。通知拥塞、超时与失败不改变 SQL 或事务结果，也不构成持久送达保证。
//! 指标出口由 `grafana.observability` 显式开启，平台 controller 独立拥有外部资源；期望实例指标由
//! 外部平台持续提供，框架只引用，不维护静态实例清单。
//!
//! # 持久化 Saga
//!
//! `saga-runtime` / `saga-runtime-pgsql` 将所选数据库的本地 ACID、Outbox 至少一次、Inbox 幂等、
//! 持久化状态机与显式补偿组合为
//! 最终一致性流程。稳定 `effect_id`、定义摘要、取消/裁决屏障、冻结补偿计划与 timer fencing
//! 让重复投递、Unknown 结果、进程崩溃和多副本竞争从已提交事实收敛。Kafka 和 Redis Streams
//! 提供受管 connector；HTTP 使用显式认证构件；gRPC 提供框架 generated command/result service、
//! mTLS principal 绑定和封闭收据。Saga 不提供跨服务 ACID、物理 exactly-once 或并发隔离。
//! 受管可靠 client 在 `saga.client.datasource_ref` 对应事务内原子追加业务事实与 start-intent，
//! dispatcher 固定扫描同一数据源。显式 `outbox.datasource_ref` 与之冲突时在 Ready 前拒绝；
//! 省略该字段或只配置轮询预算不改变绑定。`enqueue_start` 返回事件身份不等于外层事务已提交，
//! 本地已受理也不等于远端流程完成；收据不明时保留原事件继续投递。
//!
//! 启用 `application` 后，`#[nasa::initializer]` 与 `Application::register_initializer` 提供统一的
//! Ready 前业务初始化屏障。Runner 在 migration 和出站依赖准备完成后执行三轮全局屏障，全部成功
//! 才开放监听、消费与服务发现；依赖边优先于 `order`，失败会阻止 Ready 并进入逆序清理。
//!
//! # 业务优雅停机
//!
//! `application` feature 同时提供 `Application::register_graceful_shutdown`。Service 与 Batch
//! 在 UserHook 移交一次性收尾 future；受监督任务先收口，再按 priority 和同级登记顺序执行收尾，
//! 随后释放 UserHook 业务资源。Service 的 initializer 在业务任务之前清理，Batch 的静态
//! initializer 在业务资源之后清理。所有步骤共享绝对期限，单项失败不覆盖首次终止原因。
//! 已接管 future 的释放具有一次性析构隔离；直接取消 Runner 同步撤销本实例的全局入口和新资源借用，
//! 将 Starting/Ready 置为 Stopping，沿实际激活栈逆序释放 action、停机任务与所属资源，保持上述
//! Service/Batch 顺序；已有 Stopped/Failed 不变。已借出的资源随借用归还释放，
//! 任务门上尚未析构的受监督 future 会保留后续清理所有权，最后一个 future 释放后才归还依赖，
//! 不等待该过程，也不保留全局入口或新资源借用权限。
//! 此前复制的外部客户端句柄不在撤销范围内；不保证执行异步收尾或产生退出报告。
//! 此能力不替代受管组件的关闭 owner，也不提供跨进程崩溃的持久执行保证。
//!
//! # 受管基础设施
//!
//! `kafka-schema-registry` 提供有界 Schema Registry client 与批准 ID 门禁；`object-store` 提供有界
//! 单对象合同、SigV4 adapter 与内容完整性复核；`grpc` 提供统一 codegen、service registry、
//! TLS/mTLS、HTTP/2 资源门禁和有预算排空；`web` 与 `application` 同时启用后，
//! `#[nasa::application("web")]` 托管唯一明文 listener。这四项能力都进入 `full`，业务只通过本门面
//! 使用公开合同。
//!
//! `rate-limit` 通过 `nasa::application` 提供后端中立配额合同和共享 Redis 原子计数实现；业务必须
//! 显式声明 Redis 组件并决定主体身份或路由中间件。默认后端失败时 fail-open；它不替代 Web 的
//! 单实例令牌桶，也不自动改变路由。
//!
//! # 请求安全与链路传播
//!
//! `application,web` 组合通过 `nasa::application` 公开授权类型，以及同代 route 策略、未命中缺省
//! 与 generation 的完整裁决快照；Web、registry 与
//! handler 请求上下文使用同一语义，显式策略不会被公开 route 豁免绕过。对象授权 provider 不能
//! 明确放行时 fail-closed；身份验签仍由 `oauth` 或业务认证层负责。
//!
//! `telemetry` 严格继承合法上游的 sampled 位，只有 exporter sampler 能裁决无上游的新根；纯传播
//! 入口保持未采样。`scheduling` 在 leader 与 claim 权威都取得后才创建执行 span，拒绝拍次只记录
//! Skipped。门面不提供跨服务采样协调、物理 exactly-once 调度或业务对象归属推断。
//!
//! 默认只接受 HTTP/1；最终 YAML 设置 `server.http2.enabled=true` 即可在同一端口接受 h2c prior
//! knowledge，高级 transport 字段可省略。该入口不终止 TLS，也不实现 `Upgrade: h2c`。
// ============================================================================
// nasa —— nasa-runtime-rust 唯一对外门面。
//
// 只负责【模块组织 + 重导出】,不放业务状态、全局单例或转发逻辑。
// 命名原则:根模块只表达业务能力;不在根平铺 Server/Command/init 等符号
// (各模块的同名符号会撞车);不提供全量 prelude。
//
//   use nasa::hystrix::{hystrix, Command};
//   use nasa::cache::{cached, CacheLayer};
//   use nasa::tx::{self, transactional};
//   use nasa::scheduling::{Async, EnableScheduling, scheduled};
//   use nasa::web::{mvc_router, get_mapping, post_mapping, put_mapping, delete_mapping, patch_mapping};
//   use nasa::ws::{Endpoint, Server};   use nasa::ws::proto::{Message, Mode};
//
// 过程宏经 macro-support 自动发现本 crate(含 Cargo 重命名),
// 完整属性路径 #[nasa::hystrix::hystrix] / #[nasa::web::get_mapping("/x")] 同样可用。
// ============================================================================
#![forbid(unsafe_code)]

/// 应用运行时：Ready 前业务初始化、有序业务停机任务、配置快照、类型资源容器和受管任务。
///
/// `#[nasa::application("saga")]` 会隐式纳入 DB 与 Outbox；独立
/// `#[nasa::application("outbox")]` 会隐式纳入 DB。Inbox 是事务内原语，不声明为生命周期组件；
/// Kafka、Redis Streams、HTTP 或 gRPC 消息传输由业务按实际实现显式选择。
///
/// `#[nasa::initializer]` 静态项与 Service 启动 Hook 动态登记项合并后，在组件 `Prepare` 与
/// `Seal` 之间执行全部 `before -> initialize -> after`。全部成功前不发布 Ready；外部已提交事实
/// 不会被本地逆序清理撤销，业务实现必须使用事务或稳定幂等键保证可安全重跑。
///
/// # 单源与多源 YAML
///
/// 基础设施 source 由最终 YAML 创建，不在 `main` 中手工建池或建连。MySQL/PostgreSQL 单源使用
/// `database`，多源与混配使用 `datasources.<name>`；Redis 单源使用扁平 `redis`，多源使用
/// `redis.properties.<qualifier>`；Kafka 单 client 使用 `kafka`，多 client 使用
/// `kafkas.<client>`。同一种资源的单源根与多源根互斥，任一实例失败都会阻止完整命名表进入 Ready。
///
/// ```yaml
/// datasources:
///   default:
///     driver: mysql
///     url: ${APP_PRIMARY_DB_URL}
///   reporting:
///     driver: postgresql
///     url: ${APP_REPORTING_POSTGRES_URL}
/// outbox:
///   datasource_ref: reporting
/// saga:
///   role: orchestrator
///   plan_mode: custom
///   database_bootstrap: application
///   datasource_ref: reporting
///
/// redis:
///   properties:
///     primary:
///       url: ${APP_PRIMARY_REDIS_URL}
///       namespace: orders
///       profile: RustV2
///     sessions:
///       url: ${APP_SESSION_REDIS_URL}
///       namespace: sessions
///       profile: RustV2
///
/// kafkas:
///   default:
///     bootstrap_servers: ${APP_PRIMARY_KAFKA_BOOTSTRAP_SERVERS}
///   audit:
///     bootstrap_servers: ${APP_AUDIT_KAFKA_BOOTSTRAP_SERVERS}
/// ```
///
/// 单源数据库固定为 `default`；MySQL 使用 `Application::datasource`，PostgreSQL 使用
/// `Application::pg_datasource`，driver 错配不会回退。单源 Redis 的持久身份为 `primary`，并提供 `default` 查询别名；
/// 单 client Kafka 省略 `client_name` 时默认为 `default`。业务通过 `default_datasource`/
/// `datasource(name)`、`default_pg_datasource`/`pg_datasource(name)`、`default_redis`/`redis(name)`、
/// `default_kafka`/`kafka(name)` 取得受管句柄。
/// Outbox 用 `outbox.datasource_ref`；managed Saga 用角色作用域内的 `datasource_ref`，custom Saga
/// 使用顶层 `saga.datasource_ref`。Cache、缓存失效广播和 Scheduling 用 `redis_ref` 选择命名源；
/// 引用不存在时在建连前拒绝，不会猜测唯一实例或回退默认源。
/// `partition` 的 `partitions`、`queue_capacity` 与 `max_partitions` 字段描述默认 Runner；
/// `partition.runners.<name>` 可以声明多个相互隔离的分区 Runner；每个字段都可省略并逐项使用
/// 有界默认值，`default_runner` 决定 `app.partition()` 的业务投影。代码侧可用
/// `configure_partition_runner` 在 Service UserHook 登记 YAML 未占用的启动期名称，同名不能由两个
/// 入口重复声明；计划在 Hook 返回后的 Prepare 才启动，Running 阶段不追加受管 Runner。运行期间按
/// 业务参数创建执行域时，直接使用 `nasa::partition::PartitionRunnerRegistry` 并显式管理停机。
///
/// # Migration 门禁
///
/// YAML 的 `migrations` 只定义模式、锁等待和 PostgreSQL session topology。Service 可以在 UserHook 用
/// `Application::configure_migrations` 为每个 datasource 登记业务通过 `sqlx::migrate!` 嵌入的
/// `Migrator`；门禁在 initializer 与 listener 前执行。门禁模式不是 `disabled` 时，事务级 PostgreSQL 代理必须提供独立
/// `migrations.session_url`，并在 advisory lock 前与业务池复验 database/schema 身份。
/// Service 与 Batch 都可通过 `application::MIGRATION_PLANS` 静态工厂登记迁移。
/// Batch 在 initializer 与工作负载之前执行迁移，不接受工作负载 Hook 的动态登记。
///
/// # 跨副本业务配额
///
/// `rate-limit` feature 提供 `RateLimitProvider`、`RedisRateLimitProvider` 和可选 Web IP 中间件。
/// 该能力没有组件字符串或独立 YAML；业务从受管 Redis source 构造 provider，并决定 tenant、subject、
/// API key 或客户端 IP 等主体身份。Redis provider 后端失败时 fail-open，需要 fail-closed 时应注入自定义
/// provider。
#[cfg(feature = "application")]
pub mod application {
    pub use application_impl::*;
    pub use application_macro::{application, initializer};
}

#[cfg(feature = "application")]
pub use application_impl::{Application, ShutdownTaskOutput};
#[cfg(feature = "redis-job")]
pub use application_macro::redis_job;
#[cfg(feature = "application")]
pub use application_macro::{application, initializer};

/// 路由级隔离、Dashboard 监控、`#[hystrix]` 与 `#[global_fallback]` 终态降级。
#[cfg(feature = "hystrix")]
pub mod hystrix {
    pub use hystrix_impl::*;
    pub use hystrix_macro::{global_fallback, hystrix};
}

/// 接口级隔离监控：`#[grafana]`、`#[global_fallback]`、Prometheus `/metrics` 与 Grafana 面板。
#[cfg(feature = "grafana")]
pub mod grafana {
    pub use grafana_impl::*;
    pub use grafana_macro::{global_fallback, grafana};

    /// 接口指标源由 nafana 提供；受管 Application 自动登记，独立宿主可以显式复用。
    pub use grafana_impl::metrics_source;
}

/// 两级缓存(L1 moka + L2 Redis 三防)+ `#[cached]` / `#[cache_invalidate]`。
#[cfg(feature = "cache")]
pub mod cache {
    pub use cache_impl::*;
    pub use nacache_macro::{cache_invalidate, cached};

    // 提升最常用类型,避免业务写 nasa::cache::cache::CacheLayer。
    // `CacheBackend`/`ClusterConnectionBackend`:L2 后端窄接口,让 `CacheLayer` 与具体 Redis 连接
    // 类型解耦,编排层可传入复用受管 Redis 的 adapter。
    pub use cache_impl::cache::{
        field, CacheBackend, CacheLayer, ClusterConnectionBackend, GroupedCache, SEP,
    };
}

/// ambient 事务上下文与按数据库后端分区的声明式事务入口。
#[cfg(any(feature = "tx", feature = "tx-pgsql"))]
pub mod tx {
    #[cfg(feature = "tx")]
    pub use natx_macro::transactional;
    #[cfg(feature = "tx")]
    pub use tx_impl::*;

    /// PostgreSQL ambient transaction、typed 连接与专用属性宏入口。
    #[cfg(feature = "tx-pgsql")]
    pub mod pgsql {
        pub use natx_macro::transactional_pgsql as transactional;
        pub use tx_pgsql_impl::*;
    }
}

/// 数据库迁移门禁；PostgreSQL 入口显式接收与业务连接一致的 schema，并在事务池拓扑下使用独立 session 连接。
#[cfg(feature = "tx-pgsql")]
pub mod migration {
    /// PostgreSQL advisory lock、checksum 与 validate/apply 门禁。
    pub mod pgsql {
        pub use migration_pgsql_impl::*;
    }
}

/// 消息 Inbox：按数据库后端与业务副作用共享同一 ambient transaction。
#[cfg(any(feature = "inbox", feature = "inbox-pgsql"))]
pub mod inbox {
    pub use inbox_core_impl::*;
    #[cfg(feature = "inbox")]
    pub use inbox_mysql_impl::{InboxProcess, InboxStoreError, InboxTransactionError, MySqlInbox};

    /// PostgreSQL Inbox 与后端中立消费合同。
    #[cfg(feature = "inbox-pgsql")]
    pub mod pgsql {
        pub use inbox_pgsql_impl::*;
    }
}

/// 事务型 Outbox：事件、顺序投递合同与按后端分区的持久实现。
#[cfg(any(feature = "outbox", feature = "outbox-pgsql"))]
pub mod outbox {
    pub use outbox_core_impl::{
        dispatch_in_order, DispatchReport, InMemoryOutbox, OutboxEvent, OutboxPublishError,
        OutboxPublisher, OutboxWriter,
    };
    #[cfg(feature = "outbox")]
    pub use outbox_mysql_impl::{MySqlOutbox, OutboxStoreError};

    /// PostgreSQL 事务写侧、fencing dispatcher 与 retention 实现。
    #[cfg(feature = "outbox-pgsql")]
    pub mod pgsql {
        pub use outbox_pgsql_impl::*;
    }
}

/// 业务幂等状态机及按需启用的持久化 store。
#[cfg(feature = "idempotency")]
pub mod idempotency {
    pub use idempotency_impl::{
        ExecutionLease, IdempotencyError, IdempotencyKey, IdempotencyOutcome, IdempotencyStore,
        InMemoryIdempotencyStore, RequestFingerprint, StoredHeader, StoredResponse,
    };
    #[cfg(feature = "idempotency-mysql")]
    pub use idempotency_mysql_impl::MySqlIdempotencyStore;
    /// PostgreSQL 租约、generation 与响应重放 store。
    #[cfg(feature = "idempotency-pgsql")]
    pub mod pgsql {
        pub use idempotency_pgsql_impl::*;
    }
    #[cfg(feature = "idempotency-redis")]
    pub use idempotency_redis_impl::RedisIdempotencyStore;
}

/// 确定性 OpenAPI 3.1 合同类型与生成器。
#[cfg(feature = "openapi")]
pub mod openapi {
    pub use openapi_impl::*;
}

/// 事务型业务审计：事件与业务写共享所选后端事务，经同源 Outbox 可靠投递。
#[cfg(any(feature = "audit", feature = "audit-pgsql"))]
pub mod audit {
    pub use audit_impl::{AuditEvent, AuditOutcome, AuditWriteError, TransactionalAuditSink};
    #[cfg(feature = "audit")]
    pub use audit_mysql_impl::MySqlOutboxAuditSink;

    /// PostgreSQL Outbox 审计 sink。
    #[cfg(feature = "audit-pgsql")]
    pub mod pgsql {
        pub use audit_pgsql_impl::*;
    }
}

/// Secret 容器、外部 provider 合同、原子 last-good 轮换与 TLS/mTLS 引用。
#[cfg(feature = "secret")]
pub mod secret {
    #[cfg(feature = "secret-http")]
    pub use secret_http_impl::{
        RotatingTlsHttpClient, TlsHttpClientConfig, TlsHttpClientError, TlsHttpClientSnapshot,
    };
    pub use secret_impl::*;
    #[cfg(feature = "secret-vault")]
    pub use secret_vault_impl::{VaultConfigError, VaultKvV2Provider, VaultOptions};
}

/// provider-neutral 有界对象存储与 S3-compatible adapter。
///
/// 当前合同只覆盖有界单对象缓冲、path-style SigV4、`CreateOnly` 条件写、幂等删除和默认
/// SHA-256 metadata 复核；不提供 multipart、流式/range/list、STS 刷新或对象版本治理。
/// 配合 application 时可通过 object_stores 配置命名资源，由宿主绑定凭据、指标与关闭门禁。
/// 独立 adapter 保留原构造入口；bucket、凭据和数据保留策略仍由业务显式配置。
#[cfg(feature = "object-store")]
pub mod object {
    pub use object_impl::*;
}

/// Saga 编排：纯逻辑合同（身份派生/封闭状态机/补偿计划），开启
/// `saga-runtime` 或 `saga-runtime-pgsql` 后再并入对应 Orchestrator、参与方 adapter 与 `#[saga]` 宏。
///
/// PostgreSQL 包装位于 `saga::pgsql`，与 MySQL 包装共享同一状态机与 transport 裁决；受管计划根据
/// `datasource_ref` 的 driver 选择对应 typed runtime。`saga-grpc` 与 `saga-grpc-pgsql` 都会打开稳定
/// `grpc` 门面；纯出站调用无需声明 Application 的 `"grpc"` 组件，
/// 入站计划则必须声明它以取得唯一受管 listener。`full` 会编入运行时以及 Kafka、gRPC adapter；
/// Redis Streams 替代通道仍需显式 feature。
/// 业务必须声明 Application 的 `"saga"` 组件并提交流程定义、参与方信任关系和
/// 发布端。DB 与 Outbox 由 Saga 声明隐式纳入，未装配计划时启动会 fail-closed。
#[cfg(feature = "saga")]
pub mod saga {
    #[cfg(feature = "saga-runtime")]
    pub use nasaga_macro::{saga, saga_workflow};
    pub use saga_core_impl::*;
    #[cfg(feature = "saga-runtime")]
    pub use saga_runtime_impl::*;

    /// PostgreSQL Saga 宏与运行包装；状态机与 MySQL 包装共享同一 core。
    #[cfg(feature = "saga-runtime-pgsql")]
    pub mod pgsql {
        pub use nasaga_macro::saga_pgsql as saga;
        pub use saga_runtime_pgsql_impl::*;
    }
}

/// 类型化 Saga workflow definition 的链接期登记宏。
#[cfg(feature = "saga-runtime")]
pub use nasaga_macro::saga_workflow;

/// 稳定 gRPC codegen 门面、generated service registry、独立/Application listener 与有界排空。
///
/// 业务实现 generated trait 后只登记 server；health、reflection、消息/stream/RPC 预算、listener
/// readiness 与 shutdown owner 由框架统一装配。独立进程通过 `ServerPlan` 使用同一运行合同。
#[cfg(feature = "grpc")]
pub mod grpc {
    pub use grpc_impl::{
        async_trait, call_with_budget, health, include_proto, propagate_deadline_from,
        propagate_request_budget, reflection, Certificate, Channel, ClientTlsConfig, Code,
        Deadline, DeadlineSource, Endpoint, GrpcMessageLimits, GrpcMethodDescriptor,
        GrpcMethodPolicy, GrpcMethodType, GrpcRpcMethodSnapshot, GrpcRpcOutcome,
        GrpcRpcRejectionReason, GrpcServerConfig, GrpcServerError, GrpcServerHandle,
        GrpcServerObserver, GrpcServerSnapshot, GrpcServerState, GrpcTlsIdentity, Identity,
        ManagedGrpcService, ManagedService, PeerIdentity, Request, Response, ServerPlan, Status,
        Streaming,
    };

    #[doc(hidden)]
    pub use grpc_impl::{codegen, GrpcServicePolicy, CODEGEN_ABI};

    #[cfg(all(feature = "application", feature = "rest-discovery-nacos"))]
    pub use application_impl::{GrpcDiscoveredEndpoint, GrpcDiscoveredTlsMode};
}

/// OAuth Resource Server 的 JWT/JWKS 与 RFC 8414 metadata adapter。
#[cfg(feature = "oauth")]
pub mod oauth {
    pub use oauth_impl::*;
}

/// 异步执行与定时任务 + `#[Async]` / `#[scheduled]` / `#[EnableScheduling]`(`#[EnableAsync]` 为兼容别名)。
/// 入口名称强调运行时任务而非系统线程，避免调用方误判调度和取消边界。
#[cfg(feature = "scheduling")]
pub mod scheduling {
    pub use async_macro::{scheduled, Async, EnableAsync, EnableScheduling};
    pub use scheduling_impl::*;
}

/// MVC 路由与 Web 安全编排门面。
///
/// 提供 `mvc_router!`、五个 `#[*_mapping]`、`#[interceptor]`、`MappingPlan` 和
/// `MappingRuntime`。端点属性可声明 auth、decrypt/encrypt、协议、provider/condition、replay、
/// response contract 与 endpoint interceptor；effective plan 固定 auth 早于 request decrypt。
/// 具体数据面由 `web-auth`、`web-crypto` 或组合 `web-security` feature 启用。
#[cfg(feature = "web")]
pub mod web {
    pub use web_impl::*;
    pub use web_macro::{
        delete_mapping, get_mapping, interceptor, mvc_router, patch_mapping, post_mapping,
        put_mapping,
    };
}

/// 声明式 Mapper：MySQL 保持既有根入口，PostgreSQL 使用独立的 `mapper::pgsql`。
#[cfg(any(feature = "mapper", feature = "mapper-pgsql"))]
pub mod mapper {
    #[cfg(feature = "mapper")]
    pub use mapper_impl::*;
    #[cfg(feature = "mapper")]
    pub use namapper_macro::{
        Delete, Execute, Insert, Mapper, MapperEnum, MapperOrderField, Query, StreamQuery, Update,
    };

    /// PostgreSQL `$n` 占位符、Pg 连接与独立 cache identity 的 Mapper 入口。
    #[cfg(feature = "mapper-pgsql")]
    pub mod pgsql {
        pub use mapper_pgsql_impl::*;
    }
}

/// 命名隔离、严格 FIFO 的本地有界保序任务窃取执行器：每个 Runner 独立拥有 generation、slot
/// 主队列、类型路由、盗洞、延迟索引、容量和停机权威。热点任务会搬入空闲 slot 的盗洞；严格类型
/// 在迁移与归还期间保持同一受理序号，并提供非阻塞或等待型背压、任务 panic 隔离、健康与显式异步停机。
/// 已登记 delayed 任务到期时仍可能稳定拒绝；取消与到期竞争只发布一个终态并只结算一次许可。
///
/// 业务可以直接持有 `PartitionRunnerRegistry`，在 Tokio runtime 运行期间按稳定名称创建并启动 Runner；
/// 同一名称只在同一个注册表内代表同一执行域，直接模式由调用方负责完整停机。声明 Application 的
/// `"partition"` 组件则改由 YAML 或 Service UserHook 提交启动期计划，Prepare 统一启动并接管 readiness
/// 与停机，但不会在 Running 阶段追加 Runner。
///
/// Redis 分区消费由 `nasa::redis::partition::RunningPartition` 自行管理独占 Runner 集合，
/// 支持按 source、group 或 stream 划分执行与容量；跨域同业务键仍遵守消费顺序屏障。
/// 不复用本模块的 Application 命名执行域。命名 Runner 共享 Tokio runtime，不提供 CPU 或内存硬隔离。
///
/// ```
/// use nasa::partition::{PartitionRunnerRegistry, RunnerConfig};
/// ```
#[cfg(feature = "partition")]
pub mod partition {
    pub use partition_impl::*;
}

/// NASA 长连接框架(TCP/WebSocket/socket.io + 集群 fan-out)。
/// `ws-client` 提供纯出站 TCP Client；与 application 组合后由 ws_clients 命名配置装配。
/// 子能力经 feature 透传:`ws-redis`(Redis Stream 集群)、`ws-socketio`(socket.io 兼容)。
#[cfg(any(feature = "ws", feature = "ws-client"))]
pub mod ws {
    pub use ws_impl::*;

    // 显式提升高频 wire 类型(nasa::ws::proto 路径仍保留;不再增加顶层 nasa::proto)。
    pub use ws_impl::proto::{Message, Mode, WireCodec};

    /// 长连接消息队列数据面适配器与安全 typed publisher。
    #[cfg(feature = "ws-kafka")]
    pub mod kafka {
        pub use ws_impl::kafka::*;
    }
}

/// Kafka 发布、消费组、手动确认、管理端与同步借用式少拷贝入口。
///
/// `kafka-schema-registry` 额外开放 Confluent envelope、schema ID 白名单、有界正负缓存和显式
/// 兼容性/注册控制面。独立 Registry client 由业务持有；配合 `application` 时，
/// `schema_registries.<name>` 在 Prepare 装配命名资源，使用 `Application::schema_registry` 获取，
/// 由宿主登记指标、关闭新调用并等待在途调用，无需声明 Kafka 消费组件。构造不证明远端 readiness。
#[cfg(feature = "kafka")]
pub mod kafka {
    pub use kafka_impl::*;

    /// 把 Schema Registry client 的查询结局并入 Application 统一指标目录的兼容源。
    ///
    /// 独立 client 可在 UserHook 用 `app.register_metrics_source(...)` 接入；多个独立 client 使用
    /// `metrics_source_many` 登记一个聚合源。`schema_registries` 受管路径自动登记聚合源，不能重复登记。
    /// Prometheus 文本端点与 OTLP 指标导出共用同一份
    /// 进程级快照，且不把 endpoint 或 subject 引入 label。
    #[cfg(all(feature = "application", feature = "kafka-schema-registry"))]
    pub use kafka_impl::schema_metrics;
}

/// Redis 基础层提供 client/commands/pipeline(typed ticket)/lock(V1 wire lock 互操作)/partition
/// (PollCoordinator)。子能力经 feature 透传:`redis-search`(RediSearch/
/// RedisJSON 封装)、`redis-derive`(`#[derive(RedisDocument)]`,蕴含 search)。
///
///   use nasa::redis::{RedisClient, RedisConfig, CompatibilityProfile};
///   use nasa::redis::{DistributedLock, PipelineSession, PreparedPartition};
///   use nasa::redis::{SearchActuator, JsonArrayOps, RedisDocument};  // redis-search/-derive
#[cfg(feature = "redis")]
pub mod redis {
    pub use redis_impl::*;
}

/// 密码学工具(crate = `ncrypto`)。
/// `nasa = { features = ["crypto"] }` → `use nasa::crypto::{encrypt_aes, sha256, sign_rsa, ...};`
/// 提供 hash/hmac/pbkdf2/aes/rsa/ed25519/base64；Web 端点加解密由 mapping 路由属性
/// `decrypt = true` / `encrypt = true` 和统一 Web 安全运行时编排，不提供相互冲突的独立属性宏。
#[cfg(feature = "crypto")]
pub mod crypto {
    pub use crypto_impl::*;
}

/// 精确算术(crate = `numeric`)。
/// `nasa = { features = ["numeric"] }` → `use nasa::numeric::{multiply, divide, align, to_fixed_str, decimal, float, ...};`
/// i128 定点核 ×10^scale(scale≤8,默认 8)+ 全 RoundingMode + 撮合 tick 对齐 + I/O;
/// `numeric::decimal`(BigDecimal,scale>8 任意精度)+ `numeric::float`(double 便捷算术)。
#[cfg(feature = "numeric")]
pub mod numeric {
    pub use numeric_impl::*;
}

/// 日期时间便捷入口，与 `nasa::base::date` 指向同一实现。
/// `nasa = { features = ["base"] }` → `use nasa::date::{format, parse, add_days, today, ...};`
/// 日期时刻统一使用 i64 epoch 毫秒，默认采用 GMT+8 固定偏移。
#[cfg(feature = "base")]
pub mod date {
    pub use base_impl::date::*;
}

/// 日志(crate = `nalog`,基于 tracing)。
/// `nasa = { features = ["log"] }` → `use nasa::log;` → `log::init();`。
/// 文本 formatter + 独立 `error.log` + 按天/按大小滚动(`maxFileSize`/`.%i`)
/// + `maxHistory`/`totalSizeCap`/`cleanHistoryOnStart` 保留清理 + 运行期级别热切(配合 nacos)。
///
///   use nasa::log;
///   log::init_with_default("info");                       // 仅控制台
///   log::set_level("info,my_app=debug");                  // 热切级别
///   let _g = log::enable_file_logging(Some("/usr/local/logs/my-app")); // 接 info.log + error.log
#[cfg(feature = "log")]
pub mod log {
    pub use log_impl::*;
}

/// 公共响应、日期时间、容量、ID、字符串、环境变量和翻译能力(crate = `nabase`)。
/// `nasa = { features = ["base"] }` → `use nasa::base::BaseResponse;` → `BaseResponse::ok(data)` / `::err(code, msg)`。
/// 字段 `code`(默认 200)/ `msg`(提示信息)/ `aes`(需加密时的 AES 密钥)/ `data`,`None` 序列化省略。
/// 日期能力位于 `nasa::base::date`，也可以从 `nasa::date` 便捷入口使用；启用 `base` 会引入
/// `nabase` 的全部依赖和公开模块。
#[cfg(feature = "base")]
pub mod base {
    pub use base_impl::*;
}

/// 通用分层 YAML 配置加载器(crate = `yml`)。
/// `nasa = { features = ["yml"] }` → `use nasa::yml::{YmlLoader, YmlOverlay};`
/// `nasa = { features = ["yml-watch"] }` → `use nasa::yml::watch::YmlWatcher;`
/// 本地主配置 `zcf/application.yml` + profile + overlay(含 Nacos 多配置)+ 环境变量 + `${}` 占位符 → 强类型 `T`。
///
///   let cfg: AppConfig = nasa::yml::YmlLoader::standard().load()?;                       // 纯本地
///   let cfg: AppConfig = nasa::yml::YmlLoader::standard().load_with_overlays(&ovs)?;     // 叠加 Nacos 多配置
///
/// watcher 只报告精确来源变化，候选校验、运行态资源准备和配置发布仍由应用负责。
/// 边界:**不连接 Nacos、不存全局、不热替换、不认识业务 AppConfig**;`import` 只产出中性
/// `YmlImport`(File/Nacos 描述),「按 import 调 Nacos 拉取拼 overlay」的胶水在门面/app 侧(yml 零 Nacos 依赖)。
#[cfg(feature = "yml")]
pub mod yml {
    pub use yml_impl::*;

    /// yml × nacos 组合胶水(crate = `config-boot`)。共享 `NacosBootstrap` 引导配置取代各 app 手写的 NacosConfig。
    /// `nasa = { features = ["config-boot"] }` → app 引导
    ///   `let boot: BootstrapConfig = nasa::yml::nacos::load_bootstrap_checked(&loader())?;  // load_tree+旧字段守卫+反序列化`
    ///   `let imports = nasa::yml::nacos::resolve_imports(&loader().load_tree()?, loader().base_file_dir(), &boot.nacos)?;`
    ///   `let client = nasa::yml::nacos::connect_config_client(&boot.nacos).await?;`
    ///   `let ovs = nasa::yml::nacos::resolve_ordered_overlays_for_bootstrap(&client, &imports, &boot.nacos).await?;`
    /// 热刷新:`nacos_refs_for_bootstrap` → `watch_many_channel` → bundle → `assemble_overlays_from_bundle_for_bootstrap` → `load_with_overlays`。
    /// yml 对 Nacos 零认知、nacos 对 yml 零认知;「按 import 拉取拼 overlay + file_extension 格式解析 + 旧字段守卫」只在这层。
    #[cfg(feature = "config-boot")]
    pub mod nacos {
        pub use config_boot_impl::*;
    }
}

/// 图片压缩/缩放(crate = `image`,基于 image crate)。
/// `nasa = { features = ["image"] }` → `use nasa::image::{compress, compress_scale, CompressOpts, ...};`
/// 质量(JPEG)+ 尺寸(scale/width/height)压缩;默认保留输入格式。
#[cfg(feature = "image")]
pub mod image {
    pub use image_impl::*;
}

/// 服务发现/注册(provider-neutral)：中性类型 + 各后端子模块。
/// `Instance` 与具体注册中心无关,后端复用;后端按需开 feature:`nasa::discovery::nacos`(以后可加 `::eureka`)。
/// `nasa = { features = ["nacos-sdk"] }` → `use nasa::discovery::{Instance, nacos::{NacosDiscoveryClient, NacosProps}};`
///   `client.register(...)`(drop best-effort deregister;优雅停机显式 deregister)/ `discover`(健康,LB 用)/ `discover_all`(全部,管理诊断)/
///   `subscribe_channel`(LB 推荐:discover 轮询兜底,可靠反映"删到空";`subscribe_channel_with_options` 配 `SubscribeOptions` 调轮询间隔)/
///   `subscribe`(低层原始 SDK 事件,不适合 LB)。
/// 对外注册 IP:置 `NacosProps.discovery_ip`(多网卡/VPN/容器/监听 `0.0.0.0` 时必填)→ `register` 时覆盖 `Instance.ip`;
///   优先级:app 配置 / env → `NacosProps.discovery_ip` → 调用方传入的 `Instance.ip`。
#[cfg(feature = "discovery")]
pub mod discovery {
    // provider-neutral 中性类型 + 流量过滤规则 + 抽象接口(业务/RestDiscovery 面向这些,不绑定后端)。
    pub use discovery_impl::{
        is_traffic_instance, DiscoveryClient, DnsDiscovery, DnsService, Instance, Registration,
        ServiceRegistry, ServiceWatch, ServiceWatchGuard, StaticDiscovery, WatchOptions,
    };

    /// Nacos 注册中心后端(独立 `NacosDiscoveryClient`,只建命名服务)。**只给机制**(注册/心跳/优雅下线/发现/订阅),不认识业务类型。
    /// 仅 `["nacos"]`(不带 `-sdk`):API 可编译但运行时 bail,供单 binary 运行期条件启用。
    /// 后端子模块只导出 client/guard/props;中性类型/规则/接口从 `nasa::discovery` 顶层取(不绑定后端语义)。
    #[cfg(feature = "nacos")]
    pub mod nacos {
        pub use nacos_impl::{
            NacosDiscoveryClient, NacosProps, RegistrationGuard, SubscribeGuard, SubscribeOptions,
        };
    }

    /// 带服务发现与客户端负载均衡的 HTTP 门面(crate = `rest-discovery`)。
    /// `nasa = { features = ["rest-discovery-nacos"] }` → main 里:
    ///   `RestDiscovery::init_with_discovery(Arc::new(nacos_client), opts).await?;`(或 `init_external_only`)
    /// 任意位置:`RestDiscovery::get().request(Method::GET, "lb://svc/path").send().await?;`
    /// 三档:`service_request`/`lb://` 显式内部直连;裸 `http(s)` 默认普通外部,`heuristic_http=Enabled` 时
    /// host 命中服务名索引才走内部 LB(未命中按 `unknown_host`:外部直连 / `UnknownServiceHost`)。
    #[cfg(feature = "rest-discovery")]
    pub use rest_discovery_impl::RestDiscovery;

    /// 一键装配入口(crate = `rest-discovery-nacos`):读 `DiscoveryConfig`(yml)→ 连 Nacos →(可选)注册本实例
    /// → 装配 `RestDiscovery`。`nasa = { features = ["rest-discovery-nacos"] }` → main:
    ///   `let disc = nasa::discovery::init_from_config(&cfg, app_info).await?;`
    /// `disc`(`DiscoveryHandle`)由 main 持有到进程结束;优雅停机【先】`disc.deregister().await` 摘流、【再】drain HTTP。
    #[cfg(feature = "rest-discovery-nacos")]
    pub use rest_discovery_nacos_impl::{
        init_from_config, init_from_config_with_load_balancer, AppRegistrationInfo,
        DiscoveryConfig, DiscoveryHandle, HttpConfig, NacosConnConfig, ProviderKind,
        RegistrationConfig, RestConfig, RetryConfig, WatchConfig,
    };

    /// `rest-discovery` 的底层类型(client / builder / 选项 / LB)。手动装配时用:
    ///   `let kline = Arc::new(KlineRestClient::new(RestDiscovery::get()));`
    #[cfg(feature = "rest-discovery")]
    pub mod rest {
        #[doc(hidden)]
        pub use rest_discovery_impl::__private;
        pub use rest_discovery_impl::{
            reqwest, HeuristicHttpMode, InstanceScheme, LbStrategy, LoadBalancer, Method,
            NoInstancePolicy, RemoteRuntime, RequestBudget, RestDiscoveryClient,
            RestDiscoveryError, RestDiscoveryOptions, RestHeuristicOptions, RestHttpOptions,
            RestMetrics, RestMetricsSnapshot, RestRequestBuilder, RestResilienceOptions,
            RestWatchOptions, RetryOptions, RoundRobinLoadBalancer, SchemePolicy, ServiceMatchMode,
            SpanRecorder, StartupPolicy, StatusCode, TraceContext, UnknownHostPolicy,
            WeightedRoundRobinLoadBalancer,
        };

        /// 声明式 REST 客户端宏(crate = `rest-client-macro`)。
        /// `#[rest_client]` trait + `#[GetMapping/PostMapping/PutMapping/PatchMapping/DeleteMapping]` 方法属性。
        /// 参数 helper(`#[PathVariable]`/`#[RequestParam]`/`#[RequestHeader]`/`#[RequestHeaders]`/`#[QueryMap]`/`#[RequestBody]`/`#[FormBody]`)
        /// 无需 import,由 `#[rest_client]` 消费。
        #[cfg(feature = "rest-client")]
        pub use rest_client_macro::{
            rest_client, DeleteMapping, GetMapping, PatchMapping, PostMapping, PutMapping,
        };
    }
}

/// 配置中心(provider-neutral 命名空间)：各后端子模块。
/// 后端只提供原始配置文本与 watch 回调，不解析业务 `AppConfig`；解析、合并与应用策略由应用层负责。
/// `nasa = { features = ["nacos-sdk"] }` → `use nasa::config::nacos::{NacosConfigClient, NacosProps};`
///   `client.fetch(data_id, group)`(裸 yaml)/ `client.watch(...)`(推送回调拿裸 yaml)。
/// (配置与注册各用独立 client,共享 `NacosProps`:只用一边不会被迫初始化另一边。)
#[cfg(feature = "nacos")]
pub mod config {
    /// Nacos 配置中心后端(独立 `NacosConfigClient`,只建配置服务)。
    /// 单配置:`fetch`/`watch`/`watch_channel`(裸文本 + `WatchGuard`)。
    /// 多配置:`fetch_many`/`watch_many_channel`(按序拉一组 → `ConfigBundle` + `MultiWatchGuard`)。
    pub mod nacos {
        pub use nacos_impl::{
            ConfigBundle, ConfigDocument, ConfigRef, MultiWatchGuard, NacosConfigClient,
            NacosProps, WatchGuard,
        };
    }
    // 配置引导胶水(yml × nacos)统一走 nasa::yml::nacos;此处不再暴露 nasa::config::boot。
}
