//! nasa-runtime-rust 的统一业务门面。
//!
//! 业务项目优先依赖本 crate，并通过 feature 选择需要的缓存、事务、路由、调度、Redis、
//! 长连接、配置、发现和工具模块。
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
// 过程宏经 nasa-macro-support 自动发现本 crate(含 Cargo 重命名),
// 完整属性路径 #[nasa::hystrix::hystrix] / #[nasa::web::get_mapping("/x")] 同样可用。
// ============================================================================
#![forbid(unsafe_code)]

/// 应用运行时：生命周期、配置快照、类型资源容器和受管任务。
#[cfg(feature = "application")]
pub mod application {
    pub use application_impl::*;
    pub use application_macro::application;
}

#[cfg(feature = "application")]
pub use application_impl::Application;
#[cfg(feature = "application")]
pub use application_macro::application;

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

    /// nafana → napp 统一 hub 兼容源。
    ///
    /// napp 不直接依赖 nafana(避免倒置分层),故在门面层把 nafana 全局 registry 的 Prometheus 渲染
    /// 包成 `LegacyMetricsSource`。业务在 UserHook 一行接入:
    /// `app.register_metrics_source(nasa::grafana::metrics_source())?`——nafana 的族随框架统一
    /// `/metrics` 一并渲染,并纳入 descriptor 冲突审计,无需再单独挂 `nafana::metrics`。
    ///
    /// 需同时启用 `application` 与 `web`(统一 `/metrics` 由 napp 的 Web 组件暴露)。
    #[cfg(all(feature = "application", feature = "web"))]
    mod hub_source {
        use std::sync::Arc;

        use application_impl::{LegacyMetricsSource, MetricDescriptor, MetricKind};

        macro_rules! nafana_desc {
            ($ident:ident, $name:literal, $help:literal, $kind:expr, $labels:expr) => {
                nafana_desc!($ident, $name, $help, $kind, $labels, &[]);
            };
            ($ident:ident, $name:literal, $help:literal, $kind:expr, $labels:expr, $bounds:expr) => {
                static $ident: MetricDescriptor = MetricDescriptor {
                    name: $name,
                    help: $help,
                    unit: "",
                    kind: $kind,
                    label_names: $labels,
                    histogram_bounds: $bounds,
                };
            };
        }

        nafana_desc!(
            REQUESTS_TOTAL,
            "nafana_requests_total",
            "接口请求结局单调计数(success/failure/timeout/rejected/canceled)。",
            MetricKind::Counter,
            &["command", "group", "outcome"]
        );
        nafana_desc!(
            FALLBACK_TOTAL,
            "nafana_fallback_total",
            "拒绝/超时分支产出降级响应的单调计数。",
            MetricKind::Counter,
            &["command", "group"]
        );
        nafana_desc!(
            TPS_TOTAL,
            "nafana_tps_total",
            "TPS 单调计数:每请求按 tps_weight 累加。",
            MetricKind::Counter,
            &["command", "group"]
        );
        nafana_desc!(
            INFLIGHT,
            "nafana_inflight",
            "当前执行区并发。",
            MetricKind::Gauge,
            &["command", "group"]
        );
        nafana_desc!(
            INFLIGHT_ROLLING_MAX,
            "nafana_inflight_rolling_max",
            "10s 滚动窗口内并发峰值(随窗口回落)。",
            MetricKind::Gauge,
            &["command", "group"]
        );
        nafana_desc!(
            INFLIGHT_LIFETIME_MAX,
            "nafana_inflight_lifetime_max",
            "进程生命周期并发峰值(只增不减)。",
            MetricKind::Gauge,
            &["command", "group"]
        );
        nafana_desc!(
            MAX_CONCURRENT,
            "nafana_max_concurrent",
            "bulkhead 容量;0 = 不限并发。",
            MetricKind::Gauge,
            &["command", "group"]
        );
        nafana_desc!(
            TIMEOUT_MS,
            "nafana_timeout_ms",
            "单请求超时毫秒;0 = 不超时。",
            MetricKind::Gauge,
            &["command", "group"]
        );
        nafana_desc!(
            TPS_WEIGHT,
            "nafana_tps_weight",
            "TPS 权重;0 = 未标 TPS 或权重 0。",
            MetricKind::Gauge,
            &["command", "group"]
        );
        nafana_desc!(
            LATENCY,
            "nafana_latency_seconds",
            "执行延迟直方图(秒);rejected/canceled 不进延迟统计。",
            MetricKind::Histogram,
            &["command", "group"],
            &[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        );
        nafana_desc!(
            COMMAND_INFO,
            "nafana_command_info",
            "命令展示元信息(path = 真实路由)。",
            MetricKind::Gauge,
            &["command", "group", "path"]
        );

        /// nafana 全部指标族的静态 descriptor manifest。
        static NAFANA_DESCRIPTORS: [&MetricDescriptor; 11] = [
            &REQUESTS_TOTAL,
            &FALLBACK_TOTAL,
            &TPS_TOTAL,
            &INFLIGHT,
            &INFLIGHT_ROLLING_MAX,
            &INFLIGHT_LIFETIME_MAX,
            &MAX_CONCURRENT,
            &TIMEOUT_MS,
            &TPS_WEIGHT,
            &LATENCY,
            &COMMAND_INFO,
        ];

        /// 把 nafana 全局 registry 的 Prometheus 渲染包成兼容源。
        struct NafanaMetricsSource;

        impl LegacyMetricsSource for NafanaMetricsSource {
            /// 返回 nafana 兼容源拥有的静态指标族目录。
            fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
                &NAFANA_DESCRIPTORS
            }

            /// 读取 nafana 全局 registry 当前快照并追加 Prometheus exposition。
            fn render_prometheus(&self, output: &mut String) {
                output.push_str(&super::render_metrics());
            }
        }

        /// 返回 nafana 兼容源,供 `Application::register_metrics_source` 并入统一 hub。
        ///
        /// # 返回
        ///
        /// 一个无状态源:每次渲染读取 nafana 进程级全局 registry 的当前快照。
        pub fn metrics_source() -> Arc<dyn LegacyMetricsSource> {
            Arc::new(NafanaMetricsSource)
        }
    }

    #[cfg(all(feature = "application", feature = "web"))]
    pub use hub_source::metrics_source;
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

/// ambient 事务(task_local)+ `#[transactional]`(对标 原框架 @Transactional)。
#[cfg(feature = "tx")]
pub mod tx {
    pub use natx_macro::transactional;
    pub use tx_impl::*;
}

/// 消息 Inbox：同一 MySQL 事务内的消费去重标记。
#[cfg(feature = "inbox")]
pub mod inbox {
    pub use inbox_core_impl::InboxClaim;
    pub use inbox_mysql_impl::{InboxStoreError, MySqlInbox};
}

/// 事务型 Outbox：事件、顺序投递合同与 MySQL 持久化实现。
#[cfg(feature = "outbox")]
pub mod outbox {
    pub use outbox_core_impl::{
        dispatch_in_order, DispatchReport, InMemoryOutbox, OutboxEvent, OutboxPublishError,
        OutboxPublisher, OutboxWriter,
    };
    pub use outbox_mysql_impl::{MySqlOutbox, OutboxStoreError};
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
    #[cfg(feature = "idempotency-redis")]
    pub use idempotency_redis_impl::RedisIdempotencyStore;
}

/// 确定性 OpenAPI 3.1 合同类型与生成器。
#[cfg(feature = "openapi")]
pub mod openapi {
    pub use openapi_impl::*;
}

/// 事务型业务审计：事件与业务写共享 MySQL 事务，经 Outbox 可靠投递。
#[cfg(feature = "audit")]
pub mod audit {
    pub use audit_impl::{AuditEvent, AuditOutcome, AuditWriteError, TransactionalAuditSink};
    pub use audit_mysql_impl::MySqlOutboxAuditSink;
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

/// 实验性 provider-neutral 对象存储与 S3-compatible adapter。
///
/// 此模块不进入 `full`；稳定公共合同仍需两个真实上传/导出/归档项目共同验证。
#[cfg(feature = "object-store-experimental")]
pub mod object {
    pub use object_impl::*;
}

/// 实验性 gRPC transport、health/reflection 与 graceful drain。
#[cfg(feature = "grpc-experimental")]
pub mod grpc {
    pub use grpc_impl::*;
}

/// OAuth Resource Server 的 JWT/JWKS 与 RFC 8414 metadata adapter。
#[cfg(feature = "oauth")]
pub mod oauth {
    pub use oauth_impl::*;
}

/// 异步执行与定时任务 + `#[Async]` / `#[scheduled]` / `#[EnableScheduling]`(`#[EnableAsync]` 为兼容别名)。
/// (语义对标 原框架 @Async/@Scheduled;刻意不叫 `thread`,避免与系统线程混淆。)
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

/// MyBatis 风格声明式 Mapper:trait + `#[Mapper]` / `#[Query]` / `#[Insert]` 等属性宏。
#[cfg(feature = "mapper")]
pub mod mapper {
    pub use mapper_impl::*;
    pub use namapper_macro::{
        Delete, Execute, Insert, Mapper, MapperEnum, MapperOrderField, Query, StreamQuery, Update,
    };
}

/// 同 key 串行消费执行器(对照 原实现 TimingWheel.partition):同 key 严格按提交顺序串行、
/// 不同 key 最大并发;不丢任务的优雅停机 + worker 死亡检测/黑洞拒收/健康面板。
/// (注意:与 `nasa::redis::partition` 的 PollCoordinator 是两个不同概念——这里是本地
/// 同 key 串行执行器,redis 那个是分布式分区消费。)
///
///   use nasa::partition::PartitionExecutor;
#[cfg(feature = "partition")]
pub mod partition {
    pub use partition_impl::*;
}

/// NASA 长连接框架(TCP/WebSocket/socket.io + 集群 fan-out)。
/// 子能力经 feature 透传:`ws-redis`(Redis Stream 集群)、`ws-socketio`(socket.io 兼容)。
#[cfg(feature = "ws")]
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
#[cfg(feature = "kafka")]
pub mod kafka {
    pub use kafka_impl::*;
}

/// Redis 基础层，对齐既有 RedisProxy 五件套的公开语义：
/// client/commands/pipeline(typed ticket)/lock(V1 与 原实现 互锁)/partition
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

/// 密码学工具(对照 原实现 原工具包 `Encryptor`;crate = `ncrypto`)。
/// `nasa = { features = ["crypto"] }` → `use nasa::crypto::{encrypt_aes, sha256, sign_rsa, ...};`
/// 提供 hash/hmac/pbkdf2/aes/rsa/ed25519/base64；Web 端点加解密由 mapping 路由属性
/// `decrypt = true` / `encrypt = true` 和统一 Web 安全运行时编排，不提供相互冲突的独立属性宏。
#[cfg(feature = "crypto")]
pub mod crypto {
    pub use crypto_impl::*;
}

/// 精确算术(对照 原实现 原工具包 `Numeric` **全量迁移**;crate = `numeric`)。
/// `nasa = { features = ["numeric"] }` → `use nasa::numeric::{multiply, divide, align, to_fixed_str, decimal, float, ...};`
/// i128 定点核 ×10^scale(scale≤8,默认 8)+ 全 RoundingMode + 撮合 tick 对齐 + I/O;
/// `numeric::decimal`(BigDecimal,scale>8 任意精度)+ `numeric::float`(double 便捷算术)。
#[cfg(feature = "numeric")]
pub mod numeric {
    pub use numeric_impl::*;
}

/// 日期时间工具(对照 原实现 原工具包 `DateUtils`;crate = `date`,基于 chrono)。
/// `nasa = { features = ["date"] }` → `use nasa::date::{format, parse, add_days, today, ...};`
/// i64 epoch ms 规范 + GMT+8 默认 + 原实现 SimpleDateFormat 风格 pattern(`"yyyy-MM-dd HH:mm:ss"`)。
#[cfg(feature = "date")]
pub mod date {
    pub use date_impl::*;
}

/// 日志(对照 原实现 原工具包 `logback-原框架.xml`;crate = `nlog`,基于 tracing)。
/// `nasa = { features = ["log"] }` → `use nasa::log;` → `log::init();`。
/// 原实现 logback 风格 formatter + 独立 `error.log` + 按天/按大小滚动(`maxFileSize`/`.%i`)
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

/// 通用响应壳 `BaseResponse`(对照 原实现 原工具包 `com.nasa.common.base.BaseResponse`;crate = `base`)。
/// `nasa = { features = ["base"] }` → `use nasa::base::BaseResponse;` → `BaseResponse::ok(data)` / `::err(code, msg)`。
/// 字段 `code`(默认 200)/ `msg`(提示信息)/ `aes`(需加密时的 AES 密钥)/ `data`,`None` 序列化省略。
/// 另含 strings/env/size/id 纯工具;date/numeric/crypto/image 继续走 `nasa::{date,numeric,crypto,image}` 顶层入口。
#[cfg(feature = "base")]
pub mod base {
    pub use base_impl::*;
}

/// 通用分层 YAML 配置加载器(对照;crate = `yml`)。
/// `nasa = { features = ["yml"] }` → `use nasa::yml::{YmlLoader, YmlOverlay};`
/// 本地主配置 `zcf/application.yml` + profile + overlay(含 Nacos 多配置)+ 环境变量 + `${}` 占位符 → 强类型 `T`。
///
///   let cfg: AppConfig = nasa::yml::YmlLoader::standard().load()?;                       // 纯本地
///   let cfg: AppConfig = nasa::yml::YmlLoader::standard().load_with_overlays(&ovs)?;     // 叠加 Nacos 多配置
///
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

/// 图片压缩/缩放(对照 原实现 原工具包 `ImageUtils`;crate = `image`,基于 crates.io image crate)。
/// `nasa = { features = ["image"] }` → `use nasa::image::{compress, compress_scale, CompressOpts, ...};`
/// 质量(JPEG)+ 尺寸(scale/width/height)压缩;默认保留输入格式。
#[cfg(feature = "image")]
pub mod image {
    pub use image_impl::*;
}

/// 服务发现/注册(provider-neutral,对照 原框架 DiscoveryClient):中性类型 + 各后端子模块。
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

    /// 带服务发现 + 客户端负载均衡的 HTTP 门面(crate = `rest-discovery`,对标 `@LoadBalanced RestTemplate`)。
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

        /// OpenFeign 风格声明式客户端宏(crate = `rest-client-macro`)。
        /// `#[rest_client]` trait + `#[GetMapping/PostMapping/PutMapping/PatchMapping/DeleteMapping]` 方法属性。
        /// 参数 helper(`#[PathVariable]`/`#[RequestParam]`/`#[RequestHeader]`/`#[RequestHeaders]`/`#[QueryMap]`/`#[RequestBody]`/`#[FormBody]`)
        /// 无需 import,由 `#[rest_client]` 消费。
        #[cfg(feature = "rest-client")]
        pub use rest_client_macro::{
            rest_client, DeleteMapping, GetMapping, PatchMapping, PostMapping, PutMapping,
        };
    }
}

/// 配置中心(provider-neutral 命名空间,对照 原框架 配置分离):各后端子模块。
/// 后端只提供原始配置文本与 watch 回调，不解析业务 `AppConfig`；解析、合并与应用策略由应用层负责。
/// `nasa = { features = ["nacos-sdk"] }` → `use nasa::config::nacos::{NacosConfigClient, NacosProps};`
///   `client.fetch(data_id, group)`(裸 yaml)/ `client.watch(...)`(推送回调拿裸 yaml)。
/// (配置与注册各用独立 client,共享 `NacosProps`:只用一边不会被迫初始化另一边。)
#[cfg(feature = "nacos")]
pub mod config {
    /// Nacos 配置中心后端(独立 `NacosConfigClient`,只建配置服务)。
    /// 单配置:`fetch`/`watch`/`watch_channel`(裸文本 + `WatchGuard`)。
    /// 多配置(对照):`fetch_many`/`watch_many_channel`(按序拉一组 → `ConfigBundle` + `MultiWatchGuard`)。
    pub mod nacos {
        pub use nacos_impl::{
            ConfigBundle, ConfigDocument, ConfigRef, MultiWatchGuard, NacosConfigClient,
            NacosProps, WatchGuard,
        };
    }
    // 配置引导胶水(yml × nacos)统一走 nasa::yml::nacos;此处不再暴露 nasa::config::boot。
}
