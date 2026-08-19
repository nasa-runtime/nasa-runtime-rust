//! nasa-runtime-rust 的统一业务门面。
//!
//! 业务项目优先依赖本 crate，并通过 feature 选择需要的应用生命周期、事务、Inbox、Outbox、
//! Saga、消息传输、缓存、路由、调度、配置、发现和工具模块。
//!
//! # 持久化 Saga
//!
//! `saga-runtime` 将本地 ACID、Outbox 至少一次、Inbox 幂等、持久化状态机与显式补偿组合为
//! 最终一致性流程。稳定 `effect_id`、定义摘要、取消/裁决屏障、冻结补偿计划与 timer fencing
//! 让重复投递、Unknown 结果、进程崩溃和多副本竞争从已提交事实收敛。Kafka 和 Redis Streams
//! 提供受管 connector；HTTP 使用显式认证构件；gRPC 提供框架 generated command/result service、
//! mTLS principal 绑定和封闭收据。Saga 不提供跨服务 ACID、物理 exactly-once 或并发隔离。
//!
//! 启用 `application` 后，`#[nasa::initializer]` 与 `Application::register_initializer` 提供统一的
//! Ready 前业务初始化屏障。Runner 在 migration 和出站依赖准备完成后执行三轮全局屏障，全部成功
//! 才开放监听、消费与服务发现；依赖边优先于 `order`，失败会阻止 Ready 并进入逆序清理。
//!
//! # 受管基础设施
//!
//! `kafka-schema-registry` 提供有界 Schema Registry client 与批准 ID 门禁；
//! `object-store` 提供有界单对象合同、SigV4 adapter 与内容完整性复核；`grpc` 提供统一
//! codegen、service registry、TLS/mTLS、HTTP/2 资源门禁和有预算排空。它们都进入 `full`，
//! 业务只通过本门面使用公开合同。
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

/// 应用运行时：生命周期、Ready 前业务初始化屏障、配置快照、类型资源容器和受管任务。
///
/// `#[nasa::application("saga")]` 会隐式纳入 DB 与 Outbox；独立
/// `#[nasa::application("outbox")]` 会隐式纳入 DB。Inbox 是事务内原语，不声明为生命周期组件；
/// Kafka、Redis Streams 或 HTTP 等消息传输由业务按实际实现显式选择。
///
/// `#[nasa::initializer]` 静态项与 Service 启动 Hook 动态登记项合并后，在组件 `Prepare` 与
/// `Seal` 之间执行全部 `before -> initialize -> after`。全部成功前不发布 Ready；外部已提交事实
/// 不会被本地逆序清理撤销，业务实现必须使用事务或稳定幂等键保证可安全重跑。
#[cfg(feature = "application")]
pub mod application {
    pub use application_impl::*;
    pub use application_macro::{application, initializer};
}

#[cfg(feature = "application")]
pub use application_impl::Application;
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
            GLOBAL_FALLBACK_TOTAL,
            "nafana_global_fallback_total",
            "全局降级处理器结局单调计数。",
            MetricKind::Counter,
            &["command", "group", "outcome"]
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
        static LATENCY: MetricDescriptor = MetricDescriptor {
            name: "nafana_latency_seconds",
            help: "执行延迟直方图(秒);rejected/canceled 不进延迟统计。",
            unit: "seconds",
            kind: MetricKind::Histogram,
            label_names: &["command", "group"],
            histogram_bounds: &[
                0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        };
        nafana_desc!(
            COMMAND_INFO,
            "nafana_command_info",
            "命令展示元信息(path = 真实路由)。",
            MetricKind::Gauge,
            &["command", "group", "path"]
        );

        /// nafana 全部指标族的静态 descriptor manifest。
        static NAFANA_DESCRIPTORS: [&MetricDescriptor; 12] = [
            &REQUESTS_TOTAL,
            &FALLBACK_TOTAL,
            &GLOBAL_FALLBACK_TOTAL,
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
            /// 业务作用：返回 nafana 兼容源拥有的静态指标族目录。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：启动期冲突审计和结构化校验共用的全部 nafana descriptor。
            fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
                &NAFANA_DESCRIPTORS
            }

            /// 业务作用：把 nafana 全局 registry 的结构化快照映射到统一指标样本。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：始终为 `Some`，其中保留 command、group、结局与直方图形状的
            /// provider-neutral 样本。
            fn snapshot(&self) -> Option<Vec<application_impl::MetricSample>> {
                Some(
                    super::structured_metrics_snapshot()
                        .into_iter()
                        .map(|sample| application_impl::MetricSample {
                            name: sample.name,
                            labels: sample.labels,
                            value: match sample.value {
                                super::PrometheusMetricValue::Counter(value) => {
                                    application_impl::MetricValue::Counter(value)
                                }
                                super::PrometheusMetricValue::Gauge(value) => {
                                    application_impl::MetricValue::Gauge(value)
                                }
                                super::PrometheusMetricValue::Histogram {
                                    buckets,
                                    sum,
                                    count,
                                } => application_impl::MetricValue::Histogram {
                                    bounds: LATENCY.histogram_bounds,
                                    buckets,
                                    sum,
                                    count,
                                },
                            },
                        })
                        .collect(),
                )
            }

            /// 业务作用：读取 nafana 全局 registry 当前快照并追加 Prometheus exposition。
            ///
            /// 参数说明：
            /// - `output`: 接收旧源文本的缓冲区。
            ///
            /// 返回：无；该入口仅保留给显式选择文本旧源模式的兼容调用方。
            fn render_prometheus(&self, output: &mut String) {
                output.push_str(&super::render_metrics());
            }
        }

        /// 业务作用：返回 nafana 兼容源,供 `Application::register_metrics_source` 并入统一 hub。
        ///
        /// 参数说明: 无。
        ///
        /// 返回：无状态源；每次快照读取 nafana 进程级全局 registry 的当前值。
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

/// ambient 事务上下文与 `#[transactional]` 声明式事务入口。
#[cfg(feature = "tx")]
pub mod tx {
    pub use natx_macro::transactional;
    pub use tx_impl::*;
}

/// 消息 Inbox：同一 MySQL 事务内的消费去重标记。
#[cfg(feature = "inbox")]
pub mod inbox {
    pub use inbox_core_impl::InboxClaim;
    pub use inbox_mysql_impl::{InboxProcess, InboxStoreError, InboxTransactionError, MySqlInbox};
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

/// provider-neutral 有界对象存储与 S3-compatible adapter。
///
/// 当前合同只覆盖有界单对象缓冲、path-style SigV4、`CreateOnly` 条件写、幂等删除和默认
/// SHA-256 metadata 复核；不提供 multipart、流式/range/list、STS 刷新或对象版本治理。
/// adapter 由业务持有，不设 Application 组件；`full` 只负责开放构造入口，不替业务推断 bucket、
/// credential 或数据保留策略。
#[cfg(feature = "object-store")]
pub mod object {
    pub use object_impl::*;

    /// 把 adapter 的累计观测事实并入 Application 统一指标目录的兼容源。
    ///
    /// 对象存储没有独立后台所有权，也不设 napp 组件；业务在 UserHook 用
    /// `app.register_metrics_source(nasa::object::metrics::metrics_source(store))?` 一行接入，
    /// 多 adapter 进程改用 `metrics_source_many` 登记一个聚合源。Prometheus 文本端点与 OTLP
    /// 指标导出共用同一份进程级快照，且不把 bucket 或 endpoint 引入 label。
    #[cfg(feature = "application")]
    pub mod metrics {
        use std::collections::BTreeMap;
        use std::sync::Arc;

        use application_impl::{
            LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
        };

        use super::{
            ObjectDurationSample, ObjectOperation, ObjectRequestCount, ObjectStoreSnapshot,
            S3ObjectStore, OBJECT_DURATION_BOUNDS,
        };

        /// 对象存储请求结局计数；label 取值域由 adapter 的封闭枚举决定，不随 bucket 或 key 扩张。
        static REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
            name: "naobject_requests_total",
            help: "对象存储操作按封闭结局分类的累计请求数。",
            unit: "",
            kind: MetricKind::Counter,
            label_names: &["operation", "outcome"],
            histogram_bounds: &[],
        };
        /// 成功传输字节数；只统计确认成功的载荷，失败与元数据操作不计入。
        static BYTES_TOTAL: MetricDescriptor = MetricDescriptor {
            name: "naobject_transferred_bytes_total",
            help: "成功上传或下载的对象字节总数。",
            unit: "",
            kind: MetricKind::Counter,
            label_names: &["direction"],
            histogram_bounds: &[],
        };
        /// 完整返回操作的时延分布；边界与 adapter 常量同源，不在导出面另建一份。
        static DURATION_SECONDS: MetricDescriptor = MetricDescriptor {
            name: "naobject_request_duration_seconds",
            help: "对象存储操作从进入 adapter 到返回业务结局的耗时秒数，不含调用方取消；取消需结合 naobject_requests_total 观察。",
            unit: "seconds",
            kind: MetricKind::Histogram,
            label_names: &["operation"],
            histogram_bounds: &OBJECT_DURATION_BOUNDS,
        };

        /// 对象存储全部指标族的静态 descriptor manifest。
        static OBJECT_DESCRIPTORS: [&MetricDescriptor; 3] =
            [&REQUESTS_TOTAL, &BYTES_TOTAL, &DURATION_SECONDS];

        /// 持有同一业务进程内的 adapter 共享所有权、按需聚合其累计事实的兼容源。
        struct ObjectStoreMetricsSource {
            stores: Vec<Arc<S3ObjectStore>>,
        }

        impl LegacyMetricsSource for ObjectStoreMetricsSource {
            /// 业务作用：返回对象存储兼容源拥有的静态指标族目录。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：启动期冲突审计与结构化样本校验共用的全部 descriptor。
            fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
                &OBJECT_DESCRIPTORS
            }

            /// 业务作用：把全部 adapter 的累计快照聚合为统一目录的进程级结构化样本。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：`Some` 表示本源支持结构化出口；即使尚无请求也返回上传、下载零值，
            /// 多 adapter 的同类计数、字节与完整返回时延按饱和加法合并。
            fn snapshot(&self) -> Option<Vec<MetricSample>> {
                let snapshots: Vec<_> = self
                    .stores
                    .iter()
                    .map(|store| store.metrics_snapshot())
                    .collect();
                Some(samples(&aggregate_snapshots(&snapshots)))
            }

            /// 业务作用：保留旧 trait 入口；本源始终提供结构化快照，文本由统一 hub 渲染。
            ///
            /// 参数说明：
            /// - `_output`: 兼容 trait 的文本缓冲区；本源不直接写入。
            ///
            /// 返回：无；两个出口共用同一份 `snapshot()` 结果。
            fn render_prometheus(&self, _output: &mut String) {}
        }

        /// 业务作用：把 adapter 快照展开成统一目录的样本序列。
        ///
        /// 参数说明：
        /// - `snapshot`: adapter 一次读取得到的累计事实。
        ///
        /// 返回：只含有观测的组合；直方图桶为非累积形态，桶总和恒等于 count。
        fn samples(snapshot: &ObjectStoreSnapshot) -> Vec<MetricSample> {
            let mut out = Vec::new();
            for count in &snapshot.requests {
                out.push(MetricSample {
                    name: REQUESTS_TOTAL.name,
                    labels: vec![
                        ("operation", count.operation.label().to_owned()),
                        ("outcome", count.outcome.label().to_owned()),
                    ],
                    value: MetricValue::Counter(count.requests),
                });
            }
            for (direction, value) in [
                ("upload", snapshot.uploaded_bytes),
                ("download", snapshot.downloaded_bytes),
            ] {
                out.push(MetricSample {
                    name: BYTES_TOTAL.name,
                    labels: vec![("direction", direction.to_owned())],
                    value: MetricValue::Counter(value),
                });
            }
            for duration in &snapshot.durations {
                out.push(MetricSample {
                    name: DURATION_SECONDS.name,
                    labels: vec![("operation", duration.operation.label().to_owned())],
                    value: MetricValue::Histogram {
                        bounds: &OBJECT_DURATION_BOUNDS,
                        buckets: duration.buckets.clone(),
                        sum: duration.sum_seconds,
                        count: duration.count,
                    },
                });
            }
            out
        }

        /// 业务作用：把多个 adapter 快照合并为一个进程级事实，维持每个指标族只有一个 owner。
        ///
        /// 参数说明：
        /// - `snapshots`: 同一抓取轮次依次读取的 adapter 累计快照。
        ///
        /// 返回：相同操作与结局按饱和加法求和，直方图逐桶合并并从桶总和派生 count，字节数同样求和。
        fn aggregate_snapshots(snapshots: &[ObjectStoreSnapshot]) -> ObjectStoreSnapshot {
            let mut requests = BTreeMap::new();
            let mut durations: BTreeMap<ObjectOperation, (Vec<u64>, f64)> = BTreeMap::new();
            let mut uploaded_bytes = 0_u64;
            let mut downloaded_bytes = 0_u64;

            for snapshot in snapshots {
                for count in &snapshot.requests {
                    let total = requests
                        .entry((count.operation, count.outcome))
                        .or_insert(0_u64);
                    *total = total.saturating_add(count.requests);
                }
                for duration in &snapshot.durations {
                    let (buckets, sum_seconds) = durations
                        .entry(duration.operation)
                        .or_insert_with(|| (vec![0; OBJECT_DURATION_BOUNDS.len() + 1], 0.0));
                    for (target, value) in buckets.iter_mut().zip(&duration.buckets) {
                        *target = target.saturating_add(*value);
                    }
                    *sum_seconds += duration.sum_seconds;
                }
                uploaded_bytes = uploaded_bytes.saturating_add(snapshot.uploaded_bytes);
                downloaded_bytes = downloaded_bytes.saturating_add(snapshot.downloaded_bytes);
            }

            ObjectStoreSnapshot {
                requests: requests
                    .into_iter()
                    .map(|((operation, outcome), requests)| ObjectRequestCount {
                        operation,
                        outcome,
                        requests,
                    })
                    .collect(),
                durations: durations
                    .into_iter()
                    .map(|(operation, (buckets, sum_seconds))| ObjectDurationSample {
                        operation,
                        count: buckets.iter().copied().fold(0_u64, u64::saturating_add),
                        buckets,
                        sum_seconds,
                    })
                    .collect(),
                uploaded_bytes,
                downloaded_bytes,
            }
        }

        /// 业务作用：返回可直接交给 `Application::register_metrics_source` 的对象存储兼容源。
        ///
        /// 参数说明：
        /// - `store`: 业务自己构造并持有的 adapter；指标源共享其所有权，不改变其生命周期。
        ///
        /// 返回：每次抓取读取该 adapter 当前累计值的无状态源。
        pub fn metrics_source(store: Arc<S3ObjectStore>) -> Arc<dyn LegacyMetricsSource> {
            metrics_source_many([store])
        }

        /// 业务作用：为同一进程的多个对象存储 adapter 返回单一聚合指标源。
        ///
        /// 统一目录要求每个 family 只有一个 owner，因此多 bucket、多 endpoint 或多凭据域不能分别
        /// 登记同名源；聚合源保持 label 低基数，同时让全部 adapter 进入文本与 OTLP 出口。
        ///
        /// 参数说明：
        /// - `stores`: 由业务构造并持有的 adapter 集合；源共享所有权，不改变生命周期。
        ///
        /// 返回：每次抓取按操作和封闭结局聚合全部 adapter 当前累计值的无状态源。
        pub fn metrics_source_many(
            stores: impl IntoIterator<Item = Arc<S3ObjectStore>>,
        ) -> Arc<dyn LegacyMetricsSource> {
            Arc::new(ObjectStoreMetricsSource {
                stores: stores.into_iter().collect(),
            })
        }
    }
}

/// Saga 编排：纯逻辑合同（身份派生/封闭状态机/补偿计划），开启
/// `saga-runtime` 后再并入 Orchestrator、参与方 adapter 与 `#[saga]` 宏。
///
/// `saga-grpc` 会同时打开稳定 `grpc` 门面；纯出站调用无需声明 Application 的 `"grpc"` 组件，
/// 入站计划则必须声明它以取得唯一受管 listener。`full` 会编入运行时以及 Kafka、gRPC adapter；
/// Redis Streams 替代通道仍需显式 feature。
/// 业务必须声明 Application 的 `"saga"` 组件并提交流程定义、参与方信任关系和
/// 发布端。DB 与 Outbox 由 Saga 声明隐式纳入，未装配计划时启动会 fail-closed。
#[cfg(feature = "saga")]
pub mod saga {
    #[cfg(feature = "saga-runtime")]
    pub use nasaga_macro::saga;
    pub use saga_core_impl::*;
    #[cfg(feature = "saga-runtime")]
    pub use saga_runtime_impl::*;
}

/// 稳定 gRPC codegen 门面、generated service registry、独立/Application listener 与有界排空。
///
/// 业务实现 generated trait 后只登记 server；health、reflection、消息/stream/RPC 预算、listener
/// readiness 与 shutdown owner 由框架统一装配。独立进程通过 `ServerPlan` 使用同一运行合同。
#[cfg(feature = "grpc")]
pub mod grpc {
    pub use grpc_impl::{
        async_trait, health, include_proto, propagate_deadline_from, reflection, Certificate,
        Channel, ClientTlsConfig, Code, Deadline, DeadlineSource, Endpoint, GrpcMessageLimits,
        GrpcMethodDescriptor, GrpcMethodPolicy, GrpcMethodType, GrpcRpcMethodSnapshot,
        GrpcRpcOutcome, GrpcRpcRejectionReason, GrpcServerConfig, GrpcServerError,
        GrpcServerHandle, GrpcServerObserver, GrpcServerSnapshot, GrpcServerState, GrpcTlsIdentity,
        Identity, ManagedGrpcService, ManagedService, PeerIdentity, Request, Response, ServerPlan,
        Status, Streaming,
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

/// 声明式 Mapper：trait + `#[Mapper]` / `#[Query]` / `#[Insert]` 等属性宏。
#[cfg(feature = "mapper")]
pub mod mapper {
    pub use mapper_impl::*;
    pub use namapper_macro::{
        Delete, Execute, Insert, Mapper, MapperEnum, MapperOrderField, Query, StreamQuery, Update,
    };
}

/// 本地有界分区执行器：空闲 worker 可在不移动队列数据的前提下保序接管其它分区的 lane；
/// 同 key 严格按提交顺序串行，不同 key 按分区并发；提供非阻塞或等待型背压、任务 panic
/// 隔离、健康观测与显式异步停机。
///
/// 该模块与 `nasa::redis::partition` 的 `PollCoordinator` 含义不同：这里管理单进程任务执行，
/// Redis 模块管理分布式分区消费。
///
/// ```
/// use nasa::partition::PartitionExecutor;
/// ```
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
///
/// `kafka-schema-registry` 额外开放 Confluent envelope、schema ID 白名单、有界正负缓存和显式
/// 兼容性/注册控制面；Registry client 由业务持有，不参与 Kafka 组件的 Ready 或 shutdown。
#[cfg(feature = "kafka")]
pub mod kafka {
    pub use kafka_impl::*;

    /// 把 Schema Registry client 的查询结局并入 Application 统一指标目录的兼容源。
    ///
    /// Schema Registry 是 Kafka codec 子能力，没有独立后台所有权，也不设 napp 组件；
    /// 业务在 UserHook 用 `app.register_metrics_source(...)` 一行接入；多 Registry client 使用
    /// `metrics_source_many` 登记一个聚合源。Prometheus 文本端点与 OTLP 指标导出共用同一份
    /// 进程级快照，且不把 endpoint 或 subject 引入 label。
    #[cfg(all(feature = "application", feature = "kafka-schema-registry"))]
    pub mod schema_metrics {
        use std::collections::BTreeMap;
        use std::sync::Arc;

        use application_impl::{
            LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
        };

        use super::{
            ConfluentSchemaRegistry, SchemaControlCount, SchemaLookupCount, SchemaRegistrySnapshot,
        };

        /// 按封闭结局分类的 schema 查询计数；label 取值域固定，不随 schema ID 或 subject 扩张。
        static LOOKUPS_TOTAL: MetricDescriptor = MetricDescriptor {
            name: "nafka_schema_lookups_total",
            help: "Schema Registry 按缓存、拉取与取消结局分类的累计查询次数。",
            unit: "",
            kind: MetricKind::Counter,
            label_names: &["outcome"],
            histogram_bounds: &[],
        };
        /// 当前缓存占用条目数。
        static CACHE_ENTRIES: MetricDescriptor = MetricDescriptor {
            name: "nafka_schema_cache_entries",
            help: "Schema Registry 正负缓存合计占用的条目数。",
            unit: "",
            kind: MetricKind::Gauge,
            label_names: &[],
            histogram_bounds: &[],
        };
        /// 已登记 client 的缓存条目上限总和；与占用总数一起判断是否已被容量而非 TTL 驱逐。
        static CACHE_CAPACITY: MetricDescriptor = MetricDescriptor {
            name: "nafka_schema_cache_capacity",
            help: "已登记 Schema Registry client 的缓存配置条目上限总和。",
            unit: "",
            kind: MetricKind::Gauge,
            label_names: &[],
            histogram_bounds: &[],
        };
        /// 兼容性检查与注册请求计数；数据面缓存命中率不会被控制面流量稀释。
        static CONTROL_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
            name: "nafka_schema_control_requests_total",
            help: "Schema Registry 兼容性检查与注册按封闭结局分类的累计请求数。",
            unit: "",
            kind: MetricKind::Counter,
            label_names: &["operation", "outcome"],
            histogram_bounds: &[],
        };

        /// Schema Registry 全部指标族的静态 descriptor manifest。
        static SCHEMA_DESCRIPTORS: [&MetricDescriptor; 4] = [
            &LOOKUPS_TOTAL,
            &CACHE_ENTRIES,
            &CACHE_CAPACITY,
            &CONTROL_REQUESTS_TOTAL,
        ];

        /// 持有同一业务进程内的 client 共享所有权、按需聚合其累计事实的兼容源。
        struct SchemaRegistryMetricsSource {
            clients: Vec<Arc<ConfluentSchemaRegistry>>,
        }

        impl LegacyMetricsSource for SchemaRegistryMetricsSource {
            /// 业务作用：返回 Schema Registry 兼容源拥有的静态指标族目录。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：启动期冲突审计与结构化样本校验共用的全部 descriptor。
            fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
                &SCHEMA_DESCRIPTORS
            }

            /// 业务作用：把全部 client 的累计快照聚合为统一目录的进程级结构化样本。
            ///
            /// 参数说明: 无。
            ///
            /// 返回：`Some` 表示本源支持结构化出口；尚无查询时仍导出各 client 缓存容量与占用之和。
            fn snapshot(&self) -> Option<Vec<MetricSample>> {
                let snapshots: Vec<_> = self
                    .clients
                    .iter()
                    .map(|client| client.metrics_snapshot())
                    .collect();
                Some(samples(&aggregate_snapshots(&snapshots)))
            }

            /// 业务作用：保留旧 trait 入口；本源始终提供结构化快照，文本由统一 hub 渲染。
            ///
            /// 参数说明：
            /// - `_output`: 兼容 trait 的文本缓冲区；本源不直接写入。
            ///
            /// 返回：无；两个出口共用同一份 `snapshot()` 结果。
            fn render_prometheus(&self, _output: &mut String) {}
        }

        /// 业务作用：把 client 快照展开成统一目录的样本序列。
        ///
        /// 参数说明：
        /// - `snapshot`: client 一次读取得到的累计事实。
        ///
        /// 返回：非零结局的数据面与控制面计数，以及始终导出的缓存占用与容量。
        fn samples(snapshot: &SchemaRegistrySnapshot) -> Vec<MetricSample> {
            let mut out = Vec::new();
            for count in &snapshot.lookups {
                out.push(MetricSample {
                    name: LOOKUPS_TOTAL.name,
                    labels: vec![("outcome", count.outcome.label().to_owned())],
                    value: MetricValue::Counter(count.lookups),
                });
            }
            out.push(MetricSample {
                name: CACHE_ENTRIES.name,
                labels: Vec::new(),
                value: MetricValue::Gauge(snapshot.cached_entries as f64),
            });
            out.push(MetricSample {
                name: CACHE_CAPACITY.name,
                labels: Vec::new(),
                value: MetricValue::Gauge(snapshot.cache_capacity as f64),
            });
            for count in &snapshot.control_requests {
                out.push(MetricSample {
                    name: CONTROL_REQUESTS_TOTAL.name,
                    labels: vec![
                        ("operation", count.operation.label().to_owned()),
                        ("outcome", count.outcome.label().to_owned()),
                    ],
                    value: MetricValue::Counter(count.requests),
                });
            }
            out
        }

        /// 业务作用：把多个 Registry client 快照合并为一个进程级事实，维持每个指标族只有一个 owner。
        ///
        /// 参数说明：
        /// - `snapshots`: 同一抓取轮次依次读取的 client 累计快照。
        ///
        /// 返回：相同操作与结局按饱和加法求和，缓存占用与容量按 client 求和。
        fn aggregate_snapshots(snapshots: &[SchemaRegistrySnapshot]) -> SchemaRegistrySnapshot {
            let mut lookups = BTreeMap::new();
            let mut control_requests = BTreeMap::new();
            let mut cached_entries = 0_u64;
            let mut cache_capacity = 0_u64;

            for snapshot in snapshots {
                for count in &snapshot.lookups {
                    let total = lookups.entry(count.outcome).or_insert(0_u64);
                    *total = total.saturating_add(count.lookups);
                }
                for count in &snapshot.control_requests {
                    let total = control_requests
                        .entry((count.operation, count.outcome))
                        .or_insert(0_u64);
                    *total = total.saturating_add(count.requests);
                }
                cached_entries = cached_entries.saturating_add(snapshot.cached_entries);
                cache_capacity = cache_capacity.saturating_add(snapshot.cache_capacity);
            }

            SchemaRegistrySnapshot {
                lookups: lookups
                    .into_iter()
                    .map(|(outcome, lookups)| SchemaLookupCount { outcome, lookups })
                    .collect(),
                cached_entries,
                cache_capacity,
                control_requests: control_requests
                    .into_iter()
                    .map(|((operation, outcome), requests)| SchemaControlCount {
                        operation,
                        outcome,
                        requests,
                    })
                    .collect(),
            }
        }

        /// 业务作用：返回可直接交给 `Application::register_metrics_source` 的 Registry 兼容源。
        ///
        /// 参数说明：
        /// - `client`: 业务自己构造并持有的 client；指标源共享其所有权，不改变其生命周期。
        ///
        /// 返回：每次抓取读取该 client 当前累计值的无状态源。
        pub fn metrics_source(
            client: Arc<ConfluentSchemaRegistry>,
        ) -> Arc<dyn LegacyMetricsSource> {
            metrics_source_many([client])
        }

        /// 业务作用：为同一进程的多个 Schema Registry client 返回单一聚合指标源。
        ///
        /// 统一目录要求每个 family 只有一个 owner，因此多集群 client 不能分别登记同名源；聚合源
        /// 保持 label 低基数，同时让全部数据面与控制面请求进入文本和 OTLP 出口。
        ///
        /// 参数说明：
        /// - `clients`: 由业务构造并持有的 client 集合；源共享所有权，不改变生命周期。
        ///
        /// 返回：每次抓取按封闭操作与结局聚合全部 client 当前累计值的无状态源。
        pub fn metrics_source_many(
            clients: impl IntoIterator<Item = Arc<ConfluentSchemaRegistry>>,
        ) -> Arc<dyn LegacyMetricsSource> {
            Arc::new(SchemaRegistryMetricsSource {
                clients: clients.into_iter().collect(),
            })
        }
    }
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

/// 日期时间工具(crate = `date`,基于 chrono)。
/// `nasa = { features = ["date"] }` → `use nasa::date::{format, parse, add_days, today, ...};`
/// i64 epoch ms 规范 + GMT+8 默认 + 原实现 SimpleDateFormat 风格 pattern(`"yyyy-MM-dd HH:mm:ss"`)。
#[cfg(feature = "date")]
pub mod date {
    pub use date_impl::*;
}

/// 日志(crate = `nalog`,基于 tracing)。
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

/// 通用响应壳 `BaseResponse`(crate = `nabase`)。
/// `nasa = { features = ["base"] }` → `use nasa::base::BaseResponse;` → `BaseResponse::ok(data)` / `::err(code, msg)`。
/// 字段 `code`(默认 200)/ `msg`(提示信息)/ `aes`(需加密时的 AES 密钥)/ `data`,`None` 序列化省略。
/// 另含 strings/env/size/id 纯工具;date/numeric/crypto/image 继续走 `nasa::{date,numeric,crypto,image}` 顶层入口。
#[cfg(feature = "base")]
pub mod base {
    pub use base_impl::*;
}

/// 通用分层 YAML 配置加载器(对照;crate = `yml`)。
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
    /// 多配置(对照):`fetch_many`/`watch_many_channel`(按序拉一组 → `ConfigBundle` + `MultiWatchGuard`)。
    pub mod nacos {
        pub use nacos_impl::{
            ConfigBundle, ConfigDocument, ConfigRef, MultiWatchGuard, NacosConfigClient,
            NacosProps, WatchGuard,
        };
    }
    // 配置引导胶水(yml × nacos)统一走 nasa::yml::nacos;此处不再暴露 nasa::config::boot。
}
