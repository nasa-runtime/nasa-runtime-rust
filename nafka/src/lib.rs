//! 多数据源 Kafka 事件运行时公共入口。
//!
//! 本 crate 负责配置校验、发布 lane、消费者注册、确认语义、控制面与少拷贝借用入口。
//! 底层客户端类型全部限制在 `rd` 模块，业务代码应经统一门面使用这里的自有类型。
//! 命名 KafkaProxy 在同一进程连接多个集群，每个 producer lane 独立持有原生客户端与交付额度。
//! 普通消费的输入责任受进程、源、组、分区和 key 的共同上限约束，慢调用真实退出前继续计费；
//! 这些责任额度不等同于整个进程 RSS 上限，跨分区或跨进程的外部副作用仍需业务幂等。
//! DltReplayPlan 限定死信来源、目标和回投次数，原文隔离也必须取得真实目标确认。
//! TransactionPlan 绑定稳定部署身份、完整输入和批准输出，由专属 producer 原子提交组位点；
//! commit 结果未知时保护 owner，同身份恢复后按 broker 已提交位置接续。
//! Replay 固定 LSO 范围并持有独占 checkpoint；Transfer 绑定目标物理身份和分区，
//! 按目标实际确认、持久进度、状态发布的顺序推进。双方停机都保留未完成交付责任，
//! 原始事件 ID 与有界物理路径用于重复识别；单目标 at-least-once 不承诺跨资源原子性。
//!
//! # 运行架构与共同合同
//!
//! 命名源冻结配置，registry 固定路由，普通组的每个 owner 槽独占 consumer；
//! 业务结果通过当前 assignment 代次的 Ack 进入连续安全水位，broker 提交确认后释放输入责任。
//! 发布通道的受理、delivery 与事务 commit 是不同事实；取消 Future 不撤销已提交的原生操作。
//! Ready 使用当前资源证据，两段停机等待真实调用和交付退出，超时不代表已经排干。
//!
//! Java nasa-kafka 与本组件共享事件线路、组名输入域、稳定事务身份和有限任务持久格式。
//! 组名保留合法 UTF-8 原文，不裁剪或归一化；事务计划预检与执行复验均以事务错误分类拒绝非法声明。
//! 跨语言接续要求相同物理资源身份、相容计划及有效持久进度，不承诺数据库与 Kafka 的原子提交。
//!
//! # Schema Registry
//!
//! `schema-registry` feature 额外提供 Confluent wire envelope、批准 ID 门禁、有界正负缓存和
//! 兼容性/注册控制面。Registry client 不参与 consumer group、offset 或 producer 生命周期，也不
//! 启动后台刷新。独立实例经 `nasa::kafka` 显式构造；Application 可按 `schema_registries` 命名计划
//! 绑定凭据、指标与关闭门禁，使用 `Application::schema_registry` 取得受管句柄，无需 Kafka 消费组件。
//! Registry 支持独立私有 CA 与 mTLS，HTTP、查询等待与正文均有上限；取消业务等待不丢弃实际请求责任。

#![deny(unsafe_code)]
#![allow(async_fn_in_trait)]
#![deny(missing_docs)]

mod access_scope;
pub use access_scope::KafkaAccessScope;
mod admin;
mod broadcast;
mod checkpoint;
mod close_report;
mod config;
mod consumer;
mod control;
mod dlt_replay;
mod error;
mod health;
mod metric_catalog;
mod metrics;
mod runtime_metrics;
mod trace;
pub use close_report::{
    CloseConsumers, CloseLane, CloseReport, CloseResourceState, CloseWaitOutcome,
};
pub use metric_catalog::{MetricDefinition, MetricKind, METRIC_CATALOG};
pub use trace::{extract_trace_context, KafkaTraceLinks, TraceInvalid};
mod migration;
mod offset_observation;
mod producer;
mod rd;
mod replay;
#[cfg(feature = "schema-registry-metrics")]
pub mod schema_metrics;
#[cfg(feature = "schema-registry")]
mod schema_registry;
mod transaction;
mod transfer;
mod types;
mod wire;

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

pub use admin::{KafkaAdmin, TopicCreation, TopicDescription, TopicSpec};
pub use broadcast::broadcast_group;
#[cfg(unix)]
pub use checkpoint::FileCheckpointStore;
#[cfg(feature = "mysql-checkpoint")]
pub use checkpoint::MySqlCheckpointStore;
pub use checkpoint::{
    CheckpointError, CheckpointLease, CheckpointResult, CheckpointStore, ReplayCheckpoint,
    ReplayPosition,
};
pub use offset_observation::{
    ObservedOffset, OffsetIsolation, OffsetMissing, OffsetObservation, PartitionOffsets,
};
mod capabilities;
pub use capabilities::KafkaCapabilities;
pub use config::{
    AdminConfig, BehaviorConfig, ConsumerConfig, InvalidRecordPolicy, KafkaConfig, ProducerConfig,
    ProducerLaneOverride, SecurityConfig, TombstonePolicy, UnmatchedPolicy, KAFKA_CONFIG_FIELDS,
};
pub use consumer::{
    AckBatchExt, AckMode, AckToken, BatchConsumer, BorrowedKafkaRecord, ConsumeCtx,
    ConsumerRegistryBuilder, ConsumerRuntime, ExecutionMode, GroupSpec, InvalidRecordReason,
    KafkaHeaderRef, KafkaHeadersRef, KafkaRecord, PassthroughConsumer, PassthroughDeadLetterReason,
    PassthroughDisposition, PassthroughFailure, PassthroughFailureAction, PassthroughFailureReason,
    PassthroughFatalReason, PassthroughHaltReason, PassthroughRetryReason, PassthroughSkipReason,
    SingleConsumer,
};
pub use control::{ControlOutcome, ControlReport, PartitionControl};
pub use dlt_replay::{
    DltEvidence, DltInvalidPolicy, DltOrigin, DltQuarantineTrace, DltReplayError, DltReplayHandle,
    DltReplayPlan, DltReplayResult, DltReplayStatus, DltReplayTrace,
};
pub use error::{NafkaError, ProducerQueue, Result};
pub use health::{
    GroupHealth, GroupReadySnapshot, GroupState, HaltReason, PauseReason, ReadyReason,
    ReadyRequirement, ReadyStatus,
};
pub use metrics::{MetricLabels, MetricsExportStats, MetricsOptions, MetricsSink};
pub use migration::{
    inspect_legacy_config, MigrationDefaultChange, MigrationFinding, MigrationReport,
    MigrationRoute,
};
pub use producer::{
    BatchPublishError, ProducerLane, ProducerLaneStats, PublishBuilder, PublishItem,
    RawPublishBuilder, TombstoneBuilder,
};
pub use replay::{
    MissingTimestamp, ReplayError, ReplayHandle, ReplayIdentityProvider, ReplayLimits,
    ReplayObservation, ReplayPartition, ReplayPlan, ReplayProcessor, ReplayRecord, ReplayResult,
    ReplayStart, ReplayState, ReplayStatus,
};
#[cfg(feature = "schema-registry")]
pub use schema_registry::*;
pub use transaction::{
    TransactionContext, TransactionError, TransactionHandle, TransactionIdentity,
    TransactionLimits, TransactionOutput, TransactionPlan, TransactionProcessor, TransactionResult,
    TransactionState, TransactionStatus,
};
pub use transfer::{
    TransferError, TransferHandle, TransferPayloadPolicy, TransferPlan, TransferRegistries,
    TransferResource, TransferResult, TransferRoute, TransferSchemaKind, TransferSchemaMapping,
    TransferStatus, TransferTarget, TransferTrace,
};
pub use types::{
    AssignmentHandle, Delivery, KafkaHeader, KafkaHeaders, KafkaPartitionLag, StartOffset, Tp,
};
pub use wire::{
    dead_letter_headers, mode_from_wire_name, mode_wire_name, DecodePayload, EncodePayload,
    NullablePayload, PayloadCodec, Proto, ProtoMode, DEFAULT_EVENT, HEADER_DELIVERY_ATTEMPT,
    HEADER_DLT_DELIVERY_ATTEMPTS, HEADER_DLT_ORIGIN_CLUSTER, HEADER_DLT_ORIGIN_GROUP,
    HEADER_DLT_ORIGIN_OFFSET, HEADER_DLT_ORIGIN_PARTITION, HEADER_DLT_ORIGIN_SOURCE,
    HEADER_DLT_ORIGIN_TOPIC, HEADER_DLT_REASON, HEADER_EVENT, HEADER_PASSTHROUGH,
    HEADER_PAYLOAD_CODEC, HEADER_PAYLOAD_MODE, HEADER_TRACEPARENT, PAYLOAD_CODEC_PROTOCOL_BYTES,
};

#[cfg(feature = "macros")]
pub use nafka_macro::kafka_consumer;

/// 常用消费者实现所需的精简导入集合。
pub mod prelude {
    pub use crate::{
        AckBatchExt, AckMode, BatchConsumer, ConsumeCtx, DecodePayload, ExecutionMode, GroupSpec,
        KafkaProxy, KafkaRecord, NafkaError, Result, SingleConsumer,
    };

    #[cfg(feature = "macros")]
    pub use crate::kafka_consumer;
}

/// 静态属性宏收集项；只有显式调用 `with_collected` 才会注册和启动。
#[doc(hidden)]
#[linkme::distributed_slice]
pub static COLLECTED_CONSUMERS: [__private::CollectedConsumer];

/// 属性宏与运行时锁步使用的隐藏桥接层。
#[doc(hidden)]
pub mod __private {
    pub use linkme;

    pub use crate::consumer::erased::{
        erase_batch, erase_single, CollectedConsumer, ConsumerMeta, ErasedConsumer,
        InvocationOutcome,
    };
}

/// 运行时总生命周期。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Lifecycle {
    /// 已完成本地配置与句柄构造，尚未启动消费者。
    Created = 0,
    /// 消费者注册表已经冻结并运行。
    Running = 1,
    /// 正在停止消费者并排空生产者。
    ShuttingDown = 2,
    /// 所有受管资源已经停止。
    Stopped = 3,
}

/// 共享运行时内部状态。
pub(crate) struct KafkaProxyInner {
    /// 冻结后的组件配置。
    pub(crate) config: KafkaConfig,
    /// 源内全部普通消费输入的共同边界，实际责任退出前不归还进程额度。
    pub(crate) input_capacity: Arc<consumer::source_capacity::SourceCapacity>,
    /// 源级有界异步指标出口，用户代码只在专属线程执行。
    pub(crate) metrics: Arc<metrics::AsyncMetrics>,
    /// 应用持有的遥测只写入口；不拥有 exporter 生命周期。
    pub(crate) span_recorder: Option<natelemetry::SpanRecorder>,
    /// connect 时捕获的异步运行时句柄。
    #[allow(dead_code)]
    pub(crate) runtime: tokio::runtime::Handle,
    /// connect 时冻结的共享 producer lane 句柄表。
    pub(crate) producer_lanes: OnceLock<std::collections::BTreeMap<String, ProducerLane>>,
    /// 首次管理操作时构造的共享管理客户端；shutdown 转交给唯一关闭任务并等待真实退出。
    pub(crate) admin: std::sync::RwLock<Option<Arc<rd::admin::AdminHandle>>>,
    /// 已关闭准入的 Admin 原生责任；关闭等待被取消时仍由同一个线程继续收口。
    pub(crate) admin_close: std::sync::Mutex<Option<Arc<rd::admin::AdminClose>>>,
    /// Admin 的永久准入门；在最终关闭共享客户端的同一写锁内关闭。
    pub(crate) managed_admin_closed: AtomicBool,
    /// 独立回放任务的准入、真实寿命和固定线程预算。
    pub(crate) replays: replay::runtime::ReplayManager,
    /// 以当前源为目标的转发依赖；实际来源退出前不能关闭目标 producer。
    pub(crate) transfers: transfer::dependencies::TransferRegistry,
    /// 专属事务 owner 的稳定身份与实际资源目录。
    pub(crate) transactions: transaction::runtime::Manager,
    /// 全运行时共享的有界死信 dispatcher。
    pub(crate) dlt_dispatcher: OnceLock<consumer::dlt::DltDispatcher>,
    /// 已冻结并启动的 group owner 表。
    pub(crate) groups: std::sync::RwLock<
        std::collections::BTreeMap<String, Arc<consumer::supervisor::GroupHandle>>,
    >,
    /// consumer 启动握手与 shutdown 初始快照的异步线性化门。
    pub(crate) consumer_lifecycle_gate: tokio::sync::Mutex<()>,
    /// 本运行时持有的静态收集激活租约；shutdown 成功进入 Stopped 后释放。
    pub(crate) collected_lease: std::sync::Mutex<Option<String>>,
    /// 业务可见 `KafkaProxy` 句柄数；owner 持有的内部 Arc 不计入。
    external_handles: AtomicUsize,
    /// 永久消费停止意图，独立于最终发布资源关闭阶段。
    consumers_stop_requested: AtomicBool,
    /// 清除 owner 表后保留的纯计数，不延长原生资源或原文寿命。
    stopped_consumers: std::sync::Mutex<CloseConsumers>,
    /// 总生命周期原子状态。
    lifecycle: AtomicU8,
}

impl KafkaProxyInner {
    /// 业务作用：发出一个稳定 counter，调用点不得传入高基数或敏感字段。
    ///
    /// 参数说明：name 为固定名，delta 为非负增量，labels 为受控身份。
    /// 返回：仅尝试有界入队，指标失败不能改变 ACK、DLT 或提交结局。
    pub(crate) fn counter(&self, name: &'static str, delta: u64, labels: MetricLabels<'_>) {
        self.emit(|| self.metrics.counter(name, delta, labels), name);
    }

    /// 业务作用：发出一个稳定 gauge，调用点不得传入高基数或敏感字段。
    /// 参数说明：name 为固定名，value 为状态，labels 为受控身份。返回：非等待地入队或丢弃。
    pub(crate) fn gauge(&self, name: &'static str, value: i64, labels: MetricLabels<'_>) {
        self.emit(|| self.metrics.gauge(name, value, labels), name);
    }

    /// 业务作用：隔离指标准入本身的异常，用户后端由出口线程独立隔离。
    ///
    /// # 参数
    ///
    /// - `emit`: 有界队列准入。
    /// - `name`: 出问题时用于定位的指标名（框架固定常量，非高基数）。
    fn emit(&self, emit: impl FnOnce(), name: &'static str) {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(emit)).is_err() {
            tracing::error!(metric = name, "指标准入异常，已隔离并忽略本次上报");
        }
    }

    /// 业务作用：读取当前生命周期。
    pub(crate) fn lifecycle(&self) -> Lifecycle {
        match self.lifecycle.load(Ordering::Acquire) {
            0 => Lifecycle::Created,
            1 => Lifecycle::Running,
            2 => Lifecycle::ShuttingDown,
            _ => Lifecycle::Stopped,
        }
    }

    /// 业务作用：尝试从预期状态切换到新状态。
    ///
    /// # 参数
    ///
    /// - `from`: 调用方要求的当前状态。
    /// - `to`: 成功后写入的目标状态。
    ///
    /// # 错误
    ///
    /// 当前状态与 `from` 不一致时返回生命周期错误。
    pub(crate) fn transition(&self, from: Lifecycle, to: Lifecycle) -> Result<()> {
        self.lifecycle
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|actual| {
                NafkaError::Lifecycle(format!(
                    "状态切换失败: expected={from:?}, actual={}, target={to:?}",
                    lifecycle_name(actual)
                ))
            })
    }

    /// 业务作用：判断是否仍允许外部发布入口取得资格。
    ///
    /// # 错误
    ///
    /// 运行时进入关闭阶段后返回生命周期错误。
    pub(crate) fn ensure_publish_open(&self) -> Result<()> {
        match self.lifecycle() {
            Lifecycle::Created | Lifecycle::Running => Ok(()),
            Lifecycle::ShuttingDown | Lifecycle::Stopped => {
                Err(NafkaError::Lifecycle("运行时已停止接收新发布".into()))
            }
        }
    }
}

impl Drop for KafkaProxyInner {
    /// 业务作用：外部句柄已全部离开且 owner 线程真正退出后兜底归还 collected lease。
    ///
    /// 正常 shutdown 会提前取走租约；这里覆盖调用方漏掉显式 shutdown 的 best-effort
    /// 路径，释放时机仍晚于所有 owner 持有的内部 `Arc<KafkaProxyInner>`。
    fn drop(&mut self) {
        let lease = self
            .collected_lease
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(client) = lease {
            consumer::registry::release_activation(&client);
        }
    }
}

/// 业务作用：将生命周期字节转换为稳定诊断文本。
///
/// # 参数
///
/// - `value`: 原子状态中的整数值。
fn lifecycle_name(value: u8) -> &'static str {
    match value {
        0 => "Created",
        1 => "Running",
        2 => "ShuttingDown",
        3 => "Stopped",
        _ => "Unknown",
    }
}

/// Kafka 运行时共享句柄。
///
/// Clone 仅复制内部 `Arc`，不会创建新的底层连接或消费者组。
pub struct KafkaProxy {
    /// 共享的配置、生命周期和受管资源。
    pub(crate) inner: Arc<KafkaProxyInner>,
}

impl Clone for KafkaProxy {
    /// 业务作用：克隆业务句柄并单独计入 Drop 收尾判定。
    fn clone(&self) -> Self {
        self.inner.external_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for KafkaProxy {
    /// 业务作用：最后一个业务句柄离开时发送非阻塞 Stop；不在 Drop 内等待或 flush。
    fn drop(&mut self) {
        if self.inner.external_handles.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        self.inner.transfers.stop();
        self.inner.replays.stop();
        self.inner.transactions.stop();

        let (state, groups) = {
            let managed = self
                .inner
                .groups
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = self.inner.lifecycle();
            if matches!(state, Lifecycle::Created | Lifecycle::Running) {
                let _ = self.inner.transition(state, Lifecycle::ShuttingDown);
            }
            let groups = managed
                .iter()
                .map(|(name, handle)| (name.clone(), Arc::clone(handle)))
                .collect::<Vec<_>>();
            (state, groups)
        };
        if state == Lifecycle::Stopped {
            return;
        }

        if let Some(lanes) = self.inner.producer_lanes.get() {
            for lane in lanes.values() {
                lane.begin_draining();
            }
        }
        let failed: Vec<String> = groups
            .into_iter()
            .filter_map(|(name, handle)| (!handle.request_stop_best_effort()).then_some(name))
            .collect();
        tracing::warn!(
            client = self.inner.config.client_name,
            groups_with_full_stop_queue = ?failed,
            "KafkaProxy 最后一个外部句柄已 Drop；仅发送 best-effort stop，未执行 producer flush"
        );
    }
}

impl std::fmt::Debug for KafkaProxy {
    /// 业务作用：仅输出逻辑客户端名与当前生命周期，避免连接信息进入诊断文本。
    /// 参数说明：f 为调用方的格式化缓冲区。
    /// 返回：格式化结果，不输出 broker 地址、连接配置或凭据。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProxy")
            .field("client_name", &self.inner.config.client_name)
            .field("lifecycle", &self.inner.lifecycle())
            .finish()
    }
}

impl KafkaProxy {
    /// 业务作用：校验并冻结源配置，创建各 lane 的原生 producer 与有界运行资源。
    /// 参数说明：config 为完整组件配置，成功后由该源固定持有。
    /// 返回：可登记消费者或发布消息的源；配置、运行时或资源构造失败时返回错误。
    /// 原生后台线程可开始连接 broker，但此处不等待 broker 就绪，也不隐式启动消费者。
    pub fn connect(config: KafkaConfig) -> Result<Self> {
        Self::connect_inner(config, None, MetricsOptions::default(), None)
    }

    /// 业务作用：构造源及各 lane 的原生资源，并安装独立有界线程调用的指标后端。
    /// 参数说明：config 为冻结源配置；metrics 为应用提供的后端，不直接在交付或消费线程执行。
    /// 返回：尚未启动消费者的源；配置、运行时或资源构造失败时返回错误，成功不证明 broker 已就绪。
    pub fn connect_with_metrics(
        config: KafkaConfig,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<Self> {
        Self::connect_with_observability(config, metrics, None)
    }

    /// 业务作用：为源安装具有明确队列和事件字节预算的指标后端。
    /// 参数说明：config 为冻结业务配置，metrics 为用户后端，options 为独立导出预算。
    /// 返回：完成本地构造但尚未消费的源；非法配置、无异步运行时或线程创建失败时拒绝。
    pub fn connect_with_metrics_options(
        config: KafkaConfig,
        metrics: Arc<dyn MetricsSink>,
        options: MetricsOptions,
    ) -> Result<Self> {
        Self::connect_inner(config, Some(metrics), options, None)
    }

    /// 业务作用：校验配置并注入有界异步指标与只写 span 记录器。
    /// 参数说明：config 为冻结业务配置，metrics 为指标后端，span_recorder 为应用持有的只写追踪入口。
    /// 返回：已创建原生 producer 的源；配置或资源构造失败时返回错误。后台可开始连接，但不等待 broker 就绪，指标后端不占用 owner 或交付线程。
    pub fn connect_with_observability(
        config: KafkaConfig,
        metrics: Arc<dyn MetricsSink>,
        span_recorder: Option<natelemetry::SpanRecorder>,
    ) -> Result<Self> {
        Self::connect_inner(
            config,
            Some(metrics),
            MetricsOptions::default(),
            span_recorder,
        )
    }

    /// 业务作用：统一构造受管业务资源与可选有界观测出口。
    /// 参数说明：config 为冻结配置，metrics 为可选后端，options 为导出额度，span_recorder 为只写追踪入口。
    /// 返回：全部本地资源构造完成的代理；任一构造失败时不开放业务路由。
    fn connect_inner(
        config: KafkaConfig,
        metrics: Option<Arc<dyn MetricsSink>>,
        options: MetricsOptions,
        span_recorder: Option<natelemetry::SpanRecorder>,
    ) -> Result<Self> {
        config.validate()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| NafkaError::NoRuntime)?;
        let input_capacity = consumer::source_capacity::SourceCapacity::new(&config.behavior)?;
        let metrics = metrics::AsyncMetrics::new(metrics, config.client_name.clone(), options)?;
        let lane_names = producer::configured_lane_names(&config.producer.lanes);
        let inner = Arc::new(KafkaProxyInner {
            config,
            input_capacity,
            metrics,
            span_recorder,
            runtime,
            producer_lanes: OnceLock::new(),
            admin: std::sync::RwLock::new(None),
            admin_close: std::sync::Mutex::new(None),
            managed_admin_closed: AtomicBool::new(false),
            replays: replay::runtime::ReplayManager::default(),
            transactions: transaction::runtime::Manager::default(),
            transfers: transfer::dependencies::TransferRegistry::default(),
            dlt_dispatcher: OnceLock::new(),
            groups: std::sync::RwLock::new(std::collections::BTreeMap::new()),
            consumer_lifecycle_gate: tokio::sync::Mutex::new(()),
            collected_lease: std::sync::Mutex::new(None),
            external_handles: AtomicUsize::new(1),
            consumers_stop_requested: AtomicBool::new(false),
            stopped_consumers: std::sync::Mutex::new(CloseConsumers::default()),
            lifecycle: AtomicU8::new(Lifecycle::Created as u8),
        });
        let lanes = lane_names
            .into_iter()
            .map(|name| {
                let lane = ProducerLane::new(name.as_str(), &inner)?;
                Ok((name, lane))
            })
            .collect::<Result<_>>()?;
        if inner.producer_lanes.set(lanes).is_err() {
            return Err(NafkaError::Lifecycle(
                "producer lane 表发生重复初始化".into(),
            ));
        }
        let dlt_lane = inner
            .producer_lanes
            .get()
            .and_then(|lanes| lanes.get(&inner.config.behavior.dlt_producer_lane))
            .cloned()
            .ok_or_else(|| {
                NafkaError::NoSuchProducerLane(inner.config.behavior.dlt_producer_lane.clone())
            })?;
        let dispatcher = consumer::dlt::DltDispatcher::new(
            &inner,
            dlt_lane,
            inner.config.behavior.dlt_queue_capacity,
            inner.config.behavior.dlt_queue_max_bytes,
            inner.config.behavior.dlt_max_in_flight,
        )?;
        if inner.dlt_dispatcher.set(dispatcher).is_err() {
            return Err(NafkaError::Lifecycle(
                "DLT dispatcher 发生重复初始化".into(),
            ));
        }
        // 仅记录已校验的可靠性标量；默认值变化必须可见，但不能把连接配置或凭据带入启动诊断。
        tracing::info!(
            source = %inner.config.client_name,
            acks = %inner.config.producer.acks,
            idempotence = inner.config.producer.enable_idempotence,
            strict_headers = inner.config.behavior.strict_headers,
            max_consume_attempts = inner.config.behavior.max_consume_attempts,
            producer_queue_messages = inner.config.producer.queue_buffering_max_messages,
            producer_queue_kbytes = inner.config.producer.queue_buffering_max_kbytes,
            dlt_queue_bytes = inner.config.behavior.dlt_queue_max_bytes,
            "Kafka 配置合同已冻结；零次重试表示无限重试，受管 Admin 默认为只读"
        );
        Ok(Self { inner })
    }

    /// 业务作用：在后端阻塞或失败时直接观察指标出口责任。
    /// 参数说明：无。
    /// 返回：本源排队、活动、导出、丢弃与失败计数；关闭后仍可查询，不经过用户回调。
    pub fn metrics_export_stats(&self) -> MetricsExportStats {
        self.inner.metrics.stats()
    }

    /// 业务作用：返回冻结配置。
    pub fn config(&self) -> &KafkaConfig {
        &self.inner.config
    }

    /// 业务作用：区分实际资源已全部退出与仅收到宿主停机请求。
    /// 参数说明：无。
    /// 返回：仅完整 shutdown 成功后为真；超时、取消或仍有资源责任时为假。
    pub fn is_closed(&self) -> bool {
        self.inner.lifecycle() == Lifecycle::Stopped
    }

    /// 业务作用：创建一次性消费者注册 builder。
    ///
    /// 调用本方法不会发现静态消费者，也不会创建线程；这些动作由 builder 显式控制。
    pub fn consumers(&self) -> ConsumerRegistryBuilder {
        ConsumerRegistryBuilder::new(self.clone())
    }

    /// 业务作用：返回延迟构造的管理端句柄。
    pub fn admin(&self) -> KafkaAdmin {
        KafkaAdmin::new(self.clone())
    }

    /// 业务作用：分别查询构建支持、冻结启用项和实际组就绪，不把配置成功当作认证成功。
    /// 参数说明：无。返回：无网络请求且不含凭据的独立快照；普通组为空时不宣称Ready。
    pub fn capabilities(&self) -> Result<KafkaCapabilities> {
        KafkaCapabilities::snapshot(self)
    }

    /// 业务作用：等待已启动的消费组满足当前 broker 就绪要求。
    /// 参数说明：group 为最终组名；requirement 为分配要求；deadline 为绝对等待期限。
    /// 返回：满足要求的当次分配快照；组不存在、要求非法、保护或停止时失败。超时携带原要求与健康状态，成功不保证后续仍持有分区。
    pub async fn await_group_ready(
        &self,
        group: &str,
        requirement: ReadyRequirement,
        deadline: Instant,
    ) -> Result<GroupReadySnapshot> {
        let mut ready = self
            .await_groups_ready(vec![(group.to_owned(), requirement)], deadline)
            .await?;
        Ok(ready.remove(0))
    }

    /// 业务作用：观察指定 group 当前的接流量条件，无需等待或发起 broker 请求。
    /// 参数说明：group 为已启动组，requirement 为有效分配条件。
    /// 返回：包含源、要求、稳定原因和同次健康的诊断；非法要求或未知组拒绝。
    pub async fn group_readiness(
        &self,
        group: &str,
        requirement: ReadyRequirement,
    ) -> Result<ReadyStatus> {
        requirement.validate()?;
        Ok(ReadyStatus::evaluate(
            &self.config().client_name,
            requirement,
            self.group_handle(group)?.health(),
        ))
    }

    /// 业务作用：在一个总期限内复验全部 group，不锁存已经失效的早先成功。
    /// 参数说明：groups 为非空且不重复的组和要求，deadline 为所有组共享的绝对时刻。
    /// 返回：同一轮观测全部满足时返回排序快照；组不满足共同输入边界时为 Config，终止态立即失败，超时包含所有未满足项的要求和健康。
    pub async fn await_groups_ready(
        &self,
        groups: Vec<(String, ReadyRequirement)>,
        deadline: Instant,
    ) -> Result<Vec<GroupReadySnapshot>> {
        if groups.is_empty() {
            return Err(NafkaError::Config("Ready 组要求不得为空".into()));
        }
        let mut requirements = std::collections::BTreeMap::new();
        for (group, requirement) in groups {
            requirement.validate()?;
            crate::wire::validate_group(&group)?;
            if requirements.insert(group, requirement).is_some() {
                return Err(NafkaError::Config("Ready 组名不得为空或重复".into()));
            }
        }
        // 绑定具体 group 实例；等待期间同名任务重建不能把旧等待者迁移到新 owner 集合。
        let handles = requirements
            .into_iter()
            .map(|(group, requirement)| {
                self.group_handle(&group)
                    .map(|handle| (group, requirement, handle))
            })
            .collect::<Result<Vec<_>>>()?;
        let started = Instant::now();
        loop {
            let mut ready = Vec::new();
            let mut missing = Vec::new();
            for (_, requirement, handle) in &handles {
                let status = ReadyStatus::evaluate(
                    &self.config().client_name,
                    requirement.clone(),
                    handle.health(),
                );
                if status.ready() {
                    ready.push(GroupReadySnapshot {
                        group: status.health.group,
                        assignment_epoch: status
                            .health
                            .ready_assignment_epoch
                            .expect("Ready requires assignment epoch"),
                        assignment: status.health.assignment,
                        observed_at: status.observed_at,
                    });
                } else {
                    missing.push(status);
                }
            }
            // 每轮完整采样；保护态优先退出，不能因另一组仍在 join 而拖到总期限。
            for status in &missing {
                if status.reason == ReadyReason::Crashed {
                    return Err(NafkaError::GroupFatal {
                        group: status.health.group.clone(),
                        message: status
                            .health
                            .last_error
                            .clone()
                            .unwrap_or_else(|| "owner 已崩溃".into()),
                    });
                }
                if matches!(
                    status.reason,
                    ReadyReason::Degraded | ReadyReason::Stopping | ReadyReason::Stopped
                ) {
                    return Err(NafkaError::Lifecycle(format!(
                        "group `{}` 无法满足 Ready: {:?}",
                        status.health.group, status.reason
                    )));
                }
            }
            if missing.is_empty() {
                for (group, _, _) in &handles {
                    self.inner
                        .counter("group_ready_total", 1, &[("group", group)]);
                    self.inner.gauge(
                        "group_ready_wait_millis",
                        i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
                        &[("group", group)],
                    );
                }
                return Ok(ready);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                for status in &missing {
                    self.inner.counter(
                        "group_ready_timeout_total",
                        1,
                        &[("group", &status.health.group)],
                    );
                }
                return Err(NafkaError::GroupReadyTimeout {
                    groups: missing
                        .iter()
                        .map(|status| status.health.group.clone())
                        .collect(),
                    last_health: missing.iter().map(|status| status.health.clone()).collect(),
                    readiness: missing,
                });
            }
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(20))).await;
        }
    }

    /// 业务作用：同步封闭消费登记并通知全部已接纳任务永久停止，保留收尾依赖。
    /// 参数说明：无。
    /// 返回：发布停止意图，不等待退出、不释放租约或原文；调用 stop_consumers_until 或 shutdown_until 取得实际退出结论。
    pub fn request_consumer_stop(&self) {
        self.inner
            .consumers_stop_requested
            .store(true, Ordering::Release);
        let groups = {
            let groups = self
                .inner
                .groups
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.inner.lifecycle() == Lifecycle::Stopped {
                return;
            }
            // 与登记发布共用写锁；冻结 Created 后，尚在构造的 registry 只能回滚，不能在停止快照之外开放。
            if self.inner.lifecycle() == Lifecycle::Created {
                let _ = self
                    .inner
                    .transition(Lifecycle::Created, Lifecycle::Running);
            }
            groups.values().cloned().collect::<Vec<_>>()
        };
        // 先撤销所有槽的派发和确认，再通知有限任务；等待预算为零也不能继续接纳业务。
        for group in groups {
            group.request_permanent_stop();
        }
        self.inner.transfers.stop();
        self.inner.replays.stop();
        self.inner.transactions.stop();
    }

    /// 业务作用：停止普通消费者、事务、回放及转发依赖，保留 producer lane 与管理端供业务退出收尾。
    ///
    /// 该入口是容器生命周期协议，不是运行期暂停按钮。尚未进入最终关闭时，成功后保持 `Running`，
    /// 已取得的 lane 可用于收尾；若已进入最终关闭则不重新开放发布。consumer registry 永久封口，不能再次启动。
    ///
    /// # 参数说明
    ///
    /// - `deadline`: 框架提供的绝对截止时刻；消费槽、有限任务、事务和旧分配 DLT 共用，不逐任务重置。
    ///
    /// # 返回
    ///
    /// 所有消费槽、有限任务、事务与旧分配 DLT 已退出，group 表及组状态指标已清理、静态收集租约已释放时返回 `Ok(())`；累计观测不会清零。
    ///
    /// # 错误
    ///
    /// 生命周期门或任一 owner 未能在 deadline 前完成时返回 `ShutdownTimeout`；此时保留
    /// group 表和静态收集租约，使后续 final shutdown 可以继续收尾。回放、转发依赖或事务未退出时返回
    /// `ReplayShutdownTimeout`，保留相应任务身份、资源及其持有的持久 lease。
    /// 旧分配的实际死信任务尚未退出时返回 `DltShutdownTimeout`，保留源级交付责任。
    pub async fn stop_consumers_until(&self, deadline: Instant) -> Result<()> {
        self.request_consumer_stop();
        if self.inner.lifecycle() == Lifecycle::Stopped {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.shutdown_timeout_snapshot(false));
        }
        // 生命周期门覆盖 start 的完整本地构造握手。stop 先取得门时把 Created 封成
        // Running，等待中的 start 随后会稳定失败；否则它可能在 stop 返回后启动一套
        // 不受容器 active action 管理的 owner。
        let _consumer_lifecycle =
            tokio::time::timeout(remaining, self.inner.consumer_lifecycle_gate.lock())
                .await
                .map_err(|_| self.shutdown_timeout_snapshot(false))?;

        match self.inner.lifecycle() {
            Lifecycle::Stopped => return Ok(()),
            Lifecycle::Created => {
                self.inner
                    .transition(Lifecycle::Created, Lifecycle::Running)?;
            }
            Lifecycle::Running | Lifecycle::ShuttingDown => {}
        }

        let transfers = self.inner.transfers.stop();
        let replays = self.inner.replays.stop();
        let transactions = self.inner.transactions.stop();
        let groups = self.managed_groups();
        let failed_groups = stop_managed_groups(&groups, deadline).await;
        let mut failed_replays = replay::runtime::drain(&replays, deadline).await;
        failed_replays.extend(transfer::dependencies::drain(&transfers, deadline).await);
        failed_replays.extend(transaction::runtime::drain(&transactions, deadline).await);
        if !failed_replays.is_empty() {
            return Err(NafkaError::ReplayShutdownTimeout {
                tasks: failed_replays,
                groups: failed_groups,
            });
        }
        if !failed_groups.is_empty() {
            return Err(NafkaError::ShutdownTimeout {
                groups: failed_groups,
                producer_lanes: Vec::new(),
            });
        }
        // 旧分配失权后已不在 owner 待完成表中，源仍须持有它的原生交付与完成通知责任。
        if let Some(dispatcher) = self.inner.dlt_dispatcher.get() {
            dispatcher.await_idle(deadline).await?;
        }
        self.finish_consumer_stop();
        Ok(())
    }

    /// 业务作用：先停止新消费及任务准入，再排干有限任务、事务、普通消费及发布责任，最后关闭管理与观测资源。
    ///
    /// 若 [`Self::stop_consumers_until`] 已成功，本方法把空 group 表视为正常两段停机路径，
    /// 继续排空 lane 并关闭管理与观测资源；不会重复释放静态收集租约。
    ///
    /// # 参数说明
    ///
    /// - `deadline`: 框架提供的绝对截止时刻；获取生命周期门、停止 group 和排空 lane
    ///   及有限任务、事务、DLT、管理与观测责任共同消费这一份预算。
    ///
    /// # 返回
    ///
    /// 所有受管资源关闭且总状态进入 `Stopped` 时返回 `Ok(())`；重复调用同样返回成功。
    ///
    /// # 错误
    ///
    /// 生命周期门、owner 或 lane 未能在 deadline 前完成时返回带稳定 group/lane 名的
    /// `ShutdownTimeout`，后续调用继续等待实际责任。获取门禁前超时不会强行改变源状态；
    /// 进入发布关闭阶段后保持 `ShuttingDown`。有限任务、转发依赖或事务尚未退出时返回
    /// `ReplayShutdownTimeout`，此时新任务准入已关闭，仍保留发布能力供已接纳处理收尾。
    /// 旧分配死信任务未退出时返回 `DltShutdownTimeout`，不提前关闭其内部发布依赖。
    /// 管理或观测线程未退出分别返回 `AdminShutdownTimeout`、`MetricsShutdownTimeout`。
    pub async fn shutdown_until(&self, deadline: Instant) -> Result<()> {
        self.request_consumer_stop();
        if self.inner.lifecycle() == Lifecycle::Stopped {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.shutdown_timeout_snapshot(true));
        }
        // stop 与 final 共用同一门并覆盖完整收尾。这样并发 final 不会重复释放租约，
        // stop 也不会在 lane 已关闭后重新观察并操作旧 group。
        let _consumer_lifecycle =
            tokio::time::timeout(remaining, self.inner.consumer_lifecycle_gate.lock())
                .await
                .map_err(|_| self.shutdown_timeout_snapshot(true))?;

        // 先封闭回放准入并排干已接纳的同步处理；它仍可能依赖 producer，不能先撤销发布能力。
        let transfers = self.inner.transfers.stop();
        let replays = self.inner.replays.stop();
        let transactions = self.inner.transactions.stop();
        let mut failed_replays = replay::runtime::drain(&replays, deadline).await;
        failed_replays.extend(transfer::dependencies::drain(&transfers, deadline).await);
        failed_replays.extend(transaction::runtime::drain(&transactions, deadline).await);
        if !failed_replays.is_empty() {
            let groups = self.managed_groups();
            let failed_groups = stop_managed_groups(&groups, deadline).await;
            return Err(NafkaError::ReplayShutdownTimeout {
                tasks: failed_replays,
                groups: failed_groups,
            });
        }

        loop {
            match self.inner.lifecycle() {
                Lifecycle::Stopped => return Ok(()),
                Lifecycle::ShuttingDown => break,
                current @ (Lifecycle::Created | Lifecycle::Running) => {
                    if self
                        .inner
                        .transition(current, Lifecycle::ShuttingDown)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }

        let lanes = self.producer_lanes();
        for lane in lanes.values() {
            lane.begin_draining();
        }

        let groups = self.managed_groups();
        let failed_groups = stop_managed_groups(&groups, deadline).await;
        if failed_groups.is_empty() {
            // 实际死信任务退出前不能释放消费身份或关闭其内部发布依赖。
            if let Some(dispatcher) = self.inner.dlt_dispatcher.get() {
                dispatcher.await_idle(deadline).await?;
            }
            // group 一旦全部确认退出就立即清表并释放租约；不能让后续 lane 超时把已经
            // 完成的 consumer drain 伪装成仍占用激活权。
            self.finish_consumer_stop();
        }

        // group 超时不能短路：内部 DLT 可能已经进入 producer 队列，必须继续尝试 flush，
        // 并在同一错误里同时报告 consumer 与 lane 未完成项。
        for lane in lanes.values() {
            lane.enter_internal_only();
        }
        let lane_results = futures::future::join_all(lanes.values().map(|lane| async {
            if failed_groups.is_empty() {
                // 所有业务责任退出后才永久关闭内部准入；成功还必须包含原生线程与连接真实释放。
                lane.close_until(deadline).await
            } else {
                // 尚存 owner 可能需要完成已接纳 DLT，保留内部发布能力，只尝试排空现有消息。
                lane.flush_until(deadline).await
            }
        }))
        .await;
        let failed_lanes: Vec<String> = lanes
            .keys()
            .zip(lane_results)
            .filter_map(|(name, result)| result.err().map(|_| name.clone()))
            .collect();
        if !failed_groups.is_empty() || !failed_lanes.is_empty() {
            return Err(NafkaError::ShutdownTimeout {
                groups: failed_groups,
                producer_lanes: failed_lanes,
            });
        }

        let admin_close = {
            let mut admin = self
                .inner
                .admin
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // 先永久关闭全部管理入口，再转交实际资源；与惰性构造共用写锁，旧句柄不能在最终关闭后重建连接。
            self.inner
                .managed_admin_closed
                .store(true, Ordering::Release);
            let mut closing = self
                .inner
                .admin_close
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if closing.is_none() {
                if let Some(client) = admin.as_ref() {
                    // 线程构造成功前保留槽位引用，构造失败可以再次关闭，不能同步丢弃仍被借用的原生资源。
                    *closing = Some(rd::admin::AdminClose::start(Arc::clone(client))?);
                    admin.take();
                }
            }
            closing.clone()
        };
        if let Some(closing) = admin_close {
            if !closing.await_closed(deadline).await {
                return Err(NafkaError::AdminShutdownTimeout {
                    resource_holders: closing.resource_holders(),
                });
            }
        }
        // 业务资源已退出后再关闭观测；队列可丢弃，正在执行的用户代码仍需真实返回。
        self.inner.metrics.close();
        if !self.inner.metrics.await_closed(deadline).await {
            return Err(NafkaError::MetricsShutdownTimeout {
                active: self.inner.metrics.stats().active,
            });
        }
        self.inner
            .transition(Lifecycle::ShuttingDown, Lifecycle::Stopped)?;
        Ok(())
    }

    /// 业务作用：使用配置中的相对停机预算完成独立模式的全量关闭。
    ///
    /// Application 受管模式应调用 [`Self::shutdown_until`] 并传入全局绝对 deadline，
    /// 避免每个清理 action 重新获得一份完整预算。
    ///
    /// 参数说明：无。
    ///
    /// # 返回
    ///
    /// 全部受管资源进入 `Stopped` 时返回 `Ok(())`；已关闭运行时重复调用也返回成功。
    ///
    /// # 错误
    ///
    /// 保留 [`Self::shutdown_until`] 的关闭错误分类，超时不会释放尚未退出的资源责任。
    pub async fn shutdown(&self) -> Result<()> {
        let deadline = Instant::now()
            + std::time::Duration::from_millis(self.inner.config.behavior.shutdown_timeout_ms);
        self.shutdown_until(deadline).await
    }

    /// 业务作用：返回冻结后的 producer lane 表。
    ///
    /// # 返回
    ///
    /// `connect_with_metrics` 完成本地构造时一次性发布的稳定有序 lane 表。
    fn producer_lanes(&self) -> &std::collections::BTreeMap<String, ProducerLane> {
        self.inner
            .producer_lanes
            .get()
            .expect("connect 必须冻结 producer lane 表")
    }

    /// 业务作用：快照当前受管 group，供持有生命周期门的停机路径并发等待。
    ///
    /// # 返回
    ///
    /// 按 resolved group id 排序的句柄副本；句柄只延长 owner 观察期，不转移所有权。
    fn managed_groups(&self) -> Vec<(String, Arc<consumer::supervisor::GroupHandle>)> {
        self.inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(name, handle)| (name.clone(), Arc::clone(handle)))
            .collect()
    }

    /// 业务作用：在全部 owner 已确认退出后清理 consumer 侧可见状态。
    ///
    /// # 返回
    ///
    /// 本方法无返回值；group 表、注册规模指标和 collected lease 在同一生命周期门下
    /// 依次归零，重复调用不会二次释放租约。
    fn finish_consumer_stop(&self) {
        {
            let mut groups = self
                .inner
                .groups
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut saved = self
                .inner
                .stopped_consumers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // 先封存纯数值再释放 owner 表；重复关闭遇到空表不能抹去未确认业务证据。
            for group in groups.values() {
                saved.add(&group.close_report());
            }
            groups.clear();
        }
        consumer::registry::publish_registry_gauges(&self.inner, 0, 0);
        let lease = self
            .inner
            .collected_lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(client) = lease {
            consumer::registry::release_activation(&client);
        }
    }

    /// 业务作用：构造生命周期门等待失败时的脱敏未完成资源快照。
    ///
    /// # 参数
    ///
    /// - `include_lanes`: final shutdown 传 `true` 以报告全部 lane；consumer-only drain
    ///   传 `false`，因为该阶段按合同不排空 producer。
    ///
    /// # 返回
    ///
    /// 只包含稳定 group/lane 或回放任务名称的超时类别，不包含 broker 或凭据。
    fn shutdown_timeout_snapshot(&self, include_lanes: bool) -> NafkaError {
        let groups = self
            .inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
        let mut tasks = self.inner.replays.pending_names();
        tasks.extend(self.inner.transfers.pending_names());
        if !tasks.is_empty() {
            return NafkaError::ReplayShutdownTimeout { tasks, groups };
        }
        let producer_lanes = if include_lanes {
            self.producer_lanes().keys().cloned().collect()
        } else {
            Vec::new()
        };
        NafkaError::ShutdownTimeout {
            groups,
            producer_lanes,
        }
    }
}

/// 业务作用：在共享绝对截止时刻前并发停止一组 owner。
///
/// # 参数
///
/// - `groups`: 按 resolved group id 排序的受管句柄快照，由持有生命周期门的调用方提供。
/// - `deadline`: 所有 owner 共享的绝对截止时刻，不能为单个 group 重置。
///
/// # 返回
///
/// 返回未能确认退出的 group 名，按输入顺序稳定排列；空列表表示全部停止成功。
async fn stop_managed_groups(
    groups: &[(String, Arc<consumer::supervisor::GroupHandle>)],
    deadline: Instant,
) -> Vec<String> {
    let results = futures::future::join_all(
        groups
            .iter()
            .map(|(_name, handle)| handle.stop_until(deadline)),
    )
    .await;
    groups
        .iter()
        .map(|(name, _)| name.clone())
        .zip(results)
        .filter_map(|(name, result)| result.err().map(|_| name))
        .collect()
}
