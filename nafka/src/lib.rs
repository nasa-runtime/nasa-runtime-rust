//! Kafka 运行时公共入口。
//!
//! 本 crate 负责配置校验、发布 lane、消费者注册、确认语义、控制面与少拷贝借用入口。
//! 底层客户端类型全部限制在 `rd` 模块，业务代码应经统一门面使用这里的自有类型。

#![forbid(unsafe_code)]
#![allow(async_fn_in_trait)]
#![deny(missing_docs)]

mod admin;
mod config;
mod consumer;
mod error;
mod health;
mod metrics;
mod producer;
mod rd;
#[cfg(feature = "schema-registry")]
mod schema_registry;
mod types;
mod wire;

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use futures::StreamExt;

pub use admin::{KafkaAdmin, TopicDescription, TopicSpec};
pub use config::{
    AdminConfig, BehaviorConfig, ConsumerConfig, InvalidRecordPolicy, KafkaConfig, ProducerConfig,
    ProducerLaneOverride, SecurityConfig, UnmatchedPolicy, KAFKA_CONFIG_FIELDS,
};
pub use consumer::{
    AckBatchExt, AckMode, AckToken, BatchConsumer, BorrowedKafkaRecord, ConsumeCtx,
    ConsumerRegistryBuilder, ConsumerRuntime, GroupSpec, InvalidRecordReason, KafkaHeaderRef,
    KafkaHeadersRef, KafkaRecord, PassthroughConsumer, PassthroughDeadLetterReason,
    PassthroughDisposition, PassthroughFailure, PassthroughFailureAction, PassthroughFailureReason,
    PassthroughFatalReason, PassthroughHaltReason, PassthroughRetryReason, PassthroughSkipReason,
    SingleConsumer,
};
pub use error::{NafkaError, ProducerQueue, Result};
pub use health::{
    GroupHealth, GroupReadySnapshot, GroupState, HaltReason, PauseReason, ReadyRequirement,
};
pub use metrics::{MetricLabels, MetricsSink};
pub use producer::{
    BatchPublishError, ProducerLane, ProducerLaneStats, PublishBuilder, PublishItem,
    RawPublishBuilder, TombstoneBuilder,
};
#[cfg(feature = "schema-registry")]
pub use schema_registry::*;
pub use types::{
    AssignmentHandle, Delivery, KafkaHeader, KafkaHeaders, KafkaPartitionLag, StartOffset, Tp,
};
pub use wire::{
    mode_from_wire_name, mode_wire_name, DecodePayload, EncodePayload, PayloadCodec, Proto,
    ProtoMode, DEFAULT_EVENT, HEADER_DLT_ORIGIN_OFFSET, HEADER_DLT_ORIGIN_PARTITION,
    HEADER_DLT_ORIGIN_TOPIC, HEADER_DLT_REASON, HEADER_EVENT, HEADER_PASSTHROUGH,
    HEADER_PAYLOAD_CODEC, HEADER_PAYLOAD_MODE, HEADER_TRACEPARENT, PAYLOAD_CODEC_PROTOCOL_BYTES,
};

#[cfg(feature = "macros")]
pub use nafka_macro::kafka_consumer;

/// 常用消费者实现所需的精简导入集合。
pub mod prelude {
    pub use crate::{
        AckBatchExt, AckMode, BatchConsumer, ConsumeCtx, DecodePayload, GroupSpec, KafkaProxy,
        KafkaRecord, NafkaError, Result, SingleConsumer,
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
    /// 应用注入的非阻塞指标出口。
    pub(crate) metrics: Arc<dyn MetricsSink>,
    /// Application-owned 遥测的只写入口；不拥有 exporter 生命周期。
    pub(crate) span_recorder: Option<natelemetry::SpanRecorder>,
    /// connect 时捕获的异步运行时句柄。
    #[allow(dead_code)]
    pub(crate) runtime: tokio::runtime::Handle,
    /// connect 时冻结的共享 producer lane 句柄表。
    pub(crate) producer_lanes: OnceLock<std::collections::BTreeMap<String, ProducerLane>>,
    /// 首次管理操作时构造的共享管理客户端；shutdown 会显式丢弃它。
    pub(crate) admin: std::sync::RwLock<Option<Arc<rd::admin::AdminHandle>>>,
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
    /// 总生命周期原子状态。
    lifecycle: AtomicU8,
}

impl KafkaProxyInner {
    /// 发出一个稳定 counter，调用点不得传入高基数或敏感字段。
    ///
    /// 用户实现的 panic 必须在这里被吃掉：部分调用点位于 DLT 投递任务内、
    /// `sender.send(DltCompletion)` 之前，一次 panic 会让 completion 永不到达，
    /// 分区永久停在 `DltPending` 且 `shutdown()` 永远超时；另一些位于 owner 线程的
    /// catch_unwind 之外，会直接把 group 打成 `Crashed`。指标是观测面，绝不能改变数据面结局。
    pub(crate) fn counter(&self, name: &'static str, delta: u64, labels: MetricLabels<'_>) {
        self.emit(|| self.metrics.counter(name, delta, labels), name);
    }

    /// 发出一个稳定 gauge，调用点不得传入高基数或敏感字段。
    pub(crate) fn gauge(&self, name: &'static str, value: i64, labels: MetricLabels<'_>) {
        self.emit(|| self.metrics.gauge(name, value, labels), name);
    }

    /// 隔离用户指标实现的 panic；只告警，不改变调用方控制流。
    ///
    /// # 参数
    ///
    /// - `emit`: 实际的 sink 调用。
    /// - `name`: 出问题时用于定位的指标名（框架固定常量，非高基数）。
    fn emit(&self, emit: impl FnOnce(), name: &'static str) {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(emit)).is_err() {
            tracing::error!(
                metric = name,
                "MetricsSink 实现 panic，已隔离并忽略本次上报"
            );
        }
    }

    /// 读取当前生命周期。
    pub(crate) fn lifecycle(&self) -> Lifecycle {
        match self.lifecycle.load(Ordering::Acquire) {
            0 => Lifecycle::Created,
            1 => Lifecycle::Running,
            2 => Lifecycle::ShuttingDown,
            _ => Lifecycle::Stopped,
        }
    }

    /// 尝试从预期状态切换到新状态。
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

    /// 判断是否仍允许外部发布入口取得资格。
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
    /// 外部句柄已全部离开且 owner 线程真正退出后兜底归还 collected lease。
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

/// 将生命周期字节转换为稳定诊断文本。
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
    /// 克隆业务句柄并单独计入 Drop 收尾判定。
    fn clone(&self) -> Self {
        self.inner.external_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for KafkaProxy {
    /// 最后一个业务句柄离开时发送非阻塞 Stop；不在 Drop 内等待或 flush。
    fn drop(&mut self) {
        if self.inner.external_handles.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

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
    /// 输出客户端名与当前生命周期；不含 bootstrap 之外的连接细节，也不含任何凭据。
    ///
    /// - `f`: 调用方提供的格式化缓冲区。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProxy")
            .field("client_name", &self.inner.config.client_name)
            .field("lifecycle", &self.inner.lifecycle())
            .finish()
    }
}

impl KafkaProxy {
    /// 校验配置并捕获当前异步运行时句柄。
    ///
    /// 本方法只完成本地构造，不连接 broker，也不隐式启动消费者。
    ///
    /// # 参数
    ///
    /// - `config`: 完整组件配置；成功后不可变。
    ///
    /// # 错误
    ///
    /// 配置不满足不变量，或当前线程不在异步运行时中时返回错误。
    pub fn connect(config: KafkaConfig) -> Result<Self> {
        Self::connect_with_metrics(config, Arc::new(metrics::NoopMetrics))
    }

    /// 校验配置并注入后端无关的指标接收端。
    ///
    /// # 参数
    ///
    /// - `config`: 完整组件配置；成功后不可变。
    /// - `metrics`: 应用提供的非阻塞指标出口。
    ///
    /// # 错误
    ///
    /// 配置不满足不变量，或当前线程不在异步运行时中时返回错误。
    pub fn connect_with_metrics(
        config: KafkaConfig,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<Self> {
        Self::connect_with_observability(config, metrics, None)
    }

    /// 校验配置并注入指标与只写 span 记录器。
    pub fn connect_with_observability(
        config: KafkaConfig,
        metrics: Arc<dyn MetricsSink>,
        span_recorder: Option<natelemetry::SpanRecorder>,
    ) -> Result<Self> {
        config.validate()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| NafkaError::NoRuntime)?;
        let lane_names = producer::configured_lane_names(&config.producer.lanes);
        let inner = Arc::new(KafkaProxyInner {
            config,
            metrics,
            span_recorder,
            runtime,
            producer_lanes: OnceLock::new(),
            admin: std::sync::RwLock::new(None),
            dlt_dispatcher: OnceLock::new(),
            groups: std::sync::RwLock::new(std::collections::BTreeMap::new()),
            consumer_lifecycle_gate: tokio::sync::Mutex::new(()),
            collected_lease: std::sync::Mutex::new(None),
            external_handles: AtomicUsize::new(1),
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
        Ok(Self { inner })
    }

    /// 返回冻结配置。
    pub fn config(&self) -> &KafkaConfig {
        &self.inner.config
    }

    /// 创建一次性消费者注册 builder。
    ///
    /// 调用本方法不会发现静态消费者，也不会创建线程；这些动作由 builder 显式控制。
    pub fn consumers(&self) -> ConsumerRegistryBuilder {
        ConsumerRegistryBuilder::new(self.clone())
    }

    /// 返回延迟构造的管理端句柄。
    pub fn admin(&self) -> KafkaAdmin {
        KafkaAdmin::new(self.clone())
    }

    /// 等待单个 group 满足 broker 就绪要求。
    ///
    /// # 参数
    ///
    /// - `group`: 已启动的最终 group id。
    /// - `requirement`: assignment 就绪条件。
    /// - `deadline`: 所有等待共享的绝对截止时刻。
    ///
    /// # 错误
    ///
    /// group 不存在、发生致命错误或超过截止时刻时返回错误。
    pub async fn await_group_ready(
        &self,
        group: &str,
        requirement: ReadyRequirement,
        deadline: Instant,
    ) -> Result<GroupReadySnapshot> {
        validate_ready_requirement(&requirement)?;
        let handle = self.group_handle(group)?;
        let wait_started = Instant::now();
        loop {
            let health = handle.health();
            if health.state == GroupState::Crashed {
                return Err(NafkaError::GroupFatal {
                    group: group.to_owned(),
                    message: health.last_error.unwrap_or_else(|| "owner 已崩溃".into()),
                });
            }
            if matches!(health.state, GroupState::Stopping | GroupState::Stopped) {
                return Err(NafkaError::Lifecycle(format!(
                    "group `{group}` 已进入 {:?}，无法满足 ready",
                    health.state
                )));
            }
            if let Some(epoch) = health.ready_assignment_epoch {
                if ready_satisfied(&requirement, &health.assignment) {
                    self.inner
                        .counter("group_ready_total", 1, &[("group", group)]);
                    self.inner.gauge("group_ready", 1, &[("group", group)]);
                    // 用毫秒：整秒精度会把绝大多数（亚秒级）等待统统记成 0，指标失去意义。
                    self.inner.gauge(
                        "group_ready_wait_millis",
                        i64::try_from(wait_started.elapsed().as_millis()).unwrap_or(i64::MAX),
                        &[("group", group)],
                    );
                    return Ok(GroupReadySnapshot {
                        group: group.to_owned(),
                        assignment_epoch: epoch,
                        assignment: health.assignment,
                        observed_at: std::time::SystemTime::now(),
                    });
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.inner
                    .counter("group_ready_timeout_total", 1, &[("group", group)]);
                self.inner.gauge("group_ready", 0, &[("group", group)]);
                return Err(NafkaError::GroupReadyTimeout {
                    groups: vec![group.to_owned()],
                    last_health: vec![health],
                });
            }
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(20))).await;
        }
    }

    /// 并发等待多个 group 满足各自的 broker 就绪要求。
    ///
    /// # 参数
    ///
    /// - `groups`: group id 与要求的列表。
    /// - `deadline`: 全部 group 共享的绝对截止时刻。
    ///
    /// # 错误
    ///
    /// 任一 group 致命失败，或截止时仍有 group 未满足要求时返回错误。
    pub async fn await_groups_ready(
        &self,
        groups: Vec<(String, ReadyRequirement)>,
        deadline: Instant,
    ) -> Result<Vec<GroupReadySnapshot>> {
        let expected = groups.len();
        let mut futures = futures::stream::FuturesUnordered::new();
        for (group, requirement) in groups {
            futures
                .push(async move { self.await_group_ready(&group, requirement, deadline).await });
        }
        let mut ready = Vec::with_capacity(expected);
        let mut timed_out_groups = std::collections::BTreeSet::new();
        let mut timed_out_health = std::collections::BTreeMap::new();
        while let Some(result) = futures.next().await {
            match result {
                Ok(snapshot) => ready.push(snapshot),
                Err(NafkaError::GroupReadyTimeout {
                    groups,
                    last_health,
                }) => {
                    timed_out_groups.extend(groups);
                    for health in last_health {
                        timed_out_health.insert(health.group.clone(), health);
                    }
                }
                // FuturesUnordered 按完成顺序交付；fatal/lifecycle/config 等立即错误不会再
                // 被其他仍等待 assignment 的 group 拖到共享 deadline。
                Err(error) => return Err(error),
            }
        }
        if timed_out_groups.is_empty() {
            ready.sort_by(|left, right| left.group.cmp(&right.group));
            Ok(ready)
        } else {
            Err(NafkaError::GroupReadyTimeout {
                groups: timed_out_groups.into_iter().collect(),
                last_health: timed_out_health.into_values().collect(),
            })
        }
    }

    /// 在绝对截止时刻前只停止消费者，保留 producer lane 与管理端供退出收尾使用。
    ///
    /// 该入口是容器生命周期协议，不是运行期暂停按钮。成功后总状态保持 `Running`，因此
    /// 已取得的 lane 仍可发布；consumer registry 已被永久封口，不能在同一运行时再次启动。
    ///
    /// # 参数
    ///
    /// - `deadline`: 框架提供的绝对截止时刻；全部 group 共享该预算，不能按 group 重置。
    ///
    /// # 返回
    ///
    /// 所有 owner 已确认退出、group 表和指标已清零、静态收集租约已释放时返回 `Ok(())`。
    ///
    /// # 错误
    ///
    /// 生命周期门或任一 owner 未能在 deadline 前完成时返回 `ShutdownTimeout`；此时保留
    /// group 表和静态收集租约，使后续 final shutdown 可以继续收尾。
    pub async fn stop_consumers_until(&self, deadline: Instant) -> Result<()> {
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

        let groups = self.managed_groups();
        let failed_groups = stop_managed_groups(&groups, deadline).await;
        if !failed_groups.is_empty() {
            return Err(NafkaError::ShutdownTimeout {
                groups: failed_groups,
                producer_lanes: Vec::new(),
            });
        }
        self.finish_consumer_stop();
        Ok(())
    }

    /// 在绝对截止时刻前完成消费者与 producer 的最终关闭。
    ///
    /// 若 [`Self::stop_consumers_until`] 已成功，本方法把空 group 表视为正常两段停机路径，
    /// 只排空 lane 并关闭管理端；不会重复 stop owner 或二次释放静态收集租约。
    ///
    /// # 参数
    ///
    /// - `deadline`: 框架提供的绝对截止时刻；获取生命周期门、停止 group 和排空 lane
    ///   共同消费这一份预算。
    ///
    /// # 返回
    ///
    /// 所有受管资源关闭且总状态进入 `Stopped` 时返回 `Ok(())`；重复调用同样返回成功。
    ///
    /// # 错误
    ///
    /// 生命周期门、owner 或 lane 未能在 deadline 前完成时返回带稳定 group/lane 名的
    /// `ShutdownTimeout`，并保持 `ShuttingDown` 供后续调用继续收尾。
    pub async fn shutdown_until(&self, deadline: Instant) -> Result<()> {
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
            // group 一旦全部确认退出就立即清表并释放租约；不能让后续 lane 超时把已经
            // 完成的 consumer drain 伪装成仍占用激活权。
            self.finish_consumer_stop();
        }

        // group 超时不能短路：内部 DLT 可能已经进入 producer 队列，必须继续尝试 flush，
        // 并在同一错误里同时报告 consumer 与 lane 未完成项。
        for lane in lanes.values() {
            lane.enter_internal_only();
        }
        let lane_results =
            futures::future::join_all(lanes.values().map(|lane| lane.flush_until(deadline))).await;
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

        for lane in lanes.values() {
            lane.close();
        }
        // 管理端是延迟构造资源；丢弃共享句柄即可关闭本次 generation，不能依赖 Drop
        // 顺便改变 producer/consumer 的状态。
        self.inner
            .admin
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.inner
            .transition(Lifecycle::ShuttingDown, Lifecycle::Stopped)?;
        Ok(())
    }

    /// 使用配置中的相对停机预算完成独立模式的全量关闭。
    ///
    /// Application 受管模式应调用 [`Self::shutdown_until`] 并传入全局绝对 deadline，
    /// 避免每个清理 action 重新获得一份完整预算。
    ///
    /// # 返回
    ///
    /// 全部受管资源进入 `Stopped` 时返回 `Ok(())`；已关闭运行时重复调用也返回成功。
    ///
    /// # 错误
    ///
    /// 配置预算内仍有 group 或 lane 未完成时返回 `ShutdownTimeout`。
    pub async fn shutdown(&self) -> Result<()> {
        let deadline = Instant::now()
            + std::time::Duration::from_millis(self.inner.config.behavior.shutdown_timeout_ms);
        self.shutdown_until(deadline).await
    }

    /// 返回冻结后的 producer lane 表。
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

    /// 快照当前受管 group，供持有生命周期门的停机路径并发等待。
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

    /// 在全部 owner 已确认退出后清理 consumer 侧可见状态。
    ///
    /// # 返回
    ///
    /// 本方法无返回值；group 表、注册规模指标和 collected lease 在同一生命周期门下
    /// 依次归零，重复调用不会二次释放租约。
    fn finish_consumer_stop(&self) {
        self.inner
            .groups
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
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

    /// 构造生命周期门等待失败时的脱敏未完成资源快照。
    ///
    /// # 参数
    ///
    /// - `include_lanes`: final shutdown 传 `true` 以报告全部 lane；consumer-only drain
    ///   传 `false`，因为该阶段按合同不排空 producer。
    ///
    /// # 返回
    ///
    /// 只包含稳定 group/lane 名称的 `ShutdownTimeout`，不包含 broker 或凭据。
    fn shutdown_timeout_snapshot(&self, include_lanes: bool) -> NafkaError {
        let groups = self
            .inner
            .groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect();
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

/// 在共享绝对截止时刻前并发停止一组 owner。
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

/// 校验 ready 要求本身，避免把永远不可能满足的条件伪装成 broker 超时。
///
/// # 参数
///
/// - `requirement`: 调用方提供的就绪条件。
///
/// # 错误
///
/// 最小分区数为零，或 topic 列表为空、含空项/重复项时返回配置错误。
fn validate_ready_requirement(requirement: &ReadyRequirement) -> Result<()> {
    match requirement {
        ReadyRequirement::Joined => Ok(()),
        ReadyRequirement::Assigned { min_partitions: 0 } => Err(NafkaError::Config(
            "ReadyRequirement::Assigned.min_partitions 必须大于零".into(),
        )),
        ReadyRequirement::Assigned { .. } => Ok(()),
        ReadyRequirement::AssignedTopics(topics) => {
            let mut unique = std::collections::BTreeSet::new();
            if topics.is_empty()
                || topics.iter().any(|topic| topic.trim().is_empty())
                || topics.iter().any(|topic| !unique.insert(topic))
            {
                Err(NafkaError::Config(
                    "ReadyRequirement::AssignedTopics 必须非空、无空值且不重复".into(),
                ))
            } else {
                Ok(())
            }
        }
    }
}

/// 判断 assignment 快照是否满足调用方就绪要求。
///
/// # 参数
///
/// - `requirement`: 就绪条件。
/// - `assignment`: 当前分区快照。
fn ready_satisfied(requirement: &ReadyRequirement, assignment: &[Tp]) -> bool {
    match requirement {
        ReadyRequirement::Joined => true,
        ReadyRequirement::Assigned { min_partitions } => {
            *min_partitions > 0 && assignment.len() >= *min_partitions
        }
        ReadyRequirement::AssignedTopics(topics) => {
            !topics.is_empty()
                && topics
                    .iter()
                    .all(|topic| assignment.iter().any(|tp| &tp.topic == topic))
        }
    }
}
