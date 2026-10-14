//! 每 lane 独立 producer、准入状态机和 delivery observer。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{NafkaError, ProducerQueue, Result};
use crate::producer::ProducerLaneStats;
use crate::types::{Delivery, KafkaHeaders};

use super::config::{effective_producer_lane, producer_config};
use crate::config::KafkaConfig;
use crate::MetricsSink;

/// 按原生代次保存永久错误的单槽邮箱，保留最高代次的首条证据。
type TerminalMailbox = Arc<Mutex<Option<(u64, RDKafkaErrorCode, String)>>>;

/// lane 的发布准入阶段。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum LanePhase {
    /// 接受业务发布与框架内部发布。
    Open = 0,
    /// 已拒绝新的业务发布，既有请求正在排空。
    Draining = 1,
    /// 只接受关闭收尾所需的框架内部发布。
    InternalOnly = 2,
    /// 全部入口关闭。
    Closed = 3,
}

/// lane 队列压力的一次无锁采样。
///
/// send 与 fire 共用 observer 额度；active 计数用于关闭责任，两者不能相加。
pub(crate) struct QueuePressure {
    /// send 与 fire 已占用的 observer permit 数。
    pub(crate) observer_in_use: usize,
    /// fire observer 容量（`fire_observer_capacity`）。
    pub(crate) observer_capacity: usize,
    /// send + fire 合计在途请求数，包括等待同步入队的责任。
    pub(crate) active: usize,
}

/// 一次发布调用传给底层 producer 的借用记录，生命周期只覆盖同步入队阶段。
pub(crate) struct OutboundRecord<'a> {
    /// 目标 topic。
    pub(crate) topic: &'a str,
    /// 可空 payload；`None` 表示 tombstone。
    pub(crate) payload: Option<&'a [u8]>,
    /// 可选原始 key 字节。
    pub(crate) key: Option<&'a [u8]>,
    /// 可选固定分区。
    pub(crate) partition: Option<i32>,
    /// 可选消息时间戳。
    pub(crate) timestamp: Option<i64>,
    /// 有序多值 headers。
    pub(crate) headers: &'a KafkaHeaders,
}

/// 单个 lane 的真实底层 producer 与有界投递观察域。
pub(crate) struct ProducerClient {
    /// lane 名称，用于错误归因。
    lane: Arc<str>,
    /// 可用于 fatal 后构造等价底层实例的冻结配置。
    native_config: ClientConfig,
    /// 当前底层 producer generation；替换只影响后续请求。
    native: RwLock<Option<NativeProducerSnapshot>>,
    /// 关闭后仍保留最后一次代次供诊断，不因此保留网络连接。
    native_generation: AtomicU64,
    /// 包含旧代次和阻塞 flush 快照的真实原生资源责任。
    native_lifetime: Arc<NativeLifetime>,
    /// 阻塞 flush 单飞；取消调用方等待不会开放另一个并行 flush。
    flush_gate: Arc<tokio::sync::Mutex<()>>,
    /// connect 时捕获的异步运行时。
    runtime: tokio::runtime::Handle,
    /// 源级有界异步观测入口，不直接执行用户后端。
    metrics: Arc<dyn MetricsSink>,
    /// 单次发布总超时。
    publish_timeout: Duration,
    /// 单条发布的完整原始字节上限，包含 key、payload 和所有头名与头值。
    max_record_bytes: u64,
    /// 框架实际交付责任的条数与字节上限，独立于 native 对缓冲的计费口径。
    max_pending_records: usize,
    max_pending_bytes: u64,
    /// 非 fatal 无成功 delivery 触发 generation 切换的持续时长。
    stalled_rebuild_after: Duration,
    /// 非 fatal generation 切换前的连续终态失败阈值。
    stalled_rebuild_min_failures: u32,
    /// 非 fatal generation 切换的最短冷却。
    stalled_rebuild_cooldown: Duration,
    /// 当前 generation 的成功/失败进展状态。
    native_recovery: Mutex<NativeRecoveryState>,
    /// 首个认证、授权、fencing 等永久错误；一旦出现就停止 generation 重建。
    terminal_fatal: TerminalMailbox,
    /// fire delivery 失败日志的固定窗口聚合状态。
    fire_failure_log: Mutex<FireFailureLogState>,
    /// 连续被判为认证类失败的次数；达到阈值才把 lane latch 成永久错误。
    auth_failures: AtomicU64,
    /// 当前准入阶段。
    phase: AtomicU8,
    /// 已取得准入但尚未完成 delivery 的请求数。
    active: AtomicUsize,
    /// 已准入请求的终态凭据，数量受observer额度约束；flush只冻结当前集合。
    admissions: Mutex<Admissions>,
    /// active 变化通知。
    active_notify: Notify,
    /// send 与 fire 共用的 delivery observer 硬容量。
    fire_slots: Arc<Semaphore>,
    /// fire delivery observer 的冻结容量。
    fire_capacity: usize,
    /// 成功同步入队并登记 observer 的 fire 累计数。
    fire_admitted: AtomicU64,
    /// observer permit 不足的累计数。
    observer_full: AtomicU64,
    /// 框架原始字节/条数额度或底层 native queue 满的累计数。
    native_queue_full: AtomicU64,
    /// 其他同步入队失败累计数。
    enqueue_failed: AtomicU64,
    /// fatal 或持续无成功 delivery 后成功重建的累计次数。
    native_restarted: AtomicU64,
    /// 非 fatal 无进展触发底层重建的累计次数。
    native_stalled_restarted: AtomicU64,
    /// broker delivery success 累计数。
    delivery_succeeded: AtomicU64,
    /// broker delivery 失败、超时或观察中断累计数。
    delivery_failed: AtomicU64,
    /// 原生已终结但远端是否写入仍无法确定的累计数，不包含调用方单独超时。
    terminal_delivery_unknown: AtomicU64,
}

/// 有界准入账本；完成凭据随请求结束从账本移除，已有屏障可继续观察其终态。
#[derive(Default)]
struct Admissions {
    next: u64,
    pending: BTreeMap<u64, Arc<AtomicU8>>,
    bytes: u64,
}

/// 调用时的发布责任集合，包含旧原生代次，不包含随后准入的请求。
pub(crate) struct ProducerBarrier {
    completions: Vec<Arc<AtomicU8>>,
}

/// 一代底层 producer 的稳定快照。
///
/// 在途请求持有自己的 `Arc`，因此 generation 切换不会提前销毁旧实例或篡改其 delivery 结果。
#[derive(Clone)]
struct NativeProducerSnapshot {
    /// 从一开始的单调 generation。
    generation: u64,
    /// 本 generation 的底层 producer。
    producer: Arc<NativeProducer>,
}

/// 所有 producer 代次的真实析构责任，独立于发布准入和调用方句柄寿命。
#[derive(Default)]
struct NativeLifetime {
    live: AtomicUsize,
    changed: Notify,
}

/// 原生 producer 及其析构凭据；最后一个快照退出后才确认后台线程与连接已释放。
struct NativeProducer {
    producer: Option<FutureProducer<ProducerClientContext>>,
    lifetime: Arc<NativeLifetime>,
}

impl NativeProducer {
    /// 业务作用：登记一个新代次的实际资源责任，覆盖后续所有借用快照。
    /// 参数说明：producer 为已构造的原生客户端；lifetime 为所属 lane 的总资源账本。
    /// 返回：独立可共享的原生资源；责任仅在最后一次真实析构结束后归还。
    fn track(
        producer: FutureProducer<ProducerClientContext>,
        lifetime: &Arc<NativeLifetime>,
    ) -> Arc<Self> {
        lifetime.live.fetch_add(1, Ordering::AcqRel);
        Arc::new(Self {
            producer: Some(producer),
            lifetime: Arc::clone(lifetime),
        })
    }
}

impl std::ops::Deref for NativeProducer {
    type Target = FutureProducer<ProducerClientContext>;

    /// 业务作用：在快照责任内借用原生发布能力，避免绕过资源账本复制客户端。
    /// 参数说明：无。返回：只在包装器存活期间有效的原生引用。
    fn deref(&self) -> &Self::Target {
        self.producer
            .as_ref()
            .expect("存活快照必须持有原生 producer")
    }
}

impl Drop for NativeProducer {
    /// 业务作用：等待原生析构真实结束后归还关闭责任，不能把发送队列为空视作连接已退出。
    /// 参数说明：无。返回：最后一个快照的后台线程及连接已释放后通知关闭等待者。
    fn drop(&mut self) {
        // FutureProducer 析构会等待原生 poll 线程；先析构再发布零占用，防止过早报告 Stopped。
        drop(self.producer.take());
        self.lifetime.live.fetch_sub(1, Ordering::AcqRel);
        self.lifetime.changed.notify_waiters();
    }
}

/// 当前 native generation 的有界恢复判断状态。
struct NativeRecoveryState {
    /// 状态所属 generation。
    generation: u64,
    /// 自最后一次成功后的连续终态 delivery failure 数。
    consecutive_failures: u32,
    /// 当前 generation 最近一次成功 delivery 的时刻。
    last_success_at: Instant,
    /// 最近一次非 fatal generation 切换时刻。
    last_rebuild_at: Instant,
}

/// fire delivery 失败日志窗口；累计计数仍以 `delivery_failed` 为准。
struct FireFailureLogState {
    /// 当前窗口第一次详细日志的时刻。
    detailed_at: Option<Instant>,
    /// 当前窗口被聚合而未逐条打印的失败数。
    suppressed: u64,
}

/// producer 后台线程的永久错误邮箱；callback 只做有界分类与单槽写入。
#[derive(Clone)]
struct ProducerClientContext {
    /// 首个认证、授权或 fencing 错误。
    terminal_fatal: TerminalMailbox,
    /// 回调所属原生代次，旧连接的迟到错误不取得新代次控制权。
    generation: u64,
}

impl ClientContext for ProducerClientContext {
    /// 业务作用：把后台连接错误中的永久类别写入单槽，阻止无进展策略重建已知无权写入的 lane。
    /// 参数说明：error 为客户端错误，_reason 为不对外传播的原生描述。
    /// 返回：只记录固定错误码，不携带 broker 返回的自由文本。
    fn error(&self, error: KafkaError, _reason: &str) {
        if let Some(code) = error
            .rdkafka_error_code()
            .filter(|code| is_fatal_producer_code(*code))
        {
            let detail = format!("Kafka connection error: {code:?}");
            let mut terminal = self
                .terminal_fatal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // 邮箱保留最高代次的首个永久证据；尚未发布的新客户端也能保存构造期间的回调。
            if terminal
                .as_ref()
                .is_none_or(|(generation, _, _)| *generation < self.generation)
            {
                *terminal = Some((self.generation, code, detail));
            }
        }
    }
}

impl ProducerClient {
    /// 业务作用：导出交付未决、远端未知和实际原生退出的关闭证据。
    /// 参数说明：无。
    /// 返回：非阻塞独立采样，只有永久关闭且资源与交付归零才报告 lane 退出。
    pub(crate) fn close_report(&self) -> crate::CloseLane {
        let stats = self.stats();
        crate::CloseLane {
            name: self.lane.to_string(),
            pending_delivery: stats.active_requests,
            terminal_delivery_unknown: self.terminal_delivery_unknown.load(Ordering::Acquire),
            live_native: stats.live_native,
            closed: self.phase.load(Ordering::Acquire) == LanePhase::Closed as u8
                && stats.active_requests == 0
                && stats.live_native == 0,
        }
    }
    /// 业务作用：创建独立原生 producer，并冻结本 lane 的准入与交付责任预算。
    /// 参数说明：config 为源配置；lane 为物理通道名；runtime 驱动异步观察；metrics 为源级异步指标出口，回执不直接调用用户后端。
    /// 返回：可接受有界发布的 lane；配置或原生构造失败返回错误。原生后台线程可开始连接，构造成功不证明 broker 交付可用。
    pub(crate) fn create(
        config: &KafkaConfig,
        lane: &str,
        runtime: tokio::runtime::Handle,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<Arc<Self>> {
        let effective = effective_producer_lane(config, lane)?;
        let created_at = Instant::now();
        let native_config = producer_config(config, lane)?;
        let terminal_fatal = Arc::new(Mutex::new(None));
        let native_lifetime = Arc::new(NativeLifetime::default());
        let producer = native_config
            .create_with_context::<_, FutureProducer<ProducerClientContext>>(
                ProducerClientContext {
                    terminal_fatal: Arc::clone(&terminal_fatal),
                    generation: 1,
                },
            )
            .map_err(|error| {
                NafkaError::Config(format!(
                    "producer lane `{lane}` 构造失败: {}",
                    super::config::redact(&error)
                ))
            })?;
        Ok(Arc::new(Self {
            lane: Arc::from(lane),
            native_config,
            native: RwLock::new(Some(NativeProducerSnapshot {
                generation: 1,
                producer: NativeProducer::track(producer, &native_lifetime),
            })),
            native_generation: AtomicU64::new(1),
            native_lifetime,
            flush_gate: Arc::new(tokio::sync::Mutex::new(())),
            runtime,
            metrics,
            publish_timeout: Duration::from_millis(config.behavior.publish_timeout_ms),
            max_record_bytes: config.behavior.max_record_bytes as u64,
            max_pending_records: effective.queue_buffering_max_messages,
            max_pending_bytes: effective.queue_buffering_max_kbytes as u64 * 1024,
            stalled_rebuild_after: Duration::from_millis(effective.stalled_rebuild_after_ms),
            stalled_rebuild_min_failures: effective.stalled_rebuild_min_failures,
            stalled_rebuild_cooldown: Duration::from_millis(effective.stalled_rebuild_cooldown_ms),
            native_recovery: Mutex::new(NativeRecoveryState {
                generation: 1,
                consecutive_failures: 0,
                last_success_at: created_at,
                last_rebuild_at: created_at,
            }),
            terminal_fatal,
            auth_failures: AtomicU64::new(0),
            fire_failure_log: Mutex::new(FireFailureLogState {
                detailed_at: None,
                suppressed: 0,
            }),
            phase: AtomicU8::new(LanePhase::Open as u8),
            active: AtomicUsize::new(0),
            admissions: Mutex::new(Admissions::default()),
            active_notify: Notify::new(),
            fire_slots: Arc::new(Semaphore::new(effective.fire_observer_capacity)),
            fire_capacity: effective.fire_observer_capacity,
            fire_admitted: AtomicU64::new(0),
            observer_full: AtomicU64::new(0),
            native_queue_full: AtomicU64::new(0),
            enqueue_failed: AtomicU64::new(0),
            native_restarted: AtomicU64::new(0),
            native_stalled_restarted: AtomicU64::new(0),
            delivery_succeeded: AtomicU64::new(0),
            delivery_failed: AtomicU64::new(0),
            terminal_delivery_unknown: AtomicU64::new(0),
        }))
    }

    /// 业务作用：返回该物理 lane 的只读容量与累计结果快照。
    /// 无锁读取队列压力：`(in_use, capacity)`。
    ///
    /// 参数说明：无。返回：只读取原子量与不可变容量，不等待 native generation 锁。
    pub(crate) fn queue_pressure(&self) -> QueuePressure {
        QueuePressure {
            observer_in_use: self
                .fire_capacity
                .saturating_sub(self.fire_slots.available_permits()),
            observer_capacity: self.fire_capacity,
            active: self.active.load(Ordering::Acquire),
        }
    }

    /// 业务作用：汇总当前 generation、准入阶段与累计结果计数，生成管理面 lane 快照。
    /// 参数说明：无。返回：最后一次原生代次及当前占用；读取关闭后的统计不保留原生连接。
    pub(crate) fn stats(&self) -> ProducerLaneStats {
        let observer_available = self.fire_slots.available_permits();
        let native_generation = self.native_generation.load(Ordering::Acquire);
        // 两类状态分别取快照并释放锁，避免管理面同时持有恢复窗口与永久证据的相反锁序。
        let terminal_fatal = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|(generation, _, _)| *generation == native_generation);
        let consecutive_failures = self
            .native_recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .consecutive_failures;
        ProducerLaneStats {
            lane: self.lane.to_string(),
            active_requests: self.active.load(Ordering::Acquire),
            observer_capacity: self.fire_capacity,
            observer_in_use: self.fire_capacity.saturating_sub(observer_available),
            fire_admitted: self.fire_admitted.load(Ordering::Relaxed),
            observer_full: self.observer_full.load(Ordering::Relaxed),
            native_queue_full: self.native_queue_full.load(Ordering::Relaxed),
            enqueue_failed: self.enqueue_failed.load(Ordering::Relaxed),
            native_generation,
            native_restarted: self.native_restarted.load(Ordering::Relaxed),
            terminal_fatal,
            native_stalled_restarted: self.native_stalled_restarted.load(Ordering::Relaxed),
            live_native: self.native_lifetime.live.load(Ordering::Acquire),
            consecutive_failures,
            delivery_succeeded: self.delivery_succeeded.load(Ordering::Relaxed),
            delivery_failed: self.delivery_failed.load(Ordering::Relaxed),
        }
    }

    /// 业务作用：让producer span与原生交付观察共同存活，调用方取消或超时不提前结束链路。
    /// 参数说明：record为编码后的记录，deadline为编码前冻结的期限，span为可选已采样记录器凭证。
    /// 返回：调用方交付结局；native接管后span随独立观察者完成。
    pub(crate) async fn send_traced(
        self: &Arc<Self>,
        record: OutboundRecord<'_>,
        deadline: Instant,
        span: Option<natelemetry::SpanGuard>,
    ) -> Result<Delivery> {
        self.send_with_completion(record, deadline, false, false, span)
            .await
    }

    /// 业务作用：由不可中断的框架 worker 持有实际交付责任，取消任务只关闭下一条准入。
    /// 参数说明：record 为批准的原文目的记录；调用者必须持续驱动本 future 到终态。
    /// 返回：native 实际回执；入队等待仍受发布预算约束，入队后不使用业务等待超时。
    pub(crate) async fn send_actual(
        self: &Arc<Self>,
        record: OutboundRecord<'_>,
    ) -> Result<Delivery> {
        self.send_with_completion(record, self.publish_deadline(), true, true, None)
            .await
    }

    /// 业务作用：统一原生准入和交付计数，并区分外部可取消等待与框架同步实际责任。
    /// 参数说明：record 为原文，deadline 为本次发布的统一期限，internal 允许关闭收尾，actual 指定不可中断 worker 的直接观察，span随真实回执收口。
    /// 返回：外部等待可超时；实际责任路径仅在原生回执终结后返回。
    async fn send_with_completion(
        self: &Arc<Self>,
        record: OutboundRecord<'_>,
        deadline: Instant,
        internal: bool,
        actual: bool,
        span: Option<natelemetry::SpanGuard>,
    ) -> Result<Delivery> {
        self.check_deadline(record.topic, deadline)?;
        let retained = self.validate_record(&record)?;
        let permit = self.reserve_observer(record.topic)?;
        let mut admission = self.admit(internal, record.topic, retained)?;
        self.report_pressure();
        let topic = record.topic.to_owned();
        let mut native = to_future_record(record);
        let mut producer = self.current_native()?;
        let delivery = loop {
            // 编码、代次构造和队列退避共享期限；未发出的消息不能在预算耗尽后进入 native。
            self.check_deadline(&topic, deadline)?;
            match producer.producer.send_result(native) {
                Ok(delivery) => break delivery,
                Err((error, returned)) if is_queue_full(&error) => {
                    self.native_queue_full.fetch_add(1, Ordering::Relaxed);
                    if Instant::now() >= deadline {
                        return Err(self.queue_full(&topic, ProducerQueue::Native));
                    }
                    native = returned;
                    producer = self.current_native()?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err((error, _)) => {
                    self.enqueue_failed.fetch_add(1, Ordering::Relaxed);
                    if let Some(fatal) = self.record_terminal_error(&topic, &error, &producer) {
                        return Err(fatal);
                    }
                    return Err(self.publish_native_error(
                        &topic,
                        format!("同步入队失败: {error}"),
                        &producer,
                    ));
                }
            }
        };
        // native接管后任何未走完的观察都只能视为未知；确认的回执会在责任退出前覆盖该状态。
        admission.outcome = 3;
        self.metrics.counter(
            "publish_admitted_total",
            1,
            &[("lane", &self.lane), ("topic", &topic)],
        );
        if actual {
            // 同步转发 worker 不会丢弃此 future；直接观察避免异步运行时撤销后台 observer 后提前归还来源 lease。
            let result = observe_delivery(self, &producer, &topic, delivery).await;
            if let Some(span) = span {
                let _ = span.finish(None);
            }
            admission.record_outcome(&result);
            drop(permit);
            drop(admission);
            self.report_pressure();
            return result;
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let owner = Arc::clone(self);
        let observed_topic = topic.clone();
        // native 已经接管记录；在下一个 await 之前移交全部责任，使调用方取消不能遗失回执。
        self.runtime.spawn(async move {
            let result = observe_delivery(&owner, &producer, &observed_topic, delivery).await;
            if let Some(span) = span {
                let _ = span.finish(None);
            }
            admission.record_outcome(&result);
            let _ = sender.send(result);
            drop(permit);
            drop(admission);
            owner.report_pressure();
        });
        match tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), receiver)
            .await
        {
            Ok(Ok(result)) => result,
            _ => {
                self.metrics
                    .counter("publish_wait_unknown_total", 1, &[("lane", &self.lane)]);
                Err(NafkaError::OutcomeUnknown {
                    lane: self.lane.to_string(),
                    topic,
                    message: "交付等待已结束，真实交付仍由独立 observer 收口".into(),
                })
            }
        }
    }

    /// 业务作用：把fire的span与已接纳原生消息一起移交给后台观察者，准入成功本身不结束span。
    /// 参数说明：record为编码记录，deadline为编码前冻结的期限，internal允许框架收尾，span为有限链路凭证。
    /// 返回：确定准入结果；已准入的span等实际交付终结后完成。
    pub(crate) fn fire_traced(
        self: &Arc<Self>,
        record: OutboundRecord<'_>,
        deadline: Instant,
        internal: bool,
        span: Option<natelemetry::SpanGuard>,
    ) -> Result<()> {
        self.check_deadline(record.topic, deadline)?;
        let retained = self.validate_record(&record)?;
        let permit = self.reserve_observer(record.topic)?;
        let mut admission = self.admit(internal, record.topic, retained)?;
        self.report_pressure();
        let topic = record.topic.to_owned();
        let native = self.current_native()?;
        let record = to_future_record(record);
        // 原生构造和头复制同样消耗本次预算，不能因 fire 不等待回执而绕过截止时间。
        self.check_deadline(&topic, deadline)?;
        let delivery = match native.producer.send_result(record) {
            Ok(delivery) => delivery,
            Err((error, _)) => {
                if is_queue_full(&error) {
                    self.native_queue_full.fetch_add(1, Ordering::Relaxed);
                    return Err(self.queue_full(&topic, ProducerQueue::Native));
                } else {
                    self.enqueue_failed.fetch_add(1, Ordering::Relaxed);
                }
                if let Some(fatal) = self.record_terminal_error(&topic, &error, &native) {
                    return Err(fatal);
                }
                return Err(self.publish_native_error(
                    &topic,
                    format!("同步入队失败: {error}"),
                    &native,
                ));
            }
        };
        admission.outcome = 3;
        self.fire_admitted.fetch_add(1, Ordering::Relaxed);
        self.metrics.counter(
            "publish_admitted_total",
            1,
            &[("lane", &self.lane), ("topic", &topic)],
        );
        let owner = Arc::clone(self);
        self.runtime.spawn(async move {
            observe_fire_delivery(owner, native, topic, delivery, admission, permit, span).await;
        });
        Ok(())
    }

    /// 业务作用：在编码开始前冻结本次发布的绝对期限。
    /// 参数说明：无。返回：编码、准入和调用方结果等待共用的单调时钟截止时间。
    pub(crate) fn publish_deadline(&self) -> Instant {
        Instant::now() + self.publish_timeout
    }

    /// 业务作用：拒绝尚未交给 native 且已经耗尽预算的消息。
    /// 参数说明：topic 为目标，deadline 为本次统一期限。
    /// 返回：仍有预算时通过；超时为确定未发送的发布失败，不冒充交付未知。
    fn check_deadline(&self, topic: &str, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(NafkaError::Publish {
                lane: self.lane.to_string(),
                topic: topic.to_owned(),
                message: "编码或原生准入前已超过发布期限".into(),
            });
        }
        Ok(())
    }

    /// 业务作用：在 native 头复制和入队前验证完整原文的共同发布边界。
    /// 参数说明：record 为普通消息或框架内部 DLT、Transfer 的最终输出。
    /// 返回：完整 key、payload、头名 UTF-8 与非空头值的字节和；非法头或超限不取得交付责任。
    fn validate_record(&self, record: &OutboundRecord<'_>) -> Result<u64> {
        let invalid = |message: &str| NafkaError::Publish {
            lane: self.lane.to_string(),
            topic: record.topic.to_owned(),
            message: message.to_owned(),
        };
        let mut retained = record
            .key
            .map_or(0, |v| v.len() as u64)
            .saturating_add(record.payload.map_or(0, |v| v.len() as u64));
        for header in record.headers.iter() {
            // C 字符串读取不能保留内嵌 NUL；输出必须在 native 转换前拒绝名称别名。
            if header.name.is_empty() || header.name.contains('\0') {
                return Err(invalid("header 名必须非空且不含 NUL"));
            }
            retained = retained
                .saturating_add(header.name.len() as u64)
                .saturating_add(header.value.as_ref().map_or(0, |v| v.len() as u64));
        }
        if retained > self.max_record_bytes {
            return Err(invalid("消息超过 max_record_bytes"));
        }
        Ok(retained)
    }

    /// 业务作用：在任何 native 写入前保留独立交付观察额度。
    /// 参数说明：topic 为确定拒绝时的目标归因。
    /// 返回：可移交给后台观察者的 permit；饱和时立即拒绝，不影响已有责任。
    fn reserve_observer(&self, topic: &str) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.fire_slots)
            .try_acquire_owned()
            .map_err(|_| {
                self.observer_full.fetch_add(1, Ordering::Relaxed);
                self.metrics.counter(
                    "publish_admission_rejected_total",
                    1,
                    &[("lane", &self.lane), ("reason", "observer_capacity")],
                );
                self.queue_full(topic, ProducerQueue::DeliveryObserver)
            })
    }

    /// 业务作用：从真实观察额度发布 lane 压力，send 与 fire 使用相同分母。
    /// 参数说明：无。
    /// 返回：向有界观测出口提交瞬时值；不等待后端。
    fn report_pressure(&self) {
        let pressure = self.queue_pressure();
        let labels = [("lane", self.lane.as_ref())];
        let percent = pressure
            .observer_in_use
            .min(pressure.observer_capacity)
            .saturating_mul(100)
            / pressure.observer_capacity.max(1);
        self.metrics.gauge(
            "producer_lane_queue_utilization",
            i64::try_from(percent).unwrap_or(100),
            &labels,
        );
        self.metrics.gauge(
            "producer_lane_in_flight",
            i64::try_from(pressure.active).unwrap_or(i64::MAX),
            &labels,
        );
    }

    /// 业务作用：拒绝新的业务发布，保留框架内部收尾入口。
    pub(crate) fn begin_draining(&self) {
        let _ = self.phase.compare_exchange(
            LanePhase::Open as u8,
            LanePhase::Draining as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// 业务作用：进入仅允许框架内部发布的收尾阶段。
    pub(crate) fn enter_internal_only(&self) {
        // 必须原子取最大值：并发 shutdown 下 load-then-store 可能被另一路的 close_until() 插入，
        // 把相位从 Closed 退回 InternalOnly，等于在已声明关闭之后重新放开内部准入。
        self.phase
            .fetch_max(LanePhase::InternalOnly as u8, Ordering::AcqRel);
    }

    /// 业务作用：冻结当前全部发布责任，避免持续流量让flush追逐未来请求。
    /// 参数说明：无。返回：仅引用此刻尚未收口的有界凭据，跨原生代次仍可观察。
    pub(crate) fn capture_barrier(&self) -> ProducerBarrier {
        let admissions = self
            .admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ProducerBarrier {
            completions: admissions.pending.values().cloned().collect(),
        }
    }

    /// 业务作用：等到冻结责任真实退出，再报告其中是否存在未成功交付。
    /// 参数说明：barrier为调用时凭据集合，deadline为所有lane共用期限。
    /// 返回：集合全部成功时成功，存在失败返回发布错误；超时或取消不取消原交付责任。
    pub(crate) async fn await_barrier(
        &self,
        barrier: ProducerBarrier,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            // 先登记通知再读完成凭据，避免最后一次退出恰好发生在检查与等待之间。
            let notified = self.active_notify.notified();
            if barrier
                .completions
                .iter()
                .all(|state| state.load(Ordering::Acquire) != 0)
            {
                if barrier
                    .completions
                    .iter()
                    .any(|state| state.load(Ordering::Acquire) == 3)
                {
                    return Err(NafkaError::OutcomeUnknown {
                        lane: self.lane.to_string(),
                        topic: "<flush>".into(),
                        message: "屏障责任已退出，但其中存在远端交付未知；不能据此自动重发".into(),
                    });
                }
                return if barrier
                    .completions
                    .iter()
                    .any(|state| state.load(Ordering::Acquire) == 2)
                {
                    Err(self.publish_error(
                        "<flush>",
                        "屏障责任已收口，其中存在未成功交付；结果由各消息凭据保留",
                    ))
                } else {
                    Ok(())
                };
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return Err(self.close_timeout());
            }
        }
    }

    /// 业务作用：在共同关闭期限内等待观察任务及原生队列排空。
    /// 参数说明：deadline 为异步等待与阻塞 flush 共用的绝对期限。
    /// 返回：已接纳发布排空时成功；等待超时或原生失败返回错误。超时、取消不终止已开始的 flush，也不释放其资源责任。
    pub(crate) async fn flush_until(self: &Arc<Self>, deadline: Instant) -> Result<()> {
        // 关停/排空是投递失败风暴的自然终点：把窗口尾部还没输出的聚合数补出来，
        // 否则风暴结束后最后一段 suppressed 永远不会被任何后续失败带出。
        self.flush_failure_log_tail();
        self.await_active(deadline).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.close_timeout());
        }
        // 单飞锁随阻塞工作转移；调用者取消等待后，实际 flush 退出之前不能累积新的后台任务。
        let gate = tokio::time::timeout(remaining, Arc::clone(&self.flush_gate).lock_owned())
            .await
            .map_err(|_| self.close_timeout())?;
        // terminal fatal 会拒绝后续发布，但不能阻止 shutdown 取得现有 generation 并排空。
        // 这里也不能调用 current_native()：它会把认证/授权终态直接返回为发布错误，导致
        // 实际已经没有在途消息的 lane 被上层误报成 ShutdownTimeout。
        let producer = self
            .native
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|native| Arc::clone(&native.producer));
        let Some(producer) = producer else {
            return Ok(());
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.close_timeout());
        }
        // 宿主的原运行时可能已按总预算退出；显式接续关闭由当前等待者的运行时执行，不能把清理投递到已停止的执行器。
        let joined = tokio::task::spawn_blocking(move || {
            let result = producer.flush(Timeout::After(remaining));
            drop(producer);
            drop(gate);
            result
        });
        match tokio::time::timeout(remaining, joined).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(self.publish_error("<flush>", error)),
            Ok(Err(error)) => Err(self.publish_error("<flush>", format!("阻塞任务失败: {error}"))),
            Err(_) => Err(self.close_timeout()),
        }
    }

    /// 业务作用：永久封闭全部准入，排干实际回执并释放所有原生代次，长期句柄只保留诊断状态。
    /// 参数说明：deadline 为整次停机共享的绝对截止时刻；仅在所有消费及转发责任退出后调用。
    /// 返回：原生客户端真实析构后成功；超时保留关闭态和资源责任，后续调用继续等待。
    pub(crate) async fn close_until(self: &Arc<Self>, deadline: Instant) -> Result<()> {
        // 消费者及转发已经退出，先永久关闭内部准入，再等待既有请求；不能出现排空与新发送竞态。
        self.phase.store(LanePhase::Closed as u8, Ordering::Release);
        self.flush_until(deadline).await?;
        let native = self
            .native
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(native) = native {
            // 析构由当前关闭调用的阻塞执行器承担；账本在真实退出后归零，取消等待不能提前完成关闭。
            tokio::task::spawn_blocking(move || drop(native));
        }
        loop {
            let notified = self.native_lifetime.changed.notified();
            if self.native_lifetime.live.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return Err(self.close_timeout());
            }
        }
    }

    /// 业务作用：等待已接纳发布的实际责任结束，调用者结束等待不撤销回执观察。
    /// 参数说明：deadline 为共享关闭预算。返回：active 归零或包含本 lane 的关闭超时。
    async fn await_active(&self, deadline: Instant) -> Result<()> {
        loop {
            // 先登记通知再复验计数，避免最后一个回执与关闭等待交错时遗失唤醒。
            let notified = self.active_notify.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return Err(self.close_timeout());
            }
        }
    }

    /// 业务作用：让关闭调用者识别仍未完成的物理发布域。
    /// 参数说明：无。返回：包含当前 lane 名的超时，不能解释为未发送或已释放连接。
    fn close_timeout(&self) -> NafkaError {
        NafkaError::ShutdownTimeout {
            groups: Vec::new(),
            producer_lanes: vec![self.lane.to_string()],
        }
    }

    /// 业务作用：拒绝关闭后重新构造或借用已经释放的原生客户端。
    /// 参数说明：无。返回：带本 lane 身份的生命周期错误。
    fn closed_error(&self) -> NafkaError {
        NafkaError::Lifecycle(format!("producer lane `{}` 已关闭原生资源入口", self.lane))
    }

    /// 业务作用：在发布前同时预留条数、字节和实际执行责任，并用 RAII 跟踪归还。
    /// 参数说明：internal 表示框架收尾发布；topic 用于拒绝归因；retained 为完整原文的计费字节。
    /// 返回：持有责任的准入凭据；阶段不允许、额度不足或序号耗尽时拒绝，收尾发布同样受容量约束。
    fn admit(
        self: &Arc<Self>,
        internal: bool,
        topic: &str,
        retained: u64,
    ) -> Result<AdmissionGuard> {
        let allowed = |phase| match phase {
            value if value == LanePhase::Open as u8 => true,
            value if value == LanePhase::Draining as u8 => internal,
            value if value == LanePhase::InternalOnly as u8 => internal,
            _ => false,
        };
        let first = self.phase.load(Ordering::Acquire);
        if !allowed(first) {
            return Err(NafkaError::Lifecycle(format!(
                "producer lane `{}` 已停止接收新发布",
                self.lane
            )));
        }
        let (sequence, completion) = {
            let mut admissions = self
                .admissions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // 框架口径覆盖 key 和头，不能依赖 native 的消息体缓冲计费；两种额度在同一锁内取得。
            let reason = if retained > self.max_pending_bytes - admissions.bytes {
                Some("byte_capacity")
            } else if admissions.pending.len() >= self.max_pending_records {
                Some("message_capacity")
            } else {
                None
            };
            if let Some(reason) = reason {
                drop(admissions);
                self.native_queue_full.fetch_add(1, Ordering::Relaxed);
                self.metrics.counter(
                    "publish_admission_rejected_total",
                    1,
                    &[("lane", &self.lane), ("reason", reason)],
                );
                return Err(self.queue_full(topic, ProducerQueue::Native));
            }
            let sequence = admissions
                .next
                .checked_add(1)
                .ok_or_else(|| self.closed_error())?;
            let completion = Arc::new(AtomicU8::new(0));
            admissions.next = sequence;
            admissions.pending.insert(sequence, Arc::clone(&completion));
            admissions.bytes += retained;
            self.active.fetch_add(1, Ordering::AcqRel);
            (sequence, completion)
        };
        let admission = AdmissionGuard {
            owner: Arc::clone(self),
            sequence,
            completion,
            outcome: 2,
            retained,
        };
        let second = self.phase.load(Ordering::Acquire);
        if !allowed(second) {
            drop(admission);
            return Err(NafkaError::Lifecycle(format!(
                "producer lane `{}` 正在关闭",
                self.lane
            )));
        }
        Ok(admission)
    }

    /// 业务作用：完成请求账本与active责任，并唤醒可能只等其子集的发布屏障。
    /// 参数说明：sequence为准入序号，retained为实际退出请求的原始字节。返回：归还同一责任的条数和字节，通知关闭和所有已冻结屏障。
    fn release_active(&self, sequence: u64, retained: u64) {
        let mut admissions = self
            .admissions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admissions.pending.remove(&sequence);
        admissions.bytes -= retained;
        drop(admissions);
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "producer active 计数不得下溢");
        self.active_notify.notify_waiters();
    }

    /// 业务作用：构造带 lane/topic 归因的确定性发布错误。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic 或内部操作名。
    /// - `message`: 已脱敏诊断信息。
    fn publish_error(&self, topic: &str, message: impl ToString) -> NafkaError {
        NafkaError::Publish {
            lane: self.lane.to_string(),
            topic: topic.to_owned(),
            message: message.to_string(),
        }
    }

    /// 业务作用：为已准入请求借用当前代次，关闭前允许按恢复策略切换，关闭后只保留既有责任。
    /// 参数说明：无。返回：仍存活的原生快照；永久错误或资源已释放时拒绝。
    fn current_native(&self) -> Result<NativeProducerSnapshot> {
        if let Some((_, code, detail)) = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .filter(|(generation, _, _)| {
                *generation == self.native_generation.load(Ordering::Acquire)
            })
        {
            if super::errors::authorization(code) {
                return Err(NafkaError::Authorization);
            }
            return Err(
                self.publish_error("<fatal>", format!("producer 永久错误 {code}: {detail}"))
            );
        }
        let observed = self
            .native
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| self.closed_error())?;
        if native_fatal_error(&observed).is_some() {
            self.replace_fatal_native(&observed)
        } else {
            Ok(observed)
        }
    }

    /// 业务作用：仅当观察代次仍是当前 fatal 代次且准入未关闭时替换客户端。
    /// 参数说明：observed 为失败请求实际使用的代次。返回：当前可用快照，关闭后禁止重建连接。
    fn replace_fatal_native(
        &self,
        observed: &NativeProducerSnapshot,
    ) -> Result<NativeProducerSnapshot> {
        let mut native = self
            .native
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = native.as_mut().ok_or_else(|| self.closed_error())?;
        // 与最终资源摘除共用写锁；关闭期间的迟到错误不能重新建立网络连接。
        if self.phase.load(Ordering::Acquire) == LanePhase::Closed as u8 {
            return Err(self.closed_error());
        }
        if current.generation != observed.generation {
            return Ok(current.clone());
        }
        let Some((code, detail)) = native_fatal_error(current) else {
            return Ok(current.clone());
        };
        if is_fatal_producer_code(code) {
            // 原生fatal状态可能先于后台错误回调可见；直接依据当前代次的永久码保护，不能抢先重建绕过证据。
            *self
                .terminal_fatal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((
                current.generation,
                code,
                format!("Kafka connection error: {code:?}"),
            ));
            return Err(self.publish_error("<fatal>", "producer 当前代次已进入永久保护"));
        }
        let replacement = self
            .native_config
            .create_with_context::<_, FutureProducer<ProducerClientContext>>(
                ProducerClientContext {
                    terminal_fatal: Arc::clone(&self.terminal_fatal),
                    generation: current.generation.saturating_add(1),
                },
            )
            .map_err(|error| {
                let error = super::config::redact(&error);
                self.publish_error(
                    "<native-rebuild>",
                    format!(
                        "底层 producer generation {} fatal ({code}: {detail})，重建失败: {error}",
                        current.generation
                    ),
                )
            })?;
        let previous = current.generation;
        let terminal = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 构造期间可能收到旧当前代次的永久拒绝；发布新代次前复验，不能用恢复绕过无权写入。
        if terminal
            .as_ref()
            .is_some_and(|(generation, _, _)| *generation == previous)
        {
            return Err(self.publish_error("<fatal>", "producer 当前代次已进入永久保护"));
        }
        *current = NativeProducerSnapshot {
            generation: previous.saturating_add(1),
            producer: NativeProducer::track(replacement, &self.native_lifetime),
        };
        self.native_generation
            .store(current.generation, Ordering::Release);
        self.auth_failures.store(0, Ordering::Release);
        drop(terminal);
        let now = Instant::now();
        *self
            .native_recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = NativeRecoveryState {
            generation: current.generation,
            consecutive_failures: 0,
            last_success_at: now,
            last_rebuild_at: now,
        };
        self.native_restarted.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            lane = %self.lane,
            previous_generation = previous,
            native_generation = current.generation,
            fatal_code = ?code,
            fatal_detail = %detail,
            "底层 producer fatal，已切换到新 generation"
        );
        Ok(current.clone())
    }

    /// 业务作用：在返回当前发布错误前尝试收敛 fatal generation，并追加不含凭据的诊断。
    fn native_error_detail(&self, message: String, observed: &NativeProducerSnapshot) -> String {
        let Some((code, detail)) = native_fatal_error(observed) else {
            return message;
        };
        match self.replace_fatal_native(observed) {
            Ok(next) => format!(
                "{message}; 底层 producer generation {} fatal ({code}: {detail})，后续请求已切换到 generation {}",
                observed.generation, next.generation
            ),
            Err(error) => format!(
                "{message}; 底层 producer generation {} fatal ({code}: {detail})，自动重建失败: {error}",
                observed.generation
            ),
        }
    }

    /// 业务作用：把底层错误与 fatal generation 诊断合并为稳定公共错误。
    fn publish_native_error(
        &self,
        topic: &str,
        message: String,
        observed: &NativeProducerSnapshot,
    ) -> NafkaError {
        self.publish_error(topic, self.native_error_detail(message, observed))
    }

    /// 业务作用：记录当前 generation 的成功 delivery，并清零连续失败窗口。
    fn note_delivery_success(&self, observed: &NativeProducerSnapshot) {
        let mut recovery = self
            .native_recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if recovery.generation == observed.generation {
            recovery.consecutive_failures = 0;
            recovery.last_success_at = Instant::now();
            let _terminal = self
                .terminal_fatal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.native_generation.load(Ordering::Acquire) == observed.generation {
                self.auth_failures.store(0, Ordering::Release);
            }
        }
    }

    /// 业务作用：记录非 fatal 终态 delivery failure，并判断是否达到无进展切换阈值。
    fn note_stalled_failure(&self, observed: &NativeProducerSnapshot) -> bool {
        let now = Instant::now();
        let mut recovery = self
            .native_recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if recovery.generation != observed.generation {
            return false;
        }
        recovery.consecutive_failures = recovery.consecutive_failures.saturating_add(1);
        recovery.consecutive_failures >= self.stalled_rebuild_min_failures
            && now.duration_since(recovery.last_success_at) >= self.stalled_rebuild_after
            && now.duration_since(recovery.last_rebuild_at) >= self.stalled_rebuild_cooldown
    }

    /// 业务作用：在写锁内复验非 fatal 无进展窗口，关闭后禁止恢复动作重建连接。
    /// 参数说明：observed 为实际失败的代次。返回：满足恢复条件时的新快照或无需恢复，关闭时拒绝。
    fn replace_stalled_native(
        &self,
        observed: &NativeProducerSnapshot,
    ) -> Result<Option<NativeProducerSnapshot>> {
        let mut native = self
            .native
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = native.as_mut().ok_or_else(|| self.closed_error())?;
        // 先复验最终关闭门禁，再考虑失败阈值；迟到回执只归还责任，不恢复业务入口。
        if self.phase.load(Ordering::Acquire) == LanePhase::Closed as u8 {
            return Err(self.closed_error());
        }
        if current.generation != observed.generation {
            return Ok(Some(current.clone()));
        }
        let now = Instant::now();
        let mut recovery = self
            .native_recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let eligible = recovery.generation == observed.generation
            && recovery.consecutive_failures >= self.stalled_rebuild_min_failures
            && now.duration_since(recovery.last_success_at) >= self.stalled_rebuild_after
            && now.duration_since(recovery.last_rebuild_at) >= self.stalled_rebuild_cooldown;
        if !eligible {
            return Ok(None);
        }
        let failures = recovery.consecutive_failures;
        let stalled_for = now.duration_since(recovery.last_success_at);
        let replacement = self
            .native_config
            .create_with_context::<_, FutureProducer<ProducerClientContext>>(
                ProducerClientContext {
                    terminal_fatal: Arc::clone(&self.terminal_fatal),
                    generation:current.generation.saturating_add(1),
                },
            )
            .map_err(|error| {
                let error = super::config::redact(&error);
                self.publish_error(
                    "<native-rebuild>",
                    format!(
                        "底层 producer generation {} 连续 {failures} 次 delivery failure 且 {}ms 无成功，重建失败: {error}",
                        current.generation,
                        stalled_for.as_millis()
                    ),
                )
            })?;
        let previous = current.generation;
        let terminal = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 代次发布与永久证据在同一门禁复验，迟到旧回执不能把已切换的lane重新保护。
        if terminal
            .as_ref()
            .is_some_and(|(generation, _, _)| *generation == previous)
        {
            return Err(self.publish_error("<fatal>", "producer 当前代次已进入永久保护"));
        }
        *current = NativeProducerSnapshot {
            generation: previous.saturating_add(1),
            producer: NativeProducer::track(replacement, &self.native_lifetime),
        };
        self.native_generation
            .store(current.generation, Ordering::Release);
        self.auth_failures.store(0, Ordering::Release);
        drop(terminal);
        *recovery = NativeRecoveryState {
            generation: current.generation,
            consecutive_failures: 0,
            last_success_at: now,
            last_rebuild_at: now,
        };
        self.native_restarted.fetch_add(1, Ordering::Relaxed);
        self.native_stalled_restarted
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            lane = %self.lane,
            previous_generation = previous,
            native_generation = current.generation,
            consecutive_failures = failures,
            stalled_ms = stalled_for.as_millis(),
            "底层 producer 持续无成功 delivery，已切换到新 generation"
        );
        Ok(Some(current.clone()))
    }

    /// 业务作用：合并终态 delivery failure、fatal 与非 fatal 无进展切换诊断。
    fn delivery_error_detail(&self, message: String, observed: &NativeProducerSnapshot) -> String {
        if native_fatal_error(observed).is_some() {
            return self.native_error_detail(message, observed);
        }
        if !self.note_stalled_failure(observed) {
            return message;
        }
        match self.replace_stalled_native(observed) {
            Ok(Some(next)) if next.generation != observed.generation => format!(
                "{message}; 底层 producer generation {} 持续无成功 delivery，后续请求已切换到 generation {}",
                observed.generation, next.generation
            ),
            Ok(_) => message,
            Err(error) => format!(
                "{message}; 底层 producer generation {} 持续无成功 delivery，自动重建失败: {error}",
                observed.generation
            ),
        }
    }

    /// 业务作用：把 delivery 错误和两级 generation 恢复诊断合并为稳定公共错误。
    fn publish_delivery_error(
        &self,
        topic: &str,
        message: String,
        observed: &NativeProducerSnapshot,
    ) -> NafkaError {
        self.publish_error(topic, self.delivery_error_detail(message, observed))
    }

    /// 业务作用：记录认证、授权、fencing 等永久 producer 错误，禁止随后按无进展策略循环重建。
    ///
    /// - `topic`: 当前发布目标，用于公共错误归因。
    /// - `error`: delivery 或同步入队返回的底层稳定错误。
    ///
    /// 参数说明：topic为目标，error为原生证据，observed为该请求实际使用的代次。
    /// 返回：当前代次达到永久保护条件时返回错误；旧代次不改变新代次保护或认证计数。
    fn record_terminal_error(
        &self,
        topic: &str,
        error: &KafkaError,
        observed: &NativeProducerSnapshot,
    ) -> Option<NafkaError> {
        let mut terminal = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 原生错误仍返回其请求调用者，但只有当前代次可以累计认证证据或封闭后续发布。
        if self.native_generation.load(Ordering::Acquire) != observed.generation {
            return error
                .rdkafka_error_code()
                .is_some_and(super::errors::authorization)
                .then_some(NafkaError::Authorization);
        }
        let code = error.rdkafka_error_code()?;
        if is_recoverable_auth_code(code) {
            // librdkafka 把「broker 明确拒绝认证(code 58)」和「token 刷新失败 / auth 阶段
            // POLLHUP / 滚动重启」都映射成 `_AUTHENTICATION`(rdkafka_request.c:3388)，
            // 无法从错误码本身区分。因此不立即 latch(否则一次抖动永久打死 lane)，
            // 也不永不 latch(否则口令配错会无限重建)：连续 AUTH_FAILURE_LATCH 次才判永久。
            let seen = self.auth_failures.fetch_add(1, Ordering::AcqRel) + 1;
            if seen < AUTH_FAILURE_LATCH {
                return None;
            }
        } else if !is_fatal_producer_code(code) {
            return None;
        }
        let detail = error.to_string();
        if terminal
            .as_ref()
            .is_none_or(|(generation, _, _)| *generation < observed.generation)
        {
            *terminal = Some((observed.generation, code, detail.clone()));
        }
        Some(if super::errors::authorization(code) {
            NafkaError::Authorization
        } else {
            self.publish_error(topic, format!("producer 永久错误 {code}: {detail}"))
        })
    }

    /// 业务作用：决定本次 fire delivery 失败是否打印详细日志，并返回上一窗口聚合数。
    fn fire_failure_log_decision(&self) -> (bool, u64) {
        const WINDOW: Duration = Duration::from_secs(10);
        let now = Instant::now();
        let mut state = self
            .fire_failure_log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .detailed_at
            .is_none_or(|started| now.duration_since(started) >= WINDOW)
        {
            let suppressed = std::mem::take(&mut state.suppressed);
            state.detailed_at = Some(now);
            (true, suppressed)
        } else {
            state.suppressed = state.suppressed.saturating_add(1);
            (false, 0)
        }
    }

    /// 业务作用：输出并清零当前窗口累计的被抑制失败条数。
    ///
    /// 由排空/关停调用：失败风暴停止后不会再有新的 delivery 失败把尾段带出来。
    fn flush_failure_log_tail(&self) {
        let suppressed = {
            let mut state = self
                .fire_failure_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut state.suppressed)
        };
        if suppressed != 0 {
            tracing::warn!(
                lane = %self.lane,
                suppressed,
                "异步发布失败日志已按窗口聚合(尾段补发)"
            );
        }
    }

    /// 业务作用：构造确定未入队的容量错误，供上层只按 variant 映射背压。
    fn queue_full(&self, topic: &str, queue: ProducerQueue) -> NafkaError {
        NafkaError::ProducerQueueFull {
            lane: self.lane.to_string(),
            topic: topic.to_owned(),
            queue,
        }
    }
}

/// 一次已取得的 lane 发布资格。
struct AdmissionGuard {
    /// 负责 active 计数的 lane。
    owner: Arc<ProducerClient>,
    sequence: u64,
    completion: Arc<AtomicU8>,
    outcome: u8,
    /// 随实际观察者存活的原始字节，调用方取消等待不能归还。
    retained: u64,
}

impl AdmissionGuard {
    /// 业务作用：保存真实回执类别，屏障不得把交付未知降为确定失败。
    /// 参数说明：result为原生交付观察的最终结果。返回：只更新待发布终态，凭据仍在实际退出时才可见。
    fn record_outcome(&mut self, result: &Result<Delivery>) {
        self.outcome = match result {
            Ok(_) => 1,
            Err(NafkaError::OutcomeUnknown { .. }) => 3,
            Err(_) => 2,
        };
    }
}

impl Drop for AdmissionGuard {
    /// 业务作用：释放发布资格并通知关闭排空逻辑。
    fn drop(&mut self) {
        // 完成凭据只在真实责任退出时发布，不能把调用方取消或oneshot发送当成交付完成。
        self.completion.store(self.outcome, Ordering::Release);
        self.owner.release_active(self.sequence, self.retained);
    }
}

/// 业务作用：把公共自有 headers 转换为底层拥有型 headers。
///
/// # 参数
///
/// - `headers`: 保序且允许同名重复、可空值的公共 headers。
fn to_owned_headers(headers: &KafkaHeaders) -> OwnedHeaders {
    headers.iter().fold(
        OwnedHeaders::new_with_capacity(headers.len()),
        |owned, item| {
            owned.insert(Header {
                key: &item.name,
                value: item.value.as_deref(),
            })
        },
    )
}

/// 业务作用：构造借用数据的底层发布记录；底层同步入队时完成必要复制。
///
/// # 参数
///
/// - `record`: 自有隔离层发布记录。
fn to_future_record<'a>(record: OutboundRecord<'a>) -> FutureRecord<'a, [u8], [u8]> {
    let mut native =
        FutureRecord::<[u8], [u8]>::to(record.topic).headers(to_owned_headers(record.headers));
    if let Some(payload) = record.payload {
        native = native.payload(payload);
    }
    if let Some(key) = record.key {
        native = native.key(key);
    }
    if let Some(partition) = record.partition {
        native = native.partition(partition);
    }
    if let Some(timestamp) = record.timestamp {
        native = native.timestamp(timestamp);
    }
    native
}

/// 业务作用：返回指定底层 generation 的 fatal 诊断。
///
/// # 参数
///
/// - `native`: 待检查的稳定 generation 快照。
fn native_fatal_error(native: &NativeProducerSnapshot) -> Option<(RDKafkaErrorCode, String)> {
    native.producer.client().fatal_error()
}

/// 连续多少次认证类失败后把 lane latch 成永久错误。
///
/// 取值权衡：太小则一次 token 刷新抖动或 broker 滚动重启就永久打死 lane；
/// 太大则口令配错时无限重建、运维看不到确定信号。一次成功投递即清零。
const AUTH_FAILURE_LATCH: u64 = 5;

/// 业务作用：判断错误码是否属于"可能可恢复、也可能是永久拒绝"的认证类。
///
/// librdkafka 把 broker 明确拒绝(code 58)与 token 刷新失败、auth 阶段 POLLHUP、
/// SASL 重认证失败全部映射成同一个 `_AUTHENTICATION`(rdkafka_request.c:3388)，
/// 无法从错误码本身区分，只能靠连续次数裁决（见 `record_terminal_error`）。
///
/// # 参数
///
/// - `code`: librdkafka 稳定错误码。
fn is_recoverable_auth_code(code: RDKafkaErrorCode) -> bool {
    matches!(code, RDKafkaErrorCode::Authentication)
}

/// 业务作用：判断 producer 错误码是否永久不可恢复；与 consumer 分类保持同一安全边界。
///
/// 本函数只收「码值唯一、语义确定」的永久错误。`Authentication` 不在其中：
/// 它由 `is_recoverable_auth_code` 走有界 latch，避免一次抖动永久打死 lane。
///
/// # 参数
///
/// - `code`: librdkafka 稳定错误码。
fn is_fatal_producer_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::InvalidArgument
            | RDKafkaErrorCode::SSL
            | RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::UnsupportedFeature
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            | RDKafkaErrorCode::UnsupportedSASLMechanism
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
            | RDKafkaErrorCode::ProducerFenced
            | RDKafkaErrorCode::FencedInstanceId
            | RDKafkaErrorCode::FencedMemberEpoch
    )
}

/// 业务作用：判断同步入队错误是否为可退避的本地队列满。
///
/// # 参数
///
/// - `error`: 底层入队错误。
fn is_queue_full(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    )
}

/// 业务作用：持续观察真实 native 回执，并在观察终态时记录一次交付结果。
///
/// 参数说明：owner 为所属 lane；native 为入队时的 producer 代次；topic 为目标；delivery 为原生回执。
/// 返回：已确认的交付坐标或失败；此等待不受业务调用方的超时或取消影响。
async fn observe_delivery(
    owner: &ProducerClient,
    native: &NativeProducerSnapshot,
    topic: &str,
    delivery: rdkafka::producer::future_producer::DeliveryFuture,
) -> Result<Delivery> {
    let result = match delivery.await {
        Ok(Ok(report)) => {
            owner.note_delivery_success(native);
            owner.delivery_succeeded.fetch_add(1, Ordering::Relaxed);
            Ok(Delivery {
                partition: report.partition,
                offset: report.offset,
            })
        }
        Ok(Err((error, _message))) => {
            owner.delivery_failed.fetch_add(1, Ordering::Relaxed);
            Err(owner
                .record_terminal_error(topic, &error, native)
                .unwrap_or_else(|| {
                    // 原生终态只结束本地责任；超时、断连或追加后的ISR不足不能证明broker没有写入。
                    if matches!(
                        error.rdkafka_error_code(),
                        Some(
                            RDKafkaErrorCode::MessageTimedOut
                                | RDKafkaErrorCode::RequestTimedOut
                                | RDKafkaErrorCode::OperationTimedOut
                                | RDKafkaErrorCode::BrokerTransportFailure
                                | RDKafkaErrorCode::AllBrokersDown
                                | RDKafkaErrorCode::PurgeInflight
                                | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
                        )
                    ) {
                        return NafkaError::OutcomeUnknown {
                            lane: owner.lane.to_string(),
                            topic: topic.to_owned(),
                            message: owner.delivery_error_detail(
                                format!("原生交付终结但远端写入不可确定: {error}"),
                                native,
                            ),
                        };
                    }
                    owner.publish_delivery_error(topic, format!("broker 投递失败: {error}"), native)
                }))
        }
        Err(error) => {
            owner.delivery_failed.fetch_add(1, Ordering::Relaxed);
            Err(NafkaError::OutcomeUnknown {
                lane: owner.lane.to_string(),
                topic: topic.to_owned(),
                message: owner
                    .delivery_error_detail(format!("delivery 观察通道中断: {error}"), native),
            })
        }
    };
    if matches!(&result, Err(NafkaError::OutcomeUnknown { .. })) {
        owner
            .terminal_delivery_unknown
            .fetch_add(1, Ordering::Relaxed);
        owner.metrics.counter(
            "publish_delivery_unknown_total",
            1,
            &[("lane", owner.lane.as_ref())],
        );
    }
    let labels = [("lane", owner.lane.as_ref()), ("topic", topic)];
    owner.metrics.counter(
        if result.is_ok() {
            "published_total"
        } else {
            "publish_failed_total"
        },
        1,
        &labels,
    );
    result
}

/// 业务作用：让 fire 交付保持独立观察责任，失败日志按窗口有界聚合。
/// 参数说明：owner 为 lane；native 为入队代次；topic 为目标；delivery 为回执；admission 与 permit 为关闭责任及容量，span随实际回执结束。
/// 返回：真实回执终结后才释放观察额度，入队成功本身不记为交付成功。
async fn observe_fire_delivery(
    owner: Arc<ProducerClient>,
    native: NativeProducerSnapshot,
    topic: String,
    delivery: rdkafka::producer::future_producer::DeliveryFuture,
    mut admission: AdmissionGuard,
    permit: OwnedSemaphorePermit,
    span: Option<natelemetry::SpanGuard>,
) {
    let result = observe_delivery(&owner, &native, &topic, delivery).await;
    if let Some(span) = span {
        let _ = span.finish(None);
    }
    admission.record_outcome(&result);
    if let Err(error) = result {
        let (emit_detail, suppressed) = owner.fire_failure_log_decision();
        if suppressed != 0 {
            tracing::warn!(lane = %owner.lane, suppressed, "异步发布失败日志已按窗口聚合");
        }
        if emit_detail {
            tracing::error!(lane = %owner.lane, topic, error = %error, "异步发布投递未确认成功");
        }
    }
    drop(permit);
    drop(admission);
    owner.report_pressure();
}
