//! 每 lane 独立 producer、准入状态机和 delivery observer。

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

/// 借用到同步入队完成的底层发布记录。
/// lane 队列压力的一次无锁采样。
///
/// `active` 与 observer 占用**必须分开报**：fire 路径既占 observer permit 又计入
/// `active`，相加就是同一条在途消息数两次——容量 10000 时 5001 路并发 fire 会算出
/// 10002 并被 clamp 成 100%，即"半载报满"，>90% 告警在真实半载时就响。
/// 且 send 路径只动 `active`、根本不碰 fire_slots，两者不是同一量纲。
pub(crate) struct QueuePressure {
    /// fire observer 已占用的 permit 数——lane 上唯一真正有界的资源。
    pub(crate) observer_in_use: usize,
    /// fire observer 容量（`fire_observer_capacity`）。
    pub(crate) observer_capacity: usize,
    /// send + fire 合计在途请求数；send 路径无队列上界，故只作绝对值上报。
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
    native: RwLock<NativeProducerSnapshot>,
    /// connect 时捕获的异步运行时。
    runtime: tokio::runtime::Handle,
    /// 单次发布总超时。
    publish_timeout: Duration,
    /// 非 fatal 无成功 delivery 触发 generation 切换的持续时长。
    stalled_rebuild_after: Duration,
    /// 非 fatal generation 切换前的连续终态失败阈值。
    stalled_rebuild_min_failures: u32,
    /// 非 fatal generation 切换的最短冷却。
    stalled_rebuild_cooldown: Duration,
    /// 当前 generation 的成功/失败进展状态。
    native_recovery: Mutex<NativeRecoveryState>,
    /// 首个认证、授权、fencing 等永久错误；一旦出现就停止 generation 重建。
    terminal_fatal: Arc<Mutex<Option<(RDKafkaErrorCode, String)>>>,
    /// fire delivery 失败日志的固定窗口聚合状态。
    fire_failure_log: Mutex<FireFailureLogState>,
    /// 连续被判为认证类失败的次数；达到阈值才把 lane latch 成永久错误。
    auth_failures: AtomicU64,
    /// 当前准入阶段。
    phase: AtomicU8,
    /// 已取得准入但尚未完成 delivery 的请求数。
    active: AtomicUsize,
    /// active 变化通知。
    active_notify: Notify,
    /// fire delivery observer 的硬容量。
    fire_slots: Arc<Semaphore>,
    /// fire delivery observer 的冻结容量。
    fire_capacity: usize,
    /// 成功同步入队并登记 observer 的 fire 累计数。
    fire_admitted: AtomicU64,
    /// observer permit 不足的累计数。
    observer_full: AtomicU64,
    /// 底层 native queue 同步返回满的累计数。
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
}

/// 一代底层 producer 的稳定快照。
///
/// 在途请求持有自己的 `Arc`，因此 generation 切换不会提前销毁旧实例或篡改其 delivery 结果。
#[derive(Clone)]
struct NativeProducerSnapshot {
    /// 从一开始的单调 generation。
    generation: u64,
    /// 本 generation 的底层 producer。
    producer: Arc<FutureProducer<ProducerClientContext>>,
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
    terminal_fatal: Arc<Mutex<Option<(RDKafkaErrorCode, String)>>>,
}

impl ClientContext for ProducerClientContext {
    /// 把后台连接错误中的永久类别写入单槽，避免 60 秒无进展策略无限重建错误凭据。
    fn error(&self, error: KafkaError, reason: &str) {
        if let Some(code) = error
            .rdkafka_error_code()
            .filter(|code| is_fatal_producer_code(*code))
        {
            let detail = format!("{error}: {reason}");
            self.terminal_fatal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert((code, detail));
        }
    }
}

impl ProducerClient {
    /// 构造并让底层解析指定 lane 的完整配置。
    ///
    /// # 参数
    ///
    /// - `config`: 冻结后的顶层配置。
    /// - `lane`: 物理 lane 名称。
    /// - `runtime`: 异步运行时句柄。
    ///
    /// # 错误
    ///
    /// lane 配置非法或底层 producer 构造失败时返回错误。
    pub(crate) fn create(
        config: &KafkaConfig,
        lane: &str,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>> {
        let effective = effective_producer_lane(config, lane)?;
        let created_at = Instant::now();
        let native_config = producer_config(config, lane)?;
        let terminal_fatal = Arc::new(Mutex::new(None));
        let producer = native_config
            .create_with_context::<_, FutureProducer<ProducerClientContext>>(
                ProducerClientContext {
                    terminal_fatal: Arc::clone(&terminal_fatal),
                },
            )
            .map_err(|error| {
                NafkaError::Config(format!("producer lane `{lane}` 构造失败: {error}"))
            })?;
        Ok(Arc::new(Self {
            lane: Arc::from(lane),
            native_config,
            native: RwLock::new(NativeProducerSnapshot {
                generation: 1,
                producer: Arc::new(producer),
            }),
            runtime,
            publish_timeout: Duration::from_millis(config.behavior.publish_timeout_ms),
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
        }))
    }

    /// 返回该物理 lane 的只读容量与累计结果快照。
    /// 无锁读取队列压力：`(in_use, capacity)`。
    ///
    /// 专供每条消息都会走的指标上报使用。`stats()` 会取 native generation 的读锁，
    /// 放在 `fire()` 这条高频路径上等于给每条消息加一次锁竞争（还要与 generation 切换的
    /// 写锁争用）。这里只读原子量与不可变容量，不碰 native。
    pub(crate) fn queue_pressure(&self) -> QueuePressure {
        QueuePressure {
            observer_in_use: self
                .fire_capacity
                .saturating_sub(self.fire_slots.available_permits()),
            observer_capacity: self.fire_capacity,
            active: self.active.load(Ordering::Acquire),
        }
    }

    /// 汇总当前 generation、准入阶段与累计结果计数，生成管理面 lane 快照。
    pub(crate) fn stats(&self) -> ProducerLaneStats {
        let observer_available = self.fire_slots.available_permits();
        let native_generation = self
            .native
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation;
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
            terminal_fatal: self
                .terminal_fatal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some(),
            native_stalled_restarted: self.native_stalled_restarted.load(Ordering::Relaxed),
            delivery_succeeded: self.delivery_succeeded.load(Ordering::Relaxed),
            delivery_failed: self.delivery_failed.load(Ordering::Relaxed),
        }
    }

    /// 等待一条消息同步入队并取得最终 delivery report。
    ///
    /// # 参数
    ///
    /// - `record`: 借用发布记录。
    /// - `internal`: 是否为关闭收尾所需的框架内部发布。
    ///
    /// # 错误
    ///
    /// 准入关闭、队列持续满、delivery 失败或结果超时时返回带 lane/topic 的错误。
    pub(crate) async fn send(
        self: &Arc<Self>,
        record: OutboundRecord<'_>,
        internal: bool,
    ) -> Result<Delivery> {
        let _admission = self.admit(internal)?;
        let topic = record.topic.to_owned();
        let deadline = Instant::now() + self.publish_timeout;
        let mut native = to_future_record(record);
        let mut producer = self.current_native()?;
        let delivery = loop {
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
                    if let Some(fatal) = self.record_terminal_error(&topic, &error) {
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
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, delivery).await {
            Ok(Ok(Ok(delivery))) => {
                self.note_delivery_success(&producer);
                self.delivery_succeeded.fetch_add(1, Ordering::Relaxed);
                self.auth_failures.store(0, Ordering::Release);
                Ok(Delivery {
                    partition: delivery.partition,
                    offset: delivery.offset,
                })
            }
            Ok(Ok(Err((error, _message)))) => {
                self.delivery_failed.fetch_add(1, Ordering::Relaxed);
                if let Some(fatal) = self.record_terminal_error(&topic, &error) {
                    return Err(fatal);
                }
                Err(self.publish_delivery_error(
                    &topic,
                    format!("broker 投递失败: {error}"),
                    &producer,
                ))
            }
            Ok(Err(error)) => {
                self.delivery_failed.fetch_add(1, Ordering::Relaxed);
                let message = self
                    .delivery_error_detail(format!("delivery 观察通道中断: {error}"), &producer);
                Err(NafkaError::OutcomeUnknown {
                    lane: self.lane.to_string(),
                    topic,
                    message,
                })
            }
            Err(_) => {
                self.delivery_failed.fetch_add(1, Ordering::Relaxed);
                let message = self.delivery_error_detail(
                    "等待 delivery report 超时，消息可能已经成功".into(),
                    &producer,
                );
                Err(NafkaError::OutcomeUnknown {
                    lane: self.lane.to_string(),
                    topic,
                    message,
                })
            }
        }
    }

    /// 同步入队并登记有界异步 delivery 观察。
    ///
    /// # 参数
    ///
    /// - `record`: 借用发布记录。
    /// - `internal`: 是否为框架内部发布。
    ///
    /// # 错误
    ///
    /// 观察槽不足、准入关闭或同步入队失败时返回确定性错误。
    pub(crate) fn fire(self: &Arc<Self>, record: OutboundRecord<'_>, internal: bool) -> Result<()> {
        let permit = match Arc::clone(&self.fire_slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.observer_full.fetch_add(1, Ordering::Relaxed);
                return Err(self.queue_full(record.topic, ProducerQueue::DeliveryObserver));
            }
        };
        let admission = self.admit(internal)?;
        let topic = record.topic.to_owned();
        let native = self.current_native()?;
        let delivery = match native.producer.send_result(to_future_record(record)) {
            Ok(delivery) => delivery,
            Err((error, _)) => {
                if is_queue_full(&error) {
                    self.native_queue_full.fetch_add(1, Ordering::Relaxed);
                    return Err(self.queue_full(&topic, ProducerQueue::Native));
                } else {
                    self.enqueue_failed.fetch_add(1, Ordering::Relaxed);
                }
                if let Some(fatal) = self.record_terminal_error(&topic, &error) {
                    return Err(fatal);
                }
                return Err(self.publish_native_error(
                    &topic,
                    format!("同步入队失败: {error}"),
                    &native,
                ));
            }
        };
        self.fire_admitted.fetch_add(1, Ordering::Relaxed);
        let owner = Arc::clone(self);
        self.runtime.spawn(async move {
            observe_fire_delivery(owner, native, topic, delivery, admission, permit).await;
        });
        Ok(())
    }

    /// 拒绝新的业务发布，保留框架内部收尾入口。
    pub(crate) fn begin_draining(&self) {
        let _ = self.phase.compare_exchange(
            LanePhase::Open as u8,
            LanePhase::Draining as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// 进入仅允许框架内部发布的收尾阶段。
    pub(crate) fn enter_internal_only(&self) {
        // 必须原子取最大值：并发 shutdown 下 load-then-store 可能被另一路的 close() 插入，
        // 把相位从 Closed 退回 InternalOnly，等于在已声明关闭之后重新放开内部准入。
        self.phase
            .fetch_max(LanePhase::InternalOnly as u8, Ordering::AcqRel);
    }

    /// 在绝对截止时刻前等待观察任务并排空底层队列。
    ///
    /// # 参数
    ///
    /// - `deadline`: 全局关闭绝对截止时刻。
    ///
    /// # 错误
    ///
    /// 等待 active 清零、阻塞 flush 或底层排空失败时返回错误。
    pub(crate) async fn flush_until(self: &Arc<Self>, deadline: Instant) -> Result<()> {
        // 关停/排空是投递失败风暴的自然终点：把窗口尾部还没输出的聚合数补出来，
        // 否则风暴结束后最后一段 suppressed 永远不会被任何后续失败带出。
        self.flush_failure_log_tail();
        while self.active.load(Ordering::Acquire) != 0 {
            // notified() 必须先于 double-check 创建：它在**构造那一刻**就快照了 notify_waiters
            // 调用计数，首次 poll 时比对计数即可发现"通知发生在构造之后"。因此落在
            // double-check 与 .await 之间的 notify_waiters 不会丢失，排空完成不会被误报成
            // ShutdownTimeout。调整这两行的顺序、或改用不留 permit 的通知方式都会引入真实竞态。
            let notified = self.active_notify.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return Err(NafkaError::ShutdownTimeout {
                    groups: Vec::new(),
                    producer_lanes: vec![self.lane.to_string()],
                });
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(NafkaError::ShutdownTimeout {
                groups: Vec::new(),
                producer_lanes: vec![self.lane.to_string()],
            });
        }
        // terminal fatal 会拒绝后续发布，但不能阻止 shutdown 取得现有 generation 并排空。
        // 这里也不能调用 current_native()：它会把认证/授权终态直接返回为发布错误，导致
        // 实际已经没有在途消息的 lane 被上层误报成 ShutdownTimeout。
        let producer = self
            .native
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .producer
            .clone();
        let joined = tokio::task::spawn_blocking(move || producer.flush(Timeout::After(remaining)));
        match tokio::time::timeout(remaining, joined).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(self.publish_error("<flush>", error)),
            Ok(Err(error)) => Err(self.publish_error("<flush>", format!("阻塞任务失败: {error}"))),
            Err(_) => Err(NafkaError::ShutdownTimeout {
                groups: Vec::new(),
                producer_lanes: vec![self.lane.to_string()],
            }),
        }
    }

    /// 在底层队列已排空后永久关闭全部发布入口。
    pub(crate) fn close(&self) {
        self.phase.store(LanePhase::Closed as u8, Ordering::Release);
    }

    /// 获取一次发布准入资格并用 RAII 跟踪 active。
    ///
    /// # 参数
    ///
    /// - `internal`: 是否为框架内部收尾发布。
    ///
    /// # 错误
    ///
    /// 当前 lane 阶段不允许该类发布时返回生命周期错误。
    fn admit(self: &Arc<Self>, internal: bool) -> Result<AdmissionGuard> {
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
        self.active.fetch_add(1, Ordering::AcqRel);
        let second = self.phase.load(Ordering::Acquire);
        if !allowed(second) {
            self.release_active();
            return Err(NafkaError::Lifecycle(format!(
                "producer lane `{}` 正在关闭",
                self.lane
            )));
        }
        Ok(AdmissionGuard {
            owner: Arc::clone(self),
        })
    }

    /// 释放一个 active 计数并唤醒排空等待者。
    fn release_active(&self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "producer active 计数不得下溢");
        if previous == 1 {
            self.active_notify.notify_waiters();
        }
    }

    /// 构造带 lane/topic 归因的确定性发布错误。
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

    /// 返回当前可用的底层 generation；当前实例已经 fatal 时先原子切换到新实例。
    fn current_native(&self) -> Result<NativeProducerSnapshot> {
        if let Some((code, detail)) = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Err(
                self.publish_error("<fatal>", format!("producer 永久错误 {code}: {detail}"))
            );
        }
        let observed = self
            .native
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if native_fatal_error(&observed).is_some() {
            self.replace_fatal_native(&observed)
        } else {
            Ok(observed)
        }
    }

    /// 仅当观察到的 generation 仍是当前 fatal generation 时构造并替换底层实例。
    fn replace_fatal_native(
        &self,
        observed: &NativeProducerSnapshot,
    ) -> Result<NativeProducerSnapshot> {
        let mut current = self
            .native
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.generation != observed.generation {
            return Ok(current.clone());
        }
        let Some((code, detail)) = native_fatal_error(&current) else {
            return Ok(current.clone());
        };
        let replacement = self
            .native_config
            .create_with_context::<_, FutureProducer<ProducerClientContext>>(
                ProducerClientContext {
                    terminal_fatal: Arc::clone(&self.terminal_fatal),
                },
            )
            .map_err(|error| {
                self.publish_error(
                    "<native-rebuild>",
                    format!(
                        "底层 producer generation {} fatal ({code}: {detail})，重建失败: {error}",
                        current.generation
                    ),
                )
            })?;
        let previous = current.generation;
        *current = NativeProducerSnapshot {
            generation: previous.saturating_add(1),
            producer: Arc::new(replacement),
        };
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

    /// 在返回当前发布错误前尝试收敛 fatal generation，并追加不含凭据的诊断。
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

    /// 把底层错误与 fatal generation 诊断合并为稳定公共错误。
    fn publish_native_error(
        &self,
        topic: &str,
        message: String,
        observed: &NativeProducerSnapshot,
    ) -> NafkaError {
        self.publish_error(topic, self.native_error_detail(message, observed))
    }

    /// 记录当前 generation 的成功 delivery，并清零连续失败窗口。
    fn note_delivery_success(&self, observed: &NativeProducerSnapshot) {
        let mut recovery = self
            .native_recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if recovery.generation == observed.generation {
            recovery.consecutive_failures = 0;
            recovery.last_success_at = Instant::now();
        }
    }

    /// 记录非 fatal 终态 delivery failure，并判断是否达到无进展切换阈值。
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

    /// 在写锁内再次确认非 fatal 无进展窗口并单飞切换 generation。
    fn replace_stalled_native(
        &self,
        observed: &NativeProducerSnapshot,
    ) -> Result<Option<NativeProducerSnapshot>> {
        let mut current = self
            .native
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                },
            )
            .map_err(|error| {
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
        *current = NativeProducerSnapshot {
            generation: previous.saturating_add(1),
            producer: Arc::new(replacement),
        };
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

    /// 合并终态 delivery failure、fatal 与非 fatal 无进展切换诊断。
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

    /// 把 delivery 错误和两级 generation 恢复诊断合并为稳定公共错误。
    fn publish_delivery_error(
        &self,
        topic: &str,
        message: String,
        observed: &NativeProducerSnapshot,
    ) -> NafkaError {
        self.publish_error(topic, self.delivery_error_detail(message, observed))
    }

    /// 记录认证、授权、fencing 等永久 producer 错误，禁止随后按无进展策略循环重建。
    ///
    /// - `topic`: 当前发布目标，用于公共错误归因。
    /// - `error`: delivery 或同步入队返回的底层稳定错误。
    fn record_terminal_error(&self, topic: &str, error: &KafkaError) -> Option<NafkaError> {
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
        let mut terminal = self
            .terminal_fatal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        terminal.get_or_insert_with(|| (code, detail.clone()));
        Some(self.publish_error(topic, format!("producer 永久错误 {code}: {detail}")))
    }

    /// 决定本次 fire delivery 失败是否打印详细日志，并返回上一窗口聚合数。
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

    /// 输出并清零当前窗口累计的被抑制失败条数。
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

    /// 构造确定未入队的容量错误，供上层只按 variant 映射背压。
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
}

impl Drop for AdmissionGuard {
    /// 释放发布资格并通知关闭排空逻辑。
    fn drop(&mut self) {
        self.owner.release_active();
    }
}

/// 把公共自有 headers 转换为底层拥有型 headers。
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

/// 构造借用数据的底层发布记录；底层同步入队时完成必要复制。
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

/// 返回指定底层 generation 的 fatal 诊断。
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

/// 判断错误码是否属于"可能可恢复、也可能是永久拒绝"的认证类。
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

/// 判断 producer 错误码是否永久不可恢复；与 consumer 分类保持同一安全边界。
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

/// 判断同步入队错误是否为可退避的本地队列满。
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

/// 等待 fire 的 delivery report 并用结构化日志保留失败可观测性。
///
/// # 参数
///
/// - `owner`: 所属 lane。
/// - `native`: 本次记录入队时使用的底层 generation。
/// - `topic`: 目标 topic。
/// - `delivery`: 已成功入队的 delivery future。
/// - `admission`: active 生命周期资格。
/// - `permit`: observer 容量资格。
async fn observe_fire_delivery(
    owner: Arc<ProducerClient>,
    native: NativeProducerSnapshot,
    topic: String,
    delivery: rdkafka::producer::future_producer::DeliveryFuture,
    admission: AdmissionGuard,
    permit: OwnedSemaphorePermit,
) {
    match delivery.await {
        Ok(Ok(report)) => {
            owner.note_delivery_success(&native);
            owner.delivery_succeeded.fetch_add(1, Ordering::Relaxed);
            owner.auth_failures.store(0, Ordering::Release);
            tracing::debug!(
                lane = %owner.lane,
                topic,
                partition = report.partition,
                offset = report.offset,
                "异步发布投递成功"
            );
        }
        Ok(Err((error, _message))) => {
            owner.delivery_failed.fetch_add(1, Ordering::Relaxed);
            let error = owner
                .record_terminal_error(&topic, &error)
                .unwrap_or_else(|| {
                    owner.publish_delivery_error(
                        &topic,
                        format!("broker 投递失败: {error}"),
                        &native,
                    )
                });
            let (emit_detail, suppressed) = owner.fire_failure_log_decision();
            if suppressed != 0 {
                tracing::warn!(lane = %owner.lane, suppressed, "异步发布失败日志已按窗口聚合");
            }
            if emit_detail {
                tracing::error!(
                    lane = %owner.lane,
                    topic,
                    error = %error,
                    "异步发布投递失败"
                );
            }
        }
        Err(error) => {
            owner.delivery_failed.fetch_add(1, Ordering::Relaxed);
            let error =
                owner.delivery_error_detail(format!("delivery 观察通道中断: {error}"), &native);
            let (emit_detail, suppressed) = owner.fire_failure_log_decision();
            if suppressed != 0 {
                tracing::warn!(lane = %owner.lane, suppressed, "异步发布失败日志已按窗口聚合");
            }
            if emit_detail {
                tracing::error!(
                    lane = %owner.lane,
                    topic,
                    error = %error,
                    "异步发布观察通道中断"
                );
            }
        }
    }
    drop(permit);
    drop(admission);
}
