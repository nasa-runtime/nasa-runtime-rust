use crate::{ConfigError, EventKind, Notification, NotifyErrorKind};
use nametrics_core::atomic::{add, AtomicHistogram, LATENCY_SECONDS};
use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

static ENQUEUED: MetricDescriptor = MetricDescriptor {
    name: "nanotify_enqueued_total",
    help: "进入通知队列的离散事件累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["provider", "event"],
    histogram_bounds: &[],
};
static DROPPED: MetricDescriptor = MetricDescriptor {
    name: "nanotify_dropped_total",
    help: "因队列或停机门禁丢弃的通知累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["provider", "event", "reason"],
    histogram_bounds: &[],
};
static DELIVERIES: MetricDescriptor = MetricDescriptor {
    name: "nanotify_deliveries_total",
    help: "以最终结果分类的通知投递累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["provider", "event", "outcome"],
    histogram_bounds: &[],
};
static DURATION: MetricDescriptor = MetricDescriptor {
    name: "nanotify_delivery_duration_seconds",
    help: "包含有限重试和等待的通知投递时长。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["provider", "outcome"],
    histogram_bounds: LATENCY_SECONDS,
};
static DEPTH: MetricDescriptor = MetricDescriptor {
    name: "nanotify_queue_depth",
    help: "等待 worker 领取的通知数量。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static CAPACITY: MetricDescriptor = MetricDescriptor {
    name: "nanotify_queue_capacity",
    help: "当前进程通知队列的固定容量。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static DESCRIPTORS: &[&MetricDescriptor] = &[
    &ENQUEUED,
    &DROPPED,
    &DELIVERIES,
    &DURATION,
    &DEPTH,
    &CAPACITY,
];
const EVENTS: [EventKind; 3] = [
    EventKind::SlowSql,
    EventKind::ExecutionError,
    EventKind::AcquireTimeout,
];
const DROP_REASONS: [&str; 3] = ["queue_full", "stopping", "shutdown_timeout"];
const OUTCOMES: [&str; 10] = [
    "accepted",
    "timeout",
    "unavailable",
    "rate_limited",
    "rejected",
    "authentication",
    "invalid_request",
    "other",
    "panic",
    "shutdown",
];

/// 一次 envelope 投递的最终结果，不以尝试次数重复计数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum DeliveryOutcome {
    /// 适配器确认下游受理，不证明最终收件。
    Accepted,
    /// 投递超过期限，不能仅凭超时认定远端未受理。
    Timeout,
    /// 通知服务暂不可用。
    Unavailable,
    /// 通知服务拒绝了超过限额的请求。
    RateLimited,
    /// 通知服务明确拒绝受理。
    Rejected,
    /// 通知服务认证失败。
    Authentication,
    /// 请求不符合通知服务接口要求。
    InvalidRequest,
    /// 其它稳定分类之外的投递失败。
    Other,
    /// 投递或其析构因 panic 展开退出。
    Panic,
    /// 投递在宿主停机收口时被取消。
    Shutdown,
}

impl From<NotifyErrorKind> for DeliveryOutcome {
    /// 业务作用：把渠道错误映射到统一投递指标。
    /// 参数说明：`kind` 是稳定渠道失败分类。
    /// 返回：不会携带原始错误的固定结果。
    fn from(kind: NotifyErrorKind) -> Self {
        match kind {
            NotifyErrorKind::Timeout => Self::Timeout,
            NotifyErrorKind::Unavailable => Self::Unavailable,
            NotifyErrorKind::RateLimited => Self::RateLimited,
            NotifyErrorKind::Rejected => Self::Rejected,
            NotifyErrorKind::Authentication => Self::Authentication,
            NotifyErrorKind::InvalidRequest => Self::InvalidRequest,
            NotifyErrorKind::Other => Self::Other,
        }
    }
}

struct ProviderMetrics {
    id: String,
    enqueued: [AtomicU64; 3],
    dropped: [[AtomicU64; 3]; 3],
    deliveries: [[AtomicU64; 10]; 3],
    duration: [AtomicHistogram; 10],
}

/// 通知源的固定原子单元；仅抓取时构造 label 和样本。
pub struct QueueMetrics {
    providers: Vec<ProviderMetrics>,
    capacity: usize,
    depth: AtomicU64,
}

impl QueueMetrics {
    /// 业务作用：为指标注册事务提供精确最坏系列数。
    /// 参数说明：无。
    /// 返回：全部 provider、事件、结果和固定桶展开后的总系列数。
    pub fn series_budget(&self) -> usize {
        self.providers.len() * (3 + 9 + 30 + 10 * (LATENCY_SECONDS.len() + 3)) + 2
    }

    /// 业务作用：在一个 envelope 结束时写入唯一投递结果与总耗时。
    /// 参数说明：`provider` 是冻结下标，`event` 为事件，`outcome` 为最终结果，`elapsed` 包含重试。
    /// 返回：无；未知下标不写入，业务结果不受影响。
    pub fn record_delivery(
        &self,
        provider: usize,
        event: EventKind,
        outcome: DeliveryOutcome,
        elapsed: Duration,
    ) {
        if let Some(slot) = self.providers.get(provider) {
            add(&slot.deliveries[event as usize][outcome as usize], 1);
            slot.duration[outcome as usize].observe(elapsed);
        }
    }
}

impl LegacyMetricsSource for QueueMetrics {
    /// 业务作用：声明此源拥有的静态通知指标合同。
    /// 参数说明：无。
    /// 返回：固定 descriptor 集合。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        DESCRIPTORS
    }

    /// 业务作用：从原子单元生成可供 Prometheus 和 OTLP 共用的有界快照。
    /// 参数说明：无。
    /// 返回：全部冻结维度的当前累计样本。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut samples = Vec::new();
        for provider in &self.providers {
            for event in EVENTS {
                let labels = vec![
                    ("provider", provider.id.clone()),
                    ("event", event.as_str().into()),
                ];
                samples.push(MetricSample {
                    name: ENQUEUED.name,
                    labels: labels.clone(),
                    value: MetricValue::Counter(
                        provider.enqueued[event as usize].load(Ordering::Relaxed),
                    ),
                });
                for (index, reason) in DROP_REASONS.iter().enumerate() {
                    let mut labels = labels.clone();
                    labels.push(("reason", (*reason).into()));
                    samples.push(MetricSample {
                        name: DROPPED.name,
                        labels,
                        value: MetricValue::Counter(
                            provider.dropped[event as usize][index].load(Ordering::Relaxed),
                        ),
                    });
                }
                for (index, outcome) in OUTCOMES.iter().enumerate() {
                    let mut labels = labels.clone();
                    labels.push(("outcome", (*outcome).into()));
                    samples.push(MetricSample {
                        name: DELIVERIES.name,
                        labels,
                        value: MetricValue::Counter(
                            provider.deliveries[event as usize][index].load(Ordering::Relaxed),
                        ),
                    });
                }
            }
            for (index, outcome) in OUTCOMES.iter().enumerate() {
                samples.push(MetricSample {
                    name: DURATION.name,
                    labels: vec![
                        ("provider", provider.id.clone()),
                        ("outcome", (*outcome).into()),
                    ],
                    value: provider.duration[index].snapshot(),
                });
            }
        }
        samples.push(MetricSample {
            name: DEPTH.name,
            labels: vec![],
            value: MetricValue::Gauge(self.depth.load(Ordering::Relaxed) as f64),
        });
        samples.push(MetricSample {
            name: CAPACITY.name,
            labels: vec![],
            value: MetricValue::Gauge(self.capacity as f64),
        });
        Some(samples)
    }

    /// 业务作用：由统一 MetricHub 的结构化出口渲染，不建立第二份文本状态。
    /// 参数说明：`output` 是宿主的输出缓冲区。
    /// 返回：无；结构化快照由 MetricHub 统一渲染。
    fn render_prometheus(&self, output: &mut String) {
        nametrics_core::atomic::render_snapshot(
            DESCRIPTORS,
            &self.snapshot().unwrap_or_default(),
            output,
        );
    }
}

struct QueueState {
    accepting: AtomicBool,
    metrics: Arc<QueueMetrics>,
}

/// 启动期解析的具体队列路由；热路径不解析 provider 名称，不执行用户回调。
#[derive(Clone)]
pub struct AlertRoute {
    sender: mpsc::Sender<Envelope>,
    state: Arc<QueueState>,
    provider: usize,
    requires_initialized_notify: bool,
}

/// 非阻塞入队结果；不包含可以反向传播到业务调用的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// 通知已进入本地有界队列，尚未确认下游受理。
    Enqueued,
    /// 队列容量不足，新通知被丢弃。
    Full,
    /// 通知生产权已关闭，新通知被拒绝。
    Stopping,
    /// 进程默认通知实现尚未安装，本次通知不入队。
    Ignored,
}

impl AlertRoute {
    /// 业务作用：尝试移交通知所有权，满队列采用 drop-new 并记录稳定丢弃原因。
    /// 参数说明：`notification` 是规则命中后才构造的有界消息。
    /// 返回：入队、容量不足、停止或未初始化时忽略；本方法从不等待消费者或业务实现。
    pub fn try_send(&self, notification: Notification) -> EnqueueOutcome {
        // 未初始化的进程通知直接忽略，不占队列、不记录投递失败，也不在稍后初始化后补发旧事件。
        if self.requires_initialized_notify && crate::get().is_none() {
            return EnqueueOutcome::Ignored;
        }
        let event = notification.fields().event;
        let slot = &self.state.metrics.providers[self.provider];
        // 停机先关闭生产权，旧 Stream 的终态仍能安全丢弃通知而不延长业务停机。
        if !self.state.accepting.load(Ordering::Acquire) {
            add(&slot.dropped[event as usize][1], 1);
            return EnqueueOutcome::Stopping;
        }
        match self.sender.try_reserve() {
            Ok(permit) => {
                if !self.state.accepting.load(Ordering::Acquire) {
                    add(&slot.dropped[event as usize][1], 1);
                    return EnqueueOutcome::Stopping;
                }
                add(&self.state.metrics.depth, 1);
                add(&slot.enqueued[event as usize], 1);
                permit.send(Envelope {
                    provider: self.provider,
                    notification,
                    queued: true,
                    metrics: self.state.metrics.clone(),
                });
                EnqueueOutcome::Enqueued
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                add(&slot.dropped[event as usize][0], 1);
                EnqueueOutcome::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                add(&slot.dropped[event as usize][1], 1);
                EnqueueOutcome::Stopping
            }
        }
    }
}

/// 已冻结 provider 目录的队列生产能力。
#[derive(Clone)]
pub struct NotificationProducer {
    sender: mpsc::Sender<Envelope>,
    state: Arc<QueueState>,
}

impl NotificationProducer {
    /// 业务作用：在启动期把名称解析为可在热路径直接使用的队列路由。
    /// 参数说明：`provider_ref` 必须来自经过校验的冻结配置。
    /// 返回：对应路由；未知 provider 返回 None。
    pub fn route(&self, provider_ref: &str) -> Option<AlertRoute> {
        self.state
            .metrics
            .providers
            .iter()
            .position(|slot| slot.id == provider_ref)
            .map(|provider| AlertRoute {
                sender: self.sender.clone(),
                state: self.state.clone(),
                provider,
                requires_initialized_notify: false,
            })
    }

    /// 业务作用：取得使用进程通知实现的非阻塞路由，未初始化时通知不进入队列。
    /// 参数说明：无。
    /// 返回：目录包含默认路由时返回生产能力；其它目录返回 None，不增建 provider 或指标维度。
    pub fn default_route(&self) -> Option<AlertRoute> {
        self.route(crate::DEFAULT_PROVIDER_ID).map(|mut route| {
            route.requires_initialized_notify = true;
            route
        })
    }
    /// 业务作用：先撤销所有生产路由的接受权，再由宿主排空已接受消息。
    /// 参数说明：无。
    /// 返回：无；重复关闭没有额外副作用。
    pub fn close(&self) {
        self.state.accepting.store(false, Ordering::Release);
    }
    /// 业务作用：交出只持有原子单元的统一指标源。
    /// 参数说明：无。
    /// 返回：与生产端、worker 同源的指标句柄。
    pub fn metrics(&self) -> Arc<QueueMetrics> {
        self.state.metrics.clone()
    }
}

/// 消费端唯一拥有的待投递消息。
pub struct Envelope {
    /// 启动期冻结的 provider 目录索引。
    pub provider: usize,
    /// 已施加文本边界、由 worker 独占投递责任的通知。
    pub notification: Notification,
    queued: bool,
    metrics: Arc<QueueMetrics>,
}

impl Drop for Envelope {
    /// 业务作用：即使关闭与已预留生产者并发，也为未被领取的消息回收深度并记录丢弃。
    /// 参数说明：无。
    /// 返回：无；已被 worker 领取的消息不重复计数。
    fn drop(&mut self) {
        if self.queued {
            self.metrics.depth.fetch_sub(1, Ordering::Relaxed);
            add(
                &self.metrics.providers[self.provider].dropped
                    [self.notification.fields().event as usize][2],
                1,
            );
        }
    }
}

/// 单消费者接收权，宿主可在不销毁旧 route 的情况下关闭并排空。
pub struct NotificationReceiver {
    receiver: mpsc::Receiver<Envelope>,
    state: Arc<QueueState>,
}

impl NotificationReceiver {
    /// 业务作用：取走一条已接受消息并从等待深度中扣除。
    /// 参数说明：无。
    /// 返回：消息或已关闭且排空的 None。
    pub async fn recv(&mut self) -> Option<Envelope> {
        let mut message = self.receiver.recv().await;
        if let Some(message) = &mut message {
            message.queued = false;
            self.state.metrics.depth.fetch_sub(1, Ordering::Relaxed);
        }
        message
    }
    /// 业务作用：关闭生产与接收入口，保留现有消息供有限排空。
    /// 参数说明：无。
    /// 返回：无；关闭后不再接受新通知。
    pub fn close(&mut self) {
        self.state.accepting.store(false, Ordering::Release);
        self.receiver.close();
    }
    /// 业务作用：在停机预算耗尽时计数并释放尚未投递的消息。
    /// 参数说明：无。
    /// 返回：本次放弃的消息数。
    pub fn discard_remaining(&mut self) -> usize {
        self.close();
        let mut count = 0;
        while let Ok(message) = self.receiver.try_recv() {
            drop(message);
            count += 1;
        }
        count
    }
}

impl Drop for NotificationReceiver {
    /// 业务作用：异常退出也撤销旧路由并登记尚未消费的通知，避免悬空生产端。
    /// 参数说明：无。
    /// 返回：无；只进行有限队列清理和原子计数。
    fn drop(&mut self) {
        self.discard_remaining();
    }
}

/// 只负责有界内存队列的工厂，不创建任务、线程或网络客户端。
pub struct NotificationQueue;

impl NotificationQueue {
    /// 业务作用：按已引用 provider 的冻结目录预分配通知指标和有界队列。
    /// 参数说明：`provider_ids` 为唯一合法标识，`capacity` 为 1..=65536 的队列容量。
    /// 返回：生产端与唯一接收端；非法目录或容量不分配运行资源。
    pub fn bounded(
        provider_ids: Vec<String>,
        capacity: usize,
    ) -> Result<(NotificationProducer, NotificationReceiver), ConfigError> {
        if !(1..=65536).contains(&capacity) || provider_ids.is_empty() || provider_ids.len() > 32 {
            return Err(ConfigError("queue"));
        }
        let mut seen = BTreeSet::new();
        if provider_ids
            .iter()
            .any(|id| !valid_provider_id(id) || !seen.insert(id))
        {
            return Err(ConfigError("providers"));
        }
        let providers = provider_ids
            .into_iter()
            .map(|id| ProviderMetrics {
                id,
                enqueued: std::array::from_fn(|_| AtomicU64::new(0)),
                dropped: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
                deliveries: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
                duration: std::array::from_fn(|_| AtomicHistogram::new(LATENCY_SECONDS)),
            })
            .collect();
        let state = Arc::new(QueueState {
            accepting: AtomicBool::new(true),
            metrics: Arc::new(QueueMetrics {
                providers,
                capacity,
                depth: AtomicU64::new(0),
            }),
        });
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            NotificationProducer {
                sender,
                state: state.clone(),
            },
            NotificationReceiver { receiver, state },
        ))
    }
}

/// 业务作用：限制 provider 目录和指标标签的字符集与长度。
/// 参数说明：`id` 是普通配置中的渠道标识，不是渠道凭据。
/// 返回：仅当满足字母开头的 1..=64 字节 ASCII 标识时为真。
pub fn valid_provider_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id.as_bytes()[0].is_ascii_alphabetic()
        && id
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'_' | b'.' | b'-'))
}
