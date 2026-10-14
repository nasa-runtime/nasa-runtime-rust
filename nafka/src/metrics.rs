//! 后端无关的 nafka 指标出口；运行时只发稳定名字与低基数字段。

/// 指标标签的借用视图；键是框架固定常量，值来自 lane/group/topic 等受控身份。
pub type MetricLabels<'a> = &'a [(&'static str, &'a str)];

/// 应用可注入的指标接收端。
///
/// 回调在每个源独立的有界导出线程执行，绝不占用 consumer owner 或 producer 交付线程。
/// 实现仍应及时返回；阻塞会丢弃后续观测，并使源关闭等待该回调实际退出。panic 只计为导出失败。
pub trait MetricsSink: Send + Sync + 'static {
    /// 业务作用：累加一个单调 counter。
    ///
    /// # 参数
    ///
    /// - `name`: nafka 定义的固定指标名。
    /// - `delta`: 本次增量，始终为正数。
    /// - `labels`: 固定低基数字段，不包含 payload、header value 或错误详情。
    fn counter(&self, name: &'static str, delta: u64, labels: MetricLabels<'_>);

    /// 业务作用：设置一个瞬时 gauge。
    ///
    /// # 参数
    ///
    /// - `name`: nafka 定义的固定指标名。
    /// - `value`: 当前观测值。
    /// - `labels`: 固定低基数字段。
    fn gauge(&self, name: &'static str, value: i64, labels: MetricLabels<'_>);
}

use crate::{NafkaError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, TryLockError};
use std::thread::JoinHandle;
use std::time::Instant;

/// 指标导出容量，独立于业务、DLT 和 producer 的责任额度。
#[derive(Clone, Copy, Debug)]
pub struct MetricsOptions {
    /// 最多排队的观测事件数，不包括正在执行的一个回调。
    pub queue_capacity: usize,
    /// 单事件指标名和全部标签 UTF-8 字节上限；超限直接丢弃，不截断身份。
    pub max_event_bytes: usize,
}
impl Default for MetricsOptions {
    /// 业务作用：给指标出口建立有限默认预算。参数说明：无。返回：256 个排队事件，每事件最多 4096 字节。
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            max_event_bytes: 4096,
        }
    }
}
impl MetricsOptions {
    /// 业务作用：在创建导出线程前拒绝无界或不合理预算。参数说明：无。返回：有效时继续，非法配置返回 Config。
    pub(crate) fn validate(self) -> Result<()> {
        if !(1..=65536).contains(&self.queue_capacity)
            || !(64..=65536).contains(&self.max_event_bytes)
        {
            return Err(NafkaError::Config(
                "指标队列必须为 1..65536，单事件字节为 64..65536".into(),
            ));
        }
        Ok(())
    }
}

/// 指标出口本身的精确进程内统计；不依赖用户 sink 可用。
#[derive(Clone, Debug)]
pub struct MetricsExportStats {
    /// 是否安装了导出后端。
    pub enabled: bool,
    /// 尚未开始回调的事件数量。
    pub queued: usize,
    /// 已进入用户代码且尚未实际返回的回调数，只可能为零或一。
    pub active: usize,
    /// 用户回调正常返回的累计事件数，不代表远端监控已经持久保存。
    pub exported: u64,
    /// 容量、锁竞争、超限或关闭导致的累计丢弃事件数。
    pub dropped: u64,
    /// 用户回调 panic 的累计次数。
    pub failed: u64,
    /// 是否仍接受新的观测事件。
    pub accepting: bool,
    /// 导出线程是否实际退出；未安装后端时为 true。
    pub worker_exited: bool,
}

enum Value {
    Counter(u64),
    Gauge(i64),
}
struct Event {
    name: &'static str,
    value: Value,
    labels: Vec<(&'static str, String)>,
}
struct Shared {
    queue: Mutex<VecDeque<Event>>,
    wake: Condvar,
    closing: AtomicBool,
    queued: AtomicUsize,
    active: AtomicUsize,
    exported: AtomicU64,
    dropped: AtomicU64,
    failed: AtomicU64,
}

/// 热路径只进行有界复制和非等待准入，用户代码始终由专属线程执行。
pub(crate) struct AsyncMetrics {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
    source: String,
    options: MetricsOptions,
    enabled: bool,
}
impl AsyncMetrics {
    /// 业务作用：构造源级隔离的指标出口。参数说明：sink 为可选后端，source 为受管源名，options 为容量边界。返回：无后端不建线程；线程创建或配置失败拒绝构造。
    pub(crate) fn new(
        sink: Option<Arc<dyn MetricsSink>>,
        source: String,
        options: MetricsOptions,
    ) -> Result<Arc<Self>> {
        options.validate()?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(VecDeque::new()),
            wake: Condvar::new(),
            closing: AtomicBool::new(false),
            queued: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            exported: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        });
        let enabled = sink.is_some();
        let worker = if let Some(sink) = sink {
            let state = shared.clone();
            Some(
                std::thread::Builder::new()
                    .name("nafka-metrics".into())
                    .spawn(move || Self::run(state, sink))
                    .map_err(|_| NafkaError::Lifecycle("指标导出线程无法创建".into()))?,
            )
        } else {
            None
        };
        Ok(Arc::new(Self {
            shared,
            worker: Mutex::new(worker),
            source,
            options,
            enabled,
        }))
    }

    /// 业务作用：在专属线程执行用户回调并隔离失败。参数说明：shared 为有界队列与计数，sink 为后端。返回：关闭后退出；正在执行的用户代码只能等待其真实返回。
    fn run(shared: Arc<Shared>, sink: Arc<dyn MetricsSink>) {
        loop {
            let event = {
                let mut queue = shared
                    .queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while queue.is_empty() && !shared.closing.load(Ordering::Acquire) {
                    queue = shared
                        .wake
                        .wait(queue)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                if shared.closing.load(Ordering::Acquire) {
                    return;
                }
                let event = queue.pop_front().expect("非空观测队列");
                shared.queued.fetch_sub(1, Ordering::Relaxed);
                shared.active.store(1, Ordering::Release);
                event
            };
            let labels: Vec<_> = event
                .labels
                .iter()
                .map(|(key, value)| (*key, value.as_str()))
                .collect();
            // 用户调用不持有队列锁；panic 或任意长阻塞都不能阻止 owner 获取或放弃观测额度。
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match event.value {
                    Value::Counter(value) => sink.counter(event.name, value, &labels),
                    Value::Gauge(value) => sink.gauge(event.name, value, &labels),
                }));
            if result.is_ok() {
                shared.exported.fetch_add(1, Ordering::Relaxed);
            } else {
                shared.failed.fetch_add(1, Ordering::Relaxed);
            }
            shared.active.store(0, Ordering::Release);
        }
    }

    /// 业务作用：非等待地接纳一个有限观测事件。参数说明：name/value 为指标，labels 为固定身份标签。返回：成功入队或计入丢弃；绝不执行用户代码或等待容量。
    fn enqueue(&self, name: &'static str, value: Value, labels: MetricLabels<'_>) {
        if !self.enabled {
            return;
        }
        let bytes = labels.iter().fold(
            name.len()
                .saturating_add(6)
                .saturating_add(self.source.len()),
            |sum, (key, value)| sum.saturating_add(key.len()).saturating_add(value.len()),
        );
        if labels.len() > 7
            || bytes > self.options.max_event_bytes
            || labels.iter().any(|(key, _)| *key == "source")
        {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut queue = match self.shared.queue.try_lock() {
            Ok(queue) => queue,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if self.shared.closing.load(Ordering::Acquire) || queue.len() >= self.options.queue_capacity
        {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut owned: Vec<_> = labels
            .iter()
            .map(|(key, value)| (*key, (*value).to_owned()))
            .collect();
        owned.push(("source", self.source.clone()));
        queue.push_back(Event {
            name,
            value,
            labels: owned,
        });
        self.shared.queued.fetch_add(1, Ordering::Relaxed);
        self.shared.wake.notify_one();
    }

    /// 业务作用：关闭新观测准入并丢弃未执行的队列。参数说明：无。返回：不等待用户代码；活动回调责任留给 await_closed。
    pub(crate) fn close(&self) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 先关闭准入再清队列，避免释放队列后新的观测重新激活已经关闭的出口。
        self.shared.closing.store(true, Ordering::Release);
        self.shared
            .dropped
            .fetch_add(queue.len() as u64, Ordering::Relaxed);
        queue.clear();
        self.shared.queued.store(0, Ordering::Relaxed);
        self.shared.wake.notify_all();
    }

    /// 业务作用：读取不经过用户出口的自身状态。参数说明：无。返回：独立原子计数快照，worker_exited 以线程实际终态为准。
    pub(crate) fn stats(&self) -> MetricsExportStats {
        MetricsExportStats {
            enabled: self.enabled,
            queued: self.shared.queued.load(Ordering::Relaxed),
            active: self.shared.active.load(Ordering::Acquire),
            exported: self.shared.exported.load(Ordering::Relaxed),
            dropped: self.shared.dropped.load(Ordering::Relaxed),
            failed: self.shared.failed.load(Ordering::Relaxed),
            accepting: self.enabled && !self.shared.closing.load(Ordering::Acquire),
            worker_exited: self
                .worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_none_or(JoinHandle::is_finished),
        }
    }

    /// 业务作用：在源关闭的共同期限内等待指标用户代码实际退出。参数说明：deadline 为单调绝对期限。返回：线程真正退出才成功；超时保留活动诊断，后续可继续等待。
    pub(crate) async fn await_closed(&self, deadline: Instant) -> bool {
        loop {
            if self.stats().worker_exited {
                if let Some(worker) = self
                    .worker
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    let _ = worker.join();
                }
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(
                std::time::Duration::from_millis(10)
                    .min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }
}
impl MetricsSink for AsyncMetrics {
    /// 业务作用：将计数增量送入有界观测队列。参数说明：name/delta/labels 为指标身份和增量。返回：零增量忽略，拥塞丢弃不影响业务确认。
    fn counter(&self, name: &'static str, delta: u64, labels: MetricLabels<'_>) {
        if delta != 0 {
            self.enqueue(name, Value::Counter(delta), labels);
        }
    }
    /// 业务作用：将瞬时状态送入有界观测队列。参数说明：name/value/labels 为指标身份和观测值。返回：拥塞丢弃，由后续采样更新状态。
    fn gauge(&self, name: &'static str, value: i64, labels: MetricLabels<'_>) {
        self.enqueue(name, Value::Gauge(value), labels);
    }
}
impl Drop for AsyncMetrics {
    /// 业务作用：遗忘显式关闭时停止新导出并唤醒线程。参数说明：无。返回：不阻塞析构；活动用户代码仍需自行返回。
    fn drop(&mut self) {
        self.close();
    }
}
