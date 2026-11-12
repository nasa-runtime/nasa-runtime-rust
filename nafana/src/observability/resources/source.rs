use super::{
    catalog::*,
    sample::{metric, sample, Collection, Failure},
    CollectorsConfig,
};
use nametrics_core::{
    atomic::{render_snapshot, AtomicHistogram},
    LegacyMetricsSource, MetricDescriptor, MetricSample, MetricValue,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const STATES: &[&str] = &[
    "uninitialized",
    "unsupported",
    "unavailable",
    "stale",
    "ready",
    "duplicate",
];
struct Cached {
    samples: Vec<MetricSample>,
    last_success: Option<Instant>,
    last_timestamp: Option<f64>,
    state: &'static str,
    errors: u64,
    duration: AtomicHistogram,
    stale_after: Duration,
}

/// 受管资源采集源；每实例只有一个在途阻塞任务，文本与结构化出口共享缓存。
pub struct ResourceCollector {
    config: CollectorsConfig,
    cached: Mutex<BTreeMap<String, Cached>>,
    worker: tokio::sync::Mutex<Option<JoinHandle<Vec<Collection>>>>,
    running: AtomicBool,
    stopped: AtomicBool,
}

impl ResourceCollector {
    /// 业务作用：在开放端口之前冻结采集来源及最坏标签目录，不执行文件读取。
    ///
    /// 参数说明：
    /// - `config`：来源白名单和资源限制。
    ///
    /// 返回：可登记到统一 MetricHub 的来源；无效配置拒绝，关闭配置返回 None。
    pub fn new(config: CollectorsConfig) -> Result<Option<Arc<Self>>, String> {
        config.validate()?;
        if !config.enabled {
            return Ok(None);
        }
        let mut ids = Vec::new();
        if config.process {
            ids.extend(
                ["process", "process_memory", "process_fds", "process_io"].map(String::from),
            );
        }
        if config.cgroup {
            ids.extend(
                [
                    "cgroup_cpu",
                    "cgroup_memory",
                    "cgroup_pids",
                    "cgroup_io",
                    "cgroup_pressure",
                ]
                .map(String::from),
            );
        }
        ids.extend(
            config
                .filesystems
                .keys()
                .map(|alias| format!("filesystem:{alias}")),
        );
        ids.extend(
            config
                .network_interfaces
                .keys()
                .map(|alias| format!("network:{alias}")),
        );
        let cached = ids
            .into_iter()
            .map(|id| {
                let stale = if id.starts_with("filesystem:") {
                    config
                        .stale_after_ms
                        .max(config.filesystem_interval_ms.saturating_mul(3))
                } else {
                    config.stale_after_ms
                };
                (
                    id,
                    Cached {
                        samples: Vec::new(),
                        last_success: None,
                        last_timestamp: None,
                        state: "uninitialized",
                        errors: 0,
                        duration: AtomicHistogram::new(DURATION.histogram_bounds),
                        stale_after: Duration::from_millis(stale),
                    },
                )
            })
            .collect();
        Ok(Some(Arc::new(Self {
            config,
            cached: Mutex::new(cached),
            worker: tokio::sync::Mutex::new(None),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        })))
    }

    /// 业务作用：预留全部来源及固定 Histogram 桶的容量，空启动快照不能掩盖未来序列开销。
    ///
    /// 参数说明：无。
    ///
    /// 返回：保守上界，涵盖所有平台、状态和白名单标签；不会随样本成功或失败减少。
    pub fn worst_case_series(&self) -> usize {
        let sources = self
            .cached
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        let health = sources * (3 + STATES.len() + DURATION.histogram_bounds.len() + 3);
        health
            + if self.config.process {
                24
            } else {
                0
            }
            + if self.config.cgroup {
                32 + 6 * self.config.io_devices.len()
            } else {
                0
            }
            + self.config.filesystems.len() * 7
            + self.config.network_interfaces.len() * 8
    }

    /// 业务作用：发布完整采样结果，过期或超时结果不能伪装成刚刚取得的事实。
    ///
    /// 参数说明：
    /// - `collections`：唯一工作任务完成的有限结果。
    /// - `timed_out`：整轮是否已经超过预算。
    ///
    /// 返回：成功仅刷新对应来源，失败保留最后成功时间并撤销可用状态。
    fn publish(&self, collections: Vec<Collection>, timed_out: bool) {
        let mut cache = self
            .cached
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for collection in collections {
            let Some(entry) = cache.get_mut(&collection.id) else {
                continue;
            };
            entry.duration.observe(collection.duration);
            if timed_out {
                continue;
            }
            match collection.samples {
                Ok(samples) => {
                    entry.samples = samples;
                    entry.last_success = Some(collection.started);
                    entry.last_timestamp = SystemTime::now()
                        .checked_sub(collection.started.elapsed())
                        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                        .map(|time| time.as_secs_f64());
                    entry.state = "ready";
                }
                Err(failure) => {
                    entry.state = match failure {
                        Failure::Unsupported => "unsupported",
                        Failure::Unavailable => "unavailable",
                        Failure::Duplicate => "duplicate",
                    };
                    if failure == Failure::Unavailable {
                        entry.errors = entry.errors.saturating_add(1);
                    }
                }
            }
        }
    }

    /// 业务作用：将超时或工作异常标为不可用，不能继续输出旧数值暗示健康。
    ///
    /// 参数说明：
    /// - `filesystem_due`：本轮是否包含文件系统来源。
    ///
    /// 返回：只影响本轮来源，错误累计一次；保留最后成功时间用于盲区告警。
    fn failed(&self, filesystem_due: bool) {
        for (id, entry) in self
            .cached
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter_mut()
        {
            if filesystem_due || !id.starts_with("filesystem:") {
                entry.state = "unavailable";
                entry.errors = entry.errors.saturating_add(1);
            }
        }
    }

    /// 业务作用：等待唯一工作任务真实结束；取消等待时仍将句柄保存在来源中。
    ///
    /// 参数说明：无。
    ///
    /// 返回：完整采样结果或任务失败，完成后才清除工作责任。
    async fn completed(&self) -> Result<Vec<Collection>, String> {
        let mut worker = self.worker.lock().await;
        let Some(task) = worker.as_mut() else {
            return Ok(Vec::new());
        };
        let result = task
            .await
            .map_err(|_| "resource collector worker failed".into());
        worker.take();
        result
    }

    /// 业务作用：周期执行唯一阻塞采样，超时不派生替代任务，停止后不接受晚到样本。
    ///
    /// 参数说明：
    /// - `stop`：宿主的生命周期取消信号。
    ///
    /// 返回：停止调度后成功；重复运行或任务异常失败，残留阻塞工作由 finish 回收。
    pub async fn run(self: Arc<Self>, stop: CancellationToken) -> Result<(), String> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Err("resource collector already running".into());
        }
        let mut interval = tokio::time::interval(Duration::from_millis(self.config.interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_filesystem = None::<Instant>;
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => {
                    self.stopped.store(true, Ordering::Release);
                    return Ok(());
                }
                _ = interval.tick() => {},
            }
            if self.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            let filesystem_due = last_filesystem.is_none_or(|last| {
                last.elapsed() >= Duration::from_millis(self.config.filesystem_interval_ms)
            });
            if filesystem_due {
                last_filesystem = Some(Instant::now());
            }
            {
                let mut worker = self.worker.lock().await;
                if self.stopped.load(Ordering::Acquire) {
                    return Ok(());
                }
                if worker.is_some() {
                    return Err("resource collector worker is still owned".into());
                }
                let config = self.config.clone();
                *worker = Some(tokio::task::spawn_blocking(move || {
                    sample(&config, filesystem_due)
                }));
            }
            let deadline = tokio::time::sleep(Duration::from_millis(self.config.timeout_ms));
            tokio::pin!(deadline);
            let mut timed_out = false;
            let result = loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => {
                        self.stopped.store(true, Ordering::Release);
                        return Ok(());
                    }
                    _ = &mut deadline, if !timed_out => {
                        timed_out = true;
                        // 超时只撤销样本资格；原工作仍占唯一名额，不能形成无界阻塞任务队列。
                        self.failed(filesystem_due);
                    }
                    result = self.completed() => break result,
                }
            };
            match result {
                Ok(collections) => self.publish(collections, timed_out),
                Err(error) => {
                    if !timed_out {
                        self.failed(filesystem_due);
                    }
                    return Err(error);
                }
            }
        }
    }

    /// 业务作用：关闭新采样并在宿主统一期限内等待实际阻塞工作，超时明确保留未完成责任。
    ///
    /// 参数说明：
    /// - `deadline`：宿主给出的绝对停机期限。
    ///
    /// 返回：工作已经退出时成功；期限到仍未退出返回错误，句柄继续由来源持有。
    pub async fn finish(&self, deadline: tokio::time::Instant) -> Result<(), String> {
        self.stopped.store(true, Ordering::Release);
        tokio::time::timeout_at(deadline, self.completed())
            .await
            .map_err(|_| {
                "resource collector blocking work did not finish before shutdown deadline"
                    .to_string()
            })??;
        Ok(())
    }
}

impl LegacyMetricsSource for ResourceCollector {
    /// 业务作用：登记固定的资源指标目录。
    ///
    /// 参数说明：无。
    ///
    /// 返回：不随平台或配置白名单改变名称的静态合同。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        DESCRIPTORS
    }

    /// 业务作用：从有界缓存取得资源事实，抓取不触发文件访问或阻塞任务。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅包含新鲜可用数值，并始终给出来源状态、错误和最后成功时间。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let cached = self
            .cached
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut output = Vec::new();
        for (id, entry) in cached.iter() {
            let state = if entry.state == "ready"
                && entry
                    .last_success
                    .is_some_and(|time| time.elapsed() > entry.stale_after)
            {
                "stale"
            } else {
                entry.state
            };
            if state == "ready" {
                output.extend(entry.samples.iter().cloned());
            }
            output.push(metric(
                &AVAILABLE,
                &[id],
                MetricValue::Gauge(if state == "ready" {
                    1.0
                } else {
                    0.0
                }),
            ));
            output.push(metric(&ERRORS, &[id], MetricValue::Counter(entry.errors)));
            for candidate in STATES {
                output.push(metric(
                    &STATE,
                    &[id, candidate],
                    MetricValue::Gauge(if state == *candidate {
                        1.0
                    } else {
                        0.0
                    }),
                ));
            }
            if let Some(timestamp) = entry.last_timestamp {
                output.push(metric(&LAST_SUCCESS, &[id], MetricValue::Gauge(timestamp)));
            }
            output.push(metric(&DURATION, &[id], entry.duration.snapshot()));
        }
        Some(output)
    }

    /// 业务作用：为独立宿主提供与统一结构化出口一致的文本。
    ///
    /// 参数说明：
    /// - `output`：追加 Prometheus 文本的缓冲区。
    ///
    /// 返回：只渲染缓存，不读取系统资源。
    fn render_prometheus(&self, output: &mut String) {
        render_snapshot(DESCRIPTORS, &self.snapshot().unwrap_or_default(), output);
    }
}
