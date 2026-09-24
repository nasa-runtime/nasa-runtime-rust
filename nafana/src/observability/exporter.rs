use super::{AuthMode, Identity, ObservabilityConfig};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricHub, MetricKind, MetricSample, MetricValue,
};
use prost::Message;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

/// 指标出口的固定结果域，不包含端点、凭据或错误正文。
#[derive(Default)]
pub struct ExporterState {
    values: [AtomicU64; 9],
}

/// 批次所有权同时携带终态责任；任务取消和队列释放不能绕过丢弃计数。
struct RemoteBatch {
    timestamp: i64,
    samples: Vec<MetricSample>,
    state: Arc<ExporterState>,
    outcome: usize,
}

impl Drop for RemoteBatch {
    /// 业务作用：在批次唯一所有权释放时记录一次终态，包括停机预算到期的取消。
    /// 参数说明：无。
    /// 返回：仅更新本地原子计数，不执行网络调用；未确认批次默认计入 dropped_shutdown。
    fn drop(&mut self) {
        self.state.values[self.outcome].fetch_add(1, Ordering::Relaxed);
    }
}

static INFO: MetricDescriptor = MetricDescriptor {
    name: "napp_instance_info",
    help: "进程实例库存。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static HEARTBEAT: MetricDescriptor = MetricDescriptor {
    name: "napp_observability_heartbeat_unixtime_seconds",
    help: "本进程当前观测心跳时间。",
    unit: "seconds",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static SCRAPE: MetricDescriptor = MetricDescriptor {
    name: "napp_observability_scrapes_total",
    help: "抓取请求的稳定结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["outcome"],
    histogram_bounds: &[],
};
static REMOTE: MetricDescriptor = MetricDescriptor {
    name: "napp_observability_remote_write_total",
    help: "远程写入批次的稳定结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["outcome"],
    histogram_bounds: &[],
};
static DESCRIPTORS: [&MetricDescriptor; 4] = [&INFO, &HEARTBEAT, &SCRAPE, &REMOTE];

/// 外部事实源的低频快照刷新边界，仅在 exporter 内调用。
pub trait MetricRefresh: Send + Sync {
    /// 业务作用：刷新已提交的外部事实，失败由源保留 last-good 并更新自身错误指标。
    /// 参数说明：无。
    /// 返回：受出口总超时约束的异步刷新，不影响 SQL 返回。
    fn refresh(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
}

impl LegacyMetricsSource for ExporterState {
    /// 业务作用：冻结出口自观测指标目录。
    /// 参数说明：无。
    /// 返回：静态有界的 descriptor 集合。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &DESCRIPTORS
    }
    /// 业务作用：从原子累计计数读取出口健康与进程库存。
    /// 参数说明：无。
    /// 返回：累计快照；失败不会改变业务 readiness。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let mut result = vec![
            MetricSample {
                name: INFO.name,
                labels: vec![],
                value: MetricValue::Gauge(1.0),
            },
            MetricSample {
                name: HEARTBEAT.name,
                labels: vec![],
                value: MetricValue::Gauge(now_millis() as f64 / 1000.0),
            },
        ];
        for (i, outcome) in ["ok", "unauthorized", "busy", "timeout", "failed"]
            .into_iter()
            .enumerate()
        {
            result.push(MetricSample {
                name: SCRAPE.name,
                labels: vec![("outcome", outcome.into())],
                value: MetricValue::Counter(self.values[i].load(Ordering::Relaxed)),
            });
        }
        for (i, outcome) in ["ok", "failed", "dropped_oldest", "dropped_shutdown"]
            .into_iter()
            .enumerate()
        {
            result.push(MetricSample {
                name: REMOTE.name,
                labels: vec![("outcome", outcome.into())],
                value: MetricValue::Counter(self.values[i + 5].load(Ordering::Relaxed)),
            });
        }
        Some(result)
    }
    /// 业务作用：为旧调用方保留同源文本能力。
    /// 参数说明：`output` 接收累计快照。
    /// 返回：追加完整样本，不清零计数。
    fn render_prometheus(&self, output: &mut String) {
        if let Some(samples) = self.snapshot() {
            render_samples(&samples, output);
        }
    }
}

/// 一个 Application 的出口资源；只有显式启用时才会构造。
pub struct Exporter {
    pub config: ObservabilityConfig,
    pub identity: Identity,
    hub: Arc<MetricHub>,
    state: Arc<ExporterState>,
    slots: Arc<Semaphore>,
    scrape_token: Option<nasecret::SecretBytes>,
    remote_token: Option<nasecret::SecretBytes>,
    refresh: Option<Arc<dyn MetricRefresh>>,
    source_id: String,
}

impl Exporter {
    /// 业务作用：在无网络阶段登记出口 source 并冻结认证与身份。
    /// 参数说明：`config/identity` 为校验后的策略，`hub` 为统一指标源，`secrets` 只包含本组件所需材料。
    /// 返回：可挂接 Router 或启动后台投递的资源；禁用配置返回 None 且不解析材料。
    pub fn new(
        config: ObservabilityConfig,
        identity: Identity,
        hub: Arc<MetricHub>,
        secrets: &nasecret::SecretSnapshot,
    ) -> Result<Option<Arc<Self>>, String> {
        Self::new_with_refresh(config, identity, hub, secrets, None)
    }

    /// 业务作用：为需要外部事实刷新的 Application 建立同一受管出口。
    /// 参数说明：`config/identity/hub/secrets` 与基础构造一致，`refresh` 只在采样边界调用。
    /// 返回：关闭能力时无资源；启用时刷新也遵守并发与超时上限。
    pub fn new_with_refresh(
        config: ObservabilityConfig,
        identity: Identity,
        hub: Arc<MetricHub>,
        secrets: &nasecret::SecretSnapshot,
        refresh: Option<Arc<dyn MetricRefresh>>,
    ) -> Result<Option<Arc<Self>>, String> {
        config.validate()?;
        if !config.enabled {
            return Ok(None);
        }
        let resolve = |auth: &super::AuthConfig| -> Result<Option<nasecret::SecretBytes>, String> {
            if auth.mode == AuthMode::None {
                return Ok(None);
            }
            let id = super::secret_id(
                auth.token
                    .as_deref()
                    .ok_or("missing exporter credential locator")?,
            )?;
            let material = secrets
                .get(id)
                .ok_or_else(|| format!("exporter secret {id} is unavailable"))?;
            if material.is_empty()
                || material.len() > 4096
                || std::str::from_utf8(material.expose()).is_err()
                || material.expose().iter().any(|c| *c <= 32 || *c == 127)
            {
                return Err(format!(
                    "exporter secret {id} is not a valid bearer credential"
                ));
            }
            Ok(Some(nasecret::SecretBytes::new(material.expose().to_vec())))
        };
        let scrape_token = if config.scrape_enabled() {
            resolve(&config.prometheus.scrape.auth)?
        } else {
            None
        };
        let remote_token = if config.remote_write_enabled() {
            resolve(&config.prometheus.remote_write.auth)?
        } else {
            None
        };
        let state = Arc::new(ExporterState::default());
        hub.register_legacy_source_reserved(state.clone(), 11)
            .map_err(|_| "observability descriptor or series budget conflict")?;
        Ok(Some(Arc::new(Self {
            slots: Arc::new(Semaphore::new(
                config.prometheus.scrape.max_concurrent_requests,
            )),
            config,
            identity,
            hub,
            state,
            scrape_token,
            remote_token,
            refresh,
            source_id: uuid::Uuid::new_v4().to_string(),
        })))
    }

    /// 业务作用：核对当前显式预留、存量原生指标及接口目录的完整展开占用，禁止静默截断 family。
    /// 参数说明：无。
    /// 返回：检查时的已知占用能装入单批时成功；不足时返回所需与可用数量。
    /// Application 在所有 Ready 静态登记之后、终端任务启动之前调用；独立装配者也须在静态登记完成后调用。
    /// 本检查不封闭 MetricHub，也不预留未知的运行期 label；后续动态增长仍受快照与编码硬限约束。
    pub fn validate_capacity(&self) -> Result<(), String> {
        let required = self
            .hub
            .committed_series()
            .saturating_add(crate::registry::all().len().saturating_mul(33));
        if self.config.remote_write_enabled()
            && required > self.config.prometheus.remote_write.max_batch_samples
        {
            return Err(format!(
                "remote_write.max_batch_samples is smaller than committed and current interface metric series: required={required}, available={}",
                self.config.prometheus.remote_write.max_batch_samples
            ));
        }
        Ok(())
    }

    /// 业务作用：提供与 health 开关无关的唯一 metrics 路由。
    /// 参数说明：无。
    /// 返回：已携带独立认证、并发门禁与快照超时的 Router。
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route(&self.config.prometheus.scrape.path, get(scrape))
            .with_state(self.clone())
    }

    /// 业务作用：在业务 executor 之外取得有界快照，超时不积累无界阻塞任务。
    /// 参数说明：无。
    /// 返回：带冻结身份的累计样本；并发超限与超时只影响本次导出。
    pub async fn snapshot(self: &Arc<Self>) -> Result<Vec<MetricSample>, &'static str> {
        let permit = self.slots.clone().try_acquire_owned().map_err(|_| "busy")?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.config.prometheus.scrape.request_timeout_ms);
        if let Some(refresh) = &self.refresh {
            tokio::time::timeout_at(deadline, refresh.refresh())
                .await
                .map_err(|_| "timeout")?;
        }
        let this = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            // permit 随实际快照执行结束释放，客户端取消或超时不能绕过并发硬上限。
            let _permit = permit;
            let mut samples = identity_samples(this.hub.snapshot(), &this.identity)?;
            for sample in &mut samples {
                // 来源证据在 exporter 生命周期内固定，重复业务身份不能在 remote write 中合并成一条时序。
                if sample
                    .labels
                    .iter()
                    .any(|(key, _)| *key == "napp_process_id")
                {
                    return Err("identity_conflict");
                }
                sample
                    .labels
                    .push(("napp_process_id", this.source_id.clone()));
            }
            Ok(samples)
        });
        tokio::time::timeout_at(deadline, task)
            .await
            .map_err(|_| "timeout")?
            .map_err(|_| "failed")?
    }

    /// 业务作用：监督专用抓取与远程写入，取消后停止生产并终止出口任务。
    /// 参数说明：`listener` 是 Ready 前已绑定的专用端口，`stop` 为 Application 停机信号。
    /// 返回：正常取消成功；运行期网络错误只记录观测失败，不改变数据库健康。
    pub async fn run(
        self: Arc<Self>,
        listener: Option<tokio::net::TcpListener>,
        stop: CancellationToken,
    ) {
        let mut tasks = tokio::task::JoinSet::new();
        if let Some(listener) = listener {
            let router = self.router();
            let cancel = stop.clone();
            tasks.spawn(async move {
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(cancel.cancelled_owned())
                    .await;
            });
        }
        if self.config.remote_write_enabled() {
            let this = self.clone();
            let cancel = stop.clone();
            tasks.spawn(async move {
                this.remote_loop(cancel).await;
            });
        }
        loop {
            tokio::select! { _ = stop.cancelled() => break, result = tasks.join_next(), if !tasks.is_empty() => {
                if result.is_some() { tracing::warn!(target:"napp::observability", event="exporter_task_stopped", "观测出口任务停止"); }
            } }
        }
        // 先撤销生产权，再在独立排空预算内等待；到期取消剩余请求，不延长业务全局停机预算。
        let drain = Duration::from_millis(
            self.config
                .prometheus
                .remote_write
                .shutdown_drain_timeout_ms,
        );
        if tokio::time::timeout(drain, async { while tasks.join_next().await.is_some() {} })
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }

    /// 业务作用：以独立采样者和单一发送者实现有界、保序的累计 remote write。
    /// 参数说明：`stop` 为停机权威。
    /// 返回：取消后退出；满队列丢弃最旧等待批次，不对 SQL 施加背压。
    async fn remote_loop(self: Arc<Self>, stop: CancellationToken) {
        let settings = &self.config.prometheus.remote_write;
        let Ok(client) = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(settings.request_timeout_ms))
            .build()
        else {
            self.state.values[6].fetch_add(1, Ordering::Relaxed);
            stop.cancelled().await;
            return;
        };
        let queue = Arc::new(Mutex::new(VecDeque::<RemoteBatch>::new()));
        let ready = Arc::new(Notify::new());
        let producer = {
            let this = self.clone();
            let queue = queue.clone();
            let ready = ready.clone();
            let stop = stop.clone();
            async move {
                let mut interval = tokio::time::interval(Duration::from_millis(
                    this.config.prometheus.remote_write.interval_ms,
                ));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut previous = 0;
                loop {
                    tokio::select! { _ = stop.cancelled() => break, _ = interval.tick() => {
                        match this.snapshot().await {
                            Ok(samples) => {
                                let timestamp = now_millis().max(previous + 1); previous = timestamp;
                                let mut queued = queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                if queued.len() == this.config.prometheus.remote_write.queue_capacity {
                                    if let Some(mut discarded) = queued.pop_front() { discarded.outcome = 7; }
                                }
                                queued.push_back(RemoteBatch { timestamp, samples, state: this.state.clone(), outcome: 8 }); drop(queued); ready.notify_one();
                            },
                            Err(_) => { this.state.values[6].fetch_add(1, Ordering::Relaxed); }
                        }
                    } }
                }
            }
        };
        let consumer = async {
            loop {
                let next = queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front();
                let Some(mut batch) = next else {
                    tokio::select! { _ = stop.cancelled() => break, _ = ready.notified() => continue };
                };
                let body = match encode_remote_write(
                    &batch.samples,
                    batch.timestamp,
                    settings.max_batch_samples,
                ) {
                    Ok(body) => body,
                    Err(_) => {
                        batch.outcome = 6;
                        continue;
                    }
                };
                let deadline =
                    tokio::time::Instant::now() + Duration::from_millis(settings.interval_ms);
                let mut success = false;
                for attempt in 0..settings.retry_max_attempts {
                    let mut request = client
                        .post(settings.endpoint.as_deref().unwrap_or_default())
                        .header("Content-Encoding", "snappy")
                        .header("Content-Type", "application/x-protobuf")
                        .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                        .header("User-Agent", concat!("nafana/", env!("CARGO_PKG_VERSION")))
                        .body(body.clone());
                    if let Some(token) = &self.remote_token {
                        request = request
                            .bearer_auth(std::str::from_utf8(token.expose()).unwrap_or_default());
                    }
                    let response = tokio::time::timeout_at(deadline, request.send()).await;
                    match response {
                        Ok(Ok(response)) if response.status().is_success() => {
                            success = true;
                            break;
                        }
                        Ok(Ok(response))
                            if !response.status().is_server_error()
                                && response.status().as_u16() != 429 =>
                        {
                            break
                        }
                        _ => {}
                    }
                    // 单发送者在同一周期预算内重试，旧批次不会在新批次之后提交。
                    if attempt + 1 < settings.retry_max_attempts
                        && tokio::time::timeout_at(
                            deadline,
                            tokio::time::sleep(Duration::from_millis(settings.retry_backoff_ms)),
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                batch.outcome = if success {
                    5
                } else {
                    6
                };
                if !success {
                    tracing::warn!(target:"napp::observability", event="remote_write_failed", "远程指标批次未确认");
                }
            }
        };
        tokio::pin!(producer, consumer);
        tokio::select! {
            _ = stop.cancelled() => { let _ = tokio::time::timeout(Duration::from_millis(settings.shutdown_drain_timeout_ms), &mut consumer).await; },
            _ = &mut producer => { let _ = tokio::time::timeout(Duration::from_millis(settings.shutdown_drain_timeout_ms), &mut consumer).await; },
            _ = &mut consumer => {}
        }
    }
}

/// 业务作用：先验证凭据再申请快照槽，避免未授权请求占用采集预算。
/// 参数说明：`state` 为冻结出口，`headers` 只读取 Authorization。
/// 返回：成功文本或稳定 HTTP 错误，不包含 token 或内部错误。
async fn scrape(State(state): State<Arc<Exporter>>, headers: HeaderMap) -> Response {
    if let Some(token) = &state.scrape_token {
        if headers.get_all("authorization").iter().count() != 1 {
            state.state.values[1].fetch_add(1, Ordering::Relaxed);
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let supplied = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or_default()
            .as_bytes();
        let mut difference = supplied.len() ^ token.len();
        for (index, expected) in token.expose().iter().enumerate() {
            difference |= usize::from(*expected ^ supplied.get(index).copied().unwrap_or_default());
        }
        if difference != 0 {
            state.state.values[1].fetch_add(1, Ordering::Relaxed);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    match state.snapshot().await {
        Ok(samples) => {
            state.state.values[0].fetch_add(1, Ordering::Relaxed);
            let mut output = String::new();
            render_samples(&samples, &mut output);
            (
                [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
                output,
            )
                .into_response()
        }
        Err(kind) => {
            state.state.values[match kind {
                "busy" => 2,
                "timeout" => 3,
                _ => 4,
            }]
            .fetch_add(1, Ordering::Relaxed);
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

/// 业务作用：只在快照边界附加部署身份，阻止组件覆盖进程主键。
/// 参数说明：`samples` 为 Hub 累计快照，`identity` 为启动时冻结身份。
/// 返回：同源身份样本；冲突 label 拒绝本次出口。
pub fn identity_samples(
    mut samples: Vec<MetricSample>,
    identity: &Identity,
) -> Result<Vec<MetricSample>, &'static str> {
    for sample in &mut samples {
        for (key, value) in &identity.labels {
            if sample.labels.iter().any(|(name, _)| name == key) {
                return Err("identity_conflict");
            }
            sample.labels.push((key, value.clone()));
        }
    }
    Ok(samples)
}

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
}
#[derive(Clone, PartialEq, Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}
#[derive(Clone, PartialEq, Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}
#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

/// 业务作用：将同源累计快照编码为有序 label 的 Prometheus protobuf 与 Snappy block。
/// 参数说明：`samples` 为快照，`timestamp` 为毫秒时间戳，`limit` 为完整批次样本上限。
/// 返回：协议载荷；超限时整体拒绝，不截断 histogram。
pub fn encode_remote_write(
    samples: &[MetricSample],
    timestamp: i64,
    limit: usize,
) -> Result<Vec<u8>, &'static str> {
    let flat = flatten(samples);
    if flat.len() > limit {
        return Err("remote_write_sample_limit");
    }
    let timeseries = flat
        .into_iter()
        .map(|(name, mut labels, value)| {
            labels.insert("__name__".to_owned(), name);
            TimeSeries {
                labels: labels
                    .into_iter()
                    .filter(|(_, value)| !value.is_empty())
                    .map(|(name, value)| Label { name, value })
                    .collect(),
                samples: vec![Sample { value, timestamp }],
            }
        })
        .collect();
    snap::raw::Encoder::new()
        .compress_vec(&WriteRequest { timeseries }.encode_to_vec())
        .map_err(|_| "remote_write_encoding")
}

/// 业务作用：统一展开累计 histogram，保证文本与 remote write 桶完全一致。
/// 参数说明：`samples` 为结构化快照。
/// 返回：名称、排序标签和数值组成的完整系列。
fn flatten(samples: &[MetricSample]) -> Vec<(String, BTreeMap<String, String>, f64)> {
    let mut output = Vec::new();
    for sample in samples {
        let labels: BTreeMap<String, String> = sample
            .labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        match &sample.value {
            MetricValue::Counter(v) => output.push((sample.name.into(), labels, *v as f64)),
            MetricValue::Gauge(v) => output.push((sample.name.into(), labels, *v)),
            MetricValue::Histogram {
                bounds,
                buckets,
                sum,
                count,
            } => {
                let mut cumulative = 0u64;
                for (index, bound) in bounds.iter().enumerate() {
                    cumulative =
                        cumulative.saturating_add(buckets.get(index).copied().unwrap_or_default());
                    let mut bucket_labels = labels.clone();
                    bucket_labels.insert("le".into(), bound.to_string());
                    output.push((
                        format!("{}_bucket", sample.name),
                        bucket_labels,
                        cumulative as f64,
                    ));
                }
                let mut inf = labels.clone();
                inf.insert("le".into(), "+Inf".into());
                output.push((format!("{}_bucket", sample.name), inf, *count as f64));
                output.push((format!("{}_sum", sample.name), labels.clone(), *sum));
                output.push((format!("{}_count", sample.name), labels, *count as f64));
            }
        }
    }
    output
}

/// 业务作用：把完整快照编码为 Prometheus 文本，不在 SQL 路径执行。
/// 参数说明：`samples` 为同源快照，`output` 为接收缓冲。
/// 返回：追加所有系列，保留 NaN 与无数据语义。
fn render_samples(samples: &[MetricSample], output: &mut String) {
    use std::fmt::Write;
    let mut seen = std::collections::BTreeSet::new();
    for sample in samples {
        if seen.insert(sample.name) {
            let kind = match sample.value {
                MetricValue::Counter(_) => "counter",
                MetricValue::Gauge(_) => "gauge",
                MetricValue::Histogram { .. } => "histogram",
            };
            let _ = writeln!(output, "# TYPE {} {kind}", sample.name);
        }
    }
    for (name, labels, value) in flatten(samples) {
        let labels = labels
            .iter()
            .map(|(k, v)| {
                format!(
                    "{k}=\"{}\"",
                    v.replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\n', "\\n")
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(output, "{name}{{{labels}}} {value}");
    }
}

/// 业务作用：提供仅用于出口样本与心跳的 Unix 毫秒时间。
/// 参数说明：无。
/// 返回：时间异常时回落零；业务时长不使用墙上时钟。
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
