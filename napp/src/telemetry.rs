//! OpenTelemetry trace 与指标导出组件。
//!
//! `TelemetryComponent` 拥有一条**有界 span 导出管道**的生命周期:Start 按配置创建
//! [`BoundedSpanExporter`](natelemetry::BoundedSpanExporter) 并发布给生产者(Web trace 中间件在本
//! 组件激活时对每个请求产一个服务端 span),同时 spawn 一个**受管 drainer** 持续把 span 送到 sink;
//! 停机时先取消 drainer、再在全局剩余预算内 flush 已缓冲的 span。
//!
//! 组件顺序固定为 `log -> nacos-config? -> telemetry -> db/redis/kafka/web`:telemetry 早于流量
//! 入口启动,故其 Start 停机 action 在逆序栈中靠后弹出——即在 Web/DB 等 span 生产者停止后才做最终
//! flush,不遗漏在途 span。telemetry **不把 auto 强制成 Service**:Batch 也执行 Start 与停机,
//! 因此管道在两种模式下都能建立并在退出前 flush。
//!
//! sink 两种:缺省 = **结构化日志**(每条 span 打 `trace_id`/`span_id`/`name`,交付「日志 trace-id 关联」);
//! 配了 `telemetry.otlp_endpoint` = **OTLP/HTTP wire exporter**——drainer 批量把 span 编码成
//! `ExportTraceServiceRequest` POST 到 collector,失败只 warn 降级、不背压业务。编码由 `telemetry.otlp_encoding`
//! 选:`json`(缺省,proto3 JSON 映射)或 `protobuf`(二进制 wire,OTLP 规范强制服务端支持,兼容只收 protobuf 的
//! collector)。显式配置 `telemetry.otlp_metrics_endpoint` 时，另一受管任务按周期从
//! 唯一 `MetricHub` 读取结构化快照；默认关闭，失败无重试且不背压业务。
//! trace 与 metrics 共用 resource 身份、编码选择与反向停机 flush 预算，不含 payload/属性正文。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use natelemetry::{BoundedSpanExporter, SpanRecord};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, ShutdownAction, ShutdownContext,
    StartContext,
};

/// 有界队列容量缺省值:2048 条 span(满则丢弃并计数,绝不背压业务)。
const DEFAULT_QUEUE_CAPACITY: usize = 2048;
/// 配置可声明的队列硬上限，避免错误配置在启动时申请不合理资源。
const MAX_QUEUE_CAPACITY: usize = 1_000_000;
/// OTLP 指标快照的默认导出周期。
const DEFAULT_METRICS_INTERVAL_MS: u64 = 10_000;
/// 指标导出周期下界，避免配置将 collector 失效放大成密集请求。
const MIN_METRICS_INTERVAL_MS: u64 = 1_000;
/// 指标导出周期上界，避免运行者误以为已启用实时观测。
const MAX_METRICS_INTERVAL_MS: u64 = 300_000;

/// OTLP 指标出口的进程级累计摘要。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtlpMetricsSnapshot {
    /// 已发起的 HTTP 批次数，每个周期最多一次。
    pub attempted_batches: u64,
    /// collector 确认成功的批次数。
    pub exported_batches: u64,
    /// 成功批次中包含的数据点数。
    pub exported_samples: u64,
    /// 网络失败或非成功 HTTP 结果的批次数。
    pub failed_batches: u64,
}

/// 导出任务与 Application 管理面共享的原子统计。
pub(crate) struct OtlpMetricsState {
    attempted_batches: AtomicU64,
    exported_batches: AtomicU64,
    exported_samples: AtomicU64,
    failed_batches: AtomicU64,
}

impl OtlpMetricsState {
    /// 业务作用：创建全部计数为零的指标出口状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由单一导出任务写入、Application 并发读取的原子状态。
    fn new() -> Self {
        Self {
            attempted_batches: AtomicU64::new(0),
            exported_batches: AtomicU64::new(0),
            exported_samples: AtomicU64::new(0),
            failed_batches: AtomicU64::new(0),
        }
    }

    /// 业务作用：记录一次无重试 HTTP 导出的结果。
    ///
    /// 参数说明：
    /// - `sample_count`: 本批快照的数据点数。
    /// - `success`: collector 是否返回成功 HTTP 状态。
    ///
    /// 返回：无；成功只累加已确认样本，失败只累加失败批次。
    fn record(&self, sample_count: usize, success: bool) {
        self.attempted_batches.fetch_add(1, Ordering::Relaxed);
        if success {
            self.exported_batches.fetch_add(1, Ordering::Relaxed);
            self.exported_samples
                .fetch_add(sample_count as u64, Ordering::Relaxed);
        } else {
            self.failed_batches.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 业务作用：读取不含端点与 label 的累计摘要。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：各原子计数的当前快照；读取不清零。
    pub(crate) fn snapshot(&self) -> OtlpMetricsSnapshot {
        OtlpMetricsSnapshot {
            attempted_batches: self.attempted_batches.load(Ordering::Relaxed),
            exported_batches: self.exported_batches.load(Ordering::Relaxed),
            exported_samples: self.exported_samples.load(Ordering::Relaxed),
            failed_batches: self.failed_batches.load(Ordering::Relaxed),
        }
    }
}

/// 遥测组件负责读取的顶层配置根投影。
#[derive(Default, Deserialize)]
#[serde(default)]
struct TelemetryConfigRoot {
    telemetry: Option<TelemetryConfig>,
}

/// OTLP/HTTP 载荷编码(对齐 `OTEL_EXPORTER_OTLP_PROTOCOL` 的 http 变体):`json` = proto3 JSON 映射(缺省,
/// 通用 collector 都接收);`protobuf` = protobuf 二进制(OTLP 规范强制服务端支持,兼容只收 protobuf 的 collector)。
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum OtlpEncoding {
    /// proto3 JSON 映射,`Content-Type: application/json`。
    #[default]
    Json,
    /// protobuf 二进制,`Content-Type: application/x-protobuf`。
    Protobuf,
}

/// `telemetry` 配置段。`deny_unknown_fields`:拼写错误在建立管道前即被拒。
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TelemetryConfig {
    /// 运行期 kill-switch;`false` 时组件成为无副作用空操作(不建 exporter、不 spawn drainer)。
    enabled: bool,
    /// 服务名(低基数,进 span 诊断上下文与 OTLP resource `service.name`);缺省空串。
    service_name: String,
    /// OTLP resource `service.instance.id`；空值由 Application 按进程启动事实生成。
    service_instance_id: String,
    /// 有界导出队列容量;缺省 2048,必须 ≥ 1。
    queue_capacity: usize,
    /// OTLP/HTTP traces 端点(如 `http://collector:4318/v1/traces`);设置即 sink 换成 OTLP 导出器,
    /// 否则保持日志 sink。必须是合法 http(s) URL。
    #[serde(default)]
    otlp_endpoint: Option<String>,
    /// OTLP/HTTP metrics 端点；未设置时指标导出完全关闭，Prometheus 文本出口保持。
    #[serde(default)]
    otlp_metrics_endpoint: Option<String>,
    /// 结构化指标快照导出周期，单位毫秒。
    metrics_interval_ms: u64,
    /// OTLP trace 与 metrics 载荷编码；缺省 `json`，任一 OTLP endpoint 启用时生效。
    #[serde(default)]
    otlp_encoding: OtlpEncoding,
    /// 没有上游 traceparent 时的新根链路采样率；0.0=全关，1.0=全开。
    root_sample_ratio: f64,
}

impl Default for TelemetryConfig {
    /// 业务作用：使用启用状态、空服务名、2048 队列和 JSON OTLP 的保守缺省。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回不启用外部 OTLP 指标出口、但允许应用按需建立 span 管道的配置。
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: String::new(),
            service_instance_id: String::new(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            otlp_endpoint: None,
            otlp_metrics_endpoint: None,
            metrics_interval_ms: DEFAULT_METRICS_INTERVAL_MS,
            otlp_encoding: OtlpEncoding::Json,
            root_sample_ratio: 1.0,
        }
    }
}

impl TelemetryConfig {
    /// 业务作用：无副作用校验:队列容量必须 ≥ 1(0 容量的 mpsc 会阻塞每次入队);`otlp_endpoint` 必须是合法 http(s) URL。
    ///
    /// 参数说明：
    /// - `phase`: 本次校验所属生命周期阶段，用于错误归因。
    ///
    /// 返回：配置满足队列、身份、采样率、端点与导出周期边界时成功，否则返回 Telemetry 错误。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<()> {
        if self.enabled && self.queue_capacity == 0 {
            return Err(telemetry_error(
                phase,
                "telemetry.queue_capacity must be greater than zero",
            ));
        }
        if self.enabled && self.queue_capacity > MAX_QUEUE_CAPACITY {
            return Err(telemetry_error(
                phase,
                format!("telemetry.queue_capacity must not exceed {MAX_QUEUE_CAPACITY}"),
            ));
        }
        if self.enabled && self.service_name.len() > 128 {
            return Err(telemetry_error(
                phase,
                "telemetry.service_name must not exceed 128 bytes",
            ));
        }
        if self.enabled && self.service_instance_id.len() > 128 {
            return Err(telemetry_error(
                phase,
                "telemetry.service_instance_id must not exceed 128 bytes",
            ));
        }
        if self.enabled
            && (!self.root_sample_ratio.is_finite()
                || !(0.0..=1.0).contains(&self.root_sample_ratio))
        {
            return Err(telemetry_error(
                phase,
                "telemetry.root_sample_ratio must be between 0.0 and 1.0",
            ));
        }
        validate_otlp_endpoint(self.otlp_endpoint.as_deref(), "otlp_endpoint", phase)?;
        validate_otlp_endpoint(
            self.otlp_metrics_endpoint.as_deref(),
            "otlp_metrics_endpoint",
            phase,
        )?;
        if self.enabled
            && self
                .otlp_metrics_endpoint
                .as_deref()
                .is_some_and(|endpoint| !endpoint.trim().is_empty())
            && !(MIN_METRICS_INTERVAL_MS..=MAX_METRICS_INTERVAL_MS)
                .contains(&self.metrics_interval_ms)
        {
            return Err(telemetry_error(
                phase,
                format!(
                    "telemetry.metrics_interval_ms must be between {MIN_METRICS_INTERVAL_MS} and \
                     {MAX_METRICS_INTERVAL_MS} when OTLP metrics are enabled"
                ),
            ));
        }
        Ok(())
    }
}

/// 业务作用：校验可选 OTLP/HTTP 端点的 URL 结构与传输协议。
///
/// 参数说明：
/// - `endpoint`: 配置中的可选端点。
/// - `field`: 用于错误定位的稳定配置键名。
/// - `phase`: 校验所属生命周期阶段。
///
/// 返回：空值或 http(s) URL 成功；非法 URL 或其它协议返回 Telemetry 错误。
fn validate_otlp_endpoint(
    endpoint: Option<&str>,
    field: &str,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(endpoint) = endpoint.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    let parsed = reqwest::Url::parse(endpoint).map_err(|error| {
        telemetry_error_src(
            phase,
            format!("telemetry.{field} is not a valid URL"),
            error,
        )
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(telemetry_error(
            phase,
            format!("telemetry.{field} must use http or https"),
        ));
    }
    Ok(())
}

/// 遥测组件:Start 建管道并发布 exporter,停机 flush。
pub(crate) struct TelemetryComponent {
    config: Option<TelemetryConfig>,
}

impl TelemetryComponent {
    /// 业务作用：创建尚未读取配置的遥测组件。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回等待 Start 阶段按最终配置建立导出管道的组件。
    pub(crate) fn new() -> Self {
        Self { config: None }
    }
}

impl ApplicationComponent for TelemetryComponent {
    /// 业务作用：返回遥测组件稳定身份。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回 Runner 用于归类遥测生命周期错误的 `telemetry` 身份。
    fn id(&self) -> ComponentId {
        ComponentId::Telemetry
    }

    /// 业务作用：读取并冻结配置,创建有界导出管道、发布 exporter、spawn drainer 并压入停机 flush action。
    ///
    /// 管道在 Start(而非 Ready)建立:Batch 模式不执行 Ready,但 Start 与停机都会执行,因此两种模式
    /// 都能建立管道并在退出前 flush。exporter 在流量入口 Ready 之前发布,生产者一旦运行即可入队。
    ///
    /// 参数说明：
    /// - `context`: 提供最终配置、Application 与 active stack 的 Start 上下文。
    ///
    /// 返回：管道发布且停机 action 建立后成功；配置、客户端或唯一发布失败时拒绝启动。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let config = read_telemetry_config(context.application())?;
            config.validate(ApplicationPhase::Start)?;
            if !config.enabled {
                tracing::info!(
                    "telemetry component is disabled by configuration; no span pipeline"
                );
                self.config = Some(config);
                return Ok(());
            }

            let (mut exporter, receiver) = BoundedSpanExporter::channel(config.queue_capacity);
            exporter
                .set_root_sample_ratio(config.root_sample_ratio)
                .map_err(|error| {
                    telemetry_error_src(
                        ApplicationPhase::Start,
                        "telemetry.root_sample_ratio is invalid",
                        error,
                    )
                })?;
            let exporter = Arc::new(exporter);
            let contributor = context.application().register_readiness(
                ComponentId::Telemetry,
                Arc::<str>::from("telemetry:exporter"),
                ReadinessPolicy {
                    affects_ready: false,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: None,
                },
            )?;
            contributor.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            );
            // 先发布再 spawn:发布后 Web 等生产者(在其 Ready 时)即可取到 exporter 入队。
            context
                .application()
                .publish_telemetry_exporter(Arc::clone(&exporter))?;

            // span 与 metrics 各有独立的非关键 readiness 证据，一个出口的恢复不会掩盖另一个失效。
            let sink = build_span_sink(&config)?;
            let cancel = CancellationToken::new();
            let span_drainer = tokio::spawn(drain_spans(
                receiver,
                cancel.clone(),
                sink,
                Arc::clone(&exporter),
                contributor,
            ));
            let mut drainers = vec![span_drainer];
            if let Some(metrics_sink) = build_metrics_sink(&config)? {
                context
                    .application()
                    .publish_otlp_metrics_state(Arc::clone(&metrics_sink.state))?;
                let metrics_contributor = context.application().register_readiness(
                    ComponentId::Telemetry,
                    Arc::<str>::from("telemetry:metrics-exporter"),
                    ReadinessPolicy {
                        affects_ready: false,
                        failure_threshold: 1,
                        recovery_threshold: 1,
                        stale_after: None,
                    },
                )?;
                metrics_contributor.observe(
                    DependencyState::Ready,
                    reason::HEALTHY,
                    std::time::Instant::now(),
                );
                drainers.push(tokio::spawn(export_metrics(
                    cancel.clone(),
                    metrics_sink,
                    context.application().clone(),
                    system_time_nanos(context.application().info().started_at()),
                    metrics_contributor,
                )));
            }
            // Start action:telemetry 声明早,其停机 action 在逆序栈中靠后弹出 → 在流量入口停止后 flush。
            context.activate(Box::new(TelemetryFlush {
                cancel,
                drainers,
                exporter,
            }));
            self.config = Some(config);
            Ok(())
        })
    }
}

/// span 导出目的地:日志 sink(交付「日志 trace-id 关联」)或 OTLP/HTTP JSON wire exporter。
enum SpanSink {
    /// 结构化日志 sink:每条 span 打 `trace_id`/`span_id`/`name`,不含 payload/属性正文。
    Log,
    /// OTLP/HTTP exporter:批量 POST `ExportTraceServiceRequest`(JSON 或 protobuf 编码)到 collector。
    Otlp {
        /// 复用的 reqwest 客户端(已设超时)。
        client: reqwest::Client,
        /// OTLP/HTTP traces 端点。
        endpoint: String,
        /// resource `service.name`。
        service_name: String,
        /// resource `service.instance.id`。
        service_instance_id: String,
        /// 载荷编码(JSON / protobuf)。
        encoding: OtlpEncoding,
    },
}

impl SpanSink {
    /// 业务作用：导出一批 span。日志 sink 逐条打日志;OTLP sink 批量 POST,失败只 warn 降级(不背压、不 panic)。
    ///
    /// 参数说明：
    /// - `batch`: 待导出的一批 span。
    ///
    /// 返回：日志写入或 collector 确认成功时返回 `true`；网络或 HTTP 失败时返回 `false`。
    async fn ship(&self, batch: &[SpanRecord]) -> bool {
        match self {
            SpanSink::Log => {
                for span in batch {
                    tracing::debug!(
                        target: "telemetry",
                        trace_id = %span.trace_id_hex,
                        span_id = %span.span_id_hex,
                        name = %span.name,
                        "span"
                    );
                }
                true
            }
            SpanSink::Otlp {
                client,
                endpoint,
                service_name,
                service_instance_id,
                encoding,
            } => {
                let (content_type, body): (&str, Vec<u8>) = match encoding {
                    OtlpEncoding::Json => (
                        "application/json",
                        otlp_traces_json(batch, service_name, service_instance_id).into_bytes(),
                    ),
                    OtlpEncoding::Protobuf => (
                        "application/x-protobuf",
                        otlp_traces_protobuf(batch, service_name, service_instance_id),
                    ),
                };
                match client
                    .post(endpoint)
                    .header(reqwest::header::CONTENT_TYPE, content_type)
                    .body(body)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => true,
                    Ok(response) => {
                        tracing::warn!(
                            "telemetry OTLP export got HTTP {} (dropping {} span(s))",
                            response.status().as_u16(),
                            batch.len()
                        );
                        false
                    }
                    Err(error) => {
                        tracing::warn!(
                            "telemetry OTLP export failed, dropping {} span(s): {error}",
                            batch.len()
                        );
                        false
                    }
                }
            }
        }
    }
}

/// 业务作用：按配置构造 span sink:配了 `otlp_endpoint` 则 OTLP JSON 导出,否则日志 sink。
///
/// 参数说明：
/// - `config`: 已校验并补齐 resource 身份的遥测配置。
///
/// 返回：未配端点时返回日志 sink；配置端点时返回受超时约束的 OTLP sink；客户端无法建立时拒绝 Start。
fn build_span_sink(config: &TelemetryConfig) -> ApplicationResult<SpanSink> {
    match config
        .otlp_endpoint
        .as_deref()
        .filter(|e| !e.trim().is_empty())
    {
        Some(endpoint) => {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| {
                    telemetry_error_src(
                        ApplicationPhase::Start,
                        "telemetry OTLP exporter client build failed",
                        error,
                    )
                })?;
            Ok(SpanSink::Otlp {
                client,
                endpoint: endpoint.to_owned(),
                service_name: config.service_name.clone(),
                service_instance_id: config.service_instance_id.clone(),
                encoding: config.otlp_encoding,
            })
        }
        None => Ok(SpanSink::Log),
    }
}

/// OTLP/HTTP 指标出口：每个周期只读取一次统一 hub 快照并发送一次。
struct MetricsSink {
    client: reqwest::Client,
    endpoint: String,
    service_name: String,
    service_instance_id: String,
    encoding: OtlpEncoding,
    interval: Duration,
    state: Arc<OtlpMetricsState>,
}

impl MetricsSink {
    /// 业务作用：将一份结构化 family 快照编码并以单次 HTTP 请求发送。
    ///
    /// 参数说明：
    /// - `families`: 从统一 `MetricHub` 一次读取得到的当前值。
    /// - `start_time_unix_nano`: 进程指标累计起点。
    /// - `time_unix_nano`: 本次快照时刻。
    ///
    /// 返回：没有数据点时返回 `None` 且不发请求；否则返回 collector 是否确认成功。
    async fn ship(
        &self,
        families: &[nametrics_core::MetricFamilySnapshot],
        start_time_unix_nano: u64,
        time_unix_nano: u64,
    ) -> Option<bool> {
        let sample_count = families
            .iter()
            .map(|family| family.samples.len())
            .sum::<usize>();
        if sample_count == 0 {
            return None;
        }
        let (content_type, body): (&str, Vec<u8>) = match self.encoding {
            OtlpEncoding::Json => (
                "application/json",
                otlp_metrics_json(
                    families,
                    &self.service_name,
                    &self.service_instance_id,
                    start_time_unix_nano,
                    time_unix_nano,
                )
                .into_bytes(),
            ),
            OtlpEncoding::Protobuf => (
                "application/x-protobuf",
                otlp_metrics_protobuf(
                    families,
                    &self.service_name,
                    &self.service_instance_id,
                    start_time_unix_nano,
                    time_unix_nano,
                ),
            ),
        };
        let success = match self
            .client
            .post(&self.endpoint)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => true,
            Ok(response) => {
                tracing::warn!(
                    "telemetry OTLP metrics export got HTTP {} (discarding one snapshot with {} sample(s))",
                    response.status().as_u16(),
                    sample_count
                );
                false
            }
            Err(error) => {
                tracing::warn!(
                    "telemetry OTLP metrics export failed, discarding one snapshot with {} sample(s): {error}",
                    sample_count
                );
                false
            }
        };
        self.state.record(sample_count, success);
        Some(success)
    }
}

/// 业务作用：在显式配置指标端点时创建 OTLP 出口，默认保持关闭。
///
/// 参数说明：
/// - `config`: 已校验并补齐 resource 身份的遥测配置。
///
/// 返回：未配 endpoint 返回 `None`；配置后返回无重试的受管 sink；客户端无法建立时拒绝 Start。
fn build_metrics_sink(config: &TelemetryConfig) -> ApplicationResult<Option<MetricsSink>> {
    let Some(endpoint) = config
        .otlp_metrics_endpoint
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| {
            telemetry_error_src(
                ApplicationPhase::Start,
                "telemetry OTLP metrics exporter client build failed",
                error,
            )
        })?;
    Ok(Some(MetricsSink {
        client,
        endpoint: endpoint.to_owned(),
        service_name: config.service_name.clone(),
        service_instance_id: config.service_instance_id.clone(),
        encoding: config.otlp_encoding,
        interval: Duration::from_millis(config.metrics_interval_ms),
        state: Arc::new(OtlpMetricsState::new()),
    }))
}

/// 业务作用：按受控周期导出统一指标快照，停机时再刷新一次最终事实。
///
/// 失败只更新非关键 readiness 与导出计数，不重试、不背压业务；
/// `MissedTickBehavior::Skip` 防止 collector 超时后补发积压的 tick。
///
/// 参数说明：
/// - `cancel`: 由逆序停机 action 在业务入口停止后触发。
/// - `sink`: 唯一指标 HTTP 出口。
/// - `application`: 拥有唯一指标目录并可刷新持久化事实源的容器。
/// - `start_time_unix_nano`: 应用启动墙上时钟。
/// - `contributor`: 只影响观测状态的 readiness 贡献项。
///
/// 返回：无；任务只在取消或运行时结束，取消分支完成最终单次 flush。
async fn export_metrics(
    cancel: CancellationToken,
    sink: MetricsSink,
    application: Application,
    start_time_unix_nano: u64,
    contributor: ReadinessContributor,
) {
    let first = tokio::time::Instant::now() + sink.interval;
    let mut ticker = tokio::time::interval_at(first, sink.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                export_metrics_once(&sink, &application, start_time_unix_nano, &contributor).await;
            }
            _ = cancel.cancelled() => break,
        }
    }
    export_metrics_once(&sink, &application, start_time_unix_nano, &contributor).await;
}

/// 业务作用：先请求外部已提交事实的低频缓存刷新，再对统一 hub 只取一次快照并发送。
///
/// 参数说明：
/// - `sink`: OTLP 指标出口。
/// - `application`: 持有唯一指标快照源与外部源刷新入口的容器。
/// - `start_time_unix_nano`: 累计起点。
/// - `contributor`: 导出结果的 readiness 贡献项。
///
/// 返回：无；空快照不发 HTTP，有数据时每次恰好一个请求。
async fn export_metrics_once(
    sink: &MetricsSink,
    application: &Application,
    start_time_unix_nano: u64,
    contributor: &ReadinessContributor,
) {
    let refresh_succeeded = if application.state() == ApplicationState::Ready {
        match application.refresh_metric_sources().await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(
                    "telemetry metric source refresh failed; exporting the last complete snapshot: {error}"
                );
                false
            }
        }
    } else {
        // 停机栈进入 `Stopping` 后数据库可能已释放，外部事实由其组件在释放前发布最终缓存；
        // 此处只读取缓存，避免最终 flush 重新访问已失效的依赖。
        true
    };
    let families = application.metrics_hub().family_snapshot();
    let now = system_time_nanos(SystemTime::now()).max(start_time_unix_nano);
    match sink.ship(&families, start_time_unix_nano, now).await {
        Some(true) if refresh_succeeded => contributor.observe(
            DependencyState::Ready,
            reason::HEALTHY,
            std::time::Instant::now(),
        ),
        Some(true) | Some(false) => contributor.observe(
            DependencyState::Degraded,
            reason::DEGRADED,
            std::time::Instant::now(),
        ),
        None => {}
    }
}

/// 业务作用：将统一 family 快照编码为 OTLP/HTTP JSON 指标请求。
///
/// Counter 与 histogram 使用 cumulative temporality，gauge 保留当前值；直方图显式边界
/// 直接来自唯一 descriptor，不重建另一份 bucket 配置。
///
/// 参数说明：
/// - `families`: 含 descriptor 语义与当前数据点的快照。
/// - `service_name`: resource 服务名。
/// - `service_instance_id`: resource 进程实例身份。
/// - `start_time_unix_nano`: 累计起点。
/// - `time_unix_nano`: 快照时刻。
///
/// 返回：符合 proto3 JSON 映射的 `ExportMetricsServiceRequest` 字符串。
fn otlp_metrics_json(
    families: &[nametrics_core::MetricFamilySnapshot],
    service_name: &str,
    service_instance_id: &str,
    start_time_unix_nano: u64,
    time_unix_nano: u64,
) -> String {
    let metrics = families
        .iter()
        .map(|family| {
            let points = family
                .samples
                .iter()
                .map(|sample| {
                    let attributes = metric_attributes_json(&sample.labels);
                    match &sample.value {
                        nametrics_core::MetricValue::Counter(value) => {
                            let mut point = serde_json::json!({
                                "attributes": attributes,
                                "startTimeUnixNano": start_time_unix_nano.to_string(),
                                "timeUnixNano": time_unix_nano.to_string(),
                            });
                            if *value <= i64::MAX as u64 {
                                point["asInt"] = serde_json::Value::String(value.to_string());
                            } else {
                                point["asDouble"] = otlp_json_double(*value as f64);
                            }
                            point
                        }
                        nametrics_core::MetricValue::Gauge(value) => serde_json::json!({
                            "attributes": attributes,
                            "timeUnixNano": time_unix_nano.to_string(),
                            "asDouble": otlp_json_double(*value),
                        }),
                        nametrics_core::MetricValue::Histogram {
                            buckets,
                            sum,
                            count,
                            ..
                        } => serde_json::json!({
                            "attributes": attributes,
                            "startTimeUnixNano": start_time_unix_nano.to_string(),
                            "timeUnixNano": time_unix_nano.to_string(),
                            "count": count.to_string(),
                            "sum": otlp_json_double(*sum),
                            "bucketCounts": buckets.iter().map(u64::to_string).collect::<Vec<_>>(),
                            "explicitBounds": family.histogram_bounds,
                        }),
                    }
                })
                .collect::<Vec<_>>();
            let data = match family.kind {
                nametrics_core::MetricKind::Counter => serde_json::json!({
                    "sum": {
                        "dataPoints": points,
                        "aggregationTemporality": 2,
                        "isMonotonic": true
                    }
                }),
                nametrics_core::MetricKind::Gauge => {
                    serde_json::json!({ "gauge": { "dataPoints": points } })
                }
                nametrics_core::MetricKind::Histogram => serde_json::json!({
                    "histogram": {
                        "dataPoints": points,
                        "aggregationTemporality": 2
                    }
                }),
            };
            let mut metric = serde_json::json!({
                "name": family.name,
                "description": family.description,
                "unit": family.unit,
            });
            if let Some(object) = data.as_object() {
                for (key, value) in object {
                    metric[key] = value.clone();
                }
            }
            metric
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": resource_attributes_json(service_name, service_instance_id)
            },
            "scopeMetrics": [{
                "scope": { "name": "nasa" },
                "metrics": metrics
            }]
        }]
    })
    .to_string()
}

/// 业务作用：将有限指标 label 转为 OTLP JSON `KeyValue` 属性。
///
/// 参数说明：
/// - `labels`: descriptor 顺序下的 label 名值对。
///
/// 返回：仅包含 string `AnyValue` 的有序属性列表。
fn metric_attributes_json(labels: &[(&'static str, String)]) -> Vec<serde_json::Value> {
    labels
        .iter()
        .map(|(name, value)| {
            serde_json::json!({
                "key": name,
                "value": { "stringValue": value }
            })
        })
        .collect()
}

/// 业务作用：生成 traces 与 metrics 共用的 OTLP JSON resource 身份属性。
///
/// 参数说明：
/// - `service_name`: 稳定服务名。
/// - `service_instance_id`: 本次进程实例身份。
///
/// 返回：按 `service.name`、`service.instance.id` 顺序排列的两个 `KeyValue`。
fn resource_attributes_json(
    service_name: &str,
    service_instance_id: &str,
) -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "key": "service.name",
            "value": { "stringValue": service_name }
        }),
        serde_json::json!({
            "key": "service.instance.id",
            "value": { "stringValue": service_instance_id }
        }),
    ]
}

/// 业务作用：按 proto3 JSON 规则表示有限值与特殊浮点值。
///
/// 参数说明：
/// - `value`: gauge 或 histogram sum 的当前值。
///
/// 返回：有限值为 JSON number，非有限值为 protobuf JSON 规定的字符串。
fn otlp_json_double(value: f64) -> serde_json::Value {
    if value.is_nan() {
        serde_json::Value::String("NaN".to_owned())
    } else if value == f64::INFINITY {
        serde_json::Value::String("Infinity".to_owned())
    } else if value == f64::NEG_INFINITY {
        serde_json::Value::String("-Infinity".to_owned())
    } else {
        serde_json::json!(value)
    }
}

/// 业务作用：把一批 span 编码成 OTLP/HTTP JSON `ExportTraceServiceRequest`(proto3 JSON 映射)。
///
/// trace/span id 用十六进制字符串(OTLP JSON 对 id 的约定)；时间戳使用生产者记录的真实开始/结束
/// Unix 纳秒。kind 取自记录(SERVER=2/INTERNAL=1/CLIENT=3)。只含低基数 name 与 id，不含
/// payload/属性正文。
///
/// 参数说明：
/// - `batch`: 待编码的一批 span。
/// - `service_name`: resource `service.name` 属性值。
/// - `service_instance_id`: resource `service.instance.id` 属性值。
///
/// 返回：包含服务与实例 resource 身份且不含业务正文的 proto3 JSON 字符串。
fn otlp_traces_json(batch: &[SpanRecord], service_name: &str, service_instance_id: &str) -> String {
    let spans: Vec<serde_json::Value> = batch
        .iter()
        .map(|span| {
            let mut encoded = serde_json::json!({
                "traceId": span.trace_id_hex,
                "spanId": span.span_id_hex,
                "name": span.name,
                "kind": span.kind.otlp_value(),
                "startTimeUnixNano": span.start_unix_nano.to_string(),
                "endTimeUnixNano": span.end_unix_nano.to_string(),
                "status": {
                    "code": if span.http_status_code.is_some_and(|status| status >= 500) { 2 } else { 1 }
                }
            });
            if let Some(parent) = &span.parent_span_id_hex {
                encoded["parentSpanId"] = serde_json::Value::String(parent.clone());
            }
            encoded
        })
        .collect();
    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": resource_attributes_json(service_name, service_instance_id)
            },
            "scopeSpans": [{
                "scope": { "name": "nasa" },
                "spans": spans
            }]
        }]
    })
    .to_string()
}

/// 业务作用：追加 protobuf varint 编码的 `u64`。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `value`: 待编码值。
///
/// 返回：无；将值的最短 varint 表示追加到缓冲区。
fn put_varint(buffer: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buffer.push(byte);
            return;
        }
        buffer.push(byte | 0x80);
    }
}

/// 业务作用：写 protobuf tag = `(field << 3) | wire_type`。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: 字段号。
/// - `wire`: wire type（0=varint、1=fixed64、2=长度分隔）。
///
/// 返回：无；把字段号与 wire type 合成的 tag 追加到缓冲区。
fn put_tag(buffer: &mut Vec<u8>, field: u32, wire: u32) {
    put_varint(buffer, u64::from((field << 3) | wire));
}

/// 业务作用：写一个 varint 字段(wire type 0),用于枚举/整数。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: 字段号。
/// - `value`: 字段值。
///
/// 返回：无；按 tag 后跟 varint 值的顺序追加字段。
fn put_varint_field(buffer: &mut Vec<u8>, field: u32, value: u64) {
    put_tag(buffer, field, 0);
    put_varint(buffer, value);
}

/// 业务作用：写一个 fixed64 字段(wire type 1,小端),用于 `*_time_unix_nano`。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: 字段号。
/// - `value`: 64 位值。
///
/// 返回：无；按 tag 后跟小端 fixed64 的顺序追加字段。
fn put_fixed64_field(buffer: &mut Vec<u8>, field: u32, value: u64) {
    put_tag(buffer, field, 1);
    buffer.extend_from_slice(&value.to_le_bytes());
}

/// 业务作用：写一个 protobuf `double` 字段（wire type 1，IEEE-754 小端）。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: 字段号。
/// - `value`: 待编码浮点值。
///
/// 返回：无；原始 bit 包含 NaN 与无穷值语义。
fn put_double_field(buffer: &mut Vec<u8>, field: u32, value: f64) {
    put_fixed64_field(buffer, field, value.to_bits());
}

/// 业务作用：写一个 protobuf `sfixed64` 字段，用于 OTLP `as_int`。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: 字段号。
/// - `value`: 有符号 64 位整数。
///
/// 返回：无；保留整数的二补数 bit。
fn put_sfixed64_field(buffer: &mut Vec<u8>, field: u32, value: i64) {
    put_fixed64_field(buffer, field, value as u64);
}

/// 业务作用：写 OTLP histogram 的 packed `fixed64` 桶计数。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: repeated 字段号。
/// - `values`: 含 `+Inf` 的非累积桶计数。
///
/// 返回：无；空数组不写字段。
fn put_packed_fixed64(buffer: &mut Vec<u8>, field: u32, values: &[u64]) {
    if values.is_empty() {
        return;
    }
    let mut packed = Vec::with_capacity(values.len() * 8);
    for value in values {
        packed.extend_from_slice(&value.to_le_bytes());
    }
    put_len_field(buffer, field, &packed);
}

/// 业务作用：写 OTLP histogram 的 packed `double` 显式边界。
///
/// 参数说明：
/// - `buffer`: 输出缓冲。
/// - `field`: repeated 字段号。
/// - `values`: 唯一 descriptor 的有序有限边界。
///
/// 返回：无；空数组不写字段。
fn put_packed_double(buffer: &mut Vec<u8>, field: u32, values: &[f64]) {
    if values.is_empty() {
        return;
    }
    let mut packed = Vec::with_capacity(values.len() * 8);
    for value in values {
        packed.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    put_len_field(buffer, field, &packed);
}

/// 业务作用：写一个长度分隔字段(wire type 2):`bytes` / `string` / 嵌套消息。
///
/// # 参数
///
/// - `buffer`:输出缓冲。
/// - `field`:字段号。
/// - `bytes`:字段内容(字符串 UTF-8 字节 / 原始字节 / 已编码的子消息)。
fn put_len_field(buffer: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    put_tag(buffer, field, 2);
    put_varint(buffer, bytes.len() as u64);
    buffer.extend_from_slice(bytes);
}

/// 业务作用：编码一个字符串 `KeyValue`，供 resource 与数据点 label 共用。
///
/// 参数说明：
/// - `key`: 属性名。
/// - `value`: 属性字符串值。
///
/// 返回：`KeyValue{key=1,value=AnyValue{string_value=1}}` 的 protobuf 字节。
fn string_key_value_protobuf(key: &str, value: &str) -> Vec<u8> {
    let mut any_value = Vec::new();
    put_len_field(&mut any_value, 1, value.as_bytes());
    let mut key_value = Vec::new();
    put_len_field(&mut key_value, 1, key.as_bytes());
    put_len_field(&mut key_value, 2, &any_value);
    key_value
}

/// 业务作用：编码 traces 与 metrics 共用的 OTLP resource 身份。
///
/// 参数说明：
/// - `service_name`: 稳定服务名。
/// - `service_instance_id`: 本次进程实例身份。
///
/// 返回：含 `service.name` 与 `service.instance.id` 两个属性的 `Resource` 字节。
fn resource_protobuf(service_name: &str, service_instance_id: &str) -> Vec<u8> {
    let mut resource = Vec::new();
    put_len_field(
        &mut resource,
        1,
        &string_key_value_protobuf("service.name", service_name),
    );
    put_len_field(
        &mut resource,
        1,
        &string_key_value_protobuf("service.instance.id", service_instance_id),
    );
    resource
}

/// 业务作用：将 descriptor 顺序下的 label 编码为数据点 repeated `KeyValue`。
///
/// 参数说明：
/// - `buffer`: 数据点消息缓冲。
/// - `field`: NumberDataPoint 或 HistogramDataPoint 的 attributes 字段号。
/// - `labels`: 已通过统一 catalog 校验的名值对。
///
/// 返回：无；每个 label 写一个字符串 `KeyValue`。
fn put_metric_attributes(buffer: &mut Vec<u8>, field: u32, labels: &[(&'static str, String)]) {
    for (name, value) in labels {
        put_len_field(buffer, field, &string_key_value_protobuf(name, value));
    }
}

/// 业务作用：把定长十六进制字符串(trace/span id)解成字节;非法字符处截断(id 由 `natelemetry` 生成,恒合法)。
///
/// 参数说明：
/// - `hex`: 偶数长度的小写十六进制串。
///
/// 返回：已成功解码的字节；遇到非法字符时只保留此前完整字节。
fn hex_to_bytes(hex: &str) -> Vec<u8> {
    let raw = hex.as_bytes();
    let mut out = Vec::with_capacity(raw.len() / 2);
    let mut index = 0;
    while index + 2 <= raw.len() {
        let hi = (raw[index] as char).to_digit(16);
        let lo = (raw[index + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => out.push((hi * 16 + lo) as u8),
            _ => break,
        }
        index += 2;
    }
    out
}

/// 业务作用：把一批 span 编码成 OTLP/HTTP protobuf `ExportTraceServiceRequest`(二进制 wire 格式)。
///
/// 手写最小编码器,只覆盖本管道实际发出的字段(字段号取自 opentelemetry-proto v1 trace.proto):
/// `Span{trace_id=1,span_id=2,name=5,kind=6,start_time_unix_nano=7,end_time_unix_nano=8}`、
/// `ScopeSpans{scope=1,spans=2}`、`ResourceSpans{resource=1,scope_spans=2}`、
/// `ExportTraceServiceRequest{resource_spans=1}`、`Resource{attributes=1}`、`KeyValue{key=1,value=2}`、
/// `AnyValue{string_value=1}`。id 由十六进制解成 `bytes`;时间戳同 JSON 使用生产者记录值;kind 取自记录。
/// 不含 payload/属性正文，只携带 `service.name` 与 `service.instance.id` resource 属性。
///
/// 参数说明：
/// - `batch`: 待编码的一批 span。
/// - `service_name`: resource `service.name` 属性值。
/// - `service_instance_id`: resource `service.instance.id` 属性值。
///
/// 返回：包含服务与实例 resource 身份且不含业务正文的 protobuf wire 字节。
fn otlp_traces_protobuf(
    batch: &[SpanRecord],
    service_name: &str,
    service_instance_id: &str,
) -> Vec<u8> {
    let resource = resource_protobuf(service_name, service_instance_id);

    // ScopeSpans{ scope=1: InstrumentationScope{ name="nasa" }, spans=2: repeated Span }
    let mut scope = Vec::new();
    put_len_field(&mut scope, 1, b"nasa");
    let mut scope_spans = Vec::new();
    put_len_field(&mut scope_spans, 1, &scope);
    for span in batch {
        let mut span_msg = Vec::new();
        put_len_field(&mut span_msg, 1, &hex_to_bytes(&span.trace_id_hex));
        put_len_field(&mut span_msg, 2, &hex_to_bytes(&span.span_id_hex));
        if let Some(parent) = &span.parent_span_id_hex {
            put_len_field(&mut span_msg, 4, &hex_to_bytes(parent));
        }
        put_len_field(&mut span_msg, 5, span.name.as_bytes());
        put_varint_field(&mut span_msg, 6, u64::from(span.kind.otlp_value()));
        put_fixed64_field(&mut span_msg, 7, span.start_unix_nano);
        put_fixed64_field(&mut span_msg, 8, span.end_unix_nano);
        let mut status = Vec::new();
        put_varint_field(
            &mut status,
            3,
            if span.http_status_code.is_some_and(|value| value >= 500) {
                2
            } else {
                1
            },
        );
        put_len_field(&mut span_msg, 15, &status);
        put_len_field(&mut scope_spans, 2, &span_msg);
    }

    // ResourceSpans{ resource=1, scope_spans=2 } → ExportTraceServiceRequest{ resource_spans=1 }
    let mut resource_spans = Vec::new();
    put_len_field(&mut resource_spans, 1, &resource);
    put_len_field(&mut resource_spans, 2, &scope_spans);
    let mut request = Vec::new();
    put_len_field(&mut request, 1, &resource_spans);
    request
}

/// 业务作用：将统一 family 快照编码为 OTLP/HTTP protobuf 指标请求。
///
/// 字段号对齐 OpenTelemetry metrics.proto：`Metric{gauge=5,sum=7,histogram=9}`、
/// `NumberDataPoint{start=2,time=3,as_double=4,as_int=6,attributes=7}`、
/// `HistogramDataPoint{start=2,time=3,count=4,sum=5,bucket_counts=6,explicit_bounds=7,attributes=9}`。
/// Counter 超过 `i64::MAX` 时使用 `as_double`，避免有符号 OTLP `as_int` 变成负值。
///
/// 参数说明：
/// - `families`: 含 descriptor 语义与当前数据点的快照。
/// - `service_name`: resource 服务名。
/// - `service_instance_id`: resource 进程实例身份。
/// - `start_time_unix_nano`: 累计起点。
/// - `time_unix_nano`: 快照时刻。
///
/// 返回：`ExportMetricsServiceRequest` 的 protobuf wire 字节。
fn otlp_metrics_protobuf(
    families: &[nametrics_core::MetricFamilySnapshot],
    service_name: &str,
    service_instance_id: &str,
    start_time_unix_nano: u64,
    time_unix_nano: u64,
) -> Vec<u8> {
    let resource = resource_protobuf(service_name, service_instance_id);
    let mut scope = Vec::new();
    put_len_field(&mut scope, 1, b"nasa");
    let mut scope_metrics = Vec::new();
    put_len_field(&mut scope_metrics, 1, &scope);

    for family in families {
        let mut metric = Vec::new();
        put_len_field(&mut metric, 1, family.name.as_bytes());
        put_len_field(&mut metric, 2, family.description.as_bytes());
        if !family.unit.is_empty() {
            put_len_field(&mut metric, 3, family.unit.as_bytes());
        }
        match family.kind {
            nametrics_core::MetricKind::Counter => {
                let mut sum = Vec::new();
                for sample in &family.samples {
                    let nametrics_core::MetricValue::Counter(value) = sample.value else {
                        continue;
                    };
                    let mut point = Vec::new();
                    put_fixed64_field(&mut point, 2, start_time_unix_nano);
                    put_fixed64_field(&mut point, 3, time_unix_nano);
                    if value <= i64::MAX as u64 {
                        put_sfixed64_field(&mut point, 6, value as i64);
                    } else {
                        put_double_field(&mut point, 4, value as f64);
                    }
                    put_metric_attributes(&mut point, 7, &sample.labels);
                    put_len_field(&mut sum, 1, &point);
                }
                put_varint_field(&mut sum, 2, 2);
                put_varint_field(&mut sum, 3, 1);
                put_len_field(&mut metric, 7, &sum);
            }
            nametrics_core::MetricKind::Gauge => {
                let mut gauge = Vec::new();
                for sample in &family.samples {
                    let nametrics_core::MetricValue::Gauge(value) = sample.value else {
                        continue;
                    };
                    let mut point = Vec::new();
                    put_fixed64_field(&mut point, 3, time_unix_nano);
                    put_double_field(&mut point, 4, value);
                    put_metric_attributes(&mut point, 7, &sample.labels);
                    put_len_field(&mut gauge, 1, &point);
                }
                put_len_field(&mut metric, 5, &gauge);
            }
            nametrics_core::MetricKind::Histogram => {
                let mut histogram = Vec::new();
                for sample in &family.samples {
                    let nametrics_core::MetricValue::Histogram {
                        ref buckets,
                        sum,
                        count,
                        ..
                    } = sample.value
                    else {
                        continue;
                    };
                    let mut point = Vec::new();
                    put_fixed64_field(&mut point, 2, start_time_unix_nano);
                    put_fixed64_field(&mut point, 3, time_unix_nano);
                    put_fixed64_field(&mut point, 4, count);
                    put_double_field(&mut point, 5, sum);
                    put_packed_fixed64(&mut point, 6, buckets);
                    put_packed_double(&mut point, 7, family.histogram_bounds);
                    put_metric_attributes(&mut point, 9, &sample.labels);
                    put_len_field(&mut histogram, 1, &point);
                }
                put_varint_field(&mut histogram, 2, 2);
                put_len_field(&mut metric, 9, &histogram);
            }
        }
        put_len_field(&mut scope_metrics, 2, &metric);
    }

    let mut resource_metrics = Vec::new();
    put_len_field(&mut resource_metrics, 1, &resource);
    put_len_field(&mut resource_metrics, 2, &scope_metrics);
    let mut request = Vec::new();
    put_len_field(&mut request, 1, &resource_metrics);
    request
}

/// 业务作用：受管 drainer:批量把 span 送到 sink,直到被取消或所有生产者释放。
///
/// 批策略:攒满 `BATCH_SIZE` 或距上条 span `FLUSH_INTERVAL` 未再入队即 flush;取消后排空缓冲的
/// 最后一批(超出全局停机预算由上层 action 的 timeout 兜底)。
///
/// # 参数
///
/// - `receiver`:span 接收端(drainer 独占)。
/// - `cancel`:停机取消令牌;触发后排空缓冲并退出。
/// - `sink`:span 导出目的地(日志或 OTLP)。
async fn drain_spans(
    mut receiver: tokio::sync::mpsc::Receiver<SpanRecord>,
    cancel: CancellationToken,
    sink: SpanSink,
    exporter: Arc<BoundedSpanExporter>,
    contributor: ReadinessContributor,
) {
    /// 单批最大 span 数(达到即 flush)。
    const BATCH_SIZE: usize = 128;
    /// 批 flush 周期:定时 tick 独立于 span 到达(不被新 span 重置),故稳定 span 流也能按期 flush。
    const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);
    let mut batch: Vec<SpanRecord> = Vec::new();
    let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
    // 首个 tick 立即返回(batch 空时是 no-op),之后每 FLUSH_INTERVAL 一次。
    loop {
        tokio::select! {
            maybe = receiver.recv() => match maybe {
                Some(span) => {
                    batch.push(span);
                    if batch.len() >= BATCH_SIZE {
                        if sink.ship(&batch).await {
                            exporter.record_exported(batch.len() as u64);
                            contributor.observe(
                                DependencyState::Ready,
                                reason::HEALTHY,
                                std::time::Instant::now(),
                            );
                        } else {
                            exporter.record_dropped(batch.len() as u64);
                            contributor.observe(
                                DependencyState::Degraded,
                                reason::DEGRADED,
                                std::time::Instant::now(),
                            );
                        }
                        batch.clear();
                    }
                }
                None => break, // 所有 exporter 已释放
            },
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    if sink.ship(&batch).await {
                        exporter.record_exported(batch.len() as u64);
                        contributor.observe(
                            DependencyState::Ready,
                            reason::HEALTHY,
                            std::time::Instant::now(),
                        );
                    } else {
                        exporter.record_dropped(batch.len() as u64);
                        contributor.observe(
                            DependencyState::Degraded,
                            reason::DEGRADED,
                            std::time::Instant::now(),
                        );
                    }
                    batch.clear();
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
    // 取消/关闭后排空剩余(不等待新 span);真正的时限由停机 action 的 timeout 约束。
    while let Ok(span) = receiver.try_recv() {
        batch.push(span);
    }
    if !batch.is_empty() {
        if sink.ship(&batch).await {
            exporter.record_exported(batch.len() as u64);
            contributor.observe(
                DependencyState::Ready,
                reason::HEALTHY,
                std::time::Instant::now(),
            );
        } else {
            exporter.record_dropped(batch.len() as u64);
            contributor.observe(
                DependencyState::Degraded,
                reason::DEGRADED,
                std::time::Instant::now(),
            );
        }
    }
}

/// 停机 flush action:取消 drainer 并在全局剩余预算内 join;超时如实报告未导出数。
struct TelemetryFlush {
    cancel: CancellationToken,
    drainers: Vec<JoinHandle<()>>,
    exporter: Arc<BoundedSpanExporter>,
}

impl ShutdownAction for TelemetryFlush {
    /// 业务作用：返回清理报告使用的稳定动作名称。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回不含配置值或 span 内容的稳定名称。
    fn label(&self) -> &'static str {
        "telemetry-flush"
    }

    /// 业务作用：取消 drainer 并在全局剩余停机预算内 join;超时把未导出计为丢弃后如实报告。
    ///
    /// 参数说明：
    /// - `context`: 提供全局剩余停机预算的清理上下文。
    ///
    /// 返回：全部 drainer 在预算内结束时成功；任务异常或预算耗尽时返回 Telemetry 错误。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.cancel.cancel();
            while !self.drainers.is_empty() {
                let mut drainer = self.drainers.swap_remove(0);
                match tokio::time::timeout(context.remaining(), &mut drainer).await {
                    Ok(Ok(())) => {}
                    // 任务异常结束时不再宣称 flush 完成，Drop 会停止仍在运行的出口。
                    Ok(Err(_join_error)) => {
                        return Err(telemetry_error(
                            ApplicationPhase::Stopping,
                            "telemetry drainer task terminated abnormally during shutdown",
                        ));
                    }
                    Err(_) => {
                        // 超时后立即中止当前与其余出口，不让 HTTP I/O 越过全局停机边界。
                        drainer.abort();
                        let _ = drainer.await;
                        for remaining in self.drainers.drain(..) {
                            remaining.abort();
                        }
                        let dropped_now = self.exporter.drop_all_pending();
                        let dropped_total = self.exporter.dropped();
                        return Err(telemetry_error(
                            ApplicationPhase::Stopping,
                            format!(
                                "telemetry flush did not finish within the global shutdown deadline \
                                 (dropped_now={dropped_now}, dropped_total={dropped_total})"
                            ),
                        ));
                    }
                }
            }
            Ok(())
        })
    }
}

impl Drop for TelemetryFlush {
    /// 业务作用：flush 被外层取消时停止 drainer，防止导出任务越过应用停机边界。
    ///
    /// 参数说明：无。
    ///
    /// 返回：无；同步取消并终止仍存活的导出任务。
    fn drop(&mut self) {
        self.cancel.cancel();
        for drainer in self.drainers.drain(..) {
            drainer.abort();
        }
    }
}

/// 业务作用：从最终配置读取 `telemetry` 段;缺失该段时使用安全缺省(enabled=true、空 service、2048 队列)。
///
/// 参数说明：
/// - `application`: 提供当前不可变配置快照与实例身份的共享上下文。
///
/// 返回：返回补齐 service 身份的配置；配置树无法反序列化时返回 Telemetry 错误。
fn read_telemetry_config(application: &Application) -> ApplicationResult<TelemetryConfig> {
    let snapshot = application.config();
    let root: TelemetryConfigRoot =
        serde_json::from_value((*snapshot.value()).clone()).map_err(|error| {
            telemetry_error_src(
                ApplicationPhase::Start,
                "invalid `telemetry` configuration section",
                error,
            )
        })?;
    let mut config = root.telemetry.unwrap_or_default();
    if config.service_name.trim().is_empty() {
        config.service_name = application.info().name().to_owned();
    }
    if config.service_instance_id.trim().is_empty() {
        config.service_instance_id = format!(
            "{}-{}",
            std::process::id(),
            system_time_nanos(application.info().started_at())
        );
    }
    Ok(config)
}

/// 业务作用：将墙上时钟安全投影为 OTLP 使用的 Unix 纳秒。
///
/// 参数说明：
/// - `time`: Application 启动或快照时刻。
///
/// 返回：Unix epoch 之后的纳秒，超出 `u64` 时饱和，epoch 之前返回零。
fn system_time_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// 业务作用：在不创建任何管道的前提下校验候选配置树中的 `telemetry` 段。
///
/// 参数说明：
/// - `tree`: 合并、插值完成但尚未发布的候选配置树。
/// - `phase`: 本次无副作用校验所属的生命周期阶段。
///
/// 返回：缺少该段或配置合法时成功；字段、类型或边界不合法时返回 Telemetry 错误。
pub(crate) fn validate_telemetry_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("telemetry") else {
        return Ok(());
    };
    let config: TelemetryConfig = serde_json::from_value(section.clone()).map_err(|error| {
        telemetry_error_src(phase, "invalid `telemetry` configuration section", error)
    })?;
    config.validate(phase)
}

/// 业务作用：创建遥测组件的稳定生命周期错误。
///
/// 参数说明：
/// - `phase`: 故障被观察到的生命周期阶段。
/// - `message`: 不含 span 内容、属性正文或配置值的稳定摘要。
///
/// 返回：返回归属 Telemetry 组件且不携带底层错误链的生命周期错误。
fn telemetry_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Telemetry, phase, message)
}

/// 业务作用：创建带底层错误链的遥测组件错误(输出前统一脱敏)。
///
/// 参数说明：
/// - `phase`: 故障被观察到的生命周期阶段。
/// - `message`: 不含敏感内容的稳定摘要。
/// - `source`: 只供诊断的底层错误。
///
/// 返回：返回归属 Telemetry 组件并保留内部错误链的生命周期错误。
fn telemetry_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Telemetry, phase, message, source)
}
