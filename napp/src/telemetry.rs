//! OpenTelemetry traces 组件。
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
//! collector)。两种 sink 共享同一有界队列/受管 drainer/停机 flush 语义,不含 payload/属性正文。

use std::sync::Arc;

use natelemetry::{BoundedSpanExporter, SpanRecord};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ComponentId, ShutdownAction, ShutdownContext, StartContext,
};

/// 有界队列容量缺省值:2048 条 span(满则丢弃并计数,绝不背压业务)。
const DEFAULT_QUEUE_CAPACITY: usize = 2048;
/// 配置可声明的队列硬上限，避免错误配置在启动时申请不合理资源。
const MAX_QUEUE_CAPACITY: usize = 1_000_000;

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
    /// 有界导出队列容量;缺省 2048,必须 ≥ 1。
    queue_capacity: usize,
    /// OTLP/HTTP traces 端点(如 `http://collector:4318/v1/traces`);设置即 sink 换成 OTLP 导出器,
    /// 否则保持日志 sink。必须是合法 http(s) URL。
    #[serde(default)]
    otlp_endpoint: Option<String>,
    /// OTLP 载荷编码;缺省 `json`。仅在配了 `otlp_endpoint` 时生效。
    #[serde(default)]
    otlp_encoding: OtlpEncoding,
    /// 没有上游 traceparent 时的新根链路采样率；0.0=全关，1.0=全开。
    root_sample_ratio: f64,
}

impl Default for TelemetryConfig {
    /// 使用启用状态、空服务名、2048 队列和 JSON OTLP 的保守缺省。
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: String::new(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            otlp_endpoint: None,
            otlp_encoding: OtlpEncoding::Json,
            root_sample_ratio: 1.0,
        }
    }
}

impl TelemetryConfig {
    /// 无副作用校验:队列容量必须 ≥ 1(0 容量的 mpsc 会阻塞每次入队);`otlp_endpoint` 必须是合法 http(s) URL。
    ///
    /// # 参数
    ///
    /// - `phase`:本次校验所属生命周期阶段,用于错误归因。
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
        if self.enabled
            && (!self.root_sample_ratio.is_finite()
                || !(0.0..=1.0).contains(&self.root_sample_ratio))
        {
            return Err(telemetry_error(
                phase,
                "telemetry.root_sample_ratio must be between 0.0 and 1.0",
            ));
        }
        if let Some(endpoint) = self
            .otlp_endpoint
            .as_deref()
            .filter(|e| !e.trim().is_empty())
        {
            let parsed = reqwest::Url::parse(endpoint).map_err(|error| {
                telemetry_error_src(phase, "telemetry.otlp_endpoint is not a valid URL", error)
            })?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(telemetry_error(
                    phase,
                    "telemetry.otlp_endpoint must use http or https",
                ));
            }
        }
        Ok(())
    }
}

/// 遥测组件:Start 建管道并发布 exporter,停机 flush。
pub(crate) struct TelemetryComponent {
    config: Option<TelemetryConfig>,
}

impl TelemetryComponent {
    /// 创建尚未读取配置的遥测组件。
    ///
    /// # 参数
    ///
    /// 本方法无参数;导出管道在 Start 阶段按最终配置创建。
    pub(crate) fn new() -> Self {
        Self { config: None }
    }
}

impl ApplicationComponent for TelemetryComponent {
    /// 返回遥测组件稳定身份。
    ///
    /// # 参数
    ///
    /// 本方法无参数;Runner 用它归类遥测相关错误。
    fn id(&self) -> ComponentId {
        ComponentId::Telemetry
    }

    /// 读取并冻结配置,创建有界导出管道、发布 exporter、spawn drainer 并压入停机 flush action。
    ///
    /// 管道在 Start(而非 Ready)建立:Batch 模式不执行 Ready,但 Start 与停机都会执行,因此两种模式
    /// 都能建立管道并在退出前 flush。exporter 在流量入口 Ready 之前发布,生产者一旦运行即可入队。
    ///
    /// # 参数
    ///
    /// - `context`:提供最终配置、Application(发布 exporter)与 active stack 的 Start 上下文。
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

            // sink:配了 otlp_endpoint 则批量 POST OTLP/HTTP JSON,否则保持日志 sink。
            let sink = build_span_sink(&config)?;
            let cancel = CancellationToken::new();
            let drainer = tokio::spawn(drain_spans(
                receiver,
                cancel.clone(),
                sink,
                Arc::clone(&exporter),
                contributor,
            ));
            // Start action:telemetry 声明早,其停机 action 在逆序栈中靠后弹出 → 在流量入口停止后 flush。
            context.activate(Box::new(TelemetryFlush {
                cancel,
                drainer: Some(drainer),
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
        /// 载荷编码(JSON / protobuf)。
        encoding: OtlpEncoding,
    },
}

impl SpanSink {
    /// 导出一批 span。日志 sink 逐条打日志;OTLP sink 批量 POST,失败只 warn 降级(不背压、不 panic)。
    ///
    /// # 参数
    ///
    /// - `batch`:待导出的一批 span(非空)。
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
                encoding,
            } => {
                let (content_type, body): (&str, Vec<u8>) = match encoding {
                    OtlpEncoding::Json => (
                        "application/json",
                        otlp_traces_json(batch, service_name).into_bytes(),
                    ),
                    OtlpEncoding::Protobuf => (
                        "application/x-protobuf",
                        otlp_traces_protobuf(batch, service_name),
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

/// 按配置构造 span sink:配了 `otlp_endpoint` 则 OTLP JSON 导出,否则日志 sink。
///
/// # 参数
///
/// - `config`:已校验的遥测配置。
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
                encoding: config.otlp_encoding,
            })
        }
        None => Ok(SpanSink::Log),
    }
}

/// 把一批 span 编码成 OTLP/HTTP JSON `ExportTraceServiceRequest`(proto3 JSON 映射)。
///
/// trace/span id 用十六进制字符串(OTLP JSON 对 id 的约定)；时间戳使用生产者记录的真实开始/结束
/// Unix 纳秒。kind 取自记录(SERVER=2/INTERNAL=1/CLIENT=3)。只含低基数 name 与 id，不含
/// payload/属性正文。
///
/// # 参数
///
/// - `batch`:待编码的一批 span。
/// - `service_name`:resource `service.name` 属性值。
fn otlp_traces_json(batch: &[SpanRecord], service_name: &str) -> String {
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
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": service_name }
                }]
            },
            "scopeSpans": [{
                "scope": { "name": "nasa" },
                "spans": spans
            }]
        }]
    })
    .to_string()
}

/// 追加 protobuf varint 编码的 `u64`。
///
/// # 参数
///
/// - `buffer`:输出缓冲。
/// - `value`:待编码值。
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

/// 写 protobuf tag = `(field << 3) | wire_type`。
///
/// # 参数
///
/// - `buffer`:输出缓冲。
/// - `field`:字段号。
/// - `wire`:wire type(0=varint,1=fixed64,2=长度分隔)。
fn put_tag(buffer: &mut Vec<u8>, field: u32, wire: u32) {
    put_varint(buffer, u64::from((field << 3) | wire));
}

/// 写一个 varint 字段(wire type 0),用于枚举/整数。
///
/// # 参数
///
/// - `buffer`:输出缓冲。
/// - `field`:字段号。
/// - `value`:字段值。
fn put_varint_field(buffer: &mut Vec<u8>, field: u32, value: u64) {
    put_tag(buffer, field, 0);
    put_varint(buffer, value);
}

/// 写一个 fixed64 字段(wire type 1,小端),用于 `*_time_unix_nano`。
///
/// # 参数
///
/// - `buffer`:输出缓冲。
/// - `field`:字段号。
/// - `value`:64 位值。
fn put_fixed64_field(buffer: &mut Vec<u8>, field: u32, value: u64) {
    put_tag(buffer, field, 1);
    buffer.extend_from_slice(&value.to_le_bytes());
}

/// 写一个长度分隔字段(wire type 2):`bytes` / `string` / 嵌套消息。
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

/// 把定长十六进制字符串(trace/span id)解成字节;非法字符处截断(id 由 `natelemetry` 生成,恒合法)。
///
/// # 参数
///
/// - `hex`:偶数长度的小写十六进制串。
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

/// 把一批 span 编码成 OTLP/HTTP protobuf `ExportTraceServiceRequest`(二进制 wire 格式)。
///
/// 手写最小编码器,只覆盖本管道实际发出的字段(字段号取自 opentelemetry-proto v1 trace.proto):
/// `Span{trace_id=1,span_id=2,name=5,kind=6,start_time_unix_nano=7,end_time_unix_nano=8}`、
/// `ScopeSpans{scope=1,spans=2}`、`ResourceSpans{resource=1,scope_spans=2}`、
/// `ExportTraceServiceRequest{resource_spans=1}`、`Resource{attributes=1}`、`KeyValue{key=1,value=2}`、
/// `AnyValue{string_value=1}`。id 由十六进制解成 `bytes`;时间戳同 JSON 使用生产者记录值;kind 取自记录。
/// 不含 payload/属性正文,只 `service.name` 一条 resource 属性。
///
/// # 参数
///
/// - `batch`:待编码的一批 span。
/// - `service_name`:resource `service.name` 属性值。
fn otlp_traces_protobuf(batch: &[SpanRecord], service_name: &str) -> Vec<u8> {
    // Resource{ attributes = 1: KeyValue{ key="service.name", value=AnyValue{ string_value } } }
    let mut any_value = Vec::new();
    put_len_field(&mut any_value, 1, service_name.as_bytes());
    let mut key_value = Vec::new();
    put_len_field(&mut key_value, 1, b"service.name");
    put_len_field(&mut key_value, 2, &any_value);
    let mut resource = Vec::new();
    put_len_field(&mut resource, 1, &key_value);

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

/// 受管 drainer:批量把 span 送到 sink,直到被取消或所有生产者释放。
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
    drainer: Option<JoinHandle<()>>,
    exporter: Arc<BoundedSpanExporter>,
}

impl ShutdownAction for TelemetryFlush {
    /// 返回清理报告使用的稳定动作名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数;名称不含配置值或 span 内容。
    fn label(&self) -> &'static str {
        "telemetry-flush"
    }

    /// 取消 drainer 并在全局剩余停机预算内 join;超时把未导出计为丢弃后如实报告。
    ///
    /// # 参数
    ///
    /// - `context`:提供全局剩余停机预算的清理上下文。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.cancel.cancel();
            let Some(drainer) = self.drainer.as_mut() else {
                return Ok(());
            };
            match tokio::time::timeout(context.remaining(), drainer).await {
                Ok(Ok(())) => {
                    self.drainer.take();
                    Ok(())
                }
                // drainer 任务 panic:不阻断其余清理,但如实报告。
                Ok(Err(_join_error)) => {
                    self.drainer.take();
                    Err(telemetry_error(
                        ApplicationPhase::Stopping,
                        "telemetry drainer task terminated abnormally during shutdown",
                    ))
                }
                Err(_) => {
                    // dropping JoinHandle 会 detach，不能让 exporter 越过全局停机 deadline 继续 I/O。
                    let drainer = self
                        .drainer
                        .take()
                        .expect("telemetry drainer remains installed while shutdown awaits it");
                    drainer.abort();
                    let _ = drainer.await;
                    let dropped_now = self.exporter.drop_all_pending();
                    let dropped_total = self.exporter.dropped();
                    Err(telemetry_error(
                        ApplicationPhase::Stopping,
                        format!(
                            "telemetry flush did not finish within the global shutdown deadline \
                             (dropped_now={dropped_now}, dropped_total={dropped_total})"
                        ),
                    ))
                }
            }
        })
    }
}

impl Drop for TelemetryFlush {
    /// flush 被外层取消时停止 drainer，防止导出任务越过应用停机边界。
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(drainer) = self.drainer.take() {
            drainer.abort();
        }
    }
}

/// 从最终配置读取 `telemetry` 段;缺失该段时使用安全缺省(enabled=true、空 service、2048 队列)。
///
/// # 参数
///
/// - `application`:提供当前不可变配置快照的共享上下文。
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
    Ok(config)
}

/// 在不创建任何管道的前提下校验候选配置树中的 `telemetry` 段。
///
/// # 参数
///
/// - `tree`:合并、插值完成但尚未发布的候选配置树。
/// - `phase`:本次无副作用校验所属的生命周期阶段。
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

/// 创建遥测组件的稳定生命周期错误。
///
/// # 参数
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含 span 内容、属性正文或配置值的稳定摘要。
fn telemetry_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Telemetry, phase, message)
}

/// 创建带底层错误链的遥测组件错误(输出前统一脱敏)。
///
/// # 参数
///
/// - `phase`:故障被观察到的生命周期阶段。
/// - `message`:不含敏感内容的稳定摘要。
/// - `source`:只供诊断的底层错误。
fn telemetry_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Telemetry, phase, message, source)
}
