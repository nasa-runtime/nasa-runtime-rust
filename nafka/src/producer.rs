//! 生产者公共骨架：物理 lane、类型化 builder、raw bytes 与批量结果边界。

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use natelemetry::{random_span_id, SpanGuard, SpanKind, SpanRecorder, TraceContext};
use serde::Serialize;
use tracing::Instrument;

use crate::consumer::ConsumeCtx;
use crate::error::{NafkaError, Result};
use crate::rd::producer::{OutboundRecord, ProducerClient};
use crate::types::{Delivery, KafkaHeader, KafkaHeaders};
use crate::wire::{
    is_reserved_header, mode_wire_name, EncodePayload, PayloadCodec, DEFAULT_EVENT, HEADER_EVENT,
    HEADER_PASSTHROUGH, HEADER_PAYLOAD_CODEC, HEADER_PAYLOAD_MODE, HEADER_TRACEPARENT,
    PAYLOAD_CODEC_PROTOCOL_BYTES,
};
use crate::{KafkaProxy, KafkaProxyInner};

/// 单个物理 producer lane 的共享句柄。
///
/// lane 在 `connect` 后按配置名冻结；不同 lane 的底层队列、准入和观察器在 P1 中独立创建。
#[derive(Clone)]
pub struct ProducerLane {
    /// 每个配置 lane 唯一的共享内部资源域。
    inner: Arc<ProducerLaneInner>,
}

/// 单个物理 producer lane 的只读累计与瞬时容量快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerLaneStats {
    /// lane 名称。
    pub lane: String,
    /// 已取得准入、尚未完成 delivery 的请求数。
    pub active_requests: usize,
    /// fire delivery observer 的固定容量。
    pub observer_capacity: usize,
    /// 当前正在等待 delivery 的 fire observer 数量。
    pub observer_in_use: usize,
    /// 已成功同步入 native queue 并登记 observer 的 fire 次数。
    pub fire_admitted: u64,
    /// 因 observer permit 已满而在写入 native queue 前拒绝的次数。
    pub observer_full: u64,
    /// 底层 producer 同步返回本地 native queue 满的次数。
    pub native_queue_full: u64,
    /// 除 native queue 满以外的同步入队失败次数。
    pub enqueue_failed: u64,
    /// 当前底层 producer generation；首次构造为一，fatal 重建后单调递增。
    pub native_generation: u64,
    /// fatal 或持续无成功 delivery 后切换到新 generation 的累计次数。
    pub native_restarted: u64,
    /// lane 是否已进入永久错误(认证/授权/fencing)：为 true 时后续发布一律被拒，
    /// 且不会再自动恢复；健康面据此区分"lane 已死"与"投递在失败"。
    pub terminal_fatal: bool,
    /// 非 fatal 但连续无成功 delivery 时切换 generation 的累计次数。
    pub native_stalled_restarted: u64,
    /// send 与 fire 合计收到 broker delivery success 的次数。
    pub delivery_succeeded: u64,
    /// send 与 fire 合计收到失败、超时或观察通道中断的次数。
    pub delivery_failed: u64,
}

/// 单个物理 lane 的共享内部资源域。
struct ProducerLaneInner {
    /// lane 名称。
    name: Arc<str>,
    /// 所属运行时弱引用，避免运行时 lane 表形成强引用环。
    runtime: Weak<KafkaProxyInner>,
    /// 本 lane 独占的底层 producer 与观察资源。
    client: Arc<ProducerClient>,
}

impl ProducerLane {
    /// 构造一个已配置 lane 的轻量句柄。
    ///
    /// # 参数
    ///
    /// - `name`: 已通过配置校验的 lane 名。
    /// - `runtime`: 所属运行时共享状态。
    pub(crate) fn new(name: impl Into<Arc<str>>, runtime: &Arc<KafkaProxyInner>) -> Result<Self> {
        let name = name.into();
        let client = ProducerClient::create(&runtime.config, &name, runtime.runtime.clone())?;
        Ok(Self {
            inner: Arc::new(ProducerLaneInner {
                name,
                runtime: Arc::downgrade(runtime),
                client,
            }),
        })
    }

    /// 返回固定 lane 名。
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// 返回当前物理 lane 的容量与累计结果快照。
    pub fn stats(&self) -> ProducerLaneStats {
        self.inner.client.stats()
    }

    /// 创建类型化发布 builder。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `payload`: 借用的业务载荷。
    pub fn publish<'a, T: EncodePayload>(
        &'a self,
        topic: &'a str,
        payload: &'a T,
    ) -> PublishBuilder<'a, T> {
        PublishBuilder::new(self.clone(), topic, payload)
    }

    /// 创建显式 tombstone builder。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `key`: tombstone 必需的字符串 key。
    pub fn tombstone<'a>(&'a self, topic: &'a str, key: &'a str) -> TombstoneBuilder<'a> {
        TombstoneBuilder::new(self.clone(), topic, key)
    }

    /// 创建不经过 codec 的 raw bytes 发布 builder。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `payload`: 借用到同步入底层队列为止的原始字节。
    pub fn publish_raw<'a>(&'a self, topic: &'a str, payload: &'a [u8]) -> RawPublishBuilder<'a> {
        RawPublishBuilder::new(self.clone(), topic, payload)
    }

    /// 在统一 deadline 内排空本 lane 的投递观察与底层队列。
    ///
    /// # 错误
    ///
    /// 排空超时或底层客户端失败时返回带 lane 归因的错误。
    pub async fn flush(&self) -> Result<()> {
        let runtime = self
            .inner
            .runtime
            .upgrade()
            .ok_or_else(|| NafkaError::Lifecycle("所属 Kafka 运行时已释放".into()))?;
        let deadline =
            Instant::now() + Duration::from_millis(runtime.config.behavior.shutdown_timeout_ms);
        self.inner.client.flush_until(deadline).await
    }

    /// 检查所属运行时是否仍允许取得外部发布资格。
    ///
    /// # 错误
    ///
    /// 运行时已释放或进入关闭阶段时返回生命周期错误。
    fn ensure_publish_open(&self) -> Result<()> {
        let runtime = self
            .inner
            .runtime
            .upgrade()
            .ok_or_else(|| NafkaError::Lifecycle("所属 Kafka 运行时已释放".into()))?;
        runtime.ensure_publish_open()
    }

    /// 克隆 Application-owned 只写记录器；运行时释放或未启用 telemetry 时返回 `None`。
    fn span_recorder(&self) -> Option<SpanRecorder> {
        self.inner
            .runtime
            .upgrade()
            .and_then(|runtime| runtime.span_recorder.clone())
    }

    /// 把已经完成编码和校验的记录交给本 lane 并等待 delivery。
    ///
    /// # 参数
    ///
    /// - `record`: 借用到同步入队完成的底层隔离记录。
    ///
    /// # 错误
    ///
    /// 准入、同步入队、投递或超时失败时返回错误。
    async fn send_record(&self, record: OutboundRecord<'_>) -> Result<Delivery> {
        let runtime = self
            .inner
            .runtime
            .upgrade()
            .ok_or_else(|| NafkaError::Lifecycle("所属 Kafka 运行时已释放".into()))?;
        let topic = record.topic.to_owned();
        let event = outbound_event(record.headers).to_owned();
        let key_present = record.key.is_some();
        let span = tracing::info_span!(
            "kafka.publish",
            lane = %self.inner.name,
            topic = %topic,
            event = %event,
            key_present
        );
        let result = self.inner.client.send(record, false).instrument(span).await;
        let labels = [
            ("lane", self.inner.name.as_ref()),
            ("topic", topic.as_str()),
        ];
        match &result {
            Ok(_) => runtime.counter("published_total", 1, &labels),
            Err(_) => runtime.counter("publish_failed_total", 1, &labels),
        }
        self.report_queue_pressure(runtime.as_ref(), &[("lane", self.inner.name.as_ref())]);
        result
    }

    /// 把已经完成编码和校验的记录同步入队并登记异步观察。
    ///
    /// # 参数
    ///
    /// - `record`: 借用到同步入队完成的底层隔离记录。
    ///
    /// # 错误
    ///
    /// 观察槽、准入或同步入队失败时返回错误。
    fn fire_record(&self, record: OutboundRecord<'_>) -> Result<()> {
        let runtime = self
            .inner
            .runtime
            .upgrade()
            .ok_or_else(|| NafkaError::Lifecycle("所属 Kafka 运行时已释放".into()))?;
        let topic = record.topic.to_owned();
        let event = outbound_event(record.headers).to_owned();
        let key_present = record.key.is_some();
        let span = tracing::info_span!(
            "kafka.publish",
            lane = %self.inner.name,
            topic = %topic,
            event = %event,
            key_present
        );
        let result = span.in_scope(|| self.inner.client.fire(record, false));
        let labels = [
            ("lane", self.inner.name.as_ref()),
            ("topic", topic.as_str()),
        ];
        match &result {
            Ok(()) => runtime.counter("published_total", 1, &labels),
            Err(_) => runtime.counter("publish_failed_total", 1, &labels),
        }
        self.report_queue_pressure(runtime.as_ref(), &[("lane", self.inner.name.as_ref())]);
        result
    }

    /// 上报本 lane 的队列压力：`utilization` 是**百分比**（0-100），另附绝对在途数。
    ///
    /// 要的是 utilization；只发绝对值时运维无法判断"离满还有多远"，
    /// 而 observer 容量是 per-lane 可配的。两条路径（send/fire）都必须发，
    /// 否则高频的 fire 路径完全没有压力观测。
    ///
    /// utilization 的分母只能是 fire observer 容量，分子也只能是它的占用：
    /// 这是 lane 上唯一有界的资源。`active` 含 fire 路径，二者相加即重复计数
    /// （见 `QueuePressure`），故 `active` 只作绝对值发布，不参与比例。
    ///
    /// # 参数
    ///
    /// - `runtime`: 指标出口。
    /// - `labels`: 已构造的 lane 标签（不含 topic：这是 lane 级量，带 topic 会让
    ///   同一 lane 在 N 个 topic 上产生 N 条同值序列，`sum by (lane)` 直接翻 N 倍）。
    fn report_queue_pressure(&self, runtime: &KafkaProxyInner, labels: crate::MetricLabels<'_>) {
        // 走无锁取数：本函数在每条消息（含高频 fire 路径）上都会执行。
        let pressure = self.inner.client.queue_pressure();
        let capacity = pressure.observer_capacity.max(1);
        // 先除后乘避免 in_use 极大时的乘法溢出（saturating 会把比例压成 100 以上的假值）。
        let percent = (pressure.observer_in_use.min(capacity) * 100) / capacity;
        runtime.gauge(
            "producer_lane_queue_utilization",
            i64::try_from(percent).unwrap_or(i64::MAX),
            labels,
        );
        runtime.gauge(
            "producer_lane_in_flight",
            i64::try_from(pressure.active).unwrap_or(i64::MAX),
            labels,
        );
    }

    /// 拒绝新的业务发布，供统一关闭流程使用。
    pub(crate) fn begin_draining(&self) {
        self.inner.client.begin_draining();
    }

    /// 关闭业务入口后只保留框架收尾发布。
    pub(crate) fn enter_internal_only(&self) {
        self.inner.client.enter_internal_only();
    }

    /// 在全局绝对截止时刻前排空本 lane。
    ///
    /// # 参数
    ///
    /// - `deadline`: shutdown 共享的绝对截止时刻。
    ///
    /// # 错误
    ///
    /// 观察任务或底层队列未能及时排空时返回错误。
    pub(crate) async fn flush_until(&self, deadline: Instant) -> Result<()> {
        self.inner.client.flush_until(deadline).await
    }

    /// 在排空成功后永久关闭本 lane 的全部入口。
    pub(crate) fn close(&self) {
        self.inner.client.close();
    }

    /// 发布框架内部 raw 记录，允许携带已经由框架构造的保留 headers。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `payload`: 可空原始 payload。
    /// - `key`: 可选原始 key 字节。
    /// - `headers`: 已完成框架校验的有序 headers。
    /// - `timestamp`: 可选原始 broker 时间戳。
    ///
    /// # 错误
    ///
    /// 内部准入、入队、投递或超时失败时返回错误。
    pub(crate) async fn send_internal_raw(
        &self,
        topic: &str,
        payload: Option<&[u8]>,
        key: Option<&[u8]>,
        headers: &KafkaHeaders,
        timestamp: Option<i64>,
    ) -> Result<Delivery> {
        self.inner
            .client
            .send(
                OutboundRecord {
                    topic,
                    payload,
                    key,
                    partition: None,
                    timestamp,
                    headers,
                },
                true,
            )
            .await
    }
}

/// 从最终 headers 读取发布事件名，非法 UTF-8 仅用于观测时回退固定分类。
///
/// - `headers`: builder 已完成校验的有序 headers。
fn outbound_event(headers: &KafkaHeaders) -> &str {
    headers
        .last(HEADER_EVENT)
        .and_then(|header| header.value.as_deref())
        .and_then(|value| std::str::from_utf8(value).ok())
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_EVENT)
}

/// 发布 builder 共享的元数据字段。
#[derive(Default)]
struct BuilderFields {
    /// 业务事件名。
    event: Option<String>,
    /// 可选字符串 key。
    key: Option<String>,
    /// 可选固定分区。
    partition: Option<i32>,
    /// 可选毫秒时间戳。
    timestamp: Option<i64>,
    /// 业务自定义 header，保持插入顺序和重复项。
    headers: Vec<KafkaHeader>,
    /// 待编码进上下文 header 的业务透传对象。
    passthrough: serde_json::Map<String, serde_json::Value>,
    /// fluent API 无法直接返回错误时暂存的首个校验错误。
    invalid: Option<String>,
    /// 当前调用的父 trace context；finish 时派生 producer 子 context。
    trace_parent: Option<TraceContext>,
}

impl BuilderFields {
    /// 写入一个业务 header，并记录保留名误用。
    ///
    /// # 参数
    ///
    /// - `name`: 大小写敏感的 header 名。
    /// - `value`: 可空 header 值。
    fn push_header(&mut self, name: &str, value: Option<&[u8]>) {
        if name.contains('\0') {
            self.invalid
                .get_or_insert_with(|| "header 名禁止包含 NUL".into());
            return;
        }
        if is_reserved_header(name) {
            self.invalid
                .get_or_insert_with(|| format!("header `{name}` 为框架保留名"));
            return;
        }
        self.headers.push(KafkaHeader {
            name: name.to_owned(),
            value: value.map(<[u8]>::to_vec),
        });
    }

    /// 生成 headers，并在存在父 context 时让 wire context 与 producer span 共用同一子 span-id。
    fn finish_headers_with_span(
        &self,
        codec: Option<PayloadCodec>,
        recorder: Option<&SpanRecorder>,
    ) -> Result<(KafkaHeaders, Option<SpanGuard>)> {
        if let Some(message) = &self.invalid {
            return Err(NafkaError::Config(message.clone()));
        }
        let mut headers = self.headers.clone();
        let span = self.trace_parent.as_ref().and_then(|parent| {
            recorder.map(|recorder| recorder.start("Kafka publish", parent, SpanKind::Producer))
        });
        if let Some(parent) = self.trace_parent {
            let context = span
                .as_ref()
                .map(SpanGuard::context)
                .unwrap_or_else(|| parent.child(random_span_id()));
            headers.push(KafkaHeader {
                name: HEADER_TRACEPARENT.into(),
                value: Some(context.to_traceparent().into_bytes()),
            });
        }
        headers.push(KafkaHeader {
            name: HEADER_EVENT.into(),
            value: Some(
                self.event
                    .as_deref()
                    .unwrap_or(DEFAULT_EVENT)
                    .as_bytes()
                    .to_vec(),
            ),
        });
        if let Some(PayloadCodec::Proto(mode)) = codec {
            headers.push(KafkaHeader {
                name: HEADER_PAYLOAD_CODEC.into(),
                value: Some(PAYLOAD_CODEC_PROTOCOL_BYTES.as_bytes().to_vec()),
            });
            headers.push(KafkaHeader {
                name: HEADER_PAYLOAD_MODE.into(),
                value: Some(mode_wire_name(mode).as_bytes().to_vec()),
            });
        }
        if !self.passthrough.is_empty() {
            let value = serde_json::to_vec(&self.passthrough)
                .map_err(|error| NafkaError::Codec(format!("透传上下文编码失败: {error}")))?;
            headers.push(KafkaHeader {
                name: HEADER_PASSTHROUGH.into(),
                value: Some(value),
            });
        }
        Ok((KafkaHeaders::from_vec(headers), span))
    }
}

/// 类型化消息发布 builder。
pub struct PublishBuilder<'a, T> {
    /// 固定的 producer lane。
    lane: ProducerLane,
    /// 目标 topic。
    topic: &'a str,
    /// 借用的业务载荷。
    payload: &'a T,
    /// 发布元数据。
    fields: BuilderFields,
}

impl<'a, T: EncodePayload> PublishBuilder<'a, T> {
    /// 构造类型化发布 builder。
    ///
    /// # 参数
    ///
    /// - `lane`: 固定发布 lane。
    /// - `topic`: 目标 topic。
    /// - `payload`: 借用的载荷。
    fn new(lane: ProducerLane, topic: &'a str, payload: &'a T) -> Self {
        Self {
            lane,
            topic,
            payload,
            fields: BuilderFields::default(),
        }
    }

    /// 设置业务事件名。
    ///
    /// # 参数
    ///
    /// - `event`: 非空事件名；最终校验在发送时完成。
    pub fn event(mut self, event: &str) -> Self {
        self.fields.event = Some(event.to_owned());
        self
    }

    /// 设置字符串 key。
    ///
    /// # 参数
    ///
    /// - `key`: 用于分区和业务去重的 key。
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.fields.key = Some(key.into());
        self
    }

    /// 固定目标分区。
    ///
    /// # 参数
    ///
    /// - `partition`: 非负分区号。
    pub fn partition(mut self, partition: i32) -> Self {
        self.fields.partition = Some(partition);
        self
    }

    /// 设置消息时间戳。
    ///
    /// # 参数
    ///
    /// - `epoch_ms`: Unix epoch 毫秒。
    pub fn timestamp(mut self, epoch_ms: i64) -> Self {
        self.fields.timestamp = Some(epoch_ms);
        self
    }

    /// 追加业务 header；同名项保留顺序，不做 Map 折叠。
    ///
    /// # 参数
    ///
    /// - `name`: header 名。
    /// - `value`: 可空 header 值。
    pub fn header(mut self, name: &str, value: Option<&[u8]>) -> Self {
        self.fields.push_header(name, value);
        self
    }

    /// 绑定当前 trace；发送时自动派生 producer 子 span 并写唯一 `traceparent`。
    pub fn trace_context(mut self, parent: &TraceContext) -> Self {
        self.fields.trace_parent = Some(*parent);
        self
    }

    /// 复制消费上下文中的透传 Map，便于消费后继续发布。
    ///
    /// # 参数
    ///
    /// - `ctx`: 当前消费记录上下文。
    pub fn ctx(mut self, ctx: &ConsumeCtx) -> Self {
        self.fields.passthrough = ctx.passthrough.clone();
        self.fields.trace_parent = ctx.trace_context();
        self
    }

    /// 写入一个可序列化透传字段。
    ///
    /// # 参数
    ///
    /// - `key`: 透传字段名。
    /// - `value`: 可转换为 JSON value 的字段值。
    ///
    /// # 错误
    ///
    /// `value` 无法转换为 JSON value 时返回 codec 错误。
    pub fn passthrough(mut self, key: &str, value: impl Serialize) -> Result<Self> {
        let value = serde_json::to_value(value)
            .map_err(|error| NafkaError::Codec(format!("透传字段编码失败: {error}")))?;
        self.fields.passthrough.insert(key.to_owned(), value);
        Ok(self)
    }

    /// 编码、入队并等待 broker delivery report。
    ///
    /// # 错误
    ///
    /// 编码、准入、入队、投递或超时失败时返回错误；超时会区分结果未知。
    pub async fn send(self) -> Result<Delivery> {
        self.lane.ensure_publish_open()?;
        validate_destination(self.topic, &self.fields)?;
        let recorder = self.lane.span_recorder();
        let (headers, span) = self
            .fields
            .finish_headers_with_span(Some(T::CODEC), recorder.as_ref())?;
        let payload = self.payload.encode()?;
        let result = self
            .lane
            .send_record(OutboundRecord {
                topic: self.topic,
                payload: Some(&payload),
                key: self.fields.key.as_deref().map(str::as_bytes),
                partition: self.fields.partition,
                timestamp: self.fields.timestamp,
                headers: &headers,
            })
            .await;
        if let Some(span) = span {
            let _ = span.finish(None);
        }
        result
    }

    /// 编码并登记可观察的异步投递。
    ///
    /// # 错误
    ///
    /// 只有观察槽位预留成功且消息同步入队后才返回成功。
    pub fn fire(self) -> Result<()> {
        self.lane.ensure_publish_open()?;
        validate_destination(self.topic, &self.fields)?;
        let recorder = self.lane.span_recorder();
        let (headers, span) = self
            .fields
            .finish_headers_with_span(Some(T::CODEC), recorder.as_ref())?;
        let payload = self.payload.encode()?;
        let result = self.lane.fire_record(OutboundRecord {
            topic: self.topic,
            payload: Some(&payload),
            key: self.fields.key.as_deref().map(str::as_bytes),
            partition: self.fields.partition,
            timestamp: self.fields.timestamp,
            headers: &headers,
        });
        if let Some(span) = span {
            let _ = span.finish(None);
        }
        result
    }
}

/// raw bytes 发布 builder；不会生成 codec header，也不会复制为 serde 临时对象。
pub struct RawPublishBuilder<'a> {
    /// 固定的 producer lane。
    lane: ProducerLane,
    /// 目标 topic。
    topic: &'a str,
    /// 原始 payload；空 slice 与 tombstone 语义不同。
    payload: &'a [u8],
    /// 发布元数据。
    fields: BuilderFields,
}

impl<'a> RawPublishBuilder<'a> {
    /// 构造 raw 发布 builder。
    ///
    /// # 参数
    ///
    /// - `lane`: 固定发布 lane。
    /// - `topic`: 目标 topic。
    /// - `payload`: 原始 payload。
    fn new(lane: ProducerLane, topic: &'a str, payload: &'a [u8]) -> Self {
        Self {
            lane,
            topic,
            payload,
            fields: BuilderFields::default(),
        }
    }

    /// 设置业务事件名。
    ///
    /// # 参数
    ///
    /// - `event`: 非空事件名。
    pub fn event(mut self, event: &str) -> Self {
        self.fields.event = Some(event.to_owned());
        self
    }

    /// 设置字符串 key。
    ///
    /// # 参数
    ///
    /// - `key`: 用于分区和业务去重的 key。
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.fields.key = Some(key.into());
        self
    }

    /// 固定目标分区。
    ///
    /// # 参数
    ///
    /// - `partition`: 非负分区号。
    pub fn partition(mut self, partition: i32) -> Self {
        self.fields.partition = Some(partition);
        self
    }

    /// 设置消息时间戳。
    ///
    /// # 参数
    ///
    /// - `epoch_ms`: Unix epoch 毫秒。
    pub fn timestamp(mut self, epoch_ms: i64) -> Self {
        self.fields.timestamp = Some(epoch_ms);
        self
    }

    /// 追加业务 header。
    ///
    /// # 参数
    ///
    /// - `name`: header 名。
    /// - `value`: 可空 header 值。
    pub fn header(mut self, name: &str, value: Option<&[u8]>) -> Self {
        self.fields.push_header(name, value);
        self
    }

    /// 绑定当前 trace；发送时自动派生 producer 子 span 并写唯一 `traceparent`。
    pub fn trace_context(mut self, parent: &TraceContext) -> Self {
        self.fields.trace_parent = Some(*parent);
        self
    }

    /// 入队并等待 broker delivery report。
    ///
    /// # 错误
    ///
    /// 准入、入队、投递或超时失败时返回错误。
    pub async fn send(self) -> Result<Delivery> {
        self.lane.ensure_publish_open()?;
        validate_destination(self.topic, &self.fields)?;
        let recorder = self.lane.span_recorder();
        let (headers, span) = self
            .fields
            .finish_headers_with_span(None, recorder.as_ref())?;
        let result = self
            .lane
            .send_record(OutboundRecord {
                topic: self.topic,
                payload: Some(self.payload),
                key: self.fields.key.as_deref().map(str::as_bytes),
                partition: self.fields.partition,
                timestamp: self.fields.timestamp,
                headers: &headers,
            })
            .await;
        if let Some(span) = span {
            let _ = span.finish(None);
        }
        result
    }

    /// 登记可观察的 raw 异步投递。
    ///
    /// # 错误
    ///
    /// 观察槽位不足、准入或同步入队失败时返回错误。
    pub fn fire(self) -> Result<()> {
        self.lane.ensure_publish_open()?;
        validate_destination(self.topic, &self.fields)?;
        let recorder = self.lane.span_recorder();
        let (headers, span) = self
            .fields
            .finish_headers_with_span(None, recorder.as_ref())?;
        let result = self.lane.fire_record(OutboundRecord {
            topic: self.topic,
            payload: Some(self.payload),
            key: self.fields.key.as_deref().map(str::as_bytes),
            partition: self.fields.partition,
            timestamp: self.fields.timestamp,
            headers: &headers,
        });
        if let Some(span) = span {
            let _ = span.finish(None);
        }
        result
    }
}

/// 显式 Kafka tombstone 发布 builder。
pub struct TombstoneBuilder<'a> {
    /// 固定 producer lane。
    lane: ProducerLane,
    /// 目标 topic。
    topic: &'a str,
    /// tombstone 必需的 key。
    key: &'a str,
    /// 发布元数据。
    fields: BuilderFields,
}

impl<'a> TombstoneBuilder<'a> {
    /// 构造 tombstone builder。
    ///
    /// # 参数
    ///
    /// - `lane`: 固定发布 lane。
    /// - `topic`: 目标 topic。
    /// - `key`: 非空消息 key。
    fn new(lane: ProducerLane, topic: &'a str, key: &'a str) -> Self {
        Self {
            lane,
            topic,
            key,
            fields: BuilderFields::default(),
        }
    }

    /// 设置业务事件名。
    ///
    /// # 参数
    ///
    /// - `event`: 非空事件名。
    pub fn event(mut self, event: &str) -> Self {
        self.fields.event = Some(event.to_owned());
        self
    }

    /// 追加业务 header。
    ///
    /// # 参数
    ///
    /// - `name`: header 名。
    /// - `value`: 可空 header 值。
    pub fn header(mut self, name: &str, value: Option<&[u8]>) -> Self {
        self.fields.push_header(name, value);
        self
    }

    /// 绑定当前 trace；发送时自动派生 producer 子 span 并写唯一 `traceparent`。
    pub fn trace_context(mut self, parent: &TraceContext) -> Self {
        self.fields.trace_parent = Some(*parent);
        self
    }

    /// 发布 null value 并等待 delivery report。
    ///
    /// # 错误
    ///
    /// key 为空、准入、入队、投递或超时失败时返回错误。
    pub async fn send(self) -> Result<Delivery> {
        self.lane.ensure_publish_open()?;
        validate_destination(self.topic, &self.fields)?;
        if self.key.is_empty() {
            return Err(NafkaError::Publish {
                lane: self.lane.name().into(),
                topic: self.topic.into(),
                message: "tombstone key 不能为空".into(),
            });
        }
        let recorder = self.lane.span_recorder();
        let (headers, span) = self
            .fields
            .finish_headers_with_span(None, recorder.as_ref())?;
        let result = self
            .lane
            .send_record(OutboundRecord {
                topic: self.topic,
                payload: None,
                key: Some(self.key.as_bytes()),
                partition: self.fields.partition,
                timestamp: self.fields.timestamp,
                headers: &headers,
            })
            .await;
        if let Some(span) = span {
            let _ = span.finish(None);
        }
        result
    }

    /// 登记可观察的 tombstone 异步投递。
    ///
    /// # 错误
    ///
    /// key 为空、观察槽位不足或同步入队失败时返回错误。
    pub fn fire(self) -> Result<()> {
        self.lane.ensure_publish_open()?;
        validate_destination(self.topic, &self.fields)?;
        if self.key.is_empty() {
            return Err(NafkaError::Publish {
                lane: self.lane.name().into(),
                topic: self.topic.into(),
                message: "tombstone key 不能为空".into(),
            });
        }
        let recorder = self.lane.span_recorder();
        let (headers, span) = self
            .fields
            .finish_headers_with_span(None, recorder.as_ref())?;
        let result = self.lane.fire_record(OutboundRecord {
            topic: self.topic,
            payload: None,
            key: Some(self.key.as_bytes()),
            partition: self.fields.partition,
            timestamp: self.fields.timestamp,
            headers: &headers,
        });
        if let Some(span) = span {
            let _ = span.finish(None);
        }
        result
    }
}

/// 批量顺序发布的单个输入项。
pub struct PublishItem<T> {
    /// 业务 payload。
    pub payload: T,
    /// 可选字符串 key。
    pub key: Option<String>,
    /// 可选事件名。
    pub event: Option<String>,
}

impl<T> PublishItem<T> {
    /// 以 payload 构造批量发布项。
    ///
    /// # 参数
    ///
    /// - `payload`: 拥有型业务载荷。
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            key: None,
            event: None,
        }
    }

    /// 设置消息 key。
    ///
    /// # 参数
    ///
    /// - `key`: 字符串 key。
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// 设置事件名。
    ///
    /// # 参数
    ///
    /// - `event`: 非空事件名。
    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }
}

/// 顺序批量发布失败；保留已经确认成功的前缀。
#[derive(Debug)]
pub struct BatchPublishError {
    /// 已确认 delivery success 的输入前缀。
    pub delivered: Vec<Delivery>,
    /// 首个失败或结果未知的输入下标。
    pub failed_index: usize,
    /// 首个失败原因。
    pub source: NafkaError,
}

impl KafkaProxy {
    /// 返回已配置 producer lane 的轻量句柄。
    ///
    /// # 参数
    ///
    /// - `name`: `default` 或配置中的命名 lane。
    ///
    /// # 错误
    ///
    /// lane 未配置时返回 [`NafkaError::NoSuchProducerLane`]。
    pub fn producer_lane(&self, name: &str) -> Result<ProducerLane> {
        self.inner
            .producer_lanes
            .get()
            .and_then(|lanes| lanes.get(name))
            .cloned()
            .ok_or_else(|| NafkaError::NoSuchProducerLane(name.to_owned()))
    }

    /// 使用 default lane 创建类型化发布 builder。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `payload`: 借用的业务载荷。
    pub fn publish<'a, T: EncodePayload>(
        &'a self,
        topic: &'a str,
        payload: &'a T,
    ) -> PublishBuilder<'a, T> {
        PublishBuilder::new(self.default_lane(), topic, payload)
    }

    /// 使用 default lane 创建 tombstone builder。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `key`: tombstone 必需 key。
    pub fn tombstone<'a>(&'a self, topic: &'a str, key: &'a str) -> TombstoneBuilder<'a> {
        TombstoneBuilder::new(self.default_lane(), topic, key)
    }

    /// 使用 default lane 创建 raw bytes 发布 builder。
    ///
    /// # 参数
    ///
    /// - `topic`: 目标 topic。
    /// - `payload`: 原始 payload。
    pub fn publish_raw<'a>(&'a self, topic: &'a str, payload: &'a [u8]) -> RawPublishBuilder<'a> {
        RawPublishBuilder::new(self.default_lane(), topic, payload)
    }

    /// 按输入顺序逐条发布，并在首个失败处停止。
    ///
    /// # 参数
    ///
    /// - `topic`: 全部输入项的目标 topic。
    /// - `items`: 拥有型发布项迭代器。
    ///
    /// # 错误
    ///
    /// 返回已经确认的成功前缀、失败下标和原始错误。
    pub async fn publish_batch<T: EncodePayload>(
        &self,
        topic: &str,
        items: impl IntoIterator<Item = PublishItem<T>>,
    ) -> std::result::Result<Vec<Delivery>, BatchPublishError> {
        let lane = self.default_lane();
        let mut delivered = Vec::new();
        for (failed_index, item) in items.into_iter().enumerate() {
            let mut builder = lane.publish(topic, &item.payload);
            if let Some(key) = item.key {
                builder = builder.key(key);
            }
            if let Some(event) = item.event {
                builder = builder.event(&event);
            }
            match builder.send().await {
                Ok(delivery) => delivered.push(delivery),
                Err(source) => {
                    return Err(BatchPublishError {
                        delivered,
                        failed_index,
                        source,
                    })
                }
            }
        }
        Ok(delivered)
    }

    /// 并发排空全部已配置 lane，并共享同一个总 deadline。
    ///
    /// # 错误
    ///
    /// 任一 lane 未排空时返回带 lane 归因的错误。
    pub async fn flush(&self) -> Result<()> {
        let deadline =
            Instant::now() + Duration::from_millis(self.inner.config.behavior.shutdown_timeout_ms);
        let lanes = self
            .inner
            .producer_lanes
            .get()
            .expect("connect 必须冻结 producer lane 表");
        let results =
            futures::future::join_all(lanes.values().map(|lane| lane.flush_until(deadline))).await;
        let mut failed = Vec::new();
        for (lane, result) in lanes.keys().zip(results) {
            if let Err(error) = result {
                tracing::error!(lane, error = %error, "producer lane 排空失败");
                failed.push(lane.clone());
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(NafkaError::ShutdownTimeout {
                groups: Vec::new(),
                producer_lanes: failed,
            })
        }
    }

    /// 构造 default lane 轻量句柄。
    fn default_lane(&self) -> ProducerLane {
        let Some(lanes) = self.inner.producer_lanes.get() else {
            unreachable!("connect 必须先冻结 producer lane 表")
        };
        let Some(lane) = lanes.get("default") else {
            unreachable!("connect 必须创建 default producer lane")
        };
        lane.clone()
    }
}

/// 校验所有发布 builder 共有的目标字段。
///
/// # 参数
///
/// - `topic`: 目标 topic。
/// - `fields`: builder 元数据。
///
/// # 错误
///
/// topic、event 或 partition 非法时返回确定失败。
fn validate_destination(topic: &str, fields: &BuilderFields) -> Result<()> {
    if topic.trim().is_empty() {
        return Err(NafkaError::Config("publish topic 不能为空".into()));
    }
    if fields.event.as_deref().is_some_and(str::is_empty) {
        return Err(NafkaError::Config("publish event 不能为空".into()));
    }
    if fields.partition.is_some_and(|partition| partition < 0) {
        return Err(NafkaError::Config("publish partition 不能为负数".into()));
    }
    Ok(())
}

/// 解析命名 lane 的有效覆盖，供 P1 底层配置构建使用。
///
/// # 参数
///
/// - `lanes`: 配置中的命名 lane 表。
///
/// # 返回
///
/// 保持名字排序的 lane 名列表。
pub(crate) fn configured_lane_names(
    lanes: &BTreeMap<String, crate::config::ProducerLaneOverride>,
) -> Vec<String> {
    std::iter::once("default".to_owned())
        .chain(lanes.keys().cloned())
        .collect()
}
