// ============================================================================
// src/stream.rs -- ordinary stream publish / subscribe for the event-field wire.
//
// wire = **event-field**:`XADD <stream> * <event> <Envelope JSON>`,event 作 entry 的 field 名,
// value 是 `{"topic":<stream>,"data":<message>[,"passthrough":..]}` 信封。
// 与 proxy/partition 的 data-field wire(field 恒 "data")**两套,不互通**——本模块专供既有事件流协议互通。
//
// 消费两种模式:
//   · Broadcast(XREAD):每个订阅者独立游标,所有节点都收到每条(如 init:coin 每节点各建引擎)。
//   · Group(XREADGROUP):同组负载均衡,一条只一个消费者处理 + XACK(如 kline-sync 一请求一节点应答)。
//
// 两个关键规避(实测踩过):
//   1. 阻塞读走【专用连接】(client.dedicated_conn),不占共享 conn——否则 XREAD BLOCK 把并发 publish 卡到 ~1/s。
//   2. 起点用【具体 id】(Now = XREVRANGE 取最后一条已有 entry id,空/不存在则 0-0),之后只按读到的 id 推进——
//      不用会重求值为"最新"的 "$",否则两轮读之间到达的 entry 被跳过。
// ============================================================================

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use redis::Value;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::{Conn, RedisClient};
use crate::config::{MAX_REDIS_NAME_BYTES, MAX_REDIS_RUNTIME_DURATION_MS, MAX_STREAM_BATCH_SIZE};
use crate::error::{NasaRedisError, Result};

/// `publish(stream, message)` 的默认事件名。
pub const STREAM_EVENT: &str = "msg";

// ───────────────────────── 低层类型 ─────────────────────────

/// 一条 stream entry 里的一个 (event, value) field(lossless:保序、保重复、保二进制)。
#[derive(Debug, Clone)]
pub struct StreamField {
    /// field 名 = 事件名(event-field wire)。
    pub event: String,
    /// field 原始 value 字节(通常是 envelope JSON)。
    pub raw: Vec<u8>,
}

/// XREAD/XREADGROUP 读到的一条 entry。
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// 所属 stream 名。
    pub stream: String,
    /// entry id(如 `1700000000000-0`)。
    pub id: String,
    /// 该 entry 的所有 field(一条 entry 可含多个 event,如 publish_many)。
    pub fields: Vec<StreamField>,
}

/// stream 信封:`{topic, data[, passthrough][, event]}`。
/// event 只作 Redis field 名、不入信封;`event` 字段仅为容忍历史消息保留,Rust 发布不主动输出。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEnvelope<T> {
    /// = 发布到的 stream 名;消费端 deserializeUnwrap 用 `topic==stream` 确认是框架 wrap。
    pub topic: String,
    /// 业务 payload。
    pub data: T,
    /// 透传 map(Rust 默认 None → 省略;仅为读历史透传保留)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passthrough: Option<serde_json::Map<String, JsonValue>>,
    /// 仅兼容反序列化;发布时恒 None → 省略(event 是物理 field 名)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
}

/// `publish_many` 的一项:一个 event + 其 data(已序列化为 `serde_json::Value`,允许各 event 类型不同)。
#[derive(Debug, Clone)]
pub struct StreamPublishItem {
    /// 事件名(= Redis field 名)。
    pub event: String,
    /// 该事件的 payload。
    pub data: JsonValue,
}

impl StreamPublishItem {
    /// 业务作用：由任意 `Serialize` 消息构造(序列化为 Value;失败返回 Err)。
    ///
    /// # 参数
    /// - `event`: 写入 Redis Stream entry 的 field 名,同时用于订阅侧选择 handler。
    /// - `message`: 业务消息体或事件载荷。
    pub fn new<T: Serialize>(event: impl Into<String>, message: &T) -> Result<Self> {
        let data = serde_json::to_value(message)
            .map_err(|e| NasaRedisError::Config(format!("StreamPublishItem 序列化失败: {e}")))?;
        Ok(Self {
            event: event.into(),
            data,
        })
    }
}

// ───────────────────────── 订阅配置类型 ─────────────────────────

/// Broadcast 起点。
#[derive(Debug, Clone)]
pub enum StreamStart {
    /// 从"当前时刻"起(具体 id:XREVRANGE 取最后一条已有 entry id,空/流不存在则 0-0),只收之后的新事件。
    Now,
    /// 从指定具体 entry id 之后；必须是 `<milliseconds>-<sequence>`，不接受会重求最新位置的 `$`。
    After(String),
    /// 从流头(全历史)。
    Beginning,
}

/// 消费组建组起点(仅首次 XGROUP CREATE 用)。
#[derive(Debug, Clone)]
pub enum StreamGroupStart {
    /// 只收建组后的新消息(`$`)。
    New,
    /// 从流头开始消费历史(`0-0`)。
    History,
}

/// 消费模式。
#[derive(Debug, Clone)]
pub enum StreamMode {
    /// 广播:XREAD,独立游标,所有订阅者都收到。
    Broadcast {
        /// 广播订阅起点。
        start: StreamStart,
    },
    /// 消费组:XREADGROUP + XACK,组内负载均衡。
    Group {
        /// Redis consumer group 名称。
        group: String,
        /// 当前消费者名称。
        consumer: String,
        /// 首次建组时使用的起始位置。
        start: StreamGroupStart,
    },
}

/// 消费组 ACK 策略。
#[derive(Debug, Clone, Copy)]
pub enum StreamAckPolicy {
    /// 默认 auto-ack:entry 完成一次分发尝试后即 ACK(handler 失败也 ACK,不滞留 PEL)。
    Auto,
    /// 更严:该 entry 中所有已注册 event handler 都成功才 ACK,任一失败不 ACK(留在 PEL)。
    ///
    /// ⚠ 语义边界:本轻量 subscribe 循环只读 `>`(新消息)、**不自动重投 PEL**——失败留在 PEL 的 entry
    ///   【不会被本消费者自动重试】,需外部恢复:XAUTOCLAIM 转移、消费者重启从 `0` 重读、或改用
    ///   **proxy 路径**(消费组 + 内置 XAUTOCLAIM reclaim,自带失败重投)。若你要"失败自动重投",请用 proxy
    ///   而非本 subscribe;OnSuccess 在此仅提供"失败不丢(留痕 PEL)+ 可外部重放"的保证。
    OnSuccess,
}

/// 订阅运行参数。
#[derive(Debug, Clone)]
pub struct StreamSubscribeCfg {
    /// 消费模式(广播 / 组)。
    pub mode: StreamMode,
    /// 每次 XREAD(GROUP) 的 COUNT。
    pub batch_size: usize,
    /// XREAD(GROUP) 的 BLOCK 毫秒(0 = 非阻塞轮询;>0 = 阻塞,走专用连接)。
    pub block_ms: u64,
    /// 空轮/出错时的退避睡眠毫秒(阻塞模式下 BLOCK 本身已等待,主要用于出错退避与非阻塞轮询间隔)。
    pub idle_sleep_ms: u64,
    /// 单个 handler 的强制超时毫秒(0 = 不限)。超时按失败处理。
    pub handler_timeout_ms: u64,
    /// 组模式 ACK 策略。
    pub ack_policy: StreamAckPolicy,
}

impl Default for StreamSubscribeCfg {
    /// 业务作用：构造 stream 订阅默认配置。
    ///
    /// 默认从当前时间开始做广播订阅,100 条批量、500ms block 和自动 ACK,适合普通事件监听。
    fn default() -> Self {
        Self {
            mode: StreamMode::Broadcast {
                start: StreamStart::Now,
            },
            batch_size: 100,
            block_ms: 500,
            idle_sleep_ms: 200,
            handler_timeout_ms: 30_000,
            ack_policy: StreamAckPolicy::Auto,
        }
    }
}

/// 分发给 `on` handler 的原始事件(raw = 该 field 的原始字节,通常是 envelope JSON;不解包)。
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// Redis stream key。
    pub stream: String,
    /// Redis stream entry id。
    pub id: String,
    /// entry 中的业务事件名。
    pub event: String,
    /// 事件原始 payload 字节。
    pub raw: Vec<u8>,
}

/// 分发给 `on_typed` handler 的解包后事件(已按 deserializeUnwrap 取 data 并反序列化为 T)。
#[derive(Debug, Clone)]
pub struct StreamTypedEvent<T> {
    /// Redis stream key。
    pub stream: String,
    /// Redis stream entry id。
    pub id: String,
    /// entry 中的业务事件名。
    pub event: String,
    /// 已反序列化的业务数据。
    pub data: T,
    /// envelope 中除标准字段外保留下来的透传字段。
    pub passthrough: Option<serde_json::Map<String, JsonValue>>,
}

// 类型擦除后的 handler:输入原始 StreamEvent,输出 Result<(), 错误说明>。
type BoxFut = Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>;
type Handler = Arc<dyn Fn(StreamEvent) -> BoxFut + Send + Sync>;

// ───────────────────────── 发布 / 低层读 API(RedisClient) ─────────────────────────

impl RedisClient {
    /// 业务作用：发布一条 stream 消息(event-field wire),等价于带 event 的 publish 入口。
    /// 写 `XADD <stream> * <event> <StreamEnvelope JSON>`。空 stream/event 或 message 序列化为 JSON null → Err。
    ///
    /// # 参数
    /// - `stream`: XADD 写入的 Redis Stream key。
    /// - `event`: entry field 名,订阅侧按该值分发 handler。
    /// - `message`: 业务消息体或事件载荷。
    pub async fn publish<T: Serialize>(
        &self,
        stream: &str,
        event: &str,
        message: &T,
    ) -> Result<String> {
        // 校验 + 信封编码走共享 helper(与 PipelineSession::publish 同一路径,保证逐字节一致)。
        let body = stream_publish_body(stream, event, message)?;
        self.x_add_bytes(stream, "*", &[(event, body.as_slice())])
            .await
    }

    /// 业务作用：使用默认 event = `STREAM_EVENT`(`"msg"`)发布一条 stream 消息。
    ///
    /// # 参数
    /// - `stream`: XADD 写入的 Redis Stream key。
    /// - `message`: 业务消息体或事件载荷。
    pub async fn publish_default<T: Serialize>(&self, stream: &str, message: &T) -> Result<String> {
        self.publish(stream, STREAM_EVENT, message).await
    }

    /// 业务作用：同一条 entry 写多个 event field,
    /// 每个 field 的 value 各自是独立 `{topic,data}` 信封。空 events 或任一 event 空 / data null → Err。
    ///
    /// # 参数
    /// - `stream`: 目标 Redis Stream 名。
    /// - `events`: 同一 entry 内要写入的多个 event field。
    pub async fn publish_many(&self, stream: &str, events: &[StreamPublishItem]) -> Result<String> {
        if stream.is_empty() {
            return Err(NasaRedisError::Config(
                "publish_many: stream 不能为空".into(),
            ));
        }
        if events.is_empty() {
            return Err(NasaRedisError::Config(
                "publish_many: events 不能为空".into(),
            ));
        }
        // 先把每项编码成 (event, envelope bytes),持有 owned buffer 再借出 &[u8]。
        let mut bufs: Vec<(String, Vec<u8>)> = Vec::with_capacity(events.len());
        for item in events {
            if item.event.is_empty() {
                return Err(NasaRedisError::Config(
                    "publish_many: event 不能为空".into(),
                ));
            }
            if item.data.is_null() {
                return Err(NasaRedisError::Config(
                    "publish_many: message 序列化为 JSON null".into(),
                ));
            }
            let bytes = encode_envelope(stream, &item.data)?;
            bufs.push((item.event.clone(), bytes));
        }
        let fields: Vec<(&str, &[u8])> = bufs
            .iter()
            .map(|(e, b)| (e.as_str(), b.as_slice()))
            .collect();
        self.x_add_bytes(stream, "*", &fields).await
    }

    /// 业务作用：裸 XREAD(非消费组,单 stream)。`block_ms=Some` → BLOCK,**走专用连接**(每次新建,供偶发一次性读;
    /// 热循环请用 `subscribe()`);`None` → 非阻塞走共享连接。调用方自管游标(推进到读到的最后一条 id)。
    ///
    /// # 参数
    /// - `stream`: 要读取的 Redis Stream 名。
    /// - `cursor`: XREAD 起始游标。
    /// - `count`: 单次最多读取条数,范围 1–10000。
    /// - `block_ms`: 可选阻塞等待毫秒数;`None` 表示非阻塞读取。
    pub async fn x_read(
        &self,
        stream: &str,
        cursor: &str,
        count: usize,
        block_ms: Option<u64>,
    ) -> Result<Vec<StreamEntry>> {
        if stream.is_empty() {
            return Err(NasaRedisError::Config("x_read: stream 不能为空".into()));
        }
        if count == 0 {
            return Err(NasaRedisError::Config("x_read: count 必须 > 0".into()));
        }
        if count > MAX_STREAM_BATCH_SIZE {
            return Err(NasaRedisError::Config(format!(
                "x_read: count 过大(上限 {MAX_STREAM_BATCH_SIZE})"
            )));
        }
        // BLOCK 0 = 无限阻塞;低层 API 无 CancellationToken,禁止(用 None 非阻塞或 Some(>0))。
        if block_ms == Some(0) {
            return Err(NasaRedisError::Config(
                "x_read: block_ms=Some(0)(无限阻塞)不允许——用 None 非阻塞或 Some(>0)".into(),
            ));
        }
        let cmd = build_xread_cmd(stream, cursor, count, block_ms);
        let v: Value = match block_ms {
            None => self.execute_raw(&cmd).await?,
            Some(_) => {
                let mut conn = self.dedicated_conn("x_read blocking").await?;
                cmd.query_async(&mut conn).await?
            }
        };
        parse_xread_reply(v)
    }

    /// 业务作用：裸 XREADGROUP(消费组,单 stream,读 `>` 新消息)。同 `x_read`:阻塞走专用连接。
    /// 不自动建组(需先 `XGROUP CREATE`);不自动 ACK(调用方按需 `x_ack`)。
    ///
    /// # 参数
    /// - `stream`: 要读取的 Redis Stream 名。
    /// - `group`: consumer group 名。
    /// - `consumer`: 当前消费者名。
    /// - `cursor`: XREADGROUP 游标,通常为 `>`。
    /// - `count`: 单次最多读取条数,范围 1–10000。
    /// - `block_ms`: 可选阻塞等待毫秒数;`None` 表示非阻塞读取。
    pub async fn x_read_group(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        cursor: &str,
        count: usize,
        block_ms: Option<u64>,
    ) -> Result<Vec<StreamEntry>> {
        if stream.is_empty() || group.is_empty() || consumer.is_empty() {
            return Err(NasaRedisError::Config(
                "x_read_group: stream / group / consumer 不能为空".into(),
            ));
        }
        if count == 0 {
            return Err(NasaRedisError::Config(
                "x_read_group: count 必须 > 0".into(),
            ));
        }
        if count > MAX_STREAM_BATCH_SIZE {
            return Err(NasaRedisError::Config(format!(
                "x_read_group: count 过大(上限 {MAX_STREAM_BATCH_SIZE})"
            )));
        }
        // BLOCK 0 = 无限阻塞;低层 API 无 CancellationToken,禁止(用 None 非阻塞或 Some(>0))。
        if block_ms == Some(0) {
            return Err(NasaRedisError::Config(
                "x_read_group: block_ms=Some(0)(无限阻塞)不允许——用 None 非阻塞或 Some(>0)".into(),
            ));
        }
        let cmd = build_xreadgroup_cmd(stream, group, consumer, cursor, count, block_ms);
        let v: Value = match block_ms {
            None => self.execute_raw(&cmd).await?,
            Some(_) => {
                let mut conn = self.dedicated_conn("x_read_group blocking").await?;
                cmd.query_async(&mut conn).await?
            }
        };
        parse_xread_reply(v)
    }

    /// 业务作用：stream 订阅入口。返回 builder,链式 `.group()/.on()/.start()`。
    /// 默认 Broadcast + Now;`.group(g,c)` 切消费组。
    ///
    ///
    /// # 参数
    /// - `stream`: XREAD/XREADGROUP 订阅的 Redis Stream key。
    pub fn subscribe(self: &Arc<Self>, stream: impl Into<String>) -> StreamSubscriber {
        StreamSubscriber {
            client: Arc::clone(self),
            stream: stream.into(),
            cfg: StreamSubscribeCfg::default(),
            handlers: HashMap::new(),
        }
    }
}

// ───────────────────────── 订阅 builder ─────────────────────────

/// stream 订阅构建器。move-fluent:每个方法 `self -> Self`,末尾 `start().await` 起后台任务。
pub struct StreamSubscriber {
    client: Arc<RedisClient>,
    stream: String,
    cfg: StreamSubscribeCfg,
    handlers: HashMap<String, Handler>,
}

impl StreamSubscriber {
    /// 业务作用：整体替换运行参数。
    ///
    /// # 参数
    /// - `cfg`: stream 订阅运行配置。
    pub fn with_cfg(mut self, cfg: StreamSubscribeCfg) -> Self {
        self.cfg = cfg;
        self
    }

    /// 业务作用：设为广播模式并指定起点。
    ///
    /// # 参数
    /// - `start`: 广播模式读取起点。
    pub fn broadcast_from(mut self, start: StreamStart) -> Self {
        self.cfg.mode = StreamMode::Broadcast { start };
        self
    }

    /// 业务作用：设为消费组模式(默认建组起点 New;可再用 group_from 调整)。
    ///
    /// # 参数
    /// - `group`: consumer group 名。
    /// - `consumer`: 当前消费者名。
    pub fn group(mut self, group: impl Into<String>, consumer: impl Into<String>) -> Self {
        self.cfg.mode = StreamMode::Group {
            group: group.into(),
            consumer: consumer.into(),
            start: StreamGroupStart::New,
        };
        self
    }

    /// 业务作用：调整消费组建组起点(仅在已是 Group 模式时生效;否则忽略)。
    ///
    /// # 参数
    /// - `start`: 消费组首次建组时使用的起点。
    pub fn group_from(mut self, start: StreamGroupStart) -> Self {
        if let StreamMode::Group {
            group, consumer, ..
        } = self.cfg.mode
        {
            self.cfg.mode = StreamMode::Group {
                group,
                consumer,
                start,
            };
        }
        self
    }

    /// 业务作用：设置组模式 ACK 策略。
    ///
    /// # 参数
    /// - `policy`: handler 成功或失败后的 ACK 策略。
    pub fn ack_policy(mut self, policy: StreamAckPolicy) -> Self {
        self.cfg.ack_policy = policy;
        self
    }

    /// 业务作用：注册某 event 的 handler(收到原始 `StreamEvent`,raw 是 field 原始字节)。
    ///
    ///
    /// # 参数
    /// - `event`: 订阅的 Redis Stream entry field 名。
    /// - `handler`: 业务处理函数,在匹配事件或任务时被调用。
    pub fn on<F, Fut>(mut self, event: impl Into<String>, handler: F) -> Self
    where
        F: Fn(StreamEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let h: Handler = Arc::new(move |ev| Box::pin(handler(ev)));
        self.handlers.insert(event.into(), h);
        self
    }

    /// 业务作用：注册某 event 的类型化 handler:内部按 deserializeUnwrap 取 data 反序列化为 T 再回调。
    /// 解包/反序列化失败 → 视为该 field 处理失败(记 error,按 ack 策略处置)。
    ///
    ///
    /// # 参数
    /// - `event`: 订阅的 Redis Stream entry field 名。
    /// - `handler`: 业务处理函数,在匹配事件或任务时被调用。
    pub fn on_typed<T, F, Fut>(mut self, event: impl Into<String>, handler: F) -> Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(StreamTypedEvent<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let stream = self.stream.clone();
        let handler = Arc::new(handler);
        let h: Handler = Arc::new(move |ev: StreamEvent| {
            let handler = Arc::clone(&handler);
            let stream = stream.clone();
            Box::pin(async move {
                let (data_val, passthrough) = unwrap_envelope(&ev.raw, &stream)
                    .map_err(|e| format!("envelope 解包失败: {e}"))?;
                let data: T = serde_json::from_value(data_val)
                    .map_err(|e| format!("data 反序列化失败: {e}"))?;
                let tev = StreamTypedEvent {
                    stream: ev.stream,
                    id: ev.id,
                    event: ev.event,
                    data,
                    passthrough,
                };
                handler(tev).await
            })
        });
        self.handlers.insert(event.into(), h);
        self
    }

    /// 业务作用：起后台订阅任务。启动阶段同步:建专用连接、解析 Broadcast 起点(XREVRANGE)/建消费组(XGROUP CREATE),
    /// 这些若失败当场返回 Err;之后进入读循环。返回 `StreamSubscription`(drop 即 best-effort 停,shutdown().await 优雅停)。
    pub async fn start(self) -> Result<StreamSubscription> {
        if self.stream.trim().is_empty()
            || self.stream != self.stream.trim()
            || self.stream.len() > MAX_REDIS_NAME_BYTES
        {
            return Err(NasaRedisError::Config(
                format!(
                    "stream subscribe: stream key 必须无首尾空白、非空且不超过 {MAX_REDIS_NAME_BYTES} 字节"
                ),
            ));
        }
        if self.cfg.batch_size == 0 {
            return Err(NasaRedisError::Config(
                "stream subscribe: batch_size 必须 > 0".into(),
            ));
        }
        if self.cfg.batch_size > MAX_STREAM_BATCH_SIZE {
            return Err(NasaRedisError::Config(format!(
                "stream subscribe: batch_size 过大(上限 {MAX_STREAM_BATCH_SIZE})"
            )));
        }
        if self.cfg.idle_sleep_ms == 0
            || self.cfg.idle_sleep_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || self.cfg.block_ms > MAX_REDIS_RUNTIME_DURATION_MS
            || self.cfg.handler_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS
        {
            return Err(NasaRedisError::Config(format!(
                "stream subscribe: block/idle/handler 时长超出允许范围(idle 必须 >0，最大 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            )));
        }
        if matches!(
            &self.cfg.mode,
            StreamMode::Broadcast {
                start: StreamStart::After(id)
            } if !valid_stream_entry_id(id)
        ) {
            return Err(NasaRedisError::Config(
                "stream subscribe: After 必须使用 `<milliseconds>-<sequence>` 形式的具体 entry ID"
                    .into(),
            ));
        }
        if let StreamMode::Group {
            group, consumer, ..
        } = &self.cfg.mode
        {
            if group.trim().is_empty()
                || consumer.trim().is_empty()
                || group != group.trim()
                || consumer != consumer.trim()
                || group.len() > MAX_REDIS_NAME_BYTES
                || consumer.len() > MAX_REDIS_NAME_BYTES
            {
                return Err(NasaRedisError::Config(
                    format!(
                        "stream subscribe: group / consumer 必须无首尾空白、非空且不超过 {MAX_REDIS_NAME_BYTES} 字节"
                    ),
                ));
            }
        }
        let cancel = CancellationToken::new();
        let child = cancel.child_token();

        // 专用连接使 BLOCK 不占共享 command lane。Cluster 也创建独立 ClusterConnection；
        // redis-rs 会按 XREAD/XREADGROUP 的 STREAMS 首个 key 计算 slot，并在拓扑变化时处理
        // MOVED/ASK。本 builder 只允许单 stream，因此不会产生跨 slot 阻塞读。
        let conn = self.client.dedicated_conn("stream subscribe").await?;

        // 启动阶段解析起点 / 建组(错误当场上抛)。
        let read_kind = match &self.cfg.mode {
            StreamMode::Broadcast { start } => {
                let cursor = resolve_broadcast_cursor(&self.client, &self.stream, start).await?;
                ReadKind::Broadcast { cursor }
            }
            StreamMode::Group {
                group,
                consumer,
                start,
            } => {
                ensure_group(&self.client, &self.stream, group, start).await?;
                ReadKind::Group {
                    group: group.clone(),
                    consumer: consumer.clone(),
                }
            }
        };

        let handle = tokio::spawn(run_loop(
            self.client,
            self.stream,
            self.cfg,
            self.handlers,
            conn,
            read_kind,
            child,
        ));
        Ok(StreamSubscription {
            cancel,
            handle: Some(handle),
        })
    }
}

/// 订阅句柄。`shutdown().await` 优雅停(cancel + join);drop 只 best-effort cancel(任务下一轮退出)。
pub struct StreamSubscription {
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

impl StreamSubscription {
    /// 业务作用：优雅停机:取消 + 等任务退出。
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}

impl Drop for StreamSubscription {
    /// 业务作用：丢弃订阅句柄时触发协作取消。
    ///
    /// 不直接 abort Tokio task；读、退避、重连、handler 与 XACK 的 `select` 会观察该信号。
    /// handler future 若正在等待会被丢弃，组消息不 ACK 并留在 PEL；纯 CPU 且不 yield 的 handler
    /// 无法被异步取消，仍须由业务移入 `spawn_blocking` 或自行设置边界。
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ───────────────────────── 后台读循环 ─────────────────────────

// 运行时读模式:Broadcast 自管游标;Group 走 `>` + PEL,无本地游标。
enum ReadKind {
    Broadcast { cursor: String },
    Group { group: String, consumer: String },
}

// 订阅主循环:专用连接上 BLOCK 读 → 逐 entry 按 field 分发 → 广播推进游标 / 组按策略 XACK。
///
/// # 参数
/// 业务作用：- `client`: 底层客户端或连接句柄。
/// - `stream`: 正在读取的 Redis Stream key。
/// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
/// - `handlers`: stream 消息类型到业务处理器的映射。
/// - `conn`: 订阅循环独占的 Redis connection manager。
/// - `read_kind`: stream 消费使用的读取模式。
/// - `cancel`: 后台任务使用的取消信号。
async fn run_loop(
    client: Arc<RedisClient>,
    stream: String,
    cfg: StreamSubscribeCfg,
    handlers: HashMap<String, Handler>,
    mut conn: Conn,
    mut read_kind: ReadKind,
    cancel: CancellationToken,
) {
    // BLOCK 不能 >= 连接级 response_timeout(否则会被连接超时误杀);默认 30s >> 500ms,仅防误配。
    let resp_timeout = client.config().command.response_timeout_ms;
    let block_ms = effective_block(cfg.block_ms, resp_timeout);
    let idle = Duration::from_millis(cfg.idle_sleep_ms.max(1));

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let cmd = match &read_kind {
            ReadKind::Broadcast { cursor } => build_xread_cmd(
                &stream,
                cursor,
                cfg.batch_size,
                (block_ms > 0).then_some(block_ms),
            ),
            ReadKind::Group { group, consumer } => build_xreadgroup_cmd(
                &stream,
                group,
                consumer,
                ">",
                cfg.batch_size,
                (block_ms > 0).then_some(block_ms),
            ),
        };

        // 读,同时抢跑取消信号(shutdown 立即生效,不必等 BLOCK 结束)。
        let v: Value = tokio::select! {
            _ = cancel.cancelled() => break,
            r = cmd.query_async::<Value>(&mut conn) => match r {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(stream = %stream, err = %e, "stream 订阅读失败,退避后重建连接");
                    if sleep_or_cancel(&cancel, idle).await {
                        break;
                    }
                    let reconnect = client.dedicated_conn("stream subscribe reconnect");
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        result = reconnect => {
                            if let Ok(c) = result {
                                conn = c;
                            }
                        }
                    }
                    continue;
                }
            }
        };

        let entries = match parse_xread_reply(v) {
            Ok(e) => e,
            Err(e) => {
                // fail-closed:异常形态可见(不静默当空轮询);退避后重试,避免持续异常时空转刷屏。
                tracing::warn!(stream = %stream, err = %e, "stream 响应解析失败,退避重试");
                if sleep_or_cancel(&cancel, idle).await {
                    break;
                }
                continue;
            }
        };

        if entries.is_empty() {
            // 阻塞模式:BLOCK 已等过;非阻塞模式:歇 idle 再轮询。
            if block_ms == 0 && sleep_or_cancel(&cancel, idle).await {
                break;
            }
            continue;
        }

        for entry in entries {
            let mut all_ok = true;
            for field in &entry.fields {
                let Some(handler) = handlers.get(&field.event) else {
                    continue; // 未注册 event → 跳过(不影响 ACK)
                };
                let ev = StreamEvent {
                    stream: entry.stream.clone(),
                    id: entry.id.clone(),
                    event: field.event.clone(),
                    raw: field.raw.clone(),
                };
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        tracing::debug!(stream = %stream, id = %entry.id, "stream handler 因订阅停机被取消，消息不 ACK");
                        return;
                    }
                    ok = dispatch_one(handler, ev, cfg.handler_timeout_ms) => {
                        if !ok {
                            all_ok = false;
                        }
                    }
                }
            }
            match &mut read_kind {
                ReadKind::Broadcast { cursor } => {
                    // 广播无 ACK,只推进游标(handler 失败无重投;不丢事件的场景由调用方自备恢复)。
                    *cursor = entry.id.clone();
                }
                ReadKind::Group { group, .. } => {
                    let should_ack = match cfg.ack_policy {
                        StreamAckPolicy::Auto => true,
                        StreamAckPolicy::OnSuccess => all_ok,
                    };
                    if should_ack {
                        let ack_ids = [entry.id.as_str()];
                        let ack = client.x_ack(&stream, group, &ack_ids);
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => {
                                tracing::debug!(stream = %stream, id = %entry.id, "stream XACK 因订阅停机被取消，确认结果按不确定态处理");
                                return;
                            }
                            result = ack => {
                                if let Err(e) = result {
                                    tracing::warn!(stream = %stream, id = %entry.id, err = %e, "XACK 失败");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    tracing::debug!(stream = %stream, "stream 订阅任务退出");
}

/// 业务作用：等待退避或订阅取消；返回 `true` 表示取消已触发。
async fn sleep_or_cancel(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

/// 业务作用：校验订阅断点必须是具体 Redis Stream entry ID，拒绝会在每轮重求“最新”的 `$`。
fn valid_stream_entry_id(value: &str) -> bool {
    let Some((milliseconds, sequence)) = value.split_once('-') else {
        return false;
    };
    !milliseconds.is_empty()
        && !sequence.is_empty()
        && milliseconds.bytes().all(|byte| byte.is_ascii_digit())
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && milliseconds.parse::<u64>().is_ok()
        && sequence.parse::<u64>().is_ok()
}

// 跑一个 handler:catch_unwind + 可选超时;成功=true。
///
/// # 参数
/// 业务作用：- `handler`: 业务处理函数,在匹配事件或任务时被调用。
/// - `ev`: 当前投递给处理器的 stream 事件。
/// - `timeout_ms`: 超时时间毫秒数。
async fn dispatch_one(handler: &Handler, ev: StreamEvent, timeout_ms: u64) -> bool {
    // 把 handler(ev) 的**构造**也放进 guarded 闭包内:`on` 允许两段式闭包(同步 prologue + async block),
    // 若 prologue 同步 panic,必须被 catch_unwind 兜住——否则 panic 逃逸出 run_loop、订阅任务静默死掉(spec)。
    let guarded =
        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
            async move { handler(ev).await },
        ));
    let res = if timeout_ms > 0 {
        match tokio::time::timeout(Duration::from_millis(timeout_ms), guarded).await {
            Ok(r) => r,
            Err(_) => {
                tracing::warn!("stream handler 超时({timeout_ms}ms),按失败处理");
                return false;
            }
        }
    } else {
        guarded.await
    };
    match res {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::warn!(err = %e, "stream handler 返回 Err");
            false
        }
        Err(_) => {
            tracing::error!("stream handler panic");
            false
        }
    }
}

// ───────────────────────── 内部 helper ─────────────────────────

// BLOCK 上限:>= 连接级 response_timeout 会被误杀 → 收敛到其一半。
///
/// # 参数
/// 业务作用：- `block_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
/// - `resp_timeout_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
fn effective_block(block_ms: u64, resp_timeout_ms: u64) -> u64 {
    if resp_timeout_ms > 0 && block_ms >= resp_timeout_ms {
        (resp_timeout_ms / 2).max(1)
    } else {
        block_ms
    }
}

// 编码一条 `{topic:stream, data}` 信封为 JSON 字节(passthrough/event 省略)。
///
/// # 参数
/// 业务作用：- `stream`: 将写入信封 `topic` 字段的 Redis Stream key。
/// - `data`: 待写入信封 `data` 字段的 JSON 业务值。
fn encode_envelope(stream: &str, data: &JsonValue) -> Result<Vec<u8>> {
    let env = StreamEnvelope {
        topic: stream.to_string(),
        data,
        passthrough: None,
        event: None,
    };
    serde_json::to_vec(&env)
        .map_err(|e| NasaRedisError::Config(format!("envelope 序列化失败: {e}")))
}

/// 业务作用：stream 发布的**共享编码**:校验 stream/event 非空 + message 序列化(JSON null 拒)+ 编码
/// `{topic,data}` 信封 → body 字节。`RedisClient::publish` 与 `PipelineSession::publish` 共用此路径,
/// 从构造上保证两者逐字节一致(避免各自 copy 一份校验/序列化逻辑而漂移)。
pub(crate) fn stream_publish_body<T: Serialize>(
    stream: &str,
    event: &str,
    message: &T,
) -> Result<Vec<u8>> {
    if stream.is_empty() || event.is_empty() {
        return Err(NasaRedisError::Config(
            "publish: stream / event 不能为空".into(),
        ));
    }
    let data = serde_json::to_value(message)
        .map_err(|e| NasaRedisError::Config(format!("publish 序列化失败: {e}")))?;
    if data.is_null() {
        return Err(NasaRedisError::Config(
            "publish: message 序列化为 JSON null".into(),
        ));
    }
    encode_envelope(stream, &data)
}

// deserializeUnwrap:value 是 object 且 topic==stream → 取 data + passthrough;否则整体当裸 payload。
///
/// # 参数
/// 业务作用：- `raw`: 待解析的原始字符串、字节或配置值。
/// - `stream`: 当前消费的 Redis Stream key,用于判断信封 `topic` 是否匹配。
fn unwrap_envelope(
    raw: &[u8],
    stream: &str,
) -> Result<(JsonValue, Option<serde_json::Map<String, JsonValue>>)> {
    let v: JsonValue = serde_json::from_slice(raw)
        .map_err(|e| NasaRedisError::Config(format!("JSON 解析失败: {e}")))?;
    if let Some(obj) = v.as_object() {
        if obj.get("topic").and_then(|t| t.as_str()) == Some(stream) {
            let data = obj.get("data").cloned().unwrap_or(JsonValue::Null);
            let passthrough = obj.get("passthrough").and_then(|p| p.as_object().cloned());
            return Ok((data, passthrough));
        }
    }
    // 裸 xAdd 写入(非框架 wrap)→ 整体当业务对象。
    Ok((v, None))
}

// 构建 `XREAD COUNT n [BLOCK b] STREAMS <stream> <cursor>`。
///
/// # 参数
/// 业务作用：- `stream`: XREAD 的目标 Redis Stream key。
/// - `cursor`: stream 或分页查询继续读取的位置。
/// - `count`: Redis 命令、分页或批处理使用的数量上限。
/// - `block_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
fn build_xread_cmd(stream: &str, cursor: &str, count: usize, block_ms: Option<u64>) -> redis::Cmd {
    let mut cmd = redis::cmd("XREAD");
    cmd.arg("COUNT").arg(count);
    if let Some(b) = block_ms {
        cmd.arg("BLOCK").arg(b);
    }
    cmd.arg("STREAMS").arg(stream).arg(cursor);
    cmd
}

// 构建 `XREADGROUP GROUP g c COUNT n [BLOCK b] STREAMS <stream> <cursor>`。
///
/// # 参数
/// 业务作用：- `stream`: XREADGROUP 的目标 Redis Stream key。
/// - `group`: 消费组、服务分组或任务分组名称。
/// - `consumer`: Redis Stream consumer 名称。
/// - `cursor`: stream 或分页查询继续读取的位置。
/// - `count`: Redis 命令、分页或批处理使用的数量上限。
/// - `block_ms`: 毫秒时间参数,用于控制超时、延迟或调度窗口。
fn build_xreadgroup_cmd(
    stream: &str,
    group: &str,
    consumer: &str,
    cursor: &str,
    count: usize,
    block_ms: Option<u64>,
) -> redis::Cmd {
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP").arg(group).arg(consumer);
    cmd.arg("COUNT").arg(count);
    if let Some(b) = block_ms {
        cmd.arg("BLOCK").arg(b);
    }
    cmd.arg("STREAMS").arg(stream).arg(cursor);
    cmd
}

// Broadcast 起点解析成具体 cursor(Now = XREVRANGE 取最后一条已有 entry id,空/流不存在则 0-0)。
///
/// # 参数
/// 业务作用：- `client`: 底层客户端或连接句柄。
/// - `stream`: 需要计算订阅起点的 Redis Stream key。
/// - `start`: 起始位置或范围下界。
async fn resolve_broadcast_cursor(
    client: &RedisClient,
    stream: &str,
    start: &StreamStart,
) -> Result<String> {
    match start {
        StreamStart::After(id) => Ok(id.clone()),
        StreamStart::Beginning => Ok("0-0".to_string()),
        StreamStart::Now => {
            // 用 XREVRANGE + - COUNT 1 取最后一条已有 entry 的 id:缺失/空流返回空数组、**不报错** → 回落 0-0。
            // 比 XINFO 稳:XINFO 对缺失 key 报错、得靠错误文本区分"不存在 vs 真故障";真故障(网络/WRONGTYPE)
            // 经 `?` 上抛 → start() 失败让调用方重试,**不会**误从 0-0 灌整条高频流历史(如 match)。
            let mut cmd = redis::cmd("XREVRANGE");
            cmd.arg(stream).arg("+").arg("-").arg("COUNT").arg(1);
            let v: Value = client.execute_raw(&cmd).await?;
            Ok(first_entry_id(&v)?.unwrap_or_else(|| "0-0".into()))
        }
    }
}

// 幂等建消费组(BUSYGROUP 忽略)。
///
/// # 参数
/// 业务作用：- `client`: 底层客户端或连接句柄。
/// - `stream`: 需要创建消费组的 Redis Stream key。
/// - `group`: 消费组、服务分组或任务分组名称。
/// - `start`: 起始位置或范围下界。
async fn ensure_group(
    client: &RedisClient,
    stream: &str,
    group: &str,
    start: &StreamGroupStart,
) -> Result<()> {
    let mkid = match start {
        StreamGroupStart::New => "$",
        StreamGroupStart::History => "0",
    };
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg(mkid)
        .arg("MKSTREAM");
    match client.execute_raw::<Value>(&cmd).await {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
        Err(e) => Err(e),
    }
}

// ───────────────────────── XREAD 响应解析(RESP2 Array / RESP3 Map / Nil)─────────────────────────

// 把 XREAD/XREADGROUP 的原始 Value 解析成 entries(单/多 stream 通用;lossless)。
// **fail-closed**:除 Nil / 空数组 / 空 Map(= 无消息)外,任何不符合 XREAD 合同的形态返回 Err,
// 不静默当"无消息"——否则协议漂移 / redis crate 解码变化 / 服务端异常会被伪装成空轮询;
// Group 模式下更会掩盖 XREADGROUP 已投递但本地解析跳过的 entry(留在 PEL 且诊断被延迟)。
///
/// # 参数
/// 业务作用：- `v`: 待转换的值。
fn parse_xread_reply(v: Value) -> Result<Vec<StreamEntry>> {
    let mut out = Vec::new();
    let stream_pairs: Vec<(Value, Value)> = match v {
        Value::Nil => return Ok(out),
        // RESP3:{stream => entries}(空 Map = 无消息)
        Value::Map(m) => m,
        // RESP2:[[stream, entries], ...](空数组 = 无消息)
        Value::Array(arr) | Value::Set(arr) => {
            let mut pairs = Vec::with_capacity(arr.len());
            for item in arr {
                match item {
                    Value::Array(mut kv) if kv.len() == 2 => {
                        let entries = kv.pop().unwrap();
                        let stream = kv.pop().unwrap();
                        pairs.push((stream, entries));
                    }
                    other => return Err(shape_err("stream item 应为 [stream, entries]", &other)),
                }
            }
            pairs
        }
        other => return Err(shape_err("顶层应为 Array/Map/Nil", &other)),
    };

    for (stream_v, entries_v) in stream_pairs {
        let stream = xread_string(&stream_v)?;
        let entries = match entries_v {
            Value::Array(e) | Value::Set(e) => e,
            other => return Err(shape_err("entries 应为 Array", &other)),
        };
        for entry in entries {
            let mut idfields = match entry {
                Value::Array(a) if a.len() == 2 => a,
                other => return Err(shape_err("entry 应为 [id, fields]", &other)),
            };
            let fields_v = idfields.pop().unwrap();
            let id_v = idfields.pop().unwrap();
            let id = xread_string(&id_v)?;
            let fields = parse_fields(fields_v)?;
            out.push(StreamEntry {
                stream: stream.clone(),
                id,
                fields,
            });
        }
    }
    Ok(out)
}

// 解析 entry 的 field 段:RESP2 扁平 [f,v,f,v] 或 RESP3 Map(均保序、保重复)。field/value 须成对(奇数 → Err)。
///
/// # 参数
/// 业务作用：- `v`: 待转换的值。
fn parse_fields(v: Value) -> Result<Vec<StreamField>> {
    let mut fields = Vec::new();
    match v {
        Value::Array(fv) | Value::Set(fv) => {
            if fv.len() % 2 != 0 {
                return Err(NasaRedisError::Config(format!(
                    "XREAD 响应形态异常:entry fields 数量为奇数({})",
                    fv.len()
                )));
            }
            let mut it = fv.into_iter();
            while let (Some(f), Some(val)) = (it.next(), it.next()) {
                fields.push(StreamField {
                    event: xread_string(&f)?,
                    raw: xread_bytes(&val)?,
                });
            }
        }
        Value::Map(m) => {
            for (f, val) in m {
                fields.push(StreamField {
                    event: xread_string(&f)?,
                    raw: xread_bytes(&val)?,
                });
            }
        }
        other => return Err(shape_err("entry fields 应为 Array/Map", &other)),
    }
    Ok(fields)
}

// XREAD 响应形态异常错误(fail-closed;不静默吞掉,便于诊断协议漂移 / 解码变化)。
///
/// # 参数
/// 业务作用：- `what`: 错误或超时日志中标识当前操作的名称。
/// - `got`: 实际读取到的 Redis 返回值。
fn shape_err(what: &str, got: &Value) -> NasaRedisError {
    NasaRedisError::Config(format!("XREAD 响应形态异常:{what}(实得 {got:?})"))
}

// XREVRANGE 回复取第一条(=最新)entry 的 id。空数组/Nil → Ok(None)(= 空/不存在流,回落 0-0);
// 非法形态(顶层非 Array/Nil、entry 非数组、id 非字符串)→ Err(视为协议失败上抛,**不**当空流回落)。
///
/// # 参数
/// 业务作用：- `v`: 待转换的值。
fn first_entry_id(v: &Value) -> Result<Option<String>> {
    let arr = match v {
        Value::Array(a) | Value::Set(a) => a,
        Value::Nil => return Ok(None),
        other => return Err(shape_err("XREVRANGE 顶层应为 Array/Nil", other)),
    };
    let Some(first) = arr.first() else {
        return Ok(None);
    };
    let idfields = match first {
        Value::Array(a) | Value::Set(a) if !a.is_empty() => a,
        other => return Err(shape_err("XREVRANGE entry 应为 [id, fields]", other)),
    };
    Ok(Some(xread_string(&idfields[0])?))
}

// XREAD 里 stream 名 / entry id / field 名严格取字符串:恒为 bulk/simple string;
// Int/Nil/Map/Array 等 → Err(不把异常静默转成 "123" 继续)。
///
/// # 参数
/// 业务作用：- `v`: 待转换的值。
fn xread_string(v: &Value) -> Result<String> {
    match v {
        Value::BulkString(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Value::SimpleString(s) => Ok(s.clone()),
        other => Err(shape_err("应为字符串(bulk/simple)", other)),
    }
}

// XREAD 里 field value 严格取字节:恒为 bulk/simple string;其它形态(Int/Nil/Map/Array)→ Err
// (不静默变空 payload——否则 Group+Auto 下异常 value 会被当空并 ACK,诊断更差)。
///
/// # 参数
/// 业务作用：- `v`: 待转换的值。
fn xread_bytes(v: &Value) -> Result<Vec<u8>> {
    match v {
        Value::BulkString(b) => Ok(b.clone()),
        Value::SimpleString(s) => Ok(s.clone().into_bytes()),
        other => Err(shape_err("field value 应为字符串/字节", other)),
    }
}
