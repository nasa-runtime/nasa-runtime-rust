// ============================================================================
// src/stream.rs —— event-field 线格式的普通 Stream 发布与订阅。
//
// wire = **event-field**:`XADD <stream> * <event> <Envelope JSON>`,event 作 entry 的 field 名,
// value 是 `{"topic":<stream>,"data":<message>[,"passthrough":..]}` 信封。
// 与 proxy/partition 的 data-field wire(field 恒 "data")**两套,不互通**——本模块专供既有事件流协议互通。
//
// 消费两种模式:
//   · Broadcast(XREAD):每个订阅者独立游标,所有节点都收到每条(如 init:coin 每节点各建引擎)。
//   · Group(XREADGROUP):同组竞争领取，每次分配给一个消费者并按策略 XACK；外部 PEL 恢复可能造成重复处理。
//
// 连接与游标边界：
//   1. 阻塞读走【专用连接】(client.dedicated_conn)，避免 XREAD BLOCK 阻塞共享连接上的并发发布。
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
use tokio_util::sync::CancellationToken;

use crate::client::{Conn, RedisClient};
use crate::config::{MAX_REDIS_NAME_BYTES, MAX_REDIS_RUNTIME_DURATION_MS, MAX_STREAM_BATCH_SIZE};
use crate::error::{NasaRedisError, Result};

/// `publish(stream, message)` 的默认事件名。
pub const STREAM_EVENT: &str = "msg";

// ───────────────────────── 低层类型 ─────────────────────────

/// 一条 entry 内的事件字段，保留次序、重复事件名及值的原始字节；事件名必须是合法 UTF-8。
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
    /// 该 entry 的全部字段；仅消费组重读历史 PEL 的已删除条目为空，此时仍保留待确认 ID。
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
    /// 业务作用：将一个事件的业务消息转换为可与其它事件共同发布的 JSON 值。
    ///
    /// 参数说明：
    /// - `event`：写入 entry 的事件字段名，订阅端据此选择处理器。
    /// - `message`：需要转换为 JSON 值的业务消息。
    ///
    /// 返回：序列化成功时保存事件名和 JSON 值，失败或捕获到的展开返回不含业务错误正文的配置错误；空事件及 null 值由发布入口拒绝，此处不发送 Redis 命令。
    pub fn new<T: Serialize>(event: impl Into<String>, message: &T) -> Result<Self> {
        let data = serialize_stream_value(message)?;
        Ok(Self {
            event: event.into(),
            data,
        })
    }
}

/// 业务作用：隔离业务消息转换为 JSON 值时的序列化失败，避免错误摘要携带业务正文。
///
/// 参数说明：
/// - `message`：调用方提供的可序列化业务消息，不在此处校验目标事件或拒绝 null。
///
/// 返回：成功得到 JSON 值；序列化返回错误时只保留类别与行列，捕获到展开时返回固定配置错误，不发送 Redis 命令。
fn serialize_stream_value<T: Serialize>(message: &T) -> Result<JsonValue> {
    // Serialize 是用户代码；须在进入命令队列前完成，并将失败限制为不包含业务正文的本地结果。
    let encoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serde_json::to_value(message)
    }))
    .map_err(|_| NasaRedisError::Config("Stream 消息序列化展开".into()))?;
    encoded.map_err(|error| {
        NasaRedisError::Config(format!(
            "Stream 消息序列化失败: {:?} at {}:{}",
            error.classify(),
            error.line(),
            error.column()
        ))
    })
}

// ───────────────────────── 订阅配置类型 ─────────────────────────

/// Broadcast 起点。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStart {
    /// 从"当前时刻"起(具体 id:XREVRANGE 取最后一条已有 entry id,空/流不存在则 0-0),只收之后的新事件。
    Now,
    /// 从指定具体 entry id 之后；必须是 `<milliseconds>-<sequence>`，不接受会重求最新位置的 `$`。
    After(String),
    /// 从流头(全历史)。
    Beginning,
}

/// 消费组建组起点(仅首次 XGROUP CREATE 用)。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamGroupStart {
    /// 只收建组后的新消息(`$`)。
    New,
    /// 从流头开始消费历史(`0-0`)。
    History,
}

/// 消费模式。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamAckPolicy {
    /// 默认 auto-ack:entry 完成一次分发尝试后即 ACK(handler 失败也 ACK,不滞留 PEL)。
    Auto,
    /// 更严:该 entry 中所有已注册 event handler 都成功才 ACK,任一失败不 ACK(留在 PEL)。
    ///
    /// ⚠ 语义边界:本轻量 subscribe 循环只读 `>`(新消息)、**不自动重投 PEL**——失败留在 PEL 的 entry
    ///   不会被本消费者自动重试，需外部恢复：XAUTOCLAIM 转移、通过低层 API 重读本消费者 PEL，或改用
    ///   **proxy 路径**(消费组 + 内置 XAUTOCLAIM reclaim,自带失败重投)。若你要"失败自动重投",请用 proxy
    ///   而非本 subscribe;OnSuccess 在此仅提供"失败不丢(留痕 PEL)+ 可外部重放"的保证。
    OnSuccess,
}

/// 订阅运行参数。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
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
    /// 业务作用：提供从当前尾部开始的普通广播订阅参数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：100 条批量、500ms 阻塞、200ms 退避、30 秒处理器超时及 Auto 确认策略；切换到组模式后才使用确认策略。
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
    /// 事件所属的 Redis Stream key。
    pub stream: String,
    /// 事件所属条目的 Redis Stream 身份。
    pub id: String,
    /// entry 中的业务事件名。
    pub event: String,
    /// 事件原始 payload 字节。
    pub raw: Vec<u8>,
}

/// 分发给 `on_typed` handler 的解包后事件(已按 deserializeUnwrap 取 data 并反序列化为 T)。
#[derive(Debug, Clone)]
pub struct StreamTypedEvent<T> {
    /// 类型化事件所属的 Redis Stream key。
    pub stream: String,
    /// 类型化事件所属条目的 Redis Stream 身份。
    pub id: String,
    /// entry 中的业务事件名。
    pub event: String,
    /// 已反序列化的业务数据。
    pub data: T,
    /// 信封中对象类型的 passthrough 字段；裸值、缺席或非对象透传得到 None。
    pub passthrough: Option<serde_json::Map<String, JsonValue>>,
}

// 类型擦除后的 handler:输入原始 StreamEvent,输出 Result<(), 错误说明>。
type BoxFut = Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>;
type Handler = Arc<dyn Fn(StreamEvent) -> BoxFut + Send + Sync>;

// ───────────────────────── 发布 / 低层读 API(RedisClient) ─────────────────────────

impl RedisClient {
    /// 业务作用：以事件名作为字段，将业务消息包装为 Stream 信封后追加一条 entry。
    ///
    /// 参数说明：
    /// - `stream`：目标 Redis Stream key，同时写入信封 topic。
    /// - `event`：entry 字段名，订阅端据此选择处理器。
    /// - `message`：需序列化并写入信封 data 的业务消息。
    ///
    /// 返回：返回底层 XADD 的身份或错误；空名称、编码失败或 JSON null 在发送前拒绝，发送后的错误不证明追加未发生。
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

    /// 业务作用：用默认 msg 事件名发布一条普通 Stream 信封。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream key，同时作为信封 topic。
    /// - `message`：写入信封 data 的业务消息。
    ///
    /// 返回：沿用 publish 的编码、名称校验和 XADD 结果；发送后的错误不证明消息未追加。
    pub async fn publish_default<T: Serialize>(&self, stream: &str, message: &T) -> Result<String> {
        self.publish(stream, STREAM_EVENT, message).await
    }

    /// 业务作用：将多个事件的独立信封合并到同一次 XADD，保留同一 entry 内的字段顺序与重复事件名。
    ///
    /// 参数说明：
    /// - `stream`：全部信封共用的目标 Stream key。
    /// - `events`：按输入顺序写入同一 entry 的事件名与 JSON 值。
    ///
    /// 返回：全部字段编码成功后发送一次 XADD，返回底层身份或错误；空来源、空列表、空事件或 null 值在发送前拒绝，不进行部分发布。
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

    /// 业务作用：对单个 Stream 执行一次广播读取，阻塞调用使用新建的专用连接以免占用共享命令通道。
    ///
    /// 参数说明：
    /// - `stream`：读取的 Redis Stream key，不能为空。
    /// - `cursor`：原样传给 XREAD 的起始游标，后续推进由调用方管理。
    /// - `count`：请求的条数上限，范围 1..=10000。
    /// - `block_ms`：None 使用共享连接非阻塞读取；Some 为正毫秒数，禁止无限阻塞的零值。
    ///
    /// 返回：来源匹配、身份严格递增且条数不超过请求的完整条目，合法空回复得到空集合；参数、连接、命令或解析错误向上传播，不保存游标。
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
        parse_xread_reply(v, stream, count, cursor, false)
    }

    /// 业务作用：为已有消费组执行单个 Stream 的一次领取，阻塞读取使用专用连接，不自动建组或确认。
    ///
    /// 参数说明：
    /// - `stream`：目标 Stream key，不能为空。
    /// - `group`：已存在的非空消费组名称。
    /// - `consumer`：本次领取所属的非空消费者名称。
    /// - `cursor`：原样传给 XREADGROUP 的游标，通常以 > 读取新消息。
    /// - `count`：请求的条数上限，范围 1..=10000。
    /// - `block_ms`：None 为共享连接非阻塞读取；Some 为正毫秒数，零值在发送前拒绝。
    ///
    /// 返回：来源匹配、身份递增且数量有界的条目或错误；历史 PEL 中已删除的条目保留 ID 且 fields 为空。
    /// 失败或等待取消不证明远端没有分配 PEL，确认由调用方另行执行。
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
        parse_xread_reply(v, stream, count, cursor, cursor != ">")
    }

    /// 业务作用：为指定来源构造普通 Stream 订阅目录，默认从当前尾部开始广播。
    ///
    /// 参数说明：
    /// - `stream`：后续读取的 Redis Stream key，启动时校验名称。
    ///
    /// 返回：持有当前客户端、默认配置和空处理器目录的构建器；此处不建组、连接或读取，可继续配置后启动。
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
    /// 准备连接、建组与确认均使用该固定来源。
    client: Arc<RedisClient>,
    /// 本订阅唯一读取来源，同时参与信封解包匹配。
    stream: String,
    /// 启动时校验并冻结的读取与确认策略。
    cfg: StreamSubscribeCfg,
    /// 事件名到处理器的目录，同名登记覆盖。
    handlers: HashMap<String, Handler>,
}

impl StreamSubscriber {
    /// 业务作用：为尚未启动的订阅整体替换读取模式、容量、等待与确认策略。
    ///
    /// 参数说明：
    /// - `cfg`：替换原配置的完整订阅参数。
    ///
    /// 返回：保留来源和处理器目录的构建器；合法性在启动时校验，此处不执行远端操作。
    pub fn with_cfg(mut self, cfg: StreamSubscribeCfg) -> Self {
        self.cfg = cfg;
        self
    }

    /// 业务作用：将订阅切换为独立游标广播，覆盖此前消费组模式。
    ///
    /// 参数说明：
    /// - `start`：启动时解析的广播起点；Now 在准备阶段读取当时尾部身份。
    ///
    /// 返回：采用新广播起点的构建器，其余配置与处理器保持；不在此处读取游标或连接 Redis。
    pub fn broadcast_from(mut self, start: StreamStart) -> Self {
        self.cfg.mode = StreamMode::Broadcast {
            start,
        };
        self
    }

    /// 业务作用：将订阅切换为共享消费组领取，默认只在首次建组时从新消息开始。
    ///
    /// 参数说明：
    /// - `group`：同组竞争领取使用的消费组名称。
    /// - `consumer`：领取记录写入 PEL 时使用的消费者名称。
    ///
    /// 返回：采用 Group/New 模式的构建器，覆盖先前模式及起点；名称在启动时校验，已存在组的历史起点不因此改变。
    pub fn group(mut self, group: impl Into<String>, consumer: impl Into<String>) -> Self {
        self.cfg.mode = StreamMode::Group {
            group: group.into(),
            consumer: consumer.into(),
            start: StreamGroupStart::New,
        };
        self
    }

    /// 业务作用：在已经选择消费组模式时调整首次建组起点。
    ///
    /// 参数说明：
    /// - `start`：新建组从历史或新消息开始的策略。
    ///
    /// 返回：Group 模式保留组名与消费者并替换起点，Broadcast 模式原样返回；不立即建组，也不重置已有组游标。
    pub fn group_from(mut self, start: StreamGroupStart) -> Self {
        if let StreamMode::Group {
            group,
            consumer,
            ..
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

    /// 业务作用：选择组消息完成一次分发尝试后是否根据处理结果确认。
    ///
    /// 参数说明：
    /// - `policy`：Auto 尝试后确认，OnSuccess 要求所有已注册字段处理成功。
    ///
    /// 返回：更新确认策略的构建器；广播不使用此配置，未注册事件不阻止确认，保留 PEL 不提供本订阅自动重投。
    pub fn ack_policy(mut self, policy: StreamAckPolicy) -> Self {
        self.cfg.ack_policy = policy;
        self
    }

    /// 业务作用：登记按事件字段名分发的原始消息处理器，不进行信封解包。
    ///
    /// 参数说明：
    /// - `event`：匹配 entry 字段名的处理器键。
    /// - `handler`：接收来源、身份、事件名和原始字节的异步回调，成功返回 Ok，业务失败返回 Err。
    ///
    /// 返回：登记后的构建器；同名后登记覆盖先前处理器，此处不执行业务，构造和轮询异常由运行期分发入口处理。
    pub fn on<F, Fut>(mut self, event: impl Into<String>, handler: F) -> Self
    where
        F: Fn(StreamEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        let h: Handler = Arc::new(move |ev| Box::pin(handler(ev)));
        self.handlers.insert(event.into(), h);
        self
    }

    /// 业务作用：登记类型化事件处理器，在运行期按当前 Stream 匹配信封并将业务值解码后交付。
    ///
    /// 参数说明：
    /// - `event`：用于选择处理器的 entry 字段名。
    /// - `handler`：接收类型化数据及来源元信息的异步回调。
    ///
    /// 返回：登记后的构建器，同名处理器被覆盖；解包或类型解码失败时不调用业务回调，运行循环依确认策略处理失败，不在此处消费消息。
    /// 类型解码错误仅保留类别与位置，不把原值或自定义反序列化错误正文交给运行期告警。
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
                // 自定义 Deserialize 可携带完整业务正文；失败只发布有界的类别与位置证据。
                let data: T = serde_json::from_value(data_val).map_err(|e| {
                    format!(
                        "data 反序列化失败: category={:?}, line={}, column={}",
                        e.classify(),
                        e.line(),
                        e.column()
                    )
                })?;
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

    /// 业务作用：提供已登记事件目录大小，用于宿主在连接前校验计划容量。
    ///
    /// 参数说明：无。
    ///
    /// 返回：去重后的事件处理器数量。
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }

    /// 业务作用：完成专用连接和读取起点准备后，立即开放普通 Stream 消费。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回持有激活、关闭及退出证据的订阅句柄，准备错误直接传播；Drop 发起关闭，shutdown 等待任务退出，不等同于全部历史消息处理成功。
    pub async fn start(self) -> Result<StreamSubscription> {
        let subscription = self.start_suspended().await?;
        subscription.activate();
        Ok(subscription)
    }

    /// 业务作用：准备专用连接与读取起点，业务读取等待宿主显式激活。
    ///
    /// 参数说明：无。
    ///
    /// 返回：带激活门禁及独立退出 owner 的订阅；准备失败时不创建消费循环。
    pub async fn start_suspended(self) -> Result<StreamSubscription> {
        self.start_suspended_with_activation(CancellationToken::new())
            .await
    }

    /// 业务作用：准备消费任务并等待调用方统一开放多个领域入口。
    ///
    /// 参数说明：
    /// - `ready`：取消表示本代消费获准；共享许可时由宿主统一取消，不能逐项调用 activate。
    ///
    /// 返回：执行器接纳准备且配置、连接、起点及本地任务责任成立时返回订阅；缺少上下文、接纳失败或交付前失去责任返回错误。
    /// 消费与收尾任务使用准备时取得的执行器，许可之前不读取或调用业务，关闭优先于激活。
    /// 准备期可能已经建立消费组，后续错误或等待取消不回滚该远端副作用。
    pub async fn start_suspended_with_activation(
        self,
        ready: CancellationToken,
    ) -> Result<StreamSubscription> {
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
            group,
            consumer,
            ..
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
        // 进入连接与计时器前确认执行器能够接纳任务，已关闭上下文不能继续建立半完成订阅。
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| NasaRedisError::Config("Stream 启动需要有效的 Tokio 执行上下文".into()))?;
        runtime
            .spawn(async {})
            .await
            .map_err(|_| NasaRedisError::Config("Stream 启动执行器未接纳准备任务".into()))?;
        let cancel = CancellationToken::new();
        let child = cancel.child_token();

        // 专用连接使 BLOCK 不占共享 command lane。Cluster 也创建独立 ClusterConnection；
        // redis-rs 会按 XREAD/XREADGROUP 的 STREAMS 首个 key 计算 slot，并在拓扑变化时处理
        // MOVED/ASK。本 builder 只允许单 stream，因此不会产生跨 slot 阻塞读。
        let conn = self.client.dedicated_conn("stream subscribe").await?;

        // 启动阶段解析起点 / 建组(错误当场上抛)。
        let read_kind = match &self.cfg.mode {
            StreamMode::Broadcast {
                start,
            } => {
                let cursor = resolve_broadcast_cursor(&self.client, &self.stream, start).await?;
                ReadKind::Broadcast {
                    cursor,
                }
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

        let gate = ready.clone();
        let activity = Arc::new(crate::activity::Activity::default());
        let active = activity.task(crate::activity::TaskKind::Consumer);
        let progress = activity.clone();
        let handle = runtime.spawn(async move {
            let _active = active;
            // 准备成功不能提前读取或 ACK；启动回滚时取消优先于统一放行。
            tokio::select! {
                biased;
                _ = child.cancelled() => return,
                _ = gate.cancelled() => {},
            }
            run_loop(self, conn, read_kind, child, progress).await;
        });
        let abort = handle.abort_handle();
        let (done, outcome) = tokio::sync::watch::channel(StreamTaskState::Running);
        let done = activity.completion(done);
        let owner_cancel = cancel.clone();
        let owner_stop = cancel.clone().drop_guard();
        runtime.spawn(async move {
            let _stop = owner_stop;
            let result = handle.await;
            let state = if result.is_ok() && owner_cancel.is_cancelled() {
                StreamTaskState::Stopped
            } else {
                StreamTaskState::Failed
            };
            owner_cancel.cancel();
            done.send_replace(state);
        });
        let running = StreamSubscription {
            cancel,
            ready,
            abort,
            outcome,
            activity,
        };
        // 准备期间执行器可能已经关闭；交付前复验实际责任，不能把已被拒收的任务发布为就绪。
        if running.with_running(|| ()).is_none() {
            running.begin_shutdown();
            return Err(NasaRedisError::Config(
                "Stream 启动时未取得完整任务责任，已请求关闭".into(),
            ));
        }
        Ok(running)
    }
}

/// 普通 Stream 消费任务的退出证据，不改变消息的游标或 ACK 合同。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTaskState {
    /// 已准备或正在消费。
    Running,
    /// 收到关闭请求后正常退出。
    Stopped,
    /// 未预期退出、panic 或强制终止。
    Failed,
}

/// 订阅的激活与停止句柄，独立 owner 等待任务退出，取消等待不会丢失完成状态。
pub struct StreamSubscription {
    /// 接流门禁、活动任务与本地工作责任。
    activity: Arc<crate::activity::Activity>,
    /// 永久停止本代读取及业务等待的信号。
    cancel: CancellationToken,
    /// 取消表示宿主开放消费，不能覆盖已关闭状态。
    ready: CancellationToken,
    /// 宿主到期时提出任务终止请求，不能替代退出证据。
    abort: tokio::task::AbortHandle,
    /// 独立 owner 发布的任务退出状态，等待者可取消后重新读取。
    outcome: tokio::sync::watch::Receiver<StreamTaskState>,
}

impl StreamSubscription {
    /// 业务作用：在本地消费责任完整时完成宿主接流裁决，与任务退出串行化。
    ///
    /// 参数说明：
    /// - `publish`：在活动状态锁内执行的同步接流裁决，必须短小、不阻塞、不重入本订阅。
    ///
    /// 返回：未关闭且任务仍运行时返回 Some，否则不执行；不保证远端连接未发生尚未观察到的故障。
    pub fn with_running<T>(&self, publish: impl FnOnce() -> T) -> Option<T> {
        self.activity
            .with_tasks(1, 0, 0, || {
                if self.cancel.is_cancelled() || self.state() != StreamTaskState::Running {
                    None
                } else {
                    Some(publish())
                }
            })
            .flatten()
    }
    /// 业务作用：观察消费任务和本地处理责任，不额外查询业务 Stream 或 PEL。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有效读取与处理进展；被取消的未结束处理仍保留在未完成数量中。
    pub fn observation(&self) -> crate::RedisTaskObservation {
        self.activity.snapshot()
    }

    /// 业务作用：准备完成后开放本代业务读取。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未关闭时成功，关闭后不允许重新激活。
    pub fn activate(&self) -> bool {
        if self.cancel.is_cancelled() {
            return false;
        }
        self.ready.cancel();
        true
    }

    /// 业务作用：停止新的读取与 handler，未 ACK 的组消息保留 PEL。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已发出关闭请求，不等同于任务实际退出。
    pub fn begin_shutdown(&self) {
        self.activity.close();
        self.cancel.cancel();
    }

    /// 业务作用：在关闭预算耗尽时请求终止消费任务。
    ///
    /// 参数说明：无。
    ///
    /// 返回：请求已发出，仍应等待退出证据。
    pub fn abort(&self) {
        self.begin_shutdown();
        self.abort.abort();
    }

    /// 业务作用：提供当前消费任务状态用于宿主监督。
    ///
    /// 参数说明：无。
    ///
    /// 返回：本地任务状态；退出证据通道丢失时报告失败，不表示所有历史消息均已消费。
    pub fn state(&self) -> StreamTaskState {
        let disconnected = self.outcome.has_changed().is_err();
        let state = *self.outcome.borrow();
        if disconnected && state == StreamTaskState::Running {
            StreamTaskState::Failed
        } else {
            state
        }
    }

    /// 业务作用：等待独立 owner 取得实际退出结果，支持重复及取消后再次等待。
    ///
    /// 参数说明：无。
    ///
    /// 返回：正常停止或异常退出；证据通道消失时按失败处理。
    pub async fn wait_closed(&self) -> StreamTaskState {
        let mut outcome = self.outcome.clone();
        loop {
            let state = *outcome.borrow_and_update();
            if state != StreamTaskState::Running {
                return state;
            }
            if outcome.changed().await.is_err() {
                return StreamTaskState::Failed;
            }
        }
    }

    /// 业务作用：关闭消费并等待任务实际退出。
    ///
    /// 参数说明：无。
    ///
    /// 返回：关闭完成，异常退出记录固定原因；未确认 ACK 不自动补偿。
    pub async fn shutdown(self) {
        self.begin_shutdown();
        if self.wait_closed().await == StreamTaskState::Failed {
            tracing::warn!("Stream 消费任务异常退出");
        }
    }
}

impl Drop for StreamSubscription {
    /// 业务作用：释放业务句柄时停止准入，独立 owner 保留任务等待责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：协作取消；纯 CPU 且不 yield 的 handler 仍须由业务提供执行边界。
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

// ───────────────────────── 后台读循环 ─────────────────────────

/// 已准备的读取状态；广播使用固定游标，组模式持续读新消息且不自动回收 PEL。
enum ReadKind {
    /// 每轮按完成分发的身份推进独立游标。
    Broadcast {
        /// 下一轮读取排除该身份及更早记录。
        cursor: String,
    },
    /// 共享组中每轮领取新消息的固定身份。
    Group {
        /// 记录本订阅待确认消息的消费组。
        group: String,
        /// 本次领取使用的消费者名称。
        consumer: String,
    },
}

/// 业务作用：读取普通 Stream，按 field 执行业务并按原策略推进游标或确认消费。
///
/// 参数说明：
/// - `subscriber`：固定来源、Stream、配置与事件处理器目录。
/// - `conn`：独立读连接，阻塞读取不占用共享命令通道。
/// - `read_kind`：广播游标或消费组身份，决定读命令与确认方式。
/// - `cancel`：停止读取、重连及处理等待的永久关闭信号。
/// - `activity`：记录读取进展、已接纳工作与本地结束次数。
///
/// 返回：关闭后退出；已登记但未处理结束的工作保留未完成，取消中的业务或 ACK 不推断远端结局。
/// 广播完成一次尝试即推进游标；组模式按策略确认，未注册字段不阻止确认，本循环不重读 PEL。
/// 尚未取得回复的远端领取不计入本地条数，零未完成不能证明 PEL 为空；读取、处理和确认等待均可被关闭终止。
async fn run_loop(
    subscriber: StreamSubscriber,
    mut conn: Conn,
    mut read_kind: ReadKind,
    cancel: CancellationToken,
    activity: Arc<crate::activity::Activity>,
) {
    let StreamSubscriber {
        client,
        stream,
        cfg,
        handlers,
    } = subscriber;
    // BLOCK 不能 >= 连接级 response_timeout(否则会被连接超时误杀);默认 30s >> 500ms,仅防误配。
    let resp_timeout = client.config().command.response_timeout_ms;
    let block_ms = effective_block(cfg.block_ms, resp_timeout);
    let idle = Duration::from_millis(cfg.idle_sleep_ms.max(1));

    loop {
        // 关闭后不再开启新领取；已有远端副作用不因本地退出自动撤销。
        if cancel.is_cancelled() {
            break;
        }
        let cmd = match &read_kind {
            ReadKind::Broadcast {
                cursor,
            } => build_xread_cmd(
                &stream,
                cursor,
                cfg.batch_size,
                (block_ms > 0).then_some(block_ms),
            ),
            ReadKind::Group {
                group,
                consumer,
            } => build_xreadgroup_cmd(
                &stream,
                group,
                consumer,
                ">",
                cfg.batch_size,
                (block_ms > 0).then_some(block_ms),
            ),
        };

        // 已可观察的关闭优先于开启读取；中断等待不撤销远端领取，也不伪造未知回复的条数。
        let v: Value = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            r = cmd.query_async::<Value>(&mut conn) => match r {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(stream = %stream, err = %e, "stream 订阅读失败,退避后重建连接");
                    if sleep_or_cancel(&cancel, idle).await {
                        break;
                    }
                    // 同一读取来源恢复连接，不重置广播游标；关闭时不再等待重连或开放下一轮。
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

        // 在登记责任和调用业务前核对本轮查询；错误来源或身份不能取得本 Stream 的确认权限。
        let cursor = match &read_kind {
            ReadKind::Broadcast {
                cursor,
            } => cursor.as_str(),
            ReadKind::Group {
                ..
            } => ">",
        };
        let entries = match parse_xread_reply(v, &stream, cfg.batch_size, cursor, false) {
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

        // 一次有效读取只能证明本地取得该批；后续取消保留尚未结束的数量，不把它视为业务成功。
        activity.finish(0, false);
        activity.accept(entries.len(), 0);
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
                // 关闭优先于新的回调或继续轮询；未完成字段使本条仍缺少可确认的处理结束证据。
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
                ReadKind::Broadcast {
                    cursor,
                } => {
                    // 广播无 ACK,只推进游标(handler 失败无重投;不丢事件的场景由调用方自备恢复)。
                    *cursor = entry.id.clone();
                }
                ReadKind::Group {
                    group,
                    ..
                } => {
                    let should_ack = match cfg.ack_policy {
                        StreamAckPolicy::Auto => true,
                        StreamAckPolicy::OnSuccess => all_ok,
                    };
                    if should_ack {
                        let ack_ids = [entry.id.as_str()];
                        let ack = client.x_ack(&stream, group, &ack_ids);
                        // 已发出的确认可能在取消后生效；停止等待不补造重投保证，也不记本条完成。
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
            activity.finish(1, false);
        }
    }
    tracing::debug!(stream = %stream, "stream 订阅任务退出");
}

/// 业务作用：在空轮或失败后退避，同时允许关闭请求立即结束等待。
///
/// 参数说明：
/// - `cancel`：当前订阅的永久关闭信号。
/// - `duration`：未取消时最多等待的退避时长。
///
/// 返回：取消优先时返回 true，计时完成返回 false；两者同时就绪时以关闭为准，不产生远端副作用。
async fn sleep_or_cancel(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

/// 业务作用：校验订阅断点必须是具体 Redis Stream entry ID，拒绝会在每轮重求“最新”的 `$`。
///
/// 参数说明：
/// - `value`：调用方指定的广播断点文本。
///
/// 返回：两个非空 ASCII 十进制 u64 分量以单个连字符分隔时为 true，包括 0-0；符号、空白、额外分隔符或越界均为 false，不检查远端是否存在该条目。
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

/// 业务作用：把完整 Stream 身份转为无精度损失的数值坐标，用于检查读取范围和顺序。
///
/// 参数说明：
/// - `value`：由两个 ASCII 十进制 u64 分量组成的完整身份文本。
///
/// 返回：合法时返回毫秒与序号，非法文本或越界返回 None；允许作为下界的 0-0，实际条目是否可为零由调用点裁决。
fn stream_id_parts(value: &str) -> Option<(u64, u64)> {
    if !valid_stream_entry_id(value) {
        return None;
    }
    let (milliseconds, sequence) = value.split_once('-')?;
    Some((milliseconds.parse().ok()?, sequence.parse().ok()?))
}

/// 业务作用：在把 XADD 回复发布为追加成功前核对完整身份与请求指定的坐标。
///
/// 参数说明：
/// - `reply`：本次 XADD 的未做通用类型转换的回复。
/// - `requested`：发送的身份选择器，可为 *、完整 ID、仅毫秒或毫秒后接 -*。
///
/// 返回：仅非零完整身份且与指定坐标一致时成功；错误类型、非法身份或坐标不符返回固定解析错误。
/// 不回显回复正文，不撤销已经发生的追加，也不授权自动重发。
pub(crate) fn check_stream_id_reply(reply: &Value, requested: &str) -> Result<()> {
    let invalid = || NasaRedisError::Parsing("XADD 回复不是与请求一致的有效条目身份".into());
    let text = match reply {
        Value::BulkString(bytes) => std::str::from_utf8(bytes).map_err(|_| invalid())?,
        Value::SimpleString(text) => text.as_str(),
        _ => return Err(invalid()),
    };
    let parts = stream_id_parts(text)
        .filter(|parts| *parts != (0, 0))
        .ok_or_else(invalid)?;
    let matches = if requested == "*" {
        true
    } else if let Some(expected) = stream_id_parts(requested) {
        expected == parts
    } else {
        let milliseconds = requested.strip_suffix("-*").unwrap_or(requested);
        !milliseconds.is_empty()
            && milliseconds.bytes().all(|byte| byte.is_ascii_digit())
            && milliseconds.parse::<u64>().ok() == Some(parts.0)
            && (requested.ends_with("-*") || parts.1 == 0)
    };
    // 明确指定的毫秒或完整身份不能被其它记录的成功回包替代。
    if !matches {
        return Err(invalid());
    }
    Ok(())
}

/// 业务作用：核对 XACK 的确认数量证据，防止通用数值转换把畸形回包解释为已解除消费责任。
///
/// 参数说明：
/// - `reply`：本次 XACK 的原始回复。
/// - `requested`：实际发送的 ID 数量，包含重复项但不包含已被调用方过滤的项。
///
/// 返回：仅非负 Redis 整数且不超过发送数量时成功；其它回复返回固定解析错误。
/// 合法计数不指明具体哪些 ID 被确认，解析错误也不能证明远端确认未发生。
pub(crate) fn check_stream_ack_reply(reply: &Value, requested: usize) -> Result<()> {
    if matches!(reply, Value::Int(count) if usize::try_from(*count).is_ok_and(|count| count <= requested))
    {
        Ok(())
    } else {
        Err(NasaRedisError::Parsing(
            "XACK 回复不是有效的确认计数".into(),
        ))
    }
}

/// 业务作用：隔离单个事件处理器的构造与轮询展开，并按可选异步超时裁决本次处理结果。
///
/// 参数说明：
/// - `handler`：匹配当前事件的业务回调。
/// - `ev`：交付给回调的来源、身份和原始事件字节。
/// - `timeout_ms`：异步等待期限毫秒，零表示不设置期限。
///
/// 返回：仅正常返回 Ok 时为 true，业务错误、展开或超时为 false；不撤销外部副作用，也不能强制中断不让出执行权的同步回调。
async fn dispatch_one(handler: &Handler, ev: StreamEvent, timeout_ms: u64) -> bool {
    let guarded = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
        async move {
            // 回调构造本身也可能展开，必须在受保护 Future 内调用，不能使整条消费任务提前退出。
            handler(ev).await
        },
        // 展开隔离只裁决本次处理失败，不回滚已经发生的业务副作用。
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

/// 业务作用：当请求的 Redis 阻塞时长达到连接响应期限时缩短阻塞，为传输响应留出余量。
///
/// 参数说明：
/// - `block_ms`：配置的阻塞毫秒数，零由调用方解释为非阻塞。
/// - `resp_timeout_ms`：连接级响应期限毫秒，零表示没有该项约束。
///
/// 返回：达到非零响应期限时取其一半且至少一毫秒，否则保留原值；不保证网络、排队及服务器调度总耗时小于响应期限。
fn effective_block(block_ms: u64, resp_timeout_ms: u64) -> u64 {
    if resp_timeout_ms > 0 && block_ms >= resp_timeout_ms {
        (resp_timeout_ms / 2).max(1)
    } else {
        block_ms
    }
}

/// 业务作用：将目标来源与已构造的 JSON 业务值编码为普通 Stream 信封。
///
/// 参数说明：
/// - `stream`：写入信封 topic 的目标 Stream key。
/// - `data`：写入信封 data 的 JSON 值。
///
/// 返回：成功得到仅含 topic 与 data 的 JSON 字节，编码失败返回配置错误；不检验名称或 null，调用方在发布前负责这些门禁。
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

/// 业务作用：为直接发布与 Pipeline 发布共用名称、非空正文校验及信封编码规则。
///
/// 参数说明：
/// - `stream`：目标 Stream key，并写入信封 topic。
/// - `event`：物理字段名，只校验名称，不写入信封。
/// - `message`：需要序列化为 JSON data 的业务消息。
///
/// 返回：名称非空且序列化结果不是 null 时返回编码字节，否则返回配置错误；序列化错误摘要不含业务正文，捕获到的展开转为固定错误。
/// 不发送命令，相同 JSON 值沿用相同信封编码；空名称在调用用户序列化代码之前拒绝。
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
    let data = serialize_stream_value(message)?;
    if data.is_null() {
        return Err(NasaRedisError::Config(
            "publish: message 序列化为 JSON null".into(),
        ));
    }
    encode_envelope(stream, &data)
}

/// 业务作用：区分属于当前 Stream 的框架信封与直接写入的裸 JSON 业务值。
///
/// 参数说明：
/// - `raw`：一个事件字段的原始 JSON 字节。
/// - `stream`：当前订阅来源，用于精确匹配信封 topic。
///
/// 返回：匹配信封时返回 data 与对象类型的 passthrough，缺 data 取 null；其它合法 JSON 整体作为裸值且无透传，JSON 解析失败返回错误。
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

/// 业务作用：按单一来源构造广播读取命令，统一一次性读取与持续订阅的线路参数。
///
/// 参数说明：
/// - `stream`：XREAD 的唯一来源 key。
/// - `cursor`：传给 Redis 的读取起点，不在此处推进或规范化。
/// - `count`：请求的条数上限。
/// - `block_ms`：Some 时追加 BLOCK 毫秒参数，None 不使用阻塞。
///
/// 返回：尚未执行的命令；名称、容量与阻塞边界由调用入口校验，此处不取得读取或连接证据。
fn build_xread_cmd(stream: &str, cursor: &str, count: usize, block_ms: Option<u64>) -> redis::Cmd {
    let mut cmd = redis::cmd("XREAD");
    cmd.arg("COUNT").arg(count);
    if let Some(b) = block_ms {
        cmd.arg("BLOCK").arg(b);
    }
    cmd.arg("STREAMS").arg(stream).arg(cursor);
    cmd
}

/// 业务作用：构造带消费组与消费者身份的单来源领取命令。
///
/// 参数说明：
/// - `stream`：领取消息的唯一 Stream key。
/// - `group`：记录 PEL 的消费组名称。
/// - `consumer`：本次领取的消费者名称。
/// - `cursor`：原样传给 Redis 的领取游标，通常以 > 读取新消息。
/// - `count`：请求的条数上限。
/// - `block_ms`：Some 时追加 BLOCK 毫秒参数，None 不阻塞。
///
/// 返回：尚未执行的 XREADGROUP 命令；不建组、不确认，参数门禁由调用入口负责。
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

/// 业务作用：把广播起点固定为具体游标，避免后续每轮使用会重新计算尾部的 $ 而跳过间隔内消息。
///
/// 参数说明：
/// - `client`：读取尾部身份使用的固定 Redis 来源。
/// - `stream`：目标 Stream key。
/// - `start`：指定身份、全部历史或准备时刻尾部的起点策略。
///
/// 返回：After 保留已由启动入口校验的身份，Beginning 取 0-0，Now 读取尾部并在空流时取 0-0；读取或解析失败返回错误，不将故障当空流。
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

/// 业务作用：在组模式订阅前创建消费组及必要的空 Stream，已有组继续使用原游标。
///
/// 参数说明：
/// - `client`：执行建组命令的固定来源。
/// - `stream`：消费组所属的 Stream key。
/// - `group`：准备创建或复用的消费组名称。
/// - `start`：只影响首次建组的历史或新消息起点。
///
/// 返回：仅明确的 OK 回复或精确 BUSYGROUP 错误码允许继续；畸形成功回复返回解析错误，其它错误传播。
/// 不重置已有组，已建成的组不会随后续启动失败回滚；错误不能证明远端未创建组。
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
        Ok(Value::Okay) => Ok(()),
        Ok(Value::SimpleString(reply)) if reply == "OK" => Ok(()),
        Ok(Value::BulkString(reply)) if reply == b"OK" => Ok(()),
        // 已存在的明确错误码才允许沿用消费组，正文子串或相似码不能授权后续领取。
        Err(NasaRedisError::Redis(error)) if error.code() == Some("BUSYGROUP") => Ok(()),
        Ok(_) => Err(NasaRedisError::Parsing(
            "Stream 建组回复不是预期的 OK".into(),
        )),
        Err(e) => Err(e),
    }
}

// ───────────────────────── XREAD 响应解析(RESP2 Array / RESP3 Map / Nil)─────────────────────────

/// 业务作用：在交付前复验单来源读取的身份、数量、顺序与正文结构，避免异常回复取得业务路由或确认权限。
///
/// 参数说明：
/// - `v`：底层 Redis 命令返回的完整回复。
/// - `expected_stream`：本次读取命令中唯一的 Stream key。
/// - `count`：本次命令请求的条数上限。
/// - `cursor`：本次读取的排他下界或协议特殊游标。
/// - `history`：仅消费组以具体游标重读 PEL 时为 true，允许已删除条目以空 fields 表示。
///
/// 返回：Nil 或空集合得到空列表；只接受匹配来源、有界且严格递增的完整非零身份和合法字段。
/// 任一条目异常使整批失败，正文不进入错误摘要；不推进游标或 ACK，失败也不撤销远端已发生的领取。
fn parse_xread_reply(
    v: Value,
    expected_stream: &str,
    count: usize,
    cursor: &str,
    history: bool,
) -> Result<Vec<StreamEntry>> {
    let stream_pairs: Vec<(Value, Value)> = match v {
        Value::Nil => return Ok(Vec::new()),
        Value::Map(m) => m,
        Value::Array(arr) => {
            if arr.len() > 1 {
                return Err(NasaRedisError::Parsing("单来源读取返回多个 Stream".into()));
            }
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
    // 同一回复出现多个来源或重复来源时，不能将其中一部分确认为本次查询证据。
    if stream_pairs.len() > 1 {
        return Err(NasaRedisError::Parsing("单来源读取返回多个 Stream".into()));
    }
    let mut out = Vec::new();
    for (stream_v, entries_v) in stream_pairs {
        let stream = xread_string(&stream_v)?;
        if stream != expected_stream {
            return Err(NasaRedisError::Parsing(
                "Stream 回复来源与请求不一致".into(),
            ));
        }
        let entries = match entries_v {
            Value::Array(e) => e,
            other => return Err(shape_err("entries 应为 Array", &other)),
        };
        if entries.len() > count {
            return Err(NasaRedisError::Parsing("Stream 回复超过请求条数".into()));
        }
        // Redis 允许省略游标的序号，此时下界序号为零；特殊游标没有可预知的数值下界。
        let mut previous = stream_id_parts(cursor)
            .or_else(|| {
                if !cursor.is_empty() && cursor.bytes().all(|byte| byte.is_ascii_digit()) {
                    cursor
                        .parse::<u64>()
                        .ok()
                        .map(|milliseconds| (milliseconds, 0))
                } else {
                    None
                }
            })
            .unwrap_or((0, 0));
        for entry in entries {
            let mut idfields = match entry {
                Value::Array(a) if a.len() == 2 => a,
                other => return Err(shape_err("entry 应为 [id, fields]", &other)),
            };
            let fields_v = idfields.pop().unwrap();
            let id_v = idfields.pop().unwrap();
            let id = xread_string(&id_v)?;
            let parts = stream_id_parts(&id)
                .filter(|parts| *parts > previous)
                .ok_or_else(|| NasaRedisError::Parsing("Stream 身份非法或不满足读取顺序".into()))?;
            // 只有显式 PEL 历史读取允许正文已删除；正常广播和新消息领取不得把缺正文当完成。
            let fields = if history && matches!(fields_v, Value::Nil) {
                Vec::new()
            } else {
                parse_fields(fields_v)?
            };
            previous = parts;
            out.push(StreamEntry {
                stream: stream.clone(),
                id,
                fields,
            });
        }
    }
    Ok(out)
}

/// 业务作用：将一个条目的字段集合拆成事件名与原始值，保留输入次序和重复字段。
///
/// 参数说明：
/// - `v`：Redis 条目中的非空成对字段数组。
///
/// 返回：字段名按严格 UTF-8、值按原始字节转换后得到事件列表；空数组、奇数项、错误集合或字段类型返回错误，不跳过部分异常字段。
fn parse_fields(v: Value) -> Result<Vec<StreamField>> {
    let mut fields = Vec::new();
    match v {
        Value::Array(fv) => {
            // 正常条目至少有一个字段；无正文的历史 PEL 必须由读取模式单独识别。
            if fv.is_empty() || fv.len() % 2 != 0 {
                return Err(NasaRedisError::Parsing(
                    "Stream 字段必须为非空且成对的数组".into(),
                ));
            }
            let mut it = fv.into_iter();
            while let (Some(f), Some(val)) = (it.next(), it.next()) {
                fields.push(StreamField {
                    event: xread_string(&f)?,
                    raw: xread_bytes(&val)?,
                });
            }
        }
        other => return Err(shape_err("entry fields 应为 Array", &other)),
    }
    Ok(fields)
}

/// 业务作用：将无法解释的 Redis 回复结构转换为向上层传播的解析错误。
///
/// 参数说明：
/// - `what`：调用点期望的结构说明。
/// - `got`：未通过该结构检查的实际回复值。
///
/// 返回：只包含预期结构和实际类型的配置错误，不回显回复正文或嵌套集合，不产生空列表或远端操作。
fn shape_err(what: &str, got: &Value) -> NasaRedisError {
    let kind = match got {
        Value::Nil => "Nil",
        Value::Array(_) => "Array",
        Value::Map(_) => "Map",
        Value::Set(_) => "Set",
        Value::BulkString(_) => "BulkString",
        Value::SimpleString(_) => "SimpleString",
        Value::Int(_) => "Int",
        _ => "其它类型",
    };
    NasaRedisError::Config(format!("Stream 回复结构异常:{what}(实得 {kind})"))
}

/// 业务作用：从最新一条的完整范围回复提取广播起点，拒绝把畸形回复当作空流或任意断点。
///
/// 参数说明：
/// - `v`：XREVRANGE + - COUNT 1 的完整回复。
///
/// 返回：仅空数组返回 None；恰好一条且身份非零、字段结构完整时返回身份，二进制字段名称不影响尾部定位。
/// 类型、数量、身份或正文结构不符时返回错误，不能用不完整证据开放后续订阅。
fn first_entry_id(v: &Value) -> Result<Option<String>> {
    let arr = match v {
        Value::Array(a) if a.len() <= 1 => a,
        other => return Err(shape_err("XREVRANGE 应为最多一条的 Array", other)),
    };
    let Some(first) = arr.first() else {
        return Ok(None);
    };
    let idfields = match first {
        Value::Array(a) if a.len() == 2 => a,
        other => return Err(shape_err("XREVRANGE entry 应为 [id, fields]", other)),
    };
    let id = xread_string(&idfields[0])?;
    if stream_id_parts(&id).is_none_or(|parts| parts == (0, 0)) {
        return Err(NasaRedisError::Parsing("XREVRANGE 条目身份非法".into()));
    }
    // 尾部定位不分发旧正文，但仍须证实这是完整条目；字段保持二进制语义，不强加事件路由编码。
    let fields = match &idfields[1] {
        Value::Array(fields) if !fields.is_empty() && fields.len() % 2 == 0 => fields,
        other => return Err(shape_err("XREVRANGE fields 应为非空成对 Array", other)),
    };
    if !fields
        .iter()
        .all(|field| matches!(field, Value::BulkString(_) | Value::SimpleString(_)))
    {
        return Err(NasaRedisError::Parsing("XREVRANGE 字段不是字符串".into()));
    }
    Ok(Some(id))
}

/// 业务作用：把读取回复中的来源、条目身份或事件字段名转为上层使用的文本。
///
/// 参数说明：
/// - `v`：预期为 BulkString 或 SimpleString 的回复字段。
///
/// 返回：仅合法 UTF-8 字符串转换成功，非法编码或其它类型返回错误；不替换身份字节，也不把数字或 Nil 隐式转成名称。
fn xread_string(v: &Value) -> Result<String> {
    match v {
        // 名称用于路由，替代非法字节会把不同远端身份合并为同一业务身份。
        Value::BulkString(b) => String::from_utf8(b.clone())
            .map_err(|_| NasaRedisError::Parsing("Stream 文本不是合法 UTF-8".into())),
        Value::SimpleString(s) => Ok(s.clone()),
        other => Err(shape_err("应为字符串(bulk/simple)", other)),
    }
}

/// 业务作用：保留事件字段值的原始字节，拒绝把异常回复转换为空正文后交付或确认。
///
/// 参数说明：
/// - `v`：预期为 BulkString 或 SimpleString 的字段值。
///
/// 返回：批量字符串复制原始字节，简单字符串复制其 UTF-8 字节；其它类型返回错误，不进行业务解码。
fn xread_bytes(v: &Value) -> Result<Vec<u8>> {
    match v {
        Value::BulkString(b) => Ok(b.clone()),
        Value::SimpleString(s) => Ok(s.clone().into_bytes()),
        other => Err(shape_err("field value 应为字符串/字节", other)),
    }
}
