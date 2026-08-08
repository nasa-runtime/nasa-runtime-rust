//! Wire 协议层:header 常量、payload 编解码契约与跨语言互通规则。
//!
//! 本模块是跨语言兼容面:常量与 mode 名和上游对标实现逐字节一致,改名即破坏互通,
//! 任何调整都是协议变更,必须同步跨语言 golden 向量。

use serde::{de::DeserializeOwned, Serialize};

use crate::error::{NafkaError, Result};
use crate::types::{KafkaHeader, KafkaHeaders};

// ==================== 固定 header(与参照实现逐字节一致,禁止改名) ====================

/// publish 未指定 event 时写入的默认事件名;消费端 header 缺失同样回退此值。
pub const DEFAULT_EVENT: &str = "DEFAULT";
/// 业务事件名 header:消费端按 (group, topic, event) 三元组路由到唯一 consumer。
pub const HEADER_EVENT: &str = "X-Nasa-Event";
/// payload 编码标记 header;值固定为 [`PAYLOAD_CODEC_PROTOCOL_BYTES`],JSON 不打此 header(兼容旧消息)。
pub const HEADER_PAYLOAD_CODEC: &str = "X-Nasa-Payload-Codec";
/// ProtocolBytes 的 Mode 名 header:消费端校验与类型声明一致,防协议代次或异构实现解码错乱。
pub const HEADER_PAYLOAD_MODE: &str = "X-Nasa-Payload-Mode";
/// 透传快照 header(JSON):trace 等跨服务上下文放 header 而非 payload,与二进制 payload 共存。
pub const HEADER_PASSTHROUGH: &str = "X-Nasa-Passthrough";
/// W3C Trace Context；producer 写子 span context，consumer 解析后继续同一 trace。
pub const HEADER_TRACEPARENT: &str = "traceparent";
/// 当前记录在本 consumer 会话中的可信投递次数；owner 会覆盖来源侧同名值，业务不得自报。
pub const HEADER_DELIVERY_ATTEMPT: &str = "X-Nasa-Delivery-Attempt";
/// DLT 记录的来源 topic。
pub const HEADER_DLT_ORIGIN_TOPIC: &str = "X-Nasa-DLT-Origin-Topic";
/// DLT 记录的来源分区。
pub const HEADER_DLT_ORIGIN_PARTITION: &str = "X-Nasa-DLT-Origin-Partition";
/// DLT 记录的来源 offset;三元组共同构成 DLT 消费端的去重键。
pub const HEADER_DLT_ORIGIN_OFFSET: &str = "X-Nasa-DLT-Origin-Offset";
/// DLT 原因(截断后的安全文本,<=1024 字节,不含 payload/凭据)。
pub const HEADER_DLT_REASON: &str = "X-Nasa-DLT-Reason";
/// DLT 前已发生的业务 handler 投递次数；与原始 envelope、headers 和来源 offset 一起留证。
pub const HEADER_DLT_DELIVERY_ATTEMPTS: &str = "X-Nasa-DLT-Delivery-Attempts";
/// DLT 记录的来源 consumer group；用于归属告警和人工重放审计。
pub const HEADER_DLT_ORIGIN_GROUP: &str = "X-Nasa-Dlt-Origin-Group";
/// [`HEADER_PAYLOAD_CODEC`] 的唯一合法值。
pub const PAYLOAD_CODEC_PROTOCOL_BYTES: &str = "protocol-bytes";

/// 业务作用：判断 header 名是否为框架保留:业务发布接口写保留 header 直接报错,不静默覆盖。
///
/// # 参数
/// - `name`: 待检查的 header 名,大小写敏感(保留 header 一律精确大小写)。
pub fn is_reserved_header(name: &str) -> bool {
    matches!(
        name,
        HEADER_EVENT
            | HEADER_PAYLOAD_CODEC
            | HEADER_PAYLOAD_MODE
            | HEADER_PASSTHROUGH
            | HEADER_TRACEPARENT
            | HEADER_DELIVERY_ATTEMPT
            | HEADER_DLT_ORIGIN_TOPIC
            | HEADER_DLT_ORIGIN_PARTITION
            | HEADER_DLT_ORIGIN_OFFSET
            | HEADER_DLT_REASON
            | HEADER_DLT_DELIVERY_ATTEMPTS
            | HEADER_DLT_ORIGIN_GROUP
    )
}

/// 业务作用：为 durability-first DLT 构造唯一可信的来源证据 headers。
///
/// 原始业务 headers 保持顺序与重复项；producer 可伪造的 delivery/DLT 同名字段会先按
/// 大小写不敏感规则全部剥离，再由 consumer owner 写入唯一可信值。该函数不复制 payload，
/// 仅集中跨自定义 DLT 与受管 DLT 都必须遵守的证据格式。
///
/// 参数说明：
/// - `original`: 来源记录的原始有序 headers。
/// - `group`: 实际消费来源记录的稳定 group。
/// - `topic`: 来源 topic。
/// - `partition`: 来源分区。
/// - `offset`: 来源 offset。
/// - `reason`: 已脱敏失败原因；按 UTF-8 边界截断到 1024 字节。
/// - `delivery_attempts`: 进入 DLT 前的可信投递次数，零会收敛为一。
///
/// 返回：保留业务 headers 并追加来源、原因和次数证据的新集合。
pub fn dead_letter_headers(
    original: &KafkaHeaders,
    group: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    reason: &str,
    delivery_attempts: u32,
) -> KafkaHeaders {
    let mut headers = original
        .iter()
        .filter(|header| !is_dead_letter_evidence_header(&header.name))
        .cloned()
        .collect::<Vec<_>>();
    let delivery_attempts = delivery_attempts.max(1);
    headers.extend([
        KafkaHeader {
            name: HEADER_DELIVERY_ATTEMPT.into(),
            value: Some(delivery_attempts.to_string().into_bytes()),
        },
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_TOPIC.into(),
            value: Some(topic.as_bytes().to_vec()),
        },
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_PARTITION.into(),
            value: Some(partition.to_string().into_bytes()),
        },
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_OFFSET.into(),
            value: Some(offset.to_string().into_bytes()),
        },
        KafkaHeader {
            name: HEADER_DLT_REASON.into(),
            value: Some(truncate_utf8_bytes(reason, 1024).as_bytes().to_vec()),
        },
        KafkaHeader {
            name: HEADER_DLT_DELIVERY_ATTEMPTS.into(),
            value: Some(delivery_attempts.to_string().into_bytes()),
        },
        KafkaHeader {
            name: HEADER_DLT_ORIGIN_GROUP.into(),
            value: Some(group.as_bytes().to_vec()),
        },
    ]);
    KafkaHeaders::from_vec(headers)
}

/// 业务作用：识别必须由 consumer owner 覆盖的投递/DLT 证据字段。
///
/// 参数说明：
/// - `name`: 来源 header 名。
///
/// 返回：属于框架证据字段时返回真；比较大小写不敏感，防止变体绕过清理。
fn is_dead_letter_evidence_header(name: &str) -> bool {
    name.eq_ignore_ascii_case(HEADER_DELIVERY_ATTEMPT)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_TOPIC)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_PARTITION)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_OFFSET)
        || name.eq_ignore_ascii_case(HEADER_DLT_REASON)
        || name.eq_ignore_ascii_case(HEADER_DLT_DELIVERY_ATTEMPTS)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_GROUP)
}

/// 业务作用：按 UTF-8 字节上限安全截断 DLT 原因，避免多字节文本突破 broker header 合同。
///
/// 参数说明：
/// - `value`: 待截断文本。
/// - `max_bytes`: 最大字节数。
///
/// 返回：不超过上限且仍位于字符边界的前缀。
fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

// ==================== Mode 名映射(跨语言) ====================

/// 业务作用：把 ProtocolBytes Mode 映射为跨语言 wire 名(上游对标实现的枚举名)。
///
/// 为什么单列函数:mode 名进 [`HEADER_PAYLOAD_MODE`],两端必须逐字节一致;
/// 集中一处 + golden 向量锁死,避免散落的字符串常量各写各的。
///
/// # 参数
/// - `mode`: naws-proto 的线协议模式。
pub fn mode_wire_name(mode: naws_proto::Mode) -> &'static str {
    match mode {
        naws_proto::Mode::JsonBytes => "JSON_BYTES",
        naws_proto::Mode::VarintTlv => "VARINT_TLV",
        naws_proto::Mode::BitpackTlv => "BITPACK_TLV",
        naws_proto::Mode::FastFixed => "FAST_FIXED",
    }
}

/// 业务作用：从 wire 名解析 Mode;大小写不敏感(参照实现消费端即忽略大小写比较)。
///
/// # 参数
/// - `name`: header 携带的 mode 名。
///
/// # 返回
/// 未知名字返回 None,由调用方按 undecodable 策略处理,不猜默认值。
pub fn mode_from_wire_name(name: &str) -> Option<naws_proto::Mode> {
    if name.eq_ignore_ascii_case("JSON_BYTES") {
        Some(naws_proto::Mode::JsonBytes)
    } else if name.eq_ignore_ascii_case("VARINT_TLV") {
        Some(naws_proto::Mode::VarintTlv)
    } else if name.eq_ignore_ascii_case("BITPACK_TLV") {
        Some(naws_proto::Mode::BitpackTlv)
    } else if name.eq_ignore_ascii_case("FAST_FIXED") {
        Some(naws_proto::Mode::FastFixed)
    } else {
        None
    }
}

// ==================== payload 编解码契约 ====================

/// payload 的编码方式,编译期由类型定死,无运行时探测。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadCodec {
    /// serde JSON UTF-8:默认路径,不打 codec header,与旧消息兼容。
    Json,
    /// ProtocolBytes 二进制,携带具体 Mode;发布时打 codec + mode 两个 header。
    Proto(naws_proto::Mode),
}

/// 发布方向的 payload 契约:producer 只依赖本 trait。
///
/// 与 [`DecodePayload`] 分离的原因:单向 DTO(只发不收/只收不发)
/// 不应被迫实现用不到的 serde 方向。
pub trait EncodePayload: Sync + 'static {
    /// 本类型的编码方式;producer 据此决定是否打 codec/mode header。
    const CODEC: PayloadCodec;

    /// 业务作用：把业务对象编码为 Kafka value 字节。
    ///
    /// # 错误
    /// 编码失败返回 [`NafkaError::Codec`];发布路径把它归为确定失败(可安全重试构造)。
    fn encode(&self) -> Result<Vec<u8>>;
}

/// 消费方向的 payload 契约:consumer 的关联类型只依赖本 trait。
pub trait DecodePayload: Sized + Send + 'static {
    /// 本类型声明的编码方式;消费端用它对 codec/mode header 做一致性校验。
    const CODEC: PayloadCodec;

    /// 业务作用：从 Kafka value 字节还原业务对象。
    ///
    /// # 参数
    /// - `bytes`: 消息 value 原始字节(非空;tombstone 在路由前处理,不会进到这里)。
    ///
    /// # 错误
    /// 解码失败返回 [`NafkaError::Codec`];dispatcher 将该 offset 精确标记为 undecodable。
    fn decode(bytes: &[u8]) -> Result<Self>;
}

// JSON blanket:一切 serde 类型默认走 JSON UTF-8,与参照实现互通。
// 与下方 Proto<T> 的具体 impl 不重叠:Proto<T> 不实现 Serialize/DeserializeOwned,
// 且孤儿规则封死了下游补实现的可能，该编译期边界不能由下游绕开。
impl<T: Serialize + Sync + 'static> EncodePayload for T {
    const CODEC: PayloadCodec = PayloadCodec::Json;

    /// 业务作用：将 serde 业务值编码为 UTF-8 JSON 字节。
    ///
    /// # 错误
    /// JSON 序列化失败时返回 codec 错误。
    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| NafkaError::Codec(format!("JSON 编码失败: {e}")))
    }
}

impl<T: DeserializeOwned + Send + 'static> DecodePayload for T {
    const CODEC: PayloadCodec = PayloadCodec::Json;

    /// 业务作用：从 UTF-8 JSON 字节恢复业务值。
    ///
    /// - `bytes`: Kafka value 原始字节。
    ///
    /// # 错误
    /// JSON 反序列化失败时返回 codec 错误。**错误文本不得包含 payload 内容**：
    /// 该错误会进 DLT reason header 与公开的 `GroupHealth.last_error`（均不可信下游可见），
    /// 而 serde_json 的 `Display` 对类型不符会内嵌出错的输入值
    /// （`invalid type: string "sk-live-…"`），原样转出会造成凭据外泄；解码 panic 与更常见的
    /// serde Err 主路径都必须统一脱敏。
    /// 只保留不含内容的定位信息：错误类别、行列位置与目标类型名（编译期常量）。
    fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| {
            NafkaError::Codec(format!(
                "JSON 解码失败(内容已抑制,类型 {},{:?} @ line {} col {})",
                std::any::type_name::<Self>(),
                e.classify(),
                e.line(),
                e.column()
            ))
        })
    }
}

/// 类型级 Mode 声明:每个 ProtocolBytes 业务类型一行 impl,声明自己的线协议模式。
///
/// 为什么需要它:naws-proto 的 Mode 在 encode/decode 调用点传参,类型上没有
/// 静态关联;而消费端 mode header 校验需要"类型 → 期望 Mode"的编译期映射。
pub trait ProtoMode {
    /// 本类型固定使用的线协议模式;FAST_FIXED 当前未实现,注册校验会拒绝。
    const MODE: naws_proto::Mode;
}

/// ProtocolBytes 载荷的选择加入包装:`type Message = Proto<FastTicker>` 即切换到二进制路径。
///
/// 只是 codec marker + 透明包装,无运行时开销(repr(transparent))。
/// 禁止为它实现 serde Serialize/Deserialize——那会与 JSON blanket impl 产生编码歧义,
/// 该类型约束在编译期锁住编码路径边界。
#[repr(transparent)]
pub struct Proto<T>(pub T);

impl<T> Proto<T> {
    /// 业务作用：取出内部业务值,零成本。
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for Proto<T> {
    type Target = T;

    /// 业务作用：借用透明包装中的业务值。
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> EncodePayload for Proto<T>
where
    T: naws_proto::WireCodec + ProtoMode + Sync + 'static,
{
    const CODEC: PayloadCodec = PayloadCodec::Proto(T::MODE);

    /// 业务作用：按类型声明的固定 Mode 编码业务值。
    ///
    /// # 错误
    /// 线协议编码失败时返回 codec 错误。
    fn encode(&self) -> Result<Vec<u8>> {
        self.0.encode(T::MODE).map_err(|e| {
            NafkaError::Codec(format!("ProtocolBytes 编码失败 mode={:?}: {e}", T::MODE))
        })
    }
}

impl<T> DecodePayload for Proto<T>
where
    T: naws_proto::WireCodec + ProtoMode + Send + 'static,
{
    const CODEC: PayloadCodec = PayloadCodec::Proto(T::MODE);

    /// 业务作用：按类型声明的固定 Mode 解码业务值。
    ///
    /// - `bytes`: Kafka value 原始字节。
    ///
    /// # 错误
    /// 线协议解码失败时返回 codec 错误。与 JSON blanket 同理，错误文本不得包含 payload 内容：
    /// `CodecError::Json` 变体（JSON_BYTES 子模式）内嵌 serde 文本，可携带出错的输入值，
    /// 会经 DLT reason header / `GroupHealth.last_error` 外泄；其余变体只带 tag/offset/count
    /// 等结构信息，可安全展示。
    fn decode(bytes: &[u8]) -> Result<Self> {
        T::decode(T::MODE, bytes).map(Proto).map_err(|e| {
            let detail = match &e {
                naws_proto::CodecError::Json(_) => "内容已抑制".to_owned(),
                structural => format!("{structural:?}"),
            };
            NafkaError::Codec(format!(
                "ProtocolBytes 解码失败 mode={:?} 类型 {}: {detail}",
                T::MODE,
                std::any::type_name::<T>()
            ))
        })
    }
}
