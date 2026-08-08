// ============================================================================
// src/wire.rs —— wire 帧:[4B len BE][1B type][1B mode][payload]。
// len = 2 + payload.len(覆盖 type+mode+payload)。架构说明 / 附录 A.0。
// TCP 与 WS 共享此 codec，WS 将 binary message 交给同一解码器处理。
// ============================================================================

use bytes::{Buf, BufMut, Bytes, BytesMut};
use naws_proto::{MessageRef, Mode};
use tokio_util::codec::Decoder;

/// 帧类型(1 byte)。
pub mod frame_type {
    /// 客户端发起鉴权请求。
    pub const AUTH: u8 = 0x01;
    /// 服务端返回鉴权响应。
    pub const AUTH_RESP: u8 = 0x02;
    /// 业务事件帧。
    pub const EVENT: u8 = 0x03;
    /// 业务 ACK 帧预留类型。
    pub const EVENT_ACK: u8 = 0x04;
    /// 心跳请求。
    pub const PING: u8 = 0x05;
    /// 心跳响应。
    pub const PONG: u8 = 0x06;
    /// 服务端广播事件帧。
    pub const BROADCAST: u8 = 0x07;
    /// 协议化关闭帧。
    pub const CLOSE: u8 = 0x08;
}

/// mode byte = ProtocolBytes.Mode.ordinal()。控制帧固定 VARINT(1)。
pub mod wire_mode {
    /// JSON_BYTES payload 模式。
    pub const JSON: u8 = 0;
    /// VARINT_TLV payload 模式。
    pub const VARINT: u8 = 1;
    /// BITPACK_TLV payload 模式。
    pub const BITPACK: u8 = 2;
}

/// 入站解出的帧。payload 是 opaque `Bytes`(业务/上层再按 mode 解 Message)。
#[derive(Debug, Clone)]
pub struct Frame {
    /// 帧类型,取值见 [`frame_type`]。
    pub typ: u8,
    /// payload 编码模式,取值见 [`wire_mode`]。
    pub mode: u8,
    /// 不透明 payload 字节,上层按 `mode` 解码。
    pub payload: Bytes,
}

/// 业务作用：出站统一入口:把一帧编码成完整 `Bytes`(头+payload 一次成形)。
/// fan-out 时编一次、`Bytes::clone()` 给 N 个 outbox(零拷贝,= 原实现 retainedDuplicate)。
///
/// # 参数
/// - `typ`: frame 类型字节,例如 EVENT/PING/PONG/CLOSE。
/// - `mode`: payload 编码模式字节,例如 VARINT/BITPACK/JSON。
/// - `payload`: frame payload 字节,长度会写入 4 字节前缀。
pub fn encode_frame(typ: u8, mode: u8, payload: &[u8]) -> Bytes {
    // checked 长度转换:payload > u32::MAX-2 时 `as u32` 会静默回绕成损坏帧头。
    // 实际路径由 Session::send_business 的 max_frame 上限更早拦截,这里是最后一道不变量。
    let len =
        u32::try_from(2 + payload.len() as u64).expect("frame payload exceeds u32 wire length");
    let mut b = BytesMut::with_capacity(6 + payload.len());
    b.put_u32(len);
    b.put_u8(typ);
    b.put_u8(mode);
    b.put_slice(payload);
    b.freeze()
}

/// 业务作用：把借用业务信封直接编码进最终 NASA EVENT frame。
///
/// 二进制模式只分配最终 `BytesMut`；payload 从借用 slice 写入最终 frame 一次，不先构造消息中间 `Vec`。
///
/// # 参数
///
/// - `message`: 全字段借用的业务信封。
/// - `mode`: 业务消息 wire mode。
/// - `max_frame`: frame body 上限，口径与 [`FrameCodec`] 一致且包含 type+mode。
///
/// # 错误
///
/// 信封编码失败、长度溢出、超过 wire u32 或 `max_frame` 时返回协议错误。
pub fn encode_event_frame_ref(
    message: &MessageRef<'_>,
    mode: Mode,
    max_frame: usize,
) -> naws_proto::Result<Bytes> {
    let payload_len = message.encoded_len(mode)?;
    let body_len = payload_len
        .checked_add(2)
        .ok_or(naws_proto::CodecError::LengthOverflow)?;
    if body_len > max_frame || u32::try_from(body_len).is_err() {
        return Err(naws_proto::CodecError::LengthExceeds);
    }
    let capacity = body_len
        .checked_add(4)
        .ok_or(naws_proto::CodecError::LengthOverflow)?;
    let mut output = BytesMut::with_capacity(capacity);
    output.put_u32(body_len as u32);
    output.put_u8(frame_type::EVENT);
    output.put_u8(mode.ordinal());
    let written = message.encode_into(mode, &mut output)?;
    debug_assert_eq!(written, payload_len, "encoded_len 必须与实际写入一致");
    Ok(output.freeze())
}

/// 业务作用：控制帧便捷构造(空 payload,VARINT mode)。
pub fn ping() -> Bytes {
    encode_frame(frame_type::PING, wire_mode::VARINT, &[])
}

/// 业务作用：构造 pong 控制帧；用于响应心跳并保持连接活跃。
pub fn pong() -> Bytes {
    encode_frame(frame_type::PONG, wire_mode::VARINT, &[])
}

/// 入站解帧器(只解码,出站走 encode_frame)。长度前缀切帧 + 设防。
pub struct FrameCodec {
    max_frame: usize,
}

impl FrameCodec {
    /// 业务作用：构造入站 frame 解码器。
    ///
    /// # 参数
    /// - `max_frame`: 单条 frame 声明长度上限,超过即拒绝解码。
    pub fn new(max_frame: usize) -> Self {
        Self { max_frame }
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = std::io::Error;

    /// 业务作用：从 TCP/WS 字节流中尝试切出一条完整 frame。
    ///
    /// # 参数
    /// - `src`: tokio codec 提供的入站累计缓冲区;函数会在成功解帧后消费对应字节。
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, std::io::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        // 先 peek 长度,数据不足时不消费,等更多字节。
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        //:body 至少含 type+mode 两字节。
        if len < 2 {
            return Err(err("frame body < 2 bytes"));
        }
        if len > self.max_frame {
            return Err(err("frame exceeds max_frame"));
        }
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }
        src.advance(4);
        let typ = src.get_u8();
        let mode = src.get_u8();
        let payload = src.split_to(len - 2).freeze();
        Ok(Some(Frame { typ, mode, payload }))
    }
}

/// 业务作用：构造协议解析错误；用于把非法帧转换为统一 IO 错误。
///
/// # 参数
/// - `msg`: 业务消息体或事件载荷。
fn err(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}
