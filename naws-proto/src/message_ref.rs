//! 业务消息信封的借用式编码器，面向少拷贝转发热路径。

use bytes::BufMut;

use crate::io::{zigzag_enc, MAX_BYTES_LEN, MAX_STR_LEN, WT_LEN, WT_VARINT};
use crate::{CodecError, Message, Mode, Result};

/// 全字段借用的业务消息信封。
///
/// 路由字符串、payload 都由调用方持有；二进制模式可直接写入最终 frame 缓冲区。
#[derive(Clone, Copy, Debug, Default)]
pub struct MessageRef<'a> {
    /// 业务事件名列表。
    pub events: Option<&'a [&'a str]>,
    /// 发送方用户 ID。
    pub from_uid: Option<&'a str>,
    /// 发送方 session/client id。
    pub from_client: Option<&'a str>,
    /// 目标 endpoint/router 列表。
    pub routers: Option<&'a [&'a str]>,
    /// 目标群组。
    pub group: Option<&'a str>,
    /// 目标用户 ID 列表。
    pub uids: Option<&'a [&'a str]>,
    /// 排除用户 ID 列表。
    pub excludes: Option<&'a [&'a str]>,
    /// 原始业务 payload。
    pub payload: Option<&'a [u8]>,
    /// ACK 关联 ID。
    pub ack_id: i64,
}

impl MessageRef<'_> {
    /// 计算指定模式的精确编码长度。
    ///
    /// # 参数
    ///
    /// - `mode`: 目标 wire mode。
    ///
    /// # 错误
    ///
    /// 字段超限、长度加法溢出或模式未实现时返回错误。
    pub fn encoded_len(&self, mode: Mode) -> Result<usize> {
        validate_fields(self)?;
        match mode {
            Mode::BitpackTlv => bitpack_len(self),
            Mode::VarintTlv => varint_len(self),
            Mode::JsonBytes => serde_json::to_vec(&self.to_owned_message())
                .map(|value| value.len())
                .map_err(|error| CodecError::Json(error.to_string())),
            Mode::FastFixed => Err(CodecError::Unsupported("FAST_FIXED")),
        }
    }

    /// 把当前信封直接编码进调用方提供的最终缓冲区。
    ///
    /// # 参数
    ///
    /// - `mode`: 目标 wire mode。
    /// - `output`: 调用方持有的可增长或定长缓冲区。
    ///
    /// # 返回
    ///
    /// 本次追加的精确字节数。
    ///
    /// # 错误
    ///
    /// 字段超限、输出容量不足、长度溢出或模式未实现时返回错误。
    pub fn encode_into<B: BufMut>(&self, mode: Mode, output: &mut B) -> Result<usize> {
        let expected = self.encoded_len(mode)?;
        if output.remaining_mut() < expected {
            return Err(CodecError::BufferTooSmall {
                required: expected,
                remaining: output.remaining_mut(),
            });
        }
        match mode {
            Mode::BitpackTlv => encode_bitpack(self, output),
            Mode::VarintTlv => encode_varint(self, output),
            Mode::JsonBytes => {
                let value = serde_json::to_vec(&self.to_owned_message())
                    .map_err(|error| CodecError::Json(error.to_string()))?;
                output.put_slice(&value);
            }
            Mode::FastFixed => return Err(CodecError::Unsupported("FAST_FIXED")),
        }
        Ok(expected)
    }

    /// 构造语义等价的拥有型信封。
    ///
    /// 仅在目标协议必须拥有数据时调用；原生二进制直推路径不需要执行该转换。
    ///
    /// # 返回
    ///
    /// 完整复制借用字段后的拥有型消息。
    pub fn to_owned_message(&self) -> Message {
        Message {
            events: own_array(self.events),
            from_uid: self.from_uid.map(str::to_owned),
            from_client: self.from_client.map(str::to_owned),
            routers: own_array(self.routers),
            group: self.group.map(str::to_owned),
            uids: own_array(self.uids),
            excludes: own_array(self.excludes),
            payload: self.payload.map(<[u8]>::to_vec),
            ack_id: self.ack_id,
        }
    }
}

/// 借用字符串数组转为兼容 JSON 路径的拥有型数组。
///
/// # 参数
///
/// - `value`: 可选借用字符串数组。
fn own_array(value: Option<&[&str]>) -> Option<Vec<Option<String>>> {
    value.map(|items| items.iter().map(|item| Some((*item).to_owned())).collect())
}

/// 校验单字段和数组上限，避免写入期发生不可控放大。
///
/// # 参数
///
/// - `message`: 待编码信封。
///
/// # 错误
///
/// 任一字符串、payload 或数组元素数超过协议上限时返回错误。
fn validate_fields(message: &MessageRef<'_>) -> Result<()> {
    for value in [message.from_uid, message.from_client, message.group]
        .into_iter()
        .flatten()
    {
        if value.len() > MAX_STR_LEN {
            return Err(CodecError::LengthExceeds);
        }
    }
    for values in [
        message.events,
        message.routers,
        message.uids,
        message.excludes,
    ]
    .into_iter()
    .flatten()
    {
        if values.len() as u64 > crate::io::MAX_ARRAY_ELEMS
            || values.iter().any(|value| value.len() > MAX_STR_LEN)
        {
            return Err(CodecError::LengthExceeds);
        }
    }
    if message
        .payload
        .is_some_and(|value| value.len() > MAX_BYTES_LEN)
    {
        return Err(CodecError::LengthExceeds);
    }
    Ok(())
}

/// 计算 BITPACK_TLV 长度。
///
/// # 参数
///
/// - `message`: 已校验信封。
fn bitpack_len(message: &MessageRef<'_>) -> Result<usize> {
    let mut total = 2_usize;
    for values in [
        message.events,
        message.routers,
        message.uids,
        message.excludes,
    ]
    .into_iter()
    .flatten()
    {
        total = checked_add(total, array_value_len(values)?)?;
    }
    for value in [message.from_uid, message.from_client, message.group]
        .into_iter()
        .flatten()
    {
        total = checked_add(total, length_delimited_len(value.len())?)?;
    }
    if let Some(payload) = message.payload {
        total = checked_add(total, length_delimited_len(payload.len())?)?;
    }
    checked_add(total, unsigned_varint_len(zigzag_enc(message.ack_id)))
}

/// 计算 VARINT_TLV 长度。
///
/// # 参数
///
/// - `message`: 已校验信封。
fn varint_len(message: &MessageRef<'_>) -> Result<usize> {
    let mut total = 0_usize;
    for (tag, values) in [
        (10_u64, message.events),
        (40, message.routers),
        (60, message.uids),
        (70, message.excludes),
    ] {
        if let Some(values) = values {
            total = checked_add(total, field_key_len(tag, WT_LEN))?;
            total = checked_add(total, array_value_len(values)?)?;
        }
    }
    for (tag, value) in [
        (20_u64, message.from_uid),
        (30, message.from_client),
        (50, message.group),
    ] {
        if let Some(value) = value {
            total = checked_add(total, field_key_len(tag, WT_LEN))?;
            total = checked_add(total, length_delimited_len(value.len())?)?;
        }
    }
    if let Some(payload) = message.payload {
        total = checked_add(total, field_key_len(80, WT_LEN))?;
        total = checked_add(total, length_delimited_len(payload.len())?)?;
    }
    total = checked_add(total, field_key_len(90, WT_VARINT))?;
    checked_add(total, unsigned_varint_len(zigzag_enc(message.ack_id)))
}

/// 编码 BITPACK_TLV。
///
/// # 参数
///
/// - `message`: 已校验信封。
/// - `output`: 容量已检查的输出。
fn encode_bitpack<B: BufMut>(message: &MessageRef<'_>, output: &mut B) {
    let mut bitmap = 1_u16 << 8;
    for (bit, present) in [
        message.events.is_some(),
        message.from_uid.is_some(),
        message.from_client.is_some(),
        message.routers.is_some(),
        message.group.is_some(),
        message.uids.is_some(),
        message.excludes.is_some(),
        message.payload.is_some(),
    ]
    .into_iter()
    .enumerate()
    {
        if present {
            bitmap |= 1_u16 << bit;
        }
    }
    output.put_u8(bitmap as u8);
    output.put_u8((bitmap >> 8) as u8);
    if let Some(value) = message.events {
        put_array(output, value);
    }
    if let Some(value) = message.from_uid {
        put_len_delimited(output, value.as_bytes());
    }
    if let Some(value) = message.from_client {
        put_len_delimited(output, value.as_bytes());
    }
    if let Some(value) = message.routers {
        put_array(output, value);
    }
    if let Some(value) = message.group {
        put_len_delimited(output, value.as_bytes());
    }
    if let Some(value) = message.uids {
        put_array(output, value);
    }
    if let Some(value) = message.excludes {
        put_array(output, value);
    }
    if let Some(value) = message.payload {
        put_payload(output, value);
    }
    put_varint(output, zigzag_enc(message.ack_id));
}

/// 编码 VARINT_TLV。
///
/// # 参数
///
/// - `message`: 已校验信封。
/// - `output`: 容量已检查的输出。
fn encode_varint<B: BufMut>(message: &MessageRef<'_>, output: &mut B) {
    if let Some(value) = message.events {
        put_key(output, 10, WT_LEN);
        put_array(output, value);
    }
    if let Some(value) = message.from_uid {
        put_key(output, 20, WT_LEN);
        put_len_delimited(output, value.as_bytes());
    }
    if let Some(value) = message.from_client {
        put_key(output, 30, WT_LEN);
        put_len_delimited(output, value.as_bytes());
    }
    if let Some(value) = message.routers {
        put_key(output, 40, WT_LEN);
        put_array(output, value);
    }
    if let Some(value) = message.group {
        put_key(output, 50, WT_LEN);
        put_len_delimited(output, value.as_bytes());
    }
    if let Some(value) = message.uids {
        put_key(output, 60, WT_LEN);
        put_array(output, value);
    }
    if let Some(value) = message.excludes {
        put_key(output, 70, WT_LEN);
        put_array(output, value);
    }
    if let Some(value) = message.payload {
        put_key(output, 80, WT_LEN);
        put_payload(output, value);
    }
    put_key(output, 90, WT_VARINT);
    put_varint(output, zigzag_enc(message.ack_id));
}

/// 写字符串数组的 LENGTH_DELIMITED 值。
///
/// # 参数
///
/// - `output`: 输出缓冲区。
/// - `values`: 非空与空字符串都按现有 wire 规则写入。
fn put_array<B: BufMut>(output: &mut B, values: &[&str]) {
    let inner = values
        .iter()
        .fold(unsigned_varint_len(values.len() as u64), |total, value| {
            total + unsigned_varint_len(value.len() as u64) + value.len()
        });
    put_varint(output, inner as u64);
    put_varint(output, values.len() as u64);
    for value in values {
        put_varint(output, value.len() as u64);
        output.put_slice(value.as_bytes());
    }
}

/// 写字段 key。
///
/// # 参数
///
/// - `output`: 输出缓冲区。
/// - `tag`: schema tag。
/// - `wire_type`: wire type。
fn put_key<B: BufMut>(output: &mut B, tag: u64, wire_type: u64) {
    put_varint(output, (tag << 3) | wire_type);
}

/// 把原始 payload 直接写入最终调用方缓冲区。
///
/// # 参数
///
/// - `output`: 最终编码缓冲区。
/// - `payload`: 借用原始 payload。
fn put_payload<B: BufMut>(output: &mut B, payload: &[u8]) {
    put_len_delimited(output, payload);
}

/// 写 LENGTH_DELIMITED 值。
///
/// # 参数
///
/// - `output`: 输出缓冲区。
/// - `value`: 原始值。
fn put_len_delimited<B: BufMut>(output: &mut B, value: &[u8]) {
    put_varint(output, value.len() as u64);
    output.put_slice(value);
}

/// 写无符号 varint。
///
/// # 参数
///
/// - `output`: 输出缓冲区。
/// - `value`: 待编码值。
fn put_varint<B: BufMut>(output: &mut B, mut value: u64) {
    while value & !0x7f != 0 {
        output.put_u8(((value & 0x7f) | 0x80) as u8);
        value >>= 7;
    }
    output.put_u8(value as u8);
}

/// 计算字段 key 长度。
///
/// # 参数
///
/// - `tag`: schema tag。
/// - `wire_type`: wire type。
fn field_key_len(tag: u64, wire_type: u64) -> usize {
    unsigned_varint_len((tag << 3) | wire_type)
}

/// 计算字符串数组的完整 LENGTH_DELIMITED 长度。
///
/// # 参数
///
/// - `values`: 借用字符串数组。
fn array_value_len(values: &[&str]) -> Result<usize> {
    let mut inner = unsigned_varint_len(values.len() as u64);
    for value in values {
        inner = checked_add(inner, unsigned_varint_len(value.len() as u64))?;
        inner = checked_add(inner, value.len())?;
    }
    checked_add(unsigned_varint_len(inner as u64), inner)
}

/// 计算 LENGTH_DELIMITED 值长度。
///
/// # 参数
///
/// - `value_len`: 原值字节数。
fn length_delimited_len(value_len: usize) -> Result<usize> {
    checked_add(unsigned_varint_len(value_len as u64), value_len)
}

/// 计算无符号 varint 字节数。
///
/// # 参数
///
/// - `value`: 待编码数值。
fn unsigned_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// 执行 checked 长度加法。
///
/// # 参数
///
/// - `left`: 已累计长度。
/// - `right`: 待增加长度。
///
/// # 错误
///
/// `usize` 溢出时返回长度错误。
fn checked_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or(CodecError::LengthOverflow)
}
