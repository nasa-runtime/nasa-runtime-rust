//! typed 与借用消费共用的路由声明门禁，不复制 header 内容。

use super::*;

/// 已确认唯一性、非 null 和 UTF-8 的线路声明；业务重复头不受此结构约束。
pub(super) struct RouteHeaders<'a> {
    pub event: &'a str,
    codec: Option<&'a str>,
    mode: Option<&'a str>,
}

impl<'a> RouteHeaders<'a> {
    /// 业务作用：在路由查找前验证声明，避免未知路由绕过非法 codec 的处置策略。
    ///
    /// 参数说明：`headers` 为按原始顺序借用的名称和值；`strict` 表示拒绝重复保留头。
    /// 返回：缺失 event 使用 DEFAULT；重复、null、非法 UTF-8、空白事件或未知 codec 返回脱敏原因。
    pub fn parse(
        headers: impl Iterator<Item = (&'a str, Option<&'a [u8]>)>,
        strict: bool,
    ) -> std::result::Result<Self, &'static str> {
        let mut values = [None; 3];
        for (name, value) in headers {
            let index = match name {
                HEADER_EVENT => 0,
                HEADER_PAYLOAD_CODEC => 1,
                HEADER_PAYLOAD_MODE => 2,
                _ => continue,
            };
            // 歧义声明可能把相同消息送到不同业务；默认必须拒绝，兼容模式才采用最后值。
            if strict && values[index].is_some() {
                return Err("重复保留头");
            }
            values[index] = Some(value);
        }
        let event = singleton_text(values[0])?.unwrap_or(DEFAULT_EVENT);
        if crate::wire::is_blank_event(event) {
            return Err("event 为空");
        }
        let codec = singleton_text(values[1])?;
        let mode = singleton_text(values[2])?;
        if codec.is_some_and(|value| value != PAYLOAD_CODEC_PROTOCOL_BYTES) {
            return Err("未知 codec");
        }
        Ok(Self { event, codec, mode })
    }

    /// 业务作用：在解码前核对线路声明和 route 的静态类型，禁止带 mode 的 JSON 被默认为纯 JSON。
    ///
    /// 参数说明：`expected` 为注册时冻结的 codec 和 ProtocolBytes 模式。
    /// 返回：声明匹配时成功；JSON 携带 codec/mode 或二进制声明不匹配时返回 Codec。
    pub fn validate_codec(&self, expected: PayloadCodec) -> Result<()> {
        match expected {
            PayloadCodec::Json if self.codec.is_none() && self.mode.is_none() => Ok(()),
            PayloadCodec::Json => Err(NafkaError::Codec(
                "JSON route 不接受 codec/mode 声明".into(),
            )),
            PayloadCodec::Proto(mode) => {
                if self.codec != Some(PAYLOAD_CODEC_PROTOCOL_BYTES) {
                    return Err(NafkaError::Codec(
                        "ProtocolBytes codec header 不匹配".into(),
                    ));
                }
                // 缺失 mode 沿用类型声明；存在时必须匹配，不能猜测其它解码方式。
                if self
                    .mode
                    .is_some_and(|value| crate::wire::mode_from_wire_name(value) != Some(mode))
                {
                    return Err(NafkaError::Codec("ProtocolBytes mode header 不匹配".into()));
                }
                Ok(())
            }
        }
    }
}

/// 业务作用：区分缺失声明与显式 null，避免无效字节经有损替换后改变路由身份。
///
/// 参数说明：`value` 的外层 None 表示缺失，内层 None 表示显式 null。
/// 返回：缺失为 None，合法 UTF-8 为借用文本；其它情况返回固定原因。
fn singleton_text(value: Option<Option<&[u8]>>) -> std::result::Result<Option<&str>, &'static str> {
    match value {
        None => Ok(None),
        Some(None) => Err("保留头不能为 null"),
        Some(Some(bytes)) => std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| "保留头不是 UTF-8"),
    }
}
