//! Socket.IO 包头语法；不解析或重写事件业务参数。

/// Socket.IO 协议中的七种包类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// 建立指定命名空间的逻辑连接，身份校验由应用负责。
    Connect = 0,
    /// 结束命名空间的逻辑连接，不携带事件参数。
    Disconnect = 1,
    /// 无二进制附件的事件，可携带请求确认编号。
    Event = 2,
    /// 对某一确认编号的响应，业务是否成功由响应合同决定。
    Ack = 3,
    /// 命名空间连接被拒绝的协议响应。
    ConnectError = 4,
    /// 带后续二进制附件的事件，文本头声明附件数量。
    BinaryEvent = 5,
    /// 带后续二进制附件的确认响应，必须携带确认编号。
    BinaryAck = 6,
}

/// 借用包头视图，JSON 参数由应用按自己的合同解析。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet<'a> {
    /// 标准包类别，不决定应用身份或业务权限。
    pub kind: Kind,
    /// 借用的逻辑命名空间，生命周期不超过原始输入。
    pub namespace: &'a str,
    /// 可选标准确认编号，不代表应用处理已经完成。
    pub ack_id: Option<u64>,
    /// 后续二进制附件数量，由调用方限定可接受上限。
    pub attachments: usize,
    /// 保留原始 JSON 参数，由应用自行进行结构和语义校验。
    pub json: &'a str,
}

impl<'a> Packet<'a> {
    /// 业务作用：从 Engine.IO MESSAGE 的正文解析 Socket.IO 包头，保留原始 JSON 字节。
    ///
    /// 参数说明：
    /// - `text`：不含 Engine.IO 类型字符的完整 Socket.IO 文本包。
    /// - `maximum_attachments`：调用者允许暂存的附件数量。
    ///
    /// 返回：无拷贝包头视图；非法类型、命名空间、确认编号或附件计数拒绝。
    pub fn decode(text: &'a str, maximum_attachments: usize) -> Option<Self> {
        let kind = match text.as_bytes().first()? {
            b'0' => Kind::Connect,
            b'1' => Kind::Disconnect,
            b'2' => Kind::Event,
            b'3' => Kind::Ack,
            b'4' => Kind::ConnectError,
            b'5' => Kind::BinaryEvent,
            b'6' => Kind::BinaryAck,
            _ => return None,
        };
        let mut rest = &text[1..];
        let attachments = if matches!(kind, Kind::BinaryEvent | Kind::BinaryAck) {
            let end = rest.find('-')?;
            let count = decimal(&rest[..end])?;
            if count == 0 || count > maximum_attachments as u64 {
                return None;
            }
            rest = &rest[end + 1..];
            count as usize
        } else {
            0
        };
        let namespace = if rest.starts_with('/') {
            let end = rest.find(',').unwrap_or(rest.len());
            let namespace = &rest[..end];
            if namespace.len() > 256
                || namespace
                    .bytes()
                    .any(|b| b.is_ascii_control() || b"?#%\\".contains(&b))
            {
                return None;
            }
            rest = if end == rest.len() {
                ""
            } else {
                &rest[end + 1..]
            };
            namespace
        } else {
            "/"
        };
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let ack_id = if digits > 0 {
            let id = decimal(&rest[..digits])?;
            if id > 9_007_199_254_740_991 {
                return None;
            }
            rest = &rest[digits..];
            Some(id)
        } else {
            None
        };
        if ack_id.is_some()
            && !matches!(
                kind,
                Kind::Event | Kind::Ack | Kind::BinaryEvent | Kind::BinaryAck
            )
        {
            return None;
        }
        if matches!(kind, Kind::Ack | Kind::BinaryAck) && ack_id.is_none() {
            return None;
        }
        if kind == Kind::Disconnect && !rest.is_empty() {
            return None;
        }
        if matches!(
            kind,
            Kind::Event | Kind::Ack | Kind::BinaryEvent | Kind::BinaryAck
        ) && !rest.starts_with('[')
        {
            return None;
        }
        Some(Self {
            kind,
            namespace,
            ack_id,
            attachments,
            json: rest,
        })
    }

    /// 业务作用：生成标准文本包头并保留参数 JSON 原文，二进制附件由同一 writer 随后写出。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含 Engine.IO 类型字符的 Socket.IO 文本包；非法包头返回 None。
    pub fn encode(&self) -> Option<String> {
        let mut out = String::new();
        out.push((b'0' + self.kind as u8) as char);
        if matches!(self.kind, Kind::BinaryEvent | Kind::BinaryAck) {
            out.push_str(&self.attachments.to_string());
            out.push('-');
        } else if self.attachments != 0 {
            return None;
        }
        if self.namespace != "/" {
            out.push_str(self.namespace);
            out.push(',');
        }
        if let Some(id) = self.ack_id {
            out.push_str(&id.to_string());
        }
        out.push_str(self.json);
        let verified = Packet::decode(&out, self.attachments)?;
        if verified.kind != self.kind
            || verified.namespace != self.namespace
            || verified.ack_id != self.ack_id
        {
            return None;
        }
        Some(out)
    }
}

/// 业务作用：限定包头数字的规范十进制形式，防止溢出或非数字分隔造成不同解释。
///
/// 参数说明：
/// - `value`：包头中的数字子串。
///
/// 返回：规范无符号整数；空串、前导零、多余符号和溢出拒绝。
fn decimal(value: &str) -> Option<u64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}
