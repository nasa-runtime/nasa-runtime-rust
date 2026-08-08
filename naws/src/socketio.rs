// ============================================================================
// src/socketio.rs —— socket.io v5 / engine.io v4 兼容层(feature = "socketio")。
//
// 步骤 1(本文件):engine.io + socket.io 文本包编解码 + NASA 映射 helper(自包含)。
// 步骤 2(后续):ProtocolSelector(HTTP upgrade 看 ?EIO= 分流)+ 握手状态机 + 接入 WS pipeline。
//
// 协议参考(架构说明 / agent 调研):
//   engine.io 文本包:<type char><data>,type: 0 OPEN/1 CLOSE/2 PING/3 PONG/4 MESSAGE/5 UPGRADE/6 NOOP
//   socket.io 包(载于 engine.io MESSAGE 内):<type><[/ns,]><[ackId]><json>
//     type: 0 CONNECT/1 DISCONNECT/2 EVENT/3 ACK/4 CONNECT_ERROR/5 BINARY_EVENT/6 BINARY_ACK
//
// NASA 映射(对照 原实现 socketio):
//   engine.io 2(PING)→ NASA PING;1(CLOSE)→ CLOSE
//   sio 0(CONNECT,带 auth)→ NASA AUTH(token 取自 connect data 的 "token");1(DISCONNECT)→ CLOSE
//   sio 2(EVENT)→ NASA EVENT(event=args[0],payload=args[1] 的 JSON 字节);3(ACK)→ 丢弃(NASA 无 awaitAck)
//   出站:AUTH_RESP ok → sio CONNECT `40{"sid":..}`;ok=false → CONNECT_ERROR `44{"message":..}`
//        EVENT/BROADCAST → `42["event",payload]`;PONG → engine.io `3`;CLOSE → sio DISCONNECT `41`
// ============================================================================

use serde_json::Value;

/// engine.io 包类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineType {
    /// 建立 Engine.IO 会话并下发握手参数。
    Open = 0,
    /// 请求关闭当前 Engine.IO 会话。
    Close = 1,
    /// 发起心跳探测，要求对端返回 `Pong`。
    Ping = 2,
    /// 响应对端的 `Ping`，证明连接仍然存活。
    Pong = 3,
    /// 承载 Socket.IO 包或其它应用消息。
    Message = 4,
    /// 确认传输层升级已经完成。
    Upgrade = 5,
    /// 保持协议时序但不携带业务动作的空操作包。
    Noop = 6,
}

impl EngineType {
    /// 业务作用：从 char 构造结果；用于统一输入适配。
    ///
    /// # 参数
    /// - `c`: 当前解析到的字符或连接对象。
    fn from_char(c: char) -> Option<EngineType> {
        Some(match c {
            '0' => EngineType::Open,
            '1' => EngineType::Close,
            '2' => EngineType::Ping,
            '3' => EngineType::Pong,
            '4' => EngineType::Message,
            '5' => EngineType::Upgrade,
            '6' => EngineType::Noop,
            _ => return None,
        })
    }

    /// 业务作用：转换为 char 表示；用于对接下游接口。
    fn to_char(self) -> char {
        (b'0' + self as u8) as char
    }
}

/// engine.io 文本包:`<type><data>`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnginePacket {
    /// 决定 Engine.IO 帧控制语义的包类型。
    pub typ: EngineType,
    /// 类型字符之后的协议载荷；控制包允许为空。
    pub data: String,
}

impl EnginePacket {
    /// 业务作用：构造 engine.io 文本包。
    ///
    /// # 参数
    /// - `typ`: engine.io 包类型。
    /// - `data`: 包类型后的文本数据部分。
    pub fn new(typ: EngineType, data: impl Into<String>) -> Self {
        EnginePacket {
            typ,
            data: data.into(),
        }
    }

    /// 业务作用：编码 encode 数据；用于生成可传输或可存储的字节。
    pub fn encode(&self) -> String {
        format!("{}{}", self.typ.to_char(), self.data)
    }

    /// 业务作用：解码 engine.io 文本包。
    ///
    /// # 参数
    /// - `s`: 完整 engine.io 文本帧内容。
    pub fn decode(s: &str) -> Option<EnginePacket> {
        let mut chars = s.chars();
        let typ = EngineType::from_char(chars.next()?)?;
        Some(EnginePacket {
            typ,
            data: chars.as_str().to_string(),
        })
    }
}

/// socket.io 包类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SioType {
    /// 建立指定 namespace 的 Socket.IO 逻辑会话。
    Connect = 0,
    /// 断开指定 namespace 的 Socket.IO 逻辑会话。
    Disconnect = 1,
    /// 携带事件名与业务 JSON 参数。
    Event = 2,
    /// 响应带有 ack id 的事件。
    Ack = 3,
    /// 返回 namespace 建连或认证失败原因。
    ConnectError = 4,
}

impl SioType {
    /// 业务作用：从 char 构造结果；用于统一输入适配。
    ///
    /// # 参数
    /// - `c`: 当前解析到的字符或连接对象。
    fn from_char(c: char) -> Option<SioType> {
        Some(match c {
            '0' => SioType::Connect,
            '1' => SioType::Disconnect,
            '2' => SioType::Event,
            '3' => SioType::Ack,
            '4' => SioType::ConnectError,
            _ => return None,
        })
    }

    /// 业务作用：转换为 char 表示；用于对接下游接口。
    fn to_char(self) -> char {
        (b'0' + self as u8) as char
    }
}

/// socket.io 包:`<type>[/ns,][ackId]<json>`。默认 namespace "/" 省略。
#[derive(Debug, Clone, PartialEq)]
pub struct SioPacket {
    /// 决定 Socket.IO 包处理分支的协议类型。
    pub typ: SioType,
    /// 目标 namespace；根 namespace 使用 `/`。
    pub namespace: String,
    /// 事件确认标识；仅需要对端应答时存在。
    pub ack_id: Option<u64>,
    /// CONNECT、EVENT、ACK 或错误包携带的 JSON 数据。
    pub data: Value,
}

impl SioPacket {
    /// 业务作用：构造 socket.io 包。
    ///
    /// # 参数
    /// - `typ`: socket.io 包类型。
    /// - `data`: socket.io 包 JSON 数据体,`Value::Null` 表示无数据体。
    pub fn new(typ: SioType, data: Value) -> Self {
        SioPacket {
            typ,
            namespace: "/".into(),
            ack_id: None,
            data,
        }
    }

    /// 业务作用：编码 encode 数据；用于生成可传输或可存储的字节。
    pub fn encode(&self) -> String {
        let mut out = String::new();
        out.push(self.typ.to_char());
        // 非默认 namespace → "/ns,"
        if self.namespace != "/" {
            out.push_str(&self.namespace);
            out.push(',');
        }
        if let Some(id) = self.ack_id {
            out.push_str(&id.to_string());
        }
        if !self.data.is_null() {
            out.push_str(&self.data.to_string());
        }
        out
    }

    /// 业务作用：解码 socket.io 包。
    ///
    /// # 参数
    /// - `s`: engine.io MESSAGE 中承载的 socket.io 文本内容。
    pub fn decode(s: &str) -> Option<SioPacket> {
        let mut chars = s.char_indices();
        let (_, tc) = chars.next()?;
        let typ = SioType::from_char(tc)?;
        let rest = &s[tc.len_utf8()..];

        // namespace: 以 '/' 开头则读到 ','。
        let (namespace, rest) = if rest.starts_with('/') {
            match rest.find(',') {
                Some(idx) => (rest[..idx].to_string(), &rest[idx + 1..]),
                // "/ns" 无逗号无 data 也算合法(整段是 ns)
                None => (rest.to_string(), ""),
            }
        } else {
            ("/".to_string(), rest)
        };

        // ackId: 前导数字。
        let digits_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let (ack_id, rest) = if digits_end > 0 {
            (rest[..digits_end].parse::<u64>().ok(), &rest[digits_end..])
        } else {
            (None, rest)
        };

        // data: 剩余为 JSON(空则 Null)。
        let data = if rest.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(rest).ok()?
        };

        Some(SioPacket {
            typ,
            namespace,
            ack_id,
            data,
        })
    }
}

/* ============================== NASA 映射 helper(步骤 2 接入用)============================== */

/// 业务作用：engine.io OPEN 包(WS 连上后服务端首发)。
///
/// # 参数
/// - `sid`: 分配给 socket.io 连接的 session id。
/// - `ping_interval_ms`: 服务端发送 ping 的间隔毫秒数。
/// - `ping_timeout_ms`: 客户端 pong 超时时间毫秒数。
/// - `max_payload`: 单条 WebSocket message 的最大 payload 字节数。
pub fn engineio_open(
    sid: &str,
    ping_interval_ms: u64,
    ping_timeout_ms: u64,
    max_payload: usize,
) -> String {
    let json = serde_json::json!({
        "sid": sid,
        "upgrades": [],
        "pingInterval": ping_interval_ms,
        "pingTimeout": ping_timeout_ms,
        "maxPayload": max_payload,
    });
    EnginePacket::new(EngineType::Open, json.to_string()).encode()
}

/// 业务作用：engine.io PONG。
pub fn engineio_pong() -> String {
    EnginePacket::new(EngineType::Pong, "").encode()
}

/// 业务作用：socket.io CONNECT 成功(`40[/ns,]{"sid":..}`),在对应 namespace 上回应。
///
/// # 参数
/// - `sid`: 鉴权通过后的 session id。
/// - `namespace`: socket.io namespace,默认通常为 `/`。
pub fn sio_connect_ok(sid: &str, namespace: &str) -> String {
    let mut sio = SioPacket::new(SioType::Connect, serde_json::json!({ "sid": sid }));
    sio.namespace = namespace.to_string();
    EnginePacket::new(EngineType::Message, sio.encode()).encode()
}

/// 业务作用：socket.io CONNECT_ERROR(`44[/ns,]{"message":..}`)。
///
/// # 参数
/// - `message`: 连接失败原因,会写入 JSON `message` 字段。
/// - `namespace`: socket.io namespace,默认通常为 `/`。
pub fn sio_connect_error(message: &str, namespace: &str) -> String {
    let mut sio = SioPacket::new(
        SioType::ConnectError,
        serde_json::json!({ "message": message }),
    );
    sio.namespace = namespace.to_string();
    EnginePacket::new(EngineType::Message, sio.encode()).encode()
}

/// 业务作用：socket.io EVENT(`42["event", data]`),载于 engine.io MESSAGE。
///
/// # 参数
/// - `event`: socket.io 事件名。
/// - `data`: 事件第二个 JSON 参数,由 payload bridge 生成。
pub fn sio_event(event: &str, data: Value) -> String {
    let arr = Value::Array(vec![Value::String(event.to_string()), data]);
    let sio = SioPacket::new(SioType::Event, arr);
    EnginePacket::new(EngineType::Message, sio.encode()).encode()
}

/// 业务作用：engine.io PING(`2`):EIO v4 服务端发,客户端回 PONG。
pub fn engineio_ping() -> String {
    EnginePacket::new(EngineType::Ping, "").encode()
}

/// 业务作用：socket.io DISCONNECT(`41`)。
pub fn sio_disconnect() -> String {
    EnginePacket::new(
        EngineType::Message,
        SioPacket::new(SioType::Disconnect, Value::Null).encode(),
    )
    .encode()
}

/// 业务作用：socket.io ACK(`43<ackId><data>`):对带 ackId 的 EVENT 的应答,data 为返回参数数组。
///
/// # 参数
/// - `ack_id`: 客户端 EVENT 携带的 ack id。
/// - `data`: ACK 返回参数数组或 JSON 值。
pub fn sio_ack(ack_id: u64, data: Value) -> String {
    let mut sp = SioPacket::new(SioType::Ack, data);
    sp.ack_id = Some(ack_id);
    EnginePacket::new(EngineType::Message, sp.encode()).encode()
}

/// 解析一条 engine.io 文本帧 → 对框架的意图(供步骤 2 转成 NASA Frame)。
#[derive(Debug, Clone, PartialEq)]
pub enum Inbound {
    /// sio CONNECT,携带 auth token(取自 connect data 的 "token" 字段)。
    Auth {
        /// 交给框架认证链校验的客户端凭据。
        token: String,
        /// 客户端申请加入的 Socket.IO namespace。
        namespace: String,
    },
    /// sio EVENT:事件名 + 第二参数(opaque,作 NASA payload)+ 可选 ackId(带则需回 ACK)。
    Event {
        /// 用于路由业务处理器的事件名。
        event: String,
        /// 保持原始 JSON 语义的事件数据。
        data: Value,
        /// 客户端要求确认时携带的 ack id。
        ack_id: Option<u64>,
    },
    /// engine.io PING(EIO v3 客户端发;服务端回 PONG)。
    Ping,
    /// engine.io PONG(EIO v4 客户端对服务端 PING 的应答;刷新心跳)。
    Pong,
    /// 关闭(engine.io CLOSE 或 sio DISCONNECT)。
    Close,
    /// 其它(UPGRADE/NOOP/ACK/无法识别):忽略。
    Ignore,
}

/// 业务作用：解析 socket.io/engine.io 入站文本帧为框架意图。
///
/// # 参数
/// - `text`: WebSocket 收到的完整 engine.io 文本帧。
pub fn parse_inbound(text: &str) -> Inbound {
    let Some(ep) = EnginePacket::decode(text) else {
        return Inbound::Ignore;
    };
    match ep.typ {
        EngineType::Ping => Inbound::Ping,
        EngineType::Pong => Inbound::Pong,
        EngineType::Close => Inbound::Close,
        EngineType::Message => match SioPacket::decode(&ep.data) {
            Some(sp) => match sp.typ {
                SioType::Connect => {
                    // auth data 里取 token(socket.io v5 的 auth payload)
                    let token = sp
                        .data
                        .get("token")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    Inbound::Auth {
                        token,
                        namespace: sp.namespace,
                    }
                }
                SioType::Disconnect => Inbound::Close,
                SioType::Event => {
                    // data 形如 ["event", arg1, ...];取 event 名 + 第二个参数。
                    if let Value::Array(arr) = &sp.data {
                        let event = arr
                            .first()
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let data = arr.get(1).cloned().unwrap_or(Value::Null);
                        if event.is_empty() {
                            Inbound::Ignore
                        } else {
                            Inbound::Event {
                                event,
                                data,
                                ack_id: sp.ack_id,
                            }
                        }
                    } else {
                        Inbound::Ignore
                    }
                }
                // ACK / CONNECT_ERROR(入站不处理)
                _ => Inbound::Ignore,
            },
            None => Inbound::Ignore,
        },
        // OPEN/PONG/UPGRADE/NOOP 入站忽略
        _ => Inbound::Ignore,
    }
}
