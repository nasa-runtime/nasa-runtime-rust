//! 不依赖业务消息模型的有界 TCP/WebSocket 接入与字节写出。
//!
//! TCP 使用四字节大端帧体长度；WebSocket 使用标准消息边界。接收操作需要独占读取任务，
//! 中途取消意味着整个连接必须关闭，不能在已经消费部分输入后重试同一帧。
//! TCP 分段写出不复制业务体；当前 WebSocket 后端在组装消息时需要一次连续缓冲复制。

pub mod budget;

use bytes::{Bytes, BytesMut};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    time::{timeout_at, Instant},
};
use tokio_tungstenite::{
    tungstenite::{self, protocol::WebSocketConfig, Message},
    WebSocketStream,
};
pub use tungstenite::handshake::server::{ErrorResponse, Request, Response};

/// 协议入口容量；最大消息包含 TCP 长度前缀，不包含 WebSocket 自身帧头。
#[derive(Clone, Copy)]
pub struct Limits {
    /// 单条消息的字节上限；TCP 包含四字节长度前缀，WebSocket 不包含帧头。
    pub maximum_message: usize,
    /// 读取与重组一条消息的时长上限；控制帧不能延长未完成的重组期限。
    pub assembly_timeout: Duration,
    /// 单次写出、刷出或关闭操作的时长上限，避免慢连接永久占用 writer。
    pub write_timeout: Duration,
}

/// 底层传输事实，不表达应用是否已经认证或获得业务权限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// 以四字节大端长度前缀划分消息的原生 TCP 连接。
    Tcp,
    /// 已完成 HTTP Upgrade、按 WebSocket 消息边界收发的连接。
    WebSocket,
}

/// 传输接收结果，控制 ping/pong 与应用文本或二进制明确区分。
pub enum Inbound {
    /// 完整二进制消息；TCP 保留四字节长度前缀，供上层协议直接解析完整帧。
    Binary(Bytes),
    /// WebSocket 文本消息；内容已满足 UTF-8 要求。
    Text(String),
    /// WebSocket 探活请求，应用数据处理须与此控制消息分开。
    Ping(Bytes),
    /// WebSocket 探活响应，不代表业务消息处理成功。
    Pong(Bytes),
    /// 对端关闭或输入结束，此连接不能继续接收业务消息。
    Closed,
}

/// 同一 writer 顺序写出的完整消息；头和业务体可以共享各自的 Bytes。
#[derive(Clone)]
pub enum Outbound {
    /// 按头、正文顺序写出一条二进制消息；TCP 的长度前缀由调用方放入头部。
    Binary {
        /// 已编码的协议头；TCP 须包含长度前缀，写出时不重新序列化。
        header: Bytes,
        /// 可选原始业务体；TCP 分段写出，WebSocket 后端合并为连续消息。
        body: Option<Bytes>,
    },
    /// WebSocket 文本消息；TCP 传输拒绝此类型。
    Text(String),
    /// 刷出后端待发送数据，包括自动生成的 WebSocket 控制响应。
    Flush,
    /// 关闭当前传输写端；不表达业务消息已被对端确认。
    Close,
}

/// 一条已经完成传输协商的连接，应用负责其认证与准入。
pub struct Connection {
    io: ConnectionIo,
    limits: Limits,
    budget: Option<Arc<budget::Budget>>,
    stream_budget: Option<Arc<budget::StreamBudget>>,
}
enum ConnectionIo {
    Tcp(TcpStream),
    WebSocket(Box<WebSocketStream<budget::Stream>>),
}

/// 独占接收半部；不支持取消后继续解析同一连接。
pub struct Reader {
    io: ReadIo,
    limits: Limits,
    budget: Option<Arc<budget::Budget>>,
    stream_budget: Option<Arc<budget::StreamBudget>>,
}
enum ReadIo {
    Tcp(OwnedReadHalf),
    WebSocket(SplitStream<WebSocketStream<budget::Stream>>),
}

/// 独占写入半部；WebSocket 的自动控制帧与应用消息共用同一个后端状态。
pub struct Writer {
    io: WriteIo,
    limits: Limits,
}
enum WriteIo {
    Tcp(OwnedWriteHalf),
    WebSocket(SplitSink<WebSocketStream<budget::Stream>, Message>),
}

impl Limits {
    /// 业务作用：在任何输入分配前验证传输容量和截止预算，避免无限帧或零期限配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：有效的有界传输参数；不合法时不接管连接。
    fn validate(self) -> io::Result<()> {
        if !(4..=64 * 1024 * 1024).contains(&self.maximum_message)
            || self.assembly_timeout.is_zero()
            || self.write_timeout.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid transport limits",
            ));
        }
        Ok(())
    }
}

impl Connection {
    /// 业务作用：在原始接入截止时间内区分长度前缀 TCP 与 HTTP Upgrade，并执行应用握手策略。
    ///
    /// 参数说明：
    /// - `stream`：唯一 owner 移交的已接收 TCP 连接。
    /// - `limits`：分配、重组和写出预算。
    /// - `deadline`：从 accept 时确定的总截止点，协议识别不会重新计时。
    /// - `allow_tcp`：是否允许长度前缀 TCP 入口。
    /// - `upgrade`：检查路径、来源及子协议并填写响应的应用策略。
    ///
    /// 返回：协商完成的连接；错误、拒绝或超时会关闭输入连接。
    pub async fn accept<F>(
        stream: TcpStream,
        limits: Limits,
        deadline: Instant,
        allow_tcp: bool,
        upgrade: F,
    ) -> io::Result<Self>
    where
        F: FnOnce(&Request, Response) -> Result<Response, ErrorResponse> + Send + Unpin,
    {
        Self::accept_with_budget(stream, limits, deadline, allow_tcp, upgrade, None).await
    }

    /// 业务作用：在握手与重组分配之前启用共享预算，消息移交后仍由其全部字节视图持有容量。
    ///
    /// 参数说明：
    /// - `stream`：唯一 TCP 连接。
    /// - `limits`：固定帧、组装与写出边界。
    /// - `deadline`：原始认证总截止点。
    /// - `allow_tcp`：是否允许四字节长度前缀入口。
    /// - `upgrade`：应用的 WebSocket 握手策略。
    /// - `budget`：与其它连接共享的可选入站容量。
    ///
    /// 返回：使用预算时必须通过 Reader::receive_bytes 接收；失败关闭连接。
    pub async fn accept_with_budget<F>(
        stream: TcpStream,
        limits: Limits,
        deadline: Instant,
        allow_tcp: bool,
        upgrade: F,
        budget: Option<Arc<budget::Budget>>,
    ) -> io::Result<Self>
    where
        F: FnOnce(&Request, Response) -> Result<Response, ErrorResponse> + Send + Unpin,
    {
        limits.validate()?;
        let operation = async {
            let mut first = [0u8; 1];
            if stream.peek(&mut first).await? != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "closed before classification",
                ));
            }
            if first[0] == b'G' {
                let config = WebSocketConfig {
                    write_buffer_size: 0,
                    max_write_buffer_size: limits.maximum_message + 1024,
                    max_message_size: Some(limits.maximum_message),
                    max_frame_size: Some(limits.maximum_message),
                    accept_unmasked_frames: false,
                    ..Default::default()
                };
                let stream_budget = budget
                    .as_ref()
                    .map(|value| budget::StreamBudget::new(value.clone(), limits.maximum_message))
                    .transpose()?;
                let stream = budget::Stream::new(stream, stream_budget.clone());
                let ws =
                    tokio_tungstenite::accept_hdr_async_with_config(stream, upgrade, Some(config))
                        .await
                        .map_err(protocol_error)?;
                if let Some(state) = &stream_budget {
                    state.completed();
                }
                Ok(Self {
                    io: ConnectionIo::WebSocket(Box::new(ws)),
                    limits,
                    budget,
                    stream_budget,
                })
            } else if allow_tcp && first[0] <= 4 {
                Ok(Self {
                    io: ConnectionIo::Tcp(stream),
                    limits,
                    budget,
                    stream_budget: None,
                })
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported transport prefix",
                ))
            }
        };
        timeout_at(deadline, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "transport handshake deadline"))?
    }

    /// 业务作用：读取已经协商的传输类别，供应用选择固定解析 profile。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：TCP 或 WebSocket，不推测业务编码。
    pub fn kind(&self) -> Kind {
        match self.io {
            ConnectionIo::Tcp(_) => Kind::Tcp,
            ConnectionIo::WebSocket(_) => Kind::WebSocket,
        }
    }

    /// 业务作用：将接收与写出交给各自唯一任务，保持连接状态和后端自动控制帧的同步。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：拥有连接的读写半部，最后一个半部销毁时释放底层 socket。
    pub fn split(self) -> (Reader, Writer) {
        let (reader, writer) = match self.io {
            ConnectionIo::Tcp(stream) => {
                let (r, w) = stream.into_split();
                (ReadIo::Tcp(r), WriteIo::Tcp(w))
            }
            ConnectionIo::WebSocket(stream) => {
                let (w, r) = (*stream).split();
                (ReadIo::WebSocket(r), WriteIo::WebSocket(w))
            }
        };
        (
            Reader {
                io: reader,
                limits: self.limits,
                budget: self.budget,
                stream_budget: self.stream_budget,
            },
            Writer {
                io: writer,
                limits: self.limits,
            },
        )
    }
}

impl Reader {
    /// 业务作用：按完整消息边界接收有界字节，在读取帧长之后才分配已核验的容量。
    ///
    /// 参数说明：
    /// - `deadline`：调用者的认证或活性截止点；WebSocket 的等待和组装共用更短的组装预算。
    ///
    /// 返回：一条完整消息或关闭事实；中途超时后调用者必须销毁连接。
    pub async fn receive(&mut self, deadline: Instant) -> io::Result<Inbound> {
        if self.budget.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "budgeted transport requires receive_bytes",
            ));
        }
        let maximum = self.limits.maximum_message;
        let deadline = if matches!(self.io, ReadIo::WebSocket(_)) {
            deadline.min(Instant::now() + self.limits.assembly_timeout)
        } else {
            deadline
        };
        let operation = async {
            match &mut self.io {
                ReadIo::Tcp(stream) => {
                    let mut prefix = [0u8; 4];
                    if stream.read(&mut prefix[..1]).await? == 0 {
                        return Ok(Inbound::Closed);
                    }
                    // 首字节到达后固定总组装截止点，持续碎片不能无限延长占用。
                    let assembled = async {
                        stream.read_exact(&mut prefix[1..]).await?;
                        let size = u32::from_be_bytes(prefix) as usize;
                        if size > maximum - 4 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "frame exceeds capacity",
                            ));
                        }
                        let mut bytes = BytesMut::with_capacity(size + 4);
                        bytes.extend_from_slice(&prefix);
                        bytes.resize(size + 4, 0);
                        stream.read_exact(&mut bytes[4..]).await?;
                        Ok(Inbound::Binary(bytes.freeze()))
                    };
                    tokio::time::timeout(self.limits.assembly_timeout, assembled)
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "frame assembly deadline")
                        })?
                }
                ReadIo::WebSocket(stream) => match stream.next().await {
                    None => Ok(Inbound::Closed),
                    Some(Err(error)) => Err(protocol_error(error)),
                    Some(Ok(Message::Binary(bytes))) => Ok(Inbound::Binary(bytes.into())),
                    Some(Ok(Message::Text(text))) => Ok(Inbound::Text(text)),
                    Some(Ok(Message::Ping(bytes))) => Ok(Inbound::Ping(bytes.into())),
                    Some(Ok(Message::Pong(bytes))) => Ok(Inbound::Pong(bytes.into())),
                    Some(Ok(Message::Close(_))) => Ok(Inbound::Closed),
                    Some(Ok(Message::Frame(_))) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected raw frame",
                    )),
                },
            }
        };
        timeout_at(deadline, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "receive deadline"))?
    }
}

impl Writer {
    /// 业务作用：在统一截止预算内顺序完成整条消息，TCP 直接使用原始共享字节段。
    ///
    /// 参数说明：
    /// - `message`：调用者已授权的完整消息或传输控制动作。
    /// - `deadline`：调用者的投递期限，不能由写出重试延长。
    ///
    /// 返回：后端写出完成；失败后调用者必须关闭连接，不重写半条消息。
    pub async fn send(&mut self, message: Outbound, deadline: Instant) -> io::Result<()> {
        let maximum = self.limits.maximum_message;
        let deadline = deadline.min(Instant::now() + self.limits.write_timeout);
        let operation = async {
            match (&mut self.io, message) {
                (
                    WriteIo::Tcp(stream),
                    Outbound::Binary {
                        header,
                        body,
                    },
                ) => {
                    if header
                        .len()
                        .saturating_add(body.as_ref().map_or(0, Bytes::len))
                        > maximum
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "outbound capacity",
                        ));
                    }
                    stream.write_all(&header).await?;
                    if let Some(body) = body {
                        stream.write_all(&body).await?;
                    }
                    Ok(())
                }
                (WriteIo::Tcp(stream), Outbound::Close) => stream.shutdown().await,
                (WriteIo::Tcp(stream), Outbound::Flush) => stream.flush().await,
                (WriteIo::Tcp(_), Outbound::Text(_)) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TCP requires binary framing",
                )),
                (WriteIo::WebSocket(stream), message) => {
                    let message = match message {
                        Outbound::Binary {
                            header,
                            body,
                        } => {
                            let length = header
                                .len()
                                .saturating_add(body.as_ref().map_or(0, Bytes::len));
                            if length > maximum {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "outbound capacity",
                                ));
                            }
                            let mut bytes = Vec::with_capacity(length);
                            bytes.extend_from_slice(&header);
                            if let Some(body) = body {
                                bytes.extend_from_slice(&body);
                            }
                            Message::Binary(bytes)
                        }
                        Outbound::Text(text) => {
                            if text.len() > maximum {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "outbound capacity",
                                ));
                            }
                            Message::Text(text)
                        }
                        Outbound::Close => Message::Close(None),
                        Outbound::Flush => return stream.flush().await.map_err(protocol_error),
                    };
                    stream.send(message).await.map_err(protocol_error)
                }
            }
        };
        timeout_at(deadline, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write deadline"))?
    }
}

/// 业务作用：将协议后端错误映射为传输失败，不回显输入帧内容或认证材料。
///
/// 参数说明：
/// - `_error`：后端协议错误，仅用于确定失败分支。
///
/// 返回：固定描述的 InvalidData 错误。
fn protocol_error(_error: tungstenite::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "WebSocket protocol failure")
}
