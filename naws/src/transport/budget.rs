//! 可选共享入站字节预算；只管理传输缓冲和拥有的字节，不包含应用身份或路由。

use bytes::Bytes;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// 共享硬上限；容量随实际 owner 归还，零不表示无限。
pub struct Budget {
    capacity: usize,
    available: Arc<Semaphore>,
}

struct OwnedBytes {
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for OwnedBytes {
    /// 业务作用：暴露原缓冲的借用视图，共享切片不复制正文也不提前归还预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 owner 同寿命的原始字节。
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Budget {
    /// 业务作用：创建可由多个传输共享的有限入站容量。
    ///
    /// 参数说明：
    /// - `capacity`：可保留的字节预算，范围为 1 到 1 GiB。
    ///
    /// 返回：共享预算；零值和过大容量拒绝。
    pub fn new(capacity: usize) -> io::Result<Arc<Self>> {
        if !(1..=1024 * 1024 * 1024).contains(&capacity) {
            return Err(invalid("invalid receive budget"));
        }
        Ok(Arc::new(Self {
            capacity,
            available: Arc::new(Semaphore::new(capacity)),
        }))
    }
    /// 业务作用：提供仍被传输缓冲或已移交消息持有的预算事实。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前已用预算，不等同于进程 RSS。
    pub fn used_bytes(&self) -> usize {
        self.capacity - self.available.available_permits()
    }
    /// 业务作用：取得启动时固定的共享容量，供宿主展示资源边界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置的总字节数。
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    /// 业务作用：在分配或读取之前取得容量，饱和时不排队保留连接解析状态。
    ///
    /// 参数说明：
    /// - `amount`：本次必须持有的字节数。
    ///
    /// 返回：随 owner 释放的许可；预算不足时立即拒绝。
    pub(super) fn reserve(&self, amount: usize) -> io::Result<OwnedSemaphorePermit> {
        let amount = u32::try_from(amount).map_err(|_| invalid("receive budget capacity"))?;
        self.available
            .clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "receive budget exhausted"))
    }
    /// 业务作用：把借用输入交给异步 owner 前先取得容量，避免预算不足时仍分配正文。
    ///
    /// 参数说明：
    /// - `bytes`：本次同步调用期间有效的原始字节。
    ///
    /// 返回：进行一次拥有型复制的受管 Bytes；容量不足时不复制输入。
    pub fn copy_from_slice(&self, bytes: &[u8]) -> io::Result<Bytes> {
        let permit = self.reserve(bytes.len())?;
        Ok(Self::owned(Bytes::copy_from_slice(bytes), permit))
    }
    /// 业务作用：把已有拥有型缓冲纳入共享预算，切片和 clone 继续承担原分配责任。
    ///
    /// 参数说明：
    /// - `bytes`：不再由其它独立 owner 负责计费的原始字节。
    /// - `retained_capacity`：底层分配容量，不得小于可见长度。
    ///
    /// 返回：零正文复制的受管 Bytes；预算不足时拒绝并销毁输入 owner。
    pub fn retain(&self, bytes: Bytes, retained_capacity: usize) -> io::Result<Bytes> {
        if retained_capacity < bytes.len() {
            return Err(invalid("receive allocation capacity"));
        }
        let permit = self.reserve(retained_capacity)?;
        Ok(Self::owned(bytes, permit))
    }
    /// 业务作用：移交已经在分配前取得的容量，避免同一 TCP 分配重复计费。
    ///
    /// 参数说明：
    /// - `bytes`：容量已经覆盖的原始缓冲。
    /// - `permit`：随全部共享视图保留的预算。
    ///
    /// 返回：最后一个视图销毁时才归还容量的 Bytes。
    pub(super) fn owned(bytes: Bytes, permit: OwnedSemaphorePermit) -> Bytes {
        Bytes::from_owner(OwnedBytes {
            bytes,
            _permit: permit,
        })
    }
}

/// 文本也使用拥有型 UTF-8 字节，避免 String 逃逸后失去预算责任。
pub enum Received {
    /// 完整二进制消息；字节共享视图继续持有入站预算。
    Binary(Bytes),
    /// 已验证 UTF-8 的文本字节；使用 Bytes 保留预算 owner。
    Text(Bytes),
    /// WebSocket 探活请求的受管内容，不属于应用消息正文。
    Ping(Bytes),
    /// WebSocket 探活响应的受管内容，不代表业务投递回执。
    Pong(Bytes),
    /// 输入已经结束，没有需要继续持有的新消息字节。
    Closed,
}

struct State {
    wire_bytes: usize,
    reserved: usize,
    permit: OwnedSemaphorePermit,
    handshake: bool,
    started: Option<Instant>,
}

/// 后端保留的缓冲按连接高水位计费，不能在消息返回时假定 Vec 已缩容。
pub(super) struct StreamBudget {
    pub(super) budget: Arc<Budget>,
    maximum: usize,
    state: Mutex<State>,
}
impl StreamBudget {
    /// 业务作用：在 WebSocket 后端建立读取缓冲前预留固定开销。
    ///
    /// 参数说明：
    /// - `budget`：跨连接共享的内存容量。
    /// - `maximum`：完整消息的最大字节数。
    ///
    /// 返回：握手和后续重组共用的实际 owner；固定容量不足时拒绝。
    pub(super) fn new(budget: Arc<Budget>, maximum: usize) -> io::Result<Arc<Self>> {
        let permit = budget.reserve(16384)?;
        Ok(Arc::new(Self {
            budget,
            maximum,
            state: Mutex::new(State {
                wire_bytes: 0,
                reserved: 16384,
                permit,
                handshake: true,
                started: None,
            }),
        }))
    }
    /// 业务作用：在后端读取更多字节前覆盖缓冲增长、重组及临时复制的保守上界。
    ///
    /// 参数说明：
    /// - `amount`：本次最多读取的字节数，不超过 4096。
    ///
    /// 返回：容量已覆盖；超出握手或消息线长限制时关闭连接。
    fn prepare(&self, amount: usize) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let next = state
            .wire_bytes
            .checked_add(amount)
            .ok_or_else(|| invalid("receive wire capacity"))?;
        let limit = if state.handshake {
            65536
        } else {
            self.maximum.saturating_mul(2).saturating_add(65536)
        };
        if next > limit {
            return Err(invalid("receive wire capacity"));
        }
        // 后端的输入 Vec、帧复制和分片 Vec 可同时存在；高水位保留到两个 I/O owner 均退出。
        let required = 16384 + next * 8;
        if required > state.reserved {
            let extra = self.budget.reserve(required - state.reserved)?;
            state.permit.merge(extra);
            state.reserved = required;
        }
        Ok(())
    }
    /// 业务作用：记录真实读入量，WouldBlock 不制造新的组装期限。
    ///
    /// 参数说明：
    /// - `amount`：实际取得的字节数。
    ///
    /// 返回：非空输入建立本次组装起点。
    fn read(&self, amount: usize) {
        if amount == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.wire_bytes += amount;
        state.started.get_or_insert_with(Instant::now);
    }
    /// 业务作用：完整消息结束后开始新的线长窗口，同时覆盖后端可能预读的一个固定块。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：清除组装计时；实际分配高水位继续保留，不解除内存责任。
    pub(super) fn completed(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.handshake = false;
        state.wire_bytes = 4096;
        state.started = None;
    }
    /// 业务作用：让控制帧不能刷新尚未结束的消息重组期限。
    ///
    /// 参数说明：
    /// - `timeout`：固定消息组装时长。
    ///
    /// 返回：已有输入采用原始起点，无输入采用当前等待上限。
    pub(super) fn deadline(&self, timeout: std::time::Duration) -> Instant {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .started
            .unwrap_or_else(Instant::now)
            + timeout
    }
}

pub(super) struct Stream {
    socket: TcpStream,
    budget: Option<Arc<StreamBudget>>,
}
impl Stream {
    /// 业务作用：为相同 WebSocket 后端选择有界或既有读取路径，不改变写入协议。
    ///
    /// 参数说明：
    /// - `socket`：唯一物理连接。
    /// - `budget`：显式启用的共享读取责任。
    ///
    /// 返回：可供标准握手和帧后端使用的异步流。
    pub(super) fn new(socket: TcpStream, budget: Option<Arc<StreamBudget>>) -> Self {
        Self {
            socket,
            budget,
        }
    }
}
impl AsyncRead for Stream {
    /// 业务作用：在后端可能扩展缓冲之前取得容量，空闲连接不预留最大业务帧。
    ///
    /// 参数说明：
    /// - `cx`：运行时读取唤醒上下文。
    /// - `output`：后端提供的有限读取目标。
    ///
    /// 返回：真实读取状态；预算耗尽返回错误，调用者必须关闭连接。
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(budget) = &this.budget else {
            return Pin::new(&mut this.socket).poll_read(cx, output);
        };
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            match this.socket.poll_read_ready(cx) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
            let amount = output.remaining().min(4096);
            if let Err(error) = budget.prepare(amount) {
                return Poll::Ready(Err(error));
            }
            let target = output.initialize_unfilled_to(amount);
            match this.socket.try_read(target) {
                Ok(read) => {
                    budget.read(read);
                    output.advance(read);
                    return Poll::Ready(Ok(()));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }
}
impl AsyncWrite for Stream {
    /// 业务作用：保持原始 socket 写入行为，入站预算不会被用来授予或阻塞业务写出权限。
    ///
    /// 参数说明：
    /// - `cx`：运行时写入唤醒上下文。
    /// - `buffer`：后端已经构造的输出字节。
    ///
    /// 返回：底层实际写入长度或错误。
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().socket).poll_write(cx, buffer)
    }
    /// 业务作用：沿用底层输出刷新，不改变读取预算的生命周期。
    ///
    /// 参数说明：
    /// - `cx`：运行时刷新上下文。
    ///
    /// 返回：底层刷新状态。
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }
    /// 业务作用：结束物理写入方向，读取缓冲仍由最后一个 owner 负责释放。
    ///
    /// 参数说明：
    /// - `cx`：运行时关闭上下文。
    ///
    /// 返回：底层关闭状态。
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}
/// 业务作用：使用固定描述拒绝容量及协议配置，不回显网络输入。
///
/// 参数说明：
/// - `message`：调用点声明的固定错误分类。
///
/// 返回：无输入内容的 InvalidData 错误。
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl super::Reader {
    /// 业务作用：接收随共享字节视图持有预算的完整消息，TCP 在分配前计费，WebSocket 读入前覆盖重组容量。
    ///
    /// 参数说明：
    /// - `deadline`：调用方原始认证或活性截止点，控制帧不刷新未完成组装的起点。
    ///
    /// 返回：拥有型消息；中途取消、超时或容量不足后必须销毁连接，不能重试半条输入。
    pub async fn receive_bytes(&mut self, deadline: Instant) -> io::Result<Received> {
        use futures::StreamExt;
        use tokio::io::AsyncReadExt;
        use tokio_tungstenite::tungstenite::Message;
        let budget = self
            .budget
            .clone()
            .ok_or_else(|| invalid("receive budget required"))?;
        let maximum = self.limits.maximum_message;
        let assembly = self.limits.assembly_timeout;
        let deadline = match &self.stream_budget {
            Some(state) => deadline.min(state.deadline(assembly)),
            None => deadline,
        };
        let operation = async {
            match &mut self.io {
                super::ReadIo::Tcp(stream) => {
                    let mut prefix = [0u8; 4];
                    if stream.read(&mut prefix[..1]).await? == 0 {
                        return Ok(Received::Closed);
                    }
                    let assembled = async {
                        stream.read_exact(&mut prefix[1..]).await?;
                        let length = u32::from_be_bytes(prefix) as usize;
                        if length > maximum - 4 {
                            return Err(invalid("frame exceeds capacity"));
                        }
                        // 声明长度必须先通过共享预约，不能在预算不足时先分配再等待。
                        let permit = budget.reserve(length + 4)?;
                        let mut bytes = vec![0u8; length + 4];
                        bytes[..4].copy_from_slice(&prefix);
                        stream.read_exact(&mut bytes[4..]).await?;
                        Ok(Received::Binary(Budget::owned(bytes.into(), permit)))
                    };
                    tokio::time::timeout(assembly, assembled)
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "frame assembly deadline")
                        })?
                }
                super::ReadIo::WebSocket(stream) => {
                    let state = self
                        .stream_budget
                        .as_ref()
                        .ok_or_else(|| invalid("stream budget required"))?;
                    let message = match stream.next().await {
                        None => return Ok(Received::Closed),
                        Some(Err(error)) => return Err(super::protocol_error(error)),
                        Some(Ok(message)) => message,
                    };
                    let (kind, data) = match message {
                        Message::Binary(bytes) => (0, bytes),
                        Message::Text(text) => (1, text.into_bytes()),
                        Message::Ping(bytes) => (2, bytes),
                        Message::Pong(bytes) => (3, bytes),
                        Message::Close(_) => return Ok(Received::Closed),
                        Message::Frame(_) => return Err(invalid("unexpected raw frame")),
                    };
                    let capacity = data.capacity();
                    // 返回消息离开后端后可以被任意长时间保留，必须独立计费而不借用下一次读取的容量。
                    let data = budget.retain(data.into(), capacity)?;
                    if kind <= 1 {
                        state.completed();
                    }
                    Ok(match kind {
                        0 => Received::Binary(data),
                        1 => Received::Text(data),
                        2 => Received::Ping(data),
                        _ => Received::Pong(data),
                    })
                }
            }
        };
        tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "receive deadline"))?
    }
}
