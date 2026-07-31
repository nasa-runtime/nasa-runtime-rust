// ============================================================================
// src/client.rs —— Rust 异步客户端 SDK(R0.5)。
// 连接 + AUTH 握手 + 收发(event/uid/group/endpoint)+ 自动心跳 + 回调。
// 兼作协议与压测的 Rust 侧收发工具。架构说明(R0.5)。
// 设计同服务端:writer task 独占 mpsc outbox;reader task 解帧分派到 handler。
// ============================================================================

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;

use naws_proto::{AuthRequest, AuthResponse, Message, Mode, WireCodec};

use crate::wire::{encode_frame, frame_type, ping, wire_mode, FrameCodec};

/// 客户端 builder 保持非 fallible；所有进入 Tokio timer 的外部时长在 build 时收敛到该上限。
const MAX_CLIENT_RUNTIME_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 保证计时器既不会因零间隔忙循环，也不会因极端时长溢出。
fn bounded_client_duration(duration: Duration) -> Duration {
    duration.clamp(Duration::from_millis(1), MAX_CLIENT_RUNTIME_DURATION)
}

/// 客户端连接、握手、协议和断开状态错误。
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// TCP 建连或读写阶段发生系统 IO 错误。
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// 建连或鉴权响应等待超过 `connect_timeout`。
    #[error("connect timeout")]
    Timeout,
    /// 收到不符合状态机或编码约定的协议帧。
    #[error("protocol error: {0}")]
    Protocol(String),
    /// 服务端明确拒绝鉴权。
    #[error("auth failed: {0}")]
    AuthFailed(String),
    /// 连接已关闭或主动 close 取消了当前操作。
    #[error("disconnected")]
    Disconnected,
}

/// 收到 EVENT/BROADCAST 时按 event 名回调,拿到整条 Message(含 fromUid/group)。
pub type EventCallback = Arc<dyn Fn(Message) + Send + Sync>;
/// 客户端连接断开后的通知回调。
pub type DisconnectCallback = Arc<dyn Fn() + Send + Sync>;

/// 保存内部共享状态；用于在多个调用路径之间复用数据。
struct Inner {
    addr: String,
    endpoint: String,
    token: String,
    device_id: String,
    version: String,
    auto_heartbeat: bool,
    connect_timeout: Duration,
    max_frame: usize,
    handlers: Mutex<HashMap<String, EventCallback>>,
    on_disconnect: Mutex<Option<DisconnectCallback>>,
    outbox: Mutex<Option<mpsc::Sender<Bytes>>>,
    session_id: Mutex<Option<String>>,
    connected: AtomicBool,
    auto_reconnect: bool,
    reconnect_min: Duration,
    reconnect_max: Duration,
    /// close() 置位:停止自动重连。
    closed: AtomicBool,
    /// connect() 运行门禁:已在运行则拒绝再次 connect(防多 supervisor 互相覆盖 outbox)。
    running: AtomicBool,
    /// 本次连接的取消令牌(**level-triggered**):close() 取消它,read_loop/establish/退避 select 之即退。
    /// 用 CancellationToken 而非 Notify:即便 close() 早于 read_loop 开始 select 也不丢唤醒
    /// ——connect 成功后立即 close_and_wait 不再永久卡住。每次 connect() 换新令牌。
    cancel: Mutex<CancellationToken>,
    /// supervisor 退出(running→false)时通知:供 `wait_closed`/`close_and_wait` 做确定性重连屏障。
    running_done: tokio::sync::Notify,
}

impl Inner {
    /// 释放运行门禁并**唤醒** `wait_closed` 等待者。所有 `running→false` 的出口(首连失败、
    /// 握手期被 close、supervisor 退出)都必须走它,否则 `close_and_wait` 会丢唤醒永久挂起
    ///。
    fn release_running(&self) {
        self.running.store(false, Ordering::Release);
        self.running_done.notify_waiters();
    }

    /// 当前连接的取消令牌克隆(level-triggered;读循环/握手/退避据此即时退出)。
    fn cancel_token(&self) -> CancellationToken {
        self.cancel.lock().unwrap().clone()
    }
}

/// 维护客户端连接状态；用于发送消息、订阅事件和管理重连。
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

/// 保存客户端构造参数；用于逐步配置连接、鉴权和重连策略。
pub struct ClientBuilder {
    inner: Inner,
}

impl Client {
    /// 创建客户端 builder。
    ///
    /// # 参数
    /// - `addr`: TCP 服务端地址,例如 `127.0.0.1:19091`。
    pub fn builder(addr: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            inner: Inner {
                addr: addr.into(),
                endpoint: "/ws".into(),
                token: String::new(),
                device_id: String::new(),
                version: "1.0".into(),
                auto_heartbeat: true,
                connect_timeout: Duration::from_secs(5),
                max_frame: 16 * 1024 * 1024,
                handlers: Mutex::new(HashMap::new()),
                on_disconnect: Mutex::new(None),
                outbox: Mutex::new(None),
                session_id: Mutex::new(None),
                connected: AtomicBool::new(false),
                auto_reconnect: false,
                reconnect_min: Duration::from_millis(200),
                reconnect_max: Duration::from_secs(5),
                closed: AtomicBool::new(false),
                running: AtomicBool::new(false),
                cancel: Mutex::new(CancellationToken::new()),
                running_done: tokio::sync::Notify::new(),
            },
        }
    }

    /// 返回当前对象的 connected 状态。
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    /// 读取 session id 状态；用于向调用方暴露当前运行信息。
    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id.lock().unwrap().clone()
    }

    /// 连接 + AUTH 握手。成功后后台 reader + heartbeat task 已启动。
    /// auto_reconnect 开启时:**首连成功后**若断开会自动重连(首连失败仍返回 Err,由调用方决定)。
    pub async fn connect(&self) -> Result<AuthResponse, ClientError> {
        // 运行门禁:已在运行 → 拒绝(防多 supervisor 同时跑、互相覆盖 outbox)。
        if self
            .inner
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ClientError::Protocol("client already connected".into()));
        }
        self.inner.closed.store(false, Ordering::Release);
        // 本次连接换一枚全新取消令牌(上一枚可能已被 close 取消,CancellationToken 不能复活)。
        *self.inner.cancel.lock().unwrap() = CancellationToken::new();
        let (resp, framed) = match establish_once(self.inner.clone()).await {
            Ok(v) => v,
            Err(e) => {
                // 首连失败/被 close 取消 → 释放门禁**并唤醒** wait_closed(否则永久挂起;R7 P1)。
                self.inner.release_running();
                return Err(e);
            }
        };
        // 握手成功后、spawn supervisor 前再查 closed:若并发 close 已发生,这里直接收尾,
        // 避免"close 的通知早于新 supervisor 开始等待"导致 wait_closed 丢唤醒。
        if self.inner.closed.load(Ordering::Acquire) {
            *self.inner.outbox.lock().unwrap() = None;
            self.inner.connected.store(false, Ordering::Release);
            self.inner.release_running();
            return Err(ClientError::Disconnected);
        }
        // 单个 supervisor task:跑读循环,断开后(如开了 auto_reconnect)在其内部 loop 重连。
        tokio::spawn(supervisor(self.inner.clone(), framed));
        Ok(resp)
    }

    /// 注册某事件的接收回调(可在 connect 前或后调)。
    ///
    /// # 参数
    /// - `event`: 客户端需要监听的业务事件名,匹配服务端下发的 `Message.events`。
    /// - `f`: 客户端收到该事件消息时执行的业务回调。
    pub fn on_event<F>(&self, event: impl Into<String>, f: F)
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        self.inner
            .handlers
            .lock()
            .unwrap()
            .insert(event.into(), Arc::new(f));
    }

    /// 注册断开连接回调；用于连接关闭后通知调用方清理状态。
    ///
    /// # 参数
    /// - `f`: 客户端连接断开后执行的业务回调。
    pub fn on_disconnect<F>(&self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.inner.on_disconnect.lock().unwrap() = Some(Arc::new(f));
    }

    /* ============================== 发送 ============================== */

    /// 发完整 Message(BITPACK)。返回是否成功入队。
    ///
    /// # 参数
    /// - `msg`: 已组装好的业务消息信封,会按 BITPACK_TLV 编码成 EVENT frame。
    pub fn send_message(&self, msg: &Message) -> bool {
        let payload = match msg.encode(Mode::BitpackTlv) {
            Ok(p) => p,
            Err(_) => return false,
        };
        let frame = encode_frame(frame_type::EVENT, wire_mode::BITPACK, &payload);
        match self.inner.outbox.lock().unwrap().as_ref() {
            Some(tx) => tx.try_send(frame).is_ok(),
            None => false,
        }
    }

    /// 业务事件(无路由,仅触发服务端 handler)。
    ///
    /// # 参数
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    pub fn send(&self, event: &str, payload: &[u8]) -> bool {
        self.send_message(&base(event, payload))
    }

    /// 私聊:发给指定 uid 的所有 session。
    ///
    /// # 参数
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    /// - `uids`: 目标用户 ID 列表,服务端会投递到这些用户的所有 session。
    pub fn send_to_uid(&self, event: &str, payload: &[u8], uids: &[&str]) -> bool {
        let mut m = base(event, payload);
        m.uids = Some(uids.iter().map(|u| Some(u.to_string())).collect());
        self.send_message(&m)
    }

    /// 群聊:发给群成员。
    ///
    /// # 参数
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    /// - `group`: 目标群组名。
    pub fn send_to_group(&self, event: &str, payload: &[u8], group: &str) -> bool {
        let mut m = base(event, payload);
        m.group = Some(group.to_string());
        self.send_message(&m)
    }

    /// 端点级广播。
    ///
    /// # 参数
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    /// - `endpoints`: 目标 endpoint 路径列表。
    pub fn send_to_endpoint(&self, event: &str, payload: &[u8], endpoints: &[&str]) -> bool {
        let mut m = base(event, payload);
        m.routers = Some(endpoints.iter().map(|e| Some(e.to_string())).collect());
        self.send_message(&m)
    }

    /// 主动关闭(**异步触发**,不阻塞):停自动重连 + 丢 outbox(writer 退出)+ 唤醒读循环/退避。
    /// 注意:运行门禁(`running`)由 supervisor 退出时的 RAII guard 复位,**不是**本函数同步清——
    /// 否则旧 supervisor 与新 connect 会互相覆盖状态。因此 close() 后**立刻** connect()
    /// 可能短暂返回 "already connected",直到旧 supervisor 收尾。`is_connected()` **不是** join 屏障
    /// (它在 close() 里被同步清,但 supervisor 仍在收尾)——要确定性重连请用 `close_and_wait()`
    /// 或 `wait_closed()` 等门禁真正释放后再 connect。
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        *self.inner.outbox.lock().unwrap() = None;
        self.inner.connected.store(false, Ordering::Release);
        self.inner.cancel.lock().unwrap().cancel(); // level-triggered:即便 read_loop 尚未 select 也不丢
    }

    /// 等 supervisor 真正退出(运行门禁 `running` 释放)。此后 `connect()` 不会再返回
    /// "already connected"。未在运行则立即返回。
    pub async fn wait_closed(&self) {
        loop {
            // 先登记 notified 再查门禁,避免「查到 running=true → 漏掉 guard 的 notify → 永久挂起」。
            let notified = self.inner.running_done.notified();
            if !self.inner.running.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// 关闭并等待 supervisor 收尾——确定性重连屏障:`close_and_wait().await` 后即可安全 `connect()`。
    pub async fn close_and_wait(&self) {
        self.close();
        self.wait_closed().await;
    }
}

impl ClientBuilder {
    /// 设置客户端连接的 endpoint。
    ///
    /// # 参数
    /// - `ep`: 鉴权请求携带的 endpoint,默认 `/ws`。
    pub fn endpoint(mut self, ep: impl Into<String>) -> Self {
        self.inner.endpoint = ep.into();
        self
    }

    /// 设置客户端鉴权令牌；用于握手阶段携带身份信息。
    ///
    /// # 参数
    /// - `t`: 业务鉴权 token,会写入 AuthRequest。
    pub fn token(mut self, t: impl Into<String>) -> Self {
        self.inner.token = t.into();
        self
    }

    /// 设置客户端设备标识；用于服务端区分同一用户的不同终端。
    ///
    /// # 参数
    /// - `d`: 设备 ID,会写入 AuthRequest。
    pub fn device_id(mut self, d: impl Into<String>) -> Self {
        self.inner.device_id = d.into();
        self
    }

    /// 设置客户端版本号；用于握手和兼容性判断。
    ///
    /// # 参数
    /// - `v`: 客户端版本号,会写入 AuthRequest。
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.inner.version = v.into();
        self
    }

    /// 设置是否自动发送心跳；用于保持长连接活跃。
    ///
    /// # 参数
    /// - `on`: `true` 表示握手成功后由客户端定时发送 PING。
    pub fn auto_heartbeat(mut self, on: bool) -> Self {
        self.inner.auto_heartbeat = on;
        self
    }

    /// 设置连接超时时间；用于限制建立连接的等待窗口。
    ///
    /// # 参数
    /// - `d`: TCP 连接和 AUTH 首次握手的超时时间。
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.inner.connect_timeout = d;
        self
    }

    /// 开启断线自动重连(指数退避 min..max)。
    ///
    /// # 参数
    /// - `on`: `true` 表示首连成功后的断线由 supervisor 自动重连。
    pub fn auto_reconnect(mut self, on: bool) -> Self {
        self.inner.auto_reconnect = on;
        self
    }

    /// 设置重连退避区间；用于控制断线重连频率。
    ///
    /// # 参数
    /// - `min`: 首次或最小重连退避时间。
    /// - `max`: 指数退避增长后的最大等待时间。
    pub fn reconnect_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.inner.reconnect_min = min;
        self.inner.reconnect_max = max;
        self
    }

    /// 完成 builder 装配并返回可运行对象。
    pub fn build(mut self) -> Client {
        self.inner.connect_timeout = bounded_client_duration(self.inner.connect_timeout);
        self.inner.reconnect_min = bounded_client_duration(self.inner.reconnect_min);
        self.inner.reconnect_max =
            bounded_client_duration(self.inner.reconnect_max).max(self.inner.reconnect_min);
        Client {
            inner: Arc::new(self.inner),
        }
    }
}

/* ============================== 后台 task ============================== */

/// 写入 writer task 内容；用于输出数据或持久化状态。
///
/// # 参数
/// - `wh`: TCP 写半边。
/// - `rx`: 后台任务接收消息的通道。
async fn writer_task(mut wh: OwnedWriteHalf, mut rx: mpsc::Receiver<Bytes>) {
    while let Some(bytes) = rx.recv().await {
        if wh.write_all(&bytes).await.is_err() {
            break;
        }
    }
    let _ = wh.shutdown().await;
}

type ClientFramed = FramedRead<tokio::net::tcp::OwnedReadHalf, FrameCodec>;

/// 连接 + AUTH 握手 + 启动 writer/heartbeat。返回握手结果 + 读半边(由 supervisor 跑读循环)。
/// **不** spawn 读任务(避免 async 递归),供 connect() 与 supervisor 重连共用。
///
/// # 参数
/// - `inner`: 已解析的内部值或被包装对象。
async fn establish_once(inner: Arc<Inner>) -> Result<(AuthResponse, ClientFramed), ClientError> {
    let cancel = inner.cancel_token();
    // TCP connect 也可被 close() 取消:否则 close 最坏要等满 connect_timeout。
    let stream = tokio::select! {
        s = tokio::time::timeout(inner.connect_timeout, TcpStream::connect(&inner.addr)) => {
            s.map_err(|_| ClientError::Timeout)??
        }
        _ = cancel.cancelled() => return Err(ClientError::Disconnected),
    };
    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = mpsc::channel::<Bytes>(256);
    tokio::spawn(writer_task(write_half, rx));
    let mut framed = FramedRead::new(read_half, FrameCodec::new(inner.max_frame));

    let req = AuthRequest {
        token: Some(inner.token.clone()),
        endpoint: Some(inner.endpoint.clone()),
        device_id: opt(&inner.device_id),
        version: opt(&inner.version),
        ..Default::default()
    };
    tx.send(encode_frame(
        frame_type::AUTH,
        wire_mode::VARINT,
        &req.encode(Mode::VarintTlv).unwrap(),
    ))
    .await
    .map_err(|_| ClientError::Disconnected)?;

    // 等 AUTH_RESP:受 connect_timeout 约束,且可被 close() 取消。
    let frame = tokio::select! {
        f = tokio::time::timeout(inner.connect_timeout, framed.next()) => {
            f.map_err(|_| ClientError::Timeout)?.ok_or(ClientError::Disconnected)??
        }
        _ = cancel.cancelled() => return Err(ClientError::Disconnected),
    };
    if frame.typ != frame_type::AUTH_RESP {
        return Err(ClientError::Protocol(format!(
            "expected AUTH_RESP, got 0x{:02x}",
            frame.typ
        )));
    }
    // 控制帧固定 VARINT:不接受任意 mode 的 AUTH_RESP。
    if frame.mode != wire_mode::VARINT {
        return Err(ClientError::Protocol(format!(
            "AUTH_RESP must be VARINT mode, got 0x{:02x}",
            frame.mode
        )));
    }
    let resp = AuthResponse::decode(Mode::VarintTlv, &frame.payload)
        .map_err(|e| ClientError::Protocol(format!("bad AUTH_RESP: {e}")))?;
    if !resp.ok {
        return Err(ClientError::AuthFailed(
            resp.err_msg.clone().unwrap_or_default(),
        ));
    }

    *inner.outbox.lock().unwrap() = Some(tx.clone());
    *inner.session_id.lock().unwrap() = resp.session_id.clone();
    inner.connected.store(true, Ordering::Release);
    if inner.auto_heartbeat {
        let period = heartbeat_period(resp.heartbeat_timeout_ms);
        tokio::spawn(heartbeat_task(tx, period, inner.clone()));
    }
    Ok((resp, framed))
}

/// 读循环:解帧分派;连接结束(EOF/CLOSE/错误)或 close() 唤醒即返回。
///
/// # 参数
/// - `framed`: 已建立连接的 frame 读取器。
/// - `inner`: 已解析的内部值或被包装对象。
async fn read_loop(mut framed: ClientFramed, inner: &Arc<Inner>) {
    let cancel = inner.cancel_token();
    loop {
        let item = tokio::select! {
            it = framed.next() => it,
            _ = cancel.cancelled() => break, // close() → level-triggered,即便先于此 select 也不丢
        };
        let Some(item) = item else { break };
        let frame = match item {
            Ok(f) => f,
            Err(_) => break,
        };
        match frame.typ {
            frame_type::EVENT | frame_type::BROADCAST => {
                let Some(mode) = Mode::from_ordinal(frame.mode) else {
                    continue;
                };
                let Ok(msg) = Message::decode(mode, &frame.payload) else {
                    continue;
                };
                dispatch(inner, msg);
            }
            // 控制帧校验:PONG 必须 VARINT + 空 payload;非法即视为协议错误断开。
            frame_type::PONG if frame.mode != wire_mode::VARINT || !frame.payload.is_empty() => {
                tracing::warn!("client: malformed PONG, closing");
                break;
            }
            frame_type::PONG => {}
            // CLOSE:校验 VARINT + 合法 CloseReason(无论如何都要断开,但校验防静默吞畸形)。
            frame_type::CLOSE => {
                if frame.mode != wire_mode::VARINT
                    || naws_proto::CloseReason::decode(Mode::VarintTlv, &frame.payload).is_err()
                {
                    tracing::warn!("client: malformed CLOSE");
                }
                break;
            }
            _ => {}
        }
    }
}

/// supervisor 退出守卫:任何退出路径(正常 / panic unwind)都复位状态,防 running 永久卡死
/// (on_disconnect panic 曾使 supervisor 提前 unwind、漏掉 running=false)。
struct SupervisorGuard {
    inner: Arc<Inner>,
}
impl Drop for SupervisorGuard {
    /// 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        self.inner.connected.store(false, Ordering::Release);
        *self.inner.outbox.lock().unwrap() = None;
        self.inner.release_running();
    }
}

/// 连接 supervisor(每客户端一个,无递归):跑读循环;断开后按需退避重连。
///
/// # 参数
/// - `inner`: 已解析的内部值或被包装对象。
/// - `framed`: 已建立连接的 frame 读取器。
async fn supervisor(inner: Arc<Inner>, framed: ClientFramed) {
    let _guard = SupervisorGuard {
        inner: inner.clone(),
    };
    read_loop(framed, &inner).await;
    on_disconnect_cleanup(&inner);

    let cancel = inner.cancel_token();
    let mut backoff = inner.reconnect_min;
    while inner.auto_reconnect && !inner.closed.load(Ordering::Acquire) {
        // 退避 sleep 可被 close() 取消 → close 后最坏不必等到 reconnect_max。
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = cancel.cancelled() => break,
        }
        if inner.closed.load(Ordering::Acquire) {
            break;
        }
        match establish_once(inner.clone()).await {
            Ok((_, framed)) => {
                backoff = inner.reconnect_min;
                read_loop(framed, &inner).await;
                on_disconnect_cleanup(&inner);
            }
            Err(e) => {
                tracing::debug!("reconnect failed: {e}; retry in {backoff:?}");
                backoff = (backoff * 2).min(inner.reconnect_max);
            }
        }
    }
    // _guard drop:复位 connected/outbox/running(任何路径,含 panic)。
}

/// 执行断连清理；用于关闭会话状态并触发回调。
///
/// # 参数
/// - `inner`: 已解析的内部值或被包装对象。
fn on_disconnect_cleanup(inner: &Arc<Inner>) {
    inner.connected.store(false, Ordering::Release);
    *inner.outbox.lock().unwrap() = None;
    *inner.session_id.lock().unwrap() = None; // 断连清旧 session_id,避免误用
    let cb = inner.on_disconnect.lock().unwrap().clone();
    if let Some(cb) = cb {
        // 断连回调也做 panic 隔离(事件回调已隔离,这里补上)——否则 panic 掀掉 supervisor。
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb())).is_err() {
            tracing::error!("client on_disconnect callback panicked, isolated");
        }
    }
}

/// 分发收到的消息；用于按事件名调用注册的处理器。
///
/// # 参数
/// - `inner`: 已解析的内部值或被包装对象。
/// - `msg`: 业务消息体或事件载荷。
fn dispatch(inner: &Arc<Inner>, msg: Message) {
    let Some(events) = &msg.events else { return };
    // 先把命中的回调 clone 出来再**释放锁**,避免回调内再注册 handler 造成自锁死。
    let cbs: Vec<EventCallback> = {
        let handlers = inner.handlers.lock().unwrap();
        events
            .iter()
            .flatten()
            .filter_map(|ev| handlers.get(ev).cloned())
            .collect()
    };
    for cb in cbs {
        // 回调 panic 隔离:不让用户回调 panic 掀掉 supervisor、跳过断线清理。
        let m = msg.clone();
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(m))).is_err() {
            tracing::error!("client event callback panicked, isolated");
        }
    }
}

/// 运行客户端心跳任务；用于定期发送 ping 保持连接。
///
/// # 参数
/// - `tx`: 后台任务发送消息的通道或事务句柄。
/// - `period`: 任务调度周期。
/// - `inner`: 已解析的内部值或被包装对象。
async fn heartbeat_task(tx: mpsc::Sender<Bytes>, period: Duration, inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if !inner.connected.load(Ordering::Acquire) {
            break;
        }
        if tx.try_send(ping()).is_err() {
            break;
        }
    }
}

/* ============================== helpers ============================== */

/// 构造基础消息帧；用于统一客户端外发事件格式。
///
/// # 参数
/// - `event`: 客户端要发送到 endpoint 的业务事件名。
/// - `payload`: 业务事件的原始 payload 字节。
fn base(event: &str, payload: &[u8]) -> Message {
    Message {
        events: Some(vec![Some(event.to_string())]),
        payload: Some(payload.to_vec()),
        ..Default::default()
    }
}

/// 把空字符串转换为空选项；用于清理握手参数。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
fn opt(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// 心跳周期 = 服务端 timeout 的一半(至少 1s,缺省 30s)。
///
/// # 参数
/// - `timeout_ms`: 超时时间毫秒数。
fn heartbeat_period(timeout_ms: i64) -> Duration {
    if timeout_ms <= 0 {
        Duration::from_secs(30)
    } else {
        Duration::from_millis((timeout_ms as u64 / 2).max(1000)).min(MAX_CLIENT_RUNTIME_DURATION)
    }
}
