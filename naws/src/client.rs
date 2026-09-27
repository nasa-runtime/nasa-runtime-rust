// ============================================================================
// src/client.rs —— Rust 异步客户端 SDK。
// 连接 + AUTH 握手 + 收发(event/uid/group/endpoint)+ 自动心跳 + 回调。
// 设计同服务端:writer task 独占 mpsc outbox;reader task 解帧分派到 handler。
// ============================================================================

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// 业务作用：保证计时器既不会因零间隔忙循环，也不会因极端时长溢出。
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

/// 客户端固定失败类别，不包含对端地址、会话或认证材料。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientFailure {
    /// TCP 或握手超时等建连失败。
    Connect,
    /// 对端明确拒绝身份材料。
    Authentication,
    /// 对端关闭帧或读半边结束。
    Disconnected,
    /// 控制帧不符合当前协议状态。
    Protocol,
    /// 已认证连接的读取或帧解码失败。
    Read,
    /// 已认证连接的帧写出失败。
    Write,
    /// 未在心跳期限内收到 PONG。
    Heartbeat,
    /// 接收缓冲或发送队列无法接纳必要工作。
    Capacity,
    /// 同步业务回调发生已隔离的 panic。
    Callback,
}
/// 客户端本地证据；时间只表示最近事件距采样时刻的间隔，不代表远端业务健康。
#[derive(Debug, Clone)]
pub struct ClientObservation {
    /// 本代 owner 尚未取得全部子任务退出证据。
    pub running: bool,
    /// 当前存在认证成功的连接，不等于远端业务健康。
    pub connected: bool,
    /// supervisor、writer、heartbeat 的存活数量。
    pub active_tasks: usize,
    /// 已隔离的业务回调 panic 总次数。
    pub callback_failures: usize,
    /// 最近认证成功的间隔，关闭后仍保留该历史事实。
    pub last_authenticated_ago: Option<Duration>,
    /// 当前认证连接的最近 PONG 间隔，新连接认证时清空。
    pub last_pong_ago: Option<Duration>,
    /// 最近一次失败的固定类别，重新连接不会抹除历史失败。
    pub last_failure: Option<ClientFailure>,
    /// 最近失败距本次采样的间隔。
    pub last_failure_ago: Option<Duration>,
}

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
    queue_capacity: usize,
    events_ready: CancellationToken,
    active_tasks: AtomicUsize,
    callback_failures: AtomicUsize,
    last_authenticated: Mutex<Option<tokio::time::Instant>>,
    last_pong: Mutex<Option<tokio::time::Instant>>,
    last_failure: Mutex<Option<(ClientFailure, tokio::time::Instant)>>,
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
    /// 业务作用：撤销本地认证连接，与宿主最终接流裁决串行化。
    /// 参数说明：`closing` 表示同时禁止自动重连。
    /// 返回：发送队列与连接事实同时撤销；未观察到的远端故障仍由协议任务报告。
    fn invalidate_connection(&self, closing: bool) {
        let mut outbox = self.outbox.lock().unwrap();
        if closing {
            self.closed.store(true, Ordering::Release);
        }
        self.connected.store(false, Ordering::Release);
        *outbox = None;
    }
    /// 业务作用：释放运行门禁并**唤醒** `wait_closed` 等待者。所有 `running→false` 的出口(首连失败、
    /// 握手期被 close、supervisor 退出)都必须走它,否则 `close_and_wait` 会丢唤醒永久挂起
    ///。
    fn release_running(&self) {
        self.running.store(false, Ordering::Release);
        self.running_done.notify_waiters();
    }
    /// 业务作用：记录固定类别的最近失败，供本地观测区分连接与业务回调证据。
    /// 参数说明：`failure` 为不携带秘密材料的类别。
    /// 返回：替换最近失败事实，不创建任务或触发重试。
    fn record_failure(&self, failure: ClientFailure) {
        *self.last_failure.lock().unwrap() = Some((failure, tokio::time::Instant::now()));
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
    /// 业务作用：在认证连接仍有效的本地保护范围内完成宿主接流裁决。
    /// 参数说明：`publish` 必须是短小同步操作，不阻塞、不重入本客户端、不执行业务回调。
    /// 返回：连接已认证且 owner 和发送队列仍有效时执行并返回 Some，否则不执行；不证明远端此刻存活。
    pub fn with_authenticated_connection<T>(&self, publish: impl FnOnce() -> T) -> Option<T> {
        let outbox = self.inner.outbox.lock().unwrap();
        // 已关闭、失去认证或发送任务退出时证据不再完整，必须保持宿主接流屏障关闭。
        if self.inner.closed.load(Ordering::Acquire)
            || !self.inner.connected.load(Ordering::Acquire)
            || !self.inner.running.load(Ordering::Acquire)
            || outbox.as_ref().is_none_or(|sender| sender.is_closed())
        {
            return None;
        }
        Some(publish())
    }
    /// 业务作用：创建客户端 builder。
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
                queue_capacity: 256,
                events_ready: {
                    let ready = CancellationToken::new();
                    ready.cancel();
                    ready
                },
                active_tasks: AtomicUsize::new(0),
                callback_failures: AtomicUsize::new(0),
                last_authenticated: Mutex::new(None),
                last_pong: Mutex::new(None),
                last_failure: Mutex::new(None),
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

    /// 业务作用：返回当前对象的 connected 状态。
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    /// 业务作用：读取 session id 状态；用于向调用方暴露当前运行信息。
    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id.lock().unwrap().clone()
    }

    /// 业务作用：建立本代连接 owner 并等待首次认证，首连成功后才允许自动重连。
    /// 参数说明：无。
    /// 返回：认证响应或连接错误；取消等待会取消本代，运行门禁在全部子任务退出后释放。
    pub async fn connect(&self) -> Result<AuthResponse, ClientError> {
        let cancel = {
            let mut token = self.inner.cancel.lock().unwrap();
            // 门禁与取消令牌一起更换，close 不会误取消上一代而遗漏正在创建的新代。
            if self
                .inner
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(ClientError::Protocol("client already connected".into()));
            }
            self.inner.closed.store(false, Ordering::Release);
            *token = CancellationToken::new();
            token.clone()
        };
        let mut guard = ConnectWaitGuard(Some(cancel.clone()));
        let (answer, receiver) = tokio::sync::oneshot::channel();
        let inner = self.inner.clone();
        let run = Arc::new(ClientRun {
            inner: inner.clone(),
            cancel: cancel.clone(),
            completion: Mutex::new(RunCompletion {
                owner_exited: false,
                released: false,
            }),
        });
        let mut owner_guard = ClientOwner {
            run: run.clone(),
            completed: false,
        };
        let tasks = tokio_util::task::TaskTracker::new();
        let worker_inner = inner.clone();
        let worker_cancel = cancel.clone();
        let worker_tasks = tasks.clone();
        inner.active_tasks.fetch_add(1, Ordering::AcqRel);
        // 计数守卫随 future 一起移交，任务首次轮询前被取消也必须归还计数。
        let worker_active = ActiveClientTask(run.clone());
        let worker = tokio::spawn(async move {
            let _active = worker_active;
            let _stop = worker_cancel.clone().drop_guard();
            supervisor(worker_inner, worker_cancel, worker_tasks, answer, run).await;
        });
        // 首次 await 前已有独立 owner；它等待 supervisor 以及被 supervisor 创建的所有任务。
        tokio::spawn(async move {
            let worker_completed = worker.await.is_ok();
            cancel.cancel();
            tasks.close();
            tasks.wait().await;
            owner_guard.completed = worker_completed;
            drop(owner_guard);
        });
        let result = receiver.await.map_err(|_| ClientError::Disconnected)?;
        if result.is_ok() {
            guard.0 = None;
        } else {
            self.wait_closed().await;
        }
        result
    }

    /// 业务作用：分别读取连接、认证、PONG 与失败事实，不把任务存活视为远端可用。
    /// 参数说明：无。
    /// 返回：有界本地快照；事件时间与计数不是跨字段原子采样。
    pub fn observation(&self) -> ClientObservation {
        let failure = *self.inner.last_failure.lock().unwrap();
        ClientObservation {
            running: self.is_running(),
            connected: self.is_connected(),
            active_tasks: self.active_tasks(),
            callback_failures: self.callback_failures(),
            last_authenticated_ago: self
                .inner
                .last_authenticated
                .lock()
                .unwrap()
                .map(|at| at.elapsed()),
            last_pong_ago: self.inner.last_pong.lock().unwrap().map(|at| at.elapsed()),
            last_failure: failure.map(|(kind, _)| kind),
            last_failure_ago: failure.map(|(_, at)| at.elapsed()),
        }
    }

    /// 业务作用：宿主放行后允许分发准备期间有界缓冲的业务事件。
    /// 参数说明：无。
    /// 返回：开放业务回调；不会重新建立已关闭连接。
    pub fn activate(&self) {
        self.inner.events_ready.cancel();
    }

    /// 业务作用：观察连接 owner 是否仍持有运行责任。
    /// 参数说明：无。
    /// 返回：只有全部子任务退出后才为 false。
    pub fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::Acquire)
    }

    /// 业务作用：观察本客户端仍存活的 supervisor、writer 和 heartbeat 数。
    /// 参数说明：无。
    /// 返回：任务计数，不以 connected 标志代替退出证据。
    pub fn active_tasks(&self) -> usize {
        self.inner.active_tasks.load(Ordering::Acquire)
    }

    /// 业务作用：提供固定维度的业务回调异常累计值。
    /// 参数说明：无。
    /// 返回：被隔离的 callback panic 次数，不包含业务消息内容。
    pub fn callback_failures(&self) -> usize {
        self.inner.callback_failures.load(Ordering::Acquire)
    }

    /// 业务作用：注册某事件的接收回调(可在 connect 前或后调)。
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

    /// 业务作用：注册断开连接回调；用于连接关闭后通知调用方清理状态。
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

    /// 业务作用：将合法业务信封编码后提交给当前连接的有界发送队列。
    /// 参数说明：`msg` 是完整消息信封，按 BITPACK_TLV 编码为 EVENT 帧。
    /// 返回：true 仅证明本地入队；未激活、关闭、帧超限或队列已满均拒绝，不承诺远端执行。
    pub fn send_message(&self, msg: &Message) -> bool {
        if self.inner.closed.load(Ordering::Acquire) || !self.inner.events_ready.is_cancelled() {
            return false;
        }
        // 先检查已存在的材料字节，拒绝明显超限消息，避免为关闭或超量发送复制大正文。
        let mut material = msg.payload.as_ref().map_or(0, Vec::len);
        for value in [&msg.from_uid, &msg.from_client, &msg.group]
            .into_iter()
            .flatten()
        {
            material = material.saturating_add(value.len());
        }
        for values in [&msg.events, &msg.routers, &msg.uids, &msg.excludes]
            .into_iter()
            .flatten()
        {
            material = material.saturating_add(values.len());
            for value in values.iter().flatten() {
                material = material.saturating_add(value.len());
            }
        }
        if material > self.inner.max_frame {
            return false;
        }
        let payload = match msg.encode(Mode::BitpackTlv) {
            Ok(p) => p,
            Err(_) => return false,
        };
        if payload.len().saturating_add(2) > self.inner.max_frame
            || self.inner.closed.load(Ordering::Acquire)
            || !self.inner.events_ready.is_cancelled()
        {
            return false;
        }
        let frame = encode_frame(frame_type::EVENT, wire_mode::BITPACK, &payload);
        match self.inner.outbox.lock().unwrap().as_ref() {
            Some(tx) if !self.inner.closed.load(Ordering::Acquire) => tx.try_send(frame).is_ok(),
            Some(_) => false,
            None => false,
        }
    }

    /// 业务作用：业务事件(无路由,仅触发服务端 handler)。
    ///
    /// # 参数
    /// - `event`: 业务事件名。
    /// - `payload`: 事件 payload 字节。
    pub fn send(&self, event: &str, payload: &[u8]) -> bool {
        self.send_message(&base(event, payload))
    }

    /// 业务作用：私聊:发给指定 uid 的所有 session。
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

    /// 业务作用：群聊:发给群成员。
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

    /// 业务作用：端点级广播。
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

    /// 业务作用：主动关闭(**异步触发**,不阻塞):停自动重连 + 丢 outbox(writer 退出)+ 唤醒读循环/退避。
    /// 注意:运行门禁(`running`)由关闭 owner 在全部连接任务退出后复位,**不是**本函数同步清——
    /// 否则旧 supervisor 与新 connect 会互相覆盖状态。因此 close() 后**立刻** connect()
    /// 可能短暂返回 "already connected",直到旧 supervisor 收尾。`is_connected()` **不是** join 屏障
    /// (它在 close() 里被同步清,但 supervisor 仍在收尾)——要确定性重连请用 `close_and_wait()`
    /// 或 `wait_closed()` 等门禁真正释放后再 connect。
    /// 参数说明：无。
    /// 返回：同步撤销发送准入并通知本代取消，实际退出仍需等待连接 owner。
    pub fn close(&self) {
        let token = self.inner.cancel.lock().unwrap();
        self.inner.invalidate_connection(true);
        token.cancel();
    }

    /// 业务作用：等 supervisor、writer 与 heartbeat 全部退出并释放运行门禁。此后 `connect()` 不会再返回
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

    /// 业务作用：关闭并等待全部连接任务收尾——确定性重连屏障:`close_and_wait().await` 后即可安全 `connect()`。
    pub async fn close_and_wait(&self) {
        self.close();
        self.wait_closed().await;
    }
}

impl ClientBuilder {
    /// 业务作用：设置客户端连接的 endpoint。
    ///
    /// # 参数
    /// - `ep`: 鉴权请求携带的 endpoint,默认 `/ws`。
    pub fn endpoint(mut self, ep: impl Into<String>) -> Self {
        self.inner.endpoint = ep.into();
        self
    }

    /// 业务作用：设置客户端鉴权令牌；用于握手阶段携带身份信息。
    ///
    /// # 参数
    /// - `t`: 业务鉴权 token,会写入 AuthRequest。
    pub fn token(mut self, t: impl Into<String>) -> Self {
        self.inner.token = t.into();
        self
    }

    /// 业务作用：设置客户端设备标识；用于服务端区分同一用户的不同终端。
    ///
    /// # 参数
    /// - `d`: 设备 ID,会写入 AuthRequest。
    pub fn device_id(mut self, d: impl Into<String>) -> Self {
        self.inner.device_id = d.into();
        self
    }

    /// 业务作用：设置客户端版本号；用于握手和兼容性判断。
    ///
    /// # 参数
    /// - `v`: 客户端版本号,会写入 AuthRequest。
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.inner.version = v.into();
        self
    }

    /// 业务作用：设置是否自动发送心跳；用于保持长连接活跃。
    ///
    /// # 参数
    /// - `on`: `true` 表示握手成功后由客户端定时发送 PING。
    pub fn auto_heartbeat(mut self, on: bool) -> Self {
        self.inner.auto_heartbeat = on;
        self
    }

    /// 业务作用：设置连接超时时间；用于限制建立连接的等待窗口。
    ///
    /// # 参数
    /// - `d`: TCP 连接和 AUTH 首次握手的超时时间。
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.inner.connect_timeout = d;
        self
    }

    /// 业务作用：开启断线自动重连(指数退避 min..max)。
    ///
    /// # 参数
    /// - `on`: `true` 表示首连成功后的断线由 supervisor 自动重连。
    pub fn auto_reconnect(mut self, on: bool) -> Self {
        self.inner.auto_reconnect = on;
        self
    }

    /// 业务作用：设置重连退避区间；用于控制断线重连频率。
    ///
    /// # 参数
    /// - `min`: 首次或最小重连退避时间。
    /// - `max`: 指数退避增长后的最大等待时间。
    pub fn reconnect_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.inner.reconnect_min = min;
        self.inner.reconnect_max = max;
        self
    }

    /// 业务作用：限制出站队列和单帧大小，使长连接资源可纳入宿主容量预算。
    /// 参数说明：`queue_capacity` 为待发送帧数；`max_frame_bytes` 为单帧正文上限。
    /// 返回：容量收敛为有限正值的 builder。
    pub fn capacity(mut self, queue_capacity: usize, max_frame_bytes: usize) -> Self {
        self.inner.queue_capacity = queue_capacity.clamp(1, 4096);
        self.inner.max_frame = max_frame_bytes.clamp(1, 16 * 1024 * 1024);
        self
    }

    /// 业务作用：让认证后事件等待宿主激活，期间保持协议控制帧处理。
    /// 参数说明：无。
    /// 返回：带有界事件缓冲的 builder；缓冲耗尽时断开，不提前调用业务。
    pub fn start_suspended(mut self) -> Self {
        self.inner.events_ready = CancellationToken::new();
        self
    }

    /// 业务作用：让发送和业务回调等待调用方统一发布启动许可，协议控制帧仍可处理。
    /// 参数说明：`ready` 的取消表示许可发布；共享许可时仅由宿主取消，不逐项调用 activate。
    /// 返回：使用指定屏障的 builder，许可发布前的事件仍受缓冲容量限制。
    pub fn activation_barrier(mut self, ready: CancellationToken) -> Self {
        self.inner.events_ready = ready;
        self
    }

    /// 业务作用：完成 builder 装配并返回可运行对象。
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

/// 等待方取消只取消本代，门禁仍由独立 owner 在子任务退出后释放。
struct ConnectWaitGuard(Option<CancellationToken>);
impl Drop for ConnectWaitGuard {
    /// 业务作用：首连等待被丢弃时撤销本代连接请求。
    /// 参数说明：无。
    /// 返回：触发取消，不直接释放运行门禁。
    fn drop(&mut self) {
        if let Some(token) = &self.0 {
            token.cancel();
        }
    }
}
struct RunCompletion {
    owner_exited: bool,
    released: bool,
}
struct ClientRun {
    inner: Arc<Inner>,
    cancel: CancellationToken,
    completion: Mutex<RunCompletion>,
}
impl ClientRun {
    /// 业务作用：收尾 owner 和全部连接任务归还责任后才释放本代运行门禁。
    /// 参数说明：无。
    /// 返回：执行器销毁与正常退出均可完成本地收口，旧任务不能覆盖下一代连接状态。
    fn finish_if_idle(&self) {
        let mut completion = self.completion.lock().unwrap();
        if !completion.owner_exited
            || completion.released
            || self.inner.active_tasks.load(Ordering::Acquire) != 0
        {
            return;
        }
        completion.released = true;
        // 准入已关闭且没有存活任务，先撤销旧会话再允许新的 connect 取得运行权。
        self.inner.invalidate_connection(false);
        *self.inner.session_id.lock().unwrap() = None;
        self.inner.release_running();
    }
}
struct ClientOwner {
    run: Arc<ClientRun>,
    completed: bool,
}
impl Drop for ClientOwner {
    /// 业务作用：收尾任务完成或被执行器销毁时关闭本代发送，并保留子任务的退出责任。
    /// 参数说明：无。
    /// 返回：最后一个任务归还后释放门禁，异常销毁记录连接中断，不宣称消息送达。
    fn drop(&mut self) {
        self.run.inner.invalidate_connection(true);
        self.run.cancel.cancel();
        if !self.completed {
            self.run.inner.record_failure(ClientFailure::Disconnected);
        }
        self.run.completion.lock().unwrap().owner_exited = true;
        self.run.finish_if_idle();
    }
}
struct ActiveClientTask(Arc<ClientRun>);
impl Drop for ActiveClientTask {
    /// 业务作用：任务实际退出时归还计数，panic 路径同样可观察。
    /// 参数说明：无。
    /// 返回：减少本代存活任务计数；最后一个任务归还且收尾 owner 已退出后才解除门禁。
    fn drop(&mut self) {
        // 任意连接任务失去运行责任都会撤销认证事实；先取得同一发布锁，避免接流裁决使用旧证据。
        self.0.inner.invalidate_connection(false);
        if self.0.inner.active_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.finish_if_idle();
        }
    }
}

/// 业务作用：串行写出本连接的帧，连接取消时立即释放写半边。
/// 参数说明：`wh` 是独占写半边；`rx` 是有界队列；`cancel` 仅属于本次连接；`inner` 留存失败事实。
/// 返回：关闭或写失败后结束；已入队帧不提升为远端确认。
async fn writer_task(
    mut wh: OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
    cancel: CancellationToken,
    inner: Arc<Inner>,
) {
    let _stop = cancel.clone().drop_guard();
    loop {
        let bytes = tokio::select! {biased; _=cancel.cancelled()=>break, item=rx.recv()=>match item {Some(bytes)=>bytes,None=>break}};
        tokio::select! {biased; _=cancel.cancelled()=>break, result=wh.write_all(&bytes)=>if result.is_err(){inner.record_failure(ClientFailure::Write);break}}
    }
}

type ClientFramed = FramedRead<tokio::net::tcp::OwnedReadHalf, FrameCodec>;
struct ConnectionSession {
    framed: ClientFramed,
    cancel: CancellationToken,
    handles: Vec<tokio::task::JoinHandle<()>>,
    pong: Arc<Mutex<tokio::time::Instant>>,
}
impl ConnectionSession {
    /// 业务作用：旧连接子任务退出后才允许重连发布新通道。
    /// 参数说明：无。
    /// 返回：writer 和 heartbeat 均取得 join 证据。
    async fn stop(&mut self) {
        self.cancel.cancel();
        for handle in self.handles.drain(..) {
            let _ = handle.await;
        }
    }
}
impl Drop for ConnectionSession {
    /// 业务作用：异常离开连接时撤销本连接任务，顶层 tracker 继续保留等待责任。
    /// 参数说明：无。
    /// 返回：请求停止剩余子任务，不发布新连接状态。
    fn drop(&mut self) {
        self.cancel.cancel();
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// 业务作用：在同一启动截止点内完成 TCP 与认证，再创建本连接的 writer/heartbeat。
/// 参数说明：`inner` 为固定参数；`run_cancel` 为本次运行令牌；`tasks` 跟踪所有子任务；`run` 保留本代退出责任。
/// 返回：已认证会话；失败前不发布 outbox，握手取消时直接释放两半连接。
async fn establish_once(
    inner: Arc<Inner>,
    run_cancel: &CancellationToken,
    tasks: &tokio_util::task::TaskTracker,
    run: &Arc<ClientRun>,
) -> Result<(AuthResponse, ConnectionSession), ClientError> {
    let deadline = tokio::time::Instant::now() + inner.connect_timeout;
    let stream = tokio::select! {biased; _=run_cancel.cancelled()=>return Err(ClientError::Disconnected), result=tokio::time::timeout_at(deadline,TcpStream::connect(&inner.addr))=>result.map_err(|_|ClientError::Timeout)??};
    let (read_half, mut write_half) = stream.into_split();
    let mut framed = FramedRead::new(read_half, FrameCodec::new(inner.max_frame));
    let req = AuthRequest {
        token: Some(inner.token.clone()),
        endpoint: Some(inner.endpoint.clone()),
        device_id: opt(&inner.device_id),
        version: opt(&inner.version),
        ..Default::default()
    };
    let payload = req
        .encode(Mode::VarintTlv)
        .map_err(|_| ClientError::Protocol("invalid auth request".into()))?;
    let auth = encode_frame(frame_type::AUTH, wire_mode::VARINT, &payload);
    // 握手阶段直接持有写半边，取消 future 不留下尚未纳入 owner 的 writer。
    tokio::select! {biased; _=run_cancel.cancelled()=>return Err(ClientError::Disconnected), result=tokio::time::timeout_at(deadline,write_half.write_all(&auth))=>result.map_err(|_|ClientError::Timeout)??};
    let frame = tokio::select! {biased; _=run_cancel.cancelled()=>return Err(ClientError::Disconnected), result=tokio::time::timeout_at(deadline,framed.next())=>result.map_err(|_|ClientError::Timeout)?.ok_or(ClientError::Disconnected)??};
    if frame.typ != frame_type::AUTH_RESP || frame.mode != wire_mode::VARINT {
        return Err(ClientError::Protocol("invalid AUTH_RESP frame".into()));
    }
    let resp = AuthResponse::decode(Mode::VarintTlv, &frame.payload)
        .map_err(|_| ClientError::Protocol("invalid AUTH_RESP payload".into()))?;
    if !resp.ok {
        return Err(ClientError::AuthFailed("authentication rejected".into()));
    }
    let cancel = run_cancel.child_token();
    let (tx, rx) = mpsc::channel(inner.queue_capacity);
    let pong = Arc::new(Mutex::new(tokio::time::Instant::now()));
    let mut session = ConnectionSession {
        framed,
        cancel: cancel.clone(),
        handles: Vec::new(),
        pong: pong.clone(),
    };
    let writer_inner = inner.clone();
    let writer_cancel = cancel.clone();
    inner.active_tasks.fetch_add(1, Ordering::AcqRel);
    let writer_active = ActiveClientTask(run.clone());
    session.handles.push(tasks.spawn(async move {
        let _active = writer_active;
        writer_task(write_half, rx, writer_cancel, writer_inner).await;
    }));
    if inner.auto_heartbeat {
        let period = heartbeat_period(resp.heartbeat_timeout_ms);
        let heartbeat_inner = inner.clone();
        let heartbeat_cancel = cancel.clone();
        let heartbeat_tx = tx.clone();
        inner.active_tasks.fetch_add(1, Ordering::AcqRel);
        let heartbeat_active = ActiveClientTask(run.clone());
        session.handles.push(tasks.spawn(async move {
            let _active = heartbeat_active;
            heartbeat_task(
                heartbeat_tx,
                period,
                heartbeat_cancel,
                pong,
                heartbeat_inner,
            )
            .await;
        }));
    }
    {
        let mut outbox = inner.outbox.lock().unwrap();
        // 发布前复验关闭；旧代任务尚未退出时运行门禁仍禁止新的 connect。
        if run_cancel.is_cancelled()
            || cancel.is_cancelled()
            || tx.is_closed()
            || inner.closed.load(Ordering::Acquire)
        {
            return Err(ClientError::Disconnected);
        }
        *inner.last_authenticated.lock().unwrap() = Some(tokio::time::Instant::now());
        *inner.last_pong.lock().unwrap() = None;
        *outbox = Some(tx);
        *inner.session_id.lock().unwrap() = resp.session_id.clone();
        inner.connected.store(true, Ordering::Release);
    }
    Ok((resp, session))
}

/// 业务作用：持续处理控制帧，激活前有界缓冲业务帧，断线或本连接取消时退出。
/// 参数说明：`session` 属于当前连接；`inner` 提供固定回调和宿主激活屏障。
/// 返回：读循环停止，不自行发布下一代连接。
async fn read_loop(session: &mut ConnectionSession, inner: &Arc<Inner>) {
    let mut pending = VecDeque::new();
    let mut buffered_bytes = 0usize;
    loop {
        if inner.events_ready.is_cancelled() {
            while let Some(message) = pending.pop_front() {
                if session.cancel.is_cancelled() {
                    return;
                }
                dispatch(inner, message);
            }
            buffered_bytes = 0;
        }
        let item = tokio::select! {biased;
            _=session.cancel.cancelled()=>break,
            _=inner.events_ready.cancelled(),if !pending.is_empty()=>continue,
            item=session.framed.next()=>item,
        };
        let frame = match item {
            Some(Ok(frame)) => frame,
            Some(Err(_)) => {
                inner.record_failure(ClientFailure::Read);
                break;
            }
            None => {
                inner.record_failure(ClientFailure::Disconnected);
                break;
            }
        };
        match frame.typ {
            frame_type::EVENT | frame_type::BROADCAST => {
                let Some(mode) = Mode::from_ordinal(frame.mode) else {
                    continue;
                };
                let Ok(message) = Message::decode(mode, &frame.payload) else {
                    continue;
                };
                if inner.events_ready.is_cancelled() {
                    dispatch(inner, message);
                } else {
                    buffered_bytes = buffered_bytes.saturating_add(frame.payload.len());
                    if pending.len() >= 64 || buffered_bytes > inner.max_frame {
                        inner.record_failure(ClientFailure::Capacity);
                        break;
                    }
                    pending.push_back(message);
                }
            }
            frame_type::PONG if frame.mode == wire_mode::VARINT && frame.payload.is_empty() => {
                let now = tokio::time::Instant::now();
                *session.pong.lock().unwrap() = now;
                *inner.last_pong.lock().unwrap() = Some(now);
            }
            frame_type::PONG => {
                inner.record_failure(ClientFailure::Protocol);
                break;
            }
            frame_type::CLOSE => {
                inner.record_failure(ClientFailure::Disconnected);
                break;
            }
            _ => {}
        }
    }
}

/// 业务作用：串行管理首连与重连，旧会话退出后才能发布新会话。
/// 参数说明：`inner` 为本代状态；`cancel` 可终止握手和退避；`tasks` 持有子任务；`answer` 交付首连结果；`run` 保留退出责任。
/// 返回：本代监督任务结束，最终门禁由外层 owner 在 tracker 排空后释放。
async fn supervisor(
    inner: Arc<Inner>,
    cancel: CancellationToken,
    tasks: tokio_util::task::TaskTracker,
    answer: tokio::sync::oneshot::Sender<Result<AuthResponse, ClientError>>,
    run: Arc<ClientRun>,
) {
    let mut session = match establish_once(inner.clone(), &cancel, &tasks, &run).await {
        Ok((response, session)) => {
            if answer.send(Ok(response)).is_err() {
                cancel.cancel();
            }
            session
        }
        Err(error) => {
            if !cancel.is_cancelled() {
                inner.record_failure(connect_failure(&error));
            }
            let _ = answer.send(Err(error));
            return;
        }
    };
    let mut backoff = inner.reconnect_min;
    loop {
        read_loop(&mut session, &inner).await;
        // 先撤销旧通道并取得全部子任务退出证据，再通知断连与尝试下一次连接。
        inner.invalidate_connection(false);
        session.stop().await;
        on_disconnect_cleanup(&inner);
        if !inner.auto_reconnect || cancel.is_cancelled() || inner.closed.load(Ordering::Acquire) {
            return;
        }
        loop {
            tokio::select! {biased; _=cancel.cancelled()=>return,_=tokio::time::sleep(backoff)=>{}}
            match establish_once(inner.clone(), &cancel, &tasks, &run).await {
                Ok((_, next)) => {
                    session = next;
                    backoff = inner.reconnect_min;
                    break;
                }
                Err(error) => {
                    if !cancel.is_cancelled() {
                        inner.record_failure(connect_failure(&error));
                    }
                    if cancel.is_cancelled() {
                        return;
                    }
                    backoff = (backoff * 2).min(inner.reconnect_max);
                }
            }
        }
    }
}

/// 业务作用：执行断连清理；用于关闭会话状态并触发回调。
///
/// # 参数
/// - `inner`: 已解析的内部值或被包装对象。
fn on_disconnect_cleanup(inner: &Arc<Inner>) {
    inner.invalidate_connection(false);
    *inner.session_id.lock().unwrap() = None; // 断连清旧 session_id,避免误用
    let cb = inner.on_disconnect.lock().unwrap().clone();
    if let Some(cb) = cb {
        // 断连回调的异常不能越过连接 owner，使子任务失去等待责任。
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb())).is_err() {
            inner.record_failure(ClientFailure::Callback);
            inner.callback_failures.fetch_add(1, Ordering::AcqRel);
            tracing::error!("client on_disconnect callback panicked, isolated");
        }
    }
}

/// 业务作用：分发收到的消息；用于按事件名调用注册的处理器。
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
            inner.record_failure(ClientFailure::Callback);
            inner.callback_failures.fetch_add(1, Ordering::AcqRel);
            tracing::error!("client event callback panicked, isolated");
        }
    }
}

/// 业务作用：本连接独立发送心跳并检测未获 PONG 的半开状态。
/// 参数说明：`tx` 仅属于本连接；`period` 为周期；`cancel` 是连接令牌；`pong` 保存本连接最近响应；`inner` 留存失败事实。
/// 返回：取消、发送失败或超过两个周期未获响应时关闭本连接，不读取新连接的 connected 状态。
async fn heartbeat_task(
    tx: mpsc::Sender<Bytes>,
    period: Duration,
    cancel: CancellationToken,
    pong: Arc<Mutex<tokio::time::Instant>>,
    inner: Arc<Inner>,
) {
    let _stop = cancel.clone().drop_guard();
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await;
    loop {
        tokio::select! {biased;_=cancel.cancelled()=>break,_=ticker.tick()=>{}}
        if pong.lock().unwrap().elapsed() > period.saturating_mul(2) {
            inner.record_failure(ClientFailure::Heartbeat);
            break;
        }
        if tx.try_send(ping()).is_err() {
            inner.record_failure(ClientFailure::Capacity);
            break;
        }
    }
}

/// 业务作用：将握手失败映射为固定类别，避免通过观测输出远端返回文本。
/// 参数说明：`error` 为首连或重连失败。
/// 返回：认证、协议或普通连接类别。
fn connect_failure(error: &ClientError) -> ClientFailure {
    match error {
        ClientError::AuthFailed(_) => ClientFailure::Authentication,
        ClientError::Protocol(_) => ClientFailure::Protocol,
        _ => ClientFailure::Connect,
    }
}

/* ============================== helpers ============================== */

/// 业务作用：构造基础消息帧；用于统一客户端外发事件格式。
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

/// 业务作用：把空字符串转换为空选项；用于清理握手参数。
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

/// 业务作用：心跳周期 = 服务端 timeout 的一半(至少 1s,缺省 30s)。
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
