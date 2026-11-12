// ============================================================================
// src/server.rs —— 单机 TCP + WebSocket server。
// transport 无关的连接状态机(鉴权/心跳/分派)对 `Inbound` 编程;
// TCP 与 WS 只共享 NASA frame codec,各自的读/写实现分离。
// ============================================================================

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::WebSocketStream;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use naws_proto::{AuthResponse, CloseReason as ProtoCloseReason, Message, Mode, WireCodec};

use crate::auth::{
    AuthContext, AuthResult, AuthorizeProvider, InboundPolicy, PolicyContext, RouteDecision,
};
use crate::bridge::{default_bridge, PayloadBridge};
use crate::cluster::{Cluster, ClusterDataPublisher, Incarnation, Notifier, DEFAULT_NODE_TTL};
use crate::endpoint::{Endpoint, EndpointRegistry, EventHandler};
use crate::sender::Sender;
use crate::session::{
    BackpressurePolicy, Outbox, OutboxReceiver, Protocol, Session, SessionHandle, SessionRegistry,
    Transport,
};
use crate::wire::{encode_frame, frame_type, pong, wire_mode, Frame, FrameCodec};

static SESSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// 业务作用：生成带本地序号及随机 UUID 的连接标识，供认证响应与 Engine.IO sid 使用。
///
/// 参数说明：无。
///
/// 返回：`s{seq}-{uuid}` 格式文本；随机部分来自 UUID v4，序号仅用于本进程关联，不构成授权或全局时间顺序。
fn new_session_id() -> String {
    let seq = SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("s{seq}-{}", uuid::Uuid::new_v4().simple())
}

/// 活跃连接计数(graceful shutdown 等排空用)。
#[derive(Default)]
pub struct ConnTracker {
    /// 尚未归还的连接或未认证名额数量，具体口径由持有者决定。
    count: AtomicUsize,
    /// 名额归还时唤醒已登记的等待者。
    notify: tokio::sync::Notify,
}

/// 连接生命期 RAII:进 +1,退(handle_conn 返回)-1 并唤醒等待者。
struct ConnGuard {
    /// 本守卫负责归还一次活跃连接计数的目标。
    tracker: Arc<ConnTracker>,
}
impl ConnGuard {
    /// 业务作用：在连接任务移交前记入活跃连接数，确保协议分类与握手也消耗连接预算。
    ///
    /// 参数说明：
    /// - `tracker`: 当前服务共享的活跃连接计数器。
    ///
    /// 返回：已增加一次计数的守卫，释放时归还；本函数不检查容量上限，上层须先完成接入裁决。
    fn new(tracker: Arc<ConnTracker>) -> Self {
        tracker.count.fetch_add(1, Ordering::Relaxed);
        ConnGuard {
            tracker,
        }
    }
}
impl Drop for ConnGuard {
    /// 业务作用：连接任务结束或被销毁时归还活跃连接计数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：减少一次计数并通知等待者；不保证独立 writer 或 handler 子任务也已结束。
    fn drop(&mut self) {
        self.tracker.count.fetch_sub(1, Ordering::Relaxed);
        self.tracker.notify.notify_waiters();
    }
}

/// 未认证连接占位 RAII:连接进入鉴权窗口时 +1,鉴权成功或连接结束时释放。
struct UnauthGuard {
    /// 已原子预留一次未认证名额的计数器。
    tracker: Arc<ConnTracker>,
}

impl UnauthGuard {
    /// 业务作用：尝试预留一个未认证连接名额;已达上限时返回 `None`。
    ///
    /// 参数说明：
    /// - `tracker`: 服务共享的未认证连接计数，多个 listener 在同一原子值上争取名额。
    /// - `max`: 当前允许的名额总数；此函数中零表示无名额，上层通过跳过预留实现不限额配置。
    ///
    /// 返回：原子预留成功时返回守卫，达到上限时为 None；守卫销毁前该名额持续占用。
    fn try_new(tracker: Arc<ConnTracker>, max: usize) -> Option<Self> {
        loop {
            let current = tracker.count.load(Ordering::Relaxed);
            if current >= max {
                return None;
            }
            if tracker
                .count
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(Self {
                    tracker,
                });
            }
        }
    }
}

impl Drop for UnauthGuard {
    /// 业务作用：释放未认证连接名额。
    ///
    /// 参数说明：无。
    ///
    /// 返回：归还一次预留并通知等待者；可能因认证成功或连接结束发生，不等同于活跃连接退出。
    fn drop(&mut self) {
        self.tracker.count.fetch_sub(1, Ordering::AcqRel);
        self.tracker.notify.notify_waiters();
    }
}

/// 业务作用：调用用户同步闭包并隔离 panic —— 防止用户回调 panic 让连接 task unwind、跳过清理。
///
/// 参数说明：
/// - `what`：发生 panic 时写入日志的固定回调类别，不应包含认证材料或消息正文。
/// - `f`：当前线程内执行的同步业务回调，不能依赖本函数中止阻塞操作。
///
/// 返回：回调正常完成或 unwind 被记录后结束；不回滚回调已产生的副作用，不能捕获进程 abort。
fn isolate<F: FnOnce()>(what: &str, f: F) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err() {
        tracing::error!("user callback panicked, isolated: {what}");
    }
}

/// 连接退出时撤销会话索引与发送资格；断连通知的唯一责任由 Session 生命周期裁决。
/// 守卫随正常返回或 unwind 释放，避免仅依赖函数末尾注销而遗漏异常路径。
struct ConnCleanup<'a> {
    /// 退出时撤销索引、发送资格并通知生命周期回调的会话。
    session: &'a Arc<Session>,
    /// 本守卫是否已经发起清理，跨调用方的通知去重仍由 Session 负责。
    done: bool,
}
impl<'a> ConnCleanup<'a> {
    /// 业务作用：在会话进入认证流程前建立退出清理责任，覆盖正常返回与 unwind。
    ///
    /// 参数说明：
    /// - `session`: 需要在当前连接流程结束时关闭并注销的会话。
    ///
    /// 返回：尚未执行清理的守卫；不注册会话或改变认证状态，清理在守卫释放时发生。
    fn new(session: &'a Arc<Session>) -> Self {
        Self {
            session,
            done: false,
        }
    }
}
impl Drop for ConnCleanup<'_> {
    /// 业务作用：会话流程结束时撤销索引与发送资格，并按生命周期裁决通知断连。
    ///
    /// 参数说明：无。
    ///
    /// 返回：首次清理关闭会话；取得断连通知责任时调用绑定端点回调，随后关闭 outbox，但不等待 writer 实际写完。
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        // lifecycle 锁内撤销会话与索引，避免订阅变更在关闭后留下可投递条目。
        // cleanup 仅在取得断连通知责任时返回 true；连接回调尚未完成时由 finish_connect 延后通知。
        let was_auth = self.session.cleanup();
        if was_auth {
            fire_disconnect(self.session); // 锁外触发,用绑定快照,内部已 panic 隔离
        }
        self.session.close_outbox(); // writer 排空已入队帧(含 CLOSE)后退出,不靠 Arc 自然 drop
    }
}

type WsStream = WebSocketStream<TcpStream>;
type WsSink = futures::stream::SplitSink<WsStream, WsMessage>;
type WsSplit = futures::stream::SplitStream<WsStream>;

/// 服务端配置。
#[derive(Clone)]
pub struct ServerConfig {
    /// TCP 监听地址。
    pub addr: String,
    /// 可选 WebSocket 监听地址;None 表示不启用 WS 端口。
    pub ws_addr: Option<String>,
    /// 连接建立后等待 AUTH 或 socket.io CONNECT 的最长时间。
    pub auth_timeout: Duration,
    /// 协议心跳的最大静默时长；NASA 由 PING 续期，Socket.IO 由 Engine.IO PING/PONG 续期。
    pub heartbeat_timeout: Duration,
    /// 单条 NASA frame 的**内层 declared 长度**上限(type+mode+payload,与 FrameCodec
    /// 同语义)。合法范围受三重约束(build 期校验):
    /// `MIN_MAX_FRAME ≤ max_frame ≤ u32::MAX`(NASA 帧头是 4 字节无符号长度域),
    /// 且派生的 WS 外层上限必须可表示。
    pub max_frame: usize,
    /// WS 单条 message 的**外层聚合**字节上限(一条 Binary 可携带多条 NASA frame,
    /// `max_frame` 只约束**每条** NASA frame）。该字段提供粗粒度 DoS 保护。
    /// `None` = 自动 `4 × (max_frame + 4)`;显式设置时 build 校验 ≥ max_frame + 4。
    pub max_ws_message_bytes: Option<usize>,
    /// 业务出站队列条数上限。
    pub outbox_cap: usize,
    /// 业务出站队列字节上限(防小帧多条/大帧少条积压)。
    pub outbox_max_bytes: usize,
    /// 出站队列满时的背压策略。
    pub backpressure: BackpressurePolicy,
    /// Async handler 的**全局**最大在飞并发;超过则拒绝该事件(过载保护)。
    pub max_inflight_handlers: usize,
    /// **单连接** async handler 在飞并发上限(与全局 `max_inflight_handlers` 叠加):
    /// 防单连接狂发事件抢占全局池、饿死其它连接。`0` = 不限制(仅受全局约束)。
    pub max_inflight_handlers_per_conn: usize,
    /// **同时活跃连接总数上限**(accept 处背压):达到后新连接被立即关闭、不 spawn 连接任务。
    /// 连接预算包含协议分类、握手与认证；多个 listener 并发接入时可能有短暂竞争。`0` = 不限制。
    /// 默认给一个较大的兜底值,大规模部署可 `max_connections()` 上调。
    pub max_connections: usize,
    /// **未认证连接数上限**(accept 处精确预留):限制已接入但尚未完成 AUTH/socket.io CONNECT 的连接。
    /// 防慢握手/慢鉴权连接占满全局连接池。鉴权成功后释放该名额,连接仍受 `max_connections` 管控。
    /// `0` = 不限制。
    pub max_unauthenticated: usize,
    /// 集群启动策略:false=FailFast(集群未就绪则 bind 失败,默认);true=Degraded(降级继续,打 warning)。
    pub cluster_degraded: bool,
    /// cluster transport 完成初始就绪的总等待时间。
    pub cluster_ready_timeout: Duration,
}

impl Default for ServerConfig {
    /// 业务作用：返回默认配置；用于未显式设置时提供稳定基线。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅启用本机 TCP、有限连接和队列预算、集群失败即拒绝启动的配置；不提供认证器或绑定端口。
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:9090".into(),
            ws_addr: None,
            auth_timeout: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(60),
            max_frame: 16 * 1024 * 1024,
            max_ws_message_bytes: None,
            outbox_cap: 256,
            outbox_max_bytes: 64 * 1024 * 1024,
            backpressure: BackpressurePolicy::DropNew,
            max_inflight_handlers: 4096,
            // 单连接默认 256:远低于全局 4096(约需 16 个连接才能打满全局),既够正常业务并发,
            // 又防单连接独占全局池。
            max_inflight_handlers_per_conn: 256,
            // 连接总数默认 100k 兜底:正常服务够用、又能封顶跑飞;大规模部署 max_connections() 上调。
            max_connections: 100_000,
            // 未认证连接默认 4096:允许正常突发握手,但避免慢握手/鉴权洪泛占满全部连接槽。
            max_unauthenticated: 4096,
            cluster_degraded: false,
            cluster_ready_timeout: Duration::from_secs(5),
        }
    }
}

impl ServerConfig {
    /// 业务作用：WS 外层 message 上限的**生效值**:显式配置或自动 `4 × (max_frame + 4)`
    /// (容多条最大帧聚合)。**饱和运算兜底**:字段公开,
    /// 调用方可绕过 builder 直接构造——极端 max_frame 在此绝不 panic/回绕,溢出语义
    /// 校验由 `build()` 用 checked 运算在启动期完成。
    ///
    /// 参数说明：无。
    ///
    /// 返回：显式外层上限或饱和派生值；仅计算数值，不证明配置能装下一条最大内部帧。
    pub fn ws_message_limit(&self) -> usize {
        self.max_ws_message_bytes
            .unwrap_or_else(|| self.max_frame.saturating_add(4).saturating_mul(4))
    }
}

/// 框架内部共享状态。**`pub(crate)`**:业务不应直达内部装配
/// 对外只通过 `RunningServer` 的受控访问器(registry/sender/endpoints/active_conns 等)。
pub(crate) struct Shared {
    /// 构建阶段校验并冻结的监听、协议与容量配置。
    pub config: ServerConfig,
    /// 主入口是否按首字节区分原生 TCP 与 WebSocket。
    multiplex: bool,
    /// 可选应用协议所有者，接管整条连接后不使用内置 Message 分派。
    protocol_handler: Option<ProtocolHandler>,
    /// 本地会话及用户、订阅、端点投递索引。
    pub registry: Arc<SessionRegistry>,
    /// 动态端点目录及旧代会话撤销回调。
    pub endpoints: Arc<EndpointRegistry>,
    /// 绑定本地目录与可选集群发布入口的发送器。
    pub sender: Arc<Sender>,
    /// 内置协议认证器，成功 UID 仍须通过非空白校验。
    pub authorize: AuthorizeProvider,
    /// 客户端携带路由目标时执行的转发裁决。
    pub policy: InboundPolicy,
    /// 可选跨节点编排与目录状态，不包含应用协议自己的中继实现。
    pub cluster: Option<Arc<Cluster>>,
    /// Async handler 在飞并发上限。
    pub handler_sem: Arc<tokio::sync::Semaphore>,
    /// 活跃连接计数(graceful shutdown 用)。
    pub active: Arc<ConnTracker>,
    /// 未认证连接计数(慢握手/慢鉴权 DoS 背压)。
    pub unauthenticated: Arc<ConnTracker>,
    /// socket.io payload 桥接。仅 socketio 路径读取;非 socketio 构建下不读(允许 dead)。
    #[cfg_attr(not(feature = "socketio"), allow(dead_code))]
    pub bridge: Arc<dyn PayloadBridge>,
    /// 服务取消信号；接入与分派流程观察它，writer 通过 outbox 关闭退出，应用协议须主动响应。
    pub cancel: CancellationToken,
    /// 所有服务端后台任务的跟踪器:graceful shutdown 时 close+wait 真正 join。
    pub tasks: TaskTracker,
}

type ProtocolHandler = Arc<
    dyn Fn(
            TcpStream,
            ProtocolContext,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// 应用协议的单连接生命周期，认证预算与任务排空仍由 naws 统一管理。
pub struct ProtocolContext {
    /// 本次物理连接的随机关联标识，不授予业务身份。
    session_id: String,
    /// 操作系统提供的直接 TCP 对端。
    peer: std::net::SocketAddr,
    /// accept 时确定的初次认证截止点。
    deadline: Instant,
    /// 是否来自配置的独立 WebSocket 入口。
    websocket_only: bool,
    /// 本连接的取消令牌，同时继承服务取消。
    cancel: CancellationToken,
    /// 应用子任务登记到的服务级退出跟踪器。
    tasks: TaskTracker,
    /// 初次认证前占用的名额，确认认证或上下文销毁时归还。
    unauthenticated: Option<UnauthGuard>,
    /// 向连接 owner 一次性报告认证已完成的通道。
    admitted: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ProtocolContext {
    /// 业务作用：取得框架为本次物理连接生成的随机关联标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅属于此连接的标识，不具有业务授权含义。
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// 业务作用：取得操作系统确认的直接对端地址，避免把客户端自报代理头当作来源事实。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：真实 TCP peer 地址；代理后的用户身份须由应用另外验证。
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        self.peer
    }

    /// 业务作用：读取 accept 时确定的认证截止点，不因协议分类或 HTTP Upgrade 重新计时。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前连接唯一的初次认证截止点。
    pub fn auth_deadline(&self) -> Instant {
        self.deadline
    }

    /// 业务作用：告知应用当前入口是否为独立 WebSocket 监听器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：独立 WS 入口为 true；应用不能据此开放未配置的业务路由。
    pub fn websocket_only(&self) -> bool {
        self.websocket_only
    }

    /// 业务作用：提供连接取消信号，兼顾宿主停机与连接 owner 销毁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同一连接的子取消令牌，不能撤销父服务的取消事实。
    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// 业务作用：应用完成必要认证后释放未认证容量，逾期或停机时不得扩大连接资格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次且在截止前确认成功；重复、逾期或取消时返回 false。
    pub fn authenticated(&mut self) -> bool {
        if self.cancel.is_cancelled() || Instant::now() >= self.deadline {
            return false;
        }
        let Some(admitted) = self.admitted.take() else {
            return false;
        };
        self.unauthenticated.take();
        admitted.send(()).is_ok()
    }

    /// 业务作用：把连接读写或业务子任务纳入同一个服务排空屏障，防止父任务退出后遗漏责任。
    ///
    /// 参数说明：
    /// - `future`：必须观察本连接 cancellation 的有限任务。
    ///
    /// 返回：可等待的任务句柄，服务关闭同时等待该任务真正结束。
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(future)
    }
}

impl Drop for ProtocolContext {
    /// 业务作用：连接协议 owner 结束时撤销所有子任务准入，确保迟到操作不会继续使用连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取消当前连接任务组，未认证容量由 guard 自动释放。
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// `ServerBuilder::build` 的配置错误(启动期校验,不静默用错误默认)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
    /// 未提供鉴权 provider。
    NoAuthorize,
    /// 启用集群但 node_id 为空。
    EmptyNodeId,
    /// 设了 incarnation 却没启用集群(`cluster()` 未调)。
    IncarnationWithoutCluster,
    /// 启用集群却没注入 incarnation——生产必须用持久单调源(Redis INCR)分配,不退化墙钟。
    ClusterRequiresIncarnation,
    /// 注入了 data publisher，但没有启用对应的集群 notifier。
    DataPublisherWithoutCluster,
    /// cluster ready 超时为零，transport 没有任何完成启动的机会。
    ClusterReadyTimeoutZero,
    /// `max_frame` 小于 [`MIN_MAX_FRAME`]:必需的**成功控制帧**(AUTH_RESP 带 sessionId、
    /// engine.io OPEN、CLOSE reason)都可能超限,形成"请求可接受、成功响应必超限"的半可用
    /// 配置。
    MaxFrameTooSmall,
    /// 显式 `max_ws_message_bytes` 小于 `max_frame + 4`:连一条最大合法 NASA frame 都装不下
    ///。
    WsMessageLimitTooSmall,
    /// `max_frame` 大到派生上限(`max_frame+4` 或默认 `4×(max_frame+4)`)溢出 usize
    /// ——启动期拒绝,不把配置错误拖到 bind/握手阶段。
    FrameLimitOverflow,
    /// `max_frame` 超过 NASA 线协议 4 字节长度域(`u32::MAX`)——任何入站帧都表达不了,
    /// 超大出站编码会在 `encode_frame` 的 checked 转换处 panic。配置声明必须与线协议能力
    /// 一致。
    MaxFrameExceedsWireLimit,
    /// 全局 handler 并发容量为零，或超过 Tokio semaphore 可表达的许可数。
    ///
    /// 该值最终直接构造全局 [`tokio::sync::Semaphore`]；不在 build 期拒绝会分别形成
    /// “所有事件永久过载”或启动 panic。
    InvalidMaxInflightHandlers,
    /// 握手、心跳或集群 ready 时限为零或超过框架硬上限。
    InvalidRuntimeDuration,
}

/// `max_frame` 的构建期下限:必须容得下最大的成功控制帧——AUTH_RESP(sessionId ≤ ~60B)、
/// engine.io OPEN(JSON ~170B)、CLOSE(reason 截断 ≤ [`MAX_REASON_LEN`])。任何真实部署
/// 都远大于此;低于它的配置在 build 期拒绝,不进运行时。
pub const MIN_MAX_FRAME: usize = 512;
/// 握手、心跳与集群启动等待的统一硬上限。
pub const MAX_SERVER_RUNTIME_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 动态错误文本(auth 失败原因 / CLOSE reason / sio connect_error message)的统一截断上限:
/// 业务 authorize 返回的 reason 不可控,不截断会让控制帧超 max_frame 被丢、客户端收不到
/// 失败响应。
pub const MAX_REASON_LEN: usize = 100;

/// 业务作用：按 UTF-8 字符边界截断 reason(超限取前缀;控制帧体积可控的前提)。
///
/// 参数说明：
/// - `s`: 准备放入认证失败、协议关闭或 Socket.IO 错误响应的原因文本。
///
/// 返回：不超过 MAX_REASON_LEN 字节且不切断 UTF-8 字符的原文借用；不复制、不脱敏，也不保证 JSON 转义后的长度相同。
fn truncate_reason(s: &str) -> std::borrow::Cow<'_, str> {
    if s.len() <= MAX_REASON_LEN {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut end = MAX_REASON_LEN;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    std::borrow::Cow::Borrowed(&s[..end])
}

impl std::fmt::Display for BuildError {
    /// 业务作用：将服务构建拒绝原因转换为稳定说明，供调用方定位不满足的配置约束。
    ///
    /// 参数说明：
    /// - `f`: 错误展示使用的标准格式化器。
    ///
    /// 返回：写出对应固定说明时成功，格式化器失败时传播错误；不包含实际认证材料或修改配置。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = match self {
            BuildError::NoAuthorize => "authorize provider required",
            BuildError::EmptyNodeId => "cluster node_id must not be empty",
            BuildError::IncarnationWithoutCluster => {
                "cluster_incarnation set but cluster() not enabled"
            }
            BuildError::ClusterRequiresIncarnation => {
                "cluster requires an injected Incarnation (production must not fall back to wall-clock)"
            }
            BuildError::DataPublisherWithoutCluster => {
                "cluster data publisher requires cluster notifier"
            }
            BuildError::ClusterReadyTimeoutZero => {
                "cluster_ready_timeout must be greater than zero"
            }
            BuildError::MaxFrameTooSmall => {
                "max_frame too small: required success control frames (AUTH_RESP/OPEN/CLOSE) would exceed it"
            }
            BuildError::WsMessageLimitTooSmall => {
                "max_ws_message_bytes must be >= max_frame + 4 (one full NASA frame)"
            }
            BuildError::FrameLimitOverflow => {
                "max_frame too large: derived WS message limit overflows usize"
            }
            BuildError::MaxFrameExceedsWireLimit => {
                "max_frame exceeds NASA wire u32 length field (max u32::MAX)"
            }
            BuildError::InvalidMaxInflightHandlers => {
                "max_inflight_handlers must be in 1..=tokio::sync::Semaphore::MAX_PERMITS"
            }
            BuildError::InvalidRuntimeDuration => {
                "auth_timeout, heartbeat_timeout and cluster_ready_timeout must be within (0, 365 days]"
            }
        };
        write!(f, "{m}")
    }
}
impl std::error::Error for BuildError {}

/// 保存服务端构造参数；用于配置监听地址、鉴权和入口策略。
pub struct ServerBuilder {
    /// 待校验的监听、心跳与容量配置。
    config: ServerConfig,
    /// 是否复用主监听端口接收原生协议与 WebSocket。
    multiplex: bool,
    /// 可选接管连接的应用协议任务工厂。
    protocol_handler: Option<ProtocolHandler>,
    /// 构建前可共享装配、构建后仍可动态变更的端点目录。
    endpoints: Arc<EndpointRegistry>,
    /// 必须显式提供的内置协议认证器。
    authorize: Option<AuthorizeProvider>,
    /// 客户端投递目标的入站策略，默认拒绝自动转发。
    policy: InboundPolicy,
    /// 集群回环过滤与实例围栏使用的稳定逻辑节点 ID。
    cluster_node: Option<String>,
    /// 可选的跨节点控制与 presence 广播介质。
    notifier: Option<Arc<dyn Notifier>>,
    /// 可选少拷贝 data publisher；presence 仍固定走 notifier。
    cluster_data_publisher: Option<Arc<dyn ClusterDataPublisher>>,
    /// 调用方注入的已校验实例代次；集群模式缺少时拒绝构建，不回退到墙钟。
    cluster_incarnation: Option<Incarnation>,
    /// Socket.IO 参数与原始 payload 的同步转换接口。
    bridge: Arc<dyn PayloadBridge>,
}

impl ServerBuilder {
    /// 业务作用：把已接收连接交给应用协议适配器，继续复用服务的连接容量、启动和排空责任。
    ///
    /// 参数说明：
    /// - `handler`：拥有整条连接的协议任务；必须观察上下文取消并通过上下文登记子任务。
    ///
    /// 返回：更新后的构建器；显式接管后不执行既有 Message、endpoint、bridge 或 relay 协议。
    pub fn protocol_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(TcpStream, ProtocolContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.protocol_handler = Some(Arc::new(move |stream, context| {
            Box::pin(handler(stream, context))
        }));
        self
    }
    /// 业务作用：让原生 TCP 与 WebSocket 共用一个受管业务监听器。
    ///
    /// 参数说明：
    ///
    /// - `enabled`：开启协议分发；不能同时配置第二个 WebSocket 地址。
    ///
    /// 返回：更新后的构建器；共享监听约束在 bind 产生网络副作用前校验。
    pub fn multiplex(mut self, enabled: bool) -> Self {
        self.multiplex = enabled;
        self
    }

    /// 业务作用：提供同一端点目录供受权路由控制器安装和撤销业务入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享目录；构建服务后目录变更继续经过框架的会话代际清理。
    pub fn endpoint_registry(&self) -> Arc<EndpointRegistry> {
        self.endpoints.clone()
    }
    /// 业务作用：识别已经安装的集群策略，供宿主拒绝重复接管同一服务。
    ///
    /// 参数说明：无。
    ///
    /// 返回：存在通知器、节点身份或数据发布器时返回真，不改变 builder。
    pub fn has_cluster(&self) -> bool {
        self.cluster_node.is_some()
            || self.notifier.is_some()
            || self.cluster_data_publisher.is_some()
            || self.cluster_incarnation.is_some()
    }
    /// 业务作用：设置 TCP 监听地址。
    ///
    /// 参数说明：
    /// - `addr`: TCP 服务监听地址,例如 `0.0.0.0:19091`。
    ///
    /// 返回：保存主监听地址的 builder，不立即解析或绑定；地址格式、端口占用错误在 bind 阶段返回。
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.config.addr = addr.into();
        self
    }

    /// 业务作用：配置独立 WebSocket 监听地址，与主 TCP 入口分别接收连接。
    ///
    /// 参数说明：
    /// - `addr`: WebSocket 服务监听地址。
    ///
    /// 返回：保存独立地址的 builder，监听尚未建立；不能同时开启 multiplex，冲突在 bind 前拒绝。
    pub fn ws_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.ws_addr = Some(addr.into());
        self
    }

    /// 业务作用：设置鉴权超时时间；用于限制连接建立后的认证等待。
    ///
    /// 参数说明：
    /// - `d`: accept 后协议分类、HTTP Upgrade 与 AUTH/CONNECT 共用的总认证时长。
    ///
    /// 返回：保存认证时限的 builder；build 拒绝零值或超过 365 天的配置，不为协议阶段分别延长时限。
    pub fn auth_timeout(mut self, d: Duration) -> Self {
        self.config.auth_timeout = d;
        self
    }

    /// 业务作用：设置心跳超时时间；用于及时关闭失活连接。
    ///
    /// 参数说明：
    /// - `d`: 未收到协议心跳的允许时长；原生协议由合法 PING 刷新，Socket.IO 由 Engine.IO PING/PONG 刷新，业务事件不续期。
    ///
    /// 返回：保存心跳时限的 builder；build 拒绝零值或超过 365 天的值，超时由运行期周期检查发现。
    pub fn heartbeat_timeout(mut self, d: Duration) -> Self {
        self.config.heartbeat_timeout = d;
        self
    }

    /// 业务作用：设置**全局** async handler 并发上限;用于控制全服务的事件处理压力(过载保护)。
    ///
    /// 参数说明：
    /// - `n`: 全服务允许同时运行的异步 handler 数量上限。
    ///
    /// 返回：保存全局容量的 builder；build 拒绝零值或超出 Semaphore 上限，运行时容量不足会跳过该事件而非等待。
    pub fn max_inflight_handlers(mut self, n: usize) -> Self {
        self.config.max_inflight_handlers = n;
        self
    }

    /// 业务作用：设置**单连接** async handler 并发上限(与全局叠加);防单连接抢占全局池饿死其它连接。
    ///
    /// 参数说明：
    /// - `n`: 单个连接允许同时运行的异步 handler 数量上限;`0` = 不限制(仅受全局约束)。
    ///
    /// 返回：保存连接级容量的 builder；运行时先预留该名额再取全局许可，任一不足只拒绝当前事件，不排队等待。
    pub fn max_inflight_handlers_per_conn(mut self, n: usize) -> Self {
        self.config.max_inflight_handlers_per_conn = n;
        self
    }

    /// 业务作用：设置**同时活跃连接总数上限**(accept 处背压);达到后新连接被立即关闭。
    ///
    /// 参数说明：
    /// - `n`: 允许同时活跃的连接总数;`0` = 不限制。
    ///
    /// 返回：保存接入阈值的 builder；包含分类与认证阶段，多个 listener 的检查与递增并非一次原子预留，可短暂竞争。
    pub fn max_connections(mut self, n: usize) -> Self {
        self.config.max_connections = n;
        self
    }

    /// 业务作用：设置**未认证连接数上限**(accept 处精确预留);达到后新连接被立即关闭。
    ///
    /// 参数说明：
    /// - `n`: 允许同时处于握手/AUTH 阶段的连接数;`0` = 不限制。
    ///
    /// 返回：保存未认证容量的 builder；非零时接入前原子预留，认证成功或连接结束归还，零值同时停用此项计数。
    pub fn max_unauthenticated(mut self, n: usize) -> Self {
        self.config.max_unauthenticated = n;
        self
    }

    /// 业务作用：出站背压策略(默认 DropNew;行情快照类可用 DropOldest)。
    ///
    /// 参数说明：
    /// - `p`: 业务帧队列满时的取舍策略。
    ///
    /// 返回：保存队列策略的 builder；策略作用于每个会话的业务帧容量，不保证送达，也不改变独立控制队列的淘汰规则。
    pub fn backpressure(mut self, p: BackpressurePolicy) -> Self {
        self.config.backpressure = p;
        self
    }

    /// 业务作用：单条 NASA frame 内层 declared 上限(默认 16MiB)。WS 外层另由
    /// `max_ws_message_bytes` 约束(一条 message 可聚合多条 frame)。
    ///
    /// 参数说明：
    /// - `n`: 内层 declared 字节上限，包含 type、mode 与 payload，不含四字节长度头。
    ///
    /// 返回：将至少为一的值存入 builder；build 仍要求不低于 MIN_MAX_FRAME 且符合线协议及派生长度约束。
    pub fn max_frame_bytes(mut self, n: usize) -> Self {
        self.config.max_frame = n.max(1);
        self
    }

    /// 业务作用：WS 单条 message 外层聚合上限(默认自动 `4 × (max_frame + 4)`);
    /// build 校验 ≥ max_frame + 4。
    ///
    /// 参数说明：
    /// - `n`: WebSocket 单条 message 允许聚合的最大字节数。
    ///
    /// 返回：保存显式外层上限的 builder；过小值在 build 拒绝，不放宽每条 NASA 帧或 Socket.IO 广告负载的限制。
    pub fn max_ws_message_bytes(mut self, n: usize) -> Self {
        self.config.max_ws_message_bytes = Some(n);
        self
    }

    /// 业务作用：业务出站队列条数上限(默认 256)。
    ///
    /// 参数说明：
    /// - `n`: 每个 session 业务出站队列允许缓存的帧数量。
    ///
    /// 返回：将至少为一的容量保存到 builder；字节预算同时生效，控制队列不占用此业务帧计数。
    pub fn outbox_capacity(mut self, n: usize) -> Self {
        self.config.outbox_cap = n.max(1);
        self
    }

    /// 业务作用：业务出站队列字节上限(默认 64MiB)。
    ///
    /// 参数说明：
    /// - `n`: 每个 session 业务出站队列允许缓存的总字节数。
    ///
    /// 返回：将至少为一的预算保存到 builder；只约束排队的已编码业务帧，不等于连接总内存或正在写出帧的预算。
    pub fn outbox_max_bytes(mut self, n: usize) -> Self {
        self.config.outbox_max_bytes = n.max(1);
        self
    }

    /// 业务作用：集群降级模式:true=集群未就绪也继续 bind(打 warning);默认 false=FailFast(bind 失败)。
    ///
    /// 参数说明：
    /// - `on`: 是否允许集群 transport 未就绪时继续启动本地服务。
    ///
    /// 返回：保存启动失败处理策略的 builder；开启后只允许本地继续接入，不保证跨节点发送，也不自动重建失败的 transport。
    pub fn cluster_degraded(mut self, on: bool) -> Self {
        self.config.cluster_degraded = on;
        self
    }

    /// 业务作用：设置 cluster transport 的初始就绪总超时。
    ///
    /// Kafka 模式应覆盖 TopicContract、group join 与 assignment 的最坏启动时长。
    ///
    /// 参数说明：
    ///
    /// - `timeout`: 非零启动等待时长。
    ///
    /// 返回：保存初始就绪预算的 builder；build 验证不超过 365 天，bind 超时按 cluster_degraded 选择拒绝或降级继续。
    pub fn cluster_ready_timeout(mut self, timeout: Duration) -> Self {
        self.config.cluster_ready_timeout = timeout;
        self
    }

    /// 业务作用：设置鉴权回调；用于握手阶段验证连接身份。
    ///
    /// 参数说明：
    /// - `f`: 握手阶段执行的鉴权回调,入参为认证请求和连接上下文。
    ///
    /// 返回：替换认证器的 builder，不立即调用；运行时受原认证截止点约束，成功结果中的空白 UID 仍会被框架拒绝。
    pub fn authorize<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(naws_proto::AuthRequest, AuthContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = AuthResult> + Send + 'static,
    {
        self.authorize = Some(Arc::new(move |req, ctx| Box::pin(f(req, ctx))));
        self
    }

    /// 业务作用：设置入站策略；用于控制客户端触发的自动 relay。
    ///
    /// 参数说明：
    /// - `p`: 入站路由授权策略,默认 deny all。
    ///
    /// 返回：保存策略的 builder；仅带有效投递目标的消息进入该裁决，不带路由的事件仍可触发已注册 handler。
    pub fn policy(mut self, p: InboundPolicy) -> Self {
        self.policy = p;
        self
    }

    /// 业务作用：注入 socket.io payload 桥接(默认 JSON 透传)。
    ///
    /// 参数说明：
    /// - `b`: payload bridge,用于 socket.io JSON 参数与内部 payload 字节互转。
    ///
    /// 返回：保存同步桥接器的 builder；不改变原生 NASA 编码或执行权限校验，桥接结果的合法性由实现负责。
    pub fn payload_bridge(mut self, b: Arc<dyn PayloadBridge>) -> Self {
        self.bridge = b;
        self
    }

    /// 业务作用：在构建阶段向共享端点目录登记一个带代次的入口快照。
    ///
    /// 参数说明：
    /// - `ep`: 要注册到服务端路由表的 endpoint 快照。
    ///
    /// 返回：登记成功后返回 builder；路径为空、超限或已存在时 panic，不用此入口替换已有端点。
    pub fn endpoint(self, ep: Endpoint) -> Self {
        self.endpoints.register(ep).expect("endpoint register");
        self
    }

    /// 业务作用：启用集群:本节点 ID + 跨节点 transport。
    ///
    /// 参数说明：
    /// - `node_id`: 本节点稳定 ID,用于防回环、presence 和 fencing。
    /// - `notifier`: 跨节点广播 transport。
    ///
    /// 返回：替换节点与通知器配置的 builder，不启动收发；build 还要求非空节点 ID 与显式注入的持久单调实例代次。
    pub fn cluster(mut self, node_id: impl Into<String>, notifier: Arc<dyn Notifier>) -> Self {
        self.cluster_node = Some(node_id.into());
        self.notifier = Some(notifier);
        self
    }

    /// 业务作用：注入独立 data plane publisher；control/presence 仍由 `cluster` 的 notifier 承载。
    ///
    /// 参数说明：
    ///
    /// - `publisher`: 绑定固定 data topic 与 producer lane 的发布器。
    ///
    /// 返回：保存数据发布器的 builder，不启动或取得就绪；未配置集群 notifier 时 build 拒绝，发布器生命周期由宿主管理。
    pub fn cluster_data_publisher(mut self, publisher: Arc<dyn ClusterDataPublisher>) -> Self {
        self.cluster_data_publisher = Some(publisher);
        self
    }

    /// 业务作用：注入已校验的集群 incarnation fencing token(业务用 `Incarnation::from_epoch(Redis INCR)` 构造)。
    ///
    /// 参数说明：
    /// - `incarnation`: 当前逻辑节点的非零实例代次，部署方保证同节点重启时持久单调递增。
    ///
    /// 返回：保存实例代次的 builder；集群缺少此值或单机单独提供此值均在 build 拒绝，不回退到墙钟。
    pub fn cluster_incarnation(mut self, incarnation: Incarnation) -> Self {
        self.cluster_incarnation = Some(incarnation);
        self
    }

    /// 业务作用：验证认证器、容量及集群身份约束，装配会话目录、动态端点清理与发送入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：有效配置对应的未绑定服务；缺鉴权、非法限额或不完整集群身份均拒绝。
    /// 监听地址解析、共享端口冲突及自定义协议与内置集群冲突在 bind 前另行检查。
    pub fn build(self) -> Result<Server, BuildError> {
        let Some(authorize) = self.authorize else {
            return Err(BuildError::NoAuthorize);
        };
        if self.cluster_data_publisher.is_some() && self.notifier.is_none() {
            return Err(BuildError::DataPublisherWithoutCluster);
        }
        if self.config.cluster_ready_timeout.is_zero() {
            return Err(BuildError::ClusterReadyTimeoutZero);
        }
        if self.config.auth_timeout.is_zero()
            || self.config.heartbeat_timeout.is_zero()
            || self.config.auth_timeout > MAX_SERVER_RUNTIME_DURATION
            || self.config.heartbeat_timeout > MAX_SERVER_RUNTIME_DURATION
            || self.config.cluster_ready_timeout > MAX_SERVER_RUNTIME_DURATION
        {
            return Err(BuildError::InvalidRuntimeDuration);
        }
        if self.config.max_inflight_handlers == 0
            || self.config.max_inflight_handlers > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(BuildError::InvalidMaxInflightHandlers);
        }
        // 帧上限下限校验:小于必需成功控制帧的配置直接拒绝。
        if self.config.max_frame < MIN_MAX_FRAME {
            return Err(BuildError::MaxFrameTooSmall);
        }
        // **线协议上界**:NASA declared length 是 4 字节无符号整数,
        // max_frame > u32::MAX 的配置任何入站帧都表达不了、出站会在 encode_frame panic
        // ——先于派生计算拒绝(显式 max_ws_message_bytes 不能绕过)。
        if self.config.max_frame as u64 > u32::MAX as u64 {
            return Err(BuildError::MaxFrameExceedsWireLimit);
        }
        // WS 外层上限校验,**checked 运算**:溢出与"装不下一条
        // 最大帧"都在启动期拒绝,不拖到 bind/握手阶段 panic 或回绕成错误小上限。
        let min_ws = self
            .config
            .max_frame
            .checked_add(4)
            .ok_or(BuildError::FrameLimitOverflow)?;
        match self.config.max_ws_message_bytes {
            Some(n) if n < min_ws => return Err(BuildError::WsMessageLimitTooSmall),
            Some(_) => {}
            // 默认自动派生 4×(max_frame+4):必须可表示。
            None => {
                min_ws
                    .checked_mul(4)
                    .ok_or(BuildError::FrameLimitOverflow)?;
            }
        }
        // 集群必须使用稳定节点身份和显式实例代次；单机不允许悬空的围栏配置。
        match (&self.cluster_node, &self.cluster_incarnation) {
            (None, Some(_)) => return Err(BuildError::IncarnationWithoutCluster),
            (Some(node), inc) => {
                if node.trim().is_empty() {
                    return Err(BuildError::EmptyNodeId);
                }
                if inc.is_none() {
                    return Err(BuildError::ClusterRequiresIncarnation);
                }
            }
            (None, None) => {}
        }

        let registry = SessionRegistry::new();
        let sender = Sender::new_with_max_frame(
            registry.clone(),
            self.bridge.clone(),
            self.config.max_frame,
        );

        // **endpoint 代际语义装配**
        // ① mutation hook:replace/unregister 后主动关闭该 path 的旧代际会话——发协议化
        //    CLOSE(NASA ENDPOINT_REMOVED / sio DISCONNECT)→ cleanup(注销索引,立即停止
        //    uid/group fan-out)→ 绑定快照的 on_disconnect(exactly once,由 cleanup 的
        //    "首位关闭者"语义保证与连接任务不重复)→ close_outbox(排空 CLOSE 后 writer 退出)。
        //    hook 在执行时读取**当前**代际:并发多次变更的 hook 乱序到达也只按最新代际清旧。
        //    hook/resolver **只捕获弱引用**:强引用会形成
        //    `Arc<EndpointRegistry> → mutation_hook → Arc<EndpointRegistry>` 的确定性引用环,
        //    Server drop 后 EndpointRegistry/SessionRegistry/全部业务回调永久泄漏;
        //    upgrade 失败(Server 已销毁)→ no-op,不依赖调用方走特定关闭路径。
        {
            let weak_reg = Arc::downgrade(&registry);
            let weak_eps = Arc::downgrade(&self.endpoints);
            self.endpoints
                .set_mutation_hook(Arc::new(move |path: &str| {
                    let (Some(reg), Some(eps)) = (weak_reg.upgrade(), weak_eps.upgrade()) else {
                        return; // Server 已销毁:无会话可关
                    };
                    let survive = eps.current_generation(path);
                    for id in reg.ids_by_endpoint(path) {
                        let Some(s) = reg.by_id_arc(&id) else {
                            continue;
                        };
                        if s.bound_generation().is_some() && s.bound_generation() == survive {
                            continue; // 当前代际的会话保留
                        }
                        s.send_endpoint_removed_close();
                        let was_auth = s.cleanup(); // 首位关闭者才 true(exactly-once);
                                                    // 同时 cancel 连接令牌 → reader 任务退出
                        if was_auth {
                            fire_disconnect(&s);
                        }
                        s.close_outbox();
                    }
                }));
        }
        // ② 出站投递兜底:fan-out 跳过旧代际会话(主动关闭之外的竞态兜底)。
        //    同样弱引用:Sender→EndpointRegistry 当前虽无环,引用方向统一为弱,杜绝演化成环。
        {
            let weak_eps = Arc::downgrade(&self.endpoints);
            sender.set_endpoint_gen_resolver(Arc::new(move |p: &str| {
                weak_eps.upgrade().and_then(|e| e.current_generation(p))
            }));
        }

        // 集群装配:cluster 收到对端事件 → sender.send_local(本地分发);
        //   sender.send/relay → cluster.publish(跨节点)。Weak 避免 Arc 环。
        let cluster_incarnation = self.cluster_incarnation;
        let cluster_data_publisher = self.cluster_data_publisher;
        let cluster = match (self.cluster_node, self.notifier) {
            (Some(node), Some(notifier)) => {
                let s = sender.clone();
                let on_local: Arc<dyn Fn(Message, Mode) + Send + Sync> =
                    Arc::new(move |m, mode| {
                        s.send_local(&m, mode);
                    });
                // 校验已保证启用集群必有 incarnation(不退化墙钟)。
                let inc = cluster_incarnation.expect("validated: cluster requires incarnation");
                let c = Cluster::with_incarnation(node, notifier, on_local, inc);
                if let Some(publisher) = cluster_data_publisher {
                    // 每个集群只允许一个数据发布器，重复安装意味着控制面存在竞争所有者。
                    if c.set_data_publisher(publisher).is_err() {
                        unreachable!("新构造的 Cluster 不会重复安装 data publisher");
                    }
                }
                // 版本与本地订阅集合在同一把锁中采集，不能组合跨时刻的 presence 快照。
                let reg = registry.clone();
                c.set_presence_provider(Arc::new(move || reg.presence_snapshot()));
                // 本地订阅者数量跨过零点时广播增量，减少目录滞后；投递仍可能丢失，由周期全量校正。
                let cd = Arc::downgrade(&c);
                registry.set_group_change_hook(Arc::new(
                    move |group: &str, delta, version: i64| {
                        if let Some(c) = cd.upgrade() {
                            let present = matches!(delta, crate::session::GroupDelta::Gained);
                            c.publish_presence_delta(group, present, version);
                        }
                    },
                ));
                let cw = Arc::downgrade(&c);
                sender.set_cluster_publish(Arc::new(move |m: &Message, mode: Mode| {
                    if let Some(c) = cw.upgrade() {
                        c.publish(m, mode);
                    }
                }));
                Some(c)
            }
            _ => None,
        };

        let handler_sem = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_inflight_handlers,
        ));
        Ok(Server {
            shared: Arc::new(Shared {
                config: self.config,
                multiplex: self.multiplex,
                protocol_handler: self.protocol_handler,
                registry,
                endpoints: self.endpoints,
                sender,
                authorize,
                policy: self.policy,
                cluster,
                handler_sem,
                active: Arc::new(ConnTracker::default()),
                unauthenticated: Arc::new(ConnTracker::default()),
                bridge: self.bridge,
                cancel: CancellationToken::new(),
                tasks: TaskTracker::new(),
            }),
        })
    }
}

/// 保存运行服务的共享资源；用于提供发送器、会话表和端点表。
pub struct Server {
    /// 已装配且尚未移交监听运行句柄的服务状态。
    shared: Arc<Shared>,
}

/// 保存已绑定服务的运行句柄；用于关闭监听器和后台任务。
pub struct RunningServer {
    /// 实际绑定的 TCP 地址;传入 `127.0.0.1:0` 时可从这里取随机端口。
    pub local_addr: SocketAddr,
    /// 实际绑定的 WebSocket 地址;未启用 WS 时为 None。
    pub ws_local_addr: Option<SocketAddr>,
    /// 运行配置、会话目录与统一关闭责任。
    pub(crate) shared: Arc<Shared>,
    /// 取消表示宿主允许接流，独立于服务关闭令牌。
    admission: CancellationToken,
}

/// 直接 drop handle(不显式 shutdown)也必须停 accept/连接/集群任务、释放 listener 端口
///。cancel 令所有循环 select 退出;cluster.shutdown 停集群任务。
impl Drop for RunningServer {
    /// 业务作用：运行句柄被丢弃时请求集群与本地服务停止，保留取消路径的资源关闭责任。
    ///
    /// 参数说明：无。
    ///
    /// 返回：发出集群关闭及服务取消信号，不等待任务退出，端口实际释放取决于监听任务结束。
    fn drop(&mut self) {
        if let Some(c) = &self.shared.cluster {
            c.shutdown();
        }
        self.shared.cancel.cancel();
    }
}

impl RunningServer {
    /// 业务作用：在宿主完整就绪后放行 TCP 与 WebSocket 监听器。
    ///
    /// 参数说明：无。
    ///
    /// 返回：首次调用开放准入；关闭后调用不会恢复服务。
    pub fn activate(&self) {
        if !self.shared.cancel.is_cancelled() {
            self.admission.cancel();
        }
    }
    /// 业务作用：观察集群目录规模与广播拒绝计数，供部署监控节点身份增长及通道背压。
    ///
    /// 参数说明：无。
    ///
    /// 返回：单机为 None，集群返回本地快照；不向对端查询，不能据此保证所有节点或客户端可用。
    pub fn cluster_stats(&self) -> Option<crate::cluster::ClusterStats> {
        self.shared.cluster.as_ref().map(|c| c.stats())
    }

    /// 业务作用：发起集群与本地连接任务的取消，停止继续接流。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已请求关闭但不等待退出；内置协议可尝试发送关闭帧，不保证客户端收到，应用协议须自行响应取消。
    pub fn shutdown(&self) {
        if let Some(c) = &self.shared.cluster {
            c.shutdown(); // 停 presence + notifier(Redis)任务,否则永久存活
        }
        self.shared.cancel.cancel();
    }

    /// 业务作用：先请求停止集群输入与本地任务，再在同一截止点内等待受管任务实际退出。
    ///
    /// 参数说明：
    /// - `deadline`: 本次关闭的总时间预算,server 与 cluster 后台任务共同使用。
    ///
    /// 返回：所有被跟踪的等待均完成为 true，超时为 false；预算最多 365 天，不强制 abort 阻塞任务，也不确认已排队消息送达。
    #[must_use = "返回是否在 deadline 内排空,应检查"]
    pub async fn shutdown_graceful(&self, deadline: Duration) -> bool {
        // 单一绝对截止点:server 任务与集群任务**共用**同一预算,而非各给一份完整 deadline,
        // 故 shutdown_graceful 真正在 deadline 内返回,不会累加成数倍。
        // API 返回 bool 而非 Result；极端调用值收敛到 builder 同款上限，避免 deadline 加法 panic。
        let end = Instant::now() + deadline.min(MAX_SERVER_RUNTIME_DURATION);
        // 先关闭 cluster ingress，防止 Kafka/Redis reader 在 socket outbox 排空期间继续注入新 frame。
        if let Some(c) = &self.shared.cluster {
            c.shutdown();
        }
        self.shared.cancel.cancel();
        // tracker.close 标记可结束等待，不禁止 spawn；取消信号负责让任务生产者停止创建工作。
        self.shared.tasks.close();
        let mut ok = tokio::time::timeout_at(end, self.shared.tasks.wait())
            .await
            .is_ok();
        // 集群后台任务(presence + Redis reader/publisher)共用同一截止点 join。
        if let Some(c) = &self.shared.cluster {
            ok &= c.shutdown_join(end).await;
        }
        ok
    }

    /// 业务作用：当前活跃连接数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：接入后尚未结束的连接 owner 数量，包含协议分类及认证阶段，不包含独立 writer、handler 的完整退出证据。
    pub fn active_conns(&self) -> usize {
        self.shared.active.count.load(Ordering::Relaxed)
    }

    /// 业务作用：当前处于握手/AUTH 阶段的未认证连接数。
    ///
    /// 参数说明：无。
    ///
    /// 返回：尚未归还的未认证名额数量；max_unauthenticated 为零时不预留名额，该值为零并不代表所有连接已认证。
    pub fn unauthenticated_conns(&self) -> usize {
        self.shared.unauthenticated.count.load(Ordering::Relaxed)
    }

    /// 业务作用：返回会话注册表；用于查询和管理在线连接。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享注册表借用，索引可能含已登记但尚未发布认证成功的会话；取得表不授予业务投递权限。
    pub fn registry(&self) -> &Arc<SessionRegistry> {
        &self.shared.registry
    }

    /// 业务作用：提供按用户、订阅或端点筛选本地会话并按配置中继集群的发送入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享 Sender 借用；调用方负责业务授权，发送报告只描述本地准入，不代表客户端送达。
    pub fn sender(&self) -> &Arc<Sender> {
        &self.shared.sender
    }

    /// 业务作用：返回端点注册表；用于动态查看或更新事件入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：共享目录借用；替换或删除会经已安装 hook 关闭旧代会话，目录查询本身不保留后续连接资格。
    pub fn endpoints(&self) -> &Arc<EndpointRegistry> {
        &self.shared.endpoints
    }
}

impl Server {
    /// 业务作用：创建保持独立 TCP/WS 监听默认行为的服务构建器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未绑定网络或建立集群资源的构建器，共享监听需要显式启用。
    pub fn builder() -> ServerBuilder {
        ServerBuilder {
            config: ServerConfig::default(),
            multiplex: false,
            protocol_handler: None,
            endpoints: EndpointRegistry::new(),
            authorize: None,
            policy: crate::auth::deny_all(),
            cluster_node: None,
            notifier: None,
            cluster_data_publisher: None,
            cluster_incarnation: None,
            bridge: default_bridge(),
        }
    }

    /// 业务作用：取 Sender（**bind 前**即可），消除“listener 已 accept 但 Sender 尚未注入”的丢消息窗口。
    /// 业务可在 `build()` 后、`bind()` 前注入到自己的 handler 上下文。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已装配 Sender 的共享借用，不建立连接或证明集群就绪；业务权限仍由调用方验证。
    pub fn sender(&self) -> &Arc<Sender> {
        &self.shared.sender
    }

    /// 业务作用：返回只暴露 incarnation 判定能力的 Kafka 来源围栏。
    ///
    /// Server build 后、bind 前可把该句柄注入 WsKafkaRuntime；不会暴露节点目录写接口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：集群模式提供仅含来源围栏接口的共享句柄，单机为 None；围栏会更新本地实例与存活事实，不替代发送方认证。
    #[cfg(feature = "kafka")]
    pub fn kafka_source_fence(&self) -> Option<Arc<dyn crate::kafka::SourceFence>> {
        self.shared.cluster.as_ref().map(|cluster| {
            let registry: Arc<crate::cluster::NodeRegistry> = Arc::clone(cluster.nodes());
            registry as Arc<dyn crate::kafka::SourceFence>
        })
    }

    /// 业务作用：绑定监听地址并启动服务；用于接受 TCP 或 WebSocket 连接。
    ///
    /// 参数说明：无。
    ///
    /// 返回：完成监听准备后立即开放准入并交回运行句柄；地址、监听或不允许降级的集群启动失败时返回 IO 错误。
    pub async fn bind(self) -> std::io::Result<RunningServer> {
        let running = self.bind_suspended().await?;
        running.activate();
        Ok(running)
    }

    /// 业务作用：绑定监听地址并准备集群设施，保留业务连接准入屏障。
    ///
    /// 参数说明：无。
    ///
    /// 返回：资源准备成功时返回未接流的 owner；失败或取消准备时关闭本次设施。
    pub async fn bind_suspended(self) -> std::io::Result<RunningServer> {
        self.validate_listener_mode()?;
        // **先**把所有 listener bind 成功(可失败步骤前置),再启动 cluster / spawn accept——
        // 否则 bind 失败时已 start 的 cluster 不回滚、已 spawn 的 accept 泄漏。
        let tcp = listen_reuseaddr(&self.shared.config.addr)?;
        let ws_listener = match &self.shared.config.ws_addr {
            Some(ws) => Some(listen_reuseaddr(ws)?),
            None => None,
        };
        self.bind_listeners(tcp, ws_listener).await
    }

    /// 业务作用：接管宿主已经绑定的 socket，保留既有集群准备和业务准入屏障。
    ///
    /// 参数说明：
    ///
    /// - `listener`：由宿主绑定并移交唯一所有权的 TCP listener。
    ///
    /// 返回：未接流的运行句柄；失败或取消关闭 socket，不进行探测后的二次绑定。
    pub async fn bind_suspended_on(self, listener: TcpListener) -> std::io::Result<RunningServer> {
        self.validate_listener_mode()?;
        // 一个移交结果只代表一个真实端口，禁止在接管过程中暗中增加另一监听地址。
        if self.shared.config.ws_addr.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "prebound listener cannot have a separate WebSocket address",
            ));
        }
        self.bind_listeners(listener, None).await
    }

    /// 业务作用：在监听产生副作用前确认共享端口具有唯一协议解释。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原有独立监听不受影响；共享入口存在地址冲突或歧义长度时返回 InvalidInput。
    fn validate_listener_mode(&self) -> std::io::Result<()> {
        if self.shared.protocol_handler.is_some() && self.shared.cluster.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "custom protocol owns its message routing and cannot use legacy cluster relay",
            ));
        }
        // HTTP GET 首字节必须落在合法原生帧长度之外，避免同一输入有两个解释。
        if self.shared.multiplex
            && (self.shared.config.ws_addr.is_some()
                || self.shared.config.max_frame > 64 * 1024 * 1024)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "multiplex requires no separate ws_addr and max_frame <= 64 MiB",
            ));
        }
        Ok(())
    }

    /// 业务作用：为已绑定入口建立集群准备与统一关闭责任。
    ///
    /// 参数说明：
    ///
    /// - `tcp`：主入口的已绑定 listener。
    /// - `ws_listener`：可选独立 WebSocket 入口。
    ///
    /// 返回：集群准备完成后返回关闭准入的 owner；任一准备失败不开放监听。
    async fn bind_listeners(
        self,
        tcp: TcpListener,
        ws_listener: Option<TcpListener>,
    ) -> std::io::Result<RunningServer> {
        let local_addr = tcp.local_addr()?;
        let ws_local_addr = match &ws_listener {
            Some(l) => Some(l.local_addr()?),
            None => None,
        };

        // 在集群任务可能启动前建立同步关闭责任，取消 bind future 也不会留下无 owner 的循环。
        let running = RunningServer {
            local_addr,
            ws_local_addr,
            shared: self.shared.clone(),
            admission: CancellationToken::new(),
        };

        // listener 都就位后才启动 cluster(reader 定好游标)+ 周期 presence。
        // FailFast(默认):集群未就绪 → 回滚 cluster 并 bind 失败,不"假装已启动"。
        if let Some(c) = &self.shared.cluster {
            if let Err(e) = c
                .start_with_timeout(self.shared.config.cluster_ready_timeout)
                .await
            {
                if self.shared.config.cluster_degraded {
                    tracing::warn!("cluster start failed (degraded, continuing): {e:?}");
                } else {
                    c.shutdown(); // 回滚已 spawn 的集群任务
                    return Err(std::io::Error::other(format!(
                        "cluster start failed: {e:?}"
                    )));
                }
            }
            c.start_presence(DEFAULT_NODE_TTL / 3);
        }

        let shared = self.shared.clone();
        self.shared.tasks.spawn(accept_loop(
            tcp,
            shared.clone(),
            false,
            running.admission.clone(),
        ));
        tracing::info!("rust-ws TCP server bound on {local_addr}");
        if let Some(ws_listener) = ws_listener {
            self.shared.tasks.spawn(accept_loop(
                ws_listener,
                shared.clone(),
                true,
                running.admission.clone(),
            ));
            tracing::info!("rust-ws WS server bound on {}", ws_local_addr.unwrap());
        }

        Ok(running)
    }
}

/// 业务作用：精确判定 socket.io / engine.io v4 WebSocket 连接，避免普通连接绕过自身升级门禁。
///
/// 按 query 参数逐对解析,要求 `EIO=4` **且** `transport=websocket`。不再用子串 `contains("EIO")`——
/// 那会把 `?NOT_EIO=1`、`?device_EIO=x` 等 NASA 连接误分流到 socket.io(首帧错发 engine.io
/// OPEN 文本,且跳过 NASA upgrade path 校验);`transport=polling` 也不属于本 WS-only 实现。
///
/// 参数说明：
/// - `query`: 查询对象或 query 参数集合。
///
/// 返回：同时满足 `EIO=4` 与 `transport=websocket` 时返回 true，否则返回 false。
#[cfg(feature = "socketio")] // 未编译 socketio 时不参与判定(is_sio 恒 false)
fn is_socketio_query(query: &str) -> bool {
    let mut eio_v4 = false;
    let mut ws_transport = false;
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        let k = kv.next().unwrap_or("");
        let v = kv.next().unwrap_or("");
        match k {
            "EIO" => eio_v4 = v == "4",
            "transport" => ws_transport = v == "websocket",
            _ => {}
        }
    }
    eio_v4 && ws_transport
}

/// 业务作用：带 SO_REUSEADDR 的监听(便于重启快速重绑端口)。
///
/// 参数说明：
/// - `addr`: 可解析为 SocketAddr 的本地 IP 与端口，不执行域名解析。
///
/// 返回：设置 SO_REUSEADDR 并以 backlog 1024 绑定成功的 listener；解析、socket 设置、绑定或监听失败时传播错误。
fn listen_reuseaddr(addr: &str) -> std::io::Result<TcpListener> {
    let sa: SocketAddr = addr.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("bad addr {addr}: {e}"),
        )
    })?;
    let socket = if sa.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(sa)?;
    socket.listen(1024)
}

/// 业务作用：在宿主放行后接收连接，保留分类、握手与认证共用的时间上限。
///
/// 参数说明：
///
/// - `listener`：实际接收连接的入口。
/// - `shared`：服务运行态与生命周期信号。
/// - `is_ws`：指定此 listener 是否为独立 WebSocket 入口。
/// - `admission`：宿主完成准备后的放行信号。
///
/// 返回：关闭时结束接收并释放 listener；连接任务由同一任务容器负责排空。
async fn accept_loop(
    listener: TcpListener,
    shared: Arc<Shared>,
    is_ws: bool,
    admission: CancellationToken,
) {
    // 优先观察停机，避免 Ready 与取消同时到达时接受新业务连接。
    tokio::select! {
        biased;
        _ = shared.cancel.cancelled() => return,
        _ = admission.cancelled() => {},
    }
    loop {
        tokio::select! {
            biased;
            _ = shared.cancel.cancelled() => break,
            res = listener.accept() => match res {
                Ok((stream, peer)) => {
                    // 连接总数上限(过载/DoS 背压):超过则立即关闭新连接,不 spawn 连接任务、不升级 WS。
                    // 任务启动前计入连接，协议分类也消耗总预算；多个 listener 仍可能同时竞争。
                    // 0 表示调用方显式关闭该层上限，未认证预算另行约束。
                    let max_conns = shared.config.max_connections;
                    if max_conns != 0 && shared.active.count.load(Ordering::Relaxed) >= max_conns {
                        tracing::warn!("connection rejected: max_connections {} reached", max_conns);
                        drop(stream); // 关闭 TCP(FIN/RST),不占连接槽
                        continue;
                    }
                    let unauth_guard = match shared.config.max_unauthenticated {
                        0 => None,
                        max => match UnauthGuard::try_new(shared.unauthenticated.clone(), max) {
                            Some(guard) => Some(guard),
                            None => {
                                tracing::warn!("connection rejected: max_unauthenticated {} reached", max);
                                drop(stream);
                                continue;
                            }
                        },
                    };
                    let shared2 = shared.clone();
                    let auth_deadline = Instant::now() + shared.config.auth_timeout;
                    let connection = ConnGuard::new(shared.active.clone());
                    // 连接任务也进 TaskTracker → graceful shutdown 能 join。
                    shared.tasks.spawn(async move {
                        let _connection = connection;
                        if let Some(handler) = shared2.protocol_handler.clone() {
                            handle_protocol(
                                stream,
                                shared2,
                                handler,
                                peer,
                                is_ws,
                                unauth_guard,
                                auth_deadline,
                            )
                            .await;
                        } else if !is_ws && shared2.multiplex {
                            handle_multiplex(stream, shared2, unauth_guard, auth_deadline).await;
                        } else if is_ws {
                            handle_ws(stream, shared2, unauth_guard, auth_deadline).await;
                        } else {
                            handle_tcp(stream, shared2, unauth_guard, auth_deadline).await;
                        }
                    });
                }
                Err(e) => tracing::warn!("accept error: {e}"),
            },
        }
    }
}

/// 业务作用：在既有连接和未认证配额内执行应用协议，认证确认前统一执行原始截止点。
///
/// 参数说明：
/// - `stream`：当前已接入连接。
/// - `shared`：受管服务状态和任务屏障。
/// - `handler`：应用选择的固定协议处理器。
/// - `peer`：系统确认的直接对端。
/// - `websocket_only`：入口是否只允许 WebSocket。
/// - `unauthenticated`：尚未完成认证时保留的容量。
/// - `deadline`：accept 时确定的认证截止点。
///
/// 返回：协议结束或认证截止后释放连接责任；所有登记子任务仍属于服务排空范围。
async fn handle_protocol(
    stream: TcpStream,
    shared: Arc<Shared>,
    handler: ProtocolHandler,
    peer: std::net::SocketAddr,
    websocket_only: bool,
    unauthenticated: Option<UnauthGuard>,
    deadline: Instant,
) {
    let (admitted, received) = tokio::sync::oneshot::channel();
    let context = ProtocolContext {
        session_id: uuid::Uuid::new_v4().to_string(),
        peer,
        deadline,
        websocket_only,
        cancel: shared.cancel.child_token(),
        tasks: shared.tasks.clone(),
        unauthenticated,
        admitted: Some(admitted),
    };
    let future = handler(stream, context);
    tokio::pin!(future);
    // 认证确认已在发送端复验原始截止点；通知与计时器同时就绪时，不能撤销期限内取得的资格。
    // 停机仍优先结束连接，未确认认证的连接继续受同一截止点约束。
    tokio::select! {
        biased;
        _ = shared.cancel.cancelled() => {},
        _ = &mut future => {},
        result = received => {
            if result.is_ok() {
                future.await;
            }
        },
        _ = tokio::time::sleep_until(deadline) => {},
    }
}

/* ============================== transport 无关的 inbound ============================== */

/// 业务作用：在共享监听器上选择唯一协议处理器，保持原始字节交给原协议解析器。
///
/// 参数说明：
///
/// - `stream`：等待协议分类的已接入连接。
/// - `shared`：服务运行态与生命周期信号。
/// - `unauth_guard`：分类期间保留的未认证连接容量。
/// - `auth_deadline`：接入时固定的认证截止点。
///
/// 返回：连接结束后释放容量；超时、未知前缀或停机均关闭连接，不尝试备用解码器。
async fn handle_multiplex(
    stream: TcpStream,
    shared: Arc<Shared>,
    unauth_guard: Option<UnauthGuard>,
    auth_deadline: Instant,
) {
    let mut prefix = [0_u8; 1];
    let read = tokio::select! {
        biased;
        _ = shared.cancel.cancelled() => return,
        read = tokio::time::timeout_at(auth_deadline, stream.peek(&mut prefix)) => read,
    };
    // 不读取或分配客户端声明的长度；分类失败不能占用新的协议状态和缓冲。
    if !matches!(read, Ok(Ok(1))) {
        return;
    }
    match prefix[0] {
        b'G' => handle_ws(stream, shared, unauth_guard, auth_deadline).await,
        0..=4 => handle_tcp(stream, shared, unauth_guard, auth_deadline).await,
        _ => {}
    }
}

/// 入站帧来源:TCP 直读 framed;WS 把 binary message 累积后用同一 FrameCodec 解。
enum Inbound {
    /// 直接从 TCP 读取带长度头的原生帧。
    Tcp(FramedRead<OwnedReadHalf, FrameCodec>),
    /// 将 WebSocket 二进制消息累积为原生帧。
    Ws(WsInbound),
}

impl Inbound {
    /// 业务作用：读取下一帧消息；用于统一 TCP 和 WebSocket 的入站循环。
    ///
    /// 参数说明：无。
    ///
    /// 返回：完整帧为 Some(Ok)，已暴露的读取或协议错误为 Some(Err)，连接结束为 None；不在本方法设置认证或心跳截止点。
    async fn next_frame(&mut self) -> Option<std::io::Result<Frame>> {
        match self {
            Inbound::Tcp(f) => f.next().await,
            Inbound::Ws(w) => w.next_frame().await,
        }
    }
}

/// WS 入站适配:累积 binary 字节 → 同一 FrameCodec 解;支持 1 帧/消息、多帧/消息、帧跨消息。
struct WsInbound {
    /// 已升级连接的独占读取半边。
    ws: WsSplit,
    /// 检查内部 declared 长度并提取帧的解码器。
    codec: FrameCodec,
    /// 已从外层消息复制、尚未被内部帧解码消费的字节。
    buf: BytesMut,
}

impl WsInbound {
    /// 业务作用：为 WebSocket 二进制流建立 NASA 帧累积器，支持帧跨消息及单消息内多帧。
    ///
    /// 参数说明：
    /// - `ws`: 已升级 WebSocket 的独占读取半边，外层容量由握手配置限制。
    /// - `max_frame`: 单条 frame 允许的最大 declared 长度。
    ///
    /// 返回：空缓冲与限定内部帧长度的读取适配器；不立即读取或预分配最大消息长度。
    fn new(ws: WsSplit, max_frame: usize) -> Self {
        Self {
            ws,
            codec: FrameCodec::new(max_frame),
            buf: BytesMut::new(),
        }
    }

    /// 业务作用：持续累积 WebSocket 二进制正文并提取一条完整 NASA 帧，控制消息不作为业务帧。
    ///
    /// 参数说明：无。
    ///
    /// 返回：完整帧为 Some(Ok)，内部解码或收到文本为 Some(Err)；WebSocket 关闭、读取错误或流结束均为 None，不区分外层错误原因。
    async fn next_frame(&mut self) -> Option<std::io::Result<Frame>> {
        loop {
            match self.codec.decode(&mut self.buf) {
                Ok(Some(frame)) => return Some(Ok(frame)),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }
            match self.ws.next().await {
                Some(Ok(WsMessage::Binary(d))) => self.buf.extend_from_slice(d.as_ref()),
                Some(Ok(WsMessage::Text(_))) => {
                    return Some(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "WS text frame not allowed (binary only)",
                    )));
                }
                Some(Ok(WsMessage::Close(_))) => return None,
                // Ping/Pong/Frame:WS 控制层,tungstenite 自动回 Pong;不当 NASA 帧。
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return None,
            }
        }
    }
}

/* ============================== TCP 接入 ============================== */

/// 业务作用：在原生 TCP 上建立会话，完成认证后进入业务分发。
///
/// 参数说明：
///
/// - `stream`：已选择原生 TCP 协议的连接。
/// - `shared`：服务运行态与生命周期信号。
/// - `unauth_guard`：认证完成前保留的连接容量。
/// - `auth_deadline`：接入时固定的认证截止点。
///
/// 返回：认证失败或连接结束后清理会话，不因协议分类重新计算认证期限。
async fn handle_tcp(
    stream: TcpStream,
    shared: Arc<Shared>,
    unauth_guard: Option<UnauthGuard>,
    auth_deadline: Instant,
) {
    let (read_half, write_half) = stream.into_split();
    let (outbox, rx) = Outbox::new(
        shared.config.outbox_cap,
        shared.config.outbox_max_bytes,
        shared.config.backpressure,
    );
    shared.tasks.spawn(tcp_writer_task(write_half, rx)); // writer 入 tracker → graceful 等其 flush

    let session = new_session(Transport::Tcp, Protocol::Nasa, outbox, &shared);
    let mut inbound = Inbound::Tcp(FramedRead::new(
        read_half,
        FrameCodec::new(shared.config.max_frame),
    ));
    serve(&mut inbound, &session, &shared, auth_deadline, unauth_guard).await;
}

/// 业务作用：运行 TCP 写出任务；用于把出站消息顺序写回客户端。
///
/// 参数说明：
/// - `wh`: TCP 写半边。
/// - `rx`: 会话 outbox 接收端，控制帧优先于业务帧取出。
///
/// 返回：队列关闭且取空或写失败后尝试关闭写半边；不独立观察服务取消，也无单次写超时，不把写入成功当成客户端处理确认。
async fn tcp_writer_task(mut wh: OwnedWriteHalf, mut rx: OutboxReceiver) {
    while let Some(bytes) = rx.recv().await {
        if wh.write_all(&bytes).await.is_err() {
            break;
        }
    }
    let _ = wh.shutdown().await;
}

/* ============================== WebSocket 接入 ============================== */

/// 业务作用：在同一认证期限内完成 HTTP 升级并选择原生或 Socket.IO 会话协议。
///
/// 参数说明：
///
/// - `stream`：等待 HTTP 升级的连接。
/// - `shared`：服务运行态与生命周期信号。
/// - `unauth_guard`：升级与认证期间保留的连接容量。
/// - `auth_deadline`：接入时固定的认证截止点。
///
/// 返回：未知路径、无效握手或认证超时关闭连接，结束后清理会话资源。
#[allow(clippy::result_large_err)]
async fn handle_ws(
    stream: TcpStream,
    shared: Arc<Shared>,
    unauth_guard: Option<UnauthGuard>,
    auth_deadline: Instant,
) {
    // 分类、升级和远程认证共用接入截止点，慢速前缀不能延长未认证资源占用。
    // upgrade 阶段:用 URL path 当 endpoint,先过 EndpointRegistry 校验,不通过返 404。
    // 同时探测 ?EIO=(socket.io / engine.io v4)→ 分流到 socket.io 处理。
    let eps = shared.endpoints.clone();
    let path_cell: Arc<std::sync::Mutex<Option<(String, bool)>>> =
        Arc::new(std::sync::Mutex::new(None));
    let cell = path_cell.clone();
    let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        let path = req.uri().path().to_string();
        // **feature-aware**:socketio 未编译时 `is_sio` 恒 false——否则
        // 握手按 socket.io 放行(跳过 NASA 404 校验)、握手后却按 NASA 处理,任意 path
        // 可白占完整 WS 握手 + 鉴权 deadline + 连接资源。探测与 handler 分流必须同一规则。
        #[cfg(feature = "socketio")]
        let is_sio = req.uri().query().map(is_socketio_query).unwrap_or(false);
        #[cfg(not(feature = "socketio"))]
        let is_sio = false;
        // NASA WS:URL path 即 endpoint,握手期校验。
        // socket.io:逻辑 endpoint 来自 CONNECT 的 namespace(URL path 常固定为 /socket.io/),
        //   故握手期不校验 path,推迟到 CONNECT 时按 namespace 校验。
        if !is_sio && !eps.contains(&path) {
            let err = tokio_tungstenite::tungstenite::http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Some("endpoint not found".to_string()))
                .unwrap();
            return Err(err);
        }
        *cell.lock().unwrap() = Some((path, is_sio));
        Ok(resp)
    };

    // 下沉外层上限到 tungstenite(粗粒度 DoS 保护):
    // **三层口径**
    //   - `max_frame` = 单条 NASA frame 内层 declared(FrameCodec 精确执行);
    //   - WS 外层 message = `ws_message_limit()`(默认 4×(max_frame+4)):一条 Binary 可
    //     聚合多条合法 NASA frame(WsInbound 支持多帧/消息、帧跨消息),不再被"单帧级"
    //     外层上限拒掉合法组合;
    //   - socket.io 入站文本另在 handle_socketio 按广告的 maxPayload(=max_frame)精确检查。
    // WebSocketConfig 是 #[non_exhaustive],只能 default + 改字段。
    #[allow(clippy::field_reassign_with_default)]
    let ws_cfg = {
        let limit = shared.config.ws_message_limit();
        let mut c = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
        c.max_message_size = Some(limit);
        c.max_frame_size = Some(limit);
        c
    };
    // 握手与 shutdown + 截止点同 select:半握手连接(连上不发 Upgrade)不能拖住 graceful,
    // 也不能无限占用连接槽;用连接级绝对 deadline,不另起一份。
    let handshake = tokio::time::timeout_at(
        auth_deadline,
        tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(ws_cfg)),
    );
    let ws = tokio::select! {
        r = handshake => match r {
            Ok(Ok(w)) => w,
            Ok(Err(e)) => {
                tracing::debug!("ws handshake failed: {e}");
                return;
            }
            Err(_) => {
                tracing::debug!("ws handshake timed out");
                return;
            }
        },
        _ = shared.cancel.cancelled() => return,
        // shutdown 期间未完成握手 → 放弃
    };
    let (path, is_sio) = match path_cell.lock().unwrap().take() {
        Some(p) => p,
        None => return,
    };

    // socket.io 分流(feature 开启时)。
    #[cfg(feature = "socketio")]
    if is_sio {
        handle_socketio(ws, shared, path, auth_deadline, unauth_guard).await;
        return;
    }
    let _ = is_sio; // feature 关时未用

    // NASA 原生 WS。
    let (sink, ws_stream) = ws.split();
    let (outbox, rx) = Outbox::new(
        shared.config.outbox_cap,
        shared.config.outbox_max_bytes,
        shared.config.backpressure,
    );
    shared.tasks.spawn(ws_writer_task(sink, rx));

    let session = new_session(Transport::Ws, Protocol::Nasa, outbox, &shared);
    session.set_endpoint(path); // WS:endpoint 来自 URL path
    let mut inbound = Inbound::Ws(WsInbound::new(ws_stream, shared.config.max_frame));
    serve(&mut inbound, &session, &shared, auth_deadline, unauth_guard).await;
}

/// 业务作用：运行 WebSocket 写出任务；用于把出站消息转换为协议帧。
///
/// 参数说明：
/// - `sink`: WebSocket 写入 sink。
/// - `rx`: 会话 outbox 接收端，取出的字节已经包含 NASA 长度头。
///
/// 返回：每条内部帧写为一个 Binary message，队列结束或写失败后尝试关闭 sink；不独立设置写超时或确认客户端处理。
async fn ws_writer_task(mut sink: WsSink, mut rx: OutboxReceiver) {
    while let Some(bytes) = rx.recv().await {
        // 一个 NASA frame → 一个 WS binary message(含 4B len 头,统一 wire)。
        if sink.send(WsMessage::Binary(bytes.into())).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

/* ============================== socket.io 接入(feature=socketio)============================== */

/// 业务作用：socket.io / engine.io v4 连接生命周期。与 NASA `serve` 平行:
/// engine.io OPEN → sio CONNECT(auth)→ EVENT/PING/PONG 循环。出站经 outbox 走 WS **Text** 帧。
/// payload 翻译由 `Shared.bridge`(默认 JSON 透传)负责。
///
/// 参数说明：
/// - `ws`: 已完成 HTTP 升级的 WebSocket 连接。
/// - `shared`: 提供认证器、端点目录、心跳和容量预算的服务状态。
/// - `path`: WebSocket 握手 URL path,在默认 namespace 下可作为 socket.io endpoint。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
/// - `unauth_guard`: 未认证连接名额的 RAII 占位守卫。
///
/// 返回：认证失败、协议关闭、读取结束、心跳超时或服务取消后清理会话；认证成功释放未认证名额，不等待独立 writer 结束。
/// 此入口处理文本协议，忽略二进制业务消息，不支持 Socket.IO 二进制附件或 polling transport。
#[cfg(feature = "socketio")]
async fn handle_socketio(
    ws: WsStream,
    shared: Arc<Shared>,
    path: String,
    auth_deadline: Instant,
    mut unauth_guard: Option<UnauthGuard>,
) {
    use crate::socketio as sio;

    let (sink, mut ws_stream) = ws.split();
    let (outbox, rx) = Outbox::new(
        shared.config.outbox_cap,
        shared.config.outbox_max_bytes,
        shared.config.backpressure,
    );
    shared.tasks.spawn(ws_text_writer_task(sink, rx));

    let session = new_session(Transport::Ws, Protocol::SocketIo, outbox, &shared);
    let cleanup = ConnCleanup::new(&session); // Drop/panic 时 exactly-once 清理
                                              // 注意:不在此设 endpoint —— socket.io 的逻辑 endpoint 由 CONNECT 的 namespace 决定
                                              //   (默认 namespace "/" 时回退到 URL path)。见 handle_sio_text 的 Auth 分支。

    // engine.io OPEN:首帧告知 sid + 心跳参数。
    let ping_interval = (shared.config.heartbeat_timeout.as_millis() as u64 / 2).max(1000);
    let ping_timeout = shared.config.heartbeat_timeout.as_millis() as u64;
    let open = sio::engineio_open(
        session.id(),
        ping_interval,
        ping_timeout,
        shared.config.max_frame,
    );
    let _ = session.send_control(Bytes::from(open));

    let hb_timeout = shared.config.heartbeat_timeout;
    let mut last_seen = Instant::now();
    // EIO v4:服务端每 ping_interval 发 engine.io PING,客户端回 PONG;久无 PONG 即超时。
    let mut ticker = tokio::time::interval(Duration::from_millis(ping_interval));
    ticker.tick().await;
    let mut authed = false;
    // 鉴权 deadline 是连接级绝对截止点，包含 Upgrade 握手耗时并与 NASA 使用同一窗口；
    // 客户端必须在该截止点前完成 CONNECT，否则关闭。
    // 连接级关闭令牌:外部 cleanup → 主循环立即退出(与 NASA 同语义)。
    let closed = session.closed_token();

    loop {
        tokio::select! {
            () = closed.cancelled() => break,
            // 外部 cleanup(endpoint 下线/替换)→ 任务退出
            msg = ws_stream.next() => match msg {
                Some(Ok(WsMessage::Text(t))) => {
                    // 入站按 OPEN 广告的 maxPayload(= max_frame)**精确**检查
                    // tungstenite 外层是聚合粗上限(ws_message_limit),不替代协议广告值。
                    if t.len() > shared.config.max_frame {
                        tracing::debug!(
                            "sio inbound {}B exceeds advertised maxPayload {}, closing",
                            t.len(),
                            shared.config.max_frame
                        );
                        break;
                    }
                    if !handle_sio_text(
                        t.as_str(),
                        &path,
                        &session,
                        &shared,
                        &mut authed,
                        &mut last_seen,
                        auth_deadline,
                    )
                    .await
                    {
                        break;
                    }
                    if authed {
                        drop(unauth_guard.take());
                    }
                }
                Some(Ok(WsMessage::Close(_))) => break,
                // 二进制附件不进入文本分派；WS Ping/Pong/Frame 由 tungstenite 自动处理。
                Some(Ok(_)) => {}
                _ => break,
            },
            // 鉴权超时:未认证且过了 deadline → 关闭(provider 永挂时下面还有 timeout 兜底)。
            _ = tokio::time::sleep_until(auth_deadline), if !authed => {
                let _ = session.send_control(Bytes::from(sio::sio_connect_error("auth timeout", "/")));
                break;
            }
            _ = ticker.tick() => {
                if last_seen.elapsed() >= hb_timeout {
                    break;
                }
                let _ = session.send_control(Bytes::from(sio::engineio_ping()));
            }
            _ = shared.cancel.cancelled() => {
                let _ = session.send_control(Bytes::from(sio::sio_disconnect()));
                break;
            }
        }
    }

    // 先撤销索引再按生命周期通知断连，防止关闭后的订阅仍被本地投递命中。
    drop(cleanup);
}

/// 业务作用：处理一条 socket.io 入站文本帧，并在鉴权、协议或连接状态失效时要求关闭连接。
///
/// 参数说明：
/// - `text`: socket.io 文本包内容。
/// - `url_path`: WebSocket 握手 URL path,用于默认 namespace 的 endpoint 回退。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 提供认证器、端点代次、桥接器、处理容量和服务取消信号的运行态。
/// - `authed`: 当前连接是否已经完成认证。
/// - `last_seen`: 最近一次 Engine.IO PING/PONG 的本地时刻，业务 EVENT 不刷新。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
///
/// 返回：连接可继续时为 true；认证、端点或连接状态失效时为 false，由外层清理。
/// EVENT 的空 ACK 只表示已走过接收分派入口，不证明处理器被调度或业务完成；未知包可被忽略并返回 true。
#[cfg(feature = "socketio")]
#[allow(clippy::too_many_arguments)] // 连接级状态较多;拆 struct 反而更绕,内部 helper 可接受
async fn handle_sio_text(
    text: &str,
    url_path: &str,
    session: &Arc<Session>,
    shared: &Arc<Shared>,
    authed: &mut bool,
    last_seen: &mut Instant,
    auth_deadline: Instant,
) -> bool {
    use crate::socketio::{self as sio, Inbound as SioInbound};

    // select 竞态兜底:closed 会话的任何
    // 入站包都不得继续刷新心跳/执行逻辑。
    if session.is_closed() {
        return false;
    }
    match sio::parse_inbound(text) {
        SioInbound::Auth {
            token,
            namespace,
        } => {
            if *authed {
                return true; // 重复 CONNECT:忽略
            }
            // namespace 长度前置校验:超长 ns 会让 CONNECT ok / 回显它的
            // connect_error 超 max_frame 被丢 → 客户端只见静默断开。合法 ns 必为已注册
            // endpoint(≤ MAX_ENDPOINT_LEN),超长直接给 "/" 上的短错误并关闭。
            if namespace.len() > crate::endpoint::MAX_ENDPOINT_LEN {
                let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                    "namespace too long",
                    "/",
                )));
                return false;
            }
            // 逻辑 endpoint:非默认 namespace 即 endpoint;默认 "/" 回退到 URL path。
            let ep = if namespace != "/" {
                namespace.clone()
            } else {
                url_path.to_string()
            };
            if !shared.endpoints.contains(&ep) {
                let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                    "namespace not registered",
                    &namespace,
                )));
                return false;
            }
            session.set_endpoint(ep.clone());
            session.set_sio_namespace(namespace.clone()); // 出站事件按此 namespace 成帧
            let req = naws_proto::AuthRequest {
                token: Some(token),
                endpoint: Some(ep),
                ..Default::default()
            };
            // authorize 用**连接级总 deadline**(timeout_at),不是从 CONNECT 起重新计时——
            // 否则"慢发 CONNECT + 慢 authorize"可叠加超过 auth_timeout 仍成功。
            // 同时与 shutdown 同一 select:shutdown 触发即取消 authorize、立刻收场。
            let authorized = tokio::select! {
                a = tokio::time::timeout_at(
                    auth_deadline,
                    (shared.authorize)(req, AuthContext::from_session(session)),
                ) => a,
                _ = shared.cancel.cancelled() => {
                    let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                        "server shutting down",
                        &namespace,
                    )));
                    return false;
                }
            };
            let result = match authorized {
                Ok(r) => r,
                Err(_elapsed) => {
                    let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                        "auth timeout",
                        &namespace,
                    )));
                    return false;
                }
            };
            match result {
                AuthResult::Ok {
                    uid,
                } if !uid.trim().is_empty() => {
                    // 与 NASA run_auth 同一套"鉴权 ready 状态机"+
                    // endpoint 代际绑定/复核
                    // set_uid → 绑定 slot 快照 → register → 复核 generation →
                    // 入队 connect_ok(校验 Queued)→ set_authenticated → fire_connect(bound)。
                    session.set_uid(uid);
                    let ep_path = session.endpoint().unwrap_or_default().to_string();
                    let Some((generation, endpoint)) = shared.endpoints.slot(&ep_path) else {
                        let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                            "endpoint removed during auth",
                            &namespace,
                        )));
                        return false;
                    };
                    session.bind_endpoint(generation, endpoint);
                    // **register 前预编码** connect_ok 并验长:endpoint 注册期
                    // 上限 + 上面的 ns 前置校验已保证不超;此处是最后一道防线——超限给明确
                    // 短错误再关闭,绝不"注册成功却发不出成功帧"。
                    let ok_pkt = sio::sio_connect_ok(session.id(), &namespace);
                    if ok_pkt.len() > shared.config.max_frame {
                        let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                            "namespace too long",
                            "/",
                        )));
                        return false;
                    }
                    if !shared.registry.register(session) {
                        let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                            "registration rejected",
                            &namespace,
                        )));
                        return false;
                    }
                    // 绑定快照后仍可能发生替换；注册后复验，确保旧快照会话不会被开放为当前代连接。
                    if shared.endpoints.current_generation(&ep_path) != Some(generation) {
                        let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                            "endpoint replaced during auth",
                            &namespace,
                        )));
                        return false;
                    }
                    let queued = session.send_control(Bytes::from(ok_pkt));
                    if queued != crate::session::SendOutcome::Queued {
                        return false; // 连接已死:不置 authenticated、不 fire_connect
                    }
                    // 生命周期串行。
                    if !session.begin_connect() {
                        return false;
                    }
                    session.set_authenticated(true);
                    fire_connect(session);
                    if session.finish_connect() {
                        fire_disconnect(session); // on_connect 期间被下线:补配对 disconnect
                        return false;
                    }
                    *authed = true;
                    true
                }
                AuthResult::Ok {
                    ..
                } => {
                    let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                        "auth returned blank uid",
                        &namespace,
                    )));
                    false
                }
                AuthResult::Fail {
                    reason,
                } => {
                    // 限制业务原因的原始字节长度；JSON 转义仍可能扩大文本，最终入队还受 max_frame 限制。
                    let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                        &truncate_reason(&reason),
                        &namespace,
                    )));
                    false
                }
            }
        }
        SioInbound::Event {
            event,
            data,
            ack_id,
        } => {
            if !*authed {
                // CONNECT 之前发 EVENT 属协议错误 → 关闭连接。
                let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                    "event before connect",
                    "/",
                )));
                return false;
            }
            // Socket.IO EVENT 不携带 NASA 路由字段，只触发本端 handler；
            // 需要对外投递时由 handler 在完成业务授权后显式调用发送入口。
            let payload = shared.bridge.inbound(&event, &data.to_string());
            let msg = Message {
                events: Some(vec![Some(event)]),
                payload: Some(payload),
                from_uid: session.uid().map(String::from),
                from_client: Some(session.id().to_string()),
                ..Default::default()
            };
            let Some(ep_path) = session.endpoint() else {
                return false;
            };
            // 绑定快照分派 + 代际兜底
            // 旧会话不执行新 endpoint handler;endpoint 已变更 → DISCONNECT 收场。
            let Some(bound) = session.bound_endpoint() else {
                return false;
            };
            if shared.endpoints.current_generation(ep_path) != Some(bound.generation) {
                let _ = session.send_control(Bytes::from(sio::sio_disconnect()));
                return false;
            }
            let endpoint = bound.endpoint.clone();
            if has_routing(&msg) {
                match (shared.policy)(&PolicyContext::new(session), &msg) {
                    RouteDecision::Deny => return true,
                    RouteDecision::HandleOnly => {}
                    RouteDecision::RelayAndHandle => {
                        shared.sender.relay(&msg, Mode::VarintTlv);
                    }
                }
            }
            dispatch_handlers(&endpoint, &msg, session, shared);
            // 空 ACK 只确认记录经过接收分派，不等待异步 handler，也不能代表过载时已受理事件。
            if let Some(id) = ack_id {
                let _ = session.send_control(Bytes::from(sio::sio_ack(
                    id,
                    serde_json::Value::Array(Vec::new()),
                )));
            }
            true
        }
        SioInbound::Ping => {
            // EIO v3 客户端主动 PING → 回 PONG;刷新心跳。
            *last_seen = Instant::now();
            let _ = session.send_control(Bytes::from(sio::engineio_pong()));
            true
        }
        SioInbound::Pong => {
            *last_seen = Instant::now();
            true
        }
        SioInbound::Close => false,
        SioInbound::Ignore => true,
    }
}

/// 业务作用：socket.io 出站:每个 outbox 帧(UTF-8 文本字节)→ 一个 WS Text 消息。
///
/// 参数说明：
/// - `sink`: WebSocket 写入 sink。
/// - `rx`: 携带已编码 Engine.IO/Socket.IO 文本字节的会话 outbox 接收端。
///
/// 返回：队列结束或写失败后尝试关闭 sink；每条消息复制为 String，非法 UTF-8 使用替换字符，不提供业务送达确认或独立写超时。
#[cfg(feature = "socketio")]
async fn ws_text_writer_task(mut sink: WsSink, mut rx: OutboxReceiver) {
    while let Some(bytes) = rx.recv().await {
        let text = String::from_utf8_lossy(&bytes).into_owned();
        if sink.send(WsMessage::Text(text)).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

/* ============================== 公共连接生命周期 ============================== */

/// 业务作用：为已接入连接创建未认证会话，关联协议、发送队列与本地会话目录。
///
/// 参数说明：
/// - `transport`: 当前连接使用的传输类型。
/// - `protocol`: 当前连接使用的协议类型。
/// - `outbox`: 会话待发送消息队列。
/// - `shared`: 提供注册表、payload bridge 与最大帧容量的服务装配状态。
///
/// 返回：带新随机标识的共享会话；尚未设置 UID、绑定端点或注册在线索引，不赋予业务发送资格。
fn new_session(
    transport: Transport,
    protocol: Protocol,
    outbox: Outbox,
    shared: &Arc<Shared>,
) -> Arc<Session> {
    Session::new(
        new_session_id(),
        transport,
        protocol,
        outbox,
        &shared.registry,
        shared.bridge.clone(),
        shared.config.max_frame,
    )
}

/// 业务作用：transport 无关:鉴权 → 分派循环。清理交给 ConnCleanup(Drop 时执行,panic 也保证跑)。
/// `auth_deadline` 为**连接级**绝对截止点：TCP 从接入时刻起算，WS 从 Upgrade 前起算，
/// 并与握手共用同一份预算。
///
/// 参数说明：
/// - `inbound`: 已建立连接的入站读半边抽象。
/// - `session`: 尚未认证且已绑定出站队列的本连接会话。
/// - `shared`: 认证器、端点目录、接入预算与服务取消状态。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
/// - `unauth_guard`: 未认证连接名额的 RAII 占位守卫。
///
/// 返回：认证成功后归还未认证名额并进入分派；失败或分派结束触发会话清理，不等待独立 writer 排空，也不改变原认证截止点。
async fn serve(
    inbound: &mut Inbound,
    session: &Arc<Session>,
    shared: &Arc<Shared>,
    auth_deadline: Instant,
    mut unauth_guard: Option<UnauthGuard>,
) {
    let _cleanup = ConnCleanup::new(session);
    if run_auth(inbound, session, shared, auth_deadline).await {
        drop(unauth_guard.take());
        dispatch_loop(inbound, session, shared).await;
    }
    // 无论认证结果如何都由守卫撤销会话；断连通知是否执行由生命周期裁决。
}

/// 业务作用：鉴权状态机。外层 timeout 覆盖"等 AUTH 帧 + authorize 执行",用**连接级绝对
/// deadline**（WS 与 Upgrade 握手共用，总窗口恒为 auth_timeout）；
/// 同时可被 shutdown 取消——否则 authorize 慢/挂会让摘流延迟受 auth_timeout 控制。
///
/// 参数说明：
/// - `inbound`: 已建立连接的入站读半边抽象。
/// - `session`: 当前待认证会话，WS 已保存 URL 端点，TCP 从 AUTH 信封取得端点。
/// - `shared`: 提供认证器、动态端点目录、注册表、心跳配置及停机信号。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
///
/// 返回：非空白 UID、会话注册、端点代次复验、成功响应入队与连接回调流程均通过时为 true；其余为 false，由外层清理。
/// 超时覆盖首帧与认证器等待；入队成功不等于客户端收到，注册及同步连接回调不由该 timeout 抢占。
async fn run_auth(
    inbound: &mut Inbound,
    session: &Arc<Session>,
    shared: &Arc<Shared>,
    auth_deadline: Instant,
) -> bool {
    let auth_fut = tokio::time::timeout_at(auth_deadline, async {
        let frame = match inbound.next_frame().await {
            Some(Ok(f)) => f,
            _ => return Err("no AUTH frame".to_string()),
        };
        if frame.typ != frame_type::AUTH {
            return Err("AUTH expected as first frame".to_string());
        }
        // 控制帧固定 wire(控制帧必须 VARINT_TLV)——不接受任意 mode 字节。
        if frame.mode != wire_mode::VARINT {
            return Err(format!(
                "AUTH must be VARINT mode, got 0x{:02x}",
                frame.mode
            ));
        }
        let req = naws_proto::AuthRequest::decode(Mode::VarintTlv, &frame.payload)
            .map_err(|e| format!("auth payload malformed: {e}"))?;

        // endpoint:WS 已由 path 预设;TCP 从 AUTH 帧取。
        let ep = match session.endpoint() {
            Some(ep) => ep.to_string(),
            None => req
                .endpoint
                .clone()
                .ok_or_else(|| "endpoint missing".to_string())?,
        };
        if !shared.endpoints.contains(&ep) {
            return Err(format!("endpoint not registered: {ep}"));
        }
        session.set_endpoint(ep);

        match (shared.authorize)(req, AuthContext::from_session(session)).await {
            AuthResult::Ok {
                uid,
            } if !uid.trim().is_empty() => Ok(uid),
            AuthResult::Ok {
                ..
            } => Err("auth returned blank uid".to_string()),
            AuthResult::Fail {
                reason,
            } => Err(reason),
        }
    });

    // 鉴权与 shutdown 同一 select:shutdown 触发(或 sender 关闭)即取消 authorize、立刻收场。
    let outcome = tokio::select! {
        o = auth_fut => o,
        _ = shared.cancel.cancelled() => return auth_fail(session, "server shutting down"),
    };

    let uid = match outcome {
        Ok(Ok(uid)) => uid,
        Ok(Err(reason)) => return auth_fail(session, &reason),
        Err(_elapsed) => return auth_fail(session, "auth timeout"),
    };

    // **鉴权 ready 状态机**
    //   ① set_uid + **绑定 endpoint 快照**(generation + Arc),
    //      保持 authenticated=false ——"已注册但未 ready"的 pending 态;
    //   ② register:session 进 uid/endpoint 索引、对 fan-out 可见,但 send_business 统一
    //      拒绝未认证会话 → 并发投递不可能抢在成功帧前上线;
    //   ③ 注册后复核 generation：从绑定 slot 到注册期间端点可能被
    //      replace/unregister；此刻会话已注册、可被 mutation hook 枚举，两边至少一边
    //      看到对方:复核失败 → 直接收场(authenticated 仍 false,不会短暂进入认证态);
    //      复核通过 → mutation 必然晚于本检查,枚举会关掉本会话;
    //   ④ register 成功后**先**把 AUTH_RESP(ok) 入 control 队(校验 Queued);
    //   ⑤ 再 set_authenticated(true) 放行业务帧;
    //   ⑥ 最后 fire_connect(用绑定快照;其中 send_event 的帧必然排在 AUTH_RESP 之后)。
    // 不能把 AUTH_RESP 挪到 register 之前:register 失败时客户端会先收到成功响应。
    session.set_uid(uid);
    let ep_path = session.endpoint().unwrap_or_default().to_string();
    let Some((generation, endpoint)) = shared.endpoints.slot(&ep_path) else {
        return auth_fail(session, "endpoint removed during auth");
    };
    session.bind_endpoint(generation, endpoint);
    if !shared.registry.register(session) {
        return auth_fail(session, "registration rejected");
    }
    if shared.endpoints.current_generation(&ep_path) != Some(generation) {
        return auth_fail(session, "endpoint replaced during auth"); // cleanup 仅注销(未认证)
    }
    let resp = AuthResponse {
        ok: true,
        session_id: Some(session.id().to_string()),
        err_msg: None,
        heartbeat_timeout_ms: shared.config.heartbeat_timeout.as_millis() as i64,
    };
    let queued = session.send_control(encode_frame(
        frame_type::AUTH_RESP,
        wire_mode::VARINT,
        &resp.encode(Mode::VarintTlv).unwrap(),
    ));
    if queued != crate::session::SendOutcome::Queued {
        return false; // 连接已死/控制队列异常:不置 authenticated、不 fire_connect(cleanup 仅注销)
    }
    // **生命周期串行**:begin_connect(Connecting,先于 authenticated,
    // 杜绝"authenticated=true 而 phase=Pending"窗口)→ on_connect → finish_connect。
    // on_connect 期间被并发 replace/unregister:cleanup 只做逻辑注销并置
    // ClosingDuringConnect,on_disconnect 由 finish_connect 在 on_connect **返回后**补触发
    // ——绝不倒序并发,exactly once。
    if !session.begin_connect() {
        return false; // register 后被并发下线:跳过 on_connect(无配对 disconnect 需要触发)
    }
    session.set_authenticated(true);
    fire_connect(session);
    if session.finish_connect() {
        fire_disconnect(session); // on_connect 期间被下线:此刻补配对的 on_disconnect
        return false;
    }
    true
}

/// 业务作用：为认证失败尝试排入 AUTH_RESP 与 CLOSE 控制帧，让外层结束认证流程。
///
/// 参数说明：
/// - `session`: 当前连接会话或会话句柄。
/// - `reason`: 鉴权失败原因,会写入 AUTH_RESP 和 CLOSE 控制帧。
///
/// 返回：始终为 false；截断原因后尝试发送两条控制帧，不检查入队结果，不在此注销会话或等待网络写出。
fn auth_fail(session: &Arc<Session>, reason: &str) -> bool {
    // 业务 authorize 的失败 reason 不可控 → 截断,保证 AUTH_RESP/CLOSE 控制帧恒在
    // max_frame 内。
    let reason = truncate_reason(reason);
    let resp = AuthResponse {
        ok: false,
        session_id: None,
        err_msg: Some(reason.to_string()),
        heartbeat_timeout_ms: 0,
    };
    send_control(
        session,
        frame_type::AUTH_RESP,
        &resp.encode(Mode::VarintTlv).unwrap(),
    );
    let cr = ProtoCloseReason {
        code: ProtoCloseReason::CODE_AUTH_FAILED,
        reason: Some(reason.to_string()),
    };
    send_control(
        session,
        frame_type::CLOSE,
        &cr.encode(Mode::VarintTlv).unwrap(),
    );
    false
}

/// 业务作用：运行入站消息分发循环；用于持续读取帧并交给事件处理。
///
/// 参数说明：
/// - `inbound`: 已建立连接的入站读半边抽象。
/// - `session`: 已完成认证、用于判断连接关闭与控制帧发送的会话。
/// - `shared`: 提供心跳周期、事件分派依赖与服务取消信号。
///
/// 返回：读取结束、处理器要求关闭、会话取消、心跳超时或服务取消时结束，由外层执行清理；只有合法 PING 刷新原生协议心跳。
/// 检查周期最低为 500 毫秒，超时发现受周期及同步回调执行影响，不提供严格即时终止保证。
async fn dispatch_loop(inbound: &mut Inbound, session: &Arc<Session>, shared: &Arc<Shared>) {
    let hb_timeout = shared.config.heartbeat_timeout;
    let check = hb_timeout
        .checked_div(2)
        .unwrap_or(hb_timeout)
        .max(Duration::from_millis(500));
    let mut last_ping = Instant::now();
    let mut ticker = tokio::time::interval(check);
    ticker.tick().await;
    // 连接级关闭令牌:endpoint 下线/替换等**外部** cleanup 触发后,
    // 读循环立即退出——不依赖心跳超时、不依赖客户端配合(狂发 PING 也无法保活)。
    let closed = session.closed_token();

    loop {
        tokio::select! {
            f = inbound.next_frame() => match f {
                Some(Ok(frame)) => {
                    if !handle_frame(frame, session, shared, &mut last_ping).await {
                        break;
                    }
                }
                _ => break,
            },
            _ = closed.cancelled() => break,
            // 外部 cleanup → reader/任务退出(CLOSE 已由清理方入队)
            _ = ticker.tick() => {
                if last_ping.elapsed() >= hb_timeout {
                    let cr = ProtoCloseReason {
                        code: ProtoCloseReason::CODE_TIMEOUT,
                        reason: Some(format!("heartbeat timeout {}ms", hb_timeout.as_millis())),
                    };
                    send_control(
                        session,
                        frame_type::CLOSE,
                        &cr.encode(Mode::VarintTlv).unwrap(),
                    );
                    break;
                }
            }
            _ = shared.cancel.cancelled() => {
                let cr = ProtoCloseReason {
                    code: ProtoCloseReason::CODE_SERVER_SHUTDOWN,
                    reason: Some("server shutdown".into()),
                };
                send_control(
                    session,
                    frame_type::CLOSE,
                    &cr.encode(Mode::VarintTlv).unwrap(),
                );
                break;
            }
        }
    }
}

/// 业务作用：处理单个入站帧；用于区分控制帧、心跳和业务事件。
///
/// 参数说明：
/// - `frame`: 已通过外层长度校验的原生 NASA 帧。
/// - `session`: 当前已完成认证流程的会话。
/// - `shared`: 提供端点目录、路由策略与 handler 容量的服务状态。
/// - `last_ping`: 最近一次合法空正文 VARINT PING 的本地时刻，仅该类型允许更新。
///
/// 返回：合法心跳或可继续的事件为 true；会话已关闭、合法 CLOSE、非法或意外控制帧为 false，协议错误会尝试发送关闭响应。
async fn handle_frame(
    frame: Frame,
    session: &Arc<Session>,
    shared: &Arc<Shared>,
    last_ping: &mut Instant,
) -> bool {
    // select 竞态兜底:closed 会话的任何帧(含合法 PING)都不得继续刷新
    // 心跳/执行逻辑——否则恶意客户端可在外部 cleanup 与本帧的竞态窗口里保活连接任务。
    if session.is_closed() {
        return false;
    }
    match frame.typ {
        frame_type::PING => {
            // 控制帧固定 VARINT mode + PING 必须空 payload。
            if frame.mode != wire_mode::VARINT {
                return close_protocol(session, "PING must be VARINT mode");
            }
            if !frame.payload.is_empty() {
                return close_protocol(session, "PING must have empty payload");
            }
            *last_ping = Instant::now(); // 只有 PING 刷新心跳
            let _ = session.send_control(pong());
            true
        }
        frame_type::EVENT => handle_event(frame, session, shared).await,
        frame_type::CLOSE => {
            // CLOSE 固定 VARINT + 必须是合法 CloseReason(拒绝畸形/trailing);校验后正常关闭。
            if frame.mode != wire_mode::VARINT
                || ProtoCloseReason::decode(Mode::VarintTlv, &frame.payload).is_err()
            {
                return close_protocol(session, "malformed CLOSE frame");
            }
            false
        }
        other => {
            let cr = ProtoCloseReason {
                code: ProtoCloseReason::CODE_PROTOCOL_ERROR,
                reason: Some(format!("unexpected frame type 0x{other:02x}")),
            };
            send_control(
                session,
                frame_type::CLOSE,
                &cr.encode(Mode::VarintTlv).unwrap(),
            );
            false
        }
    }
}

/// 业务作用：处理业务事件帧；用于查找端点回调并决定连接是否继续。
///
/// 参数说明：
/// - `frame`: EVENT 帧，模式字节决定 Message 信封的解码方式。
/// - `session`: 提供可信来源 UID、连接 ID 与已绑定端点快照的会话。
/// - `shared`: 提供当前端点代次、入站策略和本地及集群发送入口的运行态。
///
/// 返回：模式、信封或端点绑定失效时为 false；路由策略拒绝时丢弃该消息但返回 true，允许时按策略先中继再分派处理器。
/// 来源字段被会话身份覆盖；处理器过载可拒绝事件，因此 true 不表示业务处理成功。
async fn handle_event(frame: Frame, session: &Arc<Session>, shared: &Arc<Shared>) -> bool {
    let Some(mode) = Mode::from_ordinal(frame.mode) else {
        return close_protocol(session, "bad mode byte");
    };
    let mut msg = match Message::decode(mode, &frame.payload) {
        Ok(m) => m,
        Err(_) => return close_protocol(session, "message payload malformed"),
    };

    msg.from_uid = session.uid().map(String::from);
    msg.from_client = Some(session.id().to_string());

    let Some(ep_path) = session.endpoint() else {
        return false;
    };
    // **绑定快照分派 + 代际兜底**:handler 恒用鉴权时绑定的 endpoint
    // (旧会话绝不执行新 endpoint 代码);endpoint 已被 replace/unregister(与主动关闭
    // 竞态的窗口内)→ CLOSE(ENDPOINT_REMOVED) 收场。
    let Some(bound) = session.bound_endpoint() else {
        return false;
    };
    if shared.endpoints.current_generation(ep_path) != Some(bound.generation) {
        let cr = ProtoCloseReason {
            code: ProtoCloseReason::CODE_ENDPOINT_REMOVED,
            reason: Some(format!("endpoint removed or replaced: {ep_path}")),
        };
        send_control(
            session,
            frame_type::CLOSE,
            &cr.encode(Mode::VarintTlv).unwrap(),
        );
        return false;
    }
    let endpoint = bound.endpoint.clone();

    if has_routing(&msg) {
        // policy 是用户闭包,panic 隔离;panic 时按最安全的 Deny 处理。
        let decision = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (shared.policy)(&PolicyContext::new(session), &msg)
        }))
        .unwrap_or(RouteDecision::Deny);
        match decision {
            RouteDecision::Deny => return true,
            RouteDecision::HandleOnly => {}
            RouteDecision::RelayAndHandle => {
                shared.sender.relay(&msg, mode);
            }
        }
    }
    dispatch_handlers(&endpoint, &msg, session, shared);
    true
}

/// 业务作用：分发业务处理器任务；用于按并发策略执行事件回调。
///
/// 参数说明：
/// - `endpoint`: 会话认证时绑定并已复验代次的端点快照。
/// - `msg`: 含事件名与正文的消息；正文复制一次后由命中回调共享。
/// - `session`: 提供受控 SessionHandle 与连接级异步处理名额的会话。
/// - `shared`: 提供全局异步许可、任务 tracker 和服务关闭信号。
///
/// 返回：忽略空名、同条消息重复名与未知事件；inline 同步执行并隔离 panic，async 取得两层许可后登记任务，容量不足则跳过。
/// 不等待异步业务结果，不保证各事件或同 UID 串行；异步任务观察服务取消，普通会话断开不单独取消已登记任务。
fn dispatch_handlers(
    endpoint: &Endpoint,
    msg: &Message,
    session: &Arc<Session>,
    shared: &Arc<Shared>,
) {
    let Some(events) = &msg.events else {
        return;
    };
    let payload = Bytes::from(msg.payload.clone().unwrap_or_default());
    let mut seen = std::collections::HashSet::new();
    for ev in events.iter().flatten() {
        if ev.is_empty() || !seen.insert(ev.as_str()) {
            continue;
        }
        let Some(handler) = endpoint.event(ev) else {
            continue;
        };
        match handler {
            // Inline:同步短操作,读循环内直接调;panic 隔离防 unwind 跳过清理。
            // 传**受控 SessionHandle**(非裸 Arc<Session>),业务不能篡改生命周期/发控制帧。
            EventHandler::Inline(f) => {
                let h = SessionHandle::new(session.clone());
                isolate("inline handler", || f(h, payload.clone()))
            }
            // Async:有界并发——先占**本连接**名额、再抢**全局** permit,两者都拿到才 spawn
            // 进 TaskTracker 且 select cancel,graceful shutdown 时取消未完成 handler 并 join。
            EventHandler::Async(f) => {
                // per-conn 配额:防单连接狂发事件抢占全局池、饿死其它连接。占不到直接拒绝该事件。
                let per_conn = match session
                    .try_reserve_handler(shared.config.max_inflight_handlers_per_conn)
                {
                    Some(g) => g,
                    None => {
                        tracing::warn!(
                            "async handler '{ev}' rejected: per-conn in-flight limit reached on {}",
                            session.id()
                        );
                        continue;
                    }
                };
                match shared.handler_sem.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let fut = f(SessionHandle::new(session.clone()), payload.clone());
                        let cancel = shared.cancel.clone();
                        shared.tasks.spawn(async move {
                            let _permit = permit; // 全局许可:持有到 handler 结束
                            let _per_conn = per_conn; // 本连接名额:同上,任务结束/panic 都释放(Drop)
                            tokio::select! {
                                _ = fut => {},
                                _ = cancel.cancelled() => {},
                                // shutdown:取消未完成 handler
                            }
                        });
                    }
                    Err(_) => {
                        // 全局池满:per_conn 在此作用域结束自动释放(Drop),不泄漏本连接名额。
                        tracing::warn!(
                            "async handler '{ev}' rejected: global in-flight limit reached on {}",
                            session.id()
                        );
                    }
                }
            }
        }
    }
}

/// 业务作用：识别客户端消息是否请求选择投递目标，决定是否必须执行入站路由策略。
///
/// 参数说明：
/// - `msg`: 待检查的 NASA 业务消息。
///
/// 返回：group 非空或 uids/routers 至少有一个非空字符串时为 true；不裁剪空白，不把 excludes 或来源字段当作目标。
fn has_routing(msg: &Message) -> bool {
    let group = msg.group.as_deref().map(|g| !g.is_empty()).unwrap_or(false);
    let nonempty = |v: &Option<Vec<Option<String>>>| {
        v.as_ref()
            .map(|a| a.iter().flatten().any(|s| !s.is_empty()))
            .unwrap_or(false)
    };
    group || nonempty(&msg.uids) || nonempty(&msg.routers)
}

/// 业务作用：为协议错误尝试排入关闭控制帧，并通知调用方结束读循环。
///
/// 参数说明：
/// - `session`: 当前连接会话或会话句柄。
/// - `reason`: 写入 CLOSE 的原因，按 MAX_REASON_LEN 字节与 UTF-8 边界截断。
///
/// 返回：始终为 false；不直接注销会话或停止后台任务，实际清理由上层执行，不保证关闭帧入队或送达。
fn close_protocol(session: &Arc<Session>, reason: &str) -> bool {
    let reason = truncate_reason(reason); // CLOSE reason 截断,控制帧体积可控
    let cr = ProtoCloseReason {
        code: ProtoCloseReason::CODE_PROTOCOL_ERROR,
        reason: Some(reason.to_string()),
    };
    send_control(
        session,
        frame_type::CLOSE,
        &cr.encode(Mode::VarintTlv).unwrap(),
    );
    false
}

/// 业务作用：发送控制帧；用于返回鉴权、错误或协议级响应。
///
/// 参数说明：
/// - `session`: 当前连接会话或会话句柄。
/// - `typ`: NASA frame type,例如 AUTH_RESP、PONG 或 CLOSE。
/// - `payload`: 已按控制帧 schema 编码完成的 payload 字节。
///
/// 返回：以固定 VARINT 模式封装后尝试入控制队列，忽略入队结果；不验证 payload schema，不等待网络写出。
fn send_control(session: &Arc<Session>, typ: u8, payload: &[u8]) {
    let _ = session.send_control(encode_frame(typ, wire_mode::VARINT, payload));
}

/// 业务作用：connect/disconnect hook **恒用会话绑定的 endpoint 快照**
/// 绝不在触发时按 path 重新解析当前 endpoint——replace 后旧会话的 on_disconnect 仍回到
/// 旧 endpoint(hook 成对),新 endpoint 不会被"从未 on_connect 过的会话"调 on_disconnect。
///
/// 参数说明：
/// - `session`: 已由 begin_connect 取得建连回调责任且绑定端点快照的会话。
///
/// 返回：存在绑定回调时同步执行并隔离其 unwind，否则直接结束；本方法不执行去重或完成生命周期迁移。
fn fire_connect(session: &Arc<Session>) {
    if let Some(b) = session.bound_endpoint() {
        if let Some(f) = b.endpoint.on_connect() {
            let h = SessionHandle::new(session.clone());
            isolate("on_connect", || f(h));
        }
    }
}

/// 业务作用：触发断开连接回调；用于通知端点释放业务资源。
///
/// 参数说明：
/// - `session`: cleanup 或 finish_connect 已授予断连通知责任的会话，回调仍使用原绑定端点。
///
/// 返回：存在回调时同步调用并隔离 unwind，否则直接结束；本函数不再次注销索引，恰好一次的裁决由调用方生命周期状态保证。
fn fire_disconnect(session: &Arc<Session>) {
    if let Some(b) = session.bound_endpoint() {
        if let Some(f) = b.endpoint.on_disconnect() {
            let h = SessionHandle::new(session.clone());
            isolate("on_disconnect", || f(h));
        }
    }
}
