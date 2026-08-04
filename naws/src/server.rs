// ============================================================================
// src/server.rs —— 单机 TCP + WebSocket server。
// transport 无关的连接状态机(鉴权/心跳/分派)对 `Inbound` 编程;
// TCP 与 WS 只共享 NASA frame codec,各自的读/写实现分离。
// 架构说明。
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

/// 会话 ID:`s{seq}-{uuid v4 simple}`。后缀用 **UUID v4(getrandom/OS CSPRNG)**——明确承诺的
/// 随机源,不再依赖 std hasher 的未承诺安全属性;该 ID 作为 AUTH_RESP
/// sessionId / engine.io sid 下发,不可预测。序号前缀仅用于排障(日志按时间可排序)。
fn new_session_id() -> String {
    let seq = SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("s{seq}-{}", uuid::Uuid::new_v4().simple())
}

/// 活跃连接计数(graceful shutdown 等排空用)。
#[derive(Default)]
pub struct ConnTracker {
    count: AtomicUsize,
    notify: tokio::sync::Notify,
}

/// 连接生命期 RAII:进 +1,退(handle_conn 返回)-1 并唤醒等待者。
struct ConnGuard {
    tracker: Arc<ConnTracker>,
}
impl ConnGuard {
    /// 构造新实例；用于集中初始化内部字段和默认状态。
    ///
    /// # 参数
    /// - `tracker`: 连接计数或生命周期追踪器。
    fn new(tracker: Arc<ConnTracker>) -> Self {
        tracker.count.fetch_add(1, Ordering::Relaxed);
        ConnGuard { tracker }
    }
}
impl Drop for ConnGuard {
    /// 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        self.tracker.count.fetch_sub(1, Ordering::Relaxed);
        self.tracker.notify.notify_waiters();
    }
}

/// 未认证连接占位 RAII:连接进入鉴权窗口时 +1,鉴权成功或连接结束时释放。
struct UnauthGuard {
    tracker: Arc<ConnTracker>,
}

impl UnauthGuard {
    /// 尝试预留一个未认证连接名额;已达上限时返回 `None`。
    ///
    /// # 参数
    /// - `tracker`: 连接计数或生命周期追踪器。
    /// - `max`: 允许的最大值或区间上界。
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
                return Some(Self { tracker });
            }
        }
    }
}

impl Drop for UnauthGuard {
    /// 释放未认证连接名额。
    fn drop(&mut self) {
        self.tracker.count.fetch_sub(1, Ordering::AcqRel);
        self.tracker.notify.notify_waiters();
    }
}

/// 调用用户同步闭包并隔离 panic —— 防止用户回调 panic 让连接 task unwind、跳过清理。
fn isolate<F: FnOnce()>(what: &str, f: F) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err() {
        tracing::error!("user callback panicked, isolated: {what}");
    }
}

/// 连接清理 RAII:Drop 时(正常返回或 panic unwind)若已认证,exactly-once 注销 + on_disconnect。
/// 取代"函数尾部 unregister"——尾部在 panic 时不会执行,会永久泄漏 Session。
struct ConnCleanup<'a> {
    session: &'a Arc<Session>,
    done: bool,
}
impl<'a> ConnCleanup<'a> {
    /// 构造新实例；用于集中初始化内部字段和默认状态。
    ///
    /// # 参数
    /// - `session`: 当前连接会话或会话句柄。
    fn new(session: &'a Arc<Session>) -> Self {
        Self {
            session,
            done: false,
        }
    }
}
impl Drop for ConnCleanup<'_> {
    /// 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        // lifecycle 锁内:置 closed + 注销(与 join_group/leave_group 互斥,防幽灵索引)。
        // cleanup 返回"首位关闭者且已认证":endpoint mutation hook 可能已先行下线本会话,
        // exactly-once 的 on_disconnect 由此保证。
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
    /// 认证成功后允许多久未收到 PING 心跳。
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
    /// 粗粒度 DoS/过载背压(计数由 ConnGuard 维护,accept 读取略滞后,不追求精确)。`0` = 不限制。
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
    /// 返回默认配置；用于未显式设置时提供稳定基线。
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
    /// WS 外层 message 上限的**生效值**:显式配置或自动 `4 × (max_frame + 4)`
    /// (容多条最大帧聚合)。**饱和运算兜底**:字段公开,
    /// 调用方可绕过 builder 直接构造——极端 max_frame 在此绝不 panic/回绕,溢出语义
    /// 校验由 `build()` 用 checked 运算在启动期完成。
    pub fn ws_message_limit(&self) -> usize {
        self.max_ws_message_bytes
            .unwrap_or_else(|| self.max_frame.saturating_add(4).saturating_mul(4))
    }
}

/// 框架内部共享状态。**`pub(crate)`**:业务不应直达内部装配
/// 对外只通过 `RunningServer` 的受控访问器(registry/sender/endpoints/active_conns 等)。
pub(crate) struct Shared {
    pub config: ServerConfig,
    pub registry: Arc<SessionRegistry>,
    pub endpoints: Arc<EndpointRegistry>,
    pub sender: Arc<Sender>,
    pub authorize: AuthorizeProvider,
    pub policy: InboundPolicy,
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
    /// 全局取消信号:所有 accept/连接/writer/handler 循环都 select 它,cancel 即退出。
    pub cancel: CancellationToken,
    /// 所有服务端后台任务的跟踪器:graceful shutdown 时 close+wait 真正 join。
    pub tasks: TaskTracker,
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

/// 按 UTF-8 字符边界截断 reason(超限取前缀;控制帧体积可控的前提)。
///
/// # 参数
/// - `s`: 要解析的输入字符串。
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
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = match self {
            BuildError::NoAuthorize => "authorize provider required",
            BuildError::EmptyNodeId => "cluster node_id must not be empty",
            BuildError::IncarnationWithoutCluster => "cluster_incarnation set but cluster() not enabled",
            BuildError::ClusterRequiresIncarnation => {
                "cluster requires an injected Incarnation (production must not fall back to wall-clock)"
            }
            BuildError::DataPublisherWithoutCluster => {
                "cluster data publisher requires cluster notifier"
            }
            BuildError::ClusterReadyTimeoutZero => "cluster_ready_timeout must be greater than zero",
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
    config: ServerConfig,
    endpoints: Arc<EndpointRegistry>,
    authorize: Option<AuthorizeProvider>,
    policy: InboundPolicy,
    cluster_node: Option<String>,
    notifier: Option<Arc<dyn Notifier>>,
    /// 可选少拷贝 data publisher；presence 仍固定走 notifier。
    cluster_data_publisher: Option<Arc<dyn ClusterDataPublisher>>,
    /// 业务注入的、已校验的 incarnation fencing token(如 Redis INCR 分配);None 用墙钟默认。
    cluster_incarnation: Option<Incarnation>,
    bridge: Arc<dyn PayloadBridge>,
}

impl ServerBuilder {
    /// 设置 TCP 监听地址。
    ///
    /// # 参数
    /// - `addr`: TCP 服务监听地址,例如 `0.0.0.0:19091`。
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.config.addr = addr.into();
        self
    }

    /// 启用 WebSocket 接入(独立端口);不设则只跑 TCP。
    ///
    /// # 参数
    /// - `addr`: WebSocket 服务监听地址。
    pub fn ws_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.ws_addr = Some(addr.into());
        self
    }

    /// 设置鉴权超时时间；用于限制连接建立后的认证等待。
    ///
    /// # 参数
    /// - `d`: 从连接建立到 AUTH 完成允许等待的最长时间。
    pub fn auth_timeout(mut self, d: Duration) -> Self {
        self.config.auth_timeout = d;
        self
    }

    /// 设置心跳超时时间；用于及时关闭失活连接。
    ///
    /// # 参数
    /// - `d`: 会话未收到心跳或业务数据后允许存活的最长时间。
    pub fn heartbeat_timeout(mut self, d: Duration) -> Self {
        self.config.heartbeat_timeout = d;
        self
    }

    /// 设置**全局** async handler 并发上限;用于控制全服务的事件处理压力(过载保护)。
    ///
    /// # 参数
    /// - `n`: 全服务允许同时运行的异步 handler 数量上限。
    pub fn max_inflight_handlers(mut self, n: usize) -> Self {
        self.config.max_inflight_handlers = n;
        self
    }

    /// 设置**单连接** async handler 并发上限(与全局叠加);防单连接抢占全局池饿死其它连接。
    ///
    /// # 参数
    /// - `n`: 单个连接允许同时运行的异步 handler 数量上限;`0` = 不限制(仅受全局约束)。
    pub fn max_inflight_handlers_per_conn(mut self, n: usize) -> Self {
        self.config.max_inflight_handlers_per_conn = n;
        self
    }

    /// 设置**同时活跃连接总数上限**(accept 处背压);达到后新连接被立即关闭。
    ///
    /// # 参数
    /// - `n`: 允许同时活跃的连接总数;`0` = 不限制。
    pub fn max_connections(mut self, n: usize) -> Self {
        self.config.max_connections = n;
        self
    }

    /// 设置**未认证连接数上限**(accept 处精确预留);达到后新连接被立即关闭。
    ///
    /// # 参数
    /// - `n`: 允许同时处于握手/AUTH 阶段的连接数;`0` = 不限制。
    pub fn max_unauthenticated(mut self, n: usize) -> Self {
        self.config.max_unauthenticated = n;
        self
    }

    /// 出站背压策略(默认 DropNew;行情快照类可用 DropOldest)。
    ///
    /// # 参数
    /// - `p`: 业务帧队列满时的取舍策略。
    pub fn backpressure(mut self, p: BackpressurePolicy) -> Self {
        self.config.backpressure = p;
        self
    }

    /// 单条 NASA frame 内层 declared 上限(默认 16MiB)。WS 外层另由
    /// `max_ws_message_bytes` 约束(一条 message 可聚合多条 frame)。
    ///
    /// # 参数
    /// - `n`: 单条内部 frame payload 最大字节数,最小会兜底为 1。
    pub fn max_frame_bytes(mut self, n: usize) -> Self {
        self.config.max_frame = n.max(1);
        self
    }

    /// WS 单条 message 外层聚合上限(默认自动 `4 × (max_frame + 4)`);
    /// build 校验 ≥ max_frame + 4。
    ///
    /// # 参数
    /// - `n`: WebSocket 单条 message 允许聚合的最大字节数。
    pub fn max_ws_message_bytes(mut self, n: usize) -> Self {
        self.config.max_ws_message_bytes = Some(n);
        self
    }

    /// 业务出站队列条数上限(默认 256)。
    ///
    /// # 参数
    /// - `n`: 每个 session 业务出站队列允许缓存的帧数量。
    pub fn outbox_capacity(mut self, n: usize) -> Self {
        self.config.outbox_cap = n.max(1);
        self
    }

    /// 业务出站队列字节上限(默认 64MiB)。
    ///
    /// # 参数
    /// - `n`: 每个 session 业务出站队列允许缓存的总字节数。
    pub fn outbox_max_bytes(mut self, n: usize) -> Self {
        self.config.outbox_max_bytes = n.max(1);
        self
    }

    /// 集群降级模式:true=集群未就绪也继续 bind(打 warning);默认 false=FailFast(bind 失败)。
    ///
    /// # 参数
    /// - `on`: 是否允许集群 transport 未就绪时继续启动本地服务。
    pub fn cluster_degraded(mut self, on: bool) -> Self {
        self.config.cluster_degraded = on;
        self
    }

    /// 设置 cluster transport 的初始就绪总超时。
    ///
    /// Kafka 模式应覆盖 TopicContract、group join 与 assignment 的最坏启动时长。
    ///
    /// # 参数
    ///
    /// - `timeout`: 非零启动等待时长。
    pub fn cluster_ready_timeout(mut self, timeout: Duration) -> Self {
        self.config.cluster_ready_timeout = timeout;
        self
    }

    /// 设置鉴权回调；用于握手阶段验证连接身份。
    ///
    /// # 参数
    /// - `f`: 握手阶段执行的鉴权回调,入参为认证请求和连接上下文。
    pub fn authorize<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(naws_proto::AuthRequest, AuthContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = AuthResult> + Send + 'static,
    {
        self.authorize = Some(Arc::new(move |req, ctx| Box::pin(f(req, ctx))));
        self
    }

    /// 设置入站策略；用于控制客户端触发的自动 relay。
    ///
    /// # 参数
    /// - `p`: 入站路由授权策略,默认 deny all。
    pub fn policy(mut self, p: InboundPolicy) -> Self {
        self.policy = p;
        self
    }

    /// 注入 socket.io payload 桥接(默认 JSON 透传)。
    ///
    /// # 参数
    /// - `b`: payload bridge,用于 socket.io JSON 参数与内部 payload 字节互转。
    pub fn payload_bridge(mut self, b: Arc<dyn PayloadBridge>) -> Self {
        self.bridge = b;
        self
    }

    /// 注册 endpoint。
    ///
    /// # 参数
    /// - `ep`: 要注册到服务端路由表的 endpoint 快照。
    pub fn endpoint(self, ep: Endpoint) -> Self {
        self.endpoints.register(ep).expect("endpoint register");
        self
    }

    /// 启用集群:本节点 ID + 跨节点 transport。
    ///
    /// # 参数
    /// - `node_id`: 本节点稳定 ID,用于防回环、presence 和 fencing。
    /// - `notifier`: 跨节点广播 transport。
    pub fn cluster(mut self, node_id: impl Into<String>, notifier: Arc<dyn Notifier>) -> Self {
        self.cluster_node = Some(node_id.into());
        self.notifier = Some(notifier);
        self
    }

    /// 注入独立 data plane publisher；control/presence 仍由 `cluster` 的 notifier 承载。
    ///
    /// # 参数
    ///
    /// - `publisher`: 绑定固定 data topic 与 producer lane 的发布器。
    pub fn cluster_data_publisher(mut self, publisher: Arc<dyn ClusterDataPublisher>) -> Self {
        self.cluster_data_publisher = Some(publisher);
        self
    }

    /// 注入已校验的集群 incarnation fencing token(业务用 `Incarnation::from_epoch(Redis INCR)` 构造)。
    /// 不设则用墙钟默认(单机可用,生产必须注入)。
    ///
    /// # 参数
    /// - `incarnation`: 当前进程 boot id,同 node_id 重启时用于拒绝旧实例迟到消息。
    pub fn cluster_incarnation(mut self, incarnation: Incarnation) -> Self {
        self.cluster_incarnation = Some(incarnation);
        self
    }

    /// 装配服务端。**配置校验**:缺鉴权 / 空 node_id / incarnation 与 cluster
    /// 不匹配 / 集群缺 incarnation → 返回 `Err`,不静默用错误默认。
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
        // 集群配置一致性校验。
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
                    // 失败分支的错误值是 `Arc<dyn ClusterDataPublisher>`(非 Debug),不能交给 expect
                    // 格式化;这里保持"重复安装即缺陷"的原意,只改成不依赖 Debug 的断言形式。
                    if c.set_data_publisher(publisher).is_err() {
                        unreachable!("新构造的 Cluster 不会重复安装 data publisher");
                    }
                }
                // 全量 presence 取本节点 (版本, 本地群) 一致快照(seqlock;精确群播目录来源)。
                let reg = registry.clone();
                c.set_presence_provider(Arc::new(move || reg.presence_snapshot()));
                // 群 0↔1 跃迁 → 即时单群增量 presence(避免"刚入群跨节点收不到"的窗口)。Weak 避免环。
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
    shared: Arc<Shared>,
}

/// 保存已绑定服务的运行句柄；用于关闭监听器和后台任务。
pub struct RunningServer {
    /// 实际绑定的 TCP 地址;传入 `127.0.0.1:0` 时可从这里取随机端口。
    pub local_addr: SocketAddr,
    /// 实际绑定的 WebSocket 地址;未启用 WS 时为 None。
    pub ws_local_addr: Option<SocketAddr>,
    pub(crate) shared: Arc<Shared>,
}

/// 直接 drop handle(不显式 shutdown)也必须停 accept/连接/集群任务、释放 listener 端口
///。cancel 令所有循环 select 退出;cluster.shutdown 停集群任务。
impl Drop for RunningServer {
    /// 释放关联资源；用于对象离开作用域时执行兜底清理。
    fn drop(&mut self) {
        if let Some(c) = &self.shared.cluster {
            c.shutdown();
        }
        self.shared.cancel.cancel();
    }
}

impl RunningServer {
    /// 集群规模只读快照;单机(未启用集群)返回 `None`。`tombstone_count` 的无界增长说明
    /// `node_id` 不稳定,部署应据此告警(标准 ServerBuilder 集成路径现可监控该指标)。
    pub fn cluster_stats(&self) -> Option<crate::cluster::ClusterStats> {
        self.shared.cluster.as_ref().map(|c| c.stats())
    }

    /// 立即触发关闭(各连接收到 CLOSE(SHUTDOWN);停 accept + 集群后台任务);不等待 join。
    pub fn shutdown(&self) {
        if let Some(c) = &self.shared.cluster {
            c.shutdown(); // 停 presence + notifier(Redis)任务,否则永久存活
        }
        self.shared.cancel.cancel();
    }

    /// 优雅关闭:cancel 所有任务 → 在 deadline 内**真正 join**(TaskTracker.wait),
    /// 不再靠固定 sleep 猜 flush。返回 `true`=已全部退出;`false`=到 deadline 仍有未退
    /// (剩余任务已被 cancel,会尽快退出)——不静默吞掉超时。
    ///
    /// # 参数
    /// - `deadline`: 本次关闭的总时间预算,server 与 cluster 后台任务共同使用。
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
        self.shared.tasks.close(); // 不再接受新任务;已 spawn 的继续被 join
                                   // cancel 后:连接循环发 CLOSE+break → cleanup 关 outbox → writer 排空退出;accept 退出。
        let mut ok = tokio::time::timeout_at(end, self.shared.tasks.wait())
            .await
            .is_ok();
        // 集群后台任务(presence + Redis reader/publisher)共用同一截止点 join。
        if let Some(c) = &self.shared.cluster {
            ok &= c.shutdown_join(end).await;
        }
        ok
    }

    /// 当前活跃连接数。
    pub fn active_conns(&self) -> usize {
        self.shared.active.count.load(Ordering::Relaxed)
    }

    /// 当前处于握手/AUTH 阶段的未认证连接数。
    pub fn unauthenticated_conns(&self) -> usize {
        self.shared.unauthenticated.count.load(Ordering::Relaxed)
    }

    /// 返回会话注册表；用于查询和管理在线连接。
    pub fn registry(&self) -> &Arc<SessionRegistry> {
        &self.shared.registry
    }

    /// 返回消息发送器；用于向会话、群组或全体连接发送消息。
    pub fn sender(&self) -> &Arc<Sender> {
        &self.shared.sender
    }

    /// 返回端点注册表；用于动态查看或更新事件入口。
    pub fn endpoints(&self) -> &Arc<EndpointRegistry> {
        &self.shared.endpoints
    }
}

impl Server {
    /// 构建 builder 结果；用于把配置和上下文组装成可执行对象。
    pub fn builder() -> ServerBuilder {
        ServerBuilder {
            config: ServerConfig::default(),
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

    /// 取 Sender（**bind 前**即可），消除“listener 已 accept 但 Sender 尚未注入”的丢消息窗口。
    /// 业务可在 `build()` 后、`bind()` 前注入到自己的 handler 上下文。
    pub fn sender(&self) -> &Arc<Sender> {
        &self.shared.sender
    }

    /// 返回只暴露 incarnation 判定能力的 Kafka 来源围栏。
    ///
    /// Server build 后、bind 前可把该句柄注入 WsKafkaRuntime；不会暴露节点目录写接口。
    #[cfg(feature = "kafka")]
    pub fn kafka_source_fence(&self) -> Option<Arc<dyn crate::kafka::SourceFence>> {
        self.shared.cluster.as_ref().map(|cluster| {
            let registry: Arc<crate::cluster::NodeRegistry> = Arc::clone(cluster.nodes());
            registry as Arc<dyn crate::kafka::SourceFence>
        })
    }

    /// 绑定监听地址并启动服务；用于接受 TCP 或 WebSocket 连接。
    pub async fn bind(self) -> std::io::Result<RunningServer> {
        // **先**把所有 listener bind 成功(可失败步骤前置),再启动 cluster / spawn accept——
        // 否则 bind 失败时已 start 的 cluster 不回滚、已 spawn 的 accept 泄漏。
        let tcp = listen_reuseaddr(&self.shared.config.addr)?;
        let local_addr = tcp.local_addr()?;
        let ws_listener = match &self.shared.config.ws_addr {
            Some(ws) => Some(listen_reuseaddr(ws)?),
            None => None,
        };
        let ws_local_addr = match &ws_listener {
            Some(l) => Some(l.local_addr()?),
            None => None,
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
        self.shared
            .tasks
            .spawn(accept_loop(tcp, shared.clone(), false));
        tracing::info!("rust-ws TCP server bound on {local_addr}");
        if let Some(ws_listener) = ws_listener {
            self.shared
                .tasks
                .spawn(accept_loop(ws_listener, shared.clone(), true));
            tracing::info!("rust-ws WS server bound on {}", ws_local_addr.unwrap());
        }

        Ok(RunningServer {
            local_addr,
            ws_local_addr,
            shared: self.shared,
        })
    }
}

/// socket.io / engine.io v4 WebSocket 连接的**精确**判定:按 query 参数
/// 逐对解析,要求 `EIO=4` **且** `transport=websocket`。不再用子串 `contains("EIO")`——
/// 那会把 `?NOT_EIO=1`、`?device_EIO=x` 等 NASA 连接误分流到 socket.io(首帧错发 engine.io
/// OPEN 文本,且跳过 NASA upgrade path 校验);`transport=polling` 也不属于本 WS-only 实现。
///
/// # 参数
/// - `query`: 查询对象或 query 参数集合。
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

/// 带 SO_REUSEADDR 的监听(便于重启快速重绑端口)。
///
/// # 参数
/// - `addr`: 监听地址或远端地址。
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

/// 持续接受新连接；用于把监听器上的连接交给会话处理流程。
///
/// # 参数
/// - `listener`: TCP 或 WS listener。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `is_ws`: 当前 accept loop 是否处理 WebSocket 连接。
async fn accept_loop(listener: TcpListener, shared: Arc<Shared>, is_ws: bool) {
    loop {
        tokio::select! {
            res = listener.accept() => match res {
                Ok((stream, _peer)) => {
                    // 连接总数上限(过载/DoS 背压):超过则立即关闭新连接,不 spawn 连接任务、不升级 WS。
                    // 计数由 ConnGuard 在 handle_* 里维护,accept 处读取略滞后(突发下可能小幅过量),
                    // 作为粗粒度封顶足够(目的是背压,不追求精确)。0 = 不限制。
                    let max_conns = shared.config.max_connections;
                    if max_conns != 0 && shared.active.count.load(Ordering::Relaxed) >= max_conns {
                        tracing::warn!(
                            "connection rejected: max_connections {} reached",
                            max_conns
                        );
                        drop(stream); // 关闭 TCP(FIN/RST),不占连接槽
                        continue;
                    }
                    let unauth_guard = match shared.config.max_unauthenticated {
                        0 => None,
                        max => match UnauthGuard::try_new(shared.unauthenticated.clone(), max) {
                            Some(guard) => Some(guard),
                            None => {
                                tracing::warn!(
                                    "connection rejected: max_unauthenticated {} reached",
                                    max
                                );
                                drop(stream);
                                continue;
                            }
                        },
                    };
                    let shared2 = shared.clone();
                    // 连接任务也进 TaskTracker → graceful shutdown 能 join。
                    if is_ws {
                        shared.tasks.spawn(handle_ws(stream, shared2, unauth_guard));
                    } else {
                        shared.tasks.spawn(handle_tcp(stream, shared2, unauth_guard));
                    }
                }
                Err(e) => tracing::warn!("accept error: {e}"),
            },
            _ = shared.cancel.cancelled() => break, // 取消 → 停 accept、释放 listener
        }
    }
}

/* ============================== transport 无关的 inbound ============================== */

/// 入站帧来源:TCP 直读 framed;WS 把 binary message 累积后用同一 FrameCodec 解。
enum Inbound {
    Tcp(FramedRead<OwnedReadHalf, FrameCodec>),
    Ws(WsInbound),
}

impl Inbound {
    /// 读取下一帧消息；用于统一 TCP 和 WebSocket 的入站循环。
    async fn next_frame(&mut self) -> Option<std::io::Result<Frame>> {
        match self {
            Inbound::Tcp(f) => f.next().await,
            Inbound::Ws(w) => w.next_frame().await,
        }
    }
}

/// WS 入站适配:累积 binary 字节 → 同一 FrameCodec 解;支持 1 帧/消息、多帧/消息、帧跨消息。
struct WsInbound {
    ws: WsSplit,
    codec: FrameCodec,
    buf: BytesMut,
}

impl WsInbound {
    /// 构造新实例；用于集中初始化内部字段和默认状态。
    ///
    /// # 参数
    /// - `ws`: WebSocket 连接对象。
    /// - `max_frame`: 单条 frame 允许的最大 declared 长度。
    fn new(ws: WsSplit, max_frame: usize) -> Self {
        Self {
            ws,
            codec: FrameCodec::new(max_frame),
            buf: BytesMut::new(),
        }
    }

    /// 读取下一帧消息；用于统一 TCP 和 WebSocket 的入站循环。
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
                    )))
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

/// 处理单个 TCP 连接；用于建立会话并进入消息分发循环。
///
/// # 参数
/// - `stream`: 已 accept 的 TCP socket,后续会拆成读写半边。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `unauth_guard`: 未认证连接名额的 RAII 占位守卫。
async fn handle_tcp(stream: TcpStream, shared: Arc<Shared>, unauth_guard: Option<UnauthGuard>) {
    let _guard = ConnGuard::new(shared.active.clone());
    // 连接级唯一鉴权 deadline:接入即起算(WS 路径同;单一窗口)。
    let auth_deadline = Instant::now() + shared.config.auth_timeout;
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

/// 运行 TCP 写出任务；用于把出站消息顺序写回客户端。
///
/// # 参数
/// - `wh`: TCP 写半边。
/// - `rx`: 后台任务接收消息的通道。
async fn tcp_writer_task(mut wh: OwnedWriteHalf, mut rx: OutboxReceiver) {
    while let Some(bytes) = rx.recv().await {
        if wh.write_all(&bytes).await.is_err() {
            break;
        }
    }
    let _ = wh.shutdown().await;
}

/* ============================== WebSocket 接入 ============================== */

// upgrade 回调的 Err 变体类型(http::Response)由 tungstenite 的 accept_hdr_async 签名固定,无法缩小。
///
/// # 参数
/// - `stream`: 已完成 TCP accept、等待 WebSocket upgrade 的 socket。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `unauth_guard`: 未认证连接名额的 RAII 占位守卫。
#[allow(clippy::result_large_err)]
async fn handle_ws(stream: TcpStream, shared: Arc<Shared>, unauth_guard: Option<UnauthGuard>) {
    let _guard = ConnGuard::new(shared.active.clone());
    // **连接级唯一绝对 deadline**:Upgrade 握手 + NASA AUTH / socket.io CONNECT + authorize
    // 共用同一截止点。原先"握手一份 timeout + 鉴权再一份"叠加成 2×auth_timeout,慢速连接
    // 可占用连接槽至两倍窗口。
    let auth_deadline = Instant::now() + shared.config.auth_timeout;
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
        _ = shared.cancel.cancelled() => return, // shutdown 期间未完成握手 → 放弃
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

/// 运行 WebSocket 写出任务；用于把出站消息转换为协议帧。
///
/// # 参数
/// - `sink`: WebSocket 写入 sink。
/// - `rx`: 后台任务接收消息的通道。
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

/// socket.io / engine.io v4 连接生命周期。与 NASA `serve` 平行:
/// engine.io OPEN → sio CONNECT(auth)→ EVENT/PING/PONG 循环。出站经 outbox 走 WS **Text** 帧。
/// payload 翻译由 `Shared.bridge`(默认 JSON 透传)负责。
///
/// # 参数
/// - `ws`: WebSocket 连接对象。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `path`: WebSocket 握手 URL path,在默认 namespace 下可作为 socket.io endpoint。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
/// - `unauth_guard`: 未认证连接名额的 RAII 占位守卫。
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
            () = closed.cancelled() => break, // 外部 cleanup(endpoint 下线/替换)→ 任务退出
            msg = ws_stream.next() => match msg {
                Some(Ok(WsMessage::Text(t))) => {
                    // 入站按 OPEN 广告的 maxPayload(= max_frame)**精确**检查
                    // tungstenite 外层是聚合粗上限(ws_message_limit),不替代协议广告值。
                    if t.len() > shared.config.max_frame {
                        tracing::debug!("sio inbound {}B exceeds advertised maxPayload {}, closing",
                            t.len(), shared.config.max_frame);
                        break;
                    }
                    if !handle_sio_text(t.as_str(), &path, &session, &shared, &mut authed, &mut last_seen, auth_deadline).await {
                        break;
                    }
                    if authed {
                        drop(unauth_guard.take());
                    }
                }
                Some(Ok(WsMessage::Close(_))) => break,
                // BINARY_EVENT 暂不支持;WS Ping/Pong/Frame 由 tungstenite 自动处理。
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

    drop(cleanup); // 显式:若已认证 → unregister + on_disconnect(exactly once)
}

/// 处理一条 socket.io 入站文本帧。返回 false 表示连接应关闭。
///
/// # 参数
/// - `text`: socket.io 文本包内容。
/// - `url_path`: WebSocket 握手 URL path,用于默认 namespace 的 endpoint 回退。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `authed`: 当前连接是否已经完成认证。
/// - `last_seen`: 最近一次心跳或连接活动时间。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
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
        SioInbound::Auth { token, namespace } => {
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
                a = tokio::time::timeout_at(auth_deadline, (shared.authorize)(req, AuthContext::from_session(session))) => a,
                _ = shared.cancel.cancelled() => {
                    let _ = session.send_control(Bytes::from(sio::sio_connect_error("server shutting down", &namespace)));
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
                AuthResult::Ok { uid } if !uid.trim().is_empty() => {
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
                    // 注册后复核代际(authorize await 期间被 replace → 不得进入认证态)。
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
                AuthResult::Ok { .. } => {
                    let _ = session.send_control(Bytes::from(sio::sio_connect_error(
                        "auth returned blank uid",
                        &namespace,
                    )));
                    false
                }
                AuthResult::Fail { reason } => {
                    // 业务 reason 不可控 → 截断,connect_error 控制帧恒在 max_frame 内。
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
            // socket.io EVENT 不带 NASA 路由字段(group/uids/routers),仅触发本端 handler;
            // 跨端广播由 handler 自行调 sender 完成(同 active-exch 的 socket.io 用法)。
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
            // 客户端 emit 带 ackId → 回执 ACK(本步为**收到确认**,空参数;
            // 返回业务数据的 ack 需扩展 handler API,留作后续)。
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

/// socket.io 出站:每个 outbox 帧(UTF-8 文本字节)→ 一个 WS Text 消息。
///
/// # 参数
/// - `sink`: WebSocket 写入 sink。
/// - `rx`: 后台任务接收消息的通道。
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

/// 创建服务端会话对象；用于绑定连接身份、端点和出站队列。
///
/// # 参数
/// - `transport`: 当前连接使用的传输类型。
/// - `protocol`: 当前连接使用的协议类型。
/// - `outbox`: 会话待发送消息队列。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
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

/// transport 无关:鉴权 → 分派循环。清理交给 ConnCleanup(Drop 时执行,panic 也保证跑)。
/// `auth_deadline` 为**连接级**绝对截止点：TCP 从接入时刻起算，WS 从 Upgrade 前起算，
/// 并与握手共用同一份预算。
///
/// # 参数
/// - `inbound`: 已建立连接的入站读半边抽象。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
/// - `unauth_guard`: 未认证连接名额的 RAII 占位守卫。
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
    // _cleanup drop:若已认证 → unregister + on_disconnect(exactly once)
}

/// 鉴权状态机。外层 timeout 覆盖"等 AUTH 帧 + authorize 执行",用**连接级绝对
/// deadline**（WS 与 Upgrade 握手共用，总窗口恒为 auth_timeout）；
/// 同时可被 shutdown 取消——否则 authorize 慢/挂会让摘流延迟受 auth_timeout 控制。
///
/// # 参数
/// - `inbound`: 已建立连接的入站读半边抽象。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `auth_deadline`: 连接完成鉴权前的截止时间。
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
            AuthResult::Ok { uid } if !uid.trim().is_empty() => Ok(uid),
            AuthResult::Ok { .. } => Err("auth returned blank uid".to_string()),
            AuthResult::Fail { reason } => Err(reason),
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
    //   ③ **注册后复核 generation**:authorize await 期间 endpoint 可能被
    //      replace/unregister——此刻本会话已注册、可被 mutation hook 枚举,两边至少一边
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

/// 处理认证失败结果；用于发送错误控制帧并关闭会话。
///
/// # 参数
/// - `session`: 当前连接会话或会话句柄。
/// - `reason`: 鉴权失败原因,会写入 AUTH_RESP 和 CLOSE 控制帧。
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

/// 运行入站消息分发循环；用于持续读取帧并交给事件处理。
///
/// # 参数
/// - `inbound`: 已建立连接的入站读半边抽象。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
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
            _ = closed.cancelled() => break, // 外部 cleanup → reader/任务退出(CLOSE 已由清理方入队)
            _ = ticker.tick() => {
                if last_ping.elapsed() >= hb_timeout {
                    let cr = ProtoCloseReason { code: ProtoCloseReason::CODE_TIMEOUT,
                        reason: Some(format!("heartbeat timeout {}ms", hb_timeout.as_millis())) };
                    send_control(session, frame_type::CLOSE, &cr.encode(Mode::VarintTlv).unwrap());
                    break;
                }
            },
            _ = shared.cancel.cancelled() => {
                let cr = ProtoCloseReason { code: ProtoCloseReason::CODE_SERVER_SHUTDOWN,
                    reason: Some("server shutdown".into()) };
                send_control(session, frame_type::CLOSE, &cr.encode(Mode::VarintTlv).unwrap());
                break;
            }
        }
    }
}

/// 处理单个入站帧；用于区分控制帧、心跳和业务事件。
///
/// # 参数
/// - `frame`: 当前正在解码、编码或路由的网络帧。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
/// - `last_ping`: 最近一次收到 ping 或心跳的时间戳。
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

/// 处理业务事件帧；用于查找端点回调并决定连接是否继续。
///
/// # 参数
/// - `frame`: 当前正在解码、编码或路由的网络帧。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
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

/// 分发业务处理器任务；用于按并发策略执行事件回调。
///
/// # 参数
/// - `endpoint`: 当前消息所属的连接端点。
/// - `msg`: 业务消息体或事件载荷。
/// - `session`: 当前连接会话或会话句柄。
/// - `shared`: 运行时共享状态,包含连接、配置、指标或取消信号。
fn dispatch_handlers(
    endpoint: &Endpoint,
    msg: &Message,
    session: &Arc<Session>,
    shared: &Arc<Shared>,
) {
    let Some(events) = &msg.events else { return };
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
                                _ = fut => {}
                                _ = cancel.cancelled() => {} // shutdown:取消未完成 handler
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

/// 判断是否存在 routing；用于快速决定后续处理路径。
///
/// # 参数
/// - `msg`: 待检查的 NASA 业务消息。
fn has_routing(msg: &Message) -> bool {
    let group = msg.group.as_deref().map(|g| !g.is_empty()).unwrap_or(false);
    let nonempty = |v: &Option<Vec<Option<String>>>| {
        v.as_ref()
            .map(|a| a.iter().flatten().any(|s| !s.is_empty()))
            .unwrap_or(false)
    };
    group || nonempty(&msg.uids) || nonempty(&msg.routers)
}

/// 关闭 close protocol 流程；用于释放资源并停止后台任务。
///
/// # 参数
/// - `session`: 当前连接会话或会话句柄。
/// - `reason`: 协议错误原因,会写入 CLOSE 控制帧并按最大帧长截断。
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

/// 发送控制帧；用于返回鉴权、错误或协议级响应。
///
/// # 参数
/// - `session`: 当前连接会话或会话句柄。
/// - `typ`: NASA frame type,例如 AUTH_RESP、PONG 或 CLOSE。
/// - `payload`: 已按控制帧 schema 编码完成的 payload 字节。
fn send_control(session: &Arc<Session>, typ: u8, payload: &[u8]) {
    let _ = session.send_control(encode_frame(typ, wire_mode::VARINT, payload));
}

/// connect/disconnect hook **恒用会话绑定的 endpoint 快照**
/// 绝不在触发时按 path 重新解析当前 endpoint——replace 后旧会话的 on_disconnect 仍回到
/// 旧 endpoint(hook 成对),新 endpoint 不会被"从未 on_connect 过的会话"调 on_disconnect。
///
/// # 参数
/// - `session`: 当前连接会话或会话句柄。
fn fire_connect(session: &Arc<Session>) {
    if let Some(b) = session.bound_endpoint() {
        if let Some(f) = b.endpoint.on_connect() {
            let h = SessionHandle::new(session.clone());
            isolate("on_connect", || f(h));
        }
    }
}

/// 触发断开连接回调；用于通知端点释放业务资源。
///
/// # 参数
/// - `session`: 当前连接会话或会话句柄。
fn fire_disconnect(session: &Arc<Session>) {
    if let Some(b) = session.bound_endpoint() {
        if let Some(f) = b.endpoint.on_disconnect() {
            let h = SessionHandle::new(session.clone());
            isolate("on_disconnect", || f(h));
        }
    }
}
