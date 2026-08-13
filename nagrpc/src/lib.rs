//! 实验性 tonic gRPC 传输配置与独立 listener 生命周期。
//!
//! 本 crate 只负责强制 HTTP/2 listener、边界配置、显式健康/反射 adapter 和有预算的 graceful drain。
//! 业务 proto/generated service 仍归业务 crate；独立和 Application 受管入口都保持显式实验开关。
//!
//! 独立模式由 `GrpcServerHandle` 独占 shutdown；与 `nasa` 的 `application` 组合时，业务在 UserHook
//! 提交 Router 工厂，Application 完成 initializer 后才绑定 listener，并独占 readiness、监督和排空。
//! 两种模式都先取得连接 permit 再 accept，先停止准入再等待在途 RPC，排空超时会终止 serve task，
//! 不遗留 detached listener。
//!
//! health/reflection、业务 service、proto 兼容、TLS 身份和方法级授权都需要业务显式装配；本 crate
//! 不是 service mesh、API gateway、客户端连接池或负载均衡器。

#![forbid(unsafe_code)]

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// generated service 单消息的框架硬上限；业务可按接口进一步收紧。
pub const MAX_GRPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
/// 单连接业务并发的框架硬上限，防止底层 semaphore 因异常 `usize` 配置 panic。
pub const MAX_GRPC_CONCURRENCY_PER_CONNECTION: usize = 65_535;
/// 进程级并发连接硬上限，限制 socket、HTTP/2 状态与 TLS 会话占用。
pub const MAX_GRPC_CONNECTIONS: usize = 1_000_000;
/// gRPC 请求、keepalive 与 drain 计时参数的统一硬上限。
pub const MAX_GRPC_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// accept 资源压力后的首次退避，避免持续错误时形成忙循环。
const ACCEPT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
/// accept 连续失败时的最大退避；listener 所有权仍保留，资源恢复后继续接流。
const ACCEPT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// 业务 generated service 必须应用的消息硬上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcMessageLimits {
    /// 最大解码消息字节数。
    pub max_decoding_bytes: usize,
    /// 最大编码消息字节数。
    pub max_encoding_bytes: usize,
}

impl Default for GrpcMessageLimits {
    /// 业务作用：使用 tonic 常见的 4 MiB 编解码上限作为保守默认值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：编码与解码上限均为 4 MiB 的消息边界。
    fn default() -> Self {
        Self {
            max_decoding_bytes: 4 * 1024 * 1024,
            max_encoding_bytes: 4 * 1024 * 1024,
        }
    }
}

/// 把已校验的消息上限应用到一个 tonic generated server。
///
/// tonic 的编解码发生在每个 generated service 内，而不是 transport `Server` 上，因此不能由
/// [`GrpcServerConfig::server_builder`] 偷偷设置。该宏保留表达式的具体 generated 类型，并强制同时
/// 调用其 `max_decoding_message_size` 与 `max_encoding_message_size` builder。
#[macro_export]
macro_rules! apply_message_limits {
    ($limits:expr, $service:expr) => {{
        let __limits = $limits;
        ($service)
            .max_decoding_message_size(__limits.max_decoding_bytes)
            .max_encoding_message_size(__limits.max_encoding_bytes)
    }};
}

/// gRPC transport 与停机边界。
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// listener 同时交给 tonic 的连接上限。
    pub max_connections: usize,
    /// 每连接并发 RPC 上限。
    pub concurrency_limit_per_connection: usize,
    /// 单 RPC server timeout。
    pub request_timeout: Duration,
    /// HTTP/2 keepalive ping 周期。
    pub keepalive_interval: Duration,
    /// HTTP/2 keepalive ack 超时。
    pub keepalive_timeout: Duration,
    /// 单连接最大并发 stream。
    pub max_concurrent_streams: u32,
    /// graceful drain 总预算。
    pub drain_timeout: Duration,
    /// generated service 消息上限。
    pub message_limits: GrpcMessageLimits,
}

impl Default for GrpcServerConfig {
    /// 业务作用：提供连接、并发、超时、keepalive、stream 与 drain 均有界的保守默认值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接通过校验且仅在显式 start 后产生副作用的 server 配置。
    fn default() -> Self {
        Self {
            max_connections: 1_024,
            concurrency_limit_per_connection: 256,
            request_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            max_concurrent_streams: 256,
            drain_timeout: Duration::from_secs(20),
            message_limits: GrpcMessageLimits::default(),
        }
    }
}

impl GrpcServerConfig {
    /// 业务作用：校验 transport、连接、消息与停机配置的所有受管上界。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部字段在非零硬上限内时成功，否则返回稳定配置错误。
    pub fn validate(&self) -> Result<(), GrpcServerError> {
        if self.max_connections == 0
            || self.max_connections > MAX_GRPC_CONNECTIONS
            || self.concurrency_limit_per_connection == 0
            || self.concurrency_limit_per_connection > MAX_GRPC_CONCURRENCY_PER_CONNECTION
            || self.request_timeout.is_zero()
            || self.keepalive_interval.is_zero()
            || self.keepalive_timeout.is_zero()
            || self.max_concurrent_streams == 0
            || self.drain_timeout.is_zero()
            || self.message_limits.max_decoding_bytes == 0
            || self.message_limits.max_encoding_bytes == 0
            || self.request_timeout > MAX_GRPC_DURATION
            || self.keepalive_interval > MAX_GRPC_DURATION
            || self.keepalive_timeout > MAX_GRPC_DURATION
            || self.drain_timeout > MAX_GRPC_DURATION
            || self.message_limits.max_decoding_bytes > MAX_GRPC_MESSAGE_BYTES
            || self.message_limits.max_encoding_bytes > MAX_GRPC_MESSAGE_BYTES
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        Ok(())
    }

    /// 业务作用：校验配置并生成只接受 HTTP/2 的 tonic Server builder。
    ///
    /// 每个 generated service 在 `add_service` 前还必须经过 [`apply_message_limits!`]；tonic 的消息
    /// codec 位于 generated service，transport builder 本身没有可设置该限制的 API。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置合法时返回带并发、stream、keepalive 与 handler 超时边界的 builder。
    pub fn server_builder(&self) -> Result<tonic::transport::Server, GrpcServerError> {
        self.validate()?;
        Ok(tonic::transport::Server::builder()
            .accept_http1(false)
            .load_shed(true)
            .concurrency_limit_per_connection(self.concurrency_limit_per_connection)
            .timeout(self.request_timeout)
            .http2_keepalive_interval(Some(self.keepalive_interval))
            .http2_keepalive_timeout(Some(self.keepalive_timeout))
            .max_concurrent_streams(Some(self.max_concurrent_streams)))
    }

    /// 业务作用：按同一份已校验配置绑定 listener，并把 Router 交给唯一受管 server owner。
    ///
    /// 参数说明：
    /// - `router`: 已由 `server_builder()` 构造并追加业务 service 的 Router。
    /// - `bind`: listener 绑定地址。
    ///
    /// 返回：端口与连接预算均取得所有权时返回 handle；配置或绑定失败时不遗留 serve task。
    pub async fn start(
        &self,
        router: tonic::transport::server::Router,
        bind: SocketAddr,
    ) -> Result<GrpcServerHandle, GrpcServerError> {
        self.validate()?;
        GrpcServerHandle::start_with_limit(router, bind, self.max_connections, self.drain_timeout)
            .await
    }
}

/// listener 运行状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcServerState {
    /// listener 已绑定且 serve 任务仍持有接流所有权；持续 accept 失败需结合观察句柄判断。
    Running,
    /// 已停止准入，正在排空。
    Draining,
    /// 正常关闭。
    Closed,
    /// serve future 异常退出。
    Failed,
}

impl GrpcServerState {
    /// 业务作用：将公开状态编码为原子存储使用的紧凑整数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与公开状态一一对应的内部整数。
    fn encode(self) -> u8 {
        match self {
            Self::Running => 1,
            Self::Draining => 2,
            Self::Closed => 3,
            Self::Failed => 4,
        }
    }

    /// 业务作用：从原子值恢复状态；未知值按失败处理，避免误报可用。
    ///
    /// 参数说明：
    /// - `value`: 原子状态槽读取的内部整数。
    ///
    /// 返回：已知映射对应的状态；未知值返回 `Failed`。
    fn decode(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Draining,
            3 => Self::Closed,
            _ => Self::Failed,
        }
    }
}

/// gRPC listener/serve/drain 错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcServerError {
    /// 配置含零值或无界值。
    InvalidConfiguration,
    /// listener bind 失败。
    BindFailed,
    /// tonic serve 异常退出。
    ServeFailed,
    /// 排空超过预算，任务已强制 abort。
    DrainTimeout,
    /// shutdown 已经被另一个 owner 消费。
    AlreadyClosed,
}

impl fmt::Display for GrpcServerError {
    /// 业务作用：输出不包含监听地址或业务消息的稳定错误分类。
    ///
    /// 参数说明：
    /// - `formatter`: 接收稳定分类文本的格式化缓冲区。
    ///
    /// 返回：文本写入成功时完成，底层格式化失败时透传错误。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "gRPC server error: {self:?}")
    }
}

impl std::error::Error for GrpcServerError {}

/// server handle 共享的地址、状态、取消令牌与唯一 join slot。
struct Inner {
    local_addr: SocketAddr,
    state: Arc<AtomicU8>,
    metrics: Arc<StdMutex<GrpcServerMetrics>>,
    shutdown: CancellationToken,
    join: Mutex<Option<JoinHandle<Result<(), GrpcServerError>>>>,
    drain_timeout: Duration,
}

/// listener 观测量的互斥状态；连接受理、释放与失败段更新在同一临界区提交。
#[derive(Debug, Default)]
struct GrpcServerMetrics {
    active_connections: usize,
    accepted_total: u64,
    accept_failures_total: u64,
    consecutive_accept_failures: u64,
    accept_failure_since: Option<Instant>,
}

/// 业务作用：在观测状态临界区内发布 listener 生命周期状态，使状态与连接快照保持同一顺序边界。
///
/// 参数说明：
/// - `state`: serve 与停机路径共享的原子状态槽。
/// - `metrics`: 与快照读取共用的观测状态门。
/// - `next`: 即将发布的生命周期状态。
///
/// 返回：无；状态在锁内以 Release 顺序发布，随后抓取不会混合迁移前后的业务事实。
fn publish_state(state: &AtomicU8, metrics: &StdMutex<GrpcServerMetrics>, next: GrpcServerState) {
    let _snapshot_gate = metrics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.store(next.encode(), Ordering::Release);
}

/// listener 在某一时刻的完整受理与接流事实。
///
/// 逐个 getter 分别读取会得到互相矛盾的组合（例如已计入受理但在途尚未自增）；
/// 导出面必须用一次 `snapshot()` 取同一时刻的整组值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcServerSnapshot {
    /// 当前生命周期状态。
    pub state: GrpcServerState,
    /// 已交给 tonic 且尚未关闭的连接数。
    pub active_connections: usize,
    /// 自启动以来进入 listener 所有权的连接总数。
    pub accepted_total: u64,
    /// 自启动以来 `accept` 返回错误的累计次数。
    pub accept_failures_total: u64,
    /// 最近一次成功 accept 之后的连续失败次数；成功即归零。
    pub consecutive_accept_failures: u64,
    /// 当前连续失败已持续的毫秒数；没有进行中的失败段时为 0。
    pub accept_stall_millis: u64,
}

/// 不含停机权的 gRPC listener 观察句柄。
#[derive(Clone)]
pub struct GrpcServerObserver {
    local_addr: SocketAddr,
    state: Arc<AtomicU8>,
    metrics: Arc<StdMutex<GrpcServerMetrics>>,
}

impl GrpcServerObserver {
    /// 业务作用：返回 listener 实际取得的本地地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预绑定成功后固定不变的 socket 地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 业务作用：读取 serve 与排空所有者发布的当前生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Running、Draining、Closed 或 Failed 的原子快照。
    pub fn state(&self) -> GrpcServerState {
        GrpcServerState::decode(self.state.load(Ordering::Acquire))
    }

    /// 业务作用：一次取得受理、在途与失败的同时刻快照，供导出面生成互相自洽的指标样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：同一次读取得到的状态、在途连接、受理总数与失败计数；失败时长按单调时钟换算为毫秒。
    pub fn snapshot(&self) -> GrpcServerSnapshot {
        // 连接受理会同时改变累计与在途两个量，失败恢复会同时改变连续次数与起点；
        // 必须在同一临界区读取，避免导出业务上不可能成立的组合。
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        GrpcServerSnapshot {
            state: self.state(),
            active_connections: metrics.active_connections,
            accepted_total: metrics.accepted_total,
            accept_failures_total: metrics.accept_failures_total,
            consecutive_accept_failures: metrics.consecutive_accept_failures,
            accept_stall_millis: u64::try_from(
                metrics
                    .accept_failure_since
                    .as_ref()
                    .map(Instant::elapsed)
                    .map(|elapsed| elapsed.as_millis())
                    .unwrap_or(0),
            )
            .unwrap_or(u64::MAX),
        }
    }

    /// 业务作用：读取进入 listener 所有权的连接总数，用于区分"没有流量"与"接不进来"。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：单调累计的受理连接数；socket 在受理前断开不计入。
    pub fn accepted_total(&self) -> u64 {
        self.metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepted_total
    }

    /// 业务作用：读取当前已交给 tonic 且尚未关闭的连接数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不超过配置 `max_connections` 的瞬时连接数。
    pub fn active_connections(&self) -> usize {
        self.metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_connections
    }

    /// 业务作用：读取 listener 生命周期内累计发生的 accept 失败次数，供趋势与告警观测。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：单调饱和计数；瞬时和持续失败都会计入，成功接流不会清零。
    pub fn accept_failures_total(&self) -> u64 {
        self.metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accept_failures_total
    }

    /// 业务作用：读取最近一次成功接流之后连续发生的 accept 失败次数，区分瞬时抖动与持续黑洞。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功 accept 后归零；连续失败期间单调饱和增长。
    pub fn consecutive_accept_failures(&self) -> u64 {
        self.metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .consecutive_accept_failures
    }

    /// 业务作用：读取最近一次成功接流之前，当前连续 accept 失败已经持续的单调时长。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未发生失败或失败后已经成功 accept 时返回 `None`；否则返回从首次连续失败到现在的时长。
    pub fn accept_failure_duration(&self) -> Option<Duration> {
        self.metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accept_failure_since
            .as_ref()
            .map(Instant::elapsed)
    }
}

/// 业务作用：对长期运行的观测计数执行饱和递增，避免极端持续故障发生整数回绕。
///
/// 参数说明：
/// - `counter`: 已在观测状态临界区内取得的累计值。
///
/// 返回：无；达到 `u64::MAX` 后保持不变。
fn increment_saturating(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}

/// 持有连接 permit 的 HTTP/2 I/O；只有连接真正关闭后才释放进程级容量。
struct LimitedConnection {
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    _permit: OwnedSemaphorePermit,
    metrics: Arc<StdMutex<GrpcServerMetrics>>,
}

impl LimitedConnection {
    /// 业务作用：把已接受 socket 与连接容量绑定成同生命周期的 tonic I/O。
    ///
    /// 参数说明：
    /// - `stream`: listener 已接受的 TCP 连接。
    /// - `permit`: 该连接独占的进程级容量凭证。
    /// - `metrics`: 观察面使用的受理与在途连接状态。
    ///
    /// 返回：直到 I/O 析构才释放凭证的连接包装。
    fn new(
        stream: tokio::net::TcpStream,
        permit: OwnedSemaphorePermit,
        metrics: Arc<StdMutex<GrpcServerMetrics>>,
    ) -> io::Result<Self> {
        // peer_addr 失败表示对端在受理前已断开，该 socket 从未进入 listener 所有权，
        // 因此受理计数只在构造成功后自增，与在途计数保持同一进出口径。
        let peer_addr = stream.peer_addr()?;
        {
            let mut snapshot = metrics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            increment_saturating(&mut snapshot.accepted_total);
            snapshot.active_connections = snapshot.active_connections.saturating_add(1);
        }
        Ok(Self {
            stream,
            peer_addr,
            _permit: permit,
            metrics,
        })
    }
}

impl AsyncRead for LimitedConnection {
    /// 业务作用：把 tonic 的读取轮询转发给持有容量凭证的底层 socket。
    ///
    /// 参数说明：
    /// - `cx`: 异步任务唤醒上下文。
    /// - `buf`: 接收网络字节的目标缓冲区。
    ///
    /// 返回：底层 socket 的读取进度或 I/O 错误。
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for LimitedConnection {
    /// 业务作用：把 tonic 的写入轮询转发给持有容量凭证的底层 socket。
    ///
    /// 参数说明：
    /// - `cx`: 异步任务唤醒上下文。
    /// - `buf`: 需要发送的网络字节。
    ///
    /// 返回：底层 socket 已接受的字节数或 I/O 错误。
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    /// 业务作用：把缓冲数据刷新到持有容量凭证的底层 socket。
    ///
    /// 参数说明：
    /// - `cx`: 异步任务唤醒上下文。
    ///
    /// 返回：底层 socket 的刷新结果。
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    /// 业务作用：关闭底层 socket 的写方向并结束 tonic 输出。
    ///
    /// 参数说明：
    /// - `cx`: 异步任务唤醒上下文。
    ///
    /// 返回：底层 socket 的关闭结果。
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }

    /// 业务作用：尝试向底层 socket 写入多个连续缓冲区。
    ///
    /// 参数说明：
    /// - `cx`: 异步任务唤醒上下文。
    /// - `bufs`: 需要按顺序发送的缓冲区集合。
    ///
    /// 返回：底层 socket 已接受的总字节数或 I/O 错误。
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    /// 业务作用：声明底层 socket 支持 vectored write，供 tonic 选择等价高效路径。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层 TCP I/O 的 vectored write 能力。
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

impl tonic::transport::server::Connected for LimitedConnection {
    type ConnectInfo = SocketAddr;

    /// 业务作用：向请求扩展发布已由 socket 确认的直连对端地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：建立该连接时固定的 peer socket 地址。
    fn connect_info(&self) -> Self::ConnectInfo {
        self.peer_addr
    }
}

impl Drop for LimitedConnection {
    /// 业务作用：在 tonic 释放连接时同步归还观察计数；permit 随后由字段析构归还。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；计数只减去本实例构造时登记的一次。
    fn drop(&mut self) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.active_connections = metrics.active_connections.saturating_sub(1);
    }
}

/// 唯一 shutdown owner 的 gRPC server handle。
pub struct GrpcServerHandle {
    inner: Arc<Inner>,
}

impl GrpcServerHandle {
    /// 业务作用：以兼容入口先绑定独立 TCP listener，再启动 tonic Router。
    ///
    /// `router` 应由 [`GrpcServerConfig::server_builder`] 创建，并由业务追加 health、reflection 与业务
    /// service。此入口使用框架连接硬上限；需要更小业务上限时使用 [`GrpcServerConfig::start`]。
    ///
    /// 参数说明：
    /// - `router`: 已完成 service 装配的 tonic Router。
    /// - `bind`: listener 绑定地址。
    /// - `drain_timeout`: owner 主动停机时的最大排空时长。
    ///
    /// 返回：预绑定和 serve task 建立成功时返回唯一 owner；配置或端口失败时不遗留任务。
    pub async fn start(
        router: tonic::transport::server::Router,
        bind: SocketAddr,
        drain_timeout: Duration,
    ) -> Result<Self, GrpcServerError> {
        Self::start_with_limit(router, bind, MAX_GRPC_CONNECTIONS, drain_timeout).await
    }

    /// 业务作用：预绑定独立 TCP listener，并以硬连接预算启动 tonic Router。
    ///
    /// 参数说明：
    /// - `router`: 已完成 service 装配的 tonic Router。
    /// - `bind`: listener 绑定地址。
    /// - `max_connections`: 同时交给 tonic 的连接数上限。
    /// - `drain_timeout`: owner 主动停机时的最大排空时长。
    ///
    /// 返回：绑定与 serve task 建立成功时返回唯一 owner；配置或端口失败时不产生后台所有权。
    pub async fn start_with_limit(
        router: tonic::transport::server::Router,
        bind: SocketAddr,
        max_connections: usize,
        drain_timeout: Duration,
    ) -> Result<Self, GrpcServerError> {
        if max_connections == 0
            || max_connections > MAX_GRPC_CONNECTIONS
            || drain_timeout.is_zero()
            || drain_timeout > MAX_GRPC_DURATION
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .map_err(|_| GrpcServerError::BindFailed)?;
        let local_addr = listener
            .local_addr()
            .map_err(|_| GrpcServerError::BindFailed)?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let state = Arc::new(AtomicU8::new(GrpcServerState::Running.encode()));
        let task_state = Arc::clone(&state);
        let metrics = Arc::new(StdMutex::new(GrpcServerMetrics::default()));
        let task_metrics = Arc::clone(&metrics);
        let serve_metrics = Arc::clone(&metrics);
        let connection_slots = Arc::new(Semaphore::new(max_connections));
        let incoming = async_stream::stream! {
            let mut accept_backoff = ACCEPT_RETRY_INITIAL_BACKOFF;
            loop {
                // 必须先取得 permit 再 accept；否则等待容量期间已经接受的 socket 会让实际
                // listener 所有权比配置上限多一条连接。
                let permit = match Arc::clone(&connection_slots).acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        yield Err::<LimitedConnection, io::Error>(io::Error::new(
                            io::ErrorKind::NotConnected,
                            "gRPC listener is draining",
                        ));
                        break;
                    }
                };
                let (stream, _) = match listener.accept().await {
                    Ok(accepted) => {
                        accept_backoff = ACCEPT_RETRY_INITIAL_BACKOFF;
                        let mut metrics = task_metrics
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        metrics.consecutive_accept_failures = 0;
                        metrics.accept_failure_since = None;
                        drop(metrics);
                        accepted
                    }
                    Err(error) => {
                        // `EMFILE`、`ENFILE` 与连接建立期异常不会使 listener 本身失权。释放本轮
                        // permit 后有界退避并继续持有 socket，避免瞬时资源压力被误判为干净关闭。
                        drop(permit);
                        {
                            let mut metrics = task_metrics
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            increment_saturating(&mut metrics.accept_failures_total);
                            increment_saturating(&mut metrics.consecutive_accept_failures);
                            if metrics.accept_failure_since.is_none() {
                                metrics.accept_failure_since = Some(Instant::now());
                            }
                        }
                        tracing::warn!(
                            error = %error,
                            retry_ms = accept_backoff.as_millis() as u64,
                            "gRPC listener accept temporarily unavailable; retrying"
                        );
                        tokio::time::sleep(accept_backoff).await;
                        accept_backoff = accept_backoff
                            .saturating_mul(2)
                            .min(ACCEPT_RETRY_MAX_BACKOFF);
                        continue;
                    }
                };
                yield LimitedConnection::new(
                    stream,
                    permit,
                    Arc::clone(&task_metrics),
                );
            }
        };
        let join = tokio::spawn(async move {
            let result = router
                .serve_with_incoming_shutdown(incoming, task_shutdown.cancelled_owned())
                .await
                .map_err(|_| GrpcServerError::ServeFailed);
            publish_state(
                &task_state,
                &serve_metrics,
                if result.is_ok() {
                    GrpcServerState::Closed
                } else {
                    GrpcServerState::Failed
                },
            );
            result
        });
        Ok(Self {
            inner: Arc::new(Inner {
                local_addr,
                state,
                metrics,
                shutdown,
                join: Mutex::new(Some(join)),
                drain_timeout,
            }),
        })
    }

    /// 业务作用：返回 listener 实际取得的绑定地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预绑定后固定不变的本地 socket 地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// 业务作用：读取 serve 与排空 owner 发布的当前生命周期状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Running、Draining、Closed 或 Failed 的原子快照。
    pub fn state(&self) -> GrpcServerState {
        GrpcServerState::decode(self.inner.state.load(Ordering::Acquire))
    }

    /// 业务作用：派生不含停机权的观察句柄，供 Application 健康监督和业务管理面读取。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：共享地址、状态与在途连接计数但不能取消 listener 的克隆句柄。
    pub fn observer(&self) -> GrpcServerObserver {
        GrpcServerObserver {
            local_addr: self.inner.local_addr,
            state: Arc::clone(&self.inner.state),
            metrics: Arc::clone(&self.inner.metrics),
        }
    }

    /// 业务作用：停止准入并在 owner 配置预算内等待在途 RPC 排空。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：serve task 正常结束时关闭；超时或 serve 失败时返回对应分类。
    pub async fn shutdown(&self) -> Result<(), GrpcServerError> {
        self.shutdown_with_timeout(self.inner.drain_timeout).await
    }

    /// 业务作用：停止准入，并按调用方预算与 owner 配置预算的较小值等待在途 RPC 排空。
    ///
    /// 参数说明：
    /// - `timeout`: 当前上层生命周期仍允许本 listener 消费的最长时长。
    ///
    /// 返回：serve task 正常结束时关闭；预算为零或排空超时时会立即终止任务并保留
    /// `DrainTimeout` 证据；超过框架硬上限的预算返回配置错误。
    pub async fn shutdown_with_timeout(&self, timeout: Duration) -> Result<(), GrpcServerError> {
        if timeout > MAX_GRPC_DURATION {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        let mut guard = self.inner.join.lock().await;
        let Some(join) = guard.as_mut() else {
            return Err(GrpcServerError::AlreadyClosed);
        };
        publish_state(
            &self.inner.state,
            &self.inner.metrics,
            GrpcServerState::Draining,
        );
        self.inner.shutdown.cancel();
        // JoinHandle 必须留在共享 slot 里直到 await 真正结束。若调用方的 shutdown future 被外层
        // deadline/cancellation 丢弃，MutexGuard 会释放但 slot 仍是 Some；后续 shutdown 可继续
        // drain，最终 handle 的 Drop 也仍能 abort。先 take 再 await 会把取消变成 detached listener。
        match tokio::time::timeout(timeout.min(self.inner.drain_timeout), join).await {
            Ok(Ok(Ok(()))) => {
                let _ = guard.take();
                publish_state(
                    &self.inner.state,
                    &self.inner.metrics,
                    GrpcServerState::Closed,
                );
                Ok(())
            }
            Ok(Ok(Err(error))) => {
                let _ = guard.take();
                publish_state(
                    &self.inner.state,
                    &self.inner.metrics,
                    GrpcServerState::Failed,
                );
                Err(error)
            }
            Ok(Err(_join_error)) => {
                let _ = guard.take();
                publish_state(
                    &self.inner.state,
                    &self.inner.metrics,
                    GrpcServerState::Failed,
                );
                Err(GrpcServerError::ServeFailed)
            }
            Err(_) => {
                let join = guard
                    .take()
                    .expect("gRPC join slot remains populated while shutdown holds its gate");
                join.abort();
                let _ = join.await;
                publish_state(
                    &self.inner.state,
                    &self.inner.metrics,
                    GrpcServerState::Failed,
                );
                Err(GrpcServerError::DrainTimeout)
            }
        }
    }
}

impl Drop for GrpcServerHandle {
    /// 业务作用：未显式 shutdown 时至少停止准入并终止 serve task，禁止遗留 detached listener。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；同步析构只执行兜底取消，正常排空必须走异步 shutdown。
    fn drop(&mut self) {
        // Drop 不能异步排空，但也不能把 detached listener 留在进程里。正常路径必须显式 shutdown；
        // 异常 owner drop 至少立即停止准入并 abort serve task。
        self.inner.shutdown.cancel();
        if let Ok(mut join) = self.inner.join.try_lock() {
            if let Some(join) = join.take() {
                join.abort();
            }
        }
        if self.state() != GrpcServerState::Closed {
            publish_state(
                &self.inner.state,
                &self.inner.metrics,
                GrpcServerState::Failed,
            );
        }
    }
}

/// tonic 类型门面，供业务 generated code 不必直接新增 tonic 版本 ownership。
pub mod tonic_api {
    pub use tonic::*;
}

/// 标准 gRPC health adapter。
pub mod health {
    pub use tonic_health::*;
}

/// 标准 gRPC server reflection adapter。
pub mod reflection {
    pub use tonic_reflection::*;
}
