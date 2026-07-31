//! 实验性 tonic gRPC 传输配置与独立 listener 生命周期。
//!
//! 本 crate 只负责强制 HTTP/2 listener、边界配置、显式健康/反射 adapter 和有预算的 graceful drain。
//! 业务 proto/generated service 仍归业务 crate，breaking check 归 CI。稳定组件字符串需真实业务验证后再定。

#![forbid(unsafe_code)]

use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

/// generated service 单消息的框架硬上限；业务可按接口进一步收紧。
pub const MAX_GRPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
/// 单连接业务并发的框架硬上限，防止底层 semaphore 因异常 `usize` 配置 panic。
pub const MAX_GRPC_CONCURRENCY_PER_CONNECTION: usize = 65_535;
/// gRPC 请求、keepalive 与 drain 计时参数的统一硬上限。
pub const MAX_GRPC_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 业务 generated service 必须应用的消息硬上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcMessageLimits {
    /// 最大解码消息字节数。
    pub max_decoding_bytes: usize,
    /// 最大编码消息字节数。
    pub max_encoding_bytes: usize,
}

impl Default for GrpcMessageLimits {
    /// 使用 tonic 常见的 4 MiB 编解码上限作为保守默认值。
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
    /// 提供有界并发、超时、keepalive、stream 与 drain 的生产保守默认值。
    fn default() -> Self {
        Self {
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
    /// 校验 transport 与消息配置并生成只接受 HTTP/2 的 tonic Server builder。
    ///
    /// 每个 generated service 在 `add_service` 前还必须经过 [`apply_message_limits!`]；tonic 的消息
    /// codec 位于 generated service，transport builder 本身没有可设置该限制的 API。
    pub fn server_builder(&self) -> Result<tonic::transport::Server, GrpcServerError> {
        if self.concurrency_limit_per_connection == 0
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
        Ok(tonic::transport::Server::builder()
            .accept_http1(false)
            .load_shed(true)
            .concurrency_limit_per_connection(self.concurrency_limit_per_connection)
            .timeout(self.request_timeout)
            .http2_keepalive_interval(Some(self.keepalive_interval))
            .http2_keepalive_timeout(Some(self.keepalive_timeout))
            .max_concurrent_streams(Some(self.max_concurrent_streams)))
    }
}

/// listener 运行状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcServerState {
    /// listener 已绑定并接受请求。
    Running,
    /// 已停止准入，正在排空。
    Draining,
    /// 正常关闭。
    Closed,
    /// serve future 异常退出。
    Failed,
}

impl GrpcServerState {
    /// 将公开状态编码为原子存储使用的紧凑整数。
    fn encode(self) -> u8 {
        match self {
            Self::Running => 1,
            Self::Draining => 2,
            Self::Closed => 3,
            Self::Failed => 4,
        }
    }

    /// 从原子值恢复状态；未知值按失败处理，避免误报可用。
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
    /// 输出不包含监听地址或业务消息的稳定错误分类。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "gRPC server error: {self:?}")
    }
}

impl std::error::Error for GrpcServerError {}

/// server handle 共享的地址、状态、取消令牌与唯一 join slot。
struct Inner {
    local_addr: SocketAddr,
    state: Arc<AtomicU8>,
    shutdown: CancellationToken,
    join: Mutex<Option<JoinHandle<Result<(), GrpcServerError>>>>,
    drain_timeout: Duration,
}

/// 唯一 shutdown owner 的 gRPC server handle。
pub struct GrpcServerHandle {
    inner: Arc<Inner>,
}

impl GrpcServerHandle {
    /// 先绑定独立 TCP listener，再启动 tonic Router。
    ///
    /// `router` 应由 [`GrpcServerConfig::server_builder`] 创建，并由业务追加 health、reflection 与业务
    /// service。预绑定保证本方法成功返回时端口 ownership 已确定。
    pub async fn start(
        router: tonic::transport::server::Router,
        bind: SocketAddr,
        drain_timeout: Duration,
    ) -> Result<Self, GrpcServerError> {
        if drain_timeout.is_zero() || drain_timeout > MAX_GRPC_DURATION {
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
        let incoming = TcpListenerStream::new(listener);
        let join = tokio::spawn(async move {
            let result = router
                .serve_with_incoming_shutdown(incoming, task_shutdown.cancelled_owned())
                .await
                .map_err(|_| GrpcServerError::ServeFailed);
            task_state.store(
                if result.is_ok() {
                    GrpcServerState::Closed.encode()
                } else {
                    GrpcServerState::Failed.encode()
                },
                Ordering::Release,
            );
            result
        });
        Ok(Self {
            inner: Arc::new(Inner {
                local_addr,
                state,
                shutdown,
                join: Mutex::new(Some(join)),
                drain_timeout,
            }),
        })
    }

    /// 实际绑定地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// 当前生命周期状态。
    pub fn state(&self) -> GrpcServerState {
        GrpcServerState::decode(self.inner.state.load(Ordering::Acquire))
    }

    /// 停止准入并在预算内等待在途 RPC 排空。
    pub async fn shutdown(&self) -> Result<(), GrpcServerError> {
        let mut guard = self.inner.join.lock().await;
        let Some(join) = guard.as_mut() else {
            return Err(GrpcServerError::AlreadyClosed);
        };
        self.inner
            .state
            .store(GrpcServerState::Draining.encode(), Ordering::Release);
        self.inner.shutdown.cancel();
        // JoinHandle 必须留在共享 slot 里直到 await 真正结束。若调用方的 shutdown future 被外层
        // deadline/cancellation 丢弃，MutexGuard 会释放但 slot 仍是 Some；后续 shutdown 可继续
        // drain，最终 handle 的 Drop 也仍能 abort。先 take 再 await 会把取消变成 detached listener。
        match tokio::time::timeout(self.inner.drain_timeout, join).await {
            Ok(Ok(Ok(()))) => {
                let _ = guard.take();
                self.inner
                    .state
                    .store(GrpcServerState::Closed.encode(), Ordering::Release);
                Ok(())
            }
            Ok(Ok(Err(error))) => {
                let _ = guard.take();
                self.inner
                    .state
                    .store(GrpcServerState::Failed.encode(), Ordering::Release);
                Err(error)
            }
            Ok(Err(_join_error)) => {
                let _ = guard.take();
                self.inner
                    .state
                    .store(GrpcServerState::Failed.encode(), Ordering::Release);
                Err(GrpcServerError::ServeFailed)
            }
            Err(_) => {
                let join = guard
                    .take()
                    .expect("gRPC join slot remains populated while shutdown holds its gate");
                join.abort();
                let _ = join.await;
                self.inner
                    .state
                    .store(GrpcServerState::Failed.encode(), Ordering::Release);
                Err(GrpcServerError::DrainTimeout)
            }
        }
    }
}

impl Drop for GrpcServerHandle {
    /// 未显式 shutdown 时至少停止准入并 abort serve task，禁止遗留 detached listener。
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
            self.inner
                .state
                .store(GrpcServerState::Failed.encode(), Ordering::Release);
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
