//! tonic gRPC 的受管 service、运行时边界与独立 listener 生命周期。
//!
//! # 核心价值与运行架构
//!
//! generated server 通过 [`ManagedGrpcService`] 把消息上限、descriptor、方法形态和 codegen ABI
//! 交给唯一 registry。`ServerPlan` 自动装配 health、可选 reflection、HTTP/2 安全参数和有预算的 drain；
//! 业务不构造 Router，也不直接选择 tonic/prost 版本。Application 组合入口由 `napp` 持有同一 registry
//! 与 shutdown owner，独立入口只把最终 owner 交给调用方。
//!
//! listener 在 bind 前冻结 service/method 目录、TLS 与容量；运行期按连接、RPC、stream 和消息分别
//! 执行有界准入，并以固定原因和结局导出观测事实。停机先停止新准入，再发送 HTTP/2 GOAWAY 并在
//! `drain_timeout` 内等待已接纳调用。它不提供 service mesh、客户端负载均衡或业务授权模型。

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use prost::Message;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// generated code 与运行时共同遵守的 ABI 标识。
pub const CODEGEN_ABI: u32 = 2;
/// generated service 单消息的框架硬上限；业务可按接口进一步收紧。
pub const MAX_GRPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
/// 单连接业务并发的框架硬上限，防止底层 semaphore 因异常 `usize` 配置 panic。
pub const MAX_GRPC_CONCURRENCY_PER_CONNECTION: usize = 65_535;
/// 进程级并发连接硬上限，限制 socket、HTTP/2 状态与 TLS 会话占用。
pub const MAX_GRPC_CONNECTIONS: usize = 4_096;
/// gRPC 请求、keepalive 与 drain 计时参数的统一硬上限。
pub const MAX_GRPC_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);
/// 进程级在途 RPC 的框架硬上限。
pub const MAX_GRPC_INFLIGHT_RPCS: usize = 65_535;
/// 进程级消息字节 permit 的框架硬上限。
pub const MAX_GRPC_INFLIGHT_MESSAGE_BYTES: usize = 1024 * 1024 * 1024;
/// 受管内存预算的绝对上限。
pub const MAX_GRPC_MANAGED_MEMORY_BYTES: usize = 2 * 1024 * 1024 * 1024;
/// 单个进程可登记的业务 gRPC service 上限。
pub const MAX_GRPC_SERVICES: usize = 64;
/// 单个进程可登记的业务 gRPC method 总数上限。
pub const MAX_GRPC_METHODS: usize = 256;
/// 单个 generated descriptor set 的最大字节数。
pub const MAX_GRPC_DESCRIPTOR_BYTES: usize = 16 * 1024 * 1024;
/// HTTP/2 stream window 的框架硬上限。
pub const MAX_GRPC_STREAM_WINDOW_BYTES: u32 = 1024 * 1024;
/// HTTP/2 connection window 的框架硬上限。
pub const MAX_GRPC_CONNECTION_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
/// 默认 managed memory 预算。
pub const DEFAULT_GRPC_MANAGED_MEMORY_BYTES: usize = 768 * 1024 * 1024;
/// 配置允许占用 managed memory 预算的最大比例分子，剩余容量用于 allocator 与未纳入会计的协议状态。
const MEMORY_UTILIZATION_NUMERATOR: usize = 3;
/// 配置允许占用 managed memory 预算的最大比例分母。
const MEMORY_UTILIZATION_DENOMINATOR: usize = 4;
/// 每连接固定会计权重。
const CONNECTION_FIXED_BYTES: usize = 16 * 1024;
/// 每连接 reset 状态会计权重。
const RESET_STATE_BYTES: usize = 8 * 1024;
/// 每个 transport stream 状态会计权重。
const TRANSPORT_STREAM_STATE_BYTES: usize = 1024;
/// 每 RPC 固定会计权重。
const RPC_FIXED_BYTES: usize = 8 * 1024;
/// accept 资源压力后的首次退避，避免持续错误时形成忙循环。
const ACCEPT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
/// accept 连续失败时的最大退避；listener 所有权仍保留，资源恢复后继续接流。
const ACCEPT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);
/// 独立 listener 在未覆盖时要求证书至少还能安全完成一次发布替换。
const DEFAULT_TLS_MINIMUM_REMAINING: Duration = Duration::from_secs(24 * 60 * 60);
/// 证书生效时间可容忍的默认时钟偏差。
const DEFAULT_TLS_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
/// 防止调用方用过大时钟偏差使未生效证书过早开放。
const MAX_TLS_CLOCK_SKEW: Duration = Duration::from_secs(60 * 60);
/// 证书启动最低剩余期的配置上限。
const MAX_TLS_MINIMUM_REMAINING: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// tonic 自定义 incoming 路径不会应用 Server builder 的 TCP 选项，因此 accept 后必须显式投影。
#[derive(Debug, Clone, Copy)]
struct TcpSocketConfig {
    keepalive: Duration,
    keepalive_interval: Duration,
    keepalive_retries: u32,
    nodelay: bool,
}

/// 每条明文 HTTP/2 连接与 listener 共享的协议建立和控制帧门禁。
#[derive(Clone)]
struct ConnectionRuntimeConfig {
    tcp: TcpSocketConfig,
    handshake_timeout: Duration,
    max_frame_size: u32,
    control_frames_per_second: u32,
    control_frames_burst: u32,
    process_control_frames_per_second: u32,
    process_control_frames_burst: u32,
    process_control_bucket: Arc<StdMutex<TokenBucket>>,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
}

impl From<&GrpcServerConfig> for ConnectionRuntimeConfig {
    /// 业务作用：把已校验配置冻结为 accept 循环和全部连接共同使用的运行边界。
    ///
    /// 参数说明：
    /// - `config`: 已通过时间、frame、速率与受管内存校验的 server 配置。
    ///
    /// 返回：TCP 参数按连接复制，进程级控制帧 bucket 在全部连接之间共享。
    fn from(config: &GrpcServerConfig) -> Self {
        Self {
            tcp: TcpSocketConfig::from(config),
            handshake_timeout: config.connection_handshake_timeout,
            max_frame_size: config.max_frame_size,
            control_frames_per_second: config.control_frames_per_second,
            control_frames_burst: config.control_frames_burst,
            process_control_frames_per_second: config.process_control_frames_per_second,
            process_control_frames_burst: config.process_control_frames_burst,
            process_control_bucket: Arc::new(StdMutex::new(TokenBucket::full(
                config.process_control_frames_burst,
            ))),
            tls_acceptor: None,
        }
    }
}

/// 业务作用：把已解析 PEM identity 构造成只协商 HTTP/2、最低 TLS 1.2 的 rustls acceptor。
///
/// 参数说明：
/// - `identity`: 已从独立调用方或 Application secret 快照取得的启动期冻结材料。
///
/// 返回：certificate chain、private key 与可选 client CA 均合法时返回 acceptor；任一材料无法解析、
/// 密钥不匹配或 CA 无有效证书时返回 TLS 配置错误。
fn build_tls_acceptor(
    identity: &GrpcTlsIdentity,
) -> Result<tokio_rustls::TlsAcceptor, GrpcServerError> {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(
        identity.certificate_chain_pem.as_slice(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|_| GrpcServerError::TlsConfiguration)?;
    if certificates.is_empty() {
        return Err(GrpcServerError::TlsConfiguration);
    }
    validate_certificate_lifetime(
        &certificates,
        identity.minimum_remaining,
        identity.clock_skew,
    )?;
    let private_key =
        rustls_pemfile::private_key(&mut BufReader::new(identity.private_key_pem.as_slice()))
            .map_err(|_| GrpcServerError::TlsConfiguration)?
            .ok_or(GrpcServerError::TlsConfiguration)?;

    let builder = rustls::ServerConfig::builder();
    let mut server = if let Some(client_ca_pem) = &identity.client_ca_pem {
        let mut roots = rustls::RootCertStore::empty();
        let client_roots = rustls_pemfile::certs(&mut BufReader::new(client_ca_pem.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| GrpcServerError::TlsConfiguration)?;
        if client_roots.is_empty() {
            return Err(GrpcServerError::TlsConfiguration);
        }
        let (accepted, _) = roots.add_parsable_certificates(client_roots);
        if accepted == 0 {
            return Err(GrpcServerError::TlsConfiguration);
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|_| GrpcServerError::TlsConfiguration)?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .map_err(|_| GrpcServerError::TlsConfiguration)?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|_| GrpcServerError::TlsConfiguration)?
    };
    // gRPC transport 只允许 ALPN h2；客户端没有协商 HTTP/2 时握手不产生可用业务连接。
    server.alpn_protocols = vec![b"h2".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server)))
}

/// 业务作用：复验 server identity chain 的时间有效性与叶证书 serverAuth 用途。
///
/// 参数说明：
/// - `certificates`: rustls 已从 PEM 读取的有序证书链。
/// - `minimum_remaining`: Ready 时到最早到期时刻必须保留的最小窗口。
/// - `clock_skew`: 允许证书尚未生效的最大本机时钟偏差。
///
/// 返回：链可解析、叶证书允许 serverAuth 且时间窗口满足时返回最早的
/// Unix 到期时间；否则返回不含证书内容的 TLS 配置错误。
fn validate_certificate_lifetime(
    certificates: &[rustls::pki_types::CertificateDer<'_>],
    minimum_remaining: Duration,
    clock_skew: Duration,
) -> Result<u64, GrpcServerError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| GrpcServerError::TlsConfiguration)?
        .as_secs();
    let latest_allowed_not_before = now
        .checked_add(clock_skew.as_secs())
        .ok_or(GrpcServerError::TlsConfiguration)?;
    let minimum_not_after = now
        .checked_add(minimum_remaining.as_secs())
        .ok_or(GrpcServerError::TlsConfiguration)?;
    let mut earliest_expiry = u64::MAX;

    for (index, certificate) in certificates.iter().enumerate() {
        let (remaining, parsed) = x509_parser::parse_x509_certificate(certificate.as_ref())
            .map_err(|_| GrpcServerError::TlsConfiguration)?;
        if !remaining.is_empty() {
            return Err(GrpcServerError::TlsConfiguration);
        }
        let not_before = u64::try_from(parsed.validity().not_before.timestamp())
            .map_err(|_| GrpcServerError::TlsConfiguration)?;
        let not_after = u64::try_from(parsed.validity().not_after.timestamp())
            .map_err(|_| GrpcServerError::TlsConfiguration)?;
        if not_before > latest_allowed_not_before || not_after < minimum_not_after {
            return Err(GrpcServerError::TlsConfiguration);
        }
        if index == 0
            && parsed
                .extended_key_usage()
                .map_err(|_| GrpcServerError::TlsConfiguration)?
                .is_some_and(|usage| !usage.value.any && !usage.value.server_auth)
        {
            return Err(GrpcServerError::TlsConfiguration);
        }
        // identity chain 最后一张自签名证书是信任锚，它不参与实际服务身份到期会计。
        let trust_anchor = certificates.len() > 1
            && index + 1 == certificates.len()
            && parsed.subject() == parsed.issuer();
        if !trust_anchor {
            earliest_expiry = earliest_expiry.min(not_after);
        }
    }
    if earliest_expiry == u64::MAX {
        return Err(GrpcServerError::TlsConfiguration);
    }
    Ok(earliest_expiry)
}

/// 单调时钟驱动的 token bucket；只保存低基数连接协议会计，不记录对端身份。
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// 业务作用：创建初始可承受一次批准突发的通用 bucket。
    ///
    /// 参数说明：
    /// - `burst`: bucket 最多保留的 token 数。
    ///
    /// 返回：token 为 burst、补充时刻为当前单调时钟的 bucket。
    fn new(burst: u32) -> Self {
        Self::full(burst)
    }

    /// 业务作用：创建初始可承受一次批准突发的控制帧 bucket。
    ///
    /// 参数说明：
    /// - `burst`: bucket 最多保留的 token 数。
    ///
    /// 返回：token 为 burst、补充时刻为当前单调时钟的 bucket。
    fn full(burst: u32) -> Self {
        Self {
            tokens: f64::from(burst),
            last_refill: Instant::now(),
        }
    }

    /// 业务作用：按固定速率补充 token，并为一个控制帧执行无等待准入。
    ///
    /// 参数说明：
    /// - `rate`: 每秒补充的 token 数。
    /// - `burst`: token 容量上限。
    /// - `now`: 当前单调时刻，确保同一次连接读取使用一致时间。
    ///
    /// 返回：有 token 时消费一个并返回 true；容量耗尽时返回 false。
    fn try_take(&mut self, rate: u32, burst: u32, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed.as_secs_f64() * f64::from(rate)).min(f64::from(burst));
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

impl From<&GrpcServerConfig> for TcpSocketConfig {
    /// 业务作用：从已经校验的 server 配置提取每条已受理 socket 必须应用的参数。
    ///
    /// 参数说明：
    /// - `config`: 最终 gRPC server 配置。
    ///
    /// 返回：不含 listener 或业务状态的 TCP 参数副本。
    fn from(config: &GrpcServerConfig) -> Self {
        Self {
            keepalive: config.tcp_keepalive,
            keepalive_interval: config.tcp_keepalive_interval,
            keepalive_retries: config.tcp_keepalive_retries,
            nodelay: config.tcp_nodelay,
        }
    }
}

/// 业务 generated service 必须应用的消息硬上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcMessageLimits {
    /// 最大解码消息字节数。
    pub max_decoding_bytes: usize,
    /// 最大编码消息字节数。
    pub max_encoding_bytes: usize,
}

/// 独立 `ServerPlan` 使用的已解析 TLS 身份材料；Application 模式从 secret 快照构造同一类型。
pub struct GrpcTlsIdentity {
    certificate_chain_pem: Vec<u8>,
    private_key_pem: zeroize::Zeroizing<Vec<u8>>,
    client_ca_pem: Option<Vec<u8>>,
    minimum_remaining: Duration,
    clock_skew: Duration,
}

impl fmt::Debug for GrpcTlsIdentity {
    /// 业务作用：只输出 TLS 模式和材料长度，禁止证书正文或私钥进入调试链。
    ///
    /// 参数说明：
    /// - `formatter`: 接收脱敏元数据的格式化目标。
    ///
    /// 返回：成功写入长度和 mTLS 标志时完成，否则透传格式化错误。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcTlsIdentity")
            .field("certificate_chain_bytes", &self.certificate_chain_pem.len())
            .field("private_key_bytes", &self.private_key_pem.len())
            .field("mutual_tls", &self.client_ca_pem.is_some())
            .field("minimum_remaining", &self.minimum_remaining)
            .field("clock_skew", &self.clock_skew)
            .finish()
    }
}

impl GrpcTlsIdentity {
    /// 业务作用：接管 server certificate chain 与 private key，启用只认证服务端的 TLS。
    ///
    /// 参数说明：
    /// - `certificate_chain_pem`: 一个或多个 PEM certificate 组成的服务端链。
    /// - `private_key_pem`: 与叶证书匹配的 PKCS#1、PKCS#8 或 SEC1 PEM private key。
    ///
    /// 返回：材料非空时返回启动期再做密码学复验的身份；空材料立即返回配置错误。
    pub fn server(
        certificate_chain_pem: Vec<u8>,
        private_key_pem: Vec<u8>,
    ) -> Result<Self, GrpcServerError> {
        Self::new(certificate_chain_pem, private_key_pem, None)
    }

    /// 业务作用：接管 server identity 与 client CA roots，使 listener 强制验证客户端证书。
    ///
    /// 参数说明：
    /// - `certificate_chain_pem`: 服务端 certificate chain。
    /// - `private_key_pem`: 与服务端叶证书匹配的 private key。
    /// - `client_ca_pem`: 批准签发 client certificate 的 PEM CA 集合。
    ///
    /// 返回：三份材料非空时返回启动期再做证书/密钥校验的 mTLS 身份；否则返回配置错误。
    pub fn mutual(
        certificate_chain_pem: Vec<u8>,
        private_key_pem: Vec<u8>,
        client_ca_pem: Vec<u8>,
    ) -> Result<Self, GrpcServerError> {
        Self::new(certificate_chain_pem, private_key_pem, Some(client_ca_pem))
    }

    /// 业务作用：执行两种公开 TLS 构造入口共用的非空材料门禁，并把私钥交给清零容器。
    ///
    /// 参数说明：
    /// - `certificate_chain_pem`: 服务端证书链字节。
    /// - `private_key_pem`: 服务端私钥字节。
    /// - `client_ca_pem`: mTLS 模式下的 client CA 字节。
    ///
    /// 返回：全部必需材料非空时返回受控身份；缺失时返回配置错误。
    fn new(
        certificate_chain_pem: Vec<u8>,
        private_key_pem: Vec<u8>,
        client_ca_pem: Option<Vec<u8>>,
    ) -> Result<Self, GrpcServerError> {
        if certificate_chain_pem.is_empty()
            || private_key_pem.is_empty()
            || client_ca_pem.as_ref().is_some_and(Vec::is_empty)
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        Ok(Self {
            certificate_chain_pem,
            private_key_pem: zeroize::Zeroizing::new(private_key_pem),
            client_ca_pem,
            minimum_remaining: DEFAULT_TLS_MINIMUM_REMAINING,
            clock_skew: DEFAULT_TLS_CLOCK_SKEW,
        })
    }

    /// 业务作用：为 listener 设置启动最低剩余期与未生效时钟偏差门禁。
    ///
    /// 参数说明：
    /// - `minimum_remaining`: 证书在端口绑定时必须剩余的最小有效期。
    /// - `clock_skew`: 本机时钟可容忍的最大偏差。
    ///
    /// 返回：两个边界在硬上限内时返回更新后的身份；零值或越界时返回配置错误。
    pub fn certificate_lifetime_policy(
        mut self,
        minimum_remaining: Duration,
        clock_skew: Duration,
    ) -> Result<Self, GrpcServerError> {
        if minimum_remaining.is_zero()
            || minimum_remaining > MAX_TLS_MINIMUM_REMAINING
            || clock_skew > MAX_TLS_CLOCK_SKEW
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        self.minimum_remaining = minimum_remaining;
        self.clock_skew = clock_skew;
        Ok(self)
    }

    /// 业务作用：在 bind 前获取实际 server identity chain 的最早到期时间。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：证书链满足时间与 serverAuth 门禁时返回 Unix 秒；否则返回 TLS 配置错误。
    pub fn certificate_expiry_timestamp(&self) -> Result<u64, GrpcServerError> {
        let certificates =
            rustls_pemfile::certs(&mut BufReader::new(self.certificate_chain_pem.as_slice()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| GrpcServerError::TlsConfiguration)?;
        if certificates.is_empty() {
            return Err(GrpcServerError::TlsConfiguration);
        }
        validate_certificate_lifetime(&certificates, self.minimum_remaining, self.clock_skew)
    }
}

/// 已由 mTLS 链验证的对端身份；指纹来自实际 client leaf certificate。
#[derive(Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    principal: Arc<str>,
}

impl PeerIdentity {
    /// 业务作用：返回与验证通过的 client certificate 绑定的稳定 principal。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`sha256:<hex>` 形式的 leaf certificate 指纹；该值不来自客户端自报 metadata。
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// 业务作用：从 rustls 已验证的 client leaf certificate 派生不可伪造的对端身份。
    ///
    /// 参数说明：
    /// - `certificate`: TLS session 已验证链的第一张 client certificate。
    ///
    /// 返回：仅保留 SHA-256 指纹的 principal，不保留证书 DER 或 subject。
    fn from_certificate(certificate: &rustls::pki_types::CertificateDer<'_>) -> Self {
        use std::fmt::Write as _;

        let digest = Sha256::digest(certificate.as_ref());
        let mut principal = String::with_capacity(7 + digest.len() * 2);
        principal.push_str("sha256:");
        for byte in digest {
            let _ = write!(&mut principal, "{byte:02x}");
        }
        Self {
            principal: Arc::from(principal),
        }
    }
}

impl fmt::Debug for PeerIdentity {
    /// 业务作用：在业务调试中展示已验证 principal，不暴露原始证书或 subject。
    ///
    /// 参数说明：
    /// - `formatter`: 接收结构化 principal 的格式化目标。
    ///
    /// 返回：写入成功时完成，否则透传格式化错误。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerIdentity")
            .field("principal", &self.principal)
            .finish()
    }
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

/// RPC 的 protobuf streaming 形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcMethodType {
    /// 单请求、单响应。
    Unary,
    /// 客户端发送请求流，服务端返回单响应。
    ClientStreaming,
    /// 客户端发送单请求，服务端返回响应流。
    ServerStreaming,
    /// 请求与响应均为流。
    BidirectionalStreaming,
}

impl GrpcMethodType {
    /// 业务作用：返回指标与诊断使用的固定 RPC 形态名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`unary`、`client_streaming`、`server_streaming` 或 `bidirectional_streaming`。
    pub fn label(self) -> &'static str {
        match self {
            Self::Unary => "unary",
            Self::ClientStreaming => "client_streaming",
            Self::ServerStreaming => "server_streaming",
            Self::BidirectionalStreaming => "bidirectional_streaming",
        }
    }
}

/// 当前 RPC 有效截止点的来源；业务可以据此选择是否继续发起下游工作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineSource {
    /// 客户端 `grpc-timeout` 比服务端形态上限更早。
    Client,
    /// 服务端 unary 或 streaming 策略先到期，或客户端没有提供 deadline。
    Server,
}

/// 框架解析并放入每个受管请求 extensions 的单调截止点。
#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    effective: tokio::time::Instant,
    source: DeadlineSource,
}

impl Deadline {
    /// 业务作用：读取当前 RPC 距有效截止点的剩余单调时长，避免 handler 重复解析 metadata。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：截止点尚未到达时返回剩余预算；到达后返回零。
    pub fn remaining(&self) -> Duration {
        self.effective
            .saturating_duration_since(tokio::time::Instant::now())
    }

    /// 业务作用：说明有效截止点由客户端预算还是服务端安全策略决定。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：稳定的 Client 或 Server 分类。
    pub fn source(&self) -> DeadlineSource {
        self.source
    }
}

/// 业务作用：把入站受管 deadline 显式传播到一条因果相关的 generated client 请求。
///
/// 参数说明：
/// - `request`: 尚未发出的下游 tonic 请求。
/// - `deadline`: 从当前入站请求 extensions 取得的有效截止点。
///
/// 返回：扣除固定 1ms 转发余量后仍有预算时写入 `grpc-timeout`；预算耗尽时本地返回
/// `DeadlineExceeded`，不发起必然越界的下游调用。
pub fn propagate_deadline_from<T>(
    request: &mut tonic::Request<T>,
    deadline: &Deadline,
) -> Result<(), tonic::Status> {
    let remaining = deadline
        .remaining()
        .checked_sub(Duration::from_millis(1))
        .ok_or_else(|| tonic::Status::deadline_exceeded("gRPC deadline budget is exhausted"))?;
    if remaining.is_zero() {
        return Err(tonic::Status::deadline_exceeded(
            "gRPC deadline budget is exhausted",
        ));
    }
    request.set_timeout(remaining);
    Ok(())
}

/// generated code 提交给 registry 的静态方法身份。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcMethodDescriptor {
    /// `/package.Service/Method` 完整路径。
    pub full_name: &'static str,
    /// protobuf 声明的调用形态。
    pub rpc_type: GrpcMethodType,
}

/// 受管 RPC 的封闭完成结局；值域不包含业务输入、对端地址或动态 status 文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcRpcOutcome {
    /// 最终 gRPC code 为 OK，响应 body 与 trailers 已完整结束。
    Ok,
    /// handler 或 codec 返回固定 gRPC code。
    Code(tonic::Code),
    /// 客户端取消、响应 body 未被消费完或 handler future 被丢弃。
    Cancelled,
    /// 客户端提供的 deadline 先耗尽。
    ClientDeadlineExceeded,
    /// 服务端 unary 或 streaming 安全时间边界先耗尽。
    ServerTimeout,
    /// 服务端允许的 streaming 总持续时间先耗尽。
    ServerStreamDuration,
    /// streaming 在规定时间内没有完成任何业务消息。
    ServerStreamIdle,
    /// 接收方向累计完整消息数达到单流上限。
    StreamReceivedMessages,
    /// 发送方向累计完整消息数达到单流上限。
    StreamSentMessages,
    /// 接收方向累计 protobuf payload 字节数达到单流上限。
    StreamReceivedBytes,
    /// 发送方向累计 protobuf payload 字节数达到单流上限。
    StreamSentBytes,
    /// 已接纳 RPC 因连接或响应传输失败而未形成完整结果。
    TransportLost,
}

/// `GrpcRpcOutcome` 在指标中可能形成的稳定 label 总数；`Ok` 与 `Code(Ok)` 共用一个 label。
pub const GRPC_RPC_OUTCOME_CARDINALITY: usize = 27;

impl PartialOrd for GrpcRpcOutcome {
    /// 业务作用：按公开稳定 label 排序完成结局，使快照与导出顺序可重复。
    ///
    /// 参数说明：
    /// - `other`: 需要比较的另一封闭结局。
    ///
    /// 返回：始终返回与全序一致的比较结果。
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GrpcRpcOutcome {
    /// 业务作用：按公开稳定 label 为封闭结局建立全序，不依赖 tonic 内部枚举表示。
    ///
    /// 参数说明：
    /// - `other`: 需要比较的另一封闭结局。
    ///
    /// 返回：两个稳定 label 的字典序结果。
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.label().cmp(other.label())
    }
}

impl GrpcRpcOutcome {
    /// 业务作用：返回指标使用的封闭低基数结局值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功、固定 gRPC code 或框架取消/超时/传输分类；不包含 status message。
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Code(tonic::Code::Cancelled) => "code_cancelled",
            Self::Code(tonic::Code::Unknown) => "code_unknown",
            Self::Code(tonic::Code::InvalidArgument) => "code_invalid_argument",
            Self::Code(tonic::Code::DeadlineExceeded) => "code_deadline_exceeded",
            Self::Code(tonic::Code::NotFound) => "code_not_found",
            Self::Code(tonic::Code::AlreadyExists) => "code_already_exists",
            Self::Code(tonic::Code::PermissionDenied) => "code_permission_denied",
            Self::Code(tonic::Code::ResourceExhausted) => "code_resource_exhausted",
            Self::Code(tonic::Code::FailedPrecondition) => "code_failed_precondition",
            Self::Code(tonic::Code::Aborted) => "code_aborted",
            Self::Code(tonic::Code::OutOfRange) => "code_out_of_range",
            Self::Code(tonic::Code::Unimplemented) => "code_unimplemented",
            Self::Code(tonic::Code::Internal) => "code_internal",
            Self::Code(tonic::Code::Unavailable) => "code_unavailable",
            Self::Code(tonic::Code::DataLoss) => "code_data_loss",
            Self::Code(tonic::Code::Unauthenticated) => "code_unauthenticated",
            Self::Code(tonic::Code::Ok) => "ok",
            Self::Cancelled => "cancelled",
            Self::ClientDeadlineExceeded => "client_deadline_exceeded",
            Self::ServerTimeout => "server_timeout",
            Self::ServerStreamDuration => "server_stream_duration",
            Self::ServerStreamIdle => "server_stream_idle",
            Self::StreamReceivedMessages => "stream_received_messages",
            Self::StreamSentMessages => "stream_sent_messages",
            Self::StreamReceivedBytes => "stream_received_bytes",
            Self::StreamSentBytes => "stream_sent_bytes",
            Self::TransportLost => "transport_lost",
        }
    }
}

/// handler 前拒绝 RPC 的封闭原因；值域只描述框架门禁，不包含动态方法或对端数据。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GrpcRpcRejectionReason {
    /// 单条 HTTP/2 连接的并发 RPC permit 已耗尽。
    ConnectionConcurrency,
    /// listener 进程级 RPC permit 已耗尽。
    ProcessConcurrency,
    /// descriptor 方法的独立并发 permit 已耗尽。
    MethodConcurrency,
    /// descriptor 方法的 token bucket 已耗尽。
    MethodRate,
    /// 方法要求 mTLS peer identity，但当前连接没有验证身份。
    PeerIdentity,
}

/// handler 前框架拒绝原因的稳定 label 总数。
pub const GRPC_RPC_REJECTION_REASON_CARDINALITY: usize = 5;

impl GrpcRpcRejectionReason {
    /// 业务作用：返回指标使用的封闭低基数拒绝原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只可能是连接、进程、方法容量、方法速率或身份门禁的稳定 label。
    pub fn label(self) -> &'static str {
        match self {
            Self::ConnectionConcurrency => "connection_concurrency",
            Self::ProcessConcurrency => "process_concurrency",
            Self::MethodConcurrency => "method_concurrency",
            Self::MethodRate => "method_rate",
            Self::PeerIdentity => "peer_identity",
        }
    }
}

const GRPC_RPC_REJECTION_REASONS: [GrpcRpcRejectionReason; GRPC_RPC_REJECTION_REASON_CARDINALITY] = [
    GrpcRpcRejectionReason::ConnectionConcurrency,
    GrpcRpcRejectionReason::ProcessConcurrency,
    GrpcRpcRejectionReason::MethodConcurrency,
    GrpcRpcRejectionReason::MethodRate,
    GrpcRpcRejectionReason::PeerIdentity,
];

/// 单个 descriptor 固定方法的一次请求会计快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRpcMethodSnapshot {
    /// protobuf 完整 service name。
    pub service: &'static str,
    /// proto 中的 method 短名称。
    pub method: &'static str,
    /// descriptor 声明的 RPC 形态。
    pub rpc_type: GrpcMethodType,
    /// 已接纳但尚未形成最终结果的 RPC 数。
    pub active: u64,
    /// 自 listener 启动以来已接纳的 RPC 总数。
    pub started_total: u64,
    /// 因连接/进程容量、方法身份、方法并发或方法速率门禁在 handler 前拒绝的总数。
    pub rejected_total: u64,
    /// handler 前拒绝按封闭原因拆分的单调计数；各项之和等于 `rejected_total`。
    pub rejections: Vec<(GrpcRpcRejectionReason, u64)>,
    /// 已结束 RPC 按封闭结局聚合的单调计数。
    pub outcomes: Vec<(GrpcRpcOutcome, u64)>,
}

/// 业务作用：保存单个 descriptor 固定方法的在途、受理、拒绝与封闭结局累计事实。
#[derive(Debug, Default)]
struct RpcMethodMetrics {
    active: u64,
    started_total: u64,
    rejections: BTreeMap<GrpcRpcRejectionReason, u64>,
    outcomes: BTreeMap<GrpcRpcOutcome, u64>,
}

/// Prepare 封口的方法目录与运行期会计共用的固定键空间。
struct RpcMetrics {
    methods: BTreeMap<&'static str, (&'static str, &'static str, GrpcMethodType)>,
    values: StdMutex<BTreeMap<&'static str, RpcMethodMetrics>>,
}

impl RpcMetrics {
    /// 业务作用：从封口 descriptor 创建固定方法目录，运行期只更新既有 key 而不创建动态序列。
    ///
    /// 参数说明：
    /// - `methods`: 业务、health 与可选 reflection 的静态方法描述。
    ///
    /// 返回：每个合法完整方法名均有一条零值会计记录的共享状态。
    fn new(methods: impl IntoIterator<Item = GrpcMethodDescriptor>) -> Self {
        let mut directory = BTreeMap::new();
        let mut values = BTreeMap::new();
        for descriptor in methods {
            if let Some((service, method)) = split_method_name(descriptor.full_name) {
                directory.insert(descriptor.full_name, (service, method, descriptor.rpc_type));
                values.insert(
                    descriptor.full_name,
                    RpcMethodMetrics {
                        rejections: GRPC_RPC_REJECTION_REASONS
                            .into_iter()
                            .map(|reason| (reason, 0))
                            .collect(),
                        ..RpcMethodMetrics::default()
                    },
                );
            }
        }
        Self {
            methods: directory,
            values: StdMutex::new(values),
        }
    }

    /// 业务作用：记录 handler 前的连接/进程容量、方法身份、方法并发或速率拒绝，不增加 started 或 active。
    ///
    /// 参数说明：
    /// - `method`: descriptor 中已封口的完整方法路径。
    /// - `reason`: 触发拒绝的框架门禁。
    ///
    /// 返回：无；未知路径不创建动态记录。
    fn reject(&self, method: &'static str, reason: GrpcRpcRejectionReason) {
        let mut values = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(value) = values.get_mut(method) {
            let count = value.rejections.entry(reason).or_default();
            increment_saturating(count);
        }
    }

    /// 业务作用：按请求路径定位 sealed descriptor 方法并记录连接层拒绝，避免动态路径进入指标键空间。
    ///
    /// 参数说明：
    /// - `path`: hyper 已解析的请求 URI path。
    /// - `reason`: 连接 driver 已确定的封闭拒绝原因。
    ///
    /// 返回：路径属于已登记方法时记录一次并返回 `true`；未知路径不创建指标并返回 `false`。
    fn reject_path(&self, path: &str, reason: GrpcRpcRejectionReason) -> bool {
        let Some(method) = self.methods.keys().copied().find(|method| *method == path) else {
            return false;
        };
        self.reject(method, reason);
        true
    }

    /// 业务作用：接纳一条 RPC 并建立唯一完成守卫，使任何 future/body 丢弃路径都能归还会计。
    ///
    /// 参数说明：
    /// - `owner`: 共享会计所有权。
    /// - `method`: descriptor 中已封口的完整方法路径。
    /// - `permit`: 该 RPC 独占的进程级容量凭证。
    /// - `method_permit`: 当前方法显式配置并发上限时取得的容量凭证。
    ///
    /// 返回：负责 active 减一、结局加一和 permit 释放的唯一守卫。
    fn begin(
        owner: Arc<Self>,
        method: &'static str,
        permit: OwnedSemaphorePermit,
        method_permit: Option<OwnedSemaphorePermit>,
    ) -> RpcCompletionGuard {
        {
            let mut values = owner
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(value) = values.get_mut(method) {
                increment_saturating(&mut value.started_total);
                value.active = value.active.saturating_add(1);
            }
        }
        RpcCompletionGuard {
            owner,
            method,
            _permit: Some(permit),
            _method_permit: method_permit,
            state: Arc::new(RpcCompletionState {
                outcome: StdMutex::new(None),
                finalized: AtomicBool::new(false),
            }),
        }
    }

    /// 业务作用：一次读取全部固定方法的活动量、受理、拒绝和完成结局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按完整方法路径稳定排序的快照；尚无流量的方法仍保留零值基础会计。
    fn snapshot(&self) -> Vec<GrpcRpcMethodSnapshot> {
        let values = self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.methods
            .iter()
            .filter_map(|(full_name, (service, method, rpc_type))| {
                let value = values.get(full_name)?;
                Some(GrpcRpcMethodSnapshot {
                    service,
                    method,
                    rpc_type: *rpc_type,
                    active: value.active,
                    started_total: value.started_total,
                    rejected_total: value.rejections.values().copied().sum(),
                    rejections: value
                        .rejections
                        .iter()
                        .map(|(reason, count)| (*reason, *count))
                        .collect(),
                    outcomes: value
                        .outcomes
                        .iter()
                        .map(|(outcome, count)| (*outcome, *count))
                        .collect(),
                })
            })
            .collect()
    }
}

/// 业务作用：拆分由 codegen 生成的 `/package.Service/Method`，拒绝运行期未知标签。
///
/// 参数说明：
/// - `full_name`: descriptor 中的静态完整方法路径。
///
/// 返回：格式完整时返回静态 service 与 method 切片，否则返回 `None`。
fn split_method_name(full_name: &'static str) -> Option<(&'static str, &'static str)> {
    let without_prefix = full_name.strip_prefix('/')?;
    let (service, method) = without_prefix.rsplit_once('/')?;
    (!service.is_empty() && !method.is_empty()).then_some((service, method))
}

/// 业务作用：校验配置方法键遵循 `/package.Service/Method`，阻止开放式路由文本进入策略表。
///
/// 参数说明：
/// - `full_name`: 最终配置中出现的 owned 方法路径。
///
/// 返回：service 与 method 都非空且没有额外路径段时返回两段切片，否则返回 `None`。
fn split_owned_method_name(full_name: &str) -> Option<(&str, &str)> {
    let without_prefix = full_name.strip_prefix('/')?;
    let (service, method) = without_prefix.split_once('/')?;
    (!service.is_empty() && !method.is_empty() && !method.contains('/'))
        .then_some((service, method))
}

/// 请求与响应 body 共用的完成结局槽；owner 析构后拒绝迟到写入。
struct RpcCompletionState {
    outcome: StdMutex<Option<GrpcRpcOutcome>>,
    finalized: AtomicBool,
}

/// 请求方向使用的完成结局报告句柄；它不拥有 active 或 permit 的最终释放责任。
#[derive(Clone)]
struct RpcCompletionReporter {
    state: Arc<RpcCompletionState>,
}

impl RpcCompletionReporter {
    /// 业务作用：提交请求或响应方向观察到的首个终止结局，供唯一 owner 在同一边界结算。
    ///
    /// 参数说明：
    /// - `outcome`: 从方向性累计边界、deadline、trailers 或传输状态派生的封闭结局。
    ///
    /// 返回：owner 尚未结算且此前没有结局时写入；迟到或重复报告保持首次事实。
    fn finish(&self, outcome: GrpcRpcOutcome) {
        if self.state.finalized.load(Ordering::Acquire) {
            return;
        }
        let mut current = self
            .state
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.state.finalized.load(Ordering::Acquire) && current.is_none() {
            *current = Some(outcome);
        }
    }
}

/// 已接纳 RPC 的唯一会计 owner；请求方向只报告结局，active 与 permit 仅由本 guard 释放。
struct RpcCompletionGuard {
    owner: Arc<RpcMetrics>,
    method: &'static str,
    _permit: Option<OwnedSemaphorePermit>,
    _method_permit: Option<OwnedSemaphorePermit>,
    state: Arc<RpcCompletionState>,
}

impl RpcCompletionGuard {
    /// 业务作用：设置本 RPC 的唯一最终结局；真正提交在 drop 中与 active/permit 同步完成。
    ///
    /// 参数说明：
    /// - `outcome`: 从固定 gRPC code 或框架边界派生的封闭结局。
    ///
    /// 返回：无；重复调用保留首次结局。
    fn finish(&self, outcome: GrpcRpcOutcome) {
        self.reporter().finish(outcome);
    }

    /// 业务作用：为请求 body 派生不拥有资源释放权的结局报告句柄，使接收方向越界能结算同一 RPC。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与唯一 owner 共用首个结局槽的克隆句柄。
    fn reporter(&self) -> RpcCompletionReporter {
        RpcCompletionReporter {
            state: Arc::clone(&self.state),
        }
    }
}

impl Drop for RpcCompletionGuard {
    /// 业务作用：在 handler、response body、取消或断连任一路径退出时恰好结算一次并归还 permit。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；首个已报告结局被提交，未报告结局按 `cancelled` 结算，active 与两级 permit 同步释放。
    fn drop(&mut self) {
        self.state.finalized.store(true, Ordering::Release);
        let outcome = self
            .state
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unwrap_or(GrpcRpcOutcome::Cancelled);
        let mut values = self
            .owner
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(value) = values.get_mut(self.method) {
            value.active = value.active.saturating_sub(1);
            let count = value.outcomes.entry(outcome).or_default();
            increment_saturating(count);
        }
        let _ = self._permit.take();
        let _ = self._method_permit.take();
    }
}

/// generated server 进入稳定 registry 的唯一适配合同。
pub trait ManagedGrpcService: Send + 'static {
    /// 业务作用：返回 protobuf 完整 service name，作为路由与重复登记身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：编译期固定且不含动态业务数据的 service name。
    fn service_name(&self) -> &'static str;

    /// 业务作用：返回构建期生成的完整 descriptor set，供冲突检查和 reflection 使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与本 server 同一次 codegen 产生的静态 descriptor bytes。
    fn descriptor_set(&self) -> &'static [u8];

    /// 业务作用：返回生成器写入的 ABI 标识，阻止不同主版本的生成物进入同一运行时。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：codegen ABI 整数。
    fn codegen_abi(&self) -> u32;

    /// 业务作用：返回该 service 的固定方法目录，供容量、指标和策略在开放 listener 前封口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：完整方法路径和 streaming 形态的静态切片。
    fn methods(&self) -> &'static [GrpcMethodDescriptor];

    /// 业务作用：应用最终消息上限并把具体 generated server 加入类型擦除路由。
    ///
    /// 参数说明：
    /// - `routes`: 进程内唯一 tonic routes builder。
    /// - `limits`: 已通过全局与 service 策略合并的消息边界。
    /// - `policy`: 全部 service 共享的 RPC、字节与 streaming 运行边界。
    ///
    /// 返回：无；消费本 service 后路由新增且不能再次登记。
    fn add_to_routes(
        self: Box<Self>,
        routes: &mut tonic::service::RoutesBuilder,
        limits: GrpcMessageLimits,
        policy: GrpcServicePolicy,
    );
}

/// descriptor 复验通过后可与 service 一起原子提交的文件与 symbol。
struct DescriptorValidation {
    files: Vec<(String, Vec<u8>)>,
    symbols: Vec<String>,
}

/// UserHook 与独立 `ServerPlan` 共用的线性 service registry。
#[derive(Default)]
pub struct ManagedServiceRegistry {
    names: BTreeSet<&'static str>,
    method_names: BTreeSet<&'static str>,
    descriptor_files: BTreeMap<String, Vec<u8>>,
    descriptor_symbols: BTreeSet<String>,
    services: Vec<Box<dyn ManagedGrpcService>>,
    method_count: usize,
}

impl ManagedServiceRegistry {
    /// 业务作用：创建尚未登记 service、未产生 Router 或网络副作用的 registry。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可接收最多 64 个 service、256 个 method 的空 registry。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：登记一个由统一 codegen 生成的 service，并冻结其身份元数据。
    ///
    /// 参数说明：
    /// - `service`: 尚未加入任何 Router 的 generated server。
    ///
    /// 返回：ABI、descriptor、名称与规模合法且未重复时成功；失败时 registry 保持原样。
    pub fn register<S>(&mut self, service: S) -> Result<(), GrpcServerError>
    where
        S: ManagedGrpcService,
    {
        self.register_boxed(Box::new(service))
    }

    /// 业务作用：登记已经由组合计划完成类型擦除的 generated service，复用与普通登记相同的原子门禁。
    ///
    /// 参数说明：
    /// - `service`: 尚未加入任何 Router、仍由调用方独占的 boxed generated server。
    ///
    /// 返回：ABI、descriptor、名称与规模合法且未重复时成功；失败时 registry 保持原状。
    pub fn register_boxed(
        &mut self,
        service: Box<dyn ManagedGrpcService>,
    ) -> Result<(), GrpcServerError> {
        if service.codegen_abi() != CODEGEN_ABI {
            return Err(GrpcServerError::CodegenAbiMismatch);
        }
        let name = service.service_name();
        if name.is_empty()
            || name.len() > 256
            || service.descriptor_set().is_empty()
            || service.descriptor_set().len() > MAX_GRPC_DESCRIPTOR_BYTES
        {
            return Err(GrpcServerError::InvalidServiceMetadata);
        }
        if self.names.contains(name) {
            return Err(GrpcServerError::DuplicateService);
        }
        let descriptor = self.validate_descriptor(service.as_ref())?;
        let next_methods = self
            .method_count
            .checked_add(service.methods().len())
            .ok_or(GrpcServerError::ServiceCapacityExceeded)?;
        if self.services.len() >= MAX_GRPC_SERVICES || next_methods > MAX_GRPC_METHODS {
            return Err(GrpcServerError::ServiceCapacityExceeded);
        }
        let method_prefix = format!("/{name}/");
        if service.methods().iter().any(|method| {
            method.full_name.is_empty()
                || method.full_name.len() > 512
                || !method.full_name.starts_with(&method_prefix)
                || self.method_names.contains(method.full_name)
        }) {
            return Err(GrpcServerError::InvalidServiceMetadata);
        }
        self.names.insert(name);
        self.method_names
            .extend(service.methods().iter().map(|method| method.full_name));
        self.descriptor_files.extend(descriptor.files);
        self.descriptor_symbols.extend(descriptor.symbols);
        self.method_count = next_methods;
        self.services.push(service);
        Ok(())
    }

    /// 业务作用：返回已经冻结的业务 service 数量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含自动 health/reflection service 的登记数。
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// 业务作用：判断是否尚未登记任何业务 service。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空 registry 返回 `true`。
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// 业务作用：返回 sealed registry 的业务 RPC method 数，供 listener 绑定前计算固定观测基数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含自动 health 与可选 reflection 方法，且不超过受管 method 硬上限。
    pub fn method_count(&self) -> usize {
        self.method_count
    }

    /// 业务作用：把启动前冻结的 service、名称与 descriptor 线性移交给唯一 `ServerPlan`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：每个 service 仅能消费一次的路由输入，以及 health/reflection 使用的静态元数据。
    fn into_services(self) -> Vec<Box<dyn ManagedGrpcService>> {
        self.services
    }

    /// 业务作用：复验 descriptor 内确实存在本 service/method，并拒绝同文件或同 symbol 的不同定义。
    ///
    /// 参数说明：
    /// - `service`: 尚未提交 registry 的 generated service 元数据。
    ///
    /// 返回：可在其它门禁通过后原子提交的新文件与 symbol；解析、身份或冲突不合法时返回错误。
    fn validate_descriptor<S>(&self, service: &S) -> Result<DescriptorValidation, GrpcServerError>
    where
        S: ManagedGrpcService + ?Sized,
    {
        let descriptor = prost_types::FileDescriptorSet::decode(service.descriptor_set())
            .map_err(|_| GrpcServerError::InvalidServiceMetadata)?;
        if descriptor.file.is_empty() || descriptor.file.len() > 256 {
            return Err(GrpcServerError::InvalidServiceMetadata);
        }
        let mut new_files = Vec::new();
        let mut new_symbols = Vec::new();
        let mut candidate_symbols = BTreeSet::new();
        let mut declared_methods = None;
        for file in descriptor.file {
            let file_name = file
                .name
                .as_deref()
                .filter(|name| !name.is_empty() && name.len() <= 512)
                .ok_or(GrpcServerError::InvalidServiceMetadata)?;
            let encoded = file.encode_to_vec();
            if let Some(existing) = self.descriptor_files.get(file_name) {
                if existing != &encoded {
                    return Err(GrpcServerError::DescriptorConflict);
                }
            } else {
                let package = file.package.as_deref().unwrap_or_default();
                for declared_service in &file.service {
                    let service_name = declared_service
                        .name
                        .as_deref()
                        .ok_or(GrpcServerError::InvalidServiceMetadata)?;
                    let full_name = if package.is_empty() {
                        service_name.to_owned()
                    } else {
                        format!("{package}.{service_name}")
                    };
                    if self.descriptor_symbols.contains(&full_name)
                        || !candidate_symbols.insert(full_name.clone())
                    {
                        return Err(GrpcServerError::DescriptorConflict);
                    }
                    new_symbols.push(full_name);
                }
                new_files.push((file_name.to_owned(), encoded));
            }

            let package = file.package.as_deref().unwrap_or_default();
            for declared_service in &file.service {
                let Some(short_name) = declared_service.name.as_deref() else {
                    continue;
                };
                let full_name = if package.is_empty() {
                    short_name.to_owned()
                } else {
                    format!("{package}.{short_name}")
                };
                if full_name == service.service_name() {
                    if declared_methods.is_some() {
                        return Err(GrpcServerError::DescriptorConflict);
                    }
                    declared_methods = Some(
                        declared_service
                            .method
                            .iter()
                            .map(|method| {
                                let method_name = method
                                    .name
                                    .as_deref()
                                    .ok_or(GrpcServerError::InvalidServiceMetadata)?;
                                let rpc_type =
                                    match (method.client_streaming(), method.server_streaming()) {
                                        (false, false) => GrpcMethodType::Unary,
                                        (true, false) => GrpcMethodType::ClientStreaming,
                                        (false, true) => GrpcMethodType::ServerStreaming,
                                        (true, true) => GrpcMethodType::BidirectionalStreaming,
                                    };
                                Ok((format!("/{full_name}/{method_name}"), rpc_type))
                            })
                            .collect::<Result<Vec<_>, GrpcServerError>>()?,
                    );
                }
            }
        }
        let declared_methods = declared_methods.ok_or(GrpcServerError::InvalidServiceMetadata)?;
        let generated_methods = service
            .methods()
            .iter()
            .map(|method| (method.full_name.to_owned(), method.rpc_type))
            .collect::<Vec<_>>();
        if declared_methods != generated_methods {
            return Err(GrpcServerError::InvalidServiceMetadata);
        }
        Ok(DescriptorValidation {
            files: new_files,
            symbols: new_symbols,
        })
    }
}

/// generated adapter 使用的共享 RPC 与消息预算；字段保持私有，业务不能绕过 `ServerPlan` 构造。
#[doc(hidden)]
#[derive(Clone)]
pub struct GrpcServicePolicy {
    rpc_slots: Arc<Semaphore>,
    message_slots: Arc<Semaphore>,
    rpc_metrics: Arc<RpcMetrics>,
    method_policies: Arc<BTreeMap<&'static str, MethodPolicyRuntime>>,
    unary_timeout: Duration,
    stream_idle_timeout: Duration,
    max_stream_duration: Duration,
    max_received_messages_per_stream: u64,
    max_sent_messages_per_stream: u64,
    max_received_stream_bytes: u64,
    max_sent_stream_bytes: u64,
    message_limits: GrpcMessageLimits,
}

impl GrpcServicePolicy {
    /// 业务作用：从已校验 server 配置创建全部 service 共用的进程级 permit 与 streaming 边界。
    ///
    /// 参数说明：
    /// - `config`: 已通过容量、时间和 managed memory 门禁的最终配置。
    /// - `rpc_metrics`: 由封口 descriptor 预创建的固定方法会计目录。
    ///
    /// 返回：RPC 与消息 permit 尚未占用的共享策略。
    fn from_config(config: &GrpcServerConfig, rpc_metrics: Arc<RpcMetrics>) -> Self {
        let method_policies = config
            .method_policies
            .iter()
            .filter_map(|(method, policy)| {
                let method = rpc_metrics
                    .methods
                    .keys()
                    .copied()
                    .find(|candidate| *candidate == method)?;
                Some((method, MethodPolicyRuntime::new(policy)))
            })
            .collect();
        Self {
            rpc_slots: Arc::new(Semaphore::new(config.max_inflight_rpcs)),
            message_slots: Arc::new(Semaphore::new(config.max_inflight_message_bytes)),
            rpc_metrics,
            method_policies: Arc::new(method_policies),
            unary_timeout: config.unary_timeout,
            stream_idle_timeout: config.stream_idle_timeout,
            max_stream_duration: config.max_stream_duration,
            max_received_messages_per_stream: config.max_received_messages_per_stream,
            max_sent_messages_per_stream: config.max_sent_messages_per_stream,
            max_received_stream_bytes: config.max_received_stream_bytes,
            max_sent_stream_bytes: config.max_sent_stream_bytes,
            message_limits: config.message_limits,
        }
    }
}

/// 单个 descriptor 方法的运行期授权、并发与速率状态。
struct MethodPolicyRuntime {
    require_peer_identity: bool,
    inflight: Option<Arc<Semaphore>>,
    rate: Option<(u32, u32, StdMutex<TokenBucket>)>,
}

/// 方法级准入失败的内部分类；调用层将其同时映射为稳定 status 与指标 reason。
enum MethodAdmissionError {
    PeerIdentity,
    Concurrency,
    Rate,
}

impl MethodAdmissionError {
    /// 业务作用：把方法级准入失败映射为客户端可见的稳定 gRPC status。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份缺失为 `Unauthenticated`，容量与速率耗尽为 `ResourceExhausted`。
    fn status(&self) -> tonic::Status {
        match self {
            Self::PeerIdentity => {
                tonic::Status::unauthenticated("verified gRPC peer identity is required")
            }
            Self::Concurrency => {
                tonic::Status::resource_exhausted("gRPC method concurrency exhausted")
            }
            Self::Rate => tonic::Status::resource_exhausted("gRPC method rate exhausted"),
        }
    }

    /// 业务作用：把方法级失败映射为封闭指标原因，保持 status 文本不进入标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与当前门禁一一对应的稳定拒绝原因。
    fn reason(&self) -> GrpcRpcRejectionReason {
        match self {
            Self::PeerIdentity => GrpcRpcRejectionReason::PeerIdentity,
            Self::Concurrency => GrpcRpcRejectionReason::MethodConcurrency,
            Self::Rate => GrpcRpcRejectionReason::MethodRate,
        }
    }
}

impl MethodPolicyRuntime {
    /// 业务作用：从已校验的配置创建固定容量 permit 与 token bucket，不在请求路径动态建表。
    ///
    /// 参数说明：
    /// - `policy`: 对 descriptor 方法的最终收紧策略。
    ///
    /// 返回：全部状态在 listener 开放前创建的运行期策略。
    fn new(policy: &GrpcMethodPolicy) -> Self {
        Self {
            require_peer_identity: policy.require_peer_identity,
            inflight: policy.max_inflight_rpcs.map(Semaphore::new).map(Arc::new),
            rate: policy
                .requests_per_second
                .zip(policy.burst)
                .map(|(rate, burst)| (rate, burst, StdMutex::new(TokenBucket::new(burst)))),
        }
    }

    /// 业务作用：在 handler 前执行已验证身份、方法并发和方法速率门禁，失败不进入业务代码。
    ///
    /// 参数说明：
    /// - `has_peer_identity`: TLS driver 是否发布了已验证 mTLS leaf 身份。
    ///
    /// 返回：授权与容量均满足时返回方法 permit；否则返回可同时生成 status 与指标 reason 的封闭分类。
    fn admit(
        &self,
        has_peer_identity: bool,
    ) -> Result<Option<OwnedSemaphorePermit>, MethodAdmissionError> {
        if self.require_peer_identity && !has_peer_identity {
            return Err(MethodAdmissionError::PeerIdentity);
        }
        let permit = self
            .inflight
            .as_ref()
            .map(|slots| {
                Arc::clone(slots)
                    .try_acquire_owned()
                    .map_err(|_| MethodAdmissionError::Concurrency)
            })
            .transpose()?;
        if let Some((rate, burst, bucket)) = &self.rate {
            let allowed = bucket
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .try_take(*rate, *burst, Instant::now());
            if !allowed {
                return Err(MethodAdmissionError::Rate);
            }
        }
        Ok(permit)
    }
}

/// generated server 的统一运行包装，保留 protobuf service name 并跨所有 service 共享资源门禁。
#[doc(hidden)]
pub struct ManagedService<S> {
    inner: S,
    policy: GrpcServicePolicy,
    methods: &'static [GrpcMethodDescriptor],
}

impl<S> ManagedService<S> {
    /// 业务作用：把 generated server、共享预算与静态方法目录绑定为不可分离的路由 service。
    ///
    /// 参数说明：
    /// - `inner`: 已应用单消息编解码上限的 generated server。
    /// - `policy`: 本 listener 全部业务 service 共用的运行预算。
    /// - `methods`: codegen 从 descriptor 生成的完整方法目录。
    ///
    /// 返回：尚未处理请求但已具备所有受管门禁的 service。
    pub fn new(
        inner: S,
        policy: GrpcServicePolicy,
        methods: &'static [GrpcMethodDescriptor],
    ) -> Self {
        Self {
            inner,
            policy,
            methods,
        }
    }
}

impl<S> Clone for ManagedService<S>
where
    S: Clone,
{
    /// 业务作用：为 tonic 每连接路由复制 generated server 句柄，同时保持进程级 semaphore 共享。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：拥有独立 service clone、共享预算和同一静态方法目录的实例。
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            policy: self.policy.clone(),
            methods: self.methods,
        }
    }
}

impl<S> tonic::server::NamedService for ManagedService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// listener 全部请求与响应 body 共用的消息字节 permit 池。
struct MessagePermits {
    semaphore: Arc<Semaphore>,
}

impl MessagePermits {
    /// 业务作用：为一条 RPC 创建尚未占用消息字节预算的共享会计状态。
    ///
    /// 参数说明：
    /// - `semaphore`: listener 全局消息字节 permit 池。
    ///
    /// 返回：可由请求和响应 body 同时申请实际在途字节的状态。
    fn new(semaphore: Arc<Semaphore>) -> Self {
        Self { semaphore }
    }

    /// 业务作用：为已经实际读入或即将交给 transport 的消息字节取得 permit，避免声明长度预扣整份预算。
    ///
    /// 参数说明：
    /// - `bytes`: 当前已经进入 body 的 protobuf payload 或压缩展开保守权重。
    ///
    /// 返回：容量可立即取得时返回由当前消息持有的 permit；零字节返回 `None`；不足时返回
    /// `ResourceExhausted`，不继续接收该消息。
    fn acquire(&self, bytes: u32) -> Result<Option<OwnedSemaphorePermit>, tonic::Status> {
        if bytes == 0 {
            return Ok(None);
        }
        let permit = Arc::clone(&self.semaphore)
            .try_acquire_many_owned(bytes)
            .map_err(|_| tonic::Status::resource_exhausted("gRPC message budget exhausted"))?;
        Ok(Some(permit))
    }
}

/// gRPC frame 会计失败的封闭内部分类；方向由 body policy 映射为公开完成结局。
enum GrpcFrameAccountingFailure {
    InvalidCompressionFlag,
    MessageLimit,
    ByteLimit,
    InflightBudget,
}

impl GrpcFrameAccountingFailure {
    /// 业务作用：把 frame 会计失败映射为稳定客户端 status，不暴露动态 payload 数据。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：格式异常为 `Internal`，累计或进程容量越界为 `ResourceExhausted`。
    fn status(&self) -> tonic::Status {
        match self {
            Self::InvalidCompressionFlag => {
                tonic::Status::internal("invalid gRPC compression flag")
            }
            Self::MessageLimit | Self::ByteLimit => {
                tonic::Status::resource_exhausted("gRPC stream message boundary exceeded")
            }
            Self::InflightBudget => {
                tonic::Status::resource_exhausted("gRPC message budget exhausted")
            }
        }
    }
}

/// 一个方向的 gRPC length-prefixed frame 会计状态。
#[derive(Default)]
struct GrpcFrameAccounting {
    header: [u8; 5],
    header_len: usize,
    payload_remaining: u32,
    messages: u64,
    payload_bytes: u64,
    current_compressed: bool,
    current_payload_bytes: u32,
    current_permits: Vec<OwnedSemaphorePermit>,
}

impl GrpcFrameAccounting {
    /// 业务作用：跨 HTTP/2 DATA chunk 解析 gRPC frame 边界，并在读取 payload 前落实数量、字节和进程预算。
    ///
    /// 参数说明：
    /// - `data`: 本轮 body frame 的连续字节。
    /// - `max_messages`: 当前方向允许的完整 gRPC frame 数量。
    /// - `max_bytes`: 当前方向允许的 frame payload 累计字节数。
    /// - `compressed_message_bytes`: 压缩帧在解压或编码期间按单消息硬上限预留的保守字节权重。
    /// - `permits`: 本 RPC 入站与出站共享的进程级字节 permit owner。
    ///
    /// - `release_after_frame`: 已完成消息的 permit 移交位置；调用方在下次 body poll 时释放，确保
    ///   当前 DATA frame 被下游消费前仍计入在途。
    ///
    /// 返回：所有实际读入字节都取得预算时返回本轮是否完成至少一条业务消息；格式、数量、字节或
    /// 进程容量越界时返回封闭失败分类。
    fn account(
        &mut self,
        data: &[u8],
        max_messages: u64,
        max_bytes: u64,
        compressed_message_bytes: u32,
        permits: &MessagePermits,
        release_after_frame: &mut Vec<OwnedSemaphorePermit>,
    ) -> Result<bool, GrpcFrameAccountingFailure> {
        let mut offset = 0_usize;
        let mut message_completed = false;
        while offset < data.len() {
            if self.payload_remaining > 0 {
                let consumed = usize::try_from(self.payload_remaining)
                    .unwrap_or(usize::MAX)
                    .min(data.len() - offset);
                let consumed_u32 = u32::try_from(consumed)
                    .map_err(|_| GrpcFrameAccountingFailure::InflightBudget)?;
                if let Some(permit) = permits
                    .acquire(consumed_u32)
                    .map_err(|_| GrpcFrameAccountingFailure::InflightBudget)?
                {
                    self.current_permits.push(permit);
                }
                self.payload_remaining -= consumed as u32;
                offset += consumed;
                if self.payload_remaining == 0 {
                    if self.current_compressed {
                        let expansion =
                            compressed_message_bytes.saturating_sub(self.current_payload_bytes);
                        if let Some(permit) = permits
                            .acquire(expansion)
                            .map_err(|_| GrpcFrameAccountingFailure::InflightBudget)?
                        {
                            self.current_permits.push(permit);
                        }
                    }
                    release_after_frame.append(&mut self.current_permits);
                    message_completed = true;
                }
                continue;
            }

            let header_bytes = (5 - self.header_len).min(data.len() - offset);
            self.header[self.header_len..self.header_len + header_bytes]
                .copy_from_slice(&data[offset..offset + header_bytes]);
            self.header_len += header_bytes;
            offset += header_bytes;
            if self.header_len < 5 {
                continue;
            }
            if self.header[0] > 1 {
                return Err(GrpcFrameAccountingFailure::InvalidCompressionFlag);
            }
            let payload = u32::from_be_bytes([
                self.header[1],
                self.header[2],
                self.header[3],
                self.header[4],
            ]);
            let next_messages = self.messages.saturating_add(1);
            let next_bytes = self.payload_bytes.saturating_add(u64::from(payload));
            if next_messages > max_messages {
                return Err(GrpcFrameAccountingFailure::MessageLimit);
            }
            if next_bytes > max_bytes {
                return Err(GrpcFrameAccountingFailure::ByteLimit);
            }
            self.messages = next_messages;
            self.payload_bytes = next_bytes;
            self.payload_remaining = payload;
            self.current_compressed = self.header[0] == 1;
            self.current_payload_bytes = payload;
            self.header_len = 0;
            if payload == 0 {
                if self.current_compressed {
                    if let Some(permit) = permits
                        .acquire(compressed_message_bytes)
                        .map_err(|_| GrpcFrameAccountingFailure::InflightBudget)?
                    {
                        self.current_permits.push(permit);
                    }
                }
                release_after_frame.append(&mut self.current_permits);
                message_completed = true;
            }
        }
        Ok(message_completed)
    }
}

/// 为请求或响应 body 应用 deadline、idle、frame 数量、字节和 permit 生命周期。
struct ManagedBody<B> {
    inner: B,
    accounting: GrpcFrameAccounting,
    permits: Arc<MessagePermits>,
    max_messages: u64,
    max_bytes: u64,
    compressed_message_bytes: u32,
    deadline: Pin<Box<tokio::time::Sleep>>,
    idle: Option<Pin<Box<tokio::time::Sleep>>>,
    idle_timeout: Option<Duration>,
    completion_owner: Option<RpcCompletionGuard>,
    completion_reporter: RpcCompletionReporter,
    completion_outcome: Option<GrpcRpcOutcome>,
    deadline_outcome: GrpcRpcOutcome,
    message_limit_outcome: GrpcRpcOutcome,
    byte_limit_outcome: GrpcRpcOutcome,
    release_after_frame: Vec<OwnedSemaphorePermit>,
    ended: bool,
}

/// 单方向 body 在开始轮询前已冻结的消息、时间与结局边界。
#[derive(Clone, Copy)]
struct ManagedBodyPolicy {
    max_messages: u64,
    max_bytes: u64,
    compressed_message_bytes: u32,
    deadline: tokio::time::Instant,
    idle_timeout: Option<Duration>,
    deadline_outcome: GrpcRpcOutcome,
    message_limit_outcome: GrpcRpcOutcome,
    byte_limit_outcome: GrpcRpcOutcome,
}

impl<B> ManagedBody<B> {
    /// 业务作用：创建与当前 RPC 共用消息会计和截止时间的单方向 body 包装。
    ///
    /// 参数说明：
    /// - `inner`: tonic 即将读取或写出的底层 body。
    /// - `permits`: 请求与响应共同持有的消息字节 permit owner。
    /// - `policy`: 本方向已冻结的 frame 数、字节、压缩权重和时间边界。
    /// - `completion_reporter`: 请求与响应方向共用的首个终止结局报告句柄。
    /// - `completion_owner`: 只由响应 body 持有的唯一 RPC 会计 owner。
    /// - `completion_outcome`: response headers 已携带最终 grpc-status 时的预解析结局。
    ///
    /// 返回：尚未轮询底层 body 的受管包装。
    fn new(
        inner: B,
        permits: Arc<MessagePermits>,
        policy: ManagedBodyPolicy,
        completion_reporter: RpcCompletionReporter,
        completion_owner: Option<RpcCompletionGuard>,
        completion_outcome: Option<GrpcRpcOutcome>,
    ) -> Self {
        Self {
            inner,
            accounting: GrpcFrameAccounting::default(),
            permits,
            max_messages: policy.max_messages,
            max_bytes: policy.max_bytes,
            compressed_message_bytes: policy.compressed_message_bytes,
            deadline: Box::pin(tokio::time::sleep_until(policy.deadline)),
            idle: policy
                .idle_timeout
                .map(|duration| Box::pin(tokio::time::sleep(duration))),
            idle_timeout: policy.idle_timeout,
            completion_owner,
            completion_reporter,
            completion_outcome,
            deadline_outcome: policy.deadline_outcome,
            message_limit_outcome: policy.message_limit_outcome,
            byte_limit_outcome: policy.byte_limit_outcome,
            release_after_frame: Vec::new(),
            ended: false,
        }
    }

    /// 业务作用：在响应流终结前为唯一完成守卫记录结局，随后由守卫统一减 active 并释放 permit。
    ///
    /// 参数说明：
    /// - `outcome`: 当前 frame、trailers 或时间边界确定的封闭结局。
    ///
    /// 返回：无；请求与响应方向竞争同一个首次结局槽，资源仍只由响应 owner 释放。
    fn finish(&self, outcome: GrpcRpcOutcome) {
        self.completion_reporter.finish(outcome);
    }

    /// 业务作用：在任一运行边界越界后形成一次终止 frame，并停止继续轮询业务 body。
    ///
    /// 参数说明：
    /// - `status`: 需要返回给 tonic codec 或远端的稳定 gRPC 错误。
    /// - `outcome`: 与当前方向、deadline 或容量边界一一对应的封闭完成结局。
    ///
    /// 返回：响应方向生成标准 `grpc-status` trailers，使客户端观察准确 code；请求方向返回 body error，
    /// 由 generated server 转成同一 status 响应。
    fn terminate(
        &mut self,
        status: tonic::Status,
        outcome: GrpcRpcOutcome,
    ) -> Poll<Option<Result<http_body::Frame<bytes::Bytes>, tonic::Status>>> {
        self.finish(outcome);
        self.ended = true;
        if self.completion_owner.is_some() {
            let mut trailers = tonic::codegen::http::HeaderMap::new();
            let code =
                tonic::codegen::http::HeaderValue::from_str(&(status.code() as i32).to_string())
                    .expect("tonic Code decimal representation is always a valid header value");
            trailers.insert("grpc-status", code);
            Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))))
        } else {
            Poll::Ready(Some(Err(status)))
        }
    }
}

impl<B> http_body::Body for ManagedBody<B>
where
    B: http_body::Body<Data = bytes::Bytes, Error = tonic::Status> + Unpin,
{
    type Data = bytes::Bytes;
    type Error = tonic::Status;

    /// 业务作用：在转发 DATA/trailer 前复验持续时间、空闲、frame 数量和共享字节预算。
    ///
    /// 参数说明：
    /// - `context`: 底层 body 和 deadline timer 的异步唤醒上下文。
    ///
    /// 返回：边界内透传底层 frame；越界时只返回一次 gRPC status 并结束本方向。
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        // 上一 DATA frame 只有在下游再次轮询 body 时才可视为已经消费；在此释放可避免把长流
        // 累计字节误当在途，同时仍覆盖跨 poll 拼接的半条消息。
        self.release_after_frame.clear();
        if self.deadline.as_mut().poll(context).is_ready() {
            let outcome = self.deadline_outcome;
            return self.terminate(
                tonic::Status::deadline_exceeded("gRPC stream duration exceeded"),
                outcome,
            );
        }
        if self
            .idle
            .as_mut()
            .is_some_and(|idle| idle.as_mut().poll(context).is_ready())
        {
            let outcome = GrpcRpcOutcome::ServerStreamIdle;
            return self.terminate(
                tonic::Status::deadline_exceeded("gRPC stream idle timeout exceeded"),
                outcome,
            );
        }
        match Pin::new(&mut self.inner).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if self.completion_owner.is_some() {
                    if let Some(trailers) = frame.trailers_ref() {
                        let outcome =
                            grpc_outcome_from_headers(trailers).unwrap_or(GrpcRpcOutcome::Ok);
                        self.finish(outcome);
                    }
                }
                if let Some(data) = frame.data_ref() {
                    let max_messages = self.max_messages;
                    let max_bytes = self.max_bytes;
                    let compressed_message_bytes = self.compressed_message_bytes;
                    let permits = Arc::clone(&self.permits);
                    let mut release_after_frame = std::mem::take(&mut self.release_after_frame);
                    let message_completed = match self.accounting.account(
                        data,
                        max_messages,
                        max_bytes,
                        compressed_message_bytes,
                        &permits,
                        &mut release_after_frame,
                    ) {
                        Ok(completed) => completed,
                        Err(failure) => {
                            self.release_after_frame = release_after_frame;
                            let outcome = match failure {
                                GrpcFrameAccountingFailure::MessageLimit => {
                                    self.message_limit_outcome
                                }
                                GrpcFrameAccountingFailure::ByteLimit => self.byte_limit_outcome,
                                GrpcFrameAccountingFailure::InvalidCompressionFlag => {
                                    GrpcRpcOutcome::Code(tonic::Code::Internal)
                                }
                                GrpcFrameAccountingFailure::InflightBudget => {
                                    GrpcRpcOutcome::Code(tonic::Code::ResourceExhausted)
                                }
                            };
                            return self.terminate(failure.status(), outcome);
                        }
                    };
                    self.release_after_frame = release_after_frame;
                    if message_completed {
                        let idle_timeout = self.idle_timeout;
                        if let (Some(idle), Some(duration)) = (self.idle.as_mut(), idle_timeout) {
                            idle.as_mut().reset(tokio::time::Instant::now() + duration);
                        }
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                if self.completion_owner.is_some() {
                    let outcome = self.completion_outcome.unwrap_or(GrpcRpcOutcome::Ok);
                    self.finish(outcome);
                }
                self.ended = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(status))) => {
                self.finish(GrpcRpcOutcome::TransportLost);
                self.ended = true;
                Poll::Ready(Some(Err(status)))
            }
            other => other,
        }
    }

    /// 业务作用：保留底层 body 的精确剩余长度提示，不把安全上限误报为实际长度。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层 body 当前提供的 size hint。
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }

    /// 业务作用：在本包装已经终止时立即报告结束，否则遵循底层 body 状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不再可能产生 frame 时为 `true`。
    fn is_end_stream(&self) -> bool {
        self.ended || self.inner.is_end_stream()
    }
}

impl<B> Drop for ManagedBody<B> {
    /// 业务作用：响应 body 未被轮询但 headers 已含最终 status 时仍提交该结局，再由 owner 完成唯一结算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；没有最终 headers 的提前析构仍由 owner 归类为 `cancelled`。
    fn drop(&mut self) {
        if self.completion_owner.is_some() {
            if let Some(outcome) = self.completion_outcome {
                self.completion_reporter.finish(outcome);
            }
        }
    }
}

/// 业务作用：严格解析 gRPC `grpc-timeout` header，并拒绝会歧义或溢出的非标准输入。
///
/// 参数说明：
/// - `headers`: 当前 HTTP/2 请求的 metadata header 集合。
///
/// 返回：header 缺失时返回 `Ok(None)`；1..=8 位十进制数和合法单位返回时长；其它输入返回错误。
fn parse_grpc_timeout(headers: &tonic::codegen::http::HeaderMap) -> Result<Option<Duration>, ()> {
    let Some(value) = headers.get("grpc-timeout") else {
        return Ok(None);
    };
    let text = value.to_str().map_err(|_| ())?;
    if text.len() < 2 || text.len() > 9 {
        return Err(());
    }
    let (digits, unit) = text.split_at(text.len() - 1);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    let amount = digits.parse::<u64>().map_err(|_| ())?;
    let duration = match unit {
        "H" => Duration::from_secs(amount.checked_mul(60 * 60).ok_or(())?),
        "M" => Duration::from_secs(amount.checked_mul(60).ok_or(())?),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return Err(()),
    };
    Ok(Some(duration))
}

/// 业务作用：从 response headers 或 trailers 中解析最终 `grpc-status`，不读取动态错误正文。
///
/// 参数说明：
/// - `headers`: generated service 返回的 HTTP/2 headers 或 trailers。
///
/// 返回：存在合法整数 code 时返回封闭结局；缺失或非法时返回 `None`，由 body 终点继续判定。
fn grpc_outcome_from_headers(headers: &tonic::codegen::http::HeaderMap) -> Option<GrpcRpcOutcome> {
    let code = headers.get("grpc-status")?.to_str().ok()?.parse().ok()?;
    let code = tonic::Code::from_i32(code);
    Some(if code == tonic::Code::Ok {
        GrpcRpcOutcome::Ok
    } else {
        GrpcRpcOutcome::Code(code)
    })
}

impl<S> tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
    for ManagedService<S>
where
    S: tonic::codegen::Service<
            tonic::codegen::http::Request<ManagedBody<tonic::body::Body>>,
            Response = tonic::codegen::http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    /// 业务作用：沿用 generated server 的就绪状态，使 tonic 可以维持标准 service 背压协议。
    ///
    /// 参数说明：
    /// - `context`: 下层 service 的异步唤醒上下文。
    ///
    /// 返回：generated server 当前是否可受理下一条请求。
    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    /// 业务作用：按方法形态取得进程 RPC permit，并把 unary/streaming 全部运行边界包到请求与响应 body。
    ///
    /// 参数说明：
    /// - `request`: tonic 解码前的单条 gRPC HTTP/2 请求。
    ///
    /// 返回：取得容量后执行 generated service；容量或 deadline 越界时返回标准 gRPC status 响应。
    fn call(
        &mut self,
        mut request: tonic::codegen::http::Request<tonic::body::Body>,
    ) -> Self::Future {
        let descriptor = self
            .methods
            .iter()
            .find(|method| method.full_name == request.uri().path())
            .copied();
        let rpc_type = descriptor
            .map(|method| method.rpc_type)
            .unwrap_or(GrpcMethodType::Unary);
        let streaming = rpc_type != GrpcMethodType::Unary;
        let server_duration = if streaming {
            self.policy.max_stream_duration
        } else {
            self.policy.unary_timeout
        };
        let now = tokio::time::Instant::now();
        let server_deadline = now + server_duration;
        let client_deadline = match parse_grpc_timeout(request.headers()) {
            Ok(timeout) => timeout.map(|duration| now + duration),
            Err(()) => {
                return Box::pin(async {
                    Ok(
                        tonic::Status::invalid_argument("invalid grpc-timeout metadata")
                            .into_http(),
                    )
                });
            }
        };
        let (deadline, deadline_source) = match client_deadline {
            Some(client) if client <= server_deadline => (client, DeadlineSource::Client),
            _ => (server_deadline, DeadlineSource::Server),
        };
        request.extensions_mut().insert(Deadline {
            effective: deadline,
            source: deadline_source,
        });
        let rpc_permit = match Arc::clone(&self.policy.rpc_slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                if let Some(method) = descriptor {
                    self.policy
                        .rpc_metrics
                        .reject(method.full_name, GrpcRpcRejectionReason::ProcessConcurrency);
                }
                return Box::pin(async {
                    Ok(tonic::Status::resource_exhausted("gRPC RPC budget exhausted").into_http())
                });
            }
        };
        let method_permit = match descriptor
            .and_then(|method| self.policy.method_policies.get(method.full_name))
            .map(|policy| policy.admit(request.extensions().get::<PeerIdentity>().is_some()))
            .transpose()
        {
            Ok(permit) => permit.flatten(),
            Err(admission) => {
                if let Some(method) = descriptor {
                    self.policy
                        .rpc_metrics
                        .reject(method.full_name, admission.reason());
                }
                let status = admission.status();
                return Box::pin(async move { Ok(status.into_http()) });
            }
        };
        let completion = RpcMetrics::begin(
            Arc::clone(&self.policy.rpc_metrics),
            descriptor.map(|method| method.full_name).unwrap_or(""),
            rpc_permit,
            method_permit,
        );
        let max_received_messages = if streaming {
            self.policy.max_received_messages_per_stream
        } else {
            1
        };
        let max_received_bytes = if streaming {
            self.policy.max_received_stream_bytes
        } else {
            self.policy
                .max_received_stream_bytes
                .min(u64::from(u32::MAX))
        };
        let max_sent_messages = if streaming {
            self.policy.max_sent_messages_per_stream
        } else {
            1
        };
        let max_sent_bytes = if streaming {
            self.policy.max_sent_stream_bytes
        } else {
            self.policy.max_sent_stream_bytes.min(u64::from(u32::MAX))
        };
        let idle_timeout = streaming.then_some(self.policy.stream_idle_timeout);
        let deadline_outcome = match (deadline_source, streaming) {
            (DeadlineSource::Client, _) => GrpcRpcOutcome::ClientDeadlineExceeded,
            (DeadlineSource::Server, true) => GrpcRpcOutcome::ServerStreamDuration,
            (DeadlineSource::Server, false) => GrpcRpcOutcome::ServerTimeout,
        };
        let max_encoded_message_bytes = self.policy.message_limits.max_encoding_bytes as u32;
        let permits = Arc::new(MessagePermits::new(Arc::clone(&self.policy.message_slots)));
        let request_completion = completion.reporter();
        let request = request.map(|body| {
            ManagedBody::new(
                body,
                Arc::clone(&permits),
                ManagedBodyPolicy {
                    max_messages: max_received_messages,
                    max_bytes: max_received_bytes,
                    compressed_message_bytes: self.policy.message_limits.max_decoding_bytes as u32,
                    deadline,
                    idle_timeout,
                    deadline_outcome,
                    message_limit_outcome: GrpcRpcOutcome::StreamReceivedMessages,
                    byte_limit_outcome: GrpcRpcOutcome::StreamReceivedBytes,
                },
                request_completion,
                None,
                None,
            )
        });
        let future = self.inner.call(request);
        Box::pin(async move {
            let completion = completion;
            let mut response = match tokio::time::timeout_at(deadline, future).await {
                Ok(Ok(response)) => response,
                Ok(Err(never)) => match never {},
                Err(_) => {
                    completion.finish(match deadline_source {
                        DeadlineSource::Client => GrpcRpcOutcome::ClientDeadlineExceeded,
                        DeadlineSource::Server => GrpcRpcOutcome::ServerTimeout,
                    });
                    return Ok(
                        tonic::Status::deadline_exceeded("gRPC method duration exceeded")
                            .into_http(),
                    );
                }
            };
            let completion_outcome = grpc_outcome_from_headers(response.headers());
            let body = std::mem::take(response.body_mut());
            let response_completion = completion.reporter();
            *response.body_mut() = tonic::body::Body::new(ManagedBody::new(
                body,
                permits,
                ManagedBodyPolicy {
                    max_messages: max_sent_messages,
                    max_bytes: max_sent_bytes,
                    compressed_message_bytes: max_encoded_message_bytes,
                    deadline,
                    idle_timeout,
                    deadline_outcome,
                    message_limit_outcome: GrpcRpcOutcome::StreamSentMessages,
                    byte_limit_outcome: GrpcRpcOutcome::StreamSentBytes,
                },
                response_completion,
                Some(completion),
                completion_outcome,
            ));
            Ok(response)
        })
    }
}

/// gRPC transport 与停机边界。
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// listener 同时交给 tonic 的连接上限。
    pub max_connections: usize,
    /// 每连接并发 RPC 上限。
    pub concurrency_limit_per_connection: usize,
    /// accept 后完成 HTTP/2 preface 与首个 SETTINGS 的最长时间。
    pub connection_handshake_timeout: Duration,
    /// HTTP/2 建立后收到首个业务 HEADERS 的最长时间。
    pub first_request_timeout: Duration,
    /// 没有活动 RPC 的已建立连接允许保持的最长时间。
    pub idle_connection_timeout: Duration,
    /// unary RPC 从准入到最终 status 的 server timeout。
    pub unary_timeout: Duration,
    /// streaming 在没有完整业务消息进展时的最大空闲时长。
    pub stream_idle_timeout: Duration,
    /// 单条 streaming RPC 的最大持续时间。
    pub max_stream_duration: Duration,
    /// 单条 stream 接收方向允许的最大完整消息数。
    pub max_received_messages_per_stream: u64,
    /// 单条 stream 发送方向允许的最大完整消息数。
    pub max_sent_messages_per_stream: u64,
    /// 单条 stream 接收方向允许累计的最大消息字节数。
    pub max_received_stream_bytes: u64,
    /// 单条 stream 发送方向允许累计的最大消息字节数。
    pub max_sent_stream_bytes: u64,
    /// HTTP/2 keepalive ping 周期。
    pub keepalive_interval: Duration,
    /// HTTP/2 keepalive ack 超时。
    pub keepalive_timeout: Duration,
    /// 单连接最大并发 stream。
    pub max_concurrent_streams: u32,
    /// HTTP/2 stream 初始流控窗口。
    pub initial_stream_window_size: u32,
    /// HTTP/2 connection 初始流控窗口。
    pub initial_connection_window_size: u32,
    /// HTTP/2 最大 frame 大小。
    pub max_frame_size: u32,
    /// HTTP/2 request header list 上限。
    pub max_header_list_size: u32,
    /// HTTP/2 HPACK dynamic table 上限。
    pub header_table_size: u32,
    /// 每个 HTTP/2 stream 的发送缓冲上限。
    pub max_send_buffer_size: usize,
    /// Rapid Reset 的 pending accept reset 上限。
    pub max_pending_accept_reset_streams: usize,
    /// 本地协议错误 reset stream 上限。
    pub max_local_error_reset_streams: usize,
    /// 单连接每秒允许的 SETTINGS、PING 与 RST_STREAM 帧数。
    pub control_frames_per_second: u32,
    /// 单连接控制帧 token bucket 容量。
    pub control_frames_burst: u32,
    /// listener 全部连接每秒允许的控制帧总数。
    pub process_control_frames_per_second: u32,
    /// listener 进程级控制帧 token bucket 容量。
    pub process_control_frames_burst: u32,
    /// TCP keepalive 周期。
    pub tcp_keepalive: Duration,
    /// TCP keepalive probe 间隔。
    pub tcp_keepalive_interval: Duration,
    /// TCP keepalive 最大重试次数。
    pub tcp_keepalive_retries: u32,
    /// 是否关闭 Nagle 以降低 unary 延迟。
    pub tcp_nodelay: bool,
    /// 主动连接轮转时长；`None` 表示关闭。
    pub max_connection_age: Option<Duration>,
    /// 逐连接驱逐允许 PING 与既有 stream 共用的总排空预算。
    pub connection_eviction_grace: Duration,
    /// graceful drain 总预算。
    pub drain_timeout: Duration,
    /// 进程内受理的 RPC 总并发上限。
    pub max_inflight_rpcs: usize,
    /// 进程内实际读入请求、压缩展开权重和待发送响应共同使用的在途字节 permit 上限。
    pub max_inflight_message_bytes: usize,
    /// 框架受管连接、stream、RPC 与消息状态的会计预算。
    pub managed_memory_budget_bytes: usize,
    /// generated service 消息上限。
    pub message_limits: GrpcMessageLimits,
    /// 以 descriptor 完整方法路径为键的授权、并发与速率收紧策略。
    pub method_policies: BTreeMap<String, GrpcMethodPolicy>,
}

/// descriptor 固定方法的可选安全策略；所有字段只能收紧 listener 全局边界。
#[derive(Debug, Clone, Default)]
pub struct GrpcMethodPolicy {
    /// 是否要求请求来自验证通过的 mTLS client certificate。
    pub require_peer_identity: bool,
    /// 该方法的独立在途上限；`None` 表示只使用全局上限。
    pub max_inflight_rpcs: Option<usize>,
    /// 该方法每秒补充的进程内 token 数；`None` 表示不启用方法级速率门禁。
    pub requests_per_second: Option<u32>,
    /// 方法级 token bucket 容量；启用速率时必须同时提供且不小于每秒速率。
    pub burst: Option<u32>,
}

impl Default for GrpcServerConfig {
    /// 业务作用：提供连接、并发、超时、keepalive、stream 与 drain 均有界的保守默认值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接通过校验且仅在显式 start 后产生副作用的 server 配置。
    fn default() -> Self {
        Self {
            max_connections: 256,
            concurrency_limit_per_connection: 128,
            connection_handshake_timeout: Duration::from_secs(10),
            first_request_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(5 * 60),
            unary_timeout: Duration::from_secs(30),
            stream_idle_timeout: Duration::from_secs(5 * 60),
            max_stream_duration: Duration::from_secs(60 * 60),
            max_received_messages_per_stream: 1_000_000,
            max_sent_messages_per_stream: 1_000_000,
            max_received_stream_bytes: 16 * 1024 * 1024 * 1024,
            max_sent_stream_bytes: 16 * 1024 * 1024 * 1024,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            max_concurrent_streams: 128,
            initial_stream_window_size: 64 * 1024,
            initial_connection_window_size: 1024 * 1024,
            max_frame_size: 16 * 1024,
            max_header_list_size: 16 * 1024,
            header_table_size: 4 * 1024,
            max_send_buffer_size: 64 * 1024,
            max_pending_accept_reset_streams: 20,
            max_local_error_reset_streams: 20,
            control_frames_per_second: 100,
            control_frames_burst: 200,
            process_control_frames_per_second: 10_000,
            process_control_frames_burst: 20_000,
            tcp_keepalive: Duration::from_secs(60),
            tcp_keepalive_interval: Duration::from_secs(10),
            tcp_keepalive_retries: 3,
            tcp_nodelay: true,
            max_connection_age: None,
            connection_eviction_grace: Duration::from_secs(15),
            drain_timeout: Duration::from_secs(20),
            max_inflight_rpcs: 1_024,
            max_inflight_message_bytes: 128 * 1024 * 1024,
            managed_memory_budget_bytes: DEFAULT_GRPC_MANAGED_MEMORY_BYTES,
            message_limits: GrpcMessageLimits::default(),
            method_policies: BTreeMap::new(),
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
            || self.connection_handshake_timeout.is_zero()
            || self.first_request_timeout.is_zero()
            || self.idle_connection_timeout.is_zero()
            || self.unary_timeout.is_zero()
            || self.stream_idle_timeout.is_zero()
            || self.max_stream_duration.is_zero()
            || self.max_received_messages_per_stream == 0
            || self.max_received_messages_per_stream > 100_000_000
            || self.max_sent_messages_per_stream == 0
            || self.max_sent_messages_per_stream > 100_000_000
            || self.max_received_stream_bytes == 0
            || self.max_received_stream_bytes > 1024_u64.pow(4)
            || self.max_sent_stream_bytes == 0
            || self.max_sent_stream_bytes > 1024_u64.pow(4)
            || self.keepalive_interval.is_zero()
            || self.keepalive_timeout.is_zero()
            || self.max_concurrent_streams == 0
            || self.initial_stream_window_size == 0
            || self.initial_connection_window_size == 0
            || self.max_frame_size < 16_384
            || self.max_frame_size > 65_535
            || self.max_header_list_size == 0
            || self.header_table_size == 0
            || self.max_send_buffer_size == 0
            || self.max_pending_accept_reset_streams == 0
            || self.max_local_error_reset_streams == 0
            || self.control_frames_per_second == 0
            || self.control_frames_burst < self.control_frames_per_second
            || self.process_control_frames_per_second == 0
            || self.process_control_frames_burst < self.process_control_frames_per_second
            || self.tcp_keepalive.is_zero()
            || self.tcp_keepalive_interval.is_zero()
            || self.tcp_keepalive_retries == 0
            || self.connection_eviction_grace.is_zero()
            || self.drain_timeout.is_zero()
            || self.connection_eviction_grace >= self.drain_timeout
            || self.max_inflight_rpcs == 0
            || self.max_inflight_rpcs > MAX_GRPC_INFLIGHT_RPCS
            || self.max_inflight_message_bytes == 0
            || self.max_inflight_message_bytes > MAX_GRPC_INFLIGHT_MESSAGE_BYTES
            || self.managed_memory_budget_bytes == 0
            || self.managed_memory_budget_bytes > MAX_GRPC_MANAGED_MEMORY_BYTES
            || self.message_limits.max_decoding_bytes == 0
            || self.message_limits.max_encoding_bytes == 0
            || self.unary_timeout > Duration::from_secs(5 * 60)
            || self.connection_handshake_timeout > Duration::from_secs(60)
            || self.first_request_timeout > Duration::from_secs(5 * 60)
            || self.idle_connection_timeout > Duration::from_secs(60 * 60)
            || self.stream_idle_timeout > Duration::from_secs(60 * 60)
            || self.max_stream_duration > Duration::from_secs(7 * 24 * 60 * 60)
            || self.keepalive_interval > MAX_GRPC_DURATION
            || self.keepalive_timeout > MAX_GRPC_DURATION
            || self.tcp_keepalive > MAX_GRPC_DURATION
            || self.tcp_keepalive_interval > MAX_GRPC_DURATION
            || self.drain_timeout > MAX_GRPC_DURATION
            || self.max_connection_age.is_some_and(|age| {
                age < Duration::from_secs(60) || age > Duration::from_secs(24 * 60 * 60)
            })
            || self.max_concurrent_streams > 65_535
            || self.initial_stream_window_size > MAX_GRPC_STREAM_WINDOW_BYTES
            || self.initial_connection_window_size > MAX_GRPC_CONNECTION_WINDOW_BYTES
            || self.max_header_list_size > 64 * 1024
            || self.header_table_size > 16 * 1024
            || self.max_send_buffer_size > 1024 * 1024
            || self.max_pending_accept_reset_streams > 1_024
            || self.max_local_error_reset_streams > 1_024
            || self.control_frames_per_second > 1_000
            || self.control_frames_burst > 2_000
            || self.process_control_frames_per_second > 100_000
            || self.process_control_frames_burst > 200_000
            || self.concurrency_limit_per_connection > self.max_concurrent_streams as usize
            || self.concurrency_limit_per_connection > self.max_inflight_rpcs
            || self.message_limits.max_decoding_bytes > MAX_GRPC_MESSAGE_BYTES
            || self.message_limits.max_encoding_bytes > MAX_GRPC_MESSAGE_BYTES
            || self.method_policies.len() > MAX_GRPC_METHODS
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        for (method, policy) in &self.method_policies {
            if split_owned_method_name(method).is_none()
                || policy.max_inflight_rpcs == Some(0)
                || policy
                    .max_inflight_rpcs
                    .is_some_and(|limit| limit > self.max_inflight_rpcs)
                || policy.requests_per_second == Some(0)
                || policy
                    .requests_per_second
                    .is_some_and(|rate| rate > 1_000_000)
                || policy.burst == Some(0)
                || policy.burst.is_some_and(|burst| burst > 2_000_000)
                || policy.requests_per_second.is_some() != policy.burst.is_some()
                || matches!((policy.requests_per_second, policy.burst), (Some(rate), Some(burst)) if burst < rate)
            {
                return Err(GrpcServerError::InvalidConfiguration);
            }
        }
        let required_connection_window = u64::from(self.initial_stream_window_size)
            .checked_mul(u64::from(self.max_concurrent_streams.min(16)))
            .ok_or(GrpcServerError::InvalidConfiguration)?;
        let accounting_limit = self
            .managed_memory_budget_bytes
            .checked_mul(MEMORY_UTILIZATION_NUMERATOR)
            .map(|bytes| bytes / MEMORY_UTILIZATION_DENOMINATOR)
            .ok_or(GrpcServerError::InvalidConfiguration)?;
        if u64::from(self.initial_connection_window_size) < required_connection_window
            || self.managed_memory_bytes()? > accounting_limit
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        Ok(())
    }

    /// 业务作用：计算连接、transport stream、RPC 与消息 permit 的保守受管内存权重。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部乘加可表示时返回预算字节数；溢出时返回配置错误并阻止网络 I/O。
    pub fn managed_memory_bytes(&self) -> Result<usize, GrpcServerError> {
        let per_connection = CONNECTION_FIXED_BYTES
            .checked_add(self.initial_connection_window_size as usize)
            .and_then(|value| value.checked_add(RESET_STATE_BYTES))
            .and_then(|value| value.checked_add(self.header_table_size as usize))
            .and_then(|value| value.checked_add(self.max_send_buffer_size))
            .and_then(|value| {
                (self.max_concurrent_streams as usize)
                    .checked_mul(TRANSPORT_STREAM_STATE_BYTES)
                    .and_then(|streams| value.checked_add(streams))
            })
            .ok_or(GrpcServerError::InvalidConfiguration)?;
        let connections = self
            .max_connections
            .checked_mul(per_connection)
            .ok_or(GrpcServerError::InvalidConfiguration)?;
        let per_rpc = RPC_FIXED_BYTES
            .checked_add(self.initial_stream_window_size as usize)
            .and_then(|value| value.checked_add(self.max_header_list_size as usize))
            .ok_or(GrpcServerError::InvalidConfiguration)?;
        connections
            .checked_add(
                self.max_inflight_rpcs
                    .checked_mul(per_rpc)
                    .ok_or(GrpcServerError::InvalidConfiguration)?,
            )
            .and_then(|value| value.checked_add(self.max_inflight_message_bytes))
            .ok_or(GrpcServerError::InvalidConfiguration)
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
    /// generated service 与运行时使用不同 codegen ABI。
    CodegenAbiMismatch,
    /// service name、method 目录或 descriptor 不满足受管合同。
    InvalidServiceMetadata,
    /// 多个 descriptor 对同一 proto 文件或 service symbol 给出了不同定义。
    DescriptorConflict,
    /// 同一 protobuf service 已经登记。
    DuplicateService,
    /// service 或 method 数量超过单进程容量。
    ServiceCapacityExceeded,
    /// 未登记业务 service 且没有显式允许 health-only 模式。
    MissingService,
    /// descriptor 无法构造 reflection service。
    ReflectionBuildFailed,
    /// TLS certificate、private key 或 client CA 无法形成受管 acceptor。
    TlsConfiguration,
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

/// health 状态与 listener owner 同生命周期，确保 drain 前先从服务发现视角摘流。
#[derive(Clone)]
struct ManagedHealth {
    reporter: tonic_health::server::HealthReporter,
    service_names: Vec<&'static str>,
}

impl ManagedHealth {
    /// 业务作用：把整体状态和全部业务 service 切换到不可接新流量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；所有 watch 订阅者按 service name 收到 `NotServing`。
    async fn mark_not_serving(&self) {
        self.reporter
            .set_service_status("", tonic_health::ServingStatus::NotServing)
            .await;
        for name in &self.service_names {
            self.reporter
                .set_service_status(name, tonic_health::ServingStatus::NotServing)
                .await;
        }
    }

    /// 业务作用：只在 listener 已绑定并由 serve task 持有后发布整体和业务 service 可用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；所有登记状态按 service name 切换到 `Serving`。
    async fn mark_serving(&self) {
        for name in &self.service_names {
            self.reporter
                .set_service_status(name, tonic_health::ServingStatus::Serving)
                .await;
        }
        self.reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;
    }
}

/// 独立 listener 与 Application component 共用的稳定装配计划。
pub struct ServerPlan {
    config: GrpcServerConfig,
    registry: ManagedServiceRegistry,
    reflection_enabled: bool,
    health_only: bool,
    tls_identity: Option<GrpcTlsIdentity>,
}

/// 标准 health 方法目录，使自动 service 与业务 service 使用同一公共请求边界。
const HEALTH_METHODS: [GrpcMethodDescriptor; 2] = [
    GrpcMethodDescriptor {
        full_name: "/grpc.health.v1.Health/Check",
        rpc_type: GrpcMethodType::Unary,
    },
    GrpcMethodDescriptor {
        full_name: "/grpc.health.v1.Health/Watch",
        rpc_type: GrpcMethodType::ServerStreaming,
    },
];

/// 标准 reflection v1 方法目录，使 descriptor 查询也受 streaming 与进程预算约束。
const REFLECTION_METHODS: [GrpcMethodDescriptor; 1] = [GrpcMethodDescriptor {
    full_name: "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
    rpc_type: GrpcMethodType::BidirectionalStreaming,
}];

/// 业务作用：把 generated descriptor 中的 service 定义收敛到 sealed registry allowlist，同时保留消息与依赖。
///
/// 参数说明：
/// - `encoded`: codegen 产出的完整 descriptor set。
/// - `allowed_services`: 当前 listener 实际登记的编译期完整 service name 集合。
///
/// 返回：只含 allowlist service 定义的 owned descriptor set；输入无法解码时拒绝开放 reflection。
fn reflection_descriptor_set(
    encoded: &'static [u8],
    allowed_services: &BTreeSet<&'static str>,
) -> Result<prost_types::FileDescriptorSet, GrpcServerError> {
    let mut descriptor = prost_types::FileDescriptorSet::decode(encoded)
        .map_err(|_| GrpcServerError::InvalidServiceMetadata)?;
    for file in &mut descriptor.file {
        let package = file.package.as_deref().unwrap_or_default();
        file.service.retain(|service| {
            let Some(name) = service.name.as_deref() else {
                return false;
            };
            let full_name = if package.is_empty() {
                name.to_owned()
            } else {
                format!("{package}.{name}")
            };
            allowed_services.contains(full_name.as_str())
        });
    }
    Ok(descriptor)
}

impl Default for ServerPlan {
    /// 业务作用：创建自动 health、默认关闭 reflection 且要求至少一个业务 service 的装配计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：尚未登记 service、绑定端口或创建后台任务的计划。
    fn default() -> Self {
        Self {
            config: GrpcServerConfig::default(),
            registry: ManagedServiceRegistry::new(),
            reflection_enabled: false,
            health_only: false,
            tls_identity: None,
        }
    }
}

impl ServerPlan {
    /// 业务作用：创建使用受管默认边界的独立 gRPC server 计划。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：自动提供 health 且要求业务 service 的空计划。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：创建使用调用方 transport 边界的装配计划，配置在网络 I/O 前统一校验。
    ///
    /// 参数说明：
    /// - `config`: listener、HTTP/2、消息和 drain 的完整受管配置。
    ///
    /// 返回：持有配置但尚未产生网络副作用的计划。
    pub fn with_config(config: GrpcServerConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// 业务作用：接收 UserHook 阶段已经冻结的 registry，维持 service 只能消费一次的所有权。
    ///
    /// 参数说明：
    /// - `registry`: 已完成 ABI、重复身份与容量校验的 service 集合。
    ///
    /// 返回：替换空 registry 后的线性装配计划。
    pub fn with_registry(mut self, registry: ManagedServiceRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// 业务作用：登记 generated server，使独立 listener 也走与 Application 相同的门禁和装配路径。
    ///
    /// 参数说明：
    /// - `service`: 统一 codegen 生成且尚未被 Router 消费的 server。
    ///
    /// 返回：登记成功时返回计划继续链式配置；合同不匹配时保留明确错误分类。
    pub fn add_service<S>(mut self, service: S) -> Result<Self, GrpcServerError>
    where
        S: ManagedGrpcService,
    {
        self.registry.register(service)?;
        Ok(self)
    }

    /// 业务作用：显式控制是否开放 server reflection；默认关闭以避免暴露协议目录。
    ///
    /// 参数说明：
    /// - `enabled`: 是否把 sealed registry allowlist 内的受管 descriptor 交给 reflection v1 service。
    ///
    /// 返回：更新 reflection 选择后的计划；开启后业务 service full name 自动取自 generated registry。
    pub fn reflection(mut self, enabled: bool) -> Self {
        self.reflection_enabled = enabled;
        self
    }

    /// 业务作用：显式允许只启动标准 health service，供基础设施探针或分阶段部署使用。
    ///
    /// 参数说明：
    /// - `enabled`: 没有业务 service 时是否仍允许绑定 listener。
    ///
    /// 返回：更新 health-only 门禁后的计划。
    pub fn health_only(mut self, enabled: bool) -> Self {
        self.health_only = enabled;
        self
    }

    /// 业务作用：为独立 listener 提交已解析的启动期 TLS/mTLS identity，不依赖 Application secret owner。
    ///
    /// 参数说明：
    /// - `identity`: 已接管 private key 且 Debug 脱敏的 TLS identity。
    ///
    /// 返回：记录材料但尚未解析证书、绑定端口或执行握手的计划。
    pub fn tls(mut self, identity: GrpcTlsIdentity) -> Self {
        self.tls_identity = Some(identity);
        self
    }

    /// 业务作用：冻结 registry，自动装配 health/reflection/消息边界并启动唯一 listener owner。
    ///
    /// 参数说明：
    /// - `bind`: 需要预绑定的本地 socket 地址，可用端口 0 让操作系统分配临时端口。
    ///
    /// 返回：端口、路由和 health 状态都成功移交给 serve task 后返回 owner；任一门禁失败时不开放端口。
    pub async fn start(self, bind: SocketAddr) -> Result<GrpcServerHandle, GrpcServerError> {
        self.config.validate()?;
        if self.registry.is_empty() && !self.health_only {
            return Err(GrpcServerError::MissingService);
        }
        if !self.registry.is_empty() && self.health_only {
            return Err(GrpcServerError::InvalidConfiguration);
        }
        if self.health_only && self.reflection_enabled {
            return Err(GrpcServerError::InvalidConfiguration);
        }

        let services = self.registry.into_services();
        let service_names = services
            .iter()
            .map(|service| service.service_name())
            .collect::<Vec<_>>();
        let mut descriptors = BTreeSet::new();
        descriptors.extend(services.iter().map(|service| service.descriptor_set()));
        let mut rpc_methods = HEALTH_METHODS.to_vec();
        rpc_methods.extend(
            services
                .iter()
                .flat_map(|service| service.methods().iter().copied()),
        );
        if self.reflection_enabled {
            rpc_methods.extend(REFLECTION_METHODS);
        }
        let rpc_metrics = Arc::new(RpcMetrics::new(rpc_methods));
        if self
            .config
            .method_policies
            .keys()
            .any(|method| !rpc_metrics.methods.contains_key(method.as_str()))
        {
            return Err(GrpcServerError::InvalidConfiguration);
        }

        let (reporter, health_service) = tonic_health::server::health_reporter();
        let health = ManagedHealth {
            reporter,
            service_names: service_names.clone(),
        };
        // listener 尚未绑定时先发布不可服务，避免 health watch 在路由开放前观察到 Serving。
        health.mark_not_serving().await;

        let limits = self.config.message_limits;
        let policy = GrpcServicePolicy::from_config(&self.config, Arc::clone(&rpc_metrics));
        let mut routes = tonic::service::Routes::builder();
        routes.add_service(ManagedService::new(
            health_service
                .max_decoding_message_size(limits.max_decoding_bytes)
                .max_encoding_message_size(limits.max_encoding_bytes),
            policy.clone(),
            &HEALTH_METHODS,
        ));
        for service in services {
            service.add_to_routes(&mut routes, limits, policy.clone());
        }
        if self.reflection_enabled {
            // sealed registry 的 generated service name 是唯一 allowlist；业务无需再手工登记 descriptor，
            // descriptor 内未实际装配的相邻 service 会在 bind 前移除，不能通过 symbol 查询旁路暴露。
            let allowed_services = service_names.iter().copied().collect::<BTreeSet<_>>();
            let mut reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .with_service_name("grpc.health.v1.Health")
                .with_service_name("grpc.reflection.v1.ServerReflection");
            for service_name in &service_names {
                reflection = reflection.with_service_name(*service_name);
            }
            for descriptor in descriptors {
                reflection = reflection.register_file_descriptor_set(reflection_descriptor_set(
                    descriptor,
                    &allowed_services,
                )?);
            }
            let reflection = reflection
                .build_v1()
                .map_err(|_| GrpcServerError::ReflectionBuildFailed)?
                .max_decoding_message_size(limits.max_decoding_bytes)
                .max_encoding_message_size(limits.max_encoding_bytes);
            routes.add_service(ManagedService::new(
                reflection,
                policy.clone(),
                &REFLECTION_METHODS,
            ));
        }

        GrpcServerHandle::start_managed(
            routes.routes(),
            bind,
            self.config,
            self.tls_identity,
            Some(health),
            rpc_metrics,
        )
        .await
    }
}

/// server handle 共享的地址、状态、取消令牌与唯一 join slot。
struct Inner {
    local_addr: SocketAddr,
    state: Arc<AtomicU8>,
    metrics: Arc<StdMutex<GrpcServerMetrics>>,
    rpc_metrics: Arc<RpcMetrics>,
    shutdown: CancellationToken,
    join: Mutex<Option<JoinHandle<Result<(), GrpcServerError>>>>,
    drain_timeout: Duration,
    health: Option<ManagedHealth>,
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
    rpc_metrics: Arc<RpcMetrics>,
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

    /// 业务作用：读取由 sealed descriptor 固定键空间的方法级 RPC 会计快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按完整方法路径稳定排序的 active、started、rejected 与完成结局计数；不会产生未知方法标签。
    pub fn rpc_snapshot(&self) -> Vec<GrpcRpcMethodSnapshot> {
        self.rpc_metrics.snapshot()
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

const HTTP2_CLIENT_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HTTP2_FRAME_HEADER_BYTES: usize = 9;
const HTTP2_FRAME_RST_STREAM: u8 = 0x3;
const HTTP2_FRAME_SETTINGS: u8 = 0x4;
const HTTP2_FRAME_PING: u8 = 0x6;

/// 入站 HTTP/2 frame 的有界解析状态，只识别建立边界与控制帧类型，不解码 HPACK 或业务正文。
struct Http2InboundGuard {
    preface_read: usize,
    frame_header: [u8; HTTP2_FRAME_HEADER_BYTES],
    frame_header_read: usize,
    frame_payload_remaining: usize,
    current_frame_type: u8,
    first_frame: bool,
    handshake_complete: bool,
    handshake_deadline: Pin<Box<tokio::time::Sleep>>,
    activity: Arc<ConnectionActivity>,
    max_frame_size: u32,
    control_frames_per_second: u32,
    control_frames_burst: u32,
    connection_control_bucket: TokenBucket,
    process_control_frames_per_second: u32,
    process_control_frames_burst: u32,
    process_control_bucket: Arc<StdMutex<TokenBucket>>,
}

impl Http2InboundGuard {
    /// 业务作用：从 accept 时刻建立握手 deadline 和两级控制帧准入状态。
    ///
    /// 参数说明：
    /// - `config`: listener 已冻结并由全部连接共享的协议运行配置。
    /// - `accepted_at`: socket 进入 listener 所有权的单调时刻。
    /// - `activity`: connection driver 与请求准入共用的协议建立、首请求和活动 RPC 状态。
    ///
    /// 返回：尚未读入 client preface 的解析状态。
    fn new(
        config: &ConnectionRuntimeConfig,
        accepted_at: tokio::time::Instant,
        activity: Arc<ConnectionActivity>,
    ) -> Self {
        Self {
            preface_read: 0,
            frame_header: [0; HTTP2_FRAME_HEADER_BYTES],
            frame_header_read: 0,
            frame_payload_remaining: 0,
            current_frame_type: 0,
            first_frame: true,
            handshake_complete: false,
            handshake_deadline: Box::pin(tokio::time::sleep_until(
                accepted_at + config.handshake_timeout,
            )),
            activity,
            max_frame_size: config.max_frame_size,
            control_frames_per_second: config.control_frames_per_second,
            control_frames_burst: config.control_frames_burst,
            connection_control_bucket: TokenBucket::full(config.control_frames_burst),
            process_control_frames_per_second: config.process_control_frames_per_second,
            process_control_frames_burst: config.process_control_frames_burst,
            process_control_bucket: Arc::clone(&config.process_control_bucket),
        }
    }

    /// 业务作用：在继续读取网络数据前强制执行 HTTP/2 建立预算，防止零字节连接长期占槽。
    ///
    /// 参数说明：
    /// - `context`: 连接读取任务的异步唤醒上下文。
    ///
    /// 返回：HTTP/2 尚在建立预算内时成功；preface/SETTINGS 超时时返回连接级错误。首请求预算由
    /// connection driver 在 HTTP/2 建立后以 GOAWAY 路径执行。
    fn poll_deadlines(&mut self, context: &mut Context<'_>) -> io::Result<()> {
        if !self.handshake_complete && self.handshake_deadline.as_mut().poll(context).is_ready() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "gRPC HTTP/2 handshake timed out",
            ));
        }
        Ok(())
    }

    /// 业务作用：解析本次 socket 读取新增的完整或分段 frame，并执行建立期和控制帧门禁。
    ///
    /// 参数说明：
    /// - `bytes`: 本次读取新增的连续字节，不包含此前已经解析的内容。
    ///
    /// 返回：协议前缀、frame 长度和控制帧速率都合法时成功；否则关闭该连接。
    fn ingest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut offset = 0_usize;
        while offset < bytes.len() {
            if self.preface_read < HTTP2_CLIENT_PREFACE.len() {
                let remaining = HTTP2_CLIENT_PREFACE.len() - self.preface_read;
                let take = remaining.min(bytes.len() - offset);
                if bytes[offset..offset + take]
                    != HTTP2_CLIENT_PREFACE[self.preface_read..self.preface_read + take]
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid HTTP/2 client preface",
                    ));
                }
                self.preface_read += take;
                offset += take;
                continue;
            }

            if self.frame_payload_remaining > 0 {
                let take = self.frame_payload_remaining.min(bytes.len() - offset);
                self.frame_payload_remaining -= take;
                offset += take;
                if self.frame_payload_remaining == 0 {
                    self.finish_frame();
                }
                continue;
            }

            let take =
                (HTTP2_FRAME_HEADER_BYTES - self.frame_header_read).min(bytes.len() - offset);
            self.frame_header[self.frame_header_read..self.frame_header_read + take]
                .copy_from_slice(&bytes[offset..offset + take]);
            self.frame_header_read += take;
            offset += take;
            if self.frame_header_read < HTTP2_FRAME_HEADER_BYTES {
                continue;
            }

            let payload_len = (usize::from(self.frame_header[0]) << 16)
                | (usize::from(self.frame_header[1]) << 8)
                | usize::from(self.frame_header[2]);
            let frame_type = self.frame_header[3];
            let flags = self.frame_header[4];
            let stream_id = u32::from_be_bytes([
                self.frame_header[5] & 0x7f,
                self.frame_header[6],
                self.frame_header[7],
                self.frame_header[8],
            ]);
            if payload_len > self.max_frame_size as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP/2 frame exceeds the configured boundary",
                ));
            }
            if self.first_frame
                && (frame_type != HTTP2_FRAME_SETTINGS || stream_id != 0 || flags & 0x1 != 0)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "first HTTP/2 frame is not client SETTINGS",
                ));
            }
            if matches!(
                frame_type,
                HTTP2_FRAME_RST_STREAM | HTTP2_FRAME_SETTINGS | HTTP2_FRAME_PING
            ) {
                self.admit_control_frame()?;
            }
            self.current_frame_type = frame_type;
            self.frame_payload_remaining = payload_len;
            self.frame_header_read = 0;
            if payload_len == 0 {
                self.finish_frame();
            }
        }
        Ok(())
    }

    /// 业务作用：在两级 token bucket 中为一个控制帧执行无等待准入，阻止多连接绕过单连接速率。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：两级容量都可立即取得时成功；任一级耗尽时返回连接级拒绝。
    fn admit_control_frame(&mut self) -> io::Result<()> {
        let now = Instant::now();
        if !self.connection_control_bucket.try_take(
            self.control_frames_per_second,
            self.control_frames_burst,
            now,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "HTTP/2 connection control-frame rate exceeded",
            ));
        }
        let mut process = self
            .process_control_bucket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !process.try_take(
            self.process_control_frames_per_second,
            self.process_control_frames_burst,
            now,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "HTTP/2 process control-frame rate exceeded",
            ));
        }
        Ok(())
    }

    /// 业务作用：在完整消费一个 frame 后推进握手与首个完整 header block 的时间状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；只迁移一次握手状态，其余 frame 仅重置解析头。
    fn finish_frame(&mut self) {
        if self.first_frame {
            self.first_frame = false;
            self.handshake_complete = true;
            self.activity.mark_http2_established();
        }
        self.current_frame_type = 0;
    }
}

/// 单条 HTTP/2 连接的首请求与活动 RPC 状态；driver 与 service wrapper 共用同一事实。
struct ConnectionActivity {
    http2_established: AtomicBool,
    first_request_seen: AtomicBool,
    active_rpcs: AtomicUsize,
    changed: Notify,
}

impl ConnectionActivity {
    /// 业务作用：创建尚未收到业务 HEADERS、没有活动 RPC 的连接会计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由每连接 service 和 driver 共享的空状态。
    fn new() -> Self {
        Self {
            http2_established: AtomicBool::new(false),
            first_request_seen: AtomicBool::new(false),
            active_rpcs: AtomicUsize::new(0),
            changed: Notify::new(),
        }
    }

    /// 业务作用：在 client preface 与首个 SETTINGS 完整通过后发布 HTTP/2 已建立事实，启动首请求预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；重复发布保持幂等并唤醒 connection driver。
    fn mark_http2_established(&self) {
        self.http2_established.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// 业务作用：从 HTTP/2 建立时刻等待首个业务请求，超时后让 driver 经 GOAWAY 驱逐连接。
    ///
    /// 参数说明：
    /// - `timeout`: HTTP/2 建立后允许客户端发送首个业务 HEADERS 的最长时长。
    ///
    /// 返回：预算耗尽且仍没有请求时完成；首请求到达后永久等待，不误触后续 idle 路径。
    async fn wait_for_first_request_timeout(&self, timeout: Duration) {
        loop {
            let changed = self.changed.notified();
            if self.http2_established.load(Ordering::Acquire) {
                break;
            }
            changed.await;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.first_request_seen.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            let changed = self.changed.notified();
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    if !self.first_request_seen.load(Ordering::Acquire) {
                        return;
                    }
                }
                _ = changed => {}
            }
        }
    }

    /// 业务作用：在请求进入每连接准入后登记活动 RPC，并唤醒首请求/idle driver。
    ///
    /// 参数说明：
    /// - `state`: 当前连接共享的活动状态。
    ///
    /// 返回：持有到最终 response body 结束的会计凭证。
    fn begin(state: &Arc<Self>) -> ConnectionRpcGuard {
        state.first_request_seen.store(true, Ordering::Release);
        state.active_rpcs.fetch_add(1, Ordering::AcqRel);
        state.changed.notify_waiters();
        ConnectionRpcGuard {
            state: Arc::clone(state),
        }
    }

    /// 业务作用：等待首个 RPC 已经完整结束且连接持续空闲达到配置预算。
    ///
    /// 参数说明：
    /// - `idle_timeout`: 没有活动 RPC 时允许保留连接的时长。
    ///
    /// 返回：空闲预算到期时完成；任何新 RPC 都重新开始完整预算。
    async fn wait_until_idle(&self, idle_timeout: Duration) {
        loop {
            let changed = self.changed.notified();
            if !self.first_request_seen.load(Ordering::Acquire)
                || self.active_rpcs.load(Ordering::Acquire) != 0
            {
                changed.await;
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(idle_timeout) => {
                    if self.active_rpcs.load(Ordering::Acquire) == 0 {
                        return;
                    }
                }
                _ = changed => {}
            }
        }
    }
}

/// 活动 RPC 的每连接会计凭证；所有返回、取消与连接丢失都通过析构归零。
struct ConnectionRpcGuard {
    state: Arc<ConnectionActivity>,
}

impl Drop for ConnectionRpcGuard {
    /// 业务作用：在 RPC response body 终结或被丢弃时撤销活动计数并重新武装 idle 判断。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；每个凭证只减少自己建立的一次活动计数。
    fn drop(&mut self) {
        self.state.active_rpcs.fetch_sub(1, Ordering::AcqRel);
        self.state.changed.notify_waiters();
    }
}

/// response body 的每连接生命周期包装；并发 permit 与活动凭证保留到最终 trailers 写完或断连。
struct ConnectionBody {
    inner: tonic::body::Body,
    _connection_permit: Option<OwnedSemaphorePermit>,
    _activity: Option<ConnectionRpcGuard>,
}

impl ConnectionBody {
    /// 业务作用：把标准 gRPC response body 与每连接并发/idle 会计绑定为同一生命周期。
    ///
    /// 参数说明：
    /// - `inner`: 需要交给 hyper 写出的 tonic body。
    /// - `connection_permit`: 本 RPC 的每连接并发 permit；协议拒绝响应不持有。
    /// - `activity`: 本 RPC 的活动会计凭证；协议拒绝响应不登记为已准入 RPC。
    ///
    /// 返回：body 结束或被丢弃时自动释放两个凭证的包装。
    fn new(
        inner: tonic::body::Body,
        connection_permit: Option<OwnedSemaphorePermit>,
        activity: Option<ConnectionRpcGuard>,
    ) -> Self {
        Self {
            inner,
            _connection_permit: connection_permit,
            _activity: activity,
        }
    }
}

impl http_body::Body for ConnectionBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;

    /// 业务作用：把 gRPC DATA/trailers 原样写给 hyper，并把最终帧作为 permit 释放边界。
    ///
    /// 参数说明：
    /// - `context`: hyper connection driver 的异步唤醒上下文。
    ///
    /// 返回：底层 tonic body 的下一帧、错误或结束状态。
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(context)
    }

    /// 业务作用：保留底层 response 的剩余长度提示，不把容量上限误报为实际长度。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：tonic body 当前的 size hint。
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }

    /// 业务作用：让 hyper 在底层已经终结时立即观察结束，同时仍由本包装持有会计凭证到析构。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层 tonic body 是否不会再产生 frame。
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

/// 稳定 ServerPlan 的逐连接 HTTP service；在进入业务路由前执行每连接无等待并发准入。
#[derive(Clone)]
struct ManagedConnectionService {
    routes: tonic::service::Routes,
    connection_slots: Arc<Semaphore>,
    activity: Arc<ConnectionActivity>,
    rpc_metrics: Arc<RpcMetrics>,
    peer_addr: SocketAddr,
    peer_identity: Option<PeerIdentity>,
}

impl hyper::service::Service<hyper::Request<hyper::body::Incoming>> for ManagedConnectionService {
    type Response = hyper::Response<ConnectionBody>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    /// 业务作用：按每连接容量受理 HTTP/2 stream，并把活动事实保留到最终 gRPC body 终结。
    ///
    /// 参数说明：
    /// - `request`: hyper 已验证协议与 header 边界的 HTTP/2 请求。
    ///
    /// 返回：容量不足时立即返回 `ResourceExhausted`；成功准入时执行唯一 Routes 并绑定生命周期会计。
    fn call(&self, request: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let permit = match Arc::clone(&self.connection_slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.rpc_metrics.reject_path(
                    request.uri().path(),
                    GrpcRpcRejectionReason::ConnectionConcurrency,
                );
                return Box::pin(async {
                    let response =
                        tonic::Status::resource_exhausted("gRPC connection concurrency exhausted")
                            .into_http()
                            .map(|body| ConnectionBody::new(body, None, None));
                    Ok(response)
                });
            }
        };
        let activity = ConnectionActivity::begin(&self.activity);
        let mut request = request.map(tonic::body::Body::new);
        request.extensions_mut().insert(self.peer_addr);
        if let Some(identity) = &self.peer_identity {
            request.extensions_mut().insert(identity.clone());
        }
        let mut routes = self.routes.clone();
        Box::pin(async move {
            std::future::poll_fn(|context| {
                <tonic::service::Routes as tonic::codegen::Service<
                    tonic::codegen::http::Request<tonic::body::Body>,
                >>::poll_ready(&mut routes, context)
            })
            .await?;
            let response = tonic::codegen::Service::call(&mut routes, request)
                .await?
                .map(|body| ConnectionBody::new(body, Some(permit), Some(activity)));
            Ok(response)
        })
    }
}

/// 明文或已完成 server TLS 握手的统一 HTTP/2 I/O。
enum ConnectionIo {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::TlsStream<tokio::net::TcpStream>>),
}

/// accept 后立即建立的连接所有权；TLS/HTTP2 建立失败也必须归还 active 与 permit。
struct ConnectionOwnership {
    peer_addr: SocketAddr,
    peer_identity: Option<PeerIdentity>,
    _permit: OwnedSemaphorePermit,
    metrics: Arc<StdMutex<GrpcServerMetrics>>,
}

impl Drop for ConnectionOwnership {
    /// 业务作用：在 TLS、HTTP/2 或业务连接任一阶段结束时同步归还在途连接会计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；累计受理保持单调，只有 active_connections 减少一次。
    fn drop(&mut self) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.active_connections = metrics.active_connections.saturating_sub(1);
    }
}

/// 持有连接 permit 的 HTTP/2 I/O；只有连接真正关闭后才释放进程级容量。
struct LimitedConnection {
    stream: ConnectionIo,
    ownership: ConnectionOwnership,
    activity: Arc<ConnectionActivity>,
    protocol: Http2InboundGuard,
}

impl LimitedConnection {
    /// 业务作用：把已接受 socket 与连接容量绑定成同生命周期的 tonic I/O。
    ///
    /// 参数说明：
    /// - `stream`: listener 已接受的 TCP 连接。
    /// - `permit`: 该连接独占的进程级容量凭证。
    /// - `metrics`: 观察面使用的受理与在途连接状态。
    /// - `runtime`: TCP、HTTP/2 建立与控制帧的最终运行边界。
    ///
    /// 返回：直到 I/O 析构才释放凭证的连接包装。
    async fn new(
        stream: tokio::net::TcpStream,
        permit: OwnedSemaphorePermit,
        metrics: Arc<StdMutex<GrpcServerMetrics>>,
        runtime: &ConnectionRuntimeConfig,
    ) -> io::Result<Self> {
        apply_tcp_socket_config(&stream, runtime.tcp)?;
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
        let mut ownership = ConnectionOwnership {
            peer_addr,
            peer_identity: None,
            _permit: permit,
            metrics,
        };
        let accepted_at = tokio::time::Instant::now();
        let (stream, peer_identity) = if let Some(acceptor) = &runtime.tls_acceptor {
            let tls = tokio::time::timeout_at(
                accepted_at + runtime.handshake_timeout,
                acceptor.accept(stream),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "gRPC TLS handshake timed out"))?
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "gRPC TLS handshake failed"))?;
            let peer_identity = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .map(PeerIdentity::from_certificate);
            (ConnectionIo::Tls(Box::new(tls.into())), peer_identity)
        } else {
            (ConnectionIo::Plain(stream), None)
        };
        ownership.peer_identity = peer_identity;
        let activity = Arc::new(ConnectionActivity::new());
        Ok(Self {
            stream,
            ownership,
            protocol: Http2InboundGuard::new(runtime, accepted_at, Arc::clone(&activity)),
            activity,
        })
    }
}

/// 业务作用：把最终 TCP 配置应用到一条已 accept 的 socket，避免自定义 incoming 绕过 transport 合同。
///
/// 参数说明：
/// - `stream`: 尚未交给 tonic 的已受理连接。
/// - `config`: 从最终 server 配置提取的 keepalive 与低延迟参数。
///
/// 返回：操作系统接受全部当前平台支持的参数时成功；任一必要参数失败时拒绝把连接交给协议层。
fn apply_tcp_socket_config(
    stream: &tokio::net::TcpStream,
    config: TcpSocketConfig,
) -> io::Result<()> {
    stream.set_nodelay(config.nodelay)?;
    let socket = socket2::SockRef::from(stream);
    socket.set_keepalive(true)?;
    let keepalive = socket2::TcpKeepalive::new().with_time(config.keepalive);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        target_os = "nuttx",
        all(target_os = "wasi", not(target_env = "p1")),
    ))]
    let keepalive = keepalive.with_interval(config.keepalive_interval);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        target_os = "nuttx",
        all(target_os = "wasi", not(target_env = "p1")),
    ))]
    let keepalive = keepalive.with_retries(config.keepalive_retries);
    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        target_os = "nuttx",
        all(target_os = "wasi", not(target_env = "p1")),
    )))]
    let _ = (config.keepalive_interval, config.keepalive_retries);
    socket.set_tcp_keepalive(&keepalive)
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
        self.protocol.poll_deadlines(cx)?;
        let before = buf.filled().len();
        let poll = match &mut self.stream {
            ConnectionIo::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            ConnectionIo::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        };
        match poll {
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                if after > before {
                    self.protocol.ingest(&buf.filled()[before..after])?;
                }
                self.protocol.poll_deadlines(cx)?;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
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
        match &mut self.stream {
            ConnectionIo::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            ConnectionIo::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    /// 业务作用：把缓冲数据刷新到持有容量凭证的底层 socket。
    ///
    /// 参数说明：
    /// - `cx`: 异步任务唤醒上下文。
    ///
    /// 返回：底层 socket 的刷新结果。
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match &mut self.stream {
            ConnectionIo::Plain(stream) => Pin::new(stream).poll_flush(cx),
            ConnectionIo::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
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
        match &mut self.stream {
            ConnectionIo::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            ConnectionIo::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
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
        match &mut self.stream {
            ConnectionIo::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            ConnectionIo::Tls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    /// 业务作用：声明底层 socket 支持 vectored write，供 tonic 选择等价高效路径。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：底层 TCP I/O 的 vectored write 能力。
    fn is_write_vectored(&self) -> bool {
        match &self.stream {
            ConnectionIo::Plain(stream) => stream.is_write_vectored(),
            ConnectionIo::Tls(stream) => stream.is_write_vectored(),
        }
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
        self.ownership.peer_addr
    }
}

/// 业务作用：为主动连接轮转派生固定在 ±10% 内的单连接时刻，避免同批 channel 同时迁移。
///
/// 参数说明：
/// - `age`: 配置的基础连接年龄。
/// - `peer`: 已由 socket 确认的对端地址，只参与本连接本地散列，不进入日志或指标。
///
/// 返回：不低于基础值 90%、不高于 110% 的轮转时长。
fn jittered_connection_age(age: Duration, peer: SocketAddr) -> Duration {
    let base_millis = age.as_millis().min(u128::from(u64::MAX)) as u64;
    let span = base_millis / 10;
    if span == 0 {
        return age;
    }
    let mut hash = u64::from(peer.port());
    for byte in peer.ip().to_string().bytes() {
        hash = hash
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(byte));
    }
    let width = span.saturating_mul(2).saturating_add(1);
    Duration::from_millis(
        base_millis
            .saturating_sub(span)
            .saturating_add(hash % width),
    )
}

/// 业务作用：用受管 hyper HTTP/2 driver 执行一条连接，并在停机、首请求、空闲或年龄边界触发两阶段 GOAWAY。
///
/// 参数说明：
/// - `connection`: 已应用 TCP、握手和控制帧门禁且持有容量 permit 的 I/O。
/// - `routes`: Prepare 封口并自动包含 health/reflection 的唯一路由表。
/// - `config`: 已校验且在该连接生命周期内冻结的 transport 配置。
/// - `shutdown`: listener owner 的全局停机信号。
/// - `rpc_metrics`: sealed 方法目录与拒绝会计；连接层容量拒绝也必须进入同一固定键空间。
///
/// 返回：连接正常结束或受管驱逐完成时成功；hyper driver 无法建立/维持协议时返回连接级失败。
async fn drive_managed_connection(
    connection: LimitedConnection,
    routes: tonic::service::Routes,
    config: GrpcServerConfig,
    shutdown: CancellationToken,
    rpc_metrics: Arc<RpcMetrics>,
) -> Result<(), GrpcServerError> {
    let peer_addr = connection.ownership.peer_addr;
    let activity = Arc::clone(&connection.activity);
    let service = ManagedConnectionService {
        routes,
        connection_slots: Arc::new(Semaphore::new(config.concurrency_limit_per_connection)),
        activity: Arc::clone(&activity),
        rpc_metrics,
        peer_addr,
        peer_identity: connection.ownership.peer_identity.clone(),
    };
    let mut builder =
        hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder
        .timer(hyper_util::rt::TokioTimer::new())
        .adaptive_window(false)
        .initial_stream_window_size(Some(config.initial_stream_window_size))
        .initial_connection_window_size(Some(config.initial_connection_window_size))
        .max_concurrent_streams(Some(config.max_concurrent_streams))
        .keep_alive_interval(Some(config.keepalive_interval))
        .keep_alive_timeout(config.keepalive_timeout)
        .max_pending_accept_reset_streams(Some(config.max_pending_accept_reset_streams))
        .max_local_error_reset_streams(Some(config.max_local_error_reset_streams))
        .max_frame_size(Some(config.max_frame_size))
        .max_header_list_size(config.max_header_list_size)
        .header_table_size(Some(config.header_table_size))
        .max_send_buf_size(config.max_send_buffer_size);

    let connection = builder.serve_connection(hyper_util::rt::TokioIo::new(connection), service);
    tokio::pin!(connection);
    let idle = activity.wait_until_idle(config.idle_connection_timeout);
    tokio::pin!(idle);
    let first_request = activity.wait_for_first_request_timeout(config.first_request_timeout);
    tokio::pin!(first_request);
    let max_age = async {
        match config.max_connection_age {
            Some(age) => tokio::time::sleep(jittered_connection_age(age, peer_addr)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(max_age);

    let shutdown_drain = tokio::select! {
        result = &mut connection => {
            return result.map_err(|_| GrpcServerError::ServeFailed);
        }
        _ = shutdown.cancelled() => true,
        _ = &mut first_request => false,
        _ = &mut idle => false,
        _ = &mut max_age => false,
    };

    // HTTP/2 已建立后不能直接关闭 socket。hyper/h2 在这里先发初始 GOAWAY 与 shutdown PING，
    // 再按已处理 stream high-water mark 发最终 GOAWAY。停机必须把整个 listener drain 预算交给
    // 已接纳 RPC；只有首请求、空闲和连接年龄驱逐使用较短的逐连接 grace。
    connection.as_mut().graceful_shutdown();
    let grace = if shutdown_drain {
        config.drain_timeout
    } else {
        config.connection_eviction_grace
    };
    match tokio::time::timeout(grace, &mut connection).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(GrpcServerError::ServeFailed),
        Err(_) => Ok(()),
    }
}

/// 唯一 shutdown owner 的 gRPC server handle。
pub struct GrpcServerHandle {
    inner: Arc<Inner>,
}

impl GrpcServerHandle {
    /// 业务作用：预绑定稳定 ServerPlan listener，并由框架逐连接持有 HTTP/2 driver 与 GOAWAY 权限。
    ///
    /// 参数说明：
    /// - `routes`: 已自动装配业务 service、health 与可选 reflection 的封口路由。
    /// - `bind`: 需要绑定的本地 socket 地址。
    /// - `config`: 已在网络 I/O 前复验的完整 server 配置。
    /// - `tls_identity`: 可选的 server TLS/mTLS 启动期材料。
    /// - `health`: 与 listener 同生命周期的标准 health 状态 owner。
    /// - `rpc_metrics`: 由封口 descriptor 预创建且与 observer 共用的方法级会计目录。
    ///
    /// 返回：listener、accept 循环和逐连接 driver 全部建立后返回唯一停机 owner；失败时不遗留任务。
    async fn start_managed(
        routes: tonic::service::Routes,
        bind: SocketAddr,
        config: GrpcServerConfig,
        tls_identity: Option<GrpcTlsIdentity>,
        health: Option<ManagedHealth>,
        rpc_metrics: Arc<RpcMetrics>,
    ) -> Result<Self, GrpcServerError> {
        config.validate()?;
        let mut transport = ConnectionRuntimeConfig::from(&config);
        transport.tls_acceptor = tls_identity.as_ref().map(build_tls_acceptor).transpose()?;
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
        let task_rpc_metrics = Arc::clone(&rpc_metrics);
        let connection_slots = Arc::new(Semaphore::new(config.max_connections));
        let drain_timeout = config.drain_timeout;
        let join = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut accept_backoff = ACCEPT_RETRY_INITIAL_BACKOFF;
            let serve_result = loop {
                // permit 必须先于 accept 取得；停机信号可以取消容量等待，避免 listener owner 卡在空槽。
                let permit = tokio::select! {
                    _ = task_shutdown.cancelled() => break Ok(()),
                    permit = Arc::clone(&connection_slots).acquire_owned() => {
                        match permit {
                            Ok(permit) => permit,
                            Err(_) => break Ok(()),
                        }
                    }
                };
                let accepted = tokio::select! {
                    _ = task_shutdown.cancelled() => {
                        drop(permit);
                        break Ok(());
                    }
                    accepted = listener.accept() => accepted
                };
                let (stream, _) = match accepted {
                    Ok(accepted) => {
                        accept_backoff = ACCEPT_RETRY_INITIAL_BACKOFF;
                        let mut snapshot = task_metrics
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        snapshot.consecutive_accept_failures = 0;
                        snapshot.accept_failure_since = None;
                        drop(snapshot);
                        accepted
                    }
                    Err(error) => {
                        drop(permit);
                        {
                            let mut snapshot = task_metrics
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            increment_saturating(&mut snapshot.accept_failures_total);
                            increment_saturating(&mut snapshot.consecutive_accept_failures);
                            if snapshot.accept_failure_since.is_none() {
                                snapshot.accept_failure_since = Some(Instant::now());
                            }
                        }
                        tracing::warn!(
                            error = %error,
                            retry_ms = accept_backoff.as_millis() as u64,
                            "gRPC listener accept temporarily unavailable; retrying"
                        );
                        tokio::select! {
                            _ = task_shutdown.cancelled() => break Ok(()),
                            _ = tokio::time::sleep(accept_backoff) => {}
                        }
                        accept_backoff = accept_backoff
                            .saturating_mul(2)
                            .min(ACCEPT_RETRY_MAX_BACKOFF);
                        continue;
                    }
                };
                let connection = match LimitedConnection::new(
                    stream,
                    permit,
                    Arc::clone(&task_metrics),
                    &transport,
                )
                .await
                {
                    Ok(connection) => connection,
                    Err(error) => {
                        tracing::debug!(error = %error, "gRPC accepted socket rejected before HTTP/2");
                        continue;
                    }
                };
                let connection_routes = routes.clone();
                let connection_config = config.clone();
                let connection_shutdown = task_shutdown.clone();
                let connection_rpc_metrics = Arc::clone(&task_rpc_metrics);
                connections.spawn(async move {
                    drive_managed_connection(
                        connection,
                        connection_routes,
                        connection_config,
                        connection_shutdown,
                        connection_rpc_metrics,
                    )
                    .await
                });

                while let Some(result) = connections.try_join_next() {
                    if result.is_err() {
                        break;
                    }
                }
            };

            // 停止 accept 后通知全部连接走 graceful shutdown，并等待每条 driver 回收容量凭证。
            task_shutdown.cancel();
            let mut driver_failed = false;
            while let Some(result) = connections.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::debug!(error = %error, "gRPC connection ended with a protocol error");
                    }
                    Err(_) => driver_failed = true,
                }
            }
            let result = if driver_failed {
                Err(GrpcServerError::ServeFailed)
            } else {
                serve_result
            };
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
        let handle = Self {
            inner: Arc::new(Inner {
                local_addr,
                state,
                metrics,
                rpc_metrics,
                shutdown,
                join: Mutex::new(Some(join)),
                drain_timeout,
                health,
            }),
        };
        if let Some(health) = &handle.inner.health {
            // health 只在 listener 已绑定且 accept/driver owner 就位后对外发布 Serving。
            health.mark_serving().await;
        }
        Ok(handle)
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
            rpc_metrics: Arc::clone(&self.inner.rpc_metrics),
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
        if let Some(health) = &self.inner.health {
            // 停止准入前先摘除 health，给上游负载均衡留出停止派发新 RPC 的因果边界。
            health.mark_not_serving().await;
        }
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

/// 在业务模块内包含统一 codegen 生成的 Rust 类型与同一次构建产生的 descriptor set。
///
/// package 必须与 `.proto` 的入口 `package` 完整一致；生成文件只从 Cargo `OUT_DIR` 读取。宏包含
/// codegen 自动建立的规范 package 模块树，因此跨 package import 与入口类型保持同一 Rust 身份。
#[macro_export]
macro_rules! include_proto {
    ($package:literal) => {
        include!(concat!(env!("OUT_DIR"), "/", $package, ".rs"));
    };
}

/// generated code 的版本锁定依赖门面；业务代码不直接依赖这些 crate。
#[doc(hidden)]
pub mod codegen {
    pub use prost;
    pub use prost_types;
    pub use tonic;
    pub use tonic_prost;
}

/// 稳定请求、响应、状态、客户端连接和异步 trait 类型。
pub use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
pub use tonic::{async_trait, Code, Request, Response, Status, Streaming};

/// 标准 gRPC health 的探针客户端与消息类型；server 由 `ServerPlan` 自动装配。
pub mod health {
    pub use tonic_health::pb::{health_client, HealthCheckRequest, HealthCheckResponse};
    pub use tonic_health::ServingStatus;
}

/// 标准 gRPC server reflection v1 的探针客户端与消息类型；server 仅按最终配置开放。
pub mod reflection {
    pub mod v1 {
        pub use tonic_reflection::pb::v1::{
            server_reflection_client, server_reflection_request, server_reflection_response,
            ServerReflectionRequest, ServerReflectionResponse,
        };
    }
}
