//! 稳定 gRPC service registry 与 listener 的 Application 生命周期接入。
//!
//! 业务在 UserHook 只登记统一 codegen 生成的 server；组件在 Prepare 封口 registry，在全部业务
//! initializer 成功后的 Ready 阶段自动装配 health/reflection、绑定端口并发布只读观察句柄。serve 状态由关键任务监督，
//! 停机先关闭准入，再按 listener 子预算与 Application 全局剩余时间派生提前收口预算，
//! 为结果归类和后续逆序清理保留尾部时间。

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Deserialize;

use crate::readiness::{reason, DependencyState, ReadinessContributor, ReadinessPolicy};
use crate::{
    Application, ApplicationComponent, ApplicationError, ApplicationFuture, ApplicationPhase,
    ApplicationResult, ApplicationState, ComponentId, PrepareContext, ReadyContext, ShutdownAction,
    ShutdownContext, StartContext,
};

const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(5);
/// accept 失败后持续未成功接流达到该时长即摘流，不终止仍持有 listener 的 serve 任务。
const ACCEPT_FAILURE_NOT_READY_AFTER: Duration = Duration::from_secs(1);
/// 摘流后的本机 TCP 恢复探测上限；只验证 listener 能重新 accept，不发送业务协议数据。
const ACCEPT_RECOVERY_PROBE_TIMEOUT: Duration = Duration::from_millis(100);
/// gRPC drain 完成后为 Runner 收割关键任务和发布最终报告保留的最小时间。
const GRPC_SHUTDOWN_TAIL_RESERVE: Duration = Duration::from_secs(2);
/// 同时使用服务发现时，注销调用在 gRPC 停止准入之前可占用的最大时间。
const GRPC_DISCOVERY_DEREGISTER_RESERVE: Duration = Duration::from_secs(3);

#[derive(Default, Deserialize)]
#[serde(default)]
/// 业务作用：承载最终 Application 配置中的 `grpc` 根节点，避免解析其它配置段时放宽字段合同。
struct GrpcConfigRoot {
    /// 从总配置树提取的 gRPC 配置，内部字段保持严格校验。
    grpc: GrpcSettings,
}

/// 固定 `grpc` 配置根；全部容量与时间字段都必须在绑定端口前通过硬上限校验。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GrpcSettings {
    /// 要预绑定的 IP 与端口，缺省仅对 loopback 开放。
    bind: String,
    /// 对外注册的 DNS 或 IP 身份，供客户端验证服务端证书。
    authority: Option<String>,
    /// 是否明确允许在非 loopback 地址提供明文服务。
    allow_insecure_remote: bool,
    /// 监听器同时保有的连接数上限。
    max_connections: usize,
    /// 每连接可同时执行的 RPC 上限。
    concurrency_limit_per_connection: usize,
    /// 单次连接握手允许占用的毫秒数。
    connection_handshake_timeout_ms: u64,
    /// 建连后等候首个请求的毫秒上限。
    first_request_timeout_ms: u64,
    /// 空闲连接保留的毫秒上限。
    idle_connection_timeout_ms: u64,
    /// 单次 unary RPC 的服务端毫秒预算。
    unary_timeout_ms: u64,
    /// 流式 RPC 连续无进展的毫秒上限。
    stream_idle_timeout_ms: u64,
    /// 单个流式 RPC 的总持续毫秒上限。
    max_stream_duration_ms: u64,
    /// 每条流累计可接收的消息条数。
    max_received_messages_per_stream: u64,
    /// 每条流累计可发送的消息条数。
    max_sent_messages_per_stream: u64,
    /// 每条流入站消息累计字节上限。
    max_received_stream_bytes: u64,
    /// 每条流出站消息累计字节上限。
    max_sent_stream_bytes: u64,
    /// HTTP/2 保活探测间隔，单位毫秒。
    keepalive_interval_ms: u64,
    /// HTTP/2 保活响应等候上限，单位毫秒。
    keepalive_timeout_ms: u64,
    /// HTTP/2 对端同时打开的流数量上限。
    max_concurrent_streams: u32,
    /// 每条 HTTP/2 流的初始接收窗口字节数。
    initial_stream_window_size: u32,
    /// HTTP/2 连接的初始接收窗口字节数。
    initial_connection_window_size: u32,
    /// HTTP/2 帧负载字节上限。
    max_frame_size: u32,
    /// HTTP/2 请求头列表容量上限。
    http2_max_header_list_size: u32,
    /// HPACK 解码动态表容量上限。
    http2_header_table_size: u32,
    /// 每条 HTTP/2 流的发送缓冲字节上限。
    http2_max_send_buffer_size: usize,
    /// 接收端保留的待处理 reset stream 数上限。
    http2_max_pending_accept_reset_streams: usize,
    /// 本地错误允许占用的 reset stream 数上限。
    http2_max_local_error_reset_streams: usize,
    /// 每连接每秒允许处理的 HTTP/2 控制帧数。
    http2_control_frames_per_second: u32,
    /// 每连接控制帧的短时突发容量。
    http2_control_frames_burst: u32,
    /// 当前监听器全部连接每秒允许处理的 HTTP/2 控制帧总数。
    http2_process_control_frames_per_second: u32,
    /// 进程级控制帧的短时突发容量。
    http2_process_control_frames_burst: u32,
    /// TCP 空闲后开始保活探测的毫秒数。
    tcp_keepalive_ms: u64,
    /// TCP 保活探测重试间隔，单位毫秒。
    tcp_keepalive_interval_ms: u64,
    /// TCP 保活未获应答时的重试次数。
    tcp_keepalive_retries: u32,
    /// 是否关闭 TCP 小包合并以降低 RPC 延迟。
    tcp_nodelay: bool,
    /// 可选连接寿命上限，单位毫秒；未配置时不按年龄淘汰。
    max_connection_age_ms: Option<u64>,
    /// 逐连接淘汰时 PING 与既有流共用的毫秒排空预算。
    connection_eviction_grace_ms: Option<u64>,
    /// 监听器停机排空的毫秒子预算，仍受应用全局期限限制。
    drain_timeout_ms: u64,
    /// 进程级在途 RPC 数量上限。
    max_inflight_rpcs: usize,
    /// 进程级在途消息字节容量上限。
    max_inflight_message_bytes: usize,
    /// 受管连接、流及消息容量的内存规划预算，不等于进程 RSS 硬限制。
    managed_memory_budget_bytes: usize,
    /// 单条消息解码字节上限。
    max_decoding_bytes: usize,
    /// 单条消息编码字节上限。
    max_encoding_bytes: usize,
    /// 服务端 TLS 模式、秘密引用与证书时间策略。
    tls: GrpcTlsSettings,
    /// 标准 reflection 服务的显式开放策略。
    reflection: GrpcReflectionSettings,
    /// 是否允许未登记业务服务而只开放健康服务。
    health_only: bool,
    /// 对 descriptor 中已存在方法收紧身份或容量边界的配置。
    methods: BTreeMap<String, GrpcMethodSettings>,
}

/// descriptor 固定方法的可选授权与容量收紧配置。
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GrpcMethodSettings {
    /// 是否要求通过 mTLS 提取的对端身份。
    require_peer_identity: bool,
    /// 当前方法的并发上限，未指定时沿用受管默认值。
    max_inflight_rpcs: Option<usize>,
    /// 当前方法的每秒速率限制，与 burst 配套声明。
    requests_per_second: Option<u32>,
    /// 当前方法速率限制的短时突发容量。
    burst: Option<u32>,
}

/// reflection 的显式开放策略；独立子表避免布尔值与后续 allowlist 合同冲突。
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GrpcReflectionSettings {
    /// 是否开放标准服务描述查询，缺省关闭。
    enabled: bool,
}

/// listener TLS 模式；未显式声明时仅 loopback 可使用明文。
#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum GrpcTlsMode {
    /// 使用明文，非 loopback 还需显式允许远程明文。
    #[default]
    Disabled,
    /// 提供服务端证书，不要求客户端证书。
    Server,
    /// 双向 TLS，必须提供获准的客户端信任根。
    Mutual,
}

impl GrpcTlsMode {
    /// 业务作用：把冻结的 TLS 模式投影为服务发现使用的封闭协议值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅可能为 `disabled`、`server` 或 `mutual` 的稳定文本。
    #[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
    fn discovery_value(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Server => "server",
            Self::Mutual => "mutual",
        }
    }
}

/// 受管 TLS 只保存 secret locator 和时间门禁，不允许 PEM 进入普通配置树。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GrpcTlsSettings {
    /// 当前监听器采用的传输身份模式。
    mode: GrpcTlsMode,
    /// 服务端证书链的 secret locator，不接收 PEM 明文。
    certificate: Option<String>,
    /// 与证书匹配的私钥 secret locator。
    private_key: Option<String>,
    /// 双向 TLS 客户端信任根的 secret locator。
    client_ca: Option<String>,
    /// 到期前标记 Degraded 的预警窗口，单位毫秒。
    certificate_warning_window_ms: u64,
    /// 准备身份时要求的最短剩余有效期，单位毫秒。
    certificate_minimum_remaining_ms: u64,
    /// 证书时间校验允许的时钟偏差，单位毫秒。
    clock_skew_ms: u64,
}

impl Default for GrpcTlsSettings {
    /// 业务作用：提供不隐式开启 TLS、但在开启后有发布替换窗口的安全默认。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：TLS 关闭，到期前 30 天降级、启动至少剩余 24 小时、时钟偏差 5 分钟。
    fn default() -> Self {
        Self {
            mode: GrpcTlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificate_warning_window_ms: 30 * 24 * 60 * 60 * 1_000,
            certificate_minimum_remaining_ms: 24 * 60 * 60 * 1_000,
            clock_skew_ms: 5 * 60 * 1_000,
        }
    }
}

/// Start 阶段已校验的 TLS locator 与时间边界；真实材料延迟到 Ready 从同代 secret 快照取得。
#[derive(Clone)]
pub(crate) struct GrpcTlsPlan {
    /// 已验证的服务端或双向 TLS 模式。
    mode: GrpcTlsMode,
    /// 从同代秘密快照读取证书链的规范化标识。
    certificate_id: Arc<str>,
    /// 从同代秘密快照读取私钥的规范化标识。
    private_key_id: Arc<str>,
    /// 双向 TLS 必须具备的客户端信任根标识。
    client_ca_id: Option<Arc<str>>,
    /// 运行时证书到期预警窗口。
    warning_window: Duration,
    /// 准备候选身份时要求的最短剩余有效期。
    minimum_remaining: Duration,
    /// 身份准备时允许的证书时间偏差。
    clock_skew: Duration,
}

/// Start 之前已通过地址、容量、TLS 与服务发现门禁的 listener 计划。
struct ValidatedGrpcSettings {
    /// 通过远程明文与地址门禁的预绑定地址。
    bind: SocketAddr,
    /// 已统一校验容量与时间边界的底层监听器配置。
    config: nagrpc::GrpcServerConfig,
    /// 是否在服务目录中装配 reflection。
    reflection: bool,
    /// 是否允许空业务服务目录。
    health_only: bool,
    /// 明文为 None，TLS 为等待 Ready 阶段读取秘密的身份计划。
    tls: Option<GrpcTlsPlan>,
    /// 对外服务发现使用的 DNS 或 IP 身份。
    authority: Option<String>,
}

impl Default for GrpcSettings {
    /// 业务作用：提供仅绑定 loopback、容量有界且自动 health 的稳定 listener 缺省配置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端口 50051、受管连接/消息边界、关闭 reflection 且要求业务 service 的配置投影。
    fn default() -> Self {
        let config = nagrpc::GrpcServerConfig::default();
        Self {
            bind: "127.0.0.1:50051".to_owned(),
            authority: None,
            allow_insecure_remote: false,
            max_connections: config.max_connections,
            concurrency_limit_per_connection: config.concurrency_limit_per_connection,
            connection_handshake_timeout_ms: duration_millis(config.connection_handshake_timeout),
            first_request_timeout_ms: duration_millis(config.first_request_timeout),
            idle_connection_timeout_ms: duration_millis(config.idle_connection_timeout),
            unary_timeout_ms: duration_millis(config.unary_timeout),
            stream_idle_timeout_ms: duration_millis(config.stream_idle_timeout),
            max_stream_duration_ms: duration_millis(config.max_stream_duration),
            max_received_messages_per_stream: config.max_received_messages_per_stream,
            max_sent_messages_per_stream: config.max_sent_messages_per_stream,
            max_received_stream_bytes: config.max_received_stream_bytes,
            max_sent_stream_bytes: config.max_sent_stream_bytes,
            keepalive_interval_ms: duration_millis(config.keepalive_interval),
            keepalive_timeout_ms: duration_millis(config.keepalive_timeout),
            max_concurrent_streams: config.max_concurrent_streams,
            initial_stream_window_size: config.initial_stream_window_size,
            initial_connection_window_size: config.initial_connection_window_size,
            max_frame_size: config.max_frame_size,
            http2_max_header_list_size: config.max_header_list_size,
            http2_header_table_size: config.header_table_size,
            http2_max_send_buffer_size: config.max_send_buffer_size,
            http2_max_pending_accept_reset_streams: config.max_pending_accept_reset_streams,
            http2_max_local_error_reset_streams: config.max_local_error_reset_streams,
            http2_control_frames_per_second: config.control_frames_per_second,
            http2_control_frames_burst: config.control_frames_burst,
            http2_process_control_frames_per_second: config.process_control_frames_per_second,
            http2_process_control_frames_burst: config.process_control_frames_burst,
            tcp_keepalive_ms: duration_millis(config.tcp_keepalive),
            tcp_keepalive_interval_ms: duration_millis(config.tcp_keepalive_interval),
            tcp_keepalive_retries: config.tcp_keepalive_retries,
            tcp_nodelay: config.tcp_nodelay,
            max_connection_age_ms: config.max_connection_age.map(duration_millis),
            connection_eviction_grace_ms: None,
            drain_timeout_ms: duration_millis(config.drain_timeout),
            max_inflight_rpcs: config.max_inflight_rpcs,
            max_inflight_message_bytes: config.max_inflight_message_bytes,
            managed_memory_budget_bytes: config.managed_memory_budget_bytes,
            max_decoding_bytes: config.message_limits.max_decoding_bytes,
            max_encoding_bytes: config.message_limits.max_encoding_bytes,
            tls: GrpcTlsSettings::default(),
            reflection: GrpcReflectionSettings::default(),
            health_only: false,
            methods: BTreeMap::new(),
        }
    }
}

impl GrpcSettings {
    /// 业务作用：解析绑定地址并把毫秒配置投影为 nagrpc 的唯一 server 边界对象。
    ///
    /// 参数说明：
    /// - `phase`: 当前配置校验所属 Application 阶段。
    ///
    /// 返回：地址与全部硬上限合法时返回可构造 Router 和 listener 的配置，否则返回 gRPC 错误。
    fn validate(&self, phase: ApplicationPhase) -> ApplicationResult<ValidatedGrpcSettings> {
        let bind = self.bind.parse::<SocketAddr>().map_err(|error| {
            grpc_error_src(phase, "grpc.bind must be an IP socket address", error)
        })?;
        let config = nagrpc::GrpcServerConfig {
            max_connections: self.max_connections,
            concurrency_limit_per_connection: self.concurrency_limit_per_connection,
            connection_handshake_timeout: Duration::from_millis(
                self.connection_handshake_timeout_ms,
            ),
            first_request_timeout: Duration::from_millis(self.first_request_timeout_ms),
            idle_connection_timeout: Duration::from_millis(self.idle_connection_timeout_ms),
            unary_timeout: Duration::from_millis(self.unary_timeout_ms),
            stream_idle_timeout: Duration::from_millis(self.stream_idle_timeout_ms),
            max_stream_duration: Duration::from_millis(self.max_stream_duration_ms),
            max_received_messages_per_stream: self.max_received_messages_per_stream,
            max_sent_messages_per_stream: self.max_sent_messages_per_stream,
            max_received_stream_bytes: self.max_received_stream_bytes,
            max_sent_stream_bytes: self.max_sent_stream_bytes,
            keepalive_interval: Duration::from_millis(self.keepalive_interval_ms),
            keepalive_timeout: Duration::from_millis(self.keepalive_timeout_ms),
            max_concurrent_streams: self.max_concurrent_streams,
            initial_stream_window_size: self.initial_stream_window_size,
            initial_connection_window_size: self.initial_connection_window_size,
            max_frame_size: self.max_frame_size,
            max_header_list_size: self.http2_max_header_list_size,
            header_table_size: self.http2_header_table_size,
            max_send_buffer_size: self.http2_max_send_buffer_size,
            max_pending_accept_reset_streams: self.http2_max_pending_accept_reset_streams,
            max_local_error_reset_streams: self.http2_max_local_error_reset_streams,
            control_frames_per_second: self.http2_control_frames_per_second,
            control_frames_burst: self.http2_control_frames_burst,
            process_control_frames_per_second: self.http2_process_control_frames_per_second,
            process_control_frames_burst: self.http2_process_control_frames_burst,
            tcp_keepalive: Duration::from_millis(self.tcp_keepalive_ms),
            tcp_keepalive_interval: Duration::from_millis(self.tcp_keepalive_interval_ms),
            tcp_keepalive_retries: self.tcp_keepalive_retries,
            tcp_nodelay: self.tcp_nodelay,
            max_connection_age: self.max_connection_age_ms.map(Duration::from_millis),
            connection_eviction_grace: self
                .connection_eviction_grace_ms
                .map(Duration::from_millis)
                .unwrap_or_else(|| {
                    Duration::from_millis(self.drain_timeout_ms.saturating_mul(3) / 4)
                }),
            drain_timeout: Duration::from_millis(self.drain_timeout_ms),
            max_inflight_rpcs: self.max_inflight_rpcs,
            max_inflight_message_bytes: self.max_inflight_message_bytes,
            managed_memory_budget_bytes: self.managed_memory_budget_bytes,
            message_limits: nagrpc::GrpcMessageLimits {
                max_decoding_bytes: self.max_decoding_bytes,
                max_encoding_bytes: self.max_encoding_bytes,
            },
            method_policies: self
                .methods
                .iter()
                .map(|(method, settings)| {
                    (
                        method.clone(),
                        nagrpc::GrpcMethodPolicy {
                            require_peer_identity: settings.require_peer_identity,
                            max_inflight_rpcs: settings.max_inflight_rpcs,
                            requests_per_second: settings.requests_per_second,
                            burst: settings.burst,
                        },
                    )
                })
                .collect(),
        };
        config.validate().map_err(|error| {
            grpc_error_src(
                phase,
                "grpc configuration exceeds a managed server boundary",
                error,
            )
        })?;
        if self.health_only && self.reflection.enabled {
            return Err(grpc_error(
                phase,
                "grpc.health_only cannot be combined with reflection",
            ));
        }
        let tls = self.tls.validate(bind, self.allow_insecure_remote, phase)?;
        let authority = self
            .authority
            .as_deref()
            .map(validate_authority)
            .transpose()
            .map_err(|_| grpc_error(phase, "grpc.authority is invalid"))?;
        Ok(ValidatedGrpcSettings {
            bind,
            config,
            reflection: self.reflection.enabled,
            health_only: self.health_only,
            tls,
            authority,
        })
    }
}

/// 业务作用：校验服务发现与 TLS client 使用的 gRPC authority，阻止 URI 或路径注入 metadata。
///
/// 参数说明：
/// - `authority`: 配置中的 DNS 名或 IP，不含 scheme、端口和路径。
///
/// 返回：IP 或长度有界的 DNS 名合法时返回规范副本；空值、非法分段或 URI 分隔符返回配置错误。
fn validate_authority(authority: &str) -> Result<String, &'static str> {
    if authority.parse::<IpAddr>().is_ok() {
        return Ok(authority.to_owned());
    }
    if authority.is_empty()
        || authority.len() > 253
        || authority.starts_with('.')
        || authority.ends_with('.')
        || authority.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err("authority must be a DNS name or IP without scheme, port, or path");
    }
    Ok(authority.to_ascii_lowercase())
}

impl GrpcTlsSettings {
    /// 业务作用：校验 TLS 模式、`secret://` locator 完整性和证书时间政策。
    ///
    /// 参数说明：
    /// - `bind`: 最终 listener 地址，用于拒绝未批准的远程明文暴露。
    /// - `allow_insecure_remote`: 是否明确承担非 loopback 明文运行责任。
    /// - `phase`: 当前 Application 错误归属阶段。
    ///
    /// 返回：明文模式合法时返回 `None`；TLS/mTLS 材料引用齐全且时间边界
    /// 合法时返回 Ready 可消费的计划；否则在 bind 前拒绝启动。
    fn validate(
        &self,
        bind: SocketAddr,
        allow_insecure_remote: bool,
        phase: ApplicationPhase,
    ) -> ApplicationResult<Option<GrpcTlsPlan>> {
        if self.mode == GrpcTlsMode::Disabled {
            if self.certificate.is_some() || self.private_key.is_some() || self.client_ca.is_some()
            {
                return Err(grpc_error(
                    phase,
                    "grpc.tls material cannot be configured while TLS is disabled",
                ));
            }
            if !bind.ip().is_loopback() && !allow_insecure_remote {
                return Err(grpc_error(
                    phase,
                    "remote plaintext grpc.bind requires allow_insecure_remote=true",
                ));
            }
            return Ok(None);
        }
        if allow_insecure_remote {
            return Err(grpc_error(
                phase,
                "allow_insecure_remote only applies when grpc.tls.mode is disabled",
            ));
        }
        let certificate_id = parse_secret_locator(self.certificate.as_deref(), phase)?;
        let private_key_id = parse_secret_locator(self.private_key.as_deref(), phase)?;
        let client_ca_id = match self.mode {
            GrpcTlsMode::Mutual => Some(parse_secret_locator(self.client_ca.as_deref(), phase)?),
            GrpcTlsMode::Server => {
                if self.client_ca.is_some() {
                    return Err(grpc_error(
                        phase,
                        "grpc.tls.client_ca is only valid in mutual mode",
                    ));
                }
                None
            }
            GrpcTlsMode::Disabled => unreachable!("disabled mode returned before TLS planning"),
        };
        let warning_window = Duration::from_millis(self.certificate_warning_window_ms);
        let minimum_remaining = Duration::from_millis(self.certificate_minimum_remaining_ms);
        let clock_skew = Duration::from_millis(self.clock_skew_ms);
        if warning_window.is_zero()
            || warning_window > Duration::from_secs(365 * 24 * 60 * 60)
            || minimum_remaining.is_zero()
            || minimum_remaining > warning_window
            || clock_skew > Duration::from_secs(60 * 60)
        {
            return Err(grpc_error(
                phase,
                "grpc.tls certificate time policy exceeds a managed boundary",
            ));
        }
        Ok(Some(GrpcTlsPlan {
            mode: self.mode,
            certificate_id,
            private_key_id,
            client_ca_id,
            warning_window,
            minimum_remaining,
            clock_skew,
        }))
    }
}

/// 业务作用：把公开配置中的 `secret://` locator 缩减为同代 secret 快照 ID。
///
/// 参数说明：
/// - `locator`: 必须存在的 TLS 材料 locator。
/// - `phase`: 当前 Application 错误归属阶段。
///
/// 返回：协议、长度和分段均合法时返回 ID；否则返回不回显 locator 的配置错误。
fn parse_secret_locator(
    locator: Option<&str>,
    phase: ApplicationPhase,
) -> ApplicationResult<Arc<str>> {
    let id = locator
        .and_then(|value| value.strip_prefix("secret://"))
        .filter(|value| valid_secret_locator_id(value))
        .ok_or_else(|| {
            grpc_error(
                phase,
                "grpc.tls material must use a valid secret:// locator",
            )
        })?;
    Ok(Arc::from(id))
}

/// 业务作用：校验 secret locator 的有界层级 ID，避免空分段、路径穿越和控制字符。
///
/// 参数说明：
/// - `value`: 已移除 `secret://` 协议头的 ID。
///
/// 返回：长度不超过 128 且每个 `/` 分段均为短 ASCII 标识时返回 true。
fn valid_secret_locator_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

/// Ready 阶段已复验的 TLS 时间事实；不包含任何 PEM 或 secret locator。
#[derive(Clone)]
struct PreparedTlsCertificate {
    /// 服务端证书链最早到期的 Unix 秒时间戳，不含证书内容。
    expiry_timestamp: u64,
    /// 剩余有效期进入此窗口后标记 Degraded。
    warning_window: Duration,
}

/// 业务作用：从 Application 同代 secret 快照构造启动期冻结的 TLS/mTLS identity。
///
/// 参数说明：
/// - `plan`: Start 已验证的 locator、模式与证书时间边界。
/// - `secrets`: 与当前最终配置相同 generation 的不可变 secret 快照。
///
/// 返回：材料齐全、证书与密钥合法且剩余期达标时返回 identity 和脱敏到期
/// 事实；任一门禁失败时在 bind 前返回 Ready 错误。
fn prepare_tls_identity(
    plan: &GrpcTlsPlan,
    secrets: &nasecret::SecretSnapshot,
) -> ApplicationResult<(nagrpc::GrpcTlsIdentity, PreparedTlsCertificate)> {
    prepare_tls_identity_with_ca(plan, secrets, &[])
}

/// 业务作用：用同代服务端身份和仍在重叠窗口内的客户端 CA 构造握手资源。
///
/// 参数说明：
/// - `plan`：`plan` 固定 TLS 边界。
/// - `secrets`：`secrets` 为同代材料。
/// - `previous_ca`：`previous_ca` 为获准暂存的旧信任根。
///
/// 返回：身份、时间政策和 CA 均合法时返回完整身份与到期事实；任一材料非法时拒绝候选。
fn prepare_tls_identity_with_ca(
    plan: &GrpcTlsPlan,
    secrets: &nasecret::SecretSnapshot,
    previous_ca: &[Vec<u8>],
) -> ApplicationResult<(nagrpc::GrpcTlsIdentity, PreparedTlsCertificate)> {
    let certificate = read_tls_secret(secrets, &plan.certificate_id)?;
    let private_key = read_tls_secret(secrets, &plan.private_key_id)?;
    let identity = match plan.mode {
        GrpcTlsMode::Server => nagrpc::GrpcTlsIdentity::server(certificate, private_key),
        GrpcTlsMode::Mutual => {
            let client_ca_id = plan.client_ca_id.as_ref().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC mutual TLS client CA was not prepared",
                )
            })?;
            let mut client_ca = read_tls_secret(secrets, client_ca_id)?;
            for ca in previous_ca {
                client_ca.push(b'\n');
                client_ca.extend_from_slice(ca);
            }
            nagrpc::GrpcTlsIdentity::mutual(certificate, private_key, client_ca)
        }
        GrpcTlsMode::Disabled => {
            return Err(grpc_error(
                ApplicationPhase::Ready,
                "disabled gRPC TLS mode cannot prepare an identity",
            ));
        }
    }
    .and_then(|identity| {
        identity.certificate_lifetime_policy(plan.minimum_remaining, plan.clock_skew)
    })
    .map_err(|error| {
        grpc_error_src(
            ApplicationPhase::Ready,
            "gRPC TLS identity does not satisfy the managed certificate contract",
            error,
        )
    })?;
    let expiry_timestamp = identity.certificate_expiry_timestamp().map_err(|error| {
        grpc_error_src(
            ApplicationPhase::Ready,
            "gRPC TLS certificate lifetime could not be validated",
            error,
        )
    })?;
    Ok((
        identity,
        PreparedTlsCertificate {
            expiry_timestamp,
            warning_window: plan.warning_window,
        },
    ))
}

/// 业务作用：保存服务端 TLS 候选及旧 CA 的到期边界，请求路径只选择已完成校验的握手器。
#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
pub(crate) struct RotatingGrpcTlsSnapshot {
    /// 后续材料轮换必须沿用的监听器 TLS 政策。
    #[cfg(feature = "nacos-config")]
    plan: GrpcTlsPlan,
    /// 与候选身份同代的秘密快照，用于下一轮比较信任根。
    secrets: Arc<nasecret::SecretSnapshot>,
    /// 仅使用当前客户端信任根的握手器。
    current: nagrpc::GrpcTlsAcceptor,
    /// 当前服务端身份的证书时间事实。
    certificate: PreparedTlsCertificate,
    /// 旧客户端 CA 字节及各自单调到期点，最多保留八个重叠项。
    previous_ca: Vec<(Vec<u8>, std::time::Instant)>,
    /// 按到期点排序的预构造握手器，握手开始时选择仍有效的信任集合。
    overlap_acceptors: Vec<(std::time::Instant, nagrpc::GrpcTlsAcceptor)>,
}

#[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
impl RotatingGrpcTlsSnapshot {
    /// 业务作用：在配置发布前同时准备新服务端证书、私钥和所有有效客户端 CA 组合。
    ///
    /// 参数说明：
    /// - `plan`：`plan` 是固定监听器政策。
    /// - `secrets`：`secrets` 为候选秘密材料。
    /// - `previous`：`previous` 是已提交资源。
    /// - `overlap`：`overlap` 限制旧 CA 接受时长。
    ///
    /// 返回：全部握手器可构造时返回候选；缺失、过期、不匹配材料或超过八个重叠窗口时拒绝发布。
    pub(crate) fn prepare(
        plan: &GrpcTlsPlan,
        secrets: Arc<nasecret::SecretSnapshot>,
        previous: Option<&Self>,
        overlap: Duration,
    ) -> ApplicationResult<Self> {
        let (identity, certificate) = prepare_tls_identity(plan, &secrets)?;
        let prepare = |identity: &nagrpc::GrpcTlsIdentity| {
            nagrpc::GrpcTlsAcceptor::prepare(identity).map_err(|_| {
                grpc_error(
                    ApplicationPhase::Running,
                    "gRPC TLS rotation candidate cannot prepare a handshake",
                )
            })
        };
        let current = prepare(&identity)?;
        let now = std::time::Instant::now();
        let mut previous_ca = Vec::new();
        if let (Some(reference), Some(previous)) = (&plan.client_ca_id, previous) {
            let candidate_ca = read_tls_secret(&secrets, reference)?;
            let old_ca = read_tls_secret(&previous.secrets, reference)?;
            previous_ca = previous
                .previous_ca
                .iter()
                .filter(|(ca, expires)| *expires > now && *ca != candidate_ca)
                .cloned()
                .collect();
            if candidate_ca != old_ca {
                previous_ca.retain(|(ca, _)| *ca != old_ca);
                previous_ca.push((old_ca, now + overlap));
            }
        }
        if previous_ca.len() > 8 {
            return Err(grpc_error(
                ApplicationPhase::Running,
                "gRPC TLS rotation overlap capacity exceeded",
            ));
        }
        previous_ca.sort_by_key(|(_, expires)| *expires);
        let mut boundaries: Vec<_> = previous_ca.iter().map(|(_, expires)| *expires).collect();
        boundaries.dedup();
        let mut overlap_acceptors = Vec::new();
        for boundary in boundaries {
            let cas: Vec<_> = previous_ca
                .iter()
                .filter(|(_, expires)| *expires >= boundary)
                .map(|(ca, _)| ca.clone())
                .collect();
            let (identity, _) = prepare_tls_identity_with_ca(plan, &secrets, &cas)?;
            overlap_acceptors.push((boundary, prepare(&identity)?));
        }
        Ok(Self {
            #[cfg(feature = "nacos-config")]
            plan: plan.clone(),
            secrets,
            current,
            certificate,
            previous_ca,
            overlap_acceptors,
        })
    }

    /// 业务作用：用冻结的监听器政策验证下一代材料，保持地址与 TLS 模式不随秘密材料漂移。
    ///
    /// 参数说明：
    /// - `secrets`：`secrets` 是完整候选。
    /// - `overlap`：`overlap` 是旧信任根的有界保留窗口。
    ///
    /// 返回：可发布的完整资源；校验失败保持当前资源不变。
    #[cfg(feature = "nacos-config")]
    pub(crate) fn rotated(
        &self,
        secrets: Arc<nasecret::SecretSnapshot>,
        overlap: Duration,
    ) -> ApplicationResult<Self> {
        Self::prepare(&self.plan, secrets, Some(self), overlap)
    }

    /// 业务作用：在新连接开始握手时固定一份尚未越过旧 CA 到期边界的信任集合。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前窗口的不可变握手器；旧 CA 到期后自动只采用剩余信任根。
    pub(crate) fn acceptor(&self) -> nagrpc::GrpcTlsAcceptor {
        let now = std::time::Instant::now();
        self.overlap_acceptors
            .iter()
            .find(|(expires, _)| *expires > now)
            .map(|(_, acceptor)| acceptor.clone())
            .unwrap_or_else(|| self.current.clone())
    }

    /// 业务作用：让监控与实际握手身份使用相同证书到期事实。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前服务端证书链最早到期的 Unix 秒。
    pub(crate) fn expiry_timestamp(&self) -> u64 {
        self.certificate.expiry_timestamp
    }
}

/// 业务作用：从冻结 secret 快照复制一份交给 TLS identity 独占的材料。
///
/// 参数说明：
/// - `secrets`: 当前配置 generation 的 secret 快照。
/// - `id`: 已校验且不回显到错误链的 secret ID。
///
/// 返回：材料存在时返回拥有字节；缺失或为空时返回不含 ID 与内容的 Ready 错误。
fn read_tls_secret(secrets: &nasecret::SecretSnapshot, id: &str) -> ApplicationResult<Vec<u8>> {
    let material = secrets
        .get(id)
        .filter(|material| !material.is_empty())
        .ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Ready,
                "gRPC TLS secret material is unavailable",
            )
        })?;
    Ok(material.expose().to_vec())
}

/// 运行期证书到期监督；通过只读身份源跟随轮换，并独占 TLS readiness 贡献。
#[derive(Clone)]
struct TlsCertificateRuntime {
    /// 无可轮换身份源时使用的启动证书到期 Unix 秒。
    expiry_timestamp: u64,
    /// 与实际监听握手同源的可轮换证书观察入口。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    source: Option<Arc<crate::saga::security::ManagedGrpcTlsSource>>,
    /// 剩余有效期不足时标记降级的预警窗口。
    warning_window: Duration,
    /// 当前 TLS 依赖状态的发布句柄，不包含关闭 listener 的权限。
    contributor: ReadinessContributor,
}

impl TlsCertificateRuntime {
    /// 业务作用：把当前握手证书的剩余期投影为 Ready、Degraded 或 NotReady。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：证书未到期时发布 Ready 或 Degraded 并成功；到期时先发布 NotReady 再返回运行错误。
    /// 系统时钟早于 Unix 起点时直接返回错误，不发布新的贡献状态；调用方必须将错误交由 Runner 处理停机。
    fn observe(&self) -> ApplicationResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| {
                grpc_error_src(
                    ApplicationPhase::Running,
                    "system clock cannot validate gRPC TLS certificate lifetime",
                    error,
                )
            })?
            .as_secs();
        let expiry_timestamp = self.expiry_timestamp;
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
        let expiry_timestamp = self
            .source
            .as_ref()
            .map(|source| source.expiry_timestamp())
            .unwrap_or(expiry_timestamp);
        // 先报告证书失效再返回关键错误，由 Runner 关闭准入；仅靠新握手拒绝无法停止既有连接。
        if now >= expiry_timestamp {
            self.contributor.observe(
                DependencyState::NotReady,
                reason::GRPC_TLS_CERTIFICATE_EXPIRED,
                std::time::Instant::now(),
            );
            return Err(grpc_error(
                ApplicationPhase::Running,
                "gRPC TLS certificate has expired",
            ));
        }
        let remaining = expiry_timestamp.saturating_sub(now);
        let (state, reason) = if remaining <= self.warning_window.as_secs() {
            (
                DependencyState::Degraded,
                reason::GRPC_TLS_CERTIFICATE_EXPIRING,
            )
        } else {
            (DependencyState::Ready, reason::HEALTHY)
        };
        self.contributor
            .observe(state, reason, std::time::Instant::now());
        Ok(())
    }
}

/// 受管 listener 指标族；连接/TLS 族无 label，RPC 族只使用 sealed descriptor 的固定维度。
macro_rules! grpc_metric {
    ($ident:ident, $name:literal, $help:literal, $kind:expr) => {
        static $ident: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: $kind,
            label_names: &[],
            histogram_bounds: &[],
        };
    };
}

grpc_metric!(
    GRPC_SERVING,
    "napp_grpc_serving",
    "受管 gRPC listener 当前是否处于 Running 并对新连接开放准入。",
    nametrics_core::MetricKind::Gauge
);

static GRPC_RPCS_ACTIVE: nametrics_core::MetricDescriptor = nametrics_core::MetricDescriptor {
    name: "napp_grpc_rpcs_active",
    help: "按 sealed descriptor 固定 service、method 与 RPC 形态统计的当前在途调用数。",
    unit: "",
    kind: nametrics_core::MetricKind::Gauge,
    label_names: &["service", "method", "rpc_type"],
    histogram_bounds: &[],
};
static GRPC_RPCS_STARTED_TOTAL: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_grpc_rpcs_started_total",
        help: "按 sealed descriptor 固定方法统计的已接纳 RPC 总数。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &["service", "method", "rpc_type"],
        histogram_bounds: &[],
    };
static GRPC_RPCS_REJECTED_TOTAL: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_grpc_rpcs_rejected_total",
        help: "在 handler 前按连接容量、进程容量、方法并发、方法速率或身份门禁分类的拒绝总数。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &["service", "method", "rpc_type", "reason"],
        histogram_bounds: &[],
    };
static GRPC_RPCS_COMPLETED_TOTAL: nametrics_core::MetricDescriptor =
    nametrics_core::MetricDescriptor {
        name: "napp_grpc_rpcs_completed_total",
        help: "按固定 gRPC code 与框架取消、deadline、流式持续/空闲/方向累计边界、传输丢失结局统计的完成总数。",
        unit: "",
        kind: nametrics_core::MetricKind::Counter,
        label_names: &["service", "method", "rpc_type", "outcome"],
        histogram_bounds: &[],
    };
grpc_metric!(
    GRPC_CONNECTIONS_ACTIVE,
    "napp_grpc_connections_active",
    "已交给受管 HTTP/2 connection driver 且尚未关闭的连接数。",
    nametrics_core::MetricKind::Gauge
);
grpc_metric!(
    GRPC_CONNECTIONS_ACCEPTED,
    "napp_grpc_connections_accepted_total",
    "进入 listener 所有权的连接总数。",
    nametrics_core::MetricKind::Counter
);
grpc_metric!(
    GRPC_ACCEPT_FAILURES,
    "napp_grpc_accept_failures_total",
    "accept 返回错误的累计次数。",
    nametrics_core::MetricKind::Counter
);
grpc_metric!(
    GRPC_ACCEPT_CONSECUTIVE_FAILURES,
    "napp_grpc_accept_consecutive_failures",
    "最近一次成功 accept 之后的连续失败次数。",
    nametrics_core::MetricKind::Gauge
);
grpc_metric!(
    GRPC_ACCEPT_STALL_SECONDS,
    "napp_grpc_accept_stall_seconds",
    "当前连续 accept 失败已持续的秒数；没有进行中的失败段时为 0。",
    nametrics_core::MetricKind::Gauge
);
grpc_metric!(
    GRPC_TLS_CERTIFICATE_EXPIRY_TIMESTAMP_SECONDS,
    "napp_grpc_tls_certificate_expiry_timestamp_seconds",
    "当前已发布 TLS server identity chain 的最早 Unix 到期时间。",
    nametrics_core::MetricKind::Gauge
);

/// 受管 listener 的全部 descriptor；启动期一次性登记，避免 Ready 后再扩张观测面。
static GRPC_DESCRIPTORS: [&nametrics_core::MetricDescriptor; 11] = [
    &GRPC_SERVING,
    &GRPC_CONNECTIONS_ACTIVE,
    &GRPC_CONNECTIONS_ACCEPTED,
    &GRPC_ACCEPT_FAILURES,
    &GRPC_ACCEPT_CONSECUTIVE_FAILURES,
    &GRPC_ACCEPT_STALL_SECONDS,
    &GRPC_TLS_CERTIFICATE_EXPIRY_TIMESTAMP_SECONDS,
    &GRPC_RPCS_ACTIVE,
    &GRPC_RPCS_STARTED_TOTAL,
    &GRPC_RPCS_REJECTED_TOTAL,
    &GRPC_RPCS_COMPLETED_TOTAL,
];

/// 单个 gRPC 组件允许占用的公开序列子预算；进程总预算仍由 nametrics-core 原子复验。
const GRPC_METRIC_SERIES_BUDGET: usize = 20_000;
/// 明文 listener 必然出现的无 label 序列数；TLS 到期序列只在 TLS 模式另加一项。
const GRPC_FIXED_METRIC_SERIES: usize = 6;
/// health service 固定 Check/Watch 两个方法；reflection 开启时再增加一个双向流方法。
const GRPC_HEALTH_METHODS: usize = 2;
const GRPC_REFLECTION_METHODS: usize = 1;

/// 业务作用：按 sealed method 目录与全部封闭 label domain 计算 gRPC 指标最坏公开序列数。
///
/// 参数说明：
/// - `registry`: UserHook 已封口且尚未移交 listener 的业务 service registry。
/// - `reflection`: 是否启用标准 reflection service。
/// - `tls`: 是否启用会产生证书到期序列的 TLS 模式。
///
/// 返回：计算结果不溢出且不超过 gRPC 子预算时返回精确上界；否则在绑定 listener 前拒绝 Prepare。
fn grpc_worst_case_metric_series(
    registry: &nagrpc::ManagedServiceRegistry,
    reflection: bool,
    tls: bool,
) -> ApplicationResult<usize> {
    let methods = registry
        .method_count()
        .checked_add(GRPC_HEALTH_METHODS)
        .and_then(|count| count.checked_add(usize::from(reflection) * GRPC_REFLECTION_METHODS))
        .ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Prepare,
                "gRPC metric series calculation overflowed",
            )
        })?;
    let per_method = 2_usize
        .checked_add(nagrpc::GRPC_RPC_REJECTION_REASON_CARDINALITY)
        .and_then(|count| count.checked_add(nagrpc::GRPC_RPC_OUTCOME_CARDINALITY))
        .ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Prepare,
                "gRPC metric series calculation overflowed",
            )
        })?;
    let fixed = GRPC_FIXED_METRIC_SERIES + usize::from(tls);
    let total = methods
        .checked_mul(per_method)
        .and_then(|count| count.checked_add(fixed))
        .ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Prepare,
                "gRPC metric series calculation overflowed",
            )
        })?;
    if total > GRPC_METRIC_SERIES_BUDGET {
        return Err(grpc_error(
            ApplicationPhase::Prepare,
            "gRPC metric series budget is exceeded",
        ));
    }
    Ok(total)
}

/// 把受管 listener 的接流事实接入唯一指标目录的兼容源。
struct GrpcMetricsSource {
    /// 已发布监听器与 TLS 事实的共享只读来源，不取得网络资源的关闭权。
    state: Arc<GrpcRuntimeState>,
}

impl nametrics_core::LegacyMetricsSource for GrpcMetricsSource {
    /// 业务作用：返回受管 listener 固定 family 目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期登记并用于结构化样本校验的全部 listener descriptor。
    fn descriptors(&self) -> &'static [&'static nametrics_core::MetricDescriptor] {
        &GRPC_DESCRIPTORS
    }

    /// 业务作用：读取 listener、TLS 和方法计数的观察视图，并映射为统一结构化样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：listener 已在 Ready 发布时返回连接、TLS 与 descriptor 固定方法样本；尚未绑定时返回空样本集，
    /// 表示本源支持结构化出口但当前无数据，不回落到自渲染。
    /// 连接字段使用一次 listener 快照；TLS 与 RPC 另行读取，不保证全部指标来自同一原子时刻。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        let Some(observer) = self.state.observer_if_published() else {
            return Some(Vec::new());
        };
        // listener 六个值必须来自同一次快照：分别读取会导出"受理数已增、在途仍为旧值"这类
        // 不存在的组合，运维按它做容量判断会得到错误结论。
        let snapshot = observer.snapshot();
        let mut samples = vec![
            grpc_gauge(
                GRPC_SERVING.name,
                f64::from(u8::from(snapshot.state == nagrpc::GrpcServerState::Running)),
            ),
            grpc_gauge(
                GRPC_CONNECTIONS_ACTIVE.name,
                snapshot.active_connections as f64,
            ),
            grpc_counter(GRPC_CONNECTIONS_ACCEPTED.name, snapshot.accepted_total),
            grpc_counter(GRPC_ACCEPT_FAILURES.name, snapshot.accept_failures_total),
            grpc_gauge(
                GRPC_ACCEPT_CONSECUTIVE_FAILURES.name,
                snapshot.consecutive_accept_failures as f64,
            ),
            grpc_gauge(
                GRPC_ACCEPT_STALL_SECONDS.name,
                snapshot.accept_stall_millis as f64 / 1_000_f64,
            ),
        ];
        if let Some(expiry) = self.state.tls_certificate_expiry_timestamp() {
            samples.push(grpc_gauge(
                GRPC_TLS_CERTIFICATE_EXPIRY_TIMESTAMP_SECONDS.name,
                expiry as f64,
            ));
        }
        for rpc in observer.rpc_snapshot() {
            let labels = vec![
                ("service", rpc.service.to_owned()),
                ("method", rpc.method.to_owned()),
                ("rpc_type", rpc.rpc_type.label().to_owned()),
            ];
            samples.push(grpc_labeled_gauge(
                GRPC_RPCS_ACTIVE.name,
                labels.clone(),
                rpc.active as f64,
            ));
            samples.push(grpc_labeled_counter(
                GRPC_RPCS_STARTED_TOTAL.name,
                labels.clone(),
                rpc.started_total,
            ));
            for (reason, count) in rpc.rejections {
                let mut rejection_labels = labels.clone();
                rejection_labels.push(("reason", reason.label().to_owned()));
                samples.push(grpc_labeled_counter(
                    GRPC_RPCS_REJECTED_TOTAL.name,
                    rejection_labels,
                    count,
                ));
            }
            for (outcome, count) in rpc.outcomes {
                let mut outcome_labels = labels.clone();
                outcome_labels.push(("outcome", outcome.label().to_owned()));
                samples.push(grpc_labeled_counter(
                    GRPC_RPCS_COMPLETED_TOTAL.name,
                    outcome_labels,
                    count,
                ));
            }
        }
        Some(samples)
    }

    /// 业务作用：保留旧 trait 入口；本源始终提供结构化快照，文本出口由统一 hub 渲染。
    ///
    /// 参数说明：
    /// - `_output`: 兼容 trait 的文本缓冲区；本源不直接写入。
    ///
    /// 返回：无；两个出口共用同一份 `snapshot()` 结果。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：构造一个无 label 的 listener 当前状态样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `value`: 当前状态值。
///
/// 返回：不含地址、对端或业务 service 名的 gauge 样本。
fn grpc_gauge(name: &'static str, value: f64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Gauge(value),
    }
}

/// 业务作用：构造一个无 label 的 listener 单调计数样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `value`: 当前累计值。
///
/// 返回：不含地址、对端或业务 service 名的 counter 样本。
fn grpc_counter(name: &'static str, value: u64) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels: Vec::new(),
        value: nametrics_core::MetricValue::Counter(value),
    }
}

/// 业务作用：构造 descriptor 固定维度的方法级 gauge 样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `labels`: 由 sealed descriptor 派生的 service、method 与 RPC 形态。
/// - `value`: 当前瞬时值。
///
/// 返回：不含业务输入或对端身份的 gauge 样本。
fn grpc_labeled_gauge(
    name: &'static str,
    labels: Vec<(&'static str, String)>,
    value: f64,
) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels,
        value: nametrics_core::MetricValue::Gauge(value),
    }
}

/// 业务作用：构造 descriptor 固定维度的方法级 counter 样本。
///
/// 参数说明：
/// - `name`: 已登记 family 名。
/// - `labels`: 由 sealed descriptor 与封闭结局派生的低基数标签。
/// - `value`: 单调累计值。
///
/// 返回：不含业务输入或动态 status 文本的 counter 样本。
fn grpc_labeled_counter(
    name: &'static str,
    labels: Vec<(&'static str, String)>,
    value: u64,
) -> nametrics_core::MetricSample {
    nametrics_core::MetricSample {
        name,
        labels,
        value: nametrics_core::MetricValue::Counter(value),
    }
}

/// UserHook registry、Ready 发布与运行期观察共用的单实例状态。
pub(crate) struct GrpcRuntimeState {
    /// 串行化业务服务登记与一次性封口，封口后不允许补登记。
    registry: Mutex<GrpcRegistryState>,
    /// 预绑定成功后只发布一次的观察视图，保留到应用状态释放。
    published: OnceLock<GrpcPublishedRuntime>,
}

/// Ready 一次性发布的 listener 观察面与可选 TLS 到期事实。
struct GrpcPublishedRuntime {
    /// 已预绑定监听器的地址、接流状态及计数，不包含激活或停机权限。
    observer: nagrpc::GrpcServerObserver,
    /// 首次发布的证书到期 Unix 秒，明文模式为空。
    tls_certificate_expiry_timestamp: Option<u64>,
    /// 存在时优先从当前握手身份读取证书到期值。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    tls_source: Option<Arc<crate::saga::security::ManagedGrpcTlsSource>>,
    /// 服务发现 metadata 使用的实际 TLS 模式。
    #[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
    tls_mode: GrpcTlsMode,
    /// 服务发现公开的可选证书 DNS 或 IP 身份。
    #[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
    authority: Option<String>,
}

/// 服务发现注册阶段读取的已发布 gRPC endpoint 事实。
#[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
pub(crate) struct GrpcEndpointRegistration {
    /// listener 实际绑定的端口，包含 `port=0` 的内核分配结果。
    pub(crate) port: u16,
    /// provider metadata 使用的封闭 TLS 模式。
    pub(crate) tls_mode: &'static str,
    /// 优先使用显式 DNS/IP authority；未指定时由绑定地址或 resolver 的注册 IP 补齐。
    pub(crate) authority: Option<String>,
}

/// 服务登记权的单向状态，封口后的 registry 由组件独占。
enum GrpcRegistryState {
    /// 仍允许接收符合 descriptor 合同的业务服务。
    Open(nagrpc::ManagedServiceRegistry),
    /// 已将 registry 移出，不再接受登记或重复消费。
    Taken,
}

impl GrpcRuntimeState {
    /// 业务作用：创建 service 登记入口开放、listener 尚未发布的状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：允许多个唯一 service 登记且只允许一次 registry 消费和观察句柄发布的共享状态。
    pub(crate) fn new() -> Self {
        Self {
            registry: Mutex::new(GrpcRegistryState::Open(
                nagrpc::ManagedServiceRegistry::new(),
            )),
            published: OnceLock::new(),
        }
    }

    /// 业务作用：在 UserHook 把 generated server 登记到 Application 唯一 gRPC registry。
    ///
    /// 参数说明：
    /// - `service`: 统一 codegen 生成且尚未加入任何 Router 的 server。
    ///
    /// 返回：入口开放且 service 合同唯一、合法时成功；晚到或冲突登记返回 UserHook 错误。
    pub(crate) fn register<S>(&self, service: S) -> ApplicationResult<()>
    where
        S: nagrpc::ManagedGrpcService,
    {
        let mut state = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            GrpcRegistryState::Open(registry) => registry.register(service).map_err(|error| {
                grpc_error_src(
                    ApplicationPhase::UserHook,
                    "gRPC service does not satisfy the managed registry contract",
                    error,
                )
            }),
            GrpcRegistryState::Taken => Err(grpc_error(
                ApplicationPhase::Prepare,
                "gRPC service registration is closed",
            )),
        }
    }

    /// 业务作用：接收组合计划已经类型擦除的 generated service，使 Saga 等组件复用同一 registry。
    ///
    /// 参数说明：
    /// - `service`: 仍由 UserHook 独占、尚未加入 Router 的 boxed managed service。
    ///
    /// 返回：登记窗口开放且 service 身份唯一时成功；晚到、重复或 descriptor 不合法时返回阶段错误。
    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
    pub(crate) fn register_boxed(
        &self,
        service: Box<dyn nagrpc::ManagedGrpcService>,
    ) -> ApplicationResult<()> {
        let mut state = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut *state {
            GrpcRegistryState::Open(registry) => {
                registry.register_boxed(service).map_err(|error| {
                    grpc_error_src(
                        ApplicationPhase::UserHook,
                        "gRPC service does not satisfy the managed registry contract",
                        error,
                    )
                })
            }
            GrpcRegistryState::Taken => Err(grpc_error(
                ApplicationPhase::Prepare,
                "gRPC service registration is closed",
            )),
        }
    }

    /// 业务作用：在 Prepare 永久关闭 service 登记入口并取得唯一 registry。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次消费返回完整 registry，包括允许后续 health-only 判定的空集合；重复消费时拒绝启动。
    fn take_registry(&self) -> ApplicationResult<nagrpc::ManagedServiceRegistry> {
        let mut state = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 在移交 registry 前永久封口，后续 Prepare 失败也不能让晚到服务进入已冻结目录。
        let previous = std::mem::replace(&mut *state, GrpcRegistryState::Taken);
        match previous {
            GrpcRegistryState::Open(registry) => Ok(registry),
            GrpcRegistryState::Taken => Err(grpc_error(
                ApplicationPhase::Prepare,
                "gRPC service registry was already consumed",
            )),
        }
    }

    /// 业务作用：预绑定后发布只读 listener 观察句柄，不向观察者授予接流或关闭权限。
    ///
    /// 参数说明：
    /// - `observer`: 不含 shutdown 权限的地址、状态与连接计数视图。
    /// - `tls_mode`: Ready 阶段实际冻结并用于 listener 的 TLS 模式。
    /// - `tls_certificate_expiry_timestamp`：首次准备的 TLS 链最早到期 Unix 秒时间戳；明文模式为 None。
    /// - `authority`: 服务发现发布的可选 TLS DNS/IP 身份。
    /// - `tls_source`：启用 Saga gRPC 时持有的可轮换身份源，供观察当前证书到期时间；不向观察者开放私钥或轮换权。
    ///
    /// 返回：首次写入只读发布槽时成功，重复发布返回 Ready 错误；发布不等于 listener 已激活或应用已进入 Running。
    /// 不再复核地址或 TLS 配置，不持有 listener 的关闭权，失败后的网络清理由调用方负责。
    fn publish(
        &self,
        observer: nagrpc::GrpcServerObserver,
        tls_mode: GrpcTlsMode,
        tls_certificate_expiry_timestamp: Option<u64>,
        authority: Option<String>,
        #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))] tls_source: Option<
            Arc<crate::saga::security::ManagedGrpcTlsSource>,
        >,
    ) -> ApplicationResult<()> {
        #[cfg(not(any(feature = "nacos-discovery", feature = "discovery-runtime")))]
        let _ = (tls_mode, authority);
        self.published
            .set(GrpcPublishedRuntime {
                observer,
                tls_certificate_expiry_timestamp,
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                tls_source,
                #[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
                tls_mode,
                #[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
                authority,
            })
            .map_err(|_| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC listener observer was already published",
                )
            })
    }

    /// 业务作用：仅在 Ready 已发布 listener 时返回观察句柄，供指标源区分"未绑定"与"零流量"。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已发布时返回克隆句柄；尚未绑定时返回 `None`，调用方应导出空样本集而不是零值。
    fn observer_if_published(&self) -> Option<nagrpc::GrpcServerObserver> {
        self.published
            .get()
            .map(|published| published.observer.clone())
    }

    /// 业务作用：读取已发布 TLS 身份的最早到期事实，供证书监控使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份源可轮换时读取其当前 Unix 秒时间戳，否则使用首次发布值；明文或尚未发布为 None。
    /// 不检查 listener 的当前接流状态，停机后仍可读取已发布的证书事实。
    fn tls_certificate_expiry_timestamp(&self) -> Option<u64> {
        self.published.get().and_then(|published| {
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            if let Some(source) = &published.tls_source {
                return Some(source.expiry_timestamp());
            }
            published.tls_certificate_expiry_timestamp
        })
    }

    /// 业务作用：向服务发现组件提供预绑定端口与 TLS 注册计划，外部发布必须等待 Running。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预绑定观察面存在时返回固定协议事实；尚未绑定时返回 None，本投影本身不证明可接流。
    #[cfg(any(feature = "nacos-discovery", feature = "discovery-runtime"))]
    pub(crate) fn endpoint_registration(&self) -> Option<GrpcEndpointRegistration> {
        self.published.get().map(|published| {
            let address = published.observer.local_addr();
            GrpcEndpointRegistration {
                port: address.port(),
                tls_mode: published.tls_mode.discovery_value(),
                authority: published
                    .authority
                    .clone()
                    .or_else(|| (!address.ip().is_unspecified()).then(|| address.ip().to_string())),
            }
        })
    }

    /// 业务作用：取得已由 Ready 阶段发布且不含停机权限的 listener 观察句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：listener 已绑定时返回共享观察句柄；尚未 Ready 时返回阶段错误。
    pub(crate) fn observer(&self) -> ApplicationResult<nagrpc::GrpcServerObserver> {
        self.observer_if_published().ok_or_else(|| {
            grpc_error(
                ApplicationPhase::Running,
                "gRPC listener is not published yet",
            )
        })
    }
}

/// 稳定受管 gRPC listener 组件。
pub(crate) struct GrpcComponent {
    /// Start 已验证、Ready 尚待预绑定的地址。
    bind: Option<SocketAddr>,
    /// Start 冻结、随后按方法目录应用收紧策略的监听配置。
    config: Option<nagrpc::GrpcServerConfig>,
    /// Prepare 独占取得、Ready 消费的服务目录。
    registry: Option<nagrpc::ManagedServiceRegistry>,
    /// 是否装配标准 reflection 服务。
    reflection: bool,
    /// 是否允许缺少业务服务而只提供健康入口。
    health_only: bool,
    /// Start 校验的 TLS 秘密引用与时间计划，Ready 才读取材料。
    tls: Option<GrpcTlsPlan>,
    /// 对外注册时使用的可选 DNS 或 IP 身份。
    authority: Option<String>,
    /// listener 的 readiness 独占贡献句柄。
    contributor: Option<ReadinessContributor>,
    /// TLS 开启时登记的证书 readiness 贡献句柄。
    tls_contributor: Option<ReadinessContributor>,
    /// 等待 Runner 统一放行的激活与持续监督任务，仅移交一次。
    critical_task: Option<ApplicationFuture<'static>>,
}

impl GrpcComponent {
    /// 业务作用：创建尚未读取配置、registry 和 listener 的组件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：等待 Start 校验配置、Prepare 消费 registry、Ready 绑定端口的组件。
    pub(crate) fn new() -> Self {
        Self {
            bind: None,
            config: None,
            registry: None,
            reflection: false,
            health_only: false,
            tls: None,
            authority: None,
            contributor: None,
            tls_contributor: None,
            critical_task: None,
        }
    }
}

impl ApplicationComponent for GrpcComponent {
    /// 业务作用：返回受管 listener 的稳定生命周期身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`grpc` 组件身份。
    fn id(&self) -> ComponentId {
        ComponentId::Grpc
    }

    /// 业务作用：从最终配置校验全部 listener 边界，并登记关键 readiness 贡献项。
    ///
    /// 参数说明：
    /// - `context`: 提供配置快照与共享 readiness 注册表的 Start 上下文。
    ///
    /// 返回：配置和贡献项合法时成功；端口、容量、消息或时间边界非法时拒绝启动。
    fn start<'a>(&'a mut self, context: &'a mut StartContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let root: GrpcConfigRoot = context.application().config_as()?;
            let validated = root.grpc.validate(ApplicationPhase::Start)?;
            self.contributor = Some(context.application().register_readiness(
                ComponentId::Grpc,
                Arc::<str>::from("grpc:listener"),
                ReadinessPolicy {
                    affects_ready: true,
                    failure_threshold: 1,
                    recovery_threshold: 1,
                    stale_after: Some(HEALTH_STALE_AFTER),
                },
            )?);
            if validated.tls.is_some() {
                self.tls_contributor = Some(context.application().register_readiness(
                    ComponentId::Grpc,
                    Arc::<str>::from("grpc:tls-certificate"),
                    ReadinessPolicy {
                        affects_ready: true,
                        failure_threshold: 1,
                        recovery_threshold: 1,
                        stale_after: Some(HEALTH_STALE_AFTER),
                    },
                )?);
            }
            self.bind = Some(validated.bind);
            self.config = Some(validated.config);
            self.reflection = validated.reflection;
            self.health_only = validated.health_only;
            self.tls = validated.tls;
            self.authority = validated.authority;
            Ok(())
        })
    }

    /// 业务作用：在 initializer 之前封口 service registry，确保后续装配不能越过生命周期边界。
    ///
    /// 参数说明：
    /// - `context`: 提供共享 Application 的 Prepare 上下文。
    ///
    /// 返回：首次取得完整 registry 时成功；重复消费时禁止进入业务初始化。
    fn prepare<'a>(&'a mut self, context: &'a mut PrepareContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let state = context.application().grpc_runtime();
            let registry = state.take_registry()?;
            let series =
                grpc_worst_case_metric_series(&registry, self.reflection, self.tls.is_some())?;
            context
                .application()
                .metrics_hub()
                .register_legacy_source_reserved(
                    Arc::new(GrpcMetricsSource {
                        state,
                    }),
                    series,
                )
                .map_err(|error| match error {
                    nametrics_core::MetricSourceRegistrationError::Conflict(conflict) => {
                        let message = format!(
                            "gRPC metric descriptor `{}` conflicts with an existing registration",
                            conflict.name
                        );
                        grpc_error(ApplicationPhase::Prepare, message)
                    }
                    nametrics_core::MetricSourceRegistrationError::SeriesBudgetExceeded => {
                        grpc_error(
                            ApplicationPhase::Prepare,
                            "gRPC metric series reservation exceeds the process budget",
                        )
                    }
                })?;
            self.registry = Some(registry);
            Ok(())
        })
    }

    /// 业务作用：装配受管路由、预绑定端口并建立停机所有权，接流许可由全应用屏障控制。
    ///
    /// 参数说明：
    /// - `context`: 提供共享 Application、启动剩余预算和 active stack 的 Ready 上下文。
    ///
    /// 返回：listener 为 Bound、观察句柄已发布且 shutdown action 已压栈时成功；registry、
    /// reflection、绑定或发布失败时完整关闭新 listener 后返回错误。
    fn ready<'a>(&'a mut self, context: &'a mut ReadyContext<'_>) -> ApplicationFuture<'a> {
        Box::pin(async move {
            let bind = self.bind.ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC bind address was not prepared during Start",
                )
            })?;
            let config = self.config.clone().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC server configuration was not prepared during Start",
                )
            })?;
            let registry = self.registry.take().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC service registry was not prepared before initialization",
                )
            })?;
            let tls = self
                .tls
                .as_ref()
                .map(|plan| prepare_tls_identity(plan, &context.application().secrets()))
                .transpose()?;
            let (tls_identity, tls_prepared) = match tls {
                Some((identity, prepared)) => (Some(identity), Some(prepared)),
                None => (None, None),
            };
            let mut plan = nagrpc::ServerPlan::with_config(config)
                .with_registry(registry)
                .reflection(self.reflection)
                .health_only(self.health_only);
            if let Some(identity) = tls_identity {
                // certificate 与 private key 已在 bind 前从同代 secret 快照取得并复验；
                // 每次握手固定一份完整身份，不在请求中混用两代材料。
                plan = plan.tls(identity);
            }
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            let tls_source = self
                .tls
                .as_ref()
                .map(|tls| crate::saga::security::grpc_tls_source(context.application(), tls))
                .transpose()?
                .flatten();
            #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
            if let Some(source) = &tls_source {
                plan = plan.tls_source(source.clone());
            }
            let (handle, activation) = plan.bind(bind).await.map_err(|error| {
                grpc_error_src(
                    ApplicationPhase::Ready,
                    "gRPC listener could not bind",
                    error,
                )
            })?;
            let observer = handle.observer();
            let contributor = self.contributor.clone().ok_or_else(|| {
                grpc_error(
                    ApplicationPhase::Ready,
                    "gRPC readiness contributor was not registered during Start",
                )
            })?;
            let tls_runtime = match (tls_prepared, self.tls_contributor.clone()) {
                (Some(prepared), Some(tls_contributor)) => Some(TlsCertificateRuntime {
                    expiry_timestamp: prepared.expiry_timestamp,
                    #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                    source: tls_source.clone(),
                    warning_window: prepared.warning_window,
                    contributor: tls_contributor,
                }),
                (None, None) => None,
                _ => {
                    let _ = handle.shutdown_with_timeout(context.remaining()).await;
                    return Err(grpc_error(
                        ApplicationPhase::Ready,
                        "gRPC TLS readiness state was not prepared consistently",
                    ));
                }
            };

            if let Err(error) = context.application().grpc_runtime().publish(
                observer.clone(),
                self.tls
                    .as_ref()
                    .map(|plan| plan.mode)
                    .unwrap_or(GrpcTlsMode::Disabled),
                tls_runtime.as_ref().map(|runtime| runtime.expiry_timestamp),
                self.authority.clone(),
                #[cfg(any(feature = "saga-grpc", feature = "saga-grpc-pgsql"))]
                tls_source,
            ) {
                let _ = handle.shutdown_with_timeout(context.remaining()).await;
                return Err(error);
            }
            contributor.observe(
                DependencyState::NotReady,
                reason::NOT_READY,
                std::time::Instant::now(),
            );
            if let Some(runtime) = &tls_runtime {
                runtime.observe()?;
            }
            let application = context.application().clone();
            let monitor_contributor = contributor.clone();
            let monitor_tls = tls_runtime.clone();
            self.critical_task = Some(Box::pin(async move {
                // 此主体只在 Application Ready 与统一执行许可发布后 poll；monitor 不再绕过其监督对象的准入门禁。
                activation.activate().await.map_err(|error| {
                    grpc_error_src(
                        ApplicationPhase::Running,
                        "gRPC listener could not activate",
                        error,
                    )
                })?;
                run_health_monitor(application, observer, monitor_contributor, monitor_tls).await
            }));
            // 地址和观察面成功发布后才压入 shutdown owner；此后任一失败都先停止 gRPC 准入，
            // 再按反向顺序释放 handler 依赖的数据库、消息 transport 与业务资源。
            context.activate(Box::new(GrpcShutdown {
                handle,
                contributor,
                tls_contributor: tls_runtime.map(|runtime| runtime.contributor),
            }));
            Ok(())
        })
    }

    /// 业务作用：把 listener 激活权与状态 monitor 一起移交 Runner，放行前不 accept，运行失败统一停机。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Ready 已建立 listener 时返回一次性关键任务，否则返回 `None`。
    fn take_critical_task(&mut self) -> Option<(&'static str, ApplicationFuture<'static>)> {
        self.critical_task
            .take()
            .map(|task| ("grpc-listener-monitor", task))
    }
}

/// 业务作用：周期复验 listener 所有权与接流进展，把持续 accept 失败摘流并将所有权丢失升级为关键失败。
///
/// 参数说明：
/// - `application`: 区分正常停机与运行期失权的共享容器。
/// - `observer`: 不含取消权的 listener 状态视图。
/// - `contributor`: `grpc:listener` readiness 的独占更新句柄。
/// - `tls`：可选的证书时限监督状态，包含当前身份源与独立 TLS readiness 贡献；明文监听器不提供该项。
///
/// 返回：观察到应用停止或失败状态时将 listener 标记为 NotReady 并成功退出；接流停滞时摘流、做本机 TCP 探测并继续观察。
/// 运行期 listener 不再 Running，或 TLS 监督失败时返回错误，由 Runner 承担停止准入与清理责任。
/// 周期等待本身不监听取消信号，正常停机状态在下一轮观察。
async fn run_health_monitor(
    application: Application,
    observer: nagrpc::GrpcServerObserver,
    contributor: ReadinessContributor,
    tls: Option<TlsCertificateRuntime>,
) -> ApplicationResult<()> {
    loop {
        if matches!(
            application.state(),
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
        ) {
            contributor.observe(
                DependencyState::NotReady,
                reason::NOT_READY,
                std::time::Instant::now(),
            );
            return Ok(());
        }
        if let Some(tls) = &tls {
            // 证书到期后即使 socket 仍在，也必须先摘流并上报关键任务失败，
            // Runner 才能使用新 secret 重建进程，而不是让旧身份继续对外接流。
            tls.observe()?;
        }
        match observer.state() {
            nagrpc::GrpcServerState::Running => {
                if observer
                    .accept_failure_duration()
                    .is_some_and(|elapsed| elapsed >= ACCEPT_FAILURE_NOT_READY_AFTER)
                {
                    // listener 仍由 serve 任务持有时不能终止它；先从业务路由摘流并持续观察，
                    // 成功 accept 会清零连续失败计数，下一轮即可恢复 Ready。
                    contributor.observe(
                        DependencyState::NotReady,
                        reason::GRPC_ACCEPT_STALLED,
                        std::time::Instant::now(),
                    );
                    // 摘流后外部负载均衡可能不再建立新连接；主动发起一次本机 TCP 探测，
                    // 资源恢复时促成成功 accept 并清除连续失败状态，避免恢复依赖业务流量。
                    let probe =
                        tokio::net::TcpStream::connect(local_probe_address(observer.local_addr()));
                    let _ = tokio::time::timeout(ACCEPT_RECOVERY_PROBE_TIMEOUT, probe).await;
                } else {
                    contributor.observe(
                        DependencyState::Ready,
                        reason::HEALTHY,
                        std::time::Instant::now(),
                    );
                }
            }
            nagrpc::GrpcServerState::Bound
            | nagrpc::GrpcServerState::Draining
            | nagrpc::GrpcServerState::Closed
            | nagrpc::GrpcServerState::Failed => {
                contributor.observe(
                    DependencyState::NotReady,
                    reason::GRPC_LISTENER_UNAVAILABLE,
                    std::time::Instant::now(),
                );
                return Err(grpc_error(
                    ApplicationPhase::Running,
                    "gRPC listener lost serving ownership",
                ));
            }
        }
        tokio::time::sleep(HEALTH_INTERVAL).await;
    }
}

/// 业务作用：把 listener 的本机绑定地址转换为可主动连接的恢复探测地址。
///
/// 参数说明：
/// - `address`: listener 实际绑定地址，可能使用 IPv4 或 IPv6 未指定地址。
///
/// 返回：保留端口与地址族；未指定 IP 映射为对应 loopback，其余地址保持不变。
fn local_probe_address(mut address: SocketAddr) -> SocketAddr {
    if address.ip().is_unspecified() {
        address.set_ip(match address.ip() {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        });
    }
    address
}

/// 停机 action 独占 gRPC listener 的取消和 join 权限。
struct GrpcShutdown {
    /// 唯一拥有停止接流与等待网络任务退出权限的句柄。
    handle: nagrpc::GrpcServerHandle,
    /// 停机时先标记不可用的 listener 贡献。
    contributor: ReadinessContributor,
    /// 存在时随停机一同标记不可用的证书贡献。
    tls_contributor: Option<ReadinessContributor>,
}

impl ShutdownAction for GrpcShutdown {
    /// 业务作用：返回停机报告使用的稳定 action 名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含地址或业务 service 名的固定标签。
    fn label(&self) -> &'static str {
        "grpc-listener"
    }

    /// 业务作用：先把 listener 摘出 Ready，再在全局剩余预算内停止准入并排空在途 RPC。
    ///
    /// 参数说明：
    /// - `context`: 提供全部 active step 共用的绝对停机 deadline。
    ///
    /// 返回：serve task 在子预算内完整退出时成功；排空超时或 serve 失败时返回可归因停机错误。
    fn shutdown<'a>(&'a mut self, context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        Box::pin(async move {
            self.contributor.observe(
                DependencyState::NotReady,
                reason::NOT_READY,
                std::time::Instant::now(),
            );
            if let Some(contributor) = &self.tls_contributor {
                contributor.observe(
                    DependencyState::NotReady,
                    reason::NOT_READY,
                    std::time::Instant::now(),
                );
            }
            self.handle
                .shutdown_with_timeout(context.child_budget(Duration::MAX))
                .await
                .map_err(|error| {
                    grpc_error_src(
                        ApplicationPhase::Stopping,
                        "gRPC listener did not drain cleanly",
                        error,
                    )
                })
        })
    }
}

/// 业务作用：把受管默认时长投影为配置毫秒且保持上界可表示。
///
/// 参数说明：
/// - `duration`: nagrpc 默认 server 时长。
///
/// 返回：不超过 `u64` 的毫秒值。
fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// 业务作用：在不创建 Router、socket 或任务的前提下校验候选 `grpc` 配置段。
///
/// 参数说明：
/// - `tree`: 合并、插值完成但尚未发布的候选配置树。
/// - `phase`: 本轮无副作用校验所属生命周期阶段。
///
/// 返回：配置段缺失或全部字段合法时成功；反序列化、地址或边界非法时拒绝候选快照。
pub(crate) fn validate_grpc_section(
    tree: &serde_json::Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    let Some(section) = tree.get("grpc") else {
        return Ok(());
    };
    let settings: GrpcSettings = serde_json::from_value(section.clone())
        .map_err(|error| grpc_error_src(phase, "invalid `grpc` configuration section", error))?;
    settings.validate(phase).map(|_| ())
}

/// 业务作用：从本地最终配置计算 gRPC 组件图要求的最小全局停机预算。
///
/// 参数说明：
/// - `tree`: 已完成本地合并与占位符解析、尚未产生网络副作用的配置树。
/// - `has_discovery`: 组件图是否会在 gRPC 之前先执行注册中心摘流。
///
/// 返回：覆盖 provider 注销、完整 listener drain 与尾部收割的预算；gRPC 配置非法时在 preflight 拒绝。
pub(crate) fn minimum_shutdown_timeout(
    tree: &serde_json::Value,
    has_discovery: bool,
) -> ApplicationResult<Duration> {
    let settings = tree
        .get("grpc")
        .cloned()
        .map(serde_json::from_value::<GrpcSettings>)
        .transpose()
        .map_err(|error| {
            grpc_error_src(
                ApplicationPhase::Bootstrap,
                "invalid `grpc` configuration section",
                error,
            )
        })?
        .unwrap_or_default();
    let config = settings.validate(ApplicationPhase::Bootstrap)?.config;
    let discovery = if has_discovery {
        GRPC_DISCOVERY_DEREGISTER_RESERVE
    } else {
        Duration::ZERO
    };
    Ok(discovery
        .saturating_add(config.drain_timeout)
        .saturating_add(GRPC_SHUTDOWN_TAIL_RESERVE))
}

/// 业务作用：创建不包含绑定地址或业务 service 名的 gRPC 生命周期错误。
///
/// 参数说明：
/// - `phase`: 错误被裁决时的 Application 生命周期阶段。
/// - `message`: 稳定配置或所有权摘要。
///
/// 返回：归属 `grpc` 组件的统一错误。
fn grpc_error(phase: ApplicationPhase, message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ComponentId::Grpc, phase, message)
}

/// 业务作用：创建保留底层错误链且不把动态细节拼入稳定摘要的 gRPC 错误。
///
/// 参数说明：
/// - `phase`: 错误被裁决时的 Application 生命周期阶段。
/// - `message`: 不含地址、证书或业务 service 名的稳定摘要。
/// - `source`: 仅供统一诊断通道处理的底层错误。
///
/// 返回：归属 `grpc` 组件并保留 source 的统一错误。
fn grpc_error_src(
    phase: ApplicationPhase,
    message: impl Into<String>,
    source: impl Into<anyhow::Error>,
) -> ApplicationError {
    ApplicationError::with_source(ComponentId::Grpc, phase, message, source)
}
