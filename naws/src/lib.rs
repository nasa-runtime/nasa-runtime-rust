//! TCP/WebSocket 长连接消息中心。
//!
//! 提供鉴权、会话注册、事件分发、心跳、fan-out、端点管理和可选 Redis 集群广播。
// ============================================================================
// rust-ws —— 纯 Rust 长连接消息中心(对照 原实现 nasa-netty)。
// R0.2/R0.3:单机 TCP server + 鉴权 + 路由授权 + fan-out + 心跳。
// WS(R0.4)/ 客户端(R0.5)/ 集群(R0.6)按路线图后续加入。架构说明。
// ============================================================================

/// 鉴权回调、路由授权策略和只读连接上下文。
pub mod auth;
/// socket.io 与原生 payload 的转换桥。
pub mod bridge;
/// 原生 TCP 客户端 SDK,用于压测、内部调用和端到端验证。
pub mod client;
/// 跨节点广播、presence、fencing 和 notifier 抽象。
pub mod cluster;
/// endpoint 注册表、生命周期 hook 和业务事件 handler。
pub mod endpoint;
/// 会话 fan-out 发送器。
pub mod sender;
/// TCP/WebSocket server builder 与运行句柄。
pub mod server;
/// 会话状态、索引表、出站队列和会话句柄。
pub mod session;
/// NASA frame 编解码器和帧类型常量。
pub mod wire;

/// 消息队列承载的长连接少拷贝数据面适配器。
#[cfg(feature = "kafka")]
pub mod kafka;

/// socket.io / engine.io 兼容层(R0.7,feature = "socketio")。步骤1:包编解码 + NASA 映射。
#[cfg(feature = "socketio")]
pub mod socketio;

// 常用类型再导出。
pub use auth::{
    allow_all, deny_all, AuthContext, AuthResult, InboundPolicy, PolicyContext, RouteDecision,
};
#[cfg(feature = "socketio")]
pub use bridge::Base64Bridge;
pub use bridge::{default_bridge, JsonPassthrough, PayloadBridge};
pub use client::{Client, ClientBuilder, ClientError};
pub use cluster::{
    Cluster, ClusterDataPublisher, ClusterSourceRef, ClusterStats, DataPublishOutcome, Incarnation,
    IncarnationError, NodeRegistry, Notifier, PublishOutcome, StartError,
};
pub use endpoint::{Endpoint, EndpointBuilder, EndpointRegistry, EventHandler, MAX_ENDPOINT_LEN};
pub use sender::{BorrowedSendResult, SendReport, Sender};
pub use server::{
    BuildError, RunningServer, Server, ServerBuilder, ServerConfig, MAX_REASON_LEN, MIN_MAX_FRAME,
};
pub use session::{
    BackpressurePolicy, GroupDelta, Protocol, SendOutcome, Session, SessionHandle, SessionRegistry,
    Transport,
};
pub use wire::{encode_event_frame_ref, encode_frame, frame_type, wire_mode, Frame, FrameCodec};

/// 协议模型再导出,供业务构造 Message 和控制帧 payload。
pub mod proto {
    pub use naws_proto::*;
}
