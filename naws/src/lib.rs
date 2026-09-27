//! TCP/WebSocket 长连接消息中心。
//!
//! 提供鉴权、路由授权、会话注册、事件分发、心跳、fan-out、有界出站队列、排空和可选集群 transport。
//! 业务负责实现身份校验、endpoint 与 handler；server 负责连接生命周期和资源边界。
//! `Client` 使用原生 TCP 帧协议，提供首连取消、激活门禁及全部连接子任务退出屏障。
//! Application 的 `ws-client` feature 可按命名配置管理该客户端，无需入站 listener。
//!
//! # 出站 Client 的运行架构
//!
//! 首连在同一超时内完成 TCP 与 AUTH；Service 的发送和业务回调等待与宿主共用的启动许可，
//! Batch 在工作负载前开放发送。Ready 前的本地状态保护覆盖关键认证连接与任务责任，
//! 不能同步检测远端隔离。关键 Client 断连时为 NotReady，可选 Client 为 Degraded，重新认证后恢复。
//! 重连先等待旧 writer 与 heartbeat；关闭先撤销准入，再等待全部子任务退出，晚到认证不能重新开放。
//! `send_message` 成功只表示本地有界队列接受，不证明远端送达。Client 不支持 ws/wss、TLS 或业务重放。

/// 鉴权回调、路由授权策略和只读连接上下文。
pub mod auth;
/// socket.io 与原生 payload 的转换桥。
pub mod bridge;
/// 原生 TCP 客户端 SDK，用于业务连接和协议交互。
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

/// socket.io / engine.io 兼容层(feature = "socketio")，负责包编解码与 NASA 映射。
#[cfg(feature = "socketio")]
pub mod socketio;

// 常用类型再导出。
pub use auth::{
    allow_all, deny_all, AuthContext, AuthResult, InboundPolicy, PolicyContext, RouteDecision,
};
#[cfg(feature = "socketio")]
pub use bridge::Base64Bridge;
pub use bridge::{default_bridge, JsonPassthrough, PayloadBridge};
pub use client::{Client, ClientBuilder, ClientError, ClientFailure, ClientObservation};
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
