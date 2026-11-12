//! Nacos SDK 配置与服务发现模块共享的内部基础能力。
//!
//! 包含缓存、错误转换、后台任务与远端连接；各模块分别承担以下职责：
//!
//! - `cache`：维护配置及发现结果的磁盘缓存，缓存不替代远端当前事实。
//! - `error`：把业务响应状态转换为 SDK 错误。
//! - `executor`：共享异步运行时及定时任务调度。
//! - `remote`：管理 Nacos 地址列表、gRPC 连接与通信消息。
//! - `util`：客户端标识生成、参数检查与无序快照比较。
//! - `log`：启用 tracing-log 后提供的可选日志初始化。

pub(crate) mod cache;
pub(crate) mod error;
pub(crate) mod executor;
pub(crate) mod remote;
pub(crate) mod util;

#[cfg(feature = "tracing-log")]
pub(crate) mod log;
