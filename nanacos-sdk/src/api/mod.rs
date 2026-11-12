/// SDK 默认值及受支持的环境变量键。
pub mod constants;
/// SDK 操作错误及结果类型。
pub mod error;
/// 客户端连接、身份与配置属性。
pub mod props;

/// 客户端认证、配置过滤与加解密的扩展接口。
pub mod plugin;

/// 配置查询、发布及监听的公开接口，仅在启用 config 时提供。
#[cfg(feature = "config")]
pub mod config;

/// 服务实例注册、发现及订阅的公开接口，仅在启用 naming 时提供。
#[cfg(feature = "naming")]
pub mod naming;
