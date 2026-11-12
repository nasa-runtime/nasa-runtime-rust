use std::time::Duration;

use reqwest::header::HeaderValue;
use tonic::transport::Uri;

/// 构造 gRPC 端点所需的目标、流控及保活设置；未指定的可选项沿用传输层默认值。
#[derive(Clone)]
pub(crate) struct GrpcConfiguration {
    /// 目标主机或已含端口的 authority 文本。
    pub(crate) host: String,
    /// 显式端口；存在时追加到 host 后构造 authority。
    pub(crate) port: Option<u16>,
    /// 请求使用的可选 origin，不替代实际连接地址。
    pub(crate) origin: Option<Uri>,
    /// 客户端请求附带的 User-Agent 头值。
    pub(crate) user_agent: Option<HeaderValue>,
    /// 单次请求等待传输响应的超时，不等于建立连接的超时。
    pub(crate) timeout: Option<Duration>,
    /// 端点同时处理的请求数量上限。
    pub(crate) concurrency_limit: Option<usize>,
    /// 每个指定时间窗口允许的请求数。
    pub(crate) rate_limit: Option<(u64, Duration)>,
    /// HTTP/2 单流初始接收窗口字节数。
    pub(crate) init_stream_window_size: Option<u32>,
    /// HTTP/2 整条连接初始接收窗口字节数。
    pub(crate) init_connection_window_size: Option<u32>,
    /// TCP 保活间隔；为空时关闭端点配置的 TCP 保活。
    pub(crate) tcp_keepalive: Option<Duration>,
    /// 是否禁用 TCP Nagle 聚合，控制小包发送延迟。
    pub(crate) tcp_nodelay: bool,
    /// HTTP/2 保活探测间隔。
    pub(crate) http2_keep_alive_interval: Option<Duration>,
    /// HTTP/2 保活探测等待响应的最长时长。
    pub(crate) http2_keep_alive_timeout: Option<Duration>,
    /// 没有活动请求时是否继续进行 HTTP/2 保活。
    pub(crate) http2_keep_alive_while_idle: Option<bool>,
    /// 建立底层连接的最长等待时长。
    pub(crate) connect_timeout: Option<Duration>,
    /// 是否启用 HTTP/2 接收窗口自适应调整。
    pub(crate) http2_adaptive_window: Option<bool>,
}

#[allow(dead_code)]
impl GrpcConfiguration {
    /// 业务作用：为后续 gRPC 端点构造设置可选 origin。
    ///
    /// 参数说明：
    /// - `uri`：待解析的 origin URI 文本，是否完整适用于端点由后续构造过程决定。
    ///
    /// 返回：解析成功时更新 origin，失败时保留原配置；两种情况均返回配置对象，不发起连接。
    pub(crate) fn with_origin(mut self, uri: &str) -> Self {
        if let Ok(uri) = uri.parse::<Uri>() {
            self.origin = Some(uri);
        }
        self
    }

    /// 业务作用：设置客户端发起 gRPC 请求时使用的 User-Agent 头值。
    ///
    /// 参数说明：
    /// - `ua`：待转换为 HTTP 头值的文本，不允许非法头字符。
    ///
    /// 返回：转换成功时替换头值；失败时保留原值并返回配置，不产生错误结果或网络副作用。
    pub(crate) fn with_user_agent(mut self, ua: String) -> Self {
        if let Ok(ua) = HeaderValue::try_from(ua) {
            self.user_agent = Some(ua);
        }
        self
    }
}

impl Default for GrpcConfiguration {
    /// 业务作用：建立 gRPC 传输配置的初始值，供客户端构建器覆盖目标和连接限制。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：默认环回主机、未指定端口的配置；HTTP/2 保活间隔为 360 秒、等待为 20 秒，其它可选限制未设置，不建立连接。
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: Default::default(),
            origin: Default::default(),
            user_agent: Default::default(),
            timeout: Default::default(),
            concurrency_limit: Default::default(),
            rate_limit: Default::default(),
            init_stream_window_size: Default::default(),
            init_connection_window_size: Default::default(),
            tcp_keepalive: Default::default(),
            tcp_nodelay: Default::default(),
            http2_keep_alive_interval: Some(Duration::from_secs(360)),
            http2_keep_alive_timeout: Some(Duration::from_secs(20)),
            http2_keep_alive_while_idle: Default::default(),
            connect_timeout: Default::default(),
            http2_adaptive_window: Default::default(),
        }
    }
}
