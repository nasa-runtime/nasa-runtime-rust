use nacos_macro::request;

/// 服务端通知配置变化的控制请求，客户端须据此重新查询正文。
#[request(identity = "ConfigChangeNotifyRequest", module = "config")]
pub(crate) struct ConfigChangeNotifyRequest {}
