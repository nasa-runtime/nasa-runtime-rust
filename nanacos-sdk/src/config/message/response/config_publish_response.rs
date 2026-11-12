use nacos_macro::response;

/// 服务端配置发布结果，是否成功由协议结果码决定。
#[response(identity = "ConfigPublishResponse", module = "config")]
pub(crate) struct ConfigPublishResponse {}
