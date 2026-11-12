use nacos_macro::response;

/// 服务端配置删除结果，传输成功后仍需检查业务结果码。
#[response(identity = "ConfigRemoveResponse", module = "config")]
pub(crate) struct ConfigRemoveResponse {}
