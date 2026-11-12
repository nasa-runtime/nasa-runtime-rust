use nacos_macro::response;

/// 客户端针对配置变化通知返回的协议回执。
#[response(identity = "ConfigChangeNotifyResponse", module = "config")]
pub(crate) struct ConfigChangeNotifyResponse {}

impl ConfigChangeNotifyResponse {
    /// 业务作用：把通知回执绑定到服务端请求，便于对端匹配本次通知。
    ///
    /// 参数说明：
    /// - `request_id`：原通知的请求标识，原样附加到回执。
    ///
    /// 返回：带关联标识的回执，不改变结果码或发送网络消息。
    pub(crate) fn request_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }
}
