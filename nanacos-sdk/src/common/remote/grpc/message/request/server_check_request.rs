use nacos_macro::request;

use crate::common::remote::generate_request_id;

#[request(identity = "ServerCheckRequest", module = "internal")]
pub(crate) struct ServerCheckRequest {}

impl ServerCheckRequest {
    /// 业务作用：建立带关联标识的服务端检查请求，用于连接建立阶段确认远端响应。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：已分配本地请求序号的消息，尚未发送或证明服务端健康。
    pub(crate) fn new() -> Self {
        let request_id = Some(generate_request_id());

        Self {
            request_id,
            ..Default::default()
        }
    }
}
