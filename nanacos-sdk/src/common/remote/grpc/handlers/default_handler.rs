use crate::{
    common::remote::grpc::nacos_grpc_service::ServerRequestHandler, nacos_proto::v2::Payload,
};
use async_trait::async_trait;
use tracing::info;

pub(crate) struct DefaultHandler;

#[async_trait]
impl ServerRequestHandler for DefaultHandler {
    /// 业务作用：记录没有专用处理器的服务端请求并停止分派，避免把未知推送当作可执行操作。
    ///
    /// 参数说明：
    /// - `request`：无法匹配已知处理器的 Payload；其内容不构成可信业务指令。
    ///
    /// 返回：始终为空，不向服务端声明业务执行成功或创建新的处理规则。
    async fn request_reply(&self, request: Payload) -> Option<Payload> {
        // 未知推送的正文、类型和请求头可能包含配置或凭据，只记录规模，不执行字符串转换。
        let payload_bytes = request.body.as_ref().map_or(0, |body| body.value.len());
        let header_count = request
            .metadata
            .as_ref()
            .map_or(0, |metadata| metadata.headers.len());
        info!(
            payload_bytes,
            header_count, "Nacos gRPC 未知服务端请求已忽略"
        );
        None
    }
}
