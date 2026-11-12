use crate::{
    common::remote::grpc::{
        message::{
            GrpcMessage, GrpcMessageBuilder, request::ClientDetectionRequest,
            response::ClientDetectionResponse,
        },
        nacos_grpc_service::ServerRequestHandler,
    },
    nacos_proto::v2::Payload,
};
use async_trait::async_trait;
use tracing::{debug, error};

pub(crate) struct ClientDetectionRequestHandler;

#[async_trait]
impl ServerRequestHandler for ClientDetectionRequestHandler {
    /// 业务作用：响应服务端发起的客户端存活探测，沿用请求标识返回标准探测成功消息。
    ///
    /// 参数说明：
    /// - `request`：双向流收到的探测 Payload，须能按 ClientDetectionRequest 解码。
    ///
    /// 返回：请求可解析且响应可编码时返回回复；任一步失败返回空，不伪造探测成功。
    async fn request_reply(&self, request: Payload) -> Option<Payload> {
        let request_message = GrpcMessage::<ClientDetectionRequest>::from_payload(request);
        let Ok(request_message) = request_message else {
            error!("Nacos 客户端探测请求解码失败");
            return None;
        };

        let request_message = request_message.into_body();
        // 探测正文可能附带认证头，仅记录收到探测，不展开整个请求对象。
        debug!("Nacos 客户端探测请求已到达");
        let request_id = request_message.request_id;

        let response_message = ClientDetectionResponse {
            request_id,
            ..ClientDetectionResponse::ok()
        };

        let grpc_message = GrpcMessageBuilder::new(response_message).build();
        let payload = grpc_message.into_payload();
        let Ok(payload) = payload else {
            error!("Nacos 客户端探测响应编码失败");
            return None;
        };
        Some(payload)
    }
}
