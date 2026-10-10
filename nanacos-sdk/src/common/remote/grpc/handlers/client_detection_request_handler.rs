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
    async fn request_reply(&self, request: Payload) -> Option<Payload> {
        let request_message = GrpcMessage::<ClientDetectionRequest>::from_payload(request);
        let Ok(request_message) = request_message else {
            error!("convert payload to ClientDetectionRequest error. {request_message:?}");
            return None;
        };

        let request_message = request_message.into_body();
        debug!("ClientDetectionRequestHandler receive a request: {request_message:?}");
        let request_id = request_message.request_id;

        let response_message = ClientDetectionResponse {
            request_id,
            ..ClientDetectionResponse::ok()
        };

        let grpc_message = GrpcMessageBuilder::new(response_message).build();
        let payload = grpc_message.into_payload();
        let Ok(payload) = payload else {
            error!("occur an error when handing ClientDetectionRequest. {payload:?}");
            return None;
        };
        Some(payload)
    }
}
