use std::sync::Arc;

use crate::common::remote::grpc::message::{GrpcMessage, GrpcMessageBuilder};
use crate::common::remote::grpc::nacos_grpc_service::ServerRequestHandler;
use crate::naming::observable::service_info_observable::ServiceInfoEmitter;

use async_trait::async_trait;
use tracing::{Instrument, error, info};

use crate::{
    nacos_proto::v2::Payload,
    naming::message::{request::NotifySubscriberRequest, response::NotifySubscriberResponse},
};

pub(crate) struct NamingPushRequestHandler {
    /// 实例快照校验、缓存更新与监听通知共用的发布入口。
    service_info_emitter: Arc<ServiceInfoEmitter>,
}

impl NamingPushRequestHandler {
    /// 业务作用：将服务端实例推送绑定到统一缓存与观察者发布入口。
    ///
    /// 参数说明：
    /// - `service_info_emitter`：负责空推送保护、快照比较和通知安排的共享发布器。
    ///
    /// 返回：持有发布器的控制流处理器，不立即更新实例目录或发送回执。
    pub(crate) fn new(service_info_emitter: Arc<ServiceInfoEmitter>) -> Self {
        Self {
            service_info_emitter,
        }
    }
}

#[async_trait]
impl ServerRequestHandler for NamingPushRequestHandler {
    /// 业务作用：接收命名服务推送，经统一发布器处理实例快照后生成关联回执。
    ///
    /// 参数说明：
    /// - `request`：必须符合 NotifySubscriberRequest 合同的服务端 Payload。
    ///
    /// 返回：解码及回执编码成功时返回成功回执，沿用请求 ID；发布器忽略过期或受保护快照也会回执，不能据此认定缓存已变化或监听回调完成。
    async fn request_reply(&self, request: Payload) -> Option<Payload> {
        let request = GrpcMessage::<NotifySubscriberRequest>::from_payload(request);
        let Ok(request) = request else {
            error!("convert payload to NotifySubscriberRequest error. {request:?}");
            return None;
        };

        let body = request.into_body();
        info!("receive NotifySubscriberRequest from nacos server: {body:?}");

        let request_id = body.request_id;
        self.service_info_emitter
            .emit(body.service_info)
            .in_current_span()
            .await;

        let mut response = NotifySubscriberResponse::ok();
        response.request_id = request_id;

        let grpc_message = GrpcMessageBuilder::new(response).build();
        let payload = grpc_message.into_payload();
        let Ok(payload) = payload else {
            error!("occur an error when handing NotifySubscriberRequest. {payload:?}");
            return None;
        };

        Some(payload)
    }
}
