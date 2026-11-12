use crate::common::remote::grpc::message::{GrpcMessage, GrpcMessageBuilder};
use crate::common::remote::grpc::nacos_grpc_service::ServerRequestHandler;
use crate::config::message::request::ConfigChangeNotifyRequest;
use crate::config::message::response::ConfigChangeNotifyResponse;
use crate::config::util;
use crate::nacos_proto::v2::Payload;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

/// 将配置变化推送转换为本地重新查询任务的处理器。
pub(crate) struct ConfigChangeNotifyHandler {
    /// 有界配置身份通知队列，消费者负责重新读取正文。
    pub(crate) notify_change_tx: Sender<String>,
}

#[async_trait]
impl ServerRequestHandler for ConfigChangeNotifyHandler {
    /// 业务作用：识别配置变化通知，将配置身份交给刷新队列并返回协议回执。
    ///
    /// 参数说明：
    /// - `request`：服务端通知 Payload，必须可解码且包含 data_id 与 group。
    ///
    /// 返回：有效通知尝试入队后生成成功回执，队列关闭错误不会阻止回执；解码、身份字段缺失或回执编码失败返回空。回执不证明正文已刷新或监听者已获通知。
    async fn request_reply(&self, request: Payload) -> Option<Payload> {
        tracing::debug!("[ConfigChangeNotifyHandler] receive config-change, handle start.");

        let request = GrpcMessage::<ConfigChangeNotifyRequest>::from_payload(request);
        let Ok(request) = request else {
            tracing::error!("convert payload to ConfigChangeNotifyRequest error. {request:?}");
            return None;
        };
        let server_req = request.into_body();

        let server_req_id = server_req.request_id.unwrap_or_default();
        let req_namespace = server_req.namespace.unwrap_or_default();
        let (Some(req_data_id), Some(req_group)) = (server_req.data_id, server_req.group) else {
            // 缺少配置身份无法准确定位刷新目标，不能使用空标识查询其它配置。
            tracing::error!("data_id or group is missing in ConfigChangeNotifyRequest");
            return None;
        };
        tracing::info!(
            "receive config-change, dataId={req_data_id},group={req_group},namespace={req_namespace}"
        );
        // 推送仅携带变化信号，正文必须通过后续查询获取；此处不等待缓存更新完成。
        let group_key = util::group_key(&req_data_id, &req_group, &req_namespace);
        let _ = self.notify_change_tx.send(group_key).await;

        // 回执确认通知处理路径结束，不将其解释为配置落盘或监听回调完成。
        let response = ConfigChangeNotifyResponse::ok().request_id(server_req_id);
        let grpc_message = GrpcMessageBuilder::new(response).build();
        let resp_payload = grpc_message.into_payload();
        let Ok(resp_payload) = resp_payload else {
            tracing::error!("payload conversion failed. {resp_payload:?}");
            return None;
        };

        Some(resp_payload)
    }
}
