use std::sync::Arc;
use tracing::{Instrument, debug, error, instrument};

use crate::common::remote::generate_request_id;
use crate::common::remote::grpc::NacosGrpcClient;
use crate::common::remote::grpc::message::GrpcMessageData;
use crate::naming::message::request::*;
use crate::naming::message::response::*;
use crate::naming::redo::AutomaticRequest;
use crate::naming::redo::CallBack;

/// 为命名服务请求生成重放执行与任务身份合同，复用既有业务请求内容。
macro_rules! impl_automatic_request {
    ($request:ty, $response:ty) => {
        #[async_trait::async_trait]
        impl AutomaticRequest for $request {
            /// 业务作用：为既有命名操作重新分配请求 ID 并发送一次，将传输及解码结果交给重放状态回调。
            /// 参数说明：
            /// - `grpc_client`：执行重放的共享认证客户端。
            /// - `call_back`：接收本次结果的回调，成功可使任务冻结，失败可保留重试意图。
            ///
            /// 返回：请求和回调执行后结束；发送或解码失败交给回调，不单独检查响应正文的业务结果码，也不在此循环重试。
            #[instrument(skip_all)]
            async fn run(&self, grpc_client: Arc<NacosGrpcClient>, call_back: CallBack) {
                let mut req = self.clone();
                req.request_id = Some(generate_request_id());
                debug!("automatically execute {}. {:?}", stringify!($request), req);
                let ret = grpc_client
                    .send_request::<$request, $response>(req)
                    .in_current_span()
                    .await;
                if let Err(e) = ret {
                    error!(
                        "automatically execute {} occur an error. {:?}",
                        stringify!($request),
                        e
                    );
                    call_back(Err(e));
                } else {
                    call_back(Ok(()));
                }
            }

            /// 业务作用：由作用域、服务名称和协议类型生成重放任务键，供执行器替换或移除同类意图。
            /// 参数说明: 无。
            ///
            /// 返回：缺失身份字段按空串处理后用 @@ 连接的键；不含具体实例、操作码或订阅集群，因此这些差异不会产生独立任务键。
            fn name(&self) -> String {
                let namespace = self.namespace.as_deref().unwrap_or("");
                let service_name = self.service_name.as_deref().unwrap_or("");
                let group_name = self.group_name.as_deref().unwrap_or("");
                format!(
                    "{}@@{}@@{}@@{}",
                    namespace,
                    group_name,
                    service_name,
                    <$request>::identity()
                )
            }
        }
    };
}

impl_automatic_request!(InstanceRequest, InstanceResponse);

impl_automatic_request!(BatchInstanceRequest, BatchInstanceResponse);

impl_automatic_request!(PersistentInstanceRequest, InstanceResponse);

impl_automatic_request!(SubscribeServiceRequest, SubscribeServiceResponse);
