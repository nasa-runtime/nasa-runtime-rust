use nacos_macro::request;

use crate::{api::naming::ServiceInstance, common::remote::generate_request_id};

#[request(identity = "BatchInstanceRequest", module = "naming")]
pub(crate) struct BatchInstanceRequest {
    /// 批量注册操作码，协议字段名为 type。
    #[serde(rename = "type")]
    pub(crate) r_type: String,

    /// 本次向同一服务作用域提交的实例集合。
    pub(crate) instances: Vec<ServiceInstance>,
}

impl BatchInstanceRequest {
    /// 业务作用：为同一服务作用域组装批量实例注册请求。
    ///
    /// 参数说明：
    /// - `instances`：待注册实例集合，原样移入，不去重或拒绝空集合。
    /// - `namespace`：可选命名空间。
    /// - `service_name`：可选服务名称。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：带 batchRegisterInstance 操作码和新请求 ID 的消息，不执行远端注册或本地重放登记。
    pub(crate) fn new(
        instances: Vec<ServiceInstance>,
        namespace: Option<String>,
        service_name: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        let request_id = Some(generate_request_id());
        Self {
            r_type: "batchRegisterInstance".to_string(),
            instances,
            request_id,
            namespace,
            service_name,
            group_name,
            ..Default::default()
        }
    }
}
