use nacos_macro::request;

use crate::common::remote::generate_request_id;

#[request(identity = "SubscribeServiceRequest", module = "naming")]
pub(crate) struct SubscribeServiceRequest {
    /// 远端订阅开关，为假时请求取消该订阅。
    pub(crate) subscribe: bool,

    /// 服务端按其规则解释的集群筛选文本。
    pub(crate) clusters: String,
}

impl SubscribeServiceRequest {
    /// 业务作用：组装服务实例变化的订阅或取消请求，并分配协议关联标识。
    ///
    /// 参数说明：
    /// - `subscribe`：为真建立远端订阅，为假取消。
    /// - `clusters`：接收范围的集群文本，原样传给服务端。
    /// - `service_name`：可选服务名称。
    /// - `namespace`：可选命名空间。
    /// - `group_name`：可选服务分组。
    ///
    /// 返回：带新请求 ID 的订阅操作对象，不改变本地接收者、缓存或重放意图。
    pub(crate) fn new(
        subscribe: bool,
        clusters: String,
        service_name: Option<String>,
        namespace: Option<String>,
        group_name: Option<String>,
    ) -> Self {
        let request_id = Some(generate_request_id());
        Self {
            subscribe,
            clusters,
            request_id,
            namespace,
            service_name,
            group_name,
            ..Default::default()
        }
    }
}
