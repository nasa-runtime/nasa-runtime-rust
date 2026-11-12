use crate::common::remote::generate_request_id;
use nacos_macro::request;

/// 客户端删除指定配置的协议请求。
#[request(identity = "ConfigRemoveRequest", module = "config")]
pub(crate) struct ConfigRemoveRequest {
    /// 可选配置标签，此构造流程默认不指定。
    pub(crate) tag: Option<String>,
}

impl ConfigRemoveRequest {
    /// 业务作用：构造删除指定作用域配置的请求，并分配关联标识。
    ///
    /// 参数说明：
    /// - `data_id`：待删除配置标识。
    /// - `group`：配置分组。
    /// - `namespace`：配置所属命名空间。
    ///
    /// 返回：不带标签条件的删除请求，不执行远端删除或撤销本地监听。
    pub fn new(data_id: String, group: String, namespace: String) -> Self {
        Self {
            request_id: Some(generate_request_id()),
            data_id: Some(data_id),
            group: Some(group),
            namespace: Some(namespace),
            tag: None,
            ..Default::default()
        }
    }
}
