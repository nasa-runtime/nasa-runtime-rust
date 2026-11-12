use crate::common::remote::generate_request_id;
use nacos_macro::request;

/// 客户端按配置身份读取正文的协议请求。
#[request(identity = "ConfigQueryRequest", module = "config")]
pub(crate) struct ConfigQueryRequest {}

impl ConfigQueryRequest {
    /// 业务作用：构造指定作用域内的配置查询，并分配请求关联标识。
    ///
    /// 参数说明：
    /// - `data_id`：待查询配置标识。
    /// - `group`：配置分组。
    /// - `namespace`：配置所属命名空间。
    ///
    /// 返回：保存完整查询身份的请求，不访问本地缓存或发起网络查询。
    pub fn new(data_id: String, group: String, namespace: String) -> Self {
        Self {
            request_id: Some(generate_request_id()),
            data_id: Some(data_id),
            group: Some(group),
            namespace: Some(namespace),
            ..Default::default()
        }
    }
}
