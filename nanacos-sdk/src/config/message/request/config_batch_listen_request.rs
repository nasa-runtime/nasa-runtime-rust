use crate::common::remote::generate_request_id;
use nacos_macro::request;
use serde::{Deserialize, Serialize};

/// 客户端对多个配置统一订阅或取消的协议请求。
#[request(identity = "ConfigBatchListenRequest", module = "config")]
pub(crate) struct ConfigBatchListenRequest {
    /// 为真建立监听，为假移除监听。
    pub(crate) listen: bool,
    /// 本次操作的配置身份及已有内容摘要。
    pub(crate) config_listen_contexts: Vec<ConfigListenContext>,
}

impl ConfigBatchListenRequest {
    /// 业务作用：建立配置批量订阅或取消请求，并为响应关联分配请求标识。
    ///
    /// 参数说明：
    /// - `listen`：为真订阅配置，为假取消订阅，具体配置条目随后补充。
    ///
    /// 返回：条目为空的请求，不发送到服务端或改变本地监听状态。
    pub fn new(listen: bool) -> Self {
        Self {
            listen,
            config_listen_contexts: Vec::new(),
            request_id: Some(generate_request_id()),
            ..Default::default()
        }
    }

    /// 业务作用：替换本次批量监听请求覆盖的配置条目集合。
    ///
    /// 参数说明：
    /// - `contexts`：包含配置身份和本地摘要的完整集合，替换已有条目。
    ///
    /// 返回：持有新集合的请求，不校验重复条目或执行订阅。
    pub fn config_listen_context(mut self, contexts: Vec<ConfigListenContext>) -> Self {
        self.config_listen_contexts = contexts;
        self
    }

    /// 业务作用：为当前批量监听请求追加一个配置及其摘要。
    ///
    /// 参数说明：
    /// - `context`：待加入的配置监听条目，追加时不去重。
    ///
    /// 返回：保留已有顺序并追加条目的请求，不通知服务端。
    pub fn add_config_listen_context(mut self, context: ConfigListenContext) -> Self {
        self.config_listen_contexts.push(context);
        self
    }
}

/// 服务端进行配置变化比较所需的客户端快照信息。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ConfigListenContext {
    /// 被监听的配置标识。
    #[serde(rename = "dataId")]
    data_id: String,
    /// 配置分组，与标识及命名空间共同定位配置。
    group: String,
    /// 协议 tenant 字段对应的命名空间。
    tenant: String,
    /// 客户端当前内容的 MD5 摘要，不承载配置正文。
    md5: String,
}

impl ConfigListenContext {
    /// 业务作用：描述本地配置快照身份和摘要，供服务端比较是否需要通知变更。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置分组。
    /// - `tenant`：配置所属命名空间。
    /// - `md5`：本地已持有内容的摘要，原样提供给服务端比较。
    ///
    /// 返回：监听上下文，不计算摘要、读取内容或验证权限。
    pub fn new(data_id: String, group: String, tenant: String, md5: String) -> Self {
        ConfigListenContext {
            data_id,
            group,
            tenant,
            md5,
        }
    }
}
