use nacos_macro::response;
use serde::{Deserialize, Serialize};

/// 服务端批量比较配置摘要后的结果，列出需要重新获取的配置。
#[response(identity = "ConfigChangeBatchListenResponse", module = "config")]
pub(crate) struct ConfigChangeBatchListenResponse {
    /// 与客户端摘要不同的配置身份；不包含配置正文。
    pub(crate) changed_configs: Option<Vec<ConfigContext>>,
}

/// 发生变化的配置身份，供客户端定位后续查询目标。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ConfigContext {
    /// 发生变化的配置标识。
    #[serde(rename = "dataId")]
    pub(crate) data_id: String,
    /// 配置分组。
    pub(crate) group: String,
    /// 协议 tenant 字段承载的命名空间。
    #[serde(rename = "tenant")]
    pub(crate) namespace: String,
}
