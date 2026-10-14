/// ConfigFilter
#[async_trait::async_trait]
pub trait ConfigFilter: Send + Sync {
    /// Filter the config_req or config_resp. You can modify their values as needed.
    ///
    /// [`ConfigReq`] and [`ConfigResp`] will not be [`None`] at the same time.
    /// Only one of [`ConfigReq`] and [`ConfigResp`] is [`Some`].
    async fn filter(
        &self,
        config_req: Option<&mut ConfigReq>,
        config_resp: Option<&mut ConfigResp>,
    );
}

/// ConfigReq for [`ConfigFilter`]
pub struct ConfigReq {
    /// DataId
    pub data_id: String,
    /// Group
    pub group: String,
    /// Namespace/Tenant
    pub namespace: String,
    /// Content
    pub content: String,
    /// Content's Encrypted Data Key.
    pub encrypted_data_key: String,
}

impl ConfigReq {
    /// 业务作用：构造传给配置过滤器的作用域和正文。
    /// 参数说明：`data_id`、`group`、`namespace` 定位配置，`content` 为正文，`encrypted_data_key` 为加密数据密钥材料。
    /// 返回：返回待过滤的数据，不发起网络请求。
    pub fn new(
        data_id: String,
        group: String,
        namespace: String,
        content: String,
        encrypted_data_key: String,
    ) -> Self {
        ConfigReq {
            data_id,
            group,
            namespace,
            content,
            encrypted_data_key,
        }
    }
}

/// ConfigResp for [`ConfigFilter`]
pub struct ConfigResp {
    /// DataId
    pub data_id: String,
    /// Group
    pub group: String,
    /// Namespace/Tenant
    pub namespace: String,
    /// Content
    pub content: String,
    /// Content's Encrypted Data Key.
    pub encrypted_data_key: String,
}

impl ConfigResp {
    /// 业务作用：构造传给配置过滤器的作用域和正文。
    /// 参数说明：`data_id`、`group`、`namespace` 定位配置，`content` 为正文，`encrypted_data_key` 为加密数据密钥材料。
    /// 返回：返回待过滤的数据，不发起网络请求。
    pub fn new(
        data_id: String,
        group: String,
        namespace: String,
        content: String,
        encrypted_data_key: String,
    ) -> Self {
        ConfigResp {
            data_id,
            group,
            namespace,
            content,
            encrypted_data_key,
        }
    }
}
