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
