/// 配置收发路径的转换扩展，SDK 按登记顺序逐个调用。
#[async_trait::async_trait]
pub trait ConfigFilter: Send + Sync {
    /// 业务作用：在配置发布前或读取后执行原地转换，使应用可插入正文加解密等处理。
    ///
    /// 参数说明：
    /// - `config_req`：发布方向的可变数据，SDK 调用此方向时不提供响应数据。
    /// - `config_resp`：读取方向的可变数据，SDK 调用此方向时不提供请求数据。
    ///
    /// 返回：转换完成后的修改留在传入对象中；接口没有错误返回值。SDK 正常调用只提供一侧，自行调用时须遵守该约定。
    async fn filter(
        &self,
        config_req: Option<&mut ConfigReq>,
        config_resp: Option<&mut ConfigResp>,
    );
}

/// 传入 ConfigFilter 的发布数据，转换结果将用于后续配置发布。
pub struct ConfigReq {
    /// 配置标识，参与发布目标定位及算法选择。
    pub data_id: String,
    /// 配置分组，与标识共同定位目标。
    pub group: String,
    /// 配置所在的租户命名空间。
    pub namespace: String,
    /// 当前发布正文；加密过滤器可将明文替换为密文。
    pub content: String,
    /// 随密文传输的受保护数据密钥，未采用密钥封装时为空。
    pub encrypted_data_key: String,
}

impl ConfigReq {
    /// 业务作用：构造传给配置过滤器的作用域和正文。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置所属分组。
    /// - `namespace`：会话或配置的命名空间。
    /// - `content`：完整配置正文。
    /// - `encrypted_data_key`：配置正文的加密数据密钥材料。
    ///
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

/// 传入 ConfigFilter 的读取数据，转换完成后交付给配置调用方。
pub struct ConfigResp {
    /// 响应对应的配置标识，供过滤器选择处理方式。
    pub data_id: String,
    /// 响应所属的配置分组。
    pub group: String,
    /// 响应所属的租户命名空间。
    pub namespace: String,
    /// 服务端返回并可被过滤器转换的正文。
    pub content: String,
    /// 用于解封正文密钥的受保护材料，不是可直接使用的明文密钥。
    pub encrypted_data_key: String,
}

impl ConfigResp {
    /// 业务作用：构造传给配置过滤器的作用域和正文。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置所属分组。
    /// - `namespace`：会话或配置的命名空间。
    /// - `content`：完整配置正文。
    /// - `encrypted_data_key`：配置正文的加密数据密钥材料。
    ///
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
