use crate::api::plugin::{ConfigFilter, ConfigReq, ConfigResp};

/// 声明配置需要加密处理的名称前缀，例如 cipher-AES-dataId。
pub const DEFAULT_CIPHER_PREFIX: &str = "cipher-";

/// 加密配置名中用于分隔算法标识的字符。
pub const DEFAULT_CIPHER_SPLIT: &str = "-";

/// 配置正文及数据密钥的加解密扩展合同，具体密码算法与密钥管理由实现提供。
#[async_trait::async_trait]
pub trait EncryptionPlugin: Send + Sync {
    /// 业务作用：按配置名的加密前缀及算法段判断是否由当前插件处理。
    ///
    /// 参数说明：
    /// - `data_id`：配置标识，默认合同采用 cipher-算法名-资源名的形式。
    ///
    /// 返回：前缀匹配且第二段与插件算法名完全一致时为真；不验证正文是否已加密，也不确认算法实现可用。
    fn need_cipher(&self, data_id: &str) -> bool {
        data_id.starts_with(DEFAULT_CIPHER_PREFIX)
            && self
                .parse_algorithm_name(data_id)
                .eq(&self.algorithm_name())
    }

    /// 业务作用：从配置标识中取得用于选择加解密插件的算法段。
    ///
    /// 参数说明：
    /// - `data_id`：含连字符分隔段的配置标识；此入口本身不检查 cipher- 前缀。
    ///
    /// 返回：第二段的原始文本，允许为空；缺少第二段时产生 panic，不推断默认算法。
    fn parse_algorithm_name(&self, data_id: &str) -> String {
        data_id
            .split(DEFAULT_CIPHER_SPLIT)
            .nth(1)
            .expect("data_id should have algorithm name in the second segment")
            .to_string()
    }

    /// 业务作用：将配置明文转换为可持久化的密文，由插件实现所声明的算法合同。
    ///
    /// 参数说明：
    /// - `secret_key`：本条配置的数据密钥明文，不可与返回密文一同直接发送。
    /// - `content`：待保护的配置明文，可能包含业务秘密。
    ///
    /// 返回：密文文本；接口没有错误返回通道，实现不得以未加密原文冒充成功结果。
    async fn encrypt(&self, secret_key: &str, content: &str) -> String;

    /// 业务作用：将配置密文还原为可交付给调用方的配置内容。
    ///
    /// 参数说明：
    /// - `secret_key`：已经解封的数据密钥明文。
    /// - `content`：服务端返回的密文文本，必须符合当前插件的编码及认证合同。
    ///
    /// 返回：解密后的明文；接口没有错误返回通道，实现负责拒绝无效密文，不能静默当作正常配置。
    async fn decrypt(&self, secret_key: &str, content: &str) -> String;

    /// 业务作用：为一次配置发布生成数据密钥，由同一插件加密正文并封装密钥。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：算法所需的数据密钥明文；随机性、长度及保密处理由实现保证，框架不替代检查。
    async fn generate_secret_key(&self) -> String;

    /// 业务作用：声明当前插件接受的配置名算法段，用于过滤器选择匹配插件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：区分大小写的算法名称；仅用于名称匹配，不表示框架已校验算法强度或实现。
    fn algorithm_name(&self) -> String;

    /// 业务作用：封装数据密钥，使其能以 encrypted_data_key 随密文配置传输和持久化。
    ///
    /// 参数说明：
    /// - `secret_key`：本次正文加密所用的数据密钥明文。
    ///
    /// 返回：受保护的密钥文本；实现自行管理封装密钥，不能直接返回传入明文。
    async fn encrypt_secret_key(&self, secret_key: &str) -> String;

    /// 业务作用：解封服务端随配置返回的数据密钥，供正文解密使用。
    ///
    /// 参数说明：
    /// - `secret_key`：encrypted_data_key 中的受保护密钥文本，不能直接当作正文密钥使用。
    ///
    /// 返回：数据密钥明文；解封及真实性检查由实现负责，接口不提供结构化失败结果。
    async fn decrypt_secret_key(&self, secret_key: &str) -> String;
}

/// 按配置名称选择 EncryptionPlugin 的过滤器，不为未匹配的配置自动提供加密。
pub struct ConfigEncryptionFilter {
    /// 按调用顺序排列的算法插件，首个匹配项负责当前方向的数据转换。
    encryption_plugins: Vec<Box<dyn EncryptionPlugin>>,
}

impl ConfigEncryptionFilter {
    /// 业务作用：将算法插件登记为配置加解密过滤器。
    ///
    /// 参数说明：
    /// - `encryption_plugins`：配置正文加密插件集合。
    ///
    /// 返回：返回按算法名称选择插件的过滤器。
    pub fn new(encryption_plugins: Vec<Box<dyn EncryptionPlugin>>) -> Self {
        Self {
            encryption_plugins,
        }
    }
}

#[async_trait::async_trait]
impl ConfigFilter for ConfigEncryptionFilter {
    /// 业务作用：在发布前加密配置，或在读取后解密配置；按登记顺序仅使用首个名称匹配的插件。
    ///
    /// 参数说明：
    /// - `config_req`：可选的发布数据；命中算法后替换正文与受保护数据密钥。
    /// - `config_resp`：可选的读取数据；仅在受保护数据密钥非空时尝试解密正文。
    ///
    /// 返回：原地更新命中的数据；无匹配插件时保持原值，不拒绝发布。调用方正常只提供一侧，两侧均提供时按发布、读取顺序分别处理。
    async fn filter(
        &self,
        config_req: Option<&mut ConfigReq>,
        config_resp: Option<&mut ConfigResp>,
    ) {
        // 发布路径先取得密文和受保护密钥，再替换请求字段，避免插件等待中途留下半套加密内容。
        if let Some(config_req) = config_req {
            for plugin in &self.encryption_plugins {
                if !plugin.need_cipher(&config_req.data_id) {
                    continue;
                }

                let secret_key = plugin.generate_secret_key().await;
                let encrypted_content = plugin.encrypt(&secret_key, &config_req.content).await;
                let encrypted_secret_key = plugin.encrypt_secret_key(&secret_key).await;

                config_req.encrypted_data_key = encrypted_secret_key;
                config_req.content = encrypted_content;
                break;
            }
        }

        // 没有密钥封装信息时不尝试解密；名称匹配本身不能证明当前正文是有效密文。
        if let Some(config_resp) = config_resp
            && !config_resp.encrypted_data_key.is_empty()
        {
            for plugin in &self.encryption_plugins {
                if !plugin.need_cipher(&config_resp.data_id) {
                    continue;
                }

                let encrypted_secret_key = &config_resp.encrypted_data_key;
                let encrypted_content = &config_resp.content;

                let decrypted_secret_key = plugin.decrypt_secret_key(encrypted_secret_key).await;
                let decrypted_content = plugin
                    .decrypt(&decrypted_secret_key, encrypted_content)
                    .await;

                config_resp.content = decrypted_content;
                break;
            }
        }
    }
}
