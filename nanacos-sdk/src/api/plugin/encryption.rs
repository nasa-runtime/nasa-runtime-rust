use crate::api::plugin::{ConfigFilter, ConfigReq, ConfigResp};

/**
 * For example：cipher-AES-dataId.
 */
pub const DEFAULT_CIPHER_PREFIX: &str = "cipher-";

/// 加密配置名中用于分隔算法标识的字符。
pub const DEFAULT_CIPHER_SPLIT: &str = "-";

/// EncryptionPlugin for Config.
#[async_trait::async_trait]
pub trait EncryptionPlugin: Send + Sync {
    /**
     * Whether need to do cipher.
     *
     * e.g. data_id = "cipher-AES-dataId"
     */
    fn need_cipher(&self, data_id: &str) -> bool {
        data_id.starts_with(DEFAULT_CIPHER_PREFIX)
            && self
                .parse_algorithm_name(data_id)
                .eq(&self.algorithm_name())
    }

    /**
     * Parse encryption algorithm name.
     *
     * @param data_id data_id
     * @return algorithm name
     */
    fn parse_algorithm_name(&self, data_id: &str) -> String {
        data_id
            .split(DEFAULT_CIPHER_SPLIT)
            .nth(1)
            .expect("data_id should have algorithm name in the second segment")
            .to_string()
    }

    /**
     * Encrypt interface.
     *
     * @param secret_key secret key
     * @param content   content unencrypted
     * @return encrypt value
     */
    async fn encrypt(&self, secret_key: &str, content: &str) -> String;

    /**
     * Decrypt interface.
     *
     * @param secret_key secret key
     * @param content   encrypted
     * @return decrypt value
     */
    async fn decrypt(&self, secret_key: &str, content: &str) -> String;

    /**
     * Generate secret key. It only be known by you.
     *
     * @return Secret key
     */
    async fn generate_secret_key(&self) -> String;

    /**
     * Algorithm name. e.g. AES,AES128,AES256,DES,3DES,...
     *
     * @return name
     */
    fn algorithm_name(&self) -> String;

    /**
     * Encrypt secret Key. It will be transmitted in the network.
     *
     * @param secretKey secretKey
     * @return encrypted secretKey
     */
    async fn encrypt_secret_key(&self, secret_key: &str) -> String;

    /**
     * Decrypt secret Key.
     *
     * @param secret_key secretKey
     * @return decrypted secretKey
     */
    async fn decrypt_secret_key(&self, secret_key: &str) -> String;
}

/// ConfigEncryptionFilter handle with [`EncryptionPlugin`]
pub struct ConfigEncryptionFilter {
    encryption_plugins: Vec<Box<dyn EncryptionPlugin>>,
}

impl ConfigEncryptionFilter {
    /// 业务作用：将算法插件登记为配置加解密过滤器。
    /// 参数说明：`encryption_plugins` 为可用于配置正文的算法插件。
    /// 返回：返回按算法名称选择插件的过滤器。
    pub fn new(encryption_plugins: Vec<Box<dyn EncryptionPlugin>>) -> Self {
        Self { encryption_plugins }
    }
}

#[async_trait::async_trait]
impl ConfigFilter for ConfigEncryptionFilter {
    async fn filter(
        &self,
        config_req: Option<&mut ConfigReq>,
        config_resp: Option<&mut ConfigResp>,
    ) {
        // Publish configuration, encrypt
        if let Some(config_req) = config_req {
            for plugin in &self.encryption_plugins {
                if !plugin.need_cipher(&config_req.data_id) {
                    continue;
                }

                let secret_key = plugin.generate_secret_key().await;
                let encrypted_content = plugin.encrypt(&secret_key, &config_req.content).await;
                let encrypted_secret_key = plugin.encrypt_secret_key(&secret_key).await;

                // set encrypted data.
                config_req.encrypted_data_key = encrypted_secret_key;
                config_req.content = encrypted_content;
                break;
            }
        }

        // Get configuration, decrypt
        if let Some(config_resp) = config_resp
            && !config_resp.encrypted_data_key.is_empty()
        {
            for plugin in &self.encryption_plugins {
                if !plugin.need_cipher(&config_resp.data_id) {
                    continue;
                }

                // get encrypted data.
                let encrypted_secret_key = &config_resp.encrypted_data_key;
                let encrypted_content = &config_resp.content;

                let decrypted_secret_key = plugin.decrypt_secret_key(encrypted_secret_key).await;
                let decrypted_content = plugin
                    .decrypt(&decrypted_secret_key, encrypted_content)
                    .await;

                // set decrypted data.
                config_resp.content = decrypted_content;
                break;
            }
        }
    }
}
