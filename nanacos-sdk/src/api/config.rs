use crate::api::{error, plugin, props};
use std::collections::HashMap;
use std::sync::Arc;

/// Api [`ConfigService`].
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> nacos_sdk::api::error::Result<()> {
/// let config_service = nacos_sdk::api::config::ConfigServiceBuilder::new(
///       nacos_sdk::api::props::ClientProps::new()
///          .server_addr("127.0.0.1:8848")
///          // Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
///          .namespace("")
///          .app_name("todo-your-app-name"),
///  )
///  .build()
///  .await?;
/// # Ok(())
/// # }
/// ```
#[doc(alias("config", "sdk", "api"))]
#[derive(Clone, Debug)]
pub struct ConfigService {
    inner: Arc<crate::config::NacosConfigService>,
}

impl ConfigService {
    /// Get config, return the content.
    ///
    /// Attention to [`error::Error::ConfigNotFound`], [`error::Error::ConfigQueryConflict`]
    pub async fn get_config(
        &self,
        data_id: String,
        group: String,
    ) -> error::Result<ConfigResponse> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        self.inner.get_config(data_id, group).await
    }

    /// Publish config, return true/false.
    pub async fn publish_config(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
    ) -> error::Result<bool> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        crate::common::util::check_not_blank(&content, "content")?;
        self.inner
            .publish_config(data_id, group, content, content_type)
            .await
    }

    /// Cas publish config with cas_md5 (prev content's md5), return true/false.
    pub async fn publish_config_cas(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: String,
    ) -> error::Result<bool> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        crate::common::util::check_not_blank(&content, "content")?;
        crate::common::util::check_not_blank(&cas_md5, "cas_md5")?;
        self.inner
            .publish_config_cas(data_id, group, content, content_type, cas_md5)
            .await
    }

    /// Beta publish config, return true/false.
    pub async fn publish_config_beta(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        beta_ips: String,
    ) -> error::Result<bool> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        crate::common::util::check_not_blank(&content, "content")?;
        crate::common::util::check_not_blank(&beta_ips, "beta_ips")?;
        self.inner
            .publish_config_beta(data_id, group, content, content_type, beta_ips)
            .await
    }

    /// 业务作用：携带附加参数向 Nacos 发布指定配置，可按原内容摘要约束写入。
    /// 参数说明：`data_id` 与 `group` 定位配置，`content` 为正文，`content_type` 为可选类型；
    /// `cas_md5` 为可选原内容摘要，`params` 为附加参数，键定义见 [`constants`]。
    /// 返回：服务端确认发布成功时为 true；空参数、传输失败或服务端拒绝时返回错误。
    pub async fn publish_config_param(
        &self,
        data_id: String,
        group: String,
        content: String,
        content_type: Option<String>,
        cas_md5: Option<String>,
        params: HashMap<String, String>,
    ) -> error::Result<bool> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        crate::common::util::check_not_blank(&content, "content")?;
        self.inner
            .publish_config_param(data_id, group, content, content_type, cas_md5, params)
            .await
    }

    /// Remove config, return true/false.
    pub async fn remove_config(&self, data_id: String, group: String) -> error::Result<bool> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        self.inner.remove_config(data_id, group).await
    }

    /// Listen the config change.
    pub async fn add_listener(
        &self,
        data_id: String,
        group: String,
        listener: Arc<dyn ConfigChangeListener>,
    ) -> error::Result<()> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        self.inner.add_listener(data_id, group, listener).await
    }

    /// Remove a Listener.
    pub async fn remove_listener(
        &self,
        data_id: String,
        group: String,
        listener: Arc<dyn ConfigChangeListener>,
    ) -> error::Result<()> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        self.inner.remove_listener(data_id, group, listener).await
    }
}

/// The ConfigChangeListener receive notify of [`ConfigResponse`].
pub trait ConfigChangeListener: Send + Sync {
    /// 业务作用：把配置变更交给业务监听器。
    /// 参数说明：`config_resp` 包含当前配置正文及作用域。
    /// 返回：监听器处理变更；回调没有返回值。
    fn notify(&self, config_resp: ConfigResponse);
}

/// ConfigResponse for api.
#[derive(Debug, Clone)]
pub struct ConfigResponse {
    /// Namespace/Tenant
    namespace: String,
    /// DataId
    data_id: String,
    /// Group
    group: String,
    /// Content
    content: String,
    /// Content's Type; e.g. json,properties,xml,html,text,yaml
    content_type: String,
    /// Content's md5
    md5: String,
}

impl std::fmt::Display for ConfigResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigResponse(namespace={n},data_id={d},group={g},md5={m},content=",
            n = self.namespace,
            d = self.data_id,
            g = self.group,
            m = self.md5,
        )?;
        // Truncate content for display if it exceeds 30 chars
        if self.content.chars().count() > 30 {
            for c in self.content.chars().take(30) {
                write!(f, "{}", c)?;
            }
            write!(f, "...")?;
        } else {
            write!(f, "{}", self.content)?;
        }
        write!(f, ")")
    }
}

impl ConfigResponse {
    /// 业务作用：封装一次配置读取的作用域、正文和摘要。
    /// 参数说明：`data_id`、`group`、`namespace` 定位配置，`content` 为正文，`content_type` 为内容类型，`md5` 为摘要。
    /// 返回：返回配置响应值，不触发网络查询。
    pub fn new(
        data_id: String,
        group: String,
        namespace: String,
        content: String,
        content_type: String,
        md5: String,
    ) -> Self {
        ConfigResponse {
            data_id,
            group,
            namespace,
            content,
            content_type,
            md5,
        }
    }

    /// 业务作用：供业务读取配置所属命名空间。
    /// 参数说明：无。
    /// 返回：返回当前记录中的配置所属命名空间，不触发网络查询。
    pub fn namespace(&self) -> &String {
        &self.namespace
    }
    /// 业务作用：供业务读取配置标识。
    /// 参数说明：无。
    /// 返回：返回当前记录中的配置标识，不触发网络查询。
    pub fn data_id(&self) -> &String {
        &self.data_id
    }
    /// 业务作用：供业务读取配置所属分组。
    /// 参数说明：无。
    /// 返回：返回当前记录中的配置所属分组，不触发网络查询。
    pub fn group(&self) -> &String {
        &self.group
    }
    /// 业务作用：供业务读取配置正文。
    /// 参数说明：无。
    /// 返回：返回当前记录中的配置正文，不触发网络查询。
    pub fn content(&self) -> &String {
        &self.content
    }
    /// 业务作用：供业务读取配置正文类型。
    /// 参数说明：无。
    /// 返回：返回当前记录中的配置正文类型，不触发网络查询。
    pub fn content_type(&self) -> &String {
        &self.content_type
    }
    /// 业务作用：供业务读取配置正文摘要。
    /// 参数说明：无。
    /// 返回：返回当前记录中的配置正文摘要，不触发网络查询。
    pub fn md5(&self) -> &String {
        &self.md5
    }
}

/// SDK 默认值及受支持的环境变量键。
pub mod constants {
    /// param type, use for [`crate::api::config::ConfigService::publish_config_param`]
    pub const KEY_PARAM_CONTENT_TYPE: &str = "type";

    /// param betaIps, use for [`crate::api::config::ConfigService::publish_config_param`]
    pub const KEY_PARAM_BETA_IPS: &str = "betaIps";

    /// param appName, use for [`crate::api::config::ConfigService::publish_config_param`]
    pub const KEY_PARAM_APP_NAME: &str = "appName";

    /// param tag, use for [`crate::api::config::ConfigService::publish_config_param`]
    pub const KEY_PARAM_TAG: &str = "tag";

    /// param encryptedDataKey, use inner.
    pub(crate) const KEY_PARAM_ENCRYPTED_DATA_KEY: &str = "encryptedDataKey";
}

/// Builder of api [`ConfigService`].
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> nacos_sdk::api::error::Result<()> {
/// let config_service = nacos_sdk::api::config::ConfigServiceBuilder::new(
///       nacos_sdk::api::props::ClientProps::new()
///          .server_addr("127.0.0.1:8848")
///          // Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
///          .namespace("")
///          .app_name("todo-your-app-name"),
///  )
///  .build()
///  .await?;
/// # Ok(())
/// # }
/// ```
#[doc(alias("config", "builder"))]
pub struct ConfigServiceBuilder {
    client_props: props::ClientProps,
    auth_plugin: Option<Arc<dyn plugin::AuthPlugin>>,
    config_filters: Vec<Box<dyn plugin::ConfigFilter>>,
}

impl Default for ConfigServiceBuilder {
    fn default() -> Self {
        ConfigServiceBuilder {
            client_props: props::ClientProps::new(),
            auth_plugin: None,
            config_filters: Vec::new(),
        }
    }
}

impl ConfigServiceBuilder {
    /// 业务作用：使用显式连接属性创建 Nacos 客户端构建器。
    /// 参数说明：`client_props` 为服务端地址、命名空间和客户端属性。
    /// 返回：返回尚未建立连接的构建器。
    pub fn new(client_props: props::ClientProps) -> Self {
        ConfigServiceBuilder {
            client_props,
            auth_plugin: None,
            config_filters: Vec::new(),
        }
    }

    /// 业务作用：为客户端选择 Nacos HTTP 登录认证。
    /// 参数说明：无。
    /// 返回：返回已绑定 HTTP 认证插件的构建器，不立即登录。
    #[cfg(feature = "auth-by-http")]
    pub fn enable_auth_plugin_http(self) -> Self {
        self.with_auth_plugin(Arc::new(plugin::HttpLoginAuthPlugin::default()))
    }

    /// 业务作用：为客户端选择 Aliyun RAM 签名认证。
    /// 参数说明：无。
    /// 返回：返回已绑定 RAM 认证插件的构建器，不立即发起请求。
    #[cfg(feature = "auth-by-aliyun")]
    pub fn enable_auth_plugin_aliyun(self) -> Self {
        self.with_auth_plugin(Arc::new(plugin::AliyunRamAuthPlugin::default()))
    }

    /// Set [`plugin::AuthPlugin`]
    pub fn with_auth_plugin(mut self, auth_plugin: Arc<dyn plugin::AuthPlugin>) -> Self {
        self.auth_plugin = Some(auth_plugin);
        self
    }

    /// 业务作用：设置配置读写的过滤器链。
    /// 参数说明：`config_filters` 为按声明顺序执行的过滤器。
    /// 返回：替换原过滤器列表并返回构建器。
    pub fn with_config_filters(
        mut self,
        config_filters: Vec<Box<dyn plugin::ConfigFilter>>,
    ) -> Self {
        self.config_filters = config_filters;
        self
    }

    /// 业务作用：为配置读写追加一个过滤器。
    /// 参数说明：`config_filter` 为新增的内容处理逻辑。
    /// 返回：在现有链尾部追加并返回构建器。
    pub fn add_config_filter(mut self, config_filter: Box<dyn plugin::ConfigFilter>) -> Self {
        self.config_filters.push(config_filter);
        self
    }

    /// 业务作用：将加密插件组合成配置过滤器，供后续配置读写使用。
    /// 参数说明：`encryption_plugins` 为实现 [`plugin::EncryptionPlugin`] 的插件集合。
    /// 返回：追加 [`plugin::ConfigEncryptionFilter`] 的构建器，不立即建立网络连接。
    pub fn with_encryption_plugins(
        self,
        encryption_plugins: Vec<Box<dyn plugin::EncryptionPlugin>>,
    ) -> Self {
        self.add_config_filter(Box::new(plugin::ConfigEncryptionFilter::new(
            encryption_plugins,
        )))
    }

    /// Builds a new [`ConfigService`].
    pub async fn build(self) -> error::Result<ConfigService> {
        #[cfg(feature = "tracing-log")]
        {
            // $HOME/logs/nacos
            let log_path = crate::common::util::HOME_DIR.to_owned() + "/logs/nacos";
            let log_level = "INFO".to_string();
            crate::common::log::init(log_path, log_level);
        }

        let auth_plugin = match self.auth_plugin {
            None => Arc::new(plugin::NoopAuthPlugin::default()),
            Some(plugin) => plugin,
        };
        let inner = crate::config::NacosConfigService::new(
            self.client_props,
            auth_plugin,
            self.config_filters,
        )
        .await?;
        let inner = Arc::new(inner);
        Ok(ConfigService { inner })
    }
}
