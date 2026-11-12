use crate::api::{error, plugin, props};
use std::collections::HashMap;
use std::sync::Arc;

/// 在固定命名空间内读取、发布和监听配置的客户端入口。
///
/// # 示例
///
/// ```no_run
/// # async fn run() -> nacos_sdk::api::error::Result<()> {
/// let config_service = nacos_sdk::api::config::ConfigServiceBuilder::new(
///       nacos_sdk::api::props::ClientProps::new()
///          .server_addr("127.0.0.1:8848")
///          // 公共命名空间使用空字符串，其它命名空间使用服务端分配的标识。
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
    /// 当前命名空间共享的配置请求与监听运行时。
    inner: Arc<crate::config::NacosConfigService>,
}

impl ConfigService {
    /// 业务作用：读取指定配置的正文与摘要，调用前拒绝空白身份。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    ///
    /// 返回：配置响应；配置缺失、查询冲突、参数无效或远端请求失败时返回错误。
    pub async fn get_config(
        &self,
        data_id: String,
        group: String,
    ) -> error::Result<ConfigResponse> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        self.inner.get_config(data_id, group).await
    }

    /// 业务作用：向当前命名空间发布完整配置内容。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    /// - `content`：非空完整正文。
    /// - `content_type`：可选正文类型，不承担内容格式校验。
    ///
    /// 返回：远端发布结果；参数无效或请求失败时返回错误，不能据此证明所有监听器已应用。
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

    /// 业务作用：以旧正文摘要约束配置更新，避免覆盖不匹配的远端内容。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    /// - `content`：非空新正文。
    /// - `content_type`：可选正文类型。
    /// - `cas_md5`：期望的旧正文 MD5，必须非空。
    ///
    /// 返回：远端条件发布结果；参数无效、条件不满足或请求失败按远端响应返回错误或拒绝结果。
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

    /// 业务作用：发布仅对指定客户端地址生效的灰度配置。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    /// - `content`：非空灰度正文。
    /// - `content_type`：可选正文类型。
    /// - `beta_ips`：服务端支持的客户端地址列表文本，必须非空。
    ///
    /// 返回：远端灰度发布结果；参数无效或请求失败时返回错误，不表示全量配置已替换。
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
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置所属分组。
    /// - `content`：完整配置正文。
    /// - `content_type`：可选内容类型。
    /// - `cas_md5`：可选原内容摘要，用于约束并发更新。
    /// - `params`：附加发布参数，键定义见 [`constants`]。
    ///
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

    /// 业务作用：删除指定命名空间和分组中的配置。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    ///
    /// 返回：远端删除结果；参数无效或请求失败时返回错误，不同步等待所有监听者完成回调。
    pub async fn remove_config(&self, data_id: String, group: String) -> error::Result<bool> {
        crate::common::util::check_not_blank(&data_id, "data_id")?;
        crate::common::util::check_not_blank(&group, "group")?;
        self.inner.remove_config(data_id, group).await
    }

    /// 业务作用：为指定配置登记本地变更监听器。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    /// - `listener`：配置变更通知接收者，后续移除须传回同一实例。
    ///
    /// 返回：本地登记完成；参数无效时失败，成功不保证远端已有配置或首次回调已完成。
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

    /// 业务作用：移除指定配置上的本地监听器，保留其它监听者。
    ///
    /// 参数说明：
    /// - `data_id`：非空配置标识。
    /// - `group`：非空配置分组。
    /// - `listener`：需要移除的原监听器实例。
    ///
    /// 返回：本地移除操作完成；参数无效时失败，不等待已经开始执行的回调结束。
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

/// 接收配置正文及其身份和摘要的业务监听契约。
pub trait ConfigChangeListener: Send + Sync {
    /// 业务作用：把配置变更交给业务监听器。
    ///
    /// 参数说明：
    /// - `config_resp`：当前配置正文及作用域。
    ///
    /// 返回：监听器处理变更；回调没有返回值。
    fn notify(&self, config_resp: ConfigResponse);
}

/// 一次配置读取或变更通知的完整内容快照。
#[derive(Debug, Clone)]
pub struct ConfigResponse {
    /// 配置所属命名空间。
    namespace: String,
    /// 配置标识。
    data_id: String,
    /// 配置分组。
    group: String,
    /// 完整正文。
    content: String,
    /// 正文类型，例如 json、properties、xml、html、text、yaml。
    content_type: String,
    /// 正文的 MD5 摘要。
    md5: String,
}

impl std::fmt::Display for ConfigResponse {
    /// 业务作用：展示配置身份、摘要和有界正文预览。
    ///
    /// 参数说明：
    /// - `f`：接收展示文本的格式化器。
    ///
    /// 返回：最多输出正文前 30 个字符；目标写入失败时返回格式化错误，预览不进行秘密值脱敏。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigResponse(namespace={n},data_id={d},group={g},md5={m},content=",
            n = self.namespace,
            d = self.data_id,
            g = self.group,
            m = self.md5,
        )?;
        // 按字符截取避免切断 UTF-8，但预览仍包含正文，调用方应自行控制展示范围。
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
    ///
    /// 参数说明：
    /// - `data_id`：配置标识。
    /// - `group`：配置所属分组。
    /// - `namespace`：会话或配置的命名空间。
    /// - `content`：完整配置正文。
    /// - `content_type`：配置响应声明的内容类型。
    /// - `md5`：配置正文摘要。
    ///
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
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回当前记录中的配置所属命名空间，不触发网络查询。
    pub fn namespace(&self) -> &String {
        &self.namespace
    }
    /// 业务作用：供业务读取配置标识。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回当前记录中的配置标识，不触发网络查询。
    pub fn data_id(&self) -> &String {
        &self.data_id
    }
    /// 业务作用：供业务读取配置所属分组。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回当前记录中的配置所属分组，不触发网络查询。
    pub fn group(&self) -> &String {
        &self.group
    }
    /// 业务作用：供业务读取配置正文。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回当前记录中的配置正文，不触发网络查询。
    pub fn content(&self) -> &String {
        &self.content
    }
    /// 业务作用：供业务读取配置正文类型。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回当前记录中的配置正文类型，不触发网络查询。
    pub fn content_type(&self) -> &String {
        &self.content_type
    }
    /// 业务作用：供业务读取配置正文摘要。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回当前记录中的配置正文摘要，不触发网络查询。
    pub fn md5(&self) -> &String {
        &self.md5
    }
}

/// SDK 默认值及受支持的环境变量键。
pub mod constants {
    /// 附加发布参数中的正文类型键。
    pub const KEY_PARAM_CONTENT_TYPE: &str = "type";

    /// 附加发布参数中的灰度客户端地址列表键。
    pub const KEY_PARAM_BETA_IPS: &str = "betaIps";

    /// 附加发布参数中的应用名键。
    pub const KEY_PARAM_APP_NAME: &str = "appName";

    /// 附加发布参数中的配置标签键。
    pub const KEY_PARAM_TAG: &str = "tag";

    /// 加密过滤器使用的加密数据密钥参数键。
    pub(crate) const KEY_PARAM_ENCRYPTED_DATA_KEY: &str = "encryptedDataKey";
}

/// 组合连接属性、认证插件和内容过滤器的配置客户端构建器。
///
/// # 示例
///
/// ```no_run
/// # async fn run() -> nacos_sdk::api::error::Result<()> {
/// let config_service = nacos_sdk::api::config::ConfigServiceBuilder::new(
///       nacos_sdk::api::props::ClientProps::new()
///          .server_addr("127.0.0.1:8848")
///          // 公共命名空间使用空字符串，其它命名空间使用服务端分配的标识。
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
    /// 构建时使用的连接、命名空间与运行参数。
    client_props: props::ClientProps,
    /// 可选认证插件，缺省使用空身份插件。
    auth_plugin: Option<Arc<dyn plugin::AuthPlugin>>,
    /// 按登记顺序执行的配置内容过滤器。
    config_filters: Vec<Box<dyn plugin::ConfigFilter>>,
}

impl Default for ConfigServiceBuilder {
    /// 业务作用：创建仅含默认连接属性的配置客户端构建器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：未连接、未安装认证插件且过滤器列表为空的构建器。
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
    ///
    /// 参数说明：
    /// - `client_props`：服务端地址、命名空间和客户端属性。
    ///
    /// 返回：返回尚未建立连接的构建器。
    pub fn new(client_props: props::ClientProps) -> Self {
        ConfigServiceBuilder {
            client_props,
            auth_plugin: None,
            config_filters: Vec::new(),
        }
    }

    /// 业务作用：为客户端选择 Nacos HTTP 登录认证。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回已绑定 HTTP 认证插件的构建器，不立即登录。
    #[cfg(feature = "auth-by-http")]
    pub fn enable_auth_plugin_http(self) -> Self {
        self.with_auth_plugin(Arc::new(plugin::HttpLoginAuthPlugin::default()))
    }

    /// 业务作用：为客户端选择 Aliyun RAM 签名认证。
    ///
    /// 参数说明：无。
    ///
    /// 返回：返回已绑定 RAM 认证插件的构建器，不立即发起请求。
    #[cfg(feature = "auth-by-aliyun")]
    pub fn enable_auth_plugin_aliyun(self) -> Self {
        self.with_auth_plugin(Arc::new(plugin::AliyunRamAuthPlugin::default()))
    }

    /// 业务作用：为后续配置请求指定认证身份来源。
    ///
    /// 参数说明：
    /// - `auth_plugin`：登录与请求身份插件。
    ///
    /// 返回：替换原插件后的构建器；此时不登录或发送网络请求。
    pub fn with_auth_plugin(mut self, auth_plugin: Arc<dyn plugin::AuthPlugin>) -> Self {
        self.auth_plugin = Some(auth_plugin);
        self
    }

    /// 业务作用：设置配置读写的过滤器链。
    ///
    /// 参数说明：
    /// - `config_filters`：按声明顺序执行的过滤器。
    ///
    /// 返回：替换原过滤器列表并返回构建器。
    pub fn with_config_filters(
        mut self,
        config_filters: Vec<Box<dyn plugin::ConfigFilter>>,
    ) -> Self {
        self.config_filters = config_filters;
        self
    }

    /// 业务作用：为配置读写追加一个过滤器。
    ///
    /// 参数说明：
    /// - `config_filter`：新增的内容过滤器。
    ///
    /// 返回：在现有链尾部追加并返回构建器。
    pub fn add_config_filter(mut self, config_filter: Box<dyn plugin::ConfigFilter>) -> Self {
        self.config_filters.push(config_filter);
        self
    }

    /// 业务作用：将加密插件组合成配置过滤器，供后续配置读写使用。
    ///
    /// 参数说明：
    /// - `encryption_plugins`：配置正文加密插件集合。
    ///
    /// 返回：追加 [`plugin::ConfigEncryptionFilter`] 的构建器，不立即建立网络连接。
    pub fn with_encryption_plugins(
        self,
        encryption_plugins: Vec<Box<dyn plugin::EncryptionPlugin>>,
    ) -> Self {
        self.add_config_filter(Box::new(plugin::ConfigEncryptionFilter::new(
            encryption_plugins,
        )))
    }

    /// 业务作用：根据固定属性和插件创建配置请求与监听运行时。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可发起配置操作的共享客户端；初始化失败返回错误，不表示某项业务配置已读取。
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
        Ok(ConfigService {
            inner,
        })
    }
}
