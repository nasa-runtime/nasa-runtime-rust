use std::{collections::HashMap, path::PathBuf};

use crate::api::constants::*;
use crate::properties::{get_value, get_value_bool, get_value_option};

/// Nacos 客户端的地址、缓存、标签和认证来源配置。
#[derive(Debug, Clone)]
pub struct ClientProps {
    /// 直接服务地址列表，未使用 endpoint 时按分隔规则解析。
    server_addr: String,
    /// 解析服务列表的固定入口；完整 URL 原样使用，裸地址补默认路径和端口。
    endpoint: Option<String>,
    /// 可选远程 gRPC 端口；省略时按服务端地址推导。
    grpc_port: Option<u16>,
    /// 原始逻辑 namespace；空值表示默认公共空间。
    namespace: String,
    /// 客户端所属应用名称，进入受控标签。
    app_name: String,
    /// 是否保留已有发现结果以应对空推送；默认开启，不能替代业务新鲜度判断。
    naming_push_empty_protection: bool,
    /// 是否在启动时加载发现磁盘缓存；默认关闭。
    naming_load_cache_at_start: bool,
    /// 是否在启动时加载配置磁盘缓存；默认关闭。
    config_load_cache_at_start: bool,
    /// 部署者管理的可信磁盘根目录，模块和 namespace 仅生成摘要子目录。
    cache_dir: Option<PathBuf>,
    /// 读取时是否优先使用环境属性；默认开启。
    env_first: bool,
    /// 客户端标签，应用名称由当前应用配置覆盖。
    labels: HashMap<String, String>,
    /// 客户端向服务端报告的协议实现标识。
    client_version: String,
    /// 传给认证插件的私有凭据与扩展属性，不应写入业务日志。
    auth_context: HashMap<String, String>,
    /// 已弃用的初始化重试上限；不控制连接建立后的持续恢复。
    #[deprecated]
    max_retries: Option<u32>,
}

impl ClientProps {
    /// 业务作用：按环境优先策略选择直接服务地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前地址副本；此步骤不执行连接。
    pub(crate) fn get_server_addr(&self) -> String {
        if self.env_first {
            get_value(
                ENV_NACOS_CLIENT_COMMON_SERVER_ADDRESS,
                self.server_addr.clone(),
            )
        } else {
            self.server_addr.clone()
        }
    }

    /// 业务作用：按环境优先策略选择服务列表解析入口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可选入口；未配置时使用直接服务列表。
    pub(crate) fn get_endpoint(&self) -> Option<String> {
        if self.env_first {
            get_value_option(ENV_NACOS_CLIENT_COMMON_ENDPOINT).or_else(|| self.endpoint.clone())
        } else {
            self.endpoint.clone()
        }
    }

    /// 业务作用：为连接来源选择稳定标识，endpoint 优先于直接服务地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前来源标识，不证明远端可用。
    pub(crate) fn get_address_identifier(&self) -> String {
        self.get_endpoint()
            .unwrap_or_else(|| self.get_server_addr())
    }

    /// 业务作用：读取显式 gRPC 端口覆盖，不受其它地址字段隐式改写。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可选端口；None 交由后续地址解析推导。
    pub(crate) fn get_remote_grpc_port(&self) -> Option<u16> {
        self.grpc_port
    }

    /// 业务作用：为协议请求补足公共命名空间的标准标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空 namespace 替换为 DEFAULT_NAMESPACE，其它原值保持不变。
    pub(crate) fn get_namespace_default_if_empty(&self) -> String {
        let namespace = self.get_namespace();
        if namespace.is_empty() {
            DEFAULT_NAMESPACE.to_owned()
        } else {
            namespace
        }
    }

    /// 业务作用：按环境优先策略取得当前逻辑命名空间。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：原始 namespace 副本，不执行路径解释。
    pub(crate) fn get_namespace(&self) -> String {
        if self.env_first {
            get_value(ENV_NACOS_CLIENT_COMMON_NAMESPACE, self.namespace.clone())
        } else {
            self.namespace.clone()
        }
    }

    /// 业务作用：按环境优先策略取得应用标签来源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：应用名称副本，不访问网络。
    pub(crate) fn get_app_name(&self) -> String {
        if self.env_first {
            get_value(ENV_NACOS_CLIENT_COMMON_APP_NAME, self.app_name.clone())
        } else {
            self.app_name.clone()
        }
    }

    /// 业务作用：取得空发现推送的保护策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前布尔策略；非法环境布尔值采用实例默认值。
    pub(crate) fn get_naming_push_empty_protection(&self) -> bool {
        if self.env_first {
            get_value_bool(
                ENV_NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION,
                self.naming_push_empty_protection,
            )
        } else {
            self.naming_push_empty_protection
        }
    }

    /// 业务作用：取得发现缓存的启动加载策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前开关；加载磁盘不代表取得实时实例资格。
    pub(crate) fn get_naming_load_cache_at_start(&self) -> bool {
        if self.env_first {
            get_value_bool(
                ENV_NACOS_CLIENT_NAMING_LOAD_CACHE_AT_START,
                self.naming_load_cache_at_start,
            )
        } else {
            self.naming_load_cache_at_start
        }
    }

    /// 业务作用：取得配置缓存的启动加载策略。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前开关；内容仍需后续服务端同步。
    pub(crate) fn get_config_load_cache_at_start(&self) -> bool {
        if self.env_first {
            get_value_bool(
                ENV_NACOS_CLIENT_CONFIG_LOAD_CACHE_AT_START,
                self.config_load_cache_at_start,
            )
        } else {
            self.config_load_cache_at_start
        }
    }

    /// 业务作用：选择部署者管理的磁盘缓存根，忽略空白环境目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：环境优先时选取非空 NACOS_CLIENT_CACHE_DIR，否则使用实例设置。
    pub(crate) fn get_cache_dir(&self) -> Option<PathBuf> {
        if self.env_first {
            get_value_option(ENV_NACOS_CLIENT_CACHE_DIR)
                .filter(|cache_dir| !cache_dir.trim().is_empty())
                .map(PathBuf::from)
                .or_else(|| self.cache_dir.clone())
        } else {
            self.cache_dir.clone()
        }
    }

    /// 业务作用：生成当前客户端标签并固定应用名称的权威来源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：标签副本，其中 app_name 由当前应用配置覆盖。
    pub(crate) fn get_labels(&self) -> HashMap<String, String> {
        let mut labels = self.labels.clone();
        labels.insert(KEY_LABEL_APP_NAME.to_string(), self.get_app_name());
        labels
    }

    /// 业务作用：提供握手中声明的客户端实现标识。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时确定的实现版本字符串副本。
    pub(crate) fn get_client_version(&self) -> String {
        self.client_version.clone()
    }

    /// 业务作用：汇总当前启用认证插件所需凭据，按环境优先策略覆盖同名项。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：私有认证属性副本；此步骤不执行登录。
    pub(crate) fn get_auth_context(&self) -> HashMap<String, String> {
        let mut auth_context = self.auth_context.clone();
        if self.env_first {
            #[cfg(feature = "auth-by-http")]
            self.get_http_auth_context(&mut auth_context);
            #[cfg(feature = "auth-by-aliyun")]
            self.get_aliyun_auth_context(&mut auth_context);
        }
        auth_context
    }

    /// 业务作用：把可用的环境用户名和密码合并到 HTTP 认证上下文。
    ///
    /// 参数说明：
    /// - `context`：调用方已有的私有认证上下文。
    ///
    /// 返回：原上下文就地更新；未提供的属性保持原值。
    #[cfg(feature = "auth-by-http")]
    fn get_http_auth_context(&self, context: &mut HashMap<String, String>) {
        if let Some(u) = get_value_option(ENV_NACOS_CLIENT_AUTH_USERNAME) {
            context.insert(crate::api::plugin::USERNAME.into(), u);
        }
        if let Some(p) = get_value_option(ENV_NACOS_CLIENT_AUTH_PASSWORD) {
            context.insert(crate::api::plugin::PASSWORD.into(), p);
        }
    }

    /// 业务作用：把环境 RAM 密钥和签名地域合并到认证上下文。
    ///
    /// 参数说明：
    /// - `context`：调用方已有的私有认证上下文。
    ///
    /// 返回：原上下文就地更新；未提供的属性保持原值。
    #[cfg(feature = "auth-by-aliyun")]
    fn get_aliyun_auth_context(&self, context: &mut HashMap<String, String>) {
        if let Some(ak) = get_value_option(ENV_NACOS_CLIENT_AUTH_ACCESS_KEY) {
            context.insert(crate::api::plugin::ACCESS_KEY.into(), ak);
        }
        if let Some(sk) = get_value_option(ENV_NACOS_CLIENT_AUTH_ACCESS_SECRET) {
            context.insert(crate::api::plugin::ACCESS_SECRET.into(), sk);
        }
        if let Some(sign_region_id) = get_value_option(ENV_NACOS_CLIENT_SIGN_REGION_ID) {
            context.insert(crate::api::plugin::SIGN_REGION_ID.into(), sign_region_id);
        }
    }

    /// 业务作用：读取兼容保留的初始化重试设置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：显式上限或 None，不改变运行期恢复策略。
    pub(crate) fn get_max_retries(&self) -> Option<u32> {
        #[allow(deprecated)]
        self.max_retries
    }
}

#[allow(clippy::new_without_default)]
impl ClientProps {
    /// 业务作用：建立 Nacos 客户端的默认配置，认证和网络连接留给服务构造流程。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：环境优先、空推送保护开启、磁盘预加载关闭且没有显式凭据的实例。
    pub fn new() -> Self {
        let env_project_version = env!("CARGO_PKG_VERSION");
        let client_version = format!("Nacos-Rust-Client:{}", env_project_version);

        ClientProps {
            server_addr: String::from(DEFAULT_SERVER_ADDR),
            endpoint: None,
            namespace: String::from(""),
            app_name: UNKNOWN.to_string(),
            naming_push_empty_protection: true,
            naming_load_cache_at_start: false,
            config_load_cache_at_start: false,
            cache_dir: None,
            env_first: true,
            labels: HashMap::default(),
            client_version,
            auth_context: HashMap::default(),
            grpc_port: None,
            #[allow(deprecated)]
            max_retries: None,
        }
    }

    /// 业务作用：配置直接服务列表，只有未选用 endpoint 时才作为地址来源。
    ///
    /// 参数说明：
    /// - `server_addr`：直接服务地址列表，未使用 endpoint 时按分隔规则解析。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn server_addr(mut self, server_addr: impl Into<String>) -> Self {
        self.server_addr = server_addr.into();
        self
    }

    /// 业务作用：配置服务列表解析入口；完整 URL 保留路径，仅缺失时补 namespace 查询参数。
    ///
    /// 参数说明：
    /// - `endpoint`：完整 URL 或裸地址；裸地址默认 /nacos/serverlist 和 8080 端口。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// 业务作用：显式覆盖远程 gRPC 端口，保留与 HTTP 端口分离的部署能力。
    ///
    /// 参数说明：
    /// - `grpc_port`：可选远程 gRPC 端口；省略时按服务端地址推导。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn remote_grpc_port(mut self, grpc_port: u16) -> Self {
        self.grpc_port = Some(grpc_port);
        self
    }

    /// 业务作用：配置逻辑命名空间，配置缓存按原始标识保持隔离。
    ///
    /// 参数说明：
    /// - `namespace`：原始逻辑 namespace；空值表示默认公共空间。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// 业务作用：设置用于客户端识别和标签投影的应用名称。
    ///
    /// 参数说明：
    /// - `app_name`：客户端所属应用名称，进入受控标签。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// 业务作用：选择是否对空发现推送保留已有缓存。
    ///
    /// 参数说明：
    /// - `naming_push_empty_protection`：是否保留已有发现结果以应对空推送；默认开启，不能替代业务新鲜度判断。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn naming_push_empty_protection(mut self, naming_push_empty_protection: bool) -> Self {
        self.naming_push_empty_protection = naming_push_empty_protection;
        self
    }

    /// 业务作用：选择发现服务是否读取启动磁盘缓存。
    ///
    /// 参数说明：
    /// - `naming_load_cache_at_start`：是否在启动时加载发现磁盘缓存；默认关闭。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn naming_load_cache_at_start(mut self, naming_load_cache_at_start: bool) -> Self {
        self.naming_load_cache_at_start = naming_load_cache_at_start;
        self
    }

    /// 业务作用：选择配置服务是否读取启动磁盘缓存。
    ///
    /// 参数说明：
    /// - `config_load_cache_at_start`：是否在启动时加载配置磁盘缓存；默认关闭。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn config_load_cache_at_start(mut self, config_load_cache_at_start: bool) -> Self {
        self.config_load_cache_at_start = config_load_cache_at_start;
        self
    }

    /// 业务作用：统一设置配置与发现两类缓存的启动加载策略。
    ///
    /// 参数说明：
    /// - `load_cache_at_start`：两类服务共用的预加载开关。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn load_cache_at_start(mut self, load_cache_at_start: bool) -> Self {
        self.naming_load_cache_at_start = load_cache_at_start;
        self.config_load_cache_at_start = load_cache_at_start;
        self
    }

    /// 业务作用：指定可信缓存根，业务 namespace 和键只能生成固定摘要路径。
    ///
    /// 参数说明：
    /// - `cache_dir`：部署者独占管理的目录；环境优先时 NACOS_CLIENT_CACHE_DIR 可覆盖。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(cache_dir.into());
        self
    }

    /// 业务作用：选择环境属性与实例设置之间的优先级。
    ///
    /// 参数说明：
    /// - `env_first`：true 时环境值优先；false 时仅使用实例设置。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn env_first(mut self, env_first: bool) -> Self {
        self.env_first = env_first;
        self
    }

    /// 业务作用：合并调用方的客户端标签，同名项由本次输入覆盖。
    ///
    /// 参数说明：
    /// - `labels`：待合并标签；应用名称仍在读取时使用 app_name 的配置。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels.extend(labels);
        self
    }

    /// 业务作用：为 HTTP 认证插件保存登录名称。
    ///
    /// 参数说明：
    /// - `username`：私有认证用户名，不作为发现服务名称。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    #[cfg(feature = "auth-by-http")]
    pub fn auth_username(mut self, username: impl Into<String>) -> Self {
        self.auth_context
            .insert(crate::api::plugin::USERNAME.into(), username.into());
        self
    }

    /// 业务作用：为 HTTP 认证插件保存登录密钥。
    ///
    /// 参数说明：
    /// - `password`：私有登录密码，禁止输出到业务日志。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    #[cfg(feature = "auth-by-http")]
    pub fn auth_password(mut self, password: impl Into<String>) -> Self {
        self.auth_context
            .insert(crate::api::plugin::PASSWORD.into(), password.into());
        self
    }

    /// 业务作用：为 RAM 认证插件保存访问密钥标识。
    ///
    /// 参数说明：
    /// - `access_key`：签名身份使用的访问标识。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    #[cfg(feature = "auth-by-aliyun")]
    pub fn auth_access_key(mut self, access_key: impl Into<String>) -> Self {
        self.auth_context
            .insert(crate::api::plugin::ACCESS_KEY.into(), access_key.into());
        self
    }

    /// 业务作用：为 RAM 认证插件保存签名密钥材料。
    ///
    /// 参数说明：
    /// - `access_secret`：私有签名密钥，不得写入日志。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    #[cfg(feature = "auth-by-aliyun")]
    pub fn auth_access_secret(mut self, access_secret: impl Into<String>) -> Self {
        self.auth_context.insert(
            crate::api::plugin::ACCESS_SECRET.into(),
            access_secret.into(),
        );
        self
    }

    /// 业务作用：固定 RAM 请求的签名地域。
    ///
    /// 参数说明：
    /// - `signature_region_id`：签名地域标识，需与服务端认证策略一致。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    #[cfg(feature = "auth-by-aliyun")]
    pub fn auth_signature_region_id(mut self, signature_region_id: impl Into<String>) -> Self {
        self.auth_context.insert(
            crate::api::plugin::SIGN_REGION_ID.into(),
            signature_region_id.into(),
        );
        self
    }

    /// 业务作用：向认证插件提供一项扩展上下文，同名属性由本次调用覆盖。
    ///
    /// 参数说明：
    /// - `key`：插件约定的属性名称。
    /// - `val`：插件使用的私有属性值。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    pub fn auth_ext(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.auth_context.insert(key.into(), val.into());
        self
    }

    /// 业务作用：保留初始化阶段的重试配置兼容入口。
    ///
    /// 参数说明：
    /// - `max_retries`：仅用于初始化的显式上限；运行期恢复不使用该值。
    ///
    /// 返回：保留其它配置的当前实例；实际网络或磁盘操作尚未执行。
    #[deprecated]
    #[allow(deprecated)]
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }
}
