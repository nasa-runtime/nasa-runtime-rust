/// 未显式指定时使用的本机 Nacos 地址。
pub const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:8848";

/// Nacos 默认 HTTP 端口。
pub const DEFAULT_SERVER_PORT: u16 = 8848;

/// 空命名空间在客户端缓存和标识中使用的占位名称；线上请求仍读取原始 namespace，不据此改写为自定义空间。
pub(crate) const DEFAULT_NAMESPACE: &str = "public";

/// 未显式指定时使用的 Nacos 默认分组。
pub const DEFAULT_GROUP: &str = "DEFAULT_GROUP";

/// 未取得属性值时使用的占位文本。
pub const UNKNOWN: &str = "unknown";

/// 连接标签中的调用应用名称键。
pub(crate) const KEY_LABEL_APP_NAME: &str = "AppName";

/// 远端连接使用的来源及模块标签，用于区分配置与服务发现连接。
pub(crate) mod common_remote {

    /// 连接来源的标签键。
    pub const LABEL_SOURCE: &str = "source";

    /// 声明该连接来自 SDK 的来源标签值。
    pub const LABEL_SOURCE_SDK: &str = "sdk";

    /// 连接所属 SDK 模块的标签键。
    pub const LABEL_MODULE: &str = "module";

    /// 服务发现连接使用的模块标签值。
    pub const LABEL_MODULE_NAMING: &str = "naming";

    /// 配置订阅连接使用的模块标签值。
    pub const LABEL_MODULE_CONFIG: &str = "config";
}

/// 指定客户端属性文件路径的环境变量键。
pub(crate) const ENV_NACOS_CLIENT_PROPS_FILE_PATH: &str = "NACOS_CLIENT_PROPS_FILE_PATH";

/// SDK 公共 Tokio 运行时的工作线程数环境变量键；未设置、无法解析或为零时使用一个线程。
pub const ENV_NACOS_CLIENT_COMMON_THREAD_CORES: &str = "NACOS_CLIENT_COMMON_THREAD_CORES";

/// 覆盖Nacos 服务端地址的环境变量键。
pub const ENV_NACOS_CLIENT_COMMON_SERVER_ADDRESS: &str = "NACOS_CLIENT_SERVER_ADDRESS";

/// 覆盖Nacos HTTP 端口的环境变量键。
pub const ENV_NACOS_CLIENT_COMMON_SERVER_PORT: &str = "NACOS_CLIENT_SERVER_PORT";

/// 覆盖服务端地址查询入口的环境变量键。
pub const ENV_NACOS_CLIENT_COMMON_ENDPOINT: &str = "NACOS_CLIENT_ENDPOINT";

/// 覆盖Nacos 命名空间的环境变量键。
pub const ENV_NACOS_CLIENT_COMMON_NAMESPACE: &str = "NACOS_CLIENT_NAMESPACE";

/// 覆盖调用应用名称的环境变量键。
pub const ENV_NACOS_CLIENT_COMMON_APP_NAME: &str = "NACOS_CLIENT_APP_NAME";

/// 覆盖磁盘缓存根目录的环境变量键；仅在环境优先策略启用且值非空白时生效。
pub const ENV_NACOS_CLIENT_CACHE_DIR: &str = "NACOS_CLIENT_CACHE_DIR";

/// 覆盖登录用户名的环境变量键。
pub const ENV_NACOS_CLIENT_AUTH_USERNAME: &str = "NACOS_CLIENT_USERNAME";

/// 覆盖登录密码的环境变量键。
pub const ENV_NACOS_CLIENT_AUTH_PASSWORD: &str = "NACOS_CLIENT_PASSWORD";

/// 覆盖Aliyun 访问密钥标识的环境变量键。
pub const ENV_NACOS_CLIENT_AUTH_ACCESS_KEY: &str = "NACOS_CLIENT_ACCESS_KEY";

/// 覆盖Aliyun 签名密钥的环境变量键。
pub const ENV_NACOS_CLIENT_AUTH_ACCESS_SECRET: &str = "NACOS_CLIENT_SECRET_KEY";

/// 覆盖Aliyun 签名区域的环境变量键。
pub const ENV_NACOS_CLIENT_SIGN_REGION_ID: &str = "NACOS_CLIENT_SIGN_REGION_ID";

/// 覆盖服务发现空推送保护开关的环境变量键，实例默认开启。
pub const ENV_NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION: &str =
    "NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION";

/// 覆盖服务发现启动时加载磁盘缓存开关的环境变量键，实例默认关闭。
pub const ENV_NACOS_CLIENT_NAMING_LOAD_CACHE_AT_START: &str =
    "NACOS_CLIENT_NAMING_LOAD_CACHE_AT_START";

/// 覆盖配置客户端启动时加载磁盘缓存开关的环境变量键，实例默认关闭。
pub const ENV_NACOS_CLIENT_CONFIG_LOAD_CACHE_AT_START: &str =
    "NACOS_CLIENT_CONFIG_LOAD_CACHE_AT_START";

/// 指定 TLS 自定义 CA 证书文件路径的环境变量键，供连接凭据解析使用。
pub const ENV_NACOS_CLIENT_TLS_CA_CERT: &str = "NACOS_CLIENT_TLS_CA_CERT";
