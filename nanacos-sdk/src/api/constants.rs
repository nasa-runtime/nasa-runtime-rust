/// 未显式指定时使用的本机 Nacos 地址。
pub const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:8848";

/// Nacos 默认 HTTP 端口。
pub const DEFAULT_SERVER_PORT: u16 = 8848;

/// Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
pub(crate) const DEFAULT_NAMESPACE: &str = "public";

/// 未显式指定时使用的 Nacos 默认分组。
pub const DEFAULT_GROUP: &str = "DEFAULT_GROUP";

/// 未取得属性值时使用的占位文本。
pub const UNKNOWN: &str = "unknown";

/// label AppName
pub(crate) const KEY_LABEL_APP_NAME: &str = "AppName";

/// label for crate inner common::remote
pub(crate) mod common_remote {

    pub const LABEL_SOURCE: &str = "source";

    /// LABEL_SOURCE value sdk
    pub const LABEL_SOURCE_SDK: &str = "sdk";

    pub const LABEL_MODULE: &str = "module";

    /// LABEL_MODULE value naming
    pub const LABEL_MODULE_NAMING: &str = "naming";

    /// LABEL_MODULE value config
    pub const LABEL_MODULE_CONFIG: &str = "config";
}

pub(crate) const ENV_NACOS_CLIENT_PROPS_FILE_PATH: &str = "NACOS_CLIENT_PROPS_FILE_PATH";

/// env `NACOS_CLIENT_COMMON_THREAD_CORES` to set nacos-client-thread-pool num, default 1
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

/// env `NACOS_CLIENT_CACHE_DIR` to set the root directory for on-disk caches
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

/// env `NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION`, default true
pub const ENV_NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION: &str =
    "NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION";

/// env `NACOS_CLIENT_NAMING_LOAD_CACHE_AT_START`, default false
pub const ENV_NACOS_CLIENT_NAMING_LOAD_CACHE_AT_START: &str =
    "NACOS_CLIENT_NAMING_LOAD_CACHE_AT_START";

/// env `NACOS_CLIENT_CONFIG_LOAD_CACHE_AT_START`, default false
pub const ENV_NACOS_CLIENT_CONFIG_LOAD_CACHE_AT_START: &str =
    "NACOS_CLIENT_CONFIG_LOAD_CACHE_AT_START";

/// env `NACOS_CLIENT_TLS_CA_CERT` to set custom CA certificate file path for TLS
pub const ENV_NACOS_CLIENT_TLS_CA_CERT: &str = "NACOS_CLIENT_TLS_CA_CERT";
