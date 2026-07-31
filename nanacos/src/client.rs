//! 连接参数与共享构造原语。
//!
//! 配置中心与注册中心**各用独立 client**([`NacosConfigClient`](crate::NacosConfigClient) /
//! [`NacosDiscoveryClient`](crate::NacosDiscoveryClient)),只共享 [`NacosProps`]:这样"只用配置"的服务不会被迫初始化命名服务,
//! 反之亦然(对照 config 与 discovery 命名空间的分离)。

/// 连接 Nacos 所需参数(配置/注册两 client 共用;app 从自己的启动配置里读出来传进来)。
///
/// `Debug` 手写:**脱敏 username/password**,避免日志/排查时泄漏凭据。
///
/// `#[non_exhaustive]`:外部 crate **不能用结构体字面量构造**,必须走 [`NacosProps::new`] + 链式 `with_*`(或 `Default` + 改字段);
/// 这样本结构以后再加字段也【不会破坏】任何下游构造代码(向后兼容)。
#[derive(Clone)]
#[non_exhaustive]
pub struct NacosProps {
    /// 服务端地址 `"ip:port[,ip:port]"`。
    pub server_addr: String,
    /// 命名空间【ID】(非显示名;做环境/租户隔离)。public 空间传 `""`。
    pub namespace: String,
    /// 默认分组(配置/命名共用的默认值;配置侧 fetch/watch 仍可逐次显式传 group)。
    pub group: String,
    /// 应用名(Nacos 端来源标识/审计;不影响拉哪份配置)。
    pub app_name: String,
    /// 注册中心实例对外 IP。非空时由注册中心门面在 `register` 时覆盖 `Instance.ip`。
    /// 用于服务监听 `0.0.0.0`、本机多网卡、VPN/容器网卡等场景,避免注册不可拨号地址。
    pub discovery_ip: Option<String>,
    /// 鉴权用户名(Nacos 1.2+ 开启鉴权时必填)。
    pub username: String,
    /// 鉴权密码。
    pub password: String,
}

impl Default for NacosProps {
    /// 返回默认配置；用于未显式设置时提供稳定基线。
    fn default() -> Self {
        Self {
            server_addr: String::new(),
            namespace: String::new(),
            group: "DEFAULT_GROUP".to_string(),
            app_name: String::new(),
            discovery_ip: None,
            username: String::new(),
            password: String::new(),
        }
    }
}

impl NacosProps {
    /// 起手式:只给必填的 `server_addr`(`"ip:port[,ip:port]"`),其余取默认;再链式 `with_*` 覆盖。
    /// 配合 `#[non_exhaustive]`:这是外部 crate 构造 `NacosProps` 的推荐入口,字段增减都不破坏调用方。
    ///
    /// # 参数
    /// - `server_addr`: Nacos 服务端地址,支持 `"ip:port[,ip:port]"`;会 trim 首尾空白。
    pub fn new(server_addr: impl Into<String>) -> Self {
        Self {
            server_addr: trim_into(server_addr),
            ..Default::default()
        }
    }

    /// 命名空间【ID】(public 空间留空)。
    ///
    /// # 参数
    /// - `namespace`: Nacos namespace ID,不是控制台显示名;会 trim 首尾空白。
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = trim_into(namespace);
        self
    }

    /// 默认分组(配置/命名共用)。
    ///
    /// # 参数
    /// - `group`: 默认 Nacos group;会 trim 首尾空白。
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = trim_into(group);
        self
    }

    /// 应用名(Nacos 端来源标识/审计)。
    ///
    /// # 参数
    /// - `app_name`: Nacos 端展示和审计使用的应用名;会 trim 首尾空白。
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = trim_into(app_name);
        self
    }

    /// HTTP 鉴权用户名 + 密码(Nacos 1.2+ 开启鉴权时必填)。
    /// 用户名按账号名处理会 trim 首尾空白;密码是 opaque secret,保持原样不修改。
    ///
    /// # 参数
    /// - `username`: Nacos 鉴权用户名,会 trim 首尾空白。
    /// - `password`: Nacos 鉴权密码,作为敏感字符串原样保存不 trim。
    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = trim_into(username);
        self.password = password.into();
        self
    }

    /// 注册中心实例对外 IP(见字段说明);非空时在 `register` 覆盖 `Instance.ip`。
    ///
    /// # 参数
    /// - `discovery_ip`: 服务注册给消费者拨号的对外 IP;会 trim 首尾空白。
    pub fn with_discovery_ip(mut self, discovery_ip: impl Into<String>) -> Self {
        self.discovery_ip = Some(trim_into(discovery_ip));
        self
    }

    /// 同 [`with_discovery_ip`](Self::with_discovery_ip),但接受 `Option`:`Some` 设置、**`None` 是 no-op(保持当前值,不清空)**。
    /// 便于从可选配置直接链式传入(`...with_discovery_ip_opt(cfg.discovery_ip.as_deref())`),省去 `if let`。
    /// ⚠️ 要【清空】已设的 discovery_ip 用 [`without_discovery_ip`](Self::without_discovery_ip),不是传 `None`。
    ///
    /// # 参数
    /// - `discovery_ip`: 可选对外 IP;`Some` 时设置,`None` 时保持原值。
    pub fn with_discovery_ip_opt(mut self, discovery_ip: Option<impl Into<String>>) -> Self {
        if let Some(ip) = discovery_ip {
            self.discovery_ip = Some(trim_into(ip));
        }
        self
    }

    /// 清空 discovery_ip(回到"注册 IP 由 `Instance.ip` 决定")。与 `with_discovery_ip_opt(None)`(no-op)区分。
    pub fn without_discovery_ip(mut self) -> Self {
        self.discovery_ip = None;
        self
    }
}

/// 清理字符串参数两端空白；用于配置转换前统一输入形态。
///
/// # 参数
/// - `value`: Nacos 配置项中的字符串字段。
fn trim_into(value: impl Into<String>) -> String {
    value.into().trim().to_string()
}

impl std::fmt::Debug for NacosProps {
    /// 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 脱敏:username/password 不打印明文(只标记是否已设)。
        f.debug_struct("NacosProps")
            .field("server_addr", &self.server_addr)
            .field("namespace", &self.namespace)
            .field("group", &self.group)
            .field("app_name", &self.app_name)
            .field("discovery_ip", &self.discovery_ip.as_deref().unwrap_or(""))
            .field(
                "username",
                &if self.username.is_empty() {
                    "<empty>"
                } else {
                    "<set>"
                },
            )
            .field(
                "password",
                &if self.password.is_empty() {
                    "<empty>"
                } else {
                    "<redacted>"
                },
            )
            .finish()
    }
}

/// 连接参数基本校验(共享层 fail-fast,胜过把空地址交给 SDK 拿到晦涩的内部错误)。两 client 的 connect 共用。
#[cfg(feature = "nacos")]
pub(crate) fn validate_props(props: &NacosProps) -> anyhow::Result<()> {
    anyhow::ensure!(
        !props.server_addr.trim().is_empty(),
        "nacos: server_addr 不能为空"
    );
    anyhow::ensure!(!props.group.trim().is_empty(), "nacos: group 不能为空");
    ensure_no_outer_ws("server_addr", &props.server_addr)?;
    ensure_no_outer_ws("namespace", &props.namespace)?;
    ensure_no_outer_ws("group", &props.group)?;
    ensure_no_outer_ws("app_name", &props.app_name)?;
    ensure_no_outer_ws("username", &props.username)?;
    Ok(())
}

/// 校验 ensure no outer ws 约束；用于在进入运行流程前 fail-fast。
pub(crate) fn ensure_no_outer_ws(name: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value == value.trim(),
        "nacos: {name} 不能包含首尾空白,当前 {value:?}"
    );
    Ok(())
}

/// 由 [`NacosProps`] 造 SDK 的 `ClientProps`(config/naming builder 各取一份;builder 取所有权,不可复用)。
#[cfg(feature = "nacos")]
pub(crate) fn sdk_client_props(props: &NacosProps) -> nacos_sdk::api::props::ClientProps {
    nacos_sdk::api::props::ClientProps::new()
        .server_addr(&props.server_addr)
        .namespace(&props.namespace)
        .app_name(&props.app_name)
        .auth_username(&props.username)
        .auth_password(&props.password)
}

/// feature 关时各 connect 的统一 bail 文案。
#[cfg(not(feature = "nacos"))]
pub(crate) const DISABLED_MSG: &str =
    "nacos 功能未启用:本 binary 未带 `--features nacos` 编译;请用 `cargo build --features nacos` 重编译";
