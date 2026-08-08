//! 路由安全策略的稳定值类型。
//!
//! 过程宏只生成本模块中的静态数据，不接触运行时密钥或后端连接。运行时在监听端口前审计这些值，
//! 从而让“声明了保护但缺少实现”成为启动错误，而不是首个请求才暴露的问题。

/// 路由需要的登录身份强度。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthRequirement {
    /// 兼容旧路由的未声明状态；生产严格模式会在启动审计中拒绝。
    Unspecified,
    /// 请求必须得到有效身份，匿名结果会被拒绝。
    Required,
    /// 没有凭证可以匿名继续，但携带无效凭证仍会被拒绝。
    Optional,
    /// 路由明确公开，不访问身份后端。
    Public,
}

impl AuthRequirement {
    /// 业务作用：把宏参数中的稳定小写文本解析为身份要求。
    ///
    /// # 参数
    ///
    /// - `value`: 过程宏已经限制长度的静态文本；允许 `required`、`optional`、`public`。
    ///
    /// # 返回
    ///
    /// 返回对应身份要求；未知文本返回 `None`，由宏生成定位准确的编译错误。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "required" => Some(Self::Required),
            "optional" => Some(Self::Optional),
            "public" => Some(Self::Public),
            _ => None,
        }
    }
}

/// 路由某个加解密方向是否属于强制合同。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CryptoRequirement {
    /// 该方向不执行应用层加解密。
    Disabled,
    /// 该方向必须成功执行；任何缺失或错误都关闭请求。
    Required,
}

/// 路由是否要求执行请求重放占位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplayRequirement {
    /// 不执行重放占位；启动审计会把写路由上的该状态列为高风险。
    Disabled,
    /// 认证解密成功后必须完成原子占位，后端故障时关闭请求。
    Required,
}

/// 一次请求最终允许执行的加解密方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CryptoDirections {
    /// 是否处理请求密文。
    pub request: bool,
    /// 是否处理响应明文。
    pub response: bool,
}

impl CryptoDirections {
    /// 业务作用：从路由静态合同建立初始方向。
    ///
    /// # 参数
    ///
    /// - `policy`: 已匹配真实路由的不可变策略。
    ///
    /// # 返回
    ///
    /// 返回 condition 尚未收窄前的方向集合。
    pub fn declared(policy: &RoutePolicy) -> Self {
        Self {
            request: policy.crypto_request == CryptoRequirement::Required,
            response: policy.crypto_response == CryptoRequirement::Required,
        }
    }

    /// 业务作用：判断两个方向是否都被关闭。
    ///
    /// # 返回
    ///
    /// 请求和响应方向均为 `false` 时返回 `true`。
    pub fn is_empty(self) -> bool {
        !self.request && !self.response
    }
}

/// 一条实际注册路由的完整安全合同。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutePolicy {
    /// 稳定路由标识，默认由方法、路径模板与 handler 模块路径组成。
    pub route_id: &'static str,
    /// 大写 HTTP 方法文本。
    pub method: &'static str,
    /// Axum 使用的静态路径模板，不包含运行时参数值。
    pub path_template: &'static str,
    /// 业务处理函数的稳定模块路径，用于冲突诊断。
    pub handler: &'static str,
    /// 路由身份要求。
    pub auth: AuthRequirement,
    /// 身份 provider ID；省略时使用运行时默认值。
    pub auth_provider: Option<&'static str>,
    /// 身份 condition ID；省略时不改变静态身份要求。
    pub auth_condition: Option<&'static str>,
    /// 请求方向加密合同。
    pub crypto_request: CryptoRequirement,
    /// 响应方向加密合同。
    pub crypto_response: CryptoRequirement,
    /// 线协议 ID；启用任一方向时必须存在。
    pub crypto_protocol: Option<&'static str>,
    /// 密码 provider ID；启用任一方向时必须存在或可由默认值解析。
    pub crypto_provider: Option<&'static str>,
    /// 路由密钥域；启用任一方向时必须存在。
    pub crypto_key_scope: Option<&'static str>,
    /// 可选加密 condition ID，只能关闭已声明方向。
    pub crypto_condition: Option<&'static str>,
    /// 请求重放要求。
    pub replay: ReplayRequirement,
    /// 统一错误映射 profile。
    pub error_profile: &'static str,
    /// 跨语言稳定的协议受众字符串。
    pub audience: &'static str,
    /// legacy 响应结构合同；现代完整响应协议不需要该字段。
    pub response_contract: Option<&'static str>,
}

/// MappingRuntime 启动审计使用的路由与拦截器组合合同。
///
/// `RoutePolicy` 保持纯静态 HTTP/crypto 元数据；是否由 auth-stage interceptor 提供身份
/// 上下文属于 effective interceptor plan，必须在二者合并后再审计。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteSecurityPlan {
    /// 过程宏生成的静态端点策略。
    pub policy: RoutePolicy,
    /// effective plan 是否在解密前挂载至少一个 auth-stage interceptor。
    pub auth_interceptor: bool,
    /// auth interceptor 是否显式依赖当前快照中的 `AuthRuntime`。
    ///
    /// 该位会进入冻结路由合同，因此热更新不能用缺少 provider/condition
    /// 的快照绕过启动审计。
    pub auth_runtime: bool,
}

impl RoutePolicy {
    /// 业务作用：构造不带身份和加密策略的兼容路由。
    ///
    /// # 参数
    ///
    /// - `route_id`: 编译期生成的稳定路由标识。
    /// - `method`: 大写 HTTP 方法。
    /// - `path_template`: 静态路径模板。
    /// - `handler`: handler 的稳定模块路径。
    ///
    /// # 返回
    ///
    /// 返回可供旧 `register_all` 安全注册的空策略。
    pub const fn plain(
        route_id: &'static str,
        method: &'static str,
        path_template: &'static str,
        handler: &'static str,
    ) -> Self {
        Self {
            route_id,
            method,
            path_template,
            handler,
            auth: AuthRequirement::Unspecified,
            auth_provider: None,
            auth_condition: None,
            crypto_request: CryptoRequirement::Disabled,
            crypto_response: CryptoRequirement::Disabled,
            crypto_protocol: None,
            crypto_provider: None,
            crypto_key_scope: None,
            crypto_condition: None,
            replay: ReplayRequirement::Disabled,
            error_profile: "http-standard",
            audience: route_id,
            response_contract: None,
        }
    }

    /// 业务作用：判断路由是否声明了需要安全运行时参与的能力。
    ///
    /// # 返回
    ///
    /// 身份、任一加解密方向或依赖安全运行时解释的附属元数据已声明时返回 `true`。
    pub const fn has_security(self) -> bool {
        !matches!(self.auth, AuthRequirement::Unspecified)
            || self.auth_provider.is_some()
            || self.auth_condition.is_some()
            || !matches!(self.crypto_request, CryptoRequirement::Disabled)
            || !matches!(self.crypto_response, CryptoRequirement::Disabled)
            || self.crypto_protocol.is_some()
            || self.crypto_provider.is_some()
            || self.crypto_key_scope.is_some()
            || self.crypto_condition.is_some()
            || !matches!(self.replay, ReplayRequirement::Disabled)
            || self.response_contract.is_some()
    }

    /// 业务作用：判断路由是否启用了任一加解密方向。
    ///
    /// # 返回
    ///
    /// 请求或响应方向为 required 时返回 `true`。
    pub const fn has_crypto(self) -> bool {
        !matches!(self.crypto_request, CryptoRequirement::Disabled)
            || !matches!(self.crypto_response, CryptoRequirement::Disabled)
    }
}
