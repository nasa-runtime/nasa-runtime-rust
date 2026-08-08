//! 与具体业务存储解耦的端点身份认证合同。
//!
//! 本模块只规定身份输入、认证结果、条件收窄和运行时注册表。会话字段、缓存策略与远程存储
//! 查询属于应用适配器，避免通用路由层反向依赖某一种登录系统。

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::http::{Extensions, HeaderMap, StatusCode};
use axum::response::Response;

use crate::{AuthRequirement, MappingBuildError, RoutePolicy};

/// provider 与 condition 返回的对象安全异步结果。
///
/// 生命周期 `'a` 把 future 限定在本次请求借用期内，既允许实现借用受限请求视图，也避免
/// 为了对象安全把短生命周期数据复制到全局状态。
pub type AuthFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 认证 provider 可以读取的只读请求视图。
#[derive(Clone, Copy)]
pub struct AuthInput<'a> {
    /// 大写 HTTP 方法，由已匹配路由提供，不接受客户端覆盖。
    pub method: &'a str,
    /// 已匹配路由的稳定标识，用于选择受控业务规则。
    pub route_id: &'a str,
    /// URI 路径，不包含 query，动态白名单只能对该字段做完整匹配。
    pub path: &'a str,
    /// 原始请求头只读视图；实现只能提取身份凭证，不得记录完整值。
    pub headers: &'a HeaderMap,
    /// 上游可信中间件写入的扩展只读视图，不允许 provider 修改其它安全上下文。
    pub extensions: &'a Extensions,
}

/// 身份 condition 可以读取的受限请求视图。
#[derive(Clone, Copy)]
pub struct AuthConditionInput<'a> {
    /// 大写 HTTP 方法，由路由静态元数据提供。
    pub method: &'a str,
    /// 已匹配路由的稳定标识，用于绑定白名单或灰度规则。
    pub route_id: &'a str,
    /// URI 路径，不包含 query，防止查询参数改变身份合同。
    pub path: &'a str,
    /// 请求头只读视图；condition 不得读取 body 或保存凭证。
    pub headers: &'a HeaderMap,
    /// 可信入口扩展只读视图，用于区分公网输入与内部控制面标记。
    pub extensions: &'a Extensions,
}

/// 认证成功后写入 request extensions 的最小身份上下文。
#[derive(Clone)]
pub struct AuthContext {
    /// 业务主体稳定标识；不得放入指标标签或未经脱敏的日志。
    pub subject: Arc<str>,
    /// 已验证租户域；modern-v2 会把它绑定进密钥派生和 AAD。
    pub tenant: Arc<str>,
    /// 认证方式的低基数名称，例如会话或服务身份。
    pub authentication_kind: &'static str,
    /// 应用自行定义的最小主体对象，不应包含密码、完整凭证或无关隐私字段。
    pub principal: Arc<dyn Any + Send + Sync>,
}

impl std::fmt::Debug for AuthContext {
    /// 业务作用：输出不含主体值和业务对象的认证摘要。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthContext")
            .field("authentication_kind", &self.authentication_kind)
            .finish_non_exhaustive()
    }
}

impl AuthContext {
    /// 业务作用：按应用 DTO 类型读取认证主体。
    ///
    /// # 类型参数
    ///
    /// - `T`: provider 写入的具体主体类型，必须可在线程间安全共享。
    ///
    /// # 返回
    ///
    /// 类型一致时返回主体引用，否则返回 `None`；该操作不会复制敏感字段。
    pub fn principal_as<T>(&self) -> Option<&T>
    where
        T: Any + Send + Sync,
    {
        self.principal.downcast_ref::<T>()
    }
}

/// 短路响应是否继续执行路由的响应加密合同。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortCircuitCrypto {
    /// 在可信密码上下文建立前直接返回结构化明文错误。
    Plaintext,
    /// 已建立可信密码上下文后，按静态 route response policy 处理响应。
    ApplyRouteResponsePolicy,
}

/// provider 直接完成请求时返回的受控短路结果。
pub struct AuthShortCircuit {
    /// 已由应用适配器构造的结构化 HTTP 响应，不得包含来源不明的密文。
    pub response: Response,
    /// endpoint composer 对该响应采用的静态加密行为。
    pub crypto: ShortCircuitCrypto,
}

/// provider 对一次请求的认证判断。
pub enum AuthOutcome {
    /// 凭证有效，后续步骤可读取最小身份上下文。
    Authenticated(AuthContext),
    /// 请求未携带凭证；只有 optional 路由可以继续。
    Anonymous,
    /// 业务适配器已产生完整响应，例如禁用账户或 address-read 空模板。
    ShortCircuit(AuthShortCircuit),
}

/// endpoint composer 使用的规范化身份决策。
pub enum AuthDecision {
    /// 路由公开或 optional 请求没有凭证。
    Anonymous,
    /// 认证成功并应写入请求扩展的身份上下文。
    Authenticated(AuthContext),
    /// provider 已决定提前终止请求。
    ShortCircuit(AuthShortCircuit),
}

/// 认证失败的内部稳定类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthErrorKind {
    /// required 路由没有可用凭证。
    Missing,
    /// 请求携带了格式错误、过期或不存在的凭证。
    Invalid,
    /// 认证主体已被业务规则禁用。
    Disabled,
    /// 会话存储、远程配置或本地资源暂时不可用。
    Backend,
    /// provider 或 condition 违反静态路由合同。
    Policy,
}

/// 不携带凭证原文和后端错误详情的认证错误。
#[derive(Debug, Clone)]
pub struct AuthError {
    /// 供统一 HTTP profile 映射的内部类别。
    pub kind: AuthErrorKind,
    /// 低基数内部原因，仅用于受控日志，不得拼接外部输入。
    pub internal_reason: &'static str,
}

impl AuthError {
    /// 业务作用：建立一个不包含敏感输入的认证错误。
    ///
    /// # 参数
    ///
    /// - `kind`: 统一错误映射使用的稳定类别。
    /// - `internal_reason`: 由实现者写死的低基数原因，不得包含凭证或后端响应。
    ///
    /// # 返回
    ///
    /// 返回可安全传过 endpoint composer 的内部错误。
    pub const fn new(kind: AuthErrorKind, internal_reason: &'static str) -> Self {
        Self {
            kind,
            internal_reason,
        }
    }

    /// 业务作用：返回标准 HTTP profile 对应的状态码。
    ///
    /// # 返回
    ///
    /// 缺失或无效凭证映射为 401，禁用映射为 403，依赖与策略错误映射为 503。
    pub const fn status(&self) -> StatusCode {
        match self.kind {
            AuthErrorKind::Missing | AuthErrorKind::Invalid => StatusCode::UNAUTHORIZED,
            AuthErrorKind::Disabled => StatusCode::FORBIDDEN,
            AuthErrorKind::Backend | AuthErrorKind::Policy => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl std::fmt::Display for AuthError {
    /// 业务作用：输出不含凭证与后端细节的内部类别。
    ///
    /// # 参数
    ///
    /// - `formatter`: 错误链提供的输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "身份认证失败：{:?}", self.kind)
    }
}

impl std::error::Error for AuthError {}

/// 应用实现的身份认证扩展点。
pub trait AuthProvider: Send + Sync + 'static {
    /// 业务作用：返回注册表使用的稳定 provider ID。
    ///
    /// # 返回
    ///
    /// 返回非空、有限长度的 ASCII 标识，不得包含实例地址或凭证信息。
    fn id(&self) -> &'static str;

    /// 业务作用：在监听端口前检查 provider 对当前路由的应用侧依赖。
    ///
    /// # 参数
    ///
    /// - `route`: 过程宏生成的静态路由合同，不含任何请求或凭证数据。
    ///
    /// # 返回
    ///
    /// 默认实现不增加约束；维护 route registry 的应用适配器应在缺少稳定 route ID 时拒绝启动。
    fn audit_route(&self, _route: &RoutePolicy) -> Result<(), MappingBuildError> {
        Ok(())
    }

    /// 业务作用：根据受限请求视图认证一次请求。
    ///
    /// # 参数
    ///
    /// - `input`: 只在本次调用期有效的请求视图；header 可能含敏感凭证，禁止持久化和日志输出。
    ///
    /// # 返回
    ///
    /// 返回认证、匿名或受控短路结果；后端错误必须 fail closed。
    fn authenticate<'a>(
        &'a self,
        input: AuthInput<'a>,
    ) -> AuthFuture<'a, Result<AuthOutcome, AuthError>>;
}

/// 应用实现的身份要求条件扩展点。
pub trait AuthCondition: Send + Sync + 'static {
    /// 业务作用：返回注册表使用的稳定 condition ID。
    ///
    /// # 返回
    ///
    /// 返回非空、有限长度的 ASCII 标识。
    fn id(&self) -> &'static str;

    /// 业务作用：依据可信元数据收窄静态身份要求。
    ///
    /// # 参数
    ///
    /// - `input`: 不含 body 的请求视图，生命周期限定为本次调用。
    /// - `declared`: 路由静态声明的上限，condition 不得提升到更强的未审计要求。
    ///
    /// # 返回
    ///
    /// 返回本请求的有效身份要求；错误时 endpoint composer 关闭请求。
    fn requirement<'a>(
        &'a self,
        input: AuthConditionInput<'a>,
        declared: AuthRequirement,
    ) -> AuthFuture<'a, Result<AuthRequirement, AuthError>>;
}

/// 构建后不可变的身份 provider 与 condition 注册表。
pub struct AuthRuntime {
    providers: HashMap<&'static str, Arc<dyn AuthProvider>>,
    conditions: HashMap<&'static str, Arc<dyn AuthCondition>>,
    default_provider: Option<&'static str>,
}

impl std::fmt::Debug for AuthRuntime {
    /// 业务作用：输出注册数量，避免泄露适配器内部缓存与连接状态。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthRuntime")
            .field("provider_count", &self.providers.len())
            .field("condition_count", &self.conditions.len())
            .finish_non_exhaustive()
    }
}

impl AuthRuntime {
    /// 业务作用：从完整注册表建立不可变身份运行时。
    ///
    /// # 参数
    ///
    /// - `providers`: 应用启动期创建的有限 provider 集合，ID 必须唯一。
    /// - `conditions`: 应用启动期创建的有限 condition 集合，ID 必须唯一。
    /// - `default_provider`: 路由没有显式指定时使用的 provider ID。
    ///
    /// # 返回
    ///
    /// 配置一致时返回运行时；重复 ID、空 ID 或不存在的默认项会拒绝启动。
    pub fn new(
        providers: Vec<Arc<dyn AuthProvider>>,
        conditions: Vec<Arc<dyn AuthCondition>>,
        default_provider: Option<&'static str>,
    ) -> Result<Self, MappingBuildError> {
        let mut provider_map = HashMap::with_capacity(providers.len());
        for provider in providers {
            validate_component_id("auth provider", provider.id())?;
            if provider_map.insert(provider.id(), provider).is_some() {
                return Err(MappingBuildError::new("auth provider ID 重复"));
            }
        }
        let mut condition_map = HashMap::with_capacity(conditions.len());
        for condition in conditions {
            validate_component_id("auth condition", condition.id())?;
            if condition_map.insert(condition.id(), condition).is_some() {
                return Err(MappingBuildError::new("auth condition ID 重复"));
            }
        }
        if let Some(id) = default_provider {
            if !provider_map.contains_key(id) {
                return Err(MappingBuildError::new(format!(
                    "默认 auth provider {id} 未注册"
                )));
            }
        }
        Ok(Self {
            providers: provider_map,
            conditions: condition_map,
            default_provider,
        })
    }

    /// 业务作用：在监听端口前检查一条路由的身份依赖。
    ///
    /// # 参数
    ///
    /// - `route`: 过程宏生成的不可变路由合同，不包含请求数据。
    ///
    /// # 返回
    ///
    /// provider 与 condition 都可解析时返回空值，否则拒绝启动。
    pub fn audit(&self, route: &RoutePolicy) -> Result<(), MappingBuildError> {
        let provider_id = route
            .auth_provider
            .or(self.default_provider)
            .ok_or_else(|| {
                MappingBuildError::new(format!("路由 {} 缺少 auth provider", route.route_id))
            })?;
        let provider = self.providers.get(provider_id).ok_or_else(|| {
            MappingBuildError::new(format!(
                "路由 {} 引用未注册 auth provider {}",
                route.route_id, provider_id
            ))
        })?;
        provider.audit_route(route)?;
        if let Some(condition_id) = route.auth_condition {
            if !self.conditions.contains_key(condition_id) {
                return Err(MappingBuildError::new(format!(
                    "路由 {} 引用未注册 auth condition {}",
                    route.route_id, condition_id
                )));
            }
        }
        Ok(())
    }

    /// 业务作用：按固定顺序执行身份 condition 与 provider。
    ///
    /// # 参数
    ///
    /// - `route`: 已通过启动审计的静态路由合同。
    /// - `input`: 本次请求的受限认证视图，header 借用只在 await 期间有效。
    ///
    /// # 返回
    ///
    /// 返回规范化身份决策；condition/provider 缺失和后端故障均 fail closed。
    pub async fn evaluate(
        &self,
        route: &RoutePolicy,
        input: AuthInput<'_>,
    ) -> Result<AuthDecision, AuthError> {
        let mut requirement = route.auth;
        if let Some(condition_id) = route.auth_condition {
            let condition = self
                .conditions
                .get(condition_id)
                .ok_or_else(|| AuthError::new(AuthErrorKind::Policy, "auth-condition-missing"))?;
            let condition_input = AuthConditionInput {
                method: input.method,
                route_id: input.route_id,
                path: input.path,
                headers: input.headers,
                extensions: input.extensions,
            };
            requirement = condition.requirement(condition_input, requirement).await?;
            if !requirement_within_declared(route.auth, requirement) {
                return Err(AuthError::new(
                    AuthErrorKind::Policy,
                    "auth-condition-expanded",
                ));
            }
        }

        if requirement == AuthRequirement::Public {
            return Ok(AuthDecision::Anonymous);
        }
        if requirement == AuthRequirement::Unspecified {
            return Err(AuthError::new(
                AuthErrorKind::Policy,
                "auth-requirement-unspecified",
            ));
        }

        let provider_id = route
            .auth_provider
            .or(self.default_provider)
            .ok_or_else(|| AuthError::new(AuthErrorKind::Policy, "auth-provider-unspecified"))?;
        let provider = self
            .providers
            .get(provider_id)
            .ok_or_else(|| AuthError::new(AuthErrorKind::Policy, "auth-provider-missing"))?;
        match provider.authenticate(input).await? {
            AuthOutcome::Authenticated(context) => {
                validate_auth_context(&context)?;
                Ok(AuthDecision::Authenticated(context))
            }
            AuthOutcome::Anonymous if requirement == AuthRequirement::Optional => {
                Ok(AuthDecision::Anonymous)
            }
            AuthOutcome::Anonymous => Err(AuthError::new(AuthErrorKind::Missing, "auth-missing")),
            AuthOutcome::ShortCircuit(short_circuit) => {
                Ok(AuthDecision::ShortCircuit(short_circuit))
            }
        }
    }
}

/// 业务作用：校验应用 provider 返回的身份上下文不会把空值、控制字符或无界文本带入密钥域和业务层。
///
/// # 参数
///
/// - `context`: provider 刚返回、尚未写入 request extensions 的最小身份对象。
///
/// # 返回
///
/// subject、tenant 和认证方式满足长度与字符边界时返回空值；适配器违反合同时返回 Policy，
/// 请求不会进入解密或 handler。
pub(crate) fn validate_auth_context(context: &AuthContext) -> Result<(), AuthError> {
    let bounded_text = |value: &str, maximum: usize| {
        !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
    };
    let valid_kind = bounded_text(context.authentication_kind, 64)
        && context
            .authentication_kind
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if bounded_text(&context.subject, 256) && bounded_text(&context.tenant, 128) && valid_kind {
        Ok(())
    } else {
        Err(AuthError::new(
            AuthErrorKind::Policy,
            "auth-context-invalid",
        ))
    }
}

/// 业务作用：判断 condition 返回值是否仍在路由静态声明允许的范围内。
///
/// # 参数
///
/// - `declared`: 启动审计可见的身份上限。
/// - `effective`: condition 根据可信元数据计算的本请求要求。
///
/// # 返回
///
/// required 可收窄为任意显式状态，optional 只能保持 optional 或 public，其它状态必须保持不变。
fn requirement_within_declared(declared: AuthRequirement, effective: AuthRequirement) -> bool {
    match declared {
        AuthRequirement::Required => effective != AuthRequirement::Unspecified,
        AuthRequirement::Optional => {
            matches!(
                effective,
                AuthRequirement::Optional | AuthRequirement::Public
            )
        }
        AuthRequirement::Public => effective == AuthRequirement::Public,
        AuthRequirement::Unspecified => effective == AuthRequirement::Unspecified,
    }
}

/// 业务作用：校验启动配置中的有限 ASCII 组件标识。
///
/// # 参数
///
/// - `kind`: 错误消息使用的组件类别，不含外部输入。
/// - `id`: provider 或 condition 暴露的静态 ID，最大 64 字节。
///
/// # 返回
///
/// 合法时返回空值；空值、过长或非安全 ASCII 字符会拒绝启动。
fn validate_component_id(kind: &str, id: &str) -> Result<(), MappingBuildError> {
    let valid = !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(MappingBuildError::new(format!("{kind} ID 非法")))
    }
}
