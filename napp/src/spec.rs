use std::collections::HashSet;
#[cfg(feature = "web")]
use std::sync::Arc;

use crate::{ApplicationError, ApplicationMode, ApplicationPhase, ApplicationResult, ComponentId};

#[cfg(feature = "web")]
use crate::Application;

/// 一条自动收集路由的稳定诊断元数据。
///
/// 元数据不持有处理函数，只用于在构造底层路由前完成确定性冲突检查。
#[derive(Debug, Clone, Copy)]
#[cfg(feature = "web")]
pub struct RouteMeta {
    /// 规范化的大写 HTTP 方法，用于同一路径的方法集合冲突检查。
    pub method: &'static str,
    /// 以 `/` 开头的路由模板，用于跨方法结构冲突检查。
    pub path: &'static str,
    /// 完整处理函数身份，只进入稳定冲突诊断，不参与请求分发。
    pub handler: &'static str,
    /// 响应媒体类型；未声明时生成器采用 JSON 的保守缺省。
    pub produces: Option<&'static str>,
    /// 请求媒体类型；未声明时不生成 requestBody。
    pub consumes: Option<&'static str>,
    /// 显式请求 DTO schema 工厂。
    pub request_schema: Option<naweb::ApiSchemaFactory>,
    /// 显式响应 DTO schema 工厂。
    pub response_schema: Option<naweb::ApiSchemaFactory>,
    /// 显式 query 参数集工厂。
    pub query_parameters: Option<naweb::ApiParametersFactory>,
    /// 显式 header 参数集工厂。
    pub header_parameters: Option<naweb::ApiParametersFactory>,
    /// 主要成功 HTTP 状态码。
    pub success_status: u16,
    /// 额外响应集工厂。
    pub additional_responses: Option<naweb::ApiResponsesFactory>,
    /// 是否为流式响应。
    pub streaming: bool,
    /// 端点静态身份合同是否要求认证。
    pub auth_required: bool,
}

/// 业务二进制内生成的路由元数据投影函数。
#[cfg(feature = "web")]
pub type WebRouteMetaFactory = fn() -> Vec<RouteMeta>;

/// napp Ready 阶段交给业务二进制自动路由工厂的完整构建上下文。
#[cfg(feature = "web")]
pub struct WebBuildContext {
    application: Application,
    mapping_runtime: Arc<naweb::MappingRuntime>,
    mapping_plan: naweb::MappingPlan<Application>,
}

#[cfg(feature = "web")]
impl WebBuildContext {
    /// 建立同源 Application、MappingRuntime 和已封口 MappingPlan 的构建上下文。
    pub(crate) fn new(
        application: Application,
        mapping_runtime: Arc<naweb::MappingRuntime>,
        mapping_plan: naweb::MappingPlan<Application>,
    ) -> Self {
        Self {
            application,
            mapping_runtime,
            mapping_plan,
        }
    }

    /// 调用业务 crate 中单态化的 `try_register_all`，并把 mapping 构建错误收敛为 Web Ready 错误。
    pub fn build<F>(self, factory: F) -> ApplicationResult<axum::Router<Application>>
    where
        F: FnOnce(
            axum::Router<Application>,
            Arc<naweb::MappingRuntime>,
            naweb::MappingPlan<Application>,
            Application,
        ) -> Result<axum::Router<Application>, naweb::MappingBuildError>,
    {
        factory(
            axum::Router::new(),
            self.mapping_runtime,
            self.mapping_plan,
            self.application,
        )
        .map_err(|error| {
            ApplicationError::with_source(
                ComponentId::Web,
                ApplicationPhase::Ready,
                "mapping route registration failed",
                error,
            )
        })
    }
}

/// 业务二进制内生成的自动路由构造函数。
///
/// 返回的是**尚未补齐状态**的 `Router<Application>`：`configure_router` 注册的定制、框架探针都必须在
/// `with_state` 之前作用于同一个类型，因此状态由 Web 组件在装配末尾统一补齐，而不是在业务 crate 里补。
#[cfg(feature = "web")]
pub type WebRouterFactory = fn(WebBuildContext) -> ApplicationResult<axum::Router<Application>>;

/// 属性入口生成的静态应用描述，保存进程元数据、组件顺序和业务二进制内的 Web 工厂。
#[derive(Debug, Clone, Copy)]
pub struct ApplicationSpec {
    components: &'static [ComponentId],
    default_name: &'static str,
    #[cfg(feature = "web")]
    web_route_meta: Option<WebRouteMetaFactory>,
    #[cfg(feature = "web")]
    web_factory: Option<WebRouterFactory>,
}

impl ApplicationSpec {
    /// 创建只包含静态组件声明的应用描述。
    ///
    /// # 参数
    ///
    /// - `components`：属性入口按源码顺序生成的静态组件切片。
    pub const fn new(components: &'static [ComponentId]) -> Self {
        Self {
            components,
            default_name: "application",
            #[cfg(feature = "web")]
            web_route_meta: None,
            #[cfg(feature = "web")]
            web_factory: None,
        }
    }

    /// 设置配置未声明名称时使用的编译期缺省名。
    ///
    /// # 参数
    ///
    /// - `default_name`：通常来自业务包元数据、trim 后必须非空的静态名称。
    pub const fn with_default_name(mut self, default_name: &'static str) -> Self {
        self.default_name = default_name;
        self
    }

    /// 设置由业务二进制生成的自动路由元数据投影函数。
    ///
    /// # 参数
    ///
    /// - `factory`：每次调用都返回同一组静态路由语义的无捕获函数。
    #[cfg(feature = "web")]
    pub const fn with_web_route_meta(mut self, factory: WebRouteMetaFactory) -> Self {
        self.web_route_meta = Some(factory);
        self
    }

    /// 设置由业务二进制生成的 Web 路由构造函数。
    ///
    /// # 参数
    ///
    /// - `factory`：接收统一 Application 状态并返回已补齐状态的路由。
    #[cfg(feature = "web")]
    pub const fn with_web_factory(mut self, factory: WebRouterFactory) -> Self {
        self.web_factory = Some(factory);
        self
    }

    /// 返回保持源码声明顺序的组件切片。
    ///
    /// # 参数
    ///
    /// 本方法无参数；运行时校验不会重排该切片。
    pub fn components(&self) -> &'static [ComponentId] {
        self.components
    }

    /// 返回同步 preflight 使用的编译期缺省应用名。
    ///
    /// # 参数
    ///
    /// 本方法无参数；名称不会写回配置树。
    pub fn default_name(&self) -> &'static str {
        self.default_name
    }

    /// 校验重复声明和内置组件的静态顺序约束。
    ///
    /// # 参数
    ///
    /// 本方法无参数；错误直接拒绝启动而不会静默拓扑重排。
    pub fn validate(&self) -> ApplicationResult<()> {
        if self.default_name.trim().is_empty() {
            return Err(spec_error("application default name cannot be empty"));
        }

        validate_component_order(self.components)
    }

    /// 校验声明组件所需的业务二进制工厂是否已经同时提供。
    ///
    /// # 参数
    ///
    /// 本方法无参数；属性入口生成工厂，手写描述缺失工厂时会在创建运行时前失败。
    pub(crate) fn validate_runtime_bindings(&self) -> ApplicationResult<()> {
        #[cfg(feature = "web")]
        {
            let declares_web = self.components.contains(&ComponentId::Web);
            let has_route_meta = self.web_route_meta.is_some();
            let has_web_factory = self.web_factory.is_some();
            if declares_web && !(has_route_meta && has_web_factory) {
                return Err(spec_error(
                    "web component requires route metadata and router factories",
                ));
            }
            if !declares_web && (has_route_meta || has_web_factory) {
                return Err(spec_error(
                    "web factories cannot be installed without declaring the web component",
                ));
            }
            Ok(())
        }
        #[cfg(not(feature = "web"))]
        {
            if self.components.contains(&ComponentId::Web) {
                return Err(spec_error(
                    "web component requires the runtime web capability",
                ));
            }
            Ok(())
        }
    }

    /// 返回 Web 组件使用的路由元数据投影函数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只有运行时绑定校验通过且声明 Web 时才返回值。
    #[cfg(feature = "web")]
    pub(crate) fn web_route_meta(&self) -> Option<WebRouteMetaFactory> {
        self.web_route_meta
    }

    /// 返回 Web 组件使用的业务路由构造函数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；函数指针不捕获业务状态，具体状态由调用参数传入。
    #[cfg(feature = "web")]
    pub(crate) fn web_factory(&self) -> Option<WebRouterFactory> {
        self.web_factory
    }

    /// 业务作用：根据长生命周期组件把 auto 固定为 Service 或 Batch，避免守护任务被批模式提前结束。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：声明 Saga、Kafka、Outbox、Web、长连接、注册发现或调度时返回 Service，否则返回 Batch。
    pub(crate) fn resolve_auto_mode(&self) -> ApplicationMode {
        if self.components.iter().any(|component| {
            matches!(
                component,
                ComponentId::Saga
                    | ComponentId::Kafka
                    | ComponentId::Outbox
                    | ComponentId::Web
                    | ComponentId::Ws
                    | ComponentId::NacosDiscovery
                    | ComponentId::Scheduling
            )
        }) {
            ApplicationMode::Service
        } else {
            ApplicationMode::Batch
        }
    }

    /// 业务作用：校验显式模式与组件生命周期是否相容，阻止长驻组件进入会主动结束的批模式。
    ///
    /// 参数说明：
    /// - `mode`：ApplicationSettings 已解析的最终 Service 或 Batch 模式。
    ///
    /// 返回：模式与组件相容时成功；Batch 包含任一长生命周期组件时返回配置错误。
    pub(crate) fn validate_mode(&self, mode: ApplicationMode) -> ApplicationResult<()> {
        if mode == ApplicationMode::Batch {
            if let Some(component) = self.components.iter().find(|component| {
                matches!(
                    component,
                    ComponentId::Saga
                        | ComponentId::Kafka
                        | ComponentId::Outbox
                        | ComponentId::Web
                        | ComponentId::Ws
                        | ComponentId::NacosDiscovery
                        | ComponentId::Scheduling
                )
            }) {
                return Err(spec_error(format!(
                    "batch mode cannot declare long-lived component `{component}`"
                )));
            }
        }
        Ok(())
    }
}

/// 业务作用：校验组件身份唯一性以及全部内置顺序约束，保证依赖先启动且后停机。
///
/// 属性生成的静态描述与低层 Runner 共用该函数，防止手写组件表绕过编译期检查后获得不同的
/// 启动语义。约束只在两端组件同时存在时生效，不会强制纯本地应用声明配置中心。
///
/// 参数说明：
/// - `components`：保持业务声明顺序的组件身份切片。
///
/// 返回：身份唯一且依赖顺序安全时成功；重复或逆序时返回启动前配置错误。
pub(crate) fn validate_component_order(components: &[ComponentId]) -> ApplicationResult<()> {
    let mut seen = HashSet::new();
    for (index, component) in components.iter().copied().enumerate() {
        if !seen.insert(component) {
            return Err(spec_error(format!(
                "application component `{component}` is declared more than once"
            )));
        }
        if component == ComponentId::Log && index != 0 {
            return Err(spec_error(
                "invalid application component order: `log` must be declared first",
            ));
        }
    }

    if components.contains(&ComponentId::Saga)
        && (!components.contains(&ComponentId::Db) || !components.contains(&ComponentId::Outbox))
    {
        return Err(spec_error(
            "component `saga` requires managed `db` and `outbox` components",
        ));
    }
    if components.contains(&ComponentId::Outbox) && !components.contains(&ComponentId::Db) {
        return Err(spec_error(
            "component `outbox` requires managed `db` to be declared",
        ));
    }

    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Db)?;
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Saga)?;
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Redis)?;
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Kafka)?;
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Web)?;
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Ws)?;
    ensure_before_if_both(
        components,
        ComponentId::NacosConfig,
        ComponentId::NacosDiscovery,
    )?;
    ensure_before_if_both(
        components,
        ComponentId::NacosConfig,
        ComponentId::Scheduling,
    )?;
    ensure_before_if_both(components, ComponentId::Web, ComponentId::NacosDiscovery)?;
    ensure_before_if_both(components, ComponentId::Db, ComponentId::Kafka)?;
    ensure_before_if_both(components, ComponentId::Db, ComponentId::Saga)?;
    ensure_before_if_both(components, ComponentId::Db, ComponentId::Outbox)?;
    ensure_before_if_both(components, ComponentId::Saga, ComponentId::Kafka)?;
    ensure_before_if_both(components, ComponentId::Saga, ComponentId::Outbox)?;
    ensure_before_if_both(components, ComponentId::Saga, ComponentId::Web)?;
    ensure_before_if_both(components, ComponentId::Saga, ComponentId::Ws)?;
    ensure_before_if_both(components, ComponentId::Redis, ComponentId::Kafka)?;
    ensure_before_if_both(components, ComponentId::Kafka, ComponentId::Web)?;
    ensure_before_if_both(components, ComponentId::Kafka, ComponentId::Outbox)?;
    ensure_before_if_both(components, ComponentId::Outbox, ComponentId::Web)?;
    ensure_before_if_both(components, ComponentId::Outbox, ComponentId::Ws)?;
    ensure_before_if_both(components, ComponentId::Outbox, ComponentId::NacosDiscovery)?;
    ensure_before_if_both(components, ComponentId::Outbox, ComponentId::Scheduling)?;
    ensure_before_if_both(components, ComponentId::Kafka, ComponentId::Ws)?;
    ensure_before_if_both(components, ComponentId::Kafka, ComponentId::NacosDiscovery)?;
    ensure_before_if_both(components, ComponentId::Kafka, ComponentId::Scheduling)?;
    // telemetry:配置中心先于 telemetry(读最终 overlay),telemetry 先于所有 span 生产者/流量入口
    // (Start 发布 exporter 早于它们的 Start/Ready)。telemetry 不强制 Service、可用于 Batch。
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Telemetry)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Db)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Saga)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Redis)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Kafka)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Outbox)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Web)?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Ws)?;
    ensure_before_if_both(
        components,
        ComponentId::Telemetry,
        ComponentId::NacosDiscovery,
    )?;
    ensure_before_if_both(components, ComponentId::Telemetry, ComponentId::Scheduling)?;
    // cache:redis 先于 cache(配 redis_ref 时复用其连接),cache 先于 kafka/web。cache 不强制
    // Service 和 Batch 都可使用该能力；仅在同时声明 Redis 时校验相对顺序，避免把可选后端误设为强依赖。
    ensure_before_if_both(components, ComponentId::Redis, ComponentId::Cache)?;
    ensure_before_if_both(components, ComponentId::Cache, ComponentId::Kafka)?;
    ensure_before_if_both(components, ComponentId::Cache, ComponentId::Web)?;
    // auth:配置中心先于 auth(读最终 overlay),auth 先于 Web(Ready 发布 Authenticator 供 Web 消费)。
    ensure_before_if_both(components, ComponentId::NacosConfig, ComponentId::Auth)?;
    ensure_before_if_both(components, ComponentId::Kafka, ComponentId::Auth)?;
    ensure_before_if_both(components, ComponentId::Auth, ComponentId::Web)?;
    // auth 必须与 Web 同时声明:没有 Web 消费者的独立 AuthComponent 在当前 runtime 无意义
    // 且它据此自然进入 Service 模式(Web 是长生命周期组件)。
    if components.contains(&ComponentId::Auth) && !components.contains(&ComponentId::Web) {
        return Err(spec_error(
            "component `auth` requires `web` to be declared (auth publishes an authenticator consumed only by web)",
        ));
    }
    Ok(())
}

/// 当两个组件同时声明时校验前者必须先出现。
///
/// # 参数
///
/// - `components`：保持源码声明顺序的组件切片。
/// - `first`：有条件要求先出现的组件。
/// - `second`：与前者同时存在时必须后出现的组件。
fn ensure_before_if_both(
    components: &[ComponentId],
    first: ComponentId,
    second: ComponentId,
) -> ApplicationResult<()> {
    let first_index = components.iter().position(|component| *component == first);
    let second_index = components.iter().position(|component| *component == second);
    if let (Some(first_index), Some(second_index)) = (first_index, second_index) {
        if first_index > second_index {
            return Err(spec_error(format!(
                "invalid application component order: `{second}` appears before `{first}`"
            )));
        }
    }
    Ok(())
}

/// 创建静态应用描述的 Bootstrap 错误。
///
/// # 参数
///
/// - `message`：不包含配置值的稳定校验摘要。
fn spec_error(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(
        ComponentId::Application,
        ApplicationPhase::Bootstrap,
        message,
    )
}
