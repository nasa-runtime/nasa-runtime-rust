//! MVC 路由、interceptor 与 Web 安全流水线运行时。
//!
//! 该 crate 重导出 `#[*_mapping]`、`#[interceptor]` 与 `mvc_router!`，并提供 effective-plan
//! 编排、启动/热更新审计、请求固定快照、身份 gate、双协议密码处理、replay、低基数指标和
//! `MappingRuntime`。业务 Token、Header、会话 DTO 与 Redis Key 不属于本 crate。
// ============================================================================
// naweb —— mapping 宏的公共 Web 运行时与安全编排门面。
// 除重导出注解宏和第三方依赖桥接外，还持有不可变路由合同、interceptor plan 与可热更新安全快照。
// `crate::__mvc` 仍由 mvc_router! 生成在【业务 crate 内】(State 单态化 + linkme
// 链接期收集都要求如此)；业务根 Router State 仍归应用容器所有。
// ============================================================================

pub use naweb_macro::{
    delete_mapping, get_mapping, interceptor, mvc_router, patch_mapping, post_mapping, put_mapping,
};

mod interceptor;
mod policy;
mod runtime;

#[cfg(any(feature = "auth", feature = "crypto"))]
mod metrics;

pub use interceptor::*;
pub use policy::*;
pub use runtime::*;

#[cfg(any(feature = "auth", feature = "crypto"))]
pub use metrics::*;

#[cfg(feature = "auth")]
pub mod auth;

#[cfg(feature = "crypto")]
pub mod crypto;

#[cfg(any(feature = "auth", feature = "crypto"))]
mod endpoint;

#[cfg(any(feature = "auth", feature = "crypto"))]
pub use endpoint::{endpoint_middleware, EndpointLayerState};

/// 响应扩展标记:本响应体不可再被下游压缩。
///
/// 加密响应(尤其 legacy-v1 密文 content-type 为 `application/json`,与明文无法凭类型区分)插入本标记,
/// 供外层压缩中间件在压缩前检测并跳过,规避压缩+加密同用的 CRIME/BREACH 侧信道。是**服务端内部**
/// 响应扩展,不出现在响应头/线缆上。即便本 crate 未启用 auth/crypto 也无条件定义,使编排层(napp)的
/// 压缩谓词可以稳定引用本类型,不受 Web 安全 feature 组合影响。
#[derive(Clone, Copy, Debug)]
pub struct UncompressibleResponse;

/// 业务端点和路由定制使用的稳定 Web 类型入口。
///
/// 集中重导出可以让业务控制器不依赖底层组件的内部模块布局。
pub mod types {
    pub use axum::extract::{Extension, OriginalUri, Path, Query, Request, State};
    pub use axum::http::{HeaderMap, StatusCode};
    // `from_fn` 中间件的处理函数签名需要 Request/Next；缺了它们业务就只能绕开门面直连 axum。
    pub use axum::middleware::{from_fn, from_fn_with_state, Next};
    pub use axum::response::{IntoResponse, Response};
    pub use axum::routing::{delete, get, patch, post, put};
    pub use axum::{Form, Json, Router};
}

// 门面模块本身已经叫 `web`，高频 Axum 类型提升到 crate 根；需要分类导入时可用
// `naweb::types::*` / `nasa::web::types::*`，不会形成 `web::web` 重复路径。
pub use types::{
    delete, from_fn, from_fn_with_state, get, patch, post, put, Extension, Form, HeaderMap,
    IntoResponse, Json, Next, OriginalUri, Path, Query, Request, Response, Router, State,
    StatusCode,
};

/// 显式 OpenAPI DTO schema 的静态描述。
///
/// Web 路由层只保存名称和 JSON Schema 文本，不解析 JSON，也不依赖 OpenAPI 生成器。只有应用启用
/// OpenAPI 能力并请求文档时，`napp`/`naopenapi` 才会解析、校验和登记该 schema。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiSchemaDescriptor {
    /// `components.schemas` 下的稳定名称。
    pub name: &'static str,
    /// 一份完整 JSON Schema；必须是 JSON object 或 JSON boolean。
    pub json_schema: &'static str,
}

/// 路由 DTO 的显式 schema 合同。
///
/// 框架刻意不从 handler 参数或 serde 行为猜测 schema。业务 DTO 需要实现本 trait，并在
/// `#[*_mapping(request_schema = Type, response_schema = Type)]` 上显式引用。
pub trait ApiSchema {
    /// `components.schemas` 下的稳定名称。
    const NAME: &'static str;
    /// 完整 JSON Schema 文本。
    const JSON_SCHEMA: &'static str;
}

/// 业务作用：把一个 DTO 类型投影成可放入静态路由表的描述符。
///
/// # 参数
///
/// 本函数没有运行时参数；类型参数必须实现 [`ApiSchema`]。
pub fn api_schema<T: ApiSchema>() -> ApiSchemaDescriptor {
    ApiSchemaDescriptor {
        name: T::NAME,
        json_schema: T::JSON_SCHEMA,
    }
}

/// 静态路由表保存的无捕获 schema 工厂。
pub type ApiSchemaFactory = fn() -> ApiSchemaDescriptor;

/// query/header 参数的静态 OpenAPI 描述。
#[derive(Debug, Clone, Copy)]
pub struct ApiParameterDescriptor {
    /// 参数名；query 名区分大小写，header 名由 OpenAPI 按不区分大小写解释。
    pub name: &'static str,
    /// 是否必填。
    pub required: bool,
    /// 参数值 schema 工厂。
    pub schema: ApiSchemaFactory,
}

/// 一组显式参数合同。
///
/// 框架不从 handler extractor 猜测字段；业务为 query/header DTO 各实现一个参数集，并在路由属性中
/// 通过 `query_parameters = Type` 或 `header_parameters = Type` 引用。
pub trait ApiParameters {
    /// 参数按业务声明顺序提供；生成器会校验重名并按名称稳定排序。
    const PARAMETERS: &'static [ApiParameterDescriptor];
}

/// 业务作用：把参数集类型投影成可放入静态路由表的工厂返回值。
pub fn api_parameters<T: ApiParameters>() -> &'static [ApiParameterDescriptor] {
    T::PARAMETERS
}

/// 静态路由表保存的无捕获参数集工厂。
pub type ApiParametersFactory = fn() -> &'static [ApiParameterDescriptor];

/// 一条额外 HTTP 响应的静态 OpenAPI 描述。
#[derive(Debug, Clone, Copy)]
pub struct ApiResponseDescriptor {
    /// HTTP 状态码。
    pub status: u16,
    /// 非空稳定描述。
    pub description: &'static str,
    /// 响应媒体类型；无响应体时为 `None`。
    pub produces: Option<&'static str>,
    /// 响应 DTO schema；无响应体时为 `None`。
    pub schema: Option<ApiSchemaFactory>,
}

/// 一组成功响应之外的显式响应合同。
pub trait ApiResponses {
    /// 额外响应集合；不能与路由的 `success_status` 或彼此重复。
    const RESPONSES: &'static [ApiResponseDescriptor];
}

/// 业务作用：把响应集类型投影成静态路由表可保存的描述符切片。
pub fn api_responses<T: ApiResponses>() -> &'static [ApiResponseDescriptor] {
    T::RESPONSES
}

/// 静态路由表保存的无捕获响应集工厂。
pub type ApiResponsesFactory = fn() -> &'static [ApiResponseDescriptor];

/// 宏展开专用的第三方依赖桥。**不属于稳定业务 API**。
#[doc(hidden)]
pub mod __private {
    pub use axum;
    pub use linkme;
    pub use tracing;
}
