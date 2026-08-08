use std::{
    fmt,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
};

use crate::{state::StateCell, ApplicationState, RouteMeta};

/// Web 路由在只读清单中的来源类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebRouteOrigin {
    /// 由业务端点属性自动收集并通过启动预检的路由。
    Business,
    /// 由运行时在业务路由装配完成后追加的管理路由。
    Runtime,
}

impl WebRouteOrigin {
    /// 业务作用：返回适合日志、指标标签和管理端响应使用的稳定名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不会包含业务输入。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Business => "business",
            Self::Runtime => "runtime",
        }
    }
}

/// 一条已经通过 Web 启动预检的只读路由信息。
///
/// 该类型只保存诊断元数据，不持有处理函数、路由服务图或应用状态。
#[derive(Debug, Clone, Copy)]
pub struct RouteInfo {
    /// 规范化的大写请求方法。
    method: &'static str,
    /// 相对于统一上下文前缀的路由模板。
    path: &'static str,
    /// 用于定位路由来源的稳定处理器名称。
    handler: &'static str,
    /// 静态响应媒体类型。
    produces: Option<&'static str>,
    /// 静态请求媒体类型。
    consumes: Option<&'static str>,
    /// 显式请求 DTO schema 工厂。
    request_schema: Option<naweb::ApiSchemaFactory>,
    /// 显式响应 DTO schema 工厂。
    response_schema: Option<naweb::ApiSchemaFactory>,
    /// 显式 query 参数集工厂。
    query_parameters: Option<naweb::ApiParametersFactory>,
    /// 显式 header 参数集工厂。
    header_parameters: Option<naweb::ApiParametersFactory>,
    /// 主要成功 HTTP 状态码。
    success_status: u16,
    /// 额外响应集工厂。
    additional_responses: Option<naweb::ApiResponsesFactory>,
    /// 是否为流式响应。
    streaming: bool,
    /// 端点声明是否要求认证。
    auth_required: bool,
    /// 区分业务端点和运行时管理端点的来源。
    origin: WebRouteOrigin,
}

impl RouteInfo {
    /// 业务作用：从一条已通过预检的业务路由元数据创建只读信息。
    ///
    /// # 参数
    ///
    /// - `route`：属性入口生成并已完成冲突检查的静态路由元数据。
    pub(crate) const fn business(route: RouteMeta) -> Self {
        Self {
            method: route.method,
            path: route.path,
            handler: route.handler,
            produces: route.produces,
            consumes: route.consumes,
            request_schema: route.request_schema,
            response_schema: route.response_schema,
            query_parameters: route.query_parameters,
            header_parameters: route.header_parameters,
            success_status: route.success_status,
            additional_responses: route.additional_responses,
            streaming: route.streaming,
            auth_required: route.auth_required,
            origin: WebRouteOrigin::Business,
        }
    }

    /// 业务作用：创建一条由运行时拥有的管理路由信息。
    ///
    /// # 参数
    ///
    /// - `method`：规范化的大写请求方法。
    /// - `path`：相对于统一上下文前缀的静态路由模板。
    /// - `handler`：不包含业务输入的稳定处理器名称。
    pub(crate) const fn runtime(
        method: &'static str,
        path: &'static str,
        handler: &'static str,
    ) -> Self {
        Self {
            method,
            path,
            handler,
            produces: None,
            consumes: None,
            request_schema: None,
            response_schema: None,
            query_parameters: None,
            header_parameters: None,
            success_status: 200,
            additional_responses: None,
            streaming: false,
            auth_required: false,
            origin: WebRouteOrigin::Runtime,
        }
    }

    /// 业务作用：返回规范化的大写请求方法。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值与清单共同存活。
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// 业务作用：返回相对于统一上下文前缀的路由模板。
    ///
    /// # 参数
    ///
    /// 本方法无参数；完整对外路径应由 `WebHandle::context_path` 与该值组合。
    pub const fn path(&self) -> &'static str {
        self.path
    }

    /// 业务作用：返回用于诊断的稳定处理器名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该名称不能用于直接调用处理器。
    pub const fn handler(&self) -> &'static str {
        self.handler
    }

    /// 业务作用：返回静态响应媒体类型。
    pub const fn produces(&self) -> Option<&'static str> {
        self.produces
    }

    /// 业务作用：返回静态请求媒体类型。
    pub const fn consumes(&self) -> Option<&'static str> {
        self.consumes
    }

    /// 业务作用：返回显式请求 DTO schema 工厂。
    pub const fn request_schema(&self) -> Option<naweb::ApiSchemaFactory> {
        self.request_schema
    }

    /// 业务作用：返回显式响应 DTO schema 工厂。
    pub const fn response_schema(&self) -> Option<naweb::ApiSchemaFactory> {
        self.response_schema
    }

    /// 业务作用：返回显式 query 参数集工厂。
    pub const fn query_parameters(&self) -> Option<naweb::ApiParametersFactory> {
        self.query_parameters
    }

    /// 业务作用：返回显式 header 参数集工厂。
    pub const fn header_parameters(&self) -> Option<naweb::ApiParametersFactory> {
        self.header_parameters
    }

    /// 业务作用：返回主要成功 HTTP 状态码。
    pub const fn success_status(&self) -> u16 {
        self.success_status
    }

    /// 业务作用：返回额外响应集工厂。
    pub const fn additional_responses(&self) -> Option<naweb::ApiResponsesFactory> {
        self.additional_responses
    }

    /// 业务作用：返回是否为流式响应。
    pub const fn streaming(&self) -> bool {
        self.streaming
    }

    /// 业务作用：返回端点属性是否要求认证；运行时 route policy 可能进一步把公开声明收紧。
    pub const fn auth_required(&self) -> bool {
        self.auth_required
    }

    /// 业务作用：返回该路由属于业务端点还是运行时管理端点。
    ///
    /// # 参数
    ///
    /// 本方法无参数；来源不依赖路径文本推断。
    pub const fn origin(&self) -> WebRouteOrigin {
        self.origin
    }
}

/// Web 数据面相对于统一应用生命周期的只读就绪状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebReadinessState {
    /// 应用仍在启动，监听器可能尚未完成绑定。
    Starting,
    /// 全部 Ready action 已完成，可以承接正常流量。
    Ready,
    /// 应用已开始摘流和等待在途请求完成。
    Draining,
    /// 正常停机清理已经完成。
    Closed,
    /// 启动、运行或停机发生主故障且清理已经收敛。
    Failed,
}

impl WebReadinessState {
    /// 业务作用：返回适合管理端响应和指标标签使用的稳定名称。
    ///
    /// # 参数
    ///
    /// 本方法无参数；返回值不会包含故障详情。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }

    /// 业务作用：判断 Web 数据面是否处于统一应用定义的可接流状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只有 `Ready` 返回 `true`。
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

impl From<ApplicationState> for WebReadinessState {
    /// 业务作用：将统一应用状态映射为 Web 数据面的对外状态。
    ///
    /// # 参数
    ///
    /// - `state`：从共享生命周期原子单元读取的当前状态。
    fn from(state: ApplicationState) -> Self {
        match state {
            ApplicationState::Starting => Self::Starting,
            ApplicationState::Ready => Self::Ready,
            ApplicationState::Stopping => Self::Draining,
            ApplicationState::Stopped => Self::Closed,
            ApplicationState::Failed => Self::Failed,
        }
    }
}

/// 某一读取时刻的 Web 请求计数快照。
///
/// 各字段独立采用原子读取，因此高并发下相邻字段可能来自极短的不同瞬间；它适合观测，不能作为计费依据。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebMetricsSnapshot {
    /// 已进入最外层观测中间件的请求总数。
    requests_started: u64,
    /// 已正常生成响应的请求总数。
    requests_completed: u64,
    /// 当前仍在路由或响应 future 中的请求数。
    requests_in_flight: u64,
    /// 已生成的客户端错误响应总数。
    responses_client_error: u64,
    /// 已生成的服务端错误响应总数。
    responses_server_error: u64,
}

impl WebMetricsSnapshot {
    /// 业务作用：返回已经进入 Web 观测边界的请求总数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；计数包含业务路由、管理探针和未匹配请求。
    pub const fn requests_started(&self) -> u64 {
        self.requests_started
    }

    /// 业务作用：返回已经正常生成响应的请求总数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；被取消且未生成响应的请求不计入该值。
    pub const fn requests_completed(&self) -> u64 {
        self.requests_completed
    }

    /// 业务作用：返回读取快照时仍处于处理过程中的请求数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；请求 future 被取消时守卫也会递减该值。
    pub const fn requests_in_flight(&self) -> u64 {
        self.requests_in_flight
    }

    /// 业务作用：返回已经生成的 4xx 响应总数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；该值按响应状态分类，不解析响应正文。
    pub const fn responses_client_error(&self) -> u64 {
        self.responses_client_error
    }

    /// 业务作用：返回已经生成的 5xx 响应总数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；连接在形成响应前中断不会计入该值。
    pub const fn responses_server_error(&self) -> u64 {
        self.responses_server_error
    }
}

/// 应用容器对外开放的 Web 只读能力句柄。
///
/// 克隆句柄只延长运行时元数据和生命周期状态单元的存活时间，不延长监听器、路由服务图、服务任务或
/// 业务资源的生命周期。
#[derive(Clone)]
pub struct WebHandle {
    /// 不含服务对象的 Web 运行时元数据与计数器。
    runtime: Arc<WebRuntimeState>,
    /// 与 `Application::state` 共用的生命周期原子状态来源。
    application_state: Arc<StateCell>,
}

impl WebHandle {
    /// 业务作用：从容器内部共享状态创建一个只读能力句柄。
    ///
    /// # 参数
    ///
    /// - `runtime`：Web 组件独占写入、外部只读查询的运行时状态。
    /// - `application_state`：与 Application 共用的生命周期状态单元。
    pub(crate) fn new(runtime: Arc<WebRuntimeState>, application_state: Arc<StateCell>) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：返回监听器成功绑定后的真实本地地址。
    ///
    /// # 参数
    ///
    /// 本方法无参数；绑定完成前返回 `None`，配置端口为零时返回系统分配后的真实端口。
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.runtime
            .published
            .get()
            .map(|published| published.local_addr)
    }

    /// 业务作用：返回 Web 配置采用的统一上下文前缀。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Start 发布配置前暂时返回空字符串，之后固定为本次进程使用的值。
    pub fn context_path(&self) -> &str {
        self.runtime
            .context_path
            .get()
            .map(AsRef::as_ref)
            .unwrap_or("")
    }

    /// 业务作用：返回绑定成功时发布的不可变路由清单。
    ///
    /// 清单包含自动收集端点和运行时探针；通过不透明路由定制闭包追加的端点无法可靠枚举，因而不在清单中。
    ///
    /// # 参数
    ///
    /// 本方法无参数；绑定完成前返回空切片，调用方只能读取，不能增删路由。
    pub fn routes(&self) -> Arc<[RouteInfo]> {
        self.runtime
            .published
            .get()
            .map(|published| Arc::clone(&published.routes))
            .unwrap_or_else(|| Arc::from([]))
    }

    /// 业务作用：从与 Application 共用的原子状态来源读取 Web 就绪状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；不会维护第二份可能与应用状态漂移的布尔标志。
    pub fn readiness(&self) -> WebReadinessState {
        self.application_state.load().into()
    }

    /// 业务作用：返回当前 Web 请求计数的只读瞬时快照。
    ///
    /// # 参数
    ///
    /// 本方法无参数；不会交出内部原子计数器或指标注册表的所有权。
    pub fn metrics(&self) -> WebMetricsSnapshot {
        self.runtime.metrics.snapshot()
    }
}

impl fmt::Debug for WebHandle {
    /// 业务作用：写出不包含路由服务对象和业务资源的只读诊断摘要。
    ///
    /// # 参数
    ///
    /// - `formatter`：接收结构化调试字段的格式化缓冲区。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebHandle")
            .field("local_addr", &self.local_addr())
            .field("context_path", &self.context_path())
            .field("readiness", &self.readiness())
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

/// Web 组件写入、能力句柄读取的共享运行时状态。
pub(crate) struct WebRuntimeState {
    /// Start 校验成功后一次性发布的上下文前缀。
    context_path: OnceLock<Arc<str>>,
    /// 路由预检和监听绑定都成功后不可分割地发布的运行身份。
    published: OnceLock<WebPublishedState>,
    /// 最外层请求观测中间件更新的内部计数器。
    metrics: WebMetrics,
}

impl WebRuntimeState {
    /// 业务作用：创建尚未发布配置和监听信息的 Web 运行时状态。
    ///
    /// # 参数
    ///
    /// 本方法无参数；真实地址与路由清单由 Ready 阶段成组发布。
    pub(crate) fn new() -> Self {
        Self {
            context_path: OnceLock::new(),
            published: OnceLock::new(),
            metrics: WebMetrics::default(),
        }
    }

    /// 业务作用：一次性发布已经完成配置校验的上下文前缀。
    ///
    /// # 参数
    ///
    /// - `context_path`：本次 Web 服务统一使用的已校验路径前缀。
    pub(crate) fn set_context_path(&self, context_path: Arc<str>) -> Result<(), Arc<str>> {
        self.context_path.set(context_path)
    }

    /// 业务作用：一次性发布真实监听地址与已经完成预检的不可变路由清单。
    ///
    /// # 参数
    ///
    /// - `local_addr`：监听器绑定后读取的真实本地地址。
    /// - `routes`：按方法、路径和处理器稳定排序的路由信息。
    pub(crate) fn publish(&self, local_addr: SocketAddr, routes: Arc<[RouteInfo]>) -> bool {
        self.published
            .set(WebPublishedState { local_addr, routes })
            .is_ok()
    }

    /// 业务作用：判断 Web 运行身份是否已经由 Ready 阶段发布。
    ///
    /// # 参数
    ///
    /// 本方法无参数；只用于阻止同一 Application 重复提交运行时身份。
    pub(crate) fn is_published(&self) -> bool {
        self.published.get().is_some()
    }

    /// 业务作用：在请求进入最外层观测边界时创建计数守卫。
    ///
    /// # 参数
    ///
    /// - `runtime`：当前服务任务持有的共享 Web 运行时状态。
    pub(crate) fn begin_request(runtime: &Arc<Self>) -> WebRequestGuard {
        runtime
            .metrics
            .requests_started
            .fetch_add(1, Ordering::Relaxed);
        runtime
            .metrics
            .requests_in_flight
            .fetch_add(1, Ordering::Relaxed);
        WebRequestGuard {
            runtime: Arc::clone(runtime),
        }
    }
}

/// 监听绑定成功后一次性提交的 Web 运行身份。
struct WebPublishedState {
    /// 系统可能已经为零端口配置分配完成的真实监听地址。
    local_addr: SocketAddr,
    /// 与该监听器使用的路由服务图对应的不可变诊断清单。
    routes: Arc<[RouteInfo]>,
}

/// Web 请求计数器的内部共享存储。
#[derive(Default)]
struct WebMetrics {
    /// 已进入观测边界的请求总数。
    requests_started: AtomicU64,
    /// 已正常生成响应的请求总数。
    requests_completed: AtomicU64,
    /// 当前仍在处理中的请求数。
    requests_in_flight: AtomicU64,
    /// 已生成的 4xx 响应总数。
    responses_client_error: AtomicU64,
    /// 已生成的 5xx 响应总数。
    responses_server_error: AtomicU64,
}

impl WebMetrics {
    /// 业务作用：读取全部原子计数并组成一个不携带可变能力的值快照。
    ///
    /// # 参数
    ///
    /// 本方法无参数；Relaxed 顺序足以满足独立单调计数的观测用途。
    fn snapshot(&self) -> WebMetricsSnapshot {
        WebMetricsSnapshot {
            requests_started: self.requests_started.load(Ordering::Relaxed),
            requests_completed: self.requests_completed.load(Ordering::Relaxed),
            requests_in_flight: self.requests_in_flight.load(Ordering::Relaxed),
            responses_client_error: self.responses_client_error.load(Ordering::Relaxed),
            responses_server_error: self.responses_server_error.load(Ordering::Relaxed),
        }
    }
}

/// 保证请求完成或 future 被取消时都能递减在途计数的内部守卫。
pub(crate) struct WebRequestGuard {
    /// 只包含 Web 运行态计数器的共享引用，不持有 Application 或路由服务图。
    runtime: Arc<WebRuntimeState>,
}

impl WebRequestGuard {
    /// 业务作用：记录一次已经形成响应的请求结果。
    ///
    /// # 参数
    ///
    /// - `status`：响应的三位 HTTP 状态码，用于归入客户端错误或服务端错误计数。
    pub(crate) fn complete(self, status: u16) {
        self.runtime
            .metrics
            .requests_completed
            .fetch_add(1, Ordering::Relaxed);
        if (400..500).contains(&status) {
            self.runtime
                .metrics
                .responses_client_error
                .fetch_add(1, Ordering::Relaxed);
        } else if status >= 500 {
            self.runtime
                .metrics
                .responses_server_error
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for WebRequestGuard {
    /// 业务作用：在正常响应和取消路径上统一结束一次在途计数。
    ///
    /// # 参数
    ///
    /// 本方法无参数；原子递减不访问 Application 生命周期或服务任务。
    fn drop(&mut self) {
        self.runtime
            .metrics
            .requests_in_flight
            .fetch_sub(1, Ordering::Relaxed);
    }
}
