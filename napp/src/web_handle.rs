use std::{
    fmt,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
};

use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};

use crate::{state::StateCell, ApplicationState, RouteMeta};

static WEB_REQUESTS_BY_PROTOCOL_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "napp_web_requests_by_protocol_total",
    help: "Web listener 按 HTTP 协议统计的入站请求累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["protocol"],
    histogram_bounds: &[],
};
static WEB_HTTP2_REQUESTS_IN_FLIGHT: MetricDescriptor = MetricDescriptor {
    name: "napp_web_http2_requests_in_flight",
    help: "Web listener 当前仍在路由或响应 future 中的 HTTP/2 请求数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static WEB_CONNECTIONS_ACCEPTED_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "napp_web_connections_accepted_total",
    help: "Web listener 已接受的 TCP 连接累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &[],
    histogram_bounds: &[],
};
static WEB_CONNECTIONS_REJECTED_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "napp_web_connections_rejected_total",
    help: "Web listener 在协议解析前按原因拒绝的 TCP 连接累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["reason"],
    histogram_bounds: &[],
};
static WEB_CONNECTIONS_ACTIVE: MetricDescriptor = MetricDescriptor {
    name: "napp_web_connections_active",
    help: "Web listener 当前管理的 TCP 连接数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static WEB_ACCEPT_ERRORS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "napp_web_accept_errors_total",
    help: "Web listener 可恢复 accept 错误的累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &[],
    histogram_bounds: &[],
};
static WEB_CONNECTION_ERRORS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "napp_web_connection_errors_total",
    help: "Web listener 协议判定或 HTTP driver 连接级错误的累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &[],
    histogram_bounds: &[],
};
static WEB_METRIC_DESCRIPTORS: [&MetricDescriptor; 7] = [
    &WEB_REQUESTS_BY_PROTOCOL_TOTAL,
    &WEB_HTTP2_REQUESTS_IN_FLIGHT,
    &WEB_CONNECTIONS_ACCEPTED_TOTAL,
    &WEB_CONNECTIONS_REJECTED_TOTAL,
    &WEB_CONNECTIONS_ACTIVE,
    &WEB_ACCEPT_ERRORS_TOTAL,
    &WEB_CONNECTION_ERRORS_TOTAL,
];

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
    /// 参数说明: 无。
    ///
    /// 返回：业务路由为 business，运行时管理路由为 runtime，名称不含业务输入。
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
    /// 参数说明：
    ///
    /// - `route`：属性入口生成并已完成冲突检查的静态路由元数据。
    ///
    /// 返回：复制静态声明且来源为 Business 的只读信息；不执行 schema 工厂或重复校验路由冲突。
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
    /// 参数说明：
    ///
    /// - `method`：规范化的大写请求方法。
    /// - `path`：相对于统一上下文前缀的静态路由模板。
    /// - `handler`：不包含业务输入的稳定处理器名称。
    ///
    /// 返回：来源为 Runtime、成功状态为 200 且未填媒体类型或 schema 工厂的只读声明；认证标记默认为 false，不替代管理端实际访问控制。
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
    /// 参数说明: 无。
    ///
    /// 返回：预检时保存的静态方法名，不从具体入站请求推断。
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// 业务作用：返回相对于统一上下文前缀的路由模板。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：静态路由模板；完整对外路径还需结合 WebHandle::context_path，不包含当前请求的实际参数值。
    pub const fn path(&self) -> &'static str {
        self.path
    }

    /// 业务作用：返回用于诊断的稳定处理器名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：预检时保存的处理器名称，仅用于诊断关联，不提供函数调用能力。
    pub const fn handler(&self) -> &'static str {
        self.handler
    }

    /// 业务作用：返回静态响应媒体类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端点声明的响应媒体类型，未声明则为 None；不保证处理器实际返回相同 Content-Type。
    pub const fn produces(&self) -> Option<&'static str> {
        self.produces
    }

    /// 业务作用：返回静态请求媒体类型。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端点声明的请求媒体类型，未声明则为 None；本访问器不验证请求头或正文格式。
    pub const fn consumes(&self) -> Option<&'static str> {
        self.consumes
    }

    /// 业务作用：返回显式请求 DTO schema 工厂。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：声明的请求结构工厂，未声明则为 None；不执行工厂，不验证当前请求。
    pub const fn request_schema(&self) -> Option<naweb::ApiSchemaFactory> {
        self.request_schema
    }

    /// 业务作用：返回显式响应 DTO schema 工厂。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：声明的响应结构工厂，未声明则为 None；只提供文档元数据，不执行响应校验。
    pub const fn response_schema(&self) -> Option<naweb::ApiSchemaFactory> {
        self.response_schema
    }

    /// 业务作用：返回显式 query 参数集工厂。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：声明的查询参数描述工厂，未声明则为 None；不会解析实际查询串。
    pub const fn query_parameters(&self) -> Option<naweb::ApiParametersFactory> {
        self.query_parameters
    }

    /// 业务作用：返回显式 header 参数集工厂。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：声明的请求头描述工厂，未声明则为 None；不会读取或验证实际头部。
    pub const fn header_parameters(&self) -> Option<naweb::ApiParametersFactory> {
        self.header_parameters
    }

    /// 业务作用：返回主要成功 HTTP 状态码。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：用于接口描述的成功状态码，运行时管理路由缺省为 200；不改写处理器的实际响应。
    pub const fn success_status(&self) -> u16 {
        self.success_status
    }

    /// 业务作用：返回额外响应集工厂。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端点额外响应描述的工厂，未声明则为 None；不调用工厂或生成业务响应。
    pub const fn additional_responses(&self) -> Option<naweb::ApiResponsesFactory> {
        self.additional_responses
    }

    /// 业务作用：返回是否为流式响应。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端点的静态流式声明，运行时管理路由缺省为 false；不观察实际响应体传输进度。
    pub const fn streaming(&self) -> bool {
        self.streaming
    }

    /// 业务作用：返回端点属性是否要求认证；运行时 route policy 可能进一步把公开声明收紧。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：端点声明的认证标记；false 只表示没有静态要求，不能用它绕过运行时访问控制。
    pub const fn auth_required(&self) -> bool {
        self.auth_required
    }

    /// 业务作用：返回该路由属于业务端点还是运行时管理端点。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造时固定的业务或运行时来源，不按路径推断，也不表示当前调用者具有管理权限。
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
    /// 参数说明: 无。
    ///
    /// 返回：对应状态的固定小写名称，仅表达生命周期分类，不附带故障详情。
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
    /// 参数说明: 无。
    ///
    /// 返回：仅 Ready 为 true；这是已读取状态的判断，不保证返回后的请求仍获准进入。
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

impl From<ApplicationState> for WebReadinessState {
    /// 业务作用：将统一应用状态映射为 Web 数据面的对外状态。
    ///
    /// 参数说明：
    ///
    /// - `state`：从共享生命周期原子单元读取的当前状态。
    ///
    /// 返回：Stopping 映射为 Draining、Stopped 映射为 Closed，其余保持相应生命周期含义；不主动探测监听或依赖健康。
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
    /// 参数说明: 无。
    ///
    /// 返回：快照保存的入站累计数，包含进入观测边界的业务路由、管理探针及未匹配请求，不实时刷新。
    pub const fn requests_started(&self) -> u64 {
        self.requests_started
    }

    /// 业务作用：返回已经正常生成响应的请求总数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：快照保存的已生成响应累计数，未生成响应就取消的不计入；不等于响应体已经发送完毕。
    pub const fn requests_completed(&self) -> u64 {
        self.requests_completed
    }

    /// 业务作用：返回读取快照时仍处于处理过程中的请求数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：快照读取时尚持有请求观测守卫的数量；生成响应或取消导致守卫释放后递减，不统计后续响应体传输。
    pub const fn requests_in_flight(&self) -> u64 {
        self.requests_in_flight
    }

    /// 业务作用：返回已经生成的 4xx 响应总数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：快照保存的 4xx 响应数量，仅按状态码分类，不读取响应正文或推断具体拒绝原因。
    pub const fn responses_client_error(&self) -> u64 {
        self.responses_client_error
    }

    /// 业务作用：返回已经生成的 5xx 响应总数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：快照保存的 5xx 响应数量，形成响应前的中断不计入；不能据此统计所有连接故障。
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
    /// 参数说明：
    ///
    /// - `runtime`：Web 组件独占写入、外部只读查询的运行时状态。
    /// - `application_state`：与 Application 共用的生命周期状态单元。
    ///
    /// 返回：仅持有元数据、计数器与生命周期状态的只读句柄；不取得监听器或路由服务图的所有权。
    pub(crate) fn new(runtime: Arc<WebRuntimeState>, application_state: Arc<StateCell>) -> Self {
        Self {
            runtime,
            application_state,
        }
    }

    /// 业务作用：返回监听器成功绑定后的真实本地地址。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功发布的历史绑定地址，端口零已替换为系统分配值；发布前为 None，停机后仍可保留该值，不证明端口在线。
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.runtime
            .published
            .get()
            .map(|published| published.local_addr)
    }

    /// 业务作用：返回 Web 配置采用的统一上下文前缀。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：Start 阶段一次发布的上下文前缀，发布前为空字符串；读取不触发配置刷新或判断监听状态。
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
    /// 参数说明: 无。
    ///
    /// 返回：发布后共享不可变路由元数据，发布前为空集合；停机后仍可读取，不能据此修改路由或证明其可访问。
    pub fn routes(&self) -> Arc<[RouteInfo]> {
        self.runtime
            .published
            .get()
            .map(|published| Arc::clone(&published.routes))
            .unwrap_or_else(|| Arc::from([]))
    }

    /// 业务作用：从与 Application 共用的原子状态来源读取 Web 就绪状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前应用状态对应的 Web 生命周期状态；不额外探测网络或汇总依赖健康，不形成持续准入许可。
    pub fn readiness(&self) -> WebReadinessState {
        self.application_state.load().into()
    }

    /// 业务作用：返回当前 Web 请求计数的只读瞬时快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：独立读取各原子计数得到的快照，字段间不保证同一时刻；调用方不能借此更新计数或注册指标。
    pub fn metrics(&self) -> WebMetricsSnapshot {
        self.runtime.metrics.snapshot()
    }
}

impl fmt::Debug for WebHandle {
    /// 业务作用：写出不包含路由服务对象和业务资源的只读诊断摘要。
    ///
    /// 参数说明：
    ///
    /// - `formatter`：接收结构化调试字段的格式化缓冲区。
    ///
    /// 返回：写出历史地址、上下文前缀、当前生命周期和计数后完成；输出失败传播格式化错误，各字段不构成原子快照。
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
    /// 参数说明: 无。
    ///
    /// 返回：前缀及绑定信息均未发布、计数全为零的状态；真实地址和路由清单等待 Ready 阶段成组发布。
    pub(crate) fn new() -> Self {
        Self {
            context_path: OnceLock::new(),
            published: OnceLock::new(),
            metrics: WebMetrics::default(),
        }
    }

    /// 业务作用：一次性发布已经完成配置校验的上下文前缀。
    ///
    /// 参数说明：
    ///
    /// - `context_path`：本次 Web 服务统一使用的已校验路径前缀。
    ///
    /// 返回：首次设置成功时完成；已发布时原值不变并交还传入前缀，不执行运行期替换。
    pub(crate) fn set_context_path(&self, context_path: Arc<str>) -> Result<(), Arc<str>> {
        self.context_path.set(context_path)
    }

    /// 业务作用：一次性发布真实监听地址与已经完成预检的不可变路由清单。
    ///
    /// 参数说明：
    ///
    /// - `local_addr`：监听器绑定后读取的真实本地地址。
    /// - `routes`：按方法、路径和处理器稳定排序的路由信息。
    ///
    /// 返回：首次成组发布成功时为 true；已有身份时为 false 且保留原值，不开放准入或复验监听状态。
    pub(crate) fn publish(&self, local_addr: SocketAddr, routes: Arc<[RouteInfo]>) -> bool {
        // 地址与路由清单共享单次发布边界，读者不能看到新地址配旧清单的部分身份。
        self.published
            .set(WebPublishedState {
                local_addr,
                routes,
            })
            .is_ok()
    }

    /// 业务作用：判断 Web 运行身份是否已经由 Ready 阶段发布。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：曾完成绑定身份发布时为 true，用于阻止重复提交；停机不会将其清零，不代表当前监听可用。
    pub(crate) fn is_published(&self) -> bool {
        self.published.get().is_some()
    }

    /// 业务作用：在请求进入最外层观测边界时创建计数守卫。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：当前服务任务持有的共享 Web 运行时状态。
    /// - `http2`：请求是否由 HTTP/2 stream 承载，用于固定协议维度和在途请求会计。
    ///
    /// 返回：在请求完成或 future 取消时自动结束在途会计的守卫。
    pub(crate) fn begin_request(runtime: &Arc<Self>, http2: bool) -> WebRequestGuard {
        runtime
            .metrics
            .requests_started
            .fetch_add(1, Ordering::Relaxed);
        runtime
            .metrics
            .requests_in_flight
            .fetch_add(1, Ordering::Relaxed);
        if http2 {
            runtime
                .metrics
                .requests_http2
                .fetch_add(1, Ordering::Relaxed);
            runtime
                .metrics
                .http2_requests_in_flight
                .fetch_add(1, Ordering::Relaxed);
        } else {
            runtime
                .metrics
                .requests_http1
                .fetch_add(1, Ordering::Relaxed);
        }
        WebRequestGuard {
            runtime: Arc::clone(runtime),
            http2,
        }
    }

    /// 业务作用：记录一条已经由操作系统 accept 的 Web TCP 连接。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：listener 与能力句柄共用的 Web 运行时状态。
    ///
    /// 返回：无；只累加连接接纳事实。
    pub(crate) fn accept_connection(runtime: &Arc<Self>) {
        runtime
            .metrics
            .connections_accepted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录因 listener 容量门禁而在协议解析前关闭的连接。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：listener 与能力句柄共用的 Web 运行时状态。
    ///
    /// 返回：无；只累加容量拒绝事实。
    pub(crate) fn reject_connection(runtime: &Arc<Self>) {
        runtime
            .metrics
            .connections_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录一次可恢复的 TCP accept 错误。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：listener 与能力句柄共用的 Web 运行时状态。
    ///
    /// 返回：无；只累加 accept 错误事实。
    pub(crate) fn record_accept_error(runtime: &Arc<Self>) {
        runtime
            .metrics
            .accept_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录一条在协议判定或 HTTP driver 阶段失败的连接。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：listener 与能力句柄共用的 Web 运行时状态。
    ///
    /// 返回：无；只累加连接级错误事实。
    pub(crate) fn record_connection_error(runtime: &Arc<Self>) {
        runtime
            .metrics
            .connection_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：创建覆盖协议判定、请求处理和排空阶段的活跃连接守卫。
    ///
    /// 参数说明：
    ///
    /// - `runtime`：listener 与能力句柄共用的 Web 运行时状态。
    ///
    /// 返回：析构时自动递减活跃连接数的所有权守卫。
    pub(crate) fn track_connection(runtime: &Arc<Self>) -> WebConnectionGuard {
        runtime
            .metrics
            .connections_active
            .fetch_add(1, Ordering::Relaxed);
        WebConnectionGuard {
            runtime: Arc::clone(runtime),
        }
    }
}

impl LegacyMetricsSource for WebRuntimeState {
    /// 业务作用：返回 Web listener 在启动期登记到统一指标目录的静态指标族。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：连接容量、连接结果和协议请求维度共用的稳定 descriptor 集合。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &WEB_METRIC_DESCRIPTORS
    }

    /// 业务作用：把 Web 原子计数转换为 Prometheus 与 OTLP 共用的结构化快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终返回固定低基数的八条样本，不包含地址、路由或客户端输入。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let metrics = self.metrics.transport_snapshot();
        Some(vec![
            MetricSample {
                name: WEB_REQUESTS_BY_PROTOCOL_TOTAL.name,
                labels: vec![("protocol", "http1".to_owned())],
                value: MetricValue::Counter(metrics.requests_http1),
            },
            MetricSample {
                name: WEB_REQUESTS_BY_PROTOCOL_TOTAL.name,
                labels: vec![("protocol", "http2".to_owned())],
                value: MetricValue::Counter(metrics.requests_http2),
            },
            MetricSample {
                name: WEB_HTTP2_REQUESTS_IN_FLIGHT.name,
                labels: Vec::new(),
                value: MetricValue::Gauge(metrics.http2_requests_in_flight as f64),
            },
            MetricSample {
                name: WEB_CONNECTIONS_ACCEPTED_TOTAL.name,
                labels: Vec::new(),
                value: MetricValue::Counter(metrics.connections_accepted),
            },
            MetricSample {
                name: WEB_CONNECTIONS_REJECTED_TOTAL.name,
                labels: vec![("reason", "capacity".to_owned())],
                value: MetricValue::Counter(metrics.connections_rejected),
            },
            MetricSample {
                name: WEB_CONNECTIONS_ACTIVE.name,
                labels: Vec::new(),
                value: MetricValue::Gauge(metrics.connections_active as f64),
            },
            MetricSample {
                name: WEB_ACCEPT_ERRORS_TOTAL.name,
                labels: Vec::new(),
                value: MetricValue::Counter(metrics.accept_errors),
            },
            MetricSample {
                name: WEB_CONNECTION_ERRORS_TOTAL.name,
                labels: Vec::new(),
                value: MetricValue::Counter(metrics.connection_errors),
            },
        ])
    }

    /// 业务作用：为只消费旧文本入口的调用方追加与结构化快照同源的 Prometheus 文本。
    ///
    /// 参数说明：
    /// - `output`：接收 Web 指标文本的目标缓冲区。
    ///
    /// 返回：无；在现有内容尾部追加固定 family 与低基数样本。
    fn render_prometheus(&self, output: &mut String) {
        use std::fmt::Write as _;

        let metrics = self.metrics.transport_snapshot();
        let _ = writeln!(
            output,
            "# HELP {} {}",
            WEB_REQUESTS_BY_PROTOCOL_TOTAL.name, WEB_REQUESTS_BY_PROTOCOL_TOTAL.help
        );
        let _ = writeln!(
            output,
            "# TYPE {} counter",
            WEB_REQUESTS_BY_PROTOCOL_TOTAL.name
        );
        let _ = writeln!(
            output,
            "{}{{protocol=\"http1\"}} {}",
            WEB_REQUESTS_BY_PROTOCOL_TOTAL.name, metrics.requests_http1
        );
        let _ = writeln!(
            output,
            "{}{{protocol=\"http2\"}} {}",
            WEB_REQUESTS_BY_PROTOCOL_TOTAL.name, metrics.requests_http2
        );
        for (descriptor, metric_type, value) in [
            (
                &WEB_HTTP2_REQUESTS_IN_FLIGHT,
                "gauge",
                metrics.http2_requests_in_flight,
            ),
            (
                &WEB_CONNECTIONS_ACCEPTED_TOTAL,
                "counter",
                metrics.connections_accepted,
            ),
            (&WEB_CONNECTIONS_ACTIVE, "gauge", metrics.connections_active),
            (&WEB_ACCEPT_ERRORS_TOTAL, "counter", metrics.accept_errors),
            (
                &WEB_CONNECTION_ERRORS_TOTAL,
                "counter",
                metrics.connection_errors,
            ),
        ] {
            let _ = writeln!(output, "# HELP {} {}", descriptor.name, descriptor.help);
            let _ = writeln!(output, "# TYPE {} {metric_type}", descriptor.name);
            let _ = writeln!(output, "{} {value}", descriptor.name);
        }
        let _ = writeln!(
            output,
            "# HELP {} {}",
            WEB_CONNECTIONS_REJECTED_TOTAL.name, WEB_CONNECTIONS_REJECTED_TOTAL.help
        );
        let _ = writeln!(
            output,
            "# TYPE {} counter",
            WEB_CONNECTIONS_REJECTED_TOTAL.name
        );
        let _ = writeln!(
            output,
            "{}{{reason=\"capacity\"}} {}",
            WEB_CONNECTIONS_REJECTED_TOTAL.name, metrics.connections_rejected
        );
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
    /// 经 HTTP/1.0 或 HTTP/1.1 进入路由边界的请求总数。
    requests_http1: AtomicU64,
    /// 经 HTTP/2 进入路由边界的请求总数。
    requests_http2: AtomicU64,
    /// 当前仍在路由或响应 future 中的 HTTP/2 请求数。
    http2_requests_in_flight: AtomicU64,
    /// listener 已接纳的 TCP 连接总数。
    connections_accepted: AtomicU64,
    /// 因连接容量耗尽而拒绝的 TCP 连接总数。
    connections_rejected: AtomicU64,
    /// 当前仍由 listener 管理的 TCP 连接数。
    connections_active: AtomicU64,
    /// TCP accept 调用返回错误的累计次数。
    accept_errors: AtomicU64,
    /// 协议判定或 HTTP driver 返回错误的连接总数。
    connection_errors: AtomicU64,
}

/// Web transport 指标源一次读取使用的内部值快照。
struct WebTransportMetricsSnapshot {
    /// 进入观测边界的 HTTP/1 请求累计数。
    requests_http1: u64,
    /// 进入观测边界的 HTTP/2 请求累计数。
    requests_http2: u64,
    /// 读取时仍持有 HTTP/2 请求守卫的数量。
    http2_requests_in_flight: u64,
    /// 操作系统 accept 成功的连接累计数。
    connections_accepted: u64,
    /// 按容量门禁拒绝的连接累计数。
    connections_rejected: u64,
    /// 读取时仍持有连接守卫的数量。
    connections_active: u64,
    /// 可恢复的 accept 调用失败累计数。
    accept_errors: u64,
    /// 协议判定或 HTTP 驱动报告的连接失败累计数。
    connection_errors: u64,
}

impl WebMetrics {
    /// 业务作用：读取全部原子计数并组成一个不携带可变能力的值快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：独立读取各请求计数形成的值快照；Relaxed 适用于观测，不保证跨字段一致性，在途数也不具有单调性。
    fn snapshot(&self) -> WebMetricsSnapshot {
        WebMetricsSnapshot {
            requests_started: self.requests_started.load(Ordering::Relaxed),
            requests_completed: self.requests_completed.load(Ordering::Relaxed),
            requests_in_flight: self.requests_in_flight.load(Ordering::Relaxed),
            responses_client_error: self.responses_client_error.load(Ordering::Relaxed),
            responses_server_error: self.responses_server_error.load(Ordering::Relaxed),
        }
    }

    /// 业务作用：读取协议、连接与 HTTP/2 在途请求原子计数供统一指标源导出。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含固定低基数 transport 数值的内部快照。
    fn transport_snapshot(&self) -> WebTransportMetricsSnapshot {
        WebTransportMetricsSnapshot {
            requests_http1: self.requests_http1.load(Ordering::Relaxed),
            requests_http2: self.requests_http2.load(Ordering::Relaxed),
            http2_requests_in_flight: self.http2_requests_in_flight.load(Ordering::Relaxed),
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            accept_errors: self.accept_errors.load(Ordering::Relaxed),
            connection_errors: self.connection_errors.load(Ordering::Relaxed),
        }
    }
}

/// 保证连接任务正常结束、协议失败或被排空预算取消时都能递减活跃连接数。
pub(crate) struct WebConnectionGuard {
    /// 只包含 Web 运行态计数器的共享引用。
    runtime: Arc<WebRuntimeState>,
}

impl Drop for WebConnectionGuard {
    /// 业务作用：在连接所有权结束时统一递减活跃连接计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；守卫释放时只递减活跃连接计数。
    fn drop(&mut self) {
        self.runtime
            .metrics
            .connections_active
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// 保证请求完成或 future 被取消时都能递减在途计数的内部守卫。
pub(crate) struct WebRequestGuard {
    /// 只包含 Web 运行态计数器的共享引用，不持有 Application 或路由服务图。
    runtime: Arc<WebRuntimeState>,
    /// 当前请求是否经 HTTP/2 进入路由边界。
    http2: bool,
}

impl WebRequestGuard {
    /// 业务作用：记录一次已经形成响应的请求结果。
    ///
    /// 参数说明：
    ///
    /// - `status`：响应的三位 HTTP 状态码，用于归入客户端错误或服务端错误计数。
    ///
    /// 返回：累计已生成响应数，400 至 499 计客户端错误、500 及以上计服务端错误；消费守卫后自动减少在途数，不等待响应体发送。
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
    /// 参数说明: 无。
    ///
    /// 返回：减少通用在途计数，HTTP/2 请求额外减少其协议在途数；取消路径不增加完成数，不访问应用生命周期。
    fn drop(&mut self) {
        self.runtime
            .metrics
            .requests_in_flight
            .fetch_sub(1, Ordering::Relaxed);
        if self.http2 {
            self.runtime
                .metrics
                .http2_requests_in_flight
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}
