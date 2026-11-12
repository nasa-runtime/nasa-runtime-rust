use std::{collections::HashMap, sync::Arc, time::Duration};
use tower::layer::util::Stack;
use tracing::{Instrument, instrument};

use crate::api::error::Error;
use crate::api::plugin::{AuthPlugin, NoopAuthPlugin, init_auth_plugin};
use crate::common::remote::grpc::message::{
    GrpcMessage, GrpcMessageBuilder, GrpcRequestMessage, GrpcResponseMessage,
};
use crate::common::remote::grpc::message::{GrpcMessageData, request::NacosClientAbilities};
use crate::common::remote::grpc::nacos_grpc_service::DynamicUnaryCallLayerWrapper;
use crate::common::remote::server_list::{PollingServerListService, ServerListProvider};

use super::handlers::client_detection_request_handler::ClientDetectionRequestHandler;
use super::message::request::{ClientDetectionRequest, HealthCheckRequest};
use super::nacos_grpc_connection::{NacosGrpcConnection, SendRequest};
use super::nacos_grpc_service::{
    DynamicBiStreamingCallLayer, DynamicBiStreamingCallLayerWrapper, DynamicUnaryCallLayer,
};
use super::tonic::TonicBuilder;
use super::{config::GrpcConfiguration, nacos_grpc_service::ServerRequestHandler};

const APP_FIELD: &str = "app";

pub(crate) struct NacosGrpcClient {
    /// 后续请求默认使用的应用头值。
    app_name: String,
    /// 负责背压、连接状态机及健康检查的共享发送入口。
    send_request: Arc<dyn SendRequest + Send + Sync + 'static>,
    /// 按请求资源生成身份材料的插件。
    auth_plugin: Arc<dyn AuthPlugin>,
}

impl NacosGrpcClient {
    /// 业务作用：为请求补充资源身份和应用头，经共享连接发送并按预期响应类型解码。
    ///
    /// 参数说明：
    /// - `request`：待发送请求，其头部被移交；资源认证字段覆盖同名请求头，请求头可覆盖默认 app 头。
    ///
    /// 返回：传输与解码成功后的响应正文；编码、连接或响应合同失败返回错误。目标类型可解码不代表其业务结果码成功，调用方仍须判断。
    #[instrument(skip_all)]
    pub(crate) async fn send_request<Request, Response>(
        &self,
        mut request: Request,
    ) -> Result<Response, Error>
    where
        Request: GrpcRequestMessage + 'static,
        Response: GrpcResponseMessage + 'static,
    {
        let mut request_headers = request.take_headers();
        // 身份必须按当前请求资源获取，不能让调用方同名头覆盖认证插件提供的材料。
        if let Some(resource) = request.request_resource() {
            let auth_context = self.auth_plugin.get_login_identity(resource);
            request_headers.extend(auth_context.contexts);
        }

        let grpc_request = GrpcMessageBuilder::new(request)
            .header(APP_FIELD.to_owned(), self.app_name.clone())
            .headers(request_headers)
            .build();
        let grpc_request = grpc_request.into_payload()?;

        let grpc_response = self
            .send_request
            .send_request(grpc_request)
            .in_current_span()
            .await?;

        let grpc_response = GrpcMessage::<Response>::from_payload(grpc_response)?;
        Ok(grpc_response.into_body())
    }
}

type HandlerMap = HashMap<String, Arc<dyn ServerRequestHandler>>;
type ConnectedListener = Arc<dyn Fn(String) + Send + Sync + 'static>;
type DisconnectedListener = Arc<dyn Fn(String) + Send + Sync + 'static>;

pub(crate) struct NacosGrpcClientBuilder {
    /// 请求的默认 app 头值。
    app_name: String,
    /// 连接握手声明的客户端实现版本。
    client_version: String,
    /// 握手 tenant 字段使用的命名空间。
    namespace: String,
    /// 连接初始化时发送的客户端标签。
    labels: HashMap<String, String>,
    /// 握手向服务端声明的能力集合。
    client_abilities: NacosClientAbilities,
    /// 端点建立、流控和保活配置。
    grpc_config: GrpcConfiguration,
    /// 按消息协议身份索引的推送处理器。
    server_request_handler_map: HandlerMap,
    /// 每次连接尝试读取的候选目录来源。
    server_list_provider: Arc<dyn ServerListProvider>,
    /// 观察到连接身份出现时触发的回调。
    connected_listener: Option<ConnectedListener>,
    /// 观察到已有连接身份消失时触发的回调。
    disconnected_listener: Option<DisconnectedListener>,
    /// 单次调用服务的可选中间件链。
    unary_call_layer: Option<DynamicUnaryCallLayer>,
    /// 双向流建立服务的可选中间件链。
    bi_call_layer: Option<DynamicBiStreamingCallLayer>,
    /// 登录与请求身份生成的认证实现。
    auth_plugin: Arc<dyn AuthPlugin>,
    /// 认证插件初始化及刷新使用的参数，可能含秘密材料。
    auth_context: HashMap<String, String>,
    /// 首次连接初始化的失败尝试阈值，不限制成功建连后的重连。
    max_retries: Option<u32>,
    /// 是否允许首次连通性请求失败后仍返回客户端。
    emergency_start: bool,
}

#[allow(dead_code)]
impl NacosGrpcClientBuilder {
    /// 业务作用：建立 Nacos 客户端装配配置，等待调用方补充身份、能力及传输策略。
    ///
    /// 参数说明：
    /// - `server_list_provider`：连接时读取的候选地址来源，首次目录必须非空。
    ///
    /// 返回：采用默认传输设置和空认证插件的构造器；应急启动关闭，不在构造时建立连接。
    pub(crate) fn new(server_list_provider: Arc<dyn ServerListProvider>) -> Self {
        Self {
            app_name: "unknown".to_owned(),
            client_version: Default::default(),
            namespace: Default::default(),
            labels: Default::default(),
            client_abilities: Default::default(),
            grpc_config: Default::default(),
            server_request_handler_map: Default::default(),
            server_list_provider,
            connected_listener: None,
            disconnected_listener: None,
            unary_call_layer: None,
            bi_call_layer: None,
            auth_context: Default::default(),
            auth_plugin: Arc::new(NoopAuthPlugin::default()),
            max_retries: None,
            emergency_start: false,
        }
    }

    /// 业务作用：设置后续请求默认携带的应用标识，供服务端归属识别。
    ///
    /// 参数说明：
    /// - `app_name`：写入 app 请求头的应用名称，不作为认证凭据。
    ///
    /// 返回：更新后的构造器；显式请求头仍可覆盖此默认值。
    pub(crate) fn app_name(self, app_name: String) -> Self {
        Self {
            app_name,
            ..self
        }
    }

    /// 业务作用：设置连接握手声明的客户端实现版本，供服务端协议协商参考。
    ///
    /// 参数说明：
    /// - `client_version`：握手携带的版本文本，原样保留。
    ///
    /// 返回：保存版本声明的构造器，不据此启用能力或验证兼容性。
    pub(crate) fn client_version(self, client_version: String) -> Self {
        Self {
            client_version,
            ..self
        }
    }

    /// 业务作用：设置连接握手的租户作用域，保持会话声明与业务配置一致。
    ///
    /// 参数说明：
    /// - `namespace`：写入握手 tenant 字段的命名空间，不执行缺省值替换。
    ///
    /// 返回：保存作用域的构造器，不改变目录提供者或完成远端授权。
    pub(crate) fn namespace(self, namespace: String) -> Self {
        Self {
            namespace,
            ..self
        }
    }

    /// 业务作用：为连接握手附加一个客户端标签，供服务端识别客户端属性。
    ///
    /// 参数说明：
    /// - `key`：握手标签键名，按原文匹配。
    /// - `value`：该标签的值，同名配置被覆盖。
    ///
    /// 返回：更新标签集合的构造器，不触发已建连接的动态更新。
    pub(crate) fn add_label(mut self, key: String, value: String) -> Self {
        self.labels.insert(key, value);
        Self {
            ..self
        }
    }

    /// 业务作用：合并一组连接握手标签，集中装配客户端属性声明。
    ///
    /// 参数说明：
    /// - `labels`：移入握手的标签集合，同名键以本次传入值为准。
    ///
    /// 返回：保留其它已有标签的构造器；标签本身不授予访问权限。
    pub(crate) fn add_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels.extend(labels);
        Self {
            ..self
        }
    }

    /// 业务作用：设置首次连接建立阶段的失败尝试阈值，控制初始化等待边界。
    ///
    /// 参数说明：
    /// - `max_retries`：未指定时连接状态机使用 1；零值使首次就绪检查立即越过初始化等待。
    ///
    /// 返回：保存阈值的构造器；阈值仅在尚未成功初始化时生效，不限制后续重连次数。
    pub(crate) fn max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.max_retries = max_retries;
        Self {
            ..self
        }
    }

    /// 业务作用：选择首次连通性请求失败后是否仍交付客户端，以支持上层采用离线缓存启动。
    ///
    /// 参数说明：
    /// - `emergency_start`：为真允许启动探测失败后继续装配，为假向调用方返回错误。
    ///
    /// 返回：更新策略的构造器；开启不会伪造连接健康、加载缓存或免除后续业务请求失败。
    pub(crate) fn emergency_start(mut self, emergency_start: bool) -> Self {
        self.emergency_start = emergency_start;
        Self {
            ..self
        }
    }

    /// 业务作用：在握手能力中声明客户端是否支持远端连接功能。
    ///
    /// 参数说明：
    /// - `enable`：写入能力声明的开关，由服务端协商使用。
    ///
    /// 返回：更新声明的构造器；该开关不创建传输实现或替代访问授权。
    pub(crate) fn support_remote_connection(mut self, enable: bool) -> Self {
        self.client_abilities.support_remote_connection(enable);
        Self {
            ..self
        }
    }

    /// 业务作用：声明配置模块的远端指标能力，供连接握手协商。
    ///
    /// 参数说明：
    /// - `enable`：是否声明支持配置远端指标。
    ///
    /// 返回：保存能力位的构造器，不在此处注册指标处理器或发送采集数据。
    pub(crate) fn support_config_remote_metrics(mut self, enable: bool) -> Self {
        self.client_abilities.support_config_remote_metrics(enable);
        Self {
            ..self
        }
    }

    /// 业务作用：声明命名服务增量推送能力，使服务端了解客户端可接收的更新形式。
    ///
    /// 参数说明：
    /// - `enable`：是否在握手中宣告增量推送能力。
    ///
    /// 返回：更新能力声明的构造器；调用方须确保处理实现与声明一致，此处不安装增量处理器。
    pub(crate) fn support_naming_delta_push(mut self, enable: bool) -> Self {
        self.client_abilities.support_naming_delta_push(enable);
        Self {
            ..self
        }
    }

    /// 业务作用：声明命名服务远端指标能力，作为连接握手的客户端特征。
    ///
    /// 参数说明：
    /// - `enable`：是否声明支持命名服务远端指标。
    ///
    /// 返回：保存该能力位的构造器，不启动指标采集或远端推送。
    pub(crate) fn support_naming_remote_metric(mut self, enable: bool) -> Self {
        self.client_abilities.support_naming_remote_metric(enable);
        Self {
            ..self
        }
    }

    /// 业务作用：保存传输配置中的主机初值，供构造流程维护统一配置对象。
    ///
    /// 参数说明：
    /// - `host`：主机文本；正常 build 流程最终以目录选中地址覆盖它。
    ///
    /// 返回：更新配置的构造器；此方法不能绕过目录提供者固定连接目标。
    pub(crate) fn host(mut self, host: String) -> Self {
        self.grpc_config.host = host;
        self
    }

    /// 业务作用：选择 gRPC 连接使用的显式端口或目录基础端口偏移规则。
    ///
    /// 参数说明：
    /// - `port`：存在时所有选中主机使用此端口；为空时采用目录端口加 1000。
    ///
    /// 返回：保存端口选择的构造器，不验证监听状态或偏移后是否超出 u16 范围。
    pub(crate) fn port(mut self, port: Option<u16>) -> Self {
        self.grpc_config.port = port;
        self
    }

    /// 业务作用：设置请求 origin，供传输层生成请求来源与 authority。
    ///
    /// 参数说明：
    /// - `uri`：待解析的 origin URI，实际连接仍使用目录选择的地址。
    ///
    /// 返回：解析成功更新 origin，失败保留原值的构造器；不返回解析错误。
    pub(crate) fn origin(mut self, uri: &str) -> Self {
        self.grpc_config = self.grpc_config.with_origin(uri);
        self
    }

    /// 业务作用：设置请求中的客户端代理标识，供服务端观测调用来源。
    ///
    /// 参数说明：
    /// - `ua`：待转换为合法 HTTP 头值的 User-Agent 文本。
    ///
    /// 返回：头值合法时保存，非法时保留已有设置；不创建连接或返回错误。
    pub(crate) fn user_agent(mut self, ua: String) -> Self {
        self.grpc_config = self.grpc_config.with_user_agent(ua);
        self
    }

    /// 业务作用：限制端点单次请求等待传输响应的时长，避免无限占用调用等待资源。
    ///
    /// 参数说明：
    /// - `timeout`：交给 tonic 端点的请求超时，不是连接建立超时或整条双向流存续时长。
    ///
    /// 返回：保存超时设置的构造器，实际超时在发送请求后由传输层执行。
    pub(crate) fn timeout(mut self, timeout: Duration) -> Self {
        self.grpc_config.timeout = Some(timeout);
        self
    }

    /// 业务作用：设置单个端点可同时处理的请求上限，为传输提供背压。
    ///
    /// 参数说明：
    /// - `concurrency_limit`：交给 tonic 的并发请求数，此入口不拒绝零值。
    ///
    /// 返回：保存并发限制的构造器；限制属于端点，不是全应用或集群配额。
    pub(crate) fn concurrency_limit(mut self, concurrency_limit: usize) -> Self {
        self.grpc_config.concurrency_limit = Some(concurrency_limit);
        self
    }

    /// 业务作用：限制端点单位时间内提交的请求数量，避免短时请求集中冲击连接。
    ///
    /// 参数说明：
    /// - `rate_limit`：请求数量与时间窗口组成的配额，原样交给 tonic。
    ///
    /// 返回：保存端点限速参数的构造器；参数合法性由后续传输构造处理，不形成跨节点配额。
    pub(crate) fn rate_limit(mut self, rate_limit: (u64, Duration)) -> Self {
        self.grpc_config.rate_limit = Some(rate_limit);
        self
    }

    /// 业务作用：设置 HTTP/2 单流初始接收窗口，控制对端可连续发送的数据量。
    ///
    /// 参数说明：
    /// - `init_stream_window_size`：单流接收窗口字节数，交给传输层应用。
    ///
    /// 返回：保存窗口值的构造器；不改变业务消息大小限制或提前分配正文缓冲。
    pub(crate) fn init_stream_window_size(mut self, init_stream_window_size: u32) -> Self {
        self.grpc_config.init_stream_window_size = Some(init_stream_window_size);
        self
    }

    /// 业务作用：设置 HTTP/2 整条连接的初始接收窗口，约束多个流共享的传输额度。
    ///
    /// 参数说明：
    /// - `init_connection_window_size`：连接级接收窗口字节数。
    ///
    /// 返回：保存连接窗口设置的构造器；与单流窗口分别生效，不是应用队列容量。
    pub(crate) fn init_connection_window_size(mut self, init_connection_window_size: u32) -> Self {
        self.grpc_config.init_connection_window_size = Some(init_connection_window_size);
        self
    }

    /// 业务作用：启用底层 TCP 保活，帮助传输层发现失去响应的连接。
    ///
    /// 参数说明：
    /// - `tcp_keepalive`：交给端点的 TCP 保活时长，具体行为由操作系统实现。
    ///
    /// 返回：保存保活配置的构造器；TCP 保活不替代 Nacos 应用层健康请求。
    pub(crate) fn tcp_keepalive(mut self, tcp_keepalive: Duration) -> Self {
        self.grpc_config.tcp_keepalive = Some(tcp_keepalive);
        self
    }

    /// 业务作用：控制 TCP 是否使用 Nagle 聚合，调整小消息的发送延迟。
    ///
    /// 参数说明：
    /// - `tcp_nodelay`：为真时禁用 Nagle 聚合，为假时允许内核聚合小包。
    ///
    /// 返回：保存套接字策略的构造器；不承诺业务消息立即到达对端。
    pub(crate) fn tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.grpc_config.tcp_nodelay = tcp_nodelay;
        self
    }

    /// 业务作用：设置 HTTP/2 保活探测间隔，供传输连接识别失联。
    ///
    /// 参数说明：
    /// - `http2_keep_alive_interval`：HTTP/2 PING 之间的间隔。
    ///
    /// 返回：保存探测频率的构造器；无活动流时是否探测由独立开关决定。
    pub(crate) fn http2_keep_alive_interval(mut self, http2_keep_alive_interval: Duration) -> Self {
        self.grpc_config.http2_keep_alive_interval = Some(http2_keep_alive_interval);
        self
    }

    /// 业务作用：限定 HTTP/2 保活等待响应的时长，使失联连接可被传输层识别。
    ///
    /// 参数说明：
    /// - `http2_keep_alive_timeout`：保活探测发出后等待回应的时长。
    ///
    /// 返回：保存保活超时的构造器；不改变业务请求超时或 Nacos 健康检查周期。
    pub(crate) fn http2_keep_alive_timeout(mut self, http2_keep_alive_timeout: Duration) -> Self {
        self.grpc_config.http2_keep_alive_timeout = Some(http2_keep_alive_timeout);
        self
    }

    /// 业务作用：决定 HTTP/2 连接没有活动流时是否仍发送保活探测。
    ///
    /// 参数说明：
    /// - `http2_keep_alive_while_idle`：允许空闲保活的开关，实际探测仍依赖保活间隔设置。
    ///
    /// 返回：更新空闲探测策略的构造器；不创建应用层心跳任务。
    pub(crate) fn http2_keep_alive_while_idle(mut self, http2_keep_alive_while_idle: bool) -> Self {
        self.grpc_config.http2_keep_alive_while_idle = Some(http2_keep_alive_while_idle);
        self
    }

    /// 业务作用：限定底层连接建立等待，避免不可达地址长期占据连接尝试。
    ///
    /// 参数说明：
    /// - `connect_timeout`：交给 tonic 连接器的建立超时。
    ///
    /// 返回：保存连接超时的构造器；不包含后续 Nacos 握手和所有重连退避时间。
    pub(crate) fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.grpc_config.connect_timeout = Some(connect_timeout);
        self
    }

    /// 业务作用：选择 HTTP/2 接收窗口是否由传输层自适应调整。
    ///
    /// 参数说明：
    /// - `http2_adaptive_window`：自适应窗口开关，开启后窗口调整由 HTTP/2 实现控制。
    ///
    /// 返回：保存策略的构造器；不改变应用层消息队列或内存限额。
    pub(crate) fn http2_adaptive_window(mut self, http2_adaptive_window: bool) -> Self {
        self.grpc_config.http2_adaptive_window = Some(http2_adaptive_window);
        self
    }

    /// 业务作用：选择连接建立后负责登录和请求身份生成的认证插件。
    ///
    /// 参数说明：
    /// - `auth_plugin`：共享认证实现，覆盖此前插件；实际登录在 build 中执行。
    ///
    /// 返回：保存插件的构造器，不验证凭据或宣告远端授权成功。
    pub(crate) fn auth_plugin(self, auth_plugin: Arc<dyn AuthPlugin>) -> Self {
        Self {
            auth_plugin,
            ..self
        }
    }

    /// 业务作用：配置认证插件登录所需的上下文，供初始化和后续刷新使用。
    ///
    /// 参数说明：
    /// - `auth_context`：完整认证参数集合，整体替换原集合，可能包含秘密材料。
    ///
    /// 返回：持有上下文的构造器，不进行登录，不把参数自动写入业务请求头。
    pub(crate) fn auth_context(self, auth_context: HashMap<String, String>) -> Self {
        Self {
            auth_context,
            ..self
        }
    }

    /// 业务作用：为指定协议类型登记服务端推送处理器，使双向流按消息身份分派。
    ///
    /// 参数说明：
    /// - `handler`：类型 T 对应的共享处理器，覆盖同一身份的既有处理器。
    ///
    /// 返回：更新分派表的构造器；build 会将客户端探测类型重新绑定到内置处理器。
    pub(crate) fn register_server_request_handler<T: GrpcMessageData>(
        mut self,
        handler: Arc<dyn ServerRequestHandler>,
    ) -> Self {
        self.server_request_handler_map
            .insert(T::identity().to_string(), handler);
        Self {
            ..self
        }
    }

    /// 业务作用：登记连接身份从缺失变为存在时的通知，供上层恢复注册或订阅。
    ///
    /// 参数说明：
    /// - `listener`：接收连接 ID 的同步回调，在后台观察任务中执行，应避免阻塞。
    ///
    /// 返回：保存唯一建连回调的构造器；后续设置覆盖前值，watch 合并更新时不保证逐次观察所有状态迁移。
    pub(crate) fn connected_listener(
        mut self,
        listener: impl Fn(String) + Send + Sync + 'static,
    ) -> Self {
        self.connected_listener = Some(Arc::new(listener));
        Self {
            ..self
        }
    }

    /// 业务作用：登记已观察连接身份消失时的通知，供上层撤销会话相关状态。
    ///
    /// 参数说明：
    /// - `listener`：接收此前连接 ID 的同步回调，在后台观察任务中执行。
    ///
    /// 返回：保存唯一断连回调的构造器；通知依赖状态观察，不代表所有远端资源已释放。
    pub(crate) fn disconnected_listener(
        mut self,
        listener: impl Fn(String) + Send + Sync + 'static,
    ) -> Self {
        self.disconnected_listener = Some(Arc::new(listener));
        Self {
            ..self
        }
    }

    /// 业务作用：将新的单次调用策略叠加到已有中间件链，保留已配置的外层处理。
    ///
    /// 参数说明：
    /// - `layer`：新增层；存在旧层时，新层位于旧层内侧、原始服务外侧。
    ///
    /// 返回：包含组合层的构造器，不执行请求；正常向内转发时旧层先收到请求。
    pub(crate) fn unary_call_layer(self, layer: DynamicUnaryCallLayer) -> Self {
        let stack = if let Some(unary_call_layer) = self.unary_call_layer {
            Arc::new(Stack::new(
                DynamicUnaryCallLayerWrapper(layer),
                DynamicUnaryCallLayerWrapper(unary_call_layer),
            ))
        } else {
            layer
        };

        Self {
            unary_call_layer: Some(stack),
            ..self
        }
    }

    /// 业务作用：将新的双向流调用策略加入既有中间件链，统一流建立阶段处理。
    ///
    /// 参数说明：
    /// - `layer`：新增双向流层，已有层保持在其外侧。
    ///
    /// 返回：保存组合层的构造器；此处不建立流或改变流内消息顺序。
    pub(crate) fn bi_call_layer(self, layer: DynamicBiStreamingCallLayer) -> Self {
        let stack = if let Some(bi_call_layer) = self.bi_call_layer {
            Arc::new(Stack::new(
                DynamicBiStreamingCallLayerWrapper(layer),
                DynamicBiStreamingCallLayerWrapper(bi_call_layer),
            ))
        } else {
            layer
        };

        Self {
            bi_call_layer: Some(stack),
            ..self
        }
    }

    /// 业务作用：装配轮询连接、推送处理和认证刷新，执行首次连通性请求后交付客户端。
    ///
    /// 参数说明：
    /// - `id`：客户端实例标识，用于连接与认证任务的诊断关联。
    ///
    /// 返回：探测传输成功或允许应急启动时继续初始化认证并返回客户端；非应急探测失败返回错误。探测不解码业务结果码，目录为空或端点构造非法仍可能 panic。
    pub(crate) async fn build(mut self, id: String) -> Result<NacosGrpcClient, Error> {
        // 探测回执使用固定合同，覆盖同类型自定义处理器，保证连接存活检测的一致行为。
        self.server_request_handler_map.insert(
            ClientDetectionRequest::identity().to_string(),
            Arc::new(ClientDetectionRequestHandler),
        );

        let send_request = {
            let server_list =
                PollingServerListService::from_provider(self.server_list_provider.clone()).await;
            let mut tonic_builder = TonicBuilder::new(self.grpc_config, server_list);
            if let Some(layer) = self.unary_call_layer {
                tonic_builder = tonic_builder.unary_call_layer(layer);
            }

            if let Some(layer) = self.bi_call_layer {
                tonic_builder = tonic_builder.bi_call_layer(layer);
            }

            let mut connection = NacosGrpcConnection::new(
                id.clone(),
                tonic_builder,
                self.server_request_handler_map,
                self.client_version,
                self.namespace,
                self.labels,
                self.client_abilities,
                self.max_retries,
            );

            if let Some(connected_listener) = self.connected_listener {
                connection = connection.connected_listener(connected_listener);
            }

            if let Some(disconnected_listener) = self.disconnected_listener {
                connection = connection.disconnected_listener(disconnected_listener);
            }

            let failover_connection = connection.into_failover_connection(id.clone());
            Arc::new(failover_connection) as Arc<dyn SendRequest + Send + Sync + 'static>
        };

        // 先触发连接状态机与基础传输，再初始化认证；此处只检查传输结果，不解释健康响应正文。
        let health_check_request = GrpcMessageBuilder::new(HealthCheckRequest::default())
            .build()
            .into_payload()?;
        match send_request.send_request(health_check_request).await {
            Ok(_) => {
                tracing::info!("health check passed, connected to Nacos server");
            }
            Err(e) => {
                if self.emergency_start {
                    // 应急策略只允许返回客户端，上层须自行处理离线数据与后续请求失败。
                    tracing::warn!(
                        "health check failed, cannot connect to Nacos server, but continuing startup in emergency mode: {}",
                        e
                    );
                } else {
                    // 未明确允许离线启动时，不能把不可用连接交付为已完成初始化。
                    return Err(e);
                }
            }
        }

        init_auth_plugin(
            self.auth_plugin.clone(),
            self.server_list_provider.clone(),
            self.auth_context.clone(),
            id,
        )
        .await;

        let app_name = self.app_name;
        let auth_plugin = self.auth_plugin;

        Ok(NacosGrpcClient {
            app_name,
            send_request,
            auth_plugin,
        })
    }
}
