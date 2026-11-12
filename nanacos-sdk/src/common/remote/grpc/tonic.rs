use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, StreamExt};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::{Service, layer::util::Stack};
use tracing::{Instrument, Span, debug, debug_span, error};

use super::config::GrpcConfiguration;
use super::nacos_grpc_service::{
    BiStreamingCallIdentityLayer, Callback, DynamicBiStreamingCallLayer,
    DynamicBiStreamingCallLayerWrapper, DynamicBiStreamingCallService, DynamicUnaryCallLayer,
    DynamicUnaryCallLayerWrapper, DynamicUnaryCallService, GrpcStream, NacosGrpcCall,
    UnaryCallIdentityLayer,
};
use crate::api::error::Error;
use crate::api::error::Error::NoAvailableServer;
use crate::api::error::Error::TonicGrpcStatus;
use crate::common::remote::server_list::ServerAddress;
use crate::nacos_proto::v2::{
    Payload, bi_request_stream_client::BiRequestStreamClient, request_client::RequestClient,
};

#[derive(Clone)]
pub(crate) struct Tonic {
    /// 共享传输通道，同时用于端点就绪检查。
    channel: Channel,
    /// 单次请求使用的生成客户端。
    request_client: RequestClient<Channel>,
    /// 双向流建立使用的生成客户端。
    bi_client: BiRequestStreamClient<Channel>,
    /// 每次单次调用装配的中间件链。
    unary_call_layer: DynamicUnaryCallLayer,
    /// 每次流建立装配的中间件链。
    bi_call_layer: DynamicBiStreamingCallLayer,
    /// 当前端点 URI，供传输诊断关联。
    endpoint_uri: Uri,
    /// 可撤销连接尝试许可的地址对象。
    server_address: Arc<dyn ServerAddress>,
}

impl Tonic {
    /// 业务作用：按选定地址创建延迟连接的 gRPC 端点，装配传输限制、TLS 和两类请求客户端。
    ///
    /// 参数说明：
    /// - `grpc_config`：已确定目标的传输配置，可选项按需应用到端点。
    /// - `unary_call_layer`：单次调用服务使用的中间件链。
    /// - `bi_call_layer`：双向流建立服务使用的中间件链。
    /// - `server_address`：地址可用性来源，后续就绪检查仍会查询它。
    ///
    /// 返回：共享延迟 Channel 的适配器，不证明网络已连通；URI、头值、TLS 配置或 CA 文件无效时可能 panic，需要 Tokio 运行环境。
    pub fn new(
        grpc_config: GrpcConfiguration,
        unary_call_layer: DynamicUnaryCallLayer,
        bi_call_layer: DynamicBiStreamingCallLayer,
        server_address: Arc<dyn ServerAddress>,
    ) -> Self {
        let url_authority = if let Some(port) = grpc_config.port {
            format!("{}:{}", grpc_config.host, port)
        } else {
            grpc_config.host
        };

        let scheme = if cfg!(feature = "tls") {
            "https"
        } else {
            "http"
        };

        let endpoint_uri = Uri::builder()
            .scheme(scheme)
            .authority(url_authority)
            .path_and_query("/")
            .build()
            .expect("Endpoint URI construction should not fail with valid inputs");

        debug!("create new endpoint :{}", endpoint_uri);
        let mut endpoint = Endpoint::from(endpoint_uri.clone());

        if let Some(origin) = grpc_config.origin {
            endpoint = endpoint.origin(origin);
        }

        if let Some(user_agent) = grpc_config.user_agent {
            endpoint = endpoint
                .user_agent(user_agent)
                .expect("User agent should be settable with valid input");
        }

        if let Some(timeout) = grpc_config.timeout {
            endpoint = endpoint.timeout(timeout);
        }

        if let Some(concurrency_limit) = grpc_config.concurrency_limit {
            endpoint = endpoint.concurrency_limit(concurrency_limit);
        }

        if let Some(rate_limit) = grpc_config.rate_limit {
            endpoint = endpoint.rate_limit(rate_limit.0, rate_limit.1);
        }

        if let Some(init_stream_window_size) = grpc_config.init_stream_window_size {
            endpoint = endpoint.initial_stream_window_size(init_stream_window_size);
        }

        if let Some(init_connection_window_size) = grpc_config.init_connection_window_size {
            endpoint = endpoint.initial_connection_window_size(init_connection_window_size);
        }

        if let Some(http2_keep_alive_interval) = grpc_config.http2_keep_alive_interval {
            endpoint = endpoint.http2_keep_alive_interval(http2_keep_alive_interval);
        }

        if let Some(http2_keep_alive_timeout) = grpc_config.http2_keep_alive_timeout {
            endpoint = endpoint.keep_alive_timeout(http2_keep_alive_timeout);
        }

        if let Some(http2_keep_alive_while_idle) = grpc_config.http2_keep_alive_while_idle {
            endpoint = endpoint.keep_alive_while_idle(http2_keep_alive_while_idle);
        }

        if let Some(http2_adaptive_window) = grpc_config.http2_adaptive_window {
            endpoint = endpoint.http2_adaptive_window(http2_adaptive_window);
        }

        if let Some(connect_time) = grpc_config.connect_timeout {
            endpoint = endpoint.connect_timeout(connect_time);
        }

        endpoint = endpoint.tcp_nodelay(grpc_config.tcp_nodelay);
        endpoint = endpoint.tcp_keepalive(grpc_config.tcp_keepalive);

        #[cfg(feature = "tls")]
        {
            // 启用 TLS 后不降级明文；显式 CA 读取或端点安全配置失败直接终止构造。
            use tonic::transport::{Certificate, ClientTlsConfig};

            let mut tls_config = ClientTlsConfig::new();
            if let Ok(ca_cert_path) =
                std::env::var(crate::api::constants::ENV_NACOS_CLIENT_TLS_CA_CERT)
            {
                let ca_cert_pem = std::fs::read_to_string(&ca_cert_path).unwrap_or_else(|e| {
                    panic!(
                        "Failed to read TLS CA certificate file '{}': {}",
                        ca_cert_path, e
                    )
                });
                tls_config = tls_config.ca_certificate(Certificate::from_pem(ca_cert_pem));
            }
            endpoint = endpoint
                .tls_config(tls_config)
                .expect("Failed to configure TLS on gRPC endpoint");
        }

        let channel = endpoint.connect_lazy();

        let request_client = RequestClient::new(channel.clone());

        let bi_client = BiRequestStreamClient::new(channel.clone());

        Self {
            channel,
            request_client,
            bi_client,
            unary_call_layer,
            bi_call_layer,
            endpoint_uri,
            server_address,
        }
    }
}

/// 业务作用：在接收方仍需要结果且中间件就绪后执行单次请求，将结果移交一次性回调。
///
/// 参数说明：
/// - `service`：已组合中间件的单次调用服务。
/// - `request`：请求 Payload 与结果回调，整体移交给任务。
///
/// 返回：等待需求、就绪、调用及回传的任务；需求撤销或回传失败使任务失败，业务调用错误作为回调内容传递。
fn unary_request(
    mut service: DynamicUnaryCallService,
    request: (Payload, Callback<Result<Payload, Error>>),
) -> GrpcCallTask {
    let task = async move {
        let (payload, mut cb) = request;
        let is_ok = cb.can_send().await;
        // 接收方已取消时不再启动请求，避免无人消费的远端副作用。
        if !is_ok {
            return Err(Error::ErrResult(
                "unary_request failed, callback can not invoke, receiver has been closed."
                    .to_string(),
            ));
        }
        let ready = futures::future::poll_fn(|cx| service.poll_ready(cx))
            .in_current_span()
            .await;
        if let Err(e) = ready {
            error!("unary request service is not ready.{e}");
            return Err(e);
        }

        let response = service.call(payload).in_current_span().await;
        let send_ret = cb.send(response).await;
        if let Err(e) = send_ret {
            error!(
                "unary_request failed, callback can not invoke, send error. {}",
                e
            );
            return Err(Error::ErrResult(
                "unary_request failed, callback can not invoke, send error.".to_string(),
            ));
        }
        Ok(())
    }
    .in_current_span();

    GrpcCallTask::new(task)
}

/// 业务作用：在调用方仍等待结果时建立双向流，将接收流或建立失败交给回调。
///
/// 参数说明：
/// - `service`：已装配中间件的双向流服务。
/// - `request`：客户端发送流与服务端接收流的建立结果回调。
///
/// 返回：等待需求、就绪和流建立的任务；需求撤销、就绪或回传失败使任务失败，流建立的业务错误通过回调返回。
fn bi_request(
    mut service: DynamicBiStreamingCallService,
    request: (
        GrpcStream<Payload>,
        Callback<Result<GrpcStream<Result<Payload, Error>>, Error>>,
    ),
) -> GrpcCallTask {
    let task = async move {
        let (stream, mut cb) = request;
        let is_ok = cb.can_send().await;
        // 只有接收方仍在等待才建立长连接，防止取消请求遗留无人管理的流。
        if !is_ok {
            return Err(Error::ErrResult(
                "bi_request failed, callback can not invoke, receiver has been closed.".to_string(),
            ));
        }

        let ready = futures::future::poll_fn(|cx| service.poll_ready(cx))
            .in_current_span()
            .await;
        if let Err(e) = ready {
            error!("bi request service is not ready. {e}");
            return Err(e);
        }

        let response = service.call(stream).in_current_span().await;

        let send_ret = cb.send(response).await;
        if let Err(e) = send_ret {
            error!(
                "bi_request failed, callback can not invoke, send error. {}",
                e
            );
            return Err(Error::ErrResult(
                "bi_request failed, callback can not invoke, send error.".to_string(),
            ));
        }
        Ok(())
    }
    .in_current_span();

    GrpcCallTask::new(task)
}

pub(crate) struct TonicBuilder<S> {
    /// 每次选择地址后应用的传输配置模板。
    grpc_config: GrpcConfiguration,
    /// 候选地址选择服务。
    server_list: S,
    /// 工厂创建的端点共用的单次调用层。
    unary_call_layer: DynamicUnaryCallLayer,
    /// 工厂创建的端点共用的双向流层。
    bi_call_layer: DynamicBiStreamingCallLayer,
}

impl<S> TonicBuilder<S>
where
    S: Service<(), Response = Arc<dyn ServerAddress>, Error = Error>,
    S::Future: Send + 'static,
{
    /// 业务作用：绑定地址选择服务和传输配置，为连接状态机提供可重复创建的端点工厂。
    ///
    /// 参数说明：
    /// - `grpc_config`：后续每次建连复制的端点配置。
    /// - `server_list`：负责每次选择候选地址的服务。
    ///
    /// 返回：采用直通中间件的工厂；尚未选择地址或创建端点。
    pub(crate) fn new(grpc_config: GrpcConfiguration, server_list: S) -> Self {
        Self {
            grpc_config,
            server_list,
            unary_call_layer: Arc::new(UnaryCallIdentityLayer),
            bi_call_layer: Arc::new(BiStreamingCallIdentityLayer),
        }
    }

    /// 业务作用：为工厂创建的单次调用端点叠加中间件策略。
    ///
    /// 参数说明：
    /// - `layer`：新增动态层，置于原有层内侧、原始调用服务外侧。
    ///
    /// 返回：保存组合层的工厂，已有外层保持其调用次序，不立即执行网络操作。
    pub(crate) fn unary_call_layer(self, layer: DynamicUnaryCallLayer) -> Self {
        let stack = Arc::new(Stack::new(
            DynamicUnaryCallLayerWrapper(layer),
            DynamicUnaryCallLayerWrapper(self.unary_call_layer),
        ));
        Self {
            unary_call_layer: stack,
            ..self
        }
    }

    /// 业务作用：为工厂创建的双向流端点叠加流建立策略。
    ///
    /// 参数说明：
    /// - `layer`：新增动态层，原有层继续包裹新增层。
    ///
    /// 返回：保存双向流组合层的工厂，不建立流或消费客户端消息。
    pub(crate) fn bi_call_layer(self, layer: DynamicBiStreamingCallLayer) -> Self {
        let stack = Arc::new(Stack::new(
            DynamicBiStreamingCallLayerWrapper(layer),
            DynamicBiStreamingCallLayerWrapper(self.bi_call_layer),
        ));
        Self {
            bi_call_layer: stack,
            ..self
        }
    }
}

impl<S> Service<()> for TonicBuilder<S>
where
    S: Service<(), Response = Arc<dyn ServerAddress>, Error = Error>,
    S::Future: Send + 'static,
{
    type Response = Tonic;

    type Error = Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// 业务作用：在创建端点前确认地址选择服务能接受下一次请求。
    ///
    /// 参数说明：
    /// - `cx`：当前建连任务的唤醒上下文。
    ///
    /// 返回：地址选择服务的就绪、等待或错误状态；不证明选中端点可连接。
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let _span_enter = debug_span!("tonic_builder").entered();
        self.server_list.poll_ready(cx)
    }

    /// 业务作用：选择本次连接地址，套用 gRPC 端口规则并创建延迟连接适配器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：地址选择失败时返回错误，否则返回端点。主机以目录值覆盖配置；未指定端口时加 1000，该加法未校验溢出，端点配置非法也可能 panic。
    fn call(&mut self, _: ()) -> Self::Future {
        let _span_enter = debug_span!("tonic_builder").entered();

        let server_info_fut = self.server_list.call(());
        let mut grpc_config = self.grpc_config.clone();
        let unary_call_layer = self.unary_call_layer.clone();
        let bi_call_layer = self.bi_call_layer.clone();

        let tonic_fut = async move {
            let server_address = server_info_fut.await?;

            if grpc_config.port.is_none() {
                grpc_config.port = Some(server_address.port() + 1000);
            }
            grpc_config.host = server_address.host();

            let tonic = Tonic::new(grpc_config, unary_call_layer, bi_call_layer, server_address);
            Ok(tonic)
        }
        .in_current_span();
        Box::pin(tonic_fut)
    }
}

impl Service<NacosGrpcCall> for Tonic {
    type Response = ();

    type Error = Error;

    type Future = GrpcCallTask;

    /// 业务作用：先检查地址是否仍允许使用，再查询 Channel 的请求就绪状态。
    ///
    /// 参数说明：
    /// - `cx`：等待传输背压解除时使用的唤醒上下文。
    ///
    /// 返回：地址不可用时立即拒绝；否则保留 Channel 等待或成功状态，并转换传输错误，不执行 Nacos 业务健康检查。
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // 地址来源已撤销许可时不能继续复用旧 Channel，即使底层仍报告可发送。
        if !self.server_address.is_available() {
            error!(
                "the server address {}:{} is not available",
                self.server_address.host(),
                self.server_address.port()
            );
            return Poll::Ready(Err(NoAvailableServer));
        }
        match self.channel.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::TonicGrpcTransport(e))),
        }
    }

    /// 业务作用：按单次请求或双向流建立的类型创建调用任务，并装配对应中间件。
    ///
    /// 参数说明：
    /// - `call`：请求内容及一次性回调，携带单次消息或客户端发送流。
    ///
    /// 返回：保持当前调用诊断上下文的任务；实际网络工作在任务被轮询后执行。
    fn call(&mut self, call: NacosGrpcCall) -> Self::Future {
        match call {
            NacosGrpcCall::RequestService(request) => {
                let _span_enter =
                    debug_span!("tonic_unary", server = self.endpoint_uri.to_string()).entered();
                let unary_request_client = self.request_client.clone();
                let unary_call_service = UnaryCallService::new(unary_request_client);
                let dynamic_unary_call_service =
                    self.unary_call_layer.layer(Box::new(unary_call_service));
                unary_request(dynamic_unary_call_service, request)
            }
            NacosGrpcCall::BIRequestService(request) => {
                let _span_enter =
                    debug_span!("tonic_bi_stream", server = self.endpoint_uri.to_string())
                        .entered();
                let bi_request_client = self.bi_client.clone();
                let bi_call_service = BiStreamingCallService::new(bi_request_client);
                let dynamic_bi_call_service = self.bi_call_layer.layer(Box::new(bi_call_service));
                bi_request(dynamic_bi_call_service, request)
            }
        }
    }
}

type InnerTask = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;

pub(crate) struct GrpcCallTask {
    /// 请求执行及回调结果移交任务。
    inner: InnerTask,
    /// 创建任务时捕获的调用诊断上下文。
    span: Span,
}

impl GrpcCallTask {
    /// 业务作用：把异步传输处理绑定到当前诊断上下文，供后台执行器统一调度。
    ///
    /// 参数说明：
    /// - `inner`：完成请求及回调移交的任务，成功不一定代表远端业务成功。
    ///
    /// 返回：固定任务地址并保存当前 span 的包装器，尚未轮询或启动任务。
    pub(crate) fn new<F>(inner: F) -> Self
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        Self {
            inner: Box::pin(inner),
            span: Span::current(),
        }
    }
}

impl Future for GrpcCallTask {
    type Output = Result<(), Error>;

    /// 业务作用：在捕获的诊断上下文中推进传输任务，并记录任务级失败。
    ///
    /// 参数说明：
    /// - `cx`：后台执行任务的唤醒上下文。
    ///
    /// 返回：原任务的等待、完成或错误状态；记录失败后仍保留原错误，不自动重试。
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let _enter = this.span.enter();

        let poll = this.inner.as_mut().poll(cx);

        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(ret)) => Poll::Ready(Ok(ret)),
            Poll::Ready(Err(e)) => {
                error!("grpc call task get an error: {e}");
                Poll::Ready(Err(e))
            }
        }
    }
}

pub(crate) struct UnaryCallService {
    /// 持有共享 Channel 的单次请求客户端。
    client: RequestClient<Channel>,
}

impl UnaryCallService {
    /// 业务作用：将生成的 Nacos 单次请求客户端适配为可组合的 tower 服务。
    ///
    /// 参数说明：
    /// - `client`：使用已配置 Channel 的生成客户端。
    ///
    /// 返回：持有客户端的服务，不发起请求或重新建立 Channel。
    pub(crate) fn new(client: RequestClient<Channel>) -> Self {
        Self {
            client,
        }
    }
}

impl Service<Payload> for UnaryCallService {
    type Response = Payload;

    type Error = Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    /// 业务作用：允许单次调用适配器接收请求，实际传输就绪等待由生成客户端执行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：立即就绪，不证明远端已连接或请求一定成功。
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// 业务作用：经生成客户端执行一次 Nacos 请求，保留响应 Payload 交给上层解码。
    ///
    /// 参数说明：
    /// - `req`：已编码的请求，所有权移入异步发送任务。
    ///
    /// 返回：成功时移交响应正文，gRPC status 失败时转换为框架错误；不在此判断 Nacos 业务结果码。
    fn call(&mut self, req: Payload) -> Self::Future {
        let mut client = self.client.clone();
        let fut = async move {
            let response = client.request(req).in_current_span().await;
            match response {
                Ok(ret) => Ok(ret.into_inner()),
                Err(status) => Err(TonicGrpcStatus(status)),
            }
        }
        .in_current_span();
        Box::pin(fut)
    }
}

pub(crate) struct BiStreamingCallService {
    /// 持有共享 Channel 的双向流客户端。
    client: BiRequestStreamClient<Channel>,
}

impl BiStreamingCallService {
    /// 业务作用：将生成的双向流客户端适配为 tower 服务，供流建立中间件组合。
    ///
    /// 参数说明：
    /// - `client`：使用既有 Channel 的双向流客户端。
    ///
    /// 返回：持有客户端的适配器，尚未打开网络流或消费消息。
    pub(crate) fn new(client: BiRequestStreamClient<Channel>) -> Self {
        Self {
            client,
        }
    }
}

impl Service<GrpcStream<Payload>> for BiStreamingCallService {
    type Response = GrpcStream<Result<Payload, Error>>;

    type Error = Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    /// 业务作用：允许双向流适配器接收建立请求，传输等待在生成客户端内部进行。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：立即就绪，不表示流已建立或服务端已认可连接握手。
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// 业务作用：建立 Nacos 双向流，将服务端逐条响应转换为统一流接口。
    ///
    /// 参数说明：
    /// - `req`：客户端发送流，交给生成客户端持续读取。
    ///
    /// 返回：建立成功时返回保序接收流，每项 status 错误转换为框架错误；建立失败直接返回错误，不缓存或重放消息。
    fn call(&mut self, req: GrpcStream<Payload>) -> Self::Future {
        let mut client = self.client.clone();

        let fut = async move {
            let response = client.request_bi_stream(req).in_current_span().await;
            match response {
                Ok(response) => {
                    let response = response
                        .into_inner()
                        .map(|item| match item {
                            Ok(payload) => Ok(payload),
                            Err(status) => Err(TonicGrpcStatus(status)),
                        })
                        .boxed();
                    Ok(GrpcStream::new(response))
                }
                Err(status) => Err(TonicGrpcStatus(status)),
            }
        }
        .in_current_span();
        Box::pin(fut)
    }
}
