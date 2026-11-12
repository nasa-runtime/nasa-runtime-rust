use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Future, Stream};

use async_trait::async_trait;
use tokio::sync::oneshot::Sender;
use tower::{Layer, Service};
use want::Giver;

use crate::api::error::Error::ErrResult;
use crate::{api::error::Error, nacos_proto::v2::Payload};

pub(crate) enum NacosGrpcCall {
    /// 单次请求正文及响应接收方的回调。
    RequestService((Payload, Callback<Result<Payload, Error>>)),

    /// 客户端发送流及服务端接收流的建立结果回调。
    BIRequestService(
        (
            GrpcStream<Payload>,
            Callback<Result<GrpcStream<Result<Payload, Error>>, Error>>,
        ),
    ),
}

pub(crate) struct GrpcStream<T> {
    /// 保持原始消息顺序、等待和结束语义的底层流。
    inner: Pin<Box<dyn Stream<Item = T> + Send + 'static>>,
}

impl<T> GrpcStream<T> {
    /// 业务作用：隐藏底层流实现，使 Nacos 双向传输和中间件共享一致的流接口。
    ///
    /// 参数说明：
    /// - `inner`：已固定地址且可跨线程移动的流，所有权交给包装器。
    ///
    /// 返回：保留原流事件顺序和结束语义的包装器，不主动消费或缓冲数据。
    pub(crate) fn new(inner: Pin<Box<dyn Stream<Item = T> + Send + 'static>>) -> Self {
        Self {
            inner,
        }
    }
}

impl<T> Stream for GrpcStream<T> {
    type Item = T;

    /// 业务作用：将下一条流消息的等待和唤醒交给底层传输，不改变消息顺序。
    ///
    /// 参数说明：
    /// - `cx`：消费任务的唤醒上下文，传递给底层流。
    ///
    /// 返回：原流的消息、等待或结束状态，不重试也不复制消息。
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut().inner.as_mut();
        pin.poll_next(cx)
    }
}

pub(crate) struct Callback<T> {
    /// 接收方需求信号，关闭后不应继续启动传输工作。
    gv: Giver,
    /// 尚未消耗的单次结果发送权，首次发送尝试即取走。
    tx: Option<Sender<T>>,
}

impl<T> Callback<T> {
    /// 业务作用：绑定调用方的需求信号和一次性结果通道，使传输可以在消费取消后停止工作。
    ///
    /// 参数说明：
    /// - `gv`：等待接收方表达需求或撤销需求的信号端。
    /// - `tx`：用于移交唯一结果的发送端。
    ///
    /// 返回：尚未发送结果的回调，不启动请求或等待需求。
    pub(crate) fn new(gv: Giver, tx: Sender<T>) -> Self {
        Self {
            gv,
            tx: Some(tx),
        }
    }

    /// 业务作用：在传输处理前等待调用方确认仍然需要结果，避免继续为已取消请求工作。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：接收方表达需求时为真，需求端关闭时为假；返回真不保证稍后发送仍能成功。
    pub(crate) async fn can_send(&mut self) -> bool {
        if let Err(_closed) = self.gv.want().await {
            return false;
        }
        true
    }

    /// 业务作用：把传输结果移交给等待者，消耗一次性发送权。
    ///
    /// 参数说明：
    /// - `data`：待移交的响应或失败结果，发送失败时随本次调用释放。
    ///
    /// 返回：成功入通道或发送权已被消耗时返回成功；仍持有发送权但接收端已关闭时返回错误，不恢复发送权。
    pub(crate) async fn send(&mut self, data: T) -> Result<(), Error> {
        let sender = self.tx.take();
        if let Some(sender) = sender
            && sender.send(data).is_err()
        {
            return Err(ErrResult(
                "callback send failed. the receiver has been dropped".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait ServerRequestHandler: Send + Sync {
    /// 业务作用：处理双向流中的服务端控制请求，并按具体协议决定是否返回回复。
    ///
    /// 参数说明：
    /// - `request`：收到的完整 Payload；实现负责解码、校验和业务状态处理。
    ///
    /// 返回：需要回执时提供编码后的回复；为空时不发送回执，不能由此推断操作已成功执行。
    async fn request_reply(&self, request: Payload) -> Option<Payload>;
}

pub(crate) type DynamicUnaryCallService = Box<
    dyn Service<
            Payload,
            Error = Error,
            Response = Payload,
            Future = Pin<Box<dyn Future<Output = Result<Payload, Error>> + Send + 'static>>,
        > + Sync
        + Send,
>;

pub(crate) type DynamicUnaryCallLayer =
    Arc<dyn Layer<DynamicUnaryCallService, Service = DynamicUnaryCallService> + Sync + Send>;

pub(crate) struct DynamicUnaryCallLayerWrapper(
    /// 对单次调用服务执行装配的动态层。
    pub(crate) DynamicUnaryCallLayer,
);

impl Layer<DynamicUnaryCallService> for DynamicUnaryCallLayerWrapper {
    type Service = DynamicUnaryCallService;

    /// 业务作用：把动态单次调用中间件接入 tower 组合，使请求经过已配置的策略。
    ///
    /// 参数说明：
    /// - `inner`：被当前中间件包裹的单次调用服务。
    ///
    /// 返回：动态层产生的服务；包装过程不执行请求，其错误与副作用由具体层决定。
    fn layer(&self, inner: DynamicUnaryCallService) -> Self::Service {
        self.0.layer(inner)
    }
}

pub(crate) struct UnaryCallIdentityLayer;
impl Layer<DynamicUnaryCallService> for UnaryCallIdentityLayer {
    type Service = DynamicUnaryCallService;

    /// 业务作用：在未配置单次调用中间件时保留统一装配入口。
    ///
    /// 参数说明：
    /// - `inner`：原始单次调用服务。
    ///
    /// 返回：原服务所有权，不附加鉴权、重试、限流或其它调用行为。
    fn layer(&self, inner: DynamicUnaryCallService) -> Self::Service {
        inner
    }
}

pub(crate) type DynamicBiStreamingCallService = Box<
    dyn Service<
            GrpcStream<Payload>,
            Error = Error,
            Response = GrpcStream<Result<Payload, Error>>,
            Future = Pin<
                Box<
                    dyn Future<Output = Result<GrpcStream<Result<Payload, Error>>, Error>>
                        + Send
                        + 'static,
                >,
            >,
        > + Sync
        + Send
        + 'static,
>;

pub(crate) type DynamicBiStreamingCallLayer = Arc<
    dyn Layer<DynamicBiStreamingCallService, Service = DynamicBiStreamingCallService> + Sync + Send,
>;

pub(crate) struct DynamicBiStreamingCallLayerWrapper(
    /// 对双向流服务执行装配的动态层。
    pub(crate) DynamicBiStreamingCallLayer,
);

impl Layer<DynamicBiStreamingCallService> for DynamicBiStreamingCallLayerWrapper {
    type Service = DynamicBiStreamingCallService;

    /// 业务作用：把动态双向流中间件接入 tower 组合，统一流建立阶段的策略装配。
    ///
    /// 参数说明：
    /// - `inner`：需要被包裹的双向流服务。
    ///
    /// 返回：动态层产生的双向流服务；不在此处建立网络流或消费消息。
    fn layer(&self, inner: DynamicBiStreamingCallService) -> Self::Service {
        self.0.layer(inner)
    }
}

pub(crate) struct BiStreamingCallIdentityLayer;
impl Layer<DynamicBiStreamingCallService> for BiStreamingCallIdentityLayer {
    type Service = DynamicBiStreamingCallService;

    /// 业务作用：在未配置双向流中间件时保留原始流服务的行为。
    ///
    /// 参数说明：
    /// - `inner`：原始双向流服务，所有权直接交给调用方。
    ///
    /// 返回：未增加额外处理的服务，不建立流、重排消息或改变错误结果。
    fn layer(&self, inner: DynamicBiStreamingCallService) -> Self::Service {
        inner
    }
}
