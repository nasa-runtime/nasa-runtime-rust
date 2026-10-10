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
    RequestService((Payload, Callback<Result<Payload, Error>>)),

    BIRequestService(
        (
            GrpcStream<Payload>,
            Callback<Result<GrpcStream<Result<Payload, Error>>, Error>>,
        ),
    ),
}

pub(crate) struct GrpcStream<T> {
    inner: Pin<Box<dyn Stream<Item = T> + Send + 'static>>,
}

impl<T> GrpcStream<T> {
    pub(crate) fn new(inner: Pin<Box<dyn Stream<Item = T> + Send + 'static>>) -> Self {
        Self { inner }
    }
}

impl<T> Stream for GrpcStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut().inner.as_mut();
        pin.poll_next(cx)
    }
}

pub(crate) struct Callback<T> {
    gv: Giver,
    tx: Option<Sender<T>>,
}

impl<T> Callback<T> {
    pub(crate) fn new(gv: Giver, tx: Sender<T>) -> Self {
        Self { gv, tx: Some(tx) }
    }

    pub(crate) async fn can_send(&mut self) -> bool {
        if let Err(_closed) = self.gv.want().await {
            return false;
        }
        true
    }

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

pub(crate) struct DynamicUnaryCallLayerWrapper(pub(crate) DynamicUnaryCallLayer);

impl Layer<DynamicUnaryCallService> for DynamicUnaryCallLayerWrapper {
    type Service = DynamicUnaryCallService;

    fn layer(&self, inner: DynamicUnaryCallService) -> Self::Service {
        self.0.layer(inner)
    }
}

pub(crate) struct UnaryCallIdentityLayer;
impl Layer<DynamicUnaryCallService> for UnaryCallIdentityLayer {
    type Service = DynamicUnaryCallService;

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

pub(crate) struct DynamicBiStreamingCallLayerWrapper(pub(crate) DynamicBiStreamingCallLayer);

impl Layer<DynamicBiStreamingCallService> for DynamicBiStreamingCallLayerWrapper {
    type Service = DynamicBiStreamingCallService;

    fn layer(&self, inner: DynamicBiStreamingCallService) -> Self::Service {
        self.0.layer(inner)
    }
}

pub(crate) struct BiStreamingCallIdentityLayer;
impl Layer<DynamicBiStreamingCallService> for BiStreamingCallIdentityLayer {
    type Service = DynamicBiStreamingCallService;

    fn layer(&self, inner: DynamicBiStreamingCallService) -> Self::Service {
        inner
    }
}
