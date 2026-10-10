//! 双向流只缓存一个协议 frame，后台期限不依赖业务继续 poll 正文。

use super::*;
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame};
use http_body_util::BodyExt;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

pub(super) struct AbortOnDrop(pub tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    /// 业务作用：响应被丢弃或发送提前结束时终止对应正文泵。
    /// 参数说明：无。
    /// 返回：任务仍由 discovery owner 回收。
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Frames {
    receiver: mpsc::Receiver<Result<Frame<Bytes>, Status>>,
    _abort: Option<AbortOnDrop>,
    complete: Arc<AtomicBool>,
    budget: RequestBudget,
    ended: bool,
}
impl HttpBody for Frames {
    type Data = Bytes;
    type Error = Status;
    /// 业务作用：将有界帧通道提供给 tonic，预算终止不能伪装为正常 EOF。
    /// 参数说明：`cx` 提供当前正文读取任务的唤醒器。
    /// 返回：数据或 trailers 帧、真实 EOF，或固定期限/关闭错误。
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        // 已缓存的收据也必须在原期限内交付，后台读完不代表调用方已经确认。
        if self.budget.remaining().is_zero() {
            self.ended = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "gRPC receipt deadline elapsed",
            ))));
        }
        if !self.complete.load(Ordering::Acquire) {
            if let Err(error) = self.budget.check() {
                self.ended = true;
                return Poll::Ready(Some(Err(budget_error(error))));
            }
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(None) => {
                self.ended = true;
                if !self.complete.load(Ordering::Acquire) {
                    Poll::Ready(Some(Err(Status::unavailable("gRPC stream owner closed"))))
                } else {
                    Poll::Ready(None)
                }
            }
            value => value,
        }
    }
}

/// 业务作用：将请求流交给有限正文泵，不将完整客户端流缓存在内存中。
/// 参数说明：`shared` 提供 owner 容量，`body` 是 tonic 请求正文，`budget` 延续原调用期限。
/// 返回：给 HTTP/2 的正文与由 RPC scope 持有的终止句柄。
pub(super) fn pump_request(
    shared: &Shared,
    body: Body,
    budget: RequestBudget,
) -> Result<(Body, tokio::task::AbortHandle), Status> {
    let (receiver, task, complete) = pump(shared, body, budget.clone(), async {})?;
    Ok((
        Body::new(Frames {
            receiver,
            _abort: None,
            complete,
            budget,
            ended: false,
        }),
        task,
    ))
}

/// 业务作用：持续监督响应流，即使业务从未读取响应也会按期限终止并释放容量。
/// 参数说明：`shared` 提供 owner，`body` 是网络正文，`budget` 是原期限，`resources` 持有 RPC 归还责任。
/// 返回：可由所有 tonic 调用类型消费的受管正文。
pub(super) fn pump_response(
    shared: &Shared,
    body: hyper::body::Incoming,
    budget: RequestBudget,
    resources: impl Future<Output = ()> + Send + 'static,
) -> Result<Body, Status> {
    let (receiver, task, complete) = pump(shared, body, budget.clone(), resources)?;
    Ok(Body::new(Frames {
        receiver,
        _abort: Some(AbortOnDrop(task)),
        complete,
        budget,
        ended: false,
    }))
}

type PumpResources = (
    mpsc::Receiver<Result<Frame<Bytes>, Status>>,
    tokio::task::AbortHandle,
    Arc<AtomicBool>,
);

/// 业务作用：在 owner 下运行仅一个 frame 缓冲的正文转发，正常 EOF 与关闭分别记录。
/// 参数说明：`shared` 是容量来源，`body` 是输入帧，`budget` 控制全过程，`resources` 随任务结束释放。
/// 返回：有界帧接收端与任务终止句柄。
fn pump<B>(
    shared: &Shared,
    mut body: B,
    budget: RequestBudget,
    resources: impl Future<Output = ()> + Send + 'static,
) -> Result<PumpResources, Status>
where
    B: HttpBody<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let owner = shared
        .discovery
        .reserve_transport()
        .map_err(discovery_error)?;
    let maximum = shared.options.max_frame_bytes;
    let (sender, receiver) = mpsc::channel(1);
    let complete = Arc::new(AtomicBool::new(false));
    let finished = complete.clone();
    let task = owner
        .spawn(budget.deadline(), async move {
            let _resources = resources;
            let _ = budget
                .run(async move {
                    while let Some(frame) = body.frame().await {
                        let frame = frame
                            .map_err(|_| Status::unavailable("gRPC stream transport failed"))
                            .and_then(|frame| {
                                if frame.data_ref().is_some_and(|data| data.len() > maximum) {
                                    Err(Status::resource_exhausted(
                                        "gRPC frame exceeds managed capacity",
                                    ))
                                } else {
                                    Ok(frame)
                                }
                            });
                        let failed = frame.is_err();
                        if sender.send(frame).await.is_err() || failed {
                            return;
                        }
                    }
                    finished.store(true, Ordering::Release);
                })
                .await;
        })
        .map_err(discovery_error)?;
    Ok((receiver, task, complete))
}
