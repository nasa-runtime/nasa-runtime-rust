//! 受管流式响应。后台泵只缓存一个 chunk，截止时间与关闭信号也作用于尚未读取的正文。

use crate::{
    background::BackgroundOwner,
    error::{RestDiscoveryError, Result},
};
use nabudget::RequestBudget;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// 绑定调用预算和发现证据的 HTTP 响应；读取正文仍会复验发送资格。
pub struct ManagedResponse {
    status: reqwest::StatusCode,
    headers: reqwest::header::HeaderMap,
    receiver: mpsc::Receiver<Result<bytes::Bytes>>,
    task: tokio::task::AbortHandle,
    background: Arc<BackgroundOwner>,
    budget: RequestBudget,
    complete: Arc<AtomicBool>,
}

impl ManagedResponse {
    /// 业务作用：接管响应正文并登记有界消费任务，调用方不读取时仍会按期限关闭 I/O。
    /// 参数说明：`response` 是已取得响应头的请求，`background` 保留运行时关闭权，`budget` 延续原请求期限，`permit` 已在发送前预约容量。
    /// 返回：受管流式响应；运行时关闭时不接受新的正文任务。
    pub(crate) fn new(
        mut response: reqwest::Response,
        background: Arc<BackgroundOwner>,
        budget: RequestBudget,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Self> {
        let status = response.status();
        let headers = response.headers().clone();
        let (sender, receiver) = mpsc::channel(1);
        let remaining = budget.clone();
        let complete = Arc::new(AtomicBool::new(false));
        let completed = complete.clone();
        let task = background.spawn(async move {
            let _permit = permit;
            let _ = remaining
                .run(async move {
                    loop {
                        match response.chunk().await {
                            Ok(Some(chunk)) => {
                                if chunk.len() > 8 * 1024 * 1024 {
                                    let _ = sender
                                        .send(Err(RestDiscoveryError::ResponseCapacity))
                                        .await;
                                    break;
                                }
                                if sender.send(Ok(chunk)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(None) => {
                                completed.store(true, Ordering::Release);
                                break;
                            }
                            Err(error) => {
                                let _ = sender.send(Err(RestDiscoveryError::Http(error))).await;
                                break;
                            }
                        }
                    }
                })
                .await;
        })?;
        Ok(Self {
            status,
            headers,
            receiver,
            task,
            background,
            budget,
            complete,
        })
    }

    /// 业务作用：读取业务响应状态，不隐式消费正文。
    /// 参数说明：无。
    /// 返回：原始 HTTP 状态码。
    pub fn status(&self) -> reqwest::StatusCode {
        self.status
    }

    /// 业务作用：读取响应头供流式业务判断内容类型。
    /// 参数说明：无。
    /// 返回：原始响应头的只读引用。
    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        &self.headers
    }

    /// 业务作用：在原请求期限内读取下一段正文。
    /// 参数说明：无。
    /// 返回：EOF 为空；超时、关闭和传输失败保留独立错误类别。
    pub async fn chunk(&mut self) -> Result<Option<bytes::Bytes>> {
        self.background
            .run(async {
                self.budget
                    .run(async {
                        match self.receiver.recv().await {
                            Some(value) => value.map(Some),
                            None if self.complete.load(Ordering::Acquire) => Ok(None),
                            None => Err(RestDiscoveryError::RuntimeClosed),
                        }
                    })
                    .await
                    .map_err(RestDiscoveryError::from)?
            })
            .await
    }
}

impl Drop for ManagedResponse {
    /// 业务作用：业务放弃响应时停止正文泵，避免占用连接直到远端结束。
    /// 参数说明：无。
    /// 返回：任务归运行时继续回收，正文不再被读取。
    fn drop(&mut self) {
        self.task.abort();
    }
}
