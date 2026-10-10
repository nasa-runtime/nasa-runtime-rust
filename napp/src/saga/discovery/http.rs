//! Saga HTTP 收据必须在同一有限预算内完整读取，发现 owner 关闭时取消尚未完成的网络活动。

use super::*;

pub(in crate::saga) struct SagaHttpResponse {
    status: reqwest::StatusCode,
    body: Vec<u8>,
}

impl SagaHttpResponse {
    /// 业务作用：向调用协议暴露已完整接收的响应状态。
    /// 参数说明：无。
    /// 返回：状态只用于业务裁决，不代表数据库事务已经提交。
    pub(in crate::saga) fn status(&self) -> reqwest::StatusCode {
        self.status
    }

    /// 业务作用：让业务协议继续执行其更小的收据尺寸限制。
    /// 参数说明：无。
    /// 返回：已经读取的真实字节数，不信任可缺失的 Content-Length。
    pub(in crate::saga) fn content_length(&self) -> Option<u64> {
        Some(self.body.len() as u64)
    }

    /// 业务作用：移交在原期限内完整取得的收据正文。
    /// 参数说明：无。
    /// 返回：正文已经通过传输边界限制，不再持有网络资源。
    pub(in crate::saga) async fn bytes(self) -> ApplicationResult<Vec<u8>> {
        Ok(self.body)
    }

    /// 业务作用：按业务收据类型解码有限正文，不再延长网络预算。
    /// 参数说明：类型参数指定当前协议的响应结构。
    /// 返回：类型合法的完整 JSON；错误不回显上游正文。
    pub(in crate::saga) async fn json<T: serde::de::DeserializeOwned>(
        self,
    ) -> ApplicationResult<T> {
        serde_json::from_slice(&self.body).map_err(|_| failure("Saga HTTP receipt is invalid"))
    }
}

/// 业务作用：限制完整收据内存，缺失长度头或分块响应也不能绕过边界。
/// 参数说明：`response` 为已经提交请求后收到的响应，读取仍在外层原预算内。
/// 返回：最多四 MiB 的收据；超限和不完整正文拒绝作为成功证据。
async fn receive(mut response: reqwest::Response) -> ApplicationResult<SagaHttpResponse> {
    const LIMIT: usize = 4 * 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|length| length > LIMIT as u64)
    {
        return Err(failure("Saga HTTP receipt exceeds byte limit"));
    }
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| failure("Saga HTTP receipt is incomplete"))?
    {
        if chunk.len() > LIMIT.saturating_sub(body.len()) {
            return Err(failure("Saga HTTP receipt exceeds byte limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(SagaHttpResponse { status, body })
}

/// 业务作用：隐藏 URL、凭据和上游正文，保留投递结果不确定的调用边界。
/// 参数说明：`message` 是固定诊断。
/// 返回：当前业务运行阶段的错误。
fn failure(message: &'static str) -> ApplicationError {
    saga_error(ApplicationPhase::Running, message)
}

#[cfg(feature = "discovery-runtime")]
struct AbortOnDrop(tokio::task::AbortHandle);
#[cfg(feature = "discovery-runtime")]
impl Drop for AbortOnDrop {
    /// 业务作用：调用方放弃等待时取消唯一收据任务，避免请求脱离宿主继续占用资源。
    /// 参数说明：无。
    /// 返回：取消不代表远端事务未执行。
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl ManagedHttpTarget {
    /// 业务作用：在实际 HTTP 提交处复验发现资格，并在原期限内收齐有界响应正文。
    /// 参数说明：`request` 含冻结的路径、认证和剩余期限；没有更短期限时采用目标的有限上限。
    /// 返回：完整收据；已提交后的错误保持结果不明，不自动重放业务请求。
    pub(in crate::saga) async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> ApplicationResult<SagaHttpResponse> {
        let (client, request) = request.build_split();
        let request = request.map_err(|_| failure("invalid Saga HTTP request"))?;
        let timeout = request
            .timeout()
            .copied()
            .unwrap_or(self.timeout)
            .min(self.timeout);
        let deadline = tokio::time::Instant::now() + timeout;
        #[cfg(feature = "discovery-runtime")]
        if let Some(admission) = self.admission.clone() {
            let lease = admission
                .handle
                .reserve_transport()
                .map_err(|_| failure("Saga HTTP owner is unavailable"))?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let task = lease
                .spawn(deadline, async move {
                    use std::{future::Future, task::Poll};
                    let operation = async {
                        let future = client.execute(request);
                        tokio::pin!(future);
                        // 只在实际首次 poll 时消费当前实例资格；之后的不确定结果不能恢复旧许可。
                        let first = futures_util::future::poll_fn(|cx| {
                            Poll::Ready(
                                admission
                                    .observation
                                    .submit_if_current_strict(&admission.instance, || {
                                        future.as_mut().poll(cx)
                                    }),
                            )
                        })
                        .await
                        .map_err(|_| {
                            failure("Saga target lost discovery eligibility before submission")
                        })?;
                        let response = match first {
                            Poll::Ready(response) => response,
                            Poll::Pending => future.await,
                        }
                        .map_err(|_| failure("Saga HTTP delivery result is uncertain"))?;
                        receive(response).await
                    }
                    .await;
                    let _ = sender.send(operation);
                })
                .map_err(|_| failure("Saga HTTP owner is unavailable"))?;
            let _abort = AbortOnDrop(task);
            return receiver
                .await
                .map_err(|_| failure("Saga HTTP delivery ended without a complete receipt"))?;
        }
        tokio::time::timeout_at(deadline, async {
            let response = client
                .execute(request)
                .await
                .map_err(|_| failure("Saga HTTP delivery result is uncertain"))?;
            receive(response).await
        })
        .await
        .map_err(|_| failure("Saga HTTP receipt deadline elapsed"))?
    }
}
