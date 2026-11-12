//! gRPC 操作结果转换与单次回调通道构造。

use crate::api::error::Error;
use tokio::sync::oneshot;
use tracing::error;

use super::nacos_grpc_service::Callback;

/// 业务作用：把底层 gRPC 操作失败映射为当前调用阶段的稳定错误上下文。
///
/// 参数说明：
/// - `result`：待转换的操作结果，成功值保持原有所有权。
/// - `context`：调用阶段的固定错误摘要，不应包含请求秘密。
///
/// 返回：成功值原样返回；失败先记录底层错误，再仅以 context 构造 ErrResult，不保留原错误链。
pub(crate) fn convert<T>(result: Result<T, Error>, context: &str) -> Result<T, Error> {
    result.map_err(|e| {
        error!("{}: {}", context, e);
        Error::ErrResult(context.to_string())
    })
}

/// 业务作用：解释等待 gRPC 回调通道的结果，将通道提前关闭转换为 SDK 错误。
///
/// 参数说明：
/// - `result`：已经完成等待的 oneshot 结果，不在本入口发起新的等待。
/// - `context`：通道关闭时返回给调用方的固定操作摘要。
///
/// 返回：收到回调时原样返回值；发送端关闭时记录接收错误并返回 ErrResult，不推断远端操作是否提交。
pub(crate) fn recv_response<T>(
    result: Result<T, oneshot::error::RecvError>,
    context: &str,
) -> Result<T, Error> {
    result.map_err(|e| {
        error!("{}: {}", context, e);
        Error::ErrResult(context.to_string())
    })
}

/// 业务作用：建立单次 gRPC 调用的结果通道和接收意愿协作句柄。
///
/// 参数说明: 无。
///
/// 返回：由发送端持有的 Callback、由调用方等待的 oneshot 接收端以及控制发送意愿的 Taker；此时尚未投递请求。
pub(crate) fn create_grpc_callback<T>() -> (Callback<T>, oneshot::Receiver<T>, want::Taker) {
    let (gv, tk) = want::new();
    let (tx, rx) = oneshot::channel::<T>();
    (Callback::new(gv, tx), rx, tk)
}

/// 业务作用：要求 gRPC 流程中的必要对象存在，使缺失状态以 SDK 错误结束。
///
/// 参数说明：
/// - `opt`：当前阶段要求存在的对象，不在此处创建替代对象。
/// - `context`：对象缺失时使用的固定错误摘要。
///
/// 返回：存在时移交原值；缺失时记录 context 并返回 ErrResult，不继续进入后续调用。
pub(crate) fn unwrap_option<T>(opt: Option<T>, context: &str) -> Result<T, Error> {
    opt.ok_or_else(|| {
        error!("{}", context);
        Error::ErrResult(context.to_string())
    })
}
