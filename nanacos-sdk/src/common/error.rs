use crate::api::error::Error;
use crate::api::error::Result;
use crate::common::remote::grpc::message::GrpcResponseMessage;

/// 业务作用：把 Nacos 业务响应状态转换为调用结果，区分传输完成与业务操作被接受。
///
/// 参数说明：
/// - `response`：已收到的 gRPC 业务响应，通过其成功标记和错误字段裁决。
/// - `operation`：调用方给出的固定操作名称，仅用于失败摘要。
///
/// 返回：业务成功时完成；否则返回包含响应代码和消息的 ErrResult，不在此处重试或脱敏远端消息。
pub(crate) fn handle_response<T: GrpcResponseMessage>(response: &T, operation: &str) -> Result<()> {
    if response.is_success() {
        Ok(())
    } else {
        Err(Error::ErrResult(to_err_msg(response, operation)))
    }
}

/// 业务作用：保留服务端业务拒绝的代码与说明，供 SDK 构造错误结果。
///
/// 参数说明：
/// - `response`：失败响应，其可选消息为空时使用空串。
/// - `operation`：标识失败操作的稳定名称，不应包含凭据或配置正文。
///
/// 返回：包含操作名、结果码、错误码和原始响应消息的文本；调用方对外输出前仍需遵守诊断脱敏约束。
pub(crate) fn to_err_msg<T: GrpcResponseMessage>(response: &T, operation: &str) -> String {
    format!(
        "handle {} failed: result_code={}, error_code={}, message={}",
        operation,
        response.result_code(),
        response.error_code(),
        response.message().map(|s| s.as_str()).unwrap_or("")
    )
}
