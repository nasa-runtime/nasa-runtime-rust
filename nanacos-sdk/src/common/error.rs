use crate::api::error::Error;
use crate::api::error::Result;
use crate::common::remote::grpc::message::GrpcResponseMessage;

/// Handles a standard gRPC response and converts it to a Result.
/// Returns Ok(()) if the response is successful, otherwise returns an Err with the error details.
pub(crate) fn handle_response<T: GrpcResponseMessage>(response: &T, operation: &str) -> Result<()> {
    if response.is_success() {
        Ok(())
    } else {
        Err(Error::ErrResult(to_err_msg(response, operation)))
    }
}

pub(crate) fn to_err_msg<T: GrpcResponseMessage>(response: &T, operation: &str) -> String {
    format!(
        "handle {} failed: result_code={}, error_code={}, message={}",
        operation,
        response.result_code(),
        response.error_code(),
        response.message().map(|s| s.as_str()).unwrap_or("")
    )
}
