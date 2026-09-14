//! 公开 gRPC start 的原始正文与 schema 边界。

use super::nasaga_runtime;

/// 业务作用：在领域摘要与数据库创建前保留 gRPC 业务正文的精确字节与解释合同。
///
/// 参数说明：input 是可选业务正文，空 content_type 按既有 JSON 默认解释。
///
/// 返回：合法正文直接映射到领域值；非法媒体类型、schema 或 JSON 返回 INVALID_ARGUMENT。
pub(super) fn decode_grpc_start_payload(
    input: Option<nasaga_runtime::orchestrator_proto::SagaPayload>,
) -> Result<Option<nasaga_runtime::SagaPayload>, nagrpc::Status> {
    input
        .map(|input| {
            nasaga_runtime::SagaPayload::new(
                if input.content_type.is_empty() {
                    "application/json".to_owned()
                } else {
                    input.content_type
                },
                input.schema_id,
                input.body,
            )
            .map_err(|_| nagrpc::Status::invalid_argument("Saga input contract is invalid"))
        })
        .transpose()
}
