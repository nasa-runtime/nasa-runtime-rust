//! 结构化 broker 错误证据的内部适配，不按远端错误文本推断类别。

use rdkafka::error::RDKafkaErrorCode;

/// 业务作用：读取完整原生错误分类，包括独立的 Admin 请求事件。
/// 参数说明：error 为当前原生调用的错误。返回：存在固定错误码时保留该码，不从自由文本推断类别。
pub(crate) fn code(error: &rdkafka::error::KafkaError) -> Option<RDKafkaErrorCode> {
    match error {
        // Admin 请求失败携带独立的错误码，必须显式读取以区分明确拒绝与修改结果未知。
        rdkafka::error::KafkaError::AdminOp(code) => Some(*code),
        _ => error.rdkafka_error_code(),
    }
}

/// 业务作用：识别 broker 已明确拒绝的资源权限，防止把它作为瞬态故障自动恢复。
/// 参数说明：code 为原生协议错误码。返回：明确授权拒绝时 true，连接中断与认证失败不冒充 ACL 结论。
pub(crate) fn authorization(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
            | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
    )
}
