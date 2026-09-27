//! 在 SQLx 类型擦除之前分类数据库结果，不读取错误正文或约束名称。

use namapper_core::observability::{CallOutcome, DbOutcome};

/// 业务作用：将 SQLx 结果映射为跨后端固定数据库分类。
/// 参数说明：`result` 为尚未转换成 anyhow 的执行结果。
/// 返回：成功或稳定错误分类，不改变结果所有权。
pub fn classify_sqlx_result<T>(result: &Result<T, sqlx::Error>) -> DbOutcome {
    match result {
        Ok(_) => DbOutcome::Ok,
        Err(error) => classify_error(error),
    }
}

/// 业务作用：识别逻辑调用的未找到与其它失败，保持缓存后续失败独立于数据库成功。
/// 参数说明：`result` 为 Mapper 最终返回值。
/// 返回：方法完成分类；取消和展开由 guard 负责。
pub fn classify_method_result<T>(result: &anyhow::Result<T>) -> CallOutcome {
    match result {
        Ok(_) => CallOutcome::Ok,
        Err(error)
            if matches!(
                error.downcast_ref::<sqlx::Error>(),
                Some(sqlx::Error::RowNotFound)
            ) =>
        {
            CallOutcome::NotFound
        }
        Err(_) => CallOutcome::Error,
    }
}

/// 业务作用：从结构化 SQLx 变体及数据库错误种类提取低基数分类。
/// 参数说明：`error` 是原始 SQLx 错误，正文不参与判定。
/// 返回：未知变体进入 other，数据库码另由受控日志提取。
pub(crate) fn classify_error(error: &sqlx::Error) -> DbOutcome {
    use sqlx::Error::*;
    match error {
        Configuration(_) => DbOutcome::Configuration,
        InvalidArgument(_) => DbOutcome::InvalidArgument,
        Database(error) => match error.kind() {
            sqlx::error::ErrorKind::UniqueViolation
            | sqlx::error::ErrorKind::ForeignKeyViolation
            | sqlx::error::ErrorKind::NotNullViolation
            | sqlx::error::ErrorKind::CheckViolation => DbOutcome::Constraint,
            _ => DbOutcome::Database,
        },
        Io(_) => DbOutcome::Io,
        Tls(_) => DbOutcome::Tls,
        Protocol(_) => DbOutcome::Protocol,
        RowNotFound => DbOutcome::NotFound,
        TypeNotFound { .. } | ColumnIndexOutOfBounds { .. } | ColumnNotFound(_) => {
            DbOutcome::Schema
        }
        ColumnDecode { .. } | Decode(_) => DbOutcome::Decode,
        Encode(_) => DbOutcome::Encode,
        AnyDriverError(_) => DbOutcome::Driver,
        _ => DbOutcome::Other,
    }
}
