//! PostgreSQL SQLx 结果在类型擦除前转成后端中立分类。

use namapper_core::observability::{CallOutcome, DbOutcome};

/// 业务作用：分类实际数据库结果，不使缺失可选行变成数据库错误。
/// 参数说明：`result` 为 SQLx 原始结果。
/// 返回：成功或固定错误分类，不修改返回值。
pub fn classify_sqlx_result<T>(result: &Result<T, sqlx::Error>) -> DbOutcome {
    match result {
        Ok(_) => DbOutcome::Ok,
        Err(error) => classify_error(error),
    }
}

/// 业务作用：收口 Mapper 逻辑结果，保持数据库与缓存失败的独立计量。
/// 参数说明：`result` 为逻辑方法的最终结果。
/// 返回：未找到、成功或错误；取消由守卫记录。
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

/// 业务作用：只根据 SQLx 变体和结构化约束类别判断数据库失败。
/// 参数说明：`error` 为未擦除的底层错误。
/// 返回：与 MySQL 一致的固定词表，不包含服务端错误原文。
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
