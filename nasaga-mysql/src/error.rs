//! Saga store 的脱敏错误类型与底层错误收敛。
//!
//! 错误文本是可能进入日志与告警的信息面：这里统一收敛为稳定、不含 SQL/凭据/payload
//! 的原因短语，业务身份与状态值一律不回显，防止把 business key、命令载荷等敏感内容
//! 经由错误链泄漏到低权级的观测系统。

/// Saga store I/O 或合同错误。文本不包含 SQL、凭据、业务键或 payload。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaStoreError {
    /// 稳定、脱敏的错误原因。
    pub reason: String,
}

impl SagaStoreError {
    /// 业务作用：用不含 SQL、连接信息或业务内容的稳定原因构造错误。
    ///
    /// 参数说明：
    /// - `reason`: 稳定原因短语；调用方负责保证其中不携带敏感值。
    ///
    /// 返回：可直接向上传播的脱敏错误。
    pub(crate) fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for SagaStoreError {
    /// 业务作用：输出脱敏后的 Saga 持久层错误摘要，供日志与告警使用。
    ///
    /// 参数说明：
    /// - `formatter`: 标准库格式化器。
    ///
    /// 返回：格式化成功返回 `Ok`；写入失败时透传格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "saga store error: {}", self.reason)
    }
}

impl std::error::Error for SagaStoreError {}

/// 业务作用：将连接获取失败收敛为不泄露 datasource 信息的稳定错误。
///
/// 参数说明：
/// - `_error`: 底层连接错误，仅用于类型收敛，内容不回显。
///
/// 返回：稳定的连接不可用错误。
pub(crate) fn map_connection(_error: anyhow::Error) -> SagaStoreError {
    SagaStoreError::new("connection unavailable")
}

/// 业务作用：将 SQLx 错误收敛为不泄露 SQL 与参数的数据库失败。
///
/// 参数说明：
/// - `_error`: 底层数据库错误，仅用于类型收敛，内容不回显。
///
/// 返回：稳定的数据库操作失败错误。
pub(crate) fn map_database(_error: sqlx::Error) -> SagaStoreError {
    SagaStoreError::new("database operation failed")
}

/// 业务作用：识别唯一键冲突，使幂等入口能把"重复"与"真实故障"区分开。
///
/// 创建幂等、attempt 去重与 timer 去重都依赖唯一键作为最终仲裁；不识别冲突类别
/// 就只能把合法的重复请求当成故障向上抛，破坏 at-least-once 重投的可吸收性。
///
/// 参数说明：
/// - `error`: 待判定的 SQLx 错误。
///
/// 返回：是唯一键冲突时返回真。
pub(crate) fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if db.is_unique_violation())
}

/// 业务作用：把持久化列解析失败收敛为标记列名（不含列值）的数据损坏错误。
///
/// 列值可能包含业务身份或攻击者构造的内容，因此只回显列名定位问题。
///
/// 参数说明：
/// - `column`: 解析失败的列名。
///
/// 返回：稳定的数据损坏错误。
pub(crate) fn corrupt(column: &'static str) -> SagaStoreError {
    SagaStoreError::new(format!("corrupt persisted value in column `{column}`"))
}
