//! MySQL Inbox：唯一去重标记与消息业务副作用共享同一 `natx` ambient 事务。
//!
//! `claim` **拒绝事务外调用**，不会静默 autocommit。首次 INSERT 与后续业务 SQL 同提交/回滚：
//! 进程在 commit 前退出时二者都不可见；重复投递的 INSERT 受唯一键串行化，只会有一个事务得到
//! [`InboxClaim::Claimed`]。本合同只覆盖同一 MySQL datasource 内的副作用，外部 HTTP/Kafka 副作用
//! 仍需 Outbox 或目标系统幂等键。

#![forbid(unsafe_code)]

pub use nainbox_core::InboxClaim;

/// Inbox I/O 或合同错误。文本不包含 SQL、凭据或 payload。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxStoreError {
    /// 稳定、脱敏的错误原因。
    pub reason: String,
}

impl InboxStoreError {
    /// 用不含 SQL、连接信息或消息内容的稳定原因构造错误。
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for InboxStoreError {
    /// 输出脱敏后的 Inbox 持久层错误摘要。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "inbox store error: {}", self.reason)
    }
}

impl std::error::Error for InboxStoreError {}

/// 无状态 MySQL Inbox。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlInbox;

impl MySqlInbox {
    /// 创建 store，不建连。
    pub fn new() -> Self {
        Self
    }

    /// 创建演示 schema。生产环境应由 migration 拥有。
    pub async fn ensure_schema() -> Result<(), InboxStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inbox_message ( \
             consumer_name VARCHAR(128) NOT NULL, \
             message_id VARCHAR(190) NOT NULL, \
             processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
             PRIMARY KEY (consumer_name, message_id) \
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        )
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(())
    }

    /// 在当前 ambient 事务内竞争消息唯一标记。
    ///
    /// 返回 `Claimed` 后，调用方必须在**同一 `natx::run`/`#[transactional]` 调用栈**内完成业务 SQL；
    /// 返回 `Duplicate` 时必须跳过副作用并正常确认消息。
    pub async fn claim(
        &self,
        consumer_name: &str,
        message_id: &str,
    ) -> Result<InboxClaim, InboxStoreError> {
        validate_key("consumer_name", consumer_name, 128)?;
        validate_key("message_id", message_id, 190)?;
        if !natx::in_transaction() {
            return Err(InboxStoreError::new(
                "claim requires an ambient transaction; autocommit is forbidden",
            ));
        }
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let result = sqlx::query(
            "INSERT IGNORE INTO inbox_message (consumer_name, message_id) VALUES (?, ?)",
        )
        .bind(consumer_name)
        .bind(message_id)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(if result.rows_affected() == 1 {
            InboxClaim::Claimed
        } else {
            InboxClaim::Duplicate
        })
    }
}

/// 校验 Inbox 复合唯一键分量的空白、长度和 NUL 边界。
fn validate_key(field: &'static str, value: &str, max: usize) -> Result<(), InboxStoreError> {
    if value.is_empty() || value.trim() != value || value.len() > max || value.contains('\0') {
        return Err(InboxStoreError::new(format!("invalid {field}")));
    }
    Ok(())
}

/// 将连接获取失败收敛为不泄露 datasource 信息的稳定错误。
fn map_connection(_error: anyhow::Error) -> InboxStoreError {
    InboxStoreError::new("connection unavailable")
}

/// 将 SQLx 错误收敛为不泄露 SQL 与参数的数据库失败。
fn map_database(_error: sqlx::Error) -> InboxStoreError {
    InboxStoreError::new("database operation failed")
}
