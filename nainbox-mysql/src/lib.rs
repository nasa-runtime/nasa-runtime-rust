//! MySQL Inbox：唯一去重标记与消息业务副作用共享同一 `natx` ambient 事务。
//!
//! `claim` **拒绝事务外调用**，不会静默 autocommit。首次 INSERT 与后续业务 SQL 同提交/回滚：
//! 进程在 commit 前退出时二者都不可见；重复投递的 INSERT 受唯一键串行化，只会有一个事务得到
//! [`InboxClaim::Claimed`]。本合同只覆盖同一 MySQL datasource 内的副作用，外部 HTTP/Kafka 副作用
//! 仍需 Outbox 或目标系统幂等键。

#![forbid(unsafe_code)]

pub use nainbox_core::InboxClaim;
use natx::{TxDecision, TxRunError};

/// Inbox I/O 或合同错误。文本不包含 SQL、凭据或 payload。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxStoreError {
    /// 稳定、脱敏的错误原因。
    pub reason: String,
}

impl InboxStoreError {
    /// 业务作用：用不含 SQL、连接信息或消息内容的稳定原因构造持久层错误。
    ///
    /// 参数说明：
    /// - `reason`：允许向上游暴露的稳定失败分类。
    ///
    /// 返回：不携带底层敏感信息的 Inbox 错误。
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for InboxStoreError {
    /// 业务作用：输出脱敏后的 Inbox 持久层错误摘要。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：稳定摘要写入成功时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "inbox store error: {}", self.reason)
    }
}

impl std::error::Error for InboxStoreError {}

/// 无状态 MySQL Inbox。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlInbox;

/// 业务作用：区分首次消息已经提交业务效果与重复消息被幂等吸收。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxProcess<T> {
    /// 本事务首次取得消息并已确认提交业务处理结果。
    Applied(T),
    /// 既有事务已经提交同一消息，本轮没有再次执行业务处理函数。
    Duplicate,
}

/// 业务作用：保留 Inbox 事务基础设施的封闭失败阶段，供 transport 决定是否允许消耗重试预算。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxTransactionError {
    /// 内层错误把事务标记为只能回滚。
    RollbackOnly,
    /// 数据库明确拒绝提交，原消息不得确认。
    CommitRejected,
    /// 提交请求结果不确定，必须无界重投并依赖 Inbox 吸收可能的重复。
    CommitUncertain,
    /// 物理回滚失败，不能声称本轮没有副作用。
    RollbackFailed,
    /// 事务开始或所有权基础设施失败。
    Infrastructure,
}

impl InboxTransactionError {
    /// 业务作用：判断失败是否禁止被普通有限重试预算转入死信。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：提交拒绝/不确定或回滚失败返回真；其余已确认未提交阶段返回假。
    pub fn requires_unbounded_redelivery(self) -> bool {
        matches!(
            self,
            Self::CommitRejected | Self::CommitUncertain | Self::RollbackFailed
        )
    }
}

impl std::fmt::Display for InboxTransactionError {
    /// 业务作用：输出不含 SQL、连接信息、消息身份或业务正文的稳定事务阶段。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：稳定摘要写入成功时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RollbackOnly => "Inbox transaction rollback-only",
            Self::CommitRejected => "Inbox transaction commit rejected",
            Self::CommitUncertain => "Inbox transaction commit uncertain",
            Self::RollbackFailed => "Inbox transaction rollback failed",
            Self::Infrastructure => "Inbox transaction infrastructure failed",
        })
    }
}

impl std::error::Error for InboxTransactionError {}

impl MySqlInbox {
    /// 业务作用：创建无状态 Inbox 入口，不提前建立连接或持有消息身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可在任意事务调用栈内复用的轻量句柄。
    pub fn new() -> Self {
        Self
    }

    /// 业务作用：为本地自举创建 Inbox 表；生产结构仍由 migration 拥有。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：表已经存在或创建成功时完成；连接与数据库错误返回脱敏失败。
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

    /// 业务作用：在当前 ambient 事务内竞争消息唯一标记，保证业务副作用与去重事实同提交。
    ///
    /// 返回 `Claimed` 后，调用方必须在**同一 `natx::run`/`#[transactional]` 调用栈**内完成业务 SQL；
    /// 返回 `Duplicate` 时必须跳过副作用并正常确认消息。
    ///
    /// 参数说明：
    /// - `consumer_name`：跨副本与重启保持稳定的消费命名空间。
    /// - `message_id`：transport 提供的稳定消息身份。
    ///
    /// 返回：首次占用返回 `Claimed`，既有提交返回 `Duplicate`；事务缺失、键非法或数据库失败时返回错误。
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

    /// 业务作用：统一执行 `Inbox claim → 业务处理 → COMMIT`，让消息入口不再重复手写事务模板。
    ///
    /// 处理函数只在首次 claim 时调用，并且与唯一标记共享默认 datasource 的同一事务。返回
    /// `Applied` 或 `Duplicate` 都表示数据库已经明确确认提交，transport 才能据此 ACK；任何错误都
    /// 必须保留原消息。
    ///
    /// 参数说明：
    /// - `consumer_name`：稳定消费命名空间，不得随副本或重启变化。
    /// - `message_id`：transport 提供的稳定消息身份。
    /// - `handle`：只包含本地数据库业务副作用的异步处理函数。
    ///
    /// 返回：首次处理提交后返回 `Applied`；重复消息返回 `Duplicate`；业务回滚保留原错误，事务基础设施
    /// 失败返回可向下转型的 [`InboxTransactionError`]。
    pub async fn process<F, Fut, T>(
        &self,
        consumer_name: &str,
        message_id: &str,
        handle: F,
    ) -> anyhow::Result<InboxProcess<T>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        natx::run_decided(async move {
            let claim = match self.claim(consumer_name, message_id).await {
                Ok(claim) => claim,
                Err(error) => return TxDecision::Rollback(anyhow::Error::new(error)),
            };
            if !claim.should_process() {
                return TxDecision::Commit(InboxProcess::Duplicate);
            }
            match handle().await {
                Ok(value) => TxDecision::Commit(InboxProcess::Applied(value)),
                Err(error) => TxDecision::Rollback(error),
            }
        })
        .await
        .map_err(map_transaction_error)
    }
}

/// 业务作用：把 `natx` 封闭事务阶段映射为 Inbox 对外分类，同时保留业务回滚原始错误。
///
/// 参数说明：
/// - `error`：事务内核返回的业务或基础设施失败。
///
/// 返回：业务回滚返回原错误；其它阶段返回可供 transport 精确分类的稳定错误类型。
fn map_transaction_error(error: TxRunError<anyhow::Error>) -> anyhow::Error {
    match error {
        TxRunError::Rollback(error) => error,
        TxRunError::RollbackOnly { .. } => InboxTransactionError::RollbackOnly.into(),
        TxRunError::CommitRejected { .. } => InboxTransactionError::CommitRejected.into(),
        TxRunError::CommitUncertain { .. } => InboxTransactionError::CommitUncertain.into(),
        TxRunError::RollbackFailed { .. } => InboxTransactionError::RollbackFailed.into(),
        TxRunError::Infrastructure { .. } => InboxTransactionError::Infrastructure.into(),
    }
}

/// 业务作用：校验 Inbox 复合唯一键分量的空白、长度和 NUL 边界，避免身份被截断或归一化碰撞。
///
/// 参数说明：
/// - `field`：用于稳定错误分类的字段名。
/// - `value`：待进入唯一键的原始身份。
/// - `max`：数据库列允许的最大字节长度。
///
/// 返回：身份可无损持久化时成功，否则返回脱敏合同错误。
fn validate_key(field: &'static str, value: &str, max: usize) -> Result<(), InboxStoreError> {
    if value.is_empty() || value.trim() != value || value.len() > max || value.contains('\0') {
        return Err(InboxStoreError::new(format!("invalid {field}")));
    }
    Ok(())
}

/// 业务作用：将连接获取失败收敛为不泄露 datasource 信息的稳定错误。
///
/// 参数说明：
/// - `_error`：仅用于分类、不向调用方展开的底层连接错误。
///
/// 返回：固定连接不可用错误。
fn map_connection(_error: anyhow::Error) -> InboxStoreError {
    InboxStoreError::new("connection unavailable")
}

/// 业务作用：将 SQLx 错误收敛为不泄露 SQL 与参数的数据库失败。
///
/// 参数说明：
/// - `_error`：仅用于归因、不向调用方展开的底层数据库错误。
///
/// 返回：固定数据库操作失败错误。
fn map_database(_error: sqlx::Error) -> InboxStoreError {
    InboxStoreError::new("database operation failed")
}
