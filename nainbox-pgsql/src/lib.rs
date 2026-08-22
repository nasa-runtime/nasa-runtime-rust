//! PostgreSQL Inbox：消息唯一标记与本地业务副作用共享同一 `natx-pgsql` ambient transaction。
//!
//! `claim` 在事务外明确失败。首次写入只以 `inbox_message_pkey` 为冲突目标；因此其它唯一约束、
//! 检查约束与数据库失败不会被误判为重复消息。句柄固定 default 或命名 datasource，不跨库回退。

#![forbid(unsafe_code)]

pub use nainbox_core::{
    InboxClaim, InboxProcess, InboxStore, InboxStoreError, InboxTransactionError,
};
use natx_pgsql::{TxDecision, TxRunError};

const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS inbox_message (
    consumer_name TEXT COLLATE "C" NOT NULL CHECK (octet_length(consumer_name) BETWEEN 1 AND 128),
    message_id TEXT COLLATE "C" NOT NULL CHECK (octet_length(message_id) BETWEEN 1 AND 190),
    processed_at_ms BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT)
        CHECK (processed_at_ms >= 0),
    CONSTRAINT inbox_message_pkey PRIMARY KEY (consumer_name, message_id)
)"#;

/// PostgreSQL Inbox 句柄；全部持久操作固定到构造时选择的 datasource。
#[derive(Debug, Clone)]
pub struct PgInbox {
    datasource: natx_pgsql::DatasourceRef,
}

impl Default for PgInbox {
    /// 业务作用：创建绑定默认 PostgreSQL datasource 的轻量 Inbox 句柄。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`PgInbox::new`] 相同的句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl PgInbox {
    /// 业务作用：创建绑定默认 PostgreSQL datasource 的 Inbox 入口，不提前建立连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：后续 claim、process 与 schema 操作固定使用 default datasource 的轻量句柄。
    pub fn new() -> Self {
        Self {
            datasource: natx_pgsql::DatasourceRef::default(),
        }
    }

    /// 业务作用：创建绑定命名 PostgreSQL datasource 的 Inbox 入口。
    ///
    /// 参数说明：
    /// - `datasource`：启动期已登记的稳定 datasource 名称。
    ///
    /// 返回：名称合法时返回固定绑定句柄；非法名称在网络 I/O 前失败。
    pub fn with_datasource(datasource: impl AsRef<str>) -> anyhow::Result<Self> {
        Ok(Self {
            datasource: natx_pgsql::DatasourceRef::new(datasource)?,
        })
    }

    /// 业务作用：读取该 Inbox 全部持久操作绑定的 datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不可变且不含连接信息的 qualifier 引用。
    pub fn datasource_ref(&self) -> &natx_pgsql::DatasourceRef {
        &self.datasource
    }

    /// 业务作用：为显式自举在默认 datasource 创建 Inbox 表。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：表已存在或创建成功时完成；连接与 DDL 失败返回脱敏错误。
    pub async fn ensure_schema() -> Result<(), InboxStoreError> {
        Self::ensure_schema_for(natx_pgsql::DEFAULT_DATASOURCE).await
    }

    /// 业务作用：为显式自举在命名 datasource 创建 Inbox 表。
    ///
    /// 参数说明：
    /// - `datasource`：启动期已登记的 PostgreSQL datasource 名称。
    ///
    /// 返回：表已存在或创建成功时完成；名称、连接或 DDL 失败返回脱敏错误。
    pub async fn ensure_schema_for(datasource: impl AsRef<str>) -> Result<(), InboxStoreError> {
        let datasource = natx_pgsql::DatasourceRef::new(datasource).map_err(map_connection)?;
        let mut connection = natx_pgsql::conn_for(&datasource)
            .await
            .map_err(map_connection)?;
        sqlx::raw_sql(CREATE_TABLE_SQL)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        Ok(())
    }

    /// 业务作用：在当前 PostgreSQL ambient transaction 内竞争目标消息唯一标记。
    ///
    /// 返回 `Claimed` 后，调用方必须在同源 `natx_pgsql::run_for` 或带 datasource 的
    /// `#[transactional]` 调用栈内完成业务 SQL；返回 `Duplicate` 时必须跳过业务副作用。
    ///
    /// 参数说明：
    /// - `consumer_name`：跨副本和重启稳定的消费命名空间。
    /// - `message_id`：transport 提供的稳定消息身份。
    ///
    /// 返回：首次占用为 `Claimed`，目标主键已提交为 `Duplicate`；事务缺失、键非法或其它数据库失败时返回错误。
    pub async fn claim(
        &self,
        consumer_name: &str,
        message_id: &str,
    ) -> Result<InboxClaim, InboxStoreError> {
        validate_key("consumer_name", consumer_name, 128)?;
        validate_key("message_id", message_id, 190)?;
        if !natx_pgsql::in_transaction() {
            return Err(InboxStoreError::new(
                "claim requires an ambient transaction; autocommit is forbidden",
            ));
        }

        // 只声明目标主键，确保其它唯一约束冲突继续中止事务，不能误确认原消息。
        let mut connection = natx_pgsql::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let result = sqlx::query(
            "INSERT INTO inbox_message (consumer_name, message_id) VALUES ($1, $2) \
             ON CONFLICT ON CONSTRAINT inbox_message_pkey DO NOTHING",
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

    /// 业务作用：统一执行 `Inbox claim → 业务处理 → COMMIT`，只在明确提交后开放消息确认。
    ///
    /// 处理函数只在首次 claim 时调用，并与唯一标记共享句柄绑定 datasource 的同一事务。
    ///
    /// 参数说明：
    /// - `consumer_name`：稳定消费命名空间，不得随副本或重启变化。
    /// - `message_id`：transport 提供的稳定消息身份。
    /// - `handle`：只包含本地 PostgreSQL 业务副作用的异步处理函数。
    ///
    /// 返回：首次业务提交为 `Applied`，重复消息为 `Duplicate`；业务回滚保留原错误，事务失败返回
    /// 可向下转型的 [`InboxTransactionError`]。
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
        natx_pgsql::run_decided_for(&self.datasource, async move {
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

#[async_trait::async_trait]
impl InboxStore for PgInbox {
    /// 业务作用：通过后端中立合同在当前 PostgreSQL ambient transaction 内竞争消息唯一标记。
    ///
    /// 参数说明：
    /// - `consumer_name`：跨副本和重启稳定的消费命名空间。
    /// - `message_id`：transport 提供的稳定消息身份。
    ///
    /// 返回：首次取得标记为 `Claimed`，目标主键已提交为 `Duplicate`；其它失败返回错误。
    async fn claim(
        &self,
        consumer_name: &str,
        message_id: &str,
    ) -> Result<InboxClaim, InboxStoreError> {
        PgInbox::claim(self, consumer_name, message_id).await
    }
}

/// 业务作用：把 PostgreSQL 事务内核阶段映射为 Inbox transport 可穷举的提交分类。
///
/// 参数说明：
/// - `error`：事务内核返回的业务或基础设施失败。
///
/// 返回：业务回滚保留原错误；其它阶段返回后端中立的 [`InboxTransactionError`]。
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

/// 业务作用：校验 Inbox 复合主键分量的空白、字节长度和 NUL 边界，防止身份改变后仍被接收。
///
/// 参数说明：
/// - `field`：用于稳定错误分类的字段名。
/// - `value`：待进入主键的原始身份。
/// - `max`：MySQL/PostgreSQL 公共合同允许的最大 UTF-8 字节长度。
///
/// 返回：身份可无损持久化时成功；空值、首尾空白、超长或 NUL 返回脱敏错误。
fn validate_key(field: &'static str, value: &str, max: usize) -> Result<(), InboxStoreError> {
    if value.is_empty() || value.trim() != value || value.len() > max || value.contains('\0') {
        return Err(InboxStoreError::new(format!("invalid {field}")));
    }
    Ok(())
}

/// 业务作用：将连接与 datasource 选择失败收敛为不泄露 endpoint 的稳定错误。
///
/// 参数说明：
/// - `_error`：仅触发分类、不进入公开文本的底层错误。
///
/// 返回：固定连接不可用错误。
fn map_connection(_error: anyhow::Error) -> InboxStoreError {
    InboxStoreError::new("connection unavailable")
}

/// 业务作用：将 SQLx 错误收敛为不泄露 SQL、约束参数或消息身份的数据库失败。
///
/// 参数说明：
/// - `_error`：仅触发分类、不进入公开文本的底层数据库错误。
///
/// 返回：固定数据库操作失败错误。
fn map_database(_error: sqlx::Error) -> InboxStoreError {
    InboxStoreError::new("database operation failed")
}
