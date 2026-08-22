//! MySQL Inbox：唯一去重标记与消息业务副作用共享同一 `natx` ambient 事务。
//!
//! `claim` **拒绝事务外调用**，不会静默 autocommit。首次 INSERT 与后续业务 SQL 同提交/回滚：
//! 进程在 commit 前退出时二者都不可见；重复投递的 INSERT 受唯一键串行化，只会有一个事务得到
//! [`InboxClaim::Claimed`]。本合同只覆盖同一 MySQL datasource 内的副作用，外部 HTTP/Kafka 副作用
//! 仍需 Outbox 或目标系统幂等键。

#![forbid(unsafe_code)]

pub use nainbox_core::{
    InboxClaim, InboxProcess, InboxStore, InboxStoreError, InboxTransactionError,
};
use natx::{TxDecision, TxRunError};

/// 业务作用：以不可变 datasource 身份统一 Inbox claim 与本地业务事务。
#[derive(Debug, Clone)]
pub struct MySqlInbox {
    datasource: natx::DatasourceRef,
}

impl Default for MySqlInbox {
    /// 业务作用：以兼容语义构造绑定默认 datasource 的 Inbox。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`MySqlInbox::new`] 相同的轻量句柄。
    fn default() -> Self {
        Self::new()
    }
}

impl MySqlInbox {
    /// 业务作用：创建绑定默认 datasource 的轻量 Inbox 入口，不提前建立连接或持有消息身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可在任意事务调用栈内复用的轻量句柄。
    pub fn new() -> Self {
        Self {
            datasource: natx::DatasourceRef::default(),
        }
    }

    /// 业务作用：创建绑定命名 datasource 的 Inbox 入口。
    ///
    /// 参数说明：`datasource` 为启动期已注册的数据源名称。
    ///
    /// 返回：claim、process 和 schema 操作固定使用该 datasource；名称非法时在 I/O 前失败。
    pub fn with_datasource(datasource: impl AsRef<str>) -> anyhow::Result<Self> {
        Ok(Self {
            datasource: natx::DatasourceRef::new(datasource)?,
        })
    }

    /// 业务作用：读取该 Inbox 全部持久操作绑定的 datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不可变、不含连接信息的 qualifier 引用。
    pub fn datasource_ref(&self) -> &natx::DatasourceRef {
        &self.datasource
    }

    /// 业务作用：为本地自举创建 Inbox 表；生产结构仍由 migration 拥有。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：表已经存在或创建成功时完成；连接与数据库错误返回脱敏失败。
    pub async fn ensure_schema() -> Result<(), InboxStoreError> {
        Self::ensure_schema_for(natx::DEFAULT_DATASOURCE).await
    }

    /// 业务作用：在指定 datasource 上创建 Inbox 表。
    ///
    /// 参数说明：`datasource` 是启动期已注册的数据源名称。
    ///
    /// 返回：表已存在或创建成功时完成；名称、连接或 DDL 失败时返回脱敏错误。
    pub async fn ensure_schema_for(datasource: impl AsRef<str>) -> Result<(), InboxStoreError> {
        let datasource = natx::DatasourceRef::new(datasource).map_err(map_connection)?;
        let mut connection = natx::conn_for(&datasource).await.map_err(map_connection)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inbox_message ( \
             consumer_name VARCHAR(128) NOT NULL, message_id VARCHAR(190) NOT NULL, \
             processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
             PRIMARY KEY (consumer_name, message_id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        )
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(())
    }

    /// 业务作用：在当前 ambient 事务内竞争消息唯一标记，保证业务副作用与去重事实同提交。
    ///
    /// 返回 `Claimed` 后，调用方必须在**同源 `natx::run_for`/带 datasource 的
    /// `#[transactional]` 调用栈**内完成业务 SQL；
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
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        match sqlx::query("INSERT INTO inbox_message (consumer_name, message_id) VALUES (?, ?)")
            .bind(consumer_name)
            .bind(message_id)
            .execute(connection.as_mut())
            .await
        {
            Ok(_) => Ok(InboxClaim::Claimed),
            Err(error) if is_target_primary_conflict(&error) => Ok(InboxClaim::Duplicate),
            Err(error) => Err(map_database(error)),
        }
    }

    /// 业务作用：统一执行 `Inbox claim → 业务处理 → COMMIT`，让消息入口不再重复手写事务模板。
    ///
    /// 处理函数只在首次 claim 时调用，并且与唯一标记共享句柄绑定 datasource 的同一事务。返回
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
        natx::run_decided_for(&self.datasource, async move {
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
impl InboxStore for MySqlInbox {
    /// 业务作用：通过后端中立合同在当前 MySQL ambient transaction 内竞争消息唯一标记。
    ///
    /// 参数说明：
    /// - `consumer_name`：跨副本和重启稳定的消费命名空间。
    /// - `message_id`：transport 提供的稳定消息身份。
    ///
    /// 返回：首次取得标记为 `Claimed`，既有已提交标记为 `Duplicate`；其它失败返回错误。
    async fn claim(
        &self,
        consumer_name: &str,
        message_id: &str,
    ) -> Result<InboxClaim, InboxStoreError> {
        MySqlInbox::claim(self, consumer_name, message_id).await
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

/// 业务作用：只把 `inbox_message` 的复合主键冲突识别为重复消息，拒绝吞掉其它唯一约束失败。
///
/// 参数说明：
/// - `error`：MySQL INSERT 返回的数据库错误。
///
/// 返回：错误号为重复键且服务端报告的索引名为 `PRIMARY` 时返回 `true`；无法结构化确认时返回 `false`。
fn is_target_primary_conflict(error: &sqlx::Error) -> bool {
    let Some(database) = error.as_database_error() else {
        return false;
    };
    let Some(mysql) = database.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() else {
        return false;
    };
    if mysql.number() != 1062 {
        return false;
    }
    mysql
        .message()
        .rsplit_once(" for key ")
        .map(|(_, key)| {
            key.trim_end_matches('.')
                .trim_matches(['\'', '`'])
                .rsplit('.')
                .next()
                == Some("PRIMARY")
        })
        .unwrap_or(false)
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
