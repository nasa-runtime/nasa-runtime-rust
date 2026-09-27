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
);
CREATE INDEX IF NOT EXISTS inbox_message_retention_idx
    ON inbox_message (consumer_name, processed_at_ms)
"#;

/// PostgreSQL Inbox 句柄；全部持久操作固定到构造时选择的 datasource。
#[derive(Debug, Clone)]
pub struct PgInbox {
    datasource: natx_pgsql::DatasourceRef,
}

/// 保留轮次独占的物理会话守卫；只有明确确认未持锁或已解除锁后才允许连接回池。
struct RetentionSession {
    connection: natx_pgsql::PgConn,
    reusable: bool,
}

impl RetentionSession {
    /// 业务作用：接管保留轮次连接，并在首次锁命令发出前默认把会话视为不可复用。
    ///
    /// 参数说明：
    /// - `connection`：轮次独占的非事务池连接。
    ///
    /// 返回：取消、超时或未知结果路径会在析构时关闭物理会话的守卫。
    fn new(connection: natx_pgsql::PgConn) -> Self {
        Self {
            connection,
            reusable: false,
        }
    }

    /// 业务作用：取得守卫持有的 PostgreSQL 会话，保证锁与全部清理语句使用同一物理连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前轮次独占的底层连接引用。
    fn connection(&mut self) -> &mut sqlx::PgConnection {
        self.connection.as_mut()
    }

    /// 业务作用：在服务端明确确认未持有会话锁后允许连接正常归还连接池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；调用后析构不再关闭物理会话。
    fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Drop for RetentionSession {
    /// 业务作用：在取消、超时或锁结果不确定时关闭物理会话，避免可能仍持锁的连接回到池中。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；关闭动作由 SQLx 在池连接析构时执行。
    fn drop(&mut self) {
        if !self.reusable {
            let _ = self.connection.close_on_drop();
        }
    }
}

/// 保留主体的有界结束方式；预算中断意味着当前会话协议状态不可复用。
enum RetentionRoundEnd {
    /// 候选已经清空，连接仍可尝试显式解除会话锁。
    Completed(nainbox_core::InboxRetentionRoundReport),
    /// 数据库步骤被墙钟预算截断，必须关闭会话释放锁。
    BudgetAbandon(nainbox_core::InboxRetentionRoundReport),
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
        Self::ensure_schema_on_connection(&mut connection).await
    }

    /// 业务作用：复用调用方已持有的 PostgreSQL 连接创建 Inbox 表，使外层 schema
    /// 互斥权覆盖整个 DDL 操作。
    ///
    /// 参数说明：`connection` 是调用方已取得 schema 互斥权的连接。
    ///
    /// 返回：表已存在或创建成功时完成；DDL 失败返回脱敏错误。
    pub async fn ensure_schema_on_connection(
        connection: &mut natx_pgsql::PgConn,
    ) -> Result<(), InboxStoreError> {
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

#[async_trait::async_trait]
impl nainbox_core::DurableInboxRetention for PgInbox {
    /// 业务作用：对单个消费命名空间执行一轮 owner 互斥的过期去重标记清理。
    ///
    /// 参数说明：
    /// - `consumer_name`：目标消费命名空间；advisory lock 按该值隔离清理者。
    /// - `policy`：保留策略；实现先复验再执行，拒绝把非法窗口带进删除语句。
    ///
    /// 返回：本轮账目报告；owner 竞争失败以 `claim_contended` 返回而不是并发删除。
    async fn retention_round(
        &self,
        consumer_name: &str,
        policy: &nainbox_core::InboxRetentionPolicy,
    ) -> Result<nainbox_core::InboxRetentionRoundReport, InboxStoreError> {
        validate_key("consumer_name", consumer_name, 128)?;
        policy.validate().map_err(|reason| {
            InboxStoreError::new(format!("invalid retention policy: {reason}"))
        })?;
        let mut report = nainbox_core::InboxRetentionRoundReport::default();
        // 墙钟预算覆盖取连接、竞争锁、删除、统计与解除锁；任何被截断的会话都关闭而不回池。
        let deadline = std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(
                policy.round_time_budget_ms as u64,
            ))
            .ok_or_else(|| InboxStoreError::new("retention time budget is out of range"))?;
        // 清理轮次需要逐批 autocommit 并拥有完整 session 锁生命周期；环境事务内执行会让
        // 删除报告早于外层提交且提前释放 owner 锁，因此在任何 driver 事务中都必须拒绝。
        let connection = match tokio::time::timeout(
            deadline.saturating_duration_since(std::time::Instant::now()),
            natx_pgsql::never_conn_for(&self.datasource),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => return Err(map_connection(error)),
            Err(_) => {
                report.budget_exhausted = true;
                return Ok(report);
            }
        };
        // 首次锁命令的结果也可能因取消或超时未知；先武装守卫，只有明确未取锁或已释放才回池。
        let mut session = RetentionSession::new(connection);
        // owner 互斥使用会话级 advisory lock；把实际 database、当前 schema 与消费命名空间共同
        // 哈希，让 datasource alias 仍保护同一物理表，同时隔离同库不同 schema 的独立 Inbox 表。
        // 非阻塞尝试——竞争失败按合同报告 contended，由下一轮重试，不排队叠加清理者。
        let locked: Option<bool> = match tokio::time::timeout(
            deadline.saturating_duration_since(std::time::Instant::now()),
            sqlx::query_scalar(
                "SELECT pg_try_advisory_lock(hashtextextended(\
                 'nainbox:retention:' || current_database() || ':' || current_schema() || ':' || $1, 0))",
            )
            .bind(consumer_name)
            .fetch_one(session.connection()),
        )
        .await
        {
            Ok(Ok(locked)) => locked,
            Ok(Err(error)) => return Err(map_database(error)),
            Err(_) => {
                report.budget_exhausted = true;
                return Ok(report);
            }
        };
        match locked {
            Some(true) => {}
            Some(false) => {
                session.mark_reusable();
                report.claim_contended = true;
                return Ok(report);
            }
            None => {
                return Err(InboxStoreError::new(
                    "advisory lock returned an invalid result",
                ));
            }
        }
        // 锁与删除必须同连接:advisory lock 是会话级,换连接等于无锁。
        let round =
            retention_round_locked(session.connection(), consumer_name, policy, deadline).await;
        let mut report = match round {
            Ok(RetentionRoundEnd::Completed(report)) => report,
            Ok(RetentionRoundEnd::BudgetAbandon(report)) => return Ok(report),
            Err(error) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if !remaining.is_zero() {
                    let released = tokio::time::timeout(
                        remaining,
                        sqlx::query_scalar::<_, bool>(
                            "SELECT pg_advisory_unlock(hashtextextended(\
                             'nainbox:retention:' || current_database() || ':' || current_schema() || ':' || $1, 0))",
                        )
                        .bind(consumer_name)
                        .fetch_one(session.connection()),
                    )
                    .await;
                    if matches!(released, Ok(Ok(true))) {
                        session.mark_reusable();
                    }
                }
                return Err(error);
            }
        };
        // 会话级锁只随会话终结自动释放;连接回到连接池后会话仍然存活,归还失败会让后续轮次
        // 持续 contended,因此失败必须留下告警证据(连接已断的场景锁随会话消亡,无需补救)。
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let released = tokio::time::timeout(
            remaining,
            sqlx::query_scalar::<_, bool>(
                "SELECT pg_advisory_unlock(hashtextextended(\
                 'nainbox:retention:' || current_database() || ':' || current_schema() || ':' || $1, 0))",
            )
            .bind(consumer_name)
            .fetch_one(session.connection()),
        )
        .await;
        if matches!(released, Ok(Ok(true))) {
            session.mark_reusable();
        } else {
            if released.is_err() {
                report.budget_exhausted = true;
            }
            // 未确认解除会话锁时守卫关闭物理 session，可靠释放可能仍持有的锁。
            tracing::warn!("inbox retention advisory lock release was not confirmed");
        }
        Ok(report)
    }
}

/// 业务作用：在已持有 advisory lock 的连接上执行分批删除与账目统计。
///
/// 参数说明：
/// - `connection`：持有会话级 advisory lock 的同一 PostgreSQL 连接。
/// - `consumer_name`：目标消费命名空间。
/// - `policy`：已复验的保留策略。
///
/// 返回：本轮账目报告；删除或统计失败返回脱敏错误。
async fn retention_round_locked(
    connection: &mut sqlx::PgConnection,
    consumer_name: &str,
    policy: &nainbox_core::InboxRetentionPolicy,
    deadline: std::time::Instant,
) -> Result<RetentionRoundEnd, InboxStoreError> {
    let mut report = nainbox_core::InboxRetentionRoundReport::default();
    // 每个数据库步骤只使用本轮剩余预算；被截断后会话状态未知，由外层守卫关闭物理连接。
    macro_rules! bounded_step {
        ($future:expr) => {{
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                report.budget_exhausted = true;
                return Ok(RetentionRoundEnd::BudgetAbandon(report));
            }
            match tokio::time::timeout(remaining, $future).await {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => return Err(map_database(error)),
                Err(_) => {
                    report.budget_exhausted = true;
                    return Ok(RetentionRoundEnd::BudgetAbandon(report));
                }
            }
        }};
    }
    // cutoff 取整轮开始时刻:轮内固定不动,长轮次的后批不得吃进开轮时尚未到龄的行,
    // 否则"删除窗口不小于重投视界"的安全证明被批间时间推进悄悄削弱。
    let cutoff_ms: i64 = bounded_step!(sqlx::query_scalar(
        "SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - $1",
    )
    .bind(policy.processed_min_age_ms)
    .fetch_one(&mut *connection));
    loop {
        // PostgreSQL 的 DELETE 无 LIMIT，经 ctid 子查询沿保留索引取最老的有界批，
        // 避免大表每轮扫描全部 consumer 历史，也使有限预算优先回收最陈旧标记。
        let deleted = bounded_step!(sqlx::query(
            "DELETE FROM inbox_message WHERE ctid IN ( \
             SELECT ctid FROM inbox_message \
             WHERE consumer_name = $1 AND processed_at_ms < $2 \
             ORDER BY processed_at_ms LIMIT $3)",
        )
        .bind(consumer_name)
        .bind(cutoff_ms)
        .bind(i64::from(policy.batch_limit))
        .execute(&mut *connection))
        .rows_affected();
        report.deleted = report.deleted.saturating_add(deleted);
        if deleted < u64::from(policy.batch_limit) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            report.budget_exhausted = true;
            return Ok(RetentionRoundEnd::BudgetAbandon(report));
        }
    }
    // 最老候选年龄进入观测:该值持续增长而 deleted 为零说明清理被卡,是保留面板的核心信号。
    let oldest_ms: Option<i64> = bounded_step!(sqlx::query_scalar(
        "SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - MIN(processed_at_ms) \
         FROM inbox_message WHERE consumer_name = $1",
    )
    .bind(consumer_name)
    .fetch_one(&mut *connection));
    report.oldest_candidate_age_ms = oldest_ms;
    Ok(RetentionRoundEnd::Completed(report))
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
