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

/// 保留轮次独占的物理会话守卫；只有明确确认未持锁或已解除锁后才允许连接回池。
struct RetentionSession {
    connection: natx::Conn,
    reusable: bool,
}

impl RetentionSession {
    /// 业务作用：接管保留轮次连接，并在首次锁命令发出前默认把会话视为不可复用。
    ///
    /// 参数说明：
    /// - `connection`：轮次独占的非事务池连接。
    ///
    /// 返回：取消、超时或未知结果路径会在析构时关闭物理会话的守卫。
    fn new(connection: natx::Conn) -> Self {
        Self {
            connection,
            reusable: false,
        }
    }

    /// 业务作用：取得守卫持有的 MySQL 会话，保证锁与全部清理语句使用同一物理连接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前轮次独占的底层连接引用。
    fn connection(&mut self) -> &mut sqlx::MySqlConnection {
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
        Self::ensure_schema_on_connection(&mut connection).await
    }

    /// 业务作用：复用调用方已持有的 MySQL 连接建立 Inbox 结构，使外层 schema 互斥权
    /// 覆盖建表、索引收敛和最终判定。
    ///
    /// 参数说明：`connection` 是调用方已取得 schema 互斥权的连接。
    ///
    /// 返回：表和保留索引可用时完成；DDL 或结构冲突返回脱敏错误。
    pub async fn ensure_schema_on_connection(
        connection: &mut natx::Conn,
    ) -> Result<(), InboxStoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS inbox_message ( \
             consumer_name VARCHAR(128) NOT NULL, message_id VARCHAR(190) NOT NULL, \
             processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
             PRIMARY KEY (consumer_name, message_id), \
             KEY inbox_message_retention_idx (consumer_name, processed_at) \
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        )
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if !retention_index_is_usable(connection.as_mut()).await? {
            // 保留清理按 consumer 与时间范围扫描；自举既要覆盖新表，也要为既有本地表补齐索引，
            // 否则历史增长后每轮清理会退化为全表扫描。
            let created = sqlx::query(
                "CREATE INDEX inbox_message_retention_idx \
                 ON inbox_message (consumer_name, processed_at)",
            )
            .execute(connection.as_mut())
            .await;
            if let Err(error) = created {
                // 并发自举可能在本连接完成检查后由另一副本先建成同名索引；只有重新读取后确认
                // 前导列合同完整时才能采用该结果，其它同名索引仍按 DDL 失败拒绝启动。
                if !is_duplicate_index_name(&error)
                    || !retention_index_is_usable(connection.as_mut()).await?
                {
                    return Err(map_database(error));
                }
            }
        }
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

#[async_trait::async_trait]
impl nainbox_core::DurableInboxRetention for MySqlInbox {
    /// 业务作用：对单个消费命名空间执行一轮 owner 互斥的过期去重标记清理。
    ///
    /// 参数说明：
    /// - `consumer_name`：目标消费命名空间；`GET_LOCK` 按该值隔离清理者。
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
            natx::never_conn_for(&self.datasource),
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
        // MySQL named lock 在整个服务实例共享；把实际 database 与消费命名空间共同哈希，既让指向
        // 同一物理表的 datasource alias 竞争同一 owner，也避免不同 database 的同名消费者互相阻塞。
        // SHA-256 十六进制结果恰好 64 字节，不会触发 named lock 名称截断。
        // 非阻塞取锁(timeout=0)，竞争失败按合同报告 contended，由下一轮重试。
        let locked: Option<i64> = match tokio::time::timeout(
            deadline.saturating_duration_since(std::time::Instant::now()),
            sqlx::query_scalar(
                "SELECT GET_LOCK(SHA2(CONCAT('nainbox:retention:', DATABASE(), ':', ?), 256), 0)",
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
            Some(1) => {}
            Some(0) => {
                session.mark_reusable();
                report.claim_contended = true;
                return Ok(report);
            }
            _ => {
                return Err(InboxStoreError::new(
                    "advisory lock returned an invalid result",
                ));
            }
        }
        // 锁与删除必须同连接:GET_LOCK 是会话级,换连接等于无锁。以下所有语句都走同一 connection。
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
                        sqlx::query_scalar::<_, Option<i64>>(
                            "SELECT RELEASE_LOCK(SHA2(CONCAT('nainbox:retention:', DATABASE(), ':', ?), 256))",
                        )
                        .bind(consumer_name)
                        .fetch_one(session.connection()),
                    )
                    .await;
                    if matches!(released, Ok(Ok(Some(1)))) {
                        session.mark_reusable();
                    }
                }
                return Err(error);
            }
        };
        // 无论清理结果如何都归还锁。会话级锁只随会话终结自动释放;连接回到连接池后会话仍然存活,
        // 归还失败会让后续轮次持续 contended,因此失败必须留下告警证据(连接已断的场景锁随会话
        // 消亡,无需补救)。
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let released = tokio::time::timeout(
            remaining,
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT RELEASE_LOCK(SHA2(CONCAT('nainbox:retention:', DATABASE(), ':', ?), 256))",
            )
            .bind(consumer_name)
            .fetch_one(session.connection()),
        )
        .await;
        if matches!(released, Ok(Ok(Some(1)))) {
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

/// 业务作用：在已持有 owner 锁的连接上执行分批删除与账目统计。
///
/// 参数说明：
/// - `connection`：持有 `GET_LOCK` 的同一 MySQL 连接。
/// - `consumer_name`：目标消费命名空间。
/// - `policy`：已复验的保留策略。
///
/// 返回：本轮账目报告；删除或统计失败返回脱敏错误。
async fn retention_round_locked(
    connection: &mut sqlx::MySqlConnection,
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
    // cutoff 取整轮开始时刻并冻结为字面时间参数,轮内不随批次推进——否则长轮次的后批会吃进
    // 开轮时尚未到龄的行,与 trait 合同和 PostgreSQL 实现的口径不一致。时钟取数据库端
    // (processed_at 由建表 DEFAULT CURRENT_TIMESTAMP(6) 写入,同源无进程间时钟偏差),
    // 冻结值与后续比较都在同一连接的同一会话时区内,字符串字面量没有时区歧义。
    let min_age_us = policy.processed_min_age_ms.saturating_mul(1_000);
    let cutoff: String = bounded_step!(sqlx::query_scalar(
        "SELECT DATE_FORMAT(CURRENT_TIMESTAMP(6) - INTERVAL ? MICROSECOND, '%Y-%m-%d %H:%i:%s.%f')",
    )
    .bind(min_age_us)
    .fetch_one(&mut *connection));
    loop {
        let deleted = bounded_step!(sqlx::query(
            "DELETE FROM inbox_message \
             WHERE consumer_name = ? AND processed_at < ? \
             ORDER BY processed_at \
             LIMIT ?",
        )
        .bind(consumer_name)
        .bind(&cutoff)
        .bind(policy.batch_limit)
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
    let oldest_us: Option<i64> = bounded_step!(sqlx::query_scalar(
        "SELECT TIMESTAMPDIFF(MICROSECOND, MIN(processed_at), CURRENT_TIMESTAMP(6)) \
         FROM inbox_message WHERE consumer_name = ?",
    )
    .bind(consumer_name)
    .fetch_one(&mut *connection));
    report.oldest_candidate_age_ms = oldest_us.map(|value| value / 1_000);
    Ok(RetentionRoundEnd::Completed(report))
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

/// 业务作用：确认保留索引的前导列满足按消费命名空间和处理时刻扫描的执行合同。
///
/// 参数说明：
/// - `connection`：目标 datasource 的同一 MySQL 会话。
///
/// 返回：同名索引以前两列 `(consumer_name, processed_at)` 排列时返回真；查询失败返回脱敏数据库错误。
async fn retention_index_is_usable(
    connection: &mut sqlx::MySqlConnection,
) -> Result<bool, InboxStoreError> {
    let matching_columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.statistics \
         WHERE table_schema = DATABASE() AND table_name = 'inbox_message' \
         AND index_name = 'inbox_message_retention_idx' \
         AND ((seq_in_index = 1 AND column_name = 'consumer_name') \
           OR (seq_in_index = 2 AND column_name = 'processed_at'))",
    )
    .fetch_one(connection)
    .await
    .map_err(map_database)?;
    Ok(matching_columns == 2)
}

/// 业务作用：识别 MySQL 的同名索引竞争结局，供自举在服务端事实复验后采用另一副本的建索引结果。
///
/// 参数说明：
/// - `error`：`CREATE INDEX` 返回的 SQLx 错误。
///
/// 返回：服务端错误号为 1061 时返回真；其它数据库或驱动错误返回假。
fn is_duplicate_index_name(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>())
        .is_some_and(|mysql| mysql.number() == 1061)
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
