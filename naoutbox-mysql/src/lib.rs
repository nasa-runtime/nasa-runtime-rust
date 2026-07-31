//! Outbox 的 MySQL 后端。
//!
//! **写侧**:`append` 经 [`natx::conn`]——在 `natx::run`/`#[transactional]` 事务内调用则
//! 事件 INSERT 与业务写**同一事务原子提交/回滚**,消除“业务已提交但事件丢失”窗口;事务外则走连接池。
//!
//! **投递侧(dispatcher)**:`dispatch_batch` 轮询未投递行(按 `id` 升序保序)→ 复用
//! [`naoutbox_core::dispatch_in_order`] 保序至少一次投递 → 把成功前缀 `mark_dispatched`。投递失败的行
//! 留 `dispatched=0` 下轮重试。**至少一次**:下游消费者按 `event_id` 幂等去重。每轮先竞争 MySQL
//! advisory claim，同一数据库任一时刻只有一个 dispatcher 轮询/发布/标记；连接断开时 claim 自动释放。
//! 成功只按本轮精确 id 标记，绝不使用 `id <= max` 越权标记其它 dispatcher 的记录。
//!
//! **可选 DLT(毒丸死信,opt-in)**:[`MySqlOutbox::dispatch_batch_with_dlt`] 在保序投递之上给**首个失败行**
//! 记 `attempts`;连续失败达 `max_attempts` 即标 `dead=1` 移出投递流(死信),下一轮从其后继续——毒丸不再
//! 永久阻塞后续事件。**代价是明确的**:死信之后、同聚合根的后续事件会先于它落地(局部有序对毒丸让步);
//! 不接受此让步的场景继续用 [`MySqlOutbox::dispatch_batch`],该入口永不跳过,毒丸会阻塞直到人工介入。死信行保留
//! 全部字段供人工重放(`dead=0` 复活)。
//!
//! 底层错误一律脱敏为 [`OutboxStoreError`](不回显 SQL/凭据/payload)。

#![forbid(unsafe_code)]

use naoutbox_core::{dispatch_in_order, DispatchReport, OutboxEvent, OutboxPublisher};
use sqlx::{pool::PoolConnection, MySql, MySqlConnection, Row as _};

/// outbox 表建表语句(部署应由迁移拥有 schema;此处便于演示环境自举)。
const CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS outbox_event ( \
     id BIGINT NOT NULL AUTO_INCREMENT, \
     event_id CHAR(36) NOT NULL, \
     aggregate_type VARCHAR(128) NOT NULL, \
     aggregate_id VARCHAR(190) NOT NULL, \
     event_type VARCHAR(128) NOT NULL, \
     payload LONGBLOB NOT NULL, \
     traceparent VARCHAR(64) NULL, \
     dispatched TINYINT NOT NULL DEFAULT 0, \
     attempts INT NOT NULL DEFAULT 0, \
     dead TINYINT NOT NULL DEFAULT 0, \
     created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
     dispatched_at TIMESTAMP NULL, \
     PRIMARY KEY (id), \
     UNIQUE KEY uk_event_id (event_id), \
     KEY idx_pending (dispatched, id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// outbox store I/O 失败(脱敏;不含 SQL/凭据/payload)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxStoreError {
    /// 稳定脱敏原因。
    pub reason: String,
}

impl OutboxStoreError {
    /// 用脱敏原因构造。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for OutboxStoreError {
    /// 输出不含 SQL、连接信息或 payload 的稳定存储错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "outbox store error: {}", self.reason)
    }
}

impl std::error::Error for OutboxStoreError {}

/// 一条待投递行(带 DB 主键 id,供投递成功后 `mark_dispatched`)。
#[derive(Debug, Clone)]
struct PendingRow {
    id: i64,
    event: OutboxEvent,
}

/// 绑定 MySQL session advisory lock 的 dispatcher 连接。
///
/// 正常路径必须先调用 [`Self::release`]：确认 `RELEASE_LOCK` 成功后连接才会回到 pool。错误返回、
/// future cancellation 或 panic 会走 [`Drop`]，把 pooled connection 标为 close-on-drop；物理连接关闭
/// 后 MySQL 才能可靠释放该 session 持有的 named lock。不能只把连接普通归还 pool，否则锁会跟着
/// session 留在池中。
struct DispatchClaim {
    connection: Option<PoolConnection<MySql>>,
}

impl DispatchClaim {
    /// 用专用池连接创建尚未释放的 dispatcher claim guard。
    fn new(connection: PoolConnection<MySql>) -> Self {
        Self {
            connection: Some(connection),
        }
    }

    /// 借用绑定 named lock 的底层 MySQL session。
    fn connection(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_mut()
            .expect("dispatch claim connection must exist until release")
    }

    /// 当前 session 没有取得 claim 时解除兜底关闭，让健康连接正常回池。
    fn disarm(mut self) {
        let _ = self.connection.take();
    }

    /// 显式释放 named lock；只有服务端确认释放成功才允许连接回池。
    async fn release(mut self) {
        let released: Result<Option<i64>, _> = sqlx::query_scalar(
            "SELECT RELEASE_LOCK(SHA2(CONCAT('nasa-outbox:', DATABASE()), 256))",
        )
        .fetch_one(self.connection())
        .await;
        if matches!(released, Ok(Some(1))) {
            let _ = self.connection.take();
        }
        // 失败/非 owner:保留 connection 给 Drop，关闭 session 兜底释放可能仍持有的锁。
    }
}

impl Drop for DispatchClaim {
    /// 未确认 RELEASE_LOCK 的路径关闭物理连接，阻止 named lock 随池连接泄漏。
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

/// MySQL outbox。无自身状态:每次操作经 `natx::conn()` 取连接,自动感知 ambient 事务。
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlOutbox;

impl MySqlOutbox {
    /// 创建 store(不建连)。
    pub fn new() -> Self {
        Self
    }

    /// 确保 outbox 表存在。部署由迁移拥有 schema;此方法供演示环境自举。需先 `natx::init`。
    pub async fn ensure_schema() -> Result<(), OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        sqlx::query(CREATE_TABLE_SQL)
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        // 既有表升级兜底(CREATE IF NOT EXISTS 不改旧表):best-effort 补 DLT 两列,已存在则忽略错误。
        // 部署环境仍应由迁移拥有 schema;这里只服务演示环境自举的平滑升级。
        for statement in [
            "ALTER TABLE outbox_event ADD COLUMN attempts INT NOT NULL DEFAULT 0",
            "ALTER TABLE outbox_event ADD COLUMN dead TINYINT NOT NULL DEFAULT 0",
            "ALTER TABLE outbox_event ADD COLUMN event_id CHAR(36) NULL",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        // 旧 demo 表补事件 id；UUID 只在 migration 期生成一次，后续重投始终复用同一 id。
        sqlx::query("UPDATE outbox_event SET event_id = UUID() WHERE event_id IS NULL")
            .execute(conn.as_mut())
            .await
            .map_err(map_err)?;
        let _ = sqlx::query(
            "ALTER TABLE outbox_event MODIFY event_id CHAR(36) NOT NULL, \
             ADD UNIQUE KEY uk_event_id (event_id)",
        )
        .execute(conn.as_mut())
        .await;
        Ok(())
    }

    /// 在**当前事务内**追加一条事件(与业务写同提交/回滚)。事务外调用则走连接池独立提交。
    pub async fn append(&self, event: &OutboxEvent) -> Result<(), OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        Self::append_on(conn.as_mut(), event).await
    }

    /// 只允许在当前 ambient MySQL 事务内追加事件。
    ///
    /// 审计、资金等关键双写应使用此入口；事务上下文缺失时明确失败，绝不回退到 autocommit pool。
    pub async fn append_transactional(&self, event: &OutboxEvent) -> Result<(), OutboxStoreError> {
        let mut conn = natx::mandatory_conn().await.map_err(map_err)?;
        Self::append_on(conn.as_mut(), event).await
    }

    /// 在调用方提供的连接或 ambient 事务内写入完整 outbox 事件。
    async fn append_on(
        connection: &mut MySqlConnection,
        event: &OutboxEvent,
    ) -> Result<(), OutboxStoreError> {
        sqlx::query(
            "INSERT INTO outbox_event \
             (event_id, aggregate_type, aggregate_id, event_type, payload, traceparent, dispatched) \
             VALUES (?, ?, ?, ?, ?, ?, 0)",
        )
        .bind(&event.event_id)
        .bind(&event.aggregate_type)
        .bind(&event.aggregate_id)
        .bind(&event.event_type)
        .bind(&event.payload)
        .bind(&event.traceparent)
        .execute(connection)
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// 未投递事件数(监控用)。
    pub async fn pending_count(&self) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row =
            sqlx::query("SELECT COUNT(*) AS n FROM outbox_event WHERE dispatched = 0 AND dead = 0")
                .fetch_one(conn.as_mut())
                .await
                .map_err(map_err)?;
        let count: i64 = row.try_get("n").map_err(map_err)?;
        Ok(count.max(0) as u64)
    }

    /// 一轮投递:轮询未投递行(保序)→ 保序至少一次投递 → 标记成功前缀已投递。返回本轮报告。
    ///
    /// # 参数
    /// - `publisher`:下游发布端(如 Kafka topic 适配器)。
    /// - `limit`:单轮最多取多少行(有界,防一次拉爆)。
    pub async fn dispatch_batch<P>(
        &self,
        publisher: &P,
        limit: u32,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        let Some(mut dispatch_claim) = self.try_claim_dispatcher().await? else {
            return Ok(DispatchReport {
                published: 0,
                total: 0,
                first_error: None,
            });
        };
        let pending = Self::poll_pending(dispatch_claim.connection(), limit).await?;
        let events: Vec<OutboxEvent> = pending.iter().map(|row| row.event.clone()).collect();
        let report = dispatch_in_order(&events, publisher).await;
        if report.published > 0 {
            Self::mark_dispatched_exact(dispatch_claim.connection(), &pending[..report.published])
                .await?;
        }
        dispatch_claim.release().await;
        Ok(report)
    }

    /// 一轮**带 DLT** 的投递:保序投递之上,给首个失败行记 `attempts`,达到 `max_attempts` 即标死信
    /// (`dead=1`)移出投递流,下一轮从其后继续。返回本轮报告(报告口径与 [`dispatch_batch`](Self::dispatch_batch) 一致)。
    ///
    /// 有序性让步见模块文档:死信行之后的同聚合根事件会先于它落地。`max_attempts` 应 ≥ 1;计数按
    /// 「本方法观察到的失败轮次」累计,与 `dispatch_batch` 混用不会误标(那条路径不动 `attempts`)。
    ///
    /// # 参数
    /// - `publisher`:下游发布端。
    /// - `limit`:单轮最多取多少行。
    /// - `max_attempts`:同一行连续失败多少次后标死信。
    pub async fn dispatch_batch_with_dlt<P>(
        &self,
        publisher: &P,
        limit: u32,
        max_attempts: u32,
    ) -> Result<DispatchReport, OutboxStoreError>
    where
        P: OutboxPublisher + ?Sized,
    {
        if max_attempts == 0 {
            return Err(OutboxStoreError::new(
                "max_attempts must be greater than zero",
            ));
        }
        let Some(mut dispatch_claim) = self.try_claim_dispatcher().await? else {
            return Ok(DispatchReport {
                published: 0,
                total: 0,
                first_error: None,
            });
        };
        let pending = Self::poll_pending(dispatch_claim.connection(), limit).await?;
        let events: Vec<OutboxEvent> = pending.iter().map(|row| row.event.clone()).collect();
        let report = dispatch_in_order(&events, publisher).await;
        if report.published > 0 {
            Self::mark_dispatched_exact(dispatch_claim.connection(), &pending[..report.published])
                .await?;
        }
        // 首个失败行记一次尝试;达到上限即标死信(单行静态 SQL,原子)。
        if report.first_error.is_some() && report.published < pending.len() {
            let failed_id = pending[report.published].id;
            Self::record_failed_attempt(dispatch_claim.connection(), failed_id, max_attempts)
                .await?;
        }
        dispatch_claim.release().await;
        Ok(report)
    }

    /// 死信行数(监控用)。
    pub async fn dead_count(&self) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row = sqlx::query("SELECT COUNT(*) AS n FROM outbox_event WHERE dead = 1")
            .fetch_one(conn.as_mut())
            .await
            .map_err(map_err)?;
        let count: i64 = row.try_get("n").map_err(map_err)?;
        Ok(count.max(0) as u64)
    }

    /// 给一行记一次失败尝试;累计达到 `max_attempts` 即标死信。
    ///
    /// # 参数
    /// - `id`:失败行主键。
    /// - `max_attempts`:死信阈值。
    async fn record_failed_attempt(
        connection: &mut MySqlConnection,
        id: i64,
        max_attempts: u32,
    ) -> Result<(), OutboxStoreError> {
        sqlx::query(
            "UPDATE outbox_event SET \
             dead = IF(attempts + 1 >= ?, 1, dead), \
             attempts = attempts + 1 \
             WHERE id = ? AND dispatched = 0 AND dead = 0",
        )
        .bind(max_attempts)
        .bind(id)
        .execute(connection)
        .await
        .map_err(map_err)?;
        Ok(())
    }

    /// 轮询未投递行(按 id 升序,保投递顺序)。
    async fn poll_pending(
        connection: &mut MySqlConnection,
        limit: u32,
    ) -> Result<Vec<PendingRow>, OutboxStoreError> {
        let rows = sqlx::query(
            "SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, traceparent \
             FROM outbox_event WHERE dispatched = 0 AND dead = 0 ORDER BY id ASC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(connection)
        .await
        .map_err(map_err)?;
        let mut pending = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.try_get("id").map_err(map_err)?;
            let event = OutboxEvent {
                event_id: row.try_get("event_id").map_err(map_err)?,
                aggregate_type: row.try_get("aggregate_type").map_err(map_err)?,
                aggregate_id: row.try_get("aggregate_id").map_err(map_err)?,
                event_type: row.try_get("event_type").map_err(map_err)?,
                payload: row.try_get("payload").map_err(map_err)?,
                traceparent: row.try_get("traceparent").map_err(map_err)?,
            };
            pending.push(PendingRow { id, event });
        }
        Ok(pending)
    }

    /// 只标记本轮已经收到 broker ack 的精确行；不能用 `id <= max`，否则并发/死信洞会被误标。
    async fn mark_dispatched_exact(
        connection: &mut MySqlConnection,
        rows: &[PendingRow],
    ) -> Result<(), OutboxStoreError> {
        for row in rows {
            sqlx::query(
                "UPDATE outbox_event SET dispatched = 1, dispatched_at = CURRENT_TIMESTAMP \
                 WHERE dispatched = 0 AND dead = 0 AND id = ? AND event_id = ?",
            )
            .bind(row.id)
            .bind(&row.event.event_id)
            .execute(&mut *connection)
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }

    /// 尝试取得数据库级 dispatcher claim。
    ///
    /// dispatcher 必须运行在业务事务之外：claim、网络 publish 与标记不能占用 ambient transaction，
    /// 否则会把业务事务跨外部 I/O 长时间悬挂。取得的 pooled connection 同时承载 poll/mark，故
    /// `max_connections=1` 也不会发生“持 claim 后再等第二条连接”的自锁。
    async fn try_claim_dispatcher(&self) -> Result<Option<DispatchClaim>, OutboxStoreError> {
        let conn = natx::conn().await.map_err(map_err)?;
        let natx::Conn::Pool(conn) = conn else {
            return Err(OutboxStoreError::new(
                "dispatcher cannot run inside an ambient transaction",
            ));
        };
        // GET_LOCK 请求本身若在响应前取消/断线，服务端可能已经取得锁；先 armed，异常即关闭 session。
        let mut claim = DispatchClaim::new(conn);
        let claimed: Option<i64> =
            sqlx::query_scalar("SELECT GET_LOCK(SHA2(CONCAT('nasa-outbox:', DATABASE()), 256), 0)")
                .fetch_one(claim.connection())
                .await
                .map_err(map_err)?;
        if claimed == Some(1) {
            Ok(Some(claim))
        } else {
            claim.disarm();
            Ok(None)
        }
    }
}

/// 把任意底层错误映射为脱敏的 [`OutboxStoreError`]。
fn map_err<E>(_error: E) -> OutboxStoreError {
    OutboxStoreError::new("database error")
}
