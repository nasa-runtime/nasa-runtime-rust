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
//! 事务确认提交后会推进进程内提交代际，受管 dispatcher 据此立即尝试投递；定时轮询继续承担
//! 跨进程写入、进程崩溃和通知合并后的持久化兜底，不作为正常提交的固定等待时间。
//!
//! **可选 DLT(毒丸死信,opt-in)**:[`MySqlOutbox::dispatch_batch_with_dlt`] 在保序投递之上给**首个失败行**
//! 记 `attempts`;连续失败达 `max_attempts` 即标 `dead=1` 移出投递流(死信),下一轮从其后继续——毒丸不再
//! 永久阻塞后续事件。**代价是明确的**:死信之后、同聚合根的后续事件会先于它落地(局部有序对毒丸让步);
//! 不接受此让步的场景继续用 [`MySqlOutbox::dispatch_batch`],该入口永不跳过,毒丸会阻塞直到人工介入。死信行保留
//! 全部字段供人工重放(`dead=0` 复活)。
//!
//! 底层错误一律脱敏为 [`OutboxStoreError`](不回显 SQL/凭据/payload)。

#![forbid(unsafe_code)]

use std::sync::OnceLock;

use naoutbox_core::{dispatch_in_order, DispatchReport, OutboxEvent, OutboxPublisher};
use sqlx::{pool::PoolConnection, MySql, MySqlConnection, Row as _};
use tokio::sync::watch;

/// 进程内提交代际只承担低延迟唤醒；数据库轮询仍是跨进程和崩溃恢复的最终事实来源。
static COMMIT_SIGNAL: OnceLock<watch::Sender<u64>> = OnceLock::new();

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
     KEY idx_dispatchable (dispatched, dead, id), \
     KEY idx_dead (dead, id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// outbox store I/O 失败(脱敏;不含 SQL/凭据/payload)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxStoreError {
    /// 稳定脱敏原因。
    pub reason: String,
}

impl OutboxStoreError {
    /// 业务作用：用稳定脱敏原因构造 Outbox 持久层错误。
    ///
    /// 参数说明：
    /// - `reason`：允许向上游暴露的稳定失败分类。
    ///
    /// 返回：不携带 SQL、凭据或事件正文的存储错误。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for OutboxStoreError {
    /// 业务作用：输出不含 SQL、连接信息或 payload 的稳定存储错误。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：稳定摘要写入成功时返回 `Ok`。
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
    /// 业务作用：用专用池连接创建尚未释放的 dispatcher claim guard，预先启用断线释放兜底。
    ///
    /// 参数说明：
    /// - `connection`：即将竞争 named lock 的独占池连接。
    ///
    /// 返回：持有连接关闭责任的 claim guard。
    fn new(connection: PoolConnection<MySql>) -> Self {
        Self {
            connection: Some(connection),
        }
    }

    /// 业务作用：借用绑定 named lock 的底层 MySQL session，确保领取、发布标记与释放使用同一会话。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：claim guard 独占持有的可变 MySQL 连接。
    fn connection(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_mut()
            .expect("dispatch claim connection must exist until release")
    }

    /// 业务作用：在当前 session 明确没有取得 claim 时解除兜底关闭，让健康连接正常回池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；本方法消费 guard 并归还连接所有权。
    fn disarm(mut self) {
        let _ = self.connection.take();
    }

    /// 业务作用：显式释放 named lock；只有服务端确认释放成功才允许连接回池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；释放结果不确定时保留关闭责任，由 `Drop` 关闭物理会话。
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
    /// 业务作用：未确认 `RELEASE_LOCK` 时关闭物理连接，阻止 named lock 随池连接泄漏。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；只标记连接在析构时关闭，不执行阻塞 I/O。
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
    /// 业务作用：创建无状态 Outbox 入口，不提前建连或取得 dispatcher claim。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可在事务写侧或独立投递侧复用的轻量句柄。
    pub fn new() -> Self {
        Self
    }

    /// 业务作用：为受控自举环境创建 Outbox 表，并把历史演示表补齐稳定身份、死信字段和投递索引。
    ///
    /// 参数说明: 无；连接来自已经初始化的 `natx` 默认池。
    ///
    /// 返回：表结构达到当前运行合同后成功；建表、历史回填或强制约束失败时返回脱敏存储错误。
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
        // 指标抓取与 dispatcher 共用控制面数据库；必须先补齐覆盖索引，避免历史表按总行数扫描，
        // 也避免本地死信长期停留在 dispatched=0 区间时拖慢每一轮待投递查询。
        for statement in [
            "ALTER TABLE outbox_event ADD INDEX idx_dispatchable (dispatched, dead, id)",
            "ALTER TABLE outbox_event ADD INDEX idx_dead (dead, id)",
        ] {
            let _ = sqlx::query(statement).execute(conn.as_mut()).await;
        }
        Ok(())
    }

    /// 业务作用：追加一条 Outbox 事件，并在数据库明确提交后唤醒当前进程的 dispatcher。
    ///
    /// 参数说明：
    /// - `event`：与业务事实共享事务、携带稳定事件身份的待发布事件。
    ///
    /// 返回：事务内完成 INSERT 并登记提交后唤醒、或事务外完成独立提交时成功；连接与写入失败返回
    /// 脱敏存储错误。事务回滚时不会发送唤醒。
    pub async fn append(&self, event: &OutboxEvent) -> Result<(), OutboxStoreError> {
        let transactional = natx::in_transaction();
        let mut conn = natx::conn().await.map_err(map_err)?;
        Self::append_on(conn.as_mut(), event).await?;
        drop(conn);
        if transactional {
            register_commit_notification()?;
        } else {
            notify_committed_append();
        }
        Ok(())
    }

    /// 业务作用：只在当前 ambient MySQL 事务内追加关键事件，并把投递唤醒绑定到最外层提交确认。
    ///
    /// 审计、资金等关键双写应使用此入口；事务上下文缺失时明确失败，绝不回退到 autocommit pool。
    ///
    /// 参数说明：
    /// - `event`：必须与当前业务事实原子提交的待发布事件。
    ///
    /// 返回：INSERT 与提交后唤醒均成功登记时完成；缺少事务或写入失败返回脱敏存储错误。回滚、
    /// rollback-only 与提交失败都不会唤醒 dispatcher。
    pub async fn append_transactional(&self, event: &OutboxEvent) -> Result<(), OutboxStoreError> {
        let mut conn = natx::mandatory_conn().await.map_err(map_err)?;
        Self::append_on(conn.as_mut(), event).await?;
        drop(conn);
        register_commit_notification()?;
        Ok(())
    }

    /// 业务作用：订阅当前进程 Outbox 提交代际，使受管 dispatcher 在新事实提交后立即尝试投递。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：初值为当前代际的接收端；通知只优化本进程延迟，调用方仍须保留数据库轮询兜底。
    pub fn subscribe_committed_appends() -> watch::Receiver<u64> {
        commit_signal().subscribe()
    }

    /// 业务作用：在调用方提供的连接内写入完整 Outbox 事件，不改变连接的提交所有权。
    ///
    /// 参数说明：
    /// - `connection`：业务事务或独立提交路径持有的 MySQL 连接。
    /// - `event`：已具备稳定事件身份的待发布事件。
    ///
    /// 返回：事件行写入成功时完成；数据库失败返回脱敏错误，由外层决定提交或回滚。
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

    /// 业务作用：读取仍可投递的持久事件数量，供积压指标和恢复决策使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：基于 `(dispatched, dead, id)` 覆盖索引返回非死信积压；数据库失败返回脱敏错误。
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

    /// 业务作用：在数据库级唯一 claim 下保序发布一个有界批次，并只标记下游已确认的成功前缀。
    ///
    /// 参数说明：
    /// - `publisher`：下游发布端；成功必须表示下游已经确认接收。
    /// - `limit`：单轮最多领取的行数，限制内存与外部 I/O 占用。
    ///
    /// 返回：claim 未取得时返回空报告；否则返回发布总量、成功前缀和首个错误，存储失败返回脱敏错误。
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

    /// 业务作用：在保序投递之上累计首个失败事件的预算，达到阈值后将其移入本地死信集合。
    ///
    /// 给首个失败行记 `attempts`,达到 `max_attempts` 即标死信
    /// (`dead=1`)移出投递流,下一轮从其后继续。返回本轮报告(报告口径与 [`dispatch_batch`](Self::dispatch_batch) 一致)。
    ///
    /// 有序性让步见模块文档:死信行之后的同聚合根事件会先于它落地。`max_attempts` 应 ≥ 1;计数按
    /// 「本方法观察到的失败轮次」累计,与 `dispatch_batch` 混用不会误标(那条路径不动 `attempts`)。
    ///
    /// 参数说明：
    /// - `publisher`：下游发布端。
    /// - `limit`：单轮最多领取的行数。
    /// - `max_attempts`：同一事件累计多少个失败轮次后标记死信。
    ///
    /// 返回：返回本轮发布报告；阈值为零或存储失败时返回脱敏错误。进入死信意味着显式放弃该位置的
    /// 严格顺序，调用方必须事先批准。
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

    /// 业务作用：读取本地死信集合数量，供毒丸策略告警和人工恢复决策使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：基于 `(dead, id)` 覆盖索引返回死信总数；数据库失败返回脱敏错误。
    pub async fn dead_count(&self) -> Result<u64, OutboxStoreError> {
        let mut conn = natx::conn().await.map_err(map_err)?;
        let row = sqlx::query("SELECT COUNT(*) AS n FROM outbox_event WHERE dead = 1")
            .fetch_one(conn.as_mut())
            .await
            .map_err(map_err)?;
        let count: i64 = row.try_get("n").map_err(map_err)?;
        Ok(count.max(0) as u64)
    }

    /// 业务作用：原子累计一个未确认事件的失败轮次，并在预算耗尽时标记为本地死信。
    ///
    /// 参数说明：
    /// - `connection`：持有 dispatcher claim 的 MySQL 会话。
    /// - `id`：首个失败事件的数据库主键。
    /// - `max_attempts`：死信阈值。
    ///
    /// 返回：行仍处于可投递状态时完成原子更新；数据库失败返回脱敏错误。
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

    /// 业务作用：按 `id` 升序领取真实待投递候选，避免死信空洞破坏局部顺序和批次上限。
    ///
    /// 参数说明：
    /// - `connection`：持有 dispatcher claim 的 MySQL 会话。
    /// - `limit`：最多回表读取的事件行数。
    ///
    /// 返回：联合索引定位后回表得到的有序事件集合；数据库或解码失败返回脱敏错误。
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

    /// 业务作用：只标记本轮已经得到下游确认的精确行，防止并发或死信空洞被范围更新越过。
    ///
    /// 参数说明：
    /// - `connection`：持有 dispatcher claim 的 MySQL 会话。
    /// - `rows`：已经按顺序得到下游确认的成功前缀。
    ///
    /// 返回：全部精确身份完成幂等标记时成功；数据库失败返回脱敏错误。
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

    /// 业务作用：尝试取得数据库级 dispatcher claim，阻止多个进程同时推进同一表的顺序游标。
    ///
    /// dispatcher 必须运行在业务事务之外：claim、网络 publish 与标记不能占用 ambient transaction，
    /// 否则会把业务事务跨外部 I/O 长时间悬挂。取得的 pooled connection 同时承载 poll/mark，故
    /// `max_connections=1` 也不会发生“持 claim 后再等第二条连接”的自锁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：取得锁时返回 armed guard，锁已被其它进程持有时返回 `None`；事务上下文或数据库失败
    /// 返回脱敏错误。
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

/// 业务作用：取得进程唯一的提交代际发布端，供写侧和受管 dispatcher 共享唤醒事实。
///
/// 参数说明: 无。
///
/// 返回：惰性创建且进程内稳定的 watch 发布端。
fn commit_signal() -> &'static watch::Sender<u64> {
    COMMIT_SIGNAL.get_or_init(|| watch::channel(0).0)
}

/// 业务作用：在数据库提交已经确定后推进代际，缩短持久化事件进入 dispatcher 的等待时间。
///
/// 参数说明: 无。
///
/// 返回：无；接收端可合并连续代际，真实待投递集合始终以数据库为准。
fn notify_committed_append() {
    commit_signal().send_modify(|generation| {
        *generation = generation.wrapping_add(1);
    });
}

/// 业务作用：把 Outbox 唤醒挂到 ambient transaction 的提交确认之后，防止未提交事件提前触发投递。
///
/// 参数说明: 无。
///
/// 返回：成功登记时返回 `Ok`；事务上下文意外丢失时返回脱敏错误并让调用方回滚本次写入。
fn register_commit_notification() -> Result<(), OutboxStoreError> {
    natx::after_commit(|| async {
        notify_committed_append();
    })
    .map_err(map_err)
}

/// 业务作用：把任意底层错误映射为脱敏的 [`OutboxStoreError`]，防止 SQL、凭据或 payload 外泄。
///
/// 参数说明：
/// - `_error`：只用于丢弃敏感细节的底层错误。
///
/// 返回：固定数据库失败分类。
fn map_err<E>(_error: E) -> OutboxStoreError {
    OutboxStoreError::new("database error")
}
