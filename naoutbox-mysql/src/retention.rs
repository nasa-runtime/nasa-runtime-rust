//! Outbox 保留、归档与清理执行器（MySQL）。
//!
//! 执行边界（与投递面完全隔离）：
//!
//! - 清理使用**独立** session-bound retention claim（`nasa-outbox-retention:` 命名空间），
//!   同一数据库任一时刻只有一个清理 owner；它不持有 dispatcher claim，claim 竞争失败即
//!   让路，不等待、不阻塞投递。
//! - 候选只可能来自 `dispatched = 1`（已投递）或按独立批准的 `dead = 1`（死信）；
//!   `dispatched = 0 AND dead = 0` 的待投递行被 WHERE 结构性排除，永不删除。
//! - 若启用归档，先按 `event_id` 幂等写归档端并取得可复验收据，才允许删除源行；
//!   回包不确定时用收据重查恢复，不盲目重写、不删源行。
//! - 删除以本批已选定的主键集合执行，每批一条 DELETE 且在显式事务内提交；遇到锁等待、
//!   语句超时或超出时间预算即停止本轮并弃用连接——未提交的事务随断连回滚,轮次返回后
//!   不存在迟到提交的未决副作用;绝不无上限 DELETE 或反复全表扫描。

use std::time::Instant;

use naoutbox_core::{OutboxArchive, OutboxEvent, OutboxRetentionPolicy};
use sqlx::{pool::PoolConnection, MySql, MySqlConnection, Row as _};

use crate::{map_err, MySqlOutbox, OutboxStoreError};

/// 存储锁竞争的稳定原因——保留清理的删除/处置语句等待行锁超时或死锁时使用。
/// 受管执行器据此把锁冲突与归档故障、普通存储失败分开计数与告警,不折叠成同一类。
pub const RETENTION_LOCK_CONTENTION_REASON: &str = "retention storage lock contention";

/// 删除事务的 COMMIT 已发出但数据库应答无法确认时使用的稳定原因。
///
/// 该分类表示源行可能已经删除，受管层只能记录失败并由下一轮根据持久候选事实收敛，
/// 不得把本批计入已确认删除或按普通预算耗尽处理。
pub const RETENTION_COMMIT_UNCERTAIN_REASON: &str =
    "retention database commit acknowledgement is uncertain";

/// 业务作用：把删除/处置语句的数据库错误按锁竞争分类——MySQL 1205(锁等待超时)与
/// 1213(死锁)映射为稳定原因,其余保持通用脱敏错误。
///
/// 参数说明：
/// - `error`: sqlx 数据库错误。
///
/// 返回：分类后的脱敏存储错误。
fn map_delete_err(error: sqlx::Error) -> OutboxStoreError {
    if let Some(db) = error.as_database_error() {
        // MySQL 驱动的 code() 是 SQLSTATE(1205/1213 同为 HY000/40001 类),错误号要走
        // 具体驱动错误的 number()。
        if let Some(mysql) = db.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>() {
            if matches!(mysql.number(), 1205 | 1213) {
                return OutboxStoreError::new(RETENTION_LOCK_CONTENTION_REASON);
            }
        }
    }
    map_err(error)
}

/// 业务作用：对删除事务的 COMMIT 错误做封闭分类，保留“数据库明确拒绝”和“应答不确定”
/// 的安全差异，避免受管层把可能已经提交的删除当成普通存储失败。
///
/// 参数说明：
/// - `error`：COMMIT 返回的 sqlx 错误。
///
/// 返回：连接、TLS、协议或驱动工作线程中断映射为提交不确定；数据库明确拒绝及其它
/// 基础设施失败返回各自稳定的脱敏原因。
fn map_commit_err(error: sqlx::Error) -> OutboxStoreError {
    match error {
        sqlx::Error::Database(_) => {
            OutboxStoreError::new("retention database rejected transaction commit")
        }
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::WorkerCrashed => OutboxStoreError::new(RETENTION_COMMIT_UNCERTAIN_REASON),
        _ => OutboxStoreError::new("retention transaction commit infrastructure failed"),
    }
}

/// 清理会话的锁等待上界（秒）：候选行与业务写发生行锁冲突时快速让路，
/// 由下一轮重试，绝不让清理反压业务提交。
const RETENTION_LOCK_WAIT_SECONDS: u32 = 5;

/// 业务作用：一轮保留清理的低基数结果，供受管层累计指标与退避决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetentionRoundReport {
    /// 本轮真实写入归档端（或收据重查确认）的事件数。
    pub archived: u64,
    /// 本轮删除的已投递行数。
    pub deleted_dispatched: u64,
    /// 本轮删除的死信行数。
    pub deleted_dead: u64,
    /// retention claim 被其它 owner 持有，本轮未执行任何清理。
    pub claim_contended: bool,
    /// 本轮因时间预算耗尽提前停止，仍有候选留待下一轮。被截断的数据库步骤随弃连
    /// 回滚,已计入的删除/归档数都是已提交事实,不存在轮次返回后才生效的副作用。
    pub budget_exhausted: bool,
    /// 本轮观察到的最老候选年龄（毫秒）；无候选时为空。
    pub oldest_candidate_age_ms: Option<i64>,
}

/// 绑定 MySQL session named lock 的 retention claim。
///
/// 与 dispatcher claim 同一释放纪律：只有服务端确认 session 配置已复原且
/// `RELEASE_LOCK` 成功，连接才回池；释放不确定、错误返回或取消都走 `Drop` 关闭
/// 物理连接，锁随 session 终结而释放，绝不让带锁或带残留配置的 session 回到连接池。
struct RetentionClaim {
    connection: Option<PoolConnection<MySql>>,
    /// 进入清理前捕获的 `innodb_lock_wait_timeout` 原值。清理把它收紧到秒级只是
    /// 本轮策略,连接来自通用业务池——归还前必须恢复原值,否则后续借到这条连接的
    /// 业务事务会在正常冲突下被随机提前判 1205。
    restore_lock_wait: Option<u64>,
}

impl RetentionClaim {
    /// 业务作用：以独占池连接构造尚未释放的 claim guard，预置断线释放兜底。
    ///
    /// 参数说明：
    /// - `connection`：即将竞争 retention named lock 的池连接。
    ///
    /// 返回：持有连接关闭责任的 guard。
    fn new(connection: PoolConnection<MySql>) -> Self {
        Self {
            connection: Some(connection),
            restore_lock_wait: None,
        }
    }

    /// 业务作用：借用绑定锁的底层会话，保证候选查询、归档核对与删除都在同一 session。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：guard 独占的可变 MySQL 连接。
    fn connection(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_mut()
            .expect("retention claim connection must exist until release")
    }

    /// 业务作用：明确未取得 claim 时解除兜底关闭，让健康连接正常回池。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；消费 guard 并交还连接所有权。
    fn disarm(mut self) {
        let _ = self.connection.take();
    }

    /// 业务作用：捕获并收紧本 session 的 `innodb_lock_wait_timeout`——清理与业务写
    /// 争行锁时快速让路,由下一轮重试,绝不让清理反压业务提交。
    ///
    /// 原值先于收紧被捕获并记入 guard,归还路径据此复原;捕获或收紧任一失败都
    /// 返回错误,由调用方弃连,不让配置状态不明的连接继续执行清理。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：捕获与收紧都得到服务端确认时返回 `Ok`。
    async fn tighten_lock_wait(&mut self) -> Result<(), OutboxStoreError> {
        let prior: u64 = sqlx::query_scalar("SELECT @@SESSION.innodb_lock_wait_timeout")
            .fetch_one(self.connection())
            .await
            .map_err(map_err)?;
        // 语句由本文件常量拼装,无任何外部输入;显式声明已过注入审计。
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET SESSION innodb_lock_wait_timeout = {RETENTION_LOCK_WAIT_SECONDS}"
        )))
        .execute(self.connection())
        .await
        .map_err(map_err)?;
        self.restore_lock_wait = Some(prior);
        Ok(())
    }

    /// 业务作用：复原 session 配置并显式释放 retention named lock；两步都得到服务端
    /// 确认连接才回池。
    ///
    /// 复原必须先于释放:释放成功即解除关闭责任,若此时配置尚未复原,收紧到 5 秒的
    /// 锁等待会随连接回池污染后续业务事务。任一步失败或不确定都把关闭责任留给
    /// `Drop`,由 session 终结统一收口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；复原或释放不确定时保留关闭责任给 `Drop`。
    async fn release(mut self) {
        if let Some(prior) = self.restore_lock_wait {
            // 原值来自服务端捕获,数值拼装无外部输入。
            let restored = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SET SESSION innodb_lock_wait_timeout = {prior}"
            )))
            .execute(self.connection())
            .await;
            if restored.is_err() {
                return;
            }
        }
        let released: Result<Option<i64>, _> = sqlx::query_scalar(
            "SELECT RELEASE_LOCK(SHA2(CONCAT('nasa-outbox-retention:', DATABASE()), 256))",
        )
        .fetch_one(self.connection())
        .await;
        if matches!(released, Ok(Some(1))) {
            let _ = self.connection.take();
        }
    }
}

impl Drop for RetentionClaim {
    /// 业务作用：未确认释放时关闭物理连接，阻止 retention lock 随池连接泄漏。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；只标记关闭，不做阻塞 I/O。
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

/// 单轮清理主体的收束方式——决定 claim 连接的去向:完整走完才允许回池,
/// 预算中断意味着某个数据库步骤被截断,连接状态不明必须弃用。
enum InnerEnd {
    /// 候选清空或死信合同完成,本轮完整结束。
    Completed,
    /// 剩余预算截断了数据库步骤;报告按预算耗尽上报,连接交由 Drop 关闭。
    BudgetAbandon,
}

/// 候选行的完整快照：归档需要全部事件字段，删除按主键集合执行。
struct RetentionCandidate {
    id: i64,
    event: OutboxEvent,
}

impl MySqlOutbox {
    /// 业务作用：执行一轮受策略约束的保留清理——已投递行到龄归档/删除，死信只在独立
    /// 批准与收据齐备时清理，待投递行永不触碰。
    ///
    /// 每批一条独立提交的 DELETE；claim 竞争失败、时间预算耗尽或任一步骤不确定都停止
    /// 本轮并如实上报，由受管层退避后重试。归档收据缺失时该行**保留**，绝不先删后补。
    ///
    /// 参数说明：
    /// - `policy`: 已通过 [`OutboxRetentionPolicy::validate`] 的冻结策略。
    /// - `archive`: 归档端；`policy.archive_required` 或 `policy.delete_dead` 时必须提供。
    /// - `now_ms`: 当前 epoch 毫秒，由调用方注入统一时钟。
    ///
    /// 返回：本轮清理结果；策略/归档配置不自洽、连接失败或语句失败返回脱敏错误
    /// （已删除的批次保持已提交，未处理候选自然留在表内）。
    pub async fn retention_round<A>(
        &self,
        policy: &OutboxRetentionPolicy,
        archive: Option<&A>,
        now_ms: i64,
    ) -> Result<RetentionRoundReport, OutboxStoreError>
    where
        A: OutboxArchive + ?Sized,
    {
        policy.validate().map_err(|reason| {
            OutboxStoreError::new(format!("invalid retention policy: {reason}"))
        })?;
        if policy.archive_required && archive.is_none() {
            return Err(OutboxStoreError::new(
                "retention policy requires an archive target but none was provided",
            ));
        }
        // 死信删除无条件要求归档收据:死信是尚未成功投递的业务事实,没有归档副本的删除
        // 等于销毁唯一证据。
        if policy.delete_dead && archive.is_none() {
            return Err(OutboxStoreError::new(
                "dead-letter retention always requires an archive target",
            ));
        }
        if now_ms < 0 {
            return Err(OutboxStoreError::new(
                "retention clock must be non-negative",
            ));
        }

        let mut report = RetentionRoundReport::default();
        // 预算从轮次入口起算:取连接、竞争 claim、会话配置与最终释放都是本轮墙钟的一部分,
        // 只从主体开始计时会让 round_time_budget_ms 不是完整轮次的上界。
        let deadline = Instant::now()
            + std::time::Duration::from_millis(policy.round_time_budget_ms.max(0) as u64);
        // 清理绝不复用 ambient 事务:候选查询、归档核对与逐批 DELETE 必须各自独立提交,
        // 也不允许把发布/归档网络 I/O 圈进业务长事务。
        let connection = match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            natx::conn(),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                // 数据源或连接池明确失败必须进入失败轮次；伪装成预算耗尽会掩盖存储
                // 故障并绕过失败指标。
                return Err(map_err(error));
            }
            Err(_) => {
                // 只有等待连接超过本轮 deadline 才属于预算耗尽；尚未取得任何权威，
                // 返回时没有数据库副作用。
                report.budget_exhausted = true;
                return Ok(report);
            }
        };
        let natx::Conn::Pool(connection) = connection else {
            return Err(OutboxStoreError::new(
                "retention cannot run inside an ambient transaction",
            ));
        };
        let mut claim = RetentionClaim::new(connection);
        // 独立 retention claim:与 dispatcher 的 'nasa-outbox:' 命名空间不同,清理不
        // 持有投递权威;GET_LOCK 超时 0 表示竞争失败立即让路,不等待。
        let now = Instant::now();
        if now >= deadline {
            drop(claim);
            report.budget_exhausted = true;
            return Ok(report);
        }
        // 服务端锁等待参数为 0 只约束锁竞争，不能约束连接层卡顿；客户端 deadline
        // 负责在提交前安全弃连，连接断开会释放可能已取得但未回包的 named lock。
        let acquired = match tokio::time::timeout(deadline - now, async {
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT GET_LOCK(SHA2(CONCAT('nasa-outbox-retention:', DATABASE()), 256), 0)",
            )
            .fetch_one(claim.connection())
            .await
        })
        .await
        {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                drop(claim);
                return Err(map_err(error));
            }
            Err(_) => {
                drop(claim);
                report.budget_exhausted = true;
                return Ok(report);
            }
        };
        if acquired != Some(1) {
            claim.disarm();
            report.claim_contended = true;
            return Ok(report);
        }
        // 收紧锁等待前先捕获原值:连接来自通用业务池,收紧只在本轮有效,归还前由
        // release 复原。捕获/收紧失败即弃连,不让配置状态不明的连接执行清理或回池。
        let now = Instant::now();
        if now >= deadline {
            drop(claim);
            report.budget_exhausted = true;
            return Ok(report);
        }
        match tokio::time::timeout(deadline - now, claim.tighten_lock_wait()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                drop(claim);
                return Err(OutboxStoreError::new(
                    "retention session could not bound its lock wait",
                ));
            }
            Err(_) => {
                // 会话配置尚未完整确认时绝不回池；断开同时释放 retention claim。
                drop(claim);
                report.budget_exhausted = true;
                return Ok(report);
            }
        }

        let outcome = self
            .retention_round_inner(policy, archive, now_ms, deadline, &mut claim, &mut report)
            .await;
        match outcome {
            // 只有本轮完整走完才允许把连接交还池:release 确认会话配置已复原且 named
            // lock 已释放。释放同样受本轮预算约束——超时即弃连,由服务端断开释放锁。
            Ok(InnerEnd::Completed) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if tokio::time::timeout(remaining, claim.release())
                    .await
                    .is_err()
                {
                    report.budget_exhausted = true;
                }
                Ok(report)
            }
            // 预算中断:某个数据库步骤被剩余预算截断,连接上可能残留半途协议状态或
            // 裸事务——弃用连接(Drop 标记关闭),服务端断开时隐式回滚并释放 named
            // lock;绝不把状态不明的连接放回池。
            Ok(InnerEnd::BudgetAbandon) => {
                report.budget_exhausted = true;
                drop(claim);
                Ok(report)
            }
            // 任一错误(含处置事务中途失败):同样弃用连接。裸 BEGIN 不产生 SQLx 事务
            // guard,池归还路径只做 ping,不会替我们回滚——未回滚的处置行、行锁与
            // 事务上下文会被下一位借用者继承。close-on-drop 是唯一可靠的收口。
            Err(error) => {
                drop(claim);
                Err(error)
            }
        }
    }

    /// 业务作用：claim 生效期内的清理主体——已投递行与（可选）死信行的批次循环。
    ///
    /// 参数说明：
    /// - `policy`/`archive`/`now_ms`: 同 [`retention_round`](Self::retention_round)。
    /// - `deadline`: 本轮时间预算的截止时刻。
    /// - `claim`: 已取得的 retention claim。
    /// - `report`: 逐批累计的本轮结果。
    ///
    /// 返回：候选清空、预算耗尽或死信合同完成时返回 `Ok`；任一步骤失败返回脱敏错误，
    /// 已提交批次不回滚。
    async fn retention_round_inner<A>(
        &self,
        policy: &OutboxRetentionPolicy,
        archive: Option<&A>,
        now_ms: i64,
        deadline: Instant,
        claim: &mut RetentionClaim,
        report: &mut RetentionRoundReport,
    ) -> Result<InnerEnd, OutboxStoreError>
    where
        A: OutboxArchive + ?Sized,
    {
        /// 剩余预算内执行一个数据库步骤;超时即预算中断——被截断的连接由外层弃用。
        macro_rules! bounded_step {
            ($fut:expr) => {{
                let now = Instant::now();
                if now >= deadline {
                    return Ok(InnerEnd::BudgetAbandon);
                }
                match tokio::time::timeout(deadline - now, $fut).await {
                    Ok(result) => result?,
                    Err(_) => return Ok(InnerEnd::BudgetAbandon),
                }
            }};
        }
        // 最老候选年龄按各自生命周期时刻的 MIN 聚合计算,与批次顺序、自增 id 顺序
        // 无关:lane 并行投递下 id 序与 dispatched_at 序会背离,"第一行"不是最老;
        // 死信的年龄依据是 dead_at 而不是 created_at。两类候选并存时取最大值。
        let oldest_dispatched: Option<i64> = bounded_step!(async {
            sqlx::query_scalar(
                "SELECT CAST(ROUND((? / 1000.0 - UNIX_TIMESTAMP(MIN(dispatched_at))) * 1000) AS SIGNED) \
                 FROM outbox_event \
                 WHERE dispatched = 1 AND dead = 0 AND dispatched_at IS NOT NULL \
                 AND dispatched_at <= FROM_UNIXTIME((? - ?) / 1000.0)",
            )
            .bind(now_ms)
            .bind(now_ms)
            .bind(policy.dispatched_min_age_ms)
            .fetch_one(claim.connection())
            .await
            .map_err(map_err)
        });
        let oldest_dead: Option<i64> = if policy.delete_dead {
            let dead_min_age = policy
                .dead_min_age_ms
                .expect("validated policy carries dead_min_age_ms");
            bounded_step!(async {
                sqlx::query_scalar(
                    "SELECT CAST(ROUND((? / 1000.0 - UNIX_TIMESTAMP(MIN(dead_at))) * 1000) AS SIGNED) \
                     FROM outbox_event \
                     WHERE dead = 1 AND dead_at IS NOT NULL \
                     AND dead_at <= FROM_UNIXTIME((? - ?) / 1000.0)",
                )
                .bind(now_ms)
                .bind(now_ms)
                .bind(dead_min_age)
                .fetch_one(claim.connection())
                .await
                .map_err(map_err)
            })
        } else {
            None
        };
        report.oldest_candidate_age_ms = match (oldest_dispatched, oldest_dead) {
            (Some(dispatched), Some(dead)) => Some(dispatched.max(dead)),
            (single, None) | (None, single) => single,
        };

        // 阶段一:已投递行。候选=dispatched=1 AND dead=0 且投递时刻早于保留期;
        // 待投递行(dispatched=0 AND dead=0)被条件结构性排除。
        loop {
            if Instant::now() >= deadline {
                return Ok(InnerEnd::BudgetAbandon);
            }
            let candidates = bounded_step!(fetch_candidates(
                claim.connection(),
                "SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, \
                 traceparent, tenant \
                 FROM outbox_event \
                 WHERE dispatched = 1 AND dead = 0 AND dispatched_at IS NOT NULL \
                 AND dispatched_at <= FROM_UNIXTIME((? - ?) / 1000.0) \
                 ORDER BY id ASC LIMIT ?",
                now_ms,
                policy.dispatched_min_age_ms,
                policy.batch_limit,
            ));
            if candidates.is_empty() {
                break;
            }
            let ids = self
                .settle_batch(
                    policy.archive_required,
                    archive,
                    &candidates,
                    deadline,
                    report,
                )
                .await?;
            if ids.is_empty() {
                // 收据缺失:不能再推进已投递阶段,保留候选退避;预算截断按中断弃连。
                return Ok(if report.budget_exhausted {
                    InnerEnd::BudgetAbandon
                } else {
                    InnerEnd::Completed
                });
            }
            // 删除在显式事务内提交:预算截断只可能落在 BEGIN/DELETE 阶段(尚未提交,
            // 弃连即回滚);COMMIT 一旦发出就不再接受客户端截断——取消 COMMIT 无法
            // 回滚服务端可能已经完成的提交,报告会凭空少算已删除的行。
            match delete_rows_committed(
                claim.connection(),
                "dispatched = 1 AND dead = 0",
                &ids,
                deadline,
            )
            .await?
            {
                TxOutcome::Committed(deleted) => report.deleted_dispatched += deleted,
                TxOutcome::BudgetAbandon => return Ok(InnerEnd::BudgetAbandon),
            }
        }

        // 阶段二:死信。默认不清理;独立批准 + 最小年龄 + 归档收据齐备才逐批清理。
        if !policy.delete_dead {
            return Ok(InnerEnd::Completed);
        }
        let dead_min_age = policy
            .dead_min_age_ms
            .expect("validated policy carries dead_min_age_ms");
        loop {
            if Instant::now() >= deadline {
                return Ok(InnerEnd::BudgetAbandon);
            }
            let candidates = bounded_step!(fetch_candidates(
                claim.connection(),
                "SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, \
                 traceparent, tenant \
                 FROM outbox_event \
                 WHERE dead = 1 AND dead_at IS NOT NULL \
                 AND dead_at <= FROM_UNIXTIME((? - ?) / 1000.0) \
                 ORDER BY id ASC LIMIT ?",
                now_ms,
                dead_min_age,
                policy.batch_limit,
            ));
            if candidates.is_empty() {
                return Ok(InnerEnd::Completed);
            }
            // 死信删除无条件要求收据(独立于 archive_required)。
            let ids = self
                .settle_batch(true, archive, &candidates, deadline, report)
                .await?;
            if ids.is_empty() {
                // settle 因预算截断或收据不可得停止收集:预算截断按中断弃连处理。
                return Ok(if report.budget_exhausted {
                    InnerEnd::BudgetAbandon
                } else {
                    InnerEnd::Completed
                });
            }
            // 处置事实与删除同事务提交:每条被清理的死信都留下 (approval, 收据) 的
            // 可复验记录,批准不再只是一个配置串。
            let approval = policy
                .dead_approval
                .as_deref()
                .expect("validated policy carries dead_approval");
            match delete_dead_rows_with_disposal(
                claim.connection(),
                &ids,
                &candidates,
                approval,
                deadline,
            )
            .await?
            {
                TxOutcome::Committed(deleted) => report.deleted_dead += deleted,
                TxOutcome::BudgetAbandon => return Ok(InnerEnd::BudgetAbandon),
            }
        }
    }

    /// 业务作用：为一批候选完成"归档收据在手"的删除前置，返回可安全删除的主键集合。
    ///
    /// 收据获取顺序固定:先 `receipt_of` 重查（回包丢失后的幂等恢复），未归档才真实写入；
    /// 任一行拿不到收据即停止收集（保序：后续行留待下一轮），绝不跳行删除。
    ///
    /// 参数说明：
    /// - `require_receipt`: 本批是否要求收据；不要求时直接返回全部主键。
    /// - `archive`: 归档端。
    /// - `candidates`: 本批候选完整快照。
    /// - `report`: 归档计数累计目标。
    ///
    /// 返回：已满足删除前置的主键集合；归档端不可达返回错误。
    async fn settle_batch<A>(
        &self,
        require_receipt: bool,
        archive: Option<&A>,
        candidates: &[RetentionCandidate],
        deadline: Instant,
        report: &mut RetentionRoundReport,
    ) -> Result<Vec<i64>, OutboxStoreError>
    where
        A: OutboxArchive + ?Sized,
    {
        if !require_receipt {
            return Ok(candidates.iter().map(|row| row.id).collect());
        }
        let archive = archive.ok_or_else(|| {
            OutboxStoreError::new("receipt-gated retention requires an archive target")
        })?;
        let mut cleared = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            // 单轮时间预算贯穿到逐候选归档:预算耗尽即停止收集(已取得收据的前缀仍可
            // 删除,后续候选留待下一轮);归档端悬挂由剩余预算作超时上界,超时按预算
            // 耗尽处理而不是错误——候选原位保留,公开执行器的 round_time_budget_ms
            // 合同不再被慢归档击穿。
            let now = Instant::now();
            if now >= deadline {
                report.budget_exhausted = true;
                break;
            }
            let remaining = deadline - now;
            let settled = tokio::time::timeout(remaining, async {
                // 回包丢失恢复:先重查收据,已归档则不重写;未归档才执行幂等写入。
                let receipt = match archive
                    .receipt_of(&candidate.event.event_id)
                    .await
                    .map_err(|error| OutboxStoreError::new(error.reason.clone()))?
                {
                    Some(receipt) => receipt,
                    None => archive
                        .archive(&candidate.event)
                        .await
                        .map_err(|error| OutboxStoreError::new(error.reason.clone()))?,
                };
                Ok::<_, OutboxStoreError>(receipt)
            })
            .await;
            let receipt = match settled {
                Ok(receipt) => receipt?,
                Err(_) => {
                    report.budget_exhausted = true;
                    break;
                }
            };
            if receipt.event_id != candidate.event.event_id {
                return Err(OutboxStoreError::new(
                    "archive receipt does not match the event identity",
                ));
            }
            report.archived += 1;
            cleared.push(candidate.id);
        }
        Ok(cleared)
    }
}

/// 业务作用：按固定语句拉取一批候选行完整快照，供归档与按主键删除。
///
/// 参数说明：
/// - `connection`: retention claim 绑定的会话。
/// - `sql`: 候选查询（阶段一/二各自固定语句）。
/// - `now_ms`: 统一时钟。
/// - `min_age_ms`: 该阶段最小保留期。
/// - `limit`: 批大小。
///
/// 返回：按 `id` 升序的候选集合；查询失败返回脱敏错误。
async fn fetch_candidates(
    connection: &mut MySqlConnection,
    sql: &'static str,
    now_ms: i64,
    min_age_ms: i64,
    limit: u32,
) -> Result<Vec<RetentionCandidate>, OutboxStoreError> {
    let rows = sqlx::query(sql)
        .bind(now_ms)
        .bind(min_age_ms)
        .bind(limit)
        .fetch_all(connection)
        .await
        .map_err(map_err)?;
    rows.iter()
        .map(|row| {
            Ok(RetentionCandidate {
                id: row.try_get("id").map_err(map_err)?,
                event: OutboxEvent {
                    event_id: row.try_get("event_id").map_err(map_err)?,
                    aggregate_type: row.try_get("aggregate_type").map_err(map_err)?,
                    aggregate_id: row.try_get("aggregate_id").map_err(map_err)?,
                    event_type: row.try_get("event_type").map_err(map_err)?,
                    payload: row.try_get("payload").map_err(map_err)?,
                    traceparent: row.try_get("traceparent").map_err(map_err)?,
                    // 受信租户归因唯一来源是持久列;归档端据此选择隔离空间与授权,
                    // 绝不从 payload 或聚合字段推导身份。
                    tenant: row.try_get("tenant").map_err(map_err)?,
                },
            })
        })
        .collect()
}

/// 业务作用：按已选定主键集合删除一批行，删除条件复验行分类防止越界。
///
/// 每次调用是一条独立提交的 DELETE：锁等待超时或失败时本批放弃、已提交批次不受影响。
/// `IN` 集合来自本轮已选定候选，上限即 `batch_limit`，不存在无上限删除。
///
/// 参数说明：
/// - `connection`: retention claim 绑定的会话。
/// - `class_guard`: 行分类复验条件（`dispatched = 1 AND dead = 0` 或 `dead = 1`）。
/// - `ids`: 本批主键集合。
///
/// 返回：实际删除行数；语句失败返回脱敏错误。
/// 业务作用：在同一显式事务内删除死信行并写入处置事实——批准标识与归档收据身份
/// 随删除一起持久化,人工处置可事后复验;事务中止时两者一起消失,不产生"已删无据"。
///
/// 参数说明：
/// - `connection`: 持有 retention claim 的 MySQL 会话。
/// - `ids`: 已满足收据前置的主键集合。
/// - `candidates`: 本批候选快照(提供 event_id 身份)。
/// - `approval`: 独立批准标识(来自已校验策略)。
///
/// 返回：实际删除的行数;数据库失败返回脱敏错误(事务经会话关闭隐式回滚)。
/// 单个删除事务的收束方式——提交与预算中断是互斥的两种确定结果。
enum TxOutcome {
    /// 服务端已确认提交,行数为已生效事实。
    Committed(u64),
    /// 提交前被剩余预算截断;事务未提交,调用方弃连后由服务端回滚。
    BudgetAbandon,
}

/// 业务作用：在剩余预算内执行一个可取消的事务前语句;超时表示事务尚未提交,
/// 由调用方按预算中断处理。
macro_rules! before_commit {
    ($deadline:expr, $fut:expr) => {{
        let now = Instant::now();
        if now >= $deadline {
            return Ok(TxOutcome::BudgetAbandon);
        }
        match tokio::time::timeout($deadline - now, $fut).await {
            Ok(result) => result?,
            Err(_) => return Ok(TxOutcome::BudgetAbandon),
        }
    }};
}

/// 业务作用：把已投递行的批删除包进显式事务提交——排除语句在连接被弃用后于服务端
/// 迟到提交的可能。
///
/// 预算只截断 `BEGIN`/`DELETE`:这两步之后事务尚未提交,弃连即回滚,结果确定。
/// `COMMIT` 一旦发出就必须等到服务端应答——客户端超时既不能回滚已完成的提交,也
/// 无法区分"服务端未提交"与"已提交但回包丢失",截断它会让报告少算真实删除的行。
/// 因此单轮预算的唯一例外是提交应答本身,其时长由存储刷盘而非锁等待决定。
///
/// 参数说明：
/// - `connection`: 持有 retention claim 的 MySQL 会话。
/// - `class_guard`: 行分类复验条件。
/// - `ids`: 本批主键集合。
/// - `deadline`: 本轮预算截止时刻。
///
/// 返回：提交成功返回已确认删除行数;提交前预算耗尽返回中断;语句失败返回脱敏错误
/// (事务经会话关闭隐式回滚)。
async fn delete_rows_committed(
    connection: &mut MySqlConnection,
    class_guard: &str,
    ids: &[i64],
    deadline: Instant,
) -> Result<TxOutcome, OutboxStoreError> {
    if ids.is_empty() {
        return Ok(TxOutcome::Committed(0));
    }
    before_commit!(deadline, async {
        sqlx::raw_sql("BEGIN")
            .execute(&mut *connection)
            .await
            .map_err(map_err)
    });
    let deleted = before_commit!(deadline, delete_rows(&mut *connection, class_guard, ids));
    sqlx::raw_sql("COMMIT")
        .execute(&mut *connection)
        .await
        .map_err(map_commit_err)?;
    Ok(TxOutcome::Committed(deleted))
}

async fn delete_dead_rows_with_disposal(
    connection: &mut MySqlConnection,
    ids: &[i64],
    candidates: &[RetentionCandidate],
    approval: &str,
    deadline: Instant,
) -> Result<TxOutcome, OutboxStoreError> {
    if ids.is_empty() {
        return Ok(TxOutcome::Committed(0));
    }
    before_commit!(deadline, async {
        sqlx::raw_sql("BEGIN")
            .execute(&mut *connection)
            .await
            .map_err(map_err)
    });
    for candidate in candidates.iter().filter(|row| ids.contains(&row.id)) {
        before_commit!(deadline, async {
            sqlx::query(
                "INSERT INTO outbox_dead_disposal (event_id, approval, receipt_event_id) \
                 VALUES (?, ?, ?) \
                 ON DUPLICATE KEY UPDATE approval = VALUES(approval)",
            )
            .bind(&candidate.event.event_id)
            .bind(approval)
            .bind(&candidate.event.event_id)
            .execute(&mut *connection)
            .await
            .map_err(map_delete_err)
        });
    }
    let deleted = before_commit!(deadline, delete_rows(&mut *connection, "dead = 1", ids));
    // 处置事实与删除同事务提交:提交应答不接受客户端截断,否则会出现"服务端已提交
    // 但报告未计入"的假阴性,人工复验会以为处置未发生。
    sqlx::raw_sql("COMMIT")
        .execute(&mut *connection)
        .await
        .map_err(map_commit_err)?;
    Ok(TxOutcome::Committed(deleted))
}

async fn delete_rows(
    connection: &mut MySqlConnection,
    class_guard: &str,
    ids: &[i64],
) -> Result<u64, OutboxStoreError> {
    if ids.is_empty() {
        return Ok(0);
    }
    let placeholders = vec!["?"; ids.len()].join(", ");
    // 语句骨架来自本文件固定分类条件与占位符序列,id 全部经参数绑定;无外部文本进入 SQL。
    let sql = format!("DELETE FROM outbox_event WHERE {class_guard} AND id IN ({placeholders})");
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for id in ids {
        query = query.bind(id);
    }
    let result = query.execute(connection).await.map_err(map_delete_err)?;
    Ok(result.rows_affected())
}
