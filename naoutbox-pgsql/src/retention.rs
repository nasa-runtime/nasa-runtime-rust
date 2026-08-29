//! PostgreSQL Outbox 保留、归档与清理执行器。
//!
//! retention 使用独立数据库 owner 行，不持有 dispatcher owner。候选读取和删除都复验 fencing；
//! 待投递行被 SQL 条件结构性排除，死信删除还要求独立批准与归档收据。

use std::time::Instant;

use naoutbox_core::{
    ArchiveReceipt, OutboxArchive, OutboxEvent, OutboxRetentionPolicy, RetentionRoundReport,
};
use sqlx::Row as _;

use super::{
    ensure_owner, map_err, OutboxStoreError, OwnerClaim, PendingRow, PgOutbox, OWNER_LEASE_MS,
};

const RETENTION_OWNER_ROLE: &str = "retention";
const RETENTION_OWNER_LANE: &str = "global";
const RETENTION_BUDGET_EXHAUSTED_REASON: &str = "retention round time budget exhausted";

/// PostgreSQL 锁当前不可获得、死锁或语句因锁预算取消时使用的稳定原因。
pub const RETENTION_LOCK_CONTENTION_REASON: &str = "retention storage lock contention";

/// 删除事务 COMMIT 已发出但数据库应答无法确认时使用的稳定原因。
pub const RETENTION_COMMIT_UNCERTAIN_REASON: &str =
    "retention database commit acknowledgement is uncertain";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateKind {
    Dispatched,
    Dead,
}

#[derive(Debug, Clone)]
/// 业务作用：保存保留清理候选的持久行、终态类别与进入该终态的时间证据。
struct RetentionCandidate {
    row: PendingRow,
    kind: CandidateKind,
    lifecycle_at_ms: i64,
}

impl PgOutbox {
    /// 业务作用：执行一轮有界 PostgreSQL Outbox 保留清理，待投递行在任何策略下都不会成为候选。
    ///
    /// 已投递行只在满足最小年龄后删除；需要归档时必须先取得可复验收据。死信还必须具备独立批准、
    /// 最小年龄和归档收据，处置事实与源行删除同事务提交。
    ///
    /// 参数说明：
    /// - `policy`：冻结保留策略。
    /// - `archive`：策略要求归档或删除死信时提供的幂等归档端。
    /// - `now_ms`：调用方注入的统一 epoch 毫秒时钟。
    ///
    /// 返回：只累计已确认提交事实的轮次报告；owner 竞争返回 `claim_contended`，策略、归档或数据库失败返回错误。
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
        if now_ms < 0 {
            return Err(OutboxStoreError::new(
                "retention clock must be non-negative",
            ));
        }
        if policy.archive_required && archive.is_none() {
            return Err(OutboxStoreError::new(
                "retention policy requires an archive target",
            ));
        }
        if policy.delete_dead && archive.is_none() {
            return Err(OutboxStoreError::new(
                "dead-letter retention always requires an archive target",
            ));
        }
        if natx_pgsql::in_transaction() {
            return Err(OutboxStoreError::new(
                "retention cannot run inside an ambient transaction",
            ));
        }

        let deadline =
            Instant::now() + std::time::Duration::from_millis(policy.round_time_budget_ms as u64);
        let lease_ms = policy
            .round_time_budget_ms
            .saturating_add(60_000)
            .max(OWNER_LEASE_MS);
        let mut report = RetentionRoundReport::default();
        // retention 使用独立 role 权威；只有数据库授予 claim 后才允许读取或删除治理候选。
        let claim = match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            self.try_claim_owner(RETENTION_OWNER_ROLE, RETENTION_OWNER_LANE, lease_ms),
        )
        .await
        {
            Ok(Ok(Some(claim))) => claim,
            Ok(Ok(None)) => {
                report.claim_contended = true;
                return Ok(report);
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                report.budget_exhausted = true;
                return Ok(report);
            }
        };

        let outcome = self
            .retention_round_owned(&claim, policy, archive, now_ms, deadline, report)
            .await;
        match outcome {
            Ok(mut report) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    report.budget_exhausted = true;
                    return Ok(report);
                }
                // 受控结束时尽早释放 owner；若释放本身越过预算，不再假定成功，由租约到期门禁兜底。
                match tokio::time::timeout(remaining, claim.release()).await {
                    Ok(Ok(())) => Ok(report),
                    Ok(Err(error)) => Err(error),
                    Err(_) => {
                        report.budget_exhausted = true;
                        Ok(report)
                    }
                }
            }
            Err(error) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if !remaining.is_zero() {
                    // 已知失败同样尝试释放，但释放失败不能覆盖原始原因，租约到期仍会封住旧执行者。
                    let _ = tokio::time::timeout(remaining, claim.release()).await;
                }
                Err(error)
            }
        }
    }

    /// 业务作用：在已取得 retention owner 后执行候选、归档与精确删除主体。
    ///
    /// 参数说明：
    /// - `claim`：本轮 retention owner 权威。
    /// - `policy`：冻结保留策略。
    /// - `archive`：需要收据时使用的幂等归档端。
    /// - `now_ms`：统一 epoch 毫秒时钟。
    /// - `deadline`：包含 owner 竞争与释放在内的墙钟截止时刻。
    /// - `report`：取得 owner 前已经初始化的轮次报告。
    ///
    /// 返回：候选清空或预算耗尽时返回已确认事实；归档、失权或数据库失败返回错误。
    async fn retention_round_owned<A>(
        &self,
        claim: &OwnerClaim,
        policy: &OutboxRetentionPolicy,
        archive: Option<&A>,
        now_ms: i64,
        deadline: Instant,
        mut report: RetentionRoundReport,
    ) -> Result<RetentionRoundReport, OutboxStoreError>
    where
        A: OutboxArchive + ?Sized,
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            report.budget_exhausted = true;
            return Ok(report);
        }
        report.oldest_candidate_age_ms =
            match tokio::time::timeout(remaining, self.oldest_candidate_age(claim, policy, now_ms))
                .await
            {
                Ok(result) => result?,
                Err(_) => {
                    report.budget_exhausted = true;
                    return Ok(report);
                }
            };

        loop {
            if Instant::now() >= deadline {
                report.budget_exhausted = true;
                break;
            }
            let candidates = match tokio::time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                self.fetch_retention_candidates(claim, policy, now_ms),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    report.budget_exhausted = true;
                    break;
                }
            };
            if candidates.is_empty() {
                break;
            }

            let mut receipts = Vec::with_capacity(candidates.len());
            for candidate in &candidates {
                if Instant::now() >= deadline {
                    report.budget_exhausted = true;
                    break;
                }
                let needs_receipt =
                    policy.archive_required || candidate.kind == CandidateKind::Dead;
                let receipt = if needs_receipt {
                    let archive = archive.ok_or_else(|| {
                        OutboxStoreError::new("retention archive target is required")
                    })?;
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        report.budget_exhausted = true;
                        break;
                    }
                    match tokio::time::timeout(
                        remaining,
                        confirm_archive_receipt(archive, &candidate.row.event),
                    )
                    .await
                    {
                        Ok(result) => {
                            let receipt = result?;
                            report.archived = report.archived.saturating_add(1);
                            Some(receipt)
                        }
                        Err(_) => {
                            // 归档调用被截止时不删除源行；下一轮先用 receipt_of 复验远端可能已落地的事实。
                            report.budget_exhausted = true;
                            break;
                        }
                    }
                } else {
                    None
                };
                receipts.push(receipt);
            }
            if receipts.len() != candidates.len() {
                break;
            }

            let deleted = match self
                .delete_retention_candidates(
                    claim,
                    policy,
                    now_ms,
                    deadline,
                    &candidates,
                    &receipts,
                )
                .await
            {
                Ok(deleted) => deleted,
                Err(error)
                    if error.reason == RETENTION_BUDGET_EXHAUSTED_REASON
                        || (error.reason == RETENTION_LOCK_CONTENTION_REASON
                            && Instant::now() >= deadline) =>
                {
                    report.budget_exhausted = true;
                    break;
                }
                Err(error) => return Err(error),
            };
            let mut deleted_any = false;
            for ((candidate, _receipt), was_deleted) in
                candidates.iter().zip(&receipts).zip(deleted)
            {
                if !was_deleted {
                    continue;
                }
                deleted_any = true;
                match candidate.kind {
                    CandidateKind::Dispatched => {
                        report.deleted_dispatched = report.deleted_dispatched.saturating_add(1);
                    }
                    CandidateKind::Dead => {
                        report.deleted_dead = report.deleted_dead.saturating_add(1);
                    }
                }
            }
            if !deleted_any {
                break;
            }
        }

        Ok(report)
    }

    /// 业务作用：在 retention fencing 下读取本轮最老可清理候选年龄。
    ///
    /// 参数说明：
    /// - `claim`：本轮 retention owner 权威。
    /// - `policy`：冻结保留策略。
    /// - `now_ms`：统一 epoch 毫秒时钟。
    ///
    /// 返回：存在候选时返回非负年龄；无候选返回 `None`；失权或数据库失败返回错误。
    async fn oldest_candidate_age(
        &self,
        claim: &OwnerClaim,
        policy: &OutboxRetentionPolicy,
        now_ms: i64,
    ) -> Result<Option<i64>, OutboxStoreError> {
        let mut transaction = claim.pool.begin().await.map_err(map_err)?;
        // 年龄观测会驱动治理决策，先复验 owner，避免失权实例继续产出本轮报告。
        ensure_owner(&mut transaction, claim).await?;
        let dispatched_before = now_ms.saturating_sub(policy.dispatched_min_age_ms);
        let dead_before = policy.dead_min_age_ms.map(|age| now_ms.saturating_sub(age));
        let oldest: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(lifecycle_at_ms) FROM ( \
             SELECT dispatched_at_ms AS lifecycle_at_ms FROM outbox_event \
             WHERE dispatched AND NOT dead AND dispatched_at_ms IS NOT NULL \
               AND dispatched_at_ms <= $1 \
             UNION ALL \
             SELECT dead_at_ms AS lifecycle_at_ms FROM outbox_event \
             WHERE $2 AND dead AND dead_at_ms IS NOT NULL AND dead_at_ms <= $3 \
             ) candidates",
        )
        .bind(dispatched_before)
        .bind(policy.delete_dead)
        .bind(dead_before.unwrap_or(-1))
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_err)?;
        transaction.commit().await.map_err(map_commit_error)?;
        Ok(oldest.map(|value| now_ms.saturating_sub(value).max(0)))
    }

    /// 业务作用：在短事务中复验 retention owner，并用 `FOR UPDATE SKIP LOCKED` 领取有界候选。
    ///
    /// 参数说明：
    /// - `claim`：本轮 retention owner 权威。
    /// - `policy`：冻结保留策略。
    /// - `now_ms`：统一 epoch 毫秒时钟。
    ///
    /// 返回：按生命周期与 `id` 稳定排序的候选；失权、提交不确定或数据库失败返回错误。
    async fn fetch_retention_candidates(
        &self,
        claim: &OwnerClaim,
        policy: &OutboxRetentionPolicy,
        now_ms: i64,
    ) -> Result<Vec<RetentionCandidate>, OutboxStoreError> {
        let mut transaction = claim.pool.begin().await.map_err(map_err)?;
        // 在候选加锁前先锁定 owner 行，确保本轮领取与权威复验属于同一数据库事务。
        ensure_owner(&mut transaction, claim).await?;
        let dispatched_before = now_ms.saturating_sub(policy.dispatched_min_age_ms);
        let dead_before = policy
            .dead_min_age_ms
            .map(|age| now_ms.saturating_sub(age))
            .unwrap_or(-1);
        let rows = sqlx::query(
            "SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, traceparent, \
             tenant, CASE WHEN dead THEN 1 ELSE 0 END AS candidate_kind, \
             CASE WHEN dead THEN dead_at_ms ELSE dispatched_at_ms END AS lifecycle_at_ms \
             FROM outbox_event WHERE \
             (dispatched AND NOT dead AND dispatched_at_ms IS NOT NULL AND dispatched_at_ms <= $1) \
             OR ($2 AND dead AND dead_at_ms IS NOT NULL AND dead_at_ms <= $3) \
             ORDER BY lifecycle_at_ms ASC, id ASC LIMIT $4 FOR UPDATE SKIP LOCKED",
        )
        .bind(dispatched_before)
        .bind(policy.delete_dead)
        .bind(dead_before)
        .bind(i64::from(policy.batch_limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_delete_error)?;
        transaction.commit().await.map_err(map_commit_error)?;

        rows.into_iter()
            .map(|row| {
                let kind: i32 = row.try_get("candidate_kind").map_err(map_err)?;
                let lifecycle_at_ms: i64 = row.try_get("lifecycle_at_ms").map_err(map_err)?;
                let pending = PendingRow {
                    id: row.try_get("id").map_err(map_err)?,
                    event: OutboxEvent {
                        event_id: row.try_get("event_id").map_err(map_err)?,
                        aggregate_type: row.try_get("aggregate_type").map_err(map_err)?,
                        aggregate_id: row.try_get("aggregate_id").map_err(map_err)?,
                        event_type: row.try_get("event_type").map_err(map_err)?,
                        payload: row.try_get("payload").map_err(map_err)?,
                        traceparent: row.try_get("traceparent").map_err(map_err)?,
                        tenant: row.try_get("tenant").map_err(map_err)?,
                    },
                };
                Ok(RetentionCandidate {
                    row: pending,
                    kind: if kind == 1 {
                        CandidateKind::Dead
                    } else {
                        CandidateKind::Dispatched
                    },
                    lifecycle_at_ms,
                })
            })
            .collect()
    }

    /// 业务作用：在同一 fenced 事务中写死信处置证据并按精确身份删除仍符合策略的候选。
    ///
    /// 参数说明：
    /// - `claim`：本轮 retention owner 权威。
    /// - `policy`：冻结保留策略。
    /// - `now_ms`：统一 epoch 毫秒时钟。
    /// - `deadline`：本轮墙钟截止时刻，用于收紧事务级锁等待与语句执行上限。
    /// - `candidates`：已经按批次领取并完成必要归档的候选。
    /// - `receipts`：与候选一一对应的可选归档收据。
    ///
    /// 返回：与候选一一对应的删除结果；失权、收据缺失、提交不确定或数据库失败返回错误。
    async fn delete_retention_candidates(
        &self,
        claim: &OwnerClaim,
        policy: &OutboxRetentionPolicy,
        now_ms: i64,
        deadline: Instant,
        candidates: &[RetentionCandidate],
        receipts: &[Option<ArchiveReceipt>],
    ) -> Result<Vec<bool>, OutboxStoreError> {
        let remaining_ms = remaining_millis(deadline)
            .ok_or_else(|| OutboxStoreError::new(RETENTION_BUDGET_EXHAUSTED_REASON))?;
        let mut transaction = claim.pool.begin().await.map_err(map_err)?;
        let lock_wait_ms = remaining_ms.min(5_000);
        sqlx::query("SELECT set_config('lock_timeout', $1, true)")
            .bind(format!("{lock_wait_ms}ms"))
            .execute(&mut *transaction)
            .await
            .map_err(map_delete_error)?;
        sqlx::query("SELECT set_config('statement_timeout', $1, true)")
            .bind(format!("{remaining_ms}ms"))
            .execute(&mut *transaction)
            .await
            .map_err(map_delete_error)?;
        // 删除与处置证据是不可逆治理动作，必须在已收紧超时的同一事务内再次复验 fencing。
        ensure_owner(&mut transaction, claim).await?;
        let mut deleted = Vec::with_capacity(candidates.len());
        for (candidate, receipt) in candidates.iter().zip(receipts) {
            let affected = match candidate.kind {
                CandidateKind::Dispatched => {
                    if policy.archive_required && receipt.is_none() {
                        return Err(OutboxStoreError::new(
                            "dispatched retention requires an archive receipt",
                        ));
                    }
                    sqlx::query(
                        "DELETE FROM outbox_event WHERE id = $1 AND event_id = $2 \
                         AND dispatched AND NOT dead AND dispatched_at_ms = $3 \
                         AND dispatched_at_ms <= $4",
                    )
                    .bind(candidate.row.id)
                    .bind(&candidate.row.event.event_id)
                    .bind(candidate.lifecycle_at_ms)
                    .bind(now_ms.saturating_sub(policy.dispatched_min_age_ms))
                    .execute(&mut *transaction)
                    .await
                    .map_err(map_delete_error)?
                    .rows_affected()
                }
                CandidateKind::Dead => {
                    let receipt = receipt.as_ref().ok_or_else(|| {
                        OutboxStoreError::new("dead-letter retention requires an archive receipt")
                    })?;
                    let approval = policy.dead_approval.as_deref().ok_or_else(|| {
                        OutboxStoreError::new("dead-letter retention requires approval")
                    })?;
                    let affected = sqlx::query(
                        "DELETE FROM outbox_event WHERE id = $1 AND event_id = $2 \
                         AND dead AND NOT dispatched AND dead_at_ms = $3 AND dead_at_ms <= $4",
                    )
                    .bind(candidate.row.id)
                    .bind(&candidate.row.event.event_id)
                    .bind(candidate.lifecycle_at_ms)
                    .bind(now_ms.saturating_sub(policy.dead_min_age_ms.unwrap_or(i64::MAX)))
                    .execute(&mut *transaction)
                    .await
                    .map_err(map_delete_error)?
                    .rows_affected();
                    if affected == 1 {
                        // 处置证据与源行删除共享事务，不能留下“已处置但源行仍存在”的半完成状态。
                        sqlx::query(
                            "INSERT INTO outbox_dead_disposal \
                             (event_id, approval, receipt_event_id, disposed_at_ms) \
                             VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO UPDATE SET \
                             approval = EXCLUDED.approval, receipt_event_id = EXCLUDED.receipt_event_id, \
                             disposed_at_ms = EXCLUDED.disposed_at_ms",
                        )
                        .bind(&candidate.row.event.event_id)
                        .bind(approval)
                        .bind(&receipt.event_id)
                        .bind(now_ms)
                        .execute(&mut *transaction)
                        .await
                        .map_err(map_delete_error)?;
                    }
                    affected
                }
            };
            deleted.push(affected == 1);
        }
        transaction.commit().await.map_err(map_commit_error)?;
        Ok(deleted)
    }
}

/// 业务作用：取得或复验某事件的幂等归档收据，回包丢失时先重查而不盲目重写。
///
/// 参数说明：
/// - `archive`：幂等归档端。
/// - `event`：待取得归档证据的完整事件。
///
/// 返回：收据身份与事件一致时成功；归档失败、缺失或身份不一致返回脱敏错误。
async fn confirm_archive_receipt<A>(
    archive: &A,
    event: &OutboxEvent,
) -> Result<ArchiveReceipt, OutboxStoreError>
where
    A: OutboxArchive + ?Sized,
{
    let receipt = match archive.receipt_of(&event.event_id).await {
        Ok(Some(receipt)) => receipt,
        Ok(None) => archive
            .archive(event)
            .await
            .map_err(|error| OutboxStoreError::new(error.reason))?,
        Err(error) => return Err(OutboxStoreError::new(error.reason)),
    };
    if receipt.event_id != event.event_id {
        return Err(OutboxStoreError::new(
            "archive receipt identity does not match the outbox event",
        ));
    }
    Ok(receipt)
}

/// 业务作用：把剩余墙钟预算转换为 PostgreSQL 可接受的正毫秒上限。
///
/// 参数说明：
/// - `deadline`：本轮统一截止时刻。
///
/// 返回：尚有预算时返回至少一毫秒且不超过 `i64` 的上限；截止后返回 `None`。
fn remaining_millis(deadline: Instant) -> Option<i64> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    if remaining.is_zero() {
        return None;
    }
    Some(remaining.as_millis().max(1).min(i64::MAX as u128) as i64)
}

/// 业务作用：把候选/删除 SQL 的 PostgreSQL 错误按锁竞争与通用存储失败分类。
///
/// 参数说明：
/// - `error`：SQLx 数据库错误。
///
/// 返回：锁不可用、死锁或查询取消映射为稳定竞争原因；其它错误返回通用脱敏分类。
fn map_delete_error(error: sqlx::Error) -> OutboxStoreError {
    if error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| matches!(code.as_ref(), "55P03" | "40P01" | "57014"))
    {
        OutboxStoreError::new(RETENTION_LOCK_CONTENTION_REASON)
    } else {
        map_err(error)
    }
}

/// 业务作用：把 retention COMMIT 错误区分为明确拒绝、应答不确定与基础设施失败。
///
/// 参数说明：
/// - `error`：SQLx 提交阶段返回的错误。
///
/// 返回：不含 SQL、endpoint 或事件内容的稳定错误。
fn map_commit_error(error: sqlx::Error) -> OutboxStoreError {
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
