//! Saga 运行指标的已提交事实聚合。
//!
//! 生命周期、Unknown、Manual 与冲突计数直接来自 MySQL，避免进程在事务
//! COMMIT 前崩溃时留下虚假成功指标。查询只返回低基数聚合，不暴露 tenant、saga_id
//! 或 step 等高基数标签。

use sqlx::Row as _;

use crate::error::{corrupt, map_connection, map_database, SagaStoreError};
use crate::MySqlSagaStore;

/// 业务作用：表示 MySQL 中可重建的 Saga 运行指标快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SagaStoreMetrics {
    /// 历史创建实例数。
    pub started_total: u64,
    /// 进入 `COMPLETED` 的历史迁移数。
    pub completed_total: u64,
    /// 进入 `COMPENSATED` 的历史迁移数。
    pub compensated_total: u64,
    /// 进入 `MANUAL_INTERVENTION` 的历史迁移数。
    pub manual_intervention_total: u64,
    /// 参与方报告 `UNKNOWN` 的历史 attempt 数。
    pub unknown_result_total: u64,
    /// attempt 号大于 1 的历史重试数。
    pub retry_attempt_total: u64,
    /// 已持久化的互斥事实数。
    pub conflict_total: u64,
    /// 当前正向执行实例数。
    pub running_current: u64,
    /// 当前等待 Unknown 裁决实例数。
    pub waiting_resolution_current: u64,
    /// 当前执行补偿实例数。
    pub compensating_current: u64,
    /// 当前人工介入实例数。
    pub manual_intervention_current: u64,
    /// 当前可领取且已到期的 durable timer 数。
    pub due_timer_current: u64,
    /// 已终结实例的持久化生命周期样本数。
    pub lifecycle_duration_count: u64,
    /// 已终结实例从创建到最后状态更新的累计微秒数。
    pub lifecycle_duration_micros_sum: u64,
}

impl MySqlSagaStore {
    /// 业务作用：从已提交的 Saga 表聚合一份低基数运行指标快照。
    ///
    /// 参数说明：
    /// - `now_ms`: 当前 epoch 毫秒，用于计算已到期且可领取的 timer。
    ///
    /// 返回：查询成功返回可重建指标；时钟或数据库失败返回错误。
    pub async fn load_operational_metrics(
        &self,
        now_ms: i64,
    ) -> Result<SagaStoreMetrics, SagaStoreError> {
        if now_ms < 0 {
            return Err(SagaStoreError::new(
                "Saga metrics time must be non-negative",
            ));
        }
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let row = sqlx::query(
            "SELECT \
             CAST((SELECT COUNT(*) FROM saga_instance) AS SIGNED) AS started_total, \
             CAST((SELECT COUNT(*) FROM saga_transition WHERE to_state = 'COMPLETED') AS SIGNED) AS completed_total, \
             CAST((SELECT COUNT(*) FROM saga_transition WHERE to_state = 'COMPENSATED') AS SIGNED) AS compensated_total, \
             CAST((SELECT COUNT(*) FROM saga_transition WHERE to_state = 'MANUAL_INTERVENTION') AS SIGNED) AS manual_total, \
             CAST((SELECT COUNT(*) FROM saga_step_attempt WHERE status = 'UNKNOWN') AS SIGNED) AS unknown_total, \
             CAST((SELECT COUNT(*) FROM saga_step_attempt WHERE attempt_no > 1) AS SIGNED) AS retry_total, \
             CAST((SELECT COUNT(*) FROM saga_conflict_fact) AS SIGNED) AS conflict_total, \
             CAST((SELECT COUNT(*) FROM saga_instance WHERE status = 'RUNNING') AS SIGNED) AS running_current, \
             CAST((SELECT COUNT(*) FROM saga_instance WHERE status = 'WAITING_RESOLUTION') AS SIGNED) AS waiting_current, \
             CAST((SELECT COUNT(*) FROM saga_instance WHERE status = 'COMPENSATING') AS SIGNED) AS compensating_current, \
             CAST((SELECT COUNT(*) FROM saga_instance WHERE status = 'MANUAL_INTERVENTION') AS SIGNED) AS manual_current, \
             CAST((SELECT COUNT(*) FROM saga_timer WHERE state = 'READY' AND available_at <= ?) AS SIGNED) AS due_timer_current, \
             CAST((SELECT COUNT(*) FROM saga_instance WHERE status IN ('COMPLETED', 'COMPENSATED', 'FAILED')) AS SIGNED) AS duration_count, \
             CAST((SELECT COALESCE(SUM(TIMESTAMPDIFF(MICROSECOND, created_at, updated_at)), 0) \
                FROM saga_instance WHERE status IN ('COMPLETED', 'COMPENSATED', 'FAILED')) AS SIGNED) AS duration_micros_sum",
        )
        .bind(now_ms)
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;

        Ok(SagaStoreMetrics {
            started_total: metric(&row, "started_total")?,
            completed_total: metric(&row, "completed_total")?,
            compensated_total: metric(&row, "compensated_total")?,
            manual_intervention_total: metric(&row, "manual_total")?,
            unknown_result_total: metric(&row, "unknown_total")?,
            retry_attempt_total: metric(&row, "retry_total")?,
            conflict_total: metric(&row, "conflict_total")?,
            running_current: metric(&row, "running_current")?,
            waiting_resolution_current: metric(&row, "waiting_current")?,
            compensating_current: metric(&row, "compensating_current")?,
            manual_intervention_current: metric(&row, "manual_current")?,
            due_timer_current: metric(&row, "due_timer_current")?,
            lifecycle_duration_count: metric(&row, "duration_count")?,
            lifecycle_duration_micros_sum: metric(&row, "duration_micros_sum")?,
        })
    }
}

/// 业务作用：从 MySQL 聚合行中安全解码非负计数。
///
/// 参数说明：
/// - `row`: 聚合查询结果。
/// - `column`: 固定 SQL 中的低基数列别名。
///
/// 返回：MySQL 显式 `SIGNED` 聚合可解码且非负时返回 `u64`；类型漂移、负值或溢出
/// 按持久化损坏返回错误。
fn metric(row: &sqlx::mysql::MySqlRow, column: &str) -> Result<u64, SagaStoreError> {
    let value: i64 = row
        .try_get(column)
        .map_err(|_| corrupt("Saga metrics aggregate"))?;
    u64::try_from(value).map_err(|_| corrupt("Saga metrics aggregate"))
}
