//! Saga store 可重建低基数指标快照。

/// 业务作用：表示任意数据库后端中可从已提交事实重建的 Saga 指标快照。
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
    /// 进入 `MANUALLY_CLOSED` 的历史迁移数。
    pub manually_closed_total: u64,
    /// 参与方报告 `UNKNOWN` 的历史 attempt 数。
    pub unknown_result_total: u64,
    /// attempt 号大于一的历史重试数。
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
    /// 已终结实例生命周期累计微秒数。
    pub lifecycle_duration_micros_sum: u64,
}
