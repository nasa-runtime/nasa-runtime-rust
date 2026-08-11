//! Saga 运行指标快照与 Prometheus 文本导出。
//!
//! 业务状态指标来自 MySQL 已提交事实；Kafka 处理耗时和 transport 动作是
//! 进程内低基数计数。高基数 saga_id、tenant、workflow 只进结构化日志和审计 API，
//! 禁止进入指标 label。

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "kafka")]
use std::time::Duration;

use nasaga_mysql::SagaStoreMetrics;

static KAFKA_RESULT_PROCESSING_TOTAL: AtomicU64 = AtomicU64::new(0);
/// 按租户配额拒绝的创建请求进程累计。
static QUOTA_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static ACTION_RATE_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_PROCESSING_MICROS: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_DLT_REQUESTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_ACK_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_RESULT_DUPLICATE_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_PROCESSING_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_PROCESSING_MICROS: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_DLT_REQUESTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_ACK_TOTAL: AtomicU64 = AtomicU64::new(0);
static KAFKA_COMMAND_DUPLICATE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// 业务作用：聚合 Saga 已提交状态与当前进程 Kafka transport 指标。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SagaOperationalMetrics {
    /// 历史创建实例数。
    pub started_total: u64,
    /// 进入 `COMPLETED` 的历史迁移数。
    pub completed_total: u64,
    /// 进入 `COMPENSATED` 的历史迁移数。
    pub compensated_total: u64,
    /// 进入 `MANUAL_INTERVENTION` 的历史迁移数。
    pub manual_intervention_total: u64,
    /// 进入 `MANUALLY_CLOSED` 的历史迁移数（系统外处置后人工关闭自动化）。
    pub manually_closed_total: u64,
    /// 当前进程按租户配额拒绝的创建请求数（不携带租户标签,精确用量走受鉴权管理查询）。
    pub quota_rejections_total: u64,
    /// 当前进程按租户速率拒绝的变更类管理动作数（不携带租户标签,当前窗口用量走
    /// 受鉴权管理查询）。
    pub action_rate_rejections_total: u64,
    /// 历史 Unknown attempt 数。
    pub unknown_result_total: u64,
    /// 历史业务 attempt 重试数。
    pub retry_attempt_total: u64,
    /// 历史互斥事实数。
    pub conflict_total: u64,
    /// 当前正向运行实例数。
    pub running_current: u64,
    /// 当前等待 Unknown 裁决实例数。
    pub waiting_resolution_current: u64,
    /// 当前补偿中实例数。
    pub compensating_current: u64,
    /// 当前人工介入实例数。
    pub manual_intervention_current: u64,
    /// 当前已到期且可领取 timer 数。
    pub due_timer_current: u64,
    /// 终态生命周期样本数。
    pub lifecycle_duration_count: u64,
    /// 终态生命周期累计微秒数。
    pub lifecycle_duration_micros_sum: u64,
    /// 当前进程处理 Saga result 次数。
    pub kafka_result_processing_total: u64,
    /// 当前进程处理 Saga result 累计微秒数。
    pub kafka_result_processing_micros_sum: u64,
    /// 当前进程请求 Kafka 保留 offset 重投的次数。
    pub kafka_result_retry_total: u64,
    /// 当前进程请求 `nafka` 进入 durability-first DLT 的次数。
    pub kafka_result_dlt_requested_total: u64,
    /// 当前进程在本地事务提交后完成手动 ACK 的次数。
    pub kafka_result_ack_total: u64,
    /// 当前进程由 Inbox 幂等吸收后 ACK 的重复结果数。
    pub kafka_result_duplicate_total: u64,
    /// 当前进程处理 Saga command 次数。
    pub kafka_command_processing_total: u64,
    /// 当前进程处理 Saga command 累计微秒数。
    pub kafka_command_processing_micros_sum: u64,
    /// 当前进程因 Participant 事务未提交而保留 command offset 的次数。
    pub kafka_command_retry_total: u64,
    /// 当前进程请求 command 进入 durability-first DLT 的次数。
    pub kafka_command_dlt_requested_total: u64,
    /// 当前进程在 Participant COMMIT 后完成手动 ACK 的 command 数。
    pub kafka_command_ack_total: u64,
    /// 当前进程由 Participant Inbox 幂等吸收后 ACK 的重复 command 数。
    pub kafka_command_duplicate_total: u64,
}

impl SagaOperationalMetrics {
    /// 业务作用：把低基数快照渲染为 Prometheus text exposition 格式。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可直接作为 `/metrics` 响应体的 UTF-8 文本，不包含高基数标签。
    pub fn render_prometheus(&self) -> String {
        let mut output = String::with_capacity(2_048);
        counter(&mut output, "nasaga_started_total", self.started_total);
        counter(&mut output, "nasaga_completed_total", self.completed_total);
        counter(
            &mut output,
            "nasaga_compensated_total",
            self.compensated_total,
        );
        counter(
            &mut output,
            "nasaga_manual_intervention_total",
            self.manual_intervention_total,
        );
        counter(
            &mut output,
            "nasaga_manually_closed_total",
            self.manually_closed_total,
        );
        counter(
            &mut output,
            "nasaga_quota_rejections_total",
            self.quota_rejections_total,
        );
        counter(
            &mut output,
            "nasaga_action_rate_rejections_total",
            self.action_rate_rejections_total,
        );
        counter(
            &mut output,
            "nasaga_unknown_result_total",
            self.unknown_result_total,
        );
        counter(
            &mut output,
            "nasaga_retry_attempt_total",
            self.retry_attempt_total,
        );
        counter(&mut output, "nasaga_conflict_total", self.conflict_total);
        gauge(&mut output, "nasaga_running", self.running_current);
        gauge(
            &mut output,
            "nasaga_waiting_resolution",
            self.waiting_resolution_current,
        );
        gauge(
            &mut output,
            "nasaga_compensating",
            self.compensating_current,
        );
        gauge(
            &mut output,
            "nasaga_manual_intervention",
            self.manual_intervention_current,
        );
        gauge(&mut output, "nasaga_due_timer", self.due_timer_current);
        summary(
            &mut output,
            "nasaga_lifecycle_duration_seconds",
            self.lifecycle_duration_count,
            self.lifecycle_duration_micros_sum,
        );
        summary(
            &mut output,
            "nasaga_kafka_result_processing_duration_seconds",
            self.kafka_result_processing_total,
            self.kafka_result_processing_micros_sum,
        );
        counter(
            &mut output,
            "nasaga_kafka_result_retry_total",
            self.kafka_result_retry_total,
        );
        counter(
            &mut output,
            "nasaga_kafka_result_dlt_requested_total",
            self.kafka_result_dlt_requested_total,
        );
        counter(
            &mut output,
            "nasaga_kafka_result_ack_total",
            self.kafka_result_ack_total,
        );
        counter(
            &mut output,
            "nasaga_kafka_result_duplicate_total",
            self.kafka_result_duplicate_total,
        );
        summary(
            &mut output,
            "nasaga_kafka_command_processing_duration_seconds",
            self.kafka_command_processing_total,
            self.kafka_command_processing_micros_sum,
        );
        counter(
            &mut output,
            "nasaga_kafka_command_retry_total",
            self.kafka_command_retry_total,
        );
        counter(
            &mut output,
            "nasaga_kafka_command_dlt_requested_total",
            self.kafka_command_dlt_requested_total,
        );
        counter(
            &mut output,
            "nasaga_kafka_command_ack_total",
            self.kafka_command_ack_total,
        );
        counter(
            &mut output,
            "nasaga_kafka_command_duplicate_total",
            self.kafka_command_duplicate_total,
        );
        output
    }
}

impl From<SagaStoreMetrics> for SagaOperationalMetrics {
    /// 业务作用：将 MySQL 已提交快照与当前进程 transport 计数组装为对外指标。
    ///
    /// 参数说明：
    /// - `store`: MySQL 聚合的已提交事实。
    ///
    /// 返回：包含完整业务与 transport 指标的快照。
    fn from(store: SagaStoreMetrics) -> Self {
        Self {
            started_total: store.started_total,
            completed_total: store.completed_total,
            compensated_total: store.compensated_total,
            manual_intervention_total: store.manual_intervention_total,
            manually_closed_total: store.manually_closed_total,
            quota_rejections_total: QUOTA_REJECTIONS_TOTAL.load(Ordering::Relaxed),
            action_rate_rejections_total: ACTION_RATE_REJECTIONS_TOTAL.load(Ordering::Relaxed),
            unknown_result_total: store.unknown_result_total,
            retry_attempt_total: store.retry_attempt_total,
            conflict_total: store.conflict_total,
            running_current: store.running_current,
            waiting_resolution_current: store.waiting_resolution_current,
            compensating_current: store.compensating_current,
            manual_intervention_current: store.manual_intervention_current,
            due_timer_current: store.due_timer_current,
            lifecycle_duration_count: store.lifecycle_duration_count,
            lifecycle_duration_micros_sum: store.lifecycle_duration_micros_sum,
            kafka_result_processing_total: KAFKA_RESULT_PROCESSING_TOTAL.load(Ordering::Relaxed),
            kafka_result_processing_micros_sum: KAFKA_RESULT_PROCESSING_MICROS
                .load(Ordering::Relaxed),
            kafka_result_retry_total: KAFKA_RESULT_RETRY_TOTAL.load(Ordering::Relaxed),
            kafka_result_dlt_requested_total: KAFKA_RESULT_DLT_REQUESTED_TOTAL
                .load(Ordering::Relaxed),
            kafka_result_ack_total: KAFKA_RESULT_ACK_TOTAL.load(Ordering::Relaxed),
            kafka_result_duplicate_total: KAFKA_RESULT_DUPLICATE_TOTAL.load(Ordering::Relaxed),
            kafka_command_processing_total: KAFKA_COMMAND_PROCESSING_TOTAL.load(Ordering::Relaxed),
            kafka_command_processing_micros_sum: KAFKA_COMMAND_PROCESSING_MICROS
                .load(Ordering::Relaxed),
            kafka_command_retry_total: KAFKA_COMMAND_RETRY_TOTAL.load(Ordering::Relaxed),
            kafka_command_dlt_requested_total: KAFKA_COMMAND_DLT_REQUESTED_TOTAL
                .load(Ordering::Relaxed),
            kafka_command_ack_total: KAFKA_COMMAND_ACK_TOTAL.load(Ordering::Relaxed),
            kafka_command_duplicate_total: KAFKA_COMMAND_DUPLICATE_TOTAL.load(Ordering::Relaxed),
        }
    }
}

/// 业务作用：累计一次按租户配额拒绝的创建请求,供低基数观测面导出。
///
/// 参数说明: 无。
///
/// 返回：无返回值。
pub(crate) fn record_quota_rejection() {
    QUOTA_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：累计一次按租户速率拒绝的变更类管理动作,供低基数观测面导出。
///
/// 参数说明: 无。
///
/// 返回：无返回值。
pub(crate) fn record_action_rate_rejection() {
    ACTION_RATE_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录一次 Kafka Saga result 的端到端 handler 耗时。
///
/// 参数说明：
/// - `elapsed`: 从 transport 认证开始到本地事务结束的单调耗时。
///
/// 返回：无；计数以饱和加法更新，不阻塞业务线程。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_processing(elapsed: Duration) {
    KAFKA_RESULT_PROCESSING_TOTAL.fetch_add(1, Ordering::Relaxed);
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let _ = KAFKA_RESULT_PROCESSING_MICROS.fetch_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |current| Some(current.saturating_add(micros)),
    );
}

/// 业务作用：记录 Saga result 因事务未提交而保留 offset 重投。
///
/// 参数说明: 无。
///
/// 返回：无。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_retry() {
    KAFKA_RESULT_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Saga result 已请求 `nafka` 进入 durability-first DLT。
///
/// 参数说明: 无。
///
/// 返回：无；真正 broker 成功数由 `nafka` 的 `dlt_total` 指标确认。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_dlt_requested() {
    KAFKA_RESULT_DLT_REQUESTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 MySQL 提交后已完成的 Kafka 手动 ACK。
///
/// 参数说明：
/// - `duplicate`: 本次是否由 Inbox 吸收的重复结果。
///
/// 返回：无。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_ack(duplicate: bool) {
    KAFKA_RESULT_ACK_TOTAL.fetch_add(1, Ordering::Relaxed);
    if duplicate {
        KAFKA_RESULT_DUPLICATE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// 业务作用：记录一次 Kafka Saga command 从身份门禁到 Participant 提交的处理耗时。
///
/// 参数说明：
/// - `elapsed`: transport 处理的单调耗时。
///
/// 返回：无；使用饱和累计且不阻塞业务线程。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_command_processing(elapsed: Duration) {
    KAFKA_COMMAND_PROCESSING_TOTAL.fetch_add(1, Ordering::Relaxed);
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let _ = KAFKA_COMMAND_PROCESSING_MICROS.fetch_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |current| Some(current.saturating_add(micros)),
    );
}

/// 业务作用：记录 Saga command 因本地事务未提交而保留 offset 重投。
///
/// 参数说明: 无。
///
/// 返回：无。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_command_retry() {
    KAFKA_COMMAND_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Saga command 已请求 `nafka` 进入 durability-first DLT。
///
/// 参数说明: 无。
///
/// 返回：无；broker durable 成功仍由 `nafka` 指标证明。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_command_dlt_requested() {
    KAFKA_COMMAND_DLT_REQUESTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// 业务作用：记录 Participant COMMIT 后完成的 command 手动 ACK 与 Inbox 重复吸收。
///
/// 参数说明：
/// - `duplicate`: 本次是否由 Participant Inbox 吸收重复 command。
///
/// 返回：无。
#[cfg(feature = "kafka")]
pub(crate) fn record_kafka_command_ack(duplicate: bool) {
    KAFKA_COMMAND_ACK_TOTAL.fetch_add(1, Ordering::Relaxed);
    if duplicate {
        KAFKA_COMMAND_DUPLICATE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// 业务作用：输出一个无 label 的 Prometheus counter。
fn counter(output: &mut String, name: &str, value: u64) {
    let _ = writeln!(output, "# TYPE {name} counter\n{name} {value}");
}

/// 业务作用：输出一个无 label 的 Prometheus gauge。
fn gauge(output: &mut String, name: &str, value: u64) {
    let _ = writeln!(output, "# TYPE {name} gauge\n{name} {value}");
}

/// 业务作用：以 Prometheus summary 的 `_count`/`_sum` 形式输出累计耗时。
fn summary(output: &mut String, name: &str, count: u64, micros_sum: u64) {
    let seconds_sum = micros_sum as f64 / 1_000_000.0;
    let _ = writeln!(
        output,
        "# TYPE {name} summary\n{name}_count {count}\n{name}_sum {seconds_sum:.6}"
    );
}
