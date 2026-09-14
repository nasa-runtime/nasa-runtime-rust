//! Saga 运行指标快照与 Prometheus 文本导出。
//!
//! 业务状态指标来自当前后端的已提交事实；Kafka 处理耗时和 transport 动作是
//! 进程内低基数计数。高基数 saga_id、tenant、workflow 只进结构化日志和审计 API，
//! 禁止进入指标 label。

use std::fmt::Write as _;

use nasaga_backend::SagaStoreMetrics;

/// 业务作用：聚合 Saga 已提交状态与当前进程 Kafka transport 指标。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
    /// 按冻结 workflow/definition version 分组的端到端终态时延分位数。
    pub lifecycle_quantiles: Vec<nasaga_backend::SagaLifecycleQuantiles>,
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
        for quantiles in &self.lifecycle_quantiles {
            labeled_gauge(
                &mut output,
                "nasaga_lifecycle_duration_seconds",
                &quantiles.workflow,
                quantiles.definition_version,
                "0.5",
                quantiles.p50_micros,
            );
            labeled_gauge(
                &mut output,
                "nasaga_lifecycle_duration_seconds",
                &quantiles.workflow,
                quantiles.definition_version,
                "0.95",
                quantiles.p95_micros,
            );
            labeled_gauge(
                &mut output,
                "nasaga_lifecycle_duration_seconds",
                &quantiles.workflow,
                quantiles.definition_version,
                "0.99",
                quantiles.p99_micros,
            );
        }
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
        output.push_str(&crate::render_saga_latency_metrics());
        output
    }
}

impl From<SagaStoreMetrics> for SagaOperationalMetrics {
    /// 业务作用：将后端已提交快照与当前进程 transport 计数组装为对外指标。
    ///
    /// 参数说明：
    /// - `store`: 当前 Saga 后端聚合的已提交事实。
    ///
    /// 返回：包含完整业务与 transport 指标的快照。
    fn from(store: SagaStoreMetrics) -> Self {
        let process = crate::process_metrics_snapshot();
        Self {
            started_total: store.started_total,
            completed_total: store.completed_total,
            compensated_total: store.compensated_total,
            manual_intervention_total: store.manual_intervention_total,
            manually_closed_total: store.manually_closed_total,
            quota_rejections_total: process.quota_rejections_total,
            action_rate_rejections_total: process.action_rate_rejections_total,
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
            lifecycle_quantiles: store.lifecycle_quantiles,
            kafka_result_processing_total: process.kafka_result_processing_total,
            kafka_result_processing_micros_sum: process.kafka_result_processing_micros_sum,
            kafka_result_retry_total: process.kafka_result_retry_total,
            kafka_result_dlt_requested_total: process.kafka_result_dlt_requested_total,
            kafka_result_ack_total: process.kafka_result_ack_total,
            kafka_result_duplicate_total: process.kafka_result_duplicate_total,
            kafka_command_processing_total: process.kafka_command_processing_total,
            kafka_command_processing_micros_sum: process.kafka_command_processing_micros_sum,
            kafka_command_retry_total: process.kafka_command_retry_total,
            kafka_command_dlt_requested_total: process.kafka_command_dlt_requested_total,
            kafka_command_ack_total: process.kafka_command_ack_total,
            kafka_command_duplicate_total: process.kafka_command_duplicate_total,
        }
    }
}

/// 业务作用：渲染带冻结流程版本和固定 quantile 的生命周期 gauge，保持标签集合低基数。
///
/// 参数说明：`output` 是目标文本，`name` 是固定指标名，`workflow` 与 `version` 来自 definition，
/// `quantile` 是固定分位，`micros` 是持久样本值。
///
/// 返回：无返回值；格式写入内存字符串不会向调用方传播失败。
fn labeled_gauge(
    output: &mut String,
    name: &str,
    workflow: &str,
    version: u32,
    quantile: &str,
    micros: u64,
) {
    let workflow = workflow.replace('\\', "\\\\").replace('"', "\\\"");
    let _ = writeln!(
        output,
        "{name}{{workflow=\"{workflow}\",definition_version=\"{version}\",quantile=\"{quantile}\"}} {}",
        micros as f64 / 1_000_000.0
    );
}

/// 业务作用：累计一次按租户配额拒绝的创建请求,供低基数观测面导出。
///
/// 参数说明: 无。
///
/// 返回：无返回值。
pub(crate) fn record_quota_rejection() {
    crate::record_quota_rejection();
}

/// 业务作用：累计一次按租户速率拒绝的变更类管理动作,供低基数观测面导出。
///
/// 参数说明: 无。
///
/// 返回：无返回值。
pub(crate) fn record_action_rate_rejection() {
    crate::record_action_rate_rejection();
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
