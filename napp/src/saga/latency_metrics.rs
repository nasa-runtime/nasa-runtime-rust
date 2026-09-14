//! Saga 阶段耗时在 Application 指标目录中的唯一适配。

use super::nasaga_runtime;
use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};

static DURATION: MetricDescriptor = MetricDescriptor {
    name: "nasaga_stage_duration_seconds",
    help: "Saga 阶段的实际占用时间，包含失败和取消尝试；timer_lateness 表示到期后的调度滞后。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["stage"],
    histogram_bounds: nasaga_runtime::SAGA_LATENCY_BUCKETS,
};
static DESCRIPTORS: [&MetricDescriptor; 1] = [&DURATION];

/// 业务作用：让所有 Saga 角色将相同阶段边界接入 Prometheus 与 OTLP。
pub(super) struct SagaLatencyMetricsSource;

impl LegacyMetricsSource for SagaLatencyMetricsSource {
    /// 业务作用：登记唯一耗时 family 与固定阶段标签。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：编译期固定 descriptor，冲突时由指标目录拒绝启动。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &DESCRIPTORS
    }

    /// 业务作用：读取同源非累积直方图供全部 exporter 使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定五个阶段的完整桶、总量和计数，微秒换算为秒。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        Some(
            nasaga_runtime::saga_latency_snapshot()
                .into_iter()
                .map(|sample| MetricSample {
                    name: DURATION.name,
                    labels: vec![("stage", sample.stage.to_owned())],
                    value: MetricValue::Histogram {
                        bounds: nasaga_runtime::SAGA_LATENCY_BUCKETS,
                        buckets: sample.buckets,
                        sum: sample.micros_sum as f64 / 1_000_000.0,
                        count: sample.count,
                    },
                })
                .collect(),
        )
    }

    /// 业务作用：为直接文本调用提供与结构化快照一致的阶段指标。
    ///
    /// 参数说明：`output` 接收 Prometheus 文本。
    ///
    /// 返回：追加同源文本；正常 exporter 使用结构化快照。
    fn render_prometheus(&self, output: &mut String) {
        output.push_str(&nasaga_runtime::render_saga_latency_metrics());
    }
}
