//! 接口原子计数的统一 MetricHub 快照适配。

use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};
use std::sync::{Arc, OnceLock};

macro_rules! descriptor {
    ($name:ident,$wire:literal,$help:literal,$kind:ident,$labels:expr) => {
        static $name: MetricDescriptor = MetricDescriptor {
            name: $wire,
            help: $help,
            unit: "",
            kind: MetricKind::$kind,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}
descriptor!(
    REQUESTS,
    "nafana_requests_total",
    "接口请求结局单调计数(success/failure/timeout/rejected/canceled)。",
    Counter,
    &["command", "group", "outcome"]
);
descriptor!(
    FALLBACK,
    "nafana_fallback_total",
    "拒绝/超时分支产出降级响应的单调计数。",
    Counter,
    &["command", "group"]
);
descriptor!(
    GLOBAL_FALLBACK,
    "nafana_global_fallback_total",
    "全局降级处理器结局单调计数。",
    Counter,
    &["command", "group", "outcome"]
);
descriptor!(
    TPS,
    "nafana_tps_total",
    "TPS 单调计数:每请求按 tps_weight 累加。",
    Counter,
    &["command", "group"]
);
descriptor!(
    INFLIGHT,
    "nafana_inflight",
    "当前执行区并发。",
    Gauge,
    &["command", "group"]
);
descriptor!(
    ROLLING,
    "nafana_inflight_rolling_max",
    "10s 滚动窗口内并发峰值(随窗口回落)。",
    Gauge,
    &["command", "group"]
);
descriptor!(
    LIFETIME,
    "nafana_inflight_lifetime_max",
    "进程生命周期并发峰值(只增不减)。",
    Gauge,
    &["command", "group"]
);
descriptor!(
    MAX_CONCURRENT,
    "nafana_max_concurrent",
    "bulkhead 容量;0 = 不限并发。",
    Gauge,
    &["command", "group"]
);
descriptor!(
    TIMEOUT,
    "nafana_timeout_ms",
    "单请求超时毫秒;0 = 不超时。",
    Gauge,
    &["command", "group"]
);
descriptor!(
    WEIGHT,
    "nafana_tps_weight",
    "TPS 权重;0 = 未标 TPS 或权重 0。",
    Gauge,
    &["command", "group"]
);
descriptor!(
    INFO,
    "nafana_command_info",
    "命令展示元信息(path = 真实路由)。",
    Gauge,
    &["command", "group", "path"]
);
static LATENCY: MetricDescriptor = MetricDescriptor {
    name: "nafana_latency_seconds",
    help: "执行延迟直方图(秒);rejected/canceled 不进延迟统计。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["command", "group"],
    histogram_bounds: &[
        0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ],
};
static DESCRIPTORS: [&MetricDescriptor; 12] = [
    &REQUESTS,
    &FALLBACK,
    &GLOBAL_FALLBACK,
    &TPS,
    &INFLIGHT,
    &ROLLING,
    &LIFETIME,
    &MAX_CONCURRENT,
    &TIMEOUT,
    &WEIGHT,
    &LATENCY,
    &INFO,
];
struct Source;
impl LegacyMetricsSource for Source {
    /// 业务作用：冻结接口域的公共指标合同。
    /// 参数说明：无。
    /// 返回：与独立 Prometheus 出口一致的 descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &DESCRIPTORS
    }
    /// 业务作用：把接口生命周期计数映射为各出口共用的累计样本。
    /// 参数说明：无。
    /// 返回：保留命令、分组、结局及固定 histogram 桶的快照。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        Some(
            crate::structured_metrics_snapshot()
                .into_iter()
                .map(|s| MetricSample {
                    name: s.name,
                    labels: s.labels,
                    value: match s.value {
                        crate::PrometheusMetricValue::Counter(v) => MetricValue::Counter(v),
                        crate::PrometheusMetricValue::Gauge(v) => MetricValue::Gauge(v),
                        crate::PrometheusMetricValue::Histogram {
                            buckets,
                            sum,
                            count,
                        } => MetricValue::Histogram {
                            bounds: LATENCY.histogram_bounds,
                            buckets,
                            sum,
                            count,
                        },
                    },
                })
                .collect(),
        )
    }
    /// 业务作用：保持独立调用方的文本快照合同。
    /// 参数说明：`output` 为追加目标。
    /// 返回：与结构化指标同源的文本。
    fn render_prometheus(&self, output: &mut String) {
        output.push_str(&crate::render_metrics());
    }
}

/// 业务作用：提供进程唯一的接口指标源，受管 Application 自动登记，独立宿主可显式登记。
/// 参数说明：无。
/// 返回：共享 source；重复登记同一对象不会产生重复 family。
pub fn metrics_source() -> Arc<dyn LegacyMetricsSource> {
    static SOURCE: OnceLock<Arc<dyn LegacyMetricsSource>> = OnceLock::new();
    SOURCE.get_or_init(|| Arc::new(Source)).clone()
}
