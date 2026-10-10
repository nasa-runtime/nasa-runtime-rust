//! 中性发现状态通过应用现有指标目录导出，不创建独立 exporter。

use nadisc_runtime::{OwnerPhase, RegistrationState, RuntimeObserver};
use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};

pub(super) struct DiscoveryMetrics(pub RuntimeObserver);

static PHASE: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_owner_phase",
    help: "生命周期：Prepared=0，Starting=1，Serving=2，Quiescing=3，Closed=4。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static FAILED: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_supervisor_failed",
    help: "监督异常时为一。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static INSTANCES: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_retained_instances",
    help: "包括被保留旧快照的实例数量。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static BYTES: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_snapshot_bytes",
    help: "包括被保留旧快照的内存计费字节数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static TRANSPORT: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_transport_tasks",
    help: "owner 当前接管的传输任务数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static REMAINING: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_shutdown_remaining_milliseconds",
    help: "停机剩余预算，未进入停机为负一。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
static PREPARED: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_connection_prepared",
    help: "连接初始化成功，不等同业务就绪。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static OBSERVATIONS: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_observations",
    help: "活跃共享观察数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static USABLE: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_usable_observations",
    help: "当前仍有可用发现证据的观察数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static AGE: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_oldest_observation_milliseconds",
    help: "最旧成功观察的年龄，没有成功证据为负一。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static QUERIES: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_queries_total",
    help: "完整发现查询总次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static ERRORS: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_query_errors_total",
    help: "完整发现查询失败次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static MICROS: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_query_duration_microseconds_total",
    help: "完整查询累计耗时，包括有限队列等待。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["connection"],
    histogram_bounds: &[],
};
static READY: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_registration_ready",
    help: "注册当前是否同时满足远端证据和本地资格。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["registration"],
    histogram_bounds: &[],
};
static STATE: MetricDescriptor = MetricDescriptor { name: "napp_discovery_registration_state", help: "Planned=0，Registering=1，OutcomeUnknown=2，PendingHealth=3，Active=4，Suspended=5，Withdrawing=6，Residual=7，Removed=8。", unit: "", kind: MetricKind::Gauge, label_names: &["registration"], histogram_bounds: &[] };
static REGAGE: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_registration_observation_milliseconds",
    help: "注册最近成功证据的年龄，无证据为负一。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["registration"],
    histogram_bounds: &[],
};
static CRITICAL: MetricDescriptor = MetricDescriptor {
    name: "napp_discovery_registration_readiness_critical",
    help: "注册故障是否影响应用整体就绪。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &["registration"],
    histogram_bounds: &[],
};
static DESCRIPTORS: [&MetricDescriptor; 17] = [
    &PHASE,
    &FAILED,
    &INSTANCES,
    &BYTES,
    &TRANSPORT,
    &REMAINING,
    &PREPARED,
    &OBSERVATIONS,
    &USABLE,
    &AGE,
    &QUERIES,
    &ERRORS,
    &MICROS,
    &READY,
    &STATE,
    &REGAGE,
    &CRITICAL,
];

impl LegacyMetricsSource for DiscoveryMetrics {
    /// 业务作用：向唯一指标目录声明固定发现指标，不使用动态服务名作为标签。
    /// 参数说明：无。
    /// 返回：完整静态指标合同。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &DESCRIPTORS
    }
    /// 业务作用：导出同一 owner 的只读快照，抓取不发起后端请求。
    /// 参数说明：无。
    /// 返回：配置名约束的有限样本；owner 释放后不再输出旧就绪状态。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let Some(state) = self.0.snapshot() else {
            return Some(Vec::new());
        };
        let phase = match state.phase {
            OwnerPhase::Prepared => 0,
            OwnerPhase::Starting => 1,
            OwnerPhase::Serving => 2,
            OwnerPhase::Quiescing => 3,
            OwnerPhase::Closed => 4,
        };
        let mut samples =
            Vec::with_capacity(6 + state.connections.len() * 7 + state.registrations.len() * 4);
        for (descriptor, value) in [
            (&PHASE, phase as f64),
            (&FAILED, u8::from(state.supervisor_failed) as f64),
            (&INSTANCES, state.retained_instances as f64),
            (&BYTES, state.snapshot_bytes as f64),
            (&TRANSPORT, state.transport_tasks as f64),
            (
                &REMAINING,
                state
                    .shutdown_remaining_ms
                    .map_or(-1.0, |value| value as f64),
            ),
        ] {
            samples.push(MetricSample {
                name: descriptor.name,
                labels: vec![],
                value: MetricValue::Gauge(value),
            });
        }
        for connection in state.connections {
            let labels = vec![("connection", connection.name)];
            for (descriptor, value) in [
                (&PREPARED, u8::from(connection.prepared) as f64),
                (&OBSERVATIONS, connection.observations as f64),
                (&USABLE, connection.usable_observations as f64),
                (
                    &AGE,
                    connection
                        .oldest_observation_age_ms
                        .map_or(-1.0, |value| value as f64),
                ),
            ] {
                samples.push(MetricSample {
                    name: descriptor.name,
                    labels: labels.clone(),
                    value: MetricValue::Gauge(value),
                });
            }
            for (descriptor, value) in [
                (&QUERIES, connection.queries),
                (&ERRORS, connection.query_errors),
                (&MICROS, connection.query_micros),
            ] {
                samples.push(MetricSample {
                    name: descriptor.name,
                    labels: labels.clone(),
                    value: MetricValue::Counter(value),
                });
            }
        }
        for registration in state.registrations {
            let ready = registration.is_ready();
            let labels = vec![("registration", registration.id)];
            let phase = match registration.state {
                RegistrationState::Planned => 0,
                RegistrationState::Registering => 1,
                RegistrationState::OutcomeUnknown => 2,
                RegistrationState::PendingHealth => 3,
                RegistrationState::Active => 4,
                RegistrationState::Suspended => 5,
                RegistrationState::Withdrawing => 6,
                RegistrationState::Residual => 7,
                RegistrationState::Removed => 8,
            };
            for (descriptor, value) in [
                (&READY, u8::from(ready) as f64),
                (&STATE, phase as f64),
                (
                    &REGAGE,
                    registration
                        .observed_at
                        .map_or(-1.0, |at| at.elapsed().as_millis() as f64),
                ),
                (&CRITICAL, u8::from(registration.readiness_critical) as f64),
            ] {
                samples.push(MetricSample {
                    name: descriptor.name,
                    labels: labels.clone(),
                    value: MetricValue::Gauge(value),
                });
            }
        }
        Some(samples)
    }
    /// 业务作用：保持结构化出口唯一，Prometheus 文本由统一指标目录渲染。
    /// 参数说明：`_output` 是兼容 trait 的文本缓冲区。
    /// 返回：本源总是提供结构化样本，不追加另一份文本。
    fn render_prometheus(&self, _output: &mut String) {}
}
