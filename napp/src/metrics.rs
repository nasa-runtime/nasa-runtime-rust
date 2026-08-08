//! 指标域迁移到统一 `nametrics_core::MetricHub`。
//!
//! `napp` 持有唯一进程级 `MetricHub`;各领域按 顺序逐个把记录接到它上面。
//!
//! - **nafka**(消除应用手工 sink):nafka 通过 `MetricsSink`(by-name)上报,
//!   [`NafkaMetricSinkAdapter`] 把它桥接到 hub(by-descriptor),启动期用
//!   [`register_nafka_descriptors`] 做冲突审计。nafka 是**原生**域(记录直接进 hub cells)。
//! - **naweb**(安全端点指标):naweb 自持 `SecurityMetrics` registry 并能自渲染 Prometheus 文本,
//!   [`NawebMetricsSource`] 把它包成 `LegacyMetricsSource`——descriptor 并入统一 catalog 供冲突审计,
//!   值仍由 naweb 自渲染。这样一次 `hub.render_prometheus()` 同时得到 nafka(原生)+ naweb(兼容源)。
//!
//! nafana 迁移属 nasa 门面层增量(napp 不依赖 nafana,强接会倒置分层),此处不做。

#[cfg(any(feature = "kafka", feature = "web-security"))]
use std::sync::Arc;

#[cfg(any(feature = "kafka", feature = "web-security"))]
use nametrics_core::{MetricConflict, MetricDescriptor, MetricHub, MetricKind};

// ───────────────────────────── nafka 域(原生) ─────────────────────────────

#[cfg(feature = "kafka")]
macro_rules! nafka_counter {
    ($ident:ident, $name:literal, $help:literal, $labels:expr) => {
        static $ident: MetricDescriptor = MetricDescriptor {
            name: $name,
            help: $help,
            unit: "",
            kind: MetricKind::Counter,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}

#[cfg(feature = "kafka")]
macro_rules! nafka_gauge {
    ($ident:ident, $name:literal, $help:literal, $unit:literal, $labels:expr) => {
        static $ident: MetricDescriptor = MetricDescriptor {
            name: $name,
            help: $help,
            unit: $unit,
            kind: MetricKind::Gauge,
            label_names: $labels,
            histogram_bounds: &[],
        };
    };
}

#[cfg(feature = "kafka")]
nafka_counter!(
    GROUP_READY_TOTAL,
    "group_ready_total",
    "consumer group 达成就绪条件的次数",
    &["group"]
);
#[cfg(feature = "kafka")]
nafka_counter!(
    GROUP_READY_TIMEOUT_TOTAL,
    "group_ready_timeout_total",
    "consumer group 在就绪窗口内未满足条件而超时的次数",
    &["group"]
);
#[cfg(feature = "kafka")]
nafka_gauge!(
    GROUP_READY,
    "group_ready",
    "consumer group 当前是否就绪(1/0)",
    "",
    &["group"]
);
#[cfg(feature = "kafka")]
nafka_gauge!(
    GROUP_READY_WAIT_MILLIS,
    "group_ready_wait_millis",
    "consumer group 达成就绪所等待的毫秒数",
    "milliseconds",
    &["group"]
);
#[cfg(feature = "kafka")]
nafka_counter!(
    PUBLISHED_TOTAL,
    "published_total",
    "producer 成功发布的消息数",
    &["lane", "topic"]
);
#[cfg(feature = "kafka")]
nafka_counter!(
    PUBLISH_FAILED_TOTAL,
    "publish_failed_total",
    "producer 发布失败的消息数",
    &["lane", "topic"]
);

/// nafka 全部指标的静态 descriptor manifest。
#[cfg(feature = "kafka")]
static NAFKA_DESCRIPTORS: [&MetricDescriptor; 6] = [
    &GROUP_READY_TOTAL,
    &GROUP_READY_TIMEOUT_TOTAL,
    &GROUP_READY,
    &GROUP_READY_WAIT_MILLIS,
    &PUBLISHED_TOTAL,
    &PUBLISH_FAILED_TOTAL,
];

/// 业务作用：启动期把 nafka 的 descriptor 注册进 hub 并做冲突审计。
///
/// # 错误
///
/// 任一 nafka descriptor 与已注册项冲突时返回首个 [`MetricConflict`]。
#[cfg(feature = "kafka")]
pub fn register_nafka_descriptors(hub: &MetricHub) -> Result<(), MetricConflict> {
    for descriptor in NAFKA_DESCRIPTORS {
        hub.register(descriptor)?;
    }
    Ok(())
}

/// 业务作用：按 name 查找 nafka descriptor。
#[cfg(feature = "kafka")]
fn nafka_descriptor(name: &str) -> Option<&'static MetricDescriptor> {
    NAFKA_DESCRIPTORS
        .iter()
        .copied()
        .find(|descriptor| descriptor.name == name)
}

/// 业务作用：按 `descriptor.label_names` 顺序,从 nafka 的 (key, value) 对里提取 label 值;缺失用空串。
#[cfg(feature = "kafka")]
fn label_values(descriptor: &MetricDescriptor, labels: &[(&'static str, &str)]) -> Vec<String> {
    descriptor
        .label_names
        .iter()
        .map(|name| {
            labels
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| (*value).to_owned())
                .unwrap_or_default()
        })
        .collect()
}

/// 把 nafka 的 `MetricsSink`(by-name)桥接到统一 `MetricHub`(by-descriptor)。
///
/// nafka 上报 (name, labels);本适配器按 name 查静态 descriptor、按 descriptor 顺序提取 label 值,
/// 再记入 hub。未在 manifest 中的 name 被安全忽略(不会凭空造 descriptor,保持静态审计契约)。
#[cfg(feature = "kafka")]
pub struct NafkaMetricSinkAdapter {
    hub: Arc<MetricHub>,
}

#[cfg(feature = "kafka")]
impl NafkaMetricSinkAdapter {
    /// 业务作用：用给定的进程级 hub 创建适配器。
    pub fn new(hub: Arc<MetricHub>) -> Self {
        Self { hub }
    }
}

#[cfg(feature = "kafka")]
impl nafka::MetricsSink for NafkaMetricSinkAdapter {
    /// 业务作用：将 nafka counter 名称映射到静态 descriptor，并按声明顺序记录 labels。
    fn counter(&self, name: &'static str, delta: u64, labels: nafka::MetricLabels<'_>) {
        use nametrics_core::MetricRecorder;
        let Some(descriptor) = nafka_descriptor(name) else {
            return;
        };
        let values = label_values(descriptor, labels);
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        self.hub.counter(descriptor, delta, &refs);
    }

    /// 业务作用：将 nafka gauge 名称映射到静态 descriptor，并把整数值写入统一 hub。
    fn gauge(&self, name: &'static str, value: i64, labels: nafka::MetricLabels<'_>) {
        use nametrics_core::MetricRecorder;
        let Some(descriptor) = nafka_descriptor(name) else {
            return;
        };
        let values = label_values(descriptor, labels);
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        self.hub.gauge(descriptor, value as f64, &refs);
    }
}

// ───────────────────────────── naweb 域(兼容源) ─────────────────────────────

#[cfg(feature = "web-security")]
use nametrics_core::LegacyMetricsSource;

/// naweb 安全端点指标的 descriptor manifest(仅供统一 catalog 冲突审计;值由 naweb 自渲染)。
///
/// help/label 与 `naweb::SecurityMetrics::render_prometheus` 一致;histogram 桶边界与
/// `naweb` 的 `DURATION_BUCKET_LABELS` 对齐(秒)。
#[cfg(feature = "web-security")]
static MAPPING_SECURITY_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_security_requests_total",
    help: "安全端点最终请求结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "outcome"],
    histogram_bounds: &[],
};
#[cfg(feature = "web-security")]
static MAPPING_AUTH_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_auth_requests_total",
    help: "身份阶段结果计数,不包含身份值。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "requirement", "outcome"],
    histogram_bounds: &[],
};
#[cfg(feature = "web-security")]
static MAPPING_CRYPTO_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_requests_total",
    help: "密码方向执行结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "protocol", "direction", "outcome"],
    histogram_bounds: &[],
};
#[cfg(feature = "web-security")]
static MAPPING_CRYPTO_REPLAY_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_replay_total",
    help: "required replay 占位结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "outcome"],
    histogram_bounds: &[],
};
#[cfg(feature = "web-security")]
static MAPPING_CRYPTO_BYPASS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_bypass_total",
    help: "静态 condition 实际关闭密码方向的次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "condition"],
    histogram_bounds: &[],
};
#[cfg(feature = "web-security")]
static MAPPING_CRYPTO_DURATION_SECONDS: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_duration_seconds",
    help: "安全流水线固定阶段延迟秒数。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["route_id", "protocol", "operation"],
    histogram_bounds: &[
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    ],
};
#[cfg(feature = "web-security")]
static MAPPING_CRYPTO_KEY_RELOAD_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_key_reload_total",
    help: "安全快照热更新结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["outcome"],
    histogram_bounds: &[],
};
#[cfg(feature = "web-security")]
static MAPPING_CRYPTO_SNAPSHOT_GENERATION: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_snapshot_generation",
    help: "当前安全快照代次。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};

/// naweb 安全端点全部指标的静态 descriptor manifest。
#[cfg(feature = "web-security")]
static NAWEB_DESCRIPTORS: [&MetricDescriptor; 8] = [
    &MAPPING_SECURITY_REQUESTS_TOTAL,
    &MAPPING_AUTH_REQUESTS_TOTAL,
    &MAPPING_CRYPTO_REQUESTS_TOTAL,
    &MAPPING_CRYPTO_REPLAY_TOTAL,
    &MAPPING_CRYPTO_BYPASS_TOTAL,
    &MAPPING_CRYPTO_DURATION_SECONDS,
    &MAPPING_CRYPTO_KEY_RELOAD_TOTAL,
    &MAPPING_CRYPTO_SNAPSHOT_GENERATION,
];

/// 把 naweb 的 `SecurityMetrics` registry 包成 `LegacyMetricsSource`。
///
/// descriptor 并入统一 catalog 供冲突审计,值仍由 naweb 自渲染(它的 registry 拥有原子计数器)。
/// hub 渲染时原生循环跳过这些族名,只由本源自渲染,避免重复 HELP/TYPE。
#[cfg(feature = "web-security")]
pub struct NawebMetricsSource {
    metrics: Arc<naweb::SecurityMetrics>,
}

#[cfg(feature = "web-security")]
impl NawebMetricsSource {
    /// 业务作用：用 Web Ready 后发布的 `SecurityMetrics` 句柄创建兼容源。
    pub fn new(metrics: Arc<naweb::SecurityMetrics>) -> Self {
        Self { metrics }
    }
}

#[cfg(feature = "web-security")]
impl LegacyMetricsSource for NawebMetricsSource {
    /// 业务作用：返回 naweb 兼容源拥有的静态指标族目录。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &NAWEB_DESCRIPTORS
    }

    /// 业务作用：读取 naweb 当前 registry 快照并追加 Prometheus exposition。
    fn render_prometheus(&self, output: &mut String) {
        output.push_str(&self.metrics.render_prometheus());
    }
}

/// 业务作用：把 naweb 兼容源注册进 hub(审计其 descriptor 并保存以便渲染)。
///
/// # 错误
///
/// 任一 naweb descriptor 与已注册项冲突时返回首个 [`MetricConflict`]。
#[cfg(feature = "web-security")]
pub fn register_naweb_source(
    hub: &MetricHub,
    source: Arc<NawebMetricsSource>,
) -> Result<(), MetricConflict> {
    hub.register_legacy_source(source)
}
