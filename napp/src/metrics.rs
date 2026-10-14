//! 各领域指标连接统一 `nametrics_core::MetricHub`。
//!
//! `napp` 持有进程级 `MetricHub`，在启动时登记领域指标的名称、类型、单位与标签。
//!
//! - **nafka**: 通过有界异步 `MetricsSink` 上报,
//!   [`NafkaMetricSinkAdapter`] 把它桥接到 hub(by-descriptor),启动期用
//!   [`register_nafka_descriptors`] 检查定义冲突；`source` 标签保留不同数据源的独立序列。
//! - **naweb**(安全端点指标):naweb 自持 `SecurityMetrics` registry，
//!   [`NawebMetricsSource`] 把它包成 `LegacyMetricsSource`——descriptor 并入统一 catalog，结构化
//!   快照同时供 Prometheus 与 OTLP 使用。这样一次 `hub.render_prometheus()` 同时得到 nafka
//!   (原生)+ naweb(兼容源)。

#[cfg(any(feature = "kafka", feature = "web-auth", feature = "web-crypto"))]
use std::sync::Arc;

#[cfg(feature = "kafka")]
use nametrics_core::MetricConflict;
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
use nametrics_core::MetricSourceRegistrationError;
#[cfg(any(feature = "kafka", feature = "web-auth", feature = "web-crypto"))]
use nametrics_core::{MetricDescriptor, MetricHub, MetricKind};

// ───────────────────────────── nafka 域(原生) ─────────────────────────────

/// 从运行时固定目录建立后端定义，避免新增运行样本被门面静默丢弃。
#[cfg(feature = "kafka")]
static NAFKA_DESCRIPTORS: std::sync::LazyLock<Vec<MetricDescriptor>> =
    std::sync::LazyLock::new(|| {
        nafka::METRIC_CATALOG
            .iter()
            .map(|definition| MetricDescriptor {
                name: definition.name,
                help: definition.description,
                unit: definition.unit,
                kind: match definition.kind {
                    nafka::MetricKind::Counter => MetricKind::Counter,
                    nafka::MetricKind::Gauge => MetricKind::Gauge,
                },
                label_names: definition.labels,
                histogram_bounds: &[],
            })
            .collect()
    });

/// 业务作用：启动期把 nafka 的 descriptor 注册进 hub 并做冲突审计。
/// 参数说明：hub 为受管应用的进程级指标目录。
/// 返回：全部登记成功或遇到第一个定义冲突；调用方须在开放业务前处理失败。
///
/// # 错误
///
/// 任一 nafka descriptor 与已注册项冲突时返回首个 [`MetricConflict`]。
#[cfg(feature = "kafka")]
pub fn register_nafka_descriptors(hub: &MetricHub) -> Result<(), MetricConflict> {
    for descriptor in NAFKA_DESCRIPTORS.iter() {
        hub.register(descriptor)?;
    }
    Ok(())
}

/// 业务作用：按 name 查找 nafka descriptor。
/// 参数说明：name 为 runtime 发出的固定逻辑指标名。
/// 返回：已声明定义；未知名称不创建动态 family。
#[cfg(feature = "kafka")]
fn nafka_descriptor(name: &str) -> Option<&'static MetricDescriptor> {
    NAFKA_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.name == name)
}

/// 业务作用：按目录顺序提取完整且唯一的标签，避免缺失源身份造成不同数据源合流。
/// 参数说明：descriptor 为固定定义；labels 为异步出口携带的完整标签集。
/// 返回：标签形状正确时返回有序值；缺失、重复或多余标签均拒绝当前样本。
#[cfg(feature = "kafka")]
fn label_values(
    descriptor: &MetricDescriptor,
    labels: &[(&'static str, &str)],
) -> Option<Vec<String>> {
    if labels.len() != descriptor.label_names.len() {
        return None;
    }
    descriptor
        .label_names
        .iter()
        .map(|name| {
            let mut values = labels.iter().filter(|(key, _)| key == name);
            let (_, value) = values.next()?;
            if values.next().is_some() {
                return None;
            }
            Some((*value).to_owned())
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
    /// 参数说明：hub 为已经登记 nafka 定义的应用指标目录。
    /// 返回：共享该目录的适配器，不自行创建后台线程。
    pub fn new(hub: Arc<MetricHub>) -> Self {
        Self { hub }
    }
}

#[cfg(feature = "kafka")]
impl nafka::MetricsSink for NafkaMetricSinkAdapter {
    /// 业务作用：将 nafka counter 名称映射到静态 descriptor，并按声明顺序记录 labels。
    /// 参数说明：name 为固定名称；delta 为非负增量；labels 为包含 source 的完整标签。
    /// 返回：已知 counter 且标签有效时累计；其它样本忽略，不影响消息交付。
    fn counter(&self, name: &'static str, delta: u64, labels: nafka::MetricLabels<'_>) {
        use nametrics_core::MetricRecorder;
        let Some(descriptor) = nafka_descriptor(name) else {
            return;
        };
        if descriptor.kind != MetricKind::Counter {
            return;
        }
        let Some(values) = label_values(descriptor, labels) else {
            return;
        };
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        self.hub.counter(descriptor, delta, &refs);
    }

    /// 业务作用：将 nafka gauge 名称映射到静态 descriptor，并把整数值写入统一 hub。
    /// 参数说明：name 为固定名称；value 为瞬时值；labels 为包含 source 的完整标签。
    /// 返回：已知 gauge 且标签有效时更新；其它样本忽略，不影响消息交付。
    fn gauge(&self, name: &'static str, value: i64, labels: nafka::MetricLabels<'_>) {
        use nametrics_core::MetricRecorder;
        let Some(descriptor) = nafka_descriptor(name) else {
            return;
        };
        if descriptor.kind != MetricKind::Gauge {
            return;
        }
        let Some(values) = label_values(descriptor, labels) else {
            return;
        };
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        self.hub.gauge(descriptor, value as f64, &refs);
    }
}

// ───────────────────────────── naweb 域(兼容源) ─────────────────────────────

#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
use nametrics_core::LegacyMetricsSource;

/// naweb 安全端点指标的 descriptor manifest，供统一 catalog 冲突审计、结构化样本校验与文本渲染。
///
/// help/label 与 `naweb::SecurityMetrics::render_prometheus` 一致;histogram 桶边界与
/// `naweb` 的 `DURATION_BUCKET_LABELS` 对齐(秒)。
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_SECURITY_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_security_requests_total",
    help: "安全端点最终请求结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "outcome"],
    histogram_bounds: &[],
};
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_AUTH_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_auth_requests_total",
    help: "身份阶段结果计数,不包含身份值。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "requirement", "outcome"],
    histogram_bounds: &[],
};
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_CRYPTO_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_requests_total",
    help: "密码方向执行结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "protocol", "direction", "outcome"],
    histogram_bounds: &[],
};
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_CRYPTO_REPLAY_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_replay_total",
    help: "required replay 占位结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "outcome"],
    histogram_bounds: &[],
};
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_CRYPTO_BYPASS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_bypass_total",
    help: "静态 condition 实际关闭密码方向的次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["route_id", "condition"],
    histogram_bounds: &[],
};
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
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
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_CRYPTO_KEY_RELOAD_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_key_reload_total",
    help: "安全快照热更新结果计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["outcome"],
    histogram_bounds: &[],
};
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
static MAPPING_CRYPTO_SNAPSHOT_GENERATION: MetricDescriptor = MetricDescriptor {
    name: "mapping_crypto_snapshot_generation",
    help: "当前安全快照代次。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};

/// naweb 安全端点全部指标的静态 descriptor manifest。
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
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
/// descriptor 并入统一 catalog，值由 naweb registry 生成结构化快照后交给 hub 统一渲染；
/// 本源始终返回 `Some`，当前无样本也不会回落到另一份文本数据路径。
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
pub struct NawebMetricsSource {
    metrics: Arc<naweb::SecurityMetrics>,
}

#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
impl NawebMetricsSource {
    /// 业务作用：用 Web Ready 后发布的 `SecurityMetrics` 句柄创建兼容源。
    ///
    /// 参数说明：
    /// - `metrics`: naweb 安全 registry 的共享所有权。
    ///
    /// 返回：可登记到统一指标 hub 的兼容源。
    pub fn new(metrics: Arc<naweb::SecurityMetrics>) -> Self {
        Self { metrics }
    }
}

#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
impl LegacyMetricsSource for NawebMetricsSource {
    /// 业务作用：返回 naweb 兼容源拥有的静态指标族目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期冲突审计和结构化校验共用的全部 naweb descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &NAWEB_DESCRIPTORS
    }

    /// 业务作用：把 naweb 的结构化 registry 快照映射到统一指标样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：始终为 `Some`，其中保留 family、label、计数与直方图形状的 provider-neutral 样本。
    fn snapshot(&self) -> Option<Vec<nametrics_core::MetricSample>> {
        Some(
            self.metrics
                .structured_snapshot()
                .into_iter()
                .map(|sample| nametrics_core::MetricSample {
                    name: sample.name,
                    labels: sample.labels,
                    value: match sample.value {
                        naweb::SecurityMetricValue::Counter(value) => {
                            nametrics_core::MetricValue::Counter(value)
                        }
                        naweb::SecurityMetricValue::Gauge(value) => {
                            nametrics_core::MetricValue::Gauge(value)
                        }
                        naweb::SecurityMetricValue::Histogram {
                            buckets,
                            sum,
                            count,
                        } => nametrics_core::MetricValue::Histogram {
                            bounds: MAPPING_CRYPTO_DURATION_SECONDS.histogram_bounds,
                            buckets,
                            sum,
                            count,
                        },
                    },
                })
                .collect(),
        )
    }

    /// 业务作用：读取 naweb 当前 registry 快照并追加 Prometheus exposition。
    ///
    /// 参数说明：
    /// - `output`: 接收旧源文本的缓冲区。
    ///
    /// 返回：无；该入口仅保留给显式选择文本旧源模式的兼容调用方。
    fn render_prometheus(&self, output: &mut String) {
        output.push_str(&self.metrics.render_prometheus());
    }
}

/// 业务作用：把 naweb 兼容源及其按静态路由展开的最坏公开序列预算事务式注册进 hub。
///
/// 参数说明：
/// - hub：Application 唯一进程级指标目录。
/// - source：Web Ready 发布的安全指标源。
///
/// 返回：descriptor 无冲突且完整序列容量可预留时发布；失败时目录、源和容量账目均保持不变。
#[cfg(any(feature = "web-auth", feature = "web-crypto"))]
pub fn register_naweb_source(
    hub: &MetricHub,
    source: Arc<NawebMetricsSource>,
) -> Result<(), MetricSourceRegistrationError> {
    let worst_case_series = source.metrics.freeze_worst_case_series();
    hub.register_legacy_source_reserved(source, worst_case_series)
}
