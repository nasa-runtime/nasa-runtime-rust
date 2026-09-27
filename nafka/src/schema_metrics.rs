//! Schema Registry 查询、缓存和控制面请求的低基数聚合指标。

use std::collections::BTreeMap;
use std::sync::Arc;

use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};

use crate::{
    ConfluentSchemaRegistry, SchemaControlCount, SchemaLookupCount, SchemaRegistrySnapshot,
};

/// 按封闭结局分类的 schema 查询计数；label 取值域固定，不随 schema ID 或 subject 扩张。
static LOOKUPS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "nafka_schema_lookups_total",
    help: "Schema Registry 按缓存、拉取与取消结局分类的累计查询次数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["outcome"],
    histogram_bounds: &[],
};
/// 当前缓存占用条目数。
static CACHE_ENTRIES: MetricDescriptor = MetricDescriptor {
    name: "nafka_schema_cache_entries",
    help: "Schema Registry 正负缓存合计占用的条目数。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
/// 已登记 client 的缓存条目上限总和；与占用总数一起判断是否已被容量而非 TTL 驱逐。
static CACHE_CAPACITY: MetricDescriptor = MetricDescriptor {
    name: "nafka_schema_cache_capacity",
    help: "已登记 Schema Registry client 的缓存配置条目上限总和。",
    unit: "",
    kind: MetricKind::Gauge,
    label_names: &[],
    histogram_bounds: &[],
};
/// 兼容性检查与注册请求计数；数据面缓存命中率不会被控制面流量稀释。
static CONTROL_REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "nafka_schema_control_requests_total",
    help: "Schema Registry 兼容性检查与注册按封闭结局分类的累计请求数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["operation", "outcome"],
    histogram_bounds: &[],
};

/// Schema Registry 全部指标族的静态 descriptor manifest。
static SCHEMA_DESCRIPTORS: [&MetricDescriptor; 4] = [
    &LOOKUPS_TOTAL,
    &CACHE_ENTRIES,
    &CACHE_CAPACITY,
    &CONTROL_REQUESTS_TOTAL,
];

/// 持有同一业务进程内的 client 共享所有权、按需聚合其累计事实的兼容源。
struct SchemaRegistryMetricsSource {
    clients: Vec<Arc<ConfluentSchemaRegistry>>,
}

impl LegacyMetricsSource for SchemaRegistryMetricsSource {
    /// 业务作用：返回 Schema Registry 兼容源拥有的静态指标族目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期冲突审计与结构化样本校验共用的全部 descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &SCHEMA_DESCRIPTORS
    }

    /// 业务作用：把全部 client 的累计快照聚合为统一目录的进程级结构化样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Some` 表示本源支持结构化出口；尚无查询时仍导出各 client 缓存容量与占用之和。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let snapshots: Vec<_> = self
            .clients
            .iter()
            .map(|client| client.metrics_snapshot())
            .collect();
        Some(samples(&aggregate_snapshots(&snapshots)))
    }

    /// 业务作用：保留旧 trait 入口；本源始终提供结构化快照，文本由统一 hub 渲染。
    ///
    /// 参数说明：
    /// - `_output`: 兼容 trait 的文本缓冲区；本源不直接写入。
    ///
    /// 返回：无；两个出口共用同一份 `snapshot()` 结果。
    fn render_prometheus(&self, _output: &mut String) {}
}

/// 业务作用：把 client 快照展开成统一目录的样本序列。
///
/// 参数说明：
/// - `snapshot`: client 一次读取得到的累计事实。
///
/// 返回：非零结局的数据面与控制面计数，以及始终导出的缓存占用与容量。
fn samples(snapshot: &SchemaRegistrySnapshot) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for count in &snapshot.lookups {
        out.push(MetricSample {
            name: LOOKUPS_TOTAL.name,
            labels: vec![("outcome", count.outcome.label().to_owned())],
            value: MetricValue::Counter(count.lookups),
        });
    }
    out.push(MetricSample {
        name: CACHE_ENTRIES.name,
        labels: Vec::new(),
        value: MetricValue::Gauge(snapshot.cached_entries as f64),
    });
    out.push(MetricSample {
        name: CACHE_CAPACITY.name,
        labels: Vec::new(),
        value: MetricValue::Gauge(snapshot.cache_capacity as f64),
    });
    for count in &snapshot.control_requests {
        out.push(MetricSample {
            name: CONTROL_REQUESTS_TOTAL.name,
            labels: vec![
                ("operation", count.operation.label().to_owned()),
                ("outcome", count.outcome.label().to_owned()),
            ],
            value: MetricValue::Counter(count.requests),
        });
    }
    out
}

/// 业务作用：把多个 Registry client 快照合并为一个进程级事实，维持每个指标族只有一个 owner。
///
/// 参数说明：
/// - `snapshots`: 同一抓取轮次依次读取的 client 累计快照。
///
/// 返回：相同操作与结局按饱和加法求和，缓存占用与容量按 client 求和。
fn aggregate_snapshots(snapshots: &[SchemaRegistrySnapshot]) -> SchemaRegistrySnapshot {
    let mut lookups = BTreeMap::new();
    let mut control_requests = BTreeMap::new();
    let mut cached_entries = 0_u64;
    let mut cache_capacity = 0_u64;

    for snapshot in snapshots {
        for count in &snapshot.lookups {
            let total = lookups.entry(count.outcome).or_insert(0_u64);
            *total = total.saturating_add(count.lookups);
        }
        for count in &snapshot.control_requests {
            let total = control_requests
                .entry((count.operation, count.outcome))
                .or_insert(0_u64);
            *total = total.saturating_add(count.requests);
        }
        cached_entries = cached_entries.saturating_add(snapshot.cached_entries);
        cache_capacity = cache_capacity.saturating_add(snapshot.cache_capacity);
    }

    SchemaRegistrySnapshot {
        lookups: lookups
            .into_iter()
            .map(|(outcome, lookups)| SchemaLookupCount { outcome, lookups })
            .collect(),
        cached_entries,
        cache_capacity,
        control_requests: control_requests
            .into_iter()
            .map(|((operation, outcome), requests)| SchemaControlCount {
                operation,
                outcome,
                requests,
            })
            .collect(),
    }
}

/// 业务作用：返回可直接交给 `Application::register_metrics_source` 的 Registry 兼容源。
///
/// 参数说明：
/// - `client`: 业务自己构造并持有的 client；指标源共享其所有权，不改变其生命周期。
///
/// 返回：每次抓取读取该 client 当前累计值的无状态源。
pub fn metrics_source(client: Arc<ConfluentSchemaRegistry>) -> Arc<dyn LegacyMetricsSource> {
    metrics_source_many([client])
}

/// 业务作用：为同一进程的多个 Schema Registry client 返回单一聚合指标源。
///
/// 统一目录要求每个 family 只有一个 owner，因此多集群 client 不能分别登记同名源；聚合源
/// 保持 label 低基数，同时让全部数据面与控制面请求进入文本和 OTLP 出口。
///
/// 参数说明：
/// - `clients`: 由业务构造并持有的 client 集合；源共享所有权，不改变生命周期。
///
/// 返回：每次抓取按封闭操作与结局聚合全部 client 当前累计值的无状态源。
pub fn metrics_source_many(
    clients: impl IntoIterator<Item = Arc<ConfluentSchemaRegistry>>,
) -> Arc<dyn LegacyMetricsSource> {
    Arc::new(SchemaRegistryMetricsSource {
        clients: clients.into_iter().collect(),
    })
}
