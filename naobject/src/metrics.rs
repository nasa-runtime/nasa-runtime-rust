//! 对象存储累计事实的有界指标目录适配。

use std::collections::BTreeMap;
use std::sync::Arc;

use nametrics_core::{
    LegacyMetricsSource, MetricDescriptor, MetricKind, MetricSample, MetricValue,
};

use super::{
    ObjectDurationSample, ObjectOperation, ObjectRequestCount, ObjectStoreSnapshot, S3ObjectStore,
    OBJECT_DURATION_BOUNDS,
};

/// 对象存储请求结局计数；label 取值域由 adapter 的封闭枚举决定，不随 bucket 或 key 扩张。
static REQUESTS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "naobject_requests_total",
    help: "对象存储操作按封闭结局分类的累计请求数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["operation", "outcome"],
    histogram_bounds: &[],
};
/// 成功传输字节数；只统计确认成功的载荷，失败与元数据操作不计入。
static BYTES_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "naobject_transferred_bytes_total",
    help: "成功上传或下载的对象字节总数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["direction"],
    histogram_bounds: &[],
};
/// 完整返回操作的时延分布；边界与 adapter 常量同源，不在导出面另建一份。
static DURATION_SECONDS: MetricDescriptor = MetricDescriptor {
    name: "naobject_request_duration_seconds",
    help: "对象存储操作从进入 adapter 到返回业务结局的耗时秒数，不含调用方取消；取消需结合 naobject_requests_total 观察。",
    unit: "seconds",
    kind: MetricKind::Histogram,
    label_names: &["operation"],
    histogram_bounds: &OBJECT_DURATION_BOUNDS,
};

/// 对象存储全部指标族的静态 descriptor manifest。
static OBJECT_DESCRIPTORS: [&MetricDescriptor; 3] =
    [&REQUESTS_TOTAL, &BYTES_TOTAL, &DURATION_SECONDS];

/// 持有同一业务进程内的 adapter 共享所有权、按需聚合其累计事实的兼容源。
struct ObjectStoreMetricsSource {
    stores: Vec<Arc<S3ObjectStore>>,
}

impl LegacyMetricsSource for ObjectStoreMetricsSource {
    /// 业务作用：返回对象存储兼容源拥有的静态指标族目录。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期冲突审计与结构化样本校验共用的全部 descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &OBJECT_DESCRIPTORS
    }

    /// 业务作用：把全部 adapter 的累计快照聚合为统一目录的进程级结构化样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Some` 表示本源支持结构化出口；即使尚无请求也返回上传、下载零值，
    /// 多 adapter 的同类计数、字节与完整返回时延按饱和加法合并。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let snapshots: Vec<_> = self
            .stores
            .iter()
            .map(|store| store.metrics_snapshot())
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

/// 业务作用：把 adapter 快照展开成统一目录的样本序列。
///
/// 参数说明：
/// - `snapshot`: adapter 一次读取得到的累计事实。
///
/// 返回：只含有观测的组合；直方图桶为非累积形态，桶总和恒等于 count。
fn samples(snapshot: &ObjectStoreSnapshot) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for count in &snapshot.requests {
        out.push(MetricSample {
            name: REQUESTS_TOTAL.name,
            labels: vec![
                ("operation", count.operation.label().to_owned()),
                ("outcome", count.outcome.label().to_owned()),
            ],
            value: MetricValue::Counter(count.requests),
        });
    }
    for (direction, value) in [
        ("upload", snapshot.uploaded_bytes),
        ("download", snapshot.downloaded_bytes),
    ] {
        out.push(MetricSample {
            name: BYTES_TOTAL.name,
            labels: vec![("direction", direction.to_owned())],
            value: MetricValue::Counter(value),
        });
    }
    for duration in &snapshot.durations {
        out.push(MetricSample {
            name: DURATION_SECONDS.name,
            labels: vec![("operation", duration.operation.label().to_owned())],
            value: MetricValue::Histogram {
                bounds: &OBJECT_DURATION_BOUNDS,
                buckets: duration.buckets.clone(),
                sum: duration.sum_seconds,
                count: duration.count,
            },
        });
    }
    out
}

/// 业务作用：把多个 adapter 快照合并为一个进程级事实，维持每个指标族只有一个 owner。
///
/// 参数说明：
/// - `snapshots`: 同一抓取轮次依次读取的 adapter 累计快照。
///
/// 返回：相同操作与结局按饱和加法求和，直方图逐桶合并并从桶总和派生 count，字节数同样求和。
fn aggregate_snapshots(snapshots: &[ObjectStoreSnapshot]) -> ObjectStoreSnapshot {
    let mut requests = BTreeMap::new();
    let mut durations: BTreeMap<ObjectOperation, (Vec<u64>, f64)> = BTreeMap::new();
    let mut uploaded_bytes = 0_u64;
    let mut downloaded_bytes = 0_u64;

    for snapshot in snapshots {
        for count in &snapshot.requests {
            let total = requests
                .entry((count.operation, count.outcome))
                .or_insert(0_u64);
            *total = total.saturating_add(count.requests);
        }
        for duration in &snapshot.durations {
            let (buckets, sum_seconds) = durations
                .entry(duration.operation)
                .or_insert_with(|| (vec![0; OBJECT_DURATION_BOUNDS.len() + 1], 0.0));
            for (target, value) in buckets.iter_mut().zip(&duration.buckets) {
                *target = target.saturating_add(*value);
            }
            *sum_seconds += duration.sum_seconds;
        }
        uploaded_bytes = uploaded_bytes.saturating_add(snapshot.uploaded_bytes);
        downloaded_bytes = downloaded_bytes.saturating_add(snapshot.downloaded_bytes);
    }

    ObjectStoreSnapshot {
        requests: requests
            .into_iter()
            .map(|((operation, outcome), requests)| ObjectRequestCount {
                operation,
                outcome,
                requests,
            })
            .collect(),
        durations: durations
            .into_iter()
            .map(|(operation, (buckets, sum_seconds))| ObjectDurationSample {
                operation,
                count: buckets.iter().copied().fold(0_u64, u64::saturating_add),
                buckets,
                sum_seconds,
            })
            .collect(),
        uploaded_bytes,
        downloaded_bytes,
    }
}

/// 业务作用：返回可直接交给 `Application::register_metrics_source` 的对象存储兼容源。
///
/// 参数说明：
/// - `store`: 业务自己构造并持有的 adapter；指标源共享其所有权，不改变其生命周期。
///
/// 返回：每次抓取读取该 adapter 当前累计值的无状态源。
pub fn metrics_source(store: Arc<S3ObjectStore>) -> Arc<dyn LegacyMetricsSource> {
    metrics_source_many([store])
}

/// 业务作用：为同一进程的多个对象存储 adapter 返回单一聚合指标源。
///
/// 统一目录要求每个 family 只有一个 owner，因此多 bucket、多 endpoint 或多凭据域不能分别
/// 登记同名源；聚合源保持 label 低基数，同时让全部 adapter 进入文本与 OTLP 出口。
///
/// 参数说明：
/// - `stores`: 由业务构造并持有的 adapter 集合；源共享所有权，不改变生命周期。
///
/// 返回：每次抓取按操作和封闭结局聚合全部 adapter 当前累计值的无状态源。
pub fn metrics_source_many(
    stores: impl IntoIterator<Item = Arc<S3ObjectStore>>,
) -> Arc<dyn LegacyMetricsSource> {
    Arc::new(ObjectStoreMetricsSource {
        stores: stores.into_iter().collect(),
    })
}
