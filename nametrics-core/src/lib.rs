//! 进程级 provider-neutral 指标核心。
//!
//! # 核心价值
//!
//! [`MetricHub`] 是一个进程内唯一的 descriptor catalog、记录 backend 和结构化快照源。领域 crate
//! (`nafana`/`naweb`/`nafka`)继续拥有自己的静态 family、低基数 label 与记录时机；本 crate 只统一
//! 冲突审计、容量和导出，不重新定义领域指标语义，也不依赖 OpenTelemetry、Prometheus client、
//! Axum 或 `napp`。
//!
//! # 运行架构与安全边界
//!
//! 原生记录入口只接受启动期登记且语义一致的 descriptor。受管兼容源通过
//! [`MetricHub::register_legacy_source_reserved`] 在同一临界区提交 descriptor、source 所有权和最坏
//! exposition series 预算；冲突、溢出或容量不足时三者都不改变。原生 cell 与显式预留共享
//! [`MAX_METRIC_SERIES`]，目录自身的拒绝诊断预先占位，满载时仍能解释拒绝原因。
//!
//! Prometheus 文本和 OTLP adapter 都读取同一次结构化快照，不各自维护 registry。label 形状、单值
//! 大小、value kind、histogram 结构或基数不满足目录合同时拒绝当前样本，并以固定 `source/reason`
//! 计数，不把动态 label 内容写进诊断。动态结构化兼容源只使用扣除原生序列与显式预留后的余量，
//! 按 family/label 排序逐个接纳完整样本。仅文本的旧兼容源不在结构化容量保证内。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// 固定桶与原子采集，供低开销领域 source 复用。
pub mod atomic;

/// 进程内原生、结构化兼容源与诊断按 histogram 展开后的公开序列硬上限。
pub const MAX_METRIC_SERIES: usize = 100_000;
/// 统一目录自身按两种来源和七种拒绝原因展开后的固定诊断序列数。
const INTERNAL_METRIC_SERIES: usize = 2 * SAMPLE_REJECTION_REASONS.len();

/// 单个 label 值允许占用的最大 UTF-8 字节数。
///
/// 该上限只约束单值内存，不限制 label 组合数量；基数仍由全局 cell 预算独立约束。
const MAX_LABEL_VALUE_BYTES: usize = 4 * 1024;

/// 指标类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// 单调递增计数器。
    Counter,
    /// 可增可减的瞬时值。
    Gauge,
    /// 分桶分布(Prometheus `le` 累积桶 + sum + count)。
    Histogram,
}

impl MetricKind {
    /// 业务作用：Prometheus `# TYPE` 行使用的类型名。
    fn prometheus_type(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// 一个指标 family 的静态描述。所有字段都是编译期常量,不含动态内容。
#[derive(Debug)]
pub struct MetricDescriptor {
    /// family 名称(如 `napp_web_requests_total`),进程内唯一。
    pub name: &'static str,
    /// `# HELP` 文本。
    pub help: &'static str,
    /// 单位(如 `seconds`、`bytes`;无单位用 `""`)。
    pub unit: &'static str,
    /// 指标类型。
    pub kind: MetricKind,
    /// 有序、低基数的 label 名;记录时按同一顺序提供 label 值。
    pub label_names: &'static [&'static str],
    /// histogram 的 `le` 上界(升序);非 histogram 用 `&[]`。
    pub histogram_bounds: &'static [f64],
}

static SAMPLE_REJECTIONS_TOTAL: MetricDescriptor = MetricDescriptor {
    name: "nametrics_samples_rejected_total",
    help: "统一指标目录拒绝的不合规样本累计数。",
    unit: "",
    kind: MetricKind::Counter,
    label_names: &["source", "reason"],
    histogram_bounds: &[],
};

#[derive(Clone, Copy)]
#[repr(usize)]
enum SampleRejectionReason {
    UnknownFamily,
    LabelShape,
    LabelValueTooLong,
    ValueKind,
    HistogramShape,
    UnregisteredDescriptor,
    CardinalityLimit,
}

const SAMPLE_REJECTION_REASONS: [SampleRejectionReason; 7] = [
    SampleRejectionReason::UnknownFamily,
    SampleRejectionReason::LabelShape,
    SampleRejectionReason::LabelValueTooLong,
    SampleRejectionReason::ValueKind,
    SampleRejectionReason::HistogramShape,
    SampleRejectionReason::UnregisteredDescriptor,
    SampleRejectionReason::CardinalityLimit,
];

impl SampleRejectionReason {
    /// 业务作用：返回拒绝原因在固定原子计数数组中的稳定位置。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`SAMPLE_REJECTION_REASONS`] 顺序一致的数组下标。
    const fn index(self) -> usize {
        self as usize
    }

    /// 业务作用：返回可用于低基数指标 label 的稳定拒绝原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含动态输入或样本内容的固定原因文本。
    const fn label(self) -> &'static str {
        match self {
            Self::UnknownFamily => "unknown_family",
            Self::LabelShape => "label_shape",
            Self::LabelValueTooLong => "label_value_too_long",
            Self::ValueKind => "value_kind",
            Self::HistogramShape => "histogram_shape",
            Self::UnregisteredDescriptor => "unregistered_descriptor",
            Self::CardinalityLimit => "cardinality_limit",
        }
    }
}

#[derive(Clone, Copy)]
enum SampleRejectionSource {
    Native,
    Legacy,
}

impl SampleRejectionSource {
    /// 业务作用：返回可用于低基数指标 label 的稳定样本来源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`native` 表示直接记录入口，`legacy` 表示兼容源结构化快照。
    const fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Legacy => "legacy",
        }
    }
}

/// 业务作用：以固定来源和固定原因维度保存目录拒绝计数，使基数预算耗尽时仍能暴露拒绝事实。
struct SampleRejectionCounters {
    native: [AtomicU64; SAMPLE_REJECTION_REASONS.len()],
    legacy: [AtomicU64; SAMPLE_REJECTION_REASONS.len()],
}

impl SampleRejectionCounters {
    /// 业务作用：创建按固定来源和原因分组的拒绝计数器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部计数从零开始的无锁计数器集合。
    fn new() -> Self {
        Self {
            native: std::array::from_fn(|_| AtomicU64::new(0)),
            legacy: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// 业务作用：累计一次被统一目录拒绝的样本检查事件。
    ///
    /// 参数说明：
    /// - `source`: 样本来自直接记录入口还是兼容源快照。
    /// - `reason`: 不包含动态内容的固定拒绝原因。
    ///
    /// 返回：无；对应来源与原因的累计值饱和前单调递增。
    fn increment(&self, source: SampleRejectionSource, reason: SampleRejectionReason) {
        let counters = match source {
            SampleRejectionSource::Native => &self.native,
            SampleRejectionSource::Legacy => &self.legacy,
        };
        let counter = &counters[reason.index()];
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }

    /// 业务作用：把非零拒绝计数转换为统一目录自身的结构化指标样本。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按来源、原因稳定排序的累计 counter，不暴露被拒样本的 label 内容。
    fn snapshot(&self) -> Vec<MetricSample> {
        let mut samples = Vec::new();
        for (source, counters) in [
            (SampleRejectionSource::Native, &self.native),
            (SampleRejectionSource::Legacy, &self.legacy),
        ] {
            for reason in SAMPLE_REJECTION_REASONS {
                let value = counters[reason.index()].load(Ordering::Relaxed);
                if value == 0 {
                    continue;
                }
                samples.push(MetricSample {
                    name: SAMPLE_REJECTIONS_TOTAL.name,
                    labels: vec![
                        ("source", source.label().to_owned()),
                        ("reason", reason.label().to_owned()),
                    ],
                    value: MetricValue::Counter(value),
                });
            }
        }
        samples
    }
}

impl MetricDescriptor {
    /// 业务作用：两个同名 descriptor 是否语义一致(kind/unit/help/label_names/histogram_bounds 全相同)。
    fn semantically_eq(&self, other: &MetricDescriptor) -> bool {
        self.kind == other.kind
            && self.unit == other.unit
            && self.help == other.help
            && self.label_names == other.label_names
            && self.histogram_bounds == other.histogram_bounds
    }
}

/// 领域 crate 记录指标的入口。`napp` 持有唯一 `MetricHub` 实例并实现本 trait。
pub trait MetricRecorder: Send + Sync {
    /// 业务作用：计数器累加 `delta`。
    fn counter(&self, descriptor: &'static MetricDescriptor, delta: u64, labels: &[&str]);
    /// 业务作用：设置 gauge 为 `value`。
    fn gauge(&self, descriptor: &'static MetricDescriptor, value: f64, labels: &[&str]);
    /// 业务作用：记录一次 histogram 观测 `value`。
    fn histogram(&self, descriptor: &'static MetricDescriptor, value: f64, labels: &[&str]);
}

/// descriptor 注册冲突。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricConflict {
    /// 冲突的 family 名称。
    pub name: &'static str,
}

/// 指标源在启动期登记 descriptor 与最坏序列预算时的失败分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricSourceRegistrationError {
    /// 任一 family 与已有目录的静态语义冲突。
    Conflict(MetricConflict),
    /// 本源最坏序列数会越过进程硬上限。
    SeriesBudgetExceeded,
}

impl From<MetricConflict> for MetricSourceRegistrationError {
    /// 业务作用：把既有 descriptor 冲突纳入原子指标源登记的统一失败分类。
    ///
    /// 参数说明：
    /// - `conflict`: 已包含稳定 family 名称的目录冲突。
    ///
    /// 返回：保持原始冲突事实的指标源登记错误。
    fn from(conflict: MetricConflict) -> Self {
        Self::Conflict(conflict)
    }
}

impl fmt::Display for MetricSourceRegistrationError {
    /// 业务作用：输出不携带动态 label 或指标样本内容的稳定登记失败分类。
    ///
    /// 参数说明：
    /// - `formatter`: 接收稳定错误文本的格式化目标。
    ///
    /// 返回：错误文本写入成功时完成，否则透传格式化失败。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(conflict) => {
                write!(formatter, "metric descriptor `{}` conflicts", conflict.name)
            }
            Self::SeriesBudgetExceeded => formatter.write_str("metric series budget is exceeded"),
        }
    }
}

impl std::error::Error for MetricSourceRegistrationError {}

/// 兼容期桥：旧指标源先实现本 trait，启动期用 `descriptors()` 做全局冲突审计，文本与非文本出口
/// 优先读取 `snapshot()` 的同源结构化值；明确返回 `None` 的旧源仅在文本抓取时回落到
/// `render_prometheus()`。迁移完成后弃用。
pub trait LegacyMetricsSource: Send + Sync {
    /// 业务作用：本源拥有的静态 descriptor,用于启动期冲突审计。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本源拥有且在启动期登记的全部 descriptor。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor];
    /// 业务作用：导出与文本端点同源的结构化当前值，供 OTLP 等非文本后端复用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`Some` 表示该源支持结构化快照，即使当前没有样本也返回 `Some(Vec::new())`；`None`
    /// 表示旧源只支持 Prometheus 自渲染，不会进入非文本 exporter。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        None
    }
    /// 业务作用：追加渲染本源的 Prometheus 文本(保持既有 family/HELP/TYPE/label 语义)。
    ///
    /// 参数说明：
    /// - `output`: 接收本源文本的目标缓冲区。
    ///
    /// 返回：无；在现有缓冲区尾部追加，不清空已有内容。
    fn render_prometheus(&self, output: &mut String);
}

/// 单个 (family, label 值) 组合的进程内值。
enum Cell {
    Counter(u64),
    Gauge(f64),
    Histogram {
        /// 长度 = `bounds.len() + 1`,最后一个为 `+Inf` 桶;每个是"落入该桶"的观测数(非累积)。
        buckets: Vec<u64>,
        sum: f64,
        count: u64,
    },
}

#[derive(Clone)]
struct RegisteredSource {
    source: Arc<dyn LegacyMetricsSource>,
    budget: Option<usize>,
}

/// 唯一进程级指标 backend:descriptor catalog(带冲突审计)+ 进程内记录 + 结构化/Prometheus 导出。
pub struct MetricHub {
    descriptors: RwLock<BTreeMap<&'static str, &'static MetricDescriptor>>,
    /// (family 名, label 值序列) → 值。BTreeMap 使导出顺序稳定,便于 golden 对比。
    cells: RwLock<BTreeMap<(&'static str, Vec<String>), Cell>>,
    /// 兼容领域源(nafana/naweb 等)的 registry；其 descriptor 已并入 catalog，结构化快照由
    /// hub 统一导出，只有明确不支持结构化快照的旧源才使用自身 Prometheus 文本入口。
    sources: RwLock<Vec<RegisteredSource>>,
    /// 原生 cell 展开为公开序列后的占用；写入与 cell 创建共用 cells 写锁。
    native_series: AtomicUsize,
    /// 启动期已由受管指标源原子提交的最坏导出序列数；与运行期原生 cell 共用进程硬上限。
    reserved_series: Mutex<usize>,
    /// 所有拒绝原因均为固定低基数 label；不保存被拒样本的动态内容。
    rejections: SampleRejectionCounters,
}

impl MetricHub {
    /// 业务作用：创建只预留统一目录拒绝指标、尚未登记业务指标源的 hub。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可执行 descriptor 冲突审计、记录和双出口快照的独立指标目录。
    pub fn new() -> Self {
        let mut descriptors = BTreeMap::new();
        descriptors.insert(SAMPLE_REJECTIONS_TOTAL.name, &SAMPLE_REJECTIONS_TOTAL);
        Self {
            descriptors: RwLock::new(descriptors),
            cells: RwLock::new(BTreeMap::new()),
            sources: RwLock::new(Vec::new()),
            native_series: AtomicUsize::new(0),
            // 内部拒绝族必须在任何业务预留之前占位，保证目录满载时仍能解释后续拒绝。
            reserved_series: Mutex::new(INTERNAL_METRIC_SERIES),
            rejections: SampleRejectionCounters::new(),
        }
    }

    /// 业务作用：注册一个静态 descriptor 并审计冲突。
    ///
    /// 参数说明：
    /// - `descriptor`: 需要纳入进程唯一目录的静态指标语义。
    ///
    /// 返回：首次登记或同一业务 descriptor 幂等登记时成功；内部诊断 family 与业务 family
    /// 语义冲突时失败，目录保持原状。
    ///
    /// # 错误
    ///
    /// 同名但语义不同(kind/unit/help/label_names/histogram_bounds 任一不同)时返回
    /// [`MetricConflict`];同名且完全一致是幂等的(返回 `Ok`)。
    pub fn register(&self, descriptor: &'static MetricDescriptor) -> Result<(), MetricConflict> {
        validate_descriptor(descriptor)?;
        if descriptor.name == SAMPLE_REJECTIONS_TOTAL.name
            && !std::ptr::eq(descriptor, &SAMPLE_REJECTIONS_TOTAL)
        {
            return Err(MetricConflict {
                name: descriptor.name,
            });
        }
        let mut descriptors = self
            .descriptors
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = descriptors.get(descriptor.name) {
            if existing.semantically_eq(descriptor) {
                return Ok(());
            }
            return Err(MetricConflict {
                name: descriptor.name,
            });
        }
        descriptors.insert(descriptor.name, descriptor);
        Ok(())
    }

    /// 业务作用：审计一个 [`LegacyMetricsSource`] 的 descriptor 纳入统一 catalog，并保存该源以便导出。
    ///
    /// 审计通过后，`Some` 结构化快照同时进入 Prometheus 与非文本出口；明确返回 `None` 的旧源
    /// 只进入 `render_prometheus` 回落路径。原生渲染循环跳过文本旧源声明的族名，避免重复
    /// HELP/TYPE。
    ///
    /// 参数说明：`source` 为拥有独立 family 的兼容指标源。
    ///
    /// 返回：目录无冲突时登记；结构化快照只使用全进程预留之外的动态余量，超额样本在导出时拒绝。
    ///
    /// # 错误
    ///
    /// 任一 descriptor 与已注册项冲突时返回首个 [`MetricConflict`],此时源不会被保存。
    pub fn register_legacy_source(
        &self,
        source: Arc<dyn LegacyMetricsSource>,
    ) -> Result<(), MetricConflict> {
        {
            let sources = self
                .sources
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if sources
                .iter()
                .any(|existing| Arc::ptr_eq(&existing.source, &source))
            {
                return Ok(());
            }
        }
        let candidates = source.descriptors();
        for descriptor in candidates {
            validate_descriptor(descriptor)?;
        }
        let mut descriptors = self
            .descriptors
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 兼容源拥有它声明的 family；即使语义相同，也不能与原生/另一兼容源双重拥有并渲染。
        if let Some(conflict) = candidates
            .iter()
            .find(|descriptor| descriptors.contains_key(descriptor.name))
        {
            return Err(MetricConflict {
                name: conflict.name,
            });
        }
        for descriptor in source.descriptors() {
            descriptors.insert(descriptor.name, descriptor);
        }
        drop(descriptors);
        self.sources
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(RegisteredSource {
                source,
                budget: None,
            });
        Ok(())
    }

    /// 业务作用：在一个临界区内校验并提交兼容指标源的 descriptor、所有权与最坏序列预算。
    ///
    /// 参数说明：
    /// - `source`: 只产生 sealed label domain 的结构化指标源。
    /// - `worst_case_series`: 该源全部 family 展开 label 与 histogram 后的最大公开序列数。
    ///
    /// 返回：目录无冲突且进程预留不超过 [`MAX_METRIC_SERIES`] 时原子提交；失败时 descriptor、source
    /// 和预留计数均保持不变，不会留下半份公开指标源。
    pub fn register_legacy_source_reserved(
        &self,
        source: Arc<dyn LegacyMetricsSource>,
        worst_case_series: usize,
    ) -> Result<(), MetricSourceRegistrationError> {
        let candidates = source.descriptors();
        for descriptor in candidates {
            validate_descriptor(descriptor)?;
        }
        let mut descriptors = self
            .descriptors
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut sources = self
            .sources
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sources
            .iter()
            .any(|existing| Arc::ptr_eq(&existing.source, &source))
        {
            return Ok(());
        }
        if let Some(conflict) = candidates
            .iter()
            .find(|descriptor| descriptors.contains_key(descriptor.name))
        {
            return Err(MetricConflict {
                name: conflict.name,
            }
            .into());
        }
        let cells = self
            .cells
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut reserved = self
            .reserved_series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = reserved
            .checked_add(worst_case_series)
            .and_then(|total| {
                total
                    .checked_add(self.native_series.load(Ordering::Relaxed))
                    .map(|used| (total, used))
            })
            .filter(|(_, used)| *used <= MAX_METRIC_SERIES)
            .map(|(total, _)| total)
            .ok_or(MetricSourceRegistrationError::SeriesBudgetExceeded)?;

        // 三类状态只在全部门禁通过后一起发布；任何前置失败都不能占用预算或暴露部分 family。
        for descriptor in candidates {
            descriptors.insert(descriptor.name, descriptor);
        }
        sources.push(RegisteredSource {
            source,
            budget: Some(worst_case_series),
        });
        drop(cells);
        *reserved = next;
        Ok(())
    }

    /// 业务作用：读取启动期已提交的最坏公开序列预留，供 Ready 门禁与诊断核对全进程余量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：目录自身诊断族与显式登记源的预留之和；原生 cell 的实际展开占用另行参与同一总量门禁。
    pub fn reserved_series(&self) -> usize {
        *self
            .reserved_series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 业务作用：为出口启动门禁读取已承诺的完整公开序列占用，包含已创建的原生指标。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：内部诊断与显式源的最坏预留，加上原生 cell 的展开占用；histogram 包含全部桶、sum 和 count。
    /// 无预留的兼容源与检查之后新增的原生 label 不在此值内，调用方仍需单独约束动态增长。
    pub fn committed_series(&self) -> usize {
        // 沿用创建 cell 和登记 source 的 cells → reserved 锁顺序，保证两个占用来自同一时刻，
        // 不能让并发登记或原生首次写入在 Ready 核对时落入两个读数之间的空隙。
        let _cells = self
            .cells
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reserved = self
            .reserved_series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reserved.saturating_add(self.native_series.load(Ordering::Relaxed))
    }

    /// 业务作用：校验直接记录入口的 label 数量与单值内存上限。
    ///
    /// 参数说明：
    /// - `descriptor`: 已声明 label 顺序的静态指标语义。
    /// - `labels`: 调用方按 descriptor 顺序提交的 label 值。
    ///
    /// 返回：满足数量和单值上限时成功，否则返回稳定拒绝原因。
    fn validate_labels(
        descriptor: &MetricDescriptor,
        labels: &[&str],
    ) -> Result<(), SampleRejectionReason> {
        if labels.len() != descriptor.label_names.len() {
            return Err(SampleRejectionReason::LabelShape);
        }
        if labels
            .iter()
            .any(|value| value.len() > MAX_LABEL_VALUE_BYTES)
        {
            return Err(SampleRejectionReason::LabelValueTooLong);
        }
        Ok(())
    }

    /// 业务作用：确认调用方使用的是已登记且语义完全一致的 descriptor，防止同名漂移写入。
    ///
    /// 参数说明：
    /// - `descriptor`: 本次记录入口携带的静态 descriptor。
    ///
    /// 返回：业务 descriptor 与目录语义一致时返回 `true`；内部拒绝指标只接受目录私有实例。
    fn is_registered(&self, descriptor: &MetricDescriptor) -> bool {
        if descriptor.name == SAMPLE_REJECTIONS_TOTAL.name {
            return std::ptr::eq(descriptor, &SAMPLE_REJECTIONS_TOTAL);
        }
        self.descriptors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(descriptor.name)
            .is_some_and(|registered| registered.semantically_eq(descriptor))
    }

    /// 业务作用：在原生 cell 写锁内核对并占用完整公开序列预算，histogram 不能只按一个 cell 计费。
    /// 参数说明：
    /// - `cells/key`: 已锁定的存量与本次 label 组合。
    /// - `descriptor`: 决定本次创建需要占用的完整展开序列数。
    ///
    /// 返回：存量更新不再占用；新组合容量不足时拒绝且不改变占用。
    fn can_insert_cell(
        &self,
        cells: &BTreeMap<(&'static str, Vec<String>), Cell>,
        key: &(&'static str, Vec<String>),
        descriptor: &MetricDescriptor,
    ) -> bool {
        if cells.contains_key(key) {
            return true;
        }
        let reserved = *self
            .reserved_series
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let used = self.native_series.load(Ordering::Relaxed);
        let cost = descriptor_series(descriptor);
        // 完整 histogram 在创建前一次性占位，避免桶、sum、count 越过进程硬限。
        if used
            .checked_add(cost)
            .and_then(|total| total.checked_add(reserved))
            .is_none_or(|total| total > MAX_METRIC_SERIES)
        {
            return false;
        }
        self.native_series.store(used + cost, Ordering::Relaxed);
        true
    }

    /// 业务作用：将 descriptor 名与借用 label 值固化为 hub 的有序 cell key。
    fn key(descriptor: &MetricDescriptor, labels: &[&str]) -> (&'static str, Vec<String>) {
        (
            descriptor.name,
            labels.iter().map(|value| (*value).to_owned()).collect(),
        )
    }

    /// 业务作用：导出全部原生与兼容源指标的结构化快照，family 与 label 组合按名称有序。
    ///
    /// 兼容源必须直接读取自己的原子 registry，不从 Prometheus 文本反解析。源返回的 family、label
    /// 或 value kind 与启动期 descriptor 不一致时，该样本会被拒绝，避免非文本出口绕过 catalog。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 family 与 label 有序且符合全局公开序列预算的当前快照；拒绝事实在同次快照中可见，
    /// 读取不会清零计数或 histogram。
    pub fn snapshot(&self) -> Vec<MetricSample> {
        self.collect_snapshot().0
    }

    /// 业务作用：一次性收集原生与结构化兼容源样本，并识别只能自渲染文本的旧源。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：第一项是经过 descriptor 校验的统一快照，第二项是明确返回 `None` 的文本旧源；结构化
    /// 源即使当前为空或全部样本被拒绝也不会退回另一条数据路径。
    fn collect_snapshot(&self) -> (Vec<MetricSample>, Vec<Arc<dyn LegacyMetricsSource>>) {
        let mut out = Vec::new();
        let mut text_only_sources = Vec::new();
        let sources = self
            .sources
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let native = self.native_snapshot();
        let native_count = native
            .iter()
            .filter(|sample| sample.name != SAMPLE_REJECTIONS_TOTAL.name)
            .map(sample_series)
            .sum::<usize>();
        let reserved = sources
            .iter()
            .filter_map(|entry| entry.budget)
            .sum::<usize>();
        let mut remaining = MAX_METRIC_SERIES
            .saturating_sub(INTERNAL_METRIC_SERIES)
            .saturating_sub(native_count)
            .saturating_sub(reserved);
        for entry in sources {
            let source = entry.source;
            let Some(mut samples) = source.snapshot() else {
                text_only_sources.push(source);
                continue;
            };
            let owned: BTreeMap<&'static str, &'static MetricDescriptor> = source
                .descriptors()
                .iter()
                .map(|descriptor| (descriptor.name, *descriptor))
                .collect();
            // 固定排序使容量拒绝不依赖兼容源的枚举顺序；受管源不能挤占其它源已承诺的预算。
            samples.sort_by(|left, right| {
                left.name
                    .cmp(right.name)
                    .then_with(|| left.labels.cmp(&right.labels))
            });
            let mut available = entry.budget.unwrap_or(remaining);
            for sample in samples {
                let Some(descriptor) = owned.get(sample.name).copied() else {
                    self.rejections.increment(
                        SampleRejectionSource::Legacy,
                        SampleRejectionReason::UnknownFamily,
                    );
                    continue;
                };
                match validate_sample(&sample, descriptor) {
                    Ok(()) => {
                        let cost = sample_series(&sample);
                        // 容量不足时拒绝整个样本，不能发布缺少部分桶或 sum/count 的 histogram。
                        if cost > available {
                            self.rejections.increment(
                                SampleRejectionSource::Legacy,
                                SampleRejectionReason::CardinalityLimit,
                            );
                            continue;
                        }
                        available -= cost;
                        out.push(sample);
                    }
                    Err(reason) => self
                        .rejections
                        .increment(SampleRejectionSource::Legacy, reason),
                }
            }
            if entry.budget.is_none() {
                remaining = available;
            }
        }
        out.extend(
            native
                .into_iter()
                .filter(|sample| sample.name != SAMPLE_REJECTIONS_TOTAL.name),
        );
        // 拒绝发生后再读固定诊断，使本次导出本身就能解释容量损失。
        out.extend(self.rejections.snapshot());
        out.sort_by(|left, right| {
            left.name
                .cmp(right.name)
                .then_with(|| left.labels.cmp(&right.labels))
        });
        (out, text_only_sources)
    }

    /// 业务作用：导出由 hub 直接记录的指标值，不触发兼容源的快照副作用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 hub 有序 cell 生成的当前值以及非零拒绝诊断；调用时不清零任何计数。
    fn native_snapshot(&self) -> Vec<MetricSample> {
        let descriptors = self
            .descriptors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cells = self
            .cells
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out = Vec::with_capacity(cells.len());
        for ((name, label_values), cell) in cells.iter() {
            let Some(descriptor) = descriptors.get(name) else {
                continue;
            };
            let labels: Vec<(&'static str, String)> = descriptor
                .label_names
                .iter()
                .copied()
                .zip(label_values.iter().cloned())
                .collect();
            let value = match cell {
                Cell::Counter(v) => MetricValue::Counter(*v),
                Cell::Gauge(v) => MetricValue::Gauge(*v),
                Cell::Histogram {
                    buckets,
                    sum,
                    count,
                } => MetricValue::Histogram {
                    bounds: descriptor.histogram_bounds,
                    buckets: buckets.clone(),
                    sum: *sum,
                    count: *count,
                },
            };
            out.push(MetricSample {
                name,
                labels,
                value,
            });
        }
        drop(cells);
        drop(descriptors);
        out.extend(self.rejections.snapshot());
        out
    }

    /// 业务作用：把当前值按启动期 descriptor 聚合成可直接编码的指标 family 快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只包含当前至少有一个样本的 family；名称、说明、单位、类型与 histogram 边界均来自
    /// 唯一 catalog，OTLP 与 Prometheus 不会各自维护第二份指标语义。
    pub fn family_snapshot(&self) -> Vec<MetricFamilySnapshot> {
        let samples = self.snapshot();
        let descriptors = self
            .descriptors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut families: BTreeMap<&'static str, MetricFamilySnapshot> = BTreeMap::new();
        for sample in samples {
            let Some(descriptor) = descriptors.get(sample.name).copied() else {
                continue;
            };
            families
                .entry(descriptor.name)
                .or_insert_with(|| MetricFamilySnapshot {
                    name: descriptor.name,
                    description: descriptor.help,
                    unit: descriptor.unit,
                    kind: descriptor.kind,
                    histogram_bounds: descriptor.histogram_bounds,
                    samples: Vec::new(),
                })
                .samples
                .push(sample);
        }
        families.into_values().collect()
    }

    /// 业务作用：把 hub 拥有的全部指标渲染为 Prometheus 文本(HELP/TYPE + 样本),追加到 `output`。
    ///
    /// 渲染顺序:先按名称有序输出原生 family 和结构化兼容源，再按注册顺序处理文本旧源。返回
    /// `Some(Vec::new())` 的结构化源只表示当前无样本，不会调用另一份自渲染入口。
    /// 同一 family 始终只有一组 HELP/TYPE，结构化源与 OTLP 共用同一值和边界。
    ///
    /// 参数说明：
    /// - `output`: 接收完整 Prometheus exposition 的目标缓冲区。
    ///
    /// 返回：无；在现有缓冲区尾部追加，不清空已有内容。
    pub fn render_prometheus(&self, output: &mut String) {
        use std::fmt::Write as _;
        // 单次收集同时决定结构化与文本旧源，避免“结构化快照为空”在不同出口被解释成不同数据源。
        let (samples, text_only_sources) = self.collect_snapshot();
        let text_only_names: BTreeSet<&'static str> = text_only_sources
            .iter()
            .flat_map(|source| source.descriptors().iter().map(|d| d.name))
            .collect();
        let descriptors = self
            .descriptors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 按 family 分组渲染原生族,每个 family 一组 HELP/TYPE。
        for (name, descriptor) in descriptors.iter() {
            if text_only_names.contains(name) {
                continue;
            }
            if *name == SAMPLE_REJECTIONS_TOTAL.name
                && !samples.iter().any(|sample| sample.name == *name)
            {
                continue;
            }
            let _ = writeln!(output, "# HELP {name} {}", descriptor.help);
            let _ = writeln!(
                output,
                "# TYPE {name} {}",
                descriptor.kind.prometheus_type()
            );
            for sample in samples.iter().filter(|s| s.name == *name) {
                sample.render_prometheus(output);
            }
        }
        drop(descriptors);
        // 只有显式声明不支持结构化快照的旧源使用自身文本入口；结构化源的文本与 OTLP 始终共用上面的值。
        for source in text_only_sources {
            source.render_prometheus(output);
        }
    }
}

/// 业务作用：计算 descriptor 的一个 label 组合占用的公开序列数。
/// 参数说明：`descriptor` 为已校验的静态指标语义。
/// 返回：普通样本占一条，histogram 包括所有有限桶、+Inf、sum 和 count。
fn descriptor_series(descriptor: &MetricDescriptor) -> usize {
    if descriptor.kind == MetricKind::Histogram {
        descriptor.histogram_bounds.len() + 3
    } else {
        1
    }
}

/// 业务作用：按实际样本结构核算出口容量。
/// 参数说明：`sample` 为已经过 descriptor 校验的完整样本。
/// 返回：与 Prometheus 展开结果一致的序列数。
fn sample_series(sample: &MetricSample) -> usize {
    match &sample.value {
        MetricValue::Histogram { bounds, .. } => bounds.len() + 3,
        _ => 1,
    }
}

/// 业务作用：验证兼容源样本与启动期 descriptor 的名称、label 顺序、资源上限和 value kind 完全一致。
///
/// 参数说明：
/// - `sample`: 兼容源从自身 registry 读取的当前值。
/// - `descriptor`: 该源在注册时提交并通过冲突审计的静态语义。
///
/// 返回：样本可安全进入统一快照时成功；不符合合同则返回可观测的固定拒绝原因。
fn validate_sample(
    sample: &MetricSample,
    descriptor: &MetricDescriptor,
) -> Result<(), SampleRejectionReason> {
    if sample.name != descriptor.name
        || sample.labels.len() != descriptor.label_names.len()
        || sample
            .labels
            .iter()
            .zip(descriptor.label_names)
            .any(|((actual, _), expected)| actual != expected)
    {
        return Err(SampleRejectionReason::LabelShape);
    }
    if sample
        .labels
        .iter()
        .any(|(_, value)| value.len() > MAX_LABEL_VALUE_BYTES)
    {
        return Err(SampleRejectionReason::LabelValueTooLong);
    }
    match (&sample.value, descriptor.kind) {
        (MetricValue::Counter(_), MetricKind::Counter)
        | (MetricValue::Gauge(_), MetricKind::Gauge) => Ok(()),
        (
            MetricValue::Histogram {
                bounds,
                buckets,
                count,
                ..
            },
            MetricKind::Histogram,
        ) => {
            if *bounds == descriptor.histogram_bounds
                && buckets.len() == bounds.len() + 1
                && buckets.iter().copied().fold(0_u64, u64::saturating_add) == *count
            {
                Ok(())
            } else {
                Err(SampleRejectionReason::HistogramShape)
            }
        }
        _ => Err(SampleRejectionReason::ValueKind),
    }
}

impl Default for MetricHub {
    /// 业务作用：创建空指标目录与样本存储。
    fn default() -> Self {
        Self::new()
    }
}

impl MetricRecorder for MetricHub {
    /// 业务作用：对已登记 counter 做饱和累加，并对不合规写入累计固定原因诊断。
    ///
    /// 参数说明：
    /// - `descriptor`: 启动期登记的静态 counter 语义。
    /// - `delta`: 本次需要累计的非负增量。
    /// - `labels`: 按 descriptor 顺序提供的低基数 label 值。
    ///
    /// 返回：无；合法写入饱和累加，非法 descriptor、label 或超基数写入不改变业务样本。
    fn counter(&self, descriptor: &'static MetricDescriptor, delta: u64, labels: &[&str]) {
        if let Err(reason) = Self::validate_labels(descriptor, labels) {
            self.rejections
                .increment(SampleRejectionSource::Native, reason);
            return;
        }
        if !self.is_registered(descriptor) {
            self.rejections.increment(
                SampleRejectionSource::Native,
                SampleRejectionReason::UnregisteredDescriptor,
            );
            return;
        }
        let mut cells = self
            .cells
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::key(descriptor, labels);
        if !self.can_insert_cell(&cells, &key, descriptor) {
            self.rejections.increment(
                SampleRejectionSource::Native,
                SampleRejectionReason::CardinalityLimit,
            );
            return;
        }
        match cells.entry(key).or_insert(Cell::Counter(0)) {
            Cell::Counter(v) => *v = v.saturating_add(delta),
            _ => debug_assert!(false, "metric `{}` kind mismatch", descriptor.name),
        }
    }

    /// 业务作用：覆盖已登记 gauge 的当前值，并对不合规写入累计固定原因诊断。
    ///
    /// 参数说明：
    /// - `descriptor`: 启动期登记的静态 gauge 语义。
    /// - `value`: 需要发布的当前值。
    /// - `labels`: 按 descriptor 顺序提供的低基数 label 值。
    ///
    /// 返回：无；合法写入覆盖当前值，非法 descriptor、label 或超基数写入不改变业务样本。
    fn gauge(&self, descriptor: &'static MetricDescriptor, value: f64, labels: &[&str]) {
        if let Err(reason) = Self::validate_labels(descriptor, labels) {
            self.rejections
                .increment(SampleRejectionSource::Native, reason);
            return;
        }
        if !self.is_registered(descriptor) {
            self.rejections.increment(
                SampleRejectionSource::Native,
                SampleRejectionReason::UnregisteredDescriptor,
            );
            return;
        }
        let mut cells = self
            .cells
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::key(descriptor, labels);
        if !self.can_insert_cell(&cells, &key, descriptor) {
            self.rejections.increment(
                SampleRejectionSource::Native,
                SampleRejectionReason::CardinalityLimit,
            );
            return;
        }
        match cells.entry(key).or_insert(Cell::Gauge(0.0)) {
            Cell::Gauge(v) => *v = value,
            _ => debug_assert!(false, "metric `{}` kind mismatch", descriptor.name),
        }
    }

    /// 业务作用：把观测值计入首个匹配边界或 `+Inf` 桶，并对不合规写入累计固定原因诊断。
    ///
    /// 参数说明：
    /// - `descriptor`: 启动期登记的静态 histogram 语义与边界。
    /// - `value`: 本次观测值。
    /// - `labels`: 按 descriptor 顺序提供的低基数 label 值。
    ///
    /// 返回：无；合法写入同步累计 bucket、sum、count，非法写入不改变业务样本。
    fn histogram(&self, descriptor: &'static MetricDescriptor, value: f64, labels: &[&str]) {
        if let Err(reason) = Self::validate_labels(descriptor, labels) {
            self.rejections
                .increment(SampleRejectionSource::Native, reason);
            return;
        }
        if !self.is_registered(descriptor) {
            self.rejections.increment(
                SampleRejectionSource::Native,
                SampleRejectionReason::UnregisteredDescriptor,
            );
            return;
        }
        let bounds = descriptor.histogram_bounds;
        let mut cells = self
            .cells
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::key(descriptor, labels);
        if !self.can_insert_cell(&cells, &key, descriptor) {
            self.rejections.increment(
                SampleRejectionSource::Native,
                SampleRejectionReason::CardinalityLimit,
            );
            return;
        }
        let cell = cells.entry(key).or_insert_with(|| Cell::Histogram {
            buckets: vec![0; bounds.len() + 1],
            sum: 0.0,
            count: 0,
        });
        if let Cell::Histogram {
            buckets,
            sum,
            count,
        } = cell
        {
            // 落入第一个 `value <= bound` 的桶;都不满足则落入 +Inf 桶(最后一个)。
            let index = bounds
                .iter()
                .position(|bound| value <= *bound)
                .unwrap_or(bounds.len());
            buckets[index] = buckets[index].saturating_add(1);
            *sum += value;
            *count = count.saturating_add(1);
        } else {
            debug_assert!(false, "metric `{}` kind mismatch", descriptor.name);
        }
    }
}

/// 业务作用：校验 Prometheus descriptor 结构。错误沿用 `MetricConflict` 以保持现有 API，但注册不会产生副作用。
fn validate_descriptor(descriptor: &'static MetricDescriptor) -> Result<(), MetricConflict> {
    let valid_name = |name: &str| {
        let mut bytes = name.bytes();
        bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':'))
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':'))
    };
    let valid_label = |name: &str| {
        let mut bytes = name.bytes();
        bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    };
    let labels: BTreeSet<&str> = descriptor.label_names.iter().copied().collect();
    let invalid = !valid_name(descriptor.name)
        || descriptor.help.is_empty()
        || descriptor.help.contains(['\n', '\r'])
        || labels.len() != descriptor.label_names.len()
        || descriptor
            .label_names
            .iter()
            .any(|name| !valid_label(name) || *name == "le")
        || match descriptor.kind {
            MetricKind::Histogram => {
                descriptor.histogram_bounds.is_empty()
                    || descriptor
                        .histogram_bounds
                        .iter()
                        .any(|bound| !bound.is_finite())
                    || descriptor
                        .histogram_bounds
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
            }
            MetricKind::Counter | MetricKind::Gauge => !descriptor.histogram_bounds.is_empty(),
        };
    if invalid {
        Err(MetricConflict {
            name: descriptor.name,
        })
    } else {
        Ok(())
    }
}

/// 单个指标样本(family + label 组合 + 值)的结构化快照。
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// family 名称。
    pub name: &'static str,
    /// (label 名, label 值) 有序对。
    pub labels: Vec<(&'static str, String)>,
    /// 样本值。
    pub value: MetricValue,
}

/// 一个带唯一 descriptor 语义的指标 family 当前快照。
#[derive(Debug, Clone)]
pub struct MetricFamilySnapshot {
    /// family 名称。
    pub name: &'static str,
    /// 稳定业务说明。
    pub description: &'static str,
    /// UCUM 单位；无单位时为空串。
    pub unit: &'static str,
    /// counter、gauge 或 histogram 类型。
    pub kind: MetricKind,
    /// histogram 显式边界；其它类型为空切片。
    pub histogram_bounds: &'static [f64],
    /// 按 label 有序的当前数据点。
    pub samples: Vec<MetricSample>,
}

impl MetricSample {
    /// 业务作用：渲染本样本的 Prometheus 文本行(不含 HELP/TYPE),追加到 `output`。
    fn render_prometheus(&self, output: &mut String) {
        use std::fmt::Write as _;
        match &self.value {
            MetricValue::Counter(v) => {
                let _ = writeln!(output, "{}{} {v}", self.name, self.render_labels(&[]));
            }
            MetricValue::Gauge(v) => {
                let _ = writeln!(output, "{}{} {v}", self.name, self.render_labels(&[]));
            }
            MetricValue::Histogram {
                bounds,
                buckets,
                sum,
                count,
            } => {
                // 累积 `le` 桶。
                let mut cumulative = 0u64;
                for (i, bound) in bounds.iter().enumerate() {
                    cumulative = cumulative.saturating_add(buckets[i]);
                    let le = format!("{bound}");
                    let _ = writeln!(
                        output,
                        "{}_bucket{} {cumulative}",
                        self.name,
                        self.render_labels(&[("le", le.as_str())])
                    );
                }
                cumulative = cumulative.saturating_add(buckets[bounds.len()]);
                let _ = writeln!(
                    output,
                    "{}_bucket{} {cumulative}",
                    self.name,
                    self.render_labels(&[("le", "+Inf")])
                );
                let _ = writeln!(output, "{}_sum{} {sum}", self.name, self.render_labels(&[]));
                let _ = writeln!(
                    output,
                    "{}_count{} {count}",
                    self.name,
                    self.render_labels(&[])
                );
            }
        }
    }

    /// 业务作用：渲染 `{k="v",...}` label 集合;`extra` 追加在 descriptor label 之后(如 histogram 的 `le`)。
    fn render_labels(&self, extra: &[(&str, &str)]) -> String {
        let mut parts: Vec<String> = self
            .labels
            .iter()
            .map(|(k, v)| format!("{k}=\"{}\"", escape_label(v)))
            .collect();
        for (k, v) in extra {
            parts.push(format!("{k}=\"{}\"", escape_label(v)));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", parts.join(","))
        }
    }
}

/// 业务作用：Prometheus label 值转义(`\`、`"`、换行)。
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// 指标样本值。
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// 计数器当前值。
    Counter(u64),
    /// gauge 当前值。
    Gauge(f64),
    /// histogram 分布。
    Histogram {
        /// `le` 上界(升序)。
        bounds: &'static [f64],
        /// 每桶观测数(非累积;长度 = `bounds.len() + 1`)。
        buckets: Vec<u64>,
        /// 观测值之和。
        sum: f64,
        /// 观测次数。
        count: u64,
    },
}
