//! 进程级 provider-neutral 指标核心。
//!
//! 提供唯一的进程级 descriptor catalog、进程内记录与结构化/Prometheus 文本导出。领域 crate
//! (`nafana`/`naweb`/`nafka`)拥有自己的静态指标名、低基数 label 和记录时机;本 crate 只拥有
//! **descriptor 冲突审计**和 **metric backend**,不重新拥有领域指标语义,也**不依赖**
//! OpenTelemetry / Prometheus client / Axum / `napp`(否则会形成反向依赖)。
//!
//! 设计要点:
//! - 记录入口只接受**启动期注册过的静态 descriptor**;label 数量必须与 `label_names` 精确匹配,
//!   否则该次记录被丢弃(debug 下断言),防止高基数/错位 label。
//! - `MetricHub::register` 审计同名 descriptor:`kind`/`unit`/`help`/`label_names`/`histogram_bounds`
//!   任一不同都在启动期返回冲突错误,避免同一 family 在不同 crate 被赋予不同语义。
//! - Prometheus 文本由本 crate 统一渲染(HELP/TYPE + 样本),Web scrape adapter 与 OTLP exporter
//!   都读取同一 `MetricHub`,不各自建 registry。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

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

/// 兼容期桥:旧的自渲染指标源(`nafana::render_metrics`、`naweb::SecurityMetrics` 等)先实现本
/// trait,启动期用 `descriptors()` 做全局冲突审计,抓取时才调用 `render_prometheus`。迁移完成后弃用。
pub trait LegacyMetricsSource: Send + Sync {
    /// 业务作用：本源拥有的静态 descriptor,用于启动期冲突审计。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor];
    /// 业务作用：追加渲染本源的 Prometheus 文本(保持既有 family/HELP/TYPE/label 语义)。
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

/// 唯一进程级指标 backend:descriptor catalog(带冲突审计)+ 进程内记录 + 结构化/Prometheus 导出。
pub struct MetricHub {
    descriptors: RwLock<BTreeMap<&'static str, &'static MetricDescriptor>>,
    /// (family 名, label 值序列) → 值。BTreeMap 使导出顺序稳定,便于 golden 对比。
    cells: RwLock<BTreeMap<(&'static str, Vec<String>), Cell>>,
    /// 兼容领域源(nafana/naweb 等)自渲染其族的 Prometheus 文本;其 descriptor 已并入
    /// catalog 供冲突审计,但值仍由源自渲染。渲染时 hub 原生循环跳过这些族名以避免重复 HELP/TYPE。
    sources: RwLock<Vec<Arc<dyn LegacyMetricsSource>>>,
}

impl MetricHub {
    /// 业务作用：创建空 hub。
    pub fn new() -> Self {
        Self {
            descriptors: RwLock::new(BTreeMap::new()),
            cells: RwLock::new(BTreeMap::new()),
            sources: RwLock::new(Vec::new()),
        }
    }

    /// 业务作用：注册一个静态 descriptor 并审计冲突。
    ///
    /// # 错误
    ///
    /// 同名但语义不同(kind/unit/help/label_names/histogram_bounds 任一不同)时返回
    /// [`MetricConflict`];同名且完全一致是幂等的(返回 `Ok`)。
    pub fn register(&self, descriptor: &'static MetricDescriptor) -> Result<(), MetricConflict> {
        validate_descriptor(descriptor)?;
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

    /// 业务作用：审计一个 [`LegacyMetricsSource`] 的 descriptor 纳入统一 catalog,并保存该源以便渲染。
    ///
    /// 审计通过后,`render_prometheus` 会在原生族之后追加该源自渲染的族文本;原生渲染循环
    /// 跳过该源声明的族名,避免重复的 HELP/TYPE 行。
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
                .any(|existing| Arc::ptr_eq(existing, &source))
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
            .push(source);
        Ok(())
    }

    /// 业务作用：label 值切片是否与 descriptor 的 `label_names` 数量匹配。
    fn labels_match(descriptor: &MetricDescriptor, labels: &[&str]) -> bool {
        labels.len() == descriptor.label_names.len()
            && labels.iter().all(|value| value.len() <= 256)
    }

    /// 业务作用：确认调用方使用的是已登记且语义完全一致的 descriptor，防止同名漂移写入。
    fn is_registered(&self, descriptor: &MetricDescriptor) -> bool {
        self.descriptors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(descriptor.name)
            .is_some_and(|registered| registered.semantically_eq(descriptor))
    }

    /// 业务作用：只允许更新现有序列或在全局基数预算内创建新 label 组合。
    fn can_insert_cell(
        cells: &BTreeMap<(&'static str, Vec<String>), Cell>,
        key: &(&'static str, Vec<String>),
    ) -> bool {
        cells.contains_key(key) || cells.len() < 100_000
    }

    /// 业务作用：将 descriptor 名与借用 label 值固化为 hub 的有序 cell key。
    fn key(descriptor: &MetricDescriptor, labels: &[&str]) -> (&'static str, Vec<String>) {
        (
            descriptor.name,
            labels.iter().map(|value| (*value).to_owned()).collect(),
        )
    }

    /// 业务作用：导出全部已记录指标的结构化快照,family 与 label 组合按名称有序。
    pub fn snapshot(&self) -> Vec<MetricSample> {
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
        out
    }

    /// 业务作用：把 hub 拥有的全部指标渲染为 Prometheus 文本(HELP/TYPE + 样本),追加到 `output`。
    ///
    /// 渲染顺序:先按名称有序输出**原生族**(hub 直接记录的 descriptor + 样本),跳过兼容源声明的
    /// 族名;再按注册顺序追加每个 [`LegacyMetricsSource`] 自渲染的族文本。这样一次调用即得到 nafka
    /// (原生)+ nafana/naweb(兼容源)的统一 exposition,且同一 family 只有一组 HELP/TYPE。
    pub fn render_prometheus(&self, output: &mut String) {
        use std::fmt::Write as _;
        // 先取 snapshot(内部完成锁的获取与释放),再取其余读锁,避免同线程嵌套读锁
        // (std RwLock 递归读锁在部分平台会死锁)。
        let samples = self.snapshot();
        let sources = self
            .sources
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 兼容源声明的族名:原生循环必须跳过,值由源自渲染,否则重复 HELP/TYPE。
        let legacy_names: BTreeSet<&'static str> = sources
            .iter()
            .flat_map(|source| source.descriptors().iter().map(|d| d.name))
            .collect();
        let descriptors = self
            .descriptors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 按 family 分组渲染原生族,每个 family 一组 HELP/TYPE。
        for (name, descriptor) in descriptors.iter() {
            if legacy_names.contains(name) {
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
        // 追加兼容源自渲染的族文本(nafana/naweb 各自的 registry)。
        for source in sources.iter() {
            source.render_prometheus(output);
        }
    }
}

impl Default for MetricHub {
    /// 业务作用：创建空指标目录与样本存储。
    fn default() -> Self {
        Self::new()
    }
}

impl MetricRecorder for MetricHub {
    /// 业务作用：对已登记 counter 做饱和累加；非法 descriptor、label 或超基数写入被拒绝。
    fn counter(&self, descriptor: &'static MetricDescriptor, delta: u64, labels: &[&str]) {
        debug_assert!(
            Self::labels_match(descriptor, labels),
            "counter `{}` label count mismatch",
            descriptor.name
        );
        if !Self::labels_match(descriptor, labels) || !self.is_registered(descriptor) {
            return;
        }
        let mut cells = self
            .cells
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::key(descriptor, labels);
        if !Self::can_insert_cell(&cells, &key) {
            return;
        }
        match cells.entry(key).or_insert(Cell::Counter(0)) {
            Cell::Counter(v) => *v = v.saturating_add(delta),
            _ => debug_assert!(false, "metric `{}` kind mismatch", descriptor.name),
        }
    }

    /// 业务作用：覆盖已登记 gauge 的当前值；非法 descriptor、label 或超基数写入被拒绝。
    fn gauge(&self, descriptor: &'static MetricDescriptor, value: f64, labels: &[&str]) {
        debug_assert!(
            Self::labels_match(descriptor, labels),
            "gauge `{}` label count mismatch",
            descriptor.name
        );
        if !Self::labels_match(descriptor, labels) || !self.is_registered(descriptor) {
            return;
        }
        let mut cells = self
            .cells
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::key(descriptor, labels);
        if !Self::can_insert_cell(&cells, &key) {
            return;
        }
        match cells.entry(key).or_insert(Cell::Gauge(0.0)) {
            Cell::Gauge(v) => *v = value,
            _ => debug_assert!(false, "metric `{}` kind mismatch", descriptor.name),
        }
    }

    /// 业务作用：把观测值计入首个匹配边界或 `+Inf` 桶，并同步累计 sum/count。
    fn histogram(&self, descriptor: &'static MetricDescriptor, value: f64, labels: &[&str]) {
        debug_assert!(
            Self::labels_match(descriptor, labels),
            "histogram `{}` label count mismatch",
            descriptor.name
        );
        if !Self::labels_match(descriptor, labels) || !self.is_registered(descriptor) {
            return;
        }
        let bounds = descriptor.histogram_bounds;
        let mut cells = self
            .cells
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = Self::key(descriptor, labels);
        if !Self::can_insert_cell(&cells, &key) {
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
