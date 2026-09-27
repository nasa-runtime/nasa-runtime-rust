//! 预分配原子计数与固定桶分布；写侧不构造标签，快照侧转换为统一指标值。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::MetricValue;

/// 业务作用：在独立 source 的文本出口复用统一样本编码，避免与 OTLP 快照产生语义分叉。
/// 参数说明：`descriptors` 为静态指标合同，`samples` 为同一轮快照，`output` 接收文本。
/// 返回：追加带 HELP/TYPE 的 Prometheus 文本，不执行采集或网络动作。
pub fn render_snapshot(
    descriptors: &[&crate::MetricDescriptor],
    samples: &[crate::MetricSample],
    output: &mut String,
) {
    use std::fmt::Write;
    for descriptor in descriptors {
        let _ = writeln!(output, "# HELP {} {}", descriptor.name, descriptor.help);
        let _ = writeln!(
            output,
            "# TYPE {} {}",
            descriptor.name,
            descriptor.kind.prometheus_type()
        );
        for sample in samples
            .iter()
            .filter(|sample| sample.name == descriptor.name)
        {
            sample.render_prometheus(output);
        }
    }
}

/// 数据库客户端、方法、连接等待与首行延迟的固定秒桶。
pub const LATENCY_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];
/// 流连接占用生命周期的固定秒桶。
pub const STREAM_LIFETIME_SECONDS: &[f64] = &[
    0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0,
];

/// 业务作用：饱和累加长期运行的计数，避免溢出后把累计事实变成小值。
///
/// 参数说明：`counter` 为目标单元，`amount` 为本次增量。
/// 返回：无；达到 u64 上限后保持上限。
pub fn add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

/// 固定容量直方图；各桶非累积存储，时长总和以微秒饱和累加。
pub struct AtomicHistogram {
    bounds: &'static [f64],
    buckets: [AtomicU64; 16],
    micros: AtomicU64,
}

impl AtomicHistogram {
    /// 业务作用：为静态描述符建立无需热路径分配的桶单元。
    ///
    /// 参数说明：`bounds` 必须为升序秒边界，最多十五个。
    /// 返回：全部为零的固定桶分布；非正、非有限、无序边界或超出容量立即拒绝。
    pub const fn new(bounds: &'static [f64]) -> Self {
        assert!(bounds.len() < 16);
        let mut index = 0;
        while index < bounds.len() {
            assert!(bounds[index] > 0.0 && bounds[index] < f64::INFINITY);
            assert!(index == 0 || bounds[index - 1] < bounds[index]);
            index += 1;
        }
        Self {
            bounds,
            buckets: [const { AtomicU64::new(0) }; 16],
            micros: AtomicU64::new(0),
        }
    }

    /// 业务作用：把一次客户端观察时长记入唯一非累积桶。
    ///
    /// 参数说明：`duration` 来自单调时钟，超过最大边界进入无限桶。
    /// 返回：无；不分配、不取锁，不影响被观察操作的结果。
    pub fn observe(&self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        let bucket = self.bounds.partition_point(|bound| *bound < seconds);
        add(
            &self.micros,
            duration.as_micros().min(u64::MAX as u128) as u64,
        );
        add(&self.buckets[bucket], 1);
    }

    /// 业务作用：在出口侧读取近似并发快照，保证 count 与同份桶之和一致。
    ///
    /// 参数说明：无。
    /// 返回：包含固定边界、非累积桶、秒总和及计数的统一指标值。
    pub fn snapshot(&self) -> MetricValue {
        let buckets: Vec<u64> = self.buckets[..=self.bounds.len()]
            .iter()
            .map(|cell| cell.load(Ordering::Relaxed))
            .collect();
        let count = buckets.iter().copied().fold(0u64, u64::saturating_add);
        MetricValue::Histogram {
            bounds: self.bounds,
            buckets,
            sum: self.micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            count,
        }
    }
}
