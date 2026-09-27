//! 进程生命周期单调计数 + 延迟直方图——Prometheus 的事实源(合同 计数口径)。
//!
//! rolling.rs 负责 10s 实时快照；二者在热路径同源记录、各管一轨:
//! counter/bucket/sum/count 永远单调递增,**绝不**从滚动窗口重算(合同)。
//! 热路径使用无锁原子；直方图先登记总数，再以 Release 发布 bucket，保证并发快照不会看到
//! bucket 领先于 count 的不完整观测。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// 结局/降级/TPS 的单调计数,一个 Command 一份。
#[derive(Default)]
pub(crate) struct CommandCounters {
    /// 成功(非 5xx)累计。
    pub(crate) success: AtomicU64,
    /// 失败(5xx)累计。
    pub(crate) failure: AtomicU64,
    /// 超时累计。
    pub(crate) timeout: AtomicU64,
    /// bulkhead 拒绝累计。
    pub(crate) rejected: AtomicU64,
    /// 执行 future 在产生正常结局前被取消或中止的累计。
    pub(crate) canceled: AtomicU64,
    /// 降级响应产出累计。
    pub(crate) fallback: AtomicU64,
    /// 全局降级成功产出业务响应累计。
    pub(crate) global_fallback_handled: AtomicU64,
    /// 全局处理器主动退回内置响应累计。
    pub(crate) global_fallback_builtin: AtomicU64,
    /// 全局处理器配置冲突、崩溃或递归累计。
    pub(crate) global_fallback_failed: AtomicU64,
    /// TPS 累计:每请求(含被拒、被取消)按 tps_weight 增加;未标 TPS 的命令恒 0。
    pub(crate) tps: AtomicU64,
}

/// 直方图桶上界(毫秒)。渲染时换算成秒作 `le` 标签;+Inf 桶由 count 表达。
pub(crate) const LATENCY_BOUNDS_MS: [u64; 13] =
    [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// 与 [`LATENCY_BOUNDS_MS`] 一一对应的 `le` 标签字符串(秒),预置常量避免浮点格式化漂移。
pub(crate) const LATENCY_LE_LABELS: [&str; 13] = [
    "0.001", "0.002", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
];

/// 延迟直方图:桶计数按"首个 ≥ 样本值的上界"入桶(内部非累计,渲染时按 le 累计)。
/// 只记录产生完整执行耗时的请求(success/failure/timeout);rejected/canceled 不进延迟统计。
pub(crate) struct LatencyHistogram {
    /// 各桶命中数(非累计);超出最大上界的样本只进 count/sum(即 +Inf)。
    buckets: [AtomicU64; 13],
    /// 样本总数(= +Inf 桶的累计值)。
    count: AtomicU64,
    /// 样本总耗时(微秒累计,渲染时换算成秒)。
    sum_micros: AtomicU64,
}

impl LatencyHistogram {
    /// 业务作用：构造全零直方图。
    pub(crate) fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    /// 业务作用：记录一次执行耗时，并按 count 先于 bucket 的发布顺序维持并发快照不变量。
    ///
    /// 参数说明：
    /// - `elapsed`: 本次执行耗时（进入执行区到产出结局）。
    ///
    /// 返回：无；累计总数、命中 bucket 与耗时总和，读取方一旦看到 bucket 增量就一定能看到对应总数。
    pub(crate) fn observe(&self, elapsed: Duration) {
        let micros = elapsed.as_micros() as u64;
        // 总数必须先于 bucket 登记；bucket 的 Release 与快照的 Acquire 配对，避免统一指标目录
        // 把并发写入中的正常样本误判为 bucket 总和超过 count 的畸形数据。
        self.count.fetch_add(1, Ordering::Relaxed);
        for (i, bound_ms) in LATENCY_BOUNDS_MS.iter().enumerate() {
            if micros <= bound_ms * 1000 {
                self.buckets[i].fetch_add(1, Ordering::Release);
                break;
            }
        }
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    /// 业务作用：导出可供文本与结构化出口共用的直方图当前视图。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 `le` 语义累计的 bucket、总数与秒数总和；并发写入时允许短暂把尚未可见的有限
    /// bucket 计入 `+Inf`，但不会返回 bucket 总和超过 count 的快照，下一次读取会自然收敛。
    pub(crate) fn export(&self) -> HistogramExport {
        let mut cumulative = [0u64; 13];
        let mut acc = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            acc += b.load(Ordering::Acquire);
            cumulative[i] = acc;
        }
        HistogramExport {
            cumulative,
            count: self.count.load(Ordering::Acquire),
            sum_seconds: self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }
}

/// 直方图的渲染视图。
pub(crate) struct HistogramExport {
    /// 按 `le` 累计后的各桶值(与 [`LATENCY_LE_LABELS`] 对齐)。
    pub(crate) cumulative: [u64; 13],
    /// 样本总数(+Inf 桶)。
    pub(crate) count: u64,
    /// 样本总耗时(秒)。
    pub(crate) sum_seconds: f64,
}
