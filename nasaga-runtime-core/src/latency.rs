//! Saga 各阶段的进程内耗时直方图；固定阶段标签不包含业务身份。

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// 阶段耗时的秒单位桶边界；最后一个桶包含超过最大边界的样本。
pub const SAGA_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// 业务作用：固定采样阶段，使错误输入和动态定义不能扩张指标标签集合。
#[derive(Clone, Copy)]
pub(crate) enum LatencyStage {
    Handler,
    ParticipantTransaction,
    StartTransaction,
    TransitionTransaction,
    TimerLateness,
}

const STAGES: [&str; 5] = [
    "participant_handler",
    "participant_transaction",
    "start_transaction",
    "transition_transaction",
    "timer_lateness",
];

/// 业务作用：表达一个固定阶段的非累积直方图快照，供 Prometheus 与 OTLP 共用。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SagaLatencySnapshot {
    /// 固定阶段名，不包含 tenant、workflow、payload 或实例身份。
    pub stage: &'static str,
    /// 已观测尝试数；包含失败或取消的处理耗时，不代表提交成功数。
    pub count: u64,
    /// 所有尝试累计微秒数。
    pub micros_sum: u64,
    /// 非累积桶计数，长度为边界数加一。
    pub buckets: Vec<u64>,
}

/// 业务作用：获取进程共享的固定大小指标存储。
///
/// 参数说明: 无。
///
/// 返回：一次初始化的同步存储；不随租户或定义数量增长。
fn metrics() -> &'static Mutex<Vec<SagaLatencySnapshot>> {
    static METRICS: OnceLock<Mutex<Vec<SagaLatencySnapshot>>> = OnceLock::new();
    METRICS.get_or_init(|| {
        Mutex::new(
            STAGES
                .iter()
                .map(|stage| SagaLatencySnapshot {
                    stage,
                    buckets: vec![0; SAGA_LATENCY_BUCKETS.len() + 1],
                    ..Default::default()
                })
                .collect(),
        )
    })
}

/// 业务作用：读取全部阶段的同一时刻快照，不清零累计值。
///
/// 参数说明: 无。
///
/// 返回：固定五个阶段的计数和桶；进程重启后重新累计。
pub fn saga_latency_snapshot() -> Vec<SagaLatencySnapshot> {
    metrics()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// 业务作用：记录真实耗时或数据库 timer 到期后的调度滞后。
///
/// 参数说明：`stage` 是固定采样边界，`duration` 是单调耗时或非负调度滞后。
///
/// 返回：更新一个阶段的计数、总量和非累积桶，不改变任何业务状态。
pub(crate) fn observe(stage: LatencyStage, duration: Duration) {
    let mut metrics = metrics()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let metric = &mut metrics[stage as usize];
    let bucket = SAGA_LATENCY_BUCKETS.partition_point(|bound| *bound < duration.as_secs_f64());
    metric.count = metric.count.saturating_add(1);
    metric.micros_sum = metric
        .micros_sum
        .saturating_add(duration.as_micros().min(u64::MAX as u128) as u64);
    metric.buckets[bucket] = metric.buckets[bucket].saturating_add(1);
}

/// 业务作用：将异步取消与提前失败的占用时间也纳入阶段耗时。
pub(crate) struct LatencyGuard {
    stage: LatencyStage,
    started: Instant,
}

impl LatencyGuard {
    /// 业务作用：在阶段开始时固定单调时钟。
    ///
    /// 参数说明：`stage` 指定阶段边界。
    ///
    /// 返回：离开作用域时采样的守卫，不持有指标锁跨越业务等待。
    pub(crate) fn new(stage: LatencyStage) -> Self {
        Self {
            stage,
            started: Instant::now(),
        }
    }
}

impl Drop for LatencyGuard {
    /// 业务作用：在成功、错误或取消路径离开阶段时记录实际占用时间。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：更新指标；不影响事务提交或回滚的裁决。
    fn drop(&mut self) {
        observe(self.stage, self.started.elapsed());
    }
}

/// 业务作用：限定参与方 handler 的采样范围，不混入 Inbox、gate 或 Outbox 数据库等待。
///
/// 参数说明：`body` 是业务 handler 的异步调用。
///
/// 返回：透传业务返回值；耗时不被解释为成功或失败的证明。
pub(crate) async fn measure_handler<T>(body: impl std::future::Future<Output = T>) -> T {
    let _latency = LatencyGuard::new(LatencyStage::Handler);
    body.await
}

/// 业务作用：将固定阶段耗时渲染为可聚合直方图，支持 histogram_quantile 查询。
///
/// 参数说明: 无。
///
/// 返回：包含完整累积桶、总量和计数的 Prometheus 文本，不包含业务身份标签。
pub fn render_saga_latency_metrics() -> String {
    use std::fmt::Write as _;
    let name = "nasaga_stage_duration_seconds";
    let mut output = format!("# TYPE {name} histogram\n");
    for sample in saga_latency_snapshot() {
        let mut cumulative = 0u64;
        for (index, bucket) in sample.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*bucket);
            let bound = SAGA_LATENCY_BUCKETS
                .get(index)
                .map(f64::to_string)
                .unwrap_or_else(|| "+Inf".to_owned());
            let _ = writeln!(
                output,
                "{name}_bucket{{stage=\"{}\",le=\"{bound}\"}} {cumulative}",
                sample.stage
            );
        }
        let _ = writeln!(
            output,
            "{name}_count{{stage=\"{}\"}} {}",
            sample.stage, sample.count
        );
        let _ = writeln!(
            output,
            "{name}_sum{{stage=\"{}\"}} {}",
            sample.stage,
            sample.micros_sum as f64 / 1_000_000.0
        );
    }
    output
}
