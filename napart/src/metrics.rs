//! Runner 低基数指标与有界失败证据。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::{RunnerHealth, RunnerPhase, TaskStatus, TaskType};

/// 不持有业务载荷的任务冻结证据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenEvidence {
    /// 稳定任务类型；兼容入口使用保留类型。
    pub task_type: TaskType,
    /// key 哈希得到的原始分区。
    pub partition: u32,
    /// 权威终态，当前固定为 Failed。
    pub status: TaskStatus,
    /// 稳定失败原因。
    pub reason: Option<&'static str>,
}

/// 有界冻结诊断快照。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrozenEvidenceSnapshot {
    /// 最近的有界诊断样本，按登记先后排列。
    pub recent: Vec<FrozenEvidence>,
    /// generation 生命周期内累计登记数。
    pub total: u64,
    /// 环形缓冲覆盖的旧样本数。
    pub overwritten: u64,
}

/// Runner 当前指标快照；全部字段都是低基数累计值或资源瞬时值。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunnerMetricsSnapshot {
    /// 当前生命周期阶段。
    pub phase: RunnerPhase,
    /// 当前 readiness 健康状态。
    pub health: RunnerHealth,
    /// 当前 generation epoch。
    pub epoch: u64,
    /// worker 分区数。
    pub partitions: u64,
    /// 当前仍可接纳任务的 slot 数。
    pub accepting_partitions: u64,
    /// 当前隔离失败的 slot 数。
    pub failed_partitions: u64,
    /// 已受理任务总数。
    pub submitted: u64,
    /// 正常完成任务总数。
    pub completed: u64,
    /// 执行前取消任务总数。
    pub cancelled: u64,
    /// 失败任务总数。
    pub failed: u64,
    /// 业务 Future 展开总数。
    pub task_panics: u64,
    /// 有损停止中止执行中任务总数。
    pub aborted: u64,
    /// 因 Runner 停止被拒总数。
    pub rejected_shutting_down: u64,
    /// 因类型排队容量耗尽被拒总数。
    pub rejected_queue_full: u64,
    /// 因 Runner 全局预算耗尽被拒总数。
    pub rejected_overloaded: u64,
    /// 因类型顺序要求冲突被拒总数。
    pub rejected_ordering_conflict: u64,
    /// 因类型状态失败被拒总数。
    pub rejected_lane_failed: u64,
    /// 因类型状态上限耗尽被拒总数。
    pub rejected_lane_limit: u64,
    /// 因使用兼容保留类型被拒总数。
    pub rejected_reserved_type: u64,
    /// 集中观察发布的盗洞机会总数。
    pub steal_attempts: u64,
    /// 成功安装盗洞总数。
    pub steal_successes: u64,
    /// 严格盗洞完成归还总数。
    pub releases: u64,
    /// 冻结任务累计数。
    pub frozen: u64,
    /// 当前类型状态数；兼容旧字段名称 `lanes`。
    pub lanes: u64,
    /// 当前 generation 允许的类型状态总上限。
    pub type_state_limit: u64,
    /// 当前失败类型数；兼容旧字段名称 `failed_lanes`。
    pub failed_lanes: u64,
    /// 当前异常退出 worker 数。
    pub dead_workers: i64,
    /// 全部类型已受理但尚未进入 Running 的任务数，覆盖入队、移动、盗洞和 worker 暂存，
    /// 不包含仍在等待另一层容量的提交调用。
    pub queued_depth: u64,
    /// 全部 slot 主队列的物理任务深度。
    pub main_queue_depth: u64,
    /// 全部 slot 控制队列的物理请求深度。
    pub control_queue_depth: u64,
    /// 主队列与控制队列当前被 reservation 阻塞的队头数量。
    pub reserved_queue_heads: u64,
    /// 当前位于队列序号分配或跨调用 reservation 临界区的 producer 数。
    pub producer_inflight: u64,
    /// Runner 全局预算剩余许可。
    pub admit_available: u64,
    /// 当前未到期延迟任务数。
    pub delayed_pending: u64,
    /// 当前 timer 物理槽位数；取消完成后应与逻辑索引同步下降。
    pub timer_physical_slots: u64,
    /// generation 内成功物理移除定时槽位的取消次数。
    pub timer_cancelled: u64,
    /// generation 内为保持物理槽位有界执行的定时堆压缩次数。
    pub timer_compactions: u64,
    /// 当前执行中业务任务数。
    pub running: u64,
    /// 当前物理移动任务数。
    pub moving: u64,
    /// 当前活动非严格盗洞数。
    pub non_strict_tunnels: u64,
    /// 当前活动严格盗洞数。
    pub strict_tunnels: u64,
    /// 当前仍开放 producer 的非严格盗洞数。
    pub non_strict_tunnels_open: u64,
    /// 当前已经关门但尚未物理排空的非严格盗洞数。
    pub non_strict_tunnels_draining: u64,
    /// 严格盗洞 stock FIFO 物理深度。
    pub strict_stock_depth: u64,
    /// 严格盗洞 incremental FIFO 物理深度。
    pub strict_incremental_depth: u64,
    /// 严格盗洞 staging FIFO 物理深度。
    pub strict_staging_depth: u64,
    /// 当前处于 Local 的严格类型数。
    pub strict_local: u64,
    /// 当前处于 Migrating 的严格类型数。
    pub strict_migrating: u64,
    /// 当前处于 Stolen 的严格类型数。
    pub strict_stolen: u64,
    /// 当前处于 ReturnPrepare 的严格类型数。
    pub strict_return_prepare: u64,
    /// 当前处于 Returning 的严格类型数。
    pub strict_returning: u64,
    /// 当前处于 LocalCatchup 的严格类型数。
    pub strict_local_catchup: u64,
    /// 当前处于 StolenCatchup 的严格类型数。
    pub strict_stolen_catchup: u64,
    /// 当前处于 Failed 的严格类型数。
    pub strict_failed: u64,
    /// 冻结诊断环当前保留的样本数。
    pub frozen_evidence_samples: u64,
    /// 冻结诊断环已经覆盖的旧样本数。
    pub frozen_evidence_overwritten: u64,
    /// 当前受监督但尚未 join 的内部任务数。
    pub supervised_tasks: u64,
}

/// 兼容旧执行器公开名称的指标别名。
pub type MetricsSnapshot = RunnerMetricsSnapshot;

pub(crate) struct RunnerMetrics {
    pub(crate) submitted: AtomicU64,
    pub(crate) completed: AtomicU64,
    pub(crate) cancelled: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) task_panics: AtomicU64,
    pub(crate) aborted: AtomicU64,
    pub(crate) frozen: AtomicU64,
    pub(crate) rejected_shutting_down: AtomicU64,
    pub(crate) rejected_queue_full: AtomicU64,
    pub(crate) rejected_overloaded: AtomicU64,
    pub(crate) rejected_ordering_conflict: AtomicU64,
    pub(crate) rejected_type_failed: AtomicU64,
    pub(crate) rejected_type_limit: AtomicU64,
    pub(crate) rejected_reserved_type: AtomicU64,
    pub(crate) steal_attempts: AtomicU64,
    pub(crate) steal_successes: AtomicU64,
    pub(crate) releases: AtomicU64,
    pub(crate) failed_types: AtomicU64,
    pub(crate) dead_workers: AtomicI64,
    pub(crate) running: AtomicU64,
    pub(crate) moving: AtomicU64,
    pub(crate) non_strict_tunnels: AtomicU64,
    pub(crate) strict_tunnels: AtomicU64,
}

impl RunnerMetrics {
    /// 业务作用：创建全部计数为零的 generation 指标集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可由 Runner 内部并发更新的原子指标。
    pub(crate) fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            task_panics: AtomicU64::new(0),
            aborted: AtomicU64::new(0),
            frozen: AtomicU64::new(0),
            rejected_shutting_down: AtomicU64::new(0),
            rejected_queue_full: AtomicU64::new(0),
            rejected_overloaded: AtomicU64::new(0),
            rejected_ordering_conflict: AtomicU64::new(0),
            rejected_type_failed: AtomicU64::new(0),
            rejected_type_limit: AtomicU64::new(0),
            rejected_reserved_type: AtomicU64::new(0),
            steal_attempts: AtomicU64::new(0),
            steal_successes: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            failed_types: AtomicU64::new(0),
            dead_workers: AtomicI64::new(0),
            running: AtomicU64::new(0),
            moving: AtomicU64::new(0),
            non_strict_tunnels: AtomicU64::new(0),
            strict_tunnels: AtomicU64::new(0),
        }
    }
}

pub(crate) struct FrozenEvidenceRing {
    capacity: usize,
    recent: Mutex<VecDeque<FrozenEvidence>>,
    total: AtomicU64,
    overwritten: AtomicU64,
}

impl FrozenEvidenceRing {
    /// 业务作用：创建固定容量诊断环，样本永不持有任务或业务 Future 强引用。
    ///
    /// 参数说明：
    /// - `capacity`: 最多保留的最近证据条数。
    ///
    /// 返回：累计和覆盖计数为零的诊断环。
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            recent: Mutex::new(VecDeque::with_capacity(capacity)),
            total: AtomicU64::new(0),
            overwritten: AtomicU64::new(0),
        }
    }

    /// 业务作用：登记一条稳定失败证据；容量满时覆盖最旧样本但保留累计事实。
    ///
    /// 参数说明：
    /// - `evidence`: 不含业务载荷的诊断记录。
    ///
    /// 返回：无；累计计数先于样本可见。
    pub(crate) fn push(&self, evidence: FrozenEvidence) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let mut recent = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        if recent.len() == self.capacity {
            recent.pop_front();
            self.overwritten.fetch_add(1, Ordering::Relaxed);
        }
        recent.push_back(evidence);
    }

    /// 业务作用：导出有界证据、累计数和覆盖数的一致诊断快照。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不持有内部锁或任务引用的独立快照。
    pub(crate) fn snapshot(&self) -> FrozenEvidenceSnapshot {
        FrozenEvidenceSnapshot {
            recent: self
                .recent
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .cloned()
                .collect(),
            total: self.total.load(Ordering::Acquire),
            overwritten: self.overwritten.load(Ordering::Acquire),
        }
    }

    /// 业务作用：读取诊断环当前样本数与覆盖数，指标路径无需克隆完整证据集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：当前样本数和累计覆盖数。
    pub(crate) fn counts(&self) -> (u64, u64) {
        (
            self.recent.lock().unwrap_or_else(|e| e.into_inner()).len() as u64,
            self.overwritten.load(Ordering::Acquire),
        )
    }
}
