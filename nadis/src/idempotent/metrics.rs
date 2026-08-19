//! 幂等计数观测：低基数累计与布局事实；`ttl_missing` 一旦发生即让健康保持 degraded 直至进程重启。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use super::config::IdempotentTtlMode;
use super::result::IdempotentRejection;

/// 幂等计数长期复用的线程安全指标容器；family 固定、label 封闭，进入统一指标目录无动态基数。
#[derive(Debug, Default)]
pub struct IdempotentCounterMetrics {
    applied: AtomicU64,
    duplicate: AtomicU64,
    ttl_missing: AtomicU64,
    probe_failures: AtomicU64,
    rejected_ledger_type: AtomicU64,
    rejected_operation: AtomicU64,
    rejected_command: AtomicU64,
    /// `ttl_missing` 出现后置位并保持：凭证 TTL 未确认意味着回收风险，不能因后续成功自动清零。
    degraded: AtomicBool,
    layout: Mutex<LayoutFact>,
}

/// 已解析布局的只读事实快照。
#[derive(Debug, Clone, Default)]
struct LayoutFact {
    marker: String,
    mode: Option<IdempotentTtlMode>,
}

impl IdempotentCounterMetrics {
    /// 业务作用：记录一次首次成功计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；`applied_total` 单调加一。
    pub(crate) fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录一次窗口内重复 nonce。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；`duplicate_total` 单调加一。
    pub(crate) fn record_duplicate(&self) {
        self.duplicate.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录一次凭证 TTL 未确认，并把健康永久降级至进程重启。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；`ttl_missing_total` 加一且 `degraded` 置位，不再自动恢复。
    pub(crate) fn record_ttl_missing(&self) {
        self.ttl_missing.fetch_add(1, Ordering::Relaxed);
        // 一旦有凭证未挂 TTL，账本可能残留旧 nonce；健康必须保持 degraded，避免运维误判为完全健康。
        self.degraded.store(true, Ordering::Release);
    }

    /// 业务作用：按封闭原因记录一次拒绝。
    ///
    /// 参数说明：
    /// - `reason`: 结构化拒绝原因。
    ///
    /// 返回：无；对应 `rejected_total{reason}` 单调加一。
    pub(crate) fn record_rejected(&self, reason: IdempotentRejection) {
        match reason {
            IdempotentRejection::LedgerType => &self.rejected_ledger_type,
            IdempotentRejection::Operation => &self.rejected_operation,
            IdempotentRejection::Command => &self.rejected_command,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：记录一次全 master 能力探测不完整，供运维判断布局降级原因。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；`probe_failures_total` 单调加一。
    pub(crate) fn record_probe_failure(&self) {
        self.probe_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用：发布已解析布局事实，供指标导出模式与 marker。
    ///
    /// 参数说明：
    /// - `mode`: 已解析的 `HASH_FIELD`/`HASH_BUCKET`。
    /// - `marker`: Redis 中实际生效的 marker 文本。
    ///
    /// 返回：无；覆盖布局事实快照。
    pub(crate) fn resolve_layout(&self, mode: IdempotentTtlMode, marker: &str) {
        let mut fact = self
            .layout
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fact.mode = Some(mode);
        fact.marker = marker.to_owned();
    }

    /// 业务作用：一次读取全部幂等计数观测事实，供健康端点与指标桥接。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：封闭累计、拒绝分类、布局事实与降级标志的快照。
    pub fn snapshot(&self) -> IdempotentCounterSnapshot {
        let fact = self
            .layout
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        IdempotentCounterSnapshot {
            applied_total: self.applied.load(Ordering::Relaxed),
            duplicate_total: self.duplicate.load(Ordering::Relaxed),
            ttl_missing_total: self.ttl_missing.load(Ordering::Relaxed),
            probe_failures_total: self.probe_failures.load(Ordering::Relaxed),
            rejected_ledger_type_total: self.rejected_ledger_type.load(Ordering::Relaxed),
            rejected_operation_total: self.rejected_operation.load(Ordering::Relaxed),
            rejected_command_total: self.rejected_command.load(Ordering::Relaxed),
            degraded: self.degraded.load(Ordering::Acquire),
            resolved_ttl_mode: fact.mode,
            layout_marker: fact.marker.clone(),
        }
    }
}

/// 幂等计数在某一时刻的完整观测事实。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotentCounterSnapshot {
    /// 首次成功计数总数。
    pub applied_total: u64,
    /// 窗口内重复 nonce 总数。
    pub duplicate_total: u64,
    /// 凭证 TTL 未确认总数；大于 0 时健康保持 degraded。
    pub ttl_missing_total: u64,
    /// 能力探测不完整总数。
    pub probe_failures_total: u64,
    /// 账本类型拒绝总数。
    pub rejected_ledger_type_total: u64,
    /// 操作非法拒绝总数。
    pub rejected_operation_total: u64,
    /// 原生命令拒绝总数。
    pub rejected_command_total: u64,
    /// 是否因 TTL 未确认进入不可自动恢复的降级。
    pub degraded: bool,
    /// 已解析的 TTL 模式；未解析布局时为 `None`。
    pub resolved_ttl_mode: Option<IdempotentTtlMode>,
    /// 实际生效的 layout marker 文本。
    pub layout_marker: String,
}
