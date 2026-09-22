//! Claim 任期与本任期内批次序号；两者共同过滤异步迟到事件。

use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CLAIM_EPOCH: AtomicU64 = AtomicU64::new(1);

/// 来源持锁任期与批次的联合身份；任期不随读取或重试递增。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BatchIdentity {
    pub(super) claim_epoch: u64,
    pub(super) batch_seq: u64,
}

impl BatchIdentity {
    /// 业务作用：签发进程内不复用的 Claim 任期，隔离同分区再次取得锁后的旧事件。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：新任期从批次零开始；计数空间耗尽时拒绝接管，绝不回绕。
    pub(super) fn new_claim() -> Option<Self> {
        NEXT_CLAIM_EPOCH
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .ok()
            .map(|claim_epoch| Self {
                claim_epoch,
                batch_seq: 0,
            })
    }

    /// 业务作用：为同一持锁任期的下一轮读取或恢复签发批次身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：保留 claim_epoch 并递增 batch_seq；耗尽时不改变当前身份。
    pub(super) fn advance(&mut self) -> Option<Self> {
        self.batch_seq = self.batch_seq.checked_add(1)?;
        Some(*self)
    }
}
