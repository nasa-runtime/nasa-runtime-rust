//! Saga step 投影和 attempt journal 的后端中立结果。

use nasaga_core::{
    StepAttemptStatus, StepCancelStatus, StepCompensationStatus, StepForwardStatus,
    StepResolutionStatus,
};

/// 业务作用：区分 attempt 首次登记和完全一致的幂等重放。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStart {
    /// 本事务真实登记了该 attempt。
    Recorded,
    /// 同一 attempt 已以相同身份登记过，无新副作用。
    AlreadyRecorded,
}

/// 业务作用：区分 outcome 首次记账、幂等重放和互斥事实冲突。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcomeRecord {
    /// 本事务真实写入了该终态。
    Recorded,
    /// 同一 attempt 已记录完全相同的终态。
    AlreadyRecorded,
    /// 同一 attempt 已有不同终态，调用方必须升级人工介入。
    Conflicting {
        /// journal 中已经提交的真实终态。
        existing: StepAttemptStatus,
    },
}

/// 业务作用：描述一次 step journal 投影更新；空字段保留持久列既有值。
#[derive(Debug, Clone, Copy, Default)]
pub struct StepJournalPatch<'a> {
    /// 正向阶段状态；为空保留既有值。
    pub forward_status: Option<StepForwardStatus>,
    /// 取消屏障裁决状态；为空保留既有值。
    pub cancel_status: Option<StepCancelStatus>,
    /// 补偿阶段状态；为空保留既有值。
    pub compensation_status: Option<StepCompensationStatus>,
    /// 解决阶段状态；为空保留既有值。
    pub resolution_status: Option<StepResolutionStatus>,
    /// 稳定失败原因码；为空保留既有值。
    pub last_error_code: Option<&'a str>,
    /// 为真时把结束时刻定格为数据库当前时刻。
    pub mark_finished: bool,
}
