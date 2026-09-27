//! Saga 状态迁移、控制、管理和冲突事实的数据库无关审计模型。

use nasaga_core::{AttemptNo, SagaId, StepAttemptStatus, StepName, StepPhase};

/// 业务作用：以数据库分配的全局单调序号定位最后一条已交付审计事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SagaAuditEventCursor {
    /// 最后一条已返回事件的全局序号；零表示从实例历史起点读取。
    pub audit_seq: u64,
}

/// 业务作用：用封闭类别承载不可变审计事件在发生时刻的事实快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaAuditEventRecord {
    /// command attempt 开始或终态变化快照。
    Attempt(SagaAttemptAuditRow),
    /// Saga 状态迁移事实。
    Transition(SagaTransitionAuditRow),
    /// pause/resume 控制态事实。
    Control(SagaControlAuditRow),
    /// 人工恢复或关闭事实。
    Management(SagaManagementAuditRow),
    /// 互斥结果证据。
    Conflict(SagaConflictFactRow),
}

/// 业务作用：把全局单调序号与一条不可变审计事实绑定，供跨类别分页和断线续读。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaAuditEventRow {
    /// 数据库在事实事务内分配的全局单调序号。
    pub audit_seq: u64,
    /// 事实发生时的封闭类别快照。
    pub record: SagaAuditEventRecord,
}

/// 业务作用：定位 attempt 审计稳定列序中的最后一条已返回事实。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaAttemptAuditCursor {
    /// 步骤名称游标。
    pub step: StepName,
    /// 阶段游标。
    pub phase: StepPhase,
    /// attempt 序号游标。
    pub attempt: AttemptNo,
}

/// 业务作用：定位以不可变数据库时间和稳定业务身份排序的最后一条审计事实。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaTimedAuditCursor {
    /// 数据库格式化的 UTC 微秒时间文本。
    pub occurred_at: String,
    /// 同一时间内的稳定业务身份。
    pub identity: String,
}

/// 业务作用：把 attempt journal 事实与其不可变开始时间组合为公开审计记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaAttemptAuditRow {
    /// attempt 完整事实。
    pub attempt: crate::SagaStepAttemptRow,
    /// 数据库格式化的 UTC 微秒开始时间。
    pub occurred_at: String,
    /// UTC epoch 毫秒，用于公开 protobuf Timestamp。
    pub occurred_at_ms: i64,
}

/// 业务作用：区分结果事实冲突的稳定类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaConflictKind {
    /// 同一 attempt 收到两个互斥终态。
    AttemptTerminal,
    /// 迟到 phase 结果与其它 phase 已提交强事实互斥。
    CrossPhaseFact,
}

impl SagaConflictKind {
    /// 业务作用：返回冲突类别的稳定持久化名称。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可用于数据库和低基数观测的稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AttemptTerminal => "attempt_terminal_conflict",
            Self::CrossPhaseFact => "cross_phase_fact_conflict",
        }
    }
}

/// 业务作用：表示序号与实例版本同源的一条业务状态迁移审计。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaTransitionAuditRow {
    /// 状态迁移序号。
    pub transition_seq: u64,
    /// 迁移前状态；初始创建为 `NONE`。
    pub from_state: String,
    /// 迁移后状态。
    pub to_state: String,
    /// event、timer 或 admin 等触发类别。
    pub trigger_kind: String,
    /// 稳定触发身份。
    pub trigger_id: String,
    /// 驱动迁移的 definition 版本。
    pub definition_version: u32,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
    /// UTC epoch 毫秒，用于公开 protobuf Timestamp。
    pub occurred_at_ms: i64,
}

/// 业务作用：表示一次 pause/resume 控制态 CAS 与主体审计。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaControlAuditRow {
    /// 独立 control generation。
    pub control_seq: u64,
    /// 切换前控制状态。
    pub from_state: String,
    /// 切换后控制状态。
    pub to_state: String,
    /// 管理请求幂等身份。
    pub operation_id: String,
    /// 认证主体稳定 id。
    pub actor: String,
    /// 业务原因。
    pub reason: String,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
    /// UTC epoch 毫秒，用于公开 protobuf Timestamp。
    pub occurred_at_ms: i64,
}

/// 业务作用：表示一次改变业务状态或发布命令的人工恢复操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaManagementAuditRow {
    /// 管理请求幂等身份。
    pub operation_id: String,
    /// 稳定低基数动作名。
    pub action: String,
    /// 认证主体稳定 id。
    pub actor: String,
    /// 业务原因。
    pub reason: String,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
    /// UTC epoch 毫秒，用于公开 protobuf Timestamp。
    pub occurred_at_ms: i64,
}

/// 业务作用：表示同一 attempt 收到互斥终态的人工介入证据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaConflictFactRow {
    /// 后到矛盾结果的 event id。
    pub incoming_event_id: String,
    /// 发生冲突的步骤。
    pub step: StepName,
    /// 发生冲突的阶段。
    pub phase: StepPhase,
    /// 发生冲突的 attempt。
    pub attempt: AttemptNo,
    /// journal 中先到且不可覆盖的终态。
    pub existing_status: StepAttemptStatus,
    /// 后到 envelope 携带的互斥终态。
    pub incoming_status: StepAttemptStatus,
    /// 稳定冲突类别。
    pub conflict_kind: String,
    /// 数据库格式化的 UTC 时间文本。
    pub occurred_at: String,
    /// UTC epoch 毫秒，用于公开 protobuf Timestamp。
    pub occurred_at_ms: i64,
}

/// 业务作用：聚合一条互斥结果证据的身份与双方裁决，避免跨参数错位。
#[derive(Debug, Clone, Copy)]
pub struct AttemptConflictFact<'a> {
    /// Saga 实例身份。
    pub saga_id: &'a SagaId,
    /// 冲突步骤。
    pub step: &'a StepName,
    /// 冲突阶段。
    pub phase: StepPhase,
    /// 冲突 attempt。
    pub attempt: AttemptNo,
    /// journal 已提交的先到终态。
    pub existing_status: StepAttemptStatus,
    /// 后到 envelope 携带的互斥终态。
    pub incoming_status: StepAttemptStatus,
    /// 后到结果事件身份。
    pub incoming_event_id: &'a str,
    /// 稳定冲突类别。
    pub conflict_kind: SagaConflictKind,
}
