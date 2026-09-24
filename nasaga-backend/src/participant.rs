//! Saga Participant gate 的数据库无关输入与准入结果。

use nasaga_core::{
    DefinitionVersion, SagaId, StepCompensationStatus, StepForwardStatus, StepName,
    StepResolutionStatus, TenantId, WorkflowName,
};

/// 业务作用：标识参与方 gate 所属步骤并携带 envelope 合同复验字段。
#[derive(Debug, Clone)]
pub struct ParticipantGateKey<'a> {
    /// 实例身份。
    pub saga_id: &'a SagaId,
    /// 步骤名称。
    pub step: &'a StepName,
    /// 租户身份。
    pub tenant: &'a TenantId,
    /// workflow 名称。
    pub workflow: &'a WorkflowName,
    /// definition 版本。
    pub definition_version: DefinitionVersion,
    /// definition canonical 摘要。
    pub definition_digest: &'a str,
}

/// 业务作用：区分 execute 准入、终态重放和取消屏障抑制。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteAdmission {
    /// 准入成功；业务效果与 settle 必须留在同一事务。
    Admitted,
    /// 该效果已有确定终态，不重复业务效果。
    AlreadyTerminal(StepForwardStatus),
    /// 取消屏障已建立，执行被本地状态拒绝。
    Suppressed,
}

/// 业务作用：区分补偿准入、既有裁决和缺少正向事实的协议违规。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompensationAdmission {
    /// 准入成功；补偿与 settle 必须留在同一事务。
    Admitted,
    /// 本地已有补偿终态或不确定事实，不重复补偿业务。
    AlreadySettled(StepCompensationStatus),
    /// 本地无正向成功效果且无补偿证据。
    MissingForwardEffect,
}

/// 业务作用：区分取消屏障确认、正向既有终态和仍待解决事实。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelAdjudication {
    /// 执行从未开始，admission fence 已建立。
    Confirmed,
    /// 执行已有确定终态。
    AlreadyTerminal(StepForwardStatus),
    /// 已提交外部 intent、调用在途或结果未知。
    ResolutionPending,
}

/// 业务作用：区分 externally-cancellable 取消是否需要调用业务 handler。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalCancelAdmission {
    /// 已取得 gate 串行化权威，可以调用外部取消 handler。
    Admitted,
    /// 本地已有真实取消裁决，禁止重复调用外部系统。
    AlreadyAdjudicated(CancelAdjudication),
}

/// 业务作用：标识 resolve 正在裁决正向效果还是补偿效果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionTarget {
    /// 裁决此前未知的正向效果。
    Forward,
    /// 裁决此前未知的补偿效果。
    Compensation,
}

/// 业务作用：区分 resolve 查询准入、终态重放和缺少未知事实的协议违规。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionAdmission {
    /// 存在待裁决的未知效果，可以执行查询 handler。
    Admitted(ResolutionTarget),
    /// 本地已有确定裁决，不再次查询或执行裁决型副作用。
    AlreadySettled {
        /// 既有裁决所属方向。
        target: ResolutionTarget,
        /// 已提交的解决终态。
        status: StepResolutionStatus,
    },
    /// 本地没有可查询的未知效果，也没有目标明确、可安全重放的既有事实。
    MissingUnknownEffect,
}
