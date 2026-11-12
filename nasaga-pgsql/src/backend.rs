//! PostgreSQL Saga store 对后端中立分组能力的兼容实现。

use async_trait::async_trait;
use nasaga_backend::{
    ActionRateReservation, AttemptConflictFact, AttemptOutcomeRecord, AttemptStart,
    CancelAdjudication, CasOutcome, CompensationAdmission, ControlCasOutcome,
    ControlTransitionSpec, ExecuteAdmission, ExternalCancelAdmission, ManagementAuditOutcome,
    NewSagaInstance, ParticipantGateKey, QuotaReservation, ResolutionAdmission, ResolutionTarget,
    SagaAttemptAuditCursor, SagaAttemptAuditRow, SagaAuditEventCursor, SagaAuditEventRow,
    SagaAuditStore, SagaBackendError, SagaBackendErrorKind, SagaConflictFactRow,
    SagaControlAuditRow, SagaCreation, SagaGovernanceStore, SagaInstanceQuery, SagaInstanceRow,
    SagaInstanceStore, SagaInstanceSummary, SagaJournalStore, SagaManagementAuditRow,
    SagaParticipantStore, SagaStepAttemptRow, SagaStepRow, SagaStoreMetrics, SagaTimedAuditCursor,
    SagaTimerStore, SagaTransitionAuditRow, StepJournalPatch, TimerClaimBatch, TimerFencing,
    TimerFencingToken, TimerReschedule, TimerSchedule, TimerScope, TimerSpec, TransitionSpec,
};
use nasaga_core::{
    AttemptNo, BusinessKey, CommandId, CompensationPlan, Direction, EffectId, SagaId, SagaStatus,
    StepAttemptStatus, StepCancelStatus, StepCompensationStatus, StepForwardStatus, StepName,
    StepPhase, StepResolutionStatus, TenantId, WorkflowDefinition, WorkflowName,
};

use crate::error::SagaStoreErrorKind;
use crate::{PgSagaStore, SagaStoreError};

/// 业务作用：统一当前事务路径中具体存储返回的脱敏错误类别。
///
/// 参数说明：
/// - `error`：具体存储提供的稳定错误种类和原因。
///
/// 返回：连接不可用或数据库操作错误映射 Retryable，冲突和基础设施错误保持对应类别；不在此验证事务已回滚，是否可重试仍由事务边界裁决。
fn map_backend_error(error: SagaStoreError) -> SagaBackendError {
    let kind = match error.kind() {
        SagaStoreErrorKind::ConnectionUnavailable | SagaStoreErrorKind::DatabaseOperation => {
            SagaBackendErrorKind::Retryable
        }
        SagaStoreErrorKind::Conflict => SagaBackendErrorKind::Conflict,
        SagaStoreErrorKind::Infrastructure => SagaBackendErrorKind::Infrastructure,
    };
    SagaBackendError::new(kind, error.reason)
}

/// 业务作用：对独立提交的 timer 写操作区分发起前失败和结果未知。
///
/// 参数说明：
/// - `error`：独立提交路径提供的稳定错误类别和原因。
///
/// 返回：连接获取失败映射 Retryable，数据库操作阶段失败映射 OutcomeUnknown，其余保持冲突或基础设施类别；不自动重放未知结果的写入。
fn map_autocommit_error(error: SagaStoreError) -> SagaBackendError {
    let kind = match error.kind() {
        SagaStoreErrorKind::ConnectionUnavailable => SagaBackendErrorKind::Retryable,
        SagaStoreErrorKind::DatabaseOperation => SagaBackendErrorKind::OutcomeUnknown,
        SagaStoreErrorKind::Conflict => SagaBackendErrorKind::Conflict,
        SagaStoreErrorKind::Infrastructure => SagaBackendErrorKind::Infrastructure,
    };
    SagaBackendError::new(kind, error.reason)
}

#[async_trait]
impl SagaInstanceStore for PgSagaStore {
    /// 业务作用：在当前同源事务内幂等创建实例及初始 transition。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `spec`：是创建所需的稳定身份、摘要、期限和触发事实。
    ///
    /// 返回：区分首次创建与既有实例；事务或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn create_instance(
        &self,
        spec: &NewSagaInstance<'_>,
    ) -> Result<SagaCreation, SagaBackendError> {
        PgSagaStore::create_instance(self, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按实例身份读取 CAS 裁决所需的已提交快照。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份。
    ///
    /// 返回：存在时返回快照，不存在返回 `None`，读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_instance(
        &self,
        saga_id: &SagaId,
    ) -> Result<Option<SagaInstanceRow>, SagaBackendError> {
        PgSagaStore::load_instance(self, saga_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：从首行开始有界扫描非终态实例。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `limit`：是单页上限。
    ///
    /// 返回：按稳定实例身份排序的快照集合；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn list_non_terminal(
        &self,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaBackendError> {
        PgSagaStore::list_non_terminal(self, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：执行租户受限、无 payload 的实例检索。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `query`：是租户、状态、时间窗和 keyset 页条件。
    ///
    /// 返回：满足条件的实例摘要；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn list_instances(
        &self,
        query: &SagaInstanceQuery<'_>,
    ) -> Result<Vec<SagaInstanceSummary>, SagaBackendError> {
        PgSagaStore::list_instances(self, query)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：从给定 keyset 游标继续有界扫描非终态实例。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `after`：是上一页末尾实例
    /// - `limit`：是单页上限。
    ///
    /// 返回：严格位于游标之后的已提交快照；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn list_non_terminal_after(
        &self,
        after: Option<&SagaId>,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaBackendError> {
        PgSagaStore::list_non_terminal_after(self, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按租户、workflow 与业务幂等键查找既有实例。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：用于隔离业务事实的受信租户身份。
    /// - `workflow`：业务意图所属的规范工作流名。
    /// - `business_key`：与租户和工作流共同确定创建幂等意图的业务键。
    ///
    /// 返回：命中时返回实例，不存在返回 `None`，读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn find_instance(
        &self,
        tenant: &TenantId,
        workflow: &WorkflowName,
        business_key: &BusinessKey,
    ) -> Result<Option<SagaInstanceRow>, SagaBackendError> {
        PgSagaStore::find_instance(self, tenant, workflow, business_key)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在当前事务内更新实例的 canonical trace 上下文。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份
    /// - `traceparent`：是已校验的 W3C 上下文。
    ///
    /// 返回：上下文写入成功返回 `Ok`；实例或事务异常返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn update_trace_context(
        &self,
        saga_id: &SagaId,
        traceparent: &str,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::update_trace_context(self, saga_id, traceparent)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按实例版本和来源状态执行受保护状态迁移并记录 transition。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `expected_version`：状态迁移必须仍然匹配的实例业务版本。
    /// - `from_status`：迁移要求当前实例仍处于的来源状态。
    /// - `spec`：本次目标迁移、触发身份及需要原子记录的状态事实。
    ///
    /// 返回：区分应用、竞争失败与重复触发；基础设施失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn advance(
        &self,
        saga_id: &SagaId,
        expected_version: u64,
        from_status: SagaStatus,
        spec: &TransitionSpec<'_>,
    ) -> Result<CasOutcome, SagaBackendError> {
        PgSagaStore::advance(self, saga_id, expected_version, from_status, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：以业务版本和控制 generation 共同门禁暂停或恢复。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `spec`：包含预期权威、目标控制态和审计事实。
    ///
    /// 返回：区分应用、幂等重放与竞争失败；基础设施失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn set_control_state(
        &self,
        spec: &ControlTransitionSpec<'_>,
    ) -> Result<ControlCasOutcome, SagaBackendError> {
        PgSagaStore::set_control_state(self, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在业务副作用同一事务内登记人工管理操作的幂等审计。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `operation_id`：需与同事务人工管理审计匹配的稳定操作身份。
    /// - `action`：要登记的人工管理动作名称。
    /// - `actor`：管理动作的已认证执行主体。
    /// - `reason`：记录在管理审计中的操作原因。
    ///
    /// 返回：区分首次登记与一致重放；冲突或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn record_management_operation(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
        action: &str,
        actor: &str,
        reason: &str,
    ) -> Result<ManagementAuditOutcome, SagaBackendError> {
        PgSagaStore::record_management_operation(self, saga_id, operation_id, action, actor, reason)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaJournalStore for PgSagaStore {
    /// 业务作用：在实例创建事务内登记 definition 的完整步骤骨架。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份
    /// - `definition`：是已冻结 workflow 定义。
    ///
    /// 返回：全部步骤幂等登记成功返回 `Ok`，否则返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn register_steps(
        &self,
        saga_id: &SagaId,
        definition: &WorkflowDefinition,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::register_steps(self, saga_id, definition)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按 definition 顺序读取步骤投影。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份。
    ///
    /// 返回：完整步骤集合；读取或解码失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_steps(&self, saga_id: &SagaId) -> Result<Vec<SagaStepRow>, SagaBackendError> {
        PgSagaStore::load_steps(self, saga_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：确认实例是否已经产生不可撤销的补偿成功事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份。
    ///
    /// 返回：存在已成功补偿步骤时为 `true`；读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn any_compensation_succeeded(&self, saga_id: &SagaId) -> Result<bool, SagaBackendError> {
        PgSagaStore::any_compensation_succeeded(self, saga_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复验人工关闭所需的同实例、同 operation 审计事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份
    /// - `operation_id`：是管理操作身份。
    ///
    /// 返回：审计存在时为 `true`；读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn manual_close_audited(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
    ) -> Result<bool, SagaBackendError> {
        PgSagaStore::manual_close_audited(self, saga_id, operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在命令 Outbox 同一事务内登记 attempt 与稳定效果身份。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `phase`：本次步骤尝试所属的执行阶段。
    /// - `attempt`：在指定步骤阶段内定位事实的尝试号。
    /// - `effect`：同一业务效果跨尝试保持稳定的效果身份。
    /// - `command`：与本次 attempt 对应的投递命令身份。
    ///
    /// 返回：区分首次登记与一致重放；身份冲突或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn record_attempt_started(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        effect: &EffectId,
        command: &CommandId,
    ) -> Result<AttemptStart, SagaBackendError> {
        PgSagaStore::record_attempt_started(self, saga_id, step, phase, attempt, effect, command)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：把参与方结果记入既有 attempt 终态，保留先到事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `phase`：本次步骤尝试所属的执行阶段。
    /// - `attempt`：在指定步骤阶段内定位事实的尝试号。
    /// - `status`：本次参与方结果要写入的 attempt 终态。
    /// - `outcome_event_id`：可选的参与方结果事件身份，用于幂等及冲突裁决。
    ///
    /// 返回：区分首次记录、一致重放和互斥结果；持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn record_attempt_outcome(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        status: StepAttemptStatus,
        outcome_event_id: Option<&str>,
    ) -> Result<AttemptOutcomeRecord, SagaBackendError> {
        PgSagaStore::record_attempt_outcome(
            self,
            saga_id,
            step,
            phase,
            attempt,
            status,
            outcome_event_id,
        )
        .await
        .map_err(map_backend_error)
    }

    /// 业务作用：读取一个步骤的完整 attempt 事实序列。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    ///
    /// 返回：按阶段与尝试号排序的事实集合；读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
    ) -> Result<Vec<SagaStepAttemptRow>, SagaBackendError> {
        PgSagaStore::load_attempts(self, saga_id, step)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按效果方向统计已登记的 resolution 尝试预算。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `direction`：需要统计结果查询尝试预算的效果方向。
    ///
    /// 返回：已登记次数；读取或数值转换失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn count_resolution_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        direction: Direction,
    ) -> Result<u32, SagaBackendError> {
        PgSagaStore::count_resolution_attempts(self, saga_id, step, direction)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在结果事实同一事务内单调更新步骤 journal 投影。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `patch`：仅包含本次允许单调更新字段的步骤投影补丁。
    ///
    /// 返回：投影更新成功返回 `Ok`；行缺失或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn update_step_journal(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        patch: &StepJournalPatch<'_>,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::update_step_journal(self, saga_id, step, patch)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：凭同事务管理审计重新开放冻结的补偿步骤。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `operation_id`：需与同事务人工管理审计匹配的稳定操作身份。
    ///
    /// 返回：唯一合法步骤重开时成功；门禁不成立或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn reopen_halted_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::reopen_halted_compensation(self, saga_id, step, operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：凭同事务管理审计重新开放冻结的结果查询。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `operation_id`：需与同事务人工管理审计匹配的稳定操作身份。
    ///
    /// 返回：唯一合法查询重开时成功；门禁不成立或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn reopen_halted_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::reopen_halted_resolution(self, saga_id, step, operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：把冻结补偿计划原子投影到计划成员及稳定逆序位置。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是实例身份
    /// - `plan`：是已由状态机冻结的计划。
    ///
    /// 返回：全部成员投影完成时成功；计划或持久化异常返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn mark_compensation_plan(
        &self,
        saga_id: &SagaId,
        plan: &CompensationPlan,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::mark_compensation_plan(self, saga_id, plan)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaTimerStore for PgSagaStore {
    /// 业务作用：在状态迁移同一事务内幂等调度 timer。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `spec`：是 timer 身份、作用域、期限、attempt 和实例版本。
    ///
    /// 返回：区分首次调度与一致重放；冲突或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn schedule_timer(
        &self,
        spec: &TimerSpec<'_>,
    ) -> Result<TimerSchedule, SagaBackendError> {
        PgSagaStore::schedule_timer(self, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在状态离开时取消同一作用域下尚未触发的 timer。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `scope`：限定 timer 属于整个实例或某个步骤的作用域。
    /// - `kind`：可选 timer 种类过滤，None 覆盖该作用域全部种类。
    ///
    /// 返回：实际取消行数；持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn cancel_scope_timers(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: Option<&str>,
    ) -> Result<u64, SagaBackendError> {
        PgSagaStore::cancel_scope_timers(self, saga_id, scope, kind)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：重排仍可复活的 timer，并递增 generation 撤销旧领取权威。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `scope`：限定 timer 属于整个实例或某个步骤的作用域。
    /// - `kind`：与作用域和尝试号共同定位待重排 timer 的种类。
    /// - `attempt`：与 timer 作用域及种类共同定位调度意图的尝试号。
    /// - `due_at_ms`：重排后 timer 的 epoch 毫秒到期时刻。
    /// - `expected_saga_version`：timer 被消费时需要匹配的实例业务版本。
    ///
    /// 返回：区分已重排与不存在；持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn reschedule_timer(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: &str,
        attempt: AttemptNo,
        due_at_ms: i64,
        expected_saga_version: u64,
    ) -> Result<TimerReschedule, SagaBackendError> {
        PgSagaStore::reschedule_timer(
            self,
            saga_id,
            scope,
            kind,
            attempt,
            due_at_ms,
            expected_saga_version,
        )
        .await
        .map_err(map_backend_error)
    }

    /// 业务作用：以数据库可见租约和不可复制 token 领取有界到期 timer 批次。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `owner`：本轮 timer 领取者的持久标识。
    /// - `fencing_token`：标识本轮领取、后续复验租约权威所需的 token。
    /// - `now_ms`：本次到期或租约裁决采用的 epoch 毫秒时刻。
    /// - `lease_ms`：本轮领取权威的有效毫秒时长。
    /// - `limit`：本轮领取条数上限。
    ///
    /// 返回：领取事务明确提交后返回与 token 绑定的批次；不确定或失败返回封闭错误。独立提交的数据库操作失败按 OutcomeUnknown 处理，禁止据此盲目重放。
    async fn claim_due_timers(
        &self,
        owner: &str,
        fencing_token: TimerFencingToken,
        now_ms: i64,
        lease_ms: i64,
        limit: u32,
    ) -> Result<TimerClaimBatch, SagaBackendError> {
        PgSagaStore::claim_due_timers(self, owner, fencing_token, now_ms, lease_ms, limit)
            .await
            .map_err(map_autocommit_error)
    }

    /// 业务作用：凭当前 claim token 把 timer 标记为已触发。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `timer_id`：需要完成或交还的持久 timer 身份。
    /// - `fencing_token`：标识本轮领取、后续复验租约权威所需的 token。
    /// - `now_ms`：本次到期或租约裁决采用的 epoch 毫秒时刻。
    ///
    /// 返回：区分应用与失权；持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn complete_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
    ) -> Result<TimerFencing, SagaBackendError> {
        PgSagaStore::complete_timer(self, timer_id, fencing_token, now_ms)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：凭当前 claim token 交还未处理 timer 并设置下一可用时刻。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `timer_id`：需要完成或交还的持久 timer 身份。
    /// - `fencing_token`：标识本轮领取、后续复验租约权威所需的 token。
    /// - `now_ms`：本次到期或租约裁决采用的 epoch 毫秒时刻。
    /// - `available_at_ms`：交还后允许重新领取的 epoch 毫秒时刻。
    ///
    /// 返回：区分应用与失权；持久化失败返回封闭错误。独立提交的数据库操作失败按 OutcomeUnknown 处理，禁止据此盲目重放。
    async fn release_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
        available_at_ms: i64,
    ) -> Result<TimerFencing, SagaBackendError> {
        PgSagaStore::release_timer(self, timer_id, fencing_token, now_ms, available_at_ms)
            .await
            .map_err(map_autocommit_error)
    }

    /// 业务作用：把实例尚未完成的 timer 提前置为可领取，驱动恢复收敛。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：是待唤醒实例。
    ///
    /// 返回：实际唤醒行数；持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn wake_saga_timers(&self, saga_id: &SagaId) -> Result<u64, SagaBackendError> {
        PgSagaStore::wake_saga_timers(self, saga_id)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaParticipantStore for PgSagaStore {
    /// 业务作用：在 Inbox 事务内裁决 execute 是否首次允许产生外部效果。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `gate`：绑定组合身份
    /// - `execute_effect`：是跨尝试稳定效果身份。
    ///
    /// 返回：执行许可、等待或既有结果；合同或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn admit_execute(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
    ) -> Result<ExecuteAdmission, SagaBackendError> {
        PgSagaStore::admit_execute(self, gate, execute_effect)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在结果 Outbox 同一事务内结算 execute gate。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `status`：已裁决的正向执行结果。
    ///
    /// 返回：唯一合法结算成功时返回 `Ok`；冲突或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn settle_execute(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepForwardStatus,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::settle_execute(self, saga_id, step, status)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：以行锁建立 cancel 屏障并裁决本地取消结果。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `gate`：绑定实例、步骤、租户和冻结工作流定义的参与方门禁键。
    /// - `execute_effect`：需要保持跨尝试一致的正向业务效果身份。
    /// - `cancel_effect`：需要保持幂等的取消效果身份。
    ///
    /// 返回：可确定取消结果或等待；身份冲突及持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn adjudicate_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<CancelAdjudication, SagaBackendError> {
        PgSagaStore::adjudicate_cancel(self, gate, execute_effect, cancel_effect)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：建立外部 cancel 独占门禁，阻止并发 execute 提交互斥效果。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `gate`：绑定实例、步骤、租户和冻结工作流定义的参与方门禁键。
    /// - `execute_effect`：需要保持跨尝试一致的正向业务效果身份。
    /// - `cancel_effect`：需要保持幂等的取消效果身份。
    ///
    /// 返回：执行外部取消或重放既有裁决；冲突及持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn admit_external_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<ExternalCancelAdmission, SagaBackendError> {
        PgSagaStore::admit_external_cancel(self, gate, execute_effect, cancel_effect)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在结果 Outbox 同一事务内结算外部 cancel 与正向最终事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `forward`：本次外部取消同时裁决的正向终态。
    /// - `cancel`：本次外部取消的取消终态，须与正向事实组成合法组合。
    ///
    /// 返回：组合事实唯一结算成功时返回 `Ok`；否则返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn settle_external_cancel(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        forward: StepForwardStatus,
        cancel: StepCancelStatus,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::settle_external_cancel(self, saga_id, step, forward, cancel)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：裁决补偿是否首次允许执行，并门禁人工恢复身份。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `gate`：绑定实例、步骤、租户和冻结工作流定义的参与方门禁键。
    /// - `compensate_effect`：需要保持幂等的补偿效果身份。
    /// - `recovery_operation_id`：人工重新开放冻结动作时必须匹配的可选管理操作身份。
    ///
    /// 返回：执行许可、等待或既有结果；冲突及持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn admit_compensation(
        &self,
        gate: &ParticipantGateKey<'_>,
        compensate_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<CompensationAdmission, SagaBackendError> {
        PgSagaStore::admit_compensation(self, gate, compensate_effect, recovery_operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在结果 Outbox 同一事务内结算补偿事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `status`：已裁决的补偿执行结果。
    ///
    /// 返回：合法结算成功时返回 `Ok`；冲突或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn settle_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepCompensationStatus,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::settle_compensation(self, saga_id, step, status)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：裁决 Unknown 结果查询是否首次允许执行，并门禁人工恢复身份。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `gate`：绑定实例、步骤、租户和冻结工作流定义的参与方门禁键。
    /// - `resolve_effect`：本次 Unknown 结果查询的稳定效果身份。
    /// - `recovery_operation_id`：人工重新开放冻结动作时必须匹配的可选管理操作身份。
    ///
    /// 返回：查询许可、等待或既有结果；冲突及持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn admit_resolution(
        &self,
        gate: &ParticipantGateKey<'_>,
        resolve_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<ResolutionAdmission, SagaBackendError> {
        PgSagaStore::admit_resolution(self, gate, resolve_effect, recovery_operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在结果 Outbox 同一事务内结算正向或补偿 Unknown 查询事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `step`：该实例内已冻结的规范步骤名。
    /// - `target`：本次结果查询要结算的正向或补偿目标。
    /// - `status`：已裁决的 Unknown 结果查询状态。
    ///
    /// 返回：合法结算成功时返回 `Ok`；冲突或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn settle_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        target: ResolutionTarget,
        status: StepResolutionStatus,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::settle_resolution(self, saga_id, step, target, status)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaAuditStore for PgSagaStore {
    /// 业务作用：在互斥 attempt 结果裁决同一事务内保存双方证据。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `fact`：包含定位、既有事实、incoming 事实和冲突类别。
    ///
    /// 返回：幂等保存成功返回 `Ok`；合同或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn record_attempt_conflict(
        &self,
        fact: &AttemptConflictFact<'_>,
    ) -> Result<(), SagaBackendError> {
        PgSagaStore::record_attempt_conflict(self, fact)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按全局单调序号读取跨类别、不可变的实例审计事件。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：定位实例
    /// - `after`：是最后已交付序号
    /// - `limit`：限制返回量。
    ///
    /// 返回：严格按 `audit_seq` 递增的事件；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_audit_events(
        &self,
        saga_id: &SagaId,
        after: SagaAuditEventCursor,
        limit: u32,
    ) -> Result<Vec<SagaAuditEventRow>, SagaBackendError> {
        PgSagaStore::load_audit_events(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：有界读取实例的 attempt 审计事实。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：定位实例
    /// - `after`：是上一页末项
    /// - `limit`：限制返回量。
    ///
    /// 返回：稳定排序的事实集合；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_attempt_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaAttemptAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaAttemptAuditRow>, SagaBackendError> {
        PgSagaStore::load_attempt_audit(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按序号游标读取实例状态迁移审计。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `after_seq`：上一页最后交付的审计序号，仅读取其后的事实。
    /// - `limit`：本页最多返回的持久事实数量。
    ///
    /// 返回：稳定排序的 transition 集合；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_transition_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaTransitionAuditRow>, SagaBackendError> {
        PgSagaStore::load_transition_audit(self, saga_id, after_seq, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按序号游标读取实例控制态迁移审计。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：需要读取或变更的持久 Saga 实例身份。
    /// - `after_seq`：上一页最后交付的审计序号，仅读取其后的事实。
    /// - `limit`：本页最多返回的持久事实数量。
    ///
    /// 返回：稳定排序的控制审计集合；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_control_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaControlAuditRow>, SagaBackendError> {
        PgSagaStore::load_control_audit(self, saga_id, after_seq, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：有界读取实例的人工管理审计。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：定位实例
    /// - `after`：是上一页末项
    /// - `limit`：限制返回量。
    ///
    /// 返回：稳定排序的管理审计集合；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_management_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaManagementAuditRow>, SagaBackendError> {
        PgSagaStore::load_management_audit(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：有界读取实例的互斥事实证据。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `saga_id`：定位实例
    /// - `after`：是上一页末项
    /// - `limit`：限制返回量。
    ///
    /// 返回：稳定排序的冲突事实集合；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_conflict_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaConflictFactRow>, SagaBackendError> {
        PgSagaStore::load_conflict_audit(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaGovernanceStore for PgSagaStore {
    /// 业务作用：在实例创建事务内原子预留租户在飞名额。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：是受信租户
    /// - `cap`：是可选上限。
    ///
    /// 返回：区分已预留、超额和账本未初始化；持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn reserve_tenant_quota(
        &self,
        tenant: &TenantId,
        cap: Option<u64>,
    ) -> Result<QuotaReservation, SagaBackendError> {
        PgSagaStore::reserve_tenant_quota(self, tenant, cap)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：确认租户账本是否已按持久实例事实完成初始化。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：是受信租户。
    ///
    /// 返回：已初始化时为 `true`；读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn tenant_quota_initialized(&self, tenant: &TenantId) -> Result<bool, SagaBackendError> {
        PgSagaStore::tenant_quota_initialized(self, tenant)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：读取租户在飞实例账本值。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：是受信租户。
    ///
    /// 返回：不存在账本时为零；读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn tenant_quota_usage(&self, tenant: &TenantId) -> Result<u64, SagaBackendError> {
        PgSagaStore::tenant_quota_usage(self, tenant)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在同源事务内锁定账本并按非终态实例事实重算配额。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：是受信租户。
    ///
    /// 返回：对账后的在飞数；事务或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn reconcile_tenant_quota(&self, tenant: &TenantId) -> Result<u64, SagaBackendError> {
        PgSagaStore::reconcile_tenant_quota(self, tenant)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：在管理动作事务内按数据库时钟预留租户频率预算。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：用于隔离业务事实的受信租户身份。
    /// - `max_actions`：一个租户时间窗口内允许预留的最大管理动作数。
    /// - `window_ms`：按数据库时钟划分动作频率窗口的毫秒长度。
    ///
    /// 返回：区分已预留和已耗尽；参数或持久化失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn reserve_tenant_action_rate(
        &self,
        tenant: &TenantId,
        max_actions: u64,
        window_ms: i64,
    ) -> Result<ActionRateReservation, SagaBackendError> {
        PgSagaStore::reserve_tenant_action_rate(self, tenant, max_actions, window_ms)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：按数据库时钟读取当前租户动作窗口及已用次数。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `tenant`：是受信租户
    /// - `window_ms`：是窗口长度。
    ///
    /// 返回：窗口起点和已用次数；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn tenant_action_rate_usage(
        &self,
        tenant: &TenantId,
        window_ms: i64,
    ) -> Result<(u64, u64), SagaBackendError> {
        PgSagaStore::tenant_action_rate_usage(self, tenant, window_ms)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：从已提交 Saga 表读取低基数运行指标快照。本适配层委托同名具体存储方法并统一错误。
    ///
    /// 参数说明：
    /// - `now_ms`：是统一 epoch 毫秒观测时刻。
    ///
    /// 返回：数据库事实快照；参数或读取失败返回封闭错误。具体连接及数据库操作失败由本层映射为 Retryable，重试须服从外层事务裁决。
    async fn load_operational_metrics(
        &self,
        now_ms: i64,
    ) -> Result<SagaStoreMetrics, SagaBackendError> {
        PgSagaStore::load_operational_metrics(self, now_ms)
            .await
            .map_err(map_backend_error)
    }
}
