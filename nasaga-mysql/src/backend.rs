//! MySQL Saga store 对后端中立分组能力的兼容实现。

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
use crate::{MySqlSagaStore, SagaStoreError};

/// 业务作用：把既有 MySQL store 的脱敏错误收敛到运行核心可穷举的保守分类。
///
/// 参数说明：`error` 是不含 SQL、连接串和业务内容的既有错误。
///
/// 返回：数据库语句和连接失败仅在 ambient transaction 内标记为可重试；身份冲突与
/// 缺失事实分别归类，其余合同或配置异常归入基础设施失败。
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

/// 业务作用：分类不受外层事务保护的 timer 租约写，禁止在结果不确定后直接重执。
///
/// 参数说明：`error` 是独立提交路径返回的脱敏错误。
///
/// 返回：连接获取失败可安全重试；语句执行或提交阶段统一按结果不确定停止盲目重执。
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
impl SagaInstanceStore for MySqlSagaStore {
    /// 业务作用：复用既有 MySQL 创建事务语义实现后端中立实例创建。
    async fn create_instance(
        &self,
        spec: &NewSagaInstance<'_>,
    ) -> Result<SagaCreation, SagaBackendError> {
        MySqlSagaStore::create_instance(self, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 查询读取实例快照。
    async fn load_instance(
        &self,
        saga_id: &SagaId,
    ) -> Result<Option<SagaInstanceRow>, SagaBackendError> {
        MySqlSagaStore::load_instance(self, saga_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 有界非终态扫描。
    async fn list_non_terminal(
        &self,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaBackendError> {
        MySqlSagaStore::list_non_terminal(self, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 租户受限实例检索。
    async fn list_instances(
        &self,
        query: &SagaInstanceQuery<'_>,
    ) -> Result<Vec<SagaInstanceSummary>, SagaBackendError> {
        MySqlSagaStore::list_instances(self, query)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL keyset 非终态扫描。
    async fn list_non_terminal_after(
        &self,
        after: Option<&SagaId>,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaBackendError> {
        MySqlSagaStore::list_non_terminal_after(self, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 业务幂等键查询。
    async fn find_instance(
        &self,
        tenant: &TenantId,
        workflow: &WorkflowName,
        business_key: &BusinessKey,
    ) -> Result<Option<SagaInstanceRow>, SagaBackendError> {
        MySqlSagaStore::find_instance(self, tenant, workflow, business_key)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 同事务 trace 更新。
    async fn update_trace_context(
        &self,
        saga_id: &SagaId,
        traceparent: &str,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::update_trace_context(self, saga_id, traceparent)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL CAS 迁移与 transition 原子写入。
    async fn advance(
        &self,
        saga_id: &SagaId,
        expected_version: u64,
        from_status: SagaStatus,
        spec: &TransitionSpec<'_>,
    ) -> Result<CasOutcome, SagaBackendError> {
        MySqlSagaStore::advance(self, saga_id, expected_version, from_status, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 控制态双版本门禁。
    async fn set_control_state(
        &self,
        spec: &ControlTransitionSpec<'_>,
    ) -> Result<ControlCasOutcome, SagaBackendError> {
        MySqlSagaStore::set_control_state(self, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 管理操作幂等审计。
    async fn record_management_operation(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
        action: &str,
        actor: &str,
        reason: &str,
    ) -> Result<ManagementAuditOutcome, SagaBackendError> {
        MySqlSagaStore::record_management_operation(
            self,
            saga_id,
            operation_id,
            action,
            actor,
            reason,
        )
        .await
        .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaJournalStore for MySqlSagaStore {
    /// 业务作用：复用既有 MySQL 步骤骨架登记。
    async fn register_steps(
        &self,
        saga_id: &SagaId,
        definition: &WorkflowDefinition,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::register_steps(self, saga_id, definition)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 步骤投影查询。
    async fn load_steps(&self, saga_id: &SagaId) -> Result<Vec<SagaStepRow>, SagaBackendError> {
        MySqlSagaStore::load_steps(self, saga_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 补偿成功事实复验。
    async fn any_compensation_succeeded(&self, saga_id: &SagaId) -> Result<bool, SagaBackendError> {
        MySqlSagaStore::any_compensation_succeeded(self, saga_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 人工关闭审计复验。
    async fn manual_close_audited(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
    ) -> Result<bool, SagaBackendError> {
        MySqlSagaStore::manual_close_audited(self, saga_id, operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL attempt 与命令身份登记。
    async fn record_attempt_started(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        effect: &EffectId,
        command: &CommandId,
    ) -> Result<AttemptStart, SagaBackendError> {
        MySqlSagaStore::record_attempt_started(self, saga_id, step, phase, attempt, effect, command)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL attempt 终态记账。
    async fn record_attempt_outcome(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        status: StepAttemptStatus,
        outcome_event_id: Option<&str>,
    ) -> Result<AttemptOutcomeRecord, SagaBackendError> {
        MySqlSagaStore::record_attempt_outcome(
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

    /// 业务作用：复用既有 MySQL attempt 事实查询。
    async fn load_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
    ) -> Result<Vec<SagaStepAttemptRow>, SagaBackendError> {
        MySqlSagaStore::load_attempts(self, saga_id, step)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL resolution 预算统计。
    async fn count_resolution_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        direction: Direction,
    ) -> Result<u32, SagaBackendError> {
        MySqlSagaStore::count_resolution_attempts(self, saga_id, step, direction)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL journal 单调投影更新。
    async fn update_step_journal(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        patch: &StepJournalPatch<'_>,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::update_step_journal(self, saga_id, step, patch)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 冻结补偿重开门禁。
    async fn reopen_halted_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::reopen_halted_compensation(self, saga_id, step, operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 冻结查询重开门禁。
    async fn reopen_halted_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::reopen_halted_resolution(self, saga_id, step, operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 冻结补偿计划投影。
    async fn mark_compensation_plan(
        &self,
        saga_id: &SagaId,
        plan: &CompensationPlan,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::mark_compensation_plan(self, saga_id, plan)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaTimerStore for MySqlSagaStore {
    /// 业务作用：复用既有 MySQL 同事务 timer 调度。
    async fn schedule_timer(
        &self,
        spec: &TimerSpec<'_>,
    ) -> Result<TimerSchedule, SagaBackendError> {
        MySqlSagaStore::schedule_timer(self, spec)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 作用域 timer 作废。
    async fn cancel_scope_timers(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: Option<&str>,
    ) -> Result<u64, SagaBackendError> {
        MySqlSagaStore::cancel_scope_timers(self, saga_id, scope, kind)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL timer 重排与 generation 递增。
    async fn reschedule_timer(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: &str,
        attempt: AttemptNo,
        due_at_ms: i64,
        expected_saga_version: u64,
    ) -> Result<TimerReschedule, SagaBackendError> {
        MySqlSagaStore::reschedule_timer(
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

    /// 业务作用：复用既有 MySQL 独立提交的 timer 批量领取，并保守处理结果不确定。
    async fn claim_due_timers(
        &self,
        owner: &str,
        fencing_token: TimerFencingToken,
        now_ms: i64,
        lease_ms: i64,
        limit: u32,
    ) -> Result<TimerClaimBatch, SagaBackendError> {
        MySqlSagaStore::claim_due_timers(self, owner, fencing_token, now_ms, lease_ms, limit)
            .await
            .map_err(map_autocommit_error)
    }

    /// 业务作用：复用既有 MySQL 同事务 timer 完成 fencing。
    async fn complete_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
    ) -> Result<TimerFencing, SagaBackendError> {
        MySqlSagaStore::complete_timer(self, timer_id, fencing_token, now_ms)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 独立 timer 交还，并保守处理结果不确定。
    async fn release_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
        available_at_ms: i64,
    ) -> Result<TimerFencing, SagaBackendError> {
        MySqlSagaStore::release_timer(self, timer_id, fencing_token, now_ms, available_at_ms)
            .await
            .map_err(map_autocommit_error)
    }

    /// 业务作用：复用既有 MySQL 恢复时 timer 唤醒。
    async fn wake_saga_timers(&self, saga_id: &SagaId) -> Result<u64, SagaBackendError> {
        MySqlSagaStore::wake_saga_timers(self, saga_id)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaParticipantStore for MySqlSagaStore {
    /// 业务作用：复用既有 MySQL execute gate 准入。
    async fn admit_execute(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
    ) -> Result<ExecuteAdmission, SagaBackendError> {
        MySqlSagaStore::admit_execute(self, gate, execute_effect)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL execute gate 结算。
    async fn settle_execute(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepForwardStatus,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::settle_execute(self, saga_id, step, status)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 本地取消屏障裁决。
    async fn adjudicate_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<CancelAdjudication, SagaBackendError> {
        MySqlSagaStore::adjudicate_cancel(self, gate, execute_effect, cancel_effect)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 外部取消独占门禁。
    async fn admit_external_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<ExternalCancelAdmission, SagaBackendError> {
        MySqlSagaStore::admit_external_cancel(self, gate, execute_effect, cancel_effect)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 外部取消组合结算。
    async fn settle_external_cancel(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        forward: StepForwardStatus,
        cancel: StepCancelStatus,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::settle_external_cancel(self, saga_id, step, forward, cancel)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 补偿准入与人工恢复门禁。
    async fn admit_compensation(
        &self,
        gate: &ParticipantGateKey<'_>,
        compensate_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<CompensationAdmission, SagaBackendError> {
        MySqlSagaStore::admit_compensation(self, gate, compensate_effect, recovery_operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 补偿结算。
    async fn settle_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepCompensationStatus,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::settle_compensation(self, saga_id, step, status)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL Unknown 查询准入与人工恢复门禁。
    async fn admit_resolution(
        &self,
        gate: &ParticipantGateKey<'_>,
        resolve_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<ResolutionAdmission, SagaBackendError> {
        MySqlSagaStore::admit_resolution(self, gate, resolve_effect, recovery_operation_id)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL Unknown 查询结算。
    async fn settle_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        target: ResolutionTarget,
        status: StepResolutionStatus,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::settle_resolution(self, saga_id, step, target, status)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaAuditStore for MySqlSagaStore {
    /// 业务作用：复用既有 MySQL 互斥 attempt 事实写入。
    async fn record_attempt_conflict(
        &self,
        fact: &AttemptConflictFact<'_>,
    ) -> Result<(), SagaBackendError> {
        MySqlSagaStore::record_attempt_conflict(self, fact)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用 MySQL 全局序号审计事件查询。
    async fn load_audit_events(
        &self,
        saga_id: &SagaId,
        after: SagaAuditEventCursor,
        limit: u32,
    ) -> Result<Vec<SagaAuditEventRow>, SagaBackendError> {
        MySqlSagaStore::load_audit_events(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL attempt 审计查询。
    async fn load_attempt_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaAttemptAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaAttemptAuditRow>, SagaBackendError> {
        MySqlSagaStore::load_attempt_audit(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL transition 审计查询。
    async fn load_transition_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaTransitionAuditRow>, SagaBackendError> {
        MySqlSagaStore::load_transition_audit(self, saga_id, after_seq, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 控制态审计查询。
    async fn load_control_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaControlAuditRow>, SagaBackendError> {
        MySqlSagaStore::load_control_audit(self, saga_id, after_seq, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 管理操作审计查询。
    async fn load_management_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaManagementAuditRow>, SagaBackendError> {
        MySqlSagaStore::load_management_audit(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 冲突事实审计查询。
    async fn load_conflict_audit(
        &self,
        saga_id: &SagaId,
        after: Option<&SagaTimedAuditCursor>,
        limit: u32,
    ) -> Result<Vec<SagaConflictFactRow>, SagaBackendError> {
        MySqlSagaStore::load_conflict_audit(self, saga_id, after, limit)
            .await
            .map_err(map_backend_error)
    }
}

#[async_trait]
impl SagaGovernanceStore for MySqlSagaStore {
    /// 业务作用：复用既有 MySQL 租户在飞实例配额预留。
    async fn reserve_tenant_quota(
        &self,
        tenant: &TenantId,
        cap: Option<u64>,
    ) -> Result<QuotaReservation, SagaBackendError> {
        MySqlSagaStore::reserve_tenant_quota(self, tenant, cap)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 租户配额初始化复验。
    async fn tenant_quota_initialized(&self, tenant: &TenantId) -> Result<bool, SagaBackendError> {
        MySqlSagaStore::tenant_quota_initialized(self, tenant)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 租户配额账本读取。
    async fn tenant_quota_usage(&self, tenant: &TenantId) -> Result<u64, SagaBackendError> {
        MySqlSagaStore::tenant_quota_usage(self, tenant)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 租户配额事实对账。
    async fn reconcile_tenant_quota(&self, tenant: &TenantId) -> Result<u64, SagaBackendError> {
        MySqlSagaStore::reconcile_tenant_quota(self, tenant)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 管理动作频率预算预留。
    async fn reserve_tenant_action_rate(
        &self,
        tenant: &TenantId,
        max_actions: u64,
        window_ms: i64,
    ) -> Result<ActionRateReservation, SagaBackendError> {
        MySqlSagaStore::reserve_tenant_action_rate(self, tenant, max_actions, window_ms)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 管理动作当前窗口读取。
    async fn tenant_action_rate_usage(
        &self,
        tenant: &TenantId,
        window_ms: i64,
    ) -> Result<(u64, u64), SagaBackendError> {
        MySqlSagaStore::tenant_action_rate_usage(self, tenant, window_ms)
            .await
            .map_err(map_backend_error)
    }

    /// 业务作用：复用既有 MySQL 已提交 Saga 指标快照查询。
    async fn load_operational_metrics(
        &self,
        now_ms: i64,
    ) -> Result<SagaStoreMetrics, SagaBackendError> {
        MySqlSagaStore::load_operational_metrics(self, now_ms)
            .await
            .map_err(map_backend_error)
    }
}
