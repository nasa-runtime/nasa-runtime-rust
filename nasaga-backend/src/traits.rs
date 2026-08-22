//! Saga 运行核心依赖的分组持久能力与完整后端组合身份。

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use nainbox_core::InboxStore;
use naoutbox_core::DurableOutboxAppend;
use nasaga_core::{
    AttemptNo, BusinessKey, CommandId, CompensationPlan, Direction, EffectId, SagaId, SagaStatus,
    StepAttemptStatus, StepCancelStatus, StepCompensationStatus, StepForwardStatus, StepName,
    StepPhase, StepResolutionStatus, TenantId, WorkflowDefinition, WorkflowName,
};
use natx_core::{DatabaseDriver, DatasourceRef};

use crate::{
    ActionRateReservation, AttemptConflictFact, AttemptOutcomeRecord, AttemptStart,
    CancelAdjudication, CasOutcome, CompensationAdmission, ControlCasOutcome,
    ControlTransitionSpec, ExecuteAdmission, ExternalCancelAdmission, ManagementAuditOutcome,
    NewSagaInstance, ParticipantGateKey, QuotaReservation, ResolutionAdmission, ResolutionTarget,
    SagaBackendError, SagaConflictFactRow, SagaControlAuditRow, SagaCreation, SagaInstanceQuery,
    SagaInstanceRow, SagaInstanceSummary, SagaManagementAuditRow, SagaStepAttemptRow, SagaStepRow,
    SagaStoreMetrics, SagaTransitionAuditRow, StepJournalPatch, TimerClaimBatch, TimerFencing,
    TimerFencingToken, TimerReschedule, TimerSchedule, TimerScope, TimerSpec, TransitionSpec,
};

/// 事务执行器接受的有界异步业务操作。
pub type SagaTransactionFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// 业务作用：保留 Saga 业务回滚原因与数据库事务终结阶段，供 transport 做封闭裁决。
#[derive(Debug)]
pub enum SagaTransactionRunError<E> {
    /// 业务闭包主动拒绝提交，物理回滚已确认。
    Rollback(E),
    /// 嵌套操作已把外层事务标记为 rollback-only。
    RollbackOnly,
    /// 数据库明确拒绝 COMMIT。
    CommitRejected,
    /// COMMIT 发出后无法确认服务端结果。
    CommitUncertain,
    /// 物理回滚结果无法确认。
    RollbackFailed,
    /// 事务开始前或执行内核不可用。
    Infrastructure,
}

/// 业务作用：在后端绑定的 datasource 上执行 Saga 原子操作，并保留提交结果分类。
pub trait SagaTransactionRunner: Send + Sync {
    /// 业务作用：建立同源 ambient transaction，执行操作并按操作裁决提交或回滚。
    ///
    /// 参数说明：
    /// - `operation`：只能在该事务作用域内完成的 Saga、Inbox 与 Outbox 组合操作。
    ///
    /// 返回：提交确认后返回业务值；回滚、提交不确定或基础设施失败返回封闭错误。
    fn run<'a, T, E>(
        &'a self,
        operation: SagaTransactionFuture<'a, T, E>,
    ) -> Pin<Box<dyn Future<Output = Result<T, SagaTransactionRunError<E>>> + Send + 'a>>
    where
        T: Send + 'a,
        E: Send + 'a;
}

/// 业务作用：提供 Saga 实例创建、检索与 CAS 迁移能力。
#[async_trait]
pub trait SagaInstanceStore: Send + Sync {
    /// 业务作用：在当前同源事务内幂等创建实例及初始 transition。
    ///
    /// 参数说明：`spec` 是创建所需的稳定身份、摘要、期限和触发事实。
    ///
    /// 返回：区分首次创建与既有实例；事务或持久化失败返回封闭错误。
    async fn create_instance(
        &self,
        spec: &NewSagaInstance<'_>,
    ) -> Result<SagaCreation, SagaBackendError>;

    /// 业务作用：按实例身份读取 CAS 裁决所需的已提交快照。
    ///
    /// 参数说明：`saga_id` 是实例身份。
    ///
    /// 返回：存在时返回快照，不存在返回 `None`，读取失败返回封闭错误。
    async fn load_instance(
        &self,
        saga_id: &SagaId,
    ) -> Result<Option<SagaInstanceRow>, SagaBackendError>;

    /// 业务作用：从首行开始有界扫描非终态实例。
    ///
    /// 参数说明：`limit` 是单页上限。
    ///
    /// 返回：按稳定实例身份排序的快照集合；参数或读取失败返回封闭错误。
    async fn list_non_terminal(&self, limit: u32)
        -> Result<Vec<SagaInstanceRow>, SagaBackendError>;

    /// 业务作用：执行租户受限、无 payload 的实例检索。
    ///
    /// 参数说明：`query` 是租户、状态、时间窗和 keyset 页条件。
    ///
    /// 返回：满足条件的实例摘要；参数或读取失败返回封闭错误。
    async fn list_instances(
        &self,
        query: &SagaInstanceQuery<'_>,
    ) -> Result<Vec<SagaInstanceSummary>, SagaBackendError>;

    /// 业务作用：从给定 keyset 游标继续有界扫描非终态实例。
    ///
    /// 参数说明：`after` 是上一页末尾实例，`limit` 是单页上限。
    ///
    /// 返回：严格位于游标之后的已提交快照；参数或读取失败返回封闭错误。
    async fn list_non_terminal_after(
        &self,
        after: Option<&SagaId>,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaBackendError>;

    /// 业务作用：按租户、workflow 与业务幂等键查找既有实例。
    ///
    /// 参数说明：`tenant`、`workflow` 与 `business_key` 共同限定业务意图。
    ///
    /// 返回：命中时返回实例，不存在返回 `None`，读取失败返回封闭错误。
    async fn find_instance(
        &self,
        tenant: &TenantId,
        workflow: &WorkflowName,
        business_key: &BusinessKey,
    ) -> Result<Option<SagaInstanceRow>, SagaBackendError>;

    /// 业务作用：在当前事务内更新实例的 canonical trace 上下文。
    ///
    /// 参数说明：`saga_id` 是实例身份，`traceparent` 是已校验的 W3C 上下文。
    ///
    /// 返回：上下文写入成功返回 `Ok`；实例或事务异常返回封闭错误。
    async fn update_trace_context(
        &self,
        saga_id: &SagaId,
        traceparent: &str,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：按实例版本和来源状态执行受保护状态迁移并记录 transition。
    ///
    /// 参数说明：`saga_id`、`expected_version`、`from_status` 是 CAS 权威，`spec` 是迁移事实。
    ///
    /// 返回：区分应用、竞争失败与重复触发；基础设施失败返回封闭错误。
    async fn advance(
        &self,
        saga_id: &SagaId,
        expected_version: u64,
        from_status: SagaStatus,
        spec: &TransitionSpec<'_>,
    ) -> Result<CasOutcome, SagaBackendError>;

    /// 业务作用：以业务版本和控制 generation 共同门禁暂停或恢复。
    ///
    /// 参数说明：`spec` 包含预期权威、目标控制态和审计事实。
    ///
    /// 返回：区分应用、幂等重放与竞争失败；基础设施失败返回封闭错误。
    async fn set_control_state(
        &self,
        spec: &ControlTransitionSpec<'_>,
    ) -> Result<ControlCasOutcome, SagaBackendError>;

    /// 业务作用：在业务副作用同一事务内登记人工管理操作的幂等审计。
    ///
    /// 参数说明：实例、操作身份、动作、主体和原因共同构成审计事实。
    ///
    /// 返回：区分首次登记与一致重放；冲突或持久化失败返回封闭错误。
    async fn record_management_operation(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
        action: &str,
        actor: &str,
        reason: &str,
    ) -> Result<ManagementAuditOutcome, SagaBackendError>;
}

/// 业务作用：提供步骤骨架、attempt 事实和补偿计划投影能力。
#[async_trait]
pub trait SagaJournalStore: Send + Sync {
    /// 业务作用：在实例创建事务内登记 definition 的完整步骤骨架。
    ///
    /// 参数说明：`saga_id` 是实例身份，`definition` 是已冻结 workflow 定义。
    ///
    /// 返回：全部步骤幂等登记成功返回 `Ok`，否则返回封闭错误。
    async fn register_steps(
        &self,
        saga_id: &SagaId,
        definition: &WorkflowDefinition,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：按 definition 顺序读取步骤投影。
    ///
    /// 参数说明：`saga_id` 是实例身份。
    ///
    /// 返回：完整步骤集合；读取或解码失败返回封闭错误。
    async fn load_steps(&self, saga_id: &SagaId) -> Result<Vec<SagaStepRow>, SagaBackendError>;

    /// 业务作用：确认实例是否已经产生不可撤销的补偿成功事实。
    ///
    /// 参数说明：`saga_id` 是实例身份。
    ///
    /// 返回：存在已成功补偿步骤时为 `true`；读取失败返回封闭错误。
    async fn any_compensation_succeeded(&self, saga_id: &SagaId) -> Result<bool, SagaBackendError>;

    /// 业务作用：复验人工关闭所需的同实例、同 operation 审计事实。
    ///
    /// 参数说明：`saga_id` 是实例身份，`operation_id` 是管理操作身份。
    ///
    /// 返回：审计存在时为 `true`；读取失败返回封闭错误。
    async fn manual_close_audited(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
    ) -> Result<bool, SagaBackendError>;

    /// 业务作用：在命令 Outbox 同一事务内登记 attempt 与稳定效果身份。
    ///
    /// 参数说明：实例、步骤、阶段、尝试号、效果和命令身份共同确定 attempt。
    ///
    /// 返回：区分首次登记与一致重放；身份冲突或持久化失败返回封闭错误。
    async fn record_attempt_started(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        effect: &EffectId,
        command: &CommandId,
    ) -> Result<AttemptStart, SagaBackendError>;

    /// 业务作用：把参与方结果记入既有 attempt 终态，保留先到事实。
    ///
    /// 参数说明：实例、步骤、阶段和尝试号定位事实，`status` 与事件身份描述结果。
    ///
    /// 返回：区分首次记录、一致重放和互斥结果；持久化失败返回封闭错误。
    async fn record_attempt_outcome(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        phase: StepPhase,
        attempt: AttemptNo,
        status: StepAttemptStatus,
        outcome_event_id: Option<&str>,
    ) -> Result<AttemptOutcomeRecord, SagaBackendError>;

    /// 业务作用：读取一个步骤的完整 attempt 事实序列。
    ///
    /// 参数说明：`saga_id` 与 `step` 定位步骤。
    ///
    /// 返回：按阶段与尝试号排序的事实集合；读取失败返回封闭错误。
    async fn load_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
    ) -> Result<Vec<SagaStepAttemptRow>, SagaBackendError>;

    /// 业务作用：按效果方向统计已登记的 resolution 尝试预算。
    ///
    /// 参数说明：实例、步骤和 `direction` 共同限定统计范围。
    ///
    /// 返回：已登记次数；读取或数值转换失败返回封闭错误。
    async fn count_resolution_attempts(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        direction: Direction,
    ) -> Result<u32, SagaBackendError>;

    /// 业务作用：在结果事实同一事务内单调更新步骤 journal 投影。
    ///
    /// 参数说明：实例和步骤定位行，`patch` 只携带本次允许变化的字段。
    ///
    /// 返回：投影更新成功返回 `Ok`；行缺失或持久化失败返回封闭错误。
    async fn update_step_journal(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        patch: &StepJournalPatch<'_>,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：凭同事务管理审计重新开放冻结的补偿步骤。
    ///
    /// 参数说明：实例、步骤和 `operation_id` 共同定位授权事实。
    ///
    /// 返回：唯一合法步骤重开时成功；门禁不成立或持久化失败返回封闭错误。
    async fn reopen_halted_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：凭同事务管理审计重新开放冻结的结果查询。
    ///
    /// 参数说明：实例、步骤和 `operation_id` 共同定位授权事实。
    ///
    /// 返回：唯一合法查询重开时成功；门禁不成立或持久化失败返回封闭错误。
    async fn reopen_halted_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        operation_id: &str,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：把冻结补偿计划原子投影到计划成员及稳定逆序位置。
    ///
    /// 参数说明：`saga_id` 是实例身份，`plan` 是已由状态机冻结的计划。
    ///
    /// 返回：全部成员投影完成时成功；计划或持久化异常返回封闭错误。
    async fn mark_compensation_plan(
        &self,
        saga_id: &SagaId,
        plan: &CompensationPlan,
    ) -> Result<(), SagaBackendError>;
}

/// 业务作用：提供 durable timer 调度、带 fencing 领取、完成和交还能力。
#[async_trait]
pub trait SagaTimerStore: Send + Sync {
    /// 业务作用：在状态迁移同一事务内幂等调度 timer。
    ///
    /// 参数说明：`spec` 是 timer 身份、作用域、期限、attempt 和实例版本。
    ///
    /// 返回：区分首次调度与一致重放；冲突或持久化失败返回封闭错误。
    async fn schedule_timer(&self, spec: &TimerSpec<'_>)
        -> Result<TimerSchedule, SagaBackendError>;

    /// 业务作用：在状态离开时取消同一作用域下尚未触发的 timer。
    ///
    /// 参数说明：实例与 `scope` 定位范围，`kind` 可进一步限定种类。
    ///
    /// 返回：实际取消行数；持久化失败返回封闭错误。
    async fn cancel_scope_timers(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: Option<&str>,
    ) -> Result<u64, SagaBackendError>;

    /// 业务作用：重排仍可复活的 timer，并递增 generation 撤销旧领取权威。
    ///
    /// 参数说明：实例、作用域、种类和 attempt 定位 timer，期限与实例版本给出新约束。
    ///
    /// 返回：区分已重排与不存在；持久化失败返回封闭错误。
    async fn reschedule_timer(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: &str,
        attempt: AttemptNo,
        due_at_ms: i64,
        expected_saga_version: u64,
    ) -> Result<TimerReschedule, SagaBackendError>;

    /// 业务作用：以数据库可见租约和不可复制 token 领取有界到期 timer 批次。
    ///
    /// 参数说明：`owner` 与 token 建立权威，时钟、租约和上限约束本轮领取。
    ///
    /// 返回：领取事务明确提交后返回与 token 绑定的批次；不确定或失败返回封闭错误。
    async fn claim_due_timers(
        &self,
        owner: &str,
        fencing_token: TimerFencingToken,
        now_ms: i64,
        lease_ms: i64,
        limit: u32,
    ) -> Result<TimerClaimBatch, SagaBackendError>;

    /// 业务作用：凭当前 claim token 把 timer 标记为已触发。
    ///
    /// 参数说明：`timer_id` 定位行，token 与时钟复验租约权威。
    ///
    /// 返回：区分应用与失权；持久化失败返回封闭错误。
    async fn complete_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
    ) -> Result<TimerFencing, SagaBackendError>;

    /// 业务作用：凭当前 claim token 交还未处理 timer 并设置下一可用时刻。
    ///
    /// 参数说明：timer、token、当前时钟和 `available_at_ms` 共同约束交还。
    ///
    /// 返回：区分应用与失权；持久化失败返回封闭错误。
    async fn release_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
        available_at_ms: i64,
    ) -> Result<TimerFencing, SagaBackendError>;

    /// 业务作用：把实例尚未完成的 timer 提前置为可领取，驱动恢复收敛。
    ///
    /// 参数说明：`saga_id` 是待唤醒实例。
    ///
    /// 返回：实际唤醒行数；持久化失败返回封闭错误。
    async fn wake_saga_timers(&self, saga_id: &SagaId) -> Result<u64, SagaBackendError>;
}

/// 业务作用：提供参与方 execute、cancel、compensate 和 resolve 的持久 gate。
#[async_trait]
pub trait SagaParticipantStore: Send + Sync {
    /// 业务作用：在 Inbox 事务内裁决 execute 是否首次允许产生外部效果。
    ///
    /// 参数说明：`gate` 绑定组合身份，`execute_effect` 是跨尝试稳定效果身份。
    ///
    /// 返回：执行许可、等待或既有结果；合同或持久化失败返回封闭错误。
    async fn admit_execute(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
    ) -> Result<ExecuteAdmission, SagaBackendError>;

    /// 业务作用：在结果 Outbox 同一事务内结算 execute gate。
    ///
    /// 参数说明：实例和步骤定位 gate，`status` 是已裁决业务结果。
    ///
    /// 返回：唯一合法结算成功时返回 `Ok`；冲突或持久化失败返回封闭错误。
    async fn settle_execute(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepForwardStatus,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：以行锁建立 cancel 屏障并裁决本地取消结果。
    ///
    /// 参数说明：gate 与 execute/cancel 效果身份共同限定一次取消意图。
    ///
    /// 返回：可确定取消结果或等待；身份冲突及持久化失败返回封闭错误。
    async fn adjudicate_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<CancelAdjudication, SagaBackendError>;

    /// 业务作用：建立外部 cancel 独占门禁，阻止并发 execute 提交互斥效果。
    ///
    /// 参数说明：gate 与 execute/cancel 效果身份共同限定外部取消。
    ///
    /// 返回：执行外部取消或重放既有裁决；冲突及持久化失败返回封闭错误。
    async fn admit_external_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<ExternalCancelAdmission, SagaBackendError>;

    /// 业务作用：在结果 Outbox 同一事务内结算外部 cancel 与正向最终事实。
    ///
    /// 参数说明：实例和步骤定位 gate，`forward` 与 `cancel` 必须是合法组合。
    ///
    /// 返回：组合事实唯一结算成功时返回 `Ok`；否则返回封闭错误。
    async fn settle_external_cancel(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        forward: StepForwardStatus,
        cancel: StepCancelStatus,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：裁决补偿是否首次允许执行，并门禁人工恢复身份。
    ///
    /// 参数说明：gate、补偿效果身份及可选恢复 operation 共同限定许可。
    ///
    /// 返回：执行许可、等待或既有结果；冲突及持久化失败返回封闭错误。
    async fn admit_compensation(
        &self,
        gate: &ParticipantGateKey<'_>,
        compensate_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<CompensationAdmission, SagaBackendError>;

    /// 业务作用：在结果 Outbox 同一事务内结算补偿事实。
    ///
    /// 参数说明：实例和步骤定位 gate，`status` 是补偿裁决结果。
    ///
    /// 返回：合法结算成功时返回 `Ok`；冲突或持久化失败返回封闭错误。
    async fn settle_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepCompensationStatus,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：裁决 Unknown 结果查询是否首次允许执行，并门禁人工恢复身份。
    ///
    /// 参数说明：gate、查询效果身份及可选恢复 operation 共同限定许可。
    ///
    /// 返回：查询许可、等待或既有结果；冲突及持久化失败返回封闭错误。
    async fn admit_resolution(
        &self,
        gate: &ParticipantGateKey<'_>,
        resolve_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<ResolutionAdmission, SagaBackendError>;

    /// 业务作用：在结果 Outbox 同一事务内结算正向或补偿 Unknown 查询事实。
    ///
    /// 参数说明：实例和步骤定位 gate，`target` 与 `status` 描述裁决对象和结果。
    ///
    /// 返回：合法结算成功时返回 `Ok`；冲突或持久化失败返回封闭错误。
    async fn settle_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        target: ResolutionTarget,
        status: StepResolutionStatus,
    ) -> Result<(), SagaBackendError>;
}

/// 业务作用：提供 Saga 审计事实写入和有界读取能力。
#[async_trait]
pub trait SagaAuditStore: Send + Sync {
    /// 业务作用：在互斥 attempt 结果裁决同一事务内保存双方证据。
    ///
    /// 参数说明：`fact` 包含定位、既有事实、incoming 事实和冲突类别。
    ///
    /// 返回：幂等保存成功返回 `Ok`；合同或持久化失败返回封闭错误。
    async fn record_attempt_conflict(
        &self,
        fact: &AttemptConflictFact<'_>,
    ) -> Result<(), SagaBackendError>;

    /// 业务作用：有界读取实例的 attempt 审计事实。
    ///
    /// 参数说明：`saga_id` 定位实例，`limit` 限制返回量。
    ///
    /// 返回：稳定排序的事实集合；参数或读取失败返回封闭错误。
    async fn load_attempt_audit(
        &self,
        saga_id: &SagaId,
        limit: u32,
    ) -> Result<Vec<SagaStepAttemptRow>, SagaBackendError>;

    /// 业务作用：按序号游标读取实例状态迁移审计。
    ///
    /// 参数说明：实例、`after_seq` 和 `limit` 共同限定结果页。
    ///
    /// 返回：稳定排序的 transition 集合；参数或读取失败返回封闭错误。
    async fn load_transition_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaTransitionAuditRow>, SagaBackendError>;

    /// 业务作用：按序号游标读取实例控制态迁移审计。
    ///
    /// 参数说明：实例、`after_seq` 和 `limit` 共同限定结果页。
    ///
    /// 返回：稳定排序的控制审计集合；参数或读取失败返回封闭错误。
    async fn load_control_audit(
        &self,
        saga_id: &SagaId,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<SagaControlAuditRow>, SagaBackendError>;

    /// 业务作用：有界读取实例的人工管理审计。
    ///
    /// 参数说明：`saga_id` 定位实例，`limit` 限制返回量。
    ///
    /// 返回：稳定排序的管理审计集合；参数或读取失败返回封闭错误。
    async fn load_management_audit(
        &self,
        saga_id: &SagaId,
        limit: u32,
    ) -> Result<Vec<SagaManagementAuditRow>, SagaBackendError>;

    /// 业务作用：有界读取实例的互斥事实证据。
    ///
    /// 参数说明：`saga_id` 定位实例，`limit` 限制返回量。
    ///
    /// 返回：稳定排序的冲突事实集合；参数或读取失败返回封闭错误。
    async fn load_conflict_audit(
        &self,
        saga_id: &SagaId,
        limit: u32,
    ) -> Result<Vec<SagaConflictFactRow>, SagaBackendError>;
}

/// 业务作用：提供租户配额、管理动作频率与数据库事实指标能力。
#[async_trait]
pub trait SagaGovernanceStore: Send + Sync {
    /// 业务作用：在实例创建事务内原子预留租户在飞名额。
    ///
    /// 参数说明：`tenant` 是受信租户，`cap` 是可选上限。
    ///
    /// 返回：区分已预留、超额和账本未初始化；持久化失败返回封闭错误。
    async fn reserve_tenant_quota(
        &self,
        tenant: &TenantId,
        cap: Option<u64>,
    ) -> Result<QuotaReservation, SagaBackendError>;

    /// 业务作用：确认租户账本是否已按持久实例事实完成初始化。
    ///
    /// 参数说明：`tenant` 是受信租户。
    ///
    /// 返回：已初始化时为 `true`；读取失败返回封闭错误。
    async fn tenant_quota_initialized(&self, tenant: &TenantId) -> Result<bool, SagaBackendError>;

    /// 业务作用：读取租户在飞实例账本值。
    ///
    /// 参数说明：`tenant` 是受信租户。
    ///
    /// 返回：不存在账本时为零；读取失败返回封闭错误。
    async fn tenant_quota_usage(&self, tenant: &TenantId) -> Result<u64, SagaBackendError>;

    /// 业务作用：在同源事务内锁定账本并按非终态实例事实重算配额。
    ///
    /// 参数说明：`tenant` 是受信租户。
    ///
    /// 返回：对账后的在飞数；事务或持久化失败返回封闭错误。
    async fn reconcile_tenant_quota(&self, tenant: &TenantId) -> Result<u64, SagaBackendError>;

    /// 业务作用：在管理动作事务内按数据库时钟预留租户频率预算。
    ///
    /// 参数说明：`tenant` 是受信租户，动作上限和窗口共同定义预算。
    ///
    /// 返回：区分已预留和已耗尽；参数或持久化失败返回封闭错误。
    async fn reserve_tenant_action_rate(
        &self,
        tenant: &TenantId,
        max_actions: u64,
        window_ms: i64,
    ) -> Result<ActionRateReservation, SagaBackendError>;

    /// 业务作用：按数据库时钟读取当前租户动作窗口及已用次数。
    ///
    /// 参数说明：`tenant` 是受信租户，`window_ms` 是窗口长度。
    ///
    /// 返回：窗口起点和已用次数；参数或读取失败返回封闭错误。
    async fn tenant_action_rate_usage(
        &self,
        tenant: &TenantId,
        window_ms: i64,
    ) -> Result<(u64, u64), SagaBackendError>;

    /// 业务作用：从已提交 Saga 表读取低基数运行指标快照。
    ///
    /// 参数说明：`now_ms` 是统一 epoch 毫秒观测时刻。
    ///
    /// 返回：数据库事实快照；参数或读取失败返回封闭错误。
    async fn load_operational_metrics(
        &self,
        now_ms: i64,
    ) -> Result<SagaStoreMetrics, SagaBackendError>;
}

/// 业务作用：汇总运行核心所需的全部 Saga 持久能力，不包含 schema 自举。
pub trait SagaStore:
    SagaInstanceStore
    + SagaJournalStore
    + SagaTimerStore
    + SagaParticipantStore
    + SagaAuditStore
    + SagaGovernanceStore
{
}

impl<T> SagaStore for T where
    T: SagaInstanceStore
        + SagaJournalStore
        + SagaTimerStore
        + SagaParticipantStore
        + SagaAuditStore
        + SagaGovernanceStore
{
}

/// 业务作用：把同一数据库后端的 Saga store、Inbox、Outbox 和事务执行器冻结成组合身份。
pub trait SagaBackend: Send + Sync {
    /// Saga 状态与参与方 gate 的持久实现。
    type Store: SagaStore;
    /// 与 Saga store 共享 datasource 和 ambient transaction 的 Inbox 实现。
    type Inbox: InboxStore;
    /// 与 Saga store 共享 datasource 和 ambient transaction 的 Outbox 写实现。
    type Outbox: DurableOutboxAppend;
    /// 为三种持久角色建立同源事务的执行器。
    type TransactionRunner: SagaTransactionRunner;

    /// 业务作用：读取该组合绑定的 Saga store。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：生命周期受组合对象约束的 store 引用。
    fn store(&self) -> &Self::Store;

    /// 业务作用：读取该组合绑定的 Inbox。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 store 同 datasource 的 Inbox 引用。
    fn inbox(&self) -> &Self::Inbox;

    /// 业务作用：读取该组合绑定的 Outbox 写端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 store 同 datasource 的 Outbox 引用。
    fn outbox(&self) -> &Self::Outbox;

    /// 业务作用：读取为组合持久角色建立原子边界的事务执行器。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：绑定相同 datasource 和 driver 的执行器引用。
    fn transaction_runner(&self) -> &Self::TransactionRunner;

    /// 业务作用：读取组合内全部持久角色共享的 datasource 身份。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不含连接信息的规范 qualifier。
    fn datasource_ref(&self) -> &DatasourceRef;

    /// 业务作用：读取组合使用的数据库 driver，阻止跨 driver 事务误配。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：构造组合时冻结的 driver。
    fn driver(&self) -> DatabaseDriver;
}

/// 业务作用：为兼容运行入口按 datasource 构造同源 Saga 完整后端。
pub trait SagaBackendFactory: SagaBackend + Sized {
    /// 业务作用：构造绑定默认 datasource 的完整后端。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：身份可用时返回不建连的后端句柄；构造条件不成立时返回封闭错误。
    fn default_backend() -> Result<Self, SagaBackendError>;

    /// 业务作用：构造绑定命名 datasource 的完整后端并在 I/O 前复验同源。
    ///
    /// 参数说明：`datasource` 是启动期冻结的规范 qualifier。
    ///
    /// 返回：名称与全部持久角色匹配时返回后端；否则返回封闭错误。
    fn backend_for(datasource: &str) -> Result<Self, SagaBackendError>;
}
