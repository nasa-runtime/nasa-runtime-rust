//! 实例的创建幂等、加载与 CAS 状态推进。
//!
//! 本模块承载两条安全不变量：
//!
//! 1. **创建是受保护事务**：实例 INSERT 与 `NONE -> RUNNING` 初始 transition 在同一
//!    ambient 事务内落库，调用方在同一事务里追加首步命令 Outbox、durable timer 与 Audit，
//!    全部原子 COMMIT。唯一键冲突时只返回已存在实例，绝不重复发布首步命令。
//! 2. **推进走数据库 CAS**：`UPDATE ... WHERE version = ? AND status = ? AND control_state = 'ACTIVE'`
//!    影响为 0 表示状态已被其它副本推进、结果已过期或已被暂停，调用方必须重新读取并按幂等规则裁决，
//!    不得继续用旧快照写 Outbox。`transition_seq` 直接取 CAS 推进后的 `version`。

use crate::error::{is_unique_violation, map_connection, map_database, SagaStoreError};
use crate::row::{parse_instance_row, SagaInstanceRow};
use crate::MySqlSagaStore;
use nasaga_core::{
    check_transition, ControlState, DefinitionVersion, Direction, SagaId, SagaStatus, StepName,
    TransitionGuard, TriggerKind,
};
use sqlx::Row as _;

/// 初始 transition 的 `from_state` 哨兵值：实例诞生前没有业务状态。
///
/// 该值只出现在 `saga_transition.from_state`，不属于 [`SagaStatus`] 封闭集合，
/// 因此不可能与任何真实状态迁移混淆。
const FROM_STATE_NONE: &str = "NONE";

/// 业务作用：描述一次实例创建请求的全部持久化输入。
///
/// 字段说明：`trigger_kind`/`trigger_id` 记入初始 transition——由 Kafka 事件触发时是
/// 来源 event id，由 HTTP/管理面触发时是稳定 operation id；它们与业务幂等键共同保证
/// 创建入口可重放。
#[derive(Debug, Clone)]
pub struct NewSagaInstance<'a> {
    /// 实例身份，由调用方生成并全程稳定。
    pub saga_id: &'a SagaId,
    /// 租户身份；无租户部署使用固定 system tenant。
    pub tenant: &'a nasaga_core::TenantId,
    /// workflow 名称。
    pub workflow: &'a nasaga_core::WorkflowName,
    /// 业务幂等键，同一业务意图只允许一个实例。
    pub business_key: &'a nasaga_core::BusinessKey,
    /// 固定到实例的 definition 版本。
    pub definition_version: DefinitionVersion,
    /// definition 的 canonical 内容摘要（64 位小写十六进制）。
    pub definition_digest: &'a str,
    /// 启动请求的 canonical 摘要；只保存摘要，不把首步 payload 复制进 Orchestrator 表。
    pub start_request_digest: &'a str,
    /// 实例级业务 deadline（epoch 毫秒）；无全局期限时为空。
    pub deadline_at_ms: Option<i64>,
    /// 创建时定位的首个步骤；step 超时裁决依赖 `current_step` 与 timer 步骤一致。
    pub current_step: Option<&'a StepName>,
    /// 初始 transition 的触发来源类别。
    pub trigger_kind: TriggerKind,
    /// 初始 transition 的触发身份（event id 或 operation id）。
    pub trigger_id: &'a str,
}

/// 业务作用：区分创建入口的两种合法结果，驱动调用方决定是否发布首步命令。
///
/// 分支说明：`Existing` 表示业务幂等键命中已有实例——调用方**不得**再写首步命令
/// Outbox、timer 或初始 transition，只能向上返回已存在实例的当前状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaCreation {
    /// 本事务真实创建了实例；调用方须在同一事务内补齐首步命令 Outbox、timer 与 Audit。
    Created(SagaInstanceRow),
    /// 业务幂等键命中已有实例；不产生任何新副作用。
    Existing(SagaInstanceRow),
}

/// 业务作用：描述一次 CAS 状态推进要写入的全部内容。
///
/// 字段说明：`failure_code` 与 `compensation_plan_version` 为空时**保留列上既有值**
/// 而不是清空——失败证据与已冻结的计划摘要都不允许被后续迁移静默抹掉。
#[derive(Debug, Clone)]
pub struct TransitionSpec<'a> {
    /// 目标业务状态。
    pub to_status: SagaStatus,
    /// 推进后的方向。
    pub direction: Direction,
    /// 推进后的当前步骤；无明确定位时为空。
    pub current_step: Option<&'a StepName>,
    /// 触发来源类别，与 `trigger_id` 共同构成"同一触发只推进一次"唯一键。
    pub trigger_kind: TriggerKind,
    /// 触发身份（event id、timer id 或管理 operation id）。
    pub trigger_id: &'a str,
    /// 实例固定的 definition 版本，记入 transition 审计行。
    pub definition_version: DefinitionVersion,
    /// 本次迁移产生的稳定失败原因码；为空时保留既有值。
    pub failure_code: Option<&'a str>,
    /// 进入补偿时冻结的计划摘要；为空时保留既有值。
    pub compensation_plan_version: Option<&'a str>,
}

/// 业务作用：聚合一次暂停/恢复 CAS 的实例版本权威与审计事实，避免跨参数错配控制操作。
#[derive(Debug, Clone, Copy)]
pub struct ControlTransitionSpec<'a> {
    /// 实例身份。
    pub saga_id: &'a SagaId,
    /// 调用方持有的实例版本。
    pub expected_version: u64,
    /// 调用方持有的控制态 generation。
    pub expected_control_version: u64,
    /// 预期当前控制状态。
    pub from: ControlState,
    /// 目标控制状态。
    pub to: ControlState,
    /// 管理请求稳定幂等身份。
    pub operation_id: &'a str,
    /// 认证层提供的稳定主体 id。
    pub actor: &'a str,
    /// 本次控制动作的工单或事故原因。
    pub reason: &'a str,
}

/// 业务作用：区分 CAS 推进的三种结果，让调用方对"输掉竞争"与"重复触发"分别裁决。
///
/// 分支说明：`Conflict` 与 `DuplicateTrigger` 都意味着**本 ambient 事务必须放弃提交**
/// （向 `natx::run` 返回错误回滚）：`Conflict` 时实例行未被本事务修改，回滚后重新读取
/// 再裁决；`DuplicateTrigger` 时实例行已在本事务内被 CAS 修改，只有回滚才能撤销它，
/// 之后按"该触发已生效"确认消息即可。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome {
    /// 推进成功；`new_version` 同时是新的 transition_seq。
    Applied {
        /// CAS 之后的实例版本。
        new_version: u64,
    },
    /// 预期 version/status 未命中：状态已被其它副本推进，当前快照过期。
    Conflict,
    /// 同一触发已在本实例上推进过一次；本事务必须回滚后按已生效确认。
    DuplicateTrigger,
}

/// 业务作用：区分控制状态 CAS 的结果；`Conflict` 表示业务实例版本、控制 generation
/// 或当前控制状态与预期不符。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlCasOutcome {
    /// 控制状态已切换。
    Applied,
    /// 同一 operation 已提交过；不得再次改变控制态，调用方可按幂等成功返回。
    AlreadyApplied,
    /// 预期 version/control_version/control_state 未命中，调用方须重新读取。
    Conflict,
}

/// 业务作用：区分人工业务动作审计的首次写入与完全一致幂等重放。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagementAuditOutcome {
    /// 本事务首次登记该 operation。
    Recorded,
    /// 同一 operation、action、actor、reason 已提交过，调用方不得再次产生业务副作用。
    AlreadyRecorded,
}

/// 按实例身份加载快照的查询；列集合与 [`parse_instance_row`] 一一对应。
const SELECT_INSTANCE_BY_ID_SQL: &str = "SELECT saga_id, tenant_id, workflow_name, business_key, \
     definition_version, definition_digest, start_request_digest, status, control_state, control_version, direction, current_step, \
     compensation_plan_version, version, deadline_at, failure_code FROM saga_instance \
     WHERE saga_id = ?";

/// 按业务幂等键加载快照的查询；列集合与 [`parse_instance_row`] 一一对应。
const SELECT_INSTANCE_BY_BUSINESS_SQL: &str = "SELECT saga_id, tenant_id, workflow_name, \
     business_key, definition_version, definition_digest, status, control_state, control_version, direction, \
     start_request_digest, current_step, compensation_plan_version, version, deadline_at, failure_code \
     FROM saga_instance WHERE tenant_id = ? AND workflow_name = ? AND business_key = ?";

impl MySqlSagaStore {
    /// 业务作用：以受保护事务幂等创建 Saga 实例。
    ///
    /// 在当前 ambient 事务内写入 `status = RUNNING, version = 1` 的实例行与
    /// `transition_seq = 1, NONE -> RUNNING` 的初始 transition；业务幂等键
    /// `UNIQUE(tenant_id, workflow_name, business_key)` 冲突时不创建第二个实例，
    /// 只查询并返回已存在实例。
    ///
    /// 参数说明：
    /// - `spec`: 创建请求的全部持久化输入。
    ///
    /// 返回：真实创建返回 [`SagaCreation::Created`]，调用方必须在**同一事务**内补齐
    /// 首步命令 Outbox、durable timer 与 Audit；幂等命中返回 [`SagaCreation::Existing`]，
    /// 调用方不得再产生任何创建副作用。事务缺失、摘要/触发身份非法或底层失败返回错误。
    pub async fn create_instance(
        &self,
        spec: &NewSagaInstance<'_>,
    ) -> Result<SagaCreation, SagaStoreError> {
        validate_digest(spec.definition_digest)?;
        validate_digest(spec.start_request_digest)?;
        validate_trigger_id(spec.trigger_id)?;
        // 创建必须与首步命令 Outbox/timer/Audit 同一事务原子提交:事务外 autocommit 会打开
        // "实例已存在但首步命令永远丢失"的窗口。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;

        let inserted = sqlx::query(
            "INSERT INTO saga_instance (saga_id, tenant_id, workflow_name, business_key, \
             definition_version, definition_digest, start_request_digest, status, control_state, control_version, direction, \
             current_step, version, deadline_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 1, ?)",
        )
        .bind(spec.saga_id.as_str())
        .bind(spec.tenant.as_str())
        .bind(spec.workflow.as_str())
        .bind(spec.business_key.as_str())
        .bind(spec.definition_version.get())
        .bind(spec.definition_digest)
        .bind(spec.start_request_digest)
        .bind(SagaStatus::INITIAL.as_str())
        .bind(ControlState::Active.as_str())
        .bind(Direction::Forward.as_str())
        .bind(spec.current_step.map(StepName::as_str))
        .bind(spec.deadline_at_ms)
        .execute(connection.as_mut())
        .await;

        match inserted {
            Ok(_) => {}
            // 唯一键冲突 = 同一业务意图已有实例(或极端情况下 saga_id 撞车):
            // 只返回既有实例状态,绝不重复写初始 transition 或让调用方重发首步命令。
            Err(error) if is_unique_violation(&error) => {
                // natx 事务槽锁不可重入:必须先归还本方法持有的连接,
                // 嵌套查询才能重新取得同一事务连接,否则自死锁。
                drop(connection);
                let existing = self
                    .find_instance(spec.tenant, spec.workflow, spec.business_key)
                    .await?
                    .ok_or_else(|| {
                        // 业务幂等键查不到但插入仍冲突,说明 saga_id 与其它业务意图撞车,
                        // 这是身份生成被破坏的信号,不能当作幂等命中静默吸收。
                        SagaStoreError::new("saga_id collides with a different business intent")
                    })?;
                // business_key 只证明“同一业务槽位”，不能证明调用参数相同；摘要不一致
                // 若仍按幂等成功返回，会让调用方误以为新 payload/deadline 已被接受。
                if existing.start_request_digest.as_deref() != Some(spec.start_request_digest) {
                    return Err(SagaStoreError::new(
                        "business idempotency key was reused with a different start request",
                    ));
                }
                return Ok(SagaCreation::Existing(existing));
            }
            Err(error) => return Err(map_database(error)),
        }

        // RUNNING 是状态机唯一合法初始状态;初始 transition 与实例行同事务落库,
        // 使"实例存在但没有诞生审计"的中间态不可能被观察到。
        sqlx::query(
            "INSERT INTO saga_transition (saga_id, transition_seq, from_state, to_state, \
             trigger_kind, trigger_id, definition_version) VALUES (?, 1, ?, ?, ?, ?, ?)",
        )
        .bind(spec.saga_id.as_str())
        .bind(FROM_STATE_NONE)
        .bind(SagaStatus::INITIAL.as_str())
        .bind(spec.trigger_kind.as_str())
        .bind(spec.trigger_id)
        .bind(spec.definition_version.get())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;

        // natx 事务槽锁不可重入:先归还连接再回读,回读仍在同一 ambient 事务内
        // (read-your-writes),否则嵌套取锁自死锁。
        drop(connection);
        let created = self.load_instance(spec.saga_id).await?.ok_or_else(|| {
            SagaStoreError::new("instance vanished inside its own creation transaction")
        })?;
        Ok(SagaCreation::Created(created))
    }

    /// 业务作用：按实例身份加载已提交快照，作为一切推进裁决的输入。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    ///
    /// 返回：存在时返回快照；不存在返回 `None`；读回列损坏或底层失败返回错误。
    pub async fn load_instance(
        &self,
        saga_id: &SagaId,
    ) -> Result<Option<SagaInstanceRow>, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let row = sqlx::query(SELECT_INSTANCE_BY_ID_SQL)
            .bind(saga_id.as_str())
            .fetch_optional(connection.as_mut())
            .await
            .map_err(map_database)?;
        row.as_ref().map(parse_instance_row).transpose()
    }

    /// 业务作用：分页扫描非终态实例，服务启动预检（definition 摘要一致性）与恢复巡检。
    ///
    /// 参数说明：
    /// - `limit`: 单次最多返回的实例数（有界，防一次拉爆）。
    ///
    /// 返回：状态不在终态集合内的实例快照，按 saga_id 排序；读回列损坏或底层失败返回错误。
    pub async fn list_non_terminal(
        &self,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaStoreError> {
        self.list_non_terminal_after(None, limit).await
    }

    /// 业务作用：以 saga_id keyset 分页扫描非终态实例，避免启动预检只覆盖首批数据。
    ///
    /// 参数说明：
    /// - `after`: 上一页最后一个 saga_id；为空从首行开始。
    /// - `limit`: 本页最多返回的实例数，必须为正。
    ///
    /// 返回：严格位于 cursor 之后的非终态实例，按 saga_id 升序；参数非法、读回列损坏
    /// 或底层失败返回错误。
    pub async fn list_non_terminal_after(
        &self,
        after: Option<&SagaId>,
        limit: u32,
    ) -> Result<Vec<SagaInstanceRow>, SagaStoreError> {
        if limit == 0 {
            return Err(SagaStoreError::new(
                "non-terminal scan page size must be positive",
            ));
        }
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let rows =
            match after {
                Some(after) => sqlx::query(
                    "SELECT saga_id, tenant_id, workflow_name, business_key, definition_version, \
                     definition_digest, start_request_digest, status, control_state, control_version, direction, current_step, \
                     compensation_plan_version, version, deadline_at, failure_code \
                     FROM saga_instance WHERE saga_id > ? AND status NOT IN (?, ?) \
                     ORDER BY saga_id ASC LIMIT ?",
                )
                .bind(after.as_str())
                .bind(SagaStatus::Completed.as_str())
                .bind(SagaStatus::Compensated.as_str())
                .bind(limit)
                .fetch_all(connection.as_mut())
                .await,
                None => sqlx::query(
                    "SELECT saga_id, tenant_id, workflow_name, business_key, definition_version, \
                     definition_digest, start_request_digest, status, control_state, control_version, direction, current_step, \
                     compensation_plan_version, version, deadline_at, failure_code \
                     FROM saga_instance WHERE status NOT IN (?, ?) \
                     ORDER BY saga_id ASC LIMIT ?",
                )
                .bind(SagaStatus::Completed.as_str())
                .bind(SagaStatus::Compensated.as_str())
                .bind(limit)
                .fetch_all(connection.as_mut())
                .await,
            }
            .map_err(map_database)?;
        rows.iter().map(parse_instance_row).collect()
    }

    /// 业务作用：按业务幂等键查找实例，服务创建幂等命中与管理面按业务身份定位。
    ///
    /// 参数说明：
    /// - `tenant`: 租户身份。
    /// - `workflow`: workflow 名称。
    /// - `business_key`: 业务幂等键。
    ///
    /// 返回：存在时返回快照；不存在返回 `None`；读回列损坏或底层失败返回错误。
    pub async fn find_instance(
        &self,
        tenant: &nasaga_core::TenantId,
        workflow: &nasaga_core::WorkflowName,
        business_key: &nasaga_core::BusinessKey,
    ) -> Result<Option<SagaInstanceRow>, SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        let row = sqlx::query(SELECT_INSTANCE_BY_BUSINESS_SQL)
            .bind(tenant.as_str())
            .bind(workflow.as_str())
            .bind(business_key.as_str())
            .fetch_optional(connection.as_mut())
            .await
            .map_err(map_database)?;
        row.as_ref().map(parse_instance_row).transpose()
    }

    /// 业务作用：以数据库 CAS 推进实例状态，并在同一事务内写 transition 审计行。
    ///
    /// 迁移先经 [`check_transition`] 在封闭状态机内裁决（含"已发生补偿副作用后禁止
    /// 恢复正向"的业务门禁），再执行 `WHERE version = ? AND status = ?` 的条件更新；
    /// `transition_seq` 直接取推进后的 `version`，不引入第二个序列来源。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `expected_version`: 调用方持有快照的实例版本，作为 CAS 预期值。
    /// - `from_status`: 调用方持有快照的状态，必须来自持久化读取。
    /// - `spec`: 本次推进要写入的全部内容。
    ///
    /// 返回：成功返回 [`CasOutcome::Applied`] 与新实例版本；版本或状态预期未命中，或
    /// 实例已暂停时返回 `Conflict`（实例行未被修改）；同一触发重复返回
    /// `DuplicateTrigger`（实例行已被本事务修改，
    /// 调用方**必须回滚**整个 ambient 事务）。迁移不在封闭集合内、事务缺失或底层失败
    /// 返回错误。
    pub async fn advance(
        &self,
        saga_id: &SagaId,
        expected_version: u64,
        from_status: SagaStatus,
        spec: &TransitionSpec<'_>,
    ) -> Result<CasOutcome, SagaStoreError> {
        validate_trigger_id(spec.trigger_id)?;
        if let Some(failure_code) = spec.failure_code {
            validate_reason_code(failure_code)?;
        }
        if let Some(plan_version) = spec.compensation_plan_version {
            validate_digest(plan_version)?;
        }
        // transition 与后续补偿命令 Outbox/Audit 必须原子:状态推进了但命令丢失,
        // 或命令发出了但状态没推进,都会造成半完成状态。
        require_ambient_transaction()?;

        // 人工介入恢复正向前必须确认没有任何补偿副作用已经发生;该事实只能来自
        // 已提交 journal,同一事务内查询保证与 CAS 看到同一份快照。
        let guard = if from_status == SagaStatus::ManualIntervention
            && spec.to_status == SagaStatus::Running
        {
            TransitionGuard::new(self.any_compensation_succeeded(saga_id).await?)
        } else {
            TransitionGuard::default()
        };
        // 在写入任何行之前统一裁决结构性与业务合法性:非法迁移是不变量破坏,
        // 必须显式失败而不是写入后再补救。
        check_transition(from_status, spec.to_status, guard)
            .map_err(|violation| SagaStoreError::new(violation.code()))?;
        // version 同时充当 transition_seq；回绕会复用旧审计序号并破坏 CAS 单调性，
        // 因此必须在发出 UPDATE 前 fail-closed，不能依赖 debug 溢出 panic 或数据库报错。
        let new_version = expected_version
            .checked_add(1)
            .ok_or_else(|| SagaStoreError::new("saga version overflow"))?;

        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        // 暂停与业务推进必须争夺同一行级权威：即使 worker 先读到 ACTIVE，
        // 运维 pause 一旦先提交，这个 CAS 也必须失败，禁止在“暂停成功”后发新命令。
        let updated = sqlx::query(
            "UPDATE saga_instance SET status = ?, direction = ?, current_step = ?, \
             failure_code = COALESCE(?, failure_code), \
             compensation_plan_version = COALESCE(?, compensation_plan_version), \
             version = version + 1 \
             WHERE saga_id = ? AND version = ? AND status = ? AND control_state = 'ACTIVE'",
        )
        .bind(spec.to_status.as_str())
        .bind(spec.direction.as_str())
        .bind(spec.current_step.map(StepName::as_str))
        .bind(spec.failure_code)
        .bind(spec.compensation_plan_version)
        .bind(saga_id.as_str())
        .bind(expected_version)
        .bind(from_status.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        // 影响为 0 = 已被其它副本推进或结果过期:当前处理者必须重新读取再裁决,
        // 绝不能继续用旧快照写 Outbox。
        if updated.rows_affected() == 0 {
            return Ok(CasOutcome::Conflict);
        }

        // CAS 赢家独占 version=new_version,因此 (saga_id, transition_seq) 不可能撞车;
        // 这里唯一可能的冲突是 uk_trigger:同一触发已经推进过本实例。
        let transition = sqlx::query(
            "INSERT INTO saga_transition (saga_id, transition_seq, from_state, to_state, \
             trigger_kind, trigger_id, definition_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(saga_id.as_str())
        .bind(new_version)
        .bind(from_status.as_str())
        .bind(spec.to_status.as_str())
        .bind(spec.trigger_kind.as_str())
        .bind(spec.trigger_id)
        .bind(spec.definition_version.get())
        .execute(connection.as_mut())
        .await;
        match transition {
            Ok(_) => Ok(CasOutcome::Applied { new_version }),
            Err(error) if is_unique_violation(&error) => Ok(CasOutcome::DuplicateTrigger),
            Err(error) => Err(map_database(error)),
        }
    }

    /// 业务作用：以 CAS 切换管理面控制状态（暂停/恢复），不触碰业务状态机。
    ///
    /// 控制状态与业务状态正交：本操作**不递增 version 也不写 transition 行**——
    /// version 与 transition_seq 同源，控制态切换不产生业务状态迁移，占用序号会在
    /// 审计链里留下空洞；在途结果的记账也不因暂停而失效（暂停只停止发出新的自动动作）。
    /// CAS 同时使用业务 version 与独立 control_version：前者阻止跨业务推进的旧操作，
    /// 后者阻止 ACTIVE→PAUSED→ACTIVE 后旧快照再次命中的 ABA；稳定 `operation_id`
    /// 另写独立审计链，阻止公开 API 重载最新快照后把同一旧操作再次执行。
    ///
    /// 参数说明：
    /// - `spec`: 实例版本权威、from/to 状态和不可分割的 operation/actor/reason 审计事实。
    ///
    /// 返回：切换成功返回 `Applied`；同一 operation 已提交返回 `AlreadyApplied` 且不再次
    /// 改状态；业务 version、control_version 或当前控制状态与预期不符返回 `Conflict`。
    /// operation 身份被复用于另一控制动作、`from == to`、generation 非法、事务缺失或
    /// 底层失败返回错误。
    pub async fn set_control_state(
        &self,
        spec: &ControlTransitionSpec<'_>,
    ) -> Result<ControlCasOutcome, SagaStoreError> {
        if spec.from == spec.to {
            return Err(SagaStoreError::new(
                "control state transition requires distinct states",
            ));
        }
        if spec.expected_control_version == 0 || spec.expected_control_version == u64::MAX {
            return Err(SagaStoreError::new(
                "control version must be positive and incrementable",
            ));
        }
        validate_trigger_id(spec.operation_id)?;
        validate_audit_text(spec.actor, 128, "actor")?;
        validate_audit_text(spec.reason, 512, "reason")?;
        // 暂停/恢复必须与 operation 审计同事务：没有审计的管理动作无法幂等重放，
        // 也无法在事故复盘时归因。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;

        // 公开 API 会在每次调用时重载最新快照，仅靠 control_version 挡不住同一旧请求
        // 在一次完整 ABA 后再次执行。稳定 operation_id 命中时直接返回，禁止再触碰控制态。
        let prior = sqlx::query(
            "SELECT from_state, to_state, actor, reason FROM saga_control_transition \
             WHERE saga_id = ? AND operation_id = ? FOR UPDATE",
        )
        .bind(spec.saga_id.as_str())
        .bind(spec.operation_id)
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;
        if let Some(prior) = prior {
            let prior_from: String = prior.try_get("from_state").map_err(map_database)?;
            let prior_to: String = prior.try_get("to_state").map_err(map_database)?;
            let prior_actor: String = prior.try_get("actor").map_err(map_database)?;
            let prior_reason: String = prior.try_get("reason").map_err(map_database)?;
            if prior_from != spec.from.as_str()
                || prior_to != spec.to.as_str()
                || prior_actor != spec.actor
                || prior_reason != spec.reason
            {
                return Err(SagaStoreError::new(
                    "control operation id was reused with different audit facts",
                ));
            }
            return Ok(ControlCasOutcome::AlreadyApplied);
        }

        let updated = sqlx::query(
            "UPDATE saga_instance SET control_state = ?, control_version = control_version + 1, \
             paused_at = IF(? = 'PAUSED', CURRENT_TIMESTAMP(6), NULL) \
             WHERE saga_id = ? AND version = ? AND control_version = ? AND control_state = ?",
        )
        .bind(spec.to.as_str())
        .bind(spec.to.as_str())
        .bind(spec.saga_id.as_str())
        .bind(spec.expected_version)
        .bind(spec.expected_control_version)
        .bind(spec.from.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Ok(ControlCasOutcome::Conflict);
        }
        let control_seq = spec
            .expected_control_version
            .checked_add(1)
            .ok_or_else(|| SagaStoreError::new("control version overflow"))?;
        // 审计插入失败时外层事务必须连同上面的状态 UPDATE 一起回滚，绝不留下
        // 无稳定 operation 身份、以后无法安全判定是否执行过的控制变化。
        sqlx::query(
            "INSERT INTO saga_control_transition \
             (saga_id, control_seq, from_state, to_state, operation_id, actor, reason) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(spec.saga_id.as_str())
        .bind(control_seq)
        .bind(spec.from.as_str())
        .bind(spec.to.as_str())
        .bind(spec.operation_id)
        .bind(spec.actor)
        .bind(spec.reason)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(ControlCasOutcome::Applied)
    }

    /// 业务作用：在人工恢复产生状态迁移或命令 Outbox 的同一事务内登记主体与原因。
    ///
    /// operation id 是恢复 API 的幂等边界：完全一致重放返回 `AlreadyRecorded`，调用方
    /// 必须直接返回当前已提交状态；任一审计事实变化都 fail-closed，防止用同一 operation
    /// 身份掩盖第二次人工动作。
    ///
    /// 参数说明：
    /// - `saga_id`: 被操作实例。
    /// - `operation_id`: 管理请求稳定幂等身份。
    /// - `action`: 低基数稳定动作名。
    /// - `actor`: 认证主体稳定 id。
    /// - `reason`: 人工动作原因或工单摘要。
    ///
    /// 返回：首次登记返回 `Recorded`；完全一致重放返回 `AlreadyRecorded`；事实漂移、
    /// 事务缺失或数据库失败返回错误。
    pub async fn record_management_operation(
        &self,
        saga_id: &SagaId,
        operation_id: &str,
        action: &str,
        actor: &str,
        reason: &str,
    ) -> Result<ManagementAuditOutcome, SagaStoreError> {
        validate_trigger_id(operation_id)?;
        validate_audit_identifier(action, 64, "action")?;
        validate_audit_text(actor, 128, "actor")?;
        validate_audit_text(reason, 512, "reason")?;
        // 审计必须与业务迁移、命令 Outbox 同事务；先落审计再失败也会整体回滚。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let prior = sqlx::query(
            "SELECT action, actor, reason FROM saga_management_audit \
             WHERE saga_id = ? AND operation_id = ? FOR UPDATE",
        )
        .bind(saga_id.as_str())
        .bind(operation_id)
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;
        if let Some(prior) = prior {
            let prior_action: String = prior.try_get("action").map_err(map_database)?;
            let prior_actor: String = prior.try_get("actor").map_err(map_database)?;
            let prior_reason: String = prior.try_get("reason").map_err(map_database)?;
            if prior_action != action || prior_actor != actor || prior_reason != reason {
                return Err(SagaStoreError::new(
                    "management operation id was reused with different audit facts",
                ));
            }
            return Ok(ManagementAuditOutcome::AlreadyRecorded);
        }
        sqlx::query(
            "INSERT INTO saga_management_audit \
             (saga_id, operation_id, action, actor, reason) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(saga_id.as_str())
        .bind(operation_id)
        .bind(action)
        .bind(actor)
        .bind(reason)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(ManagementAuditOutcome::Recorded)
    }
}

/// 业务作用：统一断言当前处于 ambient 事务，拒绝关键写静默退化为 autocommit。
///
/// 参数说明: 无。
///
/// 返回：在事务内返回 `Ok`；否则返回稳定错误。
pub(crate) fn require_ambient_transaction() -> Result<(), SagaStoreError> {
    if !natx::in_transaction() {
        return Err(SagaStoreError::new(
            "saga store writes require an ambient transaction; autocommit is forbidden",
        ));
    }
    Ok(())
}

/// 业务作用：校验 definition 摘要是 64 位小写十六进制，拒绝把任意文本固定到实例。
///
/// 摘要列被启动预检用来判定"definition 内容未被静默改动"，格式外的值会让该校验
/// 永远失败或永远通过。
///
/// 参数说明：
/// - `digest`: 待校验的摘要文本。
///
/// 返回：合法返回 `Ok`；否则返回稳定错误。
fn validate_digest(digest: &str) -> Result<(), SagaStoreError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SagaStoreError::new(
            "definition digest must be 64 lowercase hex characters",
        ));
    }
    Ok(())
}

/// 业务作用：校验稳定失败码与 VARCHAR(64) 持久化合同一致，避免数据库截断变成毒消息重投。
///
/// 参数说明：
/// - `code`: 将写入 instance/step journal 的稳定原因码。
///
/// 返回：非空、至多 64 字节且只含安全标识符字符时返回 `Ok`；否则返回稳定合同错误。
pub(crate) fn validate_reason_code(code: &str) -> Result<(), SagaStoreError> {
    if code.is_empty()
        || code.len() > 64
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(SagaStoreError::new(
            "reason code must be a stable identifier of at most 64 bytes",
        ));
    }
    Ok(())
}

/// 业务作用：校验触发身份的空白、长度与控制字符边界，保护 transition 唯一键与审计可读性。
///
/// 参数说明：
/// - `trigger_id`: 待校验的触发身份。
///
/// 返回：合法返回 `Ok`；否则返回稳定错误。
fn validate_trigger_id(trigger_id: &str) -> Result<(), SagaStoreError> {
    if trigger_id.is_empty()
        || trigger_id.trim() != trigger_id
        || trigger_id.len() > 190
        || trigger_id.chars().any(char::is_control)
    {
        return Err(SagaStoreError::new("invalid trigger id"));
    }
    Ok(())
}

/// 业务作用：校验 actor/reason 审计文本，阻止列截断、控制字符注入与空主体落库。
///
/// 参数说明：
/// - `value`: 待持久化文本。
/// - `max_len`: 对应列字节上限。
/// - `field`: 低基数字段名，仅用于脱敏错误。
///
/// 返回：边界合法返回成功；否则返回稳定合同错误。
fn validate_audit_text(
    value: &str,
    max_len: usize,
    field: &'static str,
) -> Result<(), SagaStoreError> {
    if value.is_empty()
        || value.len() > max_len
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(SagaStoreError::new(format!("invalid management {field}")));
    }
    Ok(())
}

/// 业务作用：校验低基数管理动作名，避免任意文本污染索引与观测标签。
///
/// 参数说明：
/// - `value`: action 名。
/// - `max_len`: 数据库列上限。
/// - `field`: 脱敏错误中的字段名。
///
/// 返回：只含安全标识符字符且长度有界时返回成功。
fn validate_audit_identifier(
    value: &str,
    max_len: usize,
    field: &'static str,
) -> Result<(), SagaStoreError> {
    if value.is_empty()
        || value.len() > max_len
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(SagaStoreError::new(format!("invalid management {field}")));
    }
    Ok(())
}
