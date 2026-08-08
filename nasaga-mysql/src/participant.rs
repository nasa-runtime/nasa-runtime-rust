//! 参与方本地 step gate：execute 准入、取消屏障裁决与补偿准入的持久化基础。
//!
//! 跨服务参与方只持有自己的 Inbox、业务事实
//! 和这张最小 step 记录。**gate、业务写和结果 Outbox 必须在同一本地事务内**——只有
//! 满足这个前提，Cancel 与 execute 才能在同一串行化点（行锁）上安全收敛为
//! `CancelConfirmed` 或一个确定终态，而不是并发出两个互相矛盾的事实。
//!
//! 四个阶段状态列刻意分开保存（不用一个扁平枚举混淆阶段）：`forward_status` 记录
//! 正向裁决，`cancel_status` 记录取消屏障裁决，`compensation_status` 记录补偿进度，
//! `resolution_status` 记录解决通道进度。外部 intent 的幂等键必须使用对应 `effect_id`。

use nasaga_core::{
    DefinitionVersion, EffectId, SagaId, StepCancelStatus, StepCompensationStatus,
    StepForwardStatus, StepName, StepPhase, StepResolutionStatus, TenantId, WorkflowName,
};
use sqlx::Row as _;

use crate::error::{is_unique_violation, map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::MySqlSagaStore;

/// 参与方 step gate 建表语句。行级锁（`SELECT ... FOR UPDATE`）是 execute 与 cancel
/// 竞争的串行化点；`uk_execute_effect` 保证同一业务效果只有一行 gate。
const CREATE_PARTICIPANT_SQL: &str = "CREATE TABLE IF NOT EXISTS saga_participant_step ( \
     saga_id VARCHAR(256) NOT NULL, \
     step_name VARCHAR(128) NOT NULL, \
     tenant_id VARCHAR(256) NOT NULL, \
     workflow_name VARCHAR(128) NOT NULL, \
     definition_version INT UNSIGNED NOT NULL, \
     definition_digest CHAR(64) NOT NULL, \
     forward_status VARCHAR(32) NOT NULL, \
     cancel_status VARCHAR(32) NOT NULL, \
     compensation_status VARCHAR(32) NOT NULL, \
     resolution_status VARCHAR(32) NOT NULL, \
     execute_effect_id CHAR(36) NOT NULL, \
     cancel_effect_id CHAR(36) NULL, \
     compensate_effect_id CHAR(36) NULL, \
     resolve_effect_id CHAR(36) NULL, \
     created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
     updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
     CONSTRAINT chk_saga_participant_definition_digest CHECK (definition_digest IS NOT NULL \
       AND CHAR_LENGTH(definition_digest) = 64 \
       AND BINARY definition_digest = BINARY LOWER(definition_digest) \
       AND definition_digest NOT REGEXP '[^0-9a-f]'), \
     PRIMARY KEY (saga_id, step_name), \
     UNIQUE KEY uk_execute_effect (execute_effect_id) \
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";

/// 业务作用：标识参与方 gate 所属的步骤，并携带与 envelope 交叉校验所需的合同字段。
#[derive(Debug, Clone)]
pub struct ParticipantGateKey<'a> {
    /// 实例身份。
    pub saga_id: &'a SagaId,
    /// 步骤名称。
    pub step: &'a StepName,
    /// 租户身份；必须与 envelope 规范化 tenant 匹配。
    pub tenant: &'a TenantId,
    /// workflow 名称。
    pub workflow: &'a WorkflowName,
    /// definition 版本。
    pub definition_version: DefinitionVersion,
    /// definition canonical 摘要；必须与参与方首次见到的合同完全一致。
    pub definition_digest: &'a str,
}

/// 业务作用：区分 execute 准入的三种合法裁决，驱动 adapter 决定是否调用业务 handler。
///
/// 分支说明：`Suppressed` 表示取消屏障已先建立 admission fence——adapter 只提交
/// Inbox claim 并返回可 ACK 结果，**零正向业务效果**，且不得伪造 `StepRejected`
/// （取消事实已由先前同事务写出的 `CancelConfirmed` Outbox 证明）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteAdmission {
    /// 准入成功；调用方必须在同一事务内执行业务并 [`MySqlSagaStore::settle_execute`]。
    Admitted,
    /// 该效果已有确定终态（重投）；按既有终态返回幂等快照，不重复业务效果。
    AlreadyTerminal(StepForwardStatus),
    /// 取消屏障已建立，执行被本地状态拒绝；零业务效果。
    Suppressed,
}

/// 业务作用：区分补偿准入的三种合法裁决。
///
/// 分支说明：`MissingForwardEffect` 表示本地既无正向成功效果也无已补偿证据——补偿只会
/// 对 Orchestrator 已记账成功的步骤发出，出现该分支说明协议被破坏，调用方必须提交
/// `CompensationOutcome::Halted` 合同违规事实并告警，不得自旋等待正向命令。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompensationAdmission {
    /// 准入成功；调用方必须在同一事务内执行补偿并 [`MySqlSagaStore::settle_compensation`]。
    Admitted,
    /// 本地已有补偿终态或不确定事实；普通命令按原状态重发，绝不再次调用补偿业务。
    AlreadySettled(StepCompensationStatus),
    /// 本地无正向成功效果且无补偿证据：协议破坏，按 `Halted` 合同违规提交。
    MissingForwardEffect,
}

/// 业务作用：区分取消屏障的三种合法裁决，与 [`nasaga_core::CancelOutcome`] 语义对齐。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelAdjudication {
    /// 执行从未开始，admission fence 已建立；零正向效果。
    Confirmed,
    /// 执行已有确定终态，取消无从谈起；携带真实终态供 Orchestrator 记账。
    AlreadyTerminal(StepForwardStatus),
    /// 已提交外部 intent、调用在途或结果未知；不谎报取消成功。
    ResolutionPending,
}

/// 业务作用：区分 externally-cancellable 取消是否需要调用业务 handler。
///
/// `Admitted` 表示 store 已持有与 execute 相同的 gate 行锁，调用方必须在同一事务内调用
/// 类型化外部取消并落账；`AlreadyAdjudicated` 表示既有屏障事实只需重发 result，禁止重复
/// 调用可能带外部副作用的 cancel API。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalCancelAdmission {
    /// 已取得 gate 串行化权威，可以调用外部取消 handler。
    Admitted,
    /// 本地已有可重放的真实取消裁决，不得再次调用外部系统。
    AlreadyAdjudicated(CancelAdjudication),
}

/// 业务作用：标识 resolve 正在裁决正向效果还是补偿效果，避免把退款查询结果写入正向状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionTarget {
    /// 裁决此前 `execute=UNKNOWN` 的正向效果。
    Forward,
    /// 裁决此前 `compensation=UNKNOWN` 的补偿效果。
    Compensation,
}

/// 业务作用：区分 resolve 查询的准入、终态重放与协议违规，保证查询可重试但确定裁决不可回退。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionAdmission {
    /// 存在待裁决的未知效果；调用方可以执行类型化查询 handler。
    Admitted(ResolutionTarget),
    /// 本地已有确定裁决；新 attempt 只重发该事实，不再次查询或执行裁决型副作用。
    AlreadySettled {
        /// 既有裁决所属方向。
        target: ResolutionTarget,
        /// 已提交的解决终态。
        status: StepResolutionStatus,
    },
    /// 本地不存在任何未知效果；该 resolve 命令违反 Orchestrator/参与方合同。
    MissingUnknownEffect,
}

impl MySqlSagaStore {
    /// 业务作用：创建参与方 gate 表。生产环境应由 migration 拥有 schema，此方法只供
    /// 受控自举环境使用。需先 `natx::init`。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：建表成功返回 `Ok`；连接不可用或 DDL 失败返回脱敏错误。
    pub async fn ensure_participant_schema() -> Result<(), SagaStoreError> {
        let mut connection = natx::conn().await.map_err(map_connection)?;
        sqlx::query(CREATE_PARTICIPANT_SQL)
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        let has_definition_digest: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
             AND TABLE_NAME = 'saga_participant_step' AND COLUMN_NAME = 'definition_digest'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if has_definition_digest == 0 {
            // 历史 gate 无法从本地事实反推 definition 摘要；保持 NULL 使新 runtime
            // fail-closed，禁止为了兼容而伪造摘要。生产库由有序 migration 执行同样变更。
            sqlx::query(
                "ALTER TABLE saga_participant_step ADD COLUMN definition_digest CHAR(64) NULL \
                 AFTER definition_version",
            )
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        let invalid_gates: i64 = sqlx::query_scalar(
            "SELECT CAST(COUNT(*) AS SIGNED) FROM saga_participant_step \
             WHERE definition_digest IS NULL \
                OR CHAR_LENGTH(definition_digest) <> 64 \
                OR BINARY definition_digest <> BINARY LOWER(definition_digest) \
                OR definition_digest REGEXP '[^0-9a-f]'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if invalid_gates != 0 {
            // 新 binary 在历史摘要完整且格式合法前必须拒绝 Ready；NULL 在非严格 DDL 下会
            // 被静默改为空串，因此两者都必须拦截。摘要只能由受信发布记录回填。
            return Err(SagaStoreError::new(
                "participant definition digest backfill is incomplete or invalid",
            ));
        }
        let has_digest_check: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE \
             CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'saga_participant_step' \
             AND CONSTRAINT_NAME = 'chk_saga_participant_definition_digest' \
             AND CONSTRAINT_TYPE = 'CHECK'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if has_digest_check == 0 {
            // CHECK 必须先于 NOT NULL 封口建立；它让非严格 sql_mode 也无法把 NULL 静默
            // 归一化为空串后放行，从而保持升级和新建表相同的摘要合同。
            sqlx::query(
                "ALTER TABLE saga_participant_step \
                 ADD CONSTRAINT chk_saga_participant_definition_digest CHECK (\
                   definition_digest IS NOT NULL \
                   AND CHAR_LENGTH(definition_digest) = 64 \
                   AND BINARY definition_digest = BINARY LOWER(definition_digest) \
                   AND definition_digest NOT REGEXP '[^0-9a-f]')",
            )
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        let nullable: String = sqlx::query_scalar(
            "SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
             AND TABLE_NAME = 'saga_participant_step' AND COLUMN_NAME = 'definition_digest'",
        )
        .fetch_one(connection.as_mut())
        .await
        .map_err(map_database)?;
        if nullable == "YES" {
            // 空表或已完成受信回填的受控环境立即封闭兼容窗，保持与摘要约束脚本的生产终态一致。
            sqlx::query(
                "ALTER TABLE saga_participant_step MODIFY COLUMN definition_digest CHAR(64) NOT NULL",
            )
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        Ok(())
    }

    /// 业务作用：在业务写之前竞争 execute 准入，与取消屏障在同一行锁上串行化。
    ///
    /// 必须发生在**任何业务写之前**：先锁 gate 行再执行业务，Cancel 与 execute 才能
    /// 收敛为唯一裁决；先写业务再竞争 gate 会让取消屏障对已发生的效果谎报成功。
    ///
    /// 参数说明：
    /// - `gate`: 步骤 gate 身份与合同字段。
    /// - `execute_effect`: execute 阶段跨 attempt 稳定的效果身份。
    ///
    /// 返回：准入成功返回 [`ExecuteAdmission::Admitted`]；效果已有终态返回
    /// `AlreadyTerminal`；取消屏障已建立返回 `Suppressed`。既有行的合同字段或效果身份
    /// 与本次命令不一致（跨 workflow/version 冒用 gate）、事务缺失或底层失败返回错误。
    pub async fn admit_execute(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
    ) -> Result<ExecuteAdmission, SagaStoreError> {
        // gate 与业务写、结果 Outbox 必须同一事务:准入提交而业务效果丢失(或反之)
        // 都会让 Orchestrator 记到与本地事实相反的账。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        // FOR UPDATE 行锁是 execute 与 cancel 的唯一串行化点;无行时锁间隙,
        // 与并发 INSERT 由唯一键仲裁。
        let existing = sqlx::query(
            "SELECT tenant_id, workflow_name, definition_version, definition_digest, \
             forward_status, execute_effect_id \
             FROM saga_participant_step WHERE saga_id = ? AND step_name = ? FOR UPDATE",
        )
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;

        let Some(row) = existing else {
            let inserted = sqlx::query(
                "INSERT INTO saga_participant_step (saga_id, step_name, tenant_id, \
                 workflow_name, definition_version, definition_digest, forward_status, cancel_status, \
                 compensation_status, resolution_status, execute_effect_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .bind(gate.tenant.as_str())
            .bind(gate.workflow.as_str())
            .bind(gate.definition_version.get())
            .bind(gate.definition_digest)
            .bind(StepForwardStatus::Pending.as_str())
            .bind(StepCancelStatus::None.as_str())
            .bind(StepCompensationStatus::None.as_str())
            .bind(StepResolutionStatus::None.as_str())
            .bind(execute_effect.to_string())
            .execute(connection.as_mut())
            .await;
            return match inserted {
                Ok(_) => Ok(ExecuteAdmission::Admitted),
                // 间隙锁下仍撞唯一键 = 同一效果的并发准入,让本事务失败重投,
                // 由先提交者的终态在重投时给出幂等答案。
                Err(error) if is_unique_violation(&error) => Err(SagaStoreError::new(
                    "concurrent gate admission lost the race; redeliver to adjudicate",
                )),
                Err(error) => Err(map_database(error)),
            };
        };

        verify_gate_contract(&row, gate)?;
        let stored_effect: String = row.try_get("execute_effect_id").map_err(map_database)?;
        // 同一 (saga, step) 出现第二个效果身份说明命令伪造或派生漂移;
        // 继续执行会把别的效果的终态当成自己的幂等答案。
        if stored_effect != execute_effect.to_string() {
            return Err(SagaStoreError::new(
                "gate effect identity mismatch; command is not for this step effect",
            ));
        }
        let forward: String = row.try_get("forward_status").map_err(map_database)?;
        let forward = StepForwardStatus::parse(&forward)
            .ok_or_else(|| crate::error::corrupt("forward_status"))?;
        Ok(match forward {
            StepForwardStatus::Cancelled => ExecuteAdmission::Suppressed,
            // gate 行与业务写同事务提交,已提交行不可能停留在 PENDING;
            // 出现即说明协议被绕过(gate 外提交),按损坏处理。
            StepForwardStatus::Pending => {
                return Err(SagaStoreError::new(
                    "gate row committed in PENDING state; protocol was bypassed",
                ))
            }
            settled => ExecuteAdmission::AlreadyTerminal(settled),
        })
    }

    /// 业务作用：在业务执行完成后，把 execute 裁决落到 gate 行。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `status`: 业务裁决（SUCCEEDED/REJECTED/UNKNOWN/HALTED，不允许 PENDING/CANCELLED）。
    ///
    /// 返回：落账成功返回 `Ok`；gate 行不在 PENDING（未先准入或并发破坏）、
    /// 状态非法、事务缺失或底层失败返回错误。
    pub async fn settle_execute(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepForwardStatus,
    ) -> Result<(), SagaStoreError> {
        if !matches!(
            status,
            StepForwardStatus::Succeeded
                | StepForwardStatus::Rejected
                | StepForwardStatus::Unknown
                | StepForwardStatus::Halted
        ) {
            return Err(SagaStoreError::new(
                "execute settlement requires a business adjudication status",
            ));
        }
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_participant_step SET forward_status = ? \
             WHERE saga_id = ? AND step_name = ? AND forward_status = ?",
        )
        .bind(status.as_str())
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(StepForwardStatus::Pending.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Err(SagaStoreError::new(
                "execute settlement found no admitted gate row",
            ));
        }
        Ok(())
    }

    /// 业务作用：在与 execute 相同的串行化点上裁决取消，实现取消屏障的参与方半边。
    ///
    /// 参数说明：
    /// - `gate`: 步骤 gate 身份与合同字段。
    /// - `execute_effect`: 该步骤 execute 阶段的效果身份（参与方本地派生）。
    /// - `cancel_effect`: cancel 阶段的效果身份，记入 gate 行供审计关联。
    ///
    /// 返回：执行未开始时建立 admission fence 并返回 [`CancelAdjudication::Confirmed`]
    /// （此后到达的 execute 被本地状态拒绝，零正向效果）；已有确定终态返回
    /// `AlreadyTerminal` 携带真实终态；结果未知返回 `ResolutionPending`，绝不谎报取消
    /// 成功。合同字段不一致、事务缺失或底层失败返回错误。
    pub async fn adjudicate_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<CancelAdjudication, SagaStoreError> {
        // 取消裁决与 CancelConfirmed Outbox 必须同事务:fence 建立而取消事实丢失,
        // Orchestrator 将永远等不到裁决;反之则谎报了并未建立的屏障。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let existing = sqlx::query(
            "SELECT tenant_id, workflow_name, definition_version, definition_digest, \
             forward_status, cancel_status, execute_effect_id, cancel_effect_id \
             FROM saga_participant_step WHERE saga_id = ? AND step_name = ? FOR UPDATE",
        )
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;

        let Some(row) = existing else {
            // execute 尚未到达:建立 admission fence,后到的 execute 将命中 CANCELLED 被拒。
            let inserted = sqlx::query(
                "INSERT INTO saga_participant_step (saga_id, step_name, tenant_id, \
                 workflow_name, definition_version, definition_digest, forward_status, cancel_status, \
                 compensation_status, resolution_status, execute_effect_id, cancel_effect_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .bind(gate.tenant.as_str())
            .bind(gate.workflow.as_str())
            .bind(gate.definition_version.get())
            .bind(gate.definition_digest)
            .bind(StepForwardStatus::Cancelled.as_str())
            .bind(StepCancelStatus::Confirmed.as_str())
            .bind(StepCompensationStatus::None.as_str())
            .bind(StepResolutionStatus::None.as_str())
            .bind(execute_effect.to_string())
            .bind(cancel_effect.to_string())
            .execute(connection.as_mut())
            .await;
            return match inserted {
                Ok(_) => Ok(CancelAdjudication::Confirmed),
                Err(error) if is_unique_violation(&error) => Err(SagaStoreError::new(
                    "concurrent gate admission lost the race; redeliver to adjudicate",
                )),
                Err(error) => Err(map_database(error)),
            };
        };

        verify_gate_contract(&row, gate)?;
        let stored_execute_effect: String =
            row.try_get("execute_effect_id").map_err(map_database)?;
        if stored_execute_effect != execute_effect.to_string() {
            return Err(SagaStoreError::new(
                "gate execute effect identity mismatch during cancel adjudication",
            ));
        }
        let forward: String = row.try_get("forward_status").map_err(map_database)?;
        let forward = StepForwardStatus::parse(&forward)
            .ok_or_else(|| crate::error::corrupt("forward_status"))?;
        let adjudication = match forward {
            // 重复取消命令按已建立的 fence 幂等确认。
            StepForwardStatus::Cancelled => CancelAdjudication::Confirmed,
            StepForwardStatus::Succeeded
            | StepForwardStatus::Rejected
            | StepForwardStatus::Halted => CancelAdjudication::AlreadyTerminal(forward),
            // 外部 intent 已提交/调用在途/结果未知:不谎报取消成功,交解决通道裁决。
            StepForwardStatus::Unknown
            | StepForwardStatus::Pending
            | StepForwardStatus::TimedOut => CancelAdjudication::ResolutionPending,
        };
        let cancel_status = match adjudication {
            CancelAdjudication::Confirmed => StepCancelStatus::Confirmed,
            CancelAdjudication::AlreadyTerminal(_) => StepCancelStatus::AlreadyTerminal,
            CancelAdjudication::ResolutionPending => StepCancelStatus::ResolutionPending,
        };
        let current_cancel: String = row.try_get("cancel_status").map_err(map_database)?;
        let current_cancel = StepCancelStatus::parse(&current_cancel)
            .ok_or_else(|| crate::error::corrupt("cancel_status"))?;
        let stored_cancel_effect: Option<String> =
            row.try_get("cancel_effect_id").map_err(map_database)?;
        if current_cancel != StepCancelStatus::None {
            // 既有取消裁决只允许同一稳定 effect 幂等重放；不同裁决或身份说明
            // 滚动发布/派生已漂移，必须 fail-closed 而不是最后写入者覆盖屏障。
            if current_cancel != cancel_status
                || stored_cancel_effect.as_deref() != Some(&cancel_effect.to_string())
            {
                return Err(SagaStoreError::new(
                    "cancel adjudication conflicts with the persisted gate fact",
                ));
            }
            return Ok(adjudication);
        }
        let updated = sqlx::query(
            "UPDATE saga_participant_step SET cancel_status = ?, cancel_effect_id = ? \
             WHERE saga_id = ? AND step_name = ? AND cancel_status = ?",
        )
        .bind(cancel_status.as_str())
        .bind(cancel_effect.to_string())
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .bind(StepCancelStatus::None.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() != 1 {
            return Err(SagaStoreError::new(
                "cancel adjudication lost gate authority",
            ));
        }
        Ok(adjudication)
    }

    /// 业务作用：为 externally-cancellable 步骤取得与 execute 共用的 gate 串行化权威。
    ///
    /// 与本地取消不同，本方法绝不自行伪造 `CancelConfirmed`：没有正向行时只插入事务内
    /// `PENDING` 占位并保持行锁，必须等待类型化业务 handler 的真实裁决再落账。已有取消
    /// 事实只重放，避免相同外部取消 effect 被再次调用。
    ///
    /// 参数说明：
    /// - `gate`: 步骤身份与合同字段。
    /// - `execute_effect`: 本地派生的 execute 效果身份。
    /// - `cancel_effect`: 本地派生的 cancel 效果身份。
    ///
    /// 返回：需要真实调用时返回 `Admitted`；已有裁决返回可重放事实；身份漂移、损坏、
    /// 缺少事务或数据库失败返回错误并要求整个事务回滚。
    pub async fn admit_external_cancel(
        &self,
        gate: &ParticipantGateKey<'_>,
        execute_effect: &EffectId,
        cancel_effect: &EffectId,
    ) -> Result<ExternalCancelAdmission, SagaStoreError> {
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        // gate 行锁在外部 cancel 调用期间保持到 COMMIT；否则并发 execute 可能在取消尚未
        // 裁决时提交外部 intent，最终同时出现 CancelConfirmed 与真实成功效果。
        let existing = sqlx::query(
            "SELECT tenant_id, workflow_name, definition_version, definition_digest, \
             forward_status, cancel_status, \
             execute_effect_id, cancel_effect_id FROM saga_participant_step \
             WHERE saga_id = ? AND step_name = ? FOR UPDATE",
        )
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;

        let Some(row) = existing else {
            // PENDING 只存在于当前未提交事务中，用来占住 execute/cancel 的唯一串行化点；
            // handler 返回错误会连同行一起回滚，成功则必须在提交前改成真实裁决。
            let inserted = sqlx::query(
                "INSERT INTO saga_participant_step (saga_id, step_name, tenant_id, \
                 workflow_name, definition_version, definition_digest, forward_status, cancel_status, \
                 compensation_status, resolution_status, execute_effect_id, cancel_effect_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .bind(gate.tenant.as_str())
            .bind(gate.workflow.as_str())
            .bind(gate.definition_version.get())
            .bind(gate.definition_digest)
            .bind(StepForwardStatus::Pending.as_str())
            .bind(StepCancelStatus::None.as_str())
            .bind(StepCompensationStatus::None.as_str())
            .bind(StepResolutionStatus::None.as_str())
            .bind(execute_effect.to_string())
            .bind(cancel_effect.to_string())
            .execute(connection.as_mut())
            .await;
            return match inserted {
                Ok(_) => Ok(ExternalCancelAdmission::Admitted),
                Err(error) if is_unique_violation(&error) => Err(SagaStoreError::new(
                    "concurrent external cancel admission lost the race; redeliver to adjudicate",
                )),
                Err(error) => Err(map_database(error)),
            };
        };

        verify_gate_contract(&row, gate)?;
        let stored_execute: String = row.try_get("execute_effect_id").map_err(map_database)?;
        if stored_execute != execute_effect.to_string() {
            return Err(SagaStoreError::new(
                "gate execute effect identity mismatch during external cancel",
            ));
        }
        let forward = row
            .try_get::<String, _>("forward_status")
            .map_err(map_database)?;
        let forward = StepForwardStatus::parse(&forward)
            .ok_or_else(|| crate::error::corrupt("forward_status"))?;
        let cancel = row
            .try_get::<String, _>("cancel_status")
            .map_err(map_database)?;
        let cancel = StepCancelStatus::parse(&cancel)
            .ok_or_else(|| crate::error::corrupt("cancel_status"))?;
        let stored_cancel: Option<String> =
            row.try_get("cancel_effect_id").map_err(map_database)?;
        if let Some(stored_cancel) = stored_cancel.as_deref() {
            if stored_cancel != cancel_effect.to_string() {
                return Err(SagaStoreError::new(
                    "gate cancel effect identity mismatch during external cancel",
                ));
            }
        }

        let existing = match cancel {
            StepCancelStatus::Confirmed => Some(CancelAdjudication::Confirmed),
            StepCancelStatus::AlreadyTerminal => {
                if matches!(
                    forward,
                    StepForwardStatus::Succeeded
                        | StepForwardStatus::Rejected
                        | StepForwardStatus::Halted
                ) {
                    Some(CancelAdjudication::AlreadyTerminal(forward))
                } else {
                    return Err(SagaStoreError::new(
                        "external cancel terminal adjudication has no terminal forward fact",
                    ));
                }
            }
            StepCancelStatus::ResolutionPending => Some(CancelAdjudication::ResolutionPending),
            StepCancelStatus::None => None,
        };
        if let Some(existing) = existing {
            return Ok(ExternalCancelAdmission::AlreadyAdjudicated(existing));
        }

        match forward {
            StepForwardStatus::Cancelled => {
                drop(connection);
                self.persist_existing_cancel_adjudication(
                    gate.saga_id,
                    gate.step,
                    cancel_effect,
                    StepCancelStatus::Confirmed,
                )
                .await?;
                Ok(ExternalCancelAdmission::AlreadyAdjudicated(
                    CancelAdjudication::Confirmed,
                ))
            }
            StepForwardStatus::Succeeded
            | StepForwardStatus::Rejected
            | StepForwardStatus::Halted => {
                drop(connection);
                self.persist_existing_cancel_adjudication(
                    gate.saga_id,
                    gate.step,
                    cancel_effect,
                    StepCancelStatus::AlreadyTerminal,
                )
                .await?;
                Ok(ExternalCancelAdmission::AlreadyAdjudicated(
                    CancelAdjudication::AlreadyTerminal(forward),
                ))
            }
            StepForwardStatus::Unknown | StepForwardStatus::TimedOut => {
                sqlx::query(
                    "UPDATE saga_participant_step SET cancel_effect_id = ? \
                     WHERE saga_id = ? AND step_name = ? AND cancel_status = ?",
                )
                .bind(cancel_effect.to_string())
                .bind(gate.saga_id.as_str())
                .bind(gate.step.as_str())
                .bind(StepCancelStatus::None.as_str())
                .execute(connection.as_mut())
                .await
                .map_err(map_database)?;
                Ok(ExternalCancelAdmission::Admitted)
            }
            // 已提交 PENDING 说明旧调用方取得准入后绕过 settle 仍然提交；继续调用外部
            // cancel 会建立在不完整事实之上，必须 fail-closed 等人工修复。
            StepForwardStatus::Pending => Err(SagaStoreError::new(
                "external cancel gate row was committed in PENDING state",
            )),
        }
    }

    /// 业务作用：把 externally-cancellable handler 的真实裁决落到仍持锁的 gate 行。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤身份。
    /// - `forward`: 裁决后的正向事实（CANCELLED、终态或 UNKNOWN）。
    /// - `cancel`: 对应取消状态（CONFIRMED、ALREADY_TERMINAL 或 RESOLUTION_PENDING）。
    ///
    /// 返回：组合合法且仍持有未裁决 gate 时更新成功；竞态、组合非法、事务缺失或数据库
    /// 失败返回错误，使业务取消调用与结果 Outbox 全部回滚。
    pub async fn settle_external_cancel(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        forward: StepForwardStatus,
        cancel: StepCancelStatus,
    ) -> Result<(), SagaStoreError> {
        let valid = matches!(
            (forward, cancel),
            (StepForwardStatus::Cancelled, StepCancelStatus::Confirmed)
                | (
                    StepForwardStatus::Succeeded
                        | StepForwardStatus::Rejected
                        | StepForwardStatus::Halted,
                    StepCancelStatus::AlreadyTerminal
                )
                | (
                    StepForwardStatus::Unknown,
                    StepCancelStatus::ResolutionPending
                )
        );
        if !valid {
            return Err(SagaStoreError::new(
                "external cancel settlement has an invalid forward/cancel combination",
            ));
        }
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_participant_step SET forward_status = ?, cancel_status = ? \
             WHERE saga_id = ? AND step_name = ? AND cancel_status = ? \
             AND forward_status IN (?, ?, ?)",
        )
        .bind(forward.as_str())
        .bind(cancel.as_str())
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(StepCancelStatus::None.as_str())
        .bind(StepForwardStatus::Pending.as_str())
        .bind(StepForwardStatus::Unknown.as_str())
        .bind(StepForwardStatus::TimedOut.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() != 1 {
            return Err(SagaStoreError::new(
                "external cancel settlement lost gate authority",
            ));
        }
        Ok(())
    }

    /// 业务作用：为已有正向终态补记取消效果与裁决，不调用任何外部业务。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤身份。
    /// - `cancel_effect`: 当前取消效果身份。
    /// - `status`: 由既有正向事实确定的取消状态。
    ///
    /// 返回：单行更新成功返回 `Ok`；gate 丢失或数据库失败返回错误。
    async fn persist_existing_cancel_adjudication(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        cancel_effect: &EffectId,
        status: StepCancelStatus,
    ) -> Result<(), SagaStoreError> {
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_participant_step SET cancel_status = ?, cancel_effect_id = ? \
             WHERE saga_id = ? AND step_name = ? AND cancel_status = ?",
        )
        .bind(status.as_str())
        .bind(cancel_effect.to_string())
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(StepCancelStatus::None.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() != 1 {
            return Err(SagaStoreError::new(
                "existing external cancel adjudication lost gate authority",
            ));
        }
        Ok(())
    }

    /// 业务作用：在补偿业务写之前竞争补偿准入，保证补偿只作用于真实存在的正向效果。
    ///
    /// 参数说明：
    /// - `gate`: 步骤 gate 身份与合同字段。
    /// - `compensate_effect`: compensate 阶段的效果身份，记入 gate 行。
    /// - `recovery_operation_id`: 已认证 Orchestrator 签发的人工恢复操作；普通命令为空。
    ///
    /// 返回：正向成功且补偿从未开始返回 [`CompensationAdmission::Admitted`]（调用方在同一
    /// 事务内执行补偿并 [`MySqlSagaStore::settle_compensation`]）；补偿已有
    /// `Succeeded/Unknown/Halted` 事实默认返回 `AlreadySettled` 幂等重放；只有携带已认证
    /// 管理恢复身份且正向事实为 `Succeeded` 的 `Halted` 才重新准入，`Unknown` 永不直接重做。
    /// 本地无正向成功效果且无补偿证据返回
    /// `MissingForwardEffect`（协议破坏，调用方必须提交 `Halted` 合同违规事实并告警）。
    pub async fn admit_compensation(
        &self,
        gate: &ParticipantGateKey<'_>,
        compensate_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<CompensationAdmission, SagaStoreError> {
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let existing = sqlx::query(
            "SELECT tenant_id, workflow_name, definition_version, definition_digest, \
             forward_status, compensation_status \
             FROM saga_participant_step WHERE saga_id = ? AND step_name = ? FOR UPDATE",
        )
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;

        let Some(row) = existing else {
            // 补偿只会对已记账成功的步骤发出；本地连 gate 行都没有说明协议被破坏。
            // 仍需把 HALTED gate 与结果 Outbox 同事务持久化，否则重投永远看不到已冻结证据。
            let execute_effect = EffectId::derive(
                gate.saga_id,
                gate.definition_version,
                gate.step,
                StepPhase::Execute,
            );
            sqlx::query(
                "INSERT INTO saga_participant_step (saga_id, step_name, tenant_id, \
                 workflow_name, definition_version, definition_digest, forward_status, cancel_status, \
                 compensation_status, resolution_status, execute_effect_id, compensate_effect_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .bind(gate.tenant.as_str())
            .bind(gate.workflow.as_str())
            .bind(gate.definition_version.get())
            .bind(gate.definition_digest)
            .bind(StepForwardStatus::Halted.as_str())
            .bind(StepCancelStatus::None.as_str())
            .bind(StepCompensationStatus::Halted.as_str())
            .bind(StepResolutionStatus::None.as_str())
            .bind(execute_effect.to_string())
            .bind(compensate_effect.to_string())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
            return Ok(CompensationAdmission::MissingForwardEffect);
        };
        verify_gate_contract(&row, gate)?;
        let forward: String = row.try_get("forward_status").map_err(map_database)?;
        let forward = StepForwardStatus::parse(&forward)
            .ok_or_else(|| crate::error::corrupt("forward_status"))?;
        let compensation: String = row.try_get("compensation_status").map_err(map_database)?;
        let compensation = StepCompensationStatus::parse(&compensation)
            .ok_or_else(|| crate::error::corrupt("compensation_status"))?;

        if matches!(
            compensation,
            StepCompensationStatus::Succeeded | StepCompensationStatus::Unknown
        ) {
            // UNKNOWN 可能代表退款已发生但响应丢失；再次进入 handler 会产生二次退款。
            return Ok(CompensationAdmission::AlreadySettled(compensation));
        }
        if compensation == StepCompensationStatus::Halted {
            // 只有管理面已审计、并由可信 Orchestrator route 携带的恢复身份才能解除 HALTED；
            // 普通 timeout/new attempt 即使 effect 相同也无权让外部退款再次执行。
            if recovery_operation_id.is_none() || forward != StepForwardStatus::Succeeded {
                return Ok(CompensationAdmission::AlreadySettled(compensation));
            }
        }
        if compensation == StepCompensationStatus::Pending {
            // gate、业务事实和 settlement 同事务提交，正常已提交行不可能停在 PENDING；
            // 看到它说明有人绕过 wrapper，不能冒险重做补偿。
            return Err(SagaStoreError::new(
                "compensation gate committed in PENDING state; protocol was bypassed",
            ));
        }
        // 只有正向 SUCCEEDED 才证明存在可撤销效果；补偿状态本身不能反向证明效果存在，
        // 否则一条伪造补偿命令先写 HALTED 后，下一 attempt 就会被错误准入业务撤销。
        if forward != StepForwardStatus::Succeeded {
            // 既有 gate 没有成功效果却收到补偿命令属于合同破坏；把冻结事实写回 gate，
            // 让重复投递稳定命中同一人工介入结论，而不是每次重新“发现”缺失效果。
            sqlx::query(
                "UPDATE saga_participant_step SET compensation_status = ?, compensate_effect_id = ? \
                 WHERE saga_id = ? AND step_name = ?",
            )
            .bind(StepCompensationStatus::Halted.as_str())
            .bind(compensate_effect.to_string())
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
            return Ok(CompensationAdmission::MissingForwardEffect);
        }
        sqlx::query(
            "UPDATE saga_participant_step SET compensation_status = ?, compensate_effect_id = ?, \
             resolution_status = ? \
             WHERE saga_id = ? AND step_name = ?",
        )
        .bind(StepCompensationStatus::Pending.as_str())
        .bind(compensate_effect.to_string())
        // resolve=REJECTED 表示上一轮补偿确定未发生；新一轮补偿开始时清除旧裁决，
        // 否则后续补偿再次 Unknown 会错误重放上一轮 resolution 终态。
        .bind(StepResolutionStatus::None.as_str())
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(CompensationAdmission::Admitted)
    }

    /// 业务作用：在补偿业务完成后，把补偿裁决落到 gate 行。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `status`: 补偿裁决（SUCCEEDED/UNKNOWN/HALTED；补偿没有 Rejected 语义）。
    ///
    /// 返回：落账成功返回 `Ok`；gate 行不在 PENDING、状态非法、事务缺失或底层失败返回错误。
    pub async fn settle_compensation(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        status: StepCompensationStatus,
    ) -> Result<(), SagaStoreError> {
        if !matches!(
            status,
            StepCompensationStatus::Succeeded
                | StepCompensationStatus::Unknown
                | StepCompensationStatus::Halted
        ) {
            return Err(SagaStoreError::new(
                "compensation settlement requires a business adjudication status",
            ));
        }
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_participant_step SET compensation_status = ? \
             WHERE saga_id = ? AND step_name = ? AND compensation_status = ?",
        )
        .bind(status.as_str())
        .bind(saga_id.as_str())
        .bind(step.as_str())
        .bind(StepCompensationStatus::Pending.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Err(SagaStoreError::new(
                "compensation settlement found no admitted gate row",
            ));
        }
        Ok(())
    }

    /// 业务作用：在 resolve handler 前锁定参与方 gate，确认查询方向并阻止终态被重复裁决。
    ///
    /// 新 attempt 在既有 `PENDING` 上允许再次查询，因为 `Unknown` 必须在有界预算内持续收敛；
    /// 一旦已有 `SUCCEEDED/REJECTED/HALTED`，普通新 attempt 只能重放原事实；`HALTED`
    /// 仅在携带已认证管理恢复身份且 Unknown 目标仍存在时重新进入查询接口。
    ///
    /// 参数说明：
    /// - `gate`: 步骤 gate 身份与合同字段。
    /// - `resolve_effect`: resolve 阶段跨 attempt 稳定的效果身份。
    /// - `recovery_operation_id`: 已认证 Orchestrator 签发的人工恢复操作；普通命令为空。
    ///
    /// 返回：存在未知正向/补偿效果时返回准入方向；已有终态返回稳定重放；没有未知效果时
    /// 返回 `MissingUnknownEffect` 并持久化冻结证据。事务缺失、身份漂移或数据损坏返回错误。
    pub async fn admit_resolution(
        &self,
        gate: &ParticipantGateKey<'_>,
        resolve_effect: &EffectId,
        recovery_operation_id: Option<&str>,
    ) -> Result<ResolutionAdmission, SagaStoreError> {
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let existing = sqlx::query(
            "SELECT tenant_id, workflow_name, definition_version, definition_digest, forward_status, \
             compensation_status, resolution_status, resolve_effect_id \
             FROM saga_participant_step WHERE saga_id = ? AND step_name = ? FOR UPDATE",
        )
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .fetch_optional(connection.as_mut())
        .await
        .map_err(map_database)?;

        let Some(row) = existing else {
            // 没有 execute/compensate gate 却收到 resolve 说明命令链断裂。冻结本地证据后返回
            // HALTED，避免毒消息永久重投或业务 handler凭空查询另一笔效果。
            let execute_effect = EffectId::derive(
                gate.saga_id,
                gate.definition_version,
                gate.step,
                StepPhase::Execute,
            );
            sqlx::query(
                "INSERT INTO saga_participant_step (saga_id, step_name, tenant_id, \
                 workflow_name, definition_version, definition_digest, forward_status, cancel_status, \
                 compensation_status, resolution_status, execute_effect_id, resolve_effect_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .bind(gate.tenant.as_str())
            .bind(gate.workflow.as_str())
            .bind(gate.definition_version.get())
            .bind(gate.definition_digest)
            .bind(StepForwardStatus::Halted.as_str())
            .bind(StepCancelStatus::None.as_str())
            .bind(StepCompensationStatus::None.as_str())
            .bind(StepResolutionStatus::Halted.as_str())
            .bind(execute_effect.to_string())
            .bind(resolve_effect.to_string())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
            return Ok(ResolutionAdmission::MissingUnknownEffect);
        };

        verify_gate_contract(&row, gate)?;
        let stored_effect: Option<String> =
            row.try_get("resolve_effect_id").map_err(map_database)?;
        if stored_effect
            .as_deref()
            .is_some_and(|stored| stored != resolve_effect.to_string())
        {
            return Err(SagaStoreError::new(
                "resolution effect identity mismatch; command is not for this step effect",
            ));
        }
        let forward = parse_forward_status(&row)?;
        let compensation = parse_compensation_status(&row)?;
        let resolution = parse_resolution_status(&row)?;

        if matches!(
            resolution,
            StepResolutionStatus::Succeeded | StepResolutionStatus::Rejected
        ) || (resolution == StepResolutionStatus::Halted && recovery_operation_id.is_none())
        {
            let target = resolved_target(forward, compensation, resolution);
            return Ok(ResolutionAdmission::AlreadySettled {
                target,
                status: resolution,
            });
        }

        let target = if compensation == StepCompensationStatus::Unknown {
            ResolutionTarget::Compensation
        } else if forward == StepForwardStatus::Unknown {
            ResolutionTarget::Forward
        } else {
            // 只有 UNKNOWN 业务事实能进入解决通道；TIMED_OUT 或普通 PENDING 不能由参与方
            // 自行猜测为哪种外部效果，否则可能查询/撤销并未提交的请求。
            sqlx::query(
                "UPDATE saga_participant_step SET resolution_status = ?, resolve_effect_id = ? \
                 WHERE saga_id = ? AND step_name = ?",
            )
            .bind(StepResolutionStatus::Halted.as_str())
            .bind(resolve_effect.to_string())
            .bind(gate.saga_id.as_str())
            .bind(gate.step.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
            return Ok(ResolutionAdmission::MissingUnknownEffect);
        };

        // 人工恢复携带受信 operation id 时才允许从 HALTED 回到 PENDING；未知业务事实
        // 始终保留，查询未形成确定裁决前绝不能发正向或补偿命令。
        // PENDING 与 resolve effect 先写入同一事务；handler 若 Retryable，整个事务回滚，
        // Inbox claim 也不会留下，transport 才能安全重投本次 command。
        sqlx::query(
            "UPDATE saga_participant_step SET resolution_status = ?, resolve_effect_id = ? \
             WHERE saga_id = ? AND step_name = ?",
        )
        .bind(StepResolutionStatus::Pending.as_str())
        .bind(resolve_effect.to_string())
        .bind(gate.saga_id.as_str())
        .bind(gate.step.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(ResolutionAdmission::Admitted(target))
    }

    /// 业务作用：把类型化 resolve handler 的裁决写回正确方向，并保持未知事实的单调性。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `step`: 步骤名称。
    /// - `target`: 本次查询裁决的正向或补偿方向。
    /// - `status`: `PENDING/SUCCEEDED/REJECTED/HALTED`；`PENDING` 表示仍为 Unknown。
    ///
    /// 返回：方向与未知事实匹配且落账成功返回 `Ok`；越权改写、非法状态或事务失败返回错误。
    pub async fn settle_resolution(
        &self,
        saga_id: &SagaId,
        step: &StepName,
        target: ResolutionTarget,
        status: StepResolutionStatus,
    ) -> Result<(), SagaStoreError> {
        if status == StepResolutionStatus::None {
            return Err(SagaStoreError::new(
                "resolution settlement requires a query adjudication status",
            ));
        }
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let (forward, compensation) = match (target, status) {
            (ResolutionTarget::Forward, StepResolutionStatus::Succeeded) => {
                (Some(StepForwardStatus::Succeeded), None)
            }
            (ResolutionTarget::Forward, StepResolutionStatus::Rejected) => {
                (Some(StepForwardStatus::Rejected), None)
            }
            (ResolutionTarget::Compensation, StepResolutionStatus::Succeeded) => {
                (None, Some(StepCompensationStatus::Succeeded))
            }
            // 补偿确定未发生后必须重新开放补偿准入；不能保留 UNKNOWN，否则新 attempt
            // 会被当成“可能已退款”而只重放，永远无法按 Orchestrator 裁决重试。
            (ResolutionTarget::Compensation, StepResolutionStatus::Rejected) => {
                (None, Some(StepCompensationStatus::None))
            }
            (_, StepResolutionStatus::Pending | StepResolutionStatus::Halted) => (None, None),
            (_, StepResolutionStatus::None) => unreachable!("NONE 已在入口拒绝"),
        };
        let updated = match target {
            ResolutionTarget::Forward => {
                sqlx::query(
                    "UPDATE saga_participant_step SET resolution_status = ?, \
                     forward_status = COALESCE(?, forward_status) WHERE saga_id = ? AND \
                     step_name = ? AND forward_status = ? AND resolution_status = ?",
                )
                .bind(status.as_str())
                .bind(forward.map(StepForwardStatus::as_str))
                .bind(saga_id.as_str())
                .bind(step.as_str())
                .bind(StepForwardStatus::Unknown.as_str())
                .bind(StepResolutionStatus::Pending.as_str())
                .execute(connection.as_mut())
                .await
            }
            ResolutionTarget::Compensation => {
                sqlx::query(
                    "UPDATE saga_participant_step SET resolution_status = ?, \
                     compensation_status = COALESCE(?, compensation_status) WHERE saga_id = ? AND \
                     step_name = ? AND compensation_status = ? AND resolution_status = ?",
                )
                .bind(status.as_str())
                .bind(compensation.map(StepCompensationStatus::as_str))
                .bind(saga_id.as_str())
                .bind(step.as_str())
                .bind(StepCompensationStatus::Unknown.as_str())
                .bind(StepResolutionStatus::Pending.as_str())
                .execute(connection.as_mut())
                .await
            }
        }
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Err(SagaStoreError::new(
                "resolution settlement found no matching unknown gate fact",
            ));
        }
        if target == ResolutionTarget::Forward
            && matches!(
                status,
                StepResolutionStatus::Succeeded | StepResolutionStatus::Rejected
            )
        {
            // externally-cancellable 的旧取消裁决可能仍是 RESOLUTION_PENDING；Poll 已给出
            // 确定正向终态后必须同事务提升为 AlreadyTerminal，否则新 cancel attempt 会
            // 重放过期“仍未知”事实，破坏 gate 单调性。resolve-only 的 NONE 不受影响。
            sqlx::query(
                "UPDATE saga_participant_step SET cancel_status = ? WHERE saga_id = ? AND \
                 step_name = ? AND cancel_status = ?",
            )
            .bind(StepCancelStatus::AlreadyTerminal.as_str())
            .bind(saga_id.as_str())
            .bind(step.as_str())
            .bind(StepCancelStatus::ResolutionPending.as_str())
            .execute(connection.as_mut())
            .await
            .map_err(map_database)?;
        }
        Ok(())
    }
}

/// 业务作用：从 gate 行严格解析正向状态，损坏文本不得进入解决方向推断。
///
/// 参数说明：
/// - `row`: 已持有行锁的参与方 gate 记录。
///
/// 返回：合法正向状态；列读取失败或状态文本损坏时返回脱敏错误。
fn parse_forward_status(row: &sqlx::mysql::MySqlRow) -> Result<StepForwardStatus, SagaStoreError> {
    let value: String = row.try_get("forward_status").map_err(map_database)?;
    StepForwardStatus::parse(&value).ok_or_else(|| crate::error::corrupt("forward_status"))
}

/// 业务作用：从 gate 行严格解析补偿状态，防止错误方向被静默当成无补偿。
///
/// 参数说明：
/// - `row`: 已持有行锁的参与方 gate 记录。
///
/// 返回：合法补偿状态；列读取失败或状态文本损坏时返回脱敏错误。
fn parse_compensation_status(
    row: &sqlx::mysql::MySqlRow,
) -> Result<StepCompensationStatus, SagaStoreError> {
    let value: String = row.try_get("compensation_status").map_err(map_database)?;
    StepCompensationStatus::parse(&value)
        .ok_or_else(|| crate::error::corrupt("compensation_status"))
}

/// 业务作用：从 gate 行严格解析解决状态，未知持久化值必须冻结而不是重新查询。
///
/// 参数说明：
/// - `row`: 已持有行锁的参与方 gate 记录。
///
/// 返回：合法解决状态；列读取失败或状态文本损坏时返回脱敏错误。
fn parse_resolution_status(
    row: &sqlx::mysql::MySqlRow,
) -> Result<StepResolutionStatus, SagaStoreError> {
    let value: String = row.try_get("resolution_status").map_err(map_database)?;
    StepResolutionStatus::parse(&value).ok_or_else(|| crate::error::corrupt("resolution_status"))
}

/// 业务作用：从已提交解决终态及业务投影恢复原裁决方向，供新 attempt 安全重放。
///
/// 参数说明：
/// - `forward`: gate 的正向投影。
/// - `compensation`: gate 的补偿投影。
/// - `resolution`: 已提交的解决终态。
///
/// 返回：与既有终态一致的正向或补偿裁决方向。
fn resolved_target(
    forward: StepForwardStatus,
    compensation: StepCompensationStatus,
    resolution: StepResolutionStatus,
) -> ResolutionTarget {
    match resolution {
        StepResolutionStatus::Succeeded if compensation == StepCompensationStatus::Succeeded => {
            ResolutionTarget::Compensation
        }
        StepResolutionStatus::Rejected if forward != StepForwardStatus::Rejected => {
            ResolutionTarget::Compensation
        }
        StepResolutionStatus::Halted if compensation == StepCompensationStatus::Unknown => {
            ResolutionTarget::Compensation
        }
        _ => ResolutionTarget::Forward,
    }
}

/// 业务作用：校验既有 gate 行的合同字段与本次命令一致，拒绝跨 tenant/workflow/version/digest 冒用 gate。
///
/// 参数说明：
/// - `row`: 既有 gate 行（含 tenant/workflow/version/digest 合同列）。
/// - `gate`: 本次命令声明的 gate 身份。
///
/// 返回：一致返回 `Ok`；不一致返回稳定错误，调用方不得继续进入业务 handler。
fn verify_gate_contract(
    row: &sqlx::mysql::MySqlRow,
    gate: &ParticipantGateKey<'_>,
) -> Result<(), SagaStoreError> {
    let tenant: String = row.try_get("tenant_id").map_err(map_database)?;
    let workflow: String = row.try_get("workflow_name").map_err(map_database)?;
    let version: u32 = row.try_get("definition_version").map_err(map_database)?;
    let digest: Option<String> = row.try_get("definition_digest").map_err(map_database)?;
    if tenant != gate.tenant.as_str()
        || workflow != gate.workflow.as_str()
        || version != gate.definition_version.get()
        || digest.as_deref() != Some(gate.definition_digest)
    {
        return Err(SagaStoreError::new(
            "gate contract mismatch: command targets a different tenant, workflow, version, or digest",
        ));
    }
    Ok(())
}
