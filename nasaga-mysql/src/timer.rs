//! Durable timer store：调度、作废、租约领取与 fencing 完成。
//!
//! timer 的生死必须与触发它的状态迁移同事务：写下一步命令 Outbox 时同事务调度对应
//! timeout timer，迁移离开该步骤时同事务作废旧 timer。多副本可以竞争领取到期 timer，
//! 但**消费时必须复验 fencing token**（以及调用方侧的 `expected_saga_version` 与
//! `generation`），失去租约的旧 owner 即使迟到也不能推进新状态。

use nasaga_backend::{
    SagaTimerRow, TimerClaimBatch, TimerFencing, TimerFencingToken, TimerReschedule, TimerSchedule,
    TimerScope, TimerSpec, TimerState,
};
use nasaga_core::{AttemptNo, SagaId};
use sqlx::Row as _;

use crate::error::{is_unique_violation, map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::MySqlSagaStore;

impl MySqlSagaStore {
    /// 业务作用：在触发它的状态迁移事务内调度一个 durable timer。
    ///
    /// 参数说明：
    /// - `spec`: 调度输入。
    ///
    /// 返回：真实创建返回 [`TimerSchedule::Scheduled`]；同一 `(scope, kind, attempt)`
    /// 已存在**同 id** timer 返回 `AlreadyScheduled`（调度事务崩溃重试幂等）。同键但
    /// 不同 id 说明 timer 身份生成不稳定，返回错误；标识非法、事务缺失或底层失败返回错误。
    pub async fn schedule_timer(
        &self,
        spec: &TimerSpec<'_>,
    ) -> Result<TimerSchedule, SagaStoreError> {
        validate_timer_id(spec.timer_id)?;
        validate_kind(spec.kind)?;
        validate_epoch_ms(spec.due_at_ms, "timer due_at")?;
        validate_saga_version(spec.expected_saga_version)?;
        // timer 与写命令 Outbox 的迁移同事务:命令发出而超时保护缺席,步骤将可能无限滞留。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let inserted = sqlx::query(
            "INSERT INTO saga_timer (saga_id, scope_kind, scope_key, kind, attempt_no, \
             timer_id, due_at, available_at, state, expected_saga_version, generation) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        )
        .bind(spec.saga_id.as_str())
        .bind(spec.scope.kind_str())
        .bind(spec.scope.key_str())
        .bind(spec.kind)
        .bind(spec.attempt.get())
        .bind(spec.timer_id)
        .bind(spec.due_at_ms)
        .bind(spec.due_at_ms)
        .bind(TimerState::Pending.as_str())
        .bind(spec.expected_saga_version)
        .execute(connection.as_mut())
        .await;
        match inserted {
            Ok(_) => Ok(TimerSchedule::Scheduled),
            Err(error) if is_unique_violation(&error) => {
                let existing = sqlx::query(
                    "SELECT timer_id FROM saga_timer WHERE saga_id = ? AND scope_kind = ? \
                     AND scope_key = ? AND kind = ? AND attempt_no = ?",
                )
                .bind(spec.saga_id.as_str())
                .bind(spec.scope.kind_str())
                .bind(spec.scope.key_str())
                .bind(spec.kind)
                .bind(spec.attempt.get())
                .fetch_optional(connection.as_mut())
                .await
                .map_err(map_database)?;
                match existing {
                    Some(row) => {
                        let existing_id: String = row.try_get("timer_id").map_err(map_database)?;
                        if existing_id == spec.timer_id {
                            Ok(TimerSchedule::AlreadyScheduled)
                        } else {
                            // 同一逻辑 timer 出现两个身份说明 timer_id 生成不稳定,
                            // 继续会让 fencing 与审计无法对齐同一行。
                            Err(SagaStoreError::new(
                                "timer already scheduled with a different timer id",
                            ))
                        }
                    }
                    // 自然键无行却冲突 = timer_id 被其它作用域占用,身份复用是禁止的。
                    None => Err(SagaStoreError::new(
                        "timer id collides with a different timer scope",
                    )),
                }
            }
            Err(error) => Err(map_database(error)),
        }
    }

    /// 业务作用：在迁移离开某作用域的同一事务内作废其未消费 timer。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `scope`: 要作废的作用域。
    /// - `kind`: 只作废该种类；为空作废该作用域全部种类。
    ///
    /// 返回：被作废的 timer 行数；事务缺失或底层失败返回错误。
    pub async fn cancel_scope_timers(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: Option<&str>,
    ) -> Result<u64, SagaStoreError> {
        if let Some(kind) = kind {
            validate_kind(kind)?;
        }
        // 作废必须与离开该步骤的迁移同事务:旧 timer 存活到新状态会触发过期语义的超时。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let result = match kind {
            Some(kind) => {
                sqlx::query(
                    "UPDATE saga_timer SET state = ?, owner = NULL, fencing_token = NULL, \
                     claimed_until = NULL WHERE saga_id = ? AND scope_kind = ? AND scope_key = ? \
                     AND kind = ? AND state IN (?, ?)",
                )
                .bind(TimerState::Cancelled.as_str())
                .bind(saga_id.as_str())
                .bind(scope.kind_str())
                .bind(scope.key_str())
                .bind(kind)
                .bind(TimerState::Pending.as_str())
                .bind(TimerState::Claimed.as_str())
                .execute(connection.as_mut())
                .await
            }
            None => {
                sqlx::query(
                    "UPDATE saga_timer SET state = ?, owner = NULL, fencing_token = NULL, \
                     claimed_until = NULL WHERE saga_id = ? AND scope_kind = ? AND scope_key = ? \
                     AND state IN (?, ?)",
                )
                .bind(TimerState::Cancelled.as_str())
                .bind(saga_id.as_str())
                .bind(scope.kind_str())
                .bind(scope.key_str())
                .bind(TimerState::Pending.as_str())
                .bind(TimerState::Claimed.as_str())
                .execute(connection.as_mut())
                .await
            }
        }
        .map_err(map_database)?;
        Ok(result.rows_affected())
    }

    /// 业务作用：在显式业务裁决要求新 deadline 时重排既有 timer，复用同一行并递增代数。
    ///
    /// 复用行而不是插入新行，是 `UNIQUE(saga_id, scope_kind, scope_key, kind, attempt_no)`
    /// 的直接推论；`generation` 递增加上租约清零，使旧代的在途消费全部被 fencing 拒绝。
    ///
    /// 参数说明：
    /// - `saga_id`: 实例身份。
    /// - `scope`: 作用域。
    /// - `kind`: timer 种类。
    /// - `attempt`: 关联尝试序号。
    /// - `due_at_ms`: 新的到期时刻（epoch 毫秒）。
    /// - `expected_saga_version`: 重排时刻的实例版本。
    ///
    /// 返回：重排成功返回 [`TimerReschedule::Rescheduled`]；目标不存在或已进入
    /// `FIRED/CANCELLED` 终态返回 `NotFound`（终态 timer 的重排属于新动作，应使用
    /// 新 attempt，而不是复活旧身份）。
    /// 标识非法、事务缺失或底层失败返回错误。
    pub async fn reschedule_timer(
        &self,
        saga_id: &SagaId,
        scope: TimerScope<'_>,
        kind: &str,
        attempt: AttemptNo,
        due_at_ms: i64,
        expected_saga_version: u64,
    ) -> Result<TimerReschedule, SagaStoreError> {
        validate_kind(kind)?;
        validate_epoch_ms(due_at_ms, "timer due_at")?;
        validate_saga_version(expected_saga_version)?;
        // 重排与产生新 deadline 的业务裁决同事务：裁决提交而重排丢失，会让新期限
        // 永远不再被检查。普通 pause/resume 只改 available_at，不得调用本方法顺延 due_at。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_timer SET due_at = ?, available_at = ?, expected_saga_version = ?, \
             generation = generation + 1, state = ?, owner = NULL, fencing_token = NULL, \
             claimed_until = NULL WHERE saga_id = ? AND scope_kind = ? AND scope_key = ? \
             AND kind = ? AND attempt_no = ? AND state IN (?, ?)",
        )
        .bind(due_at_ms)
        .bind(due_at_ms)
        .bind(expected_saga_version)
        .bind(TimerState::Pending.as_str())
        .bind(saga_id.as_str())
        .bind(scope.kind_str())
        .bind(scope.key_str())
        .bind(kind)
        .bind(attempt.get())
        .bind(TimerState::Pending.as_str())
        .bind(TimerState::Claimed.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Ok(TimerReschedule::NotFound);
        }
        Ok(TimerReschedule::Rescheduled)
    }

    /// 业务作用：以租约方式竞争领取到期 timer，多副本可安全并发调用。
    ///
    /// 同时接管租约已过期的 `CLAIMED` 行（持有者崩溃），新 fencing token 使旧持有者的
    /// 后续完成被拒绝。**必须在 ambient 事务之外调用**：领取是独立提交的租约操作，
    /// 放进业务事务会把租约悬挂在未提交状态上。
    ///
    /// 参数说明：
    /// - `owner`: 本副本的稳定标识。
    /// - `fencing_token`: 本轮发行且尚未消费的唯一 capability；调用后所有权进入返回批次。
    /// - `now_ms`: 当前时刻（epoch 毫秒），由调用方注入统一时钟。
    /// - `lease_ms`: 租约时长（毫秒），必须为正。
    /// - `limit`: 单轮最多领取行数（有界，防一次拉爆）。
    ///
    /// 返回：本轮领取到的 token 批次（timer 行含 `expected_saga_version` 与 `generation`）；
    /// 参数非法、在事务内调用或底层失败返回错误。错误同样消耗 capability；数据库结果不确定时
    /// 不允许重建旧 token，可能已领取的行必须等待租约到期后由新 token 接管。
    pub async fn claim_due_timers(
        &self,
        owner: &str,
        fencing_token: TimerFencingToken,
        now_ms: i64,
        lease_ms: i64,
        limit: u32,
    ) -> Result<TimerClaimBatch, SagaStoreError> {
        validate_owner(owner)?;
        validate_epoch_ms(now_ms, "timer claim now")?;
        if lease_ms <= 0 || limit == 0 {
            return Err(SagaStoreError::new(
                "timer claim requires a positive lease and limit",
            ));
        }
        // 领取绝不允许挂在业务事务里:租约必须立即对其它副本可见,否则互斥失效。
        if natx::in_transaction() {
            return Err(SagaStoreError::new(
                "timer claim cannot run inside an ambient transaction",
            ));
        }
        let claimed_until = now_ms
            .checked_add(lease_ms)
            .ok_or_else(|| SagaStoreError::new("timer claim lease deadline overflow"))?;
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        // claim 结果若在网络层不确定，token 会随本次调用销毁；禁止把字符串抄出重试，
        // 否则两个调用可能同时回读同一批权威。可能已提交的租约只能按过期接管路径恢复。
        sqlx::query(
            "UPDATE saga_timer SET state = ?, owner = ?, fencing_token = ?, claimed_until = ? \
             WHERE ((state = ? AND available_at <= ?) \
             OR (state = ? AND claimed_until IS NOT NULL AND claimed_until <= ?)) \
             ORDER BY due_at ASC LIMIT ?",
        )
        .bind(TimerState::Claimed.as_str())
        .bind(owner)
        .bind(fencing_token.persistence_value())
        .bind(claimed_until)
        .bind(TimerState::Pending.as_str())
        .bind(now_ms)
        .bind(TimerState::Claimed.as_str())
        .bind(now_ms)
        .bind(limit)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;

        let rows = sqlx::query(
            "SELECT timer_id, saga_id, scope_kind, scope_key, kind, due_at, available_at, \
             state, attempt_no, expected_saga_version, generation, owner, claimed_until \
             FROM saga_timer WHERE owner = ? AND fencing_token = ? AND state = ? \
             ORDER BY due_at ASC",
        )
        .bind(owner)
        .bind(fencing_token.persistence_value())
        .bind(TimerState::Claimed.as_str())
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        let timers = rows
            .iter()
            .map(parse_timer_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TimerClaimBatch::from_committed_claim(fencing_token, timers))
    }

    /// 业务作用：在到期触发的状态迁移事务内，以 fencing 校验消费一个已领取 timer。
    ///
    /// 参数说明：
    /// - `timer_id`: timer 稳定身份。
    /// - `fencing_token`: 领取时取得的 token。
    /// - `now_ms`: 执行 fencing 校验时的当前 epoch 毫秒，必须是最新时钟读数。
    ///
    /// 返回：校验通过并标记 `FIRED` 返回 [`TimerFencing::Applied`]；租约已被接管、
    /// timer 已重排或已作废返回 `Lost`——**调用方必须放弃本次迁移并回滚事务**。
    /// 事务缺失或底层失败返回错误。
    pub async fn complete_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
    ) -> Result<TimerFencing, SagaStoreError> {
        validate_timer_id(timer_id)?;
        validate_epoch_ms(now_ms, "timer completion now")?;
        // 消费必须与它触发的迁移同事务:迁移提交而 timer 未标 FIRED 会重复触发,
        // 反之超时事实丢失。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_timer SET state = ?, owner = NULL, fencing_token = NULL, \
             claimed_until = NULL WHERE timer_id = ? AND state = ? AND fencing_token = ? \
             AND claimed_until IS NOT NULL AND claimed_until > ?",
        )
        .bind(TimerState::Fired.as_str())
        .bind(timer_id)
        .bind(TimerState::Claimed.as_str())
        .bind(fencing_token.persistence_value())
        .bind(now_ms)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Ok(TimerFencing::Lost);
        }
        Ok(TimerFencing::Applied)
    }

    /// 业务作用：把已领取但决定不消费的 timer（如实例处于 `PAUSED`）交还队列。
    ///
    /// 参数说明：
    /// - `timer_id`: timer 稳定身份。
    /// - `fencing_token`: 领取时取得的 token。
    /// - `now_ms`: 交还 fencing 校验时的当前 epoch 毫秒，必须是最新时钟读数。
    /// - `available_at_ms`: 暂停状态仍持续时的下次轮询时刻；只影响扫描节奏，不改业务 deadline。
    ///
    /// 返回：交还成功返回 [`TimerFencing::Applied`]；租约已被接管或 timer 已变更
    /// 返回 `Lost`（无需补救，接管方自会处理）。底层失败返回错误。
    pub async fn release_timer(
        &self,
        timer_id: &str,
        fencing_token: &TimerFencingToken,
        now_ms: i64,
        available_at_ms: i64,
    ) -> Result<TimerFencing, SagaStoreError> {
        validate_timer_id(timer_id)?;
        validate_epoch_ms(now_ms, "timer release now")?;
        validate_epoch_ms(available_at_ms, "timer available_at")?;
        // 交还是独立提交的租约动作；若错误加入业务事务，事务回滚会让调用方误以为
        // 已经释放控制权，而其它副本仍要等待旧租约到期。
        if natx::in_transaction() {
            return Err(SagaStoreError::new(
                "timer release cannot run inside an ambient transaction",
            ));
        }
        let mut connection = natx::conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_timer AS timer JOIN saga_instance AS instance \
             ON instance.saga_id = timer.saga_id \
             SET timer.state = ?, \
             timer.available_at = IF(instance.control_state = 'PAUSED', ?, timer.due_at), \
             timer.owner = NULL, timer.fencing_token = NULL, timer.claimed_until = NULL \
             WHERE timer.timer_id = ? AND timer.state = ? AND timer.fencing_token = ? \
             AND timer.claimed_until IS NOT NULL AND timer.claimed_until > ?",
        )
        .bind(TimerState::Pending.as_str())
        .bind(available_at_ms)
        .bind(timer_id)
        .bind(TimerState::Claimed.as_str())
        .bind(fencing_token.persistence_value())
        .bind(now_ms)
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        if updated.rows_affected() == 0 {
            return Ok(TimerFencing::Lost);
        }
        Ok(TimerFencing::Applied)
    }

    /// 业务作用：恢复 Saga 时立即唤醒暂停期间已经逾期的待领取 timer，不顺延业务期限。
    ///
    /// 只修改扫描调度列 `available_at`，`due_at` 保持原业务 deadline。已被 worker
    /// 领取的行不在此抢占；worker 随后的交还会原子复验控制态，ACTIVE 时同样恢复为
    /// 原 `due_at`，从而覆盖 resume/交还竞态。
    ///
    /// 参数说明：
    /// - `saga_id`: 刚恢复为 ACTIVE 的实例。
    ///
    /// 返回：被提前唤醒的 PENDING timer 数；事务缺失、时钟非法或底层失败返回错误。
    pub async fn wake_saga_timers(&self, saga_id: &SagaId) -> Result<u64, SagaStoreError> {
        // 控制态恢复与 timer 唤醒必须同事务：若只提交 ACTIVE 而唤醒丢失，业务 deadline
        // 会被暂停退避悄悄顺延；反之则会在仍 PAUSED 时制造热轮询。
        require_ambient_transaction()?;
        let mut connection = natx::mandatory_conn_for(&self.datasource)
            .await
            .map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_timer SET available_at = due_at WHERE saga_id = ? AND state = ? \
             AND available_at <> due_at",
        )
        .bind(saga_id.as_str())
        .bind(TimerState::Pending.as_str())
        .execute(connection.as_mut())
        .await
        .map_err(map_database)?;
        Ok(updated.rows_affected())
    }
}

/// 业务作用：从查询结果解析 timer 行。
///
/// 参数说明：
/// - `row`: `SELECT` 出的 timer 行。
///
/// 返回：解析成功返回强类型行；状态列不在词汇表内或身份非法时返回数据损坏错误。
fn parse_timer_row(row: &sqlx::mysql::MySqlRow) -> Result<SagaTimerRow, SagaStoreError> {
    use crate::error::corrupt;
    let saga_id: String = row.try_get("saga_id").map_err(map_database)?;
    let state: String = row.try_get("state").map_err(map_database)?;
    let attempt: u32 = row.try_get("attempt_no").map_err(map_database)?;
    Ok(SagaTimerRow {
        timer_id: row.try_get("timer_id").map_err(map_database)?,
        saga_id: SagaId::new(saga_id).map_err(|_| corrupt("saga_id"))?,
        scope_kind: row.try_get("scope_kind").map_err(map_database)?,
        scope_key: row.try_get("scope_key").map_err(map_database)?,
        kind: row.try_get("kind").map_err(map_database)?,
        due_at_ms: row.try_get("due_at").map_err(map_database)?,
        available_at_ms: row.try_get("available_at").map_err(map_database)?,
        state: TimerState::parse(&state).ok_or_else(|| corrupt("state"))?,
        attempt: AttemptNo::new(attempt).map_err(|_| corrupt("attempt_no"))?,
        expected_saga_version: row.try_get("expected_saga_version").map_err(map_database)?,
        generation: row.try_get("generation").map_err(map_database)?,
        owner: row.try_get("owner").map_err(map_database)?,
        claimed_until_ms: row.try_get("claimed_until").map_err(map_database)?,
    })
}

/// 业务作用：校验 timer 身份的空白、长度与控制字符边界，保护唯一键与 fencing 对齐。
///
/// 参数说明：
/// - `timer_id`: 待校验的 timer 身份。
///
/// 返回：合法返回 `Ok`；否则返回稳定错误。
fn validate_timer_id(timer_id: &str) -> Result<(), SagaStoreError> {
    if timer_id.is_empty()
        || timer_id.trim() != timer_id
        || timer_id.len() > 190
        || timer_id.chars().any(char::is_control)
    {
        return Err(SagaStoreError::new("invalid timer id"));
    }
    Ok(())
}

/// 业务作用：校验 timer 种类走严格标识符字符集，它会进入唯一键与运维查询维度。
///
/// 参数说明：
/// - `kind`: 待校验的 timer 种类。
///
/// 返回：合法返回 `Ok`；否则返回稳定错误。
fn validate_kind(kind: &str) -> Result<(), SagaStoreError> {
    if kind.is_empty()
        || kind.len() > 64
        || !kind
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(SagaStoreError::new("invalid timer kind"));
    }
    Ok(())
}

/// 业务作用：校验租约持有者标识的边界。
///
/// 参数说明：
/// - `owner`: 待校验的持有者标识。
///
/// 返回：合法返回 `Ok`；否则返回稳定错误。
fn validate_owner(owner: &str) -> Result<(), SagaStoreError> {
    if owner.is_empty()
        || owner.trim() != owner
        || owner.len() > 128
        || owner.chars().any(char::is_control)
    {
        return Err(SagaStoreError::new("invalid timer owner"));
    }
    Ok(())
}

/// 业务作用：校验调用方注入的 epoch 毫秒属于持久化时钟的有效非负区间。
///
/// 参数说明：
/// - `value`: 待校验的 epoch 毫秒。
/// - `field`: 稳定诊断字段名，不得包含业务数据。
///
/// 返回：非负值返回 `Ok`；负值返回稳定错误，阻止失真时钟提前触发或永久隐藏 timer。
fn validate_epoch_ms(value: i64, field: &str) -> Result<(), SagaStoreError> {
    if value < 0 {
        return Err(SagaStoreError::new(format!(
            "{field} must be a non-negative epoch millisecond"
        )));
    }
    Ok(())
}

/// 业务作用：校验 timer 绑定的 Saga 实例版本已初始化，避免零值绕过 fencing。
///
/// 参数说明：
/// - `version`: 调度或重排时固定的实例版本。
///
/// 返回：正版本返回 `Ok`；零返回稳定错误。
fn validate_saga_version(version: u64) -> Result<(), SagaStoreError> {
    if version == 0 {
        return Err(SagaStoreError::new(
            "timer expected saga version must start at 1",
        ));
    }
    Ok(())
}
