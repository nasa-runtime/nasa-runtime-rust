//! Durable timer store：调度、作废、租约领取与 fencing 完成。
//!
//! timer 的生死必须与触发它的状态迁移同事务：写下一步命令 Outbox 时同事务调度对应
//! timeout timer，迁移离开该步骤时同事务作废旧 timer。多副本可以竞争领取到期 timer，
//! 但**消费时必须复验 fencing token**（以及调用方侧的 `expected_saga_version` 与
//! `generation`），失去租约的旧 owner 即使迟到也不能推进新状态。

use std::sync::atomic::{AtomicU64, Ordering};

use nasaga_core::{AttemptNo, SagaId, StepName};
use sqlx::Row as _;
use uuid::Uuid;

use crate::error::{is_unique_violation, map_connection, map_database, SagaStoreError};
use crate::instance::require_ambient_transaction;
use crate::MySqlSagaStore;

/// 实例级 timer 的固定 canonical scope key。
///
/// 唯一键各列必须 `NOT NULL`，实例级 timer 没有真实步骤名，用固定值占位；
/// `scope_kind` 列已经隔离命名空间，业务步骤即使叫 `instance` 也不会与之冲突。
const INSTANCE_SCOPE_KEY: &str = "instance";

/// 派生 timer 领取 fencing token 的固定命名空间（ASCII `nasasaga-v1-fcns`）。
const FENCING_TOKEN_NAMESPACE: Uuid = Uuid::from_bytes(*b"nasasaga-v1-fcns");

/// 业务作用：以长度前缀编码 fencing 派生字段，消除 owner 等可变长字段的拼接歧义。
///
/// 参数说明：
/// - `fields`: 按固定顺序给出的 runtime nonce、owner、时钟和领取序号。
///
/// 返回：字段边界唯一、可直接送入 UUIDv5 的规范字节串。
fn canonical_fencing_bytes(fields: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for field in fields {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field);
    }
    encoded
}

/// 业务作用：表示只能由安全发行器产生的 timer 租约 capability，禁止调用方用裸字符串伪造权威。
///
/// 类型不实现 `Clone`，且不提供字符串/UUID 构造入口；token 被 claim 消费后只能通过
/// [`TimerClaimBatch::token`] 借用完成或交还本批 timer，不能再次用于领取另一批。
#[derive(PartialEq, Eq)]
pub struct TimerFencingToken(String);

impl TimerFencingToken {
    /// 业务作用：仅在 store 内读取 opaque token 的持久化表示，禁止 capability 原文泄漏到公共 API。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：固定为小写 UUID 文本的只读切片，仅用于 SQL fencing 条件绑定。
    fn as_str(&self) -> &str {
        &self.0
    }
}

/// 业务作用：为单个 timer worker runtime 发行跨实例不碰撞、且只能消费一次的 fencing capability。
///
/// `owner` 仍承担租约归属与审计身份；实例私有随机 nonce 让两个误配同名副本或同名重启进程
/// 也无法派生相同 token，进程内序号则保证同一实例连续领取各不相同。
pub struct TimerFencingTokenIssuer {
    /// 构造时由 OS 随机源生成且不允许配置注入，避免副本同名把 token 唯一性降级为运维约定。
    runtime_nonce: Uuid,
    /// 单个 runtime 实例内的领取序号；只参与 token 唯一性，不承载业务状态。
    claim_seq: AtomicU64,
}

impl TimerFencingTokenIssuer {
    /// 业务作用：建立一个独立的 fencing token 发行域，隔离副本误配与进程重启。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：持有不可注入 UUIDv4 runtime nonce、领取序号从零开始的新发行器。
    pub fn new() -> Self {
        // nonce 必须在 worker 构造时从随机源产生，禁止从 owner 或部署配置派生；否则同名副本
        // 仍可能共享 fencing 权威，失去租约的旧进程便无法仅靠 token 被拒绝。
        Self {
            runtime_nonce: Uuid::new_v4(),
            claim_seq: AtomicU64::new(0),
        }
    }

    /// 业务作用：为一次 timer 批量领取生成只属于当前 runtime 实例和领取批次的 capability。
    ///
    /// 参数说明：
    /// - `owner`: 本副本稳定的人类可读租约身份，仅用于域分离，不承担唯一性兜底。
    /// - `now_ms`: 本轮领取时注入的 epoch 毫秒。
    ///
    /// 返回：包含 runtime 随机熵与进程内单调序号、且不能复制或从字符串重建的 token。
    pub fn issue(&self, owner: &str, now_ms: i64) -> TimerFencingToken {
        // Relaxed 足以保证同一 AtomicU64 不返回重复序号；token 不发布业务内存状态，
        // 因而不需要用更强内存序把 fencing 与数据库事务错误地耦合。
        let seq = self.claim_seq.fetch_add(1, Ordering::Relaxed);
        TimerFencingToken(
            Uuid::new_v5(
                &FENCING_TOKEN_NAMESPACE,
                &canonical_fencing_bytes(&[
                    self.runtime_nonce.as_bytes(),
                    owner.as_bytes(),
                    &now_ms.to_be_bytes(),
                    &seq.to_be_bytes(),
                ]),
            )
            .to_string(),
        )
    }
}

impl Default for TimerFencingTokenIssuer {
    /// 业务作用：按安全默认值创建独立的 worker fencing token 发行域。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与 [`TimerFencingTokenIssuer::new`] 相同的新发行器。
    fn default() -> Self {
        Self::new()
    }
}

/// 业务作用：区分 timer 的作用域，把实例级期限与步骤级超时放进不同命名空间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerScope<'a> {
    /// 实例级（全局 deadline、resolution 预算等）。
    Instance,
    /// 步骤级，绑定真实 step name。
    Step(&'a StepName),
}

impl TimerScope<'_> {
    /// 业务作用：返回作用域类别的稳定文本名，写入 `scope_kind` 列。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`INSTANCE` 或 `STEP`。
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::Instance => "INSTANCE",
            Self::Step(_) => "STEP",
        }
    }

    /// 业务作用：返回作用域键，写入 `scope_key` 列。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：实例级返回固定 canonical key；步骤级返回真实 step name。
    pub fn key_str(&self) -> &str {
        match self {
            Self::Instance => INSTANCE_SCOPE_KEY,
            Self::Step(step) => step.as_str(),
        }
    }
}

/// 业务作用：表示 timer 行的生命周期状态。
///
/// 分支说明：`Claimed` 是带租约的中间态——租约到期未完成会被其它副本以新 fencing token
/// 重新领取，旧 owner 的完成尝试随后被 fencing 拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerState {
    /// 等待到期。
    Pending,
    /// 已被某个副本领取，租约生效中。
    Claimed,
    /// 已消费：到期触发的状态迁移已提交。
    Fired,
    /// 已作废：所属步骤/实例已迁移离开，即使到期也不得触发。
    Cancelled,
}

impl TimerState {
    /// 业务作用：返回状态的稳定文本名，用于持久化列与运维查询。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：timer 状态稳定名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Claimed => "CLAIMED",
            Self::Fired => "FIRED",
            Self::Cancelled => "CANCELLED",
        }
    }

    /// 业务作用：把持久化列中的稳定文本解析回状态，是 `as_str` 的严格逆映射。
    ///
    /// 参数说明：
    /// - `raw`: 持久化读出的状态文本。
    ///
    /// 返回：识别成功返回对应状态；文本不在词汇表内返回 `None`，调用方按数据损坏处理。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "PENDING" => Some(Self::Pending),
            "CLAIMED" => Some(Self::Claimed),
            "FIRED" => Some(Self::Fired),
            "CANCELLED" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// 业务作用：描述一次 timer 调度的全部持久化输入。
///
/// 字段说明：`expected_saga_version` 是调度时刻的实例版本——消费方触发迁移前必须
/// 复验实例当前版本仍与之一致，版本前进说明 timer 语义所依附的状态已经变化。
#[derive(Debug, Clone)]
pub struct TimerSpec<'a> {
    /// timer 的稳定身份，由调用方生成（跨调度重试稳定）。
    pub timer_id: &'a str,
    /// 所属实例。
    pub saga_id: &'a SagaId,
    /// 作用域。
    pub scope: TimerScope<'a>,
    /// timer 种类（如步骤超时、resolution 预算），进入唯一键与运维查询。
    pub kind: &'a str,
    /// 到期时刻（epoch 毫秒）。
    pub due_at_ms: i64,
    /// 关联的尝试序号；同一 attempt 的调度重试命中唯一键幂等吸收。
    pub attempt: AttemptNo,
    /// 调度时刻的实例版本，消费前复验。
    pub expected_saga_version: u64,
}

/// 业务作用：区分 timer 调度的两种合法结果，使调度事务的崩溃重试可被幂等吸收。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerSchedule {
    /// 本事务真实创建了 timer。
    Scheduled,
    /// 同一 (scope, kind, attempt) 已存在同 id timer（调度重试）；无新副作用。
    AlreadyScheduled,
}

/// 业务作用：区分重排 timer 的结果；终态 timer 不得原地复活，新的业务动作必须用新 attempt。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerReschedule {
    /// 已重排：due 更新、generation 递增、回到 `PENDING`。
    Rescheduled,
    /// 目标 timer 不存在、已 `FIRED` 或已 `CANCELLED`；调用方应按新业务裁决决定是否
    /// 为新 attempt 调度新 timer，不能复活旧身份。
    NotFound,
}

/// 业务作用：区分带 fencing 的 timer 操作结果。
///
/// 分支说明：`Lost` 表示租约已被其它副本接管或 timer 已被重排/作废——旧 owner
/// **必须立即停止推进**，不得基于该 timer 发布任何命令。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerFencing {
    /// 操作生效，fencing 校验通过。
    Applied,
    /// fencing 失败：租约/状态已不属于本 owner。
    Lost,
}

/// 业务作用：claim 批次中的 timer 行，携带除 capability 外的全部持久化复查依据。
///
/// fencing token 刻意只由 [`TimerClaimBatch`] 持有，避免每行复制后被取出用于另一轮 claim。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SagaTimerRow {
    /// timer 稳定身份。
    pub timer_id: String,
    /// 所属实例。
    pub saga_id: SagaId,
    /// 作用域类别（`INSTANCE`/`STEP`）。
    pub scope_kind: String,
    /// 作用域键。
    pub scope_key: String,
    /// timer 种类。
    pub kind: String,
    /// 到期时刻（epoch 毫秒）。
    pub due_at_ms: i64,
    /// 下次允许 worker 领取的时刻；暂停退避只改本列，不顺延业务 `due_at`。
    pub available_at_ms: i64,
    /// 当前状态。
    pub state: TimerState,
    /// 关联尝试序号。
    pub attempt: AttemptNo,
    /// 调度时刻的实例版本；消费前必须与实例当前版本比对。
    pub expected_saga_version: u64,
    /// 重排代数；每次重排递增，旧代 timer 即使迟到也不能推进新状态。
    pub generation: u32,
    /// 当前租约持有者。
    pub owner: Option<String>,
    /// 租约到期时刻（epoch 毫秒）。
    pub claimed_until_ms: Option<i64>,
}

/// 业务作用：绑定一次领取返回的 timer 集合与其不可复制 fencing capability。
///
/// token 的所有权保留在批次内，调用方只能借用它完成或交还本批 timer；这使同一 token
/// 无法再次传给 [`MySqlSagaStore::claim_due_timers`]，从类型层落实“每轮唯一”。
pub struct TimerClaimBatch {
    token: TimerFencingToken,
    timers: Vec<SagaTimerRow>,
}

impl TimerClaimBatch {
    /// 业务作用：借用本批领取的 fencing capability，供完成、交还或 Orchestrator 裁决使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与本批数据库领取绑定的只读 token；所有权不会泄露给调用方复用。
    pub fn token(&self) -> &TimerFencingToken {
        &self.token
    }

    /// 业务作用：读取本轮成功领取的 timer 快照集合。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只读 timer 切片；每项只能凭本批 [`Self::token`] 通过 fencing。
    pub fn timers(&self) -> &[SagaTimerRow] {
        &self.timers
    }

    /// 业务作用：返回本轮成功领取的 timer 数量，供有界批处理和容量门禁计数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本批 timer 数；零表示没有到期或可接管的 timer。
    pub fn len(&self) -> usize {
        self.timers.len()
    }

    /// 业务作用：判断本轮是否未领取任何 timer，避免调用方用 token 存在性误判工作量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：timer 集合为空时返回 `true`。
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }
}

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
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
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
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
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
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
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
        let mut connection = natx::conn().await.map_err(map_connection)?;
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
        .bind(fencing_token.as_str())
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
        .bind(fencing_token.as_str())
        .bind(TimerState::Claimed.as_str())
        .fetch_all(connection.as_mut())
        .await
        .map_err(map_database)?;
        let timers = rows
            .iter()
            .map(parse_timer_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TimerClaimBatch {
            token: fencing_token,
            timers,
        })
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
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
        let updated = sqlx::query(
            "UPDATE saga_timer SET state = ?, owner = NULL, fencing_token = NULL, \
             claimed_until = NULL WHERE timer_id = ? AND state = ? AND fencing_token = ? \
             AND claimed_until IS NOT NULL AND claimed_until > ?",
        )
        .bind(TimerState::Fired.as_str())
        .bind(timer_id)
        .bind(TimerState::Claimed.as_str())
        .bind(fencing_token.as_str())
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
        let mut connection = natx::conn().await.map_err(map_connection)?;
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
        .bind(fencing_token.as_str())
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
        let mut connection = natx::mandatory_conn().await.map_err(map_connection)?;
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
