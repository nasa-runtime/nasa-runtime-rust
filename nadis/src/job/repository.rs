//! Job 定义登记仓库：把已冻结定义原子写入分片索引、定义 HASH 与首个调度时刻。
//!
//! 登记以单调 `definition_revision` 防回滚，同修订号但摘要不同时封闭调度进入 `CONFLICT`，不按节点启动
//! 顺序裁决，避免同一任务在集群里出现两套语义。

use std::collections::BTreeSet;
use std::sync::Arc;

use base64::Engine;
use redis::cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo};
use redis::streams::StreamRangeReply;

use crate::client::{Conn, RedisClient};
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::identifiers::{manual_run_id, scheduled_run_id};
use crate::job::keyspace::JobKeyspace;
use crate::job::model::{JobDefinitionState, JobResultCode, JobState};
use crate::job::names::require_name;
use crate::job::payload::JobPayload;
use crate::job::run::JobRun;
use crate::job::script::{eval, value_to_string, JobScript};
use crate::partition::compat_string_hash;

/// 登记定义的原子脚本。
static JOB_REGISTER: JobScript = JobScript::new(include_str!("lua/job_register.lua"));
/// 建立或复验 `(qualifier, namespace)` 的不可变布局指纹。
static JOB_LAYOUT: JobScript = JobScript::new(include_str!("lua/job_layout.lua"));
/// 手工触发的幂等创建脚本。
static MANUAL_FIRE: JobScript = JobScript::new(include_str!("lua/manual_fire.lua"));
/// 单个 Run 的只读脚本。
static READ_RUN: JobScript = JobScript::new(include_str!("lua/read_run.lua"));
/// 到期调度项的有界只读扫描脚本。
static SCAN_DUE: JobScript = JobScript::new(include_str!("lua/scan_due.lua"));
/// 单个到期项的原子触发脚本。
static FIRE_DUE: JobScript = JobScript::new(include_str!("lua/fire_due.lua"));
/// 同分片批量触发到期项的脚本。
static FIRE_DUE_BATCH: JobScript = JobScript::new(include_str!("lua/fire_due_batch.lua"));
/// 同 slot 批量续期执行权的脚本。
static RENEW_BATCH: JobScript = JobScript::new(include_str!("lua/renew_batch.lua"));
/// 领取 attempt 执行权、分配 fencing token 与租约的脚本。
static START_RUN: JobScript = JobScript::new(include_str!("lua/start_run.lua"));
/// 续期当前 attempt 租约并捎带取消信号的脚本。
static RENEW_RUN: JobScript = JobScript::new(include_str!("lua/renew_run.lua"));
/// 提交 attempt 结果并进入重试或终态的脚本。
static FINISH_RUN: JobScript = JobScript::new(include_str!("lua/finish_run.lua"));
/// 服务端确认租约到期后恢复 Run 的脚本。
static RECOVER_EXPIRED: JobScript = JobScript::new(include_str!("lua/recover_expired.lua"));
/// 本地无容量时把已拉取 Run 放回可见索引的脚本。
static DEFER_RUN: JobScript = JobScript::new(include_str!("lua/defer_run.lua"));
/// 提升到期可见成员并重建丢失 Dispatch 消息的脚本。
static PROMOTE_VISIBLE: JobScript = JobScript::new(include_str!("lua/promote_visible.lua"));
/// 请求取消普通 Run 的脚本。
static REQUEST_CANCEL: JobScript = JobScript::new(include_str!("lua/request_cancel.lua"));
/// Fanout 根未按时提交时收敛普通根 Run 的脚本。
static FAIL_WAITING_CREATION: JobScript =
    JobScript::new(include_str!("lua/fail_waiting_creation.lua"));
/// 暂停任务新自动触发的脚本。
static JOB_PAUSE: JobScript = JobScript::new(include_str!("lua/job_pause.lua"));
/// 从新逻辑时刻恢复任务触发的脚本。
static JOB_RESUME: JobScript = JobScript::new(include_str!("lua/job_resume.lua"));
/// 以单调修订号删除任务并留 tombstone 的脚本。
static JOB_DELETE: JobScript = JobScript::new(include_str!("lua/job_delete.lua"));
/// 显式选择摘要解除 CONFLICT 的脚本。
static JOB_RESOLVE_CONFLICT: JobScript =
    JobScript::new(include_str!("lua/job_resolve_conflict.lua"));
/// 设置分片内普通 Run 命名空间门禁的脚本。
static NAMESPACE_SET_STATE: JobScript = JobScript::new(include_str!("lua/namespace_set_state.lua"));
/// 回收到期 tombstone 的脚本。
static JOB_CLEANUP_TOMBSTONES: JobScript =
    JobScript::new(include_str!("lua/job_cleanup_tombstones.lua"));
/// 按批终态化已删除定义残留 waitq Run 的脚本。
static JOB_REAP: JobScript = JobScript::new(include_str!("lua/job_reap.lua"));
/// 普通根 Run 转入 FANOUT_CREATING 的脚本。
static PREPARE_FANOUT_ROOT: JobScript = JobScript::new(include_str!("lua/prepare_fanout_root.lua"));
/// 把桶内 Fanout 状态回填普通根 Run 的脚本。
static FINISH_FANOUT_ROOT: JobScript = JobScript::new(include_str!("lua/finish_fanout_root.lua"));

/// 手工触发的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualFireOutcome {
    /// 首次创建成功，返回 Run 标识与 Dispatch 消息 ID。
    Fired {
        /// 新创建的 Run 标识。
        run_id: String,
        /// 对应 Dispatch Stream 的消息 ID。
        message_id: String,
    },
    /// 同 requestId 已存在，幂等采用既有 Run。
    Adopted {
        /// 相同 requestId 已关联的 Run 标识。
        run_id: String,
    },
    /// 任务定义不存在。
    NotFound,
    /// 任务或命名空间未启用，或 FANOUT_ONLY 不能独立触发。
    StateMismatch,
}

/// 普通根 Run 转入跨 slot Fanout intent 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareFanoutRootOutcome {
    /// 首次转入 FANOUT_CREATING：权威时刻与创建截止毫秒。
    Prepared {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 创建截止毫秒；超时由对账收敛。
        deadline: i64,
    },
    /// 同一 fanoutId 已转入，幂等采用，返回既有创建截止。
    Adopted {
        /// 既有创建截止毫秒。
        deadline: i64,
    },
    /// Run 不在 RUNNING，拒绝转入。
    StateMismatch,
    /// owner 或 attemptToken 不匹配，拒绝转入。
    StaleOwner,
    /// 任务已删除或 Run 落在删除修订 fence 内，完成权威仍由普通 attempt 持有。
    JobDeleted,
}

/// 把桶内 Fanout 状态回填普通根 Run 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishFanoutRootOutcome {
    /// 回填成功：根进入的状态、权威时刻或创建截止，以及可选唤醒延迟。
    Ok {
        /// 回填后根 Run 的状态（COMMITTED 回填为 WAITING_CHILDREN，或桶内终态）。
        state: JobState,
        /// WAITING_CHILDREN 时为创建截止，终态时为权威当前毫秒。
        redis_now_or_deadline: i64,
        /// 供调度器重新唤醒的延迟毫秒；无则为 `None`。
        wake_delay_ms: Option<i64>,
    },
    /// 目标状态已回填，幂等采用。
    Adopted,
    /// fanoutId 或 rootAttempt 不匹配，回填作废。
    Stale,
    /// 根已终态，无需回填。
    AlreadyCompleted {
        /// 根当前终态。
        state: JobState,
    },
    /// 根状态不允许该回填。
    StateMismatch,
    /// 定义删除 fence 阻止已提交桶继续开放分片执行权。
    JobDeleted {
        /// 普通根当前状态。
        state: JobState,
        /// Fanout 创建截止毫秒；删除收敛在该时刻后驱动跨 slot 取消。
        deadline: i64,
    },
}

/// 定义控制类脚本（pause/resume/resolve_conflict）的共享封闭结局；每个入口只产生其子集。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionControlOutcome {
    /// 控制动作生效。
    Ok,
    /// 目标定义不存在。
    NotFound,
    /// 定义当前状态不允许该动作（如 resume 一个 DELETED/CONFLICT、resolve 一个非 CONFLICT）。
    StateMismatch,
    /// resolve_conflict 时管理者提供的修订号或摘要与 Redis 权威记录不一致，拒绝解除。
    Stale,
}

/// 删除定义的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// 删除成功并写入 tombstone；返回权威时刻。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
    },
    /// 目标定义不存在。
    NotFound,
    /// 当前修订号高于删除修订号，拒绝低修订号覆盖。
    Stale {
        /// Redis 中当前生效的定义修订号。
        revision: i64,
    },
}

/// 单个调度分片的命名空间门禁只读快照；治理聚合据此区分已收敛、未收敛与未触达分片。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceShardSnapshot {
    /// 分片序号。
    pub shard: u32,
    /// `namespaceState` 原文(ENABLED/PAUSED)；从未治理过的分片为 `None`，按缺省 ENABLED 解释。
    pub state: Option<String>,
    /// 最近一次治理发布的权威毫秒时刻。
    pub updated_at_ms: Option<i64>,
    /// 最近一次治理的操作来源(审计字段)。
    pub updated_by: Option<String>,
}

/// 设置普通 Run 命名空间门禁的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceStateOutcome {
    /// 门禁已发布；返回权威时刻。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
    },
    /// 目标状态既非 ENABLED 也非 PAUSED，拒绝设置。
    Invalid,
}

/// 批量触发的一项：目标定义与计算好的时刻/标志；同一批全部落在同一调度分片。
#[derive(Debug, Clone)]
pub struct FireDueBatchItem<'a> {
    /// 目标任务定义。
    pub definition: &'a JobDefinition,
    /// 本次权威逻辑触发时刻毫秒。
    pub logical_fire_at: i64,
    /// 计算好的下一逻辑触发时刻；`FIXED_DELAY` 传 0。
    pub proposed_next_fire_at: i64,
    /// 误触发跳过时为真，只推进调度时刻不建 Run。
    pub skip_run: bool,
    /// 记为补偿触发时为真，triggerType 用 `MISFIRE`。
    pub misfire: bool,
    /// 非 FIXED_DELAY 且要求下一时刻必须在未来时为真。
    pub must_end_in_future: bool,
}

/// 批量触发中一项的结果：派生 Run 标识、状态码与可选消息 ID。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FireDueBatchResult {
    /// 该项派生的 Run 标识。
    pub run_id: String,
    /// 触发状态码（OK/ADOPTED/STALE/STATE_MISMATCH/NOT_DUE/NEED_RECOMPUTE/SKIPPED）。
    pub code: String,
    /// OK 时的 Dispatch 消息 ID，其它状态为空。
    pub message_id: String,
}

/// 批量续期的一项：同 slot 的记录/租约/等待键、索引成员与当前 token。
#[derive(Debug, Clone)]
pub struct RenewBatchItem {
    /// Run 或 shard 记录 HASH key。
    pub record_key: String,
    /// 该 slot 的 leases ZSET key。
    pub leases_key: String,
    /// 该 slot 的 waiting ZSET key。
    pub waiting_key: String,
    /// 租约索引成员（普通 Run 为 runId，Fanout shard 为 `fanoutId:seq`）。
    pub member: String,
    /// 当前 attempt 的 fencing token。
    pub attempt_token: i64,
}

/// 批量续期中一项的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewBatchResult {
    /// 续期状态码（OK/STALE_OWNER/STATE_MISMATCH）。
    pub code: String,
    /// OK 时的新截止毫秒，其它状态为 0。
    pub deadline: i64,
    /// 是否已收到取消请求。
    pub cancel_requested: bool,
}

/// 一个到期调度项：任务名、权威逻辑时刻与定义修订号；修订号用于触发前 CAS 复验。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueEntry {
    /// 任务名。
    pub job_name: String,
    /// 调度 ZSET 中的权威逻辑触发时刻毫秒（score）。
    pub logical_fire_at: i64,
    /// 扫描时观测到的定义修订号；触发脚本再次复验，迟到扫描不能按旧修订号触发。
    pub definition_revision: i64,
}

/// 一次调度扫描的结果：Redis 权威当前时刻、命名空间门禁、下一最小 score 与本批到期项。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleDueScan {
    /// 脚本内 `TIME` 观测到的权威当前毫秒；调度节奏据此推进，不用本地时钟。
    pub redis_now: i64,
    /// ZSET 中下一最小 score；为空表示无待触发项，供自适应扫描间隔使用。
    pub next_score: Option<i64>,
    /// 当前分片的命名空间门禁是否开放；未知持久值按关闭处理。
    pub namespace_enabled: bool,
    /// 本批到期项，最多 `limit` 条。
    pub entries: Vec<DueEntry>,
}

/// 通用到期索引的一项；成员与 score 只用于定位，真正推进仍由对应状态脚本复验。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueIndexEntry {
    /// ZSET 成员。
    pub member: String,
    /// 扫描时的到期 score。
    pub score: i64,
}

/// 通用到期索引扫描结果；用于普通 Run 的 lease 与 Fanout 创建 waiting 恢复。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueIndexScan {
    /// 脚本观测到的 Redis 当前毫秒。
    pub redis_now: i64,
    /// 当前索引最小 score；空索引为 `None`。
    pub next_score: Option<i64>,
    /// 不晚于当前时刻的有限成员。
    pub entries: Vec<DueIndexEntry>,
}

/// 单个到期项触发的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FireDueOutcome {
    /// 首次触发成功，创建了 Run 与 Dispatch 消息。
    Fired {
        /// 本次触发的 Run 标识（由逻辑时刻与触发类型稳定派生）。
        run_id: String,
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// Dispatch Stream 消息 ID。
        message_id: String,
    },
    /// 同一逻辑时刻的 Run 已存在，幂等采用既有 Run。
    Adopted {
        /// 同一逻辑触发时刻已经创建的 Run 标识。
        run_id: String,
    },
    /// 误触发跳过：只推进下一调度时刻，不创建 Run。
    Skipped {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 推进后的下一逻辑触发时刻毫秒。
        next_fire_at: i64,
    },
    /// 该逻辑时刻尚未到期，扫描过早。
    NotDue {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
    },
    /// 提议的下一触发时刻已落后当前时刻，需要重新计算后再触发。
    NeedRecompute {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
    },
    /// 定义或命名空间未启用（禁止触发）。
    StateMismatch,
    /// 修订号或 schedule score 已被更新的调度推进，本次触发作废。
    Stale,
}

/// 领取 attempt 执行权的封闭结局；带数据的两种表示成功获得当前执行权。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartOutcome {
    /// 首次领取成功：新 attempt、单调 fencing token 与租约截止。
    Started {
        /// 本次 attempt 序号。
        attempt: i64,
        /// 单调 fencing token；副作用写外部资源时必须携带以拒绝旧 owner。
        attempt_token: i64,
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 服务端租约截止毫秒。
        lease_until: i64,
    },
    /// 同 owner 重放：采用既有 attempt 并顺延租约。
    Adopted {
        /// 既有 attempt 序号。
        attempt: i64,
        /// 既有 fencing token。
        attempt_token: i64,
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 顺延后的租约截止毫秒。
        lease_until: i64,
    },
    /// 同 owner 的重复消息到达时既有租约已过期，旧 attempt 不得被重新开放。
    LeaseExpired,
    /// 定义删除 fence 已覆盖本 Run，脚本已把它收敛为取消终态。
    JobDeleted,
    /// Fanout 根未提交，shard 不能开放执行权。
    NotCommitted,
    /// 目标节点或 assignment epoch 已变，本次领取作废。
    StaleAssignment,
    /// Run 记录不存在（已被清理或从未创建）。
    NotFound,
    /// 任务暂停或本地缺少兼容 Handler，已放回可见索引延后。
    Deferred,
    /// 当前执行器不理解 Run 的协议版本，保留等待兼容节点。
    ProtocolUnsupported,
    /// 记录声明的 source 与当前运行时不一致，不能按本地上下文执行。
    SourceMismatch,
    /// Fanout 记录中的稳定执行身份与调用方派生值不一致。
    IdentityMismatch,
    /// 记录的合同、Schema 或 codec 与本地 Handler 不兼容。
    ContractMismatch,
    /// 定义、合同、Schema 或 codec 不匹配，拒绝解码执行。
    Conflict,
    /// Run 已不在可领取状态（已终态或被他人领取）。
    Stale,
    /// 领取前发现取消请求，已置为终态取消。
    Cancelled,
    /// 串行/丢弃策略下本次触发被跳过。
    Skipped,
    /// 串行并发下已有运行实例，本 Run 入队阻塞等待。
    Blocked,
    /// fencing token 未严格递增，封闭执行入口。
    FencingRegression,
}

/// 续期当前 attempt 租约的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewOutcome {
    /// 续期成功：新的服务端截止与是否已收到取消请求。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 顺延后的租约截止毫秒。
        deadline: i64,
        /// 是否已记录取消请求；为真时业务应尽快收口当前 attempt。
        cancel_requested: bool,
    },
    /// Run 已不在 RUNNING/FANOUT_CREATING，续期无意义。
    StateMismatch,
    /// owner 或 attemptToken 不匹配，续期请求来自失权 attempt。
    StaleOwner,
}

/// 提交 attempt 结果的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishOutcome {
    /// 提交成功：Run 进入的下一状态、权威时刻与可选唤醒延迟。
    Ok {
        /// 提交后 Run 的下一状态。
        next_state: JobState,
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 供调度器重新唤醒的延迟毫秒；无则为 `None`。
        wake_delay_ms: Option<i64>,
    },
    /// Run 不在 RUNNING，提交无效。
    StateMismatch,
    /// owner 或 attemptToken 不匹配，迟到提交不能覆盖新 attempt。
    StaleOwner,
    /// Fanout assignment epoch 不匹配。
    StaleAssignment,
    /// Fanout 记录中的稳定执行身份与提交方派生值不一致。
    IdentityMismatch,
}

/// 恢复到期 Run 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverOutcome {
    /// 恢复成功：撤销旧 owner 后 Run 进入的下一状态与可选唤醒延迟。
    Ok {
        /// 恢复后 Run 的下一状态。
        next_state: JobState,
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 供调度器重新唤醒的延迟毫秒；无则为 `None`。
        wake_delay_ms: Option<i64>,
    },
    /// 租约尚未真正到期，恢复请求过早。
    NotDue {
        /// 当前租约截止毫秒。
        lease_until: i64,
    },
    /// Run 不在 RUNNING 或索引与记录不一致，已清理陈旧索引。
    Stale,
    /// 取消专用恢复观察到 Fanout 根尚未进入 CANCELLING，拒绝撤销执行权。
    RootNotCancelling,
}

/// 一次可见性提升的结果：本轮提升数、下一最小 score 与权威时刻。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    /// 本轮重建 Dispatch 消息并重新可见的 Run 数量。
    pub promoted: i64,
    /// 提升后 visible ZSET 的下一最小 score；为空表示已无待提升项。
    pub next_score: Option<i64>,
    /// 脚本观测到的权威当前毫秒。
    pub redis_now: i64,
}

/// 请求取消普通 Run 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// 取消已受理：未开始 Run 立即终态，运行中 Run 发布协作式取消信号。
    Ok {
        /// 释放串行槽后开放下一队首的唤醒延迟；纯协作式取消无此值。
        wake_delay_ms: Option<i64>,
    },
    /// Run 记录不存在。
    NotFound,
    /// Run 已处于终态，无需取消。
    AlreadyCompleted,
}

/// 收敛未按时提交的 Fanout 根 Run 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailWaitingOutcome {
    /// 收敛成功：Run 进入的下一状态（FAILED）与可选唤醒延迟。
    Ok {
        /// 收敛后 Run 的下一状态。
        next_state: JobState,
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// 供调度器重新唤醒的延迟毫秒；无则为 `None`。
        wake_delay_ms: Option<i64>,
    },
    /// Run 不在 FANOUT_CREATING，收敛无效。
    StateMismatch,
    /// 创建截止点尚未到达，收敛请求过早。
    NotDue {
        /// 当前创建截止点毫秒。
        deadline: i64,
    },
}

/// 延后已拉取 Run 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferOutcome {
    /// 已放回可见索引，返回下次可见延迟毫秒。
    Deferred {
        /// 重新可见的延迟毫秒。
        delay_ms: i64,
    },
    /// Run 不在 QUEUED，延后无效。
    Stale,
    /// Run 已落入定义删除 fence，并在确认当前消息时同步终态化。
    JobDeleted,
}

/// 定义登记的封闭结局；`revision` 为 Redis 中当前实际生效的定义修订号。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRegisterOutcome {
    /// 首次或高修订号登记成功。
    Registered {
        /// 登记后 Redis 中生效的定义修订号。
        revision: i64,
    },
    /// 同修订号同摘要，已存在，无需改写。
    Adopted {
        /// 已存在且摘要一致的定义修订号。
        revision: i64,
    },
    /// 请求修订号低于当前，已拒绝回滚。
    Stale {
        /// Redis 中阻止低修订号覆盖的当前定义修订号。
        revision: i64,
    },
    /// 同修订号但摘要不同，已封闭调度，等待显式管理动作。
    Conflict {
        /// 出现摘要分歧并封闭调度的定义修订号。
        revision: i64,
    },
    /// 定义已被删除，低修订号不能复活。
    Deleted {
        /// Redis 中已删除定义的当前修订号。
        revision: i64,
    },
    /// 已有定义声明了其它 source，不能由当前运行时收养。
    SourceMismatch {
        /// 既有定义绑定的 source 名称。
        current_source: String,
    },
}

/// Completion Stream 一轮双界裁剪结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionTrimOutcome {
    /// 由近似长度界删除的事件数。
    pub length_trimmed: i64,
    /// 由精确时间界删除的事件数；单轮不超过配置批量。
    pub retention_trimmed: i64,
    /// 裁剪后最旧事件 ID；Stream 为空时为 `None`。
    pub oldest_id: Option<String>,
    /// 本轮使用的 Redis 权威毫秒时刻。
    pub redis_now: i64,
}

/// 已删除定义存量的一轮有界收敛结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobReapOutcome {
    /// 本轮实际终态化的 Run 数量。
    pub reaped: i64,
    /// 当前任务仍需立即推进游标、等待租约或继续清理索引。
    pub active: bool,
    /// 已到等待截止点、需由 control loop 跨 slot 推进的 Fanout 根。
    pub(crate) fanout_requests: Vec<JobReapFanoutRequest>,
}

/// 删除收敛中一个到期 Fanout 根的跨 slot 定位证据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JobReapFanoutRequest {
    /// 普通根任务名。
    pub(crate) job_name: String,
    /// 普通根 Run 标识。
    pub(crate) run_id: String,
    /// Fanout 桶内根标识。
    pub(crate) fanout_id: String,
    /// 转移完成权的根 attempt。
    pub(crate) root_attempt: i64,
}

/// Job 定义与 Run 仓库；持有连接、冻结键模型与配置标量。
#[derive(Clone)]
pub struct JobRepository {
    client: Arc<RedisClient>,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
}

impl JobRepository {
    /// 业务作用：返回当前 source 冻结的调度分片数，供命名空间控制面逐分片传播状态。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：布局初始化时冻结的正整数分片数。
    pub(crate) fn shard_count(&self) -> u32 {
        self.keyspace.shard_count()
    }

    /// 业务作用：按冻结 keyspace 计算任务所属调度分片，供同 source 的运行循环筛选本地定义。
    ///
    /// 参数说明：`job_name` 为已校验任务名。
    ///
    /// 返回：小于冻结 `shard_count` 的稳定分片下标。
    pub(crate) fn schedule_shard(&self, job_name: &str) -> u32 {
        self.keyspace.schedule_shard(job_name)
    }

    /// 业务作用：绑定连接、键模型与配置，创建定义与 Run 仓库。
    ///
    /// 参数说明：
    /// - `client`: 承载控制连接的 RedisClient。
    /// - `keyspace`: 冻结的 Job 键模型。
    /// - `config`: 已校验的 Job 配置，提供可见性、协议版本与参数上限。
    ///
    /// 返回：可执行原子登记与手工触发的仓库。
    pub fn new(client: Arc<RedisClient>, keyspace: JobKeyspace, config: Arc<JobConfig>) -> Self {
        Self {
            client,
            keyspace,
            config,
        }
    }

    /// 业务作用：对普通调度分片的 Completion Stream 应用配置的长度上界与最小时间保留窗口。
    ///
    /// 参数说明：`shard` 为调度分片下标。
    ///
    /// 返回：本轮两类删除数、裁剪后最旧 ID 与权威时刻；命令或协议失败时返回错误。
    pub async fn trim_completion(&self, shard: u32) -> Result<CompletionTrimOutcome> {
        self.trim_completion_key(&self.keyspace.completion(shard))
            .await
    }

    /// 业务作用：在 Fanout 桶长期没有新完成写入时继续裁剪 Completion Stream，保证时间界仍然生效。
    ///
    /// 参数说明：`bucket` 为固定 Fanout 桶下标。
    ///
    /// 返回：本轮两类删除数、裁剪后最旧 ID 与权威时刻；命令或协议失败时返回错误。
    pub async fn trim_fanout_completion(&self, bucket: u32) -> Result<CompletionTrimOutcome> {
        self.trim_completion_key(&self.keyspace.fanout_completion_at(bucket))
            .await
    }

    /// 业务作用：对一个已冻结 Completion key 执行有界双界裁剪，并读回可观测的最旧保留 ID。
    ///
    /// 参数说明：`key` 必须由当前 JobKeyspace 的普通分片或 Fanout 桶派生。
    ///
    /// 返回：长度界采用近似有界裁剪；时间界先读安全边界再精确删除，单轮不会超过配置批量。
    async fn trim_completion_key(&self, key: &str) -> Result<CompletionTrimOutcome> {
        let mut connection = self.client.conn();
        let (seconds, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut connection)
            .await
            .map_err(NasaRedisError::Redis)?;
        let redis_now = seconds.saturating_mul(1_000).saturating_add(micros / 1_000);
        let length_trimmed: i64 = redis::cmd("XTRIM")
            .arg(key)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.config.completion_max_len)
            .arg("LIMIT")
            .arg(self.config.completion_trim_batch_size)
            .query_async(&mut connection)
            .await
            .map_err(NasaRedisError::Redis)?;
        let retention_ms = i64::try_from(self.config.completion_retention_ms).unwrap_or(i64::MAX);
        let cutoff = redis_now.saturating_sub(retention_ms).max(0);
        let cutoff_id = format!("{cutoff}-0");
        let expired: StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg(format!("({cutoff_id}"))
            .arg("COUNT")
            .arg(self.config.completion_trim_batch_size.saturating_add(1))
            .query_async(&mut connection)
            .await
            .map_err(NasaRedisError::Redis)?;
        let batch = self.config.completion_trim_batch_size as usize;
        let retention_trimmed = if expired.ids.is_empty() {
            0
        } else {
            // 边界取第 batch+1 个过期 ID；精确 MINID 只删除它之前的至多 batch 条，不让一次清理独占 Redis。
            let boundary = expired
                .ids
                .get(batch)
                .map(|entry| entry.id.clone())
                .unwrap_or(cutoff_id);
            redis::cmd("XTRIM")
                .arg(key)
                .arg("MINID")
                .arg("=")
                .arg(boundary)
                .query_async::<i64>(&mut connection)
                .await
                .map_err(NasaRedisError::Redis)?
        };
        let oldest: StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut connection)
            .await
            .map_err(NasaRedisError::Redis)?;
        Ok(CompletionTrimOutcome {
            length_trimmed,
            retention_trimmed,
            oldest_id: oldest.ids.first().map(|entry| entry.id.clone()),
            redis_now,
        })
    }

    /// 业务作用：在任何定义、执行器或消费组写入前建立并复验跨语言 Job 布局 marker。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：既有指纹完全一致或本节点首次建立时成功；任一持久布局字段不一致时 fail-closed。
    pub async fn confirm_layout(&self) -> Result<()> {
        let fingerprint = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            self.config.protocol_version,
            self.keyspace.qualifier(),
            self.keyspace.namespace(),
            self.keyspace.shard_count(),
            self.keyspace.fanout_bucket_count(),
            self.config.dispatch_group,
            self.config.pubsub_mode.wire_name(),
        );
        let raw = eval(
            &self.client,
            &JOB_LAYOUT,
            &[self.keyspace.layout_marker().into_bytes()],
            &[fingerprint.as_bytes().to_vec()],
        )
        .await?;
        match raw
            .first()
            .map(value_to_string)
            .unwrap_or_default()
            .as_str()
        {
            "OK" => {}
            "MISMATCH" => Err(crate::job::JobError::Protocol(format!(
                "Job layout 与既有 marker 不一致: qualifier={} namespace={}",
                self.keyspace.qualifier(),
                self.keyspace.namespace()
            )))?,
            _ => return Err(protocol("job_layout 返回未知码")),
        }

        let runtime_fingerprint = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.config.protocol_version,
            self.config.lease_ms,
            self.config.visibility_timeout_ms,
            self.config.max_run_duration_ms,
            self.config.max_result_summary_bytes,
            self.config.max_dispatch_attempts,
            self.config.max_serial_backlog,
            self.config.serial_overflow_policy.wire_name(),
            self.config.run_retention_ms,
            self.config.completion_retention_ms,
            self.config.completion_max_len,
            self.config.completion_trim_batch_size,
            self.config.tombstone_retention_ms,
            self.config.max_parameter_bytes,
            self.config.max_catch_up_runs,
            self.config.max_catch_up_window_ms,
            self.config.executor_expire_ms,
            self.config.registry_gc_grace_ms,
            self.config.ready_max_wakeups,
            self.config.fanout_capacity_wait_ms,
            self.config.fanout_max_assignments,
            self.config.node_unready_evidence_count,
            self.config.fanout_max_members,
            self.config.fanout_delivery_batch_size,
            self.config.fanout_create_timeout_ms,
            self.config.fanout_max_wait_ms,
            self.config.fanout_retention_ms,
            self.config.fanout_cleanup_batch_size,
            self.config.fanout_max_total_parameter_bytes,
            self.config.json_default_typing,
            self.config.dispatch_group,
            self.config.pubsub_mode.wire_name(),
            self.keyspace.qualifier(),
            crate::job::trigger::CRON_SEMANTICS_ID,
            crate::job::trigger::CRON_TZDB_VERSION,
        );
        let runtime = eval(
            &self.client,
            &JOB_LAYOUT,
            &[self.keyspace.runtime_layout_marker().into_bytes()],
            &[runtime_fingerprint.into_bytes()],
        )
        .await?;
        match runtime
            .first()
            .map(value_to_string)
            .unwrap_or_default()
            .as_str()
        {
            "OK" => Ok(()),
            "MISMATCH" => Err(crate::job::JobError::Protocol(format!(
                "Job Rust runtime layout 与既有 marker 不一致: qualifier={} namespace={}",
                self.keyspace.qualifier(),
                self.keyspace.namespace()
            ))
            .into()),
            _ => Err(protocol("job_layout companion 返回未知码")),
        }
    }

    /// 业务作用：在建立定义、执行器或消费组前验证每个 Redis master 都支持 Job 数据面所需命令。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：standalone 或稳定 Cluster 的全部 master 完整确认时成功；ACL、命令缺失、拓扑变化或响应不完整时拒绝。
    pub async fn probe_capabilities(&self) -> Result<()> {
        let mut names = vec![
            "TIME",
            "EVALSHA",
            "EVAL",
            "XAUTOCLAIM",
            "XREADGROUP",
            "XGROUP",
            "HSET",
            "ZADD",
            "SADD",
        ];
        match self.config.pubsub_mode {
            crate::job::model::JobPubSubMode::Sharded => {
                names.extend(["SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE"]);
            }
            crate::job::model::JobPubSubMode::Broadcast => {
                names.extend(["PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE"]);
            }
        }
        let supported = match self.client.conn() {
            Conn::Single(mut connection) => {
                let command = command_info(&names);
                command
                    .query_async::<redis::Value>(&mut connection)
                    .await
                    .is_ok_and(|value| command_info_supported(&value, names.len()))
            }
            Conn::Cluster(mut connection) => {
                let mut complete = false;
                for _ in 0..3 {
                    let Some(before) = cluster_primary_addresses(&mut connection).await else {
                        break;
                    };
                    let responses = connection
                        .route_command(
                            command_info(&names),
                            RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None)),
                        )
                        .await;
                    let Some(after) = cluster_primary_addresses(&mut connection).await else {
                        break;
                    };
                    if before != after {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    complete = responses.ok().is_some_and(|value| {
                        all_master_command_info_supported(&value, &before, names.len())
                    });
                    break;
                }
                complete
            }
        };
        if supported {
            Ok(())
        } else {
            Err(crate::job::JobError::Config(format!(
                "RedisJob source {} 的 master 命令能力或 ACL 不完整",
                self.keyspace.qualifier()
            ))
            .into())
        }
    }

    /// 业务作用：以 requestId 幂等创建手工 Run、参数、Dispatch 消息与可见性索引。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义。
    /// - `request_id`: 调用方幂等请求标识，按名称合同校验。
    /// - `payload`: 业务参数载荷，必须匹配已登记 Worker 的线编码与 schema。
    ///
    /// 返回：首次触发返回 Run 与消息 ID；重复 requestId 返回既有 Run；任务/命名空间未启用或不存在返回
    /// 对应结局；载荷超限或契约不符时在发命令前拒绝。
    pub async fn manual_fire(
        &self,
        definition: &JobDefinition,
        request_id: &str,
        payload: &JobPayload,
    ) -> Result<ManualFireOutcome> {
        let request_id = require_name(request_id, "requestId")?;
        // 载荷契约在发命令前复验：超过参数上限或线编码/schema 与登记定义不符的载荷不能进入执行链。
        if payload.bytes().len() > self.config.max_parameter_bytes as usize {
            return Err(cfg("manual fire 参数超过 max_parameter_bytes"));
        }
        if !definition.codecs().contains(&payload.codec()) {
            return Err(cfg("manual fire 载荷线编码不在定义声明的 codecs 中"));
        }
        if payload.schema_id() != definition.schema_id() {
            return Err(cfg("manual fire 载荷 schema_id 与定义不符"));
        }
        let shard = self.keyspace.schedule_shard(definition.name());
        let run_id = manual_run_id(
            self.keyspace.qualifier(),
            self.keyspace.namespace(),
            definition.name(),
            &request_id,
        );
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.run(shard, &run_id).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace
                .dispatch(shard, definition.worker_name())
                .into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
            self.keyspace.control(shard).into_bytes(),
        ];
        let encoded = base64::engine::general_purpose::STANDARD.encode(payload.bytes());
        let argv: Vec<Vec<u8>> = vec![
            run_id.clone().into_bytes(),
            request_id.into_bytes(),
            encoded.into_bytes(),
            payload.schema_id().as_bytes().to_vec(),
            payload.codec().wire_name().as_bytes().to_vec(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            self.config.protocol_version.to_string().into_bytes(),
            shard.to_string().into_bytes(),
            definition.name().as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &MANUAL_FIRE, &keys, &argv).await?;
        interpret_manual_fire(&raw, run_id)
    }

    /// 业务作用：按 Run 标识读取只读投影，用于触发复核与状态观测。
    ///
    /// 参数说明：
    /// - `job_name`: 任务名（决定分片）。
    /// - `run_id`: Run 标识。
    ///
    /// 返回：Run 存在时返回投影；不存在返回 `None`；字段数或状态不符合合同时 fail-closed。
    pub async fn read_run(&self, job_name: &str, run_id: &str) -> Result<Option<JobRun>> {
        let shard = self.keyspace.schedule_shard(job_name);
        let key = self.keyspace.run(shard, run_id).into_bytes();
        let raw = eval(&self.client, &READ_RUN, &[key], &[]).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(JobRun::from_fields(&raw)?))
    }

    /// 业务作用：有界扫描一个调度分片内已到期的任务及其定义修订号，并读回下一最小 score 供自适应节奏。
    ///
    /// 参数说明：
    /// - `shard`: 调度分片下标。
    /// - `limit`: 单次候选上限；调用方按扫描批量配置传入。
    ///
    /// 返回：权威当前时刻、下一最小 score 与到期项列表；脚本只读，真正触发仍由 `fire_due` 逐项复验。
    pub async fn scan_schedule_due(&self, shard: u32, limit: u32) -> Result<ScheduleDueScan> {
        let keys = vec![
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.control(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            limit.to_string().into_bytes(),
            b"SCHEDULE".to_vec(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &SCAN_DUE, &keys, &argv).await?;
        interpret_schedule_scan(&raw)
    }

    /// 业务作用：扫描一个调度分片内已到期的普通 Run 租约，供恢复脚本撤销失权 owner。
    ///
    /// 参数说明：
    /// - `shard`: 调度分片下标。
    ///
    /// 返回：权威时间、下一 score 与有界到期 Run 标识；扫描只读，不直接改变执行权。
    pub async fn scan_leases_due(&self, shard: u32) -> Result<DueIndexScan> {
        self.scan_index_due(self.keyspace.leases(shard)).await
    }

    /// 业务作用：扫描一个调度分片内已到期的 Fanout 创建 waiting 根，供创建中断恢复路径收敛。
    ///
    /// 参数说明：
    /// - `shard`: 调度分片下标。
    ///
    /// 返回：权威时间、下一 score 与有界到期 Run 标识；扫描只读。
    pub async fn scan_waiting_due(&self, shard: u32) -> Result<DueIndexScan> {
        self.scan_index_due(self.keyspace.waiting(shard)).await
    }

    /// 业务作用：按已知分片读取 Run，避免恢复索引只有 runId 时重新猜测任务分片。
    ///
    /// 参数说明：
    /// - `shard`: 已由 lease/waiting 索引确定的分片。
    /// - `run_id`: Run 标识。
    ///
    /// 返回：Run 存在时返回投影；不存在返回 `None`；字段异常 fail-closed。
    pub async fn read_run_at_shard(&self, shard: u32, run_id: &str) -> Result<Option<JobRun>> {
        let raw = eval(
            &self.client,
            &READ_RUN,
            &[self.keyspace.run(shard, run_id).into_bytes()],
            &[],
        )
        .await?;
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(JobRun::from_fields(&raw)?))
    }

    /// 业务作用：用 Redis TIME 只读扫描任意 Job ZSET 的到期成员，统一 lease 与 waiting 恢复证据格式。
    ///
    /// 参数说明：
    /// - `key`: 已由当前 source 键模型派生的索引 key。
    ///
    /// 返回：有界到期成员；返回结构不符合二元组步长时 fail-closed。
    async fn scan_index_due(&self, key: String) -> Result<DueIndexScan> {
        let argv = vec![
            self.config.scan_batch_size.to_string().into_bytes(),
            Vec::new(),
            Vec::new(),
        ];
        let raw = eval(&self.client, &SCAN_DUE, &[key.into_bytes()], &argv).await?;
        interpret_index_scan(&raw)
    }

    /// 业务作用：在任务分片内原子复验到期逻辑时刻，创建唯一自动 Run、Dispatch 消息与可见性索引，并推进下一调度时刻。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义（提供修订号、调度类型、能力名与首个线编码）。
    /// - `logical_fire_at`: 本次权威逻辑触发时刻毫秒，必须等于调度 ZSET 中当前 score 才触发。
    /// - `proposed_next_fire_at`: 计算好的下一逻辑触发时刻；`FIXED_DELAY` 传 0（触发后移出调度，待 finish 重挂）。
    /// - `skip_run`: 误触发跳过时为真，只推进调度时刻不创建 Run。
    /// - `misfire`: 记为补偿触发时为真，triggerType 用 `MISFIRE`，据此派生独立 runId。
    ///
    /// 返回：首次创建返回 Run 与消息 ID；重放返回既有 Run；未到期/需重算/状态或修订不符时返回对应结局，均不覆盖新 attempt。
    pub async fn fire_due(
        &self,
        definition: &JobDefinition,
        logical_fire_at: i64,
        proposed_next_fire_at: i64,
        skip_run: bool,
        misfire: bool,
    ) -> Result<FireDueOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let schedule_type = definition.schedule_type().wire_name();
        // triggerType 决定 runId 派生：补偿触发与常规触发即使同一逻辑时刻也必须是不同 Run。
        let trigger_type = if misfire {
            "MISFIRE"
        } else {
            schedule_type
        };
        let run_id = scheduled_run_id(
            self.keyspace.qualifier(),
            self.keyspace.namespace(),
            definition.name(),
            logical_fire_at,
            trigger_type,
        );
        // 首个声明的线编码作为本次 Run 的编码；定义登记时保证 codecs 至少含一个成员。
        let codec = definition
            .codecs()
            .first()
            .ok_or_else(|| cfg("fire due 定义缺少线编码"))?
            .wire_name();
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.run(shard, &run_id).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace
                .dispatch(shard, definition.worker_name())
                .into_bytes(),
            self.keyspace.control(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            definition.definition_revision().to_string().into_bytes(),
            logical_fire_at.to_string().into_bytes(),
            proposed_next_fire_at.to_string().into_bytes(),
            run_id.clone().into_bytes(),
            schedule_type.as_bytes().to_vec(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            self.config.protocol_version.to_string().into_bytes(),
            definition.name().as_bytes().to_vec(),
            shard.to_string().into_bytes(),
            codec.as_bytes().to_vec(),
            schedule_type.as_bytes().to_vec(),
            if skip_run {
                b"SKIP".to_vec()
            } else {
                b"RUN".to_vec()
            },
            trigger_type.as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &FIRE_DUE, &keys, &argv).await?;
        interpret_fire_due(&raw, run_id)
    }

    /// 业务作用：在一个调度分片内批量提交经过链式计算的到期项，每项独立 CAS 且共享一次 Redis 时间。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片；全部项必须属于该分片。
    /// - `items`: 批量触发项，各自携带定义与计算好的时刻/标志。
    ///
    /// 返回：与输入等长的结果序列，逐项给出派生 Run 标识、状态码与消息 ID；返回结构不符合合同时 fail-closed。
    pub async fn fire_due_batch(
        &self,
        shard: u32,
        items: &[FireDueBatchItem<'_>],
    ) -> Result<Vec<FireDueBatchResult>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut keys: Vec<Vec<u8>> = vec![
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.control(shard).into_bytes(),
        ];
        let mut argv: Vec<Vec<u8>> = vec![items.len().to_string().into_bytes()];
        let mut run_ids: Vec<String> = Vec::with_capacity(items.len());
        for item in items {
            let definition = item.definition;
            let schedule_type = definition.schedule_type().wire_name();
            let trigger_type = if item.misfire {
                "MISFIRE"
            } else {
                schedule_type
            };
            let run_id = scheduled_run_id(
                self.keyspace.qualifier(),
                self.keyspace.namespace(),
                definition.name(),
                item.logical_fire_at,
                trigger_type,
            );
            let codec = definition
                .codecs()
                .first()
                .ok_or_else(|| cfg("fire due batch 定义缺少线编码"))?
                .wire_name();
            keys.push(self.keyspace.job(shard, definition.name()).into_bytes());
            keys.push(self.keyspace.run(shard, &run_id).into_bytes());
            keys.push(
                self.keyspace
                    .dispatch(shard, definition.worker_name())
                    .into_bytes(),
            );
            argv.extend([
                definition.definition_revision().to_string().into_bytes(),
                item.logical_fire_at.to_string().into_bytes(),
                item.proposed_next_fire_at.to_string().into_bytes(),
                run_id.clone().into_bytes(),
                schedule_type.as_bytes().to_vec(),
                self.config.visibility_timeout_ms.to_string().into_bytes(),
                self.config.protocol_version.to_string().into_bytes(),
                definition.name().as_bytes().to_vec(),
                shard.to_string().into_bytes(),
                codec.as_bytes().to_vec(),
                if item.skip_run {
                    b"SKIP".to_vec()
                } else {
                    b"RUN".to_vec()
                },
                trigger_type.as_bytes().to_vec(),
                if item.must_end_in_future {
                    b"1".to_vec()
                } else {
                    b"0".to_vec()
                },
            ]);
            run_ids.push(run_id);
        }
        let raw = eval(&self.client, &FIRE_DUE_BATCH, &keys, &argv).await?;
        // 返回为 {redisNow, (code, messageId)*}；逐项按序解出状态码与消息 ID。
        let mut results = Vec::with_capacity(items.len());
        for (index, run_id) in run_ids.into_iter().enumerate() {
            let code_at = 1 + index * 2;
            let code = raw
                .get(code_at)
                .map(value_to_string)
                .ok_or_else(|| protocol("fire_due_batch 返回项数不足"))?;
            let message_id = raw
                .get(code_at + 1)
                .map(value_to_string)
                .unwrap_or_default();
            results.push(FireDueBatchResult {
                run_id,
                code,
                message_id,
            });
        }
        Ok(results)
    }

    /// 业务作用：一次续期同 slot 的多个执行权，并逐项返回新截止与取消信号；每项独立 CAS 且共享一次 Redis 时间。
    ///
    /// 参数说明：
    /// - `owner`: 当前 owner，全部项共用；与各记录不符的项返回 `STALE_OWNER`。
    /// - `items`: 同 slot 续期项，各自携带记录/租约/等待键、索引成员与 token。
    ///
    /// 返回：与输入等长的结果序列，逐项给出状态码、新截止与取消标志；返回结构不符合合同时 fail-closed。
    pub async fn renew_batch(
        &self,
        owner: &str,
        items: &[RenewBatchItem],
    ) -> Result<Vec<RenewBatchResult>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(items.len() * 3);
        let mut argv: Vec<Vec<u8>> = vec![items.len().to_string().into_bytes()];
        for item in items {
            keys.push(item.record_key.clone().into_bytes());
            keys.push(item.leases_key.clone().into_bytes());
            keys.push(item.waiting_key.clone().into_bytes());
            argv.extend([
                item.member.clone().into_bytes(),
                owner.as_bytes().to_vec(),
                item.attempt_token.to_string().into_bytes(),
                self.config.lease_ms.to_string().into_bytes(),
            ]);
        }
        let raw = eval(&self.client, &RENEW_BATCH, &keys, &argv).await?;
        // 返回为 {redisNow, (code, deadline, cancelRequestedAt)*}；逐项解出三元组。
        let mut results = Vec::with_capacity(items.len());
        for index in 0..items.len() {
            let base = 1 + index * 3;
            let code = raw
                .get(base)
                .map(value_to_string)
                .ok_or_else(|| protocol("renew_batch 返回项数不足"))?;
            let deadline = raw
                .get(base + 1)
                .map(value_to_string)
                .and_then(|text| text.parse::<i64>().ok())
                .unwrap_or(0);
            let cancel_requested = !raw
                .get(base + 2)
                .map(value_to_string)
                .unwrap_or_default()
                .is_empty();
            results.push(RenewBatchResult {
                code,
                deadline,
                cancel_requested,
            });
        }
        Ok(results)
    }

    /// 业务作用：原子领取一个普通 Run 的 attempt 执行权、分配单调 fencing token 与租约，并确认当前 Dispatch 消息。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义，提供并发、协议、合同、schema 与线编码复验依据。
    /// - `run_id`: 目标 Run 标识。
    /// - `executor_id`: 本地执行器身份，成为新 owner。
    /// - `message_id`: 当前 Dispatch Stream 消息 ID，随领取一并 ACK/删除。
    ///
    /// 返回：成功返回 Started/Adopted 及 attempt、token、租约；任务门禁、协议或合同不满足返回对应结局，
    /// 均不越权开放第二个执行权。
    pub async fn start_run(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        executor_id: &str,
        message_id: &str,
    ) -> Result<StartOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.running(shard).into_bytes(),
            self.keyspace.waitq(shard, definition.name()).into_bytes(),
            self.keyspace.fences(shard).into_bytes(),
            self.keyspace
                .dispatch(shard, definition.worker_name())
                .into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.control(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            definition.name().as_bytes().to_vec(),
            run_id.as_bytes().to_vec(),
            executor_id.as_bytes().to_vec(),
            message_id.as_bytes().to_vec(),
            self.config.lease_ms.to_string().into_bytes(),
            definition.concurrency().wire_name().as_bytes().to_vec(),
            self.config.max_serial_backlog.to_string().into_bytes(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            self.config.dispatch_group.as_bytes().to_vec(),
            self.config.run_retention_ms.to_string().into_bytes(),
            b"NORMAL".to_vec(),
            self.config
                .serial_overflow_policy
                .wire_name()
                .as_bytes()
                .to_vec(),
            self.config.protocol_version.to_string().into_bytes(),
            definition.definition_revision().to_string().into_bytes(),
            definition.contract_revision().to_string().into_bytes(),
            definition.schema_id().as_bytes().to_vec(),
            definition.wire_codecs().as_bytes().to_vec(),
            self.keyspace.shard_prefix(shard).into_bytes(),
            self.keyspace.qualifier().as_bytes().to_vec(),
            self.config.max_scan_interval_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &START_RUN, &keys, &argv).await?;
        interpret_start(&raw)
    }

    /// 业务作用：续期当前 attempt 的执行租约，并把取消请求随响应捎回。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义（决定分片）。
    /// - `run_id`: 目标 Run 标识。
    /// - `executor_id`: 当前 owner，必须与记录一致才能续期。
    /// - `attempt_token`: 当前 fencing token，必须与记录一致。
    ///
    /// 返回：续期成功返回新截止与取消标志；owner/token 不符或状态非运行态时返回对应结局，失权 attempt 不能续期。
    pub async fn renew_run(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        executor_id: &str,
        attempt_token: i64,
    ) -> Result<RenewOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            executor_id.as_bytes().to_vec(),
            attempt_token.to_string().into_bytes(),
            self.config.lease_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &RENEW_RUN, &keys, &argv).await?;
        interpret_renew(&raw)
    }

    /// 业务作用：提交当前 attempt 结果，原子释放执行权并按重试上限进入重试或终态。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义，提供重试上限与延迟基数。
    /// - `run_id`: 目标 Run 标识。
    /// - `executor_id`: 当前 owner，必须与记录一致。
    /// - `attempt_token`: 当前 fencing token，必须与记录一致。
    /// - `result_code`: Handler 结果码。
    /// - `summary`: 结果摘要，写入前按 `max_result_summary_bytes` 在字符边界截断。
    ///
    /// 返回：提交成功返回下一状态与可选唤醒延迟；owner/token 不符或状态非运行态时返回对应结局，迟到提交不覆盖新 attempt。
    pub async fn finish_run(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        executor_id: &str,
        attempt_token: i64,
        result_code: JobResultCode,
        summary: &str,
    ) -> Result<FinishOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
            self.keyspace.running(shard).into_bytes(),
            self.keyspace.waitq(shard, definition.name()).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
            self.keyspace
                .dispatch(shard, definition.worker_name())
                .into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
        ];
        let retry_delay = self.retry_delay(definition.retry_delay_ms(), run_id);
        let bounded_summary = limit_summary(summary, self.config.max_result_summary_bytes as usize);
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            executor_id.as_bytes().to_vec(),
            attempt_token.to_string().into_bytes(),
            definition.name().as_bytes().to_vec(),
            result_code.wire_name().as_bytes().to_vec(),
            bounded_summary.into_bytes(),
            definition.max_attempts().to_string().into_bytes(),
            retry_delay.to_string().into_bytes(),
            self.config.run_retention_ms.to_string().into_bytes(),
            b"NORMAL".to_vec(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            b"0".to_vec(),
            b"0".to_vec(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &FINISH_RUN, &keys, &argv).await?;
        interpret_finish(&raw)
    }

    /// 业务作用：在服务端确认租约已到期后撤销旧 owner 权威，并把 Run 推进到重试或终态。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义，提供重试上限与延迟基数。
    /// - `run_id`: 目标 Run 标识。
    ///
    /// 返回：恢复成功返回下一状态与可选唤醒延迟；租约未到期返回 `NotDue`；状态或索引不一致返回 `Stale`。
    pub async fn recover_expired(
        &self,
        definition: &JobDefinition,
        run_id: &str,
    ) -> Result<RecoverOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.running(shard).into_bytes(),
            self.keyspace.waitq(shard, definition.name()).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
        ];
        let retry_delay = self.retry_delay(definition.retry_delay_ms(), run_id);
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            definition.name().as_bytes().to_vec(),
            definition.max_attempts().to_string().into_bytes(),
            retry_delay.to_string().into_bytes(),
            self.config.run_retention_ms.to_string().into_bytes(),
            b"NORMAL".to_vec(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            b"0".to_vec(),
            b"0".to_vec(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &RECOVER_EXPIRED, &keys, &argv).await?;
        interpret_recover(&raw)
    }

    /// 业务作用：本地暂无执行容量或缺少兼容 Handler 时，把已拉取的 QUEUED Run 放回可见索引并确认当前消息。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义（决定分片与 Dispatch Stream）。
    /// - `run_id`: 目标 Run 标识。
    /// - `message_id`: 当前 Dispatch Stream 消息 ID，随延后一并 ACK/删除。
    ///
    /// 返回：延后成功返回下次可见延迟；Run 不在 QUEUED 返回 `Stale`；删除 fence 命中返回 `JobDeleted`。
    pub async fn defer_run(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        message_id: &str,
    ) -> Result<DeferOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace
                .dispatch(shard, definition.worker_name())
                .into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            message_id.as_bytes().to_vec(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
            self.config.dispatch_group.as_bytes().to_vec(),
            self.config.run_retention_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &DEFER_RUN, &keys, &argv).await?;
        interpret_defer(&raw)
    }

    /// 业务作用：在本节点没有任务定义时，仍按消息携带的任务名和实际 Stream 原子确认消息并恢复持久可见性。
    ///
    /// 参数说明：
    /// - `job_name`: 消息声明的任务名，用于定位 Run 所在分片。
    /// - `run_id`: 目标 Run 标识。
    /// - `message_id`: 当前消费组消息标识。
    /// - `stream`: 实际读取消息的 Dispatch Stream，禁止由不可信 worker 字段重新拼接。
    ///
    /// 返回：成功延后返回新的可见延迟；Run 已不可延后时返回 `Stale`；删除 fence 命中返回 `JobDeleted`。
    pub(crate) async fn defer_stream(
        &self,
        job_name: &str,
        run_id: &str,
        message_id: &str,
        stream: &str,
    ) -> Result<DeferOutcome> {
        let shard = self.keyspace.schedule_shard(job_name);
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            stream.as_bytes().to_vec(),
            self.keyspace.job(shard, job_name).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            message_id.as_bytes().to_vec(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
            self.config.dispatch_group.as_bytes().to_vec(),
            self.config.run_retention_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &DEFER_RUN, &keys, &argv).await?;
        interpret_defer(&raw)
    }

    /// 业务作用：扫描一个分片内到期的可见成员，为仍可执行的 Run 重建可能丢失的 Dispatch 消息。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片。
    ///
    /// 返回：本轮提升数量、下一最小 score 与权威时刻；HASH 与 ZSET 截止点不一致的陈旧成员被清理而不重放。
    pub async fn promote_visible(&self, shard: u32) -> Result<Promotion> {
        let keys = vec![
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.control(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            self.config.scan_batch_size.to_string().into_bytes(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            self.config.max_dispatch_attempts.to_string().into_bytes(),
            self.config.max_scan_interval_ms.to_string().into_bytes(),
            self.keyspace.shard_prefix(shard).into_bytes(),
            self.config.run_retention_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &PROMOTE_VISIBLE, &keys, &argv).await?;
        interpret_promotion(&raw)
    }

    /// 业务作用：读取单个调度分片当前可见性积压量，供 source 聚合容量观测。
    ///
    /// 参数说明：`shard` 为冻结布局内的调度分片下标。
    ///
    /// 返回：当前 visible ZSET 成员数；传输或类型异常时返回错误并交由扫描监督处理。
    pub async fn visible_size(&self, shard: u32) -> Result<i64> {
        let mut connection = self.client.conn();
        redis::cmd("ZCARD")
            .arg(self.keyspace.visible(shard))
            .query_async(&mut connection)
            .await
            .map_err(NasaRedisError::Redis)
    }

    /// 业务作用：读取一个串行任务等待队列的当前深度，供本地 source 发布容量压力而不暴露任务名 label。
    ///
    /// 参数说明：`job_name` 为已冻结定义名，调用方负责只查询当前分片的串行定义。
    ///
    /// 返回：waitq ZSET 当前成员数；传输或类型异常时返回错误并交由扫描监督处理。
    pub(crate) async fn serial_wait_depth(&self, job_name: &str) -> Result<i64> {
        let shard = self.keyspace.schedule_shard(job_name);
        let mut connection = self.client.conn();
        redis::cmd("ZCARD")
            .arg(self.keyspace.waitq(shard, job_name))
            .query_async(&mut connection)
            .await
            .map_err(NasaRedisError::Redis)
    }

    /// 业务作用：请求取消一个普通 Run；未开始 Run 立即收敛为终态，运行中 Run 发布协作式取消信号由续期传递。
    ///
    /// 参数说明：
    /// - `definition`: 目标任务定义（决定分片与串行队列）。
    /// - `run_id`: 目标 Run 标识。
    ///
    /// 返回：取消受理返回 `Ok`（可能带释放串行槽后的唤醒延迟）；Run 不存在或已终态返回对应结局，
    /// 运行中 Run 不被伪造为已停止。
    pub async fn request_cancel(
        &self,
        definition: &JobDefinition,
        run_id: &str,
    ) -> Result<CancelOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
            self.keyspace.running(shard).into_bytes(),
            self.keyspace.waitq(shard, definition.name()).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            definition.name().as_bytes().to_vec(),
            self.config.run_retention_ms.to_string().into_bytes(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &REQUEST_CANCEL, &keys, &argv).await?;
        interpret_cancel(&raw)
    }

    /// 业务作用：在跨 slot Fanout 桶未于创建截止点前提交时，把普通根 Run 收敛为失败并释放串行槽。
    ///
    /// 参数说明：
    /// - `definition`: 根任务定义（决定分片与串行队列）。
    /// - `run_id`: 普通根 Run 标识。
    ///
    /// 返回：确已到期返回 `Ok(FAILED)`；Run 不在 FANOUT_CREATING 返回 `StateMismatch`；截止点未到返回 `NotDue`。
    pub async fn fail_waiting_creation(
        &self,
        definition: &JobDefinition,
        run_id: &str,
    ) -> Result<FailWaitingOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
            self.keyspace.running(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.waitq(shard, definition.name()).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            definition.name().as_bytes().to_vec(),
            self.config.run_retention_ms.to_string().into_bytes(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &FAIL_WAITING_CREATION, &keys, &argv).await?;
        interpret_fail_waiting(&raw)
    }

    /// 业务作用：暂停一个任务的新自动触发，并从调度索引移除；已取得的执行权不受影响。
    ///
    /// 参数说明：
    /// - `job_name`: 目标任务名。
    ///
    /// 返回：定义存在时置为 PAUSED 并返回 `Ok`；不存在返回 `NotFound`。
    pub async fn pause(&self, job_name: &str) -> Result<DefinitionControlOutcome> {
        let shard = self.keyspace.schedule_shard(job_name);
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.job(shard, job_name).into_bytes(),
        ];
        let raw = eval(
            &self.client,
            &JOB_PAUSE,
            &keys,
            &[job_name.as_bytes().to_vec()],
        )
        .await?;
        interpret_control(&raw)
    }

    /// 业务作用：从调用方计算的新逻辑时刻恢复任务触发；DELETED/CONFLICT 不能借普通 resume 绕过门禁。
    ///
    /// 参数说明：
    /// - `job_name`: 目标任务名。
    /// - `next_fire_at`: 恢复后的下一逻辑触发时刻毫秒，零表示无自动时刻。
    ///
    /// 返回：ENABLED/PAUSED 定义恢复并返回 `Ok`；不存在返回 `NotFound`；DELETED/CONFLICT 返回 `StateMismatch`。
    pub async fn resume(
        &self,
        job_name: &str,
        next_fire_at: i64,
    ) -> Result<DefinitionControlOutcome> {
        let shard = self.keyspace.schedule_shard(job_name);
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.job(shard, job_name).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            job_name.as_bytes().to_vec(),
            next_fire_at.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &JOB_RESUME, &keys, &argv).await?;
        interpret_control(&raw)
    }

    /// 业务作用：以单调修订号删除任务并保留 tombstone，阻止旧进程用过期配置重新登记。
    ///
    /// 参数说明：
    /// - `job_name`: 目标任务名。
    /// - `revision`: 删除修订号；低于当前修订号时拒绝。
    ///
    /// 返回：删除成功返回 `Ok` 与权威时刻；不存在返回 `NotFound`；删除修订号偏低返回 `Stale` 与当前修订号。
    pub async fn delete(&self, job_name: &str, revision: i64) -> Result<DeleteOutcome> {
        let shard = self.keyspace.schedule_shard(job_name);
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.jobs(shard).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.job(shard, job_name).into_bytes(),
            self.keyspace.reaping(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            job_name.as_bytes().to_vec(),
            revision.to_string().into_bytes(),
            self.config.tombstone_retention_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &JOB_DELETE, &keys, &argv).await?;
        interpret_delete(&raw)
    }

    /// 业务作用：经显式管理动作选择当前持久摘要，解除 CONFLICT 并按新基准恢复调度。
    ///
    /// 参数说明：
    /// - `definition`: 管理者选定的目标定义，提供修订号与摘要基准。
    /// - `next_fire_at`: 解除后的下一逻辑触发时刻毫秒。
    ///
    /// 返回：解除成功返回 `Ok`；不存在返回 `NotFound`；非 CONFLICT 返回 `StateMismatch`；修订号或摘要不符返回 `Stale`。
    pub async fn resolve_conflict(
        &self,
        definition: &JobDefinition,
        next_fire_at: i64,
    ) -> Result<DefinitionControlOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            definition.name().as_bytes().to_vec(),
            definition.definition_revision().to_string().into_bytes(),
            definition.definition_digest().as_bytes().to_vec(),
            JobDefinitionState::Enabled.wire_name().as_bytes().to_vec(),
            next_fire_at.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &JOB_RESOLVE_CONFLICT, &keys, &argv).await?;
        interpret_control(&raw)
    }

    /// 业务作用：设置一个调度分片的普通 Run 命名空间门禁；新触发、未领取 Run 与可见性重建均在权威脚本内复验该状态。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片（整命名空间暂停需对全部分片逐一设置）。
    /// - `state`: 目标门禁状态，仅 `Enabled`/`Paused` 有效。
    /// - `actor`: 操作来源，用于审计。
    ///
    /// 返回：合法状态返回 `Ok` 与权威时刻；其它状态返回 `Invalid`；该门禁阻止普通 Run 的新执行权，不撤销已有 attempt。
    pub async fn set_namespace_state(
        &self,
        shard: u32,
        state: JobDefinitionState,
        actor: &str,
    ) -> Result<NamespaceStateOutcome> {
        let actor = require_name(actor, "actor")?;
        let key = self.keyspace.control(shard).into_bytes();
        let argv: Vec<Vec<u8>> = vec![state.wire_name().as_bytes().to_vec(), actor.into_bytes()];
        let raw = eval(&self.client, &NAMESPACE_SET_STATE, &[key], &argv).await?;
        interpret_namespace_state(&raw)
    }

    /// 业务作用：读取一个调度分片当前的命名空间门禁快照，供逐分片治理操作的收敛核对。
    ///
    /// 只读控制 HASH 的三个既有字段，不触碰任何脚本或写路径，与 Java 侧共享布局保持逐字节兼容。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片。
    ///
    /// 返回：三字段的原样快照；分片从未被治理操作触达时三者均为 `None`(消费方按缺省 ENABLED 解释)。
    pub async fn namespace_state_snapshot(&self, shard: u32) -> Result<NamespaceShardSnapshot> {
        let key = self.keyspace.control(shard);
        let mut connection = self.client.conn();
        let (state, updated_at_ms, updated_by): (Option<String>, Option<i64>, Option<String>) =
            redis::cmd("HMGET")
                .arg(&key)
                .arg("namespaceState")
                .arg("updatedAt")
                .arg("updatedBy")
                .query_async(&mut connection)
                .await
                .map_err(NasaRedisError::Redis)?;
        Ok(NamespaceShardSnapshot {
            shard,
            state,
            updated_at_ms,
            updated_by,
        })
    }

    /// 业务作用：有界回收一个分片内 tombstone 已到期的定义枚举与 fencing 字段；保留期内绝不删除。
    ///
    /// 参数说明：
    /// - `shard`: 目标调度分片。
    ///
    /// 返回：本轮回收的定义数量；结构不符合脚本合同时 fail-closed。
    pub async fn cleanup_tombstones(&self, shard: u32) -> Result<i64> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.jobs(shard).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.fences(shard).into_bytes(),
            self.keyspace.reaping(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            self.config.scan_batch_size.to_string().into_bytes(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &JOB_CLEANUP_TOMBSTONES, &keys, &argv).await?;
        raw.first()
            .map(value_to_string)
            .ok_or_else(|| protocol("job_cleanup_tombstones 返回结构非法"))?
            .parse::<i64>()
            .map_err(|_| protocol("job_cleanup_tombstones 数量非法"))
    }

    /// 业务作用：按固定批次终态化已删除任务遗留在 waitq 与共享可见/租约/等待索引的 Run，使删除控制动作保持常数复杂度。
    ///
    /// 参数说明：`shard` 为调度分片下标；任务名由分片 reaping SET 在脚本内选择。
    ///
    /// 返回：本批终态化数量、继续推进标志与到期 Fanout 根；跨 slot 请求必须在当前 control 周期内先关闭桶内执行权。
    pub async fn reap_deleted(&self, shard: u32) -> Result<JobReapOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.reaping(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            self.config.scan_batch_size.to_string().into_bytes(),
            self.config.run_retention_ms.to_string().into_bytes(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &JOB_REAP, &keys, &argv).await?;
        if raw.len() < 3 || (raw.len() - 3) % 3 != 0 {
            return Err(protocol("job_reap 返回结构非法"));
        }
        let job_name = value_to_string(&raw[0]);
        let reaped = value_to_string(&raw[1])
            .parse::<i64>()
            .map_err(|_| protocol("job_reap reapedCount 非法"))?;
        let active = match value_to_string(&raw[2]).as_str() {
            "0" => false,
            "1" => true,
            _ => return Err(protocol("job_reap active 非法")),
        };
        let mut fanout_requests = Vec::with_capacity((raw.len() - 3) / 3);
        for fields in raw[3..].as_chunks::<3>().0 {
            let run_id = value_to_string(&fields[0]);
            let fanout_id = value_to_string(&fields[1]);
            let root_attempt = value_to_string(&fields[2])
                .parse::<i64>()
                .map_err(|_| protocol("job_reap rootAttempt 非法"))?;
            if job_name.is_empty() || run_id.is_empty() || fanout_id.is_empty() {
                return Err(protocol("job_reap Fanout 定位字段为空"));
            }
            fanout_requests.push(JobReapFanoutRequest {
                job_name: job_name.clone(),
                run_id,
                fanout_id,
                root_attempt,
            });
        }
        Ok(JobReapOutcome {
            reaped,
            active,
            fanout_requests,
        })
    }

    /// 业务作用：把一个 RUNNING 普通根 Run 原子转入 FANOUT_CREATING，把完成权威从普通 Handler 移交跨 slot 对账流程。
    ///
    /// 参数说明：
    /// - `definition`: 根任务定义（决定分片）。
    /// - `run_id`: 根 Run 标识。
    /// - `owner`/`attempt_token`: 当前执行权，必须与记录一致才能转入。
    /// - `fanout_id`/`snapshot_id`/`shard_total`: 本批 Fanout 标识、快照标识与分片总数。
    ///
    /// 返回：首次转入返回 `Prepared`；同 fanoutId 重放返回 `Adopted`；删除 fence、非 RUNNING 或执行权不符返回对应结局。
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_fanout_root(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        owner: &str,
        attempt_token: i64,
        fanout_id: &str,
        snapshot_id: &str,
        shard_total: i64,
    ) -> Result<PrepareFanoutRootOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.leases(shard).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            owner.as_bytes().to_vec(),
            attempt_token.to_string().into_bytes(),
            fanout_id.as_bytes().to_vec(),
            snapshot_id.as_bytes().to_vec(),
            shard_total.to_string().into_bytes(),
            self.config
                .fanout_create_timeout_ms
                .to_string()
                .into_bytes(),
        ];
        let raw = eval(&self.client, &PREPARE_FANOUT_ROOT, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STATE_MISMATCH" => PrepareFanoutRootOutcome::StateMismatch,
            "STALE_OWNER" => PrepareFanoutRootOutcome::StaleOwner,
            "JOB_DELETED" => PrepareFanoutRootOutcome::JobDeleted,
            "ADOPTED" => PrepareFanoutRootOutcome::Adopted {
                deadline: raw
                    .get(1)
                    .map(value_to_string)
                    .ok_or_else(|| protocol("prepare_fanout_root 缺少 deadline"))?
                    .parse::<i64>()
                    .map_err(|_| protocol("prepare_fanout_root deadline 非法"))?,
            },
            "PREPARED" => PrepareFanoutRootOutcome::Prepared {
                redis_now: raw
                    .get(1)
                    .map(value_to_string)
                    .ok_or_else(|| protocol("prepare_fanout_root 缺少 redisNow"))?
                    .parse::<i64>()
                    .map_err(|_| protocol("prepare_fanout_root redisNow 非法"))?,
                deadline: raw
                    .get(2)
                    .map(value_to_string)
                    .ok_or_else(|| protocol("prepare_fanout_root 缺少 deadline"))?
                    .parse::<i64>()
                    .map_err(|_| protocol("prepare_fanout_root deadline 非法"))?,
            },
            _ => return Err(protocol("prepare_fanout_root 返回未知码")),
        })
    }

    /// 业务作用：把桶内 Fanout 的 COMMITTED 或终态幂等回填到普通根 Run，终态时释放串行槽并唤醒下一队首。
    ///
    /// 参数说明：
    /// - `definition`: 根任务定义（决定分片与串行队列）。
    /// - `run_id`: 根 Run 标识。
    /// - `fanout_id`/`root_attempt`: 本批 Fanout 标识与根 attempt，必须与记录一致。
    /// - `target_state`: 回填目标，`COMMITTED` 转入 WAITING_CHILDREN，或桶内终态线名。
    /// - `error_type`/`result_summary`: 终态时写入的错误类型与结果摘要。
    ///
    /// 返回：回填成功返回状态与时刻；删除 fence 命中返回 `JobDeleted`；已回填、定位不符或根已终态返回对应结局。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_fanout_root(
        &self,
        definition: &JobDefinition,
        run_id: &str,
        fanout_id: &str,
        root_attempt: i64,
        target_state: &str,
        error_type: &str,
        result_summary: &str,
    ) -> Result<FinishFanoutRootOutcome> {
        self.finish_fanout_root_for_job(
            definition.name(),
            run_id,
            fanout_id,
            root_attempt,
            target_state,
            error_type,
            result_summary,
        )
        .await
    }

    /// 业务作用：不依赖本地定义对象，按任务名回填删除收敛发现的普通 Fanout 根。
    ///
    /// 参数说明：`job_name` 定位调度分片与串行队列；其余参数携带普通根、桶终态与结果归因。
    ///
    /// 返回：语义与 `finish_fanout_root` 相同；供定义已从本节点移除后的 control loop 继续收敛。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn finish_fanout_root_for_job(
        &self,
        job_name: &str,
        run_id: &str,
        fanout_id: &str,
        root_attempt: i64,
        target_state: &str,
        error_type: &str,
        result_summary: &str,
    ) -> Result<FinishFanoutRootOutcome> {
        let shard = self.keyspace.schedule_shard(job_name);
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.run(shard, run_id).into_bytes(),
            self.keyspace.waiting(shard).into_bytes(),
            self.keyspace.running(shard).into_bytes(),
            self.keyspace.visible(shard).into_bytes(),
            self.keyspace.waitq(shard, job_name).into_bytes(),
            self.keyspace.completion(shard).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            run_id.as_bytes().to_vec(),
            fanout_id.as_bytes().to_vec(),
            root_attempt.to_string().into_bytes(),
            target_state.as_bytes().to_vec(),
            error_type.as_bytes().to_vec(),
            self.config.fanout_max_wait_ms.to_string().into_bytes(),
            result_summary.as_bytes().to_vec(),
            job_name.as_bytes().to_vec(),
            self.config.run_retention_ms.to_string().into_bytes(),
            self.config.visibility_timeout_ms.to_string().into_bytes(),
            self.keyspace.shard_prefix(shard).into_bytes(),
        ];
        let raw = eval(&self.client, &FINISH_FANOUT_ROOT, &keys, &argv).await?;
        interpret_finish_fanout_root(&raw)
    }

    /// 业务作用：按既有规则计算 Run 的重试可见延迟；抖动用 Run 标识的历史字符串哈希，被最大执行时长钳制。
    ///
    /// 参数说明：
    /// - `base`: 定义声明的重试延迟基数毫秒。
    /// - `run_id`: 参与抖动派生的 Run 标识。
    ///
    /// 返回：`min(base + floorMod(hash(runId), max(1, base/5)), max_run_duration_ms)`，与既有节点写入的 visible score 一致。
    fn retry_delay(&self, base: u64, run_id: &str) -> i64 {
        let base = base as i64;
        let span = (base / 5).max(1);
        let jitter = (compat_string_hash(run_id) as i64).rem_euclid(span);
        (base + jitter).min(self.config.max_run_duration_ms as i64)
    }

    /// 业务作用：原子登记一个定义，并按状态与下一触发时刻维护调度 ZSET。
    ///
    /// 参数说明：
    /// - `definition`: 已冻结并校验的任务定义。
    /// - `next_fire_at`: 逻辑下一触发时刻毫秒；`ENABLED` 且大于 0 时进入调度，否则移出调度。
    ///
    /// 返回：登记结局与当前生效修订号；返回结构或返回码不符合脚本合同时 fail-closed。
    pub async fn register(
        &self,
        definition: &JobDefinition,
        next_fire_at: i64,
    ) -> Result<JobRegisterOutcome> {
        let shard = self.keyspace.schedule_shard(definition.name());
        // 三个 key 都由同分片 hash tag 派生，保证 SADD/HSET/ZADD 在一段 Lua 内跨 key 原子提交。
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.jobs(shard).into_bytes(),
            self.keyspace.schedule(shard).into_bytes(),
            self.keyspace.job(shard, definition.name()).into_bytes(),
        ];
        let codec_field = definition.wire_codecs().to_owned();
        let argv: Vec<Vec<u8>> = vec![
            definition.name().as_bytes().to_vec(),
            definition.definition_revision().to_string().into_bytes(),
            definition.definition_digest().as_bytes().to_vec(),
            JobDefinitionState::Enabled.wire_name().as_bytes().to_vec(),
            shard.to_string().into_bytes(),
            definition.worker_name().as_bytes().to_vec(),
            definition.worker_key().as_bytes().to_vec(),
            definition.trigger().wire_name().as_bytes().to_vec(),
            definition.schedule_type().wire_name().as_bytes().to_vec(),
            definition.cron().as_bytes().to_vec(),
            definition.zone().as_bytes().to_vec(),
            definition.interval_ms().to_string().into_bytes(),
            definition.concurrency().wire_name().as_bytes().to_vec(),
            definition.misfire().wire_name().as_bytes().to_vec(),
            definition.timeout_ms().to_string().into_bytes(),
            definition.max_attempts().to_string().into_bytes(),
            definition.retry_delay_ms().to_string().into_bytes(),
            definition.contract_revision().to_string().into_bytes(),
            definition.schema_id().as_bytes().to_vec(),
            codec_field.into_bytes(),
            definition
                .fanout_receipt_timeout_ms()
                .to_string()
                .into_bytes(),
            definition
                .fanout_receipt_max_retries()
                .to_string()
                .into_bytes(),
            definition
                .fanout_failure_policy()
                .wire_name()
                .as_bytes()
                .to_vec(),
            next_fire_at.to_string().into_bytes(),
            self.keyspace.qualifier().as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &JOB_REGISTER, &keys, &argv).await?;
        interpret_register(&raw)
    }

    /// 业务作用：读取已登记定义的核心事实，用于登记复核与冲突诊断。
    ///
    /// 参数说明：
    /// - `job_name`: 任务名。
    ///
    /// 返回：定义存在时返回状态、修订号与摘要快照；不存在返回 `None`；字段缺失或非法时 fail-closed。
    pub async fn definition_record(&self, job_name: &str) -> Result<Option<JobDefinitionRecord>> {
        let shard = self.keyspace.schedule_shard(job_name);
        let key = self.keyspace.job(shard, job_name);
        let mut conn = self.client.conn();
        let raw: Vec<Option<Vec<u8>>> = redis::cmd("HMGET")
            .arg(key.as_bytes())
            .arg("state")
            .arg("definitionRevision")
            .arg("definitionDigest")
            .arg("nextFireAt")
            .query_async(&mut conn)
            .await
            .map_err(NasaRedisError::Redis)?;
        if raw.iter().all(Option::is_none) {
            return Ok(None);
        }
        let state_text = utf8(raw.first().cloned().flatten())?;
        let state = JobDefinitionState::parse(&state_text)
            .ok_or_else(|| protocol("job 定义 state 字段非法"))?;
        let revision = parse_i64(raw.get(1).cloned().flatten())?;
        let digest = utf8(raw.get(2).cloned().flatten())?;
        let next_fire_at = parse_i64(raw.get(3).cloned().flatten())?;
        Ok(Some(JobDefinitionRecord {
            state,
            revision,
            digest,
            next_fire_at,
        }))
    }
}

/// 已登记定义的只读核心事实。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDefinitionRecord {
    /// 定义状态。
    pub state: JobDefinitionState,
    /// 当前生效修订号。
    pub revision: i64,
    /// 规范定义摘要。
    pub digest: String,
    /// 当前调度下一触发时刻毫秒；未调度时为 0。
    pub next_fire_at: i64,
}

/// 业务作用：把登记脚本原始返回解释为封闭结局；返回结构或返回码不符合合同时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回的 `{code, revision}` 二元素。
///
/// 返回：已知返回码返回对应结局；元素数或未知码返回协议错误。
fn interpret_register(raw: &[redis::Value]) -> Result<JobRegisterOutcome> {
    if raw.len() != 2 {
        return Err(protocol("job_register 返回结构非法"));
    }
    let code = value_to_string(&raw[0]);
    if code == "SOURCE_MISMATCH" {
        return Ok(JobRegisterOutcome::SourceMismatch {
            current_source: value_to_string(&raw[1]),
        });
    }
    let revision = value_to_string(&raw[1])
        .parse::<i64>()
        .map_err(|_| protocol("job_register 修订号非法"))?;
    Ok(match code.as_str() {
        "OK" => JobRegisterOutcome::Registered { revision },
        "ADOPTED" => JobRegisterOutcome::Adopted { revision },
        "STALE" => JobRegisterOutcome::Stale { revision },
        "CONFLICT" => JobRegisterOutcome::Conflict { revision },
        "DELETED" => JobRegisterOutcome::Deleted { revision },
        _ => return Err(protocol("job_register 返回未知码")),
    })
}

/// 业务作用：把手工触发脚本原始返回解释为封闭结局；结构或返回码不符合合同时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
/// - `run_id`: 请求派生的 Run 标识，用于 OK/ADOPTED 结局。
///
/// 返回：已知返回码返回对应结局；OK 缺少消息 ID 或未知码返回协议错误。
fn interpret_manual_fire(raw: &[redis::Value], run_id: String) -> Result<ManualFireOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "NOT_FOUND" => Ok(ManualFireOutcome::NotFound),
        "STATE_MISMATCH" => Ok(ManualFireOutcome::StateMismatch),
        "ADOPTED" => Ok(ManualFireOutcome::Adopted { run_id }),
        "OK" => {
            if raw.len() != 4 {
                return Err(protocol("manual_fire OK 返回结构非法"));
            }
            Ok(ManualFireOutcome::Fired {
                run_id,
                message_id: value_to_string(&raw[3]),
            })
        }
        _ => Err(protocol("manual_fire 返回未知码")),
    }
}

/// 业务作用：把 scan_due 的 SCHEDULE 模式返回解释为扫描结果；门禁未知或结构不符合合同时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回 `{redisNow, nextScore, namespaceState, (member, score, revision)*}`。
///
/// 返回：解析出的权威时刻、命名空间门禁、下一 score 与到期项；头部缺失或成员三元组不齐返回协议错误。
fn interpret_schedule_scan(raw: &[redis::Value]) -> Result<ScheduleDueScan> {
    if raw.len() < 3 {
        return Err(protocol("scan_due 返回缺少调度头"));
    }
    let redis_now = value_to_string(&raw[0])
        .parse::<i64>()
        .map_err(|_| protocol("scan_due redisNow 非法"))?;
    let next_text = value_to_string(&raw[1]);
    let next_score = if next_text.is_empty() {
        None
    } else {
        Some(
            next_text
                .parse::<i64>()
                .map_err(|_| protocol("scan_due nextScore 非法"))?,
        )
    };
    let namespace_enabled = value_to_string(&raw[2]) == JobDefinitionState::Enabled.wire_name();
    let body = &raw[3..];
    if !body.len().is_multiple_of(3) {
        return Err(protocol("scan_due 成员三元组不完整"));
    }
    let mut entries = Vec::with_capacity(body.len() / 3);
    for chunk in body.as_chunks::<3>().0 {
        entries.push(DueEntry {
            job_name: value_to_string(&chunk[0]),
            logical_fire_at: value_to_string(&chunk[1])
                .parse::<i64>()
                .map_err(|_| protocol("scan_due score 非法"))?,
            definition_revision: value_to_string(&chunk[2])
                .parse::<i64>()
                .map_err(|_| protocol("scan_due definitionRevision 非法"))?,
        });
    }
    Ok(ScheduleDueScan {
        redis_now,
        next_score,
        namespace_enabled,
        entries,
    })
}

/// 业务作用：把 scan_due 通用模式返回解释为到期索引投影，避免恢复路径按字符串猜测字段位置。
///
/// 参数说明：
/// - `raw`: `{redisNow, nextScore, (member, score)*}` 原始数组。
///
/// 返回：解析后的索引扫描；时间、score 非法或成员二元组不完整时返回协议错误。
fn interpret_index_scan(raw: &[redis::Value]) -> Result<DueIndexScan> {
    if raw.len() < 2 {
        return Err(protocol("scan_due 通用模式缺少时间头"));
    }
    let redis_now = value_to_string(&raw[0])
        .parse::<i64>()
        .map_err(|_| protocol("scan_due 通用模式 redisNow 非法"))?;
    let next = value_to_string(&raw[1]);
    let next_score = if next.is_empty() {
        None
    } else {
        Some(
            next.parse::<i64>()
                .map_err(|_| protocol("scan_due 通用模式 nextScore 非法"))?,
        )
    };
    let body = &raw[2..];
    if !body.len().is_multiple_of(2) {
        return Err(protocol("scan_due 通用模式成员二元组不完整"));
    }
    let mut entries = Vec::with_capacity(body.len() / 2);
    for pair in body.as_chunks::<2>().0 {
        entries.push(DueIndexEntry {
            member: value_to_string(&pair[0]),
            score: value_to_string(&pair[1])
                .parse::<i64>()
                .map_err(|_| protocol("scan_due 通用模式 score 非法"))?,
        });
    }
    Ok(DueIndexScan {
        redis_now,
        next_score,
        entries,
    })
}

/// 业务作用：把 fire_due 原始返回解释为封闭结局；结构或返回码不符合合同时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
/// - `run_id`: 调用方派生的 Run 标识，用于 ADOPTED/OK 结局。
///
/// 返回：已知返回码返回对应结局；缺失随附字段或未知码返回协议错误。
fn interpret_fire_due(raw: &[redis::Value], run_id: String) -> Result<FireDueOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    let now_at = |index: usize| -> Result<i64> {
        raw.get(index)
            .map(value_to_string)
            .ok_or_else(|| protocol("fire_due 缺少 redisNow"))?
            .parse::<i64>()
            .map_err(|_| protocol("fire_due redisNow 非法"))
    };
    Ok(match code.as_str() {
        "STATE_MISMATCH" => FireDueOutcome::StateMismatch,
        "STALE" => FireDueOutcome::Stale,
        "NOT_DUE" => FireDueOutcome::NotDue {
            redis_now: now_at(1)?,
        },
        "NEED_RECOMPUTE" => FireDueOutcome::NeedRecompute {
            redis_now: now_at(1)?,
        },
        "SKIPPED" => FireDueOutcome::Skipped {
            redis_now: now_at(1)?,
            next_fire_at: raw
                .get(2)
                .map(value_to_string)
                .ok_or_else(|| protocol("fire_due SKIPPED 缺少 nextFireAt"))?
                .parse::<i64>()
                .map_err(|_| protocol("fire_due nextFireAt 非法"))?,
        },
        "ADOPTED" => FireDueOutcome::Adopted { run_id },
        "OK" => FireDueOutcome::Fired {
            run_id,
            redis_now: now_at(1)?,
            message_id: raw
                .get(2)
                .map(value_to_string)
                .ok_or_else(|| protocol("fire_due OK 缺少 messageId"))?,
        },
        _ => return Err(protocol("fire_due 返回未知码")),
    })
}

/// 业务作用：把 start_run 原始返回解释为封闭结局；成功返回缺字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：已知返回码返回对应结局；Started/Adopted 缺随附字段或未知码返回协议错误。
pub(crate) fn interpret_start(raw: &[redis::Value]) -> Result<StartOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    let number = |index: usize, field: &str| -> Result<i64> {
        raw.get(index)
            .map(value_to_string)
            .ok_or_else(|| protocol("start_run 成功返回字段缺失"))?
            .parse::<i64>()
            .map_err(|_| protocol(&format!("start_run {field} 字段非法")))
    };
    let started_fields = || -> Result<(i64, i64, i64, i64)> {
        Ok((
            number(1, "attempt")?,
            number(2, "attemptToken")?,
            number(3, "redisNow")?,
            number(4, "leaseUntil")?,
        ))
    };
    Ok(match code.as_str() {
        "STARTED" => {
            let (attempt, attempt_token, redis_now, lease_until) = started_fields()?;
            StartOutcome::Started {
                attempt,
                attempt_token,
                redis_now,
                lease_until,
            }
        }
        "ADOPTED" => {
            let (attempt, attempt_token, redis_now, lease_until) = started_fields()?;
            StartOutcome::Adopted {
                attempt,
                attempt_token,
                redis_now,
                lease_until,
            }
        }
        "NOT_COMMITTED" => StartOutcome::NotCommitted,
        "STALE_ASSIGNMENT" => StartOutcome::StaleAssignment,
        "NOT_FOUND" => StartOutcome::NotFound,
        "DEFERRED" => StartOutcome::Deferred,
        "LEASE_EXPIRED" => StartOutcome::LeaseExpired,
        "JOB_DELETED" => StartOutcome::JobDeleted,
        "PROTOCOL_UNSUPPORTED" => StartOutcome::ProtocolUnsupported,
        "SOURCE_MISMATCH" => StartOutcome::SourceMismatch,
        "IDENTITY_MISMATCH" => StartOutcome::IdentityMismatch,
        "CONTRACT_MISMATCH" => StartOutcome::ContractMismatch,
        "CONFLICT" => StartOutcome::Conflict,
        // start_run 对 QUEUED 之外的非终态返回 STATE_MISMATCH，语义与 STALE 一致（Run 已不可领取）。
        "STALE" | "STATE_MISMATCH" => StartOutcome::Stale,
        "CANCELLED" => StartOutcome::Cancelled,
        "SKIPPED" => StartOutcome::Skipped,
        "BLOCKED" => StartOutcome::Blocked,
        "FENCING_REGRESSION" => StartOutcome::FencingRegression,
        _ => return Err(protocol("start_run 返回未知码")),
    })
}

/// 业务作用：把 renew_run 原始返回解释为封闭结局；成功返回缺字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回新截止与取消标志；已知拒绝码返回对应结局；缺字段或未知码返回协议错误。
pub(crate) fn interpret_renew(raw: &[redis::Value]) -> Result<RenewOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "STATE_MISMATCH" => Ok(RenewOutcome::StateMismatch),
        "STALE_OWNER" => Ok(RenewOutcome::StaleOwner),
        "OK" => {
            let redis_now = raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("renew_run 缺少 redisNow"))?
                .parse::<i64>()
                .map_err(|_| protocol("renew_run redisNow 非法"))?;
            let deadline = raw
                .get(2)
                .map(value_to_string)
                .ok_or_else(|| protocol("renew_run 缺少 deadline"))?
                .parse::<i64>()
                .map_err(|_| protocol("renew_run deadline 非法"))?;
            // cancelRequestedAt 非空即表示已收到取消请求，与既有实现按“字段是否为空”判定一致。
            let cancel_requested = !raw
                .get(3)
                .map(value_to_string)
                .unwrap_or_default()
                .is_empty();
            Ok(RenewOutcome::Ok {
                redis_now,
                deadline,
                cancel_requested,
            })
        }
        _ => Err(protocol("renew_run 返回未知码")),
    }
}

/// 业务作用：把 finish_run 原始返回解释为封闭结局；OK 缺字段或状态非法时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回下一状态、时刻与可选唤醒延迟；已知拒绝码返回对应结局；未知码返回协议错误。
pub(crate) fn interpret_finish(raw: &[redis::Value]) -> Result<FinishOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "STATE_MISMATCH" => Ok(FinishOutcome::StateMismatch),
        "STALE_OWNER" => Ok(FinishOutcome::StaleOwner),
        "STALE_ASSIGNMENT" => Ok(FinishOutcome::StaleAssignment),
        "IDENTITY_MISMATCH" => Ok(FinishOutcome::IdentityMismatch),
        "OK" => {
            let (next_state, redis_now, wake_delay_ms) = parse_completion(raw)?;
            Ok(FinishOutcome::Ok {
                next_state,
                redis_now,
                wake_delay_ms,
            })
        }
        _ => Err(protocol("finish_run 返回未知码")),
    }
}

/// 业务作用：把 recover_expired 原始返回解释为封闭结局；OK 缺字段或状态非法时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回下一状态；NOT_DUE 返回租约截止；STALE 返回陈旧；取消模式根状态变化返回 `RootNotCancelling`；未知码返回协议错误。
pub(crate) fn interpret_recover(raw: &[redis::Value]) -> Result<RecoverOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "STALE" => Ok(RecoverOutcome::Stale),
        "ROOT_NOT_CANCELLING" => Ok(RecoverOutcome::RootNotCancelling),
        "NOT_DUE" => Ok(RecoverOutcome::NotDue {
            lease_until: raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("recover_expired 缺少 leaseUntil"))?
                .parse::<i64>()
                .map_err(|_| protocol("recover_expired leaseUntil 非法"))?,
        }),
        "OK" => {
            let (next_state, redis_now, wake_delay_ms) = parse_completion(raw)?;
            Ok(RecoverOutcome::Ok {
                next_state,
                redis_now,
                wake_delay_ms,
            })
        }
        _ => Err(protocol("recover_expired 返回未知码")),
    }
}

/// 业务作用：解析 finish/recover 共用的 `{OK, nextState, redisNow, optionalWakeDelayMs}` 尾部字段。
///
/// 参数说明：
/// - `raw`: 以 OK 开头的返回数组。
///
/// 返回：下一状态、权威时刻与可选唤醒延迟；状态非法或时刻缺失时返回协议错误。
fn parse_completion(raw: &[redis::Value]) -> Result<(JobState, i64, Option<i64>)> {
    let next_state = JobState::parse(&raw.get(1).map(value_to_string).unwrap_or_default())
        .ok_or_else(|| protocol("完成返回 nextState 非法"))?;
    let redis_now = raw
        .get(2)
        .map(value_to_string)
        .ok_or_else(|| protocol("完成返回缺少 redisNow"))?
        .parse::<i64>()
        .map_err(|_| protocol("完成返回 redisNow 非法"))?;
    let wake_delay_ms = match raw.get(3).map(value_to_string) {
        Some(text) if !text.is_empty() => Some(
            text.parse::<i64>()
                .map_err(|_| protocol("完成返回 wakeDelay 非法"))?,
        ),
        _ => None,
    };
    Ok((next_state, redis_now, wake_delay_ms))
}

/// 业务作用：把 defer_run 原始返回解释为封闭结局；DEFERRED 缺字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：DEFERRED 返回下次可见延迟；STALE 返回陈旧；JOB_DELETED 返回删除终态；未知码返回协议错误。
fn interpret_defer(raw: &[redis::Value]) -> Result<DeferOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "STALE" => Ok(DeferOutcome::Stale),
        "JOB_DELETED" => Ok(DeferOutcome::JobDeleted),
        "DEFERRED" => Ok(DeferOutcome::Deferred {
            delay_ms: raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("defer_run 缺少 delayMs"))?
                .parse::<i64>()
                .map_err(|_| protocol("defer_run delayMs 非法"))?,
        }),
        _ => Err(protocol("defer_run 返回未知码")),
    }
}

/// 业务作用：把 promote_visible 的 `{promotedCount, nextScore, redisNow}` 解释为提升结果；结构不符时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回三元组。
///
/// 返回：提升数、下一 score 与权威时刻；缺字段或非数值返回协议错误。
fn interpret_promotion(raw: &[redis::Value]) -> Result<Promotion> {
    if raw.len() < 3 {
        return Err(protocol("promote_visible 返回结构非法"));
    }
    let promoted = value_to_string(&raw[0])
        .parse::<i64>()
        .map_err(|_| protocol("promote_visible promoted 非法"))?;
    let next_text = value_to_string(&raw[1]);
    let next_score = if next_text.is_empty() {
        None
    } else {
        Some(
            next_text
                .parse::<i64>()
                .map_err(|_| protocol("promote_visible nextScore 非法"))?,
        )
    };
    let redis_now = value_to_string(&raw[2])
        .parse::<i64>()
        .map_err(|_| protocol("promote_visible redisNow 非法"))?;
    Ok(Promotion {
        promoted,
        next_score,
        redis_now,
    })
}

/// 业务作用：把 request_cancel 原始返回解释为封闭结局；未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回可选唤醒延迟；NOT_FOUND/ALREADY_COMPLETED 返回对应结局；未知码返回协议错误。
fn interpret_cancel(raw: &[redis::Value]) -> Result<CancelOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "NOT_FOUND" => Ok(CancelOutcome::NotFound),
        "ALREADY_COMPLETED" => Ok(CancelOutcome::AlreadyCompleted),
        "OK" => {
            let wake_delay_ms = match raw.get(1).map(value_to_string) {
                Some(text) if !text.is_empty() => Some(
                    text.parse::<i64>()
                        .map_err(|_| protocol("request_cancel wakeDelay 非法"))?,
                ),
                _ => None,
            };
            Ok(CancelOutcome::Ok { wake_delay_ms })
        }
        _ => Err(protocol("request_cancel 返回未知码")),
    }
}

/// 业务作用：把 fail_waiting_creation 原始返回解释为封闭结局；OK 缺字段或状态非法时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回下一状态与可选唤醒延迟；STATE_MISMATCH/NOT_DUE 返回对应结局；未知码返回协议错误。
fn interpret_fail_waiting(raw: &[redis::Value]) -> Result<FailWaitingOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "STATE_MISMATCH" => Ok(FailWaitingOutcome::StateMismatch),
        "NOT_DUE" => Ok(FailWaitingOutcome::NotDue {
            deadline: raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("fail_waiting_creation 缺少 deadline"))?
                .parse::<i64>()
                .map_err(|_| protocol("fail_waiting_creation deadline 非法"))?,
        }),
        "OK" => {
            let (next_state, redis_now, wake_delay_ms) = parse_completion(raw)?;
            Ok(FailWaitingOutcome::Ok {
                next_state,
                redis_now,
                wake_delay_ms,
            })
        }
        _ => Err(protocol("fail_waiting_creation 返回未知码")),
    }
}

/// 业务作用：把定义控制脚本的简单文本返回解释为共享结局；未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组（首元素为状态码）。
///
/// 返回：已知返回码返回对应结局；未知码返回协议错误。
fn interpret_control(raw: &[redis::Value]) -> Result<DefinitionControlOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    Ok(match code.as_str() {
        "OK" => DefinitionControlOutcome::Ok,
        "NOT_FOUND" => DefinitionControlOutcome::NotFound,
        "STATE_MISMATCH" => DefinitionControlOutcome::StateMismatch,
        "STALE" => DefinitionControlOutcome::Stale,
        _ => return Err(protocol("定义控制脚本返回未知码")),
    })
}

/// 业务作用：把 finish_fanout_root 原始返回解释为封闭结局；OK/ALREADY_COMPLETED 缺字段或状态非法时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：已知返回码返回对应结局；状态非法或未知码返回协议错误。
fn interpret_finish_fanout_root(raw: &[redis::Value]) -> Result<FinishFanoutRootOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "STALE" => Ok(FinishFanoutRootOutcome::Stale),
        "ADOPTED" => Ok(FinishFanoutRootOutcome::Adopted),
        "STATE_MISMATCH" => Ok(FinishFanoutRootOutcome::StateMismatch),
        "JOB_DELETED" => Ok(FinishFanoutRootOutcome::JobDeleted {
            state: JobState::parse(&raw.get(1).map(value_to_string).unwrap_or_default())
                .ok_or_else(|| protocol("finish_fanout_root 删除状态字段非法"))?,
            deadline: raw
                .get(2)
                .map(value_to_string)
                .ok_or_else(|| protocol("finish_fanout_root 删除截止字段缺失"))?
                .parse::<i64>()
                .map_err(|_| protocol("finish_fanout_root 删除截止字段非法"))?,
        }),
        "ALREADY_COMPLETED" => Ok(FinishFanoutRootOutcome::AlreadyCompleted {
            state: JobState::parse(&raw.get(1).map(value_to_string).unwrap_or_default())
                .ok_or_else(|| protocol("finish_fanout_root 终态字段非法"))?,
        }),
        "OK" => {
            let state = JobState::parse(&raw.get(1).map(value_to_string).unwrap_or_default())
                .ok_or_else(|| protocol("finish_fanout_root state 字段非法"))?;
            let redis_now_or_deadline = raw
                .get(2)
                .map(value_to_string)
                .ok_or_else(|| protocol("finish_fanout_root 缺少时刻"))?
                .parse::<i64>()
                .map_err(|_| protocol("finish_fanout_root 时刻非法"))?;
            let wake_delay_ms = match raw.get(3).map(value_to_string) {
                Some(text) if !text.is_empty() => Some(
                    text.parse::<i64>()
                        .map_err(|_| protocol("finish_fanout_root wakeDelay 非法"))?,
                ),
                _ => None,
            };
            Ok(FinishFanoutRootOutcome::Ok {
                state,
                redis_now_or_deadline,
                wake_delay_ms,
            })
        }
        _ => Err(protocol("finish_fanout_root 返回未知码")),
    }
}

/// 业务作用：把 job_delete 原始返回解释为封闭结局；OK/STALE 缺随附字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回权威时刻；STALE 返回当前修订号；NOT_FOUND 返回对应结局；未知码返回协议错误。
fn interpret_delete(raw: &[redis::Value]) -> Result<DeleteOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "NOT_FOUND" => Ok(DeleteOutcome::NotFound),
        "STALE" => Ok(DeleteOutcome::Stale {
            revision: raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("job_delete 缺少当前修订号"))?
                .parse::<i64>()
                .map_err(|_| protocol("job_delete 修订号非法"))?,
        }),
        "OK" => Ok(DeleteOutcome::Ok {
            redis_now: raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("job_delete 缺少 redisNow"))?
                .parse::<i64>()
                .map_err(|_| protocol("job_delete redisNow 非法"))?,
        }),
        _ => Err(protocol("job_delete 返回未知码")),
    }
}

/// 业务作用：把 namespace_set_state 原始返回解释为封闭结局；OK 缺字段或未知码时 fail-closed。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
///
/// 返回：OK 返回权威时刻；INVALID 返回对应结局；未知码返回协议错误。
fn interpret_namespace_state(raw: &[redis::Value]) -> Result<NamespaceStateOutcome> {
    let code = raw.first().map(value_to_string).unwrap_or_default();
    match code.as_str() {
        "INVALID" => Ok(NamespaceStateOutcome::Invalid),
        "OK" => Ok(NamespaceStateOutcome::Ok {
            redis_now: raw
                .get(1)
                .map(value_to_string)
                .ok_or_else(|| protocol("namespace_set_state 缺少 redisNow"))?
                .parse::<i64>()
                .map_err(|_| protocol("namespace_set_state redisNow 非法"))?,
        }),
        _ => Err(protocol("namespace_set_state 返回未知码")),
    }
}

/// 业务作用：把结果摘要按字节上限在 UTF-8 字符边界截断，避免写入半个多字节字符。
///
/// 参数说明：
/// - `value`: 原始摘要。
/// - `max_bytes`: 允许的最大字节数。
///
/// 返回：不超过上限且不切断字符的摘要；空串或本就不超限时原样返回。
fn limit_summary(value: &str, max_bytes: usize) -> String {
    if value.is_empty() || value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut length = max_bytes;
    while length > 0 && !value.is_char_boundary(length) {
        length -= 1;
    }
    value[..length].to_owned()
}

/// 业务作用：构造一条批量 `COMMAND INFO` 探测命令，保持命令名集合不进入日志或动态标签。
///
/// 参数说明：
/// - `names`: 当前 pubsub 模式所需的完整命令名列表。
///
/// 返回：可发送到单节点或 AllMasters 路由的 Redis 命令。
fn command_info(names: &[&str]) -> redis::Cmd {
    let mut command = redis::cmd("COMMAND");
    command.arg("INFO");
    for name in names {
        command.arg(name);
    }
    command
}

/// 业务作用：复验单个 master 对全部命令都返回非 nil 定义，未知命令不能被部分成功遮蔽。
///
/// 参数说明：
/// - `value`: `COMMAND INFO` 原始响应。
/// - `expected`: 期望命令定义数量。
///
/// 返回：数组长度精确匹配且每项均为命令定义时为真。
fn command_info_supported(value: &redis::Value, expected: usize) -> bool {
    matches!(
        value,
        redis::Value::Array(items)
            if items.len() == expected
                && items.iter().all(|item| !matches!(item, redis::Value::Nil | redis::Value::ServerError(_)))
    )
}

/// 业务作用：复验 Cluster AllMasters 响应地址与稳定拓扑一一对应，并要求每个 master 支持全部命令。
///
/// 参数说明：
/// - `value`: AllMasters 聚合响应。
/// - `expected_addresses`: 探测前后稳定的 master 地址集合。
/// - `command_count`: 每个节点应返回的命令定义数量。
///
/// 返回：无缺失、额外或重复节点且所有能力完整时为真。
fn all_master_command_info_supported(
    value: &redis::Value,
    expected_addresses: &BTreeSet<String>,
    command_count: usize,
) -> bool {
    let redis::Value::Map(entries) = value else {
        return false;
    };
    let mut answered = BTreeSet::new();
    for (address, response) in entries {
        let Some(address) = redis_text(address) else {
            return false;
        };
        if !answered.insert(address) || !command_info_supported(response, command_count) {
            return false;
        }
    }
    answered == *expected_addresses
}

/// 业务作用：在同一 Cluster transport 上读取 distinct master 地址，供能力响应进行完整覆盖复验。
///
/// 参数说明：
/// - `connection`: 已完成认证与拓扑发现的 Cluster 连接。
///
/// 返回：结构完整且非空的地址集合；通配或畸形地址返回 `None`。
async fn cluster_primary_addresses(
    connection: &mut redis::cluster_async::ClusterConnection,
) -> Option<BTreeSet<String>> {
    let mut command = redis::cmd("CLUSTER");
    command.arg("SLOTS");
    let value = connection
        .route_command(
            command,
            RoutingInfo::SingleNode(SingleNodeRoutingInfo::RandomPrimary),
        )
        .await
        .ok()?;
    let redis::Value::Array(rows) = value else {
        return None;
    };
    let mut addresses = BTreeSet::new();
    for row in rows {
        let redis::Value::Array(fields) = row else {
            return None;
        };
        let Some(redis::Value::Array(primary)) = fields.get(2) else {
            return None;
        };
        let host = redis_text(primary.first()?)?;
        let port = match primary.get(1)? {
            redis::Value::Int(port) if (1..=u16::MAX as i64).contains(port) => *port as u16,
            _ => return None,
        };
        if host.is_empty() || host == "?" {
            return None;
        }
        addresses.insert(format!("{host}:{port}"));
    }
    (!addresses.is_empty()).then_some(addresses)
}

/// 业务作用：严格读取 Redis UTF-8 文本，避免有损地址转换让全量覆盖比较误通过。
///
/// 参数说明：
/// - `value`: bulk 或 simple Redis 字符串。
///
/// 返回：合法 UTF-8 文本；其它类型返回 `None`。
fn redis_text(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

/// 业务作用：构造 Job 配置类错误（载荷契约或参数上限不满足）。
///
/// 参数说明：
/// - `message`: 稳定错误摘要。
///
/// 返回：`JobError::Config` 错误。
fn cfg(message: &str) -> NasaRedisError {
    crate::job::JobError::Config(message.to_owned()).into()
}

/// 业务作用：把可选字节解释为 UTF-8 文本；缺失或非法时按协议错误处理。
fn utf8(value: Option<Vec<u8>>) -> Result<String> {
    match value {
        Some(bytes) => String::from_utf8(bytes).map_err(|_| protocol("job 字段不是合法 UTF-8")),
        None => Ok(String::new()),
    }
}

/// 业务作用：把可选字节解释为 i64；缺失取 0，非法时协议错误。
fn parse_i64(value: Option<Vec<u8>>) -> Result<i64> {
    match value {
        None => Ok(0),
        Some(bytes) => String::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .ok_or_else(|| protocol("job 数值字段非法")),
    }
}

/// 业务作用：构造 Job 协议错误。参数说明：`message` 摘要。返回：协议错误。
fn protocol(message: &str) -> NasaRedisError {
    crate::job::JobError::Protocol(message.to_owned()).into()
}
