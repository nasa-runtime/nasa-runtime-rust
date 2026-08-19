//! Fanout 数据模型：在固定 Fanout 桶中建立根 intent、按批幂等写入 shard、提交批次，并按固定字段顺序读回。
//!
//! 同一 `fanout_id` 只能绑定一个 `snapshot_id`；根 HASH、shard HASH 与看门狗索引都落在同一
//! `{qualifier:namespace:fanout:桶}`
//! slot，创建中断后仍有持久恢复入口。`snapshotPayload` 字节与既有实现的能力快照序列化一致，`executionKey`
//! 为 `qualifier:namespace:fanoutId:seq`。

use std::sync::Arc;

use base64::Engine;

use crate::client::RedisClient;
use crate::error::{NasaRedisError, Result};
use crate::job::config::JobConfig;
use crate::job::definition::JobDefinition;
use crate::job::identifiers::shard_run_id;
use crate::job::keyspace::JobKeyspace;
use crate::job::model::{JobResultCode, JobState, JobWireCodec};
use crate::job::registry::{ClusterSnapshot, ExecutorMember};
use crate::job::repository::{
    interpret_finish, interpret_recover, interpret_renew, interpret_start, FinishOutcome,
    RecoverOutcome, RenewOutcome, StartOutcome,
};
use crate::job::script::{eval, value_to_string, JobScript};

/// Fanout 定向 inbox 的固定消费组名；同一节点全部 shard inbox 共用它接管 PEL。
const INBOX_GROUP: &str = "redis-job-fanout";

/// 建立 Fanout 根 intent 的脚本。
static FANOUT_BEGIN: JobScript = JobScript::new(include_str!("lua/fanout_begin.lua"));
/// 按批幂等写入 shard 的脚本。
static FANOUT_ADD_SHARDS: JobScript = JobScript::new(include_str!("lua/fanout_add_shards.lua"));
/// 提交 Fanout 根的脚本。
static FANOUT_COMMIT: JobScript = JobScript::new(include_str!("lua/fanout_commit.lua"));
/// 只读 Fanout 根投影的脚本。
static READ_FANOUT_ROOT: JobScript = JobScript::new(include_str!("lua/read_fanout_root.lua"));
/// 只读 Fanout shard 投影的脚本。
static READ_FANOUT_SHARD: JobScript = JobScript::new(include_str!("lua/read_fanout_shard.lua"));
/// 建立 inbox 投递与 receipt 截止的脚本。
static FANOUT_DELIVER_BATCH: JobScript =
    JobScript::new(include_str!("lua/fanout_deliver_batch.lua"));
/// 目标节点确认接收 shard 的脚本。
static FANOUT_ACCEPT_SHARD: JobScript = JobScript::new(include_str!("lua/fanout_accept_shard.lua"));
/// receipt 超时重发通知的脚本。
static FANOUT_RETRY_RECEIPT: JobScript =
    JobScript::new(include_str!("lua/fanout_retry_receipt.lua"));
/// 唤醒已接收未 start 的 shard 的脚本。
static FANOUT_PROMOTE_READY: JobScript =
    JobScript::new(include_str!("lua/fanout_promote_ready.lua"));
/// 本地执行槽暂满时延后已接收 shard 的启动可见时刻。
static FANOUT_DEFER_READY: JobScript = JobScript::new(include_str!("lua/fanout_defer_ready.lua"));
/// 与普通 Run 共用的领取执行权脚本（FANOUT 模式）。
static START_RUN: JobScript = JobScript::new(include_str!("lua/start_run.lua"));
/// 与普通 Run 共用的单 attempt 租约续期脚本。
static RENEW_RUN: JobScript = JobScript::new(include_str!("lua/renew_run.lua"));
/// 与普通 Run 共用的提交结果脚本（FANOUT 模式）。
static FINISH_RUN: JobScript = JobScript::new(include_str!("lua/finish_run.lua"));
/// 看门狗读取根状态并重排的脚本。
static FANOUT_WATCH_ROOT: JobScript = JobScript::new(include_str!("lua/fanout_watch_root.lua"));
/// 标记根已对账的脚本。
static FANOUT_MARK_RECONCILED: JobScript =
    JobScript::new(include_str!("lua/fanout_mark_reconciled.lua"));
/// CAS 推进能力补投游标的脚本。
static FANOUT_ADVANCE_CAPABILITY_CURSOR: JobScript =
    JobScript::new(include_str!("lua/fanout_advance_capability_cursor.lua"));
/// 收敛超时未提交根的脚本。
static FANOUT_FAIL_CREATING: JobScript =
    JobScript::new(include_str!("lua/fanout_fail_creating.lua"));
/// 对账期把不可达 shard 收敛进根终态的脚本。
static FANOUT_AGGREGATE: JobScript = JobScript::new(include_str!("lua/fanout_aggregate.lua"));
/// 把 shard 重分配到新目标的脚本。
static FANOUT_REASSIGN_SHARD: JobScript =
    JobScript::new(include_str!("lua/fanout_reassign_shard.lua"));
/// 有界清理终态 Fanout 的脚本。
static FANOUT_CLEANUP: JobScript = JobScript::new(include_str!("lua/fanout_cleanup.lua"));
/// 有界推进取消批次的脚本。
static FANOUT_CANCEL_BATCH: JobScript = JobScript::new(include_str!("lua/fanout_cancel_batch.lua"));
/// 与普通 Run 共用的租约恢复脚本（FANOUT 模式）。
static RECOVER_EXPIRED: JobScript = JobScript::new(include_str!("lua/recover_expired.lua"));
/// 按桶批量扫描四类到期索引的脚本。
static FANOUT_SCAN_DUE: JobScript = JobScript::new(include_str!("lua/fanout_scan_due.lua"));

/// 建立 Fanout 根 intent 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutBeginOutcome {
    /// 首次建立成功，返回权威时刻。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
    },
    /// 同 `fanout_id` 同 `snapshot_id` 已存在，幂等采用。
    Adopted,
    /// 同 `fanout_id` 绑定了不同 `snapshot_id`，拒绝覆盖。
    Conflict,
}

/// 写入一批 shard 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutAddShardsOutcome {
    /// 写入成功：累计新增数与根记录当前已建立 shard 总数。
    Ok {
        /// 本次调用（跨批次累计）实际新增的 shard 数。
        added: i64,
        /// 根记录中 `createdShardCount` 的当前值。
        created_shard_count: i64,
    },
    /// 根不在 CREATING，拒绝写入。
    StateMismatch,
    /// `snapshot_id` 不一致或同 seq 出现不同 shardIndex，拒绝整批。
    Conflict,
}

/// 提交 Fanout 根的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutCommitOutcome {
    /// 提交成功，返回权威时刻。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
    },
    /// 根已提交或已进入等待子任务，幂等采用。
    Adopted,
    /// 根不在 CREATING，拒绝提交。
    StateMismatch,
    /// 已建立 shard 数与 `shard_total` 不一致，拒绝提交。
    Invalid,
}

/// 一批 shard 投递的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutDeliverOutcome {
    /// 投递成功：本次实际投递数与根记录当前投递游标。
    Ok {
        /// 本次调用（跨批次累计）实际写入 inbox 的 shard 数。
        delivered: i64,
        /// 根记录中 `deliveryCursor` 的当前值。
        delivery_cursor: i64,
    },
    /// 根未提交，拒绝投递。
    NotCommitted,
}

/// 目标节点确认接收 shard 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutAcceptOutcome {
    /// 接收成功：权威时刻与 start 恢复可见截止。
    Ok {
        /// 脚本观测到的权威当前毫秒。
        redis_now: i64,
        /// start 重新可见截止毫秒。
        start_visible_at: i64,
    },
    /// 根未提交，拒绝接收。
    NotCommitted,
    /// 同 assignment 已确认，幂等采用。
    Adopted,
    /// shard 不在 AWAITING_RECEIPT，拒绝接收。
    StateMismatch,
    /// 目标身份或 assignment epoch 不匹配，拒绝接收。
    StaleAssignment,
    /// shard 声明的 source 与当前运行时不一致，拒绝确认。
    SourceMismatch,
    /// shard 协议版本高于当前运行时能力，拒绝确认。
    ProtocolUnsupported,
    /// shard 合同、Schema 或 codec 与本地定义不一致，拒绝确认。
    ContractMismatch,
}

/// receipt 超时重发的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutReceiptRetryOutcome {
    /// 重发成功：当前重发计数与推进后的下一截止毫秒。
    Ok {
        /// 当前累计重发次数。
        count: i64,
        /// 推进后的下一 receipt 截止毫秒。
        next_deadline: i64,
    },
    /// shard 不在 AWAITING_RECEIPT，证据已陈旧。
    Stale,
    /// assignment epoch 不匹配。
    StaleAssignment,
    /// 当前截止点尚未到达。
    NotDue {
        /// 当前 receipt 截止毫秒。
        deadline: i64,
    },
    /// 普通重发已耗尽（非 STRICT_SNAPSHOT），需升级失败策略。
    RetryExhausted {
        /// 已用尽的重发次数。
        count: i64,
    },
}

/// 唤醒已接收未 start 的 shard 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutReadyPromoteOutcome {
    /// 唤醒成功：当前唤醒计数与推进后的下一可见毫秒。
    Ok {
        /// 当前累计唤醒次数。
        count: i64,
        /// 推进后的下一可见毫秒。
        next_visible_at: i64,
    },
    /// shard 不在 RECEIVED，证据已陈旧。
    Stale,
    /// assignment epoch 不匹配。
    StaleAssignment,
    /// 当前可见点尚未到达。
    NotDue {
        /// 当前可见毫秒。
        visible_at: i64,
    },
    /// 本地容量窗口耗尽，需按根失败策略处理但不能归因于节点无响应。
    CapacityExhausted {
        /// 已用尽的唤醒次数。
        count: i64,
    },
    /// 普通唤醒已耗尽（非 STRICT_SNAPSHOT），需升级失败策略。
    WakeupExhausted {
        /// 已用尽的唤醒次数。
        count: i64,
    },
}

/// 已接收 shard 因本地容量不足而延后启动的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutReadyDeferOutcome {
    /// 当前 assignment 保持不变，返回新的启动可见时刻。
    Deferred {
        /// Redis 权威时间计算出的下一可见毫秒。
        next_visible_at: i64,
    },
    /// 当前目标连续等待容量已达到独立上限；非严格策略重新开放根失败策略，严格快照保持固定目标与既有退避。
    CapacityExhausted {
        /// 本 assignment 首次确认容量不足的 Redis 毫秒时刻。
        first_deferred_at: i64,
        /// 已连续等待容量的毫秒数。
        elapsed_ms: i64,
    },
    /// shard 已不在 RECEIVED，当前容量信号不再有效。
    Stale,
    /// 目标身份或 assignment epoch 已变化，拒绝改写新代次。
    StaleAssignment,
}

/// 看门狗读取根状态的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutWatchRootOutcome {
    /// 根存在，返回状态与根 Run 定位信息。
    Ok {
        /// 根状态。
        state: String,
        /// 根 Run 标识。
        root_run_id: String,
        /// 根 attempt。
        root_attempt: i64,
        /// 根任务名。
        root_job_name: String,
        /// 根调度分片。
        root_schedule_shard: i64,
        /// 取消原因；未取消时为空。
        cancel_reason: String,
        /// 错误类型；无错误时为空。
        error_type: String,
    },
    /// 根不存在（已清理）。
    NotFound,
}

/// 标记根已对账的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutMarkReconciledOutcome {
    /// 标记成功，返回对账时刻。
    Ok {
        /// 对账完成毫秒时刻。
        reconciled_at: i64,
    },
    /// 根状态不允许标记对账。
    StateMismatch,
}

/// CAS 推进能力补投游标的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutCapabilityCursorOutcome {
    /// 推进成功，返回新游标。
    Ok {
        /// 推进后的能力补投游标。
        next_cursor: i64,
    },
    /// 根不存在。
    NotFound,
    /// 观察游标与权威不一致，拒绝推进。
    Stale {
        /// 权威当前游标。
        current_cursor: i64,
    },
}

/// 收敛超时未提交根的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutFailCreatingOutcome {
    /// 收敛成功：根终态与保留截止。
    Ok {
        /// 收敛后的根状态（FAILED）。
        state: String,
        /// 终态保留截止毫秒。
        expire_at: i64,
    },
    /// 根不在 CREATING，收敛无效。
    StateMismatch,
    /// 创建截止尚未到达。
    NotDue {
        /// 当前创建截止毫秒。
        deadline: i64,
    },
}

/// 对账期收敛不可达 shard 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutAggregateOutcome {
    /// 收敛成功：根状态（或仍等待）与保留截止（或已终态计数）。
    Ok {
        /// 根状态或 WAITING。
        root_state_or_waiting: String,
        /// 根终态时为保留截止毫秒，否则为已终态 shard 计数。
        expire_at_or_terminal_count: i64,
    },
    /// shard 已终态，无需再次收敛。
    AlreadyCompleted,
}

/// 把 shard 重分配到新目标的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutReassignOutcome {
    /// 重分配成功，返回新 assignment epoch。
    Ok {
        /// 重分配后的 assignment epoch。
        next_epoch: i64,
    },
    /// shard 正在执行（RUNNING），不能重分配。
    Busy,
    /// 预期 assignment epoch 不匹配。
    StaleAssignment,
    /// 已达重分配上限，判无可用执行器。
    NoCapableExecutor,
    /// 容量路由独立预算耗尽，保持当前 assignment 等待下一窗口。
    CapacityRouteExhausted,
    /// 容量路由裁决所需的持久容量证据已过期。
    StaleCapacity,
}

/// 有界清理终态 Fanout 的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutCleanupOutcome {
    /// 本批清理成功，返回删除数与下一游标（仍有剩余）。
    Ok {
        /// 本批删除的 shard 数。
        deleted: i64,
        /// 推进后的清理游标。
        next_cursor: i64,
    },
    /// 全部清理完成，根与索引已删除。
    Completed {
        /// 本批删除的 shard 数。
        deleted: i64,
        /// 完成时的清理游标。
        cursor: i64,
    },
    /// 根状态不允许清理。
    StateMismatch,
    /// 保留期尚未到达。
    NotDue {
        /// 终态保留截止毫秒。
        expire_at: i64,
    },
    /// 根尚未对账，不能清理。
    NotReconciled,
}

/// 有界推进取消批次的封闭结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutCancelBatchOutcome {
    /// 本批已发布协作式取消，返回下一游标（仍在取消中）。
    Cancelling {
        /// 推进后的取消游标。
        next_cursor: i64,
    },
    /// 全部收敛为取消终态，返回保留截止。
    Cancelled {
        /// 终态保留截止毫秒。
        expire_at: i64,
    },
    /// 根已终态，无需取消。
    AlreadyCompleted {
        /// 根当前终态。
        state: String,
    },
    /// 观察 cancelCursor 与权威不一致。
    Stale {
        /// 权威当前取消游标。
        cursor: i64,
    },
}

/// 一个到期索引的扫描结果：下一最小 score 与本批到期成员。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueIndexScan {
    /// 该索引下一最小 score；为空表示无待处理项。
    pub next_score: Option<i64>,
    /// 本批到期的 `(member, score)`，最多 `scan_batch_size` 条。
    pub members: Vec<(String, i64)>,
}

/// 一个 Fanout 桶四类到期索引的批量扫描结果，共享一次 Redis 时间。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutDueScans {
    /// 脚本观测到的权威当前毫秒。
    pub redis_now: i64,
    /// 桶内非终态看门狗根数量。
    pub root_count: i64,
    /// receipt 截止索引扫描。
    pub receipts: DueIndexScan,
    /// ready 截止索引扫描。
    pub ready: DueIndexScan,
    /// 租约截止索引扫描。
    pub leases: DueIndexScan,
    /// 清理截止索引扫描。
    pub gc: DueIndexScan,
}

/// 一次 Fanout 创建冻结的目标 Worker 合同；字段进入根与 shard 持久记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutContract {
    /// Worker 能力名。
    pub worker_name: String,
    /// 契约修订号。
    pub contract_revision: i64,
    /// Schema 标识。
    pub schema_id: String,
    /// 本批唯一线编码。
    pub wire_codec: JobWireCodec,
    /// 目标失联或执行失败时的收敛策略。
    pub failure_policy: crate::job::model::JobFanoutFailurePolicy,
}

/// Fanout 根记录的只读投影（按 `read_fanout_root.lua` 声明顺序）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutRoot {
    /// Fanout 标识。
    pub fanout_id: String,
    /// 根状态（CREATING/COMMITTED/…）。
    pub state: String,
    /// 根 Run 标识。
    pub root_run_id: String,
    /// 根 attempt。
    pub root_attempt: i64,
    /// 根任务名。
    pub root_job_name: String,
    /// 根调度分片。
    pub root_schedule_shard: i64,
    /// Worker 能力名。
    pub worker_name: String,
    /// 契约修订号。
    pub contract_revision: i64,
    /// schema 标识。
    pub schema_id: String,
    /// 线编码名。
    pub wire_codec: String,
    /// 分片总数。
    pub shard_total: i64,
    /// 投递游标。
    pub delivery_cursor: i64,
    /// 接收回执超时毫秒。
    pub receipt_timeout_ms: i64,
    /// 接收回执重发上限。
    pub receipt_max_retries: i64,
    /// 失败策略。
    pub failure_policy: String,
    /// 终态保留截止毫秒；未终态时为 0。
    pub expire_at: i64,
    /// 清理游标。
    pub cleanup_cursor: i64,
    /// 对账完成时刻毫秒；未对账时为 0。
    pub reconciled_at: i64,
    /// 取消原因；未取消时为空。
    pub cancel_reason: String,
    /// 能力补投游标。
    pub capability_cursor: i64,
    /// 取消传播游标。
    pub cancel_cursor: i64,
    /// 创建截止毫秒。
    pub create_deadline_at: i64,
    /// 根记录冻结的语言无关 source id。
    pub scheduler_qualifier: String,
}

/// Fanout shard 记录的只读投影（按 `read_fanout_shard.lua` 声明顺序）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutShard {
    /// Fanout 标识。
    pub fanout_id: String,
    /// 根 Run 标识。
    pub root_run_id: String,
    /// 快照标识。
    pub snapshot_id: String,
    /// Worker 能力名。
    pub worker_name: String,
    /// 契约修订号。
    pub contract_revision: i64,
    /// schema 标识。
    pub schema_id: String,
    /// 线编码名。
    pub wire_codec: String,
    /// 分片序号（枚举位置）。
    pub shard_index: i64,
    /// 分片总数。
    pub shard_total: i64,
    /// 稳定分片序号。
    pub seq: i64,
    /// 多实现共享的业务幂等键（`namespace:fanoutId:seq`）。
    pub execution_key: String,
    /// 目标稳定节点身份。
    pub target_node_identity: String,
    /// 目标启动代次。
    pub target_startup_id: String,
    /// assignment epoch；重分配自增。
    pub assignment_epoch: i64,
    /// 重分配次数。
    pub assignment_count: i64,
    /// shard 状态。
    pub state: String,
    /// 当前 attempt。
    pub attempt: i64,
    /// 当前 attempt 的 fencing token；未领取为空。
    pub attempt_token: String,
    /// 当前 owner；未领取为空。
    pub owner: String,
    /// 租约截止毫秒；未领取为 0。
    pub lease_until: i64,
    /// Base64 参数载荷。
    pub parameter_payload: String,
    /// inbox 投递消息 ID。
    pub inbox_message_id: String,
    /// 回执重发次数。
    pub receipt_retry_count: i64,
    /// ready 唤醒次数。
    pub ready_wakeup_count: i64,
    /// 发起执行器身份。
    pub origin_executor_id: String,
    /// 接收回执截止毫秒。
    pub receipt_deadline_at: i64,
    /// 恢复可见截止毫秒。
    pub start_visible_at: i64,
    /// 目标心跳修订号。
    pub target_heartbeat_revision: i64,
}

/// Fanout 数据模型仓库；持有连接、键模型与配置。
#[derive(Clone)]
pub struct FanoutRepository {
    client: Arc<RedisClient>,
    keyspace: JobKeyspace,
    config: Arc<JobConfig>,
}

impl FanoutRepository {
    /// 业务作用：绑定连接、键模型与配置，创建 Fanout 数据模型仓库。
    ///
    /// 参数说明：
    /// - `client`: 承载 Fanout 连接的 RedisClient。
    /// - `keyspace`: 冻结的 Job 键模型。
    /// - `config`: 已校验的 Job 配置，提供创建超时与投递批量。
    ///
    /// 返回：可建立/提交/读取 Fanout 批次的仓库。
    pub fn new(client: Arc<RedisClient>, keyspace: JobKeyspace, config: Arc<JobConfig>) -> Self {
        Self {
            client,
            keyspace,
            config,
        }
    }

    /// 业务作用：在固定 Fanout 桶中幂等建立根 intent，冻结能力快照与后续恢复所需合同。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识（与根 attempt 一一对应）。
    /// - `root_run_id`/`root_attempt`: 根 Run 标识与 attempt。
    /// - `snapshot`: 冻结的能力快照，提供 `snapshot_id` 与逐字节兼容的 `snapshotPayload`。
    /// - `root_definition`: 根任务定义，提供回执参数与根调度定位。
    /// - `contract`: 本次冻结的目标 Worker、合同、schema、编码与失败策略。
    /// - `shard_total`: 分片总数。
    ///
    /// 返回：首次建立返回 `Ok`；同快照重放返回 `Adopted`；绑定了不同快照返回 `Conflict`。
    #[allow(clippy::too_many_arguments)]
    pub async fn begin(
        &self,
        fanout_id: &str,
        root_run_id: &str,
        root_attempt: i64,
        snapshot: &ClusterSnapshot,
        root_definition: &JobDefinition,
        contract: &FanoutContract,
        shard_total: i64,
    ) -> Result<FanoutBeginOutcome> {
        let root_shard = self.keyspace.schedule_shard(root_definition.name());
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
        ];
        // snapshotPayload = Base64(快照 JSON)，字节与既有实现一致，供跨实现恢复进行中 Fanout。
        let snapshot_payload =
            base64::engine::general_purpose::STANDARD.encode(snapshot.wire_json());
        let argv: Vec<Vec<u8>> = vec![
            fanout_id.as_bytes().to_vec(),
            root_run_id.as_bytes().to_vec(),
            root_attempt.to_string().into_bytes(),
            snapshot.snapshot_id.clone().into_bytes(),
            snapshot_payload.into_bytes(),
            contract.worker_name.as_bytes().to_vec(),
            contract.contract_revision.to_string().into_bytes(),
            contract.schema_id.as_bytes().to_vec(),
            contract.wire_codec.wire_name().as_bytes().to_vec(),
            shard_total.to_string().into_bytes(),
            root_definition
                .fanout_receipt_timeout_ms()
                .to_string()
                .into_bytes(),
            root_definition
                .fanout_receipt_max_retries()
                .to_string()
                .into_bytes(),
            contract.failure_policy.wire_name().as_bytes().to_vec(),
            self.config
                .fanout_create_timeout_ms
                .to_string()
                .into_bytes(),
            root_definition.name().as_bytes().to_vec(),
            root_shard.to_string().into_bytes(),
            self.keyspace.qualifier().as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &FANOUT_BEGIN, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "ADOPTED" => FanoutBeginOutcome::Adopted,
            "CONFLICT" => FanoutBeginOutcome::Conflict,
            "OK" => FanoutBeginOutcome::Ok {
                redis_now: parse_num(raw.get(1), "fanout_begin redisNow")?,
            },
            _ => return Err(protocol("fanout_begin 返回未知码")),
        })
    }

    /// 业务作用：把全部 shard 按配置批量幂等写入，目标由快照成员按序 1:1 分配；分批共享一次 Redis 时间。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    /// - `snapshot`: 能力快照，成员按序作为各 shard 的目标节点。
    /// - `contract`: 本次冻结的 Worker、合同、schema 与线编码。
    /// - `root_run_id`: 根 Run 标识。
    /// - `origin_executor_id`: 发起本次创建的执行器身份。
    /// - `shard_payloads`: 每个 shard 的业务参数字节，长度必须等于快照成员数。
    ///
    /// 返回：全部批次成功返回累计新增数与已建立总数；任一批次拒绝立即返回对应结局。
    #[allow(clippy::too_many_arguments)]
    pub async fn add_shards(
        &self,
        fanout_id: &str,
        snapshot: &ClusterSnapshot,
        contract: &FanoutContract,
        root_run_id: &str,
        origin_executor_id: &str,
        shard_payloads: &[Vec<u8>],
    ) -> Result<FanoutAddShardsOutcome> {
        if shard_payloads.len() != snapshot.members.len() {
            return Err(protocol("add_shards 载荷数必须与快照成员数一致"));
        }
        let shard_total = shard_payloads.len() as i64;
        let batch_size = self.config.fanout_delivery_batch_size.max(1) as usize;
        let mut added_total = 0_i64;
        let mut created_shard_count = 0_i64;
        for offset in (0..shard_payloads.len()).step_by(batch_size) {
            let end = (offset + batch_size).min(shard_payloads.len());
            let mut keys: Vec<Vec<u8>> = vec![self.keyspace.fanout_root(fanout_id).into_bytes()];
            let mut argv: Vec<Vec<u8>> = vec![
                snapshot.snapshot_id.clone().into_bytes(),
                (end - offset).to_string().into_bytes(),
                self.keyspace.qualifier().as_bytes().to_vec(),
                self.config.protocol_version.to_string().into_bytes(),
            ];
            // index 同时定位 shard_payloads 与 snapshot.members（1:1 目标分配），并作为 seq。
            #[allow(clippy::needless_range_loop)]
            for index in offset..end {
                let seq = index as i64;
                let member = &snapshot.members[index];
                keys.push(self.keyspace.fanout_shard(fanout_id, seq).into_bytes());
                let execution_key = self.keyspace.execution_key(fanout_id, seq);
                let payload =
                    base64::engine::general_purpose::STANDARD.encode(&shard_payloads[index]);
                argv.extend([
                    seq.to_string().into_bytes(),
                    fanout_id.as_bytes().to_vec(),
                    root_run_id.as_bytes().to_vec(),
                    contract.worker_name.as_bytes().to_vec(),
                    contract.contract_revision.to_string().into_bytes(),
                    contract.schema_id.as_bytes().to_vec(),
                    contract.wire_codec.wire_name().as_bytes().to_vec(),
                    shard_total.to_string().into_bytes(),
                    seq.to_string().into_bytes(),
                    execution_key.into_bytes(),
                    member.node_identity.clone().into_bytes(),
                    member.startup_id.clone().into_bytes(),
                    member.heartbeat_revision.to_string().into_bytes(),
                    origin_executor_id.as_bytes().to_vec(),
                    payload.into_bytes(),
                ]);
            }
            let raw = eval(&self.client, &FANOUT_ADD_SHARDS, &keys, &argv).await?;
            let code = raw.first().map(value_to_string).unwrap_or_default();
            match code.as_str() {
                "STATE_MISMATCH" => return Ok(FanoutAddShardsOutcome::StateMismatch),
                "CONFLICT" => return Ok(FanoutAddShardsOutcome::Conflict),
                "OK" => {
                    added_total += parse_num(raw.get(1), "add_shards added")?;
                    created_shard_count = parse_num(raw.get(2), "add_shards createdShardCount")?;
                }
                _ => return Err(protocol("fanout_add_shards 返回未知码")),
            }
        }
        Ok(FanoutAddShardsOutcome::Ok {
            added: added_total,
            created_shard_count,
        })
    }

    /// 业务作用：在全部 shard 已幂等建立后提交 Fanout 根，开放首次投递与跨 slot 对账。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：提交成功返回 `Ok`；已提交返回 `Adopted`；非 CREATING 返回 `StateMismatch`；shard 数不齐返回 `Invalid`。
    pub async fn commit(&self, fanout_id: &str) -> Result<FanoutCommitOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
            self.keyspace.fanout_completion(fanout_id).into_bytes(),
        ];
        let raw = eval(
            &self.client,
            &FANOUT_COMMIT,
            &keys,
            &[fanout_id.as_bytes().to_vec()],
        )
        .await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "ADOPTED" => FanoutCommitOutcome::Adopted,
            "STATE_MISMATCH" => FanoutCommitOutcome::StateMismatch,
            "INVALID" => FanoutCommitOutcome::Invalid,
            "OK" => FanoutCommitOutcome::Ok {
                redis_now: parse_num(raw.get(1), "fanout_commit redisNow")?,
            },
            _ => return Err(protocol("fanout_commit 返回未知码")),
        })
    }

    /// 业务作用：按固定字段顺序读取 Fanout 根的跨监视器权威快照。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：根存在时返回投影；不存在返回 `None`；字段数不符合合同时 fail-closed。
    pub async fn read_root(&self, fanout_id: &str) -> Result<Option<FanoutRoot>> {
        let key = self.keyspace.fanout_root(fanout_id).into_bytes();
        let raw = eval(&self.client, &READ_FANOUT_ROOT, &[key], &[]).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        if raw.len() != 23 {
            return Err(protocol("read_fanout_root 返回字段数不符合合同"));
        }
        Ok(Some(FanoutRoot {
            fanout_id: text(&raw, 0),
            state: text(&raw, 1),
            root_run_id: text(&raw, 2),
            root_attempt: num(&raw, 3)?,
            root_job_name: text(&raw, 4),
            root_schedule_shard: num(&raw, 5)?,
            worker_name: text(&raw, 6),
            contract_revision: num(&raw, 7)?,
            schema_id: text(&raw, 8),
            wire_codec: text(&raw, 9),
            shard_total: num(&raw, 10)?,
            delivery_cursor: num(&raw, 11)?,
            receipt_timeout_ms: num(&raw, 12)?,
            receipt_max_retries: num(&raw, 13)?,
            failure_policy: text(&raw, 14),
            expire_at: num(&raw, 15)?,
            cleanup_cursor: num(&raw, 16)?,
            reconciled_at: num(&raw, 17)?,
            cancel_reason: text(&raw, 18),
            capability_cursor: num(&raw, 19)?,
            cancel_cursor: num(&raw, 20)?,
            create_deadline_at: num(&raw, 21)?,
            scheduler_qualifier: text(&raw, 22),
        }))
    }

    /// 业务作用：按固定字段顺序读取一个 Fanout shard 的合同、assignment、执行权、参数与恢复截止点。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    /// - `seq`: 稳定分片序号。
    ///
    /// 返回：shard 存在时返回投影；不存在返回 `None`；字段数不符合合同时 fail-closed。
    pub async fn read_shard(&self, fanout_id: &str, seq: i64) -> Result<Option<FanoutShard>> {
        let key = self.keyspace.fanout_shard(fanout_id, seq).into_bytes();
        let raw = eval(&self.client, &READ_FANOUT_SHARD, &[key], &[]).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        if raw.len() != 28 {
            return Err(protocol("read_fanout_shard 返回字段数不符合合同"));
        }
        Ok(Some(FanoutShard {
            fanout_id: text(&raw, 0),
            root_run_id: text(&raw, 1),
            snapshot_id: text(&raw, 2),
            worker_name: text(&raw, 3),
            contract_revision: num(&raw, 4)?,
            schema_id: text(&raw, 5),
            wire_codec: text(&raw, 6),
            shard_index: num(&raw, 7)?,
            shard_total: num(&raw, 8)?,
            seq: num(&raw, 9)?,
            execution_key: text(&raw, 10),
            target_node_identity: text(&raw, 11),
            target_startup_id: text(&raw, 12),
            assignment_epoch: num(&raw, 13)?,
            assignment_count: num(&raw, 14)?,
            state: text(&raw, 15),
            attempt: num(&raw, 16)?,
            attempt_token: text(&raw, 17),
            owner: text(&raw, 18),
            lease_until: num(&raw, 19)?,
            parameter_payload: text(&raw, 20),
            inbox_message_id: text(&raw, 21),
            receipt_retry_count: num(&raw, 22)?,
            ready_wakeup_count: num(&raw, 23)?,
            origin_executor_id: text(&raw, 24),
            receipt_deadline_at: num(&raw, 25)?,
            start_visible_at: num(&raw, 26)?,
            target_heartbeat_revision: num(&raw, 27)?,
        }))
    }

    /// 业务作用：为已提交 Fanout 的全部 shard 按配置批量建立持久 inbox 消息与 receipt 截止，再发布低延迟通知。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    /// - `snapshot`: 能力快照，成员按序对应各 shard 的目标节点（决定 inbox 与通知频道）。
    /// - `definition`: 根任务定义，提供接收回执超时。
    ///
    /// 返回：全部批次成功返回累计投递数与投递游标；根未提交返回 `NotCommitted`。只投递当前 assignment 且尚无 inbox 消息的 shard。
    pub async fn deliver_batch(
        &self,
        fanout_id: &str,
        snapshot: &ClusterSnapshot,
        _definition: &JobDefinition,
    ) -> Result<FanoutDeliverOutcome> {
        let batch_size = self.config.fanout_delivery_batch_size.max(1) as usize;
        let mut delivered_total = 0_i64;
        let mut delivery_cursor = 0_i64;
        for offset in (0..snapshot.members.len()).step_by(batch_size) {
            let end = (offset + batch_size).min(snapshot.members.len());
            let mut shards = Vec::with_capacity(end - offset);
            for index in offset..end {
                let seq = index as i64;
                let shard = self
                    .read_shard(fanout_id, seq)
                    .await?
                    .ok_or_else(|| protocol("首次投递缺少已提交的 shard"))?;
                shards.push(shard);
            }
            match self.deliver_assignments(fanout_id, &shards, true).await? {
                FanoutDeliverOutcome::NotCommitted => {
                    return Ok(FanoutDeliverOutcome::NotCommitted);
                }
                FanoutDeliverOutcome::Ok {
                    delivered,
                    delivery_cursor: cursor,
                } => {
                    delivered_total += delivered;
                    delivery_cursor = cursor;
                }
            }
        }
        Ok(FanoutDeliverOutcome::Ok {
            delivered: delivered_total,
            delivery_cursor,
        })
    }

    /// 业务作用：把权威 shard assignment 建立为持久 inbox 与 receipt 截止；既用于首次投递恢复，也用于重分配后的单点补投。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    /// - `shards`: 从 Redis 读回的当前 assignment；目标、epoch 与回执期限都由持久字段决定。
    /// - `advance_cursor`: 首次顺序投递时为 true；重分配补投不得推进根投递游标。
    ///
    /// 返回：脚本接受时返回实际新建 inbox 数与根游标；根未提交返回 `NotCommitted`；空集合返回当前游标。
    pub async fn deliver_assignments(
        &self,
        fanout_id: &str,
        shards: &[FanoutShard],
        advance_cursor: bool,
    ) -> Result<FanoutDeliverOutcome> {
        let root = match self.read_root(fanout_id).await? {
            Some(root) if root.state == "COMMITTED" || root.state == "WAITING_CHILDREN" => root,
            _ => return Ok(FanoutDeliverOutcome::NotCommitted),
        };
        if shards.is_empty() {
            return Ok(FanoutDeliverOutcome::Ok {
                delivered: 0,
                delivery_cursor: root.delivery_cursor,
            });
        }
        let publish = self.config.pubsub_mode.publish_command();
        let batch_size = self.config.fanout_delivery_batch_size.max(1) as usize;
        let mut delivered_total = 0_i64;
        let mut delivery_cursor = 0_i64;
        for batch in shards.chunks(batch_size) {
            let mut keys: Vec<Vec<u8>> = vec![
                self.keyspace.fanout_root(fanout_id).into_bytes(),
                self.keyspace.fanout_receipts(fanout_id).into_bytes(),
            ];
            let mut argv: Vec<Vec<u8>> = vec![
                batch.len().to_string().into_bytes(),
                fanout_id.as_bytes().to_vec(),
                publish.as_bytes().to_vec(),
                if advance_cursor {
                    b"1".to_vec()
                } else {
                    b"0".to_vec()
                },
            ];
            for shard in batch {
                if shard.fanout_id != fanout_id {
                    return Err(protocol("投递 shard 与 fanout_id 不一致"));
                }
                keys.push(
                    self.keyspace
                        .fanout_shard(fanout_id, shard.seq)
                        .into_bytes(),
                );
                keys.push(
                    self.keyspace
                        .fanout_inbox(fanout_id, &shard.target_node_identity)
                        .into_bytes(),
                );
                keys.push(
                    self.keyspace
                        .fanout_notify_channel(fanout_id, &shard.target_node_identity)
                        .into_bytes(),
                );
                argv.extend([
                    shard.seq.to_string().into_bytes(),
                    shard.assignment_epoch.to_string().into_bytes(),
                    shard_run_id(fanout_id, shard.seq).into_bytes(),
                    root.receipt_timeout_ms.to_string().into_bytes(),
                ]);
            }
            let raw = eval(&self.client, &FANOUT_DELIVER_BATCH, &keys, &argv).await?;
            let code = raw.first().map(value_to_string).unwrap_or_default();
            match code.as_str() {
                "NOT_COMMITTED" => return Ok(FanoutDeliverOutcome::NotCommitted),
                "OK" => {
                    delivered_total += parse_num(raw.get(1), "deliver delivered")?;
                    delivery_cursor = parse_num(raw.get(2), "deliver deliveryCursor")?;
                }
                _ => return Err(protocol("fanout_deliver_batch 返回未知码")),
            }
        }
        Ok(FanoutDeliverOutcome::Ok {
            delivered: delivered_total,
            delivery_cursor,
        })
    }

    /// 业务作用：目标节点持久确认已接收一个 Fanout shard，建立 start 截止索引后向根执行器发回执信号。
    ///
    /// 参数说明：
    /// - `fanout_id`/`seq`: Fanout 标识与分片序号。
    /// - `target_node_identity`: 本节点稳定身份，必须与 shard 记录一致。
    /// - `executor_id`: 本进程执行器身份，成为接收方。
    /// - `assignment_epoch`: 当前 assignment epoch，必须与 shard 记录一致。
    /// - `origin_executor_id`: 发起执行器身份（从 shard 记录读得），决定回执频道。
    ///
    /// 返回：接收成功返回权威时刻与 start 可见截止；根未提交/已接收/状态或 assignment 不符返回对应结局。
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_shard(
        &self,
        definition: &JobDefinition,
        fanout_id: &str,
        seq: i64,
        target_node_identity: &str,
        executor_id: &str,
        assignment_epoch: i64,
        origin_executor_id: &str,
    ) -> Result<FanoutAcceptOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_shard(fanout_id, seq).into_bytes(),
            self.keyspace.fanout_receipts(fanout_id).into_bytes(),
            self.keyspace.fanout_ready(fanout_id).into_bytes(),
            self.keyspace
                .fanout_receipt_channel(fanout_id, origin_executor_id)
                .into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            fanout_id.as_bytes().to_vec(),
            seq.to_string().into_bytes(),
            target_node_identity.as_bytes().to_vec(),
            executor_id.as_bytes().to_vec(),
            assignment_epoch.to_string().into_bytes(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
            self.config
                .pubsub_mode
                .publish_command()
                .as_bytes()
                .to_vec(),
            self.keyspace.qualifier().as_bytes().to_vec(),
            self.config.protocol_version.to_string().into_bytes(),
            definition.contract_revision().to_string().into_bytes(),
            definition.schema_id().as_bytes().to_vec(),
            definition.wire_codecs().as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &FANOUT_ACCEPT_SHARD, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "NOT_COMMITTED" => FanoutAcceptOutcome::NotCommitted,
            "ADOPTED" => FanoutAcceptOutcome::Adopted,
            "STATE_MISMATCH" => FanoutAcceptOutcome::StateMismatch,
            "STALE_ASSIGNMENT" => FanoutAcceptOutcome::StaleAssignment,
            "SOURCE_MISMATCH" => FanoutAcceptOutcome::SourceMismatch,
            "PROTOCOL_UNSUPPORTED" => FanoutAcceptOutcome::ProtocolUnsupported,
            "CONTRACT_MISMATCH" => FanoutAcceptOutcome::ContractMismatch,
            "OK" => FanoutAcceptOutcome::Ok {
                redis_now: parse_num(raw.get(1), "accept redisNow")?,
                start_visible_at: parse_num(raw.get(2), "accept startVisibleAt")?,
            },
            _ => return Err(protocol("fanout_accept_shard 返回未知码")),
        })
    }

    /// 业务作用：当 shard 未在 receipt 截止前确认时，重发当前 assignment 的通知并推进下一截止点。
    ///
    /// 参数说明：
    /// - `root`: Fanout 根投影，提供回执上限、超时与失败策略（创建时冻结）。
    /// - `shard`: shard 投影，提供 assignment epoch 与目标节点（决定通知频道）。
    ///
    /// 返回：重发成功返回计数与下一截止；未到期/证据陈旧/assignment 不符/重发耗尽返回对应结局。
    pub async fn retry_receipt(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
    ) -> Result<FanoutReceiptRetryOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace
                .fanout_shard(&root.fanout_id, shard.seq)
                .into_bytes(),
            self.keyspace.fanout_receipts(&root.fanout_id).into_bytes(),
            self.keyspace
                .fanout_notify_channel(&root.fanout_id, &shard.target_node_identity)
                .into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            root.fanout_id.as_bytes().to_vec(),
            shard.seq.to_string().into_bytes(),
            shard.assignment_epoch.to_string().into_bytes(),
            root.receipt_max_retries.to_string().into_bytes(),
            root.receipt_timeout_ms.to_string().into_bytes(),
            self.config
                .pubsub_mode
                .publish_command()
                .as_bytes()
                .to_vec(),
            root.failure_policy.as_bytes().to_vec(),
            self.config.max_scan_interval_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_RETRY_RECEIPT, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STALE" => FanoutReceiptRetryOutcome::Stale,
            "STALE_ASSIGNMENT" => FanoutReceiptRetryOutcome::StaleAssignment,
            "NOT_DUE" => FanoutReceiptRetryOutcome::NotDue {
                deadline: parse_num(raw.get(1), "retry_receipt deadline")?,
            },
            "RETRY_EXHAUSTED" => FanoutReceiptRetryOutcome::RetryExhausted {
                count: parse_num(raw.get(1), "retry_receipt count")?,
            },
            "OK" => FanoutReceiptRetryOutcome::Ok {
                count: parse_num(raw.get(1), "retry_receipt count")?,
                next_deadline: parse_num(raw.get(2), "retry_receipt nextDeadline")?,
            },
            _ => return Err(protocol("fanout_retry_receipt 返回未知码")),
        })
    }

    /// 业务作用：重新唤醒已持久接收但尚未取得执行权的 shard，推进下一可见点。
    ///
    /// 参数说明：
    /// - `root`: Fanout 根投影，提供失败策略。
    /// - `shard`: shard 投影，提供 assignment epoch 与目标节点（决定通知频道）。
    ///
    /// 返回：唤醒成功返回计数与下一可见点；未到期/证据陈旧/assignment 不符/唤醒耗尽返回对应结局。
    pub async fn promote_ready(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
    ) -> Result<FanoutReadyPromoteOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace
                .fanout_shard(&root.fanout_id, shard.seq)
                .into_bytes(),
            self.keyspace.fanout_ready(&root.fanout_id).into_bytes(),
            self.keyspace
                .fanout_notify_channel(&root.fanout_id, &shard.target_node_identity)
                .into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            root.fanout_id.as_bytes().to_vec(),
            shard.seq.to_string().into_bytes(),
            shard.assignment_epoch.to_string().into_bytes(),
            self.config.ready_max_wakeups.to_string().into_bytes(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
            self.config
                .pubsub_mode
                .publish_command()
                .as_bytes()
                .to_vec(),
            root.failure_policy.as_bytes().to_vec(),
            self.config.max_scan_interval_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_PROMOTE_READY, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STALE" => FanoutReadyPromoteOutcome::Stale,
            "STALE_ASSIGNMENT" => FanoutReadyPromoteOutcome::StaleAssignment,
            "NOT_DUE" => FanoutReadyPromoteOutcome::NotDue {
                visible_at: parse_num(raw.get(1), "promote_ready visibleAt")?,
            },
            "WAKEUP_EXHAUSTED" => FanoutReadyPromoteOutcome::WakeupExhausted {
                count: parse_num(raw.get(1), "promote_ready count")?,
            },
            "CAPACITY_EXHAUSTED" => FanoutReadyPromoteOutcome::CapacityExhausted {
                count: parse_num(raw.get(1), "promote_ready count")?,
            },
            "OK" => FanoutReadyPromoteOutcome::Ok {
                count: parse_num(raw.get(1), "promote_ready count")?,
                next_visible_at: parse_num(raw.get(2), "promote_ready nextVisibleAt")?,
            },
            _ => return Err(protocol("fanout_promote_ready 返回未知码")),
        })
    }

    /// 业务作用：目标节点已经确认接收、但本地执行槽暂满时短暂保留当前 assignment；连续等待超过容量窗口后，非严格策略重新开放根失败策略，严格快照保持固定目标与既有退避。
    ///
    /// 参数说明：`shard` 提供当前 Fanout、分片序号、目标稳定身份与 assignment epoch。
    ///
    /// 返回：当前 RECEIVED assignment 成功延后时返回新可见时刻，超过容量窗口时返回累计证据，状态或 assignment 已变化时返回封闭陈旧结局；Redis 或协议失败返回错误。
    pub async fn defer_ready_for_capacity(
        &self,
        shard: &FanoutShard,
    ) -> Result<FanoutReadyDeferOutcome> {
        self.defer_ready_for_capacity_mode(shard, false).await
    }

    /// 业务作用：在容量压力复查时开启下一段等待窗口，避免同一 assignment 因历史窗口耗尽永久停滞。
    ///
    /// 参数说明：`reopen` 为真时重置容量窗口起点；仅监视器在确认没有可切换兼容目标时使用。
    ///
    /// 返回：与 `defer_ready_for_capacity` 相同；assignment 或状态变化时返回陈旧结局。
    pub(crate) async fn defer_ready_for_capacity_mode(
        &self,
        shard: &FanoutShard,
        reopen: bool,
    ) -> Result<FanoutReadyDeferOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace
                .fanout_shard(&shard.fanout_id, shard.seq)
                .into_bytes(),
            self.keyspace.fanout_ready(&shard.fanout_id).into_bytes(),
            self.keyspace.fanout_root(&shard.fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            shard.fanout_id.as_bytes().to_vec(),
            shard.seq.to_string().into_bytes(),
            shard.target_node_identity.as_bytes().to_vec(),
            shard.assignment_epoch.to_string().into_bytes(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
            self.config.fanout_capacity_wait_ms.to_string().into_bytes(),
            self.config.ready_max_wakeups.to_string().into_bytes(),
            if reopen {
                b"REOPEN".to_vec()
            } else {
                b"CONTINUE".to_vec()
            },
        ];
        let raw = eval(&self.client, &FANOUT_DEFER_READY, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STALE" => FanoutReadyDeferOutcome::Stale,
            "STALE_ASSIGNMENT" => FanoutReadyDeferOutcome::StaleAssignment,
            "DEFERRED" => FanoutReadyDeferOutcome::Deferred {
                next_visible_at: parse_num(raw.get(1), "defer_ready nextVisibleAt")?,
            },
            "CAPACITY_EXHAUSTED" => FanoutReadyDeferOutcome::CapacityExhausted {
                first_deferred_at: parse_num(raw.get(1), "defer_ready firstDeferredAt")?,
                elapsed_ms: parse_num(raw.get(2), "defer_ready elapsedMs")?,
            },
            _ => return Err(protocol("fanout_defer_ready 返回未知码")),
        })
    }

    /// 业务作用：以 FANOUT 模式原子领取一个 shard 的 attempt 执行权，复验根提交状态、目标 assignment 与 fencing，并确认 inbox 消息。
    ///
    /// 参数说明：
    /// - `fanout_id`/`seq`: Fanout 标识与分片序号。
    /// - `worker_name`: Worker 能力名。
    /// - `node_identity`: 本节点稳定身份，必须与 shard 目标一致。
    /// - `executor_id`: 本进程执行器身份，成为 owner。
    /// - `assignment_epoch`: 当前 assignment epoch，必须与 shard 记录一致。
    /// - `message_id`: 当前 inbox 消息 ID，随领取一并 ACK/删除。
    ///
    /// 返回：成功返回 Started/Adopted 及 attempt、token、租约；根未提交、assignment 或 fencing 不符返回对应结局。
    /// 与普通 Run 共用同一 `start_run` 内核，不分叉。
    #[allow(clippy::too_many_arguments)]
    pub async fn start_shard(
        &self,
        definition: &JobDefinition,
        fanout_id: &str,
        seq: i64,
        node_identity: &str,
        executor_id: &str,
        assignment_epoch: i64,
        message_id: &str,
    ) -> Result<StartOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_shard(fanout_id, seq).into_bytes(),
            self.keyspace.fanout_leases(fanout_id).into_bytes(),
            self.keyspace.fanout_ready(fanout_id).into_bytes(),
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_receipts(fanout_id).into_bytes(),
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace
                .fanout_inbox(fanout_id, node_identity)
                .into_bytes(),
            self.keyspace.fanout_completion(fanout_id).into_bytes(),
            self.keyspace.fanout_root(fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            definition.worker_name().as_bytes().to_vec(),
            shard_run_id(fanout_id, seq).into_bytes(),
            executor_id.as_bytes().to_vec(),
            message_id.as_bytes().to_vec(),
            self.config.lease_ms.to_string().into_bytes(),
            b"PARALLEL".to_vec(),
            b"0".to_vec(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
            INBOX_GROUP.as_bytes().to_vec(),
            self.config.fanout_retention_ms.to_string().into_bytes(),
            b"FANOUT".to_vec(),
            node_identity.as_bytes().to_vec(),
            assignment_epoch.to_string().into_bytes(),
            fanout_id.as_bytes().to_vec(),
            seq.to_string().into_bytes(),
            self.keyspace.execution_key(fanout_id, seq).into_bytes(),
            Vec::new(),
            Vec::new(),
            self.keyspace.qualifier().as_bytes().to_vec(),
            self.config.protocol_version.to_string().into_bytes(),
            definition.contract_revision().to_string().into_bytes(),
            definition.schema_id().as_bytes().to_vec(),
            definition.wire_codecs().as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &START_RUN, &keys, &argv).await?;
        interpret_start(&raw)
    }

    /// 业务作用：只为当前 owner/token 续期一个 Fanout shard，并把持久取消请求同步到本地权威门禁。
    ///
    /// 参数说明：`fanout_id`/`seq` 定位 shard，`executor_id`/`attempt_token` 证明当前执行权。
    ///
    /// 返回：续期成功返回 Redis 时刻、新截止与取消标志；状态或执行权不符返回封闭结局。
    pub async fn renew_shard(
        &self,
        fanout_id: &str,
        seq: i64,
        executor_id: &str,
        attempt_token: i64,
    ) -> Result<RenewOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_shard(fanout_id, seq).into_bytes(),
            self.keyspace.fanout_leases(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            format!("{fanout_id}:{seq}").into_bytes(),
            executor_id.as_bytes().to_vec(),
            attempt_token.to_string().into_bytes(),
            self.config.lease_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &RENEW_RUN, &keys, &argv).await?;
        interpret_renew(&raw)
    }

    /// 业务作用：以 FANOUT 模式提交一个 shard 的 attempt 结果，释放执行权，并在最后一个 shard 结束时原子决定 Fanout 根终态。
    ///
    /// 参数说明：
    /// - `fanout_id`/`seq`: Fanout 标识与分片序号。
    /// - `worker_name`: Worker 能力名。
    /// - `executor_id`: 当前 owner，必须与记录一致。
    /// - `attempt_token`: 当前 fencing token，必须与记录一致。
    /// - `assignment_epoch`: 当前 assignment epoch，必须与记录一致。
    /// - `definition`: 根任务定义，提供重试上限与重试延迟基数（FANOUT 用未抖动基数）。
    /// - `result_code`: Handler 结果码。
    /// - `summary`: 结果摘要（按既有实现原样写入，业务侧保证有界）。
    ///
    /// 返回：提交成功返回 shard 下一状态与权威时刻；owner/token/assignment 不符返回对应结局，迟到提交不覆盖新 attempt。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_shard(
        &self,
        fanout_id: &str,
        seq: i64,
        worker_name: &str,
        executor_id: &str,
        attempt_token: i64,
        assignment_epoch: i64,
        definition: &JobDefinition,
        result_code: JobResultCode,
        summary: &str,
    ) -> Result<FinishOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_shard(fanout_id, seq).into_bytes(),
            self.keyspace.fanout_leases(fanout_id).into_bytes(),
            self.keyspace.fanout_ready(fanout_id).into_bytes(),
            self.keyspace.fanout_receipts(fanout_id).into_bytes(),
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
            self.keyspace.fanout_completion(fanout_id).into_bytes(),
            self.keyspace.fanout_gc(fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            shard_run_id(fanout_id, seq).into_bytes(),
            executor_id.as_bytes().to_vec(),
            attempt_token.to_string().into_bytes(),
            worker_name.as_bytes().to_vec(),
            result_code.wire_name().as_bytes().to_vec(),
            summary.as_bytes().to_vec(),
            definition.max_attempts().to_string().into_bytes(),
            definition.retry_delay_ms().to_string().into_bytes(),
            self.config.fanout_retention_ms.to_string().into_bytes(),
            b"FANOUT".to_vec(),
            fanout_id.as_bytes().to_vec(),
            seq.to_string().into_bytes(),
            assignment_epoch.to_string().into_bytes(),
            self.keyspace.execution_key(fanout_id, seq).into_bytes(),
        ];
        let raw = eval(&self.client, &FINISH_RUN, &keys, &argv).await?;
        interpret_finish(&raw)
    }

    /// 业务作用：看门狗读取 Fanout 根的权威状态与根 Run 定位信息，并把非终态根重排到下一看门时刻。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：根存在返回状态与定位字段；不存在返回 `NotFound`。
    pub async fn watch_root(&self, fanout_id: &str) -> Result<FanoutWatchRootOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            fanout_id.as_bytes().to_vec(),
            self.config.max_scan_interval_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_WATCH_ROOT, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "NOT_FOUND" => FanoutWatchRootOutcome::NotFound,
            "OK" => FanoutWatchRootOutcome::Ok {
                state: text(&raw, 1),
                root_run_id: text(&raw, 2),
                root_attempt: num(&raw, 3)?,
                root_job_name: text(&raw, 4),
                root_schedule_shard: num(&raw, 5)?,
                cancel_reason: text(&raw, 6),
                error_type: text(&raw, 7),
            },
            _ => return Err(protocol("fanout_watch_root 返回未知码")),
        })
    }

    /// 业务作用：把已完成对账的 Fanout 根原子标记为已对账，并从看门狗索引移除。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：标记成功返回对账时刻；根状态不允许标记返回 `StateMismatch`。
    pub async fn mark_reconciled(&self, fanout_id: &str) -> Result<FanoutMarkReconciledOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
        ];
        let raw = eval(
            &self.client,
            &FANOUT_MARK_RECONCILED,
            &keys,
            &[fanout_id.as_bytes().to_vec()],
        )
        .await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STATE_MISMATCH" => FanoutMarkReconciledOutcome::StateMismatch,
            "OK" => FanoutMarkReconciledOutcome::Ok {
                reconciled_at: parse_num(raw.get(1), "mark_reconciled reconciledAt")?,
            },
            _ => return Err(protocol("fanout_mark_reconciled 返回未知码")),
        })
    }

    /// 业务作用：以 CAS 推进 Fanout 根的能力补投游标，避免并发监视器重复补投同一区间。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    /// - `current_cursor`: 调用方观察到的当前游标。
    /// - `next_cursor`: 推进后的目标游标。
    ///
    /// 返回：推进成功返回新游标；根不存在返回 `NotFound`；观察游标过期返回 `Stale` 与权威游标。
    pub async fn advance_capability_cursor(
        &self,
        fanout_id: &str,
        current_cursor: i64,
        next_cursor: i64,
    ) -> Result<FanoutCapabilityCursorOutcome> {
        let argv: Vec<Vec<u8>> = vec![
            current_cursor.to_string().into_bytes(),
            next_cursor.to_string().into_bytes(),
        ];
        let raw = eval(
            &self.client,
            &FANOUT_ADVANCE_CAPABILITY_CURSOR,
            &[self.keyspace.fanout_root(fanout_id).into_bytes()],
            &argv,
        )
        .await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "NOT_FOUND" => FanoutCapabilityCursorOutcome::NotFound,
            "STALE" => FanoutCapabilityCursorOutcome::Stale {
                current_cursor: parse_num(raw.get(1), "advance_capability currentCursor")?,
            },
            "OK" => FanoutCapabilityCursorOutcome::Ok {
                next_cursor: parse_num(raw.get(1), "advance_capability nextCursor")?,
            },
            _ => return Err(protocol("fanout_advance_capability_cursor 返回未知码")),
        })
    }

    /// 业务作用：在跨 slot Fanout 桶超过创建截止仍未提交时，把根收敛为 FAILED 并开放清理。
    ///
    /// 参数说明：
    /// - `fanout_id`: Fanout 标识。
    ///
    /// 返回：确已到期返回 `Ok(FAILED)` 与保留截止；根不在 CREATING 返回 `StateMismatch`；截止未到返回 `NotDue`。
    pub async fn fail_creating(&self, fanout_id: &str) -> Result<FanoutFailCreatingOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_roots(fanout_id).into_bytes(),
            self.keyspace.fanout_completion(fanout_id).into_bytes(),
            self.keyspace.fanout_gc(fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            fanout_id.as_bytes().to_vec(),
            self.config.fanout_retention_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_FAIL_CREATING, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STATE_MISMATCH" => FanoutFailCreatingOutcome::StateMismatch,
            "NOT_DUE" => FanoutFailCreatingOutcome::NotDue {
                deadline: parse_num(raw.get(1), "fail_creating deadline")?,
            },
            "OK" => FanoutFailCreatingOutcome::Ok {
                state: text(&raw, 1),
                expire_at: parse_num(raw.get(2), "fail_creating expireAt")?,
            },
            _ => return Err(protocol("fanout_fail_creating 返回未知码")),
        })
    }

    /// 业务作用：对账期把一个确认不可达的 shard 以给定终态收敛进根，并在最后一个 shard 时原子决定根终态。
    ///
    /// 参数说明：
    /// - `fanout_id`/`seq`: Fanout 标识与分片序号。
    /// - `shard_terminal_state`: 该 shard 的收敛终态（如 SKIPPED）。
    /// - `result_code`: 收敛结果码。
    /// - `result_summary`: 收敛原因摘要。
    ///
    /// 返回：收敛成功返回根状态（或仍等待）与保留截止/计数；shard 已终态返回 `AlreadyCompleted`。
    pub async fn aggregate(
        &self,
        fanout_id: &str,
        seq: i64,
        shard_terminal_state: JobState,
        result_code: &str,
        result_summary: &str,
    ) -> Result<FanoutAggregateOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(fanout_id).into_bytes(),
            self.keyspace.fanout_shard(fanout_id, seq).into_bytes(),
            self.keyspace.fanout_receipts(fanout_id).into_bytes(),
            self.keyspace.fanout_ready(fanout_id).into_bytes(),
            self.keyspace.fanout_leases(fanout_id).into_bytes(),
            self.keyspace.fanout_completion(fanout_id).into_bytes(),
            self.keyspace.fanout_gc(fanout_id).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![
            fanout_id.as_bytes().to_vec(),
            seq.to_string().into_bytes(),
            shard_terminal_state.wire_name().as_bytes().to_vec(),
            result_code.as_bytes().to_vec(),
            result_summary.as_bytes().to_vec(),
            self.config.fanout_retention_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_AGGREGATE, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "ALREADY_COMPLETED" => FanoutAggregateOutcome::AlreadyCompleted,
            "OK" => FanoutAggregateOutcome::Ok {
                root_state_or_waiting: text(&raw, 1),
                expire_at_or_terminal_count: parse_num(raw.get(2), "aggregate expireAtOrCount")?,
            },
            _ => return Err(protocol("fanout_aggregate 返回未知码")),
        })
    }

    /// 业务作用：把一个未在当前 assignment 收敛的 shard 原子重分配到新目标节点，并推进 assignment epoch。
    ///
    /// 参数说明：
    /// - `root`: Fanout 根投影，提供 fanoutId。
    /// - `shard`: shard 投影，提供 seq、旧目标与预期 assignment epoch。
    /// - `new_target`: 新目标执行器成员；当前无兼容节点时为空，使 shard 进入等待能力状态。
    ///
    /// 返回：重分配成功返回新 epoch；shard 正在执行返回 `Busy`；epoch 不符返回 `StaleAssignment`；超过上限返回 `NoCapableExecutor`。
    pub async fn reassign_shard(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        new_target: Option<&ExecutorMember>,
    ) -> Result<FanoutReassignOutcome> {
        self.reassign_shard_mode(root, shard, new_target, "FAILURE")
            .await
    }

    /// 业务作用：按故障或容量路由模式原子切换 Fanout assignment，并使用对应的独立预算。
    ///
    /// 参数说明：`mode` 取 `FAILURE`、`CAPACITY` 或 `CAPACITY_TARGET_LOST`。
    ///
    /// 返回：切换成功、竞争陈旧、故障配额耗尽或容量路由配额耗尽的明确结局。
    pub(crate) async fn reassign_shard_mode(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        new_target: Option<&ExecutorMember>,
        mode: &str,
    ) -> Result<FanoutReassignOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace
                .fanout_shard(&root.fanout_id, shard.seq)
                .into_bytes(),
            self.keyspace.fanout_receipts(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_ready(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_leases(&root.fanout_id).into_bytes(),
            self.keyspace
                .fanout_inbox(&root.fanout_id, &shard.target_node_identity)
                .into_bytes(),
        ];
        let (node_identity, startup_id, heartbeat_revision) = match new_target {
            Some(target) => (
                target.node_identity.as_str(),
                target.startup_id.as_str(),
                target.heartbeat_revision,
            ),
            None => ("", "", 0),
        };
        let argv: Vec<Vec<u8>> = vec![
            root.fanout_id.as_bytes().to_vec(),
            shard.seq.to_string().into_bytes(),
            shard.target_node_identity.as_bytes().to_vec(),
            shard.assignment_epoch.to_string().into_bytes(),
            node_identity.as_bytes().to_vec(),
            startup_id.as_bytes().to_vec(),
            heartbeat_revision.to_string().into_bytes(),
            INBOX_GROUP.as_bytes().to_vec(),
            self.config.fanout_max_assignments.to_string().into_bytes(),
            mode.as_bytes().to_vec(),
            self.config.fanout_capacity_wait_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_REASSIGN_SHARD, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "BUSY" => FanoutReassignOutcome::Busy,
            "STALE_ASSIGNMENT" => FanoutReassignOutcome::StaleAssignment,
            "NO_CAPABLE_EXECUTOR" => FanoutReassignOutcome::NoCapableExecutor,
            "CAPACITY_ROUTE_EXHAUSTED" => FanoutReassignOutcome::CapacityRouteExhausted,
            "STALE_CAPACITY" => FanoutReassignOutcome::StaleCapacity,
            "OK" => FanoutReassignOutcome::Ok {
                next_epoch: parse_num(raw.get(1), "reassign nextEpoch")?,
            },
            _ => return Err(protocol("fanout_reassign_shard 返回未知码")),
        })
    }

    /// 业务作用：对已对账的终态 Fanout 有界清理一批 shard 与索引，游标推进到全部删除后再删根。
    ///
    /// 参数说明：
    /// - `root`: 终态且已对账的 Fanout 根投影，提供清理游标与分片总数。
    ///
    /// 返回：本批删除数与下一游标；全部删除返回 `Completed`；未对账/未到期/状态不符返回对应结局。
    pub async fn cleanup(&self, root: &FanoutRoot) -> Result<FanoutCleanupOutcome> {
        let remaining = (root.shard_total - root.cleanup_cursor).max(0);
        let count = remaining.min(self.config.fanout_cleanup_batch_size as i64);
        let mut keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_gc(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_roots(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_receipts(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_ready(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_leases(&root.fanout_id).into_bytes(),
        ];
        for index in 0..count {
            let seq = root.cleanup_cursor + index;
            // 目标节点身份从 shard 读取，缺失时回退固定占位，仅用于定位 inbox key。
            let target = match self.read_shard(&root.fanout_id, seq).await? {
                Some(s) => s.target_node_identity,
                None => "cleanup".to_owned(),
            };
            keys.push(
                self.keyspace
                    .fanout_shard(&root.fanout_id, seq)
                    .into_bytes(),
            );
            keys.push(
                self.keyspace
                    .fanout_inbox(&root.fanout_id, &target)
                    .into_bytes(),
            );
        }
        let argv: Vec<Vec<u8>> = vec![
            root.fanout_id.as_bytes().to_vec(),
            count.to_string().into_bytes(),
            INBOX_GROUP.as_bytes().to_vec(),
        ];
        let raw = eval(&self.client, &FANOUT_CLEANUP, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "STATE_MISMATCH" => FanoutCleanupOutcome::StateMismatch,
            "NOT_RECONCILED" => FanoutCleanupOutcome::NotReconciled,
            "NOT_DUE" => FanoutCleanupOutcome::NotDue {
                expire_at: parse_num(raw.get(1), "cleanup expireAt")?,
            },
            "COMPLETED" => FanoutCleanupOutcome::Completed {
                deleted: parse_num(raw.get(1), "cleanup deleted")?,
                cursor: parse_num(raw.get(2), "cleanup cursor")?,
            },
            "OK" => FanoutCleanupOutcome::Ok {
                deleted: parse_num(raw.get(1), "cleanup deleted")?,
                next_cursor: parse_num(raw.get(2), "cleanup nextCursor")?,
            },
            _ => return Err(protocol("fanout_cleanup 返回未知码")),
        })
    }

    /// 业务作用：对处于 CANCELLING 的 Fanout 有界推进一批取消；运行中 shard 只写协作式取消信号，等完成或租约恢复。
    ///
    /// 参数说明：
    /// - `root`: CANCELLING 的 Fanout 根投影，提供取消游标与分片总数。
    /// - `reason`: 取消原因。
    ///
    /// 返回：仍在取消返回 `Cancelling` 与下一游标；全部收敛返回 `Cancelled` 与保留截止；已终态或游标过期返回对应结局。
    pub async fn cancel_batch(
        &self,
        root: &FanoutRoot,
        reason: &str,
    ) -> Result<FanoutCancelBatchOutcome> {
        let remaining = (root.shard_total - root.cancel_cursor).max(0);
        let count = remaining.min(self.config.fanout_cleanup_batch_size as i64);
        let mut keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_root(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_roots(&root.fanout_id).into_bytes(),
            self.keyspace
                .fanout_completion(&root.fanout_id)
                .into_bytes(),
            self.keyspace.fanout_gc(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_receipts(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_ready(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_leases(&root.fanout_id).into_bytes(),
        ];
        for index in 0..count {
            let seq = root.cancel_cursor + index;
            let target = match self.read_shard(&root.fanout_id, seq).await? {
                Some(s) => s.target_node_identity,
                None => "cancel".to_owned(),
            };
            keys.push(
                self.keyspace
                    .fanout_shard(&root.fanout_id, seq)
                    .into_bytes(),
            );
            keys.push(
                self.keyspace
                    .fanout_inbox(&root.fanout_id, &target)
                    .into_bytes(),
            );
        }
        let argv: Vec<Vec<u8>> = vec![
            root.fanout_id.as_bytes().to_vec(),
            reason.as_bytes().to_vec(),
            self.config.fanout_retention_ms.to_string().into_bytes(),
            root.cancel_cursor.to_string().into_bytes(),
            count.to_string().into_bytes(),
            INBOX_GROUP.as_bytes().to_vec(),
            self.config.min_scan_interval_ms.to_string().into_bytes(),
        ];
        let raw = eval(&self.client, &FANOUT_CANCEL_BATCH, &keys, &argv).await?;
        let code = raw.first().map(value_to_string).unwrap_or_default();
        let tag = raw.get(1).map(value_to_string).unwrap_or_default();
        Ok(match code.as_str() {
            "ALREADY_COMPLETED" => FanoutCancelBatchOutcome::AlreadyCompleted { state: tag },
            "STALE" => FanoutCancelBatchOutcome::Stale {
                cursor: parse_num(raw.get(1), "cancel_batch cursor")?,
            },
            "OK" if tag == "CANCELLING" => FanoutCancelBatchOutcome::Cancelling {
                next_cursor: parse_num(raw.get(2), "cancel_batch nextCursor")?,
            },
            "OK" if tag == "CANCELLED" => FanoutCancelBatchOutcome::Cancelled {
                expire_at: parse_num(raw.get(2), "cancel_batch expireAt")?,
            },
            _ => return Err(protocol("fanout_cancel_batch 返回未知码")),
        })
    }

    /// 业务作用：以 FANOUT 模式在服务端确认 shard 租约到期后撤销旧 owner 权威，并按失败策略推进重试、重分配或收敛。
    ///
    /// 参数说明：
    /// - `root`: Fanout 根投影，提供 fanoutId 与失败策略。
    /// - `shard`: shard 投影，提供 seq 与 Worker 名。
    /// - `definition`: 根任务定义，提供重试上限与重试延迟基数。
    ///
    /// 返回：恢复成功返回下一状态与可选唤醒延迟；租约未到期返回 `NotDue`；状态或索引不一致返回 `Stale`。
    /// 与普通 Run 共用同一 `recover_expired` 内核，不分叉。
    pub async fn recover_shard(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        definition: &JobDefinition,
    ) -> Result<RecoverOutcome> {
        self.recover_shard_with_mode(
            root,
            shard,
            definition.max_attempts(),
            definition.retry_delay_ms(),
            "FANOUT",
        )
        .await
    }

    /// 业务作用：在本地 Worker 契约已撤销时，只为 CANCELLING 根收敛到期 shard，不建立新执行入口。
    ///
    /// 参数说明：`root` 与 `shard` 提供持久失败策略、桶定位及当前租约证据。
    ///
    /// 返回：根确为 CANCELLING 且租约到期时把 shard 收敛为取消；根状态变化返回 `RootNotCancelling`。
    pub(crate) async fn recover_cancelling_shard(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
    ) -> Result<RecoverOutcome> {
        self.recover_shard_with_mode(root, shard, 0, 0, "FANOUT_CANCEL_ONLY")
            .await
    }

    /// 业务作用：以显式恢复模式调用共享租约撤权脚本，统一普通 Fanout 恢复与删除取消恢复的键序。
    ///
    /// 参数说明：`max_attempts`、`retry_delay_ms` 仅在 FANOUT 模式生效；`mode` 决定是否允许新 attempt。
    ///
    /// 返回：服务端复验后的封闭恢复结局；未知模式或返回码按协议错误处理。
    async fn recover_shard_with_mode(
        &self,
        root: &FanoutRoot,
        shard: &FanoutShard,
        max_attempts: u32,
        retry_delay_ms: u64,
        mode: &str,
    ) -> Result<RecoverOutcome> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace
                .fanout_shard(&root.fanout_id, shard.seq)
                .into_bytes(),
            self.keyspace.fanout_leases(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_ready(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_root(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_receipts(&root.fanout_id).into_bytes(),
            self.keyspace.fanout_roots(&root.fanout_id).into_bytes(),
            self.keyspace
                .fanout_completion(&root.fanout_id)
                .into_bytes(),
            self.keyspace.fanout_gc(&root.fanout_id).into_bytes(),
        ];
        // 租约索引成员是 `fanoutId:seq`，与 start_shard 写入的成员一致。
        let member = format!("{}:{}", root.fanout_id, shard.seq);
        let argv: Vec<Vec<u8>> = vec![
            member.into_bytes(),
            shard.worker_name.as_bytes().to_vec(),
            max_attempts.to_string().into_bytes(),
            retry_delay_ms.to_string().into_bytes(),
            self.config.fanout_retention_ms.to_string().into_bytes(),
            mode.as_bytes().to_vec(),
            root.fanout_id.as_bytes().to_vec(),
            shard.seq.to_string().into_bytes(),
            root.failure_policy.as_bytes().to_vec(),
            Vec::new(),
        ];
        let raw = eval(&self.client, &RECOVER_EXPIRED, &keys, &argv).await?;
        interpret_recover(&raw)
    }

    /// 业务作用：在一个 Fanout 桶内用同一次 Redis 时间批量扫描四类到期索引（receipt/ready/租约/清理），供监视器分派后续脚本。
    ///
    /// 参数说明：
    /// - `bucket`: 目标 Fanout 桶下标。
    ///
    /// 返回：权威时刻、桶内看门狗根数量与四类到期扫描；脚本只读，真正推进仍由各自写脚本按证据复验。
    pub async fn scan_due(&self, bucket: u32) -> Result<FanoutDueScans> {
        let keys: Vec<Vec<u8>> = vec![
            self.keyspace.fanout_roots_at(bucket).into_bytes(),
            self.keyspace.fanout_receipts_at(bucket).into_bytes(),
            self.keyspace.fanout_ready_at(bucket).into_bytes(),
            self.keyspace.fanout_leases_at(bucket).into_bytes(),
            self.keyspace.fanout_gc_at(bucket).into_bytes(),
        ];
        let argv: Vec<Vec<u8>> = vec![self.config.scan_batch_size.to_string().into_bytes()];
        let raw = eval(&self.client, &FANOUT_SCAN_DUE, &keys, &argv).await?;
        if raw.len() < 2 {
            return Err(protocol("fanout_scan_due 返回缺少时间头"));
        }
        let redis_now = parse_num(raw.first(), "fanout_scan_due redisNow")?;
        let root_count = parse_num(raw.get(1), "fanout_scan_due rootCount")?;
        let mut cursor = 2_usize;
        let receipts = parse_due_index(&raw, &mut cursor)?;
        let ready = parse_due_index(&raw, &mut cursor)?;
        let leases = parse_due_index(&raw, &mut cursor)?;
        let gc = parse_due_index(&raw, &mut cursor)?;
        Ok(FanoutDueScans {
            redis_now,
            root_count,
            receipts,
            ready,
            leases,
            gc,
        })
    }
}

/// 业务作用：从批量扫描返回的游标位置解析一个到期索引组（nextScore、dueCount、成员对）。
///
/// 参数说明：
/// - `raw`: 脚本返回数组。
/// - `cursor`: 当前解析游标，解析后前移到下一组起点。
///
/// 返回：该组的下一 score 与到期成员对；结构不完整或非数值 fail-closed。
fn parse_due_index(raw: &[redis::Value], cursor: &mut usize) -> Result<DueIndexScan> {
    let next_text = raw
        .get(*cursor)
        .map(value_to_string)
        .ok_or_else(|| protocol("fanout_scan_due 缺少 nextScore"))?;
    *cursor += 1;
    let next_score = if next_text.is_empty() {
        None
    } else {
        Some(
            next_text
                .parse::<i64>()
                .map_err(|_| protocol("fanout_scan_due nextScore 非法"))?,
        )
    };
    let count = parse_num(raw.get(*cursor), "fanout_scan_due dueCount")?;
    *cursor += 1;
    let mut members = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count.max(0) {
        let member = raw
            .get(*cursor)
            .map(value_to_string)
            .ok_or_else(|| protocol("fanout_scan_due 缺少成员"))?;
        *cursor += 1;
        let score = parse_num(raw.get(*cursor), "fanout_scan_due score")?;
        *cursor += 1;
        members.push((member, score));
    }
    Ok(DueIndexScan {
        next_score,
        members,
    })
}

/// 业务作用：把投影字段归一化为文本；nil 与非字符串取空串。
fn text(fields: &[redis::Value], index: usize) -> String {
    match fields.get(index) {
        Some(redis::Value::Nil) | None => String::new(),
        Some(redis::Value::Int(n)) => n.to_string(),
        Some(redis::Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        Some(_) => String::new(),
    }
}

/// 业务作用：把投影数值字段解释为 i64；nil 或空取 0，非法文本 fail-closed。
fn num(fields: &[redis::Value], index: usize) -> Result<i64> {
    match fields.get(index) {
        Some(redis::Value::Nil) | None => Ok(0),
        Some(redis::Value::Int(n)) => Ok(*n),
        Some(redis::Value::BulkString(bytes)) => {
            let s = String::from_utf8_lossy(bytes);
            if s.is_empty() {
                Ok(0)
            } else {
                s.parse::<i64>()
                    .map_err(|_| protocol("Fanout 投影数值字段非法"))
            }
        }
        Some(redis::Value::SimpleString(s)) if s.is_empty() => Ok(0),
        Some(redis::Value::SimpleString(s)) => s
            .parse::<i64>()
            .map_err(|_| protocol("Fanout 投影数值字段非法")),
        Some(_) => Err(protocol("Fanout 投影数值字段类型非法")),
    }
}

/// 业务作用：把脚本返回的一个可选数值元素解释为 i64；缺失或非数值 fail-closed。
fn parse_num(value: Option<&redis::Value>, field: &str) -> Result<i64> {
    value
        .map(value_to_string)
        .ok_or_else(|| protocol(&format!("{field} 缺失")))?
        .parse::<i64>()
        .map_err(|_| protocol(&format!("{field} 非法")))
}

/// 业务作用：构造 Job 协议错误。参数说明：`message` 摘要。返回：协议错误。
fn protocol(message: &str) -> NasaRedisError {
    crate::job::JobError::Protocol(message.to_owned()).into()
}
