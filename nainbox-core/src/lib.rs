//! Inbox 消费去重核心：重复消息裁决必须与业务副作用处于同一事务。

#![forbid(unsafe_code)]

/// 一次事务内 Inbox 去重裁决。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxClaim {
    /// 本事务首次取得消息，调用方可以继续执行业务副作用。
    Claimed,
    /// 唯一标记已经由此前成功事务提交；本次必须跳过业务副作用并正常确认消息。
    Duplicate,
}

impl InboxClaim {
    /// 业务作用：是否允许当前事务执行一次业务副作用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅首次取得唯一标记时为 `true`。
    pub fn should_process(self) -> bool {
        matches!(self, Self::Claimed)
    }
}

/// Inbox I/O 或合同错误；公开文本不包含 SQL、凭据、datasource 或消息正文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxStoreError {
    /// 稳定、脱敏的错误原因。
    pub reason: String,
}

impl InboxStoreError {
    /// 业务作用：用允许向上游公开的稳定原因构造 Inbox 错误。
    ///
    /// 参数说明：
    /// - `reason`：不含底层敏感信息的失败分类。
    ///
    /// 返回：可跨数据库 adapter 共用的脱敏错误。
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for InboxStoreError {
    /// 业务作用：输出不含 SQL、连接信息、消息身份或正文的稳定摘要。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：摘要成功写入时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "inbox store error: {}", self.reason)
    }
}

impl std::error::Error for InboxStoreError {}

/// 首次消息已经提交业务效果或重复消息被幂等吸收的封闭结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxProcess<T> {
    /// 本事务首次取得消息并已确认提交业务处理结果。
    Applied(T),
    /// 既有事务已经提交同一消息，本轮没有再次调用业务处理函数。
    Duplicate,
}

/// Inbox 事务基础设施的封闭失败阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxTransactionError {
    /// 内层错误把事务标记为只能回滚。
    RollbackOnly,
    /// 数据库明确拒绝提交，原消息不得确认。
    CommitRejected,
    /// 提交请求结果不确定，必须持续重投并依赖 Inbox 吸收可能的重复。
    CommitUncertain,
    /// 物理回滚失败，不能声称本轮没有副作用。
    RollbackFailed,
    /// 事务开始、连接或所有权基础设施失败。
    Infrastructure,
}

impl InboxTransactionError {
    /// 业务作用：判断失败是否禁止被普通有限重试预算转入死信。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：提交拒绝/不确定或回滚失败返回 `true`；明确未提交阶段返回 `false`。
    pub fn requires_unbounded_redelivery(self) -> bool {
        matches!(
            self,
            Self::CommitRejected | Self::CommitUncertain | Self::RollbackFailed
        )
    }
}

impl std::fmt::Display for InboxTransactionError {
    /// 业务作用：输出不含 SQL、连接信息、消息身份或业务正文的稳定事务阶段。
    ///
    /// 参数说明：
    /// - `formatter`：标准格式化输出目标。
    ///
    /// 返回：阶段摘要成功写入时返回 `Ok`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RollbackOnly => "Inbox transaction rollback-only",
            Self::CommitRejected => "Inbox transaction commit rejected",
            Self::CommitUncertain => "Inbox transaction commit uncertain",
            Self::RollbackFailed => "Inbox transaction rollback failed",
            Self::Infrastructure => "Inbox transaction infrastructure failed",
        })
    }
}

impl std::error::Error for InboxTransactionError {}

/// 后端中立的事务内 Inbox claim 合同。
///
/// adapter 必须拒绝事务外调用，并保证 `Claimed` 对应的唯一标记与随后业务 SQL 使用同一 datasource、
/// 同一数据库事务。只有目标消息唯一约束可以返回 `Duplicate`。
#[async_trait::async_trait]
pub trait InboxStore: Send + Sync {
    /// 业务作用：在当前 ambient transaction 内竞争消息唯一标记。
    ///
    /// 参数说明：
    /// - `consumer_name`：跨副本和重启稳定的消费命名空间。
    /// - `message_id`：transport 提供的稳定消息身份。
    ///
    /// 返回：首次取得标记为 `Claimed`，既有已提交标记为 `Duplicate`；其它失败返回错误。
    async fn claim(
        &self,
        consumer_name: &str,
        message_id: &str,
    ) -> Result<InboxClaim, InboxStoreError>;
}

// ───────────────────────────── retention(去重标记保留与清理合同)─────────────────────────────

/// Inbox 去重标记的保留清理策略。
///
/// 去重标记只增不减会让判重表随消费历史无限增长，最终拖慢唯一键判重本身。清理的安全边界由
/// `redelivery_horizon_ms` 承载：删除窗口一旦小于消息源的最大重投视界，窗口外重投的同
/// `message_id` 会再次判为首见并造成二次消费。该字段没有默认值——它取决于消息源(如 Kafka 源
/// topic 的保留期与消费重置策略中的较大者)，只有业务能够声明；超过视界的重复消息本就无法判重，
/// 属于既有合同边界而不是清理引入的损失。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboxRetentionPolicy {
    /// 消息源最大重投视界毫秒；由业务按消息源事实显式声明，是清理下限的安全依据。
    pub redelivery_horizon_ms: i64,
    /// 已处理标记的最小保留毫秒；必须不小于 `redelivery_horizon_ms`。
    pub processed_min_age_ms: i64,
    /// 单批删除行数上限；每轮循环删除直到空批或预算耗尽。
    pub batch_limit: u32,
    /// 单轮时间预算毫秒；到达预算即返回报告，剩余候选留待下一轮。
    pub round_time_budget_ms: i64,
}

impl InboxRetentionPolicy {
    /// 业务作用：校验策略自洽性，把"删除窗口小于重投视界"这类必然造成二次消费的配置拦在启动期。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部约束成立时返回 `Ok`；否则返回不含敏感信息的稳定原因文本。
    pub fn validate(&self) -> Result<(), String> {
        if self.redelivery_horizon_ms <= 0 {
            return Err("redelivery_horizon_ms must be positive".into());
        }
        if self.processed_min_age_ms < self.redelivery_horizon_ms {
            return Err(
                "processed_min_age_ms must be at least redelivery_horizon_ms; \
                 deleting markers inside the redelivery horizon re-admits duplicates"
                    .into(),
            );
        }
        if self.batch_limit == 0 || self.batch_limit > 10_000 {
            return Err("batch_limit must be within 1..=10000".into());
        }
        if self.round_time_budget_ms <= 0 {
            return Err("round_time_budget_ms must be positive".into());
        }
        Ok(())
    }
}

/// 一轮 Inbox 保留清理的账目报告；四项全部进入观测，不允许静默轮次。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InboxRetentionRoundReport {
    /// 本轮删除的过期标记行数。
    pub deleted: u64,
    /// retention owner 已被其它执行者持有，本轮没有执行清理。
    pub claim_contended: bool,
    /// 时间预算耗尽提前返回，仍有候选留待下一轮。
    pub budget_exhausted: bool,
    /// 本轮结束时最老候选标记的年龄毫秒；无候选时为 `None`。
    pub oldest_candidate_age_ms: Option<i64>,
}

/// 后端中立的 Inbox 保留清理合同。
///
/// 实现必须以后端原生互斥(如 advisory lock)保证同一物理 Inbox 表内的同一
/// `consumer_name` 同时至多一个清理者；互斥身份必须包含数据库命名空间，避免同一数据库实例中
/// 互不相干的库或 schema 相互阻塞。竞争失败以 `claim_contended` 报告而不是并发删除。删除只允许命中 `processed_at` 早于
/// cutoff 的行：claim 在 ambient 事务内写入，行对清理可见时其事务必已提交，按时间下界删除
/// 不存在半行竞态。cutoff 必须取整轮开始时刻，轮内不得随时间推进。
#[async_trait::async_trait]
pub trait DurableInboxRetention: Send + Sync {
    /// 业务作用：对单个消费命名空间执行一轮 owner 互斥的过期标记清理。
    ///
    /// 参数说明：
    /// - `consumer_name`：目标消费命名空间；owner 互斥按该值隔离。
    /// - `policy`：已通过 `validate` 的保留策略；实现必须复验，拒绝未校验策略。
    ///
    /// 返回：本轮账目报告；连接、互斥或删除失败返回脱敏错误。
    async fn retention_round(
        &self,
        consumer_name: &str,
        policy: &InboxRetentionPolicy,
    ) -> Result<InboxRetentionRoundReport, InboxStoreError>;
}
