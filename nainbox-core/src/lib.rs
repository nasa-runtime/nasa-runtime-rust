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
