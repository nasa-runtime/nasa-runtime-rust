//! DLT 来源证据与有限重放合同；来源字段不提供访问授权。

mod evidence;
pub use evidence::DltEvidence;
pub(crate) mod origin;
pub use origin::DltOrigin;
mod plan;
pub use plan::{DltInvalidPolicy, DltReplayPlan};
mod trace;
pub use trace::DltReplayTrace;
mod quarantine;
pub use quarantine::DltQuarantineTrace;
mod runtime;
pub use runtime::{DltReplayHandle, DltReplayStatus};

/// DLT 重放的稳定失败类别，不附带消息正文、原始 SQL 或连接凭据。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DltReplayError {
    /// broker 明确拒绝来源或目标资源权限。
    Authorization,
    /// 声明不完整、越界或有歧义。
    InvalidConfiguration,
    /// 持久计划损坏、截断或非规范。
    CorruptPlan,
    /// 冻结处理合同与实际入口不一致。
    ContractMismatch,
    /// 输入没有 DLT 来源证据，不能作为死信回投。
    MissingEvidence,
    /// 来源证据不完整、重复、损坏或非法。
    HeaderInvalid,
    /// 已达到显式批准的回投次数。
    ReplayLimit,
    /// 目标物理身份或资源代次改变。
    TargetIdentityChanged,
    /// 目标完整分区集合改变。
    TargetPartitionsChanged,
    /// 当前资源或 schema 证据不可观测。
    ObservationFailed,
    /// 操作预算耗尽。
    Timeout,
    /// lane 未配置 acks=all 或 -1；确认范围仍由 broker 的 ISR 和最小同步副本配置决定。
    UnconfirmedLane,
    /// 来源 schema 不在批准集合中。
    SchemaUnapproved,
    /// 双端 schema 正文或类别不符。
    SchemaMismatch,
    /// 目标交付明确未成功。
    DeliveryFailed,
    /// 目标实际副作用无法确定。
    OutcomeUnknown,
    /// 来源或目标已经关闭新任务准入。
    Closed,
    /// 任务或证据容量耗尽。
    Capacity,
}

impl std::fmt::Display for DltReplayError {
    /// 业务作用：提供可用于任务保护和诊断的脱敏分类。
    /// 参数说明：formatter 为输出目标。返回：固定类别，不包含原始输入或底层异常。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Kafka DLT replay: {self:?}")
    }
}
impl std::error::Error for DltReplayError {}

/// DLT 来源、计划和执行的显式结果域。
pub type DltReplayResult<T> = std::result::Result<T, DltReplayError>;
