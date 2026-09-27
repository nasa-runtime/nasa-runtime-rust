//! RedisJob 结构化错误合同：业务按封闭类别决策，摘要只用于诊断，不参与控制流。

/// RedisJob 的封闭错误类别；不包含 Redis endpoint、payload、nonce 或任意运行期 label。
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// 冻结配置、名称、容量或时间关系不合法。
    #[error("RedisJob 配置不合法: {0}")]
    Config(String),
    /// 计划引用了未登记的 canonical source。
    #[error("RedisJob source 未登记: {0}")]
    UnknownSource(String),
    /// 计划引用了部署显式关闭的 source。
    #[error("RedisJob source 已禁用: {0}")]
    SourceDisabled(String),
    /// 持久记录、客户端与当前运行时声明的 source 不一致。
    #[error("RedisJob source 不一致: {0}")]
    SourceMismatch(String),
    /// Lua 返回、字段、键布局或持久状态不满足协议合同。
    #[error("RedisJob 协议不一致: {0}")]
    Protocol(String),
    /// Worker 修订、Schema、codec 或定义摘要不兼容。
    #[error("RedisJob Worker 合同不兼容: {0}")]
    ContractMismatch(String),
    /// 持久 payload 的外层编码、结构预算或业务解码不合法。
    #[error("RedisJob payload 不合法: {0}")]
    InvalidPayload(String),
    /// 当前冻结快照没有满足合同且可接收 Fanout 的执行器。
    #[error("RedisJob 没有兼容执行器: {0}")]
    NoCapableExecutor(String),
    /// attempt owner 已变化，迟到提交不得覆盖新 owner。
    #[error("RedisJob owner 已失效: {0}")]
    StaleOwner(String),
    /// Fanout assignment epoch 已变化，迟到分片不得继续提交。
    #[error("RedisJob assignment 已失效: {0}")]
    StaleAssignment(String),
    /// fencing token 或稳定执行身份出现倒退、不一致。
    #[error("RedisJob fencing 不变量不成立: {0}")]
    FencingRegression(String),
    /// 当前执行的本地副作用门禁已经关闭。
    #[error("RedisJob 执行已停止: {0}")]
    ExecutionStopped(String),
    /// 控制命令可能已写出，必须读取权威状态后再决策。
    #[error("RedisJob 执行结局未知: {0}")]
    ExecutionUnknown(String),
    /// 受管停机未能在共享绝对截止前收口。
    #[error("RedisJob 停机超过截止: {0}")]
    ShutdownDeadline(String),
}
