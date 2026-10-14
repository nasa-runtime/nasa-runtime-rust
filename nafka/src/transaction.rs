//! 同一物理 Kafka 集群内的专属事务合同；普通发布和外部数据库不属于原子范围。

use crate::{AckMode, Tp};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) mod context;
pub use context::{TransactionContext, TransactionOutput, TransactionProcessor};
pub(crate) mod runtime;
pub use runtime::{TransactionHandle, TransactionState, TransactionStatus};

/// 事务阶段的固定失败域，不携带原生凭据或消息正文。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionError {
    /// 合同缺失、越界或编码不唯一。
    InvalidConfiguration,
    /// 来源或任务已停止新准入。
    Closed,
    /// 组或稳定事务身份仍被实际 owner 占用。
    Busy,
    /// 任务、输入或暂存输出额度耗尽。
    Capacity,
    /// 实际物理集群与批准合同不同。
    ClusterMismatch,
    /// 显式输入隔离配置与 read_committed 冲突。
    IsolationMismatch,
    /// broker 身份认证或授权失败。
    Authorization,
    /// producer 或静态 consumer 身份已被更新代次取代。
    Fenced,
    /// 输入所属的组权威发生变化。
    Rebalanced,
    /// 同步业务回调失败或 panic。
    HandlerFailed,
    /// 回调超过本批业务预算，不能在晚到返回后提交。
    HandlerTimeout,
    /// 事务准备、原生操作或等待预算耗尽。
    TransactionTimeout,
    /// Manual 模式未确认全部输入。
    MissingAck,
    /// 上下文已封口，不能晚到确认或追加发送。
    ContextClosed,
    /// 取消撤销了尚未进入 commit 的批次。
    Cancelled,
    /// commit 已开始但最终确认未知，只能保护 owner 并按稳定身份恢复。
    CommitUnknown,
    /// abort 未取得确认，不能开始新事务猜测旧事务结果。
    AbortUnknown,
    /// 其它不能作为业务成功证据的原生失败。
    BrokerError,
}

impl std::fmt::Display for TransactionError {
    /// 业务作用：向业务提供固定且脱敏的事务失败文本。
    /// 参数说明：formatter 为输出目标。返回：仅包含固定错误类别。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Kafka transaction: {self:?}")
    }
}
impl std::error::Error for TransactionError {}

/// 事务专属结果域，不复用普通发布 Delivered。
pub type TransactionResult<T> = std::result::Result<T, TransactionError>;

/// 崩溃后复用的稳定部署身份；同时活动的部署槽必须互异。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionIdentity {
    /// 稳定应用身份，最多256 UTF-8字节。
    pub application: String,
    /// 稳定部署槽，不能使用每次启动变化的随机值。
    pub slot: String,
    /// 同槽内唯一事务工作单元。
    pub owner: String,
}

impl TransactionIdentity {
    /// 业务作用：按共同编码派生 Java 与 Rust 可以互相接管的事务 ID。
    /// 参数说明：无。返回：NSKTXI01 与三个大端 u16 长度前缀 UTF-8 字段的 SHA-256，以 nasa-tx- 开头；空值、NUL 或超长字段拒绝。
    pub fn transactional_id(&self) -> TransactionResult<String> {
        let mut bytes = b"NSKTXI01".to_vec();
        for value in [&self.application, &self.slot, &self.owner] {
            text(value, 256)?;
            bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        Ok(format!("nasa-tx-{:x}", Sha256::digest(bytes)))
    }
}

/// 每个 owner 的完整输入、暂存输出及确认中止后重试预算。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionLimits {
    /// 每批输入记录数，1至4096。
    pub max_batch_records: u32,
    /// 每批独立输入字节，1至64MiB。
    pub max_batch_bytes: u64,
    /// 每批暂存输出数，1至4096。
    pub max_output_records: u32,
    /// 每批暂存输出字节，1至64MiB。
    pub max_output_bytes: u64,
    /// 同步回调预算；耗尽不证明回调线程已退出。
    pub handler_timeout_ms: u32,
    /// 整批期限与 broker 事务超时，1000至900000毫秒。
    pub transaction_timeout_ms: u32,
    /// 原生阻塞操作预算，不能覆盖事务剩余期限。
    pub operation_timeout_ms: u32,
    /// 只有确认 abort 后才能使用的重试次数，0至100。
    pub max_aborted_retries: u32,
    /// 整批确认中止后的退避，10至60000毫秒。
    pub retry_backoff_ms: u32,
}

impl Default for TransactionLimits {
    /// 业务作用：提供双端一致且有界的单 owner 初始预算。
    /// 参数说明：无。返回：100条输入、1000条输出、8MiB输入、16MiB输出、10秒回调、60秒事务与3次中止重试。
    fn default() -> Self {
        Self {
            max_batch_records: 100,
            max_batch_bytes: 8 * 1024 * 1024,
            max_output_records: 1000,
            max_output_bytes: 16 * 1024 * 1024,
            handler_timeout_ms: 10000,
            transaction_timeout_ms: 60000,
            operation_timeout_ms: 10000,
            max_aborted_retries: 3,
            retry_backoff_ms: 250,
        }
    }
}

impl TransactionLimits {
    /// 业务作用：对公开可变预算重新验证，避免溢出或未给提交留下时间。
    /// 参数说明：无。返回：所有上限合法且回调与一次原生操作之和小于事务时限才通过。
    pub fn validate(&self) -> TransactionResult<()> {
        if !(1..=4096).contains(&self.max_batch_records)
            || !(1..=4096).contains(&self.max_output_records)
            || !(1..=64 * 1024 * 1024).contains(&self.max_batch_bytes)
            || !(1..=64 * 1024 * 1024).contains(&self.max_output_bytes)
            || !(1000..=900000).contains(&self.transaction_timeout_ms)
            || self.handler_timeout_ms == 0
            || self.operation_timeout_ms == 0
            || u64::from(self.handler_timeout_ms) + u64::from(self.operation_timeout_ms)
                >= u64::from(self.transaction_timeout_ms)
            || self.max_aborted_retries > 100
            || !(10..=60000).contains(&self.retry_backoff_ms)
        {
            return Err(TransactionError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// 同集群 consume-transform-produce 的完整白名单与确认合同。
#[derive(Clone, Debug)]
pub struct TransactionPlan {
    /// 已装配的命名 Kafka 源。
    pub source_id: String,
    /// 批准的物理 Kafka 集群 ID。
    pub cluster_id: String,
    /// 崩溃后复用的事务身份。
    pub identity: TransactionIdentity,
    /// 专属 subscribe 消费组，遵守共同实际组输入域，不允许普通提交协调器同时使用。
    pub group: String,
    /// 1至256个明确输入 topic，不支持手动 assign。
    pub input_topics: BTreeSet<String>,
    /// 1至4096个明确输出分区；DLT 也必须属于此集合。
    pub outputs: BTreeSet<Tp>,
    /// Auto 在回调正常返回后确认整批，Manual 还要求全部输入显式确认。
    pub ack_mode: AckMode,
    /// 单批的有限资源合同。
    pub limits: TransactionLimits,
}

impl TransactionPlan {
    /// 业务作用：执行前冻结并验证输入组、物理集群和全部输出范围。
    /// 参数说明：无。返回：完整合法合同才通过，组名原样保留；组或其它声明非法时返回 InvalidConfiguration，不取得组权威、不发送数据。
    pub fn validate(&self) -> TransactionResult<()> {
        text(&self.source_id, 256)?;
        text(&self.cluster_id, 256)?;
        // 组名决定真实消费和提交责任，计划必须复用运行时边界，并保留事务专属错误分类。
        crate::wire::validate_group(&self.group)
            .map_err(|_| TransactionError::InvalidConfiguration)?;
        self.identity.transactional_id()?;
        self.limits.validate()?;
        if self.input_topics.is_empty()
            || self.input_topics.len() > 256
            || self.outputs.is_empty()
            || self.outputs.len() > 4096
        {
            return Err(TransactionError::InvalidConfiguration);
        }
        for input in &self.input_topics {
            topic(input)?;
        }
        for output in &self.outputs {
            topic(&output.topic)?;
            if output.partition < 0 {
                return Err(TransactionError::InvalidConfiguration);
            }
        }
        Ok(())
    }
}

/// 业务作用：确保共同身份字段不被底层 C 字符串截断或超出声明上限。
/// 参数说明：value 为精确文本，max 为 UTF-8字节上限。返回：空、NUL 或超长字段拒绝，不做隐式裁剪。
fn text(value: &str, max: usize) -> TransactionResult<()> {
    if value.is_empty() || value.len() > max || value.contains('\0') {
        return Err(TransactionError::InvalidConfiguration);
    }
    Ok(())
}

/// 业务作用：验证明确业务 topic，防止非法目标穿透白名单。
/// 参数说明：value 为输入或输出 topic。返回：1至249字节的合法 Kafka ASCII 名称。
fn topic(value: &str) -> TransactionResult<()> {
    if value.is_empty()
        || value.len() > 249
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(TransactionError::InvalidConfiguration);
    }
    Ok(())
}
