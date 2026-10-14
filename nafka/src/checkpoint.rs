//! 回放任务的有界持久进度与条件更新合同。

#[cfg(unix)]
mod file;
#[cfg(feature = "mysql-checkpoint")]
mod mysql;
mod wire;
#[cfg(unix)]
pub use file::FileCheckpointStore;
#[cfg(feature = "mysql-checkpoint")]
pub use mysql::MySqlCheckpointStore;

/// checkpoint 操作的封闭错误，不附带任务正文、路径或存储异常。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointError {
    /// 输入身份、范围或资源声明非法。
    InvalidConfiguration,
    /// 任务已有活动所有者。
    Busy,
    /// 恢复合同、分区集合或冻结范围不一致。
    ContractMismatch,
    /// 条件更新序号落后，不产生存储副作用。
    StaleRevision,
    /// 当前 lease 已失去推进权限或无法复验原持久证据。
    Fenced,
    /// 持久证据格式、完整性或范围非法。
    Corrupt,
    /// 存储读取、锁或资源关闭失败。
    Io,
    /// 持久化结果未知，不能以原内存状态继续写入。
    OutcomeUnknown,
    /// 当前 lease 已永久关闭。
    Closed,
    /// 活动任务数、序号或文件容量已耗尽。
    Capacity,
}

impl std::fmt::Display for CheckpointError {
    /// 业务作用：以固定类别输出可操作的持久化失败。
    /// 参数说明：formatter 为诊断输出。
    /// 返回：不包含原始文件路径或 checkpoint 内容的错误文本。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Kafka checkpoint: {self:?}")
    }
}
impl std::error::Error for CheckpointError {}

/// checkpoint 操作结果；未知写入与明确拒绝通过错误枚举区分。
pub type CheckpointResult<T> = std::result::Result<T, CheckpointError>;

/// 单分区冻结范围与下一次恢复坐标，采用左闭右开区间。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayPosition {
    /// 来源 topic，限制为 Kafka 接受的 ASCII 名称。
    pub topic: String,
    /// 非负分区号。
    pub partition: i32,
    /// 冻结开始位置。
    pub start_offset: i64,
    /// 冻结结束位置，不包含此 offset。
    pub end_offset: i64,
    /// 已满足任务成功条件后的下一待处理位置。
    pub next_offset: i64,
}

impl ReplayPosition {
    /// 业务作用：拒绝越过授权范围或不能表示 Kafka 坐标的进度。
    /// 参数说明：无。
    /// 返回：名称、分区和范围有效时成功，否则不允许写入存储。
    pub fn validate(&self) -> CheckpointResult<()> {
        if self.topic.is_empty()
            || self.topic.len() > 249
            || matches!(self.topic.as_str(), "." | "..")
            || !self.topic.bytes().all(identifier_byte)
            || self.partition < 0
            || self.start_offset < 0
            || self.end_offset < self.start_offset
            || self.next_offset < self.start_offset
            || self.next_offset > self.end_offset
        {
            return Err(CheckpointError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// 冻结任务合同及完整分区进度，generation 表示独占权代次，revision 表示持久条件更新序号。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayCheckpoint {
    /// 稳定任务 ID，最多 128 个 ASCII 字符。
    pub task_id: String,
    /// 完整回放合同的 SHA-256 小写十六进制。
    pub contract_fingerprint: String,
    /// 独占权代次；未登记声明为零，已持久化快照必须为正。
    pub generation: u64,
    /// 条件更新序号，不超过 i64 最大值以保持跨语言范围一致。
    pub revision: u64,
    /// 按 topic 和分区严格递增的完整集合，最多 4096 项。
    pub positions: Vec<ReplayPosition>,
}

impl ReplayCheckpoint {
    /// 最大冻结分区数，同时约束恢复解析和内存占用。
    pub const MAX_PARTITIONS: usize = 4096;

    /// 业务作用：固定任务身份和完整有界范围，并生成共同排序。
    /// 参数说明：task_id 与 contract_fingerprint 为稳定合同；generation/revision 为所有权与更新序号；positions 为完整分区集合。
    /// 返回：合法规范化快照；重复坐标、非法身份或范围拒绝。
    pub fn new(
        task_id: String,
        contract_fingerprint: String,
        generation: u64,
        revision: u64,
        mut positions: Vec<ReplayPosition>,
    ) -> CheckpointResult<Self> {
        positions.sort_by(|left, right| {
            (&left.topic, left.partition).cmp(&(&right.topic, right.partition))
        });
        let checkpoint = Self {
            task_id,
            contract_fingerprint,
            generation,
            revision,
            positions,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// 业务作用：在读写存储边界复验可由业务构造的快照，不能信任可变公开字段。
    /// 参数说明：无。
    /// 返回：身份、序号、范围和严格排序均合法时成功。
    pub fn validate(&self) -> CheckpointResult<()> {
        validate_task(&self.task_id)?;
        if self.contract_fingerprint.len() != 64
            || !self
                .contract_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.generation > i64::MAX as u64
            || self.revision > i64::MAX as u64
            || self.positions.is_empty()
            || self.positions.len() > Self::MAX_PARTITIONS
        {
            return Err(CheckpointError::InvalidConfiguration);
        }
        for position in &self.positions {
            position.validate()?;
        }
        if self
            .positions
            .windows(2)
            .any(|pair| (&pair[0].topic, pair[0].partition) >= (&pair[1].topic, pair[1].partition))
        {
            return Err(CheckpointError::InvalidConfiguration);
        }
        Ok(())
    }

    /// 业务作用：判断冻结范围是否全部形成已确认进度。
    /// 参数说明：无。
    /// 返回：全部 next_offset 到达 end_offset 时为 true，不要求日志存在 end_offset-1 的记录。
    pub fn complete(&self) -> bool {
        self.validate().is_ok()
            && self
                .positions
                .iter()
                .all(|position| position.next_offset == position.end_offset)
    }

    /// 业务作用：阻止恢复或推进时静默替换任务处理合同及读取范围。
    /// 参数说明：other 为待比较快照。
    /// 返回：忽略代次、序号及进度，其余身份和范围必须完全相同。
    pub fn same_contract(&self, other: &Self) -> bool {
        self.validate().is_ok()
            && other.validate().is_ok()
            && self.task_id == other.task_id
            && self.contract_fingerprint == other.contract_fingerprint
            && self.positions.len() == other.positions.len()
            && self
                .positions
                .iter()
                .zip(&other.positions)
                .all(|(left, right)| {
                    left.topic == right.topic
                        && left.partition == right.partition
                        && left.start_offset == right.start_offset
                        && left.end_offset == right.end_offset
                })
    }
}

/// 回放持久存储接口；共享后端必须实现原子 claim/CAS 及真实持久化，调用由独立有界执行资源承担。
pub trait CheckpointStore: Send + Sync {
    /// 业务作用：只读查看持久进度，不授予业务副作用权限。
    /// 参数说明：task_id 为稳定任务名。
    /// 返回：完整快照或不存在；损坏、身份不一致或读取失败明确拒绝。
    fn load(&self, task_id: &str) -> CheckpointResult<Option<ReplayCheckpoint>>;

    /// 业务作用：独占任务并持久化新的所有权代次后交付推进权。
    /// 参数说明：initial 为 generation/revision 为零且所有进度位于起点的冻结声明。
    /// 返回：只有独占权和持久证据均成立才返回 lease；活动任务、合同变化或损坏拒绝。
    fn claim(&self, initial: &ReplayCheckpoint) -> CheckpointResult<Box<dyn CheckpointLease>>;
}

/// 覆盖实际任务寿命的独占推进权，关闭之前必须完成或明确停止全部在途业务责任。
pub trait CheckpointLease: Send {
    /// 业务作用：读取当前 lease 最后确认持久化的完整进度。
    /// 参数说明：无。
    /// 返回：独立快照；关闭后读取不重新授予推进权限。
    fn checkpoint(&self) -> ReplayCheckpoint;

    /// 业务作用：在新业务副作用前复验当前所有权与最后持久证据，历史快照不能授权继续执行。
    /// 参数说明：无。返回：可证明的推进权仍成立时成功；失权或证据不足后禁止此 lease 后续推进。
    /// 此检查与远端副作用不是原子操作，已经在途的调用仍可能形成至少一次重复。
    fn verify(&mut self) -> CheckpointResult<()>;

    /// 业务作用：在目标成功之后条件推进连续安全进度。
    /// 参数说明：expected_revision 为预期序号，positions 为范围不变的完整新进度。
    /// 返回：持久化后的快照；旧序号、回退、越界、失权或存储未知拒绝继续推进。
    fn advance(
        &mut self,
        expected_revision: u64,
        positions: Vec<ReplayPosition>,
    ) -> CheckpointResult<ReplayCheckpoint>;

    /// 业务作用：归还任务独占责任并永久拒绝旧 lease 的新推进。
    /// 参数说明：无。
    /// 返回：重复关闭无副作用；关闭不能撤销已经发生的远端副作用。
    fn close(&mut self) -> CheckpointResult<()>;
}

/// 业务作用：建立不含目录语义的稳定任务身份。
/// 参数说明：task 为用户声明的 ID。
/// 返回：首字母数字且只含 ASCII 字母数字、点、横线和下划线时成功，最大 128 字符。
fn validate_task(task: &str) -> CheckpointResult<()> {
    if task.is_empty()
        || task.len() > 128
        || !task.as_bytes()[0].is_ascii_alphanumeric()
        || !task.bytes().all(identifier_byte)
    {
        return Err(CheckpointError::InvalidConfiguration);
    }
    Ok(())
}

/// 业务作用：为双端任务名和 topic 采用共同 ASCII 字符边界。
/// 参数说明：byte 为身份中的一个字节。
/// 返回：可安全参与稳定身份的字符为 true。
fn identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

/// 业务作用：避免所有权或更新序号溢出后复用旧权限。
/// 参数说明：value 为当前序号。
/// 返回：下一跨语言有效序号，耗尽时拒绝。
fn increment(value: u64) -> CheckpointResult<u64> {
    if value >= i64::MAX as u64 {
        Err(CheckpointError::Capacity)
    } else {
        Ok(value + 1)
    }
}
