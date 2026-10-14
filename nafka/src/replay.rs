//! 有限回放的只读规划、共同合同、受管执行与持久恢复门禁。

mod inspect;
pub(crate) mod runtime;
mod wire;
use crate::{ReplayCheckpoint, ReplayPosition, Tp};
pub(crate) use inspect::{inspect_consumer, inspection_consumer, observe};
pub use runtime::{
    ReplayHandle, ReplayIdentityProvider, ReplayProcessor, ReplayRecord, ReplayState, ReplayStatus,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// 回放规划、恢复与实际执行的稳定失败原因，不包含连接凭据或消息正文。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayError {
    /// broker 明确拒绝来源资源权限。
    Authorization,
    /// 声明不合法或超出共同预算边界。
    InvalidConfiguration,
    /// 本轮 broker 证据不可完整观测。
    ObservationFailed,
    /// 总查询预算耗尽。
    Timeout,
    /// 来源物理身份或 topic 外部代次变化。
    IdentityChanged,
    /// 冻结分区集合与当前集合不一致。
    PartitionsChanged,
    /// 待处理坐标早于当前保留范围。
    DataGap,
    /// 冻结结束位置超过当前稳定末尾。
    EndRegressed,
    /// checkpoint 与任务合同不一致。
    ContractMismatch,
    /// 时间定位无结果且计划要求拒绝。
    TimestampMissing,
    /// 持久计划损坏、截断或不是规范编码。
    CorruptPlan,
    /// broker 明确报告所选 topic 不存在。
    TopicMissing,
    /// 受管来源已经最终关闭，不得重新建立探测客户端。
    Closed,
    /// 同一来源已有该任务的实际责任。
    Busy,
    /// 任务、worker 或记录预算不足。
    Capacity,
    /// 实际处理未满足成功合同。
    ProcessingFailed,
    /// 持久进度操作失败；具体存储类别由状态独立提供。
    CheckpointFailed,
}

impl std::fmt::Display for ReplayError {
    /// 业务作用：输出不携带敏感内容的稳定规划与执行失败原因。
    /// 参数说明：formatter 为输出目的地。
    /// 返回：固定类别文本。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Kafka replay: {self:?}")
    }
}
impl std::error::Error for ReplayError {}
/// 回放操作成功或具有明确操作含义的失败。
pub type ReplayResult<T> = std::result::Result<T, ReplayError>;

/// 单分区保留范围与可选时间定位结果，stable_end_offset 为 read_committed 的 LSO。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayPartition {
    /// Kafka 坐标。
    pub tp: Tp,
    /// 当前最早保留位置。
    pub beginning_offset: i64,
    /// 本轮稳定结束位置，不包含此 offset。
    pub stable_end_offset: i64,
    /// 时间查询结果；None 表示无匹配或本轮未请求时间查询。
    pub timestamp_offset: Option<i64>,
}

/// 不加入消费组的完整只读观测，不能被解释为已经取得任务执行权。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayObservation {
    /// broker 物理集群 ID。
    pub cluster_id: String,
    /// 本轮时间查询参数。
    pub timestamp_millis: Option<i64>,
    /// 按 topic/partition 严格排序的完整所选 topic 集合。
    pub partitions: Vec<ReplayPartition>,
}

impl ReplayObservation {
    /// 业务作用：规范化有界完整证据，供共同计划冻结和恢复比较。
    /// 参数说明：cluster_id 为物理源，timestamp_millis 为可选非负时间，partitions 为全部所选分区。
    /// 返回：排序后的合法证据；重复或相互矛盾的范围拒绝。
    pub fn new(
        cluster_id: String,
        timestamp_millis: Option<i64>,
        mut partitions: Vec<ReplayPartition>,
    ) -> ReplayResult<Self> {
        partitions.sort_by(|left, right| left.tp.cmp(&right.tp));
        let observation = Self {
            cluster_id,
            timestamp_millis,
            partitions,
        };
        observation.validate()?;
        Ok(observation)
    }

    /// 业务作用：在使用公开可变观测前复验身份和范围，不能信任结构字面量。
    /// 参数说明：无。
    /// 返回：完整且严格排序的证据通过；非法或超额内容拒绝。
    pub fn validate(&self) -> ReplayResult<()> {
        token(&self.cluster_id, 1024)?;
        if self.timestamp_millis.is_some_and(|time| time < 0)
            || self.partitions.is_empty()
            || self.partitions.len() > ReplayCheckpoint::MAX_PARTITIONS
            || self
                .partitions
                .windows(2)
                .any(|pair| pair[0].tp >= pair[1].tp)
        {
            return Err(ReplayError::InvalidConfiguration);
        }
        for partition in &self.partitions {
            ReplayPosition {
                topic: partition.tp.topic.clone(),
                partition: partition.tp.partition,
                start_offset: partition.beginning_offset,
                end_offset: partition.stable_end_offset,
                next_offset: partition.beginning_offset,
            }
            .validate()
            .map_err(|_| ReplayError::InvalidConfiguration)?;
            if partition.timestamp_offset.is_some_and(|offset| offset < 0)
                || self.timestamp_millis.is_none() && partition.timestamp_offset.is_some()
            {
                return Err(ReplayError::InvalidConfiguration);
            }
        }
        Ok(())
    }
}

/// 回放的独立速率和实际责任容量；速率零表示不节流，容量必须为正。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayLimits {
    /// 每秒记录上限。
    pub records_per_second: u64,
    /// 每秒消息字节上限。
    pub bytes_per_second: u64,
    /// 实际在途记录数上限。
    pub max_in_flight_records: u32,
    /// 实际在途字节上限。
    pub max_in_flight_bytes: u64,
    /// 并行分区数量，单分区内部保持串行。
    pub concurrency: u32,
}

impl Default for ReplayLimits {
    /// 业务作用：为历史回放提供独立于在线消费的保守额度。
    /// 参数说明：无。
    /// 返回：每秒 1000 条及 16 MiB、最多 256 条及 16 MiB 在途、单分区执行。
    fn default() -> Self {
        Self {
            records_per_second: 1000,
            bytes_per_second: 16 * 1024 * 1024,
            max_in_flight_records: 256,
            max_in_flight_bytes: 16 * 1024 * 1024,
            concurrency: 1,
        }
    }
}

impl ReplayLimits {
    /// 业务作用：限制跨语言数值边界和执行并行，防止无限驻留责任。
    /// 参数说明：无。
    /// 返回：速率和容量可共同表示且执行并行不超容量时通过。
    pub fn validate(&self) -> ReplayResult<()> {
        if self.records_per_second > i64::MAX as u64
            || self.bytes_per_second > i64::MAX as u64
            || self.max_in_flight_records == 0
            || self.max_in_flight_records > 1_000_000
            || self.max_in_flight_bytes == 0
            || self.max_in_flight_bytes > i64::MAX as u64
            || self.concurrency == 0
            || self.concurrency > 256
            || self.concurrency > self.max_in_flight_records
        {
            return Err(ReplayError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// 时间查询无记录时的显式合同。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissingTimestamp {
    /// 本分区选择本轮冻结 end，形成空范围。
    FreezeEnd,
    /// 无结果即拒绝计划。
    Reject,
}

/// 首次规划的起点；恢复复用原计划和 checkpoint，不重新计算 latest 或时间起点。
#[derive(Clone, Debug)]
pub enum ReplayStart {
    /// 当前仍保留的最早坐标。
    Earliest,
    /// 完整分区起点表。
    Offsets(BTreeMap<Tp, i64>),
    /// 使用观测中的一次时间查询结果。
    Timestamp(MissingTimestamp),
}

/// 可审阅有限任务；topic_generations 来自识别删除重建的外部目录，pipeline_fingerprint 覆盖 schema、转换、过滤、成功条件和目的路由。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayPlan {
    /// 稳定任务 ID。
    pub task_id: String,
    /// 逻辑来源。
    pub source_id: String,
    /// 物理集群身份。
    pub cluster_id: String,
    /// topic 名到外部资源代次；不能以名字或分区数代替。
    pub topic_generations: BTreeMap<String, String>,
    /// 业务处理合同的 SHA-256 小写十六进制。
    pub pipeline_fingerprint: String,
    /// 独立预算。
    pub limits: ReplayLimits,
    /// 初始 next 等于 start 的冻结范围。
    pub ranges: Vec<ReplayPosition>,
}

impl ReplayPlan {
    /// 完整计划归档的最大字节数。
    pub const MAX_ENCODED_BYTES: usize = 8 * 1024 * 1024;

    /// 业务作用：保存完整冻结合同，恢复时不再计算移动末尾或时间起点。
    /// 参数说明：无。
    /// 返回：共同正文及 SHA-256 完整性尾部；摘要不能替代归档存储的写权限。
    pub fn encode(&self) -> ReplayResult<Vec<u8>> {
        let mut bytes = self.payload()?;
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    /// 业务作用：还原持久计划并拒绝截断、损坏、错序和无界声明。
    /// 参数说明：bytes 为完整归档，最大 8 MiB。
    /// 返回：经过完整验证的冻结合同；读取不授予执行权。
    pub fn decode(bytes: &[u8]) -> ReplayResult<Self> {
        wire::decode(bytes)
    }

    /// 业务作用：把一次完整 broker 观测转换为不再移动的历史范围，不启动消费或写入 checkpoint。
    /// 参数说明：task_id/source_id 为身份，observation 为 LSO 证据，topic_generations 为当前外部代次，pipeline_fingerprint 为处理合同，limits 为独立额度，start 为起点策略。
    /// 返回：有界完整计划；缺项、保留缺口、起点超末尾或时间无结果按声明拒绝。
    pub fn freeze(
        task_id: String,
        source_id: String,
        observation: &ReplayObservation,
        topic_generations: BTreeMap<String, String>,
        pipeline_fingerprint: String,
        limits: ReplayLimits,
        start: ReplayStart,
    ) -> ReplayResult<Self> {
        observation.validate()?;
        if let ReplayStart::Offsets(starts) = &start {
            if starts.len() != observation.partitions.len()
                || observation
                    .partitions
                    .iter()
                    .any(|partition| !starts.contains_key(&partition.tp))
            {
                return Err(ReplayError::InvalidConfiguration);
            }
        }
        if matches!(start, ReplayStart::Timestamp(_)) && observation.timestamp_millis.is_none() {
            return Err(ReplayError::InvalidConfiguration);
        }
        let mut ranges = Vec::with_capacity(observation.partitions.len());
        for partition in &observation.partitions {
            let offset = match &start {
                ReplayStart::Earliest => partition.beginning_offset,
                ReplayStart::Offsets(starts) => *starts
                    .get(&partition.tp)
                    .ok_or(ReplayError::InvalidConfiguration)?,
                ReplayStart::Timestamp(policy) => match partition.timestamp_offset {
                    Some(offset) => offset.min(partition.stable_end_offset),
                    None if *policy == MissingTimestamp::FreezeEnd => partition.stable_end_offset,
                    None => return Err(ReplayError::TimestampMissing),
                },
            };
            if offset < 0 {
                return Err(ReplayError::InvalidConfiguration);
            }
            if offset < partition.beginning_offset {
                return Err(ReplayError::DataGap);
            }
            if offset > partition.stable_end_offset {
                return Err(ReplayError::EndRegressed);
            }
            ranges.push(ReplayPosition {
                topic: partition.tp.topic.clone(),
                partition: partition.tp.partition,
                start_offset: offset,
                end_offset: partition.stable_end_offset,
                next_offset: offset,
            });
        }
        let plan = Self {
            task_id,
            source_id,
            cluster_id: observation.cluster_id.clone(),
            topic_generations,
            pipeline_fingerprint,
            limits,
            ranges,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// 业务作用：在公开计划用于指纹或执行前复验完整恢复合同。
    /// 参数说明：无。
    /// 返回：身份、严格排序范围及外部代次一一对应时通过。
    pub fn validate(&self) -> ReplayResult<()> {
        token(&self.source_id, 128)?;
        token(&self.cluster_id, 1024)?;
        self.limits.validate()?;
        let checkpoint = ReplayCheckpoint {
            task_id: self.task_id.clone(),
            contract_fingerprint: self.pipeline_fingerprint.clone(),
            generation: 0,
            revision: 0,
            positions: self.ranges.clone(),
        };
        checkpoint
            .validate()
            .map_err(|_| ReplayError::InvalidConfiguration)?;
        let topics: BTreeSet<&str> = self
            .ranges
            .iter()
            .map(|range| range.topic.as_str())
            .collect();
        if topics != self.topic_generations.keys().map(String::as_str).collect()
            || self
                .ranges
                .iter()
                .any(|range| range.next_offset != range.start_offset)
        {
            return Err(ReplayError::InvalidConfiguration);
        }
        for generation in self.topic_generations.values() {
            token(generation, 1024)?;
        }
        Ok(())
    }

    /// 业务作用：生成 Java/Rust 相同的任务合同摘要，防止恢复静默改变处理语义。
    /// 参数说明：无。
    /// 返回：长度前缀、固定排序和大端整数编码的 SHA-256 小写十六进制；非法公开字段拒绝。
    pub fn fingerprint(&self) -> ReplayResult<String> {
        Ok(format!("{:x}", Sha256::digest(self.payload()?)))
    }

    /// 业务作用：以同一规范正文支持合同摘要和完整持久归档。
    /// 参数说明：无。
    /// 返回：校验后的有界大端字节，不包含运行进度。
    fn payload(&self) -> ReplayResult<Vec<u8>> {
        self.validate()?;
        let mut bytes = b"NSKRPL01".to_vec();
        for value in [
            &self.task_id,
            &self.source_id,
            &self.cluster_id,
            &self.pipeline_fingerprint,
        ] {
            text(&mut bytes, value);
        }
        bytes.extend_from_slice(&(self.topic_generations.len() as u32).to_be_bytes());
        for (topic, generation) in &self.topic_generations {
            text(&mut bytes, topic);
            text(&mut bytes, generation);
        }
        bytes.extend_from_slice(&self.limits.records_per_second.to_be_bytes());
        bytes.extend_from_slice(&self.limits.bytes_per_second.to_be_bytes());
        bytes.extend_from_slice(&self.limits.max_in_flight_records.to_be_bytes());
        bytes.extend_from_slice(&self.limits.max_in_flight_bytes.to_be_bytes());
        bytes.extend_from_slice(&self.limits.concurrency.to_be_bytes());
        bytes.extend_from_slice(&(self.ranges.len() as u32).to_be_bytes());
        for range in &self.ranges {
            text(&mut bytes, &range.topic);
            bytes.extend_from_slice(&range.partition.to_be_bytes());
            bytes.extend_from_slice(&range.start_offset.to_be_bytes());
            bytes.extend_from_slice(&range.end_offset.to_be_bytes());
        }
        Ok(bytes)
    }

    /// 业务作用：生成尚未接管任务的初始持久声明。
    /// 参数说明：无。
    /// 返回：generation/revision 为零且 next 位于 start 的 checkpoint；不获得独占权。
    pub fn initial_checkpoint(&self) -> ReplayResult<ReplayCheckpoint> {
        Ok(ReplayCheckpoint {
            task_id: self.task_id.clone(),
            contract_fingerprint: self.fingerprint()?,
            generation: 0,
            revision: 0,
            positions: self.ranges.clone(),
        })
    }

    /// 业务作用：在接管前拒绝错连、资源重建、分区变化及历史缺口，不重新计算冻结边界。
    /// 参数说明：current 为新 broker 观测，generations 为重新查询的外部代次，checkpoint 为原持久进度。
    /// 返回：所有证据一致且剩余范围可读时通过，不改变存储或消费组。
    pub fn validate_resume(
        &self,
        current: &ReplayObservation,
        generations: &BTreeMap<String, String>,
        checkpoint: &ReplayCheckpoint,
    ) -> ReplayResult<()> {
        self.validate()?;
        current.validate()?;
        if self.cluster_id != current.cluster_id || self.topic_generations != *generations {
            return Err(ReplayError::IdentityChanged);
        }
        if !self.initial_checkpoint()?.same_contract(checkpoint) {
            return Err(ReplayError::ContractMismatch);
        }
        if current.partitions.len() != self.ranges.len() {
            return Err(ReplayError::PartitionsChanged);
        }
        for (saved, observed) in checkpoint.positions.iter().zip(&current.partitions) {
            if saved.topic != observed.tp.topic || saved.partition != observed.tp.partition {
                return Err(ReplayError::PartitionsChanged);
            }
            // 不以 earliest 重置掩盖保留缺口，也不把原 end 替换为新的移动末尾。
            if saved.next_offset < observed.beginning_offset {
                return Err(ReplayError::DataGap);
            }
            if saved.end_offset > observed.stable_end_offset {
                return Err(ReplayError::EndRegressed);
            }
        }
        Ok(())
    }
}

/// 业务作用：限制不透明合同身份到共同 ASCII 编码和有界长度。
/// 参数说明：value 为身份，maximum 为最大字节数。
/// 返回：非空可见 ASCII 时通过。
fn token(value: &str, maximum: usize) -> ReplayResult<()> {
    if value.is_empty()
        || value.len() > maximum
        || !value.bytes().all(|byte| (33..=126).contains(&byte))
    {
        return Err(ReplayError::InvalidConfiguration);
    }
    Ok(())
}

/// 业务作用：为合法合同文本生成无歧义长度前缀。
/// 参数说明：bytes 为内存编码缓冲，value 为已校验文本。
/// 返回：追加两字节长度及 ASCII 内容，不产生外部副作用。
fn text(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}
