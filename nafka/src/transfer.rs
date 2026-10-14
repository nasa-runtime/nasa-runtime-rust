//! 固定来源范围、目标身份及原文策略的单目标转发合同。

pub(crate) mod dependencies;
mod runtime;
mod trace;
pub(crate) mod wire;
use crate::{ReplayObservation, ReplayPlan, ReplayPosition, Tp};
pub(crate) use runtime::{replay_error, Delivery, TargetInspection};
pub use runtime::{TransferHandle, TransferRegistries, TransferStatus};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
pub use trace::{TransferResource, TransferTrace};

/// 转发的稳定失败类别，不携带 schema、消息正文或连接凭据。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferError {
    /// broker 明确拒绝来源或目标资源权限。
    Authorization,
    /// 声明不完整、越界或有歧义。
    InvalidConfiguration,
    /// 持久计划损坏、截断或非规范。
    CorruptPlan,
    /// 处理摘要与目的合同不一致。
    ContractMismatch,
    /// 目标物理身份或 topic 代次改变。
    TargetIdentityChanged,
    /// 目标完整分区集合改变。
    TargetPartitionsChanged,
    /// 当前证据不可观测。
    ObservationFailed,
    /// 总预算耗尽。
    Timeout,
    /// 目标 lane 不提供 broker 确认。
    UnconfirmedLane,
    /// 来源 schema 不在批准映射中。
    SchemaUnapproved,
    /// 来源与目标 schema 不满足固定正文合同。
    SchemaMismatch,
    /// 来源链路证据不完整、重复或非法。
    HeaderInvalid,
    /// 目标资源已经在经过路径中。
    LoopDetected,
    /// 转发跳数达到上限。
    HopLimit,
    /// 目标交付未成功。
    DeliveryFailed,
    /// 目标实际副作用无法确定。
    OutcomeUnknown,
    /// 依赖来源已关闭准入。
    Closed,
    /// 转发任务或证据容量耗尽。
    Capacity,
}
impl std::fmt::Display for TransferError {
    /// 业务作用：对转发失败提供脱敏稳定诊断。参数说明：formatter 为输出目标。返回：固定类别，不包含底层异常正文。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Kafka transfer: {self:?}")
    }
}
impl std::error::Error for TransferError {}
/// 转发合同及执行的显式结果域。
pub type TransferResult<T> = std::result::Result<T, TransferError>;

/// 一条固定来源分区到目标分区的白名单路由。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferRoute {
    /// 冻结来源坐标。
    pub source: Tp,
    /// 控制面批准的目标坐标。
    pub destination: Tp,
}

/// 目标资源完整身份；不断增长的目标 offset 不参与身份判断。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTarget {
    /// 逻辑目标源名。
    pub source_id: String,
    /// broker 物理集群 ID。
    pub cluster_id: String,
    /// 固定独立发布 lane。
    pub lane: String,
    /// 能识别删除重建的当前外部资源代次。
    pub topic_generations: BTreeMap<String, String>,
    /// 所选目标 topic 的完整、严格排序分区集合。
    pub partitions: Vec<Tp>,
}
impl TransferTarget {
    /// 业务作用：从完整只读观测提取目标资源身份，排除不断移动的目标位点。
    /// 参数说明：source_id/lane 为显式控制面选择，observation 为新观测，generations 为当前外部目录证据。
    /// 返回：合法固定目标；不发送消息或取得所有权。
    pub fn observed(
        source_id: String,
        lane: String,
        observation: &ReplayObservation,
        generations: BTreeMap<String, String>,
    ) -> TransferResult<Self> {
        observation
            .validate()
            .map_err(|_| TransferError::InvalidConfiguration)?;
        let target = Self {
            source_id,
            lane,
            cluster_id: observation.cluster_id.clone(),
            topic_generations: generations,
            partitions: observation
                .partitions
                .iter()
                .map(|partition| partition.tp.clone())
                .collect(),
        };
        target.validate()?;
        Ok(target)
    }
    /// 业务作用：在使用公开可变字段之前拒绝不完整或非规范目标。
    /// 参数说明：无。返回：逻辑身份、外部代次和完整有序分区合法时通过。
    pub fn validate(&self) -> TransferResult<()> {
        token(&self.source_id, 128)?;
        token(&self.cluster_id, 1024)?;
        if self.lane.is_empty()
            || self.lane.len() > 64
            || !self.lane.as_bytes()[0].is_ascii_lowercase()
            || !self.lane.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            })
            || self.partitions.is_empty()
            || self.partitions.len() > 4096
            || self.partitions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(TransferError::InvalidConfiguration);
        }
        for partition in &self.partitions {
            valid_partition(partition)?;
        }
        let topics: BTreeSet<_> = self
            .partitions
            .iter()
            .map(|partition| &partition.topic)
            .collect();
        if topics != self.topic_generations.keys().collect() {
            return Err(TransferError::InvalidConfiguration);
        }
        for generation in self.topic_generations.values() {
            token(generation, 1024)?;
        }
        Ok(())
    }
    /// 业务作用：发送前拒绝错连、同名重建和目标扩容，不重算已经批准的分区映射。
    /// 参数说明：current 为完整新观测，generations 为重新查询的当前外部代次。
    /// 返回：物理身份、代次和完整分区集合全部相同才通过，目标日志增长不影响合同。
    pub fn validate_current(
        &self,
        current: &ReplayObservation,
        generations: &BTreeMap<String, String>,
    ) -> TransferResult<()> {
        self.validate()?;
        current
            .validate()
            .map_err(|_| TransferError::ObservationFailed)?;
        if self.cluster_id != current.cluster_id || self.topic_generations != *generations {
            return Err(TransferError::TargetIdentityChanged);
        }
        if !self
            .partitions
            .iter()
            .eq(current.partitions.iter().map(|partition| &partition.tp))
        {
            return Err(TransferError::TargetPartitionsChanged);
        }
        Ok(())
    }
}

/// Confluent schema 类别；转发不自动执行 schema 演进。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferSchemaKind {
    /// Avro schema。
    Avro,
    /// Protobuf schema，正文和 message-indexes 含义保持不变。
    Protobuf,
    /// JSON Schema。
    Json,
}
/// 批准的跨 Registry ID 映射及共同 schema 正文证据。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferSchemaMapping {
    /// 来源正 schema ID。
    pub source_id: i32,
    /// 目标正 schema ID。
    pub destination_id: i32,
    /// 双方一致的 schema 类别。
    pub kind: TransferSchemaKind,
    /// 双方同一 UTF-8 schema 正文的 SHA-256 小写十六进制。
    pub schema_fingerprint: String,
}
/// 明确原文含义，不能靠猜测消息首字节选择跨 Registry 策略。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferPayloadPolicy {
    /// 原文保留；业务承担其含义，不把源 Registry ID 自动解释为目标 ID。
    Opaque,
    /// 原文采用 Confluent envelope，只重写经过双端 schema 复验的四字节 ID。
    Confluent {
        /// 稳定来源 Registry 名。
        source_registry: String,
        /// 稳定目标 Registry 名。
        destination_registry: String,
        /// 按来源 ID 严格递增的批准集合，最多4096项。
        schemas: Vec<TransferSchemaMapping>,
    },
}
impl TransferPayloadPolicy {
    /// 业务作用：建立排序且无歧义的明确 schema 转发策略。
    /// 参数说明：两个 registry 为稳定逻辑身份，schemas 为批准的 ID 与共同正文映射。
    /// 返回：合法策略；不会查询、注册或修改任一 Registry。
    pub fn confluent(
        source_registry: String,
        destination_registry: String,
        mut schemas: Vec<TransferSchemaMapping>,
    ) -> TransferResult<Self> {
        schemas.sort_by_key(|schema| schema.source_id);
        let policy = Self::Confluent {
            source_registry,
            destination_registry,
            schemas,
        };
        policy.validate()?;
        Ok(policy)
    }
    /// 业务作用：拒绝公开字段中的重复 ID、无效摘要或缺失 Registry 身份。
    /// 参数说明：无。返回：策略完整且有界时通过。
    pub fn validate(&self) -> TransferResult<()> {
        if let Self::Confluent {
            source_registry,
            destination_registry,
            schemas,
        } = self
        {
            token(source_registry, 128)?;
            token(destination_registry, 128)?;
            if schemas.is_empty()
                || schemas.len() > 4096
                || schemas
                    .windows(2)
                    .any(|pair| pair[0].source_id >= pair[1].source_id)
                || schemas.iter().any(|schema| {
                    schema.source_id <= 0
                        || schema.destination_id <= 0
                        || !fingerprint(&schema.schema_fingerprint)
                })
            {
                return Err(TransferError::InvalidConfiguration);
            }
        }
        Ok(())
    }
}

/// 冻结单目标 at-least-once 转发；同集群目的也不自动升级成事务。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferPlan {
    /// 已绑定转发成功合同的有限来源范围。
    pub replay: ReplayPlan,
    /// 完整目标物理身份与固定 lane。
    pub target: TransferTarget,
    /// 按来源坐标严格排序且覆盖全部来源分区的路由。
    pub routes: Vec<TransferRoute>,
    /// 明确原文与 schema ID 策略。
    pub payload_policy: TransferPayloadPolicy,
    /// 最多允许的转发次数，范围1到16。
    pub max_hops: u8,
}
impl TransferPlan {
    /// 共同完整计划的最大字节数。
    pub const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;

    /// 业务作用：保留来源冻结范围，使用内置目标成功条件重新绑定 checkpoint 合同。
    /// 参数说明：source_ranges 为只读来源范围，target/routes/payload_policy/max_hops 决定目标及处理语义。
    /// 返回：新转发合同；不沿用原范围计划的 handler 指纹，已有不同 checkpoint 会明确冲突。
    pub fn freeze(
        mut source_ranges: ReplayPlan,
        target: TransferTarget,
        mut routes: Vec<TransferRoute>,
        payload_policy: TransferPayloadPolicy,
        max_hops: u8,
    ) -> TransferResult<Self> {
        source_ranges
            .validate()
            .map_err(|_| TransferError::InvalidConfiguration)?;
        if routes.len() != source_ranges.ranges.len() {
            return Err(TransferError::InvalidConfiguration);
        }
        routes.sort_by(|left, right| left.source.cmp(&right.source));
        let spec = spec(&target, &routes, &payload_policy, max_hops)?;
        source_ranges.pipeline_fingerprint = format!("{:x}", Sha256::digest(spec));
        let plan = Self {
            replay: source_ranges,
            target,
            routes,
            payload_policy,
            max_hops,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// 业务作用：阻止恢复时静默更换目的地、分区或处理策略。
    /// 参数说明：无。返回：来源范围、完整路由、目标白名单和处理摘要全部一致才通过。
    pub fn validate(&self) -> TransferResult<()> {
        self.replay
            .validate()
            .map_err(|_| TransferError::InvalidConfiguration)?;
        let bytes = spec(
            &self.target,
            &self.routes,
            &self.payload_policy,
            self.max_hops,
        )?;
        if self.routes.len() != self.replay.ranges.len() {
            return Err(TransferError::InvalidConfiguration);
        }
        let allowed: BTreeSet<_> = self.target.partitions.iter().collect();
        let mut used = BTreeSet::new();
        for (route, range) in self.routes.iter().zip(&self.replay.ranges) {
            if route.source.topic != range.topic
                || route.source.partition != range.partition
                || !allowed.contains(&route.destination)
            {
                return Err(TransferError::InvalidConfiguration);
            }
            used.insert(&route.destination.topic);
        }
        if used != self.target.topic_generations.keys().collect() {
            return Err(TransferError::InvalidConfiguration);
        }
        if self.replay.pipeline_fingerprint != format!("{:x}", Sha256::digest(bytes)) {
            return Err(TransferError::ContractMismatch);
        }
        Ok(())
    }

    /// 业务作用：保存完整转发合同，恢复不重新计算来源 latest 或目标映射。
    /// 参数说明：无。返回：NSKTPL01 规范正文与SHA-256尾部，最大16MiB；摘要不承担认证。
    pub fn encode(&self) -> TransferResult<Vec<u8>> {
        self.validate()?;
        let ranges = self
            .replay
            .encode()
            .map_err(|_| TransferError::InvalidConfiguration)?;
        let mut bytes = b"NSKTPL01".to_vec();
        bytes.extend_from_slice(&(ranges.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&ranges);
        bytes.extend_from_slice(&spec(
            &self.target,
            &self.routes,
            &self.payload_policy,
            self.max_hops,
        )?);
        if bytes.len() + 32 > Self::MAX_ENCODED_BYTES {
            return Err(TransferError::InvalidConfiguration);
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        Ok(bytes)
    }

    /// 业务作用：从受控持久字节恢复原冻结合同，不授予执行权。
    /// 参数说明：bytes 为完整编码。返回：规范合法计划；损坏、截断、无界字段或摘要不一致均为CorruptPlan。
    pub fn decode(bytes: &[u8]) -> TransferResult<Self> {
        wire::decode(bytes)
    }
}

/// 业务作用：规范编码目的地及处理语义，作为来源checkpoint的处理指纹。
/// 参数说明：target/routes/policy/hops 为固定转发字段。返回：已校验、有界、排序且采用大端整数的共同正文。
pub(crate) fn spec(
    target: &TransferTarget,
    routes: &[TransferRoute],
    policy: &TransferPayloadPolicy,
    hops: u8,
) -> TransferResult<Vec<u8>> {
    target.validate()?;
    policy.validate()?;
    if !(1..=16).contains(&hops)
        || routes.is_empty()
        || routes.len() > 4096
        || routes
            .windows(2)
            .any(|pair| pair[0].source >= pair[1].source)
    {
        return Err(TransferError::InvalidConfiguration);
    }
    let mut bytes = b"NSKTSP01".to_vec();
    text(&mut bytes, &target.source_id);
    text(&mut bytes, &target.cluster_id);
    text(&mut bytes, &target.lane);
    bytes.push(hops);
    bytes.extend_from_slice(&(target.topic_generations.len() as u32).to_be_bytes());
    for (topic, generation) in &target.topic_generations {
        text(&mut bytes, topic);
        text(&mut bytes, generation);
    }
    bytes.extend_from_slice(&(target.partitions.len() as u32).to_be_bytes());
    for tp in &target.partitions {
        partition(&mut bytes, tp);
    }
    bytes.extend_from_slice(&(routes.len() as u32).to_be_bytes());
    for route in routes {
        valid_partition(&route.source)?;
        valid_partition(&route.destination)?;
        partition(&mut bytes, &route.source);
        partition(&mut bytes, &route.destination);
    }
    match policy {
        TransferPayloadPolicy::Opaque => {
            bytes.push(0);
            text(&mut bytes, "");
            text(&mut bytes, "");
            bytes.extend_from_slice(&0u32.to_be_bytes());
        }
        TransferPayloadPolicy::Confluent {
            source_registry,
            destination_registry,
            schemas,
        } => {
            bytes.push(1);
            text(&mut bytes, source_registry);
            text(&mut bytes, destination_registry);
            bytes.extend_from_slice(&(schemas.len() as u32).to_be_bytes());
            for schema in schemas {
                bytes.extend_from_slice(&schema.source_id.to_be_bytes());
                bytes.extend_from_slice(&schema.destination_id.to_be_bytes());
                bytes.push(match schema.kind {
                    TransferSchemaKind::Avro => 1,
                    TransferSchemaKind::Protobuf => 2,
                    TransferSchemaKind::Json => 3,
                });
                for index in (0..64).step_by(2) {
                    bytes.push(
                        u8::from_str_radix(&schema.schema_fingerprint[index..index + 2], 16)
                            .map_err(|_| TransferError::InvalidConfiguration)?,
                    );
                }
            }
        }
    }
    Ok(bytes)
}
/// 业务作用：限制跨语言稳定身份到有界可见ASCII。参数说明：value为身份，max为最大字节数。返回：合法时通过。
fn token(value: &str, max: usize) -> TransferResult<()> {
    if value.is_empty()
        || value.len() > max
        || !value.bytes().all(|byte| (33..=126).contains(&byte))
    {
        Err(TransferError::InvalidConfiguration)
    } else {
        Ok(())
    }
}
/// 业务作用：核对共同SHA-256文本格式。参数说明：value为摘要。返回：仅64位小写十六进制为true。
fn fingerprint(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
/// 业务作用：拒绝非法Kafka映射坐标。参数说明：tp为显式分区。返回：合法topic与非负分区通过。
fn valid_partition(tp: &Tp) -> TransferResult<()> {
    ReplayPosition {
        topic: tp.topic.clone(),
        partition: tp.partition,
        start_offset: 0,
        end_offset: 0,
        next_offset: 0,
    }
    .validate()
    .map_err(|_| TransferError::InvalidConfiguration)
}
/// 业务作用：编码合法分区坐标。参数说明：bytes为内存缓冲，tp为坐标。返回：追加topic长度前缀与大端分区。
fn partition(bytes: &mut Vec<u8>, tp: &Tp) {
    text(bytes, &tp.topic);
    bytes.extend_from_slice(&tp.partition.to_be_bytes());
}
/// 业务作用：生成身份文本的无歧义共同表达。参数说明：bytes为内存缓冲，value为已校验文本。返回：追加两字节长度与原字节。
fn text(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}
