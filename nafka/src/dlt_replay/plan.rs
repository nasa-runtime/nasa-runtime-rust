//! 冻结 DLT 来源选择、目标路由及隔离策略；不修改线上消费组。

use super::{DltReplayError as Error, DltReplayResult as Result};
use crate::{ReplayPlan, Tp, TransferPayloadPolicy, TransferPlan, TransferRoute, TransferTarget};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// 非法 DLT 证据的明确处理合同，不包含静默丢弃。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DltInvalidPolicy {
    /// 停止并保留失败坐标。
    Halt,
    /// 原文发送到固定隔离分区，真实确认后才推进；不能与业务回投分区重叠。
    Quarantine(Tp),
}

/// 有限 DLT 重放计划，冻结字段共同参与 checkpoint 指纹。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DltReplayPlan {
    /// 已绑定 DLT 成功合同的固定 LSO 读取范围。
    pub replay: ReplayPlan,
    /// 单一目标源、lane、物理身份及完整分区集合。
    pub target: TransferTarget,
    /// source 指原 DLT 来源 topic/partition，非当前死信分区；必须严格排序且唯一。
    pub routes: Vec<TransferRoute>,
    /// 原来源组的精确筛选，按 UTF-8 字节排序；空集合表示全部组，最多256项。
    pub origin_groups: Vec<String>,
    /// 业务回投的明确正文及 schema 策略；隔离保持原文。
    pub payload_policy: TransferPayloadPolicy,
    /// 非法证据如何隔离，默认应显式选择 Halt。
    pub invalid_policy: DltInvalidPolicy,
    /// 最多允许回投次数，范围1至16。
    pub max_replays: u8,
}

impl DltReplayPlan {
    /// 完整冻结计划的共同容量上限。
    pub const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;

    /// 业务作用：保留只读 DLT 范围，用过滤、回投及隔离语义重绑持久合同。
    /// 参数说明：source_ranges 为固定范围；target 为批准目标；routes 选择原 DLT 坐标；origin_groups 为空表示全部组；payload_policy 为 schema 策略；invalid_policy 为失败隔离；max_replays 为1至16的回投上限。
    /// 返回：排序固定的计划，不沿用原 handler 指纹、不取得 lease 或发送消息。
    pub fn freeze(
        mut source_ranges: ReplayPlan,
        target: TransferTarget,
        mut routes: Vec<TransferRoute>,
        mut origin_groups: Vec<String>,
        payload_policy: TransferPayloadPolicy,
        invalid_policy: DltInvalidPolicy,
        max_replays: u8,
    ) -> Result<Self> {
        source_ranges
            .validate()
            .map_err(|_| Error::InvalidConfiguration)?;
        // 排序前限制可变公开集合，超额声明不能先消耗无界比较和分配成本。
        if routes.is_empty()
            || routes.len() > 4096
            || origin_groups.len() > 256
            || origin_groups
                .iter()
                .any(|group| group.is_empty() || group.len() > 1024)
        {
            return Err(Error::InvalidConfiguration);
        }
        routes.sort_by(|left, right| left.source.cmp(&right.source));
        origin_groups.sort();
        source_ranges.pipeline_fingerprint = format!(
            "{:x}",
            Sha256::digest(spec(
                &target,
                &routes,
                &origin_groups,
                &payload_policy,
                &invalid_policy,
                max_replays
            )?)
        );
        let plan = Self {
            replay: source_ranges,
            target,
            routes,
            origin_groups,
            payload_policy,
            invalid_policy,
            max_replays,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// 业务作用：在执行公开可变字段前验证完整合同，阻止恢复时静默换过滤或输出规则。
    /// 参数说明：无。返回：范围、目标、路由、隔离和处理指纹全部一致才通过。
    pub fn validate(&self) -> Result<()> {
        self.replay
            .validate()
            .map_err(|_| Error::InvalidConfiguration)?;
        let bytes = spec(
            &self.target,
            &self.routes,
            &self.origin_groups,
            &self.payload_policy,
            &self.invalid_policy,
            self.max_replays,
        )?;
        let allowed: BTreeSet<_> = self.target.partitions.iter().collect();
        let mut used = BTreeSet::new();
        for route in &self.routes {
            if !allowed.contains(&route.destination) {
                return Err(Error::InvalidConfiguration);
            }
            self.output(&route.destination)?;
            used.insert(&route.destination.topic);
        }
        if let DltInvalidPolicy::Quarantine(tp) = &self.invalid_policy {
            // 隔离目标不能与业务回投分区重叠，错误记录不能混入普通业务输出。
            if !allowed.contains(tp) || self.routes.iter().any(|route| route.destination == *tp) {
                return Err(Error::InvalidConfiguration);
            }
            self.output(tp)?;
            used.insert(&tp.topic);
        }
        if used != self.target.topic_generations.keys().collect() {
            return Err(Error::InvalidConfiguration);
        }
        if self.replay.pipeline_fingerprint != format!("{:x}", Sha256::digest(bytes)) {
            return Err(Error::ContractMismatch);
        }
        Ok(())
    }

    /// 业务作用：保存范围与全部处理规则，恢复不重算移动边界或重新解释消息头路由。
    /// 参数说明：无。返回：NSKDPL01 规范正文与 SHA-256，最大16MiB；摘要不认证来源。
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let ranges = self
            .replay
            .encode()
            .map_err(|_| Error::InvalidConfiguration)?;
        let mut bytes = b"NSKDPL01".to_vec();
        bytes.extend_from_slice(&(ranges.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&ranges);
        bytes.extend_from_slice(&spec(
            &self.target,
            &self.routes,
            &self.origin_groups,
            &self.payload_policy,
            &self.invalid_policy,
            self.max_replays,
        )?);
        if bytes.len() + 32 > Self::MAX_ENCODED_BYTES {
            return Err(Error::InvalidConfiguration);
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        Ok(bytes)
    }

    /// 业务作用：验证持久计划的完整性与唯一表达，在非法或截断输入上拒绝执行。
    /// 参数说明：bytes 为完整计划。返回：规范合法的只读合同，否则统一为 CorruptPlan。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if !(44..=Self::MAX_ENCODED_BYTES).contains(&bytes.len()) {
            return Err(Error::CorruptPlan);
        }
        let payload = &bytes[..bytes.len() - 32];
        if Sha256::digest(payload)[..] != bytes[bytes.len() - 32..] {
            return Err(Error::CorruptPlan);
        }
        parse(payload)
            .and_then(|plan| {
                if plan.encode()? != bytes {
                    Err(Error::CorruptPlan)
                } else {
                    Ok(plan)
                }
            })
            .map_err(|_| Error::CorruptPlan)
    }

    /// 业务作用：禁止向正在读取的 DLT topic 直接写回，避免固定范围任务自产死信输入。
    /// 参数说明：tp 为批准输出坐标。返回：不同于当前同集群输入 topic 才通过。
    fn output(&self, tp: &Tp) -> Result<()> {
        if self.replay.cluster_id == self.target.cluster_id
            && self.replay.topic_generations.contains_key(&tp.topic)
        {
            Err(Error::InvalidConfiguration)
        } else {
            Ok(())
        }
    }
}

/// 业务作用：以唯一大端正文绑定全部输出规则，复用转发的目标和 schema 格式。
/// 参数说明：target/routes/groups/payload_policy/invalid_policy/max_replays 为固定处理语义。
/// 返回：NSKDSP01 规范字节；无界、重复或非规范字段拒绝。
fn spec(
    target: &TransferTarget,
    routes: &[TransferRoute],
    groups: &[String],
    payload_policy: &TransferPayloadPolicy,
    invalid_policy: &DltInvalidPolicy,
    max_replays: u8,
) -> Result<Vec<u8>> {
    if groups.len() > 256
        || groups.iter().any(|g| g.is_empty() || g.len() > 1024)
        || groups.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(Error::InvalidConfiguration);
    }
    let common = crate::transfer::spec(target, routes, payload_policy, max_replays)
        .map_err(|_| Error::InvalidConfiguration)?;
    let mut bytes = b"NSKDSP01".to_vec();
    bytes.extend_from_slice(&(common.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&common);
    bytes.extend_from_slice(&(groups.len() as u16).to_be_bytes());
    for group in groups {
        text(&mut bytes, group);
    }
    match invalid_policy {
        DltInvalidPolicy::Halt => bytes.push(0),
        DltInvalidPolicy::Quarantine(tp) => {
            crate::ReplayPosition {
                topic: tp.topic.clone(),
                partition: tp.partition,
                start_offset: 0,
                end_offset: 0,
                next_offset: 0,
            }
            .validate()
            .map_err(|_| Error::InvalidConfiguration)?;
            bytes.push(1);
            text(&mut bytes, &tp.topic);
            bytes.extend_from_slice(&tp.partition.to_be_bytes());
        }
    }
    Ok(bytes)
}

/// 业务作用：在集合分配前检查长度，拒绝计划中的尾部和类型歧义。
/// 参数说明：payload 为已通过摘要的正文。返回：完整字段组成的冻结计划，来源及处理身份仍须复验。
fn parse(payload: &[u8]) -> Result<DltReplayPlan> {
    let mut input = Input {
        bytes: payload,
        position: 0,
    };
    input.magic(b"NSKDPL01")?;
    let length = input.u32()? as usize;
    if !(40..=ReplayPlan::MAX_ENCODED_BYTES).contains(&length) {
        return Err(Error::CorruptPlan);
    }
    let replay = ReplayPlan::decode(input.take(length)?).map_err(|_| Error::CorruptPlan)?;
    input.magic(b"NSKDSP01")?;
    let length = input.u32()? as usize;
    if !(8..=TransferPlan::MAX_ENCODED_BYTES).contains(&length) {
        return Err(Error::CorruptPlan);
    }
    let target =
        crate::transfer::wire::decode_spec(input.take(length)?).map_err(|_| Error::CorruptPlan)?;
    let count = input.u16()? as usize;
    if count > 256 {
        return Err(Error::CorruptPlan);
    }
    let mut origin_groups = Vec::with_capacity(count);
    for _ in 0..count {
        origin_groups.push(input.text(1024)?);
    }
    let invalid_policy = match input.take(1)?[0] {
        0 => DltInvalidPolicy::Halt,
        1 => DltInvalidPolicy::Quarantine(Tp {
            topic: input.text(249)?,
            partition: input.u32()? as i32,
        }),
        _ => return Err(Error::CorruptPlan),
    };
    if input.position != payload.len() {
        return Err(Error::CorruptPlan);
    }
    let plan = DltReplayPlan {
        replay,
        target: target.target,
        routes: target.routes,
        origin_groups,
        payload_policy: target.payload_policy,
        invalid_policy,
        max_replays: target.max_hops,
    };
    plan.validate()?;
    Ok(plan)
}

/// 业务作用：用长度前缀表达已验证 UTF-8 文本，避免拼接歧义。
/// 参数说明：bytes 为输出缓冲，value 为文本。返回：追加大端 u16 长度及原文。
fn text(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

/// 输入只能在完整正文的有界切片内前进。
struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Input<'a> {
    /// 业务作用：拒绝溢出、截断和越界字段。参数说明：length 为所需字节数。返回：完整借用切片并移动游标。
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(Error::CorruptPlan)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(Error::CorruptPlan)?;
        self.position = end;
        Ok(bytes)
    }
    /// 业务作用：读取共同大端长度字段。参数说明：无。返回：完整 u16 值。
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| Error::CorruptPlan)?,
        ))
    }
    /// 业务作用：读取共同大端数量或坐标字段。参数说明：无。返回：完整 u32 位模式，业务范围由外层验证。
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::CorruptPlan)?,
        ))
    }
    /// 业务作用：在分配身份文本之前限制长度并采用严格 UTF-8。参数说明：max 为字节上限。返回：非空完整文本。
    fn text(&mut self, max: usize) -> Result<String> {
        let length = self.u16()? as usize;
        if length == 0 || length > max {
            return Err(Error::CorruptPlan);
        }
        std::str::from_utf8(self.take(length)?)
            .map(str::to_owned)
            .map_err(|_| Error::CorruptPlan)
    }
    /// 业务作用：拒绝其它协议被解释为 DLT 合同。参数说明：expected 为固定协议标识。返回：完全相同才继续。
    fn magic(&mut self, expected: &[u8]) -> Result<()> {
        if self.take(expected.len())? == expected {
            Ok(())
        } else {
            Err(Error::CorruptPlan)
        }
    }
}
