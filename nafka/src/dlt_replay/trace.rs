//! DLT 回投的共同来源证据；摘要检测内容变化，不认证写入者或授予目标权限。

use super::{DltEvidence, DltReplayError as Error, DltReplayPlan, DltReplayResult as Result};
use crate::{KafkaHeader, KafkaHeaders, ReplayRecord, Tp, TransferResource, TransferTrace};
use sha2::{Digest, Sha256};

/// 保留首次实际 DLT 坐标和原事件身份；再次回投改变计数和目标，不改变原位置。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DltReplayTrace {
    /// 首次实际读取的物理 DLT topic。
    pub origin: TransferResource,
    /// 首次 DLT 分区。
    pub partition: i32,
    /// 首次 DLT offset，不是后续回投位置。
    pub offset: i64,
    /// 首次 DLT Kafka 时间戳，不可用时为-1。
    pub timestamp: i64,
    /// 首次 DLT 记录报告的原业务来源，不包含其物理资源代次。
    pub original_evidence: DltEvidence,
    /// 先前的转发身份；回投不沿用其活动路径头。
    pub original_transfer: Option<TransferTrace>,
    /// 原转发ID或首次DLT物理坐标的共同摘要，不包含新目标offset。
    pub event_id: String,
    /// 已发生回投次数，范围1至16。
    pub replay_count: u8,
    /// 最近一次批准的目标物理资源。
    pub destination: TransferResource,
}

impl DltReplayTrace {
    /// 唯一规范字段名，重复及ASCII大小写别名均拒绝。
    pub const HEADER: &'static str = "x-nasa-dlt-replay";

    /// 业务作用：在使用可变公开来源字段前验证坐标、原证据和稳定ID。
    /// 参数说明：无。返回：全部字段合法且ID与原事件一致才通过，不认证消息来源。
    pub fn validate(&self) -> Result<()> {
        self.origin.validate().map_err(|_| Error::HeaderInvalid)?;
        self.destination
            .validate()
            .map_err(|_| Error::HeaderInvalid)?;
        self.original_evidence
            .validate()
            .map_err(|_| Error::HeaderInvalid)?;
        if self.partition < 0
            || self.offset < 0
            || self.timestamp < -1
            || !(1..=16).contains(&self.replay_count)
        {
            return Err(Error::HeaderInvalid);
        }
        let expected = if let Some(original) = &self.original_transfer {
            original.validate().map_err(|_| Error::HeaderInvalid)?;
            original.event_id.clone()
        } else {
            TransferTrace::identity(&self.origin, self.partition, self.offset)
        };
        if self.event_id != expected {
            return Err(Error::HeaderInvalid);
        }
        Ok(())
    }

    /// 业务作用：读取可用于重复识别的历史回投证据，不以其字段授权路由。
    /// 参数说明：headers为完整有序消息头。返回：普通输入为None；重复、别名、损坏或超额证据拒绝。
    pub fn read(headers: &KafkaHeaders) -> Result<Option<Self>> {
        check_headers(headers)?;
        let mut encoded = None;
        for header in headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(Self::HEADER))
        {
            if encoded.is_some() || header.name != Self::HEADER {
                return Err(Error::HeaderInvalid);
            }
            encoded = Some(header.value.as_deref().ok_or(Error::HeaderInvalid)?);
        }
        encoded.map(Self::decode).transpose()
    }

    /// 业务作用：保留原事件ID并限制再次失败的回投，不松动普通Transfer的回环保护。
    /// 参数说明：plan为批准合同，record为实际DLT输入，evidence为已验证当前来源，target为固定映射目标。
    /// 返回：保留业务头并写入唯一回投证据，旧活动转发头的身份被封装到历史字段。
    pub(super) fn forward(
        plan: &DltReplayPlan,
        record: &ReplayRecord,
        evidence: DltEvidence,
        target: &Tp,
    ) -> Result<KafkaHeaders> {
        check_headers(&record.headers)?;
        let previous = Self::read(&record.headers)?;
        let transferred = TransferTrace::read(&record.headers).map_err(|_| Error::HeaderInvalid)?;
        let destination = TransferResource {
            cluster_id: plan.target.cluster_id.clone(),
            topic: target.topic.clone(),
            generation: plan
                .target
                .topic_generations
                .get(&target.topic)
                .ok_or(Error::ContractMismatch)?
                .clone(),
        };
        let next = if let Some(mut previous) = previous {
            // 控制面批准回投原业务资源，但新任务名不能把已有回投计数清零。
            if previous.replay_count >= plan.max_replays {
                return Err(Error::ReplayLimit);
            }
            previous.replay_count += 1;
            previous.destination = destination;
            previous
        } else {
            let origin = TransferResource {
                cluster_id: record.cluster_id.clone(),
                topic: record.topic.clone(),
                generation: plan
                    .replay
                    .topic_generations
                    .get(&record.topic)
                    .ok_or(Error::ContractMismatch)?
                    .clone(),
            };
            let event_id = transferred
                .as_ref()
                .map(|trace| trace.event_id.clone())
                .unwrap_or_else(|| {
                    TransferTrace::identity(&origin, record.partition, record.offset)
                });
            Self {
                origin,
                partition: record.partition,
                offset: record.offset,
                timestamp: record.timestamp,
                original_evidence: evidence,
                original_transfer: transferred,
                event_id,
                replay_count: 1,
                destination,
            }
        };
        // 回投会离开旧Transfer路径，旧活动头不能继续宣称当前位置；原身份已包含在回投证据中。
        let mut headers: Vec<_> = record
            .headers
            .iter()
            .filter(|h| {
                !h.name.eq_ignore_ascii_case(Self::HEADER)
                    && !h.name.eq_ignore_ascii_case(TransferTrace::HEADER)
            })
            .cloned()
            .collect();
        headers.push(KafkaHeader {
            name: Self::HEADER.into(),
            value: Some(next.encode()?),
        });
        let headers = KafkaHeaders::from_vec(headers);
        check_headers(&headers)?;
        Ok(headers)
    }

    /// 业务作用：生成首次来源与回投次数的唯一共同字节，不编码当前目标offset。
    /// 参数说明：无。返回：无来源扩展时为NSKDRH01，有扩展时为NSKDRH02，正文附加完整性摘要，单头最大64KiB。
    fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = if self.original_evidence.origin.is_some() {
            b"NSKDRH02"
        } else {
            b"NSKDRH01"
        }
        .to_vec();
        resource(&mut bytes, &self.origin);
        bytes.extend(self.partition.to_be_bytes());
        bytes.extend(self.offset.to_be_bytes());
        bytes.extend(self.timestamp.to_be_bytes());
        text(&mut bytes, &self.original_evidence.origin_topic);
        bytes.extend(self.original_evidence.origin_partition.to_be_bytes());
        bytes.extend(self.original_evidence.origin_offset.to_be_bytes());
        text(&mut bytes, &self.original_evidence.origin_group);
        text(&mut bytes, &self.original_evidence.reason);
        bytes.extend(self.original_evidence.delivery_attempts.to_be_bytes());
        if let Some(origin) = &self.original_evidence.origin {
            text(&mut bytes, &origin.source_id);
            text(&mut bytes, &origin.cluster_id);
        }
        let transfer = self
            .original_transfer
            .as_ref()
            .map(TransferTrace::encode)
            .transpose()
            .map_err(|_| Error::HeaderInvalid)?
            .unwrap_or_default();
        bytes.extend((transfer.len() as u32).to_be_bytes());
        bytes.extend(transfer);
        for i in (0..64).step_by(2) {
            bytes.push(
                u8::from_str_radix(&self.event_id[i..i + 2], 16)
                    .map_err(|_| Error::HeaderInvalid)?,
            );
        }
        bytes.push(self.replay_count);
        resource(&mut bytes, &self.destination);
        let digest = Sha256::digest(&bytes);
        bytes.extend(digest);
        if bytes.len() > DltEvidence::MAX_HEADER_BYTES {
            return Err(Error::HeaderInvalid);
        }
        Ok(bytes)
    }

    /// 业务作用：在使用历史计数前验证完整摘要及唯一表达，不能接受尾部或截断证据。
    /// 参数说明：encoded为单个头值。返回：规范有界证据，全部格式错误为HeaderInvalid。
    fn decode(encoded: &[u8]) -> Result<Self> {
        if !(80..=DltEvidence::MAX_HEADER_BYTES).contains(&encoded.len()) {
            return Err(Error::HeaderInvalid);
        }
        let body = &encoded[..encoded.len() - 32];
        if Sha256::digest(body)[..] != encoded[encoded.len() - 32..] {
            return Err(Error::HeaderInvalid);
        }
        let mut input = Input(body);
        let magic = input.take(8)?;
        let extended = magic == b"NSKDRH02";
        if !extended && magic != b"NSKDRH01" {
            return Err(Error::HeaderInvalid);
        }
        let origin = input.resource()?;
        let partition = input.u32()? as i32;
        let offset = input.i64()?;
        let timestamp = input.i64()?;
        let original_evidence = DltEvidence {
            origin_topic: input.text(249, false)?,
            origin_partition: input.u32()? as i32,
            origin_offset: input.i64()?,
            origin_group: input.text(1024, false)?,
            reason: input.text(1024, true)?,
            delivery_attempts: input.u32()?,
            origin: if extended {
                Some(super::DltOrigin {
                    source_id: input.text(1024, false)?,
                    cluster_id: input.text(1024, false)?,
                })
            } else {
                None
            },
        };
        let length = input.u32()? as usize;
        if length > DltEvidence::MAX_HEADER_BYTES {
            return Err(Error::HeaderInvalid);
        }
        let original_transfer = if length == 0 {
            None
        } else {
            Some(TransferTrace::decode(input.take(length)?).map_err(|_| Error::HeaderInvalid)?)
        };
        let event_id = input.take(32)?.iter().map(|b| format!("{b:02x}")).collect();
        let replay_count = input.take(1)?[0];
        let destination = input.resource()?;
        let trace = Self {
            origin,
            partition,
            offset,
            timestamp,
            original_evidence,
            original_transfer,
            event_id,
            replay_count,
            destination,
        };
        if !input.0.is_empty() || trace.encode()? != encoded {
            return Err(Error::HeaderInvalid);
        }
        Ok(trace)
    }
}

/// 业务作用：约束证据与业务头的总规模，防止回投元数据造成无限增长。
/// 参数说明：headers为完整集合。返回：最多256项、合计64KiB才通过。
pub(super) fn check_headers(headers: &KafkaHeaders) -> Result<()> {
    let size = headers.iter().try_fold(0usize, |n, h| {
        n.checked_add(h.name.len())?
            .checked_add(h.value.as_ref().map_or(0, Vec::len))
    });
    if headers.len() > DltEvidence::MAX_HEADERS
        || size.is_none_or(|n| n > DltEvidence::MAX_HEADER_BYTES)
    {
        return Err(Error::HeaderInvalid);
    }
    Ok(())
}
/// 业务作用：编码有界来源文本，保留Unicode证据。参数说明：out为正文，value为已验证文本。返回：u16大端长度及UTF-8字节。
fn text(out: &mut Vec<u8>, value: &str) {
    out.extend((value.len() as u16).to_be_bytes());
    out.extend(value.as_bytes());
}
/// 业务作用：编码物理资源而非逻辑别名。参数说明：out为正文，resource为合法身份。返回：三个长度前缀字段。
pub(super) fn resource(out: &mut Vec<u8>, resource: &TransferResource) {
    text(out, &resource.cluster_id);
    text(out, &resource.topic);
    text(out, &resource.generation);
}

/// 游标只在完整的有界正文内前进。
pub(super) struct Input<'a>(pub(super) &'a [u8]);
impl<'a> Input<'a> {
    /// 业务作用：拒绝越界与截断字段。参数说明：length为字节数。返回：完整切片并移动游标。
    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        if length > self.0.len() {
            return Err(Error::HeaderInvalid);
        }
        let (value, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(value)
    }
    /// 业务作用：读取共同大端坐标或次数位模式。参数说明：无。返回：完整u32，具体业务范围由外层验证。
    pub(super) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::HeaderInvalid)?,
        ))
    }
    /// 业务作用：读取有符号offset或时间戳。参数说明：无。返回：完整i64，负值语义由对应字段复验。
    pub(super) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::HeaderInvalid)?,
        ))
    }
    /// 业务作用：在分配文本前约束长度和UTF-8合法性。参数说明：max为字节上限，empty决定是否允许空原因。返回：完整唯一文本。
    pub(super) fn text(&mut self, max: usize, empty: bool) -> Result<String> {
        let length = u16::from_be_bytes(self.take(2)?.try_into().map_err(|_| Error::HeaderInvalid)?)
            as usize;
        if length > max || !empty && length == 0 {
            return Err(Error::HeaderInvalid);
        }
        std::str::from_utf8(self.take(length)?)
            .map(str::to_owned)
            .map_err(|_| Error::HeaderInvalid)
    }
    /// 业务作用：恢复并复验物理资源身份，防止文本替换掩盖不同资源。参数说明：无。返回：有界ASCII资源。
    pub(super) fn resource(&mut self) -> Result<TransferResource> {
        let resource = TransferResource {
            cluster_id: self.text(1024, false)?,
            topic: self.text(249, false)?,
            generation: self.text(1024, false)?,
        };
        resource.validate().map_err(|_| Error::HeaderInvalid)?;
        Ok(resource)
    }
}
