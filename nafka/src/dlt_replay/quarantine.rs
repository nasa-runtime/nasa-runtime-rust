//! 原文隔离证据；原始错误头不得被覆盖或静默丢弃。

use super::trace::{check_headers, resource, Input};
use super::{DltEvidence, DltReplayError as Error, DltReplayPlan, DltReplayResult as Result};
use crate::{KafkaHeader, KafkaHeaders, ReplayRecord, TransferResource, TransferTrace};
use sha2::{Digest, Sha256};

/// 来源采用实际DLT坐标，不信任无效消息自报身份。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DltQuarantineTrace {
    /// 实际读取的物理DLT资源。
    pub origin: TransferResource,
    /// 实际分区。
    pub partition: i32,
    /// 实际offset。
    pub offset: i64,
    /// 原时间戳，不可用时为-1。
    pub timestamp: i64,
    /// 确定的输入拒绝类别，不能为交付未知或基础设施错误。
    pub reason: Error,
    /// 实际DLT位置的稳定摘要。
    pub event_id: String,
}
impl DltQuarantineTrace {
    /// 唯一规范字段名，保留原始无效证据时追加一次。
    pub const HEADER: &'static str = "x-nasa-dlt-replay-failure";
    /// 业务作用：复验隔离来源与确定拒绝，不以错误记录自报位置替代实际来源。
    /// 参数说明：无。返回：合法坐标、原因与事件ID才通过，不认证写入者。
    pub fn validate(&self) -> Result<()> {
        self.origin.validate().map_err(|_| Error::HeaderInvalid)?;
        if self.partition < 0
            || self.offset < 0
            || self.timestamp < -1
            || !Self::eligible(self.reason)
            || self.event_id != TransferTrace::identity(&self.origin, self.partition, self.offset)
        {
            return Err(Error::HeaderInvalid);
        }
        Ok(())
    }
    /// 业务作用：读取隔离处置所需的完整来源事实，不覆盖原始错误字段。
    /// 参数说明：headers为完整有序头。返回：无证据时None，重复、别名或损坏拒绝。
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
    /// 业务作用：限定原文隔离可以承接的确定输入拒绝。
    /// 参数说明：reason为失败类别。返回：网络、schema或交付未知不能作为隔离成功。
    pub(super) fn eligible(reason: Error) -> bool {
        matches!(
            reason,
            Error::MissingEvidence | Error::HeaderInvalid | Error::ReplayLimit
        )
    }
    /// 业务作用：完整保留原始输入并追加唯一隔离事实。
    /// 参数说明：plan为冻结合同，record为实际输入，reason为确定拒绝。返回：有界新头；已有标记或容量不足则停止。
    pub(super) fn append(
        plan: &DltReplayPlan,
        record: &ReplayRecord,
        reason: Error,
    ) -> Result<KafkaHeaders> {
        check_headers(&record.headers)?;
        // 覆盖先前隔离证据会掩盖失败并允许无限回流，必须显式停止。
        if record
            .headers
            .iter()
            .any(|h| h.name.eq_ignore_ascii_case(Self::HEADER))
        {
            return Err(Error::HeaderInvalid);
        }
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
        let event_id = TransferTrace::identity(&origin, record.partition, record.offset);
        let trace = Self {
            origin,
            partition: record.partition,
            offset: record.offset,
            timestamp: record.timestamp,
            reason,
            event_id,
        };
        let mut headers: Vec<_> = record.headers.iter().cloned().collect();
        headers.push(KafkaHeader {
            name: Self::HEADER.into(),
            value: Some(trace.encode()?),
        });
        let headers = KafkaHeaders::from_vec(headers);
        check_headers(&headers)?;
        Ok(headers)
    }
    /// 业务作用：生成共同隔离字节，不编码新目标offset。
    /// 参数说明：无。返回：NSKDQH01正文及完整性摘要，不提供认证。
    fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = b"NSKDQH01".to_vec();
        resource(&mut bytes, &self.origin);
        bytes.extend(self.partition.to_be_bytes());
        bytes.extend(self.offset.to_be_bytes());
        bytes.extend(self.timestamp.to_be_bytes());
        bytes.push(match self.reason {
            Error::MissingEvidence => 0,
            Error::HeaderInvalid => 1,
            Error::ReplayLimit => 2,
            _ => return Err(Error::HeaderInvalid),
        });
        for i in (0..64).step_by(2) {
            bytes.push(
                u8::from_str_radix(&self.event_id[i..i + 2], 16)
                    .map_err(|_| Error::HeaderInvalid)?,
            );
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend(digest);
        Ok(bytes)
    }
    /// 业务作用：验证完整隔离证据，不接受尾部、截断或非规范内容。
    /// 参数说明：encoded为头值。返回：合法实际坐标及确定拒绝原因。
    fn decode(encoded: &[u8]) -> Result<Self> {
        if !(80..=DltEvidence::MAX_HEADER_BYTES).contains(&encoded.len()) {
            return Err(Error::HeaderInvalid);
        }
        let body = &encoded[..encoded.len() - 32];
        if Sha256::digest(body)[..] != encoded[encoded.len() - 32..] {
            return Err(Error::HeaderInvalid);
        }
        let mut input = Input(body);
        if input.take(8)? != b"NSKDQH01" {
            return Err(Error::HeaderInvalid);
        }
        let origin = input.resource()?;
        let partition = input.u32()? as i32;
        let offset = input.i64()?;
        let timestamp = input.i64()?;
        let reason = match input.take(1)?[0] {
            0 => Error::MissingEvidence,
            1 => Error::HeaderInvalid,
            2 => Error::ReplayLimit,
            _ => return Err(Error::HeaderInvalid),
        };
        let event_id = input.take(32)?.iter().map(|b| format!("{b:02x}")).collect();
        let trace = Self {
            origin,
            partition,
            offset,
            timestamp,
            reason,
            event_id,
        };
        if !input.0.is_empty() || trace.encode()? != encoded {
            return Err(Error::HeaderInvalid);
        }
        Ok(trace)
    }
}
