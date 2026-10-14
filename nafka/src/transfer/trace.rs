//! 转发坐标与路径的有界共同编码；摘要检测完整性，不承担身份认证。

use super::{TransferError, TransferPlan, TransferResult, token, valid_partition};
use crate::{KafkaHeader, KafkaHeaders, ReplayRecord, Tp};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// 能区分删除重建的物理 topic 资源。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransferResource {
    /// broker 物理集群 ID。
    pub cluster_id: String,
    /// 完整 topic 名。
    pub topic: String,
    /// 外部目录提供的资源代次。
    pub generation: String,
}
impl TransferResource {
    /// 业务作用：拒绝未完成物理身份绑定或超额的路径节点。
    /// 参数说明：无。返回：有界 ASCII 身份和合法 topic 才通过。
    pub(crate) fn validate(&self) -> TransferResult<()> {
        token(&self.cluster_id, 1024)
            .and_then(|()| token(&self.generation, 1024))
            .and_then(|()| {
                valid_partition(&Tp {
                    topic: self.topic.clone(),
                    partition: 0,
                })
            })
            .map_err(|_| TransferError::HeaderInvalid)
    }
}

/// 稳定原始事件及有序路径；可信性依赖消息写权限和当前资源目录。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTrace {
    /// 首次转发前的原始资源。
    pub origin: TransferResource,
    /// 原始 Kafka 分区。
    pub partition: i32,
    /// 原始 Kafka offset。
    pub offset: i64,
    /// 原始毫秒时间，缺失为 -1。
    pub timestamp: i64,
    /// 原始资源与坐标的 SHA-256 小写十六进制，不随任务名或目标 offset 改变。
    pub event_id: String,
    /// 从原始资源到当前记录所在资源的完整路径，最多十七个节点。
    pub path: Vec<TransferResource>,
}
impl TransferTrace {
    /// 业务作用：为转发与DLT采用相同的实际Kafka事件身份，避免目的offset改变重复识别。
    /// 参数说明：origin为物理资源，partition/offset为原坐标。返回：规范坐标的SHA-256文本，不承担身份认证。
    pub(crate) fn identity(origin: &TransferResource, partition: i32, offset: i64) -> String {
        identity(origin, partition, offset)
    }
    /// 唯一合法名称；大小写变体和重复项均拒绝。
    pub const HEADER: &'static str = "x-nasa-transfer";
    /// 单个证据以及转发后全部消息头的共同字节上限。
    pub const MAX_HEADER_BYTES: usize = 65536;

    /// 业务作用：从目标原文解析来源幂等身份，不把消息头当作访问授权。
    /// 参数说明：headers 为包含重复项和 null 的完整有序消息头。
    /// 返回：普通记录为 None；非规范、重复或损坏证据拒绝。
    pub fn read(headers: &KafkaHeaders) -> TransferResult<Option<Self>> {
        let mut encoded = None;
        for header in headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(Self::HEADER))
        {
            if encoded.is_some() || header.name != Self::HEADER {
                return Err(TransferError::HeaderInvalid);
            }
            encoded = Some(
                header
                    .value
                    .as_deref()
                    .ok_or(TransferError::HeaderInvalid)?,
            );
        }
        encoded.map(Self::decode).transpose()
    }

    /// 业务作用：在任何目的副作用之前追加经过复验的目标节点，保留业务头的全部字节语义。
    /// 参数说明：plan 为冻结合同，record 为实际来源，destination 为批准目标分区。
    /// 返回：新头集合；回环、跳数、结构或容量违规时拒绝。
    pub(super) fn forward(
        plan: &TransferPlan,
        record: &ReplayRecord,
        destination: &Tp,
    ) -> TransferResult<KafkaHeaders> {
        check_headers(&record.headers)?;
        let current = TransferResource {
            cluster_id: record.cluster_id.clone(),
            topic: record.topic.clone(),
            generation: plan
                .replay
                .topic_generations
                .get(&record.topic)
                .ok_or(TransferError::HeaderInvalid)?
                .clone(),
        };
        let target = TransferResource {
            cluster_id: plan.target.cluster_id.clone(),
            topic: destination.topic.clone(),
            generation: plan
                .target
                .topic_generations
                .get(&destination.topic)
                .ok_or(TransferError::HeaderInvalid)?
                .clone(),
        };
        let mut trace = if let Some(previous) = Self::read(&record.headers)? {
            // 复制到其它 topic 的旧头不能冒充完整路径；最后节点必须是当前实际来源。
            if previous.path.last() != Some(&current) {
                return Err(TransferError::HeaderInvalid);
            }
            previous
        } else {
            Self {
                event_id: identity(&current, record.partition, record.offset),
                origin: current.clone(),
                partition: record.partition,
                offset: record.offset,
                timestamp: record.timestamp,
                path: vec![current],
            }
        };
        if trace.path.contains(&target) {
            return Err(TransferError::LoopDetected);
        }
        if trace.path.len() > usize::from(plan.max_hops) {
            return Err(TransferError::HopLimit);
        }
        trace.path.push(target);
        let mut outgoing: Vec<_> = record
            .headers
            .iter()
            .filter(|h| !h.name.eq_ignore_ascii_case(Self::HEADER))
            .cloned()
            .collect();
        outgoing.push(KafkaHeader {
            name: Self::HEADER.into(),
            value: Some(trace.encode()?),
        });
        let headers = KafkaHeaders::from_vec(outgoing);
        check_headers(&headers)?;
        Ok(headers)
    }

    /// 业务作用：复验公开结构的来源坐标和路径唯一性，防止通过可变字段绕过编码边界。
    /// 参数说明：无。返回：坐标、路径、节点和事件摘要全部合法才通过。
    pub(crate) fn validate(&self) -> TransferResult<()> {
        self.origin.validate()?;
        if self.partition < 0
            || self.offset < 0
            || self.timestamp < -1
            || !(2..=17).contains(&self.path.len())
            || self.path.first() != Some(&self.origin)
            || self.path.iter().collect::<BTreeSet<_>>().len() != self.path.len()
            || self.event_id != identity(&self.origin, self.partition, self.offset)
        {
            return Err(TransferError::HeaderInvalid);
        }
        self.path.iter().try_for_each(TransferResource::validate)
    }

    /// 业务作用：输出来源证据唯一共同表达，附加检测内容变化的摘要。
    /// 参数说明：无。返回：有界规范字节；不能作为认证或授权证明。
    pub(crate) fn encode(&self) -> TransferResult<Vec<u8>> {
        self.validate()?;
        let mut bytes = b"NSKTRH01".to_vec();
        resource(&mut bytes, &self.origin);
        bytes.extend(self.partition.to_be_bytes());
        bytes.extend(self.offset.to_be_bytes());
        bytes.extend(self.timestamp.to_be_bytes());
        bytes.extend(identity_bytes(&self.origin, self.partition, self.offset));
        bytes.push(self.path.len() as u8);
        for node in &self.path {
            resource(&mut bytes, node);
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend(digest);
        if bytes.len() > Self::MAX_HEADER_BYTES {
            return Err(TransferError::HeaderInvalid);
        }
        Ok(bytes)
    }

    /// 业务作用：限制长度并拒绝截断、损坏、歧义及非规范证据。
    /// 参数说明：encoded 为单个原始头字节。返回：完整合法结构，不执行网络访问。
    pub(crate) fn decode(encoded: &[u8]) -> TransferResult<Self> {
        if !(82..=Self::MAX_HEADER_BYTES).contains(&encoded.len()) {
            return Err(TransferError::HeaderInvalid);
        }
        let (body, hash) = encoded.split_at(encoded.len() - 32);
        if Sha256::digest(body).as_slice() != hash {
            return Err(TransferError::HeaderInvalid);
        }
        let mut reader = Reader(body);
        if reader.take(8)? != b"NSKTRH01" {
            return Err(TransferError::HeaderInvalid);
        }
        let origin = reader.resource()?;
        let partition = i32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| TransferError::HeaderInvalid)?,
        );
        let offset = i64::from_be_bytes(
            reader
                .take(8)?
                .try_into()
                .map_err(|_| TransferError::HeaderInvalid)?,
        );
        let timestamp = i64::from_be_bytes(
            reader
                .take(8)?
                .try_into()
                .map_err(|_| TransferError::HeaderInvalid)?,
        );
        let event_id = hex(reader.take(32)?);
        let count = usize::from(reader.take(1)?[0]);
        if !(2..=17).contains(&count) {
            return Err(TransferError::HeaderInvalid);
        }
        let mut path = Vec::with_capacity(count);
        for _ in 0..count {
            path.push(reader.resource()?);
        }
        let trace = Self {
            origin,
            partition,
            offset,
            timestamp,
            event_id,
            path,
        };
        if !reader.0.is_empty() || trace.encode()? != encoded {
            return Err(TransferError::HeaderInvalid);
        }
        Ok(trace)
    }
}

/// 业务作用：限制包含重复项的总头容量，防止来源路径把记录膨胀为无界分配。
/// 参数说明：headers 为完整集合。返回：最多256项且名称与值合计不超过共同上限。
fn check_headers(headers: &KafkaHeaders) -> TransferResult<()> {
    let bytes = headers.iter().try_fold(0usize, |size, h| {
        size.checked_add(h.name.len())?
            .checked_add(h.value.as_ref().map_or(0, Vec::len))
    });
    if headers.len() > 256 || bytes.is_none_or(|n| n > TransferTrace::MAX_HEADER_BYTES) {
        return Err(TransferError::HeaderInvalid);
    }
    Ok(())
}
/// 业务作用：固定来源事件身份的原始摘要，不混入任务名或目标坐标。
/// 参数说明：origin 为实际资源，partition/offset 为原始坐标。返回：共同 SHA-256。
fn identity_bytes(origin: &TransferResource, partition: i32, offset: i64) -> [u8; 32] {
    let mut bytes = b"NSKTID01".to_vec();
    resource(&mut bytes, origin);
    bytes.extend(partition.to_be_bytes());
    bytes.extend(offset.to_be_bytes());
    Sha256::digest(bytes).into()
}
/// 业务作用：以公开共同十六进制表示来源事件身份。
/// 参数说明：origin 为资源，partition/offset 为原始坐标。返回：小写64字符摘要。
fn identity(origin: &TransferResource, partition: i32, offset: i64) -> String {
    hex(&identity_bytes(origin, partition, offset))
}
/// 业务作用：避免字节摘要在跨语言文本表示时出现大小写歧义。
/// 参数说明：bytes 为摘要。返回：小写十六进制。
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
/// 业务作用：以固定字段顺序编码合法物理资源。
/// 参数说明：out 为正文，node 为已校验节点。返回：写入三段长度前缀ASCII身份。
fn resource(out: &mut Vec<u8>, node: &TransferResource) {
    for value in [&node.cluster_id, &node.topic, &node.generation] {
        out.extend((value.len() as u16).to_be_bytes());
        out.extend(value.as_bytes());
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    /// 业务作用：解析前核对剩余边界，禁止截断输入导致越界。
    /// 参数说明：count 为所需字节。返回：借用片段并推进游标，不足拒绝。
    fn take(&mut self, count: usize) -> TransferResult<&'a [u8]> {
        if count > self.0.len() {
            return Err(TransferError::HeaderInvalid);
        }
        let (value, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(value)
    }
    /// 业务作用：在分配身份字符串前核对长度和共同字符集。
    /// 参数说明：max 为身份长度限制。返回：合法可见ASCII文本。
    fn text(&mut self, max: usize) -> TransferResult<String> {
        let size = usize::from(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| TransferError::HeaderInvalid)?,
        ));
        if size == 0 || size > max {
            return Err(TransferError::HeaderInvalid);
        }
        let bytes = self.take(size)?;
        if bytes.iter().any(|b| !(33..=126).contains(b)) {
            return Err(TransferError::HeaderInvalid);
        }
        String::from_utf8(bytes.to_vec()).map_err(|_| TransferError::HeaderInvalid)
    }
    /// 业务作用：重建并校验路径节点的三段物理身份。
    /// 参数说明：无。返回：合法资源，错误不接受部分身份。
    fn resource(&mut self) -> TransferResult<TransferResource> {
        let node = TransferResource {
            cluster_id: self.text(1024)?,
            topic: self.text(249)?,
            generation: self.text(1024)?,
        };
        node.validate()?;
        Ok(node)
    }
}
