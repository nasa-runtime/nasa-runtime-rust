//! 转发持久合同的有界解析，只有唯一规范编码可以取得计划身份。
use super::*;

/// 业务作用：验证完整摘要并拒绝可解析但非规范的转发计划。
/// 参数说明：bytes为完整归档，最大16MiB。返回：合法计划；全部格式和合同错误统一为CorruptPlan。
pub(super) fn decode(bytes: &[u8]) -> TransferResult<TransferPlan> {
    if bytes.len() < 44 || bytes.len() > TransferPlan::MAX_ENCODED_BYTES {
        return Err(TransferError::CorruptPlan);
    }
    let payload = &bytes[..bytes.len() - 32];
    if Sha256::digest(payload)[..] != bytes[bytes.len() - 32..] {
        return Err(TransferError::CorruptPlan);
    }
    parse(payload)
        .and_then(|plan| {
            if plan.encode()?.as_slice() != bytes {
                Err(TransferError::CorruptPlan)
            } else {
                Ok(plan)
            }
        })
        .map_err(|_| TransferError::CorruptPlan)
}
/// 业务作用：在分配集合和文本之前逐项校验声明边界。
/// 参数说明：payload为已验证完整性的正文。返回：完整计划；尾部或缺失字段均拒绝。
fn parse(payload: &[u8]) -> TransferResult<TransferPlan> {
    let mut input = Input {
        bytes: payload,
        position: 0,
    };
    input.magic(b"NSKTPL01")?;
    let length = input.u32()? as usize;
    if !(40..=ReplayPlan::MAX_ENCODED_BYTES).contains(&length) {
        return Err(TransferError::CorruptPlan);
    }
    let replay = ReplayPlan::decode(input.take(length)?).map_err(|_| TransferError::CorruptPlan)?;
    let spec = read_spec(&mut input)?;
    if input.position != payload.len() {
        return Err(TransferError::CorruptPlan);
    }
    let plan = TransferPlan {
        replay,
        target: spec.target,
        routes: spec.routes,
        payload_policy: spec.payload_policy,
        max_hops: spec.max_hops,
    };
    plan.validate()?;
    Ok(plan)
}

/// 具有不同来源选择规则的有限任务共用的目标处理合同。
pub(crate) struct Spec {
    pub(crate) target: TransferTarget,
    pub(crate) routes: Vec<TransferRoute>,
    pub(crate) payload_policy: TransferPayloadPolicy,
    pub(crate) max_hops: u8,
}

/// 业务作用：从独立有界正文读取共同目标合同，禁止追加歧义内容。
/// 参数说明：bytes 为完整目标规范正文。返回：字段合法且无尾部的合同，来源语义由外层计划绑定。
pub(crate) fn decode_spec(bytes: &[u8]) -> TransferResult<Spec> {
    let mut input = Input { bytes, position: 0 };
    let spec = read_spec(&mut input)?;
    if input.position != bytes.len() {
        return Err(TransferError::CorruptPlan);
    }
    Ok(spec)
}

/// 业务作用：共用目标身份、路由和 schema 的受限解析，不授权任意来源消息。
/// 参数说明：input 为已限长正文游标。返回：完整字段，规范排序及来源绑定仍由外层合同复验。
fn read_spec(input: &mut Input<'_>) -> TransferResult<Spec> {
    input.magic(b"NSKTSP01")?;
    let source_id = input.text(128, false)?;
    let cluster_id = input.text(1024, false)?;
    let lane = input.text(64, false)?;
    let max_hops = input.u8()?;
    let mut topic_generations = BTreeMap::new();
    for _ in 0..input.count(false)? {
        if topic_generations
            .insert(input.text(249, false)?, input.text(1024, false)?)
            .is_some()
        {
            return Err(TransferError::CorruptPlan);
        }
    }
    let mut partitions = Vec::new();
    for _ in 0..input.count(false)? {
        partitions.push(input.partition()?);
    }
    let mut routes = Vec::new();
    for _ in 0..input.count(false)? {
        routes.push(TransferRoute {
            source: input.partition()?,
            destination: input.partition()?,
        });
    }
    let kind = input.u8()?;
    let source_registry = input.text(128, true)?;
    let destination_registry = input.text(128, true)?;
    let mut schemas = Vec::new();
    for _ in 0..input.count(true)? {
        let source_id = input.u32()? as i32;
        let destination_id = input.u32()? as i32;
        let kind = match input.u8()? {
            1 => TransferSchemaKind::Avro,
            2 => TransferSchemaKind::Protobuf,
            3 => TransferSchemaKind::Json,
            _ => return Err(TransferError::CorruptPlan),
        };
        let schema_fingerprint = input
            .take(32)?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        schemas.push(TransferSchemaMapping {
            source_id,
            destination_id,
            kind,
            schema_fingerprint,
        });
    }
    let payload_policy = match kind {
        0 if source_registry.is_empty()
            && destination_registry.is_empty()
            && schemas.is_empty() =>
        {
            TransferPayloadPolicy::Opaque
        }
        1 => TransferPayloadPolicy::Confluent {
            source_registry,
            destination_registry,
            schemas,
        },
        _ => return Err(TransferError::CorruptPlan),
    };
    Ok(Spec {
        target: TransferTarget {
            source_id,
            cluster_id,
            lane,
            topic_generations,
            partitions,
        },
        routes,
        payload_policy,
        max_hops,
    })
}
struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Input<'a> {
    /// 业务作用：在切片之前拒绝超出正文的长度。参数说明：count为所需字节。返回：完整借用片段并移动游标。
    fn take(&mut self, count: usize) -> TransferResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or(TransferError::CorruptPlan)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(TransferError::CorruptPlan)?;
        self.position = end;
        Ok(bytes)
    }
    /// 业务作用：读取单字节协议判别值。参数说明：无。返回：完整字节或截断错误。
    fn u8(&mut self) -> TransferResult<u8> {
        Ok(self.take(1)?[0])
    }
    /// 业务作用：读取共同大端数量或坐标字段。参数说明：无。返回：完整四字节整数。
    fn u32(&mut self) -> TransferResult<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| TransferError::CorruptPlan)?,
        ))
    }
    /// 业务作用：在集合分配前约束条目数。参数说明：empty表示允许零项。返回：最多4096项。
    fn count(&mut self, empty: bool) -> TransferResult<usize> {
        let count = self.u32()? as usize;
        if count > 4096 || !empty && count == 0 {
            Err(TransferError::CorruptPlan)
        } else {
            Ok(count)
        }
    }
    /// 业务作用：拒绝非法身份或无界文本。参数说明：max为最大长度，empty为是否允许空值。返回：独立ASCII文本。
    fn text(&mut self, max: usize, empty: bool) -> TransferResult<String> {
        let count = u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| TransferError::CorruptPlan)?,
        ) as usize;
        if count > max || !empty && count == 0 {
            return Err(TransferError::CorruptPlan);
        }
        let bytes = self.take(count)?;
        if !bytes.iter().all(|byte| (33..=126).contains(byte)) {
            return Err(TransferError::CorruptPlan);
        }
        String::from_utf8(bytes.to_vec()).map_err(|_| TransferError::CorruptPlan)
    }
    /// 业务作用：读取并立即复验Kafka坐标。参数说明：无。返回：合法分区。
    fn partition(&mut self) -> TransferResult<Tp> {
        let tp = Tp {
            topic: self.text(249, false)?,
            partition: self.u32()? as i32,
        };
        valid_partition(&tp)?;
        Ok(tp)
    }
    /// 业务作用：拒绝把其它持久协议当作转发合同。参数说明：expected为协议标识。返回：完全一致才继续。
    fn magic(&mut self, expected: &[u8]) -> TransferResult<()> {
        if self.take(expected.len())? != expected {
            Err(TransferError::CorruptPlan)
        } else {
            Ok(())
        }
    }
}
