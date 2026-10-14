//! 冻结计划的有界共同编码读取。

use super::{ReplayError, ReplayLimits, ReplayPlan, ReplayResult};
use crate::{ReplayCheckpoint, ReplayPosition};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Cursor, Read};

/// 业务作用：只接受一种规范计划表达，避免损坏或重排文件成为不同恢复依据。
/// 参数说明：bytes 为完整字节，最大 8 MiB。
/// 返回：合法冻结计划或 CorruptPlan，不执行 broker 或存储副作用。
pub(super) fn decode(bytes: &[u8]) -> ReplayResult<ReplayPlan> {
    if bytes.len() < 40 || bytes.len() > ReplayPlan::MAX_ENCODED_BYTES {
        return Err(ReplayError::CorruptPlan);
    }
    let payload = &bytes[..bytes.len() - 32];
    if Sha256::digest(payload).as_slice() != &bytes[payload.len()..] {
        return Err(ReplayError::CorruptPlan);
    }
    let mut input = Cursor::new(payload);
    if &read::<8>(&mut input)? != b"NSKRPL01" {
        return Err(ReplayError::CorruptPlan);
    }
    let task_id = text(&mut input, 128)?;
    let source_id = text(&mut input, 128)?;
    let cluster_id = text(&mut input, 1024)?;
    let pipeline_fingerprint = text(&mut input, 64)?;
    let topic_count = count(&mut input)?;
    let mut topic_generations = BTreeMap::new();
    for _ in 0..topic_count {
        if topic_generations
            .insert(text(&mut input, 249)?, text(&mut input, 1024)?)
            .is_some()
        {
            return Err(ReplayError::CorruptPlan);
        }
    }
    let limits = ReplayLimits {
        records_per_second: u64::from_be_bytes(read(&mut input)?),
        bytes_per_second: u64::from_be_bytes(read(&mut input)?),
        max_in_flight_records: u32::from_be_bytes(read(&mut input)?),
        max_in_flight_bytes: u64::from_be_bytes(read(&mut input)?),
        concurrency: u32::from_be_bytes(read(&mut input)?),
    };
    let range_count = count(&mut input)?;
    let mut ranges = Vec::with_capacity(range_count);
    for _ in 0..range_count {
        let topic = text(&mut input, 249)?;
        let partition = i32::from_be_bytes(read(&mut input)?);
        let start_offset = i64::from_be_bytes(read(&mut input)?);
        let end_offset = i64::from_be_bytes(read(&mut input)?);
        ranges.push(ReplayPosition {
            topic,
            partition,
            start_offset,
            end_offset,
            next_offset: start_offset,
        });
    }
    let plan = ReplayPlan {
        task_id,
        source_id,
        cluster_id,
        topic_generations,
        pipeline_fingerprint,
        limits,
        ranges,
    };
    // 可解析并不代表规范；重新编码比较同时拒绝乱序 topic 和额外尾随内容。
    if input.position() != payload.len() as u64
        || plan.encode().map_err(|_| ReplayError::CorruptPlan)? != bytes
    {
        return Err(ReplayError::CorruptPlan);
    }
    Ok(plan)
}

/// 业务作用：在分配集合前限制声明数量，防止归档引入无界资源占用。
/// 参数说明：input 为有界正文读取器。
/// 返回：1 到 4096 的合法数量，否则拒绝。
fn count(input: &mut Cursor<&[u8]>) -> ReplayResult<usize> {
    let count = u32::from_be_bytes(read(input)?) as usize;
    if count == 0 || count > ReplayCheckpoint::MAX_PARTITIONS {
        Err(ReplayError::CorruptPlan)
    } else {
        Ok(count)
    }
}

/// 业务作用：读取完整有界身份，阻止异常编码或短读进入恢复合同。
/// 参数说明：input 为正文读取器，limit 为该字段最大字节数。
/// 返回：非空可见 ASCII 文本，否则拒绝。
fn text(input: &mut Cursor<&[u8]>, limit: usize) -> ReplayResult<String> {
    let length = u16::from_be_bytes(read(input)?) as usize;
    if length == 0 || length > limit {
        return Err(ReplayError::CorruptPlan);
    }
    let mut bytes = vec![0; length];
    input
        .read_exact(&mut bytes)
        .map_err(|_| ReplayError::CorruptPlan)?;
    if !bytes.iter().all(|byte| (33..=126).contains(byte)) {
        return Err(ReplayError::CorruptPlan);
    }
    String::from_utf8(bytes).map_err(|_| ReplayError::CorruptPlan)
}

/// 业务作用：固定宽度字段必须完整存在，截断不能补零成为合法范围。
/// 参数说明：input 为正文读取器，N 为字段宽度。
/// 返回：完整字节或格式拒绝。
fn read<const N: usize>(input: &mut Cursor<&[u8]>) -> ReplayResult<[u8; N]> {
    let mut bytes = [0; N];
    input
        .read_exact(&mut bytes)
        .map_err(|_| ReplayError::CorruptPlan)?;
    Ok(bytes)
}
