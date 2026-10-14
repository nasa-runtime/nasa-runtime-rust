//! 确定性 checkpoint 字节序与完整性校验。

use super::{CheckpointError, CheckpointResult, ReplayCheckpoint, ReplayPosition};
use sha2::{Digest, Sha256};
use std::io::{Cursor, Read};

pub(super) const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"NSKCP001";

/// 业务作用：生成与 Java 共用的完整 checkpoint 文件，写入前复验全部输入。
/// 参数说明：checkpoint 为待持久化快照。
/// 返回：大端正文及 SHA-256，超限或非法内容拒绝。
pub(super) fn encode(checkpoint: &ReplayCheckpoint) -> CheckpointResult<Vec<u8>> {
    checkpoint.validate()?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    text(&mut bytes, &checkpoint.task_id);
    for pair in checkpoint.contract_fingerprint.as_bytes().chunks_exact(2) {
        bytes.push(
            u8::from_str_radix(
                std::str::from_utf8(pair).map_err(|_| CheckpointError::InvalidConfiguration)?,
                16,
            )
            .map_err(|_| CheckpointError::InvalidConfiguration)?,
        );
    }
    bytes.extend_from_slice(&checkpoint.generation.to_be_bytes());
    bytes.extend_from_slice(&checkpoint.revision.to_be_bytes());
    bytes.extend_from_slice(&(checkpoint.positions.len() as u32).to_be_bytes());
    for position in &checkpoint.positions {
        text(&mut bytes, &position.topic);
        bytes.extend_from_slice(&position.partition.to_be_bytes());
        bytes.extend_from_slice(&position.start_offset.to_be_bytes());
        bytes.extend_from_slice(&position.end_offset.to_be_bytes());
        bytes.extend_from_slice(&position.next_offset.to_be_bytes());
    }
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum);
    if bytes.len() > MAX_BYTES {
        return Err(CheckpointError::Capacity);
    }
    Ok(bytes)
}

/// 业务作用：拒绝无法作为恢复依据的损坏、截断、错序及越界文件。
/// 参数说明：bytes 为完整有界文件内容。
/// 返回：严格验证后的快照，不接受额外尾随字节。
pub(super) fn decode(bytes: &[u8]) -> CheckpointResult<ReplayCheckpoint> {
    if bytes.len() > MAX_BYTES || bytes.len() < 8 + 2 + 1 + 32 + 16 + 4 + 32 {
        return Err(CheckpointError::Corrupt);
    }
    let payload = &bytes[..bytes.len() - 32];
    if Sha256::digest(payload).as_slice() != &bytes[payload.len()..] {
        return Err(CheckpointError::Corrupt);
    }
    let mut input = Cursor::new(payload);
    if &read::<8>(&mut input)? != MAGIC {
        return Err(CheckpointError::Corrupt);
    }
    let task_id = read_text(&mut input, 128)?;
    let contract_fingerprint = hex(&read::<32>(&mut input)?);
    let generation = u64::from_be_bytes(read(&mut input)?);
    let revision = u64::from_be_bytes(read(&mut input)?);
    let count = u32::from_be_bytes(read(&mut input)?) as usize;
    if count == 0 || count > ReplayCheckpoint::MAX_PARTITIONS {
        return Err(CheckpointError::Corrupt);
    }
    let mut positions = Vec::with_capacity(count);
    for _ in 0..count {
        positions.push(ReplayPosition {
            topic: read_text(&mut input, 249)?,
            partition: i32::from_be_bytes(read(&mut input)?),
            start_offset: i64::from_be_bytes(read(&mut input)?),
            end_offset: i64::from_be_bytes(read(&mut input)?),
            next_offset: i64::from_be_bytes(read(&mut input)?),
        });
    }
    let checkpoint = ReplayCheckpoint {
        task_id,
        contract_fingerprint,
        generation,
        revision,
        positions,
    };
    checkpoint
        .validate()
        .map_err(|_| CheckpointError::Corrupt)?;
    if input.position() != payload.len() as u64 {
        return Err(CheckpointError::Corrupt);
    }
    Ok(checkpoint)
}

/// 业务作用：以无路径含义的摘要命名稳定任务文件。
/// 参数说明：task_id 为任务 ID。
/// 返回：通过身份校验后的 SHA-256 小写十六进制。
pub(super) fn key(task_id: &str) -> CheckpointResult<String> {
    super::validate_task(task_id)?;
    Ok(hex(&Sha256::digest(task_id.as_bytes())))
}

/// 业务作用：以固定字母表表达协议摘要，避免平台格式差异。
/// 参数说明：bytes 为摘要。
/// 返回：每字节两位的小写十六进制。
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// 业务作用：写入已经校验的有界 ASCII 身份。
/// 参数说明：output 为正文缓冲，value 为任务名或 topic。
/// 返回：追加大端 u16 长度与原文字节。
fn text(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

/// 业务作用：在分配字符串前检查长度并拒绝截断或非 ASCII 身份。
/// 参数说明：input 为有界正文，limit 为对应身份上限。
/// 返回：完整文本，否则为损坏分类。
fn read_text(input: &mut Cursor<&[u8]>, limit: usize) -> CheckpointResult<String> {
    let length = u16::from_be_bytes(read(input)?) as usize;
    if length == 0 || length > limit {
        return Err(CheckpointError::Corrupt);
    }
    let mut bytes = vec![0; length];
    input
        .read_exact(&mut bytes)
        .map_err(|_| CheckpointError::Corrupt)?;
    if !bytes.is_ascii() {
        return Err(CheckpointError::Corrupt);
    }
    String::from_utf8(bytes).map_err(|_| CheckpointError::Corrupt)
}

/// 业务作用：完整读取固定宽度协议字段，短读不能补零成为合法坐标。
/// 参数说明：input 为正文游标，N 为协议字段宽度。
/// 返回：完整字段字节，截断则拒绝。
fn read<const N: usize>(input: &mut Cursor<&[u8]>) -> CheckpointResult<[u8; N]> {
    let mut bytes = [0; N];
    input
        .read_exact(&mut bytes)
        .map_err(|_| CheckpointError::Corrupt)?;
    Ok(bytes)
}
