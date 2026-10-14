//! 消息自带的有界 DLT 来源描述；原坐标不包含原 topic 物理代次，也不是回投后的新 offset。

use super::{DltReplayError as Error, DltReplayResult as Result};
use crate::{wire::*, KafkaHeaders, ReplayPosition};
use std::collections::BTreeMap;

const FIELDS: [&str; 6] = [
    HEADER_DLT_ORIGIN_TOPIC,
    HEADER_DLT_ORIGIN_PARTITION,
    HEADER_DLT_ORIGIN_OFFSET,
    HEADER_DLT_ORIGIN_GROUP,
    HEADER_DLT_REASON,
    HEADER_DLT_DELIVERY_ATTEMPTS,
];

/// 消息报告的 DLT 原来源；格式验证不授予访问权，可信性依赖部署 ACL 和真实读取计划。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DltEvidence {
    /// 原失败输入的 topic 名，不代表原资源物理代次。
    pub origin_topic: String,
    /// 原失败输入的非负分区。
    pub origin_partition: i32,
    /// 原失败输入的 offset，回投不会恢复这个日志位置。
    pub origin_offset: i64,
    /// 原消费组，最多1024个 UTF-8 字节。
    pub origin_group: String,
    /// 最多1024个 UTF-8 字节的原因描述，可为空。
    pub reason: String,
    /// 原失败流程中的正投递次数，不是当前 DLT 消费次数。
    pub delivery_attempts: u32,
    /// 可选的逻辑源与物理集群扩展；历史证据缺失时不推测身份。
    pub origin: Option<super::DltOrigin>,
}

impl DltEvidence {
    /// 包含业务头在内的完整头字节上限。
    pub const MAX_HEADER_BYTES: usize = 65536;
    /// 包含重复及空值的完整头数量上限。
    pub const MAX_HEADERS: usize = 256;

    /// 业务作用：在使用公开来源字段前拒绝非法坐标和无界文本，不认证消息自报身份。
    /// 参数说明：无。返回：字段完整且范围有效才通过；非法声明为 InvalidConfiguration。
    pub fn validate(&self) -> Result<()> {
        if let Some(origin) = &self.origin {
            origin.validate()?;
        }
        if self.origin_topic.len() > 249 {
            return Err(Error::InvalidConfiguration);
        }
        ReplayPosition {
            topic: self.origin_topic.clone(),
            partition: self.origin_partition,
            start_offset: self.origin_offset,
            end_offset: self.origin_offset,
            next_offset: self.origin_offset,
        }
        .validate()
        .map_err(|_| Error::InvalidConfiguration)?;
        if self.origin_group.is_empty()
            || self.origin_group.len() > 1024
            || self.reason.len() > 1024
            || self.delivery_attempts < 1
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }

    /// 业务作用：在重放前拒绝缺项、重复、别名和非规范数字，半份证据不能作为普通输入跳过。
    /// 参数说明：headers 为完整有序消息头。返回：没有任何 DLT 字段为 None；六字段完整才返回来源，非法结构为 HeaderInvalid。
    pub fn read(headers: &KafkaHeaders) -> Result<Option<Self>> {
        let mut size = 0usize;
        let mut fields = BTreeMap::new();
        for (index, header) in headers.iter().enumerate() {
            size = size
                .checked_add(header.name.len())
                .and_then(|n| n.checked_add(header.value.as_ref().map_or(0, Vec::len)))
                .ok_or(Error::HeaderInvalid)?;
            if index >= Self::MAX_HEADERS || size > Self::MAX_HEADER_BYTES {
                return Err(Error::HeaderInvalid);
            }
            for name in FIELDS {
                if !header.name.eq_ignore_ascii_case(name) {
                    continue;
                }
                // 任何大小写别名或重复都拒绝，不能靠“取最后一个值”悄悄更换恢复来源。
                if header.name != name
                    || fields
                        .insert(name, header.value.as_deref().ok_or(Error::HeaderInvalid)?)
                        .is_some()
                {
                    return Err(Error::HeaderInvalid);
                }
            }
        }
        let origin = super::DltOrigin::read(headers)?;
        if fields.is_empty() {
            if origin.is_some() {
                return Err(Error::HeaderInvalid);
            }
            return Ok(None);
        }
        if fields.len() != FIELDS.len() {
            return Err(Error::HeaderInvalid);
        }
        let mut current_attempt = false;
        for header in headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(HEADER_DELIVERY_ATTEMPT))
        {
            if current_attempt || header.name != HEADER_DELIVERY_ATTEMPT {
                return Err(Error::HeaderInvalid);
            }
            decimal(
                header.value.as_deref().ok_or(Error::HeaderInvalid)?,
                u32::MAX as i64,
                false,
            )?;
            current_attempt = true;
        }
        // 当前 owner 会重新写 DELIVERY_ATTEMPT；原失败次数只取 DLT 专有字段，不要求两者相等。
        let evidence = Self {
            origin_topic: text(fields[FIELDS[0]], 249, false)?,
            origin_partition: decimal(fields[FIELDS[1]], i32::MAX as i64, true)? as i32,
            origin_offset: decimal(fields[FIELDS[2]], i64::MAX, true)?,
            origin_group: text(fields[FIELDS[3]], 1024, false)?,
            reason: text(fields[FIELDS[4]], 1024, true)?,
            delivery_attempts: decimal(fields[FIELDS[5]], u32::MAX as i64, false)? as u32,
            origin,
        };
        evidence.validate().map_err(|_| Error::HeaderInvalid)?;
        Ok(Some(evidence))
    }
}

/// 业务作用：避免字段通过非规范十进制或溢出改变来源身份。
/// 参数说明：bytes 为字段原文，max 为业务上限，zero 决定是否允许零。返回：规范非负数；空白、正负号、前导零和非 ASCII 数字拒绝。
fn decimal(bytes: &[u8], max: i64, zero: bool) -> Result<i64> {
    if bytes.is_empty() || bytes.len() > 19 || bytes.len() > 1 && bytes[0] == b'0' {
        return Err(Error::HeaderInvalid);
    }
    let mut value = 0i64;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return Err(Error::HeaderInvalid);
        }
        let digit = i64::from(*byte - b'0');
        if value > (max - digit) / 10 {
            return Err(Error::HeaderInvalid);
        }
        value = value * 10 + digit;
    }
    if !zero && value == 0 {
        return Err(Error::HeaderInvalid);
    }
    Ok(value)
}

/// 业务作用：在分配来源身份前限制字段长度并使用严格 UTF-8。
/// 参数说明：bytes 为字段，max 为字节上限，empty 决定是否允许空值。返回：唯一有效文本，不用替代字符构造身份。
fn text(bytes: &[u8], max: usize, empty: bool) -> Result<String> {
    if bytes.len() > max || !empty && bytes.is_empty() {
        return Err(Error::HeaderInvalid);
    }
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| Error::HeaderInvalid)
}
