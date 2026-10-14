//! DLT 逻辑源与物理集群扩展；字段不授予权限，也不包含 topic 物理代次。

use super::{DltEvidence, DltReplayError as Error, DltReplayResult as Result};
use crate::{wire::*, KafkaHeader, KafkaHeaders};

/// 可信 owner 构造的源身份；读取外部消息时仍须结合部署 ACL 判断来源。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DltOrigin {
    /// 实际消费源的逻辑别名。
    pub source_id: String,
    /// broker 查询得到的物理集群身份。
    pub cluster_id: String,
}

impl DltOrigin {
    /// 业务作用：拒绝无界、空或含NUL的来源身份。
    /// 参数说明：无。返回：两个字段均为1至1024个UTF-8字节时通过；非法身份为InvalidConfiguration。
    pub fn validate(&self) -> Result<()> {
        for value in [&self.source_id, &self.cluster_id] {
            if value.is_empty() || value.len() > 1024 || value.contains('\0') {
                return Err(Error::InvalidConfiguration);
            }
        }
        Ok(())
    }

    /// 业务作用：重写来源自报的源和集群字段，同时保留其他有序业务头。
    /// 参数说明：headers为已构造的完整死信头。返回：唯一规范来源字段的新集合；非法身份拒绝，不认证传入身份。
    pub fn attach(&self, headers: &KafkaHeaders) -> Result<KafkaHeaders> {
        self.validate()?;
        let mut values = headers
            .iter()
            .filter(|h| !is_header(&h.name))
            .cloned()
            .collect::<Vec<_>>();
        values.extend([
            KafkaHeader {
                name: HEADER_DLT_ORIGIN_SOURCE.into(),
                value: Some(self.source_id.as_bytes().to_vec()),
            },
            KafkaHeader {
                name: HEADER_DLT_ORIGIN_CLUSTER.into(),
                value: Some(self.cluster_id.as_bytes().to_vec()),
            },
        ]);
        Ok(KafkaHeaders::from_vec(values))
    }

    /// 业务作用：读取可选来源扩展，拒绝半份身份和重复别名。
    /// 参数说明：headers为完整记录头。返回：历史记录为None；完整扩展返回身份，歧义或越界为HeaderInvalid。
    pub fn read(headers: &KafkaHeaders) -> Result<Option<Self>> {
        let mut fields = std::collections::BTreeMap::new();
        let mut size = 0usize;
        for (index, header) in headers.iter().enumerate() {
            size = size
                .checked_add(header.name.len())
                .and_then(|n| n.checked_add(header.value.as_ref().map_or(0, Vec::len)))
                .ok_or(Error::HeaderInvalid)?;
            if index >= DltEvidence::MAX_HEADERS || size > DltEvidence::MAX_HEADER_BYTES {
                return Err(Error::HeaderInvalid);
            }
            if !is_header(&header.name) {
                continue;
            }
            if !matches!(
                header.name.as_str(),
                HEADER_DLT_ORIGIN_SOURCE | HEADER_DLT_ORIGIN_CLUSTER
            ) || fields.contains_key(header.name.as_str())
            {
                return Err(Error::HeaderInvalid);
            }
            let bytes = header.value.as_deref().ok_or(Error::HeaderInvalid)?;
            if bytes.len() > 1024 {
                return Err(Error::HeaderInvalid);
            }
            fields.insert(
                header.name.as_str(),
                std::str::from_utf8(bytes)
                    .map_err(|_| Error::HeaderInvalid)?
                    .to_owned(),
            );
        }
        if fields.is_empty() {
            return Ok(None);
        }
        if fields.len() != 2 {
            return Err(Error::HeaderInvalid);
        }
        let origin = Self {
            source_id: fields
                .remove(HEADER_DLT_ORIGIN_SOURCE)
                .ok_or(Error::HeaderInvalid)?,
            cluster_id: fields
                .remove(HEADER_DLT_ORIGIN_CLUSTER)
                .ok_or(Error::HeaderInvalid)?,
        };
        origin.validate().map_err(|_| Error::HeaderInvalid)?;
        Ok(Some(origin))
    }
}

/// 业务作用：按共同ASCII规则识别必须重写的来源字段。
/// 参数说明：name为头名。返回：规范名称及ASCII别名均为true。
pub(crate) fn is_header(name: &str) -> bool {
    name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_SOURCE)
        || name.eq_ignore_ascii_case(HEADER_DLT_ORIGIN_CLUSTER)
}
