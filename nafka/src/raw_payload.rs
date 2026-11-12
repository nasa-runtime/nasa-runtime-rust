//! 异步消费的共享原文，不解析业务对象，也不提前释放来源输入额度。

use crate::{consumer::erased::RawRecord, DecodePayload, PayloadCodec, Result};
use std::sync::Arc;

/// 可跨异步调用保留的不可变正文；最后一个引用释放后才归还来源输入责任。
#[derive(Clone)]
pub struct RawPayload {
    /// 共享框架记录或调用方显式复制的正文，不复制底层字节。
    storage: Storage,
}

#[derive(Clone)]
enum Storage {
    /// 原文与输入额度独立于重投上下文，更新投递次数不得要求复制仍在转发的正文。
    Record {
        /// 框架只物化一次的正文。
        bytes: Arc<Vec<u8>>,
        /// 最后一个原文 owner 退出前持续保留的输入预算。
        _permit: Arc<crate::consumer::capacity::RecordPermit>,
    },
    /// 独立构造的原文，不携带任何来源确认权限。
    Bytes(Arc<[u8]>),
}

impl RawPayload {
    /// 业务作用：共享已准入记录的正文及其输入额度，避免异步消费再次复制原文。
    ///
    /// 参数说明：
    /// - `record`：已确认不是 tombstone 的框架记录。
    ///
    /// 返回：保留同一底层 owner 的只读正文，不附加确认权限。
    pub(crate) fn from_record(record: &RawRecord) -> Self {
        Self {
            storage: Storage::Record {
                bytes: record
                    .payload
                    .as_ref()
                    .expect("non-null raw payload")
                    .clone(),
                _permit: record.input_permit(),
            },
        }
    }

    /// 业务作用：为非 broker 来源建立独立原文所有权，调用后原切片可安全修改或释放。
    ///
    /// 参数说明：
    /// - `bytes`：需要复制一次的原始正文，空切片不是 tombstone。
    ///
    /// 返回：独立只读正文，不携带 broker 输入额度或确认能力。
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        Self {
            storage: Storage::Bytes(Arc::from(bytes)),
        }
    }
}

impl AsRef<[u8]> for RawPayload {
    /// 业务作用：借用固定原文，供校验、摘要或下游共享转发使用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：仅在当前引用存活期间有效的只读字节，不解析业务对象。
    fn as_ref(&self) -> &[u8] {
        match &self.storage {
            Storage::Record {
                bytes,
                ..
            } => bytes,
            Storage::Bytes(bytes) => bytes,
        }
    }
}

impl DecodePayload for RawPayload {
    const CODEC: PayloadCodec = PayloadCodec::Raw;

    /// 业务作用：在只有借用切片时建立原文所有权，不执行业务反序列化。
    ///
    /// 参数说明：
    /// - `bytes`：已校验线路声明的非 null 正文。
    ///
    /// 返回：复制一次的不可变原文，空正文保持为空。
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self::copy_from_slice(bytes))
    }

    /// 业务作用：接管框架已经物化的原文引用，使异步消费不增加正文副本。
    ///
    /// 参数说明：
    /// - `payload`：来源输入预算随引用保留的正文。
    ///
    /// 返回：原引用直接交接，正文和输入责任不重新分配。
    fn decode_owned(payload: RawPayload) -> Result<Self> {
        Ok(payload)
    }
}
