//! 回调期内有效的本地确认槽；不接触 broker，也不直接修改提交水位。

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use crate::error::{NafkaError, Result};

/// 确认槽仍可写入。
const SLOT_OPEN: u8 = 0;
/// 当前回调已经做出临时确认。
const SLOT_ACKED: u8 = 1;
/// 回调已结束或 assignment 已失效，确认槽永久封存。
const SLOT_SEALED: u8 = 2;

/// 单条记录的确认身份，只供诊断和内部 fencing 使用。
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct AckIdentity {
    /// 本次 handler 调用序号。
    pub(crate) invocation_id: u64,
    /// 创建 token 时的 assignment epoch。
    pub(crate) assignment_epoch: u64,
    /// topic 名。
    pub(crate) topic: String,
    /// 分区号。
    pub(crate) partition: i32,
    /// 当前记录 offset。
    pub(crate) offset: i64,
}

/// 单条记录的原子确认槽。
pub(crate) struct AckSlot {
    /// 确认状态。
    state: AtomicU8,
    /// 与本槽绑定的不可伪造身份。
    #[allow(dead_code)]
    identity: AckIdentity,
}

impl AckSlot {
    /// 创建开放确认槽。
    ///
    /// # 参数
    ///
    /// - `identity`: 本次调用和记录的内部身份。
    fn new(identity: AckIdentity) -> Self {
        Self {
            state: AtomicU8::new(SLOT_OPEN),
            identity,
        }
    }

    /// 在回调期内幂等记录临时确认。
    ///
    /// # 错误
    ///
    /// 回调已结束或 assignment 已失效时返回 [`NafkaError::AckExpired`]。
    fn acknowledge(&self) -> Result<()> {
        loop {
            match self.state.load(Ordering::Acquire) {
                SLOT_OPEN => {
                    if self
                        .state
                        .compare_exchange(
                            SLOT_OPEN,
                            SLOT_ACKED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                SLOT_ACKED => return Ok(()),
                _ => return Err(NafkaError::AckExpired),
            }
        }
    }

    /// 封存槽并返回封存前是否已确认。
    fn seal(&self) -> bool {
        self.state.swap(SLOT_SEALED, Ordering::AcqRel) == SLOT_ACKED
    }
}

/// 手动确认 token；刻意不实现 `Clone`，避免复制确认能力。
pub struct AckToken {
    /// 当前记录独占的本地确认槽。
    inner: Arc<AckSlot>,
}

impl AckToken {
    /// 在当前 handler 回调期内同步记录本地确认。
    ///
    /// 返回成功只表示临时确认已记录；只有 handler 最终返回成功且 assignment epoch 仍有效，
    /// owner 才会把连续安全前缀纳入提交水位。
    ///
    /// # 错误
    ///
    /// 回调已经结束或 assignment 已失效时返回 [`NafkaError::AckExpired`]。
    pub fn ack(&self) -> Result<()> {
        self.inner.acknowledge()
    }

    /// 暴露内部身份给 owner fencing，不向业务公开 offset 修改入口。
    #[allow(dead_code)]
    pub(crate) fn identity(&self) -> &AckIdentity {
        &self.inner.identity
    }
}

/// 单条记录携带的确认模式。
pub(crate) enum RecordAck {
    /// 自动模式不为每条消息分配确认槽。
    Auto,
    /// 手动模式持有本条记录唯一 token。
    Manual(AckToken),
}

impl RecordAck {
    /// 对当前记录执行手动确认。
    ///
    /// # 错误
    ///
    /// 自动模式返回 [`NafkaError::AckNotManual`]；过期 token 返回确认过期。
    pub(crate) fn acknowledge(&self) -> Result<()> {
        match self {
            Self::Auto => Err(NafkaError::AckNotManual),
            Self::Manual(token) => token.ack(),
        }
    }

    /// 消费自身并取出手动 token。
    ///
    /// # 错误
    ///
    /// 自动模式没有 token，返回 [`NafkaError::AckNotManual`]。
    pub(crate) fn into_token(self) -> Result<AckToken> {
        match self {
            Self::Auto => Err(NafkaError::AckNotManual),
            Self::Manual(token) => Ok(token),
        }
    }
}

/// 一次连续 handler run 的手动确认槽集合。
pub(crate) struct ManualAckSet {
    /// 与输入记录顺序一致的确认槽。
    slots: Vec<Arc<AckSlot>>,
}

/// 一次手动确认集合封存后的审计摘要。
pub(crate) struct ManualAckSummary {
    /// 从首记录开始的连续安全前缀长度。
    pub(crate) prefix: usize,
    /// 实际被业务确认的记录总数。
    pub(crate) acked: usize,
}

impl ManualAckSet {
    /// 按连续记录身份构造一组开放确认槽。
    ///
    /// # 参数
    ///
    /// - `identities`: 与 handler 输入顺序一致的记录身份。
    pub(crate) fn new(identities: Vec<AckIdentity>) -> Self {
        Self {
            slots: identities
                .into_iter()
                .map(|identity| Arc::new(AckSlot::new(identity)))
                .collect(),
        }
    }

    /// 为指定输入下标生成不可复制的记录确认能力。
    ///
    /// # 参数
    ///
    /// - `index`: handler 输入记录下标。
    pub(crate) fn record_ack(&self, index: usize) -> RecordAck {
        let inner = Arc::clone(&self.slots[index]);
        RecordAck::Manual(AckToken { inner })
    }

    /// 结束本次回调，封存全部 token，并计算有效连续确认前缀。
    ///
    /// # 参数
    ///
    /// - `handler_succeeded`: 仅 handler 返回成功时允许临时确认生效。
    ///
    /// # 返回
    ///
    /// handler 成功时返回从首记录开始的连续确认数；失败时固定返回零。
    pub(crate) fn finish(&self, handler_succeeded: bool) -> ManualAckSummary {
        let states: Vec<bool> = self.slots.iter().map(|slot| slot.seal()).collect();
        if handler_succeeded {
            ManualAckSummary {
                prefix: states.iter().take_while(|acked| **acked).count(),
                acked: states.iter().filter(|acked| **acked).count(),
            }
        } else {
            ManualAckSummary {
                prefix: 0,
                acked: 0,
            }
        }
    }
}

impl Drop for ManualAckSet {
    /// handler future 被 timeout 或取消丢弃时也必须封存全部 token。
    fn drop(&mut self) {
        for slot in &self.slots {
            let _ = slot.seal();
        }
    }
}
