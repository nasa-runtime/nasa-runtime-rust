//! 回调期内有效的本地确认槽；不接触 broker，也不直接修改提交水位。

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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
    /// 并发调用的实时确认权威；owner 失权或预算到期时立即撤销。
    pub(crate) authority: Option<Arc<AtomicBool>>,
    /// 源所有者的永久停止意图，不等待 owner 消费控制邮箱才失权。
    pub(crate) stopping: Option<Arc<AtomicBool>>,
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
    /// 业务作用：为当前记录建立开放确认槽，将临时确认绑定到调用和 assignment 身份。
    ///
    /// 参数说明：
    ///
    /// - `identity`: 本次调用和记录的内部身份。
    ///
    /// 返回：尚未确认的本地槽；不验证当前权限、不接触 broker 或推进提交水位。
    fn new(identity: AckIdentity) -> Self {
        Self {
            state: AtomicU8::new(SLOT_OPEN),
            identity,
        }
    }

    /// 业务作用：在回调期内幂等记录临时确认。
    ///
    /// # 错误
    ///
    /// 回调已结束或 assignment 已失效时返回 [`NafkaError::AckExpired`]。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅当前有效调用可临时确认；失权、超时或封存后返回 AckExpired。
    fn acknowledge(&self) -> Result<()> {
        loop {
            // 先复验 owner 的实时权威；即使同步 handler 尚未归还线程，也不能继续确认。
            if !self.authorized() {
                self.state.store(SLOT_SEALED, Ordering::Release);
                return Err(NafkaError::AckExpired);
            }
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
                        return if self.authorized() {
                            Ok(())
                        } else {
                            Err(NafkaError::AckExpired)
                        };
                    }
                }
                SLOT_ACKED => return Ok(()),
                _ => return Err(NafkaError::AckExpired),
            }
        }
    }

    /// 业务作用：封存槽并返回封存前是否已确认。
    ///
    /// 参数说明：无。
    ///
    /// 返回：仅确认状态和 owner 权威同时有效时为 true，失权后的临时确认不能进入水位。
    fn seal(&self) -> bool {
        self.state.swap(SLOT_SEALED, Ordering::AcqRel) == SLOT_ACKED && self.authorized()
    }

    /// 业务作用：读取并发调用的实时确认许可，兼容 owner 栈内的同步路由。
    ///
    /// 参数说明：无。
    ///
    /// 返回：源尚未永久停止且调用仍获 owner 授权；同步调用也不能跨源停止边界确认。
    fn authorized(&self) -> bool {
        !self
            .identity
            .stopping
            .as_ref()
            .is_some_and(|stopping| stopping.load(Ordering::Acquire))
            && self
                .identity
                .authority
                .as_ref()
                .is_none_or(|authority| authority.load(Ordering::Acquire))
    }
}

/// 手动确认 token；刻意不实现 `Clone`，避免复制确认能力。
pub struct AckToken {
    /// 当前记录独占的本地确认槽。
    inner: Arc<AckSlot>,
}

impl AckToken {
    /// 业务作用：在当前 handler 回调期内同步记录本地确认。
    ///
    /// 返回成功只表示临时确认已记录；只有 handler 最终返回成功且 assignment epoch 仍有效，
    /// owner 才会把连续安全前缀纳入提交水位。
    ///
    /// # 错误
    ///
    /// 回调已经结束或 assignment 已失效时返回 [`NafkaError::AckExpired`]。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：本地临时确认被接受时成功；源停止、调用失权或槽已封存时返回 AckExpired。成功不代表 offset 已提交。
    pub fn ack(&self) -> Result<()> {
        self.inner.acknowledge()
    }

    /// 业务作用：暴露内部身份给 owner fencing，不向业务公开 offset 修改入口。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：槽绑定的原始身份引用；读取本身不复验实时权威，也不能重新开放已封存的槽。
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
    /// 业务作用：对当前记录执行手动确认。
    ///
    /// # 错误
    ///
    /// 自动模式返回 [`NafkaError::AckNotManual`]；过期 token 返回确认过期。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：手动模式委托 token 记录临时确认；自动模式或失效确认返回对应错误，不直接改变 broker 进度。
    pub(crate) fn acknowledge(&self) -> Result<()> {
        match self {
            Self::Auto => Err(NafkaError::AckNotManual),
            Self::Manual(token) => token.ack(),
        }
    }

    /// 业务作用：消费自身并取出手动 token。
    ///
    /// # 错误
    ///
    /// 自动模式没有 token，返回 [`NafkaError::AckNotManual`]。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：手动模式移交原 token，自动模式返回错误；移交不确认记录或延长 token 的有效期。
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
    /// 业务作用：按连续记录身份构造一组开放确认槽。
    ///
    /// 参数说明：
    ///
    /// - `identities`: 与 handler 输入顺序一致的记录身份。
    ///
    /// 返回：每条记录各有独立开放槽的集合；空输入得到空集合，不生成业务 token 或授予源读取权。
    pub(crate) fn new(identities: Vec<AckIdentity>) -> Self {
        Self {
            slots: identities
                .into_iter()
                .map(|identity| Arc::new(AckSlot::new(identity)))
                .collect(),
        }
    }

    /// 业务作用：为指定输入下标生成不实现 Clone 的记录确认 token，由 owner 保证每条输入只交付一次。
    ///
    /// 参数说明：
    ///
    /// - `index`: handler 输入记录下标。
    ///
    /// 返回：共享对应确认槽的手动 token；下标越界会 panic，本方法不记录下标是否已经领取，再次调用仍会生成同槽 token。
    pub(crate) fn record_ack(&self, index: usize) -> RecordAck {
        let inner = Arc::clone(&self.slots[index]);
        RecordAck::Manual(AckToken {
            inner,
        })
    }

    /// 业务作用：结束本次回调，封存全部 token，并计算有效连续确认前缀。
    ///
    /// 参数说明：
    ///
    /// - `handler_succeeded`: 仅 handler 返回成功时允许临时确认生效。
    ///
    /// 返回：handler 成功时同时统计有效连续前缀和全部有效确认数，失败时两者均为零；所有槽先被封存，重复调用不恢复确认。
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
    /// 业务作用：handler future 被 timeout 或取消丢弃时也必须封存全部 token。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部槽进入永久封存状态，遗留 token 不能再确认；不根据此前临时确认推进任何提交水位。
    fn drop(&mut self) {
        for slot in &self.slots {
            let _ = slot.seal();
        }
    }
}
