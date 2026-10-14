//! 同组分配范围与逐记录重试元数据的共享容量；不授予输入确认或业务执行权。

use crate::types::Tp;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

pub(crate) struct MetadataCapacity {
    max_assignment: usize,
    max_retries: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    assignments: BTreeMap<usize, BTreeSet<Tp>>,
    retries: usize,
}

impl MetadataCapacity {
    /// 业务作用：冻结全组元数据上限，空闲 owner 不预占份额。
    /// 参数说明：max_assignment 为当前分配并集上限，max_retries 为失败记录数量上限。
    /// 返回：未使用的共享容量池。
    pub(crate) fn new(max_assignment: usize, max_retries: usize) -> Arc<Self> {
        Arc::new(Self {
            max_assignment,
            max_retries,
            state: Mutex::new(State::default()),
        })
    }

    /// 业务作用：在发布分配权威前原子校验所有 owner 的分区并集。
    /// 参数说明：slot 为稳定槽号，partitions 为该槽的完整新集合。
    /// 返回：成功替换；超限返回所需并集数量，原集合保持不变。
    pub(crate) fn assign(&self, slot: usize, partitions: &[Tp]) -> Result<(), usize> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut union: BTreeSet<&Tp> = partitions.iter().collect();
        for (owner, assigned) in &state.assignments {
            if *owner != slot {
                union.extend(assigned);
            }
        }
        // 接管同一分区只计一次；旧实际调用继续由输入与执行凭证单独约束。
        if union.len() > self.max_assignment {
            return Err(union.len());
        }
        if partitions.is_empty() {
            state.assignments.remove(&slot);
        } else {
            state
                .assignments
                .insert(slot, partitions.iter().cloned().collect());
        }
        Ok(())
    }

    /// 业务作用：将本会话分配登记绑定到退出路径，任何初始化失败或会话结束均归还范围。
    /// 参数说明：slot 为唯一 owner 槽号。
    /// 返回：析构时清除该槽分配登记的凭证，不影响重试及实际调用额度。
    pub(crate) fn session(self: &Arc<Self>, slot: usize) -> AssignmentSession {
        AssignmentSession {
            pool: Arc::clone(self),
            slot,
        }
    }

    /// 业务作用：原子接纳整批新增失败记录，避免只登记半个业务失败批次。
    /// 参数说明：count 为尚未持有重试凭证的记录数。
    /// 返回：每条记录独立凭证；容量不足返回 None 且不增加占用。
    pub(crate) fn reserve_retries(self: &Arc<Self>, count: usize) -> Option<Vec<RetryPermit>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if count > self.max_retries - state.retries {
            return None;
        }
        state.retries += count;
        Some(
            (0..count)
                .map(|_| RetryPermit {
                    pool: Arc::clone(self),
                })
                .collect(),
        )
    }
}

pub(crate) struct AssignmentSession {
    pool: Arc<MetadataCapacity>,
    slot: usize,
}
impl Drop for AssignmentSession {
    /// 业务作用：结束会话时撤销本槽分配容量登记。
    /// 参数说明：无。
    /// 返回：减少当前分配并集；旧实际业务责任仍保留各自凭证。
    fn drop(&mut self) {
        let _ = self.pool.assign(self.slot, &[]);
    }
}

pub(crate) struct RetryPermit {
    pool: Arc<MetadataCapacity>,
}
impl Drop for RetryPermit {
    /// 业务作用：在失败元数据被明确完成或失权清除后归还单条额度。
    /// 参数说明：无。
    /// 返回：全组重试占用减少一，不推进任何 broker 位点。
    fn drop(&mut self) {
        self.pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retries -= 1;
    }
}
