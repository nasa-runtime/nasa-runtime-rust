//! 分区连续安全水位状态机:at-least-once 保证的正确性核心。
//!
//! 本模块是纯逻辑,不依赖底层客户端。P3 poll_task 负责:
//! 把每条记录的处理结果映射成 [`RecordOutcome`] 喂给 [`PartitionOffsetState::observe`]、
//! 读 [`PartitionOffsetState::committable_next`] 决定同步 commit 到哪、读
//! [`PartitionOffsetState::retry_from`] 决定失败 seek 回哪、commit 成功后调
//! [`PartitionOffsetState::confirm_committed`] 推进并裁剪。
//!
//! 核心裁决规则:
//! > committable_next = 首个不安全(Failed/Unprocessed)观察记录的 offset;
//! > 若全部安全,则 = 最末观察 offset + 1。retry_from = 首个不安全 offset。
//!
//! 为什么这条规则天然吃掉 offset 空洞:Kafka commit 是 next-offset(高水位),提交 next=首个不安全
//! offset 即声明"其下全部已处理"——包含被 compact/read_committed 过滤掉、从未投递的中间 offset。
//! 因此绝不越过失败记录,也不重复提交已安全的前缀,且无需假设 offset 数值连续。

use std::collections::{BTreeMap, BTreeSet};

use crate::consumer::PassthroughSkipReason;
use crate::types::Tp;

/// 分区进度状态机检测到的框架不变量错误。
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OffsetStateError {
    /// 一次进行中的观察跨越了 assignment epoch。
    #[error("观察跨 assignment epoch: expected={expected}, actual={actual}")]
    EpochMismatch {
        /// 当前进行中进度所属 epoch。
        expected: u64,
        /// 新观察携带的 epoch。
        actual: u64,
    },
    /// 新记录 offset 没有严格递增。
    #[error("观察 offset 未严格递增: previous={previous}, actual={actual}")]
    NonIncreasingOffset {
        /// 最近一次已观察 offset。
        previous: i64,
        /// 新观察携带的 offset。
        actual: i64,
    },
    /// 已安全处理的最大 offset 无法表示其 next-offset。
    #[error("next-offset 溢出: offset={offset}")]
    NextOffsetOverflow {
        /// 导致溢出的记录 offset。
        offset: i64,
    },
    /// commit 确认值不是当前状态机给出的安全前沿。
    #[error("commit 确认值非法: expected={expected:?}, actual={actual}")]
    InvalidCommitConfirmation {
        /// 当前唯一允许确认的 next-offset。
        expected: Option<i64>,
        /// 调用方声称 broker 已确认的 next-offset。
        actual: i64,
    },
    /// 单分区进行中结果超过配置上限。
    #[error("分区 progress 超限: tp={tp:?}, max={max}")]
    PartitionProgressFull {
        /// 超限分区。
        tp: Tp,
        /// 单分区最大结果数。
        max: usize,
    },
    /// group 进行中结果总数超过配置上限。
    #[error("group progress 超限: max={max}")]
    GroupProgressFull {
        /// group 最大结果总数。
        max: usize,
    },
}

impl OffsetStateError {
    /// 判断错误是否只是 outcome permit 暂时不足，而不是 offset/epoch 不变量破坏。
    pub(crate) fn is_capacity(&self) -> bool {
        matches!(
            self,
            Self::PartitionProgressFull { .. } | Self::GroupProgressFull { .. }
        )
    }
}

/// 分区进度状态机的内部结果类型。
type OffsetResult<T> = std::result::Result<T, OffsetStateError>;

/// 单条记录进入安全水位的确认来源。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AckSource {
    /// 自动模式:handler 返回成功后框架确认。
    AutoOnSuccess,
    /// 手动模式:业务在回调期内显式 ack 且 handler 最终成功。
    Manual,
}

/// 单条记录被安全跳过(可越过提交)的原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// 无匹配 route 且 unmatched_policy=Skip。
    UnmatchedRoute,
    /// typed route 上的 tombstone(空 payload)默认跳过。
    Tombstone,
    /// dead_letter_required=false 时对失败记录的显式可用性优先跳过。
    AvailabilityFirst,
    /// passthrough 回调返回的类型化安全跳过(本节点无目标/回环/旧 incarnation 等)。
    Passthrough(PassthroughSkipReason),
}

/// 单条已观察记录的处理结果;`is_safe` 决定它是否可被提交越过。
///
/// 说明:协议合同的 `Failed(FailureUnit)` 里 FailureUnit(失败单元是单条还是连续 run)
/// 属重投层(P2 retry.rs)概念,frontier 计算并不需要它,故此处 `Failed` 只保留"不安全"这一位,
/// 失败单元归属由 retry.rs 管理,不在本纯状态机内耦合。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    /// 已确认(自动成功 / 手动确认)。
    Acknowledged(AckSource),
    /// 已按安全规则跳过。
    Skipped(SkipReason),
    /// 已成功转入死信 topic(broker delivery success)。
    DeadLettered,
    /// handler 失败 / panic / timeout,或解码失败且 DLT 未成功等——不安全。
    Failed,
    /// 已观察但尚未派发处理(如失败分区其后记录)——不安全。
    #[allow(dead_code)]
    Unprocessed,
}

impl RecordOutcome {
    /// 是否可被提交越过:已确认 / 已跳过 / 已入死信为安全;失败与未处理为不安全。
    ///
    /// 这是整条水位规则唯一的分类点。
    fn is_safe(self) -> bool {
        matches!(
            self,
            RecordOutcome::Acknowledged(_)
                | RecordOutcome::Skipped(_)
                | RecordOutcome::DeadLettered
        )
    }
}

/// 单条观察记录:真实 offset + 结果。保存真实 offset 而非数组下标,以支持 offset 空洞。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ObservedOutcome {
    /// 记录真实 offset。
    offset: i64,
    /// 处理结果。
    outcome: RecordOutcome,
}

/// 当前 assignment epoch 内的进行中进度。
struct PartitionProgress {
    /// 观察发生时的 assignment epoch;rebalance 换 epoch 时 poll_task 应先 [`PartitionOffsetState::reset`]。
    assignment_epoch: u64,
    /// 按 offset 严格递增(允许 gap)的已观察记录。
    observed: Vec<ObservedOutcome>,
}

/// 单分区的连续安全水位状态。
pub(crate) struct PartitionOffsetState {
    /// 当前 assignment 实际开始读取的 next-offset；没有提交历史时也必须存在。
    baseline: i64,
    /// broker 已确认的 next offset(不是本地 consumer position);None = 本 assignment 尚无提交。
    last_confirmed: Option<i64>,
    /// last_confirmed 之后、当前 epoch 内的进行中进度;None = 无待处理观察。
    in_flight: Option<PartitionProgress>,
}

impl PartitionOffsetState {
    /// 在 assignment 时用起始位点初始化。
    ///
    /// # 参数
    ///
    /// - `committed`: broker 已确认的 next-offset；没有提交历史时为 `None`。
    /// - `resolved_start`: 底层按重置策略解析后的实际读取起点；必须是非负 offset。
    ///
    /// # 错误
    ///
    /// `committed` 或 `resolved_start` 为负数时返回不变量错误。
    pub(crate) fn new(committed: Option<i64>, resolved_start: i64) -> OffsetResult<Self> {
        if committed.is_some_and(|offset| offset < 0) || resolved_start < 0 {
            return Err(OffsetStateError::InvalidCommitConfirmation {
                expected: committed,
                actual: resolved_start,
            });
        }
        Ok(Self {
            baseline: committed.unwrap_or(resolved_start),
            last_confirmed: committed,
            in_flight: None,
        })
    }

    /// 返回 broker 已确认的 next offset。
    #[allow(dead_code)]
    pub(crate) fn last_confirmed(&self) -> Option<i64> {
        self.last_confirmed
    }

    /// 记录一条已处理记录的结果。
    ///
    /// # 参数
    ///
    /// - `assignment_epoch`: 观察发生时的 epoch;必须与本 in_flight 一致(rebalance 后 poll_task 先 reset)。
    /// - `offset`: 记录真实 offset;必须严格大于上一条观察 offset(允许与其相差多于 1,即 gap)。
    /// - `outcome`: 该记录处理结果。
    ///
    /// # 错误
    ///
    /// epoch 不一致或 offset 没有严格递增时返回框架不变量错误；调用方必须 Halt，不能继续提交。
    pub(crate) fn observe(
        &mut self,
        assignment_epoch: u64,
        offset: i64,
        outcome: RecordOutcome,
    ) -> OffsetResult<()> {
        let progress = self.in_flight.get_or_insert_with(|| PartitionProgress {
            assignment_epoch,
            observed: Vec::new(),
        });
        if progress.assignment_epoch != assignment_epoch {
            return Err(OffsetStateError::EpochMismatch {
                expected: progress.assignment_epoch,
                actual: assignment_epoch,
            });
        }
        if let Some(last) = progress.observed.last() {
            if offset <= last.offset {
                return Err(OffsetStateError::NonIncreasingOffset {
                    previous: last.offset,
                    actual: offset,
                });
            }
        }
        progress.observed.push(ObservedOutcome { offset, outcome });
        Ok(())
    }

    /// 在不修改状态的前提下校验一个连续交付 run 能否原子接纳。
    ///
    /// # 参数
    ///
    /// - `assignment_epoch`: run 所属 assignment epoch。
    /// - `offsets`: 按 broker 交付顺序排列的非空真实 offsets；允许数值 gap。
    ///
    /// # 错误
    ///
    /// epoch 失配或 offset 没有相对当前状态严格递增时返回错误。
    fn validate_run(&self, assignment_epoch: u64, offsets: &[i64]) -> OffsetResult<()> {
        if let Some(progress) = &self.in_flight {
            if progress.assignment_epoch != assignment_epoch {
                return Err(OffsetStateError::EpochMismatch {
                    expected: progress.assignment_epoch,
                    actual: assignment_epoch,
                });
            }
        }
        let mut previous = self
            .in_flight
            .as_ref()
            .and_then(|progress| progress.observed.last())
            .map(|observed| observed.offset);
        for offset in offsets {
            if let Some(previous_offset) = previous {
                if *offset <= previous_offset {
                    return Err(OffsetStateError::NonIncreasingOffset {
                        previous: previous_offset,
                        actual: *offset,
                    });
                }
            }
            previous = Some(*offset);
        }
        Ok(())
    }

    /// 计算当前可安全提交的 next-offset(高水位);`None` 表示没有比 last_confirmed 更高的安全前缀。
    ///
    /// 规则见模块头:首个不安全 offset,或全安全时的末尾 offset + 1;并要求结果严格大于 last_confirmed
    /// 才有提交意义(否则本批没有推进已确认位点)。
    pub(crate) fn committable_next(&self) -> OffsetResult<Option<i64>> {
        let Some(progress) = self.in_flight.as_ref() else {
            return Ok(None);
        };
        if progress.observed.is_empty() {
            return Ok(None);
        }
        let frontier = frontier_of(&progress.observed)?;
        Ok(match self.last_confirmed {
            // frontier 不高于已确认位点 → 本批无安全前缀可推进。
            Some(confirmed) if frontier <= confirmed => None,
            // 无提交历史时也不能把首条失败的 resolved_start 当成可提交进展。
            None if frontier <= self.baseline => None,
            _ => Some(frontier),
        })
    }

    /// 返回失败重投的起点(首个不安全 offset);`None` 表示本批无失败记录。
    ///
    /// poll_task 据此 seek 回该 offset 重投,并让其后尚未派发的预取记录重新拉取。
    #[allow(dead_code)]
    pub(crate) fn retry_from(&self) -> Option<i64> {
        let progress = self.in_flight.as_ref()?;
        progress
            .observed
            .iter()
            .find(|observed| !observed.outcome.is_safe())
            .map(|observed| observed.offset)
    }

    /// broker 同步 commit 成功后调用:推进 last_confirmed 并裁剪已提交的观察记录。
    ///
    /// # 参数
    ///
    /// - `next_offset`: 已成功提交的 next-offset(应来自本状态机的 [`Self::committable_next`])。
    ///
    /// 裁剪保留 `offset >= next_offset` 的记录:例如提交到首个不安全 offset 时,该失败记录仍留在
    /// observed 里参与后续重投判定;全安全提交(next=末尾+1)时全部裁完,in_flight 清空。
    /// 这样 observed 的常数上限由"commit 后即裁剪"保证,失败时不继续扩大。
    /// # 错误
    ///
    /// `next_offset` 不是调用当下的唯一安全前沿时返回不变量错误，状态保持不变。
    pub(crate) fn confirm_committed(&mut self, next_offset: i64) -> OffsetResult<()> {
        let expected = self.committable_next()?;
        if expected != Some(next_offset) {
            return Err(OffsetStateError::InvalidCommitConfirmation {
                expected,
                actual: next_offset,
            });
        }
        self.last_confirmed = Some(next_offset);
        self.baseline = next_offset;
        if let Some(progress) = self.in_flight.as_mut() {
            progress
                .observed
                .retain(|observed| observed.offset >= next_offset);
            if progress.observed.is_empty() {
                self.in_flight = None;
            }
        }
        Ok(())
    }

    /// 清空当前 epoch 的进行中进度(rebalance 收回分区或作废本地 progress 时用)。
    ///
    /// 只丢弃未提交的观察进度,不改动 last_confirmed(broker 已确认的位点由新 owner 重放继续)。
    pub(crate) fn reset(&mut self) {
        self.in_flight = None;
    }

    /// 当前进行中观察记录数,供 per-partition 容量上限计量。
    pub(crate) fn observed_len(&self) -> usize {
        self.in_flight
            .as_ref()
            .map_or(0, |progress| progress.observed.len())
    }

    /// 当前是否存在不安全（失败 / 未处理）的已观察记录。
    fn has_unsafe_observation(&self) -> bool {
        self.in_flight.as_ref().is_some_and(|progress| {
            progress
                .observed
                .iter()
                .any(|observed| !observed.outcome.is_safe())
        })
    }
}

/// 单 owner 内全部分区的连续安全水位协调器。
///
/// 该类型只维护内存事实，不直接访问 broker；调用方仅在同步提交成功后调用
/// [`Self::confirm_committed`]，从而把“业务已安全处理”和“broker 已确认”严格分开。
pub(crate) struct GroupOffsetState {
    /// 每个已观察分区的独立状态机。
    partitions: BTreeMap<Tp, PartitionOffsetState>,
    /// 当前所有分区尚未裁剪的结果总数。
    observed_total: usize,
    /// 单分区结果硬上限。
    max_partition_records: usize,
    /// group 结果总量硬上限。
    max_group_records: usize,
}

impl GroupOffsetState {
    /// 构造带双层容量上限的 group 状态。
    ///
    /// # 参数
    ///
    /// - `max_partition_records`: 每分区最大未裁剪结果数。
    /// - `max_group_records`: group 跨分区最大未裁剪结果数。
    pub(crate) fn new(max_partition_records: usize, max_group_records: usize) -> Self {
        Self {
            partitions: BTreeMap::new(),
            observed_total: 0,
            max_partition_records,
            max_group_records,
        }
    }

    /// 原子接纳一条处理结果。
    ///
    /// 容量检查先于状态修改；任一门禁失败都不会留下半条 progress。
    ///
    /// # 参数
    ///
    /// - `tp`: 记录所属分区。
    /// - `assignment_epoch`: 当前 assignment epoch。
    /// - `offset`: broker 交付的真实 offset。
    /// - `outcome`: 本次处理结果。
    ///
    /// # 错误
    ///
    /// 容量超限、epoch 失配或 offset 未递增时返回不变量错误。
    pub(crate) fn observe(
        &mut self,
        tp: &Tp,
        assignment_epoch: u64,
        offset: i64,
        outcome: RecordOutcome,
    ) -> OffsetResult<()> {
        let partition_len = self
            .partitions
            .get(tp)
            .map_or(0, PartitionOffsetState::observed_len);
        if partition_len >= self.max_partition_records {
            return Err(OffsetStateError::PartitionProgressFull {
                tp: tp.clone(),
                max: self.max_partition_records,
            });
        }
        if self.observed_total >= self.max_group_records {
            return Err(OffsetStateError::GroupProgressFull {
                max: self.max_group_records,
            });
        }
        let state = match self.partitions.entry(tp.clone()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PartitionOffsetState::new(None, offset)?)
            }
        };
        state.observe(assignment_epoch, offset, outcome)?;
        self.observed_total += 1;
        Ok(())
    }

    /// 原子接纳同一分区的一个连续交付 run。
    ///
    /// 容量、epoch 与 offset 顺序全部预校验成功后才写入，避免批 handler 的部分 outcome
    /// 进入水位而剩余项因容量不足丢失。
    ///
    /// # 参数
    ///
    /// - `tp`: run 所属分区。
    /// - `assignment_epoch`: 当前 assignment epoch。
    /// - `outcomes`: 按交付顺序排列的真实 offset 与结果。
    ///
    /// # 错误
    ///
    /// run 为空直接成功；容量超限、epoch 失配或 offset 未递增时返回错误且状态不变。
    pub(crate) fn observe_run(
        &mut self,
        tp: &Tp,
        assignment_epoch: u64,
        outcomes: &[(i64, RecordOutcome)],
    ) -> OffsetResult<()> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let partition_len = self
            .partitions
            .get(tp)
            .map_or(0, PartitionOffsetState::observed_len);
        let next_partition = partition_len.checked_add(outcomes.len()).ok_or_else(|| {
            OffsetStateError::PartitionProgressFull {
                tp: tp.clone(),
                max: self.max_partition_records,
            }
        })?;
        if next_partition > self.max_partition_records {
            return Err(OffsetStateError::PartitionProgressFull {
                tp: tp.clone(),
                max: self.max_partition_records,
            });
        }
        let next_group = self.observed_total.checked_add(outcomes.len()).ok_or(
            OffsetStateError::GroupProgressFull {
                max: self.max_group_records,
            },
        )?;
        if next_group > self.max_group_records {
            return Err(OffsetStateError::GroupProgressFull {
                max: self.max_group_records,
            });
        }
        let offsets = outcomes
            .iter()
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>();
        if let Some(state) = self.partitions.get(tp) {
            state.validate_run(assignment_epoch, &offsets)?;
        } else {
            PartitionOffsetState::new(None, outcomes[0].0)?
                .validate_run(assignment_epoch, &offsets)?;
        }
        let state = match self.partitions.entry(tp.clone()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PartitionOffsetState::new(None, outcomes[0].0)?)
            }
        };
        for (offset, outcome) in outcomes {
            state.observe(assignment_epoch, *offset, *outcome)?;
        }
        self.observed_total = next_group;
        Ok(())
    }

    /// 返回每个分区当前可以安全同步提交的 next-offset。
    ///
    /// # 错误
    ///
    /// 任一分区的末尾 offset 无法表示 next-offset 时返回错误。
    pub(crate) fn committable_offsets(&self) -> OffsetResult<BTreeMap<Tp, i64>> {
        self.partitions
            .iter()
            .filter_map(|(tp, state)| match state.committable_next() {
                Ok(Some(offset)) => Some(Ok((tp.clone(), offset))),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    /// 在一次 broker 批量同步提交成功后原子确认并裁剪全部对应分区。
    ///
    /// # 参数
    ///
    /// - `offsets`: 本次已由 broker 确认的分区 next-offset 表。
    ///
    /// # 错误
    ///
    /// 任一 offset 不等于其分区当前安全前沿时，所有状态保持不变。
    pub(crate) fn confirm_committed(&mut self, offsets: &BTreeMap<Tp, i64>) -> OffsetResult<()> {
        for (tp, offset) in offsets {
            let expected = self
                .partitions
                .get(tp)
                .map(PartitionOffsetState::committable_next)
                .transpose()?
                .flatten();
            if expected != Some(*offset) {
                return Err(OffsetStateError::InvalidCommitConfirmation {
                    expected,
                    actual: *offset,
                });
            }
        }
        for (tp, offset) in offsets {
            self.partitions
                .get_mut(tp)
                .expect("预校验已确认分区存在")
                .confirm_committed(*offset)?;
        }
        self.recount();
        Ok(())
    }

    /// 丢弃指定分区未提交的本地进度。
    ///
    /// # 参数
    ///
    /// - `tp`: seek、失败重投或 revoke 的分区。
    pub(crate) fn reset_partition(&mut self, tp: &Tp) {
        // 热失败、DLT 与 rebalance 清理只能作废本 epoch 的观察结果；broker 已确认水位是
        // 跨 epoch 的事实。删除整个条目会让下一条消息被当作新 baseline，掩盖 fetch position
        // 跳变并允许未处理记录被越过。
        if let Some(state) = self.partitions.get_mut(tp) {
            state.reset();
        }
        self.recount();
    }

    /// 只保留新 assignment 仍归当前 owner 的分区，并清空它们的旧 epoch 观察。
    ///
    /// # 参数
    ///
    /// - `assigned`: 新 assignment 集合。
    pub(crate) fn reset_for_assignment(&mut self, assigned: &[Tp]) {
        let assigned: BTreeSet<&Tp> = assigned.iter().collect();
        self.partitions.retain(|tp, state| {
            if assigned.contains(tp) {
                state.reset();
                true
            } else {
                false
            }
        });
        self.recount();
    }

    /// 清空全部本地进度；用于显式重订阅、取消订阅和 restart。
    pub(crate) fn clear(&mut self) {
        self.partitions.clear();
        self.observed_total = 0;
    }

    /// 返回当前未裁剪结果总数，作为按记录数触发 commit 的计量值。
    pub(crate) fn observed_total(&self) -> usize {
        self.observed_total
    }

    /// 判断 group outcome permit 是否至少释放出一个位置。
    pub(crate) fn has_group_capacity(&self) -> bool {
        self.observed_total < self.max_group_records
    }

    /// 判断是否可以安全地恢复派发。
    ///
    /// 判据 = group 总量有余 **且** 触发本次反压的那个分区自身有余。
    ///
    /// 只查 group 总量不够：触发方可能是单分区上限，此时 group 仍有余量，恢复后会立刻
    /// 再次撞上同一分区上限，形成 commit/pause/resume/seek 的每轮空转。
    /// 但也**不能**要求"每个分区都有余"：门禁会暂停全部分区，只要有任意一个分区卡在上限
    /// （例如已 Halt 且前沿无法推进），整组就再也无法恢复——把空转升级成了全组死锁。
    ///
    /// # 参数
    ///
    /// - `trigger`: 触发反压的分区；`None` 表示只按 group 总量裁决。
    pub(crate) fn has_dispatch_capacity(&self, trigger: Option<&Tp>) -> bool {
        if !self.has_group_capacity() {
            return false;
        }
        trigger.is_none_or(|tp| {
            self.partitions
                .get(tp)
                .is_none_or(|state| state.observed_len() < self.max_partition_records)
        })
    }

    /// 是否存在已观察但不安全（失败 / 未处理）的记录，即"空洞仍在"。
    ///
    /// 规则 5 用它区分「等待重试/DLT 收尾的正常反压」与「满且无任何可推进理由」
    /// 的框架不变量破坏。
    pub(crate) fn has_pending_gap(&self) -> bool {
        self.partitions
            .values()
            .any(PartitionOffsetState::has_unsafe_observation)
    }

    /// 重新汇总总结果数；只在提交裁剪或显式清理后调用。
    fn recount(&mut self) {
        self.observed_total = self
            .partitions
            .values()
            .map(PartitionOffsetState::observed_len)
            .sum();
    }
}

/// 计算一段严格递增观察序列的提交上界(next-offset)。
///
/// # 参数
///
/// - `observed`: 非空、按 offset 严格递增的观察序列。
///
/// 返回首个不安全 offset;若全部安全,返回末尾 offset + 1。
fn frontier_of(observed: &[ObservedOutcome]) -> OffsetResult<i64> {
    for entry in observed {
        if !entry.outcome.is_safe() {
            // 首个不安全 offset 即提交上界:其下(含 gap)全部已安全,它本身及之后不得越过。
            return Ok(entry.offset);
        }
    }
    // 全部安全:提交到最末观察 offset 的下一位(中间 gap 已随"首个不安全越过"或此处一并越过)。
    let offset = observed
        .last()
        .expect("frontier_of 的前置条件是非空序列")
        .offset;
    offset
        .checked_add(1)
        .ok_or(OffsetStateError::NextOffsetOverflow { offset })
}
