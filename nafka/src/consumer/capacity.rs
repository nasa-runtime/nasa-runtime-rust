//! 跨 owner 的输入责任额度；取入、重试与待提交共享同一凭证。

use super::offset_state::GroupOffsetState;
use super::source_capacity::{SourceCapacity, SourcePermit};
use crate::types::Tp;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// 原文、拥有型上下文与完整键顺序域的共同保守预留，不计 native 缓存或业务解码堆。
const INPUT_COPY_RESERVATIONS: usize = 8;

/// 业务作用：在取入前覆盖框架必要副本，避免原输入占满字节后没有空间执行业务。
/// 参数说明：raw_bytes 为原始 payload/key/headers 字节和。
/// 返回：整个输入生命周期的保守预留；溢出饱和，随后由单条准入门禁拒绝。
pub(crate) fn input_reservation_bytes(raw_bytes: usize) -> usize {
    raw_bytes.saturating_mul(INPUT_COPY_RESERVATIONS)
}

#[derive(Default)]
struct Usage {
    records: usize,
    bytes: usize,
    partitions: BTreeMap<Tp, PartitionUsage>,
}

#[derive(Default)]
struct PartitionUsage {
    records: usize,
    bytes: usize,
    keys: BTreeMap<InputKey, (usize, usize)>,
}

/// 固定长度计费身份；摘要碰撞只会保守合并额度，不参与业务顺序权限裁决。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InputKey(Option<[u8; 32]>);

impl InputKey {
    /// 业务作用：在准入前生成有界计费身份，避免等待容量本身保存巨大原始 key。
    /// 参数说明：raw 为原始键；None 与空字节不同。
    /// 返回：分区内的计费身份，不替代执行层的无损 key 顺序域。
    fn new(raw: Option<&[u8]>) -> Self {
        Self(raw.map(|key| Sha256::digest(key).into()))
    }
}

/// 同一组的原始输入账本，实际占用不随 owner 数量扩大。
pub(crate) struct GroupCapacity {
    source: Arc<SourceCapacity>,
    max_records: usize,
    max_bytes: usize,
    max_partition_records: usize,
    max_partition_bytes: usize,
    max_key_records: usize,
    max_key_bytes: usize,
    usage: Mutex<Usage>,
}

impl GroupCapacity {
    /// 业务作用：提供同一计费锁内的全组占用，避免槽间迁移时拼接不同时刻的局部计数。
    /// 参数说明：无。
    /// 返回：实际保留记录数与副本预留字节的共同快照，不代表后续准入权利。
    pub(crate) fn usage(&self) -> (usize, usize) {
        let usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (usage.records, usage.bytes)
    }

    /// 业务作用：冻结全组输入数量、字节与分区记录上限，不为 owner 预留固定份额。
    /// 参数说明：source 为源共享池；max_records/max_bytes 为组边界；max_partition_records/max_partition_bytes 为分区边界；max_key_records/max_key_bytes 为同分区同键边界。
    /// 返回：尚无责任的共享池；所有额度在实际取入前申请。
    pub(crate) fn new(
        source: Arc<SourceCapacity>,
        max_records: usize,
        max_bytes: usize,
        max_partition_records: usize,
        max_partition_bytes: usize,
        max_key_records: usize,
        max_key_bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            source,
            max_records,
            max_bytes,
            max_partition_records,
            max_partition_bytes,
            max_key_records,
            max_key_bytes,
            usage: Mutex::new(Usage::default()),
        })
    }

    /// 业务作用：在物化原文前同时裁决组与分区数量、组字节额度，避免先复制再发现容量不足。
    /// 参数说明：tp/key 为计费身份；bytes 为原始 key、payload 与完整有序头的副本预留；owner 为实际责任的槽计数。
    /// 返回：覆盖当前输入责任的共享凭证；不足时不修改任何计数。
    fn reserve(
        self: &Arc<Self>,
        tp: &Tp,
        key: InputKey,
        bytes: usize,
        owner: &Arc<Mutex<(usize, usize, usize, usize)>>,
    ) -> Option<Arc<RecordPermit>> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.available(&usage, tp, key, bytes) {
            return None;
        }
        // 上层容量须在本组计数增加前取得；失败直接交回原始坐标，不留部分责任。
        let source = self.source.reserve(bytes)?;
        usage.records += 1;
        usage.bytes += bytes;
        let partition = usage.partitions.entry(tp.clone()).or_default();
        partition.records += 1;
        partition.bytes += bytes;
        let keyed = partition.keys.entry(key).or_default();
        keyed.0 += 1;
        keyed.1 += bytes;
        let mut owned = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owned.0 += 1;
        owned.1 += bytes;
        Some(Arc::new(RecordPermit {
            capacity: Arc::clone(self),
            owner: Arc::clone(owner),
            tp: tp.clone(),
            key,
            bytes,
            _source: source,
            dlt_intent: Mutex::new(None),
            keyed: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// 业务作用：在同一计费锁内判断输入能否进入，数量与字节必须同时满足。
    /// 参数说明：usage 为当前占用；tp/key/bytes 为待取入记录身份和大小。
    /// 返回：仅表示该快照有余量，后续预留仍须重新取得计费锁。
    fn available(&self, usage: &Usage, tp: &Tp, key: InputKey, bytes: usize) -> bool {
        if usage.records >= self.max_records || bytes > self.max_bytes - usage.bytes {
            return false;
        }
        if let Some(partition) = usage.partitions.get(tp) {
            if partition.records >= self.max_partition_records
                || bytes > self.max_partition_bytes - partition.bytes
            {
                return false;
            }
            let keyed = partition.keys.get(&key).copied().unwrap_or_default();
            if keyed.0 >= self.max_key_records || bytes > self.max_key_bytes - keyed.1 {
                return false;
            }
        } else if bytes > self.max_partition_bytes.min(self.max_key_bytes) {
            return false;
        }
        self.source.can_reserve(bytes)
    }

    /// 业务作用：决定因共享容量暂停的分区是否值得再次尝试取入，不在等待期间反复 seek。
    /// 参数说明：tp/key/bytes 为上次因容量交回的记录。
    /// 返回：有余量时为 true；并发 owner 仍可能先取得该余量。
    fn can_reserve(&self, tp: &Tp, key: InputKey, bytes: usize) -> bool {
        self.available(
            &self
                .usage
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            tp,
            key,
            bytes,
        )
    }
}

/// 同一条责任可由输入批和提交账本共同持有，最后一份实际责任退出才归还额度。
pub(crate) struct RecordPermit {
    _source: SourcePermit,
    capacity: Arc<GroupCapacity>,
    owner: Arc<Mutex<(usize, usize, usize, usize)>>,
    tp: Tp,
    key: InputKey,
    bytes: usize,
    dlt_intent: Mutex<Option<DeadLetterIntent>>,
    /// 一旦路由确认为按键模式，指标责任随此凭证保持到最后一个实际引用释放。
    keyed: std::sync::atomic::AtomicBool,
}

/// 已发生的失败事实，仅随已准入输入存活；容量重读不重复调用业务或增加尝试次数。
#[derive(Clone)]
pub(crate) struct DeadLetterIntent {
    pub(crate) reason: String,
    pub(crate) attempts: u32,
}

impl RecordPermit {
    /// 业务作用：路由确定后为同一输入凭证登记一次按键观测，不重新申请容量或复制原文。
    /// 参数说明：无。返回：重复识别无副作用；计数随凭证实际寿命减少。
    pub(crate) fn mark_keyed(&self) {
        if !self.keyed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let mut owned = self
                .owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            owned.2 += 1;
            owned.3 += self.bytes;
        }
    }
}

impl Drop for RecordPermit {
    /// 业务作用：最后一个输入、执行或提交责任持有者退出后归还全组额度。
    /// 参数说明：无。
    /// 返回：数量与字节同时归还，不把释放解释为业务成功。
    fn drop(&mut self) {
        let mut usage = self
            .capacity
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.records -= 1;
        usage.bytes -= self.bytes;
        let partition = usage
            .partitions
            .get_mut(&self.tp)
            .expect("记录拥有分区计费责任");
        partition.records -= 1;
        partition.bytes -= self.bytes;
        let keyed = partition
            .keys
            .get_mut(&self.key)
            .expect("记录拥有 key 计费责任");
        keyed.0 -= 1;
        keyed.1 -= self.bytes;
        if keyed.0 == 0 {
            partition.keys.remove(&self.key);
        }
        if partition.records == 0 {
            usage.partitions.remove(&self.tp);
        }
        let mut owned = self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owned.0 -= 1;
        owned.1 -= self.bytes;
        if self.keyed.load(std::sync::atomic::Ordering::Acquire) {
            owned.2 -= 1;
            owned.3 -= self.bytes;
        }
    }
}

/// owner 的稳定输入责任表；从失败位置重新取入时复用凭证，不制造预算空窗。
pub(crate) struct RecordLedger {
    /// 停止期间的清理只形成未核实交回计数，不恢复来源存活或提交证明。
    closing: std::sync::atomic::AtomicBool,
    released_unverified: std::sync::atomic::AtomicU64,
    capacity: Arc<GroupCapacity>,
    records: Mutex<BTreeMap<(Tp, i64), Arc<RecordPermit>>>,
    blocked: Mutex<BTreeMap<Tp, (i64, InputKey, usize)>>,
    dlt_refetch: Mutex<BTreeMap<Tp, i64>>,
    usage: Arc<Mutex<(usize, usize, usize, usize)>>,
}

impl RecordLedger {
    /// 业务作用：在永久停止通知前启用未确认释放留证，不保存记录历史或原文。
    /// 参数说明：无。
    /// 返回：之后的失权清理累计计数；broker 确认裁剪不计入未确认释放。
    pub(crate) fn begin_closing(&self) {
        self.closing
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// 业务作用：读取停止期间未获提交确认的释放次数。
    /// 参数说明：无。
    /// 返回：单调饱和计数；来源仍可重读需要另行核实。
    pub(crate) fn released_unverified(&self) -> u64 {
        self.released_unverified
            .load(std::sync::atomic::Ordering::Acquire)
    }
    /// 业务作用：死信意图重新取入前冻结必须再次见到的原坐标，不能由后继消息替代。
    /// 参数说明：tp/offset 为当前会话仍持有的失败输入。
    /// 返回：后续首次交付必须与该位置一致，失权或人工定位会撤销要求。
    pub(crate) fn expect_dead_letter(&self, tp: &Tp, offset: i64) {
        self.dlt_refetch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(tp.clone(), offset);
    }

    /// 业务作用：在复制和派发前检查死信重读没有被保留清理或压缩空洞越过。
    /// 参数说明：tp/offset 为 native 实际交付的坐标。
    /// 返回：匹配则解除本次重读门禁并返回 None；不匹配返回原期望坐标，调用方必须保护分区。
    pub(crate) fn accept_dead_letter_position(&self, tp: &Tp, offset: i64) -> Option<i64> {
        let mut expected = self
            .dlt_refetch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let original = *expected.get(tp)?;
        if original == offset {
            expected.remove(tp);
            None
        } else {
            Some(original)
        }
    }

    /// 业务作用：死信前缀确认后定位下一条仍有失败意图的原始输入，保留合法数字空洞。
    /// 参数说明：tp 为来源分区，from 为已确认后的首个可能位置。
    /// 返回：当前会话已登记的最早后继意图坐标；不存在时为 None。
    pub(crate) fn next_dead_letter(&self, tp: &Tp, from: i64) -> Option<i64> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        records
            .range((tp.clone(), from)..)
            .take_while(|((partition, _), _)| partition == tp)
            .find_map(|((_, offset), permit)| {
                permit
                    .dlt_intent
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some()
                    .then_some(*offset)
            })
    }

    /// 业务作用：把不可重新执行业务的死信意图绑定到现有输入凭证，等待期间不复制原文。
    /// 参数说明：tp/offset 为本会话原坐标，reason 为失败原因，attempts 为可信投递次数。
    /// 返回：输入存在时冻结首次意图；不存在时为 false，调用方不得构造脱离责任账本的死信。
    pub(crate) fn remember_dead_letter(
        &self,
        tp: &Tp,
        offset: i64,
        reason: &str,
        attempts: u32,
    ) -> bool {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(permit) = records.get(&(tp.clone(), offset)) else {
            return false;
        };
        let mut intent = permit
            .dlt_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if intent.is_none() {
            let mut end = reason.len().min(1024);
            while !reason.is_char_boundary(end) {
                end -= 1;
            }
            *intent = Some(DeadLetterIntent {
                reason: reason[..end].into(),
                attempts: attempts.max(1),
            });
        }
        true
    }

    /// 业务作用：读取当前输入的死信意图和原文字节，用于不执行业务的容量重读。
    /// 参数说明：tp/offset 为重新观察到的原坐标。
    /// 返回：仅已登记意图的输入返回有界副本及原文计量；失权或提交裁剪后为 None。
    pub(crate) fn dead_letter_intent(
        &self,
        tp: &Tp,
        offset: i64,
    ) -> Option<(DeadLetterIntent, usize)> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let permit = records.get(&(tp.clone(), offset))?;
        let intent = permit
            .dlt_intent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        Some((intent, permit.bytes / INPUT_COPY_RESERVATIONS))
    }

    /// 业务作用：识别所有输入层级中不可容纳的单条原文，避免永久反压。
    /// 参数说明：无。
    /// 返回：进程、源、组、分区与 key 的最小预留边界除以副本系数后的原文字节上限。
    pub(crate) fn maximum_record_bytes(&self) -> usize {
        self.capacity
            .max_bytes
            .min(self.capacity.max_partition_bytes)
            .min(self.capacity.max_key_bytes)
            .min(self.capacity.source.maximum_record_bytes())
            / INPUT_COPY_RESERVATIONS
    }

    /// 业务作用：将一个 owner 的输入身份绑定到全组共享池，保留独立的恢复账本。
    /// 参数说明：capacity 为本组所有 owner 共用的上限。
    /// 返回：空账本，不预占闲置额度。
    pub(crate) fn new(capacity: Arc<GroupCapacity>) -> Self {
        Self {
            capacity,
            closing: std::sync::atomic::AtomicBool::new(false),
            released_unverified: std::sync::atomic::AtomicU64::new(0),
            records: Mutex::new(BTreeMap::new()),
            blocked: Mutex::new(BTreeMap::new()),
            dlt_refetch: Mutex::new(BTreeMap::new()),
            usage: Arc::new(Mutex::new((0, 0, 0, 0))),
        }
    }

    /// 业务作用：在消息进入拥有型批次或借用业务前登记从取入到提交的完整责任。
    /// 参数说明：tp/offset 为本会话内真实坐标；bytes 为包含必要副本的保守预留，raw_key 为不复制的原始键。
    /// 返回：已有重试复用同一凭证；新输入容量不足时登记暂停触发点并返回 None。
    pub(crate) fn retain(
        &self,
        tp: &Tp,
        offset: i64,
        bytes: usize,
        raw_key: Option<&[u8]>,
    ) -> Option<Arc<RecordPermit>> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = (tp.clone(), offset);
        if let Some(permit) = records.get(&identity) {
            return Some(Arc::clone(permit));
        }
        let key = InputKey::new(raw_key);
        let Some(permit) = self.capacity.reserve(tp, key, bytes, &self.usage) else {
            self.blocked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(tp.clone(), (offset, key, bytes));
            return None;
        };
        records.insert(identity, Arc::clone(&permit));
        Some(permit)
    }

    /// 业务作用：估计当前逻辑批是否还适合继续取入同类消息，优先让已计费输入释放责任。
    /// 参数说明：tp、bytes、raw_key 描述刚刚接纳的输入，仅作为后续取入成本的提示。
    /// 返回：共享额度此刻是否能容纳同类输入；不预留额度、不暂停分区，实际下一条仍须原子准入。
    pub(crate) fn may_accept_similar(&self, tp: &Tp, bytes: usize, raw_key: Option<&[u8]>) -> bool {
        self.capacity.can_reserve(tp, InputKey::new(raw_key), bytes)
    }

    /// 业务作用：读取本 owner 仍持有的实际输入责任，失权后的存活批也继续计费。
    /// 参数说明：无。
    /// 返回：同一计费快照中的记录数与副本预留字节；不包含 native 预取及解码对象堆内存。
    pub(crate) fn usage(&self) -> (usize, usize) {
        let value = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (value.0, value.1)
    }
    /// 业务作用：观测已识别为按键路线的实际输入责任，包含运行、重试和等待提交的原文。
    /// 参数说明：无。返回：记录数和同一输入凭证计费字节；最后引用释放才归零。
    pub(crate) fn key_usage(&self) -> (usize, usize) {
        let value = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (value.2, value.3)
    }

    /// 业务作用：从任一 owner 读取全组同一时刻的实际责任，供组级仪表使用。
    /// 参数说明：无。
    /// 返回：全组计费快照，包含其它 owner 及失权后仍存活的输入批。
    pub(crate) fn group_usage(&self) -> (usize, usize) {
        self.capacity.usage()
    }

    /// 业务作用：只按 broker 已确认的连续前沿裁剪原文责任，提交未知不能提前释放。
    /// 参数说明：progress 为本会话的权威提交账本。
    /// 返回：已确认覆盖的记录归还本账本持有份额；仍被输入批持有时保留实际计费。
    pub(crate) fn prune_committed(&self, progress: &GroupOffsetState) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(tp, offset), _| {
                progress
                    .confirmed_offset(tp)
                    .is_none_or(|next| *offset >= next)
            });
    }

    /// 业务作用：逐分区判断共享额度能否解除取入门禁，不让单个大记录阻挡其它分区。
    /// 参数说明：无。
    /// 返回：触发记录现在有额度的分区及原重读坐标，并清除对应等待点；取入时仍须原子复验。
    pub(crate) fn resumable(&self) -> BTreeMap<Tp, i64> {
        let mut blocked = self
            .blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ready = blocked
            .iter()
            .filter(|(tp, (_, key, bytes))| self.capacity.can_reserve(tp, *key, *bytes))
            .map(|(tp, (offset, _, _))| (tp.clone(), *offset))
            .collect::<BTreeMap<_, _>>();
        for tp in ready.keys() {
            blocked.remove(tp);
        }
        ready
    }

    /// 业务作用：回退到已计费输入时解除新输入等待，保证满额状态下仍可重试原失败前缀。
    /// 参数说明：offsets 为已经成功定位的重新交付坐标。
    /// 返回：可撤销输入容量暂停的分区与已定位坐标；没有原凭证的新记录仍须等待共享额度。
    pub(crate) fn replaying(&self, offsets: &BTreeMap<Tp, i64>) -> BTreeMap<Tp, i64> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut blocked = self
            .blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        offsets
            .iter()
            .filter_map(|(tp, offset)| {
                if records.contains_key(&(tp.clone(), *offset)) && blocked.remove(tp).is_some() {
                    Some((tp.clone(), *offset))
                } else {
                    None
                }
            })
            .collect()
    }

    /// 业务作用：失权、人工定位或实际会话退出后交回已无本地提交权威的输入。
    /// 参数说明：targets 为需要交回的分区；None 表示全部会话责任。
    /// 返回：只释放账本份额；调用方必须先等待实际业务退出，仍存活输入批继续持有计费凭证。
    pub(crate) fn clear(&self, targets: Option<&[Tp]>) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = records.len();
        records.retain(|(tp, _), _| targets.is_some_and(|targets| !targets.contains(tp)));
        if self.closing.load(std::sync::atomic::Ordering::Acquire) {
            let removed = (before - records.len()) as u64;
            // 失权清表不是提交确认；只保存数量，不用保留范围推断 compact 后的具体记录仍存在。
            let _ = self.released_unverified.fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |value| Some(value.saturating_add(removed)),
            );
        }
        drop(records);
        let mut blocked = self
            .blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        blocked.retain(|tp, _| targets.is_some_and(|targets| !targets.contains(tp)));
        self.dlt_refetch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|tp, _| targets.is_some_and(|targets| !targets.contains(tp)));
    }
}
