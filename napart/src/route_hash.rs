//! 冻结的进程内路由摘要，供调用方分桶与 Runner 提交共用。

use std::hash::{Hash, Hasher};

/// 不透明的进程内路由值；不写入持久化协议，也不接受 Redis 物理分区哈希。
///
/// 摘要碰撞会保守合并本地顺序域。需要保存业务顺序门禁的接入层应同时使用此值分桶
/// 并通过 [`crate::PartitionRunner::submit_routed_typed`] 提交，避免重复哈希。
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteHash(pub(crate) u64);

impl RouteHash {
    /// 业务作用：在释放业务 key 前冻结与普通提交入口相同的本地路由。
    ///
    /// 参数说明：
    /// - `key`: 以稳定字段实现 `Hash` 的业务键，可以是借用切片。
    ///
    /// 返回：仅用于当前进程内分桶与选择 Runner home slot 的摘要。
    pub fn from_key<K: Hash + ?Sized>(key: &K) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        Self(hasher.finish())
    }

    /// 业务作用：为无业务顺序键的任务选择固定 home，由 relaxed 类型执行热点分发。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：选择 home slot 0 的摘要；顺序策略仍由 `TaskSpec` 显式决定。
    pub const fn unordered() -> Self {
        Self(0)
    }
}
