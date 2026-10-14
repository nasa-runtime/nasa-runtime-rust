//! 带读取隔离和逐字段缺失原因的只读位点证据。

use crate::Tp;
use std::time::SystemTime;

/// 水位与时间定位请求的明确读取隔离级别。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetIsolation {
    /// 普通日志可读末尾，可能包含未完成事务之后的位置。
    ReadUncommitted,
    /// 已提交读取末尾为 LSO，未完成事务限制当前可读范围。
    ReadCommitted,
}

/// 没有可信位置的稳定原因，不以零 offset 代替失败。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetMissing {
    /// 调用方没有请求该维度。
    NotRequested,
    /// broker 明确没有该组分区的提交历史。
    NoCommittedOffset,
    /// broker 明确没有符合时间条件的记录。
    NoTimestampMatch,
    /// broker 元数据明确报告目标 topic 或分区不存在。
    ResourceMissing,
    /// 当前身份没有查询所需权限。
    Authorization,
    /// 共同期限耗尽或原生请求明确超时。
    Timeout,
    /// 查询执行方被中断；取消 Rust future 本身不会交付报告。
    Interrupted,
    /// 本轮连接、响应缺项或其它原因使位置不可观测。
    Unavailable,
    /// 边界证据相互矛盾，不能据此计算恢复位置。
    Inconsistent,
}

/// 一个位置或缺失结论，观测时刻是本地取得证据的时间。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedOffset {
    /// 非负位置或距离，有效零原样保留。
    pub value: Option<i64>,
    /// 无有效值时的原因，与 value 恰有一个存在。
    pub missing: Option<OffsetMissing>,
    /// 本地取得本项结果的时刻，不是 broker 提交时刻。
    pub observed_at: SystemTime,
}

impl ObservedOffset {
    /// 业务作用：封存原生查询确认的非负位置。
    /// 参数说明：value 为已校验的实际 offset 或距离。
    /// 返回：有值且没有缺失原因的时点证据。
    pub(crate) fn present(value: i64) -> Self {
        Self {
            value: Some(value),
            missing: None,
            observed_at: SystemTime::now(),
        }
    }

    /// 业务作用：保留本项证据缺席的原因，不补造默认位置。
    /// 参数说明：reason 为确定缺失或暂不可观测的类别。
    /// 返回：没有位置且带原因的时点证据。
    pub(crate) fn absent(reason: OffsetMissing) -> Self {
        Self {
            value: None,
            missing: Some(reason),
            observed_at: SystemTime::now(),
        }
    }
}

/// 一个请求分区的完整位置维度；不同维度不属于 broker 原子快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionOffsets {
    /// 原请求目标。
    pub tp: Tp,
    /// 当前保留起点。
    pub beginning: ObservedOffset,
    /// 所选隔离级别下的当前末尾。
    pub end: ObservedOffset,
    /// 消费组确认提交的 next offset。
    pub committed: ObservedOffset,
    /// 时间条件对应的首个位置，未匹配与查询失败分别报告。
    pub timestamp_offset: ObservedOffset,
    /// max(0, end - committed)，是位置距离，不是业务记录数量。
    pub lag: ObservedOffset,
}

/// 一次有共同期限的源级只读位置观测。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OffsetObservation {
    /// 源的配置身份，不以 bootstrap 地址代替。
    pub source_id: String,
    /// 可选消费组；None 表示不请求提交与 lag。
    pub group: Option<String>,
    /// 末尾和时间定位使用的隔离级别。
    pub isolation: OffsetIsolation,
    /// 开始观测的本地时间。
    pub started_at: SystemTime,
    /// 返回报告封存的本地时间。
    pub completed_at: SystemTime,
    /// 按 topic 和分区排序的完整请求结果，晚到结果不改写本报告。
    pub partitions: Vec<PartitionOffsets>,
}
