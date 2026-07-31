//! 双时钟抽象:单调时钟与墙钟分离，调用方可按运行环境注入实现。
//!
//! - [`MonotonicClock`]:用于 **deadline、TTL、stale、退避、耗时**。永不回拨,只在同一时钟内做差
//!   有意义;不映射到墙上时间,也**不得**作为分布式 lease/fence 的权威时间。
//! - [`UtcClock`]:用于 **协议时间窗(JWT `exp`/`iat`)、审计时间戳**。可被系统回拨,分布式 fencing
//!   不能相信它。
//!
//! 默认 [`SystemClock`] 使用操作系统时间；需要虚拟时间、回放或其他时钟来源的业务可自行实现上述
//! 两个 trait，并以 `Arc<dyn _>` 传给相应组件。正式 crate 不携带可拨动时钟实现。

use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime};

/// 单调时刻:相对**同一时钟族**私有基准的偏移。
///
/// 只在同一时钟产生的两个实例之间做差才有意义(跨时钟比较无定义,见模块文档)。刻意不包裹
/// [`std::time::Instant`],使非系统时钟实现也能构造同一抽象下的时刻。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonotonicInstant {
    since_base: Duration,
}

impl MonotonicInstant {
    /// 用相对时钟基准的偏移构造。时钟实现用它产生合成时刻。
    pub const fn from_since_base(since_base: Duration) -> Self {
        Self { since_base }
    }

    /// 返回相对时钟基准的偏移。
    pub const fn since_base(self) -> Duration {
        self.since_base
    }

    /// `self` 晚于 `earlier` 的时长;`self` 早于 `earlier` 时饱和为 0(单调时钟正常不会出现)。
    pub fn saturating_duration_since(self, earlier: MonotonicInstant) -> Duration {
        self.since_base.saturating_sub(earlier.since_base)
    }

    /// 在本时刻上加一段时长构造 deadline;溢出返回 `None`。
    pub fn checked_add(self, delta: Duration) -> Option<MonotonicInstant> {
        self.since_base
            .checked_add(delta)
            .map(|since_base| Self { since_base })
    }

    /// 在本时刻上加一段时长构造 deadline;溢出饱和到最大值。
    pub fn saturating_add(self, delta: Duration) -> MonotonicInstant {
        Self {
            since_base: self.since_base.saturating_add(delta),
        }
    }
}

/// 单调时钟:deadline、TTL、stale、退避、耗时。永不回拨。
pub trait MonotonicClock: Send + Sync {
    /// 返回当前单调时刻。连续两次调用满足非递减。
    fn now(&self) -> MonotonicInstant;
}

/// 墙钟(UTC):协议时间窗、JWT 时间声明、审计时间。可回拨,不得用于分布式 fencing。
pub trait UtcClock: Send + Sync {
    /// 返回当前墙上时间。可能因系统校时而非单调。
    fn now(&self) -> SystemTime;
}

/// 进程级单调基准:首次取时钟固定,使所有 [`SystemClock`] 实例产生同一族可比较的时刻。
fn system_monotonic_base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

/// 生产用系统时钟:单调走 [`Instant`],墙钟走 [`SystemTime`]。零状态,可自由克隆。
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl SystemClock {
    /// 创建系统时钟句柄。
    pub const fn new() -> Self {
        Self
    }
}

impl MonotonicClock for SystemClock {
    /// 返回相对于进程级单调基准的当前偏移，供 deadline 与耗时计算使用。
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant {
            since_base: system_monotonic_base().elapsed(),
        }
    }
}

impl UtcClock for SystemClock {
    /// 读取操作系统当前墙钟，供协议时间声明与审计时间戳使用。
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}
