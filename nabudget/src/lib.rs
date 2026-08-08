//! Provider-neutral 请求/调用链预算。
//!
//! 预算使用 Tokio 单调时钟表达绝对 deadline，并携带显式取消信号。Web、REST、DB/Redis adapter
//! 共享此类型，避免每层重新开始一个独立 timeout，导致调用链总耗时突破入口 SLA。

#![forbid(unsafe_code)]

use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// 防止异常配置把单调时钟加法推到平台表示范围之外；业务请求不应持有跨年预算。
const MAX_BUDGET: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 入站到全部下游工作的绝对 deadline 与取消树。
#[derive(Clone)]
pub struct RequestBudget {
    deadline: Instant,
    cancel: CancellationToken,
}

impl RequestBudget {
    /// 业务作用：从当前单调时刻起创建预算。
    pub fn from_now(total: Duration) -> Self {
        Self {
            deadline: Instant::now() + total.min(MAX_BUDGET),
            cancel: CancellationToken::new(),
        }
    }

    /// 业务作用：从已有绝对 deadline 创建预算，便于跨 adapter 保持同一到期时刻。
    pub fn until(deadline: Instant) -> Self {
        Self {
            deadline,
            cancel: CancellationToken::new(),
        }
    }

    /// 业务作用：当前剩余预算；过期后饱和为零。
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// 业务作用：将单次 operation 上限收敛到剩余总预算；耗尽时返回 `None`。
    pub fn operation_timeout(&self, maximum: Duration) -> Option<Duration> {
        let remaining = self.remaining();
        (!remaining.is_zero()).then(|| remaining.min(maximum))
    }

    /// 业务作用：是否已经耗尽。
    pub fn is_exhausted(&self) -> bool {
        self.remaining().is_zero()
    }

    /// 业务作用：绝对到期时刻。
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// 业务作用：派生不超过父 deadline 的子预算，并继承父取消信号。
    pub fn child(&self, maximum: Duration) -> Self {
        Self {
            deadline: self.deadline.min(Instant::now() + maximum.min(MAX_BUDGET)),
            cancel: self.cancel.child_token(),
        }
    }

    /// 业务作用：父/当前预算被显式取消时完成。
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }

    /// 业务作用：取消当前预算及全部子预算。
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl std::fmt::Debug for RequestBudget {
    /// 业务作用：仅展示剩余预算与取消状态，不暴露内部 token 或 deadline 表示。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestBudget")
            .field("remaining", &self.remaining())
            .field("cancelled", &self.cancel.is_cancelled())
            .finish()
    }
}
