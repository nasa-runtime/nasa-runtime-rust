//! Provider-neutral 请求/调用链预算。
//!
//! 预算使用 Tokio 单调时钟表达绝对 deadline，并携带显式取消信号。
//! adapter 显式传播同一预算；停止本地等待不能证明已经发出的远端操作未执行。

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

/// 本地调用停止等待的原因，不描述远端是否已经产生副作用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetError {
    /// 显式取消已到达当前调用。
    Cancelled,
    /// 绝对截止点已经到达。
    DeadlineExceeded,
}

impl std::fmt::Display for BudgetError {
    /// 业务作用：提供不包含请求内容的固定失败原因。
    /// 参数说明：`formatter` 为错误输出目标。
    /// 返回：成功输出原因或传播格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Cancelled => "request budget cancelled",
            Self::DeadlineExceeded => "request deadline exceeded",
        })
    }
}

impl std::error::Error for BudgetError {}

/// 请求或响应体 owner 退出时取消其预算与子预算。
pub struct BudgetGuard(RequestBudget);

impl Drop for BudgetGuard {
    /// 业务作用：请求被取消、响应结束或 owner 被丢弃时通知相关下游停止等待。
    /// 参数说明：无。
    /// 返回：发出取消信号，不等待远端工作退出。
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl RequestBudget {
    /// 业务作用：从当前单调时刻起创建预算。
    /// 参数说明：`total` 为总时长，最大一年。
    /// 返回：具有独立取消根的预算。
    pub fn from_now(total: Duration) -> Self {
        Self {
            deadline: Instant::now() + total.min(MAX_BUDGET),
            cancel: CancellationToken::new(),
        }
    }

    /// 业务作用：从已有绝对 deadline 创建预算，便于跨 adapter 保持同一到期时刻。
    /// 参数说明：`deadline` 为有效单调截止点。
    /// 返回：保留截止点的新预算，不推断上游的取消信号。
    pub fn until(deadline: Instant) -> Self {
        Self {
            deadline,
            cancel: CancellationToken::new(),
        }
    }

    /// 业务作用：当前剩余预算；过期后饱和为零。
    /// 参数说明：无。
    /// 返回：距截止点的时间；显式取消由 `check` 单独判定。
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// 业务作用：将单次 operation 上限收敛到剩余总预算；耗尽时返回 `None`。
    /// 参数说明：`maximum` 为本次操作的最大时长。
    /// 返回：仍允许调用时的非零上限；已取消、到期或上限为零时返回 `None`。
    pub fn operation_timeout(&self, maximum: Duration) -> Option<Duration> {
        let remaining = self.remaining();
        (!self.is_cancelled() && !remaining.is_zero() && !maximum.is_zero())
            .then(|| remaining.min(maximum))
    }

    /// 业务作用：判定预算是否已不允许开始新的调用。
    /// 参数说明：无。
    /// 返回：已取消或绝对时间耗尽时为真。
    pub fn is_exhausted(&self) -> bool {
        self.is_cancelled() || self.remaining().is_zero()
    }

    /// 业务作用：读取显式取消状态，与截止点到期分开分类。
    /// 参数说明：无。
    /// 返回：当前预算或父预算已发出取消信号时为真。
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// 业务作用：在调用开始前拒绝已经终止的预算。
    /// 参数说明：无。
    /// 返回：可以开始等待时成功；取消优先于到期报告。
    pub fn check(&self) -> Result<(), BudgetError> {
        if self.is_cancelled() {
            return Err(BudgetError::Cancelled);
        }
        if self.remaining().is_zero() {
            return Err(BudgetError::DeadlineExceeded);
        }
        Ok(())
    }

    /// 业务作用：使当前调用的等待同时服从绝对截止点和取消信号。
    /// 参数说明：`operation` 为尚未轮询的异步调用；调用方负责远端副作用的不确定结果语义。
    /// 返回：正常完成时保留调用结果；预算终止时丢弃调用 future，不承诺撤销远端操作。
    pub async fn run<F: std::future::Future>(
        &self,
        operation: F,
    ) -> Result<F::Output, BudgetError> {
        self.check()?;
        tokio::select! {
            biased;
            _ = self.cancelled() => Err(BudgetError::Cancelled),
            _ = tokio::time::sleep_until(self.deadline) => Err(BudgetError::DeadlineExceeded),
            value = operation => Ok(value),
        }
    }

    /// 业务作用：把预算取消责任移交给请求或响应体的生命周期 owner。
    /// 参数说明：无。
    /// 返回：丢弃即取消预算的守卫；创建守卫本身不改变预算状态。
    pub fn cancel_on_drop(&self) -> BudgetGuard {
        BudgetGuard(self.clone())
    }

    /// 业务作用：绝对到期时刻。
    /// 参数说明：无。
    /// 返回：创建或派生时确定的单调截止点。
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// 业务作用：派生不超过父 deadline 的子预算，并继承父取消信号。
    /// 参数说明：`maximum` 为子调用允许的最大时间。
    /// 返回：取消不反向影响父预算的子预算。
    pub fn child(&self, maximum: Duration) -> Self {
        Self {
            deadline: self.deadline.min(Instant::now() + maximum.min(MAX_BUDGET)),
            cancel: self.cancel.child_token(),
        }
    }

    /// 业务作用：父/当前预算被显式取消时完成。
    /// 参数说明：无。
    /// 返回：显式取消后完成；仅等待此方法不监听截止点。
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }

    /// 业务作用：取消当前预算及全部子预算。
    /// 参数说明：无。
    /// 返回：取消信号立即可见，不证明下游已退出。
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl std::fmt::Debug for RequestBudget {
    /// 业务作用：仅展示剩余预算与取消状态，不暴露内部 token 或 deadline 表示。
    /// 参数说明：`formatter` 为输出目标。
    /// 返回：成功输出预算摘要或传播格式化错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestBudget")
            .field("remaining", &self.remaining())
            .field("cancelled", &self.cancel.is_cancelled())
            .finish()
    }
}
