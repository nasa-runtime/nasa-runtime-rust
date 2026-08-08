//! 10s 滚动窗口:按秒分桶的结局计数 + 桶级并发峰值。
//!
//! 使用 1s 滑动桶。桶内额外记录 `max_inflight`，让滚动并发峰值随窗口回落；进程终身峰值
//! 单独导出 gauge。延迟只写入可跨实例聚合的 Prometheus histogram，不在进程内缓存样本。
//!
//! 本窗口服务滚动并发峰值、`current_tps()` 和周期请求汇总日志。Prometheus 的单调 counter 在
//! counters.rs，与本窗口同源记录、各管一轨，互不重算(合同 计数口径)。

use std::collections::VecDeque;
use std::time::Instant;

/// 滚动窗口长度(秒)。
pub(crate) const WINDOW_SECS: u64 = 10;

/// 一次执行的结局,决定滚动桶与单调 counter 各自加到哪一格。
#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    /// 下游正常返回且非 5xx；4xx 业务响应不计失败。
    Success,
    /// 下游返回 5xx。
    Failure,
    /// tokio timeout 触发。
    Timeout,
    /// bulkhead 满被拒(未进入执行区,不进入延迟 histogram)。
    Rejected,
    /// 执行 future 在产生正常结局前被取消或中止，不进入延迟 histogram。
    Canceled,
}

/// 滚动窗口中的一个 1 秒统计桶。
#[derive(Default, Clone, Copy)]
struct Bucket {
    /// 桶代表的"第几秒"(相对窗口起点的单调秒数,作 key)。
    sec: u64,
    /// 这一秒内的成功次数。
    success: u64,
    /// 这一秒内的失败(5xx)次数。
    failure: u64,
    /// 这一秒内的超时次数。
    timeout: u64,
    /// 这一秒内被 bulkhead 拒绝的次数。
    rejected: u64,
    /// 这一秒内执行被取消或中止的次数。
    canceled: u64,
    /// 这一秒内产出降级响应的次数(拒绝/超时分支;fn/静态串/默认壳都算一次)。
    fallback: u64,
    /// 这一秒内观察到的最大并发(滚动峰值的数据源,进入执行区时采样)。
    max_inflight: u64,
}

/// 命令的滚动统计窗口：计数桶只保留最近 [`WINDOW_SECS`] 秒。
pub(crate) struct RollingWindow {
    /// 窗口起点(单调时钟),桶 key 相对它计秒,不受墙钟回拨影响。
    start: Instant,
    /// 按秒滑动的计数桶,队首最旧、队尾最新。
    buckets: VecDeque<Bucket>,
}

impl RollingWindow {
    /// 业务作用：构造空窗口,起点锚定当前时刻。
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            buckets: VecDeque::new(),
        }
    }

    /// 业务作用：返回当前时间相对窗口起点的第几秒(桶 key)。
    fn now_sec(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    /// 业务作用：丢弃十秒滚动窗口之外的旧桶。
    ///
    /// # 参数
    /// - `now`: 当前秒。
    fn evict(&mut self, now: u64) {
        let window_floor = now.saturating_sub(WINDOW_SECS - 1);
        while self
            .buckets
            .front()
            .map(|bucket| bucket.sec < window_floor)
            .unwrap_or(false)
        {
            self.buckets.pop_front();
        }
    }

    /// 业务作用：取/建当前秒的桶(队尾不是当前秒就补一个空桶),返回可变引用。
    ///
    /// # 参数
    /// - `now`: 当前秒。
    fn bucket_mut(&mut self, now: u64) -> &mut Bucket {
        if self.buckets.back().map(|b| b.sec) != Some(now) {
            self.buckets.push_back(Bucket {
                sec: now,
                ..Default::default()
            });
        }
        self.buckets.back_mut().unwrap()
    }

    /// 业务作用：记录一次执行结局。
    ///
    /// # 参数
    /// - `outcome`: 本次结局(成功/失败/超时/被拒/被取消)。
    pub(crate) fn record(&mut self, outcome: Outcome) {
        let now = self.now_sec();
        self.evict(now);
        let b = self.bucket_mut(now);
        match outcome {
            Outcome::Success => b.success += 1,
            Outcome::Failure => b.failure += 1,
            Outcome::Timeout => b.timeout += 1,
            Outcome::Rejected => b.rejected += 1,
            Outcome::Canceled => b.canceled += 1,
        }
    }

    /// 业务作用：记一次"产出了降级响应"。在拒绝/超时分支记完主结局后、同一把锁内连续调用。
    pub(crate) fn record_fallback(&mut self) {
        let now = self.now_sec();
        self.evict(now);
        self.bucket_mut(now).fallback += 1;
    }

    /// 业务作用：进入执行区时采样当前并发,抬高当前秒桶的 `max_inflight`(滚动峰值数据源)。
    ///
    /// # 参数
    /// - `current`: 本次进入后的并发数(gauge +1 之后的值)。
    pub(crate) fn observe_inflight(&mut self, current: u64) {
        let now = self.now_sec();
        self.evict(now);
        let b = self.bucket_mut(now);
        if current > b.max_inflight {
            b.max_inflight = current;
        }
    }

    /// 业务作用：汇总当前滚动窗口:先驱逐过期数据,再累加计数与滚动并发峰值。
    pub(crate) fn snapshot(&mut self) -> WindowSum {
        let now = self.now_sec();
        self.evict(now);
        let window_floor = now.saturating_sub(WINDOW_SECS - 1);
        let mut sum = WindowSum::default();
        for b in &self.buckets {
            if b.sec < window_floor {
                continue;
            }
            sum.success += b.success;
            sum.failure += b.failure;
            sum.timeout += b.timeout;
            sum.rejected += b.rejected;
            sum.canceled += b.canceled;
            sum.fallback += b.fallback;
            if b.max_inflight > sum.rolling_max_inflight {
                sum.rolling_max_inflight = b.max_inflight;
            }
        }
        sum
    }

    /// 业务作用：返回当前十秒窗口请求总数。
    pub(crate) fn request_count(&mut self) -> u64 {
        let now = self.now_sec();
        self.evict(now);
        let window_floor = now.saturating_sub(WINDOW_SECS - 1);
        self.buckets
            .iter()
            .filter(|bucket| bucket.sec >= window_floor)
            .map(|bucket| {
                bucket.success + bucket.failure + bucket.timeout + bucket.rejected + bucket.canceled
            })
            .sum()
    }
}

/// 一次滚动窗口汇总的产物，提供给 `/metrics` 实时快照与周期请求日志。
#[derive(Default)]
pub(crate) struct WindowSum {
    /// 窗口内成功总数。
    pub(crate) success: u64,
    /// 窗口内失败(5xx)总数。
    pub(crate) failure: u64,
    /// 窗口内超时总数。
    pub(crate) timeout: u64,
    /// 窗口内被 bulkhead 拒绝总数。
    pub(crate) rejected: u64,
    /// 窗口内执行被取消或中止总数。
    pub(crate) canceled: u64,
    /// 窗口内产出降级响应总数。
    pub(crate) fallback: u64,
    /// 窗口内并发峰值(随窗口回落的滚动口径)。
    pub(crate) rolling_max_inflight: u64,
}
