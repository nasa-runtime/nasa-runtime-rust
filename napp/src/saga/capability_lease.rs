//! capability 收据的本地有效期边界。

use std::time::{Duration, Instant};

/// 业务作用：将已提交租约的绝对期限收窄为本地单调期限，限制响应等待和时钟偏移的影响。
///
/// 参数说明：`accepted_until_ms` 是服务端截止时间，`requested_lease_ms` 是请求预算，
/// `started` 是请求发起时刻，`now_ms` 是当前本地 Unix 毫秒时间。
///
/// 返回：未到期时返回两种预算中更早的期限；过期、零预算或时间溢出时拒绝。
pub(super) fn receipt_deadline(
    accepted_until_ms: i64,
    requested_lease_ms: u64,
    started: Instant,
    now_ms: i64,
) -> Option<Instant> {
    let now = Instant::now();
    let remaining = accepted_until_ms.checked_sub(now_ms)?;
    // 回包成功不能延长服务端租期，也不能补回请求已经消耗的本地预算。
    if remaining <= 0 || requested_lease_ms == 0 {
        return None;
    }
    let remaining = (remaining as u64).min(requested_lease_ms);
    let deadline = now
        .checked_add(Duration::from_millis(remaining))?
        .min(started.checked_add(Duration::from_millis(requested_lease_ms))?);
    (deadline > now).then_some(deadline)
}
