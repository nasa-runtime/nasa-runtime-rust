//! 随机:整数 + 定点步长。用 `rand`(非 原实现 的 `Math.random()` 弱 RNG)。

use crate::{NumericError, Result};
use rand::Rng;

/// `[min, max]` 闭区间随机整数(两端含)。对照 原实现 `Numeric.nextInt(min,max)`。
///
/// `min == max` 返 `min`;`min > max` → `Err(Range)`(对照 原实现 抛 `IllegalArgumentException`)。
///
/// **`min > max` 不再静默返 `min`**——静默会吞掉上游范围/配置 bug,且违反 crate
/// "可失败 API 返 Result"纪律。**`max == i32::MAX` 正常工作**(`gen_range(0..=i32::MAX)` 无溢出),
/// 是对 原实现 `max+1` 溢出 bug 的有意修正。
///
/// # 参数
///
/// - `min`: 随机闭区间下界，包含在结果范围内。
/// - `max`: 随机闭区间上界，包含在结果范围内。
pub fn next_int(min: i32, max: i32) -> Result<i32> {
    if min == max {
        return Ok(min);
    }
    if min > max {
        return Err(NumericError::Range(format!(
            "next_int: min({min}) > max({max})"
        )));
    }
    Ok(rand::thread_rng().gen_range(min..=max))
}

/// `[0, max]` 闭区间随机整数。对照 原实现 `Numeric.nextInt(max)`。
///
/// `max < 0` → `Err(Range)`(经 `next_int(0, max)` 的 `min>max` 判定);`max == i32::MAX` 正常(见 [`next_int`])。
///
/// # 参数
///
/// - `max`: 随机闭区间上界，下界固定为 `0`。
pub fn next_int_max(max: i32) -> Result<i32> {
    next_int(0, max)
}
// 注:`random_step` 已移到 `decimal` 模块(走 BigDecimal,无 scale≤8 上限,对照 原实现 返 BigDecimal)。
