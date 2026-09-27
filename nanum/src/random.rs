//! 使用 rand 生成有界随机整数；定点步长由 decimal 模块提供。

use crate::{NumericError, Result};
use rand::Rng;

/// 业务作用：生成包含上下界的随机整数，区间退化时返回唯一值。
/// 返回：区间内整数；下界大于上界时返回 Range 错误。上界为 i32::MAX 仍使用闭区间计算，
/// 不通过加 1 构造可能溢出的半开区间。
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

/// 业务作用: `[0, max]` 闭区间随机整数。
///
/// `max < 0` → `Err(Range)`(经 `next_int(0, max)` 的 `min>max` 判定);`max == i32::MAX` 正常(见 [`next_int`])。
///
/// # 参数
///
/// - `max`: 随机闭区间上界，下界固定为 `0`。
pub fn next_int_max(max: i32) -> Result<i32> {
    next_int(0, max)
}
// `decimal::random_step` 返回 BigDecimal，不受定点八位小数上限限制。
