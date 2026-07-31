//! 加减 / 截断 / 区间 / 当下。均以 i64 epoch 毫秒为输入输出。

use crate::{
    format, gmt8, now_ms, parse, to_datetime, DateError, Result, DAY_MS, F_YM, F_Y_M_D,
    F_Y_M_D_H_M_S,
};
use chrono::Months;

// ==================== 加减(毫秒域 checked)====================

/// 按倍率累加时间毫秒值；用于统一处理天、小时等时间单位换算。
///
/// # 参数
/// - `ms`: 毫秒时间值。
/// - `n`: 长度、数量或循环次数。
/// - `factor`: 日期位移或周期计算的倍率。
fn add_scaled(ms: i64, n: i64, factor: i64) -> Result<i64> {
    n.checked_mul(factor)
        .and_then(|d| ms.checked_add(d))
        .ok_or(DateError::Overflow)
}

/// 加毫秒(可负)。对照 原实现 `addMillis`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的毫秒数；负数表示向过去移动。
pub fn add_millis(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 1)
}

/// 加秒(可负)。对照 原实现 `addSeconds`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的秒数；负数表示向过去移动。
pub fn add_seconds(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 1000)
}

/// 加分钟(可负)。对照 原实现 `addMinutes`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的分钟数；负数表示向过去移动。
pub fn add_minutes(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 60 * 1000)
}

/// 加小时(可负)。对照 原实现 `addHours`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的小时数；负数表示向过去移动。
pub fn add_hours(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 60 * 60 * 1000)
}

/// 加天(可负)。对照 原实现 `addDays`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的天数；负数表示向过去移动。
pub fn add_days(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, DAY_MS)
}

/// 加周(可负)。对照 原实现 `addWeeks`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的周数；负数表示向过去移动。
pub fn add_weeks(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 7 * DAY_MS)
}

/// 加月(可负),**GMT+8 日历**,月末按 chrono 规则收缩(如 1/31 + 1 月 = 2/28,对齐 原实现 `Calendar`)。对照 原实现 `addMonths`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳，会按 GMT+8 日历解释年月日。
/// - `n`: 要增加的自然月数；负数表示向过去移动。
pub fn add_months(ms: i64, n: i32) -> Result<i64> {
    let dt = to_datetime(gmt8(), ms)?;
    let shifted = if n >= 0 {
        dt.checked_add_months(Months::new(n as u32))
    } else {
        dt.checked_sub_months(Months::new(n.unsigned_abs()))
    }
    .ok_or(DateError::Overflow)?;
    Ok(shifted.timestamp_millis())
}

/// 加年(可负),**GMT+8 日历**(= 加 `n×12` 月,2/29 + 1 年 = 2/28)。对照 原实现 `addYears`。
///
/// # 参数
///
/// - `ms`: 原始 epoch 毫秒时间戳，会按 GMT+8 日历解释年月日。
/// - `n`: 要增加的自然年数；负数表示向过去移动。
pub fn add_years(ms: i64, n: i32) -> Result<i64> {
    let months = n.checked_mul(12).ok_or(DateError::Overflow)?;
    add_months(ms, months)
}

// ==================== 步长单位(用于 all_time)====================

/// 时间步长单位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// 毫秒步长。
    Millis,
    /// 秒步长。
    Seconds,
    /// 分钟步长。
    Minutes,
    /// 小时步长。
    Hours,
    /// 自然日步长。
    Days,
    /// 自然周步长。
    Weeks,
    /// 自然月步长，按 GMT+8 日历推进。
    Months,
    /// 自然年步长，按 GMT+8 日历推进。
    Years,
}

/// 按指定时间单位推进毫秒时间戳；用于日期区间枚举的核心位移。
///
/// # 参数
/// - `ms`: 当前 epoch 毫秒时间戳。
/// - `unit`: 每一步推进的时间单位。
/// - `n`: 推进数量；通常为 `1`，负数表示向过去移动。
fn step(ms: i64, unit: Unit, n: i64) -> Result<i64> {
    match unit {
        Unit::Millis => add_millis(ms, n),
        Unit::Seconds => add_seconds(ms, n),
        Unit::Minutes => add_minutes(ms, n),
        Unit::Hours => add_hours(ms, n),
        Unit::Days => add_days(ms, n),
        Unit::Weeks => add_weeks(ms, n),
        Unit::Months => add_months(ms, i32::try_from(n).map_err(|_| DateError::Overflow)?),
        Unit::Years => add_years(ms, i32::try_from(n).map_err(|_| DateError::Overflow)?),
    }
}

// ==================== 截断 / 区间 ====================

/// 取 `ms` 在 `原实现_pattern` 精度下的**最早时刻**(GMT+8)。
/// = `parse(format(ms, pat), pat)`:格式化丢弃低于精度的位,再解析回。对照 原实现 `earliest`。
///
/// 例:`earliest(t, "yyyy-MM-dd")` → 当天 00:00:00(GMT+8)。
///
/// # 参数
///
/// - `ms`: 需要截断到指定精度的 epoch 毫秒时间戳。
/// - `pattern`: 决定截断粒度的日期格式，例如 `yyyy-MM-dd`。
pub fn earliest(ms: i64, pattern: &str) -> Result<i64> {
    let s = format(ms, pattern)?;
    parse(&s, pattern)
}

/// `[start, end)` 区间内、按 `precision_pattern` 精度对齐、以 `unit` 步进的所有时刻,用 `return_pattern` 格式化。
/// 对照 原实现 `allTime`(do-while 逐值复刻):`start > end` 返空;否则**先产出对齐后的起点、
/// 再步进判断 `cur >= end` 终止**——因此 `start == end` 时返回含起点的 1 个元素(非严格右开区间)。
///
/// # 参数
///
/// - `start`: 起点 epoch 毫秒(对齐 `precision_pattern` 后必然产出)。
/// - `end`: 终点 epoch 毫秒(起点之后的步进值 `>= end` 即停,不再产出)。
/// - `precision_pattern`: 起点对齐时使用的日期精度格式。
/// - `return_pattern`: 每个结果时间点输出时使用的格式。
/// - `unit`: 区间枚举时每一步推进的时间单位。
pub fn all_time(
    start: i64,
    end: i64,
    precision_pattern: &str,
    return_pattern: &str,
    unit: Unit,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if start > end {
        return Ok(out);
    }
    let mut cur = earliest(start, precision_pattern)?;
    loop {
        out.push(format(cur, return_pattern)?);
        cur = step(cur, unit, 1)?;
        if cur >= end {
            break;
        }
    }
    Ok(out)
}

/// `[start, end)` 内逐日的字符串(按 `yyyy-MM-dd` 精度对齐,步进 1 天)。
///
/// # 参数
///
/// - `start`: 左闭区间起点 epoch 毫秒。
/// - `end`: 右开区间终点 epoch 毫秒。
/// - `return_pattern`: 每个自然日输出时使用的格式。
pub fn all_days(start: i64, end: i64, return_pattern: &str) -> Result<Vec<String>> {
    all_time(start, end, F_Y_M_D, return_pattern, Unit::Days)
}

/// `[start, end)` 内逐月的字符串(按 `yyyyMM` 精度对齐,步进 1 月)。对照 原实现 `allMonth`。
///
/// # 参数
///
/// - `start`: 左闭区间起点 epoch 毫秒。
/// - `end`: 右开区间终点 epoch 毫秒。
/// - `return_pattern`: 每个自然月输出时使用的格式。
pub fn all_months(start: i64, end: i64, return_pattern: &str) -> Result<Vec<String>> {
    all_time(start, end, F_YM, return_pattern, Unit::Months)
}

// ==================== 当下 ====================

/// 当前时刻按默认格式 `yyyy-MM-dd HH:mm:ss`(GMT+8)。对照 原实现 `now`。
pub fn now() -> Result<String> {
    format(now_ms(), F_Y_M_D_H_M_S)
}

/// 今天(`yyyy-MM-dd`,GMT+8)。对照 原实现 `today()`。
pub fn today() -> Result<String> {
    format(now_ms(), F_Y_M_D)
}

/// 今天按指定 pattern(GMT+8)。对照 原实现 `today(f)`。
///
/// # 参数
///
/// - `pattern`: 今天日期输出时使用的格式。
pub fn today_fmt(pattern: &str) -> Result<String> {
    format(now_ms(), pattern)
}

/// 昨天(`yyyy-MM-dd`,GMT+8)。对照 原实现 `yesterday()`。
pub fn yesterday() -> Result<String> {
    format(add_days(now_ms(), -1)?, F_Y_M_D)
}

/// 昨天按指定 pattern(GMT+8)。对照 原实现 `yesterday(f)`。
///
/// # 参数
///
/// - `pattern`: 昨天日期输出时使用的格式。
pub fn yesterday_fmt(pattern: &str) -> Result<String> {
    format(add_days(now_ms(), -1)?, pattern)
}
