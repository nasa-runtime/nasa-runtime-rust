//! 日期位移、精度截断、区间枚举和当前日期便捷入口，统一使用 `i64` epoch 毫秒。

use super::{
    format, gmt8, now_ms, parse, to_datetime, DateError, Result, DAY_MS, F_YM, F_Y_M_D,
    F_Y_M_D_H_M_S,
};
use chrono::Months;

// ==================== 受检日期位移 ====================

/// 业务作用: 按倍率累加时间毫秒值；用于统一处理天、小时等时间单位换算。
///
/// 参数说明:
/// - `ms`: 毫秒时间值。
/// - `n`: 长度、数量或循环次数。
/// - `factor`: 日期位移或周期计算的倍率。
///
/// 返回: 乘法和加法都未溢出时返回位移结果，否则返回 [`DateError::Overflow`]。
fn add_scaled(ms: i64, n: i64, factor: i64) -> Result<i64> {
    n.checked_mul(factor)
        .and_then(|d| ms.checked_add(d))
        .ok_or(DateError::Overflow)
}

/// 业务作用: 按毫秒单位位移 epoch 时间戳，负数表示向过去移动。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的毫秒数；负数表示向过去移动。
///
/// 返回: 位移未溢出时返回新时间戳，否则返回 [`DateError::Overflow`]。
pub fn add_millis(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 1)
}

/// 业务作用: 按秒单位位移 epoch 时间戳，负数表示向过去移动。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的秒数；负数表示向过去移动。
///
/// 返回: 单位换算和位移均未溢出时返回新时间戳，否则返回 [`DateError::Overflow`]。
pub fn add_seconds(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 1000)
}

/// 业务作用: 按分钟单位位移 epoch 时间戳，负数表示向过去移动。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的分钟数；负数表示向过去移动。
///
/// 返回: 单位换算和位移均未溢出时返回新时间戳，否则返回 [`DateError::Overflow`]。
pub fn add_minutes(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 60 * 1000)
}

/// 业务作用: 按小时单位位移 epoch 时间戳，负数表示向过去移动。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的小时数；负数表示向过去移动。
///
/// 返回: 单位换算和位移均未溢出时返回新时间戳，否则返回 [`DateError::Overflow`]。
pub fn add_hours(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 60 * 60 * 1000)
}

/// 业务作用: 按固定 24 小时自然日位移 epoch 时间戳，负数表示向过去移动。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的天数；负数表示向过去移动。
///
/// 返回: 单位换算和位移均未溢出时返回新时间戳，否则返回 [`DateError::Overflow`]。
pub fn add_days(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, DAY_MS)
}

/// 业务作用: 按固定七天周期位移 epoch 时间戳，负数表示向过去移动。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳。
/// - `n`: 要增加的周数；负数表示向过去移动。
///
/// 返回: 单位换算和位移均未溢出时返回新时间戳，否则返回 [`DateError::Overflow`]。
pub fn add_weeks(ms: i64, n: i64) -> Result<i64> {
    add_scaled(ms, n, 7 * DAY_MS)
}

/// 业务作用: 按 GMT+8 日历位移自然月；目标月份天数不足时收缩到该月最后一天。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳，会按 GMT+8 日历解释年月日。
/// - `n`: 要增加的自然月数；负数表示向过去移动。
///
/// 返回: 日历位移有效时返回新时间戳；输入越界或目标日期超出支持范围时返回
/// [`DateError::Overflow`]。
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

/// 业务作用: 按 GMT+8 日历位移自然年；闰日落入平年时收缩到二月最后一天。
///
/// 参数说明:
/// - `ms`: 原始 epoch 毫秒时间戳，会按 GMT+8 日历解释年月日。
/// - `n`: 要增加的自然年数；负数表示向过去移动。
///
/// 返回: 年数换算和日历位移有效时返回新时间戳，否则返回 [`DateError::Overflow`]。
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

/// 业务作用: 按指定时间单位推进毫秒时间戳；用于日期区间枚举的核心位移。
///
/// 参数说明:
/// - `ms`: 当前 epoch 毫秒时间戳。
/// - `unit`: 每一步推进的时间单位。
/// - `n`: 推进数量；通常为 `1`，负数表示向过去移动。
///
/// 返回: 位移有效时返回新时间戳；数量转换、单位换算或日期范围越界时返回
/// [`DateError::Overflow`]。
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

/// 业务作用: 按指定日期模式丢弃低于模式精度的字段，得到 GMT+8 下的最早时刻。
///
/// 例:`earliest(t, "yyyy-MM-dd")` → 当天 00:00:00(GMT+8)。
///
/// 参数说明:
/// - `ms`: 需要截断到指定精度的 epoch 毫秒时间戳。
/// - `pattern`: 决定截断粒度的日期格式，例如 `yyyy-MM-dd`。
///
/// 返回: 格式化和回解析成功时返回截断后的 epoch 毫秒，否则返回相应日期错误。
pub fn earliest(ms: i64, pattern: &str) -> Result<i64> {
    let s = format(ms, pattern)?;
    parse(&s, pattern)
}

/// 业务作用: 枚举按指定精度对齐、按指定单位推进的时间点，并输出为日期文本。
///
/// 当 `start > end` 时返回空列表；否则先产出对齐后的起点，再推进并判断边界。因此
/// 对齐值可能早于 `start`；`start == end` 时仍返回包含对齐起点的一个元素。
/// 返回集合大小随区间长度线性增长，本函数不施加硬上限，调用方必须在业务边界限制区间。
///
/// 参数说明:
/// - `start`: 起点 epoch 毫秒；会先向下对齐到 `precision_pattern` 对应的精度。
/// - `end`: 终点 epoch 毫秒(起点之后的步进值 `>= end` 即停,不再产出)。
/// - `precision_pattern`: 起点对齐时使用的日期精度格式。
/// - `return_pattern`: 每个结果时间点输出时使用的格式。
/// - `unit`: 区间枚举时每一步推进的时间单位。
///
/// 返回: 成功时返回有序日期文本；对齐、位移或格式化失败时返回相应日期错误。
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

/// 业务作用: 从 `start` 所在的 GMT+8 日期起逐日枚举，对齐值达到
/// `end` 时停止。
///
/// 参数说明:
/// - `start`: 区间起点 epoch 毫秒；会先对齐到所在日的零点。
/// - `end`: 停止边界 epoch 毫秒；对齐时间点达到该值后不再产出。
/// - `return_pattern`: 每个自然日输出时使用的格式。
///
/// 返回: 成功时返回逐日文本；对齐、位移或格式化失败时返回相应日期错误。
pub fn all_days(start: i64, end: i64, return_pattern: &str) -> Result<Vec<String>> {
    all_time(start, end, F_Y_M_D, return_pattern, Unit::Days)
}

/// 业务作用: 枚举区间内逐月的字符串，按 `yyyyMM` 精度对齐并按自然月推进。
///
/// 参数说明:
/// - `start`: 区间起点 epoch 毫秒；会先对齐到所在月的首日零点。
/// - `end`: 停止边界 epoch 毫秒；对齐时间点达到该值后不再产出。
/// - `return_pattern`: 每个自然月输出时使用的格式。
///
/// 返回: 成功时返回逐月文本；对齐、位移或格式化失败时返回相应日期错误。
pub fn all_months(start: i64, end: i64, return_pattern: &str) -> Result<Vec<String>> {
    all_time(start, end, F_YM, return_pattern, Unit::Months)
}

// ==================== 当下 ====================

/// 业务作用: 读取当前墙钟，并按 GMT+8 默认日期时间模式格式化。
///
/// 参数说明: 无。
///
/// 返回: 成功时返回当前日期时间文本；墙钟超出支持范围时返回日期错误。
pub fn now() -> Result<String> {
    format(now_ms(), F_Y_M_D_H_M_S)
}

/// 业务作用: 读取当前墙钟，并按 GMT+8 格式化当天日期。
///
/// 参数说明: 无。
///
/// 返回: 成功时返回 `yyyy-MM-dd` 日期文本；墙钟超出支持范围时返回日期错误。
pub fn today() -> Result<String> {
    format(now_ms(), F_Y_M_D)
}

/// 业务作用: 读取当前墙钟，并按 GMT+8 和指定日期模式格式化。
///
/// 参数说明:
/// - `pattern`: 今天日期输出时使用的格式。
///
/// 返回: 成功时返回当前日期文本；墙钟超出支持范围时返回日期错误。
pub fn today_fmt(pattern: &str) -> Result<String> {
    format(now_ms(), pattern)
}

/// 业务作用: 读取当前墙钟前一日，并按 GMT+8 格式化日期。
///
/// 参数说明: 无。
///
/// 返回: 成功时返回昨日 `yyyy-MM-dd` 日期文本；日期位移或格式化失败时返回相应错误。
pub fn yesterday() -> Result<String> {
    format(add_days(now_ms(), -1)?, F_Y_M_D)
}

/// 业务作用: 读取当前墙钟前一日，并按 GMT+8 和指定日期模式格式化。
///
/// 参数说明:
/// - `pattern`: 昨天日期输出时使用的格式。
///
/// 返回: 成功时返回昨日日期文本；日期位移或格式化失败时返回相应错误。
pub fn yesterday_fmt(pattern: &str) -> Result<String> {
    format(add_days(now_ms(), -1)?, pattern)
}
