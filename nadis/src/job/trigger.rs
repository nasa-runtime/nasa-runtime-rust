//! 触发引擎：由任务定义与当前逻辑时刻计算下一逻辑触发时刻，供调度器写入共享 `schedule` ZSET。
//!
//! `CRON` 用命名时区解析基准时刻并求严格晚于基准的下一触发；`FIXED_RATE`/`FIXED_DELAY` 为逻辑时刻加固定间隔；
//! `MANUAL`/`FANOUT_ONLY` 无自动时刻返回零。下一触发时刻是写入共享调度索引的 score，多实现据此调度同一任务，
//! 因此计算必须稳定；cron 只接收显式冻结的 Spring 数字语法交集，星期编号在进入本地引擎前转换。

use std::str::FromStr;

use chrono::{LocalResult, TimeZone};
use chrono_tz::Tz;
use cron::Schedule;

use crate::error::{NasaRedisError, Result};
use crate::job::definition::JobDefinition;
use crate::job::model::JobScheduleType;

/// 跨语言部署应写入 manifest 的 Cron 语义标识。
pub const CRON_SEMANTICS_ID: &str = "spring-numeric-intersection";
/// 当前编译产物使用的 IANA tzdb 版本。
pub const CRON_TZDB_VERSION: &str = chrono_tz::IANA_TZDB_VERSION;

/// 业务作用：按调度类型计算严格晚于给定逻辑时刻的下一逻辑触发时刻；结果作为共享调度 score。
///
/// 参数说明：
/// - `definition`: 任务定义，提供调度类型、cron 表达式、时区与固定间隔。
/// - `logical_fire_at`: 基准逻辑时刻毫秒（求其之后的下一触发）。
///
/// 返回：`CRON` 返回下一匹配时刻毫秒；`FIXED_RATE`/`FIXED_DELAY` 返回 `logical_fire_at + interval_ms`；
/// `MANUAL`/`FANOUT_ONLY` 返回 0；cron/时区非法或间隔溢出时返回配置错误。
pub fn next_fire_at(definition: &JobDefinition, logical_fire_at: i64) -> Result<i64> {
    match definition.schedule_type() {
        JobScheduleType::Cron => next_cron(definition.cron(), definition.zone(), logical_fire_at),
        JobScheduleType::FixedRate | JobScheduleType::FixedDelay => {
            let interval =
                i64::try_from(definition.interval_ms()).map_err(|_| err("固定调度间隔超出 i64"))?;
            logical_fire_at
                .checked_add(interval)
                .ok_or_else(|| err("固定间隔下一触发时刻溢出"))
        }
        JobScheduleType::Manual | JobScheduleType::FanoutOnly => Ok(0),
    }
}

/// 业务作用：一步跨过长时间积压，计算严格晚于 Redis 当前时刻的第一个逻辑触发时刻。
///
/// 参数说明：`definition` 提供调度语义，`logical_fire_at` 是当前网格锚点，`redis_now` 是脚本观测的权威时刻。
///
/// 返回：自动调度返回严格晚于 `redis_now` 的时刻；手工与 Fanout-only 返回零；算术溢出或 Cron 无后继时返回配置错误。
pub fn first_fire_at_after(
    definition: &JobDefinition,
    logical_fire_at: i64,
    redis_now: i64,
) -> Result<i64> {
    match definition.schedule_type() {
        JobScheduleType::FixedRate => {
            let interval =
                i64::try_from(definition.interval_ms()).map_err(|_| err("固定速率间隔超出 i64"))?;
            let distance = redis_now
                .checked_sub(logical_fire_at)
                .ok_or_else(|| err("固定速率逻辑时刻差溢出"))?;
            let steps = distance.div_euclid(interval).saturating_add(1).max(1);
            logical_fire_at
                .checked_add(
                    interval
                        .checked_mul(steps)
                        .ok_or_else(|| err("固定速率跨度溢出"))?,
                )
                .ok_or_else(|| err("固定速率下一触发时刻溢出"))
        }
        JobScheduleType::FixedDelay => {
            let interval =
                i64::try_from(definition.interval_ms()).map_err(|_| err("固定延迟间隔超出 i64"))?;
            redis_now
                .checked_add(interval)
                .ok_or_else(|| err("固定延迟下一触发时刻溢出"))
        }
        JobScheduleType::Cron => next_cron(
            definition.cron(),
            definition.zone(),
            logical_fire_at.max(redis_now),
        ),
        JobScheduleType::Manual | JobScheduleType::FanoutOnly => Ok(0),
    }
}

/// 业务作用：校验表达式属于已冻结的 Spring 六段数字语法交集，并确认时区可由编译期 tzdb 解析。
///
/// 参数说明：`expr` 是秒到星期的六段表达式，`zone` 是 IANA 时区 ID。
///
/// 返回：表达式可无损转换为本地引擎语义时成功；Spring 扩展、年字段、歧义日期组合或未知时区被拒绝。
pub fn validate_cron_compatibility(expr: &str, zone: &str) -> Result<()> {
    normalize_cron(expr)?;
    Tz::from_str(zone.trim()).map_err(|_| err(&format!("非法时区: {zone}")))?;
    Ok(())
}

/// 业务作用：解析命名时区与 cron 表达式，求严格晚于基准毫秒的下一触发时刻。
///
/// 参数说明：
/// - `expr`: 6 段 cron 表达式（秒 分 时 日 月 周）。
/// - `zone`: 命名时区（如 `UTC`、`Asia/Shanghai`）。
/// - `from_millis`: 基准逻辑时刻毫秒。
///
/// 返回：下一触发时刻毫秒；时区或 cron 非法、基准时刻无法表示或无下一触发时返回配置错误。
fn next_cron(expr: &str, zone: &str, from_millis: i64) -> Result<i64> {
    let normalized = normalize_cron(expr)?;
    let tz = Tz::from_str(zone).map_err(|_| err(&format!("非法时区: {zone}")))?;
    let base = tz
        .timestamp_millis_opt(from_millis)
        .single()
        .ok_or_else(|| err("cron 基准时刻无法在该时区唯一表示"))?;
    let schedule = Schedule::from_str(&normalized)
        .map_err(|error| err(&format!("非法 cron 表达式: {error}")))?;
    if let Some(next) = next_match_inside_overlap(&schedule, tz, &base) {
        return Ok(next.timestamp_millis());
    }
    let next = schedule
        .after(&base)
        .next()
        .ok_or_else(|| err("cron 表达式无下一触发时刻"))?;
    let next = match tz.from_local_datetime(&next.naive_local()) {
        LocalResult::Ambiguous(earlier, _) if earlier > base => earlier,
        LocalResult::Ambiguous(_, later) if later > base => later,
        _ => next,
    };
    Ok(next.timestamp_millis())
}

/// 业务作用：在 DST 回拨的重复本地时间内按绝对时间寻找最早匹配，保留 Spring 会依次返回两个 offset 实例的语义。
///
/// 参数说明：`schedule` 是已转换的表达式，`tz` 是命名时区，`base` 是严格下界。
///
/// 返回：基准落在 overlap 本地区间时返回第一个后续匹配；其它情况返回 `None` 交由常规迭代器处理。
fn next_match_inside_overlap(
    schedule: &Schedule,
    tz: Tz,
    base: &chrono::DateTime<Tz>,
) -> Option<chrono::DateTime<Tz>> {
    let LocalResult::Ambiguous(earlier, later) = tz.from_local_datetime(&base.naive_local()) else {
        return None;
    };
    let overlap_ms = later
        .timestamp_millis()
        .checked_sub(earlier.timestamp_millis())?;
    if overlap_ms <= 0 {
        return None;
    }
    let first_second = base.timestamp_millis().div_euclid(1_000).checked_add(1)?;
    let last_second = base
        .timestamp_millis()
        .checked_add(overlap_ms)?
        .div_euclid(1_000);
    for second in first_second..=last_second {
        let candidate = tz.timestamp_opt(second, 0).single()?;
        if schedule.includes(candidate) {
            return Some(candidate);
        }
    }
    None
}

/// 业务作用：把 Spring 数字星期编号转为本地引擎编号，其余允许字段保持原语义。
///
/// 参数说明：`expr` 是待冻结的 Spring 六段表达式。
///
/// 返回：本地 `cron` 引擎可执行的六段表达式；交集外语法返回配置错误。
fn normalize_cron(expr: &str) -> Result<String> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 6 {
        return Err(err("cron 必须恰好包含六个字段"));
    }
    let bounds = [(0, 59), (0, 59), (0, 23), (1, 31), (1, 12)];
    let mut normalized = Vec::with_capacity(6);
    for (index, (field, (min, max))) in fields.iter().take(5).zip(bounds).enumerate() {
        normalized.push(normalize_numeric_field(field, min, max, index == 3)?);
    }
    let day_of_month_any = matches!(fields[3], "*" | "?");
    let day_of_week_any = matches!(fields[5], "*" | "?");
    if !day_of_month_any && !day_of_week_any {
        return Err(err("cron 日与星期不能同时受限"));
    }
    if fields[3] == "?" {
        normalized[3] = "*".to_owned();
    }
    normalized.push(normalize_day_of_week(fields[5])?);
    let normalized = normalized.join(" ");
    Schedule::from_str(&normalized)
        .map_err(|error| err(&format!("cron 交集表达式无法解析: {error}")))?;
    Ok(normalized)
}

/// 业务作用：校验一个数值 cron 字段的通配、列表、范围和步进语法。
///
/// 参数说明：`field` 是单字段文本，`min`/`max` 是包含边界，`allow_question` 控制是否将 `?` 视为通配。
///
/// 返回：语法与数值边界合法时返回规范文本；否则返回配置错误。
fn normalize_numeric_field(
    field: &str,
    min: u32,
    max: u32,
    allow_question: bool,
) -> Result<String> {
    if field == "?" {
        return if allow_question {
            Ok("*".to_owned())
        } else {
            Err(err("cron 字段不允许 '?'"))
        };
    }
    if field.is_empty()
        || !field
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'*' | b',' | b'-' | b'/'))
    {
        return Err(err("cron 只允许数字、通配、列表、范围和步进"));
    }
    for item in field.split(',') {
        let mut step_parts = item.split('/');
        let base = step_parts.next().unwrap_or_default();
        let step = step_parts.next();
        if step_parts.next().is_some() {
            return Err(err("cron 步进字段包含多个 '/'"));
        }
        if let Some(step) = step {
            let step = parse_ordinal(step, 1, max.saturating_sub(min).saturating_add(1))?;
            if step == 0 {
                return Err(err("cron 步进必须大于零"));
            }
        }
        if base == "*" {
            continue;
        }
        if let Some((start, end)) = base.split_once('-') {
            let start = parse_ordinal(start, min, max)?;
            let end = parse_ordinal(end, min, max)?;
            if start > end {
                return Err(err("cron 范围起点不能晚于终点"));
            }
        } else {
            parse_ordinal(base, min, max)?;
        }
    }
    Ok(field.to_owned())
}

/// 业务作用：转换 Spring 星期编号（0 或 7 为周日）到本地引擎编号（1 为周日）。
///
/// 参数说明：`field` 只允许通配或数字列表。
///
/// 返回：去重排序后的本地星期字段；范围、步进、名称或越界编号被拒绝。
fn normalize_day_of_week(field: &str) -> Result<String> {
    if matches!(field, "*" | "?") {
        return Ok("*".to_owned());
    }
    let mut days = Vec::new();
    for item in field.split(',') {
        let spring = parse_ordinal(item, 0, 7)?;
        let local = if spring == 0 || spring == 7 {
            1
        } else {
            spring + 1
        };
        days.push(local);
    }
    days.sort_unstable();
    days.dedup();
    Ok(days
        .into_iter()
        .map(|day| day.to_string())
        .collect::<Vec<_>>()
        .join(","))
}

/// 业务作用：解析并检查 cron 数字原子的包含边界。
///
/// 参数说明：`value` 是十进制原子，`min`/`max` 是该字段的合法区间。
///
/// 返回：区间内数值；空值、非数字或越界时返回配置错误。
fn parse_ordinal(value: &str, min: u32, max: u32) -> Result<u32> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| err("cron 数字字段无法解析"))?;
    if !(min..=max).contains(&parsed) {
        return Err(err("cron 数字字段越界"));
    }
    Ok(parsed)
}

/// 业务作用：构造触发引擎配置错误，便于业务定位非法 cron/时区或溢出。
///
/// 参数说明：
/// - `message`: 稳定错误摘要。
///
/// 返回：`JobError::Config` 错误。
fn err(message: &str) -> NasaRedisError {
    crate::job::JobError::Config(format!("trigger {message}")).into()
}
