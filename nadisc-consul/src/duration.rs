use nadisc::{http::error, managed::*};
use std::time::Duration;

/// 业务作用：将 agent 可读检查时限转换为精确时长，避免单位和复合表达影响冻结计划核验。
/// 参数说明：`value` 是 Interval 或 Timeout 字段，单位遵循 Consul 的 Go duration 表达。
/// 返回：完整且可精确表示为纳秒的非负时长；缺失、溢出或非法表达返回 IncompleteSnapshot。
pub(super) fn read(value: Option<&str>) -> Result<Duration> {
    value.and_then(parse).ok_or_else(|| {
        error(
            ErrorCode::IncompleteSnapshot,
            "Consul HTTP check duration is missing or invalid",
        )
    })
}

/// 业务作用：有界解析 agent 的时长字符串，不因浮点舍入把不同检查期限认证为同一值。
/// 参数说明：`value` 是带单位的时长，可以包含小时、分钟、秒及小数分量。
/// 返回：纳秒精度时长；负值、无法精确表达的子纳秒和超出后端整数范围的值拒绝。
fn parse(value: &str) -> Option<Duration> {
    if value.is_empty() || value.len() > 128 {
        return None;
    }
    let mut remaining = value.strip_prefix('+').unwrap_or(value);
    if remaining == "0" {
        return Some(Duration::ZERO);
    }
    let mut total = 0u128;
    let mut components = 0;
    while !remaining.is_empty() {
        let number_end = remaining.find(|c: char| !c.is_ascii_digit() && c != '.')?;
        let (number, rest) = remaining.split_at(number_end);
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let (unit, rest) = rest.split_at(unit_end);
        let scale: u128 = match unit {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty() && fraction.is_empty() {
            return None;
        }
        let whole = if whole.is_empty() {
            0
        } else {
            whole.parse::<u128>().ok()?
        };
        let mut nanos = whole.checked_mul(scale)?;
        if !fraction.is_empty() {
            let denominator = 10u128.checked_pow(fraction.len().try_into().ok()?)?;
            let numerator = fraction.parse::<u128>().ok()?.checked_mul(scale)?;
            if numerator % denominator != 0 {
                return None;
            }
            nanos = nanos.checked_add(numerator / denominator)?;
        }
        total = total.checked_add(nanos)?;
        if total > i64::MAX as u128 {
            return None;
        }
        components += 1;
        remaining = rest;
    }
    (components > 0).then(|| Duration::from_nanos(total as u64))
}
