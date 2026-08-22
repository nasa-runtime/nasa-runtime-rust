//! 日期时间解析、格式化、日历位移、区间枚举与时钟抽象。
//!
//! 日期时刻统一表示为 `i64` epoch 毫秒，未显式传入偏移时使用固定 GMT+8。日期模式支持
//! `yyyy`、`yy`、`MM`、`dd`、`HH`、`mm`、`ss`、`SSS` 和单引号字面量，未声明的字符
//! 按字面量处理。
//!
//! ## 能力边界
//!
//! 本模块不读取配置、不维护时区数据库，也不处理夏令时规则。需要地域时区变化的业务应在边界层
//! 完成转换，再以 epoch 毫秒或明确的固定偏移调用本模块。
//!
//! - 格式化:[`format()`] / [`format_offset`] / [`format_default`] + 一组命名便捷([`format_y_m_d`] 等)
//! - 解析:[`parse`] / [`parse_offset`] / [`parse_auto`] 自动识别格式 / [`get_format`]
//! - 加减:[`add_millis`]/[`add_seconds`]/…/[`add_days`]/[`add_weeks`]/[`add_months`]/[`add_years`]
//! - 截断/区间:[`earliest`](按 pattern 精度取最早)/ [`all_time`]/[`all_days`]/[`all_months`]
//! - 当下:[`now_ms`]/[`now`]/[`today`]/[`today_fmt`]/[`yesterday`]/[`yesterday_fmt`]

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};

mod calc;
pub use calc::{
    add_days, add_hours, add_millis, add_minutes, add_months, add_seconds, add_weeks, add_years,
    all_days, all_months, all_time, earliest, now, today, today_fmt, yesterday, yesterday_fmt,
    Unit,
};

pub mod clock;
pub use clock::{MonotonicClock, MonotonicInstant, SystemClock, UtcClock};

// ==================== 常量 ====================

/// 一天的秒数。
pub const DAY_S: i64 = 24 * 60 * 60;
/// 一天的毫秒数。
pub const DAY_MS: i64 = DAY_S * 1000;
/// GMT+8 偏移秒数(北京/上海)。
pub const GMT8_OFFSET_SECS: i32 = 8 * 3600;

/// `yyyy-MM`
pub const F_Y_M: &str = "yyyy-MM";
/// `yyyy-MM-dd`
pub const F_Y_M_D: &str = "yyyy-MM-dd";
/// `yyyy-MM-dd HH:mm:ss`(默认格式)
pub const F_Y_M_D_H_M_S: &str = "yyyy-MM-dd HH:mm:ss";
/// `yyyyMM`
pub const F_YM: &str = "yyyyMM";
/// `yyyyMMdd`
pub const F_YMD: &str = "yyyyMMdd";
/// `yyyyMMddHHmmss`
pub const F_YMDHMS: &str = "yyyyMMddHHmmss";
/// `yyyy/MM`
pub const F_YM_PATH: &str = "yyyy/MM";
/// `yyyy/MM/dd`
pub const F_YMD_PATH: &str = "yyyy/MM/dd";
/// `yyyy/MM/dd HH:mm:ss`
pub const F_YMDHMS_PATH: &str = "yyyy/MM/dd HH:mm:ss";

/// `get_format` 自动识别使用的模式表，匹配顺序决定优先级。
const AUTO_FORMATS: &[(&str, &str)] = &[
    (F_Y_M_D_H_M_S, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
    (F_Y_M_D, r"^\d{4}-\d{2}-\d{2}$"),
    (F_Y_M, r"^\d{4}-\d{2}$"),
    (F_YM, r"^\d{6}$"),
    (F_YMD, r"^\d{8}$"),
    (F_YMDHMS, r"^\d{14}$"),
    ("yyyyMMddHH", r"^\d{10}$"),
    ("yyyyMMddHHmm", r"^\d{12}$"),
    (F_YMDHMS_PATH, r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$"),
    (F_YMD_PATH, r"^\d{4}/\d{2}/\d{2}$"),
    (F_YM_PATH, r"^\d{4}/\d{2}$"),
    ("yyyy", r"^\d{4}$"),
    ("HH:mm:ss", r"^\d{2}:\d{2}:\d{2}$"),
    ("yyyy-MM-dd HH:mm", r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$"),
    ("yyyy-MM-dd HH", r"^\d{4}-\d{2}-\d{2} \d{2}$"),
];

// ==================== 错误 ====================

/// 日期工具错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateError {
    /// 解析失败(输入与 pattern 不匹配,或非法时刻)。
    Parse(String),
    /// 时间戳越 chrono 域 / 加减溢出。
    Overflow,
    /// 非法时区偏移。
    Offset,
    /// 无法识别的日期格式(`get_format`/`parse_auto`)。
    UnknownFormat(String),
}

impl core::fmt::Display for DateError {
    /// 业务作用: 将日期错误转换为稳定的可读文本，供错误链和日志输出。
    ///
    /// 参数说明:
    /// - `f`: 标准格式化器。
    ///
    /// 返回: 文本成功写入时返回成功，否则透传格式化错误。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DateError::Parse(s) => write!(f, "date parse error: {s}"),
            DateError::Overflow => write!(f, "timestamp overflow / out of range"),
            DateError::Offset => write!(f, "invalid timezone offset"),
            DateError::UnknownFormat(s) => write!(f, "unknown date format: {s:?}"),
        }
    }
}

impl std::error::Error for DateError {}

/// 本 crate 统一 `Result`。
pub type Result<T> = core::result::Result<T, DateError>;

// ==================== 时区 / 当前时刻 ====================

/// 业务作用: 构造日期工具默认使用的 GMT+8 固定偏移。
///
/// 参数说明: 无。
///
/// 返回: 始终返回合法的 GMT+8 固定偏移。
pub fn gmt8() -> FixedOffset {
    FixedOffset::east_opt(GMT8_OFFSET_SECS).expect("GMT+8 偏移合法")
}

/// 业务作用: 构造东 `hours` 时区偏移(如 `offset_hours(8)` = GMT+8;负数为西区)。
///
/// 参数说明:
/// - `hours`: 相对 UTC 的小时偏移量；正数为东区，负数为西区。
///
/// 返回: 偏移位于 `chrono` 支持范围内时返回固定偏移，乘法溢出或偏移非法时返回
/// [`DateError::Offset`]。
pub fn offset_hours(hours: i32) -> Result<FixedOffset> {
    // 先做受检乘法，避免异常配置在计算偏移秒数时终止进程。
    hours
        .checked_mul(3600)
        .and_then(FixedOffset::east_opt)
        .ok_or(DateError::Offset)
}

/// 业务作用: 读取当前 UTC 墙钟并转换为 epoch 毫秒。
///
/// 参数说明: 无。
///
/// 返回: 当前墙钟对应的 epoch 毫秒；系统校时可能使连续结果回拨。
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

// ==================== 日期模式转换 ====================

/// 业务作用: 将公开日期模式转换为 `chrono` 格式化模式。
///
/// 支持:`yyyy`→`%Y`、`yy`→`%y`、`MM`→`%m`、`dd`→`%d`、`HH`→`%H`、`mm`→`%M`、`ss`→`%S`、`SSS`→`%3f`;
/// `'T'` 等单引号内为字面量;`- / : . 空格` 等为字面量(`%` 字面量转义为 `%%`)。
///
/// 参数说明:
/// - `pat`: 业务传入的日期模式。
///
/// 返回: 可交给 `chrono` 解析或格式化的模式文本；不支持的 token 按字面量保留。
pub(crate) fn pattern_to_chrono(pat: &str) -> String {
    let mut out = String::with_capacity(pat.len() + 4);
    // 多字符 token 按最长匹配；按字符推进才能保证多字节字面量始终停在 UTF-8 边界。
    let tokens: &[(&str, &str)] = &[
        ("yyyy", "%Y"),
        ("yy", "%y"),
        ("MM", "%m"),
        ("dd", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),
        ("ss", "%S"),
        ("SSS", "%3f"),
    ];
    let mut rest = pat;
    'outer: while let Some(c) = rest.chars().next() {
        if c == '\'' {
            // 单引号字面量,直到下一个单引号(引号内也可能是多字节字符)。
            rest = &rest[c.len_utf8()..];
            while let Some(ch) = rest.chars().next() {
                rest = &rest[ch.len_utf8()..];
                if ch == '\'' {
                    break; // 跳过闭合单引号
                }
                push_literal(&mut out, ch);
            }
            continue;
        }
        for (jt, ct) in tokens {
            if rest.starts_with(jt) {
                out.push_str(ct);
                rest = &rest[jt.len()..];
                continue 'outer;
            }
        }
        push_literal(&mut out, c);
        rest = &rest[c.len_utf8()..];
    }
    out
}

/// 业务作用: 向格式化结果写入字面字符；用于保留模式串里的普通文本。
///
/// 参数说明:
/// - `out`: chrono pattern 输出缓冲区。
/// - `c`: 原 pattern 中的字面字符。
///
/// 返回: 无返回值；`%` 会转义，其余字符原样追加到缓冲区。
fn push_literal(out: &mut String, c: char) {
    if c == '%' {
        out.push_str("%%");
    } else {
        out.push(c);
    }
}

// ==================== 内部:ms ↔ DateTime ====================

/// 业务作用: 转换为带固定时区偏移的 datetime。
///
/// 参数说明:
/// - `off`: 目标固定时区偏移。
/// - `ms`: epoch 毫秒时间戳。
///
/// 返回: 时间戳位于 `chrono` 支持范围内时返回目标偏移下的时间，否则返回
/// [`DateError::Overflow`]。
pub(crate) fn to_datetime(off: FixedOffset, ms: i64) -> Result<DateTime<FixedOffset>> {
    let utc = DateTime::<Utc>::from_timestamp_millis(ms).ok_or(DateError::Overflow)?;
    Ok(utc.with_timezone(&off))
}

// ==================== 格式化 ====================

/// 业务作用: 按指定固定偏移和日期模式格式化 epoch 毫秒。
///
/// 参数说明:
/// - `off`: 目标固定时区偏移。
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
/// - `pattern`: 输出日期模式。
///
/// 返回: 成功时返回格式化文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_offset(off: FixedOffset, ms: i64, pattern: &str) -> Result<String> {
    let dt = to_datetime(off, ms)?;
    let cp = pattern_to_chrono(pattern);
    Ok(dt.format(&cp).to_string())
}

/// 业务作用: 按 GMT+8 和指定日期模式格式化 epoch 毫秒。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
/// - `pattern`: 输出日期模式。
///
/// 返回: 成功时返回格式化文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format(ms: i64, pattern: &str) -> Result<String> {
    format_offset(gmt8(), ms, pattern)
}

/// 业务作用: 按 GMT+8 和默认模式 `yyyy-MM-dd HH:mm:ss` 格式化 epoch 毫秒。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
///
/// 返回: 成功时返回格式化文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_default(ms: i64) -> Result<String> {
    format(ms, F_Y_M_D_H_M_S)
}

/// 业务作用: 按 GMT+8 将 epoch 毫秒格式化为 `yyyy-MM-dd`。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
///
/// 返回: 成功时返回日期文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_y_m_d(ms: i64) -> Result<String> {
    format(ms, F_Y_M_D)
}

/// 业务作用: 按 GMT+8 将 epoch 毫秒格式化为 `yyyy-MM`。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
///
/// 返回: 成功时返回年月文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_y_m(ms: i64) -> Result<String> {
    format(ms, F_Y_M)
}

/// 业务作用: 按 GMT+8 将 epoch 毫秒格式化为 `yyyyMMdd`。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
///
/// 返回: 成功时返回紧凑日期文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_ymd(ms: i64) -> Result<String> {
    format(ms, F_YMD)
}

/// 业务作用: 按 GMT+8 将 epoch 毫秒格式化为 `yyyyMMddHHmmss`。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
///
/// 返回: 成功时返回紧凑日期时间文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_ymdhms(ms: i64) -> Result<String> {
    format(ms, F_YMDHMS)
}

/// 业务作用: 按 GMT+8 将 epoch 毫秒格式化为路径可读的 `yyyy/MM/dd`。
///
/// 参数说明:
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
///
/// 返回: 成功时返回日期路径文本；时间戳越界时返回 [`DateError::Overflow`]。
pub fn format_ymd_path(ms: i64) -> Result<String> {
    format(ms, F_YMD_PATH)
}

// ==================== 解析 ====================

/// 业务作用: 按指定固定偏移和日期模式解析 epoch 毫秒；缺失字段按年 1970、月日 01、
/// 时分秒 00 补齐。
///
/// 参数说明:
/// - `off`: 输入文本应按哪个固定时区偏移解释。
/// - `input`: 待解析的日期时间文本。
/// - `pattern`: 输入文本对应的日期模式。
///
/// 返回: 成功时返回 epoch 毫秒；格式不匹配、日期非法或本地时刻无法唯一映射时返回
/// [`DateError::Parse`]。
pub fn parse_offset(off: FixedOffset, input: &str, pattern: &str) -> Result<i64> {
    let cp = pattern_to_chrono(pattern);
    let naive = augment_parse(input.trim(), &cp)?;
    let dt = off
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| DateError::Parse(format!("时刻在时区 {off} 不存在/有歧义: {input:?}")))?;
    Ok(dt.timestamp_millis())
}

/// 业务作用: 按 GMT+8 和指定日期模式解析 epoch 毫秒。
///
/// 参数说明:
/// - `input`: 待解析的日期时间文本。
/// - `pattern`: 输入文本对应的日期模式。
///
/// 返回: 成功时返回 epoch 毫秒；格式不匹配或日期非法时返回 [`DateError::Parse`]。
pub fn parse(input: &str, pattern: &str) -> Result<i64> {
    parse_offset(gmt8(), input, pattern)
}

/// 业务作用: 从预置格式中自动识别日期文本，并按 GMT+8 解析为 epoch 毫秒。
///
/// 参数说明:
/// - `input`: 待自动识别格式并解析的日期时间文本。
///
/// 返回: 成功时返回 epoch 毫秒；格式无法识别时返回 [`DateError::UnknownFormat`]，日期非法时
/// 返回 [`DateError::Parse`]。
pub fn parse_auto(input: &str) -> Result<i64> {
    let pat = get_format(input)?;
    parse(input, pat)
}

/// 业务作用: 按固定优先级识别日期文本对应的预置模式。
///
/// 参数说明:
/// - `input`: 待识别格式的日期时间文本。
///
/// 返回: 匹配时返回静态日期模式；无预置模式匹配时返回 [`DateError::UnknownFormat`]。
pub fn get_format(input: &str) -> Result<&'static str> {
    let s = input.trim();
    for (pat, re) in AUTO_FORMATS {
        if simple_match(re, s) {
            return Ok(pat);
        }
    }
    Err(DateError::UnknownFormat(s.to_string()))
}

/// 业务作用: 把不含 Y/m/d/H/M/S 的字段补默认后用 chrono 解析为 NaiveDateTime。
///
/// 参数说明:
/// - `input`: 待解析的日期时间文本。
/// - `chrono_pat`: 已转换得到的 `chrono` 日期模式。
///
/// 返回: 字段完整且日期合法时返回本地日期时间，否则返回 [`DateError::Parse`]。
fn augment_parse(input: &str, chrono_pat: &str) -> Result<NaiveDateTime> {
    let defaults: &[(&str, &str)] = &[
        ("%Y", "1970"),
        ("%m", "01"),
        ("%d", "01"),
        ("%H", "00"),
        ("%M", "00"),
        ("%S", "00"),
    ];
    let mut pat = chrono_pat.to_string();
    let mut inp = input.to_string();
    // 年份存在性须同时识别 %Y(yyyy)与 %y(yy 两位年):否则 "yy" pattern 会被误补
    // 一个冲突的 `%Y 1970`,导致除 xx70 外所有两位年解析失败。
    let has_year = chrono_pat.contains("%Y") || chrono_pat.contains("%y");
    for (spec, def) in defaults {
        let present = if *spec == "%Y" {
            has_year
        } else {
            chrono_pat.contains(spec)
        };
        if !present {
            pat.push(' ');
            pat.push_str(spec);
            inp.push(' ');
            inp.push_str(def);
        }
    }
    // 补齐主字段后,input 与 pattern 必为完整 datetime,chrono 直接解析。
    NaiveDateTime::parse_from_str(&inp, &pat)
        .map_err(|e| DateError::Parse(format!("{input:?} 不匹配 {chrono_pat:?}: {e}")))
}

/// 业务作用: 极简正则匹配。
///
/// 只支持本 crate `AUTO_FORMATS` 用到的 `^`、`$`、`\d{n}` 和字面量，
/// 避免为了日期格式识别引入完整 regex 依赖。
///
/// 参数说明:
/// - `re`: 简化正则表达式。
/// - `s`: 待匹配的日期时间文本。
///
/// 返回: 输入完整匹配表达式时返回 `true`，否则返回 `false`。
fn simple_match(re: &str, s: &str) -> bool {
    // 形如 ^...$ 的锚定;主体由 \d{n} 与字面量交替
    let body = re
        .strip_prefix('^')
        .and_then(|r| r.strip_suffix('$'))
        .unwrap_or(re);
    let sb = s.as_bytes();
    let rb = body.as_bytes();
    let (mut si, mut ri) = (0usize, 0usize);
    while ri < rb.len() {
        if rb[ri] == b'\\' && ri + 1 < rb.len() && rb[ri + 1] == b'd' {
            // \d{n}
            ri += 2;
            let mut n = 0usize;
            if ri < rb.len() && rb[ri] == b'{' {
                ri += 1;
                while ri < rb.len() && rb[ri].is_ascii_digit() {
                    n = n * 10 + (rb[ri] - b'0') as usize;
                    ri += 1;
                }
                if ri < rb.len() && rb[ri] == b'}' {
                    ri += 1;
                }
            } else {
                n = 1;
            }
            for _ in 0..n {
                if si >= sb.len() || !sb[si].is_ascii_digit() {
                    return false;
                }
                si += 1;
            }
        } else {
            // 字面量字符
            if si >= sb.len() || sb[si] != rb[ri] {
                return false;
            }
            si += 1;
            ri += 1;
        }
    }
    si == sb.len()
}
