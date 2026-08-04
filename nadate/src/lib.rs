//! # date —— 日期时间工具(对照 原实现 原工具包 `DateUtils`)
//!
//! 规范表示 = **i64 epoch 毫秒**(对照 原实现 `long`)。默认时区 **GMT+8**(对照 `DateUtils.GMT8`)。
//! pattern 用 **原实现 SimpleDateFormat 风格**(`"yyyy-MM-dd HH:mm:ss"`),内部转 chrono strftime——便于从 原实现 迁移。
//!
//! ## 设计取舍
//! 原实现 `DateUtils` 有 292 个 public 方法,绝大多数是 `Date`/`long`/`String` × `TimeZone` × pattern 的重载爆炸。
//! Rust 版**只保留不同的概念操作**,统一在 i64 ms 上做,时区/pattern 走参数或默认值:
//! - 格式化:[`format()`] / [`format_offset`] / [`format_default`] + 一组命名便捷([`format_y_m_d`] 等)
//! - 解析:[`parse`] / [`parse_offset`] / [`parse_auto`] 自动识别格式 / [`get_format`]
//! - 加减:[`add_millis`]/[`add_seconds`]/…/[`add_days`]/[`add_weeks`]/[`add_months`]/[`add_years`]
//! - 截断/区间:[`earliest`](按 pattern 精度取最早)/ [`all_time`]/[`all_days`]/[`all_months`]
//! - 当下:[`now_ms`]/[`now`]/[`today`]/[`today_fmt`]/[`yesterday`]/[`yesterday_fmt`]
//!
//! **比较 `before/after/equals` 不另设**:i64 ms 直接用 `<`/`>`/`==`(Rust 原生),无需工具函数。

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};

mod calc;
pub use calc::{
    add_days, add_hours, add_millis, add_minutes, add_months, add_seconds, add_weeks, add_years,
    all_days, all_months, all_time, earliest, now, today, today_fmt, yesterday, yesterday_fmt,
    Unit,
};

pub mod clock;
pub use clock::{MonotonicClock, MonotonicInstant, SystemClock, UtcClock};

// ==================== 常量(对照 原实现 DateUtils)====================

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

/// `get_format` 自动识别用的(正则锚 → 原实现 pattern)表,对照 原实现 `DateUtils.formats`(顺序敏感)。
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
    /// 业务作用: 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
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

/// 业务作用: GMT+8(北京/上海)固定偏移,默认时区。
pub fn gmt8() -> FixedOffset {
    FixedOffset::east_opt(GMT8_OFFSET_SECS).expect("GMT+8 偏移合法")
}

/// 业务作用: 构造东 `hours` 时区偏移(如 `offset_hours(8)` = GMT+8;负数为西区)。
///
/// # 参数
///
/// - `hours`: 相对 UTC 的小时偏移量；正数为东区，负数为西区。
pub fn offset_hours(hours: i32) -> Result<FixedOffset> {
    // checked_mul:|hours| > ~596523 时 i32 乘法溢出,须返回 Err 而非 debug panic。
    hours
        .checked_mul(3600)
        .and_then(FixedOffset::east_opt)
        .ok_or(DateError::Offset)
}

/// 业务作用: 当前 epoch 毫秒。
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

// ==================== 原实现 pattern → chrono strftime ====================

/// 业务作用: 把 原实现 SimpleDateFormat pattern 转 chrono strftime pattern。
///
/// 支持:`yyyy`→`%Y`、`yy`→`%y`、`MM`→`%m`、`dd`→`%d`、`HH`→`%H`、`mm`→`%M`、`ss`→`%S`、`SSS`→`%3f`;
/// `'T'` 等单引号内为字面量;`- / : . 空格` 等为字面量(`%` 字面量转义为 `%%`)。
pub(crate) fn pattern_to_chrono(pat: &str) -> String {
    let mut out = String::with_capacity(pat.len() + 4);
    // 多字符 token 按最长匹配。token 均为 ASCII;字面量可能是多字节 UTF-8(如中文 `年月日`),
    // 故按【字符】步进而非字节下标——旧实现 `bytes[i] as char` + `pat[i..]` 会在多字节字符中部切片 panic。
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
/// # 参数
/// - `out`: chrono pattern 输出缓冲区。
/// - `c`: 原 pattern 中的字面字符。
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
/// # 参数
/// - `off`: 目标固定时区偏移。
/// - `ms`: epoch 毫秒时间戳。
pub(crate) fn to_datetime(off: FixedOffset, ms: i64) -> Result<DateTime<FixedOffset>> {
    let utc = DateTime::<Utc>::from_timestamp_millis(ms).ok_or(DateError::Overflow)?;
    Ok(utc.with_timezone(&off))
}

// ==================== 格式化 ====================

/// 业务作用: 用指定时区偏移 + 原实现 pattern 格式化 epoch 毫秒。
///
/// # 参数
///
/// - `off`: 目标固定时区偏移。
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
/// - `pattern`: 输出格式，使用原格式风格。
pub fn format_offset(off: FixedOffset, ms: i64, pattern: &str) -> Result<String> {
    let dt = to_datetime(off, ms)?;
    let cp = pattern_to_chrono(pattern);
    Ok(dt.format(&cp).to_string())
}

/// 业务作用: 用 **GMT+8** + 原实现 pattern 格式化 epoch 毫秒。对照 原实现 `format(long, f)`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
/// - `pattern`: 输出格式，使用原格式风格。
pub fn format(ms: i64, pattern: &str) -> Result<String> {
    format_offset(gmt8(), ms, pattern)
}

/// 业务作用: 默认格式 `yyyy-MM-dd HH:mm:ss`(GMT+8)。对照 原实现 `format(long)`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
pub fn format_default(ms: i64) -> Result<String> {
    format(ms, F_Y_M_D_H_M_S)
}

/// 业务作用: `yyyy-MM-dd`(GMT+8)。对照 原实现 `format_y_M_d`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
pub fn format_y_m_d(ms: i64) -> Result<String> {
    format(ms, F_Y_M_D)
}

/// 业务作用: `yyyy-MM`(GMT+8)。对照 原实现 `format_y_M`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
pub fn format_y_m(ms: i64) -> Result<String> {
    format(ms, F_Y_M)
}

/// 业务作用: `yyyyMMdd`(GMT+8)。对照 原实现 `format_yMd`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
pub fn format_ymd(ms: i64) -> Result<String> {
    format(ms, F_YMD)
}

/// 业务作用: `yyyyMMddHHmmss`(GMT+8)。对照 原实现 `format_yMdHms`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
pub fn format_ymdhms(ms: i64) -> Result<String> {
    format(ms, F_YMDHMS)
}

/// 业务作用: `yyyy/MM/dd`(GMT+8,路径风格)。对照 原实现 `format_yMd_path`。
///
/// # 参数
///
/// - `ms`: 待格式化的 epoch 毫秒时间戳。
pub fn format_ymd_path(ms: i64) -> Result<String> {
    format(ms, F_YMD_PATH)
}

// ==================== 解析 ====================

/// 业务作用: 用指定时区偏移 + 原实现 pattern 解析为 epoch 毫秒。缺失的字段按默认补(年 1970/月日 01/时分秒 00)。
///
/// # 参数
///
/// - `off`: 输入文本应按哪个固定时区偏移解释。
/// - `input`: 待解析的日期时间文本。
/// - `pattern`: 输入文本对应的格式，使用原格式风格。
pub fn parse_offset(off: FixedOffset, input: &str, pattern: &str) -> Result<i64> {
    let cp = pattern_to_chrono(pattern);
    let naive = augment_parse(input.trim(), &cp)?;
    let dt = off
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| DateError::Parse(format!("时刻在时区 {off} 不存在/有歧义: {input:?}")))?;
    Ok(dt.timestamp_millis())
}

/// 业务作用: 用 **GMT+8** + 原实现 pattern 解析为 epoch 毫秒。对照 原实现 `parseLong(dt, f)`。
///
/// # 参数
///
/// - `input`: 待解析的日期时间文本。
/// - `pattern`: 输入文本对应的格式，使用原格式风格。
pub fn parse(input: &str, pattern: &str) -> Result<i64> {
    parse_offset(gmt8(), input, pattern)
}

/// 业务作用: **自动识别**日期字符串格式(GMT+8)解析为 epoch 毫秒。对照 原实现 `parseLong(dt)`(内部 `getFormat`)。
///
/// # 参数
///
/// - `input`: 待自动识别格式并解析的日期时间文本。
pub fn parse_auto(input: &str) -> Result<i64> {
    let pat = get_format(input)?;
    parse(input, pat)
}

/// 业务作用: 识别日期字符串的 原实现 pattern(顺序匹配预置正则,与 原实现 `getFormat` 同源)。
///
/// # 参数
///
/// - `input`: 待识别格式的日期时间文本。
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
/// # 参数
/// - `input`: 待解析的日期时间文本。
/// - `chrono_pat`: 已从原格式转换得到的 chrono pattern。
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
/// # 参数
/// - `re`: 简化正则表达式。
/// - `s`: 待匹配的日期时间文本。
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
