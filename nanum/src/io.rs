//! I/O 边界:字符串 / f64 → 定点,定点 → 字符串。

use crate::{check_scale, pow10, NumericError, Result};

/// 业务作用：按 `floor(x + 0.5)` 为定点输入取整，半数朝正无穷。
/// 参数说明：`x` 为缩放后的浮点值。
/// 返回：整数浮点值；与默认乘除的半数远离零规则不同。
pub(crate) fn compat_math_round(x: f64) -> f64 {
    (x + 0.5).floor()
}

/// 业务作用：将十进制或科学计数法文本经 f64 转为定点 i128。
/// 半数取整朝正无穷，超过 scale 的小数会舍入，例如 `to_fixed_str("0.000000001", 8)` 为零。
/// 十六进制浮点文本、NaN 和无穷会被拒绝。
///
/// f64 中转只有 53 位有效二进制精度，长十进制金额可能丢失末位；需要精确文本计算时使用
/// [`crate::decimal`]。返回：目标 scale 的定点值；解析、scale 或范围非法时返回错误。
///
/// # 参数
///
/// - `val`: 待转换的十进制或科学计数法金额文本。
/// - `scale`: 目标定点小数位数，必须不超过 [`crate::MAX_SCALE`]。
pub fn to_fixed_str(val: &str, scale: u32) -> Result<i128> {
    check_scale(scale)?;
    let parsed: f64 = val
        .trim()
        .parse()
        .map_err(|_| NumericError::Parse(format!("非法数字: {val:?}")))?;
    to_fixed_f64(parsed, scale)
}

/// 业务作用：将有限 f64 按 scale 放大并以 `floor(x + 0.5)` 转换为定点 i128。
/// 半数朝正无穷；NaN 与无穷不能作为有效金额。
/// 返回：目标 scale 的定点值；非法 scale、非有限数或越界时返回错误。
///
/// # 参数
///
/// - `val`: 待转换的有限 f64 数值。
/// - `scale`: 目标定点小数位数，必须不超过 [`crate::MAX_SCALE`]。
pub fn to_fixed_f64(val: f64, scale: u32) -> Result<i128> {
    check_scale(scale)?;
    if !val.is_finite() {
        return Err(NumericError::Parse(format!("非有限 f64: {val}")));
    }
    let scaled = compat_math_round(val * pow10(scale) as f64);
    //**正向边界必须用 `>= 2^127`**,不能用 `> i128::MAX as f64`——`i128::MAX`(=2^127-1)无法被
    // f64 精确表示,`i128::MAX as f64` 向上舍入成 `2^127`,于是 `scaled == 2^127` 漏过判断、`as i128` 饱和成
    // i128::MAX(把越界值静默变最大合法值)。`-(i128::MIN as f64)` 正好 = 2^127(i128::MIN=-2^127 可精确表示)。
    let positive_overflow_cutoff = -(i128::MIN as f64); // 2^127
    if !scaled.is_finite() || scaled < i128::MIN as f64 || scaled >= positive_overflow_cutoff {
        return Err(NumericError::Overflow);
    }
    Ok(scaled as i128)
}

/// 业务作用: 定点 i128 → 易读字符串,**自动去尾零**,整数不带小数点。
///
/// 纯整数算术,无 BigDecimal/f64。例:
/// `to_plain_string(123456789, 8) = "1.23456789"`、`(120000000,8)="1.2"`、`(100000000,8)="1"`、
/// `(-100,8)="-0.000001"`、`(50,8)="0.0000005"`、`(0,8)="0"`。
///
/// # 参数
///
/// - `fixed`: 待展示的定点 mantissa。
/// - `scale`: `fixed` 当前使用的小数位数。
pub fn to_plain_string(fixed: i128, scale: u32) -> Result<String> {
    check_scale(scale)?;
    if scale == 0 {
        return Ok(fixed.to_string());
    }
    let neg = fixed < 0;
    let abs = fixed.unsigned_abs(); // u128
    let unit = pow10(scale) as u128;
    let int_part = abs / unit;
    let frac_part = abs % unit;

    if frac_part == 0 {
        let sign = if neg && int_part != 0 {
            "-"
        } else {
            ""
        };
        return Ok(format!("{sign}{int_part}"));
    }
    // 小数部分补零到 scale 宽,去尾零。
    let mut frac_str = format!("{:0>width$}", frac_part, width = scale as usize);
    while frac_str.ends_with('0') {
        frac_str.pop();
    }
    let sign = if neg {
        "-"
    } else {
        ""
    };
    Ok(format!("{sign}{int_part}.{frac_str}"))
}

// `decimal::scale_min` 返回 BigDecimal，不受定点八位小数上限限制。

// ==================== 默认精度重载(不传 scale → DEFAULT_SCALE=8)====================
// 不传 scale 默认 8。Rust 无重载,故以 `_default` 后缀区分。

/// 业务作用: `to_fixed_str` 默认精度(scale=8)。
///
/// # 参数
///
/// - `val`: 待转换的十进制或科学计数法金额文本。
pub fn to_fixed_str_default(val: &str) -> Result<i128> {
    to_fixed_str(val, crate::DEFAULT_SCALE)
}

/// 业务作用: `to_fixed_f64` 默认精度(scale=8)。
///
/// # 参数
///
/// - `val`: 待转换的有限 f64 数值。
pub fn to_fixed_f64_default(val: f64) -> Result<i128> {
    to_fixed_f64(val, crate::DEFAULT_SCALE)
}

/// 业务作用: `to_plain_string` 默认精度(scale=8)。
///
/// # 参数
///
/// - `fixed`: 默认 8 位精度的定点 mantissa。
pub fn to_plain_string_default(fixed: i128) -> Result<String> {
    to_plain_string(fixed, crate::DEFAULT_SCALE)
}

// ==================== double → 字符串(中转定点防漂移)====================

/// 业务作用: f64 → 字符串,自动去尾零(先 round 到定点再转,避开 f64 表示噪音)。
/// 例:`to_plain_string_f64(0.30000000000000004, 8) = "0.3"`。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
/// - `scale`: 中转定点和展示使用的小数位数。
pub fn to_plain_string_f64(val: f64, scale: u32) -> Result<String> {
    to_plain_string(to_fixed_f64(val, scale)?, scale)
}

/// 业务作用: f64 → 字符串,默认精度(scale=8),去尾零。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
pub fn to_plain_string_f64_default(val: f64) -> Result<String> {
    to_plain_string_f64(val, crate::DEFAULT_SCALE)
}

// ==================== 保尾零 / 定点-显示精度分离 ====================

/// 业务作用: 定点 i128 → 字符串,**保留所有尾零**(固定 scale 位小数)。
/// 例:`to_plain_string_raw(120000000, 8) = "1.20000000"`、`(100, 8) = "0.00000100"`。
///
/// # 参数
///
/// - `fixed`: 待展示的定点 mantissa。
/// - `scale`: `fixed` 当前使用的小数位数，也是输出保留的小数位数。
pub fn to_plain_string_raw(fixed: i128, scale: u32) -> Result<String> {
    check_scale(scale)?;
    let neg = fixed < 0;
    let abs = fixed.unsigned_abs();
    let unit = pow10(scale) as u128;
    let int_part = abs / unit;
    let frac_part = abs % unit;
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(&int_part.to_string());
    if scale > 0 {
        s.push('.');
        s.push_str(&format!("{:0>w$}", frac_part, w = scale as usize));
    }
    Ok(s)
}

/// 业务作用：按独立显示精度格式化定点数并保留尾零。
/// `display_scale <= 0` 统一显示四舍五入后的整数，不把负显示精度解释为十、百等量级。
/// 所有输入使用同一整数舍入规则，包括最小有符号值。
/// 返回：指定显示位数的字符串；定点或显示精度不合法时返回错误。
///
/// # 参数
///
/// - `fixed`: 待展示的定点 mantissa。
/// - `fixed_scale`: `fixed` 当前使用的小数位数。
/// - `display_scale`: 输出时保留的小数位数；`<= 0` 表示只显示整数。
pub fn to_plain_string_raw_display(
    fixed: i128,
    fixed_scale: u32,
    display_scale: i32,
) -> Result<String> {
    if display_scale == fixed_scale as i32 {
        return to_plain_string_raw(fixed, fixed_scale);
    }
    check_scale(fixed_scale)?;
    let m = pow10(fixed_scale) as u128;
    let neg = fixed < 0;
    let abs = fixed.unsigned_abs();
    let mut int_part = abs / m;
    let frac_part = abs % m;
    let sign = if neg {
        "-"
    } else {
        ""
    };
    if display_scale <= 0 {
        let rounded = if frac_part * 2 >= m {
            int_part + 1
        } else {
            int_part
        };
        return Ok(format!("{sign}{rounded}"));
    }
    let display_scale = display_scale as u32; // 此后必 > 0
    let frac_str = if display_scale >= fixed_scale {
        let mut s = format!("{:0>w$}", frac_part, w = fixed_scale as usize);
        s.push_str(&"0".repeat((display_scale - fixed_scale) as usize));
        s
    } else {
        let divisor = pow10(fixed_scale - display_scale) as u128;
        let mut truncated = (frac_part + divisor / 2) / divisor;
        let display_m = pow10(display_scale) as u128;
        if truncated >= display_m {
            int_part += 1;
            truncated = 0;
        }
        format!("{:0>w$}", truncated, w = display_scale as usize)
    };
    Ok(format!("{sign}{int_part}.{frac_str}"))
}

/// 业务作用：按独立显示精度格式化定点数并删除小数尾零。
/// `display_scale <= 0` 统一显示四舍五入后的整数。负值舍入为零时保留负号，例如
/// `to_plain_string_display(-40_000_000, 8, 0)` 返回 `"-0"`；需要普通零时由展示层归一。
/// 返回：格式化字符串；精度不合法时返回错误。
///
/// # 参数
///
/// - `fixed`: 待展示的定点 mantissa。
/// - `fixed_scale`: `fixed` 当前使用的小数位数。
/// - `display_scale`: 输出时最多保留的小数位数；`<= 0` 表示只显示整数。
pub fn to_plain_string_display(
    fixed: i128,
    fixed_scale: u32,
    display_scale: i32,
) -> Result<String> {
    if display_scale == fixed_scale as i32 {
        return to_plain_string(fixed, fixed_scale);
    }
    check_scale(fixed_scale)?;
    let m = pow10(fixed_scale) as u128;
    let neg = fixed < 0;
    let abs = fixed.unsigned_abs();
    let mut int_part = abs / m;
    let mut frac_part = abs % m;
    let sign = if neg {
        "-"
    } else {
        ""
    };
    if display_scale <= 0 || frac_part == 0 {
        let rounded = if display_scale <= 0 && frac_part * 2 >= m {
            int_part + 1
        } else {
            int_part
        };
        return Ok(format!("{sign}{rounded}"));
    }
    let display_scale = display_scale as u32; // 此后必 > 0
    let mut effective_scale = if display_scale < fixed_scale {
        display_scale
    } else {
        fixed_scale
    };
    if display_scale < fixed_scale {
        let divisor = pow10(fixed_scale - display_scale) as u128;
        frac_part = (frac_part + divisor / 2) / divisor;
        let display_m = pow10(display_scale) as u128;
        if frac_part >= display_m {
            int_part += 1;
            frac_part = 0;
        }
    }
    if frac_part == 0 {
        return Ok(format!("{sign}{int_part}"));
    }
    while frac_part.is_multiple_of(10) {
        frac_part /= 10;
        effective_scale -= 1;
    }
    let frac_str = format!("{:0>w$}", frac_part, w = effective_scale as usize);
    Ok(format!("{sign}{int_part}.{frac_str}"))
}

/// 业务作用: f64 → 字符串,保尾零(中转定点)。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
/// - `scale`: 中转定点和输出保留的小数位数。
pub fn to_plain_string_raw_f64(val: f64, scale: u32) -> Result<String> {
    to_plain_string_raw(to_fixed_f64(val, scale)?, scale)
}

/// 业务作用: f64 → 字符串,定点/显示精度分离,保尾零(中转定点)。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
/// - `fixed_scale`: f64 中转定点时使用的小数位数。
/// - `display_scale`: 输出时保留的小数位数；`<= 0` 表示只显示整数。
pub fn to_plain_string_raw_f64_display(
    val: f64,
    fixed_scale: u32,
    display_scale: i32,
) -> Result<String> {
    to_plain_string_raw_display(to_fixed_f64(val, fixed_scale)?, fixed_scale, display_scale)
}

/// 业务作用: f64 → 字符串,定点/显示精度分离,去尾零(中转定点)。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
/// - `fixed_scale`: f64 中转定点时使用的小数位数。
/// - `display_scale`: 输出时最多保留的小数位数；`<= 0` 表示只显示整数。
pub fn to_plain_string_f64_display(
    val: f64,
    fixed_scale: u32,
    display_scale: i32,
) -> Result<String> {
    to_plain_string_display(to_fixed_f64(val, fixed_scale)?, fixed_scale, display_scale)
}

// ==================== 定点 i128 → BigDecimal ====================

/// 业务作用: 定点 i128 → [`BigDecimal`](bigdecimal::BigDecimal)(真需 BigDecimal 入参的外部 API 时用)。
/// 结果值为 `fixed × 10^-scale`，保留指定 scale。
///
/// # 参数
///
/// - `fixed`: 待转换的定点 mantissa。
/// - `scale`: `fixed` 当前使用的小数位数。
pub fn to_big_decimal(fixed: i128, scale: u32) -> Result<bigdecimal::BigDecimal> {
    check_scale(scale)?;
    // fixed 已经是定点系数，直接设置系数与 scale 可避免额外除法和舍入。
    Ok(bigdecimal::BigDecimal::new(fixed.into(), scale as i64))
}

/// 业务作用: 定点 i128 → `BigDecimal`,默认精度(scale=8)。
///
/// # 参数
///
/// - `fixed`: 默认 8 位精度的定点 mantissa。
pub fn to_big_decimal_default(fixed: i128) -> Result<bigdecimal::BigDecimal> {
    to_big_decimal(fixed, crate::DEFAULT_SCALE)
}
