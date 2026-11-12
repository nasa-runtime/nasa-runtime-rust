//! I/O 边界:字符串 / f64 → 定点,定点 → 字符串。

use crate::{check_scale, pow10, NumericError, Result};

/// 业务作用：按 `floor(x + 0.5)` 为定点输入取整，半数朝正无穷。
///
/// 参数说明：
/// - `x`：为缩放后的浮点值。
///
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
/// 参数说明：
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
///
/// 返回：目标 scale 的定点值；非法 scale、非有限数或越界时返回错误。
///
/// 参数说明：
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

/// 业务作用：用纯整数格式化定点系数并去掉小数尾零。
///
/// 参数说明：
/// - `fixed`：待格式化的有符号定点系数。
/// - `scale`：原定点精度，不超过 MAX_SCALE。
///
/// 返回：返回不带多余小数点及尾零的文本；非法精度返回 Scale，支持 i128 最小值。
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

/// 业务作用：按默认八位精度把数字文本经 f64 转为定点。
///
/// 参数说明：
/// - `val`：十进制或科学计数法文本，允许首尾空白。
///
/// 返回：返回八位定点系数；解析、非有限值或范围失败返回错误，长文本仍受 f64 中转精度限制。
pub fn to_fixed_str_default(val: &str) -> Result<i128> {
    to_fixed_str(val, crate::DEFAULT_SCALE)
}

/// 业务作用：按默认八位精度把有限浮点转换为定点。
///
/// 参数说明：
/// - `val`：待转换的有限 f64。
///
/// 返回：返回半数朝正无穷取整后的系数；非有限或超出 i128 范围时返回错误。
pub fn to_fixed_f64_default(val: f64) -> Result<i128> {
    to_fixed_f64(val, crate::DEFAULT_SCALE)
}

/// 业务作用：按默认八位精度格式化定点系数并删除尾零。
///
/// 参数说明：
/// - `fixed`：八位精度的有符号定点系数。
///
/// 返回：返回精确十进制文本，整数不附加小数点。
pub fn to_plain_string_default(fixed: i128) -> Result<String> {
    to_plain_string(fixed, crate::DEFAULT_SCALE)
}

// ==================== double → 字符串(中转定点防漂移)====================

/// 业务作用：先按目标精度量化浮点，再格式化并去尾零。
///
/// 参数说明：
/// - `val`：待量化的有限 f64。
/// - `scale`：中转及格式化精度，不超过 MAX_SCALE。
///
/// 返回：返回量化后的十进制文本；非法精度、非有限输入或范围溢出返回错误。
pub fn to_plain_string_f64(val: f64, scale: u32) -> Result<String> {
    to_plain_string(to_fixed_f64(val, scale)?, scale)
}

/// 业务作用：用默认八位定点中转格式化浮点并去尾零。
///
/// 参数说明：
/// - `val`：待量化的有限 f64。
///
/// 返回：返回量化后的十进制文本；非有限输入或范围溢出返回错误。
pub fn to_plain_string_f64_default(val: f64) -> Result<String> {
    to_plain_string_f64(val, crate::DEFAULT_SCALE)
}

// ==================== 保尾零 / 定点-显示精度分离 ====================

/// 业务作用：按原精度格式化定点系数并保留小数尾零。
///
/// 参数说明：
/// - `fixed`：待展示的有符号定点系数。
/// - `scale`：原定点精度及输出小数位数，不超过 MAX_SCALE。
///
/// 返回：返回固定小数位文本，scale 为零时只显示整数；非法精度返回 Scale。
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

/// 业务作用：以独立显示精度舍入定点数并保留输出尾零。
///
/// 参数说明：
/// - `fixed`：按 fixed_scale 表示的有符号系数。
/// - `fixed_scale`：原定点精度，不超过 MAX_SCALE。
/// - `display_scale`：显示小数位数，非正值显示整数；正值没有独立分配上限。
///
/// 返回：返回按幅值半数进位的文本；负值舍入为零时仍可显示负零。仅原精度非法返回 Scale，超大显示精度可能要求巨大字符串分配。
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

/// 业务作用：以独立显示精度舍入定点数并移除小数尾零。
///
/// 参数说明：
/// - `fixed`：按 fixed_scale 表示的有符号系数。
/// - `fixed_scale`：原定点精度，不超过 MAX_SCALE。
/// - `display_scale`：显示最多保留的小数位数，非正值统一显示整数。
///
/// 返回：返回按幅值半数进位后的文本，负值舍入到零可保留负号；原精度非法返回 Scale。高于原精度不额外补零。
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

/// 业务作用：把浮点量化为定点后按原精度保留尾零。
///
/// 参数说明：
/// - `val`：待量化的有限 f64。
/// - `scale`：中转及固定输出精度，不超过 MAX_SCALE。
///
/// 返回：返回固定小数位文本；精度、有限性或范围检查失败返回错误。
pub fn to_plain_string_raw_f64(val: f64, scale: u32) -> Result<String> {
    to_plain_string_raw(to_fixed_f64(val, scale)?, scale)
}

/// 业务作用：先量化浮点，再用独立显示精度格式化并保留尾零。
///
/// 参数说明：
/// - `val`：待量化的有限 f64。
/// - `fixed_scale`：中转定点精度，不超过 MAX_SCALE。
/// - `display_scale`：显示精度，非正值显示整数，正值没有独立分配上限。
///
/// 返回：返回舍入后的文本；量化或原精度失败返回错误，超大显示精度可能要求巨大分配。
pub fn to_plain_string_raw_f64_display(
    val: f64,
    fixed_scale: u32,
    display_scale: i32,
) -> Result<String> {
    to_plain_string_raw_display(to_fixed_f64(val, fixed_scale)?, fixed_scale, display_scale)
}

/// 业务作用：先量化浮点，再用独立显示精度格式化并去尾零。
///
/// 参数说明：
/// - `val`：待量化的有限 f64。
/// - `fixed_scale`：中转定点精度，不超过 MAX_SCALE。
/// - `display_scale`：最多显示的小数位数，非正值显示整数。
///
/// 返回：返回舍入后的文本，可保留负零；量化或原精度检查失败返回错误，不补超出原精度的尾零。
pub fn to_plain_string_f64_display(
    val: f64,
    fixed_scale: u32,
    display_scale: i32,
) -> Result<String> {
    to_plain_string_display(to_fixed_f64(val, fixed_scale)?, fixed_scale, display_scale)
}

// ==================== 定点 i128 → BigDecimal ====================

/// 业务作用：把定点系数和原精度直接组合成十进制数。
///
/// 参数说明：
/// - `fixed`：作为 BigDecimal 整数系数的有符号值。
/// - `scale`：原定点小数位数，不超过 MAX_SCALE。
///
/// 返回：返回精确的 fixed 乘以 10 的负 scale 次幂并保留 scale；非法精度返回 Scale，不经浮点或舍入。
pub fn to_big_decimal(fixed: i128, scale: u32) -> Result<bigdecimal::BigDecimal> {
    check_scale(scale)?;
    // fixed 已经是定点系数，直接设置系数与 scale 可避免额外除法和舍入。
    Ok(bigdecimal::BigDecimal::new(fixed.into(), scale as i64))
}

/// 业务作用：把默认八位定点系数转换为十进制数。
///
/// 参数说明：
/// - `fixed`：八位精度的有符号定点系数。
///
/// 返回：返回系数相同且 scale 为八的精确 BigDecimal。
pub fn to_big_decimal_default(fixed: i128) -> Result<bigdecimal::BigDecimal> {
    to_big_decimal(fixed, crate::DEFAULT_SCALE)
}
