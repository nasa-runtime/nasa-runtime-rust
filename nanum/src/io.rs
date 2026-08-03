//! I/O 边界:字符串 / f64 → 定点,定点 → 字符串。

use crate::{check_scale, pow10, NumericError, Result};

/// 业务作用: 原实现 `Math.round(double)` = `floor(x + 0.5)`,**ties 朝 +∞**(非远离零)。
/// 用于 `to_fixed_str`/`to_fixed_f64`，兼容原实现 `Numeric.toFixed`。
/// **注意**:这与 `multiply/divide` 的 `roundHalfUp`(ties away-from-zero)是**两套**——原实现 本就不一致。
pub(crate) fn compat_math_round(x: f64) -> f64 {
    (x + 0.5).floor()
}

/// 业务作用: 将十进制或科学计数法字符串转换为定点 i128，在该解析子集内兼容原实现 `Numeric.toFixed(String,scale)` 的
/// `Math.round(Double.parseDouble(val) × 10^scale)` 语义,**经 f64**(故受 f64 53 位尾数限制,与 原实现 一致)。
/// **非** 原实现 `Double.parseDouble` 全语言全集(见下「解析语言偏离」)。
///
/// 例:`to_fixed_str("123.456789", 8) = 12345678900`、`to_fixed_str("-0.000001", 8) = -100`。
///
/// **解析语言偏离(非 原实现 `Double.parseDouble` 全集)**:这里用 Rust `str::parse::<f64>()`,只接受
/// **十进制 / 科学计数法**子集;原实现 `Double.parseDouble` 额外接受的**十六进制浮点字面量**(如 `"0x1.0p0"`)
/// 在此返 `Err`。业务若只传普通十进制金额字符串则无影响。非有限值(`"Infinity"/"NaN"`)亦返
/// `Err`(同 [`to_fixed_f64`] 的有意安全偏离)。
///
/// **f64 精度上限(逐值复刻 原实现 `parseDouble + Math.round` 的固有代价)**:字符串先过 f64,
/// 有效数字 **≥16 位**开始失真(如 `"‑99999999.99999999"` 会得到 `...98` 的定点值);超过 `scale`
/// 位的小数被**静默舍入**而非报错(如 `to_fixed_str("0.000000001", 8) = Ok(0)`)。撮合常见的
/// 价格/数量(≤15 位有效数字)不受影响;需要任意精度的**精确**字符串解析请改用 [`crate::decimal`]
/// 模块(BigDecimal 路径)。
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

/// 业务作用: 将有限 f64 转换为定点 i128，兼容原实现 `Numeric.toFixed(double,scale)` 的 `Math.round(val × 10^scale)` 规则，
/// **ties 朝 +∞**(`floor(x+0.5)`,非远离零)。
///
/// **NaN/Infinity 返 `Err`(有意安全偏离,非逐值复刻 bug)**——原实现 `Math.round(NaN)=0`、
/// `Math.round(±Inf)=Long.MIN/MAX`,但金融/撮合库不应把非有限值悄悄当合法数,故 Rust 拒绝。
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

/// 业务作用: 定点 i128 → 易读字符串,**自动去尾零**,整数不带小数点。对照 原实现 `Numeric.toPlainString`。
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
        let sign = if neg && int_part != 0 { "-" } else { "" };
        return Ok(format!("{sign}{int_part}"));
    }
    // 小数部分补零到 scale 宽,去尾零。
    let mut frac_str = format!("{:0>width$}", frac_part, width = scale as usize);
    while frac_str.ends_with('0') {
        frac_str.pop();
    }
    let sign = if neg { "-" } else { "" };
    Ok(format!("{sign}{int_part}.{frac_str}"))
}

// 注:`scale_min` 已移到 `decimal` 模块(走 BigDecimal,无 scale≤8 上限,对照 原实现 返 BigDecimal)。

// ==================== 默认精度重载(不传 scale → DEFAULT_SCALE=8)====================
// 不传 scale 默认 8。Rust 无重载,故以 `_default` 后缀区分。

/// 业务作用: `to_fixed_str` 默认精度(scale=8)。对照 原实现 `Numeric.toFixed(String)`。
///
/// # 参数
///
/// - `val`: 待转换的十进制或科学计数法金额文本。
pub fn to_fixed_str_default(val: &str) -> Result<i128> {
    to_fixed_str(val, crate::DEFAULT_SCALE)
}

/// 业务作用: `to_fixed_f64` 默认精度(scale=8)。对照 原实现 `Numeric.toFixed(double)`。
///
/// # 参数
///
/// - `val`: 待转换的有限 f64 数值。
pub fn to_fixed_f64_default(val: f64) -> Result<i128> {
    to_fixed_f64(val, crate::DEFAULT_SCALE)
}

/// 业务作用: `to_plain_string` 默认精度(scale=8)。对照 原实现 `Numeric.toPlainString(long)`。
///
/// # 参数
///
/// - `fixed`: 默认 8 位精度的定点 mantissa。
pub fn to_plain_string_default(fixed: i128) -> Result<String> {
    to_plain_string(fixed, crate::DEFAULT_SCALE)
}

// ==================== double → 字符串(中转定点防漂移)====================

/// 业务作用: f64 → 字符串,自动去尾零(先 round 到定点再转,避开 f64 表示噪音)。对照 原实现 `Numeric.toPlainString(double,int)`。
/// 例:`to_plain_string_f64(0.30000000000000004, 8) = "0.3"`。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
/// - `scale`: 中转定点和展示使用的小数位数。
pub fn to_plain_string_f64(val: f64, scale: u32) -> Result<String> {
    to_plain_string(to_fixed_f64(val, scale)?, scale)
}

/// 业务作用: f64 → 字符串,默认精度(scale=8),去尾零。对照 原实现 `Numeric.toPlainString(double)`。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
pub fn to_plain_string_f64_default(val: f64) -> Result<String> {
    to_plain_string_f64(val, crate::DEFAULT_SCALE)
}

// ==================== 保尾零 / 定点-显示精度分离 ====================

/// 业务作用: 定点 i128 → 字符串,**保留所有尾零**(固定 scale 位小数)。对照 原实现 `Numeric.toPlainStringRaw(long,int)`。
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

/// 业务作用: 定点 i128 → 字符串,**定点精度与显示精度分离,保尾零**(显示位四舍五入)。
/// 对照 原实现 `Numeric.toPlainStringRaw(long,int,int)`。例:`to_plain_string_raw_display(123456789, 8, 4) = "1.2346"`。
///
/// `display_scale` 为有符号 `i32`(对照 原实现 `int`):**`<= 0` 一律只显示四舍五入后的整数**
/// (原实现 的 `displayScale <= 0` 普通分支不使用其量级,故 `-1/-2/...` 与 `0` 同效)。
///
/// **已知与 原实现 的一处偏离(`fixed == i64::MIN` 且 `display_scale < 0`)**:原实现 对 `Long.MIN_VALUE` 走
/// BigDecimal 特殊路径(`setScale(displayScale,HALF_UP)`,**尊重负量级**),与其自身普通分支不一致
/// (`MIN,ds=-1 → "-92233720370"` 但相邻的 `MIN+1,ds=-1 → "-92233720369"`)。**已决策:不复刻** 原实现 这个单
/// 魔法值特殊路径(那是 原实现 为绕开 long 取负溢出而加、顺带导致的自身不自洽),本 crate 保持**一致的整数舍入**。
/// 调用方如需逐位复刻该单点行为,应在业务层单独处理该值。
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
    let sign = if neg { "-" } else { "" };
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

/// 业务作用: 定点 i128 → 字符串,**定点精度与显示精度分离,去尾零**(显示位四舍五入)。
/// 对照 原实现 `Numeric.toPlainString(long,int,int)`。例:`to_plain_string_display(8075697000000, 8, 2) = "80756.97"`。
///
/// `display_scale` 为有符号 `i32`:**`<= 0` 一律只显示四舍五入后的整数**(同 原实现 `displayScale <= 0` 普通分支)。
/// 同 [`to_plain_string_raw_display`]:`fixed == i64::MIN` 且 `display_scale < 0` 时与 原实现 `Long.MIN_VALUE`
/// 特殊路径有一处已知偏离(原实现 自身普通/特殊分支即不一致),**已决策不复刻**,本 crate 保持一致整数舍入。
///
/// **已知复刻行为:负值舍到 0 输出 `"-0"`**(如 `to_plain_string_display(-40_000_000, 8, 0) = "-0"`)——
/// 逐值复刻 原实现 `neg ? "-" + rounded : ...`(原版无 `rounded == 0` 守卫);与两参
/// [`to_plain_string`](去尾零版,有 `-0` 守卫)不一致源于历史实现自身。展示层不接受 `-0` 时请自行归一。
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
    let sign = if neg { "-" } else { "" };
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

/// 业务作用: f64 → 字符串,保尾零(中转定点)。对照 原实现 `Numeric.toPlainStringRaw(double,int)`。
///
/// # 参数
///
/// - `val`: 待展示的有限 f64 数值。
/// - `scale`: 中转定点和输出保留的小数位数。
pub fn to_plain_string_raw_f64(val: f64, scale: u32) -> Result<String> {
    to_plain_string_raw(to_fixed_f64(val, scale)?, scale)
}

/// 业务作用: f64 → 字符串,定点/显示精度分离,保尾零(中转定点)。对照 原实现 `Numeric.toPlainStringRaw(double,int,int)`。
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

/// 业务作用: f64 → 字符串,定点/显示精度分离,去尾零(中转定点)。对照 原实现 `Numeric.toPlainString(double,int,int)`。
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
/// 对照 原实现 `Numeric.toBigDecimal(long,int)`:值 = `fixed × 10^-scale`,scale 位精度。
///
/// # 参数
///
/// - `fixed`: 待转换的定点 mantissa。
/// - `scale`: `fixed` 当前使用的小数位数。
pub fn to_big_decimal(fixed: i128, scale: u32) -> Result<bigdecimal::BigDecimal> {
    check_scale(scale)?;
    // 原实现 `BigDecimal.valueOf(fixed).divide(10^scale, scale, HALF_UP)` 等价于直接构造
    // unscaled=fixed、scale=scale 的 BigDecimal(fixed 本就是定点 mantissa,无需除法/舍入)。
    Ok(bigdecimal::BigDecimal::new(fixed.into(), scale as i64))
}

/// 业务作用: 定点 i128 → `BigDecimal`,默认精度(scale=8)。对照 原实现 `Numeric.toBigDecimal(long)`。
///
/// # 参数
///
/// - `fixed`: 默认 8 位精度的定点 mantissa。
pub fn to_big_decimal_default(fixed: i128) -> Result<bigdecimal::BigDecimal> {
    to_big_decimal(fixed, crate::DEFAULT_SCALE)
}
