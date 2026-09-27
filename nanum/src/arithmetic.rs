//! 定点算术、精度对齐与舍入。
//!
//! 乘除的整数部分使用 checked i128，余数经 f64 中转，受浮点量化限制。
//! 加减和精度对齐使用纯整数运算；默认乘除与显式舍入模式保留各自的取整规则。

use crate::{check_scale, check_scale_pair, pow10, NumericError, Result, RoundingMode};

/// 业务作用：按符号应用 `floor(abs(f) + 0.5)`，生成默认乘除的浮点舍入加数。
///
/// 大数的 `abs(f) + 0.5` 可能在 f64 中提前进位，因此不能改写为先取 floor 再比较余数；
/// 该路径与显式 HalfUp 的结果可能相差 1。
/// 返回：保留输入符号的整数浮点值，调用方在累计到整数部分时检查溢出。
///
/// # 参数
/// - `f`: 需要按 half-up 规则舍入的浮点余数。
fn round_half_up_f64(f: f64) -> f64 {
    if f >= 0.0 {
        (f + 0.5).floor()
    } else {
        -((-f + 0.5).floor())
    }
}

/// 业务作用：按指定舍入模式把浮点余数转换为可加到整数部分的 i128 增量。
/// HalfEven 结合 `int_part` 判断最终奇偶；HalfUp 使用绝对值的 floor 与小数差，
/// 大数结果可能与默认乘除的直接加 0.5 路径相差 1。
/// 返回：舍入增量；Unnecessary 遇到非整数时返回错误。输入范围由定点乘除入口约束。
///
/// # 参数
/// - `frac`: 小数部分数值。
/// - `mode`: 浮点余数转为整数增量时使用的舍入规则。
/// - `int_part`: 整数部分数值。
fn apply_rounding_f64(frac: f64, mode: RoundingMode, int_part: i128) -> Result<i128> {
    if frac == 0.0 {
        return Ok(0);
    }
    let neg = frac < 0.0;
    let abs = if neg {
        -frac
    } else {
        frac
    };
    let floor_f = abs.floor();
    let floor_i = floor_f as i128;
    let diff = abs - floor_f;
    if diff == 0.0 {
        return Ok(if neg {
            -floor_i
        } else {
            floor_i
        });
    }
    let abs_result = match mode {
        RoundingMode::Up => floor_i + 1,
        RoundingMode::Down => floor_i,
        RoundingMode::Ceiling => {
            if neg {
                floor_i
            } else {
                floor_i + 1
            }
        }
        RoundingMode::Floor => {
            if neg {
                floor_i + 1
            } else {
                floor_i
            }
        }
        RoundingMode::HalfUp => {
            if diff < 0.5 {
                floor_i
            } else {
                floor_i + 1
            }
        }
        RoundingMode::HalfDown => {
            if diff <= 0.5 {
                floor_i
            } else {
                floor_i + 1
            }
        }
        RoundingMode::HalfEven => {
            if diff < 0.5 {
                floor_i
            } else if diff > 0.5 {
                floor_i + 1
            } else {
                // tie:选 abs_result 让 (int_part ± floor) 为偶数。
                let candidate = if neg {
                    int_part - floor_i
                } else {
                    int_part + floor_i
                };
                if candidate & 1 == 0 {
                    floor_i
                } else {
                    floor_i + 1
                }
            }
        }
        RoundingMode::Unnecessary => return Err(NumericError::RoundingNecessary),
    };
    Ok(if neg {
        -abs_result
    } else {
        abs_result
    })
}

// ==================== 加 / 减(同 scale,纯整数 checked)====================

/// 业务作用: 两个同 scale 的定点数相加，溢出返回 `Err`。
///
/// # 参数
///
/// - `a`: 左侧定点 mantissa，调用方需保证与 `b` 使用同一 scale。
/// - `b`: 右侧定点 mantissa，调用方需保证与 `a` 使用同一 scale。
pub fn add(a: i128, b: i128) -> Result<i128> {
    a.checked_add(b).ok_or(NumericError::Overflow)
}

/// 业务作用: 两个**同 scale** 定点数相减。溢出返 `Err`。
///
/// # 参数
///
/// - `a`: 被减数定点 mantissa，调用方需保证与 `b` 使用同一 scale。
/// - `b`: 减数定点 mantissa，调用方需保证与 `a` 使用同一 scale。
pub fn subtract(a: i128, b: i128) -> Result<i128> {
    a.checked_sub(b).ok_or(NumericError::Overflow)
}

// ==================== 乘 / 除(定点,默认 HalfUp)====================

/// 业务作用：将两个定点数相乘并保持输入 scale，默认使用带符号的 HalfUp。
/// 算法为 `a/m*b + round_half_up_f64((a%m)/m*b)`，其中 `m=10^scale`，余数项使用 f64。
/// 显式 `multiply_rounding(.., HalfUp)` 使用不同的浮点取整步骤，大操作数结果可能相差 1。
/// 返回：同 scale 的定点值；非法 scale、溢出或不合法运算返回错误。
///
/// # 参数
///
/// - `a`: 左侧定点 mantissa，真实值为 `a × 10^-scale`。
/// - `b`: 右侧定点 mantissa，真实值为 `b × 10^-scale`。
/// - `scale`: 两个输入和结果共用的小数位数。
pub fn multiply(a: i128, b: i128, scale: u32) -> Result<i128> {
    check_scale(scale)?;
    let m = pow10(scale);
    let int_part = (a / m).checked_mul(b).ok_or(NumericError::Overflow)?;
    let frac = (a % m) as f64 / m as f64 * b as f64;
    int_part
        .checked_add(round_half_up_f64(frac) as i128)
        .ok_or(NumericError::Overflow)
}

/// 业务作用：按显式 `RoundingMode` 将两个定点数相乘，余数经 f64 中转。
/// 返回：同 scale 的定点值；非法 scale、溢出或无法满足舍入要求时返回错误。
///
/// # 参数
///
/// - `a`: 左侧定点 mantissa，真实值为 `a × 10^-scale`。
/// - `b`: 右侧定点 mantissa，真实值为 `b × 10^-scale`。
/// - `scale`: 两个输入和结果共用的小数位数。
/// - `mode`: 余数项需要取整时使用的舍入策略。
pub fn multiply_rounding(a: i128, b: i128, scale: u32, mode: RoundingMode) -> Result<i128> {
    check_scale(scale)?;
    let m = pow10(scale);
    // 整数部分先截断再使用 checked 乘法，超出 i128 范围时返回错误，不能回绕为其它业务数值。
    let int_part = (a / m).checked_mul(b).ok_or(NumericError::Overflow)?;
    // 余数部分经 f64 中转，量化误差属于该乘除入口的数值边界。
    let frac = (a % m) as f64 / m as f64 * b as f64;
    let add = apply_rounding_f64(frac, mode, int_part)?;
    int_part.checked_add(add).ok_or(NumericError::Overflow)
}

/// 业务作用：将两个定点数相除并保持输入 scale，默认使用带符号的 HalfUp。
/// 算法为 `a/b*m + round_half_up_f64((a%b)/b*m)`，其中 `m=10^scale`，余数项使用 f64。
/// 显式 `divide_rounding(.., HalfUp)` 使用不同的浮点取整步骤，大操作数结果可能相差 1。
/// 返回：同 scale 的定点值；非法 scale、溢出或不合法运算返回错误。
///
/// # 参数
///
/// - `a`: 被除数定点 mantissa，真实值为 `a × 10^-scale`。
/// - `b`: 除数定点 mantissa，真实值为 `b × 10^-scale`；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 输入和结果共用的小数位数。
pub fn divide(a: i128, b: i128, scale: u32) -> Result<i128> {
    check_scale(scale)?;
    if b == 0 {
        return Err(NumericError::DivByZero);
    }
    let m = pow10(scale);
    //`i128::MIN / -1` 溢出会 **panic**(违反 crate"不 panic、返 Result"错误模型);
    // 用 checked_div/checked_rem,溢出 → Err(Overflow)。
    let q = a.checked_div(b).ok_or(NumericError::Overflow)?;
    let rem = a.checked_rem(b).ok_or(NumericError::Overflow)?;
    let int_part = q.checked_mul(m).ok_or(NumericError::Overflow)?;
    let frac = rem as f64 / b as f64 * m as f64;
    int_part
        .checked_add(round_half_up_f64(frac) as i128)
        .ok_or(NumericError::Overflow)
}

/// 业务作用：按显式 `RoundingMode` 将两个定点数相除，余数经 f64 中转。
/// 返回：同 scale 的定点值；非法 scale、溢出或无法满足舍入要求时返回错误。
///
/// # 参数
///
/// - `a`: 被除数定点 mantissa，真实值为 `a × 10^-scale`。
/// - `b`: 除数定点 mantissa，真实值为 `b × 10^-scale`；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 输入和结果共用的小数位数。
/// - `mode`: 余数项需要取整时使用的舍入策略。
pub fn divide_rounding(a: i128, b: i128, scale: u32, mode: RoundingMode) -> Result<i128> {
    check_scale(scale)?;
    if b == 0 {
        return Err(NumericError::DivByZero);
    }
    let m = pow10(scale);
    //同 `divide`,`i128::MIN / -1` 用 checked 避免 panic。
    let q = a.checked_div(b).ok_or(NumericError::Overflow)?;
    let rem = a.checked_rem(b).ok_or(NumericError::Overflow)?;
    let int_part = q.checked_mul(m).ok_or(NumericError::Overflow)?;
    let frac = rem as f64 / b as f64 * m as f64;
    let add = apply_rounding_f64(frac, mode, int_part)?;
    int_part.checked_add(add).ok_or(NumericError::Overflow)
}

/// 业务作用: 定点乘,默认精度(scale=[`DEFAULT_SCALE`](crate::DEFAULT_SCALE)=8)。
///
/// # 参数
///
/// - `a`: 左侧默认 8 位定点 mantissa。
/// - `b`: 右侧默认 8 位定点 mantissa。
pub fn multiply_default(a: i128, b: i128) -> Result<i128> {
    multiply(a, b, crate::DEFAULT_SCALE)
}

/// 业务作用: 定点除,默认精度(scale=8)。
///
/// # 参数
///
/// - `a`: 默认 8 位定点被除数 mantissa。
/// - `b`: 默认 8 位定点除数 mantissa；为 `0` 时返回 [`NumericError::DivByZero`]。
pub fn divide_default(a: i128, b: i128) -> Result<i128> {
    divide(a, b, crate::DEFAULT_SCALE)
}

// ==================== 精度对齐(纯整数,撮合 tick 对齐)====================

/// 业务作用: 定点数对齐到更低精度 `to_scale`(仍保持 `from_scale` 体系表示),默认 **HalfUp**。
///
/// 例:`align(2002_1365, 8, 4) = 2002_0000`(0.20021365 → 0.2002,仍 ×10^8)。
///
/// # 参数
///
/// - `fixed`: 待对齐的定点 mantissa，当前按 `from_scale` 表示。
/// - `from_scale`: `fixed` 当前使用的小数位数。
/// - `to_scale`: 目标小数位数，必须小于等于 `from_scale`。
pub fn align(fixed: i128, from_scale: u32, to_scale: u32) -> Result<i128> {
    align_rounding(fixed, from_scale, to_scale, RoundingMode::HalfUp)
}

/// 业务作用: 精度对齐,指定 [`RoundingMode`]。纯整数(除模),无 f64。
///
/// # 参数
///
/// - `fixed`: 待对齐的定点 mantissa，当前按 `from_scale` 表示。
/// - `from_scale`: `fixed` 当前使用的小数位数。
/// - `to_scale`: 目标小数位数，必须小于等于 `from_scale`。
/// - `mode`: 缩减精度时使用的舍入策略。
pub fn align_rounding(
    fixed: i128,
    from_scale: u32,
    to_scale: u32,
    mode: RoundingMode,
) -> Result<i128> {
    check_scale_pair(from_scale, to_scale)?;
    let unit = pow10(from_scale - to_scale);
    let rem = fixed % unit;
    if rem == 0 {
        return Ok(fixed);
    }
    let base = fixed - rem; // 朝零截断
    let neg = fixed < 0;
    let abs_rem = rem.unsigned_abs(); // ∈ (0, unit)
    let unit_u = unit.unsigned_abs();

    let add_unit = match mode {
        RoundingMode::Up => true,
        RoundingMode::Down => false,
        RoundingMode::Ceiling => !neg,
        RoundingMode::Floor => neg,
        RoundingMode::HalfUp => abs_rem * 2 >= unit_u,
        RoundingMode::HalfDown => abs_rem * 2 > unit_u,
        RoundingMode::HalfEven => match (abs_rem * 2).cmp(&unit_u) {
            core::cmp::Ordering::Less => false,
            core::cmp::Ordering::Greater => true,
            // tie:base/unit 偶不进位、奇进位 → 都到偶数。
            core::cmp::Ordering::Equal => (base / unit) & 1 != 0,
        },
        RoundingMode::Unnecessary => return Err(NumericError::RoundingNecessary),
    };

    if !add_unit {
        return Ok(base);
    }
    if neg {
        base.checked_sub(unit).ok_or(NumericError::Overflow)
    } else {
        base.checked_add(unit).ok_or(NumericError::Overflow)
    }
}

/// 业务作用: 向上对齐(CEILING,朝 +∞)。撮合别名,等价 `align_rounding(.., Ceiling)`。
///
/// 例:`align_up(2002_1365, 8, 4) = 2003_0000`。
///
/// # 参数
///
/// - `fixed`: 待向上对齐的定点 mantissa，当前按 `from_scale` 表示。
/// - `from_scale`: `fixed` 当前使用的小数位数。
/// - `to_scale`: 目标小数位数，必须小于等于 `from_scale`。
pub fn align_up(fixed: i128, from_scale: u32, to_scale: u32) -> Result<i128> {
    align_rounding(fixed, from_scale, to_scale, RoundingMode::Ceiling)
}

/// 业务作用: 向下对齐(DOWN,朝零)。撮合别名,等价 `align_rounding(.., Down)`。
///
/// 例:`align_down(2002_1365, 8, 4) = 2002_0000`。
///
/// # 参数
///
/// - `fixed`: 待向下对齐的定点 mantissa，当前按 `from_scale` 表示。
/// - `from_scale`: `fixed` 当前使用的小数位数。
/// - `to_scale`: 目标小数位数，必须小于等于 `from_scale`。
pub fn align_down(fixed: i128, from_scale: u32, to_scale: u32) -> Result<i128> {
    align_rounding(fixed, from_scale, to_scale, RoundingMode::Down)
}

/// 业务作用: 定点数是否已对齐到 `to_scale`。
///
/// # 参数
///
/// - `fixed`: 待检查的定点 mantissa，当前按 `from_scale` 表示。
/// - `from_scale`: `fixed` 当前使用的小数位数。
/// - `to_scale`: 需要检查是否已对齐到的小数位数。
pub fn is_aligned(fixed: i128, from_scale: u32, to_scale: u32) -> Result<bool> {
    check_scale_pair(from_scale, to_scale)?;
    let unit = pow10(from_scale - to_scale);
    Ok(fixed % unit == 0)
}
