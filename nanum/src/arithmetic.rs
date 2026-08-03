//! 定点算术:加减乘除 + 精度对齐 + 全 RoundingMode。
//!
//! **既定设计**:`multiply`/`divide` 的算法**必须与 原实现 `Numeric` 一致**——
//! 整数部分纯整数(`a/m*b` / `a/b*m`),**余数部分走 `f64`**(对照 原实现 L784/L813 的 `(double)` 中转),
//! 逐值复刻 原实现(含其 f64 量化)。`add/subtract/align*` 是纯整数(原实现 也是),不变。

use crate::{check_scale, check_scale_pair, pow10, NumericError, Result, RoundingMode};

/// 业务作用: 余数 f64 → i128,**逐位复刻 原实现 `Numeric.roundHalfUp`(L731)** = `f>=0 ? floor(f+0.5) : -floor(-f+0.5)`。
///
/// ⚠ **必须直接算 `floor(f+0.5)`,不能拆成 `floor(f)+(diff>=0.5)`**:大 `f`(超 f64 半整数域)时
/// `f+0.5` 在 f64 里会进位(如 `8888888799999999.0 + 0.5` 舍入到 `8888888800000000.0`),
/// 二者差 1。这正是 原实现 `roundHalfUp`(默认 multiply/divide)与 `applyRounding`(mode 重载)在大值**互不相等**的根源——
/// 原实现 本就如此,故默认路径走本函数、mode 路径走 [`apply_rounding_f64`],**分别保真**。
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

/// 业务作用: 余数 f64 → i128 舍入(全 8 RoundingMode),逐分支复刻 原实现 `Numeric.applyRounding(double,mode,long)`(L744)。
/// 返回**舍入加数**(调用方再加到 `int_part`);HALF_EVEN 的 tie 方向用 `int_part` 定奇偶。
///
/// ⚠ HALF_UP 分支用 `floor(abs)`+`diff` 判定,与 [`round_half_up_f64`] 的 `floor(f+0.5)` **大值不等**——
/// 见上;这是 原实现 自身在默认 vs mode 路径的不一致,**照搬不修正**。
///
/// # 参数
/// - `frac`: 小数部分数值。
/// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
/// - `int_part`: 整数部分数值。
fn apply_rounding_f64(frac: f64, mode: RoundingMode, int_part: i128) -> Result<i128> {
    if frac == 0.0 {
        return Ok(0);
    }
    let neg = frac < 0.0;
    let abs = if neg { -frac } else { frac };
    let floor_f = abs.floor();
    let floor_i = floor_f as i128;
    let diff = abs - floor_f;
    if diff == 0.0 {
        return Ok(if neg { -floor_i } else { floor_i });
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
                // tie:选 abs_result 让 (int_part ± floor) 为偶数(对照 原实现 candidate&1)。
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
    Ok(if neg { -abs_result } else { abs_result })
}

// ==================== 加 / 减(同 scale,纯整数 checked)====================

/// 业务作用: 两个**同 scale** 定点数相加。溢出返 `Err`。对照 原实现 `Numeric` long 加(`a+b`)。
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

/// 业务作用: 两个定点数相乘,结果仍定点(同 scale),默认 **HalfUp**。**逐值复刻 原实现 `Numeric.multiply(a,b,scale)`**(L784)。
///
/// 算法 = `a/m*b + roundHalfUp((double)(a%m)/m*b)`(整数部分整数、**余数部分 f64**)。
///
/// 例:`multiply(6000_0000_0000_00, 1000_0000, 8) = 600_0000_0000_00`(6000 × 0.1 = 600)。
///
/// **注意**:默认走 原实现 `roundHalfUp`,与显式 `multiply_rounding(.., HalfUp)`(走 `applyRounding`)在
/// 大操作数下**可能差 1**——这是 原实现 自身的不一致,本 crate 照搬(见私有 `round_half_up_f64`)。
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

/// 业务作用: 定点乘,指定 [`RoundingMode`]。对照 原实现 `multiply(a,b,scale,mode)`(余数项走 f64 + `applyRounding`)。
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
    // 整数部分:原实现 `a/m*b`(整数截断)。i128 不复刻 原实现 long 的溢出回绕——超域返 Err。
    let int_part = (a / m).checked_mul(b).ok_or(NumericError::Overflow)?;
    // 余数部分:原实现 `(double)(a%m)/m*b`(f64 中转)。
    let frac = (a % m) as f64 / m as f64 * b as f64;
    let add = apply_rounding_f64(frac, mode, int_part)?;
    int_part.checked_add(add).ok_or(NumericError::Overflow)
}

/// 业务作用: 两个定点数相除,结果仍定点(同 scale),默认 **HalfUp**。**逐值复刻 原实现 `Numeric.divide(a,b,scale)`**(L813)。
///
/// 算法 = `a/b*m + roundHalfUp((double)(a%b)/b*m)`(整数部分整数、**余数部分 f64**)。
///
/// 例:`divide(600_0000_0000_00, 1000_0000, 8) = 6000_0000_0000_00`(600 / 0.1 = 6000)。
///
/// **注意**:默认走 原实现 `roundHalfUp`,与显式 `divide_rounding(.., HalfUp)`(走 `applyRounding`)在
/// 大操作数下**可能差 1**——原实现 自身的不一致,照搬。
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

/// 业务作用: 定点除,指定 [`RoundingMode`]。对照 原实现 `divide(a,b,scale,mode)`(余数项走 f64 + `applyRounding`)。
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

/// 业务作用: 定点乘,默认精度(scale=[`DEFAULT_SCALE`](crate::DEFAULT_SCALE)=8)。对照 原实现 `Numeric.multiply(long,long)`。
///
/// # 参数
///
/// - `a`: 左侧默认 8 位定点 mantissa。
/// - `b`: 右侧默认 8 位定点 mantissa。
pub fn multiply_default(a: i128, b: i128) -> Result<i128> {
    multiply(a, b, crate::DEFAULT_SCALE)
}

/// 业务作用: 定点除,默认精度(scale=8)。对照 原实现 `Numeric.divide(long,long)`。
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
/// 对照 原实现 `Numeric.align`。
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

/// 业务作用: 精度对齐,指定 [`RoundingMode`]。纯整数(除模),无 f64。对照 原实现 `Numeric.align(...,mode)`。
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

/// 业务作用: 向上对齐(CEILING,朝 +∞)。撮合别名,等价 `align_rounding(.., Ceiling)`。对照 原实现 `alignUp`。
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

/// 业务作用: 向下对齐(DOWN,朝零)。撮合别名,等价 `align_rounding(.., Down)`。对照 原实现 `alignDown`。
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

/// 业务作用: 定点数是否已对齐到 `to_scale`。对照 原实现 `Numeric.isAligned`。
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
