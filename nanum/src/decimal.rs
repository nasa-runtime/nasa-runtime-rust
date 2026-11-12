//! BigDecimal 高精度计算。
//!
//! 本模块不受定点 `MAX_SCALE=8` 限制，按目标 scale 使用 checked BigInt 算术和整数舍入。
//! 除法不借用固定中间精度的浮点路径，指定高 scale 时仍保留对应十进制位数。
//! scale 对齐所需的十进制展开受 `MAX_DECIMAL_EXPANSION` 限制；越界返回 `Err(Scale)`。
//! 零值和可直接返回操作数的情况不需要展开。`scale_min` 与 `random_step` 只构造系数和 scale，
//! 后续算术仍受展开上限约束。
//!
//! 返回值是 `bigdecimal::BigDecimal`；数值、scale 和序列化文本是不同合同。需要固定科学计数法、
//! 尾零或跨语言文本一致性时，调用方必须显式选择格式化规则。

use crate::{NumericError, Result, RoundingMode};
use bigdecimal::BigDecimal;
use rand::Rng;
use std::str::FromStr;

/// BigDecimal 对齐时允许展开的十进制位数上限。
/// 公开 scale 使用 i64，极端差值可能请求无法容纳的 BigInt，因此在分配之前拒绝超界展开。
/// 零值与不需要展开的结构操作不消耗此额度。
pub const MAX_DECIMAL_EXPANSION: u64 = 1_000_000;

/// 业务作用：在十进制幂展开前检查允许的指数跨度。
///
/// 参数说明：
/// - `exp`：需要展开的非负十进制位数。
///
/// 返回：不超过 MAX_DECIMAL_EXPANSION 时返回可用于分配的指数，否则返回 Scale；不限制已有系数自身大小。
fn checked_decimal_exp(exp: u64) -> Result<usize> {
    if exp > MAX_DECIMAL_EXPANSION {
        return Err(NumericError::Scale(format!(
            "decimal 展开位数 {exp} 超上限 {MAX_DECIMAL_EXPANSION}(防 OOM)"
        )));
    }
    Ok(exp as usize)
}

/// 业务作用：构造已通过调用方范围检查的十进制幂。
///
/// 参数说明：
/// - `exp`：调用方已限制的非负整数指数。
///
/// 返回：返回精确的 10 的 exp 次幂；本函数不再次验证展开预算。
fn pow10_bigint(exp: usize) -> bigdecimal::num_bigint::BigInt {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::num_traits::pow::pow as int_pow;
    int_pow(BigInt::from(10), exp)
}

/// 业务作用：按结果符号和舍入策略对非负整数比值取整。
///
/// 参数说明：
/// - `num_abs`：被除数的非负幅值。
/// - `den_abs`：除数的正幅值，调用方须保证非零。
/// - `result_negative`：最终商是否应为负数。
/// - `mode`：存在余数时决定进位或拒绝的舍入策略。
///
/// 返回：返回带正确符号的整数商；Unnecessary 遇非零余数返回 RoundingNecessary。不检查零除数这一调用前置条件。
fn round_bigint_div(
    num_abs: bigdecimal::num_bigint::BigInt,
    den_abs: bigdecimal::num_bigint::BigInt,
    result_negative: bool,
    mode: RoundingMode,
) -> Result<bigdecimal::num_bigint::BigInt> {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::num_traits::Zero;
    let q_abs = &num_abs / &den_abs;
    let r_abs = &num_abs % &den_abs;
    let increment = if r_abs.is_zero() {
        false
    } else {
        let twice_r = &r_abs + &r_abs;
        match mode {
            RoundingMode::Unnecessary => return Err(NumericError::RoundingNecessary),
            RoundingMode::Up => true,
            RoundingMode::Down => false,
            RoundingMode::Ceiling => !result_negative,
            RoundingMode::Floor => result_negative,
            RoundingMode::HalfUp => twice_r >= den_abs,
            RoundingMode::HalfDown => twice_r > den_abs,
            RoundingMode::HalfEven => {
                twice_r > den_abs
                    || (twice_r == den_abs && (&q_abs % BigInt::from(2)) == BigInt::from(1))
            }
        }
    };
    let mut q = q_abs;
    if increment {
        q += BigInt::from(1);
    }
    if result_negative {
        q = -q;
    }
    Ok(q)
}

/// 业务作用：在受限幂展开内把十进制数转换到目标 scale。
///
/// 参数说明：
/// - `val`：需要改变精度的十进制数。
/// - `scale`：目标 scale，可为负数。
/// - `mode`：降低精度时使用的舍入策略。
///
/// 返回：零值直接返回目标 scale 的零；其它值精确扩展或按模式舍入。指数差溢出或展开超界返回 Scale，需要但禁止舍入时返回 RoundingNecessary。
fn set_scale_round_checked(val: &BigDecimal, scale: i64, mode: RoundingMode) -> Result<BigDecimal> {
    use bigdecimal::num_traits::{Signed, Zero};
    let (unscaled, cur) = val.as_bigint_and_exponent(); // 值 = unscaled × 10^-cur
                                                        //零值 O(1) 短路——`0` 在任何 scale 都是 `0`,无需展开 `10^n`。
    if unscaled.is_zero() {
        return Ok(BigDecimal::new(0.into(), scale));
    }
    let k = scale.checked_sub(cur).ok_or_else(|| {
        NumericError::Scale(format!(
            "decimal scale delta 溢出:target={scale}, current={cur}"
        ))
    })?;
    let exp = checked_decimal_exp(k.unsigned_abs())?;
    if k >= 0 {
        // 放大(目标小数位更多):精确补零,无舍入。
        Ok(BigDecimal::new(unscaled * pow10_bigint(exp), scale))
    } else {
        // 缩小:Q = round(unscaled / 10^|k|)。
        let q = round_bigint_div(
            unscaled.abs(),
            pow10_bigint(exp),
            unscaled.is_negative(),
            mode,
        )?;
        Ok(BigDecimal::new(q, scale))
    }
}

/// 业务作用：先精确对齐到共同 scale，再计算加法或减法。
///
/// 参数说明：
/// - `a`：左侧加数或被减数。
/// - `b`：右侧加数或减数。
/// - `subtract`：true 计算 a 减 b，false 计算相加。
///
/// 返回：成功返回精确结果和两操作数的较大 scale；非零系数对齐的指数差溢出或展开超界时返回 Scale。
fn checked_add_bd(a: &BigDecimal, b: &BigDecimal, subtract: bool) -> Result<BigDecimal> {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::num_traits::Zero;
    let (au, ae) = a.as_bigint_and_exponent();
    let (bu, be) = b.as_bigint_and_exponent();
    let common = ae.max(be); // scale 越大小数位越多 → 对齐目标
                             //零 operand 乘 `10^shift` 仍是 0,**不必展开、不受 MAX_DECIMAL_EXPANSION 拦截**——
                             // 故仅对**非零** operand 才算 shift / 过边界(`0 + high_scale` 等无需物理对齐 0)。
    let shifted = |u: BigInt, e: i64| -> Result<BigInt> {
        if u.is_zero() {
            return Ok(BigInt::from(0));
        }
        let shift = checked_decimal_exp(
            common
                .checked_sub(e)
                .ok_or_else(|| {
                    NumericError::Scale(format!("decimal scale delta 溢出:{common}-{e}"))
                })?
                .unsigned_abs(),
        )?;
        Ok(u * pow10_bigint(shift))
    };
    let a_adj = shifted(au, ae)?;
    let b_adj = shifted(bu, be)?;
    let combined = if subtract {
        a_adj - b_adj
    } else {
        a_adj + b_adj
    };
    Ok(BigDecimal::new(combined, common))
}

/// 业务作用：相乘十进制系数并检查非零乘积的 scale 相加。
///
/// 参数说明：
/// - `a`：左侧乘数。
/// - `b`：右侧乘数。
///
/// 返回：返回精确乘积；非零乘积 scale 相加溢出时返回 Scale。任一系数为零时返回零，scale 使用饱和相加。
fn checked_mul_bd(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    use bigdecimal::num_traits::Zero;
    let (au, ae) = a.as_bigint_and_exponent();
    let (bu, be) = b.as_bigint_and_exponent();
    //零乘积 O(1) 短路——`0 × x = 0`,不需物化非零 unscaled、不需展开。scale 取 `ae+be` 的
    // **饱和值**。
    if au.is_zero() || bu.is_zero() {
        return Ok(BigDecimal::new(0.into(), ae.saturating_add(be)));
    }
    // 非零:scale 和溢出是真实不可表示→ `Err(Scale)`。
    let scale = ae
        .checked_add(be)
        .ok_or_else(|| NumericError::Scale(format!("decimal 乘法 scale 溢出:{ae}+{be}")))?;
    Ok(BigDecimal::new(au * bu, scale))
}

// ==================== BigDecimal 段====================

/// 业务作用：精确相加两个十进制数。
///
/// 参数说明：
/// - `a`：左侧加数。
/// - `b`：右侧加数。
///
/// 返回：返回共同 scale 下的精确和；对齐指数不可表示或跨度超过限制时返回 Scale。
pub fn add(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_add_bd(a, b, false)
}

/// 业务作用：精确计算两个十进制数的差。
///
/// 参数说明：
/// - `a`：被减数。
/// - `b`：减数。
///
/// 返回：返回共同 scale 下的精确差；对齐指数不可表示或跨度超过限制时返回 Scale。
pub fn subtract(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_add_bd(a, b, true)
}

/// 业务作用：精确相乘两个十进制数。
///
/// 参数说明：
/// - `a`：左侧乘数。
/// - `b`：右侧乘数。
///
/// 返回：返回系数乘积及相加的 scale；非零乘积的 scale 溢出返回 Scale，零乘积的 scale 饱和相加。
pub fn multiply(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_mul_bd(a, b)
}

/// 业务作用：计算十进制乘积并以 HalfUp 对齐到指定 scale。
///
/// 参数说明：
/// - `a`：左侧乘数。
/// - `b`：右侧乘数。
/// - `scale`：乘积的目标 scale。
///
/// 返回：返回目标精度的舍入乘积；乘法指数溢出、对齐溢出或幂展开超限返回 Scale。
pub fn multiply_scale(a: &BigDecimal, b: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(&checked_mul_bd(a, b)?, scale, RoundingMode::HalfUp)
}

/// 业务作用：在指定 scale 上以 HalfUp 计算十进制商。
///
/// 参数说明：
/// - `a`：被除数。
/// - `b`：非零除数。
/// - `scale`：商的目标 scale。
///
/// 返回：返回目标精度的商；除数为零返回 DivByZero，指数不可表示或幂展开超界返回 Scale。
pub fn divide(a: &BigDecimal, b: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    divide_mode(a, b, scale, RoundingMode::HalfUp)
}

/// 业务作用：按指定目标 scale 和舍入策略计算十进制商。
///
/// 参数说明：
/// - `a`：被除数。
/// - `b`：非零除数。
/// - `scale`：商的目标 scale。
/// - `mode`：截取整数商时使用的舍入策略。
///
/// 返回：返回目标精度的商；零除数、指数溢出或展开超限返回对应错误，Unnecessary 遇非零余数返回 RoundingNecessary。
pub fn divide_mode(
    a: &BigDecimal,
    b: &BigDecimal,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    divide_scaled_exact(a, b, scale, mode)
}

/// 业务作用：把目标精度的十进制除法转成整数系数除法，避免中间商精度限制。
///
/// 参数说明：
/// - `a`：被除数，其系数和 scale 共同参与指数计算。
/// - `b`：非零除数，其系数和 scale 共同参与指数计算。
/// - `scale`：最终商的目标 scale。
/// - `mode`：整数除法有余数时的舍入策略。
///
/// 返回：返回目标 scale 的商，零被除数直接返回零；零除数返回 DivByZero，指数溢出或展开超限返回 Scale，禁止必要舍入时返回 RoundingNecessary。
fn divide_scaled_exact(
    a: &BigDecimal,
    b: &BigDecimal,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    use bigdecimal::num_traits::Signed;
    use bigdecimal::Zero;
    if b.is_zero() {
        return Err(NumericError::DivByZero);
    }
    let (a_int, a_exp) = a.as_bigint_and_exponent(); // 值 = a_int × 10^-a_exp
    let (b_int, b_exp) = b.as_bigint_and_exponent(); // 值 = b_int × 10^-b_exp
                                                     //被除数为 0(除数非 0)→ 结果恒 0,O(1) 返目标 scale 的零,不展开。
    if a_int.is_zero() {
        return Ok(BigDecimal::new(0.into(), scale));
    }
    // 目标整数商按 Q = a/b × 10^scale 计算，以整数系数和指数调整保持目标精度。
    // 指数差必须先校验可表示性；取绝对幅值使用无符号运算，避免最小有符号指数取负溢出。
    let k = scale
        .checked_sub(a_exp)
        .and_then(|v| v.checked_add(b_exp))
        .ok_or_else(|| {
            NumericError::Scale(format!(
                "decimal 指数溢出:scale={scale}, a_exp={a_exp}, b_exp={b_exp}"
            ))
        })?;
    let exp = checked_decimal_exp(k.unsigned_abs())?; // unsigned_abs 避免 i64::MIN 取负 panic;并校验展开位数防 OOM
    let (mut num, mut den) = (a_int, b_int);
    if k >= 0 {
        num *= pow10_bigint(exp);
    } else {
        den *= pow10_bigint(exp);
    }
    let result_negative = num.is_negative() ^ den.is_negative();
    let q = round_bigint_div(num.abs(), den.abs(), result_negative, mode)?;
    Ok(BigDecimal::new(q, scale))
}

/// 业务作用：以 HalfUp 将十进制数对齐到目标 scale。
///
/// 参数说明：
/// - `val`：待对齐的十进制数。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回扩展或舍入后的值；指数差溢出或幂展开超过限制返回 Scale。
pub fn align(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::HalfUp)
}

/// 业务作用：朝正无穷方向将十进制数对齐到目标 scale。
///
/// 参数说明：
/// - `val`：待对齐的有符号十进制数。
/// - `scale`：目标 scale。
///
/// 返回：返回 Ceiling 对齐结果，负数也按正无穷方向取整；指数差溢出或展开超界返回 Scale。
pub fn align_up(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::Ceiling)
}

/// 业务作用：朝负无穷方向将十进制数对齐到目标 scale。
///
/// 参数说明：
/// - `val`：待对齐的有符号十进制数。
/// - `scale`：目标 scale。
///
/// 返回：返回 Floor 对齐结果；指数差溢出或幂展开超界返回 Scale。
pub fn align_down(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::Floor)
}

/// 业务作用：逐项精确对齐并累加十进制引用序列。
///
/// 参数说明：
/// - `vals`：待求和的有限十进制数迭代序列。
///
/// 返回：成功返回精确和，空输入返回零；任一对齐失败返回 Scale，不跳过失败项。
pub fn sum<'a, I: IntoIterator<Item = &'a BigDecimal>>(vals: I) -> Result<BigDecimal> {
    use bigdecimal::Zero;
    let mut r = BigDecimal::zero();
    for v in vals {
        r = checked_add_bd(&r, v, false)?;
    }
    Ok(r)
}

/// 业务作用：精确求和后，以 HalfUp 计算目标 scale 的算术平均值。
///
/// 参数说明：
/// - `vals`：待求平均值的有限十进制数迭代序列。
/// - `scale`：非空输入平均值的目标 scale。
///
/// 返回：非空输入返回舍入平均值，空输入返回默认 scale 的零；求和或除法的指数、展开错误传播。
pub fn avg<'a, I: IntoIterator<Item = &'a BigDecimal>>(vals: I, scale: i64) -> Result<BigDecimal> {
    use bigdecimal::Zero;
    let mut s = BigDecimal::zero();
    let mut n: u64 = 0;
    for v in vals {
        s = checked_add_bd(&s, v, false)?;
        n += 1;
    }
    if n == 0 {
        return Ok(BigDecimal::zero());
    }
    divide_scaled_exact(&s, &BigDecimal::from(n), scale, RoundingMode::HalfUp)
}

// ==================== 最小单位 / 随机步长(BigDecimal,构造不展开、不受 8 位限)====================

/// 业务作用：构造目标 scale 的最小正精度单位。
///
/// 参数说明：
/// - `scale`：用于 1 乘以 10 的负 scale 次幂的十进制指数。
///
/// 返回：返回系数为一的 BigDecimal，支持任意 i64 scale，不在构造时展开十进制幂。
pub fn scale_min(scale: i64) -> BigDecimal {
    // BigDecimal(unscaledValue=1, scale):1 × 10^-scale。
    BigDecimal::new(1.into(), scale)
}

/// 业务作用：生成以指定最小精度为单位的正随机步长。
///
/// 参数说明：
/// - `step`：系数采样的排他上界，非正值使用最小单位。
/// - `scale`：最小精度单位的 scale。
///
/// 返回：step 不大于一时返回最小单位；其余返回系数介于一和 step 减一的数。随机零映射为一，构造不展开幂。
pub fn random_step(step: i64, scale: i64) -> BigDecimal {
    if step <= 0 {
        return scale_min(scale);
    }
    let rand: i64 = rand::thread_rng().gen_range(0..step);
    let unscaled = if rand == 0 {
        1
    } else {
        rand
    };
    BigDecimal::new(unscaled.into(), scale)
}

/// 业务作用：先将十进制步长上界朝零截断并饱和为 i64，再采样最小精度单位。
///
/// 参数说明：
/// - `step`：会先朝零截断、越界时按符号饱和的上界。
/// - `scale`：返回值的最小精度 scale。
///
/// 返回：返回遵循 random_step 规则的正值，截断后的非正上界返回最小单位。
pub fn random_step_bd(step: &BigDecimal, scale: i64) -> BigDecimal {
    random_step(bigdecimal_to_i64_saturating(step), scale)
}

/// 业务作用：在不展开任意指数的情况下提取整数部分并饱和转换。
///
/// 参数说明：
/// - `v`：待朝零截断的十进制数。
///
/// 返回：可表示时返回 i64 整数部分，越界按符号返回边界，绝对值小于一返回零；不按四舍五入取整。
fn bigdecimal_to_i64_saturating(v: &BigDecimal) -> i64 {
    use bigdecimal::num_traits::{Signed, Zero};
    let (unscaled, exponent) = v.as_bigint_and_exponent(); // 值 = unscaled × 10^-exponent
    if unscaled.is_zero() {
        return 0;
    }
    let negative = unscaled.is_negative();
    let saturate = if negative {
        i64::MIN
    } else {
        i64::MAX
    };
    let digits = unscaled.abs().to_string(); // 无符号十进制位串(unscaled 已物化,无 OOM 放大)
    let int_str: String = if exponent > 0 {
        // 值 = unscaled / 10^exponent:去掉末 exponent 位即整数部分(朝零截断)。
        let d = digits.len() as i64;
        if d <= exponent {
            return 0; // |值| < 1
        }
        digits[..(d - exponent) as usize].to_string()
    } else {
        // 值 = unscaled × 10^|exponent|:整数部分 = unscaled 后接 |exponent| 个 0。
        let shift = exponent.unsigned_abs();
        if shift >= 19 {
            return saturate; // ≥ 10^19 必超 i64
        }
        let mut s = digits;
        s.push_str(&"0".repeat(shift as usize));
        s
    };
    // int_str 为无符号整数文本;> i64 域(含位数超 i128)即饱和。
    match int_str.parse::<i128>() {
        Ok(mag) => {
            let signed = if negative {
                -mag
            } else {
                mag
            };
            signed.clamp(i64::MIN as i128, i64::MAX as i128) as i64
        }
        Err(_) => saturate,
    }
}

// 字符串入口直接解析十进制数；浮点入口先生成十进制文本，再进入 BigDecimal 运算。

/// 业务作用：直接解析十进制或科学计数法文本。
///
/// 参数说明：
/// - `s`：交由 BigDecimal 解析的原始文本。
///
/// 返回：合法文本返回对应十进制值，否则返回 Parse；不执行算术展开预算校验。
pub fn parse(s: &str) -> Result<BigDecimal> {
    BigDecimal::from_str(s).map_err(|e| NumericError::Parse(format!("BigDecimal 解析失败: {e}")))
}

/// 业务作用：将有限浮点的十进制显示文本解析成十进制数。
///
/// 参数说明：
/// - `v`：需要转换的有限 f64。
///
/// 返回：成功返回显示文本对应的 BigDecimal；非有限值或文本解析失败返回 Parse，不保留输入显示尾零或二进制浮点的精确展开。
fn from_f64(v: f64) -> Result<BigDecimal> {
    if !v.is_finite() {
        return Err(NumericError::Parse(format!("非有限 f64: {v}")));
    }
    parse(&format!("{v}"))
}

/// 业务作用：转换两个十进制文本后相加。
///
/// 参数说明：
/// - `a`：十进制文本形式的左侧加数。
/// - `b`：十进制文本形式的右侧加数。
///
/// 返回：成功返回精确和；文本解析失败返回 Parse，算术指数或展开超界返回 Scale。
pub fn add_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    add(&parse(a)?, &parse(b)?)
}

/// 业务作用：转换两个有限 f64后相加。
///
/// 参数说明：
/// - `a`：有限 f64形式的左侧加数。
/// - `b`：有限 f64形式的右侧加数。
///
/// 返回：成功返回精确和；输入非有限或十进制文本转换失败返回 Parse，算术指数或展开超界返回 Scale。
pub fn add_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    add(&from_f64(a)?, &from_f64(b)?)
}

/// 业务作用：转换两个十进制文本后相减。
///
/// 参数说明：
/// - `a`：十进制文本形式的被减数。
/// - `b`：十进制文本形式的减数。
///
/// 返回：成功返回精确差；文本解析失败返回 Parse，算术指数或展开超界返回 Scale。
pub fn subtract_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    subtract(&parse(a)?, &parse(b)?)
}

/// 业务作用：转换两个有限 f64后相减。
///
/// 参数说明：
/// - `a`：有限 f64形式的被减数。
/// - `b`：有限 f64形式的减数。
///
/// 返回：成功返回精确差；输入非有限或十进制文本转换失败返回 Parse，算术指数或展开超界返回 Scale。
pub fn subtract_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    subtract(&from_f64(a)?, &from_f64(b)?)
}

/// 业务作用：转换两个十进制文本后相乘。
///
/// 参数说明：
/// - `a`：十进制文本形式的左侧乘数。
/// - `b`：十进制文本形式的右侧乘数。
///
/// 返回：成功返回精确乘积；文本解析失败返回 Parse，算术指数或展开超界返回 Scale。
pub fn multiply_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    multiply(&parse(a)?, &parse(b)?)
}

/// 业务作用：转换两个有限 f64后相乘。
///
/// 参数说明：
/// - `a`：有限 f64形式的左侧乘数。
/// - `b`：有限 f64形式的右侧乘数。
///
/// 返回：成功返回精确乘积；输入非有限或十进制文本转换失败返回 Parse，算术指数或展开超界返回 Scale。
pub fn multiply_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    multiply(&from_f64(a)?, &from_f64(b)?)
}

/// 业务作用：转换十进制文本后计算乘积并以 HalfUp 对齐精度。
///
/// 参数说明：
/// - `a`：十进制文本形式的左侧乘数。
/// - `b`：十进制文本形式的右侧乘数。
/// - `scale`：乘积目标 scale。
///
/// 返回：返回目标精度的乘积；文本解析失败、乘法或对齐失败时返回相应错误。
pub fn multiply_decimal_str_scale(a: &str, b: &str, scale: i64) -> Result<BigDecimal> {
    multiply_scale(&parse(a)?, &parse(b)?, scale)
}

/// 业务作用：转换有限 f64后计算乘积并以 HalfUp 对齐精度。
///
/// 参数说明：
/// - `a`：有限 f64形式的左侧乘数。
/// - `b`：有限 f64形式的右侧乘数。
/// - `scale`：乘积目标 scale。
///
/// 返回：返回目标精度的乘积；输入非有限或十进制文本转换失败、乘法或对齐失败时返回相应错误。
pub fn multiply_decimal_f64_scale(a: f64, b: f64, scale: i64) -> Result<BigDecimal> {
    multiply_scale(&from_f64(a)?, &from_f64(b)?, scale)
}

/// 业务作用：转换十进制文本后以HalfUp计算目标精度的商。
///
/// 参数说明：
/// - `a`：十进制文本形式的被除数。
/// - `b`：十进制文本形式的非零除数。
/// - `scale`：商的目标 scale。
///
/// 返回：返回目标 scale 的商；文本解析失败、零除数、指数溢出、展开超界时返回对应错误。
pub fn divide_decimal_str(a: &str, b: &str, scale: i64) -> Result<BigDecimal> {
    divide(&parse(a)?, &parse(b)?, scale)
}

/// 业务作用：转换有限 f64后以HalfUp计算目标精度的商。
///
/// 参数说明：
/// - `a`：有限 f64形式的被除数。
/// - `b`：有限 f64形式的非零除数。
/// - `scale`：商的目标 scale。
///
/// 返回：返回目标 scale 的商；输入非有限或十进制文本转换失败、零除数、指数溢出、展开超界时返回对应错误。
pub fn divide_decimal_f64(a: f64, b: f64, scale: i64) -> Result<BigDecimal> {
    divide(&from_f64(a)?, &from_f64(b)?, scale)
}

/// 业务作用：转换十进制文本后以指定模式计算目标精度的商。
///
/// 参数说明：
/// - `a`：十进制文本形式的被除数。
/// - `b`：十进制文本形式的非零除数。
/// - `scale`：商的目标 scale。
/// - `mode`：有非零余数时的舍入或拒绝策略。
///
/// 返回：返回目标 scale 的商；文本解析失败、零除数、指数溢出、展开超界或禁止必要舍入时返回对应错误。
pub fn divide_decimal_str_mode(
    a: &str,
    b: &str,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    divide_mode(&parse(a)?, &parse(b)?, scale, mode)
}

/// 业务作用：转换有限 f64后以指定模式计算目标精度的商。
///
/// 参数说明：
/// - `a`：有限 f64形式的被除数。
/// - `b`：有限 f64形式的非零除数。
/// - `scale`：商的目标 scale。
/// - `mode`：有非零余数时的舍入或拒绝策略。
///
/// 返回：返回目标 scale 的商；输入非有限或十进制文本转换失败、零除数、指数溢出、展开超界或禁止必要舍入时返回对应错误。
pub fn divide_decimal_f64_mode(
    a: f64,
    b: f64,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    divide_mode(&from_f64(a)?, &from_f64(b)?, scale, mode)
}

/// 业务作用：将十进制文本转换为十进制数后按 HalfUp 对齐。
///
/// 参数说明：
/// - `val`：待转换及对齐的十进制文本。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回目标精度的值；文本解析失败或对齐指数、展开超界时返回对应错误。
pub fn align_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align(&parse(val)?, scale)
}

/// 业务作用：将有限 f64转换为十进制数后按 HalfUp 对齐。
///
/// 参数说明：
/// - `val`：待转换及对齐的有限 f64。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回目标精度的值；输入非有限或十进制文本转换失败或对齐指数、展开超界时返回对应错误。
pub fn align_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align(&from_f64(val)?, scale)
}

/// 业务作用：将十进制文本转换为十进制数后按 Ceiling 对齐。
///
/// 参数说明：
/// - `val`：待转换及对齐的十进制文本。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回目标精度的值；文本解析失败或对齐指数、展开超界时返回对应错误。
pub fn align_up_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align_up(&parse(val)?, scale)
}

/// 业务作用：将有限 f64转换为十进制数后按 Ceiling 对齐。
///
/// 参数说明：
/// - `val`：待转换及对齐的有限 f64。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回目标精度的值；输入非有限或十进制文本转换失败或对齐指数、展开超界时返回对应错误。
pub fn align_up_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align_up(&from_f64(val)?, scale)
}

/// 业务作用：将十进制文本转换为十进制数后按 Floor 对齐。
///
/// 参数说明：
/// - `val`：待转换及对齐的十进制文本。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回目标精度的值；文本解析失败或对齐指数、展开超界时返回对应错误。
pub fn align_down_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align_down(&parse(val)?, scale)
}

/// 业务作用：将有限 f64转换为十进制数后按 Floor 对齐。
///
/// 参数说明：
/// - `val`：待转换及对齐的有限 f64。
/// - `scale`：目标 scale，可为负数。
///
/// 返回：返回目标精度的值；输入非有限或十进制文本转换失败或对齐指数、展开超界时返回对应错误。
pub fn align_down_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align_down(&from_f64(val)?, scale)
}

/// 业务作用：逐项转换十进制文本序列并精确求和。
///
/// 参数说明：
/// - `vals`：任一项转换失败都拒绝的十进制文本列表。
///
/// 返回：返回精确和，空输入返回零；文本解析失败或求和对齐失败返回对应错误。
pub fn sum_decimal_str(vals: &[&str]) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|s| parse(s)).collect();
    sum(&parsed?)
}

/// 业务作用：逐项转换有限 f64序列并精确求和。
///
/// 参数说明：
/// - `vals`：任一项转换失败都拒绝的有限 f64列表。
///
/// 返回：返回精确和，空输入返回零；输入非有限或十进制文本转换失败或求和对齐失败返回对应错误。
pub fn sum_decimal_f64(vals: &[f64]) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|v| from_f64(*v)).collect();
    sum(&parsed?)
}

/// 业务作用：逐项转换十进制文本后以 HalfUp 计算平均值。
///
/// 参数说明：
/// - `vals`：待求平均值的十进制文本列表。
/// - `scale`：非空输入平均值的目标 scale。
///
/// 返回：返回舍入平均值，空输入返回默认 scale 的零；文本解析失败、求和或除法失败返回对应错误。
pub fn avg_decimal_str(vals: &[&str], scale: i64) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|s| parse(s)).collect();
    avg(&parsed?, scale)
}

/// 业务作用：逐项转换有限 f64后以 HalfUp 计算平均值。
///
/// 参数说明：
/// - `vals`：待求平均值的有限 f64列表。
/// - `scale`：非空输入平均值的目标 scale。
///
/// 返回：返回舍入平均值，空输入返回默认 scale 的零；输入非有限或十进制文本转换失败、求和或除法失败返回对应错误。
pub fn avg_decimal_f64(vals: &[f64], scale: i64) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|v| from_f64(*v)).collect();
    avg(&parsed?, scale)
}
