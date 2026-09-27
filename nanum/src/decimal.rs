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

/// 业务作用: 校验 `10^exp` 展开位数不超 [`MAX_DECIMAL_EXPANSION`](防 OOM),返 `usize` 供 `int_pow`。超界 → `Err(Scale)`。
///
/// # 参数
/// - `exp`: 十进制缩放指数。
fn checked_decimal_exp(exp: u64) -> Result<usize> {
    if exp > MAX_DECIMAL_EXPANSION {
        return Err(NumericError::Scale(format!(
            "decimal 展开位数 {exp} 超上限 {MAX_DECIMAL_EXPANSION}(防 OOM)"
        )));
    }
    Ok(exp as usize)
}

/// 业务作用: `10^exp`(BigInt)。`exp` 须已过 [`checked_decimal_exp`]。
///
/// # 参数
/// - `exp`: 十进制缩放指数。
fn pow10_bigint(exp: usize) -> bigdecimal::num_bigint::BigInt {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::num_traits::pow::pow as int_pow;
    int_pow(BigInt::from(10), exp)
}

/// 业务作用: 按 `mode` 把 `num_abs / den_abs`(均 ≥ 0,`den_abs > 0`)舍入为带符号 `BigInt` 商。`Unnecessary` 遇余数非 0 → `Err`。
/// 全 8 `RoundingMode`,被 [`divide_scaled_exact`] 与 [`set_scale_round_checked`] 复用。
///
/// # 参数
/// - `num_abs`: 有理数分子的绝对值。
/// - `den_abs`: 有理数分母的绝对值。
/// - `result_negative`: 运算结果是否应为负数。
/// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
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

/// 业务作用：用 checked scale 算术和 BigInt 将数值舍入到目标 scale。
/// 放大时精确乘以十的幂，缩小时按 mode 舍入；先验证派生 scale 与展开上限，再分配整数。
/// 返回：目标 scale 的值；派生量溢出、展开超界或无法满足舍入要求时返回错误。
///
/// # 参数
/// - `val`: 要写入 Redis 或发送到下游的值。
/// - `scale`: 小数精度或缩放位数。
/// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
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

/// 业务作用: checked BigDecimal 加/减,**不调 bigdecimal 的 `+`/`-`**(其内部 `(lhs.scale - rhs.scale) as u64` 在极端 operand
/// scale 下整数溢出 panic + `ten_to_the` 天量分配)。对齐到 `max(a_scale,b_scale)` 后 BigInt 加减,
/// scale 对齐展开过 [`checked_decimal_exp`]。
///
/// # 参数
/// - `a`: 参与当前计算或编码的第一个输入值。
/// - `b`: 参与当前计算或编码的第二个输入值。
/// - `subtract`: 本次小数运算是否执行减法。
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

/// 业务作用: checked BigDecimal 乘,**不调 bigdecimal 的 `*`**(其内部 `self.scale + rhs.scale` 未 checked,极端 operand scale
/// 溢出 panic)。unscaled 直接 BigInt 相乘,结果 scale = `a_scale + b_scale`(checked)。
///
/// # 参数
/// - `a`: 参与当前计算或编码的第一个输入值。
/// - `b`: 参与当前计算或编码的第二个输入值。
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

/// 业务作用: BigDecimal 加法(精确)。
///
/// **返 `Result`**:operand 自带极端 scale(如 `BigDecimal::new(_, i64::MAX/MIN)`)对齐时会溢出,
/// 走 checked 算术返 `Err(Scale)` 而非 panic(crate "溢出→Result、不 panic" 契约)。正常输入永不出错。
///
/// # 参数
///
/// - `a`: 左侧加数，按其 BigDecimal 数值和 scale 参与对齐。
/// - `b`: 右侧加数，按其 BigDecimal 数值和 scale 参与对齐。
pub fn add(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_add_bd(a, b, false)
}

/// 业务作用: BigDecimal 减法(精确)。返 `Result`(同 [`add`])。
///
/// # 参数
///
/// - `a`: 被减数，按其 BigDecimal 数值和 scale 参与对齐。
/// - `b`: 减数，按其 BigDecimal 数值和 scale 参与对齐。
pub fn subtract(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_add_bd(a, b, true)
}

/// 业务作用: BigDecimal 乘法(精确)。返 `Result`(同 [`add`])。
///
/// # 参数
///
/// - `a`: 左侧乘数。
/// - `b`: 右侧乘数。
pub fn multiply(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_mul_bd(a, b)
}

/// 业务作用: BigDecimal 乘法,结果对齐到 `scale`(HALF_UP)。
///
/// 返 `Result`:乘法本身(`checked_mul_bd`)与对齐(`set_scale_round_checked`)均 checked,极端 scale 返 `Err(Scale)`。
///
/// # 参数
///
/// - `a`: 左侧乘数。
/// - `b`: 右侧乘数。
/// - `scale`: 结果需要保留的小数位数，可超过 fixed 8 位上限。
pub fn multiply_scale(a: &BigDecimal, b: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(&checked_mul_bd(a, b)?, scale, RoundingMode::HalfUp)
}

/// 业务作用: BigDecimal 除法,指定 `scale`(HALF_UP;避免无限循环小数)。
///
/// # 参数
///
/// - `a`: 被除数。
/// - `b`: 除数；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 结果需要保留的小数位数。
pub fn divide(a: &BigDecimal, b: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    divide_mode(a, b, scale, RoundingMode::HalfUp)
}

/// 业务作用：按指定 scale 和舍入模式执行 BigDecimal 除法。
/// 返回：目标 scale 的商；除数为零返回 `DivByZero`，Unnecessary 需要舍入时返回
/// `RoundingNecessary`，展开超界返回 `Scale`。
///
/// # 参数
///
/// - `a`: 被除数。
/// - `b`: 除数；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 结果需要保留的小数位数。
/// - `mode`: 除不尽或缩减精度时使用的舍入策略。
pub fn divide_mode(
    a: &BigDecimal,
    b: &BigDecimal,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    divide_scaled_exact(a, b, scale, mode)
}

/// 业务作用: 按目标 `scale` **直接用 BigInt 整数除法 + 舍入**,真任意精度。
///
/// **旧实现先 `let q = a / b` 再 `q.with_scale_round(scale)`,但 `bigdecimal` 的 `/` 默认中间
/// 精度仅 ~100 位有效数字,`scale` 超过后 `with_scale_round` 只会**补零**(如 `divide(1,3,120)` 约 100 位后全 0),
/// 数学错误。此处改为:把 `a/b` 化成目标 scale 下的 unscaled 商 `Q = round(a_int × 10^(scale−a_exp+b_exp) / b_int)`,
/// 用 BigInt `div_rem` + 按 `RoundingMode` 判进位,**无中间精度上限**。
///
/// # 参数
/// - `a`: 参与当前计算或编码的第一个输入值。
/// - `b`: 参与当前计算或编码的第二个输入值。
/// - `scale`: 小数精度或缩放位数。
/// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
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
    // Q = a/b × 10^scale = a_int × 10^(scale − a_exp + b_exp) / b_int。
    //checked 算术——极端 i64 scale/operand-exponent 下 `scale-a_exp+b_exp` 会整数溢出,
    // `(-k) as usize` 在 `k==i64::MIN` 取负溢出,违反 crate "不 panic" 契约。
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

/// 业务作用: BigDecimal 截到 `scale` 位(HALF_UP)。
///
/// 返 `Result`:极端 scale 经 `set_scale_round_checked` 返 `Err(Scale)` 而非 panic/OOM。
///
/// # 参数
///
/// - `val`: 需要按目标精度重新对齐的 BigDecimal 值。
/// - `scale`: 目标小数位数。
pub fn align(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::HalfUp)
}

/// 业务作用: BigDecimal 向上(+∞,CEILING)取到 `scale`。返 `Result`(同 [`align`])。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的 BigDecimal 值。
/// - `scale`: 目标小数位数。
pub fn align_up(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::Ceiling)
}

/// 业务作用: BigDecimal 向下(−∞,FLOOR)取到 `scale`。返 `Result`(同 [`align`])。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的 BigDecimal 值。
/// - `scale`: 目标小数位数。
pub fn align_down(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::Floor)
}

/// 业务作用：对 BigDecimal 引用求和，空输入返回零。
/// 输入不包含空引用；可选值由调用方过滤。使用 checked scale 对齐，超界返回 `Err(Scale)`。
///
/// # 参数
/// - `vals`: 待计算或格式化的数值列表。
pub fn sum<'a, I: IntoIterator<Item = &'a BigDecimal>>(vals: I) -> Result<BigDecimal> {
    use bigdecimal::Zero;
    let mut r = BigDecimal::zero();
    for v in vals {
        r = checked_add_bd(&r, v, false)?;
    }
    Ok(r)
}

/// 业务作用: BigDecimal 平均值,对齐到 `scale`(HALF_UP);空 → `0`。
///
/// 返 `Result`:求和阶段走 `checked_add_bd`、除法走 `divide_scaled_exact`,极端 scale
/// 全程返 `Err(Scale)` 而非 panic(指出旧实现 `s += v` 求和阶段仍会 panic)。正常 scale 永不出错。
///
/// # 参数
/// - `vals`: 待计算或格式化的数值列表。
/// - `scale`: 小数精度或缩放位数。
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

/// 业务作用：构造指定精度的最小正值 `1 × 10^-scale`，允许负 scale。
/// 构造只设置系数与 scale，不展开十的幂；后续算术仍受 `MAX_DECIMAL_EXPANSION` 约束。
/// 返回：系数为 1 的 BigDecimal，例如 `scale_min(-1)` 为 10。
///
/// # 参数
///
/// - `scale`: 小数位数；返回值为 `1 × 10^-scale`。
pub fn scale_min(scale: i64) -> BigDecimal {
    // BigDecimal(unscaledValue=1, scale):1 × 10^-scale。
    BigDecimal::new(1.into(), scale)
}

/// 业务作用: 在最小精度位上取步长随机值,范围 `[min, (step-1)×min]`(`min=10^-scale`)。
///
/// `step<=0` → `scale_min(scale)`;`rand=0` 时也返回 `min`（不返回零）。构造不展开,任意 `i64 scale` 安全。
///
/// # 参数
///
/// - `step`: 随机步长上界；`<= 0` 时直接返回 [`scale_min`]。
/// - `scale`: 最小单位的小数位数。
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

/// 业务作用：将 BigDecimal 步长上界朝零截断为 i64 后，生成最小精度单位上的随机值。
/// 超出 i64 的步长按符号饱和，避免回绕改变上界。
/// 返回：遵循 `random_step` 合同的正值；非正步长返回最小单位。
///
/// # 参数
///
/// - `step`: BigDecimal 形式的随机步长上界，会先截断为 `i64`。
/// - `scale`: 最小单位的小数位数。
pub fn random_step_bd(step: &BigDecimal, scale: i64) -> BigDecimal {
    random_step(bigdecimal_to_i64_saturating(step), scale)
}

/// 业务作用：朝零截断 BigDecimal 的整数部分，超出 i64 时按符号饱和。
/// 通过系数位数和 scale 定位有效数字，不展开十的幂，避免极端指数引发巨大分配。
/// 返回：截断后的整数，或对应符号的 i64 边界。
///
/// # 参数
/// - `v`: 待转换的值。
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

/// 业务作用: 精确解析十进制或科学计数法字符串，非法文本返回错误。
///
/// # 参数
///
/// - `s`: 十进制或科学计数法文本，按 BigDecimal 精确解析。
pub fn parse(s: &str) -> Result<BigDecimal> {
    BigDecimal::from_str(s).map_err(|e| NumericError::Parse(format!("BigDecimal 解析失败: {e}")))
}

/// 业务作用：把有限 f64 的十进制显示文本解析为 BigDecimal。
/// 例如 `1.0` 的显示文本为 `1`，所得 scale 为 0；本入口不保留输入文本的尾零或显示精度。
/// 依赖固定 scale、数据库列精度或序列化文本的调用方必须显式对齐与格式化。
/// 返回：转换后的值；非有限输入或文本解析失败时返回错误。
///
/// # 参数
/// - `v`: 待转换的值。
fn from_f64(v: f64) -> Result<BigDecimal> {
    if !v.is_finite() {
        return Err(NumericError::Parse(format!("非有限 f64: {v}")));
    }
    parse(&format!("{v}"))
}

/// 业务作用: `addDecimal(String,String)`。
///
/// # 参数
///
/// - `a`: 左侧十进制文本加数。
/// - `b`: 右侧十进制文本加数。
pub fn add_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    add(&parse(a)?, &parse(b)?)
}

/// 业务作用: `addDecimal(double,double)`。
///
/// # 参数
///
/// - `a`: 左侧 f64 加数，会先转换为 BigDecimal。
/// - `b`: 右侧 f64 加数，会先转换为 BigDecimal。
pub fn add_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    add(&from_f64(a)?, &from_f64(b)?)
}

/// 业务作用: `subtractDecimal(String,String)`。
///
/// # 参数
///
/// - `a`: 十进制文本被减数。
/// - `b`: 十进制文本减数。
pub fn subtract_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    subtract(&parse(a)?, &parse(b)?)
}

/// 业务作用: `subtractDecimal(double,double)`。
///
/// # 参数
///
/// - `a`: f64 被减数，会先转换为 BigDecimal。
/// - `b`: f64 减数，会先转换为 BigDecimal。
pub fn subtract_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    subtract(&from_f64(a)?, &from_f64(b)?)
}

/// 业务作用: `multiplyDecimal(String,String)`。
///
/// # 参数
///
/// - `a`: 左侧十进制文本乘数。
/// - `b`: 右侧十进制文本乘数。
pub fn multiply_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    multiply(&parse(a)?, &parse(b)?)
}

/// 业务作用: `multiplyDecimal(double,double)`。
///
/// # 参数
///
/// - `a`: 左侧 f64 乘数，会先转换为 BigDecimal。
/// - `b`: 右侧 f64 乘数，会先转换为 BigDecimal。
pub fn multiply_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    multiply(&from_f64(a)?, &from_f64(b)?)
}

/// 业务作用: `multiplyDecimal(String,String,scale)`。
///
/// # 参数
///
/// - `a`: 左侧十进制文本乘数。
/// - `b`: 右侧十进制文本乘数。
/// - `scale`: 乘积需要保留的小数位数。
pub fn multiply_decimal_str_scale(a: &str, b: &str, scale: i64) -> Result<BigDecimal> {
    multiply_scale(&parse(a)?, &parse(b)?, scale)
}

/// 业务作用: `multiplyDecimal(double,double,scale)`。
///
/// # 参数
///
/// - `a`: 左侧 f64 乘数，会先转换为 BigDecimal。
/// - `b`: 右侧 f64 乘数，会先转换为 BigDecimal。
/// - `scale`: 乘积需要保留的小数位数。
pub fn multiply_decimal_f64_scale(a: f64, b: f64, scale: i64) -> Result<BigDecimal> {
    multiply_scale(&from_f64(a)?, &from_f64(b)?, scale)
}

/// 业务作用: `divideDecimal(String,String,scale)`。
///
/// # 参数
///
/// - `a`: 十进制文本被除数。
/// - `b`: 十进制文本除数；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 商需要保留的小数位数。
pub fn divide_decimal_str(a: &str, b: &str, scale: i64) -> Result<BigDecimal> {
    divide(&parse(a)?, &parse(b)?, scale)
}

/// 业务作用: `divideDecimal(double,double,scale)`。
///
/// # 参数
///
/// - `a`: f64 被除数，会先转换为 BigDecimal。
/// - `b`: f64 除数，会先转换为 BigDecimal；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 商需要保留的小数位数。
pub fn divide_decimal_f64(a: f64, b: f64, scale: i64) -> Result<BigDecimal> {
    divide(&from_f64(a)?, &from_f64(b)?, scale)
}

/// 业务作用: `divideDecimal(String,String,scale,mode)`。
///
/// # 参数
///
/// - `a`: 十进制文本被除数。
/// - `b`: 十进制文本除数；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 商需要保留的小数位数。
/// - `mode`: 除不尽或缩减精度时使用的舍入策略。
pub fn divide_decimal_str_mode(
    a: &str,
    b: &str,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    divide_mode(&parse(a)?, &parse(b)?, scale, mode)
}

/// 业务作用: `divideDecimal(double,double,scale,mode)`。
///
/// # 参数
///
/// - `a`: f64 被除数，会先转换为 BigDecimal。
/// - `b`: f64 除数，会先转换为 BigDecimal；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 商需要保留的小数位数。
/// - `mode`: 除不尽或缩减精度时使用的舍入策略。
pub fn divide_decimal_f64_mode(
    a: f64,
    b: f64,
    scale: i64,
    mode: RoundingMode,
) -> Result<BigDecimal> {
    divide_mode(&from_f64(a)?, &from_f64(b)?, scale, mode)
}

/// 业务作用: `alignDecimal(String,scale)`。
///
/// # 参数
///
/// - `val`: 需要按目标精度对齐的十进制文本。
/// - `scale`: 目标小数位数。
pub fn align_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align(&parse(val)?, scale)
}

/// 业务作用: `alignDecimal(double,scale)`。
///
/// # 参数
///
/// - `val`: 需要按目标精度对齐的 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align(&from_f64(val)?, scale)
}

/// 业务作用: `alignUpDecimal(String,scale)`。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的十进制文本。
/// - `scale`: 目标小数位数。
pub fn align_up_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align_up(&parse(val)?, scale)
}

/// 业务作用: `alignUpDecimal(double,scale)`。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_up_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align_up(&from_f64(val)?, scale)
}

/// 业务作用: `alignDownDecimal(String,scale)`。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的十进制文本。
/// - `scale`: 目标小数位数。
pub fn align_down_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align_down(&parse(val)?, scale)
}

/// 业务作用: `alignDownDecimal(double,scale)`。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_down_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align_down(&from_f64(val)?, scale)
}

/// 业务作用: 精确解析并求和十进制文本，任一输入非法时返回错误，不跳过无效项。
///
/// # 参数
///
/// - `vals`: 待求和的一组十进制文本；任一项解析失败会返回错误。
pub fn sum_decimal_str(vals: &[&str]) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|s| parse(s)).collect();
    sum(&parsed?)
}

/// 业务作用: `sumDecimal(double...)`。
///
/// # 参数
///
/// - `vals`: 待求和的一组 f64 值；非有限值会返回解析错误。
pub fn sum_decimal_f64(vals: &[f64]) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|v| from_f64(*v)).collect();
    sum(&parsed?)
}

/// 业务作用: `avgDecimal(String[],scale)`。
///
/// # 参数
///
/// - `vals`: 待求平均值的一组十进制文本；空数组返回 `0`。
/// - `scale`: 平均值需要保留的小数位数。
pub fn avg_decimal_str(vals: &[&str], scale: i64) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|s| parse(s)).collect();
    avg(&parsed?, scale)
}

/// 业务作用: `avgDecimal(double[],scale)`。
///
/// # 参数
///
/// - `vals`: 待求平均值的一组 f64 值；空数组返回 `0`，非有限值会返回解析错误。
/// - `scale`: 平均值需要保留的小数位数。
pub fn avg_decimal_f64(vals: &[f64], scale: i64) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|v| from_f64(*v)).collect();
    avg(&parsed?, scale)
}
