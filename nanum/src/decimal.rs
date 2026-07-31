//! BigDecimal 高精度段(对照 原实现 `Numeric` 的 BigDecimal API + Decimal 后缀便捷段)。
//!
//! 定点 i128 热路径(`arithmetic`/`io`)受 `MAX_SCALE=8` 限(对照 原实现 fixed-long `MAX_FIXED_SCALE=8`);
//! **scale>8 走本模块的 BigDecimal 退化路径**(对照 原实现 `// BigDecimal API (scale > MAX_FIXED_SCALE 退化路径)`)。
//! 不受 fixed `MAX_SCALE=8` 上限。`scale_min`/`random_step` 仅构造、不展开 `10^n`,任意 `i64 scale` 安全;**其余
//! 全部 BigDecimal 运算(`add/subtract/multiply/multiply_scale/divide/sum/avg/align*`)在 operand scale 不一致或目标
//! scale 差异时需展开 `10^n` 对齐**,展开位数受 `MAX_DECIMAL_EXPANSION` 防 OOM 边界保护、极端 scale 返 `Err(Scale)`
//! 不 panic(故这些函数全返 `Result`,见下)。`BigDecimal` 即 `bigdecimal::BigDecimal`(re-export 自 `nasa::numeric::BigDecimal`)。
//!
//! **`divide`/`divide_mode`/`avg` 走自实现的 `divide_scaled_exact`(BigInt 按目标 scale
//! 整数除法 + 舍入),**不经 `bigdecimal` 的 `/`**(其默认中间精度仅 ~100 位,高 scale 会补零)。故"任意精度"对
//! 高 scale 除法成立(`divide(1,3,120)` 得真正 120 位)。
//!
//! **`add/subtract/multiply/multiply_scale/divide/sum/avg/align*` **不再调用 `bigdecimal`
//! 的 `+/-/*/with_scale_round`** 做 scale 对齐(其内部多处 i64 scale 派生量未 checked,极端 operand scale 整数溢出
//! panic),统一走自实现 checked BigInt 算术(`round_bigint_div`/`set_scale_round_checked`/`checked_add_bd`/
//! `checked_mul_bd`)+ `MAX_DECIMAL_EXPANSION`,极端 scale 返 `Err(Scale)` 而非 panic/OOM;**整段 API 因此返 `Result`**
//! 这是有意的兼容性破坏,调用方需要按 `Result` 处理极端 scale。
//!
//! **零值/单元素高 scale 加 O(1) 结构短路(`0` 在任何 scale 都是 `0`、`0 op x`、`x/1` 不需展开
//! `10^n`),不再被 `MAX_DECIMAL_EXPANSION` 误拦截,对齐 原实现 `BigDecimal` 语义。
//!
//! **文本表现偏离(只保证数值/scale,不保证 `to_string` 文本)**:本模块返回 `BigDecimal` **值对象**,其数值与
//! scale 与 原实现 一致;但 Rust `bigdecimal::Display` ≠ 原实现 `BigDecimal.toString()`,尤其**负 scale / 科学计数法**
//! (如 `divide(12345,1,-2)` Rust `"12300"` vs 原实现 `"1.23E+4"`、`scale_min(-1)` Rust `"10"` vs 原实现 `"1E+1"`,
//! 二者数值/scale 相同)。若业务把结果文本直接落库/序列化/比对,需自备 原实现-`BigDecimal.toString` 兼容 formatter。

use crate::{NumericError, Result, RoundingMode};
use bigdecimal::BigDecimal;
use rand::Rng;
use std::str::FromStr;

/// BigDecimal 运算展开 `10^n`(scale 对齐)的位数上限,**防天量分配 OOM**。
///
/// 原实现 `BigDecimal` 的 scale 是 `int`(≤ ~2.1e9),本 crate public scale 是 `i64`、且 operand 可由
/// `BigDecimal::new(_, i64::MIN)` 构造出极端 exponent;若不设界,`10^|k|` 会试图分配天量 BigInt 而 OOM/abort。
/// 取 100 万位:远超任何真实金融/科学精度需求(财务 ≤~30 位、加密 ≤~18 位),`10^1e6` 仅 ~415KB BigInt、计算极快。
/// 超界返 `Err(Scale)`,**不 panic、不 OOM**,符合 crate 错误模型。
pub const MAX_DECIMAL_EXPANSION: u64 = 1_000_000;

/// 校验 `10^exp` 展开位数不超 [`MAX_DECIMAL_EXPANSION`](防 OOM),返 `usize` 供 `int_pow`。超界 → `Err(Scale)`。
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

/// `10^exp`(BigInt)。`exp` 须已过 [`checked_decimal_exp`]。
///
/// # 参数
/// - `exp`: 十进制缩放指数。
fn pow10_bigint(exp: usize) -> bigdecimal::num_bigint::BigInt {
    use bigdecimal::num_bigint::BigInt;
    use bigdecimal::num_traits::pow::pow as int_pow;
    int_pow(BigInt::from(10), exp)
}

/// 按 `mode` 把 `num_abs / den_abs`(均 ≥ 0,`den_abs > 0`)舍入为带符号 `BigInt` 商。`Unnecessary` 遇余数非 0 → `Err`。
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

/// 把 `val` 舍入到目标 `scale`,**全程 checked + BigInt**,不调 `bigdecimal::with_scale_round`。对照 原实现
/// `BigDecimal.setScale(int, mode)`。
///
/// **`bigdecimal::with_scale_round` 内部多处 i64 派生量(`new_scale - self.scale`、
/// `digit_count - self.scale`、`-new_scale`)在近 `i64::MIN/MAX` operand scale 下整数溢出 panic——只包一层 delta
/// 检查挡不住。这里自实现:`Q = round(unscaled × 10^(scale − cur))`,放大(`k≥0`)精确、缩小(`k<0`)按 mode 舍入,
/// 所有 scale 算术用 `checked_*` + [`MAX_DECIMAL_EXPANSION`],极端 scale 返 `Err(Scale)`。
///
/// # 参数
/// - `val`: 要写入 Redis 或发送到下游的值。
/// - `scale`: 小数精度或缩放位数。
/// - `mode`: 当前操作使用的编码、舍入、订阅或执行模式。
fn set_scale_round_checked(val: &BigDecimal, scale: i64, mode: RoundingMode) -> Result<BigDecimal> {
    use bigdecimal::num_traits::{Signed, Zero};
    let (unscaled, cur) = val.as_bigint_and_exponent(); // 值 = unscaled × 10^-cur
                                                        //零值 O(1) 短路——`0` 在任何 scale 都是 `0`,无需展开 `10^n`(对照 原实现 `0.setScale(n)`)。
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

/// checked BigDecimal 加/减,**不调 bigdecimal 的 `+`/`-`**(其内部 `(lhs.scale - rhs.scale) as u64` 在极端 operand
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

/// checked BigDecimal 乘,**不调 bigdecimal 的 `*`**(其内部 `self.scale + rhs.scale` 未 checked,极端 operand scale
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
    // **饱和值**(对照 原实现 `0.multiply`:scale 溢出不抛、夹到 `Integer.MAX_VALUE`;Rust i64 同理用 saturating_add)。
    if au.is_zero() || bu.is_zero() {
        return Ok(BigDecimal::new(0.into(), ae.saturating_add(be)));
    }
    // 非零:scale 和溢出是真实不可表示(对照 原实现 非零 `Underflow` 抛异常)→ `Err(Scale)`。
    let scale = ae
        .checked_add(be)
        .ok_or_else(|| NumericError::Scale(format!("decimal 乘法 scale 溢出:{ae}+{be}")))?;
    Ok(BigDecimal::new(au * bu, scale))
}

// ==================== BigDecimal 段(对照 原实现 Numeric BigDecimal API)====================

/// BigDecimal 加法(精确)。对照 原实现 `add(BigDecimal,BigDecimal)`。
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

/// BigDecimal 减法(精确)。对照 原实现 `subtract`。返 `Result`(同 [`add`])。
///
/// # 参数
///
/// - `a`: 被减数，按其 BigDecimal 数值和 scale 参与对齐。
/// - `b`: 减数，按其 BigDecimal 数值和 scale 参与对齐。
pub fn subtract(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_add_bd(a, b, true)
}

/// BigDecimal 乘法(精确)。对照 原实现 `multiply(BigDecimal,BigDecimal)`。返 `Result`(同 [`add`])。
///
/// # 参数
///
/// - `a`: 左侧乘数。
/// - `b`: 右侧乘数。
pub fn multiply(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal> {
    checked_mul_bd(a, b)
}

/// BigDecimal 乘法,结果对齐到 `scale`(HALF_UP)。对照 原实现 `multiply(a,b,scale)`。
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

/// BigDecimal 除法,指定 `scale`(HALF_UP;避免无限循环小数)。对照 原实现 `divide(a,b,scale)`。
///
/// # 参数
///
/// - `a`: 被除数。
/// - `b`: 除数；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 结果需要保留的小数位数。
pub fn divide(a: &BigDecimal, b: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    divide_mode(a, b, scale, RoundingMode::HalfUp)
}

/// BigDecimal 除法,指定 `scale` 与舍入模式。对照 原实现 `divide(a,b,scale,mode)`。
///
/// `b==0` → `Err(DivByZero)`;`Unnecessary` 模式遇需舍入 → `Err(RoundingNecessary)`(对照 原实现
/// `divide(...UNNECESSARY)` 抛 `ArithmeticException`)。
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

/// 按目标 `scale` **直接用 BigInt 整数除法 + 舍入**,真任意精度。对照 原实现 `BigDecimal.divide(b, scale, mode)`。
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
                                                     //被除数为 0(除数非 0)→ 结果恒 0,O(1) 返目标 scale 的零,不展开(对照 原实现 `0.divide(x,scale)`)。
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

/// BigDecimal 截到 `scale` 位(HALF_UP)。对照 原实现 `align(val,scale)`。
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

/// BigDecimal 向上(+∞,CEILING)取到 `scale`。对照 原实现 `alignUp`。返 `Result`(同 [`align`])。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的 BigDecimal 值。
/// - `scale`: 目标小数位数。
pub fn align_up(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::Ceiling)
}

/// BigDecimal 向下(−∞,FLOOR)取到 `scale`。对照 原实现 `alignDown`。返 `Result`(同 [`align`])。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的 BigDecimal 值。
/// - `scale`: 目标小数位数。
pub fn align_down(val: &BigDecimal, scale: i64) -> Result<BigDecimal> {
    set_scale_round_checked(val, scale, RoundingMode::Floor)
}

/// BigDecimal 求和,空 → `0`。对照 原实现 `sum(BigDecimal...)` / `sum(Iterable)`。
///
/// **返 `Result`**:走 `checked_add_bd`,极端 operand scale 返 `Err(Scale)` 而非 panic。
/// 注:原实现 版会**跳过 `null` 元素**;Rust 用类型系统消除了 `null`(此处是 `&BigDecimal`,无 null 可传),
/// 故无需 null 跳过——这是语言习惯收敛,非漏实现。需表达"可选缺失"由调用方先 `filter`/用 `Option` 滤掉。
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

/// BigDecimal 平均值,对齐到 `scale`(HALF_UP);空 → `0`。对照 原实现 `avg(Iterable,scale)`。
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

/// 指定精度的最小正值 `1×10^-scale`。对照 原实现 `scaleMin(int)`(`BigDecimal.valueOf(1, scale)`,**不受 8 位限**)。
/// 构造本身不展开 `10^n`(只设 unscaled=1 + scale 字段),故任意 `i64 scale` 都安全;但把结果送入 `align*`/`divide`
/// 等会触发 `MAX_DECIMAL_EXPANSION` 边界。
///
///`scale_min` 走 BigDecimal,**不再受 fixed `MAX_SCALE=8` 限**;`scale<0` 合法
/// (`scaleMin(-1)=10`,对照 原实现)。
///
/// # 参数
///
/// - `scale`: 小数位数；返回值为 `1 × 10^-scale`。
pub fn scale_min(scale: i64) -> BigDecimal {
    // BigDecimal(unscaledValue=1, scale):1 × 10^-scale。
    BigDecimal::new(1.into(), scale)
}

/// 在最小精度位上取步长随机值,范围 `[min, (step-1)×min]`(`min=10^-scale`)。对照 原实现 `randomStep(long,scale)`。
///
/// `step<=0` → `scale_min(scale)`;`rand=0` 时也返回 `min`(不返 0,与 原实现 一致)。构造不展开,任意 `i64 scale` 安全。
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
    let unscaled = if rand == 0 { 1 } else { rand };
    BigDecimal::new(unscaled.into(), scale)
}

/// `BigDecimal` 步长版,对照 原实现 `randomStep(BigDecimal step, int scale)` = `randomStep(step.longValue(), scale)`。
///
/// `step` 先按 原实现 `BigDecimal.longValue()` 截断为整数(朝零截尾)取 `i64`;**超 i64 域时饱和**到
/// `i64::MAX`/`MIN`(有意安全偏离 原实现 的低 64 位回绕——回绕对"步长"无意义且易出错)。
///
/// # 参数
///
/// - `step`: BigDecimal 形式的随机步长上界，会先截断为 `i64`。
/// - `scale`: 最小单位的小数位数。
pub fn random_step_bd(step: &BigDecimal, scale: i64) -> BigDecimal {
    random_step(bigdecimal_to_i64_saturating(step), scale)
}

/// 原实现 `BigDecimal.longValue()` 等价:**朝零截断**取整数部分,超 `i64` 域**按符号饱和**。
///
/// **纯位数 / 字符串运算实现,不调 `with_scale_round`(其极端 scale 会 panic),也不构造 `10^exp`
/// (避免 OOM)。修正"仅凭 exponent 正负判断"的语义错误——如 `3×10^(MAX+1)` scale=`MAX+1` 的真实值是 `3`,
/// 旧实现误判为 `0`。
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
    let saturate = if negative { i64::MIN } else { i64::MAX };
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
            let signed = if negative { -mag } else { mag };
            signed.clamp(i64::MIN as i128, i64::MAX as i128) as i64
        }
        Err(_) => saturate,
    }
}

// ==================== Decimal 后缀便捷段(String/double → BigDecimal)====================
// 对照 原实现 `addDecimal/subtractDecimal/...`:String 用 `new BigDecimal(s)` 精确解析,double 先转字符串避免二进制噪音。

/// 解析字符串为 BigDecimal(精确,对照 原实现 `new BigDecimal(String)`)。
///
/// # 参数
///
/// - `s`: 十进制或科学计数法文本，按 BigDecimal 精确解析。
pub fn parse(s: &str) -> Result<BigDecimal> {
    BigDecimal::from_str(s).map_err(|e| NumericError::Parse(format!("BigDecimal 解析失败: {e}")))
}

/// double → BigDecimal,**经字符串**精确化(对照 原实现 `BigDecimal.valueOf(double)` 避免 `new BigDecimal(0.1)` 噪音)。
///
/// **scale / `toString` 偏离(只保证数值等价)**:原实现 `BigDecimal.valueOf(double)` 内部走 `Double.toString`,
/// 会保留量级 scale(`1.0 → "1.0"` scale=1、`1e-7 → "1.0E-7"` scale=8);本 crate 用 Rust `format!("{v}")`
/// (`1.0 → "1"` scale=0),故结果与 原实现 **`compareTo` 数值相等**,但 **scale / `toString` / `equals` 不一定一致**。
/// 若下游依赖 BigDecimal 的 scale(如 DB decimal 列精度、JSON 文本、`equals`),需另补 原实现 `Double.toString`
/// 兼容 formatter。
///
/// # 参数
/// - `v`: 待转换的值。
fn from_f64(v: f64) -> Result<BigDecimal> {
    if !v.is_finite() {
        return Err(NumericError::Parse(format!("非有限 f64: {v}")));
    }
    parse(&format!("{v}"))
}

/// `addDecimal(String,String)`。
///
/// # 参数
///
/// - `a`: 左侧十进制文本加数。
/// - `b`: 右侧十进制文本加数。
pub fn add_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    add(&parse(a)?, &parse(b)?)
}

/// `addDecimal(double,double)`。
///
/// # 参数
///
/// - `a`: 左侧 f64 加数，会先转换为 BigDecimal。
/// - `b`: 右侧 f64 加数，会先转换为 BigDecimal。
pub fn add_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    add(&from_f64(a)?, &from_f64(b)?)
}

/// `subtractDecimal(String,String)`。
///
/// # 参数
///
/// - `a`: 十进制文本被减数。
/// - `b`: 十进制文本减数。
pub fn subtract_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    subtract(&parse(a)?, &parse(b)?)
}

/// `subtractDecimal(double,double)`。
///
/// # 参数
///
/// - `a`: f64 被减数，会先转换为 BigDecimal。
/// - `b`: f64 减数，会先转换为 BigDecimal。
pub fn subtract_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    subtract(&from_f64(a)?, &from_f64(b)?)
}

/// `multiplyDecimal(String,String)`。
///
/// # 参数
///
/// - `a`: 左侧十进制文本乘数。
/// - `b`: 右侧十进制文本乘数。
pub fn multiply_decimal_str(a: &str, b: &str) -> Result<BigDecimal> {
    multiply(&parse(a)?, &parse(b)?)
}

/// `multiplyDecimal(double,double)`。
///
/// # 参数
///
/// - `a`: 左侧 f64 乘数，会先转换为 BigDecimal。
/// - `b`: 右侧 f64 乘数，会先转换为 BigDecimal。
pub fn multiply_decimal_f64(a: f64, b: f64) -> Result<BigDecimal> {
    multiply(&from_f64(a)?, &from_f64(b)?)
}

/// `multiplyDecimal(String,String,scale)`。
///
/// # 参数
///
/// - `a`: 左侧十进制文本乘数。
/// - `b`: 右侧十进制文本乘数。
/// - `scale`: 乘积需要保留的小数位数。
pub fn multiply_decimal_str_scale(a: &str, b: &str, scale: i64) -> Result<BigDecimal> {
    multiply_scale(&parse(a)?, &parse(b)?, scale)
}

/// `multiplyDecimal(double,double,scale)`。
///
/// # 参数
///
/// - `a`: 左侧 f64 乘数，会先转换为 BigDecimal。
/// - `b`: 右侧 f64 乘数，会先转换为 BigDecimal。
/// - `scale`: 乘积需要保留的小数位数。
pub fn multiply_decimal_f64_scale(a: f64, b: f64, scale: i64) -> Result<BigDecimal> {
    multiply_scale(&from_f64(a)?, &from_f64(b)?, scale)
}

/// `divideDecimal(String,String,scale)`。
///
/// # 参数
///
/// - `a`: 十进制文本被除数。
/// - `b`: 十进制文本除数；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 商需要保留的小数位数。
pub fn divide_decimal_str(a: &str, b: &str, scale: i64) -> Result<BigDecimal> {
    divide(&parse(a)?, &parse(b)?, scale)
}

/// `divideDecimal(double,double,scale)`。
///
/// # 参数
///
/// - `a`: f64 被除数，会先转换为 BigDecimal。
/// - `b`: f64 除数，会先转换为 BigDecimal；为 `0` 时返回 [`NumericError::DivByZero`]。
/// - `scale`: 商需要保留的小数位数。
pub fn divide_decimal_f64(a: f64, b: f64, scale: i64) -> Result<BigDecimal> {
    divide(&from_f64(a)?, &from_f64(b)?, scale)
}

/// `divideDecimal(String,String,scale,mode)`。
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

/// `divideDecimal(double,double,scale,mode)`。
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

/// `alignDecimal(String,scale)`。
///
/// # 参数
///
/// - `val`: 需要按目标精度对齐的十进制文本。
/// - `scale`: 目标小数位数。
pub fn align_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align(&parse(val)?, scale)
}

/// `alignDecimal(double,scale)`。
///
/// # 参数
///
/// - `val`: 需要按目标精度对齐的 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align(&from_f64(val)?, scale)
}

/// `alignUpDecimal(String,scale)`。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的十进制文本。
/// - `scale`: 目标小数位数。
pub fn align_up_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align_up(&parse(val)?, scale)
}

/// `alignUpDecimal(double,scale)`。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_up_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align_up(&from_f64(val)?, scale)
}

/// `alignDownDecimal(String,scale)`。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的十进制文本。
/// - `scale`: 目标小数位数。
pub fn align_down_decimal_str(val: &str, scale: i64) -> Result<BigDecimal> {
    align_down(&parse(val)?, scale)
}

/// `alignDownDecimal(double,scale)`。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_down_decimal_f64(val: f64, scale: i64) -> Result<BigDecimal> {
    align_down(&from_f64(val)?, scale)
}

/// `sumDecimal(String...)`(跳过解析失败?——原实现 直接 `new BigDecimal(v)`,这里非法即 `Err`)。
///
/// # 参数
///
/// - `vals`: 待求和的一组十进制文本；任一项解析失败会返回错误。
pub fn sum_decimal_str(vals: &[&str]) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|s| parse(s)).collect();
    sum(&parsed?)
}

/// `sumDecimal(double...)`。
///
/// # 参数
///
/// - `vals`: 待求和的一组 f64 值；非有限值会返回解析错误。
pub fn sum_decimal_f64(vals: &[f64]) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|v| from_f64(*v)).collect();
    sum(&parsed?)
}

/// `avgDecimal(String[],scale)`。
///
/// # 参数
///
/// - `vals`: 待求平均值的一组十进制文本；空数组返回 `0`。
/// - `scale`: 平均值需要保留的小数位数。
pub fn avg_decimal_str(vals: &[&str], scale: i64) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|s| parse(s)).collect();
    avg(&parsed?, scale)
}

/// `avgDecimal(double[],scale)`。
///
/// # 参数
///
/// - `vals`: 待求平均值的一组 f64 值；空数组返回 `0`，非有限值会返回解析错误。
/// - `scale`: 平均值需要保留的小数位数。
pub fn avg_decimal_f64(vals: &[f64], scale: i64) -> Result<BigDecimal> {
    let parsed: Result<Vec<BigDecimal>> = vals.iter().map(|v| from_f64(*v)).collect();
    avg(&parsed?, scale)
}
