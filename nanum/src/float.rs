//! `double`(f64)便捷算术段。**对照 原实现 `Numeric` 的 `double` 重载**(L934-L1018)。
//!
//! 原实现 用方法重载区分 `long` / `double` / `BigDecimal` 三套同名 API;Rust 无重载,故 f64 段
//! 收到 `numeric::float` 模块下(i128 定点在 crate 根、BigDecimal 在 `numeric::decimal`)。
//!
//! 算法逐值复刻 原实现(**原实现 long 不溢出域内**):各操作数先 `Math.round(x × 10^scale)`(ties→+∞,内部走定点
//! long 防漂移),再 `/10^scale` 还原 f64。**非有限值(NaN/±Inf)统一返 `Err`**(经 [`to_fixed_f64`](crate::to_fixed_f64),
//! 与定点 I/O 一致的安全偏离——原实现 `Math.round(NaN)=0`/`(±Inf)=Long.MIN/MAX,金融库不悄悄当合法数)。
//!
//! **大有限值偏离(既定设计,非缺口)**:原实现 经 `long` 中转,`Math.round` 超 i64 域会**饱和**到
//! `Long.MAX/MIN`,后续 long 运算还会**回绕**(如 `add(1e19,1.0,0)` 在 原实现 回绕为负)。本 crate 用 i128,不复刻
//! 这条 原实现 溢出 bug——见 crate 根文档「类型用 i128」一节。原实现 自身 原实现doc 也把支持域限定在
//! `|val×10^scale| < 10^16`(`Numeric.toFixed` L459-460),大有限值本就在 原实现 支持域外。

use crate::{
    align_rounding, check_scale, divide as fixed_divide, multiply as fixed_multiply, pow10,
    to_fixed_f64, NumericError, Result, RoundingMode, DEFAULT_SCALE,
};

/// 10^scale 作 f64 因子(scale 已校验 ≤8,精确可表)。
///
/// # 参数
/// - `scale`: 小数精度或缩放位数。
#[inline]
fn m(scale: u32) -> f64 {
    pow10(scale) as f64
}

/// `double` 加法,scale 位精度(内部走定点防漂移)。对照 原实现 `Numeric.add(double,double,int)`。
///
/// # 参数
///
/// - `a`: 左侧有限 f64 加数，会先按 `scale` 转成定点。
/// - `b`: 右侧有限 f64 加数，会先按 `scale` 转成定点。
/// - `scale`: 中转定点和返回值使用的小数位数。
pub fn add(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    let sum = af.checked_add(bf).ok_or(NumericError::Overflow)?;
    Ok(sum as f64 / m(scale))
}

/// `double` 加法,默认精度(scale=[`DEFAULT_SCALE`]=8)。对照 原实现 `Numeric.add(double,double)`。
///
/// # 参数
///
/// - `a`: 左侧有限 f64 加数。
/// - `b`: 右侧有限 f64 加数。
pub fn add_default(a: f64, b: f64) -> Result<f64> {
    add(a, b, DEFAULT_SCALE)
}

/// `double` 减法,scale 位精度。对照 原实现 `Numeric.subtract(double,double,int)`。
///
/// # 参数
///
/// - `a`: 有限 f64 被减数，会先按 `scale` 转成定点。
/// - `b`: 有限 f64 减数，会先按 `scale` 转成定点。
/// - `scale`: 中转定点和返回值使用的小数位数。
pub fn subtract(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    let diff = af.checked_sub(bf).ok_or(NumericError::Overflow)?;
    Ok(diff as f64 / m(scale))
}

/// `double` 减法,默认精度(scale=8)。对照 原实现 `Numeric.subtract(double,double)`。
///
/// # 参数
///
/// - `a`: 有限 f64 被减数。
/// - `b`: 有限 f64 减数。
pub fn subtract_default(a: f64, b: f64) -> Result<f64> {
    subtract(a, b, DEFAULT_SCALE)
}

/// `double` 乘法,scale 位精度(复用 i128 定点乘核)。对照 原实现 `Numeric.multiply(double,double,int)`。
///
/// # 参数
///
/// - `a`: 左侧有限 f64 乘数，会先按 `scale` 转成定点。
/// - `b`: 右侧有限 f64 乘数，会先按 `scale` 转成定点。
/// - `scale`: 中转定点和返回值使用的小数位数。
pub fn multiply(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    Ok(fixed_multiply(af, bf, scale)? as f64 / m(scale))
}

/// `double` 乘法,默认精度(scale=8)。对照 原实现 `Numeric.multiply(double,double)`。
///
/// # 参数
///
/// - `a`: 左侧有限 f64 乘数。
/// - `b`: 右侧有限 f64 乘数。
pub fn multiply_default(a: f64, b: f64) -> Result<f64> {
    multiply(a, b, DEFAULT_SCALE)
}

/// `double` 除法,scale 位精度(复用 i128 定点除核;b 折定点为 0 → `DivByZero`)。
/// 对照 原实现 `Numeric.divide(double,double,int)`。
///
/// # 参数
///
/// - `a`: 有限 f64 被除数，会先按 `scale` 转成定点。
/// - `b`: 有限 f64 除数，会先按 `scale` 转成定点；折算为 `0` 时返回除零错误。
/// - `scale`: 中转定点和返回值使用的小数位数。
pub fn divide(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    Ok(fixed_divide(af, bf, scale)? as f64 / m(scale))
}

/// `double` 除法,默认精度(scale=8)。对照 原实现 `Numeric.divide(double,double)`。
///
/// # 参数
///
/// - `a`: 有限 f64 被除数。
/// - `b`: 有限 f64 除数；折算为 `0` 时返回除零错误。
pub fn divide_default(a: f64, b: f64) -> Result<f64> {
    divide(a, b, DEFAULT_SCALE)
}

/// `double` 截取到 scale 位(HALF_UP/ties→+∞)。对照 原实现 `Numeric.align(double,int)`。
///
/// # 参数
///
/// - `val`: 需要对齐到目标精度的有限 f64 值。
/// - `scale`: 目标小数位数。
pub fn align(val: f64, scale: u32) -> Result<f64> {
    Ok(to_fixed_f64(val, scale)? as f64 / m(scale))
}

/// `double` 向上截取到 scale 位(`ceil`)。对照 原实现 `Numeric.alignUp(double,int)`。
/// 非有限值返 `Err`(同 [`crate::to_fixed_f64`] 安全偏离)。
///
/// # 参数
///
/// - `val`: 需要向正无穷方向对齐的有限 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_up(val: f64, scale: u32) -> Result<f64> {
    check_scale(scale)?;
    // 经定点中转消除 f64 表示噪音,再用整数 Ceiling 对齐(与本 crate `align`/加减乘除同纪律)。
    // 旧实现 `(val*f).ceil()` 会因 0.07 的 f64 表示尾部噪音多进一格(align_up(0.07,2)→0.08)。
    // 工作精度取 DEFAULT_SCALE(≥scale)以保留 sub-scale 信息,让 Ceiling 判定正确。
    let work = scale.max(DEFAULT_SCALE);
    let fixed = to_fixed_f64(val, work)?; // 非有限值在此返 Err(同 align)
    let aligned = align_rounding(fixed, work, scale, RoundingMode::Ceiling)?;
    Ok(aligned as f64 / m(work))
}

/// `double` 向下截取到 scale 位(`floor`)。对照 原实现 `Numeric.alignDown(double,int)`。
/// 非有限值返 `Err`。
///
/// # 参数
///
/// - `val`: 需要向负无穷方向对齐的有限 f64 值。
/// - `scale`: 目标小数位数。
pub fn align_down(val: f64, scale: u32) -> Result<f64> {
    check_scale(scale)?;
    // 同 align_up:定点中转消除 f64 噪音,再用整数 Floor(向 -∞,保持本函数原 `floor` 语义;
    // 正数场景与 arithmetic::align_down 的 Down 一致)对齐。旧实现 `(val*f).floor()` 会少一格
    // (align_down(0.29,2)→0.28)。
    let work = scale.max(DEFAULT_SCALE);
    let fixed = to_fixed_f64(val, work)?;
    let aligned = align_rounding(fixed, work, scale, RoundingMode::Floor)?;
    Ok(aligned as f64 / m(work))
}
