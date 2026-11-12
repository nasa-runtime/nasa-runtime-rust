//! f64 便捷算术。
//!
//! 操作数先按 scale 转换为 i128 定点值，执行相应算术后再除以 `10^scale` 返回 f64。
//! 定点入口位于 crate 根，BigDecimal 入口位于 `decimal`，本模块保留独立的浮点 API。
//! 输入转换的半数取整朝正无穷；NaN、无穷和超出表示范围的结果返回错误。
//! 中间值不使用溢出回绕，余数和最终浮点输出仍受 f64 精度限制。

use crate::{
    align_rounding, check_scale, divide as fixed_divide, multiply as fixed_multiply, pow10,
    to_fixed_f64, NumericError, Result, RoundingMode, DEFAULT_SCALE,
};

/// 业务作用：为已校验定点精度取得浮点缩放因子。
///
/// 参数说明：
/// - `scale`：调用方已保证不超过 MAX_SCALE 的精度。
///
/// 返回：返回可精确表示的 10 的 scale 次幂，不重复检查输入范围。
#[inline]
fn m(scale: u32) -> f64 {
    pow10(scale) as f64
}

/// 业务作用：先按指定精度转换为定点，再相加并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 加数。
/// - `b`：右侧有限 f64 加数。
/// - `scale`：中转定点使用的精度，不超过 MAX_SCALE。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。
pub fn add(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    let sum = af.checked_add(bf).ok_or(NumericError::Overflow)?;
    Ok(sum as f64 / m(scale))
}

/// 业务作用：先按默认八位精度转换为定点，再相加并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 加数。
/// - `b`：右侧有限 f64 加数。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。
pub fn add_default(a: f64, b: f64) -> Result<f64> {
    add(a, b, DEFAULT_SCALE)
}

/// 业务作用：先按指定精度转换为定点，再相减并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 被减数。
/// - `b`：右侧有限 f64 减数。
/// - `scale`：中转定点使用的精度，不超过 MAX_SCALE。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。
pub fn subtract(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    let diff = af.checked_sub(bf).ok_or(NumericError::Overflow)?;
    Ok(diff as f64 / m(scale))
}

/// 业务作用：先按默认八位精度转换为定点，再相减并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 被减数。
/// - `b`：右侧有限 f64 减数。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。
pub fn subtract_default(a: f64, b: f64) -> Result<f64> {
    subtract(a, b, DEFAULT_SCALE)
}

/// 业务作用：先按指定精度转换为定点，再相乘并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 乘数。
/// - `b`：右侧有限 f64 乘数。
/// - `scale`：中转定点使用的精度，不超过 MAX_SCALE。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。
pub fn multiply(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    Ok(fixed_multiply(af, bf, scale)? as f64 / m(scale))
}

/// 业务作用：先按默认八位精度转换为定点，再相乘并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 乘数。
/// - `b`：右侧有限 f64 乘数。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。
pub fn multiply_default(a: f64, b: f64) -> Result<f64> {
    multiply(a, b, DEFAULT_SCALE)
}

/// 业务作用：先按指定精度转换为定点，再相除并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 被除数。
/// - `b`：右侧有限 f64 除数。
/// - `scale`：中转定点使用的精度，不超过 MAX_SCALE。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。除数量化为零时返回 DivByZero。
pub fn divide(a: f64, b: f64, scale: u32) -> Result<f64> {
    let af = to_fixed_f64(a, scale)?;
    let bf = to_fixed_f64(b, scale)?;
    Ok(fixed_divide(af, bf, scale)? as f64 / m(scale))
}

/// 业务作用：先按默认八位精度转换为定点，再相除并转回浮点。
///
/// 参数说明：
/// - `a`：左侧有限 f64 被除数。
/// - `b`：右侧有限 f64 除数。
///
/// 返回：返回定点结果对应的 f64，输入量化和浮点输出均受精度限制；非法精度、非有限输入或整数溢出返回错误。除数量化为零时返回 DivByZero。
pub fn divide_default(a: f64, b: f64) -> Result<f64> {
    divide(a, b, DEFAULT_SCALE)
}

/// 业务作用：经定点中转，以半数朝正无穷的取整对齐浮点数。
///
/// 参数说明：
/// - `val`：待对齐的有限 f64。
/// - `scale`：目标精度，不超过 MAX_SCALE。
///
/// 返回：返回指定精度的浮点值；非法精度、非有限输入或定点范围溢出返回错误。
pub fn align(val: f64, scale: u32) -> Result<f64> {
    Ok(to_fixed_f64(val, scale)? as f64 / m(scale))
}

/// 业务作用：经定点中转，以Ceiling对齐浮点数。
///
/// 参数说明：
/// - `val`：待对齐的有限 f64。
/// - `scale`：目标精度，不超过 MAX_SCALE。
///
/// 返回：先量化到默认八位工作精度再按目标精度对齐，返回对应浮点值；非法精度、非有限输入或定点范围溢出返回错误。
pub fn align_up(val: f64, scale: u32) -> Result<f64> {
    check_scale(scale)?;
    // 经定点中转消除 f64 表示噪音,再用整数 Ceiling 对齐(与本 crate `align`/加减乘除同纪律)。
    // 工作精度取 DEFAULT_SCALE(≥scale)以保留 sub-scale 信息,让 Ceiling 判定正确。
    let work = scale.max(DEFAULT_SCALE);
    let fixed = to_fixed_f64(val, work)?; // 非有限值在此返 Err(同 align)
    let aligned = align_rounding(fixed, work, scale, RoundingMode::Ceiling)?;
    Ok(aligned as f64 / m(work))
}

/// 业务作用：经定点中转，以Floor对齐浮点数。
///
/// 参数说明：
/// - `val`：待对齐的有限 f64。
/// - `scale`：目标精度，不超过 MAX_SCALE。
///
/// 返回：先量化到默认八位工作精度再按目标精度对齐，返回对应浮点值；非法精度、非有限输入或定点范围溢出返回错误。
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
