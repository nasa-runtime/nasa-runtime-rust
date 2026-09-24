//! # nanum —— 定点与任意精度算术
//!
//! 面向撮合、金额、价格和最小变动单位的显式精度运算。定点值表示为
//! `真实值 = mantissa × 10^(-scale)`，mantissa 使用 i128，scale 上限为 [`MAX_SCALE`]。
//!
//! ## 算术与舍入合同
//! - `add`、`subtract` 和 `align*` 使用整数路径。
//! - `multiply` 与 `divide` 的整数部分使用整数、余数部分使用 f64，并支持全部八种 [`RoundingMode`]。
//! - `to_fixed_f64` 按 `floor(value × 10^scale + 0.5)` 处理 tie，因此 tie 朝正无穷舍入。
//! - `multiply` 与 `divide` 的 `HalfUp` 为 tie 远离零；调用方不能把两套 tie 规则混用。
//! - `to_fixed_str` 先解析为 f64，只接受 Rust `str::parse::<f64>()` 支持的十进制或科学计数法输入。
//!
//! ## 精度与失败边界
//! - 定点 scale 大于 8、除零、i128 溢出、非有限 f64、解析失败以及 `Unnecessary` 实际需要舍入时返回
//!   [`NumericError`]，不执行 wrapping，也不触发 panic。
//! - [`decimal`] 提供不受定点 scale 上限限制的 BigDecimal 运算；需要展开 `10^n` 的操作受
//!   `decimal::MAX_DECIMAL_EXPANSION` 保护。
//! - [`float`] 是 f64 便捷入口；涉及 NaN 总序时应使用 [`eq_f64`] 等专用比较函数。
//! - `decimal::random_step_bd` 超出 i64 域时按符号饱和，`next_int_max(i32::MAX)` 保持有效。
//!
//! ## API 入口
//! - 定点算术：[`add`]、[`subtract`]、[`multiply`]、[`divide`]。
//! - 精度对齐：[`align`]、[`align_up`]、[`align_down`]、[`is_aligned`]。
//! - 转换与显示：[`to_fixed_str`]、[`to_fixed_f64`]、[`to_plain_string`]、[`to_big_decimal`]。
//! - 任意精度：[`decimal`]；f64 便捷运算：[`float`]。

mod arithmetic;
/// BigDecimal 高精度段与 Decimal 后缀便捷入口；模块命名空间用于区分 i128 定点同名函数。
pub mod decimal;
/// f64 便捷算术入口；模块命名空间用于区分 i128 与 BigDecimal 同名函数。
pub mod float;
mod io;
mod random;

pub use arithmetic::{
    add, align, align_down, align_rounding, align_up, divide, divide_default, divide_rounding,
    is_aligned, multiply, multiply_default, multiply_rounding, subtract,
};
pub use io::{
    to_big_decimal, to_big_decimal_default, to_fixed_f64, to_fixed_f64_default, to_fixed_str,
    to_fixed_str_default, to_plain_string, to_plain_string_default, to_plain_string_display,
    to_plain_string_f64, to_plain_string_f64_default, to_plain_string_f64_display,
    to_plain_string_raw, to_plain_string_raw_display, to_plain_string_raw_f64,
    to_plain_string_raw_f64_display,
};
pub use random::{next_int, next_int_max};
// scale_min 与 random_step 返回 BigDecimal，不受 fixed 8 位限制；后续展开运算仍受资源边界保护。
/// 任意精度十进制类型。
pub use bigdecimal::BigDecimal;
pub use decimal::{random_step, scale_min};

/// 撮合默认精度，表示乘以 10^8 的定点单位。
pub const DEFAULT_SCALE: u32 = 8;

/// 定点 scale 上限；超过该值时返回 `NumericError::Scale`，避免 f64 的 10^s 超出安全整数域。
pub const MAX_SCALE: u32 = 8;

/// 定点与 BigDecimal 运算支持的舍入模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundingMode {
    /// 始终远离零(进位)。
    Up,
    /// 始终朝零(截断)。
    Down,
    /// 朝 +∞。
    Ceiling,
    /// 朝 −∞。
    Floor,
    /// 四舍五入,tie 远离零(默认)。
    HalfUp,
    /// 五舍六入,tie 朝零。
    HalfDown,
    /// 银行家舍入,tie 朝最近偶数。
    HalfEven,
    /// 断言无需舍入:若实际需要舍入则返 [`NumericError::RoundingNecessary`]。
    Unnecessary,
}

/// 定点算术错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumericError {
    /// i128 溢出(操作数过大)。
    Overflow,
    /// 除数为零。
    DivByZero,
    /// 非法 scale(越界 `[0, MAX_SCALE]` 或 `toScale > fromScale`)。
    Scale(String),
    /// 字符串解析失败(非法数字格式)。
    Parse(String),
    /// `RoundingMode::Unnecessary` 下却需要舍入。
    RoundingNecessary,
    /// 非法范围(`next_int` 的 `min > max` / `next_int_max` 的 `max < 0`)。
    Range(String),
}

impl core::fmt::Display for NumericError {
    /// 业务作用: 实现可读格式化输出,供错误链、日志和调试展示。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            NumericError::Overflow => write!(f, "numeric overflow (i128 范围溢出)"),
            NumericError::DivByZero => write!(f, "division by zero"),
            NumericError::Scale(s) => write!(f, "invalid scale: {s}"),
            NumericError::Parse(s) => write!(f, "parse error: {s}"),
            NumericError::RoundingNecessary => {
                write!(f, "rounding necessary but mode = Unnecessary")
            }
            NumericError::Range(s) => write!(f, "invalid range: {s}"),
        }
    }
}

impl std::error::Error for NumericError {}

/// 本 crate 统一 `Result`。
pub type Result<T> = core::result::Result<T, NumericError>;

/// 业务作用: 10^scale(i128),scale 已校验在 `[0, MAX_SCALE=8]` 时不溢出。
pub(crate) fn pow10(scale: u32) -> i128 {
    10i128.pow(scale)
}

/// 业务作用: 校验单 scale ∈ `[0, MAX_SCALE]`。
pub(crate) fn check_scale(scale: u32) -> Result<()> {
    if scale > MAX_SCALE {
        return Err(NumericError::Scale(format!(
            "scale 必须 ∈ [0, {MAX_SCALE}],实际 {scale}"
        )));
    }
    Ok(())
}

/// 业务作用: 校验双 scale(align*):`fromScale ∈ [0, MAX]`,`toScale ∈ [0, fromScale]`。
pub(crate) fn check_scale_pair(from_scale: u32, to_scale: u32) -> Result<()> {
    check_scale(from_scale)?;
    if to_scale > from_scale {
        return Err(NumericError::Scale(format!(
            "toScale 必须 ∈ [0, fromScale={from_scale}],实际 {to_scale}"
        )));
    }
    Ok(())
}

// ==================== 整数工具(纯整数,无保真歧义)====================

/// 业务作用: 整数的十进制字符串长度,**负数比正数多 1 位**(算上负号)。
///
/// 例:`string_size(0)=1`、`string_size(999)=3`、`string_size(-12)=3`。
///
/// # 参数
///
/// - `num`: 待计算十进制展示长度的整数。
pub fn string_size(num: i128) -> usize {
    if num == 0 {
        return 1;
    }
    let neg = num < 0;
    // i128::MIN 的 unsigned_abs 安全(不会溢出)。
    let digits = num.unsigned_abs().ilog10() as usize + 1;
    digits + usize::from(neg)
}

/// 业务作用: 是否偶数。
///
/// # 参数
///
/// - `num`: 待检查奇偶性的整数。
pub fn is_even(num: i128) -> bool {
    num & 1 == 0
}

/// 业务作用: 是否奇数。
///
/// # 参数
///
/// - `num`: 待检查奇偶性的整数。
pub fn is_odd(num: i128) -> bool {
    num & 1 != 0
}

// 奇偶入口统一接收 i128，其它整数类型可在不丢失数值的条件下转换后调用。

/// 业务作用：把整数的十进制 ASCII 字节写入指定缓冲区，负数包含负号。
/// 返回：写入字节数；从 start 起容量不足时发生切片越界 panic，调用方应先按 `string_size` 预留。
///
/// # 参数
///
/// - `number`: 要写入十进制 ASCII 文本的整数。
/// - `chars`: 目标字节缓冲区，调用方需保证从 `start` 起容量足够。
/// - `start`: 写入起始下标。
pub fn copy_to_char_array(number: i64, chars: &mut [u8], start: usize) -> usize {
    copy_to_char_array_with_len(number, string_size(number as i128), chars, start)
}

/// 业务作用：在指定宽度内右对齐写入整数的十进制 ASCII 字节。
/// 宽度大于 `string_size(number)` 时左侧原字节保持不变；`i64::MIN` 使用完整十进制文本。
/// 返回：写入字节数；宽度放不下文本或缓冲区不足 `start + length` 时 panic。
///
/// # 参数
///
/// - `number`: 要写入十进制 ASCII 文本的整数。
/// - `length`: 预留写入宽度，通常等于 [`string_size`]`(number)`。
/// - `chars`: 目标字节缓冲区，调用方需保证从 `start` 起容量足够。
/// - `start`: 写入起始下标。
pub fn copy_to_char_array_with_len(
    number: i64,
    length: usize,
    chars: &mut [u8],
    start: usize,
) -> usize {
    let s = number.to_string();
    let end = start + length;
    chars[end - s.len()..end].copy_from_slice(s.as_bytes());
    length
}

// 泛型比较遵循 PartialEq/PartialOrd；浮点 NaN 不具备普通全序。
// 需要 NaN 与带符号零的确定顺序时，使用专用 f64 比较族；可选值的处理由调用方决定。

/// 业务作用: 按 PartialEq 判断两个值是否相等。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn eq<T: PartialEq>(a: T, b: T) -> bool {
    a == b
}

/// 业务作用: 按 PartialEq 判断两个值是否不等。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn ne<T: PartialEq>(a: T, b: T) -> bool {
    a != b
}

/// 业务作用: 大于。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn gt<T: PartialOrd>(a: T, b: T) -> bool {
    a > b
}

/// 业务作用: 大于等于。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn ge<T: PartialOrd>(a: T, b: T) -> bool {
    a >= b
}

/// 业务作用: 小于。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn lt<T: PartialOrd>(a: T, b: T) -> bool {
    a < b
}

/// 业务作用: 小于等于。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn le<T: PartialOrd>(a: T, b: T) -> bool {
    a <= b
}

// f64 专用比较把全部 NaN 规范化为相同值，并置于所有非 NaN 之后；负零小于正零。
// compat 前缀只表示该比较与舍入合同，不表示另一套运行时或协议模式。

/// 业务作用：按确定全序比较 f64，数值相同时使用规范化位模式区分带符号零。
/// 所有 NaN 视为相等且大于非 NaN，负零小于正零。
/// 返回：按该全序得到的 Ordering。
///
/// # 参数
/// - `a`: 左侧 f64 值。
/// - `b`: 右侧 f64 值。
fn compat_double_compare(a: f64, b: f64) -> core::cmp::Ordering {
    if a < b {
        return core::cmp::Ordering::Less;
    }
    if a > b {
        return core::cmp::Ordering::Greater;
    }

    // 业务作用：数值比较无法区分带符号零与 NaN 时，规范化位模式提供确定顺序且忽略 NaN payload。
    /// 业务作用：转成兼容 IEEE-754 二进制浮点的规范化位模式。
    ///
    /// # 参数
    /// - `v`: 待转换的 f64 值。
    fn to_canonical_bits(v: f64) -> i64 {
        let bits = if v.is_nan() {
            f64::NAN.to_bits()
        } else {
            v.to_bits()
        };
        bits as i64
    }
    to_canonical_bits(a).cmp(&to_canonical_bits(b))
}

/// 业务作用: 按规范化浮点全序判断相等，任意两个 NaN 视为相等。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn eq_f64(a: f64, b: f64) -> bool {
    compat_double_compare(a, b).is_eq()
}

/// 业务作用: f64 不等(`= !eq_f64`)。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn ne_f64(a: f64, b: f64) -> bool {
    !eq_f64(a, b)
}

/// 业务作用: 按规范化浮点全序判断大于，NaN 大于任意非 NaN。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn gt_f64(a: f64, b: f64) -> bool {
    compat_double_compare(a, b).is_gt()
}

/// 业务作用: 按规范化浮点全序判断大于或等于。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn ge_f64(a: f64, b: f64) -> bool {
    compat_double_compare(a, b).is_ge()
}

/// 业务作用: 按规范化浮点全序判断小于。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn lt_f64(a: f64, b: f64) -> bool {
    !ge_f64(a, b)
}

/// 业务作用: 按规范化浮点全序判断小于或等于。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn le_f64(a: f64, b: f64) -> bool {
    !gt_f64(a, b)
}
