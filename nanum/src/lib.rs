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
    /// 非法范围(`next_int` 的 `min > max` / `next_int_max` 的 `max < 0`;对照 原实现 `IllegalArgumentException`)。
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

/// 业务作用: 整数的十进制字符串长度,**负数比正数多 1 位**(算上负号)。对照 原实现 `Numeric.stringSize`。
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

/// 业务作用: 是否偶数。对照 原实现 `Numeric.isEven`。
///
/// # 参数
///
/// - `num`: 待检查奇偶性的整数。
pub fn is_even(num: i128) -> bool {
    num & 1 == 0
}

/// 业务作用: 是否奇数。对照 原实现 `Numeric.isOdd`。
///
/// # 参数
///
/// - `num`: 待检查奇偶性的整数。
pub fn is_odd(num: i128) -> bool {
    num & 1 != 0
}

// 注:原实现 的 `isEven`/`isOdd` 各有 long/int/byte/short 四重载(仅因 原实现 基元 `&` 不自动加宽);
// Rust 单一 `i128` 版即覆盖全部(调用方 `is_even(x as i128)`),无数值分歧,故不复刻四重载。

/// 业务作用: 把整数 `number` 的十进制 ASCII 字节(负数含 `-`)写入 `chars[start..]`,返回写入长度。
/// 对照既有系统的 `Numeric.copyToCharArray(long,char[],int)`（其中 JDK 位运算只是 `StringBuilder` 的性能优化，
/// 输出与本实现逐字节一致;原实现 `char[]` 存 ASCII 数字,Rust 用字节缓冲是地道等价)。
///
/// # Panics
/// `chars[start..]` 容量不足 [`string_size`] 时 panic(切片越界,与 原实现 `ArrayIndexOutOfBounds` 一致)。
///
/// # 参数
///
/// - `number`: 要写入十进制 ASCII 文本的整数。
/// - `chars`: 目标字节缓冲区，调用方需保证从 `start` 起容量足够。
/// - `start`: 写入起始下标。
pub fn copy_to_char_array(number: i64, chars: &mut [u8], start: usize) -> usize {
    copy_to_char_array_with_len(number, string_size(number as i128), chars, start)
}

/// 业务作用: 同 [`copy_to_char_array`],但**显式给定 `length`**(对照 原实现 `copyToCharArray(long,int,char[],int)`)。
/// 原实现 用 `length` 预定位 `charPos = start + length` 再右往左填,故字节**右对齐**写入 `chars[start..start+length]`;
/// `length` 应 = [`string_size`]`(number)`(传过大则左侧位保持原值,与 原实现 一致)。
///
/// **安全偏离**:`i64::MIN` 直接经 `to_string()` 输出正确的 `-9223372036854775808`,**不复刻 原实现
/// `number = -number` 对 `Long.MIN_VALUE` 的取负溢出行为**。
///
/// # Panics
/// `length < string_size(number)`(右对齐区放不下)或 `chars` 容量不足 `start+length` 时 panic。
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

// ==================== 比较(对照 原实现 `Numeric.eq/ne/gt/ge/lt/le` → `Compares`)====================
//
// 原实现 `Compares` 泛型 `T extends Comparable<T>`,用 `compareTo`;Rust 用 `PartialEq`/`PartialOrd`。
// 下面泛型 `eq/ne/gt/ge/lt/le` 适配整数/`BigDecimal` 等正常可比类型,与 原实现 一致。
// **但 f64 的 `NaN` 在泛型 `PartialOrd` 下与 原实现 `Double.compareTo` 总序不同**(Rust 下 NaN 比较全 false);
// 完整迁移用专用族 [`eq_f64`]/[`ne_f64`]/[`gt_f64`]/[`ge_f64`]/[`lt_f64`]/[`le_f64`](复刻 `Double.compare`)。
//
// 原实现 `Compares.*` 还有 null→false 分支;Rust 无 null,自然不需要(对应 `Option` 由调用方处理)。

/// 业务作用: 相等。对照 原实现 `Numeric.eq`(`compareTo==0`;Rust 无 null,= `a == b`)。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn eq<T: PartialEq>(a: T, b: T) -> bool {
    a == b
}

/// 业务作用: 不等。对照 原实现 `Numeric.ne`(= `!eq`)。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn ne<T: PartialEq>(a: T, b: T) -> bool {
    a != b
}

/// 业务作用: 大于。对照 原实现 `Numeric.gt`。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn gt<T: PartialOrd>(a: T, b: T) -> bool {
    a > b
}

/// 业务作用: 大于等于。对照 原实现 `Numeric.ge`。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn ge<T: PartialOrd>(a: T, b: T) -> bool {
    a >= b
}

/// 业务作用: 小于。对照 原实现 `Numeric.lt`。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn lt<T: PartialOrd>(a: T, b: T) -> bool {
    a < b
}

/// 业务作用: 小于等于。对照 原实现 `Numeric.le`。
///
/// # 参数
/// - `a`: 左侧待比较值。
/// - `b`: 右侧待比较值。
pub fn le<T: PartialOrd>(a: T, b: T) -> bool {
    a <= b
}

// ── f64 专用比较:复刻 原实现 `Double.compareTo` 总序(NaN 等于 NaN 且大于一切、`-0.0 < 0.0`)──
// compat 前缀表示局部数值语义兼容,只约束本函数族的比较/舍入规则,不表达整套 legacy 协议模式。
// 泛型 `PartialOrd` 版对 f64 NaN 与 原实现 不一致(全 false),完整迁移需此族(用户:必须全量迁移)。

/// 业务作用: 完整复刻 `原实现.lang.Double.compare(a, b)`:先按 `<`/`>` 判,相等时落到**规范化位模式**比较
/// (所有 NaN 归一为同一 NaN,故 `NaN==NaN`、`NaN>` 一切;`-0.0` 位模式为负 → `-0.0 < 0.0`)。
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

    // 业务作用: 等价或含 NaN/±0:用 原实现 `doubleToLongBits` 语义(NaN 规范化,不区分 payload/符号)。
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

/// 业务作用: f64 相等,原实现 `Double.compareTo==0` 语义(`NaN==NaN` 为真)。对照 原实现 `Numeric.eq`。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn eq_f64(a: f64, b: f64) -> bool {
    compat_double_compare(a, b).is_eq()
}

/// 业务作用: f64 不等(`= !eq_f64`)。对照 原实现 `Numeric.ne`。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn ne_f64(a: f64, b: f64) -> bool {
    !eq_f64(a, b)
}

/// 业务作用: f64 大于,原实现 `Double.compareTo>0`(`NaN > 任意非NaN`)。对照 原实现 `Numeric.gt`。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn gt_f64(a: f64, b: f64) -> bool {
    compat_double_compare(a, b).is_gt()
}

/// 业务作用: f64 大于等于,原实现 `compareTo>=0`。对照 原实现 `Numeric.ge`。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn ge_f64(a: f64, b: f64) -> bool {
    compat_double_compare(a, b).is_ge()
}

/// 业务作用: f64 小于(原实现 `lt = !ge`)。对照 原实现 `Numeric.lt`。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn lt_f64(a: f64, b: f64) -> bool {
    !ge_f64(a, b)
}

/// 业务作用: f64 小于等于(原实现 `le = !gt`)。对照 原实现 `Numeric.le`。
///
/// # 参数
///
/// - `a`: 左侧 f64 值，按兼容总序比较。
/// - `b`: 右侧 f64 值，按兼容总序比较。
pub fn le_f64(a: f64, b: f64) -> bool {
    !gt_f64(a, b)
}
