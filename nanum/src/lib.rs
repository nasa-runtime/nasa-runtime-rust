//! # numeric —— 定点精确算术(对照 原实现 原工具包 `Numeric`)
//!
//! 撮合 / 金额场景的精确算术根。值表示为 **`i128` 定点数**:`真实值 = mantissa × 10^(−scale)`。
//!
//! ## 既定设计:**有限数 / 正常输入路径逐值对齐 原实现**
//! 逐值复刻 原实现 `Numeric`(含其 f64 中间运算)保证跨语言一致;**少数 原实现 边界 bug 采用安全偏离并在函数文档标注**:
//! - `to_fixed_f64`/`float::*` 的 NaN/±Inf → `Err`(非 原实现 `Math.round` 的 0/Long 边界)。
//! - `next_int_max(i32::MAX)` 正常工作(非 原实现 `max+1` 溢出抛异常)。
//! - `copy_to_char_array(i64::MIN)` 输出正确十进制串(非 原实现 `-Long.MIN_VALUE` 取负溢出 bug)。
//! - `decimal::random_step_bd` 超 i64 域按符号**饱和**(非 原实现 `BigDecimal.longValue()` 低 64 位回绕)。
//! - 泛型比较 `eq/gt/...` 的 f64 `NaN` 走 Rust `PartialOrd`;需 原实现 `Double.compareTo` 总序用 [`eq_f64`] 等专用族(**已提供,非偏离**)。
//!
//! 要点:
//! - **`multiply`/`divide`**(+全 8 mode):整数部分整数、**余数部分 f64**,复刻 原实现 `roundHalfUp`/`applyRounding`(L784/L813/L744)。
//! - **`to_fixed_f64`**:`Math.round(val×10^scale)`,**经 f64**,ties→+∞(L466)。
//!   **`to_fixed_str`**:对**十进制/科学计数法**输入按 `Math.round(parseDouble×10^scale)` 语义对齐;解析走 Rust
//!   `str::parse::<f64>()`,**非 原实现 `Double.parseDouble` 全集**(十六进制浮点字面量如 `"0x1.0p0"` 返 `Err`)。
//! - **`scale` 上限 = [`MAX_SCALE`] = 8**,对齐 原实现 `MAX_FIXED_SCALE`(超则 `Err`)。
//! - **`add`/`subtract`/`align*`/`to_plain_string`/`string_size`**:原实现 本就是纯整数,Rust 纯整数一致。
//!
//! 类型用 `i128`(非 原实现 `long`=i64):同算法下,**对 原实现 不溢出的输入逐值一致**;i128 仅避免复刻 原实现 long
//! 中间积的静默回绕(那是 bug,不该复刻),超 i128 域返 `Err`。
//!
//! ## 舍入(两套,原实现 本就不一致,分别保真)
//! - `multiply/divide`:`roundHalfUp` = **ties away-from-zero**(负 .5 远离零);全 8 mode 走 `applyRounding`。
//! - `to_fixed_*`:`Math.round` = **ties 朝 +∞**(`floor(x+0.5)`)。
//!
//! ## 错误模型
//! 溢出 / 除零 / 非法 scale / 解析失败 / `Unnecessary` 遇舍入 → 返 [`NumericError`](`Result`),**不 panic、不 wrapping**。
//!
//! ## API 速览(**全量迁移 原实现 `Numeric`**)
//! - 定点算术(i128,scale≤8):[`add`] [`subtract`] [`multiply`] [`divide`](+ `_rounding` 全模式 / `_default` 默认 scale=8)
//! - 精度对齐:[`align`] [`align_up`] [`align_down`] [`is_aligned`](撮合 tick 对齐)
//! - I/O:[`to_fixed_str`] [`to_fixed_f64`] / [`to_plain_string`] / 保尾零 [`to_plain_string_raw`] /
//!   定点&显示精度分离 [`to_plain_string_display`] [`to_plain_string_raw_display`] / [`to_big_decimal`]
//!   (均带 `_default` 默认 scale=8 与 `_f64` double 入参重载)
//! - **[`decimal`] 模块**:BigDecimal 段,**不受 fixed `MAX_SCALE=8` 限制**(对照 原实现 `Numeric` 的 BigDecimal API);
//!   所有运算(`add/subtract/multiply/multiply_scale/divide/sum/avg/align*`,均返 `Result`)在 scale 对齐时受
//!   `decimal::MAX_DECIMAL_EXPANSION` 防 OOM 边界保护、极端 scale 返 `Err(Scale)` 不 panic)+ [`scale_min`] [`random_step`]
//!   (仅构造、不展开、不设界)
//! - **[`float`] 模块**:`double` 便捷算术(对照 原实现 `Numeric` 的 `double` 重载,内部走定点防漂移)
//! - 整数 / 比较:[`string_size`] [`is_even`] [`is_odd`] [`gt`] [`ge`] [`lt`] [`le`]
//! - 随机:[`next_int`] [`next_int_max`]

mod arithmetic;
/// BigDecimal 高精度段(scale>8 退化路径)+ Decimal 后缀便捷段。对照 原实现 `Numeric` 的 BigDecimal API。
/// 用 `numeric::decimal::add(&BigDecimal, &BigDecimal)` 等(模块命名空间区分 i128 定点同名函数,对照 原实现 重载)。
pub mod decimal;
/// `double`(f64)便捷算术段。对照 原实现 `Numeric` 的 `double` 重载(模块命名空间区分 i128/BigDecimal 同名函数)。
/// 用 `numeric::float::add(a, b, scale)` 等。
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
// scale_min / random_step 走 BigDecimal(对照 原实现 返 BigDecimal,不受 fixed 8 位限制),不再是定点 String/i128。
// (构造本身不展开 10^n;后续 align*/divide 等运算受 decimal::MAX_DECIMAL_EXPANSION 边界保护。)
/// 任意精度十进制(= `bigdecimal::BigDecimal`,原实现 `BigDecimal` 等价物)。
pub use bigdecimal::BigDecimal;
pub use decimal::{random_step, scale_min};

/// 撮合默认精度(×10^8),对照 原实现 `Numeric.DEFAULT_FIXED_SCALE`。
pub const DEFAULT_SCALE: u32 = 8;

/// 定点 scale 上限 = **8**,**对齐 原实现 `Numeric.MAX_FIXED_SCALE`**(所有算法全部与 原实现 对齐)。
/// scale > 8 时 原实现 直接抛异常(无 原实现 参照、且 f64 的 `10^s` 超安全整数域),本 crate 同样返 `Err(Scale)`。
pub const MAX_SCALE: u32 = 8;

/// 舍入模式,语义对齐 `原实现.math.RoundingMode` / `BigDecimal`。
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
/// 对照 原实现 `Numeric.copyToCharArray(long,char[],int)`(其 JDK 位运算只是 `StringBuilder` 提速 hack,
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
/// `number = -number` 对 `Long.MIN_VALUE` 取负溢出的 bug**(原实现 那条会算错)。
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
    /// 转成兼容 IEEE-754 二进制浮点的规范化位模式。
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
