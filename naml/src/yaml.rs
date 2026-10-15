//! 外部 YAML 文档的有界读写。
//!
//! 本入口面向 kubeconfig、平台采集配置等协议文档。字符串键按原文保留，不执行配置路径展开、
//! imports、环境覆盖、占位符或外部文件包含。应用配置装配使用 [`crate::strict::ConfigLoader`]。
//! 只支持 JSON 值可表达的类型：字符串键映射、数组、字符串、有限数字、布尔和 null。
//! 读取要求单个 UTF-8 文档，允许 BOM 与普通锚点，拒绝重复键、merge key、未知 tag 和循环别名。
//! 编码不自动生成块标量，多行字符串使用引号及转义，避免内容中的 Tab 被消费方解释为缩进。
//! 数字固定为 i64/u64 或有限 f64；下游 arbitrary_precision 不改变输出节点类型，也不扩大精度范围。
//! 转义后超过 1024 UTF-8 字节的键使用显式键形式，适配按字节限制隐式键的读取方。

use std::fmt::{self, Write};

use serde::Serialize;
use serde_json::{Number, Value};

use crate::strict::{ConfigError, ErrorKind, LoadLimits, LoadPolicy, Result};

/// 单份 YAML 文档的解析、树结构与输出预算。
#[derive(Clone, Debug)]
pub struct Limits {
    /// 输入正文的最大字节数。
    pub input_bytes: usize,
    /// 解析累计标量文本、编码前累计字符串及数值文本，以及编码后正文的最大字节数。
    pub output_bytes: usize,
    /// 根节点深度为零，子节点允许达到的最大深度。
    pub depth: usize,
    /// 文档节点上限，解析时锚点保存与别名复制同样计费。
    pub nodes: usize,
    /// 单个字符串值或键的最大字节数。
    pub string_bytes: usize,
}

impl Default for Limits {
    /// 业务作用：为外部文档提供与配置解析一致的有限默认范围。
    /// 参数说明：无。
    /// 返回：输入四 MiB、累计文本及输出三十二 MiB、深度 64、节点 100000、单文本一 MiB。
    fn default() -> Self {
        Self {
            input_bytes: 4 * 1024 * 1024,
            output_bytes: 32 * 1024 * 1024,
            depth: 64,
            nodes: 100_000,
            string_bytes: 1024 * 1024,
        }
    }
}

/// 业务作用：用默认预算读取一份外部 YAML 文档，不加载配置来源或执行表达式。
/// 参数说明：`bytes` 是完整 UTF-8 文档。
/// 返回：保留原始键的完整值；格式、编码、重复键及预算失败均返回脱敏错误。
pub fn from_slice(bytes: &[u8]) -> Result<Value> {
    from_slice_with_limits(bytes, &Limits::default())
}

/// 业务作用：按调用方预算解析外部 YAML，复用配置组件的事件解析和别名计费。
/// 参数说明：`bytes` 为完整正文；`limits` 固定本次解析的资源边界。
/// 返回：唯一文档的完整值；失败不返回部分树，也不回显正文。
pub fn from_slice_with_limits(bytes: &[u8], limits: &Limits) -> Result<Value> {
    let policy = LoadPolicy {
        limits: LoadLimits {
            source_bytes: limits.input_bytes,
            output_bytes: limits.output_bytes,
            depth: limits.depth,
            nodes: limits.nodes,
            string_bytes: limits.string_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    crate::strict::parse_yaml_value(bytes, &policy)
}

/// 业务作用：把已构造的协议值编码为 YAML，保持字符串与数值的类型区别。
/// 参数说明：`value` 是完整值，不解释其中的表达式或来源声明。
/// 返回：默认预算内的完整 YAML 文本；结构或输出超限时不返回部分正文。
pub fn to_string(value: &Value) -> Result<String> {
    to_string_with_limits(value, &Limits::default())
}

/// 业务作用：在结构检查及受限输出缓冲内编码 YAML，供调用方完成权限复验后发布。
/// 参数说明：`value` 是待编码树；`limits` 限定深度、节点、文本及最终输出字节。
/// 返回：多行文本采用引号转义的完整正文；超限、数值精度无法保留或无法编码时返回脱敏错误。
pub fn to_string_with_limits(value: &Value, limits: &Limits) -> Result<String> {
    let mut budget = EncodeBudget {
        limits,
        nodes: 0,
        text_bytes: 0,
    };
    budget.value(value, 0)?;
    let mut writer = BoundedWriter::new(limits.output_bytes);
    // 结构由已检查的 Value 决定，不能让下游 Serde feature 把标量改成未计费的私有映射。
    writer.value(value, 0)?;
    Ok(writer.text)
}

enum ScalarNumber {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

/// 业务作用：把数字固定为中性标量，拒绝超出整数范围或会丢失十进制精度的浮点转换。
/// 参数说明：`number` 为调用方的数字；`limit` 为剩余累计文本预算。
/// 返回：原生数值和表示字节数；表示超限或不能保留数值时返回脱敏错误。
fn scalar_number(number: &Number, limit: usize) -> Result<(ScalarNumber, usize)> {
    let mut source = BoundedWriter::new(limit);
    write!(&mut source, "{number}").map_err(|_| ConfigError::new(ErrorKind::Limit))?;
    let value = if source.text == "-0" {
        ScalarNumber::Float(-0.0)
    } else if let Some(value) = number.as_i64() {
        ScalarNumber::Signed(value)
    } else if let Some(value) = number.as_u64() {
        ScalarNumber::Unsigned(value)
    } else {
        // 整数越界不能退回浮点；浮点必须能以最短往返十进制表示保留原数值，不能静默舍入或下溢。
        let value = number
            .as_f64()
            .filter(|_| number.is_f64())
            .ok_or_else(|| ConfigError::new(ErrorKind::Format))?;
        let canonical = Number::from_f64(value)
            .ok_or_else(|| ConfigError::new(ErrorKind::Format))?
            .to_string();
        if !same_decimal(&source.text, &canonical) {
            return Err(ConfigError::new(ErrorKind::Format));
        }
        ScalarNumber::Float(value)
    };
    Ok((value, source.text.len()))
}

/// 业务作用：比较十进制数值而非拼写，允许等价的小数尾零和指数形式。
/// 参数说明：`left` 与 `right` 为已由 Number 提供的有限数字表示。
/// 返回：符号、有效数字和十进制阶数一致时为 true，无法规范化时拒绝。
fn same_decimal(left: &str, right: &str) -> bool {
    match (decimal_parts(left), decimal_parts(right)) {
        (Some((ls, ld, le)), Some((rs, rd, re))) => {
            ls == rs
                && le == re
                && ld
                    .bytes()
                    .filter(|byte| *byte != b'.')
                    .eq(rd.bytes().filter(|byte| *byte != b'.'))
        }
        _ => false,
    }
}

/// 业务作用：在不展开指数或复制有效数字的条件下提取十进制比较信息。
/// 参数说明：`text` 为数字的十进制表示。
/// 返回：符号、借用的有效数字区间和阶数；指数或长度无法表示时返回 None。
fn decimal_parts(text: &str) -> Option<(bool, &str, i64)> {
    let negative = text.starts_with('-');
    let text = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
        None => (text, 0),
    };
    let Some(first) = mantissa
        .bytes()
        .position(|byte| matches!(byte, b'1'..=b'9'))
    else {
        return Some((false, "", 0));
    };
    let last = mantissa
        .bytes()
        .rposition(|byte| matches!(byte, b'1'..=b'9'))?;
    let fraction = mantissa.split_once('.').map_or(0, |(_, part)| part.len());
    let trailing = mantissa[last + 1..]
        .bytes()
        .filter(|byte| *byte != b'.')
        .count();
    let exponent = exponent
        .checked_sub(i64::try_from(fraction).ok()?)?
        .checked_add(i64::try_from(trailing).ok()?)?;
    Some((negative, &mantissa[first..=last], exponent))
}

struct EncodeBudget<'a> {
    limits: &'a Limits,
    nodes: usize,
    text_bytes: usize,
}

impl EncodeBudget<'_> {
    /// 业务作用：编码前同时限制单值和累计文本规模。
    /// 参数说明：`text` 是映射键或字符串值。
    /// 返回：预算允许时成功，否则拒绝继续编码。
    fn text(&mut self, text: &str) -> Result<()> {
        self.text_bytes = self.text_bytes.saturating_add(text.len());
        if text.len() > self.limits.string_bytes || self.text_bytes > self.limits.output_bytes {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        Ok(())
    }

    /// 业务作用：编码前复验完整树的结构和文本限额，避免在编码器中递归处理超深或超大输入。
    /// 参数说明：`value` 是当前子树；`depth` 是从根开始的层级。
    /// 返回：完整子树满足预算时成功；超限时终止整份文档编码。
    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        self.nodes = self.nodes.saturating_add(1);
        if depth > self.limits.depth || self.nodes > self.limits.nodes {
            return Err(ConfigError::new(ErrorKind::Limit));
        }
        match value {
            Value::Null | Value::Bool(_) => {}
            Value::Number(number) => {
                let (_, bytes) = scalar_number(
                    number,
                    self.limits.output_bytes.saturating_sub(self.text_bytes),
                )?;
                self.text_bytes += bytes;
            }
            Value::String(value) => {
                self.text(value)?;
            }
            Value::Array(values) => {
                for value in values {
                    self.value(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for (key, value) in values {
                    self.text(key)?;
                    self.value(value, depth + 1)?;
                }
            }
        }
        Ok(())
    }
}

struct BoundedWriter {
    text: String,
    limit: usize,
    exceeded: bool,
}

impl BoundedWriter {
    /// 业务作用：为一份正文或单个键建立有限输出缓冲。
    /// 参数说明：`limit` 是本缓冲最多允许占用的正文长度。
    /// 返回：空缓冲，容量在每次追加前复验。
    fn new(limit: usize) -> Self {
        Self {
            text: String::new(),
            limit,
            exceeded: false,
        }
    }

    /// 业务作用：以统一限额追加 YAML 的结构符号和缩进。
    /// 参数说明：`text` 是固定结构文本或已编码的键。
    /// 返回：追加成功；超限时返回 Limit，不向调用方交付部分文档。
    fn push(&mut self, text: &str) -> Result<()> {
        self.write_str(text)
            .map_err(|_| ConfigError::new(ErrorKind::Limit))
    }

    /// 业务作用：按容器深度输出固定空格缩进，避免混入文本中的 Tab。
    /// 参数说明：`depth` 为已通过结构预算的深度。
    /// 返回：完整缩进已写入，或容量不足而拒绝。
    fn indent(&mut self, depth: usize) -> Result<()> {
        for _ in 0..depth {
            self.push("  ")?;
        }
        Ok(())
    }

    /// 业务作用：只把原生标量交给编码器，保持字符串转义且禁止自动块标量。
    /// 参数说明：`value` 为原生标量；`quote` 表示键需要强制引号保护。
    /// 返回：标量及终止换行写入；编码或实际字节超限时返回脱敏错误。
    fn scalar<T: Serialize>(&mut self, value: &T, quote: bool) -> Result<()> {
        // 多行内容必须作为转义文本输出，避免首个内容行的 Tab 被消费方解释为缩进。
        let options = serde_saphyr::ser_options! { prefer_block_scalars: false, quote_all: quote };
        serde_saphyr::to_fmt_writer_with_options(self, value, options).map_err(|_| {
            ConfigError::new(if self.exceeded {
                ErrorKind::Limit
            } else {
                ErrorKind::Format
            })
        })
    }

    /// 业务作用：将容器放到下一层，将标量保留在键或序列标记后，保持结构与预算一致。
    /// 参数说明：`value` 为当前子值；`depth` 为其父容器深度。
    /// 返回：子值已完整写入；失败终止整份文档。
    fn child(&mut self, value: &Value, depth: usize) -> Result<()> {
        if matches!(value, Value::Object(values) if !values.is_empty())
            || matches!(value, Value::Array(values) if !values.is_empty())
        {
            self.push("\n")?;
        } else {
            self.push(" ")?;
        }
        self.value(value, depth + 1)
    }

    /// 业务作用：按已检查的 JSON 树写出 YAML，使用显式长键兼容按字节限制隐式键的读取方。
    /// 参数说明：`value` 为当前节点；`depth` 是其结构深度。
    /// 返回：保持节点类型和字符串内容的完整子树；数值不可表示或输出超限时拒绝。
    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        match value {
            Value::Null => self.scalar(&Option::<u8>::None, false),
            Value::Bool(value) => self.scalar(value, false),
            Value::String(value) => self.scalar(value, false),
            Value::Number(value) => match scalar_number(value, self.limit)?.0 {
                ScalarNumber::Signed(value) => self.scalar(&value, false),
                ScalarNumber::Unsigned(value) => self.scalar(&value, false),
                ScalarNumber::Float(value) => self.scalar(&value, false),
            },
            Value::Array(values) if values.is_empty() => self.push("[]\n"),
            Value::Object(values) if values.is_empty() => self.push("{}\n"),
            Value::Array(values) => {
                for value in values {
                    self.indent(depth)?;
                    self.push("-")?;
                    self.child(value, depth)?;
                }
                Ok(())
            }
            Value::Object(values) => {
                for (key, value) in values {
                    let mut encoded = Self::new(self.limit.saturating_sub(self.text.len()));
                    encoded.scalar(key, true)?;
                    if encoded.text.pop() != Some('\n') {
                        return Err(ConfigError::new(ErrorKind::Format));
                    }
                    self.indent(depth)?;
                    // 以转义后的 UTF-8 字节计边界，兼容按字节而非字符判断隐式键长度的消费方。
                    if encoded.text.len() > 1024 {
                        self.push("? ")?;
                        self.push(&encoded.text)?;
                        self.push("\n")?;
                        self.indent(depth)?;
                    } else {
                        self.push(&encoded.text)?;
                    }
                    self.push(":")?;
                    self.child(value, depth)?;
                }
                Ok(())
            }
        }
    }
}

impl Write for BoundedWriter {
    /// 业务作用：限制转义与排版后的真实正文大小，避免字符串转义放大绕过容量限制。
    /// 参数说明：`text` 为编码器本次追加的文本片段。
    /// 返回：追加后仍在预算内时成功；否则保留内部缓冲但拒绝向调用方返回正文。
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if text.len() > self.limit.saturating_sub(self.text.len()) {
            self.exceeded = true;
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}
