//! 容量大小配置类型。

use serde::Deserialize;
use std::fmt;

/// 字节大小,支持带单位字符串或纯字节整数反序列化。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// 业务作用：返回字节数。
    ///
    /// 参数说明: 无。
    ///
    /// 返回: 当前容量保存的原始字节数。
    pub fn bytes(self) -> u64 {
        self.0
    }

    /// 业务作用：解析 `"500MB"`、`"30GB"` 或纯字节数字。
    ///
    /// 单位大小写不敏感,按 1024 进制处理;不支持小数,避免隐式舍入。
    ///
    /// 参数说明:
    /// - `input`: 配置文件或环境变量里的容量文本；可以是纯数字字节数，也可以带 `KB`/`MB`/`GB` 单位。
    ///
    /// 返回: 文本和单位合法且换算未溢出时返回容量，否则返回 [`ByteSizeError::Invalid`] 或
    /// [`ByteSizeError::Overflow`]。
    pub fn parse(input: &str) -> Result<Self, ByteSizeError> {
        let s = input.trim();
        let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let (num, unit) = s.split_at(split);
        if num.is_empty() {
            return Err(ByteSizeError::Invalid(input.to_string()));
        }
        let num: u64 = num
            .parse()
            .map_err(|_| ByteSizeError::Invalid(input.to_string()))?;
        let mult: u64 = match unit.trim().to_ascii_uppercase().as_str() {
            "" | "B" => 1,
            "KB" | "KIB" => 1024,
            "MB" | "MIB" => 1024 * 1024,
            "GB" | "GIB" => 1024 * 1024 * 1024,
            _ => return Err(ByteSizeError::Invalid(input.to_string())),
        };
        num.checked_mul(mult)
            .map(ByteSize)
            .ok_or_else(|| ByteSizeError::Overflow(input.to_string()))
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    /// 业务作用：反序列化容量配置；支持数字和带单位字符串。
    ///
    /// 参数说明:
    /// - `deserializer`: serde 提供的容量字段反序列化器。
    ///
    /// 返回: 数字或容量文本合法时返回 [`ByteSize`]，否则返回反序列化错误。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// 容量反序列化访问器。
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = ByteSize;

            /// 业务作用：描述合法输入；用于反序列化错误提示。
            ///
            /// 参数说明:
            /// - `f`: 反序列化框架提供的格式化器，用于写入期待的输入说明。
            ///
            /// 返回: 期待输入说明成功写入时返回成功，否则透传格式化错误。
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a byte-size string like \"500MB\" or an integer byte count")
            }

            /// 业务作用：读取无符号字节数。
            ///
            /// 参数说明:
            /// - `v`: 配置中已解析出的非负字节数。
            ///
            /// 返回: 始终返回保存该字节数的容量值。
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }

            /// 业务作用：读取有符号字节数,负数会被拒绝。
            ///
            /// 参数说明:
            /// - `v`: 配置中已解析出的有符号整数；负数会被视为非法容量。
            ///
            /// 返回: 非负输入返回容量值，负数返回 serde 自定义错误。
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| E::custom(format!("negative byte size: {v}")))
            }

            /// 业务作用：读取带单位字符串。
            ///
            /// 参数说明:
            /// - `v`: 配置中已解析出的容量文本，会交给 [`ByteSize::parse`] 处理。
            ///
            /// 返回: 文本合法时返回容量值，否则将容量解析错误转换为 serde 错误。
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ByteSize::parse(v).map_err(|e| E::custom(e.to_string()))
            }
        }
        deserializer.deserialize_any(V)
    }
}

/// 字节大小解析错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteSizeError {
    /// 非法容量字符串。
    Invalid(String),
    /// 容量换算溢出。
    Overflow(String),
}

impl fmt::Display for ByteSizeError {
    /// 业务作用：格式化错误信息。
    ///
    /// 参数说明:
    /// - `f`: 标准格式化器，用于写入容量解析失败原因。
    ///
    /// 返回: 错误文本成功写入时返回成功，否则透传格式化错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ByteSizeError::Invalid(s) => write!(f, "invalid byte size: {s:?}"),
            ByteSizeError::Overflow(s) => write!(f, "byte size overflow: {s:?}"),
        }
    }
}

impl std::error::Error for ByteSizeError {}
