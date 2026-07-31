//! 容量大小配置类型。

use serde::Deserialize;
use std::fmt;

/// 字节大小,支持带单位字符串或纯字节整数反序列化。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// 返回字节数。
    pub fn bytes(self) -> u64 {
        self.0
    }

    /// 解析 `"500MB"`、`"30GB"` 或纯字节数字。
    ///
    /// 单位大小写不敏感,按 1024 进制处理;不支持小数,避免隐式舍入。
    ///
    /// # 参数
    ///
    /// - `input`: 配置文件或环境变量里的容量文本；可以是纯数字字节数，也可以带 `KB`/`MB`/`GB` 单位。
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
    /// 反序列化容量配置；支持数字和带单位字符串。
    ///
    /// # 参数
    /// - `deserializer`: serde 提供的容量字段反序列化器。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// 容量反序列化访问器。
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = ByteSize;

            /// 描述合法输入；用于反序列化错误提示。
            ///
            /// # 参数
            ///
            /// - `f`: 反序列化框架提供的格式化器，用于写入期待的输入说明。
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a byte-size string like \"500MB\" or an integer byte count")
            }

            /// 读取无符号字节数。
            ///
            /// # 参数
            ///
            /// - `v`: 配置中已解析出的非负字节数。
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }

            /// 读取有符号字节数,负数会被拒绝。
            ///
            /// # 参数
            ///
            /// - `v`: 配置中已解析出的有符号整数；负数会被视为非法容量。
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| E::custom(format!("negative byte size: {v}")))
            }

            /// 读取带单位字符串。
            ///
            /// # 参数
            ///
            /// - `v`: 配置中已解析出的容量文本，会交给 [`ByteSize::parse`] 处理。
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
    /// 格式化错误信息。
    ///
    /// # 参数
    ///
    /// - `f`: 标准格式化器，用于写入容量解析失败原因。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ByteSizeError::Invalid(s) => write!(f, "invalid byte size: {s:?}"),
            ByteSizeError::Overflow(s) => write!(f, "byte size overflow: {s:?}"),
        }
    }
}

impl std::error::Error for ByteSizeError {}
