use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

use super::{ConfigError, ErrorKind, Result};

/// 字段名与数组下标的身份始终分开。
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum PathSegment {
    /// 对象中的原样字段名，不与数组下标混用。
    Key(String),
    /// 从零开始的数组位置。
    Index(usize),
}

/// 点号仅用于外部简写，内部比较使用有类型的路径段。
#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ConfigPath(
    /// 从根到目标节点的有序路径段；空列表表示根节点。
    pub Vec<PathSegment>,
);

impl ConfigPath {
    /// 业务作用：解析明确的字段与数组索引引用，拒绝含糊路径。
    /// 参数说明：`text` 采用 `a.b[0].c` 形式。
    /// 返回：合法路径；空段、负下标、溢出或不支持的转义被拒绝。
    pub fn parse(text: &str) -> Result<Self> {
        if text.is_empty() || text.len() > 4096 {
            return Err(ConfigError::new(ErrorKind::Expression));
        }
        let mut segments = Vec::new();
        let bytes = text.as_bytes();
        let mut position = 0;
        while position < bytes.len() {
            let start = position;
            while position < bytes.len() && !matches!(bytes[position], b'.' | b'[' | b']') {
                if bytes[position].is_ascii_control() || bytes[position].is_ascii_whitespace() {
                    return Err(ConfigError::new(ErrorKind::Expression));
                }
                position += 1;
            }
            if start == position {
                return Err(ConfigError::new(ErrorKind::Expression));
            }
            segments.push(PathSegment::Key(text[start..position].into()));
            while position < bytes.len() && bytes[position] == b'[' {
                position += 1;
                let start = position;
                while position < bytes.len() && bytes[position].is_ascii_digit() {
                    position += 1;
                }
                if start == position || bytes.get(position) != Some(&b']') {
                    return Err(ConfigError::new(ErrorKind::Expression));
                }
                let index = text[start..position]
                    .parse()
                    .map_err(|_| ConfigError::new(ErrorKind::Expression))?;
                segments.push(PathSegment::Index(index));
                position += 1;
            }
            if position < bytes.len() {
                if bytes[position] != b'.' || position + 1 == bytes.len() {
                    return Err(ConfigError::new(ErrorKind::Expression));
                }
                position += 1;
            }
            if segments.len() > 128 {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
        }
        Ok(Self(segments))
    }

    /// 业务作用：追加确切字段名，保持字面键身份。
    /// 参数说明：`key` 是原始字段名。
    /// 返回：独立的子字段路径。
    pub fn key(&self, key: &str) -> Self {
        let mut path = self.clone();
        path.0.push(PathSegment::Key(key.into()));
        path
    }

    /// 业务作用：追加只读数组位置。
    /// 参数说明：`index` 是非负数组下标。
    /// 返回：独立的数组元素路径。
    pub fn index(&self, index: usize) -> Self {
        let mut path = self.clone();
        path.0.push(PathSegment::Index(index));
        path
    }

    /// 业务作用：按结构身份查找配置节点，不创建缺失路径。
    /// 参数说明：`tree` 是完整候选树。
    /// 返回：命中节点或不存在。
    pub fn get<'a>(&self, tree: &'a Value) -> Option<&'a Value> {
        self.0
            .iter()
            .try_fold(tree, |value, segment| match segment {
                PathSegment::Key(key) => value.as_object()?.get(key),
                PathSegment::Index(index) => value.as_array()?.get(*index),
            })
    }

    /// 业务作用：识别父级策略是否覆盖当前字段。
    /// 参数说明：`other` 为父级候选路径。
    /// 返回：当前路径位于该子树内时为真。
    pub fn starts_with(&self, other: &Self) -> bool {
        self.0.starts_with(&other.0)
    }
}

impl fmt::Display for ConfigPath {
    /// 业务作用：输出有长度限制且不包含控制字符的字段位置。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：安全展示结果，完整身份仍保留在结构中。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (number, part) in self.0.iter().take(16).enumerate() {
            match part {
                PathSegment::Key(key) => {
                    if number > 0 {
                        write!(f, ".")?;
                    }
                    for ch in key.chars().take(48) {
                        write!(
                            f,
                            "{}",
                            if ch.is_control() {
                                '\u{fffd}'
                            } else {
                                ch
                            }
                        )?;
                    }
                }
                PathSegment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

impl fmt::Debug for ConfigPath {
    /// 业务作用：调试表示沿用受限字段展示。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：不输出无界路径内容。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
