//! 字符串清洗工具。

/// 业务作用：判断字符串在去除首尾空白后是否为空。
///
/// # 参数
///
/// - `value`: 待检查的原始字符串，允许包含首尾空白。
pub fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}

/// 业务作用：判断字符串在去除首尾空白后是否仍有内容。
///
/// # 参数
///
/// - `value`: 待检查的原始字符串，允许包含首尾空白。
pub fn is_not_blank(value: &str) -> bool {
    !is_blank(value)
}

/// 业务作用：非空白字符串转为 `Some`；空白字符串转为 `None`。
///
/// # 参数
///
/// - `value`: 调用方传入的候选字符串；会按 `trim` 后是否为空决定保留或丢弃。
pub fn non_blank(value: impl Into<String>) -> Option<String> {
    let value = value.into();
    if is_blank(&value) {
        None
    } else {
        Some(value)
    }
}

/// 业务作用：可选字符串中非空白值保持为 `Some`；空白或缺失转为 `None`。
///
/// # 参数
///
/// - `value`: 可选候选字符串；`None`、空串和纯空白都会被统一清洗成 `None`。
pub fn option_non_blank(value: Option<String>) -> Option<String> {
    value.and_then(non_blank)
}

/// 业务作用：非空白字符串引用转为 `Some`；空白字符串转为 `None`。
///
/// # 参数
///
/// - `value`: 待检查的字符串引用；返回值复用同一段字符串切片，不会分配新字符串。
pub fn non_blank_ref(value: &str) -> Option<&str> {
    if is_blank(value) {
        None
    } else {
        Some(value)
    }
}

/// 业务作用：去除首尾空白并返回新的字符串。
///
/// # 参数
///
/// - `value`: 待裁剪的字符串来源，常用于配置项、请求参数或环境变量值的归一化。
pub fn trim_to_string(value: impl AsRef<str>) -> String {
    value.as_ref().trim().to_string()
}

/// 业务作用：空字符串转为 `None`；非空字符串保持原样。
///
/// # 参数
///
/// - `value`: 待检查的字符串；这里只判断长度为零，不会裁剪首尾空白。
pub fn empty_to_none(value: impl Into<String>) -> Option<String> {
    let value = value.into();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// 业务作用：清洗可选字符串:去除首尾空白,清洗后为空则返回 `None`。
///
/// # 参数
///
/// - `value`: 可选字符串输入；命中 `Some` 时会先 `trim`，再按清洗后的内容决定是否保留。
pub fn option_trimmed(value: Option<String>) -> Option<String> {
    value.and_then(|s| {
        let s = s.trim().to_string();
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    })
}
