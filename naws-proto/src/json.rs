// JSON_BYTES（Mode=0）由 serde 属性固定字段线格式：字段名使用 camelCase，
// null、空字符串与空集合通过字段属性省略，byte[] 输出 -128..127 的有符号数字数组。
// 输出紧凑 JSON，非 ASCII 文本保持 UTF-8。所有参与方必须使用同一字段省略与字节表示规则。
// 省略空值会丢失 Some(空) 与 None 的区别，解码不能恢复被省略字段的原始状态。

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{CodecError, Result};

/// 业务作用：序列化任意 schema 对象为 JSON 字节(派生宏的 JsonBytes 分支调用)。
///
/// # 参数
/// - `v`: 待编码的协议结构体或派生 schema 实例。
pub fn to_vec<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| CodecError::Json(e.to_string()))
}

/// 业务作用：从 JSON 字节反序列化(派生宏的 JsonBytes 分支调用)。
///
/// # 参数
/// - `data`: 需要解码为目标 schema 的 JSON_BYTES 载荷。
pub fn from_slice<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
    serde_json::from_slice(data).map_err(|e| CodecError::Json(e.to_string()))
}

/* ============================== serde 字段属性辅助 ============================== */

/// 业务作用：NON_EMPTY:Option<String> 为 None 或空串时省略。
///
/// # 参数
/// - `v`: 待判断的可空字符串字段。
pub fn opt_str_empty(v: &Option<String>) -> bool {
    v.as_ref().is_none_or(|s| s.is_empty())
}

/// 业务作用：NON_EMPTY:字符串数组为 None 或空数组时省略。
///
/// # 参数
/// - `v`: 待判断的可空字符串数组字段。
pub fn opt_strvec_empty(v: &Option<Vec<Option<String>>>) -> bool {
    v.as_ref().is_none_or(|a| a.is_empty())
}

/// 业务作用：NON_EMPTY:字节数组为 None 或空时省略。
///
/// # 参数
/// - `v`: 待判断的可空 byte[] 字段。
pub fn opt_bytes_empty(v: &Option<Vec<u8>>) -> bool {
    v.as_ref().is_none_or(|a| a.is_empty())
}

/// 业务作用：byte[] 出站:每字节按 **有符号** i8 写。skip 已挡掉 None/空。
///
/// # 参数
/// - `v`: 待序列化的可空字节数组字段。
/// - `s`: serde 提供的字段序列化器。
pub fn ser_opt_bytes_signed<S: Serializer>(
    v: &Option<Vec<u8>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    match v {
        Some(b) => s.collect_seq(b.iter().map(|&x| x as i8)),
        None => s.serialize_none(),
    }
}

/// 业务作用：byte[] 入站:仅接受协议允许的字节范围 `-128..=255`(有符号 i8 或无符号 u8 两种写法),
/// 越界(如 256 / -129)立即报错,**不静默截断**成另一份合法数据。
///
/// # 参数
/// - `d`: serde 提供的字段反序列化器。
pub fn de_opt_bytes_signed<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<Vec<u8>>, D::Error> {
    use serde::de::Error;
    let opt: Option<Vec<i64>> = Option::deserialize(d)?;
    match opt {
        None => Ok(None),
        Some(v) => {
            let mut out = Vec::with_capacity(v.len().min(4096));
            for n in v {
                if !(-128..=255).contains(&n) {
                    return Err(D::Error::custom(format!("byte value out of range: {n}")));
                }
                out.push(n as u8); // -128..=-1 按补码回到 128..=255
            }
            Ok(Some(out))
        }
    }
}
