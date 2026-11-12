//! 原生属性可使用映射或显式 name/value 列表，列表适用于禁止点号字段名的严格配置来源。

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use std::{collections::BTreeMap, fmt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    name: String,
    value: String,
}
struct Properties;

impl<'de> Visitor<'de> for Properties {
    type Value = BTreeMap<String, String>;

    /// 业务作用：为原生属性声明提供不包含配置值的类型错误说明。
    ///
    /// 参数说明：
    /// - `formatter`：错误文本输出目标。
    ///
    /// 返回：只输出允许的容器形式，不暴露连接凭据。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string map or a list of name/value entries")
    }

    /// 业务作用：保留原有映射输入，同时拒绝同一原生键重复覆盖。
    ///
    /// 参数说明：
    /// - `map`：反序列化器提供的完整映射访问器。
    ///
    /// 返回：精确键值目录；重复键或非字符串字段拒绝。
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut result = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            if result.insert(key, value).is_some() {
                return Err(de::Error::custom("duplicate native property"));
            }
        }
        Ok(result)
    }

    /// 业务作用：把显式列表转换为相同原生属性目录，点号保留在 name 的值内。
    ///
    /// 参数说明：
    /// - `sequence`：严格来源允许的属性条目列表。
    ///
    /// 返回：与映射形式相同的精确目录；同名重复、缺失字段及额外字段拒绝。
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Self::Value, S::Error> {
        let mut result = BTreeMap::new();
        while let Some(entry) = sequence.next_element::<Entry>()? {
            if result.insert(entry.name, entry.value).is_some() {
                return Err(de::Error::custom("duplicate native property"));
            }
        }
        Ok(result)
    }
}

/// 业务作用：在不改变公开配置字段类型的前提下支持两种无歧义原生属性声明。
///
/// 参数说明：
/// - `deserializer`：当前属性节点的格式读取器。
///
/// 返回：同一 BTreeMap；不会重命名、展开或合并不同原生键。
pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    deserializer.deserialize_any(Properties)
}
