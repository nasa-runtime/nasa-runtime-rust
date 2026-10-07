use std::fmt;

use serde::{
    de::{
        self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
        Visitor,
    },
    Deserializer,
};
use serde_json::Value;

use super::{ConfigError, ConfigPath, ErrorKind, Result};

/// 业务作用：按目标字段类型转换环境/表达式文本，密码 String 不经过数字推断。
/// 参数说明：`tree` 是已完成来源合并和表达式求值的树。
/// 返回：目标配置；失败仅携带字段路径，不传播含原值的 Serde 错误。
pub fn bind<T: DeserializeOwned>(tree: Value) -> Result<T> {
    serde_path_to_error::deserialize(Binding(tree)).map_err(|error| {
        let mut result = ConfigError::new(ErrorKind::Binding);
        if let Ok(path) = ConfigPath::parse(&error.path().to_string()) {
            result.path = Some(path);
        }
        result
    })
}

#[derive(Debug)]
struct BindingError;
impl fmt::Display for BindingError {
    /// 业务作用：隐藏目标类型校验器可能产生的原值文本。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：固定错误摘要。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configuration binding rejected")
    }
}
impl std::error::Error for BindingError {}
impl de::Error for BindingError {
    /// 业务作用：接收后端错误时立即丢弃潜在敏感内容。
    /// 参数说明：`_message` 为下游校验器的错误内容。
    /// 返回：无原值的错误载体。
    fn custom<T: fmt::Display>(_message: T) -> Self {
        Self
    }
}

struct Binding(Value);

macro_rules! number {
    ($method:ident, $ty:ty, $visit:ident) => {
        /// 业务作用：仅在目标要求数值时进行受检文本转换。
        /// 参数说明：`visitor` 提供目标字段的数值范围约束。
        /// 返回：数值格式和范围合法时成功。
        fn $method<V: Visitor<'de>>(
            self,
            visitor: V,
        ) -> std::result::Result<V::Value, Self::Error> {
            match self.0 {
                Value::String(value) => {
                    visitor.$visit(value.parse::<$ty>().map_err(|_| BindingError)?)
                }
                value => value.$method(visitor).map_err(|_| BindingError),
            }
        }
    };
}

impl<'de> Deserializer<'de> for Binding {
    type Error = BindingError;
    /// 业务作用：动态值保留原始类型，容器对子字段复用相同绑定规则。
    /// 参数说明：`visitor` 为目标类型访问者。
    /// 返回：完成递归绑定的值。
    fn deserialize_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self.0 {
            Value::Null => visitor.visit_unit(),
            Value::Bool(value) => visitor.visit_bool(value),
            Value::String(value) => visitor.visit_string(value),
            Value::Number(value) => Value::Number(value)
                .deserialize_any(visitor)
                .map_err(|_| BindingError),
            Value::Array(value) => visitor.visit_seq(Sequence(value.into_iter())),
            Value::Object(value) => visitor.visit_map(Mapping {
                values: value.into_iter(),
                pending: None,
            }),
        }
    }
    /// 业务作用：只有目标 bool 接受严格 true/false 文本。
    /// 参数说明：`visitor` 为布尔访问者。
    /// 返回：受检布尔值。
    fn deserialize_bool<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self.0 {
            Value::String(value) => visitor.visit_bool(value.parse().map_err(|_| BindingError)?),
            value => value.deserialize_bool(visitor).map_err(|_| BindingError),
        }
    }
    number!(deserialize_i8, i8, visit_i8);
    number!(deserialize_i16, i16, visit_i16);
    number!(deserialize_i32, i32, visit_i32);
    number!(deserialize_i64, i64, visit_i64);
    number!(deserialize_i128, i128, visit_i128);
    number!(deserialize_u8, u8, visit_u8);
    number!(deserialize_u16, u16, visit_u16);
    number!(deserialize_u32, u32, visit_u32);
    number!(deserialize_u64, u64, visit_u64);
    number!(deserialize_u128, u128, visit_u128);
    /// 业务作用：目标浮点拒绝 NaN 和无穷大。
    /// 参数说明：`visitor` 为浮点访问者。
    /// 返回：有限 f64 值。
    fn deserialize_f64<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self.0 {
            Value::String(value) => {
                let parsed = value.parse::<f64>().map_err(|_| BindingError)?;
                if !parsed.is_finite() {
                    return Err(BindingError);
                }
                visitor.visit_f64(parsed)
            }
            value => value.deserialize_f64(visitor).map_err(|_| BindingError),
        }
    }
    /// 业务作用：目标单精度浮点继续执行有限值检查。
    /// 参数说明：`visitor` 为浮点访问者。
    /// 返回：有限 f32 值。
    fn deserialize_f32<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self.0 {
            Value::String(value) => {
                let parsed = value.parse::<f32>().map_err(|_| BindingError)?;
                if !parsed.is_finite() {
                    return Err(BindingError);
                }
                visitor.visit_f32(parsed)
            }
            Value::Number(value) => {
                let parsed = value.as_f64().ok_or(BindingError)? as f32;
                if !parsed.is_finite() {
                    return Err(BindingError);
                }
                visitor.visit_f32(parsed)
            }
            _ => Err(BindingError),
        }
    }
    /// 业务作用：区分显式 null 与存在的空字符串。
    /// 参数说明：`visitor` 为 Option 访问者。
    /// 返回：null 对应 None，其余值继续受检绑定。
    fn deserialize_option<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        if self.0.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }
    /// 业务作用：自定义透明类型复用相同字段转换语义。
    /// 参数说明：`_name` 是类型名；`visitor` 为类型访问者。
    /// 返回：由目标类型决定的绑定结果。
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }
    /// 业务作用：保持 Serde 枚举标签和枚举内部字段的受检绑定。
    /// 参数说明：`_name/_variants` 为目标枚举信息；`visitor` 为访问者。
    /// 返回：合法变体或安全绑定错误。
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        let (name, value) = match self.0 {
            Value::String(name) => (name, Value::Null),
            Value::Object(map) if map.len() == 1 => map.into_iter().next().ok_or(BindingError)?,
            _ => return Err(BindingError),
        };
        visitor.visit_enum(Variant { name, value })
    }
    serde::forward_to_deserialize_any! { char str string bytes byte_buf unit unit_struct seq tuple tuple_struct map struct identifier ignored_any }
}

struct Sequence(std::vec::IntoIter<Value>);
impl<'de> SeqAccess<'de> for Sequence {
    type Error = BindingError;
    /// 业务作用：逐元素绑定数组，保持索引错误定位。
    /// 参数说明：`seed` 是元素目标类型。
    /// 返回：下一个受检元素或数组结束。
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> std::result::Result<Option<T::Value>, Self::Error> {
        self.0
            .next()
            .map(|value| seed.deserialize(Binding(value)))
            .transpose()
    }
}

struct Mapping {
    values: serde_json::map::IntoIter,
    pending: Option<Value>,
}
impl<'de> MapAccess<'de> for Mapping {
    type Error = BindingError;
    /// 业务作用：保留字段名称供 Serde 重命名和未知字段策略处理。
    /// 参数说明：`seed` 是字段名目标。
    /// 返回：下一字段名或映射结束。
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> std::result::Result<Option<K::Value>, Self::Error> {
        match self.values.next() {
            Some((key, value)) => {
                self.pending = Some(value);
                seed.deserialize(de::value::StringDeserializer::<BindingError>::new(key))
                    .map(Some)
            }
            None => Ok(None),
        }
    }
    /// 业务作用：对子字段复用目标类型感知绑定。
    /// 参数说明：`seed` 是字段值目标。
    /// 返回：当前字段的绑定结果。
    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        seed.deserialize(Binding(self.pending.take().ok_or(BindingError)?))
    }
}

struct Variant {
    name: String,
    value: Value,
}
impl<'de> EnumAccess<'de> for Variant {
    type Error = BindingError;
    type Variant = Self;
    /// 业务作用：选择枚举变体并保留待绑定载荷。
    /// 参数说明：`seed` 是变体名称目标。
    /// 返回：枚举名称与载荷访问者。
    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> std::result::Result<(V::Value, Self), Self::Error> {
        let name = seed.deserialize(de::value::StringDeserializer::<BindingError>::new(
            self.name.clone(),
        ))?;
        Ok((name, self))
    }
}
impl<'de> VariantAccess<'de> for Variant {
    type Error = BindingError;
    /// 业务作用：无载荷变体只接受 null。
    /// 参数说明：无。
    /// 返回：无额外载荷时成功。
    fn unit_variant(self) -> std::result::Result<(), Self::Error> {
        if self.value.is_null() {
            Ok(())
        } else {
            Err(BindingError)
        }
    }
    /// 业务作用：单字段枚举继续按目标类型绑定。
    /// 参数说明：`seed` 为载荷类型。
    /// 返回：合法变体载荷。
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> std::result::Result<T::Value, Self::Error> {
        seed.deserialize(Binding(self.value))
    }
    /// 业务作用：元组变体逐元素绑定。
    /// 参数说明：`len` 为元素数；`visitor` 为访问者。
    /// 返回：受检元组载荷。
    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        Binding(self.value).deserialize_tuple(len, visitor)
    }
    /// 业务作用：结构体变体按字段绑定并支持路径诊断。
    /// 参数说明：`fields` 为字段集合；`visitor` 为访问者。
    /// 返回：受检结构载荷。
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        Binding(self.value).deserialize_struct("variant", fields, visitor)
    }
}
