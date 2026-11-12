use super::{binary::signed, consume, invalid, FieldPlan, SchemaPlan, Type, Value};
use crate::{CodecError, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde_json::Value as Json;
use std::{collections::BTreeMap, fmt};

impl SchemaPlan {
    /// 业务作用：严格解析 JSON 对象后按 schema 还原数值宽度与引用类型，i64 不经过浮点。
    ///
    /// 参数说明：
    /// - `bytes`：完整 JSON_BYTES 业务区。
    /// - `budget`：整个对象的动态值额度。
    ///
    /// 返回：有界对象；重复键、超深结构或数值类型不匹配拒绝。
    pub(super) fn decode_json(&self, bytes: &[u8], budget: &mut usize) -> Result<Value> {
        let mut parse_budget = self.limits.max_values;
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        let value = Strict {
            budget: &mut parse_budget,
            depth: 0,
            maximum_depth: self.limits.max_depth * 2 + 2,
        }
        .deserialize(&mut decoder)
        .map_err(|_| invalid("invalid or excessive JSON structure"))?;
        decoder.end().map_err(|_| invalid("JSON trailing data"))?;
        self.bind_json_object(&value, budget)
    }

    /// 业务作用：按 Java JSON 语义编码 schema 字段，省略空引用并保留整数词法值。
    ///
    /// 参数说明：
    /// - `value`：动态对象。
    /// - `budget`：展开对象共享额度。
    ///
    /// 返回：完整 UTF-8 JSON；直接字节字段使用有符号数字列表，集合里的字节数组按 Java 布局使用 Base64。
    pub(super) fn encode_json(&self, value: &Value, budget: &mut usize) -> Result<Vec<u8>> {
        let mut writer = Bounded {
            bytes: Vec::new(),
            maximum: self.limits.max_message_bytes,
        };
        serde_json::to_writer(&mut writer, &self.to_json(value, budget)?)
            .map_err(|_| CodecError::LengthExceeds)?;
        Ok(writer.bytes)
    }

    /// 业务作用：把受检 JSON 对象绑定到已编译字段，未知业务字段按 Java 的对象兼容规则跳过。
    ///
    /// 参数说明：
    /// - `value`：受限 JSON 对象。
    /// - `budget`：整个对象共享额度。
    ///
    /// 返回：完整 schema 值；非对象或缺失非空引用拒绝。
    fn bind_json_object(&self, value: &Json, budget: &mut usize) -> Result<Value> {
        consume(budget)?;
        let Json::Object(object) = value else {
            return Err(invalid("JSON object required"));
        };
        let mut values = BTreeMap::new();
        for field in &self.fields {
            let value = match object.get(&field.descriptor.name) {
                None => field
                    .descriptor
                    .typ
                    .default_value(field.descriptor.nullable),
                Some(Json::Null) => Value::Null,
                Some(value) => self.read_json_value(field, &field.descriptor.typ, value, budget)?,
            };
            if matches!(value, Value::Null) && !field.descriptor.nullable {
                return Err(invalid("nonnullable field is absent or null"));
            }
            values.insert(field.descriptor.name.clone(), value);
        }
        Ok(Value::Object(values))
    }

    /// 业务作用：将动态对象转换为可序列化 JSON 字段，保留数值类型及 Java 引用空值省略规则。
    ///
    /// 参数说明：
    /// - `value`：schema 对象。
    /// - `budget`：展开值总额度。
    ///
    /// 返回：字段受限的 JSON 对象，非法值不产生部分输出。
    fn to_json(&self, value: &Value, budget: &mut usize) -> Result<Json> {
        consume(budget)?;
        let Value::Object(object) = value else {
            return Err(invalid("object required"));
        };
        if object
            .keys()
            .any(|key| !self.fields.iter().any(|f| &f.descriptor.name == key))
        {
            return Err(invalid("unknown object field"));
        }
        let mut out = serde_json::Map::new();
        for field in &self.fields {
            let default = field
                .descriptor
                .typ
                .default_value(field.descriptor.nullable);
            let value = object.get(&field.descriptor.name).unwrap_or(&default);
            if matches!(value, Value::Null) {
                if !field.descriptor.nullable {
                    return Err(invalid("nonnullable field is null"));
                }
                continue;
            }
            let json = self.write_json_value(field, &field.descriptor.typ, value, budget)?;
            let empty = match &json {
                Json::String(v) => v.is_empty(),
                Json::Array(v) => v.is_empty(),
                Json::Object(v) => v.is_empty(),
                _ => false,
            };
            if empty && !field.descriptor.nullable {
                return Err(invalid("empty nonnullable reference is omitted by JSON"));
            }
            if !empty {
                out.insert(field.descriptor.name.clone(), json);
            }
        }
        Ok(Json::Object(out))
    }

    /// 业务作用：按照精确字段类型读取 JSON 值，不接受整数的浮点或字符串替代。
    ///
    /// 参数说明：
    /// - `field`：资源限制和嵌套计划。
    /// - `typ`：字段或元素类型。
    /// - `value`：当前 JSON 值。
    /// - `budget`：剩余展开值额度。
    ///
    /// 返回：类型化动态值，超限与不支持的 JSON 字符语义拒绝。
    fn read_json_value(
        &self,
        field: &FieldPlan,
        typ: &Type,
        value: &Json,
        budget: &mut usize,
    ) -> Result<Value> {
        consume(budget)?;
        match typ {
            Type::Bool {} => value
                .as_bool()
                .map(Value::Bool)
                .ok_or_else(|| invalid("boolean required")),
            Type::I8 {} | Type::I16 {} | Type::I32 {} | Type::I64 {} => {
                let n = value.as_i64().ok_or_else(|| invalid("integer required"))?;
                signed(typ, n)?;
                Ok(Value::Signed(n))
            }
            Type::Char {} => {
                let text = value
                    .as_str()
                    .ok_or_else(|| invalid("Java char string required"))?;
                let mut units = text.encode_utf16();
                let unit = units.next().ok_or_else(|| invalid("empty Java char"))?;
                if units.next().is_some() {
                    return Err(invalid("Java char requires one UTF-16 unit"));
                }
                Ok(Value::Char(unit))
            }
            Type::F32 {} => {
                let n = float(value)?;
                let f = n as f32;
                if n.is_finite() && !f.is_finite() {
                    return Err(invalid("float32 overflow"));
                }
                Ok(Value::F32(f.to_bits()))
            }
            Type::F64 {} => Ok(Value::F64(float(value)?.to_bits())),
            Type::String {} => {
                let value = value.as_str().ok_or_else(|| invalid("string required"))?;
                if value.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                Ok(Value::String(value.into()))
            }
            Type::Bytes {} => {
                let array = value
                    .as_array()
                    .ok_or_else(|| invalid("signed byte array required"))?;
                if array.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                let mut bytes = Vec::with_capacity(array.len());
                for value in array {
                    let n = value
                        .as_i64()
                        .and_then(|v| i8::try_from(v).ok())
                        .ok_or(CodecError::VarintOverflow)?;
                    bytes.push(n as u8);
                }
                Ok(Value::Bytes(bytes.into()))
            }
            Type::Enum {
                variants,
            } => {
                let name = value
                    .as_str()
                    .ok_or_else(|| invalid("enum name required"))?;
                let index = variants
                    .iter()
                    .position(|v| v == name)
                    .ok_or_else(|| invalid("unknown enum name"))?;
                Ok(Value::Enum(index as u32))
            }
            Type::Message {
                ..
            } => field
                .nested
                .as_ref()
                .ok_or_else(|| invalid("missing nested plan"))?
                .bind_json_object(value, budget),
            Type::List {
                element,
                nullable_elements,
            } => {
                let values = value.as_array().ok_or_else(|| invalid("array required"))?;
                if values.len() > field.descriptor.max_elements || values.len() > *budget {
                    return Err(CodecError::LengthExceeds);
                }
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(if value.is_null() {
                        consume(budget)?;
                        if !nullable_elements {
                            return Err(invalid("null collection element"));
                        }
                        Value::Null
                    } else if matches!(**element, Type::Bytes {}) {
                        // Java 的集合元素交由 Jackson 编码 byte[]，与顶层 byte[] 的有符号数组布局不同。
                        consume(budget)?;
                        let text = value
                            .as_str()
                            .ok_or_else(|| invalid("byte collection element requires base64"))?;
                        if text.len() > field.descriptor.max_bytes.div_ceil(3) * 4 {
                            return Err(CodecError::LengthExceeds);
                        }
                        let bytes = STANDARD
                            .decode(text)
                            .map_err(|_| invalid("invalid byte collection base64"))?;
                        if bytes.len() > field.descriptor.max_bytes {
                            return Err(CodecError::LengthExceeds);
                        }
                        Value::Bytes(bytes.into())
                    } else {
                        self.read_json_value(field, element, value, budget)?
                    });
                }
                Ok(Value::List(out))
            }
        }
    }

    /// 业务作用：输出 Java 兼容的 JSON 标量与引用，特殊浮点使用明确字符串表示。
    ///
    /// 参数说明：
    /// - `field`：当前字段计划。
    /// - `typ`：当前字段或元素类型。
    /// - `value`：待编码动态值。
    /// - `budget`：展开值总额度。
    ///
    /// 返回：类型正确的 JSON 值；孤立 surrogate 不生成非法 UTF-8。
    fn write_json_value(
        &self,
        field: &FieldPlan,
        typ: &Type,
        value: &Value,
        budget: &mut usize,
    ) -> Result<Json> {
        consume(budget)?;
        match (typ, value) {
            (Type::Bool {}, Value::Bool(v)) => Ok((*v).into()),
            (Type::I8 {} | Type::I16 {} | Type::I32 {} | Type::I64 {}, Value::Signed(v)) => {
                signed(typ, *v)?;
                Ok((*v).into())
            }
            (Type::Char {}, Value::Char(v)) => Ok(Json::String(
                char::from_u32((*v).into())
                    .ok_or_else(|| invalid("isolated UTF-16 surrogate in JSON"))?
                    .to_string(),
            )),
            (Type::F32 {}, Value::F32(v)) => {
                let n = f32::from_bits(*v);
                if !n.is_finite() {
                    return Ok(special_float(n as f64));
                }
                serde_json::from_str(
                    &serde_json::to_string(&n).map_err(|_| invalid("float encoding"))?,
                )
                .map_err(|_| invalid("float encoding"))
            }
            (Type::F64 {}, Value::F64(v)) => {
                let n = f64::from_bits(*v);
                Ok(if n.is_finite() {
                    serde_json::Number::from_f64(n)
                        .ok_or_else(|| invalid("float encoding"))?
                        .into()
                } else {
                    special_float(n)
                })
            }
            (Type::String {}, Value::String(v)) => {
                if v.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                Ok(Json::String(v.clone()))
            }
            (Type::Bytes {}, Value::Bytes(v)) => {
                if v.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                Ok(Json::Array(
                    v.iter().map(|v| Json::from(*v as i8)).collect(),
                ))
            }
            (
                Type::Enum {
                    variants,
                },
                Value::Enum(v),
            ) => Ok(Json::String(
                variants
                    .get(*v as usize)
                    .ok_or_else(|| invalid("enum ordinal"))?
                    .clone(),
            )),
            (
                Type::Message {
                    ..
                },
                Value::Object(_),
            ) => field
                .nested
                .as_ref()
                .ok_or_else(|| invalid("missing nested plan"))?
                .to_json(value, budget),
            (
                Type::List {
                    element,
                    nullable_elements,
                },
                Value::List(values),
            ) => {
                if values.len() > field.descriptor.max_elements || values.len() > *budget {
                    return Err(CodecError::LengthExceeds);
                }
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(if matches!(value, Value::Null) {
                        consume(budget)?;
                        if !nullable_elements {
                            return Err(invalid("null collection element"));
                        }
                        Json::Null
                    } else if matches!(**element, Type::Bytes {}) {
                        consume(budget)?;
                        let Value::Bytes(bytes) = value else {
                            return Err(invalid("byte collection element required"));
                        };
                        if bytes.len() > field.descriptor.max_bytes {
                            return Err(CodecError::LengthExceeds);
                        }
                        Json::String(STANDARD.encode(bytes))
                    } else {
                        self.write_json_value(field, element, value, budget)?
                    });
                }
                Ok(Json::Array(out))
            }
            _ => Err(invalid("JSON dynamic value type mismatch")),
        }
    }
}

struct Bounded {
    bytes: Vec<u8>,
    maximum: usize,
}
impl std::io::Write for Bounded {
    /// 业务作用：在追加 JSON 输出片段前限制总编码容量，避免先分配超限字符串再拒绝。
    ///
    /// 参数说明：
    /// - `bytes`：序列化器准备写出的下一段。
    ///
    /// 返回：完整片段已追加；空间不足时不追加该片段。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "JSON capacity",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    /// 业务作用：结束内存输出刷新，内存缓冲没有外部持久化副作用。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：成功；最终容量已在每次追加时检查。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 业务作用：读取有限 JSON 数字或 Java 特殊浮点名称，不把任意数字字符串视为数值。
///
/// 参数说明：
/// - `value`：JSON 标量。
///
/// 返回：原始浮点含义；其它类型拒绝。
fn float(value: &Json) -> Result<f64> {
    match value {
        Json::String(v) => match v.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(invalid("invalid special float")),
        },
        _ => value.as_f64().ok_or_else(|| invalid("number required")),
    }
}

/// 业务作用：输出 Jackson 默认非有限浮点名称，避免 JSON null 丢失其类型语义。
///
/// 参数说明：
/// - `value`：非有限浮点数。
///
/// 返回：NaN 或正负 Infinity 字符串。
fn special_float(value: f64) -> Json {
    Json::String(
        if value.is_nan() {
            "NaN"
        } else if value.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        }
        .into(),
    )
}

struct Strict<'a> {
    budget: &'a mut usize,
    depth: usize,
    maximum_depth: usize,
}
impl<'de> DeserializeSeed<'de> for Strict<'_> {
    type Value = Json;
    /// 业务作用：在分配 JSON 节点前限制全局数量和嵌套深度。
    ///
    /// 参数说明：
    /// - `decoder`：当前 JSON 值读取器。
    ///
    /// 返回：受预算限制的节点，超限直接终止。
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Json, D::Error> {
        if self.depth > self.maximum_depth || *self.budget == 0 {
            return Err(D::Error::custom("JSON capacity"));
        }
        *self.budget -= 1;
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Strict<'_> {
    type Value = Json;
    /// 业务作用：提供固定解析错误描述，不回显正文。
    ///
    /// 参数说明：
    /// - `formatter`：错误文本输出位置。
    ///
    /// 返回：描述写入结果。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON")
    }
    /// 业务作用：保留布尔类型供 schema 验证。
    ///
    /// 参数说明：
    /// - `value`：JSON 布尔值。
    ///
    /// 返回：相同布尔值。
    fn visit_bool<E: Error>(self, value: bool) -> std::result::Result<Json, E> {
        Ok(value.into())
    }
    /// 业务作用：保留有符号整数完整精度。
    ///
    /// 参数说明：
    /// - `value`：JSON 整数。
    ///
    /// 返回：整数节点。
    fn visit_i64<E: Error>(self, value: i64) -> std::result::Result<Json, E> {
        Ok(value.into())
    }
    /// 业务作用：保留无符号整数类别供目标位宽拒绝越界。
    ///
    /// 参数说明：
    /// - `value`：JSON 无符号整数。
    ///
    /// 返回：整数节点，不转换为浮点。
    fn visit_u64<E: Error>(self, value: u64) -> std::result::Result<Json, E> {
        Ok(value.into())
    }
    /// 业务作用：保留有限浮点类别，整数 schema 不接受它替代整数 token。
    ///
    /// 参数说明：
    /// - `value`：JSON 浮点。
    ///
    /// 返回：有限数值或解析失败。
    fn visit_f64<E: Error>(self, value: f64) -> std::result::Result<Json, E> {
        serde_json::Number::from_f64(value)
            .map(Json::Number)
            .ok_or_else(|| E::custom("JSON number"))
    }
    /// 业务作用：接纳合法 UTF-8 字符串，长度继续由字段约束检查。
    ///
    /// 参数说明：
    /// - `value`：字符串原文。
    ///
    /// 返回：拥有型字符串节点。
    fn visit_str<E: Error>(self, value: &str) -> std::result::Result<Json, E> {
        Ok(Json::String(value.into()))
    }
    /// 业务作用：保留 null 与缺失、空数组的区别。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：null 节点。
    fn visit_unit<E: Error>(self) -> std::result::Result<Json, E> {
        Ok(Json::Null)
    }
    /// 业务作用：在逐项解析时扣除全局预算，防止嵌套列表分配膨胀。
    ///
    /// 参数说明：
    /// - `sequence`：数组读取器。
    ///
    /// 返回：有界列表，超限不返回部分结果。
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Json, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(Strict {
            budget: self.budget,
            depth: self.depth + 1,
            maximum_depth: self.maximum_depth,
        })? {
            values.push(value);
        }
        Ok(Json::Array(values))
    }
    /// 业务作用：拒绝重复对象键，避免不同 codec 对同一字段选择不同值。
    ///
    /// 参数说明：
    /// - `map`：对象读取器。
    ///
    /// 返回：唯一键对象；重复或预算不足时拒绝。
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Json, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom("duplicate JSON field"));
            }
            let value = map.next_value_seed(Strict {
                budget: self.budget,
                depth: self.depth + 1,
                maximum_depth: self.maximum_depth,
            })?;
            values.insert(key, value);
        }
        Ok(Json::Object(values))
    }
}
