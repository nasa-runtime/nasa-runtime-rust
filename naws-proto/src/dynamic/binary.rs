use super::{consume, invalid, FieldPlan, SchemaPlan, Type, Value};
use crate::{
    io::{write_varint, zigzag_dec, zigzag_enc, Reader},
    CodecError, Mode, Result,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

impl SchemaPlan {
    /// 业务作用：按已编译二进制布局读取对象并保留字节字段的共享切片。
    ///
    /// 参数说明：
    /// - `bytes`：完整对象字节区。
    /// - `budget`：整个嵌套对象剩余动态值额度。
    ///
    /// 返回：完整对象；非法字段、布局或尾随字节拒绝。
    pub(super) fn decode_binary(&self, bytes: Bytes, budget: &mut usize) -> Result<Value> {
        consume(budget)?;
        if bytes.len() > self.limits.max_message_bytes {
            return Err(CodecError::LengthExceeds);
        }
        let mut reader = Reader::new(&bytes);
        let mut values: BTreeMap<_, _> = self
            .fields
            .iter()
            .map(|f| {
                (
                    f.descriptor.name.clone(),
                    f.descriptor.typ.default_value(f.descriptor.nullable),
                )
            })
            .collect();
        match self.mode {
            Mode::VarintTlv => {
                let mut seen = BTreeSet::new();
                while reader.remaining() > 0 {
                    let key = reader.read_varint()?;
                    let tag = u32::try_from(key >> 3).map_err(|_| CodecError::VarintOverflow)?;
                    if tag == 0 || tag > i32::MAX as u32 {
                        return Err(invalid("invalid field tag"));
                    }
                    if let Some(index) = self.tags.get(&tag) {
                        if !seen.insert(tag) {
                            return Err(invalid("duplicate field tag"));
                        }
                        let field = &self.fields[*index];
                        crate::io::check_wt(tag.into(), field.descriptor.typ.wire(), key & 7)?;
                        let value = self.read_value(
                            field,
                            &field.descriptor.typ,
                            &mut reader,
                            &bytes,
                            budget,
                            false,
                        )?;
                        values.insert(field.descriptor.name.clone(), value);
                    } else {
                        reader.skip_value(key & 7)?;
                    }
                }
            }
            Mode::BitpackTlv => {
                let bitmap = reader.read_bitmap(bitmap_width(self.fields.len()))?;
                if self.fields.len() < 64 && bitmap >> self.fields.len() != 0 {
                    return Err(CodecError::BadBitmap);
                }
                for (index, field) in self.fields.iter().enumerate() {
                    if bitmap & (1u64 << index) != 0 {
                        let value = self.read_value(
                            field,
                            &field.descriptor.typ,
                            &mut reader,
                            &bytes,
                            budget,
                            false,
                        )?;
                        values.insert(field.descriptor.name.clone(), value);
                    }
                }
            }
            Mode::FastFixed => {
                if Some(bytes.len()) != self.fixed_length {
                    return Err(invalid("fixed length mismatch"));
                }
                for field in &self.fields {
                    let value = self.read_value(
                        field,
                        &field.descriptor.typ,
                        &mut reader,
                        &bytes,
                        budget,
                        true,
                    )?;
                    values.insert(field.descriptor.name.clone(), value);
                }
            }
            Mode::JsonBytes => return Err(invalid("binary plan mode mismatch")),
        }
        reader.expect_end()?;
        for field in &self.fields {
            if !field.descriptor.nullable
                && matches!(values.get(&field.descriptor.name), Some(Value::Null))
            {
                return Err(invalid("required reference is absent"));
            }
        }
        Ok(Value::Object(values))
    }

    /// 业务作用：按单一二进制模式编码完整对象，所有中间容器均受计划预算限制。
    ///
    /// 参数说明：
    /// - `value`：与当前 schema 对应的对象。
    /// - `budget`：整个对象剩余动态值额度。
    ///
    /// 返回：二进制对象字节；缺失必填引用、类型不匹配或容量不足拒绝。
    pub(super) fn encode_binary(&self, value: &Value, budget: &mut usize) -> Result<Vec<u8>> {
        consume(budget)?;
        let Value::Object(values) = value else {
            return Err(invalid("object required"));
        };
        if values
            .keys()
            .any(|key| !self.fields.iter().any(|f| &f.descriptor.name == key))
        {
            return Err(invalid("unknown object field"));
        }
        let mut out = Vec::new();
        let mut bitmap = 0u64;
        if self.mode == Mode::BitpackTlv {
            out.resize(bitmap_width(self.fields.len()), 0);
        }
        for (index, field) in self.fields.iter().enumerate() {
            let default = field
                .descriptor
                .typ
                .default_value(field.descriptor.nullable);
            let value = values.get(&field.descriptor.name).unwrap_or(&default);
            if matches!(value, Value::Null) {
                if !field.descriptor.nullable {
                    return Err(invalid("nonnullable field is null"));
                }
                continue;
            }
            match self.mode {
                Mode::VarintTlv => write_varint(
                    &mut out,
                    ((field.descriptor.tag as u64) << 3) | field.descriptor.typ.wire(),
                ),
                Mode::BitpackTlv => bitmap |= 1u64 << index,
                _ => {}
            }
            self.write_value(
                field,
                &field.descriptor.typ,
                value,
                &mut out,
                budget,
                self.mode == Mode::FastFixed,
            )?;
            if out.len() > self.limits.max_message_bytes {
                return Err(CodecError::LengthExceeds);
            }
        }
        if self.mode == Mode::BitpackTlv {
            for (index, byte) in out[..bitmap_width(self.fields.len())]
                .iter_mut()
                .enumerate()
            {
                *byte = (bitmap >> (index * 8)) as u8;
            }
        }
        Ok(out)
    }

    /// 业务作用：读取字段值并校验声明位宽，长度分隔字段先核验剩余再产生共享子区间。
    ///
    /// 参数说明：
    /// - `field`：字段限额和已解析的嵌套计划。
    /// - `typ`：当前字段或元素的类型。
    /// - `reader`：当前对象的受检游标。
    /// - `owner`：支撑借用切片的原始 Bytes。
    /// - `budget`：展开对象共享额度。
    /// - `fixed`：是否使用 primitive 固定布局。
    ///
    /// 返回：与类型一致的值；任何窄化、枚举或长度异常均拒绝。
    fn read_value(
        &self,
        field: &FieldPlan,
        typ: &Type,
        reader: &mut Reader<'_>,
        owner: &Bytes,
        budget: &mut usize,
        fixed: bool,
    ) -> Result<Value> {
        consume(budget)?;
        let result = match typ {
            Type::Bool {} => {
                let n = if fixed {
                    reader.read_fixed::<1>()?[0] as u64
                } else {
                    reader.read_varint()?
                };
                if n > 1 {
                    return Err(invalid("invalid boolean"));
                }
                Value::Bool(n == 1)
            }
            Type::I8 {} | Type::I16 {} | Type::I32 {} | Type::I64 {} => {
                let value = if fixed {
                    match typ {
                        Type::I8 {} => i8::from_le_bytes(reader.read_fixed()?) as i64,
                        Type::I16 {} => i16::from_le_bytes(reader.read_fixed()?) as i64,
                        Type::I32 {} => i32::from_le_bytes(reader.read_fixed()?) as i64,
                        _ => i64::from_le_bytes(reader.read_fixed()?),
                    }
                } else {
                    zigzag_dec(reader.read_varint()?)
                };
                signed(typ, value)?;
                Value::Signed(value)
            }
            Type::Char {} => Value::Char(if fixed {
                u16::from_le_bytes(reader.read_fixed()?)
            } else {
                u16::try_from(reader.read_varint()?).map_err(|_| CodecError::VarintOverflow)?
            }),
            Type::Enum {
                variants,
            } => {
                let ordinal = reader.read_varint()?;
                if ordinal >= variants.len() as u64 {
                    return Err(invalid("invalid enum ordinal"));
                }
                Value::Enum(ordinal as u32)
            }
            Type::F32 {} => Value::F32(u32::from_le_bytes(reader.read_fixed()?)),
            Type::F64 {} => Value::F64(u64::from_le_bytes(reader.read_fixed()?)),
            _ => {
                let length = reader.read_len()?;
                if length > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                let slice = reader.read_slice(length)?;
                self.read_content(field, typ, owner.slice_ref(slice), budget)?
            }
        };
        Ok(result)
    }

    /// 业务作用：解析已剥离长度前缀的引用字段或集合元素，沿用 Java 的零长度元素语义。
    ///
    /// 参数说明：
    /// - `field`：字段资源预算与嵌套计划。
    /// - `typ`：引用类型。
    /// - `bytes`：已经核验长度的共享区域。
    /// - `budget`：展开对象共享额度。
    ///
    /// 返回：引用值；集合计数、元素长度或嵌套对象不合法时拒绝。
    fn read_content(
        &self,
        field: &FieldPlan,
        typ: &Type,
        bytes: Bytes,
        budget: &mut usize,
    ) -> Result<Value> {
        match typ {
            Type::String {} => Ok(Value::String(
                std::str::from_utf8(&bytes)
                    .map_err(|_| CodecError::Utf8)?
                    .into(),
            )),
            Type::Bytes {} => Ok(Value::Bytes(bytes)),
            Type::Message {
                ..
            } => field
                .nested
                .as_ref()
                .ok_or_else(|| invalid("missing nested plan"))?
                .decode_binary(bytes, budget),
            Type::List {
                element,
                nullable_elements,
            } => {
                let mut reader = Reader::new(&bytes);
                let count = usize::try_from(reader.read_varint()?)
                    .map_err(|_| CodecError::LengthExceeds)?;
                if count > field.descriptor.max_elements
                    || count > reader.remaining()
                    || count > *budget
                {
                    return Err(CodecError::LengthExceeds);
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    let length = reader.read_len()?;
                    if length > field.descriptor.max_bytes {
                        return Err(CodecError::LengthExceeds);
                    }
                    let slice = reader.read_slice(length)?;
                    let value = if length == 0 {
                        consume(budget)?;
                        if !nullable_elements {
                            return Err(invalid("null collection element"));
                        }
                        Value::Null
                    } else if element.wire() == 2 {
                        consume(budget)?;
                        self.read_content(field, element, bytes.slice_ref(slice), budget)?
                    } else {
                        let mut element_reader = Reader::new(slice);
                        let value = self.read_value(
                            field,
                            element,
                            &mut element_reader,
                            &bytes,
                            budget,
                            false,
                        )?;
                        element_reader.expect_end()?;
                        value
                    };
                    values.push(value);
                }
                reader.expect_end()?;
                Ok(Value::List(values))
            }
            _ => Err(invalid("length content type mismatch")),
        }
    }

    /// 业务作用：按字段类型写出数值或长度分隔内容，固定模式保留浮点原始位。
    ///
    /// 参数说明：
    /// - `field`：当前字段计划。
    /// - `typ`：当前字段或元素类型。
    /// - `value`：已选取的动态值。
    /// - `out`：当前有限输出缓冲。
    /// - `budget`：整个对象共享额度。
    /// - `fixed`：是否按 primitive 固定宽度写出。
    ///
    /// 返回：追加一个完整值；不匹配时调用者丢弃整个对象缓冲。
    fn write_value(
        &self,
        field: &FieldPlan,
        typ: &Type,
        value: &Value,
        out: &mut Vec<u8>,
        budget: &mut usize,
        fixed: bool,
    ) -> Result<()> {
        consume(budget)?;
        match (typ, value) {
            (Type::Bool {}, Value::Bool(value)) => {
                if fixed {
                    out.push(u8::from(*value))
                } else {
                    write_varint(out, u64::from(*value))
                }
            }
            (Type::I8 {} | Type::I16 {} | Type::I32 {} | Type::I64 {}, Value::Signed(value)) => {
                signed(typ, *value)?;
                if fixed {
                    out.extend_from_slice(
                        &value.to_le_bytes()[..typ.width().expect("integer width")],
                    );
                } else {
                    write_varint(out, zigzag_enc(*value));
                }
            }
            (Type::Char {}, Value::Char(value)) => {
                if fixed {
                    out.extend_from_slice(&value.to_le_bytes())
                } else {
                    write_varint(out, (*value).into())
                }
            }
            (
                Type::Enum {
                    variants,
                },
                Value::Enum(value),
            ) => {
                if *value as usize >= variants.len() {
                    return Err(invalid("invalid enum ordinal"));
                }
                write_varint(out, (*value).into());
            }
            (Type::F32 {}, Value::F32(bits)) => {
                let bits = if !fixed && f32::from_bits(*bits).is_nan() {
                    0x7fc00000
                } else {
                    *bits
                };
                out.extend_from_slice(&bits.to_le_bytes());
            }
            (Type::F64 {}, Value::F64(bits)) => {
                let bits = if !fixed && f64::from_bits(*bits).is_nan() {
                    0x7ff8000000000000
                } else {
                    *bits
                };
                out.extend_from_slice(&bits.to_le_bytes());
            }
            (
                Type::String {}
                | Type::Bytes {}
                | Type::Message {
                    ..
                }
                | Type::List {
                    ..
                },
                _,
            ) => {
                let content = self.write_content(field, typ, value, budget)?;
                if content.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                write_varint(out, content.len() as u64);
                out.extend_from_slice(&content);
            }
            _ => return Err(invalid("dynamic value type mismatch")),
        }
        Ok(())
    }

    /// 业务作用：构造有限引用内容，集合元素使用独立长度且不重复包字段长度。
    ///
    /// 参数说明：
    /// - `field`：字段预算与嵌套计划。
    /// - `typ`：引用类型或集合元素类型。
    /// - `value`：当前值。
    /// - `budget`：整个展开对象共享额度。
    ///
    /// 返回：单个内容区域；集合中有歧义的空值遵循 nullableElements 约束。
    fn write_content(
        &self,
        field: &FieldPlan,
        typ: &Type,
        value: &Value,
        budget: &mut usize,
    ) -> Result<Vec<u8>> {
        let out = match (typ, value) {
            (Type::String {}, Value::String(value)) => {
                if value.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                value.as_bytes().to_vec()
            }
            (Type::Bytes {}, Value::Bytes(value)) => {
                if value.len() > field.descriptor.max_bytes {
                    return Err(CodecError::LengthExceeds);
                }
                value.to_vec()
            }
            (
                Type::Message {
                    ..
                },
                Value::Object(_),
            ) => field
                .nested
                .as_ref()
                .ok_or_else(|| invalid("missing nested plan"))?
                .encode_binary(value, budget)?,
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
                let mut out = Vec::new();
                write_varint(&mut out, values.len() as u64);
                for value in values {
                    let content = if matches!(value, Value::Null) {
                        consume(budget)?;
                        Vec::new()
                    } else if element.wire() == 2 {
                        consume(budget)?;
                        self.write_content(field, element, value, budget)?
                    } else {
                        let mut bytes = Vec::new();
                        self.write_value(field, element, value, &mut bytes, budget, false)?;
                        bytes
                    };
                    if content.is_empty() && !nullable_elements {
                        return Err(invalid("empty element decodes as null"));
                    }
                    write_varint(&mut out, content.len() as u64);
                    out.extend_from_slice(&content);
                    if out.len() > field.descriptor.max_bytes {
                        return Err(CodecError::LengthExceeds);
                    }
                }
                out
            }
            _ => return Err(invalid("length value type mismatch")),
        };
        if out.len() > field.descriptor.max_bytes {
            return Err(CodecError::LengthExceeds);
        }
        Ok(out)
    }
}

/// 业务作用：在编码与解码的窄化边界检查有符号整数，不允许截断回绕。
///
/// 参数说明：
/// - `typ`：目标整数位宽。
/// - `value`：完整 i64 数值。
///
/// 返回：目标位宽可表示时成功；越界返回原有 VarintOverflow 错误。
pub(super) fn signed(typ: &Type, value: i64) -> Result<()> {
    let valid = match typ {
        Type::I8 {} => i8::try_from(value).is_ok(),
        Type::I16 {} => i16::try_from(value).is_ok(),
        Type::I32 {} => i32::try_from(value).is_ok(),
        Type::I64 {} => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(CodecError::VarintOverflow)
    }
}

/// 业务作用：按 ProtocolBytes 字段数量选择 bitmap 的固定宽度。
///
/// 参数说明：
/// - `count`：已限制到最多 64 的字段数。
///
/// 返回：1、2、4 或 8 字节，空对象仍有一个零字节。
fn bitmap_width(count: usize) -> usize {
    if count <= 8 {
        1
    } else if count <= 16 {
        2
    } else if count <= 32 {
        4
    } else {
        8
    }
}
