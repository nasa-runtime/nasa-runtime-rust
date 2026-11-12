//! 受限 SchemaDescriptor 编译为不可变 SchemaPlan；业务对象按需编解码。
//!
//! 所有嵌套 schema 使用调用者选定的同一种模式。二进制集合沿用 ProtocolBytes 的
//! count、元素长度和元素字节布局，零长度元素解码为 Null，不能区分空字符串与 null。

mod binary;
mod json;
use crate::{CodecError, Mode, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// 可发布的类型描述；不允许任意类型名、Map 或执行表达式。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Type {
    /// 布尔值，缺省 primitive 值为 false。
    Bool {},
    /// 有符号八位整数，编码时验证数值范围。
    I8 {},
    /// 有符号十六位整数，编码时验证数值范围。
    I16 {},
    /// 有符号三十二位整数，编码时验证数值范围。
    I32 {},
    /// 有符号六十四位整数，不经浮点中间值转换。
    I64 {},
    /// 与 Java char 对应的单个 UTF-16 码元。
    Char {},
    /// 单精度浮点，动态值保存原始三十二位表示。
    F32 {},
    /// 双精度浮点，动态值保存原始六十四位表示。
    F64 {},
    /// 受字段字节上限约束的字符串。
    String {},
    /// 不解析内部内容的字节序列。
    Bytes {},
    /// 使用声明顺序确定二进制序号的封闭枚举。
    Enum {
        /// 不重复的枚举名称列表；位置对应从零开始的序号，顺序属于通信合同。
        variants: Vec<String>,
    },
    /// 引用同一目录中的对象定义，不允许远程加载或循环引用。
    Message {
        /// 目录键与目标 schema_id 必须一致，嵌套对象沿用外层编码模式。
        #[serde(rename = "schemaRef")]
        schema_ref: String,
    },
    /// 同一元素类型的有界集合，不支持直接嵌套集合。
    List {
        /// 元素的静态类型；对象元素只能引用已声明 schema。
        element: Box<Type>,
        /// 是否允许空元素；默认为 false，二进制零长度元素按 Null 解释。
        #[serde(default, rename = "nullableElements")]
        nullable_elements: bool,
    },
}

/// 字段的精确语义及资源上限；引用字段可选择更严格的非空约束。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldDescriptor {
    /// 大于零且在对象内唯一的字段编号，计划按此编号排序。
    pub tag: u32,
    /// 对象映射和 JSON 使用的字段名，在同一 schema 内唯一。
    pub name: String,
    /// 字段唯一类型声明，不由每条消息覆盖。
    #[serde(rename = "type")]
    pub typ: Type,
    /// 是否接受 Null；缺省为 false，primitive 的省略值按对应零值处理。
    #[serde(default)]
    pub nullable: bool,
    /// 字段可变长度内容的字节预算，默认 64 KiB，不能超过消息总上限。
    #[serde(default = "default_bytes")]
    pub max_bytes: usize,
    /// 集合元素数量上限，默认 1024，不能超过完整对象的值数量预算。
    #[serde(default = "default_elements")]
    pub max_elements: usize,
}

/// 声明对象中可传输的字段，未知 JSON 字段按 Java 兼容语义跳过。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaDescriptor {
    /// 目录内对象定义的稳定标识，必须与编译时查找键一致。
    pub schema_id: String,
    /// 对象的完整字段定义；编译时检查重复名称、编号和类型限制。
    pub fields: Vec<FieldDescriptor>,
}

/// 动态值保留浮点原始位与 i64 精度，字节字段共享输入缓冲。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// 未提供的引用值或显式空值，是否可编码取决于字段声明。
    Null,
    /// 布尔业务值。
    Bool(bool),
    /// 有符号整数，编码时按声明的整数位宽验证范围。
    Signed(i64),
    /// 单个 UTF-16 码元，不等同于任意长度的 Unicode 字符串。
    Char(u16),
    /// 单精度浮点的原始位模式。
    F32(u32),
    /// 双精度浮点的原始位模式。
    F64(u64),
    /// 已解码的 UTF-8 字符串。
    String(String),
    /// 可与输入共享底层分配的原始字节。
    Bytes(Bytes),
    /// 枚举声明中的零基序号，必须落在允许的成员范围内。
    Enum(u32),
    /// 按输入顺序保存的集合值，受元素和总值数量预算约束。
    List(Vec<Value>),
    /// 按 schema 字段名映射的对象值，不能替代类型声明传入任意 Map。
    Object(BTreeMap<String, Value>),
}

/// 独立于任何应用的编译和运行资源上限。
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// schema 引用链最大层数；运行时另外限制展开值深度。
    pub max_depth: usize,
    /// 完整计划递归展开后的字段数量上限，重复引用也计入预算。
    pub max_fields: usize,
    /// 完整业务对象编解码允许的字节上限。
    pub max_message_bytes: usize,
    /// 完整对象及其集合展开共享的动态值数量预算。
    pub max_values: usize,
}

impl Default for Limits {
    /// 业务作用：为不受信协议对象设置有限的默认编译和解析预算。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：深度 8、展开字段 4096、消息 1 MiB 和动态值 65536 的默认上限。
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_fields: 4096,
            max_message_bytes: 1024 * 1024,
            max_values: 65536,
        }
    }
}

/// 编译计划锁定唯一模式、排序字段、嵌套计划及固定布局大小。
pub struct SchemaPlan {
    descriptor: SchemaDescriptor,
    pub(crate) mode: Mode,
    pub(crate) fields: Vec<FieldPlan>,
    pub(crate) tags: BTreeMap<u32, usize>,
    pub(crate) fixed_length: Option<usize>,
    pub(crate) limits: Limits,
}

pub(crate) struct FieldPlan {
    pub descriptor: FieldDescriptor,
    pub nested: Option<Arc<SchemaPlan>>,
}

impl SchemaPlan {
    /// 业务作用：从封闭 schema 目录构建单一模式的不可变计划，拒绝循环和不适用布局。
    ///
    /// 参数说明：
    /// - `schema_id`：本次编译的对象标识。
    /// - `catalog`：已完成配置结构解析的 schema 目录。
    /// - `mode`：全部嵌套对象共同使用的唯一模式。
    /// - `limits`：编译深度、展开字段数及运行预算。
    ///
    /// 返回：可共享计划；缺引用、循环、非法字段或超限均不产出半份计划。
    pub fn compile(
        schema_id: &str,
        catalog: &BTreeMap<String, SchemaDescriptor>,
        mode: Mode,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        if limits.max_depth == 0
            || limits.max_depth > 32
            || limits.max_fields == 0
            || limits.max_fields > 65536
            || limits.max_message_bytes == 0
            || limits.max_message_bytes > 64 * 1024 * 1024
            || limits.max_values == 0
            || limits.max_values > 1_000_000
            || catalog.len() > 1024
        {
            return Err(invalid("invalid schema limits"));
        }
        let mut fields = 0;
        Self::compile_inner(
            schema_id,
            catalog,
            mode,
            limits,
            &mut Vec::new(),
            &mut fields,
        )
    }

    /// 业务作用：递归解析嵌套引用并计算合法定宽布局，不允许加载网络 schema 或代码。
    ///
    /// 参数说明：
    /// - `id`：当前引用的 schema 标识。
    /// - `catalog`：固定 schema 目录。
    /// - `mode`：统一编码模式。
    /// - `limits`：编译和解码边界。
    /// - `path`：当前引用链，用于发现循环。
    /// - `expanded`：本次完整计划已展开的字段数量。
    ///
    /// 返回：引用完备的计划；任一嵌套失败使整个编译失败。
    fn compile_inner(
        id: &str,
        catalog: &BTreeMap<String, SchemaDescriptor>,
        mode: Mode,
        limits: Limits,
        path: &mut Vec<String>,
        expanded: &mut usize,
    ) -> Result<Arc<Self>> {
        if path.len() >= limits.max_depth || path.iter().any(|v| v == id) {
            return Err(invalid("recursive or deep schema"));
        }
        let mut descriptor = catalog
            .get(id)
            .ok_or_else(|| invalid("missing schema reference"))?
            .clone();
        if descriptor.schema_id != id
            || !name(id)
            || descriptor.fields.len() > 256
            || (mode == Mode::BitpackTlv && descriptor.fields.len() > 64)
        {
            return Err(invalid("invalid schema identity or field count"));
        }
        *expanded = expanded
            .checked_add(descriptor.fields.len())
            .ok_or(CodecError::LengthOverflow)?;
        if *expanded > limits.max_fields {
            return Err(CodecError::LengthExceeds);
        }
        descriptor.fields.sort_by_key(|field| field.tag);
        let mut names = BTreeSet::new();
        let mut tags = BTreeMap::new();
        let mut fields = Vec::new();
        let mut fixed_length = Some(0usize);
        path.push(id.into());
        for (index, field) in descriptor.fields.iter().enumerate() {
            if field.tag == 0
                || field.tag > i32::MAX as u32
                || !name(&field.name)
                || !names.insert(&field.name)
                || tags.insert(field.tag, index).is_some()
                || field.max_bytes == 0
                || field.max_bytes > limits.max_message_bytes
                || field.max_elements == 0
                || field.max_elements > limits.max_values
            {
                return Err(invalid("invalid schema field"));
            }
            validate_type(&field.typ)?;
            let nested = match nested_ref(&field.typ) {
                Some(id) => Some(Self::compile_inner(
                    id, catalog, mode, limits, path, expanded,
                )?),
                None => None,
            };
            fixed_length = match (fixed_length, field.typ.width()) {
                (Some(total), Some(width)) if !field.nullable => total.checked_add(width),
                _ => None,
            };
            fields.push(FieldPlan {
                descriptor: field.clone(),
                nested,
            });
        }
        path.pop();
        if mode == Mode::FastFixed && fixed_length.is_none() {
            return Err(invalid("FAST_FIXED requires nonnullable primitives"));
        }
        Ok(Arc::new(Self {
            descriptor,
            mode,
            fields,
            tags,
            fixed_length,
            limits,
        }))
    }

    /// 业务作用：公开归一化 schema，供调用者生成摘要、目录和跨语言合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：按 tag 升序排列且默认值已显式展开的描述。
    pub fn descriptor(&self) -> &SchemaDescriptor {
        &self.descriptor
    }

    /// 业务作用：按编译时固定模式解码业务对象，不接受消息逐条覆盖模式。
    ///
    /// 参数说明：
    /// - `bytes`：完整业务区共享缓冲，字节字段可继续借用其所有权。
    ///
    /// 返回：受预算约束的对象；类型、长度或格式不合法时不返回部分对象。
    pub fn decode(&self, bytes: Bytes) -> Result<Value> {
        if bytes.len() > self.limits.max_message_bytes {
            return Err(CodecError::LengthExceeds);
        }
        let mut budget = self.limits.max_values;
        if self.mode == Mode::JsonBytes {
            self.decode_json(&bytes, &mut budget)
        } else {
            self.decode_binary(bytes, &mut budget)
        }
    }

    /// 业务作用：在单一 schema 和模式下编码对象，字节及元素预算统一应用于整个展开值。
    ///
    /// 参数说明：
    /// - `value`：与 schema 匹配的动态对象。
    ///
    /// 返回：有限共享编码缓冲；类型不一致或超限不产出部分结果。
    pub fn encode(&self, value: &Value) -> Result<Bytes> {
        self.check_value_budget(value)?;
        let mut budget = self.limits.max_values;
        let bytes = if self.mode == Mode::JsonBytes {
            self.encode_json(value, &mut budget)?
        } else {
            self.encode_binary(value, &mut budget)?
        };
        if bytes.len() > self.limits.max_message_bytes {
            return Err(CodecError::LengthExceeds);
        }
        Ok(bytes.into())
    }

    /// 业务作用：按同一 schema 导入 JSON 表示，供 HTTP 或配置边界转换为固定业务编码。
    ///
    /// 参数说明：
    /// - `bytes`：遵循 ProtocolBytes JSON 字段约定的完整对象。
    ///
    /// 返回：经过类型和资源验证的对象；不改变计划的二进制模式。
    pub fn import_json(&self, bytes: &[u8]) -> Result<Value> {
        if bytes.len() > self.limits.max_message_bytes {
            return Err(CodecError::LengthExceeds);
        }
        let mut budget = self.limits.max_values;
        self.decode_json(bytes, &mut budget)
    }

    /// 业务作用：按同一 schema 导出有界 JSON 表示，避免调用方另行猜测数值和字节字段格式。
    ///
    /// 参数说明：
    /// - `value`：待导出的 schema 对象。
    ///
    /// 返回：遵循 ProtocolBytes JSON 约定的字节；不修改计划的编码选择。
    pub fn export_json(&self, value: &Value) -> Result<Bytes> {
        self.check_value_budget(value)?;
        let mut budget = self.limits.max_values;
        self.encode_json(value, &mut budget).map(Bytes::from)
    }

    /// 业务作用：在复制动态字符串或构造 JSON 中间节点前核验完整对象资源占用。
    ///
    /// 参数说明：
    /// - `value`：调用者构造的动态对象，不假定已经通过 schema 验证。
    ///
    /// 返回：总节点、深度和原始数据预算足够时成功，不为无界输入分配输出容器。
    fn check_value_budget(&self, value: &Value) -> Result<()> {
        let mut pending = vec![(value, 0usize)];
        let mut values = 0usize;
        let mut bytes = 0usize;
        while let Some((value, depth)) = pending.pop() {
            values = values.checked_add(1).ok_or(CodecError::LengthOverflow)?;
            if values > self.limits.max_values || depth > self.limits.max_depth * 2 + 2 {
                return Err(CodecError::LengthExceeds);
            }
            let size = match value {
                Value::String(v) => v.len(),
                Value::Bytes(v) => v.len(),
                Value::List(v) => {
                    if v.len()
                        > self
                            .limits
                            .max_values
                            .saturating_sub(values + pending.len())
                    {
                        return Err(CodecError::LengthExceeds);
                    }
                    pending.extend(v.iter().map(|v| (v, depth + 1)));
                    0
                }
                Value::Object(v) => {
                    if v.len()
                        > self
                            .limits
                            .max_values
                            .saturating_sub(values + pending.len())
                    {
                        return Err(CodecError::LengthExceeds);
                    }
                    pending.extend(v.values().map(|v| (v, depth + 1)));
                    v.keys()
                        .try_fold(0usize, |total, key| total.checked_add(key.len()))
                        .ok_or(CodecError::LengthOverflow)?
                }
                _ => 0,
            };
            bytes = bytes.checked_add(size).ok_or(CodecError::LengthOverflow)?;
            if bytes > self.limits.max_message_bytes {
                return Err(CodecError::LengthExceeds);
            }
        }
        Ok(())
    }
}

impl Type {
    /// 业务作用：给出 ProtocolBytes 的稳定 wire type，不由输入数据自行决定。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：整数 0、double 1、长度分隔 2、float 5。
    pub(crate) fn wire(&self) -> u64 {
        match self {
            Self::F64 {} => 1,
            Self::F32 {} => 5,
            Self::String {}
            | Self::Bytes {}
            | Self::Message {
                ..
            }
            | Self::List {
                ..
            } => 2,
            _ => 0,
        }
    }

    /// 业务作用：识别 FAST_FIXED 允许的 primitive 固定宽度，引用与枚举不具备固定布局资格。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：合法 primitive 的字节数，其余类型为 None。
    pub(crate) fn width(&self) -> Option<usize> {
        match self {
            Self::Bool {} | Self::I8 {} => Some(1),
            Self::I16 {} | Self::Char {} => Some(2),
            Self::I32 {} | Self::F32 {} => Some(4),
            Self::I64 {} | Self::F64 {} => Some(8),
            _ => None,
        }
    }

    /// 业务作用：为新对象提供 Java primitive 缺省值，引用字段缺失保持 Null。
    ///
    /// 参数说明：
    /// - `nullable`：是否是引用包装数值或其它可空字段。
    ///
    /// 返回：零、false 或 Null，不复用前一条消息残留数据。
    pub(crate) fn default_value(&self, nullable: bool) -> Value {
        if nullable {
            return Value::Null;
        }
        match self {
            Self::Bool {} => Value::Bool(false),
            Self::I8 {} | Self::I16 {} | Self::I32 {} | Self::I64 {} => Value::Signed(0),
            Self::Char {} => Value::Char(0),
            Self::F32 {} => Value::F32(0),
            Self::F64 {} => Value::F64(0),
            _ => Value::Null,
        }
    }
}

/// 业务作用：检查精确类型和枚举顺序，拒绝 Java 二进制不支持的嵌套集合。
///
/// 参数说明：
/// - `typ`：一个字段或集合元素的类型描述。
///
/// 返回：类型可执行时成功；非法映射和多层集合拒绝。
fn validate_type(typ: &Type) -> Result<()> {
    match typ {
        Type::Enum {
            variants,
        } => {
            let mut seen = BTreeSet::new();
            if variants.is_empty()
                || variants.len() > 1024
                || variants.iter().any(|v| !name(v) || !seen.insert(v))
            {
                return Err(invalid("invalid enum variants"));
            }
        }
        Type::Message {
            schema_ref,
        } if !name(schema_ref) => return Err(invalid("invalid schema reference")),
        Type::List {
            element,
            ..
        } => {
            if matches!(**element, Type::List { .. }) {
                return Err(invalid("nested collections unsupported"));
            }
            validate_type(element)?;
        }
        _ => {}
    }
    Ok(())
}

/// 业务作用：定位字段直接或集合元素引用的嵌套对象。
///
/// 参数说明：
/// - `typ`：已验证类型描述。
///
/// 返回：静态 schema 引用；标量返回 None。
fn nested_ref(typ: &Type) -> Option<&str> {
    match typ {
        Type::Message {
            schema_ref,
        } => Some(schema_ref),
        Type::List {
            element,
            ..
        } => nested_ref(element),
        _ => None,
    }
}

/// 业务作用：限定 schema 和字段名字的可传输范围，避免无限字符串或控制字符。
///
/// 参数说明：
/// - `value`：待校验的定义名称。
///
/// 返回：非空、有界且无控制字符时为 true。
fn name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

/// 业务作用：使用既有错误类型表示不受支持的定义或动态值，保持公开错误枚举兼容。
///
/// 参数说明：
/// - `reason`：不含输入正文的固定原因。
///
/// 返回：固定 Unsupported 错误，不回显业务数据。
pub(crate) fn invalid(reason: &'static str) -> CodecError {
    CodecError::Unsupported(reason)
}

/// 业务作用：为字段提供有限的默认可变长度预算。
///
/// 参数说明: 无。
///
/// 返回：每个字段最多 64 KiB。
fn default_bytes() -> usize {
    65536
}

/// 业务作用：为集合提供有限的默认元素容量。
///
/// 参数说明: 无。
///
/// 返回：单集合最多 1024 个元素。
fn default_elements() -> usize {
    1024
}

/// 业务作用：扣除整个对象展开共享的动态值预算，限制嵌套集合总分配。
///
/// 参数说明：
/// - `budget`：尚未使用的值数量。
///
/// 返回：成功消费一个额度；额度耗尽则终止对象处理。
pub(crate) fn consume(budget: &mut usize) -> Result<()> {
    *budget = budget.checked_sub(1).ok_or(CodecError::LengthExceeds)?;
    Ok(())
}
