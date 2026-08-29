//! Job 参数载荷：业务提供的字节、线编码与 schema 标识；框架只按契约校验，不猜测字段语义。
//!
//! 载荷字节以 Base64 进入 Run 记录和 Dispatch 消息，是持久事实；`schema_id` 与 `codec` 必须匹配已登记
//! Worker 契约，否则命令在写入前拒绝，避免不可解析的载荷进入执行链。

use std::collections::HashSet;
use std::fmt;

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor,
};

use crate::job::model::JobWireCodec;

const MAX_JSON_DEPTH: usize = 64;
const MAX_JSON_NODES: usize = 100_000;

/// 业务作用：严格校验 Job JSON 载荷，递归拒绝重复键、动态类型元数据和资源消耗失控的结构。
///
/// 参数说明：`bytes` 为 Base64 外层解开后的 JSON 原始字节。
///
/// 返回：UTF-8、语法、深度、节点数和对象键合同全部成立时成功；否则返回不包含载荷内容的摘要。
pub fn validate_json_payload(bytes: &[u8]) -> core::result::Result<(), String> {
    let mut budget = JsonBudget { nodes: 0 };
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    StrictJsonSeed {
        budget: &mut budget,
        depth: 0,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())
}

/// 业务作用：在严格 JSON 门禁通过后解码静态业务类型，避免 serde 默认的重复键覆盖进入 Handler。
///
/// 参数说明：`bytes` 为当前 Run 的 JSON 原始字节，`T` 是宏签名冻结的业务类型。
///
/// 返回：安全结构与业务反序列化均成功时返回值；任一失败返回不含载荷正文的摘要。
pub fn decode_json_payload<T: DeserializeOwned>(bytes: &[u8]) -> core::result::Result<T, String> {
    validate_json_payload(bytes)?;
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}

/// 业务作用：累计单个 RedisJob JSON 载荷的解析节点，限制结构复杂度消耗。
struct JsonBudget {
    nodes: usize,
}

impl JsonBudget {
    /// 业务作用：累计 JSON 节点并在上限前停止解析，避免小字节深层或碎片结构长期占用执行器。
    ///
    /// 参数说明：`E` 为当前 serde 解析器的错误类型。
    ///
    /// 返回：预算尚存时成功；超过固定节点上限时返回解析错误。
    fn consume<E: de::Error>(&mut self) -> core::result::Result<(), E> {
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes > MAX_JSON_NODES {
            return Err(E::custom("JSON 节点数超过安全上限"));
        }
        Ok(())
    }
}

/// 业务作用：把共享节点预算与当前递归深度传入 serde 的下一层解析。
struct StrictJsonSeed<'a> {
    budget: &'a mut JsonBudget,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for StrictJsonSeed<'_> {
    type Value = ();

    /// 业务作用：为当前 JSON 节点建立递归访问器，并在进入容器前执行深度与总量门禁。
    ///
    /// 参数说明：`deserializer` 为 serde_json 当前节点解析器。
    ///
    /// 返回：节点及其全部子节点满足安全合同时成功；否则立即停止。
    fn deserialize<D>(self, deserializer: D) -> core::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if self.depth > MAX_JSON_DEPTH {
            return Err(D::Error::custom("JSON 深度超过安全上限"));
        }
        self.budget.consume::<D::Error>()?;
        deserializer.deserialize_any(StrictJsonVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}

/// 业务作用：遍历 JSON 标量与容器，同时拒绝重复键、过深结构和超量节点。
struct StrictJsonVisitor<'a> {
    budget: &'a mut JsonBudget,
    depth: usize,
}

impl<'de> Visitor<'de> for StrictJsonVisitor<'_> {
    type Value = ();

    /// 业务作用：描述严格 JSON 验证器接受的输入类别。
    ///
    /// 参数说明：`formatter` 为 serde 错误消息目标。
    ///
    /// 返回：写入固定描述后的格式化结果。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("满足 RedisJob 安全合同的 JSON 值")
    }

    /// 业务作用：接受 JSON 布尔标量，节点预算已在创建访问器时计入。
    ///
    /// 参数说明：`_value` 为布尔值，校验过程不保留业务内容。
    ///
    /// 返回：标量类型合法时成功。
    fn visit_bool<E: de::Error>(self, _value: bool) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON 有符号整数标量，节点预算已在创建访问器时计入。
    ///
    /// 参数说明：`_value` 为整数值，校验过程不保留业务内容。
    ///
    /// 返回：标量类型合法时成功。
    fn visit_i64<E: de::Error>(self, _value: i64) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON 无符号整数标量，节点预算已在创建访问器时计入。
    ///
    /// 参数说明：`_value` 为整数值，校验过程不保留业务内容。
    ///
    /// 返回：标量类型合法时成功。
    fn visit_u64<E: de::Error>(self, _value: u64) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON 浮点标量，具体有限值合同继续由目标业务类型承担。
    ///
    /// 参数说明：`_value` 为浮点值，校验过程不保留业务内容。
    ///
    /// 返回：serde_json 接受该数值时成功。
    fn visit_f64<E: de::Error>(self, _value: f64) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受借用字符串标量，不复制或记录可能包含敏感信息的正文。
    ///
    /// 参数说明：`_value` 为当前字符串切片。
    ///
    /// 返回：字符串满足 JSON UTF-8 合同时成功。
    fn visit_str<E: de::Error>(self, _value: &str) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受拥有所有权的字符串标量，不记录可能包含敏感信息的正文。
    ///
    /// 参数说明：`_value` 为当前字符串。
    ///
    /// 返回：字符串满足 JSON UTF-8 合同时成功。
    fn visit_string<E: de::Error>(self, _value: String) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 serde 对缺省值的表示，供通用访问协议保持封闭语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：缺省标量不引入额外结构时成功。
    fn visit_none<E: de::Error>(self) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON `null` 标量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：空值类型合法时成功。
    fn visit_unit<E: de::Error>(self) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：递归校验 serde 可选值中的实际节点，并沿用同一总量预算。
    ///
    /// 参数说明：`deserializer` 为可选值内部节点解析器。
    ///
    /// 返回：内部节点满足深度、总量与对象键合同时成功。
    fn visit_some<D>(self, deserializer: D) -> core::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StrictJsonSeed {
            budget: self.budget,
            depth: self.depth.saturating_add(1),
        }
        .deserialize(deserializer)
    }

    /// 业务作用：按顺序递归校验数组元素，使深度与节点预算覆盖整个持久载荷。
    ///
    /// 参数说明：`sequence` 为当前 JSON 数组访问器。
    ///
    /// 返回：全部元素满足结构安全合同时成功；任一元素越界或非法时立即停止。
    fn visit_seq<A>(self, mut sequence: A) -> core::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence
            .next_element_seed(StrictJsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
            })?
            .is_some()
        {}
        Ok(())
    }

    /// 业务作用：递归校验对象键值，并拒绝会造成语义覆盖或动态类型装载的键。
    ///
    /// 参数说明：`map` 为当前 JSON 对象访问器。
    ///
    /// 返回：对象键唯一、未包含动态类型元数据且全部值满足结构安全合同时成功。
    fn visit_map<A>(self, mut map: A) -> core::result::Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            self.budget.consume::<A::Error>()?;
            if key == "@class" || key == "@type" {
                return Err(A::Error::custom("JSON 包含禁止的动态类型元数据"));
            }
            if !keys.insert(key) {
                return Err(A::Error::custom("JSON 对象包含重复键"));
            }
            map.next_value_seed(StrictJsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
            })?;
        }
        Ok(())
    }
}

/// 多编码业务参数的显式解码合同；业务类型按线编码自行分派，框架不猜测类型或 serializer。
pub trait JobParameter: Sized {
    /// 业务作用：把已通过定义合同门禁的原始 payload 解码为业务类型。
    ///
    /// 参数说明：
    /// - `codec`: 当前 Run 持久声明的线编码。
    /// - `bytes`: Base64 外层解开后的原始参数字节。
    ///
    /// 返回：解码成功返回业务值；格式、Schema 语义或编码不支持时返回不含敏感 payload 的稳定摘要。
    fn decode(codec: JobWireCodec, bytes: &[u8]) -> core::result::Result<Self, String>;
}

/// 业务 Run 参数载荷；字节按 `codec` 解释，`schema_id` 声明其契约版本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobPayload {
    bytes: Vec<u8>,
    codec: JobWireCodec,
    schema_id: String,
}

impl JobPayload {
    /// 业务作用：以显式字节、线编码与 schema 标识构造载荷；Protobuf/RAW 原样透传字节。
    ///
    /// 参数说明：
    /// - `bytes`: 载荷原始字节。
    /// - `codec`: 线编码。
    /// - `schema_id`: 契约版本标识，必须与已登记 Worker 的 schema 一致。
    ///
    /// 返回：不做内容解释的载荷。
    pub fn new(
        bytes: impl Into<Vec<u8>>,
        codec: JobWireCodec,
        schema_id: impl Into<String>,
    ) -> Self {
        Self {
            bytes: bytes.into(),
            codec,
            schema_id: schema_id.into(),
        }
    }

    /// 业务作用：返回载荷字节。参数说明: 无。返回：原始字节切片。
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// 业务作用：返回线编码。参数说明: 无。返回：线编码。
    pub fn codec(&self) -> JobWireCodec {
        self.codec
    }
    /// 业务作用：返回 schema 标识。参数说明: 无。返回：契约版本标识。
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }
}
