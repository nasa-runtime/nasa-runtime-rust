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

/// 根节点深度为零，子节点累计后的最大递归深度。
const MAX_JSON_DEPTH: usize = 64;
/// 单个载荷的节点预算；容器、标量和对象键分别计数。
const MAX_JSON_NODES: usize = 100_000;

/// 业务作用：严格校验 Job JSON 载荷，递归拒绝重复键、动态类型元数据和资源消耗失控的结构。
///
/// 参数说明：
/// - `bytes`：已解开 Base64 外层的 JSON 原始字节，不包含线路封装。
///
/// 返回：UTF-8、语法、深度、节点数和对象键合同全部成立时成功；否则返回不包含载荷内容的摘要。
pub fn validate_json_payload(bytes: &[u8]) -> core::result::Result<(), String> {
    let mut budget = JsonBudget {
        nodes: 0,
    };
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
/// 参数说明：
/// - `bytes`：当前 Run 的 JSON 原始字节，目标类型由调用方显式指定而不从内容推断。
///
/// 返回：安全结构与业务反序列化均成功时返回值；结构拒绝保留固定原因，类型解码失败只返回错误类别及行列位置。
pub fn decode_json_payload<T: DeserializeOwned>(bytes: &[u8]) -> core::result::Result<T, String> {
    validate_json_payload(bytes)?;
    serde_json::from_slice(bytes).map_err(|error| {
        // 业务 Deserialize 可以把任意输入放进错误正文，不能将该正文送入任务结果或运行日志。
        let category = match error.classify() {
            serde_json::error::Category::Io => "IO",
            serde_json::error::Category::Syntax => "SYNTAX",
            serde_json::error::Category::Data => "DATA",
            serde_json::error::Category::Eof => "EOF",
        };
        format!(
            "JSON 业务类型解码失败：{category}，行 {}，列 {}",
            error.line(),
            error.column()
        )
    })
}

/// 业务作用：累计单个 RedisJob JSON 载荷的解析节点，限制结构复杂度消耗。
struct JsonBudget {
    /// 当前载荷已访问的节点与对象键总数，达到上界后拒绝继续。
    nodes: usize,
}

impl JsonBudget {
    /// 业务作用：累计 JSON 节点并在上限前停止解析，避免小字节深层或碎片结构长期占用执行器。
    ///
    /// 参数说明：无。
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
    /// 整个载荷共享的节点预算，避免每层容器重新获得额度。
    budget: &'a mut JsonBudget,
    /// 当前节点相对根的递归深度。
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for StrictJsonSeed<'_> {
    type Value = ();

    /// 业务作用：为当前 JSON 节点建立递归访问器，并在进入容器前执行深度与总量门禁。
    ///
    /// 参数说明：
    /// - `deserializer`：当前节点解析器，只在深度与共享预算通过后进入实际访问。
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
    /// 下一层访问继续使用的载荷总预算。
    budget: &'a mut JsonBudget,
    /// 当前节点深度，进入子节点时递增。
    depth: usize,
}

impl<'de> Visitor<'de> for StrictJsonVisitor<'_> {
    type Value = ();

    /// 业务作用：描述严格 JSON 验证器接受的输入类别。
    ///
    /// 参数说明：
    /// - `formatter`：serde 错误消息的输出目标，仅写入固定输入类别描述。
    ///
    /// 返回：写入固定描述后的格式化结果。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("满足 RedisJob 安全合同的 JSON 值")
    }

    /// 业务作用：接受 JSON 布尔标量，节点预算已在创建访问器时计入。
    ///
    /// 参数说明：
    /// - `_value`：已解析的布尔值，结构校验不保留其内容。
    ///
    /// 返回：标量类型合法时成功。
    fn visit_bool<E: de::Error>(self, _value: bool) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON 有符号整数标量，节点预算已在创建访问器时计入。
    ///
    /// 参数说明：
    /// - `_value`：已解析的有符号整数，不在结构校验阶段施加业务数值范围。
    ///
    /// 返回：标量类型合法时成功。
    fn visit_i64<E: de::Error>(self, _value: i64) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON 无符号整数标量，节点预算已在创建访问器时计入。
    ///
    /// 参数说明：
    /// - `_value`：已解析的无符号整数，业务类型的数值边界仍由后续解码负责。
    ///
    /// 返回：标量类型合法时成功。
    fn visit_u64<E: de::Error>(self, _value: u64) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受 JSON 浮点标量，具体有限值合同继续由目标业务类型承担。
    ///
    /// 参数说明：
    /// - `_value`：解析器接受的浮点数，结构校验不保留该值。
    ///
    /// 返回：serde_json 接受该数值时成功。
    fn visit_f64<E: de::Error>(self, _value: f64) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受借用字符串标量，不复制或记录可能包含敏感信息的正文。
    ///
    /// 参数说明：
    /// - `_value`：当前已解析的字符串切片，不记录正文或产生业务对象。
    ///
    /// 返回：字符串满足 JSON UTF-8 合同时成功。
    fn visit_str<E: de::Error>(self, _value: &str) -> core::result::Result<(), E> {
        Ok(())
    }

    /// 业务作用：接受拥有所有权的字符串标量，不记录可能包含敏感信息的正文。
    ///
    /// 参数说明：
    /// - `_value`：解析器移交的字符串，验证结束后释放，不写入日志。
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
    /// 参数说明：
    /// - `deserializer`：可选值中实际节点的解析器，沿用同一总预算并增加递归深度。
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
    /// 参数说明：
    /// - `sequence`：当前数组的顺序访问器，逐项校验，不创建业务元素集合。
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
    /// 参数说明：
    /// - `map`：当前对象的键值访问器；键暂存于本层集合以检测重复，值递归消耗共享预算。
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
    /// 未加 Base64 外层的业务字节，由声明的 codec 解释。
    bytes: Vec<u8>,
    /// 显式声明的线编码，不从载荷内容推断。
    codec: JobWireCodec,
    /// 业务 Schema 身份，提交或执行时需与 Worker 合同一致。
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

    /// 业务作用：借用业务原始载荷，供合同校验和线路编码使用，避免为读取复制正文。
    ///
    /// 参数说明：无。
    ///
    /// 返回：未包裹 Base64 的原始字节切片，不执行反序列化。
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// 业务作用：读取调用方声明的线编码，使提交与执行都使用同一解码合同。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造时保存的编码枚举，不检测原始字节是否符合该编码。
    pub fn codec(&self) -> JobWireCodec {
        self.codec
    }
    /// 业务作用：读取载荷声明的 Schema 身份，供 Worker 合同门禁精确比对。
    ///
    /// 参数说明：无。
    ///
    /// 返回：构造时保存的标识借用，构造与读取本身不证明其已通过校验。
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }
}
