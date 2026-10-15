use std::{borrow::Cow, collections::BTreeMap, fmt};

use granit_parser::{Event, Parser, ScalarStyle, Span, StrInput, Tag};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use yaml_rust2::Yaml;

use super::{ConfigError, ConfigPath, ErrorKind, LoadPolicy, Result, SourcePosition};
use crate::ConfigFormat;

/// 调用方取得的原始文档，名称不会被自动当作文件路径打开。
#[derive(Clone)]
pub struct SourceDocument {
    pub(crate) name: String,
    pub(crate) format: ConfigFormat,
    pub(crate) bytes: Vec<u8>,
}

/// 一份文档只解析一次，后续合并复用该结果。
#[derive(Clone)]
pub struct ParsedDocument {
    pub(crate) value: Value,
    pub(crate) source_id: usize,
    pub(crate) digest: [u8; 32],
    pub(crate) bytes: usize,
    pub(crate) parsed_nodes: usize,
    pub(crate) parsed_text: usize,
}

impl SourceDocument {
    /// 业务作用：封装已经读取的文档，避免装配阶段再次打开来源。
    /// 参数说明：`name` 是调用方身份；`format` 固定格式；`bytes` 为完整输入。
    /// 返回：尚未解析的独立文档。
    pub fn new(name: impl Into<String>, format: ConfigFormat, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            format,
            bytes: bytes.into(),
        }
    }

    /// 业务作用：按严格文档子集解析，丢弃包含正文的后端错误。
    /// 参数说明：`source_id` 是本轮编号；`policy` 指定解析及取消预算。
    /// 返回：完整映射文档；编码、重复键、格式或预算失败则拒绝。
    pub fn parse(&self, source_id: usize, policy: &LoadPolicy) -> Result<ParsedDocument> {
        self.parse_with_prior(source_id, policy, 0, 0)
    }

    /// 业务作用：在一轮装配已消耗的预算上解析下一来源，覆盖旧字段不能退回解析成本。
    /// 参数说明：`source_id` 为本轮来源编号；`policy` 为固定策略；`prior_nodes/prior_text` 为此前累计消耗。
    /// 返回：完整映射及本来源实际新增消耗，超限时不构造后续节点或返回部分文档。
    pub(super) fn parse_with_prior(
        &self,
        source_id: usize,
        policy: &LoadPolicy,
        prior_nodes: usize,
        prior_text: usize,
    ) -> Result<ParsedDocument> {
        let parse = || {
            policy.check_cancelled()?;
            if self.name.len() > 4096 || self.bytes.len() > policy.limits.source_bytes {
                return Err(ConfigError::new(ErrorKind::Limit));
            }
            let bytes = self
                .bytes
                .strip_prefix(b"\xef\xbb\xbf")
                .unwrap_or(&self.bytes);
            let text =
                std::str::from_utf8(bytes).map_err(|_| ConfigError::new(ErrorKind::Encoding))?;
            if text.trim().is_empty() {
                return Err(ConfigError::new(ErrorKind::Format));
            }
            let mut budget = ParseBudget {
                policy,
                nodes: prior_nodes,
                text_bytes: prior_text,
                failure: None,
                configuration_keys: true,
            };
            let value = match self.format {
                ConfigFormat::Yaml => parse_yaml(text, &mut budget)?,
                ConfigFormat::Json => {
                    let mut decoder = serde_json::Deserializer::from_str(text);
                    let result = Seed {
                        budget: &mut budget,
                        path: ConfigPath::default(),
                        json_source: Some(text),
                    }
                    .deserialize(&mut decoder);
                    let value = result.map_err(|error| {
                        // 分片语义错误已换算到来源坐标，不能再用外层 decoder 的位置覆盖。
                        budget.failure.take().unwrap_or_else(|| {
                            ConfigError::new(ErrorKind::Format)
                                .positioned(error.line(), error.column())
                        })
                    })?;
                    decoder.end().map_err(|error| {
                        ConfigError::new(ErrorKind::Format).positioned(error.line(), error.column())
                    })?;
                    value
                }
                ConfigFormat::Toml => {
                    let decoder = toml::de::Deserializer::parse(text).map_err(|error| {
                        text_error(ConfigError::new(ErrorKind::Format), text, error.span())
                    })?;
                    Seed {
                        budget: &mut budget,
                        path: ConfigPath::default(),
                        json_source: None,
                    }
                    .deserialize(decoder)
                    .map_err(|error| {
                        text_error(
                            budget
                                .failure
                                .clone()
                                .unwrap_or_else(|| ConfigError::new(ErrorKind::Format)),
                            text,
                            error.span(),
                        )
                    })?
                }
            };
            if !value.is_object() {
                return Err(ConfigError::new(ErrorKind::Format));
            }
            Ok(ParsedDocument {
                value,
                source_id,
                digest: Sha256::digest(&self.bytes).into(),
                bytes: self.bytes.len(),
                parsed_nodes: budget.nodes - prior_nodes,
                parsed_text: budget.text_bytes - prior_text,
            })
        };
        parse().map_err(|error| error.in_source(source_id))
    }
}

impl ParsedDocument {
    /// 业务作用：只读借用已解析树供调用方执行受信来源约束。
    /// 参数说明：无。
    /// 返回：包含原值的树引用，调用方不得直接写入日志。
    pub fn tree(&self) -> &Value {
        &self.value
    }
}

impl fmt::Debug for SourceDocument {
    /// 业务作用：避免文档的默认格式化输出正文和来源名。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：格式和字节规模。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceDocument")
            .field("format", &self.format)
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ParsedDocument {
    /// 业务作用：已解析文档不在默认诊断中暴露值或摘要。
    /// 参数说明：`f` 为格式化目标。
    /// 返回：数值来源身份和输入规模。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParsedDocument")
            .field("source_id", &self.source_id)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

struct ParseBudget<'a> {
    policy: &'a LoadPolicy,
    nodes: usize,
    text_bytes: usize,
    failure: Option<ConfigError>,
    configuration_keys: bool,
}

/// 业务作用：复用有界 YAML 解析器读取外部文档，不把原始键解释为配置路径。
/// 参数说明：`bytes` 为完整正文；`policy` 仅提供解析预算与取消状态。
/// 返回：唯一文档的原始值；编码、重复键、标签、别名或预算非法时拒绝，错误不含正文。
pub(crate) fn parse_yaml_value(bytes: &[u8], policy: &LoadPolicy) -> Result<Value> {
    policy.check_cancelled()?;
    if bytes.len() > policy.limits.source_bytes {
        return Err(ConfigError::new(ErrorKind::Limit));
    }
    let text = std::str::from_utf8(bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes))
        .map_err(|_| ConfigError::new(ErrorKind::Encoding))?;
    // 空文档由 YAML 事件判定；Unicode 空白可能是合法标量内容，不能提前按通用 trim 丢弃。
    let mut budget = ParseBudget {
        policy,
        nodes: 0,
        text_bytes: 0,
        failure: None,
        configuration_keys: false,
    };
    parse_yaml(text, &mut budget)
}

impl ParseBudget<'_> {
    /// 业务作用：在分配下一个树节点之前检查深度和总量。
    /// 参数说明：`path` 是即将构建的节点路径。
    /// 返回：预算和取消权威均允许时成功。
    fn node(&mut self, path: &ConfigPath) -> Result<()> {
        self.policy.check_cancelled()?;
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit))?;
        if path.0.len() > self.policy.limits.depth || self.nodes > self.policy.limits.nodes {
            return Err(ConfigError::new(ErrorKind::Limit).at(path));
        }
        Ok(())
    }

    /// 业务作用：累计解析文本及锚点副本的标量表示，限制别名展开的总规模。
    /// 参数说明：`bytes` 为本次文本字节数；`path` 是字段位置。
    /// 返回：累计规模在预算内时成功，超限或计数溢出时拒绝继续构建。
    fn text(&mut self, bytes: usize, path: &ConfigPath) -> Result<()> {
        self.text_bytes = self
            .text_bytes
            .checked_add(bytes)
            .ok_or_else(|| ConfigError::new(ErrorKind::Limit).at(path))?;
        if self.text_bytes > self.policy.limits.output_bytes {
            Err(ConfigError::new(ErrorKind::Limit).at(path))
        } else {
            Ok(())
        }
    }

    /// 业务作用：限制单个字符串或键的大小，不将非字符串标量套用文本单值限额。
    /// 参数说明：`value` 为已确认的字符串或键；`path` 是字段位置。
    /// 返回：单值长度与累计文本均在预算内时成功。
    fn string(&mut self, value: &str, path: &ConfigPath) -> Result<()> {
        if value.len() > self.policy.limits.string_bytes {
            return Err(ConfigError::new(ErrorKind::Limit).at(path));
        }
        self.text(value.len(), path)
    }

    /// 业务作用：让通用 Serde 后端传播安全分类而不携带原值。
    /// 参数说明：`error` 是已分类的拒绝原因。
    /// 返回：后端需要的错误载体。
    fn fail<E: serde::de::Error>(&mut self, error: ConfigError) -> E {
        self.failure = Some(error);
        E::custom("configuration rejected")
    }

    /// 业务作用：保存锚点或展开别名之前计入全部新节点、键与标量文本。
    /// 参数说明：`value` 是将要复制的子树；`path` 是目标位置。
    /// 返回：整棵子树均在预算内时成功。
    fn copy(&mut self, value: &Value, path: &ConfigPath) -> Result<()> {
        self.node(path)?;
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    self.string(key, path)?;
                    self.copy(value, &path.key(key))?;
                }
            }
            Value::Array(list) => {
                for (index, value) in list.iter().enumerate() {
                    self.copy(value, &path.index(index))?;
                }
            }
            Value::String(text) => self.string(text, path)?,
            Value::Number(number) => self.text(number.to_string().len(), path)?,
            Value::Bool(true) | Value::Null => self.text(4, path)?,
            Value::Bool(false) => self.text(5, path)?,
        }
        Ok(())
    }
}

struct Seed<'a, 'p, 's> {
    budget: &'a mut ParseBudget<'p>,
    path: ConfigPath,
    json_source: Option<&'s str>,
}

impl Seed<'_, '_, '_> {
    /// 业务作用：为不提供词法 token 的后端累计原生标量文本，避免数值来源绕过装配预算。
    /// 参数说明：`bytes` 为标量规范化表示长度；JSON 已按原文计费，不重复累计。
    /// 返回：累计预算允许时成功，超限时保存脱敏拒绝原因。
    fn scalar_text<E: serde::de::Error>(&mut self, bytes: usize) -> std::result::Result<(), E> {
        if self.json_source.is_none() {
            if let Err(error) = self.budget.text(bytes, &self.path) {
                return Err(self.budget.fail(error));
            }
        }
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for Seed<'_, '_, '_> {
    type Value = Value;
    /// 业务作用：在递归入口施加树预算，并依据 JSON 原始语法保持数字与业务映射的类型区别。
    /// 参数说明：`deserializer` 是已选择格式的解码器。
    /// 返回：单节点及其完整子树，失败只保留安全原因。
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Value, D::Error> {
        if let Some(source) = self.json_source {
            // 借用已校验的 token，不先构造通用 Value，避免下游 feature 的私有数字映射进入业务树。
            let raw = <&serde_json::value::RawValue>::deserialize(deserializer)?;
            let text = raw.get();
            // 先取得借用片段的位置，再检查树预算；检查通过前不构造对应业务节点。
            if let Err(error) = self.budget.node(&self.path) {
                return Err(self.budget.fail(json_position(error, source, text, None)));
            }
            if matches!(text.as_bytes().first(), Some(b'-' | b'0'..=b'9')) {
                if let Err(error) = self.budget.text(text.len(), &self.path) {
                    return Err(self.budget.fail(json_position(error, source, text, None)));
                }
                return json_number(text).map_err(|error| {
                    self.budget
                        .fail(json_position(error.at(&self.path), source, text, None))
                });
            }
            if matches!(text, "true" | "false" | "null") {
                if let Err(error) = self.budget.text(text.len(), &self.path) {
                    return Err(self.budget.fail(json_position(error, source, text, None)));
                }
            }
            // 容器逐项复用本 Seed；真实对象即使含有 Serde 私有字段名也保持为映射。
            let result = serde_json::Deserializer::from_str(text).deserialize_any(Seed {
                budget: self.budget,
                path: self.path,
                json_source: Some(source),
            });
            return result.map_err(|error| {
                let safe = self
                    .budget
                    .failure
                    .take()
                    .unwrap_or_else(|| ConfigError::new(ErrorKind::Format));
                let local = (error.line() > 0 && error.column() > 0).then_some(SourcePosition {
                    line: error.line(),
                    column: error.column(),
                });
                self.budget.fail(json_position(safe, source, text, local))
            });
        }
        if let Err(error) = self.budget.node(&self.path) {
            return Err(self.budget.fail(error));
        }
        deserializer.deserialize_any(self)
    }
}

/// 业务作用：把借用 JSON 片段的错误换算回完整来源，不输出局部坐标或正文。
/// 参数说明：`error` 为脱敏错误；`source` 为完整来源；`fragment` 为其借用片段；`local` 为片段内位置，缺失时定位片段首字符。
/// 返回：保留已有全局位置，或附加从一开始的来源行号与 UTF-8 字节列；片段不属于来源时不伪造位置。
fn json_position(
    error: ConfigError,
    source: &str,
    fragment: &str,
    local: Option<SourcePosition>,
) -> ConfigError {
    if error.position.is_some() {
        return error;
    }
    let Some(offset) = (fragment.as_ptr() as usize)
        .checked_sub(source.as_ptr() as usize)
        .filter(|offset| {
            fragment.len() <= source.len().saturating_sub(*offset) && *offset <= source.len()
        })
    else {
        return error;
    };
    let positioned = text_error(error, source, Some(offset..offset));
    let start = positioned.position.expect("source offset has a position");
    match local {
        Some(local) => positioned.positioned(
            start.line + local.line - 1,
            if local.line == 1 {
                start.column + local.column - 1
            } else {
                local.column
            },
        ),
        None => positioned,
    }
}

/// 业务作用：按固定数值范围解释已校验的 JSON 数字，不受下游 Serde 数字表示影响。
/// 参数说明：`text` 是一份完整且语法合法的 JSON 数值 token。
/// 返回：i64/u64 整数或有限 f64，保留负零；整数越界或浮点非有限时返回脱敏格式错误。
fn json_number(text: &str) -> Result<Value> {
    if text == "-0" || text.contains(['.', 'e', 'E']) {
        return text
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| ConfigError::new(ErrorKind::Format));
    }
    parse_integer(text)
}

impl<'de> Visitor<'de> for Seed<'_, '_, '_> {
    type Value = Value;
    /// 业务作用：提供不含输入内容的格式期待。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：通用配置节点描述。
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configuration value")
    }
    /// 业务作用：保留 null 与缺失节点的区别。
    /// 参数说明：无。
    /// 返回：预算内的显式 null，累计超限时拒绝。
    fn visit_unit<E: serde::de::Error>(mut self) -> std::result::Result<Value, E> {
        self.scalar_text(4)?;
        Ok(Value::Null)
    }
    /// 业务作用：保持文档布尔值的原始类型。
    /// 参数说明：`value` 是文档值。
    /// 返回：预算内的布尔节点，累计超限时拒绝。
    fn visit_bool<E: serde::de::Error>(mut self, value: bool) -> std::result::Result<Value, E> {
        self.scalar_text(value.to_string().len())?;
        Ok(value.into())
    }
    /// 业务作用：保持有符号整数精度。
    /// 参数说明：`value` 是文档值。
    /// 返回：预算内的整数节点，累计超限时拒绝。
    fn visit_i64<E: serde::de::Error>(mut self, value: i64) -> std::result::Result<Value, E> {
        self.scalar_text(value.to_string().len())?;
        Ok(value.into())
    }
    /// 业务作用：保持无符号整数精度。
    /// 参数说明：`value` 是文档值。
    /// 返回：预算内的整数节点，累计超限时拒绝。
    fn visit_u64<E: serde::de::Error>(mut self, value: u64) -> std::result::Result<Value, E> {
        self.scalar_text(value.to_string().len())?;
        Ok(value.into())
    }
    /// 业务作用：拒绝不能由配置树表示的非有限数。
    /// 参数说明：`value` 是文档浮点值。
    /// 返回：预算内的有限数字；非有限值或累计超限时拒绝。
    fn visit_f64<E: serde::de::Error>(mut self, value: f64) -> std::result::Result<Value, E> {
        let number = Number::from_f64(value).ok_or_else(|| {
            self.budget
                .fail(ConfigError::new(ErrorKind::Format).at(&self.path))
        })?;
        self.scalar_text(number.to_string().len())?;
        Ok(Value::Number(number))
    }
    /// 业务作用：有界接收文本并保持其原始内容。
    /// 参数说明：`value` 是文档文本。
    /// 返回：文本节点或预算错误。
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Value, E> {
        if let Err(error) = self.budget.string(value, &self.path) {
            return Err(self.budget.fail(error));
        }
        Ok(value.into())
    }
    /// 业务作用：复用文本限额处理已取得所有权的文本。
    /// 参数说明：`value` 是文档文本。
    /// 返回：不再克隆的文本节点。
    fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<Value, E> {
        if let Err(error) = self.budget.string(&value, &self.path) {
            return Err(self.budget.fail(error));
        }
        Ok(Value::String(value))
    }
    /// 业务作用：逐元素构建数组，不按不可信 size_hint 预分配。
    /// 参数说明：`sequence` 是数组访问者。
    /// 返回：预算内的完整数组。
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(Seed {
            budget: self.budget,
            path: self.path.index(values.len()),
            json_source: self.json_source,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    /// 业务作用：在重复键信息丢失前拒绝同源冲突。
    /// 参数说明：`mapping` 是映射访问者。
    /// 返回：键唯一的映射，不允许后值悄悄吞掉重复声明。
    fn visit_map<A: MapAccess<'de>>(self, mut mapping: A) -> std::result::Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = mapping.next_key::<String>()? {
            if let Err(error) =
                validate_key(&key, &self.path).and_then(|_| self.budget.string(&key, &self.path))
            {
                return Err(self.budget.fail(error));
            }
            let path = self.path.key(&key);
            if values.contains_key(&key) {
                return Err(self
                    .budget
                    .fail(ConfigError::new(ErrorKind::Duplicate).at(&path)));
            }
            let value = mapping.next_value_seed(Seed {
                budget: self.budget,
                path,
                json_source: self.json_source,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

/// 业务作用：拒绝会与字段路径简写发生歧义的文档键。
/// 参数说明：`key` 是原始键；`parent` 是父路径。
/// 返回：可无歧义引用的键，严格入口不隐式展开字面点号。
fn validate_key(key: &str, parent: &ConfigPath) -> Result<()> {
    if key.is_empty()
        || key.len() > 4096
        || key.contains(['.', '[', ']'])
        || key.chars().any(char::is_control)
    {
        Err(ConfigError::new(ErrorKind::PathConflict).at(parent))
    } else {
        Ok(())
    }
}

/// 业务作用：按事件逐节点解析 YAML，避免先构建无约束的别名树。
/// 参数说明：`text` 是 UTF-8 文档；`budget` 共享节点和深度预算。
/// 返回：唯一 YAML 文档的树；非法或重复指令、未知标签和超限输入均拒绝。
fn parse_yaml(text: &str, budget: &mut ParseBudget<'_>) -> Result<Value> {
    // 指令在文档内累积并检查重复，不能因后续声明丢失已有的默认 handle 重定义。
    // 词法缓冲受已校验的来源大小约束，树深度仍在节点构造前按调用方预算检查。
    let options = granit_parser::options! {
        emit_comments: false,
        strict_indentation: true,
        max_directive_bytes: text.len(),
        max_reserved_directive_params: text.len(),
        flow_nesting_limit: budget.policy.limits.depth.saturating_add(2),
        block_nesting_limit: budget.policy.limits.depth.saturating_add(2),
    };
    let mut parser = YamlReader {
        parser: Parser::new_from_str_with_options(text, options),
        source: text,
        position: SourcePosition { line: 1, column: 1 },
    };
    let result = (|| {
        let mut root = None;
        let mut documents = 0;
        let mut anchors = BTreeMap::new();
        loop {
            let event = next_event(&mut parser)?;
            match event {
                Event::StreamStart | Event::DocumentEnd => {}
                Event::DocumentStart(..) => {
                    documents += 1;
                    if documents > 1 {
                        return Err(ConfigError::new(ErrorKind::Format));
                    }
                }
                Event::StreamEnd => break,
                event => {
                    if root.is_some() {
                        return Err(ConfigError::new(ErrorKind::Format));
                    }
                    root = Some(yaml_node(
                        &mut parser,
                        event,
                        budget,
                        &ConfigPath::default(),
                        &mut anchors,
                    )?);
                }
            }
        }
        root.ok_or_else(|| ConfigError::new(ErrorKind::Format))
    })();
    result.map_err(|error: ConfigError| {
        if error.position.is_some() {
            error
        } else {
            error.positioned(parser.position.line, parser.position.column)
        }
    })
}

struct YamlReader<'a> {
    parser: Parser<'a, StrInput<'a>>,
    source: &'a str,
    position: SourcePosition,
}

/// 业务作用：使块标量的最后内容行按物理来源收尾，EOF 本身不产生换行字节。
/// 参数说明：`value` 为解码文本；`span` 为块内容的原文范围与起始缩进；`source` 为完整输入。
/// 返回：仅移除没有内容行结束符依据的单个 LF；真实内容换行及已结束的空行保持原值。
fn yaml_block_eof(value: &mut Cow<'_, str>, span: Span, source: &str) {
    if span.end.byte_offset() != Some(source.len()) || source.ends_with(['\r', '\n']) {
        return;
    }
    let Some(content) = value.strip_suffix('\n') else {
        return;
    };
    let last_line = source.rsplit(['\r', '\n']).next().unwrap_or_default();
    if content.is_empty() {
        // 只有头部换行与未结束的缩进空白时，没有可保留的内容行；头部换行不能变成值。
        if span.end.line() == span.start.line().saturating_add(1)
            && last_line.bytes().all(|byte| byte == b' ')
        {
            value.to_mut().pop();
        }
        return;
    }
    if content.ends_with('\n') {
        return;
    }
    let indent = span.start.col();
    // 只有缩进后的实际内容抵达 EOF 才能移除虚拟 LF；仅剩缩进时，前一行的真实 LF 必须保留。
    if last_line.len() > indent
        && last_line.as_bytes()[..indent]
            .iter()
            .all(|byte| *byte == b' ')
    {
        value.to_mut().pop();
    }
}

/// 业务作用：屏蔽 YAML 后端可能携带原文的错误。
/// 参数说明：`parser` 为流式解析状态。
/// 返回：下一个语法事件；后端嵌套上限映射为 Limit，其余解析失败只保留格式类别与位置。
fn next_event<'a>(parser: &mut YamlReader<'a>) -> Result<Event<'a>> {
    parser
        .parser
        .next_event()
        .ok_or_else(|| ConfigError::new(ErrorKind::Format))?
        .map(|(mut event, span)| {
            parser.position = SourcePosition {
                line: span.start.line(),
                column: span.start.col() + 1,
            };
            // 收尾语义先于文本计费、类型判定和锚点保存，确保绑定值与别名副本使用相同字节。
            if let Event::Scalar(value, ScalarStyle::Literal | ScalarStyle::Folded, ..) = &mut event
            {
                yaml_block_eof(value, span, parser.source);
            }
            event
        })
        .map_err(|error| {
            let kind = if matches!(
                error.kind(),
                granit_parser::ErrorKind::RecursionLimitExceeded
            ) {
                ErrorKind::Limit
            } else {
                ErrorKind::Format
            };
            ConfigError::new(kind).positioned(error.marker().line(), error.marker().col() + 1)
        })
}

/// 业务作用：有界构建 YAML 子树并在别名复制前计费。
/// 参数说明：`parser` 提供事件；`event` 是首事件；`budget` 是共享限额；`path` 为节点；`anchors` 仅保留已完整定义的锚点。
/// 返回：完整子树，不允许自循环锚点和未知标签。
fn yaml_node(
    parser: &mut YamlReader<'_>,
    event: Event<'_>,
    budget: &mut ParseBudget<'_>,
    path: &ConfigPath,
    anchors: &mut BTreeMap<usize, Value>,
) -> Result<Value> {
    budget.node(path)?;
    let (value, anchor) = match event {
        Event::Scalar(text, style, anchor, tag) => {
            // 词法文本先计累计预算，类型判定后仅对字符串施加单值限制。
            budget.text(text.len(), path)?;
            let value = yaml_scalar(&text, style, tag.as_deref())?;
            if value.is_string() && text.len() > budget.policy.limits.string_bytes {
                return Err(ConfigError::new(ErrorKind::Limit).at(path));
            }
            (value, anchor)
        }
        Event::MappingStart(_, anchor, tag) => {
            // 仅允许与节点类型一致的标准注解；未知 URI 不能借映射结构进入配置树。
            if tag
                .as_ref()
                .is_some_and(|tag| yaml_tag_name(tag) != Some("map"))
            {
                return Err(ConfigError::new(ErrorKind::Unsupported).at(path));
            }
            let mut map = Map::new();
            loop {
                let event = next_event(parser)?;
                if event == Event::MappingEnd {
                    break;
                }
                let Event::Scalar(text, style, key_anchor, tag) = event else {
                    return Err(ConfigError::new(ErrorKind::Format).at(path));
                };
                // 标准 str 明确声明字面字段；未知命名空间的同名标签不能跳过合并键门禁。
                let explicit_string = tag
                    .as_ref()
                    .is_some_and(|tag| yaml_tag_name(tag) == Some("str"));
                if key_anchor != 0
                    || (text == "<<" && style == ScalarStyle::Plain && !explicit_string)
                {
                    return Err(ConfigError::new(ErrorKind::Unsupported).at(path));
                }
                let Value::String(key) = yaml_scalar(&text, style, tag.as_deref())? else {
                    return Err(ConfigError::new(ErrorKind::Format).at(path));
                };
                // 配置路径需要拒绝歧义键；外部 YAML 文档则保留协议定义的原始字符串键。
                if budget.configuration_keys {
                    validate_key(&key, path)?;
                }
                budget.string(&key, path)?;
                let child = path.key(&key);
                if map.contains_key(&key) {
                    return Err(ConfigError::new(ErrorKind::Duplicate).at(&child));
                }
                let event = next_event(parser)?;
                map.insert(key, yaml_node(parser, event, budget, &child, anchors)?);
            }
            (Value::Object(map), anchor)
        }
        Event::SequenceStart(_, anchor, tag) => {
            // 类型注解不改变数组的解析与预算路径，也不能把不匹配标签当作无标签处理。
            if tag
                .as_ref()
                .is_some_and(|tag| yaml_tag_name(tag) != Some("seq"))
            {
                return Err(ConfigError::new(ErrorKind::Unsupported).at(path));
            }
            let mut values = Vec::new();
            loop {
                let event = next_event(parser)?;
                if event == Event::SequenceEnd {
                    break;
                }
                values.push(yaml_node(
                    parser,
                    event,
                    budget,
                    &path.index(values.len()),
                    anchors,
                )?);
            }
            (Value::Array(values), anchor)
        }
        Event::Alias(anchor) => {
            let value = anchors
                .get(&anchor)
                .ok_or_else(|| ConfigError::new(ErrorKind::Cycle).at(path))?;
            // 必须先核算整棵副本，数值密集的别名也不能在检查前分配展开树。
            budget.copy(value, path)?;
            (value.clone(), 0)
        }
        _ => return Err(ConfigError::new(ErrorKind::Format).at(path)),
    };
    if anchor != 0 {
        // 锚点表额外保留一份树，保存时同样计费，避免只约束最终返回树。
        budget.copy(&value, path)?;
        anchors.insert(anchor, value.clone());
    }
    Ok(value)
}

/// 业务作用：按完整解析后的 URI 识别允许的 YAML 标准类型，避免 handle 重定义改变类型边界。
/// 参数说明：`tag` 为解析器展开指令后的前缀与后缀，两段共同组成标签身份。
/// 返回：精确匹配的标准类型名；未知 URI 返回 None，不分配完整 URI 副本。
fn yaml_tag_name(tag: &Tag) -> Option<&'static str> {
    ["str", "int", "bool", "null", "float", "map", "seq"]
        .into_iter()
        .find(|name| {
            tag.handle()
                .bytes()
                .chain(tag.suffix().bytes())
                .eq("tag:yaml.org,2002:".bytes().chain(name.bytes()))
        })
}

/// 业务作用：统一显式与隐式 YAML 标量的类型和范围，显式类型标签优先于引号风格。
/// 参数说明：`text` 为文本；`style` 为引号风格；`tag` 为显式标签。
/// 返回：有限标量或格式/不支持错误。
fn yaml_scalar(text: &str, style: ScalarStyle, tag: Option<&Tag>) -> Result<Value> {
    let null = matches!(text, "" | "~" | "null" | "Null" | "NULL");
    let boolean = match text {
        "true" | "True" | "TRUE" => Some(true),
        "false" | "False" | "FALSE" => Some(false),
        _ => None,
    };
    if let Some(tag) = tag {
        return match yaml_tag_name(tag) {
            Some("str") => Ok(text.into()),
            Some("int") => parse_integer(text),
            Some("bool") => boolean
                .map(Value::from)
                .ok_or_else(|| ConfigError::new(ErrorKind::Format)),
            Some("null") => null
                .then_some(Value::Null)
                .ok_or_else(|| ConfigError::new(ErrorKind::Format)),
            Some("float") => text
                .parse::<f64>()
                .ok()
                .and_then(Number::from_f64)
                .map(Value::Number)
                .ok_or_else(|| ConfigError::new(ErrorKind::Format)),
            _ => Err(ConfigError::new(ErrorKind::Unsupported)),
        };
    }
    if style != ScalarStyle::Plain {
        return Ok(text.into());
    }
    if null {
        return Ok(Value::Null);
    }
    if let Some(value) = boolean {
        return Ok(value.into());
    }
    // 先识别完整整数语法再检查范围，越界数值不能回退为字符串或经浮点舍入。
    if integer_lexeme(text).is_some() {
        return parse_integer(text);
    }
    // 空值、布尔与整数已由统一规则处理；后端仅保留浮点识别，不接受其宽松整数转换。
    match Yaml::from_str(text) {
        Yaml::Real(value) => value
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| ConfigError::new(ErrorKind::Format)),
        _ => Ok(text.into()),
    }
}

/// 业务作用：将整数语法与数值范围分开，确保非法普通文本保留字符串、合法越界整数明确拒绝。
/// 参数说明：`text` 是可含一个前置符号的十进制或 0b/0o/0x 基数文本。
/// 返回：完整语法匹配时给出负号、基数和无符号数字片段，否则不认定为整数。
fn integer_lexeme(text: &str) -> Option<(bool, u32, &str)> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(text);
    let (radix, digits) = if let Some(digits) = unsigned.strip_prefix("0x") {
        (16, digits)
    } else if let Some(digits) = unsigned.strip_prefix("0o") {
        (8, digits)
    } else if let Some(digits) = unsigned.strip_prefix("0b") {
        (2, digits)
    } else {
        (10, unsigned)
    };
    (!digits.is_empty() && digits.bytes().all(|byte| char::from(byte).is_digit(radix)))
        .then_some((negative, radix, digits))
}

/// 业务作用：有符号或无符号整数直接进入树，避免经过浮点丢失精度。
/// 参数说明：`text` 是可含前置符号的十进制或 0b/0o/0x 基数整数文本。
/// 返回：负数限于 i64、非负数限于 u64；非法语法或超出范围时拒绝。
fn parse_integer(text: &str) -> Result<Value> {
    let (negative, radix, digits) =
        integer_lexeme(text).ok_or_else(|| ConfigError::new(ErrorKind::Format))?;
    let magnitude =
        u64::from_str_radix(digits, radix).map_err(|_| ConfigError::new(ErrorKind::Format))?;
    if !negative {
        return Ok(magnitude.into());
    }
    // i64 最小值的绝对值超出 i64 正数范围，先比较无符号幅值，避免取负溢出。
    if magnitude == i64::MIN.unsigned_abs() {
        return Ok(i64::MIN.into());
    }
    i64::try_from(magnitude)
        .map(|value| Value::from(-value))
        .map_err(|_| ConfigError::new(ErrorKind::Format))
}

/// 业务作用：将后端字节范围转换为安全行列信息，不保留文档片段。
/// 参数说明：`error` 是稳定类别；`text` 为本轮输入；`span` 为后端提供的字节范围。
/// 返回：可用位置附加到错误，缺失位置时保留原诊断。
fn text_error(error: ConfigError, text: &str, span: Option<std::ops::Range<usize>>) -> ConfigError {
    let Some(span) = span else {
        return error;
    };
    let prefix = &text.as_bytes()[..span.start.min(text.len())];
    let line = prefix.iter().filter(|byte| **byte == b'\n').count() + 1;
    let column = prefix.len()
        - prefix
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1)
        + 1;
    error.positioned(line, column)
}
