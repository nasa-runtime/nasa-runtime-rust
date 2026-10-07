use std::{collections::BTreeMap, fmt};

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use yaml_rust2::{
    parser::{Event, Parser, Tag},
    scanner::TScalarStyle,
    Yaml,
};

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
                nodes: 0,
                text_bytes: 0,
                failure: None,
            };
            let value = match self.format {
                ConfigFormat::Yaml => parse_yaml(text, &mut budget)?,
                ConfigFormat::Json => {
                    let mut decoder = serde_json::Deserializer::from_str(text);
                    let result = Seed {
                        budget: &mut budget,
                        path: ConfigPath::default(),
                    }
                    .deserialize(&mut decoder);
                    let value = result.map_err(|error| {
                        budget
                            .failure
                            .clone()
                            .unwrap_or_else(|| ConfigError::new(ErrorKind::Format))
                            .positioned(error.line(), error.column())
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

    /// 业务作用：限制单个文本节点，避免后续复制超出展开预算。
    /// 参数说明：`value` 是原文本；`path` 是字段位置。
    /// 返回：长度在限制内时成功。
    fn string(&mut self, value: &str, path: &ConfigPath) -> Result<()> {
        self.text_bytes = self.text_bytes.saturating_add(value.len());
        if value.len() > self.policy.limits.string_bytes
            || self.text_bytes > self.policy.limits.output_bytes
        {
            Err(ConfigError::new(ErrorKind::Limit).at(path))
        } else {
            Ok(())
        }
    }

    /// 业务作用：让通用 Serde 后端传播安全分类而不携带原值。
    /// 参数说明：`error` 是已分类的拒绝原因。
    /// 返回：后端需要的错误载体。
    fn fail<E: serde::de::Error>(&mut self, error: ConfigError) -> E {
        self.failure = Some(error);
        E::custom("configuration rejected")
    }

    /// 业务作用：别名复制之前计入全部新节点和文本。
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
            _ => {}
        }
        Ok(())
    }
}

struct Seed<'a, 'p> {
    budget: &'a mut ParseBudget<'p>,
    path: ConfigPath,
}

impl<'de> DeserializeSeed<'de> for Seed<'_, '_> {
    type Value = Value;
    /// 业务作用：在反序列化递归入口统一施加树预算。
    /// 参数说明：`deserializer` 是已选择格式的解码器。
    /// 返回：单节点及其完整子树，失败只保留安全原因。
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Value, D::Error> {
        if let Err(error) = self.budget.node(&self.path) {
            return Err(self.budget.fail(error));
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Seed<'_, '_> {
    type Value = Value;
    /// 业务作用：提供不含输入内容的格式期待。
    /// 参数说明：`f` 是格式化目标。
    /// 返回：通用配置节点描述。
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configuration value")
    }
    /// 业务作用：保留 null 与缺失节点的区别。
    /// 参数说明：无。
    /// 返回：显式 null。
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    /// 业务作用：保持文档布尔值的原始类型。
    /// 参数说明：`value` 是文档值。
    /// 返回：布尔节点。
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<Value, E> {
        Ok(value.into())
    }
    /// 业务作用：保持有符号整数精度。
    /// 参数说明：`value` 是文档值。
    /// 返回：整数节点。
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Value, E> {
        Ok(value.into())
    }
    /// 业务作用：保持无符号整数精度。
    /// 参数说明：`value` 是文档值。
    /// 返回：整数节点。
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Value, E> {
        Ok(value.into())
    }
    /// 业务作用：拒绝不能由配置树表示的非有限数。
    /// 参数说明：`value` 是文档浮点值。
    /// 返回：有限数字或安全格式错误。
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Value, E> {
        Number::from_f64(value).map(Value::Number).ok_or_else(|| {
            self.budget
                .fail(ConfigError::new(ErrorKind::Format).at(&self.path))
        })
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
/// 返回：唯一 YAML 文档的树。
fn parse_yaml(text: &str, budget: &mut ParseBudget<'_>) -> Result<Value> {
    let mut parser = YamlReader {
        parser: Parser::new_from_str(text),
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
                Event::DocumentStart => {
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
    parser: Parser<std::str::Chars<'a>>,
    position: SourcePosition,
}

/// 业务作用：屏蔽 YAML 后端可能携带原文的错误。
/// 参数说明：`parser` 为流式解析状态。
/// 返回：下一个语法事件或安全格式错误。
fn next_event(parser: &mut YamlReader<'_>) -> Result<Event> {
    parser
        .parser
        .next_token()
        .map(|(event, marker)| {
            parser.position = SourcePosition {
                line: marker.line(),
                column: marker.col() + 1,
            };
            event
        })
        .map_err(|error| {
            ConfigError::new(ErrorKind::Format)
                .positioned(error.marker().line(), error.marker().col() + 1)
        })
}

/// 业务作用：有界构建 YAML 子树并在别名复制前计费。
/// 参数说明：`parser` 提供事件；`event` 是首事件；`budget` 是共享限额；`path` 为节点；`anchors` 仅保留已完整定义的锚点。
/// 返回：完整子树，不允许自循环锚点和未知标签。
fn yaml_node(
    parser: &mut YamlReader<'_>,
    event: Event,
    budget: &mut ParseBudget<'_>,
    path: &ConfigPath,
    anchors: &mut BTreeMap<usize, Value>,
) -> Result<Value> {
    budget.node(path)?;
    let (value, anchor) = match event {
        Event::Scalar(text, style, anchor, tag) => {
            budget.string(&text, path)?;
            (yaml_scalar(&text, style, tag.as_ref())?, anchor)
        }
        Event::MappingStart(anchor, tag) => {
            if tag.is_some() {
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
                if key_anchor != 0 || (text == "<<" && style == TScalarStyle::Plain) {
                    return Err(ConfigError::new(ErrorKind::Unsupported).at(path));
                }
                let Value::String(key) = yaml_scalar(&text, style, tag.as_ref())? else {
                    return Err(ConfigError::new(ErrorKind::Format).at(path));
                };
                validate_key(&key, path)?;
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
        Event::SequenceStart(anchor, tag) => {
            if tag.is_some() {
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
            budget.copy(value, path)?;
            (value.clone(), 0)
        }
        _ => return Err(ConfigError::new(ErrorKind::Format).at(path)),
    };
    if anchor != 0 {
        budget.copy(&value, path)?;
        anchors.insert(anchor, value.clone());
    }
    Ok(value)
}

/// 业务作用：保留引号字符串，仅转换 YAML 明确标量，不忽略未知 tag。
/// 参数说明：`text` 为文本；`style` 为引号风格；`tag` 为显式标签。
/// 返回：有限标量或格式/不支持错误。
fn yaml_scalar(text: &str, style: TScalarStyle, tag: Option<&Tag>) -> Result<Value> {
    if let Some(tag) = tag {
        if tag.handle != "tag:yaml.org,2002:" && tag.handle != "!!" {
            return Err(ConfigError::new(ErrorKind::Unsupported));
        }
        return match tag.suffix.as_str() {
            "str" => Ok(text.into()),
            "int" => parse_integer(text),
            "bool" => match text {
                "true" => Ok(true.into()),
                "false" => Ok(false.into()),
                _ => Err(ConfigError::new(ErrorKind::Format)),
            },
            "null" if matches!(text, "" | "~" | "null" | "Null" | "NULL") => Ok(Value::Null),
            "float" => text
                .parse::<f64>()
                .ok()
                .and_then(Number::from_f64)
                .map(Value::Number)
                .ok_or_else(|| ConfigError::new(ErrorKind::Format)),
            _ => Err(ConfigError::new(ErrorKind::Unsupported)),
        };
    }
    if style != TScalarStyle::Plain {
        return Ok(text.into());
    }
    let numeric = text.strip_prefix(['-', '+']).unwrap_or(text);
    if !numeric.is_empty() && numeric.bytes().all(|byte| byte.is_ascii_digit()) {
        return parse_integer(text);
    }
    match Yaml::from_str(text) {
        Yaml::String(text) => Ok(text.into()),
        Yaml::Integer(value) => Ok(value.into()),
        Yaml::Boolean(value) => Ok(value.into()),
        Yaml::Null => Ok(Value::Null),
        Yaml::Real(value) => value
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| ConfigError::new(ErrorKind::Format)),
        _ => Err(ConfigError::new(ErrorKind::Format)),
    }
}

/// 业务作用：有符号或无符号整数直接进入树，避免经过浮点丢失精度。
/// 参数说明：`text` 是十进制整数文本。
/// 返回：可表示整数，溢出时拒绝。
fn parse_integer(text: &str) -> Result<Value> {
    if let Ok(value) = text.parse::<i64>() {
        return Ok(value.into());
    }
    text.parse::<u64>()
        .map(Value::from)
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
